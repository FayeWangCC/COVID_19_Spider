import datetime
import jsonpath
import json
import pymysql
import requests

# 请求头
headers_ = {
	'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.62 Safari/537.36'
}
# 数据接口
data_api = 'https://c.m.163.com/ug/api/wuhan/app/data/list-total'
# 获取昨天的日期
yesterday = datetime.date.today() - datetime.timedelta(days=2)


# 创建数据库连接
def create_conn():
	# 创建链接对象:云端数据库
	conn = pymysql.connect(user='fayewong', password='4012', host='124.222.30.249', database='covid_19', port=3306,
	                       charset="utf8")
	# 创建链接对象:本地数据库
	# conn = pymysql.connect(user='root', password='4012', host='127.0.0.1', database='covid-19_data', port=3306,
	#                        charset="utf8")
	# 创建游标对象
	curs = conn.cursor()
	return conn, curs


# 关闭数据库链接
def close_conn(conn, curs):
	conn.close()
	curs.close()


# 发送请求解析数据
def get_json_data():
	# 发送请求获取json数据
	resp = requests.get(data_api, headers=headers_)
	content = resp.content
	# 将数据序列化成json对象
	data = json.loads(content)
	# 使用jsonpath解析:解析各个国家的数据
	areaTree_country = jsonpath.jsonpath(data, '$..areaTree')[0]
	# 使用jsonpath解析:解析国内各个省自治区直辖市数据
	areaTree_province = jsonpath.jsonpath(data, '$..areaTree')[0][2]
	# 调用获取全球各国疫情数据的函数
	get_country_data(areaTree_country)
	# 调用获取国内省级疫情数据的函数
	get_province_data(areaTree_province)
	# 调用获取国内市级疫情数据的函数
	get_city_data(areaTree_province)


# 获取国内各市级疫情数据
def get_city_data(areaTree_province):
	# 连接数据库获取游标对象
	conn, curs = create_conn()
	# 获取到所有省自治区直辖市数据列表
	province_data_list = jsonpath.jsonpath(areaTree_province, '$.children')[0]
	# 便利列表获取每个省自治区直辖市的数据
	for province_data in province_data_list:
		# 省自治区直辖市名称
		province_name = province_data['name']
		city_data_list = jsonpath.jsonpath(province_data, '$.children')[0]
		for city_data in city_data_list:
			# 省自治区直辖市名称
			city_name = city_data['name']
			# 省自治区直辖市最近数据更新时间
			lastUpdateTime = city_data['lastUpdateTime']
			# 用作唯一数据值,插入数据时如果存在就更新,不存在就插入需要unique
			uni = str(city_name) + str(lastUpdateTime).split(' ')[0]
			# lastUpdateTime日期当天
			city_today = city_data['today']
			# 截至lastUpdateTime日期当天累计
			city_total = city_data['total']
			try:
				# 定义sql插入当天数据:根据uni列,存在就更新,不能存在就插入
				update_today_sql = "REPLACE INTO city_today(`id`,`name`,`confirm`,`suspect`,`heal`,`dead`,`severe`,`storeConfirm`,`lastUpdateTime`,`province_name`,`uni`) VALUES (0,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
					city_name, city_today['confirm'], city_today['suspect'], city_today['heal'],
					city_today['dead'], city_today['severe'],
					city_today['storeConfirm'], lastUpdateTime, province_name, uni)
				# 定义sql插入截至当天的累计数据:根据uni列,存在就更新,不能存在就插入
				update_total_sql = "REPLACE INTO city_total(`id`,`name`,`confirm`,`suspect`,`heal`,`dead`,`severe`,`lastUpdateTime`,`province_name`,`uni`) VALUES (0,'%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
					city_name, city_total['confirm'], city_total['suspect'], city_total['heal'],
					city_total['dead'], city_total['severe'],
					lastUpdateTime, province_name, uni)
			except:
				continue
			# 使用游标对象执行插入操作
			curs.execute(update_today_sql)
			curs.execute(update_total_sql)
		# 提交数据
		conn.commit()
	print(f'全国各省市数据已更新')
	# 关闭链接
	close_conn(conn, curs)


# 获取省级疫情数据
def get_province_data(areaTree_province):
	# 连接数据库获取游标对象
	conn, curs = create_conn()
	# 获取到所有省自治区直辖市数据列表
	province_data_list = jsonpath.jsonpath(areaTree_province, '$.children')[0]
	# 便利列表获取每个省自治区直辖市的数据
	for province_data in province_data_list:
		# 省自治区直辖市名称
		province_name = province_data['name']
		# 省自治区直辖市最近数据更新时间
		lastUpdateTime = province_data['lastUpdateTime']
		# 用作唯一数据值,插入数据时如果存在就更新,不存在就插入需要unique
		uni = str(province_name) + str(lastUpdateTime).split(' ')[0]
		# lastUpdateTime日期当天
		province_today = province_data['today']
		# 截至lastUpdateTime日期当天累计
		province_total = province_data['total']
		try:
			# 定义sql插入当天数据:根据uni列,存在就更新,不能存在就插入
			update_today_sql = "REPLACE INTO province_today(`id`,`name`,`confirm`,`suspect`,`heal`,`dead`,`severe`,`storeConfirm`,`lastUpdateTime`,`uni`) VALUES (0,'%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
				province_name, province_today['confirm'], province_today['suspect'], province_today['heal'],
				province_today['dead'], province_today['severe'],
				province_today['storeConfirm'], lastUpdateTime, uni)
			# 定义sql插入截至当天的累计数据:根据uni列,存在就更新,不能存在就插入
			update_total_sql = "REPLACE INTO province_total(`id`,`name`,`confirm`,`suspect`,`heal`,`dead`,`severe`,`input`,`lastUpdateTime`,`uni`) VALUES (0,'%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
				province_name, province_total['confirm'], province_total['suspect'], province_total['heal'],
				province_total['dead'], province_total['severe'], province_total['input'],
				lastUpdateTime, uni)
		except:
			continue
		# 链接数据库保存数据
		# 使用游标对象执行插入操作
		curs.execute(update_today_sql)
		curs.execute(update_total_sql)
	# 提交数据
	conn.commit()
	print(f'全国各省自治区直辖市数据已更新')
	# 关闭链接
	close_conn(conn, curs)


# 获取国家级疫情数据
def get_country_data(areaTree_country):
	# 链接数据库获取游标对象
	conn, curs = create_conn()
	for country in areaTree_country:
		country_name = jsonpath.jsonpath(country, '$.name')[0]
		lastUpdateTime = jsonpath.jsonpath(country, '$.lastUpdateTime')[0]
		uni = str(country_name) + str(lastUpdateTime).split(' ')[0]
		# lastUpdateTime日期当天
		country_today = jsonpath.jsonpath(country, '$.today')[0]
		# 截至日期lastUpdateTime当天累计
		country_total = jsonpath.jsonpath(country, '$.total')[0]
		try:
			# 定义sql插入当天数据:根据uni列,存在就更新,不能存在就插入
			update_today_sql = "REPLACE INTO country_today(`id`,`name`,`confirm`,`suspect`,`heal`,`dead`,`severe`,`storeConfirm`,`lastUpdateTime`,`uni`) VALUES (0,'%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
				country_name, country_today['confirm'], country_today['suspect'], country_today['heal'],
				country_today['dead'], country_today['severe'],
				country_today['storeConfirm'], lastUpdateTime, uni)
			# 定义sql插入截至当天的累计数据:根据uni列,存在就更新,不能存在就插入
			update_total_sql = "REPLACE INTO country_total(`id`,`name`,`confirm`,`suspect`,`heal`,`dead`,`severe`,`input`,`lastUpdateTime`,`uni`) VALUES (0,'%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
				country_name, country_total['confirm'], country_total['suspect'], country_total['heal'],
				country_total['dead'], country_total['severe'], country_total['input'],
				lastUpdateTime, uni)
		except:
			continue
		# 链接数据库保存数据
		# 使用游标对象执行插入操作
		curs.execute(update_today_sql)
		curs.execute(update_total_sql)
	# 提交数据
	conn.commit()
	print(f'全球各国数据已更新')
	# 关闭链接
	close_conn(conn, curs)


if __name__ == '__main__':
	# 获取疫情json数据
	get_json_data()
