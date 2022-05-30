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
	# conn = pymysql.connect(user='fayewong', password='4012', host='124.222.30.249', database='covid-19_data', port=3306,
	#                        charset="utf8")
	# 创建链接对象:云端数据库
	conn = pymysql.connect(user='root', password='4012', host='127.0.0.1', database='covid-19_data', port=3306,
	                       charset="utf8")
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
	# resp = requests.get(data_api, headers=headers_)
	# content = resp.content
	with open('./1.json', 'rb') as r:
		content = r.read()
	# 将数据序列化成json对象
	data = json.loads(content)
	# # 使用jsonpath解析:解析各个国家的数据
	# areaTree_country = jsonpath.jsonpath(data, '$..areaTree')[0]
	# 使用jsonpath解析:解析国内各个省自治区直辖市数据
	areaTree_province = jsonpath.jsonpath(data, '$..areaTree')[0][2]
	# # 调用获取国家级疫情数据的函数
	# get_country_data(areaTree_country)
	# 调用获取国内省级疫情数据的函数
	get_province_data(areaTree_province)


# 获取省级疫情数据
def get_province_data(areaTree_province):
	# 连接数据库获取游标对象
	conn, curs = create_conn()
	for province in areaTree_province:
		print(province[''])


# 获取国家级疫情数据
def get_country_data(areaTree_country):
	# 链接数据库获取游标对象
	conn, curs = create_conn()
	for country in areaTree_country:
		lastUpdateTime = jsonpath.jsonpath(country, '$.lastUpdateTime')[0]
		name = jsonpath.jsonpath(country, '$.name')[0]
		uni = str(name) + str(lastUpdateTime).split(' ')[0]
		# 日期当天
		today = jsonpath.jsonpath(country, '$.today')[0]
		# 截至日期当天累计
		total = jsonpath.jsonpath(country, '$.total')[0]
		try:
			# 定义sql插入当天数据:根据uni列,存在就更新,不能存在就插入
			update_today_sql = "REPLACE INTO country_day(`id`,`name`,`confirm`,`suspect`,`heal`,`dead`,`severe`,`storeConfirm`,`lastUpdateTime`,`uni`) VALUES (0,'%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
				name, today['confirm'], today['suspect'], today['heal'], today['dead'], today['severe'],
				today['storeConfirm'], lastUpdateTime, uni)
			# 定义sql插入截至当天的累计数据:根据uni列,存在就更新,不能存在就插入
			update_total_sql = "REPLACE INTO country_total(`id`,`name`,`confirm`,`suspect`,`heal`,`dead`,`severe`,`input`,`lastUpdateTime`,`uni`) VALUES (0,'%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
				name, total['confirm'], total['suspect'], total['heal'], total['dead'], total['severe'], total['input'],
				lastUpdateTime, uni)
		except:
			continue
		# 链接数据库保存数据
		# 使用游标对象执行插入操作
		curs.execute(update_today_sql)
		curs.execute(update_total_sql)
		print(f'国家数据保存成功\t\t{name}')
	# 提交数据
	conn.commit()
	# 关闭链接
	close_conn(conn, curs)


if __name__ == '__main__':
	# 获取疫情json数据
	get_json_data()
