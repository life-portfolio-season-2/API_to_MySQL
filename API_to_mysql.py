# Import Library 
import requests
import pandas as pd
import json
import pandas as pd 
import pymysql

# Get minute_chart
url = "https://api.upbit.com/v1/candles/minutes/1?market=KRW-BTC&count=200"
headers = {"accept": "application/json"}
response = requests.get(url, headers=headers)

# To Dataframe
_data = json.loads(response.text)
_df = pd.DataFrame(_data)
_df.to_csv("min_chart.csv")

# Connect MySQL
conn = pymysql.connect(host='127.0.0.1', user = 'root', password='1133', db = 'sys',charset = 'utf8') # MySQL에 연결
curs = conn.cursor(pymysql.cursors.DictCursor) # Mysql cursor 생성, SQL 쿼리 실행 및 결과 처리에 사용

# Execute SQL quries
queries = ["create database chart;", # Create chart Database
           "use chart;", # Use chart
        # Create min_chart Table
        '''create table min_chart(idx int, market VARCHAR(10), candle_date_time_utc VARCHAR(30),
        candle_date_time_kst VARCHAR(30),opening_price double, high_price double, low_price double, trade_price	double, 
        timestamp long, candle_acc_trade_price double, candle_acc_trade_volume	double,unit int);''']
for query in queries:
    curs.execute(query)

# Insert Data to Table
_sql = '''insert into min_chart
(market, candle_date_time_utc, candle_date_time_kst, opening_price, high_price,
low_price, trade_price, timestamp, candle_acc_trade_price, candle_acc_trade_volume, unit)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

for idx in range(len(_df)):
	curs.execute(_sql, tuple(_df.values[idx]))

# Commit
conn.commit()

# Disconnect
curs.close()
conn.close()