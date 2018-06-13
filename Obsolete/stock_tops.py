# -*- coding:utf-8 -*-
from sqlalchemy import create_engine
import tushare as ts
import pymysql
from datetime import date
import json
from mysql_connect import connMysql
import requests


engine = create_engine('mysql+pymysql://root:password@127.0.0.1/tushare?charset=utf8')


def stk_top_list(dat=str(date.today())):
    print("======获取龙虎榜列表============")
    print("\n插入数据：" + 'stk_top_list')
    try:

        df = ts.top_list(retry_count=10,pause=1,date=dat)
        # print(df)
        df.to_sql('stk_top_list',engine,if_exists='append')
    except Exception as e :
        print("insert failed." + str(e))

def stk_top_list_detail(dat=str(date.today())):
    df = ts.top_list(date=dat,retry_count=3,pause=1)
    # print(df)
    #http://vip.stock.finance.sina.com.cn/q/api/jsonp.php/var%20details=/InvestConsultService.getLHBComBSData?symbol=300708&tradedate=2018-01-24&type=5
    for code in (df[df.reason.str.contains('日涨幅偏离值达到7%的前五只证券')]['code']):
        type='01'
        url = 'http://vip.stock.finance.sina.com.cn/q/api/jsonp.php/var%20details=/InvestConsultService.getLHBComBSData?symbol=' + code + '&tradedate=' + dat + '&type=' + str(type)
        r = requests.get(url)
        f = open('r.txt','w+')
        f.write(r.text)
        # print(r.text)
        for line in f.readline():
            r1 = line.replace('.*script.*','')
            print(r1)


def stk_cap_tops(days=5,table = 'stk_cap_tops_ma5'):
    print("\n插入数据：" + table)
    try:
        conn = connMysql().createconn()
        cur = conn.cursor()
        sql = 'truncate table ' + table + ';'
        cur.execute(sql)
        cur.close()
        conn.close()
        df = ts.cap_tops(days=days)
        df.to_sql(table,engine,if_exists='append')
    except:
        print("\n收集个股上榜统计失败")


def stk_broker_tops(days=5,table = 'stk_broker_tops_ma5'):
    print("\n插入数据：" + table)
    try:
        conn = connMysql().createconn()
        cur = conn.cursor()
        sql = 'truncate table ' + table + ';'
        cur.execute(sql)
        cur.close()
        conn.close()
        df = ts.broker_tops(days=days)
        df.to_sql(table,engine,if_exists='append')
    except:
        print("\n收集营业部上榜统计失败")

def stk_inst_tops(days=5,table = 'stk_inst_tops_ma5'):
    print("\n插入数据：" + table)
    try:
        conn = connMysql().createconn()
        cur = conn.cursor()
        sql = 'truncate table ' + table + ';'
        cur.execute(sql)
        cur.close()
        conn.close()
        df = ts.inst_tops(days=days)
        df.to_sql(table,engine,if_exists='append')
    except:
        print("\n收集机构席位追踪失败")

def stk_inst_detail():
    print("======\n机构成交明细============")
    try:
        df = ts.inst_detail()
        #print(df.idx[0])
        df.to_sql('stk_inst_detail',engine,if_exists='append')
         #   print(df)
    except :
        print("insert failed.")





if (__name__  ==  '__main__'):
    stk_top_list()

    stk_cap_tops(days=5,table='stk_cap_tops_ma5')
    stk_cap_tops(days=10,table='stk_cap_tops_ma10')
    stk_cap_tops(days=30,table='stk_cap_tops_ma30')

    stk_broker_tops(days=5,table='stk_broker_tops_ma5')
    stk_broker_tops(days=10,table='stk_broker_tops_ma10')
    stk_broker_tops(days=30,table='stk_broker_tops_ma30')

    stk_inst_tops(days=5,table='stk_inst_tops_ma5')
    stk_inst_tops(days=10,table='stk_inst_tops_ma10')
    stk_inst_tops(days=30,table='stk_inst_tops_ma30')
    #
    stk_inst_detail()
    stk_top_list_detail()
