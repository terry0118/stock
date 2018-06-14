import random
import threading
import time
from datetime import date
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
from mysql_connect import connMysql


def stk_basics():
    print("\n =======获取股票列表============")
    try:
        df = ts.get_stock_basics()
        conn = connMysql().createconn()
        cur = conn.cursor()
        cur.execute('truncate table stk_basics;')
        cur.close()
        conn.close()
        df.to_sql('stk_basics', engine, if_exists='append')
    except:
        print("获取股票基本信息失败")


# def stk_k_line_data(code, dat=str(date.today())):
def stk_k_line_data(code):
    print("\n获取K线数据：" + code)
    conn = connMysql().createconn()
    conn.autocommit(True)
    cur = conn.cursor()
    # sql = 'delete from stk_k_line_data where code=' + '\'' + code + '\'' + ' and date=' + '\'' + dat + '\';'
    sql = 'delete from stk_k_line_data where code=' + '\'' + code + '\';'
    #print(sql)
    cur.execute(sql)
    df = ts.get_hist_data(code=code)
    try:
        if (type(df) == pandas.core.frame.DataFrame):
            df['code'] = code
            # print(df)
            df.to_sql('stk_k_line_data', engine, if_exists='append')
    except:
        print(code + '： 获取K线数据失败')


def bigTrade(code, dat=str(date.today())):
    df = ts.get_sina_dd(code=code, date=dat, vol=600)  # 默认400手
    conn = connMysql().createconn()
    conn.autocommit(True)
    cur = conn.cursor()
    sql = 'delete from stk_dadan_detail where code=' + '\'' + code + '\'' + ' and date=' + '\'' + dat + '\';'
    cur.execute(sql)

    try:
        if (type(df) == pandas.core.frame.DataFrame):
            df['date'] = dat
            #print()
            df.to_sql('stk_dadan_detail', engine, if_exists='append')
    except:
        print(code + ': 获取大单数据失败')


def stk_get_sme_classified():
    try:
        conn = connMysql().createconn()
        cur = conn.cursor()
        cur.execute('truncate table stk_sme_classified;')
        cur.close()
        conn.close()
        df = ts.get_sme_classified()
        df.to_sql('stk_sme_classified', engine, if_exists='append')
    except:
        print("获取中小板数据失败")

def stk_get_hist_data(stkcode,start='2017-07-01'):
    try:
        print("获取股票历史数据：" + str(stkcode))
        df = ts.get_hist_data(code=stkcode,retry_count=3,pause=1)
        df = df.sort_index()
        df['code'] = stkcode
        df['ma50'] = pd.rolling_mean(df['close'],50)
        df['v_ma50'] = pd.rolling_mean(df['volume'],50)
        df['ma120'] = pd.rolling_mean(df['close'],120)
        df['v_ma120'] = pd.rolling_mean(df['volume'],120)
        df = df.fillna(0)
        df.to_sql('stk_hist_data', engine, if_exists='append')
    except Exception as e:
        print("获取股票历史数据失败: " + str(stkcode) + str(e))




if (__name__ == '__main__'):
    engine = create_engine('mysql+pymysql://root:password@127.0.0.1/tushare?charset=utf8')

    # sql = 'delete from stk_hist_data where code = ' + '\'' + stkcode + '\';'
    sql = 'truncate table stk_hist_data;'
    conn = connMysql().createconn()
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()

    stk_basics()
    stkList = ts.get_stock_basics()
    threads = []
    x = 0
    for stk in stkList.index:
        # stk_get_hist_data(stkcode=stk)
        try:
            print(stk)
            t = threading.Thread(target=stk_get_hist_data,args=(stk,))
            threads.append(t)
        except Exception as e:
            print(e)
    try:
        for tt in threads:
            if (x % 10 == 0):
                time.sleep(random.randint(10, 20))
            tt.setDaemon(True)
            tt.start()
            x += 1
            print(x)
    except Exception as e:
        print(e)


