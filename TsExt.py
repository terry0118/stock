import json
import demjson
import pandas as pd
import requests
import tushare as ts
from pandas.io.sql import to_sql
from tushare.util import dateu as du

"""先获取每日龙虎榜股票，再获取机构买卖详细"""


def stk_lhb_broker_detail(date, code=None):
    texts = list()
    for i in range(12):
        if (i < 10):
            i = '0' + str(i)
        sina_url = 'http://vip.stock.finance.sina.com.cn/q/api/jsonp.php/var%20details=/InvestConsultService.getLHBComBSData?symbol=' + \
                   code + '&tradedate=' + date + '&type=' + str(i)
        print(sina_url)
        try:
            r = requests.get(sina_url,timeout=5)
            if (len(r.text) > 100):
                text1 = r.text.split('=((')[1].replace('))', '')
                text2 = str(demjson.decode(text1)).replace('\'', '"')
                text3 = json.loads(text2)
                df = pd.DataFrame(text3['buy'])
                df1 = df.append(pd.DataFrame(text3['sell']))
                df1['date'] = date
                return df1
                break
        except Exception as e:
            print(e)


def stk_get_hist_data(code, start):
    try:
        print("获取股票历史数据：" + str(code))
        df = ts.get_hist_data(code=code, retry_count=3, pause=1, start=start)
        df = df.sort_index()
        df['code'] = code
        df['ma50'] = pd.rolling_mean(df['close'], 50)
        df['v_ma50'] = pd.rolling_mean(df['volume'], 50)
        df['ma120'] = pd.rolling_mean(df['close'], 120)
        df['v_ma120'] = pd.rolling_mean(df['volume'], 120)
        df = df.fillna(0)
    except AttributeError as e:
        print("获取股票历史数据失败: " + str(code) + str(e))
    return df


def stk_clear_table(engine, sql):
    try:
        pd.io.sql.execute(sql, engine)
    except Exception as e:
        print(e)


def stk_get_today_ticks(code):
    df = ts.get_today_ticks()
    tks = dict()
    group = df[(df['type'] == '买盘')].groupby('price')
    g = group['amount'].sum().sort_values(ascending=False).head(3)
    tks['买盘'] = g.index[0]
    return tks


def stk_get_hist_ticks(code, date):
    df = ts.get_tick_data(code=code, date=date,src='tt')
    tks = dict()
    group = df[(df['type'] == '买盘')].groupby('price')
    g = group['amount'].sum().sort_values(ascending=False).head(3)
    tks['code'] = code
    tks['price'] = g.index.tolist()
    tks['buy'] = g.values.tolist()
    tks['date'] = date
    return tks


def stk_get_basics(engine):
    print("\n =======获取股票列表============")
    try:
        df = ts.get_stock_basics()
        return df
    except Exception as e:
        print(e)


def stk_get_today_all(engine):
    df = ts.get_today_all()
    df['date']=du.last_tddate()
    return df


def stk_broker_holds(code,quarter):
    try:
        url = 'http://vip.stock.finance.sina.com.cn/q/api/jsonp.php/var%20details=/ComStockHoldService.getJGCGDetail?symbol=' \
           + str(code) + '&quarter='  + str(quarter)
        print(url)
    except Exception as e:
        print("create url failed.")

    try:
        r = requests.get(url=url)
        t = r.text.split('((')[1].replace('))','')
        format_t = demjson.encode(demjson.decode(t))
        json_text = json.loads(format_t)
    except Exception as e:
        print("Retrive data or restruct data failed.")

    broker_types = ["qfii","fund","insurance","socialSecurity","stock"]
    dflist=[]
    for broker_type in broker_types:
        del json_text['data'][broker_type]['total']
        df = pd.DataFrame(data=json_text['data'][broker_type])
        df2 = df.T
        df2['quarter'] = quarter
        df2['code'] = code
        dflist.append(df2)
    return pd.concat(dflist)
