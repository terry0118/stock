# -*- coding: utf-8 -*-
import json
import re
from datetime import date
import time
import pandas as pd
import requests
import tushare as ts
from bs4 import BeautifulSoup as bs
from pandas.io.sql import to_sql
from sqlalchemy import create_engine


class toJson():
    def __init__(self):
        self.t1 = str()
        self.l1 = list()
        self.d1 = dict()

    def parser(self, txt):
        self.txt = txt
        if (len(self.txt) < 100):
            pass
        else:
            for m in list(self.txt.replace('[', '').replace(']', '').replace('{', '').split('},')):
                for n in m.split(','):
                    (k, v) = n.split(':', 1)
                    v = v.replace("}", '')
                    v = v.replace('"', '')
                    self.d1[k] = v
                self.l1.append(self.d1)
                self.d1 = dict()
            self.t1 = str(self.l1)
            self.t1 = self.t1.replace('\'', '\"')
            return self.t1


class tushareEXT():
    def __init__(self):
        pass

    def duoying_dxcj(self):
        self.url = 'http://vip.stock.finance.sina.com.cn/q/go.php/vDYData/kind/dxcj/index.phtml?last=1&p=1&num=10000'
        try:
            self.r = requests.get(self.url)
            self.r.encoding = 'gb2312'
            self.soup = bs(self.r.text, 'html.parser')
            self.trs = self.soup.find_all('tr')
        except Exception as e:
            print(e)

        try:
            self.stocks = []
            for tr in self.trs:
                self.stock = []
                self._soup = bs(str(tr), 'html.parser')
                self.tds = self._soup.find_all('td')
                for td in self.tds:
                    self.td = td.string.replace('%', '')
                    self.stock.append(self.td)
                self.stocks.append(self.stock)

            self.cols = ['code', 'name', 'price', 'pcratio', 'yali', 'zhicheng', 'liangjiapeihe', 'jincha']
            del self.stocks[0]
            self.data = pd.DataFrame(data=self.stocks, columns=self.cols)
            return self.data
        except Exception as e:
            print(e)

    def duoying_zlsp(self):
        self.url = 'http://vip.stock.finance.sina.com.cn/q/go.php/vDYData/kind/zlsp/index.phtml?last=2&p=1&num=10000'
        try:
            self.r = requests.get(self.url)
            self.r.encoding = 'gb2312'
            self.soup = bs(self.r.text, 'html.parser')
            self.trs = self.soup.find_all('tr')
        except Exception as e:
            print(e)

        try:
            self.stocks = []
            for tr in self.trs:
                self.stock = []
                self._soup = bs(str(tr), 'html.parser')
                self.tds = self._soup.find_all('td')
                for td in self.tds:
                    self.td = td.string.replace('%', '')
                    self.stock.append(self.td.replace('万', ''))
                self.stocks.append(self.stock)

            self.cols = ['code', 'name', 'price', 'pcratio', 'zuli_net', 'follow_net', 'sanhu_net', 'zl_ma5',
                         'zuli_ma10']
            del self.stocks[0]
            self.data = pd.DataFrame(data=self.stocks, columns=self.cols)
            return self.data
        except Exception as e:
            print(e)

    def duoying_bdmr(self):
        self.url = 'http://vip.stock.finance.sina.com.cn/q/go.php/vDYData/kind/bdmr/index.phtml?last=1&p=1&num=10000'
        try:
            self.r = requests.get(self.url)
            self.r.encoding = 'gb2312'
            self.soup = bs(self.r.text, 'html.parser')
            self.trs = self.soup.find_all('tr')
        except Exception as e:
            print(e)

        try:
            self.stocks = []
            for tr in self.trs:
                self.stock = []
                self._soup = bs(str(tr), 'html.parser')
                self.tds = self._soup.find_all(['td'])
                for td in self.tds[0:-1]:
                    self.stock.append(td.string.replace('%', ''))
                self.stock.append(self.tds[5].get_text())
                self.stocks.append(self.stock)

            self.cols = ['code', 'name', 'price', 'pcratio', 'buy_point', 'zhenduan']
            del self.stocks[0]
            self.data = pd.DataFrame(data=self.stocks, columns=self.cols)
            return self.data
        except Exception as e:
            print(e)

    def duoying_kpjc(self):
        self.url = 'http://vip.stock.finance.sina.com.cn/q/go.php/vDYData/kind/kpjc/index.phtml?last=1&p=1&num=10000'
        try:
            self.r = requests.get(self.url)
            self.r.encoding = 'gb2312'
            self.soup = bs(self.r.text, 'html.parser')
            self.trs = self.soup.find_all('tr')
        except Exception as e:
            print(e)

        try:
            self.stocks = []
            for tr in self.trs:
                self.stock = []
                self._soup = bs(str(tr), 'html.parser')
                self.tds = self._soup.find_all('td')
                for td in self.tds:
                    self.td = td.get_text().replace('%', '')
                    self.stock.append(self.td.replace('万', ''))
                self.stocks.append(self.stock)

            self.cols = ['code', 'name', 'price', 'pcratio', 'zuli_net', 'follow_net', 'sanhu_net', 'zl_kp', 'bljc']
            del self.stocks[0]
            self.data = pd.DataFrame(data=self.stocks, columns=self.cols)
            return self.data
        except Exception as e:
            print(e)

    def duoying_znzd(self):
        self.url = 'http://vip.stock.finance.sina.com.cn/q/go.php/vDYData/kind/znzd/index.phtml?last=1&p=1&num=10000'
        try:
            self.r = requests.get(self.url)
            self.r.encoding = 'gb2312'
            self.soup = bs(self.r.text, 'html.parser')
            self.trs = self.soup.find_all('tr')
        except Exception as e:
            print(e)

        try:
            self.stocks = []
            for tr in self.trs:
                self.stock = []
                self._soup = bs(str(tr), 'html.parser')
                self.tds = self._soup.find_all(['td'])
                for td in self.tds[0:-1]:
                    self.stock.append(td.string)
                self.stock.append(self.tds[4].get_text())
                self.stocks.append(self.stock)

            self.cols = ['code', 'name', 'price', 'pcratio', 'znzd']
            del self.stocks[0]
            self.data = pd.DataFrame(data=self.stocks, columns=self.cols)
            return self.data
        except Exception as e:
            print(e)

    def eastmoney_broker_hyyyb(self,day=date.today()):
        self.curdate = day
        # self.curdate = '2018-01-25'
        self.url = 'http://data.eastmoney.com/DataCenter_V3/stock2016/ActiveStatistics/pagesize=5000,page=1,sortRule=-1,sortType=JmMoney,startDate=' + str(
            self.curdate) + ',endDate=' + str(self.curdate) + ',gpfw=0,js=var%20data_tab_1.html'
        self.r = requests.get(self.url)
        self.r.encoding = 'gb2312'
        self.p = re.compile(r',"url.*$')
        self.t1 = self.r.text.replace('var data_tab_1=', '').replace('\\', '').replace(
            '{"success":true,"pages":1,"data":', '')
        self.t2 = re.sub(self.p, '', self.t1).replace('"[', '[').replace(']"', ']')
        self.js = json.loads(self.t2)
        self.dataframe = list()
        for j in self.js:
            SName = str()
            js2 = json.loads(str(j).replace('\'', '\"'))
            if (len(js2["SName"]) > 0):
                for stk in json.loads(str(js2["SName"]).replace('\'', '\"')):
                    SName = SName + stk["CodeName"] + " "
            else:
                SName = 'NA'
            js2["SName"] = SName
            if (js2["Smoney"].strip() == ""):
                js2["Smoney"] = 0
            if (js2["JmMoney"].strip() == ""):
                js2["JmMoney"] = 0
            if (js2["Bmoney"].strip() == ""):
                js2["Bmoney"] = 0
            # print(list(js2.values()))
            self.dataframe.append(list(js2.values()))
            self.cols = ['YybCode', 'YybName', 'YybBCount', 'YybSCount', 'Bmoney', 'Smoney', 'JmMoney', 'SName',
                         'TDate']
        self.df = pd.DataFrame(data=self.dataframe, columns=self.cols)

        return self.df
        # print(self.df)
        # print(self.t2)

    def stk_bigTrade_type(self, code):
        self.code = str(code)
        self.curdate = date.today()
        self.url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page=1&num=2000&sort=opendate&asc=0&daima=' + self.code
        print(self.url)
        self.r = requests.get(self.url)
        self.r.encoding = 'gb2312'
        self.p = toJson()
        self.tt = self.p.parser(txt=self.r.text)
        self.js = json.loads(self.tt)
        self.df = pd.DataFrame(data=self.js)
        max_idx = max(self.df.index)
        i = int(0)
        while (i < int(max_idx) or i == 0):
            chgLT0 = 0
            chgLT5 = 0
            chgLT_5 = 0
            chgLT_10 = 0
            try:
                chg = (float(self.df.loc[i, 'trade']) - float(self.df.loc[i + 1, 'trade'])) / float(
                    self.df.loc[i + 1, 'trade'])
            except Exception as e:
                chg = 0
            if (chg >= 0.05 and chg < 0.11):
                chgLT5 = 1
            elif (chg >= 0 and chg < 0.05):
                chgLT0 = 1
            elif (chg >= -0.05 and chg < 0):
                chgLT_5 = 1
            elif (chg >= -0.11 and chg < -0.05):
                chgLT_10 = 1
            self.df.loc[i, 'chgLT_10'] = chgLT_10
            self.df.loc[i, 'chgLT_5'] = chgLT_5
            self.df.loc[i, 'chgLT0'] = chgLT0
            self.df.loc[i, 'chgLT5'] = chgLT5
            i += 1
        self.df['code'] = self.code
        return self.df


    def eastmoney_broker_countByDay(self,day=date.today()):
        self.curdate = day
        # self.curdate = '2018-01-25'
        # http://data.eastmoney.com/DataCenter_V3/stock2016/DailyStockListStatistics/pagesize=50,page=1,sortRule=-1,sortType=PBuy,startDate=2018-01-10,endDate=2018-01-10,gpfw=0,js=var%20data_tab_1.html?rt=25259760
        self.url = 'http://data.eastmoney.com/DataCenter_V3/stock2016/DailyStockListStatistics/pagesize=500,page=1,sortRule=-1,sortType=PBuy,startDate=' + str(
            self.curdate) + ',endDate=' + str(self.curdate) + ',gpfw=0,js=var%20data_tab_1.html'
        self.r = requests.get(self.url)
        self.r.encoding = 'gb2312'
        self.p = re.compile(r',"url.*$')
        self.t1 = self.r.text.replace('var data_tab_1=', '').replace('\\', '').replace(
            '{"success":true,"pages":1,"data":', '')
        self.t2 = re.sub(self.p, '', self.t1).replace('"[', '[').replace(']"', ']')
        self.js = json.loads(self.t2)
        self.df = pd.DataFrame(data=self.js)
        self.df.drop(['RChange10DC', \
       'RChange10DO', 'RChange15DC', 'RChange15DO', 'RChange1DC', 'RChange1DO',\
       'RChange1M', 'RChange1Y', 'RChange20DC', 'RChange20DO', 'RChange2DC', \
       'RChange2DO', 'RChange30DC', 'RChange30DO', 'RChange3DC', 'RChange3DO', \
       'RChange3M', 'RChange5DC', 'RChange5DO', 'RChange6M','CTypeDes','Ntransac','Statistic','Dchratio','RID','CTypeDes','MEMO'],axis=1,inplace=True)

        return self.df

if __name__ == '__main__':

    print("Begin...")

    engine = create_engine('mysql+pymysql://root:password@127.0.0.1/tushare?charset=utf8')
    te = tushareEXT()

    # print("DXCJ")
    # df = te.duoying_dxcj()
    # pd.io.sql.execute("delete from stk_duoying_dxcj;",engine)
    # df.to_sql('stk_duoying_dxcj',engine,if_exists='append')
    #
    #
    # print("ZLSP")
    # df = te.duoying_zlsp()
    # pd.io.sql.execute("delete from stk_duoying_zlsp;",engine)
    # df.to_sql('stk_duoying_zlsp',engine,if_exists='append')
    #
    # print("BDMR")
    # df = te.duoying_bdmr()
    # # print(df)
    # pd.io.sql.execute("delete from  stk_duoying_bdmr;",engine)
    # df.to_sql('stk_duoying_bdmr',engine,if_exists='append')
    #
    # print("KPJC")
    # df = te.duoying_kpjc()
    # # print(df)
    # pd.io.sql.execute("delete from  stk_duoying_kpjc;",engine)
    # df.to_sql('stk_duoying_kpjc',engine,if_exists='append')
    #
    # print("ZNZD")
    # df = te.duoying_znzd()
    # # print(df)
    # pd.io.sql.execute("delete from  stk_duoying_znzd;",engine)
    # df.to_sql('stk_duoying_znzd',engine,if_exists='append')
    #
    # print("HYYYB")
    # cur_date = date.today()
    # # cur_date = '2018-01-12'
    # df = te.eastmoney_broker_hyyyb(day=cur_date)
    # # cur_date = '2018-01-09'
    # pd.io.sql.execute("delete from est_broker_hyyyb where Tdate=\'" + str(cur_date) + '\';',engine)
    # df.to_sql('est_broker_hyyyb',engine,if_exists='append')
    # pd.io.sql.execute("delete from est_broker_hyyyb where SName='NA';",engine)
    #
    # print("CountByDay")
    # cur_date = date.today()
    # # cur_date = '2018-01-12'
    # df = te.eastmoney_broker_countByDay(day=cur_date)
    # pd.io.sql.execute("delete from est_broker_countByDay where Tdate=\'" + str(cur_date) + '\';',engine)
    # df.to_sql('est_broker_countByDay',engine,if_exists='append')


    stkList = ts.get_stock_basics()
    x = 0
    threads = []
    for stk in stkList.index:
        try:
            print(x, stk)
            if (int(stk) < 600000):
                code = 'sz' + stk
            else:
                code = 'sh' + stk
            df = te.stk_bigTrade_type(code)
            # print(df)
            sql = "delete from stk_bigtrade_type where code = \'" + code + "\';"
            print(sql)
            pd.io.sql.execute(sql, engine)
            df.to_sql('stk_bigtrade_type', engine, if_exists='append')
            x += 1
        except Exception as e:
            print(e)
