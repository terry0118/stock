import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import random

# url = 'http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?cmd=C._AB&sty=DCFFITAM&type=ct&st=BalFlowNetRate&sr=-1&p=1&ps=5000&&token=894050c76af8597a853f5b408b759f5d'

url = 'http://gupiaodadan.com/more-1'
proxy = {'http': 'http://wsg.cmszmail.ad:8083'}


def get_netbuy_top50(url, proxy):
    r = requests.get(url, proxies=proxy)
    soup = BeautifulSoup(r.text, 'lxml')
    rows = []
    for tab in soup.find_all('div', {"class": "tablecontent"}):
        for row in tab.find_all('tr'):
            cols = []
            for col in row.find_all('td'):
                cols.append(col.get_text())
            rows.append(cols)
    cols = rows[0]
    rows.pop(0)
    df = pd.DataFrame(rows,columns=cols)
    r.close()
    return df

if __name__ == "__main__":
    stocks = set()
    while True:
        df = get_netbuy_top50(url=url,proxy=proxy)
        for stk in df['股票名称']:
            if stk not in stocks:
                stocks.add(stk)
                print(datetime.now(),stk)
        print("Waiting ...")
        time.sleep(random.randint(60,120))

