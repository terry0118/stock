# -*- coding: UTF-8 -*-
from selenium import webdriver
import pandas as pd
import time
import re
from bs4 import BeautifulSoup
import os

def get_jqk_money_fllow():
    option = webdriver.ChromeOptions()
    option.add_argument('headless')
    # option.add_argument('--proxy-server=http://wsg.cmszmail.ad:8083')
    driver = webdriver.Chrome(chrome_options=option)
    driver.get('http://data.10jqka.com.cn/funds/ggzjl/###')
    stocks = []
    # driver.find_element_by_partial_link_text("下一页").click()
    for page2 in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        soup = BeautifulSoup(driver.page_source,'html.parser')
        heads = ['Order', 'symbol', 'name', 'price', 'change', 'handover', 'money_in', 'money_out', 'money_net',
                 'money_amount','money_big']


        for table in soup.select('div[class="page-table"]'):
            for row in table.find_all('tr')[1:]:
                i = 0
                stock = {}
                for columns in row.find_all('td'):
                    if(re.search(r'\d亿',columns.string.strip())):
                        stock[heads[i]] = float(columns.string.strip().replace('亿','')) * 10000
                    elif(re.search(r'\d万',columns.string.strip())):
                        stock[heads[i]] = float(columns.string.strip().replace('万', ''))
                    else:
                        stock[heads[i]]=(columns.string.strip())
                    i += 1
                stocks.append(stock)
        driver.find_element_by_link_text(u"下一页").click()
        time.sleep(3)
    return stocks

if __name__ == "__main__":
    stk_money_flow = get_jqk_money_fllow()
    heads = ['Order', 'symbol', 'name', 'price', 'change', 'handover', 'money_in', 'money_out', 'money_net',
             'money_amount', 'money_big']
    df = pd.DataFrame(stk_money_flow,columns=heads)
    df.set_index(df['symbol'],inplace=True)
    df['money_net'].astype('float',inplace=True)
    print(df.sort_values(['money_net'],ascending=False))



    # heads = ['Order','symbol','name','price','change','handover','money_in','money_out','money_net','money_amount'
    #              ,'money_big']
    # for head in heads:
    #     print("%-12s" % head, end="")
    # for table in soup.select('div[class="page-table"]'):
    #     for row in table.find_all('tr'):
    #         for columns in row.find_all('td'):
    #             print("%-12s" % columns.string.strip().replace('万','W').replace('亿','Y'),end="")
    #         print("")
