# -*- coding: UTF-8 -*-
from selenium import webdriver
import time
from bs4 import BeautifulSoup
import os

def get_jqk_money_fllow():
    option = webdriver.ChromeOptions()
    option.add_argument('headless')
    option.add_argument('--proxy-server=http://wsg.cmszmail.ad:8083')
    driver = webdriver.Chrome(chrome_options=option)
    driver.get('http://data.10jqka.com.cn/funds/ggzjl/###')
    soup = BeautifulSoup(driver.page_source,'html.parser')
    heads = ['Order','symbol','name','price','change','handover','money_in','money_out','money_net','money_amount'
             ,'money_big']
    for head in heads:
        print("%-12s" % head, end="")
    print("")
    for table in soup.select('div[class="page-table"]'):
        for row in table.find_all('tr'):
            for columns in row.find_all('td'):
                print("%-12s" % columns.string.strip().replace('万','W').replace('亿','Y'),end="")
            print("")

if __name__ == "__main__":
    while True:
        get_jqk_money_fllow()
        os.system('cls')
