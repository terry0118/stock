import requests
import pandas as pd
import time
import demjson
import random




def stk_zlzj(url):
    requests.adapters.DEFAULT_RETRIES = 5
    r = requests.get(url)
    txt = demjson.decode(r.text)
    df = pd.DataFrame(txt)
    r.close()
    return df


if __name__ == "__main__":
    url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_ssggzj?page=1&num=20&sort=r0_ratio&asc=0&bankuai=&shichang='
    sinaurl = 'http://finance.sina.com.cn/realstock/company/'
    proxy = {'http': 'http://wsg.cmszmail.ad:8083'}
    stks = set()
    pd.set_option('expand_frame_repr',True)
    while True:
        df = stk_zlzj(url=url)
        df2 = df.loc[0:10,['name','symbol','trade',"changeratio","netamount","r0_ratio"]]
        for stk  in df2.symbol:
            if stk not in stks:
                stks.add(stk)
                # print(["name", "trade", "turnover", "netamount"])
                print(df2[df2["symbol"] == stk])
                print(sinaurl + stk + '/nc.shtml' )
        print("wainting ...")
        time.sleep(random.randint(60,120))



#http://finance.sina.com.cn/realstock/company/sh600666/nc.shtml
