import requests
from datetime import datetime
import demjson
import json
import pandas as pd



# url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillSum?num=6000&sort=ticktime&asc=0&volume=40000&amount=1000000&type=0&day=2018-06-11'

def get_dadan_sina(amount,proxy):
    # vol *= 100
    amount *= 10000
    dt = datetime.date(datetime.today())
    print(dt)
    url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillSum?' \
          'num=5000&sort=ticktime&asc=0&type=0' + '&volume=' + str(0) + '&amount=' + str(amount) + '&day=' + str(dt)
    print(url)

    r = requests.get(url,proxies=proxy)
    # print(r.text)
    txt = r.text.replace('symbol', '\"symbol\"').replace('name', '\"name\"').replace('opendate', '\"opendate\"') \
        .replace('minvol', '\"minvol\"').replace('voltype', '\"voltype\"').replace('totalvol:', '\"totalvol\":') \
        .replace('totalvolpct', '\"totalvolpct"').replace('totalamt:', '\"totalamt\":').replace('totalamtpct',
                                                                                                '\"totalamtpct\"') \
        .replace('avgprice', '\"avgprice\"').replace('kuvolume', '\"kuvolume\"').replace('kuamount', '\"kuamount\"') \
        .replace('kevolume', '\"kevolume\"').replace('keamount', '\"keamount\"').replace('kdvolume',
                                                                                         '\"kdvolume\"').replace(
        'kdamount', '\"kdamount\"') \
        .replace('stockvol', '\"stockvol\"').replace('stockamt', '\"stockamt\"')
    # print(txt)
    return pd.DataFrame(json.loads(txt),dtype=float)


if __name__ == '__main__':
    proxy = {'http': 'http://wsg.cmszmail.ad:8083'}
    df = get_dadan_sina(50,proxy=proxy)
    df['buyratio'] = round(df['kuamount']/df['stockamt'],2)
    print(df[df['buyratio'] < 1].loc[:,['buyratio','symbol','kuamount','kdamount','keamount']].sort_values(['buyratio','kuamount'],ascending=False))
    # print(df.sort_values(['kuamount'],ascending=False).loc[:,['symbol','kuamount']].head(10))
    # print(df.loc[:10])
