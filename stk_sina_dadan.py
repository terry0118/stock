import requests
from datetime import datetime
import demjson
import json
import pandas as pd



# url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillSum?num=6000&sort=ticktime&asc=0&volume=40000&amount=1000000&type=0&day=2018-06-11'

def get_dadan_sina(amount,proxy):
    # vol *= 100
    amount *= 10000
    # dt = datetime.date(datetime.today())
    dt = '2018-06-21'
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
    return pd.DataFrame(json.loads(txt),dtype='float64')


if __name__ == '__main__':
    proxy = {'http': 'http://wsg.cmszmail.ad:8083'}
    pd.set_option('display.height', 1000)
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    df = get_dadan_sina(100,proxy)
    df['buyratio'] = round(df['kuamount']/df['stockamt'],2)
    df['sellratio'] = round(df['kdamount'] / df['stockamt'], 2)
    print(df[df['buyratio'] < 1].loc[:,['name','symbol','totalamtpct','buyratio','sellratio','kuamount','kdamount','keamount']].sort_values(['buyratio','totalamtpct'],ascending=False))
    # print(df.sort_values(['kuamount'],ascending=False).loc[:,['symbol','kuamount']].head(10))
    # print(df.loc[:10])
