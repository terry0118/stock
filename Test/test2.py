import tushare as ts
import talib as ta
from tushare.util import dateu as du
import pandas as pd
from datetime import date,timedelta

# df = ts.get_hist_data('002024')
# df2 = df.sort_index()
# ma50 = ta.MA(df2['close'],timeperiod=50,matype=0)
# v_ma50 = ta.MA(df2['volume'],timeperiod=50,matype=0)
# df2['ma50'] = round(ma50,2)
# df2['vma50'] = round(v_ma50,2)
# df2 = df2.fillna(0)
# print(df2)

tod = date.today()
yed = tod + timedelta(days=-1)

df = ts.get_today_all()
df.set_index(['code'],inplace=True)
df['last_vol']='NaN'
# print(df.dtype)

for stkcode in ts.get_stock_basics().index:
    try:
        print("############" + stkcode + "###################")
        df2 = ts.get_hist_data(stkcode,start=str(yed),end=str(yed))
        df.at[stkcode,'last_vol'] = df2.at[du.last_tddate(),'volume']
        # print(df.at[stkcode,'volume'],df.at[stkcode,'last_vol'])
        if((df.at[stkcode,'volume'] / df.at[stkcode,'last_vol']) > 2 and  df.at[stkcode,'changepercent'] > 0):
            print(df.loc[stkcode])
    except Exception as e:
        print(e)
