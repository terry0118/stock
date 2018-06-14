import tushare as ts
import talib as ta

df = ts.get_hist_data('002024')
df2 = df.sort_index()
ma50 = ta.MA(df2['close'],timeperiod=50,matype=0)
v_ma50 = ta.MA(df2['volume'],timeperiod=50,matype=0)
df2['ma50'] = round(ma50,2)
df2['vma50'] = round(v_ma50,2)
df2 = df2.fillna(0)
print(df2)
