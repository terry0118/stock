import talib as ta
import tushare as ts
import numpy as np

stkcodes = ts.get_stock_basics()

for code in stkcodes.index:
    # print("process code " + str(code))
    df = ts.get_hist_data(code=code, start='2015-01-01')
    df2 = df.sort_index()
    ma50 = ta.MA(df2['close'], timeperiod=50, matype=0)
    v_ma50 = ta.MA(df2['volume'], timeperiod=50, matype=0)
    diff, dea, bar = ta.MACD(df2['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    df2['ma50'] = round(ma50, 2)
    df2['v_ma50'] = round(v_ma50, 2)
    df2 = df2.fillna(0)
    df_length = len(df2)
    df2['diff'] = round(diff, 2)
    df2['dea'] = round(dea, 2)
    df2['bar'] = round(bar, 2)
    day1 = df_length - 1
    day2 = df_length - 2
    day3 = df_length - 9
    day4 = df_length - 7
    # print(df2.iloc[day1])
    # print(df.iloc[day1]['ma5'],df.iloc[day1]['ma20'])
    # print(np.std([df2.iloc[day1]['ma5'], df2.iloc[day1]['ma20']]))
    if (
            df2.iloc[day1]['bar'] > 0
            and df2.iloc[day1]['ma5'] >= df2.iloc[day2]['ma5']
            and (df2.iloc[day1]['bar'] / df2.iloc[day2]['bar']) > 2
            # and df2.iloc[day1]['ma5'] > df2.iloc[day2]['ma5']
            and df2.iloc[day1]['ma20'] > df2.iloc[day2]['ma20']
            #and df2.iloc[day1]['open'] < df2.iloc[day1]['ma5'] < df2.iloc[day1]['close']
            #and df2.iloc[day1]['open'] < df2.iloc[day1]['ma20'] < df2.iloc[day1]['close']
            #and df2.iloc[day1]['open'] < df2.iloc[day1]['ma50'] < df2.iloc[day1]['close']
            # and df2.iloc[day1]['ma5'] > df2.iloc[day1]['ma20'] > df2.iloc[day2]['ma20']
            and df2.iloc[day1]['close'] > df2.iloc[day1]['ma20'] > df2.iloc[day1]['ma50']
            # and np.std([df2.iloc[day1]['ma5'], df2.iloc[day1]['ma20']]) < 0.1
            # and df.iloc[day1]['ma50'] > df.iloc[day2]['ma50']
    ):
        print('+++++++++++++' + code + '++++++++++++++++++++++++++')
        # print(df2.iloc[day3:day4 + 1, 0:4])
        # print(df2.iloc[day4]['close'] - df2.iloc[day3]['open'])
