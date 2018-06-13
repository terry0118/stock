import tushare as ts
import pandas as pd
import  numpy as np
import random
import time
import os
# stock = ['600360']
# df = ts.get_realtime_quotes(stock)
# # print(df[['name','open','high','b1_p','b1_v','a1_p','a1_v','code','time']])
# p = np.array(df[['name','open','high','b1_p','b1_v','a1_p','a1_v','code','time']])
# print( "%-20s%-20s%-20s%-20s%-20s%-20s%-20s%-20s%-20s" % tuple(p[0]))

def get_realtime_quotes(stock=''):
    try:
        st = stock
        df = ts.get_realtime_quotes(stock)
        p = np.array(df[['name','open','high','b1_p','b1_v','a1_p','a1_v','code','time']])
        return p
    except Exception as e :
        print("Get real time quotes failed:  e")

if __name__ == "__main__":
    def loop_dis():
        stock = ['600360','600303','002461','300726','300529']
        os.system('cls')
        print("%-20s%20s%20s%20s%20s%20s%20s%20s%20s" % ('name','open','high','b1_p','b1_v','a1_p','a1_v','code','time'))
        for st in stock:
            p = get_realtime_quotes(st)
            print("%-17s%20s%20s%20s%20s%20s%20s%20s%20s" % tuple(p[0]))
    while True:
        loop_dis()
        time.sleep(random.randint(1,5))
