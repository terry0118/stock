import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
from tushare.util import dateu as du
import threading
import time
import TsExt as te
from util.util import last_tddate_delta


if __name__ == '__main__':


    engine = create_engine('mysql+pymysql://root:password@127.0.0.1/tushare?charset=utf8')

    td = du.today()
    if(du.is_holiday(td) | int(du.get_hour()) < 16):
        dt = du.last_tddate()
    else:
        dt = td
    print(dt)


    """数据表
    stk_lhb_broker_detail   龙虎榜股票营业部买入详细情况
    stk_k_line_data         股票K线数据
    """

    """
    获取股票基本信息
    """

    df = te.stk_get_basics(engine)
    pd.io.sql.execute('truncate table stk_basics;', engine)
    df.to_sql('stk_basics', engine, if_exists='append')
    print("\n ====== End ===============")

    """
    获取股票龙虎榜机构买卖详细
    """
    for stk in set(ts.top_list()['code']):
        table = 'stk_lhb_broker_detail'
        sql_clear_broker_detail = 'delete from ' + table + ' where symbol = ' + stk + ' and date = \'' + dt + '\';'
        # print(sql_clear_broker_detail)
        print(stk)
        te.stk_clear_table(engine=engine, sql=sql_clear_broker_detail)
        try:
            df = te.stk_lhb_broker_detail(code=stk,date=dt)
            df.to_sql(table, engine, if_exists='append')
        except AttributeError as e:
            print(e)

    #

    """
    获取每日K线数据
    """

    def stk_hist_data_loops(code):
        stk = code
        dt = '2017-05-01'
        table = 'stk_hist_data'
        sql_clear_kline_table = 'delete from ' + table + ' where code=\'' + str(stk) + '\' and date >= ' + '\'' + dt + '\'' + ';'
        # print(sql_clear_kline_table)
        te.stk_clear_table(engine=engine, sql=sql_clear_kline_table)
        try:
            df = te.stk_get_hist_data(code=stk,start=dt)
            df.to_sql(table,engine,if_exists='append')
        except AttributeError as e:
            print("获取K线数据失败：" + code)


    for stk in ts.get_stock_basics().index:
        if(int(stk) < 603000):
            t = threading.Thread(target=stk_hist_data_loops, args=(stk,))
            t.start()
            if (threading.activeCount() > 10):
                time.sleep(5)

    """
    获取股票前三交易价位信息
    """
    for stk in ts.get_stock_basics().index:
        print("获取交易价位：" + stk)

        ticks = te.stk_get_hist_ticks(code=stk, date=dt)
        te.stk_clear_table(engine=engine,
                           sql='delete from stk_hist_ticks where code=' + '\'' + stk + '\'' + ' and date =' + '\'' + dt + '\';')
        try:
            df = pd.DataFrame(ticks)
            df.to_sql('stk_hist_ticks', engine, if_exists='append')
        except Exception as e:
            print(e)
