import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import threading
import time
import TsExt as te
from util.util import last_tddate_delta

if __name__ == '__main__':

    engine = create_engine('mysql+pymysql://root:password@127.0.0.1/tushare?charset=utf8')
    te.stk_clear_table(engine=engine,sql='truncate table stk_broker_holds;')
    for stk in ts.get_stock_basics().index:
        df = te.stk_broker_holds(stk,'20181')
        df.to_sql('stk_broker_holds',engine,if_exists='append')
