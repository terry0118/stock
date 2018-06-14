import tushare as ts
import TsExt as te
from datetime import date
from sqlalchemy import create_engine
from tushare.util import dateu as du
import pandas as pd


if __name__ == '__main__':
    engine = create_engine('mysql+pymysql://root:password@127.0.0.1/tushare?charset=utf8')
    if date.today() == ts.util.dateu.last_tddate():
        for code in ts.get_stock_basics().index:
            tt = te.stk_get_today_ticks(code)
            print(code + tt)

    df = te.stk_get_today_all(engine=engine)
    pd.io.sql.execute('delete from stk_today_all where date = \'' + du.last_tddate() + '\'',engine)
    df.to_sql('stk_today_all',engine,if_exists='append')


