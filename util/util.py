import datetime

from tushare.util import dateu as du


def last_tddate_delta(t):
    tt = du.last_tddate()
    tt2 = datetime.datetime.strptime(tt, '%Y-%m-%d').date()
    tt_delta = tt2 + datetime.timedelta(days=t)
    return tt_delta


