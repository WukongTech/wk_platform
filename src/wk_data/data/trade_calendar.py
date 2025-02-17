from functools import lru_cache

import pandas as pd

from wk_data import DataSource
from wk_data.mappings import register_get
from wk_data.wind_data.index_daily_status import prepare_index_daily
# from wk_data.local_data.ame_index_daily import prepare_ame_index
from wk_platform.config import CalendarType


@register_get('trade_calendar')
def get_trade_calendar(begin_date, end_date=None, calendar=CalendarType.A_SHARE_MARKET):
    ds = prepare_index_daily()
    data: pd.DataFrame = ds.fetch_data(begin_date=begin_date, end_date=end_date, instrument='000001.SH')
    data.sort_values('trade_dt', inplace=True)
    trade_calendar = data['trade_dt'].tolist()
    if calendar == CalendarType.A_SHARE_MARKET:

        return trade_calendar
    else:
        assert False
        # ame_data = prepare_ame_index(begin_date, end_date, instrument='SPX.GI')
        # ame_trade_calendar = ame_data['trade_dt'].tolist()

        # full_calendar = list(set(ame_trade_calendar + trade_calendar))
        # return sorted(full_calendar)


