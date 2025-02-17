
from wk_data.data_shard import YearlyShardData
from wk_util.configuration import Configuration
from wk_data.proc_util import process_price_data_procedure
from wk_data.mappings import register_get, register_update
from wk_util.logger import console_log
from wk_db.funcs import read_sql
from wk_data.data.trade_calendar import get_trade_calendar
from wk_data.data.mr_data import get_merger_reorganization_data

DATASET_NAME = 'a_share_market'


def get_price_data(begin_date, end_date):
    config = Configuration()

    fields = [
        "trade_dt", "s_info_windcode",
        "s_info_name", "s_info_listdate", "s_info_delistdate", "s_info_st",
        "s_info_citics1_name", "s_info_suspension",
        "s_info_limit", "s_dq_preclose", "s_dq_open", "s_dq_close",
        "s_dq_low", "s_dq_high", "s_dq_volume",
        "s_dq_amount", "s_dq_adjfactor"
    ]


    console_log(f"fetching data from {begin_date} to {end_date}")
    sql = f"select {', '.join(fields)} from asharedailystatus where trade_dt >= {begin_date} and trade_dt <= {end_date}"

    data = read_sql(sql, db_loc=config.data_src)
    return data


def process_daily_data(prev_data, data, year):
    begin_date = data['trade_dt'].min()
    end_date = data['trade_dt'].max()
    trade_calendar = get_trade_calendar(begin_date, end_date)
    mr_data = get_merger_reorganization_data()

    if prev_data is None:
        data = process_price_data_procedure(data, previous_data=None, trade_calendar=trade_calendar,
                                            mr_map=mr_data, using_parallel=False)
    else:
        data = process_price_data_procedure(data, prev_data, trade_calendar, mr_data, using_parallel=False)

    return data


def prepare_a_share_market(fetch=False):

    ds = YearlyShardData("asharedailystatus", data_name=DATASET_NAME, fetch_func=get_price_data,
                         fetch=fetch, proc_func=process_daily_data, use_prev_data=True, file_type='pkl')
    return ds


@register_update(DATASET_NAME)
def update_a_share_market(**kwargs):
    ds = prepare_a_share_market(fetch=True)
    return ds.update(**kwargs)


@register_get(DATASET_NAME)
def fetch_a_share_market(begin_date=None, end_date=None, check_calendar=False):
    if check_calendar:
        trade_calender = get_trade_calendar(begin_date, end_date)
        if len(trade_calender) > 0:
            end_date = trade_calender[-1]
    ds = prepare_a_share_market()
    return ds.fetch_data(begin_date, end_date)