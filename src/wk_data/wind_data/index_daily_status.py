from functools import lru_cache

import pandas as pd

from wk_data.data_source import DataSource
from wk_data.mappings import register_get, register_update, register_get_no_arg

from wk_data.data_shard import CodeShardData

DATASET_NAME = 'index_market'

# @register_get('index_market')
# @lru_cache(maxsize=1)
# def get_index_daily_status(begin_date, end_date=None, instrument=None, check_calendar=False):
#     """
#     check_calendar: 是否根据交易日历校正结束日期
#     """
#     ds = DataSource()
#     if check_calendar:
#         trade_calender = ds.get_trade_calendar(begin_date, end_date)
#         if len(trade_calender) > 0:
#             end_date = trade_calender[-1]
#     price_data = ds.get_index_data(instrument=instrument, begin_date=begin_date, end_date=end_date)
#     return price_data


def prepare_index_daily(fetch=False):
    code_list = ["000001.SH", "000016.SH", "000300.SH", "399905.SZ", "000852.SH", "399006.SZ", "000906.SH", "932000.CSI"]
    fields = [
        "trade_dt", "s_info_windcode", "s_dq_open", "s_dq_close",
        "s_dq_low", "s_dq_high", "s_dq_volume",
        "s_dq_amount"
    ]

    def process_index_data(full_data):
        mapping = {
            'trade_dt': 'trade_dt', 's_info_windcode': 'windcode',
            's_dq_open': 'open', 's_dq_close': 'close', 's_dq_low': 'low', 's_dq_high': 'high',
            's_dq_volume': 'volume', 's_dq_amount': 'amount'
        }
        full_data.rename(columns=mapping, inplace=True)
        # selected_data = full_data.sort_values(by=['windcode', 'trade_dt'], ascending=True)
        return full_data

    ds = CodeShardData(code_list=code_list, table_name='AINDEXEODPRICES', data_name='index_daily',
                       fields=fields, fetch=fetch, proc_func=process_index_data, file_type='pkl')
    return ds


def prepare_wind_index_daily(fetch=False):
    code_list = ["881001.WI"]
    fields = [
        "trade_dt", "s_info_windcode", "s_dq_open", "s_dq_close",
        "s_dq_low", "s_dq_high", "s_dq_volume",
        "s_dq_amount"
    ]

    def process_index_data(full_data):
        mapping = {
            'trade_dt': 'trade_dt', 's_info_windcode': 'windcode',
            's_dq_open': 'open', 's_dq_close': 'close', 's_dq_low': 'low', 's_dq_high': 'high',
            's_dq_volume': 'volume', 's_dq_amount': 'amount'
        }
        full_data.rename(columns=mapping, inplace=True)
        # selected_data = full_data.sort_values(by=['windcode', 'trade_dt'], ascending=True)
        return full_data

    ds = CodeShardData(code_list=code_list, table_name='AIndexWindIndustriesEOD', data_name='index_daily',
                       fields=fields, fetch=fetch, proc_func=process_index_data, file_type='pkl')
    return ds


@register_get(DATASET_NAME)
def fetch_index_daily_status(begin_date=None, end_date=None, *, instrument=None):
    ds = prepare_index_daily()
    ds_wind = prepare_wind_index_daily()
    if instrument is None:
        data = ds.fetch_data(begin_date, end_date, instrument=instrument)
        data_part = ds.fetch_data(begin_date, end_date, instrument="399905.SZ").copy()
        data_part['windcode'] = '000905.SH'
        data_part2 = ds_wind.fetch_data(begin_date, end_date, instrument=instrument)
        data = pd.concat([data, data_part, data_part2])
    else:
        if instrument == '000905.SH':
            data = ds.fetch_data(begin_date, end_date, instrument="399905.SZ").copy()
            data['windcode'] = '000905.SH'
        elif instrument == '881001.WI':
            data = ds_wind.fetch_data(begin_date, end_date, instrument=instrument).copy()
        else:
            data = ds.fetch_data(begin_date, end_date, instrument=instrument).copy()

    data = data.sort_values(by=['windcode', 'trade_dt'], ascending=True)
    return data


@register_update(DATASET_NAME)
def update_index_daily_status(**kwargs):
    ds1 = prepare_index_daily(fetch=True)
    ds2 = prepare_wind_index_daily(fetch=True)
    file_list = ds1.update(**kwargs) + ds2.update(**kwargs)
    return file_list


@register_get_no_arg('index_name_mapping')
def get_index_name_mapping():
    index_name_mapping = {
        "000001.SH": "上证综指",
        "000016.SH": "上证50",
        "000300.SH": "沪深300",
        "399905.SZ": "中证500",
        "000906.SH": "中证800",
        "000852.SH": "中证1000",
        "399006.SZ": "创业板指",
        "932000.CSI": "中证2000"
    }
    return index_name_mapping

