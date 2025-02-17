from functools import lru_cache

import pandas as pd

import wk_data
from wk_data.constants import ExtStatus
from wk_data.mappings import register_get, register_update

from wk_data.data_shard import CodeShardData
from wk_util.data import filter_data
from wk_util.configuration import Configuration

DATASET_NAME = 'dummy_data'


def cash(begin_date, end_date):
    data = pd.DataFrame({
        'trade_dt': wk_data.get('trade_calendar', begin_date=begin_date,  end_date=end_date)
    })
    data['windcode'] = '000000.DM'
    data['open'] = 1
    data['high'] = 1
    data['low'] = 1
    data['close'] = 1
    data['volume'] = 1e20
    data['amount'] = 1e20
    data['name'] = '现金'

    return data


@register_get(DATASET_NAME)
def fetch_dummy_data(begin_date, end_date):
    return cash(begin_date, end_date)

