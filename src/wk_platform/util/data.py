from collections import deque

import pathlib
from typing import List

import numpy as np
import pandas as pd
import datetime

from datetime import timedelta
from collections import OrderedDict

from dataclasses import dataclass


from wk_util.tqdm import tqdm
from wk_platform import __version__
from wk_platform.backtest import strategyOutput
from wk_platform.config import StrategyConfiguration

# from wk_platform.feed.mixed_feed import StockIndexSynthETFFeed

from wk_data.data_source import DataSource
from wk_platform.backtest.result import BackTestResult
from wk_data.constants import ExtStatus
from wk_util.logger import console_log
from wk_util.file_digest import md5
from wk_data.constants import SuspensionType
# from wk_platform.contrib.strategy.weight_strategy import WeightStrategyBase
from wk_platform.strategy.low_frequency_strategy import LowFreqBacktestingStrategy
from wk_platform.feed.bar import SynthIndexETFBar
import wk_db
import wk_data


def filter_market_data(data, instruments, with_mr=True):
    """
    过滤市场行情信息，仅保留instruments包含的标的
    """

    if not with_mr:
        if instruments is not None:
            instruments = pd.Series(instruments).unique()
            data = pd.DataFrame({"windcode": instruments}).merge(data, on="windcode")
        else:
            return data

    mr_map = wk_data.get("mr_data")
    if instruments is not None:
        ext_instrument = []
        for inst in instruments:
            try:
                mr_record = mr_map[inst]
                ext_instrument.append(mr_record.new_windcode)
            except KeyError:
                continue
        instruments = instruments + ext_instrument
        instruments = pd.Series(instruments).unique()
        data = pd.DataFrame({"windcode": instruments}).merge(data, on="windcode")
    return data


def align_calendar(data, calendar: List):
    calendar = pd.DataFrame({'trade_dt': calendar})

    # def fill_untradable_day(df):
    #     if df['volume'] == 0:
    #         df['open'] = df['close']
    #         df['high'] = df['close']
    #         df['low'] = df['close']
    #     return df

    def process_func(df):
        df = df.sort_values('trade_dt')
        start_dt = df['trade_dt'].min()
        try:
            if df.tail(1)['ext_status'].tolist()[0] != ExtStatus.NORMAL.value:
                end_dt = df['trade_dt'].max()
            else:
                end_dt = calendar['trade_dt'].max()
            local_calendar = calendar[(calendar['trade_dt'] >= start_dt) & (calendar['trade_dt'] <= end_dt)]
            df = df.merge(local_calendar, on='trade_dt', how='right')
            df['ext_status'] = df['ext_status'].fillna(ExtStatus.UNTRADABLE_FILLED_BAR.value)
            df = df.ffill()
        except KeyError:
            local_calendar = calendar[calendar['trade_dt'] >= start_dt]
            df = df.merge(local_calendar, on='trade_dt', how='right')
            df = df.ffill()
        return df

    data = data.groupby('windcode', group_keys=False).apply(process_func)
    return data


def add_normal_ext_status(data):
    data['ext_status'] = ExtStatus.NORMAL.value
    return data