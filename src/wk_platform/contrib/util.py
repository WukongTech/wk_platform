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





class PreprocessorSeq:
    def __init__(self, *args):
        self.__pre_proc_list = [*args]

    def add(self, *args):
        for f in args:
            self.__pre_proc_list.append(f)

    def __call__(self, data):
        for f in self.__pre_proc_list:
            data = f(data)
        return data


def check_weight_df(weight_df: pd.DataFrame, group_field='date', unique_field='windcode'):
    """
    检查同一时间是否存在重复的项
    """
    def check_group(df):
        return df[unique_field].unique().shape[0] == df.shape[0]

    result = weight_df.groupby(group_field).apply(check_group)
    for group_tag, value in result.items():
        if value is False:
            raise ValueError(f'duplicated instrument at {group_tag}')
    # return result


class StrategyWrapper:
    """
    根据权重列表定期调仓的回测策略
    """
    def __init__(self, strategy_cls):
        self.__strategy_cls = strategy_cls
        self.__weight_df = None
        self.__sign = None
        self.__result = BackTestResult()

    def __call__(self, weight, begin_date, end_date=None, config=StrategyConfiguration(), is_tag=False, **kwargs):
        """
        Parameters
        ----------
        weight: pd.DataFrame or str
            调仓权重列表

        special_case: pd.DatasFrame or str
            特殊处理列表，注意与weight保持一致的数据形式

        begin_date: str
            yyyymmdd 格式的日期字符串

        end_date: str
            yyyymmdd 格式的日期字符串

        config: StrategyConfiguration
            策略配置类
        """
        console_log('platform version:', __version__)
        if is_tag:
            data = wk_db.read_weight(weight)
            self.__weight_df = data.sort_values(['date', 'windcode'])
            self.__sign = None
        elif isinstance(weight, pd.DataFrame):
            self.__weight_df = weight.sort_values(['date', 'windcode'])
            self.__sign = None
        else:
            assert isinstance(weight, str) or isinstance(weight, pathlib.Path)
            self.__weight_df = pd.read_csv(weight, encoding="gbk").sort_values(['date', 'windcode'])
            self.__sign = md5(weight)

        self.__begin_date = begin_date
        self.__end_date = end_date
        if self.__end_date is None:
            self.__end_date = (datetime.datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
        self.__result = None
        self.__config = config
        console_log("backtest range:", self.__begin_date, self.__end_date)
        self.__kwargs = kwargs

    def __prepare_feed(self):
        weight_df = self.__weight_df
        instruments = weight_df['windcode'].unique().tolist()
        self.__feed, self.__ext_status_df, self.__mr_map = self.__strategy_cls.prepare_feed(
            self.__begin_date, self.__end_date,
            instruments=instruments, config=self.__config
        )

    def run(self):
        self.__prepare_feed()
        begin_date, end_date = self.__begin_date, self.__end_date
        dat_val = self.__weight_df

        dat_val['date'] = pd.to_datetime(dat_val['date'], format='%Y%m%d')
        dat_val['date'] = [datetime.datetime.strftime(x, '%Y%m%d') for x in dat_val['date']]
        dat_val = dat_val[(dat_val['date'] >= begin_date) & (dat_val['date'] <= end_date)]

        """
        设定起始日期为首行日期
        """
        tmp = dat_val['date']
        begin_date = tmp.iloc[0]
        weight_strategy = self.__strategy_cls(self.__feed, dat_val, begin_date, end_date, self.__config,
                                               ext_status_data=self.__ext_status_df.to_dict(orient="records"),
                                               sign=self.__sign, **self.__kwargs)

        output = strategyOutput.StrategyOutput(weight_strategy, begin_date, end_date, config=self.__config)
        output.pre_process()
        weight_strategy.run()
        output.bench_process()
        output.post_process()
        self.__result = output.result

    def strategy_class(self):
        return self.__strategy_cls

    @property
    def result(self) -> BackTestResult:
        return self.__result
