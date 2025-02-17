import datetime
import pathlib
from datetime import timedelta

import pandas as pd

from wk_platform import __version__
from wk_platform.backtest import strategyOutput
from wk_platform.config import HedgeStrategyConfiguration

from wk_platform.backtest.result import BackTestResult, BackTestResultSet
from wk_util.logger import console_log
from wk_util.file_digest import md5


class BatchExecutor:
    """
    批量执行权重回测的策略
    """
    def __init__(self, strategy_cls, files, begin_date, end_date=None, config=HedgeStrategyConfiguration(), max_process=1):
        console_log('platform version:', __version__)
        self.__strategy_cls = strategy_cls.strategy_class()
        console_log('using strategy', self.__strategy_cls.strategy_name())

        self.__result_set = [None for f in files]
        self.__files = []
        for file in files:
            if isinstance(file, pathlib.Path):
                self.__files.append(file)
            else:
                self.__files.append(pathlib.Path(file))
        self.__weights = {}
        self.__signs = {}
        self.__begin_date = begin_date
        self.__end_date = end_date
        if self.__end_date is None:
            self.__end_date = (datetime.datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
        self.__result_set = BackTestResultSet()
        self.__config = config
        console_log("backtest range:", self.__begin_date, self.__end_date)
        self.__prepare_weight_df()

    def __prepare_weight_df(self):
        for file in self.__files:
            k = file.stem
            self.__weights[k] = pd.read_csv(file, encoding='gbk').sort_values(['date', 'windcode'])
            self.__signs[k] = md5(file)

    def __get_instruments(self):
        begin_date, end_date = self.__begin_date, self.__end_date
        instruments = []
        for k, weight_df in self.__weights.items():
            weight_df['date'] = pd.to_datetime(weight_df['date'], format='%Y%m%d').apply(lambda x: x.strftime('%Y%m%d'))
            idx = (weight_df['date'] >= begin_date) & (weight_df['date'] <= end_date)
            weight_df = weight_df[idx]
            instruments += weight_df['windcode'].unique().tolist()
        instruments = list(set(instruments))
        return instruments

    def run(self):
        begin_date, end_date = self.__begin_date, self.__end_date
        feed, ext_status_df, mr_map = self.__strategy_cls.prepare_feed(begin_date, end_date,
                                                                       self.__get_instruments(),
                                                                       self.__config)

        for name, weight_df in self.__weights.items():
            datVal = weight_df

            datVal['date'] = pd.to_datetime(datVal['date'], format='%Y%m%d')
            datVal['date'] = [datetime.datetime.strftime(x, '%Y%m%d') for x in datVal['date']]
            datVal = datVal[(datVal['date'] >= begin_date) & (datVal['date'] <= end_date)]

            """
            设定起始日期为首行日期
            """
            try:
                tmp = datVal['date']
                begin_date = tmp.iloc[0]
                weight_strategy = self.__strategy_cls(feed, datVal, begin_date, end_date, self.__config,
                                                      ext_status_data=ext_status_df.to_dict(orient="records"),
                                                      mr_map=mr_map, sign=self.__signs[name])
                output = strategyOutput.StrategyOutput(weight_strategy, begin_date, end_date, config=self.__config)
                output.pre_process()
                weight_strategy.run()
                output.bench_process()
                output.post_process()
                self.__result_set[name] = output.result
            except Exception as e:
                console_log('in weight data', name, 'exception raised', e)
            finally:
                feed.reset()

    @property
    def result_set(self) -> BackTestResultSet:
        return self.__result_set
