
from collections import deque

import pathlib

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

from wk_platform.feed.fast_feed import StockIndexSynthETFFeed

from wk_data.data_source import DataSource
from wk_platform.backtest.result import BackTestResult
from wk_data.constants import ExtStatus
from wk_util.logger import console_log
from wk_util.file_digest import md5
from wk_util.data import filter_data
from wk_platform.contrib.strategy.tracker_mixin import (
    TradeDayWinRatioRecord,
    TradeDayReturnRecord,
    TradeDayRecord,
    TradeDayTrackerMixin
)
from wk_platform.strategy.low_frequency_strategy import LowFreqBacktestingStrategy
from wk_platform.feed.bar import SynthIndexETFBar
from wk_data.constants import BENCH_INDEX
import wk_db
from wk_platform.contrib.util import check_weight_df
from wk_platform.util.data import  add_normal_ext_status


class WeightStrategyExBase(LowFreqBacktestingStrategy, TradeDayTrackerMixin):
    def __init__(self, feed, data, special_case_data, begin_date, end_date,
                 config=StrategyConfiguration(), ext_status_data=None,
                 sign=None):

        super().__init__(feed, begin_date, end_date, config=config, ext_status_data=ext_status_data, sign=sign)
        TradeDayTrackerMixin.__init__(self, self.getBroker(), config)

        self.__config = config

        # 持仓表
        self.__weight = data
        # 特殊处理
        self.__sp_case = special_case_data
        self.__prev_trade_date = ''

        """
        记录调仓日期
        """
        self.__buy_date = []
        self.__sp_date = deque()

        self.__trade_date = deque()

        self.__pbar = None

    def get_trade_date(self):
        return self.__buy_date

    def __change_position(self, bars):
        """
        按照权重调仓
        """
        weight_df = self.__weight[self.__weight["windcode"].isin(bars.getInstruments())]
        date_str = bars.getDateTime().strftime("%Y%m%d")
        weight_df = weight_df[weight_df['date'] == date_str]
        weight_df.set_index('date', inplace=True)

        """
        获取了当期要买入的股票列表和权重列表
        按照权重从大到小的顺序买入，先买优先股
        """
        weight_df = weight_df.sort_values(by='weight', ascending=False)
        total_weight = weight_df['weight'].sum()
        if total_weight > self.__config.max_position:
            factor = self.__config.max_position / total_weight
            weight_df['weight'] = weight_df['weight'] * factor

        # 对于权重小于0.01 的股票做丢弃处理
        # weightPerTrade = weightPerTrade[weightPerTrade['weight'] >= 0.01]
        windcode_list = weight_df['windcode'].tolist()
        weight_list = weight_df['weight'].tolist()

        target_position = OrderedDict()
        for inst, weight in zip(windcode_list, weight_list):
            target_position[inst] = weight

        self.getBroker().simplify_position()

        if self.__prev_trade_date != '':
            win_ratio_dict = self.calc_win_ratio(bars)
            self.custom_analyzer.track('调仓日胜率',
                                       date_range=self.__prev_trade_date + '-' + date_str,
                                       **win_ratio_dict)

            ret_dict = self.calc_ret(bars)
            self.custom_analyzer.track('调仓日收益率',
                                       date_range=self.__prev_trade_date + '-' + date_str,
                                       **ret_dict)

        shares_value = self.getBroker().getSharesValue()

        # 此处依照先卖后买的基本顺序
        # 清仓不在目标持仓中的股票
        if len(self.getBroker().getPositions()) > 0:
            for inst in list(self.getBroker().getPositions().keys()):
                try:
                    target_position[inst]
                except KeyError:
                    shares = self.getBroker().getShares(inst)
                    if shares > 0:
                        # print 'sell out %s'%(inst)
                        self.enterShort(bars, inst, shares, False, False)

        """
        先对要减仓的进行处理
        """
        for (inst, weight) in zip(windcode_list, weight_list):

            # 取得该股票的旧权重
            # TODO: 在持仓价值为0时的处理
            weight_old = self.getBroker().getSharesAmount(inst, bars) / self.getBroker().get_total_equity()
            if weight_old >= weight:
                self.enterLongShortWeight(bars, inst, weight, False, False)

        """
        最后对要加仓的进行处理
        """
        for (inst, weight) in zip(windcode_list, weight_list):
            # print inst, weight
            """
            取得该股票的旧权重
            """
            if bars[inst].ext_status != ExtStatus.NORMAL:
                continue
            weight_old = self.getBroker().getSharesAmount(inst, bars) / self.getBroker().get_total_equity()
            if weight_old < weight:
                self.enterLongShortWeight(bars, inst, weight, False, False)

        total_amount = self.getBroker().long_amount + self.getBroker().short_amount

        turnover_rate = (total_amount / shares_value) if shares_value != 0 else 0

        capacity = self.calc_capacity(bars, windcode_list)

        self.refresh_return_data(bars)
        self.refresh_ref_price(bars)
        self.custom_analyzer.track('调仓日指标',
                                   date_str=date_str,
                                   turnover_rate=turnover_rate,
                                   capacity=capacity * 1000 / 1e8)

    def mr_hook(self, original_code, new_code, ratio):
        self.trade_day_tracker_mr_hook(original_code, new_code, ratio, self.current_date_str)

    def handle_special_case(self, bars):
        """
        处理当日所有特殊情况
        """
        sp_df = self.__sp_case
        sp_df = sp_df[sp_df['date'] == self.current_date_str]

        windcode_list = sp_df['windcode'].tolist()
        target_list = sp_df['target'].tolist()

        for inst, target_inst in zip(windcode_list, target_list):
            weight = self.getBroker().getSharesAmount(inst, bars) / self.getBroker().get_total_equity()
            self.enterLongShortWeight(bars, inst, 0)
            if target_inst in BENCH_INDEX:
                target_inst = SynthIndexETFBar.synth_name(target_inst)
            self.enterLongShortWeight(bars, target_inst, weight)

            # 计算胜率时剔除标的，因为有可能不同股票换为同一标的
            self.ref_price_ignore(target_inst)


    def on_start(self):
        """
        策略开始运行时执行
        """
        # 初始化自定义追踪指标
        self.custom_analyzer.add_tracker('调仓日胜率', TradeDayWinRatioRecord)
        self.custom_analyzer.add_tracker('调仓日收益率', TradeDayReturnRecord)
        self.custom_analyzer.add_tracker('调仓日指标', TradeDayRecord)

        # 获取调仓日期
        self.__buy_date = sorted(list(set(self.__weight['date'])))
        self.__trade_date = deque(self.__buy_date)

        # 特殊处理日期
        self.__sp_date = deque(sorted(list(set(self.__sp_case['date']))))

        console_log('strategy trade date is:')
        if len(self.__buy_date) > 5:
            console_log(f"{self.__buy_date[0]}, {self.__buy_date[1]}, ..., {self.__buy_date[-1]}")
        else:
            console_log(self.__buy_date)
        console_log('total trade days:', len(self.__buy_date))
        if self.__config.progress_bar:
            self.__pbar = tqdm(total=len(self.__buy_date), disable=(not self.__config.progress_bar))

    def on_finish(self, bars):
        self.getBroker().simplify_position()
        if self.__prev_trade_date != self.current_date_str:
            win_ratio_dict = self.calc_win_ratio(bars)
            self.custom_analyzer.track('调仓日胜率',
                                       date_range=self.__prev_trade_date + '-' + self.current_date_str,
                                       **win_ratio_dict)

            ret_dict = self.calc_ret(bars)
            self.custom_analyzer.track('调仓日收益率',
                                       date_range=self.__prev_trade_date + '-' + self.current_date_str,
                                       **ret_dict)

        if self.__pbar:
            self.__pbar.close()

    def on_bars(self, bars):
        """
        每天的数据流到来时触发一次
        """
        while len(self.__trade_date) > 0 and self.current_date_str > self.__trade_date[0]:  # 处理非交易日调仓的情况
            # TODO: 日志中增加警告
            self.__trade_date.popleft()
            if self.__pbar:
                self.__pbar.update(1)

        if len(self.__sp_date) > 0 and self.current_date_str == self.__sp_date[0]:
            self.handle_special_case(bars)
            self.__sp_date.popleft()

        if len(self.__trade_date) > 0 and self.current_date_str == self.__trade_date[0]:
            self.__trade_date.popleft()
            # print(date_str)
            self.__change_position(bars)
            self.__prev_trade_date = self.current_date_str
            if self.__pbar:
                self.__pbar.update(1)

    @classmethod
    def prepare_feed(cls, begin_date, end_date, instruments, config):
        ds = DataSource()
        mr_map = ds.get_mr_data()

        data = ds.get_daily(begin_date, end_date)

        ext_instrument = []
        for inst in instruments:
            try:
                mr_record = mr_map[inst]
                ext_instrument.append(mr_record.new_windcode)
            except KeyError:
                continue
        # instruments = instruments + ext_instrument
        instruments = instruments + ext_instrument
        data = pd.DataFrame({"windcode": instruments}).merge(data, on="windcode")
        ext_status_df = data[data['ext_status'] != ExtStatus.NORMAL.value].sort_values(by="trade_dt")

        console_log("preparing feed...")
        # 将读取出的股票按照windcode分组
        data = data[(data['trade_dt'] >= begin_date) & (data['trade_dt'] <= end_date)]

        end_date = data['trade_dt'].max()  # 强制对齐

        feed = StockIndexSynthETFFeed()
        feed.add_stock_bars(data)

        index_data = ds.get_index_data(begin_date=begin_date, end_date=end_date)
        index_data = add_normal_ext_status(index_data)
        feed.add_index_bars(index_data)
        feed.add_synth_index_etf_bars(index_data)

        feed.prefetch()

        return feed, ext_status_df, mr_map

    @classmethod
    def strategy_name(cls):
        return "WeightStrategyEx"


class WeightStrategyEx:
    """
    根据权重列表定期调仓的回测策略
    """
    def __init__(self, weight, special_case, begin_date, end_date=None, config=StrategyConfiguration(), is_tag=False):
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
            self.__sp_case = special_case.sort_values(['date', 'windcode'])
            self.__sign = None
        elif isinstance(weight, pd.DataFrame):
            self.__weight_df = weight.sort_values(['date', 'windcode'])
            self.__sp_case = special_case.sort_values(['date', 'windcode'])
            self.__sign = None
        else:
            assert isinstance(weight, str) or isinstance(weight, pathlib.Path)
            self.__weight_df = pd.read_csv(weight, encoding="gbk").sort_values(['date', 'windcode'])
            self.__sp_case = pd.read_csv(special_case, encoding="gbk").sort_values(['date', 'windcode'])
            self.__sign = md5(weight) + '-' + md5(special_case)

        check_weight_df(self.__weight_df)
        check_weight_df(self.__sp_case)

        self.__begin_date = begin_date
        self.__end_date = end_date
        if self.__end_date is None:
            self.__end_date = (datetime.datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
        self.__result = None
        self.__config = config
        console_log("backtest range:", self.__begin_date, self.__end_date)

    def __prepare_feed(self):
        weight_df = self.__weight_df
        instruments = weight_df['windcode'].unique().tolist()
        self.__feed, self.__ext_status_df, self.__mr_map = WeightStrategyExBase.prepare_feed(
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

        sp_case = self.__sp_case
        sp_case['date'] = pd.to_datetime(sp_case['date'], format='%Y%m%d')
        sp_case['date'] = [datetime.datetime.strftime(x, '%Y%m%d') for x in sp_case['date']]
        sp_case = filter_data(sp_case, begin_date, end_date, date_field_tag='date')

        """
        设定起始日期为首行日期
        """
        tmp = dat_val['date']
        begin_date = tmp.iloc[0]
        weight_strategy = WeightStrategyExBase(self.__feed, dat_val, sp_case, begin_date, end_date, self.__config,
                                               ext_status_data=self.__ext_status_df.to_dict(orient="records"),
                                               sign=self.__sign)

        output = strategyOutput.StrategyOutput(weight_strategy, begin_date, end_date, config=self.__config)
        output.pre_process()
        weight_strategy.run()
        output.bench_process()
        output.post_process()
        self.__result = output.result

    @classmethod
    def strategy_class(cls):
        return WeightStrategyExBase

    @property
    def result(self) -> BackTestResult:
        return self.__result
