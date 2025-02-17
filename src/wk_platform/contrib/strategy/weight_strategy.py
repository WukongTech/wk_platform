from __future__ import annotations

import inspect
import math

from collections import deque

import pathlib
import warnings
import numpy as np
import pandas as pd
import datetime

from datetime import timedelta
from collections import OrderedDict

from dataclasses import dataclass
from functools import partial

from wk_platform.broker.brokers.base_broker import Broker

from wk_util.data import filter_data

import wk_data

from wk_util.tqdm import tqdm

from wk_platform import __version__
from wk_platform.backtest import strategyOutput

from wk_platform.backtest.result import BackTestResult, BackTestResultSet
from wk_platform.contrib.strategy.tracker_mixin import (
    TradeDayWinRatioRecord,
    TradeDayReturnRecord,
    TradeDayRecord,
    TradeDayTrackerMixin
)
from wk_data.constants import ExtStatus
from wk_util.logger import console_log
from wk_util.file_digest import md5
from wk_platform.feed.bar import SynthIndexETFBar, PositionDummyBar
from wk_data.constants import BENCH_INDEX
from wk_platform.strategy.low_frequency_strategy import LowFreqBacktestingStrategy
import wk_db
from wk_platform.contrib.util import check_weight_df, PreprocessorSeq
from wk_platform.util.data import align_calendar, add_normal_ext_status, filter_market_data

from wk_platform.feed.mixed_feed import MixedFeed
from wk_platform.feed.parser import StockDataRowParser, FundDataRowParser, PositionDummyRowParser, IndexETFRowParser
from wk_platform.feed.parser import FutureDataRowParser
from wk_platform.feed.parser import IndexDataRowParser
from wk_platform.feed.parser import SyntheticIndexETFRowParser
from wk_platform.feed.parser import FundNavDataRowParser
# import wk_platform.util.data
from wk_platform.config import StrategyConfiguration, PriceType, DatasetType, CalendarType

from .weight_strategy_config import WeightStrategyConfiguration
from ...broker.brokers.extend_broker import BrokerV2
from ...feed.strategy_feed import FeedRegistry
from ...util.future import FutureUtil


class WeightStrategyBase(LowFreqBacktestingStrategy, TradeDayTrackerMixin):
    def __init__(self, feed, data, begin_date, end_date,
                 config: WeightStrategyConfiguration = WeightStrategyConfiguration(), ext_status_data=None,
                 tqdm_cls=tqdm,
                 broker_cls=Broker,
                 sign=None):
        """
        初始化策略父类，设定回测起始资金
        """
        super().__init__(feed, begin_date, end_date, config=config, ext_status_data=ext_status_data, sign=sign,
                         broker_cls=broker_cls)
        TradeDayTrackerMixin.__init__(self, self.getBroker(), config)

        self.__config: WeightStrategyConfiguration = config

        """
        记录股票权重数据
        """
        self.__weight = data

        """
        记录调仓日期
        """
        self.__buy_date = []

        self.__trade_date = deque()
        self.__ext_date = None

        self.__ext_status_data = deque(ext_status_data) if ext_status_data is not None else deque()

        self.__pbar = None
        self.__tqdm_cls = tqdm_cls

        self.__prev_trade_date = ''
        if broker_cls == BrokerV2:
            self.getBroker().register_hook(BrokerV2.Hook.MARGIN_CALL, self.margin_call_handler)
            # self.getBroker().register_hook(BrokerV2.Hook.MATURITY, self.transform_maturities)

    def get_trade_date(self):
        return self.__buy_date

    def close_long_position(self, bars, target_position):
        """
        清仓不在目标持仓表中的多头仓位
        """
        if len(self.getBroker().getPositions()) > 0:
            for inst in list(self.getBroker().getPositions().keys()):
                try:
                    target_position[inst]
                except KeyError:
                    shares = self.getBroker().getShares(inst)
                    if shares > 0:
                        # print 'sell out %s'%(inst)
                        self.enterShort(bars, inst, shares, False, False)

    def close_short_position(self, bars, target_position):
        """
        清仓不在目标持仓表中的空头仓位
        """
        if len(self.getBroker().getPositions()) > 0:
            for inst in list(self.getBroker().getPositions().keys()):
                try:
                    target_position[inst]
                except KeyError:
                    shares = self.getBroker().getShares(inst)
                    if shares < 0:
                        # print 'sell out %s'%(inst)
                        self.enterLong(bars, inst, shares, False, False)

    def reduce_long_position(self, bars, target_position):
        """
        若目标持仓小于原有持仓则减仓
        """
        for (inst, weight) in target_position.items():

            # 取得该股票的旧权重
            # TODO: 在持仓价值为0时的处理
            amount = self.getBroker().getSharesAmount(inst, bars)
            total_equity = self.getBroker().get_total_equity()
            weight_old = amount / total_equity
            if weight_old >= weight and weight_old > 0:
                self.enterLongShortWeight(bars, inst, weight, False, False)

    def reduce_short_position(self, bars, target_position):
        for (inst, weight) in target_position.items():

            # 取得该股票的旧权重
            # TODO: 在持仓价值为0时的处理
            amount = self.getBroker().getSharesAmount(inst, bars)
            total_equity = self.getBroker().get_total_equity()
            weight_old = amount / total_equity
            if weight_old <= weight and weight_old < 0:
                self.enterLongShortWeight(bars, inst, weight, False, False)

    def open_long_position(self, bars, target_position):
        """
        若目标持仓大于原有持仓（包括0）则加仓
        """
        for (inst, weight) in target_position.items():
            # print inst, weight
            """
            取得该股票的旧权重
            """
            if FutureUtil.is_index_future(inst):
                # 将年月为0000的合约自动转换为主力合约
                inst = FutureUtil.auto_contract(inst, bars.getDateTime(), bars)

            if bars[inst].ext_status != ExtStatus.NORMAL:
                continue
            weight_old = self.getBroker().getSharesAmount(inst, bars) / self.getBroker().get_total_equity()
            if weight > weight_old >= 0:
                self.enterLongShortWeight(bars, inst, weight, False, False)

    def open_short_position(self, bars, target_position):
        for (inst, weight) in target_position.items():
            # print inst, weight
            """
            取得该股票的旧权重
            """
            if FutureUtil.is_index_future(inst):
                # 将年月为0000的合约自动转换为主力合约
                inst = FutureUtil.auto_contract(inst, bars.getDateTime(), bars)

            if bars[inst].ext_status != ExtStatus.NORMAL:
                continue
            weight_old = self.getBroker().getSharesAmount(inst, bars) / self.getBroker().get_total_equity()
            if weight < weight_old <= 0:
                self.enterLongShortWeight(bars, inst, weight, False, False)

    def margin_call_handler(self, bars, current_cash, cash_requirement):
        required_weight = (cash_requirement - current_cash) / self.getBroker().get_total_equity()
        cash_weight = current_cash / self.getBroker().get_total_equity()
        weight_delta = required_weight / (1 - cash_weight)
        if len(self.getBroker().getPositions()) == 0:
            raise ValueError("无法补充保证金，回测终止")
        for inst in self.getBroker().get_stock_list():
            shares = self.getBroker().getShares(inst)
            if self.__config.whole_batch_only:
                shares = math.ceil(shares * weight_delta / 100) * 100
            else:
                shares = math.ceil(shares * weight_delta)
            self.enterShort(bars, inst, shares, False, False, msg="补充保证金卖出")

    def transform_maturities(self, bars):
        """
        到期换仓
        """

        date_str = bars.getDateTime().strftime("%Y%m%d")

        future_list = self.getBroker().get_future_list()
        if len(future_list) == 0:
            return

        for instrument in future_list:
            if bars[instrument].end_date == date_str:
                # 到期换仓
                next_inst = FutureUtil.next_contract(instrument)
                position = self.getBroker().get_position(instrument)
                nominal_amount = position.quantity * position.point
                nominal_weight = nominal_amount / self.getBroker().get_total_equity()
                self.enterLongShortWeight(bars, instrument, 0, msg='到期换出')
                self.enterLongShortWeight(bars, next_inst, nominal_weight, msg='到期换入')
                self.ref_price_ignore(next_inst)  # 股指期货不计算胜率
            #     self.enter_future_long_short_weight(bars, instrument, 0)
            #     self.enter_future_long_short_weight(bars, next_inst, -target_position)
            #     self.__current_contract = next_inst
            # elif force:
            #     self.enter_future_long_short_weight(bars, instrument, -target_position)

    def preprocess_target_position(self, bars):
        date_str = bars.getDateTime().strftime("%Y%m%d")

        def transform_future(windcode):
            if FutureUtil.is_index_future(windcode):
                return FutureUtil.auto_contract(windcode, date_str, bars)
            else:
                return windcode

        weight_df = self.__weight[self.__weight['date'] == date_str].copy()
        if not self.getBroker().by_pass_future:
            weight_df['windcode'] = weight_df['windcode'].map(transform_future)

        bars_inst = set(bars.getInstruments())

        weight_inst = set(weight_df["windcode"].tolist())

        assert len(weight_df) == len(weight_inst), f"权重表在{date_str}存在重复项"

        diff = weight_inst - bars_inst

        assert len(diff) == 0, f'instruments {diff} not found in bars on {date_str}'
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

        return target_position

    def __change_position(self, bars):
        """
        按照权重调仓
        计算思路：以全部清仓作为基准计算权重对应的金额，相应得出买入量
        """
        date_str = bars.getDateTime().strftime("%Y%m%d")
        target_position = self.preprocess_target_position(bars)

        # print(target_position)

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
        self.close_long_position(bars, target_position)

        # 先多头减仓，以最大化可用现金
        self.reduce_long_position(bars, target_position)
        self.close_short_position(bars, target_position)

        self.reduce_short_position(bars, target_position)

        # 最后对要买入的进行处理
        self.open_short_position(bars, target_position)
        self.open_long_position(bars, target_position)

        total_amount = self.getBroker().long_amount + self.getBroker().short_amount

        turnover_rate = (total_amount / shares_value) if shares_value != 0 else 0

        capacity = self.calc_capacity(bars, target_position.keys())

        self.refresh_ref_price(bars)
        self.refresh_return_data(bars)
        self.custom_analyzer.track('调仓日指标',
                                   date_str=date_str,
                                   turnover_rate=turnover_rate,
                                   capacity=capacity * 1000 / 1e8)

    def intraday_pnl(self):
        pass

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
        console_log('strategy trade date is:')
        if len(self.__buy_date) > 5:
            console_log(f"{self.__buy_date[0]}, {self.__buy_date[1]}, ..., {self.__buy_date[-1]}")
        else:
            console_log(self.__buy_date)
        console_log('total trade days:', len(self.__buy_date))
        if self.__config.progress_bar:
            self.__pbar = self.__tqdm_cls(total=len(self.__buy_date), disable=(not self.__config.progress_bar))

        self.__ext_date = deque([d['trade_dt'] for d in self.__ext_status_data])

    def on_finish(self, bars):
        self.getBroker().simplify_position()

        try:
            if self.__prev_trade_date != self.current_date_str:
                win_ratio_dict = self.calc_win_ratio(bars)
                self.custom_analyzer.track('调仓日胜率',
                                           date_range=self.__prev_trade_date + '-' + self.current_date_str,
                                           **win_ratio_dict)

                ret_dict = self.calc_ret(bars)
                self.custom_analyzer.track('调仓日收益率',
                                           date_range=self.__prev_trade_date + '-' + self.current_date_str,
                                           **ret_dict)
        except KeyError as e:
            e.args = (e.args[0], self.current_date_str,
                      'This exception may be caused by unaligned data, '
                      'please try to update the data or use an earlier `end_date`.')
            raise e

        if self.__pbar:
            self.__pbar.close()

    def mr_hook(self, original_code, new_code, ratio):
        self.trade_day_tracker_mr_hook(original_code, new_code, ratio, self.current_date_str)

    def stop_pnl_hook(self, bars, stop_pnl_args):
        if self.__config.stop_pnl_replacement is None:
            return

        target_inst = self.__config.stop_pnl_replacement
        if target_inst in BENCH_INDEX:
            target_inst = SynthIndexETFBar.synth_name(target_inst)

        total_amount = self.getBroker().getSharesAmount(target_inst, bars)
        for inst, amount, pnl in stop_pnl_args:
            total_amount += amount
        weight = total_amount / self.getBroker().get_total_equity()
        self.enterLongShortWeight(bars, target_inst, weight)
        self.stop_pnl_exclude(target_inst)

        # 计算胜率时剔除标的，因为有可能不同股票换为同一标的
        self.ref_price_ignore(target_inst)

    def on_bars(self, bars):
        """
        每天的数据流到来时触发一次
        """

        while len(self.__trade_date) > 0 and self.current_date_str > self.__trade_date[0]:  # 处理非交易日调仓的情况
            # TODO: 日志中增加警告
            self.__trade_date.popleft()
            if self.__pbar:
                self.__pbar.update(1)
        if len(self.__trade_date) > 0 and self.current_date_str == self.__trade_date[0]:
            self.__trade_date.popleft()
            # print(date_str)
            self.__change_position(bars)
            self.reset_stop_pnl_excluded()
            self.__prev_trade_date = self.current_date_str
            if self.__pbar:
                self.__pbar.update(1)

    def end_of_bar_hook(self, bars):
        if isinstance(self.getBroker(), BrokerV2):
            self.transform_maturities(bars)


    @classmethod
    def strategy_name(cls):
        return "WeightStrategy"


def prepare_feed(begin_date, end_date, instruments, config):
    console_log("preparing feed...")

    ext_status_df = pd.DataFrame()

    calendar = wk_data.get('trade_calendar', begin_date=begin_date, end_date=end_date, calendar=config.calendar)
    align_func = partial(align_calendar, calendar=calendar)

    feed = MixedFeed()
    max_time_list = []

    for dataset in config.datasets:
        dataset = DatasetType[dataset.upper()]
        if dataset == DatasetType.A_SHARE_MARKET:
            max_time = feed.add_bars('a_share_market', StockDataRowParser,
                                     begin_date=begin_date, end_date=end_date,
                                     preprocessor=PreprocessorSeq(
                                         partial(filter_market_data, instruments=instruments),
                                         align_func
                                     ))
            max_time_list.append(max_time)
            data = feed.get_processed_data('a_share_market')
            ext_status_df = data[data['ext_status'] != ExtStatus.NORMAL.value].sort_values(by="trade_dt")
            if not data.empty:
                end_date = data['trade_dt'].max()  # 强制对齐
        elif dataset == DatasetType.SYNTH_INDEX_ETF:
            max_time = feed.add_bars('index_market', SyntheticIndexETFRowParser,
                                     bars_name='synth_index_etf', begin_date=begin_date, end_date=end_date,
                                     preprocessor=PreprocessorSeq(
                                         add_normal_ext_status,
                                         align_func
                                     ))
            max_time_list.append(max_time)

        elif dataset == DatasetType.LOCAL_INDEX_ETF:
            max_time = feed.add_bars('local_index_20240417', SyntheticIndexETFRowParser,
                                     bars_name='local_synth_index_etf', begin_date=begin_date, end_date=end_date,
                                     preprocessor=PreprocessorSeq(
                                         add_normal_ext_status,
                                         align_func
                                     ))
            max_time_list.append(max_time)

        elif dataset == DatasetType.ETF_FUND_SUBSET_1:
            max_time = feed.add_bars('etf_fund_subset_1', FundDataRowParser,
                                     begin_date=begin_date, end_date=end_date, preprocessor=align_func)
            max_time_list.append(max_time)
        elif dataset == DatasetType.SPECIAL_INDEX:
            max_time = feed.add_bars('special_index', FundNavDataRowParser, begin_date=begin_date, end_date=end_date,
                                     preprocessor=align_func)
            # data = feed.get_processed_data('special_index')
            # ext_status_df = data[data['ext_status'] != ExtStatus.NORMAL.value].sort_values(by="trade_dt")
            assert config.price_type == PriceType.CLOSE, \
                f'`price_type` must be `close` when using dataset `{dataset.name.lower()}`'
            max_time_list.append(max_time)

        # elif dataset == DatasetType.POSITION_DUMMY:
        #     max_time = feed.add_bars('dummy_data', DummyPositionBar, begin_date=begin_date, end_date=end_date,
        #                   preprocessor=align_func)
        #     max_time_list.append(max_time)
        else:
            raise ValueError(f"Unsupported dataset `{dataset}`")

    feed.add_bars('dummy_data', PositionDummyRowParser, begin_date=begin_date, end_date=end_date,
                  preprocessor=align_func)

    max_time = feed.add_bars('index_market', IndexDataRowParser, begin_date=begin_date, end_date=end_date,
                             preprocessor=align_func)
    max_time_list.append(max_time)
    min_max_time = begin_date if None in max_time_list else min(max_time_list)
    feed.align_time(min_max_time)
    feed.prefetch(config.progress_bar)
    return feed, ext_status_df


#

def incremental_prepare_feed(feed: MixedFeed, end_date, instruments, config):
    console_log("add feed...")

    ext_status_df = pd.DataFrame()

    begin_date = feed.get_last_date()

    max_time_list = []

    for dataset in config.datasets:
        dataset = DatasetType[dataset.upper()]
        if dataset == DatasetType.A_SHARE_MARKET:
            max_time = feed.add_bars('a_share_market', StockDataRowParser,
                                     begin_date=begin_date, end_date=end_date,
                                     preprocessor=partial(filter_market_data, instruments=instruments))

            max_time_list.append(max_time)
            data = feed.get_processed_data('a_share_market')
            ext_status_df = data[data['ext_status'] != ExtStatus.NORMAL.value].sort_values(by="trade_dt")

            if not data.empty:
                end_date = data['trade_dt'].max()  # 强制对齐
        elif dataset == DatasetType.SYNTH_INDEX_ETF:
            max_time = feed.add_bars('index_market', SyntheticIndexETFRowParser,
                                     bars_name='synth_index_etf', begin_date=begin_date, end_date=end_date)
            max_time_list.append(max_time)

        elif dataset == DatasetType.ETF_FUND_SUBSET_1:
            max_time = feed.add_bars('etf_fund_subset_1', FundNavDataRowParser,
                                     begin_date=begin_date, end_date=end_date)
            max_time_list.append(max_time)
        else:
            raise ValueError(f"Unsupported dataset `{dataset}`")

    max_time = feed.add_bars('index_market', IndexDataRowParser, begin_date=begin_date, end_date=end_date)
    max_time_list.append(max_time)

    min_max_time = begin_date if None in max_time_list else min(max_time_list)
    feed.align_time(min_max_time)

    feed.prefetch(config.progress_bar)
    return feed, ext_status_df


class WeightStrategy:
    """
    根据权重列表定期调仓的回测策略
    """

    def __init__(self, weight, begin_date, end_date=None, config=WeightStrategyConfiguration(),
                 is_tag=False, tqdm_cls=tqdm, user_benchmark=None, broker_cls=Broker):
        """
        Parameters
        ----------
        weight: pd.DataFrame or str
            调仓权重列表

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

        check_weight_df(self.__weight_df)

        self.__begin_date = begin_date
        self.__end_date = end_date
        if self.__end_date is None:
            self.__end_date = (datetime.datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
        self.__result = None
        self.__config = config
        self.__tqdm_cls = tqdm_cls
        self.__user_benchmark = user_benchmark
        console_log("backtest range:", self.__begin_date, self.__end_date)

        self.__broker_cls = broker_cls

    def instruments(self):
        dat_val = self.__weight_df
        dat_val['date'] = pd.to_datetime(dat_val['date'], format='%Y%m%d')
        dat_val['date'] = [datetime.datetime.strftime(x, '%Y%m%d') for x in dat_val['date']]
        weight_df = dat_val
        weight_df = filter_data(weight_df, self.__begin_date, self.__end_date, date_field_tag='date')
        instruments = weight_df['windcode'].unique().tolist()

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
        return instruments

    def __prepare_feed(self):
        weight_df = self.__weight_df
        instruments = weight_df['windcode'].unique().tolist()
        feed_registry = FeedRegistry(self.__begin_date, self.__end_date, config=self.__config)
        for data_name in self.__config.datasets:
            if DatasetType[data_name.upper()] == DatasetType.A_SHARE_MARKET \
                    or DatasetType[data_name.upper()] == DatasetType.A_SHARE_MARKET_VWAP_M15 \
                    or DatasetType[data_name.upper()] == DatasetType.A_SHARE_MARKET_VWAP_M30 \
                    or DatasetType[data_name.upper()] == DatasetType.A_SHARE_MARKET_VWAP_M60:
                feed_registry.register(data_name, instruments=instruments)
            else:
                feed_registry.register(data_name)

        self.__feed, self.__ext_status_df = feed_registry.build_feed()

    def run(self, feed=None, ext_status_df=None):
        if feed is None:
            self.__prepare_feed()
        else:
            self.__feed, self.__ext_status_df = feed, ext_status_df

        begin_date, end_date = self.__begin_date, self.__end_date
        dat_val = self.__weight_df

        dat_val['date'] = pd.to_datetime(dat_val['date'], format='%Y%m%d')
        dat_val['date'] = [datetime.datetime.strftime(x, '%Y%m%d') for x in dat_val['date']]
        dat_val = dat_val[(dat_val['date'] >= begin_date) & (dat_val['date'] <= end_date)]

        # 设定起始日期为首行日期
        tmp = dat_val['date']
        begin_date = tmp.iloc[0]
        weight_strategy = WeightStrategyBase(self.__feed, dat_val, begin_date, end_date, self.__config,
                                             ext_status_data=self.__ext_status_df.to_dict(orient="records"),
                                             sign=self.__sign,
                                             tqdm_cls=self.__tqdm_cls, broker_cls=self.__broker_cls)

        output = strategyOutput.StrategyOutput(weight_strategy, begin_date, end_date,
                                               config=self.__config, user_benchmark=self.__user_benchmark)
        output.pre_process()
        weight_strategy.run()
        output.bench_process()
        output.post_process()
        self.__result = output.result

    @classmethod
    def strategy_class(cls):
        return WeightStrategyBase

    @property
    def result(self) -> BackTestResult:
        return self.__result


class BatchWeightStrategy:
    """
    批量执行权重回测的策略
    """

    def __init__(self, files, begin_date, end_date=None, config=WeightStrategyConfiguration(), max_process=1):
        console_log('platform version:', __version__)
        self.__max_process = max_process
        self.__result_set = [None for _ in files]
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

    def __prepare_feed(self):
        # ds = DataSource()
        # self.__mr_map = ds.get_mr_data()

        instruments = []
        for k, weight_df in self.__weights.items():
            instruments += weight_df['windcode'].unique().tolist()
        instruments = list(set(instruments))

        self.__feed, self.__ext_status_df = WeightStrategyBase.prepare_feed(
            self.__begin_date, self.__end_date,
            instruments=instruments, config=self.__config
        )

    def run(self):
        self.__prepare_feed()
        begin_date, end_date = self.__begin_date, self.__end_date
        for name, weight_df in self.__weights.items():
            try:
                dat_val = weight_df

                dat_val['date'] = pd.to_datetime(dat_val['date'], format='%Y%m%d')
                dat_val['date'] = [datetime.datetime.strftime(x, '%Y%m%d') for x in dat_val['date']]
                dat_val = dat_val[(dat_val['date'] >= begin_date) & (dat_val['date'] <= end_date)]

                """
                设定起始日期为首行日期
                """
                tmp = dat_val['date']
                begin_date = tmp.iloc[0]
                weight_strategy = WeightStrategyBase(self.__feed, dat_val, begin_date, end_date, self.__config,
                                                     ext_status_data=self.__ext_status_df.to_dict(orient="records"),
                                                     sign=self.__signs[name])

                output = strategyOutput.StrategyOutput(weight_strategy, begin_date, end_date, config=self.__config)
                output.pre_process()
                weight_strategy.run()
                output.bench_process()
                output.post_process()
                self.__result_set[name] = output.result
                self.__feed.reset()
            except Exception as e:
                console_log(f'failed back test {name} ', e)
                continue

    @property
    def result_set(self) -> BackTestResultSet:
        return self.__result_set
