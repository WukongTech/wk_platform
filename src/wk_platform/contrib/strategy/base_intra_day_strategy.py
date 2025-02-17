
from collections import deque, OrderedDict

import pathlib

import numpy as np
import pandas as pd
import datetime

from datetime import timedelta
from collections import OrderedDict

from dataclasses import dataclass

from pyalgotrade.bar import Frequency

from wk_util.tqdm import tqdm
from wk_platform import __version__
from wk_platform.backtest import strategyOutput
from wk_platform.config import  MaxUpDownType, PriceType, PositionCostType, TrackLevel
from wk_platform.feed.fast_feed import StockFeed, StockIndexSynthETFFeed

from wk_platform.backtest.result import BackTestResult
from wk_platform.contrib.strategy.tracker_mixin import (
    TradeDayWinRatioRecord,
    TradeDayReturnRecord,
    TradeDayRecord,
    TradeDayTrackerMixin
)
from wk_data.constants import ExtStatus
from wk_util.logger import console_log
from wk_util.file_digest import md5
from wk_platform.feed.bar import SynthIndexETFBar
from wk_data.constants import BENCH_INDEX
from wk_platform.strategy.mid_frequency_strategy import MidFreqBacktestingStrategy, MidFreqIntraDaySubStrategy
import wk_db
import wk_data
# import wk_platform.contrib.util as util
from wk_platform.broker.brokers import IntraDayBroker
from wk_platform.config import StrategyConfiguration
from wk_platform.contrib.util import check_weight_df
from wk_platform.util.data import filter_market_data, add_normal_ext_status


def gen_intra_day_market_data(data: pd.DataFrame, intra_day_list):
    """
    生成特殊的行情信息，即仅包含开盘收盘两个时间点的日内行情
    """
    data = pd.DataFrame({"trade_dt": intra_day_list}).merge(data, on="trade_dt")
    open_data = data.copy()
    close_data = data.copy()
    open_data['trade_dt'] = open_data['trade_dt'].apply(lambda dt: dt+'0930')
    open_data['close'] = open_data['open']

    close_data['trade_dt'] = close_data['trade_dt'].apply(lambda dt: dt+'1500')
    open_data['open'] = open_data['close']
    final_data = pd.concat([open_data, close_data]).sort_values('trade_dt')
    final_data['amount'] = final_data['amount'] / 2
    final_data['amount_ma'] = final_data['amount_ma'] / 2
    return final_data


def gen_intra_day_index_data(data: pd.DataFrame, intra_day_list):
    """
    生成特殊的行情信息，即仅包含开盘收盘两个时间点的日内行情
    """
    data = pd.DataFrame({"trade_dt": intra_day_list}).merge(data, on="trade_dt")
    open_data = data.copy()
    close_data = data.copy()
    open_data['trade_dt'] = open_data['trade_dt'].apply(lambda dt: dt+'0930')
    open_data['close'] = open_data['open']

    close_data['trade_dt'] = close_data['trade_dt'].apply(lambda dt: dt+'1500')
    open_data['open'] = open_data['close']
    final_data = pd.concat([open_data, close_data]).sort_values('trade_dt')
    final_data['amount'] = final_data['amount'] / 2
    return final_data


def build_intra_day_feeds(data: pd.DataFrame, intra_day_index_data, allow_synth=False, progress_bar=False):
    feeds = OrderedDict()
    data['date'] = data['trade_dt'].apply(lambda dt: dt[:8])
    grouped_data = data.groupby('date')
    console_log('load intra day bar')
    for k, group in grouped_data:
        if allow_synth:
            feed = StockIndexSynthETFFeed(frequency=Frequency.TRADE)
        else:
            feed = StockFeed(frequency=Frequency.TRADE)
        feed.add_stock_bars(group)
        feeds[k] = feed

    intra_day_index_data['date'] = intra_day_index_data['trade_dt'].apply(lambda dt: dt[:8])

    intra_day_index_data = add_normal_ext_status(intra_day_index_data)
    grouped_data = intra_day_index_data.groupby('date')
    for k, group in tqdm(grouped_data, disable=(not progress_bar)):
        feeds[k].add_index_bars(group)
        if allow_synth:
            feeds[k].add_synth_index_etf_bars(group)
        feeds[k].prefetch()

    return feeds


def build_trans_actions(trans_df):
    trans_actions = OrderedDict()
    grouped_data = trans_df.groupby('datetime')
    for k, group in grouped_data:
        trans_actions[k] = group
    return trans_actions


class IntraDayRatioStrategyConfiguration(StrategyConfiguration):
    def __init__(self,
                 initial_cash=1e8,
                 max_up_down_limit=MaxUpDownType.STRICT,
                 suspension_limit=True,
                 commission=0.0003,
                 stamp_tax=0.001,
                 risk_free_rate=0,
                 whole_batch_only=False,
                 price_with_commission=False,
                 adapt_quantity=True,
                 allow_synth_etf=False,
                 stop_pnl_replacement=None,
                 progress_bar=True,
                 position_cost_type=PositionCostType.TRADE_DATE,
                 tracking_transaction=True,
                 detailed_position_track_level=TrackLevel.TRADE_DAY,
                 position_track_level=TrackLevel.TRADE_DAY):
        """
        Parameters
        ==================
        initial_cash: float
            初始资金，默认1e8
        max_up_down_limit: MaxUpDownType
            存在涨跌停情况时的处理规则
        suspension_limit: bool
            是否开启停复牌限制，默认开启
        commission: float
            券商佣金，目前仅支持按百分比设置，默认0.0003
        stamp_tax: float
            印花税率，默认0.001
        risk_free_rate: float
            无风险利率，用于计算夏普比等指标，默认为0
        whole_batch_only: bool
            是否限制仅允许整手买入，默认不做限制
        price_with_commission: bool
            根据持仓权重计算交易量时，是否将佣金作为价格的一部分用于计算实际交易数量。
            仅适用于根据百分比收取佣金的情形，仅对买入操作生效
        adapt_quantity: bool
            在现金不足时是否根据当前现金调整交易数量。通常在每次调仓的最后一笔交易中出现
        progress_bar: bool
            是否显示加载feed和策略运行的进度条
        position_cost_type: PositionCostType
            持仓成本计算方法，默认根据最新换仓日的价格计算
        tracking_transaction: bool
            是否开启交易流水追踪，默认开启
        detailed_position_track_level: TrackLevel
            详细持仓的记录级别
        position_track_level: TrackLevel
            持仓记录级别，默认记录调仓日
        """
        super().__init__(
            initial_cash=initial_cash,
            volume_limit=None,
            max_up_down_limit=max_up_down_limit,
            suspension_limit=suspension_limit,
            commission=commission,
            stamp_tax=stamp_tax,
            risk_free_rate=risk_free_rate,
            whole_batch_only=whole_batch_only,
            progress_bar=progress_bar,
            price_with_commission=price_with_commission,
            adapt_quantity=adapt_quantity,
            price_type=PriceType.OPEN,
            position_cost_type=position_cost_type,
            allow_synth_etf=allow_synth_etf,
            broker=None,
            stop_profit=None,
            stop_loss=None,
            tracking_transaction=tracking_transaction,
            detailed_position_track_level=detailed_position_track_level,
            position_track_level=position_track_level)

        self.__stop_pnl_replacement = stop_pnl_replacement

    @property
    def stop_pnl_replacement(self):
        return self.__stop_pnl_replacement

    @property
    def type(self):
        return "股票多头"


class IntraDayRatioStrategyBase(MidFreqBacktestingStrategy, TradeDayTrackerMixin):
    """
    允许日内交易的策略
    """
    def __init__(self, feed_pair, data, begin_date, end_date,
                 config: IntraDayRatioStrategyConfiguration = IntraDayRatioStrategyConfiguration(),
                 ext_status_data=None, mr_map=None, sign=None):

        feed, id_feeds = feed_pair
        super().__init__(feed, begin_date, end_date, config=config, ext_status_data=ext_status_data, sign=sign)
        TradeDayTrackerMixin.__init__(self, self.getBroker(), config)

        self.__config = config
        self.__intra_day_feeds = id_feeds

        # 原始操作表
        self.__trans_df = data
        # 按日期切分后的操作表
        self.__trans_actions = build_trans_actions(self.__trans_df)

        self.__exec_intra_day = False
        self.__intra_day_strategy = None

        # 有交易操作的交易日
        self.__trade_date = deque()
        self.__ext_date = None

        self.__ext_status_data = deque(ext_status_data) if ext_status_data is not None else deque()
        self.__mr_map = mr_map if mr_map is not None else {}

        self.__pbar = None

        self.__prev_trade_date = ''
        self.__first_trade = True
        self.__prev_trade_dt = None

    def get_trade_date(self):
        return self.__trade_date

    def on_start(self):
        """
        策略开始运行时执行
        """
        # 初始化自定义追踪指标
        self.custom_analyzer.add_tracker('调仓胜率', TradeDayWinRatioRecord)
        self.custom_analyzer.add_tracker('调仓收益率', TradeDayReturnRecord)
        self.custom_analyzer.add_tracker('调仓指标', TradeDayRecord)

        # 初始化有交易操作的交易日
        self.__trade_date = deque(sorted(self.__trans_df['date'].unique()))
        console_log('strategy trade date is:')
        if len(self.__trade_date) > 5:
            console_log(f"{self.__trade_date[0]}, {self.__trade_date[1]}, ..., {self.__trade_date[-1]}")
        else:
            console_log(self.__trade_date)
        console_log('total trade days:', len(self.__trade_date))
        if self.__config.progress_bar:
            self.__pbar = tqdm(total=len(self.__trade_date), disable=(not self.__config.progress_bar))

        self.__ext_date = deque([d['trade_dt'] for d in self.__ext_status_data])

    def on_finish(self, bars):
        self.getBroker().simplify_position()

        if self.__prev_trade_dt != self.__intra_day_strategy.current_dt_str:
            win_ratio_dict = self.calc_win_ratio(bars)
            self.custom_analyzer.track('调仓胜率',
                                       date_range=self.__prev_trade_dt + '-' + self.__intra_day_strategy.current_dt_str,
                                       **win_ratio_dict)

            ret_dict = self.calc_ret(bars)
            self.custom_analyzer.track('调仓收益率',
                                       date_range=self.__prev_trade_dt + '-' + self.__intra_day_strategy.current_dt_str,
                                       **ret_dict)

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
        # self.ref_price_ignore(target_inst)

    def __intra_day_change_position(self, bars):
        """依照输入比率调仓
        """
        # weight_df = self.__trans_df[self.__trans_df["windcode"].isin(bars.getInstruments())]
        dt_str = bars.getDateTime().strftime("%Y%m%d%H%M")

        try:
            trans_df = self.__trans_actions[dt_str]
        except KeyError:
            return
        trans_df.set_index('datetime', inplace=True)

        full_windcode_list = trans_df['windcode'].unique()

        buy_trans = trans_df[trans_df['action'] == 'BUY']
        sell_trans = trans_df[trans_df['action'] == 'SELL']
        self.getBroker().reduce_position()

        if self.__prev_trade_dt is not None:
            win_ratio_dict = self.calc_win_ratio(bars)
            self.custom_analyzer.track('调仓胜率',
                                       date_range=self.__prev_trade_dt + '-' + dt_str,
                                       **win_ratio_dict)
            ret_dict = self.calc_ret(bars)
            self.custom_analyzer.track('调仓收益率',
                                       date_range=self.__prev_trade_dt + '-' + dt_str,
                                       **ret_dict)

        shares_value = self.getBroker().getSharesValue()

        # 先执行卖出
        for inst, ratio in zip(sell_trans['windcode'], sell_trans['ratio']):

            # 取得该股票的旧权重
            # TODO: 在持仓价值为0时的处理
            amount = self.getBroker().getSharesAmount(inst, bars)
            total_equity = self.getBroker().get_total_equity()
            weight_old = amount / total_equity
            weight_new = weight_old * (1 - ratio)
            if weight_old >= weight_new:
                self.enterLongShortWeight(bars, inst, weight_new, False, False)

        total_cash = self.getBroker().getCash()
        # 再执行买入
        for inst, ratio in zip(buy_trans['windcode'], buy_trans['ratio']):
            if bars[inst].ext_status != ExtStatus.NORMAL:
                continue
            total_equity = self.getBroker().get_total_equity()
            weight = total_cash / total_equity * ratio
            self.enterLongShortWeight(bars, inst, weight, False, False)

        total_amount = self.getBroker().long_amount + self.getBroker().short_amount

        turnover_rate = (total_amount / shares_value) if shares_value != 0 else 0

        capacity = self.calc_capacity(bars, full_windcode_list)

        self.refresh_ref_price(bars)
        self.refresh_return_data(bars)
        self.custom_analyzer.track('调仓指标',
                                   date_str=dt_str,
                                   turnover_rate=turnover_rate,
                                   capacity=capacity * 1000 / 1e8)

        self.__prev_trade_dt = self.__intra_day_strategy.current_dt_str

    def getBroker(self):
        try:
            if self.__exec_intra_day:
                return self.__intra_day_strategy.getBroker()
            else:
                return super().getBroker()
        except AttributeError:
            return super().getBroker()

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
            feed = self.__intra_day_feeds[self.current_date_str]

            self.__exec_intra_day = True
            self.__intra_day_strategy = MidFreqIntraDaySubStrategy(feed, broker_cls=IntraDayBroker, config=self.__config)

            self.__intra_day_strategy.register_hook('on_bars', lambda bars: self.__intra_day_change_position(bars))
            self.__intra_day_strategy.switch_broker_status(self.broker, self.__intra_day_strategy.getBroker())
            self.__intra_day_strategy.getBroker().init_fill_strategy(bars)
            self.__intra_day_strategy.run()
            self.__intra_day_strategy.switch_broker_status(self.__intra_day_strategy.getBroker(), self.broker)
            self.__exec_intra_day = False

            # self.__change_position(bars)
            self.reset_stop_pnl_excluded()
            self.__prev_trade_date = self.current_date_str
            if self.__pbar:
                self.__pbar.update(1)

    @classmethod
    def prepare_feed(cls, begin_date, end_date, instruments, intra_day_list, config):

        data = wk_data.get("a_share_market", begin_date=begin_date, end_date=end_date)
        mr_map = wk_data.get("mr_data")

        data = filter_market_data(data, instruments)
        ext_status_df = data[data['ext_status'] != ExtStatus.NORMAL.value].sort_values(by="trade_dt")

        index_data = wk_data.get('index_market', begin_date=begin_date, end_date=end_date)
        console_log("preparing feed...")

        intra_day_data = gen_intra_day_market_data(data, intra_day_list)
        id_index_data = gen_intra_day_index_data(index_data, intra_day_list)

        if not data.empty:
            end_date = data['trade_dt'].max()  # 强制对齐

        if config.stop_pnl_replacement is None and not config.allow_synth_etf:
            feed = StockFeed()
            feed.add_stock_bars(data)

            # index_data = ds.get_index_data(begin_date=begin_date, end_date=end_date)
            index_data = wk_data.get('index_market', begin_date=begin_date, end_date=end_date)
            feed.add_index_bars(index_data)
        else:

            feed = StockIndexSynthETFFeed()
            feed.add_stock_bars(data)

            # index_data = ds.get_index_data(begin_date=begin_date, end_date=end_date)
            index_data = wk_data.get('index_market', begin_date=begin_date, end_date=end_date)
            index_data = add_normal_ext_status(index_data)
            feed.add_index_bars(index_data)
            feed.add_synth_index_etf_bars(index_data)

        feed.prefetch(progress_bar=config.progress_bar)
        # id_feed = StockFeed(frequency=Frequency.TRADE)
        # id_feed.add_stock_bars(intra_day_data)
        id_feeds = build_intra_day_feeds(intra_day_data, id_index_data, allow_synth=config.allow_synth_etf,
                                         progress_bar=config.progress_bar)

        return feed, id_feeds, ext_status_df, mr_map, end_date

    @classmethod
    def strategy_name(cls):
        return "IntraDayRatioStrategy"


class IntraDayRatioStrategy:
    """
    根据权重列表定期调仓的回测策略
    """
    def __init__(self, weight, begin_date, end_date=None, config=StrategyConfiguration(), is_tag=False):
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
            self.__weight_df = data.sort_values(['datetime', 'windcode'])
            self.__sign = None
        elif isinstance(weight, pd.DataFrame):
            self.__weight_df = weight.sort_values(['datetime', 'windcode'])
            self.__sign = None
        else:
            assert isinstance(weight, str) or isinstance(weight, pathlib.Path)
            self.__weight_df = pd.read_csv(weight, encoding="gbk").sort_values(['datetime', 'windcode'])
            self.__sign = md5(weight)

        check_weight_df(self.__weight_df, group_field='datetime')



        self.__weight_df = self.preprocess_trans_df(self.__weight_df)

        self.__begin_date = begin_date
        self.__end_date = end_date
        self.__data_end_date = None
        if self.__end_date is None:
            self.__end_date = (datetime.datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
        self.__result = None
        self.__config = config
        console_log("backtest range:", self.__begin_date, self.__end_date)

    @classmethod
    def preprocess_trans_df(cls, trans_df):
        trans_df['datetime'] = pd.to_datetime(trans_df['datetime'], format='%Y%m%d%H%M')
        trans_df['date'] = [datetime.datetime.strftime(x, '%Y%m%d') for x in trans_df['datetime']]
        trans_df['datetime'] = [datetime.datetime.strftime(x, '%Y%m%d%H%M') for x in trans_df['datetime']]
        return trans_df

    def __prepare_feed(self):
        weight_df = self.__weight_df
        instruments = weight_df['windcode'].unique().tolist()
        id_list = weight_df['date'].unique().tolist()
        # id_list = [dt[:8] for dt in id_list]
        # id_list = list(set(id_list))
        self.__feed, self.__intra_day_feed, self.__ext_status_df, self.__mr_map, self.__data_end_date = \
            IntraDayRatioStrategyBase.prepare_feed(
                self.__begin_date, self.__end_date,
                instruments=instruments, intra_day_list=id_list, config=self.__config
        )

    def run(self):
        self.__prepare_feed()
        begin_date, end_date = self.__begin_date, self.__end_date
        end_date = min(end_date, self.__data_end_date)
        # begin_date = begin_date + '0000'
        # end_date = end_date + '2359'
        dat_val = self.__weight_df

        # dat_val['date'] = pd.to_datetime(dat_val['datetime'], format='%Y%m%d%H%M')
        # dat_val['date'] = [datetime.datetime.strftime(x, '%Y%m%d') for x in dat_val['date']]


        dat_val = dat_val[(dat_val['date'] >= begin_date) & (dat_val['date'] <= end_date)]

        """
        设定起始日期为首行日期
        """
        tmp = dat_val['date']
        begin_date = tmp.iloc[0]
        weight_strategy = IntraDayRatioStrategyBase(
            (self.__feed, self.__intra_day_feed),
            dat_val,
            begin_date, end_date,
            self.__config,
            ext_status_data=self.__ext_status_df.to_dict(orient="records"),
            mr_map=self.__mr_map, sign=self.__sign
        )

        output = strategyOutput.StrategyOutput(weight_strategy, begin_date, end_date, config=self.__config)
        output.pre_process()
        weight_strategy.run()
        output.bench_process()
        output.post_process()
        self.__result = output.result

    @classmethod
    def strategy_class(cls):
        return IntraDayRatioStrategyBase

    @property
    def result(self) -> BackTestResult:
        return self.__result
