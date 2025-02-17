from __future__ import annotations

import inspect


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



class WeightStrategyConfiguration(StrategyConfiguration):
    def __init__(self,
                 strategy_name="myStrategy",
                 datasets=('a_share_market', ),
                 initial_cash=1e8,
                 volume_limit=None,
                 max_up_down_limit='strict',
                 suspension_limit=True,
                 commission=0.0003,
                 stamp_tax=0.001,
                 future_commission=0.00023,
                 max_position=1,
                 risk_free_rate=0,
                 deposit=0.15,
                 deposit_cash_ratio=0.7,
                 trade_rule='t+1',
                 whole_batch_only=False,
                 progress_bar=True,
                 price_with_commission=False,
                 adapt_quantity=True,
                 price_type: str | PriceType='open',
                 position_cost_type='trade_date',
                 stop_profit=None,
                 stop_loss=None,
                 intraday_stop_profit=None,
                 intraday_stop_loss=None,
                 stop_pnl_replacement=None,
                 capacity_proportion=0.1,
                 capacity_quantile=0.25,
                 tracking_transaction=True,
                 detailed_position_track_level='trade_day',
                 position_track_level='trade_day',
                 calendar='a_share_market',
                 profile_runtime=True):
        """
        Parameters
        ==================
        strategy_name: str
            策略名称
        datasets: tuple(str)
            回测时使用的数据集，默认只包含日行情数据
        initial_cash: float
            初始资金，默认1e8
        volume_limit: float, None
            是否开启交易量限制，取值为0~1，代表占当天交易量的比例，取值为None时无限制，默认关闭
        max_up_down_limit: MaxUpDownType
            存在涨跌停情况时的处理规则
        suspension_limit: bool
            是否开启停复牌限制，默认开启
        commission: float
            券商佣金，目前仅支持按百分比设置，默认0.0003
        stamp_tax: float | str
            印花税率，可以使用数值指定，或根据市场指定
        max_position: float
            最大持仓比例，权重表中权重之和低于仓位时，按实际权重确定持仓；高于仓位时，按总权重进行归一化后确定其在最大持仓部分的比例
        risk_free_rate: float
            无风险利率，用于计算夏普比等指标，默认为0
        trade_rule: str
            交易规则，默认t+1
        whole_batch_only: bool
            是否限制仅允许整手买入，默认不做限制
        price_with_commission: bool
            根据持仓权重计算交易量时，是否将佣金作为价格的一部分用于计算实际交易数量。
            仅适用于根据百分比收取佣金的情形，仅对买入操作生效
        adapt_quantity: bool
            在现金不足时是否根据当前现金调整交易数量。通常在每次调仓的最后一笔交易中出现
        price_type: PriceType
            交易时使用的价格类型，默认为开盘价
        progress_bar: bool
            是否显示加载feed和策略运行的进度条
        deposit: float
            保证金比例
        deposit_cash_ratio: float
            保证金占总现金的比例，超过该比例时，在每日盯市时会触发强制调仓。注意，在交易时不考虑此约束，交易时仅考虑保证金小于现金
        stop_profit: float, None
            止盈涨幅，None表示不进行止盈操作
        stop_loss: float, None
            止损跌幅，None表示不进行止损操作。例如亏损10%时止损，设定stop_loss=-0.1
        intraday_stop_profit: float, None
            日内止盈涨幅，None表示不进行止盈操作
        intraday_stop_loss: float, None
            日内止损跌幅，None表示不进行止损操作。例如亏损10%时止损，设定stop_loss=-0.1
        stop_pnl_replacement: str, None
            止盈/止损后用于填充空仓位的标的，使用wind代码表示。若使用指数代码则用对应的模拟指数ETF填充。None表示止盈/止损后保持空仓位
        position_cost_type: PositionCostType
            持仓成本计算方法，默认根据最新换仓日的价格计算
        capacity_proportion: float
            容量估算参数，对应策略容量估算公式中的beta，默认0.1
        capacity_quantile: float
            容量估算参数，对应策略容量估算时所取的分位点，默认0.25
        tracking_transaction: bool
            是否开启交易流水追踪，默认开启
        detailed_position_track_level: TrackLevel
            详细持仓的记录级别
        position_track_level: TrackLevel
            持仓记录级别，默认记录调仓日
        profile_runtime: bool
            是否追踪运行性能
        """

        kwargs = {k: v for k, v in inspect.currentframe().f_locals.items() if k != 'self' and k != "__class__"}
        datasets = kwargs.pop('datasets')
        if stop_pnl_replacement is not None:
            datasets = list(datasets)
            datasets.append('synth_index_etf')
            datasets = tuple(set(datasets))
        kwargs['datasets'] = datasets

        self.__stop_pnl_replacement = kwargs.pop('stop_pnl_replacement')
        self.__future_commission = kwargs.pop('future_commission')


        self.__deposit = kwargs.pop('deposit')
        self.__deposit_cash_ratio = kwargs.pop('deposit_cash_ratio')
        if 'future_index' in datasets:
            assert self.__deposit is not None
            assert self.__deposit_cash_ratio is not None

        super().__init__(**kwargs)

    @property
    def deposit(self):
        return self.__deposit

    @property
    def deposit_cash_ratio(self):
        return self.__deposit_cash_ratio

    @property
    def stop_pnl_replacement(self):
        return self.__stop_pnl_replacement

    @property
    def future_commission(self):
        return self.__future_commission

