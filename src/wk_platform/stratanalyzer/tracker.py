

import copy
from collections import deque, OrderedDict
from enum import Enum
import numpy as np
import pandas as pd

from wk_platform import stratanalyzer
from wk_util import logger
from wk_platform.stratanalyzer.record import BaseRecordType
from wk_platform.stratanalyzer.record import UnfilledOrderInfo
from wk_platform.stratanalyzer.record import TransactionRecord
from wk_platform.stratanalyzer.record import DetailedPositionRecord
from wk_platform.stratanalyzer.record import PositionRecord
from wk_platform.stratanalyzer.record import TotalPositionRecord
from wk_platform.stratanalyzer.record import TotalPositionRecord2
from wk_util.recorder import dataclass_list_to_dataframe
from wk_platform.config import PositionCostType, TrackLevel, PriceType
from wk_platform.config import HedgeStrategyConfiguration
from wk_platform.broker.brokers import HedgeBroker


class StrategyTracker(stratanalyzer.StrategyAnalyzer):
    """
    追踪策略回测过程中的各类附加信息
    """



    LOGGER_NAME = 'StrategyTracker'

    def __init__(self):
        super(StrategyTracker, self).__init__()
        # self.__event = observer.Event()
        self.__logger = logger.getLogger(StrategyTracker.LOGGER_NAME, disable=True)

        self.__strategy = None

        # 用来存储每日持仓
        self.__total_position_tracker = deque()

        self.__detailed_position_track_level = TrackLevel.TRADE_DAY
        self.__detailed_position_tracker: [DetailedPositionRecord] = deque()

        self.__position_track_level = TrackLevel.TRADE_DAY
        self.__position_tracker = deque()

        self.__hedge_strategy = False

    def attached(self, strat):
        self.__logger.info("call attached")
        self.__strategy = strat

        self.__detailed_position_track_level = self.__strategy.config.detailed_position_track_level
        self.__position_track_level = self.__strategy.config.position_track_level
        if isinstance(self.__strategy.config, HedgeStrategyConfiguration):
            self.__hedge_strategy = True

    def track_position(self, strat, bars):
        datetime_str = bars.getDateTime().strftime("%Y-%m-%d")
        # bars.
        positions = strat.getBroker().getPositions()
        for k, position in positions.items():
            if not isinstance(position, int):
                position = position.quantity
            rec = PositionRecord(
                trade_dt=datetime_str,
                windcode=k,
                volume=position,
                value=bars[k].get_price(self.__strategy.config.price_type) * position
            )
            self.__position_tracker.append(rec)

    def track_total_position(self, strat, bars):
        datetime_str = bars.getDateTime().strftime("%Y%m%d")
        broker = strat.getBroker()
        total_equity = broker.getEquity(PriceType.CLOSE)
        cash = broker.getCash()
        shares_value = broker.getSharesValue(PriceType.CLOSE)  # 每日统计总市值时采用收盘价格
        position_ratio = shares_value * 1.0 / total_equity
        assert total_equity == cash + shares_value

        rec = TotalPositionRecord(
            trade_dt=datetime_str,
            equity=total_equity,
            value=shares_value,
            cash=cash,
            position=position_ratio
        )
        # 每日总资产跟踪
        self.__total_position_tracker.append(rec)

    def track_total_position2(self, strat, bars):
        """
        包含期货相关数据
        """
        datetime_str = bars.getDateTime().strftime("%Y%m%d")
        broker: HedgeBroker = strat.getBroker()
        total_equity = broker.getEquity(PriceType.CLOSE)
        cash = broker.getCash()
        shares_value = broker.getSharesValue(PriceType.CLOSE)  # 每日统计总市值时采用收盘价格
        assert total_equity == cash + shares_value

        future_value = broker.getFutureValue(PriceType.CLOSE)
        # 根据收盘价格计算当天盈亏。注意，若回测时使用收盘价作为交易价格，此项应当为0
        future_profit = broker.getFutureProfit(PriceType.CLOSE)
        cash += future_profit

        total_equity = shares_value + cash

        position_ratio = shares_value * 1.0 / total_equity
        if shares_value == 0:
            hedge_ratio = np.nan
        else:
            hedge_ratio = - future_value / shares_value

        rec = TotalPositionRecord2(
            trade_dt=datetime_str,
            equity=total_equity,
            stock_value=shares_value,
            cash=cash,
            position=position_ratio,
            future_value=future_value,
            future_profit=future_profit,
            hedge_ratio=hedge_ratio
        )
        # 每日总资产跟踪
        self.__total_position_tracker.append(rec)


    def track_detailed_position(self, strat, bars):
        """
        记录调用当天的持仓信息
        """
        broker = strat.getBroker()
        datetime_str = bars.getDateTime().strftime("%Y-%m-%d")
        sharesDict = copy.deepcopy(broker.getPositions())
        for instrument, shares in sharesDict.items():
            sec_name = bars[instrument].getSecName() # 持仓应当包含在当日行情中，否则应抛出错误
            # 获取当天的股票持仓，当存在持仓时输出该股票的具体信息
            shares = broker.getShares(instrument)
            if shares == 0:
                continue
            shares_sellable = broker.getSharesCanSell(instrument)
            open_price = bars[instrument].getOpen()
            close_price = bars[instrument].getClose()
            last_buy_time = broker.getLastBuyTime(instrument)
            last_sell_time = broker.getLastSellTime(instrument)
            position_cost = broker.getPositionCost(instrument)
            position_pnl = broker.getPositionDelta(bars, instrument)

            pos_rec = DetailedPositionRecord(
                trade_dt=datetime_str,
                windcode=instrument,
                sec_name=sec_name,
                position=shares,
                sellable=shares_sellable,
                open_price=open_price,
                close_price=close_price,
                last_buy=last_buy_time,
                last_sell=last_sell_time,
                cost=position_cost,
                pnl=position_pnl
            )

            self.__detailed_position_tracker.append(pos_rec)

    def __track(self, func, level, strat, bars):
        datetime_str = bars.getDateTime().strftime("%Y%m%d")
        if level == TrackLevel.TRADE_DAY:
            # 在交易日记录个股持仓
            if datetime_str in strat.get_trade_date():
                func(strat, bars)
        elif level == TrackLevel.EVERYDAY:
            # 每天都记录个股持仓
            func(strat, bars)
        elif level == TrackLevel.LAST_TRADE_DAY:
            # 在最后一个交易日记录个股持仓
            if datetime_str == strat.get_trade_date()[-1]:
                func(strat, bars)
        else:
            pass

    def beforeOnBars(self, strat, bars):
        pass

    def after_on_bars(self, strat, bars):
        """
        该函数在每个bars处理完后调用，用来统计今天的结果及输出
        """
        self.__logger.info("after on bars")

        if self.__strategy.config.position_cost_type == PositionCostType.TRADE_DATE:
            # 仅在换仓日刷新持仓成本
            self.__track(lambda x, y: self.__strategy.getBroker().refresh_position_cost(), TrackLevel.TRADE_DAY,
                         strat, bars)

        if self.__hedge_strategy:
            self.track_total_position2(strat, bars)
        else:
            self.track_total_position(strat, bars)

        self.__track(self.track_detailed_position, self.__detailed_position_track_level, strat, bars)
        self.__track(self.track_position, self.__position_track_level, strat, bars)

    @property
    def total_position_records(self):
        """
        class PositionRecord:
            trade_dt: str   # 交易日期
            equity: float   # 总资产
            value: float    # 总市值
            cash: float     # 总现金
            position: float # 总仓位
        """
        if not self.__hedge_strategy:
            data = dataclass_list_to_dataframe(self.__total_position_tracker, TotalPositionRecord)
            data.rename(columns={
                "trade_dt": "日期",
                "equity": "总资产",
                "value": "总市值",
                "cash": "总现金",
                "position": "总仓位"
            }, inplace=True)
        else:
            data = dataclass_list_to_dataframe(self.__total_position_tracker, TotalPositionRecord2)
            data.rename(columns={
                "trade_dt": "日期",
                "equity": "总资产",
                "stock_value": "股票总市值",
                "cash": "总现金",
                "position": "总仓位",
                "future_value": "期货头寸",
                "future_profit": "期货盈亏",
                "hedge_ratio": "对冲比率"
            }, inplace=True)
        return data

    @property
    def position_records(self):
        """
        class PositionRecord:
            trade_dt: str  # 交易日期
            windcode: str  # 股票代码
            volume: str  # 持仓数量
        """
        data = dataclass_list_to_dataframe(self.__position_tracker, PositionRecord)
        data.rename(columns={
            "trade_dt": "交易日期",
            "windcode": "股票代码",
            "volume": "持仓数量",
            "value": "市值"
        }, inplace=True)
        return data

    @property
    def detailed_position_records(self):
        """
        class DetailedPositionRecord:
            trade_dt: str       # 交易日期
            windcode: str       # 证券代码
            sec_name: str       # 证券名称
            position: int       # 持仓数目
            sellable: int       # 可卖数目
            open_price: float   # 最新开盘价格
            close_price: float  # 最新收盘价格
            last_buy: str       # 最近买入
            last_sell: str      # 最近卖出
            cost: float         # 持仓成本
            pnl: float          # 持仓盈亏
        """
        data = dataclass_list_to_dataframe(self.__detailed_position_tracker, DetailedPositionRecord)
        data.rename(columns={
            "trade_dt": "交易日期",
            "windcode": "证券代码",
            "sec_name": "证券名称",
            "position": "持仓数目",
            "sellable": "可卖数目",
            "open_price": "最新开盘价格",
            "close_price": "最新收盘价格",
            "last_buy": "最近买入",
            "last_sell": "最近卖出",
            "cost": "持仓成本",
            "pnl": "持仓盈亏"
        }, inplace=True)
        return data

    @property
    def transaction_records(self):
        """
        @dataclass(frozen=True)
        class TransactionRecord:
            trade_dt: str       # 交易日期
            windcode: str       # 证券代码
            sec_name: str       # 证券名称
            price: float        # 成交价格
            volume: int         # 成交数量
            commission: float   # 佣金花费
            stamp_tax: float    # 印花税花费
            direction: str      # 成交方向
            note: str           # 备注
        """

        transaction_tracker = self.__strategy.getBroker().transaction_tracker
        data = dataclass_list_to_dataframe(transaction_tracker, TransactionRecord)
        data.rename(columns={
            "trade_dt": "交易日期",
            "windcode": "证券代码",
            "sec_name": "证券名称",
            "price": "成交价格",
            "volume": "成交数量",
            "commission": "佣金花费",
            "stamp_tax": "印花税花费",
            "direction": "成交方向",
            "note": "备注"
        }, inplace=True)
        return data

    @property
    def unfilled_orders(self):
        unfilled_orders = self.__strategy.getBroker().unfilled_orders
        data = dataclass_list_to_dataframe(unfilled_orders, UnfilledOrderInfo)
        data.rename(columns={
            "date_time": "交易时间",
            "order_id": "订单编号",
            "instrument": "股票代码",
            "remaining": "未成交数量",
            "direction": "方向",
            "info": "原因"
        }, inplace=True)
        return data


def dataclass_list_to_dataframe_with_annotation(list_data, record_type: type(BaseRecordType)):
    data = dataclass_list_to_dataframe(list_data, record_type, exclude=['annotation_', 'index_'])
    # anno = record_type.annotation_
    data.rename(columns=record_type.annotation_, inplace=True)
    if record_type.index_:
        data.set_index(record_type.index_, inplace=True)
    return data


class StrategyCustomTracker(stratanalyzer.StrategyAnalyzer):
    LOGGER_NAME = 'StrategyCustomTracker'

    def __init__(self):
        super(StrategyCustomTracker, self).__init__()
        self._logger = logger.getLogger(StrategyTracker.LOGGER_NAME, disable=True)
        self._strategy = None
        self._trackers = OrderedDict()
        self._trackers_type = {}

    def add_tracker(self, name, record_type):
        try:
            type_ = self._trackers_type[name]
            raise ValueError(f'Duplicated Tracker `{name}`')
        except KeyError:
            pass

        self._trackers_type[name] = record_type
        self._trackers[name] = deque()

    def track(self, name_, **kwargs):
        try:
            record_cls = self._trackers_type[name_]
        except KeyError:
            raise ValueError(f'Tracker {name_} does not exist')
        self._trackers[name_].append(record_cls(**kwargs))

    def entries(self):
        for name in self._trackers.keys():
            yield name, dataclass_list_to_dataframe_with_annotation(self._trackers[name], self._trackers_type[name])

    def beforeAttach(self, strat):
        pass

    def beforeOnBars(self, strat, bars):
        pass

    def attached(self, strategy):
        self._strategy = strategy

    def after_on_bars(self, strat, bars):
        pass






