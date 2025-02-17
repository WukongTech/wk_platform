from __future__ import annotations

# import copy
import typing
from collections import OrderedDict
from collections import deque
from dataclasses import dataclass, field
# from dataclasses import dataclass, field
from enum import Enum

import numpy as np
from pyalgotrade.bar import Frequency

# from wk_platform.broker.brokers.position import FuturePosition
from wk_util import logger
from wk_platform import broker
from wk_platform.broker.fillstrategy import CommonFillStrategy, CommonFillStrategyV2, FutureStrategy
from wk_platform.config import StrategyConfiguration, PositionCostType, PriceType  # , PriceType
from wk_platform.stratanalyzer.record import UnfilledOrderInfo
from wk_data.constants import SuspensionType
from wk_platform.feed.bar import StockBar, StockIndexFutureBar
from wk_platform.util.future import FutureUtil
# from wk_platform.strategy.position import Position
from wk_platform.broker.commission import *
# from wk_platform.broker import Order
from wk_platform.broker.order import MarketOrder
from wk_platform.broker.order import LimitOrder
from wk_platform.broker.order import StopOrder
from wk_platform.broker.order import StopLimitOrder
from wk_platform.broker.brokers.base import BaseBacktestBroker
from wk_platform.broker.brokers.position import StockPosition, FuturePosition, BasePosition
from wk_util.placeholder import need_implemented_func


@dataclass
class BrokerStatus:
    cash: float
    next_order_id: int
    # shares: dict[str, float] = field(default_factory=dict)  # 当前持仓
    # shares_can_sell: dict[str, float] = field(default_factory=dict)  # 记录当前可卖持仓
    # position_cost: dict[str, float] = field(default_factory=dict)  # 记录持仓成本
    # last_buy_time: dict[str, typing.Any] = field(default_factory=dict)  # 记录最近买入时间
    # last_sell_time: dict[str, typing.Any] = field(default_factory=dict)  # 记录最近卖出时间
    # amount_total: dict[str, typing.Any] = field(default_factory=dict)  # 累计买入金额
    # position_delta: dict[str, typing.Any] = field(default_factory=dict)  # 记录持仓盈亏
    # buy_volume: dict[str, int] = field(default_factory=dict)  # 记录买入数量
    # sell_volume: dict[str, int] = field(default_factory=dict)  # 记录卖出数量
    transaction_tracker: deque = field(default_factory=deque)
    unfilled_order_tracker: deque = field(default_factory=deque)


class NoPositionException(Exception):
    def __init__(self, instrument):
        self.instrument = instrument

    def __str__(self):
        return f"{self.instrument}不在持仓中"


class MixedPositionMixin(object):
    def __init__(self, by_pass_future=True):
        self.__positions: {str: BasePosition} = {}
        self.__by_pass_future = by_pass_future

    @property
    def by_pass_future(self):
        return self.__by_pass_future

    @by_pass_future.setter
    def by_pass_future(self, value):
        self.__by_pass_future = value

    def get_stock_list(self):
        """
        获取持仓中的所有个股
        """
        stocks = []
        if self.by_pass_future:
            return self.__positions.keys()
        for k, v in self.__positions.items():
            if isinstance(v, StockPosition):
                stocks.append(k)
        return stocks

    def get_stock_dict(self):
        stock_items = {}
        if self.by_pass_future:
            return self.__positions
        for k, v in self.__positions.items():
            if isinstance(v, StockPosition):
                stock_items[k] = v
        return stock_items

    def get_future_list(self):
        """
        获取持仓中的所有期货
        """
        futures = []
        if self.by_pass_future:
            return futures
        for k, v in self.__positions.items():
            if isinstance(v, FuturePosition):
                futures.append(k)
        return futures

    def get_future_dict(self):
        """
        获取持仓中的所有期货
        """
        future_items = {}
        if self.by_pass_future:
            return future_items
        for k, v in self.__positions.items():
            if isinstance(v, FuturePosition):
                future_items[k] = v
        return future_items

    def get_position(self, instrument) -> BasePosition | None:
        try:
            return self.__positions[instrument]
        except KeyError:
            raise NoPositionException(instrument)

    def get_or_open_position(self, instrument, position_cls):
        try:
            return self.__positions[instrument]
        except KeyError:
            position = position_cls(windcode=instrument, quantity=0)
            self.__positions[instrument] = position
            return self.__positions[instrument]

    def get_positions(self):
        return self.__positions

    def set_position_zero(self, instrument):
        """
        强制将一个标的的仓位设置为0，不进行任何其他操作
        """
        if instrument in self.__positions:
            del self.__positions[instrument]

    def open_position(self, instrument, position):
        self.__positions[instrument] = position

    def get_quantity(self, instrument):
        try:
            return self.get_position(instrument).quantity
        except NoPositionException:
            return 0
        # if position is not None:
        #     return position.quantity
        # return 0

    def set_quantity(self, instrument, quantity):
        self.__positions[instrument].quantity = quantity

    def simplify_position(self):
        """
        从持仓中删去仓位为0的标的
        """
        instruments = [k for k in self.__positions.keys()]
        for inst in instruments:
            if self.__positions[inst].quantity == 0:
                del self.__positions[inst]


class InstrumentTraitsMixin:
    """
    与标的相关的混入，处理每种标的的特有性质，包括费率，fill_strategy
    """
    def __init__(self):
        self.__commission_mapping: {type: typing.Callable} = {}
        self.__stamp_tax_mapping: {type: typing.Callable} = {}
        self.__fill_strategy_mapping: {type: CommonFillStrategy} = {}

        self.__default_commission = need_implemented_func
        self.__default_stamp_tax = need_implemented_func
        self.__default_fill_strategy = need_implemented_func

    def get_commission(self, bar):
        try:
            return self.__commission_mapping[type(bar)]
        except KeyError:
            return self.__default_commission

    def get_stamp_tax(self, bar):
        try:
            return self.__stamp_tax_mapping[type(bar)]
        except KeyError:
            return self.__default_stamp_tax

    def get_fill_strategy(self, bar):
        try:
            return self.__fill_strategy_mapping[type(bar)]
        except KeyError:
            return self.__default_fill_strategy

    def fill_strategy_dict(self):
        return self.__fill_strategy_mapping

    def getFillStrategy(self, bar):
        return self.get_fill_strategy(bar)

    def set_commission(self, bar_type, func):
        if bar_type is None:
            self.__default_commission = func
        self.__commission_mapping[bar_type] = func

    def set_stamp_tax(self, bar_type, func):
        if bar_type is None:
            self.__default_stamp_tax = func
        self.__stamp_tax_mapping[bar_type] = func

    def set_fill_strategy(self, bar_type, fill_strategy):
        if bar_type is None:
            self.__default_fill_strategy = fill_strategy
        self.__fill_strategy_mapping[bar_type] = fill_strategy


class OrderManageMixin:
    """

    """
    def __init__(self, config):
        self.__nextOrderId = 1
        self.__unfilled_orders = deque()
        self.__config = config

        """
        将activeOrders从字典类改为有序字典
        """
        self.__activeOrders = OrderedDict()

    def get_next_order_id(self):
        ret = self.__nextOrderId
        self.__nextOrderId += 1
        return ret

    def register_order(self, order):
        # print('call _registerOrder')
        assert (order.getId() not in self.__activeOrders)
        assert (order.getId() is not None)
        self.__activeOrders[order.getId()] = order

    def unregister_order(self, order):
        # print('call _unRegisterOrder')
        assert (order.getId() in self.__activeOrders)
        assert (order.getId() is not None)
        del self.__activeOrders[order.getId()]

    @property
    def active_orders(self):
        return list(self.__activeOrders.values())

    def getActiveOrders(self, instrument=None):
        if instrument is None:
            ret = list(self.__activeOrders.values())
        else:
            ret = [order for order in list(self.__activeOrders.values()) if order.getInstrument() == instrument]
        return ret

    def cancelOrder(self, order):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")

        self.unregister_order(activeOrder)
        activeOrder.switchState(broker.Order.State.CANCELED)
        # noinspection PyUnresolvedReferences
        self.notifyOrderEvent(
            broker.OrderEvent(activeOrder, broker.OrderEvent.Type.CANCELED, "User requested cancellation")
        )

    def record_unfilled_order(self, order, info):
        if not self.__config.tracking_transaction:
            return

        if order.isBuy():
            direction = '买入'
        elif order.isSell():
            direction = '卖出'
        else:
            assert False

        self.__unfilled_orders.append(
            UnfilledOrderInfo(
                order.getSubmitDateTime(),
                order.getInstrument(),
                order.getId(),
                order.getRemaining(),
                direction,
                info
            )
        )

    @property
    def unfilled_orders(self):
        return self.__unfilled_orders


class CashMixin:
    def __init__(self, init_cash, allow_negative_cash):
        assert init_cash > 0
        self.__cash = init_cash
        self.__frozen_cash = 0 # 冻结的保证金
        self.__allow_negative_cash = allow_negative_cash

    @property
    def allow_negative_cash(self):
        return self.__allow_negative_cash

    @allow_negative_cash.setter
    def allow_negative_cash(self, value):
        self.__allow_negative_cash = value

    @property
    def cash(self):
        return self.__cash

    @cash.setter
    def cash(self, value):
        self.__cash = value

    @property
    def frozen_cash(self):
        return self.__frozen_cash

    @frozen_cash.setter
    def frozen_cash(self, value):
        self.__frozen_cash = value

    @property
    def deposit_ratio(self):
        """
        返回保证金占现金的比例

        注意，理论上期货为每日盯市场，由于仅在换仓时计入利润，平时计算保证金/现金比例时需扣除盈亏
        """
        return self.frozen_cash / self.cash


class UtilMixin:
    @staticmethod
    def get_exec_info(order, quantity):
        if order.isBuy():
            shares_delta = quantity
            direction = '买入'
        elif order.isSell():
            shares_delta = quantity * -1
            direction = '卖出'
        else:
            assert False
        return shares_delta, direction


class BrokerV2(
    MixedPositionMixin,
    InstrumentTraitsMixin,
    OrderManageMixin,
    CashMixin,
    UtilMixin,
    BaseBacktestBroker
):
    """
    券商类的具体实现
    """
    class Hook(Enum):
        MARGIN_CALL = 0
        MATURITY = 1
    # @dataclass
    # class ShareRecord:
    #     quantity: int
    #     price: float
    #     update_date: str

    LOGGER_NAME = "broker.backtesting"

    def __init__(self, bar_feed, config=StrategyConfiguration()):
        BaseBacktestBroker.__init__(self, bar_feed, config)
        MixedPositionMixin.__init__(self, )
        InstrumentTraitsMixin.__init__(self)
        OrderManageMixin.__init__(self, config=config)
        CashMixin.__init__(self, init_cash=config.initial_cash, allow_negative_cash=False)
        UtilMixin.__init__(self)

        # self.cash = config.initial_cash
        # assert (cash >= 0)
        self.__config = config



        self.__useAdjustedValues = True

        self.__logger = logger.getLogger(BrokerV2.LOGGER_NAME, disable=True)


        """
        新增变量：持仓详情记录
        """

        # self.__shares: dict[str, float] = {}
        # self.__sharesCanSell = {}  # 记录当前可卖持仓
        # self.__positionCost = {}  # 记录持仓成本
        # self.__lastBuyTime = {}  # 记录最近买入时间
        # self.__lastSellTime = {}  # 记录最近卖出时间
        # self.__amountTotal = {}  # 累计买入金额
        # self.__positionDelta = {}  # 记录持仓盈亏


        # It is VERY important that the broker subscribes to barfeed events before the strategy.
        bar_feed.getNewValuesEvent().subscribe(self.onBars)

        self.__barFeed = bar_feed
        # self.__allowNegativeCash = False
        self.__hooks: {BrokerV2.Hook: typing.Callable} = {}
        # self.__margin_call_handler = need_implemented_func


        # 股票佣金
        if config.commission is None:
            self.set_commission(None, NoCommission())
        else:
            self.set_commission(None, config.commission)

        # 印花税
        if config.stamp_tax is None:
            self.set_stamp_tax(None, NoCommissionTaxFee())
        else:
            self.set_stamp_tax(None, config.stamp_tax)

        self.set_fill_strategy(None, CommonFillStrategyV2(config.volume_limit, config.trade_rule))

        if StockIndexFutureBar in self.__barFeed.bar_types:
            self.set_fill_strategy(StockIndexFutureBar, FutureStrategy(config.volume_limit))
            self.by_pass_future = False

    def register_hook(self, hook: BrokerV2.Hook, func):
        self.__hooks[hook] = func



    def _getBar(self, bars, instrument):
        ret = bars.getBar(instrument)
        if ret is None:
            ret = self.__barFeed.getLastBar(instrument)
        return ret



    def getLogger(self):
        return self.__logger

    def setAllowNegativeCash(self, allowNegativeCash):
        self.allow_negative_cash = allowNegativeCash




    def getCash(self, includeShort=True):
        """
        获取当前现金总额
        """
        return self.cash
        # ret = self.__cash
        # TODO: 空头处理
        # bars = self.current_bars
        # if not includeShort and self.__barFeed.getCurrentBars() is not None:
        #     # 处理允许卖空的情况
        #     bars = self.__barFeed.getCurrentBars()
        #     for instrument, share in self.__shares.items():
        #         if share < 0:
        #             instrumentPrice = self._getBar(bars, instrument).getClose(self.getUseAdjustedValues())
        #             # ret += instrumentPrice * share.quantity
        #             ret += instrumentPrice * share
        # return ret

    def setCash(self, cash):
        self.cash = cash

    # def setFillStrategy(self, strategy):
    #     """Sets the :class:`wk_pyalgotrade.broker.fillstrategy.FillStrategy` to use."""
    #     self.__fillStrategy = strategy
    #
    # def getFillStrategy(self, bar=None):
    #     """Returns the :class:`wk_pyalgotrade.broker.fillstrategy.FillStrategy` currently set."""
    #     return self.__fillStrategy



    def _getCurrentDateTime(self):
        return self.__barFeed.getCurrentDateTime()

    def getInstrumentTraits(self, instrument):
        return broker.IntegerTraits()

    def getUseAdjustedValues(self):
        return self.__useAdjustedValues

    def setUseAdjustedValues(self, useAdjusted):
        # Deprecated since v0.15
        if not self.__barFeed.barsHaveAdjClose():
            raise Exception("The barfeed doesn't support adjusted close values")
        self.__useAdjustedValues = useAdjusted

    def getActiveInstruments(self):
        assert False
        # return [instrument for instrument, shares in self.__shares.items() if shares != 0]

    def setShareZero(self, instrument):
        """
        清仓一只股票,20180107
        """
        # if instrument in self.__shares:
        #     del self.__positions[instrument]
        self.set_position_zero(instrument)

    def clean_position(self, bars, instrument: str, msg="退市卖出"):
        """
        清仓股票，仅适用于退市的情况
        不通过常规的order执行流程，没有order信息
        """
        bar_ = bars.getBar(instrument)
        date_str = bar_.getDateTime().strftime("%Y-%m-%d")
        assert bar_.getOpen() == 0
        sec_name = bar_.getSecName()

        shares = self.get_position(instrument).quantity
        self.set_position_zero(instrument)

        self._append_transaction(
            date_str,
            instrument,
            sec_name,
            0,
            shares,
            0,
            0,
            msg
        )
        # self.__transaction_tracker.append(trans_rec)

    def transform_shares(self, bars, old_instrument, new_instrument, ratio):
        # old_shares = self.getShares(old_instrument)

        old_shares = self.get_quantity(old_instrument)
        if old_shares == 0:
            # self.setShareZero(old_instrument)
            # self.set_position_zero(old_instrument)
            return
        new_share = int(ratio * old_shares)

        bar1 = bars.getBar(old_instrument)
        bar2 = bars.getBar(new_instrument)

        date_str = bar1.getDateTime().strftime("%Y-%m-%d")
        # sec_name = bar1.getSecName()

        # self.setShareZero(old_instrument)
        self.set_position_zero(old_instrument)
        # self.__shares[new_instrument] = new_share
        position = self.get_or_open_position(new_instrument, StockPosition)
        assert isinstance(position, StockPosition)
        position.quantity += new_share
        if position.price == 0:
            position.price = bar2.get_price()


        self._append_transaction(
            date_str,
            old_instrument,
            bar1.getSecName(),
            bar1.get_price(),
            old_shares,
            0,
            0,
            "换出"
        )
        # self.__transaction_tracker.append(trans_rec)

        self._append_transaction(
            date_str,
            new_instrument,
            bar2.getSecName(),
            bar2.get_price(),
            new_share,
            0,
            0,
            "换入"
        )
        # self.__transaction_tracker.append(trans_rec)

    def transform_to_cash(self, bars, inst, price_type=PriceType.CLOSE):
        """
        将对应标的的持仓转换为现金
        """
        old_shares = self.getShares(inst)
        if old_shares == 0:
            self.setShareZero(inst)
            return

        bar = bars.getBar(inst)
        price = bar.get_price(price_type)
        delta_cash = old_shares * price
        self.setShareZero(inst)
        self.cash += delta_cash

        date_str = bar.getDateTime().strftime("%Y-%m-%d")
        self._append_transaction(
            date_str,
            inst,
            bar.getSecName(),
            price,
            old_shares,
            0,
            0,
            "到期转为现金"
        )

    def getPositions(self):
        """
        获取持仓状态
        """
        return self.get_positions()



    def getSharesAmount(self, instrument, bars, price_type=None):
        """
        获取具体股票的持仓金额，根据price_type选择使用的价格
        """
        if price_type is None:
            price_type = self.__config.price_type

        # position = self.__positions[instrument]
        # position = self.get_position(instrument)
        quantity = self.get_quantity(instrument)
        price = bars[instrument].get_price(price_type)
        amount = quantity * price
        return amount

        # amount = self.__shares.get(instrument, 0)
        # price = bars[instrument].get_price(price_type)
        # return amount * price  # self.__shares.get(instrument, 0) * bars[instrument].get_price(price_type)

    # def __getEquityWithBarsPre(self, bars):
    #     """
    #     获取收盘前总资产，每日调仓时需要用到该接口，使用Open price计算当前总资产，规避未来函数
    #     2017/10/09
    #     """
    #     ret = self.getCash()
    #     if bars is not None:
    #         for instrument, shares in self.__shares.items():
    #             instrumentPrice = self._getBar(bars, instrument).getOpen(self.getUseAdjustedValues())
    #             ret += instrumentPrice * shares
    #     return ret

    # """
    # 获取盘前总资产，用户调用接口 2017/10/09
    # """
    # def getEquityPre(self):
    #     return self.__getEquityWithBarsPre(self.__barFeed.getCurrentBars())

    def __getEquityWithBars(self, bars):
        """
        获取总资产 （收盘价）
        """
        assert False
        # ret = self.getCash()
        #
        # if bars is not None:
        #     for instrument, shares in self.__shares.items():
        #         instrumentPrice = self._getBar(bars, instrument).getClose(self.getUseAdjustedValues())
        #         ret += instrumentPrice * shares
        #
        # return ret

    """
    获取总资产（收盘价），用户调用接口
    """

    def getEquity(self, price_type=None):
        """Returns the portfolio value (cash + shares)."""
        if price_type is None:
            price_type = self.__config.price_type
        return self.get_total_equity(price_type)

    def get_total_equity(self, price_type=None):
        # 由于期货每日盯市，总资产只考虑股票和现金
        if price_type is None:
            price_type = self.__config.price_type
        stock_value = self.getSharesValue(price_type)
        return stock_value + self.getCash()

    """
    获取总市值，不包含现金(收盘价)
    """

    # def __getEquityWithoutCash(self, bars, price_type=None):
    #     if price_type is None:
    #         price_type = self.__config.price_type
    #     ret = 0
    #
    #     if bars is not None:
    #         for instrument, shares in self.__shares.items():
    #             instrumentPrice = self._getBar(bars, instrument).getClose(self.getUseAdjustedValues())
    #             ret += instrumentPrice * shares
    #             # print instrument, shares, instrumentPrice
    #     return ret

    """
    获取总市值，不包含现金(收盘价)，用户调用接口
    """

    def getSharesValue(self, price_type=None):
        if price_type is None:
            price_type = self.__config.price_type

        ret = 0
        bars = self.current_bars
        if bars is None:
            return ret
        for instrument, position in self.get_stock_dict().items():
            instrumentPrice = self._getBar(bars, instrument).get_price(price_type, self.use_adjusted_values)
            ret += instrumentPrice * position.quantity
        return ret



    """
    返回当天可卖股数， T+1规则
    """

    def getShares(self, instrument):
        return self.get_quantity(instrument)
        # position = self.get_position(instrument)
        # if position is None:
        #     return 0
        # return position.quantity

    def getSharesCanSell(self, instrument):
        # return self.__sharesCanSell.get(instrument, 0)
        try:
            position = self.get_position(instrument)
            if isinstance(position, StockPosition):
                return position.share_can_sell
            elif isinstance(position, FuturePosition):
                return np.inf
            # return self.get_position(instrument).share_can_sell
        except NoPositionException:
            return 0


    """
    返回具体股票的持仓成本
    持仓成本计算（花在这只股票上的所有金额/当前持仓数目）
    """

    def getPositionCost(self, instrument):
        position = self.get_position(instrument)
        if isinstance(position, FuturePosition):
            # 期货由于每日盯市，成本视为0
            return 0

        if self.getShares(instrument) == 0:
            return 0
        else:
            return position.position_cost / position.quantity

    def refresh_position_cost(self):
        """
        根据当前持仓和最新价格刷新持仓成本
        """
        self.__amountTotal = {}
        bars = self.__barFeed.getCurrentBars()
        for inst, position in self.get_stock_dict().items():
            # self.__amountTotal[k] = v * bars[k].get_price(self.__config.price_type)
            position.amount_total = position.quantity * bars[inst].get_price(self.__config.price_type)
            position.position_cost = position.amount_total

    """
    获取持仓盈亏情况，2017/10/27
    参数goodTillCanceled控制使用open price还是close price计算
    """

    def getPositionDelta(self, bars, instrument, goodTillCanceled=True):
        positionCost = self.getPositionCost(instrument)

        if positionCost == 0:
            return 0
        else:
            if goodTillCanceled:
                priceNow = bars[instrument].getClose()
            else:
                priceNow = bars[instrument].getOpen()

            return (priceNow - positionCost) / float(positionCost)

    def get_pnl(self, bars, instrument):
        """
        获取持仓盈亏
        """
        position_cost = self.getPositionCost(instrument)
        current_price = bars[instrument].get_price(self.__config.price_type)
        if position_cost == 0:
            return 0
        return (current_price - position_cost) / position_cost

    """
    返回最近买入时间
    """

    def getLastBuyTime(self, instrument):
        try:
            self.get_position(instrument).last_sell_time
        except NoPositionException:
            return ''
        # return self.__lastBuyTime.get(instrument, 0)

    """
    返回最近卖出时间
    """

    def getLastSellTime(self, instrument):
        # return self.__lastSellTime.get(instrument, 0)
        try:
            self.get_position(instrument).last_buy_time
        except NoPositionException:
            return ''

    @property
    def price_type(self):
        return self.__config.price_type

    """
    A股最小买入下单股数为100股，1手
    chenxiangdong, 20170720
    """

    def adjusted_shares(self, shares):
        if self.__config.whole_batch_only:  # 限制整手买入时对可买股数进行调整
            return (shares // 100) * 100
        else:
            return int(shares)

    def __update_position_cost(self, price, order, cost):
        """
        更新持仓成本价
        """
        inst = order.getInstrument()
        # position = self.__positions[inst]
        position = self.get_position(inst)
        # if isinstance(position, FuturePosition):
        #     return

        assert isinstance(position, StockPosition)
        if self.__config.position_cost_type == PositionCostType.ACCUMULATION:
            position.amount_total += (cost * (-1))
        else:
            position.amount_total = position.quantity * price
        #     if inst in self.__amountTotal:
        #         self.__amountTotal[inst] += (cost * (-1))
        #     else:
        #         self.__amountTotal[inst] = (cost * (-1))
        # else:
        #     try:
        #         self.__amountTotal[inst] = self.__shares[inst] * price
        #     except KeyError:
        #         pass



    def update_future(self):
        """
        期货每日盯市
        """
        futures_profit = 0
        deposit = 0
        count = 0
        future_list = self.get_future_list()
        if len(future_list) < 1:
            # 持仓中没有期货
            return

        for instrument in future_list:
            # position = self.__positions[instrument]
            position = self.get_position(instrument)
            price = self._get_bar(self.current_bars, instrument).get_price(self.__config.price_type)
            futures_profit += (price - position.point) * position.quantity
            position.point = price
            deposit += abs(position.point * position.quantity * self.__config.deposit)
            count += 1

        self.cash += futures_profit
        self.frozen_cash = deposit

        if self.cash <= self.frozen_cash / self.__config.deposit_cash_ratio:
            # 需要补充保证金
            self.__hooks[BrokerV2.Hook.MARGIN_CALL](self.current_bars, self.cash, self.frozen_cash / self.__config.deposit_cash_ratio)

        # print(self.cash, self.frozen_cash / self.__config.deposit_cash_ratio)

        assert self.cash >= self.frozen_cash / self.__config.deposit_cash_ratio, "保证金不足，回测终止"

        # if self.__cash <= self.__frozen_cash:
        #     action = Order.Action.BUY if position.quantity < 0 else Order.Action.SELL
        #     amount = self.__cash / (self.__config.future_commission + self.__config.deposit) * 0.999
        #     max_quantity = FutureUtil.calc_future_quantity(amount, price)
        #     order = self.createMarketOrder(action, instrument, abs(position.quantity) - max_quantity)
        #     self.submitOrder(order)
        #     self.onBarsImpl(order, self.current_bars)

    def update_deposit(self):
        """
        更新期货保证金
        """
        deposit = 0
        future_list = self.get_future_list()
        previous_deposit_ratio = self.deposit_ratio

        for instrument in future_list:
            # position = self.__positions[instrument]
            position = self.get_position(instrument)
            price = self._get_bar(self.current_bars, instrument).get_price(self.__config.price_type)
            deposit += abs(price * position.quantity) * self.__config.deposit

        # for instrument, position in self.__futures.items():
        #     price = self._get_bar(self.current_bars, instrument).open
        #     deposit += abs(price * position.quantity) * self.__config.deposit
        self.__frozen_cash = deposit
        try:
            assert self.frozen_cash < self.cash
        except Exception as e:
            self.__debug_info()
            print('[previous deposit ratio]', previous_deposit_ratio)
            print('[deposit ratio]', self.deposit_ratio)
            raise e

    def commitOrderExecution(self, bar, order, fillInfo):
        """
        Tries to commit an order execution.
        订单处理类，重要函数
        """
        price = fillInfo.getPrice()
        quantity = fillInfo.getQuantity()

        # 新增记录买入买出方向
        # action_tag = '买入' if order.isBuy() else '卖出'
        #
        # if order.isBuy():
        #     cost = price * quantity * -1
        #     assert cost < 0
        #     sharesDelta = quantity
        #
        # elif order.isSell():
        #     cost = price * quantity
        #     assert cost > 0
        #     sharesDelta = quantity * -1
        #
        # else:  # Unknown action
        #     assert False

        sharesDelta, action_tag = self.get_exec_info(order, quantity)
        amount_delta = -sharesDelta * price

        self.__logger.info("commitOrderExecution, price is %f, quantity is %d, amount_delta is %f" % (price, quantity, amount_delta))

        """
        计算订单成交所需要的佣金
        """
        # commission = self.getCommission().calculate(order, price, quantity)
        commission = self.get_commission(bar)(order, price, quantity)
        self.__logger.info("commission is %f" % commission)

        # """
        # 计算订单成交所需的印花税   2017/10/26
        # """
        # if order.isSell():
        #     # commissionTaxFee = self.getCommissionTaxFee().calculate(order, price, quantity)
        #     commissionTaxFee = self.stamp_tax.calculate(order, price, quantity)
        #     self.__logger.info("commissionTaxFee is %f" % (commissionTaxFee))
        # else:
        #     commissionTaxFee = 0
        #     self.__logger.info("commissionTaxFee is %f" % (commissionTaxFee))

        # 计算印花税
        stamp_tax = self.get_stamp_tax(bar)(order, price, quantity)
        self.__logger.info(f"stamp_tax is {stamp_tax}")

        """
        加上佣金成本，计算执行该订单后的剩余现金
        """
        amount_delta -= commission
        """
        加上印花税, 2017/10/26
        """
        amount_delta -= stamp_tax

        """
        获取剩余现金
        """
        resultingCash = self.getCash() + amount_delta

        self.__logger.info("original cash is %f" % (self.getCash()))
        self.__logger.info("resulting cash is %f" % (resultingCash))

        """
        allOrNone为False时，计算最大可买入的量
        chenxiangdong 20170720s
        此处的修改需要反复验证
        剩余现金不够时才触发此处  20171030
        """
        # t = order.isBuy(), resultingCash < self.frozen_cash, self.__config.adapt_quantity, order.getAllOrNone()
        if order.isBuy() and resultingCash < self.frozen_cash and self.__config.adapt_quantity and order.getAllOrNone() is False:


            # 因为扣除佣金税费后能购买的数量下降，因此在此处需要做一个估算
            # 仅对费率导致的现金不足进行处理

            # factor = self.getCash() / (- amount_delta) * 0.999  # 0.999用于防止舍入误差
            # quantity = (self.getCash() * factor) / price

            factor = abs((self.getCash() - self.frozen_cash) / amount_delta)
            quantity = factor * quantity
            quantity = self.adjusted_shares(quantity)
            sharesDelta = quantity

            # commission = self.getCommission().calculate(order, price, sharesDelta)
            commission = self.get_commission(bar)(order, price, sharesDelta)
            amount_delta = price * sharesDelta * (-1)
            amount_delta -= commission

            """
            卖出时需要加上印花税计算 2017/10/26
            """
            # if order.isSell():
            #     # commissionTaxFee = self.getCommissionTaxFee().calculate(order, price, sharesDelta)
            #     commissionTaxFee = self.stamp_tax.calculate(order, price, quantity)
            # else:
            #     commissionTaxFee = 0

            stamp_tax = self.get_stamp_tax(bar)(order, price, quantity)

            amount_delta -= stamp_tax

            resultingCash = self.getCash() + amount_delta
            self.__logger.info('when getAllOrNone, the quantity is %d, sharesDelta is %d, resulting Cash is %f' % (
            quantity, sharesDelta, resultingCash))

        """
        正常结算流程
        """

        bar_datetime = bar.getDateTime()
        bar_datetime_str = bar_datetime.strftime("%Y-%m-%d")
        sec_name = bar.getSecName()
        instrument = order.getInstrument()
        # Check that we're ok on cash after the commission.
        # if resultingCash >= 0 or self.__allowNegativeCash:
        if (resultingCash < self.frozen_cash and sharesDelta > 0) or quantity == 0:
            # 买入情形现金不足
            self.__logger.warning("Not enough cash to fill %s order [%s] for %s share/s" % (
                order.getInstrument(),
                order.getId(),
                order.getRemaining()
            ))

            self.record_unfilled_order(
                order,
                f"现金不足，现有{self.getCash()}， 可用{self.cash - self.frozen_cash}，需{-amount_delta}"
            )
            return


        orderExecutionInfo = broker.OrderExecutionInfo(price, quantity, commission, bar_datetime)
        order.addExecutionInfo(orderExecutionInfo)

        # dateTimeTemp = dateTime.strftime("%Y-%m-%d")
        """
        添加一次交易流水记录
        """
        self._append_transaction(
                bar_datetime_str,
                instrument,
                sec_name,
                price,
                quantity,
                commission,
                stamp_tax,
                action_tag, note=order.msg
            )

        # Commit the order execution.
        """
        更新当前剩余现金
        """
        self.cash = resultingCash
        """
        更新当前持仓数目
        注意先更新持仓
        """
        # 无需在此处重新调整交易量
        # updatedShares = order.getInstrumentTraits().roundQuantity(
        #     self.getShares(order.getInstrument()) + sharesDelta
        # )
        # updatedShares = self.getShares(instrument) + sharesDelta

        position = self.get_or_open_position(instrument, StockPosition)
        assert isinstance(position, StockPosition)
        updated_quantity = position.quantity + sharesDelta
        if updated_quantity == 0:

            """
            更新完后一只股票持仓为0，则重置其amountTotal为0 2017/10/27
            """
            # self.__amountTotal[order.getInstrument()] = 0
            # self.__shares[order.getInstrument()] = 0
            position = self.get_position(instrument)

            position.quantity = 0
            position.amount_total = 0

        else:
            # self.__shares[order.getInstrument()] = updatedShares
            # self.set_quantity(instrument, updated_quantity)
            position.quantity =  updated_quantity
            position.amount_total =  updated_quantity * price




        """
        更新持仓成本价
        """
        # if order.getInstrument() in self.__amountTotal:
        #     self.__amountTotal[order.getInstrument()] += (cost * (-1))
        # else:
        #     self.__amountTotal[order.getInstrument()] = (cost * (-1))

        self.__update_position_cost(price, order, amount_delta)

        """
        当天可卖数量的更新
        """
        sharesDeltaRound = order.getInstrumentTraits().roundQuantity(sharesDelta)

        if sharesDeltaRound < 0:  # 卖出状态

            # print "sharesDeltaRound is %d"%(sharesDeltaRound)
            # print "original shares are %d, instrument is %s"%(self.__sharesCanSell[order.getInstrument()], order.getInstrument())
            """
            更新当天可卖持仓和最新卖出时间
            """
            # self.__sharesCanSell[order.getInstrument()] += sharesDeltaRound
            # self.__lastSellTime[order.getInstrument()] = dateTimeTemp

            position.share_can_sell += sharesDeltaRound
            position.last_sell_time = bar_datetime_str

        else:  # 买入状态
            """
            更新最近买入时间
            """
            # self.__lastBuyTime[order.getInstrument()] = dateTimeTemp
            position.last_buy_time = bar_datetime_str

        """
        触发fillstrategy类的onOrderFilled函数,更新volumeLeft
        """
        # Let the strategy know that the order was filled.
        # self.__fillStrategy.onOrderFilled(self, order)
        self.get_fill_strategy(bar).onOrderFilled(self, order)

        # Notify the order update
        if order.isFilled():
            self.unregister_order(order)
            self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.FILLED, orderExecutionInfo))
        elif order.isPartiallyFilled():
            self.notifyOrderEvent(
                broker.OrderEvent(order, broker.OrderEvent.Type.PARTIALLY_FILLED, orderExecutionInfo)
            )
        else:
            assert False
        # else:



    def commitFutureOrderExecution(self, bar, order, fillInfo):
        """
        期货订单处理
        """
        price = fillInfo.getPrice()
        quantity = fillInfo.getQuantity()

        # commission = abs(price * quantity * self.__config.future_commission) # 成本为交易手续费
        commission = self.get_commission(bar)(order, price, quantity)
        cost = -commission

        shares_delta, direction = self.get_exec_info(order, quantity)

        instrument = order.getInstrument()

        position = self.get_or_open_position(instrument, FuturePosition)
        position.point = price


        # try:
        #     future_position: HedgeBroker.FuturePosition = self.__futures[order.getInstrument()]
        #     future_position.point = price
        # except KeyError:
        #     future_position = HedgeBroker.FuturePosition(
        #         code=self.__config.code,
        #         windcode=order.getInstrument(),
        #         point=price,
        #         quantity=0
        #     )
        #     self.__futures[order.getInstrument()] = future_position

        # target_quantity = position.quantity + shares_delta
        final_quantity = position.quantity + shares_delta
        net_position_delta = abs(final_quantity) - abs(position.quantity)
        if net_position_delta > 0:
            # 持仓增加
            deposit_requirement = abs(price * shares_delta * self.__config.deposit)
        else:
            deposit_requirement = -abs(price * shares_delta * self.__config.deposit)


        # cash_requirement = deposit + cost  # 原始订单的现金需求量
        # resulting_available_cash = self.cash - cash_requirement
        # ref_cash_requirement = deposit + cost  # 原始订单的现金需求量
        if deposit_requirement + self.frozen_cash > self.cash + cost:
            # 现金/保证金不足
            # amount = self.__cash / (self.__config.future_commission + self.__config.deposit) * 0.999
            # max_amount = abs(target_quantity * price) * self.cash / cash_requirement * 0.999
            # max_quantity = FutureUtil.calc_future_quantity(max_amount, price)
            # try:
            #     # 验证实际持仓小于目标持仓
            #     assert abs(position.quantity) <= max_quantity
            # except Exception as e:
            #     self.__debug_info()
            #     print('[current quantity]', position.quantity)
            #     print('[amount]', max_amount)
            #     print('[max_quantity]', max_quantity)
            #     print('[quantity]', quantity)
            #     print('[order quantity]', fillInfo.getQuantity())
            #     print('[long position]', order.isBuy())
            #     print('[price]', price)
            #     raise e
            # available_quantity = max_quantity - abs(position.quantity)
            # quantity = available_quantity
            # # commission = abs(price * quantity * self.__config.future_commission)  # 成本为交易手续费
            # commission = self.get_commission(bar)(order, price, quantity)
            # cost = -commission
            # shares_delta, direction = self.get_exec_info(order, quantity)
            # new_target_position = position.quantity + shares_delta
            # deposit = abs(price * new_target_position * self.__config.deposit)
            self.cancelOrder(order)
            self.record_unfilled_order(order, f"现金不足，现有{self.getCash()}，已冻结保证金{self.frozen_cash}，需额外保证金{deposit_requirement} 和费用{-cost}")
            return

        # resulting_available_cash = self.cash - deposit + cost
        # try:
        #     assert resulting_available_cash > 0
        # except Exception as e:
        #     self.__debug_info()
        #     print('[quantity]', quantity)
        #     print('[order quantity]', fillInfo.getQuantity())
        #     print('[long position]', order.isBuy())
        #     print('[price]', price)
        #     raise e
        #
        # if quantity == 0:
        #     self.cancelOrder(order)
        #     self.record_unfilled_order(order, f"现金不足，现有{self.getCash()}，需{ref_cash_requirement}")
        #     return



        bar_datetime = bar.getDateTime()
        bar_datetime_str = bar_datetime.strftime("%Y-%m-%d")
        instrument = order.getInstrument()
        sec_name = instrument

        order_execution_info = broker.OrderExecutionInfo(price, quantity, commission, bar_datetime)
        order.addExecutionInfo(order_execution_info)


        position.quantity = final_quantity
        # if future_position.quantity == 0:
        #     del self.__futures[order.getInstrument()]

        """
        更新当前剩余现金
        """
        self.cash = self.cash + cost
        self.frozen_cash += deposit_requirement

        # self.__future_fill_strategy.onOrderFilled(self, order)
        self.get_fill_strategy(bar).onOrderFilled(self, order)

        self._append_transaction(
                bar_datetime_str,
                instrument,
                sec_name,
                price,
                shares_delta,
                commission,
                0,
                direction,
                note=order.msg
            )

        # Notify the order update
        if order.isFilled():
            self.unregister_order(order)
            self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.FILLED, order_execution_info))
        elif order.isPartiallyFilled():
            self.notifyOrderEvent(
                broker.OrderEvent(order, broker.OrderEvent.Type.PARTIALLY_FILLED, order_execution_info)
            )
        else:
            assert False

    def submitOrder(self, order):
        if order.isInitial():
            order.setSubmitted(self.get_next_order_id(), self._getCurrentDateTime())
            self.register_order(order)
            # Switch from INITIAL -> SUBMITTED

            # add logger
            self.__logger.debug("change state to submitted")
            order.switchState(broker.Order.State.SUBMITTED)

            self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.SUBMITTED, None))
        else:
            raise Exception("The order was already processed")

    """
    将已经超时的订单删除
    """

    # Return True if further processing is needed.
    def __preProcessOrder(self, order, bar_):
        ret = True

        # For non-GTC orders we need to check if the order has expired.
        if not order.getGoodTillCanceled():
            # t =
            expired = bar_.getDateTime().date() > order.getAcceptedDateTime().date()

            # Cancel the order if it is expired.
            if expired:
                ret = False
                self.unregister_order(order)
                order.switchState(broker.Order.State.CANCELED)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "Expired"))
                self.record_unfilled_order(order, "订单过期")

        return ret

    def __postProcessOrder(self, order: broker.Order, bar_):
        # For non-GTC orders and daily (or greater) bars we need to check if orders should expire right now
        # before waiting for the next bar.
        if not order.getGoodTillCanceled():
            expired = False
            # if self.__barFeed.getFrequency() >= wk_pyalgotrade.bar.Frequency.DAY:
            if self.__barFeed.getFrequency() >= Frequency.DAY:
                # raise Exception('Check order.getAcceptedDateTime()')
                expired = bar_.getDateTime().date() >= order.getAcceptedDateTime().date()

            # Cancel the order if it will expire in the next bar.
            if expired:
                ext_msg = ''
                if order.getState() == broker.Order.State.PARTIALLY_FILLED:
                    ext_msg = f"：部成订单，总{order.getQuantity()}，未成{order.getRemaining()}"
                self.unregister_order(order)
                order.switchState(broker.Order.State.CANCELED)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "Expired"))
                self.record_unfilled_order(order, "订单过期" + ext_msg)

    def __processOrder(self, order, bar_):
        if not self.__preProcessOrder(order, bar_):
            return

        # Double dispatch to the fill strategy using the concrete order type.

        # TODO: 核对限制交易量时未成交表中的记录

        """
        order.process实际调用的是fillstrategy类中的fillMarketOrder和fillLimitOrder
        """
        # fillInfo = order.process(self, bar_)
        # if fillInfo.getQuantity() != 0:
        #     self.commitOrderExecution(order, bar_.getDateTime(), bar_.getSecName(), fillInfo)
        # else:
        #     self._unregisterOrder(order)
        #     order.switchState(broker.Order.State.CANCELED)
        #     self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "Unfilled"))
        #     self.__record_unfilled_order(order, fillInfo.getMsg().value)
        #
        # if order.isActive():
        #     self.__postProcessOrder(order, bar_)


        fillInfo = order.process(self, bar_)
        if fillInfo.getQuantity() != 0:
            if isinstance(bar_, StockBar):
                # self.commitOrderExecution(order, bar_.getDateTime(), bar_.getSecName(), fillInfo)
                self.commitOrderExecution(bar_, order, fillInfo)
            elif isinstance(bar_, StockIndexFutureBar):
                # self.commitFutureOrderExecution(order, bar_.getDateTime(), bar_.windcode, fillInfo)
                self.commitFutureOrderExecution(bar_, order, fillInfo)
            else:
                self.commitOrderExecution(bar_, order, fillInfo)
                # assert False, f'unsupported Bar type {bar_}'
        else:
            self.unregister_order(order)
            order.switchState(broker.Order.State.CANCELED)
            self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "Unfilled"))
            self.record_unfilled_order(order, fillInfo.getMsg().value)

        if order.isActive():
            self.__postProcessOrder(order, bar_)



    def check_inst_status(self, order, bar_):
        """
        检查订单是否能够执行，考虑因素包括
        1. 没有标的股票的bar信息，则直接跳过
        2. 是否存在涨停无法买入或者跌停无法卖出的情况
        3. 停牌
        """
        # 若当天没有标的股票的bar信息，则直接跳过
        if bar_ is None:
            return

        """
        判断当天能否正常交易
        判断是否存在涨停无法买入或者跌停无法卖出的情况
        """
        # if bar_.getTradeStatus() != 1 or bar_.getNewColumn() != 1:

        if bar_.getTradeStatus() != SuspensionType.NORMAL and self.__config.suspension_limit:
            # 如果当天不可交易且开启停复牌限制
            self.__logger.warning("%s today suspend, can not trade" % (order.getInstrument()))
            # 将无效订单转为CANCELED状态
            if not order.isCanceled():
                # order.switchState(broker.Order.State.CANCELED)
                self.cancelOrder(order)
            self.record_unfilled_order(order, "停牌")
        elif order.getQuantity() is None:
            # 暂停上市等使得价格不存在的情况
            if not order.isCanceled():
                # order.switchState(broker.Order.State.CANCELED)
                self.cancelOrder(order)

            self.record_unfilled_order(order, "无有效价格")
        else:
            if (bar_.getUpDownStatus(self.__config.max_up_down_limit) == 1 and order.isBuy()) \
                    or (bar_.getUpDownStatus(self.__config.max_up_down_limit) == -1 and order.isSell()):
                if not order.isCanceled():
                    self.__logger.warning("%s today up to +10 or -10, can not sell" % (order.getInstrument()))
                    # order.switchState(broker.Order.State.CANCELED)
                    self.cancelOrder(order)
                    info = "涨停" if order.isBuy() else "跌停"
                    self.record_unfilled_order(order, info)

    def onBarsImpl(self, order, bars):
        """
        执行一个订单的处理
        将其改为外部函数   chenxiangdong 2017/07/19
        """
        # IF WE'RE DEALING WITH MULTIPLE INSTRUMENTS WE SKIP ORDER PROCESSING IF THERE IS NO BAR FOR THE ORDER'S
        # INSTRUMENT TO GET THE SAME BEHAVIOUR AS IF WERE BE PROCESSING ONLY ONE INSTRUMENT.
        instrument = order.getInstrument()
        bar_ = bars.getBar(instrument)
        self.check_inst_status(order, bar_)

        # if self.__config.trade_rule == TradeRule.T1 and order.isSell():
        #     # 检查是否当日卖出
        #     if self.__sharesCanSell[instrument] < order.getQuantity():
        #         self.cancelOrder(order)
        #         self.__record_unfilled_order(order, "超过当日可卖数量")

        # Switch from SUBMITTED -> ACCEPTED
        if order.isSubmitted():
            order.setAcceptedDateTime(bar_.getDateTime())

            # 将订单状态从submitted转为accepted
            order.switchState(broker.Order.State.ACCEPTED)
            self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.ACCEPTED, None))

        if order.isActive():
            # This may trigger orders to be added/removed from __activeOrders.
            self.__processOrder(order, bar_)

        else:
            # If an order is not active it should be because it was canceled in this same loop and it should
            # have been removed.
            assert order.isCanceled()
            # assert order not in list(self.__activeOrders.values())
            assert order not in self.active_orders



    def onBars(self, dateTime, bars):
        # Let the fill strategy know that new bars are being processed.

        self.__logger.info("onBars called")
        self._reset_daily_amount()

        """
        fillStrategy.onBars中进行可成交量volume的计算
        """
        # # self.__fillStrategy.onBars(self, bars)
        # self.get_fill_strategy(None).onBars(self, bars)
        # # 新增，具体位置有待商榷，此处需要check
        # # self.__sharesCanSell = copy.deepcopy(self.__fillStrategy.getVolumeBegin())
        # share_can_sell = self.get_fill_strategy(None).getVolumeBegin()
        # self.__sharesCanSell = copy.deepcopy(self.get_fill_strategy(None).getVolumeBegin())

        for k, fill_strategy in self.fill_strategy_dict().items():
            fill_strategy.onBars(self, bars)
            share_can_sell = self.get_fill_strategy(None).getVolumeBegin()
            for inst, volume in share_can_sell.items():
                try:
                    position = self.get_position(inst)
                    if not isinstance(position, FuturePosition):
                        position.share_can_sell = volume
                except NoPositionException:
                    pass

        self.update_future()


        """
        改变订单的处理逻辑，当前逻辑为每天Feed来先触发broker onBars，计算当天允许成交的量，接着进入
        strategy的onBars，执行下单操作，当天下的单在单天执行，实时计算现金和仓位的变化
        """
        """
        # This is to froze the orders that will be processed in this event, to avoid new getting orders introduced
        # and processed on this very same event.
        ordersToProcess = self.__activeOrders.values()


        for order in ordersToProcess:
            # This may trigger orders to be added/removed from __activeOrders.

            self.__logger.info("broker.backtesting.py/call onBarsImpl")
            self.__onBarsImpl(order, bars)
        """

    def start(self):
        super(BrokerV2, self).start()

    def stop(self):
        pass

    def join(self):
        pass

    def eof(self):
        # If there are no more events in the barfeed, then there is nothing left for us to do since all processing took
        # place while processing barfeed events.
        return self.__barFeed.eof()

    def dispatch(self):
        # All events were already emitted while handling barfeed events.
        pass

    def peekDateTime(self):
        return None

    """
    暂时取消对onClose = False的限制
    """

    def createMarketOrder(self, action, instrument, quantity, onClose=False, msg=None):
        # In order to properly support market-on-close with intraday feeds I'd need to know about different
        # exchange/market trading hours and support specifying routing an order to a specific exchange/market.
        # Even if I had all this in place it would be a problem while paper-trading with a live feed since
        # I can't tell if the next bar will be the last bar of the market session or not.

        """
        if onClose is True and self.__barFeed.isIntraday():
            raise Exception("Market-on-close not supported with intraday feeds")
        """

        return MarketOrder(action, instrument, quantity, onClose, self.getInstrumentTraits(instrument), msg=msg)

    def createLimitOrder(self, action, instrument, limitPrice, quantity, inBar=False, msg=None):
        return LimitOrder(action, instrument, limitPrice, quantity, self.getInstrumentTraits(instrument), inBar=inBar, msg=msg)

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        return StopOrder(action, instrument, stopPrice, quantity, self.getInstrumentTraits(instrument))

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        return StopLimitOrder(action, instrument, stopPrice, limitPrice, quantity, self.getInstrumentTraits(instrument))



    def __debug_info(self):
        print('---------- debug info ------------')
        print('[trade date]', self.current_bars.getDateTime())
        print('[market value]', self.getSharesValue())
        print('[future value]', self.getFutureValue())
        print('[current cash]', self.cash)
        print('[frozen cash]', self.frozen_cash)
        print('[total equity]', self.total_equity)

    def export_status(self):
        """导出当前Broker状态，用于日内Broker
        TODO: 导出状态用于checkpoint
        TODO: 保存fill_strategy的状态
        导出信息包括：
        持仓详情，现金
        """

        status = BrokerStatus(
            cash=self.cash,
            next_order_id=self.__nextOrderId,
            # shares=self.__shares,
            # shares_can_sell=self.__sharesCanSell,
            # position_cost=self.__positionCost,
            # last_buy_time=self.__lastBuyTime,
            # last_sell_time=self.__lastSellTime,
            # amount_total=self.__amountTotal,
            # position_delta=self.__positionDelta,
            # sell_volume=self.__fillStrategy.sell_volume,
            # buy_volume=self.__fillStrategy.buy_volume,
            transaction_tracker=self.transaction_tracker,
            unfilled_order_tracker=self.__unfilled_orders

        )
        return status

    def update_status(self, status: BrokerStatus):
        # noinspection DuplicatedCode
        self.cash = status.cash
        self.__nextOrderId = status.next_order_id
        # self.__shares = status.shares
        # self.__sharesCanSell = status.shares_can_sell
        # self.__positionCost = status.position_cost
        # self.__lastBuyTime = status.last_buy_time
        # self.__lastSellTime = status.last_sell_time
        # self.__amountTotal = status.amount_total
        # self.__positionDelta = status.position_delta
        self.__nextOrderId = status.next_order_id
        # self.__fillStrategy.buy_volume = status.buy_volume
        # self.__fillStrategy.sell_volume = status.sell_volume
        self.transaction_tracker = status.transaction_tracker
        self.__unfilled_orders = status.unfilled_order_tracker

