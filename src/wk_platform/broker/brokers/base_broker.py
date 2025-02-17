import copy
import typing
from collections import OrderedDict
from collections import deque
from dataclasses import dataclass, field
from pyalgotrade.bar import Frequency
from wk_util import logger
from wk_platform import broker
from wk_platform.broker.fillstrategy import CommonFillStrategy
from wk_platform.config import StrategyConfiguration, PositionCostType, HedgeStrategyConfiguration, \
    TradeRule, PriceType  # , PriceType
from wk_platform.stratanalyzer.record import UnfilledOrderInfo, TransactionRecord
from wk_data.constants import SuspensionType
from wk_platform.feed.bar import StockBar, StockIndexFutureBar
# from wk_platform.util import FutureUtil
# from wk_platform.strategy.position import Position
from wk_platform.broker.commission import *
from wk_platform.broker import Order
from wk_platform.broker.order import MarketOrder
from wk_platform.broker.order import LimitOrder
from wk_platform.broker.order import StopOrder
from wk_platform.broker.order import StopLimitOrder
from wk_platform.broker.brokers.base import BrokerStatus, BaseBacktestBroker


class Broker(BaseBacktestBroker):
    """
    券商类的具体实现
    """

    @dataclass
    class ShareRecord:
        quantity: int
        price: float
        update_date: str

    LOGGER_NAME = "broker.backtesting"

    def __init__(self, bar_feed, config=StrategyConfiguration()):
        super(Broker, self).__init__(bar_feed, config)

        cash = config.initial_cash
        self.__config = config

        assert (cash >= 0)

        self.__useAdjustedValues = True

        self.__unfilled_orders = deque()

        """
        将activeOrders从字典类改为有序字典
        """
        self.__activeOrders = OrderedDict()

        self.__fillStrategy = CommonFillStrategy(config.volume_limit, config.trade_rule)

        self.__logger = logger.getLogger(Broker.LOGGER_NAME, disable=True)

        # self.__share_records: dict[str, Broker.ShareRecord] = {}
        """
        新增变量：持仓详情记录
        """
        self.__cash = cash
        self.__nextOrderId = 1
        self.__shares: dict[str, float] = {}
        self.__sharesCanSell = {}  # 记录当前可卖持仓
        self.__positionCost = {}  # 记录持仓成本
        self.__lastBuyTime = {}  # 记录最近买入时间
        self.__lastSellTime = {}  # 记录最近卖出时间
        self.__amountTotal = {}  # 累计买入金额
        self.__positionDelta = {}  # 记录持仓盈亏

        # It is VERY important that the broker subscribes to barfeed events before the strategy.
        bar_feed.getNewValuesEvent().subscribe(self.onBars)

        self.__barFeed = bar_feed
        self.__allowNegativeCash = False

        self.by_pass_future = True # 用于兼容BrokerV2

    def _getNextOrderId(self):
        ret = self.__nextOrderId
        self.__nextOrderId += 1
        return ret

    def _getBar(self, bars, instrument):
        ret = bars.getBar(instrument)
        if ret is None:
            ret = self.__barFeed.getLastBar(instrument)
        return ret

    def _registerOrder(self, order):
        assert (order.getId() not in self.__activeOrders)
        assert (order.getId() is not None)
        self.__activeOrders[order.getId()] = order

    def _unregisterOrder(self, order):
        assert (order.getId() in self.__activeOrders)
        assert (order.getId() is not None)
        del self.__activeOrders[order.getId()]

    def getLogger(self):
        return self.__logger

    def setAllowNegativeCash(self, allowNegativeCash):
        self.__allowNegativeCash = allowNegativeCash

    def getCash(self, includeShort=True):
        """
        获取当前现金总额
        """
        ret = self.__cash
        if not includeShort and self.__barFeed.getCurrentBars() is not None:
            # 处理允许卖空的情况
            bars = self.__barFeed.getCurrentBars()
            for instrument, share in self.__shares.items():
                if share < 0:
                    instrumentPrice = self._getBar(bars, instrument).getClose(self.getUseAdjustedValues())
                    # ret += instrumentPrice * share.quantity
                    ret += instrumentPrice * share
        return ret

    def setCash(self, cash):
        self.__cash = cash

    def setFillStrategy(self, strategy):
        """Sets the :class:`wk_pyalgotrade.broker.fillstrategy.FillStrategy` to use."""
        self.__fillStrategy = strategy

    def getFillStrategy(self, bar=None):
        """Returns the :class:`wk_pyalgotrade.broker.fillstrategy.FillStrategy` currently set."""
        return self.__fillStrategy

    def getActiveOrders(self, instrument=None):
        if instrument is None:
            ret = list(self.__activeOrders.values())
        else:
            ret = [order for order in list(self.__activeOrders.values()) if order.getInstrument() == instrument]
        return ret

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
        return [instrument for instrument, shares in self.__shares.items() if shares != 0]

    def getShares(self, instrument):
        """
        获取具体股票的持仓数
        """
        return self.__shares.get(instrument, 0)

    def setShareZero(self, instrument):
        """
        清仓一只股票,20180107
        """
        if instrument in self.__shares:
            del self.__shares[instrument]

    def clean_position(self, bars, instrument: str):
        """
        清仓股票，仅适用于退市的情况
        不通过常规的order执行流程，没有order信息
        """
        bar_ = bars.getBar(instrument)
        date_str = bar_.getDateTime().strftime("%Y-%m-%d")
        assert bar_.getOpen() == 0, f"{instrument} {date_str}"
        sec_name = bar_.getSecName()

        self.setShareZero(instrument)
        shares = self.getShares(instrument)
        self._append_transaction(
            date_str,
            instrument,
            sec_name,
            0,
            shares,
            0,
            0,
            "退市卖出"
        )
        # self.__transaction_tracker.append(trans_rec)

    def transform_shares(self, bars, old_instrument, new_instrument, ratio):
        old_shares = self.getShares(old_instrument)
        if old_shares == 0:
            self.setShareZero(old_instrument)
            return
        new_share = int(ratio * old_shares)

        bar1 = bars.getBar(old_instrument)
        bar2 = bars.getBar(new_instrument)

        date_str = bar1.getDateTime().strftime("%Y-%m-%d")
        # sec_name = bar1.getSecName()

        self.setShareZero(old_instrument)
        self.__shares[new_instrument] = new_share

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
        self.__cash += delta_cash

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
        return self.__shares

    def getSharesAmount(self, instrument, bars, price_type=None):
        """
        获取具体股票的持仓金额，根据price_type选择使用的价格
        """
        if price_type is None:
            price_type = self.__config.price_type

        amount = self.__shares.get(instrument, 0)
        price = bars[instrument].get_price(price_type)
        return amount * price  # self.__shares.get(instrument, 0) * bars[instrument].get_price(price_type)

    def __getEquityWithBarsPre(self, bars):
        """
        获取收盘前总资产，每日调仓时需要用到该接口，使用Open price计算当前总资产，规避未来函数
        2017/10/09
        """
        ret = self.getCash()
        if bars is not None:
            for instrument, shares in self.__shares.items():
                instrumentPrice = self._getBar(bars, instrument).getOpen(self.getUseAdjustedValues())
                ret += instrumentPrice * shares
        return ret

    # """
    # 获取盘前总资产，用户调用接口 2017/10/09
    # """
    # def getEquityPre(self):
    #     return self.__getEquityWithBarsPre(self.__barFeed.getCurrentBars())

    def __getEquityWithBars(self, bars):
        """
        获取总资产 （收盘价）
        """
        ret = self.getCash()

        if bars is not None:
            for instrument, shares in self.__shares.items():
                instrumentPrice = self._getBar(bars, instrument).getClose(self.getUseAdjustedValues())
                ret += instrumentPrice * shares

        return ret

    """
    获取总资产（收盘价），用户调用接口
    """

    def getEquity(self, price_type=None):
        """Returns the portfolio value (cash + shares)."""
        if price_type is None:
            price_type = self.__config.price_type
        return self.get_total_equity(price_type)

    def get_total_equity(self, price_type=None):
        if price_type is None:
            price_type = self.__config.price_type
        stock_value = self.getSharesValue(price_type)
        return stock_value + self.getCash()

    """
    获取总市值，不包含现金(收盘价)
    """

    def __getEquityWithoutCash(self, bars, price_type=None):
        if price_type is None:
            price_type = self.__config.price_type
        ret = 0

        if bars is not None:
            for instrument, shares in self.__shares.items():
                instrumentPrice = self._getBar(bars, instrument).getClose(self.getUseAdjustedValues())
                ret += instrumentPrice * shares
                # print instrument, shares, instrumentPrice
        return ret

    """
    获取总市值，不包含现金(收盘价)，用户调用接口
    """

    def getSharesValue(self, price_type=None):
        if price_type is None:
            price_type = self.__config.price_type

        ret = 0
        bars = self.__barFeed.getCurrentBars()
        if bars is None:
            return ret
        for instrument, shares in self.__shares.items():
            instrumentPrice = self._getBar(bars, instrument).get_price(price_type, self.use_adjusted_values)
            ret += instrumentPrice * shares
        return ret

    @property
    def unfilled_orders(self):
        return self.__unfilled_orders

    """
    返回当天可卖股数， T+1规则
    """

    def getSharesCanSell(self, instrument):
        return self.__sharesCanSell.get(instrument, 0)

    """
    返回具体股票的持仓成本
    持仓成本计算（花在这只股票上的所有金额/当前持仓数目）
    """

    def getPositionCost(self, instrument):

        if self.getShares(instrument) == 0:
            return 0
        else:
            return self.__amountTotal.get(instrument, 0) / self.getShares(instrument)

    def refresh_position_cost(self):
        """
        根据当前持仓和最新价格刷新持仓成本
        """
        self.__amountTotal = {}
        bars = self.__barFeed.getCurrentBars()
        for k, v in self.__shares.items():
            self.__amountTotal[k] = v * bars[k].get_price(self.__config.price_type)

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
        return self.__lastBuyTime.get(instrument, 0)

    """
    返回最近卖出时间
    """

    def getLastSellTime(self, instrument):
        return self.__lastSellTime.get(instrument, 0)

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
        if self.__config.position_cost_type == PositionCostType.ACCUMULATION:
            if inst in self.__amountTotal:
                self.__amountTotal[inst] += (cost * (-1))
            else:
                self.__amountTotal[inst] = (cost * (-1))
        else:
            try:
                self.__amountTotal[inst] = self.__shares[inst] * price
            except KeyError:
                pass

    def commitOrderExecution(self, order, dateTime, sec_name, fillInfo):
        """
        Tries to commit an order execution.
        订单处理类，重要函数
        """
        price = fillInfo.getPrice()
        quantity = fillInfo.getQuantity()

        # 新增记录买入买出方向
        action_tag = '买入' if order.isBuy() else '卖出'

        if order.isBuy():
            cost = price * quantity * -1
            assert cost < 0
            sharesDelta = quantity

        elif order.isSell():
            cost = price * quantity
            assert cost > 0
            sharesDelta = quantity * -1

        else:  # Unknown action
            assert False

        self.__logger.info("commitOrderExecution, price is %f, quantity is %d, cost is %f" % (price, quantity, cost))

        """
        计算订单成交所需要的佣金
        """
        commission = self.getCommission().calculate(order, price, quantity)
        self.__logger.info("commission is %f" % commission)

        """
        计算订单成交所需的印花税   2017/10/26
        """
        if order.isSell():
            # commissionTaxFee = self.getCommissionTaxFee().calculate(order, price, quantity)
            commissionTaxFee = self.stamp_tax.calculate(order, price, quantity)
            self.__logger.info("commissionTaxFee is %f" % (commissionTaxFee))
        else:
            commissionTaxFee = 0
            self.__logger.info("commissionTaxFee is %f" % (commissionTaxFee))

        """
        加上佣金成本，计算执行该订单后的剩余现金
        """
        cost -= commission
        """
        加上印花税, 2017/10/26
        """
        cost -= commissionTaxFee

        """
        获取剩余现金
        """
        resultingCash = self.getCash() + cost

        self.__logger.info("original cash is %f" % (self.getCash()))
        self.__logger.info("resulting cash is %f" % (resultingCash))

        """
        allOrNone为False时，计算最大可买入的量
        chenxiangdong 20170720s
        此处的修改需要反复验证
        剩余现金不够时才触发此处  20171030
        """
        if order.isBuy() and resultingCash < 0 and self.__config.adapt_quantity and order.getAllOrNone() is False:

            """
            因为扣除佣金税费后能购买的数量下降，因此在此处需要做一个估算
            """
            factor = self.getCash() / (- cost) * 0.999  # 0.999用于防止舍入误差
            quantity = (self.getCash() * factor) / price
            quantity = self.adjusted_shares(quantity)
            sharesDelta = quantity

            commission = self.getCommission().calculate(order, price, sharesDelta)
            cost = price * sharesDelta * (-1)
            cost -= commission

            """
            卖出时需要加上印花税计算 2017/10/26
            """
            if order.isSell():
                # commissionTaxFee = self.getCommissionTaxFee().calculate(order, price, sharesDelta)
                commissionTaxFee = self.stamp_tax.calculate(order, price, quantity)
            else:
                commissionTaxFee = 0

            cost -= commissionTaxFee

            resultingCash = self.getCash() + cost
            self.__logger.info('when getAllOrNone, the quantity is %d, sharesDelta is %d, resulting Cash is %f' % (
            quantity, sharesDelta, resultingCash))

        """
        正常结算流程
        """
        # Check that we're ok on cash after the commission.
        # if resultingCash >= 0 or self.__allowNegativeCash:
        if (resultingCash >= 0 or self.__allowNegativeCash) and quantity > 0:
            # Update the order before updating internal state since addExecutionInfo may raise.
            # addExecutionInfo should switch the order state.
            orderExecutionInfo = broker.OrderExecutionInfo(price, quantity, commission, dateTime)
            order.addExecutionInfo(orderExecutionInfo)

            dateTimeTemp = dateTime.strftime("%Y-%m-%d")
            """
            添加一次交易流水记录
            """
            self._append_transaction(
                dateTimeTemp,
                order.getInstrument(),
                sec_name,
                price,
                quantity,
                commission,
                commissionTaxFee,
                action_tag, note=order.msg
            )
            # self.__transaction_tracker.append(trans_rec)

            # Commit the order execution.
            """
            更新当前剩余现金
            """
            self.__cash = resultingCash

            """
            更新当前持仓数目
            注意先更新持仓
            """
            # 无需在此处重新调整交易量
            # updatedShares = order.getInstrumentTraits().roundQuantity(
            #     self.getShares(order.getInstrument()) + sharesDelta
            # )
            updatedShares = self.getShares(order.getInstrument()) + sharesDelta
            if updatedShares == 0:

                """
                更新完后一只股票持仓为0，则重置其amountTotal为0 2017/10/27
                """
                self.__amountTotal[order.getInstrument()] = 0
                self.__shares[order.getInstrument()] = 0

            else:
                self.__shares[order.getInstrument()] = updatedShares

            """
            更新持仓成本价
            """
            # if order.getInstrument() in self.__amountTotal:
            #     self.__amountTotal[order.getInstrument()] += (cost * (-1))
            # else:
            #     self.__amountTotal[order.getInstrument()] = (cost * (-1))

            self.__update_position_cost(price, order, cost)

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
                self.__sharesCanSell[order.getInstrument()] += sharesDeltaRound
                self.__lastSellTime[order.getInstrument()] = dateTimeTemp

            else:  # 买入状态
                """
                更新最近买入时间
                """
                self.__lastBuyTime[order.getInstrument()] = dateTimeTemp

            """
            触发fillstrategy类的onOrderFilled函数,更新volumeLeft
            """
            # Let the strategy know that the order was filled.
            self.__fillStrategy.onOrderFilled(self, order)

            # Notify the order update
            if order.isFilled():
                self._unregisterOrder(order)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.FILLED, orderExecutionInfo))
            elif order.isPartiallyFilled():
                self.notifyOrderEvent(
                    broker.OrderEvent(order, broker.OrderEvent.Type.PARTIALLY_FILLED, orderExecutionInfo)
                )
            else:
                assert False
        else:
            """
            现金不足的情况，记录日志信息
            """
            self.__logger.warning("Not enough cash to fill %s order [%s] for %s share/s" % (
                order.getInstrument(),
                order.getId(),
                order.getRemaining()
            ))

            self.__record_unfilled_order(order, f"现金不足，现有{self.getCash()}，需{-cost}")

    def submitOrder(self, order):
        if order.isInitial():
            order.setSubmitted(self._getNextOrderId(), self._getCurrentDateTime())
            self._registerOrder(order)
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
                self._unregisterOrder(order)
                order.switchState(broker.Order.State.CANCELED)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "Expired"))
                self.__record_unfilled_order(order, "订单过期")

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
                self._unregisterOrder(order)
                order.switchState(broker.Order.State.CANCELED)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "Expired"))
                self.__record_unfilled_order(order, "订单过期" + ext_msg)

    def __processOrder(self, order, bar_):
        if not self.__preProcessOrder(order, bar_):
            return

        # Double dispatch to the fill strategy using the concrete order type.

        # TODO: 核对限制交易量时未成交表中的记录

        """
        order.process实际调用的是fillstrategy类中的fillMarketOrder和fillLimitOrder
        """
        fillInfo = order.process(self, bar_)
        if fillInfo.getQuantity() != 0:
            self.commitOrderExecution(order, bar_.getDateTime(), bar_.getSecName(), fillInfo)
        else:
            self._unregisterOrder(order)
            order.switchState(broker.Order.State.CANCELED)
            self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "Unfilled"))
            self.__record_unfilled_order(order, fillInfo.getMsg().value)

        if order.isActive():
            self.__postProcessOrder(order, bar_)

    def __record_unfilled_order(self, order, info):
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
            self.__record_unfilled_order(order, "停牌")
        elif order.getQuantity() is None:
            # 暂停上市等使得价格不存在的情况
            if not order.isCanceled():
                # order.switchState(broker.Order.State.CANCELED)
                self.cancelOrder(order)

            self.__record_unfilled_order(order, "无有效价格")
        else:
            if (bar_.getUpDownStatus(self.__config.max_up_down_limit) == 1 and order.isBuy()) \
                    or (bar_.getUpDownStatus(self.__config.max_up_down_limit) == -1 and order.isSell()):
                if not order.isCanceled():
                    self.__logger.warning("%s today up to +10 or -10, can not sell" % (order.getInstrument()))
                    # order.switchState(broker.Order.State.CANCELED)
                    self.cancelOrder(order)
                    info = "涨停" if order.isBuy() else "跌停"
                    self.__record_unfilled_order(order, info)

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
            assert order not in list(self.__activeOrders.values())

    def simplify_position(self):
        """
        从持仓中删去仓位为0的标的
        """
        instruments = [k for k in self.__shares.keys()]
        for inst in instruments:
            if self.__shares[inst] == 0:
                del self.__shares[inst]
                del self.__amountTotal[inst]

    def onBars(self, dateTime, bars):
        # Let the fill strategy know that new bars are being processed.

        self.__logger.info("onBars called")
        self._reset_daily_amount()

        """
        fillStrategy.onBars中进行可成交量volume的计算
        """
        self.__fillStrategy.onBars(self, bars)
        # 新增，具体位置有待商榷，此处需要check
        self.__sharesCanSell = copy.deepcopy(self.__fillStrategy.getVolumeBegin())

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
        super(Broker, self).start()

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

    def cancelOrder(self, order):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")

        self._unregisterOrder(activeOrder)
        activeOrder.switchState(broker.Order.State.CANCELED)
        self.notifyOrderEvent(
            broker.OrderEvent(activeOrder, broker.OrderEvent.Type.CANCELED, "User requested cancellation")
        )

    def export_status(self):
        """导出当前Broker状态，用于日内Broker
        TODO: 导出状态用于checkpoint
        TODO: 保存fill_strategy的状态
        导出信息包括：
        持仓详情，现金
        """

        status = BrokerStatus(
            cash=self.__cash,
            next_order_id=self.__nextOrderId,
            shares=self.__shares,
            shares_can_sell=self.__sharesCanSell,
            position_cost=self.__positionCost,
            last_buy_time=self.__lastBuyTime,
            last_sell_time=self.__lastSellTime,
            amount_total=self.__amountTotal,
            position_delta=self.__positionDelta,
            sell_volume=self.__fillStrategy.sell_volume,
            buy_volume=self.__fillStrategy.buy_volume,
            transaction_tracker=self.transaction_tracker,
            unfilled_order_tracker=self.__unfilled_orders

        )
        return status

    def update_status(self, status: BrokerStatus):
        self.__cash = status.cash
        self.__nextOrderId = status.next_order_id
        self.__shares = status.shares
        self.__sharesCanSell = status.shares_can_sell
        self.__positionCost = status.position_cost
        self.__lastBuyTime = status.last_buy_time
        self.__lastSellTime = status.last_sell_time
        self.__amountTotal = status.amount_total
        self.__positionDelta = status.position_delta
        self.__nextOrderId = status.next_order_id
        self.__fillStrategy.buy_volume = status.buy_volume
        self.__fillStrategy.sell_volume = status.sell_volume
        self.transaction_tracker = status.transaction_tracker
        self.__unfilled_orders = status.unfilled_order_tracker

