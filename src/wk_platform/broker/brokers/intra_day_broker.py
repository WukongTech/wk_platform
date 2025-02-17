import copy
import typing
from collections import OrderedDict
from collections import deque
from dataclasses import dataclass, field
from pyalgotrade.bar import Frequency
from wk_util import logger
from wk_platform import broker
from wk_platform.broker import fillstrategy
from wk_platform.config import StrategyConfiguration, PositionCostType, HedgeStrategyConfiguration #, PriceType
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


class IntraDayBroker(BaseBacktestBroker):
    """
    券商类的具体实现，用于日内交易
    执行日内交易的的流程
    完成吸并重组股票的转换 -> 检查策略要求日内交易 -> 初始化一个日内Broker -> 初始化一个日内行情BarFeed
    -> 将日间Broker的数据同步给日内Broker -> 日内子策略执行 -> 日内Broker将数据同步给日间Broker

    问题与要点：
    1.未成交订单的跨日问题
    """

    @dataclass
    class ShareRecord:
        quantity: int
        price: float
        update_date: str

    LOGGER_NAME = "broker.backtesting"

    def __init__(self, bar_feed, config=StrategyConfiguration()):
        super(IntraDayBroker, self).__init__(bar_feed, config)

        cash = config.initial_cash
        self.__config = config

        assert (cash >= 0)

        self.__use_adjusted_values = True

        self.__unfilled_orders = deque()

        """
        将activeOrders从字典类改为有序字典
        """
        self.__active_orders = OrderedDict()

        self.__fill_strategy = fillstrategy.IntraDayFillStrategy(config.volume_limit)

        self.__logger = logger.getLogger(self.LOGGER_NAME, disable=True)

        self.__status = BrokerStatus(cash=cash, next_order_id=1)

        # It is VERY important that the broker subscribes to barfeed events before the strategy.
        bar_feed.getNewValuesEvent().subscribe(self.onBars)

        self.__bar_feed = bar_feed
        self.__allow_negative_cash = False

    def _get_next_order_id(self):
        ret = self.__status.next_order_id
        self.__status.next_order_id += 1
        return ret

    def _getBar(self, bars, instrument):
        ret = bars.getBar(instrument)
        if ret is None:
            ret = self.__bar_feed.getLastBar(instrument)
        return ret

    def _registerOrder(self, order):
        assert (order.getId() not in self.__active_orders)
        assert (order.getId() is not None)
        self.__active_orders[order.getId()] = order

    def _unregisterOrder(self, order):
        assert (order.getId() in self.__active_orders)
        assert (order.getId() is not None)
        del self.__active_orders[order.getId()]

    def getLogger(self):
        return self.__logger

    def setAllowNegativeCash(self, allowNegativeCash):
        self.__allow_negative_cash = allowNegativeCash

    def getCash(self, includeShort=False):
        """
        获取当前现金总额
        """
        ret = self.__status.cash
        # 暂时不考虑允许卖空的情况
        # if not includeShort and self.__bar_feed.getCurrentBars() is not None:
        #     # 处理允许卖空的情况
        #     bars = self.__bar_feed.getCurrentBars()
        #     for instrument, share in self.__shares.items():
        #         if share < 0:
        #             instrumentPrice = self._getBar(bars, instrument).getClose(self.getUseAdjustedValues())
        #             ret += instrumentPrice * share.quantity
        return ret

    def setCash(self, cash):
        self.__status.cash = cash

    def setFillStrategy(self, strategy):
        """Sets the :class:`wk_pyalgotrade.broker.fillstrategy.FillStrategy` to use."""
        self.__fill_strategy = strategy

    def getFillStrategy(self, bar=None):
        """Returns the :class:`wk_pyalgotrade.broker.fillstrategy.FillStrategy` currently set."""
        return self.__fill_strategy

    def getActiveOrders(self, instrument=None):
        if instrument is None:
            ret = list(self.__active_orders.values())
        else:
            ret = [order for order in list(self.__active_orders.values()) if order.getInstrument() == instrument]
        return ret

    def _getCurrentDateTime(self):
        return self.__bar_feed.getCurrentDateTime()

    def getInstrumentTraits(self, instrument):
        return broker.IntegerTraits()

    def getUseAdjustedValues(self):
        return self.__use_adjusted_values

    def setUseAdjustedValues(self, useAdjusted):
        # Deprecated since v0.15
        if not self.__bar_feed.barsHaveAdjClose():
            raise Exception("The barfeed doesn't support adjusted close values")
        self.__use_adjusted_values = useAdjusted

    def getActiveInstruments(self):
        return [instrument for instrument, shares in self.__shares.items() if shares != 0]

    def getShares(self, instrument):
        """
        获取具体股票的持仓数
        """
        return self.__status.shares.get(instrument, 0)

    def setShareZero(self, instrument):
        """
        清仓一只股票,20180107
        """
        if instrument in self.__status.shares:
            del self.__status.shares[instrument]

    def clean_position(self, bars, instrument: str):
        """
        清仓股票，仅适用于退市的情况
        不通过常规的order执行流程，没有order信息
        """
        bar_ = bars.getBar(instrument)
        date_str = bar_.getDateTime().strftime("%Y-%m-%d")
        assert bar_.getOpen() == 0
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
        self.__status.shares[new_instrument] = new_share

        self._append_transaction(
            date_str,
            old_instrument,
            bar1.getSecName(),
            bar1.getOpen(),
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
            bar2.getOpen(),
            new_share,
            0,
            0,
            "换入"
        )
        # self.__transaction_tracker.append(trans_rec)

    def getPositions(self):
        """
        获取持仓状态
        """
        return self.__status.shares

    def getSharesAmount(self, instrument, bars, price_type=None):
        """
        获取具体股票的持仓金额，用bar.open统计 2017/10/09
        """
        if price_type is None:
            price_type = self.__config.price_type

        amount = self.__status.shares.get(instrument, 0)
        price = bars[instrument].get_price(price_type)
        return amount * price  # self.__shares.get(instrument, 0) * bars[instrument].get_price(price_type)

    def getEquity(self, price_type=None):
        """Returns the portfolio value (cash + shares)."""
        return self.get_total_equity(price_type)

    def get_total_equity(self, price_type=None):
        if price_type is None:
            price_type = self.__config.price_type
        stock_value = self.getSharesValue(price_type)
        return stock_value + self.getCash()

    def getSharesValue(self, price_type=None):
        """
        获取总市值，不包含现金，用户调用接口
        """
        if price_type is None:
            price_type = self.__config.price_type

        ret = 0
        bars = self.__bar_feed.getCurrentBars()
        if bars is None:
            return ret
        for instrument, shares in self.__status.shares.items():
            instrument_price = self._getBar(bars, instrument).get_price(price_type, self.use_adjusted_values)
            ret += instrument_price * shares
        return ret

    @property
    def unfilled_orders(self):
        return self.__unfilled_orders

    def getSharesCanSell(self, instrument):
        """
        返回当天可卖股数， T+1规则
        """
        return self.__status.shares_can_sell.get(instrument, 0)

    def getPositionCost(self, instrument):
        """
        返回具体股票的持仓成本
        持仓成本计算（花在这只股票上的所有金额/当前持仓数目）
        """
        if self.getShares(instrument) == 0:
            return 0
        else:
            return self.__status.amount_total.get(instrument, 0) / self.getShares(instrument)

    def refresh_position_cost(self):
        """
        根据当前持仓和最新价格刷新持仓成本
        """
        self.__status.amount_total = {}
        bars = self.__bar_feed.getCurrentBars()
        for k, v in self.__shares.items():
            self.__amountTotal[k] = v * bars[k].get_price(self.__config.price_type)

    def get_pnl(self, bars, instrument):
        """
        获取持仓盈亏
        """
        position_cost = self.getPositionCost(instrument)
        current_price = bars[instrument].get_price(self.__config.price_type)
        if position_cost == 0:
            return 0
        return (current_price - position_cost) / position_cost

    def getLastBuyTime(self, instrument):
        """
        返回最近买入时间
        """
        return self.__status.last_buy_time.get(instrument, 0)

    def getLastSellTime(self, instrument):
        """
        返回最近卖出时间
        """
        return self.__status.last_sell_time.get(instrument, 0)

    @property
    def price_type(self):
        return self.__config.price_type

    def adjusted_shares(self, shares):
        """
        A股最小买入下单股数为100股，1手
        chenxiangdong, 20170720
        """
        if self.__config.whole_batch_only:  # 限制整手买入时对可买股数进行调整
            return (shares // 100) * 100
        else:
            return int(shares)

    def __update_position_cost(self, price, order, cost):
        """
        更新持仓成本价
        """
        inst = order.getInstrument()
        amount_total = self.__status.amount_total
        if self.__config.position_cost_type == PositionCostType.ACCUMULATION:
            if inst in amount_total:
                amount_total[inst] += (cost * (-1))
            else:
                amount_total[inst] = (cost * (-1))
        else:
            try:
                amount_total[inst] = self.__status.shares[inst] * price
            except KeyError:
                pass

    def commitOrderExecution(self, order, dateTime, sec_name, fillInfo):
        """
        订单处理类，重要函数
        """
        # Tries to commit an order execution.
        price = fillInfo.getPrice()
        quantity = fillInfo.getQuantity()

        if order.isBuy():
            cost = price * quantity * -1
            assert cost < 0
            sharesDelta = quantity

            buyOrSell = '买入'
        elif order.isSell():
            cost = price * quantity
            assert cost > 0
            sharesDelta = quantity * -1

            buyOrSell = '卖出'
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
            # assert False

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
        if (resultingCash >= 0 or self.__allow_negative_cash) and quantity > 0:
            # Update the order before updating internal state since addExecutionInfo may raise.
            # addExecutionInfo should switch the order state.
            orderExecutionInfo = broker.OrderExecutionInfo(price, quantity, commission, dateTime)
            order.addExecutionInfo(orderExecutionInfo)

            dt_str = dateTime.strftime("%Y-%m-%d %H:%M")

            # 添加一次交易流水记录
            self._append_transaction(
                dt_str,
                order.getInstrument(),
                sec_name,
                price,
                quantity,
                commission,
                commissionTaxFee,
                buyOrSell, note=order.msg
            )
            # self.__transaction_tracker.append(trans_rec)

            # Commit the order execution.
            """
            更新当前剩余现金
            """
            self.__status.cash = resultingCash

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
                self.__status.amount_total[order.getInstrument()] = 0
                self.__status.shares[order.getInstrument()] = 0

            else:
                self.__status.shares[order.getInstrument()] = updatedShares

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
                self.__status.shares_can_sell[order.getInstrument()] += sharesDeltaRound
                self.__status.last_sell_time[order.getInstrument()] = dateTime

            else:  # 买入状态
                """
                更新最近买入时间
                """
                self.__status.last_buy_time[order.getInstrument()] = dateTime

            """
            触发fillstrategy类的onOrderFilled函数,更新volumeLeft
            """
            # Let the strategy know that the order was filled.
            self.__fill_strategy.onOrderFilled(self, order)

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
            order.setSubmitted(self._get_next_order_id(), self._getCurrentDateTime())
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
            if self.__bar_feed.getFrequency() >= Frequency.DAY:
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

    """
    执行一个订单的处理
    将其改为外部函数   chenxiangdong 2017/07/19
    """

    def onBarsImpl(self, order, bars):
        # IF WE'RE DEALING WITH MULTIPLE INSTRUMENTS WE SKIP ORDER PROCESSING IF THERE IS NO BAR FOR THE ORDER'S
        # INSTRUMENT TO GET THE SAME BEHAVIOUR AS IF WERE BE PROCESSING ONLY ONE INSTRUMENT.

        """
        若当天没有标的股票的bar信息，则直接跳过
        """
        bar_ = bars.getBar(order.getInstrument())
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
            """
            将无效订单转为CANCELED状态
            """
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
        # Switch from SUBMITTED -> ACCEPTED
        if order.isSubmitted():
            order.setAcceptedDateTime(bar_.getDateTime())
            """
            将订单状态从submitted转为accepted
            """
            order.switchState(broker.Order.State.ACCEPTED)
            self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.ACCEPTED, None))
        if order.isActive():
            # This may trigger orders to be added/removed from __activeOrders.
            self.__processOrder(order, bar_)
        else:
            # If an order is not active it should be because it was canceled in this same loop and it should
            # have been removed.
            assert order.isCanceled()
            assert order not in list(self.__active_orders.values())

    def reduce_position(self):
        """
        从持仓中删去仓位为0的标的
        """
        instruments = [k for k in self.__status.shares.keys()]
        for inst in instruments:
            if self.__status.shares[inst] == 0:
                del self.__status.shares[inst]
                del self.__status.amount_total[inst]

    def onBars(self, dateTime, bars):
        # Let the fill strategy know that new bars are being processed.

        self.__logger.info("onBars called")
        self._reset_daily_amount()

        """
        filStrategy.onBars中进行可成交量volume的计算
        """
        self.__fill_strategy.onBars(self, bars)
        # 新增，具体位置有待商榷，此处需要check
        self.__status.shares_can_sell = copy.deepcopy(self.__fill_strategy.getVolumeBegin())

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
        super(IntraDayBroker, self).start()

    def stop(self):
        pass

    def join(self):
        pass

    def eof(self):
        # If there are no more events in the barfeed, then there is nothing left for us to do since all processing took
        # place while processing barfeed events.
        return self.__bar_feed.eof()

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

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        return LimitOrder(action, instrument, limitPrice, quantity, self.getInstrumentTraits(instrument))

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        return StopOrder(action, instrument, stopPrice, quantity, self.getInstrumentTraits(instrument))

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        return StopLimitOrder(action, instrument, stopPrice, limitPrice, quantity, self.getInstrumentTraits(instrument))

    def cancelOrder(self, order):
        activeOrder = self.__active_orders.get(order.getId())
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
        导出信息包括：
        持仓详情，现金
        """
        self.__status.sell_volume = self.__fill_strategy.sell_volume
        self.__status.buy_volume = self.__fill_strategy.buy_volume
        self.__status.transaction_tracker = self.transaction_tracker
        self.__status.unfilled_order_tracker = self.__unfilled_orders
        return self.__status

    def update_status(self, status: BrokerStatus):
        self.__status = status
        self.transaction_tracker = status.transaction_tracker
        self.__unfilled_orders = status.unfilled_order_tracker

    def init_fill_strategy(self, bars):
        self.__fill_strategy.init_volume_at_begin(self, bars)
