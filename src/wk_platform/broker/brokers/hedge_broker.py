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
from wk_platform.util.future import FutureUtil
# from wk_platform.strategy.position import Position
from wk_platform.broker.commission import *
from wk_platform.broker import Order
from wk_platform.broker.order import MarketOrder
from wk_platform.broker.order import LimitOrder
from wk_platform.broker.order import StopOrder
from wk_platform.broker.order import StopLimitOrder
from wk_platform.broker.brokers.base import BrokerStatus, BaseBacktestBroker


class HedgeBroker(BaseBacktestBroker):
    """
    支持股指期货对冲的券商类
    """

    LOGGER_NAME = "hedge_broker.backtesting"

    @dataclass
    class FuturePosition:
        code: str       # IF，IC
        windcode: str
        quantity: int  # 仓位，正数多头，负数空头
        point: float   # 开仓点数

    def __init__(self, bar_feed, config: HedgeStrategyConfiguration = HedgeStrategyConfiguration()):
        super().__init__(bar_feed, config)

        cash = config.initial_cash
        self.__config: HedgeStrategyConfiguration = config

        assert (cash >= 0)
        self.__cash = cash
        self.__frozen_cash = 0  # 冻结现金，用于保证金

        """
        用来记录当前持仓状态
        """
        self.__shares = {}  #
        self.__futures: {str: HedgeBroker.FuturePosition} = {}

        self.__transaction_tracker: [TransactionRecord] = deque()
        self.__unfilled_orders = deque()

        self.__fillStrategy = fillstrategy.CommonFillStrategy(config.volume_limit, config.trade_rule)
        self.__future_fill_strategy = fillstrategy.FutureStrategy(config.volume_limit)

        self.__logger = logger.getLogger(self.LOGGER_NAME, disable=True)

        """
        新增变量：持仓详情记录
        """
        self.__sharesCanSell = {}  # 记录当前可卖持仓
        self.__positionCost = {}  # 记录持仓成本
        self.__lastBuyTime = {}  # 记录最近买入时间
        self.__lastSellTime = {}  # 记录最近卖出时间
        self.__amountTotal = {}  # 累计买入金额
        self.__positionDelta = {}  # 记录持仓盈亏

        self.__allowNegativeCash = False

        # It is VERY important that the broker subscribes to barfeed events before the strategy.
        self.bar_feed_subscribe(self.onBars)

        # cache
        self.__current_position = None

    def resubscribe(self):
        """
        在bar结束时再次触发onBars
        """
        self.bar_feed_subscribe(self.onBars2)

    def getLogger(self):
        return self.__logger

    def getCash(self, includeShort=True):
        """
        获取当前现金总额
        """
        ret = self.__cash
        if not includeShort and self.current_bars is not None:
            # 处理允许卖空的情况
            bars = self.current_bars
            for instrument, shares in self.__shares.items():
                if shares < 0:
                    instrumentPrice = self._get_bar(bars, instrument).getClose(self.use_adjusted_values)
                    ret += instrumentPrice * shares
        return ret

    def current_position(self, price_type=None):
        if price_type is None:
            price_type = self.__config.price_type
        if self.__current_position is not None:
            return self.__current_position
        self.__current_position = self.getSharesValue(price_type) / self.get_total_equity(price_type)

    @property
    def frozen_cash(self):
        return self.__frozen_cash

    @property
    def deposit_ratio(self):
        """
        返回保证金占现金的比例

        注意，理论上期货为每日盯市场，由于仅在换仓时计入利润，平时计算保证金/现金比例时需扣除盈亏
        """
        return self.__frozen_cash / self.__cash

    @property
    def hedge_ratio(self):
        shares_value = self.getSharesValue()
        future_value = self.getFutureValue()
        return - future_value / shares_value

    def update_future(self):
        """
        期货每日盯市
        """
        futures_profit = 0
        deposit = 0
        count = 0
        if len( self.__futures.items()) < 1:
            return
        # print('tag', self.__futures)
        assert len( self.__futures.items()) == 1
        instrument, position = list(self.__futures.items())[0]
        price = self._get_bar(self.current_bars, instrument).get_price(self.__config.price_type)
        futures_profit += (price - position.point) * position.quantity
        position.point = price
        deposit += abs(position.point * position.quantity * self.__config.deposit)
        count += 1

        self.__cash += futures_profit
        self.__frozen_cash = deposit

        if self.__cash <= self.__frozen_cash:
            action = Order.Action.BUY if position.quantity < 0 else Order.Action.SELL
            amount = self.__cash / (self.__config.future_commission + self.__config.deposit) * 0.999
            max_quantity = FutureUtil.calc_future_quantity(amount, price)
            order = self.createMarketOrder(action, instrument, abs(position.quantity)-max_quantity)
            self.submitOrder(order)
            self.onBarsImpl(order, self.current_bars)

    def update_deposit(self):
        """
        更新期货保证金
        """
        deposit = 0
        previous_deposit_ratio = self.deposit_ratio
        for instrument, position in self.__futures.items():
            price = self._get_bar(self.current_bars, instrument).open
            deposit += abs(price * position.quantity) * self.__config.deposit
        self.__frozen_cash = deposit
        try:
            assert self.__frozen_cash < self.__cash
        except Exception as e:
            self.__debug_info()
            print('[previous deposit ratio]', previous_deposit_ratio)
            print('[deposit ratio]', self.deposit_ratio)
            raise e

    def getAvailableCash(self, include_short=True):
        """
        获取可用现金
        """
        return self.getCash(include_short) - self.__frozen_cash

    def setCash(self, cash):
        self.__cash = cash

    def setFillStrategy(self, strategy):
        """Sets the :class:`wk_pyalgotrade.broker.fillstrategy.FillStrategy` to use."""
        self.__fillStrategy = strategy

    def getFillStrategy(self, bar=None):
        if bar is None:
            return self.__fillStrategy
        elif isinstance(bar, StockIndexFutureBar):
            return self.__future_fill_strategy
        elif isinstance(bar, StockBar):
            return self.__fillStrategy

    def getShares(self, instrument):
        """
        获取具体股票的持仓数
        """
        return self.__shares.get(instrument, 0)

    def getFuturePosition(self, instrument):
        return self.__futures.get(instrument)

    def getFuturePositions(self):
        return self.__futures

    def getFutureAmount(self, instrument, bars):
        position = self.__futures.get(instrument)
        if position is None:
            return 0
        return position.quantity * bars[instrument].open

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
        # self.__transaction_tracker.append(trans_rec)

    def transform_shares(self, bars, old_instrument, new_instrument, ratio):
        old_shares = self.getShares(old_instrument)
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

    """
    获取持仓状态
    """

    def getPositions(self):
        return self.__shares

    def getSharesAmount(self, instrument, bars, price_type=None):
        """
        获取具体股票的持仓金额，用开盘价统计 2017/10/09
        """
        if price_type is None:
            price_type = self.__config.price_type
        return self.__shares.get(instrument, 0) * bars[instrument].get_price(price_type, self.getUseAdjustedValues())

    # @property
    # def total_equity_pre(self):
    #     """
    #     盘前总资产，包含股票持仓，现金，期货盈亏
    #     """
    #     return self.get_total_equity(PriceType.OPEN)
    #
    # @property
    # def total_equity(self):
    #     return self.get_total_equity(PriceType.CLOSE)

    def get_total_equity(self, price_type=None):
        if price_type is None:
            price_type = self.__config.price_type
        stock_value = self.getSharesValue(price_type)
        return stock_value + self.getCash()

    def getFutureValue(self, price_type=None):
        """
        以收盘价格计算的期货敞口
        """
        if price_type is None:
            price_type = self.__config.price_type
        futures_value = 0
        for instrument, position in self.__futures.items():
            price = self._get_bar(self.current_bars, instrument).get_price(price_type)
            futures_value += price * position.quantity
        return futures_value

    def getFutureProfit(self, price_type=None):
        if price_type is None:
            price_type = self.__config.price_type
        profit = 0
        for instrument, position in self.__futures.items():
            price = self._get_bar(self.current_bars, instrument).get_price(price_type)
            profit += (price - position.point) * position.quantity
        return profit

    # def getEquityPre(self):
    #     """
    #     获取盘前总资产，不含期货
    #     """
    #     return self.total_equity_pre
    #
    #
    def getEquity(self, price_type=None):
        """
        获取总资产（收盘价），用户调用接口，包含期货的浮动盈亏
        """
        if price_type is None:
            price_type = self.__config.price_type
        return self.get_total_equity(price_type)

    def getSharesValue(self, price_type=None):
        if price_type is None:
            price_type = self.__config.price_type

        ret = 0
        bars = self.current_bars

        if bars is not None:
            for instrument, shares in self.__shares.items():
                instrumentPrice = self._get_bar(bars, instrument).get_price(price_type, self.use_adjusted_values)
                ret += instrumentPrice * shares
                # print instrument, shares, instrumentPrice
        return ret



    @property
    def unfilled_orders(self):
        return self.__unfilled_orders

    def getSharesCanSell(self, instrument):
        """
        返回当天可卖股数， T+1规则
        """
        return self.__sharesCanSell.get(instrument, 0)

    def getPositionCost(self, instrument):
        """
        返回具体股票的持仓成本
        持仓成本计算（花在这只股票上的所有金额/当前持仓数目）
        """
        if self.getShares(instrument) == 0:
            return 0
        else:
            return self.__amountTotal.get(instrument, 0) / self.getShares(instrument)

    def refresh_position_cost(self):
        """
        根据当前持仓和最新价格刷新持仓成本
        """
        self.__amountTotal = {}
        bars = self.current_bars
        for k, v in self.__shares.items():
            if v != 0:
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

    def getLastBuyTime(self, instrument):
        """
        返回最近买入时间
        """
        return self.__lastBuyTime.get(instrument, 0)

    """
    返回最近卖出时间
    """

    def getLastSellTime(self, instrument):
        return self.__lastSellTime.get(instrument, 0)

    @property
    def price_type(self):
        return self.__config.price_type

    # def __append_transaction(self,
    #                          trade_dt: str,
    #                          windcode: str,
    #                          sec_name: str,
    #                          price: float,
    #                          volume: int,
    #                          commission: float,
    #                          stamp_tax: float,
    #                          direction: str,
    #                          note: str = ''
    #                          ):
    #
    #     if not self.__config.tracking_transaction:
    #         return
    #     trans_rec = TransactionRecord(
    #         trade_dt,
    #         windcode,
    #         sec_name,
    #         price,
    #         volume,
    #         commission,
    #         stamp_tax,
    #         direction,
    #         note
    #     )
    #     self.__transaction_tracker.append(trans_rec)

    def __debug_info(self):
        print('---------- debug info ------------')
        print('[trade date]', self.current_bars.getDateTime())
        print('[market value]', self.getSharesValue())
        print('[future value]', self.getFutureValue())
        print('[current cash]', self.__cash)
        print('[frozen cash]', self.__frozen_cash)
        print('[total equity]', self.total_equity)


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

    # Tries to commit an order execution.
    @staticmethod
    def get_shares_delta(order, quantity):
        if order.isBuy():
            shares_delta = quantity
            direction = '买入'
        elif order.isSell():
            shares_delta = quantity * -1
            direction = '卖出'
        else:
            assert False
        return shares_delta, direction

    def commitFutureOrderExecution(self, order, date_time, sec_name, fillInfo):
        """
        期货订单处理
        """
        price = fillInfo.getPrice()
        quantity = fillInfo.getQuantity()

        commission = abs(price * quantity * self.__config.future_commission) # 成本为交易手续费
        cost = -commission

        shares_delta, direction = self.get_shares_delta(order, quantity)

        try:
            future_position: HedgeBroker.FuturePosition = self.__futures[order.getInstrument()]
            future_position.point = price
        except KeyError:
            future_position = HedgeBroker.FuturePosition(
                code=self.__config.code,
                windcode=order.getInstrument(),
                point=price,
                quantity=0
            )
            self.__futures[order.getInstrument()] = future_position

        target_quantity = future_position.quantity + shares_delta
        deposit = abs(price * target_quantity * self.__config.deposit)
        resulting_available_cash = self.__cash - deposit + cost
        ref_cash_requirement = deposit + cost  # 原始订单的现金需求量
        if resulting_available_cash < 0:
            # 现金/保证金不足
            amount = self.__cash / (self.__config.future_commission + self.__config.deposit) * 0.999
            max_quantity = FutureUtil.calc_future_quantity(amount, price)
            try:
                assert abs(future_position.quantity) <= max_quantity
            except Exception as e:
                self.__debug_info()
                print('[current quantity]', future_position.quantity)
                print('[amount]', amount)
                print('[max_quantity]', max_quantity)
                print('[quantity]', quantity)
                print('[order quantity]', fillInfo.getQuantity())
                print('[long position]', order.isBuy())
                print('[price]', price)
                raise e
            available_quantity = max_quantity - abs(future_position.quantity)
            quantity = available_quantity
            commission = abs(price * quantity * self.__config.future_commission)  # 成本为交易手续费
            cost = -commission
            shares_delta, direction = self.get_shares_delta(order, quantity)
            new_target_position = future_position.quantity + shares_delta
            deposit = abs(price * new_target_position * self.__config.deposit)

        resulting_available_cash = self.__cash - deposit + cost
        try:
            assert resulting_available_cash > 0
        except Exception as e:
            self.__debug_info()
            print('[quantity]', quantity)
            print('[order quantity]', fillInfo.getQuantity())
            print('[long position]', order.isBuy())
            print('[price]', price)
            raise e

        if quantity == 0:
            self.cancelOrder(order)
            self.__record_unfilled_order(order, f"现金不足，现有{self.getCash()}，需{ref_cash_requirement}")
            return

        future_position.quantity += shares_delta
        if future_position.quantity == 0:
            del self.__futures[order.getInstrument()]

        order_execution_info = broker.OrderExecutionInfo(price, quantity, commission, date_time)
        order.addExecutionInfo(order_execution_info)

        date_time_str = date_time.strftime("%Y-%m-%d")

        self._append_transaction(
                date_time_str,
                order.getInstrument(),
                sec_name,
                price,
                shares_delta,
                commission,
                0,
                direction
            )

        """
        更新当前剩余现金
        """
        self.__cash = self.getCash() + cost
        self.__frozen_cash = deposit

        self.__future_fill_strategy.onOrderFilled(self, order)

        # Notify the order update
        if order.isFilled():
            self._unregister_order(order)
            self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.FILLED, order_execution_info))
        elif order.isPartiallyFilled():
            self.notifyOrderEvent(
                broker.OrderEvent(order, broker.OrderEvent.Type.PARTIALLY_FILLED, order_execution_info)
            )
        else:
            assert False

    def reduce_position(self):
        """
        从持仓中删去仓位为0的标的
        """
        instruments = self.__shares.keys()
        for inst in instruments:
            if self.__shares[inst] == 0:
                del self.__shares[inst]
                del self.__amountTotal[inst]


    def commitOrderExecution(self, order, dateTime, sec_name, fillInfo):
        """
        订单处理方法
        """
        price = fillInfo.getPrice()
        quantity = fillInfo.getQuantity()

        # 新增记录买入买出方向
        buyOrSell = '买入'

        if order.isBuy():
            cost = price * quantity * -1
            assert (cost < 0)
            sharesDelta = quantity

            buyOrSell = '买入'
        elif order.isSell():
            cost = price * quantity
            assert (cost > 0)
            sharesDelta = quantity * -1

            buyOrSell = '卖出'
        else:  # Unknown action
            assert (False)

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
        resultingCash = self.getAvailableCash() + cost

        self.__logger.info(f"original cash :{self.getCash()}, available {self.getAvailableCash()}")
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
            factor = self.getAvailableCash() / (- cost) * 0.999  # 0.999用于防止舍入误差
            quantity = (self.getAvailableCash() * factor) / price
            quantity = self.adjusted_shares(quantity)

            sharesDelta = quantity

            commission = self.getCommission().calculate(order, price, sharesDelta)
            cost = price * sharesDelta * (-1)
            cost -= commission

            """
            卖出时需要加上印花税计算 2017/10/26
            """
            if order.isSell():
                commissionTaxFee = self.getCommissionTaxFee().calculate(order, price, sharesDelta)
            else:
                commissionTaxFee = 0

            cost -= commissionTaxFee

            resultingCash = self.getAvailableCash() + cost
            self.__logger.info('when getAllOrNone, the quantity is %d, sharesDelta is %d, resulting Cash is %f' % (
            quantity, sharesDelta, resultingCash))

        """
        正常结算流程
        """
        # Check that we're ok on cash after the commission.
        # if resultingCash >= 0 or self.__allowNegativeCash:
        if (resultingCash >= 0 or self.allow_negative_cash) and quantity > 0:
            # Update the order before updating internal state since addExecutionInfo may raise.
            # addExecutionInfo should switch the order state.
            orderExecutionInfo = broker.OrderExecutionInfo(price, quantity, commission, dateTime)
            order.addExecutionInfo(orderExecutionInfo)

            dateTimeTemp = dateTime.strftime("%Y-%m-%d")
            """
            添加一次交易流水记录
            """
            # TODO: 检查出现交易量调整时是否用shareDelta代替quantity
            self._append_transaction(
                dateTimeTemp,
                order.getInstrument(),
                sec_name,
                price,
                quantity,
                commission,
                commissionTaxFee,
                buyOrSell
            )
            # self.__transaction_tracker.append(trans_rec)

            # Commit the order execution.
            """
            更新当前剩余现金
            """
            # self.__cash = resultingCash
            self.__cash = self.getCash() + cost

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
                删除持仓 20220527
                """
                self.__amountTotal[order.getInstrument()] = 0
                self.__shares[order.getInstrument()] = 0
            else:
                self.__shares[order.getInstrument()] = updatedShares

            """
            更新持仓成本价
            """
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
                self._unregister_order(order)
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

            self.__record_unfilled_order(order, f"现金不足，现有{self.getCash()}，可用{self.getAvailableCash()}，需{-cost}")

    def submitOrder(self, order):
        if order.isInitial():
            order.setSubmitted(self._next_order_id, self._current_date_time)
            self._register_order(order)
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
                self._unregister_order(order)
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
            if self.bar_feed_frequency >= Frequency.DAY:
                expired = bar_.getDateTime().date() >= order.getAcceptedDateTime().date()

            # Cancel the order if it will expire in the next bar.
            if expired:
                ext_msg = ''
                if order.getState() == broker.Order.State.PARTIALLY_FILLED:
                    ext_msg = f"：部成订单，总{order.getQuantity()}，未成{order.getRemaining()}"
                self._unregister_order(order)
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
            if isinstance(bar_, StockBar):
                self.commitOrderExecution(order, bar_.getDateTime(), bar_.getSecName(), fillInfo)
            elif isinstance(bar_, StockIndexFutureBar):
                self.commitFutureOrderExecution(order, bar_.getDateTime(), bar_.windcode, fillInfo)
            else:
                assert False, f'unsupported Bar type {bar_}'
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
            assert order not in self.getActiveOrders()

    def __reset_cache(self):
        self.__current_position = None


    def onBars(self, dateTime, bars):
        # Let the fill strategy know that new bars are being processed.

        self.__logger.info("onBars called")
        self.__reset_cache()

        """
        filStrategy.onBars中进行可成交量volume的计算
        """
        datetime_str = dateTime.strftime("%Y%m%d")
        self.__fillStrategy.onBars(self, bars)
        self.__future_fill_strategy.onBars(self, bars)
        # 新增，具体位置有待商榷，此处需要check
        self.__sharesCanSell = copy.deepcopy(self.__fillStrategy.getVolumeBegin())
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

    def onBars2(self, dateTime, bars):
        """
        bar结束时调用

        可以考虑将当日/即时成交操作移动至此方法
        """
        # self.update_deposit()
        # self.__logger.info("onBars called")
        # ordersToProcess = self.__activeOrders.values()
        #
        # for order in ordersToProcess:
        #     # This may trigger orders to be added/removed from __activeOrders.
        #
        #     self.__logger.info("broker.backtesting.py/call onBarsImpl")
        #     self.onBarsImpl(order, bars)



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
        activeOrder = self.getActiveOrdersDict().get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")

        self._unregister_order(activeOrder)
        activeOrder.switchState(broker.Order.State.CANCELED)
        self.notifyOrderEvent(
            broker.OrderEvent(activeOrder, broker.OrderEvent.Type.CANCELED, "User requested cancellation")
        )

    def export_status(self):
        pass

    def update_status(self, status: BrokerStatus):
        pass
