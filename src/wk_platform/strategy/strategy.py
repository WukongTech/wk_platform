# -*- coding: utf-8 -*-
# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Date: 2017/07/19
# Author: chenxiangdong
# content:
# 1. 策略Strategy基类模块，由pyalgotrade/strategy/__init__改写而成
# 2. 继承关系，基类BaseStrategy, 子类BacktestingStrategy继承自BaseStrategy，我们编写的自定义
# 类需要继承BacktestingStrategy类进行编写
#
# 主要变动：
# 1. enterLong, enterShort中添加了函数参数bars
# 2. strategy __onBars中调整了调用顺序

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""
from __future__ import annotations
import abc
import logging
from pyalgotrade import observer
from pyalgotrade import dispatcher
from pyalgotrade.barfeed import resampled
from wk_platform.config.enums import PriceType
from wk_platform.util.future import FutureUtil

from wk_util import logger

import wk_platform.broker
from wk_platform.broker.brokers import Broker
import wk_platform.strategy.position
# from wk_platform.broker.backtesting import TradePercentage
# from wk_platform.broker.backtesting import TradePercentageTaxFee
from wk_platform.config import StrategyConfiguration
# from wk_platform.stratanalyzer.tracker import StrategyCustomTracker


class BaseStrategy(object, metaclass=abc.ABCMeta):
    """Base class for strategies.

    :param barFeed: The bar feed that will supply the bars.
    :type barFeed: :class:`wk_pyalgotrade.barfeed.BaseBarFeed`.
    :param broker: The broker that will handle orders.
    :type broker: :class:`wk_pyalgotrade.broker.Broker`.

    .. note::
        This is a base class and should not be used directly.
    """

    LOGGER_NAME = "strategy"

    def __init__(self, barFeed, broker, config: StrategyConfiguration):
        self.__barFeed = barFeed

        """
        strategy中实例化了一个broker对象， chenxiangdong 20170719
        """
        self.__broker = broker
        self.__activePositions = set()
        self.__config = config
        self.__orderToPosition = {}
        self.__barsProcessedEvent = observer.Event()
        self.__analyzers = []
        self.__namedAnalyzers = {}
        self.__resampledBarFeeds = []
        self.__dispatcher = dispatcher.Dispatcher()

        """
        broker的getOrderUpdatedEvent()事件会触发strategy中的__onOrderEvent函数,
        chenxiangdong, 20170719
        """
        self.__broker.getOrderUpdatedEvent().subscribe(self.__onOrderEvent)

        """
        barFeed中的getNewValuesEvent()事件触发strategy中的onBars函数
        """
        self.__barFeed.getNewValuesEvent().subscribe(self.__onBars)

        """
        日志对象，重定向文件在logger.py文件中设定
        """
        # Initialize logging.
        self.__logger = logger.getLogger(BaseStrategy.LOGGER_NAME, disable=True)

        """
        #测试修改顺序初始化的情况
        self.__barFeed.getNewValuesEvent().subscribe(self.__onBars)
        self.__broker = broker
        self.__broker.getOrderUpdatedEvent().subscribe(self.__onOrderEvent)
        """

        self.__dispatcher.getStartEvent().subscribe(self.onStart)
        self.__dispatcher.getIdleEvent().subscribe(self.__onIdle)

        # It is important to dispatch broker events before feed events, specially if we're backtesting.
        self.__dispatcher.addSubject(self.__broker)
        self.__dispatcher.addSubject(self.__barFeed)

    @property
    def custom_analyzer(self):
        try:
            return self.get_analyzer('custom_analyzer')
        except KeyError:
            return None

    @property
    def broker(self):
        return self.__broker

    # for test
    # Only valid for testing purposes.
    def _setBroker(self, broker):
        self.__broker = broker

    def setUseEventDateTimeInLogs(self, useEventDateTime):
        if useEventDateTime:
            logger.Formatter.DATETIME_HOOK = self.getDispatcher().getCurrentDateTime
        else:
            logger.Formatter.DATETIME_HOOK = None

    def getLogger(self):
        return self.__logger

    def getActivePositions(self):
        return self.__activePositions

    def getOrderToPosition(self):
        return self.__orderToPosition

    def getDispatcher(self):
        return self.__dispatcher

    def getBarsProcessedEvent(self):
        return self.__barsProcessedEvent

    def getUseAdjustedValues(self):
        return False

    def registerPositionOrder(self, position, order):
        self.__activePositions.add(position)
        assert (order.isActive())  # Why register an inactive order ?
        self.__orderToPosition[order.getId()] = position

    def unregisterPositionOrder(self, position, order):
        del self.__orderToPosition[order.getId()]

    def unregisterPosition(self, position):
        assert (not position.isOpen())
        self.__activePositions.remove(position)

    def __notifyAnalyzers(self, lambdaExpression):
        for s in self.__analyzers:
            lambdaExpression(s)

    def get_analyzer(self, name):
        return self.__namedAnalyzers[name]

    def attachAnalyzerEx(self, strategyAnalyzer, name=None):
        """
        将回测结果分析类strategyAnalyzer注册到策略上
        """
        if strategyAnalyzer not in self.__analyzers:
            if name is not None:
                if name in self.__namedAnalyzers:
                    raise Exception("A different analyzer named '%s' was already attached" % name)
                self.__namedAnalyzers[name] = strategyAnalyzer

            strategyAnalyzer.beforeAttach(self)
            self.__analyzers.append(strategyAnalyzer)
            strategyAnalyzer.attached(self)

    def getLastPrice(self, instrument):
        ret = None
        bar = self.getFeed().getLastBar(instrument)
        if bar is not None:
            ret = bar.getPrice()
        return ret

    def getFeed(self):
        """Returns the :class:`wk_pyalgotrade.barfeed.BaseBarFeed` that this strategy is using."""
        return self.__barFeed

    def getBroker(self):
        """Returns the :class:`wk_pyalgotrade.broker.Broker` used to handle order executions."""
        return self.__broker

    def getCurrentDateTime(self):
        """Returns the :class:`datetime.datetime` for the current :class:`wk_pyalgotrade.bar.Bars`."""
        return self.__barFeed.getCurrentDateTime()

    @property
    def config(self):
        return self.__config

    """
    获取当前持仓状况
    chenxiangdong 20170719
    """

    def getResult(self):
        return self.getBroker().getEquity()

    """
    市价单下单函数，调用broker的createMarketOrder()函数
    目前暂不使用这两个函数作为外部下单接口，下单操作采用enterLong, enterShort
    chenxiangdong 20170719

    """

    def marketOrder(self, instrument, quantity, onClose=False, goodTillCanceled=False, allOrNone=False):
        """Submits a market order.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param quantity: The amount of shares. Positive means buy, negative means sell.
        :type quantity: int/float.
        :param onClose: True if the order should be filled as close to the closing price as possible (Market-On-Close order). Default is False.
        :type onClose: boolean.
        :param goodTillCanceled: True if the order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the order should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`wk_pyalgotrade.broker.MarketOrder` submitted.
        """

        ret = None
        if quantity > 0:
            ret = self.getBroker().createMarketOrder(wk_platform.broker.Order.Action.BUY, instrument, quantity, onClose)
        elif quantity < 0:
            ret = self.getBroker().createMarketOrder(wk_platform.broker.Order.Action.SELL, instrument, quantity * -1,
                                                     onClose)
        if ret:
            ret.setGoodTillCanceled(goodTillCanceled)
            ret.setAllOrNone(allOrNone)
            self.getBroker().submitOrder(ret)
        return ret

    """
    限价单下单函数，调用broker的createLimitOrder
    """

    def limitOrder(self, instrument, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Submits a limit order.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: The amount of shares. Positive means buy, negative means sell.
        :type quantity: int/float.
        :param goodTillCanceled: True if the order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the order should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`wk_pyalgotrade.broker.LimitOrder` submitted.
        """

        ret = None
        if quantity > 0:
            ret = self.getBroker().createLimitOrder(wk_platform.broker.Order.Action.BUY, instrument, limitPrice,
                                                    quantity)
        elif quantity < 0:
            ret = self.getBroker().createLimitOrder(wk_platform.broker.Order.Action.SELL, instrument, limitPrice,
                                                    quantity * -1)
        if ret:
            ret.setGoodTillCanceled(goodTillCanceled)
            ret.setAllOrNone(allOrNone)
            self.getBroker().submitOrder(ret)
        return ret

    def stopOrder(self, instrument, stopPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Submits a stop order.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param quantity: The amount of shares. Positive means buy, negative means sell.
        :type quantity: int/float.
        :param goodTillCanceled: True if the order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the order should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`wk_pyalgotrade.broker.StopOrder` submitted.
        """

        ret = None
        if quantity > 0:
            ret = self.getBroker().createStopOrder(wk_platform.broker.Order.Action.BUY, instrument, stopPrice, quantity)
        elif quantity < 0:
            ret = self.getBroker().createStopOrder(wk_platform.broker.Order.Action.SELL, instrument, stopPrice,
                                                   quantity * -1)
        if ret:
            ret.setGoodTillCanceled(goodTillCanceled)
            ret.setAllOrNone(allOrNone)
            self.getBroker().submitOrder(ret)
        return ret

    def stopLimitOrder(self, instrument, stopPrice, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Submits a stop limit order.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: The amount of shares. Positive means buy, negative means sell.
        :type quantity: int/float.
        :param goodTillCanceled: True if the order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the order should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`wk_pyalgotrade.broker.StopLimitOrder` submitted.
        """

        ret = None
        if quantity > 0:
            ret = self.getBroker().createStopLimitOrder(wk_platform.broker.Order.Action.BUY, instrument, stopPrice,
                                                        limitPrice, quantity)
        elif quantity < 0:
            ret = self.getBroker().createStopLimitOrder(wk_platform.broker.Order.Action.SELL, instrument, stopPrice,
                                                        limitPrice, quantity * -1)
        if ret:
            ret.setGoodTillCanceled(goodTillCanceled)
            ret.setAllOrNone(allOrNone)
            self.getBroker().submitOrder(ret)
        return ret

    """
    多种下单接口：
    1. 按照订单数量
    2. 按照购入/卖出资金
    3. 按照权重下单 （按照与当前总资金占比）
    """

    def adjusted_shares(self, shares):
        """
        根据配置确定是否对交易数量进行整百调整，整手交易时对数量进行调整
        """
        if self.__config.whole_batch_only:
            return (shares // 100) * 100
        else:
            return shares

    """
    添加参数bars
    chenxiangdong,2017/07/18
    按照数量下单买入，市价单
    chenxiangdong, 2017/10/09
    """

    def enterLong(self, bars, instrument, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a buy :class:`wk_pyalgotrade.broker.MarketOrder` to enter a long position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`wk_pyalgotrade.strategy.position.Position` entered.
        """

        self.__logger.info('buy quantity before adjust is %d' % (quantity))
        quantity = self.adjusted_shares(quantity)
        self.__logger.info('buy quantity after adjust is %d' % (quantity))

        if quantity != 0:
            return wk_platform.strategy.position.LongPosition(self, bars, instrument, None, None, quantity,
                                                              goodTillCanceled, allOrNone)
        else:
            return None

    """
    添加参数bars
    chenxiangdong,2017/07/18

    按照数量下单卖出，市价单
    chenxiangdong, 2017/10/09
    """

    def enterShort(self, bars, instrument, quantity, goodTillCanceled=False, allOrNone=False, msg=None):
        """Generates a sell short :class:`wk_pyalgotrade.broker.MarketOrder` to enter a short position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`wk_pyalgotrade.strategy.position.Position` entered.
        """
        """
        self.__logger.info('sell quantity before adjust is %d' %(quantity))
        quantity = self.getAjusted100Shares(quantity)
        self.__logger.info('sell quantity after adjust is %d' %(quantity))
        """

        """
        仅当本次卖出是最后一笔卖出时允许非整百卖出
        """
        sharesNow = self.getBroker().getShares(instrument)
        if quantity != sharesNow:  # 非最后一笔卖出时对数量进行调整
            quantity = self.adjusted_shares(quantity)
        """
        else:
            print 'sell all'
        """
        if quantity != 0:
            return wk_platform.strategy.position.ShortPosition(self, bars, instrument, None, None, quantity,
                                                               goodTillCanceled, allOrNone, msg=msg)
        else:
            return None

    """
    添加参数bars
    chenxiangdong,2017/07/18
    按照数量下单买入， 限价单 
    chenxiangdong, 2017/10/09
    """

    def enterLongLimit(self, bars, instrument, limitPrice, quantity, goodTillCanceled=False, allOrNone=False, msg=None):
        """Generates a buy :class:`wk_pyalgotrade.broker.LimitOrder` to enter a long position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`wk_pyalgotrade.strategy.position.Position` entered.
        """

        self.__logger.info('buy quantity before adjust is %d' % (quantity))
        quantity = self.adjusted_shares(quantity)
        self.__logger.info('buy quantity after adjust is %d' % (quantity))

        if quantity != 0:
            return wk_platform.strategy.position.LongPosition(self, bars, instrument, None, limitPrice, quantity,
                                                              goodTillCanceled, allOrNone)
        else:
            return None

    """
    添加参数bars
    chenxiangdong,2017/07/18
    按照数量下单卖出，限价单
    chenxiangdong, 2017/10/09
    """

    def enterShortLimit(self, bars, instrument, limitPrice, quantity, goodTillCanceled=False, allOrNone=False, inBar=False, msg=None):
        """Generates a sell short :class:`wk_pyalgotrade.broker.LimitOrder` to enter a short position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :param inBar: 是否允许使用bar内价格，当inBar=False时，若open价格为最优则选择open作为成交价，inBar=True时，只要limitPrice在low high之间就选择limitPrice作为成交价，即使open是最优价格
        :type inBar: boolean.

        :rtype: The :class:`wk_pyalgotrade.strategy.position.Position` entered.
        """
        """
        self.__logger.info('sell quantity before adjust is %d' %(quantity))
        quantity = self.getAjusted100Shares(quantity)
        self.__logger.info('sell quantity after adjust is %d' %(quantity))
        """

        sharesNow = self.getBroker().getShares(instrument)
        if quantity != sharesNow:  # 非最后一笔卖出时对数量进行调整
            quantity = self.adjusted_shares(quantity)

        if quantity != 0:
            return wk_platform.strategy.position.ShortPosition(self, bars, instrument, None, limitPrice, quantity,
                                                           goodTillCanceled, allOrNone,inBar=inBar, msg=msg)
        else:
            return None

    def calc_stock_quantity(self, instrument, bar, cash_amount, good_till_canceled=False, whole_batch=False):
        """
        计算输入金额可以买入的股票数目
        """

        # shares_now = self.getBroker().getShares(instrument)

        if good_till_canceled:
            """
            按照Close price计算数量
            """
            if bar.getClose() == 0:
                return None
            price = bar.getClose()
        else:
            # 根据配置选择价格
            price = bar.get_price(self.__config.price_type)
            if price == 0:
                return None
            if cash_amount > 0 and self.__config.price_with_commission:
                price = price * (1 + self.__config.commission.percentage)

        if whole_batch:
            quantity = int(cash_amount / (price * 100)) * 100
        else:
            quantity = cash_amount // price

        # quantity = abs(quantity)
        #
        # if quantity != shares_now:


        return int(quantity)

    def calc_quantity(self, instrument, bar, cash_amount, good_till_canceled=False, whole_batch=False):
        """
        计算输入金额可以买入的股票数目
        """
        if FutureUtil.is_index_future(instrument):
            return FutureUtil.calc_future_quantity(cash_amount, bar.get_price(self.__config.price_type), whole_batch=whole_batch)
        else:
            return self.calc_stock_quantity(instrument, bar, cash_amount, good_till_canceled)


    """
    下单接口，按照金额买入卖出
    """

    """
    按照金额下单买入,市价单
    """

    def enterLongCashAmount(self, bars, instrument, cashAmount, goodTillCanceled=False, allOrNone=False):
        """
        计算输入金额可以买入的股票数目
        """
        # if goodTillCanceled:
        #     """
        #     按照Close price计算数量
        #     """
        #     quantity = cashAmount // bars[instrument].getClose()
        # else:
        #     """
        #     按照open price计算数量
        #     """
        #     quantity = cashAmount // bars[instrument].getOpen()
        quantity = self.calc_quantity(bars[instrument], cashAmount, good_till_canceled=goodTillCanceled)


        self.__logger.info('buy quantity before adjust is %d' % (quantity))
        quantity = self.adjusted_shares(quantity)
        self.__logger.info('buy quantity after adjust is %d' % (quantity))

        if quantity != 0:
            return wk_platform.strategy.position.LongPosition(self, bars, instrument, None, None, quantity,
                                                              goodTillCanceled, allOrNone)
        else:
            return None

    """
    按照金额下单卖出，市价单
    """

    def enterShortCashAmount(self, bars, instrument, cashAmount, goodTillCanceled=False, allOrNone=False):

        """
        计算输入金额可以买入的股票数目
        """
        # if goodTillCanceled:
        #     """
        #     按照Close price计算数量
        #     """
        #     quantity = cashAmount // bars[instrument].getClose()
        # else:
        #     """
        #     按照open price计算数量
        #     """
        #     quantity = cashAmount // bars[instrument].getOpen()
        quantity = self.calc_quantity(bars[instrument], cashAmount, good_till_canceled=goodTillCanceled)

        """
        仅当本次卖出是最后一笔卖出时允许非整百卖出
        """
        sharesNow = self.getBroker().getShares(instrument)
        if quantity != sharesNow:
            quantity = self.adjusted_shares(quantity)

        if quantity != 0:
            return wk_platform.strategy.position.ShortPosition(self, bars, instrument, None, None, quantity,
                                                               goodTillCanceled, allOrNone)
        else:
            return None

    """
    按照金额下单买入，限价单
    """

    def enterLongLimitCashAmount(self, bars, instrument, limitPrice, cashAmount, goodTillCanceled=False,
                                 allOrNone=False):
        """
        计算输入金额可以买入的股票数目
        """
        # if goodTillCanceled:
        #     """
        #     按照Close price计算数量
        #     """
        #     quantity = cashAmount // bars[instrument].getClose()
        # else:
        #     """
        #     按照open price计算数量
        #     """
        #     quantity = cashAmount // bars[instrument].getOpen()
        quantity = self.calc_quantity(bars[instrument], cashAmount, good_till_canceled=goodTillCanceled)

        self.__logger.info('buy quantity before adjust is %d' % (quantity))
        quantity = self.adjusted_shares(quantity)
        self.__logger.info('buy quantity after adjust is %d' % (quantity))

        if quantity != 0:
            return wk_platform.strategy.position.LongPosition(self, bars, instrument, None, limitPrice, quantity,
                                                              goodTillCanceled, allOrNone)
        else:
            return None

    """
    按照金额下单卖出，限价单
    """

    def enterShortLimitCashAmount(self, bars, instrument, limitPrice, cashAmount, goodTillCanceled=False,
                                  allOrNone=False):
        """
        计算输入金额可以买入的股票数目
        """

        # if goodTillCanceled:
        #     """
        #     按照Close price计算数量
        #     """
        #     quantity = cashAmount // bars[instrument].getClose()
        # else:
        #     """
        #     按照open price计算数量
        #     """
        #     quantity = cashAmount // bars[instrument].getOpen()
        quantity = self.calc_quantity(bars[instrument], cashAmount, good_till_canceled=goodTillCanceled)

        """
        仅当本次卖出是最后一笔卖出时允许非整百卖出
        """
        sharesNow = self.getBroker().getShares(instrument)
        if quantity != sharesNow:
            quantity = self.adjusted_shares(quantity)

        if quantity != 0:
            return wk_platform.strategy.position.ShortPosition(self, bars, instrument, None, limitPrice, quantity,
                                                               goodTillCanceled, allOrNone)
        else:
            return None

    """
    下单接口，按照持仓权重买入卖出
    """

    """
    按照持仓权重下单买入卖出,市价单
    """

    def enterLongShortWeight(self, bars, instrument, weight, goodTillCanceled=False, allOrNone=False, msg=None):

        # 总资产，根据选项中选定的价格计算
        total_equity = self.getBroker().get_total_equity()

        # 计算原来持仓市值，根据选项中选定的价格计算
        amount_old = self.getBroker().getSharesAmount(instrument, bars)

        """
        计算此次要买入的资金量
        """
        amount_new = total_equity * weight
        """
        计算此次实际需要买入的市值
        """
        cashAmount = amount_new - amount_old

        # TODO: 简化价格为0时的处置

        if weight == 0:
            # 由于清仓时可能由于舍入误差导致超卖1的情况出现，此处进行数量修正
            price = bars[instrument].get_price(self.__config.price_type)
            if price == 0:
                quantity = None
            else:
                quantity = -self.getBroker().getShares(instrument)  # 清仓时数量为负数
                shares_now = self.getBroker().getShares(instrument)

        else:
            quantity = self.calc_quantity(instrument, bars[instrument], cashAmount, good_till_canceled=goodTillCanceled)

        if quantity is None:
            # 暂时使用多头仓位代替
            return wk_platform.strategy.position.DummyPosition(self, bars, instrument, None, None, quantity,
                                                              goodTillCanceled, allOrNone, msg=msg)

        """
        执行买入或者卖出
        """
        if quantity > 0:
            # quantity = self.adjusted_shares(quantity)
            if quantity > 0:
                return wk_platform.strategy.position.LongPosition(self, bars, instrument, None, None, quantity,
                                                                  goodTillCanceled, allOrNone, msg=msg)
            else:
                return None
        elif quantity < 0:
            # shares_now = self.getBroker().getShares(instrument)
            quantity = abs(quantity)

            # if quantity != shares_now:
            #     # 当本次卖出是最后一笔卖出时允许非整百卖出
            #     quantity = self.adjusted_shares(quantity)

            if quantity > 0:
                return wk_platform.strategy.position.ShortPosition(self, bars, instrument, None, None, abs(quantity),
                                                                   goodTillCanceled, allOrNone, msg=msg)

        else:
            return None



    def enterLongShortLimitWeight(self, bars, instrument, limitPrice, weight, goodTillCanceled=False, allOrNone=False, msg=None):
        """
        按照持仓权重下单买入卖出，限价单
        """
        assert False, "暂时禁用此方法"
        # 禁用这两个选项
        assert not goodTillCanceled
        assert not allOrNone
        """
        获取当前持仓权重
        """
        # totalEquity = self.getBroker().getEquityPre()
        total_equity = self.getBroker().get_total_equity(PriceType.OPEN)
        """
        计算原来持仓市值
        """
        AmountOld = self.getBroker().getSharesAmount(instrument, bars)
        """
        计算此次要买入的资金量
        """
        AmountNew = totalEquity * weight
        """
        计算此次需要买入的市值
        """
        cashAmount = AmountNew - AmountOld

        # """
        # 计算输入金额可以买入的股票数目
        # """
        # if goodTillCanceled:
        #     """
        #     按照Close price计算数量
        #     """
        #     quantity = cashAmount // bars[instrument].getClose()
        # else:
        #     """
        #     按照open price计算数量
        #     """
        #     quantity = cashAmount // bars[instrument].getOpen()

        # 计算数量
        quantity = cashAmount // limitPrice

        # print 'quantity calculated is %d' %(quantity)

        """
        执行买入或者卖出
        """
        if quantity > 0:
            quantity = self.adjusted_shares(quantity)
            if quantity > 0:
                return wk_platform.strategy.position.LongPosition(self, bars, instrument, None, limitPrice, quantity,
                                                                  goodTillCanceled, allOrNone, msg=msg)
            else:
                return None
        elif quantity < 0:
            """
            仅当本次卖出是最后一笔卖出时允许非整百卖出
            """
            sharesNow = self.getBroker().getShares(instrument)
            quantity = abs(quantity)
            if quantity != sharesNow:
                quantity = self.adjusted_shares(quantity)
            if quantity > 0:
                return wk_platform.strategy.position.ShortPosition(self, bars, instrument, None, limitPrice, quantity,
                                                                   goodTillCanceled, allOrNone, msg=msg)
        else:
            return None

    """
    测试结果，输出当前个股持仓权重
    """

    def getPositionWeight(self, bars):

        """
        获取当前总资产
        """

        """
        获取每只股票持仓，求得权重占比
        """
        print('total equity pre')
        print(self.getBroker().getEquityPre())

        print('total equity post')
        print(self.getBroker().getEquity())

        print('total instruments')
        print(bars.getInstruments())

        print('total positions')
        print(self.getBroker().getPositions())

        print('single amount& equity')
        for inst in bars.getInstruments():
            print(self.getBroker().getSharesAmount(inst, bars))

            """
            weight
            """
            print(self.getBroker().getSharesAmount(inst, bars) / self.getBroker().getEquityPre())
            print('\n')

    """
    暂时不用的接口，stop订单和stopLimit订单
    """

    """
    Stop及StopLimit类型的订单暂不使用, chenxiangdong 20170820
    """

    def enterLongStop(self, bars, instrument, stopPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a buy :class:`wk_pyalgotrade.broker.StopOrder` to enter a long position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`wk_pyalgotrade.strategy.position.Position` entered.
        """

        return wk_platform.strategy.position.LongPosition(self, bars, instrument, stopPrice, None, quantity,
                                                          goodTillCanceled, allOrNone)

    def enterShortStop(self, bars, instrument, stopPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a sell short :class:`wk_pyalgotrade.broker.StopOrder` to enter a short position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`wk_pyalgotrade.strategy.position.Position` entered.
        """

        return wk_platform.strategy.position.ShortPosition(self, bars, instrument, stopPrice, None, quantity,
                                                           goodTillCanceled, allOrNone)

    def enterLongStopLimit(self, bars, instrument, stopPrice, limitPrice, quantity, goodTillCanceled=False,
                           allOrNone=False):
        """Generates a buy :class:`wk_pyalgotrade.broker.StopLimitOrder` order to enter a long position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`wk_pyalgotrade.strategy.position.Position` entered.
        """

        return wk_platform.strategy.position.LongPosition(self, bars, instrument, stopPrice, limitPrice, quantity,
                                                          goodTillCanceled, allOrNone)

    def enterShortStopLimit(self, bars, instrument, stopPrice, limitPrice, quantity, goodTillCanceled=False,
                            allOrNone=False):
        """Generates a sell short :class:`wk_pyalgotrade.broker.StopLimitOrder` order to enter a short position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: The Stop price.
        :type stopPrice: float.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`wk_pyalgotrade.strategy.position.Position` entered.
        """

        return wk_platform.strategy.position.ShortPosition(self, bars, instrument, stopPrice, limitPrice, quantity,
                                                           goodTillCanceled, allOrNone)

    def onEnterOk(self, position):
        """Override (optional) to get notified when the order submitted to enter a position was filled. The default implementation is empty.

        :param position: A position returned by any of the enterLongXXX or enterShortXXX methods.
        :type position: :class:`wk_pyalgotrade.strategy.position.Position`.
        """
        pass

    def onEnterCanceled(self, position):
        """Override (optional) to get notified when the order submitted to enter a position was canceled. The default implementation is empty.

        :param position: A position returned by any of the enterLongXXX or enterShortXXX methods.
        :type position: :class:`wk_pyalgotrade.strategy.position.Position`.
        """
        pass

    # Called when the exit order for a position was filled.
    def onExitOk(self, position):
        """Override (optional) to get notified when the order submitted to exit a position was filled. The default implementation is empty.

        :param position: A position returned by any of the enterLongXXX or enterShortXXX methods.
        :type position: :class:`wk_pyalgotrade.strategy.position.Position`.
        """
        pass

    # Called when the exit order for a position was canceled.
    def onExitCanceled(self, position):
        """Override (optional) to get notified when the order submitted to exit a position was canceled. The default implementation is empty.

        :param position: A position returned by any of the enterLongXXX or enterShortXXX methods.
        :type position: :class:`wk_pyalgotrade.strategy.position.Position`.
        """
        pass

    """Base class for strategies. """

    def onStart(self):
        """Override (optional) to get notified when the strategy starts executing. The default implementation is empty. """
        self.on_start()

    def on_start(self):
        pass

    def onFinish(self, bars):
        """Override (optional) to get notified when the strategy finished executing. The default implementation is empty.

        :param bars: The last bars processed.
        :type bars: :class:`wk_pyalgotrade.bar.Bars`.
        """
        self.on_finish(bars)

    def on_finish(self, bars):
        pass

    def onIdle(self):
        """Override (optional) to get notified when there are no events.

       .. note::
            In a pure backtesting scenario this will not be called.
        """
        pass


    def onBars(self, bars):
        """Override (**mandatory**) to get notified when new bars are available. The default implementation raises an Exception.

        **This is the method to override to enter your trading logic and enter/exit positions**.

        :param bars: The current bars.
        :type bars: :class:`wk_pyalgotrade.bar.Bars`.
        """
        self.on_bars(bars)

    @abc.abstractmethod
    def on_bars(self, bars):
        raise NotImplementedError()


    def onOrderUpdated(self, order):
        """Override (optional) to get notified when an order gets updated.

        :param order: The order updated.
        :type order: :class:`wk_pyalgotrade.broker.Order`.
        """
        pass

    def __onIdle(self):
        # Force a resample check to avoid depending solely on the underlying
        # barfeed events.
        for resampledBarFeed in self.__resampledBarFeeds:
            resampledBarFeed.checkNow(self.getCurrentDateTime())

        self.onIdle()

    def __onOrderEvent(self, broker_, orderEvent):
        order = orderEvent.getOrder()
        self.onOrderUpdated(order)

        # Notify the position about the order event.
        pos = self.__orderToPosition.get(order.getId(), None)
        if pos is not None:
            # Unlink the order from the position if its not active anymore.
            if not order.isActive():
                """
                self.__logger.info("unregister position orders")
                """
                self.unregisterPositionOrder(pos, order)
            pos.onOrderEvent(orderEvent)

    """
    调用策略自身onBars函数
    调用策略分析类strategyAnalyzer的beforeOnBars函数
    """

    def __onBars(self, dateTime, bars):
        # THE ORDER HERE IS VERY IMPORTANT

        # 1: Let analyzers process bars.

        # 2: Let the strategy process current bars and submit orders.
        # 修改了顺序
        # self.__logger.info("__onBars call notifyAnalyzer beforeOnBars")
        self.__notifyAnalyzers(lambda s: s.beforeOnBars(self, bars))

        self.__logger.info("onBars called")
        self.onBars(bars)

        # 修改了顺序
        # self.__logger.info("__onBars call notifyAnalyzer beforeOnBars")
        self.__notifyAnalyzers(lambda s: s.after_on_bars(self, bars))

        # 3: Notify that the bars were processed.
        self.__barsProcessedEvent.emit(self, bars)

    def run(self):
        """Call once (**and only once**) to run the strategy."""
        self.__logger.info("run called")
        self.__dispatcher.run()

        if self.__barFeed.getCurrentBars() is not None:
            self.onFinish(self.__barFeed.getCurrentBars())
        else:
            raise Exception("Feed was empty")

    def stop(self):
        """Stops a running strategy."""
        self.__dispatcher.stop()

    def attachAnalyzer(self, strategyAnalyzer):
        """Adds a :class:`wk_pyalgotrade.stratanalyzer.StrategyAnalyzer`."""
        self.attachAnalyzerEx(strategyAnalyzer)

    def getNamedAnalyzer(self, name):
        return self.__namedAnalyzers.get(name, None)

    def debug(self, msg):
        """Logs a message with level DEBUG on the strategy logger."""
        self.getLogger().debug(msg)

    def info(self, msg):
        """Logs a message with level INFO on the strategy logger."""
        self.getLogger().info(msg)

    def warning(self, msg):
        """Logs a message with level WARNING on the strategy logger."""
        self.getLogger().warning(msg)

    def error(self, msg):
        """Logs a message with level ERROR on the strategy logger."""
        self.getLogger().error(msg)

    def critical(self, msg):
        """Logs a message with level CRITICAL on the strategy logger."""
        self.getLogger().critical(msg)

    def resampleBarFeed(self, frequency, callback):
        """
        Builds a resampled barfeed that groups bars by a certain frequency.

        :param frequency: The grouping frequency in seconds. Must be > 0.
        :param callback: A function similar to onBars that will be called when new bars are available.
        :rtype: :class:`wk_pyalgotrade.barfeed.BaseBarFeed`.
        """
        ret = resampled.ResampledBarFeed(self.getFeed(), frequency)
        ret.getNewValuesEvent().subscribe(callback)
        self.getDispatcher().addSubject(ret)
        self.__resampledBarFeeds.append(ret)
        return ret


class BacktestingStrategy(BaseStrategy):
    # """Base class for backtesting strategies.
    #
    # :param bar_feed: The bar feed to use to backtest the strategy.
    # :type bar_feed: :class:`wk_pyalgotrade.barfeed.BaseBarFeed`.
    # :param config: strategy configuration.
    # :type config: :class:`StrategyConfiguration`.
    #
    # .. note::
    #     This is a base class and should not be used directly.
    # """

    def __init__(self, bar_feed, broker_cls=None, config=StrategyConfiguration()):
        """
        Parameters
        ================
        bar_feed: wk_pyalgotrade.barfeed.BaseBarFeed
            回测时使用的bar feed
        config: StrategyConfiguration
            策略配置信息
        """
        # The broker should subscribe to barFeed events before the strategy.
        # This is to avoid executing orders submitted in the current tick.

        # change broker source
        if config.using_broker:
            broker_ = config.broker
        elif broker_cls is not None:
            broker_ = broker_cls(bar_feed, config)
        else:
            broker_ = Broker(bar_feed, config)

        BaseStrategy.__init__(self, bar_feed, broker_, config)

        # add for test,onBars触发顺序问题
        # barFeed.getNewValuesEvent().subscribe(broker.onBars)

        self.__useAdjustedValues = False
        self.setUseEventDateTimeInLogs(True)

        self.setDebugMode(True)

    def getUseAdjustedValues(self):
        return self.__useAdjustedValues

    def setUseAdjustedValues(self, useAdjusted):
        self.getFeed().setUseAdjustedValues(useAdjusted)
        self.getBroker().setUseAdjustedValues(useAdjusted)
        self.__useAdjustedValues = useAdjusted

    def setDebugMode(self, debugOn):
        """Enable/disable debug level messages in the strategy and backtesting broker.
        This is enabled by default."""
        level = logging.INFO if debugOn else logging.INFO
        self.getLogger().setLevel(level)
        self.getBroker().getLogger().setLevel(level)

    def get_buy_date(self):
        pass

    @classmethod
    def prepare_feed(cls, *args, **kwargs):
        raise NotImplementedError()

    @classmethod
    def strategy_name(cls):
        raise NotImplementedError()
