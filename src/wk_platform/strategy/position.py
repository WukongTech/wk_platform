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

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

"""
chenxiangdong 20170719
仓位状态类position，主要维护了订单状态的改变

重要变动：
position类初始化时增加bars参数
"""

import datetime

from wk_platform.stratanalyzer import returns
from pyalgotrade import warninghelpers
from wk_platform import broker
from wk_util import logger

"""
PositionState类为订单状态类的父类
"""
class PositionState(object):
    def onEnter(self, position):
        pass

    # Raise an exception if an order can't be submitted in the current state.
    def canSubmitOrder(self, position, order):
        raise NotImplementedError()

    def onOrderEvent(self, position, orderEvent):
        raise NotImplementedError()

    def isOpen(self, position):
        raise NotImplementedError()

    def exit(self, position, stopPrice=None, limitPrice=None, goodTillCanceled=None):
        raise NotImplementedError()

"""
订单waitingEntryState状态

"""
class WaitingEntryState(PositionState):
    def canSubmitOrder(self, position, order):
        if position.entryActive():
            raise Exception("The entry order is still active")

    def onOrderEvent(self, position, orderEvent):
        # Only entry order events are valid in this state.
        assert(position.getEntryOrder().getId() == orderEvent.getOrder().getId())
        
        if orderEvent.getEventType() in (broker.OrderEvent.Type.FILLED, broker.OrderEvent.Type.PARTIALLY_FILLED):
           
            logger.getLogger("position.py/WaitingEntryState").info("change state to openstate")
            position.switchState(OpenState())
            
            """
            当订单状态转为FILLED或者PARTIALLY_FILLEED时，触发strategy的onEnterOk()函数
            """
            position.getStrategy().onEnterOk(position)
        elif orderEvent.getEventType() == broker.OrderEvent.Type.CANCELED:
            assert(position.getEntryOrder().getFilled() == 0)
          
            logger.getLogger("position.py/WaitingEntryState").info("change state to close state")
            position.switchState(ClosedState())
            
            """
            触发strategy的onEnterCanceled函数
            """
            position.getStrategy().onEnterCanceled(position)

    def isOpen(self, position):
        return True

    def exit(self, position, stopPrice=None, limitPrice=None, goodTillCanceled=None):
        assert(position.getShares() == 0)
        assert(position.getEntryOrder().isActive())
        position.getStrategy().getBroker().cancelOrder(position.getEntryOrder())

"""订单OpenState状态

"""
class OpenState(PositionState):
    def onEnter(self, position):
        entryDateTime = position.getEntryOrder().getExecutionInfo().getDateTime()
        position.setEntryDateTime(entryDateTime)

    def canSubmitOrder(self, position, order):
        # Only exit orders should be submitted in this state.
        pass

    def onOrderEvent(self, position, orderEvent):
       
        logger.getLogger("position.py/OpenState").info("call onOrderEvent")
      
        if position.getExitOrder() and position.getExitOrder().getId() == orderEvent.getOrder().getId():
            if orderEvent.getEventType() == broker.OrderEvent.Type.FILLED:
                if position.getShares() == 0:
                    position.switchState(ClosedState())
                    position.getStrategy().onExitOk(position)
            elif orderEvent.getEventType() == broker.OrderEvent.Type.CANCELED:
                assert(position.getShares() != 0)
                position.getStrategy().onExitCanceled(position)
        elif position.getEntryOrder().getId() == orderEvent.getOrder().getId():
            # Nothing to do since the entry order may be completely filled or canceled after a partial fill.
            assert(position.getShares() != 0)
        else:
            raise Exception("Invalid order event '%s' in OpenState" % (orderEvent.getEventType()))

    def isOpen(self, position):
        return True

    def exit(self, position, stopPrice=None, limitPrice=None, goodTillCanceled=None):
        assert(position.getShares() != 0)

        # Fail if a previous exit order is active.
        if position.exitActive():
            raise Exception("Exit order is active and it should be canceled first")

        # If the entry order is active, request cancellation.
        if position.entryActive():
            position.getStrategy().getBroker().cancelOrder(position.getEntryOrder())

        position._submitExitOrder(stopPrice, limitPrice, goodTillCanceled)


class ClosedState(PositionState):
    def onEnter(self, position):
        # Set the exit datetime if the exit order was filled.
        if position.exitFilled():
            exitDateTime = position.getExitOrder().getExecutionInfo().getDateTime()
            position.setExitDateTime(exitDateTime)

        assert(position.getShares() == 0)
        position.getStrategy().unregisterPosition(position)

    def canSubmitOrder(self, position, order):
        raise Exception("The position is closed")

    def onOrderEvent(self, position, orderEvent):
        raise Exception("Invalid order event '%s' in ClosedState" % (orderEvent.getEventType()))

    def isOpen(self, position):
        return False

    def exit(self, position, stopPrice=None, limitPrice=None, goodTillCanceled=None):
        pass


"""
初始化中添加参数bars  chenxiangdong,2017/07/18
"""
class Position(object):
    """Base class for positions.

    Positions are higher level abstractions for placing orders.
    They are escentially a pair of entry-exit orders and allow
    to track returns and PnL easier that placing orders manually.

    :param strategy: The strategy that this position belongs to.
    :type strategy: :class:`wk_pyalgotrade.strategy.BaseStrategy`.
    :param entryOrder: The order used to enter the position.
    :type entryOrder: :class:`wk_pyalgotrade.broker.Order`
    :param goodTillCanceled: True if the entry order should be set as good till canceled.
    :type goodTillCanceled: boolean.
    :param allOrNone: True if the orders should be completely filled or not at all.
    :type allOrNone: boolean.

    .. note::
        This is a base class and should not be used directly.
    """
    LOGGER_NAME = "Position"
    def __init__(self, strategy, bars, entryOrder, goodTillCanceled, allOrNone):
        # The order must be created but not submitted.
        
        """
        在创建position时order的状态需要是initial状态
        """
        assert(entryOrder.isInitial())
        self.__logger = logger.getLogger(Position.LOGGER_NAME)
        self.__state = None
        self.__activeOrders = {}
        self.__shares = 0
        self.__strategy = strategy
        self.__entryOrder = None
        self.__entryDateTime = None
        self.__exitOrder = None
        self.__exitDateTime = None
        self.__posTracker = returns.PositionTracker(entryOrder.getInstrumentTraits())
        self.__allOrNone = allOrNone

        self.switchState(WaitingEntryState())

        """
        goodTillCanceled为True，使用close price; 为False,使用Open price
        """
        entryOrder.setGoodTillCanceled(goodTillCanceled)
        
        """
        设置是否允许部分成交
        """
        entryOrder.setAllOrNone(allOrNone)
        
        
        """
        self.__submitAndRegisterOrder(entryOrder)
        self.__entryOrder = entryOrder
        """
   
        """
        注册订单
        """
        self.__submitAndRegisterOrder(entryOrder)
        self.__entryOrder = entryOrder
        
        """
        此处做了逻辑流程上的修改，当提交一个订单时，没有将其放入activeOrder中等待第二天的bar到来时处理
        而是直接送往broker进行处理
        主要目的是在日线策略中能够实时获得最新持仓状态（eg:在进行策略调仓时，需要实时获取买入卖出是否成功
        以及当前最新的现金状况）
        """
        self.__processOrder(strategy, entryOrder, bars)
        
    """
    新增，直接处理订单
    """    
    def __processOrder(self,strategy, order, bars):
        """
        调用broker的onBarsImpl，流水处理单个订单
        """
        strategy.getBroker().onBarsImpl(order, bars)
        

    def __submitAndRegisterOrder(self, order):
        assert(order.isInitial())

        """
        暂时注释掉关于订单状态的判断,判断下order state的变化
        """
        
        # Check if an order can be submitted in the current state.
        self.__state.canSubmitOrder(self, order)
      
        # This may raise an exception, so we wan't to submit the order before moving forward and registering
        # the order in the strategy.
        """
        订单状态从initial转为submitted
        """
        self.getStrategy().getBroker().submitOrder(order)
        self.__logger.info("order id is %d" % (order.getId()))
        self.__activeOrders[order.getId()] = order
                            
        self.getStrategy().registerPositionOrder(self, order)

    def setEntryDateTime(self, dateTime):
        self.__entryDateTime = dateTime

    def setExitDateTime(self, dateTime):
        self.__exitDateTime = dateTime

    def switchState(self, newState):
        self.__state = newState
        self.__state.onEnter(self)

    def getStrategy(self):
        return self.__strategy

    def getLastPrice(self):
        return self.__strategy.getLastPrice(self.getInstrument())

    def getActiveOrders(self):
        return list(self.__activeOrders.values())

    def getShares(self):
        """Returns the number of shares.
        This will be a possitive number for a long position, and a negative number for a short position.

        .. note::
            If the entry order was not filled, or if the position is closed, then the number of shares will be 0.
        """
        return self.__shares

    def entryActive(self):
        """Returns True if the entry order is active."""
        return self.__entryOrder is not None and self.__entryOrder.isActive()

    def entryFilled(self):
        """Returns True if the entry order was filled."""
        return self.__entryOrder is not None and self.__entryOrder.isFilled()

    def exitActive(self):
        """Returns True if the exit order is active."""
        return self.__exitOrder is not None and self.__exitOrder.isActive()

    def exitFilled(self):
        """Returns True if the exit order was filled."""
        return self.__exitOrder is not None and self.__exitOrder.isFilled()

    def getEntryOrder(self):
        """Returns the :class:`wk_pyalgotrade.broker.Order` used to enter the position."""
        return self.__entryOrder

    def getExitOrder(self):
        """Returns the :class:`wk_pyalgotrade.broker.Order` used to exit the position. If this position hasn't been closed yet, None is returned."""
        return self.__exitOrder

    def getInstrument(self):
        """Returns the instrument used for this position."""
        return self.__entryOrder.getInstrument()

    def getReturn(self, includeCommissions=True):
        """
        Calculates cumulative percentage returns up to this point.
        If the position is not closed, these will be unrealized returns.
        """

        # Deprecated in v0.18.
        if includeCommissions is False:
            warninghelpers.deprecation_warning("includeCommissions will be deprecated in the next version.", stacklevel=2)

        ret = 0
        price = self.getLastPrice()
        if price is not None:
            ret = self.__posTracker.getReturn(price, includeCommissions)
        return ret

    def getPnL(self, includeCommissions=True):
        """
        Calculates PnL up to this point.
        If the position is not closed, these will be unrealized PnL.
        """

        # Deprecated in v0.18.
        if includeCommissions is False:
            warninghelpers.deprecation_warning("includeCommissions will be deprecated in the next version.", stacklevel=2)

        ret = 0
        price = self.getLastPrice()
        if price is not None:
            ret = self.__posTracker.getPnL(price=price, includeCommissions=includeCommissions)
        return ret

    def cancelEntry(self):
        """Cancels the entry order if its active."""
        if self.entryActive():
            self.getStrategy().getBroker().cancelOrder(self.getEntryOrder())

    def cancelExit(self):
        """Cancels the exit order if its active."""
        if self.exitActive():
            self.getStrategy().getBroker().cancelOrder(self.getExitOrder())

    def exitMarket(self, goodTillCanceled=None):
        """Submits a market order to close this position.

        :param goodTillCanceled: True if the exit order is good till canceled. If False then the order gets automatically canceled when the session closes. If None, then it will match the entry order.
        :type goodTillCanceled: boolean.

        .. note::
            * If the position is closed (entry canceled or exit filled) this won't have any effect.
            * If the exit order for this position is pending, an exception will be raised. The exit order should be canceled first.
            * If the entry order is active, cancellation will be requested.
        """

        self.__state.exit(self, None, None, goodTillCanceled)

    def exitLimit(self, limitPrice, goodTillCanceled=None):
        """Submits a limit order to close this position.

        :param limitPrice: The limit price.
        :type limitPrice: float.
        :param goodTillCanceled: True if the exit order is good till canceled. If False then the order gets automatically canceled when the session closes. If None, then it will match the entry order.
        :type goodTillCanceled: boolean.

        .. note::
            * If the position is closed (entry canceled or exit filled) this won't have any effect.
            * If the exit order for this position is pending, an exception will be raised. The exit order should be canceled first.
            * If the entry order is active, cancellation will be requested.
        """

        self.__state.exit(self, None, limitPrice, goodTillCanceled)

    def exitStop(self, stopPrice, goodTillCanceled=None):
        """Submits a stop order to close this position.

        :param stopPrice: The stop price.
        :type stopPrice: float.
        :param goodTillCanceled: True if the exit order is good till canceled. If False then the order gets automatically canceled when the session closes. If None, then it will match the entry order.
        :type goodTillCanceled: boolean.

        .. note::
            * If the position is closed (entry canceled or exit filled) this won't have any effect.
            * If the exit order for this position is pending, an exception will be raised. The exit order should be canceled first.
            * If the entry order is active, cancellation will be requested.
        """

        self.__state.exit(self, stopPrice, None, goodTillCanceled)

    def exitStopLimit(self, stopPrice, limitPrice, goodTillCanceled=None):
        """Submits a stop limit order to close this position.

        :param stopPrice: The stop price.
        :type stopPrice: float.
        :param limitPrice: The limit price.
        :type limitPrice: float.
        :param goodTillCanceled: True if the exit order is good till canceled. If False then the order gets automatically canceled when the session closes. If None, then it will match the entry order.
        :type goodTillCanceled: boolean.

        .. note::
            * If the position is closed (entry canceled or exit filled) this won't have any effect.
            * If the exit order for this position is pending, an exception will be raised. The exit order should be canceled first.
            * If the entry order is active, cancellation will be requested.
        """

        self.__state.exit(self, stopPrice, limitPrice, goodTillCanceled)

    def _submitExitOrder(self, stopPrice, limitPrice, goodTillCanceled):
        assert(not self.exitActive())

        exitOrder = self.buildExitOrder(stopPrice, limitPrice)

        # If goodTillCanceled was not set, match the entry order.
        if goodTillCanceled is None:
            goodTillCanceled = self.__entryOrder.getGoodTillCanceled()
        exitOrder.setGoodTillCanceled(goodTillCanceled)

        exitOrder.setAllOrNone(self.__allOrNone)

        self.__submitAndRegisterOrder(exitOrder)
        self.__exitOrder = exitOrder

    def onOrderEvent(self, orderEvent):
    
        logger.getLogger("position.py/onOrderEvent").info("call onOrderEvent")
      
        
        self.__updatePosTracker(orderEvent)
        order = orderEvent.getOrder()
        if not order.isActive():
            
            logger.getLogger("position.py/onOrderEvent").info('del activeOrders')
            del self.__activeOrders[order.getId()]

        # Update the number of shares.
        if orderEvent.getEventType() in (broker.OrderEvent.Type.PARTIALLY_FILLED, broker.OrderEvent.Type.FILLED):
            execInfo = orderEvent.getEventInfo()
            # roundQuantity is used to prevent bugs like the one triggered in testcases.bitstamp_test:TestCase.testRoundingBug
            if order.isBuy():
                
                self.__logger.info("update buy shares")
                self.__shares = order.getInstrumentTraits().roundQuantity(self.__shares + execInfo.getQuantity())
            else:
                
                self.__logger.info("update sells shares")
                self.__shares = order.getInstrumentTraits().roundQuantity(self.__shares - execInfo.getQuantity())
      
        self.__state.onOrderEvent(self, orderEvent)

    def __updatePosTracker(self, orderEvent):
        if orderEvent.getEventType() in (broker.OrderEvent.Type.PARTIALLY_FILLED, broker.OrderEvent.Type.FILLED):
            order = orderEvent.getOrder()
            execInfo = orderEvent.getEventInfo()
            if order.isBuy():
                """
                logger.getLogger("position.py/__updatePosTrackers").info("call __posTracker.buy")
                """
                self.__posTracker.buy(execInfo.getQuantity(), execInfo.getPrice(), execInfo.getCommission())
            else:
                """
                logger.getLogger("position.py/__updatePosTrackers").info("call __posTracker.sells")
                """
                self.__posTracker.sell(execInfo.getQuantity(), execInfo.getPrice(), execInfo.getCommission())

    def buildExitOrder(self, stopPrice, limitPrice):
        raise NotImplementedError()

    def isOpen(self):
        """Returns True if the position is open."""
        return self.__state.isOpen(self)

    def getAge(self):
        """Returns the duration in open state.

        :rtype: datetime.timedelta.

        .. note::
            * If the position is open, then the difference between the entry datetime and the datetime of the last bar is returned.
            * If the position is closed, then the difference between the entry datetime and the exit datetime is returned.
        """
        ret = datetime.timedelta()
        if self.__entryDateTime is not None:
            if self.__exitDateTime is not None:
                last = self.__exitDateTime
            else:
                last = self.__strategy.getCurrentDateTime()
            ret = last - self.__entryDateTime
        return ret
    
    


"""
初始化中添加参数bars  chenxiangdong,2017/07/18
"""
# This class is reponsible for order management in long positions.
"""
原版中marketOrder不允许goodTillCanceled，即不允许使用close price
暂时取消这条限制
"""
class LongPosition(Position):
    def __init__(self, strategy, bars, instrument, stopPrice, limitPrice, quantity, goodTillCanceled, allOrNone, msg=None):
        if limitPrice is None and stopPrice is None:
            entryOrder = strategy.getBroker().createMarketOrder(
                broker.Order.Action.BUY, instrument, quantity, goodTillCanceled, msg=msg)
        elif limitPrice is not None and stopPrice is None:
            entryOrder = strategy.getBroker().createLimitOrder(broker.Order.Action.BUY, instrument, limitPrice, quantity, msg=msg)
        elif limitPrice is None and stopPrice is not None:
            entryOrder = strategy.getBroker().createStopOrder(broker.Order.Action.BUY, instrument, stopPrice, quantity)
        elif limitPrice is not None and stopPrice is not None:
            entryOrder = strategy.getBroker().createStopLimitOrder(broker.Order.Action.BUY, instrument, stopPrice, limitPrice, quantity)
        else:
            assert False
        super(LongPosition, self).__init__(strategy, bars, entryOrder, goodTillCanceled, allOrNone)

    def buildExitOrder(self, stopPrice, limitPrice):
        quantity = self.getShares()
        assert(quantity > 0)
        if limitPrice is None and stopPrice is None:
            ret = self.getStrategy().getBroker().createMarketOrder(broker.Order.Action.SELL, self.getInstrument(), quantity, False)
        elif limitPrice is not None and stopPrice is None:
            ret = self.getStrategy().getBroker().createLimitOrder(broker.Order.Action.SELL, self.getInstrument(), limitPrice, quantity)
        elif limitPrice is None and stopPrice is not None:
            ret = self.getStrategy().getBroker().createStopOrder(broker.Order.Action.SELL, self.getInstrument(), stopPrice, quantity)
        elif limitPrice is not None and stopPrice is not None:
            ret = self.getStrategy().getBroker().createStopLimitOrder(broker.Order.Action.SELL, self.getInstrument(), stopPrice, limitPrice, quantity)
        else:
            assert(False)

        return ret

"""
初始化中添加参数bars  chenxiangdong,2017/07/18
对市价单增加goofTillCanceled参数， 20171016
"""
# This class is reponsible for order management in short positions.
class ShortPosition(Position):
    def __init__(self, strategy, bars,instrument, stopPrice, limitPrice, quantity, goodTillCanceled, allOrNone, inBar=False, msg=None):
        if limitPrice is None and stopPrice is None:
            entryOrder = strategy.getBroker().createMarketOrder(
                broker.Order.Action.SELL_SHORT, instrument, quantity, goodTillCanceled, msg=msg)
        elif limitPrice is not None and stopPrice is None:
            entryOrder = strategy.getBroker().createLimitOrder(broker.Order.Action.SELL_SHORT, instrument, limitPrice, quantity, inBar=inBar, msg=msg)
        elif limitPrice is None and stopPrice is not None:
            entryOrder = strategy.getBroker().createStopOrder(broker.Order.Action.SELL_SHORT, instrument, stopPrice, quantity)
        elif limitPrice is not None and stopPrice is not None:
            entryOrder = strategy.getBroker().createStopLimitOrder(broker.Order.Action.SELL_SHORT, instrument, stopPrice, limitPrice, quantity)
        else:
            assert False
        super(ShortPosition, self).__init__(strategy,bars,entryOrder, goodTillCanceled, allOrNone)

    def buildExitOrder(self, stopPrice, limitPrice):
        quantity = self.getShares() * -1
        assert(quantity > 0)
        if limitPrice is None and stopPrice is None:
            ret = self.getStrategy().getBroker().createMarketOrder(broker.Order.Action.BUY_TO_COVER, self.getInstrument(), quantity, False)
        elif limitPrice is not None and stopPrice is None:
            ret = self.getStrategy().getBroker().createLimitOrder(broker.Order.Action.BUY_TO_COVER, self.getInstrument(), limitPrice, quantity)
        elif limitPrice is None and stopPrice is not None:
            ret = self.getStrategy().getBroker().createStopOrder(broker.Order.Action.BUY_TO_COVER, self.getInstrument(), stopPrice, quantity)
        elif limitPrice is not None and stopPrice is not None:
            ret = self.getStrategy().getBroker().createStopLimitOrder(broker.Order.Action.BUY_TO_COVER, self.getInstrument(), stopPrice, limitPrice, quantity)
        else:
            assert False
        return ret


class DummyPosition(LongPosition):
    def __init__(self, strategy, bars, instrument, stopPrice, limitPrice, quantity, goodTillCanceled, allOrNone, msg=None):
        super().__init__(strategy, bars, instrument, stopPrice, limitPrice, quantity, goodTillCanceled, allOrNone, msg=msg)
