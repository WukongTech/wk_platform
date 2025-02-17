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


Date:20170720
Author:chenxiangdong
comment:
策略分析以及回测报告记录
"""
import math
import copy
from collections import deque
from enum import Enum

import pandas as pd

from pyalgotrade import observer
from pyalgotrade import dataseries
from wk_util import logger
from wk_platform import stratanalyzer
from wk_platform.stratanalyzer.record import UnfilledOrderInfo
from wk_platform.stratanalyzer.record import TransactionRecord
from wk_platform.stratanalyzer.record import DetailedPositionRecord
from wk_util.recorder import dataclass_list_to_dataframe


class DetailedPosistionTrackLevel(Enum):
    OFF = 0
    TRADE_DAY = 1
    EVERYDAY = 2
    LAST_TRADE_DAY = 3

# Helper class to calculate time-weighted returns in a portfolio.
# Check http://www.wikinvest.com/wiki/Time-weighted_return
class TimeWeightedReturns(object):
    def __init__(self, initialValue):
        self.__lastValue = initialValue
        self.__flows = 0.0
        self.__lastPeriodRet = 0.0
        self.__cumRet = 0.0
        
        """
        #添加年化收益率
        self.__annualReturnRate = 0.0
        #self.__countDays = 1
        """

    def deposit(self, amount):
        self.__flows += amount

    def withdraw(self, amount):
        self.__flows -= amount

    def getCurrentValue(self):
        return self.__lastValue

    # Update the value of the portfolio.
    def update(self, currentValue):
        """
        logger.getLogger("TimeWeightedReturns").info("call update portfolio")
        """
        
        if self.__lastValue:
            retSubperiod = (currentValue - self.__lastValue - self.__flows) / float(self.__lastValue)
        else:
            retSubperiod = 0.0

        self.__cumRet = (1 + self.__cumRet) * (1 + retSubperiod) - 1
        self.__lastPeriodRet = retSubperiod
        self.__lastValue = currentValue
        self.__flows = 0.0
        
        """
        #新增
        self.__countDays += 1
        """

    def getLastPeriodReturns(self):
        return self.__lastPeriodRet

    # Note that this value is not annualized.
    def getCumulativeReturns(self):
        return self.__cumRet
    
    """
    #新增
    def getAnnualReturnRate(self):
        return self.__cumRet/(self.__countDays*1.0/365)
    """
    

# Helper class to calculate PnL and returns over a single instrument (not the whole portfolio).

#记录一次交易的信息
class PositionTracker(object):
    def __init__(self, instrumentTraits):

        self.__instrumentTraits = instrumentTraits
        self.reset()

    def reset(self):
        self.__pnl = 0.0
        self.__avgPrice = 0.0  # Volume weighted average price per share.
        self.__position = 0.0
        self.__commissions = 0.0
        self.__totalCommited = 0.0  # The total amount commited to this position.

    def getPosition(self):
        return self.__position

    def getAvgPrice(self):
        return self.__avgPrice

    def getCommissions(self):
        return self.__commissions

    def getPnL(self, price=None, includeCommissions=True):
        """
        Return the PnL that would result if closing the position a the given price.
        Note that this will be different if commissions are used when the trade is executed.
        """

        ret = self.__pnl
        if price:
            ret += (price - self.__avgPrice) * self.__position
        if includeCommissions:
            ret -= self.__commissions
        return ret

    def getReturn(self, price=None, includeCommissions=True):
        ret = 0
        pnl = self.getPnL(price=price, includeCommissions=includeCommissions)
        if self.__totalCommited != 0:
            ret = pnl / float(self.__totalCommited)
        return ret

    def __openNewPosition(self, quantity, price):
        self.__avgPrice = price
        self.__position = quantity
        self.__totalCommited = self.__avgPrice * abs(self.__position)

    def __extendCurrentPosition(self, quantity, price):
        newPosition = self.__instrumentTraits.roundQuantity(self.__position + quantity)
        self.__avgPrice = (self.__avgPrice*abs(self.__position) + price*abs(quantity)) / abs(float(newPosition))
        self.__position = newPosition
        self.__totalCommited = self.__avgPrice * abs(self.__position)

    def __reduceCurrentPosition(self, quantity, price):
        # Check that we're closing or reducing partially
        assert self.__instrumentTraits.roundQuantity(abs(self.__position) - abs(quantity)) >= 0
        pnl = (price - self.__avgPrice) * quantity * -1

        self.__pnl += pnl
        self.__position = self.__instrumentTraits.roundQuantity(self.__position + quantity)
        if self.__position == 0:
            self.__avgPrice = 0.0

    def update(self, quantity, price, commission):
        """
        logger.getLogger("returns/positionTracker").info("call update")
        """
        
        assert quantity != 0, "Invalid quantity"
        assert price > 0, "Invalid price"
        assert commission >= 0, "Invalid commission"

        if self.__position == 0:
            """
            logger.getLogger("returns/positionTracker/update").info("openNewPosition")
            """
            self.__openNewPosition(quantity, price)
        else:
            # Are we extending the current position or going in the opposite direction ?
            currPosDirection = math.copysign(1, self.__position)
            tradeDirection = math.copysign(1, quantity)

            if currPosDirection == tradeDirection:
                self.__extendCurrentPosition(quantity, price)
            else:
                # If we're going in the opposite direction we could be:
                # 1: Partially reducing the current position.
                # 2: Completely closing the current position.
                # 3: Completely closing the current position and opening a new one in the opposite direction.
                if abs(quantity) <= abs(self.__position):
                    self.__reduceCurrentPosition(quantity, price)
                else:
                    newPos = self.__position + quantity
                    self.__reduceCurrentPosition(self.__position*-1, price)
                    self.__openNewPosition(newPos, price)

        self.__commissions += commission

    def buy(self, quantity, price, commission=0.0):
        assert quantity > 0, "Invalid quantity"
        self.update(quantity, price, commission)

    def sell(self, quantity, price, commission=0.0):
        assert quantity > 0, "Invalid quantity"
        self.update(quantity * -1, price, commission)



class ReturnsAnalyzerBase(stratanalyzer.StrategyAnalyzer):
    """
    策略回报分析类   chenxiangdong 20170720
    """
    LOGGER_NAME = 'ReturnsAnalyzerBase'
    def __init__(self):
        super(ReturnsAnalyzerBase, self).__init__()
        self.__event = observer.Event()
        self.__portfolioReturns = None
        
        self.__logger = logger.getLogger(ReturnsAnalyzerBase.LOGGER_NAME, disable=True)

    @classmethod
    def getOrCreateShared(cls, strat):
        """
        logger.getLogger("returnsAnalyzerBase").info("call getOrCreateShared")
        """
        
        name = cls.__name__
        # Get or create the shared ReturnsAnalyzerBase.
        ret = strat.getNamedAnalyzer(name)
        if ret is None:

            ret = ReturnsAnalyzerBase()
            strat.attachAnalyzerEx(ret, name)
        return ret

    def attached(self, strat):
        
        self.__logger.info("call attached")
        self.__portfolioReturns = TimeWeightedReturns(strat.getBroker().getEquity())


    # An event will be notified when return are calculated at each bar. The hander should receive 1 parameter:
    # 1: The current datetime.
    # 2: This analyzer's instance
    def getEvent(self):
        return self.__event

    def getNetReturn(self):
        return self.__portfolioReturns.getLastPeriodReturns()

    def getCumulativeReturn(self):
        return self.__portfolioReturns.getCumulativeReturns()

    # def track_detailed_position(self, strat, bars):
    #     """
    #     记录调用当天的持仓信息
    #     """
    #     broker = strat.getBroker()
    #     datetime_str = bars.getDateTime().strftime("%Y-%m-%d")
    #     sharesDict = copy.deepcopy(broker.getPositions())
    #     for instrument, shares in sharesDict.items():
    #         sec_name = bars[instrument].getSecName() # 持仓应当包含在当日行情中，否则应抛出错误
    #         # 获取当天的股票持仓，当存在持仓时输出该股票的具体信息
    #         shares = broker.getShares(instrument)
    #         if shares == 0:
    #             continue
    #         shares_sellable = broker.getSharesCanSell(instrument)
    #         open_price = bars[instrument].getOpen()
    #         close_price = bars[instrument].getClose()
    #         last_buy_time = broker.getLastBuyTime(instrument)
    #         last_sell_time = broker.getLastSellTime(instrument)
    #         position_cost = broker.getPositionCost(instrument)
    #         position_pnl = broker.getPositionDelta(bars, instrument)
    #
    #         pos_rec = DetailedPositionRecord(
    #             trade_dt=datetime_str,
    #             windcode=instrument,
    #             sec_name=sec_name,
    #             position=shares,
    #             sellable=shares_sellable,
    #             open_price=open_price,
    #             close_price=close_price,
    #             last_buy=last_buy_time,
    #             last_sell=last_sell_time,
    #             cost=position_cost,
    #             pnl=position_pnl
    #         )
    #
    #         self.__detailed_position_tracker.append(pos_rec)


    def beforeOnBars(self, strat, bars):
        self.__logger.info("beforeOnBars")

    def after_on_bars(self, strat, bars):
        """
        ！！！特别注意：注意此函数实际执行顺序在onBars之后
        该函数在每个bars处理完后调用，用来统计今天的结果及输出
        """

        self.__logger.info("after on bars")

        self.__portfolioReturns.update(strat.getBroker().getEquity())

        # Notify that new returns are available.

        """
        个股持仓情况的跟踪：
        1. 当开启个股持仓跟踪时，记录每一天个股的变化
        2. 当不开启个股持仓跟踪时，只记录最后一天个股的持仓情况
        """
        dateTimeTemp = bars.getDateTime().strftime("%Y-%m-%d")
        sharesDict = copy.deepcopy(strat.getBroker().getPositions())

        # if self.__detailed_position_track_level== DetailedPosistionTrackLevel.TRADE_DAY:
        #     # 在交易日记录个股持仓
        #     datetime_str = bars.getDateTime().strftime("%Y%m%d")
        #     if datetime_str in strat.getBuyDate():
        #         self.track_detailed_position(strat, bars)
        # elif self.__detailed_position_track_level == DetailedPosistionTrackLevel.EVERYDAY:
        #     # 每天都记录个股持仓
        #     self.track_detailed_position(strat, bars)
        # elif self.__detailed_position_track_level == DetailedPosistionTrackLevel.LAST_TRADE_DAY:
        #     # 在最后一个交易日记录个股持仓
        #     datetime_str = bars.getDateTime().strftime("%Y%m%d")
        #     if datetime_str == strat.getBuyDate()[-1]:
        #         self.track_detailed_position(strat, bars)
        # else:
        #     pass

         
            

        # 总资产总现金的跟踪
        # self.__pdTradeTrackerAnalyzer = strat.getBroker().getPdTradeShares()
        # self.__unfilled_orders = strat.getBroker().unfilled_orders
        # self.__transaction_tracker = strat.getBroker().transaction_tracker

        totalEquity = strat.getBroker().getEquity()
        positions = strat.getBroker().getPositions()
        cash = strat.getBroker().getCash()
        sharesValue = strat.getBroker().getSharesValue()
        positionRatio = sharesValue * 1.0 / totalEquity


        # dateTimeTemp = bars.getDateTime().strftime("%Y-%m-%d")
        # #每日总资产跟踪
        # df1= pd.DataFrame([[dateTimeTemp,totalEquity,sharesValue,cash, positionRatio]], columns = ['日期', '总资产','总市值', '总现金', '总仓位'])
        # self.__pdSharesAnalyzer =  self.__pdSharesAnalyzer.append(df1, True)
        #
        #
        # timeTemp = str(dateTimeTemp)
        # timeList = []
        # timeList.append(timeTemp)
        # df2 = pd.DataFrame(positions, index=timeList)
        # self.__pdDetails = self.__pdDetails.append(df2)
                
           
    
        self.__event.emit(bars.getDateTime(), self)
       
        
       
    #newly add
    """
    def getAnnualReturn(self):
        return self.__portfolioReturns.getAnnualReturnRate()
    """
    # def getPdTradeTracker(self):
    #     return self.__pdTradeTrackerAnalyzer
    #
    # def getPdShareTracker(self):
    #     return self.__pdSharesAnalyzer
    #
    # def getPdDetails(self):
    #     return self.__pdDetails

    # #详细持仓信息
    # def getPdDetailPositions(self):
    #     return self.__pdDetailPositions

    # @property
    # def detailed_position_tracker(self):
    #     """
    #     详细持仓信息
    #     """
    #     return self.__detailed_position_tracker
    #
    # @property
    # def transaction_tracker(self):
    #     return self.__transaction_tracker
    #
    # @property
    # def unfilled_orders(self):
    #     return self.__unfilled_orders





class Returns(stratanalyzer.StrategyAnalyzer):
    """
    A :class:`wk_pyalgotrade.stratanalyzer.StrategyAnalyzer` that calculates time-weighted returns for the
    whole portfolio.

    :param maxLen: The maximum number of values to hold in net and cumulative returs dataseries.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.
    """
    LOGGER_NAME = 'Returns'
    def __init__(self, maxLen=None):
        
        super(Returns, self).__init__()
        self.__netReturns = dataseries.SequenceDataSeries(maxLen=maxLen)
        self.__cumReturns = dataseries.SequenceDataSeries(maxLen=maxLen)
        
        
        """
        #newly add
        self.__annualReturns = dataseries.SequenceDataSeries(maxLen = maxLen)
        """
        self.__logger = logger.getLogger(Returns.LOGGER_NAME, disable=True)
        
        self.__onoff = True
        self.__pdSharesAnalyzerTemp = pd.DataFrame(columns = ['日期', '总资产','总市值', '总现金'])

        self.__pdTradeTrackerAnalyzerTemp = pd.DataFrame(columns=[
            '交易日期', '证券代码','证券名称','成交价格','成交数量','佣金花费','印花税花费','成交方向'
        ])
        self.__transaction_tracker = None

        self.__pdDetailsTemp = pd.DataFrame()
        # self.__pdDetailPositionsTemp = pd.DataFrame(columns=[
        #     '交易日期', '证券代码', '证券名称', '持仓数目','可卖数目',
        #     '最新开盘价格', '最新收盘价格', '最近买入','最近卖出','持仓成本','持仓盈亏'
        # ])

        self.__detailed_position_tracker = None

        self.__unfilled_orders = None
        
        """
        每年交易日
        """
        self.__tradeDaysOneYear = 365

    def beforeAttach(self, strat):
        # Get or create a shared ReturnsAnalyzerBase
       
        # self.__logger.info("call in beforeAttach")
        #
        # analyzer = ReturnsAnalyzerBase.getOrCreateShared(strat)
        # analyzer.getEvent().subscribe(self.__onReturns)

        # self.__pdTradeTrackerAnalyzerTemp = analyzer.getPdTradeTracker()
        # self.__pdSharesAnalyzerTemp = analyzer.getPdShareTracker()
        # self.__pdDetailsTemp = analyzer.getPdDetails()
        # self.__detailed_posistion_tracker = analyzer.detailed_position_tracker
        # self.__transaction_tracker = analyzer.transaction_tracker
        # self.__unfilled_orders = analyzer.unfilled_orders
        pass



    def __onReturns(self, dateTime, returnsAnalyzerBase):
        
        
        self.__logger.info("call in __onReturns")
        
        self.__netReturns.appendWithDateTime(dateTime, returnsAnalyzerBase.getNetReturn())
        self.__cumReturns.appendWithDateTime(dateTime, returnsAnalyzerBase.getCumulativeReturn())
        
        # TODO: 似乎每个交易日都会调用，需要检查
        # if self.__onoff == True:
        #     self.__pdTradeTrackerAnalyzerTemp = returnsAnalyzerBase.getPdTradeTracker()
        #     self.__pdSharesAnalyzerTemp = returnsAnalyzerBase.getPdShareTracker()
        #     self.__pdDetailsTemp = returnsAnalyzerBase.getPdDetails()
        #     # self.__pdDetailPositionsTemp = returnsAnalyzerBase.getPdDetailPositions()
        #     self.__detailed_position_tracker = returnsAnalyzerBase.detailed_position_tracker
        #     self.__transaction_tracker = returnsAnalyzerBase.transaction_tracker
        #     self.__unfilled_orders = returnsAnalyzerBase.unfilled_orders
        #

    def getReturns(self):
        """Returns a :class:`wk_pyalgotrade.dataseries.DataSeries` with the returns for each bar."""
        return self.__netReturns

    def getCumulativeReturns(self):
        """Returns a :class:`wk_pyalgotrade.dataseries.DataSeries` with the cumulative returns for each bar."""
        return self.__cumReturns
    
    #新增
    def getAnnualReturns(self):
        #return self.__annualReturns
        #print self.getCumulativeReturns().getDateTimes()[-1],self.getCumulativeReturns().getDateTimes()[0]
        days = (self.getCumulativeReturns().getDateTimes()[-1] - self.getCumulativeReturns().getDateTimes()[0] ).days+1
        """
        此处需要利用复利进行计算
        """
        """
        print self.__cumReturns[-1]
        print self.__tradeDaysOneYear*1.0/days
        print  math.pow(self.__cumReturns[-1],self.__tradeDaysOneYear*1.0/days)
        """
        return math.pow(self.__cumReturns[-1]+1,self.__tradeDaysOneYear*1.0/days) -1
    
    
    def getPdTradeTrackerTemp(self):
        return self.__pdTradeTrackerAnalyzerTemp

    def getPdSharesTemp(self):
        return self.__pdSharesAnalyzerTemp
    
    def getPdDetailsTemp(self):
        return self.__pdDetailsTemp
    
    # #详细持仓
    # def getPdDetailPositionsTemp(self):
    #     return self.__pdDetailPositionsTemp





