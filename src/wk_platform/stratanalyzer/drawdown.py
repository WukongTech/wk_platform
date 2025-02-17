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

from wk_platform import stratanalyzer

import datetime
import pandas as pd


class DrawDownHelper(object):
    def __init__(self):
        
    
        self.__highWatermark = None
        self.__lowWatermark = None
        self.__lastLow = None
        self.__highDateTime = None
        self.__lastDateTime = None
        
        self.__lowDateTime = None
        
        
    """
    获取最长回撤时间
    """
    # The drawdown duration, not necessarily the max drawdown duration.
    def getDuration(self):
    
        #return self.__lastDateTime - self.__highDateTime
        return self.__lowDateTime - self.__highDateTime

    """
    获取最大回撤率
    """
    def getMaxDrawDown(self):
        return (self.__lowWatermark - self.__highWatermark) / float(self.__highWatermark)

    def getCurrentDrawDown(self):
        return (self.__lastLow - self.__highWatermark) / float(self.__highWatermark)

    def update(self, dateTime, low, high):
        assert(low <= high)
        self.__lastLow = low
        self.__lastDateTime = dateTime
        

        """
        创新高时需要重新记录高点
        """
        if self.__highWatermark is None or high >= self.__highWatermark:
            
            self.__highWatermark = high
            self.__lowWatermark = low
            self.__highDateTime = dateTime
            
            self.__lowDateTime = dateTime
            

        else:
            self.__lowWatermark = min(self.__lowWatermark, low)
            
            """
            记录低点信息
            """
            if self.__lowWatermark == low:
                self.__lowDateTime = dateTime
            
      
    #newly add, get datetime
    def getHighDateTime(self):
        return self.__highDateTime
    def getLastDateTime(self):
        return self.__lastDateTime
    def getLowDateTime(self):
        return self.__lowDateTime
    

    

class DrawDown(stratanalyzer.StrategyAnalyzer):
    """A :class:`wk_pyalgotrade.stratanalyzer.StrategyAnalyzer` that calculates
    max. drawdown and longest drawdown duration for the portfolio."""

    def __init__(self):
        super(DrawDown, self).__init__()
        self.__maxDD = 0
        self.__longestDDDuration = datetime.timedelta()
        self.__currDrawDown = DrawDownHelper()
        
        #newly add
        self.__highDateTimes = 0
        self.__lowDateTimes = 0
        

    def calculateEquity(self, strat):
        return strat.getBroker().getEquity()
        # ret = strat.getBroker().getCash()
        # for instrument, shares in strat.getBroker().getPositions().iteritems():
        #     _bar = strat.getFeed().getLastBar(instrument)
        #     if shares > 0:
        #         ret += strat.getBroker().getBarLow(_bar) * shares
        #     elif shares < 0:
        #         ret += strat.getBroker().getBarHigh(_bar) * shares
        # return ret

    """
    2017.08.10 , chenxiangdong
    """
    def getDaysBetween(self, begin, end):
        delta = abs(end - begin)
        ret = delta.days + 1
        return ret


    def beforeOnBars(self, strat, bars):
        
        
        """
        获取当前总资产
        """
        equity = self.calculateEquity(strat)
        self.__currDrawDown.update(bars.getDateTime(), equity, equity)
    
        #self.__longestDDDuration = max(self.__longestDDDuration, self.__currDrawDown.getDuration())
        
        """
        self.__maxDD为负数
        """
        #print self.__currDrawDown.getLastDateTime(),self.__currDrawDown.getDuration()
        self.__maxDD = min(self.__maxDD, self.__currDrawDown.getMaxDrawDown())
        
      
        if self.__maxDD == self.__currDrawDown.getMaxDrawDown():
            self.__highDateTimes = self.__currDrawDown.getHighDateTime()
            self.__lowDateTimes = self.__currDrawDown.getLowDateTime()
        
        """
        最大回撤时长与最长回撤时间的定义不同
        chenxiangdong 20170721
        """
        #self.__longestDDDuration = self.__lowDateTimes - self.__highDateTimes
        self.__longestDDDuration = self.getDaysBetween(pd.to_datetime(self.__lowDateTimes), pd.to_datetime(self.__highDateTimes)) -1
        
      
        
       

    def getMaxDrawDown(self):
        """Returns the max. (deepest) drawdown."""
        return abs(self.__maxDD)

    def getLongestDrawDownDuration(self):
        """Returns the duration of the longest drawdown.

        :rtype: :class:`datetime.timedelta`.

        .. note::
            Note that this is the duration of the longest drawdown, not necessarily the deepest one.
        """
        return self.__longestDDDuration
    
    #newly add
    def getHighDateTimes(self):
        return self.__highDateTimes
    def getLastDateTimes(self):
        return self.__lastDateTimes
    def getLowDateTimes(self):
        return self.__lowDateTimes
    
    
def draw_down(s, base=None):
    if len(s) == 0:
        return []
    if base is None:
        current_max = s[0]
    else:
        current_max = base
    draw_down_s = []
    for i in range(0, len(s)):
        if s[i] >= current_max:
            current_max = s[i]
            draw_down_s.append(0)
        else:
            draw_down_s.append(s[i] / current_max - 1)
    return draw_down_s
