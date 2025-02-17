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
Date:20170719
Author:chenxiangdong
comment: 经过重新封装的数据bar类型，数据源为wind资讯数据库
"""

import abc
from abc import ABCMeta

import numpy as np

from pyalgotrade.bar import Bar
from wk_platform.config import MaxUpDownType
from wk_platform.config import PriceType, StrategyConfiguration
from wk_data.constants import ExtStatus, SuspensionType


class WindBarMixin(object, metaclass=abc.ABCMeta):
    """
    在windfeed中新增的列
    """
    @abc.abstractmethod
    def getNewColumn(self):
        raise NotImplementedError()
        
    @abc.abstractmethod
    def getTradeStatus(self):
        """ 返回股票当天的交易状态 """
        raise NotImplementedError()
    
    @abc.abstractmethod
    def getUpDownStatus(self, max_up_down_type=MaxUpDownType.NONE):
        """返回股票涨跌停状态"""
        raise NotImplementedError()
        
    @abc.abstractmethod
    def getSecName(self):
        """返回股票名称"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_price(self, price_type=PriceType.CLOSE, adjusted=False):
        """
        根据配置返回所需的价格
        """
        raise NotImplementedError()


class StockBar(Bar, WindBarMixin):
    # Optimization to reduce memory footprint.
    __slots__ = (
        '__dateTime',
        '__open',
        '__close',
        '__high',
        '__low',
        '__volume',
        '__amount',
        '__adj_factor',
        
        #新增列
        '_new_column',
        '_tradeStatus',
        '_maxUpDownStatus',
        '_sec_name',

        '__frequency',
        '__useAdjustedValue',
        '__extra',
    )

    #新增输入参数
    def __init__(self, date_time, pre_close, open_, high, low, close, volume, adj_factor,
                 new_column, st, suspension, max_up_down, sec_name, frequency, ext_status=ExtStatus.NORMAL, extra=None):
        
        #暂时注释掉，数据源的清洗还没完成
        """
        if high < low:
            raise Exception("high < low on %s" % (dateTime))
        elif high < open_:
            raise Exception("high < open on %s" % (dateTime))
        elif high < close:
            raise Exception("high < close on %s" % (dateTime))
        elif low > open_:
            raise Exception("low > open on %s" % (dateTime))
        elif low > close:
            raise Exception("low > close on %s" % (dateTime))
        """

        
        self.__dateTime = date_time
        self.__pre_close = pre_close
        self.__open = open_
        self.__close = close
        self.__high = high
        self.__low = low
        self.__volume = volume
        self.__adj_factor = adj_factor
       
        #新增列
        self.__new_column = new_column                 # 判断当天是否可交易，总开关
        self.__st = st
        self.__tradeStatus = suspension               # 判断股票今天交易状态，停牌处理
        self.__maxUpDownStatus = max_up_down       # 股票当天涨跌停情况处理
        self.__sec_name = sec_name                     # 股票名称
        self.__ext_status = ext_status

        self.__frequency = frequency
        self.__useAdjustedValue = True

        if extra is None:
            extra = {}
        self.__extra = extra
       

    def __setstate__(self, state):
        ( self.__dateTime,
          self.__open,
          self.__close,
          self.__high,
          self.__low,
          self.__volume,
          self.__amount,
          self.__adj_factor,


          #新增列
          self.__new_column,
          self.__st,
          self.__tradeStatus,
          self.__maxUpDownStatus,
          self.__sec_name,
          self.__ext_status,


          self.__frequency,
          self.__useAdjustedValue,
          self.__extra) = state

    def __getstate__(self):
        return (
            self.__dateTime,
            self.__open,
            self.__close,
            self.__high,
            self.__low,
            self.__volume,
            self.__amount,
            self.__adj_factor,

            self.__new_column,
            self.__st,
            self.__tradeStatus,
            self.__maxUpDownStatus,
            self.__sec_name,
            self.__ext_status,
            
            
            self.__frequency,
            self.__useAdjustedValue,
            self.__extra
        )

    def setUseAdjustedValue(self, useAdjusted):
        if useAdjusted and self.__adj_factor is None:
            raise Exception("Adjusted close is not available")
        self.__useAdjustedValue = useAdjusted

    def getUseAdjValue(self):
        return self.__useAdjustedValue

    def getDateTime(self):
        return self.__dateTime

    def getOpen(self, adjusted=True):
        if adjusted:
            if self.__adj_factor is None:
                raise Exception("Adjusted close is missing")
             
            """    
            logger.getLogger("bar.py").info("adjClose price is %f" %(self.__adjClose))
            logger.getLogger("bar.py").info("close price is %f" %(self.__close))
            """
            return self.__adj_factor * self.__open
        else:
            return self.__open

    def getHigh(self, adjusted=True):
        if adjusted:
            if self.__adj_factor is None:
                raise Exception("Adjusted close is missing")
            return self.__adj_factor * self.__high
        else:
            return self.__high

    def getLow(self, adjusted=True):
        if adjusted:
            if self.__adj_factor is None:
                raise Exception("Adjusted close is missing")
            return self.__adj_factor * self.__low
        else:
            return self.__low

    def getClose(self, adjusted=True):
        if adjusted:
            if self.__adj_factor is None:
                raise Exception("Adjusted close is missing")
            return self.__adj_factor * self.__close
        else:
            return self.__close

    def getPreClose(self, adjusted=True):
        if adjusted:
            if self.__adj_factor is None:
                raise Exception("Adjusted close is missing")
            return self.__adj_factor * self.__pre_close
        else:
            return self.__pre_close

    def getVolume(self):
        return self.__volume

    def getAdjClose(self):
        return self.__adj_factor * self.__close

    @property
    def adj_factor(self):
        return self.__adj_factor

    @property
    def st(self):
        return self.__st

    @property
    def ext_status(self):
        return self.__ext_status
    
    #新增函数
    def getNewColumn(self):
        return self.__new_column
 
    def getTradeStatus(self):
        """ 返回股票当天的交易状态 """
        return self.__tradeStatus
    
    def getUpDownStatus(self, max_up_down_type=MaxUpDownType.NONE):
        """
        返回股票涨跌停状态
        TODO: 考虑使用预处理而不是实时处理
        """
        if self.__maxUpDownStatus == 0:
            return 0
        if max_up_down_type == MaxUpDownType.STRICT:
            return self.__maxUpDownStatus
        elif max_up_down_type == MaxUpDownType.NONE:
            return 0

        elif max_up_down_type == MaxUpDownType.RELAX_OPEN:
            if self.__maxUpDownStatus == 1:  # 存在涨停的情况
                if self.getOpen() < self.getHigh():
                    return 0
                else:
                    return 1
            elif self.__maxUpDownStatus == -1: # 存在跌停的情况
                if self.getOpen() > self.getLow():
                    return 0
                else:
                    return -1
            else:
                raise Exception(f"Invalid max-up-down status `{self.__maxUpDownStatus}`")
        elif max_up_down_type == MaxUpDownType.RELAX_CLOSE:
            if self.__maxUpDownStatus == 1:  # 存在涨停的情况
                if self.getClose() < self.getHigh():
                    return 0
                else:
                    return 1
            elif self.__maxUpDownStatus == -1: # 存在跌停的情况
                if self.getClose() > self.getLow():
                    return 0
                else:
                    return -1
            else:
                raise Exception(f"Invalid max-up-down status `{self.__maxUpDownStatus}`")
        elif max_up_down_type == max_up_down_type.FLEXIBLE:
            if self.__maxUpDownStatus == 1:     # 存在涨停的情况
                if self.getLow() < self.getHigh():
                    return 0
                else:
                    return 1
            elif self.__maxUpDownStatus == -1:  # 存在跌停的情况
                if self.getHigh() > self.getLow():
                    return 0
                else:
                    return -1
            else:
                raise Exception(f"Invalid max-up-down status `{self.__maxUpDownStatus}`")
        else:
            raise Exception(f"Invalid max-up-down type `{max_up_down_type}`")

    
    def getSecName(self):
        """返回股票名称"""
        return self.__sec_name
    

    def getFrequency(self):
        return self.__frequency

    def getPrice(self):
        if self.__useAdjustedValue:
            return self.__adj_factor * self.__close
        else:
            return self.__close

    def get_price(self, price_type=PriceType.CLOSE, adjusted=True):
        """
        根据配置信息返回需要的价格类型
        """
        if price_type == PriceType.OPEN:
            return self.getOpen(adjusted)
        elif price_type == PriceType.CLOSE:
            return self.getClose(adjusted)
        elif price_type == PriceType.LOW:
            return self.getLow(adjusted)
        elif price_type == PriceType.HIGH:
            return self.getHigh(adjusted)
        elif price_type == PriceType.MODE1:
            return self.getTypicalPrice()
        elif price_type == PriceType.PRE_CLOSE:
            return self.getPreClose(adjusted)
        else:
            raise Exception("Unsupported price type")


    def getExtraColumns(self):
        return self.__extra


class DummyBarBase(Bar):
    """
    实现原始Bar抽象方法的占位类
    """

    def getDateTime(self):
        pass

    def getUseAdjValue(self):
        pass

    def getOpen(self, adjusted=False):
        pass

    def getHigh(self, adjusted=False):
        pass

    def getLow(self, adjusted=False):
        pass

    def getClose(self, adjusted=False):
        pass

    def getVolume(self):
        pass

    def getAdjClose(self):
        pass

    def getFrequency(self):
        pass

    def getPrice(self):
        pass

    def setUseAdjustedValue(self, useAdjusted):
        pass


class StockIndexFutureBar(DummyBarBase):
    """
    """
    def __init__(self, date_time, windcode, open_, high, low, close, settle, volume, amount,
                 start_date, end_date, oi, frequency):

        self.__date_time = date_time
        self.__windcode = windcode
        self.__open = open_
        self.__close = close
        self.__high = high
        self.__low = low
        self.__settle = settle
        self.__volume = volume
        self.__amount = amount
        self.__start_date = start_date
        self.__end_date = end_date
        self.__oi = oi
        self.__frequency = frequency

    def getDateTime(self):
        return self.__date_time

    def getSecName(self):
        return self.__windcode

    @property
    def windcode(self):
        return self.__windcode

    @property
    def open(self):
        return self.__open

    @property
    def close(self):
        return self.__close

    @property
    def high(self):
        return self.__high

    @property
    def low(self):
        return self.__low

    @property
    def settle(self):
        return self.__settle

    @property
    def volume(self):
        return self.__volume

    @property
    def amount(self):
        return self.__amount

    @property
    def oi(self):
        return self.__oi

    @property
    def start_date(self):
        return self.__start_date

    @property
    def end_date(self):
        return self.__end_date

    def getUpDownStatus(self, *args, **kwargs):
        return 0

    def getTradeStatus(self):
        return SuspensionType.NORMAL

    def getOpen(self, adjusted=False):
        return self.open

    def getHigh(self, adjusted=False):
        return self.high

    def getLow(self, adjusted=False):
        return self.low

    def getClose(self, adjusted=False):
        return self.close

    def get_price(self, price_type=PriceType.CLOSE, adjusted=True):
        """
        根据配置信息返回需要的价格类型
        """
        if price_type == PriceType.OPEN:
            return self.open
        elif price_type == PriceType.CLOSE:
            return self.close
        elif price_type == PriceType.LOW:
            return self.low
        elif price_type == PriceType.HIGH:
            return self.high
        elif price_type == PriceType.MODE1:
            return self.getTypicalPrice()
        else:
            raise Exception("Unsupported price type")

    @property
    def ext_status(self):
        return ExtStatus.NORMAL


class IndexBar(DummyBarBase):
    def __init__(self, date_time, windcode, open_, high, low, close, volume, amount, frequency,
                 ext_status=ExtStatus.UNTRADABLE):
        self.__date_time = date_time
        self.__windcode = windcode
        self.__open = open_
        self.__close = close
        self.__high = high
        self.__low = low
        self.__volume = volume
        self.__amount = amount
        self.__frequency = frequency
        self.__ext_status = ext_status

    def getDateTime(self):
        return self.__date_time

    @property
    def windcode(self):
        return self.__windcode

    @property
    def open(self):
        return self.__open

    @property
    def close(self):
        return self.__close

    @property
    def high(self):
        return self.__high

    @property
    def low(self):
        return self.__low

    @property
    def settle(self):
        return self.__settle

    @property
    def volume(self):
        return self.__volume

    @property
    def amount(self):
        return self.__amount

    @property
    def oi(self):
        return self.__oi

    @property
    def start_date(self):
        return self.__start_date

    @property
    def end_date(self):
        return self.__end_date

    @property
    def ext_status(self):
        return self.__ext_status

    def getUpDownStatus(self, *args, **kwargs):
        return 0

    def getTradeStatus(self):
        return SuspensionType.NORMAL

    def getOpen(self, adjusted=False):
        return self.open

    def getHigh(self, adjusted=False):
        return self.high

    def getLow(self, adjusted=False):
        return self.low

    def getClose(self, adjusted=False):
        return self.close

    def get_price(self, price_type=PriceType.CLOSE, adjusted=True):
        """
        根据配置信息返回需要的价格类型
        """
        if price_type == PriceType.OPEN:
            return self.open
        elif price_type == PriceType.CLOSE:
            return self.close
        elif price_type == PriceType.LOW:
            return self.low
        elif price_type == PriceType.HIGH:
            return self.high
        elif price_type == PriceType.MODE1:
            return self.getTypicalPrice()
        else:
            raise Exception("Unsupported price type")


class SynthIndexETFBar(IndexBar):
    def __init__(self, date_time, windcode, open_, high, low, close, volume, amount, frequency, ext_status=ExtStatus.NORMAL):
        super().__init__(date_time, windcode, open_, high, low, close, volume, amount, frequency,
                         ext_status=ext_status)

    @classmethod
    def synth_name(cls, code):
        return f"SETF.{code}"

    def getSecName(self):
        return self.synth_name(self.windcode)


class IndexETFBar(IndexBar):
    def __init__(self, date_time, windcode, open_, high, low, close, volume, amount, frequency, ext_status=ExtStatus.NORMAL):
        super().__init__(date_time, windcode, open_, high, low, close, volume, amount, frequency,
                         ext_status=ext_status)

    def getSecName(self):
        return self.windcode

class PositionDummyBar(IndexBar):
    def __init__(self, date_time, windcode, open_, high, low, close, volume, amount, name, frequency, ext_status=ExtStatus.NORMAL):
        super().__init__(date_time, windcode, open_, high, low, close, volume, amount, frequency,
                         ext_status=ext_status)

        self.name = name

    def getSecName(self):
        return self.name


class FundNavBar(IndexBar):
    def __init__(self, date_time, windcode, close, frequency, ext_status):
        super().__init__(date_time, windcode, close, close, close, close, np.inf, np.inf, frequency,
                         ext_status=ext_status)

    @classmethod
    def fund_name(cls, code):
        return f"ETF.{code}"

    def getSecName(self):
        return self.fund_name(self.windcode)


class FundBar(IndexBar):
    def __init__(self, date_time, windcode, open_, high, low, close, volume, amount, adj_factor, sec_name, frequency, ext_status):
        super().__init__(date_time, windcode, open_, high, low, close, volume, amount, frequency, ext_status=ext_status)
        self.__sec_name = sec_name
        self.__adj_factor = adj_factor

    @classmethod
    def fund_name(cls, code):
        return f"ETF.{code}"

    def getSecName(self):
        return self.__sec_name

    def get_price(self, price_type=PriceType.CLOSE, adjusted=True):
        """
        根据配置信息返回需要的价格类型
        """
        if price_type == PriceType.OPEN:
            price = self.open
        elif price_type == PriceType.CLOSE:
            price = self.close
        elif price_type == PriceType.LOW:
            price = self.low
        elif price_type == PriceType.HIGH:
            price = self.high
        elif price_type == PriceType.MODE1:
            price = self.getTypicalPrice()
        else:
            raise Exception("Unsupported price type")
        if adjusted:
            return price * self.__adj_factor
        else:
            return price



class Bars(object):

    """A group of :class:`Bar` objects.

    :param barDict: A map of instrument to :class:`Bar` objects.
    :type barDict: map.

    .. note::
        Bars为一组bar的集合
        All bars must have the same datetime.
    """

    def __init__(self, barDict):
        if len(barDict) == 0:
            raise Exception("No bars supplied")

        """
        一天一组bar的时间应该一致
        """
        # Check that bar datetimes are in sync
        firstDateTime = None
        firstInstrument = None
        for instrument, currentBar in barDict.items():
            if firstDateTime is None:
                firstDateTime = currentBar.getDateTime()
                firstInstrument = instrument
            elif currentBar.getDateTime() != firstDateTime:
                raise Exception("Bar data times are not in sync. %s %s != %s %s" % (
                    instrument,
                    currentBar.getDateTime(),
                    firstInstrument,
                    firstDateTime
                ))

        self.__barDict = barDict
        self.__dateTime = firstDateTime

    def __getitem__(self, instrument):
        """Returns the :class:`wk_pyalgotrade.bar.Bar` for the given instrument.
        If the instrument is not found an exception is raised."""
        return self.__barDict[instrument]

    def __contains__(self, instrument):
        """Returns True if a :class:`wk_pyalgotrade.bar.Bar` for the given instrument is available."""
        return instrument in self.__barDict

    def items(self):
        return list(self.__barDict.items())

    def keys(self):
        return list(self.__barDict.keys())

    def getInstruments(self):
        """Returns the instrument symbols."""
        # instruments = []
        # for bar in self.__barDict.items():
        #     if bar.ext_status == ExtStatus.DUMMY_BAR_FOR_DELISTING:
        #         continue
        #     instruments.append('')
        return list(self.__barDict.keys())

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` for this set of bars."""
        return self.__dateTime

    def getBar(self, instrument):
        """Returns the :class:`wk_pyalgotrade.bar.Bar` for the given instrument or None if the instrument is not found."""
        return self.__barDict.get(instrument, None)
