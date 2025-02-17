# -*- coding: utf-8 -*-
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
Author: chenxiangdong
content: 用以转换dataFrame到feed，相当于以pandas dataframe 为桥，不再以csv为桥。
"""

import datetime
from . import dataFrameBarfeed
from pyalgotrade.barfeed import common
from pyalgotrade.utils import dt
from pyalgotrade import dataseries
from wk_platform.feed import bar
from pyalgotrade.bar import Frequency
from wk_data.constants import SuspensionType, STType, ExtStatus

"""
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
"""


######################################################################
## Yahoo Finance CSV parser
# Each bar must be on its own line and fields must be separated by comma (,).
#
# Bars Format:
# Date,Open,High,Low,Close,Volume,Adj Close
#
# The csv Date column must have the following format: YYYY-MM-DD

"""
日线日期格式的解析，输入格式需要为YYYYMMDD
"""
def parse_date(date):
    # Sample: 2005-12-30
    # This custom parsing works faster than:
    # datetime.datetime.strptime(date, "%Y-%m-%d")
    
    date= str(date)
    
    """
    year = int(date[0:4])
    month = int(date[5:7])
    day = int(date[8:10])
    """
    #sql数据库的日期格式为yyyymmdd，因此改变解析方式
    year = int(date[0:4])
    month = int(date[4:6])
    day = int(date[6:8])
    
    ret = datetime.datetime(year, month, day)
    return ret


class RowParser(dataFrameBarfeed.RowParser):
    def __init__(self, dailyBarTime, frequency, timezone=None, sanitize=False):
        self.__dailyBarTime = dailyBarTime
        self.__frequency = frequency
        self.__timezone = timezone
        self.__sanitize = sanitize

    def __parseDate(self, dateString):
        
        
        ret = parse_date(dateString)
        # Time on Yahoo! Finance CSV files is empty. If told to set one, do it.
        if self.__dailyBarTime is not None:
            ret = datetime.datetime.combine(ret, self.__dailyBarTime)
        # Localize the datetime if a timezone was given.
        if self.__timezone:
            ret = dt.localize(ret, self.__timezone)
        return ret

    def getFieldNames(self):
        # It is expected for the first row to have the field names.
        return None

    def getDelimiter(self):
        return ","
    
    #对dataFrame的每行进行操作
    def handler(x):
        pass
    #row的结构 row[0]为时间，string类型。row[1]为Series类型:'open'\high\close\low\volume\amoun或price——change等，前面6项和tushare 对应  
    def parseBar(self, row):
        
        
        #原版使用iterrow进行遍历，而后进行dataframe一行的索引获取，十分耗时，固放弃该方法
        """
        dateTime = self.__parseDate(row[0]) #date                
        close = float(row[1]['Close'])
        open_ = float(row[1]['Open'])
        high = float(row[1]['High'])
        low = float(row[1]['Low'])
        volume = float(row[1]['Volume'])
        adjClose = float(row[1]['Close'])
        #newly add
        new_column = float(row[1]["new_column"])
        trade_status = float(row[1]["trade_status"])
        maxupordown = float(row[1]["maxupordown"])
        sec_name = str(row[1]["sec_name"])
        """
       
        #此处需要认真检查，若输入列顺序改变，则下标也随之改变
        #使用itertuple进行遍历，返回tuple类型，按照下表进行索引，加快处理速度
        
        """
        Index([u'Date', u'windcode', u'Open', u'High', u'Low', u'Close', u'pct_change',
        u'Volume', u'Amount', u'trade_status', u'sec_name', u'maxupordown',
        u'Adj Close', u'new_column'],
    
        
        默认HDF5输入数据格式
        [['Date','windcode','Open','High','Low','Close','industry_name',
        'Volume','st','trade_status','sec_name','maxupordown','listdate']]
        """
        
        """
        需要注意此处的解析顺序
        """
        dateTime = self.__parseDate(row.trade_dt) #date
        open_ = row.open
        high = row.high
        low = row.low
        close = row.close
        volume = row.volume
        st = STType.NORMAL if row.st == '' else STType.ST
        trade_status = SuspensionType.NORMAL if row.suspension == 0 else SuspensionType.SUSPENDED
        sec_name= row.sec_name
        maxupordown = row.max_up_down

        if row.ext_status == ExtStatus.NORMAL.value:
            ext_status = ExtStatus.NORMAL
        elif row.ext_status == ExtStatus.DUMMY_BAR_FOR_DELISTING.value:
            ext_status = ExtStatus.DUMMY_BAR_FOR_DELISTING
        elif row.ext_status == ExtStatus.DUMMY_BAR_FOR_CHANGING_WINDCODE.value:
            ext_status = ExtStatus.DUMMY_BAR_FOR_CHANGING_WINDCODE
        else:
            raise ValueError(f"Unsupported ext_status `{row.ext_status}`")

        adj_factor = row.adj_factor
        new_column = 1


        extra = {
            'amount_ma': row.amount_ma
        }
        
        
        """
        adjClose = float(row[12])
        new_column = float(row[13])
        """

        if self.__sanitize:
            open_, high, low, close = common.sanitize_ohlc(open_, high, low, close)
        
        
        #newly add
        # def __init__(self, date_time, open_, high, low, close, volume, adj_factor,
        #                  new_column, st, suspension, max_up_down, sec_name, frequency, ext_status, extra=None):
        return bar.StockBar(dateTime, open_, high, low, close, volume, adj_factor,
                            new_column, st, trade_status, maxupordown, sec_name, self.__frequency, ext_status, extra=extra)


class Feed(dataFrameBarfeed.BarFeed):
    """A :class:`wk_pyalgotrade.barfeed.csvfeed.BarFeed` that loads bars from CSV files downloaded from Yahoo! Finance.

    :param frequency: The frequency of the bars. Only **wk_pyalgotrade.bar.Frequency.DAY** or **wk_pyalgotrade.bar.Frequency.WEEK**
        are supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`wk_pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`wk_pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the opposite end.
    :type maxLen: int.

    .. note::
        Yahoo! Finance csv files lack timezone information.
        When working with multiple instruments:

            * If all the instruments loaded are in the same timezone, then the timezone parameter may not be specified.
            * If any of the instruments loaded are in different timezones, then the timezone parameter must be set.
    """

    def __init__(self, frequency=Frequency.DAY, timezone=None, maxLen=dataseries.DEFAULT_MAX_LEN):
        if isinstance(timezone, int):
            raise Exception("timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")

        if frequency not in [Frequency.DAY, Frequency.WEEK]:
            raise Exception("Invalid frequency.")

        super().__init__(frequency, maxLen)
        self.__timezone = timezone
        self.__sanitizeBars = False

    def sanitizeBars(self, sanitize):
        self.__sanitizeBars = sanitize

    def barsHaveAdjClose(self):
        return True

    def addBarsFromDataFrame(self, instrument, dataFrame, timezone=None):
        """Loads bars for a given instrument from a CSV formatted file.
        The instrument gets registered in the bar feed.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param path: The path to the CSV file.
        :type path: string.
        :param timezone: The timezone to use to localize bars. Check :mod:`wk_pyalgotrade.marketsession`.
        :type timezone: A pytz timezone.
        """

        if isinstance(timezone, int):
            raise Exception("timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")

        if timezone is None:
            timezone = self.__timezone

        rowParser = RowParser(self.getDailyBarTime(), self.getFrequency(), timezone, self.__sanitizeBars)
        super().addBarsFromDataFrame(instrument, rowParser, dataFrame)
        
        
        
        
