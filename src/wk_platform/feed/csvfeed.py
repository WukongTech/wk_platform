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
csv文件源的解析
chenxiangdong 20170719
"""
import datetime
import pytz
from pyalgotrade.utils import dt
from pyalgotrade.utils import csvutils
from pyalgotrade.barfeed import membf



# Interface for csv row parsers.
class RowParser(object):
    def parseBar(self, csvRowDict):
        raise NotImplementedError()

    def getFieldNames(self):
        raise NotImplementedError()

    def getDelimiter(self):
        raise NotImplementedError()


# Interface for bar filters.
class BarFilter(object):
    def includeBar(self, bar_):
        raise NotImplementedError()


class DateRangeFilter(BarFilter):
    def __init__(self, fromDate=None, toDate=None):
        self.__fromDate = fromDate
        self.__toDate = toDate

    def includeBar(self, bar_):
        if self.__toDate and bar_.getDateTime() > self.__toDate:
            return False
        if self.__fromDate and bar_.getDateTime() < self.__fromDate:
            return False
        return True


# US Equities Regular Trading Hours filter
# Monday ~ Friday
# 9:30 ~ 16 (GMT-5)
class USEquitiesRTH(DateRangeFilter):
    timezone = pytz.timezone("US/Eastern")

    def __init__(self, fromDate=None, toDate=None):
        super(USEquitiesRTH, self).__init__(fromDate, toDate)

        self.__fromTime = datetime.time(9, 30, 0)
        self.__toTime = datetime.time(16, 0, 0)

    def includeBar(self, bar_):
        ret = super(USEquitiesRTH, self).includeBar(bar_)
        if ret:
            # Check day of week
            barDay = bar_.getDateTime().weekday()
            if barDay > 4:
                return False

            # Check time
            barTime = dt.localize(bar_.getDateTime(), USEquitiesRTH.timezone).time()
            if barTime < self.__fromTime:
                return False
            if barTime > self.__toTime:
                return False
        return ret


class BarFeed(membf.BarFeed):
    """Base class for CSV file based :class:`wk_pyalgotrade.barfeed.BarFeed`.

    .. note::
        This is a base class and should not be used directly.
    """

    def __init__(self, frequency, maxLen=None):
        

        super(BarFeed, self).__init__(frequency, maxLen)

        self.__barFilter = None
        self.__dailyTime = datetime.time(0, 0, 0)

    def getDailyBarTime(self):
        return self.__dailyTime

    def setDailyBarTime(self, time):
        self.__dailyTime = time

    def getBarFilter(self):
        return self.__barFilter

    def setBarFilter(self, barFilter):
        self.__barFilter = barFilter

    def addBarsFromCSV(self, instrument, path, rowParser):
        # Load the csv file
        loadedBars = []
        reader = csvutils.FastDictReader(open(path, "r"), fieldnames=rowParser.getFieldNames(), delimiter=rowParser.getDelimiter())
        for row in reader:
            
            bar_ = rowParser.parseBar(row)
            if bar_ is not None and (self.__barFilter is None or self.__barFilter.includeBar(bar_)):
                loadedBars.append(bar_)
              
        self.addBarsFromSequence(instrument, loadedBars)





"""
class GenericRowParser(RowParser):
    def __init__(self, columnNames, dateTimeFormat, dailyBarTime, frequency, timezone, barClass=bar.BasicBar):
        self.__dateTimeFormat = dateTimeFormat
        self.__dailyBarTime = dailyBarTime
        self.__frequency = frequency
        self.__timezone = timezone
        self.__haveAdjClose = False
        self.__barClass = barClass
        # Column names.
        self.__dateTimeColName = columnNames["datetime"]
        self.__openColName = columnNames["open"]
        self.__highColName = columnNames["high"]
        self.__lowColName = columnNames["low"]
        self.__closeColName = columnNames["close"]
        self.__volumeColName = columnNames["volume"]
        self.__adjCloseColName = columnNames["adj_close"]
        
        self.__new_columnColName = columnNames["new_column"]
        self.__trade_statusColName = columnNames["trade_status"]
        self.__maxupordownColName = columnNames["maxupordown"]
        self.__sec_name = columnNames["sec_name"]
        
        self.__columnNames = columnNames

    def _parseDate(self, dateString):
        ret = datetime.datetime.strptime(dateString, self.__dateTimeFormat)

        if self.__dailyBarTime is not None:
            ret = datetime.datetime.combine(ret, self.__dailyBarTime)
        # Localize the datetime if a timezone was given.
        if self.__timezone:
            ret = dt.localize(ret, self.__timezone)
        return ret

    def barsHaveAdjClose(self):
        return self.__haveAdjClose

    def getFieldNames(self):
        # It is expected for the first row to have the field names.
        return None

    def getDelimiter(self):
        return ","

    def parseBar(self, csvRowDict):
        dateTime = self._parseDate(csvRowDict[self.__dateTimeColName])
        open_ = float(csvRowDict[self.__openColName])
        high = float(csvRowDict[self.__highColName])
        low = float(csvRowDict[self.__lowColName])
        close = float(csvRowDict[self.__closeColName])
        volume = float(csvRowDict[self.__volumeColName])
        
        #新增
        new_column =  float(csvRowDict[self.__new_columnColName])
        trade_status = float(csvRowDict[self.__trade_statusColName])
        maxupordown = float(csvRowDict[self.__maxupordownColName])
        sec_name = str(csvRowDict[self.__sec_name])
        
 
    
        adjClose = None
        if self.__adjCloseColName is not None:
            adjCloseValue = csvRowDict.get(self.__adjCloseColName, "")
            if len(adjCloseValue) > 0:
                adjClose = float(adjCloseValue)
                self.__haveAdjClose = True

        # Process extra columns.
        extra = {}
        for k, v in csvRowDict.iteritems():
            if k not in self.__columnNames:
                extra[k] = csvutils.float_or_string(v)

        #新增列
        return self.__barClass(
            dateTime, open_, high, low, close, volume, adjClose,new_column,trade_status,maxupordown, sec_name,self.__frequency, extra=extra
        )

"""

"""
class GenericBarFeed(BarFeed):
    
    def __init__(self, frequency, timezone=None, maxLen=None):
        super(GenericBarFeed, self).__init__(frequency, maxLen)

        self.__timezone = timezone
        # Assume bars don't have adjusted close. This will be set to True after
        # loading the first file if the adj_close column is there.
        self.__haveAdjClose = False

        self.__barClass = bar.BasicBar

        self.__dateTimeFormat = "%Y-%m-%d %H:%M:%S"
        self.__columnNames = {
            "datetime": "Date Time",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
            "adj_close": "Adj Close",
            
            #新增列
            "new_column": "new_column",
            "trade_status":"trade_status",
            "maxupordown":"maxupordown",
            "sec_name":"sec_name",
            
            
        }
        # self.__dateTimeFormat expects time to be set so there is no need to
        # fix time.
        self.setDailyBarTime(None)

    def barsHaveAdjClose(self):
        return self.__haveAdjClose

    def setNoAdjClose(self):
        self.__columnNames["adj_close"] = None
        self.__haveAdjClose = False

    def setColumnName(self, col, name):
        self.__columnNames[col] = name

    def setDateTimeFormat(self, dateTimeFormat):
        self.__dateTimeFormat = dateTimeFormat

    def setBarClass(self, barClass):
        self.__barClass = barClass

    def addBarsFromCSV(self, instrument, path, timezone=None):

        if timezone is None:
            timezone = self.__timezone

        rowParser = GenericRowParser(
            self.__columnNames, self.__dateTimeFormat, self.getDailyBarTime(), self.getFrequency(),
            timezone, self.__barClass
        )

        super(GenericBarFeed, self).addBarsFromCSV(instrument, path, rowParser)

        if rowParser.barsHaveAdjClose():
            self.__haveAdjClose = True
        elif self.__haveAdjClose:
            raise Exception("Previous bars had adjusted close and these ones don't have.")
"""
    







