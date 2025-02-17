"""
混合数据 Feed
"""
import datetime
import typing

from pyalgotrade import dataseries
from pyalgotrade.bar import Frequency
from wk_platform.feed.bar import SynthIndexETFBar
from wk_platform.barfeed.membf import FastBarFeed
from wk_platform.feed.parser import StockDataRowParser
from wk_platform.feed.parser import FutureDataRowParser
from wk_platform.feed.parser import IndexDataRowParser
from wk_platform.feed.parser import SyntheticIndexETFRowParser
from wk_util.tqdm import tqdm



class StockFeed(FastBarFeed):
    """
    个股与股指期货的复合Feed
    """
    def __init__(self, frequency=Frequency.DAY, timezone=None):
        super().__init__(frequency)
        self.__barFilter = None
        self.__dailyTime = datetime.time(0, 0, 0) if frequency == Frequency.DAY else None
        self.__timezone = timezone

    def barsHaveAdjClose(self):
        pass

    def getDailyBarTime(self):
        return self.__dailyTime

    def setDailyBarTime(self, time):
        self.__dailyTime = time

    def getBarFilter(self):
        return self.__barFilter

    def setBarFilter(self, barFilter):
        self.__barFilter = barFilter

    def add_stock_bars(self, data, progress_bar=False):
        """
        添加个股数据Bar
        """
        row_parser = StockDataRowParser(self.getDailyBarTime(), self.getFrequency(), self.__timezone)
        self.add_data_from_dataframe('stock', 'trade_dt', data, row_parser, progress_bar=progress_bar)

    def add_index_bars(self, data, progress_bar=False):
        """
        添加指数数据Bar
        """

        row_parser = IndexDataRowParser(self.getDailyBarTime(), self.getFrequency(), self.__timezone)
        self.add_data_from_dataframe('index', 'trade_dt', data, row_parser, progress_bar=progress_bar)


class StockFutureFeed(FastBarFeed):
    """
    个股与股指期货的复合Feed
    """

    def __init__(self, frequency=Frequency.DAY, timezone=None):
        super().__init__(frequency)
        self.__barFilter = None
        self.__dailyTime = datetime.time(0, 0, 0)
        self.__timezone = timezone

    def barsHaveAdjClose(self):
        pass

    def getDailyBarTime(self):
        return self.__dailyTime

    def setDailyBarTime(self, time):
        self.__dailyTime = time

    def getBarFilter(self):
        return self.__barFilter

    def setBarFilter(self, barFilter):
        self.__barFilter = barFilter

    def add_stock_bars(self, data, progress_bar=False):
        """
        添加个股数据Bar
        """

        row_parser = StockDataRowParser(self.getDailyBarTime(), self.getFrequency(), self.__timezone)
        self.add_data_from_dataframe('stock', 'trade_dt', data, row_parser, progress_bar=progress_bar)

    def add_future_bars(self, data, progress_bar=False):
        """
        添加期货数据Bar
        """
        row_parser = FutureDataRowParser(self.getDailyBarTime(), self.getFrequency(), self.__timezone)
        self.add_data_from_dataframe('future', 'trade_dt', data, row_parser, progress_bar=progress_bar)


class StockIndexSynthETFFeed(FastBarFeed):
    """
    个股与模拟指数ETF的行情
    """

    def __init__(self, frequency=Frequency.DAY, timezone=None):
        super().__init__(frequency)
        self.__barFilter = None
        self.__dailyTime = datetime.time(0, 0, 0) if frequency == Frequency.DAY else None
        self.__timezone = timezone

    def barsHaveAdjClose(self):
        pass

    def getDailyBarTime(self):
        return self.__dailyTime

    def setDailyBarTime(self, time):
        self.__dailyTime = time

    def getBarFilter(self):
        return self.__barFilter

    def setBarFilter(self, barFilter):
        self.__barFilter = barFilter

    def add_stock_bars(self, data, progress_bar=False):
        """
        添加个股数据Bar
        """
        row_parser = StockDataRowParser(self.getDailyBarTime(), self.getFrequency(), self.__timezone)
        self.add_data_from_dataframe('stock', 'trade_dt', data, row_parser, progress_bar=progress_bar)

    def add_index_bars(self, data, progress_bar=False):
        """
        添加指数Bar
        """

        row_parser = IndexDataRowParser(self.getDailyBarTime(), self.getFrequency(), self.__timezone)
        self.add_data_from_dataframe('index', 'trade_dt', data, row_parser, progress_bar=progress_bar)

    def add_synth_index_etf_bars(self, data, progress_bar=False):
        """
        添加模拟指数ETF Bar
        """

        row_parser = SyntheticIndexETFRowParser(self.getDailyBarTime(), self.getFrequency(), self.__timezone)
        self.add_data_from_dataframe('synth_etf', 'trade_dt', data, row_parser, progress_bar=progress_bar)




