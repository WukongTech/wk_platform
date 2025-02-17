"""
混合数据 Feed
"""
import datetime

from pyalgotrade import dataseries
from pyalgotrade.bar import Frequency
from wk_platform.feed.bar import SynthIndexETFBar
from wk_platform.barfeed import membf
from wk_platform.feed.parser import StockDataRowParser
from wk_platform.feed.parser import FutureDataRowParser
from wk_platform.feed.parser import IndexDataRowParser
from wk_platform.feed.parser import SyntheticIndexETFRowParser
from wk_util.tqdm import tqdm
import wk_data
from wk_platform.barfeed.membf import FastBarFeed


class MixedFeed(FastBarFeed):
    """
    个股与模拟指数ETF的行情
    """

    def __init__(self, frequency=Frequency.DAY, timezone=None):
        super().__init__(frequency)
        self.__barFilter = None
        self.__dailyTime = datetime.time(0, 0, 0) if frequency == Frequency.DAY else None
        self.__timezone = timezone
        self.__data = {}
        self.__bar_types = []

    def barsHaveAdjClose(self):
        pass

    @property
    def bar_types(self):
        return self.__bar_types

    def getDailyBarTime(self):
        return self.__dailyTime

    def setDailyBarTime(self, time):
        self.__dailyTime = time

    def add_bars(self, dataset, parser_cls, begin_date, end_date=None, bars_name=None,
                 preprocessor=lambda x: x, progress_bar=False):
        if bars_name is None:
            bars_name = dataset
        data = wk_data.get(dataset, begin_date=begin_date, end_date=end_date)
        data = preprocessor(data)
        self.__data[bars_name] = data
        row_parser = parser_cls(self.getDailyBarTime(), self.getFrequency(), self.__timezone)
        self.__bar_types.append(row_parser.bar_type)
        return self.add_data_from_dataframe(bars_name, 'trade_dt', data, row_parser, progress_bar=progress_bar)

    def get_processed_data(self, dataset):
        return self.__data[dataset]
