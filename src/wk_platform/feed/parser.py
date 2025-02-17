import datetime


from pyalgotrade.utils import dt
from pyalgotrade.bar import Frequency
from wk_data.constants import SuspensionType, STType, ExtStatus
from wk_platform.feed.bar import StockBar, IndexBar, StockIndexFutureBar, SynthIndexETFBar, FundNavBar, FundBar, \
    PositionDummyBar, IndexETFBar


def parse_date(date):
    # Sample: 2005-12-30
    # This custom parsing works faster than:
    # datetime.datetime.strptime(date, "%Y-%m-%d")

    date = str(date)

    """
    year = int(date[0:4])
    month = int(date[5:7])
    day = int(date[8:10])
    """
    # sql数据库的日期格式为yyyymmdd，因此改变解析方式
    year = int(date[0:4])
    month = int(date[4:6])
    day = int(date[6:8])

    ret = datetime.datetime(year, month, day)
    return ret


class BaseRowParser(object):
    def parse_row(self, row_data):
        raise NotImplementedError()

    # def getFieldNames(self):
    #     raise NotImplementedError()
    #
    # def getDelimiter(self):
    #     raise NotImplementedError()


class RowParser(BaseRowParser):
    def __init__(self, bar_cls, dailyBarTime, frequency, timezone=None, sanitize=False):
        self.__dailyBarTime = dailyBarTime
        self.__frequency = frequency
        self.__timezone = timezone
        self.__sanitize = sanitize
        self.__bar_cls = bar_cls

    @property
    def bar_type(self):
        return self.__bar_cls

    @property
    def frequency(self):
        return self.__frequency

    def parse_date(self, dateString):
        if self.__frequency == Frequency.DAY:
            ret = parse_date(dateString)
        else:
            ret = datetime.datetime.strptime(dateString, "%Y%m%d%H%M")
        # Time on Yahoo! Finance CSV files is empty. If told to set one, do it.
        if self.__dailyBarTime is not None:
            ret = datetime.datetime.combine(ret, self.__dailyBarTime)
        # Localize the datetime if a timezone was given.
        if self.__timezone:
            ret = dt.localize(ret, self.__timezone)
        return ret

    def parse_row(self, row_data):
        raise NotImplementedError()

    def lazy_parser(self, row):
        def parse():
            return self.parse_row(row)
        return row.windcode, parse



class StockDataRowParser(RowParser):
    def __init__(self, daily_bar_time, frequency, timezone=None, sanitize=False):
        super().__init__(StockBar, daily_bar_time, frequency, timezone, sanitize)



    def getFieldNames(self):
        # It is expected for the first row to have the field names.
        return None

    def getDelimiter(self):
        return ","

    # 对dataFrame的每行进行操作
    def handler(x):
        pass

    def parse_row(self, row):
        date_time = self.parse_date(row.trade_dt)  # date
        pre_close = row.pre_close
        open_ = row.open
        high = row.high
        low = row.low
        close = row.close
        volume = row.volume
        st = STType.NORMAL if row.st == '' else STType.ST
        trade_status = SuspensionType.NORMAL if row.suspension == 0 else SuspensionType.SUSPENDED
        sec_name = row.sec_name
        maxupordown = row.max_up_down

        if row.ext_status == ExtStatus.NORMAL.value:
            ext_status = ExtStatus.NORMAL
        elif row.ext_status == ExtStatus.DUMMY_BAR_FOR_DELISTING.value:
            ext_status = ExtStatus.DUMMY_BAR_FOR_DELISTING
        elif row.ext_status == ExtStatus.DUMMY_BAR_FOR_CHANGING_WINDCODE.value:
            ext_status = ExtStatus.DUMMY_BAR_FOR_CHANGING_WINDCODE
        else:
            raise ValueError(f"Unsupported ext_status `{row.ext_status}` of {row.windcode} on {row.trade_dt}")

        adj_factor = row.adj_factor
        new_column = 1

        extra = {'amount_ma': row.amount_ma}
        # if self.__sanitize:
        #     open_, high, low, close = common.sanitize_ohlc(open_, high, low, close)

        return (
            row.windcode,
            StockBar(date_time, pre_close, open_, high, low, close, volume, adj_factor,
                     new_column, st, trade_status, maxupordown, sec_name, self.frequency, ext_status, extra=extra)
        )


class SyntheticIndexETFRowParser(RowParser):
    """
    使用指数行情模拟的ETF标的，定价为指数点位/1000
    """

    def __init__(self, daily_bar_time, frequency, timezone=None, sanitize=False):
        super().__init__(SynthIndexETFBar, daily_bar_time, frequency, timezone, sanitize)

    def parse_row(self, row_data):

        return (
            SynthIndexETFBar.synth_name(row_data.windcode),
            SynthIndexETFBar(
                date_time=self.parse_date(row_data.trade_dt),
                windcode=row_data.windcode,
                open_=row_data.open / 1000,
                high=row_data.high / 1000,
                low=row_data.low / 1000,
                close=row_data.close / 1000,
                volume=row_data.volume * 1000,
                amount=row_data.amount,
                frequency=self.frequency,
                ext_status=ExtStatus(row_data.ext_status)
            )
        )


class IndexETFRowParser(RowParser):
    """
    使用指数行情模拟的ETF标的，定价为指数点位/1000
    """

    def __init__(self, daily_bar_time, frequency, timezone=None, sanitize=False):
        super().__init__(IndexETFBar, daily_bar_time, frequency, timezone, sanitize)

    def parse_row(self, row_data):

        return (
            row_data.windcode,
            IndexETFBar(
                date_time=self.parse_date(row_data.trade_dt),
                windcode=row_data.windcode,
                open_=row_data.open,
                high=row_data.high,
                low=row_data.low,
                close=row_data.close,
                volume=row_data.volume,
                amount=row_data.amount,
                frequency=self.frequency,
                ext_status=ExtStatus(row_data.ext_status)
            )
        )
class PositionDummyRowParser(RowParser):
    def __init__(self, daily_bar_time, frequency, timezone=None, sanitize=False):
        super().__init__(PositionDummyBar, daily_bar_time, frequency, timezone, sanitize)

    def parse_row(self, row_data):

        return (
            row_data.windcode,
            PositionDummyBar(
                date_time=self.parse_date(row_data.trade_dt),
                windcode=row_data.windcode,
                open_=row_data.open,
                high=row_data.high,
                low=row_data.low,
                close=row_data.close,
                volume=row_data.volume,
                amount=row_data.amount,
                frequency=self.frequency,
                name=row_data.name
            )
        )

class IndexDataRowParser(RowParser):
    def __init__(self, daily_bar_time, frequency, timezone=None, sanitize=False):
        super().__init__(IndexBar, daily_bar_time, frequency, timezone, sanitize)

    def parse_row(self, row_data):
        return (
            row_data.windcode,
            IndexBar(
                date_time=self.parse_date(row_data.trade_dt),
                windcode=row_data.windcode,
                open_=row_data.open,
                high=row_data.high,
                low=row_data.low,
                close=row_data.close,
                volume=row_data.volume,
                amount=row_data.amount,
                frequency=self.frequency
            )
        )


class FundNavDataRowParser(RowParser):
    def __init__(self, daily_bar_time, frequency, timezone=None, sanitize=False):
        super().__init__(FundNavBar, daily_bar_time, frequency, timezone, sanitize)

    def parse_row(self, row_data):
        return (
            row_data.windcode,
            FundNavBar(
                date_time=self.parse_date(row_data.trade_dt),
                windcode=row_data.windcode,
                close=row_data.nav_adjusted,
                frequency=self.frequency,
                ext_status=ExtStatus(row_data.ext_status)
            )
        )


class FundDataRowParser(RowParser):
    def __init__(self, daily_bar_time, frequency, timezone=None, sanitize=False):
        super().__init__(FundBar, daily_bar_time, frequency, timezone, sanitize)

    def parse_row(self, row_data):
        return (
            row_data.windcode,
            FundBar(
                date_time=self.parse_date(row_data.trade_dt),
                windcode=row_data.windcode,
                open_=row_data.open,
                high=row_data.high,
                low=row_data.low,
                close=row_data.close,
                volume=row_data.volume,
                amount=row_data.amount,
                adj_factor=row_data.adj_factor,
                sec_name=row_data.sec_name,
                frequency=self.frequency,
                ext_status=ExtStatus(row_data.ext_status)
            )
        )


class FutureDataRowParser(RowParser):
    def __init__(self, daily_bar_time, frequency, timezone=None, sanitize=False):
        super().__init__(StockIndexFutureBar, daily_bar_time, frequency, timezone, sanitize)

    def parse_row(self, row_data):
        return (
            row_data.windcode,
            StockIndexFutureBar(
                date_time=self.parse_date(row_data.trade_dt),
                windcode=row_data.windcode,
                open_=row_data.open,
                high=row_data.high,
                low=row_data.low,
                close=row_data.close,
                settle=row_data.settle,
                volume=row_data.volume,
                amount=row_data.amount,
                start_date=row_data.start_date,
                end_date=row_data.end_date,
                oi=row_data.oi,
                frequency=self.frequency
            )
        )
