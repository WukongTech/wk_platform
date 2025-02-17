from enum import Enum

# from wk_util.mixins import EnumReflectMixin


class PriceType(Enum):
    OPEN = 0
    CLOSE = 1
    LOW = 2
    HIGH = 3
    MODE1 = 11  # (high + low + close) / 3
    PRE_CLOSE = -1


class MaxUpDownType(Enum):
    STRICT = 1          # 当天存在涨跌停则不允许交易
    FLEXIBLE = 2        # 当天有非涨跌停价格即允许交易
    NONE = 0            # 不限制涨跌停情况下的交易
    RELAX_OPEN = 11     # 盘中涨跌停但开盘价没有涨跌停则允许交易
    RELAX_CLOSE = 12    # 盘中涨跌停但收盘价没有涨跌停则允许交易


class TrackLevel(Enum):
    OFF = 0
    TRADE_DAY = 1
    EVERYDAY = 2
    LAST_TRADE_DAY = 3


class PositionCostType(Enum):
    """
    持仓成本算法
    """
    ACCUMULATION = 0    # 累计持仓成本，有可能出现盈利卖出后导致历史成本为负数的情况
    TRADE_DATE = 1      # 在换仓日按当日价格更新所有持仓的成本


class TradeRule(Enum):
    T0 = 0  # t+0
    T1 = 1  # t+1
    NEVER = 3  # 策略执行期间买入的均不允许卖出


class DatasetType(Enum):
    A_SHARE_MARKET = 0
    SYNTH_INDEX_ETF = 1
    ETF_FUND_SUBSET_1 = 2
    AME_SYNTH_INDEX_ETF = 3
    SPECIAL_INDEX = 4
    LOCAL_INDEX_ETF = 5
    A_SHARE_ETF = 6
    INDEX_FUTURE = 7




class CalendarType(Enum):
    A_SHARE_MARKET = 0  # 使用A股交易日历
    A_AME = 1           # 使用A股美股交易日的并集
