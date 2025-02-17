from enum import Enum


class STType(Enum):
    NORMAL = ''  # TODO: 1.查看直接使用裸值的情况，看是否需要调整；2.研究代码对此值的使用，探究换为纯数字是否可行
    ST = 1


class SuspensionType(Enum):
    NORMAL = 0     # 正常交易
    SUSPENDED = 1  # 停牌，注意所有停牌情况均用此标签表示


class ExtStatus(Enum):
    """
    自定义的标签，标记该行数据的附加状态
    """
    NORMAL = 0                              # 正常状态
    DUMMY_BAR_FOR_DELISTING = -1            # 标记此数据是虚拟的最后一个可交易日，便于退市清仓处理
    DUMMY_BAR_FOR_CHANGING_WINDCODE = 1
    UNTRADABLE = 2                          # 用于标记不可交易的情况，仅用于barfeed中标记指数bar
    DUMMY_BAR_FOR_TRANSFORMING_TO_CASH = 3  # 标记此数据是虚拟的最后一个可交易日，将标的的全部持仓转换为现金，用于处理基金到期等情况
    UNTRADABLE_FILLED_BAR = 4               # 标记此数据是对齐日期时填充的数据，不可以交易


class MaxUpDownStatus(Enum):
    MAX_UP = 1
    NORMAL = 0
    MAX_DOWN = -1


BENCH_INDEX = ["000001.SH", "000016.SH", "000300.SH", "399905.SZ", "000852.SH", "399006.SZ"]
BENCH_MAP = {
    "000001.SH": "上证综指",
    "000016.SH": "上证50",
    "000300.SH": "沪深300",
    "399905.SZ": "中证500",
    "000852.SH": "中证1000",
    "399006.SZ": "创业板指"
}
