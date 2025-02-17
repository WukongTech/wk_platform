from enum import Enum, auto


class CodePattern(Enum):
    LOWER_PREFIX = auto()
    UPPER_PREFIX = auto()
    UPPER_SUFFIX = auto()
    JQ_PATTERN = auto()


def symbol_trans(symbol:str, to=CodePattern.UPPER_PREFIX):
    """
    统一的代码转换，输入可为多种常用代码格式，输出根据参数确定
    """
    tokens = symbol.split('.')
    if len(tokens[0]) == 6:
        code = tokens[0]
        exchange = tokens[1]
    else:
        code = tokens[1]
        exchange = tokens[0]
    if len(exchange) == 4:
        if exchange == 'XSHG':
            exchange = 'SH'
        else:
            exchange = 'SZ'
    if len(exchange) == 2:
        exchange = exchange.upper()

    if to == CodePattern.LOWER_PREFIX:
        exchange = exchange.lower()
        return exchange + '.' + code
    elif to == CodePattern.UPPER_PREFIX:
        return exchange + '.' + code
    elif to == CodePattern.UPPER_SUFFIX:
        return code + '.' + exchange
    else:
        if exchange == 'SH':
            exchange = 'XSHG'
        else:
            exchange = 'XSHE'
        return code + '.' + exchange


def filter_data(data, begin_date, end_date, *, date_field_tag='trade_dt', close_right=True):
    if close_right:
        return data[(data[date_field_tag] >= begin_date) & (data[date_field_tag] <= end_date)]
    else:
        return data[(data[date_field_tag] >= begin_date) & (data[date_field_tag] < end_date)]

