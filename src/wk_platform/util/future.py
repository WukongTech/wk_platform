import datetime


class FutureUtil:

    @classmethod
    @property
    def placeholder(cls):
        return ["IF0000.CFE", "IC0000.CFE", "IH0000.CFE"]
    @classmethod
    def is_index_future(cls, windcode: str):
        if windcode.startswith('IC') or windcode.startswith('IF'):
            return True
        else:
            return False

    @classmethod
    def auto_contract(cls, windcode, trade_dt, bars):
        code = windcode[:2]
        ym = windcode[2:6]
        if ym == '0000':
            return cls.main_contract(code, trade_dt, bars)
        else:
            return windcode

    @classmethod
    def current_contract(cls, code, date_time):
        date_str = date_time.strftime("%Y%m")[2:]
        return f"{code}{date_str}.CFE"

    @classmethod
    def main_contract(cls, code, trade_dt, bars):
        current_contract = cls.current_contract(code, datetime.datetime.strptime(trade_dt, '%Y%m%d'))
        try:
            current_bar = bars[current_contract]
        except KeyError:
            current_contract = cls.next_contract(current_contract)
        return current_contract


    @classmethod
    def next_contract(cls, windcode):
        """
        获取下一个合约代码
        """
        date_part = windcode[2:6]
        year = int(date_part[:2])
        month = int(date_part[2:]) + 1
        if month > 12:
            year += 1
            month = 1
        date_part = f"{year}{month:02}"
        return f"{windcode[:2]}{date_part}{windcode[-4:]}"

    @classmethod
    def prev_contract(cls, windcode):
        """
        获取前一个合约代码
        """
        date_part = windcode[2:6]
        year = int(date_part[:2])
        month = int(date_part[2:]) - 1
        if month == 0:
            year -= 1
            month = 12
        date_part = f"{year}{month:02}"
        return f"{windcode[:2]}{date_part}{windcode[-4:]}"

    @classmethod
    def calc_future_quantity(cls, cash_amount, price, whole_batch=False):
        """
        计算输入金额可以买入的股票数目
        """
        is_positive = cash_amount > 0
        if whole_batch:
            quantity = abs(cash_amount) // (price * 300) * 300
        else:
            quantity = int(abs(cash_amount) /price)
        if not is_positive:
            quantity = -1 * quantity

        return quantity