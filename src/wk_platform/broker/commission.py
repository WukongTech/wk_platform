"""
各类佣金、费率的定义
"""

import abc

import six

@six.add_metaclass(abc.ABCMeta)
class Commission(object):
    """Base class for implementing different commission schemes.

    .. note::
        This is a base class and should not be used directly.
    """

    @abc.abstractmethod
    def calculate(self, order, price, quantity):
        """Calculates the commission for an order execution.

        :param order: The order being executed.
        :type order: :class:`pyalgotrade.broker.Order`.
        :param price: The price for each share.
        :type price: float.
        :param quantity: The order size.
        :type quantity: float.
        :rtype: float.
        """
        raise NotImplementedError()

    def __call__(self, *args, **kwargs):
        return self.calculate(*args, **kwargs)


class NoCommission(Commission):
    """A :class:`Commission` class that always returns 0."""

    def calculate(self, order, price, quantity):
        return 0



"""
针对每笔交易收取固定的佣金
"""
class FixedPerTrade(Commission):
    """A :class:`Commission` class that charges a fixed amount for the whole trade.

    :param amount: The commission for an order.
    :type amount: float.
    """
    def __init__(self, amount):
        super(FixedPerTrade, self).__init__()
        self.__amount = amount

    def calculate(self, order, price, quantity):
        ret = 0
        # Only charge the first fill.
        if order.getExecutionInfo() is None:
            ret = self.__amount
        return ret

"""
针对交易额的比例收取佣金
"""
class TradePercentage(Commission):
    """A :class:`Commission` class that charges a percentage of the whole trade.

    :param percentage: The percentage to charge. 0.01 means 1%, and so on. It must be smaller than 1.
    :type percentage: float.
    """
    def __init__(self, percentage):
        super(TradePercentage, self).__init__()
        assert(percentage < 1)
        self.__percentage = percentage

    @property
    def percentage(self):
        return self.__percentage

    def calculate(self, order, price, quantity):
        return price * quantity * self.__percentage


######################################################################


"""
印花税，A股印花税为千一标准，卖出时收取
2017/10/27
"""
######################################################################
# TaxFee models
class CommissionTaxFee(Commission):
    """Base class for implementing different commission schemes.

    .. note::
        This is a base class and should not be used directly.
    """

    @abc.abstractmethod
    def calculate(self, order, price, quantity):
        """Calculates the commission for an order execution.

        :param order: The order being executed.
        :type order: :class:`wk_pyalgotrade.broker.Order`.
        :param price: The price for each share.
        :type price: float.
        :param quantity: The order size.
        :type quantity: float.
        :rtype: float.
        """
        raise NotImplementedError()




class NoCommissionTaxFee(CommissionTaxFee):
    """A :class:`Commission` class that always returns 0."""

    def calculate(self, order, price, quantity):
        return 0

"""
针对每笔交易收取固定的佣金
"""
class FixedPerTradeTaxFee(CommissionTaxFee):
    """A :class:`Commission` class that charges a fixed amount for the whole trade.

    :param amount: The commission for an order.
    :type amount: float.
    """
    def __init__(self, amount):
        super(FixedPerTradeTaxFee, self).__init__()
        self.__amount = amount

    def calculate(self, order, price, quantity):
        if not order.isSell():
            return 0
        ret = 0
        # Only charge the first fill.
        if order.getExecutionInfo() is None:
            ret = self.__amount
        return ret

"""
针对交易额的比例收取佣金
"""
class TradePercentageTaxFee(CommissionTaxFee):
    """A :class:`Commission` class that charges a percentage of the whole trade.

    :param percentage: The percentage to charge. 0.01 means 1%, and so on. It must be smaller than 1.
    :type percentage: float.
    """
    def __init__(self, percentage):
        super(TradePercentageTaxFee, self).__init__()
        assert(percentage < 1)
        self.__percentage = percentage

    @property
    def percentage(self):
        return self.__percentage

    def calculate(self, order, price, quantity):
        # 仅卖出时收取印花税
        if not order.isSell():
            return 0

        return price * quantity * self.__percentage





class StampTaxAShare(CommissionTaxFee):
    """
    根据交易额比例收取印花税，印花税会随日期变动，适用于A股
    """
    """A :class:`Commission` class that charges a percentage of the whole trade.

    :param percentage: The percentage to charge. 0.01 means 1%, and so on. It must be smaller than 1.
    :type percentage: float.
    """
    def __init__(self):
        super().__init__()
        self.__tax_1 = 0.001
        self.__tax_2 = 0.0005


    # @property
    # def percentage(self):
    #     return self.__percentage

    @property
    def desc(self):
        return f'<20230828: {self.__tax_1}, >=20280828: {self.__tax_2}'

    def calculate(self, order, price, quantity):
        if not order.isSell():
            return 0
        submitted_date = order.getSubmitDateTime()
        if submitted_date < '20230828':
            tax_rate = self.__tax_1
        else:
            tax_rate = self.__tax_2
        return price * quantity * tax_rate
