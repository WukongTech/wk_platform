from wk_platform import broker


class BacktestingOrderMixin(object):
    def __init__(self, msg=None, *args, **kwargs):
        self.__accepted = None
        self.__msg = msg if msg is not None else ''

    @property
    def msg(self):
        return self.__msg

    def setAcceptedDateTime(self, dateTime):
        self.__accepted = dateTime

    def getAcceptedDateTime(self):
        return self.__accepted

    # Override to call the fill strategy using the concrete order type.
    # return FillInfo or None if the order should not be filled.
    def process(self, broker_, bar_):
        raise NotImplementedError()


class MarketOrder(broker.MarketOrder, BacktestingOrderMixin):
    def __init__(self, action, instrument, quantity, onClose, instrumentTraits, msg=None):
        super(MarketOrder, self).__init__(action, instrument, quantity, onClose, instrumentTraits)
        BacktestingOrderMixin.__init__(self, msg=msg)

    def process(self, broker_, bar_):
        return broker_.getFillStrategy(bar_).fillMarketOrder(broker_, self, bar_)


class LimitOrder(broker.LimitOrder, BacktestingOrderMixin):
    def __init__(self, action, instrument, limitPrice, quantity, instrumentTraits, inBar=False, msg=None):
        super(LimitOrder, self).__init__(action, instrument, limitPrice, quantity, instrumentTraits)
        self.__in_bar = inBar
        BacktestingOrderMixin.__init__(self, msg=msg)

    @property
    def in_bar(self):
        return self.__in_bar

    def process(self, broker_, bar_):
        return broker_.getFillStrategy(bar_).fillLimitOrder(broker_, self, bar_)


class StopOrder(broker.StopOrder, BacktestingOrderMixin):
    def __init__(self, action, instrument, stopPrice, quantity, instrumentTraits):
        super(StopOrder, self).__init__(action, instrument, stopPrice, quantity, instrumentTraits)
        self.__stopHit = False

    def process(self, broker_, bar_):
        return broker_.getFillStrategy().fillStopOrder(broker_, self, bar_)

    def setStopHit(self, stopHit):
        self.__stopHit = stopHit

    def getStopHit(self):
        return self.__stopHit


# http://www.sec.gov/answers/stoplim.htm
# http://www.interactivebrokers.com/en/trading/orders/stopLimit.php
class StopLimitOrder(broker.StopLimitOrder, BacktestingOrderMixin):
    def __init__(self, action, instrument, stopPrice, limitPrice, quantity, instrumentTraits):
        super(StopLimitOrder, self).__init__(action, instrument, stopPrice, limitPrice, quantity, instrumentTraits)
        self.__stopHit = False  # Set to true when the limit order is activated (stop price is hit)

    def setStopHit(self, stopHit):
        self.__stopHit = stopHit

    def getStopHit(self):
        return self.__stopHit

    def isLimitOrderActive(self):
        # TODO: Deprecated since v0.15. Use getStopHit instead.
        return self.__stopHit

    def process(self, broker_, bar_):
        return broker_.getFillStrategy().fillStopLimitOrder(broker_, self, bar_)