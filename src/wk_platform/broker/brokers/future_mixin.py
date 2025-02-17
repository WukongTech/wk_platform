from wk_platform.broker import Order
from wk_platform.broker.brokers.position import StockPosition, FuturePosition
from wk_platform.util import FutureUtil


class FutureMixin(object):
    def __init__(self):
        self.__futures: { str: FuturePosition } = {}
        self.__frozen_cash = 0


    @property
    def frozen_cash(self):
        return self.__frozen_cash

    @property
    def deposit_ratio(self):
        """
        返回保证金占现金的比例

        注意，理论上期货为每日盯市场，由于仅在换仓时计入利润，平时计算保证金/现金比例时需扣除盈亏
        """
        return self.__frozen_cash / self.__cash

    def update_future(self):
        """
        期货每日盯市
        """
        futures_profit = 0
        deposit = 0
        count = 0
        if len( self.__futures.items()) < 1:
            return
        # print('tag', self.__futures)
        assert len( self.__futures.items()) == 1
        instrument, position = list(self.__futures.items())[0]
        price = self._get_bar(self.current_bars, instrument).get_price(self.__config.price_type)
        futures_profit += (price - position.point) * position.quantity
        position.point = price
        deposit += abs(position.point * position.quantity * self.__config.deposit)
        count += 1

        self.__cash += futures_profit
        self.__frozen_cash = deposit

        if self.__cash <= self.__frozen_cash:
            action = Order.Action.BUY if position.quantity < 0 else Order.Action.SELL
            amount = self.__cash / (self.__config.future_commission + self.__config.deposit) * 0.999
            max_quantity = FutureUtil.calc_future_quantity(amount, price)
            order = self.createMarketOrder(action, instrument, abs(position.quantity)-max_quantity)
            self.submitOrder(order)
            self.onBarsImpl(order, self.current_bars)

    def update_deposit(self):
        """
        更新期货保证金
        """
        deposit = 0
        previous_deposit_ratio = self.deposit_ratio
        for instrument, position in self.__futures.items():
            price = self._get_bar(self.current_bars, instrument).open
            deposit += abs(price * position.quantity) * self.__config.deposit
        self.__frozen_cash = deposit
        try:
            assert self.__frozen_cash < self.__cash
        except Exception as e:
            self.__debug_info()
            print('[previous deposit ratio]', previous_deposit_ratio)
            print('[deposit ratio]', self.deposit_ratio)
            raise e