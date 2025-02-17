from collections import deque

from wk_platform.config.enums import PriceType

from wk_platform.strategy.strategy import BacktestingStrategy
from wk_platform.config import StrategyConfiguration
from wk_data.constants import ExtStatus
import wk_data


class LowFreqBacktestingStrategy(BacktestingStrategy):
    def __init__(self, bar_feed, begin_date, end_date, *,
                 broker_cls=None, config=StrategyConfiguration(), ext_status_data=None, sign=None):
        super().__init__(bar_feed, broker_cls, config)

        self.__config = config

        """
        记录策略开始和结束时间
        """
        self.__begin_date = begin_date
        self.__end_date = end_date

        self.__ext_date = None
        self.__ext_status_data = deque(ext_status_data) if ext_status_data is not None else deque()

        # ds = DataSource()
        mr_map = wk_data.get('mr_data')
        self.__mr_map = mr_map if mr_map is not None else {}

        self.__current_date_str = None
        self.__sign = sign

        self.__stop_pnl_exclude: {str: bool} = {}

    @property
    def sign(self):
        return self.__sign

    @property
    def begin_date(self):
        return self.__begin_date

    @property
    def end_date(self):
        return self.__end_date

    @property
    def current_date_str(self):
        return self.__current_date_str

    def __handle_ext_status(self, bars):
        record = self.__ext_status_data[0]
        date_str = bars.getDateTime().strftime("%Y%m%d")
        if record['ext_status'] == ExtStatus.DUMMY_BAR_FOR_DELISTING.value:
            # 以0价格清仓退市股票
            inst = record['windcode']
            # shares = self.getBroker().getShares(inst)
            self.getBroker().clean_position(bars, inst)  # 直接清零，不进行交易
            self.delisting_hook(inst)

        elif record['ext_status'] == ExtStatus.DUMMY_BAR_FOR_CHANGING_WINDCODE.value:
            # 对并购重组的情况进行调仓，根据比例变换为新的持仓
            inst = record['windcode']
            mr_rec = self.__mr_map[inst]
            new_inst = mr_rec.new_windcode
            try:
                ratio = mr_rec.ratio * bars[inst].adj_factor  # 根据复权因子修正调仓比例

                # old_close = bars[inst].
                self.getBroker().transform_shares(bars, inst, new_inst, ratio)
                self.mr_hook(inst, new_inst, ratio)
            except KeyError:
                assert self.getBroker().getShares(inst) == 0, f'Unable to transform position of `{inst}`'
        elif record['ext_status'] == ExtStatus.DUMMY_BAR_FOR_TRANSFORMING_TO_CASH.value:
            # 转换持仓为现金
            inst = record['windcode']
            self.getBroker().transform_to_cash(bars, inst)
            self.transform_to_cash_cash_hook(inst)

    def mr_hook(self, original_code, new_code, ratio):
        pass

    def delisting_hook(self, code):
        pass

    def transform_to_cash_cash_hook(self, inst):
        pass

    def stop_pnl_hook(self, bars, stop_pnl_args):
        """
        发生止盈/止损时的钩子函数
        Parameters
        --------------------
        bars: Bar
            行情bar
        stop_pnl_args: [tuple(inst, amount, pnl)]
            所有发生止盈止损操作的标的详情
        """
        pass

    def intraday_stop_pnl_hook(self, bars, stop_pnl_args):
        """
        发生日内止盈/止损时的钩子函数
        Parameters
        --------------------
        bars: Bar
            行情bar
        stop_pnl_args: [tuple(inst, amount, pnl)]
            所有发生止盈止损操作的标的详情
        """
        pass

    def end_of_bar_hook(self, bars):
        """
        bar结束的hook
        """
        pass

    def stop_pnl_exclude(self, inst):
        self.__stop_pnl_exclude[inst] = True

    def stop_pnl_is_excluded(self, inst):
        return inst in self.__stop_pnl_exclude

    def reset_stop_pnl_excluded(self):
        self.__stop_pnl_exclude = {}

    def __stop_pnl(self, bars):
        """
        止盈止损操作
        """
        if not self.__config.stop_pnl:
            return

        op_list = []

        for inst in self.getBroker().getPositions().keys():
            if self.stop_pnl_is_excluded(inst):
                continue
            pnl = self.getBroker().get_pnl(bars, inst)
            prev_amount = self.getBroker().getSharesAmount(inst, bars)
            if self.__config.stop_profit and pnl >= self.__config.stop_profit:
                # 止盈清仓

                self.enterLongShortWeight(bars, inst, 0, False, False, msg='止盈卖出')  # 清仓
                op_list.append((inst, prev_amount, pnl))
                # self.enterLongShortWeight(bars, inst,stockWeight/2.0,False, False)#卖一半
            if self.__config.stop_loss and pnl <= self.__config.stop_loss:
                # 止损清仓
                self.enterLongShortWeight(bars, inst, 0, False, False, msg='止损卖出')  # 清仓
                op_list.append((inst, prev_amount, pnl))
        if len(op_list) > 0:
            self.stop_pnl_hook(bars, op_list)


    def __intraday_stop_pnl(self, bars):
        if not self.__config.intraday_stop_pnl:
            return

        op_list = []

        for inst in self.getBroker().getPositions().keys():
            if self.stop_pnl_is_excluded(inst):
                continue
            bar = bars[inst]
            # prev_amount = self.getBroker().getSharesAmount(inst, bars)
            # prev_quantity = self.getBroker().getShares(inst)
            max_loss = (bar.get_price(PriceType.LOW) - bar.get_price(PriceType.PRE_CLOSE)) / bar.get_price(PriceType.PRE_CLOSE)
            max_profit = (bar.get_price(PriceType.HIGH) - bar.get_price(PriceType.PRE_CLOSE)) / bar.get_price(
                PriceType.PRE_CLOSE)
            current_share = self.getBroker().getShares(inst)

            if self.__config.intraday_stop_loss and max_loss <= self.__config.intraday_stop_loss:
                # 止损清仓
                open_sell = bar.get_price(PriceType.OPEN)
                open_loss = (open_sell - bar.get_price(PriceType.PRE_CLOSE)) / bar.get_price(PriceType.PRE_CLOSE)

                if open_loss <= self.__config.intraday_stop_loss:
                    limit_price = bar.get_price(PriceType.OPEN)
                    pnl = open_loss
                    # self.enterShortLimit(bars, inst, bar.get_price(PriceType.OPEN), current_share, False, msg='止损卖出')  # 清仓
                    # op_list.append((inst, current_share * , open_loss))
                else:
                    stop_loss_price = (1 + self.__config.intraday_stop_loss) *  bar.get_price(PriceType.PRE_CLOSE)
                    assert stop_loss_price >= bar.get_price(PriceType.LOW)
                    limit_price = stop_loss_price
                    pnl = self.__config.intraday_stop_loss
                    # self.enterShortLimit(bars, inst, stop_loss_price, current_share, False,
                    #                                msg='止损卖出')  # 清仓
                    # op_list.append((inst, prev_amount, self.__config.intraday_stop_loss))

                self.enterShortLimit(bars, inst, limit_price, current_share, False, inBar=True, msg='止损卖出')  # 清仓
                op_list.append((inst, current_share * limit_price, pnl))

            elif self.__config.intraday_stop_profit and max_profit >= self.__config.intraday_stop_profit:
                # 如果当日同时触发止盈和止损，仅考虑止损
                # 止盈清仓
                open_sell = bar.get_price(PriceType.OPEN)
                open_profit = (open_sell - bar.get_price(PriceType.PRE_CLOSE)) / bar.get_price(PriceType.PRE_CLOSE)

                if open_profit >= self.__config.intraday_stop_profit:
                    limit_price = bar.get_price(PriceType.OPEN)
                    pnl = open_profit
                    # self.enterShortLimit(bars, inst, bar.get_price(PriceType.OPEN), current_share, False, msg='止盈卖出') # 清仓
                    # op_list.append((inst, prev_amount, open_profit))
                else:
                    stop_profit_price = (1 + self.__config.intraday_stop_profit) * bar.get_price(PriceType.PRE_CLOSE)
                    assert stop_profit_price <= bar.get_price(PriceType.HIGH)
                    limit_price = stop_profit_price
                    pnl = self.__config.intraday_stop_profit
                self.enterShortLimit(bars, inst, limit_price, current_share, False, inBar=True, msg='止盈卖出')  # 清仓
                op_list.append((inst, current_share * limit_price, pnl))

        if len(op_list) > 0:
            self.intraday_stop_pnl_hook(bars, op_list)

    def onStart(self):
        self.__ext_date = deque([d['trade_dt'] for d in self.__ext_status_data])
        self.on_start()

    def onBars(self, bars):
        self.__current_date_str = bars.getDateTime().strftime("%Y%m%d")

        while len(self.__ext_date) > 0 and self.current_date_str == self.__ext_date[0]:
            self.__handle_ext_status(bars)
            self.__ext_date.popleft()
            self.__ext_status_data.popleft()

        if self.__config.price_type == PriceType.CLOSE:
            self.__intraday_stop_pnl(bars)  # 收盘价交易时先处理日内stop_pnl
        self.on_bars(bars)
        if self.__config.price_type == PriceType.OPEN:
            self.__intraday_stop_pnl(bars)

        self.__stop_pnl(bars)
        self.end_of_bar_hook(bars)

    @classmethod
    def prepare_feed(cls, begin_date, end_date, instruments, config):
        raise NotImplementedError()



