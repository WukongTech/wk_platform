# -*- coding: utf-8 -*-
# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>

Date: 20170719
Author:chenxiangdong
content:
佣金类commision和券商类broker

主要变动：
增加了对持仓状态和交易状态的跟踪记录
新增了对印花税的处理  20171027
新增了对个股个股持仓盈亏情况的调用接口  2017/10/27
"""
import typing
from abc import abstractmethod
from collections import OrderedDict
from collections import deque
from dataclasses import dataclass, field
from wk_util import logger
from wk_platform import broker
from wk_platform.config import StrategyConfiguration, PositionCostType, HedgeStrategyConfiguration  # , PriceType
from wk_platform.stratanalyzer.record import UnfilledOrderInfo, TransactionRecord
from wk_platform.broker.commission import *


@dataclass
class BrokerStatus:
    cash: float
    next_order_id: int
    shares: dict[str, float] = field(default_factory=dict)  # 当前持仓
    shares_can_sell: dict[str, float] = field(default_factory=dict)  # 记录当前可卖持仓
    position_cost: dict[str, float] = field(default_factory=dict)  # 记录持仓成本
    last_buy_time: dict[str, typing.Any] = field(default_factory=dict)  # 记录最近买入时间
    last_sell_time: dict[str, typing.Any] = field(default_factory=dict)  # 记录最近卖出时间
    amount_total: dict[str, typing.Any] = field(default_factory=dict)  # 累计买入金额
    position_delta: dict[str, typing.Any] = field(default_factory=dict)  # 记录持仓盈亏
    buy_volume: dict[str, int] = field(default_factory=dict)  # 记录买入数量
    sell_volume: dict[str, int] = field(default_factory=dict)  # 记录卖出数量
    transaction_tracker: deque = field(default_factory=deque)
    unfilled_order_tracker: deque = field(default_factory=deque)


class BaseBacktestBroker(broker.Broker):
    """
    回测券商类一些基本功能的实现
    """

    def getInstrumentTraits(self, instrument):
        return broker.IntegerTraits()

    def __init__(self, bar_feed, config=StrategyConfiguration()):
        super(BaseBacktestBroker, self).__init__()

        self.__bar_feed = bar_feed
        self.__use_adjusted_values = True
        self.__config = config

        self.__active_orders = OrderedDict()

        # 股票佣金
        if config.commission is None:
            self.__commission = NoCommission()
        else:
            self.__commission = config.commission

        # 印花税
        if config.stamp_tax is None:
            self.__stamp_tax = NoCommissionTaxFee()
        else:
            self.__stamp_tax = config.stamp_tax

        self.__next_order_id = 1

        self.__allow_negative_cash = False

        # # 用来记录当前持仓状态
        # self.__shares = {}

        # 记录每个交易日的交易额
        self.__long_amount = 0
        self.__short_amount = 0

        self.__transaction_tracker: [TransactionRecord] = deque()

    def _reset_daily_amount(self):
        self.__long_amount = 0
        self.__short_amount = 0

    @property
    def _next_order_id(self):
        ret = self.__next_order_id
        self.__next_order_id += 1
        return ret

    @property
    def commission(self):
        return self.__commission

    @commission.setter
    def commission(self, commission):
        self.__commission = commission

    @property
    def stamp_tax(self):
        return self.__stamp_tax

    @stamp_tax.setter
    def stamp_tax(self, stamp_tax):
        self.__stamp_tax = stamp_tax

    def adjusted_shares(self, shares):
        if self.__config.whole_batch_only:  # 限制整手买入时对可买股数进行调整
            return (shares // 100) * 100
        else:
            return int(shares)

    @property
    def current_bars(self):
        return self.__bar_feed.getCurrentBars()

    def bar_feed_subscribe(self, func):
        self.__bar_feed.getNewValuesEvent().subscribe(func)

    def _get_bar(self, bars, instrument):
        ret = bars.getBar(instrument)
        if ret is None:
            ret = self.__bar_feed.getLastBar(instrument)
        return ret

    def _register_order(self, order):
        assert (order.getId() not in self.__active_orders)
        assert (order.getId() is not None)
        self.__active_orders[order.getId()] = order

    def _unregister_order(self, order):
        assert (order.getId() in self.__active_orders)
        assert (order.getId() is not None)
        del self.__active_orders[order.getId()]

    @property
    def allow_negative_cash(self):
        return self.__allow_negative_cash

    @allow_negative_cash.setter
    def allow_negative_cash(self, allowNegativeCash):
        self.__allow_negative_cash = allowNegativeCash

    @property
    def use_adjusted_values(self):
        return self.__use_adjusted_values

    @use_adjusted_values.setter
    def use_adjusted_values(self, useAdjusted):
        # Deprecated since v0.15
        if not self.__bar_feed.barsHaveAdjClose():
            raise Exception("The barfeed doesn't support adjusted close values")
        self.__use_adjusted_values = useAdjusted

    def getActiveOrders(self, instrument=None):
        if instrument is None:
            ret = list(self.__active_orders.values())
        else:
            ret = [order for order in list(self.__active_orders.values()) if order.getInstrument() == instrument]
        return ret

    def getActiveOrdersDict(self):
        return self.__active_orders

    @property
    def _current_date_time(self):
        return self.__bar_feed.getCurrentDateTime()

    @property
    def active_instruments(self):
        return [instrument for instrument, shares in self.__shares.items() if shares != 0]

    @property
    def bar_feed_frequency(self):
        return self.__bar_feed.getFrequency()

    def start(self):
        super(BaseBacktestBroker, self).start()

    def stop(self):
        pass

    def join(self):
        pass

    def eof(self):
        # If there are no more events in the barfeed, then there is nothing left for us to do since all processing took
        # place while processing barfeed events.
        return self.__bar_feed.eof()

    def dispatch(self):
        # All events were already emitted while handling barfeed events.
        pass

    def peekDateTime(self):
        return None

    def getUseAdjustedValues(self):
        return self.use_adjusted_values

    def getCommission(self):
        return self.commission

    @property
    def transaction_tracker(self):
        """
        返回交易流水记录
        """
        return self.__transaction_tracker

    @transaction_tracker.setter
    def transaction_tracker(self, transaction_tracker):
        self.__transaction_tracker = transaction_tracker

    @property
    def long_amount(self):
        return self.__long_amount

    @property
    def short_amount(self):
        return self.__short_amount

    def _append_transaction(self, trade_dt: str, windcode: str, sec_name: str,
                            price: float, volume: int, commission: float,
                            stamp_tax: float, direction: str, note: str = ''
                            ):

        assert direction in ('买入', '卖出', '换入', '换出', "退市卖出", '到期转为现金')
        if direction == '买入':
            self.__long_amount += price * volume
        elif direction == '卖出':
            self.__short_amount += price * volume

        if not self.__config.tracking_transaction:
            return
        trans_rec = TransactionRecord(
            trade_dt,
            windcode,
            sec_name,
            price,
            volume,
            commission,
            stamp_tax,
            direction,
            note
        )
        self.__transaction_tracker.append(trans_rec)

    @abstractmethod
    def export_status(self):
        """导出当前Broker状态，用于日内Broker
        TODO: 导出状态用于checkpoint
        导出信息包括：
        持仓详情，现金
        """
        raise NotImplementedError()

    @abstractmethod
    def update_status(self, status: BrokerStatus):
        raise NotImplementedError()





