from abc import abstractmethod
from collections import deque
from dataclasses import dataclass
import pathlib

import numpy as np
from wk_platform.stratanalyzer.record import BaseRecordType
from wk_data.constants import SuspensionType
from wk_platform.config import PriceType
from wk_platform.feed.bar import StockBar
from wk_platform.util.future import FutureUtil

DEFAULT_INDEX = ["000001.SH", "000016.SH", "000300.SH", "399905.SZ", "000852.SH", "399006.SZ"]


@dataclass(frozen=True)
class TradeDayWinRatioRecord(BaseRecordType):
    date_range: str
    win_ratio: float
    wr_000001_sh: float
    wr_000016_sh: float
    wr_000300_sh: float
    wr_399905_sz: float
    wr_000852_sh: float
    wr_399006_sz: float
    annotation_ = {
        'date_range': '区间',
        'win_ratio': '策略胜率',
        'wr_000001_sh': '策略对冲上证综指胜率',
        'wr_000016_sh': '策略对冲上证50胜率',
        'wr_000300_sh': '策略对冲沪深300胜率',
        'wr_399905_sz': '策略对冲中证500胜率',
        'wr_000852_sh': '策略对冲中证1000胜率',
        'wr_399006_sz': '策略对冲创业板指胜率',
    }
    index_ = '区间'


@dataclass(frozen=True)
class TradeDayReturnRecord(BaseRecordType):
    date_range: str
    ret: float
    ret_000001_sh: float
    ret_000016_sh: float
    ret_000300_sh: float
    ret_399905_sz: float
    ret_000852_sh: float
    ret_399006_sz: float
    ret_hedge_000001_sh: float
    ret_hedge_000016_sh: float
    ret_hedge_000300_sh: float
    ret_hedge_399905_sz: float
    ret_hedge_000852_sh: float
    ret_hedge_399006_sz: float
    annotation_ = {
        'date_range': '区间',
        'ret': '策略收益率',
        'ret_000001_sh': '上证综指',
        'ret_000016_sh': '上证50',
        'ret_000300_sh': '沪深300',
        'ret_399905_sz': '中证500',
        'ret_000852_sh': '中证1000',
        'ret_399006_sz': '创业板指',
        'ret_hedge_000001_sh': '策略对冲上证综指',
        'ret_hedge_000016_sh': '策略对冲上证50',
        'ret_hedge_000300_sh': '策略对冲沪深300',
        'ret_hedge_399905_sz': '策略对冲中证500',
        'ret_hedge_000852_sh': '策略对冲中证1000',
        'ret_hedge_399006_sz': '策略对冲创业板指',
    }
    index_ = '区间'

    @classmethod
    def index_return_name(cls, windcode: str):
        return f"ret_{windcode.replace('.', '_').lower()}"

    @classmethod
    def index_hedge_return_name(cls, windcode: str):
        return f"ret_hedge_{windcode.replace('.', '_').lower()}"


@dataclass(frozen=True)
class TradeDayRecord(BaseRecordType):
    date_str: str
    turnover_rate: float
    capacity: float
    annotation_ = {
        'date_str': '日期',
        'turnover_rate': '换手率',
        'capacity': '估算容量（亿元）'
    }
    index_ = '日期'


@dataclass
class PositionRecord:
    last_price: float
    update_date: str
    # ref_ratio = 0


class TradeDayTrackerMixin:
    def __init__(self, broker, config):

        self.__config = config
        # self.__broker = broker

        self.__ref_price: dict[str, PositionRecord] = {}
        self.__ref_index: dict[str, float] = {}
        self.__wr_ignore: dict[str, bool] = {}

        # 追踪调仓期间收益率
        self.__prev_total_asset = broker.get_total_equity(price_type=PriceType.CLOSE)  # 默认使用收盘价计算收益率
        self.__prev_index: dict[str, float] = {} # 追踪指数

    @abstractmethod
    def getBroker(self):
        pass

    def calc_win_ratio(self, bars):
        bench_dict = {
            "win_ratio": 0,
            "wr_000001_sh": 0,
            "wr_000016_sh": 0,
            "wr_000300_sh": 0,
            "wr_399905_sz": 0,
            "wr_000852_sh": 0,
            "wr_399006_sz": 0
        }
        if len(self.__ref_price) == 0:
            for k, v in bench_dict.items():
                bench_dict[k] = np.nan
            return bench_dict
        current_instruments = list(self.getBroker().getPositions().keys())
        total = len(current_instruments)
        if total == 0:
            return bench_dict
        default_index = ["000001.SH", "000016.SH", "000300.SH", "399905.SZ", "000852.SH", "399006.SZ"]

        for inst in current_instruments:
            try:
                if self.__wr_ignore[inst]:
                    continue
            except KeyError:
                pass
            prev_price = self.__ref_price[inst].last_price
            curr_price = bars[inst].get_price(self.__config.price_type)
            if prev_price == 0:
                stock_ret = -1
            else:
                stock_ret = curr_price / prev_price - 1
            if stock_ret > 0:
                bench_dict['win_ratio'] += 1
            for index_code in default_index:
                t = bars[index_code]
                t = self.__ref_index[index_code]
                ret = bars[index_code].get_price(self.__config.price_type) / self.__ref_index[index_code] - 1
                if stock_ret > ret:
                    bench_dict[f"wr_{index_code.replace('.', '_').lower()}"] += 1

        for k, v in bench_dict.items():
            bench_dict[k] = bench_dict[k] / total
        return bench_dict

    def refresh_ref_price(self, bars):
        self.__ref_price = {}
        self.__wr_ignore = {}
        current_instruments = list(self.getBroker().getPositions().keys())
        for inst in current_instruments:
            if FutureUtil.is_index_future(inst): # 计算胜率时忽略股指期货
                self.__wr_ignore[inst] = True

            self.__ref_price[inst] = PositionRecord(last_price=bars[inst].get_price(self.__config.price_type),
                                                    update_date=bars.getDateTime().strftime("%Y%m%d"))
        default_index = ["000001.SH", "000016.SH", "000300.SH", "399905.SZ", "000852.SH", "399006.SZ"]
        for inst in default_index:
            self.__ref_index[inst] = bars[inst].get_price(self.__config.price_type)




    def ref_price_ignore(self, inst):
        """
        非换仓日出现交易的特殊处理
        """
        self.__wr_ignore[inst] = True

    def refresh_return_data(self, bars):
        """
        刷新收益率计算数据
        """
        self.__prev_total_asset = self.getBroker().get_total_equity(price_type=PriceType.CLOSE) # 默认使用收盘价计算收益率
        for inst in DEFAULT_INDEX:
            self.__prev_index[inst] = bars[inst].get_price(PriceType.CLOSE)

    def calc_ret(self, bars):
        bench_dict = {}
        current_asset = self.getBroker().get_total_equity(price_type=PriceType.CLOSE) # 默认使用收盘价计算收益率
        ret = current_asset / self.__prev_total_asset - 1
        self.__prev_total_asset = current_asset

        bench_dict['ret'] = ret
        for inst in DEFAULT_INDEX:
            cur_price = bars[inst].get_price(PriceType.CLOSE)
            index_ret = cur_price / self.__prev_index[inst] - 1
            bench_dict[TradeDayReturnRecord.index_return_name(inst)] = index_ret
            self.__prev_index[inst] = cur_price
            bench_dict[TradeDayReturnRecord.index_hedge_return_name(inst)] = ret - index_ret

        return bench_dict

    def calc_capacity(self, bars, instruments):
        amount_ma_pair = []
        ma = []
        for inst in instruments:
            bar = bars[inst]
            # 暂时仅对个股考虑容量计算问题
            if not isinstance(bar, StockBar):
                continue
            tradable = not (bar.getTradeStatus() != SuspensionType.NORMAL and self.__config.suspension_limit)
            if not tradable:
                continue
            amount_ma = bar.getExtraColumns()['amount_ma']
            amount_ma_pair.append((inst, amount_ma))
            ma.append(amount_ma)

        if len(amount_ma_pair) == 0:
            return 0
        amount_ma_pair = sorted(amount_ma_pair, key=lambda x: x[1])
        quantile_idx = int(len(amount_ma_pair) * self.__config.capacity_quantile)
        capacity = amount_ma_pair[quantile_idx][1] * self.__config.capacity_proportion * len(instruments)
        return capacity

    def trade_day_tracker_mr_hook(self, original_code, new_code, ratio, date_str):
        try:
            last_price = self.__ref_price[original_code].last_price
            self.__ref_price[new_code] = PositionRecord(last_price / ratio, date_str)
        except KeyError:
            pass

