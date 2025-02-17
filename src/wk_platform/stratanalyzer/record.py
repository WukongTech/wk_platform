from __future__ import annotations

from dataclasses import dataclass
import typing


@dataclass(frozen=True)
class BaseRecordType:
    annotation_: typing.ClassVar[dict[str, str]]
    index_: typing.ClassVar[str | None]



@dataclass(frozen=True)
class UnfilledOrderInfo:
    # self.__logger.warning("Not enough cash to fill %s order [%s] for %s share/s" % (
    #     order.getInstrument(),
    #     order.getId(),
    #     order.getRemaining()
    # ))
    date_time: 'typing.Any'
    instrument: str
    order_id: str
    remaining: int
    direction: str
    info: str


@dataclass(frozen=True)
class TransactionRecord:
    trade_dt: str       # 交易日期
    windcode: str       # 证券代码
    sec_name: str       # 证券名称
    price: float        # 成交价格
    volume: int         # 成交数量
    commission: float   # 佣金花费
    stamp_tax: float    # 印花税花费
    direction: str      # 成交方向
    note: str = ''      # 备注


@dataclass(frozen=True)
class DetailedPositionRecord:
    trade_dt: str       # 交易日期
    windcode: str       # 证券代码
    sec_name: str       # 证券名称
    position: int       # 持仓数目
    sellable: int       # 可卖数目
    open_price: float   # 最新开盘价格
    close_price: float  # 最新收盘价格
    last_buy: str       # 最近买入
    last_sell: str      # 最近卖出
    cost: float         # 持仓成本
    pnl: float          # 持仓盈亏


@dataclass(frozen=True)
class TotalPositionRecord:
    trade_dt: str   # 交易日期
    equity: float   # 总资产
    value: float    # 总市值
    cash: float     # 总现金
    position: float # 总仓位


@dataclass(frozen=True)
class TotalPositionRecord2:
    """
    包含期货的持仓记录
    """
    trade_dt: str           # 交易日期
    equity: float           # 总资产
    stock_value: float      # 股票总市值
    cash: float             # 总现金
    position: float         # 总仓位
    future_value: float     # 期货头寸
    future_profit: float    # 期货盈亏
    hedge_ratio: float      # 对冲比率


@dataclass(frozen=True)
class PositionRecord:
    trade_dt: str   # 交易日期
    windcode: str   # 股票代码
    volume: int     # 持仓数量
    value: float    # 持仓市值


