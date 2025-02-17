from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class BasePosition:
    windcode: str
    quantity: int


@dataclass
class StockPosition(BasePosition):
    price: float = 0                     # 开仓价
    share_can_sell: int = 0              # 记录当前可卖持仓
    position_cost: float = 0             # 持仓成本
    last_buy_time: str | None = None     # 最近买入时间
    last_sell_time: str | None = None    # 记录最近卖出时间
    position_delta: float | None = None  # 记录持仓盈亏
    update_date: str | None = None       # 更新日期
    amount_total: float = 0              # 累计买入金额

@dataclass
class FuturePosition(BasePosition):
    # code: str = ''     # IF，IC
    point: float = 0   # 开仓点数
    share_can_sell: int = np.inf  # 记录当前可卖持仓
    last_buy_time: str | None = None  # 最近买入时间
    last_sell_time: str | None = None  # 记录最近卖出时间
    # position_delta: float | None = None  # 记录持仓盈亏
