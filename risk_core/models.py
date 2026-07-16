"""RiskCore 数据结构。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Side(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass
class ShadowPosition:
    symbol: str
    side: Side
    qty: float
    entry: float
    stop: float = 0.0
    life_realized_pnl: float = 0.0
    max_abs_qty: float = 0.0
    avg_cost_ib: float = 0.0  # 最近一次 IB averageCost（漂移校验）

    def __post_init__(self) -> None:
        if isinstance(self.side, str):
            self.side = Side(self.side)
        if self.max_abs_qty <= 0 and self.qty:
            self.max_abs_qty = abs(self.qty)


@dataclass
class OpenOrderView:
    """IB 挂单的极简视图（供敞口计算）。"""

    symbol: str
    order_id: int
    parent_id: int
    action: str  # BUY / SELL
    order_type: str  # LMT, STP, STP LMT, TRAIL, MKT, ...
    remaining: float
    lmt_price: float = 0.0
    aux_price: float = 0.0  # stop trigger
    currency: str = "USD"
    sec_type: str = "STK"
    perm_id: int = 0


@dataclass
class InFlightIntent:
    intent_id: str
    estimated_risk: float
    expires_at: float  # monotonic or epoch seconds
    symbol: str = ""


@dataclass
class CanOpenRequest:
    symbol: str
    qty: float
    entry_px: float
    stop_px: float = 0.0
    side: Side = Side.LONG
    order_type: str = "LMT"
    currency: str = "USD"
    sec_type: str = "STK"
    intent_id: str = ""


@dataclass
class CanOpenResult:
    allowed: bool
    reason: str = ""
    intent_id: str = ""
    estimated_risk: float = 0.0
    risk_budget: float = 0.0
    risk_light: str = ""


@dataclass
class RiskState:
    nlv: float = 0.0
    hwm: float = 0.0
    rth_close_nlv: float = 0.0
    drawdown: float = 0.0
    consecutive_losses: int = 0
    position_risk: float = 0.0
    pending_risk: float = 0.0
    in_flight_risk: float = 0.0
    total_risk: float = 0.0
    cushion: float = 1.0
    is_rth: bool = False
    risk_light: str = "🟢"
    daily_opens: int = 0
    sync_ok: bool = False
    sync_block_reason: str = ""
    connected: bool = False


@dataclass
class LifecycleSettlement:
    """一次生命周期终结的结果。"""

    symbol: str
    life_pnl: float
    streak_delta: int  # +1, 0, or reset signal (-999 means clear)
    cleared: bool = False  # True if streak reset to 0


def signed_qty(side: Side, qty: float) -> float:
    q = abs(qty)
    return q if side == Side.LONG else -q
