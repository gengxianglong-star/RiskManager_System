"""敞口：持仓分段 + pending（父单）+ 止损溢出。"""

from __future__ import annotations

from risk_core.constants import (
    ALLOWED_CURRENCY,
    ALLOWED_SEC_TYPE,
    MAX_STOP_PCT,
    MIN_ENTRY_PRICE,
    MIN_RISK_PER_SHARE,
)
from risk_core.models import OpenOrderView, ShadowPosition, Side


def _stop_trigger(order: OpenOrderView) -> float:
    if order.aux_price and order.aux_price > 0:
        return float(order.aux_price)
    if order.lmt_price and order.lmt_price > 0:
        return float(order.lmt_price)
    return 0.0


def _is_stop_exit(order: OpenOrderView) -> bool:
    ot = (order.order_type or "").upper().replace("_", " ")
    return ot in ("STP", "STP LMT", "TRAIL", "TRAIL LIMIT", "STPLMT")


def _entry_anchor(order: OpenOrderView) -> float:
    """开仓价锚：LMT 用 lmt_price；STP 用 aux_price（触发价）。"""
    return float(order.lmt_price or 0.0) or float(order.aux_price or 0.0)


def _is_entry_parent(order: OpenOrderView, positions: dict[str, ShadowPosition]) -> bool:
    """独立开仓父单（非已有仓的平仓/止损）。"""
    if order.parent_id not in (0, None):
        return False
    ot = (order.order_type or "").upper().replace("_", " ")
    if ot in ("MKT", "TRAIL", "TRAIL LIMIT"):
        return False  # 禁 MKT；追踪止损不作开仓 pending
    action = (order.action or "").upper()
    pos = positions.get(order.symbol)
    # 已有仓的反向 STP/STP LMT = 防守止损，不计开仓 pending
    if _is_stop_exit(order) and pos is not None:
        if pos.side == Side.LONG and action == "SELL":
            return False
        if pos.side == Side.SHORT and action == "BUY":
            return False
    if action == "BUY":
        # 开多或加多（含 STP / STP LMT 入场）
        if pos is None or pos.side == Side.LONG:
            return ot in ("LMT", "STP", "STP LMT", "MIT", "REL", "PEG MID") or "LMT" in ot
        return False
    if action == "SELL":
        # 开空或加空（无多仓时）
        if pos is None or pos.side == Side.SHORT:
            return ot in ("LMT", "STP", "STP LMT", "MIT") or "LMT" in ot
        return False
    return False


def covered_risk(entry: float, stop: float, qty: float, side: Side) -> float:
    if qty <= 0:
        return 0.0
    if side == Side.LONG:
        return max(0.0, entry - stop) * qty
    return max(0.0, stop - entry) * qty


def naked_risk_per_share(entry: float) -> float:
    return max(MAX_STOP_PCT * entry, MIN_RISK_PER_SHARE)


def naked_risk(entry: float, qty_naked: float) -> float:
    if qty_naked <= 0:
        return 0.0
    return naked_risk_per_share(entry) * qty_naked


def reject_low_price_naked(entry: float, qty_covered: float, qty: float) -> bool:
    """无止损覆盖且价格过低 → 应拒开/拒持有加仓。"""
    if qty <= 0:
        return False
    if qty_covered >= qty - 1e-9:
        return False
    return entry < MIN_ENTRY_PRICE


def position_risk_and_overflow(
    positions: list[ShadowPosition],
    orders: list[OpenOrderView],
) -> tuple[float, float, list[str]]:
    """
    Returns:
        position_risk, pending_overflow, warnings
    """
    warnings: list[str] = []
    pos_risk = 0.0
    overflow_pending = 0.0

    # 按 symbol 收集平仓向 stop 单
    stops_by_sym: dict[str, list[OpenOrderView]] = {}
    for o in orders:
        if o.currency != ALLOWED_CURRENCY or o.sec_type != ALLOWED_SEC_TYPE:
            continue
        if not _is_stop_exit(o):
            continue
        # 子单也可作防守覆盖（bracket 止损）
        stops_by_sym.setdefault(o.symbol, []).append(o)

    for pos in positions:
        if pos.qty <= 0:
            continue
        stops = stops_by_sym.get(pos.symbol, [])
        # 方向匹配：多仓需要 SELL stop；空仓需要 BUY stop
        matched: list[OpenOrderView] = []
        for s in stops:
            act = (s.action or "").upper()
            if pos.side == Side.LONG and act == "SELL":
                matched.append(s)
            elif pos.side == Side.SHORT and act == "BUY":
                matched.append(s)

        total_stop_qty = sum(max(0.0, s.remaining) for s in matched)
        qty_covered = min(pos.qty, total_stop_qty)
        qty_naked = max(0.0, pos.qty - qty_covered)

        if matched:
            triggers = [_stop_trigger(s) for s in matched if _stop_trigger(s) > 0]
            if pos.side == Side.LONG:
                stop_px = min(triggers) if triggers else pos.stop
            else:
                stop_px = max(triggers) if triggers else pos.stop
        else:
            stop_px = pos.stop

        if stop_px <= 0 and qty_covered > 0:
            stop_px = pos.stop

        pos_risk += covered_risk(pos.entry, stop_px if stop_px > 0 else pos.entry, qty_covered, pos.side)
        pos_risk += naked_risk(pos.entry, qty_naked)

        if reject_low_price_naked(pos.entry, qty_covered, pos.qty):
            warnings.append(f"{pos.symbol}: entry<{MIN_ENTRY_PRICE} naked")

        if total_stop_qty > pos.qty + 1e-9:
            qty_overflow = total_stop_qty - pos.qty
            ref = stop_px if stop_px > 0 else pos.entry
            overflow_pending += naked_risk_per_share(ref) * qty_overflow

    return pos_risk, overflow_pending, warnings


def pending_entry_risk(
    orders: list[OpenOrderView],
    positions: list[ShadowPosition],
) -> float:
    """未成交开仓父单 pending_risk（不含止损溢出）。"""
    pos_map = {p.symbol: p for p in positions}
    # parent_id -> stop child trigger
    stop_by_parent: dict[int, float] = {}
    for o in orders:
        if o.parent_id in (0, None):
            continue
        if _is_stop_exit(o):
            trig = _stop_trigger(o)
            if trig > 0:
                stop_by_parent[int(o.parent_id)] = trig

    total = 0.0
    for o in orders:
        if o.currency != ALLOWED_CURRENCY or o.sec_type != ALLOWED_SEC_TYPE:
            continue
        if not _is_entry_parent(o, pos_map):
            continue
        entry_px = _entry_anchor(o)
        if entry_px <= 0:
            # 无锚定价：不计（且 can_open 应已拒 MKT）
            continue
        rem = max(0.0, float(o.remaining))
        if rem <= 0:
            continue
        stop_px = stop_by_parent.get(int(o.order_id), 0.0)
        if stop_px > 0 and entry_px > 0:
            risk_pct = abs(entry_px - stop_px) / entry_px
        else:
            risk_pct = MAX_STOP_PCT
        total += entry_px * rem * risk_pct
    return total


def compute_exposure(
    positions: list[ShadowPosition],
    orders: list[OpenOrderView],
) -> tuple[float, float, list[str]]:
    """Returns position_risk, pending_risk (含 overflow), warnings."""
    pos_risk, overflow, warnings = position_risk_and_overflow(positions, orders)
    pending = pending_entry_risk(orders, positions) + overflow
    return pos_risk, pending, warnings


def estimate_new_entry_risk(
    entry_px: float,
    qty: float,
    stop_px: float = 0.0,
) -> float:
    if entry_px <= 0 or qty <= 0:
        return 0.0
    if stop_px > 0:
        return max(0.0, abs(entry_px - stop_px)) * qty
    return naked_risk_per_share(entry_px) * qty
