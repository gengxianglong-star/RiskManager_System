"""ib_insync → RiskCore 视图转换（下单工具 / 会话同步用）。"""

from __future__ import annotations

from typing import Any, Iterable

from risk_core.models import OpenOrderView


def trade_to_open_order_view(trade: Any) -> OpenOrderView | None:
    """从 ib_insync Trade 提取 OpenOrderView。"""
    try:
        c = trade.contract
        o = trade.order
        remaining = float(getattr(trade.orderStatus, "remaining", 0) or 0)
        if remaining <= 0 and getattr(trade.orderStatus, "status", "") in (
            "Filled",
            "Cancelled",
            "Inactive",
        ):
            return None
        return OpenOrderView(
            symbol=str(c.symbol),
            order_id=int(o.orderId or 0),
            parent_id=int(getattr(o, "parentId", 0) or 0),
            action=str(o.action or "").upper(),
            order_type=str(o.orderType or "").upper().replace("STPLMT", "STP LMT"),
            remaining=remaining if remaining > 0 else float(o.totalQuantity or 0),
            lmt_price=float(o.lmtPrice or 0),
            aux_price=float(o.auxPrice or 0),
            currency=str(getattr(c, "currency", "USD") or "USD"),
            sec_type=str(getattr(c, "secType", "STK") or "STK"),
            perm_id=int(getattr(o, "permId", 0) or 0),
        )
    except Exception:
        return None


def trades_to_open_orders(trades: Iterable[Any]) -> list[OpenOrderView]:
    out: list[OpenOrderView] = []
    for t in trades:
        v = trade_to_open_order_view(t)
        if v is not None:
            out.append(v)
    return out


def account_summary_to_dict(tags: Iterable[Any]) -> dict[str, float]:
    """ib accountValues / accountSummary 行 → RiskCore 账户字典。"""
    result: dict[str, float] = {}
    tag_map = {
        "NetLiquidation": "net_liquidation",
        "Cushion": "cushion",
        "ExcessLiquidity": "excess_liquidity",
        "GrossPositionValue": "gross_position_value",
    }
    for row in tags:
        key = tag_map.get(getattr(row, "tag", None))
        if not key:
            continue
        if getattr(row, "currency", "USD") not in ("USD", "", None) and key != "cushion":
            continue
        try:
            result[key] = float(row.value)
        except (TypeError, ValueError):
            continue
    return result
