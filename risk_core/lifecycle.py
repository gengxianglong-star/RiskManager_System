"""仓位生命周期：零点穿越、实质性清仓、scratch、连亏。"""

from __future__ import annotations

from risk_core.constants import MATERIAL_DEPLETION_PCT, scratch_tolerance
from risk_core.models import LifecycleSettlement, ShadowPosition, Side, signed_qty


def should_terminate_lifecycle(
    old_signed_qty: float,
    new_signed_qty: float,
    max_abs_qty: float,
    had_reducing_fill: bool,
) -> bool:
    """零点穿越或实质性清仓。"""
    # 零点穿越 / 反手
    if old_signed_qty > 0 and new_signed_qty <= 0:
        return True
    if old_signed_qty < 0 and new_signed_qty >= 0:
        return True
    # 实质性清仓（防留 1 股）
    if (
        had_reducing_fill
        and max_abs_qty > 0
        and abs(new_signed_qty) < abs(max_abs_qty) * MATERIAL_DEPLETION_PCT
        and abs(new_signed_qty) > 1e-12  # 已归零由穿越处理；残余碎股也触发
    ):
        return True
    # 归零（含粉尘）
    if abs(old_signed_qty) > 1e-12 and abs(new_signed_qty) < 1e-12:
        return True
    return False


def settle_streak(life_pnl: float, nlv: float | None = None) -> tuple[int, bool]:
    """
    Returns:
        (streak_action, cleared)
        streak_action: +1 连亏, 0 不变, -1 表示清零
    """
    tol = scratch_tolerance(nlv)
    if life_pnl < -tol:
        return 1, False
    if life_pnl > 0:
        return -1, True
    return 0, False  # scratch 带内不动作


def apply_qty_delta(
    pos: ShadowPosition | None,
    symbol: str,
    fill_signed_qty: float,
    fill_price: float,
    commission: float = 0.0,
    nlv: float | None = None,
) -> tuple[ShadowPosition | None, LifecycleSettlement | None, ShadowPosition | None]:
    """
    将一笔成交应用到生命周期。

    Returns:
        (updated_or_none, settlement_if_any, new_lifecycle_if_reversal)
    """
    if pos is None:
        # 新开
        side = Side.LONG if fill_signed_qty > 0 else Side.SHORT
        qty = abs(fill_signed_qty)
        new_pos = ShadowPosition(
            symbol=symbol,
            side=side,
            qty=qty,
            entry=fill_price,
            max_abs_qty=qty,
        )
        return new_pos, None, None

    old_signed = signed_qty(pos.side, pos.qty)
    # 已实现 PnL 增量（减仓部分）
    reducing = 0.0
    if old_signed > 0 and fill_signed_qty < 0:
        reducing = min(abs(fill_signed_qty), old_signed)
        pnl = (fill_price - pos.entry) * reducing - commission
        pos.life_realized_pnl += pnl
    elif old_signed < 0 and fill_signed_qty > 0:
        reducing = min(abs(fill_signed_qty), abs(old_signed))
        pnl = (pos.entry - fill_price) * reducing - commission
        pos.life_realized_pnl += pnl

    new_signed = old_signed + fill_signed_qty
    had_reducing = reducing > 1e-12
    pos.max_abs_qty = max(pos.max_abs_qty, abs(old_signed), abs(new_signed))

    settlement: LifecycleSettlement | None = None
    residual: ShadowPosition | None = None

    if should_terminate_lifecycle(old_signed, new_signed, pos.max_abs_qty, had_reducing):
        action, cleared = settle_streak(pos.life_realized_pnl, nlv)
        settlement = LifecycleSettlement(
            symbol=symbol,
            life_pnl=pos.life_realized_pnl,
            streak_delta=action,
            cleared=cleared,
        )
        if abs(new_signed) > 1e-12:
            # 残余或反手 → 新生命周期
            side = Side.LONG if new_signed > 0 else Side.SHORT
            residual = ShadowPosition(
                symbol=symbol,
                side=side,
                qty=abs(new_signed),
                entry=fill_price,
                max_abs_qty=abs(new_signed),
            )
            return None, settlement, residual
        return None, settlement, None

    # 未终结：更新 qty / 可能加仓均价（简单：加仓用加权）
    if abs(new_signed) < 1e-12:
        return None, settlement, None

    side = Side.LONG if new_signed > 0 else Side.SHORT
    new_qty = abs(new_signed)
    # 同向加仓：加权成本
    if (old_signed > 0 and fill_signed_qty > 0) or (old_signed < 0 and fill_signed_qty < 0):
        total_cost = pos.entry * pos.qty + fill_price * abs(fill_signed_qty)
        pos.entry = total_cost / new_qty if new_qty else pos.entry
    pos.side = side
    pos.qty = new_qty
    pos.max_abs_qty = max(pos.max_abs_qty, new_qty)
    return pos, None, None


def apply_streak_counter(current: int, settlement: LifecycleSettlement) -> int:
    if settlement.cleared or settlement.streak_delta < 0:
        return 0
    if settlement.streak_delta > 0:
        return current + settlement.streak_delta
    return current
