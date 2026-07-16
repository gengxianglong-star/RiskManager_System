"""会话开机 / 重连权威同步。"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Protocol

from risk_core.constants import (
    AVG_COST_DRIFT_PCT,
    MAX_SYNC_GAP_DAYS,
    POSITION_SYNC_WARMUP_SEC,
)
from risk_core.core import RiskCore
from risk_core.models import OpenOrderView, ShadowPosition, Side
from risk_core.rth import is_rth
from risk_core import store as risk_store

logger = logging.getLogger("risk_core.sync")


class IBPositionLike(Protocol):
    @property
    def contract(self) -> Any: ...

    @property
    def position(self) -> float: ...

    @property
    def avgCost(self) -> float: ...


@dataclass
class SyncResult:
    ok: bool
    reason: str = ""
    gap_days: float | None = None


async def wait_positions_ready(
    fetch_positions,
    *,
    warmup_sec: float = POSITION_SYNC_WARMUP_SEC,
    had_local_open: bool = False,
    gross_position_value: float | None = None,
) -> list[Any]:
    """
    禁止 connect 瞬间读空列表当真理。
    fetch_positions: async callable → list of IB positions
    """
    await asyncio.sleep(warmup_sec)
    positions = await fetch_positions()
    if had_local_open and not positions:
        # 再等一轮，避免空仓幻觉
        logger.warning("local OPEN exists but IB positions empty; waiting again")
        await asyncio.sleep(warmup_sec)
        positions = await fetch_positions()
        if (
            not positions
            and gross_position_value is not None
            and gross_position_value > 1.0
        ):
            raise RuntimeError(
                "position sync inconsistent: empty list but GrossPositionValue>0"
            )
    return positions


def ib_positions_to_shadow(
    ib_positions: list[Any],
    existing: list[ShadowPosition],
    ledger_anchors: dict[str, tuple[float, float]] | None = None,
) -> tuple[list[ShadowPosition], list[str], bool]:
    """
    将 IB 持仓转为影子仓；检测 ticker merge 嫌疑与均价漂移。

    Returns:
        new_positions, drift_symbols, merge_suspected
    """
    existing_map = {p.symbol: p for p in existing}
    ledger_anchors = ledger_anchors or {}
    ib_map: dict[str, tuple[float, float]] = {}
    for p in ib_positions:
        c = p.contract
        if getattr(c, "secType", "STK") != "STK":
            continue
        if getattr(c, "currency", "USD") != "USD":
            continue
        sym = c.symbol
        qty = float(p.position)
        if abs(qty) < 1e-12:
            continue
        # IB avgCost 常为总成本/股（含乘数）；STK 即每股成本
        avg = float(p.avgCost)
        ib_map[sym] = (qty, avg)

    vanished = [s for s in existing_map if s not in ib_map]
    appeared = [s for s in ib_map if s not in existing_map]
    merge_suspected = bool(vanished) and bool(appeared)

    out: list[ShadowPosition] = []
    drift_syms: list[str] = []

    for sym, (qty, avg) in ib_map.items():
        side = Side.LONG if qty > 0 else Side.SHORT
        abs_qty = abs(qty)
        old = existing_map.get(sym)
        if old:
            entry = old.entry
            stop = old.stop
            life = old.life_realized_pnl
            max_q = max(old.max_abs_qty, abs_qty)
            if old.entry > 0 and abs(avg - old.entry) / old.entry > AVG_COST_DRIFT_PCT:
                drift_syms.append(sym)
            out.append(
                ShadowPosition(
                    symbol=sym,
                    side=side,
                    qty=abs_qty,
                    entry=entry,
                    stop=stop,
                    life_realized_pnl=life,
                    max_abs_qty=max_q,
                    avg_cost_ib=avg,
                )
            )
        else:
            ledger_entry, ledger_stop = ledger_anchors.get(sym, (0.0, 0.0))
            out.append(
                ShadowPosition(
                    symbol=sym,
                    side=side,
                    qty=abs_qty,
                    entry=ledger_entry if ledger_entry > 0 else avg,
                    stop=ledger_stop,
                    max_abs_qty=abs_qty,
                    avg_cost_ib=avg,
                )
            )
    return out, drift_syms, merge_suspected


async def session_boot_sync(
    core: RiskCore,
    *,
    fetch_positions,
    fetch_orders,
    fetch_account: dict[str, float] | None = None,
    warmup_sec: float = POSITION_SYNC_WARMUP_SEC,
    force_reset_streak_on_long_gap: bool = True,
) -> SyncResult:
    """
    开机/重连权威同步。完成前 core.sync_ok=False。

    fetch_positions / fetch_orders: async callables
    fetch_account: optional dict with net_liquidation, cushion, gross_position_value
    """
    core.mark_sync_blocked("syncing")
    core._in_flight.clear()
    core._roll_daily_opens()  # 跨日：日开仓 + 连亏归零（随后 persist）

    acct = fetch_account or {}
    nlv = float(acct.get("net_liquidation") or acct.get("NetLiquidation") or 0)
    cushion = float(acct.get("cushion") or acct.get("Cushion") or core._cushion)
    gpv = acct.get("gross_position_value") or acct.get("GrossPositionValue")

    # 长假门禁
    gap = core.check_sync_gap_days()
    if gap is not None and gap > MAX_SYNC_GAP_DAYS:
        if force_reset_streak_on_long_gap:
            await core.reset_streak()
            core._last_sync_at = ""  # will rewrite at end
            logger.warning(
                "sync gap %.1f days > %s; streak reset", gap, MAX_SYNC_GAP_DAYS
            )
        else:
            core.mark_sync_blocked(f"sync_gap_{gap:.1f}d")
            return SyncResult(False, f"gap {gap:.1f}d requires manual ack", gap)

    had_local = any(p.qty > 0 for p in core.positions)
    try:
        ib_pos = await wait_positions_ready(
            fetch_positions,
            warmup_sec=warmup_sec,
            had_local_open=had_local,
            gross_position_value=float(gpv) if gpv is not None else None,
        )
    except RuntimeError as e:
        core.mark_sync_blocked(str(e))
        return SyncResult(False, str(e), gap)

    ledger_anchors: dict[str, tuple[float, float]] = {}
    try:
        db = await core._connect()
        try:
            ledger_anchors = await risk_store.load_open_ledger_anchors(db)
        finally:
            await db.close()
    except Exception as e:
        logger.warning("load ledger anchors for sync failed: %s", e)

    new_pos, drift_syms, merge_suspected = ib_positions_to_shadow(
        ib_pos, core.positions, ledger_anchors
    )

    if merge_suspected:
        core.positions = new_pos  # keep IB truth for qty but block
        core.mark_sync_blocked("ticker_merge_required")
        await core.persist()
        return SyncResult(False, "ticker merge suspected; call merge_lifecycle", gap)

    if drift_syms:
        core.positions = new_pos
        core.mark_sync_blocked(f"avg_cost_drift:{','.join(drift_syms)}")
        await core.persist()
        return SyncResult(False, f"avg cost drift: {drift_syms}", gap)

    # NLV / HWM
    if nlv > 0:
        in_rth = is_rth()
        core.update_nlv(nlv, force_hwm=False)
        if in_rth and nlv > core._hwm:
            core._hwm = nlv
        if core._hwm <= 0:
            core._hwm = nlv
        if core._rth_close_nlv <= 0:
            core._rth_close_nlv = nlv
        if not in_rth:
            # 盘外不抬 HWM；保持 rth_close
            pass
        else:
            # RTH 内可持续更新 close 候选
            pass

    core.update_cushion(cushion if cushion > 0 else core._cushion)
    core.positions = new_pos

    orders = await fetch_orders()
    core.update_orders(list(orders))

    core.mark_sync_ok()
    core.set_connected(True)
    await core.persist()
    logger.info(
        "session_boot_sync ok nlv=%.2f hwm=%.2f positions=%d orders=%d",
        core._last_nlv,
        core._hwm,
        len(core.positions),
        len(core._orders),
    )
    return SyncResult(True, "ok", gap)
