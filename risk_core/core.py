"""RiskCore：防弹定稿主引擎。"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

import aiosqlite

from risk_core.constants import (
    ALLOWED_CURRENCY,
    ALLOWED_ENTRY_ORDER_TYPES,
    ALLOWED_SEC_TYPE,
    AVG_COST_DRIFT_PCT,
    IN_FLIGHT_TTL_SEC,
    MAX_DAILY_TRADES,
    MAX_OVERNIGHT_RISK_PCT,
    MAX_SYNC_GAP_DAYS,
    MIN_CUSHION,
    MIN_ENTRY_PRICE,
    RISK_MAX_DRAWDOWN_GREEN,
    RISK_MAX_DRAWDOWN_YELLOW,
    RISK_PCT_PER_TRADE,
    RTH_TZ,
)
from risk_core.exposure import compute_exposure, estimate_new_entry_risk
from risk_core.lifecycle import apply_qty_delta, apply_streak_counter
from risk_core.models import (
    CanOpenRequest,
    CanOpenResult,
    InFlightIntent,
    OpenOrderView,
    RiskState,
    ShadowPosition,
    Side,
)
from risk_core.rth import equity_for_cap, is_rth
from risk_core import store
from risk_core.timeutil import monotonic, parse_iso_utc, to_ib_exec_time_str, utc_now

logger = logging.getLogger("risk_core")


class RiskCore:
    """纯本地风控状态机，供下单工具调用。"""

    def __init__(
        self,
        db_path: str,
        is_connected: Callable[[], bool] | None = None,
    ) -> None:
        self.db_path = db_path
        self._is_connected = is_connected or (lambda: False)
        self._lock = asyncio.Lock()
        self._in_flight: dict[str, InFlightIntent] = {}
        self._orders: list[OpenOrderView] = []
        self._positions: list[ShadowPosition] = []
        self._sync_ok = False
        self._sync_block_reason = ""
        self._hwm = 0.0
        self._last_nlv = 0.0
        self._rth_close_nlv = 0.0
        self._consecutive_losses = 0
        self._cushion = 1.0
        self._daily_opens = 0
        self._daily_opens_date = ""
        self._last_sync_at = ""
        self._connected_flag = False

    # ── DB helpers ──

    async def _connect(self) -> aiosqlite.Connection:
        db = await aiosqlite.connect(self.db_path)
        await db.execute("PRAGMA journal_mode=WAL;")
        await store.ensure_risk_core_schema(db)
        return db

    async def initialize(self) -> None:
        async with self._lock:
            db = await self._connect()
            try:
                await self._load_from_db(db)
            finally:
                await db.close()

    async def _load_from_db(self, db: aiosqlite.Connection) -> None:
        m = await store.get_state_map(db)
        self._hwm = float(m.get("hwm") or 0)
        self._last_nlv = float(m.get("last_nlv") or 0)
        self._rth_close_nlv = float(m.get("rth_close_nlv") or 0)
        self._consecutive_losses = int(float(m.get("consecutive_losses") or 0))
        self._cushion = float(m.get("last_cushion") or 1)
        self._daily_opens = int(float(m.get("daily_opens") or 0))
        self._daily_opens_date = m.get("daily_opens_date") or ""
        self._last_sync_at = m.get("last_sync_at") or ""
        self._sync_block_reason = m.get("sync_block_reason") or ""
        self._positions = await store.load_positions(db)
        self._in_flight.clear()

    async def _persist_baselines(self, db: aiosqlite.Connection) -> None:
        await store.set_states(
            db,
            {
                "hwm": self._hwm,
                "last_nlv": self._last_nlv,
                "rth_close_nlv": self._rth_close_nlv,
                "consecutive_losses": self._consecutive_losses,
                "last_sync_at": self._last_sync_at,
                "sync_block_reason": self._sync_block_reason,
                "last_cushion": self._cushion,
                "daily_opens": self._daily_opens,
                "daily_opens_date": self._daily_opens_date,
            },
        )

    async def _commit_state(self, db: aiosqlite.Connection) -> None:
        await store.replace_all_positions(db, self._positions)
        await self._persist_baselines(db)
        await db.commit()

    # ── In-flight TTL ──

    def _purge_expired_in_flight(self) -> None:
        now = monotonic()
        expired = [k for k, v in self._in_flight.items() if v.expires_at <= now]
        for k in expired:
            logger.info("in_flight TTL expired: %s", k)
            del self._in_flight[k]

    def _in_flight_sum(self) -> float:
        self._purge_expired_in_flight()
        return sum(v.estimated_risk for v in self._in_flight.values())

    def release_in_flight(self, intent_id: str) -> None:
        self._in_flight.pop(intent_id, None)

    def on_open_order_ack(self, intent_id: str) -> None:
        """TWS 已反映 pending → 释放 in_flight。"""
        self.release_in_flight(intent_id)

    # ── 基准 / 灯 ──

    def _drawdown(self, in_rth: bool) -> float:
        hwm = self._hwm
        if hwm <= 0:
            return 0.0
        if in_rth:
            nlv = self._last_nlv
        else:
            nlv = self._rth_close_nlv if self._rth_close_nlv > 0 else self._last_nlv
        return max(0.0, (hwm - nlv) / hwm)

    def _risk_light(self, dd: float) -> tuple[str, float]:
        streak = self._consecutive_losses
        equity = self._last_nlv if self._last_nlv > 0 else self._rth_close_nlv
        if dd >= RISK_MAX_DRAWDOWN_YELLOW or streak >= 5:
            return "🔴", 0.0
        if dd >= RISK_MAX_DRAWDOWN_GREEN or streak >= 3:
            return "🟡", equity * (RISK_PCT_PER_TRADE / 2)
        return "🟢", equity * RISK_PCT_PER_TRADE

    def _roll_daily_opens(self) -> bool:
        """跨美东交易日：日开仓计数与连亏一并归零。返回是否发生日切。"""
        today = utc_now().astimezone(ZoneInfo(RTH_TZ)).date().isoformat()
        if self._daily_opens_date != today:
            self._daily_opens = 0
            self._daily_opens_date = today
            self._consecutive_losses = 0
            return True
        return False

    # ── Public API ──

    def set_connected(self, connected: bool) -> None:
        self._connected_flag = connected
        if not connected:
            self._sync_ok = False
            self._sync_block_reason = "disconnected"

    async def get_state(self) -> RiskState:
        async with self._lock:
            await self._roll_and_persist_if_needed_locked()
            self._purge_expired_in_flight()
            connected = bool(self._is_connected()) or self._connected_flag
            in_rth = is_rth()
            dd = self._drawdown(in_rth)
            light, budget = self._risk_light(dd)
            pos_r, pend_r, _ = compute_exposure(self._positions, self._orders)
            infl = self._in_flight_sum()
            return RiskState(
                nlv=self._last_nlv,
                hwm=self._hwm,
                rth_close_nlv=self._rth_close_nlv,
                drawdown=dd,
                consecutive_losses=self._consecutive_losses,
                position_risk=pos_r,
                pending_risk=pend_r,
                in_flight_risk=infl,
                total_risk=pos_r + pend_r + infl,
                cushion=self._cushion,
                is_rth=in_rth,
                risk_light=light,
                daily_opens=self._daily_opens,
                sync_ok=self._sync_ok,
                sync_block_reason=self._sync_block_reason,
                connected=connected,
            )

    async def _roll_and_persist_if_needed_locked(self) -> None:
        """调用方须已持有 self._lock。"""
        if not self._roll_daily_opens():
            return
        db = await self._connect()
        try:
            await self._persist_baselines(db)
            await db.commit()
        finally:
            await db.close()

    async def can_open(self, req: CanOpenRequest) -> CanOpenResult:
        async with self._lock:
            await self._roll_and_persist_if_needed_locked()
            self._purge_expired_in_flight()

            # 0) 物理连接
            connected = bool(self._is_connected()) or self._connected_flag
            if not connected:
                return CanOpenResult(False, "TWS disconnected")

            if not self._sync_ok:
                return CanOpenResult(
                    False, f"sync blocked: {self._sync_block_reason or 'not ready'}"
                )

            # 资产隔离
            if (req.currency or "").upper() != ALLOWED_CURRENCY:
                return CanOpenResult(False, f"currency must be {ALLOWED_CURRENCY}")
            if (req.sec_type or "").upper() != ALLOWED_SEC_TYPE:
                return CanOpenResult(False, f"secType must be {ALLOWED_SEC_TYPE}")

            # 禁 MKT；允许 LMT / STP LMT / STP（须有价格锚点）
            ot = (req.order_type or "").upper().replace("_", " ")
            if ot == "MKT" or ot not in ALLOWED_ENTRY_ORDER_TYPES:
                return CanOpenResult(False, "entry must be LMT/STP/STP LMT with price")
            if req.entry_px <= 0:
                return CanOpenResult(False, "entry_px required")

            # 止损方向：多单止损须低于入场，空单须高于入场
            if req.stop_px > 0:
                if req.side == Side.LONG and req.stop_px >= req.entry_px:
                    return CanOpenResult(False, "stop on wrong side of long entry")
                if req.side == Side.SHORT and req.stop_px <= req.entry_px:
                    return CanOpenResult(False, "stop on wrong side of short entry")

            # 低价无止损
            if req.stop_px <= 0 and req.entry_px < MIN_ENTRY_PRICE:
                return CanOpenResult(False, f"entry < ${MIN_ENTRY_PRICE} requires stop")

            if self._cushion < MIN_CUSHION:
                return CanOpenResult(False, f"cushion {self._cushion:.2%} < {MIN_CUSHION:.0%}")

            if self._daily_opens >= MAX_DAILY_TRADES:
                return CanOpenResult(
                    False, f"daily opens {self._daily_opens}/{MAX_DAILY_TRADES}"
                )

            in_rth = is_rth()
            dd = self._drawdown(in_rth)
            light, budget = self._risk_light(dd)
            if budget <= 0:
                return CanOpenResult(False, f"risk light {light}", risk_light=light)

            est = estimate_new_entry_risk(req.entry_px, req.qty, req.stop_px)
            pos_r, pend_r, _ = compute_exposure(self._positions, self._orders)
            infl = self._in_flight_sum()
            total = pos_r + pend_r + infl
            cap_eq = equity_for_cap(self._last_nlv, self._rth_close_nlv, in_rth)
            cap = cap_eq * MAX_OVERNIGHT_RISK_PCT
            if total + est > cap + 1e-9:
                return CanOpenResult(
                    False,
                    f"total_risk {total + est:.2f} > cap {cap:.2f}",
                    estimated_risk=est,
                    risk_light=light,
                )

            intent_id = req.intent_id or str(uuid.uuid4())
            self._in_flight[intent_id] = InFlightIntent(
                intent_id=intent_id,
                estimated_risk=est,
                expires_at=monotonic() + IN_FLIGHT_TTL_SEC,
                symbol=req.symbol,
            )
            return CanOpenResult(
                True,
                "ok",
                intent_id=intent_id,
                estimated_risk=est,
                risk_budget=budget,
                risk_light=light,
            )

    async def adjust_hwm(self, delta: float) -> None:
        """出入金：平行移动三基准。"""
        async with self._lock:
            self._hwm += delta
            if self._rth_close_nlv > 0 or delta != 0:
                self._rth_close_nlv = max(0.0, self._rth_close_nlv + delta)
            self._last_nlv = max(0.0, self._last_nlv + delta)
            if self._hwm <= 0:
                self._hwm = 1e-6
                logger.warning("hwm clamped to epsilon after adjust_hwm(%s)", delta)
            db = await self._connect()
            try:
                await self._persist_baselines(db)
                await db.commit()
            finally:
                await db.close()

    async def reset_streak(self) -> None:
        async with self._lock:
            self._consecutive_losses = 0
            db = await self._connect()
            try:
                await store.set_state(db, "consecutive_losses", "0")
                await db.commit()
            finally:
                await db.close()

    async def acknowledge_corp_action(
        self,
        symbol: str,
        entry: float,
        stop: float,
        *,
        ratio: float | None = None,
    ) -> None:
        async with self._lock:
            for p in self._positions:
                if p.symbol != symbol:
                    continue
                if ratio is not None and ratio > 0 and abs(ratio - 1.0) > 1e-12:
                    p.qty *= ratio
                    p.max_abs_qty = max(p.max_abs_qty * ratio, p.qty)
                p.entry = entry
                p.stop = stop
                p.avg_cost_ib = entry
            if self._sync_block_reason.startswith("avg_cost_drift"):
                self._sync_block_reason = ""
                self._sync_ok = True
            db = await self._connect()
            try:
                await self._commit_state(db)
            finally:
                await db.close()

    async def merge_lifecycle(self, old_symbol: str, new_symbol: str) -> None:
        async with self._lock:
            old = next((p for p in self._positions if p.symbol == old_symbol), None)
            if old is None:
                raise KeyError(old_symbol)
            merged = ShadowPosition(
                symbol=new_symbol,
                side=old.side,
                qty=old.qty,
                entry=old.entry,
                stop=old.stop,
                life_realized_pnl=old.life_realized_pnl,
                max_abs_qty=old.max_abs_qty,
                avg_cost_ib=old.avg_cost_ib,
            )
            self._positions = [
                p for p in self._positions if p.symbol not in (old_symbol, new_symbol)
            ]
            self._positions.append(merged)
            if self._sync_block_reason.startswith("ticker_merge"):
                self._sync_block_reason = ""
                self._sync_ok = True
            db = await self._connect()
            try:
                await self._commit_state(db)
            finally:
                await db.close()

    def update_orders(self, orders: list[OpenOrderView]) -> None:
        self._orders = list(orders)

    def update_nlv(self, nlv: float, *, force_hwm: bool = False) -> None:
        """会话内 NLV 推送。仅 RTH 抬高 HWM。"""
        self._last_nlv = nlv
        if (force_hwm or is_rth()) and nlv > self._hwm:
            self._hwm = nlv
        # RTH 收盘快照：调用方在接近 close 时 snapshot_rth_close()
        if self._hwm <= 0 and nlv > 0:
            self._hwm = nlv

    def snapshot_rth_close(self) -> None:
        if self._last_nlv > 0:
            self._rth_close_nlv = self._last_nlv

    def update_cushion(self, cushion: float) -> None:
        self._cushion = cushion

    async def apply_fill(
        self,
        *,
        exec_id: str,
        symbol: str,
        signed_qty: float,
        price: float,
        commission: float = 0.0,
    ) -> None:
        """成交驱动生命周期（幂等 exec_id；fill+持仓+状态同一事务）。"""
        async with self._lock:
            db = await self._connect()
            try:
                if not await store.mark_fill(db, exec_id):
                    return
                pos = next((p for p in self._positions if p.symbol == symbol), None)
                updated, settlement, residual = apply_qty_delta(
                    pos, symbol, signed_qty, price, commission, self._last_nlv
                )
                self._positions = [p for p in self._positions if p.symbol != symbol]
                if updated:
                    self._positions.append(updated)
                if residual:
                    self._positions.append(residual)
                    # 反手新开计今日开仓
                    self._roll_daily_opens()
                    self._daily_opens += 1
                if settlement:
                    self._consecutive_losses = apply_streak_counter(
                        self._consecutive_losses, settlement
                    )
                # 从 0 开仓
                if pos is None and updated is not None:
                    self._roll_daily_opens()
                    self._daily_opens += 1
                await self._commit_state(db)
            finally:
                await db.close()

    def last_sync_at_ib_str(self) -> str | None:
        dt = parse_iso_utc(self._last_sync_at)
        if dt is None:
            return None
        return to_ib_exec_time_str(dt)

    def check_sync_gap_days(self) -> float | None:
        dt = parse_iso_utc(self._last_sync_at)
        if dt is None:
            return None
        return (utc_now() - dt).total_seconds() / 86400.0

    def mark_sync_blocked(self, reason: str) -> None:
        self._sync_ok = False
        self._sync_block_reason = reason

    def mark_sync_ok(self) -> None:
        self._sync_ok = True
        self._sync_block_reason = ""
        self._last_sync_at = utc_now().isoformat()

    async def persist(self) -> None:
        async with self._lock:
            db = await self._connect()
            try:
                await self._commit_state(db)
            finally:
                await db.close()
    # 供 sync 模块写入
    @property
    def positions(self) -> list[ShadowPosition]:
        return self._positions

    @positions.setter
    def positions(self, value: list[ShadowPosition]) -> None:
        self._positions = value

    def check_avg_cost_drift(self, symbol: str, ib_avg_cost: float) -> bool:
        """True if drift exceeds threshold (should block)."""
        pos = next((p for p in self._positions if p.symbol == symbol), None)
        if pos is None or pos.entry <= 0 or ib_avg_cost <= 0:
            return False
        drift = abs(ib_avg_cost - pos.entry) / pos.entry
        return drift > AVG_COST_DRIFT_PCT
