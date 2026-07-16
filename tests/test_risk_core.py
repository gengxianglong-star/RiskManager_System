"""RiskCore 定稿契约单测。"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from risk_core.constants import (
    MATERIAL_DEPLETION_PCT,
    MIN_ENTRY_PRICE,
    MIN_RISK_PER_SHARE,
)
from risk_core.core import RiskCore
from risk_core.exposure import (
    compute_exposure,
    covered_risk,
    naked_risk,
)
from risk_core.lifecycle import apply_qty_delta, apply_streak_counter, settle_streak
from risk_core.models import (
    CanOpenRequest,
    LifecycleSettlement,
    OpenOrderView,
    ShadowPosition,
    Side,
)
from risk_core.sync import ib_positions_to_shadow, session_boot_sync
from risk_core import store as risk_store
from risk_core.timeutil import to_ib_exec_time_str


@pytest.fixture
async def core(tmp_path):
    db = str(tmp_path / "risk_core_test.db")
    c = RiskCore(db, is_connected=lambda: True)
    await c.initialize()
    c.set_connected(True)
    c.mark_sync_ok()
    c._last_nlv = 100_000.0
    c._hwm = 100_000.0
    c._rth_close_nlv = 100_000.0
    c._cushion = 0.5
    return c


def test_protective_stop_long_zero_risk():
    assert covered_risk(100, 110, 100, Side.LONG) == 0.0
    pos = [ShadowPosition("TSLA", Side.LONG, 100, 100, stop=110)]
    orders = [
        OpenOrderView("TSLA", 1, 0, "SELL", "STP", 100, aux_price=110),
    ]
    pr, pend, _ = compute_exposure(pos, orders)
    assert pr == 0.0


def test_naked_penny_stock_floor():
    r = naked_risk(2.0, 10_000)
    assert r == pytest.approx(MIN_RISK_PER_SHARE * 10_000)


def test_stop_overflow_to_pending():
    pos = [ShadowPosition("TSLA", Side.LONG, 1000, 100, stop=95)]
    orders = [
        OpenOrderView("TSLA", 1, 0, "SELL", "STP", 1500, aux_price=95),
    ]
    pr, pend, _ = compute_exposure(pos, orders)
    assert pr == pytest.approx(5000.0)
    assert pend == pytest.approx(max(0.03 * 95, MIN_RISK_PER_SHARE) * 500)


def test_bracket_child_not_pending():
    orders = [
        OpenOrderView("AAPL", 10, 0, "BUY", "LMT", 100, lmt_price=150),
        OpenOrderView("AAPL", 11, 10, "SELL", "STP", 100, aux_price=145),
    ]
    pr, pend, _ = compute_exposure([], orders)
    assert pr == 0.0
    assert pend == pytest.approx(150 * 100 * (5 / 150))


def test_stp_entry_pending_uses_aux_price():
    """STP 开仓父单 lmt_price=0，须用 aux_price 计 pending risk。"""
    orders = [
        OpenOrderView("NVDA", 20, 0, "BUY", "STP", 50, aux_price=100),
        OpenOrderView("NVDA", 21, 20, "SELL", "STP", 50, aux_price=95),
    ]
    pr, pend, _ = compute_exposure([], orders)
    assert pr == 0.0
    assert pend == pytest.approx(100 * 50 * (5 / 100))


def test_protective_stp_on_position_not_pending_entry():
    """已有多仓的独立 SELL STP 是防守单，不计开仓 pending。"""
    pos = [ShadowPosition("AAPL", Side.LONG, 100, 150, stop=145)]
    orders = [
        OpenOrderView("AAPL", 1, 0, "SELL", "STP", 100, aux_price=145),
    ]
    pr, pend, _ = compute_exposure(pos, orders)
    assert pend == pytest.approx(0.0)
    assert pr == pytest.approx(500.0)


@pytest.mark.asyncio
async def test_apply_fill_idempotent_and_atomic(core):
    await core.apply_fill(
        exec_id="e1", symbol="AAPL", signed_qty=10, price=100.0
    )
    assert len(core.positions) == 1
    assert core.positions[0].qty == pytest.approx(10)
    # 重复 exec_id 不得再加仓
    await core.apply_fill(
        exec_id="e1", symbol="AAPL", signed_qty=10, price=100.0
    )
    assert len(core.positions) == 1
    assert core.positions[0].qty == pytest.approx(10)


@pytest.mark.asyncio
async def test_daily_roll_resets_streak(core):
    core._consecutive_losses = 3
    core._daily_opens = 2
    core._daily_opens_date = "2000-01-01"
    rolled = core._roll_daily_opens()
    assert rolled
    assert core._consecutive_losses == 0
    assert core._daily_opens == 0


@pytest.mark.asyncio
async def test_acknowledge_corp_action_scales_qty(core):
    core.positions = [
        ShadowPosition("AAPL", Side.LONG, 100, 200, stop=180, avg_cost_ib=200)
    ]
    await core.acknowledge_corp_action("AAPL", entry=100, stop=90, ratio=2.0)
    p = core.positions[0]
    assert p.qty == pytest.approx(200)
    assert p.entry == pytest.approx(100)
    assert p.stop == pytest.approx(90)
    assert p.avg_cost_ib == pytest.approx(100)
    action, cleared = settle_streak(-2.5, nlv=100_000)
    assert action == 0 and not cleared
    # nlv*0.0005=50 → 需严格小于 -50
    action, cleared = settle_streak(-50.01, nlv=100_000)
    assert action == 1
    action, cleared = settle_streak(10, nlv=100_000)
    assert cleared


def test_material_depletion_lifecycle():
    pos = ShadowPosition("X", Side.LONG, 1000, 50, max_abs_qty=1000)
    updated, settlement, residual = apply_qty_delta(
        pos, "X", -999, 40.0, commission=0, nlv=100_000
    )
    assert settlement is not None
    assert settlement.life_pnl < 0
    assert residual is not None
    assert residual.qty == pytest.approx(1.0)
    assert abs(1) < 1000 * MATERIAL_DEPLETION_PCT


def test_zero_cross_reversal():
    pos = ShadowPosition("TSLA", Side.LONG, 1000, 100, max_abs_qty=1000)
    updated, settlement, residual = apply_qty_delta(
        pos, "TSLA", -2000, 90.0, nlv=100_000
    )
    assert updated is None
    assert settlement is not None
    assert residual is not None
    assert residual.side == Side.SHORT
    assert residual.qty == pytest.approx(1000)


@pytest.mark.asyncio
async def test_adjust_hwm_parallel_baselines(core):
    await core.adjust_hwm(20_000)
    assert core._hwm == pytest.approx(120_000)
    assert core._rth_close_nlv == pytest.approx(120_000)
    assert core._last_nlv == pytest.approx(120_000)
    dd = (core._hwm - core._rth_close_nlv) / core._hwm
    assert dd == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_can_open_rejects_disconnected(tmp_path):
    db = str(tmp_path / "d.db")
    c = RiskCore(db, is_connected=lambda: False)
    await c.initialize()
    c.mark_sync_ok()
    r = await c.can_open(
        CanOpenRequest("AAPL", 10, 100, stop_px=95, order_type="LMT")
    )
    assert not r.allowed
    assert "disconnect" in r.reason.lower()


@pytest.mark.asyncio
async def test_can_open_rejects_opt_and_mkt(core):
    r = await core.can_open(
        CanOpenRequest("AAPL", 1, 5.0, sec_type="OPT", order_type="LMT")
    )
    assert not r.allowed
    r = await core.can_open(
        CanOpenRequest("AAPL", 10, 100, order_type="MKT")
    )
    assert not r.allowed
    r = await core.can_open(
        CanOpenRequest("PENNY", 1000, 2.0, stop_px=0, order_type="LMT")
    )
    assert not r.allowed
    assert "stop" in r.reason.lower() or str(int(MIN_ENTRY_PRICE)) in r.reason


@pytest.mark.asyncio
async def test_can_open_cushion(core):
    core._cushion = 0.05
    r = await core.can_open(
        CanOpenRequest("AAPL", 10, 100, stop_px=97, order_type="LMT")
    )
    assert not r.allowed
    assert "cushion" in r.reason.lower()


@pytest.mark.asyncio
async def test_in_flight_blocks_second_click(core):
    r1 = await core.can_open(
        CanOpenRequest("AAPL", 400, 100, stop_px=97, order_type="LMT", intent_id="a")
    )
    assert r1.allowed
    r2 = await core.can_open(
        CanOpenRequest("AAPL", 400, 100, stop_px=97, order_type="LMT", intent_id="b")
    )
    assert not r2.allowed
    core.release_in_flight("a")
    r3 = await core.can_open(
        CanOpenRequest("AAPL", 400, 100, stop_px=97, order_type="LMT", intent_id="c")
    )
    assert r3.allowed


@pytest.mark.asyncio
async def test_in_flight_ttl_expires(core):
    r1 = await core.can_open(
        CanOpenRequest("AAPL", 400, 100, stop_px=97, order_type="LMT", intent_id="x")
    )
    assert r1.allowed
    core._in_flight["x"].expires_at = time.monotonic() - 1
    r2 = await core.can_open(
        CanOpenRequest("AAPL", 400, 100, stop_px=97, order_type="LMT", intent_id="y")
    )
    assert r2.allowed


def test_ib_exec_time_format():
    dt = datetime(2026, 7, 10, 12, 0, 0, tzinfo=timezone.utc)
    s = to_ib_exec_time_str(dt)
    assert len(s) == 17
    assert s[8] == "-"
    assert "Z" not in s
    assert "+" not in s


@pytest.mark.asyncio
async def test_merge_lifecycle(core):
    core.positions = [
        ShadowPosition(
            "DWAC", Side.LONG, 100, 20, stop=18, life_realized_pnl=-50, max_abs_qty=100
        )
    ]
    await core.merge_lifecycle("DWAC", "DJT")
    assert len(core.positions) == 1
    assert core.positions[0].symbol == "DJT"
    assert core.positions[0].life_realized_pnl == -50


@pytest.mark.asyncio
async def test_session_boot_warmup_and_drift(tmp_path):
    db = str(tmp_path / "sync.db")
    c = RiskCore(db, is_connected=lambda: True)
    await c.initialize()
    c.positions = [ShadowPosition("AAPL", Side.LONG, 10, 100, stop=95)]

    @dataclass
    class C:
        symbol: str = "AAPL"
        secType: str = "STK"
        currency: str = "USD"

    @dataclass
    class P:
        contract: C
        position: float
        avgCost: float

    async def fetch_pos():
        return [P(C(), 10, 150.0)]

    async def fetch_ord():
        return []

    result = await session_boot_sync(
        c,
        fetch_positions=fetch_pos,
        fetch_orders=fetch_ord,
        fetch_account={"net_liquidation": 100000, "cushion": 0.5},
        warmup_sec=0.01,
    )
    assert not result.ok
    assert "drift" in result.reason.lower()


def test_streak_apply():
    s = LifecycleSettlement("X", -100, 1, False)
    assert apply_streak_counter(2, s) == 3
    s2 = LifecycleSettlement("X", 50, -1, True)
    assert apply_streak_counter(3, s2) == 0


@pytest.mark.asyncio
async def test_sync_stops_from_ledger_fills_missing_stop(tmp_path):
    db_path = str(tmp_path / "t.db")
    async with __import__("aiosqlite").connect(db_path) as db:
        await db.execute(
            """
            CREATE TABLE shadow_ledger (
                symbol TEXT, side TEXT, quantity REAL, entry_price REAL,
                current_stop REAL, initial_stop REAL, status TEXT
            )
            """
        )
        await risk_store.ensure_risk_core_schema(db)
        await db.execute(
            "INSERT INTO shadow_ledger "
            "(symbol, side, quantity, entry_price, current_stop, initial_stop, status) "
            "VALUES ('TSLL', 'LONG', 1126, 12.297, 12.07, 12.07, 'OPEN')"
        )
        await db.execute(
            "INSERT INTO shadow_positions "
            "(symbol, side, qty, entry, stop, avg_cost_ib) "
            "VALUES ('TSLL', 'LONG', 1126, 12.297, 0, 12.297)"
        )
        await db.commit()

        changed = await risk_store.sync_stops_from_ledger(db)
        await db.commit()
        assert changed == ["TSLL"]

        cur = await db.execute(
            "SELECT stop FROM shadow_positions WHERE symbol='TSLL'"
        )
        row = await cur.fetchone()
        assert float(row[0]) == pytest.approx(12.07)


@pytest.mark.asyncio
async def test_sync_stops_from_ledger_skips_when_already_match(tmp_path):
    db_path = str(tmp_path / "t.db")
    async with __import__("aiosqlite").connect(db_path) as db:
        await db.execute(
            """
            CREATE TABLE shadow_ledger (
                symbol TEXT, side TEXT, quantity REAL, entry_price REAL,
                current_stop REAL, status TEXT
            )
            """
        )
        await risk_store.ensure_risk_core_schema(db)
        await db.execute(
            "INSERT INTO shadow_ledger "
            "(symbol, side, quantity, entry_price, current_stop, status) "
            "VALUES ('TSLL', 'LONG', 100, 10, 9.5, 'OPEN')"
        )
        await db.execute(
            "INSERT INTO shadow_positions "
            "(symbol, side, qty, entry, stop) VALUES ('TSLL', 'LONG', 100, 10, 9.5)"
        )
        await db.commit()

        changed = await risk_store.sync_stops_from_ledger(db)
        assert changed == []


def test_ib_positions_to_shadow_uses_ledger_stop_for_new_symbol():
    @dataclass
    class C:
        symbol: str = "TSLL"
        secType: str = "STK"
        currency: str = "USD"

    @dataclass
    class P:
        contract: C
        position: float
        avgCost: float

    anchors = {"TSLL": (12.297, 12.07)}
    out, drift, merge = ib_positions_to_shadow(
        [P(C(), 1126, 12.297)], [], anchors
    )
    assert not drift and not merge
    assert len(out) == 1
    assert out[0].stop == pytest.approx(12.07)
    assert out[0].entry == pytest.approx(12.297)
