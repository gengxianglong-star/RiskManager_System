"""TWS 原生结算引擎测试。"""
import sys
from unittest.mock import AsyncMock, Mock, patch

_mock_ib = Mock()
_mock_ib.Stock = Mock()
_mock_ib.MarketOrder = Mock()
_mock_ib.FlexReport = Mock()
_mock_ib.ExecutionFilter = Mock()
_mock_ib_util = Mock()
_mock_ib_util.formatIBDatetime = Mock(return_value="20260705-00:00:00")
sys.modules["ib_insync"] = _mock_ib
sys.modules["ib_insync.util"] = _mock_ib_util
sys.modules["ib_insync.decoder"] = Mock()
sys.modules["ib_insync.wrapper"] = Mock()
sys.modules["eventkit"] = Mock()

import pytest
from tws_settlement import TWSSettlement, run_tws_settlement


async def _seed_fill(db, exec_id, symbol, side, qty, price,
                     exec_time="2026-07-10T10:00:00", processed=0,
                     order_ref="", order_type="", aux_price=0.0):
    await db.execute(
        "INSERT OR IGNORE INTO tws_fills "
        "(exec_id, symbol, side, quantity, price, exec_time, "
        "order_ref, order_type, aux_price, processed) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (exec_id, symbol, side, qty, price, exec_time,
         order_ref, order_type, aux_price, processed),
    )


async def _seed_position(db, symbol, side, qty, entry_price,
                         status="OPEN", setup_tag="Breakout",
                         tranche_id="T1", current_stop=0.0, realized_pnl=0.0):
    cursor = await db.execute(
        "INSERT INTO shadow_ledger "
        "(symbol, tranche_id, side, quantity, entry_price, "
        "initial_stop, current_stop, status, setup_tag, realized_pnl) "
        "VALUES (?, ?, ?, ?, ?, 0.0, ?, ?, ?, ?)",
        (symbol, tranche_id, side, qty, entry_price,
         current_stop, status, setup_tag, realized_pnl),
    )
    return cursor.lastrowid


# ═══════════════════════════ Phase 1: 关仓核销 ═══════════════════════════

@pytest.mark.asyncio
async def test_fifo_close_long_full(async_db):
    """LONG 全额平仓：P&L = (exit - entry) * qty"""
    engine = TWSSettlement()
    async with async_db() as db:
        await _seed_position(db, "AAPL", "LONG", 100, 150.0)
        await _seed_fill(db, "exec_001", "AAPL", "SHORT", 100, 155.0)
        await db.commit()

    closed, pnl = await engine._phase1_fifo_settle()

    async with async_db() as db:
        cur = await db.execute(
            "SELECT status, realized_pnl, exit_price "
            "FROM shadow_ledger WHERE symbol='AAPL'")
        row = await cur.fetchone()
        assert row[0] == "CLOSED"
        assert abs(float(row[1]) - 500.0) < 0.01
        assert abs(float(row[2]) - 155.0) < 0.01
    assert closed >= 1


@pytest.mark.asyncio
async def test_fifo_close_short_full(async_db):
    """SHORT 全额平仓：P&L = (entry - exit) * qty"""
    engine = TWSSettlement()
    async with async_db() as db:
        await _seed_position(db, "TSLA", "SHORT", 200, 300.0)
        await _seed_fill(db, "exec_002", "TSLA", "LONG", 200, 280.0)
        await db.commit()

    closed, pnl = await engine._phase1_fifo_settle()

    async with async_db() as db:
        cur = await db.execute(
            "SELECT status, realized_pnl FROM shadow_ledger "
            "WHERE symbol='TSLA'")
        row = await cur.fetchone()
        assert row[0] == "CLOSED"
        assert abs(float(row[1]) - 4000.0) < 0.01


@pytest.mark.asyncio
async def test_fifo_close_partial(async_db):
    """部分平仓：剩余仓位保持 OPEN"""
    engine = TWSSettlement()
    async with async_db() as db:
        await _seed_position(db, "MSFT", "LONG", 100, 400.0)
        await _seed_fill(db, "exec_003", "MSFT", "SHORT", 40, 410.0)
        await db.commit()

    await engine._phase1_fifo_settle()

    async with async_db() as db:
        cur = await db.execute(
            "SELECT status, quantity, realized_pnl FROM shadow_ledger "
            "WHERE symbol='MSFT'")
        row = await cur.fetchone()
        assert row[0] == "OPEN"
        assert abs(float(row[1]) - 60.0) < 0.01
        assert abs(float(row[2]) - 400.0) < 0.01


@pytest.mark.asyncio
async def test_fifo_multi_tranche(async_db):
    """多批次 FIFO"""
    engine = TWSSettlement()
    async with async_db() as db:
        await _seed_position(db, "NVDA", "LONG", 50, 120.0, tranche_id="T1")
        await _seed_position(db, "NVDA", "LONG", 50, 125.0, tranche_id="T2")
        await _seed_fill(db, "exec_004", "NVDA", "SHORT", 70, 130.0)
        await db.commit()

    await engine._phase1_fifo_settle()

    async with async_db() as db:
        cur = await db.execute(
            "SELECT status, quantity, realized_pnl FROM shadow_ledger "
            "WHERE symbol='NVDA' ORDER BY create_time ASC")
        rows = await cur.fetchall()
        assert rows[0][0] == "CLOSED"
        assert abs(float(rows[0][2]) - 500.0) < 0.01
        assert rows[1][0] == "OPEN"
        assert abs(float(rows[1][1]) - 30.0) < 0.01
        assert abs(float(rows[1][2]) - 100.0) < 0.01


@pytest.mark.asyncio
async def test_oversell_reverse_position(async_db):
    """卖穿检测：平仓量 > 持仓量 → 反向开仓"""
    engine = TWSSettlement()
    async with async_db() as db:
        await _seed_position(db, "META", "LONG", 100, 500.0)
        await _seed_fill(db, "exec_005", "META", "SHORT", 150, 510.0)
        await db.commit()

    await engine._phase1_fifo_settle()

    async with async_db() as db:
        cur = await db.execute(
            "SELECT side, quantity, status FROM shadow_ledger "
            "WHERE symbol='META' ORDER BY create_time ASC")
        rows = await cur.fetchall()
        assert rows[0][2] == "CLOSED"
        assert rows[1][0] == "SHORT"
        assert abs(float(rows[1][1]) - 50.0) < 0.01
        assert rows[1][2] == "OPEN"


@pytest.mark.asyncio
async def test_orphan_auto_import(async_db):
    """无匹配 OPEN 仓位 → 兜底开仓（自动补建）"""
    engine = TWSSettlement()
    async with async_db() as db:
        # 只有 SHORT 仓位，来了一笔 SHORT fill（同向）→ 兜底加仓
        await _seed_position(db, "GOOGL", "SHORT", 50, 180.0)
        await _seed_fill(db, "exec_006", "GOOGL", "SHORT", 30, 185.0)
        await db.commit()

    closed, pnl = await engine._phase1_fifo_settle()
    # 同向 fill → 兜底开仓而非关仓
    assert closed == 0

    async with async_db() as db:
        cur = await db.execute(
            "SELECT COUNT(*) as cnt FROM shadow_ledger "
            "WHERE symbol='GOOGL' AND status='OPEN'")
        row = await cur.fetchone()
        assert row[0] == 2  # 原 1 个 OPEN + 新补建的 1 个


@pytest.mark.asyncio
async def test_open_fill_auto_import(async_db):
    """无 OPEN 仓位 + LONG fill → 兜底开仓，自动补建"""
    engine = TWSSettlement()
    async with async_db() as db:
        await _seed_fill(db, "exec_007", "AMZN", "LONG", 100, 200.0)
        await db.commit()

    closed, pnl = await engine._phase1_fifo_settle()
    assert closed == 0  # 开仓不入关仓计数

    async with async_db() as db:
        cur = await db.execute(
            "SELECT quantity, entry_price, setup_tag FROM shadow_ledger "
            "WHERE symbol='AMZN' AND status='OPEN'")
        row = await cur.fetchone()
        assert abs(float(row[0]) - 100.0) < 0.01
        assert abs(float(row[1]) - 200.0) < 0.01
        assert row[2] == "Breakout"  # LONG → Breakout


@pytest.mark.asyncio
async def test_dedup_processed_fills_skipped(async_db):
    """processed=1 的 fill 应被跳过（不被 Phase 1 处理）"""
    engine = TWSSettlement()
    async with async_db() as db:
        await _seed_position(db, "AAPL", "LONG", 100, 150.0)
        await _seed_fill(db, "exec_d1", "AAPL", "SHORT", 50, 155.0, processed=1)
        await _seed_fill(db, "exec_d2", "AAPL", "SHORT", 50, 160.0, processed=0)
        await db.commit()

    await engine._phase1_fifo_settle()

    async with async_db() as db:
        cur = await db.execute(
            "SELECT quantity FROM shadow_ledger "
            "WHERE symbol='AAPL' AND status='OPEN'")
        row = await cur.fetchone()
        assert abs(float(row[0]) - 50.0) < 0.01  # 只关了 processed=0 的


@pytest.mark.asyncio
async def test_consecutive_losses_updated_on_close(async_db):
    """关仓盈利 → 重置连亏计数器"""
    engine = TWSSettlement()
    async with async_db() as db:
        await _seed_position(db, "SPY", "LONG", 10, 500.0)
        await _seed_fill(db, "exec_cl_1", "SPY", "SHORT", 10, 510.0)
        await db.commit()

    await engine._phase1_fifo_settle()

    async with async_db() as db:
        cur = await db.execute(
            "SELECT value FROM system_state WHERE key='consecutive_losses'")
        row = await cur.fetchone()
        assert row[0] == "0"


# ═══════════════════════════ Phase 2 / Settle ═══════════════════════════

@pytest.mark.asyncio
async def test_settle_runs_phase1_and_phase2(async_db):
    """settle() 应执行 backfill + collect + phase1 + phase2（reconcile）"""
    engine = TWSSettlement()
    mock_ib = Mock()
    mock_ib.isConnected.return_value = True
    mock_ib.reqExecutionsAsync = AsyncMock(return_value=[])
    mock_ib.trades.return_value = []
    mock_ib.reqPositionsAsync = AsyncMock(return_value=[])

    async with async_db() as db:
        await _seed_position(db, "QQQ", "LONG", 10, 380.0)
        await _seed_fill(db, "exec_stl", "QQQ", "SHORT", 10, 390.0)
        await db.commit()

    with patch("reconciliation.reconcile_physical_positions",
               new_callable=AsyncMock) as mock_reconcile:
        result = await engine.settle(None, mock_ib)
        mock_reconcile.assert_called_once()
        assert result["closed_count"] >= 1


@pytest.mark.asyncio
async def test_run_tws_settlement_signature(async_db):
    """模块级入口签名兼容"""
    mock_ib = Mock()
    mock_ib.isConnected.return_value = True
    mock_ib.reqExecutionsAsync = AsyncMock(return_value=[])
    mock_ib.trades.return_value = []
    mock_ib.reqPositionsAsync = AsyncMock(return_value=[])

    result = await run_tws_settlement(None, mock_ib)
    assert isinstance(result, dict)
    assert "closed_count" in result


# ═══════════════════════════ 边界条件 ═══════════════════════════

@pytest.mark.asyncio
async def test_empty_fills_no_action(async_db):
    closed, pnl = await TWSSettlement()._phase1_fifo_settle()
    assert closed == 0
    assert pnl == 0.0


@pytest.mark.asyncio
async def test_tws_disconnected_graceful():
    engine = TWSSettlement()
    assert await engine.backfill_fills(None) == 0
    assert await engine.collect_fills(None) == 0


@pytest.mark.asyncio
async def test_settlement_idempotent(async_db):
    """同一 fill 被处理后不应再被处理"""
    engine = TWSSettlement()
    async with async_db() as db:
        await _seed_position(db, "AAPL", "LONG", 100, 150.0)
        await _seed_fill(db, "exec_idem", "AAPL", "SHORT", 100, 155.0)
        await db.commit()

    c1, _ = await engine._phase1_fifo_settle()
    assert c1 >= 1

    c2, _ = await engine._phase1_fifo_settle()
    assert c2 == 0
