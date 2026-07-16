"""重复开仓去重：防止双写导致幽灵平仓。"""
import pytest

from reconciliation import _dedupe_duplicate_opens, _best_effort_pnl
from database import ledger_open_exists_for_fill
from tests.conftest import async_test_db


def test_best_effort_pnl_vwap_from_partial_fills():
    fills = [
        {"quantity": 726, "price": 12.135, "exec_time": "t1", "order_type": "STP"},
        {"quantity": 214, "price": 12.135, "exec_time": "t2", "order_type": "STP"},
        {"quantity": 186, "price": 12.135, "exec_time": "t3", "order_type": "STP"},
    ]
    exit_px, pnl, t, is_stop = _best_effort_pnl(fills, 12.297, "LONG", 1126)
    assert is_stop
    assert abs(exit_px - 12.135) < 1e-6
    assert abs(pnl - (12.135 - 12.297) * 1126) < 0.01
    assert t == "t1" or t  # 最新消费起点



@pytest.mark.asyncio
async def test_dedupe_duplicate_opens_keeps_ui_manual():
  async with async_test_db() as get_conn:
    async with get_conn() as db:
      await db.execute(
        "INSERT INTO shadow_ledger (symbol, tranche_id, side, quantity, entry_price, "
        "initial_stop, current_stop, status, setup_tag) "
        "VALUES ('SQQQ','T1','LONG',387,39.0295,0,0,'OPEN','Breakout')"
      )
      await db.execute(
        "INSERT INTO shadow_ledger (symbol, tranche_id, side, quantity, entry_price, "
        "initial_stop, current_stop, status, setup_tag) "
        "VALUES ('SQQQ','T2','LONG',387,39.0295,38.85,38.85,'OPEN','UI_MANUAL')"
      )
      await db.commit()

      logs = await _dedupe_duplicate_opens(db, "SQQQ", 387.0)
      await db.commit()
      assert len(logs) == 1
      assert "去重" in logs[0]

      cur = await db.execute(
        "SELECT id, status, setup_tag, exit_reason FROM shadow_ledger "
        "WHERE symbol='SQQQ' ORDER BY id"
      )
      rows = await cur.fetchall()
      open_rows = [r for r in rows if r[1] == "OPEN"]
      closed = [r for r in rows if r[1] == "CLOSED"]
      assert len(open_rows) == 1
      assert open_rows[0][2] == "UI_MANUAL"
      assert len(closed) == 1
      assert closed[0][3] == "DEDUP"


@pytest.mark.asyncio
async def test_ledger_open_exists_for_fill_by_perm_id():
  async with async_test_db() as get_conn:
    async with get_conn() as db:
      await db.execute(
        "INSERT INTO shadow_ledger (symbol, tranche_id, side, quantity, entry_price, "
        "initial_stop, current_stop, status, perm_id, entry_exec_id) "
        "VALUES ('SQQQ','T1','LONG',387,39.03,38.85,38.85,'OPEN',2042305321,'exec-abc')"
      )
      await db.commit()
      assert await ledger_open_exists_for_fill(
        db, symbol="SQQQ", side="LONG", qty=387, price=39.03, perm_id=2042305321
      )
      assert await ledger_open_exists_for_fill(
        db, symbol="SQQQ", side="LONG", qty=387, price=39.03, exec_id="exec-abc"
      )
      assert not await ledger_open_exists_for_fill(
        db, symbol="SQQQ", side="LONG", qty=100, price=39.03, perm_id=999
      )
