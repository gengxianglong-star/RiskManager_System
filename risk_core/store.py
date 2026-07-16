"""RiskCore SQLite 持久化。"""

from __future__ import annotations

import json
from typing import Any

import aiosqlite

from risk_core.models import ShadowPosition, Side


STATE_KEYS = (
    "hwm",
    "last_nlv",
    "rth_close_nlv",
    "consecutive_losses",
    "last_sync_at",
    "sync_block_reason",
    "last_cushion",
    "daily_opens",
    "daily_opens_date",
    "month_peak_nlv",
    "month_ym",
)


async def ensure_risk_core_schema(db: aiosqlite.Connection) -> None:
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_core_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS shadow_positions (
            symbol TEXT PRIMARY KEY,
            side TEXT NOT NULL,
            qty REAL NOT NULL,
            entry REAL NOT NULL,
            stop REAL NOT NULL DEFAULT 0,
            life_realized_pnl REAL NOT NULL DEFAULT 0,
            max_abs_qty REAL NOT NULL DEFAULT 0,
            avg_cost_ib REAL NOT NULL DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS fill_processed (
            exec_id TEXT PRIMARY KEY,
            processed_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS logical_orders (
            perm_id TEXT PRIMARY KEY,
            symbol TEXT,
            side TEXT,
            intent TEXT,
            filled_qty REAL DEFAULT 0,
            avg_price REAL DEFAULT 0,
            realized_pnl REAL DEFAULT 0,
            finalized INTEGER DEFAULT 0
        )
        """
    )
    defaults = {
        "hwm": "0",
        "last_nlv": "0",
        "rth_close_nlv": "0",
        "consecutive_losses": "0",
        "last_sync_at": "",
        "sync_block_reason": "",
        "last_cushion": "1",
        "daily_opens": "0",
        "daily_opens_date": "",
        "month_peak_nlv": "0",
        "month_ym": "",
    }
    for k, v in defaults.items():
        await db.execute(
            "INSERT OR IGNORE INTO risk_core_state (key, value) VALUES (?, ?)",
            (k, v),
        )
    await db.commit()


async def get_state_map(db: aiosqlite.Connection) -> dict[str, str]:
    cur = await db.execute("SELECT key, value FROM risk_core_state")
    rows = await cur.fetchall()
    return {r[0]: r[1] for r in rows}


async def set_state(db: aiosqlite.Connection, key: str, value: str) -> None:
    await db.execute(
        "INSERT OR REPLACE INTO risk_core_state (key, value) VALUES (?, ?)",
        (key, str(value)),
    )


async def set_states(db: aiosqlite.Connection, mapping: dict[str, Any]) -> None:
    """批量写状态；不 commit，由调用方统一提交。"""
    for k, v in mapping.items():
        await set_state(db, k, v)


async def load_positions(db: aiosqlite.Connection) -> list[ShadowPosition]:
    cur = await db.execute(
        "SELECT symbol, side, qty, entry, stop, life_realized_pnl, max_abs_qty, avg_cost_ib "
        "FROM shadow_positions WHERE qty > 1e-12"
    )
    rows = await cur.fetchall()
    out: list[ShadowPosition] = []
    for r in rows:
        out.append(
            ShadowPosition(
                symbol=r[0],
                side=Side(r[1]),
                qty=float(r[2]),
                entry=float(r[3]),
                stop=float(r[4] or 0),
                life_realized_pnl=float(r[5] or 0),
                max_abs_qty=float(r[6] or 0),
                avg_cost_ib=float(r[7] or 0),
            )
        )
    return out


async def save_position(db: aiosqlite.Connection, pos: ShadowPosition) -> None:
    await db.execute(
        """
        INSERT OR REPLACE INTO shadow_positions
        (symbol, side, qty, entry, stop, life_realized_pnl, max_abs_qty, avg_cost_ib, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            pos.symbol,
            pos.side.value,
            pos.qty,
            pos.entry,
            pos.stop,
            pos.life_realized_pnl,
            pos.max_abs_qty,
            pos.avg_cost_ib,
        ),
    )


async def delete_position(db: aiosqlite.Connection, symbol: str) -> None:
    await db.execute("DELETE FROM shadow_positions WHERE symbol=?", (symbol,))


async def replace_all_positions(
    db: aiosqlite.Connection, positions: list[ShadowPosition]
) -> None:
    """全量替换持仓；不 commit，由调用方统一提交。"""
    await db.execute("DELETE FROM shadow_positions")
    for p in positions:
        await save_position(db, p)


async def mark_fill(db: aiosqlite.Connection, exec_id: str) -> bool:
    """INSERT OR IGNORE fill；不 commit。True = 新插入（非重复）。"""
    cur = await db.execute(
        "INSERT OR IGNORE INTO fill_processed (exec_id) VALUES (?)", (exec_id,)
    )
    return cur.rowcount > 0


async def apply_split_to_shadow_positions(
    db: aiosqlite.Connection, symbol: str, ratio: float
) -> None:
    """拆股：数量 ×ratio，价格/止损/均价 ÷ratio。不 commit。"""
    if ratio <= 0 or ratio == 1.0:
        return
    await db.execute(
        """
        UPDATE shadow_positions
        SET qty = qty * ?,
            entry = entry / ?,
            stop = CASE WHEN stop > 0 THEN stop / ? ELSE 0 END,
            avg_cost_ib = CASE WHEN avg_cost_ib > 0 THEN avg_cost_ib / ? ELSE 0 END,
            updated_at = CURRENT_TIMESTAMP
        WHERE symbol = ?
        """,
        (ratio, ratio, ratio, ratio, symbol),
    )


async def load_open_ledger_anchors(
    db: aiosqlite.Connection,
) -> dict[str, tuple[float, float]]:
    """从 shadow_ledger 读取 OPEN 仓的 entry/stop 锚点（守护进程维护，权威来源）。"""
    cur = await db.execute(
        "SELECT symbol, entry_price, current_stop FROM shadow_ledger "
        "WHERE status='OPEN' AND quantity > 1e-12"
    )
    rows = await cur.fetchall()
    out: dict[str, tuple[float, float]] = {}
    for r in rows:
        sym = r[0] if not isinstance(r, dict) else r["symbol"]
        entry = float(r[1] if not isinstance(r, dict) else r["entry_price"])
        stop = float(r[2] if not isinstance(r, dict) else r["current_stop"])
        if sym in out:
            prev_e, prev_s = out[sym]
            entry = entry if entry > 0 else prev_e
            stop = stop if stop > 0 else prev_s
        out[sym] = (entry, stop)
    return out


async def sync_stops_from_ledger(db: aiosqlite.Connection) -> list[str]:
    """
    将 shadow_ledger 的止损/进场价同步到 shadow_positions。

  规则：
    - 仅当 ledger stop > 0 且 (shadow stop==0 或 |diff|>0.001) 时更新止损
    - 仅当 ledger entry > 0 且 shadow entry==0 时补进场价
    - 不 commit，由调用方统一提交

    Returns:
        发生变更的标的列表
    """
    anchors = await load_open_ledger_anchors(db)
    if not anchors:
        return []

    cur = await db.execute(
        "SELECT symbol, entry, stop FROM shadow_positions WHERE qty > 1e-12"
    )
    rows = await cur.fetchall()
    changed: list[str] = []
    for r in rows:
        sym = r[0] if not isinstance(r, dict) else r["symbol"]
        if sym not in anchors:
            continue
        ledger_entry, ledger_stop = anchors[sym]
        pos_entry = float(r[1] if not isinstance(r, dict) else r["entry"])
        pos_stop = float(r[2] if not isinstance(r, dict) else r["stop"])

        new_stop = pos_stop
        new_entry = pos_entry
        if ledger_stop > 0 and (
            pos_stop <= 0 or abs(pos_stop - ledger_stop) > 0.001
        ):
            new_stop = ledger_stop
        if ledger_entry > 0 and pos_entry <= 0:
            new_entry = ledger_entry

        if new_stop != pos_stop or new_entry != pos_entry:
            await db.execute(
                """
                UPDATE shadow_positions
                SET stop=?, entry=?, updated_at=CURRENT_TIMESTAMP
                WHERE symbol=?
                """,
                (new_stop, new_entry, sym),
            )
            changed.append(sym)
    return changed
