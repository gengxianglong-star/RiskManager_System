import datetime
from zoneinfo import ZoneInfo

import aiosqlite

from config import DB_PATH, DB_TIMEOUT, TRADING_TZ


def connect_db():
    """带锁等待超时的 SQLite 连接，降低并发写入时的 database is locked。"""
    return aiosqlite.connect(DB_PATH, timeout=DB_TIMEOUT)


def trading_now() -> datetime.datetime:
    return datetime.datetime.now(ZoneInfo(TRADING_TZ))


def trading_today_iso() -> str:
    return trading_now().date().isoformat()


async def ensure_schema() -> None:
    async with connect_db() as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS shadow_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                tranche_id TEXT,
                side TEXT NOT NULL,
                quantity REAL NOT NULL,
                entry_price REAL NOT NULL,
                initial_stop REAL NOT NULL,
                current_stop REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                exit_price REAL,
                realized_pnl REAL,
                setup_tag TEXT,
                spy_context TEXT,
                create_time DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS account_state (
                date DATE PRIMARY KEY,
                locked_equity REAL NOT NULL,
                high_water_mark REAL NOT NULL,
                risk_light TEXT NOT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_intents (
                intent_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                stop_price REAL NOT NULL,
                setup_tag TEXT NOT NULL,
                entry_price REAL NOT NULL,
                quantity REAL NOT NULL,
                spy_context TEXT,
                create_time DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.commit()


async def upsert_account_state(db_connection, equity: float, risk_light: str) -> None:
    today = trading_today_iso()
    cursor = await db_connection.execute(
        "SELECT high_water_mark FROM account_state WHERE date=?", (today,)
    )
    row = await cursor.fetchone()
    if row:
        hwm = max(float(row["high_water_mark"]), equity)
        await db_connection.execute(
            "UPDATE account_state SET locked_equity=?, high_water_mark=?, risk_light=? WHERE date=?",
            (equity, hwm, risk_light, today),
        )
    else:
        cursor = await db_connection.execute("SELECT MAX(high_water_mark) FROM account_state")
        prev = await cursor.fetchone()
        prev_hwm = float(prev[0]) if prev and prev[0] else equity
        hwm = max(prev_hwm, equity)
        await db_connection.execute(
            "INSERT INTO account_state (date, locked_equity, high_water_mark, risk_light) VALUES (?, ?, ?, ?)",
            (today, equity, hwm, risk_light),
        )
    await db_connection.commit()


async def save_pending_intent(conn, intent_id, symbol, stop_price, setup_tag, entry_price, quantity, spy_context):
    await conn.execute(
        "INSERT INTO pending_intents "
        "(intent_id, symbol, stop_price, setup_tag, entry_price, quantity, spy_context) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (intent_id, symbol, stop_price, setup_tag, entry_price, float(quantity), spy_context),
    )
    await conn.commit()


async def load_pending_intent(conn, intent_id):
    cur = await conn.execute("SELECT * FROM pending_intents WHERE intent_id=?", (intent_id,))
    return await cur.fetchone()


async def delete_pending_intent(conn, intent_id):
    await conn.execute("DELETE FROM pending_intents WHERE intent_id=?", (intent_id,))
    await conn.commit()


async def count_open_tranches(conn, symbol):
    cur = await conn.execute(
        "SELECT COUNT(*) FROM shadow_ledger WHERE symbol=? AND status='OPEN'",
        (symbol,),
    )
    row = await cur.fetchone()
    return int(row[0])


async def insert_shadow_ledger(conn, symbol, stop_price, setup_tag, entry_price, quantity, spy_context):
    tranche_num = await count_open_tranches(conn, symbol) + 1
    tranche_id = f"T{tranche_num}"
    cur = await conn.execute(
        "INSERT INTO shadow_ledger "
        "(symbol, tranche_id, side, quantity, entry_price, initial_stop, current_stop, status, setup_tag, spy_context) "
        "VALUES (?, ?, 'LONG', ?, ?, ?, ?, 'OPEN', ?, ?)",
        (symbol, tranche_id, quantity, entry_price, stop_price, stop_price, setup_tag, spy_context),
    )
    await conn.commit()
    return int(cur.lastrowid)


async def get_today_trade_count(conn) -> int:
    """获取今日已确认建仓的次数 (狙击手协议)，按 TRADING_TZ 日切。"""
    tz = ZoneInfo(TRADING_TZ)
    now = datetime.datetime.now(tz)
    day_start = datetime.datetime.combine(now.date(), datetime.time.min, tzinfo=tz)
    day_end = day_start + datetime.timedelta(days=1)
    start_utc = day_start.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    end_utc = day_end.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    cur = await conn.execute(
        "SELECT COUNT(*) FROM shadow_ledger WHERE create_time >= ? AND create_time < ?",
        (start_utc, end_utc),
    )
    row = await cur.fetchone()
    return int(row[0]) if row else 0
