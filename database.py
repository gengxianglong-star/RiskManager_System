import datetime
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

import aiosqlite

from config import DB_TIMEOUT, TRADING_TZ, resolve_db_path

DB_PATH = resolve_db_path()


@asynccontextmanager
async def connect_db():
    """带连接级 PRAGMA 的 SQLite 连接生成器。

    关键加固：
    - WAL 模式：读写并发互不阻塞
    - busy_timeout：遇到写锁时排队等待，避免直接抛 database is locked
    - synchronous=NORMAL：兼顾安全与性能（WAL 下崩溃不会损坏 DB）
    """
    conn = await aiosqlite.connect(DB_PATH, timeout=DB_TIMEOUT)
    try:
        await conn.execute("PRAGMA journal_mode=WAL;")
        await conn.execute("PRAGMA synchronous=NORMAL;")
        await conn.execute(f"PRAGMA busy_timeout={int(DB_TIMEOUT * 1000)};")
        await conn.execute("PRAGMA foreign_keys=ON;")
        yield conn
    finally:
        await conn.close()


def trading_now() -> datetime.datetime:
    return datetime.datetime.now(ZoneInfo(TRADING_TZ))


def trading_today_iso() -> str:
    return trading_now().date().isoformat()


async def ensure_schema() -> None:
    async with connect_db() as db:
        await db.execute("PRAGMA journal_mode=WAL;")
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

        # ====== 交易日志标准：扩展 shadow_ledger 列（全部 DEFAULT，向后兼容）======
        _journal_cols = [
            ("order_type", "TEXT DEFAULT ''"),
            ("order_tif", "TEXT DEFAULT ''"),
            ("exec_time", "TEXT DEFAULT ''"),
            ("exchange", "TEXT DEFAULT ''"),
            ("risk_amount", "REAL DEFAULT 0.0"),
            ("entry_equity", "REAL DEFAULT 0.0"),
            ("risk_light", "TEXT DEFAULT ''"),
            ("consec_losses", "INTEGER DEFAULT 0"),
            ("exit_reason", "TEXT DEFAULT ''"),
            ("r_multiple", "REAL DEFAULT 0.0"),
            ("holding_days", "INTEGER DEFAULT 0"),
            ("commissions", "REAL DEFAULT 0.0"),
            ("mae_pct", "REAL DEFAULT 0.0"),
            ("mfe_pct", "REAL DEFAULT 0.0"),
            ("journal_note", "TEXT DEFAULT ''"),
            ("exit_time", "TEXT DEFAULT ''"),
            ("perm_id", "INTEGER DEFAULT 0"),
            ("entry_exec_id", "TEXT DEFAULT ''"),
        ]
        for col_name, col_def in _journal_cols:
            try:
                await db.execute(f"ALTER TABLE shadow_ledger ADD COLUMN {col_name} {col_def}")
            except Exception:
                pass  # 列已存在，跳过

        # ====== 关键复合索引：防止表膨胀后 I/O 阻塞 ======
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ledger_status ON shadow_ledger(status);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ledger_symbol_status ON shadow_ledger(symbol, status);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ledger_create_time ON shadow_ledger(create_time);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ledger_exit_reason ON shadow_ledger(exit_reason);")

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS account_state (
                date DATE PRIMARY KEY,
                locked_equity        REAL NOT NULL,
                high_water_mark      REAL NOT NULL,
                risk_light           TEXT NOT NULL,
                excess_liquidity     REAL DEFAULT 0,
                buying_power         REAL DEFAULT 0,
                init_margin_req      REAL DEFAULT 0,
                maint_margin_req     REAL DEFAULT 0,
                cushion_pct          REAL DEFAULT 0,
                total_cash_value     REAL DEFAULT 0,
                stock_market_value   REAL DEFAULT 0,
                gross_position_value REAL DEFAULT 0,
                unrealized_pnl       REAL DEFAULT 0,
                realized_pnl         REAL DEFAULT 0,
                sma                  REAL DEFAULT 0,
                leverage             REAL DEFAULT 0,
                drawdown_pct         REAL DEFAULT 0
            )
            """
        )
        # ── 迁移：旧表补 account_state 扩展列 ──
        _acct_cols = [
            "excess_liquidity REAL DEFAULT 0",
            "buying_power REAL DEFAULT 0",
            "init_margin_req REAL DEFAULT 0",
            "maint_margin_req REAL DEFAULT 0",
            "cushion_pct REAL DEFAULT 0",
            "total_cash_value REAL DEFAULT 0",
            "stock_market_value REAL DEFAULT 0",
            "gross_position_value REAL DEFAULT 0",
            "unrealized_pnl REAL DEFAULT 0",
            "realized_pnl REAL DEFAULT 0",
            "sma REAL DEFAULT 0",
            "leverage REAL DEFAULT 0",
            "drawdown_pct REAL DEFAULT 0",
        ]
        for col_def in _acct_cols:
            try:
                await db.execute(f"ALTER TABLE account_state ADD COLUMN {col_def}")
            except Exception:
                pass
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
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS system_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        await db.execute(
            'INSERT OR IGNORE INTO system_state (key, value) VALUES ("consecutive_losses", "0")'
        )
        await db.execute(
            'INSERT OR IGNORE INTO system_state (key, value) VALUES ("last_reset_date", "")'
        )
        await db.execute(
            'INSERT OR IGNORE INTO system_state (key, value) VALUES ("last_review_date", "")'
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_tokens (
                symbol TEXT PRIMARY KEY,
                confession TEXT NOT NULL,
                expire_time DATETIME NOT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_reviews (
                date TEXT PRIMARY KEY,
                score INTEGER NOT NULL,
                note TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS applied_splits (
                symbol TEXT,
                split_date TEXT,
                ratio REAL,
                PRIMARY KEY (symbol, split_date)
            )
            """
        )
        # 兼容旧表结构
        try:
            await db.execute("ALTER TABLE notion_queue ADD COLUMN notion_page_id TEXT")
        except Exception:
            pass
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS notion_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                notion_page_id TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                sent INTEGER DEFAULT 0,
                retry_count INTEGER DEFAULT 0,
                UNIQUE(trade_id, event_type)
            )
            """
        )
        # 出站队列表（复用当前连接，避免锁冲突）
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA busy_timeout=15000;")
        # legacy table retained to avoid breaking existing DBs; no longer written to
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS outbound_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT NOT NULL,
                channel TEXT NOT NULL CHECK(channel IN ('telegram', 'notion')),
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0,
                notion_page_id TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                sent_at DATETIME,
                UNIQUE(event_key, channel)
            )
            """
        )
        try:
            await db.execute("ALTER TABLE outbound_queue ADD COLUMN notion_page_id TEXT")
        except Exception:
            pass

        # ═══════════════════════════════════════════════════════════
        # TWS 原生结算引擎：原始成交持久化表
        # ═══════════════════════════════════════════════════════════
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS tws_fills (
                exec_id       TEXT PRIMARY KEY,
                perm_id       INTEGER NOT NULL,
                symbol        TEXT NOT NULL,
                sec_type      TEXT DEFAULT 'STK',
                side          TEXT NOT NULL,
                quantity      REAL NOT NULL,
                price         REAL NOT NULL,
                exec_time     TEXT NOT NULL,
                order_id      INTEGER DEFAULT 0,
                order_ref     TEXT DEFAULT '',
                order_type    TEXT DEFAULT '',
                aux_price     REAL DEFAULT 0.0,
                commission    REAL DEFAULT 0.0,
                exchange      TEXT DEFAULT '',
                account       TEXT DEFAULT '',
                liquidation   INTEGER DEFAULT 0,
                cum_qty       REAL DEFAULT 0,
                con_id        INTEGER DEFAULT 0,
                processed     INTEGER DEFAULT 0,
                created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                processed_at  DATETIME
            )
            """
        )
        # ── 迁移：旧表补 perm_id 列 ──
        try:
            await db.execute(
                "ALTER TABLE tws_fills ADD COLUMN perm_id INTEGER NOT NULL DEFAULT 0"
            )
        except Exception:
            pass
        # ── 迁移：补 liquidation / cum_qty / con_id ──
        for col_def in (
            "liquidation INTEGER DEFAULT 0",
            "cum_qty REAL DEFAULT 0",
            "con_id INTEGER DEFAULT 0",
            "realized_pnl REAL",
        ):
            try:
                await db.execute(f"ALTER TABLE tws_fills ADD COLUMN {col_def}")
            except Exception:
                pass
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_fills_perm_processed "
            "ON tws_fills(perm_id, processed)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_fills_symbol ON tws_fills(symbol)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_fills_time ON tws_fills(exec_time)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_fills_processed ON tws_fills(processed)"
        )

        # ═══════════════════════════════════════════════════════════
        # 全局订单快照：reqAllOpenOrders 全量字段持久化
        # ═══════════════════════════════════════════════════════════
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS open_orders_snapshot (
                perm_id          INTEGER PRIMARY KEY,
                symbol           TEXT NOT NULL,
                order_type       TEXT NOT NULL,
                action           TEXT NOT NULL,
                aux_price        REAL NOT NULL,
                total_qty        REAL NOT NULL,
                filled_qty       REAL DEFAULT 0,
                remaining_qty    REAL DEFAULT 0,
                avg_fill_price   REAL DEFAULT 0,
                trail_stop_price REAL DEFAULT 0,
                tif              TEXT DEFAULT '',
                oca_group        TEXT DEFAULT '',
                order_ref        TEXT DEFAULT '',
                client_id        INTEGER DEFAULT 0,
                why_held         TEXT DEFAULT '',
                status           TEXT DEFAULT '',
                con_id           INTEGER DEFAULT 0,
                account          TEXT DEFAULT '',
                snapped_at       DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_orders_symbol ON open_orders_snapshot(symbol)"
        )

        # ═══════════════════════════════════════════════════════════
        # 交易日志标准：4 张新表
        # ═══════════════════════════════════════════════════════════

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS stop_adjustments (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ledger_id     INTEGER NOT NULL REFERENCES shadow_ledger(id),
                old_stop      REAL NOT NULL,
                new_stop      REAL NOT NULL,
                reason        TEXT NOT NULL,
                triggered_by  TEXT DEFAULT '',
                created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_stop_adj_ledger ON stop_adjustments(ledger_id)"
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_tags (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ledger_id INTEGER NOT NULL REFERENCES shadow_ledger(id),
                tag       TEXT NOT NULL,
                UNIQUE(ledger_id, tag)
            )
            """
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_tags_ledger ON trade_tags(ledger_id)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_tags_tag ON trade_tags(tag)"
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                ledger_id      INTEGER UNIQUE NOT NULL REFERENCES shadow_ledger(id),
                spy_price      REAL,
                spy_volume     REAL,
                vix            REAL,
                sector         TEXT DEFAULT '',
                market_regime  TEXT DEFAULT '',
                created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_reviews (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                ledger_id        INTEGER UNIQUE NOT NULL REFERENCES shadow_ledger(id),
                grade            INTEGER CHECK(grade BETWEEN 1 AND 5),
                followed_plan    INTEGER DEFAULT 0,
                mistake          TEXT DEFAULT '',
                lesson           TEXT DEFAULT '',
                screenshot_entry TEXT DEFAULT '',
                screenshot_exit  TEXT DEFAULT '',
                emotion_entry    TEXT DEFAULT '',
                emotion_exit     TEXT DEFAULT '',
                reviewed_at      DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # 近实时风控快照（ibkr-order-tool 轮询；单行 id='primary'）
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_runtime_snapshot (
                id                   TEXT PRIMARY KEY,
                updated_at           TEXT NOT NULL,
                nlv                  REAL DEFAULT 0,
                hwm                  REAL DEFAULT 0,
                hwm_drawdown_pct     REAL DEFAULT 0,
                month_peak_nlv       REAL DEFAULT 0,
                month_ym             TEXT DEFAULT '',
                month_drawdown_pct   REAL DEFAULT 0,
                consecutive_losses   INTEGER DEFAULT 0,
                daily_opens          INTEGER DEFAULT 0,
                open_risk_usd        REAL DEFAULT 0,
                open_risk_pct        REAL DEFAULT 0,
                risk_cap_usd         REAL DEFAULT 0,
                over_risk            INTEGER DEFAULT 0,
                risk_light           TEXT DEFAULT '',
                risk_budget          REAL DEFAULT 0,
                max_position_r       REAL DEFAULT 0,
                has_over_3r          INTEGER DEFAULT 0,
                cushion              REAL DEFAULT 0,
                positions_json       TEXT DEFAULT '[]',
                stale                INTEGER DEFAULT 0
            )
            """
        )

        from risk_core import store as risk_core_store

        await risk_core_store.ensure_risk_core_schema(db)

        await db.commit()


async def upsert_account_state(
    db_connection,
    equity: float,
    risk_light: str,
    excess_liquidity: float = 0,
    buying_power: float = 0,
    init_margin_req: float = 0,
    maint_margin_req: float = 0,
    cushion_pct: float = 0,
    total_cash_value: float = 0,
    stock_market_value: float = 0,
    gross_position_value: float = 0,
    unrealized_pnl: float = 0,
    realized_pnl: float = 0,
    sma: float = 0,
) -> None:
    today = trading_today_iso()
    leverage = round(gross_position_value / equity, 2) if equity > 0 else 0.0

    cursor = await db_connection.execute(
        "SELECT high_water_mark FROM account_state WHERE date=?", (today,)
    )
    row = await cursor.fetchone()
    if row:
        hwm = max(float(row["high_water_mark"]), equity)
    else:
        cursor = await db_connection.execute("SELECT MAX(high_water_mark) FROM account_state")
        prev = await cursor.fetchone()
        prev_hwm = float(prev[0]) if prev and prev[0] else equity
        hwm = max(prev_hwm, equity)

    drawdown_pct = round((1 - equity / hwm) if hwm > 0 else 0.0, 4)

    await db_connection.execute(
        "INSERT OR REPLACE INTO account_state "
        "(date, locked_equity, high_water_mark, risk_light, "
        "excess_liquidity, buying_power, init_margin_req, maint_margin_req, "
        "cushion_pct, total_cash_value, stock_market_value, "
        "gross_position_value, unrealized_pnl, realized_pnl, "
        "sma, leverage, drawdown_pct) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            today, equity, hwm, risk_light,
            excess_liquidity, buying_power, init_margin_req, maint_margin_req,
            cushion_pct, total_cash_value, stock_market_value,
            gross_position_value, unrealized_pnl, realized_pnl,
            sma, leverage, drawdown_pct,
        ),
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


async def ledger_open_exists_for_fill(
    conn: aiosqlite.Connection,
    *,
    symbol: str,
    side: str,
    qty: float,
    price: float,
    perm_id: int = 0,
    exec_id: str = "",
    day_start_utc: str = "",
    day_end_utc: str = "",
) -> bool:
    """开仓账本是否已记录该笔成交（perm_id / exec_id / 同日同价同量指纹）。"""
    if perm_id and perm_id > 0:
        cur = await conn.execute(
            "SELECT 1 FROM shadow_ledger WHERE perm_id=? LIMIT 1",
            (perm_id,),
        )
        if await cur.fetchone():
            return True
    if exec_id:
        cur = await conn.execute(
            "SELECT 1 FROM shadow_ledger WHERE entry_exec_id=? LIMIT 1",
            (exec_id,),
        )
        if await cur.fetchone():
            return True
    if day_start_utc and day_end_utc and price > 0:
        cur = await conn.execute(
            "SELECT 1 FROM shadow_ledger WHERE symbol=? AND side=? AND status='OPEN' "
            "AND create_time >= ? AND create_time < ? "
            "AND ABS(quantity - ?) < 0.01 "
            "AND ABS(entry_price - ?) / ? < 0.001 "
            "LIMIT 1",
            (symbol, side, day_start_utc, day_end_utc, qty, price, price),
        )
        if await cur.fetchone():
            return True
    return False


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


# ════════════════════════════════════════════════════════════════
# Journal 辅助函数：消除跨文件重复的 r_multiple / holding_days 计算
# ════════════════════════════════════════════════════════════════

def extract_tws_fill_fields(fill_row: dict) -> dict:
    """从 tws_fills 行提取 journal 列值，供 INSERT 路径复用。

    fill_row 是 sqlite3.Row 或普通 dict，包含 tws_fills 表的所有列。
    """
    rpnl = fill_row.get("realized_pnl")
    try:
        rpnl_f = float(rpnl) if rpnl is not None else None
    except (TypeError, ValueError):
        rpnl_f = None
    return {
        "exec_time": fill_row.get("exec_time", "") or "",
        "order_type": fill_row.get("order_type", "") or "",
        "exchange": fill_row.get("exchange", "") or "",
        "commissions": float(fill_row.get("commission") or 0),
        "realized_pnl": rpnl_f,
    }


def compute_close_journal(
    entry_price: float,
    initial_stop: float,
    close_qty: float,
    pnl: float,
    create_time: str,
    exit_time: str,
):
    """计算平仓 journal 字段：(exit_reason, r_multiple, holding_days)。

    r_multiple = pnl / (|entry - stop| * close_qty)，当 stop > 0 时有效。
    holding_days = max(1, exit_time - create_time 的日历天数)。
    exit_reason = "TARGET" 当 pnl > 0，否则 "STOP_HIT"。
    """
    risk_amount = (
        abs(entry_price - initial_stop) * close_qty if initial_stop > 0 else 0.0
    )
    r_mult = round(pnl / risk_amount, 2) if risk_amount > 0 else 0.0
    exit_reason = "TARGET" if pnl > 0 else "STOP_HIT"

    days = 1
    if create_time and exit_time:
        try:
            d1 = datetime.datetime.strptime(str(create_time)[:10], "%Y-%m-%d")
            d2 = datetime.datetime.strptime(str(exit_time)[:10], "%Y-%m-%d")
            days = max(1, (d2 - d1).days)
        except Exception:
            pass

    return exit_reason, r_mult, days
