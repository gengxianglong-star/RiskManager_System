import sqlite3
import os


def ensure_schema(db_name: str = "risk_manager.db") -> None:
    """创建缺失表（兼容旧库升级）。"""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(
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
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS account_state (
        date DATE PRIMARY KEY,
        locked_equity REAL NOT NULL,
        high_water_mark REAL NOT NULL,
        risk_light TEXT NOT NULL
    )
    """
    )
    cursor.execute(
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
    conn.commit()
    conn.close()


def create_database():
    db_name = "risk_manager.db"
    if os.path.exists(db_name):
        ensure_schema(db_name)
        print(f"⚠️ 数据库 {db_name} 已存在，已校验表结构。")
        return

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(
        """
    CREATE TABLE shadow_ledger (
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
    cursor.execute(
        """
    CREATE TABLE account_state (
        date DATE PRIMARY KEY,
        locked_equity REAL NOT NULL,
        high_water_mark REAL NOT NULL,
        risk_light TEXT NOT NULL
    )
    """
    )
    cursor.execute(
        """
    CREATE TABLE pending_intents (
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
    conn.commit()
    conn.close()
    print(f"✅ 极致风控数据库 {db_name} 初始化成功！")


if __name__ == "__main__":
    create_database()
