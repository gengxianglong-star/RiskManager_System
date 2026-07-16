"""pytest 全局配置 + 异步数据库 fixture。

每个测试获得独立的临时文件 SQLite schema（function 级别隔离）。
用临时文件而非 :memory:，因为共享缓存内存库在连接全部关闭后会被销毁，
在 Windows 上会导致 ensure_schema 建表后的下一次 connect 读到空库。
临时文件库能保证 ensure_schema + 测试多次 connect_db() 访问同一逻辑库。
"""

import os
import tempfile
import uuid

import pytest_asyncio
from contextlib import asynccontextmanager


@asynccontextmanager
async def async_test_db():
    """非 fixture 版本的 async_db——供 hypothesis @given 测试内部使用。

    hypothesis 不会为每个生成样例重置 fixture，因此需要在测试体内部
    手动 `async with async_test_db() as get_conn:` 以确保每次迭代
    都获得全新的 schema。
    """
    import config
    import database
    from database import connect_db as _connect_db, ensure_schema

    _orig_path_cfg = config.DB_PATH
    _orig_path_db = database.DB_PATH
    _orig_timeout_cfg = config.DB_TIMEOUT
    _orig_timeout_db = database.DB_TIMEOUT

    tmp_path = os.path.join(
        tempfile.gettempdir(), f"rm_test_{uuid.uuid4().hex}.db"
    )
    config.DB_PATH = tmp_path
    database.DB_PATH = tmp_path
    config.DB_TIMEOUT = 5.0
    database.DB_TIMEOUT = 5.0

    try:
        await ensure_schema()

        # 清理前次测试可能残留的数据（共享内存 DB 跨连接持久存在）
        async with _connect_db() as _cleanup_conn:
            for _tbl in ("shadow_ledger", "outbound_queue", "account_state",
                         "pending_intents", "auth_tokens",
                         "daily_reviews", "applied_splits",
                         "flex_processed_execs", "tws_fills", "fill_processed",
                         "notion_queue", "trade_tags", "trade_reviews",
                         "market_snapshots", "stop_adjustments",
                         "risk_runtime_snapshot"):
                try:
                    await _cleanup_conn.execute(f"DELETE FROM {_tbl}")
                except Exception:
                    pass
            # 恢复 ensure_schema 插入的默认 system_state 行
            await _cleanup_conn.execute("DELETE FROM system_state")
            await _cleanup_conn.execute(
                'INSERT OR IGNORE INTO system_state (key, value) VALUES '
                '("consecutive_losses", "0"), '
                '("last_reset_date", ""), '
                '("last_review_date", "")'
            )
            await _cleanup_conn.commit()

        def get_conn():
            return _connect_db()

        yield get_conn

    finally:
        config.DB_PATH = _orig_path_cfg
        database.DB_PATH = _orig_path_db
        config.DB_TIMEOUT = _orig_timeout_cfg
        database.DB_TIMEOUT = _orig_timeout_db
        for _suffix in ("", "-wal", "-shm"):
            try:
                os.remove(tmp_path + _suffix)
            except OSError:
                pass


@pytest_asyncio.fixture(scope="function")
async def async_db():
    """为兼容非 hypothesis 测试——async with async_db() as conn: ..."""
    async with async_test_db() as get_conn:
        yield get_conn
