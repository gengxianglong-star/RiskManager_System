"""pytest 全局配置 + 异步数据库 fixture。

每个测试获得独立的 :memory: SQLite schema（function 级别隔离）。
通过 monkeypatch aiosqlite.connect 将 :memory: 转为共享内存 URI，
确保 ensure_schema + 测试代码的多次 connect_db() 调用访问同一个逻辑库。
"""

import aiosqlite as _aiosqlite
import pytest_asyncio
from contextlib import asynccontextmanager


_ORIG_AIOSQLITE_CONNECT = _aiosqlite.connect


async def _shared_memory_connect(db_path: str, **kwargs):
    """将 :memory: 转为 file::memory:?cache=shared，跨连接共享同一个内存库。"""
    if db_path == ":memory:":
        db_path = "file::memory:?cache=shared"
    return await _ORIG_AIOSQLITE_CONNECT(db_path, **kwargs)


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

    config.DB_PATH = ":memory:"
    database.DB_PATH = ":memory:"
    config.DB_TIMEOUT = 1.0
    database.DB_TIMEOUT = 1.0

    _aiosqlite.connect = _shared_memory_connect

    try:
        await ensure_schema()

        # 清理前次测试可能残留的数据（共享内存 DB 跨连接持久存在）
        async with _connect_db() as _cleanup_conn:
            for _tbl in ("shadow_ledger", "outbound_queue", "account_state",
                         "pending_intents", "auth_tokens",
                         "daily_reviews", "applied_splits", "flex_processed_execs"):
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
        _aiosqlite.connect = _ORIG_AIOSQLITE_CONNECT
        config.DB_PATH = _orig_path_cfg
        database.DB_PATH = _orig_path_db
        config.DB_TIMEOUT = _orig_timeout_cfg
        database.DB_TIMEOUT = _orig_timeout_db


@pytest_asyncio.fixture(scope="function")
async def async_db():
    """为兼容非 hypothesis 测试——async with async_db() as conn: ..."""
    async with async_test_db() as get_conn:
        yield get_conn
