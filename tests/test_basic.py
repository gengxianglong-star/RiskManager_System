"""冒烟测试 — 验证测试工具链（pytest + hypothesis + async_db fixture）正常运转。"""

import aiosqlite
from hypothesis import given
from hypothesis import strategies as st


@given(st.floats(allow_nan=False, allow_infinity=False), st.floats(allow_nan=False, allow_infinity=False))
def test_add_two_floats_produces_float(a: float, b: float):
    """两个有限浮点数相加，结果必须是 float 类型。"""
    result = a + b
    assert isinstance(result, float)
    assert a + b == b + a


async def test_async_db_fixture_creates_tables(async_db):
    """async_db fixture 应创建完整 schema——shadow_ledger 表可读写。"""
    async with async_db() as conn:
        conn.row_factory = aiosqlite.Row

        # 写入一条影子账本记录
        cur = await conn.execute(
            "INSERT INTO shadow_ledger "
            "(symbol, tranche_id, side, quantity, entry_price, initial_stop, current_stop, status, setup_tag) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("AAPL", "T1", "LONG", 100.0, 150.0, 145.0, 148.0, "OPEN", "BREAKOUT"),
        )
        await conn.commit()
        row_id = cur.lastrowid

        # 读回验证
        cur = await conn.execute("SELECT * FROM shadow_ledger WHERE id=?", (row_id,))
        row = await cur.fetchone()
        assert row is not None
        assert row["symbol"] == "AAPL"
        assert row["side"] == "LONG"
        assert float(row["quantity"]) == 100.0
        assert float(row["entry_price"]) == 150.0
        assert row["status"] == "OPEN"
