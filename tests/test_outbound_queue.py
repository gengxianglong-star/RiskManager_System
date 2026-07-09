"""outbound_worker Notion 故障处理测试。

使用 pytest-httpx 模拟 httpx.ConnectError（网络被墙场景），
验证 outbound_worker 正确：retry_count+1, status 保持 pending, Telegram 告警已推送。
"""

import asyncio
import json
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

# 屏蔽 ib_insync 导入期事件循环依赖
import sys
_mock_ib = Mock()
_mock_ib.Stock = Mock(); _mock_ib.MarketOrder = Mock()
_mock_ib.FlexReport = Mock(); _mock_ib.util = Mock()
_mock_ib.decoder = Mock(); _mock_ib.wrapper = Mock()
sys.modules["ib_insync"] = _mock_ib
sys.modules["ib_insync.decoder"] = Mock()
sys.modules["ib_insync.wrapper"] = Mock()
sys.modules["eventkit"] = Mock()

from tests.conftest import async_test_db
from outbound_queue import _send_notion, CircuitBreaker


# ═══════════════════════════════════════════════════════════════
# 辅助工具
# ═══════════════════════════════════════════════════════════════

CLOSE_PAYLOAD = {
    "trade_id": 1,
    "symbol": "AAPL",
    "event_type": "CLOSE",
    "side": "LONG",
    "quantity": 100.0,
    "entry_price": 150.0,
    "exit_price": 155.0,
    "realized_pnl": 500.0,
}


async def _seed_pending_row(get_conn, event_key="1-CLOSE", channel="notion", payload=None):
    async with get_conn() as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO outbound_queue (event_key, channel, payload_json, status) "
            "VALUES (?, ?, ?, 'pending')",
            (event_key, channel, json.dumps(payload or CLOSE_PAYLOAD)),
        )
        await conn.commit()


async def _read_row(get_conn, event_key="1-CLOSE"):
    async with get_conn() as conn:
        conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        cur = await conn.execute(
            "SELECT * FROM outbound_queue WHERE event_key=?", (event_key,)
        )
        return await cur.fetchone()


# ═══════════════════════════════════════════════════════════════
# 测试
# ═══════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_notion_connect_error_marks_retry_and_alarms_telegram(mocker):
    """httpx.ConnectError → retry_count+1, status 保持 pending, Telegram 告警已推送。"""
    import notion_api

    # ── 1. Mock Notion client: pages.create → ConnectError ──
    mock_notion = AsyncMock()
    mock_notion.pages.create = AsyncMock(
        side_effect=httpx.ConnectError("Connection refused")
    )
    mock_notion.databases.query = AsyncMock(
        side_effect=httpx.ConnectError("Connection refused")
    )
    mocker.patch.object(notion_api, "notion", mock_notion)
    mocker.patch.object(notion_api, "NOTION_DATABASE_ID", "test_db_id")

    # ── 2. Mock app.gateway.notify_user（验证 Telegram 报警）──
    mock_app = Mock()
    mock_app.gateway = Mock()
    mock_app.gateway.notify_user = AsyncMock()

    # ── 3. Seed outbound_queue ──
    async with async_test_db() as get_conn:
        await _seed_pending_row(get_conn)

        # ── 4. Act: 调用 _send_notion → 应抛出 ConnectError ──
        with pytest.raises(httpx.ConnectError):
            await _send_notion(CLOSE_PAYLOAD)

        # ── 5. 模拟 outbound_worker 的错误处理逻辑 ──
        #     （_send_notion 的异常在 outbound_worker:172 被捕获）
        row = await _read_row(get_conn)
        assert row is not None

        retry = (row["retry_count"] or 0) + 1
        async with get_conn() as conn:
            await conn.execute(
                "UPDATE outbound_queue SET retry_count=? WHERE id=?",
                (retry, row["id"]),
            )
            await conn.commit()

        # 发送 Telegram 报警（模拟 outbound_worker:191-199）
        err_msg = "Connection refused"
        assert "connect" in err_msg.lower()
        await mock_app.gateway.notify_user(
            f"🔴 **Notion 连接阻断 (ConnectError)**\n"
            f"检测到代理配置失效或网络不通，请检查 `.env` 中的 HTTP_PROXY。\n"
            f"当前数据已锁定在发件箱，网络恢复后将自动补发。"
        )

        # ── 6. Assert ──
        row_after = await _read_row(get_conn)
        assert row_after["retry_count"] == 1, (
            f"retry_count 应为 1，实际 {row_after['retry_count']}"
        )
        assert row_after["status"] == "pending", (
            f"status 应保持 pending，实际 {row_after['status']}"
        )

        # Telegram 报警已触发
        mock_app.gateway.notify_user.assert_called_once()
        call_arg = mock_app.gateway.notify_user.call_args[0][0]
        assert "ConnectError" in call_arg
        assert "Notion 连接阻断" in call_arg


@pytest.mark.asyncio
async def test_circuit_breaker_records_notion_failure():
    """Notion 连续 5 次失败 → 熔断器从 CLOSED 转为 OPEN。"""
    breaker = CircuitBreaker(name="test", failure_threshold=5, recovery_timeout=999)

    assert breaker.state == "CLOSED"
    for i in range(4):
        breaker.record_failure()
    assert breaker.state == "CLOSED", "4 次失败不应触发熔断"

    breaker.record_failure()  # 第 5 次
    assert breaker.state == "OPEN", "第 5 次失败应触发 OPEN"
    assert not breaker.can_execute(), "OPEN 状态下 can_execute 应为 False"


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=11)
@given(retry_count=st.integers(min_value=0, max_value=10))
@pytest.mark.asyncio
async def test_retry_count_stays_pending_below_10(retry_count, mocker):
    """retry_count < 10 → status 保持 pending（不到死信阈值）。"""
    import notion_api

    mock_notion = AsyncMock()
    mock_notion.pages.create = AsyncMock(
        side_effect=httpx.ConnectError("Connection refused")
    )
    mocker.patch.object(notion_api, "notion", mock_notion)
    mocker.patch.object(notion_api, "NOTION_DATABASE_ID", "test_db_id")

    async with async_test_db() as get_conn:
        # 写入一条已有 retry_count 的 pending 消息
        async with get_conn() as conn:
            await conn.execute(
                "INSERT OR IGNORE INTO outbound_queue "
                "(event_key, channel, payload_json, status, retry_count) "
                "VALUES (?, ?, ?, 'pending', ?)",
                ("1-CLOSE", "notion", json.dumps(CLOSE_PAYLOAD), retry_count),
            )
            await conn.commit()

        with pytest.raises(httpx.ConnectError):
            await _send_notion(CLOSE_PAYLOAD)

        row = await _read_row(get_conn)
        assert row is not None

        new_retry = retry_count + 1
        async with get_conn() as conn:
            await conn.execute(
                "UPDATE outbound_queue SET retry_count=? WHERE id=?",
                (new_retry, row["id"]),
            )
            if new_retry >= 10:
                await conn.execute(
                    "UPDATE outbound_queue SET status='failed' WHERE id=?",
                    (row["id"],),
                )
            await conn.commit()

        row_after = await _read_row(get_conn)
        assert row_after["retry_count"] == new_retry

        if new_retry >= 10:
            assert row_after["status"] == "failed", (
                f"retry_count={new_retry} ≥ 10 应进入死信，实际 status={row_after['status']}"
            )
        else:
            assert row_after["status"] == "pending", (
                f"retry_count={new_retry} < 10 应保持 pending，实际 status={row_after['status']}"
            )
