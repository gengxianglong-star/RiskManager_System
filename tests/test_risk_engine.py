"""calculate_risk_light 单元测试 — 覆盖绿/黄/红三种风控灯态。

使用 pytest-mock 模拟数据库连接 (AsyncMock)，
控制 account_state 最高水位线和 system_state 连亏次数。
"""

from unittest.mock import AsyncMock

import pytest
from risk_engine import RiskEngine


# ── 常量（与 config.py 一致）──
EQUITY = 100_000.0
RISK_PCT_PER_TRADE = 0.003  # 0.3%


def _build_mock_cursor(*fetchone_results):
    """构造 AsyncMock cursor 列表，每个对应一次 execute() 调用。

    第一个 cursor 永远模拟 shadow_ledger 查询（fetchall=[]，无持仓）。
    后续 cursor 按传入的 tuple 依次作为 fetchone 返回值。
    """
    cursors = [
        # Q0: shadow_ledger 持仓查询 → 空
        _cursor(fetchall=[]),
    ]
    for result in fetchone_results:
        cursors.append(_cursor(fetchone=result))
    return cursors


def _cursor(fetchone=None, fetchall=None):
    """快捷构造一个 AsyncMock cursor。"""
    c = AsyncMock()
    if fetchone is not None:
        c.fetchone.return_value = fetchone
    if fetchall is not None:
        c.fetchall.return_value = fetchall
    return c


def _build_mock_connection(cursors: list):
    """构造 AsyncMock db_connection，execute 按顺序返回预制的 cursors。"""
    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=cursors)
    return conn


def _risk_engine():
    """创建一个不需要真实 IB/网关依赖的最小 RiskEngine 桩。"""
    return RiskEngine(context=None, ib_listener=None, gateway=None)


# ═══════════════════════════════════════════════════════════════
# 测试用例
# ═══════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_green_light_normal_conditions(mocker):
    """回撤=0 且连亏=0 → 🟢 绿灯，全额度风险预算。"""
    risk = _risk_engine()

    # 三组 fetchone 返回值：
    #   Q1: account_state MAX(high_water_mark) → EQUITY（无水下回撤）
    #   Q2: system_state consecutive_losses   → 0
    cursors = _build_mock_cursor(
        (EQUITY,),   # HWM == equity → drawdown = 0
        ("0",),      # consecutive_losses = 0
    )
    conn = _build_mock_connection(cursors)

    label, budget = await risk.calculate_risk_light(conn, EQUITY)

    assert label == "🟢 绿灯"
    assert budget == pytest.approx(EQUITY * RISK_PCT_PER_TRADE)  # $300


@pytest.mark.asyncio
async def test_yellow_light_consecutive_losses_3(mocker):
    """连亏=3 → 🟡 黄灯（强制），风险预算减半。"""
    risk = _risk_engine()

    cursors = _build_mock_cursor(
        (EQUITY,),   # drawdown = 0
        ("3",),      # consecutive_losses = 3 → 触发黄灯
    )
    conn = _build_mock_connection(cursors)

    label, budget = await risk.calculate_risk_light(conn, EQUITY)

    assert "🟡 黄灯" in label
    assert "连亏:3" in label
    assert budget == pytest.approx(EQUITY * RISK_PCT_PER_TRADE / 2)  # $150


@pytest.mark.asyncio
async def test_red_light_consecutive_losses_5(mocker):
    """连亏=5 → 🔴 红灯（强制），风险预算归零。"""
    risk = _risk_engine()

    cursors = _build_mock_cursor(
        (EQUITY,),   # drawdown = 0
        ("5",),      # consecutive_losses = 5 → 触发红灯
    )
    conn = _build_mock_connection(cursors)

    label, budget = await risk.calculate_risk_light(conn, EQUITY)

    assert "🔴 红灯" in label
    assert "连亏:5" in label
    assert budget == 0.0


@pytest.mark.asyncio
async def test_red_light_drawdown_exceeds_yellow_threshold(mocker):
    """回撤 ≥ 10%（RISK_MAX_DRAWDOWN_YELLOW=0.10）→ 🔴 红灯。"""
    risk = _risk_engine()

    # HWM = 120,000，当前 = 108,000 → drawdown = 12,000/120,000 = 10%
    hwm = 120_000.0
    current = 108_000.0

    cursors = _build_mock_cursor(
        (hwm,),       # drawdown = 10%
        ("0",),       # consecutive_losses = 0
    )
    conn = _build_mock_connection(cursors)

    label, budget = await risk.calculate_risk_light(conn, current)

    assert "🔴 红灯" in label
    assert budget == 0.0
