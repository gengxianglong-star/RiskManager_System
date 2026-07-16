"""_async_on_execution FIFO 平仓逻辑的 hypothesis 参数化测试。

模拟 LONG 100 股初始持仓，对随机 fill_qty / fill_price 进行平仓操作，
验证 FIFO 核销、部分平仓、卖穿（超额卖出）三种场景下的影子账本状态。

不使用真实 TWS 连接——所有 IB 相关接口通过 unittest.mock 隔离。
"""

import asyncio
import sys
from unittest.mock import AsyncMock, Mock

import aiosqlite
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

# ib_insync 在模块导入时会触发 eventkit 初始化事件循环，但 pytest
# 收集阶段尚不存在事件循环。注入假模块绕过导入期依赖。
_mock_ib_insync = Mock()
_mock_ib_insync.Stock = Mock()
_mock_ib_insync.MarketOrder = Mock()
_mock_ib_insync.FlexReport = Mock()
_mock_ib_insync.util = Mock()
_mock_ib_insync.decoder = Mock()
_mock_ib_insync.wrapper = Mock()

# 直接覆盖——如果之前被其他测试导入过，确保替换为 mock
sys.modules["ib_insync"] = _mock_ib_insync
sys.modules["ib_insync.decoder"] = Mock()
sys.modules["ib_insync.wrapper"] = Mock()
sys.modules["eventkit"] = Mock()

from ib_listener import IBKRListener, _apply_consecutive_losses
from tests.conftest import async_test_db


# ═══════════════════════════════════════════════════════════════
# 辅助工具
# ═══════════════════════════════════════════════════════════════

INITIAL_QTY = 100.0
INITIAL_ENTRY = 100.0
INITIAL_STOP = 95.0
SYMBOL = "AAPL"


def _make_lock_factory():
    """返回一个按需创建 asyncio.Lock 的 callable。"""
    locks: dict[str, asyncio.Lock] = {}
    def get_lock(symbol: str) -> asyncio.Lock:
        if symbol not in locks:
            locks[symbol] = asyncio.Lock()
        return locks[symbol]
    return get_lock


def _build_mock_listener():
    """构造一个 mock IBKRListener，CLOSE 路径所需的最小依赖。

    OPEN 路径需要的组件（ib.openTrades / fetch_account_equity /
    validate_pending_entry）ALL mock 为会崩溃的哨兵，确保 CLOSE 路径
    不会误入 OPEN 分支。
    """
    ctx = Mock()
    ctx.get_symbol_lock = _make_lock_factory()
    ctx.spawn_background_task = AsyncMock()
    ctx.risk_engine = Mock()
    ctx.risk_engine.night_watchman_on_tp = AsyncMock()
    # 哨兵——如果 CLOSE 路径误入了 OPEN 分支，这些调用会立即崩溃
    ctx.ib = Mock()
    ctx.ib.isConnected = Mock(side_effect=RuntimeError("OPEN path: must not call ib.isConnected"))
    ctx.ib.openTrades = Mock(side_effect=RuntimeError("OPEN path: must not call ib.openTrades"))

    gateway = Mock()
    gateway.notify_user = AsyncMock()  # CLOSE 路径也会通知

    listener = IBKRListener.__new__(IBKRListener)
    listener.ctx = ctx
    listener.gateway = gateway
    listener.ib = ctx.ib
    from ib_market import QuoteHub
    listener.quotes = QuoteHub(ctx.ib)
    listener._streak_applied_execs = set()
    listener._pending_pnl_alloc = {}
    # 净值偏低 → scratch 阈值 ~$10，亏损平仓可计入连亏
    listener.fetch_account_equity = AsyncMock(return_value=10_000.0)
    return listener


def _make_trade(symbol: str = SYMBOL):
    """构造 mock ib_insync Trade 对象。orderType="STP" 确保守夜人不会误触发。"""
    t = Mock()
    t.order = Mock()
    t.order.orderRef = ""
    t.order.orderType = "STP"          # ≠ "LMT" → night_watchman 立即返回
    t.order.tif = ""
    t.order.permId = 99999
    t.contract = Mock()
    t.contract.symbol = symbol
    return t


def _make_fill(symbol: str, side: str, shares: float, price: float,
               entry: float = INITIAL_ENTRY):
    """构造 mock Fill；附带 TWS commissionReport.realizedPNL。"""
    f = Mock()
    f.execution = Mock()
    f.execution.side = side      # "BOT" = 买入, "SLD" = 卖出
    f.execution.shares = shares
    f.execution.price = price
    f.execution.time = None
    f.execution.exchange = ""
    f.execution.execId = f"exec-{side}-{shares}-{price}"
    f.execution.permId = 99999
    f.execution.orderId = 1
    f.execution.liquidation = 0
    f.execution.cumQty = shares
    f.contract = Mock()
    f.contract.symbol = symbol
    f.contract.conId = 123
    cr = Mock()
    cr.commission = 1.0
    # LONG 平仓=SELL → (px-entry)*qty；SHORT 平仓=BOT → (entry-px)*qty
    if side == "SLD":
        cr.realizedPNL = (price - entry) * shares
    elif side == "BOT":
        # 测试里 BOT 多作加仓；开仓腿用哨兵
        cr.realizedPNL = 1e100
    else:
        cr.realizedPNL = 0.0
    f.commissionReport = cr
    return f


async def _seed_open_position(conn, symbol=SYMBOL, qty=INITIAL_QTY, entry=INITIAL_ENTRY,
                               stop=INITIAL_STOP, side="LONG"):
    """在影子账本中写入一笔 OPEN 仓位。"""
    c = await conn.execute(
        "INSERT INTO shadow_ledger "
        "(symbol, tranche_id, side, quantity, entry_price, initial_stop, current_stop, status, setup_tag) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)",
        (symbol, "T1", side, qty, entry, stop, stop, "BREAKOUT"),
    )
    await conn.commit()
    return c.lastrowid


# ═══════════════════════════════════════════════════════════════
# 测试用例
# ═══════════════════════════════════════════════════════════════

@settings(max_examples=20)
@given(
    fill_qty=st.floats(min_value=1.0, max_value=200.0, allow_nan=False, allow_infinity=False),
    fill_price=st.floats(min_value=50.0, max_value=200.0, allow_nan=False, allow_infinity=False),
)
@pytest.mark.asyncio
async def test_fifo_close_with_random_fills(fill_qty, fill_price):
    """对 LONG 100 股持仓执行随机数量的 SELL 成交，验证影子账本状态。

    预期行为：
    - fill_qty <  100 → 部分平仓，剩余仓位被保留
    - fill_qty ≈  100 → 全额平仓
    - fill_qty >  100 → 全额平仓 + 创建反向仓位（卖超修正）
    """
    fill_qty = round(fill_qty, 4)
    fill_price = round(fill_price, 2)

    async with async_test_db() as get_conn:
        async with get_conn() as conn:
            await _seed_open_position(conn)

        listener = _build_mock_listener()
        trade = _make_trade()
        fill = _make_fill(SYMBOL, "SLD", fill_qty, fill_price)

        await listener._async_on_execution(trade, fill)

        async with get_conn() as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(
                "SELECT id, side, quantity, entry_price, status, exit_price, realized_pnl "
                "FROM shadow_ledger WHERE symbol=? ORDER BY id",
                (SYMBOL,),
            )
            rows = await cur.fetchall()

        open_rows = [r for r in rows if r["status"] == "OPEN"]
        closed_rows = [r for r in rows if r["status"] == "CLOSED"]

        if fill_qty < INITIAL_QTY:
            assert len(open_rows) == 1, f"部分平仓应保留 1 笔 OPEN，实际 {len(open_rows)}"
            remaining_qty = INITIAL_QTY - fill_qty
            assert abs(float(open_rows[0]["quantity"]) - remaining_qty) < 0.01, (
                f"剩余数量应为 {remaining_qty}，实际 {open_rows[0]['quantity']}"
            )

        elif abs(fill_qty - INITIAL_QTY) < 0.01:
            assert len(open_rows) == 0, "全额平仓应无 OPEN 记录"
            assert len(closed_rows) == 1, "应有 1 笔 CLOSED 记录"
            assert float(closed_rows[0]["exit_price"]) == fill_price

        else:
            # ── 卖穿 (fill_qty > 100): 原仓位全平 + 新建反向仓位 ──
            assert len(closed_rows) >= 1, f"原仓位应被关闭，实际 CLOSED={len(closed_rows)}"
            assert float(closed_rows[0]["exit_price"]) == fill_price
            # 新反向仓位
            reverse_rows = [r for r in open_rows if r["side"] == "SHORT"]
            assert len(reverse_rows) == 1, (
                f"应创建 1 笔 SHORT 反向仓位（超出 {fill_qty - INITIAL_QTY:.1f} 股），"
                f"实际 OPEN={len(open_rows)}"
            )
            expected_reverse_qty = fill_qty - INITIAL_QTY
            assert abs(float(reverse_rows[0]["quantity"]) - expected_reverse_qty) < 0.01, (
                f"反向仓位数量应为 {expected_reverse_qty}，"
                f"实际 {reverse_rows[0]['quantity']}"
            )


@settings(max_examples=10, deadline=None)
@given(
    fill_qty=st.floats(min_value=1.0, max_value=50.0, allow_nan=False, allow_infinity=False),
    # 保证 (price-100)*qty < -scratch(~$10)
    fill_price=st.floats(min_value=80.0, max_value=89.0, allow_nan=False, allow_infinity=False),
)
@pytest.mark.asyncio
async def test_partial_close_loss_triggers_consecutive_loss_counter(fill_qty, fill_price):
    """亏损部分平仓 → TWS realizedPNL 计亏 → consecutive_losses +1。"""
    fill_qty = round(fill_qty, 4)
    fill_price = round(fill_price, 2)

    async with async_test_db() as get_conn:
        async with get_conn() as conn:
            await _seed_open_position(conn)

        listener = _build_mock_listener()
        trade = _make_trade()
        fill = _make_fill(SYMBOL, "SLD", fill_qty, fill_price)

        await listener._async_on_execution(trade, fill)

        async with get_conn() as conn:
            cur = await conn.execute(
                "SELECT value FROM system_state WHERE key='consecutive_losses'"
            )
            row = await cur.fetchone()
        assert row is not None
        assert int(row[0]) >= 1, f"亏损卖出后连亏计数应 ≥ 1，实际 {row[0]}"
