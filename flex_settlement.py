"""
Flex 权威结算引擎 (CQRS 读取端 / Single Source of Truth)。

职责：定期拉取 IBKR Flex Query 官方报表，与本地影子账本进行权威对账，
       关闭已平仓条目，归档盈亏，通过发件箱异步推送至 Notion。

内置：MD5 哈希防重、连亏/连赢动态统计、1001/1025 限流保护。
与主循环解耦：不依赖 TWS 实时连接，可独立调度运行。
"""

import asyncio
import datetime
import hashlib

import pandas_market_calendars as mcal
from ib_insync import FlexReport

from ai_logger import ai_trace, logger
from config import FLEX_QUERY_ID, FLEX_TOKEN
from database import connect_db
from outbound_queue import enqueue_outbound


def _flex_trade_attr(trade, *names, default=""):
    """兼容多种 Flex 报表字段命名。"""
    for name in names:
        val = getattr(trade, name, None)
        if val is not None and val != "":
            return val
    return default


@ai_trace
async def run_flex_settlement(tg_notify_func=None):
    """
    Flex 官方结算 — CQRS 的权威读取端。

    盘中 TWS 事件负责实时写入 OPEN 状态；
    此函数负责滞后拉取官方成交报表，进行精确盈亏核销。

    防重机制：
    1. MD5 哈希签名：报表内容未变 → 静默跳过
    2. realized_pnl ≈ 0 → 跳过（未平仓，交由 TWS 实时接口处理）
    """
    if FLEX_TOKEN.startswith("YOUR_") or FLEX_QUERY_ID.startswith("YOUR_"):
        logger.info("未配置 Flex Token / Query ID，跳过权威结算。")
        return

    # ── 交易日过滤 ──
    nyse = mcal.get_calendar("NYSE")
    today = datetime.datetime.now()
    valid_days = nyse.valid_days(
        start_date=today - datetime.timedelta(days=5),
        end_date=today,
    )
    if len(valid_days) < 2:
        logger.info("近期无有效交易日，跳过 Flex 结算。")
        return

    logger.info("🧾 [Flex Settlement] 开始拉取 IBKR 官方结算报表...")

    loop = asyncio.get_running_loop()
    report = None

    # ── 独立重试：Flex 特有的 1001 / 延迟生成 / SSL / 超时 ──
    for flex_attempt in range(3):
        try:
            report = await loop.run_in_executor(
                None, lambda: FlexReport(FLEX_TOKEN, FLEX_QUERY_ID)
            )
            break
        except Exception as fe:
            err_msg = str(fe)

            # 🚨 1001/1025 限流：重试毫无意义，直接跳过
            if any(kw in err_msg for kw in ("1001", "1025")):
                logger.warning(
                    f"⚠️ Flex 报表遭遇 IBKR 限流 ({err_msg[:60]})，"
                    f"系统将跳过本次对账，等待下次定时任务自动重试。"
                )
                return

            retryable = any(kw in err_msg.lower() for kw in (
                "could not be generated", "ssl", "eof",
                "timeout", "connection",
            ))
            if retryable:
                wait = 20 * (flex_attempt + 1)
                logger.warning(
                    f"Flex 报表未就绪: {err_msg[:80]}... "
                    f"{wait}s 后重试 ({flex_attempt + 1}/3)"
                )
                await asyncio.sleep(wait)
            else:
                logger.error(f"Flex 报表致命错误: {err_msg}")
                return

    if report is None:
        logger.error("❌ Flex 报表 3 次重试均未就绪，本轮结算取消。")
        return

    trades = report.extract("Trade")
    if not trades:
        logger.info("🧾 Flex 报表中无任何交易记录。")
        return

    # ── MD5 哈希防重 ──
    signature_factors = [
        (
            _flex_trade_attr(t, "symbol", "underlyingSymbol"),
            float(_flex_trade_attr(t, "tradePrice", "TradePrice", default=0) or 0),
            float(
                _flex_trade_attr(
                    t, "fifoPnlRealized", "realizedPL", "realizedPnl", "RealizedP/L",
                    default=0,
                ) or 0
            ),
        )
        for t in trades
    ]
    current_hash = hashlib.md5(str(signature_factors).encode("utf-8")).hexdigest()

    async with connect_db() as db:
        db.row_factory = lambda cursor, row: dict(
            zip([c[0] for c in cursor.description], row)
        )

        # 🚀 exec 级防重表：防止同一笔成交被重复叠加 PnL
        await db.execute(
            "CREATE TABLE IF NOT EXISTS flex_processed_execs ("
            "exec_id TEXT PRIMARY KEY, "
            "processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        # 自动清理 30 天前的历史记录，防止无限膨胀
        await db.execute(
            "DELETE FROM flex_processed_execs "
            "WHERE processed_at < datetime('now', '-30 days')"
        )

        cur = await db.execute(
            "SELECT value FROM system_state WHERE key='last_flex_hash'"
        )
        hash_row = await cur.fetchone()

        if hash_row and hash_row["value"] == current_hash:
            logger.info("✅ 报表哈希比对一致，无新清算记录，安全跳过。")
            return

        # ── 逐笔核销 ──
        closed_count = 0
        total_pnl = 0.0
        closed_symbols: list[str] = []

        for trade in trades:
            symbol = _flex_trade_attr(trade, "symbol", "underlyingSymbol")
            if not symbol:
                continue

            # 生成全局唯一 exec_id（防重基石）
            exec_id = _flex_trade_attr(trade, "ibExecID", "ibOrderID", "tradeID")
            if not exec_id:
                exec_id = hashlib.md5(
                    f"{symbol}-{getattr(trade, 'tradePrice', 0)}-"
                    f"{getattr(trade, 'fifoPnlRealized', 0)}".encode()
                ).hexdigest()

            # exec 级查重：已处理过的成交直接跳过
            cur = await db.execute(
                "SELECT 1 FROM flex_processed_execs WHERE exec_id=?", (exec_id,)
            )
            if await cur.fetchone():
                continue

            exec_price = float(
                _flex_trade_attr(trade, "tradePrice", "TradePrice", default=0) or 0
            )
            realized_pnl = float(
                _flex_trade_attr(
                    trade,
                    "fifoPnlRealized", "realizedPL", "realizedPnl", "RealizedP/L",
                    default=0,
                ) or 0
            )

            # 未实现盈亏 → 记录已处理并跳过
            if abs(realized_pnl) < 1e-4:
                await db.execute(
                    "INSERT OR IGNORE INTO flex_processed_execs (exec_id) VALUES (?)",
                    (exec_id,),
                )
                continue

            # 按 FIFO 拉取所有 OPEN 仓位（不再 LIMIT 1）
            cur = await db.execute(
                "SELECT id, setup_tag, entry_price, initial_stop, quantity, side, "
                "realized_pnl FROM shadow_ledger "
                "WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC",
                (symbol,),
            )
            open_rows = await cur.fetchall()

            if not open_rows:
                await db.execute(
                    "INSERT OR IGNORE INTO flex_processed_execs (exec_id) VALUES (?)",
                    (exec_id,),
                )
                continue

            # 提取 Flex 报表中的真实成交数量（用于按比例分摊 PnL）
            flex_qty = abs(float(
                _flex_trade_attr(trade, "quantity", "Quantity", "shares", default=0) or 0
            ))
            if flex_qty < 1e-6:
                flex_qty = sum(float(r["quantity"]) for r in open_rows)

            remaining_qty = flex_qty
            trade_pnl = realized_pnl

            for row in open_rows:
                if remaining_qty <= 1e-6:
                    break

                trade_id = row["id"]
                setup_tag = row["setup_tag"] or ""
                entry_p = float(row["entry_price"])
                init_stop = float(row["initial_stop"])
                db_qty = float(row["quantity"])
                pos_side = row["side"]
                old_pnl = float(row["realized_pnl"] or 0.0)

                close_qty = min(db_qty, remaining_qty)
                portion_pnl = trade_pnl * (close_qty / flex_qty) if flex_qty > 0 else trade_pnl
                new_total_pnl = old_pnl + portion_pnl
                new_qty = db_qty - close_qty
                is_fully_closed = new_qty <= 1e-4
                new_status = "CLOSED" if is_fully_closed else "OPEN"

                await db.execute(
                    "UPDATE shadow_ledger SET status=?, exit_price=?, quantity=?, "
                    "realized_pnl=? WHERE id=?",
                    (new_status, exec_price, new_qty, new_total_pnl, trade_id),
                )

                initial_risk = abs(entry_p - init_stop) * db_qty if init_stop > 0 else 0.0
                event_type = "CLOSE" if is_fully_closed else "UPDATE"

                await enqueue_outbound(
                    f"{trade_id}-{event_type}_FLEX_{exec_id[-6:]}", "notion",
                    {
                        "trade_id": trade_id,
                        "symbol": symbol,
                        "event_type": event_type,
                        "realized_pnl": new_total_pnl,
                        "setup_tag": setup_tag,
                        "initial_risk": initial_risk,
                        "entry_price": entry_p,
                        "exit_price": exec_price,
                        "quantity": new_qty,
                        "side": pos_side,
                    },
                )

                await db.execute(
                    "INSERT OR IGNORE INTO flex_processed_execs (exec_id) VALUES (?)",
                    (exec_id,),
                )

                if is_fully_closed:
                    closed_count += 1
                    closed_symbols.append(symbol)

                remaining_qty -= close_qty

            # ── 更新连亏计数器（基于整笔交易方向）──
            if trade_pnl > 0:
                await db.execute(
                    "INSERT INTO system_state (key, value) VALUES "
                    "('consecutive_losses', '0') "
                    "ON CONFLICT(key) DO UPDATE SET value = '0'"
                )
                logger.info(f"🎯 {symbol} 止盈，连亏计数器已重置。")
            else:
                await db.execute(
                    "INSERT INTO system_state (key, value) VALUES "
                    "('consecutive_losses', '1') "
                    "ON CONFLICT(key) DO UPDATE SET value = "
                    "CAST(CAST(value AS INTEGER) + 1 AS TEXT)"
                )
                logger.info(f"🩸 {symbol} 止损，连亏计数器 +1。")

            total_pnl += trade_pnl

        await db.execute(
            "INSERT OR REPLACE INTO system_state (key, value) VALUES "
            "('last_flex_hash', ?)",
            (current_hash,),
        )
        await db.commit()

    # ── 汇报 ──
    if closed_count > 0:
        pnl_str = (
            f"+${total_pnl:.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):.2f}"
        )
        msg = (
            f"✅ **Flex 权威清算完成**\n"
            f"成功关闭 {closed_count} 笔仓位：{', '.join(closed_symbols)}\n"
            f"合计盈亏: {pnl_str}\n"
            f"连亏计数器已同步更新，数据已归档至 Notion。"
        )
        logger.info(msg.replace("\n", " | "))
        if tg_notify_func:
            await tg_notify_func(msg)
    else:
        logger.info("🧾 Flex 报表拉取成功，当前没有需要清算的 OPEN 仓位。")

    return closed_count, total_pnl
