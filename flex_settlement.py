"""
Flex 权威结算引擎 (CQRS 读取端 / Single Source of Truth)。

职责：定期拉取 IBKR Flex Query 官方报表，与本地影子账本进行权威对账，
       关闭已平仓条目，归档盈亏，通过发件箱异步推送至 Notion。

与主循环解耦：不依赖 TWS 实时连接，可独立调度运行。
"""

import asyncio
import datetime

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
            retryable = any(kw in err_msg.lower() for kw in (
                "1001", "could not be generated", "ssl", "eof",
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

    # ── 逐笔核销 ──
    closed_count = 0
    total_pnl = 0.0
    closed_symbols: list[str] = []

    async with connect_db() as db:
        db.row_factory = lambda cursor, row: dict(
            zip([c[0] for c in cursor.description], row)
        )

        for trade in report.extract("Trade"):
            symbol = _flex_trade_attr(trade, "symbol", "underlyingSymbol")
            if not symbol:
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

            # 按 FIFO 找到最早的 OPEN 仓位进行核销（含 initial_risk 计算因子）
            cur = await db.execute(
                "SELECT id, setup_tag, entry_price, initial_stop, quantity, side "
                "FROM shadow_ledger "
                "WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC LIMIT 1",
                (symbol,),
            )
            row = await cur.fetchone()

            if row:
                trade_id = row["id"]
                setup_tag = row["setup_tag"] or ""
                entry_p = float(row["entry_price"])
                init_stop = float(row["initial_stop"])
                qty = float(row["quantity"])
                pos_side = row["side"]

                # 计算 initial_risk：Notion R-Multiple 计算的基石
                initial_risk = 0.0
                if init_stop > 0:
                    initial_risk = abs(entry_p - init_stop) * qty

                # 权威覆盖：用 Flex 官方数据关闭本地账本
                await db.execute(
                    "UPDATE shadow_ledger SET status='CLOSED', exit_price=?, realized_pnl=? "
                    "WHERE id=?",
                    (exec_price, realized_pnl, trade_id),
                )

                # 事件溯源：通过发件箱异步推送 Notion（含完整结算字段）
                await enqueue_outbound(
                    f"{trade_id}-CLOSE", "notion",
                    {
                        "trade_id": trade_id,
                        "symbol": symbol,
                        "event_type": "CLOSE",
                        "realized_pnl": realized_pnl,
                        "setup_tag": setup_tag,
                        "initial_risk": initial_risk,       # R-Multiple 计算因子
                        "entry_price": entry_p,             # Return % 计算
                        "exit_price": exec_price,           # Return % 计算
                        "quantity": qty,
                        "side": pos_side,
                    },
                )

                closed_count += 1
                total_pnl += realized_pnl
                closed_symbols.append(symbol)

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
            f"数据已通过发件箱异步归档至 Notion。"
        )
        logger.info(msg.replace("\n", " | "))
        if tg_notify_func:
            await tg_notify_func(msg)
    else:
        logger.info("🧾 Flex 报表拉取成功，当前没有需要清算的 OPEN 仓位。")

    return closed_count, total_pnl
