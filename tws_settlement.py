"""
TWS 原生结算引擎 — 替代 Flex Query。

数据源：tws_fills 本地持久化表 + ib.trades() 在线富化。
实现免疫 API 限流的 T+0 闭环对账。

两阶段：
  Phase 1 — 离线未清算成交的 FIFO 核销（兜底开仓 + 关仓自算 P&L）
  Phase 2 — 物理持仓深度对账（委托 reconcile_physical_positions）
"""

import asyncio
import datetime

from ib_insync import ExecutionFilter
from ib_insync.util import formatIBDatetime

from ai_logger import ai_trace, logger
from database import connect_db, compute_close_journal, extract_tws_fill_fields
from outbound_queue import enqueue_outbound


class TWSSettlement:
    """
    TWS 原生结算引擎。

    backfill_fills:   reqExecutionsAsync 补漏断网/宕机期间的成交
    collect_fills:    ib.trades() 订单意图富化（orderRef / auxPrice）
    _phase1_fifo_settle: 针对未清算 fills 进行 FIFO 关仓核销 + 兜底开仓
    settle:           完整双阶段结算入口
    """

    @ai_trace
    async def backfill_fills(self, ib, days: int = 5) -> int:
        """从 TWS 拉取近期成交，填补断网/系统宕机期间漏掉的物理成交。"""
        if not ib or not ib.isConnected():
            return 0

        start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        logger.info(f"📦 [TWS Settlement] Backfill: 拉取 {start.isoformat()[:10]} 至今的成交...")

        try:
            fills = await ib.reqExecutionsAsync(
                ExecutionFilter(time=formatIBDatetime(start))
            )
        except Exception as e:
            logger.error(f"Backfill reqExecutionsAsync 失败: {e}")
            return 0

        if not fills:
            return 0

        # 仅保留有效 STK 成交
        fills = [
            f for f in fills
            if f.contract.secType == "STK"
            and float(f.execution.shares) > 0
            and f.contract.symbol
        ]
        if not fills:
            return 0

        new_inserts = 0
        async with connect_db() as db:
            for fill in fills:
                execution = fill.execution
                contract = fill.contract
                symbol = contract.symbol
                side = "LONG" if execution.side == "BOT" else "SHORT"
                qty = float(execution.shares)
                price = float(execution.price)
                exec_time = (
                    execution.time.isoformat()
                    if hasattr(execution.time, "isoformat")
                    else str(execution.time)
                )
                commission_val = (
                    float(fill.commissionReport.commission)
                    if (hasattr(fill, "commissionReport")
                        and fill.commissionReport
                        and fill.commissionReport.commission)
                    else 0.0
                )

                # processed=0：交由 Phase 1 批处理核销
                cursor = await db.execute(
                    "INSERT OR IGNORE INTO tws_fills "
                    "(exec_id, perm_id, symbol, side, quantity, price, exec_time, "
                    "order_id, commission, exchange, "
                    "liquidation, cum_qty, con_id, processed) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
                    (
                        execution.execId, execution.permId, symbol, side, qty, price, exec_time,
                        execution.orderId, commission_val,
                        getattr(execution, "exchange", "") or "",
                        int(getattr(execution, "liquidation", 0) or 0),
                        float(getattr(execution, "cumQty", 0) or 0),
                        int(contract.conId),
                    ),
                )
                if cursor.rowcount > 0:
                    new_inserts += 1
            await db.commit()

        if new_inserts > 0:
            logger.info(f"📦 [TWS Settlement] Backfill 补录了 {new_inserts} 笔缺失成交。")
        return new_inserts

    @ai_trace
    async def collect_fills(self, ib) -> int:
        """从当前会话提取订单意图 (orderRef / auxPrice)，富化物理记录。"""
        if not ib or not ib.isConnected():
            return 0

        enriched_count = 0
        async with connect_db() as db:
            for trade in ib.trades():
                if not trade.fills:
                    continue
                setup_tag = trade.order.orderRef if trade.order and trade.order.orderRef else "TWS_SYNC"
                order_type = trade.order.orderType if trade.order else ""
                aux_price = (
                    float(trade.order.auxPrice)
                    if (trade.order and trade.order.auxPrice
                        and trade.order.orderType in ("STP", "STP LMT"))
                    else 0.0
                )

                for fill in trade.fills:
                    cursor = await db.execute(
                        "UPDATE tws_fills SET order_ref=?, order_type=?, aux_price=? "
                        "WHERE exec_id=? AND (order_ref='' OR order_ref IS NULL)",
                        (setup_tag, order_type, aux_price, fill.execution.execId),
                    )
                    enriched_count += cursor.rowcount
            await db.commit()

        if enriched_count > 0:
            logger.info(f"📦 [TWS Settlement] Collect 成功富化 {enriched_count} 笔订单意图。")
        return enriched_count

    @ai_trace
    async def _phase1_fifo_settle(self) -> tuple[int, float]:
        """Phase 1: 针对离线未清算 fills 进行 FIFO 关仓核销 + 兜底开仓。"""
        closed_count = 0
        total_pnl = 0.0

        async with connect_db() as db:
            db.row_factory = lambda cursor, row: dict(
                zip([c[0] for c in cursor.description], row)
            )

            # ── ORDER-LEVEL AGGREGATION: GROUP BY perm_id 还原拆单碎片为原始订单 ──
            # VWAP = SUM(price * quantity) / SUM(quantity)，财务盈亏分毫不差
            cursor = await db.execute(
                "SELECT "
                "  perm_id, "
                "  MAX(exec_id)   AS last_exec_id, "
                "  symbol, "
                "  side, "
                "  SUM(quantity)  AS agg_qty, "
                "  SUM(price * quantity) / SUM(quantity) AS agg_price, "
                "  MAX(exec_time) AS last_time, "
                "  MAX(order_ref) AS order_ref, "
                "  MAX(order_type) AS order_type, "
                "  MAX(aux_price) AS aux_price, "
                "  MAX(exchange)  AS exchange, "
                "  MAX(commission) AS commission "
                "FROM tws_fills "
                "WHERE processed=0 "
                "GROUP BY perm_id, symbol, side "
                "ORDER BY last_time ASC"
            )
            unprocessed = await cursor.fetchall()

            if not unprocessed:
                return 0, 0.0

            # 碎片数 = 实际 exec 数 vs 聚合后的订单数
            cursor2 = await db.execute(
                "SELECT COUNT(*) FROM tws_fills WHERE processed=0"
            )
            raw_count = (await cursor2.fetchone())[0]
            logger.info(
                f"⚙️ [Phase 1] {raw_count} 笔碎片 → {len(unprocessed)} 笔订单 (perm_id 聚合)，"
                f"开始 FIFO 匹配..."
            )

            for order in unprocessed:
                perm_id = order["perm_id"]
                exec_id = order["last_exec_id"]
                symbol = order["symbol"]
                f_side = order["side"]
                f_qty = float(order["agg_qty"])        # 聚合总股数
                f_price = float(order["agg_price"])     # VWAP 加权均价
                order_ref = order["order_ref"] or (
    "Breakout" if f_side == "LONG" else "DS"
)
                jf = {
                    "exec_time": order["last_time"] or "",
                    "order_type": order["order_type"] or "",
                    "exchange": order["exchange"] or "",
                    "commissions": float(order["commission"] or 0),
                }

                cur_open = await db.execute(
                    "SELECT id, side, quantity, entry_price, initial_stop, "
                    "current_stop, realized_pnl, setup_tag, create_time "
                    "FROM shadow_ledger WHERE symbol=? AND status='OPEN' "
                    "ORDER BY create_time ASC",
                    (symbol,),
                )
                open_rows = await cur_open.fetchall()

                if not open_rows or open_rows[0]["side"] == f_side:
                    # ── 顺向开仓/加仓兜底：实时监听漏了 → 自动补建 ──
                    tranche_id = f"T_SYNC_{exec_id[-6:]}"
                    cur_ins = await db.execute(
                        "INSERT INTO shadow_ledger "
                        "(symbol, tranche_id, side, quantity, entry_price, "
                        "initial_stop, current_stop, status, setup_tag, "
                        "exec_time, order_type, exchange, commissions) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?)",
                        (
                            symbol, tranche_id, f_side, f_qty, f_price,
                            float(fill["aux_price"] or 0),
                            float(fill["aux_price"] or 0),
                            order_ref,
                            jf["exec_time"], jf["order_type"],
                            jf["exchange"], jf["commissions"],
                        ),
                    )
                    new_tid = cur_ins.lastrowid
                    await db.execute(
                        "UPDATE tws_fills SET processed=1, processed_at=CURRENT_TIMESTAMP "
                        "WHERE perm_id=?",
                        (perm_id,),
                    )
                    await db.commit()  # 释放写锁，允许 enqueue_outbound 写入
                    await enqueue_outbound(
                        f"{new_tid}-OPEN", "notion",
                        {
                            "trade_id": new_tid, "symbol": symbol,
                            "event_type": "OPEN", "side": f_side,
                            "quantity": f_qty, "entry_price": f_price,
                            "initial_stop": float(fill["aux_price"] or 0),
                            "current_stop": float(fill["aux_price"] or 0),
                            "setup_tag": order_ref,
                            "create_time": fill["exec_time"],
                            "spy_context": "",
                        },
                    )
                else:
                    # ── 反向平仓核销 ──
                    remaining_qty = f_qty
                    had_profit = False
                    had_loss = False

                    for row in open_rows:
                        if remaining_qty <= 1e-6:
                            break

                        t_id = row["id"]
                        t_qty = float(row["quantity"])
                        t_entry = float(row["entry_price"])
                        t_side = row["side"]
                        old_pnl = float(row["realized_pnl"] or 0.0)
                        t_stop = float(row["initial_stop"] or 0)
                        t_create = row.get("create_time", "") or ""

                        close_qty = min(t_qty, remaining_qty)
                        if t_side == "LONG":
                            portion_pnl = (f_price - t_entry) * close_qty
                        else:
                            portion_pnl = (t_entry - f_price) * close_qty

                        if portion_pnl > 0:
                            had_profit = True
                        elif portion_pnl < 0:
                            had_loss = True

                        new_pnl = old_pnl + portion_pnl
                        total_pnl += portion_pnl
                        new_qty = t_qty - close_qty
                        is_closed = new_qty <= 1e-6

                        exit_time = (
                            fill.get("exec_time", "") if isinstance(fill, dict)
                            else getattr(fill, "exec_time", "")
                        ) or ""
                        exit_reason, t_r, t_days = compute_close_journal(
                            t_entry, t_stop, close_qty, portion_pnl, t_create, exit_time,
                        )
                        await db.execute(
                            "UPDATE shadow_ledger SET status=?, exit_price=?, "
                            "quantity=?, realized_pnl=?, exit_time=?, "
                            "exit_reason=?, r_multiple=?, holding_days=?, commissions=? "
                            "WHERE id=?",
                            (
                                "CLOSED" if is_closed else "OPEN",
                                f_price, new_qty, new_pnl, exit_time,
                                exit_reason, t_r, t_days, jf["commissions"],
                                t_id,
                            ),
                        )

                        await db.commit()  # 释放写锁
                        event_type = "CLOSE" if is_closed else "UPDATE"
                        await enqueue_outbound(
                            f"{t_id}-{event_type}_TWS_{exec_id[-6:]}", "notion",
                            {
                                "trade_id": t_id, "symbol": symbol,
                                "event_type": event_type,
                                "side": t_side,
                                "quantity": new_qty if not is_closed else t_qty,
                                "entry_price": t_entry,
                                "exit_price": f_price,
                                "realized_pnl": new_pnl,
                                "setup_tag": row["setup_tag"],
                            },
                        )

                        remaining_qty -= close_qty
                        if is_closed:
                            closed_count += 1

                    # ── 卖穿新建反向单 (Overshooting) ──
                    if remaining_qty > 1e-6:
                        os_tid = f"T_OS_{exec_id[-6:]}"
                        cur_os = await db.execute(
                            "INSERT INTO shadow_ledger "
                            "(symbol, tranche_id, side, quantity, entry_price, "
                            "initial_stop, current_stop, status, setup_tag, "
                            "exec_time, order_type, exchange, commissions) "
                            "VALUES (?, ?, ?, ?, ?, 0.0, 0.0, 'OPEN', ?, ?, ?, ?, ?)",
                            (
                                symbol, os_tid, f_side, remaining_qty,
                                f_price, order_ref,
                                jf["exec_time"], jf["order_type"],
                                jf["exchange"], jf["commissions"],
                            ),
                        )
                        new_os_id = cur_os.lastrowid
                        await db.commit()  # 释放写锁
                        await enqueue_outbound(
                            f"{new_os_id}-OPEN", "notion",
                            {
                                "trade_id": new_os_id, "symbol": symbol,
                                "event_type": "OPEN", "side": f_side,
                                "quantity": remaining_qty,
                                "entry_price": f_price,
                                "initial_stop": 0.0, "current_stop": 0.0,
                                "setup_tag": order_ref,
                                "create_time": fill["exec_time"],
                                "spy_context": "",
                            },
                        )

                    await db.execute(
                        "UPDATE tws_fills SET processed=2, "
                        "processed_at=CURRENT_TIMESTAMP WHERE perm_id=?",
                        (perm_id,),
                    )

                    # ── 更新连亏计数器 ──
                    if had_profit:
                        await db.execute(
                            "UPDATE system_state SET value='0' "
                            "WHERE key='consecutive_losses'"
                        )
                    elif had_loss:
                        await db.execute(
                            "UPDATE system_state SET value = "
                            "CAST((CAST(value AS INTEGER) + 1) AS TEXT) "
                            "WHERE key='consecutive_losses'"
                        )
            await db.commit()

        return closed_count, total_pnl

    @ai_trace
    async def settle(self, tg_notify_func, ib) -> dict:
        """双阶段 TWS 原生结算入口。"""
        # Step 0: 数据补漏 + 意图富化
        await self.backfill_fills(ib)
        if ib and ib.isConnected():
            await self.collect_fills(ib)

        # Phase 1: 离线未清算 fills 的关仓核销
        closed_count, total_pnl = await self._phase1_fifo_settle()

        # Phase 2: 物理持仓深度对账（委托 reconcile_physical_positions）
        if ib and ib.isConnected():
            try:
                from reconciliation import reconcile_physical_positions
                logger.info("⚙️ [Phase 2] 开始物理持仓深度同步...")
                await reconcile_physical_positions(ib, tg_notify_func)
            except Exception as e:
                logger.error(f"Phase 2 物理对账异常: {e}")

        # ── 汇报 ──
        if closed_count > 0 and tg_notify_func:
            pnl_str = (
                f"+${total_pnl:.2f}"
                if total_pnl >= 0
                else f"-${abs(total_pnl):.2f}"
            )
            msg = (
                f"✅ **TWS 原生结算完成**\n"
                f"成功关闭 {closed_count} 笔仓位\n"
                f"合计盈亏: {pnl_str}\n"
                f"已通过原生事件流精准归档至 Notion。"
            )
            await tg_notify_func(msg)
        elif closed_count == 0:
            logger.info("📦 [TWS Settlement] 无离线未清算记录，跳过汇报。")

        return {"closed_count": closed_count, "total_pnl": total_pnl}


# ── 模块级入口（兼容 run_flex_settlement 签名）──

@ai_trace
async def run_tws_settlement(tg_notify_func=None, ib=None):
    """TWS 原生结算入口。签名与 run_flex_settlement 完全兼容。"""
    engine = TWSSettlement()
    return await engine.settle(tg_notify_func, ib)
