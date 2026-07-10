"""物理对账引擎：幽灵单清理、自动收编、止损单同步。

从 main.py 解耦，专门处理账本与 TWS 物理仓位的一致性校验。
"""

import asyncio

import aiosqlite

from database import connect_db, count_open_tranches
from logger import logger
from ai_logger import ai_trace
from outbound_queue import enqueue_outbound


def _signed_ledger_qty(side: str, quantity: float) -> float:
    return quantity if str(side).upper() == "LONG" else -quantity


def _fmt_signed_position(qty: float) -> str:
    if abs(qty) < 1e-6:
        return "0 股"
    return f"{abs(qty):.0f} 股 {'多' if qty > 0 else '空'}"


async def _close_ledger_discrepancy(
    conn: aiosqlite.Connection, symbol: str, discrepancy: float
) -> None:
    """按 FIFO 削减账本仓位；discrepancy = 账本 signed - TWS signed。

    优先从 tws_fills 查找对应成交价自算 P&L，找不到则标记为 RECONCILE_MISS。
    """
    if abs(discrepancy) < 1e-6:
        return

    import datetime

    side_to_close = "LONG" if discrepancy > 0 else "SHORT"
    close_side = "SHORT" if side_to_close == "LONG" else "LONG"
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # 查询 tws_fills 中该 symbol 的平仓方向成交（按时间降序，最近的优先）
    fills_cursor = await conn.execute(
        "SELECT quantity, price, exec_time, order_ref FROM tws_fills "
        "WHERE symbol=? AND side=? AND processed > 0 "
        "ORDER BY exec_time DESC",
        (symbol, close_side),
    )
    fills = await fills_cursor.fetchall()

    remaining = abs(discrepancy)
    cursor = await conn.execute(
        "SELECT id, quantity, entry_price, realized_pnl FROM shadow_ledger "
        "WHERE symbol=? AND status='OPEN' AND side=? ORDER BY create_time ASC",
        (symbol, side_to_close),
    )
    for tranche in await cursor.fetchall():
        if remaining <= 0:
            break
        t_id = tranche["id"]
        t_qty = float(tranche["quantity"])
        t_entry = float(tranche["entry_price"])
        old_pnl = float(tranche["realized_pnl"] or 0)

        if t_qty <= remaining + 1e-6:
            # ── 尝试从 tws_fills 找成交价算 P&L ──
            exit_px, pnl, exit_time = _best_effort_pnl(
                fills, t_entry, side_to_close, t_qty
            )
            await conn.execute(
                "UPDATE shadow_ledger SET status='CLOSED', exit_price=?, "
                "realized_pnl=COALESCE(realized_pnl,0)+?, "
                "exit_reason='RECONCILE_CLOSE', exit_time=? WHERE id=?",
                (exit_px, pnl, exit_time or now_iso, t_id),
            )
            remaining -= t_qty
        else:
            await conn.execute(
                "UPDATE shadow_ledger SET quantity=? WHERE id=?",
                (t_qty - remaining, t_id),
            )
            remaining = 0


def _best_effort_pnl(
    fills: list, entry_price: float, pos_side: str, close_qty: float
) -> tuple[float, float, str]:
    """从 tws_fills 列表中查找最近的成交价，自算 P&L。

    Returns: (exit_price, pnl, exit_time_iso)
    """
    if fills:
        best = fills[0]
        exit_px = float(best["price"]) if isinstance(best, dict) else float(best[1])
        exit_time = (
            best["exec_time"] if isinstance(best, dict) else best[2]
        ) or ""
        if pos_side == "LONG":
            pnl = (exit_px - entry_price) * close_qty
        else:
            pnl = (entry_price - exit_px) * close_qty
        return exit_px, pnl, exit_time

    # 找不到任何 fill → 保留 entry_price 为参考，P&L=0，标记为 MISS
    return entry_price, 0.0, ""


@ai_trace
async def reconcile_physical_positions(ib, notify_func=None):
    """
    物理仓位深度对账引擎。

    Parameters
    ----------
    ib: ib_insync.IB 实例
    notify_func: 用于发送 Telegram 警告的异步回调函数，例如 tg_gateway.notify_user
    """
    logger.info("启动物理仓位深度对账...")
    if not ib.isConnected():
        logger.warning("TWS 未连接，跳过物理对账。")
        return

    try:
        positions = await asyncio.wait_for(ib.reqPositionsAsync(), timeout=15)
    except asyncio.TimeoutError:
        logger.warning("物理持仓拉取超时 (15s)，跳过对账。")
        return
    except Exception as e:
        logger.error(f"物理持仓拉取失败: {e}")
        return

    physical_inventory: dict[str, float] = {}
    for pos in positions:
        if pos.contract.secType == "STK" and pos.position != 0:
            physical_inventory[pos.contract.symbol] = float(pos.position)

    async with connect_db() as conn:
        conn.row_factory = aiosqlite.Row

        cursor = await conn.execute(
            "SELECT symbol, quantity, side FROM shadow_ledger WHERE status='OPEN'"
        )
        ledger_positions = await cursor.fetchall()

        expected_inventory: dict[str, float] = {}
        for row in ledger_positions:
            sym = row["symbol"]
            signed = _signed_ledger_qty(row["side"], float(row["quantity"]))
            expected_inventory[sym] = expected_inventory.get(sym, 0.0) + signed

        ghost_alerts: list[str] = []
        all_symbols = set(physical_inventory) | set(expected_inventory)

        for sym in sorted(all_symbols):
            actual = physical_inventory.get(sym, 0.0)
            expected = expected_inventory.get(sym, 0.0)
            if abs(actual - expected) < 1e-6:
                continue

            discrepancy = expected - actual
            same_direction = actual == 0 or expected == 0 or actual * expected > 0
            ledger_overstates = (
                expected != 0
                and same_direction
                and abs(expected) > abs(actual) + 1e-6
            )

            if ledger_overstates:
                ghost_alerts.append(
                    f"👻 **发现幽灵平仓 [{sym}]**\n"
                    f"账本预期: {_fmt_signed_position(expected)} | "
                    f"TWS实际: {_fmt_signed_position(actual)}\n"
                    f"*(系统已强制启动 FIFO 清剿修复)*"
                )
                await _close_ledger_discrepancy(conn, sym, discrepancy)
            else:
                # ── 自动导入：TWS 有新仓位但账本没有 ──
                auto_imported = []
                for pos in positions:
                    if pos.contract.symbol == sym and pos.contract.secType == "STK" and pos.position != 0:
                        raw_qty = float(pos.position)
                        auto_side = "LONG" if raw_qty > 0 else "SHORT"
                        auto_qty = abs(raw_qty)
                        auto_entry = float(pos.avgCost)
                        if auto_entry <= 0:
                            continue
                        auto_tag = "Breakout" if auto_side == "LONG" else "DS"
                        auto_exec_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
                        tranche_id = f"T{await count_open_tranches(conn, sym) + 1}"
                        cursor = await conn.execute(
                            "INSERT INTO shadow_ledger "
                            "(symbol, tranche_id, side, quantity, entry_price, initial_stop, current_stop, status, setup_tag, exec_time) "
                            "VALUES (?, ?, ?, ?, ?, 0.0, 0.0, 'OPEN', ?, ?)",
                            (sym, tranche_id, auto_side, auto_qty, auto_entry, auto_tag, auto_exec_time),
                        )
                        new_trade_id = cursor.lastrowid
                        await conn.commit()  # 释放写锁后再调用出站队列，防止 database is locked
                        auto_imported.append(
                            f"📥 {auto_side} {sym} {auto_qty:.0f}股 @ ${auto_entry:.2f}（自动收编）"
                        )
                        # 🚀 自动收编也走统一出站：Notion + Telegram 双通道
                        await enqueue_outbound(
                            f"{new_trade_id}-OPEN", "telegram",
                            {"message": f"📥 系统自动收编: {auto_side} {sym} {auto_qty:.0f}股 @ ${auto_entry:.2f}\n(请在 Notion 补齐止损后，通过 TWS 图表设置止损单)"}
                        )
                        await enqueue_outbound(
                            f"{new_trade_id}-OPEN", "notion",
                            {
                                "trade_id": new_trade_id,
                                "symbol": sym,
                                "event_type": "OPEN",
                                "side": auto_side,
                                "quantity": auto_qty,
                                "entry_price": auto_entry,
                                "initial_stop": 0.0,
                                "current_stop": 0.0,
                                "setup_tag": auto_tag,
                                "create_time": "",
                                "spy_context": "",
                            },
                        )
                ghost_alerts.extend(auto_imported)

        # ── 同步 TWS 进场价到 IMPORT 仓位（修正拆股等导致的价格漂移）──
        price_fix_log: list[str] = []
        for pos in positions:
            if pos.position == 0 or pos.contract.secType != "STK":
                continue
            sym = pos.contract.symbol
            tws_avg = float(pos.avgCost)
            if tws_avg <= 0:
                continue
            cursor = await conn.execute(
                "SELECT id, entry_price, setup_tag FROM shadow_ledger "
                "WHERE symbol=? AND status='OPEN'",
                (sym,),
            )
            db_rows = await cursor.fetchall()
            for db_row in db_rows:
                db_entry = float(db_row["entry_price"])
                tag = db_row["setup_tag"] or ""
                if abs(tws_avg - db_entry) / max(tws_avg, 0.01) > 0.01:
                    if "IMPORT" in tag:
                        await conn.execute(
                            "UPDATE shadow_ledger SET entry_price=? WHERE id=?",
                            (tws_avg, db_row["id"]),
                        )
                        price_fix_log.append(
                            f"📐 {sym} 进场价已修正: ${db_entry:.2f} → ${tws_avg:.2f}"
                        )
                    else:
                        price_fix_log.append(
                            f"⚠️ {sym} 进场价偏差: 账本 ${db_entry:.2f} vs TWS ${tws_avg:.2f}（非 IMPORT，未自动修正）"
                        )

        # ── 兜底同步 TWS 止损单到影子账本 + 补推 Notion OPEN ──
        stop_sync_log: list[str] = []
        try:
            open_trades = ib.openTrades()
            for trade in open_trades:
                o = trade.order
                if o.orderType not in ("STP", "STP LMT", "TRAIL", "TRAIL LIMIT"):
                    continue
                # ── 放宽状态检查：只要不是已取消/已成交，都视为有效止损 ──
                _skip_statuses = {"Cancelled", "Filled"}
                if trade.orderStatus.status in _skip_statuses:
                    continue
                sym = trade.contract.symbol
                stop_price = float(o.auxPrice) if o.auxPrice > 0 else float(o.lmtPrice)
                if stop_price <= 0:
                    continue

                cursor = await conn.execute(
                    "SELECT id, side, quantity, entry_price, initial_stop, current_stop, "
                    "setup_tag, create_time "
                    "FROM shadow_ledger WHERE symbol=? AND status='OPEN'",
                    (sym,),
                )
                open_rows = await cursor.fetchall()
                if not open_rows:
                    continue

                if len(open_rows) > 1:
                    logger.warning(
                        f"⚠️ {sym} 存在 {len(open_rows)} 笔不同批次的持仓，"
                        f"TWS 单一止损单兜底同步将覆盖所有批次的止损防线！"
                    )

                for row in open_rows:
                    entry = float(row["entry_price"])
                    old_init = float(row["initial_stop"])
                    # ── 只在 initial_stop 确实为 0 时用 TWS 止损初始化 ──
                    new_init = stop_price if old_init == 0.0 else old_init
                    if float(row["current_stop"]) != stop_price or old_init != new_init:
                        await conn.execute(
                            "UPDATE shadow_ledger SET current_stop=?, initial_stop=? WHERE id=?",
                            (stop_price, new_init, row["id"]),
                        )
                        await conn.commit()  # 释放写锁后再调用出站队列，防止 database is locked
                        stop_sync_log.append(
                            f"🛡️ {sym} 止损已兜底同步: "
                            + (f"${stop_price:.2f} (新增防线)" if old_init == 0.0 else f"${stop_price:.2f}")
                        )
                        # 🚀 兜底补推 Notion OPEN：包含全量字段（止损/SPY/创建时间）
                        await enqueue_outbound(
                            f"{row['id']}-OPEN", "notion",
                            {
                                "trade_id": row["id"],
                                "symbol": sym,
                                "event_type": "OPEN",
                                "side": row["side"],
                                "quantity": float(row["quantity"]),
                                "entry_price": float(row["entry_price"]),
                                "initial_stop": new_init,
                                "current_stop": stop_price,
                                "setup_tag": row["setup_tag"] or "",
                                "create_time": row["create_time"] or "",
                            },
                        )
        except Exception as e:
            logger.warning(f"止损单兜底同步失败: {e}")

        await conn.commit()

        # ── 汇总通知 ──
        if ghost_alerts or stop_sync_log or price_fix_log:
            parts: list[str] = []
            if ghost_alerts:
                parts.append("🚨 **物理对账警告 (已处理)** 🚨\n\n" + "\n\n".join(ghost_alerts))
            if price_fix_log:
                parts.append("📐 **进场价自动修正**\n\n" + "\n".join(price_fix_log))
            if stop_sync_log:
                parts.append("🛡️ **TWS 止损单兜底同步**\n\n" + "\n".join(stop_sync_log))

            alert_msg = "\n\n".join(parts)
            logger.info("对账结果汇总触发推送。")
            if notify_func:
                await notify_func(alert_msg)
        else:
            logger.info("✅ 物理仓位与影子账本 100% 吻合。")
