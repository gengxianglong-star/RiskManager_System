"""TWS 连接与成交监听模块。

负责：连接 TWS、挂载成交/挂单事件、实时同步止损、3 秒延迟捕获
bracket 止损单、拉取 SPY 市场环境、推送全量 OPEN 数据到 Notion。
"""

import asyncio
import datetime
from zoneinfo import ZoneInfo

import aiosqlite
from ib_insync import Stock

from ai_logger import ai_trace, logger
from config import CLIENT_ID, TRADING_TZ, TWS_HOST, resolve_tws_ports
from database import connect_db
from market_regime import fetch_market_regime
from outbound_queue import enqueue_outbound


class IBKRListener:
    """TWS 连接管理器 + 成交监听 + 止损同步 + Notion 全量推送。"""

    def __init__(self, context, gateway):
        self.ctx = context          # RiskManagerApp 实例
        self.gateway = gateway      # TelegramGateway 实例
        self.ib = context.ib        # IB 实例

    # ── 连接管理 ──

    async def ensure_connected(self) -> bool:
        if self.ib.isConnected():
            self._register_event_handlers()
            return True
        preferred, fallback, mode_cn = resolve_tws_ports()
        for port in (preferred, fallback):
            for attempt in range(5):
                try:
                    await self.ib.connectAsync(TWS_HOST, port, clientId=CLIENT_ID, timeout=15)
                    self.ctx.active_tws_port = port
                    used_fallback = port != preferred
                    suffix = " (备用端口)" if used_fallback else ""
                    logger.info(
                        f"✅ TWS 已连接：{mode_cn} {TWS_HOST}:{port}{suffix} (clientId={CLIENT_ID})"
                    )
                    self._register_event_handlers()
                    return True
                except Exception as e:
                    err_msg = str(e)
                    if "already in use" in err_msg.lower() or "已被使用" in err_msg:
                        wait = 60
                        logger.warning(
                            f"TWS {TWS_HOST}:{port} Client ID 被占用，{wait}s 后重试 ({attempt + 1}/5)..."
                        )
                    else:
                        wait = 10
                        logger.warning(
                            f"TWS {TWS_HOST}:{port} 连接失败({type(e).__name__})，{wait}s 后重试 ({attempt + 1}/5)"
                        )
                    await asyncio.sleep(wait)
        self.ctx.active_tws_port = None
        return False

    def _register_event_handlers(self) -> None:
        if not self.ib.isConnected():
            return
        self.ib.reqAutoOpenOrders(True)
        if self.on_execution not in self.ib.execDetailsEvent:
            self.ib.execDetailsEvent += self.on_execution
        if self.on_open_order not in self.ib.openOrderEvent:
            self.ib.openOrderEvent += self.on_open_order

    # ── 探活守护 ──

    async def keepalive_daemon(self):
        """探活与重连守护进程。"""
        while True:
            try:
                preferred, _, mode_cn = resolve_tws_ports()
                if (
                    self.ib.isConnected()
                    and self.ctx.active_tws_port is not None
                    and self.ctx.active_tws_port != preferred
                ):
                    logger.info(f"检测到桌面端切换为{mode_cn}，重连 TWS {TWS_HOST}:{preferred}…")
                    self.ib.disconnect()
                    self.ctx.active_tws_port = None
                    self.ctx.tws_online_notified = False
                was_disconnected = not self.ib.isConnected()
                if was_disconnected:
                    if await self.ensure_connected():
                        if not self.ctx.tws_online_notified:
                            self.ctx.tws_online_notified = True
                            status = "\n".join(
                                await self.gateway.build_service_status_lines()
                            ).strip()
                            await self.gateway.notify_user(
                                f"🟢 **风控军师系统已重新上线** (TWS 重连)\n\n"
                                f"{status}\n\n"
                                f"👀 成交监听已挂载，桌面端下单成交后将自动入账。"
                            )
            except Exception as e:
                logger.error(f"🚨 IB 探活守护进程异常: {e}")
            await asyncio.sleep(15)

    async def _on_tws_reconnect(self):
        """TWS 恢复连接后自动补齐：仓位对账 + Flex 交易记录。"""
        logger.info("🔄 TWS 重连：开始自动补齐...")
        await asyncio.sleep(2)
        try:
            from reconciliation import reconcile_physical_positions
            await reconcile_physical_positions(self.ib, self.gateway.notify_user)
        except Exception as e:
            logger.error(f"重连对账失败: {e}")
        self.ctx.spawn_background_task(self.ctx.sync_flex_query_job())

    # ── 行情拉取 ──

    async def fetch_entry_price(self, symbol: str):
        if not await self.ensure_connected():
            return None, ""
        contract = Stock(symbol.upper(), "SMART", "USD")
        qualified = await self.ib.qualifyContractsAsync(contract)
        if not qualified:
            return None, ""

        # 🚀 强制快照拉取，规避 NaN / 过期缓存
        try:
            ticker = await self.ib.reqMktDataAsync(
                contract, "", True, False, timeout=3.0
            )
            if ticker:
                if ticker.last and ticker.last > 0:
                    return float(ticker.last), "last"
                if ticker.close and ticker.close > 0:
                    return float(ticker.close), "close"
        except Exception:
            pass

        # 降级：快照失败时回退到 reqTickersAsync
        tickers = await self.ib.reqTickersAsync(contract)
        if tickers:
            ticker = tickers[0]
            if ticker.ask and ticker.ask > 0:
                return float(ticker.ask), "ask"
            if ticker.last and ticker.last > 0:
                return float(ticker.last), "last"
            if ticker.close and ticker.close > 0:
                return float(ticker.close), "close"
            if ticker.marketPrice() > 0:
                return float(ticker.marketPrice()), "market"
        return None, ""

    async def fetch_account_equity(self) -> float:
        if not await self.ensure_connected():
            return 0.0
        try:
            tags = await self.ib.accountSummaryAsync()
            for row in tags:
                if row.tag == "NetLiquidation" and row.currency == "USD":
                    try:
                        return float(row.value)
                    except ValueError:
                        pass
        except Exception as e:
            logger.warning(f"净值读取失败: {e}")
        return 0.0

    # ── 异步调度 ──

    def _dispatch_async(self, coro):
        """修复 IB 底层线程触发异步任务导致的 RuntimeError。"""
        try:
            asyncio.get_running_loop()
            self.ctx.spawn_background_task(coro)
        except RuntimeError:
            loop = asyncio.get_event_loop()
            loop.call_soon_threadsafe(self.ctx.spawn_background_task, coro)

    # ═══════════════════════════════════════════════════════════
    # 成交事件处理
    # ═══════════════════════════════════════════════════════════

    def on_execution(self, trade, fill):
        sym = fill.contract.symbol if fill.contract else "?"
        logger.info(
            f"📡 [成交监听] {sym} {fill.execution.side} {fill.execution.shares}@{fill.execution.price}"
        )
        self._dispatch_async(self._async_on_execution(trade, fill))

    @ai_trace
    async def _async_on_execution(self, trade, fill):
        execution = fill.execution
        contract = fill.contract
        symbol = contract.symbol
        side = "LONG" if execution.side == "BOT" else "SHORT"
        qty = float(execution.shares)
        price = float(execution.price)
        setup_tag = trade.order.orderRef if trade.order and trade.order.orderRef else ""

        # ── Kill Switch 平仓 ──
        if setup_tag == "KILL_SWITCH":
            logger.info(f"🛡️ 侦测到 Kill Switch 的平仓回报 [{symbol}]，开始清理账本...")
            lock = self.ctx.get_symbol_lock(symbol)
            async with lock:
                async with connect_db() as conn:
                    await conn.execute(
                        "UPDATE shadow_ledger SET status='CLOSED', exit_price=? "
                        "WHERE symbol=? AND status='OPEN'",
                        (price, symbol),
                    )
                    await conn.commit()
            return

        lock = self.ctx.get_symbol_lock(symbol)
        async with lock:
            async with connect_db() as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(
                    "SELECT id, side, quantity, entry_price, setup_tag FROM shadow_ledger "
                    "WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC",
                    (symbol,),
                )
                open_tranches = await cursor.fetchall()
                opening = not open_tranches or open_tranches[0]["side"] == side

                if opening:
                    # ── 新开仓 / 同日加仓 ──
                    stop_for_val = 0.0
                    if self.ib.isConnected():
                        for open_trade in self.ib.openTrades():
                            if (
                                open_trade.contract.symbol == symbol
                                and open_trade.order.orderType in ("STP", "STP LMT")
                            ):
                                stop_for_val = float(open_trade.order.auxPrice)
                                break

                    equity = await self.fetch_account_equity()
                    reject_reason = await self.ctx.risk_engine.validate_pending_entry(
                        conn, price, stop_for_val, qty, equity, is_buy=(side == "LONG")
                    )
                    if reject_reason:
                        await self.gateway.notify_user(
                            f"🛑 **UI 开仓被风控驳回** `{symbol}`\n{reject_reason}\n\n"
                            f"⚠️ 系统正在强制执行【斩立决】，清理该笔违规物理持仓！"
                        )
                        kill_res = await self.ctx.risk_engine.execute_kill_switch(
                            symbol, trigger_reason="前端 UI 违规开仓被驳回"
                        )
                        await self.gateway.notify_user(kill_res)
                        return

                    tz = ZoneInfo(TRADING_TZ)
                    now = datetime.datetime.now(tz)
                    day_start = datetime.datetime.combine(now.date(), datetime.time.min, tzinfo=tz)
                    day_end = day_start + datetime.timedelta(days=1)
                    start_utc = day_start.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    end_utc = day_end.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    cursor = await conn.execute(
                        "SELECT id, quantity, entry_price, setup_tag FROM shadow_ledger "
                        "WHERE symbol=? AND side=? AND status='OPEN' AND create_time >= ? AND create_time < ?",
                        (symbol, side, start_utc, end_utc),
                    )
                    same_day_row = await cursor.fetchone()

                    if same_day_row:
                        # 同日加仓：合并
                        old_qty = float(same_day_row["quantity"])
                        old_entry = float(same_day_row["entry_price"])
                        old_tag = same_day_row["setup_tag"] or ""
                        new_qty = old_qty + qty
                        new_entry = (old_qty * old_entry + qty * price) / new_qty
                        if setup_tag and setup_tag not in old_tag:
                            merged_tag = f"{old_tag},{setup_tag}".strip(",")
                        else:
                            merged_tag = old_tag

                        # 🚀 Minervini 铁律：侦测向下摊平 (Averaging Down)
                        is_averaging_down = (
                            (side == "LONG" and price < old_entry)
                            or (side == "SHORT" and price > old_entry)
                        )

                        if stop_for_val > 0:
                            await conn.execute(
                                "UPDATE shadow_ledger "
                                "SET quantity=?, entry_price=?, setup_tag=?, current_stop=?, initial_stop=? "
                                "WHERE id=?",
                                (new_qty, new_entry, merged_tag, stop_for_val, stop_for_val, same_day_row["id"]),
                            )
                            stop_msg = f"已同步最新防线: ${stop_for_val:.2f}"
                        else:
                            await conn.execute(
                                "UPDATE shadow_ledger SET quantity=?, entry_price=?, setup_tag=? WHERE id=?",
                                (new_qty, new_entry, merged_tag, same_day_row["id"]),
                            )
                            stop_msg = "⚠️ 警告: 未侦测到新止损单，请手动在 TWS 补齐防线"

                        violation_warning = (
                            "\n⚠️ **严重纪律违规：侦测到向下摊平 (Averaging Down)！**"
                            if is_averaging_down else ""
                        )
                        await conn.commit()
                        msg = (
                            f"🎯 **前端火力捕获 (同日加仓)**\n"
                            f"已接管来自 UI 的加仓指令：`{symbol}`\n"
                            f"新增: {qty:.0f}股 @ ${price:.2f} (均价拉至 ${new_entry:.2f})\n"
                            f"策略: {merged_tag or '未打标'}\n{stop_msg}"
                            f"{violation_warning}"
                        )
                        await enqueue_outbound(f"{same_day_row['id']}-ADD", "telegram", {"message": msg})

                        # 违规标记写入 Notion
                        if is_averaging_down:
                            await enqueue_outbound(
                                f"{same_day_row['id']}-VIOLATION", "notion",
                                {
                                    "trade_id": same_day_row["id"],
                                    "symbol": symbol,
                                    "event_type": "UPDATE",
                                    "confession": "系统侦测：逆势向下摊平(Averaging Down)",
                                },
                            )
                    else:
                        # 全新开仓
                        tranche_id = f"T{len(open_tranches) + 1}"
                        cursor = await conn.execute(
                            "INSERT INTO shadow_ledger "
                            "(symbol, tranche_id, side, quantity, entry_price, initial_stop, current_stop, status, setup_tag) "
                            "VALUES (?, ?, ?, ?, ?, 0.0, 0.0, 'OPEN', ?)",
                            (symbol, tranche_id, side, qty, price, setup_tag or ("Breakout" if side == "LONG" else "DS")),
                        )
                        new_trade_id = cursor.lastrowid
                        await conn.commit()
                        await enqueue_outbound(
                            f"{new_trade_id}-OPEN", "telegram",
                            {"message": (
                                f"🎯 **前端火力捕获 (新开仓)**\n"
                                f"已接管来自 UI 的开仓指令：`{symbol}`\n"
                                f"成交: {qty:.0f}股 @ ${price:.2f}\n"
                                f"策略: {setup_tag or '未打标'}\n*(止损线将在 3 秒内自动同步防线)*"
                            )},
                        )
                        # 🚀 核心：触发 3 秒延迟止损捕获 → Notion 全量推送
                        self.ctx.spawn_background_task(
                            self._delayed_bracket_stop_capture(symbol, new_trade_id)
                        )
                else:
                    # ── 平仓（FIFO 核销）──
                    had_profit = False
                    had_loss = False
                    remaining_exit_qty = qty
                    # 🚀 浮点 epsilon 安全边界：防止 IEEE 754 精度穿透
                    EPS = 1e-6
                    for row in open_tranches:
                        if remaining_exit_qty <= EPS:
                            break
                        t_id = row["id"]
                        t_qty = float(row["quantity"])
                        t_entry = float(row["entry_price"])
                        tranche_side = row["side"]

                        if t_qty <= remaining_exit_qty + EPS:
                            cursor = await conn.execute(
                                "SELECT setup_tag FROM shadow_ledger WHERE id=?", (t_id,)
                            )
                            tag_row = await cursor.fetchone()
                            s_tag = tag_row["setup_tag"] if tag_row and tag_row["setup_tag"] else ""
                            if tranche_side == "LONG":
                                actual_pnl = (price - t_entry) * t_qty
                            else:
                                actual_pnl = (t_entry - price) * t_qty
                            if actual_pnl < 0:
                                had_loss = True
                            elif actual_pnl > 0:
                                had_profit = True
                            await conn.execute(
                                "UPDATE shadow_ledger SET status='CLOSED', exit_price=?, realized_pnl=? WHERE id=?",
                                (price, actual_pnl, t_id),
                            )
                            pnl_sign = "+" if actual_pnl > 0 else ""
                            close_msg = (
                                f"📤 **平仓确认** `{symbol}`\n"
                                f"{tranche_side} {t_qty:.0f}股 @ ${price:.2f} | "
                                f"盈亏 {pnl_sign}${actual_pnl:.2f}"
                            )
                            await enqueue_outbound(f"{t_id}-CLOSE", "telegram", {"message": close_msg})
                            await enqueue_outbound(
                                f"{t_id}-CLOSE", "notion",
                                {
                                    "trade_id": t_id, "symbol": symbol, "event_type": "CLOSE",
                                    "side": tranche_side, "quantity": t_qty,
                                    "entry_price": t_entry, "exit_price": price,
                                    "realized_pnl": actual_pnl, "setup_tag": s_tag or "",
                                },
                            )
                            remaining_exit_qty -= t_qty
                        else:
                            # 部分平仓
                            new_qty = t_qty - remaining_exit_qty
                            is_profitable_trim = (
                                (tranche_side == "LONG" and price > t_entry)
                                or (tranche_side == "SHORT" and price < t_entry)
                            )
                            if is_profitable_trim:
                                had_profit = True
                                await conn.execute(
                                    "UPDATE shadow_ledger SET quantity=?, current_stop=? WHERE id=?",
                                    (new_qty, t_entry, t_id),
                                )
                                trim_detail = (
                                    f"成交价: ${price:.2f} > 成本价: ${t_entry:.2f}"
                                    if tranche_side == "LONG"
                                    else f"成交价: ${price:.2f} < 成本价: ${t_entry:.2f}"
                                )
                                msg = (
                                    f"🤖 **自动护航：** 侦测到 `{symbol}` 盈利减仓！\n{trim_detail}\n"
                                    f"系统已自动将剩余 {new_qty:.0f} 股止损推至成本价 ${t_entry:.2f}。\n"
                                    f"🛡️ **该笔交易风控额度已完全释放！**"
                                )
                                await enqueue_outbound(
                                    f"{t_id}-PARTIAL_CLOSE", "telegram", {"message": msg},
                                )
                            else:
                                if (tranche_side == "LONG" and price < t_entry) or (
                                    tranche_side == "SHORT" and price > t_entry
                                ):
                                    had_loss = True
                                await conn.execute(
                                    "UPDATE shadow_ledger SET quantity=? WHERE id=?", (new_qty, t_id)
                                )
                            remaining_exit_qty = 0

                    # ── 连亏追踪 ──
                    await _apply_consecutive_losses(conn, had_profit, had_loss)
                await conn.commit()

            # ── 平仓后触发守夜人 ──
            if not opening:
                self.ctx.spawn_background_task(
                    self.ctx.risk_engine.night_watchman_on_tp(trade, execution)
                )

    # ═══════════════════════════════════════════════════════════
    # 🚀 核心：3 秒延迟止损捕获 → Notion 全量推送
    # ═══════════════════════════════════════════════════════════

    @ai_trace
    async def _delayed_bracket_stop_capture(self, symbol: str, trade_id: int = 0):
        """开仓后等待 3 秒，等 TWS 生成 bracket 止损单，然后一次性抓取全量数据推给 Notion。

        这是 Telegram 显示止损 + Notion 写入完整 OPEN 记录的关键入口。
        """
        await asyncio.sleep(3.0)
        try:
            # 🚀 极速翻仓拦截：3 秒后复查仓位是否还活着
            async with connect_db() as _check_conn:
                _check_conn.row_factory = aiosqlite.Row
                _check_cur = await _check_conn.execute(
                    "SELECT status, quantity FROM shadow_ledger WHERE id=?", (trade_id,)
                )
                _pos = await _check_cur.fetchone()
                if not _pos or _pos["status"] != "OPEN" or float(_pos["quantity"]) <= 1e-6:
                    logger.warning(
                        f"🛑 [极速翻仓拦截] {symbol} (ID:{trade_id}) 在 3 秒内已被平仓，"
                        f"取消发送 Notion OPEN 事件。"
                    )
                    return

            # 🛡️ 防抖去重：部分成交会产生多个并发协程，检查 OPEN 任务是否已入队
            async with connect_db() as _dedup_conn:
                _dedup_cur = await _dedup_conn.execute(
                    "SELECT id FROM outbound_queue WHERE event_key=? AND channel='notion'",
                    (f"{trade_id}-OPEN",),
                )
                if await _dedup_cur.fetchone():
                    logger.debug(
                        f"🛑 [防抖拦截] {symbol} (ID:{trade_id}) 的 OPEN 已在出站队列中，"
                        f"跳过本次部分成交触发。"
                    )
                    return

            # 1. 抓取 TWS 止损单
            found_stop = 0.0
            if self.ib.isConnected():
                for open_trade in self.ib.openTrades():
                    if (
                        open_trade.contract.symbol == symbol
                        and open_trade.order.orderType in ("STP", "STP LMT")
                    ):
                        found_stop = float(open_trade.order.auxPrice)
                        break

            # 2. 拉取 SPY 市场环境
            spy_ctx = ""
            try:
                label, _, _ = await fetch_market_regime()
                spy_ctx = label if label else ""
            except Exception:
                pass

            async with connect_db() as conn:
                conn.row_factory = aiosqlite.Row

                # 3. 更新止损到数据库
                if found_stop > 0:
                    cursor = await conn.execute(
                        "UPDATE shadow_ledger SET current_stop=?, initial_stop=? "
                        "WHERE symbol=? AND status='OPEN' AND initial_stop=0.0",
                        (found_stop, found_stop, symbol),
                    )
                    if cursor.rowcount > 0:
                        await conn.commit()
                        msg = (
                            f"🛡️ **防线主动同步完毕：** `{symbol}` "
                            f"的底层止损已被锚定在 ${found_stop:.2f}。"
                        )
                        await enqueue_outbound(
                            f"{trade_id}-STOP_SYNCED-{found_stop:.0f}",
                            "telegram", {"message": msg},
                        )

                # 4. 获取完整数据，组装 Notion 全量 Payload
                tid = trade_id
                if tid <= 0:
                    cur2 = await conn.execute(
                        "SELECT id FROM shadow_ledger "
                        "WHERE symbol=? AND status='OPEN' ORDER BY create_time DESC LIMIT 1",
                        (symbol,),
                    )
                    row = await cur2.fetchone()
                    if row:
                        tid = row["id"]

                if tid > 0:
                    cur3 = await conn.execute(
                        "SELECT side, quantity, entry_price, setup_tag, create_time "
                        "FROM shadow_ledger WHERE id=?",
                        (tid,),
                    )
                    pos_row = await cur3.fetchone()
                    if pos_row:
                        db_stop = found_stop if found_stop > 0 else 0.0
                        # 🚀 全量 Notion OPEN：包含 SPY、止损、创建时间、风险金额
                        await enqueue_outbound(
                            f"{tid}-OPEN", "notion",
                            {
                                "trade_id": tid,
                                "symbol": symbol,
                                "event_type": "OPEN",
                                "side": pos_row["side"],
                                "quantity": float(pos_row["quantity"]),
                                "entry_price": float(pos_row["entry_price"]),
                                "initial_stop": db_stop,
                                "current_stop": db_stop,
                                "setup_tag": (
                                    pos_row["setup_tag"]
                                    if pos_row["setup_tag"] in ("Breakout", "EP", "PB", "DS")
                                    else ("Breakout" if pos_row["side"] == "LONG" else "DS")
                                ),
                                "create_time": pos_row["create_time"] or "",
                                "spy_context": spy_ctx,
                            },
                        )
        except Exception as e:
            logger.error(f"延迟捕获止损/Notion同步出错: {e}")

    # ═══════════════════════════════════════════════════════════
    # 挂单修改事件（TWS 图表手动拖拽止损）
    # ═══════════════════════════════════════════════════════════

    def on_open_order(self, trade):
        self._dispatch_async(self._async_on_open_order(trade))

    @ai_trace
    async def _async_on_open_order(self, trade):
        """捕获 TWS 图表上手動拖拽止损单的动作，同步更新影子账本。"""
        try:
            order = trade.order
            contract = trade.contract
            symbol = contract.symbol
            if order.orderType not in ("STP", "STP LMT"):
                return
            new_stop = float(order.auxPrice)
            if new_stop <= 0:
                return

            async with connect_db() as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(
                    "SELECT id, current_stop, entry_price FROM shadow_ledger "
                    "WHERE symbol=? AND status='OPEN'",
                    (symbol,),
                )
                rows = await cursor.fetchall()
                if not rows:
                    return
                old_stop = float(rows[0]["current_stop"])
                entry_price = float(rows[0]["entry_price"])
                if abs(new_stop - old_stop) <= 0.001:
                    return

                await conn.execute(
                    """
                    UPDATE shadow_ledger
                    SET current_stop=?,
                        initial_stop = CASE WHEN initial_stop = 0.0 THEN ? ELSE initial_stop END
                    WHERE symbol=? AND status='OPEN'
                    """,
                    (new_stop, new_stop, symbol),
                )
                await conn.commit()

            risk_status = (
                "✅ 止损已推至成本区以上，零风险/锁定利润！"
                if new_stop >= entry_price
                else f"📐 当前风险距离: {abs(entry_price - new_stop) / entry_price * 100:.2f}%"
            )
            msg = (
                f"🖱️ **TWS 图表同步捕获：** `{symbol}`\n"
                f"侦测到止损单修改：${old_stop:.2f} ➡️ **${new_stop:.2f}**\n"
                f"影子账本已自动同步更新。\n{risk_status}"
            )
            await self.gateway.notify_user(msg)
        except Exception as e:
            logger.error(f"图表同步止损失败: {e}")


# ── 连亏追踪工具函数 ──

async def _apply_consecutive_losses(conn, profitable: bool, losing: bool) -> None:
    """更新连亏计数器：盈利归零，亏损 +1。"""
    if profitable:
        await conn.execute(
            "UPDATE system_state SET value='0' WHERE key='consecutive_losses'"
        )
    elif losing:
        await conn.execute(
            "UPDATE system_state SET value = CAST((CAST(value AS INTEGER) + 1) AS TEXT) "
            "WHERE key='consecutive_losses'"
        )
