import asyncio
import datetime
import sys
import time
import uuid
from functools import wraps
from zoneinfo import ZoneInfo

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import aiosqlite
import pandas as pd
import pandas_market_calendars as mcal
import yfinance as yf
from ib_insync import IB, FlexReport, MarketOrder, Stock, util
from telegram import (
    Bot,
    BotCommand,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeDefault,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes


from config import (
    CLIENT_ID,
    ENABLE_EOD_SNIPER,
    FLEX_QUERY_ID,
    FLEX_TOKEN,
    MAX_POSITION_SIZE_PCT,
    MAX_STOP_PCT,
    MAX_DAILY_TRADES,
    MAX_OVERNIGHT_RISK_PCT,
    MY_TELEGRAM_CHAT_ID,
    RISK_MAX_DRAWDOWN_GREEN,
    RISK_MAX_DRAWDOWN_YELLOW,
    RISK_PCT_PER_TRADE,
    TG_BOT_TOKEN,
    TRADING_TZ,
    TWS_HOST,
    resolve_tws_ports,
)
from database import (
    connect_db,
    count_open_tranches,
    delete_pending_intent,
    ensure_schema,
    insert_shadow_ledger,
    load_pending_intent,
    save_pending_intent,
    upsert_account_state,
    get_today_trade_count,
)
from market_regime import REGIME_OFFLINE_LABEL, fetch_market_regime
from notion_api import check_notion_online, push_to_notion


class RiskManagerApp:
    def __init__(self):
        self.ib = IB()
        self.active_tws_port: int | None = None
        self.background_tasks: set[asyncio.Task] = set()
        self.bot: Bot | None = None
        self.bot_started_at: datetime.datetime | None = None
        self.tws_online_notified: bool = False
        self.symbol_locks: dict[str, asyncio.Lock] = {}
        self.killing_symbols: set[str] = set()
        self.system_status_cache = {
            "order_tool_running": False,
            "notion_online": False,
            "notion_msg": "等待首次探测...",
        }

    def get_symbol_lock(self, symbol: str) -> asyncio.Lock:
        """获取/创建属于该标的的独立并发锁。"""
        if symbol not in self.symbol_locks:
            self.symbol_locks[symbol] = asyncio.Lock()
        return self.symbol_locks[symbol]

    def spawn_background_task(self, coro) -> asyncio.Task:
        """统一的后台任务托管器。"""
        task = asyncio.create_task(coro)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
        return task

    def set_components(self, gateway, ib_listener, risk_engine):
        self.gateway = gateway
        self.ib_listener = ib_listener
        self.risk_engine = risk_engine

    def initialize_background_tasks(self):
        print("🚀 初始化并统一接管所有后台守护进程...")
        self.spawn_background_task(self.ib_listener.keepalive_daemon())
        self.spawn_background_task(self.status_probe_daemon())
        self.spawn_background_task(self.daily_rollover_daemon())
        self.spawn_background_task(self.heartbeat_2300_daemon())
        self.spawn_background_task(self.active_position_monitor_daemon())
        self.spawn_background_task(self.check_corporate_actions_job())
        self.spawn_background_task(self.sync_flex_query_job())

    async def daily_rollover_daemon(self):
        tz = ZoneInfo(TRADING_TZ)
        while True:
            try:
                now = datetime.datetime.now(tz)
                target = now.replace(hour=0, minute=0, second=5, microsecond=0)
                if now >= target:
                    target += datetime.timedelta(days=1)
                await asyncio.sleep((target - now).total_seconds())
                await self.run_daily_boot_checks()
            except Exception as e:
                print(f"🚨 跨日守护进程异常: {e}")
                await asyncio.sleep(60)

    async def status_probe_daemon(self):
        patterns = ("ibkr-order-tool", "ibkr_order_tool")
        while True:
            try:
                running = False
                for pat in patterns:
                    proc = await asyncio.create_subprocess_exec(
                        "pgrep", "-lf", pat, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    stdout, _ = await proc.communicate()
                    if stdout:
                        running = True
                        break
                self.system_status_cache["order_tool_running"] = running

                online, msg = await check_notion_online()
                self.system_status_cache["notion_online"] = online
                self.system_status_cache["notion_msg"] = msg
            except Exception:
                pass
            await asyncio.sleep(30)

    async def heartbeat_2300_daemon(self):
        shanghai_tz = ZoneInfo("Asia/Shanghai")
        while True:
            try:
                now = datetime.datetime.now(shanghai_tz)
                target = now.replace(hour=23, minute=0, second=0, microsecond=0)
                if now >= target:
                    target += datetime.timedelta(days=1)
                await asyncio.sleep((target - now).total_seconds())

                async with connect_db() as conn:
                    conn.row_factory = aiosqlite.Row
                    cursor = await conn.execute(
                        "SELECT symbol, side, quantity, setup_tag FROM shadow_ledger "
                        "WHERE status='OPEN' AND initial_stop = 0.0"
                    )
                    rows = await cursor.fetchall()
                    for row in rows:
                        if row["setup_tag"] == "IMPORT":
                            msg = f"🚨 物理收编仓位 `{row['symbol']}` 仍在裸奔！\n请尽快发送 `/update {row['symbol']} [止损价]` 补齐防线！"
                        else:
                            msg = f"🚨 越权交易 {row['symbol']} ({row['side']} {row['quantity']:.0f})！请发送 /override {row['symbol']} [坦白理由]！"
                        await self.gateway.notify_user(msg)
            except Exception as e:
                print(f"🚨 23:00 心跳守护异常: {e}")
                await asyncio.sleep(60)

    async def active_position_monitor_daemon(self):
        await asyncio.sleep(15)
        notified_3r = {}
        remind_3r_interval = 4 * 3600
        print("✅ 动态仓位巡检器 (Scale-out Financer) 已在后台安全启动...")
        while True:
            try:
                if not self.ib.isConnected():
                    await asyncio.sleep(60)
                    continue

                async with connect_db() as conn:
                    conn.row_factory = aiosqlite.Row
                    cursor = await conn.execute(
                        "SELECT id, symbol, side, quantity, entry_price, initial_stop, current_stop "
                        "FROM shadow_ledger WHERE status='OPEN'"
                    )
                    positions = await cursor.fetchall()

                for pos in positions:
                    trade_id, symbol, side = pos["id"], pos["symbol"], pos["side"]
                    entry, initial_stop, current_stop = float(pos["entry_price"]), float(pos["initial_stop"]), float(pos["current_stop"])

                    current_price, _ = await self.ib_listener.fetch_entry_price(symbol)
                    if not current_price or current_price <= 0:
                        continue
                    one_r_risk = abs(entry - initial_stop)
                    if one_r_risk <= 0:
                        continue

                    current_profit = (current_price - entry) if side == "LONG" else (entry - current_price)
                    current_r_multiple = current_profit / one_r_risk
                    at_risk = (current_stop < entry if side == "LONG" else current_stop > entry)

                    cache_key_3r = f"{trade_id}_3R"
                    if current_r_multiple >= 3.0 and at_risk:
                        now_ts = time.time()
                        last_ts = notified_3r.get(cache_key_3r)
                        if last_ts is None or (now_ts - last_ts) >= remind_3r_interval:
                            alert_msg = (
                                f"🚀 **【3R 爆发确认：严禁全仓死扛！】** `{symbol}`\n\n"
                                f"当前价格: ${current_price:.2f}\n"
                                f"当前浮盈: **+{current_r_multiple:.1f} R**\n\n"
                                f"⚠️ **纪律指令：**\n"
                                f"1. 请立刻在 TWS **市价卖出 1/4 或 1/3**，兑现免费门票！\n"
                                f"2. 成交后系统将自动平推止损，无需手动 `/update`。"
                            )
                            await self.gateway.notify_user(alert_msg)
                            notified_3r[cache_key_3r] = now_ts
                    else:
                        notified_3r.pop(cache_key_3r, None)
            except Exception as e:
                print(f"🚨 3R 巡检守护异常: {e}")
            await asyncio.sleep(300)

    async def check_corporate_actions_job(self):
        try:
            async with connect_db() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute("SELECT DISTINCT symbol FROM shadow_ledger WHERE status='OPEN'")
                rows = await cursor.fetchall()
            symbols = [r["symbol"] for r in rows]
            if not symbols:
                return

            loop = asyncio.get_running_loop()
            for sym in symbols:
                try:
                    def fetch_data(ticker_str: str):
                        tkr = yf.Ticker(ticker_str)
                        return tkr.splits, tkr.fast_info.get("lastPrice", None)

                    splits, last_price = await loop.run_in_executor(None, fetch_data, sym)
                    if splits is not None and not splits.empty:
                        for split_date, ratio in splits.tail(3).items():
                            ratio_f = float(ratio)
                            if ratio_f <= 0 or ratio_f == 1.0:
                                continue
                            date_str = split_date.strftime("%Y-%m-%d")
                            async with connect_db() as db:
                                cur = await db.execute("SELECT 1 FROM applied_splits WHERE symbol=? AND split_date=?", (sym, date_str))
                                if await cur.fetchone():
                                    continue
                                await db.execute(
                                    "UPDATE shadow_ledger SET quantity=quantity*?, entry_price=entry_price/?, initial_stop=initial_stop/?, current_stop=current_stop/? WHERE symbol=? AND status='OPEN'",
                                    (ratio_f, ratio_f, ratio_f, ratio_f, sym)
                                )
                                await db.execute("INSERT INTO applied_splits (symbol, split_date, ratio) VALUES (?, ?, ?)", (sym, date_str, ratio_f))
                                await db.commit()
                            await self.gateway.notify_user(f"✂️ **自动化拆股执行**\n系统检测到 `{sym}` 在 {date_str} 执行了 1:{ratio_f:g} 拆股。\n影子账本已静默等比例折算！")

                    if last_price is None or pd.isna(last_price):
                        await self.gateway.notify_user(f"⚠️ **公司代码异常预警**\n系统无法从雅虎财经获取 `{sym}`。该股票可能已**更名或退市**。\n请用 `/rename {sym} [新代码]` 修正账本！")
                except Exception as e:
                    print(f"巡检 {sym} 公司行动异常: {e}")
                await asyncio.sleep(0.5)
        except Exception as e:
            print(f"🚨 公司行动巡检器总体异常: {e}")

    async def sync_flex_query_job(self):
        try:
            if FLEX_TOKEN.startswith("YOUR_") or FLEX_QUERY_ID.startswith("YOUR_"):
                return
            nyse = mcal.get_calendar("NYSE")
            today = datetime.datetime.now()
            valid_days = nyse.valid_days(start_date=today - datetime.timedelta(days=5), end_date=today)
            if len(valid_days) < 2:
                return

            loop = asyncio.get_running_loop()
            report = await loop.run_in_executor(None, lambda: FlexReport(FLEX_TOKEN, FLEX_QUERY_ID))

            async with connect_db() as db_connection:
                db_connection.row_factory = aiosqlite.Row
                closed, total_pnl, closed_symbols = 0, 0.0, []
                for trade in report.extract("Trade"):
                    symbol = _flex_trade_attr(trade, "symbol", "underlyingSymbol")
                    if not symbol:
                        continue
                    exec_price = float(_flex_trade_attr(trade, "tradePrice", "TradePrice", default=0) or 0)
                    realized_pnl = float(_flex_trade_attr(trade, "fifoPnlRealized", "realizedPL", "realizedPnl", "RealizedP/L", default=0) or 0)

                    cursor = await db_connection.execute("SELECT id, setup_tag FROM shadow_ledger WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC LIMIT 1", (symbol,))
                    row = await cursor.fetchone()
                    if row:
                        trade_id, setup_tag = row["id"], row["setup_tag"] or ""
                        await db_connection.execute("UPDATE shadow_ledger SET status='CLOSED', exit_price=?, realized_pnl=? WHERE id=?", (exec_price, realized_pnl, trade_id))
                        self.spawn_background_task(push_to_notion(trade_id, symbol, realized_pnl, setup_tag))
                        closed += 1
                        total_pnl += realized_pnl
                        closed_symbols.append(symbol)
                await db_connection.commit()

            if closed > 0:
                pnl_str = f"+${total_pnl:.2f}" if total_pnl > 0 else f"-${abs(total_pnl):.2f}"
                await self.gateway.notify_user(f"✅ **盘前静默清算完成**\n成功关闭 {closed} 笔仓位：{', '.join(closed_symbols)}\n合计盈亏: {pnl_str}\n数据已归档。")
        except Exception as e:
            print(f"🚨 Flex 对账失败: {e}")

    async def run_daily_boot_checks(self):
        try:
            print("🌅 执行开机自检与复盘提醒...")
            tz = ZoneInfo(TRADING_TZ)
            today_str = datetime.datetime.now(tz).strftime("%Y-%m-%d")

            async with connect_db() as db:
                cursor = await db.execute("SELECT value FROM system_state WHERE key='last_reset_date'")
                row = await cursor.fetchone()
                if (row[0] if row else "") != today_str:
                    await db.execute("UPDATE system_state SET value='0' WHERE key='consecutive_losses'")
                    await db.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES ('last_reset_date', ?)", (today_str,))
                    await db.commit()
                    await self.gateway.notify_user("🔄 **系统启动自检**\n发现跨日，连亏状态已归零，试错额度全额恢复！")

                cursor = await db.execute("SELECT value FROM system_state WHERE key='last_review_date'")
                row = await cursor.fetchone()
                if (row[0] if row else "") != today_str:
                    await db.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES ('last_review_date', ?)", (today_str,))
                    await db.commit()
                    keyboard = [
                        [
                            InlineKeyboardButton("1 (失控)", callback_data="review_1"),
                            InlineKeyboardButton("2", callback_data="review_2"),
                            InlineKeyboardButton("3 (及格)", callback_data="review_3"),
                            InlineKeyboardButton("4", callback_data="review_4"),
                            InlineKeyboardButton("5 (完美)", callback_data="review_5"),
                        ]
                    ]
                    await self.gateway.notify_user(
                        "🌅 **开机复盘审判 (Boot Review)**\n\n请客观评估你**上一个交易日**的纪律执行情况：\n(如：是否无脑追高、是否执行了3R减仓)",
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
        except Exception as e:
            print(f"🚨 开机自检失败: {e}")


class TelegramGateway:
    def __init__(self, context: RiskManagerApp):
        self.ctx = context

    def bind_bot(self, bot: Bot) -> None:
        self.ctx.bot = bot
        self.ctx.bot_started_at = datetime.datetime.now()

    async def notify_user(self, text: str, reply_markup=None) -> None:
        if self.ctx.bot is None:
            print(f"TG 未绑定，跳过通知: {text[:80]}...")
            return
        try:
            await self.ctx.bot.send_message(chat_id=MY_TELEGRAM_CHAT_ID, text=text, reply_markup=reply_markup)
        except Exception as e:
            print(f"TG 发送失败: {e}")

    def format_bot_uptime(self) -> str:
        if self.ctx.bot_started_at is None:
            return "未知"
        delta = datetime.datetime.now() - self.ctx.bot_started_at
        total_m = int(delta.total_seconds() // 60)
        if total_m < 60:
            return f"{total_m}分钟"
        h, m = divmod(total_m, 60)
        if h < 24:
            return f"{h}小时{m}分"
        d, h = divmod(h, 24)
        return f"{d}天{h}小时"

    def format_tws_status_line(self) -> str:
        tws_ok = self.ctx.ib.isConnected()
        if tws_ok and self.ctx.active_tws_port:
            _, _, mode_cn = resolve_tws_ports()
            return f"🟢 TWS 已连 ({mode_cn} {self.ctx.active_tws_port})"
        if tws_ok:
            return "🟢 TWS 已连"
        return "🔴 TWS 未连 (请确认桌面端已登录且 API 开启)"

    def build_service_status_lines(self) -> list[str]:
        cache = self.ctx.system_status_cache
        notion_ok = cache["notion_online"]
        notion_detail = cache["notion_msg"]
        tool_ok = cache["order_tool_running"]
        return [
            f"🟢 Bot 在线 · 已运行 {self.format_bot_uptime()}",
            self.format_tws_status_line(),
            f"{'🟢' if notion_ok else '🔴'} Notion 交易复盘 · {'已连' if notion_ok else notion_detail}",
            f"{'🟢' if tool_ok else '🔴'} 桌面下单工具 · {'运行中' if tool_ok else '未启动'}",
            "",
        ]


class IBKRListener:
    def __init__(self, context: RiskManagerApp, gateway: TelegramGateway):
        self.ctx = context
        self.gateway = gateway
        self.ib = context.ib

    async def ensure_connected(self) -> bool:
        if self.ib.isConnected():
            self._register_event_handlers()
            return True
        preferred, fallback, mode_cn = resolve_tws_ports()
        for port in (preferred, fallback):
            try:
                await self.ib.connectAsync(TWS_HOST, port, clientId=CLIENT_ID, timeout=5)
                self.ctx.active_tws_port = port
                used_fallback = port != preferred
                suffix = " (备用端口)" if used_fallback else ""
                print(f"✅ TWS 已连接：{mode_cn} {TWS_HOST}:{port}{suffix} (clientId={CLIENT_ID})")
                self._register_event_handlers()
                return True
            except Exception as e:
                print(f"TWS {TWS_HOST}:{port} 连接失败: {e}")
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

    async def keepalive_daemon(self):
        """探活与重连守护进程。"""
        while True:
            try:
                preferred, _, mode_cn = resolve_tws_ports()
                if self.ib.isConnected() and self.ctx.active_tws_port is not None and self.ctx.active_tws_port != preferred:
                    print(f"检测到桌面端切换为{mode_cn}，重连 TWS {TWS_HOST}:{preferred}…")
                    self.ib.disconnect()
                    self.ctx.active_tws_port = None
                    self.ctx.tws_online_notified = False
                was_disconnected = not self.ib.isConnected()
                if was_disconnected:
                    if await self.ensure_connected():
                        if not self.ctx.tws_online_notified:
                            self.ctx.tws_online_notified = True
                            status = "\n".join(self.gateway.build_service_status_lines()).strip()
                            await self.gateway.notify_user(
                                f"🟢 **风控军师系统已重新上线** (TWS 重连)\n\n"
                                f"{status}\n\n"
                                f"👀 成交监听已挂载，桌面端下单成交后将自动入账。"
                            )
            except Exception as e:
                print(f"🚨 IB 探活守护进程异常: {e}")
            await asyncio.sleep(30)

    async def fetch_entry_price(self, symbol: str):
        if not await self.ensure_connected():
            return None, ""
        contract = Stock(symbol.upper(), "SMART", "USD")
        qualified = await self.ib.qualifyContractsAsync(contract)
        if not qualified:
            return None, ""

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
            print(f"净值读取失败: {e}")
        return 0.0

    def _dispatch_async(self, coro):
        """修复 IB 底层线程触发异步任务导致的 RuntimeError。"""
        try:
            asyncio.get_running_loop()
            self.ctx.spawn_background_task(coro)
        except RuntimeError:
            loop = asyncio.get_event_loop()
            loop.call_soon_threadsafe(self.ctx.spawn_background_task, coro)

    def on_execution(self, trade, fill):
        sym = fill.contract.symbol if fill.contract else "?"
        print(f"📡 [成交监听] {sym} {fill.execution.side} {fill.execution.shares}@{fill.execution.price}")
        self._dispatch_async(self._async_on_execution(trade, fill))

    async def _async_on_execution(self, trade, fill):
        execution = fill.execution
        contract = fill.contract
        symbol = contract.symbol
        side = "LONG" if execution.side == "BOT" else "SHORT"
        qty = float(execution.shares)
        price = float(execution.price)
        setup_tag = trade.order.orderRef if trade.order and trade.order.orderRef else ""

        if setup_tag == "KILL_SWITCH":
            print(f"🛡️ 侦测到 Kill Switch 的平仓回报 [{symbol}]，开始清理账本...")
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
                    stop_for_val = 0.0
                    if self.ib.isConnected():
                        for open_trade in self.ib.openTrades():
                            if (open_trade.contract.symbol == symbol and open_trade.order.orderType in ("STP", "STP LMT")):
                                stop_for_val = float(open_trade.order.auxPrice)
                                break
                    equity = await self.fetch_account_equity()
                    reject_reason = await validate_pending_entry(
                        conn, price, stop_for_val, qty, equity, is_buy=(side == "LONG")
                    )
                    if reject_reason:
                        await self.gateway.notify_user(
                            f"🛑 **UI 开仓被风控驳回** `{symbol}`\n{reject_reason}\n\n"
                            f"⚠️ 系统正在强制执行【斩立决】，清理该笔违规物理持仓！"
                        )
                        kill_res = await execute_kill_switch(symbol, trigger_reason="前端 UI 违规开仓被驳回")
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
                        old_qty = float(same_day_row["quantity"])
                        old_entry = float(same_day_row["entry_price"])
                        old_tag = same_day_row["setup_tag"] or ""
                        new_qty = old_qty + qty
                        new_entry = (old_qty * old_entry + qty * price) / new_qty
                        if setup_tag and setup_tag not in old_tag:
                            merged_tag = f"{old_tag},{setup_tag}".strip(",")
                        else:
                            merged_tag = old_tag

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
                        await conn.commit()
                        msg = (
                            f"🎯 **前端火力捕获 (同日加仓)**\n"
                            f"已接管来自 UI 的加仓指令：`{symbol}`\n"
                            f"新增: {qty:.0f}股 @ ${price:.2f} (均价拉至 ${new_entry:.2f})\n"
                            f"策略: {merged_tag or '未打标'}\n{stop_msg}"
                        )
                        self.ctx.spawn_background_task(self.gateway.notify_user(msg))
                    else:
                        tranche_id = f"T{len(open_tranches) + 1}"
                        await conn.execute(
                            "INSERT INTO shadow_ledger "
                            "(symbol, tranche_id, side, quantity, entry_price, initial_stop, current_stop, status, setup_tag) "
                            "VALUES (?, ?, ?, ?, ?, 0.0, 0.0, 'OPEN', ?)",
                            (symbol, tranche_id, side, qty, price, setup_tag),
                        )
                        await conn.commit()
                        msg = (
                            f"🎯 **前端火力捕获 (新开仓)**\n"
                            f"已接管来自 UI 的开仓指令：`{symbol}`\n"
                            f"成交: {qty:.0f}股 @ ${price:.2f}\n"
                            f"策略: {setup_tag or '未打标'}\n*(止损线将在 2 秒内自动同步防线)*"
                        )
                        self.ctx.spawn_background_task(self.gateway.notify_user(msg))
                        self.ctx.spawn_background_task(self._delayed_bracket_stop_capture(symbol))
                else:
                    had_profit = False
                    had_loss = False
                    remaining_exit_qty = qty
                    for row in open_tranches:
                        if remaining_exit_qty <= 0:
                            break
                        t_id = row["id"]
                        t_qty = float(row["quantity"])
                        t_entry = float(row["entry_price"])
                        tranche_side = row["side"]
                        if t_qty <= remaining_exit_qty:
                            cursor = await conn.execute("SELECT setup_tag FROM shadow_ledger WHERE id=?", (t_id,))
                            tag_row = await cursor.fetchone()
                            s_tag = tag_row["setup_tag"] if tag_row and tag_row["setup_tag"] else ""
                            if (tranche_side == "LONG" and price < t_entry) or (tranche_side == "SHORT" and price > t_entry):
                                had_loss = True
                            elif (tranche_side == "LONG" and price > t_entry) or (tranche_side == "SHORT" and price < t_entry):
                                had_profit = True
                            await conn.execute("UPDATE shadow_ledger SET status='CLOSED', exit_price=? WHERE id=?", (price, t_id))
                            self.ctx.spawn_background_task(push_to_notion(t_id, symbol, 0.0, s_tag))
                            remaining_exit_qty -= t_qty
                        else:
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
                                trim_detail = f"成交价: ${price:.2f} > 成本价: ${t_entry:.2f}" if tranche_side == "LONG" else f"成交价: ${price:.2f} < 成本价: ${t_entry:.2f}"
                                msg = (
                                    f"🤖 **自动护航：** 侦测到 `{symbol}` 盈利减仓！\n{trim_detail}\n"
                                    f"系统已自动将剩余 {new_qty:.0f} 股止损推至成本价 ${t_entry:.2f}。\n"
                                    f"🛡️ **该笔交易风控额度已完全释放！**"
                                )
                                self.ctx.spawn_background_task(self.gateway.notify_user(msg))
                            else:
                                if (tranche_side == "LONG" and price < t_entry) or (tranche_side == "SHORT" and price > t_entry):
                                    had_loss = True
                                await conn.execute("UPDATE shadow_ledger SET quantity=? WHERE id=?", (new_qty, t_id))
                            remaining_exit_qty = 0

                    await _apply_consecutive_losses(conn, had_profit, had_loss)
                await conn.commit()

            if not opening:
                self.ctx.spawn_background_task(_night_watchman_on_tp(trade, execution))

    async def _delayed_bracket_stop_capture(self, symbol: str):
        await asyncio.sleep(2.0)
        if not self.ib.isConnected():
            return
        try:
            found_stop = None
            for open_trade in self.ib.openTrades():
                if (open_trade.contract.symbol == symbol and open_trade.order.orderType in ("STP", "STP LMT")):
                    found_stop = float(open_trade.order.auxPrice)
                    break
            if not found_stop or found_stop <= 0:
                return
            async with connect_db() as conn:
                cursor = await conn.execute(
                    """
                    UPDATE shadow_ledger
                    SET current_stop=?, initial_stop=?
                    WHERE symbol=? AND status='OPEN' AND initial_stop=0.0
                    """,
                    (found_stop, found_stop, symbol),
                )
                if cursor.rowcount > 0:
                    await conn.commit()
                    msg = f"🛡️ **防线主动同步完毕：** `{symbol}` 的底层止损已被锚定在 ${found_stop:.2f}。"
                    await self.gateway.notify_user(msg)
        except Exception as e:
            print(f"延迟捕获止损出错: {e}")

    def on_open_order(self, trade):
        self._dispatch_async(self._async_on_open_order(trade))

    async def _async_on_open_order(self, trade):
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
                    "SELECT id, current_stop, entry_price FROM shadow_ledger WHERE symbol=? AND status='OPEN'",
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
                    SET current_stop=?, initial_stop = CASE WHEN initial_stop = 0.0 THEN ? ELSE initial_stop END
                    WHERE symbol=? AND status='OPEN'
                    """,
                    (new_stop, new_stop, symbol),
                )
                await conn.commit()

            risk_status = "✅ 止损已推至成本区以上，零风险/锁定利润！" if new_stop >= entry_price else f"📐 当前风险距离: {abs(entry_price - new_stop) / entry_price * 100:.2f}%"
            msg = (
                f"🖱️ **TWS 图表同步捕获：** `{symbol}`\n"
                f"侦测到止损单修改：${old_stop:.2f} ➡️ **${new_stop:.2f}**\n"
                f"影子账本已自动同步更新。\n{risk_status}"
            )
            await self.gateway.notify_user(msg)
        except Exception as e:
            print(f"图表同步止损失败: {e}")


class RiskEngine:
    def __init__(self, context: RiskManagerApp, ib_listener: IBKRListener, gateway: TelegramGateway):
        self.ctx = context
        self.ib_listener = ib_listener
        self.gateway = gateway

    async def calculate_risk_light(self, db_connection, current_total_equity: float):
        cursor = await db_connection.execute(
            "SELECT symbol, SUM(quantity * entry_price) AS pos_value "
            "FROM shadow_ledger WHERE status='OPEN' GROUP BY symbol"
        )
        rows = await cursor.fetchall()
        for row in rows:
            pos_value = row["pos_value"] or 0.0
            if pos_value > current_total_equity * MAX_POSITION_SIZE_PCT:
                return f"🚨 危险！{row['symbol']} 超过 40% 上限。", 0.0

        cursor = await db_connection.execute("SELECT MAX(high_water_mark) FROM account_state")
        row = await cursor.fetchone()
        hwm = float(row[0]) if row and row[0] else current_total_equity
        if current_total_equity > hwm:
            hwm = current_total_equity

        if hwm <= 0:
            return "🟢 绿灯", current_total_equity * RISK_PCT_PER_TRADE

        drawdown = (hwm - current_total_equity) / hwm
        if drawdown < RISK_MAX_DRAWDOWN_GREEN:
            return "🟢 绿灯", current_total_equity * RISK_PCT_PER_TRADE
        if drawdown < RISK_MAX_DRAWDOWN_YELLOW:
            return "🟡 黄灯", current_total_equity * (RISK_PCT_PER_TRADE / 2)
        return "🔴 红灯", 0.0

    async def validate_pending_entry(self, conn, entry_price: float, stop_price: float, quantity: float, equity: float, is_buy: bool) -> str | None:
        today_count = await get_today_trade_count(conn)
        if today_count >= MAX_DAILY_TRADES:
            return (
                f"🛑 **狙击手协议触发！**\n"
                f"今日弹夹已打空 ({today_count}/{MAX_DAILY_TRADES})。"
            )

        risk_light, base_risk_budget = await self.calculate_risk_light(conn, equity)
        if base_risk_budget <= 0:
            return f"🚨 风控灯 {risk_light}，当前禁止新开仓。"

        regime_label, _, risk_mult = await fetch_market_regime()

        if is_buy:
            if risk_mult == 0.0 or "BEAR THRUST" in regime_label.upper():
                return "🛑 **多头环境一票否决！** Bear Thrust 属于大盘下行坍塌期，严禁做多突破。"
        else:
            if "BULL THRUST" in regime_label.upper():
                return "🛑 **空头环境一票否决！** Bull Thrust 属于大盘强动量轧空期，严禁逆势做空。"

        cursor = await conn.execute(
            "SELECT entry_price, current_stop, quantity, side FROM shadow_ledger "
            "WHERE status='OPEN'"
        )
        open_positions = await cursor.fetchall()
        current_total_risk = 0.0
        for pos in open_positions:
            p_entry = float(pos["entry_price"])
            p_stop = float(pos["current_stop"])
            p_qty = float(pos["quantity"])
            current_total_risk += max(0.0, abs(p_entry - p_stop) * p_qty)

        if stop_price > 0:
            projected_total_risk = current_total_risk + (
                abs(entry_price - stop_price) * quantity
            )
            if projected_total_risk > equity * MAX_OVERNIGHT_RISK_PCT:
                return "🛑 **隔夜风险总闸触发！** 跨单合并总风险超标。"

        return None

    async def execute_kill_switch(self, symbol: str, trigger_reason: str = "手动授权") -> str:
        if symbol in self.ctx.killing_symbols:
            return f"ℹ️ `{symbol}` 正在执行强平中，忽略并发触发。"

        self.ctx.killing_symbols.add(symbol)
        try:
            if not await self.ib_listener.ensure_connected():
                return "❌ TWS 未连接，清仓失败。请手动在 TWS 操作！"

            contract = Stock(symbol, "SMART", "USD")
            qualified = await self.ib_listener.ib.qualifyContractsAsync(contract)
            if not qualified:
                return f"❌ 无法验证合约 `{symbol}`。"

            positions = await self.ib_listener.ib.reqPositionsAsync()
            actual_qty = 0.0
            for pos in positions:
                if (pos.contract.secType == "STK" and pos.contract.symbol == symbol and pos.position != 0):
                    actual_qty = float(pos.position)
                    break

            if actual_qty == 0:
                return f"ℹ️ {symbol} TWS 实盘持仓已为 0，跳过斩立决。"

            canceled_count = 0
            for trade in self.ib_listener.ib.trades():
                if trade.contract.symbol == symbol and not trade.isDone():
                    await self.ib_listener.ib.cancelOrderAsync(trade.order)
                    canceled_count += 1

            if canceled_count > 0:
                await asyncio.sleep(2.0)
                positions = await self.ib_listener.ib.reqPositionsAsync()
                actual_qty = 0.0
                for pos in positions:
                    if (pos.contract.secType == "STK" and pos.contract.symbol == symbol and pos.position != 0):
                        actual_qty = float(pos.position)
                        break
                if actual_qty == 0:
                    return (
                        f"ℹ️ `{symbol}` 在保护撤单期间仓位已被平掉，"
                        f"已跳过市价单强平环节，避免产生双倍裸空敞口。"
                    )

            action = "SELL" if actual_qty > 0 else "BUY"
            mkt_order = MarketOrder(action, abs(actual_qty))
            mkt_order.orderRef = "KILL_SWITCH"
            self.ib_listener.ib.placeOrder(contract, mkt_order)

            return (
                f"💀 **斩立决已成功执行 [{symbol}]**\n"
                f"触发原因: {trigger_reason}\n"
                f"已撤销 {canceled_count} 笔保护挂单\n"
                f"发送市价单: {action} {abs(actual_qty):.0f} 股。\n"
                f"*(系统将通过异步成交回报自动冲销账本)*"
            )
        except Exception as e:
            return f"❌ 斩立决发生异常: {e}"
        finally:
            loop = asyncio.get_running_loop()
            loop.call_later(10.0, self.ctx.killing_symbols.discard, symbol)

    async def night_watchman_on_tp(self, trade, execution) -> None:
        order = trade.order
        if not order or order.orderType != "LMT":
            return
        is_long_tp = execution.side == "SLD"
        is_short_tp = execution.side == "BOT"
        if not is_long_tp and not is_short_tp:
            return
        if not await self.ib_listener.ensure_connected():
            return

        symbol = trade.contract.symbol
        try:
            async with connect_db() as conn:
                conn.row_factory = aiosqlite.Row
                cur = await conn.execute(
                    "SELECT side, entry_price FROM shadow_ledger "
                    "WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC LIMIT 1",
                    (symbol,),
                )
                row = await cur.fetchone()
                if not row:
                    return
                entry_price = float(row["entry_price"])
                pos_side = row["side"]

            for open_trade in self.ib_listener.ib.openTrades():
                if open_trade.contract.symbol != symbol:
                    continue
                if open_trade.order.orderType not in ("STP", "STP LMT"):
                    continue
                stop_order = open_trade.order
                old_stop = float(stop_order.auxPrice)
                need_modify = False
                if pos_side == "LONG" and old_stop < entry_price:
                    if (stop_order.action or "").upper() == "SELL":
                        stop_order.auxPrice = entry_price
                        need_modify = True
                    else:
                        print(f"⚠️ [守夜人漏改] {symbol} 多头: 找到的止损单 action 为 '{stop_order.action}'，预期为 'SELL'")
                elif pos_side == "SHORT" and old_stop > entry_price:
                    if (stop_order.action or "").upper() == "BUY":
                        stop_order.auxPrice = entry_price
                        need_modify = True
                    else:
                        print(f"⚠️ [守夜人漏改] {symbol} 空头: 找到的止损单 action 为 '{stop_order.action}'，预期为 'BUY'")
                if not need_modify:
                    continue
                self.ib_listener.ib.placeOrder(open_trade.contract, stop_order)
                async with connect_db() as conn:
                    await conn.execute(
                        "UPDATE shadow_ledger SET current_stop=? WHERE symbol=? AND status='OPEN'",
                        (entry_price, symbol),
                    )
                    await conn.commit()
                await self.gateway.notify_user(
                    f"🛡️ **守夜人协议触发**\n"
                    f"`{symbol}` 的分批止盈单已成交！\n"
                    f"后台已将剩余仓位的止损单推移至保本价 ${entry_price:.2f}。\n"
                    f"您可以安心睡觉了。"
                )
                break
        except Exception as e:
            print(f"守夜人保本钩子失败: {e}")

    async def get_10ema(self, symbol: str) -> float:
        try:
            if not await self.ib_listener.ensure_connected():
                return 0.0
            contract = Stock(symbol.upper(), "SMART", "USD")
            qualified = await self.ib_listener.ib.qualifyContractsAsync(contract)
            if not qualified:
                return 0.0

            bars = await self.ib_listener.ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr="15 D",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=True,
            )
            if not bars:
                return 0.0

            df = util.df(bars)
            df["10EMA"] = df["close"].ewm(span=10, adjust=False).mean()
            if len(df) >= 2:
                return float(df["10EMA"].iloc[-2])
            return float(df["10EMA"].iloc[-1])
        except Exception as e:
            print(f"获取 {symbol} 10EMA 失败: {e}")
            return 0.0


app = RiskManagerApp()
tg_gateway = TelegramGateway(app)
ib_listener = IBKRListener(app, tg_gateway)
risk_engine = RiskEngine(app, ib_listener, tg_gateway)
app.set_components(tg_gateway, ib_listener, risk_engine)
ib = app.ib


def get_symbol_lock(symbol: str) -> asyncio.Lock:
    return app.get_symbol_lock(symbol)


def _spawn_background_task(coro) -> asyncio.Task:
    return app.spawn_background_task(coro)


def build_service_status_lines() -> list[str]:
    return tg_gateway.build_service_status_lines()


def bind_tg_bot(bot: Bot) -> None:
    tg_gateway.bind_bot(bot)


async def _notify_user(text: str) -> None:
    await tg_gateway.notify_user(text)


async def ensure_ib_connected() -> bool:
    return await ib_listener.ensure_connected()


async def fetch_entry_price(symbol: str):
    return await ib_listener.fetch_entry_price(symbol)


async def fetch_account_equity() -> float:
    return await ib_listener.fetch_account_equity()


def calc_share_quantity(risk_budget: float, entry_price: float, stop_price: float) -> int:
    risk_per_share = abs(entry_price - stop_price)
    if risk_per_share <= 0 or risk_budget <= 0:
        return 0
    return int(risk_budget / risk_per_share)


def _flex_trade_attr(trade, *names, default=""):
    for name in names:
        val = getattr(trade, name, None)
        if val is not None and val != "":
            return val
    return default


async def sync_flex_query(context=None):
    await app.sync_flex_query_job()


def _signed_ledger_qty(side: str, quantity: float) -> float:
    return quantity if str(side).upper() == "LONG" else -quantity


def _fmt_signed_position(qty: float) -> str:
    if abs(qty) < 1e-6:
        return "0 股"
    direction = "多" if qty > 0 else "空"
    return f"{abs(qty):.0f} 股 {direction}"


async def _close_ledger_discrepancy(
    conn: aiosqlite.Connection, symbol: str, discrepancy: float
) -> None:
    """按 FIFO 削减账本仓位；discrepancy = 账本 signed - TWS signed。"""
    if abs(discrepancy) < 1e-6:
        return
    side_to_close = "LONG" if discrepancy > 0 else "SHORT"
    remaining = abs(discrepancy)
    cursor = await conn.execute(
        "SELECT id, quantity FROM shadow_ledger "
        "WHERE symbol=? AND status='OPEN' AND side=? ORDER BY create_time ASC",
        (symbol, side_to_close),
    )
    open_tranches = await cursor.fetchall()
    for tranche in open_tranches:
        if remaining <= 0:
            break
        t_id = tranche["id"]
        t_qty = float(tranche["quantity"])
        if t_qty <= remaining + 1e-6:
            await conn.execute(
                "UPDATE shadow_ledger SET status='CLOSED', exit_price=0, realized_pnl=0 WHERE id=?",
                (t_id,),
            )
            remaining -= t_qty
        else:
            await conn.execute(
                "UPDATE shadow_ledger SET quantity=? WHERE id=?",
                (t_qty - remaining, t_id),
            )
            remaining = 0


async def reconcile_physical_positions(tg_application):
    print("🔍 启动物理仓位深度对账...")
    if not ib.isConnected():
        print("⚠️ TWS 未连接，跳过物理对账。")
        return

    try:
        positions = await ib.reqPositionsAsync()
    except Exception as e:
        print(f"❌ 物理持仓拉取失败: {e}")
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
                ghost_alerts.append(
                    f"⚠️ **发现未授权物理仓位 [{sym}]**\n"
                    f"账本预期: {_fmt_signed_position(expected)} | "
                    f"TWS实际: {_fmt_signed_position(actual)}\n"
                    f"*(请立刻使用 /init 或 /override 录入系统！)*"
                )

        await conn.commit()

        if ghost_alerts:
            alert_msg = "🚨 **物理对账警告 (已处理)** 🚨\n\n" + "\n\n".join(ghost_alerts)
            print("🚨 发生对账不一致，已发送 Telegram 警报。")
            await tg_application.bot.send_message(
                chat_id=MY_TELEGRAM_CHAT_ID,
                text=alert_msg,
            )
        else:
            print("✅ 物理仓位与影子账本 100% 吻合，防线坚固。")


async def eod_10ema_sniper_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """【收盘前5分钟审判】美东 15:55 唤醒，无视盘中洗盘，只看收盘定局。"""
    print("🎯 [EOD Sniper] 唤醒：执行收盘前 10EMA 破位终极审判...")

    ny_tz = ZoneInfo("America/New_York")
    now_ny = datetime.datetime.now(ny_tz)
    nyse = mcal.get_calendar("NYSE")
    if len(nyse.valid_days(start_date=now_ny.date(), end_date=now_ny.date())) == 0:
        print("非美股交易日，EOD Sniper 跳过。")
        return

    if not await ensure_ib_connected():
        print("❌ TWS 未连接，EOD Sniper 无法执行。将由物理止损单接管防线。")
        return

    async with connect_db() as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT symbol, side FROM shadow_ledger WHERE status='OPEN'"
        )
        open_positions = await cursor.fetchall()

    if not open_positions:
        return

    alerts: list[str] = []
    for pos in open_positions:
        sym = pos["symbol"]
        side = pos["side"]

        ema_10 = await get_10ema(sym)
        current_price, _ = await fetch_entry_price(sym)

        if not (ema_10 and current_price and ema_10 > 0):
            continue

        is_broken = (
            current_price < ema_10 * 0.99
            if side == "LONG"
            else current_price > ema_10 * 1.01
        )

        if is_broken:
            msg = (
                f"📉 **EOD 结构破位审判** `{sym}`\n"
                f"现价: ${current_price:.2f} | 10EMA: ${ema_10:.2f}\n"
                f"距离收盘仅剩 5 分钟，股价已无力收复 10EMA。\n"
                f"⚠️ 日线破位已成定局，放弃幻想，系统正在强制斩仓！"
            )
            await context.bot.send_message(chat_id=MY_TELEGRAM_CHAT_ID, text=msg)

            kill_result = await execute_kill_switch(
                sym, trigger_reason="15:55 EOD 10EMA 日线终极破位"
            )
            await context.bot.send_message(chat_id=MY_TELEGRAM_CHAT_ID, text=kill_result)
            alerts.append(sym)
        else:
            print(
                f"🛡️ {sym} 现价 ${current_price:.2f} 稳居 10EMA (${ema_10:.2f}) 防线之上，允许安全过夜。"
            )

    if not alerts:
        await context.bot.send_message(
            chat_id=MY_TELEGRAM_CHAT_ID,
            text=(
                "✅ **EOD 审判完毕**\n"
                "所有持仓均已成功扛过洗盘并守住 10EMA 防线。军师系统继续潜伏，安心睡觉！"
            ),
        )


START_HELP_TEXT = (
    "🛡️ **钢铁军师系统已上线 (EOD & 自检引擎运作中)**\n\n"
    "【日常监控】\n"
    "📊 /status — 查看当前持仓、净值与裸奔预警\n\n"
    "【风控解锁】\n"
    "🔓 /unlock [代码] [检讨≥15字] — 解锁桌面端F9发单权限 (限时5分钟)\n"
    "📝 /override [代码] [坦白理由] — 越权违规仓位事后坦白与接纳\n\n"
    "【系统维护】\n"
    "📥 /import — 一键强制收编 TWS 的未知物理持仓\n"
    "*(注: 拆股/更名/对账/止损推移均已由后台静默自动化接管)*"
)

# Telegram「/」快捷菜单：仅暴露核心四件套；其余 cmd 保留为隐藏后门
BOT_COMMANDS = [
    BotCommand("start", "指令帮助"),
    BotCommand("status", "持仓净值与裸奔预警"),
    BotCommand("unlock", "解锁 F9 发单权限"),
    BotCommand("override", "越权仓位坦白"),
    BotCommand("import", "收编 TWS 物理持仓"),
]


async def _sync_bot_commands_for(bot: Bot) -> list[str]:
    scopes = [BotCommandScopeDefault(), BotCommandScopeAllPrivateChats()]
    for scope in scopes:
        await bot.delete_my_commands(scope=scope)
        await bot.set_my_commands(BOT_COMMANDS, scope=scope)
    current = await bot.get_my_commands(scope=BotCommandScopeDefault())
    return [c.command for c in current]


async def _sync_bot_commands(app: Application) -> None:
    names = await _sync_bot_commands_for(app.bot)
    print(f"✅ Telegram 命令菜单已同步: {', '.join(names)}")


def require_auth(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        chat = update.effective_chat
        if chat is None or chat.id != MY_TELEGRAM_CHAT_ID:
            return
        return await func(update, context, *args, **kwargs)

    return wrapper


@require_auth
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("📥 [指令触达] 收到 /start")
    try:
        await _sync_bot_commands_for(context.bot)
    except Exception as e:
        print(f"⚠️ /start 命令菜单同步失败: {e}")
    status = "\n".join(build_service_status_lines()).strip()
    await update.message.reply_text(f"{START_HELP_TEXT}\n\n---\n{status}")


@require_auth
async def cmd_unlock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        msg_text = update.effective_message.text if update.effective_message else "未知消息"
        print(f"📥 [指令触达] 收到解锁请求: {msg_text}")

        args = context.args or []
        if len(args) < 2:
            await update.effective_message.reply_text(
                "⚠️ 格式错误。\n"
                "正确格式: /unlock [代码] [不少于15个字的检讨理由]\n"
                "示例: /unlock TSLA 这是一笔绝佳的放量突破值得一试"
            )
            return

        symbol = args[0].upper()
        confession = " ".join(args[1:]).strip()

        if len(confession) < 15:
            await update.effective_message.reply_text(
                f"❌ 检讨不够深刻：当前仅 {len(confession)} 字，至少需要 15 字。\n"
                f"你刚刚写的理由是：{confession}"
            )
            return

        async with connect_db() as conn:
            await conn.execute(
                "INSERT OR REPLACE INTO auth_tokens (symbol, confession, expire_time) "
                "VALUES (?, ?, datetime('now', 'localtime', '+5 minutes'))",
                (symbol, confession),
            )
            await conn.commit()

        await update.effective_message.reply_text(
            f"🔓 **授权已下发**\n"
            f"[{symbol}] 的极速下单(F9)权限已解锁，有效期 5 分钟。\n"
            f"理由：{confession}"
        )
        print(f"✅ /unlock 授权下发成功: {symbol}")

    except Exception as e:
        print(f"❌ /unlock 后台执行异常: {e}")
        if update.effective_message:
            await update.effective_message.reply_text(f"❌ 系统级异常: {e}")


def _trading_day_utc_bounds(day_offset: int = 0) -> tuple[str, str, str]:
    """返回 (date_iso, start_utc, end_utc)，统一按交易时区日切。"""
    tz = ZoneInfo(TRADING_TZ)
    target_day = (
        datetime.datetime.now(tz) - datetime.timedelta(days=day_offset)
    ).date()
    day_start = datetime.datetime.combine(
        target_day, datetime.time.min, tzinfo=tz
    )
    day_end = day_start + datetime.timedelta(days=1)
    start_utc = day_start.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    end_utc = day_end.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return target_day.isoformat(), start_utc, end_utc


async def review_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    chat = query.message.chat if query.message else None
    if chat is None or chat.id != MY_TELEGRAM_CHAT_ID:
        await query.answer()
        return
    await query.answer()
    data = query.data or ""
    if not data.startswith("review_"):
        return
    score = int(data.split("_", 1)[1])
    yesterday, start_utc, end_utc = _trading_day_utc_bounds(day_offset=1)
    async with connect_db() as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute(
            "INSERT OR REPLACE INTO daily_reviews (date, score, note) VALUES (?, ?, ?)",
            (yesterday, score, ""),
        )
        cursor = await conn.execute(
            """
            SELECT s.symbol, s.setup_tag, s.realized_pnl, s.status, a.confession
            FROM shadow_ledger s
            LEFT JOIN auth_tokens a ON s.symbol = a.symbol
            WHERE s.create_time >= ? AND s.create_time < ?
            ORDER BY s.symbol, s.create_time
            """,
            (start_utc, end_utc),
        )
        rows = await cursor.fetchall()
        await conn.commit()

    lines = [
        f"✅ 已记录昨日纪律得分：**{score} 分**。",
        "",
        "📝 **昨日复盘数据 (可直接导出 TradesViz)**:",
    ]
    if not rows:
        lines.append("昨天是空仓，最伟大的纪律就是管住了手。")
    else:
        for row in rows:
            pnl = float(row["realized_pnl"] or 0.0)
            tag = row["setup_tag"] or "未知"
            status = row["status"]
            conf = (
                f" | ⚠️ 违规理由: {row['confession']}"
                if row["confession"]
                else ""
            )
            if pnl > 0:
                pnl_str = f"+${pnl:.2f}"
            elif pnl < 0:
                pnl_str = f"-${abs(pnl):.2f}"
            else:
                pnl_str = "$0.00"
            lines.append(
                f"- **{row['symbol']}** [{tag}] {status} 盈亏: {pnl_str}{conf}"
            )
    await query.edit_message_text(text="\n".join(lines))


@require_auth
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("📥 [指令触达] 收到 /status")
    try:
        try:
            equity = await asyncio.wait_for(fetch_account_equity(), timeout=8)
        except Exception as e:
            print(f"/status 净值读取降级: {e}")
            equity = 0.0

        async with connect_db() as conn:
            conn.row_factory = aiosqlite.Row
            risk_light, risk_budget = await calculate_risk_light(conn, equity)
            await upsert_account_state(conn, equity, risk_light)
            cur = await conn.execute(
                "SELECT symbol, tranche_id, side, quantity, entry_price, current_stop, initial_stop, setup_tag "
                "FROM shadow_ledger WHERE status='OPEN' ORDER BY symbol, create_time"
            )
            rows = await cur.fetchall()

            current_total_risk = 0.0
            for r in rows:
                entry = float(r["entry_price"])
                stop = float(r["current_stop"])
                qty = float(r["quantity"])
                if stop > 0:
                    current_total_risk += abs(entry - stop) * qty
                else:
                    current_total_risk += entry * qty

            total_risk_pct = (current_total_risk / equity) * 100 if equity > 0 else 0.0
            risk_budget_pct = (risk_budget / equity) * 100 if equity > 0 else 0.0

            light_desc = ""
            if "绿灯" in risk_light:
                light_desc = "(状态良好，全额开火权)"
            elif "黄灯" in risk_light:
                light_desc = "(回撤预警，单笔额度已强制减半)"
            elif "红灯" in risk_light:
                light_desc = "(严重回撤，已禁止新开仓)"

            lines = build_service_status_lines() + [
                f"💰 净值: ${equity:,.2f}",
                f"🚦 风控灯: {risk_light} {light_desc}",
                f"📐 单笔额度: ${risk_budget:,.2f} ({risk_budget_pct:.2f}%)",
                f"🔥 总敞口风险: ${current_total_risk:,.2f} ({total_risk_pct:.2f}%)",
                "",
            ]
            if not rows:
                lines.append("📭 无 OPEN 影子仓位。")
                lines.append("*(若实盘有单，请先发送 /import)*")
            else:
                lines.append("📋 影子账本 OPEN:")
                for r in rows:
                    stop = float(r["current_stop"])
                    initial = float(r["initial_stop"])
                    naked = stop == 0.0 and initial == 0.0
                    warn = " ⚠️ [危险: 无止损(裸奔)]" if naked else ""
                    lines.append(
                        f"  • {r['side']} {r['symbol']} {r['tranche_id']} "
                        f"{r['quantity']:.0f}股 @ {r['entry_price']:.2f} "
                        f"stop {stop:.2f} [{r['setup_tag'] or ' '}]{warn}"
                    )

            chunk: list[str] = []
            chunk_len = 0
            for line in lines:
                if chunk_len + len(line) + 1 > 4000:
                    await update.message.reply_text("\n".join(chunk))
                    chunk = []
                    chunk_len = 0
                chunk.append(line)
                chunk_len += len(line) + 1
            if chunk:
                await update.message.reply_text("\n".join(chunk))
    except Exception as e:
        print(f"/status 指令后台异常: {e!r}")
        try:
            async with connect_db() as conn:
                cur = await conn.execute("SELECT COUNT(*) FROM shadow_ledger WHERE status='OPEN'")
                open_count = (await cur.fetchone())[0]
            fallback_lines = build_service_status_lines() + [
                "⚠️ 详细状态拉取失败，已切换简版回包。",
                f"📋 OPEN 仓位数: {open_count}",
                "如果 TWS 没开，净值和实时报价会暂时不可用。",
            ]
            await update.message.reply_text("\n".join(fallback_lines))
        except Exception as inner_e:
            print(f"/status 简版回包也失败: {inner_e!r}")


@require_auth
async def cmd_import(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """将 TWS 物理持仓强制收编进影子账本（模拟盘初始化 / 接管历史仓位）。"""
    print("📥 [指令触达] 收到 /import")
    if not await ensure_ib_connected():
        await update.message.reply_text("❌ 无法连接 TWS，请确认桌面端账户模式与 API 已开启。")
        return

    await update.message.reply_text("🔍 正在扫描 TWS 物理持仓…")
    try:
        positions = await ib.reqPositionsAsync()
    except Exception as e:
        await update.message.reply_text(f"❌ 拉取持仓失败: {e}")
        return

    imported: list[str] = []
    async with connect_db() as conn:
        conn.row_factory = aiosqlite.Row
        for pos in positions:
            if pos.position == 0 or pos.contract.secType != "STK":
                continue
            sym = pos.contract.symbol
            raw_qty = float(pos.position)
            side = "LONG" if raw_qty > 0 else "SHORT"
            qty = abs(raw_qty)
            entry = float(pos.avgCost)
            if entry <= 0:
                continue
            cur = await conn.execute(
                "SELECT id FROM shadow_ledger WHERE symbol=? AND status='OPEN'",
                (sym,),
            )
            if await cur.fetchone():
                continue
            tranche_id = f"T{await count_open_tranches(conn, sym) + 1}"
            await conn.execute(
                "INSERT INTO shadow_ledger "
                "(symbol, tranche_id, side, quantity, entry_price, initial_stop, current_stop, status, setup_tag) "
                "VALUES (?, ?, ?, ?, ?, 0.0, 0.0, 'OPEN', 'IMPORT')",
                (sym, tranche_id, side, qty, entry),
            )
            imported.append(f"{side} {sym} {qty:.0f}股 @ {entry:.2f}")
        await conn.commit()

    if not imported:
        await update.message.reply_text(
            "ℹ️ 未发现新的物理持仓。账本中已有的标的已跳过。\n"
            "请用 /status 查看，无止损仓位会标 ⚠️ 裸奔。"
        )
        return
    detail = "\n".join(f"  • {line}" for line in imported)
    await update.message.reply_text(
        f"✅ 成功收编 {len(imported)} 笔物理持仓：\n{detail}\n\n"
        f"请 /status 查看，并用 /update [代码] [止损] 补齐防线。"
    )


@require_auth
async def cmd_reconcile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_ib_connected():
        await update.message.reply_text("❌ TWS 未连接，无法对账。请确认桌面端账户模式与 TWS 已登录。")
        return
    await reconcile_physical_positions(context.application)
    await update.message.reply_text("✅ 物理对账已完成，若有差异已推送警报。")


@require_auth
async def cmd_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await sync_flex_query(context)
    await update.message.reply_text("✅ 静默对账已触发，若有平仓将单独推送捷报。")


@require_auth
async def cmd_init(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args = context.args
        if len(args) < 3:
            await update.message.reply_text("⚠️ 格式错误！/init [代码] [止损价] [策略标签]")
            return

        symbol = args[0].upper()
        stop_price = float(args[1])
        setup_tag = args[2].upper()

        entry_price, price_src = await fetch_entry_price(symbol)
        if entry_price is None or entry_price <= 0:
            await update.message.reply_text(
                f"❌ 无法从 IBKR 获取 {symbol} 实时报价，请确认 TWS 已连接且代码正确。"
            )
            return

        risk_pct = abs(entry_price - stop_price) / entry_price
        if risk_pct > MAX_STOP_PCT:
            await update.message.reply_text(
                f"🚨 驳回！止损距离 {risk_pct * 100:.2f}% 超过 {MAX_STOP_PCT * 100:.0f}%。"
            )
            return

        equity = await fetch_account_equity()
        async with connect_db() as conn:
            conn.row_factory = aiosqlite.Row

            today_count = await get_today_trade_count(conn)
            if today_count >= MAX_DAILY_TRADES:
                await update.message.reply_text(
                    f"🛑 **狙击手协议触发！**\n"
                    f"今日弹夹已打空 (已开仓 {today_count}/{MAX_DAILY_TRADES} 次)。\n"
                    f"在这个胜率下绝对不允许继续试错。请关掉软件，明日再战！"
                )
                return

            risk_light, base_risk_budget = await calculate_risk_light(conn, equity)
            await upsert_account_state(conn, equity, risk_light)
            if base_risk_budget <= 0:
                await update.message.reply_text(f"🚨 风控灯 {risk_light}，当前禁止新开仓。")
                return

            regime_tag, insight, risk_mult = await fetch_market_regime()
            if risk_mult == 0.0:
                await update.message.reply_text(
                    f"🛑 **宏观环境一票否决！**\n"
                    f"当前状态: {regime_tag}\n"
                    f"终端洞察: {insight}\n"
                    f"军师判定：覆巢之下无完卵。在 Bear Thrust 下，系统拒绝一切多头建仓。"
                )
                return

            dynamic_risk_budget = base_risk_budget * risk_mult

            cursor = await conn.execute(
                "SELECT entry_price, current_stop, quantity FROM shadow_ledger WHERE status='OPEN'"
            )
            open_positions = await cursor.fetchall()

            current_total_risk = 0.0
            for pos in open_positions:
                pos_entry = float(pos["entry_price"])
                pos_stop = float(pos["current_stop"])
                pos_qty = float(pos["quantity"])
                trade_risk = max(0.0, abs(pos_entry - pos_stop) * pos_qty)
                current_total_risk += trade_risk

            quantity = calc_share_quantity(dynamic_risk_budget, entry_price, stop_price)
            if quantity <= 0:
                await update.message.reply_text("🚨 动态折算后计算股数为 0，请检查止损距离。")
                return

            new_trade_risk = abs(entry_price - stop_price) * quantity
            projected_total_risk = current_total_risk + new_trade_risk
            max_allowed_risk = equity * MAX_OVERNIGHT_RISK_PCT

            if projected_total_risk > max_allowed_risk:
                pct_str = (
                    f"{(projected_total_risk / equity) * 100:.2f}%"
                    if equity > 0
                    else "N/A"
                )
                await update.message.reply_text(
                    f"🛑 **隔夜风险总闸触发！**\n"
                    f"当前总持仓风险: ${current_total_risk:.2f}\n"
                    f"这笔新单风险: ${new_trade_risk:.2f}\n"
                    f"合并后风险将达到 ${projected_total_risk:.2f} ({pct_str})，"
                    f"超过系统允许极限 {MAX_OVERNIGHT_RISK_PCT * 100:.2f}%！\n\n"
                    f"👉 **解法：** 请通过 `/update` 将现有盈利单止损推至成本价，释放额度后再试。"
                )
                return

            regime_context = f"{regime_tag} | {insight}"
            intent_id = uuid.uuid4().hex[:8]
            await save_pending_intent(
                conn, intent_id, symbol, stop_price, setup_tag,
                entry_price, quantity, regime_context,
            )

        notional = entry_price * quantity
        risk_dollar = abs(entry_price - stop_price) * quantity
        msg = (
            f"🛡️ 【{symbol} 建仓审查】\n"
            f"入场参考 ({price_src}): ${entry_price:.2f}\n"
            f"止损: ${stop_price:.2f} ({risk_pct * 100:.2f}%)\n"
            f"建议股数: {quantity} 股 (承担风险 ${risk_dollar:.2f})\n"
            f"名义金额: ${notional:,.2f}\n"
            f"策略: {setup_tag}\n"
            f"风控灯: {risk_light}\n"
            f"🎯 今日剩余子弹: {MAX_DAILY_TRADES - today_count - 1} 发\n\n"
            f"📊 宏观环境雷达:\n"
            f"状态: {regime_tag} (仓位乘数 {risk_mult}x)\n"
            f"洞察: {insight}\n"
            + (
                f"\n⚠️ **宽度雷达离线**，仓位按正常 1.0x 执行，宏观判定非实时。\n"
                if regime_tag == REGIME_OFFLINE_LABEL
                else ""
            )
            + "\n确认合规？"
        )
        keyboard = [
            [InlineKeyboardButton("🟩 确认无误 (入账)", callback_data=f"CONFIRM:{intent_id}")],
            [InlineKeyboardButton("🟥 冲动撤单", callback_data=f"CANCEL:{intent_id}")],
        ]
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    except ValueError:
        await update.message.reply_text("⚠️ 止损价必须是数字。")
    except Exception as e:
        await update.message.reply_text(f"❌ 审查失败: {e}")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None:
        return
    chat = query.message.chat if query.message else None
    if chat is None or chat.id != MY_TELEGRAM_CHAT_ID:
        await query.answer()
        return

    await query.answer()
    data = query.data or ""

    if data.startswith("CANCEL:"):
        intent_id = data.split(":", 1)[1]
        async with connect_db() as conn:
            await delete_pending_intent(conn, intent_id)
        await query.edit_message_text(text="🛑 已放弃记账。")
        return

    if data.startswith("KILL:"):
        symbol = data.split(":", 1)[1].upper()
        await query.edit_message_text(text=f"⏳ 正在执行安全锁清仓，请稍候 [{symbol}]...")
        result_msg = await execute_kill_switch(symbol, trigger_reason="Telegram按钮手动授权")
        await query.edit_message_text(text=result_msg)
        return

    if data.startswith("CONFIRM:"):
        intent_id = data.split(":", 1)[1]
        async with connect_db() as conn:
            conn.row_factory = aiosqlite.Row
            pending = await load_pending_intent(conn, intent_id)
            if pending is None:
                await query.edit_message_text(text="⚠️ 意图已过期，请重新 /init。")
                return

            equity = await fetch_account_equity()
            reject_reason = await validate_pending_entry(
                conn,
                float(pending["entry_price"]),
                float(pending["stop_price"]),
                float(pending["quantity"]),
                equity,
                is_buy=True,
            )
            if reject_reason:
                await delete_pending_intent(conn, intent_id)
                await query.edit_message_text(
                    text=f"{reject_reason}\n\n⚠️ 审查意图已作废，请重新 /init。"
                )
                return

            row_id = await insert_shadow_ledger(
                conn,
                pending["symbol"],
                float(pending["stop_price"]),
                pending["setup_tag"],
                float(pending["entry_price"]),
                float(pending["quantity"]),
                pending["spy_context"] or "",
            )
            await delete_pending_intent(conn, intent_id)
            _spawn_background_task(
                push_to_notion(row_id, pending["symbol"], 0.0, pending["setup_tag"])
            )

            await query.edit_message_text(
                text=(
                    f"✅ 已正式记入影子账本 #{row_id}\n"
                    f"{pending['symbol']} {int(pending['quantity'])}股 "
                    f"@ {pending['entry_price']:.2f} stop {pending['stop_price']:.2f}"
                )
            )


@require_auth
async def cmd_override(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg_text = update.effective_message.text if update.effective_message else "未知消息"
        print(f"📥 [指令触达] 收到越权坦白请求: {msg_text}")

        args = context.args or []
        if len(args) < 2:
            await update.effective_message.reply_text(
                "⚠️ 格式错误！\n正确格式: /override [代码] [坦白理由]"
            )
            return

        symbol = args[0].upper()
        reason = " ".join(args[1:])

        async with connect_db() as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(
                "SELECT id FROM shadow_ledger WHERE symbol=? AND status='OPEN' AND initial_stop=0.0",
                (symbol,),
            )
            rows = await cur.fetchall()

            if not rows:
                await update.effective_message.reply_text(
                    f"ℹ️ {symbol} 无待坦白的越权仓位 (此仓位可能已平仓，或本身合规)。"
                )
                return

            await conn.execute(
                "UPDATE shadow_ledger SET "
                "initial_stop=entry_price*0.9, current_stop=entry_price*0.9, setup_tag='FOMO' "
                "WHERE symbol=? AND status='OPEN' AND initial_stop=0.0",
                (symbol,),
            )
            await conn.commit()

            for row in rows:
                _spawn_background_task(
                    push_to_notion(row["id"], symbol, 0.0, "FOMO", confession=reason)
                )

        await update.effective_message.reply_text(
            f"💀 已记录坦白: {symbol} - {reason}\n"
            f"这笔交易已被军师系统接纳，但已打上违规标签。"
        )
        print(f"✅ /override 记录成功: {symbol}")

    except Exception as e:
        print(f"❌ /override 后台执行异常: {e}")
        if update.effective_message:
            await update.effective_message.reply_text(f"❌ 坦白记录失败: {e}")


@require_auth
async def cmd_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("⚠️ 格式错误！/update [代码] [新止损价]")
            return
        symbol, new_stop = args[0].upper(), float(args[1])

        async with connect_db() as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(
                "SELECT id, entry_price FROM shadow_ledger WHERE symbol=? AND status='OPEN'",
                (symbol,),
            )
            rows = await cur.fetchall()
            if not rows:
                await update.message.reply_text(f"ℹ️ {symbol} 无 OPEN 仓位。")
                return

            for row in rows:
                entry = float(row["entry_price"])
                risk_pct = abs(entry - new_stop) / entry if entry > 0 else 1.0
                if risk_pct > MAX_STOP_PCT:
                    await update.message.reply_text(
                        f"🚨 {symbol} 新止损 {new_stop} 距离入场 {risk_pct * 100:.2f}%，超过 3% 上限。"
                    )
                    return

            await conn.execute(
                "UPDATE shadow_ledger SET current_stop=? WHERE symbol=? AND status='OPEN'",
                (new_stop, symbol),
            )
            await conn.commit()

        await update.message.reply_text(f"✅ {symbol} 止损已更新为 {new_stop}，风险额度已重算。")
    except ValueError:
        await update.message.reply_text("⚠️ 新止损价必须是数字。")
    except Exception as e:
        await update.message.reply_text(f"❌ 更新失败: {e}")


@require_auth
async def cmd_split(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("⚠️ 格式错误！/split [代码] [拆股比例, 例如1拆4输入4]")
            return
        symbol, ratio = args[0].upper(), float(args[1])
        if ratio <= 0:
            await update.message.reply_text("⚠️ 比例必须大于 0。")
            return

        async with connect_db() as conn:
            cur = await conn.execute(
                "SELECT COUNT(*) FROM shadow_ledger WHERE symbol=? AND status='OPEN'",
                (symbol,),
            )
            row = await cur.fetchone()
            if int(row[0]) == 0:
                await update.message.reply_text(f"ℹ️ {symbol} 无 OPEN 仓位。")
                return

            await conn.execute(
                """
                UPDATE shadow_ledger
                SET quantity=ROUND(quantity*?, 4),
                    entry_price=entry_price/?,
                    initial_stop=initial_stop/?,
                    current_stop=current_stop/?
                WHERE symbol=? AND status='OPEN'
                """,
                (ratio, ratio, ratio, ratio, symbol),
            )
            await conn.commit()

        await update.message.reply_text(f"✂️ {symbol} 已执行 1:{ratio} 拆/合股处理。")
    except ValueError:
        await update.message.reply_text("⚠️ 比例必须是数字。")
    except Exception as e:
        await update.message.reply_text(f"❌ 拆股处理失败: {e}")


@require_auth
async def cmd_rename(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("⚠️ 格式错误！/rename [旧代码] [新代码]")
            return
        old_sym, new_sym = args[0].upper(), args[1].upper()

        async with connect_db() as conn:
            cur = await conn.execute(
                "SELECT COUNT(*) FROM shadow_ledger WHERE symbol=? AND status='OPEN'",
                (old_sym,),
            )
            row = await cur.fetchone()
            if int(row[0]) == 0:
                await update.message.reply_text(f"ℹ️ {old_sym} 无 OPEN 仓位。")
                return

            await conn.execute(
                "UPDATE shadow_ledger SET symbol=? WHERE symbol=? AND status='OPEN'",
                (new_sym, old_sym),
            )
            await conn.commit()

        await update.message.reply_text(f"🔄 影子账本代码已更名: {old_sym} -> {new_sym}")
    except Exception as e:
        await update.message.reply_text(f"❌ 更名失败: {e}")


async def _apply_consecutive_losses(conn, profitable: bool, losing: bool) -> None:
    if profitable:
        await conn.execute(
            "UPDATE system_state SET value='0' WHERE key='consecutive_losses'"
        )
    elif losing:
        await conn.execute(
            "UPDATE system_state SET value = CAST((CAST(value AS INTEGER) + 1) AS TEXT) "
            "WHERE key='consecutive_losses'"
        )


async def _post_init(tg_app: Application) -> None:
    bind_tg_bot(tg_app.bot)
    await ensure_schema()
    try:
        await _sync_bot_commands(tg_app)
    except Exception as e:
        print(f"⚠️ Telegram 命令菜单同步失败: {e}")
    await app.run_daily_boot_checks()
    app.initialize_background_tasks()
    if await ensure_ib_connected():
        print("✅ TWS 已连接，主客户端(Master Client)全局跨端监听已启动。")
        await _notify_system_online("启动")
        await reconcile_physical_positions(tg_app)
    else:
        print("⚠️ TWS 未连接，/init 报价与成交同步将不可用，Telegram 仍可运行。")
        await _notify_bot_only_online()


def main() -> None:
    if TG_BOT_TOKEN == "YOUR_TG_TOKEN":
        print("❌ 致命错误：请在 .env 文件中配置真实的 TG_BOT_TOKEN！")
        sys.exit(1)

    tg_app = (
        Application.builder()
        .token(TG_BOT_TOKEN)
        .build()
    )

    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(CommandHandler("init", cmd_init))
    tg_app.add_handler(CommandHandler("status", cmd_status))
    tg_app.add_handler(CommandHandler("import", cmd_import))
    tg_app.add_handler(CommandHandler("reconcile", cmd_reconcile))
    tg_app.add_handler(CommandHandler("sync", cmd_sync))
    tg_app.add_handler(CommandHandler("override", cmd_override))
    tg_app.add_handler(CommandHandler("update", cmd_update))
    tg_app.add_handler(CommandHandler("split", cmd_split))
    tg_app.add_handler(CommandHandler("rename", cmd_rename))
    tg_app.add_handler(CommandHandler("unlock", cmd_unlock))
    tg_app.add_handler(CallbackQueryHandler(review_callback, pattern="^review_"))
    tg_app.add_handler(CallbackQueryHandler(button_handler))

    if ENABLE_EOD_SNIPER and tg_app.job_queue:
        ny_tz = ZoneInfo("America/New_York")
        tg_app.job_queue.run_daily(
            eod_10ema_sniper_job,
            time=datetime.time(hour=15, minute=55, tzinfo=ny_tz),
            name="eod_10ema_sniper",
        )
        print("🔭 [VPS 云端模式] EOD 收盘狙击手引擎已装载。")
    elif ENABLE_EOD_SNIPER:
        print("⚠️ ENABLE_EOD_SNIPER=True 但 job_queue 不可用，EOD 狙击手未注册。")
    else:
        print("💻 [本地关机模式] EOD 狙击手休眠，夜间防线由 TWS 物理止损单全面接管。")

    async def runner() -> None:
        async with tg_app:
            await _post_init(tg_app)
            await tg_app.start()
            mode = "EOD 狙击手" if ENABLE_EOD_SNIPER else "物理止损兜底"
            print(f"✅ Telegram Bot 军师巡检器已启动 (状态自检 + {mode})。")
            await tg_app.updater.start_polling(drop_pending_updates=True)
            await asyncio.Event().wait()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        print("风控军师已停止。")


if __name__ == "__main__":
    main()
