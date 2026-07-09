import asyncio
import datetime
import os
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
import ssl
import urllib.request

import certifi

from ib_insync import IB, FlexReport, MarketOrder, Stock, util
from ib_insync.decoder import Decoder
from ib_insync.wrapper import Wrapper

# ── SSL 证书修复：Python 3.14 在 macOS 上缺少系统根证书，导致 Flex 查询失败 ──
_ssl_context = ssl.create_default_context(cafile=certifi.where())
_https_handler = urllib.request.HTTPSHandler(context=_ssl_context)
urllib.request.install_opener(urllib.request.build_opener(_https_handler))

# ── Monkey-patch #1: ib_insync 0.9.86 不兼容 TWS 10.48+ 的新消息类型 ──
_original_interpret = Decoder.interpret


def _patched_interpret(self, fields):
    try:
        msgId = int(fields[0])
        if msgId not in self.handlers:
            return
        self.handlers[msgId](fields)
    except Exception:
        self.logger.exception("Error handling fields: %s", fields)


Decoder.interpret = _patched_interpret

# ── Monkey-patch #2: completedOrder handler 未安全初始化 _results key ──
_original_completedOrder = Wrapper.completedOrder


def _safe_completedOrder(self, contract, order, orderState):
    if "completedOrders" not in self._results:
        self._results["completedOrders"] = []
    return _original_completedOrder(self, contract, order, orderState)


Wrapper.completedOrder = _safe_completedOrder
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
    EMA_PERIOD,
    ENABLE_EOD_SNIPER,
    FLEX_QUERY_ID,
    FLEX_TOKEN,
    FORCE_CONFESSION_HOUR,
    MAX_POSITION_SIZE_PCT,
    MAX_STOP_PCT,
    MAX_DAILY_TRADES,
    MAX_OVERNIGHT_RISK_PCT,
    MY_TELEGRAM_CHAT_ID,
    RISK_MAX_DRAWDOWN_GREEN,
    RISK_MAX_DRAWDOWN_YELLOW,
    RISK_PCT_PER_TRADE,
    TG_BOT_TOKEN,
    THRESHOLD_3R,
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
from logger import logger
from market_regime import REGIME_OFFLINE_LABEL, fetch_market_regime
from notion_api import check_notion_online, enqueue_notion, push_to_notion
from outbound_queue import enqueue_outbound, outbound_worker
from reconciliation import reconcile_physical_positions
from risk_engine import RiskEngine, eod_10ema_sniper_job, set_app_context
from telegram_router import register_handlers, sync_bot_commands
from flex_settlement import run_flex_settlement
from ib_listener import IBKRListener
from gateway import TelegramGateway
from scale_out_monitor import ScaleOutMonitor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.events import EVENT_JOB_ERROR


class RiskManagerApp:
    def __init__(self):
        self.ib = IB()
        self.active_tws_port: int | None = None
        self.background_tasks: set[asyncio.Task] = set()
        self.bot: Bot | None = None
        self.bot_started_at: datetime.datetime | None = None
        self.tws_online_notified: bool = False
        self.symbol_locks: dict[str, asyncio.Lock] = {}
        # APScheduler：统一管理所有定时任务
        self.scheduler = AsyncIOScheduler(timezone=ZoneInfo("America/New_York"))
        # 3R 止盈雷达
        self.scale_out_monitor: ScaleOutMonitor | None = None
        self.killing_symbols: set[str] = set()
        self._debounce_tasks: dict[str, asyncio.Task] = {}  # 防抖：symbol → timer task
        self._debounce_msgs: dict[str, list[str]] = {}       # 防抖：symbol → pending messages
        self._nightwatchman_done: set[str] = set()           # 守夜人已执行标的
        self._last_synced: dict[str, dict] = {}              # 同步守护：symbol → {qty, stop, entry}
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

    def debounced_notify(self, symbol: str, msg: str, delay: float = 3.0):
        """防抖通知：同一标的 3 秒内的多条消息合并为一条。"""
        if symbol not in self._debounce_msgs:
            self._debounce_msgs[symbol] = []
        self._debounce_msgs[symbol].append(msg)
        old = self._debounce_tasks.pop(symbol, None)
        if old and not old.done():
            old.cancel()
        async def _fire():
            await asyncio.sleep(delay)
            msgs = self._debounce_msgs.pop(symbol, [])
            self._debounce_tasks.pop(symbol, None)
            if not msgs:
                return
            unique = list(dict.fromkeys(msgs))
            if len(unique) == 1:
                text = unique[0]
            else:
                text = f"📊 **{symbol} 成交汇总** ({len(msgs)}笔)\n\n" + "\n".join(f"  • {m}" for m in unique[:5])
                if len(unique) > 5:
                    text += f"\n  ... 共 {len(unique)} 条"
            if self.bot:
                try:
                    await self.bot.send_message(chat_id=MY_TELEGRAM_CHAT_ID, text=text)
                except Exception:
                    pass
        self._debounce_tasks[symbol] = asyncio.create_task(_fire())

    def set_components(self, gateway, ib_listener, risk_engine):
        self.gateway = gateway
        self.ib_listener = ib_listener
        self.risk_engine = risk_engine

    def initialize_background_tasks(self):
        logger.info("🚀 启动工业级后台调度器 (APScheduler) 与常驻守护进程...")

        # 🚀 强制挂载全局错误拦截器，防止定时任务静默死亡
        def job_error_listener(event):
            logger.error(
                f"🚨 [调度器严重故障] 任务 {event.job_id} 崩溃！"
                f"异常: {repr(event.exception)}"
            )

        self.scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)

        # ── 常驻轮询型 Worker (asyncio Task：需要持续事件驱动) ──
        self.spawn_background_task(self.ib_listener.keepalive_daemon())
        self.spawn_background_task(self.status_probe_daemon())
        self.spawn_background_task(self.context_backfill_daemon())
        self.scale_out_monitor = ScaleOutMonitor(self)
        self.spawn_background_task(outbound_worker(self))

        # ── 定时调度型 Job (APScheduler Cron：精确按时间表执行) ──
        # 每 15 分钟执行 TWS 物理对账
        self.scheduler.add_job(
            reconcile_physical_positions,
            'interval',
            minutes=15,
            args=[self.ib, self.gateway.notify_user],
            id="reconcile_job",
            replace_existing=True,
        )

        # 每日 09:00 和 16:30 (美东) 执行 Flex 权威结算
        self.scheduler.add_job(
            run_flex_settlement,
            'cron',
            hour='9,16', minute='0,30',
            args=[self.gateway.notify_user, self.ib],
            id="flex_settlement_job",
            replace_existing=True,
        )

        # 每日 00:05 (美东) 执行跨日自检
        self.scheduler.add_job(
            self.run_daily_boot_checks,
            'cron',
            hour=0, minute=5,
            id="daily_rollover_job",
            replace_existing=True,
        )

        # 每日 23:00 (上海时间) 强制坦白提醒
        self.scheduler.add_job(
            self._heartbeat_2300_check,
            'cron',
            hour=FORCE_CONFESSION_HOUR, minute=0,
            timezone=ZoneInfo("Asia/Shanghai"),
            id="heartbeat_2300_job",
            replace_existing=True,
        )

        # 盘中每 5 分钟执行 3R 止盈雷达扫描
        self.scheduler.add_job(
            self.scale_out_monitor.run_3r_scan,
            'interval',
            minutes=5,
            id="3r_scan_job",
            replace_existing=True,
        )

        # 每日 08:00 (美东) 巡检公司行动 (拆股/更名)
        self.scheduler.add_job(
            self.check_corporate_actions_job,
            'cron',
            hour=8, minute=0,
            id="corporate_actions_job",
            replace_existing=True,
        )

        self.scheduler.start()
        logger.info("✅ APScheduler 已启动，所有 Cron Job 注册完毕。")

    async def sync_flex_query_job(self):
        """Flex 权威结算的薄封装（兼容旧调用点）。

        APScheduler 每天 09:00/16:30 自动调度 run_flex_settlement；
        此方法供 /sync 命令和 TWS 重连时手动触发。
        """
        await run_flex_settlement(self.gateway.notify_user, self.ib)

    async def _heartbeat_2300_check(self):
        """23:00 强制坦白检查（由 APScheduler 每天触发，替代原 heartbeat_2300_daemon）。"""
        async with connect_db() as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(
                "SELECT symbol, side, quantity, setup_tag FROM shadow_ledger "
                "WHERE status='OPEN' AND initial_stop = 0.0"
            )
            rows = await cursor.fetchall()
            for row in rows:
                if row["setup_tag"] in ("IMPORT", "TWS_SYNC"):
                    msg = f"🚨 物理收编仓位 `{row['symbol']}` 仍在裸奔！\n请尽快发送 `/update {row['symbol']} [止损价]` 补齐防线！"
                else:
                    msg = f"🚨 越权交易 {row['symbol']} ({row['side']} {row['quantity']:.0f})！请发送 /override {row['symbol']} [坦白理由]！"
                await self.gateway.notify_user(msg)

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
                logger.info(f"🚨 跨日守护进程异常: {e}")
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

    async def context_backfill_daemon(self):
        """补填守护：每 5 分钟扫描 OPEN 仓位，为缺失 SPY Context 的仓位补填大盘环境。

        解决两个场景：
        1. 对账引擎自动收编的仓位（TWS_SYNC）— 从未抓取过 SPY Context
        2. 成交回调时因代理瞬断导致 spy_context 为空的仓位
        """
        await asyncio.sleep(60)  # 等系统完全就绪
        while True:
            try:
                async with connect_db() as conn:
                    conn.row_factory = aiosqlite.Row
                    cur = await conn.execute(
                        "SELECT id, symbol, entry_price, initial_stop, quantity, side "
                        "FROM shadow_ledger "
                        "WHERE status='OPEN' AND (spy_context IS NULL OR spy_context = '')"
                    )
                    bare_rows = await cur.fetchall()

                if bare_rows:
                    # 拉一次 SPY 环境（所有仓位共用）
                    spy_label, _, _ = await fetch_market_regime()
                    spy_ctx = spy_label if spy_label and spy_label != REGIME_OFFLINE_LABEL else ""

                    if spy_ctx:
                        for row in bare_rows:
                            tid = row["id"]
                            sym = row["symbol"]
                            event_key = f"{tid}-UPDATE-SPY"

                            # 更新影子账本
                            async with connect_db() as conn:
                                await conn.execute(
                                    "UPDATE shadow_ledger SET spy_context=? WHERE id=?",
                                    (spy_ctx, tid),
                                )
                                # 查找 OPEN 的 notion_page_id，确保 UPDATE 原位覆盖
                                cur = await conn.execute(
                                    "SELECT notion_page_id FROM outbound_queue "
                                    "WHERE event_key=? AND channel='notion' "
                                    "AND notion_page_id IS NOT NULL",
                                    (f"{tid}-OPEN",),
                                )
                                page_row = await cur.fetchone()
                                await conn.commit()

                            # 入队 Notion UPDATE
                            await enqueue_outbound(
                                event_key, "notion",
                                {
                                    "trade_id": tid,
                                    "symbol": sym,
                                    "event_type": "UPDATE",
                                    "spy_context": spy_ctx,
                                    "entry_price": float(row["entry_price"]),
                                    "quantity": float(row["quantity"]),
                                    "initial_stop": float(row["initial_stop"] or 0),
                                    "current_stop": float(row["current_stop"] or 0),
                                },
                            )

                            # 回写 page_id：确保 worker 走 pages.update 而非 pages.create
                            if page_row and page_row["notion_page_id"]:
                                async with connect_db() as conn:
                                    await conn.execute(
                                        "UPDATE outbound_queue SET notion_page_id=? "
                                        "WHERE event_key=? AND channel='notion'",
                                        (page_row["notion_page_id"], event_key),
                                    )
                                    await conn.commit()

                        logger.info(
                            f"🔄 [上下文回填] 为 {len(bare_rows)} 个仓位补填 SPY Context: {spy_ctx}"
                        )
            except Exception as e:
                logger.warning(f"上下文回填守护异常: {e}")

            await asyncio.sleep(300)  # 每 5 分钟

    # 3R 巡检已迁移至 scale_out_monitor.py

    async def check_corporate_actions_job(self):
        try:
            async with connect_db() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT symbol, MIN(create_time) as earliest FROM shadow_ledger "
                    "WHERE status='OPEN' GROUP BY symbol"
                )
                rows = await cursor.fetchall()
            if not rows:
                return
            symbol_earliest: dict[str, str] = {r["symbol"]: r["earliest"] for r in rows}

            loop = asyncio.get_running_loop()
            for sym, earliest_create in symbol_earliest.items():
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
                            # ── 只处理仓位创建之后发生的拆股，避免历史拆股被重复执行 ──
                            if date_str <= earliest_create[:10]:
                                continue
                            async with connect_db() as db:
                                cur = await db.execute(
                                    "SELECT 1 FROM applied_splits WHERE symbol=? AND split_date=?",
                                    (sym, date_str),
                                )
                                if await cur.fetchone():
                                    continue
                                await db.execute(
                                    "UPDATE shadow_ledger SET quantity=quantity*?, entry_price=entry_price/?, "
                                    "initial_stop=initial_stop/?, current_stop=current_stop/? "
                                    "WHERE symbol=? AND status='OPEN'",
                                    (ratio_f, ratio_f, ratio_f, ratio_f, sym),
                                )
                                await db.execute(
                                    "INSERT INTO applied_splits (symbol, split_date, ratio) VALUES (?, ?, ?)",
                                    (sym, date_str, ratio_f),
                                )
                                await db.commit()
                            await self.gateway.notify_user(
                                f"✂️ **自动化拆股执行**\n"
                                f"系统检测到 `{sym}` 在 {date_str} 执行了 1:{ratio_f:g} 拆股。\n"
                                f"影子账本已静默等比例折算！"
                            )

                    if last_price is None or pd.isna(last_price):
                        await self.gateway.notify_user(f"⚠️ **公司代码异常预警**\n系统无法从雅虎财经获取 `{sym}`。该股票可能已**更名或退市**。\n请用 `/rename {sym} [新代码]` 修正账本！")
                except Exception as e:
                    logger.info(f"巡检 {sym} 公司行动异常: {e}")
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.info(f"🚨 公司行动巡检器总体异常: {e}")

    async def run_daily_boot_checks(self):
        try:
            logger.info("🌅 执行开机自检与复盘提醒...")
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
            logger.info(f"🚨 开机自检失败: {e}")


# IBKRListener 已迁移至 ib_listener.py
# TelegramGateway 已迁移至 gateway.py

app = RiskManagerApp()
tg_gateway = TelegramGateway(app)
ib_listener = IBKRListener(app, tg_gateway)
risk_engine = RiskEngine(app, ib_listener, tg_gateway)
app.set_components(tg_gateway, ib_listener, risk_engine)
# 注入 app_context 到需要模块级访问的子模块
set_app_context(app)
ib = app.ib


def get_symbol_lock(symbol: str) -> asyncio.Lock:
    return app.get_symbol_lock(symbol)


def _spawn_background_task(coro) -> asyncio.Task:
    return app.spawn_background_task(coro)


async def _enqueue_both(event_key: str, telegram_msg: str = "", notion_data: dict | None = None):
    """便捷：同时入队 Telegram + Notion 通道。"""
    if telegram_msg:
        await enqueue_outbound(event_key, "telegram", {"message": telegram_msg})
    if notion_data:
        notion_data["event_type"] = notion_data.get("event_type", event_key.split("-", 1)[1] if "-" in event_key else "UPDATE")
        await enqueue_outbound(event_key, "notion", notion_data)


async def build_service_status_lines() -> list[str]:
    return await tg_gateway.build_service_status_lines()


def bind_tg_bot(bot: Bot) -> None:
    tg_gateway.bind_bot(bot)


async def _notify_user(text: str) -> None:
    await tg_gateway.notify_user(text)


async def _notify_system_online(reason: str = "启动") -> None:
    """TWS 连上且监听就绪后，向 Telegram 推送一次上线通知。"""
    if not app.ib.isConnected() or app.tws_online_notified:
        return
    app.tws_online_notified = True
    status = "\n".join(await build_service_status_lines()).strip()
    await _notify_user(
        f"🟢 **风控军师系统已上线** ({reason})\n\n"
        f"{status}\n\n"
        f"👀 成交监听已挂载，桌面端下单成交后将自动入账。"
    )


async def _notify_bot_only_online() -> None:
    """Bot 已启动但 TWS 未连时的一次性提示。"""
    await _notify_user(
        "🟡 **军师 Bot 已启动，TWS 未连接**\n"
        "Telegram 指令可用，但成交监听暂不可用。\n"
        "请确认 TWS 已登录且 API 已开启，连上后会再推送上线通知。"
    )


async def ensure_ib_connected() -> bool:
    return await ib_listener.ensure_connected()


async def fetch_entry_price(symbol: str):
    return await ib_listener.fetch_entry_price(symbol)


async def fetch_account_equity() -> float:
    return await ib_listener.fetch_account_equity()


async def calculate_risk_light(conn, equity: float):
    return await risk_engine.calculate_risk_light(conn, equity)


async def validate_pending_entry(conn, entry_price, stop_price, quantity, equity, is_buy):
    return await risk_engine.validate_pending_entry(conn, entry_price, stop_price, quantity, equity, is_buy)


async def execute_kill_switch(symbol: str, trigger_reason: str = "手动授权") -> str:
    return await risk_engine.execute_kill_switch(symbol, trigger_reason)


async def get_10ema(symbol: str) -> float:
    return await risk_engine.get_10ema(symbol)


def calc_share_quantity(risk_budget: float, entry_price: float, stop_price: float) -> int:
    risk_per_share = abs(entry_price - stop_price)
    if risk_per_share <= 0 or risk_budget <= 0:
        return 0
    return int(risk_budget / risk_per_share)


async def sync_flex_query(context=None):
    """兼容旧调用：手动触发 Flex 结算。"""
    await app.sync_flex_query_job()



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


async def _pre_init_core() -> None:
    """阶段 0：初始化不依赖 Telegram 的核心组件（TWS / DB / 守护进程）。"""
    await ensure_schema()
    # ⚠️ 必须先连 TWS，再启动守护进程，避免 keepalive 并发抢占 clientId 0
    tws_ok = await ensure_ib_connected()
    app.initialize_background_tasks()
    if tws_ok:
        logger.info("✅ TWS 已连接，主客户端(Master Client)全局跨端监听已启动。")
        await asyncio.sleep(1)
        # 🚀 恢复开机自动拉取 Flex：MD5 哈希防重 + 1001 静默保护已就绪
        # 即使频繁重启，也不会触发 IBKR 封禁。开机强拉可第一时间补齐离线期间的结算数据。
        await reconcile_physical_positions(app.ib, tg_gateway.notify_user)
        app.spawn_background_task(run_flex_settlement(tg_gateway.notify_user, app.ib))
    else:
        logger.info("⚠️ TWS 未连接，/init 报价与成交同步将不可用。")


async def _post_init_telegram(tg_app: Application) -> None:
    """阶段 1：Telegram 就绪后同步命令 & 发送上线通知。"""
    bind_tg_bot(tg_app.bot)
    try:
        await sync_bot_commands(tg_app)
    except Exception as e:
        logger.info(f"⚠️ Telegram 命令菜单同步失败: {e}")
    try:
        await app.run_daily_boot_checks()
    except Exception as e:
        logger.info(f"⚠️ 开机自检通知发送失败（网络抖动）: {e}")
    if app.ib.isConnected():
        await _notify_system_online("启动")
    else:
        await _notify_bot_only_online()


# ── 代理清理：ALL_PROXY (socks5) 与 httpx 不兼容（缺少 httpx-socks），必须清除 ──
for _env_key in ("ALL_PROXY", "all_proxy"):
    os.environ.pop(_env_key, None)

PROXY_URL = os.getenv("HTTPS_PROXY", os.getenv("HTTP_PROXY", "http://127.0.0.1:7897"))


def main() -> None:
    if TG_BOT_TOKEN == "YOUR_TG_TOKEN":
        logger.info("❌ 致命错误：请在 .env 文件中配置真实的 TG_BOT_TOKEN！")
        sys.exit(1)

    tg_app = (
        Application.builder()
        .token(TG_BOT_TOKEN)
        .connect_timeout(15)
        .read_timeout(30)
        .build()
    )

    # 通过 telegram_router 统一注册所有指令处理器
    register_handlers(tg_app, app)

    if ENABLE_EOD_SNIPER and tg_app.job_queue:
        ny_tz = ZoneInfo("America/New_York")
        tg_app.job_queue.run_daily(
            eod_10ema_sniper_job,
            time=datetime.time(hour=15, minute=55, tzinfo=ny_tz),
            name="eod_10ema_sniper",
        )
        logger.info("🔭 [VPS 云端模式] EOD 收盘狙击手引擎已装载。")
    elif ENABLE_EOD_SNIPER:
        logger.info("⚠️ ENABLE_EOD_SNIPER=True 但 job_queue 不可用，EOD 狙击手未注册。")
    else:
        logger.info("💻 [本地关机模式] EOD 狙击手休眠，夜间防线由 TWS 物理止损单全面接管。")

    async def runner() -> None:
        # ── 阶段 0：TWS + 数据库 + 守护进程（无需 Telegram）──
        await _pre_init_core()

        post_init_done = False
        tg_ready = False

        for tg_retry in range(50):
            try:
                async with tg_app:
                    # ── 阶段 1：Telegram 上线通知 & 命令同步（仅一次）──
                    if not post_init_done:
                        await _post_init_telegram(tg_app)
                        post_init_done = True
                    await tg_app.start()
                    mode = "EOD 狙击手" if ENABLE_EOD_SNIPER else "物理止损兜底"
                    logger.info(f"✅ Telegram Bot 军师巡检器已启动 (状态自检 + {mode})。")

                    # ── 轮询（失败时在同一 context 内重试）──
                    for poll_retry in range(10):
                        try:
                            await tg_app.updater.start_polling(
                                drop_pending_updates=True, timeout=10, poll_interval=1.0
                            )
                            tg_ready = True
                            await asyncio.Event().wait()
                        except Exception as pe:
                            if any(kw in str(pe).lower() for kw in (
                                "httpx", "network", "connect", "remoteprotocol",
                                "timed out", "timeout",
                            )):
                                logger.info(f"⚠️ 轮询断开，{5}s 后重试 ({poll_retry + 1}/10): {pe}")
                                try:
                                    await tg_app.updater.stop()
                                except Exception:
                                    pass
                                await asyncio.sleep(5)
                            else:
                                raise
                    if tg_ready:
                        break  # 轮询正常结束（不会到这里）
            except Exception as e:
                if any(kw in str(e).lower() for kw in (
                    "httpx", "network", "connect", "remoteprotocol",
                    "timed out", "timeout", "still running",
                )):
                    delay = min(15 * (tg_retry + 1), 60)
                    logger.info(f"⚠️ Telegram 初始化失败，{delay}s 后重试 ({tg_retry + 1}/50): {e}")
                    await asyncio.sleep(delay)
                else:
                    logger.info(f"❌ 未知错误: {e}")
                    raise
        if not tg_ready:
            logger.info("❌ Telegram 轮询多次失败，请检查代理后重启。")

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        logger.info("风控军师已停止。")


if __name__ == "__main__":
    main()
