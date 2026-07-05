import asyncio
import datetime
import sys
import time
import uuid
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
import nest_asyncio
import pandas as pd
import pandas_market_calendars as mcal
import yfinance as yf
from ib_insync import IB, FlexReport, MarketOrder, Stock, util
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

ib = IB()
_background_tasks: set[asyncio.Task] = set()
_tg_bot: Bot | None = None
_ib_loop_patched = False


def _patch_ib_asyncio_once() -> None:
    """Telegram 先启动后再 patch，避免 httpx 报 AsyncLibraryNotFoundError。"""
    global _ib_loop_patched
    if _ib_loop_patched:
        return
    util.patchAsyncio()
    nest_asyncio.apply()
    _ib_loop_patched = True


from config import (
    CLIENT_ID,
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
    TWS_PORT,
)
from database import (
    connect_db,
    delete_pending_intent,
    ensure_schema,
    insert_shadow_ledger,
    load_pending_intent,
    save_pending_intent,
    upsert_account_state,
    get_today_trade_count,
)
from market_regime import REGIME_OFFLINE_LABEL, fetch_market_regime
from notion_api import push_to_notion


def bind_tg_bot(bot: Bot) -> None:
    global _tg_bot
    _tg_bot = bot


async def _notify_user(text: str) -> None:
    if _tg_bot is None:
        print(f"TG 未绑定，跳过通知: {text[:80]}...")
        return
    try:
        await _tg_bot.send_message(chat_id=MY_TELEGRAM_CHAT_ID, text=text)
    except Exception as e:
        print(f"TG 发送失败: {e}")


def _spawn_background_task(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


def require_auth(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat = update.effective_chat
        if chat is None or chat.id != MY_TELEGRAM_CHAT_ID:
            return
        return await func(update, context)

    return wrapper


async def ensure_ib_connected() -> bool:
    if ib.isConnected():
        return True
    _patch_ib_asyncio_once()
    try:
        await ib.connectAsync(TWS_HOST, TWS_PORT, clientId=CLIENT_ID, timeout=5)
        return True
    except Exception as e:
        print(f"TWS 未连接: {e}")
        return False


async def check_corporate_actions(tg_app_or_context=None) -> None:
    """盘前巡检：yfinance 检测拆股并自动折算账本，无法报价则预警更名/退市。"""
    async with connect_db() as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT DISTINCT symbol FROM shadow_ledger WHERE status='OPEN'"
        )
        rows = await cursor.fetchall()
    symbols = [r["symbol"] for r in rows]
    if not symbols:
        return

    loop = asyncio.get_running_loop()

    async def _notify(text: str) -> None:
        if tg_app_or_context is not None and hasattr(tg_app_or_context, "bot"):
            await tg_app_or_context.bot.send_message(
                chat_id=MY_TELEGRAM_CHAT_ID, text=text
            )
        else:
            await _notify_user(text)

    for sym in symbols:
        try:

            def fetch_data(ticker_str: str):
                tkr = yf.Ticker(ticker_str)
                splits = tkr.splits
                try:
                    last_price = tkr.fast_info["lastPrice"]
                except Exception:
                    last_price = None
                return splits, last_price

            splits, last_price = await loop.run_in_executor(None, fetch_data, sym)

            if splits is not None and not splits.empty:
                for split_date, ratio in splits.tail(3).items():
                    ratio_f = float(ratio)
                    if ratio_f <= 0 or ratio_f == 1.0:
                        continue
                    date_str = split_date.strftime("%Y-%m-%d")
                    async with connect_db() as db:
                        cur = await db.execute(
                            "SELECT 1 FROM applied_splits WHERE symbol=? AND split_date=?",
                            (sym, date_str),
                        )
                        if await cur.fetchone():
                            continue
                        await db.execute(
                            """
                            UPDATE shadow_ledger
                            SET quantity=quantity*?,
                                entry_price=entry_price/?,
                                initial_stop=initial_stop/?,
                                current_stop=current_stop/?
                            WHERE symbol=? AND status='OPEN'
                            """,
                            (ratio_f, ratio_f, ratio_f, ratio_f, sym),
                        )
                        await db.execute(
                            "INSERT INTO applied_splits (symbol, split_date, ratio) "
                            "VALUES (?, ?, ?)",
                            (sym, date_str, ratio_f),
                        )
                        await db.commit()
                    await _notify(
                        f"✂️ **自动化拆股执行**\n"
                        f"系统检测到 `{sym}` 在 {date_str} 执行了 1:{ratio_f:g} 拆股。\n"
                        f"影子账本已静默自动等比例折算，风控防线已调整！"
                    )

            if last_price is None or pd.isna(last_price):
                await _notify(
                    f"⚠️ **公司代码异常预警**\n"
                    f"系统无法从雅虎财经获取 `{sym}` 的最新行情数据。\n"
                    f"该股票可能已**更名、被收购或退市**。\n"
                    f"请在 TWS 核实后，使用 `/rename {sym} [新代码]` 手动修正账本！"
                )
        except Exception as e:
            print(f"巡检 {sym} 公司行动异常: {e}")
        await asyncio.sleep(0.5)


async def fetch_entry_price(symbol: str):
    if not await ensure_ib_connected():
        return None, ""
    contract = Stock(symbol.upper(), "SMART", "USD")
    qualified = await ib.qualifyContractsAsync(contract)
    if not qualified:
        return None, ""

    tickers = await ib.reqTickersAsync(contract)
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


async def fetch_account_equity() -> float:
    if not await ensure_ib_connected():
        return 0.0
    try:
        tags = await ib.accountSummaryAsync()
        for row in tags:
            if row.tag == "NetLiquidation" and row.currency == "USD":
                try:
                    return float(row.value)
                except ValueError:
                    pass
    except Exception as e:
        print(f"净值读取失败: {e}")
    return 0.0


async def calculate_risk_light(db_connection, current_total_equity: float):
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


def calc_share_quantity(risk_budget: float, entry_price: float, stop_price: float) -> int:
    risk_per_share = abs(entry_price - stop_price)
    if risk_per_share <= 0 or risk_budget <= 0:
        return 0
    return int(risk_budget / risk_per_share)


async def validate_pending_entry(
    conn,
    entry_price: float,
    stop_price: float,
    quantity: float,
    equity: float,
) -> str | None:
    """确认入账前二次校验，通过返回 None，否则返回拒绝文案。"""
    today_count = await get_today_trade_count(conn)
    if today_count >= MAX_DAILY_TRADES:
        return (
            f"🛑 **狙击手协议触发！**\n"
            f"今日弹夹已打空 ({today_count}/{MAX_DAILY_TRADES})。请重新 /init。"
        )

    risk_light, base_risk_budget = await calculate_risk_light(conn, equity)
    if base_risk_budget <= 0:
        return f"🚨 风控灯 {risk_light}，当前禁止新开仓。请重新 /init。"

    _, _, risk_mult = await fetch_market_regime()
    if risk_mult == 0.0:
        return "🛑 **宏观环境一票否决！** Bear Thrust 下禁止建仓。请重新 /init。"

    if entry_price > 0:
        risk_pct = abs(entry_price - stop_price) / entry_price
        if risk_pct > MAX_STOP_PCT:
            return (
                f"🚨 止损距离 {risk_pct * 100:.2f}% 超过 {MAX_STOP_PCT * 100:.0f}% 上限。"
                f"请重新 /init。"
            )

    cursor = await conn.execute(
        "SELECT entry_price, current_stop, quantity FROM shadow_ledger WHERE status='OPEN'"
    )
    open_positions = await cursor.fetchall()
    current_total_risk = 0.0
    for pos in open_positions:
        pos_entry = float(pos["entry_price"])
        pos_stop = float(pos["current_stop"])
        pos_qty = float(pos["quantity"])
        current_total_risk += max(0.0, abs(pos_entry - pos_stop) * pos_qty)

    new_trade_risk = abs(entry_price - stop_price) * quantity
    projected_total_risk = current_total_risk + new_trade_risk
    max_allowed_risk = equity * MAX_OVERNIGHT_RISK_PCT
    if projected_total_risk > max_allowed_risk:
        pct_str = (
            f"{(projected_total_risk / equity) * 100:.2f}%"
            if equity > 0
            else "N/A"
        )
        return (
            f"🛑 **隔夜风险总闸触发！**\n"
            f"合并后风险 ${projected_total_risk:.2f} ({pct_str})，"
            f"超过极限 {MAX_OVERNIGHT_RISK_PCT * 100:.2f}%。请重新 /init。"
        )

    return None


def _flex_trade_attr(trade, *names, default=""):
    for name in names:
        val = getattr(trade, name, None)
        if val is not None and val != "":
            return val
    return default


async def sync_flex_query(tg_app_or_context=None):
    """完全静默的 Flex 对账：无平仓则不通知。"""
    if FLEX_TOKEN.startswith("YOUR_") or FLEX_QUERY_ID.startswith("YOUR_"):
        return

    nyse = mcal.get_calendar("NYSE")
    today = datetime.datetime.now()
    valid_days = nyse.valid_days(
        start_date=today - datetime.timedelta(days=5),
        end_date=today,
    )
    if len(valid_days) < 2:
        return
    last_trading_day = valid_days[-2].date()
    if today.date() - last_trading_day > datetime.timedelta(days=1):
        return

    try:
        loop = asyncio.get_running_loop()
        report = await loop.run_in_executor(
            None, lambda: FlexReport(FLEX_TOKEN, FLEX_QUERY_ID)
        )
        async with connect_db() as db_connection:
            db_connection.row_factory = aiosqlite.Row
            closed = 0
            total_pnl = 0.0
            closed_symbols: list[str] = []
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
                        "fifoPnlRealized",
                        "realizedPL",
                        "realizedPnl",
                        "RealizedP/L",
                        default=0,
                    )
                    or 0
                )
                cursor = await db_connection.execute(
                    "SELECT id, setup_tag FROM shadow_ledger "
                    "WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC LIMIT 1",
                    (symbol,),
                )
                row = await cursor.fetchone()
                if row:
                    trade_id = row["id"]
                    setup_tag = row["setup_tag"] if row["setup_tag"] else ""
                    await db_connection.execute(
                        "UPDATE shadow_ledger SET status='CLOSED', exit_price=?, realized_pnl=? WHERE id=?",
                        (exec_price, realized_pnl, trade_id),
                    )
                    _spawn_background_task(
                        push_to_notion(trade_id, symbol, realized_pnl, setup_tag)
                    )
                    closed += 1
                    total_pnl += realized_pnl
                    closed_symbols.append(symbol)
            await db_connection.commit()

        if closed > 0:
            pnl_str = (
                f"+${total_pnl:.2f}" if total_pnl > 0 else f"-${abs(total_pnl):.2f}"
            )
            msg = (
                f"✅ **盘前静默清算完成**\n"
                f"成功对账并关闭 {closed} 笔仓位：{', '.join(closed_symbols)}\n"
                f"合计盈亏: {pnl_str}\n"
                f"数据已自动归档至复盘库。"
            )
            if tg_app_or_context is not None and hasattr(tg_app_or_context, "bot"):
                await tg_app_or_context.bot.send_message(
                    chat_id=MY_TELEGRAM_CHAT_ID, text=msg
                )
            else:
                await _notify_user(msg)
    except Exception as e:
        print(f"Flex 对账失败: {e}")


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

    physical_inventory = {}
    for pos in positions:
        if pos.contract.secType == "STK" and pos.position != 0:
            physical_inventory[pos.contract.symbol] = float(pos.position)

    async with connect_db() as conn:
        conn.row_factory = aiosqlite.Row

        cursor = await conn.execute(
            "SELECT symbol, quantity FROM shadow_ledger WHERE status='OPEN'"
        )
        ledger_positions = await cursor.fetchall()

        expected_inventory = {}
        for row in ledger_positions:
            sym = row["symbol"]
            expected_inventory[sym] = expected_inventory.get(sym, 0.0) + float(row["quantity"])

        ghost_alerts = []

        for sym, expected_qty in expected_inventory.items():
            actual_qty = physical_inventory.get(sym, 0.0)
            if actual_qty < expected_qty:
                discrepancy = expected_qty - actual_qty
                ghost_alerts.append(
                    f"👻 **发现幽灵平仓 [{sym}]**\n"
                    f"账本预期: {expected_qty} 股 | TWS实际: {actual_qty} 股\n"
                    f"*(系统已强制启动 FIFO 清剿修复)*"
                )

                cursor = await conn.execute(
                    "SELECT id, quantity, setup_tag FROM shadow_ledger "
                    "WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC",
                    (sym,),
                )
                open_tranches = await cursor.fetchall()
                remaining_to_close = discrepancy

                for tranche in open_tranches:
                    if remaining_to_close <= 0:
                        break
                    t_id = tranche["id"]
                    t_qty = float(tranche["quantity"])

                    if t_qty <= remaining_to_close:
                        await conn.execute(
                            "UPDATE shadow_ledger SET status='CLOSED', exit_price=0, realized_pnl=0 WHERE id=?",
                            (t_id,),
                        )
                        remaining_to_close -= t_qty
                    else:
                        await conn.execute(
                            "UPDATE shadow_ledger SET quantity=? WHERE id=?",
                            (t_qty - remaining_to_close, t_id),
                        )
                        remaining_to_close = 0

        for sym, actual_qty in physical_inventory.items():
            expected_qty = expected_inventory.get(sym, 0.0)
            if actual_qty > expected_qty:
                ghost_alerts.append(
                    f"⚠️ **发现未授权物理仓位 [{sym}]**\n"
                    f"账本预期: {expected_qty} 股 | TWS实际: {actual_qty} 股\n"
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


async def execute_kill_switch(symbol: str, trigger_reason: str = "手动授权") -> str:
    """核心斩立决逻辑，支持 Telegram 手动与 10EMA 自动触发。"""
    if not await ensure_ib_connected():
        return "❌ TWS 未连接，清仓失败。请手动在 TWS 操作！"

    try:
        contract = Stock(symbol, "SMART", "USD")
        qualified = await ib.qualifyContractsAsync(contract)
        if not qualified:
            return f"❌ 无法验证合约 `{symbol}`。"

        positions = await ib.reqPositionsAsync()
        actual_qty = 0.0
        for pos in positions:
            if (
                pos.contract.secType == "STK"
                and pos.contract.symbol == symbol
                and pos.position != 0
            ):
                actual_qty = float(pos.position)
                break

        if actual_qty == 0:
            return f"ℹ️ {symbol} TWS 实盘持仓已为 0，跳过斩立决。"

        canceled_count = 0
        for trade in ib.trades():
            if trade.contract.symbol == symbol and not trade.isDone():
                await ib.cancelOrderAsync(trade.order)
                canceled_count += 1

        if canceled_count > 0:
            await asyncio.sleep(1.5)

        action = "SELL" if actual_qty > 0 else "BUY"
        mkt_order = MarketOrder(action, abs(actual_qty))
        ib.placeOrder(contract, mkt_order)

        return (
            f"💀 **斩立决已成功执行 [{symbol}]**\n"
            f"触发原因: {trigger_reason}\n"
            f"已撤销 {canceled_count} 笔保护挂单\n"
            f"发送市价单: {action} {abs(actual_qty):.0f} 股。\n"
            f"*(系统将通过异步成交回报自动冲销账本)*"
        )
    except Exception as e:
        return f"❌ 斩立决发生异常: {e}"


@require_auth
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🛡️ 极客级动量风控军师\n\n"
        "/init [代码] [止损价] [策略标签] — 开仓前审查\n"
        "/status — 持仓与风控灯\n"
        "/override [代码] [坦白理由] — 越权坦白\n"
        "/update [代码] [新止损价] — 移动止损\n"
        "/split [代码] [比例] — 拆/合股调整\n"
        "/rename [旧代码] [新代码] — 代码更名\n"
        "/unlock [代码] [检讨≥15字] — 解锁桌面端 F9 越权下单\n"
        "/sync — 手动 Flex 对账"
    )
    await update.message.reply_text(text)


@require_auth
async def cmd_unlock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text(
            "⚠️ 格式错误。\n正确格式: /unlock [Ticker] [不少于15个字的检讨理由]"
        )
        return
    symbol = args[0].upper()
    confession = " ".join(args[1:]).strip()
    if len(confession) < 15:
        await update.message.reply_text(
            f"❌ 检讨不够深刻：当前 {len(confession)} 字，至少需要 15 字。\n"
            f"示例：/unlock TSLA 这是一个完美的VCP突破，严格止损在昨日低点下方"
        )
        return

    try:
        async with connect_db() as conn:
            await conn.execute(
                "INSERT OR REPLACE INTO auth_tokens (symbol, confession, expire_time) "
                "VALUES (?, ?, datetime('now', '+5 minutes'))",
                (symbol, confession),
            )
            await conn.commit()
    except Exception as e:
        await update.message.reply_text(
            f"❌ 写入解锁令牌失败: {e}\n请先重启军师并运行 `python database_setup.py` 建表。"
        )
        return

    await update.message.reply_text(
        f"🔓 **授权已下发**\n"
        f"[{symbol}] 的极速下单(F9)权限已解锁，有效期 5 分钟。\n"
        f"理由：{confession}"
    )


async def morning_review_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [
        [
            InlineKeyboardButton("1 (失控)", callback_data="review_1"),
            InlineKeyboardButton("2", callback_data="review_2"),
            InlineKeyboardButton("3 (及格)", callback_data="review_3"),
            InlineKeyboardButton("4", callback_data="review_4"),
            InlineKeyboardButton("5 (完美)", callback_data="review_5"),
        ]
    ]
    await context.bot.send_message(
        chat_id=MY_TELEGRAM_CHAT_ID,
        text=(
            "🌅 **晨间审判 (Morning Review)**\n\n"
            "请客观评估你**昨天**的纪律执行情况：\n"
            "(如：是否无脑追高、是否执行了3R减仓、是否知行合一)"
        ),
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def reset_daily_status_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """盘前重置连亏计数，恢复当日交易额度感知。"""
    async with connect_db() as conn:
        await conn.execute(
            "UPDATE system_state SET value='0' WHERE key='consecutive_losses'"
        )
        await conn.commit()
    await context.bot.send_message(
        chat_id=MY_TELEGRAM_CHAT_ID,
        text=(
            "🔄 **每日盘前重置**\n"
            "系统连亏状态已归零，您的交易额度已恢复，今天也要坚守纪律！"
        ),
    )


def _shanghai_day_utc_bounds(day_offset: int = 0) -> tuple[str, str, str]:
    """返回 (date_iso, start_utc, end_utc)，按北京时间日历日切。"""
    shanghai = ZoneInfo("Asia/Shanghai")
    target_day = (
        datetime.datetime.now(shanghai) - datetime.timedelta(days=day_offset)
    ).date()
    day_start = datetime.datetime.combine(
        target_day, datetime.time.min, tzinfo=shanghai
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
    yesterday, start_utc, end_utc = _shanghai_day_utc_bounds(day_offset=1)
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
    equity = await fetch_account_equity()
    async with connect_db() as conn:
        conn.row_factory = aiosqlite.Row
        risk_light, risk_budget = await calculate_risk_light(conn, equity)
        await upsert_account_state(conn, equity, risk_light)

        cur = await conn.execute(
            "SELECT symbol, tranche_id, quantity, entry_price, current_stop, setup_tag "
            "FROM shadow_ledger WHERE status='OPEN' ORDER BY symbol, create_time"
        )
        rows = await cur.fetchall()

        lines = [
            f"💰 净值: ${equity:,.2f}",
            f"🚦 风控灯: {risk_light}",
            f"📐 单笔风险预算: ${risk_budget:,.2f}",
            "",
        ]
        if not rows:
            lines.append("📭 当前无 OPEN 影子仓位。")
        else:
            lines.append("📒 影子账本 OPEN:")
            for r in rows:
                lines.append(
                    f"  • {r['symbol']} {r['tranche_id']} "
                    f"qty={r['quantity']:.0f} @ {r['entry_price']:.2f} "
                    f"stop={r['current_stop']:.2f} [{r['setup_tag']}]"
                )
        await update.message.reply_text("\n".join(lines))


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
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("⚠️ 格式错误！/override [代码] [坦白理由]")
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
                await update.message.reply_text(f"ℹ️ {symbol} 无待坦白的越权仓位。")
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

        await update.message.reply_text(
            f"💀 已记录坦白: {symbol} - {reason}\n这笔交易已被接纳，但打上了违规标签。"
        )
    except Exception as e:
        print(f"Override Error: {e}")
        await update.message.reply_text(f"❌ 坦白记录失败: {e}")


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
                SET quantity=quantity*?,
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


def _dispatch_async(coro):
    try:
        asyncio.get_running_loop()
        _spawn_background_task(coro)
    except RuntimeError:
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(_spawn_background_task, coro)


def on_execution(trade):
    _dispatch_async(_async_on_execution(trade))


async def _apply_consecutive_losses(conn, profitable: bool, losing: bool) -> None:
    if profitable:
        await conn.execute(
            "UPDATE system_state SET value='0' WHERE key='consecutive_losses'"
        )
    elif losing:
        cur = await conn.execute(
            "SELECT value FROM system_state WHERE key='consecutive_losses'"
        )
        row = await cur.fetchone()
        losses = int(row[0]) if row else 0
        await conn.execute(
            "UPDATE system_state SET value=? WHERE key='consecutive_losses'",
            (str(losses + 1),),
        )


async def _night_watchman_on_tp(trade, execution) -> None:
    """分批止盈成交后，将 TWS 止损单推移至保本价。"""
    order = trade.order
    if not order or order.orderType != "LMT":
        return
    is_long_tp = execution.side == "SLD"
    is_short_tp = execution.side == "BOT"
    if not is_long_tp and not is_short_tp:
        return
    if not await ensure_ib_connected():
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

        for open_trade in ib.openTrades():
            if open_trade.contract.symbol != symbol:
                continue
            if open_trade.order.orderType not in ("STP", "STP LMT"):
                continue
            stop_order = open_trade.order
            old_stop = float(stop_order.auxPrice)
            need_modify = False
            if pos_side == "LONG" and old_stop < entry_price:
                stop_order.auxPrice = entry_price
                need_modify = True
            elif pos_side == "SHORT" and old_stop > entry_price:
                stop_order.auxPrice = entry_price
                need_modify = True
            if not need_modify:
                continue
            ib.placeOrder(open_trade.contract, stop_order)
            async with connect_db() as conn:
                await conn.execute(
                    "UPDATE shadow_ledger SET current_stop=? WHERE symbol=? AND status='OPEN'",
                    (entry_price, symbol),
                )
                await conn.commit()
            await _notify_user(
                f"🛡️ **守夜人协议触发**\n"
                f"`{symbol}` 的分批止盈单已成交！\n"
                f"后台已将剩余仓位的止损单推移至保本价 ${entry_price:.2f}。\n"
                f"您可以安心睡觉了。"
            )
            break
    except Exception as e:
        print(f"守夜人保本钩子失败: {e}")


async def _async_on_execution(trade):
    execution = trade.execution
    contract = trade.contract
    symbol = contract.symbol
    side = "LONG" if execution.side == "BOT" else "SHORT"
    qty = float(execution.shares)
    price = float(execution.price)
    setup_tag = trade.order.orderRef if trade.order and trade.order.orderRef else ""

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
                await conn.execute(
                    "UPDATE shadow_ledger SET quantity=?, entry_price=?, setup_tag=? WHERE id=?",
                    (new_qty, new_entry, merged_tag, same_day_row["id"]),
                )
                await conn.commit()
                msg = (
                    f"🎯 **前端火力捕获 (同日加仓)**\n"
                    f"已接管来自 UI 的加仓指令：`{symbol}`\n"
                    f"新增: {qty:.0f}股 @ ${price:.2f} (均价拉至 ${new_entry:.2f})\n"
                    f"策略: {merged_tag or '未打标'}"
                )
                _spawn_background_task(_notify_user(msg))
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
                    f"策略: {setup_tag or '未打标'}\n"
                    f"*(止损线将在 2 秒内自动同步防线)*"
                )
                _spawn_background_task(_notify_user(msg))
                _spawn_background_task(_delayed_bracket_stop_capture(symbol))
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
                    cursor = await conn.execute(
                        "SELECT setup_tag FROM shadow_ledger WHERE id=?", (t_id,)
                    )
                    tag_row = await cursor.fetchone()
                    setup_tag = tag_row["setup_tag"] if tag_row and tag_row["setup_tag"] else ""
                    if (tranche_side == "LONG" and price < t_entry) or (
                        tranche_side == "SHORT" and price > t_entry
                    ):
                        had_loss = True
                    elif (tranche_side == "LONG" and price > t_entry) or (
                        tranche_side == "SHORT" and price < t_entry
                    ):
                        had_profit = True
                    await conn.execute(
                        "UPDATE shadow_ledger SET status='CLOSED', exit_price=? WHERE id=?",
                        (price, t_id),
                    )
                    _spawn_background_task(push_to_notion(t_id, symbol, 0.0, setup_tag))
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
                        if tranche_side == "LONG":
                            trim_detail = f"成交价: ${price:.2f} > 成本价: ${t_entry:.2f}"
                        else:
                            trim_detail = f"成交价: ${price:.2f} < 成本价: ${t_entry:.2f}"
                        msg = (
                            f"🤖 **自动护航：** 侦测到 `{symbol}` 盈利减仓！\n"
                            f"{trim_detail}\n"
                            f"系统已自动将剩余 {new_qty:.0f} 股止损推至成本价 ${t_entry:.2f}。\n"
                            f"🛡️ **该笔交易风控额度已完全释放！**"
                        )
                        _spawn_background_task(_notify_user(msg))
                    else:
                        if (tranche_side == "LONG" and price < t_entry) or (
                            tranche_side == "SHORT" and price > t_entry
                        ):
                            had_loss = True
                        await conn.execute(
                            "UPDATE shadow_ledger SET quantity=? WHERE id=?",
                            (new_qty, t_id),
                        )
                    remaining_exit_qty = 0

            await _apply_consecutive_losses(conn, had_profit, had_loss)

        await conn.commit()

    if not opening:
        _spawn_background_task(_night_watchman_on_tp(trade, execution))


async def _delayed_bracket_stop_capture(symbol: str):
    """延迟 2 秒主动扫描 TWS 未决止损单，修复 openOrder 先于成交的竞态。"""
    await asyncio.sleep(2.0)
    if not ib.isConnected():
        return
    try:
        found_stop = None
        for open_trade in ib.openTrades():
            if (
                open_trade.contract.symbol == symbol
                and open_trade.order.orderType in ("STP", "STP LMT")
            ):
                found_stop = float(open_trade.order.auxPrice)
                break
        if not found_stop or found_stop <= 0:
            return
        async with connect_db() as conn:
            cursor = await conn.execute(
                """
                UPDATE shadow_ledger
                SET current_stop=?,
                    initial_stop=?
                WHERE symbol=? AND status='OPEN' AND initial_stop=0.0
                """,
                (found_stop, found_stop, symbol),
            )
            if cursor.rowcount > 0:
                await conn.commit()
                msg = (
                    f"🛡️ **防线主动同步完毕：** `{symbol}` "
                    f"的底层止损已被锚定在 ${found_stop:.2f}。"
                )
                await _notify_user(msg)
    except Exception as e:
        print(f"延迟捕获止损出错: {e}")


def on_open_order(trade):
    """拦截 TWS 未决订单变更（含图表拖拽修改止损）。"""
    _dispatch_async(_async_on_open_order(trade))


async def _async_on_open_order(trade):
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

        if new_stop >= entry_price:
            risk_status = "✅ 止损已推至成本区以上，零风险/锁定利润！"
        else:
            risk_pct = abs(entry_price - new_stop) / entry_price * 100
            risk_status = f"📐 当前风险距离: {risk_pct:.2f}%"

        msg = (
            f"🖱️ **TWS 图表同步捕获：** `{symbol}`\n"
            f"侦测到止损单修改：${old_stop:.2f} ➡️ **${new_stop:.2f}**\n"
            f"影子账本已自动同步更新。\n"
            f"{risk_status}"
        )
        await _notify_user(msg)
    except Exception as e:
        print(f"图表同步止损失败: {e}")


async def heartbeat_2300(tg_application):
    shanghai_tz = ZoneInfo("Asia/Shanghai")
    while True:
        now = datetime.datetime.now(shanghai_tz)
        target = now.replace(hour=23, minute=0, second=0, microsecond=0)
        if now >= target:
            target += datetime.timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())

        async with connect_db() as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(
                "SELECT symbol, side, quantity FROM shadow_ledger "
                "WHERE status='OPEN' AND initial_stop = 0.0"
            )
            rows = await cursor.fetchall()
            for row in rows:
                await tg_application.bot.send_message(
                    chat_id=MY_TELEGRAM_CHAT_ID,
                    text=(
                        f"🚨 越权交易 {row['symbol']} ({row['side']} {row['quantity']:.0f})！"
                        f"请发送 /override {row['symbol']} [坦白理由]！"
                    ),
                )


async def get_10ema(symbol: str) -> float:
    """利用 IBKR 原生历史数据计算昨日 10EMA，避免盘中漂移。"""
    try:
        if not await ensure_ib_connected():
            return 0.0
        contract = Stock(symbol.upper(), "SMART", "USD")
        qualified = await ib.qualifyContractsAsync(contract)
        if not qualified:
            return 0.0

        bars = await ib.reqHistoricalDataAsync(
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


async def active_position_monitor(tg_application):
    """【3R 腾挪与 10EMA 追踪引擎】后台每 5 分钟巡检一次。"""
    await asyncio.sleep(15)
    notified_3r: dict[str, float] = {}
    notified_ema: set[str] = set()
    remind_3r_interval = 4 * 3600

    print("✅ 动态仓位巡检器 (Scale-out Financer) 已在后台启动...")

    while True:
        try:
            if not ib.isConnected():
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
                trade_id = pos["id"]
                symbol = pos["symbol"]
                side = pos["side"]
                entry = float(pos["entry_price"])
                initial_stop = float(pos["initial_stop"])
                current_stop = float(pos["current_stop"])

                current_price, _ = await fetch_entry_price(symbol)
                if not current_price or current_price <= 0:
                    continue

                one_r_risk = abs(entry - initial_stop)
                if one_r_risk <= 0:
                    continue

                if side == "LONG":
                    current_profit = current_price - entry
                else:
                    current_profit = entry - current_price
                current_r_multiple = current_profit / one_r_risk

                at_risk = (
                    current_stop < entry if side == "LONG" else current_stop > entry
                )
                at_breakeven = (
                    current_stop >= entry if side == "LONG" else current_stop <= entry
                )

                # 规则 1：3R 强制减仓与平推止损提醒（3R 区内每 4 小时重复督促）
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
                            f"1. 请立刻在 TWS **市价卖出 1/4 或 1/3**，用市场的钱为自己买下免费门票！\n"
                            f"2. 成交后系统将**自动**把剩余仓位止损推至成本价，无需手动 `/update`。\n\n"
                            f"*(注：平推完成后风控额度即时释放，尾仓可去冲击 10R！)*"
                        )
                        await tg_application.bot.send_message(
                            chat_id=MY_TELEGRAM_CHAT_ID, text=alert_msg
                        )
                        notified_3r[cache_key_3r] = now_ts

                elif at_breakeven:
                    notified_3r.pop(cache_key_3r, None)
                    ema_10 = await get_10ema(symbol)
                    ema_break = (
                        current_price < (ema_10 * 0.99)
                        if side == "LONG"
                        else current_price > (ema_10 * 1.01)
                    )
                    if ema_10 > 0 and ema_break:
                        cache_key = f"{trade_id}_10EMA"
                        if cache_key not in notified_ema:
                            notified_ema.add(cache_key)
                            alert_msg = (
                                f"🛑 **【尾仓趋势终结 - 机器代管介入】** `{symbol}`\n\n"
                                f"当前价格: ${current_price:.2f}\n"
                                f"昨日 10EMA: ${ema_10:.2f}\n\n"
                                f"⚠️ 价格已有效跌破 10EMA，动量已衰竭！\n"
                                f"🤖 **系统判定为危险，正在自动执行斩立决程序...**"
                            )
                            await tg_application.bot.send_message(
                                chat_id=MY_TELEGRAM_CHAT_ID,
                                text=alert_msg,
                            )
                            kill_result = await execute_kill_switch(
                                symbol, trigger_reason="10EMA跌破机器自动熔断"
                            )
                            await tg_application.bot.send_message(
                                chat_id=MY_TELEGRAM_CHAT_ID,
                                text=kill_result,
                            )

                else:
                    notified_3r.pop(cache_key_3r, None)

        except Exception as e:
            print(f"后台巡检出错: {e}")

        await asyncio.sleep(300)


async def ib_keepalive():
    while True:
        if not ib.isConnected():
            await ensure_ib_connected()
            if ib.isConnected():
                ib.reqAutoOpenOrders(True)
                if on_execution not in ib.execDetailsEvent:
                    ib.execDetailsEvent += on_execution
                if on_open_order not in ib.openOrderEvent:
                    ib.openOrderEvent += on_open_order
        await asyncio.sleep(30)


async def automated_pre_market_routine(context: ContextTypes.DEFAULT_TYPE) -> None:
    """20:30 后台静默 Flex 对账 + 公司行动巡检（不关机的双重保障）。"""
    await sync_flex_query(context)
    await check_corporate_actions(context)


async def _post_init(app: Application) -> None:
    bind_tg_bot(app.bot)
    await ensure_schema()
    _spawn_background_task(sync_flex_query(app))
    _spawn_background_task(check_corporate_actions(app))
    if await ensure_ib_connected():
        ib.reqAutoOpenOrders(True)
        if on_execution not in ib.execDetailsEvent:
            ib.execDetailsEvent += on_execution
        if on_open_order not in ib.openOrderEvent:
            ib.openOrderEvent += on_open_order
        print("✅ TWS 已连接，主客户端(Master Client)全局跨端监听已启动。")
        await reconcile_physical_positions(app)
    else:
        print("⚠️ TWS 未连接，/init 报价与成交同步将不可用，Telegram 仍可运行。")
    if app.job_queue:
        shanghai_tz = ZoneInfo("Asia/Shanghai")
        app.job_queue.run_daily(
            morning_review_job,
            time=datetime.time(9, 0, tzinfo=shanghai_tz),
            name="morning_review",
        )
        app.job_queue.run_daily(
            reset_daily_status_job,
            time=datetime.time(20, 0, tzinfo=shanghai_tz),
            name="reset_daily_status",
        )
        app.job_queue.run_daily(
            automated_pre_market_routine,
            time=datetime.time(20, 30, tzinfo=shanghai_tz),
            name="pre_market_routine",
        )
        print("✅ 定时任务已注册 (09:00 晨间审判 / 20:00 连亏重置 / 20:30 静默对账巡检)")
    _spawn_background_task(heartbeat_2300(app))
    _spawn_background_task(ib_keepalive())
    _spawn_background_task(active_position_monitor(app))


def main() -> None:
    tg_app = (
        Application.builder()
        .token(TG_BOT_TOKEN)
        .post_init(_post_init)
        .build()
    )

    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(CommandHandler("init", cmd_init))
    tg_app.add_handler(CommandHandler("status", cmd_status))
    tg_app.add_handler(CommandHandler("sync", cmd_sync))
    tg_app.add_handler(CommandHandler("override", cmd_override))
    tg_app.add_handler(CommandHandler("update", cmd_update))
    tg_app.add_handler(CommandHandler("split", cmd_split))
    tg_app.add_handler(CommandHandler("rename", cmd_rename))
    tg_app.add_handler(CommandHandler("unlock", cmd_unlock))
    tg_app.add_handler(CallbackQueryHandler(review_callback, pattern="^review_"))
    tg_app.add_handler(CallbackQueryHandler(button_handler))

    async def runner() -> None:
        async with tg_app:
            await tg_app.start()
            print("✅ Telegram Bot 轮询已启动。")
            await tg_app.updater.start_polling(drop_pending_updates=True)
            await asyncio.Event().wait()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        print("风控军师已停止。")


if __name__ == "__main__":
    main()
