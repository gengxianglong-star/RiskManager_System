import asyncio
import datetime
import sys
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
import pandas_market_calendars as mcal
from ib_insync import IB, FlexReport, Stock
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

from config import (
    CLIENT_ID,
    DB_PATH,
    FLEX_QUERY_ID,
    FLEX_TOKEN,
    MAX_POSITION_SIZE_PCT,
    MAX_STOP_PCT,
    MY_TELEGRAM_CHAT_ID,
    RISK_MAX_DRAWDOWN_GREEN,
    RISK_MAX_DRAWDOWN_YELLOW,
    RISK_PCT_PER_TRADE,
    TG_BOT_TOKEN,
    TWS_HOST,
    TWS_PORT,
)
from database import (
    delete_pending_intent,
    ensure_schema,
    insert_shadow_ledger,
    load_pending_intent,
    save_pending_intent,
    upsert_account_state,
)
from notion_api import push_to_notion

ib = IB()


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
    try:
        await ib.connectAsync(TWS_HOST, TWS_PORT, clientId=CLIENT_ID, timeout=5)
        return True
    except Exception as e:
        print(f"TWS 未连接: {e}")
        return False


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


def fetch_spy_context_sync() -> str:
    try:
        from finvizfinance.quote import finvizfinance

        spy = finvizfinance("SPY")
        fund = spy.ticker_fundament()
        if fund is not None and not fund.empty:
            change = fund.loc["Change", "SPY"] if "Change" in fund.index else None
            if change is not None:
                return f"SPY {change}"
    except Exception as e:
        print(f"Finviz SPY 语境失败: {e}")
    return "SPY N/A"


async def fetch_spy_context() -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, fetch_spy_context_sync)


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


def _flex_trade_attr(trade, *names, default=""):
    for name in names:
        val = getattr(trade, name, None)
        if val is not None and val != "":
            return val
    return default


async def sync_flex_query():
    print("⏳ 开始执行盘前静默对账协议...")
    if FLEX_TOKEN.startswith("YOUR_") or FLEX_QUERY_ID.startswith("YOUR_"):
        print("💤 未配置 Flex Token，跳过对账。")
        return

    nyse = mcal.get_calendar("NYSE")
    today = datetime.datetime.now()
    valid_days = nyse.valid_days(
        start_date=today - datetime.timedelta(days=5),
        end_date=today,
    )
    if len(valid_days) < 2:
        print("💤 交易日历不足，跳过 Flex 对账。")
        return

    last_trading_day = valid_days[-2].date()
    if today.date() - last_trading_day > datetime.timedelta(days=1):
        print("💤 上个日历日非交易日，跳过 Flex 对账。")
        return

    try:
        report = FlexReport(FLEX_TOKEN, FLEX_QUERY_ID)
        async with aiosqlite.connect(DB_PATH) as db_connection:
            db_connection.row_factory = aiosqlite.Row
            closed = 0
            for trade in report.extract("Trade"):
                symbol = _flex_trade_attr(trade, "symbol", "underlyingSymbol")
                if not symbol:
                    continue
                exec_price = float(_flex_trade_attr(trade, "tradePrice", "TradePrice", default=0) or 0)
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
                    "SELECT id, setup_tag FROM shadow_ledger WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC LIMIT 1",
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
                    asyncio.create_task(push_to_notion(trade_id, symbol, realized_pnl, setup_tag))
                    closed += 1
            await db_connection.commit()
            print(f"✅ Flex 对账完成，关闭 {closed} 条影子仓位。")
    except Exception as e:
        print(f"❌ Flex 账单拉取失败: {e}")


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

    async with aiosqlite.connect(DB_PATH) as conn:
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
        "/sync — 手动 Flex 对账"
    )
    await update.message.reply_text(text)


@require_auth
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    equity = await fetch_account_equity()
    async with aiosqlite.connect(DB_PATH) as conn:
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
    await sync_flex_query()
    await update.message.reply_text("✅ Flex 对账任务已执行，详见控制台日志。")


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
        async with aiosqlite.connect(DB_PATH) as conn:
            conn.row_factory = aiosqlite.Row
            risk_light, risk_budget = await calculate_risk_light(conn, equity)
            await upsert_account_state(conn, equity, risk_light)
            if risk_budget <= 0:
                await update.message.reply_text(f"🚨 风控灯 {risk_light}，当前禁止新开仓。")
                return

            quantity = calc_share_quantity(risk_budget, entry_price, stop_price)
            if quantity <= 0:
                await update.message.reply_text("🚨 计算股数为 0，请检查止损距离或账户净值。")
                return

            spy_context = await fetch_spy_context()
            intent_id = uuid.uuid4().hex[:8]
            await save_pending_intent(
                conn, intent_id, symbol, stop_price, setup_tag,
                entry_price, quantity, spy_context,
            )

        notional = entry_price * quantity
        risk_dollar = abs(entry_price - stop_price) * quantity
        msg = (
            f"🛡️ 【{symbol} 审查】\n"
            f"入场参考 ({price_src}): ${entry_price:.2f}\n"
            f"止损: ${stop_price:.2f} ({risk_pct * 100:.2f}%)\n"
            f"建议股数: {quantity} 股 (风险 ${risk_dollar:.2f})\n"
            f"名义金额: ${notional:,.2f}\n"
            f"策略: {setup_tag} | {spy_context}\n"
            f"风控灯: {risk_light}\n\n"
            f"确认合规？"
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
        async with aiosqlite.connect(DB_PATH) as conn:
            await delete_pending_intent(conn, intent_id)
        await query.edit_message_text(text="🛑 已放弃记账。")
        return

    if data.startswith("CONFIRM:"):
        intent_id = data.split(":", 1)[1]
        async with aiosqlite.connect(DB_PATH) as conn:
            conn.row_factory = aiosqlite.Row
            pending = await load_pending_intent(conn, intent_id)
            if pending is None:
                await query.edit_message_text(text="⚠️ 意图已过期，请重新 /init。")
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
            asyncio.create_task(
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

        async with aiosqlite.connect(DB_PATH) as conn:
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
                asyncio.create_task(
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

        async with aiosqlite.connect(DB_PATH) as conn:
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

        async with aiosqlite.connect(DB_PATH) as conn:
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

        async with aiosqlite.connect(DB_PATH) as conn:
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
        loop = asyncio.get_running_loop()
        loop.create_task(coro)
    except RuntimeError:
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(asyncio.create_task, coro)


def on_execution(trade):
    _dispatch_async(_async_on_execution(trade))


async def _async_on_execution(trade):
    execution = trade.execution
    contract = trade.contract
    symbol = contract.symbol
    side = "LONG" if execution.side == "BOT" else "SHORT"
    qty = float(execution.shares)
    price = float(execution.price)

    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute(
            "SELECT id, side, quantity, entry_price FROM shadow_ledger "
            "WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC",
            (symbol,),
        )
        open_tranches = await cursor.fetchall()

        opening = (
            execution.side == "BOT"
            and (not open_tranches or open_tranches[0]["side"] == "LONG")
        ) or (
            execution.side == "SLD"
            and (not open_tranches or open_tranches[0]["side"] == "SHORT")
        )

        if opening:
            today_str = datetime.datetime.now().strftime("%Y%m%d")
            cursor = await conn.execute(
                "SELECT id, quantity, entry_price FROM shadow_ledger "
                "WHERE symbol=? AND side=? AND status='OPEN' AND strftime('%Y%m%d', create_time)=?",
                (symbol, side, today_str),
            )
            same_day_row = await cursor.fetchone()

            if same_day_row:
                row_id = same_day_row["id"]
                old_qty = float(same_day_row["quantity"])
                old_price = float(same_day_row["entry_price"])
                new_qty = old_qty + qty
                new_price = ((old_price * old_qty) + (price * qty)) / new_qty
                await conn.execute(
                    "UPDATE shadow_ledger SET quantity=?, entry_price=? WHERE id=?",
                    (new_qty, new_price, row_id),
                )
            else:
                tranche_id = f"T{len(open_tranches) + 1}"
                await conn.execute(
                    "INSERT INTO shadow_ledger "
                    "(symbol, tranche_id, side, quantity, entry_price, initial_stop, current_stop, status) "
                    "VALUES (?, ?, ?, ?, ?, 0.0, 0.0, 'OPEN')",
                    (symbol, tranche_id, side, qty, price),
                )
        else:
            remaining_exit_qty = qty
            for row in open_tranches:
                if remaining_exit_qty <= 0:
                    break
                t_id = row["id"]
                t_qty = float(row["quantity"])
                if t_qty <= remaining_exit_qty:
                    cursor = await conn.execute(
                        "SELECT setup_tag FROM shadow_ledger WHERE id=?", (t_id,)
                    )
                    tag_row = await cursor.fetchone()
                    setup_tag = tag_row["setup_tag"] if tag_row and tag_row["setup_tag"] else ""
                    await conn.execute(
                        "UPDATE shadow_ledger SET status='CLOSED', exit_price=? WHERE id=?",
                        (price, t_id),
                    )
                    asyncio.create_task(push_to_notion(t_id, symbol, 0.0, setup_tag))
                    remaining_exit_qty -= t_qty
                else:
                    await conn.execute(
                        "UPDATE shadow_ledger SET quantity=? WHERE id=?",
                        (t_qty - remaining_exit_qty, t_id),
                    )
                    remaining_exit_qty = 0

        await conn.commit()


async def heartbeat_2300(tg_application):
    shanghai_tz = ZoneInfo("Asia/Shanghai")
    while True:
        now = datetime.datetime.now(shanghai_tz)
        target = now.replace(hour=23, minute=0, second=0, microsecond=0)
        if now >= target:
            target += datetime.timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())

        async with aiosqlite.connect(DB_PATH) as conn:
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


async def ib_keepalive():
    while True:
        if not ib.isConnected():
            await ensure_ib_connected()
            if ib.isConnected():
                ib.execDetailsEvent += on_execution
        await asyncio.sleep(30)


async def _startup_sequence() -> None:
    await ensure_schema()
    await sync_flex_query()


async def _post_init(app: Application) -> None:
    if await ensure_ib_connected():
        ib.execDetailsEvent += on_execution
        print("✅ TWS 已连接，异步成交监听已启动。")
        await reconcile_physical_positions(app)
    else:
        print("⚠️ TWS 未连接，/init 报价与成交同步将不可用，Telegram 仍可运行。")
    asyncio.create_task(heartbeat_2300(app))
    asyncio.create_task(ib_keepalive())
    print("✅ Telegram Bot 已启动。")


def main() -> None:
    asyncio.run(_startup_sequence())

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
    tg_app.add_handler(CallbackQueryHandler(button_handler))

    tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
