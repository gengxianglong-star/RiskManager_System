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
from ib_insync import IB, FlexReport, Stock, util
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

from config import (
    CLIENT_ID,
    DB_PATH,
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
    get_today_trade_count,
)
from market_regime import fetch_market_regime
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
                trade_risk = max(0.0, (pos_entry - pos_stop) * pos_qty)
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
            f"洞察: {insight}\n\n"
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


async def get_10ema(symbol: str) -> float:
    """利用 IBKR 原生历史数据计算昨日 10EMA。"""
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
        return float(df["10EMA"].iloc[-1])
    except Exception as e:
        print(f"获取 {symbol} 10EMA 失败: {e}")
        return 0.0


async def active_position_monitor(tg_application):
    """【3R 腾挪与 10EMA 追踪引擎】后台每 5 分钟巡检一次。"""
    await asyncio.sleep(15)
    notified_3r: set[str] = set()
    notified_ema: set[str] = set()

    print("✅ 动态仓位巡检器 (Scale-out Financer) 已在后台启动...")

    while True:
        try:
            if not ib.isConnected():
                await asyncio.sleep(60)
                continue

            async with aiosqlite.connect(DB_PATH) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(
                    "SELECT id, symbol, quantity, entry_price, initial_stop, current_stop "
                    "FROM shadow_ledger WHERE status='OPEN'"
                )
                positions = await cursor.fetchall()

            for pos in positions:
                trade_id = pos["id"]
                symbol = pos["symbol"]
                entry = float(pos["entry_price"])
                initial_stop = float(pos["initial_stop"])
                current_stop = float(pos["current_stop"])

                one_r_risk = entry - initial_stop
                if one_r_risk <= 0:
                    continue

                current_price, _ = await fetch_entry_price(symbol)
                if not current_price or current_price <= 0:
                    continue

                current_profit = current_price - entry
                current_r_multiple = current_profit / one_r_risk

                # 规则 1：3R 强制减仓与平推止损提醒
                if current_r_multiple >= 3.0 and current_stop < entry:
                    cache_key = f"{trade_id}_3R"
                    if cache_key not in notified_3r:
                        alert_msg = (
                            f"🚀 **【3R 爆发确认：严禁全仓死扛！】** `{symbol}`\n\n"
                            f"当前价格: ${current_price:.2f}\n"
                            f"当前浮盈: **+{current_r_multiple:.1f} R**\n\n"
                            f"⚠️ **纪律指令 (两步走)：**\n"
                            f"1. 请立刻在 TWS **市价卖出 1/4 或 1/3**，用市场的钱为自己买下免费门票！\n"
                            f"2. 卖出确认后，请立刻点击下方指令，将剩余仓位止损上移至成本价！\n\n"
                            f"👉 `/update {symbol} {entry:.2f}`\n\n"
                            f"*(注：只有平推止损后，该股票占用的风控额度才会被系统释放！让剩下的仓位去冲击 10R！)*"
                        )
                        await tg_application.bot.send_message(
                            chat_id=MY_TELEGRAM_CHAT_ID, text=alert_msg
                        )
                        notified_3r.add(cache_key)

                elif current_stop >= entry:
                    ema_10 = await get_10ema(symbol)
                    if ema_10 > 0 and current_price < (ema_10 * 0.99):
                        cache_key = f"{trade_id}_10EMA"
                        if cache_key not in notified_ema:
                            alert_msg = (
                                f"🛑 **【尾仓趋势终结】** `{symbol}`\n\n"
                                f"当前价格: ${current_price:.2f}\n"
                                f"昨日 10EMA: ${ema_10:.2f}\n\n"
                                f"⚠️ **纪律指令：**\n"
                                f"价格已有效跌破 10EMA，动量已衰竭！\n"
                                f"请立即手动清仓所有剩余的“免费彩票”，锁定波段利润！"
                            )
                            await tg_application.bot.send_message(
                                chat_id=MY_TELEGRAM_CHAT_ID, text=alert_msg
                            )
                            notified_ema.add(cache_key)

        except Exception as e:
            print(f"后台巡检出错: {e}")

        await asyncio.sleep(300)


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
    asyncio.create_task(active_position_monitor(app))
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
