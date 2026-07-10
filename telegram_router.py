"""Telegram 路由层：所有 Bot 指令、回调、菜单同步。

从 main.py 解耦，通过 app_context 访问系统组件。
注册入口: register_handlers(tg_app, app_instance)
"""

import asyncio
import datetime
import uuid
from functools import wraps
from zoneinfo import ZoneInfo

import aiosqlite
from telegram import (
    Bot,
    BotCommand,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeDefault,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

from config import (
    MAX_DAILY_TRADES,
    MAX_OVERNIGHT_RISK_PCT,
    MAX_STOP_PCT,
    MY_TELEGRAM_CHAT_ID,
    TRADING_TZ,
)
from database import (
    connect_db,
    count_open_tranches,
    delete_pending_intent,
    get_today_trade_count,
    insert_shadow_ledger,
    load_pending_intent,
    save_pending_intent,
    upsert_account_state,
)
from logger import logger
from market_regime import REGIME_OFFLINE_LABEL, fetch_market_regime
from outbound_queue import enqueue_outbound
from reconciliation import reconcile_physical_positions


# ── 模块级应用上下文（由 main.py 注入）──
app_context = None


def register_handlers(tg_app, risk_manager_app):
    """注册所有 Telegram 指令处理器到 Application。"""
    global app_context
    app_context = risk_manager_app

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

    logger.info("✅ Telegram 路由指令已注册挂载。")


# ── 常量 ──

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

BOT_COMMANDS = [
    BotCommand("start", "指令帮助"),
    BotCommand("status", "持仓净值与裸奔预警"),
    BotCommand("unlock", "解锁 F9 发单权限"),
    BotCommand("override", "越权仓位坦白"),
    BotCommand("import", "收编 TWS 物理持仓"),
]


# ── 工具函数 ──

def require_auth(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        chat = update.effective_chat
        if chat is None or chat.id != MY_TELEGRAM_CHAT_ID:
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


def calc_share_quantity(risk_budget: float, entry_price: float, stop_price: float) -> int:
    risk_per_share = abs(entry_price - stop_price)
    if risk_per_share <= 0 or risk_budget <= 0:
        return 0
    return int(risk_budget / risk_per_share)


def _trading_day_utc_bounds(day_offset: int = 0) -> tuple[str, str, str]:
    """返回 (date_iso, start_utc, end_utc)，统一按交易时区日切。"""
    tz = ZoneInfo(TRADING_TZ)
    target_day = (
        datetime.datetime.now(tz) - datetime.timedelta(days=day_offset)
    ).date()
    day_start = datetime.datetime.combine(target_day, datetime.time.min, tzinfo=tz)
    day_end = day_start + datetime.timedelta(days=1)
    start_utc = day_start.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    end_utc = day_end.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return target_day.isoformat(), start_utc, end_utc


# ── 菜单同步 ──

async def _sync_bot_commands_for(bot: Bot) -> list[str]:
    scopes = [BotCommandScopeDefault(), BotCommandScopeAllPrivateChats()]
    for scope in scopes:
        await bot.delete_my_commands(scope=scope)
        await bot.set_my_commands(BOT_COMMANDS, scope=scope)
    current = await bot.get_my_commands(scope=BotCommandScopeDefault())
    return [c.command for c in current]


async def sync_bot_commands(tg_app) -> None:
    names = await _sync_bot_commands_for(tg_app.bot)
    logger.info(f"✅ Telegram 命令菜单已同步: {', '.join(names)}")


# ── 指令处理器 ──

@require_auth
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("📥 [指令触达] 收到 /start")
    try:
        await _sync_bot_commands_for(context.bot)
    except Exception as e:
        logger.warning(f"⚠️ /start 命令菜单同步失败: {e}")
    status = "\n".join(await app_context.gateway.build_service_status_lines()).strip()
    await update.message.reply_text(f"{START_HELP_TEXT}\n\n---\n{status}")


@require_auth
async def cmd_unlock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        msg_text = update.effective_message.text if update.effective_message else "未知消息"
        logger.info(f"📥 [指令触达] 收到解锁请求: {msg_text}")

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
        logger.info(f"✅ /unlock 授权下发成功: {symbol}")

    except Exception as e:
        logger.error(f"❌ /unlock 后台执行异常: {e}")
        if update.effective_message:
            await update.effective_message.reply_text(f"❌ 系统级异常: {e}")


@require_auth
async def cmd_override(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg_text = update.effective_message.text if update.effective_message else "未知消息"
        logger.info(f"📥 [指令触达] 收到越权坦白请求: {msg_text}")

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
                await enqueue_outbound(
                    f"{row['id']}-OVERRIDE", "telegram",
                    {"message": f"⚠️ **越权坦白** `{symbol}`\n理由: {reason}\n已自动设置 10% 止损线。"},
                )
                await enqueue_outbound(
                    f"{row['id']}-OVERRIDE", "notion",
                    {"trade_id": row["id"], "symbol": symbol, "event_type": "OVERRIDE",
                     "setup_tag": "FOMO", "confession": reason},
                )

        logger.warning(f"💀 强制坦白记录: {symbol} - {reason}")
        await update.effective_message.reply_text(
            f"💀 已记录坦白: {symbol} - {reason}\n"
            f"这笔交易已被军师系统接纳，但已打上违规标签。"
        )

    except Exception as e:
        logger.error(f"❌ /override 后台执行异常: {e}")
        if update.effective_message:
            await update.effective_message.reply_text(f"❌ 坦白记录失败: {e}")


@require_auth
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("📥 [指令触达] 收到 /status")
    try:
        try:
            equity = await asyncio.wait_for(
                app_context.ib_listener.fetch_account_equity(), timeout=8
            )
        except Exception as e:
            logger.warning(f"/status 净值读取降级: {e}")
            equity = 0.0

        async with connect_db() as conn:
            conn.row_factory = aiosqlite.Row
            risk_light, risk_budget = await app_context.risk_engine.calculate_risk_light(conn, equity)
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

            lines = await app_context.gateway.build_service_status_lines() + [
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
                    entry = float(r["entry_price"])
                    side = r["side"]
                    naked = stop == 0.0 and initial == 0.0
                    stop_inverted = (side == "LONG" and stop > entry) or (side == "SHORT" and stop < entry)
                    if naked:
                        warn = " ⚠️ [危险: 无止损(裸奔)]"
                    elif stop_inverted:
                        warn = " ⚠️ [止损方向异常: 疑似止盈单]"
                    else:
                        warn = ""
                    lines.append(
                        f"  • {side} {r['symbol']} {r['tranche_id']} "
                        f"{r['quantity']:.0f}股 @ {entry:.2f} "
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
        logger.error(f"/status 指令后台异常: {e!r}")
        try:
            async with connect_db() as conn:
                cur = await conn.execute("SELECT COUNT(*) FROM shadow_ledger WHERE status='OPEN'")
                open_count = (await cur.fetchone())[0]
            fallback_lines = await app_context.gateway.build_service_status_lines() + [
                "⚠️ 详细状态拉取失败，已切换简版回包。",
                f"📋 OPEN 仓位数: {open_count}",
                "如果 TWS 没开，净值和实时报价会暂时不可用。",
            ]
            await update.message.reply_text("\n".join(fallback_lines))
        except Exception as inner_e:
            logger.error(f"/status 简版回包也失败: {inner_e!r}")


@require_auth
async def cmd_import(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """将 TWS 物理持仓强制收编进影子账本（模拟盘初始化 / 接管历史仓位）。"""
    logger.info("📥 [指令触达] 收到 /import")
    if not await app_context.ib_listener.ensure_connected():
        await update.message.reply_text("❌ 无法连接 TWS，请确认桌面端账户模式与 API 已开启。")
        return

    await update.message.reply_text("🔍 正在扫描 TWS 物理持仓…")
    try:
        positions = await app_context.ib.reqPositionsAsync()
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
                "VALUES (?, ?, ?, ?, ?, 0.0, 0.0, 'OPEN', 'TWS_SYNC')",
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
    if not await app_context.ib_listener.ensure_connected():
        await update.message.reply_text("❌ TWS 未连接，无法对账。请确认桌面端账户模式与 TWS 已登录。")
        return
    await reconcile_physical_positions(app_context.ib, app_context.gateway.notify_user)
    await update.message.reply_text("✅ 物理对账已完成，若有差异已推送警报。")


@require_auth
async def cmd_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("收到手动强制原生结算指令。")
    await app_context.sync_tws_settlement_job()
    await update.message.reply_text("✅ TWS 原生结算对账已触发。")


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

        entry_price, price_src = await app_context.ib_listener.fetch_entry_price(symbol)
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

        equity = await app_context.ib_listener.fetch_account_equity()
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

            risk_light, base_risk_budget = await app_context.risk_engine.calculate_risk_light(conn, equity)
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
                        f"🚨 {symbol} 新止损 {new_stop} 距离入场 {risk_pct * 100:.2f}%，超过 {MAX_STOP_PCT * 100:.0f}% 上限。"
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


# ── 回调处理器 ──

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
        result_msg = await app_context.risk_engine.execute_kill_switch(
            symbol, trigger_reason="Telegram按钮手动授权"
        )
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

            equity = await app_context.ib_listener.fetch_account_equity()
            reject_reason = await app_context.risk_engine.validate_pending_entry(
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
            # 读取 SPY 环境
            spy_ctx = ""
            try:
                label, _, _ = await fetch_market_regime()
                spy_ctx = label if label else ""
            except Exception:
                pass
            await enqueue_outbound(
                f"{row_id}-OPEN", "telegram",
                {"message": (
                    f"🎯 **/init 开仓确认** `{pending['symbol']}`\n"
                    f"LONG {float(pending['quantity']):.0f}股 @ ${float(pending['entry_price']):.2f}\n"
                    f"止损: ${float(pending['stop_price']):.2f} | 策略: {pending['setup_tag']}"
                )},
            )
            await enqueue_outbound(
                f"{row_id}-OPEN", "notion",
                {
                    "trade_id": row_id, "symbol": pending["symbol"], "event_type": "OPEN",
                    "side": "LONG", "quantity": float(pending["quantity"]),
                    "entry_price": float(pending["entry_price"]),
                    "initial_stop": float(pending["stop_price"]),
                    "current_stop": float(pending["stop_price"]),
                    "setup_tag": pending["setup_tag"], "spy_context": spy_ctx,
                },
            )

            await query.edit_message_text(
                text=(
                    f"✅ 已正式记入影子账本 #{row_id}\n"
                    f"{pending['symbol']} {int(pending['quantity'])}股 "
                    f"@ {pending['entry_price']:.2f} stop {pending['stop_price']:.2f}"
                )
            )
