"""核心风控引擎：风险灯计算、建仓审查、斩立决、守夜人、10EMA 狙击手。

从 main.py 解耦，独立管理所有风控规则与交易纪律执行。
"""

import asyncio
import datetime
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

from config import (
    EMA_PERIOD,
    MAX_DAILY_TRADES,
    MAX_OVERNIGHT_RISK_PCT,
    MAX_POSITION_SIZE_PCT,
    MY_TELEGRAM_CHAT_ID,
    RISK_MAX_DRAWDOWN_GREEN,
    RISK_MAX_DRAWDOWN_YELLOW,
    RISK_PCT_PER_TRADE,
)
from database import connect_db, get_today_trade_count
from logger import logger
from ai_logger import ai_trace
from market_regime import fetch_market_regime
from outbound_queue import enqueue_outbound


# ── 模块级应用上下文（由 main.py 注入）──
_app_context = None


def set_app_context(ctx):
    """注入 RiskManagerApp 实例，供 eod_10ema_sniper_job 等模块级函数使用。"""
    global _app_context
    _app_context = ctx


class RiskEngine:
    """风控规则引擎：交通灯、仓位审查、斩立决、守夜人、EMA 计算。"""

    def __init__(self, context, ib_listener, gateway):
        self.ctx = context          # RiskManagerApp 实例
        self.ib_listener = ib_listener  # IBKRListener 实例
        self.gateway = gateway      # TelegramGateway 实例

    # ── 风险灯 ──

    @ai_trace
    async def calculate_risk_light(self, db_connection, current_total_equity: float):
        cursor = await db_connection.execute(
            "SELECT symbol, SUM(quantity * entry_price) AS pos_value "
            "FROM shadow_ledger WHERE status='OPEN' GROUP BY symbol"
        )
        rows = await cursor.fetchall()
        for row in rows:
            pos_value = row["pos_value"] or 0.0
            if pos_value > current_total_equity * MAX_POSITION_SIZE_PCT:
                return f"🚨 危险！{row['symbol']} 超过 {MAX_POSITION_SIZE_PCT * 100:.0f}% 上限。", 0.0

        cursor = await db_connection.execute("SELECT MAX(high_water_mark) FROM account_state")
        row = await cursor.fetchone()
        hwm = float(row[0]) if row and row[0] else current_total_equity
        if current_total_equity > hwm:
            hwm = current_total_equity

        if hwm <= 0:
            return "🟢 绿灯", current_total_equity * RISK_PCT_PER_TRADE

        drawdown = (hwm - current_total_equity) / hwm

        # 🚀 Minervini 连亏惩罚：击球率下降 → 强制缩减暴露
        cursor = await db_connection.execute(
            "SELECT value FROM system_state WHERE key='consecutive_losses'"
        )
        loss_row = await cursor.fetchone()
        consecutive_losses = int(loss_row[0]) if loss_row else 0

        if drawdown >= RISK_MAX_DRAWDOWN_YELLOW or consecutive_losses >= 5:
            return f"🔴 红灯 (连亏:{consecutive_losses})", 0.0
        if drawdown >= RISK_MAX_DRAWDOWN_GREEN or consecutive_losses >= 3:
            return f"🟡 黄灯 (连亏:{consecutive_losses})", current_total_equity * (RISK_PCT_PER_TRADE / 2)

        return "🟢 绿灯", current_total_equity * RISK_PCT_PER_TRADE

    # ── 建仓审查 ──

    @ai_trace
    async def validate_pending_entry(
        self, conn, entry_price: float, stop_price: float,
        quantity: float, equity: float, is_buy: bool,
    ) -> str | None:
        today_count = await get_today_trade_count(conn)
        if today_count >= MAX_DAILY_TRADES:
            return (
                f"🛑 **狙击手协议触发！**\n"
                f"今日弹夹已打空 ({today_count}/{MAX_DAILY_TRADES})。"
            )

        risk_light, base_risk_budget = await self.calculate_risk_light(conn, equity)
        if base_risk_budget <= 0:
            return f"🚨 风控灯 {risk_light}，当前禁止新开仓。"

        # 🚨 已移除 SPY Market Regime 的一票否决和风险乘数干预。
        # 风控 100% 信任主观图形判断，只要账户资金曲线允许（绿灯/黄灯），一律放行。

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

    # ── 斩立决 ──

    @ai_trace
    async def execute_kill_switch(self, symbol: str, trigger_reason: str = "手动授权") -> str:
        from ib_insync import MarketOrder, Stock

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
                if pos.contract.secType == "STK" and pos.contract.symbol == symbol and pos.position != 0:
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
                    if pos.contract.secType == "STK" and pos.contract.symbol == symbol and pos.position != 0:
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

            logger.warning(f"触发斩立决: {symbol}, 动作: {action} {abs(actual_qty):.0f}股, 理由: {trigger_reason}")
            return (
                f"💀 **斩立决已成功执行 [{symbol}]**\n"
                f"触发原因: {trigger_reason}\n"
                f"已撤销 {canceled_count} 笔保护挂单\n"
                f"发送市价单: {action} {abs(actual_qty):.0f} 股。\n"
                f"*(系统将通过异步成交回报自动冲销账本)*"
            )
        except Exception as e:
            logger.error(f"斩立决异常: {e}")
            return f"❌ 斩立决发生异常: {e}"
        finally:
            loop = asyncio.get_running_loop()
            loop.call_later(10.0, self.ctx.killing_symbols.discard, symbol)

    # ── 守夜人（止盈后保本推移）──

    @ai_trace
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
        # ── 防抖：同一标的只执行一次 ──
        if symbol in self.ctx._nightwatchman_done:
            return
        self.ctx._nightwatchman_done.add(symbol)

        try:
            async with connect_db() as conn:
                conn.row_factory = __import__("aiosqlite").Row
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
                elif pos_side == "SHORT" and old_stop > entry_price:
                    if (stop_order.action or "").upper() == "BUY":
                        stop_order.auxPrice = entry_price
                        need_modify = True
                if not need_modify:
                    continue
                self.ib_listener.ib.placeOrder(open_trade.contract, stop_order)
                async with connect_db() as conn:
                    await conn.execute(
                        "UPDATE shadow_ledger SET current_stop=? WHERE symbol=? AND status='OPEN'",
                        (entry_price, symbol),
                    )
                    await conn.commit()
                await enqueue_outbound(
                    f"{trade.order.permId}-STOP_BREAKEVEN", "telegram",
                    {"message": (
                        f"🛡️ **守夜人协议触发**\n"
                        f"`{symbol}` 的分批止盈单已成交！\n"
                        f"后台已将剩余仓位的止损单推移至保本价 ${entry_price:.2f}。\n"
                        f"您可以安心睡觉了。"
                    )},
                )
                break
        except Exception as e:
            logger.error(f"守夜人保本钩子失败: {e}")

    # ── 10EMA 计算 ──

    @ai_trace
    async def get_10ema(self, symbol: str) -> float:
        from ib_insync import Stock, util

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
            df["10EMA"] = df["close"].ewm(span=EMA_PERIOD, adjust=False).mean()
            if len(df) >= 2:
                return float(df["10EMA"].iloc[-2])
            return float(df["10EMA"].iloc[-1])
        except Exception as e:
            logger.error(f"获取 {symbol} 10EMA 失败: {e}")
            return 0.0


# ═══════════════════════════════════════════════════════════
# EOD 10EMA 狙击手 (模块级函数，由 Telegram job_queue 调度)
# ═══════════════════════════════════════════════════════════

@ai_trace
async def eod_10ema_sniper_job(context) -> None:
    """【收盘前5分钟审判】美东 15:55 唤醒，无视盘中洗盘，只看收盘定局。"""
    logger.info("🎯 [EOD Sniper] 唤醒：执行收盘前 10EMA 破位终极审判...")

    ctx = _app_context
    if ctx is None:
        logger.error("EOD Sniper: app_context 未注入，跳过。")
        return

    ny_tz = ZoneInfo("America/New_York")
    now_ny = datetime.datetime.now(ny_tz)
    nyse = mcal.get_calendar("NYSE")
    if len(nyse.valid_days(start_date=now_ny.date(), end_date=now_ny.date())) == 0:
        logger.info("非美股交易日，EOD Sniper 跳过。")
        return

    if not await ctx.ib_listener.ensure_connected():
        logger.warning("TWS 未连接，EOD Sniper 无法执行。将由物理止损单接管防线。")
        return

    async with connect_db() as db:
        db.row_factory = __import__("aiosqlite").Row
        # 🚀 优化：同时拉取止损信息，只对 Runner（已锁利润）仓位执行 10EMA 审判
        cursor = await db.execute(
            "SELECT symbol, side, entry_price, initial_stop, current_stop "
            "FROM shadow_ledger WHERE status='OPEN'"
        )
        open_positions = await cursor.fetchall()

    if not open_positions:
        return

    alerts: list[str] = []
    for pos in open_positions:
        sym = pos["symbol"]
        side = pos["side"]
        init_stop = float(pos["initial_stop"] or 0)
        curr_stop = float(pos["current_stop"] or 0)

        # 🚀 Qullamaggie Runner 判定：止损必须推过保本点才算 Runner
        # 防止 init_stop=0 时任何正止损都被错误判定为 Runner
        is_runner = False
        entry = float(pos["entry_price"])
        if init_stop > 0:
            if side == "LONG" and curr_stop >= entry:
                is_runner = True
            if side == "SHORT" and curr_stop <= entry:
                is_runner = True
        if not is_runner:
            logger.info(
                f"🛡️ {sym} 尚处建仓试错期 (止损未移动)，免除 10EMA 收盘审判，由初始止损保护。"
            )
            continue

        ema_10 = await ctx.risk_engine.get_10ema(sym)
        current_price, _ = await ctx.ib_listener.fetch_entry_price(sym)

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

            kill_result = await ctx.risk_engine.execute_kill_switch(
                sym, trigger_reason="15:55 EOD 10EMA 日线终极破位"
            )
            await context.bot.send_message(chat_id=MY_TELEGRAM_CHAT_ID, text=kill_result)
            alerts.append(sym)
        else:
            logger.info(
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
