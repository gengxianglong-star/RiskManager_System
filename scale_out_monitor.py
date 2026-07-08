"""盘中 3R 动态止盈雷达 (Scale-out Monitor)。

每 5 分钟扫描一次持仓，检测是否达到 3R 止盈线，
触发 Telegram 提醒减仓锁润。
"""

import time

from ib_insync import Stock

from ai_logger import ai_trace, logger
from config import THRESHOLD_3R
from database import connect_db


class ScaleOutMonitor:
    """3R 止盈巡检引擎。"""

    def __init__(self, app_context):
        self.ctx = app_context
        # 已提醒的 tranche，避免 3R 附近反复震荡刷屏
        self.alerted_tranches: set[int] = set()
        # 4 小时后重新提醒
        self._remind_interval: float = 4 * 3600
        self._alerted_at: dict[int, float] = {}

    @ai_trace
    async def run_3r_scan(self):
        """盘中动态止盈巡检（由 APScheduler 每 5 分钟调用）。"""
        if not self.ctx.ib.isConnected():
            return

        async with connect_db() as db:
            db.row_factory = lambda cursor, row: dict(
                zip([c[0] for c in cursor.description], row)
            )
            cursor = await db.execute(
                "SELECT id, symbol, side, quantity, entry_price, initial_stop, current_stop "
                "FROM shadow_ledger WHERE status='OPEN' AND initial_stop > 0"
            )
            positions = await cursor.fetchall()

        if not positions:
            return

        for pos in positions:
            trade_id = pos["id"]
            symbol = pos["symbol"]
            side = pos["side"]
            entry = float(pos["entry_price"])
            initial_stop = float(pos["initial_stop"])
            current_stop = float(pos["current_stop"])

            # 获取最新价
            current_price, _ = await self.ctx.ib_listener.fetch_entry_price(symbol)
            if not current_price or current_price <= 0:
                continue

            one_r_risk = abs(entry - initial_stop)
            if one_r_risk <= 0.01:
                continue

            # 计算当前 R 倍数
            if side == "LONG":
                current_r = (current_price - entry) / one_r_risk
            else:
                current_r = (entry - current_price) / one_r_risk

            # 止损是否还在风险区（未推到保本以上）
            at_risk = (
                current_stop < entry
                if side == "LONG"
                else current_stop > entry
            )

            if current_r >= THRESHOLD_3R and at_risk:
                now_ts = time.time()
                last_ts = self._alerted_at.get(trade_id)

                if last_ts is None or (now_ts - last_ts) >= self._remind_interval:
                    msg = (
                        f"🔥 **【3R 止盈雷达触发】** 🔥\n\n"
                        f"代码：`{symbol}` ({side})\n"
                        f"进场：${entry:.2f} | 初始止损：${initial_stop:.2f}\n"
                        f"最新价：**${current_price:.2f}**\n"
                        f"当前浮盈：**+{current_r:.2f} R**\n\n"
                        f"💡 *纪律指令：请立刻在 TWS 市价卖出 1/4 或 1/3 仓位锁润，\n"
                        f"成交后系统将自动平推剩余仓位止损至保本价。*"
                    )
                    logger.info(f"3R 雷达命中: {symbol} @ +{current_r:.2f}R (已持有)")
                    await self.ctx.gateway.notify_user(msg)
                    self._alerted_at[trade_id] = now_ts
            else:
                # 回到 3R 以下或已保本：清除提醒标记
                self._alerted_at.pop(trade_id, None)
