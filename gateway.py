"""Telegram 消息网关与系统探针。

职责：
1. 稳定发送 Telegram 消息（含 429 限流退避、断网消息队列）
2. 搜集系统状态：TWS 连接、DB 体积、出站队列积压、CPU/RAM
"""

import asyncio
import datetime
import os

import psutil
from telegram import Bot
from telegram.error import NetworkError, RetryAfter, TimedOut

from ai_logger import ai_trace, logger
from config import DB_PATH, MY_TELEGRAM_CHAT_ID, resolve_tws_ports
from database import connect_db


class TelegramGateway:
    """全局消息推送出口 + 系统探活面板。"""

    def __init__(self, context):
        self.ctx = context          # RiskManagerApp 实例
        self.bot: Bot | None = None
        self._msg_queue: list[str] = []
        self._tg_was_offline: bool = False

    def bind_bot(self, bot: Bot) -> None:
        self.bot = bot
        self.ctx.bot = bot
        self.ctx.bot_started_at = datetime.datetime.now()

    # ── 消息推送（带 Telegram 429 退避）──

    @ai_trace
    async def notify_user(
        self, message: str, parse_mode: str = "Markdown", reply_markup=None, retry: int = 0
    ) -> None:
        """全局消息推送出口。"""
        if not self.bot:
            logger.warning(f"Telegram 未绑定，跳过通知: {message[:80]}...")
            return
        if not MY_TELEGRAM_CHAT_ID or MY_TELEGRAM_CHAT_ID == 123456789:
            logger.warning("MY_TELEGRAM_CHAT_ID 未配置，跳过通知。")
            return

        try:
            # 先排空断网积压队列
            if self._msg_queue:
                await self._drain_queue()
            await self.bot.send_message(
                chat_id=MY_TELEGRAM_CHAT_ID,
                text=message,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )
            if self._tg_was_offline:
                self._tg_was_offline = False
                await self.bot.send_message(
                    chat_id=MY_TELEGRAM_CHAT_ID,
                    text="📶 **Telegram 已恢复连接** — 断网期间通知已补发完毕。",
                )
        except RetryAfter as e:
            if retry < 3:
                logger.warning(f"Telegram 限流 (429)，退避 {e.retry_after}s...")
                await asyncio.sleep(e.retry_after + 1)
                await self.notify_user(message, parse_mode, reply_markup, retry + 1)
            else:
                logger.error("Telegram 连续限流 3 次，消息丢弃。")
        except (TimedOut, NetworkError) as e:
            if retry < 3:
                wait = 2 ** retry
                logger.warning(f"Telegram 网络错误 ({type(e).__name__})，{wait}s 后重试...")
                await asyncio.sleep(wait)
                await self.notify_user(message, parse_mode, reply_markup, retry + 1)
            else:
                self._tg_was_offline = True
                if message not in self._msg_queue:
                    self._msg_queue.append(message)
                    if len(self._msg_queue) > 50:
                        self._msg_queue = self._msg_queue[-30:]
                logger.warning(f"Telegram 持续不可达，消息已入队 ({len(self._msg_queue)} 条积压)")
        except Exception as e:
            self._tg_was_offline = True
            if message not in self._msg_queue:
                self._msg_queue.append(message)
                if len(self._msg_queue) > 50:
                    self._msg_queue = self._msg_queue[-30:]
            logger.error(f"Telegram 发送异常（已入队）: {type(e).__name__}: {e}")

    async def _drain_queue(self) -> None:
        """Telegram 恢复时批量推送积压消息。"""
        if not self._msg_queue or self.bot is None:
            return
        queue = self._msg_queue
        self._msg_queue = []
        delivered = 0
        for i, msg in enumerate(queue):
            try:
                await self.bot.send_message(chat_id=MY_TELEGRAM_CHAT_ID, text=msg)
                delivered += 1
                await asyncio.sleep(0.5)
            except Exception as e:
                self._msg_queue = queue[i:]
                logger.warning(f"补发中断 ({delivered}/{len(queue)}): {e}")
                raise
        if delivered > 0:
            logger.info(f"✅ 断网补发完成: {delivered} 条")

    # ── 状态面板 ──

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

    async def build_service_status_lines(self) -> list[str]:
        """系统探活与硬件监控面板。"""
        lines = []

        # 1. Bot & TWS
        lines.append(f"🟢 Bot 在线 · 已运行 {self.format_bot_uptime()}")
        lines.append(self.format_tws_status_line())

        # 2. Notion 探活
        cache = self.ctx.system_status_cache
        notion_ok = cache.get("notion_online", False)
        notion_detail = cache.get("notion_msg", "等待首次探测...")
        lines.append(
            f"{'🟢' if notion_ok else '🔴'} Notion 交易复盘 · "
            f"{'已连' if notion_ok else notion_detail}"
        )

        # 3. 桌面下单工具
        tool_ok = cache.get("order_tool_running", False)
        lines.append(
            f"{'🟢' if tool_ok else '🔴'} 桌面下单工具 · "
            f"{'运行中' if tool_ok else '未启动'}"
        )

        # 4. 出站队列状态
        try:
            async with connect_db() as db:
                cur = await db.execute(
                    "SELECT COUNT(*) FROM outbound_queue WHERE status='pending'"
                )
                pending_count = (await cur.fetchone())[0]
                cur = await db.execute(
                    "SELECT COUNT(*) FROM outbound_queue WHERE status='failed'"
                )
                failed_count = (await cur.fetchone())[0]
            q_status = "🟢" if pending_count == 0 else "🟡"
            lines.append(
                f"{q_status} 发件箱队列: {pending_count} 待发 | {failed_count} 死信"
            )
        except Exception:
            pass

        # 5. SQLite 体积
        try:
            if os.path.exists(DB_PATH):
                db_size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
                lines.append(f"🗄️ SQLite 体积: {db_size_mb:.2f} MB")
        except Exception:
            pass

        # 6. 服务器硬件监控
        try:
            mem = psutil.virtual_memory()
            cpu = psutil.cpu_percent(interval=0.1)
            lines.append(f"💻 系统负载: CPU {cpu}% | RAM {mem.percent}%")
        except Exception:
            pass

        lines.append("")
        return lines
