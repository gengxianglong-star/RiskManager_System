"""
统一出站队列（发件箱模式 / Outbox Pattern）：

所有 TWS/Flex/对账/用户命令的数据变更，通过 enqueue_outbound() 入队，
由 outbound_worker 统一投递到 Telegram + Notion。

内置熔断器 (Circuit Breaker) + 节流阀 (Throttler) + 指数退避 + 死信队列。
（表结构在 database.py 的 ensure_schema() 中创建）
"""

import asyncio
import json
import time

from ai_logger import ai_trace, logger
from database import connect_db


# ═══════════════════════════════════════════════════════════
# 熔断器 (Circuit Breaker)
# ═══════════════════════════════════════════════════════════

class CircuitBreaker:
    """三态熔断器：CLOSED(正常) → OPEN(熔断) → HALF_OPEN(探活) → CLOSED(恢复)"""

    def __init__(self, name: str = "default", failure_threshold: int = 5, recovery_timeout: float = 600):
        self.name = name
        self.failure_threshold = failure_threshold   # 连续失败 N 次触发熔断
        self.recovery_timeout = recovery_timeout     # 熔断多少秒后尝试半开探活
        self.failures = 0
        self.last_failure_time: float = 0.0
        self.state = "CLOSED"  # CLOSED | OPEN | HALF_OPEN

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.failure_threshold and self.state == "CLOSED":
            self.state = "OPEN"
            logger.error(
                f"🚨 [熔断器触发] {self.name} 连续失败 {self.failures} 次，"
                f"断路器已【开启】，暂停出站 {self.recovery_timeout:.0f} 秒！"
            )

    def record_success(self):
        if self.state != "CLOSED":
            logger.info(f"✅ [熔断器恢复] {self.name} 调用成功，断路器已【关闭】。")
        self.failures = 0
        self.state = "CLOSED"

    def can_execute(self) -> bool:
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                logger.info(f"⏳ [熔断器探活] {self.name} 进入半开状态，放行一个请求进行探活...")
                return True
            return False
        # HALF_OPEN: 允许放行以测试恢复
        return True


# ── 全局熔断器实例 ──
notion_breaker = CircuitBreaker(
    name="Notion",
    failure_threshold=5,
    recovery_timeout=300,  # 5 分钟熔断后探活
)


# ═══════════════════════════════════════════════════════════
# 发件箱入队
# ═══════════════════════════════════════════════════════════

async def enqueue_outbound(event_key: str, channel: str, payload: dict):
    """入队一条消息。同一 (event_key, channel) 已存在则跳过。

    ⚠️ 这是发件箱模式的唯一入口——绝不在此处直接调用外部 API。
    """
    async with connect_db() as db:
        await db.execute(
            "INSERT OR IGNORE INTO outbound_queue (event_key, channel, payload_json) "
            "VALUES (?, ?, ?)",
            (event_key, channel, json.dumps(payload)),
        )
        await db.commit()


# ═══════════════════════════════════════════════════════════
# 发件箱消费者 (Outbox Consumer)
# ═══════════════════════════════════════════════════════════

@ai_trace
async def outbound_worker(app):
    """统一投递守护：发件箱消费者。

    - Telegram 通道：无条件投递（不经过熔断器）
    - Notion 通道：受熔断器保护 + 节流阀 (350ms/条)
    - 失败重试：指数退避 (30s → 60s → 120s ... → 300s 封顶)
    - 永久失败：移入死信队列 (status='failed')
    """
    await asyncio.sleep(5)  # 等系统初始化完成

    while True:
        try:
            # ── 检查 Notion 熔断器 ──
            allow_notion = notion_breaker.can_execute()
            if not allow_notion:
                logger.debug(f"⏸️ Notion 通道已熔断，本轮跳过所有 Notion 出站任务。")

            async with connect_db() as db:
                db.row_factory = lambda cursor, row: dict(
                    zip([c[0] for c in cursor.description], row)
                )

                # 按入队顺序 FIFO，每次取最多 10 条
                cur = await db.execute(
                    "SELECT * FROM outbound_queue WHERE status='pending' "
                    "ORDER BY id LIMIT 10"
                )
                rows = await cur.fetchall()

                for row in rows:
                    channel = row["channel"]

                    # Notion 通道熔断时直接跳过
                    if channel == "notion" and not allow_notion:
                        continue

                    payload = json.loads(row["payload_json"])
                    rid = row["id"]
                    page_id = row.get("notion_page_id") or ""

                    try:
                        if channel == "telegram":
                            await _send_telegram(app, payload)
                        elif channel == "notion":
                            new_page_id = await _send_notion(payload, page_id)

                            # 探活成功：熔断器恢复正常
                            notion_breaker.record_success()

                            # 逆向穿透：记录 Notion Page ID，保证幂等
                            if new_page_id and new_page_id != page_id:
                                await db.execute(
                                    "UPDATE outbound_queue SET notion_page_id=? "
                                    "WHERE event_key=? AND channel='notion'",
                                    (new_page_id, row["event_key"]),
                                )
                                tid = payload.get("trade_id", 0)
                                if tid:
                                    # 使用 json_extract 精确匹配，拒绝 LIKE 子串污染
                                    await db.execute(
                                        "UPDATE outbound_queue SET notion_page_id=? "
                                        "WHERE json_extract(payload_json, '$.trade_id') = ? "
                                        "AND channel='notion' AND notion_page_id IS NULL",
                                        (new_page_id, tid),
                                    )

                        # 投递成功：标记 sent
                        await db.execute(
                            "UPDATE outbound_queue SET status='sent', sent_at=CURRENT_TIMESTAMP "
                            "WHERE id=?",
                            (rid,),
                        )
                        await db.commit()
                        logger.info(f"✅ 出站投递: {row['event_key']} → {channel}")

                        # 节流阀：Notion 限制 ~3 req/s，留 350ms 间隔
                        if channel == "notion":
                            await asyncio.sleep(0.35)

                    except Exception as e:
                        # 记录故障 → 熔断器
                        if channel == "notion":
                            notion_breaker.record_failure()
                            err_msg = str(e)

                            # 🚨 三级 Telegram 报警：根据错误类型精准推送，彻底告别盲盒
                            try:
                                if any(kw in err_msg for kw in (
                                    "body failed validation", "validation_error",
                                    "Bad Request", "400",
                                )):
                                    await app.gateway.notify_user(
                                        f"⚠️ **Notion 字段验证失败**\n"
                                        f"标的: `{payload.get('symbol', '?')}`\n"
                                        f"事件: `{payload.get('event_type', '?')}`\n\n"
                                        f"Notion 拒绝了数据录入。请检查数据库的**列名**和**属性类型**是否与代码匹配。\n\n"
                                        f"📋 报错原文:\n`{err_msg[:300]}`"
                                    )
                                elif any(kw in err_msg.lower() for kw in (
                                    "connect", "timeout",
                                )):
                                    logger.error(f"Notion 网络阻断: {err_msg[:120]}")
                                    await app.gateway.notify_user(
                                        f"🔴 **Notion 连接阻断 (ConnectError)**\n"
                                        f"检测到代理配置失效或网络不通，请检查 `.env` 中的 HTTP_PROXY。\n"
                                        f"当前数据已锁定在发件箱，网络恢复后将自动补发。"
                                    )
                                elif any(kw in err_msg.lower() for kw in (
                                    "invalid", "unauthorized", "api token",
                                )):
                                    logger.error(f"Notion Token 授权失败: {err_msg[:120]}")
                                    await app.gateway.notify_user(
                                        f"🔴 **Notion 授权失效**\n"
                                        f"API Token 无效或已过期！请前往 Notion Integrations 重新获取并更新 `.env` 中的 `NOTION_TOKEN`。\n"
                                        f"系统已触发断路器保护，数据安全停留在本地队列中。"
                                    )
                            except Exception:
                                pass

                        retry = (row["retry_count"] or 0) + 1
                        await db.execute(
                            "UPDATE outbound_queue SET retry_count=? WHERE id=?",
                            (retry, rid),
                        )
                        await db.commit()

                        if retry >= 10:
                            # → 死信队列
                            await db.execute(
                                "UPDATE outbound_queue SET status='failed' WHERE id=?",
                                (rid,),
                            )
                            await db.commit()
                            logger.error(
                                f"❌ 出站永久失败 (死信): {row['event_key']} → {channel}"
                            )
                        else:
                            # 严格的指数退避 (30s → 60s → 120s → 240s → 300s 封顶)
                            delay = min(30 * (2 ** (retry - 1)), 300)
                            logger.warning(
                                f"⚠️ 出站投递失败 (重试 {retry}/10, 退避 {delay}s): "
                                f"{row['event_key']} → {channel} | {type(e).__name__}: {e}"
                            )
                            await asyncio.sleep(delay)
                            break  # 退避期间中断当前批次，防止雪崩

        except Exception as e:
            logger.error(f"outbound_worker 全局异常: {type(e).__name__}: {e}")

        await asyncio.sleep(2)  # 空闲轮询间隔


# ═══════════════════════════════════════════════════════════
# 通道投递实现
# ═══════════════════════════════════════════════════════════

@ai_trace
async def _send_telegram(app, payload: dict):
    """发送 Telegram 消息。payload 中需含 'message' 字段。"""
    msg = payload.get("message", "")
    if not msg:
        return
    await app.gateway.notify_user(msg)


@ai_trace
async def _send_notion(payload: dict, existing_page_id: str = "") -> str:
    """发送 Notion 页面（创建或更新）。返回 page_id。

    内置 Query Before Create 防重机制：
    如果本地丢失了 page_id，先根据 Tranche ID 去 Notion 查询，
    查到已有页面则自动转为 Update，杜绝重复页面。
    """
    from notion_api import notion, NOTION_DATABASE_ID, _build_notion_properties

    if not notion or NOTION_DATABASE_ID == "YOUR_NOTION_DATABASE_ID":
        return ""

    props = _build_notion_properties(payload)

    # 🚀 终极防重穿透：本地没有 page_id 时，先去 Notion 查是否有同名 Tranche ID
    if not existing_page_id and payload.get("event_type") == "OPEN":
        tranche_name = f"{payload.get('symbol')}-{payload.get('trade_id')}"
        try:
            query_result = await notion.request(
                path=f"databases/{NOTION_DATABASE_ID}/query",
                method="POST",
                body={
                    "filter": {
                        "property": "Tranche ID",
                        "title": {
                            "equals": tranche_name
                        }
                    }
                }
            )
            if query_result.get("results"):
                existing_page_id = query_result["results"][0]["id"]
                logger.info(
                    f"🔍 [防重拦截] 本地无 page_id，但 Notion 已存在 {tranche_name}，"
                    f"自动转为 Update 原位覆盖。"
                )
        except Exception as e:
            # 查询失败 → 必须向上抛出，让 outbound_worker 的重试机制接管
            # 绝不能继续执行 pages.create 导致重复页面
            raise RuntimeError(
                f"Notion 防重查询失败，阻断页面创建以防止数据重复: {e}"
            ) from e

    if existing_page_id:
        await notion.pages.update(page_id=existing_page_id, properties=props)
        return existing_page_id
    else:
        page = await notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            properties=props,
        )
        return page["id"]
