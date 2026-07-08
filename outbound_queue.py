"""
统一出站队列：所有 TWS/Flex/对账/用户命令的数据变更，
通过此模块入队，由 outbound_worker 统一投递到 Telegram + Notion。
（表结构在 database.py 的 ensure_schema() 中创建）
"""
import asyncio
import json

from database import connect_db


async def enqueue_outbound(event_key: str, channel: str, payload: dict):
    """入队一条消息。同一 (event_key, channel) 已存在则跳过。"""
    async with connect_db() as db:
        await db.execute(
            "INSERT OR IGNORE INTO outbound_queue (event_key, channel, payload_json) "
            "VALUES (?, ?, ?)",
            (event_key, channel, json.dumps(payload)),
        )
        await db.commit()


async def outbound_worker(app):
    """统一投递守护：每 5s 扫描队列，投递到 Telegram / Notion。"""
    await asyncio.sleep(5)  # 等系统初始化

    while True:
        try:
            async with connect_db() as db:
                db.row_factory = lambda cursor, row: dict(
                    zip([c[0] for c in cursor.description], row)
                )
                cur = await db.execute(
                    "SELECT * FROM outbound_queue WHERE status='pending' "
                    "ORDER BY id LIMIT 10"
                )
                rows = await cur.fetchall()

                for row in rows:
                    payload = json.loads(row["payload_json"])
                    channel = row["channel"]
                    rid = row["id"]
                    page_id = row.get("notion_page_id") or ""

                    try:
                        if channel == "telegram":
                            await _send_telegram(app, payload)
                        elif channel == "notion":
                            new_page_id = await _send_notion(payload, page_id)
                            if new_page_id and new_page_id != page_id:
                                # 存 page_id 用于后续 UPDATE
                                await db.execute(
                                    "UPDATE outbound_queue SET notion_page_id=? WHERE event_key=? AND channel='notion'",
                                    (new_page_id, row["event_key"]),
                                )
                                # 同步给同 trade_id 的其他事件
                                tid = payload.get("trade_id", 0)
                                if tid:
                                    await db.execute(
                                        "UPDATE outbound_queue SET notion_page_id=? "
                                        "WHERE payload_json LIKE ? AND channel='notion' AND notion_page_id IS NULL",
                                        (new_page_id, f'%"trade_id": {tid}%'),
                                    )

                        await db.execute(
                            "UPDATE outbound_queue SET status='sent', sent_at=CURRENT_TIMESTAMP WHERE id=?",
                            (rid,),
                        )
                        await db.commit()
                        print(f"✅ 出站投递: {row['event_key']} → {channel}")

                    except Exception as e:
                        retry = (row["retry_count"] or 0) + 1
                        await db.execute(
                            "UPDATE outbound_queue SET retry_count=? WHERE id=?",
                            (retry, rid),
                        )
                        await db.commit()
                        if retry >= 10:
                            await db.execute(
                                "UPDATE outbound_queue SET status='failed' WHERE id=?",
                                (rid,),
                            )
                            await db.commit()
                            print(f"❌ 出站永久失败: {row['event_key']} → {channel}")
                        else:
                            delay = min(30 * (2 ** (retry - 1)), 300)
                            print(f"⚠️ 出站投递失败 (重试{retry}/10, {delay}s): {row['event_key']} → {channel}: {e}")
                            await asyncio.sleep(delay)
                            break  # 等退避后再处理后续

        except Exception as e:
            print(f"outbound_worker 异常: {e}")
        await asyncio.sleep(5)


# ── 通道投递实现 ──

async def _send_telegram(app, payload: dict):
    """发送 Telegram 消息。payload 中需含 'message' 字段。"""
    msg = payload.get("message", "")
    if not msg or app.bot is None:
        return
    from config import MY_TELEGRAM_CHAT_ID
    await app.bot.send_message(chat_id=MY_TELEGRAM_CHAT_ID, text=msg)


async def _send_notion(payload: dict, existing_page_id: str = "") -> str:
    """发送 Notion 页面（创建或更新）。返回 page_id。"""
    from notion_api import notion, NOTION_DATABASE_ID, _build_notion_properties

    if not notion or NOTION_DATABASE_ID == "YOUR_NOTION_DATABASE_ID":
        return ""

    props = _build_notion_properties(payload)

    if existing_page_id:
        await notion.pages.update(page_id=existing_page_id, properties=props)
        return existing_page_id
    else:
        page = await notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            properties=props,
        )
        return page["id"]
