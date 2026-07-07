from notion_client import AsyncClient
import aiosqlite

from config import NOTION_DATABASE_ID, NOTION_TOKEN
from database import connect_db

notion = (
    AsyncClient(auth=NOTION_TOKEN)
    if NOTION_TOKEN and NOTION_TOKEN != "YOUR_NOTION_TOKEN"
    else None
)


async def push_to_notion(trade_id, symbol, realized_pnl, setup_tag, confession=""):
    """
    向 Notion 推送交易复盘记录。
    自动反查 SQLite 数据库以获取完整的开平仓价格、数量与方向。
    """
    if not notion or NOTION_DATABASE_ID == "YOUR_NOTION_DATABASE_ID":
        return

    try:
        side, qty, entry_price, exit_price = "", 0.0, 0.0, 0.0
        try:
            async with connect_db() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT side, quantity, entry_price, exit_price FROM shadow_ledger WHERE id=?",
                    (trade_id,),
                )
                row = await cursor.fetchone()
                if row:
                    side = row["side"]
                    qty = float(row["quantity"])
                    entry_price = float(row["entry_price"])
                    exit_price = float(row["exit_price"] or 0.0)
        except Exception as db_e:
            print(f"⚠️ Notion 读取本地数据库失败: {db_e}")

        properties = {
            "Tranche ID": {"title": [{"text": {"content": f"{symbol}-{trade_id}"}}]},
            "Symbol": {"select": {"name": symbol}},
            "Quantity": {"number": qty},
            "Entry Price": {"number": entry_price},
            "Exit Price": {"number": exit_price},
            "Realized P&L": {"number": realized_pnl},
            "Confession": {"rich_text": [{"text": {"content": confession}}]},
            "Violation": {"checkbox": bool(confession)},
        }

        if side:
            properties["Side"] = {"select": {"name": side.upper()}}

        if setup_tag:
            tags = [{"name": t.strip()} for t in setup_tag.split(",") if t.strip()]
            if tags:
                properties["Setup Tag"] = {"multi_select": tags}

        await notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            properties=properties,
        )
        print(f"✅ Notion 归档成功: {symbol}-{trade_id}")

    except Exception as e:
        print(f"❌ Notion 推送失败: {e}")
