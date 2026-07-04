from notion_client import AsyncClient

from config import NOTION_DATABASE_ID, NOTION_TOKEN

notion = (
    AsyncClient(auth=NOTION_TOKEN)
    if NOTION_TOKEN and NOTION_TOKEN != "YOUR_NOTION_TOKEN"
    else None
)


async def push_to_notion(trade_id, symbol, realized_pnl, setup_tag, confession=""):
    if not notion or NOTION_DATABASE_ID == "YOUR_NOTION_DATABASE_ID":
        return
    try:
        await notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            properties={
                "Tranche ID": {"title": [{"text": {"content": f"{symbol}-{trade_id}"}}]},
                "Symbol": {"select": {"name": symbol}},
                "Realized P&L": {"number": realized_pnl},
                "Setup Tag": {
                    "multi_select": [{"name": setup_tag}] if setup_tag else []
                },
                "Confession": {"rich_text": [{"text": {"content": confession}}]},
                "Violation": {"checkbox": True if confession else False},
            },
        )
        print(f"✅ Notion 归档成功: {symbol}-{trade_id}")
    except Exception as e:
        print(f"❌ Notion 推送失败: {e}")
