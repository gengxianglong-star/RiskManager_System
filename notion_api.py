import asyncio
import datetime
import json

from notion_client import AsyncClient

from config import NOTION_DATABASE_ID, NOTION_TOKEN
from database import connect_db

notion = (
    AsyncClient(auth=NOTION_TOKEN)
    if NOTION_TOKEN and NOTION_TOKEN != "YOUR_NOTION_TOKEN"
    else None
)

# ── Notion 后台投递守护进程 ──
_notion_worker_started = False


async def check_notion_online() -> tuple[bool, str]:
    """探测 Notion API 与交易复盘库是否可达。"""
    if not notion:
        return False, "未配置 Token"
    if NOTION_DATABASE_ID == "YOUR_NOTION_DATABASE_ID":
        return False, "未配置数据库 ID"
    try:
        await asyncio.wait_for(
            notion.databases.retrieve(database_id=NOTION_DATABASE_ID),
            timeout=5.0,
        )
        return True, "已连"
    except asyncio.TimeoutError:
        return False, "超时"
    except Exception as exc:
        return False, type(exc).__name__


def _build_notion_properties(payload: dict) -> dict:
    """将内部 payload 转为 Notion API properties。"""
    event = payload.get("event_type", "CLOSE")
    symbol = payload["symbol"]
    trade_id = payload["trade_id"]
    side = payload.get("side", "")
    qty = payload.get("quantity", 0)
    entry = payload.get("entry_price", 0)
    exit_p = payload.get("exit_price", 0)
    pnl = payload.get("realized_pnl", 0)
    initial_stop = payload.get("initial_stop", 0)
    current_stop = payload.get("current_stop", 0)
    tag = payload.get("setup_tag", "")
    confession = payload.get("confession", "")
    spy_ctx = payload.get("spy_context", "")
    create_time = payload.get("create_time", "")
    close_type = payload.get("close_type", "Full")
    status = "CLOSED" if event == "CLOSE" else "OPEN"

    # 风险计算
    notional = entry * qty
    risk_amount = 0.0
    if initial_stop > 0:
        risk_amount = abs(entry - initial_stop) * qty
    elif current_stop > 0:
        risk_amount = abs(entry - current_stop) * qty
    r_multiple = round(pnl / risk_amount, 2) if risk_amount > 0 else None
    return_pct = round(pnl / notional * 100, 2) if notional > 0 else None

    properties = {
        "Tranche ID": {"title": [{"text": {"content": f"{symbol}-{trade_id}"}}]},
        "Symbol": {"select": {"name": symbol}},
        "Status": {"select": {"name": status}},
        "Quantity": {"number": qty},
        "Entry Price": {"number": round(entry, 2)},
    }

    if side:
        properties["Side"] = {"select": {"name": side.upper()}}

    if event == "CLOSE":
        properties["Close Type"] = {"select": {"name": close_type}}
        if exit_p > 0:
            properties["Exit Price"] = {"number": round(exit_p, 2)}
        properties["Realized P&L"] = {"number": round(pnl, 2)}
        if r_multiple is not None:
            properties["R-Multiple"] = {"number": r_multiple}
        if return_pct is not None:
            properties["Return %"] = {"number": return_pct}
        properties["Exit Date"] = {"date": {"start": datetime.date.today().isoformat()}}
    else:
        properties["Close Type"] = {"select": {"name": "Open"}}

    if initial_stop > 0:
        properties["Initial Stop"] = {"number": round(initial_stop, 2)}
    if current_stop > 0:
        properties["Current Stop"] = {"number": round(current_stop, 2)}
    if risk_amount > 0:
        properties["Risk Amount"] = {"number": round(risk_amount, 2)}

    if create_time:
        properties["Entry Date"] = {"date": {"start": create_time[:10]}}
    if tag:
        tags = [{"name": t.strip()} for t in tag.split(",") if t.strip()]
        if tags:
            properties["Setup Tag"] = {"multi_select": tags}
    if confession:
        properties["Confession"] = {"rich_text": [{"text": {"content": confession}}]}
        properties["Violation"] = {"checkbox": True}
    if spy_ctx:
        properties["SPY Context"] = {"rich_text": [{"text": {"content": spy_ctx}}]}

    return properties


async def enqueue_notion(trade_id: int, symbol: str, event_type: str, **kwargs):
    """转发到统一出站队列 → Notion 通道。"""
    from outbound_queue import enqueue_outbound
    event_key = f"{trade_id}-{event_type}"
    payload = {
        "trade_id": trade_id,
        "symbol": symbol,
        "event_type": event_type,
    }
    for k, v in kwargs.items():
        if isinstance(v, (str, int, float, bool, type(None))):
            payload[k] = v
        else:
            payload[k] = str(v)
    await enqueue_outbound(event_key, "notion", payload)


# ── 兼容旧调用 ──
async def push_to_notion(trade_id, symbol, realized_pnl, setup_tag, confession=""):
    """兼容旧接口：将平仓记录入队。"""
    await enqueue_notion(
        trade_id=trade_id,
        symbol=symbol,
        event_type="CLOSE",
        realized_pnl=realized_pnl,
        setup_tag=setup_tag,
        confession=confession,
    )
