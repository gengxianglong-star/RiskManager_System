import asyncio
import datetime
import json

from notion_client import AsyncClient

from config import NOTION_DATABASE_ID, NOTION_TOKEN
from database import connect_db

notion = (
    AsyncClient(auth=NOTION_TOKEN, notion_version="2022-06-28")
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


def _format_sqlite_date_to_iso(date_str: str) -> str:
    """将 SQLite 的 YYYY-MM-DD HH:MM:SS 转换为 Notion 要求的 ISO 8601 格式。"""
    try:
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return dt.isoformat() + "+08:00"
    except Exception:
        return ""


def _build_notion_properties(payload: dict) -> dict:
    """将内部 payload 转为 Notion API properties。

    支持三种生命周期：
    - OPEN:   建仓 — 写入标题/代码/数量/进场价/止损/SPY/Date
    - UPDATE: 改止损 — 更新 Current Stop / Risk Amount
    - CLOSE:  平仓结算 — 写入退出价/盈亏/R-Multiple/Return%

    ⚠️ 属性类型必须与 Notion 数据库列定义严格一致，否则 Notion 返回 400。
    """
    event_type = payload.get("event_type", "OPEN")
    symbol = payload.get("symbol", "UNKNOWN")
    trade_id = payload.get("trade_id", 0)

    props: dict = {}

    # ── OPEN: 建仓全量字段 ──
    if event_type == "OPEN":
        props["Tranche ID"] = {"title": [{"text": {"content": f"{symbol}-{trade_id}"}}]}
        props["Symbol"] = {"select": {"name": symbol}}
        props["Status"] = {"select": {"name": "OPEN"}}

        if "quantity" in payload:
            props["Quantity"] = {"number": float(payload["quantity"])}
        if "entry_price" in payload:
            props["Entry Price"] = {"number": round(float(payload["entry_price"]), 2)}
        if "side" in payload:
            props["Side"] = {"select": {"name": str(payload["side"]).upper()}}
        if "spy_context" in payload and payload["spy_context"]:
            props["SPY Context"] = {"rich_text": [{"text": {"content": str(payload["spy_context"])}}]}
        if "setup_tag" in payload and payload["setup_tag"]:
            tags = [{"name": t.strip()} for t in str(payload["setup_tag"]).split(",") if t.strip()]
            if tags:
                props["Setup Tag"] = {"multi_select": tags}

        # 🚨 买入时间：ISO 8601 格式写入 Entry Date 列
        if "create_time" in payload and payload["create_time"]:
            iso_date = _format_sqlite_date_to_iso(payload["create_time"])
            if iso_date:
                props["Entry Date"] = {"date": {"start": iso_date}}

    # ── OPEN / UPDATE: 止损 + 风险金额 + SPY Context 补填 ──
    if event_type in ("OPEN", "UPDATE"):
        entry = float(payload.get("entry_price", 0))
        qty = float(payload.get("quantity", 0))
        init_stop = float(payload.get("initial_stop", 0))
        curr_stop = float(payload.get("current_stop", 0))

        if init_stop > 0:
            props["Initial Stop"] = {"number": round(init_stop, 2)}
            if entry > 0:
                # 🚀 Risk Amount（美元金额）
                if qty > 0:
                    risk_amt = abs(entry - init_stop) * qty
                    props["Risk Amount"] = {"number": round(risk_amt, 2)}
                # 🚀 Risk %（Notion percent 列，存小数：0.0088 → 显示 0.88%）
                risk_pct = round(abs(entry - init_stop) / entry, 4)
                props["Risk %"] = {"number": risk_pct}

        if curr_stop > 0:
            props["Current Stop"] = {"number": round(curr_stop, 2)}

        # 🚀 SPY Context 补填：UPDATE 事件也写入（回填守护专用）
        if "spy_context" in payload and payload["spy_context"]:
            props["SPY Context"] = {"rich_text": [{"text": {"content": str(payload["spy_context"])}}]}

        if curr_stop > 0:
            props["Current Stop"] = {"number": round(curr_stop, 2)}

    # ── CLOSE: 平仓结算（更新已有页面）──
    if event_type == "CLOSE":
        props["Status"] = {"select": {"name": "CLOSED"}}

        pnl = float(payload.get("realized_pnl", 0))
        props["Realized P&L"] = {"number": round(pnl, 2)}

        if "exit_price" in payload:
            props["Exit Price"] = {"number": float(payload["exit_price"])}

        initial_risk = float(payload.get("initial_risk", 0))
        if initial_risk > 0 and pnl != 0:
            props["R-Multiple"] = {"number": round(pnl / initial_risk, 2)}

        entry_p = float(payload.get("entry_price", 0))
        exit_p = float(payload.get("exit_price", 0))
        side = payload.get("side", "LONG")
        if entry_p > 0 and exit_p > 0:
            if side == "LONG":
                ret_pct = (exit_p / entry_p - 1)
            else:
                ret_pct = (1 - exit_p / entry_p)
            props["Return %"] = {"number": round(ret_pct, 4)}

    # ── 公共字段：违规标记 ──
    confession = payload.get("confession", "")
    if confession:
        props["Confession"] = {"rich_text": [{"text": {"content": str(confession)}}]}
        props["Violation"] = {"checkbox": True}

    return props


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
