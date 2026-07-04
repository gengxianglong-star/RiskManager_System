"""Stockbee 宽度 → 市场环境（对齐 us-industry-strength terminalRegime.ts）。"""

from __future__ import annotations

import csv
import re
import time
from datetime import datetime

import aiohttp

SHEET_ID = "1O6OhS7ciA8zwfycBfGPbP2fWJnR0pn2UUvFZVDP9jpE"
MARKET_MONITOR_GID = "1082103394"
CSV_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export"
    f"?format=csv&gid={MARKET_MONITOR_GID}"
)

_CACHE_TTL = 300
_cache: tuple[float, tuple[str, str, float]] | None = None


def _parse_date(value: str) -> str | None:
    text = (value or "").strip()
    if not text:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return None


def _num(row: list[str], idx: int) -> float:
    if idx >= len(row):
        return 0.0
    try:
        return float(row[idx].strip().replace(",", "") or 0)
    except ValueError:
        return 0.0


def _derive_regime(
    up4: float,
    dn4: float,
    ratio5: float,
    ratio10: float,
    up25q: float,
    dn25q: float,
    t2108: float,
) -> tuple[str, str, float]:
    """复刻 deriveMarketRegime，并映射风险乘数。"""
    if dn4 >= 500:
        return (
            "🟢 OVERSOLD (恐慌抛售)",
            f"Dn4={dn4:.0f}。寻找高RS背离和均值回归。",
            0.5,
        )

    if t2108 >= 80 or (t2108 >= 60 and ratio10 >= 2.0 and dn4 < 100):
        return (
            "🟡 OVERBOUGHT (高潮超买)",
            f"T2108={t2108:.1f}%。防范回调，寻找 Pullback。",
            0.5,
        )

    if t2108 <= 20 or ratio10 <= 0.5:
        return (
            "🟢 OVERSOLD (极端超卖)",
            f"T2108={t2108:.1f}%, 10D={ratio10:.2f}。随时准备均值回归。",
            0.5,
        )

    if ratio10 >= 2.0 and ratio5 >= 1.2 and up25q >= dn25q:
        return (
            "🔥 BULL THRUST (主升浪)",
            f"10D={ratio10:.2f}, 5D={ratio5:.2f}。强势顺风期，激进做多！",
            1.0,
        )

    if ratio10 <= 0.5 or (up25q < dn25q and dn4 > up4):
        return (
            "🔴 BEAR THRUST (空头肆虐)",
            f"10D={ratio10:.2f}, 季Up/Dn={up25q:.0f}/{dn25q:.0f}。逆风期，绝对防守！",
            0.0,
        )

    return (
        "🟡 NEUTRAL (震荡选股)",
        f"10D={ratio10:.2f}, T2108={t2108:.1f}%。精选个股，严控仓位。",
        0.5,
    )


def _pick_latest_row(rows: list[list[str]]) -> list[str] | None:
    best_date = ""
    best_row: list[str] | None = None
    for row in rows:
        if not row or not row[0].strip():
            continue
        trade_date = _parse_date(row[0])
        if not trade_date or len(row) <= 14:
            continue
        if trade_date > best_date:
            best_date = trade_date
            best_row = row
    return best_row


async def fetch_market_regime() -> tuple[str, str, float]:
    """
    拉取 Stockbee Market Monitor CSV，判定宏观环境。
    返回: (环境标签, 洞察建议, 风险乘数)
    """
    global _cache
    now = time.time()
    if _cache and (now - _cache[0]) < _CACHE_TTL:
        return _cache[1]

    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(CSV_URL, timeout=timeout) as response:
                response.raise_for_status()
                text = await response.text()

        rows = list(csv.reader(text.splitlines()))
        latest_row = _pick_latest_row(rows)
        if not latest_row:
            result = ("⚪ UNKNOWN", "无法解析宽度数据", 0.5)
        else:
            result = _derive_regime(
                _num(latest_row, 1),
                _num(latest_row, 2),
                _num(latest_row, 3),
                _num(latest_row, 4),
                _num(latest_row, 5),
                _num(latest_row, 6),
                _num(latest_row, 14),
            )
    except Exception as e:
        result = ("⚪ ERROR", f"数据获取失败: {e}", 0.5)

    _cache = (now, result)
    return result
