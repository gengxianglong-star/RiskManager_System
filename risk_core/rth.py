"""动态 RTH（含半天市 market_close）。"""

from __future__ import annotations

import datetime
from functools import lru_cache
from zoneinfo import ZoneInfo

from risk_core.constants import RTH_TZ

try:
    import pandas_market_calendars as mcal
except ImportError:  # pragma: no cover
    mcal = None


@lru_cache(maxsize=8)
def _nyse():
    if mcal is None:
        return None
    return mcal.get_calendar("NYSE")


def rth_bounds(
    when: datetime.datetime | None = None,
) -> tuple[datetime.datetime, datetime.datetime] | None:
    """返回当日美东 RTH (open, close)；非交易日返回 None。"""
    tz = ZoneInfo(RTH_TZ)
    now = when.astimezone(tz) if when else datetime.datetime.now(tz)
    day = now.date()

    cal = _nyse()
    if cal is None:
        # 降级：普通日 9:30–16:00（无日历包时）
        open_dt = datetime.datetime(day.year, day.month, day.day, 9, 30, tzinfo=tz)
        close_dt = datetime.datetime(day.year, day.month, day.day, 16, 0, tzinfo=tz)
        if now.weekday() >= 5:
            return None
        return open_dt, close_dt

    schedule = cal.schedule(start_date=day, end_date=day)
    if schedule.empty:
        return None
    # pandas Timestamp → aware datetime in exchange tz
    open_ts = schedule.iloc[0]["market_open"].tz_convert(RTH_TZ).to_pydatetime()
    close_ts = schedule.iloc[0]["market_close"].tz_convert(RTH_TZ).to_pydatetime()
    return open_ts, close_ts


def is_rth(when: datetime.datetime | None = None) -> bool:
    bounds = rth_bounds(when)
    if bounds is None:
        return False
    open_dt, close_dt = bounds
    tz = ZoneInfo(RTH_TZ)
    now = when.astimezone(tz) if when else datetime.datetime.now(tz)
    return open_dt <= now < close_dt


def equity_for_cap(nlv: float, rth_close_nlv: float, in_rth: bool) -> float:
    """敞口上限分母：非 RTH 防盘前 NLV 虚低。"""
    if in_rth or rth_close_nlv <= 0:
        return max(nlv, 0.0)
    # 盘外：NLV 相对收盘偏离过大时用收盘净值
    if nlv <= 0:
        return rth_close_nlv
    if abs(nlv - rth_close_nlv) / rth_close_nlv > 0.05:
        return rth_close_nlv
    return max(nlv, rth_close_nlv)
