"""IB reqExecutions 时间戳锚定。"""

from __future__ import annotations

import datetime
import time
from zoneinfo import ZoneInfo

from risk_core.constants import IB_EXEC_TIMEZONE


def ib_exec_tz() -> ZoneInfo:
    """TWS 本机时区：显式配置，否则用操作系统本地时区。"""
    name = (IB_EXEC_TIMEZONE or "").strip()
    if name:
        return ZoneInfo(name)
    # 本机本地时区
    return datetime.datetime.now().astimezone().tzinfo or ZoneInfo("UTC")


def to_ib_exec_time_str(dt: datetime.datetime) -> str:
    """格式 YYYYMMDD-HH:mm:ss，按 IB 客户端本地时区，无后缀。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    local = dt.astimezone(ib_exec_tz())
    return local.strftime("%Y%m%d-%H:%M:%S")


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def parse_iso_utc(value: str) -> datetime.datetime | None:
    if not value:
        return None
    try:
        dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except ValueError:
        return None


def monotonic() -> float:
    return time.monotonic()
