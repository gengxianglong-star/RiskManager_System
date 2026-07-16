"""RiskCore 定稿契约常量（可被环境变量覆盖）。"""

from __future__ import annotations

import os

# ── 回撤灯 ──
RISK_MAX_DRAWDOWN_GREEN = float(os.getenv("RISK_MAX_DRAWDOWN_GREEN", "0.05"))
RISK_MAX_DRAWDOWN_YELLOW = float(os.getenv("RISK_MAX_DRAWDOWN_YELLOW", "0.10"))

# ── 敞口 / 仓位 ──
MAX_OVERNIGHT_RISK_PCT = float(os.getenv("MAX_OVERNIGHT_RISK_PCT", "0.015"))
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "0.03"))
MIN_RISK_PER_SHARE = float(os.getenv("MIN_RISK_PER_SHARE", "1.0"))
MIN_ENTRY_PRICE = float(os.getenv("MIN_ENTRY_PRICE", "5.0"))
MIN_CUSHION = float(os.getenv("MIN_CUSHION", "0.10"))

# ── 连亏 ──
MATERIAL_DEPLETION_PCT = float(os.getenv("MATERIAL_DEPLETION_PCT", "0.05"))
AVG_COST_DRIFT_PCT = float(os.getenv("AVG_COST_DRIFT_PCT", "0.20"))
MAX_SYNC_GAP_DAYS = int(os.getenv("MAX_SYNC_GAP_DAYS", "7"))
# Scratch：固定美元，或按净值比例取较大者（调用方传入 nlv 时）
SCRATCH_TOLERANCE_USD = float(os.getenv("SCRATCH_TOLERANCE_USD", "10.0"))
SCRATCH_TOLERANCE_NLV_PCT = float(os.getenv("SCRATCH_TOLERANCE_NLV_PCT", "0.0005"))

# ── 会话 / 并发 ──
POSITION_SYNC_WARMUP_SEC = float(os.getenv("POSITION_SYNC_WARMUP_SEC", "3.0"))
IN_FLIGHT_TTL_SEC = float(os.getenv("IN_FLIGHT_TTL_SEC", "30.0"))
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "3"))
RISK_PCT_PER_TRADE = float(os.getenv("RISK_PCT_PER_TRADE", "0.003"))

# ── 资产 / 订单硬隔离 ──
ALLOWED_CURRENCY = os.getenv("ALLOWED_CURRENCY", "USD")
ALLOWED_SEC_TYPE = os.getenv("ALLOWED_SEC_TYPE", "STK")
ALLOWED_ENTRY_ORDER_TYPES = frozenset(
    t.strip().upper()
    for t in os.getenv("ALLOWED_ENTRY_ORDER_TYPES", "LMT,STP LMT,STP").split(",")
    if t.strip()
)

# ── 时区 ──
RTH_TZ = os.getenv("RTH_TZ", "America/New_York")
# 与运行 TWS 的 OS 本地时区一致（reqExecutions 时间串）
IB_EXEC_TIMEZONE = os.getenv("IB_EXEC_TIMEZONE", "")  # 空 = 用本机 local


def scratch_tolerance(nlv: float | None = None) -> float:
    """合计已实现 PnL 低于 -tolerance 才计连亏。"""
    base = SCRATCH_TOLERANCE_USD
    if nlv is not None and nlv > 0:
        base = max(base, nlv * SCRATCH_TOLERANCE_NLV_PCT)
    return base
