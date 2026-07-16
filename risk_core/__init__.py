"""极简三项风控 RiskCore — 防弹定稿。

对外 API：
  - get_state / can_open
  - adjust_hwm / reset_streak
  - acknowledge_corp_action / merge_lifecycle
  - session_boot_sync
"""

from risk_core.ib_bridge import account_summary_to_dict, trades_to_open_orders
from risk_core.core import RiskCore
from risk_core.models import (
    CanOpenRequest,
    CanOpenResult,
    OpenOrderView,
    RiskState,
    ShadowPosition,
    Side,
)
from risk_core.sync import session_boot_sync, wait_positions_ready
from risk_core.timeutil import to_ib_exec_time_str

__all__ = [
    "RiskCore",
    "CanOpenRequest",
    "CanOpenResult",
    "OpenOrderView",
    "RiskState",
    "ShadowPosition",
    "Side",
    "session_boot_sync",
    "wait_positions_ready",
    "to_ib_exec_time_str",
    "trades_to_open_orders",
    "account_summary_to_dict",
]
