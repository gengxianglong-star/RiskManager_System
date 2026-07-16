"""RiskManager_System 已归档。

风控已并入 ibkr-order-tool 内嵌包 `ibkr_order_tool.risk_core`。
请勿再启动本文件作为守护进程。
"""

from __future__ import annotations

import sys


def main() -> int:
    print(
        "RiskManager 独立守护已停用。\n"
        "请使用 ibkr-order-tool，并勾选顶栏「RiskCore」。\n"
        "历史数据库仍保留在本目录：risk_manager_paper.db / risk_manager_live.db"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
