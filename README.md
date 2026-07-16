# RiskManager_System（已归档）

> **开仓风控已并入 [ibkr-order-tool](../Projects/ibkr-order-tool-dev)。** 本仓库不再运行独立守护进程。

## 现状

| 项目 | 说明 |
|------|------|
| 风控逻辑 | `ibkr-order-tool` → `src/ibkr_order_tool/risk_core/` |
| 开关 | 工具顶栏 `RiskCore`（`settings.risk_core_enabled`） |
| 数据库 | 仍可用本目录 `risk_manager_paper.db` / `risk_manager_live.db`（工具会优先写这里） |
| `python main.py` / `run.bat` | 仅打印归档提示，退出码 1 |

## 硬门禁（Risk ON，工具内）

- 日开仓 ≥ 3 → 拒
- 连亏 ≥ 5 → 预算减半；≥ 10 → 禁开至下一美东日
- HWM 回撤 ≥ 5% → 减半；≥ 10% → 禁开至下一美东日（到期可开，仍深则只减半+红字）
- cushion &lt; 10% / TWS 未连 / sync 未就绪 → 拒
- 仓险 &gt; 1.5%、持仓 ≥ 3R → **只高亮，不拒单**

Risk **OFF**：跳过整闸，纯下单。

## 已移除（勿再依赖）

独立守护、`risk_engine`、Flex/TWS 结算轮询、`scale_out_monitor`、`market_regime`、`gateway`、Telegram/Notion 推送。

本目录可保留作历史 DB 与文档；新开发只改 order-tool。
