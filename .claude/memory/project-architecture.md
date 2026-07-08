---
name: project-architecture
description: RiskManager_System 完整架构文档 — 17个模块、数据流、关键设计决策
metadata: 
  node_type: memory
  type: project
  originSessionId: 61c56c18-af32-423e-bb80-6a756098cce1
---

# RiskManager_System 项目架构

## GitHub
- 仓库: https://github.com/gengxianglong-star/RiskManager_System
- 分支: main
- 最新提交: 8cd33bc (2026-07-08)

## 项目概述
基于 IBKR TWS API + Telegram Bot 的量化交易风控中台，提供：
- 实时成交监听与影子账本
- 多层风控（风险灯/斩立决/3R止盈/10EMA破位）
- 物理仓位对账（幽灵单清理/自动收编）
- Flex 权威结算（CQRS 读写分离）
- Telegram Bot 指令交互
- Notion 交易复盘归档

## 模块架构（17 个 .py 文件）

### 调度层
- main.py (~540行): 纯调度器，组装组件 + APScheduler + 生命周期管理

### 连接层
- ib_listener.py (~590行): TWS 连接/成交监听/3s 延迟止损捕获/Notion 全量 OPEN 推送
- gateway.py (~175行): Telegram 网关 (429退避/断网队列/系统探针 CPU+RAM+DB)

### 交互层
- telegram_router.py (~500行): 所有 Bot 指令(/status,/init,/unlock等)/回调/菜单同步

### 风控层
- risk_engine.py (~310行): 风险灯计算/建仓审查/斩立决/守夜人(保本推移)/10EMA计算
- scale_out_monitor.py (~100行): 3R 止盈雷达 (APScheduler 每5分钟)

### 对账层
- reconciliation.py (~270行): 物理对账(幽灵单清理/自动收编/止损兜底同步+Notion补推)
- flex_settlement.py (~170行): CQRS读取端 — IBKR Flex Query 权威结算

### 出站层
- outbound_queue.py (~240行): 发件箱模式 + CircuitBreaker 熔断器 + 节流阀(350ms) + 死信队列
- notion_api.py (~170行): Notion 属性构建 (OPEN/UPDATE/CLOSE 全生命周期 + R-Multiple计算)

### 数据层
- database.py: SQLite WAL + busy_timeout + 3个复合索引
- market_regime.py: Stockbee 宽度雷达 + 3次重试

### 可观测性层
- logger.py: 结构化日志 (RotatingFileHandler 5MBx3)
- ai_logger.py: AI追踪日志 + @ai_trace 装饰器 (自动捕获入参/崩溃堆栈)

### 配置层
- config.py: 所有参数环境变量化 (THRESHOLD_3R/EMA_PERIOD/FORCE_CONFESSION_HOUR等)
- database_setup.py: 一次性建库脚本

## 关键设计决策

### 1. 三重止损同步
- 第一重 (3秒): ib_listener._delayed_bracket_stop_capture — 开仓后等TWS生成bracket止损
- 第二重 (15分钟): reconciliation.reconcile_physical_positions — 兜底扫描TWS止损单
- 第三重 (实时): ib_listener._async_on_open_order — 捕获TWS图表手动拖拽止损

### 2. Partial Fills 防抖
- ib_listener._delayed_bracket_stop_capture 入口处查询 outbound_queue
- 若 {trade_id}-OPEN 已入队则直接 return，避免 N 笔部分成交产生 N 条 Notion 记录

### 3. 熔断器 (CircuitBreaker)
- Notion 连续 5 次失败 → OPEN（跳过所有 Notion 出站）
- 5 分钟后 → HALF_OPEN（放行1条探活）
- 成功 → CLOSED；失败 → 重新 OPEN
- Telegram 通道不受影响

### 4. CQRS 读写分离
- 写端: TWS 实时成交 → shadow_ledger (OPEN)
- 读端: Flex 官方报表 → 权威核销 (CLOSED) → Notion 归档
- Flex 结算完全独立，不依赖 TWS 实时连接

### 5. APScheduler 定时任务
- reconcile_job: 每 15 分钟
- flex_settlement_job: 09:00/16:30 美东
- daily_rollover_job: 00:05 美东
- heartbeat_2300_job: 23:00 上海
- corporate_actions_job: 08:00 美东
- 3r_scan_job: 每 5 分钟

### 6. SQLite 并发加固
- WAL 模式 (读写并发不互斥)
- busy_timeout = DB_TIMEOUT * 1000ms (排队等待不崩溃)
- 3 个复合索引: idx_ledger_status, idx_ledger_symbol_status, idx_ledger_create_time

## 环境配置
- Client ID: 999 (避免与桌面端 ibkr-order-tool 冲突)
- TWS 端口: 跟随桌面端 settings.json (实盘 7496 / 模拟 7497)
- 日志文件: risk_manager.log (业务日志) + risk_manager_trace.log (AI追踪)
- .env 不含在 Git 中，.env.example 是配置模板
