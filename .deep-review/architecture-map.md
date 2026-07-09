# Codebase Map

> Generated at 2026-07-09. Commit: `545370e`. Repo: `gengxianglong-star/RiskManager_System`.

## Automated Context

### Git Activity
- Commits (3 months): 19
- Contributors: 1 — gengxianglong-star
- Recent bug fixes: 1 — `fa9b4a8` 统一出站队列：Telegram+Notion双通道可靠投递 + 全部bug修复
- Note: Shallow repo — hotspot analysis limited (19 total commits). Fall back to file size + complexity.

### Hottest Files (most changed in 3 months)
| File | Changes | Notes |
|------|---------|-------|
| main.py | 17 | App entry point, daemons, APScheduler — 763 lines |
| database.py | 9 | SQLite schema, connect_db, helper functions |
| requirements.txt | 6 | Python dependencies evolved |
| config.py | 6 | Environment-based configuration |
| notion_api.py | 5 | Notion API — page create/update |
| .env.example | 4 | Environment template |
| market_regime.py | 3 | Market width → regime classification |
| outbound_queue.py | 2 | Outbox pattern + circuit breaker |
| telegram_router.py | 1 | Recently stabilized |
| risk_engine.py | 1 | Recently stabilized |

### Known CVEs (from dependency audit)
No pip audit available. Dependencies (from requirements.txt): `ib_insync==0.9.86`, `python-telegram-bot[job-queue]>=21.0`, `apscheduler>=3.10.0`, `pandas>=2.2.0`, `pandas_market_calendars>=5.0`, `notion-client>=2.2.1`, `aiosqlite>=0.20.0`, `aiohttp>=3.9.0`, `python-dotenv>=1.0.0`, `nest_asyncio>=1.6.0`, `yfinance>=0.2.30`.

### New Code (added last month)
All 27 files added within the last month — entire project is new code (19 commits of a relatively fresh project).

### Linter Summary
Not run — no ruff/flake8/eslint configured in project.

### Runtime / Reliability Signals
- **Outbound queue** (`outbound_queue.py`): Outbox pattern with `enqueue_outbound()` → `outbound_worker()` consumer. Circuit breaker (3-state: CLOSED/OPEN/HALF_OPEN), exponential backoff (30s→60s→120s→300s cap), dead letter queue at 10 retries. Notion throttled at 350ms/item. Telegram channel bypasses circuit breaker.
- **Retry patterns**: Heavy retry throughout — TWS connection (5 attempts, 10-60s backoff), Telegram polling (10 inner × 50 outer retries), Flex query (3 attempts, 20-60s backoff), Telegram send (3 retries with 429 respect), outbound worker (10 retries with exponential backoff).
- **Locks**: Per-symbol `asyncio.Lock` pool in `RiskManagerApp.get_symbol_lock()`. Used for trade execution serialization.
- **APScheduler**: All periodic jobs (reconciliation every 15min, Flex settlement at 09:00/16:30 US Eastern, daily rollover, force confession, 3R scans every 5min, corporate actions at 08:00).
- **Circuit breaker**: `CircuitBreaker` in `outbound_queue.py` — Notion channel only, 5 failures trigger OPEN, 300s recovery timeout, HALF_OPEN probe.
- **Cache**: `market_regime.py` has 300s TTL cache for SPY market regime. `system_status_cache` dict for Notion online/order tool running.
- **Transaction boundaries**: SQLite with WAL mode. `busy_timeout=10s`. Multiple `connect_db()` calls per action (each is a separate aiosqlite connection/transaction). No `BEGIN/COMMIT` semantic grouping across multiple `connect_db()` calls.
- **Workers**: `outbound_worker` (single consumer, FIFO, 10 items per batch), `keepalive_daemon`, `status_probe_daemon`, `context_backfill_daemon`.
- **Deployment**: Local Windows desktop (`run.bat`) or cloud VPS (`ENABLE_EOD_SNIPER=true`). Single process, asyncio event loop. No Docker/compose/k8s found.
- **Proxy**: HTTP_PROXY/HTTPS_PROXY configured for Clash Verge Rev (`127.0.0.1:7897`). `ALL_PROXY` explicitly stripped for httpx compatibility.
- **Integration signals**: IBKR `ib_insync` (TWS API), Notion API via `notion-client`, Telegram Bot API via `python-telegram-bot`, yfinance for corporate actions, Google Sheets CSV for market regime.

## Company Context

- **Identity**: Personal trading risk management system ("极致动量风控军师系统" = Extreme Momentum Risk Control Strategist System). Single-user tool for a retail trader.
- **Product**: A Telegram-bot-driven risk management overlay for Interactive Brokers TWS. Monitors trades, enforces position sizing, tracks P&L, syncs to Notion as a trading journal. One-sentence: A personal AI-augmented risk manager that watches IBKR trades and enforces discipline via Telegram.
- **Stage**: Early — single developer, 19 commits, personal tool.
- **User metric**: 1 user (the developer/trader). Not SaaS.
- **Scale signals**:
  - Customer count: 1 (personal)
  - ACV band: N/A (personal use)
  - Team size: 1 engineer
  - Real money is on the line (IBKR live/paper trading accounts)
- **Trust & Compliance**:
  - Multi-tenant: No (single user)
  - Sensitive data: IBKR account equity, trade history, Telegram chat ID, API tokens, Notion database
  - Compliance: None formal. But **money is at stake** — bugs can cause real financial loss through missed stops, incorrect position tracking, or kill-switch failures.
- **Severity calibration**: Since this is personal and real-money, CRITICAL = any bug that could cause financial loss (wrong position tracking, failed kill switch, incorrect P&L) or data corruption (shadow ledger desync from TWS).

## Repo Topology
| Repo | Role | Key tech | Owns | Talks to |
|------|------|----------|------|----------|
| RiskManager_System | Backend/monolith | Python 3.14, ib_insync, python-telegram-bot, aiosqlite, apscheduler | Trade monitoring, risk management, Notion journaling | IBKR TWS (ib_insync), Telegram Bot API, Notion API, Google Sheets (market regime), yfinance (corporate actions) |

Single repo — no multi-repo topology.

## Context Profile

- **Product stage**: Early — 19 commits, single developer, personal tool. Low test coverage (no test files found), no CI/CD.
- **Deployment**: Local desktop (Windows `.bat` script or `python main.py`) or cloud VPS (`ENABLE_EOD_SNIPER=true`). Single asyncio process. No container orchestration.
- **Users & tenancy**: Single user — hardcoded `MY_TELEGRAM_CHAT_ID`. All commands gated behind `require_auth` decorator checking chat_id match.
- **Sensitive data**: IBKR account NetLiquidation (equity), complete trade history with P&L, Telegram Bot token, IBKR Flex token, Notion API token, local SQLite database. **Data loss = permanent loss of trading journal.**
- **Trust boundaries**: 
  - Telegram chat ID check (single `require_auth` decorator on all commands)
  - No authentication on the Telegram webhook — chat_id is the only gate
  - Inline keyboard callbacks also check chat_id
  - TWS connection via localhost (127.0.0.1) — no remote auth
- **Existing quality tooling**: 
  - `@ai_trace` decorator on most functions (crash report + timing)
  - Rotating log files (`risk_manager_trace.log`, 10MB × 5)
  - No linter, no type checker, no test framework, no CI/CD
- **Bug history patterns**: The single bug-fix commit (`fa9b4a8`) introduced a complete outbound queue refactor — suggests reliability of notification delivery was the primary concern.
- **Runtime topology**:
  - **Request handlers**: Telegram bot commands via `python-telegram-bot` polling
  - **Event handlers**: TWS `execDetailsEvent` and `openOrderEvent` callbacks (IB thread → asyncio dispatch)
  - **Scheduled jobs** (APScheduler): reconciliation (15min), Flex settlement (09:00/16:30 ET), daily rollover (00:05 ET), force confession (23:00 Shanghai), 3R scan (5min), corporate actions (08:00 ET)
  - **Scheduled jobs** (Telegram job_queue): EOD 10EMA sniper (15:55 ET, optional)
  - **Daemons** (asyncio tasks): outbound_worker, keepalive_daemon, status_probe_daemon, context_backfill_daemon
  - **Queues**: outbound_queue (SQLite-backed, FIFO, 10/batch) — the only queue
  - **Caches**: market_regime (300s TTL, module-level global), system_status_cache (dict, updated every 30s)
  - **Third-party APIs**: IBKR TWS (localhost), IBKR Flex (HTTP), Notion API (HTTP), Telegram Bot API (HTTP/proxy), Google Sheets (HTTP), yfinance (HTTP)
  - **Database**: SQLite (aiosqlite, WAL mode, single-file `risk_manager.db`)
- **Consistency model**:
  - **Transaction boundaries**: Each `connect_db()` call creates a new aiosqlite connection with auto-commit. No explicit multi-statement transactions. `commit()` called explicitly after each logical batch. **No cross-connection atomicity.**
  - **Retry sources**: Telegram polling (10×50 retries), TWS connection (5 attempts, 10-60s), Flex query (3 attempts, 20-60s), outbound worker (10 retries with exponential backoff), telegram send (3 retries, 429-aware)
  - **Replay sources**: outbound_queue `INSERT OR IGNORE` provides idempotency per `(event_key, channel)`. Flex settlement has MD5 hash dedup + `flex_processed_execs` table. `_delayed_bracket_stop_capture` checks outbound_queue for duplicate OPEN events.
  - **Stale-read risks**: market_regime module-level cache (300s TTL). system_status_cache (30s probe interval). No read-replica pattern (SQLite is single-file).
  - **Partial failure**: Trade execution (ib_listener.py) uses per-symbol locks, but connects to DB across multiple `connect_db()` calls within one execution handler — a crash mid-handler leaves partial DB writes.
- **Production amplifiers**:
  - **Traffic spikes**: Not applicable (single user). But rapid-fire trades from TWS (e.g., bracket order fills) can trigger multiple concurrent `_async_on_execution` calls → race on `connect_db()` connections.
  - **Retry storms**: Flex query can retry 3 times with 20-60s delays. Telegram polling retries 50× with escalating delay. Outbound queue backs off exponentially across 10 retries. No global rate limit coordinator.
  - **Worker overlap**: APScheduler reconciliation (15min) + Flex settlement (09:00/16:30) + outbound_worker (continuous) all operate on the same SQLite database. WAL mode helps but concurrent writes still contend on `busy_timeout`.
  - **Deploy overlap**: Not applicable (single process, local deployment). But TWS disconnection/reconnection triggers background reconcile + Flex sync jobs.
  - **Schema drift**: ALTER TABLE in `ensure_schema()` is try/except-passed. Old `notion_queue` table coexists with new `outbound_queue`.
  - **Pool exhaustion**: `_dispatch_async` spawns background tasks without bounds. Rapid-fire execution events could create unbounded concurrent tasks.
- **Incident clues**:
  - Multiple `asyncio.sleep()` patterns suggest race-condition workarounds (3s delay for bracket stop capture, 2s after cancel-before-kill-switch, 0.5s between yfinance calls)
  - Symbol lock granularity: per-symbol lock works for single-symbol trades but multi-symbol operations (e.g., kill switch) still need cross-symbol coordination.
  - `killing_symbols` set with 10s auto-discard prevents double-kill but has a race window if a second kill request arrives after discard.
  - `_nightwatchman_done` set never cleaned — once a symbol gets its stop pushed to breakeven, it's permanently removed from consideration (even if a new position is opened later).
  - Monkey-patches in main.py (lines 40-66): `Decoder.interpret` and `Wrapper.completedOrder` — compatibility hacks for TWS 10.48+.
- **Repo topology**: Single repo, monolith. No microservices, no shared SDK.
- **Integration surfaces**: 
  - IBKR TWS API (ib_insync, localhost)
  - IBKR Flex Web Service (HTTP + SSL)
  - Telegram Bot API (HTTP through proxy)
  - Notion API (HTTP through proxy)
  - Google Sheets CSV export (HTTP, unauthenticated)
  - yfinance (HTTP, for splits + last price)

## Impact Taxonomy

Every finding must map to ONE impact category:

- **REVENUE_LEAK**: Money lost through missed stops, incorrect position sizing, kill-switch failure, or P&L miscalculation. Real dollars via IBKR account.
- **SUPPORT_BURDEN**: N/A (single user). But confusion from wrong data in /status or notifications is a user-facing bug.
- **DATA_LOSS**: Shadow ledger data (the only source of truth for trading journal). If SQLite is corrupted or positions go out of sync, trading history is permanently lost.
- **SECURITY_BREACH**: Unauthorized Telegram commands (chat_id spoofing), token leakage in logs/db.
- **COMPLIANCE**: N/A (personal tool, no regulated entities).
- **PROD_INCIDENT**: System crash during active trading, kill switch failure, TWS disconnection during critical trade.
- **BRAND_DAMAGE**: N/A (personal tool).

## Severity Calibration

Severity = (impact category weight) × (population affected) × (frequency) × (reversibility).

For this personal system, "meaningful subset" = 100% of users (1 user). Frequency = every trade. Reversibility = varies — some losses are permanent.

### The CRITICAL test
Label CRITICAL when the user would lose money, lose trade data, or lose trade protection as a consequence of this bug. The bug must fire under normal usage (trading hours, connected TWS, active positions).

### Archetypes
- **Money loss**: Kill switch fails → position continues running without stop → unlimited loss. Duplicate close → wrong P&L → incorrect decisions. Stop sync misses TWS update → stale stop in shadow ledger → risk calculations wrong.
- **Data corruption**: Shadow ledger desyncs from TWS → P&L permanently wrong → trading journal useless.
- **Full outage**: TWS connection lost during trading hours → no execution monitoring → positions unmanaged.
- **Silent failure**: Outbound queue dead letter → critical notification never delivered → user unaware of stop being hit / kill switch executed.

## Feature Map

### Category: Core Trading
#### Feature: Trade Execution Monitoring
- Files: `ib_listener.py:184-443`, `main.py:40-66`
- Data flow: TWS `execDetailsEvent` → `on_execution` (IB thread) → `_dispatch_async` → `_async_on_execution` (asyncio). Reads/writes `shadow_ledger`, `outbound_queue`, `system_state`.
- External deps: ib_insync (TWS API, localhost)
- Risk: HIGH — real-time trade monitoring, partial writes possible across multiple DB connections, race condition on same-symbol trades

#### Feature: Position Reconciliation
- Files: `reconciliation.py`
- Data flow: `ib.reqPositionsAsync()` → compare with `shadow_ledger` → auto-close ghosts / auto-import missing → notify via outbound queue
- External deps: ib_insync
- Risk: HIGH — automated position modifications, FIFO close logic for discrepancies

#### Feature: Flex Settlement (CQRS Read Model)
- Files: `flex_settlement.py`
- Data flow: Pull IBKR Flex XML → extract trades → MD5 hash dedup → exec_id dedup → FIFO close in shadow_ledger → push CLOSE events to outbound queue
- External deps: IBKR Flex Web Service (HTTP), pandas_market_calendars
- Risk: MEDIUM — periodic, not real-time. MD5 + exec_id double dedup is defensive. But stale hash could suppress genuine new trades.

### Category: Risk Management
#### Feature: Risk Engine (Traffic Light, Position Validation, Kill Switch)
- Files: `risk_engine.py:39-301`
- Data flow: Equity check → drawdown calculation → light determination. /init command → validate → kill switch option. /unlock → execute kill switch via MarketOrder.
- External deps: ib_insync (for market data, order placement, position queries)
- Risk: CRITICAL — kill switch failure = unlimited loss. Risk light miscalculation = wrong position sizing. Per-symbol locking works but has gaps.

#### Feature: 3R Profit Radar
- Files: `scale_out_monitor.py`
- Data flow: APScheduler every 5min → scan OPEN positions with initial_stop > 0 → calculate current R-multiple → alert if ≥3R and stop not at breakeven
- External deps: ib_insync (for current price)
- Risk: LOW — advisory only, no automated actions

#### Feature: EOD 10EMA Sniper
- Files: `risk_engine.py:304-401`
- Data flow: Telegram job_queue at 15:55 ET → scan runners (stop ≥ entry) → fetch 10EMA → check price vs 10EMA → execute kill switch if broken
- External deps: ib_insync (historical data, market data, order placement)
- Risk: HIGH — automated kill switch with false-positive potential. Uses 1% buffer (0.99×EMA). Runner detection logic is subtle (must have init_stop > 0 and curr_stop ≥ entry).

### Category: Communication
#### Feature: Outbound Queue (Outbox Pattern)
- Files: `outbound_queue.py`
- Data flow: `enqueue_outbound(event_key, channel, payload)` → INSERT OR IGNORE → `outbound_worker` polls every 2s → `_send_telegram` or `_send_notion` → mark sent/failed/retry
- External deps: aiosqlite, notion_api
- Risk: MEDIUM — telegram bypasses circuit breaker. Notion has full CB + throttle + backoff + dead letter. But telegram failure is silently swallowed after 3 internal retries.

#### Feature: Telegram Command Router
- Files: `telegram_router.py`
- Data flow: Command → `require_auth` (chat_id check) → handler → DB queries → response
- External deps: python-telegram-bot
- Risk: MEDIUM — `/init` has complex multi-step flow (fetch price, calculate risk, validate, save intent, confirm via inline keyboard). Intent expiry not enforced — pending_intents never cleaned up.

### Category: Infrastructure
#### Feature: Market Regime Classification
- Files: `market_regime.py`
- Data flow: Fetch Google Sheets CSV → parse Stockbee width data → derive regime → return (label, insight, risk_multiplier). 300s TTL cache.
- External deps: aiohttp, Google Sheets (unauthenticated CSV export)
- Risk: LOW — advisory. Falls back to offline label (risk_mult=1.0) on any error.

#### Feature: Database
- Files: `database.py`
- Data flow: `ensure_schema()` → create tables + indexes. `connect_db()` → PRAGMA WAL + busy_timeout + foreign_keys. Helper CRUD functions.
- External deps: aiosqlite
- Risk: HIGH — no migrations framework. ALTER TABLE in try/except. Schema has legacy `notion_queue` table alongside new `outbound_queue`.

## Critical Workflow Ledger

### Workflow: Real-time Trade Capture (TWS → Shadow Ledger → Notion)
- Repos involved: RiskManager_System (single repo)
- Invariant: Every TWS fill must result in exactly one shadow_ledger entry and one Notion record. No duplicate entries, no missed fills.
- Entry points: `ib_listener.on_execution()` (IB event dispatch)
- Writers / side effects: `shadow_ledger` INSERT/UPDATE, `outbound_queue` INSERT, `system_state` UPDATE, TWS order placement (kill switch bypass)
- Transaction boundary: Each `connect_db()` block is its own transaction. Opening flow touches 2-3 separate `connect_db()` connections: (1) initial execution handler, (2) delayed bracket stop capture, (3) possibly context backfill.
- Retry / replay sources: None at the execution level. `INSERT OR IGNORE` on outbound_queue prevents duplicate notifications but not duplicate shadow_ledger entries. Per-symbol lock prevents concurrent same-symbol execution but not interleaved open/close.
- Ordering assumptions: Opening must complete (including bracket stop capture at +3s) before closing can begin. Per-symbol lock enforces this.
- Cache / replica assumptions: market_regime cache (300s) used during bracket stop capture for SPY context.
- Capacity dependencies: Unbounded `_dispatch_async` → `spawn_background_task` for each execution. Rapid fills could create dozens of concurrent `_delayed_bracket_stop_capture` tasks.
- Deploy / migration risk: Schema changes via ALTER TABLE try/except. Old `notion_queue` table still exists.
- Why this is likely to fail only in production: Multiple concurrent fills on the same symbol (bracket orders, partial fills) create race conditions in the per-symbol lock that work in testing but can deadlock or interleave under real IBKR fill patterns.

### Workflow: Kill Switch Execution
- Repos involved: RiskManager_System
- Invariant: Kill switch must cancel ALL open orders and close ALL positions for the given symbol. Must not create duplicate orders. Must not leave positions after execution.
- Entry points: Telegram /unlock command, risk_engine.validate_pending_entry rejection, Telegram KILL callback, EOD sniper
- Writers / side effects: TWS `cancelOrder`, TWS `placeOrder` (MarketOrder), `shadow_ledger` UPDATE, `outbound_queue` INSERT, `killing_symbols` set
- Transaction boundary: No DB transaction — kill switch operates on TWS directly. Shadow ledger cleanup happens later via async execution callback.
- Retry / replay sources: `killing_symbols` set prevents double execution for 10 seconds. But after 10s, a new kill request for same symbol would execute again.
- Ordering assumptions: Cancel open orders → sleep 2s → check positions → place market order. If positions changed during the 2s sleep, the market order size may be stale.
- Cache / replica assumptions: None.
- Capacity dependencies: Single symbol processing. Sequential — blocks the async handler thread (technically doesn't block, but the per-symbol lock serializes kill with other same-symbol operations).
- Deploy / migration risk: None specific.
- Why this is likely to fail only in production: In production with fast-moving markets, the 2s delay between cancel and re-check can result in either double-closing (position changed by another fill) or missing new positions. The `killing_symbols` 10s timer is arbitrary.

### Workflow: Flex → Shadow Ledger Close
- Repos involved: RiskManager_System
- Invariant: Flex settlement must close shadow_ledger positions that IBKR reports as closed, with correct P&L. Must never double-count P&L. Must never miss a close.
- Entry points: APScheduler cron (09:00, 16:30 ET), manual /sync command, TWS reconnect trigger
- Writers / side effects: `shadow_ledger` UPDATE (exit_price, realized_pnl), `flex_processed_execs` INSERT, `system_state` UPDATE, `outbound_queue` INSERT
- Transaction boundary: Per-trade processing. MD5 hash check gates the entire run. exec_id table prevents per-exec double-count.
- Retry / replay sources: Flex query retries (3×, 20-60s). MD5 hash prevents full re-run. exec_id table prevents per-exec re-processing.
- Ordering assumptions: Flex report represents authoritative close data. FIFO order within shadow_ledger (oldest OPEN first).
- Cache / replica assumptions: None.
- Capacity dependencies: Flex report size. IBKR Flex API rate limits (1001/1025 errors → skipped).
- Deploy / migration risk: Schema drift — `flex_processed_execs` created on-demand.
- Why this is likely to fail only in production: IBKR Flex report delays (minutes to hours after trade) mean closes may not appear for several settlement runs. MD5 hash dedup could suppress legitimate new trades if the same positions open/close with identical prices (unlikely but possible with limit orders).

### Workflow: Outbound Queue Processing
- Repos involved: RiskManager_System
- Invariant: Every enqueued message must eventually be delivered or explicitly marked as failed. No message loss.
- Entry points: `enqueue_outbound()` called from anywhere in the system
- Writers / side effects: outbound_queue UPDATE (status, retry_count, notion_page_id), Telegram send, Notion API calls
- Transaction boundary: Individual messages — each message processed independently. Retry-on-failure with break in batch processing.
- Retry / replay sources: Exponential backoff (30s→300s cap, 10 retries). Notion circuit breaker (5 failures → 300s OPEN). Telegram bypasses CB.
- Ordering assumptions: FIFO by id within batch of 10. Batch halts on retryable failure (break loop).
- Cache / replica assumptions: None.
- Capacity dependencies: Worker processes 10 per batch, 2s idle between batches. Telegram 350ms throttle per notion message.
- Deploy / migration risk: `notion_page_id` column added via ALTER TABLE.
- Why this is likely to fail only in production: Notion API outages in production cause circuit breaker OPEN → all notion messages queued. If TWS generates many trades during outage, queue could grow large. Telegram channel silently drops after 3 internal retries in gateway (separate from outbound queue retries).

### Workflow: SPY Context Backfill
- Repos involved: RiskManager_System
- Invariant: Every OPEN position with missing SPY context should eventually get it populated. Context must be the current (not historical) market regime.
- Entry points: `context_backfill_daemon` (every 5min)
- Writers / side effects: `shadow_ledger` UPDATE (spy_context), `outbound_queue` INSERT (UPDATE to Notion)
- Transaction boundary: Each position processed in its own connect_db block. One `fetch_market_regime()` call per cycle (shared across all bare positions).
- Retry / replay sources: Runs every 5min until all bare positions are filled.
- Ordering assumptions: All bare positions get the same SPY context for that cycle.
- Cache / replica assumptions: Uses `fetch_market_regime()` with its 300s cache.
- Capacity dependencies: Number of bare positions.
- Deploy / migration risk: None.
- Why this is likely to fail only in production: Production positions opened during market regime cache freshness window get the regime at backfill time (potentially different from entry-time regime). This is a design choice but could mislead journal analysis.

## Integration Map

### Integration: RiskManager → IBKR TWS
- Contract: ib_insync library (TWS API over socket, localhost)
- Source files: `ib_listener.py`, `risk_engine.py`, `reconciliation.py`, `main.py`
- Target files: TWS application (localhost:7496/7497)
- Auth / tenancy coupling: Client ID from config. Port inferred from desktop settings.json.
- Version / rollout coupling: Monkey-patches in `main.py:40-66` for TWS 10.48+ compatibility (Decoder.interpret, Wrapper.completedOrder). **Fragile — new TWS versions may break.**
- Platform-specific risks: Windows desktop deployment. Port already-in-use handling.
- Highest-risk production failure: TWS disconnects mid-trade → execution callback missed → shadow ledger desyncs from reality. Recovery path: reconciliation (15min) + Flex (09:00/16:30). Gap: up to 15min during active trading.

### Integration: RiskManager → Telegram
- Contract: python-telegram-bot (HTTP + long polling)
- Source files: `gateway.py`, `telegram_router.py`
- Target: Telegram Bot API (through proxy 127.0.0.1:7897)
- Auth: Bot token + chat ID verification
- Version: python-telegram-bot[job-queue]>=21.0
- Platform-specific risks: Proxy dependency (Clash Verge Rev). 429 rate limiting with retry.
- Highest-risk production failure: Proxy failure → all notifications dropped → user unaware of fills, stops, kills. Gateway has 50-message queue fallback. Outbound queue has persistent storage.

### Integration: RiskManager → Notion
- Contract: notion-client (HTTP REST)
- Source files: `notion_api.py`, `outbound_queue.py`
- Target: Notion API (through proxy)
- Auth: Integration token + database ID
- Version: notion-client>=2.2.1
- Platform-specific risks: Notion API validation errors (property type mismatches cause 400), rate limiting (~3 req/s), proxy dependency.
- Highest-risk production failure: Notion property schema mismatch → all OPEN/CLOSE events fail validation → dead letter queue → trade journal permanently incomplete. Circuit breaker protects Notion from overload but doesn't fix schema issues.

### Integration: RiskManager → Google Sheets (Market Regime)
- Contract: HTTP GET CSV export (unauthenticated)
- Source files: `market_regime.py`
- Target: Google Sheets CSV export URL
- Auth: None (public sheet)
- Version coupling: Column layout of Stockbee Market Monitor sheet. **Fragile — index-based column access (`_num(latest_row, 14)`). If sheet layout changes, regime classification silently breaks (falls to offline).**
- Highest-risk production failure: Google Sheets unavailable → offline label returned → risk multiplier forced to 1.0. Low-severity but means market context is silently absent during high-volatility periods.

## Action Inventory

### Action 1: User opens a new position (Telegram /init command)
- Entry point: Telegram CommandHandler `/init` → `telegram_router.py:439-569`
- Handler: `telegram_router.py:cmd_init` (line 439)
- Service method: `risk_engine.py:RiskEngine.validate_pending_entry` (line 88), `ib_listener.py:IBKRListener.fetch_entry_price` (line 119), `database.py:insert_shadow_ledger` (line 236)
- Side effects: DB write (pending_intents → shadow_ledger), Telegram message (confirmation with inline keyboard), Notion OPEN (via outbound queue on CONFIRM callback)
- Background work: None directly — CONFIRM callback triggers synchronous processing
- Complexity: HIGH — 8 validation gates, 2-step user confirmation, dynamic risk budget calculation
- Priority: HIGH — primary user action, touches risk calculation, position sizing, market regime
- Hot files: main.py, telegram_router.py

### Action 2: TWS execution detected (trade fill from IBKR)
- Entry point: TWS `execDetailsEvent` → `ib_listener.py:184` (on_execution)
- Handler: `ib_listener.py:on_execution` → `_async_on_execution` (line 192)
- Service method: `ib_listener.py:_async_on_execution` (line 192), `risk_engine.py:RiskEngine.validate_pending_entry` (line 88), `risk_engine.py:RiskEngine.execute_kill_switch` (line 130)
- Side effects: DB write (shadow_ledger INSERT/UPDATE, system_state UPDATE), outbound_queue INSERT (telegram + notion), background task (_delayed_bracket_stop_capture), possible kill switch
- Background work: `_delayed_bracket_stop_capture` (3s delay → TWS stop sync → Notion OPEN)
- Complexity: HIGH — FIFO close logic, partial fill handling, averaging-down detection, kill switch bypass, multiple DB connections
- Priority: CRITICAL — real-time trade capture, most bugs here cause data corruption or money loss
- Hot files: main.py, ib_listener.py

### Action 3: Kill switch execution
- Entry point: Telegram `/unlock`, `/override`, KILL callback, EOD sniper, or validation rejection
- Handler: `telegram_router.py:cmd_unlock` (line 159), `risk_engine.py:RiskEngine.execute_kill_switch` (line 130)
- Service method: `risk_engine.py:RiskEngine.execute_kill_switch` (line 130)
- Side effects: TWS cancelOrder (multiple), TWS placeOrder (MarketOrder), DB write (shadow_ledger CLOSE), Telegram notification
- Background work: Async execution callback (on_execution → CLOSE processing)
- Complexity: MEDIUM — serial steps (cancel → wait → check → market order), cross-system coordination
- Priority: CRITICAL — failure = unlimited loss. Unbounded market risk.
- Hot files: risk_engine.py

### Action 4: TWS stop order modification detected
- Entry point: TWS `openOrderEvent` → `ib_listener.py:573` (on_open_order)
- Handler: `ib_listener.py:_async_on_open_order` (line 577)
- Service method: N/A (inline DB update)
- Side effects: DB write (shadow_ledger current_stop UPDATE), Telegram notification
- Background work: None
- Complexity: LOW — single DB read + update, threshold check
- Priority: MEDIUM — if missed, shadow ledger has stale stop → risk calculations wrong
- Hot files: ib_listener.py

### Action 5: Physical position reconciliation (periodic)
- Entry point: APScheduler every 15min → `reconciliation.py:59`
- Handler: `reconciliation.py:reconcile_physical_positions` (line 59)
- Service method: `reconciliation.py:reconcile_physical_positions`
- Side effects: DB write (shadow_ledger INSERT for auto-import, UPDATE for ghost close), outbound_queue INSERT, Telegram notification
- Background work: None
- Complexity: HIGH — multi-phase: ghost detection → FIFO close → auto-import → price fix → stop sync → Notion push
- Priority: HIGH — automated position correction, can create wrong positions if logic is flawed
- Hot files: reconciliation.py

### Action 6: Flex authoritative settlement (periodic)
- Entry point: APScheduler cron 09:00/16:30 → `flex_settlement.py:34`
- Handler: `flex_settlement.py:run_flex_settlement` (line 34)
- Service method: `flex_settlement.py:run_flex_settlement`
- Side effects: DB write (shadow_ledger UPDATE, flex_processed_execs INSERT, system_state UPDATE), outbound_queue INSERT, Telegram notification
- Background work: None (synchronous Flex report fetch in executor)
- Complexity: MEDIUM — MD5 dedup, exec_id dedup, FIFO close, P&L accumulation, consecutive loss tracking
- Priority: MEDIUM — authoritative but delayed; real-time TWS events handle immediate close detection
- Hot files: flex_settlement.py

### Action 7: User checks status (/status)
- Entry point: Telegram CommandHandler `/status` → `telegram_router.py:266`
- Handler: `telegram_router.py:cmd_status` (line 266)
- Service method: `risk_engine.py:RiskEngine.calculate_risk_light` (line 49)
- Side effects: None (read-only)
- Background work: None
- Complexity: MEDIUM — aggregates data from multiple sources (IBKR equity, shadow_ledger, system_state, service status lines)
- Priority: LOW — read-only, advisory
- Hot files: telegram_router.py

### Action 8: User imports physical positions (/import)
- Entry point: Telegram CommandHandler `/import` → `telegram_router.py:367`
- Handler: `telegram_router.py:cmd_import` (line 367)
- Service method: N/A (inline DB INSERT loop)
- Side effects: DB write (shadow_ledger INSERT with setup_tag='TWS_SYNC'), Telegram notification
- Background work: None
- Complexity: LOW — simple loop: get positions → filter existing → insert
- Priority: MEDIUM — bulk position import, N+1 queries, no transaction boundary across multiple inserts
- Hot files: telegram_router.py

### Action 9: User updates stop loss (/update)
- Entry point: Telegram CommandHandler `/update` → `telegram_router.py:573`
- Handler: `telegram_router.py:cmd_update` (line 573)
- Service method: N/A (inline DB UPDATE)
- Side effects: DB write (shadow_ledger current_stop UPDATE)
- Background work: None
- Complexity: LOW — single validation (MAX_STOP_PCT) + UPDATE
- Priority: LOW — simple, low risk
- Hot files: telegram_router.py

### Action 10: User splits or renames stock (/split, /rename)
- Entry point: Telegram CommandHandler `/split` `/rename` → `telegram_router.py:615,657`
- Handler: `telegram_router.py:cmd_split` (line 615), `cmd_rename` (line 657)
- Service method: N/A (inline DB UPDATE with arithmetic)
- Side effects: DB write (shadow_ledger UPDATE — quantity, entry_price, initial_stop, current_stop)
- Background work: None
- Complexity: LOW — direct arithmetic UPDATE
- Priority: MEDIUM — if miscalculated, all subsequent risk/P&L calculations are wrong
- Hot files: telegram_router.py

### Action 11: User overrides a violation (/override)
- Entry point: Telegram CommandHandler `/override` → `telegram_router.py:205`
- Handler: `telegram_router.py:cmd_override` (line 205)
- Service method: N/A (inline DB UPDATE)
- Side effects: DB write (shadow_ledger UPDATE — stop to entry×0.9, setup_tag='FOMO'), outbound_queue INSERT
- Background work: None
- Complexity: LOW — single DB update + notify
- Priority: MEDIUM — sets arbitrary 10% stop without validation; could be below current price
- Hot files: telegram_router.py

### Action 12: System performs daily rollover (automated)
- Entry point: APScheduler cron 00:05 ET → `main.py:479`
- Handler: `main.py:run_daily_boot_checks` (line 479)
- Service method: `main.py:run_daily_boot_checks` (line 479)
- Side effects: DB write (system_state UPDATE), Telegram notification (review keyboard, loss counter reset)
- Background work: None
- Complexity: MEDIUM — consecutive loss reset, review prompt with inline keyboard
- Priority: MEDIUM — if wrong date used, consecutive loss counter could be wrong → wrong risk light
- Hot files: main.py

### Action 13: EOD 10EMA Sniper (automated, optional)
- Entry point: Telegram job_queue daily at 15:55 ET → `risk_engine.py:309`
- Handler: `risk_engine.py:eod_10ema_sniper_job` (line 309)
- Service method: `risk_engine.py:eod_10ema_sniper_job`, `risk_engine.py:RiskEngine.execute_kill_switch` (line 130)
- Side effects: TWS placeOrder (MarketOrder if 10EMA broken), DB write (shadow_ledger CLOSE via async execution callback), Telegram notification
- Background work: Kill switch execution triggers async execution callback for ledger cleanup
- Complexity: HIGH — runner detection logic, 10EMA fetch, 1% buffer, automated kill switch
- Priority: HIGH — automated position liquidation, false positive = unnecessarily closed position, false negative = runner held through breakdown
- Hot files: risk_engine.py

### Action 14: Corporate actions check (automated)
- Entry point: APScheduler cron 08:00 ET → `main.py:417`
- Handler: `main.py:check_corporate_actions_job` (line 417)
- Service method: N/A (inline yfinance fetch + DB UPDATE)
- Side effects: DB write (shadow_ledger UPDATE — split adjustment, applied_splits INSERT), Telegram notification
- Background work: None (synchronous yfinance call in executor)
- Complexity: MEDIUM — split arithmetic (ratio handling), date filtering relative to create_time, yfinance reliability
- Priority: HIGH — incorrect split adjustment corrupts all position data permanently
- Hot files: main.py

### Action 15: Outbound queue worker (continuous background daemon)
- Entry point: `main.py:213` → spawns `outbound_worker`
- Handler: `outbound_queue.py:outbound_worker` (line 94)
- Service method: `outbound_queue.py:outbound_worker`, `outbound_queue.py:_send_telegram`, `outbound_queue.py:_send_notion`
- Side effects: DB write (outbound_queue UPDATE), Telegram send, Notion API create/update
- Background work: Self — it IS the background worker
- Complexity: HIGH — circuit breaker, exponential backoff, batch processing, dead letter queue, Notion Query Before Create dedup, page_id reverse propagation
- Priority: CRITICAL — all notifications flow through this. Failure = silent system.
- Hot files: outbound_queue.py

### Action 16: SPY context backfill (automated)
- Entry point: `main.py:211` → spawns `context_backfill_daemon`
- Handler: `main.py:context_backfill_daemon` (line 336)
- Service method: `market_regime.py:fetch_market_regime` (line 122)
- Side effects: DB write (shadow_ledger UPDATE, outbound_queue UPDATE), outbound_queue INSERT (Notion UPDATE)
- Background work: Self — it IS the daemon
- Complexity: MEDIUM — scans bare positions, fetches SPY once, updates individually, page_id propagation
- Priority: LOW — cosmetic (fills SPY context for journal completeness)
- Hot files: main.py

### Action 17: 3R scale-out radar (periodic)
- Entry point: APScheduler every 5min → `scale_out_monitor.py:28`
- Handler: `scale_out_monitor.py:ScaleOutMonitor.run_3r_scan` (line 28)
- Service method: N/A (inline scan + notify)
- Side effects: Telegram notification (advisory only)
- Background work: None
- Complexity: LOW — scan OPEN positions, calculate R, notify if threshold
- Priority: LOW — advisory only, no automated actions
- Hot files: scale_out_monitor.py

---

Mapped 7 features and 17 user actions across the codebase — ready for team-intent and action traces.
