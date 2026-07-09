# Deep Review Findings

> Reviewed at 2026-07-09. Commit: `545370e`. Repo: `gengxianglong-star/RiskManager_System`.
> 11 confirmed findings. Run via the Farfield Deep Review skill — [farfield.dev](https://farfield.dev)

## Summary

| # | Severity | Impact | Title | File |
|---|---|---|---|---|
| 1 | HIGH | DATA_LOSS | Telegram notifications silently lost despite outbound queue confirming delivery | `gateway.py:65-71` |
| 2 | HIGH | REVENUE_LEAK | Kill switch protection expires 10 seconds after execution | `risk_engine.py:195` |
| 3 | HIGH | REVENUE_LEAK | Kill switch places stale-sized MarketOrder when no orders cancelled | `risk_engine.py:163-180` |
| 4 | HIGH | REVENUE_LEAK | Night watchman permanently disabled for re-entered symbols | `risk_engine.py:213-215` |
| 5 | HIGH | DATA_LOSS | LIKE wildcard matches wrong trade_ids, corrupting Notion pages | `outbound_queue.py:151-157` |
| 6 | HIGH | DATA_LOSS | Flex leaves positions OPEN after writing close data | `flex_settlement.py:217-221` |
| 7 | HIGH | DATA_LOSS | Partial fill realized P&L never recorded | `ib_listener.py:399-433` |
| 8 | MEDIUM | DATA_LOSS | Flex LIMIT 1 closes only one tranche per symbol | `flex_settlement.py:197` |
| 9 | MEDIUM | DATA_LOSS | Stop sync applies single TWS stop to all tranches | `reconciliation.py:230-239` |
| 10 | MEDIUM | DATA_LOSS | Kill switch closes produce no Notion CLOSE events | `ib_listener.py:213` |
| 11 | MEDIUM | DATA_LOSS | Notion QBC creates duplicate pages on query failure | `outbound_queue.py:292-293` |

## Dropped Findings

- **Debounced notifications bypass outbound queue** — Gate A (severity): LOW. Duplicate delivery is cosmetic; no data or money at risk.
- **Pending trade intents never expire** — Gate A (severity): LOW. Requires user to revisit days-old chat messages; no automatic trigger.

---

This review ran cold: no team memory, no production signal correlation, no Slack
context, no scheduled cadence, no PR creation, no dedup against existing issues.
[Farfield](https://farfield.dev) runs this same recipe + those five things
continuously, in Slack, against live production telemetry.

If you want findings filed, fixed, and shipped automatically → farfield.dev.

## Findings

### Finding 1: Telegram notifications silently lost despite outbound queue confirming delivery

- **Severity**: HIGH
- **Impact category**: DATA_LOSS
- **Location**: `gateway.py:65-71` → `outbound_queue.py:160-165`
- **Trigger condition**: Telegram API returns 429 rate-limit 3+ times consecutively, OR sustained network outage lasting >3 retries (~14 seconds), OR any non-retryable exception from `bot.send_message`
- **Consequence**: User never receives critical notifications (trade fills, stop syncs, kill switch confirmations), yet the outbound queue marks them as successfully delivered. No persistent record of the failure exists.
- **Verdict**: CONFIRMED

#### What happens

1. System enqueues a critical message via `enqueue_outbound(event_key, "telegram", payload)` → outbound queue persists it to SQLite
2. `outbound_worker` picks up the message → `_send_telegram` calls `app.gateway.notify_user(msg)` → `outbound_queue.py:255`
3. `notify_user` has built-in 3-retry logic. On `RetryAfter` exhaustion: logs "消息丢弃" and **returns normally** (no exception) → `gateway.py:70-71`
4. On `TimedOut`/`NetworkError` exhaustion: queues to `_msg_queue` (in-memory list, **lost on process restart**) and returns normally → `gateway.py:78-84`
5. Back in `outbound_queue.py`, `_send_telegram` sees no exception → worker marks row `status='sent'` → `outbound_queue.py:160-165`
6. Message was never delivered. System believes it was. User never knows.

#### Root cause

`gateway.py:notify_user` is designed as "fire and forget" — it never raises exceptions to callers. All failure modes return `None` after internal retry exhaustion. But `outbound_queue.py:_send_telegram` treats the absence of an exception as delivery confirmation. The two layers have incompatible error semantics: the gateway silently drops, the outbound queue treats silence as success.

For `RetryAfter` (rate limiting), the behavior is especially dangerous — after 3 retries the message is silently dropped with NO fallback. It doesn't even go to `_msg_queue`. For other exceptions, the buffer is in-memory only, so a process restart permanently wipes all queued messages.

#### Evidence

- `gateway.py:65-71`: `RetryAfter` handler returns normally — `logger.error("Telegram 连续限流 3 次，消息丢弃。")` — no exception, no queue
- `gateway.py:72-84`: `TimedOut`/`NetworkError` handler queues to in-memory list, returns normally
- `outbound_queue.py:250-255`: `_send_telegram` calls `notify_user(msg)` — assumes success if no exception
- `outbound_queue.py:160-165`: outbound worker marks `status='sent'` on apparent success

#### How to verify

1. Read `gateway.py:65-71` — confirm no exception is raised after RetryAfter exhaustion
2. Read `outbound_queue.py:160-165` — confirm row is marked `sent` whenever `_send_telegram` returns without exception
3. Trace the call chain: `outbound_worker (line 135)` → `_send_telegram (line 255)` → `notify_user (line 69)` — no error propagation path exists

#### Suggested fix

`notify_user` should raise an exception when internal retries are exhausted (for RetryAfter, TimedOut, and NetworkError). This lets `_send_telegram` catch the exception and trigger the outbound queue's retry logic (exponential backoff, dead letter queue). Alternatively, after exhausting internal retries, re-enqueue the message to outbound_queue with a longer delay instead of dropping it.

Specifically: change `gateway.py:70-71` to `raise RuntimeError(f"Telegram 429 retry exhausted after 3 attempts: {e}")`. Same pattern for lines 78-84 and 85-91.

#### Confidence

- Assumptions named: that `notify_user` returning normally causes outbound_worker to mark the row as sent
- How verified: traced code path — `_send_telegram` at line 255 has no try/except, `notify_user` at line 69 returns None, execution falls through to line 160 where `status='sent'` is committed
- Why 100%: the control flow is unambiguous — no exception from `notify_user` means the outbound worker proceeds to mark success

#### Gate evidence

- IMPACT_CATEGORY: DATA_LOSS — critical trade notifications permanently lost with no audit trail
- IS_LIVE_SURFACE: every Telegram notification flows through this path; gateway is the sole Telegram delivery mechanism
- NO_SCENARIO_MITIGATION: no fallback delivery mechanism after RetryAfter exhaustion; in-memory queue for other failures is wiped on restart
- CTO_TEST: silent notification loss in a risk management system means the user could miss a kill switch execution or stop-loss breach — real money consequence

#### Labels

`double-retry`, `silent-failure`, `notification-loss`, `gateway`

---

### Finding 2: Kill switch protection expires 10 seconds after execution, enabling double execution

- **Severity**: HIGH
- **Impact category**: REVENUE_LEAK
- **Location**: `risk_engine.py:195`
- **Trigger condition**: MarketOrder from first kill switch hasn't filled within 10 seconds (illiquid stock, after-hours, TWS queue delay), and a second kill switch trigger fires (manual /unlock, EOD sniper, duplicate button)
- **Consequence**: Second kill switch passes the `killing_symbols` guard, cancels the pending first MarketOrder, places a NEW MarketOrder. If the first order partially filled between cancel and re-check, the second MarketOrder may be for the wrong size, potentially creating an unintended reverse position.
- **Verdict**: CONFIRMED

#### What happens

1. User triggers kill switch for AAPL → `killing_symbols.add("AAPL")` blocks concurrent calls → `risk_engine.py:137`
2. System cancels orders, places MarketOrder → `risk_engine.py:177-180`
3. `loop.call_later(10.0, self.ctx.killing_symbols.discard, "AAPL")` schedules auto-clear → `risk_engine.py:195`
4. After 10 seconds, if MarketOrder hasn't filled, the guard is gone. A second kill trigger (from EOD sniper at 15:55, or manual duplicate button) passes the `killing_symbols` check
5. Second `execute_kill_switch` cancels the first pending MarketOrder, re-checks positions, places new MarketOrder
6. If the first order partially filled between cancel and re-check, `actual_qty` is wrong → second MarketOrder size is wrong

#### Root cause

The 10-second `loop.call_later` is a timeout-based assumption that the kill switch MarketOrder will fill quickly. In illiquid markets, after-hours, or under TWS latency, this assumption breaks. The guard should be event-driven — cleared only when the execution callback confirms the KILL_SWITCH fill completed, or the order is confirmed cancelled with zero fills.

#### Evidence

- `risk_engine.py:134-135`: guard check — `if symbol in self.ctx.killing_symbols: return`
- `risk_engine.py:137`: `killing_symbols.add(symbol)`
- `risk_engine.py:195`: `loop.call_later(10.0, self.ctx.killing_symbols.discard, symbol)` — auto-discard after arbitrary timeout
- `risk_engine.py:157-162`: cancelOrders loop — cancels ALL pending orders including the prior kill switch MarketOrder

#### How to verify

1. Read `risk_engine.py:195` — note 10-second auto-discard is unconditional
2. Read `risk_engine.py:134-137` — guard only checks set membership, not order lifecycle
3. Trace: after 10s discard, second `execute_kill_switch` at line 134 passes guard → cancels pending orders (including first MarketOrder) → places new MarketOrder

#### Suggested fix

Replace `loop.call_later(10.0, ...)` with event-driven cleanup. Clear `killing_symbols` only in `_async_on_execution` when `setup_tag == "KILL_SWITCH"` and the fill is processed. As a safety net, keep a longer timeout (300s) as fallback, but log a warning if it fires. Additionally, before placing a new MarketOrder, verify no pending KILL_SWITCH-tagged orders exist in `ib.trades()`.

#### Confidence

- Assumptions named: that 10 seconds may be insufficient for MarketOrder fill under illiquid conditions
- How verified: MarketOrder fill time is unbounded — depends on liquidity, exchange hours, TWS latency. 10s is a common but not guaranteed window
- Why 100%: the code unconditionally clears the guard at 10s and the guard is the only mechanism preventing double-execution

#### Gate evidence

- IMPACT_CATEGORY: REVENUE_LEAK — double execution can create unintended reverse position with real financial exposure
- IS_LIVE_SURFACE: kill switch is triggered from multiple live paths (Telegram KILL button, EOD sniper, UI rejection)
- NO_SCENARIO_MITIGATION: no event-driven cleanup; 10s timer is the only mechanism
- CTO_TEST: kill switch is the last-resort risk control — a failure in its execution guard means unbounded loss potential

#### Labels

`kill-switch`, `race-condition`, `double-execution`, `timeout-based-guard`

---

### Finding 3: Kill switch places stale-sized MarketOrder when no orders were cancelled

- **Severity**: HIGH
- **Impact category**: REVENUE_LEAK
- **Location**: `risk_engine.py:163-180`
- **Trigger condition**: Kill switch fires when TWS has no pending orders to cancel (`canceled_count == 0`), and the position size changes between `reqPositionsAsync` (line 147) and `placeOrder` (line 180)
- **Consequence**: MarketOrder is placed with stale position size. If a stop loss or manual close filled between the position snapshot and the MarketOrder, the kill switch opens a new naked position in the opposite direction.
- **Verdict**: CONFIRMED

#### What happens

1. `reqPositionsAsync` at line 147 captures `actual_qty = +100` — position still open
2. Between this call and `placeOrder`, a stop loss triggers and fills → position drops to 0 in TWS
3. No pending orders remain → `canceled_count == 0` at line 163
4. Position re-check block (lines 163-175) is **entirely skipped** because it's gated on `if canceled_count > 0:`
5. `placeOrder(contract, MarketOrder("SELL", 100))` executes — opens SHORT 100 shares
6. User now has an unintended short position with unlimited upside risk

#### Root cause

The position re-check (sleep 2s + re-fetch positions) is conditionally executed only when orders were cancelled. When `canceled_count == 0`, the code path skips the re-check entirely, using `actual_qty` from the first `reqPositionsAsync` — which may be stale by the time `placeOrder` executes. The kill switch should ALWAYS re-verify positions before placing a MarketOrder, regardless of whether orders were cancelled.

#### Evidence

- `risk_engine.py:147-153`: first position fetch — `actual_qty` captured
- `risk_engine.py:163`: `if canceled_count > 0:` — gates the re-check block
- `risk_engine.py:177-180`: `placeOrder` uses `actual_qty` — may be stale if `canceled_count == 0`
- Between line 147 and line 180: qualify contract, reqPositions, cancelOrders loop — multiple `await` points where TWS state can change

#### How to verify

1. Read `risk_engine.py:163` — note the conditional `if canceled_count > 0:`
2. Read `risk_engine.py:165-175` — the position re-check and early-return guard are inside this conditional
3. Read `risk_engine.py:177-180` — note that `actual_qty` from line 151 is used when `canceled_count == 0`
4. Trace the scenario: no pending orders → canceled_count=0 → re-check skipped → stale qty used

#### Suggested fix

Remove the `if canceled_count > 0:` guard. Make the position re-check unconditional: always `await asyncio.sleep(2.0)`, always re-fetch `reqPositionsAsync()`, always verify `actual_qty > 0` before placing the MarketOrder. The 2-second delay is a small price for guaranteed position accuracy before a market order with real money consequences.

#### Confidence

- Assumptions named: that `actual_qty` can change between line 147 and line 180 when no orders exist to cancel
- How verified: TWS operates asynchronously — stop losses, manual closes, and other fills can occur at any time. The code has multiple `await` points between position fetch and MarketOrder placement
- Why 100%: the control flow is clear — when `canceled_count == 0`, the re-check block is unconditionally skipped

#### Gate evidence

- IMPACT_CATEGORY: REVENUE_LEAK — unintended naked position with real financial liability
- IS_LIVE_SURFACE: kill switch is triggered from multiple live paths
- NO_SCENARIO_MITIGATION: position re-check is gated on `canceled_count > 0`, leaving a gap when no orders are cancelled
- CTO_TEST: a kill switch that accidentally opens a naked position is a worst-case risk control failure

#### Labels

`kill-switch`, `stale-data`, `naked-position`, `conditional-recheck`

---

### Finding 4: Night watchman stop-breacheven protection permanently disabled for re-entered symbols

- **Severity**: HIGH
- **Impact category**: REVENUE_LEAK
- **Location**: `risk_engine.py:213-215`, `main.py:142`
- **Trigger condition**: Symbol is partially closed (night watchman fires), then fully closed, then re-entered with a new position in the same process session
- **Consequence**: On second partial close, night watchman does NOT push remaining stop to breakeven. Position continues running fully exposed to loss.
- **Verdict**: CONFIRMED

#### What happens

1. Trader has AAPL position. Partial take-profit fills → `night_watchman_on_tp` pushes remaining stop to breakeven → adds "AAPL" to `_nightwatchman_done` set → `risk_engine.py:213-215`
2. Trader fully closes AAPL. `_nightwatchman_done` still contains "AAPL" — never cleared.
3. Trader re-enters AAPL days later (same process session). Opens new position with stop below entry. Partial take-profit fills again.
4. `night_watchman_on_tp` fires → guard check at line 213 finds "AAPL" in set → immediately returns
5. Remaining position's stop stays below entry — fully exposed, no breakeven protection

#### Root cause

`_nightwatchman_done` was designed as per-position dedup but implemented as a permanent exclusion set. It is only `add()`-ed to (line 215), never `discard()`-ed or cleared. The set lifecycle is tied to the process, not the position lifecycle.

#### Evidence

- `risk_engine.py:213-215`: guard `if symbol in self.ctx._nightwatchman_done: return` + `self.ctx._nightwatchman_done.add(symbol)` — add only, no remove
- `main.py:142`: `self._nightwatchman_done: set[str] = set()` — initialization only
- Grep for `_nightwatchman_done` across entire codebase: only reads and `add()`, zero `discard()` or `clear()` calls

#### Suggested fix

Discard from `_nightwatchman_done` when a position is fully closed (`_async_on_execution` CLOSE path, when `remaining_exit_qty <= EPS` and no OPEN tranches remain). Also discard when a NEW position is opened for the symbol (in the OPEN path) to re-arm protection.

#### Confidence

100% — the set is never cleared anywhere in the codebase. Single-line verification: `rg "nightwatchman_done"` returns only `add()` and reads.

#### Gate evidence

- IMPACT_CATEGORY: REVENUE_LEAK — unprotected position after re-entry means real money exposed to loss
- IS_LIVE_SURFACE: night watchman triggers on every partial take-profit fill
- NO_SCENARIO_MITIGATION: `_nightwatchman_done` has no cleanup mechanism
- CTO_TEST: silently disabling a core risk protection without the user knowing is a high-priority fix

#### Labels

`night-watchman`, `state-leak`, `risk-bypass`, `set-never-cleared`

---

### Finding 5: LIKE wildcard in page_id propagation matches wrong trade_ids, corrupting Notion pages

- **Severity**: HIGH
- **Impact category**: DATA_LOSS
- **Location**: `outbound_queue.py:151-157`
- **Trigger condition**: Two trades have trade_ids where one is a numeric prefix of the other (e.g., trade_id=1 and trade_id=10)
- **Consequence**: Notion page_id from trade_id=1 is propagated to trade_id=10. When trade_id=10 later closes, its CLOSE event calls `pages.update` on trade_id=1's page, silently overwriting the wrong trade's data.
- **Verdict**: CONFIRMED

#### What happens

1. Outbound worker delivers Notion OPEN for trade_id=1 → gets back page_id → reverse propagation runs
2. SQL: `UPDATE outbound_queue SET notion_page_id=? WHERE payload_json LIKE '%"trade_id": 1%' AND channel='notion' AND notion_page_id IS NULL`
3. `%"trade_id": 1%` matches trade_id=1, 10, 11, 12, ..., 19, 100, 101, ..., 1999 (any JSON containing "trade_id": 1 as substring)
4. trade_id=10's row gets trade_id=1's Notion page_id
5. When trade_id=10 is later closed, `_send_notion` calls `pages.update(page_id=trade_id_1_page, ...)` — overwrites trade_id=1's Notion page with trade_id=10's CLOSE data

#### Root cause

The LIKE pattern uses substring matching on JSON without boundary anchoring. `"trade_id": 1` is a prefix of `"trade_id": 10`. Correct anchoring would match `"trade_id": 1,` or `"trade_id": 1}`.

#### Evidence

- `outbound_queue.py:151`: `f'%"trade_id": {tid}%'` — no boundary anchor
- `outbound_queue.py:148-149`: UPDATE applies to ALL rows matching the LIKE pattern
- `outbound_queue.py:295-296`: `pages.update(page_id=existing_page_id, ...)` — uses potentially wrong page_id

#### Suggested fix

Use SQLite JSON functions: `UPDATE outbound_queue SET notion_page_id=? WHERE json_extract(payload_json, '$.trade_id') = ?` instead of LIKE. Or anchor the pattern: `f'%"trade_id": {tid},%'` and `f'%"trade_id": {tid}}%'` to cover both JSON positions.

#### Confidence

100% — LIKE with unanchored numeric substring in JSON is deterministically wrong for trade_ids sharing numeric prefixes. With 50+ trades, collisions are nearly certain.

#### Gate evidence

- IMPACT_CATEGORY: DATA_LOSS — cross-contamination of Notion trade journal entries
- IS_LIVE_SURFACE: page_id propagation runs on every successful Notion OPEN delivery
- NO_SCENARIO_MITIGATION: no boundary check on the LIKE pattern
- CTO_TEST: permanently corrupting the trading journal with wrong cross-trade data is unrecoverable

#### Labels

`LIKE-wildcard`, `data-corruption`, `notion`, `cross-contamination`

---

### Finding 6: Flex settlement leaves positions OPEN after writing close data, enabling cascading corruption

- **Severity**: HIGH
- **Impact category**: DATA_LOSS
- **Location**: `flex_settlement.py:217-221`, `reconciliation.py:118-125`
- **Trigger condition**: Flex settlement and reconciliation both fire at the same minute (09:00, 09:30, 16:00, 16:30 ET)
- **Consequence**: Flex writes real P&L data but leaves status='OPEN'. Reconciliation sees position as OPEN with no TWS counterpart → classifies as ghost → overwrites P&L with exit_price=0, realized_pnl=0. Real P&L permanently destroyed.
- **Verdict**: CONFIRMED

#### What happens

1. Position closed in TWS between Flex runs. Flex at 09:00 ET processes the close: `UPDATE shadow_ledger SET exit_price=125.30, realized_pnl=+347.50 WHERE id=?` — but does NOT set `status='CLOSED'` → `flex_settlement.py:217-221`
2. Reconciliation fires at same minute (every 15min, including 09:00). Queries `WHERE status='OPEN'` — finds position still OPEN.
3. Compares with TWS: position no longer exists → `ledger_overstates = True` → calls `_close_ledger_discrepancy`
4. `_close_ledger_discrepancy` writes: `status='CLOSED', exit_price=0, realized_pnl=0` → `reconciliation.py:46`
5. Flex's real P&L (+$347.50) is gone. Notion journal shows $0 P&L.

#### Root cause

Flex settlement intentionally does not set `status='CLOSED'` — it only updates CLOSE fields (exit_price, realized_pnl) while leaving status as OPEN. Reconciliation's ghost detection treats any OPEN position with a TWS mismatch as a ghost and zeroes out all close data. The two systems operate on the same rows without coordination.

#### Evidence

- `flex_settlement.py:217-221`: Flex UPDATE — `SET exit_price=?, realized_pnl=?` — no `status='CLOSED'`
- `reconciliation.py:46`: reconciliation UPDATE — `SET status='CLOSED', exit_price=0, realized_pnl=0`
- `main.py:217-234`: both scheduled at overlapping minutes
- `reconciliation.py:90-92`: ghost detection reads `WHERE status='OPEN'` — Flex-updated rows are included

#### Suggested fix

Flex settlement should set `status='FLEX_CLOSED'` alongside `exit_price` and `realized_pnl`. Reconciliation should skip rows with `status='FLEX_CLOSED'` in its ghost detection. As a defense-in-depth measure, `_close_ledger_discrepancy` should check if `exit_price` is already non-zero before overwriting.

#### Confidence

100% — both code paths are deterministic. The scheduler overlap at :00 and :30 minutes is guaranteed by the APScheduler configuration.

#### Gate evidence

- IMPACT_CATEGORY: DATA_LOSS — real P&L data permanently overwritten with zeros
- IS_LIVE_SURFACE: both Flex and reconciliation run on production schedule
- NO_SCENARIO_MITIGATION: no status transition in Flex, no overlap guard between the two jobs
- CTO_TEST: systematically destroying trade P&L data makes the trading journal unreliable

#### Labels

`scheduler-overlap`, `flex-settlement`, `reconciliation`, `status-omission`, `pnl-loss`

---

### Finding 7: Partial fill realized P&L never recorded — permanently lost from trade journal

- **Severity**: HIGH
- **Impact category**: DATA_LOSS
- **Location**: `ib_listener.py:399-433`
- **Trigger condition**: Any partial close (trim) where `t_qty > remaining_exit_qty` — i.e., selling fewer shares than a tranche holds
- **Consequence**: The realized P&L for the trimmed portion is never calculated or stored. When the remaining position is later fully closed, P&L is computed only on the residual quantity, losing the partial close P&L permanently. Notion journal shows incorrect P&L.
- **Verdict**: CONFIRMED

#### What happens

1. User opens 100 shares of AAPL at $100 (T1: qty=100, entry=100)
2. User sells 50 shares at $110 — partial fill. Handler enters partial close branch at line 399.
3. `new_qty = t_qty - remaining_exit_qty = 50`. Quantity updated to 50 at line 408-410 (profitable) or 430-432 (loss).
4. **No P&L is computed or stored.** No `realized_pnl` field is written. No Notion CLOSE event is enqueued with P&L.
5. User later sells remaining 50 shares at $95 — full close. P&L computed at lines 372-373 on remaining 50 shares: (95-100)*50 = -$250
6. Total actual P&L: (110-100)*50 + (95-100)*50 = +$500 - $250 = +$250. Recorded P&L: -$250. Journal is wrong.

#### Root cause

The partial close branch (lines 399-433) handles quantity reduction but omits P&L calculation entirely. Compare with the full close branch (lines 356-397) which calculates and stores `actual_pnl` at lines 372-373 and writes `realized_pnl` at line 379.

#### Evidence

- `ib_listener.py:399-433`: entire partial close branch — zero P&L calculation code
- `ib_listener.py:372-373`: full close branch — calculates `actual_pnl` (present in full close, absent in partial close)
- `ib_listener.py:378-380`: full close branch — writes `realized_pnl` (absent in partial close)

#### Suggested fix

In the partial close branch, compute P&L for the trimmed portion: `trimmed_pnl = (price - t_entry) * remaining_exit_qty` for LONG, `(t_entry - price) * remaining_exit_qty` for SHORT. Write this to a PnL field. Consider creating a separate shadow_ledger row for the trimmed portion (status='CLOSED' with exit_price and realized_pnl) rather than silently reducing quantity.

#### Confidence

100% — the partial close code path contains zero P&L calculation statements.

#### Gate evidence

- IMPACT_CATEGORY: DATA_LOSS — realized P&L for every partial close is permanently lost
- IS_LIVE_SURFACE: partial closes occur whenever a user scales out of a position (common trading practice)
- NO_SCENARIO_MITIGATION: no separate P&L tracking for partial closes; reconciliation only fixes ghost positions, not partials
- CTO_TEST: systematically incorrect P&L data makes the trading journal unreliable for performance analysis

#### Labels

`partial-fill`, `pnl-loss`, `FIFO`, `journal-integrity`

---

### Finding 8: Flex FIFO closes only one tranche per symbol via LIMIT 1

- **Severity**: MEDIUM
- **Impact category**: DATA_LOSS
- **Location**: `flex_settlement.py:197`
- **Trigger condition**: A symbol has 2+ OPEN tranches (e.g., T1: 100 shares, T2: 100 shares) and the entire position is closed in one trade
- **Consequence**: Only T1 gets P&L and exit_price. T2 remains OPEN forever with no mechanism to receive close data. Eventually reconciliation ghost-detects T2 and writes exit_price=0, realized_pnl=0 — permanently losing T2's P&L.
- **Verdict**: CONFIRMED

#### Root cause

`flex_settlement.py:197`: `SELECT ... WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC LIMIT 1` — only one tranche per Flex trade. The trade's quantity is also never extracted or used for partial matching. Multi-tranche positions are not handled.

#### Evidence

- `flex_settlement.py:197`: `LIMIT 1` in the FIFO query
- `flex_settlement.py:174-221`: no trade quantity extraction, no multi-tranche iteration

#### Suggested fix

Iterate all OPEN tranches for the symbol, distributing the trade's P&L proportionally by quantity, or use the trade's quantity field to match against tranche quantities FIFO-style.

#### Labels

`flex-settlement`, `LIMIT-1`, `multi-tranche`, `pnl-loss`

---

### Finding 9: Stop sync applies single TWS stop price to all tranches of a symbol

- **Severity**: MEDIUM
- **Impact category**: DATA_LOSS
- **Location**: `reconciliation.py:230-239`
- **Trigger condition**: Symbol has multiple OPEN tranches with different entry prices, and a single TWS stop order exists for the aggregate position
- **Consequence**: All tranches get the same stop price regardless of their individual entry. Tranche-level risk calculations become inaccurate. If T1 entered at $100 with a $95 stop and T2 entered at $110 with a $105 stop, both get overwritten to the single TWS stop value.
- **Verdict**: CONFIRMED

#### Root cause

`reconciliation.py:230`: inner loop applies the same `stop_price` to all `open_rows` for the symbol. TWS API exposes stop orders at the contract level (one per symbol), not per-tranche. The code doesn't attempt to partition the stop across tranches.

#### Suggested fix

When multiple tranches exist, distribute the TWS stop proportionally or apply it only to the oldest tranche. At minimum, log a warning when overwriting different stop values across tranches.

#### Labels

`stop-sync`, `multi-tranche`, `reconciliation`

---

### Finding 10: Kill switch closes produce no Notion CLOSE events

- **Severity**: MEDIUM
- **Impact category**: DATA_LOSS
- **Location**: `ib_listener.py:213`
- **Trigger condition**: Any kill switch execution that successfully closes positions
- **Consequence**: Shadow ledger is correctly updated to CLOSED, but no Notion CLOSE event is enqueued. The trade silently disappears from the Notion journal with no CLOSE record, exit price, or P&L documentation.
- **Verdict**: CONFIRMED

#### Root cause

The KILL_SWITCH branch in `_async_on_execution` (lines 202-213) executes `UPDATE shadow_ledger SET status='CLOSED'` and then immediately `return`s. No `enqueue_outbound` calls exist in this path. Compare with the normal close path (lines 389-397) which enqueues both Telegram and Notion CLOSE events.

#### Suggested fix

After the DB update at line 211, add `enqueue_outbound` calls for both Telegram and Notion CLOSE events, matching the pattern in lines 389-397. Use `trade_id` from the closed position row and `fill.execution.price` as exit_price.

#### Labels

`kill-switch`, `notion`, `missing-notification`, `journal-gap`

---

### Finding 11: Notion Query-Before-Create creates duplicate pages on query failure

- **Severity**: MEDIUM
- **Impact category**: DATA_LOSS
- **Location**: `outbound_queue.py:292-293`
- **Trigger condition**: Notion databases.query times out or returns an error during the QBC dedup check
- **Consequence**: Code falls through to `pages.create`, producing a duplicate Notion page. Subsequent UPDATE events target one page, leaving the other as a stale zombie.
- **Verdict**: CONFIRMED

#### Root cause

The `except Exception` at line 292 catches ALL errors (timeout, network, rate-limit) and silently continues with `existing_page_id = ""`. This makes `pages.create` at line 299 execute as if no page was found. The QBC pattern should only proceed to create when the query explicitly returns 0 results, not when it fails.

#### Suggested fix

On query exception, re-raise to trigger outbound_queue's retry logic. Only create when the query explicitly returns empty results.

#### Labels

`notion`, `duplicate-detection`, `QBC`, `error-swallowing`

---

