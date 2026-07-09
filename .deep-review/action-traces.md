# Action Trace Findings

## Phase Metadata
- Actions traced: 8
- Actions with bugs found: 5
- Cross-action synthesis passes: 1
- Raw findings before consolidation: 9
- Final findings: 7

## Findings

### Finding 1: Telegram notifications silently lost despite outbound queue confirming delivery

**WHAT HAPPENS:**
1. System enqueues a critical message (trade fill, kill switch execution, stop sync) via `enqueue_outbound(event_key, "telegram", payload)` → `outbound_queue.py:75`
2. `outbound_worker` picks up the message → `_send_telegram` calls `app.gateway.notify_user(msg)` → `outbound_queue.py:255`
3. `notify_user` has its own 3 internal retries. On `RetryAfter` (429 rate limit) exhaustion: silently logs "Telegram 连续限流 3 次，消息丢弃。" and **returns normally** (no exception) → `gateway.py:70-71`
4. On `TimedOut`/`NetworkError` exhaustion after 3 retries: queues to `_msg_queue` (in-memory list, **lost on process restart**) and returns normally → `gateway.py:78-84`
5. On all other exceptions: queues to `_msg_queue` and returns normally → `gateway.py:85-91`
6. Back in `outbound_queue.py`, `_send_telegram` sees no exception → `outbound_worker` marks the row as `status='sent'` → `outbound_queue.py:160-165`
7. **The message was never delivered and the outbound queue thinks it was.** The user never sees the notification. For a kill switch execution notification, the user may not know their position was liquidated.

**IMPACT_CATEGORY:** DATA_LOSS

**BLAST RADIUS:** Every Telegram notification sent through the outbound queue during 429 rate limiting, network outage lasting >3 retries (~14 seconds), or any non-retryable exception. The in-memory `_msg_queue` fallback is wiped on restart. `RetryAfter` exhaustion provides NO fallback at all — message is irrecoverably lost.

**VERIFICATION ARTIFACT:**
- `gateway.py:65-71`: `RetryAfter` handler returns normally after logging (no exception, no queue)
- `gateway.py:72-84`: `TimedOut`/`NetworkError` handler returns normally after queuing to in-memory list
- `outbound_queue.py:250-255`: `_send_telegram` calls `notify_user` and assumes success if no exception
- `outbound_queue.py:160-165`: marks row `status='sent'` on apparent success

**HOW TO VERIFY:**
1. Configure the system with a valid Telegram bot token
2. Trigger a rate-limit scenario (send 30+ messages/second to hit Telegram's 429 limit)
3. Observe that outbound_queue rows transition to `status='sent'` but messages are never received
4. Restart the process during a simulated network outage — check that `_msg_queue` contents are gone

**WHY:** `gateway.py:notify_user` is designed as a "fire and forget" method — it never raises exceptions to its caller. But `outbound_queue.py:_send_telegram` treats the absence of an exception as confirmation of delivery. The two layers have incompatible error semantics: the gateway silently drops after internal retries, but the outbound queue treats silence as success.

**FIX:** In `gateway.py:notify_user`, when internal retries are exhausted for `RetryAfter`, `TimedOut`, or `NetworkError`, **raise an exception** instead of returning silently. This lets `_send_telegram` catch the exception and trigger outbound_queue retry logic (exponential backoff, dead letter). Alternatively, after exhaustion, enqueue back to outbound_queue with a delay rather than dropping.

**SEVERITY:** HIGH — The outbound queue's entire purpose is reliable delivery. This bug defeats that purpose for the Telegram channel. Critical notifications (kill switch, stop sync, trade fills) can be silently lost without any persistent record of the failure.

**DISCOVERED VIA:** Action trace: "Outbound queue worker"

**LABELS:** ["double-retry", "silent-failure", "notification-loss"]

---

### Finding 2: Kill switch double-execution guard expires while MarketOrder may still be pending

**WHAT HAPPENS:**
1. User triggers kill switch for symbol AAPL → `risk_engine.py:131`
2. `killing_symbols.add("AAPL")` prevents concurrent execution → `risk_engine.py:137`
3. Cancel orders, check positions, place MarketOrder → `risk_engine.py:177-180`
4. `loop.call_later(10.0, self.ctx.killing_symbols.discard, "AAPL")` schedules auto-discard → `risk_engine.py:195`
5. After 10 seconds, `killing_symbols` no longer contains "AAPL"
6. If the MarketOrder hasn't filled yet (illiquid stock, after-hours, TWS queue delay), a second kill switch trigger for AAPL (from any source: duplicate /unlock, EOD sniper, duplicate button click) passes the guard
7. Second kill switch executes: cancels the still-pending first MarketOrder, re-checks positions, places a NEW MarketOrder
8. If the first order partially filled between the cancel and the re-check, the position calculation is now wrong — the second MarketOrder may be for the wrong size, potentially creating a naked position in the opposite direction

**IMPACT_CATEGORY:** REVENUE_LEAK

**BLAST RADIUS:** Any kill switch execution where the MarketOrder takes >10 seconds to fill. More likely with: illiquid stocks, large position sizes, after-hours trading, or TWS connection latency. The EOD sniper is a particularly risky trigger — it fires automatically at 15:55 ET and could collide with a manual kill switch executed moments before.

**VERIFICATION ARTIFACT:**
- `risk_engine.py:134-135`: guard check against `killing_symbols`
- `risk_engine.py:137`: `killing_symbols.add(symbol)`
- `risk_engine.py:195`: `loop.call_later(10.0, self.ctx.killing_symbols.discard, symbol)` — auto-discard
- `risk_engine.py:157-162`: cancelOrders loop — cancels ALL pending orders including any prior kill switch MarketOrder
- `risk_engine.py:163-175`: position re-check after cancel — if partial fill occurred, `actual_qty` is stale

**HOW TO VERIFY:**
1. Place a limit order for an illiquid stock far from market (to ensure it doesn't fill quickly)
2. Execute kill switch — this sends a MarketOrder
3. Before the MarketOrder fills, trigger kill switch again (after 10+ seconds)
4. Observe: the guard check passes, cancelOrders fires, position re-check may see a different quantity than expected

**WHY:** The 10-second timer is a timeout-based assumption that the kill switch MarketOrder will fill quickly. In fast-moving or illiquid markets, this assumption breaks. The guard should be based on the lifecycle of the kill switch (order filled or explicitly cancelled), not an arbitrary timer. `killing_symbols` should only be cleared when: (a) the execution callback confirms the KILL_SWITCH fill, or (b) the order is confirmed cancelled without any fills.

**FIX:** Replace `loop.call_later(10.0, ...)` with event-driven cleanup in `_async_on_execution` when `setup_tag == "KILL_SWITCH"`: discard `killing_symbols` only after confirming the fill processed. Additionally, check `killing_symbols` before placing a new MarketOrder even if the guard passes — verify no pending KILL_SWITCH-tagged orders exist in `ib.trades()`.

**SEVERITY:** HIGH — The kill switch is the last-resort risk control. A failure in its execution guard could create unintended positions with real financial consequences. The trigger condition (illiquid stock or delayed fill) is uncommon but realistic.

**DISCOVERED VIA:** Action trace: "Kill switch execution"

**LABELS:** ["race-condition", "kill-switch", "double-execution"]

---

### Finding 3: Night watchman breach-protection permanently disabled for re-entered symbols

**WHAT HAPPENS:**
1. Trader has position in AAPL, partial take-profit (LMT) fills → `ib_listener.py:441-443` triggers `night_watchman_on_tp`
2. Night watchman pushes remaining stop to breakeven → `risk_engine.py:213-215`: adds "AAPL" to `_nightwatchman_done` set, preventing re-trigger
3. Trader later closes AAPL completely and re-enters AAPL days later with a new position and a new stop below entry
4. Partial take-profit on the NEW position triggers `night_watchman_on_tp` again
5. But `_nightwatchman_done` still contains "AAPL" — guard check at `risk_engine.py:213` immediately returns
6. The remaining position's stop is NOT pushed to breakeven → position continues running with stop below entry, fully exposed to loss

**IMPACT_CATEGORY:** REVENUE_LEAK

**BLAST RADIUS:** Any symbol that is: (1) partially closed with profit, then (2) fully closed, then (3) re-entered later. The night watchman's breakeven stop protection is permanently disabled for that symbol after the first use. The `_nightwatchman_done` set is never cleaned anywhere in the codebase — it grows monotonically and never resets.

**VERIFICATION ARTIFACT:**
- `risk_engine.py:213-215`: guard check + `add(symbol)` with no corresponding `discard(symbol)` anywhere
- `main.py:142`: `self._nightwatchman_done: set[str] = set()` — initialization only
- `ib_listener.py:441-443`: trigger site — spawns `night_watchman_on_tp` after any closing execution

**HOW TO VERIFY:**
1. Open AAPL position, set stop below entry
2. Place and fill a partial take-profit limit order → night watchman fires, stop pushed to entry
3. Close remaining AAPL position completely
4. Open new AAPL position (days later), set stop below entry
5. Place and fill a partial take-profit limit order → night watchman does NOT fire, stop stays below entry

**WHY:** `_nightwatchman_done` was designed as a per-position dedup mechanism ("同一标的只执行一次") but was implemented as a permanent exclusion list. It is never cleared when a position is fully closed or when a new position is opened. The set lifecycle should be tied to the position lifecycle, not the symbol.

**FIX:** Clear `_nightwatchman_done.discard(symbol)` when a position is fully closed (in `_async_on_execution` CLOSE path when `remaining_exit_qty <= EPS` and all tranches are closed). Also clear it when a new position is opened for the symbol (in the OPEN path) to re-arm the night watchman for the new position.

**SEVERITY:** HIGH — This silently disables a critical risk protection. The user may assume the night watchman is protecting their re-entered position, but it isn't. The consequence is an exposed position that could have been running at breakeven.

**DISCOVERED VIA:** Action trace: "TWS execution detected"

**LABELS:** ["state-leak", "night-watchman", "risk-bypass"]

---

### Finding 4: Reconciliation permanently corrupts P&L by closing ghost positions with zero values

**WHAT HAPPENS:**
1. Reconciliation runs (every 15min) and detects a "ghost": shadow_ledger expects a position but TWS shows less/none → `reconciliation.py:118-124`
2. `_close_ledger_discrepancy` is called → `reconciliation.py:26-55`
3. Ghost positions are closed with: `status='CLOSED', exit_price=0, realized_pnl=0` → `reconciliation.py:45-47`
4. The actual P&L for that trade is permanently destroyed — it's written as $0
5. Flex settlement (`flex_settlement.py:194`) searches for `WHERE symbol=? AND status='OPEN'` — but the position is now CLOSED, so Flex can never correct it
6. The user's trading journal has permanently lost the real P&L for that trade

**IMPACT_CATEGORY:** DATA_LOSS

**BLAST RADIUS:** Every ghost position detected by reconciliation. Ghost positions can occur when: TWS execution callback was missed (TWS disconnect during trade), position was closed manually in TWS without the listener detecting it, or the shadow ledger has a stale entry from a prior bug. Each ghost = one permanently corrupted P&L record.

**VERIFICATION ARTIFACT:**
- `reconciliation.py:45-47`: `UPDATE shadow_ledger SET status='CLOSED', exit_price=0, realized_pnl=0` — hardcoded zeros
- `flex_settlement.py:194`: `WHERE symbol=? AND status='OPEN'` — Flex only finds OPEN positions, cannot correct CLOSED ones
- `reconciliation.py:118-124`: ghost detection logic — triggers the zero-value close

**HOW TO VERIFY:**
1. Manually insert a position into shadow_ledger (or have one become ghost via TWS disconnect)
2. Wait for reconciliation (or trigger manually via /reconcile)
3. Observe: the position is closed with exit_price=0, realized_pnl=0
4. Check Notion: the CLOSE event (if it fires) shows $0 P&L

**WHY:** When reconciliation auto-closes ghost positions, it has no exit price information from TWS (the position no longer exists in TWS). Rather than marking the position with a sentinel value that indicates "unknown P&L" or deferring to Flex settlement for the actual P&L, it writes zero — which is indistinguishable from a genuine break-even trade.

**FIX:** Before closing a ghost with zero, attempt to look up the actual P&L from Flex settlement (which has authoritative trade data). If Flex data is unavailable, mark the position with a sentinel `exit_price=-1` and set `setup_tag` to include "GHOST" so the user can identify and manually correct these entries. Do NOT write `realized_pnl=0` — leave it NULL to indicate unknown.

**SEVERITY:** MEDIUM — Data is permanently corrupted, but the blast radius is limited to ghost positions (which should be rare if the system is working correctly). However, when a ghost does occur, the data loss is total and irreversible for that trade.

**DISCOVERED VIA:** Action trace: "Physical position reconciliation"

**LABELS:** ["data-corruption", "pnl-loss", "reconciliation"]

---

### Finding 5: Reconciliation and Flex settlement can both modify the same position simultaneously

**WHAT HAPPENS:**
1. Flex settlement runs at 09:00 ET (APScheduler cron) → processes trades from Flex report → for each closed trade, updates `shadow_ledger` with `exit_price` and `realized_pnl` (but does NOT set `status='CLOSED'`) → `flex_settlement.py:217-221`
2. Reconciliation runs at 09:00 ET (every 15min interval, fires at same minute) → scans physical positions → finds the position still `status='OPEN'` in shadow_ledger → compares with TWS (position no longer exists) → detects ghost → calls `_close_ledger_discrepancy` → sets `status='CLOSED', exit_price=0, realized_pnl=0` → `reconciliation.py:45-47`
3. Race condition: Flex writes real P&L, then reconciliation overwrites with zeros. OR: Reconciliation closes first (zero P&L), then Flex can't find the position (`status='OPEN'` filter) and the real P&L data is discarded.
4. In both orderings, the actual P&L is lost. Only the loser's write survives.

**IMPACT_CATEGORY:** DATA_LOSS

**BLAST RADIUS:** Any position that is closed between Flex settlement runs and whose trade appears in the Flex report for the first time at the same time reconciliation fires. The reconciliation interval (15min) and Flex schedule (09:00, 16:30) create collision windows at exactly 09:00, 09:15, 09:30, 09:45, 16:30, 16:45 ET.

**VERIFICATION ARTIFACT:**
- `flex_settlement.py:217-221`: Flex writes `exit_price` and `realized_pnl` but does NOT set `status='CLOSED'`
- `reconciliation.py:45-47`: Reconciliation writes `status='CLOSED', exit_price=0, realized_pnl=0`
- `reconciliation.py:90-92`: Reconciliation reads `status='OPEN'` positions — positions with Flex data but still OPEN are included
- `main.py:217-224`: Reconciliation interval: 15 minutes
- `main.py:227-234`: Flex cron: hour 9,16 minute 0,30

**HOW TO VERIFY:**
1. Close a position in TWS between 08:00-09:00 ET so it appears in the 09:00 Flex report
2. Ensure reconciliation is running (it fires every 15min, so at 09:00)
3. Observe the race: Flex writes real P&L, reconciliation writes zeros
4. Check shadow_ledger: the position has `exit_price=0, realized_pnl=0` and `status='CLOSED'`

**WHY:** Flex settlement intentionally does not set `status='CLOSED'` — it only updates CLOSE fields while leaving status as OPEN. This is because it uses FIFO accumulation and may not close the entire position. But reconciliation's ghost detection treats any `status='OPEN'` position with a TWS mismatch as a ghost, regardless of whether Flex has already processed it.

**FIX:** Flex settlement should set `status='CLOSED'` (or a new status like `FLEX_CLOSED`) when it processes a closing trade. Reconciliation should then skip positions with `status='CLOSED'` in its ghost detection. Alternatively, reconciliation should check `flex_processed_execs` before classifying a position as ghost — if Flex has already logged the close, defer to Flex data.

**SEVERITY:** MEDIUM — The race window is limited to specific minutes (09:00, 09:15, 09:30, 09:45, 16:30, 16:45 ET). But when it fires, real P&L data is permanently lost. The Flex schedule (every 30min at 9 and 16) makes this a recurring risk.

**DISCOVERED VIA:** Action trace: "Flex authoritative settlement" × "Physical position reconciliation"

**LABELS:** ["race-condition", "data-corruption", "scheduler-overlap", "pnl-loss"]

---

### Finding 6: LIKE pattern in page_id propagation matches wrong trades, silently overwriting Notion pages

**WHAT HAPPENS:**
1. Outbound worker successfully delivers a Notion OPEN event for `trade_id=1` → gets back a `notion_page_id` → `outbound_queue.py:144-157`
2. The reverse propagation code runs: `UPDATE outbound_queue SET notion_page_id=? WHERE payload_json LIKE '%"trade_id": 1%' AND channel='notion' AND notion_page_id IS NULL`
3. The LIKE pattern `%"trade_id": 1%` matches ANY row containing `"trade_id": 1` as a substring → this includes `trade_id=10`, `trade_id=11`, ..., `trade_id=19`, `trade_id=100`, `trade_id=1000`, etc.
4. The `notion_page_id` from `trade_id=1` gets written to ALL these unrelated rows
5. When those rows are later processed for their own CLOSE/UPDATE events, they call `notion.pages.update(page_id=WRONG_ID, ...)` → silently overwriting the page that belongs to `trade_id=1` with data from a completely different trade
6. The original trade's data is permanently corrupted, and the wrong trade's CLOSE data goes to the wrong page

**IMPACT_CATEGORY:** DATA_LOSS

**BLAST RADIUS:** Every Notion OPEN delivery where `trade_id` shares a numeric prefix with another trade (e.g., trade_id=1 affects trades 10-19, 100-199, 1000-1999; trade_id=10 affects trades 100-109, 1000-1099). As the trade count grows, this becomes increasingly likely. With 50+ trades, collisions are nearly certain.

**VERIFICATION ARTIFACT:**
- `outbound_queue.py:151-157`: LIKE pattern `f'%"trade_id": {tid}%'` — substring match, no boundary anchoring
- `outbound_queue.py:148-149`: `UPDATE ... WHERE payload_json LIKE ?` — applies to ALL matching rows
- `outbound_queue.py:295-296`: `notion.pages.update(page_id=existing_page_id, properties=props)` — uses the propagated (possibly wrong) page_id

**HOW TO VERIFY:**
1. Create trade_id=1 and trade_id=10 in shadow_ledger (two separate symbols)
2. Both get enqueued as OPEN events
3. outbound_worker processes trade_id=1 → gets page_id → propagation updates trade_id=10's row too
4. When trade_id=10 is later closed, the CLOSE event calls `pages.update` with trade_id=1's page_id → overwrites trade_id=1's data with trade_id=10's CLOSE data

**WHY:** The LIKE pattern uses a naive substring match on JSON without boundary anchoring. A correct pattern would anchor the match: `"trade_id": 1,` or `"trade_id": 1}` to match only the exact integer value. JSON substring matching against numeric fields without boundary anchors is inherently ambiguous.

**FIX:** Use an exact match: extract the trade_id from the payload_json more precisely. For SQLite, use `json_extract(payload_json, '$.trade_id') = ?` instead of LIKE. Or anchor the pattern with a following character: `f'%"trade_id": {tid},%'` and `f'%"trade_id": {tid}}%'` to handle both positions in JSON.

**SEVERITY:** HIGH — This silently corrupts the Notion trading journal, the system's permanent record of trades. Cross-contamination between trades makes P&L data unreliable. The corruption is silent — users won't notice until they audit individual Notion pages and find wrong data.

**DISCOVERED VIA:** Action trace: "Outbound queue worker" — page_id reverse propagation

**LABELS:** ["data-corruption", "notion", "SQL-injection-adjacent", "LIKE-wildcard"]

---

### Finding 7: Notion Query-Before-Create silently creates duplicate pages on query failure

**WHAT HAPPENS:**
1. Outbound worker processes a Notion OPEN event with no local `page_id` → `outbound_queue.py:274`
2. Calls Notion `databases.query` to check if a page with this Tranche ID already exists → `outbound_queue.py:277-285`
3. Notion API times out, network error, or returns rate-limit error → `except Exception` at line 292 catches it
4. Warning is logged but `existing_page_id` remains `""` → code falls through to `pages.create` at line 299
5. A DUPLICATE page is created in Notion, even though the original page already exists from a prior successful delivery
6. Subsequent UPDATE/CLOSE events use `existing_page_id` (which is now the original page_id from the DB row, or the new one) — one of the two pages becomes a stale zombie with outdated data

**IMPACT_CATEGORY:** DATA_LOSS

**BLAST RADIUS:** Every Notion OPEN event retry where the original page was created successfully but the page_id was lost locally (e.g., the DB write of page_id failed, or Notion created it but the response was lost). Notion's eventual consistency means the query may return 0 results even though a page was created milliseconds ago. Network flakiness amplifies this.

**VERIFICATION ARTIFACT:**
- `outbound_queue.py:292-293`: `except Exception as e:` — catches ALL errors, including timeout, rate-limit, network error
- `outbound_queue.py:299-302`: `pages.create(...)` — runs when `existing_page_id` is empty (which stays empty after query failure)
- `outbound_queue.py:296`: `pages.update(page_id=existing_page_id, ...)` — only runs when `existing_page_id` is non-empty (skipped after query failure)

**HOW TO VERIFY:**
1. Create a Notion page for a trade successfully (page_id stored in outbound_queue)
2. Delete the page_id from outbound_queue (simulating page_id loss)
3. Trigger outbound worker while Notion API is rate-limited or network is slow
4. Observe: query fails or returns 0 → new duplicate page created

**WHY:** The QBC dedup mechanism treats query failure identically to "no page found" — both leave `existing_page_id` empty. The correct behavior is: on query failure, retry the query (with backoff) rather than falling through to create. Only `pages.create` should proceed when the query explicitly returns 0 results, not when it fails.

**FIX:** On query exception, do NOT fall through to `pages.create`. Instead, re-raise the exception to trigger outbound_queue's retry logic (exponential backoff). The QBC pattern should only create when confirmed-no-page-exists, not on uncertainty.

**SEVERITY:** MEDIUM — Duplicate pages are undesirable but the primary page eventually gets updates (if page_id propagation works). The stale zombie page is confusing but not money-impacting. However, if combined with Finding 6 (LIKE wildcard bug), the duplicate page could get cross-contaminated with another trade's data.

**DISCOVERED VIA:** Action trace: "Outbound queue worker" — Query Before Create dedup

**LABELS:** ["notion", "duplicate-detection", "error-handling", "data-integrity"]
