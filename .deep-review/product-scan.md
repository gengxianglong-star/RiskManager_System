# Product Scan Findings

## Phase Metadata
- Lenses run: 3 (Error Paths, State Machine Completeness, Data Integrity and Display Truth)
- Features/pages covered: All Telegram commands + automated notifications
- Raw findings before synthesis: 5
- Final findings: 3

## Findings

### Finding 1: Confirming a planned trade minutes later silently uses stale entry price

**WHAT THE USER SEES:**
1. User sends `/init AAPL 145.00 BREAKOUT` — system fetches real-time price (e.g., $150.23) and presents a beautiful confirmation card with risk details
2. User gets distracted, comes back 3 minutes later, clicks "🟩 确认无误 (入账)"
3. System writes the position to shadow_ledger using the entry price from 3 minutes ago — but AAPL has moved to $152.10
4. When user executes the trade in TWS at $152.10, the TWS execution listener creates a SEPARATE shadow_ledger entry (or merges with same-day logic)
5. User runs `/status` and sees TWO entries for AAPL — the /init one at $150.23 (which never actually traded) and the TWS fill one at $152.10 (or a merged entry with confusing average price)
6. User's trade journal in Notion shows either a phantom position or a wrong entry price

**ROOT CAUSE:** `telegram_router.py:534-538` — The entry_price captured at `/init` time is saved to `pending_intents`. When the user clicks CONFIRM at line 803, `insert_shadow_ledger` uses the stale `pending["entry_price"]` without re-fetching. The two-step confirmation design (plan → confirm) has an inherent time gap where price can move, but the code treats the entry price as immutable once captured.

**EVIDENCE:**
- `telegram_router.py:450`: `entry_price, price_src = await app_context.ib_listener.fetch_entry_price(symbol)` — price captured once
- `telegram_router.py:534-538`: `save_pending_intent(conn, intent_id, symbol, stop_price, setup_tag, entry_price, quantity, regime_context)` — stale price persisted
- `telegram_router.py:803-811`: `insert_shadow_ledger(conn, pending["symbol"], float(pending["stop_price"]), pending["setup_tag"], float(pending["entry_price"]), ...)` — no re-fetch
- `ib_listener.py:227-348` (OPEN path): TWS execution creates independent entry — diverges from /init entry

**IMPACT_CATEGORY:** SUPPORT_BURDEN

**IMPACT_FLAVOR:** misleading — The user trusts the system's entry price but it's stale. Creates confusion when TWS fill creates a separate/different entry. Risk calculations based on the stale price are wrong.

**FIX:** On CONFIRM callback, re-fetch the current price and compare with pending entry_price. If the difference exceeds a threshold (e.g., 1%), warn the user: "⚠️ 价格已从 ${old} 变动至 ${new} ({pct}%)。建议重新 /init。" In all cases, display the re-fetched current price alongside the originally planned price so the user is aware. Alternatively, `/init` entries should NOT automatically create shadow_ledger records — they should stay as "planned" entries that get matched to actual TWS fills by the execution listener.

**SEVERITY:** MEDIUM — Confusing but not money-losing directly. The actual TWS fill creates its own correct entry. The stale /init entry conflicts with it. User can manually reconcile but the journal has noise.

**DISCOVERED VIA:** Lens C (Data Integrity and Display Truth) — /init → CONFIRM flow

**LABELS:** ["stale-data", "two-step-confirmation", "ux-confusion"]

---

### Finding 2: Debounced trade notifications bypass outbound queue, creating inconsistent delivery guarantees

**WHAT THE USER SEES:**
1. User executes a trade in TWS — execution listener fires
2. User receives a Telegram notification immediately via `debounced_notify` (direct `bot.send_message`, bypassing outbound queue)
3. Moments later, the outbound worker processes the same event from outbound_queue and sends a SECOND notification
4. On a network hiccup: `debounced_notify` sends successfully but silently fails 3s later, while the outbound_queue notification gets stuck behind Notion items and arrives minutes late
5. Or vice versa: debounced send fails (network down), outbound_queue message eventually delivers — but the messages are different because one went through the debounce merge and the other is a single-event message

**ROOT CAUSE:** `main.py:163-189` (`debounced_notify`) sends directly to Telegram via `self.bot.send_message` — completely bypassing the outbound queue (`outbound_queue.py`). Meanwhile, `ib_listener.py:312,336,388,422` enqueues separate Telegram notifications via `enqueue_outbound`. This creates TWO independent delivery paths for the same trade event, with different merging logic (debounce merges same-symbol messages within 3s; outbound queue sends individual messages) and different failure handling (debounce silently drops on exception; outbound queue retries with backoff).

**EVIDENCE:**
- `main.py:184-188`: `await self.bot.send_message(chat_id=MY_TELEGRAM_CHAT_ID, text=text)` — direct send, no outbound queue
- `ib_listener.py:312`: `await enqueue_outbound(f"{same_day_row['id']}-ADD", "telegram", {"message": msg})` — outbound queue path for SAME event
- Line 184: `except Exception: pass` — debounced send silently swallows all errors
- `outbound_queue.py:250-255`: `_send_telegram` → `app.gateway.notify_user(msg)` — gateway has its own 3-retry logic

**IMPACT_CATEGORY:** SUPPORT_BURDEN

**IMPACT_FLAVOR:** confusing — User may receive 0, 1, or 2 notifications for the same trade depending on timing and network conditions. The notification content may differ (debounce-merged vs individual). During network issues, notifications may arrive out of order.

**FIX:** Remove direct `bot.send_message` from `debounced_notify`. Instead, enqueue the merged message through `enqueue_outbound` with a unique event_key (e.g., `f"{symbol}-DEBOUNCED-{timestamp}"`). This ensures ALL trade notifications flow through the outbound queue, providing consistent delivery guarantees (retry, backoff, dead letter) and eliminating duplicates.

**SEVERITY:** LOW — Annoying but not harmful. The user still gets notified (usually). The duplicate delivery is the main issue.

**DISCOVERED VIA:** Lens C (Data Integrity and Display Truth) — notification delivery paths

**LABELS:** ["dual-delivery-path", "notification-duplicate", "bypass"]

---

### Finding 3: Pending trade intents never expire, creating zombie entries that can be confirmed days later

**WHAT THE USER SEES:**
1. User does `/init AAPL 145.00 BREAKOUT` on Monday, sees the confirmation card
2. Gets distracted, never clicks CONFIRM or CANCEL. Closes Telegram.
3. On Friday, scrolls back through chat history, finds the old confirmation card, clicks "🟩 确认无误 (入账)"
4. The CONFIRM callback finds the intent still exists in `pending_intents` → re-validates risk (equity may have changed) → inserts into shadow_ledger
5. The user now has a position in shadow_ledger at Monday's price with Monday's market regime context — but it's Friday, the market has moved, and the position was never actually traded
6. `/status` shows this phantom position. Risk calculations are skewed. P&L tracking is corrupted.

**ROOT CAUSE:** `telegram_router.py:534-538` — `save_pending_intent` inserts into `pending_intents` with a `create_time` field. But nothing ever cleans up old intents. No TTL, no expiry check in `load_pending_intent`, no cleanup job. The `create_time` field exists in the schema (`database.py:87`) but is never used for expiry logic.

**EVIDENCE:**
- `database.py:87`: `create_time DATETIME DEFAULT CURRENT_TIMESTAMP` — exists but unused for expiry
- `telegram_router.py:782-785`: `load_pending_intent(conn, intent_id)` — `pending is None` check only, no age check
- `telegram_router.py:764-767`: CANCEL path — `delete_pending_intent` removes only when user explicitly cancels
- No cleanup job anywhere in APScheduler or daemons for old `pending_intents`

**IMPACT_CATEGORY:** SUPPORT_BURDEN

**IMPACT_FLAVOR:** misleading — User unintentionally creates phantom positions with stale data. Skews risk calculations and trade journal accuracy. Recovery requires manual DB surgery or /override to close the phantom.

**FIX:** Add expiry to pending intents: (1) In `load_pending_intent`, check `create_time` and reject intents older than 30 minutes. (2) Add a periodic cleanup job (daily) to DELETE FROM `pending_intents` WHERE `create_time < datetime('now', '-1 day')`. (3) When rejecting an expired intent in CONFIRM, show a clear message: "⚠️ 此建仓意图已于 {time} 过期。价格已变动，请重新 /init。"

**SEVERITY:** LOW — Requires specific user behavior (revisiting old chat messages). But when it happens, the phantom position corrupts the journal until manually found and fixed.

**DISCOVERED VIA:** Lens B (State Machine Completeness) — pending_intents lifecycle

**LABELS:** ["state-machine", "zombie-data", "intent-expiry"]
