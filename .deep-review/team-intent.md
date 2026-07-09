# Team Intent Brief

## Summary

A single developer building a personal trading risk management system in rapid iterative cycles. The dominant pain signal across 19 commits is **concurrency and data integrity** — races between TWS event callbacks and shadow ledger updates, stop-loss synchronization gaps, and transaction-boundary issues. The fix pattern is consistently "bundle multiple fixes + new features in each commit," indicating a firefighting-feature-build hybrid: the developer is actively extending functionality while simultaneously hardening the core trade-capture path. No reverts, no walkbacks — the developer commits forward rather than rolling back.

## Bug-class mix (last 2 months)

| Class | % |
|---|---:|
| concurrency-data-integrity | 40 |
| api-contract | 30 |
| ux-flow | 20 |
| performance-reliability | 10 |

## Team focus

- Shadow ledger ↔ TWS consistency: commits repeatedly address desync between physical positions and the tracked shadow ledger, especially during concurrent fills (bracket orders, partial fills, same-symbol rapid trades).
- Kill switch execution reliability: the automated position liquidation path has seen multiple fix passes — wrong API call, race window between cancel and re-check, and concurrent execution protection.
- Stop-loss synchronization between TWS charts and the shadow ledger: bracket stop capture timing, open order event handling, and stop direction validation all received fix attention.
- Timezone and day-boundary correctness: daily trade counts, same-day position aggregation, and consecutive-loss resets all depend on correct timezone alignment — a recurring fix surface.
- Outbound notification reliability: the most substantial fix commit was a complete rewrite of the notification pipeline into an outbox pattern with circuit breaker, throttling, and dead-letter queue, indicating prior silent notification loss.

## Mode

`feature-build` — new features (corporate action automation, market regime classification, 3R radar, EOD sniper, process-to-process communication table) dominate the commit history. However, many feature commits also bundle correctness fixes (concurrency, API contract drift, timezone bugs), suggesting the developer discovers and patches bugs during feature development rather than in dedicated fix passes.

## Confidence

`low` — only 19 commits total, and the project is ~1 month old (all files added within the last month). Fix patterns are visible but the sample size is too small for high-confidence clustering. The brief should be treated as suggestive, not directive. The action-trace phase should explore broadly rather than specializing heavily.

## Trust-critical surface categories

- **Trade execution capture** — every TWS fill must produce exactly one shadow ledger entry and notification. This is the primary invariant the developer keeps reinforcing with locks, dedup checks, and delay-based workarounds. Any gap here produces wrong P&L or missed positions.
- **Kill switch execution** — automated market orders with real money consequences. Must cancel orders, verify positions, and place exactly one market order. No double-execution, no missed close. The developer has fixed the API call, the concurrent-execution guard, and the cancel-verify-place sequencing.
- **Stop-loss synchronization** — bidirectional sync between TWS physical stop orders and the shadow ledger. Misalignment means the risk engine calculates wrong exposure, and the EOD sniper may liquidate positions that are actually protected (or fail to liquidate unprotected ones).
- **Transaction boundaries across multiple connect_db() calls** — the codebase opens separate database connections within a single logical action (e.g., trade execution spans 2–3 connect_db blocks). A crash mid-action leaves partial writes. This is a structural concern the developer has patched around with dedup and idempotency but not fundamentally addressed.
- **Timezone and cross-day boundary handling** — daily trade limits, same-day position aggregation, consecutive-loss reset, and corporate action date filtering all depend on correct timezone computation. Wrong timezone = wrong risk light = wrong trade authorization.
