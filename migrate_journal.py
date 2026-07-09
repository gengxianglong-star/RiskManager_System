"""一次性数据迁移：交易日志标准升级。

对现有数据无损迁移：
- 扩展 shadow_ledger 列（ensure_schema 自动处理）
- 拆分 setup_tag → trade_tags
- 回填 r_multiple、holding_days、exec_time
"""
import asyncio
import aiosqlite
from datetime import datetime, timezone


async def migrate():
    async with aiosqlite.connect("risk_manager.db") as db:
        db.row_factory = aiosqlite.Row

        # Phase 1: ensure schema (ALTER + new tables)
        from database import ensure_schema
        await ensure_schema()
        print("[OK] Phase 1: schema upgraded")

        # Phase 2: split setup_tag -> trade_tags
        cur = await db.execute(
            "SELECT id, setup_tag FROM shadow_ledger "
            "WHERE setup_tag IS NOT NULL AND setup_tag != ''"
        )
        rows = await cur.fetchall()
        tag_count = 0
        for row in rows:
            tags = [t.strip() for t in row["setup_tag"].split(",") if t.strip()]
            for tag in tags:
                await db.execute(
                    "INSERT OR IGNORE INTO trade_tags (ledger_id, tag) VALUES (?, ?)",
                    (row["id"], tag),
                )
                tag_count += 1
        print(f"[OK] Phase 2: {tag_count} tags split ({len(rows)} trades)")

        # Phase 3: backfill exec_time <- create_time
        await db.execute(
            "UPDATE shadow_ledger SET exec_time = create_time "
            "WHERE exec_time IS NULL OR exec_time = ''"
        )
        print("[OK] Phase 3: exec_time backfilled")

        # Phase 4: backfill r_multiple & holding_days (CLOSED only)
        cur = await db.execute(
            "SELECT id, entry_price, initial_stop, quantity, "
            "realized_pnl, create_time FROM shadow_ledger WHERE status='CLOSED'"
        )
        closed = await cur.fetchall()
        updated = 0
        for row in closed:
            entry = float(row["entry_price"])
            stop = float(row["initial_stop"])
            qty = float(row["quantity"])
            pnl = float(row["realized_pnl"] or 0)

            risk = abs(entry - stop) * qty if stop > 0 else 0.0
            r_mult = round(pnl / risk, 2) if risk > 0 else 0.0

            now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            create_day = row["create_time"][:10] if row["create_time"] else now
            try:
                d1 = datetime.strptime(create_day, "%Y-%m-%d")
                d2 = datetime.strptime(now, "%Y-%m-%d")
                days = max(1, (d2 - d1).days)
            except Exception:
                days = 1

            await db.execute(
                "UPDATE shadow_ledger SET r_multiple=?, risk_amount=?, "
                "holding_days=? WHERE id=?",
                (r_mult, risk, days, row["id"]),
            )
            updated += 1

        await db.commit()
        print(f"[OK] Phase 4: {updated} closed trades backfilled")

        # Final stats
        cur = await db.execute("SELECT COUNT(*) FROM shadow_ledger")
        total = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM trade_tags")
        tags = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM stop_adjustments")
        stops = (await cur.fetchone())[0]
        print(f"\n[DONE] {total} trades, {tags} tags, {stops} stop adjustments")


if __name__ == "__main__":
    asyncio.run(migrate())
