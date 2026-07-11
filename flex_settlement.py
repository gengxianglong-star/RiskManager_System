"""
Flex 权威结算引擎 (CQRS 读取端 / Single Source of Truth)。

职责：定期拉取 IBKR Flex Query 官方报表，与本地影子账本进行权威对账，
       关闭已平仓条目，归档盈亏，同步未平仓持仓到 Notion。

两阶段处理：
  阶段 1 — 已平仓交易（PnL ≠ 0）：匹配影子账本关仓，孤儿单也写入 Notion 供复盘
  阶段 2 — 未平仓交易（PnL = 0）：对照 TWS 实时数据，更新/补录 Notion 持仓信息

内置：MD5 哈希防重、连亏/连赢动态统计、IB 官方错误码分级重试。
"""

import asyncio
import datetime
import hashlib
import re
import ssl
import urllib.request

import certifi
import pandas_market_calendars as mcal
from ib_insync import ExecutionFilter, FlexReport
from ib_insync.util import formatIBDatetime

from ai_logger import ai_trace, logger
from config import CLIENT_ID, FLEX_QUERY_IDS, FLEX_TOKEN, TWS_HOST, resolve_tws_ports
from database import connect_db, compute_close_journal
from outbound_queue import enqueue_outbound

# ── SSL 证书修复（与 main.py 一致，避免 Flex HTTPS 在部分环境失败）──
_ssl_context = ssl.create_default_context(cafile=certifi.where())
urllib.request.install_opener(
    urllib.request.build_opener(urllib.request.HTTPSHandler(context=_ssl_context))
)

# IB Flex Web Service 错误码分级（见 IBKR Campus Flex Web Service 文档）
_PERMANENT_FLEX_CODES = frozenset({1010, 1011, 1012, 1013, 1015, 1016, 1020})
_TRANSIENT_FLEX_CODES = frozenset({
    1001, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1017, 1019, 1021,
})
_RATE_LIMIT_CODE = 1018
_FLEX_RETRY_DELAYS = (8, 12, 20, 30, 45, 60)
_FLEX_RATE_LIMIT_DELAY = 12


def _extract_flex_error_code(err_msg: str) -> int | None:
    match = re.search(r"\b(10\d{2})\b", err_msg)
    return int(match.group(1)) if match else None


def _download_flex_report_sync(query_id: str) -> FlexReport:
    """同步拉取 Flex 报表（供 run_in_executor 调用）。"""
    return FlexReport(FLEX_TOKEN, query_id)


async def _download_flex_report(loop: asyncio.AbstractEventLoop, query_id: str) -> FlexReport:
    """对单个 Query ID 按 IB 官方 pacing 规则退避重试。"""
    last_err: Exception | None = None
    max_attempts = len(_FLEX_RETRY_DELAYS) + 1

    for attempt in range(max_attempts):
        try:
            return await loop.run_in_executor(
                None, _download_flex_report_sync, query_id
            )
        except Exception as exc:
            last_err = exc
            err_msg = str(exc)
            code = _extract_flex_error_code(err_msg)

            if code in _PERMANENT_FLEX_CODES:
                raise

            if attempt >= max_attempts - 1:
                break

            if code == _RATE_LIMIT_CODE:
                wait = _FLEX_RATE_LIMIT_DELAY
            elif code in _TRANSIENT_FLEX_CODES or code is None:
                wait = _FLEX_RETRY_DELAYS[min(attempt, len(_FLEX_RETRY_DELAYS) - 1)]
            else:
                wait = _FLEX_RETRY_DELAYS[min(attempt, len(_FLEX_RETRY_DELAYS) - 1)]

            logger.warning(
                f"Flex Query {query_id} 暂不可用 (code={code}): {err_msg[:80]}... "
                f"{wait}s 后重试 ({attempt + 1}/{max_attempts})"
            )
            await asyncio.sleep(wait)

    assert last_err is not None
    raise last_err


async def _fetch_flex_report(loop: asyncio.AbstractEventLoop) -> FlexReport:
    """拉取 Flex 报表；同一 Token 下优先对单个 Query 充分重试，仅在 Query 无效时切换。"""
    last_err: Exception | None = None

    for idx, qid in enumerate(FLEX_QUERY_IDS):
        try:
            report = await _download_flex_report(loop, qid)
            logger.info(f"🧾 [Flex] Query {qid} 成功拉取报表")
            return report
        except Exception as exc:
            last_err = exc
            code = _extract_flex_error_code(str(exc))
            if code in (1012, 1013, 1015, 1011, 1010):
                raise
            if code == 1014 and idx < len(FLEX_QUERY_IDS) - 1:
                logger.warning(f"Query {qid} 无效 (1014)，尝试下一个 Query ID...")
                await asyncio.sleep(10)
                continue
            if idx < len(FLEX_QUERY_IDS) - 1:
                logger.warning(f"Query {qid} 拉取失败，10s 后尝试下一个 Query ID...")
                await asyncio.sleep(10)
                continue
            raise

    if last_err:
        raise last_err
    raise RuntimeError("未配置可用的 Flex Query ID")


# ── Executions 回退引擎内部锁池（避免循环引用 main.py）──
_fallback_locks: dict[str, asyncio.Lock] = {}


def _get_fallback_lock(symbol: str) -> asyncio.Lock:
    if symbol not in _fallback_locks:
        _fallback_locks[symbol] = asyncio.Lock()
    return _fallback_locks[symbol]


def _flex_trade_attr(trade, *names, default=""):
    """兼容多种 Flex 报表字段命名。"""
    for name in names:
        val = getattr(trade, name, None)
        if val is not None and val != "":
            return val
    return default


_REALIZED_PNL_FIELDS = (
    "fifoPnlRealized", "realizedPNL", "RealizedPNL",
    "realizedPL", "realizedPnl", "RealizedP/L",
)
_EXEC_ID_FIELDS = ("ibExecID", "ibExecutionID", "ibOrderID", "tradeID", "TradeID")


def _flex_realized_pnl(trade) -> float:
    try:
        return float(_flex_trade_attr(trade, *_REALIZED_PNL_FIELDS, default=0) or 0)
    except (TypeError, ValueError):
        return 0.0


def _flex_exec_id(trade) -> str:
    return str(_flex_trade_attr(trade, *_EXEC_ID_FIELDS, default="") or "")


def _flex_level_of_detail(trade) -> str:
    return str(
        _flex_trade_attr(trade, "levelOfDetail", "LevelOfDetail", default="") or ""
    ).lower()


def _flex_trade_sort_key(trade) -> tuple:
    """Closed Lot 行优先于 Executions 行，避免零 PnL 执行行抢占 exec_id。"""
    lod = _flex_level_of_detail(trade)
    is_closed_lot = "closed" in lod
    return (0 if is_closed_lot else 1, -abs(_flex_realized_pnl(trade)))


def _prepare_flex_trades(report) -> list:
    trades = report.extract("Trade") or []
    return sorted(trades, key=_flex_trade_sort_key)


@ai_trace
async def _run_executions_fallback(ib, tg_notify_func) -> None:
    """Flex 降级路径：当 Flex 1001/1025 限流时，从 TWS 拉取成交记录。

    按时间排序后逐笔回放，复刻实时监听的 FIFO 开平仓逻辑，
    跳过风控校验（历史成交已成事实），仅做账本核销 + Notion 同步。
    """
    logger.info("🔄 [Executions Fallback] 启动 TWS 成交记录应急通道...")

    # ── Phase 0: 拉取近 5 天 STK 成交 ──
    start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=5)
    try:
        fills = await ib.reqExecutionsAsync(
            ExecutionFilter(time=formatIBDatetime(start))
        )
    except Exception as e:
        logger.error(f"Executions 应急通道拉取失败: {e}")
        if tg_notify_func:
            await tg_notify_func(
                f"⚠️ Executions 应急通道也失败了: {type(e).__name__}\n"
                f"Flex 限流 + Executions 失败，本轮结算完全跳过。"
            )
        return

    # 仅保留有效 STK 成交
    fills = [
        f for f in fills
        if f.contract.secType == "STK"
        and float(f.execution.shares) > 0
        and f.contract.symbol
    ]
    if not fills:
        logger.info("Executions 无有效 STK 成交记录。")
        return

    # ── Phase 1: 去重 + 排序 ──
    async with connect_db() as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS fill_processed ("
            "exec_id TEXT PRIMARY KEY, "
            "processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        await db.execute(
            "DELETE FROM fill_processed WHERE processed_at < datetime('now', '-30 days')"
        )

        new_fills = []
        for f in fills:
            cur = await db.execute(
                "SELECT 1 FROM fill_processed WHERE exec_id=?",
                (f.execution.execId,),
            )
            if not await cur.fetchone():
                new_fills.append(f)

        if not new_fills:
            logger.info("Executions 无新成交记录（全部已处理）。")
            return

        # Phase 2: 按时间升序（确保 FIFO 语义正确）
        new_fills.sort(key=lambda f: f.execution.time)
        logger.info(f"Executions 发现 {len(new_fills)} 笔新成交待处理...")

        # ── Phase 3: 逐笔回放 ──
        db.row_factory = lambda cursor, row: dict(
            zip([c[0] for c in cursor.description], row)
        )
        opens_count = 0
        closes_count = 0
        EPS = 1e-6

        for fill in new_fills:
            symbol = fill.contract.symbol
            exec_id = fill.execution.execId
            side = "LONG" if fill.execution.side == "BOT" else "SHORT"
            qty = float(fill.execution.shares)
            price = float(fill.execution.price)

            lock = _get_fallback_lock(symbol)
            async with lock:
                # 查当前 OPEN 仓位
                cur = await db.execute(
                    "SELECT id, side, quantity, entry_price, current_stop, "
                    "realized_pnl, setup_tag, initial_stop, create_time "
                    "FROM shadow_ledger "
                    "WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC",
                    (symbol,),
                )
                open_rows = await cur.fetchall()
                opening = not open_rows or open_rows[0]["side"] == side

                exec_time_val = (
                    fill.execution.time.isoformat()
                    if fill.execution.time else ""
                )
                exchange_val = getattr(fill.execution, "exchange", "") or ""

                if opening:
                    # ── 开仓 / 同向加仓 ──
                    tranche_id = f"T{len(open_rows) + 1}"
                    cur = await db.execute(
                        "INSERT INTO shadow_ledger "
                        "(symbol, tranche_id, side, quantity, entry_price, "
                        "initial_stop, current_stop, status, setup_tag, "
                        "exec_time, exchange) "
                        "VALUES (?, ?, ?, ?, ?, 0.0, 0.0, 'OPEN', ?, ?, ?)",
                        (symbol, tranche_id, side, qty, price,
                         "EXEC_FALLBACK", exec_time_val, exchange_val),
                    )
                    new_id = cur.lastrowid
                    await db.commit()  # 释放写锁，允许 enqueue_outbound 写入
                    await enqueue_outbound(
                        f"{new_id}-OPEN", "notion",
                        {
                            "trade_id": new_id, "symbol": symbol,
                            "event_type": "OPEN", "side": side,
                            "quantity": qty, "entry_price": price,
                            "initial_stop": 0.0, "current_stop": 0.0,
                            "setup_tag": "EXEC_FALLBACK",
                        },
                    )
                    opens_count += 1

                else:
                    # ── 平仓 FIFO 核销 ──
                    remaining = qty
                    for row in open_rows:
                        if remaining <= EPS:
                            break
                        t_id = row["id"]
                        t_qty = float(row["quantity"])
                        t_entry = float(row["entry_price"])
                        t_side = row["side"]
                        t_tag = row["setup_tag"] or ""
                        t_stop = float(row.get("initial_stop") or 0)
                        t_create = row.get("create_time", "") or ""

                        # 竞态保护：确认仓位仍 OPEN
                        chk = await db.execute(
                            "SELECT quantity FROM shadow_ledger "
                            "WHERE id=? AND status='OPEN'",
                            (t_id,),
                        )
                        if not await chk.fetchone():
                            continue
                        db_qty = float((await db.execute(
                            "SELECT quantity FROM shadow_ledger WHERE id=?",
                            (t_id,),
                        )).fetchone()["quantity"])

                        actual_qty = min(db_qty, remaining)

                        if t_side == "LONG":
                            pnl = (price - t_entry) * actual_qty
                        else:
                            pnl = (t_entry - price) * actual_qty

                        if db_qty <= remaining + EPS:
                            # 全额平仓
                            exit_reason, t_r, t_days = compute_close_journal(
                                t_entry, t_stop, actual_qty, pnl, t_create, exec_time_val,
                            )
                            await db.execute(
                                "UPDATE shadow_ledger SET status='CLOSED', "
                                "exit_price=?, realized_pnl=COALESCE(realized_pnl,0)+?, "
                                "exit_reason=?, r_multiple=?, holding_days=?, "
                                "exit_time=? WHERE id=?",
                                (price, pnl, exit_reason, t_r, t_days,
                                 exec_time_val, t_id),
                            )
                            await db.commit()
                            await enqueue_outbound(
                                f"{t_id}-CLOSE", "notion",
                                {
                                    "trade_id": t_id, "symbol": symbol,
                                    "event_type": "CLOSE", "side": t_side,
                                    "quantity": db_qty, "entry_price": t_entry,
                                    "exit_price": price, "realized_pnl": pnl,
                                    "setup_tag": t_tag,
                                },
                            )
                        else:
                            # 部分平仓
                            new_qty = db_qty - remaining
                            await db.execute(
                                "UPDATE shadow_ledger SET quantity=?, "
                                "realized_pnl=COALESCE(realized_pnl,0)+? "
                                "WHERE id=?",
                                (new_qty, pnl, t_id),
                            )
                            await db.commit()
                            await enqueue_outbound(
                                f"{t_id}-UPDATE", "notion",
                                {
                                    "trade_id": t_id, "symbol": symbol,
                                    "event_type": "UPDATE",
                                    "quantity": new_qty,
                                    "realized_pnl": pnl,
                                    "entry_price": t_entry,
                                    "side": t_side,
                                },
                            )

                        remaining -= actual_qty
                        closes_count += 1

                    # 卖超检测：FIFO 耗尽仍有剩余 → 反向开仓
                    if remaining > EPS:
                        rev_side = side
                        cur = await db.execute(
                            "INSERT INTO shadow_ledger "
                            "(symbol, tranche_id, side, quantity, entry_price, "
                            "initial_stop, current_stop, status, setup_tag, "
                            "exec_time, exchange) "
                            "VALUES (?, ?, ?, ?, ?, 0.0, 0.0, 'OPEN', ?, ?, ?)",
                            (symbol, f"T_EXEC_REV", rev_side, remaining, price,
                             "EXEC_FALLBACK", exec_time_val, exchange_val),
                        )
                        rev_id = cur.lastrowid
                        await db.commit()
                        await enqueue_outbound(
                            f"{rev_id}-OPEN", "notion",
                            {
                                "trade_id": rev_id, "symbol": symbol,
                                "event_type": "OPEN", "side": rev_side,
                                "quantity": remaining, "entry_price": price,
                                "initial_stop": 0.0, "current_stop": 0.0,
                                "setup_tag": "EXEC_FALLBACK",
                            },
                        )
                        opens_count += 1

                # 标记成交已处理
                await db.execute(
                    "INSERT OR IGNORE INTO fill_processed (exec_id) VALUES (?)",
                    (exec_id,),
                )

        await db.commit()

    # ── Phase 4: 汇总通知 ──
    parts = [f"🔄 **Executions 应急通道完成**（Flex 降级）"]
    if opens_count:
        parts.append(f"📥 导入/翻仓 {opens_count} 笔")
    if closes_count:
        parts.append(f"📤 关仓核销 {closes_count} 笔")
    msg = "\n".join(parts)
    logger.info(msg.replace("\n", " | "))
    if tg_notify_func:
        await tg_notify_func(msg)


@ai_trace
async def run_flex_settlement(tg_notify_func=None, ib=None):
    """
    Flex 官方结算。

    阶段 1 — 关仓核销：逐笔已实现盈亏的交易匹配影子账本，关仓 + 推送 Notion。
              若影子账本无匹配（已被 TWS 实时监听先关），仍入队孤儿 CLOSE 记录。
    阶段 2 — 持仓同步：PnL=0 的未平仓交易，拉取 TWS 实时仓位对比，
              股数/成本不匹配则 Notion UPDATE，账本缺失则自动导入 + Notion OPEN。
    """
    if FLEX_TOKEN.startswith("YOUR_") or not FLEX_QUERY_IDS:
        logger.info("未配置 Flex Token / Query ID，跳过权威结算。")
        return

    # ── 交易日过滤 ──
    nyse = mcal.get_calendar("NYSE")
    today = datetime.datetime.now()
    valid_days = nyse.valid_days(
        start_date=today - datetime.timedelta(days=5),
        end_date=today,
    )
    if len(valid_days) < 2:
        logger.info("近期无有效交易日，跳过 Flex 结算。")
        return

    logger.info(
        f"🧾 [Flex Settlement] 开始拉取 IBKR 官方结算报表 "
        f"(Query 池: {len(FLEX_QUERY_IDS)} 个，主 Query: {FLEX_QUERY_IDS[0]})..."
    )

    loop = asyncio.get_running_loop()
    report = None

    try:
        report = await _fetch_flex_report(loop)
    except Exception as fe:
        logger.error(f"Flex 报表拉取失败: {fe}")

    # ── Flex 失败 → 降级为 Executions 应急通道 ──
    if report is None:
        logger.warning("⚠️ Flex 报表拉取失败，自动降级为 Executions 应急通道...")

        # ── 自愈重连：TWS 断线时尝试强制唤醒，打破单点故障 ──
        connected = ib and ib.isConnected()
        if ib and not connected:
            logger.warning("🔄 侦测到 TWS 断开，尝试自愈重连...")
            try:
                paper_port, live_port, mode = resolve_tws_ports()
                port = live_port if mode == "live" else paper_port
                await ib.connectAsync(TWS_HOST, port, clientId=CLIENT_ID, timeout=15)
                connected = True
                logger.info(f"✅ TWS 自愈重连成功 ({TWS_HOST}:{port})")
            except Exception as conn_err:
                logger.error(f"❌ TWS 自愈重连失败: {conn_err}")

        if connected:
            try:
                await _run_executions_fallback(ib, tg_notify_func)
            except Exception as fallback_e:
                logger.error(f"Executions 应急通道异常: {fallback_e}")
                if tg_notify_func:
                    await tg_notify_func(
                        f"⚠️ Executions 应急通道异常: {type(fallback_e).__name__}"
                    )
        else:
            logger.warning("TWS 未连接且重连失败，无法启动 Executions 应急通道。")
            if tg_notify_func:
                await tg_notify_func(
                    "🚨 **清算引擎警报**\n"
                    "Flex 报表全部限流且 TWS 失去连接，今日账本未能对齐！"
                )
        return

    trades = _prepare_flex_trades(report)
    if not trades:
        logger.info("🧾 Flex 报表中无任何交易记录。")
        return

    # ── MD5 哈希防重 ──
    signature_factors = [
        (
            _flex_trade_attr(t, "symbol", "underlyingSymbol"),
            float(_flex_trade_attr(t, "tradePrice", "TradePrice", default=0) or 0),
            _flex_realized_pnl(t),
        )
        for t in trades
    ]
    current_hash = hashlib.md5(str(signature_factors).encode("utf-8")).hexdigest()

    async with connect_db() as db:
        db.row_factory = lambda cursor, row: dict(
            zip([c[0] for c in cursor.description], row)
        )
        await db.execute(
            "CREATE TABLE IF NOT EXISTS flex_processed_execs ("
            "exec_id TEXT PRIMARY KEY, "
            "processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        await db.execute(
            "DELETE FROM flex_processed_execs "
            "WHERE processed_at < datetime('now', '-30 days')"
        )

        cur = await db.execute(
            "SELECT value FROM system_state WHERE key='last_flex_hash'"
        )
        hash_row = await cur.fetchone()
        if hash_row and hash_row["value"] == current_hash:
            logger.info("✅ 报表哈希比对一致，无新清算记录，安全跳过。")
            return

        # ═══════════════════════════════════════════════════
        # 阶段 1：逐笔核销已平仓交易（realized_pnl ≠ 0）
        # ═══════════════════════════════════════════════════
        closed_count = 0
        total_pnl = 0.0
        closed_symbols: list[str] = []
        orphan_closes = 0

        for trade in trades:
            symbol = _flex_trade_attr(trade, "symbol", "underlyingSymbol")
            if not symbol:
                continue

            exec_id = _flex_exec_id(trade)
            if not exec_id:
                exec_id = hashlib.md5(
                    f"{symbol}-{_flex_trade_attr(trade, 'tradePrice', 'TradePrice', default=0)}-"
                    f"{_flex_realized_pnl(trade)}".encode()
                ).hexdigest()

            cur = await db.execute(
                "SELECT 1 FROM flex_processed_execs WHERE exec_id=?", (exec_id,)
            )
            if await cur.fetchone():
                continue

            exec_price = float(
                _flex_trade_attr(trade, "tradePrice", "TradePrice", default=0) or 0
            )
            realized_pnl = _flex_realized_pnl(trade)

            # Executions 行 PnL=0：跳过且不入 flex_processed_execs，
            # 否则同 exec_id 的 Closed Lot 行会被误挡（Executions+Closed Lots 双选场景）
            if abs(realized_pnl) < 1e-4:
                continue

            # 查询影子账本 OPEN 仓位
            cur = await db.execute(
                "SELECT id, setup_tag, entry_price, initial_stop, quantity, side, "
                "realized_pnl, create_time FROM shadow_ledger "
                "WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC",
                (symbol,),
            )
            open_rows = await cur.fetchall()

            if not open_rows:
                # ── 孤儿关仓：Flex 显示已平仓，但影子账本无 OPEN 仓位 ──
                #    仍然写入 Notion，确保交易日记完整
                flex_qty = abs(float(
                    _flex_trade_attr(trade, "quantity", "Quantity", "shares", default=0) or 0
                ))
                trade_side = _flex_trade_attr(trade, "buySell", "side", "buy/Sell", default="")
                if not trade_side:
                    trade_side = "LONG" if realized_pnl > 0 else "SHORT"

                await enqueue_outbound(
                    f"FLEX_ORPHAN_{exec_id[-8:]}", "notion",
                    {
                        "trade_id": 0,
                        "symbol": symbol,
                        "event_type": "CLOSE",
                        "realized_pnl": realized_pnl,
                        "setup_tag": "FLEX_ORPHAN",
                        "entry_price": exec_price,
                        "exit_price": exec_price,
                        "quantity": flex_qty if flex_qty > 0 else 0,
                        "side": trade_side,
                    },
                )
                await db.execute(
                    "INSERT OR IGNORE INTO flex_processed_execs (exec_id) VALUES (?)",
                    (exec_id,),
                )
                orphan_closes += 1
                total_pnl += realized_pnl
                logger.info(
                    f"👻 [孤儿关仓] {symbol} PnL=${realized_pnl:.2f} "
                    f"→ 影子账本无匹配，已写入 Notion 供复盘"
                )
                continue

            # ── 正常关仓：匹配到 OPEN 仓位，FIFO 核销 ──
            flex_qty = abs(float(
                _flex_trade_attr(trade, "quantity", "Quantity", "shares", default=0) or 0
            ))
            if flex_qty < 1e-6:
                flex_qty = sum(float(r["quantity"]) for r in open_rows)

            remaining_qty = flex_qty
            trade_pnl = realized_pnl

            for row in open_rows:
                if remaining_qty <= 1e-6:
                    break

                trade_id = row["id"]
                setup_tag = row["setup_tag"] or ""
                entry_p = float(row["entry_price"])
                init_stop = float(row["initial_stop"])
                db_qty = float(row["quantity"])
                pos_side = row["side"]
                old_pnl = float(row["realized_pnl"] or 0.0)
                t_create = row.get("create_time", "") or ""

                close_qty = min(db_qty, remaining_qty)
                portion_pnl = trade_pnl * (close_qty / flex_qty) if flex_qty > 0 else trade_pnl
                new_total_pnl = old_pnl + portion_pnl
                new_qty = db_qty - close_qty
                is_fully_closed = new_qty <= 1e-4
                new_status = "CLOSED" if is_fully_closed else "OPEN"

                if is_fully_closed:
                    exit_time_flex = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    exit_reason, t_r, t_days = compute_close_journal(
                        entry_p, init_stop, close_qty, portion_pnl, t_create, exit_time_flex,
                    )
                    await db.execute(
                        "UPDATE shadow_ledger SET status=?, exit_price=?, quantity=?, "
                        "realized_pnl=?, exit_reason=?, r_multiple=?, holding_days=?, "
                        "exit_time=? WHERE id=?",
                        (new_status, exec_price, new_qty, new_total_pnl,
                         exit_reason, t_r, t_days, exit_time_flex, trade_id),
                    )
                else:
                    await db.execute(
                        "UPDATE shadow_ledger SET status=?, exit_price=?, quantity=?, "
                        "realized_pnl=? WHERE id=?",
                        (new_status, exec_price, new_qty, new_total_pnl, trade_id),
                    )

                initial_risk = abs(entry_p - init_stop) * db_qty if init_stop > 0 else 0.0
                event_type = "CLOSE" if is_fully_closed else "UPDATE"

                await enqueue_outbound(
                    f"{trade_id}-{event_type}_FLEX_{exec_id[-6:]}", "notion",
                    {
                        "trade_id": trade_id,
                        "symbol": symbol,
                        "event_type": event_type,
                        "realized_pnl": new_total_pnl,
                        "setup_tag": setup_tag,
                        "initial_risk": initial_risk,
                        "entry_price": entry_p,
                        "exit_price": exec_price,
                        "quantity": new_qty,
                        "side": pos_side,
                    },
                )

                await db.execute(
                    "INSERT OR IGNORE INTO flex_processed_execs (exec_id) VALUES (?)",
                    (exec_id,),
                )

                if is_fully_closed:
                    closed_count += 1
                    closed_symbols.append(symbol)
                remaining_qty -= close_qty

            # ── 更新连亏计数器 ──
            if trade_pnl > 0:
                await db.execute(
                    "INSERT INTO system_state (key, value) VALUES "
                    "('consecutive_losses', '0') "
                    "ON CONFLICT(key) DO UPDATE SET value = '0'"
                )
                logger.info(f"🎯 {symbol} 止盈，连亏计数器已重置。")
            else:
                await db.execute(
                    "INSERT INTO system_state (key, value) VALUES "
                    "('consecutive_losses', '1') "
                    "ON CONFLICT(key) DO UPDATE SET value = "
                    "CAST(CAST(value AS INTEGER) + 1 AS TEXT)"
                )
                logger.info(f"🩸 {symbol} 止损，连亏计数器 +1。")
            total_pnl += trade_pnl

        # ═══════════════════════════════════════════════════
        # 阶段 2：持仓同步（PnL=0 的未平仓交易 ↔ TWS 实时数据）
        # ═══════════════════════════════════════════════════
        sync_count = 0
        if ib and ib.isConnected():
            try:
                tws_positions = {
                    p.contract.symbol: p
                    for p in await ib.reqPositionsAsync()
                    if p.contract.secType == "STK" and p.position != 0
                }
            except Exception as e:
                logger.warning(f"阶段2 TWS 持仓拉取失败: {e}")
                tws_positions = {}
        else:
            tws_positions = {}

        # 收集阶段 1 跳过处理的未平仓标的
        synced_symbols: set[str] = set()
        for trade in trades:
            symbol = _flex_trade_attr(trade, "symbol", "underlyingSymbol")
            if not symbol or symbol in synced_symbols:
                continue

            realized_pnl = _flex_realized_pnl(trade)
            # 只处理未实现盈亏的（仍持有的）
            if abs(realized_pnl) >= 1e-4:
                continue

            synced_symbols.add(symbol)
            tws_pos = tws_positions.get(symbol)

            # 查影子账本
            cur = await db.execute(
                "SELECT id, side, quantity, entry_price, current_stop, setup_tag "
                "FROM shadow_ledger WHERE symbol=? AND status='OPEN' ORDER BY create_time ASC",
                (symbol,),
            )
            ledger_rows = await cur.fetchall()

            if tws_pos is None:
                # TWS 无仓位 → 跳过（无法对比）
                continue

            tws_qty = abs(float(tws_pos.position))
            tws_side = "LONG" if float(tws_pos.position) > 0 else "SHORT"
            tws_avg = float(tws_pos.avgCost) if tws_pos.avgCost and float(tws_pos.avgCost) > 0 else 0

            if not ledger_rows:
                # ── 账本无记录，TWS 有 → 自动导入 + Notion OPEN ──
                if tws_avg <= 0:
                    continue
                tranche_id = "T1"
                exec_now = datetime.datetime.now(datetime.timezone.utc).isoformat()
                cur = await db.execute(
                    "INSERT INTO shadow_ledger "
                    "(symbol, tranche_id, side, quantity, entry_price, "
                    "initial_stop, current_stop, status, setup_tag, exec_time) "
                    "VALUES (?, ?, ?, ?, ?, 0.0, 0.0, 'OPEN', 'FLEX_SYNC', ?)",
                    (symbol, tranche_id, tws_side, tws_qty, tws_avg, exec_now),
                )
                new_id = cur.lastrowid
                await enqueue_outbound(
                    f"{new_id}-OPEN", "notion",
                    {
                        "trade_id": new_id, "symbol": symbol,
                        "event_type": "OPEN", "side": tws_side,
                        "quantity": tws_qty, "entry_price": tws_avg,
                        "initial_stop": 0.0, "current_stop": 0.0,
                        "setup_tag": "FLEX_SYNC",
                    },
                )
                sync_count += 1
                logger.info(f"📥 [Flex Sync] {symbol} 自动导入: {tws_side} {tws_qty}股 @ ${tws_avg:.2f}")
                continue

            # ── 账本有记录 → 对比 TWS，不一致则 Notion UPDATE ──
            ledger_total_qty = sum(float(r["quantity"]) for r in ledger_rows)
            ledger_side = ledger_rows[0]["side"]

            qty_match = abs(ledger_total_qty - tws_qty) < 0.01
            side_match = ledger_side == tws_side
            price_match = (
                abs(float(ledger_rows[0]["entry_price"]) - tws_avg) / max(tws_avg, 0.01) < 0.01
                if tws_avg > 0 else True
            )

            if not (qty_match and side_match and price_match):
                for row in ledger_rows:
                    await enqueue_outbound(
                        f"{row['id']}-UPDATE_FLEX_SYNC", "notion",
                        {
                            "trade_id": row["id"], "symbol": symbol,
                            "event_type": "UPDATE",
                            "quantity": tws_qty if not qty_match else float(row["quantity"]),
                            "entry_price": tws_avg if not price_match else float(row["entry_price"]),
                            "side": tws_side if not side_match else row["side"],
                            "current_stop": float(row["current_stop"]),
                        },
                    )
                sync_count += 1
                mismatch_parts = []
                if not qty_match:
                    mismatch_parts.append(f"股数 {ledger_total_qty}→{tws_qty}")
                if not side_match:
                    mismatch_parts.append(f"方向 {ledger_side}→{tws_side}")
                if not price_match:
                    mismatch_parts.append(f"成本 ${float(ledger_rows[0]['entry_price']):.2f}→${tws_avg:.2f}")
                logger.info(f"🔄 [Flex Sync] {symbol} 不一致 ({', '.join(mismatch_parts)})，已推送 Notion UPDATE")

        await db.execute(
            "INSERT OR REPLACE INTO system_state (key, value) VALUES "
            "('last_flex_hash', ?)",
            (current_hash,),
        )
        await db.commit()

    # ── 汇报 ──
    parts: list[str] = []
    if closed_count > 0:
        pnl_str = f"+${total_pnl:.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):.2f}"
        parts.append(
            f"✅ 关仓 {closed_count} 笔：{', '.join(closed_symbols)}，合计 {pnl_str}"
        )
    if orphan_closes > 0:
        parts.append(f"👻 孤儿关仓 {orphan_closes} 笔（影子账本无匹配，已写入 Notion）")
    if sync_count > 0:
        parts.append(f"📊 持仓同步 {sync_count} 笔（与 TWS 对比后更新/导入）")

    if parts:
        msg = "✅ **Flex 权威结算完成**\n" + "\n".join(parts)
        logger.info(msg.replace("\n", " | "))
        if tg_notify_func:
            await tg_notify_func(msg)
    else:
        logger.info("🧾 Flex 报表拉取成功，当前没有需要清算的 OPEN 仓位。")

    return closed_count + orphan_closes, total_pnl
