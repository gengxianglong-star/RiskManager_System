"""
面向 AI 的上下文追踪日志架构（AI-Friendly Context Logging）。

用法: 在任意函数上挂 @ai_trace，崩溃时自动捕获入参/出参/耗时/完整堆栈。
支持 async 和 sync 函数，零侵入式改造。
"""

import asyncio
import functools
import logging
import time
import traceback
from logging.handlers import RotatingFileHandler

from config import LOG_LEVEL


def setup_ai_logger():
    """配置独立的 AI 追踪日志通道，写入 risk_manager_trace.log。"""
    logger = logging.getLogger("RiskManager_AI")
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    formatter = logging.Formatter(
        fmt='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    file_handler = RotatingFileHandler(
        'risk_manager_trace.log',
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8',
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


logger = setup_ai_logger()


# ── 参数安全截断（避免日志爆炸）──
def _safe_repr(obj, max_len: int = 500):
    """截断超长对象，防止日志文件被巨型 payload 撑爆。"""
    raw = repr(obj)
    if len(raw) > max_len:
        return raw[:max_len] + f"...<truncated, total {len(raw)} chars>"
    return raw


def _format_args_kwargs(args: tuple, kwargs: dict) -> str:
    """格式化参数展示，跳过 self/cls。"""
    parts = []
    for i, a in enumerate(args):
        if i == 0 and isinstance(a, object) and not isinstance(a, (int, float, str, bool, list, dict, tuple)):
            # 跳过 self / cls 避免日志污染
            parts.append(f"<{type(a).__name__} instance>")
        else:
            parts.append(_safe_repr(a))
    for k, v in kwargs.items():
        parts.append(f"{k}={_safe_repr(v)}")
    return ", ".join(parts) if parts else "(no args)"


# ═══════════════════════════════════════════════════════════
# 核心装饰器：自动识别 async / sync
# ═══════════════════════════════════════════════════════════

def ai_trace(func):
    """
    全自动上下文捕捉装饰器。

    功能:
    - 自动记录函数入口（DEBUG 模式）与出口（含耗时）
    - 崩溃时输出完整的 [CRASH REPORT]：函数名、入参、异常信息、调用栈
    - 自动识别 async / sync，无需手动选择变体

    示例:
        @ai_trace
        async def reconcile_physical_positions(ib, notify_func=None):
            ...
    """

    if asyncio.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            logger.debug(
                f"▶️ [START] {func.__module__}.{func.__name__} | {_format_args_kwargs(args, kwargs)}"
            )
            try:
                result = await func(*args, **kwargs)
                elapsed = (time.time() - start_time) * 1000
                logger.debug(
                    f"✅ [SUCCESS] {func.__module__}.{func.__name__} | 耗时: {elapsed:.2f}ms"
                )
                return result
            except Exception:
                elapsed = (time.time() - start_time) * 1000
                logger.error(
                    f"\n{'=' * 60}\n"
                    f"🚨 [CRASH REPORT] 函数崩溃: {func.__module__}.{func.__name__}\n"
                    f"⏱️  [ELAPSED] 崩溃前耗时: {elapsed:.2f}ms\n"
                    f"🛠️  [ERROR MSG] {traceback.format_exc().strip().split(chr(10))[-1]}\n"
                    f"📦 [PARAMETERS] {_format_args_kwargs(args, kwargs)}\n"
                    f"📜 [TRACEBACK]\n{traceback.format_exc().strip()}\n"
                    f"{'=' * 60}"
                )
                raise

        return async_wrapper

    else:

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            logger.debug(
                f"▶️ [START] {func.__module__}.{func.__name__} | {_format_args_kwargs(args, kwargs)}"
            )
            try:
                result = func(*args, **kwargs)
                elapsed = (time.time() - start_time) * 1000
                logger.debug(
                    f"✅ [SUCCESS] {func.__module__}.{func.__name__} | 耗时: {elapsed:.2f}ms"
                )
                return result
            except Exception:
                elapsed = (time.time() - start_time) * 1000
                logger.error(
                    f"\n{'=' * 60}\n"
                    f"🚨 [CRASH REPORT] 函数崩溃: {func.__module__}.{func.__name__}\n"
                    f"⏱️  [ELAPSED] 崩溃前耗时: {elapsed:.2f}ms\n"
                    f"🛠️  [ERROR MSG] {traceback.format_exc().strip().split(chr(10))[-1]}\n"
                    f"📦 [PARAMETERS] {_format_args_kwargs(args, kwargs)}\n"
                    f"📜 [TRACEBACK]\n{traceback.format_exc().strip()}\n"
                    f"{'=' * 60}"
                )
                raise

        return sync_wrapper
