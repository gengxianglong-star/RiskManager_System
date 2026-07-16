import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

TWS_HOST = os.getenv("TWS_HOST", "127.0.0.1")
# 兼容旧配置；优先跟随桌面端 ~/.ibkr-order-tool/settings.json
TWS_PORT = int(os.getenv("TWS_PORT", "7497"))
DEFAULT_PAPER_PORT = int(os.getenv("PAPER_PORT", "7497"))
DEFAULT_LIVE_PORT = int(os.getenv("LIVE_PORT", "7496"))
DESKTOP_SETTINGS_PATH = Path.home() / ".ibkr-order-tool" / "settings.json"
CLIENT_ID = int(os.getenv("CLIENT_ID", "1"))  # TWS API Client ID，避免与桌面端冲突
FLEX_TOKEN = os.getenv("FLEX_TOKEN", "YOUR_FLEX_TOKEN")
FLEX_QUERY_ID = os.getenv("FLEX_QUERY_ID", "1562873")
# ── Query ID 轮询池：防止单个 Query 频繁触发 1001/1025 限流 ──
FLEX_QUERY_IDS = [
    qid.strip()
    for qid in os.getenv(
        "FLEX_QUERY_IDS",
        "1562873,1568627,1568630",
    ).split(",")
    if qid.strip()
]
DB_PATH = os.getenv("DB_PATH", "risk_manager.db")
DB_TIMEOUT = float(os.getenv("DB_TIMEOUT", "10.0"))


def resolve_db_path() -> str:
    """根据桌面端账户模式返回独立数据库路径，防止模拟/实盘数据混淆。"""
    if DB_PATH not in ("risk_manager.db", ""):
        return DB_PATH  # 用户显式指定了路径，原样使用

    mode = "paper"
    if DESKTOP_SETTINGS_PATH.exists():
        try:
            import json
            data = json.loads(DESKTOP_SETTINGS_PATH.read_text(encoding="utf-8"))
            mode = str(data.get("account_mode", mode)).lower()
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass

    suffix = "live" if mode == "live" else "paper"
    return f"risk_manager_{suffix}.db"
# 狙击手协议与 account_state 日切时区（美股默认美东）
TRADING_TZ = os.getenv("TRADING_TZ", "America/New_York")

RISK_MAX_DRAWDOWN_GREEN = float(os.getenv("RISK_MAX_DRAWDOWN_GREEN", "0.05"))
RISK_MAX_DRAWDOWN_YELLOW = float(os.getenv("RISK_MAX_DRAWDOWN_YELLOW", "0.10"))
RISK_PCT_PER_TRADE = float(os.getenv("RISK_PCT_PER_TRADE", "0.003"))
MAX_POSITION_SIZE_PCT = float(os.getenv("MAX_POSITION_SIZE_PCT", "0.40"))
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "0.03"))

# 狙击手协议：每日最大开仓次数
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "3"))

# 总隔夜风险上限 (占净值比例，推荐 0.015 即 1.5%)
MAX_OVERNIGHT_RISK_PCT = float(os.getenv("MAX_OVERNIGHT_RISK_PCT", "0.015"))

# ── RiskCore 定稿契约（与 risk_core.constants 对齐）──
MIN_CUSHION = float(os.getenv("MIN_CUSHION", "0.10"))
MIN_RISK_PER_SHARE = float(os.getenv("MIN_RISK_PER_SHARE", "1.0"))
MIN_ENTRY_PRICE = float(os.getenv("MIN_ENTRY_PRICE", "5.0"))
MATERIAL_DEPLETION_PCT = float(os.getenv("MATERIAL_DEPLETION_PCT", "0.05"))
AVG_COST_DRIFT_PCT = float(os.getenv("AVG_COST_DRIFT_PCT", "0.20"))
MAX_SYNC_GAP_DAYS = int(os.getenv("MAX_SYNC_GAP_DAYS", "7"))
IN_FLIGHT_TTL_SEC = float(os.getenv("IN_FLIGHT_TTL_SEC", "30.0"))
POSITION_SYNC_WARMUP_SEC = float(os.getenv("POSITION_SYNC_WARMUP_SEC", "3.0"))
ALLOWED_CURRENCY = os.getenv("ALLOWED_CURRENCY", "USD")
ALLOWED_SEC_TYPE = os.getenv("ALLOWED_SEC_TYPE", "STK")
IB_EXEC_TIMEZONE = os.getenv("IB_EXEC_TIMEZONE", "")
ALLOWED_ENTRY_ORDER_TYPES = frozenset(
    t.strip().upper()
    for t in os.getenv("ALLOWED_ENTRY_ORDER_TYPES", "LMT,STP LMT,STP").split(",")
    if t.strip()
)

# ==========================================
# 部署环境与高级引擎开关 (Deployment & Engine Toggles)
# ==========================================

# ====== 提取硬编码参数与日志配置 ======
THRESHOLD_3R = float(os.getenv("THRESHOLD_3R", "3.0"))          # 3R爆发提醒阈值
FORCE_CONFESSION_HOUR = int(os.getenv("FORCE_CONFESSION_HOUR", "23")) # 强制坦白触发时间(小时)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")                      # 系统日志级别


def resolve_tws_ports() -> tuple[int, int, str]:
    """跟随桌面端账户模式，返回 (首选端口, 备用端口, 模式中文)。"""
    paper_port = DEFAULT_PAPER_PORT
    live_port = DEFAULT_LIVE_PORT
    account_mode = "paper"

    if DESKTOP_SETTINGS_PATH.exists():
        try:
            data = json.loads(DESKTOP_SETTINGS_PATH.read_text(encoding="utf-8"))
            paper_port = int(data.get("paper_port", paper_port))
            live_port = int(data.get("live_port", live_port))
            account_mode = str(data.get("account_mode", account_mode)).lower()
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            print(f"⚠️ 读取桌面端 settings.json 失败，使用默认端口: {exc}")
    else:
        # 无桌面配置时，用 .env 的 TWS_PORT 推断模式
        if TWS_PORT == live_port:
            account_mode = "live"
        elif TWS_PORT == paper_port:
            account_mode = "paper"

    if account_mode == "live":
        return live_port, paper_port, "实盘"
    return paper_port, live_port, "模拟"
