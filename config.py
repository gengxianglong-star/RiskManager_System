import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

MY_TELEGRAM_CHAT_ID = int(os.getenv("MY_TELEGRAM_CHAT_ID", "123456789"))
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "YOUR_TG_TOKEN")
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
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "YOUR_NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "YOUR_NOTION_DATABASE_ID")
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

# ==========================================
# 部署环境与高级引擎开关 (Deployment & Engine Toggles)
# ==========================================

# EOD 10EMA 狙击手开关。
# - 本地关机模式 (False): 依靠 TWS 物理止损单防守，忽略收盘破位。
# - 云端 VPS 模式 (True): 24 小时在线，美东 15:55 准时执行日线破位审判。
ENABLE_EOD_SNIPER = os.getenv("ENABLE_EOD_SNIPER", "false").lower() in (
    "1",
    "true",
    "yes",
)

# ====== 提取硬编码参数与日志配置 ======
THRESHOLD_3R = float(os.getenv("THRESHOLD_3R", "3.0"))          # 3R爆发提醒阈值
EMA_PERIOD = int(os.getenv("EMA_PERIOD", "10"))                 # 均线狙击手周期
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
