import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

MY_TELEGRAM_CHAT_ID = int(os.getenv("MY_TELEGRAM_CHAT_ID", "123456789"))
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "YOUR_TG_TOKEN")
TWS_HOST = os.getenv("TWS_HOST", "127.0.0.1")
TWS_PORT = int(os.getenv("TWS_PORT", "7497"))
CLIENT_ID = int(os.getenv("CLIENT_ID", "2"))
FLEX_TOKEN = os.getenv("FLEX_TOKEN", "YOUR_FLEX_TOKEN")
FLEX_QUERY_ID = os.getenv("FLEX_QUERY_ID", "YOUR_FLEX_QUERY_ID")
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "YOUR_NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "YOUR_NOTION_DATABASE_ID")
DB_PATH = os.getenv("DB_PATH", "risk_manager.db")

RISK_MAX_DRAWDOWN_GREEN = float(os.getenv("RISK_MAX_DRAWDOWN_GREEN", "0.05"))
RISK_MAX_DRAWDOWN_YELLOW = float(os.getenv("RISK_MAX_DRAWDOWN_YELLOW", "0.10"))
RISK_PCT_PER_TRADE = float(os.getenv("RISK_PCT_PER_TRADE", "0.003"))
MAX_POSITION_SIZE_PCT = float(os.getenv("MAX_POSITION_SIZE_PCT", "0.40"))
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "0.03"))

# 狙击手协议：每日最大开仓次数
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "3"))

# 总隔夜风险上限 (占净值比例，推荐 0.015 即 1.5%)
MAX_OVERNIGHT_RISK_PCT = float(os.getenv("MAX_OVERNIGHT_RISK_PCT", "0.015"))
