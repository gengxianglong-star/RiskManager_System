"""一次性建库脚本；表结构以 database.ensure_schema 为准，勿在此重复维护 DDL。"""
import asyncio

from config import DB_PATH
from database import ensure_schema


def create_database() -> None:
    asyncio.run(ensure_schema())
    print(f"✅ 数据库 {DB_PATH} 表结构已就绪。")


if __name__ == "__main__":
    create_database()
