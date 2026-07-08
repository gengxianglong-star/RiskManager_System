"""结构化日志模块：替代 print，支持文件轮转与异步安全。"""
import logging
from logging.handlers import RotatingFileHandler
from config import LOG_LEVEL


def setup_logger():
    logger = logging.getLogger("RiskManager")
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-7s | %(module)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # 文件日志：最大 5MB，保留 3 个备份
    file_handler = RotatingFileHandler(
        'risk_manager.log', maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 控制台日志
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


logger = setup_logger()
