import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv

# These paths are assuming the container's file structure.
LOG_DIR = Path("/app/logs")
DATA_DIR = Path("/app/data")
DB_FILE = DATA_DIR / "cmma.db"

TIMEFRAME_MAP = {
    "1m": "1", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "4h": "240", "1d": "D", "1w": "W", "1M": "M"
}

class AppConfig:
    def __init__(self, dotenv_path=None):
        if dotenv_path:
            load_dotenv(dotenv_path)

        self.timeframes = os.getenv("TIMEFRAMES", "1m,5m,15m,30m,1h,4h,1d").split(',')
        self.log_max_size_mb = int(os.getenv("LOG_MAX_SIZE_MB", "10"))
        self.concurrency_limit = int(os.getenv("CONCURRENCY_LIMIT", "10"))
        self.fetch_interval_seconds = int(os.getenv("FETCH_INTERVAL_SECONDS", "300"))
        self.ohlcv_history_limit = int(os.getenv("OHLCV_HISTORY_LIMIT", "5"))
        self.base_url = "https://api.bybit.com"

def setup_logging(config: AppConfig) -> logging.Logger:
    LOG_DIR.mkdir(exist_ok=True)
    log_file = LOG_DIR / "fetcher.log"
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=config.log_max_size_mb * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger("BybitFetcher")
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger
