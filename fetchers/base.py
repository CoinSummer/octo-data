"""Fetcher 基类 — 独立版，无外部通知依赖"""

import logging
import re
from typing import Optional

from db import Database

logger = logging.getLogger(__name__)

CONSECUTIVE_FAIL_THRESHOLD = 3


def normalize_ts(ts_str: str) -> str:
    """将各种时间戳格式统一为 'YYYY-MM-DD HH:MM:SS'。"""
    if not ts_str:
        return ts_str
    s = re.sub(r"\.\d+Z?$", "", ts_str)
    s = s.rstrip("Z")
    # 去掉 timezone offset (+00:00, +08:00 等)
    s = re.sub(r"[+-]\d{2}:\d{2}$", "", s)
    s = s.replace("T", " ")
    return s


class BaseFetcher:
    """所有定时 Fetcher 的基类"""

    name: str = "base"
    interval_seconds: int = 3600

    def __init__(self, db: Database):
        self.db = db
        self._consecutive_failures = 0

    def fetch_and_save(self):
        try:
            count = self._run()
            self.db.update_fetcher_status(self.name, success=True)
            self._consecutive_failures = 0
            logger.info(f"[{self.name}] Saved {count} records")
            return count
        except Exception as e:
            self._consecutive_failures += 1
            self.db.update_fetcher_status(self.name, success=False, error=str(e))
            logger.error(f"[{self.name}] Error: {e}")
            if self._consecutive_failures >= CONSECUTIVE_FAIL_THRESHOLD:
                logger.critical(
                    f"[{self.name}] {self._consecutive_failures} consecutive failures: {e}"
                )
            raise

    def _run(self) -> int:
        raise NotImplementedError
