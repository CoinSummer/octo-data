"""Fetcher 基类 — 独立版，无外部通知依赖"""

import logging
import re
import time
from typing import Optional

import httpx

from db import Database

logger = logging.getLogger(__name__)

CONSECUTIVE_FAIL_THRESHOLD = 3
RETRY_DELAYS = (10, 30, 60)  # 代理短暂断线时的重试间隔（秒）


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


def _is_connect_error(exc: Exception) -> bool:
    """判断是否为连接层错误（代理断线/DNS 失败等），值得重试"""
    return isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout, ConnectionError, OSError))


class BaseFetcher:
    """所有定时 Fetcher 的基类"""

    name: str = "base"
    interval_seconds: int = 3600

    def __init__(self, db: Database):
        self.db = db
        self._consecutive_failures = 0

    def fetch_and_save(self):
        last_exc = None
        for attempt, delay in enumerate((*RETRY_DELAYS, None)):
            try:
                count = self._run()
                self.db.update_fetcher_status(self.name, success=True)
                self._consecutive_failures = 0
                if attempt > 0:
                    logger.info(f"[{self.name}] Recovered after {attempt} retries")
                logger.info(f"[{self.name}] Saved {count} records")
                return count
            except Exception as e:
                last_exc = e
                if delay is not None and _is_connect_error(e):
                    logger.warning(f"[{self.name}] Connect error, retry in {delay}s: {e}")
                    time.sleep(delay)
                    continue
                break

        # 所有重试耗尽或非连接层错误
        self._consecutive_failures += 1
        self.db.update_fetcher_status(self.name, success=False, error=str(last_exc))
        logger.error(f"[{self.name}] Error: {last_exc}")
        if self._consecutive_failures >= CONSECUTIVE_FAIL_THRESHOLD:
            logger.critical(
                f"[{self.name}] {self._consecutive_failures} consecutive failures: {last_exc}"
            )
        raise last_exc

    def _run(self) -> int:
        raise NotImplementedError
