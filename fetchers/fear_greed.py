"""恐贪指数 Fetcher — alternative.me"""

import logging
from datetime import datetime, timezone

import httpx

from db import Database
from .base import BaseFetcher

logger = logging.getLogger(__name__)

API_URL = "https://api.alternative.me/fng/"


class FearGreedFetcher(BaseFetcher):
    name = "fear_greed"
    interval_seconds = 60 * 60  # 1 小时

    def _run(self) -> int:
        with httpx.Client(timeout=15) as client:
            resp = client.get(API_URL)
            resp.raise_for_status()
            data = resp.json()

        items = data.get("data", [])
        if not items:
            return 0

        item = items[0]
        value = int(item["value"])
        label = item.get("value_classification", "")
        # alternative.me 返回的 timestamp 是 UTC 零点
        ts = datetime.fromtimestamp(int(item["timestamp"]), tz=timezone.utc)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

        self.db.execute("""
            INSERT OR REPLACE INTO fear_greed (ts, value, label)
            VALUES (?, ?, ?)
        """, (ts_str, value, label))
        self.db.commit()
        return 1
