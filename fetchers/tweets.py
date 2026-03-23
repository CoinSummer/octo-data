"""Chainbot Tweets Fetcher — 通过 Chainbot contents API 拉取推文"""

import logging
from datetime import datetime, timezone

import httpx

from config import CHAINBOT_API_KEY, CHAINBOT_BASE_URL
from .base import BaseFetcher, normalize_ts

logger = logging.getLogger(__name__)

CONTENTS_URL = f"{CHAINBOT_BASE_URL}/api/v1/scanner/contents"


class TweetsFetcher(BaseFetcher):
    name = "tweets"
    interval_seconds = 30 * 60  # 30 分钟

    def _run(self) -> int:
        if not CHAINBOT_API_KEY:
            logger.warning("[tweets] CHAINBOT_API_KEY not set, skipping")
            return 0

        headers = {"X-API-Key": CHAINBOT_API_KEY}
        params = {"type": "tweet", "limit": "100", "order_by": "published_at_desc"}

        # 增量：只拉最新的；首次只拉最近 7 天
        latest = self.db.fetchone("SELECT MAX(ts) as max_ts FROM tweets")
        if latest and latest["max_ts"]:
            ts_str = latest["max_ts"].replace(" ", "T") + "Z"
            params["published_after"] = ts_str
        else:
            params["recent"] = "604800"  # 7 days in seconds

        count = 0
        cursor = None
        max_pages = 10

        while max_pages > 0:
            max_pages -= 1
            if cursor:
                params["cursor"] = cursor

            with httpx.Client(timeout=30) as client:
                resp = client.get(CONTENTS_URL, params=params, headers=headers)
                resp.raise_for_status()
                result = resp.json()

            items = result.get("data", [])
            if not items:
                break

            for item in items:
                item_id = item.get("id")
                if not item_id:
                    continue

                ts = normalize_ts(item.get("published_at", ""))
                content = item.get("content", "")
                username = item.get("username", "")

                try:
                    self.db.execute("""
                        INSERT OR IGNORE INTO tweets (id, ts, content, username, group_id, tags, source_url, reference)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        str(item_id),
                        ts,
                        content,
                        username,
                        "",
                        "",
                        item.get("url", ""),
                        "",
                    ))
                    count += 1
                except Exception as e:
                    logger.warning(f"[tweets] Failed to save {item_id}: {e}")

            self.db.commit()

            # 翻页
            if result.get("has_more") and result.get("next_cursor"):
                cursor = result["next_cursor"]
            else:
                break

        return count

    def backfill(self, day: str):
        """回填某天的推文。day 格式: YYYY-MM-DD"""
        if not CHAINBOT_API_KEY:
            logger.warning("[tweets] CHAINBOT_API_KEY not set")
            return 0

        headers = {"X-API-Key": CHAINBOT_API_KEY}
        params = {
            "type": "tweet",
            "limit": "100",
            "order_by": "published_at_asc",
            "published_after": f"{day}T00:00:00Z",
            "published_before": f"{day}T23:59:59Z",
        }

        total = 0
        cursor = None

        while True:
            if cursor:
                params["cursor"] = cursor

            with httpx.Client(timeout=30) as client:
                resp = client.get(CONTENTS_URL, params=params, headers=headers)
                resp.raise_for_status()
                result = resp.json()

            items = result.get("data", [])
            if not items:
                break

            for item in items:
                item_id = item.get("id")
                if not item_id:
                    continue

                try:
                    self.db.execute("""
                        INSERT OR IGNORE INTO tweets (id, ts, content, username, group_id, tags, source_url, reference)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        str(item_id),
                        normalize_ts(item.get("published_at", "")),
                        item.get("content", ""),
                        item.get("username", ""),
                        "",
                        "",
                        item.get("url", ""),
                        "",
                    ))
                    total += 1
                except Exception:
                    pass

            if result.get("has_more") and result.get("next_cursor"):
                cursor = result["next_cursor"]
            else:
                break

        self.db.commit()
        logger.info(f"[tweets] Backfilled {day}: {total} records")
        return total
