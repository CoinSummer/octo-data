"""Chainbot News Fetcher — 通过 Chainbot contents API 拉取新闻（Odaily 等）"""

import logging

import httpx

from config import CHAINBOT_API_KEY, CHAINBOT_BASE_URL
from .base import BaseFetcher, normalize_ts

logger = logging.getLogger(__name__)

CONTENTS_URL = f"{CHAINBOT_BASE_URL}/api/v1/scanner/contents"


class KBNewsFetcher(BaseFetcher):
    name = "kb_news"
    interval_seconds = 30 * 60  # 30 分钟

    def _run(self) -> int:
        if not CHAINBOT_API_KEY:
            logger.warning("[kb_news] CHAINBOT_API_KEY not set, skipping")
            return 0

        headers = {"X-API-Key": CHAINBOT_API_KEY}
        params = {"type": "news", "limit": "100", "order_by": "published_at_desc"}

        # 增量：只拉最新的；首次只拉最近 7 天
        latest = self.db.fetchone("SELECT MAX(ts) as max_ts FROM kb_news")
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
                news_id = item.get("id")
                if not news_id:
                    continue

                try:
                    self.db.execute("""
                        INSERT OR IGNORE INTO kb_news (id, ts, subject, source_name, source_url, content)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        str(news_id),
                        normalize_ts(item.get("published_at", "")),
                        item.get("title", ""),
                        item.get("source_name", ""),
                        item.get("url", ""),
                        item.get("content", ""),
                    ))
                    count += 1
                except Exception as e:
                    logger.warning(f"[kb_news] Insert failed: {e}")

            self.db.commit()

            # 翻页
            if result.get("has_more") and result.get("next_cursor"):
                cursor = result["next_cursor"]
            else:
                break

        return count
