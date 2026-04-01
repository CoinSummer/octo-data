"""通用 RSS Feed Fetcher — 配置化管理多个 RSS 源

写入 news 表，source 为各源名称。
新增源只需在 FEEDS 列表加一行。
"""

import logging
from datetime import datetime
from email.utils import parsedate_to_datetime

import feedparser
import httpx

from .base import BaseFetcher, normalize_ts

logger = logging.getLogger(__name__)

# (source_name, catalog_name, feed_url)
FEEDS = [
    ("36kr", "媒体", "https://36kr.com/feed"),
    ("techcrunch", "媒体", "https://techcrunch.com/feed/"),
    ("hackernews", "媒体", "https://hnrss.org/best"),
    ("meta_engineering", "媒体", "https://engineering.fb.com/feed/"),
]


def _parse_pub_date(entry) -> str:
    """从 feed entry 提取时间戳，统一为 YYYY-MM-DD HH:MM:SS。"""
    for key in ("published_parsed", "updated_parsed"):
        tp = getattr(entry, key, None) or entry.get(key)
        if tp:
            try:
                dt = datetime(*tp[:6])
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
    for key in ("published", "updated"):
        raw = entry.get(key, "")
        if raw:
            try:
                dt = parsedate_to_datetime(raw)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return normalize_ts(raw)
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class RSSFeedsFetcher(BaseFetcher):
    """通用 RSS 源 — 每 2 小时拉取全部配置源。"""

    name = "rss_feeds"
    interval_seconds = 7200  # 2h

    def _run(self) -> int:
        total = 0
        for source_name, catalog_name, feed_url in FEEDS:
            try:
                count = self._fetch_feed(source_name, catalog_name, feed_url)
                total += count
                logger.info(f"[rss_feeds/{source_name}] {count} articles")
            except Exception as e:
                logger.error(f"[rss_feeds/{source_name}] failed: {e}")
        return total

    def _fetch_feed(self, source_name: str, catalog_name: str, feed_url: str) -> int:
        resp = httpx.get(
            feed_url,
            headers={"User-Agent": "Mozilla/5.0 OctoData/1.0"},
            timeout=20,
            follow_redirects=True,
        )
        resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        count = 0

        for entry in feed.entries:
            title = entry.get("title", "").strip()
            if not title:
                continue

            link = entry.get("link", "")
            summary = entry.get("summary", "")
            # 截断过长的 summary
            if len(summary) > 500:
                summary = summary[:497] + "..."

            ts = _parse_pub_date(entry)
            # 用 link 做去重 code
            code = link or title

            tags = entry.get("tags", [])
            entities = ",".join(t.get("term", "") for t in tags if t.get("term"))

            self.db.execute("""
                INSERT OR IGNORE INTO news
                    (ts, catalog_id, catalog_name, title, body, body_text, code, source, url, entities)
                VALUES (?, 0, ?, ?, '', ?, ?, ?, ?, ?)
            """, (ts, catalog_name, title, summary, code, source_name, link, entities))
            count += 1

        self.db.commit()
        return count
