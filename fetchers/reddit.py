"""Reddit Fetcher — 通过 RSS feed 拉取指定 subreddit 热门帖子（无需登录）"""

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import httpx

from .base import BaseFetcher

logger = logging.getLogger(__name__)

DEFAULT_SUBREDDITS = [
    "cryptocurrency",
    "wallstreetbets",
    "bitcoin",
    "ethereum",
    "bitcoinmarkets",
    "defi",
    "stocks",
    "investing",
]
POSTS_PER_SUB = 25

NS = {"atom": "http://www.w3.org/2005/Atom"}


class RedditFetcher(BaseFetcher):
    name = "reddit"
    interval_seconds = 60 * 60  # 1 小时

    def _run(self) -> int:
        total = 0
        for sub in DEFAULT_SUBREDDITS:
            try:
                count = self._fetch_subreddit(sub)
                total += count
            except Exception as e:
                logger.warning(f"[reddit] Failed to fetch r/{sub}: {e}")
        return total

    def _fetch_subreddit(self, subreddit: str) -> int:
        url = f"https://www.reddit.com/r/{subreddit}/hot/.rss?limit={POSTS_PER_SUB}"
        with httpx.Client(timeout=30) as client:
            resp = client.get(url, headers={
                "User-Agent": "OctoData/1.0 (investment research bot)",
            })
            resp.raise_for_status()

        root = ET.fromstring(resp.text)
        entries = root.findall("atom:entry", NS)
        if not entries:
            return 0

        count = 0
        for entry in entries:
            title = entry.findtext("atom:title", "", NS).strip()
            if not title:
                continue

            link = entry.find("atom:link", NS)
            post_url = link.get("href", "") if link is not None else ""

            author_el = entry.find("atom:author/atom:name", NS)
            author = author_el.text.replace("/u/", "") if author_el is not None else ""

            published = entry.findtext("atom:published", "", NS)
            ts = self._parse_ts(published)

            post_id = entry.findtext("atom:id", "", NS)
            dedup_key = post_url or f"r/{subreddit}:{post_id}"

            try:
                self.db.execute("""
                    INSERT OR IGNORE INTO reddit_posts
                    (dedup_key, ts, subreddit, title, author, upvotes, comments, url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    dedup_key, ts, subreddit, title, author, 0, 0, post_url,
                ))
                count += 1
            except Exception as e:
                logger.warning(f"[reddit] Failed to save: {e}")

        self.db.commit()
        return count

    @staticmethod
    def _parse_ts(raw: str) -> str:
        if not raw:
            return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        try:
            dt = datetime.fromisoformat(raw)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return raw
