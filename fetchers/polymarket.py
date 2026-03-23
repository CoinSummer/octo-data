"""Polymarket 预测市场 Fetcher — Gamma API（公开，无需 API key）

拉取 crypto 相关活跃事件的赔率快照，存入 polymarket_markets 表。
用途：宏观情绪信号、政策预期概率、crypto 事件概率追踪。
"""

import json
import logging
from datetime import datetime, timezone

import httpx

from db import Database
from .base import BaseFetcher

logger = logging.getLogger(__name__)

GAMMA_API_BASE = "https://gamma-api.polymarket.com"

# 关注的 tag_id（按需扩展）
TAGS = {
    "crypto": 21,
    "bitcoin": 235,
    "politics": 9,         # 政策/选举
    "fed-interest-rates": 326,  # 联储利率
}

# 默认拉 crypto tag
FETCH_TAGS = [21]

# 额外关注的宏观话题（按 slug 搜索，覆盖 tag 搜不到的）
# 编辑这个列表即可增删关注话题
WATCHED_SLUGS = [
    "us-recession-in-2025",
    "us-recession-in-2026",
    "fed-funds-rate-december-2025",
    "fed-funds-rate-june-2026",
    "will-trump-impose-tariffs",
]

# 只保留有量的市场
MIN_VOLUME = 10_000  # $10K


class PolymarketFetcher(BaseFetcher):
    name = "polymarket"
    interval_seconds = 60 * 60  # 1 小时

    def _run(self) -> int:
        count = 0
        now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        seen_slugs = set()

        with httpx.Client(timeout=30) as client:
            # 1. Tag 搜索（crypto 等）
            for tag_id in FETCH_TAGS:
                events = self._fetch_events_by_tag(client, tag_id)
                for event in events:
                    seen_slugs.add(event.get("slug", ""))
                    count += self._save_event(event, now_str)

            # 2. Watched slug 搜索（宏观话题）
            for slug in WATCHED_SLUGS:
                if slug in seen_slugs:
                    continue
                events = self._fetch_events_by_slug(client, slug)
                for event in events:
                    count += self._save_event(event, now_str)

        return count

    def _fetch_events_by_tag(self, client: httpx.Client, tag_id: int) -> list:
        """从 Gamma API 按 tag 拉活跃事件"""
        try:
            resp = client.get(
                f"{GAMMA_API_BASE}/events",
                params={
                    "tag_id": tag_id,
                    "active": "true",
                    "closed": "false",
                    "limit": 50,
                },
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"[polymarket] tag={tag_id} fetch failed: {e}")
            return []

    def _fetch_events_by_slug(self, client: httpx.Client, slug: str) -> list:
        """按 slug 搜索单个事件"""
        try:
            resp = client.get(
                f"{GAMMA_API_BASE}/events",
                params={"slug": slug},
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"[polymarket] slug={slug} fetch failed: {e}")
            return []

    def _save_event(self, event: dict, ts: str) -> int:
        """解析事件的子市场并存入 DB"""
        slug = event.get("slug", "")
        event_title = event.get("title", "")
        saved = 0

        for m in event.get("markets", []):
            if m.get("closed", False):
                continue
            volume = float(m.get("volume", 0))
            if volume < MIN_VOLUME:
                continue

            # 解析 outcomePrices
            yes_price = self._parse_yes_price(m.get("outcomePrices", "[]"))
            change_24h = m.get("oneDayPriceChange") or 0

            self.db.execute("""
                INSERT OR REPLACE INTO polymarket_markets
                    (ts, slug, market_id, question, yes_price, volume, volume_24h,
                     liquidity, change_24h, change_1w)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ts,
                slug,
                str(m.get("id", "")),
                m.get("question", event_title),
                yes_price,
                volume,
                float(m.get("volume24hr", 0) if m.get("volume24hr") else event.get("volume24hr", 0)),
                float(m.get("liquidity", 0)),
                float(change_24h),
                float(m.get("oneWeekPriceChange", 0) or 0),
            ))
            saved += 1

        self.db.commit()
        return saved

    @staticmethod
    def _parse_yes_price(prices_str: str) -> float:
        try:
            prices = json.loads(prices_str)
            return float(prices[0]) if prices else 0
        except (json.JSONDecodeError, ValueError, TypeError, IndexError):
            return 0
