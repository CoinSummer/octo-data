"""OKX 公告 Fetcher — 轮询 REST API

数据源：https://www.okx.com/api/v5/support/announcements（公开，无需认证）
写入 announcements 表，source='okx'。
"""

import logging
from datetime import datetime

import httpx

from .base import BaseFetcher

logger = logging.getLogger(__name__)

API_URL = "https://www.okx.com/api/v5/support/announcements"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


def _ms_to_ts(ms_str: str) -> str:
    """毫秒时间戳 → 'YYYY-MM-DD HH:MM:SS'"""
    return datetime.utcfromtimestamp(int(ms_str) / 1000).strftime("%Y-%m-%d %H:%M:%S")


def _ann_type_to_catalog(ann_type: str) -> str:
    """'announcements-new-listings' → 'New listings' 风格的 catalog_name。"""
    # 去掉 announcements- 前缀，title case
    name = ann_type.removeprefix("announcements-").removeprefix("latest-")
    return name.replace("-", " ").strip().title()


class OKXAnnouncementsFetcher(BaseFetcher):
    """OKX 公告 — 轮询 REST API，每 30 分钟。"""

    name = "okx_announcements"
    interval_seconds = 1800  # 30min

    def _run(self) -> int:
        resp = httpx.get(
            API_URL,
            headers={"User-Agent": USER_AGENT},
            timeout=15,
        )
        resp.raise_for_status()

        payload = resp.json()
        if payload.get("code") != "0":
            logger.warning(f"[okx_announcements] API error: {payload.get('msg')}")
            return 0

        count = 0
        for group in payload.get("data", []):
            for item in group.get("details", []):
                ts = _ms_to_ts(item["pTime"])
                ann_type = item.get("annType", "")
                catalog = _ann_type_to_catalog(ann_type)
                title = item.get("title", "").strip()
                url = item.get("url", "")

                if not title:
                    continue

                self.db.execute("""
                    INSERT OR IGNORE INTO announcements
                        (ts, catalog_id, catalog_name, title, body, body_text, code, source, url)
                    VALUES (?, 0, ?, ?, '', '', ?, 'okx', ?)
                """, (ts, catalog, title, ann_type, url))
                count += 1

        self.db.commit()
        return count
