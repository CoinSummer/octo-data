"""Hyperliquid 公告 Fetcher — 轮询 Telegram 公开频道

数据源：https://t.me/s/hyperliquid_announcements（服务端渲染 HTML，无需认证）
写入 announcements 表，source='hyperliquid' 区分 Binance。
"""

import logging
import re

import httpx

from .base import BaseFetcher, normalize_ts

logger = logging.getLogger(__name__)

CHANNEL_URL = "https://t.me/s/hyperliquid_announcements"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# 匹配单条消息块：从 data-post 到下一个 data-post 或页面结尾
_MSG_PATTERN = re.compile(
    r'data-post="hyperliquid_announcements/(\d+)"'  # post_id
    r'.*?'
    r'tgme_widget_message_text[^>]*>(.*?)</div>'     # 消息正文 HTML
    r'.*?'
    r'datetime="([^"]+)"',                            # ISO 时间戳
    re.DOTALL,
)


def _clean_html(html: str) -> str:
    """清理 HTML 标签，<br/> → 换行，去掉其余标签。"""
    text = re.sub(r'<br\s*/?>', '\n', html)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&quot;', '"', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&#\d+;', '', text)
    return text.strip()


def _parse_messages(html: str) -> list[dict]:
    """用 regex 从 Telegram 公开频道 HTML 提取消息。"""
    messages = []
    for m in _MSG_PATTERN.finditer(html):
        post_id = m.group(1)
        raw_text = m.group(2)
        ts_iso = m.group(3)
        text = _clean_html(raw_text)
        if text:
            messages.append({"post_id": post_id, "ts": ts_iso, "text": text})
    return messages


def _make_title(text: str, max_len: int = 120) -> str:
    """取第一行或前 max_len 字符作为 title。"""
    first_line = text.split("\n")[0].strip()
    if len(first_line) <= max_len:
        return first_line
    return first_line[:max_len - 3] + "..."


class HLAnnouncementsFetcher(BaseFetcher):
    """Hyperliquid 公告 — 轮询 Telegram 频道，每 30 分钟。"""

    name = "hl_announcements"
    interval_seconds = 1800  # 30min

    def _run(self) -> int:
        resp = httpx.get(
            CHANNEL_URL,
            headers={"User-Agent": USER_AGENT},
            timeout=15,
            follow_redirects=True,
        )
        resp.raise_for_status()

        messages = _parse_messages(resp.text)
        if not messages:
            logger.warning("[hl_announcements] No messages parsed")
            return 0

        count = 0
        for msg in messages:
            # 解析时间: 2026-03-19T07:49:46+00:00 → YYYY-MM-DD HH:MM:SS
            ts = normalize_ts(msg["ts"])
            title = _make_title(msg["text"])
            body_text = msg["text"]
            post_id = msg["post_id"]

            self.db.execute("""
                INSERT OR IGNORE INTO announcements
                    (ts, catalog_id, catalog_name, title, body, body_text, code, source)
                VALUES (?, 0, 'Hyperliquid', ?, '', ?, ?, 'hyperliquid')
            """, (ts, title, body_text, post_id))
            count += 1

        self.db.commit()
        return count
