"""Binance 公告 WebSocket Fetcher

从 cs-monitors 013 迁移，简化版：
- 不做 AI 筛选（存所有公告，筛选留给消费端）
- 不做 AI 总结（后续可加）
- 保留 WebSocket 连接 + 签名 + 心跳 + 重连
"""

import asyncio
import hmac
import hashlib
import json
import logging
import re
import time
import uuid
import threading
from datetime import datetime
from typing import Optional

import httpx
import websockets

from config import BINANCE_API_KEY, BINANCE_API_SECRET
from db import Database

logger = logging.getLogger(__name__)

WS_URL = "wss://api.binance.com/sapi/wss"
TOPIC = "com_announcement_en"
RECV_WINDOW = 5000
PING_INTERVAL = 30
RECONNECT_DELAY = 5


def _generate_signature(api_secret: str) -> dict:
    """生成 Binance WebSocket 连接签名"""
    timestamp = int(time.time() * 1000)
    random_str = str(uuid.uuid4())
    payload = f"random={random_str}&recvWindow={RECV_WINDOW}&timestamp={timestamp}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return {
        "timestamp": timestamp,
        "random": random_str,
        "recvWindow": RECV_WINDOW,
        "signature": signature,
    }


def _clean_html(html: str) -> str:
    """清理 HTML 标签，返回纯文本"""
    text = re.sub(r"<[^>]+>", "", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text


class AnnouncementsFetcher:
    """Binance 公告 WebSocket fetcher。独立线程运行。"""

    name = "announcements"

    def __init__(self, db: Database):
        self.db = db
        self._thread = None
        self._running = False

    def start(self):
        """在后台线程启动 WebSocket 监听"""
        if not BINANCE_API_KEY or not BINANCE_API_SECRET:
            logger.warning("[announcements] BINANCE_API_KEY/SECRET not configured, skipping")
            return

        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("[announcements] WebSocket monitor started in background thread")

    def stop(self):
        self._running = False

    def _run_loop(self):
        """在独立线程中运行 asyncio 事件循环"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._ws_loop())
        except Exception as e:
            logger.error(f"[announcements] Loop crashed: {e}")
        finally:
            loop.close()

    async def _ws_loop(self):
        """WebSocket 主循环，含重连逻辑"""
        reconnect_delay = RECONNECT_DELAY

        while self._running:
            try:
                await self._connect_and_listen()
                reconnect_delay = RECONNECT_DELAY  # 重置
            except Exception as e:
                logger.error(f"[announcements] Connection error: {e}, reconnecting in {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

    async def _connect_and_listen(self):
        """建立连接，订阅，监听消息"""
        # 构建签名 URL
        params = _generate_signature(BINANCE_API_SECRET)
        query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        url = f"{WS_URL}?{query}"
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

        async with websockets.connect(url, additional_headers=headers, ping_interval=None) as ws:
            logger.info("[announcements] WebSocket connected")

            # 订阅
            await ws.send(json.dumps({"command": "SUBSCRIBE", "value": TOPIC}))
            logger.info(f"[announcements] Subscribed to {TOPIC}")

            # 启动心跳
            ping_task = asyncio.create_task(self._ping_loop(ws))

            try:
                async for message in ws:
                    try:
                        data = json.loads(message)

                        # 跳过订阅确认
                        if data.get("result") is not None:
                            logger.debug(f"[announcements] Subscription response: {data}")
                            continue

                        if data.get("type") == "DATA":
                            self._handle_message(data)

                    except json.JSONDecodeError as e:
                        logger.warning(f"[announcements] Bad JSON: {e}")
            finally:
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass

    async def _ping_loop(self, ws):
        """心跳循环"""
        while True:
            await asyncio.sleep(PING_INTERVAL)
            try:
                await ws.ping()
            except Exception:
                break

    def _handle_message(self, raw: dict):
        """解析并存储公告"""
        try:
            data_str = raw.get("data", "")
            data = json.loads(data_str) if isinstance(data_str, str) else data_str

            # 解析时间
            publish_ts = data.get("publishDate", 0)
            if publish_ts > 0:
                ts = datetime.fromtimestamp(publish_ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            title = data.get("title", "")
            body = data.get("body", "")
            body_text = _clean_html(body)
            catalog_name = data.get("catalogName", "")
            code = data.get("code", "")

            self.db.execute("""
                INSERT OR IGNORE INTO announcements (ts, catalog_id, catalog_name, title, body, body_text, code)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                ts,
                data.get("catalogId", 0),
                catalog_name,
                title,
                body,
                body_text,
                code,
            ))
            self.db.commit()
            self.db.update_fetcher_status(self.name, success=True)

            # code 为空时尝试从 REST API 回填
            if not code and title:
                self._backfill_code(title)

            logger.info(f"[announcements] [{catalog_name}] {title}")

        except Exception as e:
            logger.error(f"[announcements] Failed to handle message: {e}")
            self.db.update_fetcher_status(self.name, success=False, error=str(e))

    def _backfill_code(self, title: str):
        """从 REST API 查找公告 code 并回填到 DB。"""
        try:
            url = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&pageNo=1&pageSize=10"
            headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"}
            with httpx.Client(timeout=10) as client:
                resp = client.get(url, headers=headers)
                resp.raise_for_status()
                data = resp.json()

            for catalog in data.get("data", {}).get("catalogs", []):
                for article in catalog.get("articles", []):
                    if article.get("title", "").strip() == title.strip():
                        code = article.get("code", "")
                        if code:
                            self.db.execute(
                                "UPDATE announcements SET code = ? WHERE title = ? AND code = ''",
                                (code, title),
                            )
                            self.db.commit()
                            logger.info(f"[announcements] Backfilled code for: {title[:50]}")
                            return
        except Exception as e:
            logger.debug(f"[announcements] Code backfill failed: {e}")
