"""BTC/ETH Dominance Fetcher — CoinGecko"""

import logging
from datetime import datetime, timezone

import httpx

from config import COINGECKO_API_KEY, COINGECKO_BASE_URL
from db import Database
from .base import BaseFetcher

logger = logging.getLogger(__name__)


class DominanceFetcher(BaseFetcher):
    name = "dominance"
    interval_seconds = 60 * 60  # 1 小时

    def _run(self) -> int:
        headers = {}
        if COINGECKO_API_KEY:
            headers["x-cg-pro-api-key"] = COINGECKO_API_KEY

        url = f"{COINGECKO_BASE_URL}/global"

        with httpx.Client(timeout=15) as client:
            resp = client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        market_data = data.get("data", {}).get("market_cap_percentage", {})
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        count = 0

        for coin_id, pct in market_data.items():
            symbol = coin_id.upper()
            # 只存主要的
            if symbol not in ("BTC", "ETH", "USDT", "BNB", "SOL", "XRP", "USDC"):
                continue
            try:
                self.db.execute("""
                    INSERT OR REPLACE INTO dominance (ts, symbol, dominance_pct)
                    VALUES (?, ?, ?)
                """, (ts, symbol, pct))
                count += 1
            except Exception as e:
                logger.warning(f"[dominance] {symbol}: {e}")

        self.db.commit()
        return count
