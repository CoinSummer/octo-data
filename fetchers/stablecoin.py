"""稳定币供应 Fetcher — DefiLlama"""

import logging
from datetime import datetime, timezone

import httpx

from db import Database
from .base import BaseFetcher

logger = logging.getLogger(__name__)

# DefiLlama stablecoins 端点
API_URL = "https://stablecoins.llama.fi/stablecoins?includePrices=false"

# 关注的稳定币（按 DefiLlama 名称）
WATCHED = {"USDT", "USDC", "DAI", "USDS", "USDe", "FDUSD", "PYUSD", "GHO"}


class StablecoinFetcher(BaseFetcher):
    name = "stablecoin"
    interval_seconds = 60 * 60  # 1 小时

    def _run(self) -> int:
        with httpx.Client(timeout=30) as client:
            resp = client.get(API_URL)
            resp.raise_for_status()
            data = resp.json()

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        count = 0

        for coin in data.get("peggedAssets", []):
            symbol = coin.get("symbol", "")
            if symbol not in WATCHED:
                continue

            # circulating 里的 peggedUSD 是总供应
            circ = coin.get("circulating", {})
            total = circ.get("peggedUSD", 0)
            if not total:
                continue

            try:
                self.db.execute("""
                    INSERT OR REPLACE INTO stablecoin (ts, symbol, total_supply)
                    VALUES (?, ?, ?)
                """, (ts, symbol, total))
                count += 1
            except Exception as e:
                logger.warning(f"[stablecoin] {symbol}: {e}")

        self.db.commit()
        return count
