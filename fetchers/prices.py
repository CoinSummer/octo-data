"""CoinGecko 价格 Fetcher"""

import logging
from datetime import datetime, timezone

import httpx

from config import COINGECKO_API_KEY, COINGECKO_BASE_URL, PRICE_COINS
from db import Database
from .base import BaseFetcher

logger = logging.getLogger(__name__)

# CoinGecko ID → symbol 映射
COIN_SYMBOLS = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "hyperliquid": "HYPE",
    "binancecoin": "BNB",
    "ripple": "XRP",
    "cardano": "ADA",
    "dogecoin": "DOGE",
    "toncoin": "TON",
    "avalanche-2": "AVAX",
    "chainlink": "LINK",
    "sui": "SUI",
    "aave": "AAVE",
    "pendle": "PENDLE",
}


class PricesFetcher(BaseFetcher):
    name = "prices"
    interval_seconds = 15 * 60  # 15 分钟

    def __init__(self, db: Database):
        super().__init__(db)
        self.coins = PRICE_COINS

    def _run(self) -> int:
        headers = {}
        if COINGECKO_API_KEY:
            headers["x-cg-pro-api-key"] = COINGECKO_API_KEY

        ids = ",".join(self.coins)
        url = f"{COINGECKO_BASE_URL}/simple/price"
        params = {
            "ids": ids,
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true",
        }

        with httpx.Client(timeout=30) as client:
            resp = client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        count = 0

        for coin_id, values in data.items():
            symbol = COIN_SYMBOLS.get(coin_id, coin_id.upper())
            price = values.get("usd")
            if price is None:
                continue

            try:
                self.db.execute("""
                    INSERT OR REPLACE INTO prices (ts, symbol, price_usd, market_cap, volume_24h, change_24h_pct)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    ts,
                    symbol,
                    price,
                    values.get("usd_market_cap"),
                    values.get("usd_24h_vol"),
                    values.get("usd_24h_change"),
                ))
                count += 1
            except Exception as e:
                logger.warning(f"[prices] Failed to save {symbol}: {e}")

        self.db.commit()
        return count
