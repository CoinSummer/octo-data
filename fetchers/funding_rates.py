"""资金费率 Fetcher — Binance"""

import logging
from datetime import datetime, timezone

import httpx

from db import Database
from .base import BaseFetcher

logger = logging.getLogger(__name__)

# Binance Futures premiumIndex 端点，返回当前资金费率
API_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"

# 监控的币种
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "SUIUSDT"]

SYMBOL_MAP = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "SOLUSDT": "SOL",
    "BNBUSDT": "BNB",
    "XRPUSDT": "XRP",
    "DOGEUSDT": "DOGE",
    "ADAUSDT": "ADA",
    "AVAXUSDT": "AVAX",
    "SUIUSDT": "SUI",
}


class FundingRatesFetcher(BaseFetcher):
    name = "funding_rates"
    interval_seconds = 15 * 60  # 15 分钟

    def _run(self) -> int:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        count = 0

        with httpx.Client(timeout=15) as client:
            for binance_symbol in SYMBOLS:
                try:
                    resp = client.get(API_URL, params={"symbol": binance_symbol})
                    resp.raise_for_status()
                    data = resp.json()

                    rate = float(data.get("lastFundingRate", 0))
                    symbol = SYMBOL_MAP.get(binance_symbol, binance_symbol.replace("USDT", ""))

                    self.db.execute("""
                        INSERT OR REPLACE INTO funding_rates (ts, symbol, rate, exchange)
                        VALUES (?, ?, ?, 'binance')
                    """, (ts, symbol, rate))
                    count += 1

                except Exception as e:
                    logger.warning(f"[funding_rates] {binance_symbol}: {e}")

        self.db.commit()
        return count
