"""DeFi TVL Fetcher — DefiLlama"""

import logging
from datetime import datetime, timezone

import httpx

from db import Database
from .base import BaseFetcher

logger = logging.getLogger(__name__)

# DefiLlama 当前各链 TVL
CHAINS_URL = "https://api.llama.fi/v2/chains"


class DefiTvlFetcher(BaseFetcher):
    name = "defi_tvl"
    interval_seconds = 60 * 60  # 1 小时

    def _run(self) -> int:
        with httpx.Client(timeout=30) as client:
            resp = client.get(CHAINS_URL)
            resp.raise_for_status()
            chains = resp.json()

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        count = 0
        total_tvl = 0

        # 存 Top 链
        top_chains = {"Ethereum", "Solana", "BSC", "Tron", "Arbitrum", "Base", "Avalanche", "Polygon", "Optimism", "Sui"}

        for chain in chains:
            name = chain.get("name", "")
            tvl = chain.get("tvl", 0)
            total_tvl += tvl

            if name not in top_chains:
                continue

            try:
                self.db.execute("""
                    INSERT OR REPLACE INTO defi_tvl (ts, chain, tvl_usd)
                    VALUES (?, ?, ?)
                """, (ts, name, tvl))
                count += 1
            except Exception as e:
                logger.warning(f"[defi_tvl] {name}: {e}")

        # 存总 TVL
        self.db.execute("""
            INSERT OR REPLACE INTO defi_tvl (ts, chain, tvl_usd)
            VALUES (?, 'Total', ?)
        """, (ts, total_tvl))
        count += 1

        self.db.commit()
        return count
