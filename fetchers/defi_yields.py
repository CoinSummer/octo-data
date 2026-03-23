"""DeFi Yields Fetcher — DefiLlama /pools 数据，过滤后入库"""

import logging
from datetime import datetime, timezone

import httpx

from .base import BaseFetcher

logger = logging.getLogger(__name__)

YIELDS_URL = "https://yields.llama.fi/pools"

# 资产分类关键词
STABLECOIN_KW = {
    "USD", "USDC", "USDT", "DAI", "FRAX", "LUSD", "GHO", "PYUSD",
    "USDM", "USDS", "USDE", "EURC", "FDUSD", "TUSD", "CRVUSD",
    "USDAI", "SUSDAI", "SYZUSD", "SUSDU", "USDD", "IUSD",
    "USDY", "USD1", "AUSD", "MSUSD", "YOUSD", "USDH", "PYUSD0",
    "USDG", "SAVUSD", "SNUSD", "APYUSD", "JRUSDE", "SUSDE",
}
ETH_KW = {
    "ETH", "WETH", "STETH", "WSTETH", "RETH", "CBETH", "METH", "EETH", "WEETH",
    "SFRXETH", "EZETH", "RSETH", "PUFETH", "MSETH", "KHYPE", "VKHYPE",
}
BTC_KW = {"BTC", "WBTC", "TBTC", "CBBTC", "LBTC", "FBTC", "BTCB", "UBTC"}

MIN_TVL = 10_000_000  # $10M
APY_THRESHOLDS = {"usd": 7, "eth": 4, "btc": 2}

EXCLUDE_LP_PROJECTS = {
    "uniswap-v3", "uniswap-v4", "pancakeswap-v3", "sushiswap-v3",
    "camelot-v3", "aerodrome-v2", "velodrome-v3", "thruster-v3",
    "fenix-v3", "merchant-moe-v2", "trader-joe-v2.1", "ramses-v2",
    "lynex-v3", "orca", "raydium",
}


def classify(symbol: str, is_stable: bool) -> str:
    tokens = [t.strip().upper() for t in symbol.replace("/", "-").split("-")]
    if is_stable:
        return "usd"
    if any(t in STABLECOIN_KW for t in tokens) and not any(t in ETH_KW | BTC_KW for t in tokens):
        return "usd"
    has_eth = any(t in ETH_KW for t in tokens)
    has_btc = any(t in BTC_KW for t in tokens)
    if has_eth and not has_btc:
        return "eth"
    if has_btc and not has_eth:
        return "btc"
    sym_upper = symbol.upper()
    for kw in ETH_KW:
        if kw in sym_upper:
            return "eth"
    for kw in BTC_KW:
        if kw in sym_upper:
            return "btc"
    for kw in STABLECOIN_KW:
        if kw in sym_upper:
            return "usd"
    return "other"


class DefiYieldsFetcher(BaseFetcher):
    name = "defi_yields"
    interval_seconds = 60 * 60  # 1 小时

    def _run(self) -> int:
        client = httpx.Client(timeout=60)
        resp = client.get(YIELDS_URL)
        resp.raise_for_status()
        pools = resp.json().get("data", [])
        client.close()

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        count = 0
        for p in pools:
            tvl = p.get("tvlUsd") or 0
            apy = p.get("apy") or 0
            if tvl < MIN_TVL or apy <= 0:
                continue
            if p.get("outlier", False):
                continue
            if p.get("ilRisk") == "yes":
                continue
            project = p.get("project", "")
            if project in EXCLUDE_LP_PROJECTS:
                continue

            asset_type = classify(p.get("symbol", ""), p.get("stablecoin", False))
            if asset_type == "other":
                continue
            if apy < APY_THRESHOLDS.get(asset_type, 99):
                continue

            pool_id = p.get("pool", "")
            if not pool_id:
                continue

            try:
                self.db.execute("""
                    INSERT INTO defi_yields
                    (pool_id, snapshot_date, ts, chain, project, symbol, pool_meta,
                     tvl_usd, apy, apy_base, apy_reward, apy_mean_30d, apy_pct_7d,
                     il_risk, stablecoin, asset_type, volume_1d, pool_age)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(pool_id, snapshot_date) DO UPDATE SET
                        ts = excluded.ts,
                        tvl_usd = excluded.tvl_usd,
                        apy = excluded.apy,
                        apy_base = excluded.apy_base,
                        apy_reward = excluded.apy_reward,
                        apy_mean_30d = excluded.apy_mean_30d,
                        apy_pct_7d = excluded.apy_pct_7d,
                        il_risk = excluded.il_risk,
                        volume_1d = excluded.volume_1d,
                        pool_age = excluded.pool_age
                """, (
                    pool_id,
                    today,
                    now,
                    p.get("chain", ""),
                    p.get("project", ""),
                    p.get("symbol", ""),
                    p.get("poolMeta"),
                    tvl,
                    apy,
                    p.get("apyBase"),
                    p.get("apyReward"),
                    p.get("apyMean30d"),
                    p.get("apyPct7D"),
                    p.get("ilRisk", "yes"),
                    1 if p.get("stablecoin", False) else 0,
                    asset_type,
                    p.get("volumeUsd1d"),
                    p.get("count"),
                ))
                count += 1
            except Exception as e:
                logger.warning(f"[defi_yields] Failed to save {pool_id}: {e}")

        self.db.commit()
        return count
