"""交易所指标 Fetcher — DEX/CEX 渗透率 + Hyperliquid 份额追踪"""

import logging
import time
from datetime import datetime, timezone

import httpx

from .base import BaseFetcher

logger = logging.getLogger(__name__)

HL_API = "https://api.hyperliquid.xyz/info"
BN_FAPI = "https://fapi.binance.com/fapi/v1"
DFL_API = "https://api.llama.fi"

# Builder market non-crypto categories
NONCRYPTO_CATS = {"stocks", "commodities", "indices", "fx"}


class ExchangeMetricsFetcher(BaseFetcher):
    name = "exchange_metrics"
    interval_seconds = 3600  # 1 hour

    def _run(self) -> int:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        with httpx.Client(timeout=30) as client:
            bn_vol = self._fetch_bn_volume(client)
            hl_meta = self._fetch_hl_meta(client)
            hl_cats = self._fetch_hl_categories(client)
            dfl_vol = self._fetch_dfl_derivatives(client)
            dfl_oi = self._fetch_dfl_oi(client)

            # Non-crypto volume from builder markets (candleSnapshot)
            noncrypto_vol, stock_vol = self._fetch_noncrypto_volume(client, hl_cats)

        # ── Compute metrics ──

        # Hyperliquid main market 24h volume & OI
        hype_main_vol = 0.0
        hype_oi = 0.0
        if hl_meta:
            for ctx in hl_meta[1]:
                hype_main_vol += float(ctx.get("dayNtlVlm", 0))
                hype_oi += float(ctx.get("openInterest", 0)) * float(ctx.get("markPx", 0))

        # Total HYPE volume = main market (crypto) + builder market (non-crypto)
        hype_vol_24h = hype_main_vol + noncrypto_vol

        # Non-crypto percentage
        hype_noncrypto_pct = 0.0
        if hype_vol_24h > 0:
            hype_noncrypto_pct = round(noncrypto_vol / hype_vol_24h * 100, 2)

        # DEX total volume (DefiLlama — latest day)
        dex_deriv_vol = 0.0
        hype_vol_share = 0.0
        if dfl_vol:
            chart = dfl_vol.get("totalDataChart", [])
            if chart:
                dex_deriv_vol = chart[-1][1] if len(chart[-1]) > 1 else 0
            for p in dfl_vol.get("protocols", []):
                if "hyperliquid" in p.get("name", "").lower():
                    p_vol = p.get("total24h", 0) or 0
                    if dex_deriv_vol > 0:
                        hype_vol_share = round(p_vol / dex_deriv_vol * 100, 2)
                    break

        # OI share from DefiLlama
        hype_oi_share = 0.0
        if dfl_oi:
            total_dex_oi = 0.0
            hype_oi_val = 0.0
            for p in dfl_oi.get("protocols", []):
                oi_val = p.get("total24h", 0) or 0
                total_dex_oi += oi_val
                if "hyperliquid" in p.get("name", "").lower():
                    hype_oi_val = oi_val
            if total_dex_oi > 0:
                hype_oi_share = round(hype_oi_val / total_dex_oi * 100, 2)

        # DEX/BN penetration
        dex_bn_penetration = 0.0
        if dex_deriv_vol > 0 and bn_vol > 0:
            dex_bn_penetration = round(dex_deriv_vol / (dex_deriv_vol + bn_vol) * 100, 2)

        # ── Write to DB ──
        self.db.execute("""
            INSERT OR REPLACE INTO exchange_metrics (
                ts, dex_deriv_vol_24h, bn_futures_vol_24h,
                hype_vol_24h, hype_oi, hype_vol_share, hype_oi_share,
                hype_noncrypto_pct, hype_stock_vol_24h, dex_bn_penetration
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ts, dex_deriv_vol, bn_vol,
            hype_vol_24h, hype_oi, hype_vol_share, hype_oi_share,
            hype_noncrypto_pct, stock_vol, dex_bn_penetration,
        ))
        self.db.commit()

        logger.info(
            f"[exchange_metrics] DEX/BN={dex_bn_penetration:.1f}% "
            f"HYPE_vol={hype_vol_share:.1f}% OI={hype_oi_share:.1f}% "
            f"non-crypto={hype_noncrypto_pct:.1f}% stock=${stock_vol:,.0f}"
        )
        return 1

    def _fetch_noncrypto_volume(self, client: httpx.Client, hl_cats) -> tuple:
        """Fetch builder market non-crypto volume via candleSnapshot (batched)."""
        if not hl_cats:
            return 0.0, 0.0

        noncrypto = [(c[0], c[1]) for c in hl_cats
                     if isinstance(c, list) and len(c) >= 2 and c[1] in NONCRYPTO_CATS]
        if not noncrypto:
            return 0.0, 0.0

        now = int(time.time() * 1000)
        start = now - 2 * 86400000  # 2 days back

        total_nc_vol = 0.0
        total_stock_vol = 0.0
        BATCH = 30

        for i in range(0, len(noncrypto), BATCH):
            batch = noncrypto[i:i + BATCH]
            for coin, cat in batch:
                try:
                    resp = client.post(HL_API, json={
                        "type": "candleSnapshot",
                        "req": {"coin": coin, "interval": "1d", "startTime": start, "endTime": now},
                    })
                    if resp.status_code == 429:
                        time.sleep(2)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    if data:
                        latest = data[-1]
                        vol = float(latest["v"]) * float(latest["c"])
                        total_nc_vol += vol
                        if cat == "stocks":
                            total_stock_vol += vol
                except Exception:
                    pass  # Individual ticker failures are non-critical
            if i + BATCH < len(noncrypto):
                time.sleep(0.5)

        return total_nc_vol, total_stock_vol

    def _fetch_bn_volume(self, client: httpx.Client) -> float:
        try:
            resp = client.get(f"{BN_FAPI}/ticker/24hr")
            resp.raise_for_status()
            return sum(float(t.get("quoteVolume", 0)) for t in resp.json())
        except Exception as e:
            logger.warning(f"[exchange_metrics] BN volume: {e}")
            return 0.0

    def _fetch_hl_meta(self, client: httpx.Client):
        try:
            resp = client.post(HL_API, json={"type": "metaAndAssetCtxs"})
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"[exchange_metrics] HL meta: {e}")
            return None

    def _fetch_hl_categories(self, client: httpx.Client):
        try:
            resp = client.post(HL_API, json={"type": "perpCategories"})
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"[exchange_metrics] HL categories: {e}")
            return None

    def _fetch_dfl_derivatives(self, client: httpx.Client):
        try:
            resp = client.get(
                f"{DFL_API}/overview/derivatives",
                params={
                    "excludeTotalDataChart": "false",
                    "excludeTotalDataChartBreakdown": "true",
                    "dataType": "dailyVolume",
                },
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"[exchange_metrics] DFL derivatives: {e}")
            return None

    def _fetch_dfl_oi(self, client: httpx.Client):
        try:
            resp = client.get(
                f"{DFL_API}/overview/open-interest",
                params={
                    "excludeTotalDataChart": "true",
                    "excludeTotalDataChartBreakdown": "true",
                },
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"[exchange_metrics] DFL OI: {e}")
            return None
