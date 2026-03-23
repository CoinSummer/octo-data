---
name: datahub
description: Search the Octo Data database — an always-running aggregator at localhost:8420 that stores crypto prices, DeFi yields, TVL, funding rates, Binance announcements (listings/delistings/airdrops), tweets, news, Reddit, stablecoin supply, fear & greed index, and Polymarket predictions. Text data is auto-classified with topics (defi/earn/crypto/stock/macro/ai). Use this skill to look up recent tweets or posts, search who mentioned a token/protocol across all channels, check Binance new listings or airdrop announcements, get stablecoin supply snapshots, query Polymarket odds, pull fear & greed index, compare DeFi yields and TVL across chains, check funding rates, or query classified signals by topic.
---

# Octo Data Query Skill

Query the Octo Data service (localhost:8420) for market data, DeFi metrics, social signals, and prediction markets.

13 Fetchers on a schedule → SQLite (14 tables) → FastAPI :8420 (27 endpoints) + CLI.

## Data Sources

| Category | Source | Upstream API | Interval | Coverage |
|----------|--------|-------------|----------|----------|
| Prices | CoinGecko | CoinGecko Pro | 15min | Configured token list (see config.py PRICE_COINS) |
| Funding | Binance Futures | fapi.binance.com | 15min | Binance USDS-M perpetuals |
| Announcements | **Binance** | Binance WebSocket | Real-time | Binance English announcements (listings/delistings/airdrops/events) |
| Announcements | **Hyperliquid** | Telegram public channel | 30min | @hyperliquid_announcements |
| Tweets | Chainbot | Chainbot contents API | 30min | KOL/project accounts tracked by Chainbot |
| News | Chainbot (Odaily etc.) | Chainbot contents API | 30min | Chinese crypto media aggregated by Chainbot |
| Reddit | RSS feed | Reddit RSS | 1h | r/cryptocurrency, r/wallstreetbets |
| DeFi Yields | DefiLlama | yields.llama.fi/pools | 1h | All-chain DeFi pools (filtered) |
| DeFi TVL | DefiLlama | api.llama.fi/v2/chains | 1h | All-chain TVL |
| Stablecoin | DefiLlama | stablecoins.llama.fi | 1h | Major stablecoin supply |
| Fear & Greed | alternative.me | alternative.me/fng | 1h | Crypto Fear & Greed |
| Dominance | CoinGecko | CoinGecko Pro | 1h | BTC/ETH dominance |
| Predictions | Polymarket | Gamma API | 1h | Active Polymarket events |

## How to Call

### CLI (direct DB query)
```bash
python3 query.py <command> [subcommand] [options]
```

### HTTP API (when service is running)
```bash
curl -s http://localhost:8420/<endpoint> | python3 -m json.tool
```

## Command Reference

### prices — Crypto Prices

```bash
# Latest prices for all tracked tokens (sorted by market cap)
python3 query.py prices latest

# Specific token, last 24h (default)
python3 query.py prices BTC

# Specific token, custom window
python3 query.py prices ETH --hours 168
```

Output: `symbol, price_usd, change_24h_pct, ts`

### yields — DeFi Yields

```bash
# Top yields (latest snapshot, sorted by APY)
python3 query.py yields top

# Filter by asset type: usd, eth, btc
python3 query.py yields top --type usd

# Limit results
python3 query.py yields top --type eth --limit 10

# Single pool history (last 30 snapshots)
python3 query.py yields pool POOL_ID

# Project-level aggregation (pools count, total TVL, avg/max APY)
python3 query.py yields projects
```

### tvl — Chain-Level TVL

```bash
# Latest TVL by chain
python3 query.py tvl latest

# TVL changes over N hours
python3 query.py tvl changes --hours 24
```

### funding — Perpetual Funding Rates

```bash
# Latest rates for all symbols (sorted by |rate|)
python3 query.py funding latest

# Specific symbol history
python3 query.py funding BTC --hours 24
```

### announcements — Binance + Hyperliquid Announcements

```bash
# Latest 20 announcements (all sources)
python3 query.py announcements latest

# Filter by source: binance or hyperliquid
python3 query.py announcements latest --source hyperliquid
python3 query.py announcements latest --source binance

# Search by keyword (supports --source filter)
python3 query.py announcements search "launchpool"
python3 query.py announcements search "HIP" --source hyperliquid

# By catalog
python3 query.py announcements catalog "New Cryptocurrency Listing"

# Raw SQL for complex queries
python3 query.py sql "SELECT ts, title FROM announcements WHERE catalog_name = 'Airdrop' ORDER BY ts DESC LIMIT 10"
```

### tweets — Tweets (Chainbot API)

```bash
# Latest tweets
python3 query.py tweets latest

# Search by keyword
python3 query.py tweets search "morpho"

# By specific user
python3 query.py tweets user "pendle_fi"
```

### news — KB News (Chainbot API)

```bash
# Latest news
python3 query.py news latest

# Search by keyword
python3 query.py news search "Ethereum"
```

### reddit — Reddit Posts

```bash
python3 query.py reddit latest
python3 query.py reddit search "bitcoin"
```

### text — Cross-Source Text Search

Searches 4 tables: tweets, kb_news, reddit_posts, announcements.

```bash
# Keyword search across all sources
python3 query.py text "morpho"

# Custom time window
python3 query.py text "pendle" --hours 168
```

### signals — Topic-Classified Signals

Queries text records that have been classified by the auto-classifier. Useful for topic-based consumers.

```bash
# Default: earn,defi signals from last 4h
python3 query.py signals

# Specific topics
python3 query.py signals "earn,defi,crypto"

# Custom time window
python3 query.py signals "earn" --hours 24
```

### polymarket — Prediction Markets

```bash
python3 query.py polymarket           # Top 20 by volume
python3 query.py polymarket movers    # 24h big movers >5%
python3 query.py polymarket macro     # Macro signals
python3 query.py polymarket search "bitcoin"
python3 query.py polymarket slug SLUG_NAME
```

### fear — Fear & Greed Index

```bash
python3 query.py fear latest
```

### stablecoin — Stablecoin Supply

```bash
python3 query.py stablecoin latest
```

### sql — Raw SQL (SELECT only)

```bash
python3 query.py sql "SELECT COUNT(*) FROM tweets WHERE ts > datetime('now', '-24 hours')"
```

### status — System Health

```bash
python3 query.py status
```

## HTTP API (localhost:8420)

| Endpoint | Description |
|----------|-------------|
| `GET /prices/latest` | Latest prices (optional `?symbols=BTC,ETH`) |
| `GET /prices?symbol=BTC&from=2026-03-01` | Price history |
| `GET /funding-rates/latest` | Latest funding rates |
| `GET /fear-greed/latest` | Fear & greed |
| `GET /fear-greed?hours=168` | Fear & greed history |
| `GET /stablecoin/latest` | Stablecoin supply |
| `GET /dominance/latest` | Market dominance |
| `GET /defi-tvl/latest` | Chain TVL |
| `GET /defi-yields/latest` | Yields (filters: `chain`, `project`, `asset_type`, `min_tvl`, `min_apy`) |
| `GET /defi-yields/top?asset_type=usd` | Top yields (no IL, >$5M TVL) |
| `GET /defi-yields/pool/{pool_id}` | Pool history |
| `GET /announcements/latest` | Latest announcements |
| `GET /announcements?catalog=New+Cryptocurrency+Listing` | By catalog |
| `GET /tweets/latest` | Latest tweets |
| `GET /tweets?keyword=X&username=Y` | Tweet search |
| `GET /kb-news/latest` | Latest news |
| `GET /kb-news?keyword=X&source=Odaily` | News search |
| `GET /reddit/latest` | Latest Reddit posts |
| `GET /reddit?keyword=X&hours=24` | Reddit search |
| `GET /text/search?keyword=X&hours=24` | Cross-source text search |
| `GET /signals?topics=earn,defi&hours=4` | Topic-classified signals |
| `GET /polymarket/latest` | Top 20 predictions |
| `GET /polymarket/movers` | 24h big movers |
| `GET /polymarket/macro` | Macro signals |
| `GET /polymarket/search?keyword=bitcoin` | Search |
| `GET /status` | System health |
| `GET /tables` | List all DB tables |

All endpoints return `{"data": [...], "total": N}`. 27 endpoints total.

## Important Notes

- **Data freshness**: Prices/funding 15min, tweets/news 30min, yields/TVL/polymarket 1h. Check `status` for last fetch time.
- **Classifier**: Text records are auto-classified with topics (defi/earn/crypto/stock/macro/ai) every 30min via Claude Haiku.
- **Output format**: All commands return JSON.
- **Time zone**: All timestamps are UTC.
- **NOT for trading decisions**: Prices may lag up to 15 minutes. Use real-time APIs for buy/sell decisions.
