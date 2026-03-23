# Architecture

## Overview

Octo Data is a standalone crypto market data aggregator. It runs as a single process with three subsystems:

```
External Data Sources              Octo Data                          Consumers
──────────────────               ──────────────                     ─────────

CoinGecko ─────┐     ┌─ Fetchers (13, scheduled) ──┐
Binance WS ────┤     │  prices / funding_rates     │
DefiLlama ─────┤     │  defi_tvl / defi_yields     │
Chainbot ──────┤ ──→ │  tweets / kb_news           ├──→ SQLite (WAL)
Polymarket ────┤     │  announcements (WebSocket)   │        │
Reddit RSS ────┤     │  polymarket / reddit         │        ↓
alternative.me ┘     └────────────────────────────┘   FastAPI :8420
                                                          │
                     ┌─ Classifier (30min) ──────┐        ├──→ Claude Skills
                     │  haiku auto-tags text     │        ├──→ Agents
                     │  topics: defi/earn/crypto │        ├──→ External services
                     │  /stock/macro/ai          │        └──→ query.py (CLI)
                     └───────────────────────────┘
```

**One sentence**: each data type gets its own table, fetcher, and API route. Adding a new data source = one table + one fetcher + one route.

## Components

| Component | File(s) | Responsibility |
|-----------|---------|----------------|
| **Database** | `db.py` | SQLite connection, schema creation, WAL mode, dedup indexes |
| **Fetchers** | `fetchers/*.py` | Scheduled data collection from external APIs |
| **Classifier** | `classifier.py` | Auto-tag text records with topics via Claude Haiku |
| **API Server** | `api.py` | FastAPI REST API (27 endpoints, read-only) |
| **Scheduler** | `scheduler.py` | APScheduler for periodic fetcher runs |
| **CLI** | `query.py` | Direct DB queries for local use |
| **Orchestrator** | `main.py` | Starts scheduler + API + WebSocket + optional integrations |

## Data Flow

```
15min  ─── prices ──────────┐
15min  ─── funding_rates ───┤
 1h   ─── fear_greed ──────┤
 1h   ─── stablecoin ──────┤      ┌────────┐  ┌─────────┐
 1h   ─── dominance ──────┼──▶  │ SQLite │──▶│  API    │
 1h   ─── defi_tvl ───────┤      │  (WAL) │  │  :8420  │
 1h   ─── defi_yields ────┤      └────────┘  └─────────┘
 1h   ─── polymarket ─────┤          │
30min  ─── tweets ─────────┤          ▼
30min  ─── kb_news ────────┤   query.py (CLI)
30min  ─── hl_announcements┤
 1h   ─── reddit ─────────┤
Real-time ─ announcements ─┘   (Binance WebSocket)

30min  ─── classifier ──▶ auto-tag text with topics
```

## Database Schema

14 tables, all with dedup indexes to prevent duplicate inserts.

### Numeric Data

| Table | Key Columns | Dedup Index | Source |
|-------|-------------|-------------|--------|
| `prices` | symbol, price_usd, market_cap, volume_24h, change_24h_pct | (symbol, ts) | CoinGecko |
| `funding_rates` | symbol, rate, exchange | (symbol, ts, exchange) | Binance Futures |
| `fear_greed` | value (0-100), label | (ts) | alternative.me |
| `stablecoin` | symbol, total_supply | (symbol, ts) | DefiLlama |
| `dominance` | symbol, dominance_pct | (symbol, ts) | CoinGecko |
| `defi_tvl` | chain, tvl_usd | (chain, ts) | DefiLlama |
| `defi_yields` | pool_id, chain, project, symbol, tvl_usd, apy, apy_base, apy_reward, il_risk, asset_type | (pool_id, snapshot_date) | DefiLlama |
| `polymarket_markets` | slug, market_id, question, yes_price, volume, change_24h | (market_id, ts) | Polymarket |

### Text Data

| Table | Key Columns | Dedup Index | Source |
|-------|-------------|-------------|--------|
| `announcements` | catalog_name, title, body_text, source, topics | (title, ts) | Binance WS + Hyperliquid TG |
| `tweets` | username, content, tags, topics | (id) PK | Chainbot API |
| `kb_news` | subject, source_name, content, topics | (id) PK | Chainbot API |
| `reddit_posts` | subreddit, title, author, url, topics | (dedup_key) UNIQUE | Reddit RSS |

### System

| Table | Purpose |
|-------|---------|
| `fetcher_status` | Per-fetcher health: last_run, last_success, last_error, run_count, error_count |

## Fetcher Design

All fetchers inherit from `BaseFetcher` (`fetchers/base.py`):

```python
class BaseFetcher:
    name: str               # Unique ID, matches fetcher_status.name
    interval_seconds: int   # Schedule interval

    def fetch_and_save(self) -> int:
        """Fetch from upstream, save to DB, return count of new records."""
        try:
            data = self._fetch()        # HTTP call to upstream API
            count = self._save(data)    # INSERT OR IGNORE into DB
            self.db.update_fetcher_status(self.name, success=True)
            return count
        except Exception as e:
            self.db.update_fetcher_status(self.name, success=False, error=str(e))
            raise
```

Key design decisions:
- **INSERT OR IGNORE** with unique indexes prevents duplicates across restarts
- **WAL mode** allows concurrent reads while fetchers write
- Each fetcher is independent — one failure doesn't block others
- 3 consecutive failures are logged but don't stop the scheduler

## Classifier

Auto-tags text records (tweets, news, reddit, announcements) with topics every 30 minutes.

```
Untagged records → Claude Haiku → topics: "defi,earn" / "crypto,macro" / etc.
```

Topics: `defi`, `earn`, `crypto`, `stock`, `macro`, `ai`

The `/signals` API endpoint queries across all text tables filtered by these topics, enabling topic-based monitoring (e.g., "show me all DeFi/earn signals from the last 4 hours").

## API Design

All endpoints return `{"data": [...], "total": N}`.

Patterns:
- `/X/latest` — latest snapshot (most recent records per group)
- `/X?keyword=Y&from=Z` — filtered history
- `/X/search?keyword=Y` — text search

27 endpoints total. See README.md for the full list.

## Configuration

All configuration via environment variables (`.env` file, loaded by `config.py`):

| Variable | Required | Description |
|----------|----------|-------------|
| `COINGECKO_API_KEY` | Optional | Pro API for higher rate limits |
| `BINANCE_API_KEY` | Yes | For announcements WebSocket |
| `BINANCE_API_SECRET` | Yes | For announcements WebSocket |
| `CHAINBOT_API_KEY` | Yes | For tweets and news |
| `DATAHUB_PORT` | No | API port (default: 8420) |
| `DATAHUB_DB_PATH` | No | SQLite path (default: ./datahub-market.db) |
| `DATAHUB_COINS` | No | CoinGecko IDs to track (default: 14 coins) |
| `CORS_ORIGINS` | No | Allowed CORS origins (default: *) |
| `DATAHUB_SCRIPTS_DIR` | No | External scripts directory for optional integrations |

## Adding a New Data Source

1. Create `fetchers/my_source.py` inheriting `BaseFetcher`
2. Add a new table in `db.py` `_init_tables()`
3. Add API routes in `api.py`
4. Add CLI command in `query.py`
5. Register in `scheduler.py` `ALL_FETCHERS` and `main.py` imports

Each step is independent and follows the same pattern as existing fetchers.

## Deployment

### Local (development)

```bash
python main.py          # scheduler + API + WebSocket
python main.py --fetch prices  # single fetcher run (debug)
```

### Docker

```bash
docker build -t octo-data .
docker run -d -p 8420:8420 --env-file .env octo-data
```

### Production considerations

- SQLite works well for single-node deployments (~100K records/table)
- For multi-node or high-write scenarios, migrate to PostgreSQL/MySQL
- The `fetcher_status` table serves as a basic health check
- API is read-only (GET only), safe to expose behind a reverse proxy
