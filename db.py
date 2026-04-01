"""SQLite 数据库管理 — 仅市场数据表"""

import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

from config import DB_PATH

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, path: Optional[Path] = None):
        self.path = path or DB_PATH
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_tables()
        logger.info(f"Database connected: {self.path}")

    def _init_tables(self):
        cur = self.conn.cursor()

        # ── 价格 ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP NOT NULL,
                symbol TEXT NOT NULL,
                price_usd REAL NOT NULL,
                market_cap REAL,
                volume_24h REAL,
                change_24h_pct REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_prices_dedup ON prices(symbol, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_symbol_ts ON prices(symbol, ts DESC)")

        # ── 恐贪指数 ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS fear_greed (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP NOT NULL,
                value INTEGER NOT NULL,
                label TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_fg_dedup ON fear_greed(ts)")

        # ── 资金费率 ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS funding_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP NOT NULL,
                symbol TEXT NOT NULL,
                rate REAL NOT NULL,
                exchange TEXT DEFAULT 'binance',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_fr_dedup ON funding_rates(symbol, ts, exchange)")

        # ── 稳定币供应 ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS stablecoin (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP NOT NULL,
                symbol TEXT NOT NULL,
                total_supply REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sc_dedup ON stablecoin(symbol, ts)")

        # ── BTC Dominance ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dominance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP NOT NULL,
                symbol TEXT NOT NULL,
                dominance_pct REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dom_dedup ON dominance(symbol, ts)")

        # ── DeFi TVL ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS defi_tvl (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP NOT NULL,
                chain TEXT NOT NULL,
                tvl_usd REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_tvl_dedup ON defi_tvl(chain, ts)")

        # ── DeFi Yields ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS defi_yields (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pool_id TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                ts TIMESTAMP NOT NULL,
                chain TEXT NOT NULL,
                project TEXT NOT NULL,
                symbol TEXT NOT NULL,
                pool_meta TEXT,
                tvl_usd REAL NOT NULL,
                apy REAL,
                apy_base REAL,
                apy_reward REAL,
                apy_mean_30d REAL,
                apy_pct_7d REAL,
                il_risk TEXT,
                stablecoin INTEGER DEFAULT 0,
                asset_type TEXT,
                volume_1d REAL,
                pool_age INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dy_dedup ON defi_yields(pool_id, snapshot_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dy_date ON defi_yields(snapshot_date DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dy_project ON defi_yields(project, snapshot_date DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dy_chain ON defi_yields(chain, snapshot_date DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dy_asset ON defi_yields(asset_type, snapshot_date DESC)")

        # ── 交易所公告（Binance/OKX/Hyperliquid） ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP NOT NULL,
                catalog_id INTEGER,
                catalog_name TEXT NOT NULL,
                title TEXT NOT NULL,
                body TEXT,
                body_text TEXT,
                code TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ann_ts ON announcements(ts DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ann_catalog ON announcements(catalog_name, ts DESC)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ann_dedup2 ON announcements(title, ts, source)")
        # 清理旧索引（不含 source）
        try:
            cur.execute("DROP INDEX IF EXISTS idx_ann_dedup")
        except Exception:
            pass

        # 确保 announcements 有 topics 列（兼容旧 DB）
        try:
            cur.execute("ALTER TABLE announcements ADD COLUMN topics TEXT DEFAULT ''")
        except Exception:
            pass  # 已存在

        # 确保 announcements 有 source 列（binance/hyperliquid，兼容旧 DB）
        try:
            cur.execute("ALTER TABLE announcements ADD COLUMN source TEXT DEFAULT 'binance'")
        except Exception:
            pass  # 已存在

        # 确保 announcements 有 url 列
        try:
            cur.execute("ALTER TABLE announcements ADD COLUMN url TEXT DEFAULT ''")
        except Exception:
            pass  # 已存在

        # 确保 announcements 有 entities 列
        try:
            cur.execute("ALTER TABLE announcements ADD COLUMN entities TEXT DEFAULT ''")
        except Exception:
            pass  # 已存在

        # ── News（一般媒体，从 announcements 拆出） ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP NOT NULL,
                catalog_id INTEGER,
                catalog_name TEXT NOT NULL,
                title TEXT NOT NULL,
                body TEXT,
                body_text TEXT,
                code TEXT DEFAULT '',
                source TEXT DEFAULT '',
                url TEXT DEFAULT '',
                topics TEXT DEFAULT '',
                entities TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_news_ts ON news(ts DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_news_source ON news(source, ts DESC)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_news_dedup ON news(title, ts, source)")

        # ── Tweets (KB API) ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS tweets (
                id TEXT PRIMARY KEY,
                ts TIMESTAMP NOT NULL,
                content TEXT NOT NULL,
                username TEXT NOT NULL,
                group_id TEXT,
                tags TEXT,
                source_url TEXT,
                reference TEXT,
                topics TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tweets_ts ON tweets(ts DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tweets_user ON tweets(username, ts DESC)")

        # 确保 tweets 有 entities 列
        try:
            cur.execute("ALTER TABLE tweets ADD COLUMN entities TEXT DEFAULT ''")
        except Exception:
            pass

        # ── KB News ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS kb_news (
                id TEXT PRIMARY KEY,
                ts TIMESTAMP NOT NULL,
                subject TEXT NOT NULL,
                source_name TEXT,
                source_url TEXT,
                content TEXT,
                topics TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_kbn_ts ON kb_news(ts DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_kbn_source ON kb_news(source_name, ts DESC)")

        # 确保 kb_news 有 entities 列
        try:
            cur.execute("ALTER TABLE kb_news ADD COLUMN entities TEXT DEFAULT ''")
        except Exception:
            pass

        # ── Reddit ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS reddit_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dedup_key TEXT NOT NULL UNIQUE,
                ts TIMESTAMP NOT NULL,
                subreddit TEXT NOT NULL,
                title TEXT NOT NULL,
                author TEXT,
                upvotes INTEGER DEFAULT 0,
                comments INTEGER DEFAULT 0,
                url TEXT,
                topics TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reddit_ts ON reddit_posts(ts DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reddit_sub ON reddit_posts(subreddit, ts DESC)")

        # 确保 reddit_posts 有 entities 列
        try:
            cur.execute("ALTER TABLE reddit_posts ADD COLUMN entities TEXT DEFAULT ''")
        except Exception:
            pass

        # ── Polymarket ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS polymarket_markets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP NOT NULL,
                slug TEXT NOT NULL,
                market_id TEXT NOT NULL,
                question TEXT NOT NULL,
                yes_price REAL NOT NULL,
                volume REAL DEFAULT 0,
                volume_24h REAL DEFAULT 0,
                liquidity REAL DEFAULT 0,
                change_24h REAL DEFAULT 0,
                change_1w REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_pm_dedup ON polymarket_markets(market_id, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pm_ts ON polymarket_markets(ts DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pm_slug ON polymarket_markets(slug, ts DESC)")

        # ── 系统 ──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS fetcher_status (
                name TEXT PRIMARY KEY,
                last_run TIMESTAMP,
                last_success TIMESTAMP,
                last_error TEXT,
                run_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0
            )
        """)

        # ── 交易所指标（DEX/CEX 渗透率 + HYPE 份额）──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS exchange_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP NOT NULL,
                dex_deriv_vol_24h REAL,
                bn_futures_vol_24h REAL,
                hype_vol_24h REAL,
                hype_oi REAL,
                hype_vol_share REAL,
                hype_oi_share REAL,
                hype_noncrypto_pct REAL,
                hype_stock_vol_24h REAL,
                dex_bn_penetration REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_em_dedup ON exchange_metrics(ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_em_ts ON exchange_metrics(ts DESC)")

        # ── 事件记忆（三层河流模型：structural/trend/event）──

        cur.execute("""
            CREATE TABLE IF NOT EXISTS monitor_events (
                event_key TEXT PRIMARY KEY,
                level TEXT NOT NULL DEFAULT 'event',
                title TEXT NOT NULL,
                facts TEXT NOT NULL DEFAULT '[]',
                first_seen TIMESTAMP NOT NULL,
                last_pushed TIMESTAMP NOT NULL,
                status TEXT NOT NULL DEFAULT 'active'
            )
        """)

        # 兼容旧表：加 level 列
        try:
            cur.execute("ALTER TABLE monitor_events ADD COLUMN level TEXT NOT NULL DEFAULT 'event'")
        except Exception:
            pass

        self.conn.commit()

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def executemany(self, sql: str, params: List[tuple]) -> sqlite3.Cursor:
        return self.conn.executemany(sql, params)

    def commit(self):
        self.conn.commit()

    def fetchall(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        cur = self.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]

    def fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        cur = self.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None

    def update_fetcher_status(self, name: str, success: bool, error: Optional[str] = None):
        if success:
            self.execute("""
                INSERT INTO fetcher_status (name, last_run, last_success, run_count, error_count)
                VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, 0)
                ON CONFLICT(name) DO UPDATE SET
                    last_run = CURRENT_TIMESTAMP,
                    last_success = CURRENT_TIMESTAMP,
                    run_count = run_count + 1
            """, (name,))
        else:
            self.execute("""
                INSERT INTO fetcher_status (name, last_run, last_error, run_count, error_count)
                VALUES (?, CURRENT_TIMESTAMP, ?, 1, 1)
                ON CONFLICT(name) DO UPDATE SET
                    last_run = CURRENT_TIMESTAMP,
                    last_error = ?,
                    run_count = run_count + 1,
                    error_count = error_count + 1
            """, (name, error, error))
        self.commit()

    def table_stats(self) -> List[Dict[str, Any]]:
        tables = [
            "prices", "fear_greed", "funding_rates", "stablecoin",
            "dominance", "defi_tvl", "defi_yields", "announcements",
            "polymarket_markets", "tweets", "kb_news", "reddit_posts",
            "exchange_metrics",
        ]
        stats = []
        for t in tables:
            row = self.fetchone(f"SELECT COUNT(*) as count FROM {t}")
            stats.append({"table": t, "count": row["count"] if row else 0})
        return stats

    def close(self):
        if self.conn:
            self.conn.close()
