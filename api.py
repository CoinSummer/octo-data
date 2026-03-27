"""FastAPI 路由 — 仅市场数据"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from db import Database


def create_app(db: Database) -> FastAPI:
    app = FastAPI(title="DataHub Market", version="1.0.0")

    import os
    cors_origins = os.getenv("CORS_ORIGINS", "*").split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    # ── Prices ──

    @app.get("/prices/latest")
    def prices_latest(symbols: Optional[str] = Query(None, description="逗号分隔: BTC,ETH")):
        if symbols:
            symbol_list = [s.strip().upper() for s in symbols.split(",")]
            placeholders = ",".join("?" * len(symbol_list))
            rows = db.fetchall(f"""
                SELECT p.* FROM prices p
                INNER JOIN (
                    SELECT symbol, MAX(ts) as max_ts FROM prices
                    WHERE symbol IN ({placeholders})
                    GROUP BY symbol
                ) latest ON p.symbol = latest.symbol AND p.ts = latest.max_ts
            """, tuple(symbol_list))
        else:
            rows = db.fetchall("""
                SELECT p.* FROM prices p
                INNER JOIN (
                    SELECT symbol, MAX(ts) as max_ts FROM prices GROUP BY symbol
                ) latest ON p.symbol = latest.symbol AND p.ts = latest.max_ts
            """)
        return {"data": rows, "total": len(rows)}

    @app.get("/prices")
    def prices_history(
        symbol: str = Query(..., description="BTC"),
        start: Optional[str] = Query(None, alias="from"),
        to: Optional[str] = Query(None),
        limit: int = Query(500),
    ):
        sql = "SELECT * FROM prices WHERE symbol = ?"
        params = [symbol.upper()]
        if start:
            sql += " AND ts >= ?"
            params.append(start)
        if to:
            sql += " AND ts <= ?"
            params.append(to)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── Fear & Greed ──

    @app.get("/fear-greed/latest")
    def fear_greed_latest():
        row = db.fetchone("SELECT * FROM fear_greed ORDER BY ts DESC LIMIT 1")
        return {"data": row}

    @app.get("/fear-greed")
    def fear_greed_history(
        start: Optional[str] = Query(None, alias="from"),
        to: Optional[str] = Query(None),
        limit: int = Query(100),
    ):
        sql = "SELECT * FROM fear_greed WHERE 1=1"
        params = []
        if start:
            sql += " AND ts >= ?"
            params.append(start)
        if to:
            sql += " AND ts <= ?"
            params.append(to)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── Funding Rates ──

    @app.get("/funding-rates/latest")
    def funding_rates_latest(symbols: Optional[str] = Query(None)):
        if symbols:
            symbol_list = [s.strip().upper() for s in symbols.split(",")]
            placeholders = ",".join("?" * len(symbol_list))
            rows = db.fetchall(f"""
                SELECT f.* FROM funding_rates f
                INNER JOIN (
                    SELECT symbol, MAX(ts) as max_ts FROM funding_rates
                    WHERE symbol IN ({placeholders})
                    GROUP BY symbol
                ) latest ON f.symbol = latest.symbol AND f.ts = latest.max_ts
            """, tuple(symbol_list))
        else:
            rows = db.fetchall("""
                SELECT f.* FROM funding_rates f
                INNER JOIN (
                    SELECT symbol, MAX(ts) as max_ts FROM funding_rates GROUP BY symbol
                ) latest ON f.symbol = latest.symbol AND f.ts = latest.max_ts
            """)
        return {"data": rows, "total": len(rows)}

    # ── Stablecoin ──

    @app.get("/stablecoin/latest")
    def stablecoin_latest():
        rows = db.fetchall("""
            SELECT s.* FROM stablecoin s
            INNER JOIN (
                SELECT symbol, MAX(ts) as max_ts FROM stablecoin GROUP BY symbol
            ) latest ON s.symbol = latest.symbol AND s.ts = latest.max_ts
            ORDER BY s.total_supply DESC
        """)
        return {"data": rows, "total": len(rows)}

    # ── Dominance ──

    @app.get("/dominance/latest")
    def dominance_latest():
        rows = db.fetchall("""
            SELECT d.* FROM dominance d
            INNER JOIN (
                SELECT symbol, MAX(ts) as max_ts FROM dominance GROUP BY symbol
            ) latest ON d.symbol = latest.symbol AND d.ts = latest.max_ts
            ORDER BY d.dominance_pct DESC
        """)
        return {"data": rows, "total": len(rows)}

    # ── DeFi TVL ──

    @app.get("/defi-tvl/latest")
    def defi_tvl_latest():
        rows = db.fetchall("""
            SELECT t.* FROM defi_tvl t
            INNER JOIN (
                SELECT chain, MAX(ts) as max_ts FROM defi_tvl GROUP BY chain
            ) latest ON t.chain = latest.chain AND t.ts = latest.max_ts
            ORDER BY t.tvl_usd DESC
        """)
        return {"data": rows, "total": len(rows)}

    # ── DeFi Yields ──

    @app.get("/defi-yields/latest")
    def defi_yields_latest(
        chain: Optional[str] = Query(None),
        project: Optional[str] = Query(None),
        asset_type: Optional[str] = Query(None, description="usd, eth, btc"),
        min_tvl: Optional[float] = Query(None),
        min_apy: Optional[float] = Query(None),
        il_risk: Optional[str] = Query(None, description="no = 无 IL"),
        limit: int = Query(200),
    ):
        latest = db.fetchone("SELECT MAX(snapshot_date) as d FROM defi_yields")
        if not latest or not latest["d"]:
            return {"data": [], "total": 0}
        date = latest["d"]

        sql = "SELECT * FROM defi_yields WHERE snapshot_date = ?"
        params: list = [date]

        if chain:
            sql += " AND chain = ?"
            params.append(chain)
        if project:
            sql += " AND project = ?"
            params.append(project)
        if asset_type:
            sql += " AND asset_type = ?"
            params.append(asset_type)
        if min_tvl is not None:
            sql += " AND tvl_usd >= ?"
            params.append(min_tvl)
        if min_apy is not None:
            sql += " AND apy >= ?"
            params.append(min_apy)
        if il_risk is not None:
            sql += " AND il_risk = ?"
            params.append(il_risk)

        sql += " ORDER BY apy DESC LIMIT ?"
        params.append(limit)

        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows), "snapshot_date": date}

    @app.get("/defi-yields/pool/{pool_id}")
    def defi_yields_pool_history(pool_id: str, days: int = Query(30)):
        rows = db.fetchall("""
            SELECT snapshot_date, tvl_usd, apy, apy_base, apy_reward, apy_mean_30d
            FROM defi_yields
            WHERE pool_id = ?
            ORDER BY snapshot_date DESC
            LIMIT ?
        """, (pool_id, days))
        return {"data": rows, "pool_id": pool_id}

    @app.get("/defi-yields/top")
    def defi_yields_top(
        asset_type: Optional[str] = Query(None, description="usd, eth, btc"),
        limit: int = Query(20),
    ):
        latest = db.fetchone("SELECT MAX(snapshot_date) as d FROM defi_yields")
        if not latest or not latest["d"]:
            return {"data": [], "total": 0}
        date = latest["d"]

        sql = """
            SELECT * FROM defi_yields
            WHERE snapshot_date = ? AND il_risk = 'no' AND tvl_usd >= 5000000
        """
        params: list = [date]
        if asset_type:
            sql += " AND asset_type = ?"
            params.append(asset_type)
        sql += " ORDER BY apy DESC LIMIT ?"
        params.append(limit)

        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows), "snapshot_date": date}

    # ── Announcements ──

    @app.get("/announcements/latest")
    def announcements_latest(
        limit: int = Query(20),
        source: Optional[str] = Query(None, description="binance, hyperliquid, or okx"),
    ):
        sql = "SELECT id, ts, catalog_name, title, body_text, source, url, created_at FROM announcements WHERE 1=1"
        params = []
        if source:
            sql += " AND source = ?"
            params.append(source)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    @app.get("/announcements")
    def announcements_query(
        day: Optional[str] = Query(None),
        catalog: Optional[str] = Query(None, description="New Cryptocurrency Listing"),
        keyword: Optional[str] = Query(None),
        source: Optional[str] = Query(None, description="binance, hyperliquid, or okx"),
        limit: int = Query(50),
    ):
        sql = "SELECT id, ts, catalog_name, title, body_text, source, url, created_at FROM announcements WHERE 1=1"
        params = []

        if day:
            sql += " AND date(ts) = date(?)"
            params.append(day)
        if catalog:
            sql += " AND catalog_name = ?"
            params.append(catalog)
        if keyword:
            sql += " AND (title LIKE ? OR body_text LIKE ?)"
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        if source:
            sql += " AND source = ?"
            params.append(source)

        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)

        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── Polymarket ──

    @app.get("/polymarket/latest")
    def polymarket_latest(limit: int = Query(20)):
        latest_ts = db.fetchone("SELECT MAX(ts) as t FROM polymarket_markets")
        if not latest_ts or not latest_ts["t"]:
            return {"data": [], "total": 0}
        rows = db.fetchall("""
            SELECT question, yes_price, change_24h, change_1w, volume, liquidity, slug
            FROM polymarket_markets WHERE ts = ?
            ORDER BY volume DESC LIMIT ?
        """, (latest_ts["t"], limit))
        return {"data": rows, "total": len(rows), "ts": latest_ts["t"]}

    @app.get("/polymarket/movers")
    def polymarket_movers():
        rows = db.fetchall("""
            SELECT question, yes_price, change_24h, volume, slug
            FROM polymarket_markets
            WHERE ts = (SELECT MAX(ts) FROM polymarket_markets)
              AND ABS(change_24h) > 0.05
            ORDER BY ABS(change_24h) DESC
        """)
        return {"data": rows, "total": len(rows)}

    @app.get("/polymarket/macro")
    def polymarket_macro():
        rows = db.fetchall("""
            SELECT question, yes_price, change_24h, volume, slug
            FROM polymarket_markets
            WHERE ts = (SELECT MAX(ts) FROM polymarket_markets)
              AND (slug LIKE '%recession%' OR slug LIKE '%fed%' OR slug LIKE '%tariff%'
                   OR slug LIKE '%bitcoin-reserve%' OR slug LIKE '%china%bitcoin%'
                   OR slug LIKE '%capital-gains%' OR slug LIKE '%microstrategy%'
                   OR question LIKE '%Bitcoin%150%' OR question LIKE '%USDC%USDT%')
            ORDER BY volume DESC
        """)
        return {"data": rows, "total": len(rows)}

    @app.get("/polymarket/search")
    def polymarket_search(keyword: str = Query(...)):
        rows = db.fetchall("""
            SELECT question, yes_price, change_24h, volume, slug
            FROM polymarket_markets
            WHERE ts = (SELECT MAX(ts) FROM polymarket_markets)
              AND (question LIKE ? OR slug LIKE ?)
            ORDER BY volume DESC LIMIT 20
        """, (f"%{keyword}%", f"%{keyword}%"))
        return {"data": rows, "total": len(rows)}

    # ── Tweets ──

    @app.get("/tweets/latest")
    def tweets_latest(limit: int = Query(30)):
        rows = db.fetchall(
            "SELECT id, ts, username, content, tags, source_url, topics FROM tweets ORDER BY ts DESC LIMIT ?",
            (limit,),
        )
        return {"data": rows, "total": len(rows)}

    @app.get("/tweets")
    def tweets_query(
        keyword: Optional[str] = Query(None),
        username: Optional[str] = Query(None),
        start: Optional[str] = Query(None, alias="from"),
        to: Optional[str] = Query(None),
        limit: int = Query(50),
    ):
        sql = "SELECT id, ts, username, content, tags, source_url, topics FROM tweets WHERE 1=1"
        params = []
        if keyword:
            sql += " AND content LIKE ?"
            params.append(f"%{keyword}%")
        if username:
            sql += " AND username = ?"
            params.append(username)
        if start:
            sql += " AND ts >= ?"
            params.append(start)
        if to:
            sql += " AND ts <= ?"
            params.append(to)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── KB News ──

    @app.get("/kb-news/latest")
    def kb_news_latest(limit: int = Query(30)):
        rows = db.fetchall(
            "SELECT id, ts, subject, source_name, content, source_url, topics FROM kb_news ORDER BY ts DESC LIMIT ?",
            (limit,),
        )
        return {"data": rows, "total": len(rows)}

    @app.get("/kb-news")
    def kb_news_query(
        keyword: Optional[str] = Query(None),
        source: Optional[str] = Query(None),
        start: Optional[str] = Query(None, alias="from"),
        to: Optional[str] = Query(None),
        limit: int = Query(50),
    ):
        sql = "SELECT id, ts, subject, source_name, content, source_url, topics FROM kb_news WHERE 1=1"
        params = []
        if keyword:
            sql += " AND (subject LIKE ? OR content LIKE ?)"
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        if source:
            sql += " AND source_name = ?"
            params.append(source)
        if start:
            sql += " AND ts >= ?"
            params.append(start)
        if to:
            sql += " AND ts <= ?"
            params.append(to)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── Reddit ──

    @app.get("/reddit/latest")
    def reddit_latest(limit: int = Query(30)):
        rows = db.fetchall(
            "SELECT ts, subreddit, title, author, url FROM reddit_posts ORDER BY ts DESC LIMIT ?",
            (limit,),
        )
        return {"data": rows, "total": len(rows)}

    @app.get("/reddit")
    def reddit_query(
        keyword: Optional[str] = Query(None),
        subreddit: Optional[str] = Query(None),
        limit: int = Query(50),
    ):
        sql = "SELECT ts, subreddit, title, author, url FROM reddit_posts WHERE 1=1"
        params = []
        if keyword:
            sql += " AND title LIKE ?"
            params.append(f"%{keyword}%")
        if subreddit:
            sql += " AND subreddit = ?"
            params.append(subreddit)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── 全文搜索 ──

    @app.get("/text/search")
    def text_search(keyword: str = Query(...), hours: int = Query(168), limit: int = Query(20)):
        results = []

        rows = db.fetchall(
            f"SELECT 'tweet' as source, ts, username as author, content as text FROM tweets "
            f"WHERE content LIKE ? AND ts >= datetime('now', '-{hours} hours') "
            f"ORDER BY ts DESC LIMIT ?",
            (f"%{keyword}%", limit),
        )
        results.extend(rows)

        rows = db.fetchall(
            f"SELECT 'news' as source, ts, source_name as author, subject as text FROM kb_news "
            f"WHERE (subject LIKE ? OR content LIKE ?) AND ts >= datetime('now', '-{hours} hours') "
            f"ORDER BY ts DESC LIMIT ?",
            (f"%{keyword}%", f"%{keyword}%", limit),
        )
        results.extend(rows)

        rows = db.fetchall(
            f"SELECT 'reddit' as source, ts, author, title as text FROM reddit_posts "
            f"WHERE title LIKE ? AND ts >= datetime('now', '-{hours} hours') "
            f"ORDER BY ts DESC LIMIT ?",
            (f"%{keyword}%", limit),
        )
        results.extend(rows)

        rows = db.fetchall(
            f"SELECT source, ts, catalog_name as author, title as text FROM announcements "
            f"WHERE (title LIKE ? OR body_text LIKE ?) AND ts >= datetime('now', '-{hours} hours') "
            f"ORDER BY ts DESC LIMIT ?",
            (f"%{keyword}%", f"%{keyword}%", limit),
        )
        results.extend(rows)

        results.sort(key=lambda x: x.get("ts", ""), reverse=True)
        return {"data": results, "total": len(results)}

    # ── Signals (跨表 topic 查询，供 monitor 等消费端使用) ──

    @app.get("/signals")
    def signals_query(
        topics: str = Query("earn,defi", description="逗号分隔的 topic 过滤"),
        entity: Optional[str] = Query(None, description="按实体过滤，逗号分隔"),
        since: Optional[str] = Query(None, description="起始时间 YYYY-MM-DD HH:MM:SS"),
        hours: int = Query(4, description="如果没指定 since，回溯 N 小时"),
        limit: int = Query(100),
    ):
        """跨表查询已分类的文本信号，按 topics/entities 过滤。"""
        topic_list = [t.strip() for t in topics.split(",") if t.strip()]
        topic_placeholders = " OR ".join("topics LIKE ?" for _ in topic_list)
        topic_params = [f"%{t}%" for t in topic_list]

        # 可选 entity 过滤
        entity_filter = ""
        entity_params = []
        if entity:
            entity_list = [e.strip().lower() for e in entity.split(",") if e.strip()]
            if entity_list:
                entity_filter = " AND (" + " OR ".join("entities LIKE ?" for _ in entity_list) + ")"
                entity_params = [f"%{e}%" for e in entity_list]

        if since:
            cutoff = since
        else:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')

        per_query_params = [cutoff] + topic_params + entity_params

        queries = []

        # announcements
        queries.append(f"""
            SELECT 'announcements' AS source, ts, topics, entities,
                   title || CASE WHEN body_text != '' AND url != body_text THEN char(10) || SUBSTR(body_text, 1, 2000) ELSE '' END AS text,
                   catalog_name || ' (' || source || ')' AS author,
                   COALESCE(NULLIF(url, ''),
                        CASE WHEN code != '' AND source = 'binance' THEN 'https://www.binance.com/en/support/announcement/' || code
                             WHEN code != '' AND source = 'hyperliquid' THEN 'https://t.me/hyperliquid_announcements/' || code
                             ELSE '' END) AS url
            FROM announcements
            WHERE ts > ? AND ({topic_placeholders}){entity_filter}
        """)

        # tweets
        queries.append(f"""
            SELECT 'tweets' AS source, ts, topics, entities, content AS text,
                   username AS author, source_url AS url
            FROM tweets
            WHERE ts > ? AND ({topic_placeholders}){entity_filter}
        """)

        # kb_news
        queries.append(f"""
            SELECT 'kb_news' AS source, ts, topics, entities,
                   subject || CASE WHEN content != '' THEN char(10) || SUBSTR(content, 1, 500) ELSE '' END AS text,
                   source_name AS author, source_url AS url
            FROM kb_news
            WHERE ts > ? AND ({topic_placeholders}){entity_filter}
        """)

        # reddit
        queries.append(f"""
            SELECT 'reddit' AS source, ts, topics, entities, title AS text, author, url
            FROM reddit_posts
            WHERE ts > ? AND ({topic_placeholders}){entity_filter}
        """)

        all_params = per_query_params * 4 + [limit]
        sql = " UNION ALL ".join(queries) + " ORDER BY ts DESC LIMIT ?"
        rows = db.fetchall(sql, tuple(all_params))
        return {"data": rows, "total": len(rows)}

    # ── Exchange Metrics ──

    @app.get("/exchange-metrics/latest")
    def exchange_metrics_latest():
        row = db.fetchone("SELECT * FROM exchange_metrics ORDER BY ts DESC LIMIT 1")
        return {"data": row}

    @app.get("/exchange-metrics")
    def exchange_metrics_history(
        start: Optional[str] = Query(None, alias="from"),
        to: Optional[str] = Query(None),
        limit: int = Query(720, description="默认 720 条 = 30 天"),
    ):
        sql = "SELECT * FROM exchange_metrics WHERE 1=1"
        params = []
        if start:
            sql += " AND ts >= ?"
            params.append(start)
        if to:
            sql += " AND ts <= ?"
            params.append(to)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── System ──

    @app.get("/status")
    def status():
        fetchers = db.fetchall("SELECT * FROM fetcher_status ORDER BY name")
        tables = db.table_stats()
        return {"fetchers": fetchers, "tables": tables}

    @app.get("/tables")
    def tables():
        return {"data": db.table_stats()}

    # ── Monitor Events ──

    @app.get("/events")
    def events(
        status: str = Query("active", description="active / expired / all"),
        limit: int = Query(50),
    ):
        """monitor_events 表：Crypto/DeFi Monitor 提取的事件记忆。"""
        if status == "all":
            rows = db.fetchall(
                "SELECT * FROM monitor_events ORDER BY last_pushed DESC LIMIT ?",
                (limit,),
            )
        else:
            rows = db.fetchall(
                "SELECT * FROM monitor_events WHERE status = ? ORDER BY last_pushed DESC LIMIT ?",
                (status, limit),
            )
        return {"data": rows}

    return app
