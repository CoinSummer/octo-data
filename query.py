#!/usr/bin/env python3
"""DataHub Market CLI — 市场数据查询

用法:
    python3 query.py prices latest                  # 最新价格
    python3 query.py prices BTC --hours 24          # BTC 24h 价格
    python3 query.py yields top --type usd          # USD Top 收益池
    python3 query.py yields top --type eth --limit 10
    python3 query.py yields pool POOL_ID            # 单池历史
    python3 query.py yields projects                # 项目聚合
    python3 query.py tvl latest                     # 最新链级 TVL
    python3 query.py tvl changes --hours 24         # TVL 24h 变化
    python3 query.py funding latest                 # 最新资费
    python3 query.py funding BTC --hours 24         # BTC 资费历史
    python3 query.py fear latest                    # 恐贪指数
    python3 query.py stablecoin latest              # 稳定币供应
    python3 query.py announcements latest           # 最新公告
    python3 query.py announcements search "keyword" # 搜索公告
    python3 query.py polymarket                     # Top 20（按成交量）
    python3 query.py polymarket movers              # 24h 大波动 >5%
    python3 query.py polymarket macro               # 宏观信号
    python3 query.py polymarket search "bitcoin"    # 搜索关键词
    python3 query.py polymarket slug SLUG           # 按 slug 查历史
    python3 query.py tweets latest                   # 最新推文
    python3 query.py tweets search "keyword"        # 搜索推文
    python3 query.py tweets user "username"          # 指定用户推文
    python3 query.py news latest                    # 最新新闻
    python3 query.py news search "keyword"          # 搜索新闻
    python3 query.py reddit latest                  # 最新 Reddit
    python3 query.py text "keyword"                 # 跨表全文搜索
    python3 query.py sql "SELECT ..."               # 原始 SQL（只读）
    python3 query.py status                         # 系统状态
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from db import Database

DB_PATH = Path(__file__).parent / "datahub-market.db"


def get_db():
    db = Database(DB_PATH)
    db.connect()
    return db


def cmd_prices(args):
    db = get_db()
    if not args or args[0] == "latest":
        rows = db.fetchall("""
            SELECT p.symbol, p.price_usd, p.change_24h_pct, p.ts
            FROM prices p
            INNER JOIN (SELECT symbol, MAX(ts) as max_ts FROM prices GROUP BY symbol) latest
            ON p.symbol = latest.symbol AND p.ts = latest.max_ts
            ORDER BY p.market_cap DESC
        """)
    else:
        symbol = args[0].upper()
        hours = int(args[2]) if len(args) > 2 and args[1] == "--hours" else 24
        rows = db.fetchall(
            f"SELECT symbol, price_usd, change_24h_pct, ts FROM prices "
            f"WHERE symbol = ? AND ts >= datetime('now', '-{hours} hours') ORDER BY ts DESC",
            (symbol,)
        )
    return rows


def cmd_yields(args):
    db = get_db()
    sub = args[0] if args else "top"

    if sub == "top":
        asset_type = None
        limit = 20
        for i, a in enumerate(args[1:], 1):
            if a == "--type" and i < len(args):
                asset_type = args[i + 1]
            if a == "--limit" and i < len(args):
                limit = int(args[i + 1])

        latest = db.fetchone("SELECT MAX(snapshot_date) as d FROM defi_yields")
        if not latest or not latest["d"]:
            return []

        sql = """SELECT pool_id, chain, project, symbol, pool_meta, tvl_usd, apy, apy_base,
                        apy_reward, apy_mean_30d, apy_pct_7d, asset_type, pool_age
                 FROM defi_yields WHERE snapshot_date = ?"""
        params = [latest["d"]]

        if asset_type:
            sql += " AND asset_type = ?"
            params.append(asset_type)

        sql += " ORDER BY apy DESC LIMIT ?"
        params.append(limit)
        return db.fetchall(sql, tuple(params))

    elif sub == "pool":
        pool_id = args[1] if len(args) > 1 else ""
        return db.fetchall(
            "SELECT snapshot_date, tvl_usd, apy, apy_base, apy_reward FROM defi_yields "
            "WHERE pool_id = ? ORDER BY snapshot_date DESC LIMIT 30",
            (pool_id,)
        )

    elif sub == "projects":
        latest = db.fetchone("SELECT MAX(snapshot_date) as d FROM defi_yields")
        if not latest or not latest["d"]:
            return []
        return db.fetchall("""
            SELECT project, COUNT(*) as pools, SUM(tvl_usd) as total_tvl,
                   AVG(apy) as avg_apy, MAX(apy) as max_apy
            FROM defi_yields WHERE snapshot_date = ?
            GROUP BY project ORDER BY total_tvl DESC LIMIT 30
        """, (latest["d"],))

    return []


def cmd_tvl(args):
    db = get_db()
    sub = args[0] if args else "latest"

    if sub == "latest":
        return db.fetchall("""
            SELECT t.chain, t.tvl_usd, t.ts FROM defi_tvl t
            INNER JOIN (SELECT chain, MAX(ts) as max_ts FROM defi_tvl GROUP BY chain) latest
            ON t.chain = latest.chain AND t.ts = latest.max_ts
            ORDER BY t.tvl_usd DESC
        """)

    elif sub == "changes":
        hours = 24
        for i, a in enumerate(args[1:], 1):
            if a == "--hours" and i < len(args):
                hours = int(args[i + 1])

        return db.fetchall(f"""
            WITH latest AS (
                SELECT chain, tvl_usd as tvl_now, ts
                FROM defi_tvl WHERE ts = (SELECT MAX(ts) FROM defi_tvl)
            ),
            older AS (
                SELECT chain, tvl_usd as tvl_old
                FROM defi_tvl
                WHERE ts <= datetime((SELECT MAX(ts) FROM defi_tvl), '-{hours} hours')
                AND ts >= datetime((SELECT MAX(ts) FROM defi_tvl), '-{hours + 4} hours')
                GROUP BY chain
            )
            SELECT l.chain, l.tvl_now, o.tvl_old,
                   ROUND((l.tvl_now - o.tvl_old) / o.tvl_old * 100, 2) as change_pct
            FROM latest l JOIN older o ON l.chain = o.chain
            WHERE l.chain != 'Total'
            ORDER BY ABS(change_pct) DESC
        """)

    return []


def cmd_funding(args):
    db = get_db()
    if not args or args[0] == "latest":
        return db.fetchall("""
            SELECT f.symbol, f.rate, f.exchange, f.ts
            FROM funding_rates f
            INNER JOIN (SELECT symbol, MAX(ts) as max_ts FROM funding_rates GROUP BY symbol) latest
            ON f.symbol = latest.symbol AND f.ts = latest.max_ts
            ORDER BY ABS(f.rate) DESC
        """)
    else:
        symbol = args[0].upper()
        hours = int(args[2]) if len(args) > 2 and args[1] == "--hours" else 24
        return db.fetchall(
            f"SELECT symbol, rate, exchange, ts FROM funding_rates "
            f"WHERE symbol = ? AND ts >= datetime('now', '-{hours} hours') ORDER BY ts DESC",
            (symbol,)
        )


def cmd_fear(args):
    db = get_db()
    return [db.fetchone("SELECT * FROM fear_greed ORDER BY ts DESC LIMIT 1")]


def cmd_stablecoin(args):
    db = get_db()
    return db.fetchall("""
        SELECT s.symbol, s.total_supply, s.ts FROM stablecoin s
        INNER JOIN (SELECT symbol, MAX(ts) as max_ts FROM stablecoin GROUP BY symbol) latest
        ON s.symbol = latest.symbol AND s.ts = latest.max_ts
        ORDER BY s.total_supply DESC
    """)


def _parse_source_filter(args) -> tuple[str, list]:
    """解析 --source 参数，返回 (sql_clause, params)。"""
    for i, a in enumerate(args):
        if a == "--source" and i + 1 < len(args):
            return " AND source = ?", [args[i + 1]]
    return "", []


def cmd_announcements(args):
    db = get_db()
    sub = args[0] if args else "latest"
    source_clause, source_params = _parse_source_filter(args)

    if sub == "latest":
        limit = 20
        for i, a in enumerate(args[1:], 1):
            if a == "--limit" and i < len(args):
                limit = int(args[i + 1])
        return db.fetchall(
            f"SELECT id, ts, catalog_name, title, body_text, source FROM announcements"
            f" WHERE 1=1{source_clause} ORDER BY ts DESC LIMIT ?",
            (*source_params, limit),
        )

    elif sub == "search" and len(args) > 1:
        keyword = args[1]
        hours = 168
        for i, a in enumerate(args[2:], 2):
            if a == "--hours" and i < len(args):
                hours = int(args[i + 1])
        return db.fetchall(
            f"SELECT id, ts, catalog_name, title, source FROM announcements "
            f"WHERE (title LIKE ? OR body_text LIKE ?) AND ts >= datetime('now', '-{hours} hours')"
            f"{source_clause} ORDER BY ts DESC LIMIT 50",
            (f"%{keyword}%", f"%{keyword}%", *source_params),
        )

    elif sub == "catalog" and len(args) > 1:
        catalog = " ".join(a for a in args[1:] if a not in ("--source",) and not a.startswith("--"))
        return db.fetchall(
            f"SELECT ts, title, source FROM announcements WHERE catalog_name = ?{source_clause}"
            f" ORDER BY ts DESC LIMIT 20",
            (catalog, *source_params),
        )

    return []


def cmd_polymarket(args):
    db = get_db()
    sub = args[0] if args else "latest"

    if sub == "latest":
        latest_ts = db.fetchone("SELECT MAX(ts) as t FROM polymarket_markets")
        if not latest_ts or not latest_ts["t"]:
            return "No polymarket data yet"
        rows = db.fetchall("""
            SELECT question, yes_price, change_24h, change_1w, volume, liquidity, slug
            FROM polymarket_markets WHERE ts = ?
            ORDER BY volume DESC LIMIT 20
        """, (latest_ts["t"],))
        lines = [f"Polymarket Top 20 ({latest_ts['t'][:16]})", ""]
        for r in rows:
            pct = r["yes_price"] * 100
            chg = r["change_24h"] * 100
            vol = r["volume"]
            lines.append(f"  {pct:5.1f}%  {chg:+5.1f}%  ${vol/1e6:5.1f}M  {r['question'][:60]}")
        return "\n".join(lines)

    elif sub == "movers":
        rows = db.fetchall("""
            SELECT question, yes_price, change_24h, volume, slug
            FROM polymarket_markets
            WHERE ts = (SELECT MAX(ts) FROM polymarket_markets)
              AND ABS(change_24h) > 0.05
            ORDER BY ABS(change_24h) DESC
        """)
        if not rows:
            return "No big movers (>5% 24h change)"
        lines = ["Polymarket Big Movers (>5%)", ""]
        for r in rows:
            lines.append(f"  {r['yes_price']*100:5.1f}%  {r['change_24h']*100:+5.1f}%  ${r['volume']/1e6:.1f}M  {r['question'][:60]}")
        return "\n".join(lines)

    elif sub == "macro":
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
        if not rows:
            return "No macro markets found"
        lines = ["Polymarket Macro Signals", ""]
        for r in rows:
            lines.append(f"  {r['yes_price']*100:5.1f}%  {r['change_24h']*100:+5.1f}%  ${r['volume']/1e6:.1f}M  {r['question'][:65]}")
        return "\n".join(lines)

    elif sub == "search" and len(args) > 1:
        keyword = args[1]
        rows = db.fetchall("""
            SELECT question, yes_price, change_24h, volume, slug
            FROM polymarket_markets
            WHERE ts = (SELECT MAX(ts) FROM polymarket_markets)
              AND (question LIKE ? OR slug LIKE ?)
            ORDER BY volume DESC LIMIT 20
        """, (f"%{keyword}%", f"%{keyword}%"))
        if not rows:
            return f"No markets matching '{keyword}'"
        lines = [f"Polymarket search: {keyword}", ""]
        for r in rows:
            lines.append(f"  {r['yes_price']*100:5.1f}%  {r['change_24h']*100:+5.1f}%  ${r['volume']/1e6:.1f}M  {r['question'][:60]}")
        return "\n".join(lines)

    elif sub == "slug" and len(args) > 1:
        slug = args[1]
        rows = db.fetchall("""
            SELECT question, yes_price, change_24h, volume, ts
            FROM polymarket_markets
            WHERE slug = ?
            ORDER BY ts DESC LIMIT 24
        """, (slug,))
        if not rows:
            return f"No data for slug '{slug}'"
        lines = [f"Polymarket: {rows[0]['question']}", ""]
        for r in rows:
            lines.append(f"  {r['ts'][:16]}  {r['yes_price']*100:5.1f}%  {r['change_24h']*100:+5.1f}%")
        return "\n".join(lines)

    else:
        return cmd_polymarket.__doc__


def cmd_tweets(args):
    db = get_db()
    sub = args[0] if args else "latest"

    if sub == "latest":
        limit = 20
        for i, a in enumerate(args[1:], 1):
            if a == "--limit" and i < len(args):
                limit = int(args[i + 1])
        return db.fetchall(
            "SELECT id, ts, username, content FROM tweets ORDER BY ts DESC LIMIT ?",
            (limit,),
        )

    elif sub == "search" and len(args) > 1:
        keyword = args[1]
        hours = 168
        for i, a in enumerate(args[2:], 2):
            if a == "--hours" and i < len(args):
                hours = int(args[i + 1])
        return db.fetchall(
            f"SELECT id, ts, username, content FROM tweets "
            f"WHERE content LIKE ? AND ts >= datetime('now', '-{hours} hours') "
            f"ORDER BY ts DESC LIMIT 50",
            (f"%{keyword}%",),
        )

    elif sub == "user" and len(args) > 1:
        username = args[1]
        return db.fetchall(
            "SELECT id, ts, username, content FROM tweets "
            "WHERE username = ? ORDER BY ts DESC LIMIT 30",
            (username,),
        )

    return []


def cmd_news(args):
    db = get_db()
    sub = args[0] if args else "latest"

    if sub == "latest":
        limit = 20
        for i, a in enumerate(args[1:], 1):
            if a == "--limit" and i < len(args):
                limit = int(args[i + 1])
        return db.fetchall(
            "SELECT id, ts, subject, source_name, content FROM kb_news ORDER BY ts DESC LIMIT ?",
            (limit,),
        )

    elif sub == "search" and len(args) > 1:
        keyword = args[1]
        hours = 168
        for i, a in enumerate(args[2:], 2):
            if a == "--hours" and i < len(args):
                hours = int(args[i + 1])
        return db.fetchall(
            f"SELECT id, ts, subject, source_name FROM kb_news "
            f"WHERE (subject LIKE ? OR content LIKE ?) AND ts >= datetime('now', '-{hours} hours') "
            f"ORDER BY ts DESC LIMIT 50",
            (f"%{keyword}%", f"%{keyword}%"),
        )

    return []


def cmd_reddit(args):
    db = get_db()
    sub = args[0] if args else "latest"

    if sub == "latest":
        limit = 20
        for i, a in enumerate(args[1:], 1):
            if a == "--limit" and i < len(args):
                limit = int(args[i + 1])
        return db.fetchall(
            "SELECT ts, subreddit, title, author, url, sentiment FROM reddit_posts ORDER BY ts DESC LIMIT ?",
            (limit,),
        )

    elif sub == "search" and len(args) > 1:
        keyword = args[1]
        return db.fetchall(
            "SELECT ts, subreddit, title, author, url, sentiment FROM reddit_posts "
            "WHERE title LIKE ? ORDER BY ts DESC LIMIT 50",
            (f"%{keyword}%",),
        )

    elif sub == "sentiment":
        from aggregator import compute_sentiment
        hours = 24
        for i, a in enumerate(args[1:], 1):
            if a == "--hours" and i < len(args):
                hours = int(args[i + 1])
        return compute_sentiment(db, hours) or {"message": f"No scored posts in last {hours}h"}

    elif sub == "snapshot":
        from aggregator import save_daily_snapshot
        ok = save_daily_snapshot(db)
        return {"saved": ok}

    elif sub == "trend":
        days = 30
        for i, a in enumerate(args[1:], 1):
            if a == "--days" and i < len(args):
                days = int(args[i + 1])
        rows = db.fetchall(
            "SELECT date, score, weighted_avg, bull_bear_spread, post_count, btc_price, fng "
            "FROM reddit_sentiment_daily ORDER BY date DESC LIMIT ?",
            (days,),
        )
        return rows

    return []


def cmd_text(args):
    """跨表全文搜索：tweets + kb_news + reddit_posts + announcements"""
    if not args:
        print("Usage: query.py text 'keyword' [--hours 168]")
        return []
    db = get_db()
    keyword = args[0]
    hours = 168
    for i, a in enumerate(args[1:], 1):
        if a == "--hours" and i < len(args):
            hours = int(args[i + 1])

    results = []

    # tweets
    rows = db.fetchall(
        f"SELECT 'tweet' as source, ts, username as author, content as text FROM tweets "
        f"WHERE content LIKE ? AND ts >= datetime('now', '-{hours} hours') "
        f"ORDER BY ts DESC LIMIT 20",
        (f"%{keyword}%",),
    )
    results.extend(rows)

    # kb_news
    rows = db.fetchall(
        f"SELECT 'news' as source, ts, source_name as author, subject as text FROM kb_news "
        f"WHERE (subject LIKE ? OR content LIKE ?) AND ts >= datetime('now', '-{hours} hours') "
        f"ORDER BY ts DESC LIMIT 20",
        (f"%{keyword}%", f"%{keyword}%"),
    )
    results.extend(rows)

    # reddit
    rows = db.fetchall(
        f"SELECT 'reddit' as source, ts, author, title as text FROM reddit_posts "
        f"WHERE title LIKE ? AND ts >= datetime('now', '-{hours} hours') "
        f"ORDER BY ts DESC LIMIT 20",
        (f"%{keyword}%",),
    )
    results.extend(rows)

    # announcements
    rows = db.fetchall(
        f"SELECT source, ts, catalog_name as author, title as text FROM announcements "
        f"WHERE (title LIKE ? OR body_text LIKE ?) AND ts >= datetime('now', '-{hours} hours') "
        f"ORDER BY ts DESC LIMIT 20",
        (f"%{keyword}%", f"%{keyword}%"),
    )
    results.extend(rows)

    # 按时间排序
    results.sort(key=lambda x: x.get("ts", ""), reverse=True)
    return results


def cmd_signals(args):
    """跨表 topic 信号查询：signals [topic] [--hours N]"""
    from datetime import datetime, timedelta, timezone

    db = get_db()
    topics = args[0] if args and not args[0].startswith("--") else "earn,defi"
    hours = 4
    for i, a in enumerate(args):
        if a == "--hours" and i + 1 < len(args):
            hours = int(args[i + 1])

    topic_list = [t.strip() for t in topics.split(",") if t.strip()]
    topic_placeholders = " OR ".join("topics LIKE ?" for _ in topic_list)
    topic_params = [f"%{t}%" for t in topic_list]
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')

    results = []
    for table, text_col, author_col, url_col in [
        ("announcements", "title || char(10) || COALESCE(SUBSTR(body_text, 1, 2000), '')", "catalog_name", "''"),
        ("tweets", "content", "username", "source_url"),
        ("kb_news", "subject || char(10) || COALESCE(SUBSTR(content, 1, 500), '')", "source_name", "source_url"),
        ("reddit_posts", "title", "author", "url"),
    ]:
        rows = db.fetchall(
            f"SELECT '{table}' as source, ts, {text_col} as text, "
            f"{author_col} as author, topics, entities, {url_col} as url "
            f"FROM {table} "
            f"WHERE ts >= ? AND ({topic_placeholders}) "
            f"ORDER BY ts DESC LIMIT 50",
            (cutoff, *topic_params),
        )
        results.extend(rows)

    results.sort(key=lambda x: x.get("ts", ""), reverse=True)
    return results


def cmd_status(args):
    db = get_db()
    fetchers = db.fetchall("SELECT * FROM fetcher_status ORDER BY name")
    tables = db.table_stats()
    return {"fetchers": fetchers, "tables": tables}


def cmd_sql(args):
    if not args:
        print("Usage: query.py sql 'SELECT ...'")
        return []
    db = get_db()
    sql = args[0]
    if not sql.strip().upper().startswith("SELECT"):
        print("Error: only SELECT queries allowed")
        return []
    return db.fetchall(sql)


COMMANDS = {
    "prices": cmd_prices,
    "yields": cmd_yields,
    "tvl": cmd_tvl,
    "funding": cmd_funding,
    "fear": cmd_fear,
    "stablecoin": cmd_stablecoin,
    "announcements": cmd_announcements,
    "polymarket": cmd_polymarket,
    "tweets": cmd_tweets,
    "news": cmd_news,
    "reddit": cmd_reddit,
    "text": cmd_text,
    "signals": cmd_signals,
    "status": cmd_status,
    "sql": cmd_sql,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        return

    cmd = sys.argv[1]
    args = sys.argv[2:]

    if cmd not in COMMANDS:
        print(f"Unknown command: {cmd}")
        print(f"Available: {', '.join(COMMANDS.keys())}")
        return

    result = COMMANDS[cmd](args)

    if isinstance(result, str):
        print(result)
    elif isinstance(result, dict):
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    elif isinstance(result, list):
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        print(result)


if __name__ == "__main__":
    main()
