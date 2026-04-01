"""一次性迁移：把媒体类数据从 announcements 搬到 news 表

迁移的 source：36kr, techcrunch, hackernews, meta_engineering, latepost, odaily
保留在 announcements 的 source：binance, hyperliquid, okx

用法：
    python3 migrate_news.py           # dry-run，只打印统计
    python3 migrate_news.py --apply   # 实际执行迁移
"""

import argparse
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent / "datahub-market.db"

NEWS_SOURCES = ("36kr", "techcrunch", "hackernews", "meta_engineering", "latepost", "odaily")


def migrate(apply: bool = False):
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # 确保 news 表存在
    conn.execute("""
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
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_news_dedup ON news(title, ts, source)")
    conn.commit()

    placeholders = ",".join("?" for _ in NEWS_SOURCES)

    # 统计
    row = conn.execute(
        f"SELECT COUNT(*) as cnt FROM announcements WHERE source IN ({placeholders})",
        NEWS_SOURCES,
    ).fetchone()
    total = row["cnt"]

    by_source = conn.execute(
        f"SELECT source, COUNT(*) as cnt FROM announcements WHERE source IN ({placeholders}) GROUP BY source ORDER BY cnt DESC",
        NEWS_SOURCES,
    ).fetchall()

    print(f"待迁移: {total} 条")
    for r in by_source:
        print(f"  {r['source']:20s}  {r['cnt']}")

    if not apply:
        print("\n[dry-run] 加 --apply 执行迁移")
        conn.close()
        return

    # 复制到 news
    inserted = conn.execute(f"""
        INSERT OR IGNORE INTO news (ts, catalog_id, catalog_name, title, body, body_text, code, source, url, topics, entities, created_at)
        SELECT ts, catalog_id, catalog_name, title, body, body_text, code, source, url, topics, entities, created_at
        FROM announcements
        WHERE source IN ({placeholders})
    """, NEWS_SOURCES).rowcount
    print(f"\n复制到 news: {inserted} 条")

    # 从 announcements 删除
    deleted = conn.execute(
        f"DELETE FROM announcements WHERE source IN ({placeholders})",
        NEWS_SOURCES,
    ).rowcount
    print(f"从 announcements 删除: {deleted} 条")

    conn.commit()

    # 验证
    ann_count = conn.execute("SELECT COUNT(*) as cnt FROM announcements").fetchone()["cnt"]
    news_count = conn.execute("SELECT COUNT(*) as cnt FROM news").fetchone()["cnt"]
    print(f"\n迁移后: announcements={ann_count}, news={news_count}")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    migrate(apply=args.apply)
