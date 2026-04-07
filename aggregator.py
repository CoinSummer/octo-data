"""Reddit Sentiment v2 聚合引擎

加权公式: engagement × explicitness × sub_weight × time_decay
归一化: z-score → tanh → 0-100
"""

import json
import math
import logging
from datetime import datetime, timedelta, timezone

from db import Database

logger = logging.getLogger(__name__)

# ── 常量 ──

HALF_LIFE_HOURS = 12
LAMBDA = math.log(2) / HALF_LIFE_HOURS  # ≈ 0.0578

SUB_WEIGHTS = {
    "cryptocurrency": 1.3,
    "bitcoinmarkets": 1.3,
    "bitcoin": 1.0,
    "ethereum": 1.0,
    "defi": 1.0,
    "wallstreetbets": 0.7,
    "stocks": 0.7,
    "investing": 0.7,
}

EXPLICITNESS_WEIGHTS = {"strong": 1.25, "moderate": 1.0, "weak": 0.8}

# z-score bootstrap: 数据不足时的 fallback σ
BOOTSTRAP_SIGMA = 0.5
MIN_POSTS_FOR_STATS = 50


def post_weight(row: dict, now: datetime) -> float:
    """单帖权重 = engagement × explicitness × sub_weight × decay"""
    # 互动量: 无 OAuth 数据时 (upvotes=0) 所有帖子等权
    upvotes = row.get("upvotes") or 0
    comments = row.get("comments") or 0
    if upvotes > 0 or comments > 0:
        engagement = math.log(2 + upvotes + 2 * comments)
    else:
        engagement = 1.0  # RSS-only: 等权

    expl_w = EXPLICITNESS_WEIGHTS.get(row.get("explicitness") or "moderate", 1.0)
    sub_w = SUB_WEIGHTS.get(row.get("subreddit", ""), 1.0)

    # 时间衰减
    ts = row.get("ts", "")
    if isinstance(ts, str):
        try:
            post_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            post_time = now
    else:
        post_time = ts
    if post_time.tzinfo is None:
        post_time = post_time.replace(tzinfo=timezone.utc)

    hours_old = max(0, (now - post_time).total_seconds() / 3600)
    decay = math.exp(-LAMBDA * hours_old)

    return engagement * expl_w * sub_w * decay


def compute_sentiment(db: Database, hours: int = 24):
    """计算 v2 加权情绪分数。返回 headline + detail panel 数据。"""
    now = datetime.now(timezone.utc)

    rows = db.fetchall(
        "SELECT subreddit, title, topics, sentiment, explicitness, "
        "upvotes, comments, ts FROM reddit_posts "
        "WHERE sentiment IS NOT NULL AND ts > datetime('now', ?)",
        (f"-{hours} hours",),
    )
    if not rows:
        return None

    # ── 加权平均 ──
    weighted_sum = 0.0
    total_weight = 0.0
    for r in rows:
        w = post_weight(r, now)
        weighted_sum += r["sentiment"] * w
        total_weight += w

    weighted_avg = weighted_sum / total_weight if total_weight else 0

    # ── Bull-Bear Spread ──
    pos = sum(1 for r in rows if r["sentiment"] > 0)
    neg = sum(1 for r in rows if r["sentiment"] < 0)
    neu = sum(1 for r in rows if r["sentiment"] == 0)
    total = pos + neg + neu
    bbs = (pos - neg) / total if total else 0

    # ── z-score → tanh → 0-100 ──
    mu, sigma = _get_rolling_stats(db)
    z = (weighted_avg - mu) / sigma if sigma > 0.01 else 0
    score = 50 + 50 * math.tanh(z)

    # ── 分布 ──
    scores = [r["sentiment"] for r in rows]
    distribution = {v: scores.count(v) for v in [-2, -1, 0, 1, 2]}

    # ── 按 subreddit ──
    by_sub: dict[str, list] = {}
    for r in rows:
        by_sub.setdefault(r["subreddit"], []).append(r["sentiment"])
    sub_scores = {
        s: {"avg": round(sum(v) / len(v), 2), "count": len(v)}
        for s, v in by_sub.items()
    }

    # ── 按 topic ──
    by_topic: dict[str, list] = {}
    for r in rows:
        for t in (r["topics"] or "").split(","):
            t = t.strip()
            if t and t != "_none":
                by_topic.setdefault(t, []).append(r["sentiment"])
    topic_scores = {
        t: {"avg": round(sum(v) / len(v), 2), "count": len(v)}
        for t, v in by_topic.items()
    }

    # ── Volume z-scores (帖量/评论量，暂无 OAuth 时用帖量) ──
    volume = _volume_signals(db, hours)

    # ── 站内 RVS ──
    rvs = _internal_rvs(db)

    # ── explicitness 分布 ──
    expl_dist = {}
    for r in rows:
        e = r.get("explicitness") or "unknown"
        expl_dist[e] = expl_dist.get(e, 0) + 1

    return {
        "score": round(score, 1),
        "weighted_avg": round(weighted_avg, 3),
        "bull_bear_spread": round(bbs, 3),
        "total_posts": len(rows),
        "hours": hours,
        "distribution": distribution,
        "explicitness_distribution": expl_dist,
        "by_subreddit": sub_scores,
        "by_topic": topic_scores,
        "volume": volume,
        "rvs": rvs,
        "normalization": {
            "mu": round(mu, 4),
            "sigma": round(sigma, 4),
            "z": round(z, 3),
        },
    }


def _get_rolling_stats(db: Database) -> tuple[float, float]:
    """7d rolling mean/std of weighted_avg from daily snapshots.
    冷启动时 fallback to bootstrap."""
    rows = db.fetchall(
        "SELECT weighted_avg FROM reddit_sentiment_daily "
        "WHERE date > date('now', '-7 days') ORDER BY date DESC"
    )
    if len(rows) < 3:
        # 冷启动: 从原始帖子估算
        raw = db.fetchall(
            "SELECT sentiment FROM reddit_posts "
            "WHERE sentiment IS NOT NULL AND ts > datetime('now', '-7 days')"
        )
        if len(raw) < MIN_POSTS_FOR_STATS:
            return 0.0, BOOTSTRAP_SIGMA
        vals = [r["sentiment"] for r in raw]
        mu = sum(vals) / len(vals)
        variance = sum((v - mu) ** 2 for v in vals) / len(vals)
        sigma = max(math.sqrt(variance), 0.1)
        return mu, sigma

    vals = [r["weighted_avg"] for r in rows]
    mu = sum(vals) / len(vals)
    variance = sum((v - mu) ** 2 for v in vals) / len(vals)
    sigma = max(math.sqrt(variance), 0.1)
    return mu, sigma


def _volume_signals(db: Database, hours: int = 24) -> dict:
    """帖量/评论量 z-score（相对 7d rolling）。"""
    current = db.fetchone(
        "SELECT count(*) as posts, COALESCE(sum(comments), 0) as total_comments "
        "FROM reddit_posts WHERE ts > datetime('now', ?)",
        (f"-{hours} hours",),
    )
    if not current:
        return {"post_volume_z": 0, "comment_volume_z": 0}

    # 7d daily stats from snapshots
    daily = db.fetchall(
        "SELECT post_count, total_comments FROM reddit_sentiment_daily "
        "WHERE date > date('now', '-7 days')"
    )
    if len(daily) < 3:
        # 冷启动: 粗估
        return {
            "post_volume_z": 0,
            "comment_volume_z": 0,
            "post_count": current["posts"],
            "comment_count": current["total_comments"],
            "note": "cold_start",
        }

    post_counts = [d["post_count"] for d in daily if d["post_count"]]
    mu_p = sum(post_counts) / len(post_counts)
    std_p = math.sqrt(sum((x - mu_p) ** 2 for x in post_counts) / len(post_counts))

    comment_counts = [d["total_comments"] or 0 for d in daily]
    mu_c = sum(comment_counts) / len(comment_counts)
    std_c = math.sqrt(sum((x - mu_c) ** 2 for x in comment_counts) / len(comment_counts))

    def z(val, mu, std):
        return round((val - mu) / std, 2) if std > 0 else 0

    return {
        "post_volume_z": z(current["posts"], mu_p, std_p),
        "comment_volume_z": z(current["total_comments"], mu_c, std_c),
        "post_count": current["posts"],
        "comment_count": current["total_comments"],
    }


def _internal_rvs(db: Database) -> dict:
    """站内 RVS: sentiment_delta vs comment_delta (24h vs 前 24h)。"""
    now_sent = db.fetchone(
        "SELECT AVG(sentiment) as avg_s, SUM(comments) as sum_c "
        "FROM reddit_posts WHERE sentiment IS NOT NULL "
        "AND ts > datetime('now', '-24 hours')"
    )
    prev_sent = db.fetchone(
        "SELECT AVG(sentiment) as avg_s, SUM(comments) as sum_c "
        "FROM reddit_posts WHERE sentiment IS NOT NULL "
        "AND ts BETWEEN datetime('now', '-48 hours') AND datetime('now', '-24 hours')"
    )

    if not now_sent or not prev_sent or now_sent["avg_s"] is None or prev_sent["avg_s"] is None:
        return {"label": "N/A", "sentiment_delta": 0, "comment_delta": 0}

    sent_delta = (now_sent["avg_s"] or 0) - (prev_sent["avg_s"] or 0)
    comment_delta = (now_sent["sum_c"] or 0) - (prev_sent["sum_c"] or 0)

    aligned = (sent_delta * comment_delta) > 0

    return {
        "label": "ALIGNED" if aligned else "DIVERGENT",
        "aligned": aligned,
        "sentiment_delta": round(sent_delta, 3),
        "comment_delta": comment_delta,
    }


def save_daily_snapshot(db: Database) -> bool:
    """保存当日快照到 reddit_sentiment_daily。幂等（同日覆盖）。"""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    result = compute_sentiment(db, hours=24)
    if not result:
        logger.info("[snapshot] No sentiment data for today")
        return False

    # 拉 BTC 价格和 FNG
    btc = db.fetchone("SELECT price_usd FROM prices WHERE symbol = 'BTC' ORDER BY ts DESC LIMIT 1")
    fng = db.fetchone("SELECT value FROM fear_greed ORDER BY ts DESC LIMIT 1")

    db.execute(
        "INSERT OR REPLACE INTO reddit_sentiment_daily "
        "(date, score, weighted_avg, bull_bear_spread, post_count, "
        "total_comments, total_upvotes, post_volume_z, comment_volume_z, "
        "engagement_per_post_z, rvs_aligned, by_subreddit, by_topic, "
        "btc_price, fng) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            today,
            result["score"],
            result["weighted_avg"],
            result["bull_bear_spread"],
            result["total_posts"],
            result["volume"].get("comment_count", 0),
            0,  # total_upvotes: 无 OAuth 时为 0
            result["volume"].get("post_volume_z", 0),
            result["volume"].get("comment_volume_z", 0),
            0,  # engagement_per_post_z: 无 OAuth 时为 0
            1 if result["rvs"].get("aligned") else 0,
            json.dumps(result["by_subreddit"], ensure_ascii=False),
            json.dumps(result["by_topic"], ensure_ascii=False),
            btc["price_usd"] if btc else None,
            fng["value"] if fng else None,
        ),
    )
    db.commit()
    logger.info(f"[snapshot] Saved daily snapshot: {today} score={result['score']}")
    return True


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    db = Database()
    db.connect()

    if len(sys.argv) > 1 and sys.argv[1] == "snapshot":
        save_daily_snapshot(db)
    else:
        hours = 24
        for i, a in enumerate(sys.argv[1:], 1):
            if a == "--hours" and i < len(sys.argv):
                hours = int(sys.argv[i + 1])
        result = compute_sentiment(db, hours)
        if result:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            print("No data")

    db.close()
