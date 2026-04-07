"""文本分类器 — 通过 claude --print 批量打标签"""

import logging
import os
import re
import subprocess
from pathlib import Path

from db import Database
from prompts import (
    CLASSIFY_PROMPT, CLASSIFIER_BATCH_SIZE, CLASSIFIER_MODEL,
    TEXT_TABLES, VALID_TOPICS,
)

logger = logging.getLogger(__name__)


VALID_EXPLICITNESS = {"strong", "moderate", "weak"}


def parse_response(text: str, expected: int) -> list:
    """解析 LLM 响应，返回 (topics, entities, sentiment, explicitness) 元组列表。

    格式: '1: topics=defi,crypto | entities=pendle,eth | sentiment=-1 | explicitness=strong'
    旧格式兼容: '1: defi,crypto'
    """
    results = [("", "", None, None)] * expected
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"(\d+)\s*[:：]\s*(.*)", line)
        if not m:
            continue
        idx = int(m.group(1)) - 1
        raw = m.group(2).strip()
        if not (0 <= idx < expected) or not raw:
            continue

        # 新格式: topics=... | entities=... | sentiment=... | explicitness=...
        if "topics=" in raw:
            topics_str = ""
            entities_str = ""
            sentiment = None
            explicitness = None
            for part in raw.split("|"):
                part = part.strip()
                if part.startswith("topics="):
                    topics_str = part[len("topics="):]
                elif part.startswith("entities="):
                    entities_str = part[len("entities="):]
                elif part.startswith("sentiment="):
                    try:
                        sentiment = int(part[len("sentiment="):].strip())
                        sentiment = max(-2, min(2, sentiment))
                    except ValueError:
                        sentiment = None
                elif part.startswith("explicitness="):
                    val = part[len("explicitness="):].strip().lower()
                    if val in VALID_EXPLICITNESS:
                        explicitness = val
            topics = [t.strip().lower() for t in topics_str.split(",") if t.strip()]
            topics = [t for t in topics if t in VALID_TOPICS]
            entities = [e.strip().lower() for e in entities_str.split(",") if e.strip()]
            results[idx] = (",".join(topics), ",".join(entities), sentiment, explicitness)
        else:
            # 旧格式兼容: 纯 topics
            topics = [t.strip().lower() for t in raw.split(",")]
            topics = [t for t in topics if t in VALID_TOPICS]
            results[idx] = (",".join(topics), "", None, None)
    return results


def classify_batch(texts: list) -> list:
    """通过 claude --print 批量分类。"""
    numbered = "\n".join(f"{i+1}. {t[:200]}" for i, t in enumerate(texts))
    prompt = CLASSIFY_PROMPT.format(texts=numbered)
    env = os.environ.copy()
    env.setdefault("HOME", str(Path.home()))
    try:
        result = subprocess.run(
            ["claude", "--print", "--model", CLASSIFIER_MODEL, "-p", prompt],
            capture_output=True, text=True, timeout=120,
            env=env,
        )
        if result.returncode != 0:
            logger.warning(f"claude subprocess failed: {result.stderr[:200]}")
            return [("", "", None, None)] * len(texts)
        return parse_response(result.stdout, len(texts))
    except subprocess.TimeoutExpired:
        logger.warning("claude subprocess timed out")
        return [("", "", None, None)] * len(texts)
    except FileNotFoundError:
        logger.error("claude CLI not found in PATH")
        return [("", "", None, None)] * len(texts)


def run_classifier(db: Database) -> int:
    """扫描所有空 topics 记录，LLM 分类并更新。返回处理条数。"""
    total = 0

    for table, text_col, id_col in TEXT_TABLES:
        rows = db.fetchall(
            f"SELECT {id_col} AS row_id, {text_col} AS text FROM {table} "
            f"WHERE topics = '' OR topics IS NULL"
        )
        if not rows:
            continue

        logger.info(f"[classifier] {table}: {len(rows)} unclassified")

        for i in range(0, len(rows), CLASSIFIER_BATCH_SIZE):
            batch = rows[i:i + CLASSIFIER_BATCH_SIZE]
            texts = [r["text"] or "" for r in batch]
            results = classify_batch(texts)

            for row, (topics, entities, sentiment, explicitness) in zip(batch, results):
                if table == "reddit_posts" and sentiment is not None:
                    db.execute(
                        f"UPDATE {table} SET topics = ?, entities = ?, sentiment = ?, explicitness = ? WHERE {id_col} = ?",
                        (topics or "_none", entities, sentiment, explicitness, row["row_id"]),
                    )
                else:
                    db.execute(
                        f"UPDATE {table} SET topics = ?, entities = ? WHERE {id_col} = ?",
                        (topics or "_none", entities, row["row_id"]),
                    )
            db.commit()

        total += len(rows)

    if total:
        logger.info(f"[classifier] Done: {total} records classified")
        db.update_fetcher_status("classifier", True)
    else:
        logger.debug("[classifier] No unclassified records")

    return total


def reclassify_missing_entities(db: Database, limit: int = 0) -> int:
    """重分类 entities 为空但 topics 已有的记录（补提 entities）。"""
    total = 0

    for table, text_col, id_col in TEXT_TABLES:
        sql = (
            f"SELECT {id_col} AS row_id, {text_col} AS text FROM {table} "
            f"WHERE (entities = '' OR entities IS NULL) AND topics != '' AND topics IS NOT NULL AND topics != '_none'"
        )
        if limit:
            sql += f" LIMIT {limit}"
        rows = db.fetchall(sql)
        if not rows:
            continue

        logger.info(f"[reclassify] {table}: {len(rows)} records missing entities")

        for i in range(0, len(rows), CLASSIFIER_BATCH_SIZE):
            batch = rows[i:i + CLASSIFIER_BATCH_SIZE]
            texts = [r["text"] or "" for r in batch]
            results = classify_batch(texts)

            for row, (topics, entities, sentiment, explicitness) in zip(batch, results):
                if table == "reddit_posts" and sentiment is not None:
                    db.execute(
                        f"UPDATE {table} SET topics = ?, entities = ?, sentiment = ?, explicitness = ? WHERE {id_col} = ?",
                        (topics or "_none", entities, sentiment, explicitness, row["row_id"]),
                    )
                else:
                    db.execute(
                        f"UPDATE {table} SET topics = ?, entities = ? WHERE {id_col} = ?",
                        (topics or "_none", entities, row["row_id"]),
                    )
            db.commit()

        total += len(rows)

    if total:
        logger.info(f"[reclassify] Done: {total} records updated with entities")
    return total


def backfill_sentiment(db: Database, limit: int = 0) -> int:
    """为已有 topics 但无 sentiment/explicitness 的 reddit_posts 补打分。"""
    sql = (
        "SELECT id AS row_id, title AS text FROM reddit_posts "
        "WHERE (sentiment IS NULL OR explicitness IS NULL) AND topics IS NOT NULL AND topics != ''"
    )
    if limit:
        sql += f" LIMIT {limit}"
    rows = db.fetchall(sql)
    if not rows:
        logger.info("[backfill] No reddit posts need sentiment scoring")
        return 0

    logger.info(f"[backfill] {len(rows)} reddit posts need sentiment")
    total = 0

    for i in range(0, len(rows), CLASSIFIER_BATCH_SIZE):
        batch = rows[i:i + CLASSIFIER_BATCH_SIZE]
        texts = [r["text"] or "" for r in batch]
        results = classify_batch(texts)

        for row, (topics, entities, sentiment, explicitness) in zip(batch, results):
            if sentiment is not None:
                db.execute(
                    "UPDATE reddit_posts SET sentiment = ?, explicitness = ? WHERE id = ?",
                    (sentiment, explicitness, row["row_id"]),
                )
        db.commit()
        total += len(batch)

    logger.info(f"[backfill] Done: {total} posts scored")
    return total


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    from db import Database
    db = Database()
    db.connect()

    if len(sys.argv) > 1 and sys.argv[1] == "reclassify":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 0
        n = reclassify_missing_entities(db, limit=limit)
        print(f"Reclassified {n} records")
    elif len(sys.argv) > 1 and sys.argv[1] == "backfill-sentiment":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 0
        n = backfill_sentiment(db, limit=limit)
        print(f"Backfilled sentiment for {n} posts")
    else:
        n = run_classifier(db)
        print(f"Classified {n} records")

    db.close()
