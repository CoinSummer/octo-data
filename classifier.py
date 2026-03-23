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


def parse_response(text: str, expected: int) -> list:
    """解析 LLM 响应 '1: defi,crypto\\n2: macro\\n...'，返回 topics 列表。"""
    results = [""] * expected
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"(\d+)\s*[:：]\s*(.*)", line)
        if not m:
            continue
        idx = int(m.group(1)) - 1
        raw = m.group(2).strip()
        if 0 <= idx < expected and raw:
            topics = [t.strip().lower() for t in raw.split(",")]
            topics = [t for t in topics if t in VALID_TOPICS]
            results[idx] = ",".join(topics)
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
            capture_output=True, text=True, timeout=60,
            env=env,
        )
        if result.returncode != 0:
            logger.warning(f"claude subprocess failed: {result.stderr[:200]}")
            return [""] * len(texts)
        return parse_response(result.stdout, len(texts))
    except subprocess.TimeoutExpired:
        logger.warning("claude subprocess timed out")
        return [""] * len(texts)
    except FileNotFoundError:
        logger.error("claude CLI not found in PATH")
        return [""] * len(texts)


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

            for row, topics in zip(batch, results):
                db.execute(
                    f"UPDATE {table} SET topics = ? WHERE {id_col} = ?",
                    (topics or "_none", row["row_id"]),
                )
            db.commit()

        total += len(rows)

    if total:
        logger.info(f"[classifier] Done: {total} records classified")
        db.update_fetcher_status("classifier", True)
    else:
        logger.debug("[classifier] No unclassified records")

    return total
