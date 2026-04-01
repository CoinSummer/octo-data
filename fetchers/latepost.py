"""晚点 LatePost Fetcher — 通过网站内部 API 拉取独家报道

数据源：https://www.latepost.com/news/get-news-data
写入 news 表，source='latepost'。
"""

import logging

import httpx

from .base import BaseFetcher

logger = logging.getLogger(__name__)

API_URL = "https://www.latepost.com/news/get-news-data"
BASE_URL = "https://www.latepost.com"


class LatePostFetcher(BaseFetcher):
    """晚点 LatePost 独家 + 深度 — 每 2 小时。"""

    name = "latepost"
    interval_seconds = 7200  # 2h（晚点更新频率低，2-3天一篇）

    def _run(self) -> int:
        count = 0
        # programa: 0=全部, 1=独家, 2=对话, 3=深度
        for programa in [1, 3]:
            count += self._fetch_programa(programa)
        return count

    def _fetch_programa(self, programa: int) -> int:
        with httpx.Client(timeout=15) as client:
            resp = client.post(
                API_URL,
                data={"page": 1, "limit": 20, "programa": programa},
                headers={"User-Agent": "Mozilla/5.0"},
            )
            resp.raise_for_status()
            result = resp.json()

        articles = result.get("data", [])
        if not articles:
            return 0

        catalog_map = {1: "独家", 2: "对话", 3: "深度"}
        catalog_name = catalog_map.get(programa, "其他")
        count = 0

        for a in articles:
            article_id = a.get("id", "")
            title = a.get("title", "").strip()
            abstract = a.get("abstract", "").strip()
            release_time = a.get("release_time", "")
            detail_url = a.get("detail_url", "")
            url = f"{BASE_URL}{detail_url}" if detail_url else ""

            # 提取标签
            labels = a.get("label", [])
            if isinstance(labels, list) and labels and isinstance(labels[0], dict):
                entities = ",".join(lb.get("label", "") for lb in labels)
            else:
                entities = ""

            # 用 article_id 作为 code 去重
            self.db.execute("""
                INSERT OR IGNORE INTO news
                    (ts, catalog_id, catalog_name, title, body, body_text, code, source, url, entities)
                VALUES (?, 0, ?, ?, '', ?, ?, 'latepost', ?, ?)
            """, (release_time, catalog_name, title, abstract, str(article_id), url, entities))
            count += 1

        self.db.commit()
        return count
