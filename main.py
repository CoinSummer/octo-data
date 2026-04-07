#!/usr/bin/env python3
"""DataHub Market — 独立市场数据服务

用法:
    python main.py                     # 启动 scheduler + API server
    python main.py --fetch prices      # 只跑一次 prices fetcher
    python main.py --port 8420         # 指定端口
    python main.py --no-ws             # 不启动 announcements WebSocket
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

import uvicorn

from config import API_HOST, API_PORT
from db import Database
from api import create_app
from scheduler import start_scheduler, ALL_FETCHERS
from fetchers.prices import PricesFetcher
from fetchers.fear_greed import FearGreedFetcher
from fetchers.funding_rates import FundingRatesFetcher
from fetchers.stablecoin import StablecoinFetcher
from fetchers.dominance import DominanceFetcher
from fetchers.defi_tvl import DefiTvlFetcher
from fetchers.defi_yields import DefiYieldsFetcher
from fetchers.announcements import AnnouncementsFetcher
from fetchers.polymarket import PolymarketFetcher
from fetchers.tweets import TweetsFetcher
from fetchers.kb_news import KBNewsFetcher
from fetchers.reddit import RedditFetcher
from fetchers.hl_announcements import HLAnnouncementsFetcher
from fetchers.odaily_announcements import OdailyAnnouncementsFetcher
from classifier import run_classifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="DataHub Market — 市场数据服务")
    all_fetchers = [
        "prices", "fear_greed", "funding_rates", "stablecoin",
        "dominance", "defi_tvl", "defi_yields", "polymarket",
        "tweets", "kb_news", "reddit", "hl_announcements",
        "odaily_announcements",
    ]
    parser.add_argument("--fetch", choices=all_fetchers, help="只跑一次指定 fetcher")
    parser.add_argument("--classify", action="store_true", help="只跑一次 classifier")
    parser.add_argument("--port", type=int, default=API_PORT)
    parser.add_argument("--no-ws", action="store_true", help="不启动 announcements WebSocket")
    args = parser.parse_args()

    db = Database()
    db.connect()
    logger.info("Database initialized")

    # 单次分类模式
    if args.classify:
        count = run_classifier(db)
        print(f"[classifier] Done: {count} records")
        return

    # 单次运行模式
    if args.fetch:
        fetcher_map = {
            "prices": PricesFetcher,
            "fear_greed": FearGreedFetcher,
            "funding_rates": FundingRatesFetcher,
            "stablecoin": StablecoinFetcher,
            "dominance": DominanceFetcher,
            "defi_tvl": DefiTvlFetcher,
            "defi_yields": DefiYieldsFetcher,
            "polymarket": PolymarketFetcher,
            "tweets": TweetsFetcher,
            "kb_news": KBNewsFetcher,
            "reddit": RedditFetcher,
            "hl_announcements": HLAnnouncementsFetcher,
            "odaily_announcements": OdailyAnnouncementsFetcher,
        }
        fetcher = fetcher_map[args.fetch](db)
        count = fetcher.fetch_and_save()
        print(f"[{args.fetch}] Done: {count} records")
        return

    # 完整服务模式
    print("=" * 50)
    print("DataHub Market")
    print("=" * 50)

    # 1. 启动定时 fetcher
    scheduler = start_scheduler(db)
    logger.info("Scheduler started")

    # 2. 启动 announcements WebSocket（后台线程）
    ann_fetcher = None
    if not args.no_ws:
        ann_fetcher = AnnouncementsFetcher(db)
        ann_fetcher.start()

    # 3. 回调 Bot（NOTIFY_CHANNEL=slack|telegram 切换）
    from config import EXTERNAL_SCRIPTS_DIR
    import os as _os
    _notify_channel = _os.environ.get("NOTIFY_CHANNEL", "slack")
    if EXTERNAL_SCRIPTS_DIR:
        _bot_env = _os.environ.copy()
        _bot_env.setdefault("HOME", str(Path.home()))
        _log_dir = Path(__file__).parent
        if _notify_channel == "telegram":
            _bot_proc = subprocess.Popen(
                [sys.executable, "-c",
                 "import sys, logging; "
                 f"sys.path.insert(0, {EXTERNAL_SCRIPTS_DIR!r}); "
                 "logging.basicConfig(level=logging.INFO, "
                 "format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', "
                 "datefmt='%H:%M:%S'); "
                 "from tg_bot import TelegramBot; bot = TelegramBot(); bot._run()"],
                env=_bot_env,
                stdout=open(_log_dir / "tg_bot.log", "a"),
                stderr=subprocess.STDOUT,
            )
            logger.info(f"TG callback bot started (pid={_bot_proc.pid})")
        else:
            _bot_proc = subprocess.Popen(
                [sys.executable, "-c",
                 "import sys, logging; "
                 f"sys.path.insert(0, {EXTERNAL_SCRIPTS_DIR!r}); "
                 "logging.basicConfig(level=logging.INFO, "
                 "format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', "
                 "datefmt='%H:%M:%S'); "
                 "from slack_bot import SlackBot; bot = SlackBot(); bot._run()"],
                env=_bot_env,
                stdout=open(_log_dir / "slack_bot.log", "a"),
                stderr=subprocess.STDOUT,
            )
            logger.info(f"Slack bot started (pid={_bot_proc.pid})")
    else:
        _bot_proc = None
        logger.info("Callback bot skipped (EXTERNAL_SCRIPTS_DIR not set)")

    # 4. 启动 API server（主线程，阻塞）
    app = create_app(db)
    logger.info(f"API server starting on {API_HOST}:{args.port}")

    try:
        uvicorn.run(app, host=API_HOST, port=args.port, log_level="warning")
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        scheduler.shutdown(wait=False)
        if ann_fetcher:
            ann_fetcher.stop()
        if _slack_proc:
            _slack_proc.terminate()
        db.close()


if __name__ == "__main__":
    main()
