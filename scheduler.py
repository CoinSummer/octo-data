"""Fetcher 调度器"""

import logging
import subprocess
import sys
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler

from db import Database
from fetchers.prices import PricesFetcher
from fetchers.fear_greed import FearGreedFetcher
from fetchers.funding_rates import FundingRatesFetcher
from fetchers.stablecoin import StablecoinFetcher
from fetchers.dominance import DominanceFetcher
from fetchers.defi_tvl import DefiTvlFetcher
from fetchers.defi_yields import DefiYieldsFetcher
from fetchers.polymarket import PolymarketFetcher
from fetchers.tweets import TweetsFetcher
from fetchers.kb_news import KBNewsFetcher
from fetchers.reddit import RedditFetcher
from fetchers.hl_announcements import HLAnnouncementsFetcher
from fetchers.okx_announcements import OKXAnnouncementsFetcher
from fetchers.odaily_announcements import OdailyAnnouncementsFetcher
from fetchers.exchange_metrics import ExchangeMetricsFetcher
from fetchers.latepost import LatePostFetcher
from fetchers.rss_feeds import RSSFeedsFetcher
from classifier import run_classifier

logger = logging.getLogger(__name__)

ALL_FETCHERS = [
    PricesFetcher,           # 15 min
    FundingRatesFetcher,     # 15 min
    FearGreedFetcher,        # 1 hour
    StablecoinFetcher,       # 1 hour
    DominanceFetcher,        # 1 hour
    DefiTvlFetcher,          # 1 hour
    DefiYieldsFetcher,       # 1 hour
    PolymarketFetcher,       # 1 hour
    TweetsFetcher,           # 30 min
    KBNewsFetcher,           # 30 min
    RedditFetcher,           # 1 hour
    HLAnnouncementsFetcher,  # 30 min
    OKXAnnouncementsFetcher,    # 30 min
    OdailyAnnouncementsFetcher, # 30 min
    LatePostFetcher,            # 2 hour
    RSSFeedsFetcher,            # 2 hour (36kr, TechCrunch, HN, Meta Eng)
    # ExchangeMetricsFetcher,     # 1 hour — 暂停：HL API 限流影响交易
]


def start_scheduler(db: Database) -> BackgroundScheduler:
    from datetime import datetime, timedelta
    scheduler = BackgroundScheduler()

    fetchers = [cls(db) for cls in ALL_FETCHERS]

    for f in fetchers:
        scheduler.add_job(
            f.fetch_and_save,
            trigger="interval",
            seconds=f.interval_seconds,
            next_run_time=datetime.now(),
            id=f.name,
            name=f.name,
            replace_existing=True,
        )
        logger.info(f"Scheduled [{f.name}] every {f.interval_seconds // 60}min")

    # Classifier — 每 30min 分类未打标签的文本
    scheduler.add_job(
        lambda: run_classifier(db),
        trigger="interval",
        seconds=30 * 60,
        id="classifier",
        name="classifier",
        replace_existing=True,
    )
    logger.info("Scheduled [classifier] every 30min")

    # DeFi Monitor — 每 4h 运行一次（subprocess，消费端逻辑不内嵌）
    from config import EXTERNAL_SCRIPTS_DIR

    def _make_subprocess_env():
        import os
        env = os.environ.copy()
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            env.pop(k, None)
        env["NO_PROXY"] = "localhost,127.0.0.1"
        return env

    if EXTERNAL_SCRIPTS_DIR:
        def _run_defi_monitor():
            try:
                result = subprocess.run(
                    [sys.executable,
                     str(Path(EXTERNAL_SCRIPTS_DIR) / "analyzers" / "defi_monitor.py"),
                     "--hours", "4"],
                    capture_output=True, text=True, timeout=300,
                    env=_make_subprocess_env(), cwd=EXTERNAL_SCRIPTS_DIR,
                )
                logger.info(f"[defi_monitor] {result.stdout.strip()}")
                if result.returncode != 0:
                    logger.error(f"[defi_monitor] stderr: {result.stderr[:500]}")
            except Exception as e:
                logger.error(f"[defi_monitor] failed: {e}")

        scheduler.add_job(
            _run_defi_monitor,
            trigger="interval",
            seconds=4 * 3600,
            id="defi_monitor",
            name="defi_monitor",
            replace_existing=True,
            misfire_grace_time=300,
        )
        logger.info("Scheduled [defi_monitor] every 4h")

        # Crypto Monitor — 每 4h 运行（持仓+主题信号监控）
        def _run_crypto_monitor():
            try:
                result = subprocess.run(
                    [sys.executable,
                     str(Path(EXTERNAL_SCRIPTS_DIR) / "analyzers" / "crypto_monitor.py"),
                     "--hours", "4"],
                    capture_output=True, text=True, timeout=300,
                    env=_make_subprocess_env(), cwd=EXTERNAL_SCRIPTS_DIR,
                )
                logger.info(f"[crypto_monitor] {result.stdout.strip()}")
                if result.returncode != 0:
                    logger.error(f"[crypto_monitor] stderr: {result.stderr[:500]}")
            except Exception as e:
                logger.error(f"[crypto_monitor] failed: {e}")

        scheduler.add_job(
            _run_crypto_monitor,
            trigger="interval",
            seconds=4 * 3600,
            id="crypto_monitor",
            name="crypto_monitor",
            replace_existing=True,
            misfire_grace_time=300,
        )
        logger.info("Scheduled [crypto_monitor] every 4h")

        # Stock Monitor — 每 12h 运行（河流模型持仓信号监控）
        def _run_stock_monitor():
            try:
                result = subprocess.run(
                    [sys.executable,
                     str(Path(EXTERNAL_SCRIPTS_DIR) / "analyzers" / "stock_monitor.py"),
                     "--hours", "12"],
                    capture_output=True, text=True, timeout=300,
                    env=_make_subprocess_env(), cwd=EXTERNAL_SCRIPTS_DIR,
                )
                logger.info(f"[stock_monitor] {result.stdout.strip()}")
                if result.returncode != 0:
                    logger.error(f"[stock_monitor] stderr: {result.stderr[:500]}")
            except Exception as e:
                logger.error(f"[stock_monitor] failed: {e}")

        scheduler.add_job(
            _run_stock_monitor,
            trigger="interval",
            seconds=12 * 3600,
            id="stock_monitor",
            name="stock_monitor",
            replace_existing=True,
            misfire_grace_time=300,
        )
        logger.info("Scheduled [stock_monitor] every 12h")

        # DeFi Review — 每天 06:00 运行（nightly 审视管线质量）
        def _run_defi_review():
            try:
                result = subprocess.run(
                    [sys.executable,
                     str(Path(EXTERNAL_SCRIPTS_DIR) / "analyzers" / "defi_review.py")],
                    capture_output=True, text=True, timeout=900,
                    env=_make_subprocess_env(), cwd=EXTERNAL_SCRIPTS_DIR,
                )
                logger.info(f"[defi_review] {result.stdout.strip()}")
                if result.returncode != 0:
                    logger.error(f"[defi_review] stderr: {result.stderr[:500]}")
            except Exception as e:
                logger.error(f"[defi_review] failed: {e}")

        scheduler.add_job(
            _run_defi_review,
            trigger="cron",
            hour=6,
            minute=0,
            id="defi_review",
            name="defi_review",
            replace_existing=True,
            misfire_grace_time=300,
        )
        logger.info("Scheduled [defi_review] daily at 06:00")

        # Crypto Review — 每天 06:30 运行（审视河流分析质量）
        def _run_crypto_review():
            try:
                result = subprocess.run(
                    [sys.executable,
                     str(Path(EXTERNAL_SCRIPTS_DIR) / "analyzers" / "crypto_review.py")],
                    capture_output=True, text=True, timeout=900,
                    env=_make_subprocess_env(), cwd=EXTERNAL_SCRIPTS_DIR,
                )
                logger.info(f"[crypto_review] {result.stdout.strip()}")
                if result.returncode != 0:
                    logger.error(f"[crypto_review] stderr: {result.stderr[:500]}")
            except Exception as e:
                logger.error(f"[crypto_review] failed: {e}")

        scheduler.add_job(
            _run_crypto_review,
            trigger="cron",
            hour=6,
            minute=30,
            id="crypto_review",
            name="crypto_review",
            replace_existing=True,
            misfire_grace_time=300,
        )
        logger.info("Scheduled [crypto_review] daily at 06:30")

        # Stock Review — 每天 07:00 运行（审视河流分析质量）
        def _run_stock_review():
            try:
                result = subprocess.run(
                    [sys.executable,
                     str(Path(EXTERNAL_SCRIPTS_DIR) / "analyzers" / "stock_review.py")],
                    capture_output=True, text=True, timeout=900,
                    env=_make_subprocess_env(), cwd=EXTERNAL_SCRIPTS_DIR,
                )
                logger.info(f"[stock_review] {result.stdout.strip()}")
                if result.returncode != 0:
                    logger.error(f"[stock_review] stderr: {result.stderr[:500]}")
            except Exception as e:
                logger.error(f"[stock_review] failed: {e}")

        scheduler.add_job(
            _run_stock_review,
            trigger="cron",
            hour=7,
            minute=0,
            id="stock_review",
            name="stock_review",
            replace_existing=True,
            misfire_grace_time=300,
        )
        logger.info("Scheduled [stock_review] daily at 07:00")
    else:
        logger.info("External scripts skipped (DATAHUB_SCRIPTS_DIR not set)")

    scheduler.start()

    # 启动时立即跑一次
    for f in fetchers:
        try:
            f.fetch_and_save()
        except Exception as e:
            logger.error(f"Initial fetch [{f.name}] failed: {e}")

    # Classifier 由 scheduler 延迟 60s 后异步触发，不阻塞启动
    scheduler.add_job(
        lambda: run_classifier(db),
        trigger="date",
        run_date=datetime.now() + timedelta(seconds=60),
        id="classifier_initial",
        name="classifier_initial",
        replace_existing=True,
    )
    logger.info("Classifier initial run scheduled in 60s")

    return scheduler
