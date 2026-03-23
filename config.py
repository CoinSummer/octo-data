"""DataHub Market — 配置（纯环境变量，无项目路径耦合）"""

import os
from pathlib import Path

from dotenv import load_dotenv

# 优先加载同目录 .env
load_dotenv(Path(__file__).parent / ".env")

# Database
DB_PATH = Path(os.getenv("DATAHUB_DB_PATH", str(Path(__file__).parent / "datahub-market.db")))

# API
API_HOST = os.getenv("DATAHUB_HOST", "0.0.0.0")
API_PORT = int(os.getenv("DATAHUB_PORT", "8420"))

# CoinGecko
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")
COINGECKO_BASE_URL = (
    "https://pro-api.coingecko.com/api/v3"
    if COINGECKO_API_KEY
    else "https://api.coingecko.com/api/v3"
)
_DEFAULT_COINS = (
    "bitcoin,ethereum,solana,hyperliquid,binancecoin,ripple,"
    "cardano,dogecoin,toncoin,avalanche-2,chainlink,sui,aave,pendle"
)
PRICE_COINS = [
    c.strip() for c in
    os.getenv("DATAHUB_COINS", os.getenv("DATA_HUB_COINS", _DEFAULT_COINS)).split(",")
    if c.strip()
]

# Chainbot (tweets + news)
CHAINBOT_API_KEY = os.getenv("CHAINBOT_API_KEY", "")
CHAINBOT_BASE_URL = "https://api.chainbot.io"

# Binance (for announcements WebSocket)
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

# External integrations (optional — Slack bot, DeFi analyzers)
EXTERNAL_SCRIPTS_DIR = os.getenv("DATAHUB_SCRIPTS_DIR", "")
