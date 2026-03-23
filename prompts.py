"""集中管理 LLM prompt + 可调配置"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Classifier
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASSIFIER_MODEL = "haiku"
CLASSIFIER_BATCH_SIZE = 20
VALID_TOPICS = {"defi", "earn", "crypto", "stock", "macro", "ai"}

CLASSIFY_PROMPT = """对以下文本打标签。只从这 6 个类别中选：defi, earn, crypto, stock, macro, ai

类别说明：
- defi: DeFi 协议动态（yield/tvl/staking/lending/pool/swap/链上协议/Morpho/Pendle/Aave/Uniswap/Stargate 等）
- earn: CEX/CeFi 收益机会（Earn/Super Earn/Launchpool/Launchpad/HODLer Airdrop/质押奖励/APR/APY/奖池/锁仓收益/交易赛）
- crypto: 泛 crypto（BTC/ETH/交易所/监管/空投/安全/链上事件/爆仓/鲸鱼/代币上线/Binance/Hyperliquid 等）
- stock: 股票/财报/估值/IPO/美股/港股
- macro: 美联储/CPI/PPI/利率/关税/地缘政治/油价/黄金/降息/加息
- ai: AI/大模型/算力/Agent

规则：
- 可多选，逗号分隔
- 宁可多打不要漏打。只要文本明显涉及某类别就打上
- 提到 BTC/ETH/代币/交易所/链上 → 至少打 crypto
- 提到利率/通胀/美联储/地缘/油价 → 至少打 macro
- 纯广告/无意义转发/非中英文 → 留空
- 每条一行，格式严格为：序号: 标签1,标签2（无标签则写：序号:）

{texts}"""

# 每张表的 (表名, 文本列SQL, 主键列)
TEXT_TABLES = [
    ("tweets", "content", "id"),
    ("announcements", "title || ' ' || COALESCE(body_text, '')", "id"),
    ("reddit_posts", "title", "id"),
    ("kb_news", "subject || ' ' || COALESCE(SUBSTR(content, 1, 200), '')", "id"),
]
