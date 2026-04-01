"""集中管理 LLM prompt + 可调配置"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Classifier
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASSIFIER_MODEL = "haiku"
CLASSIFIER_BATCH_SIZE = 20
VALID_TOPICS = {"defi", "earn", "crypto", "stock", "macro", "ai", "tech"}

CLASSIFY_PROMPT = """对以下文本打标签并提取实体。

## 标签（topics）— 从这 7 个中选，可多选
- defi: DeFi 协议动态（yield/tvl/staking/lending/pool/swap/链上协议/Morpho/Pendle/Aave/Uniswap 等）
- earn: **可操作的收益机会**（有明确 APR/APY/收益率 + 参与方式），或**影响收益策略的规则变更**（资金费率/保证金/结算频率/费率上下限/杠杆调整等衍生品交易规则）。注意：代币上线、法币通道、支付方式、系统维护、App 更新等运营公告不是 earn
- crypto: 泛 crypto（BTC/ETH/交易所/监管/空投/安全/链上事件/爆仓/鲸鱼/代币上线 等）
- stock: 股票/财报/估值/IPO/美股/港股
- macro: 美联储/CPI/PPI/利率/关税/地缘政治/油价/黄金
- ai: AI/大模型/算力/Agent
- tech: 科技商业/消费/SaaS/硬件/汽车/电商/社交/游戏/物流/零售/医疗科技等产业动态（不属于上述 6 类但与科技公司相关的）

## 实体（entities）— 提取文本中提到的具体名称
提取：代币（BTC/ETH/USDC/sRUSDe…）、协议（Pendle/Aave/Morpho…）、交易所（Binance/Hyperliquid…）、链（Ethereum/Solana/Arbitrum…）、人物、公司
- 统一小写，去掉 $ 前缀
- 只提取明确出现的，不推断
- 无实体则留空

## 规则
- 提到 BTC/ETH/代币/交易所/链上 → 至少打 crypto
- 提到利率/通胀/美联储/地缘/油价 → 至少打 macro
- 交易所衍生品规则变更（funding rate/资金费率/保证金/结算频率/杠杆限制/风控规则/liquidation） → 打 crypto + earn（影响对冲和套利策略成本）
- 纯广告/无意义转发/非中英文 → topics 和 entities 都留空

## 输出格式（严格遵循，每条一行）
序号: topics=标签1,标签2 | entities=实体1,实体2,实体3
无内容则写：序号: topics= | entities=

{texts}"""

# 每张表的 (表名, 文本列SQL, 主键列)
TEXT_TABLES = [
    ("tweets", "content", "id"),
    ("announcements", "title || ' ' || COALESCE(body_text, '')", "id"),
    ("news", "title || ' ' || COALESCE(body_text, '')", "id"),
    ("reddit_posts", "title", "id"),
    ("kb_news", "subject || ' ' || COALESCE(SUBSTR(content, 1, 200), '')", "id"),
]
