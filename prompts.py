"""集中管理 LLM prompt + 可调配置"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Classifier
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASSIFIER_MODEL = "haiku"
CLASSIFIER_BATCH_SIZE = 20
VALID_TOPICS = {"defi", "earn", "crypto", "stock", "macro", "ai", "tech", "security"}

CLASSIFY_PROMPT = """对以下文本打标签并提取实体。

## 标签（topics）— 从这 7 个中选，可多选
- defi: DeFi 协议动态（yield/tvl/staking/lending/pool/swap/链上协议/Morpho/Pendle/Aave/Uniswap 等）
- earn: **可操作的收益机会**（有明确 APR/APY/收益率 + 参与方式），或**影响收益策略的规则变更**（资金费率/保证金/结算频率/费率上下限/杠杆调整等衍生品交易规则）。注意：代币上线、法币通道、支付方式、系统维护、App 更新等运营公告不是 earn
- crypto: 泛 crypto（BTC/ETH/交易所/监管/空投/安全/链上事件/爆仓/鲸鱼/代币上线 等）
- stock: 股票/财报/估值/IPO/美股/港股
- macro: 美联储/CPI/PPI/利率/关税/地缘政治/油价/黄金
- ai: AI/大模型/算力/Agent
- tech: 科技商业/消费/SaaS/硬件/汽车/电商/社交/游戏/物流/零售/医疗科技等产业动态（不属于上述 6 类但与科技公司相关的）
- security: 安全事件/漏洞/exploit/hack/drain/rug pull/资金被盗/审计/admin key/协议暂停/紧急暂停/资金异常流出

## 实体（entities）— 提取文本中提到的具体名称
提取：代币（BTC/ETH/USDC/sRUSDe…）、协议（Pendle/Aave/Morpho…）、交易所（Binance/Hyperliquid…）、链（Ethereum/Solana/Arbitrum…）、人物、公司
- 统一小写，去掉 $ 前缀
- 只提取明确出现的，不推断
- 无实体则留空

## 规则
- 提到 BTC/ETH/代币/交易所/链上 → 至少打 crypto
- 提到利率/通胀/美联储/地缘/油价 → 至少打 macro
- 交易所衍生品规则变更（funding rate/资金费率/保证金/结算频率/杠杆限制/风控规则/liquidation） → 打 crypto + earn（影响对冲和套利策略成本）
- 提到 exploit/hack/drain/rug/stolen/vulnerability/compromised/emergency/paused → 至少打 security + crypto
- 纯广告/无意义转发/非中英文 → topics 和 entities 都留空

## 情绪（sentiment）— 仅对 Reddit 帖子输出
- -2: 极度恐慌/绝望（"I'm done", "lost everything", "market is dead"）
- -1: 偏空/担忧（selloff, correction, bearish, hack, risk）
- 0: 中性/信息性（教程、数据、问答、日常讨论）
- +1: 偏多/乐观（bullish, recovery, buying the dip, undervalued）
- +2: 极度贪婪/FOMO（"to the moon", "all in", "generational opportunity"）
判断标准：看作者的情绪倾向，不看标题里提到的事件本身。比如"Drift hacked for $200M"如果只是客观报道=0，如果加了"DeFi is dead"=-2

## 明确度（explicitness）— 仅对 Reddit 帖子输出，判断情绪表达的明确程度
- strong: 明确的情绪表达（"I'm done", "to the moon", "lost everything", "bullish af", "this is the end"）
- moderate: 隐含情绪（负面新闻标题、乐观数据报道、反问语气）
- weak: 模糊/中性（日常讨论帖、问答帖、纯数据帖、Daily Discussion Thread）

## 输出格式（严格遵循，每条一行）
序号: topics=标签1,标签2 | entities=实体1,实体2,实体3 | sentiment=数字 | explicitness=档位
无内容则写：序号: topics= | entities= | sentiment=0 | explicitness=weak

{texts}"""

# 每张表的 (表名, 文本列SQL, 主键列)
TEXT_TABLES = [
    ("tweets", "content", "id"),
    ("announcements", "title || ' ' || COALESCE(body_text, '')", "id"),
    ("news", "title || ' ' || COALESCE(body_text, '')", "id"),
    ("reddit_posts", "title", "id"),
    ("kb_news", "subject || ' ' || COALESCE(SUBSTR(content, 1, 200), '')", "id"),
]
