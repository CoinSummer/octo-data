"""FastAPI 路由 — 仅市场数据"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from db import Database


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Octo-Data 信源看板</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--border:#30363d;--text:#e6edf3;--dim:#8b949e;--green:#3fb950;--yellow:#d29922;--red:#f85149;--blue:#58a6ff;--purple:#bc8cff}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;font-size:14px;line-height:1.5;padding:20px}
h1{font-size:20px;font-weight:600;margin-bottom:16px;display:flex;align-items:center;gap:8px}
h2{font-size:15px;font-weight:600;margin-bottom:10px;color:var(--dim)}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:16px}
.card-full{grid-column:1/-1}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);color:var(--dim);font-weight:500}
td{padding:6px 8px;border-bottom:1px solid var(--border)}
tr:last-child td{border-bottom:none}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px}
.dot-green{background:var(--green)}.dot-yellow{background:var(--yellow)}.dot-red{background:var(--red)}
.tag{display:inline-block;padding:1px 6px;border-radius:3px;font-size:11px;margin:1px 2px;background:var(--border);color:var(--text)}
.tag-defi{background:#1f3a2a;color:var(--green)}.tag-earn{background:#2a3a1f;color:#7ee787}
.tag-crypto{background:#1f2a3a;color:var(--blue)}.tag-security{background:#3a1f1f;color:var(--red)}
.tag-macro{background:#3a2a1f;color:var(--yellow)}.tag-stock{background:#2a1f3a;color:var(--purple)}
.tag-ai{background:#1f3a3a;color:#56d4dd}.tag-tech{background:#2a2a1f;color:#d2a822}
.bar-bg{background:var(--border);border-radius:3px;height:6px;width:100%;margin-top:4px}
.bar-fill{background:var(--green);border-radius:3px;height:6px;transition:width .3s}
.mono{font-family:ui-monospace,SFMono-Regular,monospace;font-size:12px}
.text-dim{color:var(--dim)}.text-green{color:var(--green)}.text-red{color:var(--red)}.text-yellow{color:var(--yellow)}
.err{font-size:11px;color:var(--red);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.preview{max-width:400px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.stats{display:flex;gap:24px;margin-bottom:12px}
.stat-item{text-align:center}.stat-val{font-size:22px;font-weight:700}.stat-label{font-size:12px;color:var(--dim)}
.refresh{font-size:12px;color:var(--dim);margin-left:auto}
.source-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px}
.source-header h2{margin-bottom:0}
.source-count{font-size:12px;color:var(--dim)}
.topic-dist{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px}
</style>
</head>
<body>
<h1>Octo-Data 信源看板 <span class="refresh" id="timer">60s</span></h1>

<div class="stats" id="overview"></div>

<div class="card card-full" style="margin-bottom:16px">
<h2>Fetcher 状态</h2>
<table><thead><tr><th>名称</th><th>状态</th><th>最近运行</th><th>成功次数</th><th>错误</th><th>最近错误</th></tr></thead>
<tbody id="fetchers"></tbody></table>
</div>

<div class="card card-full" style="margin-bottom:16px">
<h2>分类覆盖率</h2>
<table><thead><tr><th>数据表</th><th>总量</th><th>已分类</th><th>覆盖率</th><th>最新数据</th><th>Topic 分布</th></tr></thead>
<tbody id="classifier"></tbody></table>
</div>

<div class="grid" id="sources"></div>

<script>
const CONTENT_SOURCES=[
  {key:'tweets',label:'Tweets',endpoint:'/tweets/latest?limit=3',textField:'content',authorField:'username',tsField:'ts'},
  {key:'kb_news',label:'KB News',endpoint:'/kb-news/latest?limit=3',textField:'subject',authorField:'source_name',tsField:'ts'},
  {key:'announcements',label:'公告',endpoint:'/announcements/latest?limit=3',textField:'title',authorField:'source',tsField:'ts'},
  {key:'news',label:'News/RSS',endpoint:'/news/latest?limit=3',textField:'title',authorField:'source',tsField:'ts'},
  {key:'reddit_posts',label:'Reddit',endpoint:'/reddit/latest?limit=3',textField:'title',authorField:'author',tsField:'ts'},
];

function ago(ts){
  if(!ts)return'-';
  const d=new Date(ts+'Z'),now=new Date(),diff=(now-d)/1000;
  if(diff<60)return Math.floor(diff)+'s ago';
  if(diff<3600)return Math.floor(diff/60)+'m ago';
  if(diff<86400)return Math.floor(diff/3600)+'h ago';
  return Math.floor(diff/86400)+'d ago';
}

function localTS(ts){
  if(!ts)return'-';
  try{const d=new Date(ts.includes('T')?ts:ts+'Z');return d.toLocaleString('zh-CN',{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'})}catch(e){return ts}
}

function topicTag(t){return `<span class="tag tag-${t}">${t}</span>`}

function topicTags(topics){
  if(!topics||topics==='_none')return'<span class="text-dim">-</span>';
  return topics.split(',').map(t=>t.trim()).filter(Boolean).map(topicTag).join('');
}

async function loadStatus(){
  const r=await fetch('/status');const d=await r.json();
  const fetchers=d.fetchers||[];const tables=d.tables||[];
  const totalRows=tables.reduce((s,t)=>s+t.count,0);
  const classifier=fetchers.find(f=>f.name==='classifier');
  const healthyCount=fetchers.filter(f=>f.error_count===0).length;

  document.getElementById('overview').innerHTML=`
    <div class="stat-item"><div class="stat-val">${fetchers.length}</div><div class="stat-label">Fetchers</div></div>
    <div class="stat-item"><div class="stat-val text-green">${healthyCount}</div><div class="stat-label">健康</div></div>
    <div class="stat-item"><div class="stat-val">${(totalRows/1000).toFixed(1)}k</div><div class="stat-label">总记录</div></div>
    <div class="stat-item"><div class="stat-val">${classifier?ago(classifier.last_run):'-'}</div><div class="stat-label">Classifier</div></div>
  `;

  const tbody=document.getElementById('fetchers');
  tbody.innerHTML=fetchers.map(f=>{
    const isOk=f.error_count===0;
    const dotClass=isOk?'dot-green':f.error_count>100?'dot-red':'dot-yellow';
    return `<tr>
      <td><span class="dot ${dotClass}"></span>${f.name}</td>
      <td class="${isOk?'text-green':'text-yellow'}">${isOk?'OK':'⚠ '+f.error_count}</td>
      <td class="mono">${localTS(f.last_run)}</td>
      <td class="mono">${f.run_count}</td>
      <td class="mono">${f.error_count}</td>
      <td class="err" title="${(f.last_error||'').replace(/"/g,'&quot;')}">${f.last_error ? '<span class="text-dim" style="margin-right:4px">' + localTS(f.last_error_at) + '</span>' + f.last_error : '-'}</td>
    </tr>`;
  }).join('');
}

async function loadClassifier(){
  const r=await fetch('/classifier/stats');const d=await r.json();
  const tbody=document.getElementById('classifier');
  tbody.innerHTML=(d.data||[]).map(t=>{
    const pct=t.coverage_pct;
    const barColor=pct>95?'var(--green)':pct>80?'var(--yellow)':'var(--red)';
    const distHtml=Object.entries(t.topics_dist||{}).sort((a,b)=>b[1]-a[1])
      .map(([k,v])=>`<span class="tag tag-${k}">${k}: ${v}</span>`).join('');
    return `<tr>
      <td>${t.table}</td>
      <td class="mono">${t.total.toLocaleString()}</td>
      <td class="mono">${t.classified.toLocaleString()}</td>
      <td><div style="display:flex;align-items:center;gap:8px"><span class="mono">${pct}%</span><div class="bar-bg" style="width:80px"><div class="bar-fill" style="width:${pct}%;background:${barColor}"></div></div></div></td>
      <td class="mono">${ago(t.latest_ts)}</td>
      <td><div class="topic-dist">${distHtml||'-'}</div></td>
    </tr>`;
  }).join('');
}

async function loadSources(){
  const container=document.getElementById('sources');
  container.innerHTML='';
  for(const src of CONTENT_SOURCES){
    try{
      const r=await fetch(src.endpoint);const d=await r.json();
      const items=d.data||[];const total=d.total||items.length;
      let rows='';
      if(items.length===0){
        rows='<tr><td colspan="3" class="text-dim" style="text-align:center">暂无数据</td></tr>';
      }else{
        rows=items.map(item=>{
          const text=item[src.textField]||item.title||item.content||item.subject||'';
          const author=item[src.authorField]||'';
          const ts=item[src.tsField]||'';
          const topics=item.topics||'';
          return `<tr>
            <td class="mono" style="white-space:nowrap">${localTS(ts)}</td>
            <td><div class="preview">${text.substring(0,80)}</div><div style="margin-top:2px">${topicTags(topics)}</div></td>
            <td class="text-dim">${author}</td>
          </tr>`;
        }).join('');
      }
      container.innerHTML+=`<div class="card">
        <div class="source-header"><h2>${src.label}</h2><span class="source-count">${total.toLocaleString()} 条</span></div>
        <table><thead><tr><th style="width:90px">时间</th><th>内容</th><th style="width:80px">来源</th></tr></thead>
        <tbody>${rows}</tbody></table>
      </div>`;
    }catch(e){
      container.innerHTML+=`<div class="card"><h2>${src.label}</h2><p class="text-red">加载失败: ${e.message}</p></div>`;
    }
  }
}

let countdown=60;
function tick(){
  countdown--;
  document.getElementById('timer').textContent=countdown+'s';
  if(countdown<=0){countdown=60;refresh();}
}

async function refresh(){
  await Promise.all([loadStatus(),loadClassifier(),loadSources()]);
}

refresh();
setInterval(tick,1000);
</script>
</body>
</html>"""


def create_app(db: Database) -> FastAPI:
    app = FastAPI(title="DataHub Market", version="1.0.0")

    import os
    cors_origins = os.getenv("CORS_ORIGINS", "*").split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    # ── Prices ──

    @app.get("/prices/latest")
    def prices_latest(symbols: Optional[str] = Query(None, description="逗号分隔: BTC,ETH")):
        if symbols:
            symbol_list = [s.strip().upper() for s in symbols.split(",")]
            placeholders = ",".join("?" * len(symbol_list))
            rows = db.fetchall(f"""
                SELECT p.* FROM prices p
                INNER JOIN (
                    SELECT symbol, MAX(ts) as max_ts FROM prices
                    WHERE symbol IN ({placeholders})
                    GROUP BY symbol
                ) latest ON p.symbol = latest.symbol AND p.ts = latest.max_ts
            """, tuple(symbol_list))
        else:
            rows = db.fetchall("""
                SELECT p.* FROM prices p
                INNER JOIN (
                    SELECT symbol, MAX(ts) as max_ts FROM prices GROUP BY symbol
                ) latest ON p.symbol = latest.symbol AND p.ts = latest.max_ts
            """)
        return {"data": rows, "total": len(rows)}

    @app.get("/prices")
    def prices_history(
        symbol: str = Query(..., description="BTC"),
        start: Optional[str] = Query(None, alias="from"),
        to: Optional[str] = Query(None),
        limit: int = Query(500),
    ):
        sql = "SELECT * FROM prices WHERE symbol = ?"
        params = [symbol.upper()]
        if start:
            sql += " AND ts >= ?"
            params.append(start)
        if to:
            sql += " AND ts <= ?"
            params.append(to)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── Fear & Greed ──

    @app.get("/fear-greed/latest")
    def fear_greed_latest():
        row = db.fetchone("SELECT * FROM fear_greed ORDER BY ts DESC LIMIT 1")
        return {"data": row}

    @app.get("/fear-greed")
    def fear_greed_history(
        start: Optional[str] = Query(None, alias="from"),
        to: Optional[str] = Query(None),
        limit: int = Query(100),
    ):
        sql = "SELECT * FROM fear_greed WHERE 1=1"
        params = []
        if start:
            sql += " AND ts >= ?"
            params.append(start)
        if to:
            sql += " AND ts <= ?"
            params.append(to)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── Funding Rates ──

    @app.get("/funding-rates/latest")
    def funding_rates_latest(symbols: Optional[str] = Query(None)):
        if symbols:
            symbol_list = [s.strip().upper() for s in symbols.split(",")]
            placeholders = ",".join("?" * len(symbol_list))
            rows = db.fetchall(f"""
                SELECT f.* FROM funding_rates f
                INNER JOIN (
                    SELECT symbol, MAX(ts) as max_ts FROM funding_rates
                    WHERE symbol IN ({placeholders})
                    GROUP BY symbol
                ) latest ON f.symbol = latest.symbol AND f.ts = latest.max_ts
            """, tuple(symbol_list))
        else:
            rows = db.fetchall("""
                SELECT f.* FROM funding_rates f
                INNER JOIN (
                    SELECT symbol, MAX(ts) as max_ts FROM funding_rates GROUP BY symbol
                ) latest ON f.symbol = latest.symbol AND f.ts = latest.max_ts
            """)
        return {"data": rows, "total": len(rows)}

    # ── Stablecoin ──

    @app.get("/stablecoin/latest")
    def stablecoin_latest():
        rows = db.fetchall("""
            SELECT s.* FROM stablecoin s
            INNER JOIN (
                SELECT symbol, MAX(ts) as max_ts FROM stablecoin GROUP BY symbol
            ) latest ON s.symbol = latest.symbol AND s.ts = latest.max_ts
            ORDER BY s.total_supply DESC
        """)
        return {"data": rows, "total": len(rows)}

    # ── Dominance ──

    @app.get("/dominance/latest")
    def dominance_latest():
        rows = db.fetchall("""
            SELECT d.* FROM dominance d
            INNER JOIN (
                SELECT symbol, MAX(ts) as max_ts FROM dominance GROUP BY symbol
            ) latest ON d.symbol = latest.symbol AND d.ts = latest.max_ts
            ORDER BY d.dominance_pct DESC
        """)
        return {"data": rows, "total": len(rows)}

    # ── DeFi TVL ──

    @app.get("/defi-tvl/latest")
    def defi_tvl_latest():
        rows = db.fetchall("""
            SELECT t.* FROM defi_tvl t
            INNER JOIN (
                SELECT chain, MAX(ts) as max_ts FROM defi_tvl GROUP BY chain
            ) latest ON t.chain = latest.chain AND t.ts = latest.max_ts
            ORDER BY t.tvl_usd DESC
        """)
        return {"data": rows, "total": len(rows)}

    # ── DeFi Yields ──

    @app.get("/defi-yields/latest")
    def defi_yields_latest(
        chain: Optional[str] = Query(None),
        project: Optional[str] = Query(None),
        asset_type: Optional[str] = Query(None, description="usd, eth, btc"),
        min_tvl: Optional[float] = Query(None),
        min_apy: Optional[float] = Query(None),
        il_risk: Optional[str] = Query(None, description="no = 无 IL"),
        limit: int = Query(200),
    ):
        latest = db.fetchone("SELECT MAX(snapshot_date) as d FROM defi_yields")
        if not latest or not latest["d"]:
            return {"data": [], "total": 0}
        date = latest["d"]

        sql = "SELECT * FROM defi_yields WHERE snapshot_date = ?"
        params: list = [date]

        if chain:
            sql += " AND chain = ?"
            params.append(chain)
        if project:
            sql += " AND project = ?"
            params.append(project)
        if asset_type:
            sql += " AND asset_type = ?"
            params.append(asset_type)
        if min_tvl is not None:
            sql += " AND tvl_usd >= ?"
            params.append(min_tvl)
        if min_apy is not None:
            sql += " AND apy >= ?"
            params.append(min_apy)
        if il_risk is not None:
            sql += " AND il_risk = ?"
            params.append(il_risk)

        sql += " ORDER BY apy DESC LIMIT ?"
        params.append(limit)

        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows), "snapshot_date": date}

    @app.get("/defi-yields/pool/{pool_id}")
    def defi_yields_pool_history(pool_id: str, days: int = Query(30)):
        rows = db.fetchall("""
            SELECT snapshot_date, tvl_usd, apy, apy_base, apy_reward, apy_mean_30d
            FROM defi_yields
            WHERE pool_id = ?
            ORDER BY snapshot_date DESC
            LIMIT ?
        """, (pool_id, days))
        return {"data": rows, "pool_id": pool_id}

    @app.get("/defi-yields/top")
    def defi_yields_top(
        asset_type: Optional[str] = Query(None, description="usd, eth, btc"),
        limit: int = Query(20),
    ):
        latest = db.fetchone("SELECT MAX(snapshot_date) as d FROM defi_yields")
        if not latest or not latest["d"]:
            return {"data": [], "total": 0}
        date = latest["d"]

        sql = """
            SELECT * FROM defi_yields
            WHERE snapshot_date = ? AND il_risk = 'no' AND tvl_usd >= 5000000
        """
        params: list = [date]
        if asset_type:
            sql += " AND asset_type = ?"
            params.append(asset_type)
        sql += " ORDER BY apy DESC LIMIT ?"
        params.append(limit)

        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows), "snapshot_date": date}

    # ── Announcements ──

    @app.get("/announcements/latest")
    def announcements_latest(
        limit: int = Query(20),
        source: Optional[str] = Query(None, description="binance, hyperliquid, or okx"),
    ):
        sql = "SELECT id, ts, catalog_name, title, body_text, source, url, created_at FROM announcements WHERE 1=1"
        params = []
        if source:
            sql += " AND source = ?"
            params.append(source)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    @app.get("/announcements")
    def announcements_query(
        day: Optional[str] = Query(None),
        catalog: Optional[str] = Query(None, description="New Cryptocurrency Listing"),
        keyword: Optional[str] = Query(None),
        source: Optional[str] = Query(None, description="binance, hyperliquid, or okx"),
        limit: int = Query(50),
    ):
        sql = "SELECT id, ts, catalog_name, title, body_text, source, url, created_at FROM announcements WHERE 1=1"
        params = []

        if day:
            sql += " AND date(ts) = date(?)"
            params.append(day)
        if catalog:
            sql += " AND catalog_name = ?"
            params.append(catalog)
        if keyword:
            sql += " AND (title LIKE ? OR body_text LIKE ?)"
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        if source:
            sql += " AND source = ?"
            params.append(source)

        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)

        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── News（一般媒体） ──

    @app.get("/news/latest")
    def news_latest(
        limit: int = Query(20),
        source: Optional[str] = Query(None, description="36kr, techcrunch, hackernews, latepost, odaily"),
    ):
        sql = "SELECT id, ts, catalog_name, title, body_text, source, url, created_at FROM news WHERE 1=1"
        params = []
        if source:
            sql += " AND source = ?"
            params.append(source)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    @app.get("/news")
    def news_query(
        day: Optional[str] = Query(None),
        keyword: Optional[str] = Query(None),
        source: Optional[str] = Query(None),
        limit: int = Query(50),
    ):
        sql = "SELECT id, ts, catalog_name, title, body_text, source, url, created_at FROM news WHERE 1=1"
        params = []
        if day:
            sql += " AND date(ts) = date(?)"
            params.append(day)
        if keyword:
            sql += " AND (title LIKE ? OR body_text LIKE ?)"
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        if source:
            sql += " AND source = ?"
            params.append(source)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── Polymarket ──

    @app.get("/polymarket/latest")
    def polymarket_latest(limit: int = Query(20)):
        latest_ts = db.fetchone("SELECT MAX(ts) as t FROM polymarket_markets")
        if not latest_ts or not latest_ts["t"]:
            return {"data": [], "total": 0}
        rows = db.fetchall("""
            SELECT question, yes_price, change_24h, change_1w, volume, liquidity, slug
            FROM polymarket_markets WHERE ts = ?
            ORDER BY volume DESC LIMIT ?
        """, (latest_ts["t"], limit))
        return {"data": rows, "total": len(rows), "ts": latest_ts["t"]}

    @app.get("/polymarket/movers")
    def polymarket_movers():
        rows = db.fetchall("""
            SELECT question, yes_price, change_24h, volume, slug
            FROM polymarket_markets
            WHERE ts = (SELECT MAX(ts) FROM polymarket_markets)
              AND ABS(change_24h) > 0.05
            ORDER BY ABS(change_24h) DESC
        """)
        return {"data": rows, "total": len(rows)}

    @app.get("/polymarket/macro")
    def polymarket_macro():
        rows = db.fetchall("""
            SELECT question, yes_price, change_24h, volume, slug
            FROM polymarket_markets
            WHERE ts = (SELECT MAX(ts) FROM polymarket_markets)
              AND (slug LIKE '%recession%' OR slug LIKE '%fed%' OR slug LIKE '%tariff%'
                   OR slug LIKE '%bitcoin-reserve%' OR slug LIKE '%china%bitcoin%'
                   OR slug LIKE '%capital-gains%' OR slug LIKE '%microstrategy%'
                   OR question LIKE '%Bitcoin%150%' OR question LIKE '%USDC%USDT%')
            ORDER BY volume DESC
        """)
        return {"data": rows, "total": len(rows)}

    @app.get("/polymarket/search")
    def polymarket_search(keyword: str = Query(...)):
        rows = db.fetchall("""
            SELECT question, yes_price, change_24h, volume, slug
            FROM polymarket_markets
            WHERE ts = (SELECT MAX(ts) FROM polymarket_markets)
              AND (question LIKE ? OR slug LIKE ?)
            ORDER BY volume DESC LIMIT 20
        """, (f"%{keyword}%", f"%{keyword}%"))
        return {"data": rows, "total": len(rows)}

    # ── Tweets ──

    @app.get("/tweets/latest")
    def tweets_latest(limit: int = Query(30)):
        rows = db.fetchall(
            "SELECT id, ts, username, content, tags, source_url, topics FROM tweets ORDER BY ts DESC LIMIT ?",
            (limit,),
        )
        return {"data": rows, "total": len(rows)}

    @app.get("/tweets")
    def tweets_query(
        keyword: Optional[str] = Query(None),
        username: Optional[str] = Query(None),
        start: Optional[str] = Query(None, alias="from"),
        to: Optional[str] = Query(None),
        limit: int = Query(50),
    ):
        sql = "SELECT id, ts, username, content, tags, source_url, topics FROM tweets WHERE 1=1"
        params = []
        if keyword:
            sql += " AND content LIKE ?"
            params.append(f"%{keyword}%")
        if username:
            sql += " AND username = ?"
            params.append(username)
        if start:
            sql += " AND ts >= ?"
            params.append(start)
        if to:
            sql += " AND ts <= ?"
            params.append(to)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── KB News ──

    @app.get("/kb-news/latest")
    def kb_news_latest(limit: int = Query(30)):
        rows = db.fetchall(
            "SELECT id, ts, subject, source_name, content, source_url, topics FROM kb_news ORDER BY ts DESC LIMIT ?",
            (limit,),
        )
        return {"data": rows, "total": len(rows)}

    @app.get("/kb-news")
    def kb_news_query(
        keyword: Optional[str] = Query(None),
        source: Optional[str] = Query(None),
        start: Optional[str] = Query(None, alias="from"),
        to: Optional[str] = Query(None),
        limit: int = Query(50),
    ):
        sql = "SELECT id, ts, subject, source_name, content, source_url, topics FROM kb_news WHERE 1=1"
        params = []
        if keyword:
            sql += " AND (subject LIKE ? OR content LIKE ?)"
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        if source:
            sql += " AND source_name = ?"
            params.append(source)
        if start:
            sql += " AND ts >= ?"
            params.append(start)
        if to:
            sql += " AND ts <= ?"
            params.append(to)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── Reddit ──

    @app.get("/reddit/latest")
    def reddit_latest(limit: int = Query(30)):
        rows = db.fetchall(
            "SELECT ts, subreddit, title, author, url, sentiment FROM reddit_posts ORDER BY ts DESC LIMIT ?",
            (limit,),
        )
        return {"data": rows, "total": len(rows)}

    @app.get("/reddit/sentiment")
    def reddit_sentiment(hours: int = Query(24)):
        """Reddit 情绪 v2 加权聚合。headline 0-100 + detail panel。"""
        from aggregator import compute_sentiment
        result = compute_sentiment(db, hours)
        if not result:
            return {"data": None, "message": "No scored posts in timeframe"}
        return {"data": result}

    @app.get("/reddit/trend")
    def reddit_trend(days: int = Query(30)):
        """Reddit 情绪日度趋势（从 reddit_sentiment_daily）。"""
        rows = db.fetchall(
            "SELECT date, score, weighted_avg, bull_bear_spread, post_count, "
            "btc_price, fng FROM reddit_sentiment_daily "
            "ORDER BY date DESC LIMIT ?",
            (days,),
        )
        return {"data": rows, "total": len(rows)}

    @app.get("/reddit")
    def reddit_query(
        keyword: Optional[str] = Query(None),
        subreddit: Optional[str] = Query(None),
        limit: int = Query(50),
    ):
        sql = "SELECT ts, subreddit, title, author, url, sentiment FROM reddit_posts WHERE 1=1"
        params = []
        if keyword:
            sql += " AND title LIKE ?"
            params.append(f"%{keyword}%")
        if subreddit:
            sql += " AND subreddit = ?"
            params.append(subreddit)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── 全文搜索 ──

    @app.get("/text/search")
    def text_search(keyword: str = Query(...), hours: int = Query(168), limit: int = Query(20)):
        results = []

        rows = db.fetchall(
            f"SELECT 'tweet' as source, ts, username as author, content as text FROM tweets "
            f"WHERE content LIKE ? AND ts >= datetime('now', '-{hours} hours') "
            f"ORDER BY ts DESC LIMIT ?",
            (f"%{keyword}%", limit),
        )
        results.extend(rows)

        rows = db.fetchall(
            f"SELECT 'news' as source, ts, source_name as author, subject as text FROM kb_news "
            f"WHERE (subject LIKE ? OR content LIKE ?) AND ts >= datetime('now', '-{hours} hours') "
            f"ORDER BY ts DESC LIMIT ?",
            (f"%{keyword}%", f"%{keyword}%", limit),
        )
        results.extend(rows)

        rows = db.fetchall(
            f"SELECT 'reddit' as source, ts, author, title as text FROM reddit_posts "
            f"WHERE title LIKE ? AND ts >= datetime('now', '-{hours} hours') "
            f"ORDER BY ts DESC LIMIT ?",
            (f"%{keyword}%", limit),
        )
        results.extend(rows)

        rows = db.fetchall(
            f"SELECT source, ts, catalog_name as author, title as text FROM announcements "
            f"WHERE (title LIKE ? OR body_text LIKE ?) AND ts >= datetime('now', '-{hours} hours') "
            f"ORDER BY ts DESC LIMIT ?",
            (f"%{keyword}%", f"%{keyword}%", limit),
        )
        results.extend(rows)

        rows = db.fetchall(
            f"SELECT source, ts, catalog_name as author, title as text FROM news "
            f"WHERE (title LIKE ? OR body_text LIKE ?) AND ts >= datetime('now', '-{hours} hours') "
            f"ORDER BY ts DESC LIMIT ?",
            (f"%{keyword}%", f"%{keyword}%", limit),
        )
        results.extend(rows)

        results.sort(key=lambda x: x.get("ts", ""), reverse=True)
        return {"data": results, "total": len(results)}

    # ── Signals (跨表 topic 查询，供 monitor 等消费端使用) ──

    @app.get("/signals")
    def signals_query(
        topics: str = Query("earn,defi", description="逗号分隔的 topic 过滤"),
        entity: Optional[str] = Query(None, description="按实体过滤，逗号分隔"),
        since: Optional[str] = Query(None, description="起始时间 YYYY-MM-DD HH:MM:SS"),
        hours: int = Query(4, description="如果没指定 since，回溯 N 小时"),
        limit: int = Query(100),
    ):
        """跨表查询已分类的文本信号，按 topics/entities 过滤。"""
        topic_list = [t.strip() for t in topics.split(",") if t.strip()]
        topic_placeholders = " OR ".join("topics LIKE ?" for _ in topic_list)
        topic_params = [f"%{t}%" for t in topic_list]

        # 可选 entity 过滤
        entity_filter = ""
        entity_params = []
        if entity:
            entity_list = [e.strip().lower() for e in entity.split(",") if e.strip()]
            if entity_list:
                entity_filter = " AND (" + " OR ".join("entities LIKE ?" for _ in entity_list) + ")"
                entity_params = [f"%{e}%" for e in entity_list]

        if since:
            cutoff = since
        else:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')

        per_query_params = [cutoff] + topic_params + entity_params

        queries = []

        # announcements
        queries.append(f"""
            SELECT 'announcements' AS source, ts, topics, entities,
                   title || CASE WHEN body_text != '' AND url != body_text THEN char(10) || SUBSTR(body_text, 1, 2000) ELSE '' END AS text,
                   catalog_name || ' (' || source || ')' AS author,
                   COALESCE(NULLIF(url, ''),
                        CASE WHEN code != '' AND source = 'binance' THEN 'https://www.binance.com/en/support/announcement/' || code
                             WHEN code != '' AND source = 'hyperliquid' THEN 'https://t.me/hyperliquid_announcements/' || code
                             ELSE '' END) AS url
            FROM announcements
            WHERE ts > ? AND ({topic_placeholders}){entity_filter}
        """)

        # tweets
        queries.append(f"""
            SELECT 'tweets' AS source, ts, topics, entities, content AS text,
                   username AS author, source_url AS url
            FROM tweets
            WHERE ts > ? AND ({topic_placeholders}){entity_filter}
        """)

        # kb_news
        queries.append(f"""
            SELECT 'kb_news' AS source, ts, topics, entities,
                   subject || CASE WHEN content != '' THEN char(10) || SUBSTR(content, 1, 500) ELSE '' END AS text,
                   source_name AS author, source_url AS url
            FROM kb_news
            WHERE ts > ? AND ({topic_placeholders}){entity_filter}
        """)

        # reddit
        queries.append(f"""
            SELECT 'reddit' AS source, ts, topics, entities, title AS text, author, url
            FROM reddit_posts
            WHERE ts > ? AND ({topic_placeholders}){entity_filter}
        """)

        # news（一般媒体，从 announcements 拆出）
        queries.append(f"""
            SELECT 'news' AS source, ts, topics, entities,
                   title || CASE WHEN body_text != '' THEN char(10) || SUBSTR(body_text, 1, 2000) ELSE '' END AS text,
                   catalog_name || ' (' || source || ')' AS author, url
            FROM news
            WHERE ts > ? AND ({topic_placeholders}){entity_filter}
        """)

        all_params = per_query_params * 5 + [limit]
        sql = " UNION ALL ".join(queries) + " ORDER BY ts DESC LIMIT ?"
        rows = db.fetchall(sql, tuple(all_params))
        return {"data": rows, "total": len(rows)}

    # ── Exchange Metrics ──

    @app.get("/exchange-metrics/latest")
    def exchange_metrics_latest():
        row = db.fetchone("SELECT * FROM exchange_metrics ORDER BY ts DESC LIMIT 1")
        return {"data": row}

    @app.get("/exchange-metrics")
    def exchange_metrics_history(
        start: Optional[str] = Query(None, alias="from"),
        to: Optional[str] = Query(None),
        limit: int = Query(720, description="默认 720 条 = 30 天"),
    ):
        sql = "SELECT * FROM exchange_metrics WHERE 1=1"
        params = []
        if start:
            sql += " AND ts >= ?"
            params.append(start)
        if to:
            sql += " AND ts <= ?"
            params.append(to)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = db.fetchall(sql, tuple(params))
        return {"data": rows, "total": len(rows)}

    # ── System ──

    @app.get("/status")
    def status():
        fetchers = db.fetchall("SELECT * FROM fetcher_status ORDER BY name")
        tables = db.table_stats()
        return {"fetchers": fetchers, "tables": tables}

    @app.get("/tables")
    def tables():
        return {"data": db.table_stats()}

    # ── Monitor Events ──

    @app.get("/events")
    def events(
        status: str = Query("active", description="active / expired / all"),
        limit: int = Query(50),
    ):
        """monitor_events 表：Crypto/DeFi Monitor 提取的事件记忆。"""
        if status == "all":
            rows = db.fetchall(
                "SELECT * FROM monitor_events ORDER BY last_pushed DESC LIMIT ?",
                (limit,),
            )
        else:
            rows = db.fetchall(
                "SELECT * FROM monitor_events WHERE status = ? ORDER BY last_pushed DESC LIMIT ?",
                (status, limit),
            )
        return {"data": rows}

    # ── Classifier Stats ──

    @app.get("/classifier/stats")
    def classifier_stats():
        """各内容表的分类覆盖率 + topic 分布。"""
        content_tables = {
            "tweets": "content",
            "announcements": "title",
            "news": "title",
            "reddit_posts": "title",
            "kb_news": "subject",
        }
        valid_topics = ["defi", "earn", "crypto", "stock", "macro", "ai", "tech", "security"]
        results = []
        for table, text_col in content_tables.items():
            row = db.fetchone(f"""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN topics != '' AND topics IS NOT NULL AND topics != '_none' THEN 1 ELSE 0 END) as classified
                FROM {table}
            """)
            total = row["total"] if row else 0
            classified = row["classified"] if row else 0
            # topic 分布
            topics_dist = {}
            for t in valid_topics:
                cnt = db.fetchone(f"SELECT COUNT(*) as c FROM {table} WHERE topics LIKE ?", (f"%{t}%",))
                if cnt and cnt["c"] > 0:
                    topics_dist[t] = cnt["c"]
            # 最新数据时间
            latest = db.fetchone(f"SELECT ts FROM {table} ORDER BY ts DESC LIMIT 1")
            results.append({
                "table": table,
                "total": total,
                "classified": classified,
                "unclassified": total - classified,
                "coverage_pct": round(classified / total * 100, 1) if total > 0 else 0,
                "latest_ts": latest["ts"] if latest else None,
                "topics_dist": topics_dist,
            })
        return {"data": results}

    # ── Dashboard ──

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard():
        return DASHBOARD_HTML

    return app
