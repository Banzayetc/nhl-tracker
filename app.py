import os
import time
import logging
import sqlite3
from datetime import datetime

import requests
from flask import Flask, render_template_string, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

import json as _json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH           = os.environ.get("DB_PATH", "/tmp/sports.db")
POLL_INTERVAL_MIN = int(os.environ.get("POLL_INTERVAL", "60"))
TREND_MIN_DELTA   = float(os.environ.get("TREND_MIN_DELTA", "0.02"))   # 2¢ first→last
MAX_PULLBACK      = float(os.environ.get("MAX_PULLBACK", "0.015"))      # 1.5¢ max reversal
GAMMA_BASE        = "https://gamma-api.polymarket.com"

SPORTS = [
    {"key": "nhl",      "tag": "nhl",      "name": "NHL",      "emoji": "🏒", "url_path": "nhl"},
    {"key": "nba",      "tag": "nba",      "name": "NBA",      "emoji": "🏀", "url_path": "nba"},
    {"key": "soccer",   "tag": "soccer",   "name": "Soccer",   "emoji": "⚽", "url_path": "soccer"},
    {"key": "baseball", "tag": "baseball", "name": "Baseball", "emoji": "⚾", "url_path": "mlb"},
]

app = Flask(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_list(val):
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return _json.loads(val)
        except Exception:
            return []
    return []

# ── Database ──────────────────────────────────────────────────────────────────

def get_con():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    with get_con() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id   TEXT    NOT NULL,
                sport       TEXT    NOT NULL DEFAULT 'nhl',
                question    TEXT,
                team_a      TEXT,
                team_b      TEXT,
                match_start INTEGER,
                price_a     REAL,
                price_b     REAL,
                fetched_at  INTEGER NOT NULL,
                event_slug  TEXT
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_mf ON snapshots(market_id, fetched_at)")
        for col in ["event_slug", "sport"]:
            try:
                con.execute(f"ALTER TABLE snapshots ADD COLUMN {col} TEXT")
            except Exception:
                pass

# ── Polymarket API ────────────────────────────────────────────────────────────

def fetch_sport_markets(tag: str) -> list[dict]:
    results = []
    try:
        r = requests.get(
            f"{GAMMA_BASE}/events",
            params={"active": "true", "closed": "false", "tag_slug": tag, "limit": 100},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        events = data if isinstance(data, list) else data.get("events", [])

        for event in events:
            title = event.get("title", "")
            if "vs" not in title.lower():
                continue

            markets = event.get("markets", [])
            target = None

            for m in markets:
                q = (m.get("question") or "").lower()
                outcomes = _parse_list(m.get("outcomes", []))
                prices   = _parse_list(m.get("outcomePrices", []))
                if ("moneyline" in q or q == title.lower()) and len(outcomes) == 2 and len(prices) == 2:
                    target = m
                    break

            if not target:
                for m in markets:
                    outcomes = _parse_list(m.get("outcomes", []))
                    prices   = _parse_list(m.get("outcomePrices", []))
                    if len(outcomes) == 2 and len(prices) == 2:
                        target = m
                        break

            if not target:
                continue

            outcomes = _parse_list(target.get("outcomes", []))
            prices   = _parse_list(target.get("outcomePrices", []))
            if len(outcomes) != 2 or len(prices) != 2:
                continue

            try:
                tokens = [
                    {"outcome": outcomes[0], "price": float(prices[0])},
                    {"outcome": outcomes[1], "price": float(prices[1])},
                ]
            except (ValueError, TypeError):
                continue

            gst = target.get("gameStartTime") or event.get("startDate")
            if not gst:
                continue

            target["tokens"]        = tokens
            target["gameStartTime"] = gst
            target["event_slug"]    = event.get("slug", "")
            target["sport"]         = tag
            if not target.get("question"):
                target["question"] = title
            results.append(target)

    except Exception as e:
        log.error(f"fetch_sport_markets({tag}): {e}")

    log.info(f"fetch {tag}: {len(results)} markets")
    return results

def parse_game_start(m: dict):
    for field in ("gameStartTime", "startDate", "endDate"):
        val = m.get(field)
        if not val:
            continue
        try:
            val = val.strip().replace(" ", "T")
            if val.endswith("+00"):
                val += ":00"
            return int(datetime.fromisoformat(val).timestamp())
        except Exception:
            continue
    return None

def parse_market(m: dict):
    try:
        tokens = m.get("tokens", [])
        if len(tokens) < 2:
            return None
        match_start = parse_game_start(m)
        if not match_start:
            return None
        price_a = float(tokens[0].get("price") or 0)
        price_b = float(tokens[1].get("price") or 0)
        if price_a == 0 and price_b == 0:
            return None
        return {
            "market_id":   str(m.get("conditionId") or m.get("id") or ""),
            "sport":       m.get("sport", "nhl"),
            "question":    m.get("question", ""),
            "team_a":      tokens[0].get("outcome", "Team A"),
            "team_b":      tokens[1].get("outcome", "Team B"),
            "match_start": match_start,
            "price_a":     price_a,
            "price_b":     price_b,
            "event_slug":  m.get("event_slug", ""),
        }
    except Exception as e:
        log.warning(f"parse_market error: {e}")
        return None

# ── Monotonicity check ────────────────────────────────────────────────────────

def is_monotone(prices: list[float], direction: int, max_pullback: float) -> bool:
    """
    direction: +1 = uptrend, -1 = downtrend
    Returns True if price never reverses more than max_pullback.
    """
    if len(prices) < 2:
        return False
    extreme = prices[0]
    for p in prices[1:]:
        if direction == 1:
            extreme = max(extreme, p)
            if extreme - p > max_pullback:
                return False
        else:
            extreme = min(extreme, p)
            if p - extreme > max_pullback:
                return False
    return True

# ── Scheduler job ─────────────────────────────────────────────────────────────

def snapshot_markets():
    log.info("Snapshotting all sports …")
    now = int(time.time())
    total_saved = 0

    for sport in SPORTS:
        markets = fetch_sport_markets(sport["tag"])
        saved = 0
        with get_con() as con:
            for m in markets:
                parsed = parse_market(m)
                if not parsed:
                    continue
                hours = (parsed["match_start"] - now) / 3600
                if hours < 0 or hours > 72:
                    continue
                con.execute(
                    """INSERT INTO snapshots
                       (market_id, sport, question, team_a, team_b,
                        match_start, price_a, price_b, fetched_at, event_slug)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (parsed["market_id"], parsed["sport"], parsed["question"],
                     parsed["team_a"], parsed["team_b"], parsed["match_start"],
                     parsed["price_a"], parsed["price_b"], now, parsed["event_slug"]),
                )
                saved += 1
        log.info(f"  {sport['tag']}: {saved} saved")
        total_saved += saved

    log.info(f"Snapshot done: {total_saved} total")

# ── Trend detection ───────────────────────────────────────────────────────────

def get_trending_matches(sport_key: str = None) -> list[dict]:
    now = int(time.time())
    match_min   = now + 12 * 3600
    match_max   = now + 48 * 3600
    snap_window = now - 48 * 3600   # look back 48h

    with get_con() as con:
        if sport_key:
            rows = con.execute(
                """SELECT DISTINCT market_id FROM snapshots
                   WHERE sport = ? AND match_start BETWEEN ? AND ?
                     AND fetched_at >= ?""",
                (sport_key, match_min, match_max, snap_window),
            ).fetchall()
        else:
            rows = con.execute(
                """SELECT DISTINCT market_id FROM snapshots
                   WHERE match_start BETWEEN ? AND ?
                     AND fetched_at >= ?""",
                (match_min, match_max, snap_window),
            ).fetchall()

        market_ids = [r[0] for r in rows]
        results = []

        for mid in market_ids:
            snaps = con.execute(
                """SELECT price_a, price_b, fetched_at, question, team_a, team_b,
                          match_start, event_slug, sport
                   FROM snapshots
                   WHERE market_id = ? AND fetched_at >= ?
                   ORDER BY fetched_at ASC""",
                (mid, snap_window),
            ).fetchall()

            if len(snaps) < 2:
                continue

            first, last = snaps[0], snaps[-1]
            delta_a = last["price_a"] - first["price_a"]

            # Must meet minimum delta threshold
            if abs(delta_a) < TREND_MIN_DELTA:
                continue

            # Monotonicity check
            prices_a = [s["price_a"] for s in snaps]
            direction = 1 if delta_a > 0 else -1
            if not is_monotone(prices_a, direction, MAX_PULLBACK):
                continue

            trending_team = last["team_a"] if delta_a > 0 else last["team_b"]
            fading_team   = last["team_b"] if delta_a > 0 else last["team_a"]
            hours_left    = round((last["match_start"] - now) / 3600, 1)
            sport_tag     = last["sport"] or "nhl"

            # Build Polymarket URL
            slug = last["event_slug"] or ""
            sport_info = next((s for s in SPORTS if s["key"] == sport_tag), SPORTS[0])
            polymarket_url = (
                f"https://polymarket.com/sports/{sport_info['url_path']}/{slug}"
                if slug else f"https://polymarket.com/sports/{sport_info['url_path']}"
            )

            results.append({
                "market_id":      mid,
                "sport":          sport_tag,
                "sport_emoji":    sport_info["emoji"],
                "sport_name":     sport_info["name"],
                "question":       last["question"],
                "team_a":         last["team_a"],
                "team_b":         last["team_b"],
                "match_start":    last["match_start"],
                "hours_left":     hours_left,
                "delta_cents":    round(delta_a * 100, 1),
                "price_a":        round(last["price_a"], 3),
                "price_b":        round(last["price_b"], 3),
                "trending_team":  trending_team,
                "fading_team":    fading_team,
                "polymarket_url": polymarket_url,
                "history": [
                    {"t": s["fetched_at"] * 1000, "p": round(s["price_a"], 3)}
                    for s in snaps
                ],
            })

    return sorted(results, key=lambda x: abs(x["delta_cents"]), reverse=True)

# ── HTML ──────────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sports Trend Tracker</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: #0a0a0a;
  color: #ddd;
  font-family: 'Courier New', monospace;
  padding: 24px 20px;
  min-height: 100vh;
}
h1 { color: #00d4ff; font-size: 1.2rem; letter-spacing: 3px; margin-bottom: 4px; }
.sub { color: #333; font-size: 0.72rem; margin-bottom: 20px; }
.tabs {
  display: flex;
  gap: 8px;
  margin-bottom: 24px;
  flex-wrap: wrap;
}
.tab {
  padding: 6px 18px;
  border-radius: 20px;
  font-size: 0.75rem;
  cursor: pointer;
  border: 1px solid #1e1e1e;
  background: #111;
  color: #555;
  letter-spacing: 1px;
  transition: all 0.2s;
}
.tab.active, .tab:hover {
  border-color: #00d4ff44;
  color: #00d4ff;
  background: #00d4ff0a;
}
.tab .count {
  margin-left: 6px;
  font-size: 0.65rem;
  color: #333;
}
.tab.active .count { color: #00d4ff88; }
.section { margin-bottom: 32px; }
.section-header {
  font-size: 0.7rem;
  color: #333;
  letter-spacing: 2px;
  margin-bottom: 14px;
  padding-bottom: 6px;
  border-bottom: 1px solid #141414;
}
.grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
  gap: 16px;
}
.card {
  background: #111;
  border: 1px solid #1a1a1a;
  border-radius: 10px;
  padding: 18px;
}
.card.strong { border-color: #00d4ff22; }
.badge {
  display: inline-block;
  font-size: 0.65rem;
  color: #00d4ff;
  background: #00d4ff0a;
  border: 1px solid #00d4ff1a;
  border-radius: 20px;
  padding: 2px 10px;
  margin-bottom: 10px;
}
.teams { font-size: 0.95rem; font-weight: bold; margin-bottom: 3px; }
.vs { color: #2a2a2a; margin: 0 6px; }
.match-time { font-size: 0.68rem; color: #444; margin-bottom: 10px; }
.delta { font-size: 1.3rem; font-weight: bold; margin-bottom: 3px; }
.delta.up { color: #22c55e; }
.delta.down { color: #ef4444; }
.trend-label { font-size: 0.72rem; color: #555; margin-bottom: 12px; }
.trend-label strong { color: #999; }
.prices { display: flex; gap: 8px; font-size: 0.72rem; margin-bottom: 14px; flex-wrap: wrap; }
.prices span {
  background: #161616;
  border: 1px solid #1e1e1e;
  padding: 3px 10px;
  border-radius: 5px;
  color: #777;
}
.mv-up   { color: #22c55e; font-weight: bold; }
.mv-down { color: #ef4444; font-weight: bold; }
.pm-link {
  display: block;
  margin-top: 10px;
  font-size: 0.68rem;
  color: #00d4ff33;
  text-decoration: none;
  letter-spacing: 1px;
  transition: color 0.15s;
}
.pm-link:hover { color: #00d4ff; }
.empty {
  text-align: center;
  padding: 60px 20px;
  color: #252525;
  line-height: 2.2;
  font-size: 0.85rem;
}
footer { margin-top: 24px; font-size: 0.68rem; color: #1e1e1e; text-align: right; }
</style>
</head>
<body>
<h1>📈 SPORTS TREND TRACKER</h1>
<p class="sub">Polymarket · монотонный тренд ≥ 2¢ · откат &lt; 1.5¢ · матчи через 12–48ч</p>

<div class="tabs" id="tabs"></div>
<div id="root"></div>
<footer id="ts"></footer>

<script>
const ALL = {{ data | tojson }};

const SPORTS = [
  { key: "all",      name: "Все",      emoji: "📊" },
  { key: "nhl",      name: "NHL",      emoji: "🏒" },
  { key: "nba",      name: "NBA",      emoji: "🏀" },
  { key: "soccer",   name: "Soccer",   emoji: "⚽" },
  { key: "baseball", name: "Baseball", emoji: "⚾" },
];

let activeTab = "all";

function fmtDate(ts) {
  return new Date(ts * 1000).toLocaleString("ru-RU", {
    day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit"
  });
}

function renderTabs() {
  const tabs = document.getElementById("tabs");
  tabs.innerHTML = "";
  SPORTS.forEach(s => {
    const count = s.key === "all" ? ALL.length : ALL.filter(m => m.sport === s.key).length;
    const btn = document.createElement("div");
    btn.className = "tab" + (activeTab === s.key ? " active" : "");
    btn.innerHTML = `${s.emoji} ${s.name}<span class="count">${count}</span>`;
    btn.onclick = () => { activeTab = s.key; renderAll(); };
    tabs.appendChild(btn);
  });
}

function renderAll() {
  renderTabs();
  const root = document.getElementById("root");
  root.innerHTML = "";

  const matches = activeTab === "all" ? ALL : ALL.filter(m => m.sport === activeTab);

  if (!matches.length) {
    root.innerHTML = `<div class="empty">Нет матчей с монотонным трендом ≥ 2¢<br>Данные накапливаются — зайди через пару часов</div>`;
    return;
  }

  // Group by sport if showing all
  const groups = activeTab === "all"
    ? SPORTS.filter(s => s.key !== "all").map(s => ({
        sport: s, items: matches.filter(m => m.sport === s.key)
      })).filter(g => g.items.length)
    : [{ sport: SPORTS.find(s => s.key === activeTab), items: matches }];

  let chartIdx = 0;
  groups.forEach(({ sport, items }) => {
    const section = document.createElement("div");
    section.className = "section";

    if (activeTab === "all") {
      const hdr = document.createElement("div");
      hdr.className = "section-header";
      hdr.textContent = `${sport.emoji} ${sport.name} — ${items.length} матч${items.length > 1 ? 'а' : ''}`;
      section.appendChild(hdr);
    }

    const grid = document.createElement("div");
    grid.className = "grid";

    items.forEach(m => {
      const i = chartIdx++;
      const isUp  = m.delta_cents > 0;
      const arrow = isUp ? "▲" : "▼";
      const cls   = isUp ? "up" : "down";
      const strong = Math.abs(m.delta_cents) >= 5 ? " strong" : "";

      const priceAStart = m.history.length ? +(m.history[0].p * 100).toFixed(1) : null;
      const priceANow   = +(m.price_a * 100).toFixed(1);
      const priceBStart = priceAStart !== null ? +(100 - priceAStart).toFixed(1) : null;
      const priceBNow   = +(m.price_b * 100).toFixed(1);

      const mvA = priceAStart !== null
        ? `<span class="${priceANow > priceAStart ? 'mv-up' : 'mv-down'}">${priceAStart}¢ → ${priceANow}¢</span>`
        : `${priceANow}¢`;
      const mvB = priceBStart !== null
        ? `<span class="${priceBNow > priceBStart ? 'mv-up' : 'mv-down'}">${priceBStart}¢ → ${priceBNow}¢</span>`
        : `${priceBNow}¢`;

      const card = document.createElement("div");
      card.className = "card" + strong;
      card.innerHTML = `
        <div class="badge">⏱ через ${m.hours_left}ч</div>
        <div class="teams">${m.team_a}<span class="vs">vs</span>${m.team_b}</div>
        <div class="match-time">${fmtDate(m.match_start)}</div>
        <div class="delta ${cls}">${arrow} ${Math.abs(m.delta_cents)}¢ за 48ч</div>
        <div class="trend-label">
          ↑ растёт: <strong>${m.trending_team}</strong>
          &nbsp;&nbsp;↓ падает: <span style="color:#333">${m.fading_team}</span>
        </div>
        <div class="prices">
          <span>${m.team_a}: ${mvA}</span>
          <span>${m.team_b}: ${mvB}</span>
        </div>
        <canvas id="c${i}" height="80"></canvas>
        <a class="pm-link" href="${m.polymarket_url}" target="_blank" rel="noopener">↗ открыть на Polymarket</a>
      `;
      grid.appendChild(card);

      requestAnimationFrame(() => {
        const ctx = document.getElementById("c" + i);
        if (!ctx) return;
        new Chart(ctx.getContext("2d"), {
          type: "line",
          data: {
            datasets: [{
              data: m.history.map(h => ({ x: h.t, y: +(h.p * 100).toFixed(1) })),
              borderColor:     isUp ? "#22c55e" : "#ef4444",
              backgroundColor: isUp ? "#22c55e10" : "#ef444410",
              fill: true, tension: 0.35, pointRadius: 0, borderWidth: 1.5,
            }]
          },
          options: {
            responsive: true, animation: false,
            plugins: { legend: { display: false }, tooltip: {
              callbacks: { label: c => c.parsed.y.toFixed(1) + "¢" }
            }},
            scales: {
              x: {
                type: "time",
                time: { unit: "hour", displayFormats: { hour: "HH:mm" } },
                ticks: { color: "#252525", maxTicksLimit: 6, font: { size: 10 } },
                grid:  { color: "#111" },
              },
              y: {
                ticks: { color: "#252525", callback: v => v + "¢", font: { size: 10 } },
                grid:  { color: "#111" },
              }
            }
          }
        });
      });
    });

    section.appendChild(grid);
    root.appendChild(section);
  });
}

document.getElementById("ts").textContent = "обновлено: " + new Date().toLocaleTimeString("ru-RU");
setTimeout(() => location.reload(), 5 * 60 * 1000);
renderAll();
</script>
</body>
</html>
"""

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML, data=get_trending_matches())

@app.route("/api/matches")
def api_matches():
    return jsonify(get_trending_matches())

@app.route("/api/matches/<sport>")
def api_matches_sport(sport):
    return jsonify(get_trending_matches(sport))

@app.route("/health")
def health():
    return "ok", 200

@app.route("/cron")
def cron():
    snapshot_markets()
    with get_con() as con:
        total  = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        unique = con.execute("SELECT COUNT(DISTINCT fetched_at) FROM snapshots").fetchone()[0]
    return jsonify({"status": "ok", "total": total, "batches": unique})

@app.route("/debug/snapshots")
def debug_snapshots():
    with get_con() as con:
        total  = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        unique = con.execute("SELECT COUNT(DISTINCT fetched_at) FROM snapshots").fetchone()[0]
        by_sport = con.execute(
            "SELECT sport, COUNT(*) as n FROM snapshots GROUP BY sport"
        ).fetchall()
        latest = con.execute(
            "SELECT sport, question, price_a, price_b, fetched_at FROM snapshots ORDER BY fetched_at DESC LIMIT 10"
        ).fetchall()
        return jsonify({
            "total":     total,
            "batches":   unique,
            "by_sport":  [dict(r) for r in by_sport],
            "latest":    [dict(r) for r in latest],
        })

# ── Startup ───────────────────────────────────────────────────────────────────

def _bootstrap():
    init_db()
    try:
        scheduler = BackgroundScheduler(daemon=True)
        scheduler.add_job(
            snapshot_markets, "interval",
            minutes=POLL_INTERVAL_MIN,
            id="snapshot", replace_existing=True, max_instances=1,
        )
        scheduler.start()
        log.info(f"Scheduler started, interval={POLL_INTERVAL_MIN}min")
    except Exception as e:
        log.error(f"Scheduler error: {e}")
    try:
        snapshot_markets()
    except Exception as e:
        log.error(f"Initial snapshot error: {e}")

_bootstrap()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
