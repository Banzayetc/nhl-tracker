import os
import time
import logging
import sqlite3
from datetime import datetime

import requests
from flask import Flask, render_template_string, jsonify, request
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
    {"key": "epl",          "tag": "epl",          "name": "EPL",       "emoji": "⚽", "url_path": "epl"},
    {"key": "champions-league", "tag": "champions-league", "name": "UCL",  "emoji": "🏆", "url_path": "champions-league"},
    {"key": "la-liga",      "tag": "la-liga",      "name": "La Liga",   "emoji": "⚽", "url_path": "la-liga"},
    {"key": "bundesliga",   "tag": "bundesliga",   "name": "Bundesliga","emoji": "⚽", "url_path": "bundesliga"},
    {"key": "serie-a",      "tag": "serie-a",      "name": "Serie A",   "emoji": "⚽", "url_path": "serie-a"},
    {"key": "ligue-1",      "tag": "ligue-1",      "name": "Ligue 1",   "emoji": "⚽", "url_path": "ligue-1"},
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
  { key: "all",              name: "Все",        emoji: "📊" },
  { key: "nhl",              name: "NHL",        emoji: "🏒" },
  { key: "nba",              name: "NBA",        emoji: "🏀" },
  { key: "baseball",         name: "Baseball",   emoji: "⚾" },
  { key: "epl",              name: "EPL",        emoji: "⚽" },
  { key: "champions-league", name: "UCL",        emoji: "🏆" },
  { key: "la-liga",          name: "La Liga",    emoji: "⚽" },
  { key: "bundesliga",       name: "Bundesliga", emoji: "⚽" },
  { key: "serie-a",          name: "Serie A",    emoji: "⚽" },
  { key: "ligue-1",          name: "Ligue 1",    emoji: "⚽" },
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

@app.route("/history")
def history():
    """
    Analyse past matches for a team using our own snapshots.
    Usage: /history?team=Rangers&n=15&sport=nhl
    """
    team  = request.args.get("team", "").strip()
    n     = min(int(request.args.get("n", 15)), 30)
    sport = request.args.get("sport", "")

    if not team:
        return jsonify({"error": "team parameter required. Example: /history?team=Rangers"}), 400

    now = int(time.time())

    with get_con() as con:
        # Find past matches with this team in question/team_a/team_b
        if sport:
            rows = con.execute(
                """SELECT DISTINCT market_id, question, team_a, team_b, match_start, sport, event_slug
                   FROM snapshots
                   WHERE match_start < ?
                     AND sport = ?
                     AND (LOWER(team_a) LIKE ? OR LOWER(team_b) LIKE ? OR LOWER(question) LIKE ?)
                   ORDER BY match_start DESC
                   LIMIT ?""",
                (now, sport, f"%{team.lower()}%", f"%{team.lower()}%", f"%{team.lower()}%", n),
            ).fetchall()
        else:
            rows = con.execute(
                """SELECT DISTINCT market_id, question, team_a, team_b, match_start, sport, event_slug
                   FROM snapshots
                   WHERE match_start < ?
                     AND (LOWER(team_a) LIKE ? OR LOWER(team_b) LIKE ? OR LOWER(question) LIKE ?)
                   ORDER BY match_start DESC
                   LIMIT ?""",
                (now, f"%{team.lower()}%", f"%{team.lower()}%", f"%{team.lower()}%", n),
            ).fetchall()

        if not rows:
            # Show what teams we have in DB for reference
            sample = con.execute(
                "SELECT DISTINCT team_a FROM snapshots LIMIT 20"
            ).fetchall()
            return jsonify({
                "team": team,
                "matches_found": 0,
                "message": "No past matches found in our database for this team. DB is still accumulating data.",
                "sample_teams": [r[0] for r in sample],
            })

        results = []
        trend_total = 0
        trend_held  = 0

        for row in rows:
            mid         = row["market_id"]
            match_start = row["match_start"]
            snap_start  = match_start - 48 * 3600
            snap_end    = match_start - 12 * 3600

            # Get snapshots in the 12-48h window before match
            snaps = con.execute(
                """SELECT price_a, price_b, fetched_at
                   FROM snapshots
                   WHERE market_id = ?
                     AND fetched_at BETWEEN ? AND ?
                   ORDER BY fetched_at ASC""",
                (mid, snap_start, snap_end),
            ).fetchall()

            match_info = {
                "title":       row["question"],
                "team_a":      row["team_a"],
                "team_b":      row["team_b"],
                "start":       datetime.fromtimestamp(match_start).strftime("%Y-%m-%d %H:%M"),
                "sport":       row["sport"],
                "snaps_count": len(snaps),
            }

            if len(snaps) < 3:
                match_info["trend"] = None
                match_info["reason"] = f"not enough snapshots ({len(snaps)}, need 3+)"
                results.append(match_info)
                continue

            prices_a = [s["price_a"] for s in snaps]
            first_p  = prices_a[0]
            last_p   = prices_a[-1]
            delta    = last_p - first_p

            if abs(delta) < TREND_MIN_DELTA:
                match_info["trend"]      = None
                match_info["reason"]     = f"no trend (delta={round(delta*100,1)}¢)"
                match_info["delta_cents"] = round(delta * 100, 1)
                results.append(match_info)
                continue

            direction = 1 if delta > 0 else -1
            monotone  = is_monotone(prices_a, direction, MAX_PULLBACK)

            if not monotone:
                match_info["trend"]  = None
                match_info["reason"] = f"not monotone (delta={round(delta*100,1)}¢ but reversed > {MAX_PULLBACK*100}¢)"
                match_info["delta_cents"] = round(delta * 100, 1)
                results.append(match_info)
                continue

            # Trend found — check if it held into match start (last snapshot before start)
            final_snaps = con.execute(
                """SELECT price_a FROM snapshots
                   WHERE market_id = ? AND fetched_at BETWEEN ? AND ?
                   ORDER BY fetched_at DESC LIMIT 1""",
                (mid, match_start - 13 * 3600, match_start),
            ).fetchall()

            held = False
            final_price = None
            if final_snaps:
                final_price = final_snaps[0]["price_a"]
                held = (direction == 1 and final_price > first_p + TREND_MIN_DELTA / 2) or \
                       (direction == -1 and final_price < first_p - TREND_MIN_DELTA / 2)

            trend_total += 1
            if held:
                trend_held += 1

            trending_team = row["team_a"] if direction == 1 else row["team_b"]

            match_info["trend"]         = f"{'▲' if direction==1 else '▼'} {round(abs(delta)*100,1)}¢"
            match_info["trending_team"] = trending_team
            match_info["delta_cents"]   = round(delta * 100, 1)
            match_info["final_price"]   = round(final_price, 3) if final_price else None
            match_info["held"]          = held
            results.append(match_info)

    pct = round(trend_held / trend_total * 100) if trend_total > 0 else None

    return jsonify({
        "team":               team,
        "matches_checked":    len(rows),
        "matches_with_trend": trend_total,
        "trend_held_count":   trend_held,
        "trend_held_pct":     pct,
        "summary":            f"{trend_held}/{trend_total} трендов сохранились до матча ({pct}%)" if pct is not None else "Нет матчей с трендом в нашей базе",
        "note":               "Анализ на основе наших снапшотов. Чем дольше работает трекер — тем больше данных.",
        "matches":            results,
    })
    team  = request.args.get("team", "").strip()
    n     = min(int(request.args.get("n", 15)), 30)
    sport = request.args.get("sport", "")

    if not team:
        return jsonify({"error": "team parameter required. Example: /history?team=Rangers"}), 400

    tag_list = [sport] if sport else [s["tag"] for s in SPORTS]
    candidate_events = []

    for tag in tag_list:
        try:
            r = requests.get(
                f"{GAMMA_BASE}/events",
                params={"active": "false", "closed": "true", "tag_slug": tag, "limit": 100},
                timeout=20,
            )
            data = r.json()
            events = data if isinstance(data, list) else data.get("events", [])
            for e in events:
                title = e.get("title", "")
                if team.lower() in title.lower() and "vs" in title.lower():
                    e["_sport"] = tag
                    candidate_events.append(e)
        except Exception as ex:
            log.warning(f"history fetch error tag={tag}: {ex}")

    candidate_events.sort(key=lambda e: e.get("startDate") or "", reverse=True)
    candidate_events = candidate_events[:n]

    if not candidate_events:
        return jsonify({"team": team, "matches_found": 0,
                        "message": "No closed matches found for this team"})

    results = []
    trend_total = 0
    trend_held  = 0

    for event in candidate_events:
        title   = event.get("title", "")
        markets = event.get("markets", [])

        # Find moneyline market
        target = None
        for m in markets:
            q        = (m.get("question") or "").lower()
            outcomes = _parse_list(m.get("outcomes", []))
            prices   = _parse_list(m.get("outcomePrices", []))
            if len(outcomes) == 2 and len(prices) == 2:
                if "moneyline" in q or q == title.lower():
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

        # Closed markets have: oneWeekPriceChange, oneMonthPriceChange (no oneDayPriceChange)
        week_change  = target.get("oneWeekPriceChange")
        month_change = target.get("oneMonthPriceChange")
        volume       = float(target.get("volumeNum") or target.get("volume") or 0)

        if week_change is None:
            results.append({
                "title":  title,
                "start":  event.get("startDate"),
                "trend":  None,
                "reason": "no price change data available",
                "volume": round(volume),
            })
            continue

        week_change  = float(week_change)
        month_change = float(month_change) if month_change is not None else None

        # Trend exists if weekly change >= threshold
        if abs(week_change) < TREND_MIN_DELTA:
            results.append({
                "title":      title,
                "start":      event.get("startDate"),
                "trend":      None,
                "reason":     f"no trend (week change={round(week_change*100,1)}¢)",
                "volume":     round(volume),
                "week_change": round(week_change * 100, 1),
            })
            continue

        direction = 1 if week_change > 0 else -1
        outcomes  = _parse_list(target.get("outcomes", []))
        trending_team = outcomes[0] if direction == 1 else outcomes[-1] if len(outcomes) > 1 else "?"

        # "Held" = month trend same direction as week trend
        held = False
        if month_change is not None:
            held = (direction == 1 and month_change > 0) or (direction == -1 and month_change < 0)

        trend_total += 1
        if held:
            trend_held += 1

        results.append({
            "title":         title,
            "start":         event.get("startDate"),
            "sport":         event.get("_sport"),
            "trend":         f"{'▲' if direction == 1 else '▼'} {round(abs(week_change)*100,1)}¢",
            "trending_team": trending_team,
            "week_change":   round(week_change * 100, 1),
            "month_change":  round(month_change * 100, 1) if month_change is not None else None,
            "held":          held,
            "volume_usd":    round(volume),
        })

    pct = round(trend_held / trend_total * 100) if trend_total > 0 else None

    return jsonify({
        "team":               team,
        "matches_checked":    len(candidate_events),
        "matches_with_trend": trend_total,
        "trend_held_count":   trend_held,
        "trend_held_pct":     pct,
        "summary":            f"{trend_held}/{trend_total} трендов сохранились ({pct}%)" if pct is not None else "Нет матчей с трендом",
        "matches":            results,
    })

@app.route("/debug/history")
def debug_history():
    """Debug: show raw closed events and price history for a team."""
    team  = request.args.get("team", "Rangers")
    sport = request.args.get("sport", "nhl")

    try:
        r = requests.get(
            f"{GAMMA_BASE}/events",
            params={"active": "false", "closed": "true", "tag_slug": sport, "limit": 50},
            timeout=20,
        )
        data = r.json()
        events = data if isinstance(data, list) else data.get("events", [])
        team_events = [e for e in events if team.lower() in (e.get("title") or "").lower()]
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not team_events:
        return jsonify({"found": 0, "all_titles": [e.get("title") for e in events[:20]]})

    event = team_events[0]
    markets = event.get("markets", [])
    market_id = None
    for m in markets:
        outcomes = _parse_list(m.get("outcomes", []))
        if len(outcomes) == 2:
            market_id = m.get("id") or m.get("conditionId")
            break

    hist_results = {}
    for ep in ["prices-history", "timeseries", "history", "markets"]:
        try:
            h = requests.get(
                f"{GAMMA_BASE}/{ep}",
                params={"market": market_id, "interval": "1h", "fidelity": 60},
                timeout=10,
            )
            body = h.json()
            hist_results[ep] = {
                "status": h.status_code,
                "type": type(body).__name__,
                "len": len(body) if isinstance(body, list) else len(body.get("history", body.get("prices", []))),
                "sample": body[:2] if isinstance(body, list) else body,
            }
        except Exception as ex:
            hist_results[ep] = {"error": str(ex)}

    return jsonify({
        "team":        team,
        "found":       len(team_events),
        "market_id":   market_id,
        "event_title": event.get("title"),
        "event_start": event.get("startDate"),
        "market_keys": list(markets[0].keys()) if markets else [],
        "endpoints":   hist_results,
    })

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
