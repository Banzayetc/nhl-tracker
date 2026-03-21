import os
import time
import logging
import sqlite3
from datetime import datetime

import requests
from flask import Flask, render_template_string, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

# ── Config ────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH            = os.environ.get("DB_PATH", "polymarket.db")
POLL_INTERVAL_MIN  = int(os.environ.get("POLL_INTERVAL", "60"))   # minutes
TREND_THRESHOLD    = float(os.environ.get("TREND_THRESHOLD", "0.03"))  # 3 cents
GAMMA_BASE         = "https://gamma-api.polymarket.com"

import json as _json

def _parse_str_or_list(val):
    """outcomePrices/outcomes can be a JSON string or a Python list."""
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return _json.loads(val)
        except Exception:
            return []
    return []

# ── Database ──────────────────────────────────────────────────────────────────

def get_con() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    with get_con() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id   TEXT    NOT NULL,
                question    TEXT,
                team_a      TEXT,
                team_b      TEXT,
                match_start INTEGER,
                price_a     REAL,
                price_b     REAL,
                fetched_at  INTEGER NOT NULL
            )
        """)
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_fetched ON snapshots(market_id, fetched_at)"
        )

# ── Polymarket API ────────────────────────────────────────────────────────────

def fetch_nhl_markets() -> list[dict]:
    """Fetch active NHL game markets (Moneyline) from Gamma API."""
    results = []
    try:
        r = requests.get(
            f"{GAMMA_BASE}/events",
            params={"active": "true", "closed": "false", "tag_slug": "nhl", "limit": 100},
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

            # Prefer Moneyline market, fall back to first binary market
            target = None
            for m in markets:
                q = (m.get("question") or "").lower()
                if "moneyline" in q or q == title.lower():
                    outcomes = _parse_str_or_list(m.get("outcomes", []))
                    prices   = _parse_str_or_list(m.get("outcomePrices", []))
                    if len(outcomes) == 2 and len(prices) == 2:
                        target = m
                        break

            if not target:
                # fallback: first market with exactly 2 outcomes
                for m in markets:
                    outcomes = _parse_str_or_list(m.get("outcomes", []))
                    prices   = _parse_str_or_list(m.get("outcomePrices", []))
                    if len(outcomes) == 2 and len(prices) == 2:
                        target = m
                        break

            if not target:
                continue

            outcomes = _parse_str_or_list(target.get("outcomes", []))
            prices   = _parse_str_or_list(target.get("outcomePrices", []))
            if len(outcomes) != 2 or len(prices) != 2:
                continue

            try:
                tokens = [
                    {"outcome": outcomes[0], "price": float(prices[0])},
                    {"outcome": outcomes[1], "price": float(prices[1])},
                ]
            except (ValueError, TypeError):
                continue

            game_start_str = target.get("gameStartTime") or event.get("startDate")
            if not game_start_str:
                continue

            target["tokens"]        = tokens
            target["gameStartTime"] = game_start_str
            if not target.get("question"):
                target["question"] = title
            results.append(target)

        log.info(f"fetch_nhl_markets: {len(results)} markets from {len(events)} events")
    except Exception as e:
        log.error(f"fetch_nhl_markets: {e}")

    return results


def parse_game_start(m: dict) -> int | None:
    """Return Unix timestamp of game start, or None if unparseable."""
    for field in ("gameStartTime", "startDate", "endDate"):
        val = m.get(field)
        if not val:
            continue
        try:
            # Handle "2026-03-21 21:00:00+00" and ISO 8601 formats
            val = val.strip().replace(" ", "T")
            if val.endswith("+00"):
                val += ":00"
            return int(datetime.fromisoformat(val).timestamp())
        except Exception:
            continue
    return None


def parse_market(m: dict) -> dict | None:
    """Extract normalized fields from a Gamma market dict. Returns None to skip."""
    try:
        tokens = m.get("tokens", [])
        if len(tokens) < 2:
            return None

        match_start = parse_game_start(m)
        if not match_start:
            return None

        price_a = float(tokens[0].get("price") or 0)
        price_b = float(tokens[1].get("price") or 0)

        # Skip markets with no price data yet
        if price_a == 0 and price_b == 0:
            return None

        return {
            "market_id":   str(m.get("id") or m.get("conditionId") or ""),
            "question":    m.get("question", ""),
            "team_a":      tokens[0].get("outcome", "Team A"),
            "team_b":      tokens[1].get("outcome", "Team B"),
            "match_start": match_start,
            "price_a":     price_a,
            "price_b":     price_b,
        }
    except Exception as e:
        log.warning(f"parse_market error: {e}")
        return None

# ── Scheduler job ─────────────────────────────────────────────────────────────

def snapshot_markets():
    """Runs every POLL_INTERVAL_MIN minutes. Saves price snapshots to DB."""
    log.info("Snapshotting NHL markets …")
    markets = fetch_nhl_markets()
    now = int(time.time())
    saved = 0

    with get_con() as con:
        for m in markets:
            parsed = parse_market(m)
            if not parsed:
                continue

            hours_to_start = (parsed["match_start"] - now) / 3600
            # Only track matches within next 72h (captures "day before" window)
            if hours_to_start < 0 or hours_to_start > 72:
                continue

            con.execute(
                """INSERT INTO snapshots
                   (market_id, question, team_a, team_b, match_start, price_a, price_b, fetched_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    parsed["market_id"], parsed["question"],
                    parsed["team_a"],   parsed["team_b"],
                    parsed["match_start"],
                    parsed["price_a"],  parsed["price_b"], now,
                ),
            )
            saved += 1

    log.info(f"Snapshot done: {saved} saved / {len(markets)} fetched")

# ── Trend detection ───────────────────────────────────────────────────────────

def get_trending_matches() -> list[dict]:
    """
    Returns matches where:
      - game starts in 12-48h from now
      - price moved >= TREND_THRESHOLD in any direction over last 24h of snapshots
    """
    now = int(time.time())
    match_min   = now + 12 * 3600   # at least 12h away
    match_max   = now + 48 * 3600   # at most 48h away
    snap_window = now - 24 * 3600   # look back 24h

    with get_con() as con:
        market_ids = [
            r[0]
            for r in con.execute(
                """SELECT DISTINCT market_id FROM snapshots
                   WHERE match_start BETWEEN ? AND ?
                     AND fetched_at >= ?""",
                (match_min, match_max, snap_window),
            ).fetchall()
        ]

        results = []
        for mid in market_ids:
            snaps = con.execute(
                """SELECT price_a, price_b, fetched_at, question, team_a, team_b, match_start
                   FROM snapshots
                   WHERE market_id = ? AND fetched_at >= ?
                   ORDER BY fetched_at ASC""",
                (mid, snap_window),
            ).fetchall()

            if len(snaps) < 2:
                continue

            first, last = snaps[0], snaps[-1]
            # binary market: price_a + price_b ≈ 1
            # delta_a > 0 means team_a is rising, team_b is falling
            delta_a = last["price_a"] - first["price_a"]

            if abs(delta_a) < TREND_THRESHOLD:
                continue

            trending_team = last["team_a"] if delta_a > 0 else last["team_b"]
            fading_team   = last["team_b"] if delta_a > 0 else last["team_a"]
            hours_left    = round((last["match_start"] - now) / 3600, 1)

            results.append(
                {
                    "market_id":      mid,
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
                    "history": [
                        {"t": s["fetched_at"] * 1000, "p": round(s["price_a"], 3)}
                        for s in snaps
                    ],
                }
            )

    return sorted(results, key=lambda x: abs(x["delta_cents"]), reverse=True)

# ── HTML Template ─────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NHL Trend Tracker</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: #0a0a0a;
  color: #ddd;
  font-family: 'Courier New', monospace;
  padding: 28px 20px;
  min-height: 100vh;
}
header { margin-bottom: 28px; }
h1 { color: #00d4ff; font-size: 1.3rem; letter-spacing: 3px; }
.sub { color: #444; font-size: 0.75rem; margin-top: 4px; }
.grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(380px, 1fr));
  gap: 18px;
}
.card {
  background: #111;
  border: 1px solid #1e1e1e;
  border-radius: 10px;
  padding: 20px;
  transition: border-color 0.2s;
}
.card.strong { border-color: #00d4ff33; }
.badge {
  display: inline-block;
  font-size: 0.68rem;
  color: #00d4ff;
  background: #00d4ff0f;
  border: 1px solid #00d4ff22;
  border-radius: 20px;
  padding: 2px 10px;
  margin-bottom: 10px;
}
.teams { font-size: 1rem; font-weight: bold; margin-bottom: 4px; }
.vs { color: #333; margin: 0 6px; }
.match-time { font-size: 0.72rem; color: #555; margin-bottom: 12px; }
.delta { font-size: 1.4rem; font-weight: bold; margin-bottom: 4px; }
.delta.up   { color: #22c55e; }
.delta.down { color: #ef4444; }
.trend-label { font-size: 0.75rem; color: #666; margin-bottom: 14px; }
.trend-label strong { color: #aaa; }
.prices { display: flex; gap: 10px; font-size: 0.75rem; margin-bottom: 16px; }
.prices span {
  background: #1a1a1a;
  border: 1px solid #222;
  padding: 4px 12px;
  border-radius: 6px;
  color: #888;
}
.empty {
  text-align: center;
  padding: 100px 20px;
  color: #333;
  line-height: 2;
}
.empty h2 { font-size: 1rem; color: #444; }
footer { margin-top: 32px; font-size: 0.7rem; color: #2a2a2a; text-align: right; }
.mv-up   { color: #22c55e; font-weight: bold; }
.mv-down { color: #ef4444; font-weight: bold; }
</style>
</head>
<body>
<header>
  <h1>⬆ NHL TREND TRACKER</h1>
  <p class="sub">Polymarket · тренд ≥ 2¢ · окно: последние 24ч · матчи через 12–48ч</p>
</header>

<div id="root"></div>
<footer id="ts"></footer>

<script>
const MATCHES = {{ data | tojson }};

function matchIn(ts) {
  const h = Math.round((ts * 1000 - Date.now()) / 3600000);
  if (h < 1) return "< 1ч до матча";
  return "через ~" + h + "ч";
}

function fmtDate(ts) {
  return new Date(ts * 1000).toLocaleString("ru-RU", {
    day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit"
  });
}

document.getElementById("ts").textContent =
  "последнее обновление: " + new Date().toLocaleTimeString("ru-RU");

// Auto-refresh every 5 minutes
setTimeout(() => location.reload(), 5 * 60 * 1000);

const root = document.getElementById("root");

if (!MATCHES.length) {
  root.innerHTML = `
    <div class="empty">
      <h2>Нет матчей с трендом ≥ 3¢</h2>
      <p>Данные накапливаются — первые результаты появятся<br>через 1–2 часа после запуска</p>
    </div>`;
} else {
  const grid = document.createElement("div");
  grid.className = "grid";

  MATCHES.forEach((m, i) => {
    const isUp  = m.delta_cents > 0;
    const arrow = isUp ? "▲" : "▼";
    const cls   = isUp ? "up" : "down";
    const strong = Math.abs(m.delta_cents) >= 5 ? " strong" : "";

    const priceAStart = m.history.length ? +(m.history[0].p * 100).toFixed(1) : null;
    const priceANow  = +(m.price_a * 100).toFixed(1);
    const priceBStart = priceAStart !== null ? +(100 - priceAStart).toFixed(1) : null;
    const priceBNow  = +(m.price_b * 100).toFixed(1);

    const mvA = priceAStart !== null
      ? `<span class="${priceANow > priceAStart ? 'mv-up' : 'mv-down'}">${priceAStart}¢ → ${priceANow}¢</span>`
      : `${priceANow}¢`;
    const mvB = priceBStart !== null
      ? `<span class="${priceBNow > priceBStart ? 'mv-up' : 'mv-down'}">${priceBStart}¢ → ${priceBNow}¢</span>`
      : `${priceBNow}¢`;

    const card = document.createElement("div");
    card.className = "card" + strong;
    card.innerHTML = `
      <div class="badge">⏱ через ${m.hours_left}ч до матча</div>
      <div class="teams">${m.team_a}<span class="vs">vs</span>${m.team_b}</div>
      <div class="match-time">${fmtDate(m.match_start)}</div>
      <div class="delta ${cls}">${arrow} ${Math.abs(m.delta_cents)}¢ за 24ч</div>
      <div class="trend-label">
        ↑ растёт: <strong>${m.trending_team}</strong>
        &nbsp;&nbsp;↓ падает: <span style="color:#555">${m.fading_team}</span>
      </div>
      <div class="prices">
        <span>${m.team_a}: ${mvA}</span>
        <span>${m.team_b}: ${mvB}</span>
      </div>
      <canvas id="c${i}" height="90"></canvas>
    `;
    grid.appendChild(card);

    requestAnimationFrame(() => {
      const ctx = document.getElementById("c" + i).getContext("2d");
      new Chart(ctx, {
        type: "line",
        data: {
          datasets: [{
            data: m.history.map(h => ({ x: h.t, y: +(h.p * 100).toFixed(1) })),
            borderColor:     isUp ? "#22c55e" : "#ef4444",
            backgroundColor: isUp ? "#22c55e12" : "#ef444412",
            fill: true,
            tension: 0.35,
            pointRadius: 0,
            borderWidth: 1.5,
          }]
        },
        options: {
          responsive: true,
          animation: false,
          plugins: { legend: { display: false }, tooltip: {
            callbacks: { label: ctx => ctx.parsed.y.toFixed(1) + "¢" }
          }},
          scales: {
            x: {
              type: "time",
              time: { unit: "hour", displayFormats: { hour: "HH:mm" } },
              ticks: { color: "#333", maxTicksLimit: 6, font: { size: 10 } },
              grid:  { color: "#151515" },
            },
            y: {
              ticks: { color: "#333", callback: v => v + "¢", font: { size: 10 } },
              grid:  { color: "#151515" },
            }
          }
        }
      });
    });
  });

  root.appendChild(grid);
}
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

@app.route("/health")
def health():
    return "ok", 200

@app.route("/debug/api")
def debug_api():
    """Tries to find individual NHL game matchups."""
    output = {}

    # Try /events with larger limit, look for ones with gameStartTime
    try:
        r = requests.get(
            f"{GAMMA_BASE}/events",
            params={"active": "true", "closed": "false", "tag_slug": "nhl", "limit": 100},
            timeout=20,
        )
        data = r.json()
        events = data if isinstance(data, list) else data.get("events", [])
        with_game_time = [e for e in events if e.get("startDate") and "vs" in (e.get("title") or "").lower()]
        output["events_nhl_total"] = len(events)
        output["events_with_vs_title"] = len(with_game_time)
        output["sample_titles"] = [e.get("title") for e in events[:20]]
    except Exception as e:
        output["events_nhl"] = {"error": str(e)}

    # Try fetching a known game slug directly
    try:
        r = requests.get(
            f"{GAMMA_BASE}/events",
            params={"slug": "nhl-sea-cbj-2026-03-21"},
            timeout=20,
        )
        data = r.json()
        events = data if isinstance(data, list) else data.get("events", [])
        if events:
            e = events[0]
            markets = e.get("markets", [])
            output["slug_test"] = {
                "title":   e.get("title"),
                "start":   e.get("startDate"),
                "markets": len(markets),
                "first_market": {
                    "question":      markets[0].get("question") if markets else None,
                    "outcomes":      markets[0].get("outcomes") if markets else None,
                    "outcomePrices": markets[0].get("outcomePrices") if markets else None,
                    "gameStartTime": markets[0].get("gameStartTime") if markets else None,
                } if markets else None,
            }
        else:
            output["slug_test"] = "no results"
    except Exception as e:
        output["slug_test"] = {"error": str(e)}

    return jsonify(output)

@app.route("/debug/force")
def debug_force():
    """Inline fetch debug — shows exactly where market parsing fails."""
    now = int(time.time())
    try:
        r = requests.get(
            f"{GAMMA_BASE}/events",
            params={"active": "true", "closed": "false", "tag_slug": "nhl", "limit": 100},
            timeout=20,
        )
        data = r.json()
        events = data if isinstance(data, list) else data.get("events", [])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    vs_events = [e for e in events if "vs" in (e.get("title") or "")]
    results = []
    failures = []

    for event in vs_events[:5]:  # inspect first 5 game events in detail
        title   = event.get("title", "")
        markets = event.get("markets", [])
        entry = {"title": title, "markets_count": len(markets), "markets_detail": []}

        for m in markets:
            outcomes = m.get("outcomes", [])
            prices   = m.get("outcomePrices", [])
            gst      = m.get("gameStartTime")
            entry["markets_detail"].append({
                "question":      m.get("question"),
                "outcomes":      outcomes,
                "outcomePrices": prices,
                "gameStartTime": gst,
                "tokens_count":  len(m.get("tokens", [])),
            })
        results.append(entry)

    # Also try saving one market manually
    saved = 0
    for event in vs_events:
        markets = event.get("markets", [])
        for m in markets:
            outcomes = _parse_str_or_list(m.get("outcomes", []))
            prices   = _parse_str_or_list(m.get("outcomePrices", []))
            gst      = m.get("gameStartTime") or event.get("startDate")
            if len(outcomes) != 2 or len(prices) != 2:
                failures.append(f"{event.get('title')}: outcomes={len(outcomes)} prices={len(prices)}")
                continue
            if not gst:
                failures.append(f"{event.get('title')}: no gameStartTime")
                continue
            try:
                tokens = [
                    {"outcome": outcomes[0], "price": float(prices[0])},
                    {"outcome": outcomes[1], "price": float(prices[1])},
                ]
                m["tokens"] = tokens
                m["gameStartTime"] = gst
                parsed = parse_market(m)
                if not parsed:
                    failures.append(f"{event.get('title')}: parse_market returned None")
                    continue
                hours = (parsed["match_start"] - now) / 3600
                status = "✓ IN WINDOW" if 0 <= hours <= 72 else f"OUTSIDE (hours={round(hours,1)})"
                failures.append(f"{event.get('title')}: hours={round(hours,1)} {status}")
                if 0 <= hours <= 72:
                    saved += 1
            except Exception as e:
                failures.append(f"{event.get('title')}: exception {e}")
            break

    return jsonify({
        "total_events":    len(events),
        "vs_events":       len(vs_events),
        "detail":          results,
        "failures_sample": failures[:15],
        "would_save":      saved,
    })
    """Shows how many snapshots are in DB and latest entries."""
    with get_con() as con:
        total = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        latest = con.execute(
            "SELECT market_id, question, price_a, price_b, fetched_at, match_start FROM snapshots ORDER BY fetched_at DESC LIMIT 10"
        ).fetchall()
        return jsonify({
            "total_snapshots": total,
            "latest": [dict(r) for r in latest],
        })

# ── Startup (runs for both `python app.py` and gunicorn) ─────────────────────

def _bootstrap():
    init_db()
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(snapshot_markets, "interval", minutes=POLL_INTERVAL_MIN, id="snapshot")
    scheduler.start()
    log.info("Running initial snapshot …")
    snapshot_markets()

# gunicorn starts with --workers 1, so this runs exactly once per process
_bootstrap()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"Starting dev server on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
