#!/usr/bin/env python3
"""
Esports Bo3 Monitor — CS2 / Dota 2 / Valorant
Алерт при завершении карты 1 + объём Polymarket (жир/тонко/мимо)

БЕЗ внешних зависимостей — только стандартный Python.
Запуск:  python3 esports_monitor.py
Откроется браузер на http://localhost:8765
"""

import json
import re
import threading
import time
import urllib.request
import urllib.parse
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
try:
    from zoneinfo import ZoneInfo
    KYIV = ZoneInfo("Europe/Kyiv")
except Exception:
    KYIV = None

import os
PORT = int(os.environ.get("PORT", 8765))

# Дисциплины bo3.gg
DISCIPLINES = {
    "cs2":      {"id": 1, "label": "CS2",      "pm_prefix": "counter-strike"},
    "valorant": {"id": 2, "label": "Valorant", "pm_prefix": "valorant"},
    "dota2":    {"id": 4, "label": "Dota 2",   "pm_prefix": "dota"},
}

# Пороги объёма (из бэктеста). CS2 выше т.к. база ликвиднее.
VOL_THRESHOLDS = {
    "cs2":      {"fat": 150_000, "thin": 250_000},
    "valorant": {"fat": 50_000,  "thin": 50_000},
    "dota2":    {"fat": 50_000,  "thin": 50_000},
}

# Глобальное состояние (общий доступ из потоков)
STATE = {
    "running": False,
    "games": {"cs2": True, "valorant": True, "dota2": True},
    "interval": 20,
    "min_tier": "",          # "", "a", "s" — фильтр по тиру
    "log": [],
    "live": [],
    "alerts": [],
    "seen": {},              # match_id -> last score
    "alerted": set(),
}
LOCK = threading.Lock()

# ── утилиты ────────────────────────────────────────────────────────────────────

def now_ts():
    if KYIV:
        return datetime.now(KYIV).strftime("%H:%M:%S")
    return datetime.now().strftime("%H:%M:%S")

def push_log(msg, level="info"):
    with LOCK:
        STATE["log"].append({"ts": now_ts(), "msg": msg, "level": level})
        STATE["log"] = STATE["log"][-200:]

def http_get_json(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def parse_slug(slug):
    """Извлекает имена команд из slug матча bo3.gg"""
    s = re.sub(r'-\d{2}-\d{2}-\d{4}$', '', slug)          # дата
    if '-vs-' in s:
        a, b = s.split('-vs-', 1)
        for tail in ('-lol', '-dota2', '-cs2', '-valorant'):
            a = a[:-len(tail)] if a.endswith(tail) else a
            b = b[:-len(tail)] if b.endswith(tail) else b
        return a.replace('-', ' ').title(), b.replace('-', ' ').title()
    return slug, ''

# ── bo3.gg ─────────────────────────────────────────────────────────────────────

def fetch_live(discipline_id):
    base = "https://api.bo3.gg/api/v1/matches"
    params = {
        "filter[matches.discipline_id][eq]": str(discipline_id),
        "filter[matches.status][in]": "current",
        "page[limit]": "100",
        "sort": "tier_rank,-start_date",
    }
    url = base + "?" + urllib.parse.urlencode(params)
    try:
        data = http_get_json(url, timeout=8)
        return data.get("results", [])
    except Exception as e:
        push_log(f"bo3.gg ошибка: {e}", "info")
        return []

def get_score(m):
    lu = m.get("live_updates") or {}
    s1 = (lu.get("team_1") or {}).get("match_score", m.get("team1_score", 0))
    s2 = (lu.get("team_2") or {}).get("match_score", m.get("team2_score", 0))
    return int(s1 or 0), int(s2 or 0)

# ── Polymarket ─────────────────────────────────────────────────────────────────

def pm_volume(t1, t2, prefix):
    """Ищет матч на Polymarket, возвращает (volume, title)"""
    url = "https://gamma-api.polymarket.com/events?tag_slug=esports&limit=200&closed=false"
    try:
        events = http_get_json(url, timeout=8)
    except Exception:
        return None, None

    # нормализуем имена для поиска (первое слово обычно достаточно)
    t1k = t1.lower().split()[0] if t1 else ""
    t2k = t2.lower().split()[0] if t2 else ""

    for ev in events:
        title = ev.get("title", "")
        tl = title.lower()
        if prefix in tl and t1k in tl and t2k in tl and "bo3" in tl:
            vol = ev.get("volume") or 0
            try:
                vol = float(vol)
            except Exception:
                vol = 0.0
            slug = ev.get("slug") or ""
            return vol, title, slug
    return None, None, None

def vol_label(vol, game):
    th = VOL_THRESHOLDS[game]
    if vol is None:
        return "❓ Не найден на Polymarket", "#888888"
    if vol < th["fat"]:
        return f"🟢 ЖИР  ${vol:,.0f}", "#1D9E75"
    if vol < th["thin"]:
        return f"🟡 ТОНКО  ${vol:,.0f}", "#BA7517"
    return f"🔴 МИМО  ${vol:,.0f}", "#A32D2D"

# ── паттерн моментума по игре ──────────────────────────────────────────────────

def momentum_hint(game):
    if game == "cs2":
        return "CS2: ставь ПРОТИВ победителя К1 (фав взял→BUY аутсайдер; апсет→BUY фаворит-камбэк)"
    if game == "dota2":
        return "Dota: ставь ЗА фаворита если он взял К1 (снежный ком). Камбэк убыточен."
    if game == "valorant":
        return "Valorant: ставь ЗА фаворита если он взял К1 (снежный ком). Против победителя −37%."
    return ""

# ── цикл мониторинга ───────────────────────────────────────────────────────────

def monitor_loop():
    push_log("▶ Монитор запущен")
    while True:
        with LOCK:
            if not STATE["running"]:
                break
            games = dict(STATE["games"])
            interval = STATE["interval"]
            min_tier = STATE["min_tier"]

        all_live = []
        for gk, on in games.items():
            if not on:
                continue
            disc = DISCIPLINES[gk]
            for m in fetch_live(disc["id"]):
                if m.get("bo_type") != 3:
                    continue
                tier = (m.get("tier") or "").lower()
                if min_tier == "s" and tier != "s":
                    continue
                if min_tier == "a" and tier not in ("s", "a"):
                    continue

                mid = m["id"]
                t1, t2 = parse_slug(m.get("slug", ""))
                score = get_score(m)
                lu = m.get("live_updates") or {}
                gnum = int(lu.get("game_number") or 0)
                gended = bool(lu.get("game_ended"))

                all_live.append({
                    "game": disc["label"], "t1": t1, "t2": t2,
                    "s1": score[0], "s2": score[1], "gn": gnum,
                })

                with LOCK:
                    prev = STATE["seen"].get(mid)
                    akey = f"{mid}_{gnum}"
                    already = akey in STATE["alerted"]

                if prev is None:
                    with LOCK:
                        STATE["seen"][mid] = score
                    push_log(f"📡 [{disc['label']}] {t1} vs {t2}  [{score[0]}:{score[1]}]", "match")
                    continue

                total = score[0] + score[1]
                if gended and total == 1 and not already:
                    winner = t1 if score[0] > prev[0] else t2
                    loser = t2 if winner == t1 else t1
                    with LOCK:
                        STATE["alerted"].add(akey)

                    push_log(f"🚨 [{disc['label']}] КАРТА 1: {t1} vs {t2} → {winner}", "alert")
                    vol, pm_title, pm_slug = pm_volume(t1, t2, disc["pm_prefix"])
                    vtext, vcolor = vol_label(vol, gk)
                    pm_url = f"https://polymarket.com/event/{pm_slug}" if pm_slug else ""
                    push_log(f"   {vtext}", "alert")

                    with LOCK:
                        STATE["alerts"].append({
                            "id": int(time.time() * 1000),
                            "game": disc["label"],
                            "t1": t1, "t2": t2,
                            "score": f"{score[0]}:{score[1]}",
                            "winner": winner, "loser": loser,
                            "vol_text": vtext, "vol_color": vcolor,
                            "pm_title": pm_title or "",
                            "pm_url": pm_url,
                            "hint": momentum_hint(gk),
                        })
                        STATE["alerts"] = STATE["alerts"][-5:]

                with LOCK:
                    STATE["seen"][mid] = score

        with LOCK:
            STATE["live"] = all_live

        # спим интервал, но реагируем на стоп
        for _ in range(interval):
            with LOCK:
                if not STATE["running"]:
                    break
            time.sleep(1)

    push_log("⏹ Монитор остановлен")

# ── HTTP сервер ────────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/index"):
            self._send(200, "text/html; charset=utf-8", PAGE)
        elif self.path == "/ping":
            self._send(200, "text/plain", "pong")
        elif self.path.startswith("/state"):
            with LOCK:
                payload = {
                    "running": STATE["running"],
                    "live": STATE["live"],
                    "log": STATE["log"][-60:],
                    "alerts": STATE["alerts"],
                }
            self._send(200, "application/json", json.dumps(payload, ensure_ascii=False))
        else:
            self._send(404, "text/plain", "not found")

    def do_POST(self):
        ln = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(ln) if ln else b"{}"
        try:
            data = json.loads(raw)
        except Exception:
            data = {}

        if self.path == "/start":
            with LOCK:
                if not STATE["running"]:
                    STATE["running"] = True
                    STATE["games"] = data.get("games", STATE["games"])
                    STATE["interval"] = int(data.get("interval", 20))
                    STATE["min_tier"] = data.get("min_tier", "")
                    STATE["seen"] = {}
                    STATE["alerted"] = set()
                    STATE["alerts"] = []
                    start = True
                else:
                    start = False
            if start:
                threading.Thread(target=monitor_loop, daemon=True).start()
            self._send(200, "application/json", '{"ok":true}')

        elif self.path == "/stop":
            with LOCK:
                STATE["running"] = False
            self._send(200, "application/json", '{"ok":true}')

        elif self.path == "/clear_alert":
            with LOCK:
                STATE["alerts"] = []
            self._send(200, "application/json", '{"ok":true}')
        else:
            self._send(404, "text/plain", "not found")

    def log_message(self, *a):
        pass

PAGE = r"""<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Esports Bo3 Monitor</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f1117;color:#e0e0e0;min-height:100vh;padding:16px}
h1{font-size:17px;font-weight:600;color:#fff;margin-bottom:16px;display:flex;align-items:center;gap:8px}
.dot{width:9px;height:9px;border-radius:50%;background:#444;transition:.3s}
.dot.on{background:#1D9E75;animation:p 1.5s infinite}
.dot.alert{background:#E24B4A;animation:p .5s infinite}
@keyframes p{0%,100%{opacity:1}50%{opacity:.25}}
.card{background:#1a1d27;border:1px solid #2a2d3a;border-radius:12px;padding:14px;margin-bottom:12px}
.card h2{font-size:11px;text-transform:uppercase;letter-spacing:.07em;color:#555;margin-bottom:10px;font-weight:500}
label{font-size:12px;color:#888;display:block;margin-bottom:4px}
.games{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}
.chip{flex:1;min-width:90px;text-align:center;background:#12151f;border:1px solid #2a2d3a;border-radius:8px;padding:9px;font-size:13px;cursor:pointer;user-select:none;transition:.15s}
.chip.on{background:#11251c;border-color:#1D9E75;color:#7FDDBB}
.row{display:flex;gap:12px;margin-bottom:12px}
.row>div{flex:1}
input,select{width:100%;background:#12151f;border:1px solid #2a2d3a;border-radius:7px;color:#e0e0e0;padding:8px 10px;font-size:13px;outline:none}
input:focus,select:focus{border-color:#378ADD}
.btns{display:flex;gap:8px;margin-bottom:12px}
button{flex:1;padding:11px;border-radius:8px;font-size:13px;font-weight:500;cursor:pointer;border:none}
button:active{opacity:.7}
.start{background:#1D9E75;color:#fff}
.stop{background:#3d1a1a;color:#ff6b6b;border:1px solid #5a2020}
.test{background:#1a1d27;color:#888;border:1px solid #2a2d3a;flex:.5}
.live h2{font-size:11px;text-transform:uppercase;letter-spacing:.07em;color:#555;margin-bottom:8px}
.pill{display:inline-flex;align-items:center;gap:6px;background:#1a1d27;border:1px solid #2a2d3a;border-radius:20px;padding:4px 10px;font-size:12px;color:#888;margin:0 4px 4px 0}
.pill .g{font-size:10px;color:#555;text-transform:uppercase}
.pill .sc{font-weight:600;color:#e0e0e0}
.nolive{font-size:12px;color:#444;font-style:italic}
.logw{background:#1a1d27;border:1px solid #2a2d3a;border-radius:12px;overflow:hidden}
.logh{padding:8px 14px;border-bottom:1px solid #2a2d3a;font-size:11px;color:#444}
.logb{height:170px;overflow-y:auto;padding:4px 0;font-size:12px}
.lr{padding:4px 14px;display:flex;gap:8px;border-left:3px solid transparent}
.lr.info{border-color:#2a2d3a}.lr.match{border-color:#378ADD;background:#0a1520}.lr.alert{border-color:#E24B4A;background:#200a0a}
.lt{color:#444;min-width:44px;flex-shrink:0}
.lm{color:#c0c0c0;line-height:1.5}.lm.am{color:#ff8080;font-weight:500}
.ac{display:none;background:#1a0a0a;border:1px solid #E24B4A44;border-radius:12px;padding:16px;margin-bottom:12px}
.ac.show{display:block}
.ac .bd{font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:#ff6b6b;margin-bottom:8px}
.ac .gm{display:inline-block;font-size:10px;background:#2a2d3a;color:#999;padding:2px 8px;border-radius:5px;margin-left:8px}
.at{font-size:20px;font-weight:600;color:#fff;margin-bottom:4px}
.am2{font-size:12px;color:#555;margin-bottom:12px}
.wb{background:#0a1f16;border:1px solid #1D9E7555;border-radius:8px;padding:10px 14px;margin-bottom:10px}
.wb .wl{font-size:10px;color:#5DCAA5;text-transform:uppercase;letter-spacing:.08em;margin-bottom:3px}
.wb .wn{font-size:17px;font-weight:600;color:#9FE1CB}
.legs{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px}
.leg{background:#12151f;border-radius:8px;padding:9px 12px}
.leg .ll{font-size:10px;color:#444;margin-bottom:3px;text-transform:uppercase;letter-spacing:.06em}
.leg .lv{font-size:13px;font-weight:500;color:#c0c0c0}
.vb{border-radius:8px;padding:10px 14px;margin-bottom:8px}
.vb .vl{font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.08em;margin-bottom:3px}
.vb .vv{font-size:17px;font-weight:600}
.vb .vt{font-size:11px;color:#444;margin-top:4px;word-break:break-word}
.hint{font-size:11px;color:#888;background:#12151f;border-radius:7px;padding:8px 12px;line-height:1.5;margin-bottom:8px}
.dis{width:100%;background:#12151f;color:#555;border:1px solid #2a2d3a;border-radius:7px;padding:7px;font-size:12px;cursor:pointer}
.pmlink{display:block;text-align:center;background:#13212e;color:#5BA3E0;border:1px solid #2a4a63;border-radius:7px;padding:9px;font-size:12px;text-decoration:none;margin-bottom:8px;font-weight:500}
.pmlink:active{opacity:.7}
.achead{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.acx{background:none;border:none;color:#555;font-size:18px;cursor:pointer;padding:0 4px;line-height:1;width:auto;flex:none}
</style></head><body>
<h1><div class="dot" id="dot"></div> Esports Bo3 Monitor</h1>

<div id="alerts"></div>

<div class="live"><h2>Live матчи</h2><div id="live"><span class="nolive">—</span></div></div>

<div class="card">
<h2>Игры</h2>
<div class="games">
<div class="chip on" data-g="cs2" onclick="tg(this)">CS2</div>
<div class="chip on" data-g="valorant" onclick="tg(this)">Valorant</div>
<div class="chip on" data-g="dota2" onclick="tg(this)">Dota 2</div>
</div>
<div class="row">
<div><label>Интервал (сек)</label><input type="number" id="iv" value="20" min="10" max="60"></div>
<div><label>Мин. тир</label><select id="tier"><option value="">Все</option><option value="a">A+ (тир-1/2)</option><option value="s">S (топ)</option></select></div>
</div>
</div>

<div class="btns">
<button class="start" onclick="start()">▶ Запустить</button>
<button class="stop" onclick="stop()">⏹ Стоп</button>
<button class="test" onclick="test()">🔔</button>
</div>

<div class="logw"><div class="logh">Лог</div><div class="logb" id="log"></div></div>

<script>
let lastAlertId=0, lastLogLen=0, running=false;

function beep(){try{const c=new(window.AudioContext||window.webkitAudioContext)();[0,.18,.36].forEach(t=>{const o=c.createOscillator(),g=c.createGain();o.connect(g);g.connect(c.destination);o.frequency.value=880;o.type='sine';g.gain.setValueAtTime(.5,c.currentTime+t);g.gain.exponentialRampToValueAtTime(.001,c.currentTime+t+.15);o.start(c.currentTime+t);o.stop(c.currentTime+t+.16)})}catch(e){}}

function dot(s){document.getElementById('dot').className='dot'+(s?' '+s:'')}

function tg(el){el.classList.toggle('on')}

function getGames(){const g={};document.querySelectorAll('.chip').forEach(c=>g[c.dataset.g]=c.classList.contains('on'));return g}

function renderLog(log){
  const b=document.getElementById('log');
  b.innerHTML=log.map(e=>`<div class="lr ${e.level}"><span class="lt">${e.ts}</span><span class="lm ${e.level==='alert'?'am':''}">${e.msg}</span></div>`).join('');
  b.scrollTop=b.scrollHeight;
}

function renderLive(live){
  const el=document.getElementById('live');
  if(!live.length){el.innerHTML='<span class="nolive">Нет активных матчей</span>';return}
  el.innerHTML=live.map(m=>`<span class="pill"><span class="g">${m.game}</span> ${m.t1} vs ${m.t2} <span class="sc">${m.s1}:${m.s2}</span> К${m.gn}</span>`).join('');
}

const MAX_ALERTS=5;
let shownIds=[];

function escapeHtml(s){return (s||'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}

function buildCard(a){
  const pmLink = a.pm_url
    ? `<a class="pmlink" href="${a.pm_url}" target="_blank">↗ Открыть на Polymarket</a>`
    : '';
  return `<div class="ac show" data-id="${a.id}">
    <div class="achead">
      <div class="bd">🚨 Карта 1 завершена<span class="gm">${escapeHtml(a.game)}</span></div>
      <button class="acx" onclick="closeCard(${a.id})">✕</button>
    </div>
    <div class="at">${escapeHtml(a.t1)} vs ${escapeHtml(a.t2)}</div>
    <div class="am2">Счёт: ${a.score}</div>
    <div class="wb"><div class="wl">Взял К1</div><div class="wn">✅ ${escapeHtml(a.winner)}</div></div>
    <div class="legs">
      <div class="leg"><div class="ll">Нога 1 — BUY серию</div><div class="lv">${escapeHtml(a.winner)}</div></div>
      <div class="leg"><div class="ll">Нога 2 — BUY карту 2</div><div class="lv">${escapeHtml(a.loser)}</div></div>
    </div>
    <div class="vb" style="border:1px solid ${a.vol_color}44;background:${a.vol_color}11">
      <div class="vl">Объём Polymarket</div>
      <div class="vv" style="color:${a.vol_color}">${escapeHtml(a.vol_text)}</div>
      <div class="vt">${escapeHtml(a.pm_title)||'Рынок не найден'}</div>
    </div>
    ${pmLink}
    <div class="hint">${escapeHtml(a.hint)}</div>
  </div>`;
}

function showAlert(a){
  if(!a||shownIds.includes(a.id))return;
  shownIds.push(a.id);
  if(shownIds.length>MAX_ALERTS)shownIds=shownIds.slice(-MAX_ALERTS);
  beep();dot('alert');

  const box=document.getElementById('alerts');
  box.insertAdjacentHTML('afterbegin', buildCard(a));
  // обрезаем до MAX_ALERTS карточек
  while(box.children.length>MAX_ALERTS) box.removeChild(box.lastChild);

  if(Notification.permission==='granted')new Notification(a.game+' — Карта 1!',{body:a.winner+' взял К1. '+a.vol_text});
  let bl=true;const t=setInterval(()=>{document.title=bl?'🚨 К1 ЗАВЕРШЕНА':'Esports Monitor';bl=!bl;if(document.getElementById('alerts').children.length===0)clearInterval(t)},800);
}

function closeCard(id){
  const box=document.getElementById('alerts');
  [...box.children].forEach(c=>{if(c.dataset.id==id)box.removeChild(c)});
  if(box.children.length===0){dot(running?'on':'');document.title='Esports Monitor'}
}

async function poll(){
  try{
    const r=await fetch('/state');const s=await r.json();
    running=s.running;const hasAlerts=document.getElementById('alerts').children.length>0;dot(running?(hasAlerts?'alert':'on'):(hasAlerts?'alert':''));
    renderLog(s.log);renderLive(s.live);
    if(s.alerts)s.alerts.forEach(showAlert);
  }catch(e){}
}

function start(){
  if(Notification.permission==='default')Notification.requestPermission();
  fetch('/start',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({games:getGames(),interval:parseInt(document.getElementById('iv').value)||20,min_tier:document.getElementById('tier').value})});
}
function stop(){fetch('/stop',{method:'POST'})}
function test(){showAlert({id:Date.now(),game:'CS2',t1:'Team Spirit',t2:'NAVI',score:'1:0',winner:'Team Spirit',loser:'NAVI',vol_text:'🟢 ЖИР  $87,000',vol_color:'#1D9E75',pm_title:'Counter-Strike: Spirit vs NAVI (BO3) - IEM Cologne',pm_url:'https://polymarket.com/event/cs2-aaa-inf1-2026-03-10',hint:'CS2: ставь ПРОТИВ победителя К1 (фав взял→BUY аутсайдер)'})}

setInterval(poll,2000);poll();
</script></body></html>"""

def main():
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    url = f"http://0.0.0.0:{PORT}"
    print(f"\n  Esports Bo3 Monitor — CS2 / Dota 2 / Valorant")
    print(f"  Браузер: {url}")
    print(f"  Ctrl+C для выхода\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nОстановлено.")

if __name__ == "__main__":
    main()
