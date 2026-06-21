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
from datetime import datetime, timezone
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
    "cs2":      {"id": 1, "label": "CS2",      "pm_tag": "counter-strike-2"},
    "valorant": {"id": 2, "label": "Valorant", "pm_tag": "valorant"},
    "dota2":    {"id": 4, "label": "Dota 2",   "pm_tag": "dota-2"},
}

# Пороги объёма (из бэктеста). CS2 выше т.к. база ликвиднее.
# CS2: killzone <$40K (обратный паттерн), жир $40-125K, рабочий до $250K
# Dota2: жир <$25K, рабочий до $50K
VOL_THRESHOLDS = {
    "cs2":      {"kill": 40_000, "fat": 125_000, "thin": 250_000},
    "valorant": {"kill": 0,      "fat": 50_000,  "thin": 50_000},
    "dota2":    {"kill": 0,      "fat": 25_000,  "thin": 50_000},
}

# Глобальное состояние (общий доступ из потоков)
STATE = {
    "running": False,
    "games": {"cs2": True, "valorant": False, "dota2": False},
    "interval": 20,
    "min_tier": "",          # "", "a", "s" — фильтр по тиру
    "log": [],
    "live": [],
    "alerts": [],
    "seen": {},              # match_id -> last score
    "alerted": set(),
    "vol_cache": {},         # match_id -> (vol, title, slug, game_key)
    "upcoming": [],          # предстоящие матчи на 21ч вперёд
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

def fetch_upcoming(discipline_id, hours=21):
    """Предстоящие Bo3 матчи на следующие N часов"""
    base = "https://api.bo3.gg/api/v1/matches"
    params = {
        "filter[matches.discipline_id][eq]": str(discipline_id),
        "filter[matches.status][in]": "upcoming",
        "page[limit]": "100",
        "sort": "start_date",
    }
    url = base + "?" + urllib.parse.urlencode(params)
    try:
        data = http_get_json(url, timeout=8)
        results = data.get("results", [])
        now = datetime.now(timezone.utc)
        cutoff = now.timestamp() + hours * 3600
        filtered = []
        for m in results:
            sd = m.get("start_date", "")
            if not sd:
                continue
            try:
                dt = datetime.fromisoformat(sd.replace("Z", "+00:00"))
                if dt.timestamp() <= cutoff:
                    filtered.append(m)
            except Exception:
                pass
        return filtered
    except Exception as e:
        push_log(f"bo3.gg upcoming ошибка: {e}", "info")
        return []

def get_score(m):
    lu = m.get("live_updates") or {}
    s1 = (lu.get("team_1") or {}).get("match_score", m.get("team1_score", 0))
    s2 = (lu.get("team_2") or {}).get("match_score", m.get("team2_score", 0))
    return int(s1 or 0), int(s2 or 0)

# ── Polymarket ─────────────────────────────────────────────────────────────────

def _norm_tokens(name):
    """Значимые токены имени команды (для мягкого сравнения)"""
    name = (name or "").lower()
    name = re.sub(r'[^a-z0-9 ]', ' ', name)
    stop = {'esports', 'team', 'gaming', 'club', 'the', 'academy'}
    return [t for t in name.split() if len(t) >= 2 and t not in stop]

def _name_match(bo3_name, pm_name):
    """Мягкое совпадение имён команд (Yakutou~Yakult, The Mongolz~TheMongolz)"""
    a = _norm_tokens(bo3_name)
    b = _norm_tokens(pm_name)
    if not a or not b:
        return False
    # также склеенные варианты целиком (themongolz)
    a_join = "".join(a)
    b_join = "".join(b)
    if len(a_join) >= 4 and len(b_join) >= 4:
        if a_join in b_join or b_join in a_join:
            return True
    for ta in a:
        for tb in b:
            n = min(len(ta), len(tb))
            if n >= 4 and ta[:n] == tb[:n]:
                return True
            # вхождение подстроки (mongolz в themongolz)
            if len(ta) >= 4 and ta in tb:
                return True
            if len(tb) >= 4 and tb in ta:
                return True
            if n < 4 and ta == tb:
                return True
    return False

def pm_volume(t1, t2, tag):
    """Ищет матч на Polymarket по тегу игры, мягко матчит имена обеих команд.
    Возвращает (volume, title, slug)."""
    url = f"https://gamma-api.polymarket.com/events?tag_slug={tag}&closed=false&limit=100&order=startDate&ascending=false"
    try:
        events = http_get_json(url, timeout=8)
    except Exception:
        return None, None, None

    for ev in events:
        title = ev.get("title", "")
        if "bo3" not in title.lower():
            continue
        # имена команд из заголовка "Game: A vs B (BO3) - ..."
        m = re.search(r":\s*(.+?)\s+vs\s+(.+?)\s*\(", title, re.IGNORECASE)
        if m:
            pm_t1, pm_t2 = m.group(1), m.group(2)
        else:
            pm_t1, pm_t2 = title, title
        # обе команды должны совпасть (в любом порядке)
        ok = (_name_match(t1, pm_t1) and _name_match(t2, pm_t2)) or \
             (_name_match(t1, pm_t2) and _name_match(t2, pm_t1))
        if ok:
            vol = ev.get("volume") or 0
            try:
                vol = float(vol)
            except Exception:
                vol = 0.0
            return vol, title, ev.get("slug") or ""
    return None, None, None

def vol_label(vol, game):
    th = VOL_THRESHOLDS[game]
    if vol is None:
        return "❓ Не найден на Polymarket", "#888888"
    # CS2: зона killzone <$55K — обратный паттерн, не торговать
    if game == "cs2" and vol < th["kill"]:
        return f"⛔ КІЛЗОН  ${vol:,.0f}", "#666666"
    if vol < th["fat"]:
        return f"🟢 ЖИР  ${vol:,.0f}", "#1D9E75"
    # CS2: рабочая зона $125-250K
    if game == "cs2" and vol < th["thin"]:
        return f"🟡 ТОНКО  ${vol:,.0f}", "#BA7517"
    if game != "cs2" and vol < th["thin"]:
        return f"🟡 ТОНКО  ${vol:,.0f}", "#BA7517"
    return f"🔴 МИМО  ${vol:,.0f}", "#A32D2D"

def vol_verdict(vol, game):
    """Короткая пометка для списка: моментум ЖИР/ТОНКО/МИМО + цвет"""
    th = VOL_THRESHOLDS[game]
    if vol is None:
        return "?", "#888888"
    # CS2: killzone — показываем но серым (не торговать)
    if game == "cs2" and vol < th["kill"]:
        return f"⛔ ${vol/1000:.0f}K", "#555555"
    if vol < th["fat"]:
        return f"ЖИР ${vol/1000:.0f}K", "#1D9E75"
    if vol < th["thin"]:
        return f"ТОНКО ${vol/1000:.0f}K", "#BA7517"
    return f"МИМО ${vol/1000:.0f}K", "#A32D2D"

# ── паттерн моментума по игре ──────────────────────────────────────────────────

def momentum_hint(game):
    if game == "cs2":
        return "CS2: ставь ПРОТИВ победителя К1 (фав взял→BUY аутсайдер; апсет→BUY фаворит-камбэк)"
    if game == "dota2":
        return "Dota: ставь ЗА фаворита если он взял К1 (снежный ком). Камбэк убыточен."
    if game == "valorant":
        return "Valorant: ставь ЗА фаворита если он взял К1 (снежный ком). Против победителя −37%."
    return ""

def roi_hint_alert(game, fav_won_k1, map_score_t1, map_score_t2, vol):
    """ROI подсказка для алерт-карточки с учётом кто взял К1 и счёта карты"""
    # Базовый ROI по типу К1
    if game == "cs2":
        if fav_won_k1:
            base_roi = "+57%"
            k1_label = "Фав взял К1 → BUY аутсайдер"
        else:
            base_roi = "+23%"
            k1_label = "Апсет → BUY фаворит-камбэк"
    elif game == "dota2":
        if fav_won_k1:
            base_roi = "+28%"
            k1_label = "Фав взял К1 → BUY фаворит (снежный ком)"
        else:
            base_roi = "−"
            k1_label = "Апсет → пропуск (камбэк убыточен)"
    elif game == "valorant":
        if fav_won_k1:
            base_roi = "+26%"
            k1_label = "Фав взял К1 → BUY фаворит (снежный ком)"
        else:
            base_roi = "−"
            k1_label = "Апсет → пропуск"
    else:
        return "", ""

    # Корректировка по счёту карты 1
    score_hint = ""
    if map_score_t1 is not None and map_score_t2 is not None:
        margin = abs(int(map_score_t1) - int(map_score_t2))
        s1, s2 = int(map_score_t1), int(map_score_t2)
        if margin <= 4:
            score_label = f"Тесно ({s1}:{s2}, разрыв {margin})"
            if game == "cs2":
                score_hint = "→ ROI выше среднего (~+47%)"
            else:
                score_hint = "→ слабый снежный ком"
        elif margin <= 7:
            score_label = f"Средне ({s1}:{s2}, разрыв {margin})"
            score_hint = "→ ROI стандартный"
        else:
            score_label = f"Разгром ({s1}:{s2}, разрыв {margin})"
            if game == "cs2":
                score_hint = "→ ROI ниже среднего (~+27%)"
            else:
                score_hint = "→ сильный снежный ком (~+33%)"
    else:
        score_label = ""

    return k1_label, base_roi, score_label, score_hint

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

                # В список показываем только матчи где вход ещё возможен:
                # 0:0 (ждём К1) и 1:0/0:1 (момент входа). 1:1 и решённые серии скрываем.
                stotal = score[0] + score[1]
                if stotal <= 1:
                    window = (stotal == 1)   # 1:0 → окно дельта-нейтрали открыто

                    # объём с кешем (тянем один раз на матч)
                    with LOCK:
                        cached = STATE["vol_cache"].get(mid)
                    if cached is None:
                        cvol, ctitle, cslug = pm_volume(t1, t2, disc["pm_tag"])
                        with LOCK:
                            STATE["vol_cache"][mid] = (cvol, ctitle, cslug)
                    else:
                        cvol, ctitle, cslug = cached

                    vtext, vcolor = vol_verdict(cvol, gk)
                    purl = f"https://polymarket.com/event/{cslug}" if cslug else ""

                    # ROI hint по объёму
                    th = VOL_THRESHOLDS[gk]
                    if cvol is None:
                        roi_hint = ""
                    elif gk == "cs2" and cvol < th["kill"]:
                        roi_hint = "⛔ killzone"
                    elif cvol < th["fat"]:
                        roi_hint = "ROI ~+40%"
                    elif cvol < th["thin"]:
                        roi_hint = "ROI ~+25%"
                    else:
                        roi_hint = "нет edge"

                    # Время старта по Киеву
                    sd = m.get("start_date", "")
                    try:
                        dt = datetime.fromisoformat(sd.replace("Z", "+00:00"))
                        start_kyiv = dt.astimezone(KYIV).strftime("%H:%M") if KYIV else dt.strftime("%H:%M")
                    except Exception:
                        start_kyiv = ""

                    all_live.append({
                        "game": disc["label"], "t1": t1, "t2": t2,
                        "s1": score[0], "s2": score[1], "gn": gnum,
                        "window": window,
                        "vol_text": vtext, "vol_color": vcolor,
                        "pm_url": purl,
                        "roi_hint": roi_hint,
                        "start_kyiv": start_kyiv,
                    })

                total = score[0] + score[1]

                with LOCK:
                    prev = STATE["seen"].get(mid)
                    akey = f"{mid}_k1"   # один алерт на матч (факт К1), не зависит от game_number
                    already = akey in STATE["alerted"]

                if prev is None:
                    # первый раз видим матч
                    with LOCK:
                        STATE["seen"][mid] = score
                    push_log(f"📡 [{disc['label']}] {t1} vs {t2}  [{score[0]}:{score[1]}]", "match")
                    # если матч впервые увиден уже на 1:0 — это тоже окно входа, алертим
                    if total != 1:
                        continue
                    # иначе проваливаемся в блок алерта ниже

                prev_total = (prev[0] + prev[1]) if prev else -1

                # Алерт когда серия в состоянии 1:0/0:1 (К1 сыграна, перерыв),
                # и мы по этому матчу ещё не алертили. Не зависит от game_ended.
                if total == 1 and not already:
                    base = prev if prev is not None else (0, 0)
                    winner = t1 if score[0] > base[0] else (t2 if score[1] > base[1] else (t1 if score[0] == 1 else t2))
                    loser = t2 if winner == t1 else t1
                    with LOCK:
                        STATE["alerted"].add(akey)

                    push_log(f"🚨 [{disc['label']}] КАРТА 1: {t1} vs {t2} → {winner}", "alert")
                    vol, pm_title, pm_slug = pm_volume(t1, t2, disc["pm_tag"])
                    vtext, vcolor = vol_label(vol, gk)
                    pm_url = f"https://polymarket.com/event/{pm_slug}" if pm_slug else ""
                    push_log(f"   {vtext}", "alert")

                    # Счёт карты 1 из bo3.gg
                    ms1 = m.get("team1_last_game_score")
                    ms2 = m.get("team2_last_game_score")

                    # Кто победитель — фаворит или аутсайдер?
                    # Определяем через bet_updates: выше aggrement_score = фаворит
                    bu = m.get("bet_updates") or {}
                    t1_agree = (bu.get("team_1") or {}).get("aggrement_score") or 0
                    t2_agree = (bu.get("team_2") or {}).get("aggrement_score") or 0
                    if t1_agree > t2_agree:
                        fav_name = t1
                    elif t2_agree > t1_agree:
                        fav_name = t2
                    else:
                        fav_name = None
                    fav_won_k1 = (fav_name == winner) if fav_name else None

                    k1_label, base_roi, score_label, score_hint = roi_hint_alert(
                        gk, fav_won_k1, ms1, ms2, vol
                    )

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
                            "finished_at": now_ts(),
                            "k1_label": k1_label,
                            "base_roi": base_roi,
                            "score_label": score_label,
                            "score_hint": score_hint,
                            "map_score": f"{ms1}:{ms2}" if ms1 is not None else "",
                        })
                        STATE["alerts"] = STATE["alerts"][-5:]

                with LOCK:
                    STATE["seen"][mid] = score

        with LOCK:
            STATE["live"] = all_live
            # ограничиваем размер кеша объёмов (защита от роста)
            if len(STATE["vol_cache"]) > 300:
                STATE["vol_cache"] = {}

        # Upcoming матчи — обновляем каждые 3 минуты
        try:
            now_ts_int = int(time.time())
            with LOCK:
                last_up = STATE.get("_last_upcoming", 0)
            if now_ts_int - last_up >= 180:
                with LOCK:
                    STATE["_last_upcoming"] = now_ts_int
                all_upcoming = []
                for gk, on in games.items():
                    if not on:
                        continue
                    disc = DISCIPLINES[gk]
                    for m in fetch_upcoming(disc["id"], hours=21):
                        if m.get("bo_type") != 3:
                            continue
                        t1, t2 = parse_slug(m.get("slug", ""))
                        sd = m.get("start_date", "")
                        try:
                            dt = datetime.fromisoformat(sd.replace("Z", "+00:00"))
                            now_utc = datetime.now(timezone.utc)
                            diff_h = (dt.timestamp() - now_utc.timestamp()) / 3600
                            if KYIV:
                                kyiv_time = dt.astimezone(KYIV).strftime("%d.%m %H:%M")
                            else:
                                kyiv_time = dt.strftime("%d.%m %H:%M")
                        except Exception:
                            diff_h = 99
                            kyiv_time = sd[:16]

                        mid = m["id"]
                        ck = f"up_{mid}"
                        with LOCK:
                            cached = STATE["vol_cache"].get(ck)
                        if cached is None:
                            cvol, ctitle, cslug = pm_volume(t1, t2, disc["pm_tag"])
                            with LOCK:
                                STATE["vol_cache"][ck] = (cvol, ctitle, cslug)
                        else:
                            cvol, ctitle, cslug = cached

                        vtext, vcolor = vol_verdict(cvol, gk)
                        purl = f"https://polymarket.com/event/{cslug}" if cslug else ""

                        # ROI подсказка по объёму
                        th = VOL_THRESHOLDS[gk]
                        if cvol is None:
                            roi_hint = ""
                        elif gk == "cs2" and cvol < th["kill"]:
                            roi_hint = "⛔ killzone"
                        elif cvol < th["fat"]:
                            roi_hint = "ROI ~+40%"
                        elif cvol < th["thin"]:
                            roi_hint = "ROI ~+25%"
                        else:
                            roi_hint = "нет edge"

                        all_upcoming.append({
                            "game": disc["label"],
                            "t1": t1, "t2": t2,
                            "kyiv_time": kyiv_time,
                            "diff_h": round(diff_h, 1),
                            "vol_text": vtext,
                            "vol_color": vcolor,
                            "pm_url": purl,
                            "tier": m.get("tier", ""),
                            "roi_hint": roi_hint,
                        })

                all_upcoming.sort(key=lambda x: x["diff_h"])
                with LOCK:
                    STATE["upcoming"] = all_upcoming
                push_log(f"📅 Upcoming обновлено: {len(all_upcoming)} матчей", "info")
        except Exception as e:
            push_log(f"upcoming ошибка: {e}", "info")

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
                    "upcoming": STATE["upcoming"],
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
                    STATE["vol_cache"] = {}
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
.pill2{display:flex;align-items:center;gap:8px;flex-wrap:wrap;background:#1a1d27;border:1px solid #2a2d3a;border-radius:10px;padding:7px 11px;font-size:13px;color:#c8c8c8;margin-bottom:6px}
.pill2 .g{font-size:10px;color:#666;text-transform:uppercase;background:#262a36;padding:2px 6px;border-radius:4px}
.pill2 .nm{font-weight:500;color:#e8e8e8}
.pill2 .sc{font-weight:700;color:#fff;font-variant-numeric:tabular-nums}
.tag-win{font-size:11px;color:#7FE3C2;background:#0d2a1e;border:1px solid #1D9E7566;padding:2px 8px;border-radius:5px;font-weight:600}
.tag-wait{font-size:11px;color:#888;background:#161a26;border:1px solid #2a2d3a;padding:2px 8px;border-radius:5px}
.tag-vol{font-size:11px;background:#12151f;border:1px solid #333;padding:2px 8px;border-radius:5px;font-weight:600}
.tag-link{margin-left:auto;color:#5BA3E0;text-decoration:none;font-size:15px;padding:0 4px}
.tag-link:active{opacity:.6}
.nolive{font-size:12px;color:#444;font-style:italic}
.upcoming{margin-bottom:12px}
.upcoming h2{font-size:11px;text-transform:uppercase;letter-spacing:.07em;color:#555;margin-bottom:8px}
.urow{display:flex;align-items:center;gap:8px;flex-wrap:wrap;background:#12151f;border:1px solid #2a2d3a;border-radius:9px;padding:7px 11px;font-size:13px;margin-bottom:5px}
.urow .ut{font-size:10px;color:#444;background:#1a1d27;padding:2px 7px;border-radius:4px;text-transform:uppercase;min-width:34px;text-align:center}
.urow .unm{font-weight:500;color:#e0e0e0;flex:1}
.urow .utime{font-size:12px;color:#666;white-space:nowrap}
.urow .uvol{font-size:11px;font-weight:600;padding:2px 8px;border-radius:5px;background:#12151f;border:1px solid #333}
.urow .ulink{color:#5BA3E0;text-decoration:none;font-size:15px;padding:0 2px}
.urow .ulink:active{opacity:.6}
.logw{background:#1a1d27;border:1px solid #2a2d3a;border-radius:12px;overflow:hidden}
.logh{padding:8px 14px;border-bottom:1px solid #2a2d3a;font-size:11px;color:#444}
.logb{height:170px;overflow-y:auto;padding:4px 0;font-size:12px}
.lr{padding:4px 14px;display:flex;gap:8px;border-left:3px solid transparent}
.lr.info{border-color:#2a2d3a}.lr.match{border-color:#378ADD;background:#0a1520}.lr.alert{border-color:#E24B4A;background:#200a0a}
.lt{color:#444;min-width:44px;flex-shrink:0}
.lm{color:#c0c0c0;line-height:1.5}.lm.am{color:#ff8080;font-weight:500}
.ac{display:none;background:#250c0c;border:1px solid #E24B4A88;border-radius:11px;padding:12px;margin-bottom:10px;box-shadow:0 0 16px #E24B4A22}
.ac.show{display:block}
.ac .bd{font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:#ff8585;margin-bottom:7px;font-weight:600}
.ac .gm{display:inline-block;font-size:10px;background:#3a3d4a;color:#bbb;padding:2px 7px;border-radius:5px;margin-left:7px}
.at{font-size:18px;font-weight:600;color:#fff;margin-bottom:3px}
.am2{font-size:12px;color:#888;margin-bottom:9px}
.wb{background:#0d2a1e;border:1px solid #1D9E7588;border-radius:8px;padding:8px 12px;margin-bottom:8px}
.wb .wl{font-size:10px;color:#7FE3C2;text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px}
.wb .wn{font-size:16px;font-weight:600;color:#B8F0DD}
.legs{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-bottom:8px}
.leg{background:#161a26;border-radius:8px;padding:7px 11px}
.leg .ll{font-size:10px;color:#777;margin-bottom:2px;text-transform:uppercase;letter-spacing:.06em}
.leg .lv{font-size:13px;font-weight:500;color:#e0e0e0}
.vb{border-radius:8px;padding:8px 12px;margin-bottom:7px}
.vb .vl{font-size:10px;color:#888;text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px}
.vb .vv{font-size:16px;font-weight:600}
.vb .vt{font-size:11px;color:#777;margin-top:3px;word-break:break-word}
.hint{font-size:11px;color:#aaa;background:#161a26;border-radius:7px;padding:7px 11px;line-height:1.45;margin-bottom:7px}
.dis{width:100%;background:#12151f;color:#555;border:1px solid #2a2d3a;border-radius:7px;padding:7px;font-size:12px;cursor:pointer}
.pmlink{display:block;text-align:center;background:#13212e;color:#5BA3E0;border:1px solid #2a4a63;border-radius:7px;padding:9px;font-size:12px;text-decoration:none;margin-bottom:8px;font-weight:500}
.pmlink:active{opacity:.7}
.achead{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.acx{background:none;border:none;color:#555;font-size:18px;cursor:pointer;padding:0 4px;line-height:1;width:auto;flex:none}
</style></head><body>
<h1><div class="dot" id="dot"></div> Esports Bo3 Monitor</h1>

<div id="alerts"></div>

<div class="live"><h2>Live матчи</h2><div id="live"><span class="nolive">—</span></div></div>

<div class="upcoming"><h2>📅 Ближайшие матчи (21ч)</h2><div id="upcoming"><span class="nolive">—</span></div></div>

<div class="card">
<h2>Игры</h2>
<div class="games">
<div class="chip on" data-g="cs2" onclick="tg(this)">CS2</div>
<div class="chip" data-g="valorant" onclick="tg(this)">Valorant</div>
<div class="chip" data-g="dota2" onclick="tg(this)">Dota 2</div>
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

function beep(){try{const c=new(window.AudioContext||window.webkitAudioContext)();[[880,0],[660,.2],[880,.4]].forEach(([freq,t])=>{const o=c.createOscillator(),g=c.createGain();o.connect(g);g.connect(c.destination);o.frequency.value=freq;o.type='sine';g.gain.setValueAtTime(1.0,c.currentTime+t);g.gain.exponentialRampToValueAtTime(.001,c.currentTime+t+.2);o.start(c.currentTime+t);o.stop(c.currentTime+t+.21)})}catch(e){}}

function dot(s){document.getElementById('dot').className='dot'+(s?' '+s:'')}

function tg(el){el.classList.toggle('on')}

function getGames(){const g={};document.querySelectorAll('.chip').forEach(c=>g[c.dataset.g]=c.classList.contains('on'));return g}

function renderLog(log){
  const b=document.getElementById('log');
  b.innerHTML=log.map(e=>`<div class="lr ${e.level}"><span class="lt">${e.ts}</span><span class="lm ${e.level==='alert'?'am':''}">${e.msg}</span></div>`).join('');
  b.scrollTop=b.scrollHeight;
}

function renderUpcoming(upcoming){
  const el=document.getElementById('upcoming');
  if(!upcoming||!upcoming.length){el.innerHTML='<span class="nolive">Нет запланированных матчей</span>';return}
  el.innerHTML=upcoming.map(m=>{
    const vol=m.vol_text&&m.vol_text!=='?'
      ?`<span class="uvol" style="color:${m.vol_color};border-color:${m.vol_color}55">${m.vol_text}</span>`
      :`<span class="uvol" style="color:#555;border-color:#333">нет рынка</span>`;
    const roi=m.roi_hint?`<span class="uvol" style="color:#888;border-color:#333;font-weight:400">${m.roi_hint}</span>`:'';
    const link=m.pm_url?`<a class="ulink" href="${m.pm_url}" target="_blank">↗</a>`:'';
    const diffStr=m.diff_h<1?`${Math.round(m.diff_h*60)}мин`:`${m.diff_h.toFixed(1)}ч`;
    return `<div class="urow">
      <span class="ut">${escapeHtml(m.game)}</span>
      <span class="unm">${escapeHtml(m.t1)} vs ${escapeHtml(m.t2)}</span>
      <span class="utime">⏰ ${m.kyiv_time} (через ${diffStr})</span>
      ${vol}${roi}${link}
    </div>`;
  }).join('');
}

function renderLive(live){
  const el=document.getElementById('live');
  if(!live.length){el.innerHTML='<span class="nolive">Нет активных матчей</span>';return}
  el.innerHTML=live.map(m=>{
    const win = m.window
      ? `<span class="tag-win">🎯 Окно</span>`
      : `<span class="tag-wait">⏳ ждём К1</span>`;
    const vol = m.vol_text && m.vol_text!=='?'
      ? `<span class="tag-vol" style="color:${m.vol_color};border-color:${m.vol_color}55">${m.vol_text}</span>`
      : `<span class="tag-vol" style="color:#666;border-color:#333">нет рынка</span>`;
    const roi = m.roi_hint
      ? `<span class="tag-vol" style="color:#888;border-color:#333;font-weight:400">${m.roi_hint}</span>`
      : '';
    const time = m.start_kyiv
      ? `<span class="tag-vol" style="color:#555;border-color:#2a2d3a;font-weight:400">⏰ ${m.start_kyiv}</span>`
      : '';
    const link = m.pm_url
      ? `<a class="tag-link" href="${m.pm_url}" target="_blank" title="Открыть на Polymarket">↗</a>`
      : '';
    return `<div class="pill2">
      <span class="g">${m.game}</span>
      <span class="nm">${escapeHtml(m.t1)} vs ${escapeHtml(m.t2)}</span>
      <span class="sc">${m.s1}:${m.s2}</span>
      ${time}${win}${vol}${roi}${link}
    </div>`;
  }).join('');
}

const MAX_ALERTS=5;
let shownIds=[];

function escapeHtml(s){return (s||'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}

function buildCard(a){
  const pmLink = a.pm_url
    ? `<a class="pmlink" href="${a.pm_url}" target="_blank">↗ Открыть на Polymarket</a>`
    : '';
  const fin = a.finished_at ? ` · ${a.finished_at} КИЇВ` : '';
  const mapScore = a.map_score
    ? `<div class="am2">Счёт серии: ${a.score} &nbsp;|&nbsp; Рахунок К1: <b>${a.map_score}</b></div>`
    : `<div class="am2">Счёт: ${a.score}</div>`;
  const roiBlock = a.k1_label ? `
    <div class="wb" style="margin-bottom:7px">
      <div class="wl">Стратегія</div>
      <div class="wn" style="font-size:14px">${escapeHtml(a.k1_label)}</div>
    </div>
    <div style="display:flex;gap:7px;margin-bottom:8px">
      <div class="vb" style="flex:1;border:1px solid #378ADD44;background:#0a1520">
        <div class="vl">Базовий ROI</div>
        <div class="vv" style="color:#5BA3E0">${escapeHtml(a.base_roi)}</div>
      </div>
      ${a.score_label ? `<div class="vb" style="flex:2;border:1px solid #2a2d3a;background:#12151f">
        <div class="vl">Рахунок К1</div>
        <div class="vv" style="color:#e0e0e0;font-size:13px">${escapeHtml(a.score_label)}</div>
        <div class="vt">${escapeHtml(a.score_hint)}</div>
      </div>` : ''}
    </div>` : '';
  return `<div class="ac show" data-id="${a.id}">
    <div class="achead">
      <div class="bd">🚨 Карта 1 завершена${fin}<span class="gm">${escapeHtml(a.game)}</span></div>
      <button class="acx" onclick="closeCard(${a.id})">✕</button>
    </div>
    <div class="at">${escapeHtml(a.t1)} vs ${escapeHtml(a.t2)}</div>
    ${mapScore}
    <div class="wb"><div class="wl">Взял К1</div><div class="wn">✅ ${escapeHtml(a.winner)}</div></div>
    <div class="legs">
      <div class="leg"><div class="ll">Нога 1 — BUY серію</div><div class="lv">${escapeHtml(a.winner)}</div></div>
      <div class="leg"><div class="ll">Нога 2 — BUY карту 2</div><div class="lv">${escapeHtml(a.loser)}</div></div>
    </div>
    ${roiBlock}
    <div class="vb" style="border:1px solid ${a.vol_color}66;background:${a.vol_color}1a">
      <div class="vl">Об'єм Polymarket</div>
      <div class="vv" style="color:${a.vol_color}">${escapeHtml(a.vol_text)}</div>
      <div class="vt">${escapeHtml(a.pm_title)||'Ринок не знайдено'}</div>
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
    renderLog(s.log);renderLive(s.live);renderUpcoming(s.upcoming||[]);
    if(s.alerts)s.alerts.forEach(showAlert);
  }catch(e){}
}

function start(){
  if(Notification.permission==='default')Notification.requestPermission();
  fetch('/start',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({games:getGames(),interval:parseInt(document.getElementById('iv').value)||20,min_tier:document.getElementById('tier').value})});
}
function stop(){fetch('/stop',{method:'POST'})}
function test(){const t=new Date().toLocaleTimeString('uk-UA',{hour:'2-digit',minute:'2-digit',timeZone:'Europe/Kyiv'});showAlert({id:Date.now(),game:'CS2',t1:'Team Spirit',t2:'NAVI',score:'1:0',winner:'Team Spirit',loser:'NAVI',vol_text:'🟢 ЖИР  $87,000',vol_color:'#1D9E75',pm_title:'Counter-Strike: Spirit vs NAVI (BO3) - IEM Cologne',pm_url:'https://polymarket.com/event/cs2-aaa-inf1-2026-03-10',hint:'CS2: ставь ПРОТИВ победителя К1 (фав взял→BUY аутсайдер)',finished_at:t,k1_label:'Фав взял К1 → BUY аутсайдер',base_roi:'+57%',score_label:'Тесно (16:12, разрыв 4)',score_hint:'→ ROI вище середнього (~+47%)',map_score:'16:12'})}

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
