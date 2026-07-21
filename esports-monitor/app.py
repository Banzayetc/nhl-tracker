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

# Пороги объёма: БОЛЬШЕ = ЛУЧШЕ. Ниже kill — «килзон» (мало, не торговать);
# от kill до green — жёлтый (средний); выше green — зелёный (большой объём).
VOL_THRESHOLDS = {
    "cs2":      {"kill": 40_000,  "green": 100_000},
    "valorant": {"kill": 15_000,  "green": 40_000},   # объёмы ниже CS2 — пороги подбери при желании
    "dota2":    {"kill": 12_000,  "green": 30_000},
}
VOL_TTL = 90   # сек: как часто перетягивать объём матча (живое обновление)

# ── Polymarket read-only прокси (для анализа с заблокированного ISP) ──────────
# Локально Polymarket режется провайдером (HTTP 451). Этот эндпоинт даёт чистый
# выход к публичным read-API Polymarket. Только whitelisted-хосты + токен, чтобы
# не был открытым прокси. GET /pm?k=TOKEN&u=<urlencoded url> — один запрос.
# POST /pm?k=TOKEN  {"urls":[...]} — батч (список ответов в том же порядке).
PM_PROXY_KEY = os.environ.get("PM_PROXY_KEY", "lob-2c95-pm-a7f2c9b5")
PM_HOSTS = {"gamma-api.polymarket.com", "clob.polymarket.com",
            "data-api.polymarket.com", "polymarket.com"}

def _pm_fetch(url, timeout=15):
    """Тянет один Polymarket URL. Возвращает распарсенный JSON или {'_err':...}."""
    try:
        host = urllib.parse.urlparse(url).hostname or ""
        if host not in PM_HOSTS:
            return {"_err": "host not allowed", "host": host}
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
        try:
            return json.loads(raw)
        except Exception:
            return {"_raw": raw.decode("utf-8", "replace")}
    except Exception as e:
        return {"_err": str(e)}

def _book_top(tok, timeout=8):
    """(лучший бид, лучший аск) из стакана CLOB.
    Мейкер покупает по БИДУ (лимитка), тейкер — по АСКУ. Отдаём оба, тумблер выбирает."""
    if not tok:
        return (None, None)
    b = _pm_fetch("https://clob.polymarket.com/book?token_id=" + str(tok), timeout=timeout)
    if not isinstance(b, dict):
        return (None, None)
    try:
        asks = [float(x["price"]) for x in (b.get("asks") or [])]
        bids = [float(x["price"]) for x in (b.get("bids") or [])]
        return (max(bids) if bids else None, min(asks) if asks else None)  # (лучший бид, лучший аск)
    except Exception:
        return (None, None)

def _prematch_px(tok, timeout=8):
    """Пред-матчевая цена outcome = медиана ПЕРВЫХ тиков истории (цена на открытии рынка,
    до карты 1). Устойчиво к одиночному стартовому шуму."""
    if not tok:
        return None
    h = _pm_fetch("https://clob.polymarket.com/prices-history?market=" + str(tok) + "&interval=max&fidelity=10", timeout=timeout)
    hist = h.get("history") if isinstance(h, dict) else None
    if not hist:
        return None
    ps = sorted(float(x["p"]) for x in hist[:6] if "p" in x)
    return ps[len(ps) // 2] if ps else None      # медиана первых тиков

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
    "vol_cache": {},         # match_id -> (vol, title, slug, ts) — с TTL
    "upcoming": [],          # предстоящие матчи на 21ч вперёд
    "diag": [],              # ВРЕМЕННО: журнал переходов фаз для проверки источника
    "diag_seen": {},         # match_id -> последняя сигнатура (ps,s1,s2,cov,lu)
    "sounds": [],            # звуковые события фаз (m1start/m1end/m2end)
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

def emit_sound(phase, t1, t2, game):
    # звуковая фаза: m1start | m1end | m2end (фронт проговаривает голосом)
    with LOCK:
        STATE["sounds"].append({"id": int(time.time() * 1000), "phase": phase,
                                "t1": t1, "t2": t2, "game": game, "ts": now_ts()})
        STATE["sounds"] = STATE["sounds"][-12:]

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

def cs2feed(pm_url, light=False, game="cs2"):
    """Фид для калькулятора Δ-neutral: по ссылке/слугу Polymarket матча отдаёт
    цены (серия/карта2 обеих команд, статус карты1) + живое состояние карт с bo3.gg
    (счёт серии, номер карты, идёт/перерыв, кто взял К1). Всё через уже готовые хелперы.
    light=True — только состояние карт (для автоопроса), без запросов стакана (быстро).
    game — дисциплина bo3.gg для лайв-трекинга: cs2 (по умолч.) / dota2 / valorant.
    Рынки Polymarket парсятся game-agnostic (Map N Winner для CS2, Game N Winner для Dota)."""
    disc_id = DISCIPLINES.get(game, DISCIPLINES["cs2"])["id"]
    slug = (pm_url or "").strip().rstrip("/").split("/")[-1].split("?")[0]
    if not slug:
        return {"_err": "no slug"}
    ev = _pm_fetch("https://gamma-api.polymarket.com/events?slug=" + urllib.parse.quote(slug))
    if isinstance(ev, list):
        ev = ev[0] if ev else {}
    if not isinstance(ev, dict) or ev.get("_err"):
        return {"_err": "polymarket event not found", "slug": slug}
    markets = ev.get("markets", []) or []

    def pair(m):
        try:
            outs = m.get("outcomes"); pv = m.get("outcomePrices")
            tk = m.get("clobTokenIds")
            outs = json.loads(outs) if isinstance(outs, str) else outs
            pv = [float(x) for x in (json.loads(pv) if isinstance(pv, str) else pv)]
            tk = json.loads(tk) if isinstance(tk, str) else (tk or [])
            return outs, pv, tk
        except Exception:
            return None, None, None

    series = map1 = map2 = None      # каждый: (outs, pv_mid, toks)
    t1name = t2name = None
    for m in markets:
        q = (m.get("question") or "").lower()
        outs, pv, tk = pair(m)
        if not outs or len(outs) < 2 or not pv:
            continue
        # ВАЖНО: у матча есть Map-Handicap/Map-Total-Rounds рынки — берём ТОЛЬКО «Map N Winner».
        junk = ("handicap" in q) or ("total" in q) or ("rounds" in q) or ("over/under" in q)
        if ("map 1" in q or "game 1" in q) and "winner" in q and not junk:   # CS2=Map, Dota/LoL=Game
            map1 = (outs, pv, tk)
        elif ("map 2" in q or "game 2" in q) and "winner" in q and not junk:
            map2 = (outs, pv, tk)
        elif ("(bo3)" in q or "(bo5)" in q or "match winner" in q) and not junk and "map" not in q and "game" not in q:
            series = (outs, pv, tk); t1name, t2name = outs[0], outs[1]
    if not t1name:
        src = series or map2 or map1
        if src:
            t1name, t2name = src[0][0], src[0][1]

    # ЦЕНЫ из CLOB-стакана: и БИД (мейкер), и АСК (тейкер). Мид как фолбэк.
    def tops(entry):                                 # стакан по обоим токенам (1 запрос/токен)
        outs, pv, tk = entry
        return [(_book_top(tk[i]) if (tk and len(tk) > i and tk[i]) else (None, None)) for i in range(2)]
    def px(entry, tp, i, side):
        outs, pv, tk = entry
        bid, ask = tp[i]
        v = ask if side == "ask" else bid
        return round((v if v is not None else pv[i]) * 100, 1)

    prices = {"ask": {}, "bid": {}}
    if not light:                                    # автоопрос (light) не дёргает стакан
        sTop = tops(series) if series else None
        mTop = tops(map2) if map2 else None
        for side in ("ask", "bid"):
            if series:
                prices[side]["t1_series"] = px(series, sTop, 0, side)
                prices[side]["t2_series"] = px(series, sTop, 1, side)
            if map2:
                prices[side]["t1_map2"] = px(map2, mTop, 0, side)
                prices[side]["t2_map2"] = px(map2, mTop, 1, side)
    def _resolve(mkt):                          # резолв рынка Winner по миду (>0.97): карта/серия закрыта.
        if not mkt:                             # НАДЁЖНО и НЕ пропадает после конца матча (в отличие от bo3.gg,
            return None                         # который выкидывает завершённую серию из status=current).
        mx = max(mkt[1])
        return {"resolved": mx > 0.97,
                "winner": (mkt[0][mkt[1].index(mx)] if mx > 0.97 else None)}
    map1st   = _resolve(map1)                   # доступны и в light-режиме (миды парсятся до проверки light)
    map2st   = _resolve(map2)
    seriesst = _resolve(series)

    bo3 = {"matched": False}
    try:
        for lm in fetch_live(disc_id):
            if lm.get("bo_type") != 3:
                continue
            b1, b2 = parse_slug(lm.get("slug", ""))
            if not (b1 and b2 and t1name and t2name):
                continue
            ok = (_name_match(b1, t1name) and _name_match(b2, t2name)) or \
                 (_name_match(b1, t2name) and _name_match(b2, t1name))
            if not ok:
                continue
            s1, s2 = get_score(lm)                       # в порядке bo3 (b1,b2)
            if _name_match(b1, t1name):
                pm_s1, pm_s2 = s1, s2                     # b1 = наша t1
            else:
                pm_s1, pm_s2 = s2, s1                     # реверс
            lu = lm.get("live_updates") or {}
            # победитель К1: ПРИОРИТЕТ — резолв Polymarket Map 1 Winner (надёжно;
            # счёт bo3 может быть перевёрнут из-за расхождения порядка команд слуг/live_updates).
            k1w = None
            if map1st and map1st.get("resolved") and map1st.get("winner"):
                k1w = map1st["winner"]
            elif pm_s1 + pm_s2 >= 1:
                k1w = t1name if pm_s1 > pm_s2 else t2name
            # выправляем ориентацию счёта при 1:0 по победителю К1 (счёт bo3 бывает зеркальным)
            score = [pm_s1, pm_s2]
            if k1w and (pm_s1 + pm_s2) == 1:
                score = [1, 0] if k1w == t1name else ([0, 1] if k1w == t2name else score)
            bo3 = {
                "matched": True,
                "score": score,                           # серия в порядке t1:t2 (выправлено по К1)
                "game_number": int(lu.get("game_number") or 0),
                "game_ended": bool(lu.get("game_ended")),
                "maps_score": lm.get("maps_score"),
                "map1_winner": k1w,
                "bo3_slug": lm.get("slug", ""),
            }
            break
    except Exception as e:
        bo3 = {"matched": False, "_err": str(e)}

    prematch = {}                                    # пред-матчевая цена серии (для контекст-полей)
    if not light and series:
        stk = series[2]
        if stk and len(stk) >= 2:
            a = _prematch_px(stk[0]); b = _prematch_px(stk[1])
            if a is not None:
                prematch["t1"] = round(a * 100, 1)
            if b is not None:
                prematch["t2"] = round(b * 100, 1)

    return {"slug": slug, "t1name": t1name, "t2name": t2name,
            "prices": prices, "prematch": prematch,
            "map1": map1st, "map2": map2st, "series": seriesst, "bo3": bo3}

def vol_label(vol, game):
    th = VOL_THRESHOLDS[game]
    if vol is None:
        return "❓ Не найден на Polymarket", "#888888"
    if vol < th["kill"]:
        return f"⛔ КІЛЗОН  ${vol:,.0f}", "#666666"       # мало — не торговать
    if vol < th["green"]:
        return f"🟡 СРЕДНИЙ  ${vol:,.0f}", "#BA7517"       # средний объём
    return f"🟢 БОЛЬШОЙ  ${vol:,.0f}", "#1D9E75"           # большой = хорошо

def vol_verdict(vol, game):
    """Короткая пометка для списка: цвет по объёму (больше = лучше)"""
    th = VOL_THRESHOLDS[game]
    if vol is None:
        return "?", "#888888"
    if vol < th["kill"]:
        return f"⛔ ${vol/1000:.0f}K", "#666666"           # килзон (мало)
    if vol < th["green"]:
        return f"🟡 ${vol/1000:.0f}K", "#BA7517"            # средний
    return f"🟢 ${vol/1000:.0f}K", "#1D9E75"                # большой

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

                # ── ВРЕМЕННА ДІАГНОСТИКА джерела (старт/кінець карти) ──
                # Пишемо запис ЛИШЕ коли щось змінилось (parsed_status / рахунок / покриття / live_updates).
                try:
                    _ps = m.get("parsed_status")
                    _cov = bool(m.get("live_coverage"))
                    _lup = bool(lu)
                    _sig = (_ps, score[0], score[1], _cov, _lup)
                    with LOCK:
                        _prev = STATE["diag_seen"].get(mid)
                    if _sig != _prev:
                        with LOCK:
                            STATE["diag_seen"][mid] = _sig
                            STATE["diag"].append({
                                "ts": now_ts(), "slug": m.get("slug", ""),
                                "game": disc["label"], "tier": m.get("tier"),
                                "ps": _ps, "sc": f"{score[0]}:{score[1]}",
                                "cov": _cov, "lu": _lup,
                                "gn": (lu.get("game_number") if _lup else None),
                                "ge": (lu.get("game_ended") if _lup else None),
                                "maps": m.get("maps_score"),
                            })
                            STATE["diag"] = STATE["diag"][-400:]
                except Exception:
                    pass
                # ── /діагностика ──

                # В список показываем только матчи где вход ещё возможен:
                # 0:0 (ждём К1) и 1:0/0:1 (момент входа). 1:1 и решённые серии скрываем.
                stotal = score[0] + score[1]
                if stotal <= 1:
                    window = (stotal == 1)   # 1:0 → окно дельта-нейтрали открыто

                    # объём с TTL — перетягиваем вживую (объём растёт по ходу матча)
                    now_m = time.time()
                    with LOCK:
                        cached = STATE["vol_cache"].get(mid)
                    if cached is None or (now_m - cached[3]) > VOL_TTL:
                        cvol, ctitle, cslug = pm_volume(t1, t2, disc["pm_tag"])
                        with LOCK:
                            STATE["vol_cache"][mid] = (cvol, ctitle, cslug, now_m)
                    else:
                        cvol, ctitle, cslug = cached[0], cached[1], cached[2]

                    vtext, vcolor = vol_verdict(cvol, gk)
                    purl = f"https://polymarket.com/event/{cslug}" if cslug else ""

                    # ROI hint по объёму
                    th = VOL_THRESHOLDS[gk]
                    if cvol is None:
                        roi_hint = ""
                    elif cvol < th["kill"]:
                        roi_hint = "⛔ килзон (мало)"
                    elif cvol < th["green"]:
                        roi_hint = "🟡 средний"
                    else:
                        roi_hint = "🟢 большой объём"

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
                    if total == 0:
                        emit_sound("m1start", t1, t2, disc["label"])   # матч вышел в эфир = старт К1
                    # если матч впервые увиден уже на 1:0 — это тоже окно входа, алертим
                    if total != 1:
                        continue
                    # иначе проваливаемся в блок алерта ниже

                prev_total = (prev[0] + prev[1]) if prev else -1

                # Алерт когда серия в состоянии 1:0/0:1 (К1 сыграна, перерыв),
                # и мы по этому матчу ещё не алертили. Не зависит от game_ended.
                if total == 1 and not already:
                    # Кто взял К1 = у кого больше счёт серии (надёжно з match_score)
                    winner = t1 if score[0] > score[1] else t2
                    loser = t2 if winner == t1 else t1
                    with LOCK:
                        STATE["alerted"].add(akey)
                    emit_sound("m1end", t1, t2, disc["label"])   # К1 завершена = сигнал входа

                    push_log(f"🚨 [{disc['label']}] КАРТА 1: {t1} vs {t2} → {winner}", "alert")
                    vol, pm_title, pm_slug = pm_volume(t1, t2, disc["pm_tag"])
                    vtext, vcolor = vol_label(vol, gk)
                    pm_url = f"https://polymarket.com/event/{pm_slug}" if pm_slug else ""
                    push_log(f"   {vtext}", "alert")

                    # Счёт карты 1 из bo3.gg
                    ms1 = m.get("team1_last_game_score")
                    ms2 = m.get("team2_last_game_score")

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
                            "finished_at": now_ts(),
                            "map_score": f"{ms1}:{ms2}" if ms1 is not None else "",
                        })
                        STATE["alerts"] = STATE["alerts"][-5:]

                if prev is not None and prev_total == 1 and total == 2:
                    emit_sound("m2end", t1, t2, disc["label"])   # К2 завершена (1:1 / 2:0)

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
                        now_u = time.time()
                        with LOCK:
                            cached = STATE["vol_cache"].get(ck)
                        if cached is None or (now_u - cached[3]) > VOL_TTL:
                            cvol, ctitle, cslug = pm_volume(t1, t2, disc["pm_tag"])
                            with LOCK:
                                STATE["vol_cache"][ck] = (cvol, ctitle, cslug, now_u)
                        else:
                            cvol, ctitle, cslug = cached[0], cached[1], cached[2]

                        vtext, vcolor = vol_verdict(cvol, gk)
                        purl = f"https://polymarket.com/event/{cslug}" if cslug else ""

                        # ROI подсказка по объёму
                        th = VOL_THRESHOLDS[gk]
                        if cvol is None:
                            roi_hint = ""
                        elif cvol < th["kill"]:
                            roi_hint = "⛔ килзон (мало)"
                        elif cvol < th["green"]:
                            roi_hint = "🟡 средний"
                        else:
                            roi_hint = "🟢 большой объём"

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
        elif self.path.startswith("/pm"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            if (q.get("k", [""])[0]) != PM_PROXY_KEY:
                self._send(403, "application/json", '{"_err":"forbidden"}')
                return
            u = q.get("u", [""])[0]
            if not u:
                self._send(400, "application/json", '{"_err":"missing u"}')
                return
            payload = _pm_fetch(u)
            self._send(200, "application/json", json.dumps(payload, ensure_ascii=False))
        elif self.path.startswith("/cs2feed"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            if (q.get("k", [""])[0]) != PM_PROXY_KEY:
                self._send(403, "application/json", '{"_err":"forbidden"}')
                return
            u = q.get("u", [""])[0]
            if not u:
                self._send(400, "application/json", '{"_err":"missing u"}')
                return
            light = (q.get("maps", [""])[0] == "1")   # &maps=1 → только карты (автоопрос), без стакана
            game = (q.get("game", ["cs2"])[0] or "cs2")   # &game=dota2 → лайв-трекинг по нужной дисциплине bo3.gg
            try:
                payload = cs2feed(u, light=light, game=game)
            except Exception as e:
                payload = {"_err": str(e)}
            self._send(200, "application/json", json.dumps(payload, ensure_ascii=False))
        elif self.path.startswith("/weather"):
            try:
                payload = scan_weather()
            except Exception as e:
                payload = {"events": [], "error": str(e)}
            self._send(200, "application/json", json.dumps(payload, ensure_ascii=False))
        elif self.path.startswith("/tennis"):
            try:
                payload = scan_tennis()
            except Exception as e:
                payload = {"matches": [], "error": str(e)}
            self._send(200, "application/json", json.dumps(payload, ensure_ascii=False))
        elif self.path.startswith("/bo5"):
            try:
                payload = scan_bo5()
            except Exception as e:
                payload = {"series": [], "watch": [], "error": str(e)}
            self._send(200, "application/json", json.dumps(payload, ensure_ascii=False))
        elif self.path.startswith("/diag"):
            with LOCK:
                payload = {"diag": STATE["diag"], "count": len(STATE["diag"])}
            self._send(200, "application/json", json.dumps(payload, ensure_ascii=False))
        elif self.path.startswith("/state"):
            with LOCK:
                payload = {
                    "running": STATE["running"],
                    "live": STATE["live"],
                    "log": STATE["log"][-60:],
                    "alerts": STATE["alerts"],
                    "sounds": STATE["sounds"],
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

        if self.path.startswith("/pm"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            if (q.get("k", [""])[0]) != PM_PROXY_KEY:
                self._send(403, "application/json", '{"_err":"forbidden"}')
                return
            urls = data.get("urls", [])
            if not isinstance(urls, list):
                urls = []
            results = [_pm_fetch(u) for u in urls[:200]]
            self._send(200, "application/json",
                       json.dumps({"results": results}, ensure_ascii=False))
            return

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

# ── ПОГОДА (дневные температуры Polymarket, tag_id=84) ───────────────────────────
# Стратегия (измерено на 150 событиях): за ~24ч до конца рынка покупать ФАВОРИТА
# (бакет с макс. ценой) — вход ~40¢, реально выигрывает 48.6%, перекос +8.5пп, t=2.10,
# ROI +21% валовых. Плечи 10-30¢ переоценены → их фейдить. Прогноз НЕ помогает (basis risk).
WEATHER_TAG = 84
_WX_EU = {"london","paris","amsterdam","munich","madrid","moscow","helsinki","istanbul","berlin","rome","warsaw","kyiv","kiev","vienna","dublin","lisbon","oslo","stockholm","zurich","barcelona","milan","prague","athens","budapest"}
_WX_AS = {"beijing","shanghai","tokyo","seoul","hong kong","shenzhen","guangzhou","mumbai","lucknow","delhi","bangkok","manila","jakarta","singapore","osaka","taipei","hanoi","dhaka","karachi","chengdu","wuhan","busan","kolkata"}

def _wx_region(city, is_f):
    if is_f:
        return "US"
    c = city.lower()
    if c in _WX_EU:
        return "Europe"
    if c in _WX_AS:
        return "Asia"
    return "Other"

def _wx_price(m):
    for k in ("lastTradePrice", "bestAsk"):
        v = m.get(k)
        if v not in (None, ""):
            try:
                return float(v)
            except Exception:
                pass
    op = m.get("outcomePrices")
    try:
        if isinstance(op, str):
            op = json.loads(op)
        return float(op[0])
    except Exception:
        return None

def _wx_signal(h, p, low_vol):
    # Два підтверджених сигнали (бектест 1629 подій, OOS +28%, t>5):
    #   ЦІНА фаворита < 50¢ (чим дешевше тим краще: <30¢ найсильніше)
    #   НИЗЬКИЙ обсяг ринку (нижня третина) — тонкі ринки недооцінюють фаворита
    cheap = p < 0.50
    good = cheap and low_vol
    if h is None:
        return ("—", "#555", good)
    if h > 36:
        return ("⏳ рано", "#666", good)
    if h < 5:
        return ("🔴 пізно", "#E24B4A", good)
    if good:
        if h >= 14:
            tag = "🟢🟢 ВХІД+ (дешево<30¢)" if p < 0.30 else "🟢 ВХІД (дешево+тонко)"
            return (tag, "#1D9E75", good)
        return ("🟡 пізнувато (сигнал ✓)", "#D9A441", good)
    if not cheap and not low_vol:
        why = "фав >50¢ + товстий ринок"
    elif not cheap:
        why = "фав >50¢"
    else:
        why = "ринок не тонкий"
    return ("⚪ пропустити (%s)" % why, "#5a6472", good)

def scan_weather():
    import time as _t
    url = ("https://gamma-api.polymarket.com/events?tag_id=%d"
           "&closed=false&limit=100&order=volume&ascending=false" % WEATHER_TAG)
    ev = http_get_json(url) or []
    now = _t.time()
    raw = []
    for e in ev:
        title = e.get("title", "")
        mm = re.search(r"temperature in (.+?) on (\w+ \d+)", title, re.I)
        if not mm:
            continue
        mks = e.get("markets", [])
        if len(mks) < 3:
            continue
        buckets = []
        end_ts = None
        for m in mks:
            p = _wx_price(m)
            if p is None:
                continue
            buckets.append({"lab": m.get("groupItemTitle") or "", "p": p})
            if end_ts is None and m.get("endDate"):
                try:
                    end_ts = datetime.fromisoformat(
                        m["endDate"].replace("Z", "+00:00")).timestamp()
                except Exception:
                    pass
        if len(buckets) < 3:
            continue
        buckets.sort(key=lambda b: -b["p"])
        fav = buckets[0]
        # обсяг події (поточний; для відносного фільтра)
        try:
            vol = float(e.get("volume") or 0)
        except Exception:
            vol = 0.0
        if vol <= 0:
            vol = sum(float(m.get("volumeNum") or 0) for m in mks)
        is_low = "lowest" in title.lower()
        is_f = "°F" in fav["lab"] or fav["lab"].endswith("F")
        city = mm.group(1).strip()
        h = round((end_ts - now) / 3600, 1) if end_ts else None
        raw.append({"city": city, "date": mm.group(2), "hours": h,
                    "fav_lab": fav["lab"], "fav_p": fav["p"],
                    "fav_px": round(fav["p"] * 100, 1), "vol": vol,
                    "is_low": is_low, "region": _wx_region(city, is_f),
                    "url": "https://polymarket.com/event/" + (e.get("slug", "") or "")})

    # поріг "низького обсягу" = нижня третина серед поточних подій (самокалібрування)
    vols = sorted(r["vol"] for r in raw if r["vol"] > 0)
    vol_thr = vols[len(vols) // 3] if len(vols) >= 3 else 0.0

    out = []
    for r in raw:
        low_vol = r["vol"] < vol_thr
        sig, col, good = _wx_signal(r["hours"], r["fav_p"], low_vol)
        out.append({
            "city": r["city"], "date": r["date"], "hours": r["hours"],
            "fav_lab": r["fav_lab"], "fav_px": r["fav_px"],
            "vol": round(r["vol"]), "low_vol": low_vol,
            "in_zone": good, "is_low": r["is_low"], "region": r["region"],
            "signal": sig, "sig_color": col, "url": r["url"],
        })

    def _sk(x):
        h = x["hours"]
        if h is None:
            return (2, 9, 999)
        in_win = 0 if 14 <= h <= 36 else 1
        return (0 if x["in_zone"] else 1, in_win, abs(h - 24))
    out.sort(key=_sk)
    return {"events": out, "vol_thr": round(vol_thr)}

# ── ТЕННИС (Bo3) — стратегия «2 карты»: живые матчи в перерыве после сета 1 (1:0) ──
# СТРОГО ТОЛЬКО Bo3. Мужские слэмы — Bo5 (до 3 сетов) — стратегия там НЕ работает,
# отсекаем двумя признаками: (1) есть рынок Set 4/Set 5 Winner; (2) мужской слэм по slug/названию.
TENNIS_SLAM = ("wimbledon", "us-open", "us open", "australian", "roland", "french open")

def _tennis_is_bo3(slug, title, has_set45):
    if has_set45:
        return False                      # Set 4/5 Winner → это Bo5
    sl = (slug + " " + title).lower()
    is_slam = any(k in sl for k in TENNIS_SLAM)
    is_men = (slug.lower().startswith("atp-") or "atp" in title.lower().split()
              or "atp:" in title.lower() or "men'" in title.lower() or "men’" in title.lower())
    if is_slam and is_men:
        return False                      # мужской слэм = Bo5 (страховка)
    return True

def _tennis_forecast(t1s):
    # оценка цены матча победителя сета 1 при счёте 1:1 (сглажено по базе 340 матчей)
    for hi, f in [(55, 40), (62, 44), (70, 47), (78, 51), (86, 56), (101, 62)]:
        if t1s < hi:
            return f
    return 62

def _book_depth_usd(token, within=0.12, timeout=8):
    """$ на асках (покупка ноги B) в пределах `within`¢ от лучшего аска."""
    if not token:
        return None
    try:
        b = http_get_json("https://clob.polymarket.com/book?token_id=%s" % token, timeout=timeout)
    except Exception:
        return None
    asks = (b or {}).get("asks") or []
    try:
        pr = sorted((float(a["price"]), float(a["size"])) for a in asks)
    except Exception:
        return None
    if not pr:
        return 0
    best = pr[0][0]
    return round(sum(p * s for p, s in pr if p <= best + within))

def scan_tennis():
    # gamma отдаёт максимум 100/запрос; живых теннис-событий больше (ITF+слэмы),
    # сортировка по startDate прячет идущие сейчас матчи → пагинируем и дедупим.
    base = ("https://gamma-api.polymarket.com/events?tag_slug=tennis"
            "&closed=false&limit=100&order=startDate&ascending=false")
    _sev = {}
    try:
        for off in range(0, 800, 100):
            page = http_get_json(base + ("&offset=%d" % off)) or []
            for x in page:
                _sev[x.get("id")] = x
            if len(page) < 100:
                break
    except Exception as e:
        return {"matches": [], "error": str(e)}
    ev = list(_sev.values())

    def _pr(m):
        try:
            return [float(x) for x in json.loads(m.get("outcomePrices") or "[]")]
        except Exception:
            return []
    def _o(m):
        try:
            return json.loads(m.get("outcomes") or "[]")
        except Exception:
            return []
    def _tk(m):
        try:
            return json.loads(m.get("clobTokenIds") or "[]")
        except Exception:
            return []

    import time as _t
    now = _t.time()
    def _vol(m):
        try:
            return float(m.get("volumeNum") or m.get("volume") or 0)
        except Exception:
            return 0.0
    def _startts(mw, e):
        # реальное время матча = market.gameStartTime; фолбэк — event.startDate.
        for src in (mw.get("gameStartTime"), e.get("startDate")):
            if not src:
                continue
            s = str(src).strip().replace(" ", "T").replace("Z", "+00:00")
            if s.endswith("+00"):           # Py3.9: "+00" -> "+00:00"
                s = s + ":00"
            try:
                return datetime.fromisoformat(s).timestamp()
            except Exception:
                continue
        return None
    def _kyiv_label(ts):
        if ts is None:
            return None
        try:
            if KYIV:
                dt = datetime.fromtimestamp(ts, KYIV)
                today = datetime.now(KYIV).date()
            else:
                dt = datetime.utcfromtimestamp(ts)
                today = datetime.utcnow().date()
            return dt.strftime("%H:%M") if dt.date() == today else dt.strftime("%d.%m %H:%M")
        except Exception:
            return None

    breaks = []      # матчи в перерыве после сета 1 (счёт 1:0)
    watch = []       # ещё не в перерыве, но ликвидные Bo3 — что караулить
    for e in ev:
        title = e.get("title", "") or ""
        if " vs " not in title:
            continue
        low = title.lower()
        if any(k in low for k in ("double", "junior", "winner", "reach", "code", "dress", "longest")):
            continue
        slug = e.get("slug", "") or ""
        mks = e.get("markets", []) or []
        mw = s1 = s2 = None
        has45 = False
        for m in mks:
            q = (m.get("question") or "")
            if q == title:
                mw = m
            elif q.startswith("Set 1 Winner"):
                s1 = m
            elif q.startswith("Set 2 Winner"):
                s2 = m
            elif q.startswith("Set 4 Winner") or q.startswith("Set 5 Winner"):
                has45 = True
        if not (mw and s1 and s2):
            continue                              # нужны все три рынка
        if not _tennis_is_bo3(slug, title, has45):
            continue                              # ← СТРОГО Bo3, Bo5 отсекаем
        s1p, s2p, mwp = _pr(s1), _pr(s2), _pr(mw)
        if len(s1p) < 2 or len(s2p) < 2 or len(mwp) < 2:
            continue
        mo, mwtk = _o(mw), _tk(mw)
        gender = ("M" if slug.lower().startswith("atp-")
                  else ("W" if slug.lower().startswith("wta-") else "?"))
        mvol = _vol(mw)
        if max(s2p) >= 0.90:
            continue                              # сет 2 уже сыгран — вне окна стратегии
        if max(s1p) >= 0.93:
            # ── перерыв 1:0 ──
            s1o = _o(s1)
            s1w = 0 if s1p[0] >= s1p[1] else 1
            sn = (s1o[s1w].split()[-1].lower() if len(s1o) > s1w else "")
            mwi = 0 if (len(mo) >= 2 and sn and sn in mo[0].lower()) else 1
            t1s = round(mwp[mwi] * 100, 1)
            loser = 1 - s1w
            t2m = round(s2p[loser] * 100, 1)
            winner = mo[mwi] if len(mo) > mwi else "?"
            loser_nm = mo[1 - mwi] if len(mo) > (1 - mwi) else "?"
            zone = "green" if t1s <= 65 else ("yellow" if t1s <= 80 else "red")
            f = _tennis_forecast(t1s)
            edge = round(100 - (t1s + (1 - f / 100.0) * t2m), 1)
            depth = _book_depth_usd(mwtk[mwi]) if len(mwtk) > mwi else None  # основной стакан (нога A)
            breaks.append({
                "tour": title.split(":")[0], "winner": winner, "loser": loser_nm, "g": gender,
                "t1s": t1s, "t2m": t2m, "zone": zone, "forecast": f, "edge": edge,
                "depth": depth, "url": "https://polymarket.com/event/" + slug,
            })
        else:
            # ── ещё не в перерыве — в watchlist (сортировка по времени начала) ──
            favi = 0 if mwp[0] >= mwp[1] else 1
            st = _startts(mw, e)
            watch.append({
                "tour": title.split(":")[0],
                "p1": (mo[0] if len(mo) > 0 else "?"), "p2": (mo[1] if len(mo) > 1 else "?"),
                "fav": (mo[favi] if len(mo) > favi else "?"), "fav_px": round(mwp[favi] * 100, 1),
                "vol": round(mvol),
                "hours": (round((st - now) / 3600, 1) if st is not None else None),
                "start": _kyiv_label(st),
                "g": gender, "_st": st,
                "_favtk": (mwtk[favi] if len(mwtk) > favi else None),
                "url": "https://polymarket.com/event/" + slug,
            })
    zrank = {"green": 0, "yellow": 1, "red": 2}
    breaks.sort(key=lambda x: (zrank.get(x["zone"], 3), -(x["depth"] or 0), -x["edge"]))
    # watchlist: сортировка по ЛИКВИДНОСТИ (оборот) — крупные сверху (иначе ITF топит);
    # время старта показываем в каждой строке. Топ-22.
    watch.sort(key=lambda x: -(x.get("vol") or 0))
    watch = watch[:22]
    for w in watch:
        tk = w.pop("_favtk", None)
        w["depth"] = _book_depth_usd(tk) if tk else None
        w.pop("_st", None)
    return {"matches": breaks, "upcoming": watch, "count": len(breaks)}

# ── Bo5 «2 карты» (LoL + мужской теннис-слэм): серия-пойнт 2:1 ──────────────────
# Вход на 2:1 (лидер в 1 карте/сете от победы). Счёт реконструируется по резолву
# Game/Set X Winner рынков. Серия закрыта = match-winner резолвнут (max>=0.985) → скип.
def _bo5_forecast(sport, t1s):
    tbl = ([(60,33),(68,39),(76,55),(84,59),(92,78),(101,90)] if sport=="lol"
           else [(60,41),(68,47),(76,54),(84,65),(92,71),(101,85)])
    for hi, f in tbl:
        if t1s < hi:
            return f
    return tbl[-1][1]

_BO5_CFG = {
    "lol":    {"tag":"league-of-legends", "unit":"Game", "icon":"🎮", "url_pref":"lol"},
    "tennis": {"tag":"tennis",            "unit":"Set",  "icon":"🎾", "url_pref":"tennis"},
}

def _scan_bo5_sport(sport):
    import time as _t
    cfg=_BO5_CFG[sport]; now=_t.time()
    ure=re.compile(cfg["unit"]+r" (\d) Winner")
    # gamma отдаёт максимум 100/запрос; живых событий больше, а сортировка по startDate
    # прячет идущие сейчас матчи (старая дата) → пагинируем и дедупим.
    base=("https://gamma-api.polymarket.com/events?tag_slug=%s&closed=false"
          "&limit=100&order=startDate&ascending=false" % cfg["tag"])
    seen_ev={}
    for off in range(0, 800, 100):
        try:
            page=http_get_json(base + ("&offset=%d" % off)) or []
        except Exception:
            break
        for x in page:
            seen_ev[x.get("id")] = x
        if len(page) < 100:
            break
    ev=list(seen_ev.values())
    def _pr(m):
        try: return [float(x) for x in json.loads(m.get("outcomePrices") or "[]")]
        except Exception: return []
    def _o(m):
        try: return json.loads(m.get("outcomes") or "[]")
        except Exception: return []
    def _tk(m):
        try: return json.loads(m.get("clobTokenIds") or "[]")
        except Exception: return []
    def _kyiv(ts):
        try:
            dt=datetime.fromtimestamp(ts, KYIV) if KYIV else datetime.utcfromtimestamp(ts)
            return dt.strftime("%H:%M")
        except Exception: return None
    def _st(mw,e):
        for s in (mw.get("gameStartTime"), e.get("startDate")):
            if not s: continue
            x=str(s).strip().replace(" ","T").replace("Z","+00:00")
            if x.endswith("+00"): x+=":00"
            try: return datetime.fromisoformat(x).timestamp()
            except Exception: continue
        return None
    series=[]; watch=[]
    for e in ev:
        t=e.get("title","") or ""
        if " vs " not in t: continue
        low=t.lower()
        if any(k in low for k in ("double","junior","winner","reach","code","dress")): continue
        mks=e.get("markets",[]) or []
        mw=None; units={}
        for m in mks:
            q=(m.get("question") or "")
            if q==t: mw=m
            g=ure.search(q)
            if g: units[int(g.group(1))]=m
        if not mw or not units: continue
        is_bo5 = (4 in units) or (5 in units) or ("(BO5)" in t) or ("bo5" in low)
        if not is_bo5: continue
        mwp=_pr(mw); mo=_o(mw); mtk=_tk(mw)
        if len(mwp)<2 or len(mo)<2: continue
        try: mvol=round(float(mw.get("volumeNum") or mw.get("volume") or 0))
        except Exception: mvol=0
        if max(mwp)>=0.985: continue          # серия закрыта
        s0=s1=0; resolved=0
        for i in sorted(units):
            up=_pr(units[i])
            if len(up)<2 or max(up)<0.9: break
            uo=_o(units[i]); win=0 if up[0]>=up[1] else 1
            wn=(uo[win].split()[-1].lower() if uo else "")
            side=0 if (wn and wn in mo[0].lower()) else 1
            if side==0: s0+=1
            else: s1+=1
            resolved+=1
            if max(s0,s1)>=3: break
        lead=max(s0,s1); trail=min(s0,s1)
        if lead>=3: continue
        depth=None
        fav_i = 0 if mwp[0]>=mwp[1] else 1
        if lead==2 and trail in (0,1):
            lidx = 0 if s0>s1 else 1
            t1s=round(mwp[lidx]*100,1)
            nN=resolved+1; t2m=None
            if nN in units:
                np=_pr(units[nN]); no=_o(units[nN])
                if len(np)>=2 and len(no)>=2:
                    ln=mo[1-lidx].split()[-1].lower()
                    ti=0 if ln in no[0].lower() else 1
                    t2m=round(np[ti]*100,1)
            zone="green" if t1s<=65 else ("yellow" if t1s<=80 else "red")
            f=_bo5_forecast(sport,t1s)
            edge=round(100-(t1s+(1-f/100.0)*t2m),1) if t2m is not None else None
            depth=_book_depth_usd(mtk[lidx]) if len(mtk)>lidx else None
            series.append({"sport":sport,"icon":cfg["icon"],"tour":t.split(" (")[0][:44],
                "sc":"%d:%d"%(lead,trail),"leader":mo[lidx],"trailer":mo[1-lidx],
                "t1s":t1s,"t2m":t2m,"zone":zone,"forecast":f,"edge":edge,"depth":depth,"vol":mvol,
                "url":"https://polymarket.com/event/"+(e.get("slug","") or "")})
        else:
            st=_st(mw,e); depthw=_book_depth_usd(mtk[fav_i]) if len(mtk)>fav_i else None
            watch.append({"sport":sport,"icon":cfg["icon"],"tour":t.split(" (")[0][:44],
                "sc":"%d:%d"%(s0,s1),"fav":mo[fav_i],"favpx":round(mwp[fav_i]*100,1),
                "depth":depthw,"vol":mvol,"start":_kyiv(st),"_st":st,
                "url":"https://polymarket.com/event/"+(e.get("slug","") or "")})
    return series, watch

def scan_bo5():
    ser=[]; watch=[]
    for sp in ("lol","tennis"):
        s,w=_scan_bo5_sport(sp); ser+=s; watch+=w
    zr={"green":0,"yellow":1,"red":2}
    ser.sort(key=lambda x:(zr.get(x["zone"],3), -(x["depth"] or 0)))
    watch.sort(key=lambda x:(x["_st"] if x["_st"] is not None else 9e18))
    for w in watch: w.pop("_st",None)
    return {"series":ser, "watch":watch[:30], "count":len(ser)}

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
#voicebox{background:#0e1016;border:1px solid #222633;border-radius:9px;padding:9px;margin-bottom:10px;max-height:300px;overflow:auto}
.vlh{font-size:11px;color:#7FDDBB;font-weight:700;margin-bottom:6px}
.vpitch{font-size:11px;color:#9fb6c8;margin-bottom:8px;display:flex;align-items:center;gap:6px}
.vpitch input{flex:1}
.vrow{display:flex;align-items:center;gap:6px;padding:4px 5px;border-radius:6px;font-size:12px;color:#cfd8e3}
.vrow.sel{background:#0d2a1e;border:1px solid #1D9E7566}
.vrow .vn{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.vrow .vn i{color:#667;font-style:normal;font-size:10px}
.vrow button{background:#1a1d27;color:#9fb6c8;border:1px solid #2a2d3a;border-radius:5px;padding:2px 8px;cursor:pointer;font-size:11px}
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
.ac{display:none;background:#1a0a0a;border:1px solid #E24B4A66;border-radius:10px;padding:10px 12px;margin-bottom:8px;box-shadow:0 0 12px #E24B4A18;max-width:480px}
.ac.show{display:block}
.ac .bd{font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:#ff8585;margin-bottom:5px;font-weight:600}
.ac .gm{display:inline-block;font-size:10px;background:#3a3d4a;color:#bbb;padding:2px 7px;border-radius:5px;margin-left:7px}
.at{font-size:15px;font-weight:600;color:#fff;margin-bottom:2px}
.am2{font-size:11px;color:#888;margin-bottom:6px}
.wb{background:#0d2a1e;border:1px solid #1D9E7588;border-radius:7px;padding:6px 10px;margin-bottom:6px}
.wb .wl{font-size:9px;color:#7FE3C2;text-transform:uppercase;letter-spacing:.08em;margin-bottom:1px}
.wb .wn{font-size:13px;font-weight:600;color:#B8F0DD}
.legs{display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-bottom:6px}
.leg{background:#161a26;border-radius:7px;padding:5px 9px}
.leg .ll{font-size:9px;color:#777;margin-bottom:1px;text-transform:uppercase;letter-spacing:.06em}
.leg .lv{font-size:12px;font-weight:500;color:#e0e0e0}
.vb{border-radius:7px;padding:6px 10px;margin-bottom:5px}
.vb .vl{font-size:9px;color:#888;text-transform:uppercase;letter-spacing:.08em;margin-bottom:1px}
.vb .vv{font-size:13px;font-weight:600}
.vb .vt{font-size:10px;color:#777;margin-top:2px;word-break:break-word}
.hint{font-size:10px;color:#aaa;background:#161a26;border-radius:6px;padding:5px 9px;line-height:1.4;margin-bottom:5px}
.dis{width:100%;background:#12151f;color:#555;border:1px solid #2a2d3a;border-radius:7px;padding:7px;font-size:12px;cursor:pointer}
.pmlink{display:block;text-align:center;background:#13212e;color:#5BA3E0;border:1px solid #2a4a63;border-radius:6px;padding:7px;font-size:11px;text-decoration:none;margin-bottom:6px;font-weight:500}
.pmlink:active{opacity:.7}
.achead{display:flex;justify-content:space-between;align-items:center;margin-bottom:5px}
.acx{background:none;border:none;color:#555;font-size:16px;cursor:pointer;padding:0 4px;line-height:1;width:auto;flex:none}
#alerts{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,480px));gap:8px;margin-bottom:12px}
.tabs{display:flex;gap:6px;margin-bottom:14px}
.tabbtn{flex:1;text-align:center;background:#12151f;border:1px solid #2a2d3a;border-radius:9px;padding:10px;font-size:14px;font-weight:600;color:#888;cursor:pointer;user-select:none;transition:.15s}
.tabbtn.on{background:#13212e;border-color:#5BA3E0;color:#9fcdf0}
.wxrow{background:#1a1d27;border:1px solid #2a2d3a;border-radius:11px;padding:11px 13px;margin-bottom:8px}
.wxhead{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px}
.wxcity{font-size:14px;font-weight:600;color:#fff}
.wxdate{font-size:11px;color:#666}
.wxreg{font-size:10px;color:#7a8694;background:#161922;border:1px solid #2a2d3a;padding:2px 7px;border-radius:5px}
.wxsig{font-size:11px;font-weight:700;padding:3px 9px;border-radius:6px;margin-left:auto}
.wxhrs{font-size:11px;color:#888;background:#12151f;border:1px solid #2a2d3a;padding:3px 8px;border-radius:6px}
.wxfav{background:#0d2a1e;border:1px solid #1D9E7588;border-radius:8px;padding:7px 11px;margin-bottom:7px;display:flex;align-items:center;gap:10px}
.wxfav .fl{font-size:9px;color:#7FE3C2;text-transform:uppercase;letter-spacing:.07em}
.wxfav .fb{font-size:16px;font-weight:700;color:#B8F0DD}
.wxfav .fp{margin-left:auto;font-size:14px;font-weight:700;color:#7FDDBB;font-variant-numeric:tabular-nums}
.wxzone{font-size:11px;border-radius:7px;padding:6px 10px;margin-bottom:7px;line-height:1.4}
.wxzone.ok{color:#9be8c6;background:#0d2a1e;border:1px solid #1D9E7566}
.wxzone.no{color:#8893a0;background:#181a22;border:1px solid #2a2d3a}
.wxbuy{display:flex;align-items:center;gap:7px;font-size:12px;color:#9fb6c8;margin-top:8px;cursor:pointer;user-select:none}
.wxbuy input{width:16px;height:16px;cursor:pointer;accent-color:#1D9E75}
.wxbts{font-size:10px;color:#7FDDBB;margin-top:3px;min-height:11px}
.wxrow.bought{opacity:.6}
.wxcols{display:flex;gap:12px;align-items:flex-start}
.wxmain{flex:1;min-width:0}
.wxside{width:236px;flex:none;background:#0e1016;border:1px solid #222633;border-radius:10px;padding:8px}
.wxside-h{font-size:11px;font-weight:700;color:#7FDDBB;text-transform:uppercase;letter-spacing:.06em;padding:2px 4px 8px}
.wxside .wxempty2{font-size:11px;color:#555;padding:4px}
/* компактний вид куплених у вузькій колонці */
.wxside .wxrow{padding:8px 9px;margin-bottom:7px}
.wxside .wxreg,.wxside .wxhrs,.wxside .wxsig,.wxside .wxzone,.wxside .wxlink{display:none}
.wxside .wxfav{padding:5px 8px;margin-bottom:6px}
.wxside .wxfav .fb{font-size:13px}
.wxside .wxhead{gap:6px}
.wxbuys{display:flex;flex-wrap:wrap;gap:12px;margin-top:8px}
.wxside .wxbuys{gap:8px}
.wxbuy{display:flex;align-items:center;gap:6px;font-size:12px;color:#9fb6c8;cursor:pointer;user-select:none}
.wxbuy input{width:16px;height:16px;cursor:pointer;accent-color:#1D9E75}
.wxbuy.lim input{accent-color:#D9A441}
.wxbts{font-size:10px;color:#7FDDBB;margin-top:4px;min-height:11px}
.wxlink{display:block;text-align:center;background:#13212e;color:#5BA3E0;border:1px solid #2a4a63;border-radius:6px;padding:6px;font-size:11px;text-decoration:none;font-weight:500}
.wxlink:active{opacity:.7}
.wxempty{font-size:12px;color:#444;font-style:italic}
.big-vol{font-size:14px !important;font-weight:800 !important;padding:3px 11px !important}
.big-time{font-size:13px !important;font-weight:700 !important;color:#e8e8e8 !important;border-color:#3a4152 !important}
.markchk{width:15px;height:15px;cursor:pointer;accent-color:#D9A441;flex:none}
.urow.marked,.pill2.marked{border-color:#D9A441 !important;background:rgba(217,164,65,.10) !important;box-shadow:inset 3px 0 0 #D9A441}
.tabs{display:flex;gap:8px;margin:0 0 16px}
.tnhead{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin:4px 0 14px}
.tnhead h2{margin:0;font-size:16px}
#tnstatus{font-size:12px;color:#8aa0b0}
.tnhead button{background:#13212e;border:1px solid #2a3d4a;color:#9fcdf0;border-radius:7px;padding:6px 12px;cursor:pointer;font-size:13px}
.tnrow{background:#0e1620;border:1px solid #1e2a38;border-radius:11px;padding:12px 14px;margin-bottom:10px}
.tnr1{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:6px}
.tntour{font-weight:700;font-size:13px;color:#cdd8e5}
.tnzone{font-size:11px;font-weight:700;letter-spacing:.05em;padding:2px 8px;border-radius:20px}
.tnlink{margin-left:auto;font-size:12px;color:#5BA3E0;text-decoration:none}
.tnr2{font-size:13px;color:#b9c6d4;margin-bottom:6px}
.tnr3{display:flex;gap:18px;flex-wrap:wrap;font-size:12px;color:#8fa0b2;margin-bottom:6px}
.tnr4{display:flex;gap:18px;flex-wrap:wrap;font-size:13px;font-weight:600}
</style></head><body>
<h1><div class="dot" id="dot"></div> Bo3 Monitor</h1>

<div class="tabs">
  <div class="tabbtn on" data-tab="esports" onclick="switchTab(this)">🎮 Киберспорт</div>
  <div class="tabbtn" data-tab="tennis" onclick="switchTab(this)">🎾 Теннис (Bo3)</div>
  <div class="tabbtn" data-tab="bo5" onclick="switchTab(this)">🏆 Bo5 · 2:1</div>
</div>

<div id="tab-esports">

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
<button class="test" onclick="toggleVoices()" title="Вибір голосу">🎙</button>
</div>
<div id="voicebox" style="display:none"></div>

<div class="logw"><div class="logh">Лог</div><div class="logb" id="log"></div></div>

</div><!-- /tab-esports -->

<div id="tab-tennis" style="display:none">
  <div class="tnhead">
    <h2>🎾 Теннис Bo3 · перерыв после сета 1 (счёт 1:0)</h2>
    <span id="tnstatus">—</span>
    <button onclick="loadTennis()">↻ Обновить</button>
  </div>
  <div id="tnlist"><span class="nolive">—</span></div>
  <h2 style="font-size:15px;margin:20px 0 10px">⏳ Скоро / в игре · ликвидные Bo3 <span style="font-size:12px;color:#8fa0b2;font-weight:400">(глубина основного стакана · время киевское)</span></h2>
  <div id="tnwatch"><span class="nolive">—</span></div>
</div>

<div id="tab-bo5" style="display:none">
  <div class="tnhead">
    <h2>🏆 Bo5 · серия-пойнт 2:1</h2>
    <span id="b5status">—</span>
    <button onclick="loadBo5()">↻ Обновить</button>
  </div>
  <h2 style="font-size:15px;color:#c8aa6e;margin:6px 0 8px">🎮 LoL Bo5 · на 2:1</h2>
  <div id="b5_lol_ser"><span class="nolive">—</span></div>
  <div style="font-size:12px;color:#8fa0b2;margin:10px 0 6px">живые LoL Bo5 (караулить до 2:1 · время киевское):</div>
  <div id="b5_lol_watch"><span class="nolive">—</span></div>
  <h2 style="font-size:15px;color:#5bd6ff;margin:24px 0 8px">🎾 Теннис Bo5 · на 2:1 <span style="font-size:12px;color:#8fa0b2;font-weight:400">(муж. слэм)</span></h2>
  <div id="b5_ten_ser"><span class="nolive">—</span></div>
  <div style="font-size:12px;color:#8fa0b2;margin:10px 0 6px">живые теннис Bo5 (караулить до 2:1 · время киевское):</div>
  <div id="b5_ten_watch"><span class="nolive">—</span></div>
</div>


<script>
let lastAlertId=0, lastLogLen=0, running=false;

let actx=null;
function ac(){try{if(!actx)actx=new(window.AudioContext||window.webkitAudioContext)();if(actx.state==='suspended')actx.resume();}catch(e){}return actx;}
function beep(){try{const c=ac();if(!c)return;[[880,0],[660,.2],[880,.4]].forEach(([freq,t])=>{const o=c.createOscillator(),g=c.createGain();o.connect(g);g.connect(c.destination);o.frequency.value=freq;o.type='sine';g.gain.setValueAtTime(1.0,c.currentTime+t);g.gain.exponentialRampToValueAtTime(.001,c.currentTime+t+.2);o.start(c.currentTime+t);o.stop(c.currentTime+t+.21)})}catch(e){}}
function tones(seq,gain){try{const c=ac();if(!c)return;seq.forEach(([f,t])=>{const o=c.createOscillator(),g=c.createGain();o.connect(g);g.connect(c.destination);o.frequency.value=f;o.type='sine';g.gain.setValueAtTime(gain,c.currentTime+t);g.gain.exponentialRampToValueAtTime(.001,c.currentTime+t+.22);o.start(c.currentTime+t);o.stop(c.currentTime+t+.23)})}catch(e){}}
function unlockAudio(){ac();try{if(window.speechSynthesis){speechSynthesis.resume();const u=new SpeechSynthesisUtterance(' ');u.volume=0;speechSynthesis.speak(u);}}catch(e){}}
document.addEventListener('pointerdown',unlockAudio);
let _voices=[];
function loadVoices(){try{_voices=(window.speechSynthesis&&speechSynthesis.getVoices())||[];}catch(e){}}
if(window.speechSynthesis){loadVoices();try{speechSynthesis.onvoiceschanged=loadVoices;}catch(e){}}
function pickFemaleVoice(){
  if(!_voices.length)loadVoices();
  const en=_voices.filter(v=>/^en/i.test(v.lang));
  const pref=['samantha','google us english','zira','karen','moira','tessa','victoria','fiona','serena','aria','jenny','michelle','female'];
  for(const name of pref){const v=en.find(v=>v.name.toLowerCase().includes(name));if(v)return v;}
  return en[0]||_voices[0]||null;
}
function chosenVoice(){
  let pref=null; try{pref=localStorage.getItem('voicePref');}catch(e){}
  if(pref){const v=_voices.find(v=>v.name===pref); if(v)return v;}
  return pickFemaleVoice();
}
function curPitch(){let p=1.15;try{const s=localStorage.getItem('voicePitch');if(s)p=parseFloat(s);}catch(e){}return p;}
function speak(txt,vol){try{if(!window.speechSynthesis)return;speechSynthesis.resume();const u=new SpeechSynthesisUtterance(txt);u.lang='en-US';const v=chosenVoice();if(v){u.voice=v;u.lang=v.lang;}u.volume=(vol||.55);u.rate=.9;u.pitch=curPitch();speechSynthesis.cancel();setTimeout(()=>{try{speechSynthesis.resume();speechSynthesis.speak(u);}catch(e){}},60);}catch(e){}}
function toggleVoices(){const b=document.getElementById('voicebox');if(!b)return;if(b.style.display==='none'){b.style.display='block';renderVoices();}else{b.style.display='none';}}
function renderVoices(){
  loadVoices();
  const b=document.getElementById('voicebox'); if(!b)return;
  let pref=''; try{pref=localStorage.getItem('voicePref')||'';}catch(e){}
  const pitch=curPitch();
  const en=_voices.filter(v=>/^en/i.test(v.lang));
  const list=en.length?en:_voices;
  let html='<div class="vlh">Голоси браузера ('+list.length+') — ▶ послухати, ✓ вибрати</div>';
  html+='<div class="vpitch">🎈 мультяшність: <input type="range" min="1" max="2" step="0.05" value="'+pitch+'" oninput="document.getElementById(\'vpv\').textContent=(+this.value).toFixed(2)" onchange="setPitch(this.value)"> <span id="vpv">'+pitch.toFixed(2)+'</span></div>';
  if(!list.length){html+='<div style="font-size:11px;color:#888">Голоси ще не завантажились — закрий і відкрий ще раз.</div>';}
  list.forEach(v=>{
    const nm=v.name.replace(/'/g,"\\'");
    const sel=v.name===pref?' sel':'';
    html+='<div class="vrow'+sel+'"><span class="vn">'+escapeHtml(v.name)+' <i>'+escapeHtml(v.lang)+'</i></span>'
        +'<button onclick="previewVoice(\''+nm+'\')">▶</button>'
        +'<button onclick="setVoice(\''+nm+'\')">'+(v.name===pref?'✓':'вибрати')+'</button></div>';
  });
  b.innerHTML=html;
}
function setPitch(val){try{localStorage.setItem('voicePitch',val);}catch(e){}previewVoice(null);}
function setVoice(name){try{localStorage.setItem('voicePref',name);}catch(e){}renderVoices();previewVoice(name);}
function previewVoice(name){try{if(!window.speechSynthesis)return;speechSynthesis.resume();const u=new SpeechSynthesisUtterance('Map one end');const v=name?_voices.find(x=>x.name===name):chosenVoice();if(v){u.voice=v;u.lang=v.lang;}u.volume=.6;u.rate=.9;u.pitch=curPitch();speechSynthesis.cancel();setTimeout(()=>{try{speechSynthesis.resume();speechSynthesis.speak(u);}catch(e){}},60);}catch(e){}}
function playPhase(p,marked){
  if(marked){
    // помеченный матч — отдельный, более громкий сигнал
    if(p==='m1start'){tones([[700,0],[900,.12],[1100,.24]],1.0);speak('Marked. Map one start',1.0);}
    else if(p==='m1end'){tones([[1150,0],[880,.16],[1150,.32],[880,.48],[1150,.64]],1.0);speak('Marked match. Map one end',1.0);}
    else if(p==='m2end'){tones([[950,0],[720,.2],[950,.4]],.95);speak('Marked. Map two end',1.0);}
    return;
  }
  if(p==='m1start'){tones([[520,0],[680,.18]],.26);speak('Map one start');}
  else if(p==='m1end'){tones([[820,0],[640,.2],[820,.4]],.5);speak('Map one end');}
  else if(p==='m2end'){tones([[700,0],[520,.2],[420,.4]],.36);speak('Map two end');}
}
let playedSounds=[], soundsSeeded=false;
function processSounds(arr){
  if(!arr)return;
  if(!soundsSeeded){ playedSounds=arr.map(s=>s.id); soundsSeeded=true; return; } // не «взрывать» при загрузке
  arr.forEach(s=>{
    if(!s||playedSounds.includes(s.id))return;
    playedSounds.push(s.id); if(playedSounds.length>40)playedSounds=playedSounds.slice(-40);
    let marked=false; try{marked=!!localStorage.getItem('mark:'+s.game+'|'+s.t1+'|'+s.t2);}catch(_){}
    playPhase(s.phase,marked); dot('alert');
  });
}

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
      ?`<span class="uvol big-vol" style="color:${m.vol_color};border-color:${m.vol_color}55">${m.vol_text}</span>`
      :`<span class="uvol big-vol" style="color:#888;border-color:#444">нет рынка</span>`;
    const roi=m.roi_hint?`<span class="uvol" style="color:#888;border-color:#333;font-weight:400">${m.roi_hint}</span>`:'';
    const link=m.pm_url?`<a class="ulink" href="${m.pm_url}" target="_blank">↗</a>`:'';
    const diffStr=m.diff_h<1?`${Math.round(m.diff_h*60)}мин`:`${m.diff_h.toFixed(1)}ч`;
    const key='mark:'+m.game+'|'+m.t1+'|'+m.t2;
    return `<div class="urow" data-mk="${escapeHtml(key)}">
      <input type="checkbox" class="markchk" title="Пометить, чтобы не пропустить">
      <span class="ut">${escapeHtml(m.game)}</span>
      <span class="unm">${escapeHtml(m.t1)} vs ${escapeHtml(m.t2)}</span>
      <span class="utime big-time">⏰ ${m.kyiv_time} (через ${diffStr})</span>
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
      ? `<span class="tag-vol big-vol" style="color:${m.vol_color};border-color:${m.vol_color}55">${m.vol_text}</span>`
      : `<span class="tag-vol big-vol" style="color:#888;border-color:#444">нет рынка</span>`;
    const roi = m.roi_hint
      ? `<span class="tag-vol" style="color:#888;border-color:#333;font-weight:400">${m.roi_hint}</span>`
      : '';
    const time = m.start_kyiv
      ? `<span class="tag-vol big-time" style="border-color:#3a4152">⏰ ${m.start_kyiv}</span>`
      : '';
    const link = m.pm_url
      ? `<a class="tag-link" href="${m.pm_url}" target="_blank" title="Открыть на Polymarket">↗</a>`
      : '';
    const key='mark:'+m.game+'|'+m.t1+'|'+m.t2;
    return `<div class="pill2" data-mk="${escapeHtml(key)}">
      <input type="checkbox" class="markchk" title="Пометить, чтобы не пропустить">
      <span class="g">${m.game}</span>
      <span class="nm">${escapeHtml(m.t1)} vs ${escapeHtml(m.t2)}</span>
      <span class="sc">${m.s1}:${m.s2}</span>
      ${time}${win}${vol}${roi}${link}
    </div>`;
  }).join('');
}

// Помеченные матчи (localStorage) — восстановить чекбоксы и подсветку после каждого ререндера
function restoreMarks(){
  document.querySelectorAll('[data-mk]').forEach(row=>{
    const key=row.getAttribute('data-mk');
    const chk=row.querySelector('.markchk');
    if(!chk)return;
    let on=false; try{on=!!localStorage.getItem(key);}catch(_){}
    chk.checked=on; row.classList.toggle('marked',on);
    chk.onchange=function(){
      try{ if(chk.checked)localStorage.setItem(key,'1'); else localStorage.removeItem(key); }catch(_){}
      row.classList.toggle('marked',chk.checked);
    };
  });
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
  return `<div class="ac show" data-id="${a.id}">
    <div class="achead">
      <div class="bd">🚨 Карта 1 завершена${fin}<span class="gm">${escapeHtml(a.game)}</span></div>
      <button class="acx" onclick="closeCard(${a.id})">✕</button>
    </div>
    <div class="at">${escapeHtml(a.t1)} vs ${escapeHtml(a.t2)}</div>
    ${mapScore}
    <div class="wb"><div class="wl">Взял К1</div><div class="wn">✅ ${escapeHtml(a.winner)}</div></div>
    <div class="vb" style="border:1px solid ${a.vol_color}66;background:${a.vol_color}1a">
      <div class="vl">Об'єм Polymarket</div>
      <div class="vv" style="color:${a.vol_color}">${escapeHtml(a.vol_text)}</div>
      <div class="vt">${escapeHtml(a.pm_title)||'Ринок не знайдено'}</div>
    </div>
    ${pmLink}
  </div>`;
}

function showAlert(a){
  if(!a||shownIds.includes(a.id))return;
  shownIds.push(a.id);
  if(shownIds.length>MAX_ALERTS)shownIds=shownIds.slice(-MAX_ALERTS);
  dot('alert');

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
    renderLog(s.log);renderLive(s.live);renderUpcoming(s.upcoming||[]);restoreMarks();
    if(s.alerts)s.alerts.forEach(showAlert);
    processSounds(s.sounds);
  }catch(e){}
}

function start(){
  if(Notification.permission==='default')Notification.requestPermission();
  fetch('/start',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({games:getGames(),interval:parseInt(document.getElementById('iv').value)||20,min_tier:document.getElementById('tier').value})});
}
function stop(){fetch('/stop',{method:'POST'})}
function test(){playPhase('m1start');setTimeout(()=>playPhase('m1end'),1700);setTimeout(()=>playPhase('m2end'),3600);const t=new Date().toLocaleTimeString('uk-UA',{hour:'2-digit',minute:'2-digit',timeZone:'Europe/Kyiv'});showAlert({id:Date.now(),game:'CS2',t1:'Team Spirit',t2:'NAVI',score:'1:0',winner:'Team Spirit',loser:'NAVI',vol_text:'🟢 БОЛЬШОЙ  $120,000',vol_color:'#1D9E75',pm_title:'Counter-Strike: Spirit vs NAVI (BO3) - IEM Cologne',pm_url:'',finished_at:t,map_score:'16:12'})}

function switchTab(el){
  document.querySelectorAll('.tabbtn').forEach(b=>b.classList.remove('on'));
  el.classList.add('on');
  const t=el.dataset.tab;
  document.getElementById('tab-esports').style.display = t==='esports'?'':'none';
  const tn=document.getElementById('tab-tennis'); if(tn) tn.style.display = t==='tennis'?'':'none';
  const b5=document.getElementById('tab-bo5'); if(b5) b5.style.display = t==='bo5'?'':'none';
  if(t==='tennis'){ loadTennis(); if(!tnTimer) tnTimer=setInterval(loadTennis,45000); }
  else if(tnTimer){ clearInterval(tnTimer); tnTimer=null; }
  if(t==='bo5'){ loadBo5(); if(!b5Timer) b5Timer=setInterval(loadBo5,45000); }
  else if(b5Timer){ clearInterval(b5Timer); b5Timer=null; }
}
let tnTimer=null, b5Timer=null;
async function loadBo5(){
  const st=document.getElementById('b5status'); if(st)st.textContent='загрузка…';
  try{
    const r=await fetch('/bo5'); const d=await r.json();
    renderBo5(d.series||[]); renderBo5watch(d.watch||[]);
    const tm=new Date().toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'});
    if(st)st.textContent=(d.series?d.series.length:0)+' на 2:1 · '+((d.watch||[]).length)+' живых · '+tm+(d.error?(' · ошибка: '+d.error):'');
  }catch(e){ if(st)st.textContent='ошибка загрузки'; }
}
function renderBo5(ms){
  const zc={green:'#34d399',yellow:'#e8b84a',red:'#f0556b'}, zt={green:'ЗЕЛЁНАЯ',yellow:'ЖЁЛТАЯ',red:'КРАСНАЯ'};
  const wl=s=>escapeHtml((String(s||'').split(' ').pop())||String(s||''));
  function row(m){
    const z=zc[m.zone]||'#888';
    const ec=(m.edge==null)?'#8fa0b2':(m.edge>=10?'#34d399':(m.edge>=0?'#e8b84a':'#f0556b'));
    const nb=(m.t2m==null)?'—':(m.t2m+'¢');
    const ed=(m.edge==null)?'—':((m.edge>=0?'+':'')+m.edge+'¢');
    return '<div class="tnrow" style="border-left:4px solid '+z+'">'
      +'<div class="tnr1"><span class="tntour">'+escapeHtml(m.tour)+' · <b style="color:'+z+'">'+m.sc+'</b></span>'
      +'<span class="tnzone" style="color:'+z+';border:1px solid '+z+'66">'+(zt[m.zone]||'')+' '+m.t1s+'¢</span>'
      +_links(m.leader,m.trailer,m.url)+'</div>'
      +'<div class="tnr2"><b>'+escapeHtml(m.leader)+'</b> ведёт '+m.sc+' · трейлер <b>'+escapeHtml(m.trailer)+'</b></div>'
      +'<div class="tnr3"><span>Нога A: серия '+wl(m.leader)+' по <b>'+m.t1s+'¢</b> → при 2:2 продать ≈'+m.forecast+'¢</span>'
      +'<span>Нога B: '+wl(m.trailer)+' берёт след. ед. по <b>'+nb+'</b></span></div>'
      +'<div class="tnr4"><span style="color:'+ec+'">EDGE ≈ '+ed+'</span>'+_volSpan(m.vol)+_liqSpan(m.depth)+'</div>'
      +'</div>';
  }
  const lb=document.getElementById('b5_lol_ser'), tb=document.getElementById('b5_ten_ser');
  const lol=ms.filter(m=>m.sport==='lol'), ten=ms.filter(m=>m.sport==='tennis');
  if(lb) lb.innerHTML = lol.length? lol.map(row).join('') : '<span class="nolive">Нет LoL на 2:1 сейчас</span>';
  if(tb) tb.innerHTML = ten.length? ten.map(row).join('') : '<span class="nolive">Нет тенниса на 2:1 сейчас</span>';
}
function renderBo5watch(ms){
  function row(m){
    const hrs=m.start?('старт '+m.start):'';
    return '<div class="tnrow">'
      +'<div class="tnr1"><span class="tntour">'+escapeHtml(m.tour)+' · <b>'+m.sc+'</b></span>'
      +(hrs?'<span style="font-size:12px;color:#8fa0b2">'+hrs+'</span>':'')
      +_links(m.fav,'',m.url)+'</div>'
      +'<div class="tnr2">фаворит <b>'+escapeHtml(m.fav)+'</b> '+m.favpx+'¢</div>'
      +'<div class="tnr4">'+_volSpan(m.vol)+_liqSpan(m.depth)+'</div>'
      +'</div>';
  }
  const lb=document.getElementById('b5_lol_watch'), tb=document.getElementById('b5_ten_watch');
  const lol=ms.filter(m=>m.sport==='lol'), ten=ms.filter(m=>m.sport==='tennis');
  if(lb) lb.innerHTML = lol.length? lol.map(row).join('') : '<span class="nolive">Нет живых LoL Bo5</span>';
  if(tb) tb.innerHTML = ten.length? ten.map(row).join('') : '<span class="nolive">Нет живых теннис Bo5</span>';
}
function _dep(v){return v==null?'—':('$'+(v>=1000?(v/1000).toFixed(1)+'k':v));}
function _volk(v){v=v||0;return '$'+(v>=1000?(v/1000).toFixed(0)+'k':v);}
function _volSpan(v){return '<span style="color:#8fa0b2">оборот '+_volk(v)+'</span>';}
function _liqSpan(v){var ok=(v||0)>=2000;return '<span style="color:'+(v==null?'#8fa0b2':(ok?'#34d399':'#f0556b'))+'">стакан матча: '+_dep(v)+(v==null?'':(ok?' ✓ ликвидно':' ⚠ тонко'))+'</span>';}
function _links(a,b,url){
  var q=encodeURIComponent((a||'')+' '+(b||''));
  return '<span style="margin-left:auto;display:inline-flex;gap:12px;flex-wrap:wrap">'
    +'<a class="tnlink" style="margin:0;color:#e8b84a" href="https://www.sofascore.com/search?q='+q+'" target="_blank">📊 SofaScore</a>'
    +'<a class="tnlink" style="margin:0;color:#e8b84a" href="https://www.flashscore.com/search/?q='+q+'" target="_blank">Flashscore</a>'
    +'<a class="tnlink" style="margin:0" href="'+url+'" target="_blank">↗ Polymarket</a></span>';
}
async function loadTennis(){
  const st=document.getElementById('tnstatus'); if(st)st.textContent='загрузка…';
  try{
    const r=await fetch('/tennis'); const d=await r.json();
    renderBreaks(d.matches||[]); renderWatch(d.upcoming||[]);
    const tm=new Date().toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'});
    if(st)st.textContent=(d.matches?d.matches.length:0)+' в перерыве 1:0 · '+((d.upcoming||[]).length)+' на карауле · '+tm+(d.error?(' · ошибка: '+d.error):'');
  }catch(e){ if(st)st.textContent='ошибка загрузки'; }
}
function renderBreaks(ms){
  const box=document.getElementById('tnlist'); if(!box)return;
  if(!ms.length){box.innerHTML='<span class="nolive">Нет матчей в перерыве после сета 1 (Bo3)</span>';return;}
  const zc={green:'#34d399',yellow:'#e8b84a',red:'#f0556b'}, zt={green:'ЗЕЛЁНАЯ',yellow:'ЖЁЛТАЯ',red:'КРАСНАЯ'};
  const wl=s=>escapeHtml((String(s||'').split(' ').pop())||String(s||''));
  box.innerHTML=ms.map(m=>{
    const z=zc[m.zone]||'#888';
    const g=m.g==='M'?'<span style="color:#4ab3f4">♂</span>':(m.g==='W'?'<span style="color:#f06ba0">♀</span>':'');
    const ec=m.edge>=10?'#34d399':(m.edge>=0?'#e8b84a':'#f0556b');
    return '<div class="tnrow" style="border-left:4px solid '+z+'">'
      +'<div class="tnr1"><span class="tntour">'+escapeHtml(m.tour)+' '+g+'</span>'
      +'<span class="tnzone" style="color:'+z+';border:1px solid '+z+'66">'+(zt[m.zone]||'')+' '+m.t1s+'¢</span>'
      +_links(m.winner,m.loser,m.url)+'</div>'
      +'<div class="tnr2"><b>'+escapeHtml(m.winner)+'</b> взял сет 1 · соперник <b>'+escapeHtml(m.loser)+'</b></div>'
      +'<div class="tnr3"><span>Нога A: матч '+wl(m.winner)+' по <b>'+m.t1s+'¢</b> → при 1:1 продать ≈'+m.forecast+'¢</span>'
      +'<span>Нога B: Set 2 '+wl(m.loser)+' по <b>'+m.t2m+'¢</b></span></div>'
      +'<div class="tnr4"><span style="color:'+ec+'">EDGE ≈ '+(m.edge>=0?'+':'')+m.edge+'¢</span>'+_liqSpan(m.depth)+'</div>'
      +'</div>';
  }).join('');
}
function renderWatch(ms){
  const box=document.getElementById('tnwatch'); if(!box)return;
  if(!ms.length){box.innerHTML='<span class="nolive">Нет ликвидных Bo3 в ближайшем окне</span>';return;}
  box.innerHTML=ms.map(m=>{
    const g=m.g==='M'?'<span style="color:#4ab3f4">♂</span>':(m.g==='W'?'<span style="color:#f06ba0">♀</span>':'');
    const volk='$'+(m.vol>=1000?(m.vol/1000).toFixed(0)+'k':m.vol);
    const inplay=(m.hours!=null&&m.hours<0);
    const hrs=(m.start==null)?(inplay?'в игре':''):(inplay?('в игре · с '+m.start):('старт '+m.start));
    return '<div class="tnrow">'
      +'<div class="tnr1"><span class="tntour">'+escapeHtml(m.tour)+' '+g+'</span>'
      +(hrs?'<span style="font-size:12px;color:#8fa0b2">'+hrs+'</span>':'')
      +_links(m.p1,m.p2,m.url)+'</div>'
      +'<div class="tnr2">'+escapeHtml(m.p1)+' vs '+escapeHtml(m.p2)+' · фаворит <b>'+escapeHtml(m.fav)+'</b> '+m.fav_px+'¢</div>'
      +'<div class="tnr4"><span style="color:#8fa0b2">оборот '+volk+'</span>'+_liqSpan(m.depth)+'</div>'
      +'</div>';
  }).join('');
}
let wxLoaded=false, wxTimer=null;
async function loadWx(){
  const st=document.getElementById('wxstatus');st.textContent='завантаження…';
  try{
    const r=await fetch('/weather');const d=await r.json();
    renderWx(d.events||[]);
    const t=new Date().toLocaleTimeString('uk-UA',{hour:'2-digit',minute:'2-digit'});
    st.textContent=(d.events?d.events.length:0)+' ринків · оновлено '+t+(d.error?(' · помилка: '+d.error):'');
    wxLoaded=true;
  }catch(e){st.textContent='помилка завантаження'}
}
function renderWx(evs){
  const box=document.getElementById('wxlist');
  if(!evs.length){box.innerHTML='<span class="wxempty">Немає відкритих погодних ринків</span>';return}
  box.innerHTML=evs.map(e=>{
    const hrs = e.hours==null?'—':(e.hours<0?'завершено':e.hours+'г');
    const volk = e.vol>=1000 ? (e.vol/1000).toFixed(0)+'k' : e.vol;
    const wxid = e.url || (e.city+'|'+e.date+'|'+e.fav_lab);
    const sig = e.in_zone
      ? `<div class="wxzone ok">✓ дешевий фаворит ${e.fav_px}¢ + тонкий ринок ($${volk}) — обидва сигнали</div>`
      : `<div class="wxzone no">фаворит ${e.fav_px}¢ · обсяг $${volk} ${e.low_vol?'(тонкий ✓)':'(товстий)'} ${e.fav_px<50?'· ціна ✓':'· фав >50¢'}</div>`;
    const link = e.url ? `<a class="wxlink" href="${e.url}" target="_blank">↗ Відкрити на Polymarket</a>` : '';
    return `<div class="wxrow" data-wxid="${escapeHtml(wxid)}"${e.in_zone?' style="border-color:#1D9E7566"':''}>
      <div class="wxhead">
        <span class="wxcity">${escapeHtml(e.city)}</span>
        <span class="wxdate">${escapeHtml(e.date)}</span>
        <span class="wxreg">${escapeHtml(e.region)}${e.is_low?' · Lowest':' · Highest'}</span>
        <span class="wxhrs">⏳ ${hrs} до кінця</span>
        <span class="wxsig" style="color:${e.sig_color};background:${e.sig_color}1a;border:1px solid ${e.sig_color}55">${escapeHtml(e.signal)}</span>
      </div>
      <div class="wxfav">
        <div><div class="fl">Купити фаворита</div><div class="fb">${escapeHtml(e.fav_lab)}</div></div>
        <span class="fp">${e.fav_px}¢</span>
      </div>
      ${sig}${link}
      <div class="wxbuys">
        <label class="wxbuy lim"><input type="checkbox" class="wxlimchk"> Лімітка</label>
        <label class="wxbuy"><input type="checkbox" class="wxbuychk"> Куплено</label>
      </div>
      <div class="wxbts"></div>
    </div>`;
  }).join('');
  wxRestore();
}

function wxRestore(){
  const box=document.getElementById('wxlist');
  const side=document.getElementById('wxbought');
  side.innerHTML='<div class="wxside-h">✅ Куплено</div>';
  box.querySelectorAll('.wxrow').forEach(row=>{
    const id=row.getAttribute('data-wxid');
    const kb='wxbought:'+id, kl='wxlimit:'+id;
    const chkB=row.querySelector('.wxbuychk');
    const chkL=row.querySelector('.wxlimchk');
    const bts=row.querySelector('.wxbts');
    let tb=null,tl=null;
    try{tb=localStorage.getItem(kb);tl=localStorage.getItem(kl);}catch(_){}
    const stamp=()=>{const n=new Date(),p=x=>('0'+x).slice(-2);
      return p(n.getDate())+'.'+p(n.getMonth()+1)+' '+p(n.getHours())+':'+p(n.getMinutes());};
    const paint=()=>{
      let s='';
      if(tl) s+='🟡 лімітка '+tl;
      if(tb) s+=(s?' · ':'')+'✅ куплено '+tb;
      bts.textContent=s;
    };
    if(tl){chkL.checked=true;}
    if(tb){chkB.checked=true;row.classList.add('bought');side.appendChild(row);}
    paint();
    chkL.onchange=function(){
      try{ if(chkL.checked){tl=stamp();localStorage.setItem(kl,tl);}
           else{tl=null;localStorage.removeItem(kl);} paint(); }catch(_){}
    };
    chkB.onchange=function(){
      try{
        if(chkB.checked){tb=stamp();localStorage.setItem(kb,tb);row.classList.add('bought');side.appendChild(row);}
        else{tb=null;localStorage.removeItem(kb);row.classList.remove('bought');box.appendChild(row);}
        paint();
      }catch(_){}
    };
  });
  if(!side.querySelector('.wxrow'))
    {if(!side.querySelector('.wxempty2')) side.insertAdjacentHTML('beforeend','<div class="wxempty2">поки порожньо</div>');}
  else {const e=side.querySelector('.wxempty2'); if(e) e.remove();}
}
function wxAuto(){
  const b=document.getElementById('wxautobtn');
  if(wxTimer){clearInterval(wxTimer);wxTimer=null;b.style.color='#888';b.style.borderColor='#2a2d3a'}
  else{loadWx();wxTimer=setInterval(loadWx,60000);b.style.color='#7FDDBB';b.style.borderColor='#1D9E75'}
}

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
