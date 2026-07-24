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

def _gst_ts(s):
    """gameStartTime ('2026-07-21 15:35:00+00' или ISO с Z) → unix ts; None если не разобрать."""
    if not s:
        return None
    s = str(s).strip().replace(" ", "T")
    if s.endswith("+00"):
        s += ":00"                               # '+00' → '+00:00' (иначе fromisoformat падает на <3.11)
    s = s.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return None

def _prematch_px(tok, gst=None, timeout=8):
    """Пред-матчевая цена outcome = ПРЕД-ИГРОВАЯ ЛИНИЯ: цена последнего тика с t <= gameStartTime
    (устоявшийся рынок перед матчем — консистентно с тем, как fml собран в базе). Фолбэк
    (нет gst или нет тиков до старта) — медиана первых 6 тиков (цена открытия)."""
    if not tok:
        return None
    base = "https://clob.polymarket.com/prices-history?market=" + str(tok)
    def _hist(url):
        h = _pm_fetch(url, timeout=timeout)
        return h.get("history") if isinstance(h, dict) else None
    # основной запрос. Фолбэки — только если CLOB вернул пустой history (наблюдалась транзиентная пустота
    # по interval=max): узкое окно вокруг старта матча (профиль ≤15д, fidelity=30), затем 1w. Без изменения happy-path.
    hist = _hist(base + "&interval=max&fidelity=10")
    if not hist and gst:
        hist = _hist(base + ("&startTs=%d&endTs=%d&fidelity=30" % (int(gst) - 14 * 86400, int(gst) + 3600)))
    if not hist:
        hist = _hist(base + "&interval=1w&fidelity=30")
    if not hist:
        return None
    if gst:
        pre = [(int(x["t"]), float(x["p"])) for x in hist if "p" in x and "t" in x and int(x["t"]) <= gst]
        if pre:
            return max(pre, key=lambda z: z[0])[1]   # пред-игровая линия (последний тик до старта матча)
    ps = sorted(float(x["p"]) for x in hist[:6] if "p" in x)
    return ps[len(ps) // 2] if ps else None          # фолбэк: цена открытия

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
    series_gst = None                # gameStartTime серия-рынка → пред-игровая линия прематча
    gt_mkt = None; hcap_mkts = []    # v5: Games Total (Over) и Map Handicap — эквив. рынки ноги-хеджа
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
        elif ("(bo3)" in q or "(bo5)" in q or "match winner" in q) and not junk:
            # (раньше был guard "map"/"game" not in q — он ЛОЖНО срабатывал на именах команд,
            #  напр. "GamerLegion" содержит "game" → рынок серии отбрасывался. Карта-winner рынки
            #  ловятся ветками выше, junk отсекает handicap/total/rounds — этот guard лишний.)
            series = (outs, pv, tk); t1name, t2name = outs[0], outs[1]
            series_gst = m.get("gameStartTime")
        elif "games total" in q and "2.5" in q:
            gt_mkt = (outs, pv, tk, m)
        elif "map handicap" in q:
            hcap_mkts.append((outs, pv, tk, m))
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

    # v5: цены + ликвидность ноги-хеджа на ЭКВИВАЛЕНТНЫХ рынках (Games Total «Over», Map Handicap проигравший «+1.5»)
    # — для ПОКАЗА в тикете рядом с Map2 (сравнение цены+объёма). Нужен победитель К1 → проигравший.
    hedge_liq = {}
    if not light and (map1st or {}).get("winner") and t1name and t2name:
        _k1w = map1st["winner"]
        _loser = t2name if _k1w == t1name else t1name
        def _mvol(mm):
            try: return float(mm.get("volumeNum") or mm.get("volume") or 0)
            except Exception: return 0.0
        def _add_hedge(key, tok, pvf):
            b, a = _book_top(tok)
            prices["ask"][key] = round((a if a is not None else pvf) * 100, 1)
            prices["bid"][key] = round((b if b is not None else pvf) * 100, 1)
        if gt_mkt:
            _o, _pv, _tk, _m = gt_mkt
            _oi = next((i for i, o in enumerate(_o) if "over" in str(o).lower()), None)
            if _oi is not None and _tk and len(_tk) > _oi and _tk[_oi]:
                _add_hedge("hedge_gt", _tk[_oi], _pv[_oi]); hedge_liq["gt"] = _mvol(_m)
        for _o, _pv, _tk, _m in hcap_mkts:            # проигравший «+1.5» = сторона index1 рынка "X(-1.5) vs L(+1.5)"
            if len(_o) >= 2 and str(_o[1]).strip().lower() == str(_loser).strip().lower() and _tk and len(_tk) > 1 and _tk[1]:
                _add_hedge("hedge_hcap", _tk[1], _pv[1]); hedge_liq["hcap"] = _mvol(_m)
                break

    prematch = {}                                    # пред-матчевая цена серии (для контекст-полей)
    if not light and series:
        stk = series[2]
        if stk and len(stk) >= 2:
            gst = _gst_ts(series_gst)                # пред-игровая линия — по времени старта матча
            a = _prematch_px(stk[0], gst); b = _prematch_px(stk[1], gst)
            if a is not None:
                prematch["t1"] = round(a * 100, 1)
            if b is not None:
                prematch["t2"] = round(b * 100, 1)

    try:
        _vol = float(ev.get("volume") or 0)
    except Exception:
        _vol = 0.0
    return {"slug": slug, "t1name": t1name, "t2name": t2name,
            "prices": prices, "prematch": prematch,
            "map1": map1st, "map2": map2st, "series": seriesst, "bo3": bo3,
            "volume": _vol, "gst": series_gst, "hedge_liq": hedge_liq}

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
        elif self.path.startswith("/feed"):
            try:
                payload = scan_feed()
            except Exception as e:
                payload = {"matches": [], "count": 0, "error": str(e)}
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

# ── ЛЕНТА живых киберспорт-матчей (CS2 / LoL / Dota2 / Valorant) ─────────────────
# Единая доска-монитор: Bo3 команда-vs-команда, event volume ≥ $100k, предстоящие +
# идущие (не завершённые). Источник — Polymarket gamma (events?tag_slug) + CLOB /book,
# всё через _pm_fetch → локально тестируется через прокси. Никаких прогнозов/эджа —
# только показ: игра · время старта · глубина стакана · объём (он же задаёт цвет).
FEED_GAMES = [
    {"tag": "counter-strike-2",  "label": "CS2"},
    {"tag": "league-of-legends", "label": "LoL"},
    {"tag": "dota-2",            "label": "Dota2"},
    {"tag": "valorant",          "label": "Valorant"},
]
FEED_MIN_VOL = 100_000                 # порог агрегатного volume события
FEED_GRACE   = 4 * 3600                # сек: начавшийся ≤4ч назад ещё «идёт»
FEED_TTL     = 25                      # сек: серверный кэш скана (не долбить CLOB)
# маркеры аутрайта/фьючерса/пропа в СЛУГЕ (у слуга матча их нет — там team-team-дата)
_FEED_OUTRIGHT = ("winner", "qualif", "champion", "winning-region", "-mvp", "season-")
_FEED_CACHE = {"ts": 0.0, "data": None}
_UNIT_WIN_RE = re.compile(r"\b(?:map|game)\s*([1-5])\s+winner")

def _feed_vol_zone(vol):
    # цвет строки по объёму: 100k–200k серый · 200k–500k жёлтый · ≥500k зелёный
    txt = "$%.0fk" % (vol / 1000.0)
    if vol >= 500_000:
        return ("green",  "#1D9E75", txt)
    if vol >= 200_000:
        return ("yellow", "#D9A441", txt)
    return ("gray", "#8893a0", txt)

def _feed_depth(tokens, within=0.12):
    """$ ликвидности на асках обоих исходов match-winner в пределах within¢ от лучшего
    аска (сумма по сторонам). Метод тот же, что _book_depth_usd (теннис/bo5), но через
    _pm_fetch — работает и локально через прокси. None — если стакан вообще не отдался."""
    total = 0.0
    got = False
    for tok in (tokens or [])[:2]:
        if not tok:
            continue
        b = _pm_fetch("https://clob.polymarket.com/book?token_id=" + str(tok))
        if not isinstance(b, dict):
            continue
        try:
            pr = sorted((float(a["price"]), float(a["size"])) for a in (b.get("asks") or []))
        except Exception:
            pr = []
        if not pr:
            continue
        got = True
        best = pr[0][0]
        total += sum(p * s for p, s in pr if p <= best + within)
    return round(total) if got else None

def _feed_when(gst, now):
    """(метка времени по Киеву, относительная подпись) для старта матча gst (unix)."""
    try:
        if KYIV:
            dt = datetime.fromtimestamp(gst, KYIV)
            today = datetime.now(KYIV).date()
        else:
            dt = datetime.utcfromtimestamp(gst)
            today = datetime.utcnow().date()
        label = dt.strftime("%H:%M") if dt.date() == today else dt.strftime("%d.%m %H:%M")
    except Exception:
        label = ""
    d = gst - now
    if d <= 0:
        rel = "в игре"
    elif d < 3600:
        rel = "через %d мин" % int(round(d / 60.0))
    else:
        rel = "через %.1f ч" % (d / 3600.0)
    return label, rel

def _feed_jlist(m, key):
    v = m.get(key)
    try:
        return json.loads(v) if isinstance(v, str) else (v or [])
    except Exception:
        return []

def scan_feed():
    import time as _t
    now = _t.time()
    with LOCK:
        if _FEED_CACHE["data"] is not None and (now - _FEED_CACHE["ts"]) < FEED_TTL:
            return _FEED_CACHE["data"]

    matches = []
    for g in FEED_GAMES:
        url = ("https://gamma-api.polymarket.com/events?tag_slug=%s&closed=false"
               "&limit=100&order=volume&ascending=false" % g["tag"])
        ev = _pm_fetch(url)
        if not isinstance(ev, list):
            continue
        for e in ev:
            try:
                vol = float(e.get("volume") or 0)
            except Exception:
                vol = 0.0
            if vol < FEED_MIN_VOL:
                continue                                # список отсортирован ↓ по объёму
            title = e.get("title") or ""
            slug = e.get("slug") or ""
            low = title.lower()
            sl = slug.lower()
            # только матч команда-vs-команда: " vs " в заголовке + слуг без маркеров аутрайта
            if " vs " not in low:
                continue
            if any(k in sl for k in _FEED_OUTRIGHT):
                continue
            mks = e.get("markets") or []
            unit_win = {}                               # N → рынок «Map/Game N Winner»
            mw = None                                   # рынок серии (match winner)
            for m in mks:
                q = (m.get("question") or "")
                ql = q.lower()
                if ("handicap" in ql) or ("total" in ql) or ("rounds" in ql) or ("over/under" in ql):
                    continue                            # проп-рынки (не winner) — мимо
                um = _UNIT_WIN_RE.search(ql)
                if um:
                    unit_win[int(um.group(1))] = m
                elif (q.strip() == title.strip()) or ("(bo3)" in ql) or ("(bo5)" in ql):
                    mw = mw or m                        # рынок серии == заголовок / (BOn)
            # Bo3-детект: есть Map/Game 1 и 2, нет 4/5; иначе Bo1/Bo5 — вон
            if unit_win.get(4) or unit_win.get(5) or ("(bo5)" in low):
                continue
            if not (unit_win.get(1) and unit_win.get(2)):
                continue
            if mw is None:
                continue
            mo = _feed_jlist(mw, "outcomes")
            mtk = _feed_jlist(mw, "clobTokenIds")
            try:
                mpn = [float(x) for x in _feed_jlist(mw, "outcomePrices")]
            except Exception:
                mpn = []
            if len(mo) < 2 or len(mpn) < 2:
                continue
            # время старта — ТОЛЬКО market.gameStartTime (event.startDate = момент создания, врёт)
            gst = _gst_ts(mw.get("gameStartTime"))
            if gst is None:
                cand = [c for c in (_gst_ts(unit_win[i].get("gameStartTime"))
                                    for i in sorted(unit_win)) if c]
                gst = min(cand) if cand else None
            if gst is None:
                continue
            resolved = max(mpn) > 0.97
            # предстоящие + идущие; завершённые/старьё — вон
            if gst >= now:
                pass                                    # ещё не начался — показываем
            elif gst >= now - FEED_GRACE:
                if resolved:
                    continue                            # начался и резолвнут = завершён
            else:
                continue                                # старый (в closed=false висят майские)
            zone, color, vtext = _feed_vol_zone(vol)
            label, rel = _feed_when(gst, now)
            matches.append({
                "game": g["label"], "tag": g["tag"],
                "t1": str(mo[0]), "t2": str(mo[1]),
                "gst": gst, "start": label, "when": rel,
                "vol": round(vol), "vol_text": vtext, "zone": zone, "color": color,
                "depth": _feed_depth(mtk),
                "url": "https://polymarket.com/event/" + slug,
            })

    matches.sort(key=lambda x: x["gst"])                # ближайшие сверху
    out = {"matches": matches, "count": len(matches),
           "ts": (datetime.now(KYIV).strftime("%H:%M:%S") if KYIV else "")}
    with LOCK:
        _FEED_CACHE["ts"] = now
        _FEED_CACHE["data"] = out
    return out

PAGE = r"""<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Live Esports · Polymarket</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f1117;color:#e0e0e0;min-height:100vh;padding:16px;max-width:1100px;margin:0 auto}
h1{font-size:18px;font-weight:600;color:#fff;margin-bottom:4px;display:flex;align-items:center;gap:9px}
.dot{width:9px;height:9px;border-radius:50%;background:#1D9E75;animation:p 1.6s infinite}
@keyframes p{0%,100%{opacity:1}50%{opacity:.25}}
.sub{font-size:12px;color:#667;margin:0 0 14px 18px}
.bar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:12px}
.bar button{background:#13212e;border:1px solid #2a3d4a;color:#9fcdf0;border-radius:8px;padding:8px 14px;cursor:pointer;font-size:13px;font-weight:500}
.bar button:active{opacity:.7}
.bar .auto.on{background:#11251c;border-color:#1D9E75;color:#7FDDBB}
#status{font-size:12px;color:#8aa0b0}
.legend{display:flex;gap:14px;flex-wrap:wrap;font-size:11px;color:#8893a0;margin-bottom:14px;align-items:center}
.legend b{color:#a8b3c0;font-weight:600}
.lg{display:inline-flex;align-items:center;gap:5px}
.sw{width:11px;height:11px;border-radius:3px;display:inline-block}
.feed{display:flex;flex-direction:column;gap:7px}
.mrow{display:flex;align-items:center;gap:11px;flex-wrap:wrap;background:#161a24;border:1px solid #262b38;border-left-width:4px;border-radius:10px;padding:10px 13px}
.badge{font-size:11px;font-weight:800;letter-spacing:.03em;padding:4px 9px;border-radius:6px;min-width:62px;text-align:center;flex:none}
.teams{font-size:15px;font-weight:600;color:#f0f0f0;flex:1;min-width:150px}
.teams .vs{color:#5a6472;font-weight:400;font-size:13px;margin:0 5px}
.time{font-size:15px;font-weight:700;color:#fff;background:#0e1420;border:1px solid #2f3a4a;border-radius:7px;padding:5px 11px;white-space:nowrap;font-variant-numeric:tabular-nums}
.time .rel{font-size:11px;font-weight:500;color:#8492a6;margin-left:6px}
.time.live{color:#7FE3C2;border-color:#1D9E7566;background:#0d2a1e}
.time.live .rel{color:#7FDDBB}
.depth{font-size:12px;color:#9fb0c2;background:#12151f;border:1px solid #2a3140;border-radius:6px;padding:4px 9px;white-space:nowrap}
.depth b{color:#cdd8e5;font-weight:600}
.depth.thin b{color:#e0a05a}
.vol{font-size:14px;font-weight:800;padding:4px 11px;border-radius:6px;white-space:nowrap;font-variant-numeric:tabular-nums}
.lnk{color:#5BA3E0;text-decoration:none;font-size:17px;padding:0 4px;flex:none}
.lnk:active{opacity:.6}
.empty{font-size:13px;color:#556;font-style:italic;padding:24px;text-align:center}
@media(max-width:560px){.teams{min-width:100%;order:5}.badge{order:1}.time{order:2}.vol{order:3}.depth{order:4}.lnk{order:6;margin-left:auto}}
</style></head><body>
<h1><div class="dot"></div> Live Esports · Polymarket</h1>
<div class="sub">CS2 · LoL · Dota2 · Valorant — только Bo3 команда-vs-команда, объём ≥ $100k, идущие и ближайшие</div>

<div class="bar">
  <button onclick="loadFeed()">↻ Обновить</button>
  <button class="auto on" id="autobtn" onclick="toggleAuto()">⏱ Авто: вкл</button>
  <span id="status">загрузка…</span>
</div>

<div class="legend">
  <b>Объём:</b>
  <span class="lg"><span class="sw" style="background:#8893a0"></span> $100k–200k</span>
  <span class="lg"><span class="sw" style="background:#D9A441"></span> $200k–500k</span>
  <span class="lg"><span class="sw" style="background:#1D9E75"></span> ≥ $500k</span>
  <span style="color:#556">·  время киевское · «стакан» = глубина match-winner на ±12¢</span>
</div>

<div class="feed" id="feed"><div class="empty">загрузка…</div></div>

<script>
var GAME_COL = {CS2:'#F5A623', LoL:'#4C9BE0', Dota2:'#E24B4A', Valorant:'#E255A0'};
var auto = true, timer = null;

function esc(s){return (s||'').replace(/[&<>"]/g,function(c){return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'})[c]})}
function money(v){ if(v==null) return '—'; return '$'+(v>=1000?(v/1000).toFixed(v>=100000?0:1)+'k':v); }

function render(ms){
  var box = document.getElementById('feed');
  if(!ms || !ms.length){ box.innerHTML='<div class="empty">Нет активных Bo3-матчей ≥ $100k сейчас</div>'; return; }
  box.innerHTML = ms.map(function(m){
    var gc = GAME_COL[m.game] || '#8893a0';
    var badge = '<span class="badge" style="color:'+gc+';background:'+gc+'22;border:1px solid '+gc+'55">'+esc(m.game)+'</span>';
    var live = (m.when==='в игре');
    var time = '<span class="time'+(live?' live':'')+'">⏰ '+esc(m.start)+'<span class="rel">'+esc(m.when)+'</span></span>';
    var thin = (m.depth!=null && m.depth<2000);
    var depth = '<span class="depth'+(thin?' thin':'')+'">стакан <b>'+money(m.depth)+'</b></span>';
    var vol = '<span class="vol" style="color:'+m.color+';background:'+m.color+'1e;border:1px solid '+m.color+'55">'+esc(m.vol_text)+'</span>';
    var link = m.url ? '<a class="lnk" href="'+m.url+'" target="_blank" title="Открыть на Polymarket">↗</a>' : '';
    return '<div class="mrow" style="border-left-color:'+m.color+'">'
      + badge
      + '<span class="teams">'+esc(m.t1)+'<span class="vs">vs</span>'+esc(m.t2)+'</span>'
      + time + depth + vol + link
      + '</div>';
  }).join('');
}

async function loadFeed(){
  var st = document.getElementById('status');
  st.textContent = 'загрузка…';
  try{
    var r = await fetch('/feed'); var d = await r.json();
    render(d.matches||[]);
    var tm = d.ts || new Date().toLocaleTimeString('uk-UA',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
    st.textContent = (d.matches?d.matches.length:0)+' матчей · обновлено '+tm+(d.error?(' · ошибка: '+d.error):'');
  }catch(e){ st.textContent = 'ошибка загрузки'; }
}

function toggleAuto(){
  auto = !auto;
  var b = document.getElementById('autobtn');
  b.className = 'auto'+(auto?' on':'');
  b.textContent = '⏱ Авто: '+(auto?'вкл':'выкл');
  if(auto){ loadFeed(); if(!timer) timer=setInterval(loadFeed,30000); }
  else if(timer){ clearInterval(timer); timer=null; }
}

loadFeed();
timer = setInterval(loadFeed, 30000);
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
