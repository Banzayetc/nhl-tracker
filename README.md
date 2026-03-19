# NHL Polymarket Trend Tracker

Отслеживает движение цен на Polymarket (NHL-матчи).  
Алёрт: если цена двинулась **≥ 3¢ за последние 24ч** до матча — матч появляется на дашборде с графиком.

---

## Деплой на Railway (бесплатно, ~5 минут)

### 1. Залей код на GitHub

```bash
git init
git add .
git commit -m "init"
git remote add origin https://github.com/ВАШ_ЮЗЕР/nhl-tracker.git
git push -u origin main
```

### 2. Создай проект на Railway

1. Зайди на [railway.app](https://railway.app) → **New Project → Deploy from GitHub repo**
2. Выбери репозиторий `nhl-tracker`
3. Railway сам определит Python-проект

### 3. Добавь Volume (важно — иначе SQLite сбрасывается при редеплое)

В Railway → твой сервис → **Volumes → Add Volume**
- Mount path: `/data`

Затем в **Variables** добавь:

```
DB_PATH=/data/polymarket.db
```

### 4. Готово

Railway автоматически запустит `gunicorn` через `Procfile`.  
Открой URL из Railway — дашборд появится сразу.  
Первые графики появятся через **1–2 часа** (нужно накопить минимум 2 снапшота).

---

## Переменные окружения

| Переменная       | По умолчанию | Описание                          |
|-----------------|-------------|-----------------------------------|
| `DB_PATH`       | `polymarket.db` | Путь к SQLite (укажи `/data/...` на Railway) |
| `POLL_INTERVAL` | `60`        | Как часто опрашивать API (минуты) |
| `TREND_THRESHOLD` | `0.03`    | Порог движения цены (0.03 = 3¢)  |
| `PORT`          | `5000`      | Порт (Railway подставляет сам)    |

---

## Локальный запуск

```bash
pip install -r requirements.txt
python app.py
# → http://localhost:5000
```

---

## Эндпоинты

| URL | Описание |
|-----|---------|
| `/` | Веб-дашборд с графиками |
| `/api/matches` | JSON с трендовыми матчами |
| `/health` | Healthcheck для Railway |

---

## Как считается тренд

- Каждый час: снапшот цен всех NHL-матчей (стартующих в ближайшие 72ч)
- При открытии дашборда: для каждого матча (стартующего в ближайшие 48ч) берётся разница между **первым** и **последним** снапшотом за последние 24ч
- Если |Δ| ≥ 3¢ — матч попадает на дашборд, отсортированный по силе движения
