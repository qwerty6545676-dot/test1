# Arbitrage Scanner — полное руководство

> Подробный гайд по использованию сканера на русском.
> Для технической документации проекта см. [README.md](README.md).

## Содержание

1. [Что это и как работает](#что-это-и-как-работает)
2. [Установка и первый запуск](#установка-и-первый-запуск)
3. [Настройка `settings.yaml`](#настройка-settingsyaml) — детально каждое поле
4. [Настройка `.env`](#настройка-env)
5. [Запуск и остановка](#запуск-и-остановка)
6. [Как читать логи](#как-читать-логи)
7. [Telegram-алёрты](#telegram-алёрты)
8. [Анализ сигналов](#анализ-сигналов) *(планируется)*
9. [Backtest и replay](#backtest-и-replay) *(планируется)*
10. [Paper trading](#paper-trading)
11. [Production-деплой](#production-деплой) *(планируется)*
12. [Troubleshooting](#troubleshooting)
13. [FAQ](#faq)
14. [Roadmap](#roadmap)

---

## Что это и как работает

Сканер параллельно подключается к **7 биржам** по WebSocket, в режиме
реального времени читает топ-стакана (лучшие bid/ask) по всем
настроенным символам и при каждом обновлении проверяет: нет ли сейчас
возможности купить на одной бирже и продать на другой с прибылью
**после** комиссий.

### 7 бирж (spot сейчас, perp в планах)

| Биржа | Статус | Канал |
|---|---|---|
| Binance | ✅ | `bookTicker` combined stream |
| Bybit | ✅ | `orderbook.1` + snapshot/delta merge |
| Gate.io | ✅ | `spot.book_ticker` + 10s ping |
| Bitget | ✅ | `books1` + 25s ping |
| KuCoin | ✅ | REST bullet-public → `/market/ticker` |
| BingX | ✅ | gzip-фреймы, `@bookTicker`, ping/pong |
| MEXC | ✅ | Protobuf, `aggre.bookTicker@100ms` |

### Архитектурные принципы

- **Без очередей на hot path.** Listener получил фрейм → распарсил →
  записал `Tick` в общий dict `prices[symbol][exchange]` → **тут же**
  вызвал comparator. Никакой `asyncio.Queue`, никаких `sleep(0.1)`.
- **GIL-атомарный dict.** Один event-loop, одна корутина в моменте.
  Блокировки не нужны.
- **picows + uvloop + msgspec.** Cython-WebSocket поверх libuv,
  C-парсер JSON/Protobuf. Это максимум что можно выжать из CPython.
- **Каждая биржа — отдельная задача.** Упала одна → остальные
  работают. Heartbeat-монитор выкидывает "замолчавшие" биржи.

### Что находит сканер

**Spatial arbitrage** (пространственный): один и тот же символ, одна
и та же пара валют, но на разных биржах цены расходятся. Формула:

```
net% = (sell_bid * (1 - fee_sell)) / (buy_ask * (1 + fee_buy)) - 1
```

Только если `net%` выше порога из `settings.yaml` → сигнал.

**Чего сканер пока НЕ делает** (по дизайну):

- Треугольный арбитраж (BTC→ETH→USDT→BTC на одной бирже)
- Кросс-валютный (BTC/USDT vs BTC/USDC)
- Мульти-хоп через промежуточные монеты
- Perp-рынок (в плане, следующий PR)

---

## Установка и первый запуск

### Требования

- **OS:** Linux (Ubuntu/Debian), macOS. Windows не тестировался.
- **Python:** 3.11 или 3.12. **Не используй 3.13t** (free-threaded
  build — `asyncio` в нём медленнее).
- **Память:** 2 ГБ достаточно даже с запасом.
- **CPU:** 2 vCPU хватает. Всё упирается в сеть, не в CPU.
- **Сеть:** стабильный канал, низкий RTT до бирж. См.
  [FAQ → где разместить](#где-лучше-разместить-сервер).

### Первый запуск

```bash
# 1. Клонируй репо
git clone https://github.com/qwerty6545676-dot/test1.git
cd test1

# 2. Виртуальное окружение
python3.12 -m venv .venv
source .venv/bin/activate

# 3. Зависимости
pip install -r requirements.txt

# 4. Копируй шаблоны конфигов
cp settings.example.yaml settings.yaml   # свои настройки
cp .env.example .env                     # секреты (TG-токен)

# 5. Запусти
python -m arbitrage.main
```

Если всё установилось правильно, в консоли увидишь:

```
INFO  arbitrage.main          | starting exchange listeners
INFO  arbitrage.binance       | connected
INFO  arbitrage.bybit         | connected
INFO  arbitrage.gateio        | connected
...
```

**Остановка:** `Ctrl+C` (graceful shutdown, все коннекты закрываются
корректно).

---

## Настройка `settings.yaml`

Этот файл — **единственное место** где ты правишь параметры. Он
gitignored — твои изменения не уедут в git. Шаблон с комментариями
лежит в `settings.example.yaml` (закоммичен).

Все поля валидируются через msgspec. Опечатка в имени поля или
неправильный тип → ошибка **при старте**, а не молчаливо в рантайме.

### Секция `symbols`

```yaml
symbols:
  spot: [BTCUSDT, ETHUSDT, SOLUSDT]
  perp: [BTCUSDT, ETHUSDT, SOLUSDT]
```

Какие пары сканировать. Формат канонический: `БАЗА+КВОУТ` без
разделителя (`BTCUSDT`, не `BTC/USDT` и не `BTC-USDT`). Листенер
каждой биржи сам переводит в нативный формат (`BTC_USDT` для
Gate.io, `BTC-USDT` для KuCoin).

**Добавляй пары осторожно.** Каждая дополнительная пара = доп.
подписки на 7 биржах = доп. нагрузка + риск упереться в rate-limit.
Для старта 3 самых ликвидных пар оптимально.

### Секция `filters`

```yaml
filters:
  spot:
    min_profit_pct: 3.0
    cooldown_seconds: 180
    info_topic_id: 104
    tiers:
      low:  { from_pct: 3.0,  to_pct: 5.0,   topic_id: 101 }
      mid:  { from_pct: 5.0,  to_pct: 10.0,  topic_id: 102 }
      high: { from_pct: 10.0, to_pct: 999.0, topic_id: 103 }
  perp:
    min_profit_pct: 0.5
    cooldown_seconds: 180
    info_topic_id: 204
    tiers:
      low:  { from_pct: 0.5, to_pct: 1.0,   topic_id: 201 }
      mid:  { from_pct: 1.0, to_pct: 3.0,   topic_id: 202 }
      high: { from_pct: 3.0, to_pct: 999.0, topic_id: 203 }
```

**`min_profit_pct`** — минимальный `net%` после комиссий чтобы
вообще зарегистрировать сигнал. Всё что ниже — бот даже не считает
за арб.

- Для spot ставь 3.0 и выше — на ликвидных spot-парах всё что ниже
  всё равно не исполнить (slippage, withdrawal latency).
- Для perp можно 0.5 — perp арбы редкие но быстрые.

**`cooldown_seconds`** — по одной и той же паре `(symbol, buy_ex,
sell_ex)` после сигнала не шлём следующее сообщение раньше чем
через N секунд. 180 (3 мин) — разумно. Защищает от флуда когда
спред "залипает" на 5 минут.

**`tiers`** — три уровня для маршрутизации в разные Telegram-топики.
Каждый уровень = `{from_pct, to_pct, topic_id}`:

- `low`: 3–5% → топик "низкие спреды"
- `mid`: 5–10% → топик "средние"
- `high`: 10%+ → топик "высокие" (можно с @mention)

**Диапазоны не пересекаются** — сигнал попадает в **один** топик.
Если спред `4.5%` → low. Если `7%` → mid. Если `15%` → high. Если
**ниже** `min_profit_pct` — никуда не шлётся (дропается до tier'ов).

**`info_topic_id`** — топик для **служебных** сообщений: heartbeat,
silence alerts, reconnects, critical errors, daily summary. Не
перепутай с сигналами.

**`topic_id: null`** — если хочешь чтобы какой-то tier **не слался**
в TG, поставь null. Сигналы той группы всё равно будут в логе, но
без уведомления.

### Секция `fees`

```yaml
fees:
  spot:
    binance: 0.001    # 0.10%
    bybit:   0.001
    gateio:  0.002    # 0.20%
    ...
  perp:
    binance: 0.0004   # 0.04%
    bybit:   0.00055
    ...
```

**Taker-комиссии** каждой биржи как **дробь**, не процент. `0.001`
это 0.10%, `0.0004` это 0.04%.

⚠️ **Проверь свои реальные ставки** в личном кабинете каждой биржи.
Значения в `settings.example.yaml` — это публичные базовые ставки,
но ты можешь платить меньше если:

- У тебя VIP-уровень (по объёму торгов за 30 дней)
- Ты холдишь токен биржи (BNB, GT, KCS, BGB и т.д. дают скидку)
- У тебя есть реферальная/партнёрская ставка
- Ты маркет-мейкер с rebate'ом на лимитные ордеры (makerFee
  отрицательный!)

Расчёт в comparator **round-trip**: фактический расход =
`fee_buy + fee_sell + slippage`. Если у тебя 0.1% × 2 = **0.2%** на
вход и выход. Spread ниже 0.2% — чистая потеря даже на бумаге.

### Секция `limits`

```yaml
limits:
  max_age_ms: 500              # ticks старее = игнорировать
  heartbeat_interval_ms: 1000  # как часто проверяем живость
  heartbeat_timeout_ms: 3000   # >3с без обновления = выкидываем биржу
  backoff_max_s: 60.0          # максимальная пауза между reconnect
  backoff_jitter_s: 1.0        # случайность чтобы не долбить одновременно
```

**`max_age_ms`**: если тик старше этого — comparator его **не
рассматривает**. 500мс = хочешь арбитражить "сейчас", не "полсекунды
назад". Слишком маленькое (50мс) → пропустишь арбы из-за дрифта
часов; слишком большое (5000мс) → будешь гнаться за призраками.

**`heartbeat_timeout_ms`**: если биржа не прислала **ничего** больше
N мс — её entry удаляется из `prices`. Это защита от "биржа
подвисла, стакан заморожен на 30 секунд". Для жидких пар 3000мс
норм. Для неликвидных — можешь поднять до 10000.

**`backoff_max_s`, `backoff_jitter_s`**: при разрыве коннекта бот
пытается переподключиться с экспоненциальной задержкой:
`min(2**attempt, max_s) + random(0..jitter_s)`. Jitter нужен чтобы
7 коннектов не долбили сервер одновременно после сетевого сбоя.

### Секция `telegram`

```yaml
telegram:
  enabled: false           # переключи в true когда будет готов TG-интеграция
  spot_chat_id: null       # -1001234567890 (с префиксом -100!)
  perp_chat_id: null
```

**`enabled: false`** = сигналы всё равно детектятся и пишутся в
лог, но ничего не отправляется в TG. По-умолчанию off — полезно
когда ты ещё настраиваешь пороги и не хочешь спамить себя.

**`spot_chat_id`, `perp_chat_id`** — ID групп в Telegram. Как
получить:

1. Создаёшь **супергруппу** (обычная группа не поддерживает топики).
2. В настройках → `Topics` → включаешь.
3. Добавляешь своего бота как администратора с правом слать
   сообщения.
4. Узнаёшь ID через [@username_to_id_bot](https://t.me/username_to_id_bot)
   или отправляешь сообщение в группу и делаешь
   `curl "https://api.telegram.org/bot<TOKEN>/getUpdates"`.
5. ID группы начинается с `-100`, например `-1001234567890`. Весь
   этот ID, с минусом и префиксом, вставляешь в yaml.

`topic_id` в `filters.spot.tiers.*` — ID топиков (message thread id).
Узнаёшь через тот же `getUpdates` — в сообщении из топика будет поле
`message_thread_id`.

**Токен бота** лежит **не здесь**, а в `.env` (см. ниже).

---

## Настройка `.env`

```bash
# Обязательное (если telegram.enabled = true)
TELEGRAM_BOT_TOKEN=123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11

# Опциональное — указать альтернативный путь к settings.yaml
# ARB_SETTINGS_PATH=/etc/arbitrage/settings.yaml
```

**`TELEGRAM_BOT_TOKEN`** — получаешь у [@BotFather](https://t.me/BotFather)
командой `/newbot`. Длинная строка через двоеточие. Никогда **не
коммить** этот файл (`.env` уже в `.gitignore`).

**`ARB_SETTINGS_PATH`** — полезно для запуска нескольких инстансов
сканера на одной машине с разными настройками (например, live и
paper рядом).

---

## Запуск и остановка

### Обычный запуск

```bash
source .venv/bin/activate
python -m arbitrage.main
```

Логи идут в stdout. Для фонового запуска:

```bash
nohup python -m arbitrage.main > logs/arbitrage.log 2>&1 &
echo $! > arbitrage.pid
```

Убить:

```bash
kill $(cat arbitrage.pid)
```

### Production-запуск через systemd

См. раздел [Production-деплой](#production-деплой) (планируется).

### Graceful shutdown

`Ctrl+C` или `kill -TERM <pid>` → бот закрывает все WS-коннекты,
завершает корутины, пишет последнюю запись в лог, выходит с кодом 0.
**Не используй `kill -9`** — потеряешь незаписанные данные.

---

## Как читать логи

Формат лог-строки:

```
10:32:07.412 INFO arbitrage.comparator | ARB BTCUSDT: buy binance @ 64982.10000000 -> sell bybit @ 65041.50000000 | net 0.181%
```

Разбор:

- `10:32:07.412` — время UTC с миллисекундами
- `INFO` — уровень (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- `arbitrage.comparator` — источник (модуль)
- остальное — сообщение

### Ключевые сообщения

**Успешная работа:**

```
INFO arbitrage.binance  | connected
INFO arbitrage.comparator | ARB BTCUSDT: buy X @ ... -> sell Y @ ... | net 3.24%
```

Всё ок, нашёл арбитраж.

**Предупреждения (в файл-лог, не в TG):**

```
WARNING arbitrage.normalizer | dropped tick with bid > ask: ...
WARNING arbitrage.bingx | gzip decode failed for non-data frame, ignoring
WARNING arbitrage.heartbeat | evicted stale gateio/BTCUSDT (>3000ms)
```

Это нормально в небольших количествах. Если льёт постоянно — смотри
[troubleshooting](#troubleshooting).

**Серьёзные ошибки (поедут в TG когда будет интеграция):**

```
ERROR arbitrage.mexc | protobuf decode failed 5x in a row — feed broken?
ERROR arbitrage.kucoin | bullet-public REST fetch failed after 3 retries
ERROR arbitrage.main | listener <bybit> died, supervisor will restart
```

Это требует твоего внимания.

**Startup/shutdown:**

```
INFO arbitrage.main | starting exchange listeners
INFO arbitrage.main | shutting down
```

---

## Telegram-алёрты

**Статус:** ✅ Реализовано. Отсутствуют: hourly heartbeat и daily
summary — они зависят от Persistence и появятся в следующих PR.

### Структура

У тебя **две супергруппы** в TG — одна для spot, одна для perp. В
каждой — **4 топика**:

| Топик | Что туда идёт |
|---|---|
| `low` (3-5% spot / 0.5-1% perp) | Низкие спреды — много, но слабые |
| `mid` (5-10% spot / 1-3% perp) | Средние — интересные |
| `high` (10%+ spot / 3%+ perp) | Жирные — приоритет, проверить срочно |
| `info` | Silence, recovery, reconnects, errors, startup/shutdown |

Границы тиров настраиваются — см. `filters.spot.tiers` / `filters.perp.tiers`
в `settings.yaml`.

### Разовая настройка

1. Создай бота через [@BotFather](https://t.me/BotFather) (`/newbot`),
   сохрани токен в `.env` как `TELEGRAM_BOT_TOKEN`.
2. Создай **2 супергруппы** (одну для spot, одну для perp).
   Settings → Topics → включи. Добавь бота как **админа**.
3. В каждой группе создай **4 топика**: `low`, `mid`, `high`, `info`.
4. Узнай `chat_id` каждой группы + `message_thread_id` каждого топика:
   напиши в каждом топике что-нибудь боту (или просто добавь бота и
   напиши), открой
   `https://api.telegram.org/bot<TOKEN>/getUpdates` — увидишь `chat.id`
   и `message_thread_id`.
5. Проставь их в `settings.yaml`:
   ```yaml
   telegram:
     enabled: true
     spot_chat_id: -1001234567890
     perp_chat_id: -1009876543210
   filters:
     spot:
       info_topic_id: 2      # "info" топик в spot-группе
       tiers:
         low:  { from_pct: 3.0,  to_pct: 5.0,   topic_id: 3 }
         mid:  { from_pct: 5.0,  to_pct: 10.0,  topic_id: 4 }
         high: { from_pct: 10.0, to_pct: 999.0, topic_id: 5 }
     perp:
       info_topic_id: 2
       tiers:
         low:  { from_pct: 0.5, to_pct: 1.0,   topic_id: 3 }
         mid:  { from_pct: 1.0, to_pct: 3.0,   topic_id: 4 }
         high: { from_pct: 3.0, to_pct: 999.0, topic_id: 5 }
   ```

Если `telegram.enabled: false` или токен не задан — сканер продолжит
работать, просто **ничего не шлёт в TG**. В логах остаётся всё, как
было.

### Что в info-топике

- **🚀 Startup / 🛑 Shutdown** — когда сканер запускается и
  останавливается.
- **🔁 Reconnect** — каждый раз, когда биржа переподключается:
  `"binance-perp: reconnecting in 4.2s (attempt 2)"`.
- **🔇 Silence** — если биржа молчит дольше `silence_threshold_ms`
  (по умолчанию 30 с, у heartbeat-монитора):
  `"bybit: no ticks for 34s"`.
- **✅ Recovery** — когда молчавшая биржа снова шлёт тики:
  `"bybit: ticks resumed"`.
- **❌ Error** — любой `logging.ERROR` из любого места приложения
  (exception в listener'е, decode fail и т.п.) автоматически
  пробрасывается в info-топик обеих групп.

> **Что НЕ идёт в TG:** `WARNING`-уровень. Это сделано намеренно —
> warnings шумные (кривые тики, отдельные decode fails, flapping
> connection). Если их слать — утонут реальные `ERROR`. Warnings
> остаются в файл-логе.

Планируется (Persistence-PR): hourly heartbeat с агрегатом за час,
daily summary в 00:00 UTC с топ-3 спредами, weekly paper-trading PnL.

### Формат сигнала в TG

Сигналы идут в `parse_mode=HTML`, пример:

```
ARB SPOT BTCUSDT
buy  binance @ 78120.50000000
sell bybit @ 78320.30000000
net  0.876%
```

Cooldown (по умолчанию **3 минуты**) — по ключу
`(market, symbol, buy_ex, sell_ex)`. Если тот же спред продолжает
держаться — в топике он появится один раз, а потом через 3 минуты
ещё раз (при условии, что спред не пропал из тира за это время).
Настраивается в `filters.spot.cooldown_seconds` /
`filters.perp.cooldown_seconds`.

### Надёжность

- **Очередь:** все отправки идут через bounded `asyncio.Queue` (1024
  по умолчанию). Если TG медленный — сканер не тормозит, сообщения
  ждут в очереди. Если очередь переполнилась — сообщение
  дропается + лог `ERROR telegram: queue full`.
- **Rate limit (429):** уважаем `retry_after` из ответа Telegram —
  воркер спит ровно столько, сколько API просит.
- **5xx:** экспоненциальный backoff, до 5 попыток, потом дроп.
- **4xx (кроме 429):** перманентные ошибки (неправильный chat_id,
  bad parse) — не ретраем, сразу в лог.
- **Порядок:** один воркер = FIFO, сообщения приходят в том же
  порядке, что и события.

---

## Анализ сигналов

**Статус:** 🚧 Будет с Persistence-PR.

Когда будет готово, в `data/signals.jsonl` будут **все** найденные
арбы в формате JSON-lines (одна запись = одна строка):

```json
{"ts":1700123456789,"symbol":"BTCUSDT","market":"spot","buy_ex":"bitget","sell_ex":"mexc","buy_ask":78120.5,"sell_bid":78200.3,"net_pct":8.15,"bid_qty":0.62,"ask_qty":0.45}
```

### Как анализировать

**Live-поток:**

```bash
tail -f data/signals.jsonl
```

**Сколько сигналов за день:**

```bash
wc -l data/signals.jsonl
```

**Все сигналы по одному символу:**

```bash
grep '"symbol":"BTCUSDT"' data/signals.jsonl | wc -l
```

**Только сигналы выше 5%:**

```bash
jq 'select(.net_pct > 5)' data/signals.jsonl
```

**Топ-10 спредов за сутки:**

```bash
jq -s 'sort_by(-.net_pct) | .[:10]' data/signals.jsonl
```

**Средний спред по каждой паре бирж:**

```bash
jq -s '
  group_by([.buy_ex, .sell_ex])
  | map({pair: [.[0].buy_ex, .[0].sell_ex], avg: (map(.net_pct) | add / length), count: length})
  | sort_by(-.avg)
' data/signals.jsonl
```

**Распределение сигналов по часам:**

```bash
jq -r '.ts | . / 1000 | strftime("%H")' data/signals.jsonl | sort | uniq -c
```

### Когда появится Grafana

Те же данные + гистограммы + графики трендов. Ты откроешь
`http://vps:3000` в браузере, увидишь:

- Линейный график "сигналов/час по tier'ам" за последние 24ч
- Heatmap "какие пары бирж генерируют больше арбитража в какое время"
- Список "top символов по кумулятивному спреду"
- Состояние каждого listener'а (жив/мёртв, возраст последнего тика)

---

## Backtest и replay

**Статус:** 🚧 Будет с Persistence-PR.

### Что это

Сохраняем все тики в бинарный файл `ticks.bin.zst` (msgspec +
zstd компрессия, ~250 МБ/сутки). Отдельный скрипт эмулирует
входящий поток через comparator и выдаёт сигналы, как будто это
происходит live.

### Зачем

- **Тюнинг порогов.** "Если бы я поставил `min_profit_pct = 2.5`
  вместо 3.0 — сколько бы сигналов в сутки добавилось? Какого
  качества?"
- **Проверка нового кода.** "Добавил новый фильтр по BBO notional —
  не сломал ли существующую логику?"
- **Разбор инцидентов.** "Вчера в 14:32 был жирный сигнал — почему
  бот его не заметил?"

### Как пользоваться

```bash
# Прогон за вчера с настройками из settings.live.yaml
python -m arbitrage.replay \
    --ticks data/ticks-2024-01-15.bin.zst \
    --settings settings.live.yaml \
    --output signals-replay.jsonl

# Сравнение с реальными сигналами того же дня
diff <(jq -c '.' data/signals-2024-01-15.jsonl | sort) \
     <(jq -c '.' signals-replay.jsonl | sort)
```

Скорость: ~60× realtime. Сутки проходят за ~24 минуты.

---

## Paper trading

**Статус:** ✅ Реализовано (сводные отчёты — в следующей итерации).

Открытые позиции пишутся в `data/paper_open.jsonl`, закрытые — в
`data/paper_closed.jsonl`. Каждое закрытие также эхом идёт в
Telegram info-топик с нетто-PnL и временем удержания.

### Что это

Бот находит арбы **как обычно**, но ордеров **не отправляет** — ведёт
учёт "бумажных" позиций и считает теоретический PnL. Обязательная
фаза перед реальными деньгами.

### Включение

```yaml
# settings.yaml
paper_trading:
  enabled: true

  spot:
    notional_per_leg_usd: 50     # $50 покупка на A + $50 продажа на B
    slippage_pct: 0.05           # модель slippage: -0.05% от цены

  perp:
    notional_per_leg_usd: 50     # long $50 на одной, short $50 на другой
    close_threshold_pct: 0.5     # спред упал ниже 0.5% → считаем закрытием
    max_hold_seconds: 86400      # форс-клоуз через 24ч если не сошлось
    slippage_pct: 0.03
```

### Модель spot

Spot-арбитраж **инстантный** (не держим позицию):

1. Сигнал → "купили $50 BTC на Bitget по 78100, продали $50 BTC на
   MEXC по 78260"
2. Считаем PnL мгновенно:
   `(78260 * 0.9999 - 78100 * 1.0001) * ($50/78100) - fees`
3. Пишем в `paper_trades_spot.jsonl`, шлём в TG info-топик.

### Модель perp (с tracking'ом)

Perp-арбитраж **держим позицию** пока спред не сойдётся:

1. Сигнал "spread 5.2% между Bybit-perp и Binance-perp на BTCUSDT" →
   **открываем**:
   - Long $50 на дешёвой (Bybit @ 78100)
   - Short $50 на дорогой (Binance @ 82161)
   - Пишем в `paper_trades_perp_open.jsonl` с `id`, `entry_ts`,
     `entry_spread_pct`.
2. Бот продолжает отслеживать эту пару. Каждый новый тик пересчитывает
   текущий spread.
3. **Закрытие** по одному из условий:
   - Spread упал ниже `close_threshold_pct` (0.5%) → "сошлось",
     `reason: "converged"`
   - Прошло `max_hold_seconds` (24ч) → "не сошлось", форс-клоуз,
     `reason: "expired"`
4. На закрытии:
   - Симулируем продажу long-ноги + закрытие short'а по текущим
     mid-ценам с slippage
   - Считаем PnL с учётом всех fees и slippage
   - Пишем финальную запись в `paper_trades_perp_closed.jsonl`
   - Шлём в info-топик:
     ```
     ✅ PERP ARB closed after 47min [converged]
     BTCUSDT  entry 5.2% → exit 0.3%
     long bybit 78100 → 78430  (+0.42%)
     short binance 82161 → 82180  (-0.02%)
     fees: $0.18 | slippage: $0.03
     net: +$2.31
     ```
5. Если `expired` без сходимости:
   ```
   ⏰ PERP ARB closed after 24h [expired, did NOT converge]
   BTCUSDT  entry 5.2% → exit 4.8%
   net: -$1.44  (потеря на fees + slippage, спред не сошёлся)
   ```

### Отчёты

**Ежедневно (00:00 UTC):**

```
📊 Paper PnL за сутки:
 Spot:  +$18.90 (14 инстант-сделок)
 Perp:  +$47.30 (12 закрытых, 2 ещё открыты)
        └ converged: 10 (+$52.10)
        └ expired:    2  (-$4.80)
 Total: +$66.20
```

**Еженедельно (воскресенье 00:00 UTC):**

```
📈 Paper PnL за неделю:
 Spot:  +$110.40  (winrate 62%, avg +$0.35/сделка)
 Perp:  +$320.80  (winrate 78%, avg +$4.20/сделка)
        └ converged 67/85  (avg hold 35min)
        └ expired    18/85  (потеря -$1.80 на случай)
 Total: +$431.20
```

Цель: через 1-2 недели увидеть реалистичное представление сколько
бы реально заработал. Если положительно и стабильно — переключаешь
на live. Если нет — тюнишь параметры в `settings.yaml` и гоняешь
заново.

### Что считается закрытием спреда

В `close_threshold_pct` задаёшь при каком spread считать "сошлось":

- **0.5%** (рекомендуется): близко к fee-level, реалистично для
  реального закрытия позиции.
- **0.0%** (полное схождение): может никогда не случиться на
  волатильных парах, много expired-закрытий.
- **1.0%** (консервативно): закрываешь быстрее, меньше риск что
  спред развернётся в другую сторону.

Если спред не сходится до `max_hold_seconds` (24ч) — **форс-клоуз** с
пометкой `expired`. В ежедневном отчёте видно сколько позиций
сошлось vs "зависло" — это хороший показатель качества сигналов.
Если 80% expired — значит пороги низкие, ловишь шум.

---

## Production-деплой

**Статус:** 🚧 Будет после watchdog + persistence PR.

### VPS-рекомендации

- **Регион:** AWS Tokyo (ap-northeast-1) или Singapore (ap-southeast-1).
  Все 7 бирж физически в APAC. Из Европы/США сетевой RTT 150–300мс,
  из Токио — 5–15мс.
- **Размер:** 2 vCPU / 2 GB RAM / 50 ГБ SSD. Этого с огромным запасом.
- **Провайдер:** AWS (дороже), Vultr/Linode/DigitalOcean (дешевле).
- **OS:** Ubuntu 22.04/24.04 LTS.

### systemd (одна команда — стартует)

```ini
# /etc/systemd/system/arbitrage.service
[Unit]
Description=Arbitrage Scanner
After=network.target

[Service]
Type=simple
User=arbitrage
WorkingDirectory=/opt/arbitrage
EnvironmentFile=/opt/arbitrage/.env
ExecStart=/opt/arbitrage/.venv/bin/python -m arbitrage.main
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Команды:

```bash
sudo systemctl enable arbitrage    # автостарт при ребуте
sudo systemctl start arbitrage     # запустить сейчас
sudo systemctl status arbitrage    # статус
sudo systemctl restart arbitrage   # рестарт (после изменений в settings.yaml)
sudo journalctl -u arbitrage -f    # live логи
sudo journalctl -u arbitrage --since "1 hour ago"
```

### Docker Compose (альтернатива)

Удобнее если добавим Prometheus/Grafana/Redis. Один `docker compose
up -d` поднимает весь стек.

### Обновления

```bash
cd /opt/arbitrage
git pull
sudo systemctl restart arbitrage
```

---

## Troubleshooting

### Binance возвращает HTTP 451

Гео-блок. Binance блокирует US/UK и часть EU. Перенеси бот в
AWS Tokyo/Singapore/Frankfurt — работает.

### MEXC: "Blocked! Reason: ..."

Неправильное имя канала. Код использует `aggre.bookTicker@100ms`
(протестировано). Если ты случайно правишь на `batch` или старое
имя — получишь эту ошибку.

### KuCoin: "token expired" после reconnect

Не должно случаться — код делает **новый** REST-запрос за токеном
перед каждым коннектом. Если видишь — обнови код с master.

### Heartbeat выкидывает биржу через 3 секунды

Посмотри логи по этой бирже — скорее всего она упала, и код сейчас
пытается переподключиться. Если бирж 1-2 постоянно мигают —
нормально (они реально нестабильны). Если все 7 мигают — проблема
на твоей стороне (сеть).

### `ARB` сообщений нет вообще

Проверь:

1. Все 7 бирж дают тики? `grep "connected" logs/arbitrage.log`
2. Символы есть в `prices`? (будет в будущих версиях `/status` в TG)
3. `min_profit_pct` не слишком высокий? Спреды реально редкие >5%.
4. `max_age_ms` не слишком маленький?
5. Часы машины синхронизированы? (`timedatectl status` → NTPSynchronized=yes)

### "listener died" в логах

Один из listener'ов упал с exception'ом. **С watchdog PR** бот сам
перезапустит. Сейчас — нужно рестартовать процесс целиком.

### Ошибка msgspec.ValidationError при старте

Опечатка в `settings.yaml`. Сообщение точно укажет на поле:

```
msgspec.ValidationError: Object missing required field `min_profit_pct` - at `$.filters.spot`
```

Исправь и перезапусти.

### ImportError: PyYAML

Обнови зависимости: `pip install -r requirements.txt`.

---

## FAQ

### Где лучше разместить сервер?

AWS Tokyo или Singapore. Почти все 7 бирж — в APAC. Сетевой RTT из
правильного региона — **5–15мс**. Из США/Европы — **150–300мс**.
**Геолокация бьёт любую оптимизацию кода в 10 раз.**

### Нужно ли держать капитал на каждой бирже?

Для **реального** арбитража — да, нужен "inventory on both sides":
~$10k на каждой из 7 бирж = $70k. Иначе withdrawal latency (10-60
минут перегона BTC между биржами) убьёт любой арбитраж.

Для **paper trading** — не нужно ничего, просто следи за сигналами.

### Сколько сигналов в день реально ожидать?

Сильно зависит от порогов:

- `min_profit_pct: 0.5` → сотни-тысячи в день на perp (много мусора)
- `min_profit_pct: 3.0` → десятки в день на spot (большинство жирные)
- `min_profit_pct: 5.0` → единицы в день на spot
- `min_profit_pct: 10.0` → пара в неделю, но очень серьёзные

Рынок волатильный? → больше сигналов. Тихий? → меньше.

### Spot или perp?

- **Spot:** ты покупаешь монету. Нужен full notional. Нет funding
  rate. Сигналов меньше. Хорош для исследования.
- **Perp:** фьючерсы с плечом. Нужна маржа (10×-100× меньше капитала).
  Есть funding rate (может съесть прибыль). Сигналов больше.
  **Реальные арб-боты почти все на perp.**

Рекомендация: **гоняй оба в paper mode**, смотри реальный PnL,
решай что лучше для твоей стратегии.

### Что делать если рынок упал?

Сканер не торгует — он только находит арбы. Падающий рынок →
волатильность → **больше арбов**. Ты видишь больше сигналов, но
исполнить их сложнее (быстрее уходит ликвидность).

### Могу ли запустить сканер локально на macOS/Windows?

- **macOS:** да, `uvloop` работает на Darwin.
- **Windows:** не поддерживается — `uvloop` и `picows` имеют
  ограничения. Используй WSL2.

### Как добавить новую биржу?

1. Создать `arbitrage/exchanges/<name>.py` по образцу
   `bitget.py`/`binance.py`
2. Реализовать `Listener` (subclass `picows.WSListener`)
3. Реализовать `run_<name>(prices, symbols)` с reconnect loop
4. Добавить в `arbitrage/main.py` в список task'ов
5. Добавить комиссию в `fees.spot`/`fees.perp` в settings
6. Написать тесты по образцу существующих

Сложность: от 1 часа (JSON-биржа как Bitget) до дня (Protobuf как
MEXC, или нестандартный ping как BingX).

### Можно ли сканировать 20+ пар?

Технически да, но:

- Каждая биржа имеет **лимиты на подписки** (KuCoin ~100, Binance
  ~1024, и т.д.)
- Бо́льше пар = больше тиков/сек, CPU нагрузка растёт линейно
- Сигналы размываются — труднее фильтровать интересное

На старте 3-5 самых ликвидных пар — оптимально.

### Будет ли автоматическая торговля?

В планах (executor). Сейчас сканер **только наблюдает**. Перед
подключением реального executor'а обязательно проходим через
paper trading 1-2 недели.

---

## Roadmap

### ✅ Сделано

- **PR #1** — Binance + Bybit listeners
- **PR #2** — Gate.io + Bitget + KuCoin
- **PR #3** — BingX (gzip) + MEXC (Protobuf) → 7/7 бирж spot live
- **PR #4** — typed `settings.yaml` + `.env` с msgspec-валидацией

### 🚧 В работе / следующие PR

- **Perp-листенеры** для 7 бирж + `prices_perp` + отдельный comparator
- **Telegram integration** — 2 группы × 4 топика + cooldown 3мин +
  info-топик с heartbeat/silence/reconnect/errors/daily summary
- **Rate-limit middleware** для KuCoin REST (chip) в том же PR
- **Watchdog supervisor** для listener'ов (автоперезапуск + ERROR в TG)
- **Persistence** — `signals.jsonl` + `ticks.bin.zst` с ротацией
- **Backtest/replay** — прогон исторических тиков через comparator
- **Reconnect-тесты** — state-correctness для Bybit/KuCoin/BingX/MEXC
- **Paper trading** — spot (инстант) + perp (tracking convergence до
  close_threshold_pct или max_hold_seconds)
- **Prometheus + Grafana** дашборд (опционально, TG info-топик
  покрывает большую часть)

### 📅 Позже

- **systemd unit** + deployment guide для production VPS
- **Docker compose** для стека бот+prometheus+grafana+redis
- **Process separation** — scanner и executor в разных процессах
  (нужно когда появится реальная торговля)
- **Executor module** — отправка реальных ордеров через REST/WS
  каждой биржи с HMAC-подписью
- **UserData WS** — мониторинг балансов и статусов ордеров
- **Risk management** — max position size, circuit breakers,
  kill-switch по PnL

### Не планируется (по дизайну)

- Треугольный арбитраж в пределах одной биржи
- Кросс-валютный арб (BTC/USDT vs BTC/USDC)
- HFT-уровень латентности (требует C++/Rust, не Python)
- Спот + фьючи cash-and-carry на одной бирже (отдельная стратегия)

---

## Контакты и поддержка

- Проблема? Создай issue: https://github.com/qwerty6545676-dot/test1/issues
- Вопрос по коду → смотри [README.md](README.md) (техническая часть)
- Вопрос по использованию → этот guide или FAQ выше
