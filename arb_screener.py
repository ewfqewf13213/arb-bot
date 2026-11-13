"""
Telegram-скринер арбитража: межбиржевой + треугольный.
Запуск:
    pip install ccxt aiogram
    export BOT_TOKEN=123:ABC
    python arb_screener.py
Потом напиши боту /start — он начнёт слать сигналы в этот чат.
"""
import asyncio
import os
import time
from collections import defaultdict

import ccxt.async_support as ccxt
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

# ---------------- НАСТРОЙКИ ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
EXCHANGES = ["okx", "kucoin", "gate", "mexc", "bitget", "htx"]
QUOTE = "USDT"
TAKER_FEE = 0.001          # 0.1% за сделку (поменяй под свой тариф)
SCAN_INTERVAL = 10         # секунд между сканами
COOLDOWN = 300             # не повторять один и тот же сигнал N секунд
MIN_VOLUME_USD = 100_000   # мин. суточный объём пары (отсекает неликвид)

settings = {
    "min_cross": 1.0,      # мин. чистый спред межбиржевой, %
    "min_tri": 0.3,        # мин. чистая прибыль треугольника, %
    "chat_id": None,
}
last_sent: dict[str, float] = {}
# -------------------------------------------


async def load_exchanges():
    result = {}
    for name in EXCHANGES:
        try:
            ex = getattr(ccxt, name)({"enableRateLimit": True})
            await ex.load_markets()
            result[name] = ex
        except Exception as e:
            print(f"[{name}] не загрузилась: {e}")
    return result


async def fetch_all_tickers(exchanges):
    async def one(name, ex):
        try:
            return name, await ex.fetch_tickers()
        except Exception as e:
            print(f"[{name}] tickers error: {e}")
            return name, {}

    pairs = await asyncio.gather(*(one(n, e) for n, e in exchanges.items()))
    return dict(pairs)


# ---------- МЕЖБИРЖЕВОЙ ----------
def find_cross(all_tickers):
    signals = []
    by_symbol = defaultdict(dict)
    for ex_name, tickers in all_tickers.items():
        for sym, t in tickers.items():
            if not sym.endswith(f"/{QUOTE}") or not t.get("bid") or not t.get("ask"):
                continue
            if (t.get("quoteVolume") or 0) < MIN_VOLUME_USD:
                continue
            by_symbol[sym][ex_name] = t

    for sym, per_ex in by_symbol.items():
        if len(per_ex) < 2:
            continue
        buy_ex = min(per_ex, key=lambda e: per_ex[e]["ask"])
        sell_ex = max(per_ex, key=lambda e: per_ex[e]["bid"])
        if buy_ex == sell_ex:
            continue
        ask, bid = per_ex[buy_ex]["ask"], per_ex[sell_ex]["bid"]
        gross = (bid / ask - 1) * 100
        net = gross - 2 * TAKER_FEE * 100
        if net >= settings["min_cross"]:
            signals.append((net, sym, buy_ex, ask, sell_ex, bid))
    signals.sort(reverse=True)
    return signals[:10]


# ---------- ТРЕУГОЛЬНЫЙ ----------
def build_triangles(markets):
    edges = {}
    adj = defaultdict(list)
    for m in markets.values():
        if not m.get("spot") or not m.get("active", True):
            continue
        b, q, s = m["base"], m["quote"], m["symbol"]
        edges[(q, b)] = (s, "buy")
        edges[(b, q)] = (s, "sell")
        adj[q].append(b)
        adj[b].append(q)
    tris = []
    for x in adj[QUOTE]:
        for y in adj[x]:
            if y in (QUOTE, x):
                continue
            if (y, QUOTE) in edges:
                tris.append([edges[(QUOTE, x)], edges[(x, y)], edges[(y, QUOTE)]])
    return tris


def find_triangular(ex_name, tickers, triangles):
    out = []
    fee = (1 - TAKER_FEE) ** 3
    for tri in triangles:
        amount = 1.0
        ok = True
        for sym, side in tri:
            t = tickers.get(sym)
            if not t or not t.get("bid") or not t.get("ask"):
                ok = False
                break
            if (t.get("quoteVolume") or 0) < MIN_VOLUME_USD / 5:
                ok = False
                break
            amount = amount / t["ask"] if side == "buy" else amount * t["bid"]
        if not ok:
            continue
        profit = (amount * fee - 1) * 100
        if profit >= settings["min_tri"]:
            path = f"{QUOTE} → " + " → ".join(
                s.split("/")[0] if side == "buy" else s.split("/")[1]
                for s, side in tri
            )
            out.append((profit, ex_name, path, tri))
    out.sort(reverse=True)
    return out[:5]


# ---------- ОТПРАВКА ----------
def should_send(key):
    now = time.time()
    if now - last_sent.get(key, 0) < COOLDOWN:
        return False
    last_sent[key] = now
    return True


async def scan_loop(bot: Bot, exchanges):
    triangles = {n: build_triangles(e.markets) for n, e in exchanges.items()}
    print({n: len(t) for n, t in triangles.items()}, "треугольников")
    while True:
        try:
            all_t = await fetch_all_tickers(exchanges)
            if settings["chat_id"]:
                msgs = []
                for net, sym, bx, ask, sx, bid in find_cross(all_t):
                    if should_send(f"c:{sym}:{bx}:{sx}"):
                        msgs.append(
                            f"🔀 <b>Межбиржевой</b> {sym}\n"
                            f"Купить: {bx} @ {ask:g}\n"
                            f"Продать: {sx} @ {bid:g}\n"
                            f"Чистыми: <b>{net:.2f}%</b> (без комиссий на вывод!)"
                        )
                for ex_name, tickers in all_t.items():
                    for profit, _, path, _ in find_triangular(
                        ex_name, tickers, triangles[ex_name]
                    ):
                        if should_send(f"t:{ex_name}:{path}"):
                            msgs.append(
                                f"🔺 <b>Треугольный</b> {ex_name}\n"
                                f"{path}\n"
                                f"Чистыми: <b>{profit:.2f}%</b>"
                            )
                for m in msgs:
                    await bot.send_message(settings["chat_id"], m, parse_mode="HTML")
        except Exception as e:
            print("scan error:", e)
        await asyncio.sleep(SCAN_INTERVAL)


# ---------- КОМАНДЫ ----------
dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(msg: Message):
    settings["chat_id"] = msg.chat.id
    await msg.answer(
        "Скринер запущен ✅\n"
        "/status — настройки\n"
        "/cross 1.0 — мин. спред межбиржевой, %\n"
        "/tri 0.3 — мин. прибыль треугольника, %"
    )


@dp.message(Command("status"))
async def cmd_status(msg: Message):
    await msg.answer(
        f"Биржи: {', '.join(EXCHANGES)}\n"
        f"Комиссия: {TAKER_FEE*100:.2f}%\n"
        f"Мин. межбиржевой: {settings['min_cross']}%\n"
        f"Мин. треугольник: {settings['min_tri']}%"
    )


@dp.message(Command("cross"))
async def cmd_cross(msg: Message, command: CommandObject):
    try:
        settings["min_cross"] = float(command.args)
        await msg.answer(f"Мин. межбиржевой: {settings['min_cross']}%")
    except Exception:
        await msg.answer("Пример: /cross 1.5")


@dp.message(Command("tri"))
async def cmd_tri(msg: Message, command: CommandObject):
    try:
        settings["min_tri"] = float(command.args)
        await msg.answer(f"Мин. треугольник: {settings['min_tri']}%")
    except Exception:
        await msg.answer("Пример: /tri 0.4")


async def main():
    if not BOT_TOKEN:
        raise SystemExit("Задай BOT_TOKEN в переменных окружения")
    bot = Bot(BOT_TOKEN)
    exchanges = await load_exchanges()
    asyncio.create_task(scan_loop(bot, exchanges))
    try:
        await dp.start_polling(bot)
    finally:
        for e in exchanges.values():
            await e.close()


if __name__ == "__main__":
    asyncio.run(main())
