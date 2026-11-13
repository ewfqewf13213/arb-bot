"""Скринер для GitHub Actions v3: межбиржевой (с сетями) + треугольный, 2 отдельных сообщения."""
import asyncio
import json
import os
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

import ccxt.async_support as ccxt
import arb_screener as a

TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]


def env_f(name, default):
    return float(os.getenv(name) or default)


MIN_CROSS = env_f("MIN_CROSS", 1.0)    # мин. чистая прибыль (после всех комиссий), %
MAX_CROSS = env_f("MAX_CROSS", 8.0)    # выше — считаем мусором
MIN_TRI = env_f("MIN_TRI", 0.3)
MAX_TRI = env_f("MAX_TRI", 3.0)
TRADE_USD = env_f("TRADE_USD", 300)    # сумма, на которую считаем стакан
MIN_VOL = env_f("MIN_VOLUME", 500000)  # мин. суточный объём пары, $
FEE = a.TAKER_FEE
Q = a.QUOTE

NAMES = {"binance": "Binance", "bybit": "Bybit", "okx": "OKX",
         "kucoin": "KuCoin", "gate": "Gate.io", "mexc": "MEXC"}
nm = lambda e: NAMES.get(e, e.capitalize())


# ---------- форматирование ----------
def fp(x):
    if x >= 1:
        return f"{x:,.4f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0")


def fv(x):
    if x >= 1e6:
        return f"${x/1e6:.1f}M"
    if x >= 1e3:
        return f"${x/1e3:.0f}K"
    return f"${x:.0f}"


def send(text):
    data = json.dumps({"chat_id": CHAT_ID, "text": text[:4000], "parse_mode": "HTML",
                       "disable_web_page_preview": True}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage", data=data,
        headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=20)


# ---------- стакан ----------
def walk_buy(asks, quote_amt):
    """Тратим quote_amt по стакану, возвращаем купленное количество."""
    spent = qty = 0.0
    for lvl in asks:
        p, q = lvl[0], lvl[1]
        take = min(p * q, quote_amt - spent)
        qty += take / p
        spent += take
        if spent >= quote_amt - 1e-12:
            break
    return qty if spent >= quote_amt * 0.999 and qty > 0 else None


def walk_sell(bids, base_qty):
    """Продаём base_qty по стакану, возвращаем полученную сумму."""
    got = sold = 0.0
    for lvl in bids:
        p, q = lvl[0], lvl[1]
        take = min(q, base_qty - sold)
        got += take * p
        sold += take
        if sold >= base_qty - 1e-18:
            break
    return got if sold >= base_qty * 0.999 and got > 0 else None


# ---------- биржи ----------
async def load_exchanges():
    out = {}
    for name in a.EXCHANGES:
        cfg = {"enableRateLimit": True}
        k = os.getenv(f"{name.upper()}_API_KEY")
        s = os.getenv(f"{name.upper()}_API_SECRET")
        p = os.getenv(f"{name.upper()}_API_PASSWORD")
        if k and s:
            cfg["apiKey"], cfg["secret"] = k, s
        if p:
            cfg["password"] = p
        ex = None
        try:
            ex = getattr(ccxt, name)(cfg)
            await ex.load_markets()
            out[name] = ex
        except Exception as e:
            print(f"[{name}] не загрузилась: {str(e)[:100]}")
            if ex:
                await ex.close()
    return out


async def load_currencies(exs):
    async def one(n, e):
        try:
            return n, await asyncio.wait_for(e.fetch_currencies(), 40)
        except Exception as ex:
            print(f"[{n}] currencies: {str(ex)[:80]}")
            return n, None
    res = dict(await asyncio.gather(*(one(n, e) for n, e in exs.items())))
    print("данные по сетям:", {n: bool(v) for n, v in res.items()})
    return res


def net_info(cur, bx, sx, base):
    """Выбор сети для перевода bx -> sx. status: ok | blocked | unknown."""
    cb = (cur.get(bx) or {}).get(base)
    cs = (cur.get(sx) or {}).get(base)
    if not cb or not cs:
        return {"status": "unknown", "fee": None, "network": None}
    if cb.get("active") is False or cs.get("active") is False:
        return {"status": "blocked"}
    nb, ns = cb.get("networks") or {}, cs.get("networks") or {}
    if nb and ns:
        common = [n for n in nb if n in ns]
        if not common:
            return {"status": "unknown", "fee": None, "network": None}
        good = [n for n in common
                if nb[n].get("withdraw") is not False and ns[n].get("deposit") is not False]
        if not good:
            return {"status": "blocked"}
        best = min(good, key=lambda n: (nb[n].get("fee") is None, nb[n].get("fee") or 0))
        return {"status": "ok", "fee": nb[best].get("fee"), "network": best}
    if cb.get("withdraw") is False or cs.get("deposit") is False:
        return {"status": "blocked"}
    return {"status": "ok", "fee": cb.get("fee"), "network": None}


# ---------- межбиржевой ----------
def cross_candidates(all_t):
    by = defaultdict(dict)
    for ex, tk in all_t.items():
        for sym, t in tk.items():
            if not sym.endswith(f"/{Q}") or not t.get("bid") or not t.get("ask"):
                continue
            if (t.get("quoteVolume") or 0) < MIN_VOL:
                continue
            by[sym][ex] = t
    out = []
    for sym, per in by.items():
        if len(per) < 2:
            continue
        bx = min(per, key=lambda e: per[e]["ask"])
        sx = max(per, key=lambda e: per[e]["bid"])
        if bx == sx:
            continue
        net = (per[sx]["bid"] / per[bx]["ask"] - 1) * 100 - 2 * FEE * 100
        if MIN_CROSS <= net <= MAX_CROSS:
            out.append((net, sym, bx, sx, per[bx]["quoteVolume"], per[sx]["quoteVolume"]))
    out.sort(reverse=True)
    return out[:15]


async def check_cross(exs, cur, cand):
    net0, sym, bx, sx, vb, vs = cand
    base = sym.split("/")[0]
    try:
        ob_b, ob_s = await asyncio.gather(
            exs[bx].fetch_order_book(sym, 20), exs[sx].fetch_order_book(sym, 20))
    except Exception:
        return None
    qty = walk_buy(ob_b["asks"], TRADE_USD)
    if qty is None:
        return None
    buy_avg = TRADE_USD / qty
    info = net_info(cur, bx, sx, base)
    if info["status"] == "blocked":
        return None
    wfee = info.get("fee") or 0.0
    qty_sell = qty * (1 - FEE) - wfee
    if qty_sell <= 0:
        return None
    got = walk_sell(ob_s["bids"], qty_sell)
    if got is None:
        return None
    proceeds = got * (1 - FEE)
    profit = proceeds - TRADE_USD
    pct = profit / TRADE_USD * 100
    if not (MIN_CROSS <= pct <= MAX_CROSS):
        return None
    sell_avg = got / qty_sell
    return dict(sym=sym, base=base, bx=bx, sx=sx, buy=buy_avg, sell=sell_avg,
                spread=(sell_avg / buy_avg - 1) * 100, pct=pct, profit=profit,
                vb=vb, vs=vs, info=info, wfee=wfee, wfee_usd=wfee * buy_avg)


def cross_block(r):
    i = r["info"]
    L = [f"🪙 <b>{r['sym']}</b>  ·  <b>+{r['pct']:.2f}%</b>  (≈ +${r['profit']:.2f})",
         f"🟢 Купить: <b>{nm(r['bx'])}</b> по {fp(r['buy'])}",
         f"🔴 Продать: <b>{nm(r['sx'])}</b> по {fp(r['sell'])}",
         f"📊 Спред по стакану: {r['spread']:.2f}%",
         f"📦 Объём 24ч: {fv(r['vb'])} ({nm(r['bx'])}) · {fv(r['vs'])} ({nm(r['sx'])})"]
    if i["status"] == "ok":
        fee_txt = (f"комиссия вывода {fp(r['wfee'])} {r['base']} (≈ ${r['wfee_usd']:.2f})"
                   if i.get("fee") is not None else "комиссия вывода неизвестна")
        L.append(f"🌐 Сеть: <b>{i['network'] or 'не указана'}</b> · {fee_txt}")
        L.append(f"✅ Вывод с {nm(r['bx'])} · ✅ Ввод на {nm(r['sx'])} открыты")
    else:
        L.append("⚠️ Сети, комиссия и статус ввода/вывода: нет данных — проверь вручную")
    return "\n".join(L)


# ---------- треугольный ----------
def tri_candidates(ex_name, tickers, triangles):
    out = []
    mult = (1 - FEE) ** 3
    for tri in triangles:
        amt, ok = 1.0, True
        for sym, side in tri:
            t = tickers.get(sym)
            if not t or not t.get("bid") or not t.get("ask") or \
                    (t.get("quoteVolume") or 0) < MIN_VOL / 5:
                ok = False
                break
            amt = amt / t["ask"] if side == "buy" else amt * t["bid"]
        if not ok:
            continue
        pr = (amt * mult - 1) * 100
        if MIN_TRI <= pr <= MAX_TRI:
            out.append((pr, ex_name, tri))
    out.sort(key=lambda x: -x[0])
    return out[:3]


async def check_tri(exs, cand, sem):
    _, ex_name, tri = cand
    ex = exs[ex_name]
    try:
        async with sem:
            obs = await asyncio.gather(*(ex.fetch_order_book(s, 20) for s, _ in tri))
    except Exception:
        return None
    amt, legs = TRADE_USD, []
    for (sym, side), ob in zip(tri, obs):
        if not ob["asks"] or not ob["bids"]:
            return None
        if side == "buy":
            qty = walk_buy(ob["asks"], amt)
            if qty is None:
                return None
            legs.append((sym, side, ob["asks"][0][0]))
            amt = qty * (1 - FEE)
        else:
            got = walk_sell(ob["bids"], amt)
            if got is None:
                return None
            legs.append((sym, side, ob["bids"][0][0]))
            amt = got * (1 - FEE)
    pct = (amt / TRADE_USD - 1) * 100
    if not (MIN_TRI <= pct <= MAX_TRI):
        return None
    return dict(ex=ex_name, legs=legs, end=amt, pct=pct, profit=amt - TRADE_USD)


def tri_block(r):
    path = [Q]
    lines = []
    nums = ["1️⃣", "2️⃣", "3️⃣"]
    for n, (sym, side, price) in zip(nums, r["legs"]):
        b, q = sym.split("/")
        if side == "buy":
            lines.append(f"{n} Купить <b>{b}</b> за {q}  ({sym} @ {fp(price)})")
            path.append(b)
        else:
            lines.append(f"{n} Продать <b>{b}</b> за {q}  ({sym} @ {fp(price)})")
            path.append(q)
    return "\n".join([
        f"🏦 <b>{nm(r['ex'])}</b>  ·  <b>+{r['pct']:.2f}%</b>  (≈ +${r['profit']:.2f})",
        "🔁 " + " → ".join(path)] + lines + [
        f"💰 ${TRADE_USD:.0f} → <b>${r['end']:.2f}</b>  (после 3 комиссий по {FEE*100:.2f}%)"])


# ---------- main ----------
async def main():
    exs = await load_exchanges()
    now = datetime.now(timezone.utc).strftime("%H:%M")
    try:
        all_t = await a.fetch_all_tickers(exs)
        cur = await load_currencies(exs)

        # межбиржевой
        res = await asyncio.gather(*(check_cross(exs, cur, c) for c in cross_candidates(all_t)))
        cross = sorted([r for r in res if r], key=lambda r: -r["pct"])[:5]
        if cross:
            head = (f"🔀 <b>МЕЖБИРЖЕВОЙ СПРЕД</b>\n🕒 {now} UTC · расчёт на ${TRADE_USD:.0f}\n"
                    f"Прибыль указана после комиссий торговли и вывода\n")
            foot = "\n\nℹ️ Цены меняются быстро — перед сделкой перепроверь стакан и сеть."
            send(head + "\n" + "\n\n".join(cross_block(r) for r in cross) + foot)

        # треугольный
        cands = []
        for name, tk in all_t.items():
            cands += tri_candidates(name, tk, a.build_triangles(exs[name].markets))
        sem = asyncio.Semaphore(4)
        tres = await asyncio.gather(*(check_tri(exs, c, sem) for c in cands))
        tri = sorted([r for r in tres if r], key=lambda r: -r["pct"])[:4]
        if tri:
            head = (f"🔺 <b>ТРЕУГОЛЬНЫЙ АРБИТРАЖ</b>\n🕒 {now} UTC · расчёт на ${TRADE_USD:.0f}\n"
                    f"Стакан проверен по всем трём ногам\n")
            foot = "\n\nℹ️ Такие окна живут секунды — это радар, а не гарантия."
            send(head + "\n" + "\n\n".join(tri_block(r) for r in tri) + foot)

        print(f"межбиржевых: {len(cross)}, треугольных: {len(tri)}")
    finally:
        for e in exs.values():
            await e.close()


if __name__ == "__main__":
    asyncio.run(main())
