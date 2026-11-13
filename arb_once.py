"""Скринер v4 для GitHub Actions:
  1) межсетевой арбитраж: один токен в разных блокчейнах (DEX, данные DexScreener + CoinGecko)
  2) треугольный арбитраж на биржах
Каждый запуск сканирует рынок LOOPS раз с паузой INTERVAL секунд (по умолчанию 4 раза по 60 с).
Два отдельных сообщения в Telegram.
"""
import asyncio
import html
import json
import os
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

import arb_screener as a

TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]


def env_f(name, default):
    return float(os.getenv(name) or default)


# ---------- настройки ----------
LOOPS = int(env_f("LOOPS", 4))
INTERVAL = env_f("INTERVAL", 60)
REPEAT_MIN = env_f("REPEAT_MIN", 0)          # не повторять тот же сигнал N минут (0 = всегда слать)

# межсетевой
MIN_CHAIN = env_f("MIN_CHAIN", 1.0)          # мин. чистая прибыль, %
MAX_CHAIN = env_f("MAX_CHAIN", 15.0)         # выше — считаем мусором
CHAIN_USD = env_f("CHAIN_TRADE_USD", 1000)   # сумма сделки для расчёта
MIN_LIQ = env_f("MIN_LIQUIDITY", 150000)     # мин. ликвидность пула, $
MIN_DEX_VOL = env_f("MIN_DEX_VOLUME", 50000) # мин. объём пула за 24ч, $
MIN_AGE_DAYS = env_f("MIN_AGE_DAYS", 7)      # мин. возраст пула, дней
DEX_FEE = env_f("DEX_FEE", 0.3) / 100        # комиссия DEX за своп
BRIDGE_USD = env_f("BRIDGE_USD", 2.0)        # ориентир комиссии моста, $
CG_KEY = os.getenv("COINGECKO_KEY", "")      # необязательный бесплатный ключ CoinGecko

# треугольный
MIN_TRI = env_f("MIN_TRI", 0.3)
MAX_TRI = env_f("MAX_TRI", 3.0)
TRADE_USD = env_f("TRADE_USD", 300)
MIN_VOL = env_f("MIN_VOLUME", 500000)
FEE = a.TAKER_FEE
Q = a.QUOTE

# CoinGecko platform id -> (DexScreener chainId, название, ориентир газа на 1 своп, $)
CHAINS = {
    "ethereum": ("ethereum", "Ethereum", 8.0),
    "binance-smart-chain": ("bsc", "BSC", 0.3),
    "solana": ("solana", "Solana", 0.05),
    "arbitrum-one": ("arbitrum", "Arbitrum", 0.4),
    "base": ("base", "Base", 0.15),
    "polygon-pos": ("polygon", "Polygon", 0.1),
    "optimistic-ethereum": ("optimism", "Optimism", 0.3),
    "avalanche": ("avalanche", "Avalanche", 0.3),
}
GAS = {v[0]: v[2] for v in CHAINS.values()}
CNAME = {v[0]: v[1] for v in CHAINS.values()}
NAMES = {"binance": "Binance", "bybit": "Bybit", "okx": "OKX",
         "kucoin": "KuCoin", "gate": "Gate.io", "mexc": "MEXC"}
nm = lambda e: NAMES.get(e, e.capitalize())


# ---------- утилиты ----------
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


def http_json(url, headers=None, tries=3, wait=15):
    h = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    if headers:
        h.update(headers)
    for k in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and k < tries - 1:
                time.sleep(wait * (k + 1))
                continue
            print("HTTP", e.code, url[:90])
            return None
        except Exception as e:
            if k < tries - 1:
                time.sleep(3)
                continue
            print("ERR", str(e)[:80], url[:60])
            return None


sent_at = {}


def fresh(key):
    if REPEAT_MIN <= 0:
        return True
    t = time.time()
    if t - sent_at.get(key, 0) < REPEAT_MIN * 60:
        return False
    sent_at[key] = t
    return True


# ---------- стакан ----------
def walk_buy(asks, quote_amt):
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
    got = sold = 0.0
    for lvl in bids:
        p, q = lvl[0], lvl[1]
        take = min(q, base_qty - sold)
        got += take * p
        sold += take
        if sold >= base_qty - 1e-18:
            break
    return got if sold >= base_qty * 0.999 and got > 0 else None


# ================= МЕЖСЕТЕВОЙ (DEX) =================
def norm(chain, addr):
    return addr if chain == "solana" else addr.lower()


def build_chain_map():
    """Токены из топа по объёму, которые есть минимум в двух поддерживаемых сетях."""
    hdr = {"x-cg-demo-api-key": CG_KEY} if CG_KEY else None
    base = "https://api.coingecko.com/api/v3"
    lst = http_json(f"{base}/coins/list?include_platform=true", hdr)
    if not isinstance(lst, list):
        return None
    top = set()
    for page in (1, 2):
        m = http_json(f"{base}/coins/markets?vs_currency=usd&order=volume_desc"
                      f"&per_page=250&page={page}", hdr)
        if isinstance(m, list):
            top |= {c["id"] for c in m}
    if not top:
        return None
    out = {}
    for c in lst:
        if c.get("id") not in top:
            continue
        pl = {}
        for cg, addr in (c.get("platforms") or {}).items():
            if cg in CHAINS and addr:
                ch = CHAINS[cg][0]
                pl[ch] = norm(ch, addr)
        if len(pl) >= 2:
            out[c["id"]] = {"sym": (c.get("symbol") or "").upper(), "chains": pl}
    print(f"токенов в 2+ сетях: {len(out)}")
    return out


def pick_pools(responses, lookup):
    """Лучший (по ликвидности) пул токена в каждой сети с учётом фильтров."""
    best = {}
    now_ms = time.time() * 1000
    for d in responses:
        pairs = d if isinstance(d, list) else ((d or {}).get("pairs") or [])
        for p in pairs:
            try:
                ch = p["chainId"]
                cid = lookup.get((ch, norm(ch, p["baseToken"]["address"])))
                if not cid:
                    continue
                price = float(p["priceUsd"])
                liq = float((p.get("liquidity") or {}).get("usd") or 0)
                vol = float((p.get("volume") or {}).get("h24") or 0)
                created = p.get("pairCreatedAt")
                if price <= 0 or liq < MIN_LIQ or vol < MIN_DEX_VOL:
                    continue
                if created and (now_ms - created) / 86400000 < MIN_AGE_DAYS:
                    continue
                cur = best.get((cid, ch))
                if not cur or liq > cur["liq"]:
                    best[(cid, ch)] = dict(price=price, liq=liq, vol=vol, chain=ch,
                                           dex=p.get("dexId", ""), url=p.get("url", ""))
            except Exception:
                continue
    return best


def chain_net(buy, sell):
    T = CHAIN_USD
    slip_b = min(T / (buy["liq"] / 2), 1.0)
    slip_s = min(T / (sell["liq"] / 2), 1.0)
    tokens = T * (1 - DEX_FEE) / (buy["price"] * (1 + slip_b))
    proceeds = tokens * sell["price"] * (1 - slip_s) * (1 - DEX_FEE)
    gas = GAS.get(buy["chain"], 1.0) + GAS.get(sell["chain"], 1.0)
    profit = proceeds - T - gas - BRIDGE_USD
    return dict(profit=profit, pct=profit / T * 100, gas=gas,
                slip=(slip_b + slip_s) * 100, fees=2 * DEX_FEE * 100)


def find_chain_signals(best, cmap):
    by = defaultdict(list)
    for (cid, _), v in best.items():
        by[cid].append(v)
    out = []
    for cid, items in by.items():
        if len(items) < 2:
            continue
        buy = min(items, key=lambda x: x["price"])
        sell = max(items, key=lambda x: x["price"])
        spread = (sell["price"] / buy["price"] - 1) * 100
        if spread <= 0 or spread > MAX_CHAIN + 3:
            continue
        n = chain_net(buy, sell)
        if MIN_CHAIN <= n["pct"] <= MAX_CHAIN:
            out.append(dict(cid=cid, sym=cmap[cid]["sym"], buy=buy, sell=sell,
                            spread=spread, **n))
    out.sort(key=lambda r: -r["pct"])
    return out


async def scan_chains(cmap, sem):
    lookup = {(ch, ad): cid for cid, v in cmap.items() for ch, ad in v["chains"].items()}
    addrs = sorted({ad for (_, ad) in lookup})
    chunks = [addrs[i:i + 30] for i in range(0, len(addrs), 30)]

    async def one(chunk):
        async with sem:
            d = await asyncio.to_thread(
                http_json, "https://api.dexscreener.com/latest/dex/tokens/" + ",".join(chunk),
                None, 2, 10)
            await asyncio.sleep(0.2)
            return d

    res = await asyncio.gather(*(one(c) for c in chunks))
    return find_chain_signals(pick_pools(res, lookup), cmap)


def chain_block(r):
    b, s = r["buy"], r["sell"]
    link = lambda p, t: f'<a href="{html.escape(p["url"])}">{t}</a>' if p["url"] else t
    return "\n".join([
        f"🪙 <b>{html.escape(r['sym'])}</b>  ·  <b>+{r['pct']:.2f}%</b>  (≈ +${r['profit']:.0f} на ${CHAIN_USD:.0f})",
        f"🟢 Купить: <b>{CNAME.get(b['chain'], b['chain'])}</b> · ${fp(b['price'])}",
        f"     {html.escape(b['dex'])} · ликвидность {fv(b['liq'])} · {link(b, 'пул')}",
        f"🔴 Продать: <b>{CNAME.get(s['chain'], s['chain'])}</b> · ${fp(s['price'])}",
        f"     {html.escape(s['dex'])} · ликвидность {fv(s['liq'])} · {link(s, 'пул')}",
        f"📊 Разница цен: {r['spread']:.2f}%",
        f"🧾 Издержки (оценка): DEX {r['fees']:.1f}% · проскальзывание ~{r['slip']:.1f}% · "
        f"газ ≈ ${r['gas']:.1f} · мост ≈ ${BRIDGE_USD:.0f}",
    ])


# ================= ТРЕУГОЛЬНЫЙ =================
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
    path, lines = [Q], []
    for n, (sym, side, price) in zip(["1️⃣", "2️⃣", "3️⃣"], r["legs"]):
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


# ================= main =================
async def scan_once(exs, triangles, cmap, sem):
    now = datetime.now(timezone.utc).strftime("%H:%M")

    if cmap:
        try:
            sigs = [r for r in await scan_chains(cmap, sem)
                    if fresh(f"c:{r['cid']}:{r['buy']['chain']}:{r['sell']['chain']}")][:5]
            if sigs:
                head = (f"🌉 <b>МЕЖСЕТЕВОЙ АРБИТРАЖ (DEX)</b>\n🕒 {now} UTC · расчёт на ${CHAIN_USD:.0f}\n"
                        f"Прибыль — оценка после комиссий DEX, проскальзывания, газа и моста\n")
                foot = ("\n\nℹ️ Газ и мост — ориентиры, проверь реальные цифры. "
                        "Перевод занимает минуты, цена может уйти.")
                send(head + "\n" + "\n\n".join(chain_block(r) for r in sigs) + foot)
            print("межсетевых:", len(sigs))
        except Exception as e:
            print("chain error:", e)

    try:
        all_t = await a.fetch_all_tickers(exs)
        cands = []
        for name, tk in all_t.items():
            cands += tri_candidates(name, tk, triangles[name])
        res = await asyncio.gather(*(check_tri(exs, c, sem) for c in cands))
        tri = sorted([r for r in res if r and fresh(f"t:{r['ex']}:{r['legs'][0][0]}:{r['legs'][1][0]}:{r['legs'][2][0]}")],
                     key=lambda r: -r["pct"])[:4]
        if tri:
            head = (f"🔺 <b>ТРЕУГОЛЬНЫЙ АРБИТРАЖ</b>\n🕒 {now} UTC · расчёт на ${TRADE_USD:.0f}\n"
                    f"Стакан проверен по всем трём ногам\n")
            foot = "\n\nℹ️ Такие окна живут секунды — это радар, а не гарантия."
            send(head + "\n" + "\n\n".join(tri_block(r) for r in tri) + foot)
        print("треугольных:", len(tri))
    except Exception as e:
        print("tri error:", e)


async def main():
    exs = await a.load_exchanges()
    triangles = {n: a.build_triangles(e.markets) for n, e in exs.items()}
    cmap = await asyncio.to_thread(build_chain_map)
    if not cmap:
        try:
            send("⚠️ Межсетевой: CoinGecko не ответил (лимит запросов). "
                 "Треугольный работает. Добавь бесплатный ключ COINGECKO_KEY в Secrets.")
        except Exception:
            pass
    sem = asyncio.Semaphore(4)
    try:
        for i in range(LOOPS):
            t0 = time.time()
            await scan_once(exs, triangles, cmap, sem)
            if i < LOOPS - 1:
                await asyncio.sleep(max(1.0, INTERVAL - (time.time() - t0)))
    finally:
        for e in exs.values():
            await e.close()


if __name__ == "__main__":
    asyncio.run(main())
