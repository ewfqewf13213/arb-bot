"""Разовый скан для GitHub Actions. Использует логику из arb_screener.py"""
import asyncio
import json
import os
import urllib.request

import arb_screener as a

TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]
a.settings["min_cross"] = float(os.getenv("MIN_CROSS", "1.0"))
a.settings["min_tri"] = float(os.getenv("MIN_TRI", "0.3"))


def send(text):
    data = json.dumps({"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=20)


async def main():
    exchanges = await a.load_exchanges()
    try:
        all_t = await a.fetch_all_tickers(exchanges)
        lines = []
        for net, sym, bx, ask, sx, bid in a.find_cross(all_t):
            lines.append(
                f"🔀 <b>{sym}</b>: купить {bx} @ {ask:g} → продать {sx} @ {bid:g} "
                f"= <b>{net:.2f}%</b>"
            )
        for name, tickers in all_t.items():
            tris = a.build_triangles(exchanges[name].markets)
            for profit, _, path, _ in a.find_triangular(name, tickers, tris):
                lines.append(f"🔺 {name}: {path} = <b>{profit:.2f}%</b>")
        if lines:
            send("\n".join(lines[:15]))
        print(f"найдено сигналов: {len(lines)}")
    finally:
        for e in exchanges.values():
            await e.close()


asyncio.run(main())
