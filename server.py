import asyncio
import websockets
import json
import threading
import time
import numpy as np
from scipy.stats import norm
from datetime import datetime, timezone
import websocket as ws_client
import urllib.request

clients = set()
state = {
    "strike": None,
    "bucket": None,
    "price": None,
    "history": []
}

def get_bucket_5m():
    now = datetime.now(timezone.utc)
    return (now.hour * 60 + now.minute) // 5

def secs_remaining_5m():
    now = datetime.now(timezone.utc)
    total = now.hour * 3600 + now.minute * 60 + now.second
    return 300 - (total % 300)

def calc_volatility():
    h = state["history"]
    if len(h) < 10:
        return 0.001
    returns = [np.log(h[i] / h[i-1]) for i in range(1, len(h))]
    return max(np.std(returns), 0.0001)

def calc_c(current, strike, secs_left, sigma):
    if secs_left <= 0 or strike is None:
        return None, None
    d = np.log(current / strike) / (sigma * np.sqrt(secs_left))
    return round(norm.cdf(d) * 100, 1), round((1 - norm.cdf(d)) * 100, 1)

loop = None

def broadcast_sync(data):
    if loop and clients:
        asyncio.run_coroutine_threadsafe(_broadcast(data), loop)

async def _broadcast(data):
    msg = json.dumps(data)
    dead = set()
    for client in clients:
        try:
            await client.send(msg)
        except:
            dead.add(client)
    clients.difference_update(dead)

def fetch_open_price():
    """Fetch the real current 5m candle open price from Bybit REST API"""
    try:
        url = "https://api.bybit.com/v5/market/kline?category=spot&symbol=BTCUSDT&interval=5&limit=1"
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read())
            candle = data["result"]["list"][0]
            open_price = float(candle[1])
            print(f"*** REST open price fetched: ${open_price:,.2f} ***")
            return open_price
    except Exception as e:
        print(f"REST fetch error: {e}")
        return None

def on_kline(ws, message):
    data = json.loads(message)
    if "data" not in data:
        return
    b = get_bucket_5m()
    if state["bucket"] != b:
        state["bucket"] = b
        open_price = fetch_open_price()
        if open_price:
            state["strike"] = open_price
            print(f"*** New 5m strike set: ${open_price:,.2f} ***")

def on_trade(ws, message):
    data = json.loads(message)
    if "data" not in data:
        return
    for tick in data["data"]:
        price = float(tick["p"])
        state["price"] = price
        state["history"].append(price)
        if len(state["history"]) > 300:
            state["history"].pop(0)

        strike = state["strike"]
        if strike is None:
            return

        secs_left = secs_remaining_5m()
        sigma = calc_volatility()
        c_up, c_down = calc_c(price, strike, secs_left, sigma)
        if c_up is None:
            return

        diff = round(price - strike, 2)
        direction = "UP" if price >= strike else "DOWN"
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]

        print(f"[{ts}] ${price:,.2f}  {diff:+.2f}  |  "
              f"5m: {secs_left//60}:{secs_left%60:02d}  |  "
              f"Strike: ${strike:,.2f}  |  UP: {c_up}¢  DOWN: {c_down}¢")

        broadcast_sync({
    "price": price,
    "strike": strike,
    "diff": diff,
    "secs_left": secs_left,
    "c_up": c_up,
    "c_down": c_down,
    "direction": direction,
    "ts": ts,
    "unix_ms": int(datetime.now(timezone.utc).timestamp() * 1000)
})

def connect_kline():
    ws = ws_client.WebSocketApp(
        "wss://stream.bybit.com/v5/public/spot",
        on_message=on_kline,
        on_open=lambda ws: ws.send(json.dumps({
            "op": "subscribe",
            "args": ["kline.5.BTCUSDT"]
        })),
        on_error=lambda ws, e: print(f"Kline error: {e}"),
        on_close=lambda ws, c, m: (print("Kline closed — reconnecting..."), time.sleep(3), connect_kline())
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)

def connect_trade():
    ws = ws_client.WebSocketApp(
        "wss://stream.bybit.com/v5/public/spot",
        on_message=on_trade,
        on_open=lambda ws: ws.send(json.dumps({
            "op": "subscribe",
            "args": ["publicTrade.BTCUSDT"]
        })),
        on_error=lambda ws, e: print(f"Trade error: {e}"),
        on_close=lambda ws, c, m: (print("Trade closed — reconnecting..."), time.sleep(3), connect_trade())
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)

def fetch_initial_strike():
    """On startup fetch the current candle open immediately"""
    print("Fetching initial strike from REST...")
    open_price = fetch_open_price()
    if open_price:
        state["strike"] = open_price
        state["bucket"] = get_bucket_5m()
        print(f"Initial strike set: ${open_price:,.2f}")
    else:
        print("Could not fetch initial strike — waiting for next candle...")

async def handler(websocket):
    clients.add(websocket)
    print(f"Browser connected. Clients: {len(clients)}")
    try:
        await websocket.wait_closed()
    finally:
        clients.discard(websocket)
        print(f"Browser disconnected. Clients: {len(clients)}")

async def main():
    global loop
    loop = asyncio.get_running_loop()

    fetch_initial_strike()

    threading.Thread(target=connect_kline, daemon=True).start()
    threading.Thread(target=connect_trade, daemon=True).start()

    print("Server running on ws://localhost:8765")
    async with websockets.serve(handler, "localhost", 8765):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())