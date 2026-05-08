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
import os

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
    try:
        url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=1"
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read())
            open_price = float(data[0][1])
            print(f"*** REST open price fetched: ${open_price:,.2f} ***")
            return open_price
    except Exception as e:
        print(f"REST fetch error: {e}")
        return None

def fetch_initial_strike():
    print("Fetching initial strike from REST...")
    open_price = fetch_open_price()
    if open_price:
        state["strike"] = open_price
        state["bucket"] = get_bucket_5m()
        print(f"Initial strike set: ${open_price:,.2f}")
    else:
        print("Could not fetch initial strike — waiting for next candle...")

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
    if not data or "k" not in data:
        return

    kline = data["k"]
    price = float(kline["c"])

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
        "wss://stream.binance.com:9443/ws/btcusdt@kline_5m",
        on_message=on_kline,
        on_open=lambda ws: print("Kline connected..."),
        on_error=lambda ws, e: print(f"Kline error: {e}"),
        on_close=lambda ws, c, m: (print("Kline closed — reconnecting..."), time.sleep(3), connect_kline())
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)

def connect_trade():
    ws = ws_client.WebSocketApp(
        "wss://stream.binance.com:9443/ws/btcusdt@kline_1m",
        on_message=on_trade,
        on_open=lambda ws: print("Trade stream connected..."),
        on_error=lambda ws, e: print(f"Trade error: {e}"),
        on_close=lambda ws, c, m: (print("Trade closed — reconnecting..."), time.sleep(3), connect_trade())
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)

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

    PORT = int(os.environ.get("PORT", 8765))
    print(f"Server running on port {PORT}")
    async with websockets.serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())