import asyncio, json, time
import aiohttp

async def main():
    url = "wss://fx-ws.gateio.ws/v4/ws/usdt"
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(url) as ws:
            sub = {"time": int(time.time()), "channel":"futures.book_ticker",
                   "event":"subscribe", "payload":["BTC_USDT"]}
            await ws.send_str(json.dumps(sub))
            print(f"[gateio_perp] connected -> {url}")
            start = time.time(); count = 0
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    print(f"[gateio_perp] #{count}: {json.dumps(data)[:400]}")
                    count += 1
                    if count >= 4 or time.time()-start>10: break
            print(f"[gateio_perp] total {count} in {time.time()-start:.1f}s")
asyncio.run(main())
