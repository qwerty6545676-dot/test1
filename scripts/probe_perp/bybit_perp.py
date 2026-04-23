import asyncio, json, time
import aiohttp

async def main():
    url = "wss://stream.bybit.com/v5/public/linear"
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(url, heartbeat=18) as ws:
            await ws.send_str(json.dumps({"op":"subscribe","args":["orderbook.1.BTCUSDT"]}))
            print(f"[bybit_perp] connected -> {url}")
            start = time.time(); count = 0
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    print(f"[bybit_perp] #{count}: {json.dumps(data)[:400]}")
                    count += 1
                    if count >= 4 or time.time() - start > 10: break
            print(f"[bybit_perp] total {count} in {time.time()-start:.1f}s")
asyncio.run(main())
