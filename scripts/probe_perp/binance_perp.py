"""Binance USDM (perp) bookTicker probe."""
import asyncio, json, time
import aiohttp

async def main():
    url = "wss://fstream.binance.com/ws/btcusdt@bookTicker"
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.ws_connect(url) as ws:
            print(f"[binance_perp] connected -> {url}")
            start = time.time()
            count = 0
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    print(f"[binance_perp] frame#{count}: {json.dumps(data)}")
                    count += 1
                    if count >= 3 or time.time() - start > 10:
                        break
            print(f"[binance_perp] got {count} frames in {time.time()-start:.1f}s")

asyncio.run(main())
