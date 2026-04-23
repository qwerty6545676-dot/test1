import asyncio, gzip, json, time
import aiohttp

async def main():
    url = "wss://open-api-swap.bingx.com/swap-market"
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(url) as ws:
            sub = {"id":"probe","reqType":"sub","dataType":"BTC-USDT@bookTicker"}
            await ws.send_str(json.dumps(sub))
            print(f"[bingx_perp] connected -> {url}")
            start = time.time(); count = 0
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    try:
                        raw = gzip.decompress(msg.data).decode()
                    except Exception:
                        raw = repr(msg.data[:80])
                    if raw.startswith("Ping") or '"ping"' in raw.lower()[:10]:
                        print(f"[bingx_perp] ping raw='{raw}'")
                        # Per BingX docs: respond with "Pong"
                        await ws.send_str("Pong")
                        continue
                    try:
                        data = json.loads(raw)
                        print(f"[bingx_perp] #{count}: {json.dumps(data)[:400]}")
                    except Exception:
                        print(f"[bingx_perp] #{count}: raw={raw[:400]}")
                    count += 1
                    if count >= 4 or time.time()-start>10: break
                elif msg.type == aiohttp.WSMsgType.TEXT:
                    print(f"[bingx_perp] text: {msg.data[:400]}")
            print(f"[bingx_perp] total {count} in {time.time()-start:.1f}s")
asyncio.run(main())
