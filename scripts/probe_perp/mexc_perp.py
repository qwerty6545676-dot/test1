import asyncio, json, time
import aiohttp

async def main():
    # MEXC contract (perp) has its own WS host with JSON payloads (not protobuf like spot).
    url = "wss://contract.mexc.com/edge"
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(url) as ws:
            sub = {"method":"sub.ticker","param":{"symbol":"BTC_USDT"}}
            await ws.send_str(json.dumps(sub))
            print(f"[mexc_perp] connected -> {url}")
            start = time.time(); count = 0
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    print(f"[mexc_perp] #{count}: {json.dumps(data)[:400]}")
                    count += 1
                    if count >= 4 or time.time()-start>10: break
            print(f"[mexc_perp] total {count} in {time.time()-start:.1f}s")
asyncio.run(main())
