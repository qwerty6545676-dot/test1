import asyncio, json, time
import aiohttp

async def main():
    url = "wss://ws.bitget.com/v2/ws/public"
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(url) as ws:
            sub = {"op":"subscribe", "args":[
                {"instType":"USDT-FUTURES","channel":"books1","instId":"BTCUSDT"}
            ]}
            await ws.send_str(json.dumps(sub))
            print(f"[bitget_perp] connected -> {url}")
            start = time.time(); count = 0
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    print(f"[bitget_perp] #{count}: {json.dumps(data)[:400]}")
                    count += 1
                    if count >= 4 or time.time()-start>10: break
            print(f"[bitget_perp] total {count} in {time.time()-start:.1f}s")
asyncio.run(main())
