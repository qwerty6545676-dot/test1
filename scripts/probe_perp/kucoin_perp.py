import asyncio, json, time, uuid
import aiohttp

async def main():
    # KuCoin futures has its own bullet-public on futures host.
    async with aiohttp.ClientSession() as s:
        async with s.post("https://api-futures.kucoin.com/api/v1/bullet-public") as r:
            token_resp = await r.json()
        print(f"[kucoin_perp] bullet: {json.dumps(token_resp)[:400]}")
        data = token_resp["data"]
        token = data["token"]
        inst = data["instanceServers"][0]
        connect_id = str(uuid.uuid4())
        ws_url = f"{inst['endpoint']}?token={token}&connectId={connect_id}"
        async with s.ws_connect(ws_url, heartbeat=(inst.get("pingInterval",18000)//2000)) as ws:
            print(f"[kucoin_perp] connected")
            # KuCoin futures ticker: /contractMarket/tickerV2:BTCUSDTM (USDT-margined perp)
            sub = {"id": connect_id, "type":"subscribe",
                   "topic":"/contractMarket/tickerV2:XBTUSDTM",
                   "response": True}
            await ws.send_str(json.dumps(sub))
            start = time.time(); count = 0
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    payload = json.loads(msg.data)
                    print(f"[kucoin_perp] #{count}: {json.dumps(payload)[:400]}")
                    count += 1
                    if count >= 6 or time.time()-start>10: break
            print(f"[kucoin_perp] total {count} in {time.time()-start:.1f}s")
asyncio.run(main())
