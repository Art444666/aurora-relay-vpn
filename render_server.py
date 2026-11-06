import asyncio
import json
import os

from aiohttp import WSMsgType, web


async def index(request):
    return web.Response(text="aurora relay ok")


async def pump_tcp(ws, reader):
    try:
        while True:
            data = await reader.read(16384)
            if not data:
                break
            await ws.send_bytes(data)
    except Exception:
        pass
    finally:
        try:
            await ws.close()
        except Exception:
            pass


async def tunnel(request):
    ws = web.WebSocketResponse(heartbeat=25, max_msg_size=0)
    await ws.prepare(request)
    writer = None
    pump = None
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT and writer is None:
                data = json.loads(msg.data)
                reader, writer = await asyncio.open_connection(
                    data["host"], int(data["port"]))
                await ws.send_str("OK")
                pump = asyncio.create_task(pump_tcp(ws, reader))
            elif msg.type == WSMsgType.BINARY and writer is not None:
                writer.write(msg.data)
                await writer.drain()
            elif msg.type == WSMsgType.ERROR:
                break
    except Exception:
        pass
    finally:
        if pump:
            pump.cancel()
        if writer:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
    return ws


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/tunnel", tunnel)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
