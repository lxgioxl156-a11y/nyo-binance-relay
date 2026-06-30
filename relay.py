"""
NYO Binance Relay - v4: respuestas siempre HTTP 200 (status real va dentro del JSON)
para evitar que el proxy de Render sanitize el cuerpo de codigos 5xx.
Incluye /relay/selftest para aislar el problema sin parametros de ruta complejos.
"""
import asyncio
import json
import os
import time
import threading
import traceback

import httpx
import websocket as ws_client_lib
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="NYO Binance Relay")

BINANCE_REST_HOSTS = {
    "spot": "https://api.binance.com",
    "futures": "https://fapi.binance.com",
    "testnet": "https://testnet.binance.vision",
}
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"
BINANCE_FUTURES_WS_BASE = "wss://fstream.binance.com/ws"

@app.get("/")
def root():
    return {"service": "NYO Binance Relay", "status": "ok"}

@app.get("/relay/health")
def health():
    return {"status": "ok", "time": time.time()}

# Endpoint de diagnostico simple: sin parametros de ruta dinamicos, sync httpx
@app.get("/relay/selftest")
def selftest():
    out = {}
    try:
        with httpx.Client(timeout=10.0) as client:
            r = client.get("https://api.binance.com/api/v3/ping")
            out["binance_ping_status"] = r.status_code
            out["binance_ping_body"] = r.text[:200]
    except Exception:
        out["binance_ping_error"] = traceback.format_exc()
    out["httpx_version"] = httpx.__version__
    return out

@app.api_route("/relay/rest/{host_key}/{path:path}", methods=["GET", "POST", "DELETE", "PUT"])
async def rest_proxy(host_key: str, path: str, req: Request):
    result = {"ok": False}
    try:
        base = BINANCE_REST_HOSTS.get(host_key)
        if not base:
            return JSONResponse({"ok": False, "error": "host_key invalido"}, status_code=200)
        url = base + "/" + path
        params = dict(req.query_params)
        headers = {}
        for k, v in req.headers.items():
            if k.lower() == "x-mbx-apikey":
                headers["X-MBX-APIKEY"] = v
        body = await req.body()
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.request(req.method, url, params=params, headers=headers, content=body if body else None)
        try:
            content = r.json()
        except Exception:
            content = {"raw": r.text[:500]}
        return JSONResponse({"ok": True, "upstream_status": r.status_code, "data": content}, status_code=200)
    except Exception:
        result["traceback"] = traceback.format_exc()
        return JSONResponse(result, status_code=200)

@app.websocket("/relay/ws/{streams:path}")
async def ws_proxy(websocket: WebSocket, streams: str):
    await websocket.accept()
    loop = asyncio.get_event_loop()
    queue: asyncio.Queue = asyncio.Queue()
    upstream_url = BINANCE_WS_BASE + "?streams=" + streams
    def on_message(ws, message):
        asyncio.run_coroutine_threadsafe(queue.put(message), loop)
    def on_error(ws, error):
        asyncio.run_coroutine_threadsafe(queue.put(json.dumps({"relay_error": str(error)})), loop)
    def on_close(ws, *a):
        asyncio.run_coroutine_threadsafe(queue.put(None), loop)
    ws_client = ws_client_lib.WebSocketApp(upstream_url, on_message=on_message, on_error=on_error, on_close=on_close)
    t = threading.Thread(target=ws_client.run_forever, kwargs={"ping_interval": 20, "ping_timeout": 10})
    t.daemon = True
    t.start()
    try:
        while True:
            msg = await queue.get()
            if msg is None:
                break
            await websocket.send_text(msg)
    except WebSocketDisconnect:
        pass
    finally:
        try:
            ws_client.close()
        except Exception:
            pass

@app.websocket("/relay/wsfut/{stream_path:path}")
async def ws_futures_proxy(websocket: WebSocket, stream_path: str):
    await websocket.accept()
    loop = asyncio.get_event_loop()
    queue: asyncio.Queue = asyncio.Queue()
    upstream_url = BINANCE_FUTURES_WS_BASE + "/" + stream_path
    def on_message(ws, message):
        asyncio.run_coroutine_threadsafe(queue.put(message), loop)
    def on_close(ws, *a):
        asyncio.run_coroutine_threadsafe(queue.put(None), loop)
    ws_client = ws_client_lib.WebSocketApp(upstream_url, on_message=on_message, on_close=on_close)
    t = threading.Thread(target=ws_client.run_forever, kwargs={"ping_interval": 20, "ping_timeout": 10})
    t.daemon = True
    t.start()
    try:
        while True:
            msg = await queue.get()
            if msg is None:
                break
            await websocket.send_text(msg)
    except WebSocketDisconnect:
        pass
    finally:
        try:
            ws_client.close()
        except Exception:
            pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))