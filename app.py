import os
from collections import defaultdict

from dotenv import load_dotenv

load_dotenv()

from sanic import Sanic
from sanic.response import json as json_response

import db

app = Sanic("ShoppingListAPI")

# household_code -> set of connected websockets
connections: dict[str, set] = defaultdict(set)

ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*")


@app.before_server_start
async def setup_db(app):
    await db.init_pool()


@app.after_server_stop
async def teardown_db(app):
    await db.close_pool()


@app.middleware("response")
async def add_cors_headers(request, response):
    response.headers["Access-Control-Allow-Origin"] = ALLOWED_ORIGINS
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PATCH, DELETE, OPTIONS"


@app.options("/<path:path>")
async def cors_preflight(request, path):
    return json_response({})


async def broadcast(household_code: str):
    """Push the fresh state to every connected socket for this household."""
    if household_code not in connections or not connections[household_code]:
        return
    state = await db.get_full_state(household_code)
    dead = set()
    for ws in connections[household_code]:
        try:
            await ws.send(_state_message(state))
        except Exception:
            dead.add(ws)
    connections[household_code] -= dead


def _state_message(state: dict) -> str:
    import json as _json
    return _json.dumps({"type": "state", **state})


# ---------- REST: households ----------

@app.post("/api/households")
async def create_household(request):
    code = await db.create_household()
    return json_response({"code": code})


@app.get("/api/households/<code>")
async def check_household(request, code):
    exists = await db.household_exists(code)
    if not exists:
        return json_response({"exists": False}, status=404)
    return json_response({"exists": True})


# ---------- REST: current list ----------

@app.get("/api/households/<code>/list")
async def get_list(request, code):
    code = code.upper()
    if not await db.household_exists(code):
        return json_response({"error": "household not found"}, status=404)
    state = await db.get_full_state(code)
    return json_response(state)


@app.post("/api/households/<code>/items")
async def post_item(request, code):
    code = code.upper()
    if not await db.household_exists(code):
        return json_response({"error": "household not found"}, status=404)
    body = request.json or {}
    name = (body.get("name") or "").strip()
    if not name:
        return json_response({"error": "name is required"}, status=400)
    added_by = body.get("added_by")
    await db.add_item(code, name, added_by)
    await broadcast(code)
    state = await db.get_full_state(code)
    return json_response(state)


@app.patch("/api/households/<code>/items/<item_id>")
async def patch_item(request, code, item_id):
    code = code.upper()
    body = request.json or {}
    checked = bool(body.get("checked", False))
    ok = await db.set_item_checked(code, item_id, checked)
    if not ok:
        return json_response({"error": "item not found"}, status=404)
    await broadcast(code)
    state = await db.get_full_state(code)
    return json_response(state)


@app.delete("/api/households/<code>/items/<item_id>")
async def delete_item(request, code, item_id):
    code = code.upper()
    ok = await db.delete_item(code, item_id)
    if not ok:
        return json_response({"error": "item not found"}, status=404)
    await broadcast(code)
    state = await db.get_full_state(code)
    return json_response(state)


@app.post("/api/households/<code>/list/close")
async def close_list(request, code):
    code = code.upper()
    if not await db.household_exists(code):
        return json_response({"error": "household not found"}, status=404)
    await db.close_current_list(code)
    await broadcast(code)
    state = await db.get_full_state(code)
    return json_response(state)


@app.get("/api/households/<code>/history")
async def get_history(request, code):
    code = code.upper()
    history = await db.get_history(code)
    return json_response({"history": history})


# ---------- WebSocket ----------

@app.websocket("/ws/<code>")
async def ws_handler(request, ws, code):
    code = code.upper()
    if not await db.household_exists(code):
        await ws.close(code=4404, reason="household not found")
        return

    connections[code].add(ws)
    try:
        # Send current state immediately on connect
        state = await db.get_full_state(code)
        await ws.send(_state_message(state))

        # Keep the socket open; we don't expect messages from the client,
        # but read in a loop so we notice disconnects promptly.
        async for _ in ws:
            pass
    finally:
        connections[code].discard(ws)


@app.get("/health")
async def health(request):
    return json_response({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), dev=True)
