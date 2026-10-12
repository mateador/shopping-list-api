import json
import os
from collections import defaultdict

from dotenv import load_dotenv

load_dotenv()

from sanic import Sanic
from sanic.response import json as json_response

import db
import push
import webhook_config
import webhooks

from sanic_cors import CORS


app = Sanic("ShoppingListAPI")


# household_code -> set of connected websockets
connections: dict[str, set] = defaultdict(set)

ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*")
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

@app.before_server_start
async def setup_db(app):
    await db.init_pool()


@app.after_server_stop
async def teardown_db(app):
    await db.close_pool()


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
    return json.dumps({"type": "state", **state})


async def create_item(household_code: str, name: str, added_by: str | None) -> str:
    """Shared by the REST route and the inbound webhook: insert, push to
    WebSockets, then fire outbound webhooks and the OneSignal push in the
    background."""
    item_id = await db.add_item(household_code, name, added_by)
    await broadcast(household_code)
    app.add_task(webhooks.notify_item_added(household_code, item_id))
    app.add_task(push.notify_item_added(household_code, name, added_by))
    return item_id


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
    await create_item(code, name, added_by)
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


# ---------- Webhooks: management ----------

@app.post("/api/households/<code>/webhooks")
async def create_webhook(request, code):
    code = code.upper()
    if not await db.household_exists(code):
        return json_response({"error": "household not found"}, status=404)
    body = request.json or {}
    url = body.get("url")
    if not isinstance(url, str) or not url.strip():
        return json_response({"error": "url is required"}, status=400)
    url = url.strip()
    error = webhooks.validate_target_url(url)
    if error:
        return json_response({"error": error}, status=400)
    if await db.count_active_subscriptions(code) >= webhook_config.MAX_ACTIVE_SUBSCRIPTIONS:
        return json_response({"error": "too many active webhooks"}, status=400)

    secret = webhooks.new_secret()
    sub_id = await db.create_subscription(code, url, secret)
    result = {"id": sub_id, "url": url, "active": True, "secret": secret}

    # The household's inbound secret is created alongside its first
    # subscription and, like the subscription secret, shown only once.
    inbound_secret = webhooks.new_secret()
    if await db.set_webhook_secret_if_unset(code, inbound_secret):
        result["inbound_secret"] = inbound_secret
    return json_response(result)


@app.get("/api/households/<code>/webhooks")
async def list_webhooks(request, code):
    code = code.upper()
    if not await db.household_exists(code):
        return json_response({"error": "household not found"}, status=404)
    return json_response({"webhooks": await db.list_subscriptions(code)})


@app.delete("/api/households/<code>/webhooks/<webhook_id>")
async def delete_webhook(request, code, webhook_id):
    code = code.upper()
    ok = await db.deactivate_subscription(code, webhook_id)
    if not ok:
        return json_response({"error": "webhook not found"}, status=404)
    return json_response({"id": webhook_id, "active": False})


# ---------- Webhooks: inbound receiver ----------

def _parse_item_added(data) -> dict:
    """Validates an item.added payload; raises ValueError if it is bad."""
    if not isinstance(data, dict):
        raise ValueError("data must be an object")
    name = data.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("name is required")
    added_by = data.get("added_by")
    if added_by is not None and not isinstance(added_by, str):
        raise ValueError("added_by must be a string")
    return {"name": name.strip(), "added_by": added_by}


async def _apply_item_added(code: str, args: dict) -> None:
    await create_item(code, args["name"], args["added_by"])


# event type -> (validate payload, apply it). Supporting a new event type is
# one more entry here.
INBOUND_EVENTS = {
    "item.added": (_parse_item_added, _apply_item_added),
}


@app.post("/api/webhooks/<code>/events")
async def inbound_event(request, code):
    code = code.upper()
    if not await db.household_exists(code):
        return json_response({"error": "household not found"}, status=404)

    # Signature first: nothing below runs on an unverified body.
    secret = await db.get_webhook_secret(code)
    signature = request.headers.get(webhooks.SIGNATURE_HEADER)
    if not secret or not webhooks.verify_signature(secret, request.body, signature):
        return json_response({"error": "invalid signature"}, status=401)

    try:
        event = json.loads(request.body)
    except ValueError:
        return json_response({"error": "body must be valid JSON"}, status=400)
    if not isinstance(event, dict):
        return json_response({"error": "body must be a JSON object"}, status=400)

    event_id = request.headers.get(webhooks.EVENT_ID_HEADER) or event.get("id")
    if not isinstance(event_id, str) or not event_id.strip() or len(event_id) > 200:
        return json_response({"error": "event id is required"}, status=400)

    handler = INBOUND_EVENTS.get(event.get("type"))
    if handler is None:
        return json_response({"error": "unsupported event type"}, status=400)
    parse, apply = handler
    try:
        args = parse(event.get("data"))
    except ValueError as e:
        return json_response({"error": str(e)}, status=400)

    if not await db.claim_inbound_event(code, event_id):
        return json_response({"status": "duplicate"})
    try:
        await apply(code, args)
    except Exception:
        await db.release_inbound_event(code, event_id)
        raise
    return json_response({"status": "processed"})


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
