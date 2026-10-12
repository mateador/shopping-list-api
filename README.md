# Shopping List API (Sanic)

Backend for a shared, real-time shopping list. Two people, one list at a
time, per household — a REST API for reading/writing, plus a WebSocket that
pushes updates to every connected device the instant something changes.

## How it works

- **Households** are identified by a short shareable code (e.g. `Q38PQZ`),
  generated when the first person creates a list. No accounts, no passwords —
  whoever has the code can read and write that household's list. Simple by
  design, for two trusted people.
- **Exactly one list is "open" per household at a time.** Adding an item
  always adds to the currently open list, creating one automatically if none
  exists. Closing a list marks it done and immediately opens a fresh one.
- **Real-time sync via WebSocket.** Every REST mutation (add item, check
  item, close list) broadcasts the fresh state to every open WebSocket
  connection for that household. Both phones see changes within
  milliseconds — no polling.

## Run locally

You need a Postgres database. The easiest path: create a free project at
[neon.tech](https://neon.tech) — takes about a minute, no credit card — and
use the same connection string for local dev and for the Render deployment
later. (A local Postgres install works too if you already have one.)

```bash
python3 -m venv venv
source venv/bin/activate       # on Windows: venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# edit .env and paste your Neon (or local) DATABASE_URL

python app.py
```

Tables are created automatically on first startup — no migration step needed.

Test it's alive:
```bash
curl http://localhost:8000/health
```

## Deploy to Render (free)

1. Push this folder to a GitHub repo (same flow as your other two projects).
2. At [render.com](https://render.com): New → Web Service → connect this
   repo. Render reads `render.yaml` automatically for build/start commands.
3. In the Render dashboard, set the `DATABASE_URL` environment variable to
   your Neon connection string (this is the one value `render.yaml`
   deliberately leaves blank — Render will prompt you for it).
4. Once your PWA is deployed too, come back and set `ALLOWED_ORIGINS` to its
   exact URL (e.g. `https://your-shopping-list.netlify.app`) instead of `*`,
   so only your own frontend can call this API.

Render's free web service spins down after 15 minutes idle and takes
30-50 seconds to wake on the next request — completely fine for a
two-person shopping list, not fine if you were expecting instant response
at 2am every time. Your data lives in Neon (not on Render), so it's safe
across restarts, redeploys, and spin-downs — unlike Render's own free
Postgres, which expires after 30 days.

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/households` | Create a new household, returns `{code}` |
| GET | `/api/households/<code>` | Check a code is valid |
| GET | `/api/households/<code>/list` | Get the current open list + items |
| POST | `/api/households/<code>/items` | Add an item `{name, added_by?}` |
| PATCH | `/api/households/<code>/items/<id>` | Toggle checked `{checked}` |
| DELETE | `/api/households/<code>/items/<id>` | Remove an item |
| POST | `/api/households/<code>/list/close` | Close current list, open a new one |
| GET | `/api/households/<code>/history` | Past closed lists |
| POST | `/api/households/<code>/webhooks` | Register a subscriber `{url}`; returns its `secret` once |
| GET | `/api/households/<code>/webhooks` | List subscribers (id, url, active; never secrets) |
| DELETE | `/api/households/<code>/webhooks/<id>` | Deactivate a subscriber |
| POST | `/api/webhooks/<code>/events` | Inbound signed events from third parties |
| WS | `/ws/<code>` | Live state updates |

## Files

```
app.py             — routes, CORS, WebSocket connection/broadcast handling
db.py              — asyncpg pool, schema, all queries (households/lists/items/webhooks)
webhooks.py        — signing, URL checks, outbound delivery with retries
webhook_config.py  — attempts, backoff, timeout, subscription cap
tests/             — pytest suite (in-memory fake db, no network)
```

## Webhooks

Both directions use the same signature scheme: `X-Webhook-Signature:
sha256=<hex>`, where `<hex>` is the HMAC-SHA256 of the **raw request body**
using a secret. Compare in constant time.

### Outbound: get notified when an item is added

Register a subscriber (`https://` only; private, loopback and link-local
addresses are refused; max 5 active per household):

```bash
curl -X POST localhost:8000/api/households/Q38PQZ/webhooks \
  -H 'Content-Type: application/json' -d '{"url": "https://example.com/hook"}'
# -> {"id": "...", "url": "...", "active": true, "secret": "...", "inbound_secret": "..."}
```

`secret` (and `inbound_secret`, returned only with the household's first
subscriber) are shown **once** and are never returned again. Store them.

Whenever an item is added, whether through the REST route or an inbound
webhook, every active subscriber gets a `POST` in a background task (it never
delays the API response):

```json
{"type": "item.added", "id": "<event id>", "timestamp": "<ISO 8601>",
 "data": {"household": "Q38PQZ", "item": {"id": "...", "name": "Milk", ...}}}
```

with headers `X-Webhook-Signature` (signed with that subscriber's `secret`) and
`X-Webhook-Id`. Reply with any 2xx to acknowledge.

**Retries:** up to 3 attempts, waiting 1s, then 2s between them. Each request
has a 10s hard timeout and redirects are not followed. Any non-2xx response,
timeout or connection error counts as a failed attempt. After the last
attempt fails the delivery is dead-lettered: its `webhook_deliveries` row keeps
the attempt count, last status and last error, with `delivered_at` and
`next_retry_at` both empty, and nothing is retried again. Retries live in
memory, so a restart mid-retry abandons them. Tune via `webhook_config.py`
(or the `WEBHOOK_*` env vars).

### Inbound: let a third party add items

`POST /api/webhooks/<code>/events` with a body signed using the household's
`inbound_secret`:

```json
{"id": "evt-123", "type": "item.added", "data": {"name": "Milk", "added_by": "Ana"}}
```

- `401` for a missing or invalid signature (checked before the body is parsed),
  `404` for an unknown household code.
- The event ID comes from the `X-Webhook-Id` header, falling back to `id` in
  the body. Replaying an ID returns `200 {"status": "duplicate"}` and does
  nothing.
- Valid events do exactly what the REST route does: insert, WebSocket
  broadcast, outbound webhooks. Only `item.added` is supported today; new types
  are one entry in `INBOUND_EVENTS` in `app.py`.
- The inbound secret cannot currently be rotated.
- If a subscriber also posts back to this endpoint, an item.added event can
  loop; deduplicate on your side.

## Push notifications (OneSignal)

When an item is added (REST or inbound webhook) the API asks OneSignal to push
`"<added_by> added <item>"` to every device tagged `household=<code>` except
those tagged `user=<added_by>`. Without `added_by`, all the household's devices
are notified. It is one attempt in a background task; a failure is logged and
never affects the request.

1. Create a Web Push app at [onesignal.com](https://onesignal.com) for the PWA's
   URL (a separate app from any other project).
2. Set `ONESIGNAL_APP_ID` and `ONESIGNAL_API_KEY` (Settings -> Keys & IDs) in
   `.env` locally and in Render's environment. The API key is a secret.
   If either is unset, push is silently disabled.
3. The PWA must initialise OneSignal and tag each device with `household` (the
   household code) and `user` (the display name it sends as `added_by`).
   Two people with the same display name would silence each other.

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

## Notes for later

- **Household codes never expire and aren't rate-limited.** Fine for personal
  use; if you ever share this more widely, add rate limiting on
  `POST /api/households` and code guessing on the other endpoints.
- **Push notifications** are sent through OneSignal (see below), so the other
  person is notified even while the app is closed.
