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
| WS | `/ws/<code>` | Live state updates |

## Files

```
app.py    — routes, CORS, WebSocket connection/broadcast handling
db.py     — asyncpg pool, schema, all queries (households/lists/items)
```

## Notes for later

- **Household codes never expire and aren't rate-limited.** Fine for personal
  use; if you ever share this more widely, add rate limiting on
  `POST /api/households` and code guessing on the other endpoints.
- **No push notifications here** — the WebSocket already gives you real-time
  sync while the app is open. If you want a notification when your wife adds
  something while your phone's closed, that's a separate feature (would need
  the OneSignal-style setup from the habit tracker).
