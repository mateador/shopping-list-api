import os
import uuid
import random
import string
from datetime import date, datetime, timezone

import asyncpg

DATABASE_URL = os.environ["DATABASE_URL"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS households (
    code TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shopping_lists (
    id TEXT PRIMARY KEY,
    household_code TEXT NOT NULL REFERENCES households(code) ON DELETE CASCADE,
    list_date DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_lists_household_status
    ON shopping_lists (household_code, status);

CREATE TABLE IF NOT EXISTS items (
    id TEXT PRIMARY KEY,
    list_id TEXT NOT NULL REFERENCES shopping_lists(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    checked BOOLEAN NOT NULL DEFAULT false,
    added_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_items_list ON items (list_id);

ALTER TABLE households ADD COLUMN IF NOT EXISTS webhook_secret TEXT;

CREATE TABLE IF NOT EXISTS webhook_inbound_events (
    household_code TEXT NOT NULL REFERENCES households(code) ON DELETE CASCADE,
    event_id TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (household_code, event_id)
);

CREATE TABLE IF NOT EXISTS webhook_subscriptions (
    id TEXT PRIMARY KEY,
    household_code TEXT NOT NULL REFERENCES households(code) ON DELETE CASCADE,
    url TEXT NOT NULL,
    secret TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_webhook_subs_household
    ON webhook_subscriptions (household_code, active);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    subscription_id TEXT NOT NULL REFERENCES webhook_subscriptions(id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_status INTEGER,
    last_error TEXT,
    next_retry_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, subscription_id)
);
"""

_pool: asyncpg.Pool | None = None


async def init_pool():
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with _pool.acquire() as conn:
        await conn.execute(SCHEMA)
    return _pool


async def close_pool():
    if _pool:
        await _pool.close()


def pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized")
    return _pool


def new_id() -> str:
    return uuid.uuid4().hex


CODE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"  # no 0/O/1/I/L to avoid confusion


def new_household_code(length: int = 6) -> str:
    return "".join(random.choice(CODE_ALPHABET) for _ in range(length))


# ---------- households ----------

async def create_household() -> str:
    async with pool().acquire() as conn:
        for _ in range(5):
            code = new_household_code()
            exists = await conn.fetchval("SELECT 1 FROM households WHERE code = $1", code)
            if not exists:
                await conn.execute("INSERT INTO households (code) VALUES ($1)", code)
                return code
        raise RuntimeError("Could not generate a unique household code")


async def household_exists(code: str) -> bool:
    async with pool().acquire() as conn:
        row = await conn.fetchval("SELECT 1 FROM households WHERE code = $1", code.upper())
        return bool(row)


# ---------- lists + items ----------

async def get_or_create_open_list(household_code: str) -> str:
    """Returns the id of the currently open list, creating one if none exists."""
    async with pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id FROM shopping_lists WHERE household_code = $1 AND status = 'open'",
            household_code,
        )
        if row:
            return row["id"]

        list_id = new_id()
        await conn.execute(
            """INSERT INTO shopping_lists (id, household_code, list_date, status)
               VALUES ($1, $2, $3, 'open')""",
            list_id,
            household_code,
            date.today(),
        )
        return list_id


async def get_full_state(household_code: str) -> dict:
    list_id = await get_or_create_open_list(household_code)
    async with pool().acquire() as conn:
        list_row = await conn.fetchrow(
            "SELECT id, list_date, status, created_at FROM shopping_lists WHERE id = $1",
            list_id,
        )
        item_rows = await conn.fetch(
            """SELECT id, name, checked, added_by, created_at FROM items
               WHERE list_id = $1 ORDER BY created_at ASC""",
            list_id,
        )
    return {
        "list": {
            "id": list_row["id"],
            "date": list_row["list_date"].isoformat(),
            "status": list_row["status"],
            "created_at": list_row["created_at"].isoformat(),
        },
        "items": [
            {
                "id": r["id"],
                "name": r["name"],
                "checked": r["checked"],
                "added_by": r["added_by"],
                "created_at": r["created_at"].isoformat(),
            }
            for r in item_rows
        ],
    }


async def add_item(household_code: str, name: str, added_by: str | None) -> str:
    list_id = await get_or_create_open_list(household_code)
    item_id = new_id()
    async with pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO items (id, list_id, name, added_by)
               VALUES ($1, $2, $3, $4)""",
            item_id,
            list_id,
            name,
            added_by,
        )
    return item_id


async def set_item_checked(household_code: str, item_id: str, checked: bool) -> bool:
    """Returns False if the item doesn't belong to this household (safety check)."""
    async with pool().acquire() as conn:
        row = await conn.fetchrow(
            """SELECT items.id FROM items
               JOIN shopping_lists ON shopping_lists.id = items.list_id
               WHERE items.id = $1 AND shopping_lists.household_code = $2""",
            item_id,
            household_code,
        )
        if not row:
            return False
        await conn.execute("UPDATE items SET checked = $1 WHERE id = $2", checked, item_id)
        return True


async def delete_item(household_code: str, item_id: str) -> bool:
    async with pool().acquire() as conn:
        result = await conn.execute(
            """DELETE FROM items USING shopping_lists
               WHERE items.id = $1
                 AND shopping_lists.id = items.list_id
                 AND shopping_lists.household_code = $2""",
            item_id,
            household_code,
        )
        return result != "DELETE 0"


async def close_current_list(household_code: str) -> str:
    """Marks the open list done and immediately opens a fresh one. Returns new list id."""
    async with pool().acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT id FROM shopping_lists WHERE household_code = $1 AND status = 'open'",
                household_code,
            )
            if row:
                await conn.execute(
                    """UPDATE shopping_lists SET status = 'done', closed_at = $1
                       WHERE id = $2""",
                    datetime.now(timezone.utc),
                    row["id"],
                )
    # get_or_create_open_list will now create the new one
    return await get_or_create_open_list(household_code)


async def get_history(household_code: str, limit: int = 20) -> list[dict]:
    async with pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, list_date, status, created_at, closed_at
               FROM shopping_lists
               WHERE household_code = $1 AND status = 'done'
               ORDER BY closed_at DESC LIMIT $2""",
            household_code,
            limit,
        )
        result = []
        for r in rows:
            items = await conn.fetch(
                "SELECT name, checked FROM items WHERE list_id = $1 ORDER BY created_at ASC",
                r["id"],
            )
            result.append(
                {
                    "id": r["id"],
                    "date": r["list_date"].isoformat(),
                    "closed_at": r["closed_at"].isoformat() if r["closed_at"] else None,
                    "items": [{"name": i["name"], "checked": i["checked"]} for i in items],
                }
            )
        return result


# ---------- webhooks ----------

async def get_item(household_code: str, item_id: str) -> dict | None:
    async with pool().acquire() as conn:
        r = await conn.fetchrow(
            """SELECT items.id, items.list_id, items.name, items.checked,
                      items.added_by, items.created_at
               FROM items
               JOIN shopping_lists ON shopping_lists.id = items.list_id
               WHERE items.id = $1 AND shopping_lists.household_code = $2""",
            item_id,
            household_code,
        )
    if not r:
        return None
    return {
        "id": r["id"],
        "list_id": r["list_id"],
        "name": r["name"],
        "checked": r["checked"],
        "added_by": r["added_by"],
        "created_at": r["created_at"].isoformat(),
    }


async def get_webhook_secret(household_code: str) -> str | None:
    async with pool().acquire() as conn:
        return await conn.fetchval(
            "SELECT webhook_secret FROM households WHERE code = $1", household_code
        )


async def set_webhook_secret_if_unset(household_code: str, secret: str) -> bool:
    """Returns True if this call set the secret (i.e. it was previously unset)."""
    async with pool().acquire() as conn:
        row = await conn.fetchval(
            """UPDATE households SET webhook_secret = $1
               WHERE code = $2 AND webhook_secret IS NULL
               RETURNING code""",
            secret,
            household_code,
        )
        return bool(row)


async def claim_inbound_event(household_code: str, event_id: str) -> bool:
    """Returns False if this event ID was already processed for the household."""
    async with pool().acquire() as conn:
        result = await conn.execute(
            """INSERT INTO webhook_inbound_events (household_code, event_id)
               VALUES ($1, $2) ON CONFLICT DO NOTHING""",
            household_code,
            event_id,
        )
        return result == "INSERT 0 1"


async def release_inbound_event(household_code: str, event_id: str) -> None:
    """Un-claims an event whose processing failed, so the sender can retry it."""
    async with pool().acquire() as conn:
        await conn.execute(
            "DELETE FROM webhook_inbound_events WHERE household_code = $1 AND event_id = $2",
            household_code,
            event_id,
        )


async def create_subscription(household_code: str, url: str, secret: str) -> str:
    sub_id = new_id()
    async with pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO webhook_subscriptions (id, household_code, url, secret)
               VALUES ($1, $2, $3, $4)""",
            sub_id,
            household_code,
            url,
            secret,
        )
    return sub_id


async def count_active_subscriptions(household_code: str) -> int:
    async with pool().acquire() as conn:
        return await conn.fetchval(
            "SELECT count(*) FROM webhook_subscriptions WHERE household_code = $1 AND active",
            household_code,
        )


async def list_subscriptions(household_code: str) -> list[dict]:
    """Never selects the secret."""
    async with pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, url, active FROM webhook_subscriptions
               WHERE household_code = $1 ORDER BY created_at ASC""",
            household_code,
        )
    return [{"id": r["id"], "url": r["url"], "active": r["active"]} for r in rows]


async def get_active_subscriptions(household_code: str) -> list[dict]:
    async with pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, url, secret FROM webhook_subscriptions
               WHERE household_code = $1 AND active""",
            household_code,
        )
    return [{"id": r["id"], "url": r["url"], "secret": r["secret"]} for r in rows]


async def deactivate_subscription(household_code: str, sub_id: str) -> bool:
    async with pool().acquire() as conn:
        result = await conn.execute(
            """UPDATE webhook_subscriptions SET active = false
               WHERE id = $1 AND household_code = $2""",
            sub_id,
            household_code,
        )
        return result != "UPDATE 0"


async def create_delivery(event_id: str, subscription_id: str, payload_json: str) -> str:
    delivery_id = new_id()
    async with pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO webhook_deliveries (id, event_id, subscription_id, payload)
               VALUES ($1, $2, $3, $4::jsonb)""",
            delivery_id,
            event_id,
            subscription_id,
            payload_json,
        )
    return delivery_id


async def record_delivery_attempt(
    delivery_id: str,
    attempt_count: int,
    status: int | None,
    error: str | None,
    next_retry_at: datetime | None,
    delivered_at: datetime | None,
) -> None:
    async with pool().acquire() as conn:
        await conn.execute(
            """UPDATE webhook_deliveries
               SET attempt_count = $2, last_status = $3, last_error = $4,
                   next_retry_at = $5, delivered_at = $6
               WHERE id = $1""",
            delivery_id,
            attempt_count,
            status,
            error,
            next_retry_at,
            delivered_at,
        )
