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
