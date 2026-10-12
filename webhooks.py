import asyncio
import hashlib
import hmac
import ipaddress
import json
import logging
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import httpx

import db
import webhook_config as cfg

logger = logging.getLogger(__name__)

SIGNATURE_HEADER = "X-Webhook-Signature"
EVENT_ID_HEADER = "X-Webhook-Id"

# Indirection so tests can skip the real backoff wait.
_sleep = asyncio.sleep


# ---------- signing ----------

def new_secret() -> str:
    return secrets.token_hex(32)


def sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify_signature(secret: str, body: bytes, header: str | None) -> bool:
    if not header:
        return False
    return hmac.compare_digest(sign(secret, body).encode(), header.strip().encode())


# ---------- subscription URLs ----------

def validate_target_url(url: str) -> str | None:
    """Returns an error message, or None if the URL is acceptable.

    Subscribers are registered with nothing but a household code, so refuse
    anything that would make the server call into its own network.
    """
    if len(url) > cfg.MAX_URL_LENGTH:
        return "url is too long"
    try:
        parsed = urlparse(url)
        parsed.port  # raises ValueError on a malformed port
    except ValueError:
        return "url is invalid"
    if parsed.scheme != "https" or not parsed.hostname:
        return "url must be an https:// URL"
    host = parsed.hostname.lower()
    if host == "localhost" or host.endswith(".localhost"):
        return "url must not point to a local address"
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return None  # a hostname, not an IP literal
    if not ip.is_global:
        return "url must not point to a private or local address"
    return None


# ---------- outbound delivery ----------

def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=cfg.REQUEST_TIMEOUT_SECONDS, follow_redirects=False)


def build_event(event_type: str, data: dict) -> dict:
    return {
        "type": event_type,
        "id": db.new_id(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }


async def deliver(subscription: dict, event: dict) -> bool:
    """POST one signed event to one subscriber, retrying with backoff.

    Every attempt is recorded in webhook_deliveries. A row with delivered_at
    NULL and next_retry_at NULL is dead-lettered: attempts exhausted.
    """
    body = json.dumps(event, separators=(",", ":")).encode()
    headers = {
        "Content-Type": "application/json",
        SIGNATURE_HEADER: sign(subscription["secret"], body),
        EVENT_ID_HEADER: event["id"],
    }
    delivery_id = await db.create_delivery(event["id"], subscription["id"], body.decode())

    async with make_client() as client:
        for attempt in range(1, cfg.MAX_ATTEMPTS + 1):
            status = None
            try:
                response = await client.post(subscription["url"], content=body, headers=headers)
                status = response.status_code
                if response.is_success:
                    await db.record_delivery_attempt(
                        delivery_id, attempt, status, None, None, datetime.now(timezone.utc)
                    )
                    return True
                error = f"HTTP {status}"
            except Exception as e:
                error = f"{type(e).__name__}: {e}"[: cfg.MAX_ERROR_LENGTH]

            if attempt >= cfg.MAX_ATTEMPTS:
                await db.record_delivery_attempt(delivery_id, attempt, status, error, None, None)
                logger.error(
                    "webhook %s to %s dead-lettered after %d attempts: %s",
                    event["id"], subscription["url"], attempt, error,
                )
                return False

            delay = cfg.BASE_DELAY_SECONDS * 2 ** (attempt - 1)
            next_retry = datetime.now(timezone.utc) + timedelta(seconds=delay)
            await db.record_delivery_attempt(delivery_id, attempt, status, error, next_retry, None)
            await _sleep(delay)
    return False


async def fan_out(household_code: str, event: dict) -> None:
    subscriptions = await db.get_active_subscriptions(household_code)
    await asyncio.gather(*(deliver(s, event) for s in subscriptions), return_exceptions=True)


async def notify_item_added(household_code: str, item_id: str) -> None:
    """Background task: runs after the HTTP response, must never raise."""
    try:
        item = await db.get_item(household_code, item_id)
        if item is None:
            return
        event = build_event("item.added", {"household": household_code, "item": item})
        await fan_out(household_code, event)
    except Exception:
        logger.exception("webhook fan-out failed for item %s", item_id)
