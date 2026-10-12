"""Push notifications via OneSignal when an item is added.

Devices are tagged by the PWA with `household` (the household code) and `user`
(the display name, same value the PWA sends as added_by). The adder's own
devices are excluded. With no credentials configured this is a no-op.
"""
import logging
import os

import httpx

import webhook_config as cfg

logger = logging.getLogger(__name__)

ONESIGNAL_APP_ID = os.environ.get("ONESIGNAL_APP_ID")
ONESIGNAL_API_KEY = os.environ.get("ONESIGNAL_API_KEY")
ONESIGNAL_URL = "https://api.onesignal.com/notifications"

MAX_NAME_LENGTH = 100


def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=cfg.REQUEST_TIMEOUT_SECONDS, follow_redirects=False)


def build_notification(household_code: str, name: str, added_by: str | None) -> dict:
    filters = [{"field": "tag", "key": "household", "relation": "=", "value": household_code}]
    if added_by:
        filters.append({"field": "tag", "key": "user", "relation": "!=", "value": added_by})

    name = name if len(name) <= MAX_NAME_LENGTH else name[:MAX_NAME_LENGTH] + "…"
    message = f"{added_by} added {name}" if added_by else f"{name} was added"
    return {
        "app_id": ONESIGNAL_APP_ID,
        "filters": filters,
        "headings": {"en": "Shopping list"},
        "contents": {"en": message},
    }


async def notify_item_added(household_code: str, name: str, added_by: str | None) -> None:
    """Background task: runs after the HTTP response, must never raise.

    One attempt, no retries: a missed push is not worth a retry queue.
    """
    if not (ONESIGNAL_APP_ID and ONESIGNAL_API_KEY):
        return
    try:
        async with make_client() as client:
            response = await client.post(
                ONESIGNAL_URL,
                json=build_notification(household_code, name, added_by.strip() if added_by else None),
                headers={"Authorization": f"Key {ONESIGNAL_API_KEY}"},
            )
        if not response.is_success:
            logger.warning("OneSignal push failed: HTTP %s %s", response.status_code, response.text[:200])
    except Exception:
        logger.exception("OneSignal push failed for household %s", household_code)
