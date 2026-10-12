"""In-memory stand-in for db.py so tests never need Postgres or the network.

Method names match db.py; conftest patches every one that exists there.
"""
import uuid


class FakeDB:
    def __init__(self):
        self.households: dict[str, str | None] = {}  # code -> inbound secret
        self.items: list[dict] = []
        self.subscriptions: dict[str, dict] = {}
        self.inbound_events: set[tuple[str, str]] = set()
        self.deliveries: dict[str, dict] = {}

    def add_household(self, code="ABC234", secret=None):
        self.households[code] = secret
        return code

    # ---- households / items ----

    async def init_pool(self):
        pass

    async def close_pool(self):
        pass

    async def household_exists(self, code):
        return code.upper() in self.households

    async def add_item(self, household_code, name, added_by):
        item = {
            "id": uuid.uuid4().hex,
            "household": household_code,
            "list_id": "list1",
            "name": name,
            "checked": False,
            "added_by": added_by,
            "created_at": "2026-01-01T00:00:00+00:00",
        }
        self.items.append(item)
        return item["id"]

    async def get_item(self, household_code, item_id):
        for i in self.items:
            if i["id"] == item_id and i["household"] == household_code:
                return {k: v for k, v in i.items() if k != "household"}
        return None

    async def get_full_state(self, household_code):
        return {
            "list": {"id": "list1", "date": "2026-01-01", "status": "open", "created_at": ""},
            "items": [i for i in self.items if i["household"] == household_code],
        }

    # ---- webhooks ----

    async def get_webhook_secret(self, code):
        return self.households.get(code)

    async def set_webhook_secret_if_unset(self, code, secret):
        if self.households.get(code) is not None:
            return False
        self.households[code] = secret
        return True

    async def claim_inbound_event(self, code, event_id):
        if (code, event_id) in self.inbound_events:
            return False
        self.inbound_events.add((code, event_id))
        return True

    async def release_inbound_event(self, code, event_id):
        self.inbound_events.discard((code, event_id))

    async def create_subscription(self, code, url, secret):
        sub_id = uuid.uuid4().hex
        self.subscriptions[sub_id] = {
            "id": sub_id, "household": code, "url": url, "secret": secret, "active": True,
        }
        return sub_id

    async def count_active_subscriptions(self, code):
        return sum(1 for s in self.subscriptions.values() if s["household"] == code and s["active"])

    async def list_subscriptions(self, code):
        return [
            {"id": s["id"], "url": s["url"], "active": s["active"]}
            for s in self.subscriptions.values() if s["household"] == code
        ]

    async def get_active_subscriptions(self, code):
        return [
            {"id": s["id"], "url": s["url"], "secret": s["secret"]}
            for s in self.subscriptions.values() if s["household"] == code and s["active"]
        ]

    async def deactivate_subscription(self, code, sub_id):
        sub = self.subscriptions.get(sub_id)
        if not sub or sub["household"] != code:
            return False
        sub["active"] = False
        return True

    async def create_delivery(self, event_id, subscription_id, payload_json):
        delivery_id = uuid.uuid4().hex
        self.deliveries[delivery_id] = {
            "event_id": event_id, "subscription_id": subscription_id,
            "payload": payload_json, "attempt_count": 0, "last_status": None,
            "last_error": None, "next_retry_at": None, "delivered_at": None,
        }
        return delivery_id

    async def record_delivery_attempt(
        self, delivery_id, attempt_count, status, error, next_retry_at, delivered_at
    ):
        self.deliveries[delivery_id].update(
            attempt_count=attempt_count, last_status=status, last_error=error,
            next_retry_at=next_retry_at, delivered_at=delivered_at,
        )
