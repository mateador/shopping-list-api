import asyncio
import json

import httpx
import pytest

import webhook_config
import webhooks
from app import app

CODE = "ABC234"
SECRET = "inbound-secret"


def signed(body: bytes, secret=SECRET) -> dict:
    return {webhooks.SIGNATURE_HEADER: webhooks.sign(secret, body)}


def event_body(event_id="evt-1", name="Milk", **data) -> bytes:
    return json.dumps(
        {"id": event_id, "type": "item.added", "data": {"name": name, **data}}
    ).encode()


async def post_event(body: bytes, headers=None, code=CODE):
    _, response = await app.asgi_client.post(
        f"/api/webhooks/{code}/events", content=body, headers=headers or {}
    )
    return response


async def let_background_tasks_run():
    # Delivery is scheduled with app.add_task, so it runs after the response.
    await asyncio.sleep(0.05)


@pytest.fixture
def household(fake):
    fake.add_household(CODE, SECRET)
    return fake


@pytest.fixture
def notified(monkeypatch):
    """Stubs the background fan-out; records (code, item_id) calls."""
    calls = []

    async def fake_notify(code, item_id):
        calls.append((code, item_id))

    monkeypatch.setattr(webhooks, "notify_item_added", fake_notify)
    return calls


# ---------- inbound ----------

async def test_valid_signature_processes_event(household, notified):
    body = event_body(added_by="Ana")
    response = await post_event(body, signed(body))

    assert response.status_code == 200
    assert response.json == {"status": "processed"}
    await let_background_tasks_run()
    assert [(i["name"], i["added_by"]) for i in household.items] == [("Milk", "Ana")]
    assert len(notified) == 1  # same outbound fan-out as the REST route


async def test_tampered_body_is_rejected(household, notified):
    body = event_body()
    headers = signed(body)
    tampered = event_body(name="Poison")

    response = await post_event(tampered, headers)

    assert response.status_code == 401
    assert household.items == []
    assert notified == []


async def test_missing_signature_is_rejected(household):
    response = await post_event(event_body())

    assert response.status_code == 401
    assert household.items == []


async def test_wrong_secret_is_rejected(household):
    body = event_body()
    response = await post_event(body, signed(body, secret="someone-else"))

    assert response.status_code == 401


async def test_household_without_secret_rejects_everything(fake):
    fake.add_household(CODE, None)
    body = event_body()

    response = await post_event(body, signed(body, secret=""))

    assert response.status_code == 401


async def test_unknown_household_is_404(fake):
    body = event_body()
    response = await post_event(body, signed(body), code="ZZZZZZ")

    assert response.status_code == 404


async def test_replayed_event_id_is_duplicate_and_not_reprocessed(household, notified):
    body = event_body(event_id="evt-replay")
    headers = signed(body)

    first = await post_event(body, headers)
    second = await post_event(body, headers)
    await let_background_tasks_run()

    assert first.json == {"status": "processed"}
    assert second.status_code == 200
    assert second.json == {"status": "duplicate"}
    assert len(household.items) == 1
    assert len(notified) == 1


async def test_unsupported_event_type_is_400(household):
    body = json.dumps({"id": "e1", "type": "item.exploded", "data": {}}).encode()
    response = await post_event(body, signed(body))

    assert response.status_code == 400


async def test_invalid_payload_does_not_burn_the_event_id(household):
    bad = event_body(event_id="evt-x", name="  ")
    assert (await post_event(bad, signed(bad))).status_code == 400

    good = event_body(event_id="evt-x", name="Eggs")
    assert (await post_event(good, signed(good))).json == {"status": "processed"}


# ---------- outbound ----------

def subscription():
    return {"id": "sub1", "url": "https://example.com/hook", "secret": "outbound-secret"}


async def deliver_one(fake):
    fake.subscriptions["sub1"] = {**subscription(), "household": CODE, "active": True}
    event = webhooks.build_event("item.added", {"item": {"name": "Milk"}})
    ok = await webhooks.deliver(subscription(), event)
    (row,) = fake.deliveries.values()
    return ok, row


async def test_outbound_success_is_signed(fake, outbound, sleeps):
    ok, row = await deliver_one(fake)

    assert ok is True
    assert len(outbound.requests) == 1
    request = outbound.requests[0]
    assert request.headers[webhooks.SIGNATURE_HEADER] == webhooks.sign(
        "outbound-secret", request.content
    )
    payload = json.loads(request.content)
    assert payload["type"] == "item.added" and payload["id"] and payload["timestamp"]
    assert row["attempt_count"] == 1
    assert row["delivered_at"] is not None
    assert sleeps == []


async def test_outbound_retries_then_succeeds(fake, outbound, sleeps):
    outbound.responses = [500, 200]

    ok, row = await deliver_one(fake)

    assert ok is True
    assert len(outbound.requests) == 2
    assert sleeps == [1.0]
    assert row["attempt_count"] == 2
    assert row["delivered_at"] is not None
    assert row["next_retry_at"] is None


async def test_outbound_all_attempts_fail_is_dead_lettered(fake, outbound, sleeps):
    outbound.responses = [500]

    ok, row = await deliver_one(fake)

    assert ok is False
    assert len(outbound.requests) == webhook_config.MAX_ATTEMPTS == 3
    assert sleeps == [1.0, 2.0]
    assert row["attempt_count"] == 3
    assert row["last_status"] == 500
    assert row["last_error"] == "HTTP 500"
    assert row["delivered_at"] is None
    assert row["next_retry_at"] is None  # nothing scheduled: it is dead


async def test_outbound_timeout_is_retried_then_dead_lettered(fake, outbound, sleeps):
    outbound.responses = [httpx.ReadTimeout("timed out")]

    ok, row = await deliver_one(fake)

    assert ok is False
    assert len(outbound.requests) == 3
    assert sleeps == [1.0, 2.0]
    assert row["last_status"] is None
    assert row["last_error"].startswith("ReadTimeout")
    assert row["delivered_at"] is None and row["next_retry_at"] is None


def test_outbound_client_has_hard_timeout():
    client = webhooks.make_client()
    assert client.timeout == httpx.Timeout(webhook_config.REQUEST_TIMEOUT_SECONDS)
    assert client.follow_redirects is False


async def test_fan_out_reaches_only_active_subscribers(fake, outbound, sleeps):
    fake.add_household(CODE, SECRET)
    await fake.create_subscription(CODE, "https://a.example.com/h", "s1")
    gone = await fake.create_subscription(CODE, "https://b.example.com/h", "s2")
    await fake.deactivate_subscription(CODE, gone)

    item_id = await fake.add_item(CODE, "Milk", None)
    await webhooks.notify_item_added(CODE, item_id)

    assert [str(r.url) for r in outbound.requests] == ["https://a.example.com/h"]


async def test_rest_add_item_fires_outbound_in_background(household, notified):
    _, response = await app.asgi_client.post(
        f"/api/households/{CODE}/items", json={"name": "Bread"}
    )

    assert response.status_code == 200
    await let_background_tasks_run()
    assert len(notified) == 1


# ---------- management ----------

async def test_register_list_deactivate(fake):
    fake.add_household(CODE, None)

    _, created = await app.asgi_client.post(
        f"/api/households/{CODE}/webhooks", json={"url": "https://example.com/hook"}
    )
    assert created.status_code == 200
    assert created.json["secret"] and created.json["inbound_secret"]
    sub_id = created.json["id"]

    # A second registration must not reveal or rotate the inbound secret.
    _, second = await app.asgi_client.post(
        f"/api/households/{CODE}/webhooks", json={"url": "https://example.com/other"}
    )
    assert "inbound_secret" not in second.json

    _, listed = await app.asgi_client.get(f"/api/households/{CODE}/webhooks")
    assert listed.status_code == 200
    assert len(listed.json["webhooks"]) == 2
    assert all(set(w) == {"id", "url", "active"} for w in listed.json["webhooks"])
    assert "secret" not in listed.text

    _, deleted = await app.asgi_client.delete(f"/api/households/{CODE}/webhooks/{sub_id}")
    assert deleted.json == {"id": sub_id, "active": False}
    assert fake.subscriptions[sub_id]["active"] is False


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/hook",
        "https://localhost/hook",
        "https://127.0.0.1/hook",
        "https://169.254.169.254/latest",
        "https://10.0.0.5/hook",
        "https://[::1]/hook",
        "ftp://example.com",
        "not a url",
    ],
)
async def test_register_rejects_unsafe_urls(fake, url):
    fake.add_household(CODE, None)
    _, response = await app.asgi_client.post(
        f"/api/households/{CODE}/webhooks", json={"url": url}
    )

    assert response.status_code == 400
    assert fake.subscriptions == {}


async def test_register_enforces_subscription_cap(fake, monkeypatch):
    fake.add_household(CODE, None)
    monkeypatch.setattr(webhook_config, "MAX_ACTIVE_SUBSCRIPTIONS", 1)
    url = {"url": "https://example.com/hook"}

    _, first = await app.asgi_client.post(f"/api/households/{CODE}/webhooks", json=url)
    _, second = await app.asgi_client.post(f"/api/households/{CODE}/webhooks", json=url)

    assert first.status_code == 200
    assert second.status_code == 400


async def test_delete_unknown_webhook_is_404(fake):
    fake.add_household(CODE, None)
    _, response = await app.asgi_client.delete(f"/api/households/{CODE}/webhooks/nope")

    assert response.status_code == 404
