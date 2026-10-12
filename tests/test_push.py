import asyncio
import json

import httpx

import push
from app import app

CODE = "ABC234"


def sent(onesignal) -> dict:
    (request,) = onesignal.requests
    return json.loads(request.content)


async def test_push_targets_household_and_excludes_adder(onesignal):
    await push.notify_item_added(CODE, "Milk", "Ana")

    request = onesignal.requests[0]
    assert request.headers["Authorization"] == "Key secret-key"
    body = sent(onesignal)
    assert body["app_id"] == "app-123"
    assert body["filters"] == [
        {"field": "tag", "key": "household", "relation": "=", "value": CODE},
        {"field": "tag", "key": "user", "relation": "!=", "value": "Ana"},
    ]
    assert body["contents"] == {"en": "Ana added Milk"}


async def test_push_without_adder_goes_to_everyone(onesignal):
    await push.notify_item_added(CODE, "Milk", None)

    body = sent(onesignal)
    assert [f["key"] for f in body["filters"]] == ["household"]
    assert body["contents"] == {"en": "Milk was added"}


async def test_long_item_name_is_truncated(onesignal):
    await push.notify_item_added(CODE, "x" * 500, "Ana")

    assert len(sent(onesignal)["contents"]["en"]) < 150


async def test_unconfigured_push_does_nothing(monkeypatch):
    def boom():
        raise AssertionError("must not create a client when unconfigured")

    monkeypatch.setattr(push, "make_client", boom)

    await push.notify_item_added(CODE, "Milk", "Ana")


async def test_push_failure_never_raises(onesignal):
    onesignal.status = 500
    await push.notify_item_added(CODE, "Milk", "Ana")

    onesignal.error = httpx.ConnectError("down")
    await push.notify_item_added(CODE, "Milk", "Ana")


async def test_adding_item_sends_push_in_background(fake, onesignal, monkeypatch):
    import webhooks

    async def no_webhooks(code, item_id):
        pass

    monkeypatch.setattr(webhooks, "notify_item_added", no_webhooks)
    fake.add_household(CODE, None)

    _, response = await app.asgi_client.post(
        f"/api/households/{CODE}/items", json={"name": "Bread", "added_by": "Ana"}
    )
    await asyncio.sleep(0.05)

    assert response.status_code == 200
    assert sent(onesignal)["contents"] == {"en": "Ana added Bread"}
