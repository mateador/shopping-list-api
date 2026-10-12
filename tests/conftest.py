import os

# db.py reads this at import time; the fake db below means it is never used.
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

import httpx
import pytest

import db
import push
import webhooks
from tests.fake_db import FakeDB


@pytest.fixture
def fake(monkeypatch):
    """Replaces every db.py function the fake implements."""
    f = FakeDB()
    for name in dir(f):
        if not name.startswith("_") and callable(getattr(f, name)) and hasattr(db, name):
            monkeypatch.setattr(db, name, getattr(f, name))
    return f


@pytest.fixture
def sleeps(monkeypatch):
    """Skips real backoff waits and records the requested delays."""
    recorded = []

    async def fake_sleep(delay):
        recorded.append(delay)

    monkeypatch.setattr(webhooks, "_sleep", fake_sleep)
    return recorded


@pytest.fixture
def outbound(monkeypatch):
    """Routes outbound HTTP through a scripted httpx.MockTransport.

    Set `outbound.responses` to a list of status codes or exceptions, consumed
    one per request (the last one repeats). Requests are kept in `.requests`.
    """
    class Outbound:
        responses = [200]
        requests: list[httpx.Request] = []

    state = Outbound()
    state.requests = []

    def handler(request):
        state.requests.append(request)
        idx = min(len(state.requests), len(state.responses)) - 1
        outcome = state.responses[idx]
        if isinstance(outcome, Exception):
            raise outcome
        return httpx.Response(outcome)

    def make_client():
        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    monkeypatch.setattr(webhooks, "make_client", make_client)
    return state


@pytest.fixture(autouse=True)
def push_unconfigured(monkeypatch):
    """app.py loads .env at import, which may hold real OneSignal keys."""
    monkeypatch.setattr(push, "ONESIGNAL_APP_ID", None)
    monkeypatch.setattr(push, "ONESIGNAL_API_KEY", None)


@pytest.fixture
def onesignal(monkeypatch):
    """Configures push and captures requests via httpx.MockTransport."""
    monkeypatch.setattr(push, "ONESIGNAL_APP_ID", "app-123")
    monkeypatch.setattr(push, "ONESIGNAL_API_KEY", "secret-key")

    class OneSignal:
        status = 200
        error: Exception | None = None
        requests: list[httpx.Request] = []

    state = OneSignal()
    state.requests = []

    def handler(request):
        state.requests.append(request)
        if state.error:
            raise state.error
        return httpx.Response(state.status, json={"id": "n1"})

    monkeypatch.setattr(
        push, "make_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )
    return state
