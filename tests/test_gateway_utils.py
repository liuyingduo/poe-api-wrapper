from types import SimpleNamespace

import pytest

from poe_api_wrapper.service import gateway_api
from poe_api_wrapper.service.gateway import build_openai_error, extract_bearer_token, hash_api_key, SessionManager


def test_extract_bearer_token():
    assert extract_bearer_token("Bearer abc123") == "abc123"
    assert extract_bearer_token("bearer xyz") == "xyz"
    assert extract_bearer_token("Token abc") is None
    assert extract_bearer_token("") is None


def test_build_openai_error():
    payload = build_openai_error(401, "authentication_error", "Invalid API key", {"x": 1})
    assert payload["error"]["code"] == 401
    assert payload["error"]["type"] == "authentication_error"
    assert payload["error"]["message"] == "Invalid API key"
    assert payload["error"]["metadata"]["x"] == 1


def test_hash_api_key_stable():
    assert hash_api_key("abc") == hash_api_key("abc")
    assert hash_api_key("abc") != hash_api_key("abcd")


def test_session_id_resolution():
    session_id, persistent = SessionManager.resolve_session_id({"session_id": "sid-1"}, None)
    assert session_id == "sid-1"
    assert persistent is True

    session_id, persistent = SessionManager.resolve_session_id({}, "user-1")
    assert session_id == "user-1"
    assert persistent is True

    session_id, persistent = SessionManager.resolve_session_id(None, None)
    assert session_id.startswith("ephemeral-")
    assert persistent is False


@pytest.mark.asyncio
async def test_request_insufficient_points_triggers_points_sync(monkeypatch):
    created_coroutines = []

    class FakeRepo:
        async def mark_account_success(self, account_id):
            raise AssertionError("request_insufficient_points is not a success")

        async def record_account_error(self, *args, **kwargs):
            raise AssertionError("request_insufficient_points should not record account error")

    class FakePool:
        def __init__(self):
            self.invalidated = []

        async def invalidate_client(self, account_id):
            self.invalidated.append(account_id)

    class FakeLease:
        def __init__(self):
            self.released = False

        async def release(self):
            self.released = True

    def fake_create_task(coro):
        created_coroutines.append(coro)
        coro.close()
        return SimpleNamespace()

    pool = FakePool()
    lease = FakeLease()
    runtime = SimpleNamespace(
        repo=FakeRepo(),
        pool=pool,
        config=SimpleNamespace(cooldown_seconds=60),
    )
    decision = gateway_api.AccountErrorDecision(
        payload={},
        status_code=402,
        kind="request_insufficient_points",
        error_text="RuntimeError: Poe message failed with state 'error_insufficient_fund'",
    )

    monkeypatch.setattr(gateway_api.asyncio, "create_task", fake_create_task)

    await gateway_api._finalize_account_use(
        runtime,
        account_id="acc-1",
        lease=lease,
        success=False,
        error_decision=decision,
    )

    assert lease.released is True
    assert pool.invalidated == []
    assert len(created_coroutines) == 1


@pytest.mark.asyncio
async def test_estimate_file_tokens(tmp_path):
    # Test with text content
    text_file = tmp_path / "test.txt"
    text_file.write_text("Hello, this is a simple text file for testing.", encoding="utf-8")
    tokens = await gateway_api._estimate_file_tokens(str(text_file))
    assert tokens > 0

    # Test with binary extension
    png_file = tmp_path / "test.png"
    png_file.write_bytes(b"\x00\x01\x02")
    tokens = await gateway_api._estimate_file_tokens(str(png_file))
    assert tokens == 0

    # Test with null byte in the first 1024 bytes (non-standard extension)
    bin_file = tmp_path / "test.dat"
    bin_file.write_bytes(b"hello\x00world")
    tokens = await gateway_api._estimate_file_tokens(str(bin_file))
    assert tokens == 0
