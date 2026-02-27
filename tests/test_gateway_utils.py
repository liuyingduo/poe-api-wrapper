from poe_api_wrapper.openai.gateway import build_openai_error, extract_bearer_token, hash_api_key, SessionManager


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

