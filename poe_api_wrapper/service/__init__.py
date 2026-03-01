"""Service layer exports.

`app` / `start_server` are OpenAI-compatible API service entry points.
The implementation can import reverse clients from `poe_api_wrapper.reverse`.
"""

from __future__ import annotations

from typing import Any

LLM_PACKAGE = False
app = None
_IMPORT_ERROR: Exception | None = None

try:
    from .gateway_api import app, start_server

    LLM_PACKAGE = True
except Exception as exc:
    _IMPORT_ERROR = exc

    def start_server(tokens: Any = None, address: str = "127.0.0.1", port: str = "8000"):
        raise ImportError(
            "Service dependencies are missing or service initialization failed. "
            "Install requirements.txt and check the original error."
        ) from _IMPORT_ERROR


class PoeServer:
    """Compatibility wrapper for service startup."""

    def __init__(self, tokens: Any = None, address: str = "127.0.0.1", port: str = "8000"):
        start_server(tokens=tokens, address=address, port=port)


__all__ = ["app", "start_server", "PoeServer", "LLM_PACKAGE"]
