"""Top-level exports without import-time side effects."""

from __future__ import annotations

from typing import Any

__all__ = [
    "PoeApi",
    "AsyncPoeApi",
    "PoeExample",
    "PoeServer",
    "LLM_PACKAGE",
    "app",
    "start_server",
]


def __getattr__(name: str) -> Any:
    if name in {"PoeApi", "AsyncPoeApi", "PoeExample"}:
        from . import reverse as _reverse

        return getattr(_reverse, name)

    if name in {"PoeServer", "LLM_PACKAGE", "app", "start_server"}:
        from . import service as _service

        return getattr(_service, name)

    raise AttributeError(f"module 'poe_api_wrapper' has no attribute {name!r}")
