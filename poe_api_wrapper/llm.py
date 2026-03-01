"""Legacy compatibility module for service startup."""

from .service import LLM_PACKAGE, PoeServer, app, start_server

__all__ = ["PoeServer", "start_server", "LLM_PACKAGE", "app"]
