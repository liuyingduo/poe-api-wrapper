from __future__ import annotations

import os
import sys
import threading
from pathlib import Path
from typing import TextIO

from loguru import logger


_TEE_SENTINEL = "_poe_gateway_terminal_log_tee"
_LOGURU_FILE_SINK_ID = "_poe_gateway_loguru_file_sink_id"
_DEFAULT_LOG_PATH = Path("log") / "poe-gateway.log"


class _TeeStream:
    def __init__(self, stream: TextIO, log_file: TextIO, lock: threading.RLock):
        self._stream = stream
        self._log_file = log_file
        self._lock = lock
        setattr(self, _TEE_SENTINEL, True)

    def write(self, data: str) -> int:
        with self._lock:
            written = self._stream.write(data)
            self._log_file.write(data)
            return written

    def flush(self) -> None:
        with self._lock:
            self._stream.flush()
            self._log_file.flush()

    def isatty(self) -> bool:
        return self._stream.isatty()

    def fileno(self) -> int:
        return self._stream.fileno()

    @property
    def encoding(self) -> str | None:
        return self._stream.encoding

    @property
    def errors(self) -> str | None:
        return self._stream.errors

    def __getattr__(self, name: str):
        return getattr(self._stream, name)


def install_terminal_log_tee() -> Path | None:
    """Mirror service stdout/stderr to a log file without changing terminal output."""
    if os.environ.get("POE_GATEWAY_LOG_TEE", "1").lower() in {"0", "false", "no", "off"}:
        return None
    if getattr(sys.stdout, _TEE_SENTINEL, False) or getattr(sys.stderr, _TEE_SENTINEL, False):
        log_path = Path(os.environ.get("POE_GATEWAY_LOG_FILE", _DEFAULT_LOG_PATH))
        _install_loguru_file_sink(log_path)
        return log_path

    log_path = Path(os.environ.get("POE_GATEWAY_LOG_FILE", _DEFAULT_LOG_PATH)).expanduser()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    log_file = log_path.open("a", encoding="utf-8", buffering=1)
    lock = threading.RLock()
    sys.stdout = _TeeStream(sys.stdout, log_file, lock)  # type: ignore[assignment]
    sys.stderr = _TeeStream(sys.stderr, log_file, lock)  # type: ignore[assignment]
    _install_loguru_file_sink(log_path)
    return log_path


def _install_loguru_file_sink(log_path: Path) -> None:
    """Loguru binds its default stderr sink before the tee is installed."""
    if hasattr(logger, _LOGURU_FILE_SINK_ID):
        return
    sink_id = logger.add(
        log_path,
        level=os.environ.get("POE_GATEWAY_LOG_LEVEL", os.environ.get("LOGURU_LEVEL", "DEBUG")),
        encoding="utf-8",
        enqueue=True,
    )
    setattr(logger, _LOGURU_FILE_SINK_ID, sink_id)
