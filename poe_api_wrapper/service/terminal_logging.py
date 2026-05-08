from __future__ import annotations

import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

from loguru import logger


_TEE_SENTINEL = "_poe_gateway_terminal_log_tee"
_LOGURU_FILE_SINK_ID = "_poe_gateway_loguru_file_sink_id"
_ROLLING_WRITER = "_poe_gateway_rolling_log_writer"
_DEFAULT_LOG_PATH = Path("log") / "poe-gateway.log"
_DEFAULT_RETENTION_SECONDS = 24 * 60 * 60
_DEFAULT_ROTATION_SECONDS = 60 * 60


class _TeeStream:
    def __init__(self, stream: TextIO, log_file: "_RollingLogWriter", lock: threading.RLock):
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


class _RollingLogWriter:
    def __init__(self, base_path: Path, retention_seconds: int, rotation_seconds: int):
        self._base_path = base_path
        self._retention_seconds = retention_seconds
        self._rotation_seconds = rotation_seconds
        self._lock = threading.RLock()
        self._current_bucket: int | None = None
        self._file: TextIO | None = None
        self._last_cleanup = 0.0

    @property
    def base_path(self) -> Path:
        return self._base_path

    def write(self, data: str) -> int:
        if not data:
            return 0
        with self._lock:
            self._open_current_file()
            assert self._file is not None
            self._file.write(data)
            self._cleanup_old_files()
            return len(data)

    def flush(self) -> None:
        with self._lock:
            if self._file is not None:
                self._file.flush()

    def _open_current_file(self) -> None:
        now = time.time()
        bucket = int(now // self._rotation_seconds)
        if self._file is not None and bucket == self._current_bucket:
            return

        if self._file is not None:
            self._file.close()

        self._current_bucket = bucket
        path = self._path_for_bucket(bucket)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("a", encoding="utf-8", buffering=1)

    def _path_for_bucket(self, bucket: int) -> Path:
        started_at = datetime.fromtimestamp(bucket * self._rotation_seconds, tz=timezone.utc)
        stamp = started_at.strftime("%Y%m%d-%H%M%S")
        return self._base_path.with_name(f"{self._base_path.stem}.{stamp}{self._base_path.suffix}")

    def _cleanup_old_files(self) -> None:
        now = time.time()
        if now - self._last_cleanup < 60:
            return
        self._last_cleanup = now
        cutoff = now - self._retention_seconds
        pattern = f"{self._base_path.stem}.*{self._base_path.suffix}"
        for path in self._base_path.parent.glob(pattern):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink()
            except OSError:
                continue


def install_terminal_log_tee() -> Path | None:
    """Mirror service stdout/stderr to a log file without changing terminal output."""
    if os.environ.get("POE_GATEWAY_LOG_TEE", "1").lower() in {"0", "false", "no", "off"}:
        return None
    if getattr(sys.stdout, _TEE_SENTINEL, False) or getattr(sys.stderr, _TEE_SENTINEL, False):
        writer = _get_rolling_writer()
        _install_loguru_file_sink(writer)
        return writer.base_path

    writer = _get_rolling_writer()
    lock = threading.RLock()
    sys.stdout = _TeeStream(sys.stdout, writer, lock)  # type: ignore[assignment]
    sys.stderr = _TeeStream(sys.stderr, writer, lock)  # type: ignore[assignment]
    _install_loguru_file_sink(writer)
    return writer.base_path


def _get_rolling_writer() -> _RollingLogWriter:
    existing = getattr(logger, _ROLLING_WRITER, None)
    if existing is not None:
        return existing

    log_path = Path(os.environ.get("POE_GATEWAY_LOG_FILE", _DEFAULT_LOG_PATH)).expanduser()
    retention_seconds = _env_int("POE_GATEWAY_LOG_RETENTION_SECONDS", _DEFAULT_RETENTION_SECONDS)
    rotation_seconds = _env_int("POE_GATEWAY_LOG_ROTATION_SECONDS", _DEFAULT_ROTATION_SECONDS)
    writer = _RollingLogWriter(
        base_path=log_path,
        retention_seconds=max(60, retention_seconds),
        rotation_seconds=max(60, rotation_seconds),
    )
    setattr(logger, _ROLLING_WRITER, writer)
    return writer


def _install_loguru_file_sink(writer: _RollingLogWriter) -> None:
    """Loguru binds its default stderr sink before the tee is installed."""
    if hasattr(logger, _LOGURU_FILE_SINK_ID):
        return
    sink_id = logger.add(
        writer.write,
        level=os.environ.get("POE_GATEWAY_LOG_LEVEL", os.environ.get("LOGURU_LEVEL", "DEBUG")),
        enqueue=True,
    )
    setattr(logger, _LOGURU_FILE_SINK_ID, sink_id)


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default
