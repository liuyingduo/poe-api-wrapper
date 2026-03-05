from __future__ import annotations

import asyncio
import mimetypes
import os
import queue as thread_queue
import re
import statistics
import tempfile
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

try:
    import orjson
except ImportError:
    import json as _json

    class _OrjsonCompat:
        @staticmethod
        def dumps(obj):
            return _json.dumps(obj, ensure_ascii=False).encode("utf-8")

        @staticmethod
        def loads(data):
            if isinstance(data, (bytes, bytearray)):
                data = data.decode("utf-8")
            return _json.loads(data)

    orjson = _OrjsonCompat()
import aiofiles
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from starlette.responses import JSONResponse
from starlette.datastructures import UploadFile as StarletteUploadFile
from httpx import AsyncClient
from loguru import logger

from . import helpers
from .gateway import (
    AccountHealthRefresher,
    AccountLease,
    AccountRepository,
    AccountSelector,
    CapacityLimitError,
    CredentialCrypto,
    NoAccountAvailableError,
    PoeClientPool,
    RuntimeLimiter,
    SessionManager,
    build_openai_error,
    extract_bearer_token,
    fetch_poe_revision,
    hash_api_key,
    mask_secret,
    utc_now,
)
from .types import (
    AccountUpsertData,
    ChatCompletionChunk,
    ChatCompletionChunkChoice,
    ChatCompletionMessageToolCall,
    ChatCompletionResponse,
    ChatCompletionResponseChoice,
    ChatCompletionUsage,
    ChatData,
    ChoiceDeltaToolCall,
    ChoiceDeltaToolCallFunction,
    FunctionCall,
    ImagesEditData,
    ImagesGenData,
    MessageResponse,
    ResponsesData,
)

DIR = Path(__file__).resolve().parent
DEFAULT_MODEL_TOKENS = 128000
DEFAULT_MODEL_ENDPOINTS = [
    "/v1/chat/completions",
    "/v1/images/generations",
    "/v1/images/edits",
]

app = FastAPI(title="Poe API Mongo Gateway", description="OpenAI-Compatible Poe Gateway")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _load_dotenv_file(path: Path) -> bool:
    if not path.exists():
        return False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ.setdefault(key, value)
    return True


def _bootstrap_env() -> None:
    # Priority:
    # 1) Existing process environment
    # 2) Explicit config file via GATEWAY_CONFIG_FILE
    # 3) .env.gateway in cwd (single local source of truth)
    explicit_path = os.getenv("GATEWAY_CONFIG_FILE", "").strip()
    loaded_from: list[Path] = []
    if explicit_path:
        p = Path(explicit_path)
        if _load_dotenv_file(p):
            loaded_from.append(p)

    for candidate in (Path.cwd() / ".env.gateway",):
        if _load_dotenv_file(candidate):
            loaded_from.append(candidate)

    if loaded_from:
        logger.info("Loaded gateway config from: {}", ", ".join(str(p) for p in loaded_from))


_bootstrap_env()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"Environment variable {name} must be an integer")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "1" if default else "0").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"Environment variable {name} must be a boolean (true/false, 1/0)")


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


def _split_csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def _model_tokens(meta: dict[str, Any]) -> int:
    return int(meta.get("tokens", DEFAULT_MODEL_TOKENS) or DEFAULT_MODEL_TOKENS)


def _model_endpoints(meta: dict[str, Any]) -> list[str]:
    endpoints = meta.get("endpoints")
    if isinstance(endpoints, list) and endpoints:
        return endpoints
    return DEFAULT_MODEL_ENDPOINTS


@dataclass
class GatewayConfig:
    default_poe_revision: str
    mongodb_uri: str
    mongodb_db: str
    fernet_key: str
    admin_api_key: str
    service_api_keys_bootstrap: list[str]
    max_inflight_per_account: int
    global_inflight_limit: int
    depleted_threshold: int
    refresh_interval_seconds: int
    recent_active_minutes: int
    cooldown_seconds: int
    prewarm_concurrency: int
    prewarm_max_accounts: int
    prewarm_on_startup: bool
    prewarm_account_timeout_seconds: int
    prewarm_scan_interval_seconds: int
    acquire_wait_poll_seconds: float
    auto_fetch_poe_revision: bool
    daily_reset_timezone: str
    daily_reset_hour: int
    daily_reset_point_balance: int
    invalid_auto_refresh_enabled: bool
    invalid_auto_refresh_interval_seconds: int
    invalid_auto_refresh_batch_limit: int
    client_max_age_seconds: int

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        return cls(
            default_poe_revision=os.getenv("POE_REVISION", "").strip(),
            mongodb_uri=_require_env("MONGODB_URI"),
            mongodb_db=_require_env("MONGODB_DB"),
            fernet_key=_require_env("FERNET_KEY"),
            admin_api_key=_require_env("ADMIN_API_KEY"),
            service_api_keys_bootstrap=_split_csv(os.getenv("SERVICE_API_KEYS_BOOTSTRAP", "")),
            max_inflight_per_account=_env_int("MAX_INFLIGHT_PER_ACCOUNT", 2),
            global_inflight_limit=_env_int("GLOBAL_INFLIGHT_LIMIT", 200),
            depleted_threshold=_env_int("DEPLETED_THRESHOLD", 20),
            refresh_interval_seconds=_env_int("REFRESH_INTERVAL_SECONDS", 600),
            recent_active_minutes=_env_int("RECENT_ACTIVE_MINUTES", 120),
            cooldown_seconds=_env_int("COOLDOWN_SECONDS", 120),
            prewarm_concurrency=_env_int("PREWARM_CONCURRENCY", 5),
            prewarm_max_accounts=_env_int("PREWARM_MAX_ACCOUNTS", 3000),
            prewarm_on_startup=_env_bool("PREWARM_ON_STARTUP", True),
            prewarm_account_timeout_seconds=_env_int("PREWARM_ACCOUNT_TIMEOUT_SECONDS", 20),
            prewarm_scan_interval_seconds=_env_int("PREWARM_SCAN_INTERVAL_SECONDS", 15),
            acquire_wait_poll_seconds=max(0.01, _env_int("ACQUIRE_WAIT_POLL_SECONDS_MS", 100) / 1000.0),
            auto_fetch_poe_revision=_env_bool("AUTO_FETCH_POE_REVISION", True),
            daily_reset_timezone=os.getenv("DAILY_RESET_TIMEZONE", "America/Los_Angeles").strip(),
            daily_reset_hour=_env_int("DAILY_RESET_HOUR", 0),
            daily_reset_point_balance=_env_int("DAILY_RESET_POINT_BALANCE", 3000),
            invalid_auto_refresh_enabled=_env_bool("INVALID_AUTO_REFRESH_ENABLED", True),
            invalid_auto_refresh_interval_seconds=_env_int("INVALID_AUTO_REFRESH_INTERVAL_SECONDS", 900),
            invalid_auto_refresh_batch_limit=_env_int("INVALID_AUTO_REFRESH_BATCH_LIMIT", 200),
            client_max_age_seconds=_env_int("CLIENT_MAX_AGE_SECONDS", 600),
        )


@dataclass
class GatewayRuntime:
    config: GatewayConfig
    repo: AccountRepository
    limiter: RuntimeLimiter
    selector: AccountSelector
    pool: PoeClientPool
    refresher: AccountHealthRefresher
    sessions: SessionManager


class PrewarmQueueCoordinator:
    """Dedicated prewarm loop with a DB-order round-robin queue.

    Rules:
    - Build a total queue from Mongo storage order (`_id` ASC).
    - Only accounts with `balance >= current_median_balance` are prewarmed.
    - Warm pool upper bound is always `PREWARM_MAX_ACCOUNTS`.
    - Evicted/expired accounts are moved to the tail of total queue.
    """

    def __init__(self, runtime: GatewayRuntime):
        self.runtime = runtime
        # Cross-thread signal queue: move these accounts to total-queue tail.
        self._tail_request_q: thread_queue.Queue[str] = thread_queue.Queue()
        self._tail_requested_ids: set[str] = set()
        self._tail_requested_lock = threading.Lock()

        # Prewarm work queue consumed by background workers.
        self._prewarm_q: thread_queue.Queue[str] = thread_queue.Queue()
        self._queued_prewarm_ids: set[str] = set()
        self._queued_prewarm_lock = threading.Lock()

        self._failed_backoff_until: dict[str, float] = {}
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._prewarm_loop: Optional[asyncio.AbstractEventLoop] = None
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_ready = threading.Event()
        self._target_max = max(1, int(self.runtime.config.prewarm_max_accounts))

        # Round-robin snapshot (owned by prewarm loop).
        self._round_robin_ids: deque[str] = deque()
        self._round_robin_known: set[str] = set()
        self._round_robin_meta: dict[str, dict[str, Any]] = {}
        self._median_balance: float = 0.0

    # -- helpers ---------------------------------------------------------------

    def _target_count(self) -> int:
        return self._target_max

    def _failure_backoff_seconds(self) -> int:
        scan_interval = max(1, int(self.runtime.config.prewarm_scan_interval_seconds))
        cooldown = max(1, int(self.runtime.config.cooldown_seconds))
        return max(scan_interval, min(cooldown, 300))

    def _cleanup_failure_backoff(self, now: float) -> None:
        expired = [aid for aid, until in self._failed_backoff_until.items() if until <= now]
        for aid in expired:
            self._failed_backoff_until.pop(aid, None)

    def _mark_failure_backoff(self, account_id: str) -> None:
        self._failed_backoff_until[account_id] = time.monotonic() + self._failure_backoff_seconds()

    def _clear_failure_backoff(self, account_id: str) -> None:
        self._failed_backoff_until.pop(account_id, None)

    def _in_failure_backoff(self, account_id: str, now: float) -> bool:
        until = self._failed_backoff_until.get(account_id)
        return bool(until and until > now)

    # -- public API (called from main loop) ------------------------------------

    async def enqueue_account(self, account_id: str) -> bool:
        """Request this account to be moved to the total-queue tail."""
        account_id = str(account_id or "").strip()
        if not account_id:
            return False
        with self._tail_requested_lock:
            if account_id in self._tail_requested_ids:
                return False
            self._tail_requested_ids.add(account_id)
        self._tail_request_q.put(account_id)
        return True

    # -- queue state maintenance -----------------------------------------------

    async def _sync_round_robin_snapshot(self) -> None:
        docs = await self.runtime.repo.list_accounts_round_robin_snapshot(limit=0)
        db_ids: list[str] = []
        db_meta: dict[str, dict[str, Any]] = {}
        balances: list[int] = []

        for doc in docs:
            account_id = str(doc["_id"])
            balance = int(doc.get("message_point_balance", 0) or 0)
            db_ids.append(account_id)
            balances.append(balance)
            db_meta[account_id] = {
                "status": str(doc.get("status", "unknown")),
                "cooldown_until": doc.get("cooldown_until"),
                "message_point_balance": balance,
            }

        self._median_balance = float(statistics.median(balances)) if balances else 0.0
        self._round_robin_meta = db_meta

        db_set = set(db_ids)
        if self._round_robin_ids:
            self._round_robin_ids = deque(aid for aid in self._round_robin_ids if aid in db_set)
        self._round_robin_known = set(self._round_robin_ids)

        for account_id in db_ids:
            if account_id not in self._round_robin_known:
                self._round_robin_ids.append(account_id)
                self._round_robin_known.add(account_id)

    def _move_to_tail(self, account_id: str) -> bool:
        if account_id not in self._round_robin_known:
            if account_id not in self._round_robin_meta:
                return False
            self._round_robin_ids.append(account_id)
            self._round_robin_known.add(account_id)
            return True

        try:
            self._round_robin_ids.remove(account_id)
        except ValueError:
            pass
        self._round_robin_ids.append(account_id)
        return True

    async def _drain_tail_requests(self) -> int:
        moved = 0
        while True:
            try:
                account_id = self._tail_request_q.get_nowait()
            except thread_queue.Empty:
                break

            with self._tail_requested_lock:
                self._tail_requested_ids.discard(account_id)
            if self._move_to_tail(account_id):
                moved += 1
        return moved

    async def _evict_cached_below_median(self) -> int:
        """Evict idle prewarmed clients whose balance is below current median."""
        cached_ids = self.runtime.pool.cached_account_ids_in_order()
        if not cached_ids:
            return 0

        evicted = 0
        for account_id in cached_ids:
            meta = self._round_robin_meta.get(account_id)
            if not meta:
                continue
            balance = int(meta.get("message_point_balance", 0) or 0)
            if balance >= self._median_balance:
                continue

            await self.runtime.limiter.block_account(account_id)
            try:
                # Do not interrupt in-flight requests; retry in next scan.
                inflight = await self.runtime.limiter.inflight_for(account_id)
                if inflight > 0:
                    continue
                await self.runtime.pool.invalidate_client(account_id)
                self._move_to_tail(account_id)
                evicted += 1
            except Exception as exc:
                logger.debug(
                    "Evict-below-median failed for {}: {}",
                    mask_secret(account_id),
                    exc,
                )
            finally:
                await self.runtime.limiter.unblock_account(account_id)

        if evicted > 0:
            logger.info(
                "Evicted {} prewarmed account(s) below median balance (median_balance={})",
                evicted,
                int(self._median_balance),
            )
        return evicted

    def _enqueue_prewarm_job(self, account_id: str) -> bool:
        cached_count = len(self.runtime.pool.cached_account_ids())
        with self._queued_prewarm_lock:
            if account_id in self._queued_prewarm_ids:
                return False
            if (cached_count + len(self._queued_prewarm_ids)) >= self._target_count():
                return False
            self._queued_prewarm_ids.add(account_id)
        self._prewarm_q.put(account_id)
        return True

    def _mark_prewarm_done(self, account_id: str) -> None:
        with self._queued_prewarm_lock:
            self._queued_prewarm_ids.discard(account_id)

    # -- prewarm logic (runs on background loop) -------------------------------

    async def _prewarm_account(self, account_id: str) -> None:
        if self.runtime.pool.has_client(account_id):
            self._clear_failure_backoff(account_id)
            return
        account_doc = await self.runtime.repo.get_account_by_id(account_id)
        if not account_doc:
            return
        if str(account_doc.get("status", "active")) != "active":
            return
        client = None
        stored_in_pool = False
        try:
            creds = await self.runtime.repo.get_account_credentials(account_doc)
            client = await asyncio.wait_for(
                self.runtime.pool._create_client_with_fallback(account_id, creds),
                timeout=max(1, self.runtime.config.prewarm_account_timeout_seconds),
            )

            # -- Refresh account points right after successful connection ------
            balance = None
            try:
                settings = await client.get_settings()
                message_info = settings.get("messagePointInfo", {})
                subscription = settings.get("subscription", {})
                balance = int(message_info.get("subscriptionPointBalance", 0) or 0) + int(
                    message_info.get("addonPointBalance", 0) or 0
                )
                subscription_active = bool(subscription.get("isActive", False))
                await self.runtime.repo.update_account_health(
                    account_id,
                    balance=balance,
                    subscription_active=subscription_active,
                    depleted_threshold=self.runtime.config.depleted_threshold,
                )
                await self.runtime.repo.mark_account_success(account_id)
                logger.info(
                    "Prewarmed account {} (background loop) | balance={}",
                    mask_secret(account_id),
                    balance,
                )
                # If account is depleted after refresh, evict immediately
                if balance <= self.runtime.config.depleted_threshold:
                    logger.warning(
                        "Prewarmed account {} has insufficient balance ({}), evicting",
                        mask_secret(account_id),
                        balance,
                    )
                    await self.runtime.pool.invalidate_client(account_id)
            except Exception as points_exc:
                logger.warning(
                    "Prewarmed account {} connected but failed to refresh points: {}",
                    mask_secret(account_id),
                    points_exc,
                )
                # Still keep the client in the pool; points refresh is best-effort

            if balance is not None and balance <= self.runtime.config.depleted_threshold:
                logger.warning(
                    "Prewarmed account {} has insufficient balance ({}), skipping cache",
                    mask_secret(account_id),
                    balance,
                )
                await self.runtime.pool._close_client(client)
                client = None
                self._clear_failure_backoff(account_id)
                return

            # Important: migrate only after all prewarm-loop async operations are done.
            # This prevents rebinding the rebuilt httpx client back to the prewarm loop.
            client.migrate_to_loop(self._main_loop)
            self.runtime.pool.store_prewarmed_client(account_id, client)
            stored_in_pool = True
            self._clear_failure_backoff(account_id)

        except asyncio.TimeoutError:
            logger.warning(
                "Prewarm timed out for account {} after {}s",
                mask_secret(account_id),
                self.runtime.config.prewarm_account_timeout_seconds,
            )
            self._mark_failure_backoff(account_id)
        except Exception as exc:
            logger.warning("Prewarm failed for account {}: {}", mask_secret(account_id), exc)
            self._mark_failure_backoff(account_id)
            await self.runtime.repo.record_account_error(
                account_id,
                f"prewarm_error: {exc}",
                cooldown_seconds=self.runtime.config.cooldown_seconds,
            )
        finally:
            if client is not None and not stored_in_pool:
                try:
                    await self.runtime.pool._close_client(client)
                except Exception:
                    pass

    async def _producer_loop(self) -> None:
        scan_interval = max(1, int(self.runtime.config.prewarm_scan_interval_seconds))
        while not self._stop_event.is_set():
            try:
                await self._sync_round_robin_snapshot()
                moved = await self._drain_tail_requests()
                if moved > 0:
                    logger.info("Prewarm total-queue moved {} account(s) to tail", moved)
                await self._evict_cached_below_median()

                target_count = self._target_count()
                cached_count = len(self.runtime.pool.cached_account_ids())
                with self._queued_prewarm_lock:
                    queued_count = len(self._queued_prewarm_ids)
                deficit = target_count - cached_count - queued_count
                if deficit <= 0:
                    await asyncio.sleep(scan_interval)
                    continue

                if not self._round_robin_ids:
                    await asyncio.sleep(scan_interval)
                    continue

                now_utc = utc_now()
                now = time.monotonic()
                self._cleanup_failure_backoff(now)

                enqueue_count = 0
                scan_budget = len(self._round_robin_ids)
                for _ in range(scan_budget):
                    if enqueue_count >= deficit:
                        break
                    account_id = self._round_robin_ids[0]
                    self._round_robin_ids.rotate(-1)

                    if self.runtime.pool.has_client(account_id):
                        continue
                    if self._in_failure_backoff(account_id, now):
                        continue

                    meta = self._round_robin_meta.get(account_id)
                    if not meta:
                        continue
                    if meta.get("status") != "active":
                        continue

                    cooldown_until = meta.get("cooldown_until")
                    in_cooldown = bool(
                        cooldown_until
                        and isinstance(cooldown_until, datetime)
                        and cooldown_until > now_utc
                    )
                    if in_cooldown:
                        continue

                    balance = int(meta.get("message_point_balance", 0) or 0)
                    if balance < self._median_balance:
                        continue

                    if self._enqueue_prewarm_job(account_id):
                        enqueue_count += 1

                if enqueue_count > 0:
                    logger.info(
                        "Prewarm producer queued {} account(s); target={} cached={} queued={} median_balance={}",
                        enqueue_count,
                        target_count,
                        cached_count,
                        queued_count + enqueue_count,
                        int(self._median_balance),
                    )
            except Exception as exc:
                logger.warning("Prewarm producer loop failed: {}", exc)

            await asyncio.sleep(scan_interval)

    async def _consumer_loop(self, worker_idx: int) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            # Poll the thread-safe queue with a timeout so we can check the
            # stop event periodically.
            try:
                account_id = await loop.run_in_executor(
                    None, lambda: self._prewarm_q.get(timeout=1.0),
                )
            except thread_queue.Empty:
                continue
            except asyncio.CancelledError:
                break
            try:
                if (
                    not self.runtime.pool.has_client(account_id)
                    and len(self.runtime.pool.cached_account_ids()) < self._target_count()
                ):
                    await self._prewarm_account(account_id)
            except Exception as exc:
                logger.warning("Prewarm consumer {} unexpected error: {}", worker_idx, exc)
            finally:
                self._mark_prewarm_done(account_id)

    async def _ttl_cleanup_loop(self) -> None:
        check_interval = max(30, int(self.runtime.config.prewarm_scan_interval_seconds))
        while not self._stop_event.is_set():
            try:
                expired_ids = [
                    account_id
                    for account_id in self.runtime.pool.cached_account_ids()
                    if self.runtime.pool.is_client_expired(account_id)
                ]
                for account_id in expired_ids:
                    logger.info("TTL expired for account {}, evicting and re-queuing", mask_secret(account_id))
                    await self.runtime.pool.invalidate_client(account_id)
                    await self.enqueue_account(account_id)
            except Exception as exc:
                logger.warning("TTL cleanup loop failed: {}", exc)
            await asyncio.sleep(check_interval)

    # -- background thread entry point -----------------------------------------

    def _run_prewarm_loop(self) -> None:
        """Thread target: create and run a dedicated asyncio event loop."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._prewarm_loop = loop
        self._loop_ready.set()
        try:
            loop.run_until_complete(self._prewarm_main())
        except Exception:
            logger.exception("Prewarm background loop crashed")
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            loop.close()
            self._prewarm_loop = None

    async def _prewarm_main(self) -> None:
        worker_count = max(1, int(self.runtime.config.prewarm_concurrency))
        producer = asyncio.create_task(self._producer_loop(), name="prewarm-producer")
        consumers = [
            asyncio.create_task(self._consumer_loop(i + 1), name=f"prewarm-consumer-{i + 1}")
            for i in range(worker_count)
        ]
        ttl_cleanup = asyncio.create_task(self._ttl_cleanup_loop(), name="ttl-cleanup")
        logger.info(
            "Started prewarm coordinator with {} consumers; fixed_target={} scan_interval={}s",
            worker_count,
            self._target_count(),
            max(1, int(self.runtime.config.prewarm_scan_interval_seconds)),
        )
        # Block until the coordinator is asked to stop.
        while not self._stop_event.is_set():
            await asyncio.sleep(0.5)
        # Graceful shutdown of all tasks on this loop.
        producer.cancel()
        ttl_cleanup.cancel()
        for c in consumers:
            c.cancel()
        await asyncio.gather(producer, ttl_cleanup, *consumers, return_exceptions=True)

    # -- lifecycle -------------------------------------------------------------

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._main_loop = asyncio.get_running_loop()
        self._stop_event.clear()
        self._loop_ready.clear()
        self._thread = threading.Thread(
            target=self._run_prewarm_loop,
            daemon=True,
            name="prewarm-loop-thread",
        )
        self._thread.start()
        # Wait for the background loop to be ready (up to 5 s).
        self._loop_ready.wait(timeout=5.0)
        logger.info("Prewarm background thread started")

    async def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread and thread.is_alive():
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: thread.join(timeout=15),
            )
        self._thread = None
        self._prewarm_loop = None


with (DIR / "models.json").open("rb") as f:
    app.state.models = orjson.loads(f.read())


def _runtime() -> GatewayRuntime:
    runtime = getattr(app.state, "runtime", None)
    if not runtime:
        raise RuntimeError("Gateway runtime is not initialized")
    return runtime


def _openai_http_error(code: int, err_type: str, message: str, metadata: Optional[dict[str, Any]] = None):
    raise HTTPException(status_code=code, detail=build_openai_error(code, err_type, message, metadata))


def _unwrap_root_exception(exc: BaseException) -> BaseException:
    seen: set[int] = set()
    current: BaseException = exc
    while True:
        next_exc = current.__cause__ or current.__context__
        if next_exc is None:
            return current
        if id(next_exc) in seen:
            return current
        seen.add(id(current))
        current = next_exc


def _exception_summary(exc: BaseException) -> str:
    root = _unwrap_root_exception(exc)
    top = f"{type(exc).__name__}: {exc}"
    root_text = f"{type(root).__name__}: {root}"
    if root is exc:
        return top
    return f"{top} | root={root_text}"


def _is_depleted_error(error_text: str) -> bool:
    lower = error_text.lower()
    return any(token in lower for token in ("insufficient", "balance", "reached_limit", "402", "daily limit"))


def _is_invalid_error(error_text: str) -> bool:
    lower = error_text.lower()
    return any(token in lower for token in ("403", "forbidden", "unauthorized", "invalid", "challenge"))


def _is_rate_limit_error(error_text: str) -> bool:
    lower = error_text.lower()
    return any(token in lower for token in ("rate_limit", "429", "concurrent_messages", "too many requests"))


def _is_provider_timeout_error(error_text: str) -> bool:
    lower = error_text.lower()
    return any(
        token in lower
        for token in (
            "timed out waiting for response from bot",
            "websocket may have disconnected",
        )
    )


async def _account_error_payload(
    runtime: GatewayRuntime,
    account_id: str,
    session_id: str,
    persistent_session: bool,
    exc: Exception,
) -> tuple[dict[str, Any], int]:
    error_text = _exception_summary(exc)
    metadata = {"session_id": session_id, "account_id": account_id}
    if _is_depleted_error(error_text):
        await runtime.repo.mark_account_depleted(account_id, error_text)
        await runtime.pool.invalidate_client(account_id)
        if persistent_session:
            await runtime.sessions.break_session(session_id, "bound account is depleted")
            message = "Bound account has no remaining points. Create a new session_id and retry."
        else:
            message = "Insufficient credits on selected account."
        logger.warning(
            "provider_error_classified status=402 type=insufficient_credits account_id={} session_id={} detail={}",
            mask_secret(account_id),
            session_id,
            error_text,
        )
        return build_openai_error(402, "insufficient_credits", message, metadata), 402

    if _is_invalid_error(error_text):
        await runtime.repo.mark_account_invalid(account_id, error_text)
        await runtime.pool.invalidate_client(account_id)
        if persistent_session:
            await runtime.sessions.break_session(session_id, "bound account is invalid")
            message = "Bound account became invalid. Create a new session_id and retry."
        else:
            message = "Selected account failed authorization."
        logger.warning(
            "provider_error_classified status=403 type=authentication_error account_id={} session_id={} detail={}",
            mask_secret(account_id),
            session_id,
            error_text,
        )
        return build_openai_error(403, "authentication_error", message, metadata), 403

    if _is_provider_timeout_error(error_text):
        await runtime.pool.invalidate_client(account_id)
        await runtime.repo.record_account_error(
            account_id,
            error_text,
            cooldown_seconds=runtime.config.cooldown_seconds,
            error_threshold=2,
        )
        logger.warning(
            "provider_error_classified status=504 type=provider_timeout_error account_id={} session_id={} "
            "evicted_client=true detail={}",
            mask_secret(account_id),
            session_id,
            error_text,
        )
        return (
            build_openai_error(
                504,
                "provider_timeout_error",
                "Provider timeout while waiting for Poe response. Retry with backoff.",
                metadata,
            ),
            504,
        )

    if _is_rate_limit_error(error_text):
        await runtime.repo.record_account_error(
            account_id,
            error_text,
            cooldown_seconds=runtime.config.cooldown_seconds,
        )
        logger.warning(
            "provider_error_classified status=429 type=rate_limit_error account_id={} session_id={} detail={}",
            mask_secret(account_id),
            session_id,
            error_text,
        )
        return build_openai_error(429, "rate_limit_error", "Rate limit reached. Retry with backoff.", metadata), 429

    await runtime.repo.record_account_error(
        account_id,
        error_text,
        cooldown_seconds=runtime.config.cooldown_seconds,
    )
    logger.error(
        "provider_error_classified status=500 type=provider_error account_id={} session_id={} detail={}",
        mask_secret(account_id),
        session_id,
        error_text,
    )
    return build_openai_error(500, "provider_error", f"Provider error: {error_text}", metadata), 500


async def _acquire_prewarmed_account(runtime: GatewayRuntime) -> tuple[dict[str, Any], AccountLease]:
    wait_sec = max(0.01, float(runtime.config.acquire_wait_poll_seconds))
    while True:
        try:
            return await runtime.selector.select_account(prewarmed_only=True)
        except NoAccountAvailableError:
            raise
        except CapacityLimitError:
            await asyncio.sleep(wait_sec)


@app.on_event("startup")
async def startup_event() -> None:
    config = GatewayConfig.from_env()

    # 若未在环境变量中配置 POE_REVISION，则自动从 poe.com/login 页面抓取
    if not config.default_poe_revision and config.auto_fetch_poe_revision:
        fetched = await fetch_poe_revision()
        if fetched:
            config.default_poe_revision = fetched
            logger.info("自动获取 poe-revision: {}", fetched)
        else:
            logger.error("未能自动获取 poe-revision，服务启动中止。请检查网络或手动配置 POE_REVISION 环境变量。")
            raise RuntimeError("Failed to fetch poe-revision from poe.com. Service startup aborted.")
    elif not config.default_poe_revision and not config.auto_fetch_poe_revision:
        logger.error("POE_REVISION 未配置，且 AUTO_FETCH_POE_REVISION=false，服务启动中止。")
        raise RuntimeError("POE_REVISION is not set and auto-fetch is disabled. Service startup aborted.")

    crypto = CredentialCrypto(config.fernet_key)
    repo = AccountRepository(config.mongodb_uri, config.mongodb_db, crypto)
    limiter = RuntimeLimiter(
        max_inflight_per_account=config.max_inflight_per_account,
        global_inflight_limit=config.global_inflight_limit,
    )
    pool = PoeClientPool(
        repo=repo,
        default_poe_revision=config.default_poe_revision,
        client_max_age_seconds=config.client_max_age_seconds,
    )
    selector = AccountSelector(
        repo=repo,
        limiter=limiter,
        pool=pool,
        top_n=max(1, int(config.prewarm_max_accounts)),
    )
    refresher = AccountHealthRefresher(
        repo=repo,
        pool=pool,
        depleted_threshold=config.depleted_threshold,
        refresh_interval_seconds=config.refresh_interval_seconds,
        recent_active_minutes=config.recent_active_minutes,
        cooldown_seconds=config.cooldown_seconds,
        daily_reset_timezone=config.daily_reset_timezone,
        daily_reset_hour=config.daily_reset_hour,
        daily_reset_point_balance=config.daily_reset_point_balance,
        invalid_auto_refresh_enabled=config.invalid_auto_refresh_enabled,
        invalid_auto_refresh_interval_seconds=config.invalid_auto_refresh_interval_seconds,
        invalid_auto_refresh_batch_limit=config.invalid_auto_refresh_batch_limit,
    )
    sessions = SessionManager(repo=repo)

    runtime = GatewayRuntime(
        config=config,
        repo=repo,
        limiter=limiter,
        selector=selector,
        pool=pool,
        refresher=refresher,
        sessions=sessions,
    )
    app.state.runtime = runtime

    await repo.init_indexes()
    await repo.bootstrap_service_keys(config.service_api_keys_bootstrap)
    refresher.start()
    app.state.prewarm_coordinator = None
    # Let the pool know about the main loop so reconnect-exhaustion callbacks
    # can schedule cleanup coroutines on it.
    pool._main_loop = asyncio.get_running_loop()
    if config.prewarm_on_startup:
        prewarm_coordinator = PrewarmQueueCoordinator(runtime)
        prewarm_coordinator.start()
        app.state.prewarm_coordinator = prewarm_coordinator

        # When a client is evicted after reconnect exhaustion,
        # re-enqueue it for prewarm automatically.
        async def _on_pool_evict(account_id: str) -> None:
            coord = getattr(app.state, "prewarm_coordinator", None)
            if coord:
                await coord.enqueue_account(account_id)

        pool._on_evict_callback = _on_pool_evict
    else:
        logger.info("Startup prewarm disabled (PREWARM_ON_STARTUP=false)")
    logger.info("Gateway startup complete")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    runtime = getattr(app.state, "runtime", None)
    if not runtime:
        return
    prewarm_coordinator = getattr(app.state, "prewarm_coordinator", None)
    if prewarm_coordinator:
        await prewarm_coordinator.stop()
    await runtime.refresher.stop()
    await runtime.pool.close_all()
    await runtime.repo.close()


@app.middleware("http")
async def audit_middleware(request: Request, call_next):
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    started = asyncio.get_event_loop().time()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
    finally:
        elapsed_ms = (asyncio.get_event_loop().time() - started) * 1000
        logger.info(
            "request_id={} method={} path={} status={} duration_ms={:.2f} key_hash={} session_id={} account_id={} model={}",
            request_id,
            request.method,
            request.url.path,
            status_code,
            elapsed_ms,
            getattr(request.state, "service_key_hash", None),
            getattr(request.state, "session_id", None),
            getattr(request.state, "account_id", None),
            getattr(request.state, "model", None),
        )
    response.headers["x-request-id"] = request_id
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    if isinstance(exc.detail, dict) and "error" in exc.detail:
        payload = exc.detail
    else:
        payload = build_openai_error(exc.status_code, "invalid_request_error", str(exc.detail))
    err = payload.get("error", {})
    logger.warning(
        "http_error request_id={} method={} path={} status={} type={} message={} metadata={}",
        getattr(request.state, "request_id", None),
        request.method,
        request.url.path,
        exc.status_code,
        err.get("type"),
        err.get("message"),
        err.get("metadata", {}),
    )
    return JSONResponse(payload, status_code=exc.status_code)


@app.exception_handler(Exception)
async def unexpected_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error in path {}: {}", request.url.path, exc)
    payload = build_openai_error(500, "provider_error", "Internal server error")
    return JSONResponse(payload, status_code=500)


async def require_admin_auth(request: Request) -> None:
    token = extract_bearer_token(request.headers.get("Authorization", ""))
    runtime = _runtime()
    if not token or token != runtime.config.admin_api_key:
        _openai_http_error(401, "authentication_error", "Invalid admin API key")


async def require_service_auth(request: Request) -> None:
    token = extract_bearer_token(request.headers.get("Authorization", ""))
    runtime = _runtime()
    if not token:
        _openai_http_error(401, "authentication_error", "Missing bearer token")
    enabled = await runtime.repo.is_service_key_enabled(token)
    if not enabled:
        _openai_http_error(401, "authentication_error", "Invalid service API key")
    request.state.service_key_hash = hash_api_key(token)


@app.get("/", response_model=None)
async def index() -> JSONResponse:
    return JSONResponse(
        {
            "message": "Poe Mongo Gateway is running",
            "docs": "OpenAI-compatible endpoints under /v1/*",
        }
    )


@app.get("/healthz", response_model=None)
async def healthz() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/readyz", response_model=None)
async def readyz() -> JSONResponse:
    runtime = _runtime()
    await asyncio.to_thread(runtime.repo.client.admin.command, "ping")
    ready_accounts = await runtime.repo.count_ready_accounts()
    if ready_accounts <= 0:
        _openai_http_error(503, "overloaded_error", "No ready accounts in pool")
    return JSONResponse({"status": "ready", "ready_accounts": ready_accounts})


@app.post("/admin/accounts/upsert", response_model=None, dependencies=[Depends(require_admin_auth)])
async def admin_upsert_account(data: AccountUpsertData) -> JSONResponse:
    runtime = _runtime()
    account = await runtime.repo.upsert_account(
        email=data.email,
        poe_p_b=data.poe_p_b,
        poe_cf_clearance=data.poe_cf_clearance,
        poe_cf_bm=data.poe_cf_bm,
        p_lat=data.p_lat,
        formkey=data.formkey,
        poe_revision=data.poe_revision,
        user_agent=data.user_agent,
    )
    account_id = str(account.get("id", "")).strip()
    if account_id:
        await runtime.pool.invalidate_client(account_id)
        prewarm_coordinator = getattr(app.state, "prewarm_coordinator", None)
        if prewarm_coordinator:
            queued = await prewarm_coordinator.enqueue_account(account_id)
            if queued:
                logger.info(
                    "Queued account {} for async prewarm after upsert",
                    mask_secret(account_id),
                )
    logger.info(
        "Upserted account email={} p_b={} cf_clearance={}",
        data.email,
        mask_secret(data.poe_p_b),
        mask_secret(data.poe_cf_clearance),
    )
    return JSONResponse(jsonable_encoder(account))


@app.get("/admin/accounts", response_model=None, dependencies=[Depends(require_admin_auth)])
async def admin_list_accounts(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    status: Optional[str] = Query(default=None),
    min_balance: Optional[int] = Query(default=None, ge=0),
    max_balance: Optional[int] = Query(default=None, ge=0),
) -> JSONResponse:
    runtime = _runtime()
    data = await runtime.repo.list_accounts(
        page=page,
        page_size=page_size,
        status=status,
        min_balance=min_balance,
        max_balance=max_balance,
    )
    return JSONResponse(jsonable_encoder(data))


@app.get("/admin/accounts/summary", response_model=None, dependencies=[Depends(require_admin_auth)])
async def admin_accounts_summary() -> JSONResponse:
    """List all accounts with email / availability / balance, plus total & available counts."""
    runtime = _runtime()
    data = await runtime.repo.get_accounts_summary()
    return JSONResponse(data)


@app.post("/admin/accounts/refresh-points", response_model=None, dependencies=[Depends(require_admin_auth)])
async def admin_refresh_account_points(request: Request) -> JSONResponse:
    """Fetch real-time points for a specific account from Poe and update the database."""
    runtime = _runtime()
    body = await request.json()
    email = (body.get("email") or "").strip()
    if not email:
        _openai_http_error(400, "invalid_request_error", "Field 'email' is required")

    account_doc = await runtime.repo.get_account_by_email(email)
    if not account_doc:
        _openai_http_error(404, "not_found_error", f"Account with email '{email}' not found")

    account_id = str(account_doc["_id"])
    try:
        client = await runtime.pool.get_client(account_doc)
        settings = await client.get_settings()
        message_info = settings.get("messagePointInfo", {})
        subscription = settings.get("subscription", {})
        balance = int(message_info.get("subscriptionPointBalance", 0) or 0) + int(
            message_info.get("addonPointBalance", 0) or 0
        )
        subscription_active = bool(subscription.get("isActive", False))
        await runtime.repo.update_account_health(
            account_id,
            balance=balance,
            subscription_active=subscription_active,
            depleted_threshold=runtime.config.depleted_threshold,
        )
        await runtime.repo.mark_account_success(account_id)
        if balance <= runtime.config.depleted_threshold:
            await runtime.pool.invalidate_client(account_id)
        return JSONResponse(
            {
                "email": email,
                "message_point_balance": balance,
                "subscription_active": subscription_active,
                "status": "depleted" if balance <= runtime.config.depleted_threshold else "active",
                "raw": {
                    "subscriptionPointBalance": message_info.get("subscriptionPointBalance"),
                    "addonPointBalance": message_info.get("addonPointBalance"),
                },
            }
        )
    except Exception as exc:
        _openai_http_error(502, "provider_error", f"Failed to fetch points from Poe: {exc}")


@app.post("/admin/accounts/refresh-all", response_model=None, dependencies=[Depends(require_admin_auth)])
async def admin_refresh_all_account_points(request: Request) -> JSONResponse:
    """并发刷新所有账号（或指定状态的账号）的实时积分，返回汇总结果。

    可选请求体（JSON）：
    - statuses: list[str]  要刷新的账号状态列表，默认 ["active","depleted","cooldown","invalid"]
    - concurrency: int     并发刷新数量，默认 10
    """
    runtime = _runtime()
    try:
        body = await request.json()
    except Exception:
        body = {}

    raw_statuses = body.get("statuses")
    statuses: Optional[list[str]] = None
    if isinstance(raw_statuses, list) and raw_statuses:
        statuses = [str(s).strip() for s in raw_statuses if str(s).strip()]

    raw_concurrency = body.get("concurrency", 10)
    try:
        concurrency = max(1, min(int(raw_concurrency), 50))
    except (TypeError, ValueError):
        concurrency = 10

    logger.info(
        "admin_refresh_all_account_points triggered: statuses={} concurrency={}",
        statuses,
        concurrency,
    )
    result = await runtime.refresher.refresh_all_accounts(
        statuses=statuses,
        concurrency=concurrency,
    )
    return JSONResponse(result)


@app.api_route("/models/{model}", methods=["GET", "POST", "PUT", "PATCH", "HEAD"], response_model=None)
@app.api_route("/models", methods=["GET", "POST", "PUT", "PATCH", "HEAD"], response_model=None)
@app.api_route("/v1/models/{model}", methods=["GET", "POST", "PUT", "PATCH", "HEAD"], response_model=None)
@app.api_route("/v1/models", methods=["GET", "POST", "PUT", "PATCH", "HEAD"], response_model=None)
async def list_models(
    request: Request,
    model: Optional[str] = None,
    _auth: None = Depends(require_service_auth),
) -> JSONResponse:
    request.state.model = model or "models"
    if model:
        if model not in app.state.models:
            _openai_http_error(404, "not_found_error", "Model not found")
        meta = app.state.models[model]
        return JSONResponse(
            {
                "id": model,
                "object": "model",
                "created": await helpers.__generate_timestamp(),
                "owned_by": meta["owned_by"],
                "tokens": _model_tokens(meta),
                "endpoints": _model_endpoints(meta),
            }
        )

    models_data = [
        {
            "id": model_name,
            "object": "model",
            "created": await helpers.__generate_timestamp(),
            "owned_by": values["owned_by"],
            "tokens": _model_tokens(values),
            "endpoints": _model_endpoints(values),
        }
        for model_name, values in app.state.models.items()
    ]
    return JSONResponse({"object": "list", "data": models_data})


async def call_tools(client, messages, tools, tool_choice):
    try:
        response = await message_handler("gpt4_o_mini", messages, 128000, tools, tool_choice)
        tool_calls = None
        async for chunk in client.send_message(bot="gpt4_o_mini", message=response["message"]):
            try:
                raw = chunk.get("text", "").strip().replace("\n", "").replace("\\", "")
                parsed = orjson.loads(raw)
                if isinstance(parsed, list):
                    tool_calls = parsed
                    break
            except Exception:
                pass
        return tool_calls
    except Exception:
        return None


async def image_handler(base_model: str, prompt: str, tokens_limit: int) -> dict:
    try:
        message = await helpers.__progressive_summarize_text(prompt, min(len(prompt), tokens_limit))
        return {"bot": base_model, "message": message}
    except Exception as exc:
        _openai_http_error(400, "invalid_request_error", f"Failed to truncate prompt: {exc}")


async def message_handler(
    base_model: str,
    messages: List[Dict[str, str]],
    tokens_limit: int,
    tools: Optional[list[dict[str, str]]] = None,
    tool_choice: Optional[Union[str, dict]] = None,
) -> dict:
    try:
        main_request = messages[-1]["content"]
        for message in messages[::-1]:
            if message["role"] == "user":
                main_request = message["content"]
                break

        if tools:
            rest_tools = await helpers.__convert_functions_format(tools, tool_choice or "auto")
            if messages[0]["role"] == "system":
                messages[0]["content"] += rest_tools
            else:
                messages.insert(0, {"role": "system", "content": rest_tools})

        full_string = await helpers.__stringify_messages(messages=messages)
        history_string = await helpers.__stringify_messages(messages=messages[:-1])
        full_tokens = await helpers.__tokenize(full_string)

        if full_tokens > tokens_limit:
            history_string = await helpers.__progressive_summarize_text(
                history_string,
                max(tokens_limit - await helpers.__tokenize(main_request) - 100, 1),
            )

        message = (
            f"Your current message context: \n{history_string}\n\n"
            f"Reply to most recent message: {main_request}\n\n"
        )
        return {"bot": base_model, "message": message}
    except Exception as exc:
        _openai_http_error(400, "invalid_request_error", f"Failed to process messages: {exc}")


def _guess_suffix_from_url(url: str) -> str:
    path = urlparse(url).path or ""
    suffix = Path(path).suffix.lower()
    return suffix if suffix else ""


def _guess_suffix_from_content_type(content_type: str) -> str:
    if not content_type:
        return ""
    media_type = content_type.split(";", 1)[0].strip().lower()
    guessed = mimetypes.guess_extension(media_type) or ""
    if guessed == ".jpe":
        return ".jpg"
    return guessed


def _cleanup_temp_files(paths: List[str]) -> None:
    for file_path in paths:
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            logger.warning("Failed to remove temp file: {}", file_path)


def _create_temp_file_path(*, prefix: str, suffix: str) -> str:
    fd, tmp_path = tempfile.mkstemp(prefix=prefix, suffix=suffix)
    os.close(fd)
    return tmp_path


def _resolve_image_aspect(model: str, size: Optional[str]) -> str:
    # `auto` means "let provider choose default size/aspect", so we do not force any aspect.
    normalized_size = (size or "").strip().lower()
    if normalized_size in ("", "auto", "default", "1024x1024"):
        return ""

    model_sizes = app.state.models.get(model, {}).get("sizes", {})
    if size in model_sizes:
        return model_sizes[size]

    # Accept ratio form like "16:9" and pass through.
    if ":" in normalized_size:
        parts = normalized_size.split(":", 1)
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            left = int(parts[0])
            right = int(parts[1])
            if left > 0 and right > 0:
                return f"--aspect {left}:{right}"

    # Accept free-form WxH (e.g. 1536x1024) and convert to reduced ratio.
    if "x" in normalized_size:
        parts = normalized_size.split("x", 1)
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            width = int(parts[0])
            height = int(parts[1])
            if width > 0 and height > 0:
                gcd = math.gcd(width, height)
                return f"--aspect {width // gcd}:{height // gcd}"

    supported_sizes = ["auto", "1024x1024"]
    if isinstance(model_sizes, dict):
        supported_sizes.extend(model_sizes.keys())
    _openai_http_error(
        400,
        "invalid_request_error",
        f"Invalid size for model {model}. Supported values: {sorted(set(supported_sizes))}",
    )


async def _materialize_remote_attachments(attachments: List[str]) -> tuple[List[str], List[str]]:
    if not attachments:
        return [], []

    resolved_paths: List[str] = []
    temp_files: List[str] = []
    remote_urls = [
        item
        for item in attachments
        if isinstance(item, str) and item.lower().startswith(("http://", "https://"))
    ]

    if not remote_urls:
        return list(attachments), []

    remote_to_local: dict[str, str] = {}
    try:
        async with AsyncClient(http2=True, timeout=None, follow_redirects=True) as fetcher:
            for remote_url in remote_urls:
                resp = await fetcher.get(
                    remote_url,
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
                        ),
                        "Accept": "*/*",
                    },
                )
                if resp.status_code >= 400:
                    _openai_http_error(
                        400,
                        "invalid_request_error",
                        f"Failed to download attachment: {remote_url} (HTTP {resp.status_code})",
                    )

                content_type = (resp.headers.get("Content-Type") or "").lower()
                suffix = _guess_suffix_from_content_type(content_type) or _guess_suffix_from_url(remote_url) or ".bin"
                tmp_path = _create_temp_file_path(prefix="poe_att_", suffix=suffix)
                temp_files.append(tmp_path)
                async with aiofiles.open(tmp_path, "wb") as tmp:
                    await tmp.write(resp.content)
                remote_to_local[remote_url] = tmp_path
    except Exception:
        _cleanup_temp_files(temp_files)
        raise

    for item in attachments:
        if isinstance(item, str) and item in remote_to_local:
            resolved_paths.append(remote_to_local[item])
        else:
            resolved_paths.append(item)
    return resolved_paths, temp_files


async def generate_image(client, response: dict, aspect_ratio: str, image: list = None) -> str:
    image = image or []
    message = (response.get("message") or "").strip()
    normalized_aspect = (aspect_ratio or "").strip()
    if normalized_aspect and not re.search(r"(^|\s)--aspect(\s+|$)", message, flags=re.IGNORECASE):
        message = f"{message} {normalized_aspect}".strip()
    try:
        async for chunk in client.send_message(
            bot=response["bot"],
            message=message,
            file_path=image,
        ):
            pass
        # 优先从 attachments 中提取图片 URL（如 Qwen-Image 等把图片作为附件返回的模型）
        attachments = chunk.get("attachments") or []
        attachment_urls = []
        for att in attachments:
            # 优先取 file.url（不带 pmaid 参数的干净 URL），其次取顶层 url
            file_info = att.get("file") or {}
            url = file_info.get("url") or att.get("url") or ""
            mime = file_info.get("mimeType") or ""
            if url and (mime.startswith("image/") or not mime):
                attachment_urls.append(url)
        if attachment_urls:
            return " ".join(attachment_urls)
        # fallback：从文本中解析 URL（适用于把 URL 直接写在文本里的模型）
        return chunk["text"]
    except Exception as exc:
        detail = _exception_summary(exc)
        logger.error(
            "generate_image_failed bot={} aspect_ratio={} detail={}",
            response.get("bot"),
            normalized_aspect,
            detail,
        )
        raise RuntimeError(f"Failed to generate image: {detail}") from exc


async def create_completion_data(
    *,
    completion_id: str,
    created: int,
    model: str,
    chunk: Optional[str] = None,
    finish_reason: Optional[str] = None,
    include_usage: bool = False,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    raw_tool_calls: Optional[list[dict[str, Any]]] = None,
) -> Dict[str, Union[str, list, float]]:
    completion_data = ChatCompletionChunk(
        id=f"chatcmpl-{completion_id}",
        object="chat.completion.chunk",
        created=created,
        model=model,
        choices=[
            ChatCompletionChunkChoice(
                index=0,
                delta=MessageResponse(
                    role="assistant",
                    content=chunk,
                    tool_calls=[
                        ChoiceDeltaToolCall(
                            index=raw_tool_calls.index(tool_call),
                            id=f"call-{await helpers.__generate_completion_id()}",
                            function=ChoiceDeltaToolCallFunction(
                                name=tool_call["name"],
                                arguments=orjson.dumps(tool_call["arguments"]).decode("utf-8"),
                            ),
                        )
                        for tool_call in raw_tool_calls
                    ]
                    if raw_tool_calls
                    else None,
                ),
                finish_reason=finish_reason,
            )
        ],
    )

    if include_usage:
        completion_data.usage = None
        if finish_reason in ("stop", "length", "tool_calls"):
            completion_data.usage = ChatCompletionUsage(
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=prompt_tokens + completion_tokens,
            )

    return completion_data.model_dump()


async def _finalize_success(runtime: GatewayRuntime, account_id: str) -> None:
    await runtime.repo.mark_account_success(account_id)


async def generate_chunks(
    *,
    runtime: GatewayRuntime,
    client,
    response: dict,
    model: str,
    completion_id: str,
    prompt_tokens: int,
    attachment_paths: List[str],
    temp_files: List[str],
    max_tokens: Optional[int],
    include_usage: bool,
    raw_tool_calls: Optional[list[dict[str, Any]]],
    chat_code: Optional[str],
    chat_id: Optional[int],
    session_id: str,
    persistent_session: bool,
    account_id: str,
    lease: AccountLease,
) -> AsyncGenerator[bytes, None]:
    completion_timestamp = await helpers.__generate_timestamp()
    emitted_done = False
    completion_tokens = 0
    finish_reason = "stop"
    try:
        if not raw_tool_calls:
            async for chunk in client.send_message(
                bot=response["bot"],
                message=response["message"],
                file_path=attachment_paths,
                chatCode=chat_code,
                chatId=chat_id,
            ):
                if persistent_session:
                    incoming_chat_code = chunk.get("chatCode") or chat_code
                    incoming_chat_id = chunk.get("chatId") or chat_id
                    if incoming_chat_code and incoming_chat_id:
                        chat_code = incoming_chat_code
                        chat_id = incoming_chat_id
                        await runtime.sessions.update_session_chat(
                            session_id=session_id,
                            model=model,
                            account_id=account_id,
                            chat_code=chat_code,
                            chat_id=chat_id,
                        )
                completion_tokens = await helpers.__tokenize(chunk["text"])
                if max_tokens and completion_tokens >= max_tokens:
                    await client.cancel_message(chunk)
                    finish_reason = "length"
                    break

                content = await create_completion_data(
                    completion_id=completion_id,
                    created=completion_timestamp,
                    model=model,
                    chunk=chunk["response"],
                    include_usage=include_usage,
                )
                yield b"data: " + orjson.dumps(content) + b"\n\n"
                await asyncio.sleep(0.001)

            end_chunk = await create_completion_data(
                completion_id=completion_id,
                created=completion_timestamp,
                model=model,
                finish_reason=finish_reason,
                include_usage=include_usage,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
            )
            yield b"data: " + orjson.dumps(end_chunk) + b"\n\n"
        else:
            completion_tokens = await helpers.__tokenize(
                "".join([str(tool_call["name"]) + str(tool_call["arguments"]) for tool_call in raw_tool_calls])
            )
            tool_chunk = await create_completion_data(
                completion_id=completion_id,
                created=completion_timestamp,
                model=model,
                chunk=None,
                finish_reason="tool_calls",
                include_usage=include_usage,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                raw_tool_calls=raw_tool_calls,
            )
            yield b"data: " + orjson.dumps(tool_chunk) + b"\n\n"
            await asyncio.sleep(0.01)

        yield b"data: [DONE]\n\n"
        emitted_done = True
        await _finalize_success(runtime, account_id)
    except asyncio.CancelledError:
        # Client closed the streaming connection; avoid noisy ASGI stack traces.
        pass
    except Exception as exc:
        payload, _ = await _account_error_payload(runtime, account_id, session_id, persistent_session, exc)
        yield b"data: " + orjson.dumps(payload) + b"\n\n"
        if not emitted_done:
            yield b"data: [DONE]\n\n"
    finally:
        _cleanup_temp_files(temp_files)
        runtime.refresher.schedule_refresh(account_id)
        await runtime.pool.invalidate_client(account_id)
        await lease.release()


async def streaming_response(
    *,
    runtime: GatewayRuntime,
    client,
    response: dict,
    model: str,
    completion_id: str,
    prompt_tokens: int,
    attachment_paths: List[str],
    temp_files: List[str],
    max_tokens: Optional[int],
    include_usage: bool,
    raw_tool_calls: Optional[list[dict[str, Any]]],
    chat_code: Optional[str],
    chat_id: Optional[int],
    session_id: str,
    persistent_session: bool,
    account_id: str,
    lease: AccountLease,
) -> StreamingResponse:
    return StreamingResponse(
        content=generate_chunks(
            runtime=runtime,
            client=client,
            response=response,
            model=model,
            completion_id=completion_id,
            prompt_tokens=prompt_tokens,
            attachment_paths=attachment_paths,
            temp_files=temp_files,
            max_tokens=max_tokens,
            include_usage=include_usage,
            raw_tool_calls=raw_tool_calls,
            chat_code=chat_code,
            chat_id=chat_id,
            session_id=session_id,
            persistent_session=persistent_session,
            account_id=account_id,
            lease=lease,
        ),
        status_code=200,
        headers={"Content-Type": "text/event-stream", "X-Request-ID": str(uuid.uuid4())},
    )


async def non_streaming_response(
    *,
    runtime: GatewayRuntime,
    client,
    response: dict,
    model: str,
    completion_id: str,
    prompt_tokens: int,
    attachment_paths: List[str],
    temp_files: List[str],
    max_tokens: Optional[int],
    raw_tool_calls: Optional[list[dict[str, Any]]],
    chat_code: Optional[str],
    chat_id: Optional[int],
    session_id: str,
    persistent_session: bool,
    account_id: str,
    lease: AccountLease,
) -> JSONResponse:
    try:
        if not raw_tool_calls:
            finish_reason = "stop"
            async for chunk in client.send_message(
                bot=response["bot"],
                message=response["message"],
                file_path=attachment_paths,
                chatCode=chat_code,
                chatId=chat_id,
            ):
                if persistent_session:
                    incoming_chat_code = chunk.get("chatCode") or chat_code
                    incoming_chat_id = chunk.get("chatId") or chat_id
                    if incoming_chat_code and incoming_chat_id:
                        chat_code = incoming_chat_code
                        chat_id = incoming_chat_id
                        await runtime.sessions.update_session_chat(
                            session_id=session_id,
                            model=model,
                            account_id=account_id,
                            chat_code=chat_code,
                            chat_id=chat_id,
                        )
                if max_tokens and await helpers.__tokenize(chunk["text"]) >= max_tokens:
                    await client.cancel_message(chunk)
                    finish_reason = "length"
                    break
            completion_tokens = await helpers.__tokenize(chunk["text"])
            message_content = chunk["text"]
        else:
            completion_tokens = await helpers.__tokenize(
                "".join([str(tool_call["name"]) + str(tool_call["arguments"]) for tool_call in raw_tool_calls])
            )
            finish_reason = "tool_calls"
            message_content = None

        content = ChatCompletionResponse(
            id=f"chatcmpl-{completion_id}",
            object="chat.completion",
            created=await helpers.__generate_timestamp(),
            model=model,
            usage=ChatCompletionUsage(
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=prompt_tokens + completion_tokens,
            ),
            choices=[
                ChatCompletionResponseChoice(
                    index=0,
                    message=MessageResponse(
                        role="assistant",
                        content=message_content,
                        tool_calls=[
                            ChatCompletionMessageToolCall(
                                id=f"call-{await helpers.__generate_completion_id()}",
                                function=FunctionCall(
                                    name=tool_call["name"],
                                    arguments=orjson.dumps(tool_call["arguments"]).decode("utf-8"),
                                ),
                            )
                            for tool_call in raw_tool_calls
                        ]
                        if raw_tool_calls
                        else None,
                    ),
                    finish_reason=finish_reason,
                )
            ],
        )
        await _finalize_success(runtime, account_id)
        return JSONResponse(content.model_dump())
    except Exception as exc:
        payload, status = await _account_error_payload(runtime, account_id, session_id, persistent_session, exc)
        raise HTTPException(status_code=status, detail=payload)
    finally:
        _cleanup_temp_files(temp_files)
        runtime.refresher.schedule_refresh(account_id)
        await runtime.pool.invalidate_client(account_id)
        await lease.release()


async def _acquire_account_for_chat(
    runtime: GatewayRuntime,
    *,
    session_id: str,
    persistent_session: bool,
    model: str,
) -> tuple[dict[str, Any], AccountLease, Optional[str], Optional[int]]:
    if persistent_session:
        session = await runtime.sessions.get_session(session_id)
        if session:
            if session.get("state") == "broken":
                _openai_http_error(
                    409,
                    "invalid_request_error",
                    "Session is broken. Create a new session_id and retry.",
                    {"session_id": session_id},
                )
            account_id = session.get("account_id")
            if not account_id:
                _openai_http_error(409, "invalid_request_error", "Session has no bound account")
            account_doc = await runtime.repo.get_account_by_id(account_id)
            if not account_doc or account_doc.get("status") != "active":
                await runtime.sessions.break_session(session_id, "bound account unavailable")
                _openai_http_error(
                    409,
                    "invalid_request_error",
                    "Bound account is unavailable. Create a new session_id and retry.",
                    {"session_id": session_id},
                )
            wait_sec = max(0.01, float(runtime.config.acquire_wait_poll_seconds))
            while not runtime.pool.has_client(account_id):
                await asyncio.sleep(wait_sec)
            while not await runtime.limiter.try_acquire(account_id):
                await asyncio.sleep(wait_sec)
            lease = AccountLease(account_id=account_id, limiter=runtime.limiter)
            await runtime.repo.touch_session(session_id)
            return account_doc, lease, session.get("chat_code"), session.get("chat_id")

    try:
        account_doc, lease = await _acquire_prewarmed_account(runtime)
    except NoAccountAvailableError:
        _openai_http_error(402, "insufficient_credits", "No active accounts available")

    account_id = str(account_doc["_id"])
    if persistent_session:
        await runtime.sessions.bind_session(
            session_id=session_id,
            model=model,
            account_id=account_id,
            chat_code=None,
            chat_id=None,
        )
    return account_doc, lease, None, None


@app.api_route("/chat/completions", methods=["POST", "OPTIONS"], response_model=None)
@app.api_route("/v1/chat/completions", methods=["POST", "OPTIONS"], response_model=None)
async def chat_completions(
    request: Request,
    data: ChatData,
    _auth: None = Depends(require_service_auth),
) -> Union[StreamingResponse, JSONResponse]:
    return await _chat_completions_impl(request, data)


async def _chat_completions_impl(
    request: Request,
    data: ChatData,
) -> Union[StreamingResponse, JSONResponse]:
    runtime = _runtime()
    messages = data.messages
    model = data.model
    streaming = bool(data.stream)
    max_tokens = data.max_completion_tokens or data.max_tokens
    stream_options = data.stream_options
    tools = data.tools
    tool_choice = data.tool_choice
    metadata = data.metadata
    user = data.user
    request.state.model = model

    messages_valid, messages_error = await helpers.__validate_messages_format_detail(messages)
    if not messages_valid:
        _openai_http_error(
            400,
            "invalid_request_error",
            f"Invalid messages format: {messages_error}",
            {"reason": messages_error},
        )
    if model not in app.state.models:
        _openai_http_error(404, "not_found_error", "Invalid model")
    if data.n not in (None, 1):
        _openai_http_error(400, "invalid_request_error", "n must be exactly 1")
    if tools and len(tools) > 20:
        _openai_http_error(400, "invalid_request_error", "Maximum 20 tools are allowed")

    include_usage = stream_options.get("include_usage", False) if stream_options else False
    model_data = app.state.models[model]
    base_model = model_data["baseModel"]
    tokens_limit = _model_tokens(model_data)
    premium_model = bool(model_data.get("premium_model", False))

    session_id, persistent_session = runtime.sessions.resolve_session_id(metadata, user)
    request.state.session_id = session_id

    account_doc, lease, chat_code, chat_id = await _acquire_account_for_chat(
        runtime,
        session_id=session_id,
        persistent_session=persistent_session,
        model=model,
    )
    account_id = str(account_doc["_id"])
    request.state.account_id = account_id

    if premium_model and not bool(account_doc.get("subscription_active", False)):
        await runtime.refresher.refresh_account(account_id)
        refreshed = await runtime.repo.get_account_by_id(account_id)
        if not refreshed or not refreshed.get("subscription_active", False):
            await lease.release()
            _openai_http_error(
                402,
                "insufficient_credits",
                "Premium model requires an active subscription on selected account",
            )
        account_doc = refreshed

    try:
        client = await runtime.pool.get_client_for_account(account_doc, create_if_missing=False)
    except Exception as exc:
        payload, status = await _account_error_payload(runtime, account_id, session_id, persistent_session, exc)
        await lease.release()
        raise HTTPException(status_code=status, detail=payload)

    text_messages, image_urls = await helpers.__split_content(messages)
    response = await message_handler(base_model, text_messages, tokens_limit)
    prompt_tokens = await helpers.__tokenize("".join([str(message) for message in response["message"]]))

    if prompt_tokens > tokens_limit:
        await lease.release()
        _openai_http_error(
            413,
            "request_too_large",
            f"Your prompt exceeds the maximum context length of {tokens_limit} tokens",
        )
    if max_tokens and (max_tokens + prompt_tokens) > tokens_limit:
        await lease.release()
        _openai_http_error(
            413,
            "request_too_large",
            (
                f"This model's maximum context length is {tokens_limit}. "
                f"Request exceeds limit ({max_tokens} in max_tokens, {prompt_tokens} in prompt)"
            ),
        )

    raw_tool_calls = None
    if tools:
        raw_tool_calls = await call_tools(client, text_messages, tools, tool_choice)
    if raw_tool_calls:
        response = {"bot": "gpt4_o_mini", "message": ""}
        prompt_tokens = await helpers.__tokenize("".join([str(message["content"]) for message in text_messages]))

    completion_id = await helpers.__generate_completion_id()

    attachment_paths: List[str] = []
    temp_files: List[str] = []
    if not raw_tool_calls and image_urls:
        try:
            attachment_paths, temp_files = await _materialize_remote_attachments(image_urls)
        except HTTPException:
            await lease.release()
            raise
        except Exception as exc:
            await lease.release()
            _openai_http_error(
                400,
                "invalid_request_error",
                f"Failed to process attachments: {exc}",
            )

    if streaming:
        return await streaming_response(
            runtime=runtime,
            client=client,
            response=response,
            model=model,
            completion_id=completion_id,
            prompt_tokens=prompt_tokens,
            attachment_paths=attachment_paths,
            temp_files=temp_files,
            max_tokens=max_tokens,
            include_usage=include_usage,
            raw_tool_calls=raw_tool_calls,
            chat_code=chat_code,
            chat_id=chat_id,
            session_id=session_id,
            persistent_session=persistent_session,
            account_id=account_id,
            lease=lease,
        )

    return await non_streaming_response(
        runtime=runtime,
        client=client,
        response=response,
        model=model,
        completion_id=completion_id,
        prompt_tokens=prompt_tokens,
        attachment_paths=attachment_paths,
        temp_files=temp_files,
        max_tokens=max_tokens,
        raw_tool_calls=raw_tool_calls,
        chat_code=chat_code,
        chat_id=chat_id,
        session_id=session_id,
        persistent_session=persistent_session,
        account_id=account_id,
        lease=lease,
    )


@app.api_route("/images/generations", methods=["POST", "OPTIONS"], response_model=None)
@app.api_route("/v1/images/generations", methods=["POST", "OPTIONS"], response_model=None)
async def create_images(
    request: Request,
    data: ImagesGenData,
    _auth: None = Depends(require_service_auth),
) -> JSONResponse:
    runtime = _runtime()
    prompt, model, n, size = data.prompt, data.model, data.n, data.size
    request.state.model = model

    if not isinstance(prompt, str):
        _openai_http_error(400, "invalid_request_error", "Invalid prompt")
    if model not in app.state.models:
        _openai_http_error(404, "not_found_error", "Invalid model")
    if not isinstance(n, int) or n < 1:
        _openai_http_error(400, "invalid_request_error", "Invalid n value")

    aspect_ratio = _resolve_image_aspect(model, size)

    model_data = app.state.models[model]
    base_model = model_data["baseModel"]
    tokens_limit = _model_tokens(model_data)
    premium_model = bool(model_data.get("premium_model", False))

    try:
        account_doc, lease = await _acquire_prewarmed_account(runtime)
    except NoAccountAvailableError:
        _openai_http_error(402, "insufficient_credits", "No active accounts available")
    account_id = str(account_doc["_id"])
    request.state.account_id = account_id

    if premium_model and not bool(account_doc.get("subscription_active", False)):
        await lease.release()
        _openai_http_error(402, "insufficient_credits", "Premium model requires active subscription")

    try:
        client = await runtime.pool.get_client_for_account(account_doc, create_if_missing=False)
        response = await image_handler(base_model, prompt, tokens_limit)

        urls: list[str] = []
        for _ in range(n):
            image_generation = await generate_image(client, response, aspect_ratio)
            logger.info("Raw image generation response for model={}: {!r}", model, image_generation)
            extracted = [url for url in image_generation.split() if url.startswith("https://")]
            if not extracted:
                extracted = re.findall(r'https://\S+', image_generation)
            urls.extend(extracted)
            if len(urls) >= n:
                break
        urls = urls[-n:]
        if len(urls) == 0:
            _openai_http_error(500, "provider_error", f"Provider for {model} sent invalid response")

        async with AsyncClient(http2=True, timeout=None) as fetcher:
            for url in urls:
                resp = await fetcher.head(url)
                content_type = resp.headers.get("Content-Type", "")
                if not content_type.startswith("image/"):
                    _openai_http_error(500, "provider_error", "The content returned was not an image")

        await _finalize_success(runtime, account_id)
        return JSONResponse(
            {"created": await helpers.__generate_timestamp(), "data": [{"url": url} for url in urls]}
        )
    except HTTPException:
        raise
    except Exception as exc:
        payload, status = await _account_error_payload(runtime, account_id, "ephemeral", False, exc)
        raise HTTPException(status_code=status, detail=payload)
    finally:
        runtime.refresher.schedule_refresh(account_id)
        await runtime.pool.invalidate_client(account_id)
        await lease.release()


@app.api_route("/images/edits", methods=["POST", "OPTIONS"], response_model=None)
@app.api_route("/v1/images/edits", methods=["POST", "OPTIONS"], response_model=None)
async def edit_images(
    request: Request,
    _auth: None = Depends(require_service_auth),
) -> JSONResponse:
    runtime = _runtime()
    content_type = (request.headers.get("content-type") or "").lower()
    form_keys: List[str] = []

    if "multipart/form-data" in content_type:
        form = await request.form()
        form_keys = [str(k) for k in form.keys()]
        image = form.get("image")
        if image is None:
            images = form.getlist("image")
            image = images[0] if images else None
        if image is None:
            image = form.get("image[]")
        if image is None:
            images = form.getlist("image[]")
            image = images[0] if images else None
        if image is None:
            for key, value in form.multi_items():
                if key.startswith("image") and isinstance(value, (UploadFile, StarletteUploadFile)):
                    image = value
                    break

        prompt = form.get("prompt")
        model = form.get("model")
        raw_n = form.get("n", 1)
        raw_size = form.get("size", "1024x1024")
        try:
            n = int(raw_n)
        except Exception:
            _openai_http_error(400, "invalid_request_error", "Invalid n value")
        size = raw_size if isinstance(raw_size, str) else "1024x1024"
    else:
        payload = await request.json()
        data = ImagesEditData(**payload)
        image, prompt, model, n, size = data.image, data.prompt, data.model, data.n, data.size

    request.state.model = model

    if not isinstance(image, (str, UploadFile, StarletteUploadFile)):
        debug_suffix = ""
        if form_keys:
            debug_suffix = f"; form_keys={form_keys}; image_type={type(image).__name__}"
        _openai_http_error(400, "invalid_request_error", f"Invalid image input{debug_suffix}")
    if not isinstance(prompt, str):
        _openai_http_error(400, "invalid_request_error", "Invalid prompt")
    if model not in app.state.models:
        _openai_http_error(404, "not_found_error", "Invalid model")
    if not isinstance(n, int) or n < 1:
        _openai_http_error(400, "invalid_request_error", "Invalid n value")

    aspect_ratio = _resolve_image_aspect(model, size)

    model_data = app.state.models[model]
    base_model = model_data["baseModel"]
    tokens_limit = _model_tokens(model_data)
    premium_model = bool(model_data.get("premium_model", False))

    try:
        account_doc, lease = await _acquire_prewarmed_account(runtime)
    except NoAccountAvailableError:
        _openai_http_error(402, "insufficient_credits", "No active accounts available")
    account_id = str(account_doc["_id"])
    request.state.account_id = account_id

    if premium_model and not bool(account_doc.get("subscription_active", False)):
        await lease.release()
        _openai_http_error(402, "insufficient_credits", "Premium model requires active subscription")

    edit_attachment = image
    edit_temp_files: List[str] = []
    try:
        if isinstance(image, (UploadFile, StarletteUploadFile)):
            suffix = Path(image.filename or "").suffix
            if not suffix:
                ctype = (image.content_type or "").split(";", 1)[0].strip().lower()
                suffix = _guess_suffix_from_content_type(ctype) or ".jpg"
            tmp_path = _create_temp_file_path(prefix="poe_edit_", suffix=suffix)
            edit_temp_files.append(tmp_path)
            async with aiofiles.open(tmp_path, "wb") as tmp:
                while True:
                    chunk = await image.read(1024 * 1024)
                    if not chunk:
                        break
                    await tmp.write(chunk)
            edit_attachment = tmp_path
        elif isinstance(image, str) and image.lower().startswith(("http://", "https://")):
            materialized_paths, remote_temp_files = await _materialize_remote_attachments([image])
            if not materialized_paths:
                _openai_http_error(400, "invalid_request_error", "Failed to materialize edit image")
            edit_temp_files.extend(remote_temp_files)
            edit_attachment = materialized_paths[0]
        elif isinstance(image, str):
            if not os.path.exists(image):
                _openai_http_error(400, "invalid_request_error", "Invalid image input")
            edit_attachment = image

        client = await runtime.pool.get_client_for_account(account_doc, create_if_missing=False)
        response = await image_handler(base_model, prompt, tokens_limit)

        urls: list[str] = []
        for _ in range(n):
            image_generation = await generate_image(client, response, aspect_ratio, [edit_attachment])
            logger.info("Raw edit generation response for model={}: {!r}", model, image_generation)
            extracted = [url for url in image_generation.split() if url.startswith("https://")]
            if not extracted:
                extracted = re.findall(r'https://\S+', image_generation)
            urls.extend(extracted)
            if len(urls) >= n:
                break
        urls = urls[-n:]
        if len(urls) == 0:
            _openai_http_error(500, "provider_error", f"Provider for {model} sent invalid response")

        async with AsyncClient(http2=True, timeout=None) as fetcher:
            for url in urls:
                resp = await fetcher.head(url)
                content_type = resp.headers.get("Content-Type", "")
                if not content_type.startswith("image/"):
                    _openai_http_error(500, "provider_error", "The content returned was not an image")

        await _finalize_success(runtime, account_id)
        return JSONResponse(
            {"created": await helpers.__generate_timestamp(), "data": [{"url": url} for url in urls]}
        )
    except HTTPException:
        raise
    except Exception as exc:
        payload, status = await _account_error_payload(runtime, account_id, "ephemeral", False, exc)
        raise HTTPException(status_code=status, detail=payload)
    finally:
        _cleanup_temp_files(edit_temp_files)
        runtime.refresher.schedule_refresh(account_id)
        await runtime.pool.invalidate_client(account_id)
        await lease.release()


def _normalize_responses_input(input_data: Any) -> list[dict[str, Any]]:
    if isinstance(input_data, str):
        return [{"role": "user", "content": input_data}]
    if not isinstance(input_data, list):
        return [{"role": "user", "content": str(input_data)}]

    messages: list[dict[str, Any]] = []
    for item in input_data:
        if isinstance(item, dict) and "role" in item and "content" in item:
            content = item["content"]
            if isinstance(content, list):
                parts: list[dict[str, Any]] = []
                for sub in content:
                    if not isinstance(sub, dict):
                        continue
                    subtype = sub.get("type")
                    if subtype in ("input_text", "output_text", "text"):
                        parts.append({"type": "text", "text": sub.get("text", "")})
                    elif subtype in ("image_url",):
                        parts.append({"type": "image_url", "image_url": sub.get("image_url")})
                messages.append({"role": item["role"], "content": parts or ""})
            else:
                messages.append({"role": item["role"], "content": content})
        elif isinstance(item, dict) and item.get("type") in ("input_text", "output_text", "text"):
            messages.append({"role": "user", "content": item.get("text", "")})
        elif isinstance(item, str):
            messages.append({"role": "user", "content": item})
    return messages or [{"role": "user", "content": ""}]


@app.api_route("/responses", methods=["POST", "OPTIONS"], response_model=None)
@app.api_route("/v1/responses", methods=["POST", "OPTIONS"], response_model=None)
async def create_responses(
    request: Request,
    data: ResponsesData,
    _auth: None = Depends(require_service_auth),
) -> Union[StreamingResponse, JSONResponse]:
    chat_payload = ChatData(
        model=data.model,
        messages=_normalize_responses_input(data.input),
        stream=data.stream,
        max_tokens=data.max_output_tokens,
        metadata=data.metadata,
        user=data.user,
    )
    chat_response = await _chat_completions_impl(request, chat_payload)

    if data.stream:
        if isinstance(chat_response, StreamingResponse):
            chat_response.headers["x-poe-responses-compat"] = "chat-completion-stream"
        return chat_response

    if not isinstance(chat_response, JSONResponse):
        return chat_response

    payload = orjson.loads(chat_response.body)
    assistant_text = ""
    if payload.get("choices"):
        assistant_text = payload["choices"][0].get("message", {}).get("content") or ""
    usage = payload.get("usage", {})
    responses_payload = {
        "id": f"resp_{uuid.uuid4().hex}",
        "object": "response",
        "created_at": await helpers.__generate_timestamp(),
        "status": "completed",
        "model": data.model,
        "output": [
            {
                "id": f"msg_{uuid.uuid4().hex}",
                "type": "message",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": assistant_text,
                        "annotations": [],
                    }
                ],
            }
        ],
        "usage": {
            "input_tokens": usage.get("prompt_tokens", 0),
            "output_tokens": usage.get("completion_tokens", 0),
            "total_tokens": usage.get("total_tokens", 0),
        },
    }
    return JSONResponse(responses_payload, headers=dict(chat_response.headers))


@app.api_route(
    "/v1/{full_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    response_model=None,
)
async def unsupported_v1_path(
    full_path: str,
    _auth: None = Depends(require_service_auth),
) -> JSONResponse:
    _openai_http_error(
        400,
        "not_supported_error",
        f"/v1/{full_path} is not supported by this gateway yet",
    )


if __name__ == "__main__":
    uvicorn.run("poe_api_wrapper.service.gateway_api:app", host="127.0.0.1", port=8000, workers=1)


def start_server(tokens: Optional[list] = None, address: str = "127.0.0.1", port: str = "8000"):
    if tokens is not None:
        logger.warning(
            "The `tokens` argument is ignored in Mongo gateway mode. "
            "Use /admin/accounts/upsert to manage accounts."
        )
    uvicorn.run("poe_api_wrapper.service.gateway_api:app", host=address, port=int(port), workers=1)
