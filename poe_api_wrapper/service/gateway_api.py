from __future__ import annotations

import asyncio
import math
import mimetypes
import os
import re
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse
from starlette.background import BackgroundTask

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
    PoolMonitor,
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
MODELS_PATH = DIR / "models.json"
DEFAULT_MODEL_TOKENS = 128000
DEFAULT_MODEL_ENDPOINTS = [
    "/v1/chat/completions",
    "/v1/images/generations",
    "/v1/images/edits",
]

# Poe 上游以累积全文（而非增量 delta）返回 chunk 的 baseModel 列表（小写匹配）。
# gateway 层会自动做差值处理，将其转换为标准逐块流。
_RETRYABLE_STATUS_CODES = {402, 403, 429, 500, 502, 503, 504}
_MAX_CHAT_ATTEMPTS = 2

# 重试时的模型降级映射（前缀 → 降级目标 baseModel）
_RETRY_DOWNGRADE_RULES: list[tuple[str, str]] = [
    ("gpt", "GPT-5-nano"),
    ("claude", "Claude-Haiku-4.5"),
    ("gemini", "Gemini-3.1-Flash-Lite"),
]


def _downgrade_bot_for_retry(bot: str) -> str:
    """重试时将高端模型降级为轻量模型，减少资源消耗。返回降级后的 baseModel 名。"""
    lower = bot.lower()
    for prefix, fallback_bot in _RETRY_DOWNGRADE_RULES:
        if lower.startswith(prefix):
            return fallback_bot
    return bot

CUMULATIVE_RESPONSE_BOTS: set[str] = {
    "deepseek-v3.1-t",
    "deepseek-v3.2",
    "glm-4.7-fw",
    "kimi-k2-thinking",
    "kimi-k2.5",
    "qwen3-max",
}

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


def _message_content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                item_type = item.get("type")
                if item_type in {"text", "input_text", "output_text"}:
                    text = item.get("text")
                    if isinstance(text, str):
                        parts.append(text)
                        continue
                if item_type == "image_url":
                    image_url = item.get("image_url")
                    if isinstance(image_url, dict):
                        url = image_url.get("url")
                        if isinstance(url, str):
                            parts.append(url)
                            continue
                    if isinstance(image_url, str):
                        parts.append(image_url)
                        continue
            parts.append(str(item))
        return "\n".join(part for part in parts if part)
    if content is None:
        return ""
    return str(content)


def _merge_system_messages_to_index_zero(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not messages:
        return messages
    if any(not isinstance(message, dict) for message in messages):
        return messages

    system_messages: list[dict[str, Any]] = []
    non_system_messages: list[dict[str, Any]] = []
    has_system_after_zero = False

    for idx, message in enumerate(messages):
        copied = dict(message)
        if copied.get("role") == "system":
            if idx > 0:
                has_system_after_zero = True
            system_messages.append(copied)
        else:
            non_system_messages.append(copied)

    if not has_system_after_zero:
        return [dict(message) for message in messages]

    first_system = system_messages[0]
    all_system_contents = [item.get("content") for item in system_messages]
    if all(isinstance(content, list) for content in all_system_contents):
        merged_content: Any = [entry for content in all_system_contents for entry in content]
    else:
        merged_parts = [
            _message_content_to_text(content).strip()
            for content in all_system_contents
        ]
        merged_content = "\n\n".join(part for part in merged_parts if part)
    first_system["content"] = merged_content
    return [first_system, *non_system_messages]


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
    cooldown_seconds: int
    target_pool_size: int
    min_pool_size: int
    max_pool_size: int
    pool_fill_concurrency: int
    pool_monitor_interval_seconds: int
    pool_connect_timeout_seconds: int
    pool_ttl_check_interval_seconds: int
    client_max_age_seconds: int
    acquire_wait_poll_seconds: float
    auto_fetch_poe_revision: bool
    daily_reset_timezone: str
    daily_reset_hour: int
    daily_reset_point_balance: int

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
            cooldown_seconds=_env_int("COOLDOWN_SECONDS", 120),
            target_pool_size=_env_int("TARGET_POOL_SIZE", 20),
            min_pool_size=_env_int("MIN_POOL_SIZE", 5),
            max_pool_size=_env_int("MAX_POOL_SIZE", 30),
            pool_fill_concurrency=_env_int("POOL_FILL_CONCURRENCY", 10),
            pool_monitor_interval_seconds=_env_int("POOL_MONITOR_INTERVAL_SECONDS", 5),
            pool_connect_timeout_seconds=_env_int("POOL_CONNECT_TIMEOUT_SECONDS", 20),
            pool_ttl_check_interval_seconds=_env_int("POOL_TTL_CHECK_INTERVAL_SECONDS", 30),
            client_max_age_seconds=_env_int("CLIENT_MAX_AGE_SECONDS", 600),
            acquire_wait_poll_seconds=max(0.01, _env_int("ACQUIRE_WAIT_POLL_SECONDS_MS", 100) / 1000.0),
            auto_fetch_poe_revision=_env_bool("AUTO_FETCH_POE_REVISION", True),
            daily_reset_timezone=os.getenv("DAILY_RESET_TIMEZONE", "America/Los_Angeles").strip(),
            daily_reset_hour=_env_int("DAILY_RESET_HOUR", 0),
            daily_reset_point_balance=_env_int("DAILY_RESET_POINT_BALANCE", 3000),
        )


@dataclass
class GatewayRuntime:
    config: GatewayConfig
    repo: AccountRepository
    limiter: RuntimeLimiter
    selector: AccountSelector
    pool: PoeClientPool
    refresher: AccountHealthRefresher
    pool_monitor: PoolMonitor
    sessions: SessionManager


def _load_models_from_disk() -> dict[str, Any]:
    with MODELS_PATH.open("rb") as f:
        loaded = orjson.loads(f.read())
    if not isinstance(loaded, dict):
        raise RuntimeError(f"Invalid models data in {MODELS_PATH}: root must be an object")
    return loaded


def _reload_models_in_state() -> dict[str, Any]:
    models = _load_models_from_disk()
    app.state.models = models
    stat = MODELS_PATH.stat()
    loaded_at = int(utc_now().timestamp())
    return {
        "status": "ok",
        "models_count": len(models),
        "loaded_at": loaded_at,
        "models_path": str(MODELS_PATH),
        "file_mtime": int(stat.st_mtime),
        "file_size_bytes": int(stat.st_size),
    }


app.state.models = _load_models_from_disk()


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


@dataclass(frozen=True)
class AccountErrorDecision:
    payload: dict[str, Any]
    status_code: int
    kind: str
    error_text: str


async def _account_error_payload(
    *,
    account_id: str,
    session_id: str,
    persistent_session: bool,
    exc: Exception,
) -> AccountErrorDecision:
    error_text = _exception_summary(exc)
    metadata = {"session_id": session_id, "account_id": account_id}
    if _is_depleted_error(error_text):
        if persistent_session:
            message = "Bound account has no remaining points. Create a new session_id and retry."
        else:
            message = "Insufficient credits on selected account."
        logger.warning(
            "provider_error_classified status=402 type=insufficient_credits account_id={} session_id={} detail={}",
            mask_secret(account_id),
            session_id,
            error_text,
        )
        return AccountErrorDecision(
            payload=build_openai_error(402, "insufficient_credits", message, metadata),
            status_code=402,
            kind="depleted",
            error_text=error_text,
        )

    if _is_invalid_error(error_text):
        if persistent_session:
            message = "Bound account became invalid. Create a new session_id and retry."
        else:
            message = "Selected account failed authorization."
        logger.warning(
            "provider_error_classified status=403 type=authentication_error account_id={} session_id={} detail={}",
            mask_secret(account_id),
            session_id,
            error_text,
        )
        return AccountErrorDecision(
            payload=build_openai_error(403, "authentication_error", message, metadata),
            status_code=403,
            kind="invalid",
            error_text=error_text,
        )

    if _is_provider_timeout_error(error_text):
        logger.warning(
            "provider_error_classified status=504 type=provider_timeout_error account_id={} session_id={} "
            "evicted_client=true detail={}",
            mask_secret(account_id),
            session_id,
            error_text,
        )
        return AccountErrorDecision(
            payload=build_openai_error(
                504,
                "provider_timeout_error",
                "Provider timeout while waiting for Poe response. Retry with backoff.",
                metadata,
            ),
            status_code=504,
            kind="provider_timeout",
            error_text=error_text,
        )

    if _is_rate_limit_error(error_text):
        logger.warning(
            "provider_error_classified status=429 type=rate_limit_error account_id={} session_id={} detail={}",
            mask_secret(account_id),
            session_id,
            error_text,
        )
        return AccountErrorDecision(
            payload=build_openai_error(429, "rate_limit_error", "Rate limit reached. Retry with backoff.", metadata),
            status_code=429,
            kind="rate_limit",
            error_text=error_text,
        )

    logger.error(
        "provider_error_classified status=500 type=provider_error account_id={} session_id={} detail={}",
        mask_secret(account_id),
        session_id,
        error_text,
    )
    return AccountErrorDecision(
        payload=build_openai_error(500, "provider_error", f"Provider error: {error_text}", metadata),
        status_code=500,
        kind="provider_error",
        error_text=error_text,
    )


async def _finalize_account_use(
    runtime: GatewayRuntime,
    *,
    account_id: str,
    lease: Optional[AccountLease] = None,
    session_id: str = "ephemeral",
    persistent_session: bool = False,
    success: bool = False,
    error_decision: Optional[AccountErrorDecision] = None,
    evict_client: bool = True,
    release_lease: bool = True,
) -> None:
    if success:
        await runtime.repo.mark_account_success(account_id)
    elif error_decision:
        if error_decision.kind == "depleted":
            await runtime.repo.mark_account_depleted(account_id, error_decision.error_text)
            if persistent_session:
                await runtime.sessions.break_session(session_id, "bound account is depleted")
        elif error_decision.kind == "invalid":
            await runtime.repo.mark_account_invalid(account_id, error_decision.error_text)
            if persistent_session:
                await runtime.sessions.break_session(session_id, "bound account is invalid")
        elif error_decision.kind == "provider_timeout":
            await runtime.repo.record_account_error(
                account_id,
                error_decision.error_text,
                cooldown_seconds=runtime.config.cooldown_seconds,
                error_threshold=2,
            )
        elif error_decision.kind in {"rate_limit", "provider_error"}:
            await runtime.repo.record_account_error(
                account_id,
                error_decision.error_text,
                cooldown_seconds=runtime.config.cooldown_seconds,
            )

    if evict_client:
        await runtime.pool.invalidate_client(account_id)
    if lease and release_lease:
        await lease.release()


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
        depleted_threshold=config.depleted_threshold,
    )
    selector = AccountSelector(
        repo=repo,
        limiter=limiter,
        pool=pool,
        top_n=max(1, int(config.target_pool_size)),
    )
    refresher = AccountHealthRefresher(
        repo=repo,
        daily_reset_timezone=config.daily_reset_timezone,
        daily_reset_hour=config.daily_reset_hour,
        daily_reset_point_balance=config.daily_reset_point_balance,
    )
    pool_monitor = PoolMonitor(
        repo=repo,
        pool=pool,
        target_pool_size=config.target_pool_size,
        min_pool_size=config.min_pool_size,
        max_pool_size=config.max_pool_size,
        fill_concurrency=config.pool_fill_concurrency,
        monitor_interval_seconds=config.pool_monitor_interval_seconds,
        connect_timeout_seconds=config.pool_connect_timeout_seconds,
        ttl_check_interval_seconds=config.pool_ttl_check_interval_seconds,
    )
    sessions = SessionManager(repo=repo)

    runtime = GatewayRuntime(
        config=config,
        repo=repo,
        limiter=limiter,
        selector=selector,
        pool=pool,
        refresher=refresher,
        pool_monitor=pool_monitor,
        sessions=sessions,
    )
    app.state.runtime = runtime

    await repo.init_indexes()
    await repo.bootstrap_service_keys(config.service_api_keys_bootstrap)



    refresher.start()
    pool_monitor.start()
    # Let the pool know about the main loop so reconnect-exhaustion callbacks
    # can schedule cleanup coroutines on it.
    pool._main_loop = asyncio.get_running_loop()
    logger.info("Gateway startup complete")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    runtime = getattr(app.state, "runtime", None)
    if not runtime:
        return
    await runtime.pool_monitor.stop()
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
        logger.info(
            "Account {} upserted, PoolMonitor will warm it on next cycle if needed",
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


@app.post("/admin/models/reload", response_model=None, dependencies=[Depends(require_admin_auth)])
async def admin_reload_models() -> JSONResponse:
    try:
        result = _reload_models_in_state()
    except Exception as exc:
        logger.exception("Failed to reload models from {}: {}", MODELS_PATH, exc)
        _openai_http_error(500, "provider_error", f"Failed to reload models: {exc}")

    logger.info(
        "admin_reload_models succeeded: models_count={} file_mtime={} path={}",
        result["models_count"],
        result["file_mtime"],
        result["models_path"],
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


def _parse_poe_parameters(parameters: Any) -> Optional[dict[str, Any]]:
    if parameters is None:
        return None
    if isinstance(parameters, dict):
        return dict(parameters)
    if isinstance(parameters, str):
        stripped = parameters.strip()
        if not stripped:
            return None
        try:
            parsed = orjson.loads(stripped)
        except Exception:
            _openai_http_error(400, "invalid_request_error", "Invalid parameters JSON")
            raise AssertionError("unreachable")
        if not isinstance(parsed, dict):
            _openai_http_error(400, "invalid_request_error", "Parameters must be a JSON object")
            raise AssertionError("unreachable")
        return dict(parsed)
    _openai_http_error(400, "invalid_request_error", "Invalid parameters")
    raise AssertionError("unreachable")


def _extract_chat_parameters(data: ChatData) -> Optional[dict[str, Any]]:
    # Priority: explicit top-level `parameters` > `extra_body.parameters`
    if data.parameters is not None:
        return _parse_poe_parameters(data.parameters)

    extra_fields = getattr(data, "__pydantic_extra__", None) or {}
    top_level_parameters = extra_fields.get("parameters")
    if top_level_parameters is not None:
        return _parse_poe_parameters(top_level_parameters)

    extra_body = data.extra_body
    if isinstance(extra_body, dict):
        body_parameters = extra_body.get("parameters")
        if body_parameters is not None:
            return _parse_poe_parameters(body_parameters)
    return None


def _normalize_aspect_value(value: str) -> Optional[str]:
    text = (value or "").strip().lower()
    if not text:
        return None
    if text.startswith("--aspect "):
        text = text[len("--aspect "):].strip()
    elif text.startswith("--ar "):
        text = text[len("--ar "):].strip()

    if ":" in text:
        parts = text.split(":", 1)
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            left = int(parts[0])
            right = int(parts[1])
            if left > 0 and right > 0:
                return f"{left}:{right}"

    if "x" in text:
        parts = text.split("x", 1)
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            width = int(parts[0])
            height = int(parts[1])
            if width > 0 and height > 0:
                gcd_value = math.gcd(width, height)
                return f"{width // gcd_value}:{height // gcd_value}"

    return None


def _aspect_parameter_key(model: str, model_meta: dict[str, Any], params: dict[str, Any]) -> str:
    if "aspect_ratio" in params:
        return "aspect_ratio"
    if "aspect" in params:
        return "aspect"
    configured = model_meta.get("aspect_parameter")
    if isinstance(configured, str) and configured.strip() in {"aspect", "aspect_ratio"}:
        return configured.strip()
    base_model = str(model_meta.get("baseModel", "")).strip().lower()
    normalized_model = str(model or "").strip().lower()
    if "nano-banana" in normalized_model or "nano-banana" in base_model:
        return "aspect_ratio"
    return "aspect"


def _resolve_image_generation_config(
    model: str,
    size: Optional[str],
    aspect_ratio: Optional[str] = None,
    image_size: Optional[str] = None,
    parameters: Any = None,
) -> tuple[str, Optional[dict[str, Any]]]:
    model_meta = app.state.models.get(model, {})
    model_sizes = model_meta.get("sizes", {})
    image_sizes = model_meta.get("image_sizes", {})
    resolved_parameters = _parse_poe_parameters(parameters) or {}

    resolved_image_size_key = ""
    normalized_image_size = (image_size or "").strip().lower()
    if normalized_image_size:
        resolved_image_size_key = normalized_image_size
    else:
        normalized_size = (size or "").strip().lower()
        if isinstance(image_sizes, dict):
            normalized_image_sizes = {str(k).strip().lower(): v for k, v in image_sizes.items()}
            if normalized_size in normalized_image_sizes:
                resolved_image_size_key = normalized_size

    if isinstance(image_sizes, dict) and resolved_image_size_key:
        normalized_image_sizes = {str(k).strip().lower(): v for k, v in image_sizes.items()}
        normalized_image_values = {str(v).strip().lower(): str(v).strip() for v in image_sizes.values()}
        image_size_value = normalized_image_sizes.get(resolved_image_size_key)
        if image_size_value is None:
            image_size_value = normalized_image_values.get(resolved_image_size_key)
        if image_size_value is not None:
            image_size_text = str(image_size_value).strip()
            if image_size_text:
                resolved_parameters["image_size"] = image_size_text
        else:
            supported_image_sizes = sorted(set(str(k) for k in image_sizes.keys()) | set(str(v) for v in image_sizes.values()))
            _openai_http_error(
                400,
                "invalid_request_error",
                f"Invalid image_size for model {model}. Supported values: {supported_image_sizes}",
            )
            raise AssertionError("unreachable")

    candidate_aspect = (aspect_ratio or "").strip()
    if not candidate_aspect:
        normalized_size = (size or "").strip().lower()
        if normalized_size not in ("", "auto", "default", "1024x1024") and normalized_size != resolved_image_size_key:
            candidate_aspect = (size or "").strip()

    normalized_aspect = candidate_aspect.lower()
    if normalized_aspect in ("", "auto", "default", "1024x1024"):
        return "", resolved_parameters or None

    resolved_ratio: Optional[str] = None
    if isinstance(model_sizes, dict):
        direct = model_sizes.get(candidate_aspect)
        if isinstance(direct, str):
            resolved_ratio = _normalize_aspect_value(direct)
        if resolved_ratio is None:
            normalized_size_map = {str(k).strip().lower(): v for k, v in model_sizes.items()}
            normalized_hit = normalized_size_map.get(normalized_aspect)
            if isinstance(normalized_hit, str):
                resolved_ratio = _normalize_aspect_value(normalized_hit)

    if resolved_ratio is None:
        resolved_ratio = _normalize_aspect_value(candidate_aspect)

    if resolved_ratio is not None:
        key = _aspect_parameter_key(model, model_meta, resolved_parameters)
        resolved_parameters[key] = resolved_ratio
        return "", resolved_parameters or None

    supported_sizes = ["auto", "default", "1024x1024"]
    if isinstance(model_sizes, dict):
        supported_sizes.extend(str(k) for k in model_sizes.keys())
    if isinstance(image_sizes, dict):
        supported_sizes.extend(str(k) for k in image_sizes.keys())
    _openai_http_error(
        400,
        "invalid_request_error",
        f"Invalid aspect ratio or size for model {model}. Supported values: {sorted(set(supported_sizes))}",
    )
    raise AssertionError("unreachable")


def _resolve_image_aspect(model: str, size: Optional[str]) -> str:
    aspect_ratio, _ = _resolve_image_generation_config(model, size)
    return aspect_ratio


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


def _extract_attachment_urls(message_chunk: dict[str, Any], mime_prefix: Optional[str] = None) -> list[str]:
    attachments = message_chunk.get("attachments") or []
    urls: list[str] = []
    seen: set[str] = set()
    expected_prefix = (mime_prefix or "").strip().lower()

    for att in attachments:
        if not isinstance(att, dict):
            continue
        file_info = att.get("file") or {}
        url = str(file_info.get("url") or att.get("url") or "").strip()
        if not url:
            continue

        mime = str(file_info.get("mimeType") or "").strip().lower()
        if expected_prefix and mime and not mime.startswith(expected_prefix):
            continue

        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def _is_audio_generation_model(model: str) -> bool:
    model_meta = app.state.models.get(model, {})
    base_model = str(model_meta.get("baseModel") or "").strip().lower()
    model_name = str(model or "").strip().lower()
    text = f"{model_name} {base_model}"
    return ("music" in text) or ("audio" in text)


async def generate_image(
    client,
    response: dict,
    aspect_ratio: str,
    image: list = None,
    parameters: Optional[dict[str, Any]] = None,
) -> str:
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
            parameters=parameters,
        ):
            pass
        # 优先从 attachments 中提取图片 URL（如 Qwen-Image 等把图片作为附件返回的模型）
        attachment_urls = _extract_attachment_urls(chunk, mime_prefix="image/")
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
    chat_parameters: Optional[dict[str, Any]],
    shared_state: dict[str, Any],  # 用于与 BackgroundTask 共享执行结果
) -> AsyncGenerator[bytes, None]:
    completion_timestamp = await helpers.__generate_timestamp()
    emitted_done = False
    has_content = False
    completion_tokens = 0
    finish_reason = "stop"
    is_cumulative = response["bot"].lower() in CUMULATIVE_RESPONSE_BOTS
    prev_cumulative_text = ""
    try:
        if not raw_tool_calls:
            async for chunk in client.send_message(
                bot=response["bot"],
                message=response["message"],
                file_path=attachment_paths,
                chatCode=chat_code,
                chatId=chat_id,
                parameters=chat_parameters,
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

                if is_cumulative:
                    full_text = chunk["text"] or ""
                    delta = full_text[len(prev_cumulative_text):]
                    prev_cumulative_text = full_text
                else:
                    delta = chunk["response"]

                if not delta:
                    continue

                content = await create_completion_data(
                    completion_id=completion_id,
                    created=completion_timestamp,
                    model=model,
                    chunk=delta,
                    include_usage=include_usage,
                )
                yield b"data: " + orjson.dumps(content) + b"\n\n"
                has_content = True

                if str(chunk.get("state", "")).lower() == "complete":
                    attachment_urls = _extract_attachment_urls(chunk)
                    extra_urls = [url for url in attachment_urls if url and url not in str(chunk.get("text") or "")]
                    if extra_urls:
                        attachment_chunk = await create_completion_data(
                            completion_id=completion_id,
                            created=completion_timestamp,
                            model=model,
                            chunk="\n".join(extra_urls),
                            include_usage=False,
                        )
                        yield b"data: " + orjson.dumps(attachment_chunk) + b"\n\n"
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
        shared_state["succeeded"] = True  # 标记成功，供 BackgroundTask 使用
    except asyncio.CancelledError:
        # Client closed the streaming connection; avoid noisy ASGI stack traces.
        pass
    except Exception as exc:
        error_decision = await _account_error_payload(
            account_id=account_id,
            session_id=session_id,
            persistent_session=persistent_session,
            exc=exc,
            model=model,
        )

        # --- 流式重试：尚未向客户端发送任何内容时，换账号重试一次 ---
        if not has_content:
            # 先清理失败的账号（原有逻辑不变）
            await _finalize_account_use(
                runtime,
                account_id=account_id,
                lease=lease,
                session_id=session_id,
                persistent_session=persistent_session,
                success=False,
                error_decision=error_decision,
                evict_client=True,
                release_lease=True,
            )
            # 尝试获取新账号重试
            try:
                new_doc, new_lease = await _acquire_prewarmed_account(runtime)
                new_account_id = str(new_doc["_id"])
                new_client = await runtime.pool.get_client_for_account(new_doc, create_if_missing=False)
            except Exception:
                # 拿不到新账号，回退到原有错误输出
                logger.warning("stream_retry failed: no account available for retry, model={}", model)
            else:
                retry_bot = _downgrade_bot_for_retry(response["bot"])
                logger.warning(
                    "stream_retry account_id={} -> {} bot={} -> {} model={}",
                    mask_secret(account_id), mask_secret(new_account_id),
                    response["bot"], retry_bot, model,
                )
                # 更新 shared_state，让 BackgroundTask 清理新账号
                shared_state["account_id"] = new_account_id
                shared_state["lease"] = new_lease
                account_id = new_account_id
                client = new_client
                lease = new_lease

                # 重试 send_message（使用降级后的模型）
                retry_ok = False
                is_cumulative = retry_bot.lower() in CUMULATIVE_RESPONSE_BOTS
                try:
                    prev_cumulative_text = ""
                    async for chunk in new_client.send_message(
                        bot=retry_bot,
                        message=response["message"],
                        file_path=attachment_paths,
                        chatCode=None,
                        chatId=None,
                        parameters=chat_parameters,
                    ):
                        completion_tokens = await helpers.__tokenize(chunk["text"])
                        if max_tokens and completion_tokens >= max_tokens:
                            await new_client.cancel_message(chunk)
                            finish_reason = "length"
                            break

                        if is_cumulative:
                            full_text = chunk["text"] or ""
                            delta = full_text[len(prev_cumulative_text):]
                            prev_cumulative_text = full_text
                        else:
                            delta = chunk["response"]

                        if not delta:
                            continue

                        retry_content = await create_completion_data(
                            completion_id=completion_id,
                            created=completion_timestamp,
                            model=model,
                            chunk=delta,
                            include_usage=include_usage,
                        )
                        yield b"data: " + orjson.dumps(retry_content) + b"\n\n"
                        has_content = True
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
                    yield b"data: [DONE]\n\n"
                    emitted_done = True
                    shared_state["succeeded"] = True
                    retry_ok = True
                except Exception as retry_exc:
                    retry_decision = await _account_error_payload(
                        account_id=new_account_id,
                        session_id=session_id,
                        persistent_session=persistent_session,
                        exc=retry_exc,
                        model=model,
                    )
                    shared_state["error_decision"] = retry_decision
                    yield b"data: " + orjson.dumps(retry_decision.payload) + b"\n\n"
                    if not emitted_done:
                        yield b"data: [DONE]\n\n"
                    return
                if retry_ok:
                    return

        # 无法重试或已有内容输出，按原逻辑返回错误
        shared_state["error_decision"] = error_decision
        yield b"data: " + orjson.dumps(error_decision.payload) + b"\n\n"
        if not emitted_done:
            yield b"data: [DONE]\n\n"
    # 注意：清理逻辑（_finalize_account_use + _cleanup_temp_files）已移到 BackgroundTask 中


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
    chat_parameters: Optional[dict[str, Any]],
) -> StreamingResponse:

    # 用于在生成器和后台任务间共享执行结果（含重试后的账号信息）
    shared_state: dict[str, Any] = {
        "succeeded": False,
        "error_decision": None,
        "account_id": account_id,
        "lease": lease,
    }

    # 定义后台清理任务，在流式响应彻底结束后必定执行
    async def cleanup_task():
        _cleanup_temp_files(temp_files)
        # 销毁 client 并释放 lease（可能已被重试更新为新账号）
        await _finalize_account_use(
            runtime,
            account_id=shared_state["account_id"],
            lease=shared_state["lease"],
            session_id=session_id,
            persistent_session=persistent_session,
            success=shared_state["succeeded"],
            error_decision=shared_state["error_decision"],
            evict_client=True,  # 强制销毁
        )

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
            chat_parameters=chat_parameters,
            shared_state=shared_state,  # 传入共享状态
        ),
        status_code=200,
        headers={"Content-Type": "text/event-stream", "X-Request-ID": str(uuid.uuid4())},
        background=BackgroundTask(cleanup_task)  # 绑定后台清理任务
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
    chat_parameters: Optional[dict[str, Any]],
) -> JSONResponse:
    succeeded = False
    error_decision: Optional[AccountErrorDecision] = None
    try:
        if not raw_tool_calls:
            finish_reason = "stop"
            async for chunk in client.send_message(
                bot=response["bot"],
                message=response["message"],
                file_path=attachment_paths,
                chatCode=chat_code,
                chatId=chat_id,
                parameters=chat_parameters,
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
            attachment_urls = _extract_attachment_urls(chunk)
            if _is_audio_generation_model(model):
                audio_urls = _extract_attachment_urls(chunk, mime_prefix="audio/")
                if not audio_urls:
                    _openai_http_error(
                        500,
                        "provider_error",
                        f"Audio model {model} returned no audio attachment URL",
                    )
                message_content = "\n".join(audio_urls)
            elif attachment_urls:
                attachment_block = "\n".join(attachment_urls)
                if message_content:
                    if any(url not in message_content for url in attachment_urls):
                        message_content = f"{message_content}\n{attachment_block}"
                else:
                    message_content = attachment_block
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
        succeeded = True
        return JSONResponse(content.model_dump())
    except Exception as exc:
        error_decision = await _account_error_payload(
            account_id=account_id,
            session_id=session_id,
            persistent_session=persistent_session,
            exc=exc,
        )
        raise HTTPException(status_code=error_decision.status_code, detail=error_decision.payload)
    finally:
        _cleanup_temp_files(temp_files)
        await _finalize_account_use(
            runtime,
            account_id=account_id,
            lease=lease,
            session_id=session_id,
            persistent_session=persistent_session,
            success=succeeded,
            error_decision=error_decision,
            evict_client=True,
        )


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
    messages = _merge_system_messages_to_index_zero(data.messages)
    model = data.model
    streaming = bool(data.stream)
    max_tokens = data.max_completion_tokens or data.max_tokens
    stream_options = data.stream_options
    tools = data.tools
    tool_choice = data.tool_choice
    metadata = data.metadata
    user = data.user
    chat_parameters = _extract_chat_parameters(data)
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
    if streaming and _is_audio_generation_model(model):
        _openai_http_error(400, "invalid_request_error", "Streaming is not supported for audio/music models")
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

    # --- 消息预处理（与账号无关，只做一次）---
    text_messages, image_urls = await helpers.__split_content(messages)
    msg_response = await message_handler(base_model, text_messages, tokens_limit)
    prompt_tokens = await helpers.__tokenize("".join([str(message) for message in msg_response["message"]]))

    if prompt_tokens > tokens_limit:
        _openai_http_error(
            413,
            "request_too_large",
            f"Your prompt exceeds the maximum context length of {tokens_limit} tokens",
        )
    if max_tokens and (max_tokens + prompt_tokens) > tokens_limit:
        _openai_http_error(
            413,
            "request_too_large",
            (
                f"This model's maximum context length is {tokens_limit}. "
                f"Request exceeds limit ({max_tokens} in max_tokens, {prompt_tokens} in prompt)"
            ),
        )

    # --- 重试循环（账号相关）---
    last_exc: Optional[HTTPException] = None
    for attempt in range(_MAX_CHAT_ATTEMPTS):
        is_last = attempt >= _MAX_CHAT_ATTEMPTS - 1

        account_doc, lease, chat_code, chat_id = await _acquire_account_for_chat(
            runtime,
            session_id=session_id,
            persistent_session=persistent_session,
            model=model,
        )
        account_id = str(account_doc["_id"])
        request.state.account_id = account_id

        # Premium model 检查
        if premium_model and not bool(account_doc.get("subscription_active", False)):
            await _finalize_account_use(
                runtime,
                account_id=account_id,
                lease=lease,
                session_id=session_id,
                persistent_session=persistent_session,
                success=False,
                evict_client=True,
            )
            if not is_last:
                logger.warning(
                    "chat_retry attempt={}/{} reason=no_subscription account_id={} model={}",
                    attempt + 1, _MAX_CHAT_ATTEMPTS, mask_secret(account_id), model,
                )
                continue
            _openai_http_error(
                402,
                "insufficient_credits",
                "Premium model requires an active subscription on selected account",
            )

        try:
            client = await runtime.pool.get_client_for_account(account_doc, create_if_missing=False)
        except Exception as exc:
            error_decision = await _account_error_payload(
                account_id=account_id,
                session_id=session_id,
                persistent_session=persistent_session,
                exc=exc,
                model=model,
            )
            await _finalize_account_use(
                runtime,
                account_id=account_id,
                lease=lease,
                session_id=session_id,
                persistent_session=persistent_session,
                success=False,
                error_decision=error_decision,
                evict_client=True,
            )
            if not is_last:
                logger.warning(
                    "chat_retry attempt={}/{} status={} account_id={} model={}",
                    attempt + 1, _MAX_CHAT_ATTEMPTS, error_decision.status_code, mask_secret(account_id), model,
                )
                continue
            raise HTTPException(status_code=error_decision.status_code, detail=error_decision.payload)

        # 重试时降级模型
        if attempt > 0:
            retry_bot = _downgrade_bot_for_retry(msg_response["bot"])
            response = {**msg_response, "bot": retry_bot}
            logger.warning(
                "chat_retry downgrade bot={} -> {} model={}",
                msg_response["bot"], retry_bot, model,
            )
        else:
            response = msg_response

        # Tool calls（依赖 client）
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
                await _finalize_account_use(
                    runtime,
                    account_id=account_id,
                    lease=lease,
                    session_id=session_id,
                    persistent_session=persistent_session,
                    success=False,
                    evict_client=True,
                )
                raise
            except Exception as exc:
                await _finalize_account_use(
                    runtime,
                    account_id=account_id,
                    lease=lease,
                    session_id=session_id,
                    persistent_session=persistent_session,
                    success=False,
                    evict_client=True,
                )
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
                chat_parameters=chat_parameters,
            )

        try:
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
                chat_parameters=chat_parameters,
            )
        except HTTPException as exc:
            # non_streaming_response 已在 finally 中完成了账号清理
            last_exc = exc
            if not is_last and exc.status_code in _RETRYABLE_STATUS_CODES:
                logger.warning(
                    "chat_retry attempt={}/{} status={} account_id={} model={}",
                    attempt + 1, _MAX_CHAT_ATTEMPTS, exc.status_code, mask_secret(account_id), model,
                )
                continue
            raise

    raise last_exc  # type: ignore[misc]


@app.api_route("/images/generations", methods=["POST", "OPTIONS"], response_model=None)
@app.api_route("/v1/images/generations", methods=["POST", "OPTIONS"], response_model=None)
async def create_images(
    request: Request,
    data: ImagesGenData,
    _auth: None = Depends(require_service_auth),
) -> JSONResponse:
    runtime = _runtime()
    prompt, model, n, size = data.prompt, data.model, data.n, data.size
    aspect_ratio = data.aspect_ratio
    image_size = data.image_size
    raw_parameters = data.parameters
    request.state.model = model

    if not isinstance(prompt, str):
        _openai_http_error(400, "invalid_request_error", "Invalid prompt")
    if model not in app.state.models:
        _openai_http_error(404, "not_found_error", "Invalid model")
    if not isinstance(n, int) or n < 1:
        _openai_http_error(400, "invalid_request_error", "Invalid n value")

    aspect_ratio, image_parameters = _resolve_image_generation_config(
        model,
        size,
        aspect_ratio=aspect_ratio,
        image_size=image_size,
        parameters=raw_parameters,
    )

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
        await _finalize_account_use(
            runtime,
            account_id=account_id,
            lease=lease,
            session_id="ephemeral",
            persistent_session=False,
            success=False,
            evict_client=True,
        )
        _openai_http_error(402, "insufficient_credits", "Premium model requires active subscription")

    succeeded = False
    error_decision: Optional[AccountErrorDecision] = None
    try:
        client = await runtime.pool.get_client_for_account(account_doc, create_if_missing=False)
        response = await image_handler(base_model, prompt, tokens_limit)

        urls: list[str] = []
        for _ in range(n):
            image_generation = await generate_image(client, response, aspect_ratio, parameters=image_parameters)
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

        succeeded = True
        return JSONResponse(
            {"created": await helpers.__generate_timestamp(), "data": [{"url": url} for url in urls]}
        )
    except HTTPException:
        raise
    except Exception as exc:
        error_decision = await _account_error_payload(
            account_id=account_id,
            session_id="ephemeral",
            persistent_session=False,
            exc=exc,
        )
        raise HTTPException(status_code=error_decision.status_code, detail=error_decision.payload)
    finally:
        await _finalize_account_use(
            runtime,
            account_id=account_id,
            lease=lease,
            session_id="ephemeral",
            persistent_session=False,
            success=succeeded,
            error_decision=error_decision,
            evict_client=True,
        )


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
        raw_aspect_ratio = form.get("aspect_ratio")
        raw_image_size = form.get("image_size")
        raw_parameters = form.get("parameters")
        try:
            n = int(raw_n)
        except Exception:
            _openai_http_error(400, "invalid_request_error", "Invalid n value")
        size = raw_size if isinstance(raw_size, str) else "1024x1024"
        aspect_ratio = raw_aspect_ratio if isinstance(raw_aspect_ratio, str) else None
        image_size = raw_image_size if isinstance(raw_image_size, str) else None
    else:
        payload = await request.json()
        data = ImagesEditData(**payload)
        image, prompt, model, n, size = data.image, data.prompt, data.model, data.n, data.size
        aspect_ratio = data.aspect_ratio
        image_size = data.image_size
        raw_parameters = data.parameters

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

    aspect_ratio, image_parameters = _resolve_image_generation_config(
        model,
        size,
        aspect_ratio=aspect_ratio,
        image_size=image_size,
        parameters=raw_parameters,
    )

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
        await _finalize_account_use(
            runtime,
            account_id=account_id,
            lease=lease,
            session_id="ephemeral",
            persistent_session=False,
            success=False,
            evict_client=True,
        )
        _openai_http_error(402, "insufficient_credits", "Premium model requires active subscription")

    edit_attachment = image
    edit_temp_files: List[str] = []
    succeeded = False
    error_decision: Optional[AccountErrorDecision] = None
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
            image_generation = await generate_image(
                client,
                response,
                aspect_ratio,
                [edit_attachment],
                parameters=image_parameters,
            )
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

        succeeded = True
        return JSONResponse(
            {"created": await helpers.__generate_timestamp(), "data": [{"url": url} for url in urls]}
        )
    except HTTPException:
        raise
    except Exception as exc:
        error_decision = await _account_error_payload(
            account_id=account_id,
            session_id="ephemeral",
            persistent_session=False,
            exc=exc,
        )
        raise HTTPException(status_code=error_decision.status_code, detail=error_decision.payload)
    finally:
        _cleanup_temp_files(edit_temp_files)
        await _finalize_account_use(
            runtime,
            account_id=account_id,
            lease=lease,
            session_id="ephemeral",
            persistent_session=False,
            success=succeeded,
            error_decision=error_decision,
            evict_client=True,
        )


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
