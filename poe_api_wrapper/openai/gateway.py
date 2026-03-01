from __future__ import annotations

import asyncio
import hashlib
import os
import statistics
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import orjson
from cryptography.fernet import Fernet
from loguru import logger
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError

from ..async_api import AsyncPoeApi


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def hash_api_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def mask_secret(raw: str, keep: int = 4) -> str:
    if not raw:
        return ""
    if len(raw) <= keep:
        return "*" * len(raw)
    return f"{raw[:keep]}{'*' * (len(raw) - keep)}"


def build_openai_error(
    code: int,
    error_type: str,
    message: str,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return {
        "error": {
            "code": code,
            "type": error_type,
            "message": message,
            "metadata": metadata or {},
        }
    }


def extract_bearer_token(authorization_header: str) -> Optional[str]:
    if not authorization_header:
        return None
    parts = authorization_header.split(" ", 1)
    if len(parts) != 2:
        return None
    if parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    return token or None


class NoAccountAvailableError(RuntimeError):
    pass


class CapacityLimitError(RuntimeError):
    pass


class CredentialCrypto:
    def __init__(self, fernet_key: str):
        if not fernet_key:
            raise RuntimeError("FERNET_KEY is required")
        self._fernet = Fernet(fernet_key.encode("utf-8"))

    def encrypt(self, payload: dict[str, Any]) -> str:
        blob = orjson.dumps(payload)
        return self._fernet.encrypt(blob).decode("utf-8")

    def decrypt(self, ciphertext: str) -> dict[str, Any]:
        raw = self._fernet.decrypt(ciphertext.encode("utf-8"))
        return orjson.loads(raw)


class AccountRepository:
    def __init__(self, mongo_uri: str, mongo_db: str, crypto: CredentialCrypto):
        self.client = MongoClient(mongo_uri, tz_aware=True)
        self.db = self.client[mongo_db]
        self.accounts = self.db["accounts"]
        self.sessions = self.db["sessions"]
        self.service_api_keys = self.db["service_api_keys"]
        self.crypto = crypto

    async def _run(self, fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def init_indexes(self) -> None:
        await self._run(self.accounts.create_index, [("email", ASCENDING)], unique=True)
        await self._run(
            self.accounts.create_index,
            [("status", ASCENDING), ("cooldown_until", ASCENDING), ("message_point_balance", DESCENDING)],
        )
        await self._run(self.sessions.create_index, [("session_id", ASCENDING)], unique=True)
        await self._run(self.sessions.create_index, [("last_used_at", DESCENDING)])
        await self._run(self.service_api_keys.create_index, [("key_hash", ASCENDING)], unique=True)

    def _sanitize_account(self, doc: dict[str, Any]) -> dict[str, Any]:
        def _dt(value):
            if isinstance(value, datetime):
                return value.isoformat()
            return value

        return {
            "id": str(doc["_id"]),
            "email": doc.get("email"),
            "status": doc.get("status", "active"),
            "message_point_balance": int(doc.get("message_point_balance", 0) or 0),
            "subscription_active": bool(doc.get("subscription_active", False)),
            "health_score": float(doc.get("health_score", 0.0) or 0.0),
            "last_refresh_at": _dt(doc.get("last_refresh_at")),
            "last_success_at": _dt(doc.get("last_success_at")),
            "last_error": doc.get("last_error"),
            "error_count": int(doc.get("error_count", 0) or 0),
            "cooldown_until": _dt(doc.get("cooldown_until")),
            "updated_at": _dt(doc.get("updated_at")),
            "created_at": _dt(doc.get("created_at")),
        }

    async def upsert_account(
        self,
        *,
        email: str,
        poe_p_b: str,
        poe_cf_clearance: str,
        poe_cf_bm: Optional[str] = None,
        p_lat: Optional[str] = None,
        formkey: Optional[str] = None,
        poe_revision: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> dict[str, Any]:
        now = utc_now()
        credentials = {
            "poe_p_b": poe_p_b,
            "poe_cf_clearance": poe_cf_clearance,
            "poe_cf_bm": poe_cf_bm,
            "p_lat": p_lat,
            "formkey": formkey,
            "poe_revision": poe_revision,
            "user_agent": user_agent or DEFAULT_USER_AGENT,
        }
        encrypted_credentials = self.crypto.encrypt(credentials)

        def _op():
            return self.accounts.find_one_and_update(
                {"email": email},
                {
                    "$set": {
                        "credentials": encrypted_credentials,
                        "status": "active",
                        "updated_at": now,
                    },
                    "$setOnInsert": {
                        "message_point_balance": 0,
                        "subscription_active": False,
                        "last_refresh_at": None,
                        "last_success_at": None,
                        "last_error": None,
                        "error_count": 0,
                        "cooldown_until": None,
                        "health_score": 50.0,
                        "created_at": now,
                    },
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )

        updated = await self._run(_op)
        return self._sanitize_account(updated)

    async def get_account_by_id(self, account_id: str) -> Optional[dict[str, Any]]:
        def _op():
            return self.accounts.find_one({"_id": self._object_id(account_id)})

        return await self._run(_op)

    async def get_account_by_email(self, email: str) -> Optional[dict[str, Any]]:
        def _op():
            return self.accounts.find_one({"email": email})

        return await self._run(_op)

    async def get_accounts_summary(self) -> dict[str, Any]:
        """Return all accounts with key fields only, plus total / available counts."""
        now = utc_now()

        def _op():
            docs = list(
                self.accounts.find(
                    {},
                    {
                        "email": 1,
                        "status": 1,
                        "message_point_balance": 1,
                        "subscription_active": 1,
                        "cooldown_until": 1,
                        "last_success_at": 1,
                        "updated_at": 1,
                    },
                ).sort("message_point_balance", DESCENDING)
            )
            return docs, now

        docs, query_time = await self._run(_op)

        accounts = []
        available_count = 0
        for doc in docs:
            cooldown_until = doc.get("cooldown_until")
            in_cooldown = bool(
                cooldown_until
                and isinstance(cooldown_until, datetime)
                and cooldown_until > query_time
            )
            status = str(doc.get("status", "unknown"))
            is_available = status == "active" and not in_cooldown
            if is_available:
                available_count += 1
            accounts.append(
                {
                    "email": doc.get("email"),
                    "status": status,
                    "available": is_available,
                    "message_point_balance": int(doc.get("message_point_balance", 0) or 0),
                    "subscription_active": bool(doc.get("subscription_active", False)),
                    "cooldown_until": doc["cooldown_until"].isoformat() if in_cooldown else None,
                    "last_success_at": (
                        doc["last_success_at"].isoformat()
                        if doc.get("last_success_at")
                        else None
                    ),
                    "updated_at": (
                        doc["updated_at"].isoformat() if doc.get("updated_at") else None
                    ),
                }
            )

        return {
            "total": len(accounts),
            "available": available_count,
            "unavailable": len(accounts) - available_count,
            "accounts": accounts,
        }

    def _object_id(self, account_id: str):
        from bson import ObjectId

        return ObjectId(account_id)

    async def get_account_credentials(self, account_doc: dict[str, Any]) -> dict[str, Any]:
        return self.crypto.decrypt(account_doc["credentials"])

    async def update_account_formkey(self, account_id: str, formkey: str) -> None:
        account = await self.get_account_by_id(account_id)
        if not account:
            return
        credentials = await self.get_account_credentials(account)
        credentials["formkey"] = formkey
        encrypted = self.crypto.encrypt(credentials)
        await self._run(
            self.accounts.update_one,
            {"_id": account["_id"]},
            {"$set": {"credentials": encrypted, "updated_at": utc_now()}},
        )

    async def list_accounts(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status: Optional[str] = None,
        min_balance: Optional[int] = None,
        max_balance: Optional[int] = None,
    ) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if status:
            query["status"] = status
        balance_query: dict[str, Any] = {}
        if min_balance is not None:
            balance_query["$gte"] = min_balance
        if max_balance is not None:
            balance_query["$lte"] = max_balance
        if balance_query:
            query["message_point_balance"] = balance_query

        skip = max(page - 1, 0) * page_size

        def _count():
            return self.accounts.count_documents(query)

        def _find():
            return list(
                self.accounts.find(query)
                .sort("updated_at", DESCENDING)
                .skip(skip)
                .limit(page_size)
            )

        total, docs = await asyncio.gather(self._run(_count), self._run(_find))
        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "data": [self._sanitize_account(doc) for doc in docs],
        }

    async def list_candidate_accounts(self, limit: int = 1000) -> list[dict[str, Any]]:
        now = utc_now()
        query = {
            "status": "active",
            "$or": [
                {"cooldown_until": None},
                {"cooldown_until": {"$lte": now}},
                {"cooldown_until": {"$exists": False}},
            ],
        }

        def _op():
            return list(
                self.accounts.find(query)
                .sort(
                    [
                        ("health_score", DESCENDING),
                        ("message_point_balance", DESCENDING),
                        ("last_success_at", DESCENDING),
                    ]
                )
                .limit(limit)
            )

        return await self._run(_op)

    async def mark_account_success(self, account_id: str) -> None:
        now = utc_now()

        def _op():
            doc = self.accounts.find_one({"_id": self._object_id(account_id)})
            if not doc:
                return
            error_count = int(doc.get("error_count", 0) or 0)
            update_doc = {
                "last_success_at": now,
                "last_error": None,
                "error_count": max(error_count - 1, 0),
                "updated_at": now,
            }
            if doc.get("status") != "depleted":
                update_doc["status"] = "active"
                update_doc["cooldown_until"] = None
            self.accounts.update_one({"_id": doc["_id"]}, {"$set": update_doc})

        await self._run(_op)

    async def update_account_health(
        self,
        account_id: str,
        *,
        balance: int,
        subscription_active: bool,
        depleted_threshold: int,
    ) -> None:
        now = utc_now()
        status = "depleted" if balance <= depleted_threshold else "active"
        health_score = min(100.0, float(balance) / 10.0 + (20.0 if subscription_active else 0.0))

        await self._run(
            self.accounts.update_one,
            {"_id": self._object_id(account_id)},
            {
                "$set": {
                    "message_point_balance": int(balance),
                    "subscription_active": bool(subscription_active),
                    "status": status,
                    "health_score": health_score,
                    "last_refresh_at": now,
                    "updated_at": now,
                    "cooldown_until": None if status == "active" else utc_now() + timedelta(hours=6),
                }
            },
        )

    async def mark_account_depleted(self, account_id: str, error_message: Optional[str] = None) -> None:
        now = utc_now()
        await self._run(
            self.accounts.update_one,
            {"_id": self._object_id(account_id)},
            {
                "$set": {
                    "status": "depleted",
                    "cooldown_until": now + timedelta(hours=6),
                    "last_error": error_message,
                    "updated_at": now,
                }
            },
        )

    async def mark_account_invalid(self, account_id: str, error_message: str) -> None:
        now = utc_now()
        await self._run(
            self.accounts.update_one,
            {"_id": self._object_id(account_id)},
            {
                "$set": {
                    "status": "invalid",
                    "last_error": error_message,
                    "updated_at": now,
                }
            },
        )

    async def record_account_error(
        self,
        account_id: str,
        error_message: str,
        *,
        cooldown_seconds: int = 120,
        error_threshold: int = 3,
    ) -> None:
        now = utc_now()

        def _op():
            doc = self.accounts.find_one({"_id": self._object_id(account_id)})
            if not doc:
                return
            error_count = int(doc.get("error_count", 0) or 0) + 1
            status = doc.get("status", "active")
            cooldown_until = doc.get("cooldown_until")
            if error_count >= error_threshold and status != "depleted":
                status = "cooldown"
                cooldown_until = now + timedelta(seconds=cooldown_seconds)
            self.accounts.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "error_count": error_count,
                        "last_error": error_message,
                        "status": status,
                        "cooldown_until": cooldown_until,
                        "updated_at": now,
                    }
                },
            )

        await self._run(_op)

    async def bootstrap_service_keys(self, raw_keys: list[str]) -> None:
        now = utc_now()
        for idx, raw_key in enumerate(raw_keys):
            if not raw_key:
                continue
            key_hash = hash_api_key(raw_key)

            def _op():
                if self.service_api_keys.find_one({"key_hash": key_hash}):
                    return
                self.service_api_keys.insert_one(
                    {
                        "key_hash": key_hash,
                        "name": f"bootstrap-{idx + 1}",
                        "enabled": True,
                        "rate_limit": None,
                        "created_at": now,
                    }
                )

            try:
                await self._run(_op)
            except DuplicateKeyError:
                continue

    async def is_service_key_enabled(self, raw_key: str) -> bool:
        key_hash = hash_api_key(raw_key)

        def _op():
            return self.service_api_keys.find_one({"key_hash": key_hash, "enabled": True})

        return bool(await self._run(_op))

    async def get_session(self, session_id: str) -> Optional[dict[str, Any]]:
        def _op():
            return self.sessions.find_one({"session_id": session_id})

        return await self._run(_op)

    async def upsert_session(
        self,
        *,
        session_id: str,
        model: str,
        account_id: str,
        chat_code: Optional[str] = None,
        chat_id: Optional[int] = None,
        state: str = "active",
    ) -> dict[str, Any]:
        now = utc_now()

        def _op():
            return self.sessions.find_one_and_update(
                {"session_id": session_id},
                {
                    "$set": {
                        "model": model,
                        "account_id": account_id,
                        "chat_code": chat_code,
                        "chat_id": chat_id,
                        "state": state,
                        "last_used_at": now,
                    },
                    "$setOnInsert": {
                        "created_at": now,
                    },
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )

        return await self._run(_op)

    async def mark_session_broken(self, session_id: str, reason: str) -> None:
        await self._run(
            self.sessions.update_one,
            {"session_id": session_id},
            {
                "$set": {
                    "state": "broken",
                    "last_error": reason,
                    "last_used_at": utc_now(),
                }
            },
        )

    async def touch_session(self, session_id: str) -> None:
        await self._run(
            self.sessions.update_one,
            {"session_id": session_id},
            {"$set": {"last_used_at": utc_now()}},
        )

    async def count_ready_accounts(self) -> int:
        now = utc_now()
        query = {
            "status": "active",
            "$or": [
                {"cooldown_until": None},
                {"cooldown_until": {"$lte": now}},
                {"cooldown_until": {"$exists": False}},
            ],
        }
        return await self._run(self.accounts.count_documents, query)

    async def list_recent_active_accounts(self, *, minutes: int = 120, limit: int = 200) -> list[dict[str, Any]]:
        since = utc_now() - timedelta(minutes=minutes)
        query = {"status": "active", "last_success_at": {"$gte": since}}

        def _op():
            return list(self.accounts.find(query).sort("last_success_at", DESCENDING).limit(limit))

        return await self._run(_op)

    async def daily_reset_point_balance(
        self,
        *,
        point_balance: int = 3000,
        reset_statuses: Optional[list[str]] = None,
    ) -> int:
        now = utc_now()
        target_statuses = reset_statuses or ["active", "depleted", "cooldown"]
        health_score = min(100.0, float(point_balance) / 10.0)

        def _op() -> int:
            result = self.accounts.update_many(
                {"status": {"$in": target_statuses}},
                {
                    "$set": {
                        "message_point_balance": int(point_balance),
                        "status": "active",
                        "cooldown_until": None,
                        "error_count": 0,
                        "last_error": None,
                        "health_score": health_score,
                        "last_refresh_at": now,
                        "updated_at": now,
                    }
                },
            )
            return int(result.modified_count)

        return await self._run(_op)

    async def close(self) -> None:
        await self._run(self.client.close)


class RuntimeLimiter:
    def __init__(self, max_inflight_per_account: int, global_inflight_limit: int):
        self.max_inflight_per_account = max_inflight_per_account
        self.global_inflight_limit = global_inflight_limit
        self._global_inflight = 0
        self._account_inflight: dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    async def inflight_for(self, account_id: str) -> int:
        async with self._lock:
            return self._account_inflight.get(account_id, 0)

    async def try_acquire(self, account_id: str) -> bool:
        async with self._lock:
            if self._global_inflight >= self.global_inflight_limit:
                return False
            if self._account_inflight.get(account_id, 0) >= self.max_inflight_per_account:
                return False
            self._global_inflight += 1
            self._account_inflight[account_id] = self._account_inflight.get(account_id, 0) + 1
            return True

    async def release(self, account_id: str) -> None:
        async with self._lock:
            current = self._account_inflight.get(account_id, 0)
            if current <= 1:
                self._account_inflight.pop(account_id, None)
            else:
                self._account_inflight[account_id] = current - 1
            self._global_inflight = max(0, self._global_inflight - 1)


@dataclass
class AccountLease:
    account_id: str
    limiter: RuntimeLimiter
    released: bool = False

    async def release(self) -> None:
        if self.released:
            return
        self.released = True
        await self.limiter.release(self.account_id)


class AccountSelector:
    def __init__(
        self,
        repo: AccountRepository,
        limiter: RuntimeLimiter,
        *,
        top_n: int = 100,
        inflight_penalty: float = 50.0,
        recent_error_penalty: float = 20.0,
    ):
        self.repo = repo
        self.limiter = limiter
        self.top_n = top_n
        self.inflight_penalty = inflight_penalty
        self.recent_error_penalty = recent_error_penalty

    async def _top_and_primary_pool(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]], float]:
        candidates = await self.repo.list_candidate_accounts(limit=5000)
        if not candidates:
            raise NoAccountAvailableError("No active accounts are available")
        top_limit = min(self.top_n, len(candidates))
        top_accounts = candidates[:top_limit]
        balances = [int(a.get("message_point_balance", 0) or 0) for a in top_accounts]
        median_balance = float(statistics.median(balances)) if balances else 0.0
        primary = [a for a in top_accounts if int(a.get("message_point_balance", 0) or 0) >= median_balance]
        if not primary:
            primary = top_accounts
        return top_accounts, primary, median_balance

    async def get_primary_pool(self) -> list[dict[str, Any]]:
        _, primary, _ = await self._top_and_primary_pool()
        return primary

    async def _score_account(self, account: dict[str, Any]) -> float:
        account_id = str(account["_id"])
        inflight = await self.limiter.inflight_for(account_id)
        balance_weight = float(account.get("message_point_balance", 0) or 0)
        inflight_weight = float(inflight) * self.inflight_penalty
        recent_error_penalty = float(account.get("error_count", 0) or 0) * self.recent_error_penalty
        return balance_weight - inflight_weight - recent_error_penalty

    async def _select_from_pool(self, pool: list[dict[str, Any]]) -> Optional[tuple[dict[str, Any], AccountLease]]:
        scored: list[tuple[float, dict[str, Any]]] = []
        for account in pool:
            score = await self._score_account(account)
            scored.append((score, account))
        scored.sort(key=lambda item: item[0], reverse=True)

        for _, account in scored:
            account_id = str(account["_id"])
            if await self.limiter.try_acquire(account_id):
                return account, AccountLease(account_id=account_id, limiter=self.limiter)
        return None

    async def select_account(self) -> tuple[dict[str, Any], AccountLease]:
        top_accounts, primary, _ = await self._top_and_primary_pool()

        selected = await self._select_from_pool(primary)
        if selected:
            return selected

        selected = await self._select_from_pool(top_accounts)
        if selected:
            return selected

        raise CapacityLimitError("All candidate accounts are currently busy")


class PoeClientPool:
    def __init__(
        self,
        repo: AccountRepository,
        default_poe_revision: Optional[str] = None,
    ):
        self.repo = repo
        self.default_poe_revision = (default_poe_revision or "").strip()
        self._clients: dict[str, AsyncPoeApi] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    async def _get_lock(self, account_id: str) -> asyncio.Lock:
        async with self._global_lock:
            if account_id not in self._locks:
                self._locks[account_id] = asyncio.Lock()
            return self._locks[account_id]

    def _build_tokens(self, creds: dict[str, Any]) -> dict[str, str]:
        tokens: dict[str, str] = {
            "p-b": creds["poe_p_b"],
            "cf_clearance": creds["poe_cf_clearance"],
        }
        if creds.get("poe_cf_bm"):
            tokens["__cf_bm"] = creds["poe_cf_bm"]
        if creds.get("p_lat"):
            tokens["p-lat"] = creds["p_lat"]
        if creds.get("formkey"):
            tokens["formkey"] = creds["formkey"]
        poe_revision = (creds.get("poe_revision") or self.default_poe_revision or "").strip()
        if poe_revision:
            tokens["poe-revision"] = poe_revision
        return tokens

    def _build_headers(self, creds: dict[str, Any]) -> dict[str, str]:
        return {
            "User-Agent": creds.get("user_agent", DEFAULT_USER_AGENT),
            "Referer": "https://poe.com/login",
            "Origin": "https://poe.com",
        }

    async def _create_client(self, creds: dict[str, Any]) -> AsyncPoeApi:
        tokens = self._build_tokens(creds)
        headers = self._build_headers(creds)
        return await AsyncPoeApi(tokens=tokens, headers=headers).create()

    async def _create_client_with_fallback(self, account_id: str, creds: dict[str, Any]) -> AsyncPoeApi:
        try:
            return await self._create_client(creds)
        except Exception as first_error:
            if not creds.get("formkey"):
                raise first_error
            logger.warning(
                "Formkey path failed for account {}, retrying without stored formkey",
                mask_secret(account_id),
            )
            fallback_creds = dict(creds)
            fallback_creds.pop("formkey", None)
            client = await self._create_client(fallback_creds)
            if client.formkey:
                await self.repo.update_account_formkey(account_id, client.formkey)
            return client

    async def get_client(self, account_doc: dict[str, Any]) -> AsyncPoeApi:
        account_id = str(account_doc["_id"])
        existing = self._clients.get(account_id)
        if existing:
            return existing

        lock = await self._get_lock(account_id)
        async with lock:
            existing = self._clients.get(account_id)
            if existing:
                return existing
            creds = await self.repo.get_account_credentials(account_doc)
            client = await self._create_client_with_fallback(account_id, creds)
            self._clients[account_id] = client
            return client

    async def invalidate_client(self, account_id: str) -> None:
        client = self._clients.pop(account_id, None)
        if client:
            await self._close_client(client)

    async def _close_client(self, client: AsyncPoeApi) -> None:
        try:
            client.disconnect_ws()
        except Exception:
            pass
        try:
            await client.client.aclose()
        except Exception:
            pass

    async def close_all(self) -> None:
        clients = list(self._clients.values())
        self._clients.clear()
        for client in clients:
            await self._close_client(client)


class AccountHealthRefresher:
    def __init__(
        self,
        repo: AccountRepository,
        pool: PoeClientPool,
        *,
        depleted_threshold: int,
        refresh_interval_seconds: int,
        recent_active_minutes: int,
        cooldown_seconds: int,
        daily_reset_timezone: str = "America/Los_Angeles",
        daily_reset_hour: int = 0,
        daily_reset_point_balance: int = 3000,
        error_threshold: int = 3,
    ):
        self.repo = repo
        self.pool = pool
        self.depleted_threshold = depleted_threshold
        self.refresh_interval_seconds = refresh_interval_seconds
        self.recent_active_minutes = recent_active_minutes
        self.cooldown_seconds = cooldown_seconds
        self.daily_reset_timezone = daily_reset_timezone.strip() or "America/Los_Angeles"
        self.daily_reset_hour = max(0, min(int(daily_reset_hour), 23))
        self.daily_reset_point_balance = max(0, int(daily_reset_point_balance))
        self.error_threshold = error_threshold
        self._stopped = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._refresh_semaphore = asyncio.Semaphore(10)
        try:
            self._daily_reset_tz = ZoneInfo(self.daily_reset_timezone)
        except Exception:
            logger.warning(
                "Invalid DAILY_RESET_TIMEZONE='{}'. Fallback to America/Los_Angeles",
                self.daily_reset_timezone,
            )
            self.daily_reset_timezone = "America/Los_Angeles"
            self._daily_reset_tz = ZoneInfo(self.daily_reset_timezone)
        self._next_daily_reset_utc = self._compute_next_daily_reset_utc()

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._loop(), name="account-health-refresher")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def schedule_refresh(self, account_id: str) -> None:
        asyncio.create_task(self.refresh_account(account_id))

    def _compute_next_daily_reset_utc(self, now_utc: Optional[datetime] = None) -> datetime:
        now_utc = now_utc or utc_now()
        local_now = now_utc.astimezone(self._daily_reset_tz)
        target_local = local_now.replace(
            hour=self.daily_reset_hour,
            minute=0,
            second=0,
            microsecond=0,
        )
        if local_now >= target_local:
            target_local += timedelta(days=1)
        return target_local.astimezone(timezone.utc)

    async def _run_daily_point_reset_if_due(self, now_utc: datetime) -> None:
        if now_utc < self._next_daily_reset_utc:
            return
        modified_count = await self.repo.daily_reset_point_balance(
            point_balance=self.daily_reset_point_balance,
            reset_statuses=["active", "depleted", "cooldown"],
        )
        logger.info(
            "Daily Poe point reset executed at {} (tz={} {}:00). modified_accounts={}, point_balance={}",
            now_utc.isoformat(),
            self.daily_reset_timezone,
            self.daily_reset_hour,
            modified_count,
            self.daily_reset_point_balance,
        )
        self._next_daily_reset_utc = self._compute_next_daily_reset_utc(now_utc + timedelta(seconds=1))

    def _looks_like_depleted(self, error: str) -> bool:
        lower = error.lower()
        return any(token in lower for token in ("insufficient", "balance", "reached_limit", "402", "daily limit"))

    def _looks_like_invalid(self, error: str) -> bool:
        lower = error.lower()
        return any(token in lower for token in ("403", "unauthorized", "invalid", "forbidden", "challenge"))

    async def refresh_account(self, account_id: str) -> None:
        async with self._refresh_semaphore:
            account_doc = await self.repo.get_account_by_id(account_id)
            if not account_doc:
                return
            try:
                client = await self.pool.get_client(account_doc)
                settings = await client.get_settings()
                message_info = settings.get("messagePointInfo", {})
                subscription = settings.get("subscription", {})
                balance = int(message_info.get("subscriptionPointBalance", 0) or 0) + int(
                    message_info.get("addonPointBalance", 0) or 0
                )
                subscription_active = bool(subscription.get("isActive", False))
                await self.repo.update_account_health(
                    account_id,
                    balance=balance,
                    subscription_active=subscription_active,
                    depleted_threshold=self.depleted_threshold,
                )
                await self.repo.mark_account_success(account_id)
                if balance <= self.depleted_threshold:
                    await self.pool.invalidate_client(account_id)
            except Exception as exc:
                error_text = str(exc)
                if self._looks_like_depleted(error_text):
                    await self.repo.mark_account_depleted(account_id, error_text)
                    await self.pool.invalidate_client(account_id)
                    return
                if self._looks_like_invalid(error_text):
                    await self.repo.mark_account_invalid(account_id, error_text)
                    await self.pool.invalidate_client(account_id)
                    return
                await self.repo.record_account_error(
                    account_id,
                    error_text,
                    cooldown_seconds=self.cooldown_seconds,
                    error_threshold=self.error_threshold,
                )

    async def _loop(self) -> None:
        while not self._stopped.is_set():
            now_utc = utc_now()
            seconds_until_reset = max(1.0, (self._next_daily_reset_utc - now_utc).total_seconds())
            sleep_timeout = max(1.0, min(float(self.refresh_interval_seconds), seconds_until_reset))
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=sleep_timeout)
                if self._stopped.is_set():
                    break
            except asyncio.TimeoutError:
                pass

            now_utc = utc_now()
            try:
                await self._run_daily_point_reset_if_due(now_utc)
            except Exception as exc:
                logger.warning("Daily point reset failed: {}", exc)
                self._next_daily_reset_utc = utc_now() + timedelta(minutes=1)

            recent_accounts = await self.repo.list_recent_active_accounts(
                minutes=self.recent_active_minutes,
                limit=200,
            )
            if not recent_accounts:
                continue
            await asyncio.gather(
                *(self.refresh_account(str(account["_id"])) for account in recent_accounts),
                return_exceptions=True,
            )


class SessionManager:
    def __init__(self, repo: AccountRepository):
        self.repo = repo

    @staticmethod
    def resolve_session_id(metadata: Optional[dict[str, Any]], user: Optional[str]) -> tuple[str, bool]:
        if metadata and isinstance(metadata, dict):
            session_id = metadata.get("session_id")
            if isinstance(session_id, str) and session_id.strip():
                return session_id.strip(), True
        if isinstance(user, str) and user.strip():
            return user.strip(), True
        return f"ephemeral-{uuid.uuid4().hex}", False

    async def get_session(self, session_id: str) -> Optional[dict[str, Any]]:
        return await self.repo.get_session(session_id)

    async def bind_session(
        self,
        *,
        session_id: str,
        model: str,
        account_id: str,
        chat_code: Optional[str] = None,
        chat_id: Optional[int] = None,
    ) -> dict[str, Any]:
        return await self.repo.upsert_session(
            session_id=session_id,
            model=model,
            account_id=account_id,
            chat_code=chat_code,
            chat_id=chat_id,
            state="active",
        )

    async def update_session_chat(
        self,
        *,
        session_id: str,
        model: str,
        account_id: str,
        chat_code: Optional[str],
        chat_id: Optional[int],
    ) -> None:
        await self.repo.upsert_session(
            session_id=session_id,
            model=model,
            account_id=account_id,
            chat_code=chat_code,
            chat_id=chat_id,
            state="active",
        )

    async def break_session(self, session_id: str, reason: str) -> None:
        await self.repo.mark_session_broken(session_id, reason)
