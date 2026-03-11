from __future__ import annotations

import asyncio
import hashlib
import os
import re
import sys
import time as _time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Optional
from zoneinfo import ZoneInfo

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
from cryptography.fernet import Fernet
import httpx
from loguru import logger
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError

try:
    from curl_cffi import requests as cffi_requests

    HAS_CURL_CFFI = True
except Exception:
    cffi_requests = None
    HAS_CURL_CFFI = False

if TYPE_CHECKING:
    from poe_api_wrapper.reverse import AsyncPoeApi


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)
DEFAULT_NEW_ACCOUNT_POINT_BALANCE = 3000


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


_POE_REVISION_RE_OLD = re.compile(r"poecdn\.net/assets/translations/([a-f0-9]{40})/")
_POE_REVISION_RE_NEW = re.compile(r"poecdn\.net/assets/_next/static/([^/]+)/(?:chunks|css)/")

_BROWSE_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Priority": "u=0, i",
    "Sec-Ch-Ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}


async def fetch_poe_revision(
    *,
    timeout: float = 15.0,
    retries: int = 2,
) -> Optional[str]:
    """Fetch poe-revision/build-id from poe.com home/login responses."""

    def _resolve_proxy() -> str:
        return (
            os.getenv("POE_FETCH_PROXY", "").strip()
            or os.getenv("HTTPS_PROXY", "").strip()
            or os.getenv("https_proxy", "").strip()
            or os.getenv("HTTP_PROXY", "").strip()
            or os.getenv("http_proxy", "").strip()
            or os.getenv("ALL_PROXY", "").strip()
            or os.getenv("all_proxy", "").strip()
        )

    def _resolve_cf_clearance() -> str:
        return (
            os.getenv("POE_FETCH_CF_CLEARANCE", "").strip()
            or os.getenv("POE_CF_CLEARANCE", "").strip()
        )

    def _extract_revision(html: str, link_header: str) -> Optional[str]:
        for pattern in (_POE_REVISION_RE_NEW, _POE_REVISION_RE_OLD):
            m = pattern.search(link_header or "")
            if m:
                return m.group(1)
            m = pattern.search(html or "")
            if m:
                return m.group(1)
        return None

    def _extract_cookie_from_headers(set_cookie_values: list[str], name: str) -> str:
        prefix = f"{name}="
        for sc in set_cookie_values:
            if sc.startswith(prefix):
                return sc.split(";", 1)[0].split("=", 1)[1]
        return ""

    def _fetch_once_with_curl_cffi(*, timeout_sec: float, proxy_url: str) -> Optional[str]:
        session_kwargs: dict[str, Any] = {
            "impersonate": os.getenv("POE_FETCH_IMPERSONATE", "chrome131").strip() or "chrome131",
            "timeout": timeout_sec,
        }
        if proxy_url:
            session_kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}

        with cffi_requests.Session(**session_kwargs) as session:
            home = session.get("https://poe.com/", headers=_BROWSE_HEADERS, allow_redirects=False)
            p_b = session.cookies.get("p-b", "") or home.cookies.get("p-b", "")
            cf_bm = session.cookies.get("__cf_bm", "") or home.cookies.get("__cf_bm", "")
            cf_clearance = _resolve_cf_clearance() or session.cookies.get("cf_clearance", "")

            logger.debug(
                "fetch_poe_revision(curl_cffi): home status={} p_b={} cf_bm={}",
                home.status_code,
                bool(p_b),
                bool(cf_bm),
            )

            revision = _extract_revision("", home.headers.get("link", ""))
            if revision:
                return revision

            cookies: dict[str, str] = {}
            if p_b:
                cookies["p-b"] = p_b
            if cf_bm:
                cookies["__cf_bm"] = cf_bm
            if cf_clearance:
                cookies["cf_clearance"] = cf_clearance

            login = session.get(
                "https://poe.com/login?redirect_url=%2F",
                headers=_BROWSE_HEADERS,
                cookies=cookies if cookies else None,
                allow_redirects=False,
            )
            logger.debug(
                "fetch_poe_revision(curl_cffi): login status={} content_type={}",
                login.status_code,
                login.headers.get("content-type", ""),
            )
            return _extract_revision(login.text, login.headers.get("link", ""))

    proxy_url = _resolve_proxy()
    for attempt in range(1, retries + 2):
        try:
            if HAS_CURL_CFFI:
                revision = await asyncio.to_thread(
                    _fetch_once_with_curl_cffi,
                    timeout_sec=timeout,
                    proxy_url=proxy_url,
                )
                if revision:
                    return revision
                logger.warning(
                    "fetch_poe_revision: revision not found via curl_cffi (attempt {}/{})",
                    attempt,
                    retries + 1,
                )
            else:
                client_kwargs: dict[str, Any] = {
                    "timeout": timeout,
                    "follow_redirects": False,
                }
                if proxy_url:
                    client_kwargs["proxy"] = proxy_url

                async with httpx.AsyncClient(**client_kwargs) as client:
                    home_resp = await client.get("https://poe.com/", headers=_BROWSE_HEADERS)
                    set_cookies = home_resp.headers.get_list("set-cookie")
                    p_b = (
                        client.cookies.get("p-b")
                        or home_resp.cookies.get("p-b")
                        or _extract_cookie_from_headers(set_cookies, "p-b")
                    )
                    cf_bm = (
                        client.cookies.get("__cf_bm")
                        or home_resp.cookies.get("__cf_bm")
                        or _extract_cookie_from_headers(set_cookies, "__cf_bm")
                    )
                    cf_clearance = (
                        _resolve_cf_clearance()
                        or client.cookies.get("cf_clearance")
                        or home_resp.cookies.get("cf_clearance")
                        or _extract_cookie_from_headers(set_cookies, "cf_clearance")
                    )

                    logger.debug(
                        "fetch_poe_revision(httpx): home status={} p_b={} cf_bm={} (attempt {}/{})",
                        home_resp.status_code,
                        bool(p_b),
                        bool(cf_bm),
                        attempt,
                        retries + 1,
                    )

                    revision = _extract_revision("", home_resp.headers.get("link", ""))
                    if revision:
                        return revision

                    login_cookies: dict[str, str] = {}
                    if p_b:
                        login_cookies["p-b"] = p_b
                    if cf_bm:
                        login_cookies["__cf_bm"] = cf_bm
                    if cf_clearance:
                        login_cookies["cf_clearance"] = cf_clearance

                    login_resp = await client.get(
                        "https://poe.com/login?redirect_url=%2F",
                        headers=_BROWSE_HEADERS,
                        cookies=login_cookies if login_cookies else None,
                    )
                    logger.debug(
                        "fetch_poe_revision(httpx): login status={} content_type={} (attempt {}/{})",
                        login_resp.status_code,
                        login_resp.headers.get("content-type", ""),
                        attempt,
                        retries + 1,
                    )
                    revision = _extract_revision(login_resp.text, login_resp.headers.get("link", ""))
                    if revision:
                        return revision
                    logger.warning(
                        "fetch_poe_revision: revision not found via httpx (login_status={}, attempt {}/{})",
                        login_resp.status_code,
                        attempt,
                        retries + 1,
                    )
        except Exception as exc:
            logger.warning(
                "fetch_poe_revision: request failed (attempt {}/{}): {}",
                attempt,
                retries + 1,
                exc,
            )

        if attempt <= retries:
            await asyncio.sleep(2.0 * attempt)
    return None

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
        self.metadata = self.db["metadata"]
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
        initial_balance = int(DEFAULT_NEW_ACCOUNT_POINT_BALANCE)
        initial_health_score = min(100.0, float(initial_balance) / 10.0)
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
                        "message_point_balance": initial_balance,
                        "subscription_active": False,
                        "last_refresh_at": None,
                        "last_success_at": None,
                        "last_error": None,
                        "error_count": 0,
                        "cooldown_until": None,
                        "health_score": initial_health_score,
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

    async def list_all_accounts_for_refresh(
        self,
        *,
        statuses: Optional[list[str]] = None,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        """Return minimal account docs for bulk refresh."""
        query: dict[str, Any] = {}
        if statuses:
            query["status"] = {"$in": statuses}

        def _op():
            return list(
                self.accounts.find(
                    query,
                    {"_id": 1},
                )
                .sort("_id", ASCENDING)
                .limit(limit)
            )

        return await self._run(_op)

    async def fetch_accounts_sequential(
        self,
        *,
        after_id: Optional[Any] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """按 _id 顺序取 active 账号，after_id 用于翻页（从头取时传 None）。"""
        query: dict[str, Any] = {"status": "active"}
        if after_id is not None:
            query["_id"] = {"$gt": after_id}

        def _op():
            return list(
                self.accounts.find(
                    query,
                    {
                        "_id": 1,
                        "status": 1,
                        "message_point_balance": 1,
                        "ever_connected": 1,
                        "cooldown_until": 1,
                    },
                )
                .sort("_id", ASCENDING)
                .limit(limit)
            )

        return await self._run(_op)

    async def get_cursor(self, key: str) -> Optional[Any]:
        """从 metadata 集合读取持久化游标值，不存在则返回 None。"""
        def _op():
            doc = self.metadata.find_one({"_id": key})
            return doc.get("value") if doc else None
        return await self._run(_op)

    async def set_cursor(self, key: str, value: Any) -> None:
        """原子更新持久化游标值（upsert）。"""
        def _op():
            self.metadata.find_one_and_update(
                {"_id": key},
                {"$set": {"value": value, "updated_at": utc_now()}},
                upsert=True,
            )
        await self._run(_op)

    async def get_used_account_balances(self) -> list[int]:
        """返回所有 ever_connected=True 的 active 账号余额，用于计算中位数。"""
        def _op():
            docs = list(
                self.accounts.find(
                    {"ever_connected": True, "status": "active"},
                    {"message_point_balance": 1},
                )
            )
            return [int(doc.get("message_point_balance", 0) or 0) for doc in docs]

        return await self._run(_op)

    async def mark_account_ever_connected(self, account_id: str) -> None:
        """标记账号已成功建立过连接。"""
        await self._run(
            self.accounts.update_one,
            {"_id": self._object_id(account_id)},
            {"$set": {"ever_connected": True, "updated_at": utc_now()}},
        )

    _candidate_cache: Optional[list[dict[str, Any]]] = None
    _candidate_cache_at: float = 0.0
    _CANDIDATE_CACHE_TTL: float = 5.0  # 缓存 5 秒

    async def list_candidate_accounts(self, limit: int = 500) -> list[dict[str, Any]]:
        now_mono = _time.monotonic()
        if (
            self._candidate_cache is not None
            and (now_mono - self._candidate_cache_at) < self._CANDIDATE_CACHE_TTL
        ):
            return self._candidate_cache[:limit]

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
            cursor = self.accounts.find(
                query,
                {
                    "_id": 1,
                    "message_point_balance": 1,
                    "error_count": 1,
                    "subscription_active": 1,
                    "status": 1,
                },
            ).sort(
                [
                    ("message_point_balance", DESCENDING),
                    ("_id", ASCENDING),
                ]
            )
            if limit > 0:
                cursor = cursor.limit(limit)
            return list(cursor)

        result = await self._run(_op)
        self._candidate_cache = result
        self._candidate_cache_at = now_mono
        return result

    async def mark_account_success(self, account_id: str) -> None:
        now = utc_now()

        def _op():
            # 聚合管道更新：单次 IO 完成所有逻辑
            self.accounts.update_one(
                {"_id": self._object_id(account_id)},
                [
                    {
                        "$set": {
                            "last_success_at": now,
                            "last_error": None,
                            "updated_at": now,
                            "error_count": {
                                "$max": [0, {"$subtract": [{"$ifNull": ["$error_count", 0]}, 1]}]
                            },
                            "status": {
                                "$cond": [{"$eq": ["$status", "depleted"]}, "$status", "active"]
                            },
                            "cooldown_until": {
                                "$cond": [{"$eq": ["$status", "depleted"]}, "$cooldown_until", None]
                            },
                        }
                    }
                ],
            )

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
                        "ever_connected": False,  # 每日重置时重置"已使用"标记
                    }
                },
            )
            return int(result.modified_count)

        return await self._run(_op)

    async def reset_all_ever_connected(self) -> int:
        """重置所有账号的 ever_connected 标记为 False，用于服务启动时清理状态。"""
        now = utc_now()

        def _op() -> int:
            result = self.accounts.update_many(
                {},  # 所有账号
                {"$set": {"ever_connected": False, "updated_at": now}},
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
        self._blocked_accounts: set[str] = set()
        self._lock = asyncio.Lock()

    async def inflight_for(self, account_id: str) -> int:
        async with self._lock:
            return self._account_inflight.get(account_id, 0)

    async def block_account(self, account_id: str) -> None:
        async with self._lock:
            self._blocked_accounts.add(account_id)

    async def unblock_account(self, account_id: str) -> None:
        async with self._lock:
            self._blocked_accounts.discard(account_id)

    async def try_acquire(self, account_id: str) -> bool:
        async with self._lock:
            if account_id in self._blocked_accounts:
                return False
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
        pool: Optional["PoeClientPool"] = None,
        *,
        top_n: int = 300,
        inflight_penalty: float = 50.0,
        recent_error_penalty: float = 20.0,
    ):
        self.repo = repo
        self.limiter = limiter
        self.pool = pool
        self.top_n = top_n
        self.inflight_penalty = inflight_penalty
        self.recent_error_penalty = recent_error_penalty

    def _filter_prewarmed_accounts(self, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not self.pool:
            return candidates
        cached_ids = self.pool.cached_account_ids()
        if not cached_ids:
            return []
        return [account for account in candidates if str(account["_id"]) in cached_ids]

    async def _top_pool(self, *, prewarmed_only: bool = False) -> list[dict[str, Any]]:
        candidates = await self.repo.list_candidate_accounts(limit=500)
        if not candidates:
            raise NoAccountAvailableError("No active accounts are available")
        if prewarmed_only:
            candidates = self._filter_prewarmed_accounts(candidates)
            if not candidates:
                raise CapacityLimitError("No prewarmed accounts are currently available")
        top_limit = min(self.top_n, len(candidates))
        return candidates[:top_limit]

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

    async def select_account(self, *, prewarmed_only: bool = False) -> tuple[dict[str, Any], AccountLease]:
        top_accounts = await self._top_pool(prewarmed_only=prewarmed_only)
        selected = await self._select_from_pool(top_accounts)
        if selected:
            return selected

        raise CapacityLimitError("All candidate accounts are currently busy")


class PoeClientPool:
    def __init__(
        self,
        repo: AccountRepository,
        default_poe_revision: Optional[str] = None,
        client_max_age_seconds: int = 600,
        depleted_threshold: int = 20,
    ):
        self.repo = repo
        self.default_poe_revision = (default_poe_revision or "").strip()
        self.client_max_age_seconds = client_max_age_seconds
        self._depleted_threshold = depleted_threshold
        self._clients: dict[str, "AsyncPoeApi"] = {}
        self._client_created_at: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        # External callback: called when a client is evicted after reconnect
        # exhaustion.  Signature: async (account_id: str) -> None
        self._on_evict_callback: Optional[Any] = None

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

    async def _create_client(self, creds: dict[str, Any]) -> "AsyncPoeApi":
        from poe_api_wrapper.reverse import AsyncPoeApi

        tokens = self._build_tokens(creds)
        headers = self._build_headers(creds)
        client = AsyncPoeApi(tokens=tokens, headers=headers)
        ok = False
        try:
            await client.create()
            ok = True
            return client
        finally:
            if not ok:
                # 创建失败（含被 cancel），清理半成品防止 WS/HTTP 泄漏
                try:
                    client.disconnect_ws()
                except Exception:
                    pass
                try:
                    await client.client.aclose()
                except Exception:
                    pass

    async def _create_client_with_fallback(self, account_id: str, creds: dict[str, Any]) -> "AsyncPoeApi":
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

    async def get_client(self, account_doc: dict[str, Any]) -> "AsyncPoeApi":
        return await self.get_client_for_account(account_doc, create_if_missing=True)

    def cached_account_ids(self) -> set[str]:
        return set(self._clients.keys())

    def cached_account_ids_in_order(self) -> list[str]:
        # Dict insertion order tracks warm-client generation order.
        return list(self._clients.keys())

    def has_client(self, account_id: str) -> bool:
        return account_id in self._clients

    def is_client_expired(self, account_id: str) -> bool:
        import time
        created_at = self._client_created_at.get(account_id)
        if created_at is None:
            return True
        return (time.time() - created_at) > self.client_max_age_seconds

    def store_prewarmed_client(self, account_id: str, client: "AsyncPoeApi") -> None:
        """Store a client that was prewarmed on a separate event loop.

        Thread-safe at the CPython GIL level (simple dict assignment).
        The caller is responsible for having already called
        ``client.migrate_to_loop(main_loop)`` before invoking this.
        """
        import time
        self._register_reconnect_callback(account_id, client)
        self._clients[account_id] = client
        self._client_created_at[account_id] = time.time()

    async def get_client_for_account(
        self,
        account_doc: dict[str, Any],
        *,
        create_if_missing: bool,
    ) -> "AsyncPoeApi":
        account_id = str(account_doc["_id"])
        existing = self._clients.get(account_id)
        if existing:
            return existing
        if not create_if_missing:
            raise RuntimeError(f"Client is not prewarmed for account {account_id}")

        lock = await self._get_lock(account_id)
        async with lock:
            existing = self._clients.get(account_id)
            if existing:
                return existing
            if not create_if_missing:
                raise RuntimeError(f"Client is not prewarmed for account {account_id}")
            import time
            creds = await self.repo.get_account_credentials(account_doc)
            client = await self._create_client_with_fallback(account_id, creds)
            self._register_reconnect_callback(account_id, client)
            self._clients[account_id] = client
            self._client_created_at[account_id] = time.time()
            return client

    def _register_reconnect_callback(self, account_id: str, client: "AsyncPoeApi") -> None:
        """Attach a reconnect-exhaustion callback to a client.

        When the WS reconnects fail ``max_ws_reconnects`` times in a row,
        the callback fires (from the WS thread) and schedules async cleanup
        on the main event loop.
        """
        pool = self

        def _on_exhausted(cli: "AsyncPoeApi") -> None:
            loop = pool._main_loop or getattr(cli, "loop", None)
            if not loop or not loop.is_running():
                logger.warning(
                    "Reconnect exhausted for account {} but no running loop to schedule cleanup",
                    mask_secret(account_id),
                )
                return
            asyncio.run_coroutine_threadsafe(
                pool._handle_reconnect_exhausted(account_id),
                loop,
            )

        client._on_reconnect_exhausted = _on_exhausted

    async def _handle_reconnect_exhausted(self, account_id: str) -> None:
        """Evict client, record error, optionally re-enqueue for prewarm."""
        logger.warning(
            "Account {} evicted from pool after reconnect exhaustion",
            mask_secret(account_id),
        )
        await self.invalidate_client(account_id)
        await self.repo.record_account_error(
            account_id,
            "ws_reconnect_exhausted: WebSocket connection lost after max retries",
            cooldown_seconds=120,
            error_threshold=2,
        )
        # Notify external listener (e.g. prewarm coordinator) to re-queue.
        cb = self._on_evict_callback
        if cb:
            try:
                await cb(account_id)
            except Exception:
                logger.exception("on_evict_callback failed for {}", mask_secret(account_id))

    async def invalidate_client(self, account_id: str) -> None:
        client = self._clients.pop(account_id, None)
        self._client_created_at.pop(account_id, None)
        if client:
            await self._close_client(client, account_id=account_id)

    async def _close_client(self, client: "AsyncPoeApi", *, account_id: Optional[str] = None) -> None:
        # 销毁前查询余额并存库
        if account_id:
            try:
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
                    depleted_threshold=self._depleted_threshold,
                )
                logger.info(
                    "Client destroyed, updated balance for account {}: {}",
                    mask_secret(account_id),
                    balance,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to fetch balance before closing client for account {}: {}",
                    mask_secret(account_id),
                    exc,
                )
        try:
            client.disconnect_ws()
        except Exception:
            pass
        try:
            await client.client.aclose()
        except Exception:
            pass

    async def close_all(self) -> None:
        items = list(self._clients.items())
        self._clients.clear()
        self._client_created_at.clear()
        for account_id, client in items:
            await self._close_client(client, account_id=account_id)


class AccountHealthRefresher:
    def __init__(
        self,
        repo: AccountRepository,
        *,
        daily_reset_timezone: str = "America/Los_Angeles",
        daily_reset_hour: int = 0,
        daily_reset_point_balance: int = 3000,
    ):
        self.repo = repo
        self.daily_reset_timezone = daily_reset_timezone.strip() or "America/Los_Angeles"
        self.daily_reset_hour = max(0, min(int(daily_reset_hour), 23))
        self.daily_reset_point_balance = max(0, int(daily_reset_point_balance))
        self._stopped = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
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

    async def refresh_all_accounts(
        self,
        *,
        statuses: Optional[list[str]] = None,
        concurrency: int = 10,
    ) -> dict[str, Any]:
        """手动触发：通过 admin API 立即刷新所有账号积分（创建临时 client 查询后关闭）。"""
        from poe_api_wrapper.reverse import AsyncPoeApi

        target_statuses = statuses or ["active", "depleted", "cooldown", "invalid"]
        all_docs = await self.repo.list_all_accounts_for_refresh(
            statuses=target_statuses,
            limit=5000,
        )
        total = len(all_docs)
        if total == 0:
            return {"total": 0, "succeeded": 0, "failed": 0, "errors": []}

        logger.info(
            "refresh_all_accounts: starting refresh for {} accounts (statuses={})",
            total,
            target_statuses,
        )

        succeeded = 0
        failed = 0
        errors: list[dict[str, str]] = []
        sem = asyncio.Semaphore(concurrency)

        async def _do_one(account_doc: dict[str, Any]) -> None:
            nonlocal succeeded, failed
            account_id = str(account_doc["_id"])
            async with sem:
                try:
                    full_doc = await self.repo.get_account_by_id(account_id)
                    if not full_doc:
                        return
                    creds = await self.repo.get_account_credentials(full_doc)
                    tokens: dict[str, str] = {"p-b": creds["poe_p_b"], "cf_clearance": creds["poe_cf_clearance"]}
                    if creds.get("poe_cf_bm"):
                        tokens["__cf_bm"] = creds["poe_cf_bm"]
                    if creds.get("p_lat"):
                        tokens["p-lat"] = creds["p_lat"]
                    if creds.get("formkey"):
                        tokens["formkey"] = creds["formkey"]
                    if creds.get("poe_revision"):
                        tokens["poe-revision"] = creds["poe_revision"]
                    client = await AsyncPoeApi(tokens=tokens).create()
                    try:
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
                            depleted_threshold=20,
                        )
                        succeeded += 1
                    finally:
                        try:
                            client.disconnect_ws()
                        except Exception:
                            pass
                        try:
                            await client.client.aclose()
                        except Exception:
                            pass
                except Exception as exc:
                    failed += 1
                    errors.append({"account_id": account_id, "error": str(exc)})

        await asyncio.gather(*(_do_one(doc) for doc in all_docs), return_exceptions=True)

        logger.info(
            "refresh_all_accounts: done. total={} succeeded={} failed={}",
            total,
            succeeded,
            failed,
        )
        return {
            "total": total,
            "succeeded": succeeded,
            "failed": failed,
            "errors": errors[:50],
        }

    async def _loop(self) -> None:
        while not self._stopped.is_set():
            now_utc = utc_now()
            seconds_until_reset = max(60.0, (self._next_daily_reset_utc - now_utc).total_seconds())
            sleep_timeout = min(3600.0, seconds_until_reset)
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


class ProxyRotator:
    """根据连接成功/失败率自动轮换代理地区。

    代理 URL 格式: http://{user}-res-{REGION}:{password}@{host}:{port}
    从环境变量 PROXY_ROTATE_TEMPLATE 读取模板，其中地区占位符为 ``{region}``。
    若未配置模板，则从当前 https_proxy 自动推断。

    当连续失败次数达到阈值时，切换到下一个地区并更新 os.environ，
    使后续新建的 httpx AsyncClient (trust_env=True) 自动走新代理。
    """

    REGIONS = ("US", "UK", "SG", "KR", "TW", "JP")

    def __init__(
        self,
        *,
        fail_threshold: int = 5,
    ):
        self._fail_threshold = max(1, fail_threshold)
        self._consecutive_failures = 0
        self._region_index = 0
        self._template: Optional[str] = None
        self._init_template()

    def _init_template(self) -> None:
        """尝试从环境变量构建代理模板。"""
        explicit = os.getenv("PROXY_ROTATE_TEMPLATE", "").strip()
        if explicit:
            self._template = explicit
            logger.info("ProxyRotator: using explicit template from PROXY_ROTATE_TEMPLATE")
            return

        # 从当前 https_proxy 自动推断
        current = (
            os.getenv("https_proxy", "").strip()
            or os.getenv("HTTPS_PROXY", "").strip()
            or os.getenv("http_proxy", "").strip()
            or os.getenv("HTTP_PROXY", "").strip()
        )
        if not current:
            logger.info("ProxyRotator: no proxy configured, rotation disabled")
            return

        # 匹配 -res-XX 模式，替换为 {region} 占位符
        pattern = re.compile(r"(-res-)([A-Z]{2})")
        match = pattern.search(current)
        if not match:
            logger.info(
                "ProxyRotator: proxy URL does not contain -res-XX pattern, rotation disabled"
            )
            return

        self._template = pattern.sub(r"\1{region}", current)
        # 当前地区对齐到 REGIONS 列表
        current_region = match.group(2)
        try:
            self._region_index = list(self.REGIONS).index(current_region)
        except ValueError:
            self._region_index = 0
        logger.info(
            "ProxyRotator: inferred template from current proxy, starting region={}",
            self.REGIONS[self._region_index],
        )

    @property
    def enabled(self) -> bool:
        return self._template is not None

    @property
    def current_region(self) -> str:
        return self.REGIONS[self._region_index]

    def record_success(self) -> None:
        if not self.enabled:
            return
        self._consecutive_failures = 0

    def record_failure(self) -> None:
        if not self.enabled:
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._fail_threshold:
            self._rotate()

    def _rotate(self) -> None:
        old_region = self.REGIONS[self._region_index]
        self._region_index = (self._region_index + 1) % len(self.REGIONS)
        new_region = self.REGIONS[self._region_index]
        self._consecutive_failures = 0

        new_url = self._template.format(region=new_region)
        os.environ["https_proxy"] = new_url
        os.environ["http_proxy"] = new_url
        os.environ["HTTPS_PROXY"] = new_url
        os.environ["HTTP_PROXY"] = new_url
        logger.warning(
            "ProxyRotator: {} consecutive failures, rotating proxy {} -> {} | url={}",
            self._fail_threshold,
            old_region,
            new_region,
            re.sub(r"://[^@]+@", "://***@", new_url),  # 脱敏
        )


class PoolMonitor:
    """单一后台任务：监控 pool 中 client 数量，低于阈值时从 DB 顺序补充。

    补充规则：
    - 从 DB 按 _id 顺序扫描 active 账号
    - 计算 ever_connected=True 账号的余额中位数
    - 账号满足以下条件之一才建连：
        1. ever_connected=False（从未连过）
        2. ever_connected=True 且 balance >= 中位数
    - 否则跳过，取下一个账号
    - 扫完一轮后从头再来（循环）

    TTL：
    - 单独的 TTL 检查循环，每 ttl_check_interval_seconds 检查一次
    - 超过 client_max_age_seconds 的 client 会被销毁（销毁前查余额存DB）
    """

    def __init__(
        self,
        repo: AccountRepository,
        pool: PoeClientPool,
        *,
        target_pool_size: int = 20,
        min_pool_size: int = 5,
        max_pool_size: int = 30,
        fill_concurrency: int = 10,
        monitor_interval_seconds: int = 5,
        connect_timeout_seconds: int = 20,
        ttl_check_interval_seconds: int = 30,
        proxy_rotator: Optional[ProxyRotator] = None,
    ):
        self.repo = repo
        self.pool = pool
        self.proxy_rotator = proxy_rotator or ProxyRotator()
        # 动态调整范围
        self.min_pool_size = max(1, min_pool_size)
        self.max_pool_size = max(self.min_pool_size, max_pool_size)
        # 初始值在范围内
        self.target_pool_size = max(
            self.min_pool_size,
            min(self.max_pool_size, target_pool_size),
        )
        self.fill_concurrency = max(1, fill_concurrency)
        self.monitor_interval_seconds = max(1, monitor_interval_seconds)
        self.connect_timeout_seconds = max(5, connect_timeout_seconds)
        self.ttl_check_interval_seconds = max(10, ttl_check_interval_seconds)

        self._stopped = asyncio.Event()
        self._monitor_task: Optional[asyncio.Task] = None
        self._ttl_task: Optional[asyncio.Task] = None
        # 记录扫描游标（上次扫到的最后一个 _id），启动时从 DB 加载
        self._last_scanned_id: Optional[Any] = None
        self._cursor_loaded = False
        self._cursor_key = "pool_monitor_cursor"
        # 正在建连的账号 id 集合，避免重复
        self._connecting: set[str] = set()
        self._connecting_lock = asyncio.Lock()
        # 限制同时建连的并发数，防止线程池/网络被打满
        self._connect_semaphore = asyncio.Semaphore(self.fill_concurrency)
        # 滚动窗口错误统计 — 过去 N 秒内失败次数超过阈值则自杀重启
        self._error_window_seconds = int(os.getenv("POOL_ERROR_WINDOW_SECONDS", "300"))
        self._error_restart_threshold = int(os.getenv("POOL_ERROR_RESTART_THRESHOLD", "5"))
        self._error_timestamps: deque[float] = deque()

    def start(self) -> None:
        if self._monitor_task and not self._monitor_task.done():
            return
        self._stopped.clear()
        self._monitor_task = asyncio.create_task(self._monitor_loop(), name="pool-monitor")
        self._ttl_task = asyncio.create_task(self._ttl_loop(), name="pool-ttl-cleanup")
        logger.info(
            "PoolMonitor started: target={} (range {}-{}) concurrency={} interval={}s ttl_check={}s",
            self.target_pool_size,
            self.min_pool_size,
            self.max_pool_size,
            self.fill_concurrency,
            self.monitor_interval_seconds,
            self.ttl_check_interval_seconds,
        )

    async def stop(self) -> None:
        self._stopped.set()
        for task in (self._monitor_task, self._ttl_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._monitor_task = None
        self._ttl_task = None

    def _record_connect_error(self) -> None:
        now = _time.monotonic()
        self._error_timestamps.append(now)
        # 清理窗口外的旧记录
        cutoff = now - self._error_window_seconds
        while self._error_timestamps and self._error_timestamps[0] < cutoff:
            self._error_timestamps.popleft()
        recent = len(self._error_timestamps)
        if recent >= self._error_restart_threshold:
            logger.critical(
                "PoolMonitor: {} connect errors in the last {}s (threshold={}), "
                "forcing process restart!",
                recent,
                self._error_window_seconds,
                self._error_restart_threshold,
            )
            os._exit(1)

    async def _monitor_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                await self._fill_pool()
            except Exception as exc:
                logger.warning("PoolMonitor fill_pool error: {}", exc)
            try:
                await asyncio.wait_for(
                    self._stopped.wait(),
                    timeout=self.monitor_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass

    async def _ttl_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                await asyncio.wait_for(
                    self._stopped.wait(),
                    timeout=self.ttl_check_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass
            if self._stopped.is_set():
                break
            try:
                await self._evict_expired()
            except Exception as exc:
                logger.warning("PoolMonitor TTL evict error: {}", exc)

    async def _evict_expired(self) -> None:
        expired_ids = [
            aid
            for aid in list(self.pool.cached_account_ids())
            if self.pool.is_client_expired(aid)
        ]
        if not expired_ids:
            return
        logger.info("PoolMonitor TTL: evicting {} expired client(s)", len(expired_ids))
        evicted_count = 0
        for account_id in expired_ids:
            try:
                await self.pool.invalidate_client(account_id)
                evicted_count += 1
            except Exception as exc:
                logger.debug("TTL evict failed for {}: {}", mask_secret(account_id), exc)
        # TTL 正常过期销毁，动态降低池子大小
        if evicted_count > 0:
            old_size = self.target_pool_size
            self.target_pool_size = max(self.min_pool_size, self.target_pool_size - evicted_count)
            if self.target_pool_size != old_size:
                logger.info(
                    "PoolMonitor: TTL evicted {} client(s), target_pool_size {} -> {}",
                    evicted_count,
                    old_size,
                    self.target_pool_size,
                )

    async def _fill_pool(self) -> None:
        # 首次执行时从 DB 加载游标位置
        if not self._cursor_loaded:
            self._last_scanned_id = await self.repo.get_cursor(self._cursor_key)
            self._cursor_loaded = True
            logger.info("PoolMonitor: loaded cursor from DB: {}", self._last_scanned_id)

        current_count = len(self.pool.cached_account_ids())
        async with self._connecting_lock:
            in_flight = len(self._connecting)

        # 动态调整：池子数量过低时增加 target_pool_size
        if current_count < 3:
            old_size = self.target_pool_size
            self.target_pool_size = min(self.max_pool_size, self.target_pool_size + 10)
            if self.target_pool_size != old_size:
                logger.info(
                    "PoolMonitor: pool too low ({}), target_pool_size {} -> {}",
                    current_count,
                    old_size,
                    self.target_pool_size,
                )

        deficit = self.target_pool_size - current_count - in_flight

        # 计算中位数：有缓存则复用，避免每 5 秒全量查 DB
        import statistics
        now_mono = _time.monotonic()
        if (
            not hasattr(self, "_median_cache")
            or now_mono - self._median_cache_at > 30.0
            or deficit > 0
        ):
            used_balances = await self.repo.get_used_account_balances()
            median_balance = float(statistics.median(used_balances)) if used_balances else 0.0
            self._median_cache = median_balance
            self._median_cache_at = now_mono
            self._median_used_count = len(used_balances)
        else:
            median_balance = self._median_cache
            used_balances = []  # 仅用于日志

        proxy_region = self.proxy_rotator.current_region if self.proxy_rotator.enabled else "n/a"
        logger.info(
            "PoolMonitor: check | pool={} connecting={} deficit={} used_accounts={} median_balance={} proxy_region={}",
            current_count,
            in_flight,
            deficit,
            len(used_balances) or getattr(self, "_median_used_count", 0),
            int(median_balance),
            proxy_region,
        )

        if deficit <= 0:
            return

        enqueued = 0
        # 最多扫 target_pool_size * 3 个账号，避免无限扫
        max_scan = max(deficit, self.target_pool_size) * 3
        scanned = 0
        batch_size = max(deficit * 2, 50)

        while enqueued < deficit and scanned < max_scan:
            docs = await self.repo.fetch_accounts_sequential(
                after_id=self._last_scanned_id,
                limit=batch_size,
            )

            if not docs:
                # 扫完一轮，重置游标从头再来
                if self._last_scanned_id is not None:
                    self._last_scanned_id = None
                    logger.debug("PoolMonitor: full scan done, restarting from head")
                    continue
                else:
                    # DB 里根本没有 active 账号
                    break

            for doc in docs:
                scanned += 1
                self._last_scanned_id = doc["_id"]

                if enqueued >= deficit:
                    break

                account_id = str(doc["_id"])

                # 跳过已在 pool 或正在建连的
                if self.pool.has_client(account_id):
                    continue
                async with self._connecting_lock:
                    if account_id in self._connecting:
                        continue

                # 检查 cooldown
                cooldown_until = doc.get("cooldown_until")
                now_utc = utc_now()
                if (
                    cooldown_until
                    and isinstance(cooldown_until, datetime)
                    and cooldown_until > now_utc
                ):
                    continue

                # 中位数过滤
                ever_connected = bool(doc.get("ever_connected", False))
                balance = int(doc.get("message_point_balance", 0) or 0)
                if ever_connected and balance < median_balance:
                    continue

                # 满足条件，启动建连任务
                async with self._connecting_lock:
                    self._connecting.add(account_id)
                asyncio.create_task(
                    self._connect_one(account_id),
                    name=f"pool-connect-{account_id}",
                )
                enqueued += 1

            if not docs or len(docs) < batch_size:
                # 这批没取满，说明已到末尾，重置游标
                self._last_scanned_id = None
                break

        # 持久化游标到 DB
        await self.repo.set_cursor(self._cursor_key, self._last_scanned_id)

        if enqueued > 0:
            logger.info(
                "PoolMonitor: queued {} connect task(s); pool={}/{} median_balance={}",
                enqueued,
                current_count,
                self.target_pool_size,
                int(median_balance),
            )

    async def _connect_one(self, account_id: str) -> None:
        async with self._connect_semaphore:
            try:
                account_doc = await self.repo.get_account_by_id(account_id)
                if not account_doc or str(account_doc.get("status", "")) != "active":
                    return
                if self.pool.has_client(account_id):
                    return

                creds = await self.repo.get_account_credentials(account_doc)
                try:
                    client = await asyncio.wait_for(
                        self.pool._create_client_with_fallback(account_id, creds),
                        timeout=self.connect_timeout_seconds,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "PoolMonitor: connect timeout for account {} after {}s",
                        mask_secret(account_id),
                        self.connect_timeout_seconds,
                    )
                    await self.repo.record_account_error(
                        account_id,
                        "pool_monitor_connect_timeout",
                        cooldown_seconds=120,
                    )
                    self.proxy_rotator.record_failure()
                    self._record_connect_error()
                    return

                self.pool._register_reconnect_callback(account_id, client)
                self.pool._clients[account_id] = client
                self.pool._client_created_at[account_id] = _time.time()
                await self.repo.mark_account_ever_connected(account_id)
                self.proxy_rotator.record_success()
                logger.info(
                    "PoolMonitor: connected account {} | pool_size={}",
                    mask_secret(account_id),
                    len(self.pool.cached_account_ids()),
                )
            except Exception as exc:
                logger.warning(
                    "PoolMonitor: connect failed for account {}: {}",
                    mask_secret(account_id),
                    exc,
                )
                await self.repo.record_account_error(
                    account_id,
                    f"pool_monitor_connect_error: {exc}",
                    cooldown_seconds=120,
                )
                self.proxy_rotator.record_failure()
                self._record_connect_error()
            finally:
                async with self._connecting_lock:
                    self._connecting.discard(account_id)
