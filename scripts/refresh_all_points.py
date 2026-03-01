"""Refresh all account points directly from Mongo + Poe.

Usage:
  python scripts/refresh_all_points.py
  python scripts/refresh_all_points.py --statuses active depleted cooldown
  python scripts/refresh_all_points.py --concurrency 20
  python scripts/refresh_all_points.py --env-file /path/to/.env.gateway
  python scripts/refresh_all_points.py --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Optional


def _load_dotenv(path: Path) -> bool:
    if not path.exists():
        return False

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        os.environ.setdefault(key, value)

    return True


def _bootstrap_env(explicit_path: Optional[str] = None) -> None:
    if explicit_path:
        explicit = Path(explicit_path)
        if _load_dotenv(explicit):
            print(f"[env] loaded: {explicit}")
            return
        print(f"[warn] env file not found: {explicit}", file=sys.stderr)

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    for candidate in (project_root / ".env.gateway",):
        if _load_dotenv(candidate):
            print(f"[env] loaded: {candidate}")
            break


async def run(
    statuses: list[str],
    concurrency: int,
    dry_run: bool,
    depleted_threshold: int,
) -> None:
    from poe_api_wrapper.service.gateway import (
        AccountRepository,
        CredentialCrypto,
        PoeClientPool,
        fetch_poe_revision,
        mask_secret,
        utc_now,
    )

    def _require(name: str) -> str:
        value = os.environ.get(name, "").strip()
        if not value:
            print(f"[error] missing required env var: {name}", file=sys.stderr)
            raise SystemExit(1)
        return value

    mongodb_uri = _require("MONGODB_URI")
    mongodb_db = _require("MONGODB_DB")
    fernet_key = _require("FERNET_KEY")
    poe_revision = os.environ.get("POE_REVISION", "").strip()

    print(f"[config] mongodb={mongodb_uri} db={mongodb_db}")
    print(f"[config] statuses={statuses}")
    print(f"[config] concurrency={concurrency} depleted_threshold={depleted_threshold}")
    print(f"[config] dry_run={dry_run}")
    print("-" * 60)

    if not poe_revision:
        print("[info] POE_REVISION not set, fetching from poe.com/login ...")
        poe_revision = await fetch_poe_revision() or ""
        if poe_revision:
            print(f"[info] fetched poe-revision: {poe_revision}")
        else:
            print("[warn] failed to fetch poe-revision, requests will continue without it")

    crypto = CredentialCrypto(fernet_key)
    repo = AccountRepository(mongodb_uri, mongodb_db, crypto)
    pool = PoeClientPool(repo=repo, default_poe_revision=poe_revision)

    try:
        all_docs = await repo.list_all_accounts_for_refresh(statuses=statuses, limit=5000)
        total = len(all_docs)
        print(f"[info] matched accounts={total}")

        if total == 0:
            print("[info] no accounts to refresh")
            return

        if dry_run:
            print("[dry-run] accounts that would be refreshed:")
            for doc in all_docs:
                print(f"  - {doc['_id']}")
            return

        succeeded = 0
        failed = 0
        errors: list[dict[str, str]] = []
        sem = asyncio.Semaphore(concurrency)
        lock = asyncio.Lock()

        async def _refresh_one(doc: dict) -> None:
            nonlocal succeeded, failed
            account_id = str(doc["_id"])
            async with sem:
                try:
                    account_doc = await repo.get_account_by_id(account_id)
                    if not account_doc:
                        raise RuntimeError("account not found")

                    client = await pool.get_client(account_doc)
                    settings = await client.get_settings()
                    message_info = settings.get("messagePointInfo", {})
                    subscription = settings.get("subscription", {})
                    balance = int(message_info.get("subscriptionPointBalance", 0) or 0) + int(
                        message_info.get("addonPointBalance", 0) or 0
                    )
                    subscription_active = bool(subscription.get("isActive", False))

                    await repo.update_account_health(
                        account_id,
                        balance=balance,
                        subscription_active=subscription_active,
                        depleted_threshold=depleted_threshold,
                    )
                    await repo.mark_account_success(account_id)

                    status_tag = "depleted" if balance <= depleted_threshold else "active"
                    async with lock:
                        succeeded += 1
                        done = succeeded + failed
                        print(
                            f"  [{done}/{total}] {mask_secret(account_id, 8)} "
                            f"points={balance} subscription={subscription_active} status={status_tag}"
                        )
                except Exception as exc:
                    async with lock:
                        failed += 1
                        done = succeeded + failed
                        errors.append({"account_id": account_id, "error": str(exc)})
                        print(f"  [{done}/{total}] {mask_secret(account_id, 8)} [FAILED] {str(exc)[:160]}")

        print(f"[info] refreshing accounts (concurrency={concurrency}) ...")
        start_at = utc_now()
        await asyncio.gather(*(_refresh_one(doc) for doc in all_docs))
        elapsed = (utc_now() - start_at).total_seconds()

        print("-" * 60)
        print(f"[done] elapsed={elapsed:.1f}s total={total} succeeded={succeeded} failed={failed}")
        if errors:
            print(f"[errors] showing first {min(len(errors), 20)} errors:")
            for item in errors[:20]:
                print(f"  account_id={item['account_id']} error={item['error']}")
    finally:
        await pool.close_all()
        await repo.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh Poe account points directly from MongoDB (without running FastAPI service)."
    )
    parser.add_argument(
        "--statuses",
        nargs="+",
        default=["depleted", "cooldown", "invalid"],
        metavar="STATUS",
        help="Account statuses to refresh (default: depleted cooldown invalid)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        metavar="N",
        help="Concurrent refresh workers (default: 10, max: 50)",
    )
    parser.add_argument(
        "--depleted-threshold",
        type=int,
        default=None,
        metavar="N",
        help="Balance <= threshold is considered depleted (default from DEPLETED_THRESHOLD or 20)",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        metavar="PATH",
        help="Optional explicit .env.gateway path",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list accounts, do not call Poe API",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    _bootstrap_env(args.env_file)

    concurrency = max(1, min(args.concurrency, 50))

    depleted_threshold = args.depleted_threshold
    if depleted_threshold is None:
        try:
            depleted_threshold = int(os.environ.get("DEPLETED_THRESHOLD", "20"))
        except ValueError:
            depleted_threshold = 20

    asyncio.run(
        run(
            statuses=args.statuses,
            concurrency=concurrency,
            dry_run=args.dry_run,
            depleted_threshold=depleted_threshold,
        )
    )


if __name__ == "__main__":
    main()
