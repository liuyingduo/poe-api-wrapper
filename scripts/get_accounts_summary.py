"""Fetch /admin/accounts/summary and print account balances.

Usage:
  python scripts/get_accounts_summary.py
  python scripts/get_accounts_summary.py --base-url http://127.0.0.1:8000
  python scripts/get_accounts_summary.py --available-only
  python scripts/get_accounts_summary.py --raw
  python scripts/get_accounts_summary.py --env-file .env.gateway
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import httpx


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


def _bootstrap_env(explicit_path: str | None) -> None:
    if explicit_path:
        explicit = Path(explicit_path)
        if _load_dotenv(explicit):
            print(f"[env] loaded: {explicit}")
            return
        print(f"[warn] env file not found: {explicit}", file=sys.stderr)

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    default_env = project_root / ".env.gateway"
    if _load_dotenv(default_env):
        print(f"[env] loaded: {default_env}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch admin/accounts/summary from gateway.")
    parser.add_argument(
        "--base-url",
        default=os.getenv("GATEWAY_BASE_URL", "http://207.180.218.216:8003"),
        help="Gateway base URL (default: %(default)s or GATEWAY_BASE_URL).",
    )
    parser.add_argument(
        "--admin-api-key",
        default=None,
        help="Admin API key. If omitted, read from ADMIN_API_KEY env.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional explicit .env.gateway path.",
    )
    parser.add_argument(
        "--available-only",
        action="store_true",
        help="Only show available accounts.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print raw JSON response.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout seconds (default: %(default)s).",
    )
    return parser.parse_args()


def _format_bool(value: Any) -> str:
    return "yes" if bool(value) else "no"


def _print_table(rows: list[dict[str, Any]]) -> None:
    headers = ["email", "status", "available", "points", "subscription", "cooldown_until"]
    table_rows: list[list[str]] = []
    for item in rows:
        table_rows.append(
            [
                str(item.get("email", "")),
                str(item.get("status", "")),
                _format_bool(item.get("available", False)),
                str(int(item.get("message_point_balance", 0) or 0)),
                _format_bool(item.get("subscription_active", False)),
                str(item.get("cooldown_until") or ""),
            ]
        )

    widths = [len(h) for h in headers]
    for row in table_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    sep = "-+-".join("-" * w for w in widths)
    print(" | ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
    print(sep)
    for row in table_rows:
        print(" | ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))


def main() -> None:
    args = _parse_args()
    _bootstrap_env(args.env_file)

    admin_api_key = (args.admin_api_key or os.getenv("ADMIN_API_KEY", "")).strip()
    if not admin_api_key:
        print("[error] missing admin api key. use --admin-api-key or set ADMIN_API_KEY", file=sys.stderr)
        raise SystemExit(1)

    base_url = args.base_url.rstrip("/")
    url = f"{base_url}/admin/accounts/summary"

    try:
        with httpx.Client(timeout=args.timeout, trust_env=False) as client:
            resp = client.get(url, headers={"Authorization": f"Bearer {admin_api_key}"})
    except Exception as exc:
        print(f"[error] request failed: {exc}", file=sys.stderr)
        raise SystemExit(1)

    if resp.status_code >= 400:
        body = resp.text[:500]
        print(f"[error] http {resp.status_code}: {body}", file=sys.stderr)
        raise SystemExit(1)

    try:
        data = resp.json()
    except Exception as exc:
        print(f"[error] invalid json response: {exc}", file=sys.stderr)
        raise SystemExit(1)

    if args.raw:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return

    accounts = list(data.get("accounts", []))
    accounts.sort(key=lambda x: int(x.get("message_point_balance", 0) or 0), reverse=True)

    if args.available_only:
        accounts = [item for item in accounts if bool(item.get("available"))]

    print(
        "[summary] "
        f"total={data.get('total', 0)} "
        f"available={data.get('available', 0)} "
        f"unavailable={data.get('unavailable', 0)} "
        f"shown={len(accounts)}"
    )
    if not accounts:
        print("[info] no accounts matched")
        return

    _print_table(accounts)


if __name__ == "__main__":
    main()
