"""Check /admin/accounts/summary and report whether available is near zero.

Usage:
  python scripts/check_accounts_summary.py
  python scripts/check_accounts_summary.py --base-url http://127.0.0.1:8003
  python scripts/check_accounts_summary.py --near-zero-threshold 3
  python scripts/check_accounts_summary.py --env-file /path/to/.env.gateway
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Optional

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


def _bootstrap_env(explicit_path: Optional[str]) -> None:
    if explicit_path:
        p = Path(explicit_path)
        if _load_dotenv(p):
            print(f"[env] loaded: {p}")
            return
        print(f"[warn] env file not found: {p}", file=sys.stderr)

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    default_env = project_root / ".env.gateway"
    if _load_dotenv(default_env):
        print(f"[env] loaded: {default_env}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Call /admin/accounts/summary and report account availability."
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("GATEWAY_BASE_URL", "http://207.180.218.216:8003"),
        help="Gateway base URL, e.g. http://127.0.0.1:8003",
    )
    parser.add_argument(
        "--admin-key",
        default="",
        help="Admin API key. If omitted, uses ADMIN_API_KEY from env/.env.gateway.",
    )
    parser.add_argument(
        "--near-zero-threshold",
        type=int,
        default=3,
        help="Treat available <= N as near zero (default: 3).",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=10,
        help="How many unavailable account rows to print (default: 10).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout seconds (default: 20).",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional explicit env file path.",
    )
    return parser.parse_args()


def _pick_admin_key(cli_key: str) -> str:
    if cli_key.strip():
        return cli_key.strip()
    return os.getenv("ADMIN_API_KEY", "").strip()


def main() -> int:
    args = _parse_args()
    _bootstrap_env(args.env_file)

    admin_key = _pick_admin_key(args.admin_key)
    if not admin_key:
        print("[error] missing ADMIN_API_KEY (or pass --admin-key).", file=sys.stderr)
        return 1

    base_url = args.base_url.rstrip("/")
    url = f"{base_url}/admin/accounts/summary"

    headers = {"Authorization": f"Bearer {admin_key}"}
    try:
        with httpx.Client(timeout=httpx.Timeout(args.timeout, connect=min(5.0, args.timeout))) as client:
            resp = client.get(url, headers=headers)
    except Exception as exc:
        print(f"[error] request failed: {exc}", file=sys.stderr)
        return 1

    if resp.status_code != 200:
        preview = (resp.text or "")[:300].replace("\n", " ")
        print(f"[error] http={resp.status_code} body={preview!r}", file=sys.stderr)
        return 1

    try:
        payload: dict[str, Any] = resp.json()
    except Exception as exc:
        print(f"[error] invalid JSON response: {exc}", file=sys.stderr)
        return 1

    total = int(payload.get("total", 0) or 0)
    available = int(payload.get("available", 0) or 0)
    unavailable = int(payload.get("unavailable", max(total - available, 0)) or 0)
    ratio = (available / total * 100.0) if total > 0 else 0.0

    print("===== /admin/accounts/summary =====")
    print(f"url: {url}")
    print(f"total={total} available={available} unavailable={unavailable} available_ratio={ratio:.1f}%")

    near_zero = available <= max(0, int(args.near_zero_threshold))
    if near_zero:
        print(f"[warn] available is near zero (<= {args.near_zero_threshold})")
    else:
        print(f"[ok] available is above threshold ({args.near_zero_threshold})")

    accounts = payload.get("accounts")
    if isinstance(accounts, list):
        bad_accounts = [a for a in accounts if not bool(a.get("available", False))]
        if bad_accounts:
            print(f"\nUnavailable accounts (showing {min(len(bad_accounts), args.show)}):")
            for item in bad_accounts[: max(0, int(args.show))]:
                email = str(item.get("email", ""))
                status = str(item.get("status", ""))
                bal = int(item.get("message_point_balance", 0) or 0)
                cooldown = item.get("cooldown_until")
                print(f"- email={email} status={status} balance={bal} cooldown_until={cooldown}")

    return 2 if near_zero else 0


if __name__ == "__main__":
    raise SystemExit(main())

