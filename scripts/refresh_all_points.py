"""Refresh account points via gateway admin API.

Usage:
  python scripts/refresh_all_points.py
  python scripts/refresh_all_points.py --base-url http://127.0.0.1:8000
  python scripts/refresh_all_points.py --statuses active depleted cooldown
  python scripts/refresh_all_points.py --concurrency 20
  python scripts/refresh_all_points.py --env-file .env.gateway
  python scripts/refresh_all_points.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

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


def _bootstrap_env(explicit_path: str | None = None) -> None:
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
    parser = argparse.ArgumentParser(description="Refresh account points through gateway admin API.")
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
        "--statuses",
        nargs="+",
        default=["depleted", "cooldown", "invalid"],
        metavar="STATUS",
        help="Account statuses to refresh (default: depleted cooldown invalid).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        metavar="N",
        help="Concurrent refresh workers (default: 10, max: 50).",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        metavar="PATH",
        help="Optional explicit .env.gateway path.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout seconds (default: %(default)s).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print request payload, do not call gateway API.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print raw JSON response.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    _bootstrap_env(args.env_file)

    admin_api_key = (args.admin_api_key or os.getenv("ADMIN_API_KEY", "")).strip()
    if not admin_api_key:
        print("[error] missing admin api key. use --admin-api-key or set ADMIN_API_KEY", file=sys.stderr)
        raise SystemExit(1)

    concurrency = max(1, min(int(args.concurrency), 50))
    base_url = args.base_url.rstrip("/")
    url = f"{base_url}/admin/accounts/refresh-all"
    payload = {
        "statuses": [s.strip() for s in args.statuses if s.strip()],
        "concurrency": concurrency,
    }

    print(f"[config] base_url={base_url}")
    print(f"[config] statuses={payload['statuses']}")
    print(f"[config] concurrency={concurrency}")
    print(f"[request] POST {url}")

    if args.dry_run:
        print("[dry-run] request body:")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    try:
        timeout = httpx.Timeout(args.timeout, connect=min(5.0, args.timeout))
        with httpx.Client(timeout=timeout, trust_env=False) as client:
            resp = client.post(
                url,
                headers={"Authorization": f"Bearer {admin_api_key}"},
                json=payload,
            )
    except Exception as exc:
        print(f"[error] request failed: {exc}", file=sys.stderr)
        raise SystemExit(1)

    if resp.status_code >= 400:
        body = (resp.text or "")[:500].replace("\n", " ")
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

    print(
        "[done] "
        f"total={int(data.get('total', 0) or 0)} "
        f"succeeded={int(data.get('succeeded', 0) or 0)} "
        f"failed={int(data.get('failed', 0) or 0)}"
    )
    errors = data.get("errors")
    if isinstance(errors, list) and errors:
        show = min(len(errors), 20)
        print(f"[errors] showing first {show}:")
        for item in errors[:show]:
            print(f"  - {item}")


if __name__ == "__main__":
    main()
