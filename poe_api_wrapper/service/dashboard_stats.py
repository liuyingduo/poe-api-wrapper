from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo


POINT_BALANCE_DISTRIBUTION_BUCKETS = (
    {"label": "0 及以下", "min_balance": None, "max_balance": 0},
    {"label": "1 - 50", "min_balance": 1, "max_balance": 50},
    {"label": "51 - 100", "min_balance": 51, "max_balance": 100},
    {"label": "101 - 150", "min_balance": 101, "max_balance": 150},
    {"label": "151 - 200", "min_balance": 151, "max_balance": 200},
    {"label": "201 - 250", "min_balance": 201, "max_balance": 250},
    {"label": "251 - 300", "min_balance": 251, "max_balance": 300},
)
POINT_BALANCE_HISTORY_COLLECTION = "point_balance_daily_snapshots"
POINT_BALANCE_HISTORY_TYPE = "before_refresh_all"
POINT_BALANCE_HISTORY_DAYS = (7, 30)
ACCOUNT_STATUSES = ("active", "depleted", "cooldown", "invalid")


def build_point_balance_distribution(accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    distribution = [
        {
            "label": bucket["label"],
            "min_balance": bucket["min_balance"],
            "max_balance": bucket["max_balance"],
            "account_count": 0,
            "total_points": 0,
        }
        for bucket in POINT_BALANCE_DISTRIBUTION_BUCKETS
    ]

    for account in accounts:
        balance = int(account.get("message_point_balance", 0) or 0)
        for bucket in distribution:
            min_balance = bucket["min_balance"]
            max_balance = bucket["max_balance"]
            meets_min = min_balance is None or balance >= min_balance
            meets_max = max_balance is None or balance <= max_balance
            if meets_min and meets_max:
                bucket["account_count"] += 1
                bucket["total_points"] += balance
                break

    return distribution


def normalize_point_balance_history_days(days: int) -> int:
    return 30 if int(days) >= 30 else 7


def point_balance_history_start_date(now: datetime, days: int, timezone_name: str) -> date:
    local_today = now.astimezone(ZoneInfo(timezone_name)).date()
    return local_today - timedelta(days=normalize_point_balance_history_days(days) - 1)


def build_pre_refresh_point_balance_snapshot(
    accounts: list[dict[str, Any]],
    *,
    captured_at: datetime,
    timezone_name: str,
) -> dict[str, Any]:
    local_date = captured_at.astimezone(ZoneInfo(timezone_name)).date()
    status_counts = {status: 0 for status in ACCOUNT_STATUSES}
    total_points = 0
    active_points = 0

    for account in accounts:
        status = str(account.get("status", "active") or "active")
        if status not in status_counts:
            status = "active"
        balance = int(account.get("message_point_balance", 0) or 0)
        status_counts[status] += 1
        total_points += balance
        if status == "active":
            active_points += balance

    return {
        "type": POINT_BALANCE_HISTORY_TYPE,
        "date": local_date.isoformat(),
        "captured_at": captured_at,
        "timezone": timezone_name,
        "account_count": len(accounts),
        "total_points": total_points,
        "active_points": active_points,
        "status_counts": status_counts,
    }


def serialize_point_balance_history(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    history = []
    for item in items:
        captured_at = item.get("captured_at")
        history.append(
            {
                "date": item.get("date"),
                "captured_at": captured_at.isoformat() if isinstance(captured_at, datetime) else captured_at,
                "timezone": item.get("timezone"),
                "account_count": int(item.get("account_count", 0) or 0),
                "total_points": int(item.get("total_points", 0) or 0),
                "active_points": int(item.get("active_points", 0) or 0),
                "status_counts": item.get("status_counts") or {},
            }
        )
    return history
