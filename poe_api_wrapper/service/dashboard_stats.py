from __future__ import annotations

from typing import Any


POINT_BALANCE_DISTRIBUTION_BUCKETS = (
    {"label": "0 及以下", "min_balance": None, "max_balance": 0},
    {"label": "1 - 50", "min_balance": 1, "max_balance": 50},
    {"label": "51 - 100", "min_balance": 51, "max_balance": 100},
    {"label": "101 - 150", "min_balance": 101, "max_balance": 150},
    {"label": "151 - 200", "min_balance": 151, "max_balance": 200},
    {"label": "201 - 250", "min_balance": 201, "max_balance": 250},
    {"label": "251 - 300", "min_balance": 251, "max_balance": 300},
)


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
