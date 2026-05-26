from datetime import datetime, timezone

import pytest

from poe_api_wrapper.service.dashboard_stats import (
    build_point_balance_distribution,
    build_pre_daily_reset_point_balance_snapshot,
    normalize_point_balance_history_days,
    point_balance_history_start_date,
    serialize_point_balance_history,
)
from poe_api_wrapper.service.gateway import AccountHealthRefresher


def test_build_point_balance_distribution_counts_accounts_and_points():
    accounts = [
        {"message_point_balance": -1},
        {"message_point_balance": 0},
        {"message_point_balance": 1},
        {"message_point_balance": 50},
        {"message_point_balance": 51},
        {"message_point_balance": 100},
        {"message_point_balance": 101},
        {"message_point_balance": 150},
        {"message_point_balance": 151},
        {"message_point_balance": 200},
        {"message_point_balance": 201},
        {"message_point_balance": 250},
        {"message_point_balance": 251},
        {"message_point_balance": 300},
    ]

    distribution = build_point_balance_distribution(accounts)

    assert distribution == [
        {"label": "0 及以下", "min_balance": None, "max_balance": 0, "account_count": 2, "total_points": -1},
        {"label": "1 - 50", "min_balance": 1, "max_balance": 50, "account_count": 2, "total_points": 51},
        {"label": "51 - 100", "min_balance": 51, "max_balance": 100, "account_count": 2, "total_points": 151},
        {"label": "101 - 150", "min_balance": 101, "max_balance": 150, "account_count": 2, "total_points": 251},
        {"label": "151 - 200", "min_balance": 151, "max_balance": 200, "account_count": 2, "total_points": 351},
        {"label": "201 - 250", "min_balance": 201, "max_balance": 250, "account_count": 2, "total_points": 451},
        {"label": "251 - 300", "min_balance": 251, "max_balance": 300, "account_count": 2, "total_points": 551},
    ]


def test_build_pre_daily_reset_point_balance_snapshot_sums_account_points():
    captured_at = datetime(2026, 5, 25, 1, 30, tzinfo=timezone.utc)
    accounts = [
        {"status": "active", "message_point_balance": 120},
        {"status": "depleted", "message_point_balance": 10},
        {"status": "cooldown", "message_point_balance": 50},
        {"status": "invalid", "message_point_balance": 30},
        {"status": "unknown", "message_point_balance": 70},
    ]

    snapshot = build_pre_daily_reset_point_balance_snapshot(
        accounts,
        captured_at=captured_at,
        timezone_name="Asia/Hong_Kong",
    )

    assert snapshot == {
        "type": "before_daily_reset",
        "date": "2026-05-25",
        "captured_at": captured_at,
        "timezone": "Asia/Hong_Kong",
        "account_count": 5,
        "total_points": 280,
        "active_points": 190,
        "status_counts": {
            "active": 2,
            "depleted": 1,
            "cooldown": 1,
            "invalid": 1,
        },
    }


def test_point_balance_history_start_date_uses_local_day_window():
    now = datetime(2026, 5, 25, 1, 0, tzinfo=timezone.utc)

    assert normalize_point_balance_history_days(7) == 7
    assert normalize_point_balance_history_days(30) == 30
    assert normalize_point_balance_history_days(14) == 7
    assert point_balance_history_start_date(now, 7, "Asia/Hong_Kong").isoformat() == "2026-05-19"
    assert point_balance_history_start_date(now, 30, "Asia/Hong_Kong").isoformat() == "2026-04-26"


def test_serialize_point_balance_history_converts_datetime():
    captured_at = datetime(2026, 5, 25, 1, 30, tzinfo=timezone.utc)
    history = serialize_point_balance_history(
        [
            {
                "date": "2026-05-25",
                "captured_at": captured_at,
                "timezone": "Asia/Hong_Kong",
                "account_count": 2,
                "total_points": 300,
                "active_points": 240,
                "status_counts": {"active": 2},
            }
        ]
    )

    assert history == [
        {
            "date": "2026-05-25",
            "captured_at": "2026-05-25T01:30:00+00:00",
            "timezone": "Asia/Hong_Kong",
            "account_count": 2,
            "total_points": 300,
            "active_points": 240,
            "status_counts": {"active": 2},
        }
    ]


@pytest.mark.asyncio
async def test_daily_reset_records_snapshot_before_reset():
    captured_at = datetime(2026, 5, 25, 0, 0, tzinfo=timezone.utc)
    calls = []

    class FakeRepo:
        async def record_pre_daily_reset_point_balance_snapshot(self, *, captured_at, timezone_name):
            calls.append(("snapshot", captured_at, timezone_name))

        async def daily_reset_point_balance(self, *, point_balance, reset_statuses):
            calls.append(("reset", point_balance, reset_statuses))
            return 2

    refresher = AccountHealthRefresher(
        FakeRepo(),
        default_poe_revision="",
        daily_reset_timezone="Asia/Hong_Kong",
        daily_reset_hour=8,
        daily_reset_point_balance=300,
    )
    refresher._next_daily_reset_utc = captured_at

    await refresher._run_daily_point_reset_if_due(captured_at)

    assert calls == [
        ("snapshot", captured_at, "Asia/Hong_Kong"),
        ("reset", 300, ["active", "depleted", "cooldown"]),
    ]
