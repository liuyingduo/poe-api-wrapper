from poe_api_wrapper.service.dashboard_stats import build_point_balance_distribution


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
