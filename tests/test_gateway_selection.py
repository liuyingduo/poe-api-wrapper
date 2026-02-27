import pytest

from poe_api_wrapper.openai.gateway import (
    AccountSelector,
    CapacityLimitError,
    NoAccountAvailableError,
    RuntimeLimiter,
)


class FakeRepo:
    def __init__(self, accounts):
        self.accounts = accounts

    async def list_candidate_accounts(self, limit=1000):
        return self.accounts[:limit]


@pytest.mark.asyncio
async def test_select_account_uses_top_100_and_median_pool():
    accounts = []
    for i in range(150):
        accounts.append(
            {
                "_id": f"acc-{i}",
                "message_point_balance": 1000 - i,
                "health_score": 100.0 - (i / 10),
                "error_count": 0,
            }
        )

    selector = AccountSelector(
        repo=FakeRepo(accounts),
        limiter=RuntimeLimiter(max_inflight_per_account=2, global_inflight_limit=200),
        top_n=100,
    )
    top_accounts, primary_pool, median_balance = await selector._top_and_primary_pool()
    assert len(top_accounts) == 100
    assert len(primary_pool) > 0
    assert all(a["message_point_balance"] >= median_balance for a in primary_pool)


@pytest.mark.asyncio
async def test_select_account_raises_if_no_candidates():
    selector = AccountSelector(
        repo=FakeRepo([]),
        limiter=RuntimeLimiter(max_inflight_per_account=2, global_inflight_limit=200),
        top_n=100,
    )
    with pytest.raises(NoAccountAvailableError):
        await selector.select_account()


@pytest.mark.asyncio
async def test_select_account_capacity_limit():
    account = {"_id": "acc-1", "message_point_balance": 100, "health_score": 100.0, "error_count": 0}
    limiter = RuntimeLimiter(max_inflight_per_account=1, global_inflight_limit=1)
    selector = AccountSelector(repo=FakeRepo([account]), limiter=limiter, top_n=100)

    selected, lease = await selector.select_account()
    assert selected["_id"] == "acc-1"
    with pytest.raises(CapacityLimitError):
        await selector.select_account()
    await lease.release()

