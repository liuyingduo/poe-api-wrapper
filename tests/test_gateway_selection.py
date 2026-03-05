import pytest

from poe_api_wrapper.service.gateway import (
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
async def test_select_account_respects_top_n_pool():
    accounts = [
        {"_id": "acc-1", "message_point_balance": 100, "health_score": 100.0, "error_count": 0},
        {"_id": "acc-2", "message_point_balance": 90, "health_score": 99.0, "error_count": 0},
        # Highest balance but outside top_n below; should not be selected.
        {"_id": "acc-3", "message_point_balance": 9999, "health_score": 98.0, "error_count": 0},
    ]
    selector = AccountSelector(
        repo=FakeRepo(accounts),
        limiter=RuntimeLimiter(max_inflight_per_account=2, global_inflight_limit=200),
        top_n=2,
    )
    top_accounts = await selector._top_pool()
    assert len(top_accounts) == 2
    assert all(a["_id"] in {"acc-1", "acc-2"} for a in top_accounts)

    selected, lease = await selector.select_account()
    assert selected["_id"] in {"acc-1", "acc-2"}
    await lease.release()


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
