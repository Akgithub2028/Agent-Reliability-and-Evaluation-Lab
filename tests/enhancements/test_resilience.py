"""Tests for ResilientExecutor + RetryPolicy + FallbackChain + StateRecovery (P4.1)."""
from __future__ import annotations

import asyncio
import pytest

from mcp_agent.enhancements.resilience import (
    FallbackChain,
    FallbackEntry,
    ResilientExecutor,
    RetryPolicy,
    StateRecovery,
    StateSnapshot,
    new_workflow_id,
)


# ---------------------------------------------------------------------------
# RetryPolicy
# ---------------------------------------------------------------------------


def test_retry_policy_delay_for_grows_exponentially() -> None:
    p = RetryPolicy(base_delay_s=0.1, multiplier=2.0, max_delay_s=10.0, jitter=0.0)
    assert p.delay_for(1) == 0.1
    assert p.delay_for(2) == 0.2
    assert p.delay_for(3) == 0.4
    assert p.delay_for(4) == 0.8


def test_retry_policy_capped_at_max() -> None:
    p = RetryPolicy(base_delay_s=1.0, multiplier=10.0, max_delay_s=5.0, jitter=0.0)
    # attempt=2 → 1.0 * 10^1 = 10.0 → capped to 5.0
    assert p.delay_for(2) == 5.0
    # attempt=3 → 1.0 * 10^2 = 100.0 → also capped to 5.0
    assert p.delay_for(3) == 5.0
    # attempt=1 → 1.0 * 10^0 = 1.0 (below cap)
    assert p.delay_for(1) == 1.0


def test_retry_policy_jitter_adds_randomness() -> None:
    p = RetryPolicy(base_delay_s=1.0, multiplier=1.0, max_delay_s=100.0, jitter=0.5)
    delays = [p.delay_for(1) for _ in range(20)]
    # With jitter, at least some should differ
    assert len(set(delays)) > 1


def test_retry_policy_is_retriable_uses_isinstance() -> None:
    p = RetryPolicy(retriable_exceptions=(ConnectionError, TimeoutError))
    assert p.is_retriable(ConnectionError("x"))
    assert p.is_retriable(TimeoutError())
    assert not p.is_retriable(ValueError("nope"))


# ---------------------------------------------------------------------------
# ResilientExecutor — retry behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_executor_succeeds_first_try() -> None:
    ex = ResilientExecutor(RetryPolicy(max_attempts=3, base_delay_s=0.01))
    calls = [0]

    async def fn():
        calls[0] += 1
        return "ok"

    result = await ex.execute_with_resilience(fn)
    assert result == "ok"
    assert calls[0] == 1
    assert ex.stats.attempts == 1
    assert ex.stats.successes == 1


@pytest.mark.asyncio
async def test_executor_retries_on_retriable_failure() -> None:
    ex = ResilientExecutor(RetryPolicy(max_attempts=3, base_delay_s=0.01, jitter=0.0))
    calls = [0]

    async def fn():
        calls[0] += 1
        if calls[0] < 3:
            raise ConnectionError("transient")
        return "ok"

    result = await ex.execute_with_resilience(fn)
    assert result == "ok"
    assert calls[0] == 3


@pytest.mark.asyncio
async def test_executor_raises_after_max_attempts() -> None:
    ex = ResilientExecutor(RetryPolicy(max_attempts=2, base_delay_s=0.01, jitter=0.0))
    calls = [0]

    async def fn():
        calls[0] += 1
        raise ConnectionError("permanent-ish")

    with pytest.raises(ConnectionError):
        await ex.execute_with_resilience(fn)
    assert calls[0] == 2
    assert ex.stats.failures == 1


@pytest.mark.asyncio
async def test_executor_raises_immediately_on_non_retriable() -> None:
    ex = ResilientExecutor(
        RetryPolicy(max_attempts=5, retriable_exceptions=(ConnectionError,), jitter=0.0)
    )
    calls = [0]

    async def fn():
        calls[0] += 1
        raise ValueError("non-retriable")

    with pytest.raises(ValueError):
        await ex.execute_with_resilience(fn)
    assert calls[0] == 1  # no retries


# ---------------------------------------------------------------------------
# FallbackChain
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_executor_invokes_fallback_after_retries_exhausted() -> None:
    chain = FallbackChain()
    chain.add(fn=lambda: _fallback_returns("fallback"), name="fb1")

    ex = ResilientExecutor(
        RetryPolicy(max_attempts=2, base_delay_s=0.01, jitter=0.0),
        fallback=chain,
    )
    calls = [0]

    async def fn():
        calls[0] += 1
        raise ConnectionError("nope")

    result = await ex.execute_with_resilience(fn)
    assert result == "fallback"
    assert ex.stats.fallbacks_used == 1


@pytest.mark.asyncio
async def test_executor_skips_fallback_when_predicate_returns_false() -> None:
    chain = FallbackChain().add(
        fn=lambda: _fallback_returns("fb"),
        predicate=lambda attempt, exc, val: False,
        name="guarded",
    )
    ex = ResilientExecutor(
        RetryPolicy(max_attempts=1, base_delay_s=0.01, jitter=0.0),
        fallback=chain,
    )

    async def fn():
        raise ConnectionError("nope")

    with pytest.raises(ConnectionError):
        await ex.execute_with_resilience(fn)
    assert ex.stats.fallbacks_used == 0


@pytest.mark.asyncio
async def test_fallback_chain_tries_next_if_first_raises() -> None:
    chain = FallbackChain()
    chain.add(fn=lambda: _raise(ConnectionError("fb1 fails")), name="fb1")
    chain.add(fn=lambda: _fallback_returns("fb2"), name="fb2")
    ex = ResilientExecutor(
        RetryPolicy(max_attempts=1, base_delay_s=0.01, jitter=0.0),
        fallback=chain,
    )

    async def fn():
        raise ConnectionError("main fails")

    result = await ex.execute_with_resilience(fn)
    assert result == "fb2"


# helpers --------------------------------------------------------------------


async def _fallback_returns(value):
    return value


async def _raise(exc):
    raise exc


# ---------------------------------------------------------------------------
# State recovery
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_executor_restores_state_from_snapshot() -> None:
    state_store = StateRecovery()
    wid = new_workflow_id()

    # Pre-populate a snapshot at step 2
    await state_store.save(
        StateSnapshot(workflow_id=wid, step=2, state={"done": ["step1", "step2"]})
    )

    seen_state = []

    async def fn(*, state=None):
        seen_state.append(state)
        return "done"

    ex = ResilientExecutor(state=state_store)
    await ex.execute_with_resilience(fn, workflow_id=wid, step=3)
    # Restored state should have been merged in
    assert seen_state and seen_state[0] == {"done": ["step1", "step2"]}


@pytest.mark.asyncio
async def test_executor_saves_snapshot_after_success() -> None:
    state_store = StateRecovery()
    wid = new_workflow_id()

    async def fn(*, state=None):
        return "ok"

    ex = ResilientExecutor(state=state_store)
    await ex.execute_with_resilience(fn, workflow_id=wid, step=1, state={"a": 1})
    snap = await state_store.load(wid)
    assert snap is not None
    assert snap.step == 1
    assert snap.state == {"a": 1}


@pytest.mark.asyncio
async def test_state_recovery_clear_and_list() -> None:
    s = StateRecovery()
    wid = new_workflow_id()
    await s.save(StateSnapshot(workflow_id=wid, step=1, state={}))
    assert wid in await s.list()
    await s.clear(wid)
    assert wid not in await s.list()


@pytest.mark.asyncio
async def test_executor_stats_to_dict_after_run() -> None:
    ex = ResilientExecutor(RetryPolicy(max_attempts=2, base_delay_s=0.01, jitter=0.0))

    async def fn():
        raise ConnectionError("nope")

    with pytest.raises(ConnectionError):
        await ex.execute_with_resilience(fn)

    d = ex.stats.to_dict()
    assert d["attempts"] == 1
    assert d["failures"] == 1
    assert "last_error" in d and d["last_error"]
