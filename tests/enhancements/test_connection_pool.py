"""Tests for MCPConnectionPool + CircuitBreaker + QuotaManager (P2.2 + P4.1)."""
from __future__ import annotations

import asyncio
import pytest
import time

from mcp_agent.enhancements.connection import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitBreakerStats,
    CircuitState,
    MCPConnectionPool,
    Quota,
    QuotaManager,
)


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_breaker_starts_closed() -> None:
    b = CircuitBreaker("test")
    assert b.state == CircuitState.CLOSED
    assert b.stats.consecutive_failures == 0


@pytest.mark.asyncio
async def test_breaker_opens_after_threshold_failures() -> None:
    b = CircuitBreaker("test", failure_threshold=3, open_timeout_s=10.0)
    for _ in range(3):
        await b.record_failure(RuntimeError("nope"))
    assert b.state == CircuitState.OPEN
    assert b.stats.trips == 1


@pytest.mark.asyncio
async def test_breaker_before_call_raises_when_open() -> None:
    b = CircuitBreaker("test", failure_threshold=2, open_timeout_s=10.0)
    await b.record_failure(Exception())
    await b.record_failure(Exception())
    with pytest.raises(CircuitBreakerOpenError):
        await b.before_call()


@pytest.mark.asyncio
async def test_breaker_recovers_via_half_open() -> None:
    # Use a fake clock so we don't have to sleep
    t = [0.0]
    b = CircuitBreaker(
        "test",
        failure_threshold=2,
        success_threshold=2,
        open_timeout_s=1.0,
        clock=lambda: t[0],
    )
    await b.record_failure(Exception())
    await b.record_failure(Exception())
    assert b.state == CircuitState.OPEN

    # Advance time past open_timeout
    t[0] = 2.0
    await b.before_call()  # should transition to HALF_OPEN
    assert b.state == CircuitState.HALF_OPEN

    # Two consecutive successes → CLOSED
    await b.record_success()
    assert b.state == CircuitState.HALF_OPEN  # still half-open
    await b.record_success()
    assert b.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_breaker_half_open_failure_reopens_immediately() -> None:
    t = [0.0]
    b = CircuitBreaker(
        "test",
        failure_threshold=1,
        success_threshold=2,
        open_timeout_s=1.0,
        clock=lambda: t[0],
    )
    await b.record_failure(Exception())
    assert b.state == CircuitState.OPEN
    t[0] = 2.0
    await b.before_call()
    assert b.state == CircuitState.HALF_OPEN
    await b.record_failure(Exception())
    assert b.state == CircuitState.OPEN
    assert b.stats.trips == 2


@pytest.mark.asyncio
async def test_breaker_on_trip_callback_fires() -> None:
    fired = []

    async def on_trip(stats: CircuitBreakerStats) -> None:
        fired.append(stats.target)

    b = CircuitBreaker("test", failure_threshold=1, on_trip=on_trip)
    await b.record_failure(Exception())
    assert b.state == CircuitState.OPEN
    # callback is scheduled via asyncio.ensure_future; yield to event loop
    await asyncio.sleep(0.01)
    assert fired == ["test"]


# ---------------------------------------------------------------------------
# QuotaManager
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_quota_manager_enforces_concurrent_cap() -> None:
    q = QuotaManager()
    q.set_quota("agent1", Quota(max_concurrent=2))
    await q.acquire("agent1")
    await q.acquire("agent1")
    # Third acquire should block
    done = asyncio.Event()

    async def third():
        await q.acquire("agent1")
        done.set()
        q.release("agent1")

    task = asyncio.ensure_future(third())
    try:
        await asyncio.wait_for(done.wait(), timeout=0.1)
        assert False, "should have timed out"
    except asyncio.TimeoutError:
        pass

    q.release("agent1")
    await asyncio.wait_for(done.wait(), timeout=0.5)
    q.release("agent1")
    await task


@pytest.mark.asyncio
async def test_quota_manager_unknown_key_is_unlimited() -> None:
    q = QuotaManager()
    await q.acquire("unknown")
    q.release("unknown")  # safe even though we didn't set a quota


# ---------------------------------------------------------------------------
# MCPConnectionPool
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_reuses_idle_connections() -> None:
    created = []

    async def factory(target: str):
        c = {"target": target, "id": len(created)}
        created.append(c)
        return c

    pool = MCPConnectionPool(max_connections_per_target=2, factory=factory)
    c1 = await pool.acquire("server-a")
    await pool.release("server-a", c1)
    c2 = await pool.acquire("server-a")
    assert c1 is c2  # reused
    assert len(created) == 1  # only one creation
    await pool.close_all()


@pytest.mark.asyncio
async def test_pool_respects_max_per_target() -> None:
    created = []

    async def factory(target: str):
        created.append(target)
        return {"id": len(created)}

    pool = MCPConnectionPool(max_connections_per_target=2, max_total_connections=10, factory=factory)
    c1 = await pool.acquire("s")
    c2 = await pool.acquire("s")
    # Third acquire should block
    done = asyncio.Event()

    async def third():
        c3 = await pool.acquire("s")
        done.set()
        await pool.release("s", c3)

    task = asyncio.ensure_future(third())
    try:
        await asyncio.wait_for(done.wait(), timeout=0.1)
        assert False, "should have timed out"
    except asyncio.TimeoutError:
        pass
    await pool.release("s", c1)
    await asyncio.wait_for(done.wait(), timeout=0.5)
    await pool.release("s", c2)
    await task
    await pool.close_all()


@pytest.mark.asyncio
async def test_pool_context_manager_releases_on_success() -> None:
    async def factory(target: str):
        return {"id": target}

    pool = MCPConnectionPool(max_connections_per_target=2, factory=factory)
    async with pool.connection("s") as conn:
        assert conn["id"] == "s"
    # After context exit, conn should be back in the pool
    assert pool.stats()["total_live"] == 0
    assert pool.stats()["per_target"]["s"]["live"] == 1


@pytest.mark.asyncio
async def test_pool_context_manager_disposes_on_exception() -> None:
    async def factory(target: str):
        return {"id": target}

    disposed = []

    async def cleanup(conn):
        disposed.append(conn)

    pool = MCPConnectionPool(
        max_connections_per_target=2, factory=factory, cleanup=cleanup
    )
    with pytest.raises(RuntimeError):
        async with pool.connection("s") as conn:
            raise RuntimeError("boom")
    assert disposed  # cleanup was called
    assert pool.stats()["total_live"] == 0


@pytest.mark.asyncio
async def test_pool_records_failure_on_factory_exception() -> None:
    counter = [0]

    async def factory(target: str):
        counter[0] += 1
        if counter[0] <= 2:
            raise ConnectionError("cannot connect")
        return {"id": target}

    pool = MCPConnectionPool(max_connections_per_target=2, factory=factory)
    # First 2 attempts fail
    with pytest.raises(ConnectionError):
        await pool.acquire("s")
    with pytest.raises(ConnectionError):
        await pool.acquire("s")
    breaker = pool.get_breaker("s")
    assert breaker.stats.total_failures == 2


@pytest.mark.asyncio
async def test_pool_raises_circuit_open_when_breaker_open() -> None:
    async def factory(target: str):
        raise ConnectionError("nope")

    pool = MCPConnectionPool(max_connections_per_target=2, factory=factory)
    # Manually trip the breaker
    b = pool.get_breaker("s")
    for _ in range(5):
        await b.record_failure(ConnectionError())
    # Now the pool should refuse to acquire
    with pytest.raises(CircuitBreakerOpenError):
        await pool.acquire("s")


@pytest.mark.asyncio
async def test_pool_stats_reports_breaker_state() -> None:
    async def factory(target: str):
        return {"id": target}

    pool = MCPConnectionPool(max_connections_per_target=1, factory=factory)
    await pool.acquire("s")
    stats = pool.stats()
    assert stats["per_target"]["s"]["breaker"]["state"] == "closed"
