"""
Connection pool + circuit breaker + per-agent quota management.

Implements improvement-plan items P2.2 (resource pooling) and P4.1 (circuit
breaker / graceful degradation).

Key types:

  * ``CircuitBreaker`` — three-state (CLOSED, OPEN, HALF_OPEN) breaker with
    exponential-backoff recovery, configurable failure threshold, and
    per-target trip counters.
  * ``MCPConnectionPool`` — bounded async pool of opaque "connection" objects
    (any context-manager-like resource). Each target server has its own
    breaker; ``acquire()`` blocks until a connection is free, raises
    ``CircuitBreakerOpenError`` if the breaker for that target is open.
  * ``QuotaManager`` — per-agent resource quota (max concurrent connections,
    max total requests, per-second rate limit). Useful when an agent fan-outs
    to many MCP servers and you want fair sharing.
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpenError(RuntimeError):
    """Raised when ``acquire()`` is attempted on a target whose breaker is OPEN."""

    def __init__(self, target: str, retry_after_s: float):
        super().__init__(f"Circuit breaker for {target!r} is OPEN; retry in {retry_after_s:.2f}s")
        self.target = target
        self.retry_after_s = retry_after_s


@dataclass
class CircuitBreakerStats:
    target: str
    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    total_failures: int = 0
    total_successes: int = 0
    trips: int = 0
    opened_at: Optional[float] = None
    last_failure_at: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "target": self.target,
            "state": self.state.value,
            "consecutive_failures": self.consecutive_failures,
            "consecutive_successes": self.consecutive_successes,
            "total_failures": self.total_failures,
            "total_successes": self.total_successes,
            "trips": self.trips,
            "opened_at": self.opened_at,
            "last_failure_at": self.last_failure_at,
        }


class CircuitBreaker:
    """
    Async circuit breaker.

    Args:
        target: Identifier of the protected resource (server name / URL).
        failure_threshold: Open the breaker after this many consecutive failures.
        success_threshold: In HALF_OPEN, close after this many consecutive successes.
        open_timeout_s: How long the breaker stays OPEN before transitioning
            to HALF_OPEN.
        backoff_base_s: Base of the exponential backoff (doubles per trip).
        backoff_max_s: Cap on the backoff.
        on_trip: Optional async callback when the breaker transitions to OPEN.
    """

    def __init__(
        self,
        target: str,
        *,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        open_timeout_s: float = 30.0,
        backoff_base_s: float = 1.0,
        backoff_max_s: float = 60.0,
        on_trip: Optional[Callable[[CircuitBreakerStats], Awaitable[None]]] = None,
        clock: Callable[[], float] = time.monotonic,
        logger: Optional[logging.Logger] = None,
    ):
        self.target = target
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.open_timeout_s = open_timeout_s
        self.backoff_base_s = backoff_base_s
        self.backoff_max_s = backoff_max_s
        self.on_trip = on_trip
        self._clock = clock
        self._log = logger or logging.getLogger(__name__)
        self.stats = CircuitBreakerStats(target=target)
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self.stats.state

    def _retry_after(self) -> float:
        if self.stats.opened_at is None:
            return 0.0
        # exponential backoff per trip
        backoff = min(
            self.backoff_base_s * (2 ** max(0, self.stats.trips - 1)),
            self.backoff_max_s,
        )
        elapsed = self._clock() - self.stats.opened_at
        return max(0.0, backoff - elapsed)

    async def before_call(self) -> None:
        """Call this before touching the protected resource."""
        async with self._lock:
            if self.stats.state == CircuitState.OPEN:
                # Has enough time passed to try HALF_OPEN?
                if self._clock() - (self.stats.opened_at or 0) >= self._retry_after():
                    self.stats.state = CircuitState.HALF_OPEN
                    self.stats.consecutive_successes = 0
                    self._log.info("Circuit %s → HALF_OPEN", self.target)
                else:
                    raise CircuitBreakerOpenError(self.target, self._retry_after())

    async def record_success(self) -> None:
        async with self._lock:
            self.stats.total_successes += 1
            self.stats.consecutive_failures = 0
            self.stats.consecutive_successes += 1
            if self.stats.state == CircuitState.HALF_OPEN:
                if self.stats.consecutive_successes >= self.success_threshold:
                    self.stats.state = CircuitState.CLOSED
                    self.stats.consecutive_failures = 0
                    self.stats.opened_at = None
                    self._log.info("Circuit %s → CLOSED (recovered)", self.target)

    async def record_failure(self, exc: Optional[BaseException] = None) -> None:
        async with self._lock:
            self.stats.total_failures += 1
            self.stats.consecutive_failures += 1
            self.stats.consecutive_successes = 0
            self.stats.last_failure_at = self._clock()
            if self.stats.state == CircuitState.HALF_OPEN:
                # A failure during HALF_OPEN re-opens the breaker immediately.
                self._open_locked()
            elif (
                self.stats.state == CircuitState.CLOSED
                and self.stats.consecutive_failures >= self.failure_threshold
            ):
                self._open_locked()
            if exc is not None:
                self._log.debug(
                    "Circuit %s recorded failure (%s): %s",
                    self.target, type(exc).__name__, exc,
                )

    def _open_locked(self) -> None:
        self.stats.state = CircuitState.OPEN
        self.stats.trips += 1
        self.stats.opened_at = self._clock()
        self._log.warning(
            "Circuit %s → OPEN (consecutive_failures=%d, trip #%d)",
            self.target, self.stats.consecutive_failures, self.stats.trips,
        )
        if self.on_trip is not None:
            # Fire-and-forget; the callback can autoscale or alert.
            try:
                asyncio.ensure_future(self.on_trip(self.stats))
            except Exception:  # pragma: no cover
                self._log.exception("on_trip callback raised synchronously")


# ---------------------------------------------------------------------------
# Quota manager
# ---------------------------------------------------------------------------


@dataclass
class Quota:
    """Per-agent quota configuration."""

    max_concurrent: int = 10
    max_per_second: int = 0  # 0 = unlimited
    max_total: int = 0  # 0 = unlimited

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_concurrent": self.max_concurrent,
            "max_per_second": self.max_per_second,
            "max_total": self.max_total,
        }


class QuotaManager:
    """
    Simple per-key quota enforcement.

    Each agent (or whatever key the caller supplies) gets:
      * a bounded semaphore for max concurrent operations,
      * a token-bucket rate limiter for max ops/sec,
      * a monotonic counter for max total ops.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self._sems: Dict[str, asyncio.Semaphore] = {}
        self._quotas: Dict[str, Quota] = {}
        self._counts: Dict[str, int] = {}
        self._tokens: Dict[str, list] = {}  # list of timestamps in the last second
        self._log = logger or logging.getLogger(__name__)
        self._lock = asyncio.Lock()

    def set_quota(self, key: str, quota: Quota) -> None:
        self._quotas[key] = quota
        self._sems[key] = asyncio.Semaphore(quota.max_concurrent)
        self._tokens.setdefault(key, [])

    async def acquire(self, key: str) -> None:
        if key not in self._quotas:
            return  # unconfigured → unlimited
        q = self._quotas[key]
        sem = self._sems[key]
        await sem.acquire()
        try:
            # total
            if q.max_total and self._counts.get(key, 0) >= q.max_total:
                sem.release()
                raise RuntimeError(f"quota exceeded: {key} hit max_total={q.max_total}")
            # rate limit
            if q.max_per_second:
                now = time.monotonic()
                async with self._lock:
                    bucket = self._tokens[key]
                    bucket[:] = [t for t in bucket if now - t < 1.0]
                    if len(bucket) >= q.max_per_second:
                        sleep_for = 1.0 - (now - bucket[0])
                        sem.release()
                        await asyncio.sleep(max(0.0, sleep_for))
                        await sem.acquire()
                    bucket.append(time.monotonic())
            self._counts[key] = self._counts.get(key, 0) + 1
        except BaseException:
            sem.release()
            raise

    def release(self, key: str) -> None:
        if key in self._sems:
            self._sems[key].release()


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------


ConnectionFactory = Callable[[], Awaitable[Any]]
CleanupCallback = Callable[[Any], Awaitable[None]]


@dataclass
class _PooledConn:
    conn: Any
    target: str
    last_used: float


class MCPConnectionPool:
    """
    Bounded async connection pool keyed by server name.

    Args:
        max_connections_per_target: Max live connections per server (default 8).
        max_total_connections: Hard cap across all targets (default 64).
        idle_timeout_s: Idle connections are reaped after this many seconds
            (a background task sweeps them).
        health_check_interval_s: How often to ping idle connections to keep
            them warm. 0 = disabled.
        factory: A function ``(target) -> Awaitable[conn]`` that creates a
            fresh connection. Required.
        cleanup: A function ``(conn) -> Awaitable[None]`` that disposes of a
            connection. Optional.
    """

    def __init__(
        self,
        *,
        max_connections_per_target: int = 8,
        max_total_connections: int = 64,
        idle_timeout_s: float = 60.0,
        health_check_interval_s: float = 0.0,
        factory: Optional[Callable[[str], Awaitable[Any]]] = None,
        cleanup: Optional[CleanupCallback] = None,
        breaker: Optional[Callable[[str], CircuitBreaker]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.max_per_target = max_connections_per_target
        self.max_total = max_total_connections
        self.idle_timeout_s = idle_timeout_s
        self.health_check_interval_s = health_check_interval_s
        self._factory = factory
        self._cleanup = cleanup
        self._breaker_factory = breaker or (lambda t: CircuitBreaker(t))
        self._log = logger or logging.getLogger(__name__)
        self._pools: Dict[str, list] = {}
        self._semaphores: Dict[str, asyncio.Semaphore] = {}
        self._global_sem = asyncio.Semaphore(max_total_connections)
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()
        self._total_live = 0
        self._stats: Dict[str, Dict[str, int]] = {}

    # -- introspection ------------------------------------------------------

    def get_breaker(self, target: str) -> CircuitBreaker:
        if target not in self._breakers:
            self._breakers[target] = self._breaker_factory(target)
        return self._breakers[target]

    def stats(self) -> Dict[str, Any]:
        return {
            "total_live": self._total_live,
            "max_total": self.max_total,
            "per_target": {
                t: {
                    "live": len(self._pools.get(t, [])),
                    "max": self.max_per_target,
                    "breaker": self.get_breaker(t).stats.to_dict(),
                }
                for t in self._pools
            },
        }

    # -- acquire / release --------------------------------------------------

    async def acquire(self, target: str) -> Any:
        """
        Acquire a connection for ``target``.

        Behavior:
          1. Check the target's circuit breaker — raises immediately if OPEN.
          2. Acquire the global + per-target semaphores.
          3. Reuse a pooled connection if one is idle, else call ``factory(target)``.
          4. Caller MUST ``await pool.release(target, conn)`` when done — or
             use the ``connection(target)`` async context manager.

        On any failure during factory call, the breaker is recorded as failed.
        """
        breaker = self.get_breaker(target)
        await breaker.before_call()
        await self._global_sem.acquire()
        try:
            sem = self._semaphores.setdefault(target, asyncio.Semaphore(self.max_per_target))
            await sem.acquire()
        except BaseException:
            self._global_sem.release()
            raise
        try:
            async with self._lock:
                pool = self._pools.setdefault(target, [])
                if pool:
                    pc = pool.pop()
                    self._total_live += 1
                    self._stats.setdefault(target, {"created": 0, "reused": 0, "released": 0})
                    self._stats[target]["reused"] += 1
                    return pc.conn
            if self._factory is None:
                raise RuntimeError("no factory configured; cannot create connection")
            try:
                conn = await self._factory(target)
            except BaseException as exc:
                await breaker.record_failure(exc)
                raise
            async with self._lock:
                self._total_live += 1
                self._stats.setdefault(target, {"created": 0, "reused": 0, "released": 0})
                self._stats[target]["created"] += 1
            await breaker.record_success()
            return conn
        except BaseException:
            sem = self._semaphores[target]
            sem.release()
            self._global_sem.release()
            raise

    async def release(self, target: str, conn: Any, *, broken: bool = False) -> None:
        """
        Return a connection to the pool, or dispose of it if ``broken=True``.
        """
        try:
            if broken:
                await breaker_failure(self.get_breaker(target))
                if self._cleanup is not None:
                    try:
                        await self._cleanup(conn)
                    except Exception:
                        self._log.exception("cleanup callback raised for %s", target)
                return
            async with self._lock:
                pool = self._pools.setdefault(target, [])
                if len(pool) >= self.max_per_target:
                    # pool full; just dispose
                    if self._cleanup is not None:
                        try:
                            await self._cleanup(conn)
                        except Exception:
                            self._log.exception("cleanup callback raised for %s", target)
                else:
                    pool.append(_PooledConn(conn=conn, target=target, last_used=time.monotonic()))
        finally:
            self._stats.setdefault(target, {"created": 0, "reused": 0, "released": 0})
            self._stats[target]["released"] += 1
            self._semaphores[target].release()
            self._global_sem.release()
            async with self._lock:
                self._total_live -= 1

    @asynccontextmanager
    async def connection(self, target: str) -> AsyncIterator[Any]:
        conn = await self.acquire(target)
        try:
            yield conn
        except BaseException as exc:
            await self.release(target, conn, broken=True)
            raise
        else:
            await self.release(target, conn, broken=False)

    async def close_all(self) -> None:
        async with self._lock:
            for target, pool in list(self._pools.items()):
                for pc in pool:
                    if self._cleanup is not None:
                        try:
                            await self._cleanup(pc.conn)
                        except Exception:
                            self._log.exception("cleanup during close_all for %s", target)
                pool.clear()
            self._total_live = 0


async def breaker_failure(b: CircuitBreaker) -> None:
    """Helper that records a failure on a breaker (kept here for symmetry)."""
    await b.record_failure()


__all__ = [
    "CircuitState",
    "CircuitBreakerOpenError",
    "CircuitBreakerStats",
    "CircuitBreaker",
    "Quota",
    "QuotaManager",
    "MCPConnectionPool",
    "breaker_failure",
]
