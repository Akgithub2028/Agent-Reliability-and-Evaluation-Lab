"""
Resilient executor: retry with exponential backoff + fallback chains + state recovery.

Implements improvement-plan item P4.1 (graceful degradation & recovery).

Key types:

  * ``RetryPolicy`` — pure-data config: max_attempts, base delay, max delay,
    jitter, retriable exception types.
  * ``ResilientExecutor`` — wraps an async callable. ``execute_with_resilience(fn, *a, **kw)``
    runs the policy:
        1. Try fn.
        2. On retriable error → sleep with exp backoff + jitter → retry.
        3. On non-retriable error → raise immediately.
        4. After max_attempts → invoke first fallback that doesn't raise.
        5. If all fallbacks raise → raise the last error.
  * ``FallbackChain`` — ordered list of (predicate, fallback_fn) pairs.
    Predicates receive (attempt, last_error, last_value) and decide whether
    this fallback is appropriate.
  * ``StateSnapshot`` + ``StateRecovery`` — checkpoint/restore API so that
    interrupted workflows can resume from the last good state instead of
    restarting from scratch. ``StateRecovery`` is an in-memory implementation;
    production deployments can subclass it to back by Redis/DB.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    base_delay_s: float = 0.1
    max_delay_s: float = 5.0
    multiplier: float = 2.0
    jitter: float = 0.1  # 0..1 — fraction of the delay to add as random jitter
    retriable_exceptions: Tuple[Type[BaseException], ...] = (Exception,)

    def delay_for(self, attempt: int) -> float:
        """attempt is 1-indexed (1 = first failure, delay before 2nd attempt)."""
        d = self.base_delay_s * (self.multiplier ** (attempt - 1))
        d = min(d, self.max_delay_s)
        if self.jitter > 0:
            d += random.uniform(0, d * self.jitter)
        return d

    def is_retriable(self, exc: BaseException) -> bool:
        return isinstance(exc, self.retriable_exceptions)


# ---------------------------------------------------------------------------
# Fallbacks
# ---------------------------------------------------------------------------


FallbackPredicate = Callable[[int, Optional[BaseException], Any], bool]
FallbackFn = Callable[..., Awaitable[Any]]


@dataclass
class FallbackEntry:
    predicate: FallbackPredicate
    fn: FallbackFn
    name: str = ""


class FallbackChain:
    """
    Ordered list of fallback functions. The first predicate that returns
    True wins, and its function is invoked with the original args.
    """

    def __init__(self, entries: Optional[List[FallbackEntry]] = None):
        self._entries: List[FallbackEntry] = list(entries or [])

    def add(
        self, fn: FallbackFn, predicate: Optional[FallbackPredicate] = None, name: str = ""
    ) -> "FallbackChain":
        if predicate is None:
            predicate = lambda attempt, exc, value: True  # noqa: E731
        self._entries.append(FallbackEntry(predicate=predicate, fn=fn, name=name or fn.__name__))
        return self

    def __len__(self) -> int:
        return len(self._entries)

    async def try_fallbacks(
        self, attempt: int, last_exc: Optional[BaseException], last_value: Any, args: tuple, kwargs: dict
    ) -> Tuple[bool, Any]:
        for entry in self._entries:
            try:
                ok = entry.predicate(attempt, last_exc, last_value)
            except Exception:
                logger.exception("fallback predicate %s raised", entry.name)
                continue
            if not ok:
                continue
            try:
                result = await entry.fn(*args, **kwargs)
                return True, result
            except Exception as exc:
                logger.warning("fallback %s raised: %s", entry.name, exc)
                last_exc = exc
                continue
        return False, last_exc


# ---------------------------------------------------------------------------
# State recovery
# ---------------------------------------------------------------------------


@dataclass
class StateSnapshot:
    workflow_id: str
    step: int
    state: Dict[str, Any]
    created_at: float = field(default_factory=time.time)


class StateRecovery:
    """
    In-memory state-recovery store. Subclass and override ``save``/``load``
    to back by Redis/DB in production.

    The store is keyed by ``workflow_id`` and holds the most recent snapshot
    per workflow.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self._store: Dict[str, StateSnapshot] = {}
        self._log = logger or logging.getLogger(__name__)

    async def save(self, snapshot: StateSnapshot) -> None:
        self._store[snapshot.workflow_id] = snapshot
        self._log.debug("Saved snapshot for workflow %s at step %d", snapshot.workflow_id, snapshot.step)

    async def load(self, workflow_id: str) -> Optional[StateSnapshot]:
        return self._store.get(workflow_id)

    async def clear(self, workflow_id: str) -> None:
        self._store.pop(workflow_id, None)

    async def list(self) -> List[str]:
        return list(self._store.keys())


# ---------------------------------------------------------------------------
# Resilient executor
# ---------------------------------------------------------------------------


@dataclass
class ExecutionStats:
    attempts: int = 0
    successes: int = 0
    failures: int = 0
    fallbacks_used: int = 0
    total_delay_s: float = 0.0
    last_error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "attempts": self.attempts,
            "successes": self.successes,
            "failures": self.failures,
            "fallbacks_used": self.fallbacks_used,
            "total_delay_s": round(self.total_delay_s, 3),
            "last_error": self.last_error,
        }


class ResilientExecutor:
    """
    Wraps an async callable with retry + fallback + state-recovery semantics.

    Args:
        policy: Retry configuration.
        fallback: Optional fallback chain used after retries are exhausted.
        state: Optional state-recovery store. When set, callers can pass
            ``workflow_id=...`` and ``step=...`` to checkpoint progress.
    """

    def __init__(
        self,
        policy: Optional[RetryPolicy] = None,
        *,
        fallback: Optional[FallbackChain] = None,
        state: Optional[StateRecovery] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.policy = policy or RetryPolicy()
        self.fallback = fallback
        self.state = state
        self._log = logger or logging.getLogger(__name__)
        self.stats = ExecutionStats()

    async def execute_with_resilience(
        self,
        fn: Callable[..., Awaitable[Any]],
        *args: Any,
        workflow_id: Optional[str] = None,
        step: Optional[int] = None,
        state: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Run ``fn`` under the retry/fallback policy.

        If ``workflow_id`` is supplied, the executor will:
          * check the state-recovery store before invoking fn — if a snapshot
            with a >= step exists, restore it as ``state`` argument
          * save a snapshot after each successful invocation
        """
        self.stats.attempts += 1
        # Restore from state if requested. We resume if the latest snapshot
        # completed the *previous* step (restored.step >= step - 1) OR the
        # current step (restored.step >= step) — the latter is a no-op re-run.
        restored: Optional[StateSnapshot] = None
        if self.state is not None and workflow_id is not None:
            restored = await self.state.load(workflow_id)
            if (
                restored is not None
                and step is not None
                and restored.step >= step - 1
            ):
                self._log.info(
                    "Resuming workflow %s at step %d (snapshot step=%d)",
                    workflow_id, step, restored.step,
                )
                state = {**(state or {}), **restored.state}

        last_exc: Optional[BaseException] = None
        last_value: Any = None
        for attempt in range(1, self.policy.max_attempts + 1):
            try:
                if state is not None:
                    result = await fn(*args, state=state, **kwargs)
                else:
                    result = await fn(*args, **kwargs)
                self.stats.successes += 1
                # Save snapshot
                if self.state is not None and workflow_id is not None and step is not None:
                    snap = StateSnapshot(
                        workflow_id=workflow_id,
                        step=step,
                        state=state or {},
                    )
                    await self.state.save(snap)
                return result
            except self.policy.retriable_exceptions as exc:
                last_exc = exc
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                if not self.policy.is_retriable(exc) or attempt >= self.policy.max_attempts:
                    break
                delay = self.policy.delay_for(attempt)
                self.stats.total_delay_s += delay
                self._log.info(
                    "Retryable failure (attempt %d/%d): %s — backing off %.2fs",
                    attempt, self.policy.max_attempts, exc, delay,
                )
                await asyncio.sleep(delay)
            except BaseException as exc:
                # Non-retriable
                last_exc = exc
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                raise

        # Retries exhausted — try fallbacks
        if self.fallback is not None and len(self.fallback) > 0:
            ok, value = await self.fallback.try_fallbacks(
                attempt=self.stats.attempts, last_exc=last_exc, last_value=last_value, args=args, kwargs=kwargs
            )
            if ok:
                self.stats.fallbacks_used += 1
                return value

        self.stats.failures += 1
        assert last_exc is not None
        raise last_exc


def new_workflow_id() -> str:
    """Convenience helper to mint a workflow_id for state-recovery."""
    return uuid.uuid4().hex


__all__ = [
    "RetryPolicy",
    "FallbackPredicate",
    "FallbackFn",
    "FallbackEntry",
    "FallbackChain",
    "StateSnapshot",
    "StateRecovery",
    "ExecutionStats",
    "ResilientExecutor",
    "new_workflow_id",
]
