"""
Health monitoring + auto-scaling hooks.

Implements improvement-plan items P4.2 (health monitoring & auto-scaling)
and supports the predictive-failure-detection variant via simple EWMA trend
analysis on response-time / error-rate metrics.

Key types:

  * ``HealthStatus`` — enum: HEALTHY / DEGRADED / UNHEALTHY / UNKNOWN.
  * ``HealthCheck`` — wraps an async ``check() -> (status, detail)`` callable,
    tracks per-component EWMA latency + error rate, and emits warnings when
    latency trends upwards or error-rate crosses a threshold.
  * ``HealthMonitor`` — runs ``HealthCheck`` objects on a schedule,
    aggregates results, and invokes registered ``on_unhealthy`` /
    ``on_recovered`` callbacks.
  * ``AutoScaler`` — subscribes to health events and decides whether to scale
    a named "pool" up (add capacity) or down (drain). Exposes a simple
    ``Decision`` enum so external callers can wire it to their own
    scaling primitives (e.g. ``MCPConnectionPool.max_per_target``).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Status enum
# ---------------------------------------------------------------------------


class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    component: str
    status: HealthStatus
    detail: str = ""
    latency_ms: float = 0.0
    checked_at: float = field(default_factory=time.time)
    metrics: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "component": self.component,
            "status": self.status.value,
            "detail": self.detail,
            "latency_ms": round(self.latency_ms, 2),
            "checked_at": self.checked_at,
            "metrics": self.metrics,
        }


# Check function signature: returns (status, detail)
CheckFn = Callable[[], Awaitable[Tuple[HealthStatus, str]]]


class HealthCheck:
    """
    A single component health check.

    Tracks EWMA latency and error-rate, and predicts future failures by
    comparing the EWMA trend against a configurable threshold.

    Args:
        component: Name of the component (server name, agent name, etc.).
        check: Async callable returning ``(HealthStatus, detail_str)``.
        check_interval_s: How often to invoke ``check``.
        latency_warn_ms: Above this, the component is considered DEGRADED even
            if the check returned HEALTHY.
        latency_unhealthy_ms: Above this, mark UNHEALTHY.
        error_rate_threshold: Fraction (0..1) of recent checks that may fail
            before the component is auto-marked DEGRADED.
        ewma_alpha: Smoothing factor for EWMA (0..1, higher = faster reaction).
    """

    def __init__(
        self,
        component: str,
        check: CheckFn,
        *,
        check_interval_s: float = 30.0,
        latency_warn_ms: float = 500.0,
        latency_unhealthy_ms: float = 2000.0,
        error_rate_threshold: float = 0.3,
        ewma_alpha: float = 0.3,
    ):
        self.component = component
        self._check = check
        self.check_interval_s = check_interval_s
        self.latency_warn_ms = latency_warn_ms
        self.latency_unhealthy_ms = latency_unhealthy_ms
        self.error_rate_threshold = error_rate_threshold
        self.ewma_alpha = ewma_alpha
        # rolling history
        self._history_len = 20
        self._history: List[HealthCheckResult] = []
        self._ewma_latency: float = 0.0
        self._ewma_error: float = 0.0
        self._has_ewma = False

    @property
    def last_result(self) -> Optional[HealthCheckResult]:
        return self._history[-1] if self._history else None

    @property
    def ewma_latency_ms(self) -> float:
        return self._ewma_latency

    @property
    def ewma_error_rate(self) -> float:
        return self._ewma_error

    async def run_once(self) -> HealthCheckResult:
        """Invoke the underlying check and update metrics."""
        t0 = time.perf_counter()
        try:
            status, detail = await self._check()
        except Exception as exc:
            status, detail = HealthStatus.UNHEALTHY, f"{type(exc).__name__}: {exc}"
        latency_ms = (time.perf_counter() - t0) * 1000.0

        # Update EWMA
        if not self._has_ewma:
            self._ewma_latency = latency_ms
            self._ewma_error = 0.0 if status == HealthStatus.HEALTHY else 1.0
            self._has_ewma = True
        else:
            a = self.ewma_alpha
            self._ewma_latency = a * latency_ms + (1 - a) * self._ewma_latency
            err_obs = 0.0 if status == HealthStatus.HEALTHY else 1.0
            self._ewma_error = a * err_obs + (1 - a) * self._ewma_error

        # Override status based on latency thresholds
        if status == HealthStatus.HEALTHY:
            if latency_ms >= self.latency_unhealthy_ms:
                status = HealthStatus.UNHEALTHY
                detail = f"latency={latency_ms:.0f}ms exceeds {self.latency_unhealthy_ms}ms"
            elif latency_ms >= self.latency_warn_ms:
                status = HealthStatus.DEGRADED
                detail = f"latency={latency_ms:.0f}ms exceeds {self.latency_warn_ms}ms"

        # Error-rate trend → predictive failure
        if (
            self._ewma_error >= self.error_rate_threshold
            and status == HealthStatus.HEALTHY
        ):
            status = HealthStatus.DEGRADED
            detail = f"EWMA error rate {self._ewma_error:.0%} above threshold"

        result = HealthCheckResult(
            component=self.component,
            status=status,
            detail=detail,
            latency_ms=latency_ms,
            metrics={
                "ewma_latency_ms": self._ewma_latency,
                "ewma_error_rate": self._ewma_error,
            },
        )
        self._history.append(result)
        if len(self._history) > self._history_len:
            self._history.pop(0)
        return result


# ---------------------------------------------------------------------------
# Health monitor
# ---------------------------------------------------------------------------


HealthCallback = Callable[[HealthCheckResult], Awaitable[None]]


class HealthMonitor:
    """
    Runs ``HealthCheck`` instances on a schedule and aggregates results.

    Callbacks:
      * ``on_unhealthy``: fired whenever a component transitions to UNHEALTHY.
      * ``on_recovered``: fired whenever a component transitions back to HEALTHY.
    """

    def __init__(
        self,
        *,
        check_interval_s: float = 30.0,
        logger: Optional[logging.Logger] = None,
    ):
        self.check_interval_s = check_interval_s
        self._checks: Dict[str, HealthCheck] = {}
        self._results: Dict[str, HealthCheckResult] = {}
        self._on_unhealthy: List[HealthCallback] = []
        self._on_recovered: List[HealthCallback] = []
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._log = logger or logging.getLogger(__name__)

    def register(self, check: HealthCheck) -> None:
        if check.component in self._checks:
            raise ValueError(f"check for {check.component!r} already registered")
        self._checks[check.component] = check

    def on_unhealthy(self, cb: HealthCallback) -> None:
        self._on_unhealthy.append(cb)

    def on_recovered(self, cb: HealthCallback) -> None:
        self._on_recovered.append(cb)

    def status(self) -> Dict[str, HealthCheckResult]:
        """Return per-component latest ``HealthCheckResult`` (typed)."""
        return dict(self._results)

    def status_dict(self) -> Dict[str, Optional[Dict[str, Any]]]:
        """Return per-component latest result as a serializable dict."""
        return {
            comp: (r.to_dict() if r else None) for comp, r in self._results.items()
        }

    async def check_once(self) -> Dict[str, HealthCheckResult]:
        """Run all checks once (useful for tests)."""
        results: Dict[str, HealthCheckResult] = {}
        for name, chk in self._checks.items():
            try:
                result = await chk.run_once()
            except Exception as exc:
                self._log.exception("health check %s raised: %s", name, exc)
                result = HealthCheckResult(
                    component=name,
                    status=HealthStatus.UNHEALTHY,
                    detail=f"{type(exc).__name__}: {exc}",
                )
            results[name] = result
            await self._maybe_fire_callbacks(name, result)
        self._results.update(results)
        return results

    async def _maybe_fire_callbacks(self, component: str, result: HealthCheckResult) -> None:
        prev = self._results.get(component)
        if (
            result.status == HealthStatus.UNHEALTHY
            and (prev is None or prev.status != HealthStatus.UNHEALTHY)
        ):
            for cb in self._on_unhealthy:
                try:
                    await cb(result)
                except Exception:
                    self._log.exception("on_unhealthy callback raised")
        elif (
            result.status == HealthStatus.HEALTHY
            and prev is not None
            and prev.status != HealthStatus.HEALTHY
        ):
            for cb in self._on_recovered:
                try:
                    await cb(result)
                except Exception:
                    self._log.exception("on_recovered callback raised")

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.check_once()
            except Exception:
                self._log.exception("health monitor loop error")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.check_interval_s)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                return


# ---------------------------------------------------------------------------
# Auto-scaler
# ---------------------------------------------------------------------------


class ScaleDecision(str, Enum):
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    HOLD = "hold"


@dataclass
class ScaleSignal:
    component: str
    decision: ScaleDecision
    reason: str
    ewma_latency_ms: float = 0.0
    ewma_error_rate: float = 0.0
    at: float = field(default_factory=time.time)


class AutoScaler:
    """
    Subscribes to a ``HealthMonitor`` and emits ``ScaleSignal`` decisions.

    Decision logic (simple but predictable):

      * UNHEALTHY → SCALE_UP (add capacity to recover)
      * DEGRADED with ewma_latency > scale_up_latency_ms → SCALE_UP
      * HEALTHY for ``cooldown_s`` seconds with low EWMA latency → SCALE_DOWN

    External callers (e.g. ``MCPConnectionPool.max_per_target``) consume
    signals via ``on_signal``.
    """

    def __init__(
        self,
        monitor: HealthMonitor,
        *,
        scale_up_latency_ms: float = 1000.0,
        scale_down_latency_ms: float = 100.0,
        cooldown_s: float = 60.0,
        logger: Optional[logging.Logger] = None,
    ):
        self.monitor = monitor
        self.scale_up_latency_ms = scale_up_latency_ms
        self.scale_down_latency_ms = scale_down_latency_ms
        self.cooldown_s = cooldown_s
        self._log = logger or logging.getLogger(__name__)
        self._signals: List[ScaleSignal] = []
        self._on_signal: List[Callable[[ScaleSignal], Awaitable[None]]] = []
        self._last_decision_at: Dict[str, float] = {}

        # Wire into the monitor
        monitor.on_unhealthy(self._on_unhealthy)
        monitor.on_recovered(self._on_recovered)

    def on_signal(self, cb: Callable[[ScaleSignal], Awaitable[None]]) -> None:
        self._on_signal.append(cb)

    @property
    def signals(self) -> List[ScaleSignal]:
        return list(self._signals)

    async def _emit(self, sig: ScaleSignal) -> None:
        self._signals.append(sig)
        self._log.info(
            "Scale decision: %s for %s (%s)", sig.decision.value, sig.component, sig.reason
        )
        for cb in self._on_signal:
            try:
                await cb(sig)
            except Exception:
                self._log.exception("on_signal callback raised")

    async def _on_unhealthy(self, result: HealthCheckResult) -> None:
        # avoid thrashing: one decision per cooldown window per component
        if time.time() - self._last_decision_at.get(result.component, 0.0) < self.cooldown_s:
            return
        self._last_decision_at[result.component] = time.time()
        await self._emit(
            ScaleSignal(
                component=result.component,
                decision=ScaleDecision.SCALE_UP,
                reason=f"unhealthy: {result.detail}",
                ewma_latency_ms=result.metrics.get("ewma_latency_ms", 0.0),
                ewma_error_rate=result.metrics.get("ewma_error_rate", 0.0),
            )
        )

    async def _on_recovered(self, result: HealthCheckResult) -> None:
        if time.time() - self._last_decision_at.get(result.component, 0.0) < self.cooldown_s:
            return
        self._last_decision_at[result.component] = time.time()
        await self._emit(
            ScaleSignal(
                component=result.component,
                decision=ScaleDecision.SCALE_DOWN,
                reason="recovered",
                ewma_latency_ms=result.metrics.get("ewma_latency_ms", 0.0),
                ewma_error_rate=result.metrics.get("ewma_error_rate", 0.0),
            )
        )


__all__ = [
    "HealthStatus",
    "HealthCheckResult",
    "HealthCheck",
    "HealthMonitor",
    "HealthCallback",
    "ScaleDecision",
    "ScaleSignal",
    "AutoScaler",
]
