"""Tests for HealthMonitor + HealthCheck + AutoScaler (P4.2)."""
from __future__ import annotations

import asyncio
import pytest

from mcp_agent.enhancements.health import (
    AutoScaler,
    HealthCheck,
    HealthCheckResult,
    HealthMonitor,
    HealthStatus,
    ScaleDecision,
    ScaleSignal,
)


# ---------------------------------------------------------------------------
# HealthCheck
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_check_healthy_path() -> None:
    async def ok():
        return HealthStatus.HEALTHY, "fine"

    chk = HealthCheck("db", ok, check_interval_s=1.0)
    result = await chk.run_once()
    assert result.status == HealthStatus.HEALTHY
    assert result.detail == "fine"
    assert result.metrics["ewma_latency_ms"] >= 0.0


@pytest.mark.asyncio
async def test_health_check_degrades_on_slow_response() -> None:
    async def slow():
        await asyncio.sleep(0.02)
        return HealthStatus.HEALTHY, "ok"

    chk = HealthCheck("db", slow, latency_warn_ms=10.0)
    result = await chk.run_once()
    assert result.status == HealthStatus.DEGRADED
    assert "latency" in result.detail


@pytest.mark.asyncio
async def test_health_check_unhealthy_on_slow_response() -> None:
    async def slow():
        await asyncio.sleep(0.05)
        return HealthStatus.HEALTHY, "ok"

    chk = HealthCheck("db", slow, latency_warn_ms=10.0, latency_unhealthy_ms=20.0)
    result = await chk.run_once()
    assert result.status == HealthStatus.UNHEALTHY


@pytest.mark.asyncio
async def test_health_check_marks_unhealthy_on_exception() -> None:
    async def boom():
        raise RuntimeError("oops")

    chk = HealthCheck("db", boom)
    result = await chk.run_once()
    assert result.status == HealthStatus.UNHEALTHY
    assert "oops" in result.detail


@pytest.mark.asyncio
async def test_health_check_ewma_error_rate_triggers_degraded() -> None:
    """If EWMA error rate >= threshold, even a fresh HEALTHY response degrades."""
    calls = [0]

    async def flaky():
        calls[0] += 1
        # First 5 calls fail; then a healthy one should still be degraded.
        if calls[0] <= 5:
            raise RuntimeError("flaky")
        return HealthStatus.HEALTHY, "ok"

    chk = HealthCheck("flaky", flaky, error_rate_threshold=0.3, ewma_alpha=0.5)
    for _ in range(5):
        await chk.run_once()
    # EWMA error rate should now be high
    assert chk.ewma_error_rate > 0
    # Now a healthy observation should still be degraded due to EWMA trend
    result = await chk.run_once()
    assert result.status == HealthStatus.DEGRADED


# ---------------------------------------------------------------------------
# HealthMonitor
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monitor_check_once_runs_all_checks() -> None:
    async def ok():
        return HealthStatus.HEALTHY, "ok"

    mon = HealthMonitor(check_interval_s=1.0)
    mon.register(HealthCheck("a", ok))
    mon.register(HealthCheck("b", ok))
    results = await mon.check_once()
    assert set(results.keys()) == {"a", "b"}
    assert all(r.status == HealthStatus.HEALTHY for r in results.values())


@pytest.mark.asyncio
async def test_monitor_fires_on_unhealthy_callback() -> None:
    fired = []

    async def unhealthy():
        raise RuntimeError("down")

    mon = HealthMonitor(check_interval_s=1.0)
    mon.register(HealthCheck("down", unhealthy))
    mon.on_unhealthy(lambda r: _append(fired, r))
    await mon.check_once()
    assert len(fired) == 1
    assert fired[0].component == "down"


@pytest.mark.asyncio
async def test_monitor_fires_on_recovered_callback() -> None:
    recovered = []
    state = {"healthy": False}

    async def toggle():
        state["healthy"] = not state["healthy"]
        if state["healthy"]:
            return HealthStatus.HEALTHY, "back up"
        raise RuntimeError("down")

    mon = HealthMonitor(check_interval_s=1.0)
    # Set error_rate_threshold high so the EWMA trend doesn't mask recovery.
    mon.register(HealthCheck("svc", toggle, error_rate_threshold=1.1))
    mon.on_recovered(lambda r: _append(recovered, r))
    # Cycle 1: HEALTHY (toggle flips to True first call)
    # Cycle 2: UNHEALTHY (flips to False)
    # Cycle 3: HEALTHY (flips to True again) → recovery event
    await mon.check_once()
    await mon.check_once()
    await mon.check_once()
    assert len(recovered) == 1


@pytest.mark.asyncio
async def test_monitor_status_reports_per_component() -> None:
    async def ok():
        return HealthStatus.HEALTHY, "ok"

    mon = HealthMonitor()
    mon.register(HealthCheck("a", ok))
    await mon.check_once()
    status = mon.status()
    assert "a" in status
    assert status["a"].status == HealthStatus.HEALTHY


async def _append(lst, item):
    lst.append(item)


# ---------------------------------------------------------------------------
# AutoScaler
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_autoscaler_emits_scale_up_on_unhealthy() -> None:
    async def unhealthy():
        raise RuntimeError("down")

    mon = HealthMonitor()
    mon.register(HealthCheck("svc", unhealthy))
    scaler = AutoScaler(mon, cooldown_s=0.0)  # disable cooldown for test
    signals: list[ScaleSignal] = []
    scaler.on_signal(lambda s: _append(signals, s))
    await mon.check_once()
    assert any(s.decision == ScaleDecision.SCALE_UP for s in signals)


@pytest.mark.asyncio
async def test_autoscaler_emits_scale_down_on_recovery() -> None:
    state = {"n": 0}

    async def toggle():
        state["n"] += 1
        if state["n"] == 1:
            raise RuntimeError("first down")
        return HealthStatus.HEALTHY, "back up"

    mon = HealthMonitor()
    mon.register(HealthCheck("svc", toggle, error_rate_threshold=1.1))
    scaler = AutoScaler(mon, cooldown_s=0.0)
    decisions = []
    scaler.on_signal(lambda s: _append(decisions, s.decision))
    # First check: UNHEALTHY → SCALE_UP
    await mon.check_once()
    # Second check: HEALTHY → SCALE_DOWN
    await mon.check_once()
    assert ScaleDecision.SCALE_UP in decisions
    assert ScaleDecision.SCALE_DOWN in decisions


@pytest.mark.asyncio
async def test_autoscaler_cooldown_blocks_rapid_signals() -> None:
    async def unhealthy():
        raise RuntimeError("down")

    mon = HealthMonitor()
    mon.register(HealthCheck("svc", unhealthy))
    scaler = AutoScaler(mon, cooldown_s=10.0)  # long cooldown
    signals = []
    scaler.on_signal(lambda s: _append(signals, s))
    await mon.check_once()
    await mon.check_once()  # second event should be blocked by cooldown
    assert len(signals) == 1


def test_autoscaler_signal_dataclass_serialization() -> None:
    sig = ScaleSignal(
        component="x",
        decision=ScaleDecision.HOLD,
        reason="just because",
        ewma_latency_ms=12.3,
        ewma_error_rate=0.05,
    )
    assert sig.component == "x"
    assert sig.decision == ScaleDecision.HOLD
    assert sig.ewma_latency_ms == 12.3
