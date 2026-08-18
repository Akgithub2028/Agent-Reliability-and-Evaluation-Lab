"""End-to-end scenario that combines all enhancement subsystems.

The scenario: a client wants to call a remote MCP-like service. The flow:

  1. ``MCPProtocolAdapter`` negotiates v2 with the server's capabilities.
  2. The server is fronted by an ``MCPConnectionPool`` with a circuit breaker.
  3. The actual call is wrapped in a ``ResilientExecutor`` with a fallback to a
     local A2A agent (registered in a ``HybridMCPA2AGateway``).
  4. The result is streamed back to the caller via ``AdaptiveStreamProcessor``.
  5. The whole thing is monitored by a ``HealthMonitor`` with an ``AutoScaler``
     that would scale up if the breaker trips repeatedly.

This exercises every public type in ``mcp_agent.enhancements`` in one go,
which is exactly what the improvement plan calls "E2E + protocol conformance"
coverage.
"""
from __future__ import annotations

import asyncio
import pytest

from mcp_agent.enhancements import (
    AdaptiveStreamProcessor,
    A2AServer,
    A2ATask,
    AgentCard,
    AutoScaler,
    CircuitBreaker,
    CircuitState,
    HealthCheck,
    HealthMonitor,
    HealthStatus,
    HybridMCPA2AGateway,
    MCPConnectionPool,
    MCPProtocolAdapter,
    ResilientExecutor,
    RetryPolicy,
    StateRecovery,
    TaskState,
    new_workflow_id,
    QoSTier,
)


# ---------------------------------------------------------------------------
# Fake "MCP server" — a tiny in-memory target the pool will manage connections to
# ---------------------------------------------------------------------------


class FakeMCPServer:
    def __init__(self, name: str, *, fail_first: int = 0):
        self.name = name
        self.fail_first = fail_first
        self.calls = 0

    async def call_tool(self, tool_name: str, args: dict) -> dict:
        self.calls += 1
        if self.calls <= self.fail_first:
            raise ConnectionError(f"transient failure {self.calls}/{self.fail_first}")
        return {"tool": tool_name, "echo": args, "server": self.name}


@pytest.mark.asyncio
async def test_end_to_end_happy_path() -> None:
    # 1. Protocol negotiation
    adapter = MCPProtocolAdapter("auto")
    caps = adapter.negotiate({
        "protocolVersion": "2.0",
        "tools": {"structuredOutput": True},
        "resources": {},
        "sampling": {},
        "elicitation": {"url": True},
    })
    assert caps.protocol_version == "2.0"
    assert caps.supports_structured_tool_output

    # 2. Server + connection pool
    server = FakeMCPServer("primary")
    pool = MCPConnectionPool(
        max_connections_per_target=4,
        max_total_connections=8,
        factory=lambda t: _conn_factory(server),
    )

    # 3. Resilient executor with A2A fallback
    async def a2a_fallback():
        # In a real deployment this would route to a different agent
        return {"fallback": True, "server": "a2a-standby"}

    from mcp_agent.enhancements import FallbackChain
    chain = FallbackChain().add(fn=a2a_fallback, name="a2a-standby")
    executor = ResilientExecutor(
        RetryPolicy(max_attempts=3, base_delay_s=0.01, jitter=0.0),
        fallback=chain,
        state=StateRecovery(),
    )

    # 4. Health monitor (no autoscale decisions asserted here, just that it runs)
    async def health_check():
        # Treat >3 consecutive failures as unhealthy
        breaker = pool.get_breaker("primary")
        if breaker.stats.consecutive_failures >= 3:
            return HealthStatus.UNHEALTHY, f"breaker failures={breaker.stats.consecutive_failures}"
        return HealthStatus.HEALTHY, "ok"

    monitor = HealthMonitor()
    monitor.register(HealthCheck("primary", health_check, check_interval_s=1.0))
    scaler = AutoScaler(monitor, cooldown_s=0.0)

    # 5. Make the actual call
    async def invoke(server_conn, tool_name, args):
        return await server_conn.call_tool(tool_name, args)

    async with pool.connection("primary") as conn:
        result = await executor.execute_with_resilience(
            invoke, conn, "search", {"q": "hello"}, workflow_id=new_workflow_id(), step=1,
        )
    assert result["tool"] == "search"
    assert result["server"] == "primary"

    # Check health is healthy
    await monitor.check_once()
    status = monitor.status()
    assert status["primary"].status == HealthStatus.HEALTHY

    await pool.close_all()


@pytest.mark.asyncio
async def test_end_to_end_recovers_from_transient_failures() -> None:
    """Server fails first 2 calls then succeeds — pool + executor + breaker
    should all recover."""
    server = FakeMCPServer("flaky", fail_first=2)
    pool = MCPConnectionPool(
        max_connections_per_target=2,
        factory=lambda t: _conn_factory(server),
    )
    executor = ResilientExecutor(
        RetryPolicy(max_attempts=5, base_delay_s=0.01, jitter=0.0),
    )
    # Each "call" creates a fresh connection; we model the failure as the
    # tool invocation failing (rather than connection factory failing) so
    # the breaker doesn't pre-trip.
    async def invoke():
        async with pool.connection("flaky") as conn:
            return await conn.call_tool("ping", {})

    result = await executor.execute_with_resilience(invoke)
    assert result["server"] == "flaky"
    assert server.calls == 3  # 2 fails + 1 success
    await pool.close_all()


@pytest.mark.asyncio
async def test_end_to_end_fallback_to_a2a_when_primary_dead() -> None:
    """Primary server is permanently down; executor retries, then falls back to A2A."""
    from mcp_agent.enhancements.connection import CircuitBreakerOpenError

    async def always_fail_factory(target):
        raise ConnectionError("primary is dead")

    pool = MCPConnectionPool(max_connections_per_target=2, factory=always_fail_factory)
    # Lower the breaker threshold so we don't have to fail too many times
    pool._breaker_factory = lambda t: CircuitBreaker(
        t, failure_threshold=2, open_timeout_s=10.0
    )
    # Pre-trip the breaker by triggering failures
    for _ in range(2):
        try:
            await pool.acquire("primary")
        except ConnectionError:
            pass
    breaker = pool.get_breaker("primary")
    assert breaker.state == CircuitState.OPEN

    # Set up A2A fallback
    async def a2a_handler(task: A2ATask) -> A2ATask:
        task.artifacts.append({"parts": [{"text": "a2a-result"}]})
        task.state = TaskState.COMPLETED
        return task

    a2a_server = A2AServer(AgentCard(name="standby"), handler=a2a_handler)
    gw = HybridMCPA2AGateway()
    gw.register_inproc(a2a_server, name="standby")

    async def a2a_fallback():
        result = await gw.call_tool("a2a__standby", {"message": {"parts": [{"text": "hi"}]}})
        return result

    from mcp_agent.enhancements import FallbackChain
    chain = FallbackChain().add(fn=a2a_fallback, name="a2a-standby")
    executor = ResilientExecutor(
        RetryPolicy(
            max_attempts=1,
            base_delay_s=0.01,
            retriable_exceptions=(ConnectionError, CircuitBreakerOpenError),
        ),
        fallback=chain,
    )

    async def invoke():
        async with pool.connection("primary") as conn:
            return {"primary": True}

    result = await executor.execute_with_resilience(invoke)
    assert "artifacts" in result
    assert result["state"] == "completed"
    await pool.close_all()
    await gw.close()


@pytest.mark.asyncio
async def test_end_to_end_streaming_back_pressure_with_a2a_consumer() -> None:
    """Adaptive stream processor propagates backpressure from a slow A2A consumer
    back to the MCP producer."""

    async def producer():
        for i in range(100):
            yield {"i": i}

    proc = AdaptiveStreamProcessor(max_buffer=4, qos=QoSTier.BEST_EFFORT)

    consumed = []

    async for item in proc.process(producer()):
        consumed.append(item)
        await asyncio.sleep(0.001)  # slow consumer

    assert len(consumed) == 100
    assert proc.stats.backpressure_events > 0


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


async def _conn_factory(server):
    """Wrap a FakeMCPServer so the pool sees a fresh 'connection' each acquire."""
    return server
