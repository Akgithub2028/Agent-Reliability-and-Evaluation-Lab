# mcp-agent enhancements

In addition to the upstream `mcp-agent` framework, this fork ships an
**additive** enhancements package at `src/mcp_agent/enhancements/`. It is fully
backward-compatible — existing code paths are untouched, and new code opts in
by importing from `mcp_agent.enhancements`.

The full audit (before/after metrics, design rationale, reproduction
instructions) is in [`audit.md`](./audit.md).

## Quick start

```python
from mcp_agent.enhancements import (
    # P1.1 — protocol negotiation
    MCPProtocolAdapter,
    # P1.2 — A2A protocol
    A2AClient, A2AServer, AgentCard, HybridMCPA2AGateway,
    # P2.1 — adaptive streaming
    AdaptiveStreamProcessor, QoSTier,
    # P2.2 — connection pool + circuit breaker
    MCPConnectionPool, CircuitBreaker,
    # P3.1 — plugin manager with hot-reload
    PluginManager,
    # P3.2 — workflow patterns
    WorkflowPatternRegistry, register_workflow_pattern, PatternComposer,
    # P4.1 — resilient executor
    ResilientExecutor, RetryPolicy, FallbackChain, StateRecovery,
    # P4.2 — health monitoring + autoscaler
    HealthMonitor, HealthCheck, AutoScaler,
)
```

## What's new

| Area | Module | Headline capability |
|---|---|---|
| Protocol compliance | `mcp_agent.enhancements.protocol` | v1/v2 negotiation + deprecation warnings |
| A2A protocol | `mcp_agent.enhancements.a2a` | Agent discovery + task delegation + hybrid MCP/A2A gateway |
| Adaptive streaming | `mcp_agent.enhancements.streaming` | Bounded queue + 3 QoS tiers + multiplexer |
| Connection pool | `mcp_agent.enhancements.connection` | Per-target pool + circuit breaker + quota manager |
| Plugin manager | `mcp_agent.enhancements.plugin` | Hot-reload via watchdog (or polling fallback) |
| Workflow patterns | `mcp_agent.enhancements.workflow_patterns` | Registry + decorator + composer |
| Resilient executor | `mcp_agent.enhancements.resilience` | Retry + jitter + fallback + state recovery |
| Health monitoring | `mcp_agent.enhancements.health` | EWMA latency + error-rate + autoscale signals |

## Testing

The new package ships with **109 tests** (all passing) under
`tests/enhancements/`, including:

- unit tests for every public class,
- 4 cross-cutting end-to-end scenarios (`test_end_to_end.py`),
- 4 performance regression benchmarks (`test_perf_regression.py`).

Run them with:

```bash
pytest tests/enhancements -v
```

## Reproducing the audit numbers

```bash
# Test counts + pass rate
pytest tests -m "not integration" --no-header -q --tb=no

# Coverage (whole package)
pytest tests -m "not integration" --cov=src/mcp_agent --cov-report=term

# Coverage (enhancements only)
pytest tests/enhancements --cov=src/mcp_agent/enhancements --cov-report=term

# Streaming throughput benchmark
python scripts/bench_streaming.py
```
