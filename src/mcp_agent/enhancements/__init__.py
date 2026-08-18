"""
mcp_agent.enhancements — production-grade add-ons to the mcp-agent framework.

This package implements the improvement plan proposed in
``MCP Agent Improvement.md``:

  P1.1  protocol     — ``MCPProtocolAdapter`` (v1/v2 negotiation + deprecation)
  P1.2  a2a          — ``AgentCard``, ``A2AClient``, ``A2AServer``, hybrid gateway
  P2.1  streaming    — ``AdaptiveStreamProcessor``, ``StreamingMultiplexer``, QoS
  P2.2  connection   — ``MCPConnectionPool``, ``CircuitBreaker``, ``QuotaManager``
  P3.1  plugin       — ``PluginManager`` with hot-reload
  P3.2  workflow_patterns — ``WorkflowPatternRegistry``, ``PatternComposer``
  P4.1  resilience   — ``ResilientExecutor``, ``RetryPolicy``, ``FallbackChain``
  P4.2  health       — ``HealthMonitor``, ``HealthCheck``, ``AutoScaler``

Each submodule is self-contained — it does not modify any existing call-site
in ``mcp_agent.*``, so the rest of the framework continues to behave
unchanged. New code opts in by importing from this package.
"""
from __future__ import annotations

# Re-export the headline types so callers can do:
#   from mcp_agent.enhancements import MCPProtocolAdapter, A2AClient, ...
from mcp_agent.enhancements.protocol import (
    CompatibilityLayer,
    DEPRECATED_IN_V2,
    LATEST_PROTOCOL_VERSION,
    MCPProtocolAdapter,
    NegotiatedCapabilities,
    ProtocolFeatureUnavailable,
    SUPPORTED_PROTOCOL_VERSIONS,
    V2_ONLY_FEATURES,
)
from mcp_agent.enhancements.a2a import (
    A2AClient,
    A2AError,
    A2AServer,
    A2ATask,
    A2ATaskNotFoundError,
    A2ATimeoutError,
    AgentCard,
    HybridMCPA2AGateway,
    TaskHandler,
    TaskState,
    TERMINAL_STATES as A2A_TERMINAL_STATES,
)
from mcp_agent.enhancements.streaming import (
    AdaptiveStreamProcessor,
    QoSTier,
    StreamStats,
    StreamingMultiplexer,
)
from mcp_agent.enhancements.connection import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitBreakerStats,
    CircuitState,
    MCPConnectionPool,
    Quota,
    QuotaManager,
)
from mcp_agent.enhancements.plugin import (
    Plugin,
    PluginManager,
    PluginRecord,
    load_plugin,
)
from mcp_agent.enhancements.workflow_patterns import (
    DEFAULT_REGISTRY,
    PatternComposer,
    PatternRecord,
    WorkflowPattern,
    WorkflowPatternRegistry,
    register_workflow_pattern,
)
from mcp_agent.enhancements.resilience import (
    ExecutionStats,
    FallbackChain,
    FallbackEntry,
    FallbackFn,
    FallbackPredicate,
    new_workflow_id,
    ResilientExecutor,
    RetryPolicy,
    StateRecovery,
    StateSnapshot,
)
from mcp_agent.enhancements.health import (
    AutoScaler,
    HealthCallback,
    HealthCheck,
    HealthCheckResult,
    HealthMonitor,
    HealthStatus,
    ScaleDecision,
    ScaleSignal,
)

__all__ = [
    # protocol
    "MCPProtocolAdapter",
    "CompatibilityLayer",
    "NegotiatedCapabilities",
    "ProtocolFeatureUnavailable",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "LATEST_PROTOCOL_VERSION",
    "DEPRECATED_IN_V2",
    "V2_ONLY_FEATURES",
    # a2a
    "AgentCard",
    "A2ATask",
    "TaskState",
    "A2A_TERMINAL_STATES",
    "TaskHandler",
    "A2AServer",
    "A2AClient",
    "HybridMCPA2AGateway",
    "A2AError",
    "A2ATaskNotFoundError",
    "A2ATimeoutError",
    # streaming
    "AdaptiveStreamProcessor",
    "StreamingMultiplexer",
    "QoSTier",
    "StreamStats",
    # connection
    "MCPConnectionPool",
    "CircuitBreaker",
    "CircuitBreakerStats",
    "CircuitBreakerOpenError",
    "CircuitState",
    "Quota",
    "QuotaManager",
    # plugin
    "Plugin",
    "PluginManager",
    "PluginRecord",
    "load_plugin",
    # workflow_patterns
    "WorkflowPattern",
    "WorkflowPatternRegistry",
    "PatternRecord",
    "DEFAULT_REGISTRY",
    "register_workflow_pattern",
    "PatternComposer",
    # resilience
    "RetryPolicy",
    "FallbackChain",
    "FallbackEntry",
    "FallbackFn",
    "FallbackPredicate",
    "StateSnapshot",
    "StateRecovery",
    "ExecutionStats",
    "ResilientExecutor",
    "new_workflow_id",
    # health
    "HealthStatus",
    "HealthCheckResult",
    "HealthCheck",
    "HealthMonitor",
    "HealthCallback",
    "ScaleDecision",
    "ScaleSignal",
    "AutoScaler",
]
