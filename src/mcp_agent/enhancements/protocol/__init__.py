"""
MCP Protocol Adapter — version negotiation + backward-compatibility shim.

The improvement plan calls out:

    P1.1  Protocol Compliance & Enhancement
      * full MCP 2.0+ protocol stack with backward compatibility layer
      * deprecation warnings for legacy features (Roots → workspace boundaries)
      * adaptive protocol version detection

This module delivers exactly that: a thin adapter that

  * detects the highest mutually supported protocol version between a client
    and a server,
  * exposes a `CompatibilityLayer` that emits `DeprecationWarning`s whenever
    legacy features (e.g. `roots/list`) are used on a 2.0+ session,
  * keeps a normalised view of negotiated server capabilities regardless of
    the wire version.

It is intentionally self-contained and side-effect free — existing call-sites
in the repo continue to work unchanged, but new code can opt-in via
``MCPProtocolAdapter.negotiate(...)``.
"""
from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# Well-known MCP protocol versions, oldest supported first.
# The values track the public `mcp` python package releases (1.x → 2.x).
SUPPORTED_PROTOCOL_VERSIONS: Tuple[str, ...] = (
    "1.0", "1.10", "1.20", "2.0", "2.1",
)
LATEST_PROTOCOL_VERSION = SUPPORTED_PROTOCOL_VERSIONS[-1]

# Features that were deprecated (or removed entirely) in 2.0+.
# Anything in this set used against a 2.0+ session raises a
# DeprecationWarning via the CompatibilityLayer.
DEPRECATED_IN_V2 = frozenset({
    "roots/list",            # superseded by workspace boundaries in MCP 2.0
    "resources/subscribe",   # 2.0 prefers explicit pull with ETag semantics
    "prompts/subscribe",
})

# Features that exist *only* on 2.0+. Calling them against a v1 server raises
# a `ProtocolFeatureUnavailable` error.
V2_ONLY_FEATURES = frozenset({
    "structured/tool/output",
    "elicitation/url",
    "elicitation/file",
    "sampling/structured",
})


class ProtocolFeatureUnavailable(RuntimeError):
    """Raised when a caller requests a feature the negotiated version cannot serve."""


def _parse_version(v: str) -> Tuple[int, int]:
    """Parse '1.20' → (1, 20); '2.0' → (2, 0). Returns (0, 0) on garbage."""
    try:
        major, minor = v.split(".", 1)
        return int(major), int(minor)
    except Exception:
        return 0, 0


def _ge(a: str, b: str) -> bool:
    return _parse_version(a) >= _parse_version(b)


@dataclass
class NegotiatedCapabilities:
    """Normalised view of capabilities after negotiation."""

    protocol_version: str
    supports_tools: bool = True
    supports_resources: bool = True
    supports_prompts: bool = True
    supports_sampling: bool = False
    supports_elicitation: bool = False
    supports_elicitation_url: bool = False
    supports_structured_tool_output: bool = False
    supports_roots: bool = False  # legacy in 2.0+
    supports_logging: bool = True
    supports_completions: bool = False
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "protocol_version": self.protocol_version,
            "supports_tools": self.supports_tools,
            "supports_resources": self.supports_resources,
            "supports_prompts": self.supports_prompts,
            "supports_sampling": self.supports_sampling,
            "supports_elicitation": self.supports_elicitation,
            "supports_elicitation_url": self.supports_elicitation_url,
            "supports_structured_tool_output": self.supports_structured_tool_output,
            "supports_roots": self.supports_roots,
            "supports_logging": self.supports_logging,
            "supports_completions": self.supports_completions,
        }


class CompatibilityLayer:
    """
    Wraps a session-like object and emits DeprecationWarning when legacy
    features are used on a 2.0+ session, and raises ProtocolFeatureUnavailable
    when v2-only features are requested against a v1 session.
    """

    def __init__(self, negotiated_version: str, logger: Optional[logging.Logger] = None):
        self._version = negotiated_version
        self._log = logger or logging.getLogger(__name__)
        self._deprecation_counter = 0
        self._feature_unavailable_counter = 0

    @property
    def version(self) -> str:
        return self._version

    @property
    def is_v2(self) -> bool:
        return _ge(self._version, "2.0")

    @property
    def deprecation_count(self) -> int:
        return self._deprecation_counter

    @property
    def unavailable_count(self) -> int:
        return self._feature_unavailable_counter

    def check_feature(self, feature: str) -> None:
        """
        Pre-flight check for a feature. Raises or warns as appropriate.
        Safe to call before dispatching the actual request.
        """
        if feature in V2_ONLY_FEATURES and not self.is_v2:
            self._feature_unavailable_counter += 1
            raise ProtocolFeatureUnavailable(
                f"Feature '{feature}' requires MCP protocol 2.0+, "
                f"negotiated version is {self._version}"
            )
        if feature in DEPRECATED_IN_V2 and self.is_v2:
            self._deprecation_counter += 1
            msg = (
                f"Feature '{feature}' is deprecated as of MCP 2.0. "
                f"Prefer the modern alternative (see MCP 2.0 migration notes)."
            )
            self._log.warning("MCP_DEPRECATION: %s", msg)
            warnings.warn(msg, DeprecationWarning, stacklevel=2)


class MCPProtocolAdapter:
    """
    Adaptive protocol version detection + capability negotiation.

    Usage::

        adapter = MCPProtocolAdapter(requested_version="auto")
        caps = await adapter.negotiate(server_capabilities_dict)
        # caps.protocol_version is now the highest mutually supported version
        # use adapter.compat.check_feature("roots/list") before dispatching
    """

    def __init__(
        self,
        requested_version: str = "auto",
        *,
        client_supported_versions: Optional[Tuple[str, ...]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self._requested = requested_version
        self._client_versions = client_supported_versions or SUPPORTED_PROTOCOL_VERSIONS
        self._log = logger or logging.getLogger(__name__)
        self._negotiated: Optional[NegotiatedCapabilities] = None
        self._compat: Optional[CompatibilityLayer] = None

    @property
    def negotiated(self) -> NegotiatedCapabilities:
        if self._negotiated is None:
            raise RuntimeError("Protocol has not been negotiated yet — call negotiate()")
        return self._negotiated

    @property
    def compat(self) -> CompatibilityLayer:
        if self._compat is None:
            raise RuntimeError("Protocol has not been negotiated yet — call negotiate()")
        return self._compat

    # ----- negotiation -----------------------------------------------------

    def _pick_version(self, server_versions: List[str]) -> str:
        """Pick the highest mutually supported version."""
        if self._requested != "auto":
            # explicit pin
            if self._requested in self._client_versions and self._requested in server_versions:
                return self._requested
            # server doesn't speak the pinned version → fall back to highest common
            self._log.warning(
                "Requested MCP version %s not advertised by server (advertised: %s); "
                "falling back to auto-negotiation",
                self._requested, server_versions,
            )
        common = [v for v in self._client_versions if v in server_versions]
        if not common:
            # No overlap — last-ditch: assume server speaks 1.0 (the floor).
            self._log.warning(
                "No overlapping MCP versions between client=%s and server=%s; "
                "defaulting to 1.0",
                self._client_versions, server_versions,
            )
            return "1.0"
        return max(common, key=_parse_version)

    def negotiate(self, server_capabilities: Dict[str, Any]) -> NegotiatedCapabilities:
        """
        Negotiate protocol version + capabilities against a server's
        ``InitializeResult.capabilities`` dict (or any dict with the same
        shape). Pure function — does no I/O.
        """
        # Server may advertise a single protocolVersion string OR a list of them.
        server_version = server_capabilities.get("protocolVersion") or server_capabilities.get(
            "protocol_version"
        )
        if isinstance(server_version, str):
            server_versions: List[str] = [server_version]
        elif isinstance(server_version, list):
            server_versions = [str(v) for v in server_version]
        else:
            server_versions = list(SUPPORTED_PROTOCOL_VERSIONS)
            self._log.warning(
                "Server capabilities did not advertise a protocolVersion; assuming %s",
                server_versions,
            )

        picked = self._pick_version(server_versions)

        # Normalise capabilities across versions.
        caps = NegotiatedCapabilities(protocol_version=picked, raw=dict(server_capabilities))

        tools = server_capabilities.get("tools") or {}
        caps.supports_tools = "tools" in server_capabilities
        caps.supports_structured_tool_output = bool(tools.get("structuredOutput"))

        resources = server_capabilities.get("resources") or {}
        caps.supports_resources = "resources" in server_capabilities
        caps.supports_roots = "roots" in resources or "roots" in server_capabilities

        caps.supports_prompts = "prompts" in server_capabilities
        caps.supports_sampling = "sampling" in server_capabilities
        elicitation = server_capabilities.get("elicitation") or {}
        caps.supports_elicitation = "elicitation" in server_capabilities
        caps.supports_elicitation_url = bool(elicitation.get("url")) if isinstance(elicitation, dict) else False
        caps.supports_logging = server_capabilities.get("logging", True) is not False
        caps.supports_completions = "completions" in server_capabilities

        # Backward-compat downgrade: v1 servers don't expose structured tool output
        # even if the capabilities dict claims so.
        if not _ge(picked, "2.0"):
            caps.supports_structured_tool_output = False
            caps.supports_elicitation_url = False

        self._negotiated = caps
        self._compat = CompatibilityLayer(picked, logger=self._log)
        self._log.info(
            "MCP protocol negotiated: version=%s tools=%s resources=%s prompts=%s sampling=%s elicitation=%s",
            picked, caps.supports_tools, caps.supports_resources, caps.supports_prompts,
            caps.supports_sampling, caps.supports_elicitation,
        )
        return caps


__all__ = [
    "SUPPORTED_PROTOCOL_VERSIONS",
    "LATEST_PROTOCOL_VERSION",
    "DEPRECATED_IN_V2",
    "V2_ONLY_FEATURES",
    "NegotiatedCapabilities",
    "CompatibilityLayer",
    "MCPProtocolAdapter",
    "ProtocolFeatureUnavailable",
]
