"""Tests for MCPProtocolAdapter / CompatibilityLayer (P1.1)."""
from __future__ import annotations

import pytest
import warnings

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


# ---------------------------------------------------------------------------
# Version negotiation
# ---------------------------------------------------------------------------


def test_supported_versions_listed_in_order() -> None:
    # Highest must be the last entry.
    assert LATEST_PROTOCOL_VERSION == SUPPORTED_PROTOCOL_VERSIONS[-1]
    assert "1.0" in SUPPORTED_PROTOCOL_VERSIONS
    assert "2.0" in SUPPORTED_PROTOCOL_VERSIONS


def test_negotiate_picks_highest_mutual_version() -> None:
    adapter = MCPProtocolAdapter("auto")
    caps = adapter.negotiate({
        "protocolVersion": "2.0",
        "tools": {},
        "resources": {"roots": {}},
        "prompts": {},
        "sampling": {},
        "elicitation": {"url": True},
    })
    assert caps.protocol_version == "2.0"
    assert caps.supports_tools
    assert caps.supports_resources
    assert caps.supports_sampling
    assert caps.supports_elicitation
    assert caps.supports_elicitation_url
    assert caps.supports_structured_tool_output is False  # tools.structuredOutput not set
    assert caps.supports_roots is True


def test_negotiate_falls_back_to_1_when_server_only_advertises_1() -> None:
    adapter = MCPProtocolAdapter("auto")
    caps = adapter.negotiate({"protocolVersion": "1.0", "tools": {}})
    assert caps.protocol_version == "1.0"
    assert caps.supports_tools
    # v1 servers cannot expose structured tool output even if requested
    assert caps.supports_structured_tool_output is False
    assert caps.supports_elicitation_url is False


def test_negotiate_handles_list_of_server_versions() -> None:
    adapter = MCPProtocolAdapter("auto")
    caps = adapter.negotiate({"protocolVersion": ["1.0", "1.20", "2.0"]})
    assert caps.protocol_version == "2.0"


def test_negotiate_with_explicit_pin_succeeds() -> None:
    adapter = MCPProtocolAdapter("1.20")
    caps = adapter.negotiate({"protocolVersion": ["1.0", "1.20"]})
    assert caps.protocol_version == "1.20"


def test_negotiate_with_unsupported_pin_falls_back_to_auto() -> None:
    adapter = MCPProtocolAdapter("9.9")  # not in either side's list
    caps = adapter.negotiate({"protocolVersion": ["1.0", "2.0"]})
    assert caps.protocol_version == "2.0"


def test_negotiate_no_overlap_defaults_to_1_0() -> None:
    adapter = MCPProtocolAdapter("auto", client_supported_versions=("3.0",))
    caps = adapter.negotiate({"protocolVersion": ["1.0", "2.0"]})
    assert caps.protocol_version == "1.0"


def test_negotiated_capabilities_to_dict_round_trip() -> None:
    adapter = MCPProtocolAdapter("auto")
    caps = adapter.negotiate({"protocolVersion": "2.0", "tools": {}})
    d = caps.to_dict()
    assert d["protocol_version"] == "2.0"
    assert "supports_tools" in d
    assert d["supports_tools"] is True


# ---------------------------------------------------------------------------
# CompatibilityLayer
# ---------------------------------------------------------------------------


def test_compat_layer_v2_warns_on_deprecated_feature() -> None:
    adapter = MCPProtocolAdapter("auto")
    adapter.negotiate({"protocolVersion": "2.0", "tools": {}})
    assert adapter.compat.is_v2
    assert adapter.compat.deprecation_count == 0
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        adapter.compat.check_feature("roots/list")
    assert adapter.compat.deprecation_count == 1
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_compat_layer_v1_does_not_warn_on_deprecated_feature() -> None:
    adapter = MCPProtocolAdapter("auto")
    adapter.negotiate({"protocolVersion": "1.0", "tools": {}})
    assert not adapter.compat.is_v2
    adapter.compat.check_feature("roots/list")
    assert adapter.compat.deprecation_count == 0


def test_compat_layer_v2_v2only_feature_passes() -> None:
    adapter = MCPProtocolAdapter("auto")
    adapter.negotiate({"protocolVersion": "2.0"})
    adapter.compat.check_feature("elicitation/url")  # must not raise


def test_compat_layer_v1_v2only_feature_raises() -> None:
    adapter = MCPProtocolAdapter("auto")
    adapter.negotiate({"protocolVersion": "1.0"})
    with pytest.raises(ProtocolFeatureUnavailable):
        adapter.compat.check_feature("elicitation/url")
    assert adapter.compat.unavailable_count == 1


def test_deprecated_and_v2only_sets_are_disjoint() -> None:
    assert DEPRECATED_IN_V2.isdisjoint(V2_ONLY_FEATURES)


def test_compat_layer_requires_negotiate_first() -> None:
    adapter = MCPProtocolAdapter("auto")
    with pytest.raises(RuntimeError):
        _ = adapter.compat
    with pytest.raises(RuntimeError):
        _ = adapter.negotiated


def test_negotiate_normalises_root_capability_field() -> None:
    """Some servers expose roots under `capabilities.roots` instead of `capabilities.resources.roots`."""
    adapter = MCPProtocolAdapter("auto")
    caps = adapter.negotiate({"protocolVersion": "1.20", "roots": {}})
    assert caps.supports_roots is True


def test_negotiate_structured_tool_output_flag() -> None:
    adapter = MCPProtocolAdapter("auto")
    caps = adapter.negotiate({
        "protocolVersion": "2.0",
        "tools": {"structuredOutput": True},
    })
    assert caps.supports_structured_tool_output is True
