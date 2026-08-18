"""Tests for WorkflowPatternRegistry + PatternComposer (P3.2)."""
from __future__ import annotations

import pytest

from mcp_agent.enhancements.workflow_patterns import (
    DEFAULT_REGISTRY,
    PatternComposer,
    PatternRecord,
    WorkflowPattern,
    WorkflowPatternRegistry,
    register_workflow_pattern,
)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_register_and_get_pattern() -> None:
    reg = WorkflowPatternRegistry()

    class MyPattern(WorkflowPattern):
        name = "MyPattern"

        async def execute(self, input):
            return input + 1

    reg.register("mine", MyPattern, description="adds 1")
    rec = reg.get("mine")
    assert rec is not None
    assert rec.name == "mine"
    assert "adds 1" in rec.description
    assert "mine" in reg.names()


def test_register_rejects_non_pattern_class() -> None:
    reg = WorkflowPatternRegistry()
    with pytest.raises(TypeError):
        reg.register("bad", object)


def test_register_empty_name_rejected() -> None:
    reg = WorkflowPatternRegistry()
    with pytest.raises(ValueError):
        reg.register("", type("X", (), {"execute": lambda self, x: x}))


def test_register_duplicate_ignored_without_override() -> None:
    reg = WorkflowPatternRegistry()

    class A(WorkflowPattern):
        async def execute(self, input):
            return "a"

    class B(WorkflowPattern):
        async def execute(self, input):
            return "b"

    reg.register("dup", A)
    reg.register("dup", B)  # ignored
    assert reg.get("dup").pattern_cls is A


def test_register_with_override_replaces() -> None:
    reg = WorkflowPatternRegistry()

    class A(WorkflowPattern):
        async def execute(self, input):
            return "a"

    class B(WorkflowPattern):
        async def execute(self, input):
            return "b"

    reg.register("dup", A)
    reg.register("dup", B, override=True)
    assert reg.get("dup").pattern_cls is B


def test_instantiate_unknown_raises() -> None:
    reg = WorkflowPatternRegistry()
    with pytest.raises(KeyError):
        reg.instantiate("nope")


def test_unregister() -> None:
    reg = WorkflowPatternRegistry()

    class A(WorkflowPattern):
        async def execute(self, input):
            return "a"

    reg.register("x", A)
    reg.unregister("x")
    assert reg.get("x") is None


def test_default_registry_has_examples() -> None:
    # Importing examples.patterns should populate DEFAULT_REGISTRY
    import importlib
    importlib.import_module("mcp_agent.enhancements.examples.patterns")
    names = DEFAULT_REGISTRY.names()
    assert "sequential_pipeline" in names
    assert "parallel_scatter_gather" in names
    assert "retry_until_ok" in names


def test_register_workflow_pattern_decorator() -> None:
    reg = WorkflowPatternRegistry()

    @register_workflow_pattern("deco_test", registry=reg)
    class DecoPattern(WorkflowPattern):
        async def execute(self, input):
            return input * 2

    assert reg.get("deco_test") is not None
    assert reg.get("deco_test").pattern_cls is DecoPattern


# ---------------------------------------------------------------------------
# PatternComposer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_composer_runs_in_order() -> None:
    class Inc(WorkflowPattern):
        async def execute(self, input):
            return input + 1

    class Double(WorkflowPattern):
        async def execute(self, input):
            return input * 2

    composer = PatternComposer([Inc(), Double(), Inc()])
    # (1 + 1) * 2 + 1 = 5
    result = await composer.execute(1)
    assert result == 5


@pytest.mark.asyncio
async def test_composer_forwards_value_when_pattern_returns_none() -> None:
    class PassThrough(WorkflowPattern):
        async def execute(self, input):
            return None  # explicitly drop result

    class Inc(WorkflowPattern):
        async def execute(self, input):
            return input + 1

    composer = PatternComposer([Inc(), PassThrough()])
    # Inc(1) = 2; PassThrough drops to None → previous value forwarded
    result = await composer.execute(1)
    assert result == 2


@pytest.mark.asyncio
async def test_composer_propagates_exceptions() -> None:
    class Boom(WorkflowPattern):
        async def execute(self, input):
            raise RuntimeError("boom")

    composer = PatternComposer([Boom()])
    with pytest.raises(RuntimeError, match="boom"):
        await composer.execute(0)
