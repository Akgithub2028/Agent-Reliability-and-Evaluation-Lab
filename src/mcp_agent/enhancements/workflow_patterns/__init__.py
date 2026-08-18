"""
Custom workflow pattern registration API.

Implements improvement-plan item P3.2:

  * ``WorkflowPatternRegistry`` — global registry for custom workflow patterns.
  * ``@register_workflow_pattern("name")`` decorator for declarative registration.
  * ``PatternComposer`` — composes two patterns sequentially, useful when
    callers want e.g. "router → orchestrator → swarm" without writing a
    custom workflow class.

A workflow pattern is duck-typed: any class with an ``async execute(input)``
method qualifies. The registry stores them by name and lets the runtime
discover all registered patterns at startup.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Type

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pattern protocol (duck-typed)
# ---------------------------------------------------------------------------


class WorkflowPattern:
    """
    Base class for workflow patterns. Subclasses MUST implement ``execute``.

    The registry accepts either ``WorkflowPattern`` subclasses or any object
    that has an ``async execute(input: Any) -> Any`` method.
    """

    name: str = ""
    description: str = ""

    async def execute(self, input: Any) -> Any:  # noqa: D401, A002
        raise NotImplementedError


@dataclass
class PatternRecord:
    name: str
    pattern_cls: Type[Any]
    description: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class WorkflowPatternRegistry:
    """
    Global registry for workflow patterns.

    Patterns are identified by name; the first registration wins (subsequent
    registrations with the same name log a warning and are ignored, unless
    ``override=True`` is passed).
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self._patterns: Dict[str, PatternRecord] = {}
        self._log = logger or logging.getLogger(__name__)

    def register(
        self,
        name: str,
        pattern_cls: Type[Any],
        *,
        description: str = "",
        override: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not name:
            raise ValueError("pattern name must be non-empty")
        if name in self._patterns and not override:
            self._log.warning("Pattern %r already registered; ignoring", name)
            return
        # Validate that pattern_cls looks like a WorkflowPattern
        if not (isinstance(pattern_cls, type) and hasattr(pattern_cls, "execute")):
            raise TypeError(
                f"pattern_cls must be a class with an async execute() method, got {pattern_cls!r}"
            )
        execute = getattr(pattern_cls, "execute", None)
        if not callable(execute):
            raise TypeError(f"{pattern_cls!r}.execute is not callable")
        self._patterns[name] = PatternRecord(
            name=name,
            pattern_cls=pattern_cls,
            description=description or pattern_cls.__doc__ or "",
            metadata=metadata or {},
        )
        self._log.info("Registered workflow pattern %r", name)

    def get(self, name: str) -> Optional[PatternRecord]:
        return self._patterns.get(name)

    def list(self) -> List[PatternRecord]:
        return list(self._patterns.values())

    def names(self) -> List[str]:
        return list(self._patterns.keys())

    def instantiate(self, name: str, *args, **kwargs) -> Any:
        rec = self._patterns.get(name)
        if rec is None:
            raise KeyError(f"unknown pattern {name!r}")
        return rec.pattern_cls(*args, **kwargs)

    def unregister(self, name: str) -> None:
        self._patterns.pop(name, None)


# Module-level default registry
DEFAULT_REGISTRY = WorkflowPatternRegistry()


def register_workflow_pattern(
    name: str,
    *,
    registry: WorkflowPatternRegistry = DEFAULT_REGISTRY,
    description: str = "",
    override: bool = False,
):
    """
    Class decorator that registers a workflow pattern.

    Example::

        @register_workflow_pattern("custom_collaborative")
        class CustomPattern(WorkflowPattern):
            async def execute(self, input):
                ...
    """

    def deco(cls):
        registry.register(name, cls, description=description, override=override)
        return cls

    return deco


# ---------------------------------------------------------------------------
# Pattern composer
# ---------------------------------------------------------------------------


class PatternComposer(WorkflowPattern):
    """
    Compose two (or more) patterns sequentially.

    The output of pattern[i] becomes the input of pattern[i+1]. If a pattern
    returns ``None``, the previous value is forwarded.
    """

    name = "composed"

    def __init__(self, patterns: List[Any], name: str = "composed"):
        self.patterns = list(patterns)
        self.name = name

    async def execute(self, input: Any) -> Any:
        result = input
        for i, p in enumerate(self.patterns):
            try:
                out = await p.execute(result)
            except Exception as exc:
                logger.exception("pattern %d/%d failed in composer: %s", i + 1, len(self.patterns), exc)
                raise
            if out is not None:
                result = out
        return result


__all__ = [
    "WorkflowPattern",
    "PatternRecord",
    "WorkflowPatternRegistry",
    "DEFAULT_REGISTRY",
    "register_workflow_pattern",
    "PatternComposer",
]
