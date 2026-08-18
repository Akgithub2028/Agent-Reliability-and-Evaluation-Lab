"""Example workflow patterns registered into ``DEFAULT_REGISTRY`` at import time."""
from __future__ import annotations

import asyncio
from typing import Any, List

from mcp_agent.enhancements.workflow_patterns import (
    WorkflowPattern,
    register_workflow_pattern,
)


@register_workflow_pattern(
    "sequential_pipeline",
    description="Runs a fixed list of stages in order, passing the result forward.",
)
class SequentialPipelinePattern(WorkflowPattern):
    """Stage composition: stage[i].execute(result_of_stage_i_minus_1)."""

    def __init__(self, stages: List[Any] | None = None):
        self.stages = stages or []

    async def execute(self, input: Any) -> Any:  # noqa: A002
        result = input
        for stage in self.stages:
            out = await stage.execute(result)
            if out is not None:
                result = out
        return result


@register_workflow_pattern(
    "parallel_scatter_gather",
    description="Forks input into N workers, gathers their outputs as a list.",
)
class ParallelScatterGatherPattern(WorkflowPattern):
    def __init__(self, workers: List[Any] | None = None):
        self.workers = workers or []

    async def execute(self, input: Any) -> Any:  # noqa: A002
        if not self.workers:
            return []
        tasks = [asyncio.ensure_future(w.execute(input)) for w in self.workers]
        return await asyncio.gather(*tasks, return_exceptions=False)


@register_workflow_pattern(
    "retry_until_ok",
    description="Re-runs a single worker until it returns a truthy result or max_attempts is reached.",
)
class RetryUntilOkPattern(WorkflowPattern):
    def __init__(self, worker: Any | None = None, max_attempts: int = 3):
        self.worker = worker
        self.max_attempts = max_attempts

    async def execute(self, input: Any) -> Any:  # noqa: A002
        if self.worker is None:
            return None
        for _ in range(self.max_attempts):
            result = await self.worker.execute(input)
            if result:
                return result
        return None
