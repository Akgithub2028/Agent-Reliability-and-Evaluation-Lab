"""Tests for AdaptiveStreamProcessor + StreamingMultiplexer (P2.1)."""
from __future__ import annotations

import asyncio
import pytest

from mcp_agent.enhancements.streaming import (
    AdaptiveStreamProcessor,
    QoSTier,
    StreamStats,
    StreamingMultiplexer,
)


async def _aiter(n: int):
    for i in range(n):
        yield i


# ---------------------------------------------------------------------------
# AdaptiveStreamProcessor
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_processor_passes_all_items_through() -> None:
    proc = AdaptiveStreamProcessor(max_buffer=10)
    out = []
    async for item in proc.process(_aiter(50)):
        out.append(item)
    assert out == list(range(50))
    assert proc.stats.items_in == 50
    assert proc.stats.items_out == 50
    assert proc.stats.items_dropped == 0


@pytest.mark.asyncio
async def test_processor_drops_oldest_when_droppable_buffer_full() -> None:
    proc = AdaptiveStreamProcessor(max_buffer=2, qos=QoSTier.DROPPABLE, drop_on_full=True)

    async def slow_consumer(stream):
        # Consume slowly so the producer outruns us
        out = []
        async for x in stream:
            out.append(x)
            await asyncio.sleep(0.001)
        return out

    async def producer():
        # Yield quickly without yielding to event loop
        for i in range(10):
            yield i

    out = await slow_consumer(proc.process(producer(), qos=QoSTier.DROPPABLE))
    # At least one item should be dropped because buffer=2 and producer is faster
    assert proc.stats.items_dropped > 0
    # Output should be a subset of inputs (no fabrication)
    assert set(out).issubset(set(range(10)))


@pytest.mark.asyncio
async def test_processor_applies_backpressure_for_best_effort() -> None:
    # With BEST_EFFORT and a tiny buffer + slow consumer, the producer should
    # be forced to wait (backpressure_events > 0).
    proc = AdaptiveStreamProcessor(max_buffer=1, qos=QoSTier.BEST_EFFORT)

    async def fast_producer():
        for i in range(20):
            yield i

    async def consume():
        out = []
        async for x in proc.process(fast_producer()):
            out.append(x)
            await asyncio.sleep(0.001)
        return out

    out = await consume()
    assert len(out) == 20  # nothing dropped
    assert proc.stats.backpressure_events > 0


@pytest.mark.asyncio
async def test_processor_invokes_on_backpressure_callback() -> None:
    triggered = []

    async def on_bp(stats: StreamStats) -> None:
        triggered.append(stats.items_in)

    proc = AdaptiveStreamProcessor(
        max_buffer=1, qos=QoSTier.BEST_EFFORT, on_backpressure=on_bp
    )

    async def producer():
        for i in range(10):
            yield i

    async def consume():
        async for _ in proc.process(producer()):
            await asyncio.sleep(0.001)

    await consume()
    assert len(triggered) > 0


@pytest.mark.asyncio
async def test_processor_transform_called_per_item() -> None:
    async def transform(x):
        return x * 10

    proc = AdaptiveStreamProcessor(max_buffer=10, transform=transform)
    out = []
    async for item in proc.process(_aiter(5)):
        out.append(item)
    assert out == [0, 10, 20, 30, 40]


@pytest.mark.asyncio
async def test_processor_invalid_buffer_raises() -> None:
    with pytest.raises(ValueError):
        AdaptiveStreamProcessor(max_buffer=0)


@pytest.mark.asyncio
async def test_processor_stats_to_dict_serializable() -> None:
    proc = AdaptiveStreamProcessor(max_buffer=10)
    async for _ in proc.process(_aiter(5)):
        pass
    d = proc.stats.to_dict()
    assert d["items_in"] == 5
    assert d["items_out"] == 5
    assert "throughput_per_sec" in d


# ---------------------------------------------------------------------------
# StreamingMultiplexer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mux_fans_in_multiple_sources() -> None:
    async def src(name, n):
        # Yield just the integer; the mux tags each item with its source name.
        for i in range(n):
            await asyncio.sleep(0)
            yield i

    mux = StreamingMultiplexer(max_buffer=20)
    mux.add_source("a", src("a", 3), qos=QoSTier.BEST_EFFORT)
    mux.add_source("b", src("b", 3), qos=QoSTier.BEST_EFFORT)

    out = []
    async for name, item in mux:
        out.append((name, item))
    # All 6 items should be present (order may vary)
    assert sorted(out) == sorted(
        [("a", 0), ("a", 1), ("a", 2), ("b", 0), ("b", 1), ("b", 2)]
    )


@pytest.mark.asyncio
async def test_mux_empty_iter_returns_nothing() -> None:
    mux = StreamingMultiplexer()
    out = []
    async for x in mux:  # type: ignore
        out.append(x)
    assert out == []


@pytest.mark.asyncio
async def test_mux_duplicate_source_name_raises() -> None:
    mux = StreamingMultiplexer()
    mux.add_source("a", _aiter(1))
    with pytest.raises(ValueError):
        mux.add_source("a", _aiter(1))
