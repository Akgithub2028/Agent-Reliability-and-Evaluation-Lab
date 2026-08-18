"""Enhanced metrics + perf benchmark — capture post-enhancement state."""
from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

REPO = Path("/home/z/my-project/workspace/mcp-agent")
PYTHON = sys.executable
OUT = Path("/home/z/my-project/workspace/enhanced_metrics.json")


def run(cmd, cwd=REPO, env=None, timeout=900):
    e = dict(os.environ)
    if env:
        e.update(env)
    return subprocess.run(
        cmd, cwd=cwd, env=e, capture_output=True, text=True, timeout=timeout
    )


def collect_tests() -> int:
    r = run([PYTHON, "-m", "pytest", "tests", "-m", "not integration",
             "--co", "-q"], timeout=180)
    m = re.search(r"(\d+) tests collected", r.stdout + r.stderr)
    return int(m.group(1)) if m else -1


def run_tests() -> tuple[int, int]:
    r = run(
        [PYTHON, "-m", "pytest", "tests", "-m", "not integration",
         "--no-header", "-q", "--tb=no", "-p", "no:cacheprovider"],
        timeout=900,
    )
    out = r.stdout + r.stderr
    m = re.search(r"(\d+) failed, (\d+) passed(?:, (\d+) skipped)?", out)
    if m:
        return int(m.group(2)), int(m.group(1)) + int(m.group(2)) + (int(m.group(3)) if m.group(3) else 0)
    m2 = re.search(r"(\d+) passed(?:, (\d+) skipped)?", out)
    if m2:
        return int(m2.group(1)), int(m2.group(1)) + (int(m2.group(2)) if m2.group(2) else 0)
    return -1, -1


def run_coverage() -> float:
    r = run(
        [PYTHON, "-m", "pytest", "tests", "-m", "not integration",
         "--no-header", "-q", "--tb=no",
         "--cov=src/mcp_agent", "--cov-report=term"],
        env={"COVERAGE_FILE": str(REPO / ".coverage.enhanced")},
        timeout=1200,
    )
    out = r.stdout + r.stderr
    m = re.search(r"TOTAL\s+\d+\s+\d+\s+(\d+(?:\.\d+)?)%", out)
    return float(m.group(1)) if m else -1.0


def streaming_throughput_adaptive() -> float:
    """Benchmark the new AdaptiveStreamProcessor."""
    from mcp_agent.enhancements.streaming import AdaptiveStreamProcessor, QoSTier

    async def producer(n: int):
        for i in range(n):
            yield i

    async def consume(n: int) -> int:
        proc = AdaptiveStreamProcessor(max_buffer=256, qos=QoSTier.BEST_EFFORT)
        count = 0
        async for _ in proc.process(producer(n)):
            count += 1
        return count

    n = 5000
    start = time.perf_counter()
    got = asyncio.run(consume(n))
    elapsed = time.perf_counter() - start
    assert got == n
    return n / elapsed


def streaming_throughput_baseline_naive() -> float:
    """Naive baseline: no queue, no backpressure."""
    async def producer(n: int):
        for i in range(n):
            yield i

    async def consume(n: int) -> int:
        count = 0
        async for _ in producer(n):
            count += 1
            await asyncio.sleep(0)
        return count

    n = 5000
    start = time.perf_counter()
    got = asyncio.run(consume(n))
    elapsed = time.perf_counter() - start
    assert got == n
    return n / elapsed


def capability_probe(module_path: str, symbol: str | None = None) -> bool:
    import importlib
    try:
        m = importlib.import_module(module_path)
        if symbol:
            return hasattr(m, symbol)
        return True
    except Exception:
        return False


def enhancements_loc() -> dict:
    """Count lines of code in the new enhancements package."""
    base = REPO / "src" / "mcp_agent" / "enhancements"
    total_lines = 0
    file_count = 0
    for p in base.rglob("*.py"):
        file_count += 1
        total_lines += sum(1 for _ in p.open())
    test_base = REPO / "tests" / "enhancements"
    test_lines = 0
    test_files = 0
    for p in test_base.rglob("*.py"):
        test_files += 1
        test_lines += sum(1 for _ in p.open())
    return {
        "src_files": file_count,
        "src_lines": total_lines,
        "test_files": test_files,
        "test_lines": test_lines,
    }


def main():
    print("[1/6] Collecting tests…")
    total = collect_tests()
    print(f"    collected={total}")

    print("[2/6] Running tests (no coverage)…")
    passed, total = run_tests()
    print(f"    passed={passed} total={total} rate={passed/total*100:.2f}%")

    print("[3/6] Running coverage…")
    cov = run_coverage()
    print(f"    line_coverage={cov}%")

    print("[4/6] Capability probes…")
    a2a = capability_probe("mcp_agent.enhancements.a2a", "A2AClient")
    plugin = capability_probe("mcp_agent.enhancements.plugin", "PluginManager")
    cb = capability_probe("mcp_agent.enhancements.connection", "MCPConnectionPool")
    stream = capability_probe("mcp_agent.enhancements.streaming", "AdaptiveStreamProcessor")
    health = capability_probe("mcp_agent.enhancements.health", "HealthMonitor")
    proto = capability_probe("mcp_agent.enhancements.protocol", "MCPProtocolAdapter")
    resilience = capability_probe("mcp_agent.enhancements.resilience", "ResilientExecutor")
    patterns = capability_probe("mcp_agent.enhancements.workflow_patterns", "WorkflowPatternRegistry")
    print(f"    proto={proto} a2a={a2a} plugin={plugin} cb={cb} stream={stream} health={health} resilience={resilience} patterns={patterns}")

    print("[5/6] Streaming throughput micro-benchmark (adaptive)…")
    tput_a = streaming_throughput_adaptive()
    print(f"    adaptive={tput_a:.0f} msg/s")

    print("[5.5/6] Streaming throughput micro-benchmark (naive baseline)…")
    tput_n = streaming_throughput_baseline_naive()
    print(f"    naive={tput_n:.0f} msg/s")

    print("[6/6] LOC stats…")
    loc = enhancements_loc()
    print(f"    {loc}")

    metrics = {
        "scenario": "enhanced (after applying all P1-P4 improvements + 109 new tests)",
        "test_total_count": total,
        "test_pass_count": passed,
        "test_pass_rate_pct": round(passed / total * 100, 2) if total else 0.0,
        "line_coverage_pct": cov,
        "a2a_protocol_supported": a2a,
        "plugin_architecture_supported": plugin,
        "circuit_breaker_supported": cb,
        "adaptive_streaming_supported": stream,
        "health_monitor_supported": health,
        "mcp_protocol_adapter_supported": proto,
        "resilient_executor_supported": resilience,
        "workflow_pattern_registry_supported": patterns,
        "naive_streaming_throughput_msgs_per_sec": round(tput_n, 1),
        "adaptive_streaming_throughput_msgs_per_sec": round(tput_a, 1),
        "enhancements_loc": loc,
    }
    OUT.write_text(json.dumps(metrics, indent=2))
    print(f"\nWrote {OUT}")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
