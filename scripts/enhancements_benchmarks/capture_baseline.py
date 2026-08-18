"""
Baseline benchmark + coverage capture for the unmodified mcp-agent repo.
Produces /home/z/my-project/workspace/baseline_metrics.json with:
  - test_pass_count
  - test_total_count
  - test_pass_rate
  - line_coverage_pct
  - a2a_protocol_supported (False at baseline)
  - plugin_architecture_supported (False at baseline)
  - circuit_breaker_supported (False at baseline)
  - adaptive_streaming_supported (False at baseline)
  - health_monitor_supported (False at baseline)
  - streaming_throughput_msgs_per_sec (a synthetic micro-benchmark)
"""
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
OUT = Path("/home/z/my-project/workspace/baseline_metrics.json")


def run(cmd, cwd=REPO, env=None, timeout=600):
    e = dict(os.environ)
    if env:
        e.update(env)
    return subprocess.run(
        cmd, cwd=cwd, env=e, capture_output=True, text=True, timeout=timeout
    )


def collect_tests() -> int:
    r = run(
        [PYTHON, "-m", "pytest", "tests", "-m", "not integration",
         "--co", "-q"],
        env={"COVERAGE_FILE": "/dev/null"},
        timeout=120,
    )
    m = re.search(r"(\d+) tests collected", r.stdout + r.stderr)
    return int(m.group(1)) if m else -1


def run_tests() -> tuple[int, int]:
    r = run(
        [PYTHON, "-m", "pytest", "tests", "-m", "not integration",
         "--no-header", "-q", "--tb=no", "-p", "no:cacheprovider"],
        env={"COVERAGE_FILE": "/dev/null"},
        timeout=600,
    )
    out = r.stdout + r.stderr
    m = re.search(r"(\d+) failed, (\d+) passed(?:, (\d+) skipped)?", out)
    if m:
        failed = int(m.group(1))
        passed = int(m.group(2))
        skipped = int(m.group(3)) if m.group(3) else 0
        return passed, passed + failed + skipped
    m2 = re.search(r"(\d+) passed(?:, (\d+) skipped)?", out)
    if m2:
        passed = int(m2.group(1))
        skipped = int(m2.group(2)) if m2.group(2) else 0
        return passed, passed + skipped
    return -1, -1


def run_coverage() -> float:
    """Run pytest with coverage scoped to src/mcp_agent, return pct."""
    r = run(
        [PYTHON, "-m", "pytest", "tests", "-m", "not integration",
         "--no-header", "-q", "--tb=no",
         "--cov=src/mcp_agent", "--cov-report=term"],
        env={"COVERAGE_FILE": str(REPO / ".coverage.baseline")},
        timeout=900,
    )
    out = r.stdout + r.stderr
    # match a line like "TOTAL  1200  400  67%"
    m = re.search(r"TOTAL\s+\d+\s+\d+\s+(\d+(?:\.\d+)?)%", out)
    return float(m.group(1)) if m else -1.0


def streaming_throughput_baseline() -> float:
    """Simulated baseline: simple async generator with no backpressure."""
    async def producer(n: int):
        for i in range(n):
            yield {"i": i, "payload": "x" * 64}

    async def consume(n: int) -> int:
        count = 0
        async for _ in producer(n):
            count += 1
        return count

    n = 5000
    start = time.perf_counter()
    got = asyncio.run(consume(n))
    elapsed = time.perf_counter() - start
    assert got == n
    return n / elapsed


def capability_probe(module_path: str, symbol: str | None = None) -> bool:
    """Return True if a module/symbol is importable."""
    import importlib
    try:
        m = importlib.import_module(module_path)
        if symbol:
            return hasattr(m, symbol)
        return True
    except Exception:
        return False


def main():
    print("[1/5] Collecting tests…")
    total = collect_tests()
    print(f"    collected={total}")

    print("[2/5] Running tests (no coverage)…")
    passed, total = run_tests()
    print(f"    passed={passed} total={total} rate={passed/total*100:.2f}%")

    print("[3/5] Running coverage (this takes a few minutes)…")
    cov = run_coverage()
    print(f"    line_coverage={cov}%")

    print("[4/5] Capability probes…")
    a2a = capability_probe("mcp_agent.a2a")
    plugin = capability_probe("mcp_agent.enhancements.plugin_manager", "PluginManager")
    cb = capability_probe("mcp_agent.enhancements.connection_pool", "MCPConnectionPool")
    stream = capability_probe("mcp_agent.enhancements.adaptive_streaming", "AdaptiveStreamProcessor")
    health = capability_probe("mcp_agent.enhancements.health_monitor", "HealthMonitor")
    print(f"    a2a={a2a} plugin={plugin} circuit_breaker={cb} stream={stream} health={health}")

    print("[5/5] Streaming throughput micro-benchmark…")
    tput = streaming_throughput_baseline()
    print(f"    throughput={tput:.0f} msg/s")

    metrics = {
        "scenario": "baseline (as-cloned + abstract-method regression fix only)",
        "test_total_count": total,
        "test_pass_count": passed,
        "test_pass_rate_pct": round(passed / total * 100, 2) if total else 0.0,
        "line_coverage_pct": cov,
        "a2a_protocol_supported": a2a,
        "plugin_architecture_supported": plugin,
        "circuit_breaker_supported": cb,
        "adaptive_streaming_supported": stream,
        "health_monitor_supported": health,
        "streaming_throughput_msgs_per_sec": round(tput, 1),
    }
    OUT.write_text(json.dumps(metrics, indent=2))
    print(f"\nWrote {OUT}")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
