"""Quick perf benchmark: streaming throughput (naive vs adaptive)."""
import asyncio
import json
import sys
import time

sys.path.insert(0, "/home/z/my-project/workspace/mcp-agent/src")

from mcp_agent.enhancements.streaming import AdaptiveStreamProcessor, QoSTier


async def naive_producer(n):
    for i in range(n):
        yield i


async def naive_pipeline(n):
    count = 0
    async for _ in naive_producer(n):
        count += 1
        await asyncio.sleep(0)
    return count


async def adaptive_pipeline(n, buffer):
    proc = AdaptiveStreamProcessor(max_buffer=buffer, qos=QoSTier.BEST_EFFORT)
    count = 0
    async for _ in proc.process(naive_producer(n)):
        count += 1
    return count, proc.stats


async def main():
    n = 5000
    runs = 3

    naive_tps = []
    for _ in range(runs):
        t0 = time.perf_counter()
        got = await naive_pipeline(n)
        elapsed = time.perf_counter() - t0
        naive_tps.append(got / elapsed)

    adaptive_tps = []
    for _ in range(runs):
        t0 = time.perf_counter()
        got, stats = await adaptive_pipeline(n, buffer=256)
        elapsed = time.perf_counter() - t0
        adaptive_tps.append(got / elapsed)

    result = {
        "n": n,
        "runs": runs,
        "naive_throughput_msgs_per_sec_avg": round(sum(naive_tps) / len(naive_tps), 0),
        "naive_throughput_msgs_per_sec_max": round(max(naive_tps), 0),
        "adaptive_throughput_msgs_per_sec_avg": round(sum(adaptive_tps) / len(adaptive_tps), 0),
        "adaptive_throughput_msgs_per_sec_max": round(max(adaptive_tps), 0),
        "naive_all_runs": [round(x, 0) for x in naive_tps],
        "adaptive_all_runs": [round(x, 0) for x in adaptive_tps],
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
