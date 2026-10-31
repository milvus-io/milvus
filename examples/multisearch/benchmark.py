"""Compare completed 50-search groups after verify.py functional checks pass.

Ordinary client fan-out and MultiSearch both use 16 concurrent searches per
group and at most 64 active logical searches across groups. No retries.
"""
import argparse
import concurrent.futures
import json
import subprocess
import threading
import time
from pathlib import Path

import numpy as np
from pymilvus.client.prepare import Prepare

from client import MultiSearchClient


def main(args):
    source = json.loads(Path(args.results).read_text())
    assert source["all_passed"], "functional verification must pass first"
    specs = source["collections"]
    client = MultiSearchClient(args.target)
    gate = threading.Semaphore(64)
    items = []
    for i in range(50):
        s = specs[i % len(specs)]
        req = Prepare.search_requests_with_expr(collection_name=s["name"], data=[s["query"]],
            anns_field=s["vector_field"], param={"metric_type": s["metric"], "params": {}},
            limit=1+i%9, output_fields=[s["scalar_field"]], expr=f'{s["scalar_field"]} >= 0',
            use_default_consistency=False, consistency_level=0)
        items.append((str(i), req))

    def ordinary(item):
        with gate:
            r = client.search(item[1])
            assert r.status.code == 0 and r.status.error_code == 0, str(r.status)

    def server_cpu():
        text = subprocess.check_output(["docker", "exec", args.container, "cat", "/sys/fs/cgroup/cpu.stat"], text=True)
        return int(dict(line.split() for line in text.splitlines())["usage_usec"]) / 1e6

    results = {"started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "methodology": "50 Search requests/group, nq=1/item, topK=1..9; same persistent gRPC channel; 16 workers/group and global concurrency 64; no retries; all-item status validation; warmup excluded; small local FLAT dataset",
               "rows": sum(s["rows"] for s in specs), "collections": len(specs), "phases": []}

    def phase(mode, concurrency, groups):
        pools = [concurrent.futures.ThreadPoolExecutor(max_workers=16) for _ in range(concurrency)] if mode == "parallel_search" else []
        local = threading.local()
        available = iter(pools)
        pool_lock = threading.Lock()

        def group(_):
            if pools and not hasattr(local, "pool"):
                with pool_lock: local.pool = next(available)
            start = time.perf_counter()
            if mode == "multisearch":
                r = client.multi_search(items, timeout=30)
                assert r.status.code == 0 and len(r.items) == 50, str(r.status)
                for item in r.items:
                    assert item.grpc_code == 0 and item.result.status.code == 0 and item.result.status.error_code == 0, str(item)
            elif mode == "parallel_search":
                list(local.pool.map(ordinary, items))
            else:
                for item in items: ordinary(item)
            return (time.perf_counter() - start) * 1000

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as workers:
            list(workers.map(group, range(concurrency * 2)))
            client.calls.counts.clear()
            cpu0, process0 = server_cpu(), time.process_time()
            start = time.perf_counter()
            latencies = list(workers.map(group, range(groups)))
            elapsed = time.perf_counter() - start
            process_cpu = time.process_time() - process0
            cpu = server_cpu() - cpu0
            counts = dict(client.calls.counts)
        for pool in pools: pool.shutdown()
        expected = groups if mode == "multisearch" else groups * 50
        assert sum(counts.values()) == expected, counts
        result = dict(mode=mode, concurrent_groups=concurrency, groups=groups, logical_searches=groups*50,
                      rpc_count=expected, errors=0, elapsed_s=elapsed, group_p50_ms=float(np.percentile(latencies, 50)),
                      group_p95_ms=float(np.percentile(latencies, 95)), group_p99_ms=float(np.percentile(latencies, 99)),
                      logical_searches_per_s=groups*50/elapsed, client_cpu_s=process_cpu, server_cgroup_cpu_s=cpu,
                      samples_ms=latencies)
        results["phases"].append(result)
        Path(args.output).write_text(json.dumps(results, indent=2))
        print(json.dumps({k:v for k,v in result.items() if k!='samples_ms'}), flush=True)

    try:
        phase("sequential_search", 1, 20)
        for concurrency in [1, 4, 8]:
            # Alternate which mode goes first across concurrency levels.
            modes = ["parallel_search", "multisearch"] if concurrency != 4 else ["multisearch", "parallel_search"]
            for mode in modes: phase(mode, concurrency, args.groups)
    finally:
        client.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--target", default="localhost:29550")
    p.add_argument("--container", default="milvus-multisearch-prototype")
    p.add_argument("--results", default="artifacts/multisearch/results.json")
    p.add_argument("--output", default="artifacts/multisearch/performance.json")
    p.add_argument("--groups", type=int, default=60)
    main(p.parse_args())
