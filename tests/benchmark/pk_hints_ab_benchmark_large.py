#!/usr/bin/env python3
"""
A/B Benchmark: PKFilter ON vs OFF — Large Scale (1M entities, 100 segments)

Goal: With 10 PKs queried across 100 segments, only ~1-2 segments should
contain the target PKs. PKFilter should skip the other 98-99 segments.
"""

import time
import random
import statistics
import subprocess
import sys
from pymilvus import (
    connections,
    utility,
    Collection,
    CollectionSchema,
    FieldSchema,
    DataType,
)

# ── Configuration ─────────────────────────────────────────────────
COLLECTION_NAME = "pk_hints_large_bench"
DIM = 128
NUM_ENTITIES = 1_000_000
NUM_SEGMENTS = 100  # Target segment count
BATCH_SIZE = NUM_ENTITIES // NUM_SEGMENTS  # 10,000 per segment
NUM_WARMUP = 20
NUM_QUERIES = 500  # More queries for stable stats
PK_LIST_SIZE = 10  # Fixed: query 10 PKs at a time
RANDOM_SEED = 42
ETCD_ROOT = "by-dev"

# ── Helpers ───────────────────────────────────────────────────────


def etcd_set(key: str, value: str):
    full_key = f"{ETCD_ROOT}/config/{key}"
    result = subprocess.run(
        ["etcdctl", "put", full_key, value], capture_output=True, text=True, timeout=10
    )
    if result.returncode != 0:
        print(f"  [WARN] etcdctl put failed: {result.stderr.strip()}")
        return False
    print(f"  etcdctl put {full_key} = {value}")
    return True


def set_pk_hints_enabled(enabled: bool):
    val = "true" if enabled else "false"
    ok = etcd_set("queryNode/enableSegmentPkHints", val)
    if ok:
        time.sleep(2)
    return ok


def disable_compaction():
    """Disable auto-compaction so segments stay separate."""
    etcd_set("dataCoord/compaction/enabled", "false")


def create_collection():
    if utility.has_collection(COLLECTION_NAME):
        print("  Dropping existing collection ...")
        utility.drop_collection(COLLECTION_NAME)
        time.sleep(2)

    fields = [
        FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="category", dtype=DataType.VARCHAR, max_length=64),
        FieldSchema(name="value", dtype=DataType.FLOAT),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=DIM),
    ]
    schema = CollectionSchema(fields, description="PK Hints Large Benchmark")
    col = Collection(COLLECTION_NAME, schema)
    col.create_index(
        "embedding",
        {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 256},
        },
    )
    return col


def insert_data(collection):
    """Insert 1M entities in 100 batches, flush each to create 100 segments."""
    categories = ["cat_A", "cat_B", "cat_C", "cat_D", "cat_E"]
    all_pks = []
    rng = random.Random(RANDOM_SEED)

    total_batches = NUM_ENTITIES // BATCH_SIZE
    for batch_idx in range(total_batches):
        start_pk = batch_idx * BATCH_SIZE
        end_pk = start_pk + BATCH_SIZE
        pks = list(range(start_pk, end_pk))
        all_pks.extend(pks)

        data = [
            pks,
            [rng.choice(categories) for _ in range(BATCH_SIZE)],
            [rng.random() * 100 for _ in range(BATCH_SIZE)],
            [[rng.random() for _ in range(DIM)] for _ in range(BATCH_SIZE)],
        ]
        collection.insert(data)
        collection.flush()

        if (batch_idx + 1) % 10 == 0:
            print(f"  Batch {batch_idx + 1}/{total_batches} flushed")

    print(f"  Total: {NUM_ENTITIES:,} entities in {total_batches} segments")
    return all_pks


def gen_query_pks(all_pks, seed):
    """Generate deterministic PK lists.
    Each query picks 10 PKs from a narrow range (1 segment worth),
    so only ~1 segment should contain most of them.
    """
    rng = random.Random(seed)
    queries = []
    for _ in range(NUM_WARMUP + NUM_QUERIES):
        # Pick a random segment, then sample PKs from that segment's range
        seg_idx = rng.randint(0, NUM_SEGMENTS - 1)
        seg_start = seg_idx * BATCH_SIZE
        seg_end = seg_start + BATCH_SIZE
        # Pick PKs from this segment's range
        selected = rng.sample(range(seg_start, seg_end), min(PK_LIST_SIZE, BATCH_SIZE))
        # Also add 2 non-existent PKs to exercise BF filtering
        selected.extend([NUM_ENTITIES + rng.randint(0, 100000) for _ in range(2)])
        queries.append(selected)
    return queries


def gen_scattered_query_pks(all_pks, seed):
    """Generate queries where PKs are scattered across many segments."""
    rng = random.Random(seed)
    queries = []
    for _ in range(NUM_WARMUP + NUM_QUERIES):
        selected = rng.sample(all_pks, PK_LIST_SIZE)
        # Add 2 fake PKs
        selected.extend([NUM_ENTITIES + rng.randint(0, 100000) for _ in range(2)])
        queries.append(selected)
    return queries


def run_query_benchmark(collection, queries, label=""):
    """Run query benchmark, return list of latencies in ms."""
    # Warmup
    for q in queries[:NUM_WARMUP]:
        pk_str = ",".join(map(str, q))
        collection.query(
            expr=f"pk in [{pk_str}]", output_fields=["pk"], consistency_level="Strong"
        )

    # Benchmark
    latencies = []
    for q in queries[NUM_WARMUP:]:
        pk_str = ",".join(map(str, q))
        expr = f"pk in [{pk_str}]"

        t0 = time.perf_counter()
        collection.query(
            expr=expr,
            output_fields=["pk", "category", "value"],
            consistency_level="Strong",
        )
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000)

    return latencies


def run_search_benchmark(collection, queries):
    """Run search-with-PK-filter benchmark."""
    rng = random.Random(12345)

    # Warmup
    for q in queries[:NUM_WARMUP]:
        pk_str = ",".join(map(str, q))
        vec = [[rng.random() for _ in range(DIM)]]
        collection.search(
            data=vec,
            anns_field="embedding",
            param={"metric_type": "L2", "params": {"nprobe": 16}},
            limit=10,
            expr=f"pk in [{pk_str}]",
            output_fields=["pk"],
        )

    # Benchmark
    latencies = []
    for q in queries[NUM_WARMUP:]:
        pk_str = ",".join(map(str, q))
        vec = [[rng.random() for _ in range(DIM)]]

        t0 = time.perf_counter()
        collection.search(
            data=vec,
            anns_field="embedding",
            param={"metric_type": "L2", "params": {"nprobe": 16}},
            limit=10,
            expr=f"pk in [{pk_str}]",
            output_fields=["pk", "category"],
        )
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000)

    return latencies


def stats(latencies):
    if len(latencies) < 2:
        return {"mean": 0, "median": 0, "p50": 0, "p95": 0, "p99": 0, "qps": 0}
    s = sorted(latencies)
    mean = statistics.mean(latencies)
    return {
        "mean": mean,
        "median": statistics.median(latencies),
        "p50": s[int(len(s) * 0.50)],
        "p95": s[int(len(s) * 0.95)],
        "p99": s[min(int(len(s) * 0.99), len(s) - 1)],
        "qps": 1000.0 / mean if mean > 0 else 0,
    }


def run_phase(collection, label, queries_local, queries_scattered):
    """Run all benchmarks for one phase (ON or OFF)."""
    results = {}

    print(f"\n  [{label}] Query: pk IN [10 PKs from 1 segment] (localized) ...")
    lat = run_query_benchmark(collection, queries_local)
    results["query_localized"] = stats(lat)
    print(
        f"    mean={results['query_localized']['mean']:.2f}ms  "
        f"p95={results['query_localized']['p95']:.2f}ms  "
        f"qps={results['query_localized']['qps']:.0f}"
    )

    print(f"  [{label}] Query: pk IN [10 scattered PKs] ...")
    lat = run_query_benchmark(collection, queries_scattered)
    results["query_scattered"] = stats(lat)
    print(
        f"    mean={results['query_scattered']['mean']:.2f}ms  "
        f"p95={results['query_scattered']['p95']:.2f}ms  "
        f"qps={results['query_scattered']['qps']:.0f}"
    )

    print(f"  [{label}] Search + pk IN [10 localized PKs] ...")
    lat = run_search_benchmark(collection, queries_local)
    results["search_localized"] = stats(lat)
    print(
        f"    mean={results['search_localized']['mean']:.2f}ms  "
        f"p95={results['search_localized']['p95']:.2f}ms  "
        f"qps={results['search_localized']['qps']:.0f}"
    )

    print(f"  [{label}] Search + pk IN [10 scattered PKs] ...")
    lat = run_search_benchmark(collection, queries_scattered)
    results["search_scattered"] = stats(lat)
    print(
        f"    mean={results['search_scattered']['mean']:.2f}ms  "
        f"p95={results['search_scattered']['p95']:.2f}ms  "
        f"qps={results['search_scattered']['qps']:.0f}"
    )

    return results


# ── Main ──────────────────────────────────────────────────────────


def main():
    print("=" * 85)
    print("  PKFilter A/B Benchmark — Large Scale")
    print("=" * 85)
    print(f"  Entities:     {NUM_ENTITIES:,}")
    print(f"  Segments:     ~{NUM_SEGMENTS}")
    print(f"  PK list size: {PK_LIST_SIZE}")
    print(f"  Queries:      {NUM_QUERIES} per test (+ {NUM_WARMUP} warmup)")
    print(
        "  Scenarios:    localized (PKs from 1 segment) + scattered (PKs across segments)"
    )
    print()

    # ── Connect ───────────────────────────────────────────────────
    print("Connecting to Milvus ...")
    try:
        connections.connect("default", host="localhost", port="19530", timeout=30)
        print("  Connected.\n")
    except Exception as e:
        print(f"  FAILED: {e}")
        sys.exit(1)

    # ── Disable compaction ────────────────────────────────────────
    print("Disabling compaction to preserve segment count ...")
    disable_compaction()

    # ── Prepare data ──────────────────────────────────────────────
    print("Creating collection ...")
    col = create_collection()

    print(f"Inserting {NUM_ENTITIES:,} entities in {NUM_SEGMENTS} batches ...")
    all_pks = insert_data(col)

    print("Loading collection ...")
    col.load()
    time.sleep(5)

    # Verify segment count
    print("Verifying segment count ...")
    col.flush()
    seg_info = utility.get_query_segment_info(COLLECTION_NAME)
    # state can be int (enum value) or have .name attribute
    sealed_count = 0
    growing_count = 0
    for s in seg_info:
        state = s.state if isinstance(s.state, int) else s.state.value
        if state == 3:  # SegmentState.Sealed = 3
            sealed_count += 1
        elif state == 2:  # SegmentState.Growing = 2
            growing_count += 1
    print(
        f"  Sealed segments: {sealed_count}, Growing segments: {growing_count}, Total: {len(seg_info)}"
    )

    # ── Generate deterministic queries ────────────────────────────
    queries_local = gen_query_pks(all_pks, seed=RANDOM_SEED)
    queries_scattered = gen_scattered_query_pks(all_pks, seed=RANDOM_SEED + 100)

    # ── Phase A: PKFilter OFF ─────────────────────────────────────
    print("\n" + "=" * 85)
    print("  Phase A: PKFilter OFF (baseline)")
    print("=" * 85)
    set_pk_hints_enabled(False)
    results_off = run_phase(col, "OFF", queries_local, queries_scattered)

    # ── Phase B: PKFilter ON ──────────────────────────────────────
    print("\n" + "=" * 85)
    print("  Phase B: PKFilter ON (optimized)")
    print("=" * 85)
    set_pk_hints_enabled(True)
    results_on = run_phase(col, "ON", queries_local, queries_scattered)

    # ── Comparison Table ──────────────────────────────────────────
    print("\n" + "=" * 85)
    print("  COMPARISON: PKFilter OFF vs ON")
    print("=" * 85)

    header = (
        f"{'Scenario':<25} "
        f"{'OFF mean':>9} {'OFF p95':>9} {'OFF QPS':>9}  "
        f"{'ON mean':>9} {'ON p95':>9} {'ON QPS':>9}  "
        f"{'Speedup':>8}"
    )
    print(header)
    print("-" * len(header))

    for key in [
        "query_localized",
        "query_scattered",
        "search_localized",
        "search_scattered",
    ]:
        off = results_off[key]
        on = results_on[key]
        speedup = off["mean"] / on["mean"] if on["mean"] > 0 else 0

        marker = " <--" if speedup > 1.05 else (" !!!" if speedup < 0.95 else "")
        print(
            f"{key:<25} "
            f"{off['mean']:>8.2f}ms {off['p95']:>8.2f}ms {off['qps']:>8.0f}  "
            f"{on['mean']:>8.2f}ms {on['p95']:>8.2f}ms {on['qps']:>8.0f}  "
            f"{speedup:>7.2f}x{marker}"
        )

    # ── Detailed latency distribution ─────────────────────────────
    print("\n" + "=" * 85)
    print("  DETAILED LATENCY (ms)")
    print("=" * 85)
    print(f"{'Scenario':<25} {'Phase':>6} {'Mean':>8} {'P50':>8} {'P95':>8} {'P99':>8}")
    print("-" * 70)
    for key in [
        "query_localized",
        "query_scattered",
        "search_localized",
        "search_scattered",
    ]:
        off = results_off[key]
        on = results_on[key]
        print(
            f"{key:<25} {'OFF':>6} {off['mean']:>8.2f} {off['p50']:>8.2f} {off['p95']:>8.2f} {off['p99']:>8.2f}"
        )
        print(
            f"{'':<25} {'ON':>6} {on['mean']:>8.2f} {on['p50']:>8.2f} {on['p95']:>8.2f} {on['p99']:>8.2f}"
        )

    # ── Cleanup ───────────────────────────────────────────────────
    print("\n" + "=" * 85)
    print("Cleaning up ...")
    col.release()
    utility.drop_collection(COLLECTION_NAME)
    set_pk_hints_enabled(False)
    etcd_set("dataCoord/compaction/enabled", "true")
    connections.disconnect("default")
    print("Done!")


if __name__ == "__main__":
    main()
