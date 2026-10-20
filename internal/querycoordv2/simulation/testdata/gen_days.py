#!/usr/bin/env python3
"""Generate the "day" cases: one cluster shape each, living through the same
24 hours (nightly bulk insert, a crash, a scale-out for the daytime peak,
new tables, releases and reloads, a rolling upgrade, a QueryCoord restart,
a scale-in at night) so balancers can be compared phase by phase.

Times in the JSON are simulated seconds before the time scale (about a
tenth of real time); with time_scale 6 a day replays in 24 minutes.

    python3 gen_days.py            # writes cases/day_*.json
"""
import json
import os

TIME_SCALE = 6
SIM_PER_REAL = 10  # the simulator runs about ten times faster than real time
ROW_BYTES = 512


def H(hours):
    """Real hours -> simulated seconds (before the time scale)."""
    return round(hours * 3600 / SIM_PER_REAL, 1)


def rows(mb):
    return int(mb * (1 << 20) / ROW_BYTES)


# The day's ingest multipliers (1, 8, 1, 3, 1, 0.5 over 1+2+6+3+8+4 hours)
# average this much: a live table sees RATE_HOURS/24 x its 1x flush rate.
RATE_HOURS = 1 * 1 + 2 * 8 + 6 * 1 + 3 * 3 + 8 * 1 + 4 * 0.5
GROWTH_PER_DAY = 0.2  # a live table grows by this fraction of itself over the day


def tables(count, segs, mb, sigma=0.3, replicas=1, channels=1, flush_min=0, compact_min=0, rgs=None):
    t = {"count": count, "replicas": replicas, "channels": channels, "segments_per_channel": segs,
         "rows_median": rows(mb), "rows_sigma": sigma}
    if flush_min:
        t["flush_every_seconds"] = H(flush_min / 60)
        # Each seal adds a slice sized so the table grows GROWTH_PER_DAY over
        # the day; compaction (4:1) then builds larger segments from them.
        flushes = RATE_HOURS * 60 / flush_min
        t["flush_rows"] = max(1000, int(rows(mb) * segs * channels * GROWTH_PER_DAY / flushes))
    if compact_min:
        t["compact_every_seconds"] = H(compact_min / 60)
    if rgs:
        t["rgs"] = rgs
    return t


# Heavy-tailed table population, shaped like production (scan of 2026-10):
# most tables hold a few segments, a few hold hundreds. A live_frac of the
# tables take writes (a segment every flush_min minutes, compacted 4:1
# every compact_min); the rest are quiet. Only live tables feel the day's
# ingest phases.
def population(n, mb=10, sigma=0.5, replicas2=0.1, rgs=None, live_frac=0.2, flush_min=10, compact_min=40):
    out = []
    shares = [(0.45, 1, 1.5), (0.30, 3, 8), (0.18, 10, 30), (0.07, 40, 100)]
    for frac, lo, hi in shares:
        c = max(1, round(n * frac))
        segs = int((lo + hi) // 2)
        live = round(c * live_frac)
        r2 = max(0, round(c * replicas2))
        for count, rep in ((r2, 2), (c - r2, 1)):
            if count <= 0:
                continue
            l = min(live, count)
            live -= l
            if l:
                out.append(tables(l, segs, mb, sigma, replicas=rep, rgs=rgs, flush_min=flush_min, compact_min=compact_min))
            if count - l:
                out.append(tables(count - l, segs, mb, sigma, replicas=rep, rgs=rgs))
    return out


def day(name, category, qns, mem_gb, collections, *, scale_out=2, scale_in=2, rolling=3, crash_node=2,
        new_tables=None, release=0, resource_groups=None, bandwidth_mb=100,
        streaming=False, sns=0, cores=16, extra_events=(), source="", max_segment_mb=0):
    """One day in the life of a cluster. Phases (real hours):

    00-01 steady          01-03 nightly bulk insert (8x)      05 a node crashes
    05:15 it comes back   07 scale out for the day            09-12 day peak (3x)
    10 new tables arrive  13 some tables released, 13:30 back 15 rolling upgrade
    18 QueryCoord restart 20 evening (0.5x)                   22 scale in
    """
    ev = [
        {"at": H(1), "op": "ingest_rate", "multiplier": 8},
        {"at": H(3), "op": "ingest_rate", "multiplier": 1},
        {"at": H(5), "op": "kill_qn", "node": crash_node},
        {"at": H(5.25), "op": "restart_qn", "node": crash_node},
        *([{"at": H(7), "op": "add_qn", "count": max(1, scale_out // len(resource_groups)), "rg": rg["name"]} for rg in resource_groups]
          if resource_groups else [{"at": H(7), "op": "add_qn", "count": scale_out}]),
        {"at": H(9), "op": "ingest_rate", "multiplier": 3},
        {"at": H(12), "op": "ingest_rate", "multiplier": 1},
        {"at": H(15), "op": "rolling_replace", "count": rolling, "gap_seconds": H(5 / 60)},
        {"at": H(18), "op": "restart_qc"},
        {"at": H(20), "op": "ingest_rate", "multiplier": 0.5},
        {"at": H(22), "op": "stop_qn", "count": scale_in},
    ]
    if new_tables:
        ev.append({"at": H(10), "op": "load_tables", "tables": new_tables})
    if release:
        ev.append({"at": H(13), "op": "release_tables", "count": release})
        ev.append({"at": H(13.5), "op": "reload_tables", "count": release})
    ev.extend(extra_events)
    ev.sort(key=lambda e: e["at"])
    case = {
        "name": name, "category": category, "source": source or "synthetic day (gen_days.py)",
        "streaming": streaming, "qns": qns, "sns": sns, "qn_cores": cores, "qn_memory_gb": mem_gb,
        # bandwidth_mb is per node and shared by the concurrent loads: giant
        # segments need more or four of them outlast the task timeout.
        "load": {"concurrency": 4, "bandwidth_mb": bandwidth_mb, "base_ms": 10, "jitter_sigma": 0.4},
        "search": {"BaseMs": 0, "MsPerMillionRows": 0, "LoadInterference": 0},
        "time_scale": TIME_SCALE,
        "collections": collections,
        "ingest": {"rows_per_sec_per_channel": 0, "flush_every_seconds": 0, "compact_every_seconds": 0},
        "events": ev,
        "duration_seconds": H(24),
    }
    if resource_groups:
        case["resource_groups"] = resource_groups
    if max_segment_mb:
        # Compaction may build segments up to this size (the segment max size).
        case["max_segment_rows"] = rows(max_segment_mb)
    return case


CASES = [
    # Large segments on 32GB nodes: 4GB each (an eighth of a node), all alike.
    day("day_16_large_segments_uniform", "large_segments", 8, 32,
        [tables(4, 8, 4096, 0), tables(2, 6, 4096, 0, flush_min=10, compact_min=40)],
        scale_out=2, scale_in=2, rolling=2, new_tables=tables(1, 4, 4096, 0), release=2, bandwidth_mb=800, max_segment_mb=8192),
    # Large segments of unequal size: 4-8GB beside 2GB and 200MB ones.
    day("day_17_large_segments_skewed", "large_segments", 12, 32,
        [tables(2, 10, 6144, 0.3, flush_min=10, compact_min=40), tables(4, 8, 2048, 0.5), tables(30, 4, 200, 0.3)],
        scale_out=3, scale_in=2, rolling=3, new_tables=tables(1, 6, 6144, 0.3), release=10, bandwidth_mb=800, max_segment_mb=8192),
    # 8GB segments, a quarter of a 32GB node: one move changes a node by 25%.
    day("day_18_large_segments_8g", "large_segments", 16, 32,
        [tables(3, 10, 8192, 0), tables(1, 6, 8192, 0, replicas=2, flush_min=10, compact_min=40)],
        scale_out=4, scale_in=2, rolling=4, new_tables=tables(1, 4, 8192, 0), release=1, bandwidth_mb=800, max_segment_mb=8192),
    # Many tables on a big cluster: the two production pain points at once.
    day("day_01_big_cluster_many_tables", "big_cluster", 48, 64, population(2000, mb=60, live_frac=0.1),
        scale_out=6, scale_in=4, rolling=6, new_tables=tables(100, 4, 60, 0.5), release=200),
    # Thousands of tiny tables on a small cluster: segment counts, not bytes, decide.
    day("day_02_many_small_tables", "many_small", 8, 64, [tables(3600, 1, 30, 0.4), tables(400, 1, 30, 0.4, flush_min=20, compact_min=60), tables(900, 2, 30, 0.4), tables(100, 2, 30, 0.4, flush_min=20, compact_min=60)],
        new_tables=tables(500, 1, 30, 0.4), release=400),
    # Heavy-tailed sizes plus a handful of huge tables.
    day("day_03_skewed_tables", "skewed", 12, 64, population(400, mb=30) + [tables(5, 300, 80, 0.6, replicas=2, flush_min=3, compact_min=12)],
        new_tables=tables(20, 30, 30, 0.5), release=40),
    # Few big tables, two replicas each: every table must spread over every node.
    day("day_04_few_big_tables", "few_big", 24, 64, [tables(6, 400, 120, 0.6, replicas=2, flush_min=2, compact_min=8), tables(30, 8, 60, 0.5)],
        scale_out=4, scale_in=4, rolling=4, new_tables=tables(2, 200, 120, 0.6), release=2),
    # Giant segments: 2GB each on 16GB nodes, beside small tables.
    day("day_05_giant_segments", "giant_segments", 8, 16, [tables(2, 10, 2048, 0), tables(2, 10, 2048, 0, flush_min=10, compact_min=40), tables(20, 4, 100, 0.3)],
        scale_out=3, scale_in=2, new_tables=tables(1, 6, 2048, 0), release=5, bandwidth_mb=400),
    # Three equal tables with segments of 4GB, 1GB and 256MB.
    day("day_06_coarse_and_fine", "giant_segments", 6, 16,
        [tables(1, 4, 4096, 0, flush_min=10, compact_min=40), tables(1, 16, 1024, 0), tables(1, 64, 256, 0)],
        scale_out=2, scale_in=1, rolling=2, bandwidth_mb=800),
    # Nodes of two sizes: the 32GB nodes must hold half of what the 64GB ones do.
    day("day_07_mixed_node_sizes", "mixed_nodes", 8, 64, population(300, mb=80),
        extra_events=[{"at": H(0.1), "op": "add_qn", "count": 4, "memory_gb": 32}],
        new_tables=tables(30, 4, 80, 0.5), release=30),
    # Two resource groups with their own tables.
    day("day_08_two_resource_groups", "resource_groups", 12, 64,
        population(200, mb=70, rgs=["rg_a"]) + population(200, mb=70, rgs=["rg_b"]),
        resource_groups=[{"name": "rg_a", "nodes": 6}, {"name": "rg_b", "nodes": 6}],
        scale_out=2, scale_in=2, rolling=2, release=40),
    # Multi-replica tables: a replica's nodes must each carry a fair part.
    day("day_09_multi_replica", "replicas", 10, 64, [tables(10, 20, 80, 0.5, replicas=2, flush_min=5, compact_min=20), tables(50, 20, 80, 0.5, replicas=2), tables(40, 10, 80, 0.5, replicas=3)],
        new_tables=tables(10, 20, 80, 0.5, replicas=2), release=10),
    # A few tables ingest hard all day beside quiet ones.
    day("day_10_hot_ingest_tables", "ingest", 8, 64,
        [tables(4, 30, 300, 0.3, flush_min=0.5, compact_min=4), tables(60, 8, 300, 0.5)],
        new_tables=tables(10, 8, 300, 0.5), release=10),
    # High memory pressure: nodes at ~75% before the day starts.
    day("day_11_high_memory_pressure", "pressure", 8, 64, [tables(10, 60, 100, 0.4, flush_min=5, compact_min=20), tables(30, 60, 100, 0.4), tables(200, 4, 50, 0.5)],
        scale_out=2, scale_in=1, rolling=2, new_tables=tables(10, 10, 100, 0.4), release=20),
    # Streaming mode (delegators on streaming nodes), as 2.6+ clusters run.
    day("day_12_streaming_mode", "streaming", 12, 64, population(500, mb=60), streaming=True, sns=3,
        new_tables=tables(50, 4, 60, 0.5), release=50),
    # Mid-size cluster, mid-size tables, several replicas and 4 channels: the common SaaS shape.
    day("day_13_typical_saas", "typical", 16, 64, population(800, mb=40, replicas2=0.2) + [tables(10, 60, 50, 0.5, channels=4, replicas=2, flush_min=3, compact_min=12)],
        scale_out=4, scale_in=2, rolling=4, new_tables=tables(60, 4, 40, 0.5), release=80),
]


def main():
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cases")
    for c in CASES:
        path = os.path.join(out, c["name"] + ".json")
        with open(path, "w") as f:
            json.dump(c, f, indent=1)
        print(path)


if __name__ == "__main__":
    main()
