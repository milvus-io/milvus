"""Run the compiled Go load driver with Docker CPU/memory sampling."""

import argparse, http.client, json, socket, subprocess, time, urllib.request
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--cpus", type=int, default=4)
p.add_argument("--kind", choices=["stock", "multi"], required=True)
p.add_argument("--mode", choices=["search", "multi"], required=True)
p.add_argument("--workers", type=int, default=4)
p.add_argument("--duration", type=int, default=30)
p.add_argument("--warmup", type=int, default=5)
p.add_argument("--rate", type=float, default=0)
p.add_argument("--name", required=True)
a = p.parse_args()


class Docker(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect("/var/run/docker.sock")


def stats():
    c = Docker("localhost", timeout=5)
    c.request("GET", f"/containers/multisearch-v305-{a.kind}/stats?stream=false&one-shot=true")
    r = c.getresponse()
    data = json.load(r)
    c.close()
    return data


port, health = (29560, 19096) if a.kind == "stock" else (29561, 19097)
out = Path("artifacts/multisearch-v2") / (a.name + ".json")
cpuout = out.with_suffix(".resources.json")
cmd = [
    "taskset",
    "-c",
    "32-39",
    "bin/multisearch-load",
    "-target",
    f"localhost:{port}",
    "-mode",
    a.mode,
    "-workers",
    str(a.workers),
    "-warmup",
    f"{a.warmup}s",
    "-duration",
    f"{a.duration}s",
    "-rate",
    str(a.rate),
    "-out",
    str(out),
]
proc = subprocess.Popen(cmd)
samples = []
begin = time.monotonic()
previous = None
while proc.poll() is None:
    now = time.monotonic()
    s = stats()
    cpu = s["cpu_stats"]["cpu_usage"]["total_usage"]
    memory = s.get("memory_stats", {})
    row = {
        "elapsed_s": now - begin,
        "cpu_ns": cpu,
        "memory_bytes": memory.get("usage", 0) or 0,
        "throttling": s["cpu_stats"].get("throttling_data", {}),
    }
    if previous:
        row["cpu_utilization_of_limit"] = (cpu - previous[1]) / 1e9 / (now - previous[0]) / a.cpus
    previous = (now, cpu)
    try:
        metrics = (
            urllib.request.urlopen(f"http://localhost:{health}/metrics", timeout=2).read().decode()
        )
        for line in metrics.splitlines():
            if line.startswith(
                ("go_goroutines ", "go_threads ", "process_resident_memory_bytes ")
            ) or (line.startswith("milvus_proxy_queue_task_num{") and 'queue_type="dql"' in line):
                k, v = line.split()
                row[k] = float(v)
    except Exception as e:
        row["metrics_error"] = str(e)
    samples.append(row)
    time.sleep(max(0, 1 - (time.monotonic() - now)))
assert proc.wait() == 0
measured = [
    x
    for x in samples
    if a.warmup <= x["elapsed_s"] < a.warmup + a.duration and "cpu_utilization_of_limit" in x
]
summary = {
    "command": cmd,
    "cpu_limit": a.cpus,
    "cpuset": "64-67" if a.cpus == 4 else "64-65",
    "tmpfs_data_limit_gib": 4,
    "samples": samples,
    "measurement_samples": len(measured),
    "mean_cpu_utilization": sum(x["cpu_utilization_of_limit"] for x in measured)
    / max(1, len(measured)),
    "peak_memory_bytes": max((x.get("memory_bytes", 0) for x in measured), default=0),
    "peak_goroutines": max((x.get("go_goroutines", 0) for x in measured), default=0),
}
cpuout.write_text(json.dumps(summary, indent=2))
r = json.loads(out.read_text())
print(
    json.dumps(
        {
            "name": a.name,
            "qps": r["logical_success_qps"],
            "group_p99": r["groups"]["p99_ms"],
            "probe_p99": r["ordinary_search_probe"]["p99_ms"],
            "cpu_util": summary["mean_cpu_utilization"],
            "errors": r["groups"]["logical_errors"],
            "drops": r["dropped_groups"],
        }
    ),
    flush=True,
)
