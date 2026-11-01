"""Monitor a mixed-load run without changing the tested server binary."""

import argparse
import http.client
import json
import socket
import subprocess
import time
import urllib.request
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--name", required=True)
p.add_argument("--mode", choices=["none", "search", "multi"], required=True)
p.add_argument("--rate", type=int, default=0)
p.add_argument("--smoke", action="store_true")
a = p.parse_args()
root = Path("artifacts/multisearch-mixed")
root.mkdir(exist_ok=True, parents=True)


class Docker(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect("/var/run/docker.sock")


def stats():
    c = Docker("localhost", timeout=5)
    c.request("GET", "/containers/multisearch-v305-multi/stats?stream=false&one-shot=true")
    r = c.getresponse()
    assert r.status == 200
    data = json.load(r)
    c.close()
    return data


cmd = [
    "taskset",
    "-c",
    "32-39",
    "bin/multisearch-mixed-load",
    "-mode",
    a.mode,
    "-rate",
    str(a.rate),
    "-out",
    str(root / (a.name + ".json")),
]
# Headroom for fixed offered load; the closed-loop contention case stays at 16.
if a.rate >= 480:
    cmd += ["-groups", "32"]
if a.smoke:
    cmd += ["-warmup", "1s", "-baseline", "4s", "-mixed", "5s", "-recovery", "4s", "-trim", "1s"]
proc = subprocess.Popen(cmd)
samples = []
previous = None
while proc.poll() is None:
    now = time.time()
    s = stats()
    cpu = s["cpu_stats"]["cpu_usage"]["total_usage"]
    row = {
        "unix_s": now,
        "cpu_ns": cpu,
        "memory_bytes": s.get("memory_stats", {}).get("usage", 0),
        "throttling": s["cpu_stats"].get("throttling_data", {}),
    }
    if previous:
        row["cpu_utilization"] = (cpu - previous[1]) / 1e9 / (now - previous[0]) / 2
    previous = (now, cpu)
    try:
        metrics = (
            urllib.request.urlopen("http://localhost:19097/metrics", timeout=2).read().decode()
        )
        for line in metrics.splitlines():
            if line.startswith(
                ("go_goroutines ", "go_threads ", "process_resident_memory_bytes ")
            ) or (line.startswith("milvus_proxy_queue_task_num{") and 'queue_type="dql"' in line):
                k, v = line.split()
                row[k] = float(v)
    except Exception as exc:
        row["metrics_error"] = str(exc)
    samples.append(row)
    time.sleep(max(0, 1 - (time.time() - now)))
assert proc.wait() == 0
r = json.loads((root / (a.name + ".json")).read_text())
resource = {"command": cmd, "cpu_limit": 2, "cpuset": "64-65", "samples": samples, "phases": {}}
for name, window in r["resident"].items():
    ss = [
        s
        for s in samples
        if window["window_start_s"] <= s["unix_s"] - r["start_unix_s"] < window["window_end_s"]
        and "cpu_utilization" in s
    ]
    assert ss, "missing resource samples"
    resource["phases"][name] = {
        "samples": len(ss),
        "mean_cpu_utilization": sum(s["cpu_utilization"] for s in ss) / len(ss),
        "peak_goroutines": max(s.get("go_goroutines", 0) for s in ss),
        "peak_go_threads": max(s.get("go_threads", 0) for s in ss),
        "peak_memory_bytes": max(s.get("memory_bytes", 0) for s in ss),
    }
(root / (a.name + ".resources.json")).write_text(json.dumps(resource, indent=2))
print(
    json.dumps(
        {
            "name": a.name,
            "resident_qps": {k: round(v["success_qps"], 1) for k, v in r["resident"].items()},
            "resident_p99": {k: v["p99_ms"] for k, v in r["resident"].items()},
            "secondary_qps": r["secondary"]["mixed"]["success_qps"],
            "secondary_p99": r["secondary"]["mixed"]["p99_ms"],
            "cpu": {k: round(v["mean_cpu_utilization"], 3) for k, v in resource["phases"].items()},
            "errors": r["resident_errors_all"] + r["secondary_errors_all"],
            "drops": r["dropped_groups"],
            "generator_late": r["generator_late_groups"],
        }
    ),
    flush=True,
)
