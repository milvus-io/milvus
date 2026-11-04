"""Repeat the saturated resident-Search comparison on the Proxy-owned build."""

import json
import subprocess
import time
from pathlib import Path

root = Path("artifacts/multisearch-proxy-refactor")
if any(root.glob("r[0-9]-*.json")):
    raise RuntimeError("Archive existing raw formal runs before rerunning")
names = ["multisearch-v305-stock", "multisearch-v305-multi", "multisearch-v305-refactor"]
original = {
    n: json.loads(subprocess.check_output(["docker", "inspect", n]))[0]["HostConfig"] for n in names
}
(root / "original-container-limits.json").write_text(
    json.dumps(
        {n: {k: v[k] for k in ["NanoCpus", "CpusetCpus"]} for n, v in original.items()}, indent=2
    )
)
base = [
    ".venv/bin/python",
    "examples/multisearch/mixed_bench.py",
    "--target",
    "localhost:29564",
    "--container",
    names[2],
    "--metrics-url",
    "http://localhost:19098/metrics",
    "--output-dir",
    str(root),
    "--fixture",
    str(root / "fixture.json"),
]
try:
    for name, cpus, quota in [
        (names[0], "68-71", 4),
        (names[1], "72-75", 4),
        (names[2], "64-65", 2),
    ]:
        subprocess.run(
            ["docker", "update", "--cpuset-cpus", cpus, "--cpus", str(quota), name],
            check=True,
            stdout=subprocess.DEVNULL,
        )
    subprocess.run(
        base + ["--name", "smoke", "--mode", "multi", "--rate", "160", "--smoke"], check=True
    )
    smoke = json.loads((root / "smoke.json").read_text())
    assert smoke["scheduled_groups"] == smoke["admitted_groups"] == 100
    assert smoke["secondary_total"] == 800
    assert (
        smoke["resident_errors_all"]
        == smoke["secondary_errors_all"]
        == smoke["dropped_groups"]
        == smoke["generator_late_groups"]
        == 0
    )
    for rep in range(2):
        for mode in ["search", "multi"] if rep == 0 else ["multi", "search"]:
            name = f"r{rep}-{mode}-rate480"
            print("START", name, flush=True)
            subprocess.run(base + ["--name", name, "--mode", mode, "--rate", "480"], check=True)
            time.sleep(2)
finally:
    for name, cfg in original.items():
        subprocess.run(
            [
                "docker",
                "update",
                "--cpus",
                str(cfg["NanoCpus"] / 1e9),
                "--cpuset-cpus",
                cfg["CpusetCpus"],
                name,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
