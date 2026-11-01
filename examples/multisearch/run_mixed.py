"""Two reversed-order repetitions; ordinary Search stays active throughout each run."""

import json, subprocess, time
from pathlib import Path

root = Path("artifacts/multisearch-mixed")
root.mkdir(exist_ok=True, parents=True)
if any(root.glob("r[0-9]-*.json")):
    raise RuntimeError("Archive existing raw r*-*.json results before starting a fresh suite")
names = ["multisearch-v305-stock", "multisearch-v305-multi"]
original = {
    name: json.loads(subprocess.check_output(["docker", "inspect", name]))[0]["HostConfig"]
    for name in names
}
(root / "original-container-limits.json").write_text(
    json.dumps(
        {
            k: {"NanoCpus": v["NanoCpus"], "CpusetCpus": v["CpusetCpus"]}
            for k, v in original.items()
        },
        indent=2,
    )
)
try:
    subprocess.run(
        ["docker", "update", "--cpuset-cpus", "68-71", names[0]],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    subprocess.run(
        ["docker", "update", "--cpus", "2", "--cpuset-cpus", "64-65", names[1]],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    subprocess.run(
        [
            ".venv/bin/python",
            "examples/multisearch/mixed_bench.py",
            "--name",
            "smoke",
            "--mode",
            "multi",
            "--rate",
            "160",
            "--smoke",
        ],
        check=True,
    )
    smoke = json.loads((root / "smoke.json").read_text())
    assert (
        smoke["scheduled_groups"] == 100
        and smoke["admitted_groups"] == 100
        and smoke["secondary_total"] == 800
    )
    assert (
        smoke["resident_errors_all"]
        == smoke["secondary_errors_all"]
        == smoke["dropped_groups"]
        == smoke["generator_late_groups"]
        == 0
    )
    assert all(p["success_qps"] > 0 and p["samples"] > 0 for p in smoke["resident"].values())
    variants = [
        ("none", 0),
        ("search", 160),
        ("multi", 160),
        ("search", 480),
        ("multi", 480),
        ("search", 0),
        ("multi", 0),
    ]
    for rep in range(2):
        for mode, rate in variants if rep == 0 else variants[::-1]:
            label = (
                "none" if mode == "none" else f"{mode}-" + (f"rate{rate}" if rate else "closed128")
            )
            name = f"r{rep}-{label}"
            print("START", name, flush=True)
            subprocess.run(
                [
                    ".venv/bin/python",
                    "examples/multisearch/mixed_bench.py",
                    "--name",
                    name,
                    "--mode",
                    mode,
                    "--rate",
                    str(rate),
                ],
                check=True,
            )
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
