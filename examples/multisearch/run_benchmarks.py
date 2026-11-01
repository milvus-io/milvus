import json, subprocess, time
from pathlib import Path

root = Path.cwd()
log = root / ".cache/benchmark-suite.log"
variants = [("stock", "search"), ("multi", "search"), ("multi", "multi")]


def set_active(kind):
    for other in ["stock", "multi"]:
        cpus = "64-67" if other == kind else "68-71"
        subprocess.run(
            ["docker", "update", "--cpuset-cpus", cpus, "multisearch-v305-" + other],
            check=True,
            stdout=subprocess.DEVNULL,
        )


def run(kind, mode, workers, name, rate=0, duration=30):
    set_active(kind)
    time.sleep(2)
    cmd = [
        ".venv/bin/python",
        "examples/multisearch/bench.py",
        "--kind",
        kind,
        "--mode",
        mode,
        "--workers",
        str(workers),
        "--duration",
        str(duration),
        "--warmup",
        "5",
        "--name",
        name,
        "--rate",
        str(rate),
    ]
    print("START", name, flush=True)
    subprocess.run(cmd, check=True)


try:
    for rep in range(2):
        for workers in [1, 8, 16]:
            order = variants if rep == 0 else variants[::-1]
            for kind, mode in order:
                run(kind, mode, workers, f"{kind}-{mode}-c{workers*8}-r{rep}")
    # Match offered load separately from the closed-loop capacity comparisons.
    for rate in [60, 150]:
        for kind, mode in variants:
            run(kind, mode, 16, f"{kind}-{mode}-rate{rate*8}", rate=rate)
finally:
    for kind, cpus in [("stock", "64-67"), ("multi", "68-71")]:
        subprocess.run(
            ["docker", "update", "--cpuset-cpus", cpus, "multisearch-v305-" + kind],
            check=True,
            stdout=subprocess.DEVNULL,
        )
