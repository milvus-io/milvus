import subprocess, time

variants = [("stock", "search"), ("multi", "search"), ("multi", "multi")]
try:
    for rep in range(2):
        for kind, mode in variants if rep == 0 else variants[::-1]:
            for other in ["stock", "multi"]:
                subprocess.run(
                    [
                        "docker",
                        "update",
                        "--cpus",
                        "2",
                        "--cpuset-cpus",
                        "64-65" if other == kind else "68-69",
                        "multisearch-v305-" + other,
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
            time.sleep(2)
            name = f"2cpu-{kind}-{mode}-c128-r{rep}"
            print("START", name, flush=True)
            subprocess.run(
                [
                    ".venv/bin/python",
                    "examples/multisearch/bench.py",
                    "--kind",
                    kind,
                    "--mode",
                    mode,
                    "--workers",
                    "16",
                    "--duration",
                    "45",
                    "--warmup",
                    "5",
                    "--cpus",
                    "2",
                    "--name",
                    name,
                ],
                check=True,
            )
finally:
    for kind, cpus in [("stock", "64-67"), ("multi", "68-71")]:
        subprocess.run(
            ["docker", "update", "--cpus", "4", "--cpuset-cpus", cpus, "multisearch-v305-" + kind],
            check=True,
            stdout=subprocess.DEVNULL,
        )
