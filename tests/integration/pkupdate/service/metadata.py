"""Capture the retained site's catalog references and decode their delta objects."""

import json
import os
import subprocess
import sys
from pathlib import Path

from cluster_site import ROLES


def capture(site, label, timeout=90):
    m = json.loads((site / "site.json").read_text())
    if set(m["metrics"]) != set(ROLES):
        raise ValueError("invalid retained site role inventory")
    path = site / (label + ".jsonl")
    cfg = {**m["conf"], "localStorage.path": str(site / "inspector/data")}
    env = {
        **os.environ,
        **{"MILVUS_CONF_" + k.upper().replace(".", "_"): str(v) for k, v in cfg.items()},
        "MILVUSCONF": m["configs"],
        "NO_PROXY": "*",
        "no_proxy": "*",
    }
    binary = Path(m["binary"]).with_name("inspect")
    # Native libraries and service logging can also write to stdout.
    with path.with_suffix(".log").open("x") as log:
        proc = subprocess.run([binary], env=env, cwd=site, stdout=log, stderr=subprocess.STDOUT, timeout=timeout)
    with path.open("x") as out, path.with_suffix(".log").open() as log:
        for line in log:
            if line.startswith('{"') and ('"key":' in line or '"file":' in line):
                out.write(line)
    proc.check_returncode()
    return [json.loads(x) for x in path.read_text().splitlines()]


if __name__ == "__main__":
    capture(Path(sys.argv[1]).resolve(), sys.argv[2])
