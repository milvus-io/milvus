"""Launch a retained, loopback Milvus cluster for public mutation acceptance."""

import argparse
import json
import os
import socket
import subprocess
import time
from pathlib import Path

if not __debug__:
    raise RuntimeError("PK update acceptance requires Python assertion checks; unset PYTHONOPTIMIZE and omit -O")

ROLES = ("mixcoord", "datanode", "querynode", "streamingnode", "proxy")
PORTS = ("rootcoord.port", "datanode.port", "querynode.port", "streamingnode.port", "proxy.port", "proxy.internalPort")


def free_ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in sockets]
    finally:
        for sock in sockets:
            sock.close()


def create(site, binary, configs):
    site.mkdir(parents=True, exist_ok=False)
    ns = f"pk-update-{time.time_ns()}"
    ports = free_ports(len(PORTS) + len(ROLES))
    conf = dict(zip(PORTS, ports))
    conf.update(
        {
            "etcd.endpoints": "127.0.0.1:2379",
            "etcd.rootPath": ns,
            "minio.address": "127.0.0.1",
            "minio.bucketName": ns,
            "minio.rootPath": "files",
            "woodpecker.meta.prefix": ns,
            "common.chanNamePrefix.cluster": ns,
            "mq.type": "woodpecker",
            "woodpecker.storage.type": "minio",
            "common.storage.useLoonFFI": "true",
            "dataCoord.index.writeSegmentIndexToManifest": "true",
            "rootCoord.dmlChannelNum": 2,
            "dataCoord.compaction.enableAutoCompaction": "false",
        }
    )
    for component in ("proxy", "rootCoord", "dataNode", "queryNode", "streamingNode"):
        conf[component + ".ip"] = "127.0.0.1"
    manifest = {
        "binary": str(binary.resolve(strict=True)),
        "configs": str(configs.resolve(strict=True)),
        "namespace": ns,
        "conf": conf,
        "metrics": dict(zip(ROLES, ports[len(PORTS) :])),
    }
    (site / "site.json").write_text(json.dumps(manifest, indent=2))
    for role in ROLES:
        launch(site, role)


def launch(site, role, binary=None, points=None):
    m = json.loads((site / "site.json").read_text())
    conf = {**m["conf"], "localStorage.path": str((site / role / "data").resolve())}
    env = {
        **os.environ,
        **{"MILVUS_CONF_" + k.upper().replace(".", "_"): str(v) for k, v in conf.items()},
        "METRICS_PORT": str(m["metrics"][role]),
        "MILVUSCONF": m["configs"],
        "GOMAXPROCS": "2",
        "OMP_NUM_THREADS": "2",
        "NO_PROXY": "*",
        "no_proxy": "*",
    }
    binary = str(Path(binary or m["binary"]).resolve(strict=True))
    if points:
        env["GO_FAILPOINTS"] = points
    path = site / role
    path.mkdir(exist_ok=True)
    log = path / f"{time.time_ns()}.log"
    with log.open("xb") as sink:
        process = subprocess.Popen(
            [binary, "run", role, "-alias", m["namespace"] + "-" + role],
            cwd=path,
            stdin=subprocess.DEVNULL,
            stdout=sink,
            stderr=sink,
            env=env,
            start_new_session=True,
        )
    with (site / "processes.jsonl").open("a") as stream:
        stream.write(
            json.dumps(
                {"role": role, "pid": process.pid, "binary": binary, "log": str(log), "timestamp_ns": time.time_ns()}
            )
            + "\n"
        )

    return process


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("site", type=Path)
    parser.add_argument("binary", type=Path)
    parser.add_argument("configs", type=Path)
    args = parser.parse_args()
    create(args.site.resolve(), args.binary, args.configs)
