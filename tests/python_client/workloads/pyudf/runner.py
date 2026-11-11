"""One bounded entry point for retained-data PyUDF system workloads.

The scenario runner owns deployment and faults. This module owns data preparation,
Search assertions and load phases. Credentials are supplied through environment
variables; the configuration file contains only targets and workload parameters.
"""

import argparse
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from minio import Minio
from pymilvus import MilvusClient
from workloads.pyudf.workload import check_stop_file, prepare, resume


@dataclass(frozen=True)
class WorkloadConfig:
    proxy_uris: list[str]
    minio_endpoint: str
    bucket: str
    collection: str
    index_type: str
    expected_commit: str
    rows: int = 100000
    minio_secure: bool = False
    proxy_management_endpoints: list[str] | None = None

    def __post_init__(self):
        if len(self.proxy_uris) != 2 or len(set(self.proxy_uris)) != 2:
            raise ValueError("Two distinct Proxy endpoints are required")
        if not re.fullmatch(r"[0-9a-f]{7,40}", self.expected_commit):
            raise ValueError("expected_commit must identify the deployed Git commit")
        if not re.fullmatch(r"pyudf_[A-Za-z0-9_]+", self.collection):
            raise ValueError("An exclusively owned pyudf_ collection is required")
        if not 1000 <= self.rows <= 100000:
            raise ValueError("rows must be between 1000 and 100000")
        if self.index_type not in ("HNSW", "AUTOINDEX"):
            raise ValueError("index_type must be HNSW or AUTOINDEX")

    @classmethod
    def read(cls, root):
        return cls(**json.loads((root / "config.json").read_text()))

    def storage(self):
        return Minio(
            self.minio_endpoint,
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=self.minio_secure,
        )

    def connections(self):
        clients = []
        try:
            for uri in self.proxy_uris:
                client = MilvusClient(uri, token=os.environ["MILVUS_TOKEN"])
                clients.append(client)
                version = client.get_server_version(detail=True)
                assert version["git_commit"].startswith(self.expected_commit), version
            return clients
        except BaseException:
            for client in clients:
                client.close()
            raise


def save(path, value):
    path.write_text(json.dumps(value, indent=2))
    print(json.dumps(value), flush=True)


def load_checker(root):
    config = WorkloadConfig.read(root)
    clients = config.connections()
    try:
        checker = resume(clients[0], config.storage(), config, root / "input")
        return checker, clients
    except BaseException:
        for client in clients:
            client.close()
        raise


def initialize(root):
    config = WorkloadConfig.read(root)
    inputs = root / "input"
    inputs.mkdir()
    clients = config.connections()
    try:
        checker = prepare(clients[0], config.storage(), config, inputs)
        checker.baseline()
        checker.evidence = root
        save(root / "pre-soak-audit.json", checker.audit_all_rows())
        for number, client in enumerate(clients):
            for sequence in range(1, 21):
                checker.once(client, sequence)
            save(root / f"proxy-{number}-version.json", client.get_server_version(detail=True))
        save(root / "ready.json", {"event": "ready", "unix": time.time(), "collection": config.collection})
    finally:
        for client in clients:
            client.close()


def paced(checker, clients, output, duration):
    """Split scheduled slots exactly 50/50; retain all failures and skipped slots."""
    output.mkdir()
    checker.evidence = output
    results, failures, skipped = [], [], [0] * 8
    lock = threading.Lock()
    stop = threading.Event()
    started, wall = time.monotonic(), time.time()
    active = peak = 0
    area = 0.0
    changed = started
    journal = (output / "requests.jsonl").open("w")
    save(output / "started.json", {"event": output.name + "_started", "unix": wall, "duration": duration})

    def worker(number):
        nonlocal active, peak, area, changed
        sequence = number
        while sequence / 20 < duration and not stop.is_set():
            if stop.wait(max(0, started + sequence / 20 - time.monotonic())):
                break
            if time.monotonic() >= started + duration:
                break
            with lock:
                now = time.monotonic()
                area += active * (now - changed)
                changed = now
                active += 1
                peak = max(peak, active)
            try:
                check_stop_file(output.parent / "STOP")
                item = checker.once(clients[number % 2], sequence)
                item.update(proxy=number % 2, elapsed=time.monotonic() - started)
                with lock:
                    results.append(item)
                    journal.write(json.dumps(item) + "\n")
            except Exception as error:
                with lock:
                    failures.append(
                        {"sequence": sequence, "proxy": number % 2, "type": type(error).__name__, "error": str(error)}
                    )
                stop.set()
            finally:
                with lock:
                    now = time.monotonic()
                    area += active * (now - changed)
                    changed = now
                    active -= 1
            sequence += 8
            while started + sequence / 20 < time.monotonic():
                skipped[number] += 1
                sequence += 8

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(worker, number) for number in range(8)]
        pending = set(futures)
        while pending:
            # A request failure sets stop immediately; reporting still waits for
            # in-flight requests or the next interval instead of busy-looping.
            _, pending = wait(pending, timeout=10)
            with lock:
                journal.flush()
                print(
                    json.dumps(
                        {
                            "event": "progress",
                            "phase": output.name,
                            "elapsed": time.monotonic() - started,
                            "requests": len(results),
                            "failures": failures,
                        }
                    ),
                    flush=True,
                )
        for future in futures:
            future.result()
    journal.close()
    elapsed = time.monotonic() - started
    summary = {
        "event": output.name + "_summary",
        "started_unix": wall,
        "requested_seconds": duration,
        "elapsed": elapsed,
        "requests": len(results),
        "pyudf_requests": sum(r["kind"] == "pyudf" for r in results),
        "per_proxy": [sum(r["proxy"] == p for r in results) for p in range(2)],
        "skipped_slots": sum(skipped),
        "peak_inflight": peak,
        "time_weighted_inflight": area / elapsed,
        "latency_ms_p50_p95_p99": np.percentile([r["latency_ms"] for r in results], [50, 95, 99]).tolist()
        if results
        else [],
        "failures": failures,
        "outcome": "PASS" if not failures and elapsed >= duration - 1 and results else "FAIL",
    }
    save(output / "summary.json", summary)
    assert summary["outcome"] == "PASS", summary
    return summary


def soak(root, duration=7200, drain=300, replay=600):
    checker, clients = load_checker(root)
    try:
        paced(checker, clients, root / "soak", duration)
        checker.evidence = root
        save(root / "post-soak-audit.json", checker.audit_all_rows())
        save(root / "drain-started.json", {"unix": time.time(), "seconds": drain})
        time.sleep(drain)
        save(root / "drain-ended.json", {"unix": time.time()})
        paced(checker, clients, root / "replay", replay)
        checker.evidence = root
        save(root / "post-replay-audit.json", checker.audit_all_rows())
    finally:
        for client in clients:
            client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "phase", choices=("initialize", "soak", "performance", "interference", "cache", "cleanup", "concurrency")
    )
    parser.add_argument(
        "--evidence",
        type=Path,
        required=True,
        help="Owned directory containing config.json and the retained input ledger",
    )
    parser.add_argument("--duration", type=int, default=7200)
    parser.add_argument("--drain", type=int, default=300)
    parser.add_argument("--replay", type=int, default=600)
    parser.add_argument("--cache-phase", choices=("baseline", "cold", "replay", "remove", "recovery"))
    args = parser.parse_args()
    if not 0 < args.duration <= 7200 or not 0 <= args.drain <= 300 or not 0 < args.replay <= 600:
        parser.error("Durations exceed the bounded workload limits")
    if args.phase == "cache" and not args.cache_phase:
        parser.error("cache requires --cache-phase")
    WorkloadConfig.read(args.evidence)
    if args.phase == "initialize":
        initialize(args.evidence)
    elif args.phase == "soak":
        soak(args.evidence, args.duration, args.drain, args.replay)
    else:
        from workloads.pyudf import stress

        if args.phase == "cache":
            stress.cache(args.evidence, args.cache_phase)
        else:
            {
                "performance": stress.performance,
                "interference": stress.interference,
                "cleanup": stress.cleanup,
                "concurrency": stress.concurrency,
            }[args.phase](args.evidence)


if __name__ == "__main__":
    main()
