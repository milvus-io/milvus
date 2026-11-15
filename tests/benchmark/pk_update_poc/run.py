# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run static R1-ON/R1-OFF/R2 correctness and latency comparisons locally."""

import argparse
import json
import logging
import math
import re
import statistics
import subprocess
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from pymilvus import Collection, CollectionSchema, DataType, FieldSchema, connections, utility
from workload import CASES, batches, bind_updates, generate, records, validate_fixture

LOG = logging.getLogger("pk-update-poc")
MODES = ("R1-ON", "R1-OFF", "R2")


def docker(*args):
    return subprocess.run(["docker", *map(str, args)], check=True, capture_output=True, text=True).stdout


class Instance:
    """Own only the uniquely named container and data under this run directory."""

    def __init__(self, args, mode):
        self.args, self.mode = args, mode
        self.directory = args.work_dir / mode
        self.directory.mkdir()
        self.name = f"pk-update-poc-{args.work_dir.name.lower()}-{mode.lower()}"
        self.started = False
        self.config(0)
        (self.directory / "embedEtcd.yaml").write_text(
            "listen-client-urls: http://0.0.0.0:2379\n"
            "advertise-client-urls: http://0.0.0.0:2379\n"
            "quota-backend-bytes: 4294967296\n"
            "auto-compaction-mode: revision\n"
            "auto-compaction-retention: '1000'\n"
        )
        (self.directory / "data").mkdir(mode=0o777)
        (self.directory / "data").chmod(0o777)

    def config(self, collection_id):
        (self.directory / "user.yaml").write_text(
            "dataCoord:\n"
            "  enableCompaction: false\n"
            "  segment:\n"
            "    maxSize: 2048\n"
            "    sealProportion: 1.0\n"
            "    sealProportionJitter: 0\n"
            "queryNode:\n"
            "  exprCache:\n"
            "    enabled: false\n"
            "  enableSegmentPrune: false\n"
            "  pkUpdatePOC:\n"
            f"    collectionID: {collection_id}\n"
            "    updatesFile: /poc/updates.jsonl\n"
            f"    enabled_temp_index: {str(self.mode == 'R1-ON').lower()}\n"
        )

    def start(self):
        LOG.info("Starting %s with image %s", self.mode, self.args.image)
        docker(
            "run",
            "-d",
            "--name",
            self.name,
            "--security-opt",
            "seccomp=unconfined",
            "--ulimit",
            "core=0",
            "--cpus",
            self.args.cpus,
            "--memory",
            self.args.memory,
            "-e",
            "ETCD_USE_EMBED=true",
            "-e",
            "MILVUSCONF=/milvus/configs",
            "-e",
            "ETCD_DATA_DIR=/var/lib/milvus/etcd",
            "-e",
            "ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml",
            "-e",
            "COMMON_STORAGETYPE=local",
            "-e",
            "DEPLOY_MODE=STANDALONE",
            "-v",
            f"{self.directory}/data:/var/lib/milvus",
            "-v",
            f"{self.directory}/user.yaml:/milvus/configs/user.yaml:ro",
            "-v",
            f"{self.directory}/embedEtcd.yaml:/milvus/configs/embedEtcd.yaml:ro",
            "-v",
            f"{self.directory}:/poc:ro",
            "-p",
            f"127.0.0.1:{self.args.port}:19530",
            "-p",
            f"127.0.0.1:{self.args.health_port}:9091",
            self.args.image,
            "milvus",
            "run",
            "standalone",
        )
        self.started = True
        self.wait()

    def wait(self):
        deadline = time.monotonic() + self.args.timeout
        while time.monotonic() < deadline:
            state = json.loads(docker("inspect", "--format", "{{json .State}}", self.name))
            if not state["Running"]:
                raise RuntimeError(f"{self.mode} exited: {state}")
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{self.args.health_port}/healthz", timeout=3) as response:
                    if response.status == 200:
                        return
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                pass
            time.sleep(1)
        raise TimeoutError(f"{self.mode} did not become healthy in {self.args.timeout}s")

    def restart(self):
        connections.disconnect("default")
        docker("restart", "-t", "30", self.name)
        self.wait()

    def logs(self):
        result = subprocess.run(["docker", "logs", self.name], capture_output=True, text=True, check=True)
        return result.stdout + result.stderr

    def wait_logs(self, predicate, description):
        # Log delivery can lag behind a successfully completed RPC.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            logs = self.logs()
            if predicate(logs):
                return logs
            time.sleep(1)
        raise AssertionError(f"Missing server log evidence: {description}")

    def stop(self):
        if self.started:
            connections.disconnect("default")
            docker("stop", "-t", "30", self.name)
            (self.directory / "server.log").write_text(self.logs())


def connect(args):
    connections.connect(host="127.0.0.1", port=args.port, timeout=args.timeout)


def log_number(line, name):
    match = re.search(name + r'["= :]+(\d+)', line)
    return int(match[1]) if match else None


def prepare(instance, fixture, manifest):
    args = instance.args
    connect(args)
    schema = CollectionSchema(
        [
            FieldSchema("pk", DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema("age", DataType.INT64, nullable=True),
            FieldSchema("name", DataType.VARCHAR, max_length=32, nullable=True),
            FieldSchema("vector", DataType.FLOAT_VECTOR, dim=8),
        ],
        enable_dynamic_field=False,
    )
    collection = Collection("pk_update_poc", schema, shards_num=1, consistency_level="Strong")
    path = fixture / ("mirror.jsonl" if instance.mode == "R2" else "base.jsonl")
    max_insert_ts = 0
    for batch in batches(path, args.insert_batch):
        result = collection.insert(batch, timeout=args.timeout)
        if result.insert_count != len(batch):
            raise AssertionError("Incomplete insert")
        max_insert_ts = max(max_insert_ts, result.timestamp)
    collection.flush(timeout=args.timeout)
    if collection.num_entities != manifest["rows"] or len(collection.partitions) != 1:
        raise AssertionError("Unexpected persisted row count or partition layout")
    for field in ("age", "name"):
        collection.create_index(field, {"index_type": "INVERTED"}, timeout=args.timeout)
    collection.create_index("vector", {"index_type": "FLAT", "metric_type": "L2"}, timeout=args.timeout)

    # The phase-4 collection ID config is read at load time and is not dynamic.
    # Restart the isolated instance after discovering its real schema IDs.
    description = connections._fetch_handler("default").describe_collection(collection.name)
    field_ids = {field["name"]: field["field_id"] for field in description["fields"]}
    applied_ts = max_insert_ts
    pending_file = None
    if instance.mode != "R2":
        applied_ts = bind_updates(
            fixture, instance.directory / "updates.jsonl", description["collection_id"], field_ids, max_insert_ts
        )
        # The injector rejects future timestamps. Do not change query clocks.
        while time.time_ns() // 1_000_000 <= applied_ts >> 18:
            time.sleep(0.01)
        if instance.mode == "R1-ON":
            pending_file = instance.directory / "updates.pending"
            (instance.directory / "updates.jsonl").replace(pending_file)
        instance.config(description["collection_id"])
    instance.restart()
    connect(args)
    collection = Collection(collection.name)

    def load_and_query():
        collection.load(timeout=args.timeout)
        # QueryView can prepare physical registrations lazily.
        collection.query("pk >= 0", output_fields=["count(*)"], timeout=args.timeout, consistency_level="Strong")

    if pending_file:
        # Exercise missing-file retry without adding benchmark query cases.
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(load_and_query)
            deadline = time.monotonic() + args.timeout
            while time.monotonic() < deadline:
                if future.done():
                    future.result()
                    raise AssertionError("POC load unexpectedly completed without its updates file")
                if instance.logs().count("PK-update POC file missing; retrying in 3 seconds") >= 2:
                    pending_file.replace(instance.directory / "updates.jsonl")
                    LOG.info("Verified missing-file retry; atomically published R1-ON updates")
                    break
                time.sleep(1)
            else:
                raise TimeoutError("Missing-file retry was not observed")
            future.result(timeout=args.timeout)
    else:
        load_and_query()
    infos = utility.get_query_segment_info(collection.name, timeout=args.timeout)
    segments = {info.segmentID: info for info in infos}
    if len(segments) != 1 or sum(info.num_rows for info in segments.values()) != manifest["rows"]:
        raise AssertionError(f"Expected one loaded segment with {manifest['rows']} rows, got {infos}")
    segment = next(iter(segments.values()))
    # common.proto SegmentState.Sealed=3; Growing=2 must never pass this gate.
    if segment.state != 3:
        raise AssertionError(f"Expected a sealed segment, got {segment}")
    if instance.mode != "R2":
        logs = instance.wait_logs(
            lambda logs: any(
                "PK-update POC ready" in line
                and log_number(line, "appliedThroughTimestamp") == applied_ts
                and log_number(line, "appliedCount") == manifest["operations"]
                and log_number(line, "matchedRows") == manifest["row_updates"]
                for line in logs.splitlines()
            ),
            "ready with expected timestamp, operation count and matched rows",
        )
        expected_mode = str(instance.mode == "R1-ON").lower()
        if not any(
            "PK-update POC preparing" in line and re.search(r"enabled_temp_index[\"= :]+%s" % expected_mode, line)
            for line in logs.splitlines()
        ):
            raise AssertionError("Server did not confirm the requested temp-index mode")
    elif "PK-update POC ready" in instance.logs():
        raise AssertionError("R2 unexpectedly injected POC updates")
    LOG.info(
        "%s ready: segment=%s rows=%s applied_through=%s",
        instance.mode,
        segment.segmentID,
        manifest["rows"],
        applied_ts,
    )
    return collection, {
        "collection_id": description["collection_id"],
        "segment_id": segment.segmentID,
        "applied_through": applied_ts,
    }


def verify(collection, fixture, timeout):
    """Compare complete PK sets for every filter; lookup values for equality only."""
    for case, expression, _ in CASES:
        expected = {row["pk"]: row for row in records(fixture / "expected" / f"{case}.jsonl")}
        fields = ["pk", "age", "name"] if case == "eq" else ["pk"]
        iterator = collection.query_iterator(
            expr=expression, output_fields=fields, batch_size=1000, timeout=timeout, consistency_level="Strong"
        )
        seen = set()
        try:
            while page := iterator.next():
                for row in page:
                    pk = row["pk"]
                    if pk in seen or pk not in expected:
                        raise AssertionError(f"{case}: unexpected/duplicate PK {pk}")
                    if case == "eq" and any(field not in row or row[field] != expected[pk][field] for field in fields):
                        raise AssertionError(f"{case}: lookup mismatch: {row} != {expected[pk]}")
                    seen.add(pk)
        finally:
            iterator.close()
        if len(seen) != len(expected):
            missing = list(expected.keys() - seen)[:10]
            raise AssertionError(f"{case}: missing {len(expected) - len(seen)} PKs, examples={missing}")
        count = collection.query(expression, output_fields=["count(*)"], timeout=timeout, consistency_level="Strong")[
            0
        ]["count(*)"]
        if count != len(expected):
            raise AssertionError(f"{case}: count={count}, expected={len(expected)}")
        LOG.info("Verified %s: %d matching rows", case, count)


def benchmark(collection, manifest, args):
    tests = [(name, expr, ["count(*)"], None) for name, expr, _ in CASES]
    tests += [("eq_pk", "age == 42", ["pk"], 128), ("eq_lookup", "age == 42", ["pk", "age", "name"], 128)]
    output = {}
    for name, expression, fields, limit in tests:
        kwargs = {"limit": limit} if limit else {}
        latencies = []
        for i in range(args.warmup + args.repeats):
            start = time.perf_counter_ns()
            result = collection.query(
                expression, output_fields=fields, timeout=args.timeout, consistency_level="Strong", **kwargs
            )
            elapsed = (time.perf_counter_ns() - start) / 1_000_000
            if limit:
                if len(result) != min(limit, manifest["counts"]["eq"]):
                    raise AssertionError(f"{name}: unexpected result size")
            elif result[0]["count(*)"] != manifest["counts"][name]:
                raise AssertionError(f"{name}: results changed during measurement")
            if i >= args.warmup:
                latencies.append(elapsed)
        sorted_latencies = sorted(latencies)
        output[name] = {
            "median_ms": statistics.median(latencies),
            "p95_ms": sorted_latencies[math.ceil(len(latencies) * 0.95) - 1],
            "samples_ms": latencies,
        }
        LOG.info("Measured %s: median=%.3f ms p95=%.3f ms", name, output[name]["median_ms"], output[name]["p95_ms"])
    return output


def structure_stats(logs, stage):
    samples = []
    for line in logs.splitlines():
        if "PK-update POC statistics" not in line or not re.search(r'stage["= :]+%s' % stage, line):
            continue
        match = re.search(r'stats="((?:\\.|[^"\\])*)"', line)
        if match:
            samples.append(json.loads(json.loads('"' + match[1] + '"')))
    return samples


def verify_principles(instance, collection, manifest):
    """Reject wrong paths/storage before accepting any latency samples."""
    if instance.mode == "R2":
        return {}
    logs = instance.wait_logs(
        lambda text: structure_stats(text, "initialized") and structure_stats(text, "applied"),
        "initial and applied sparse-storage statistics",
    )
    initial = structure_stats(logs, "initialized")[-1]
    applied = structure_stats(logs, "applied")[-1]
    expected_index_rows = 2 * manifest["rows"] if instance.mode == "R1-ON" else 0
    assert initial["updated_cells"] == initial["update_versions"] == initial["base_rows"] == 0, initial
    assert initial["base_index_rows"] == expected_index_rows, initial
    assert applied["updated_cells"] == 2 * len(range(0, manifest["rows"], 5)), applied
    assert applied["update_versions"] == 2 * manifest["row_updates"], applied
    assert applied["base_index_rows"] == expected_index_rows, applied
    evidence = {"initialized": initial, "applied": applied, "paths": {}}

    def run_case(name, expression, expected):
        time.sleep(0.3)  # Keep rate-limited diagnostic logs outside timing runs.
        old_logs = instance.logs()
        before = (structure_stats(old_logs, "query") or [applied])[-1]
        count = collection.query(
            expression, output_fields=["count(*)"], timeout=instance.args.timeout, consistency_level="Strong"
        )[0]["count(*)"]
        assert count == expected, (name, count, expected)
        logs = instance.wait_logs(
            lambda text: len(structure_stats(text, "query")) > len(structure_stats(old_logs, "query")),
            f"per-query path evidence for {name}",
        )
        after = structure_stats(logs, "query")[-1]
        diff = {key: after[key] - before[key] for key in after}
        evidence["paths"][name] = diff
        return diff

    equality = run_case("equality", "age == 42", manifest["counts"]["eq"])
    assert equality["base_rows"] == equality["value_rows"] == 0, equality
    if instance.mode == "R1-ON":
        assert equality["old_index_reads"] == equality["correction_rows"] == 0, equality
        assert 0 < equality["index_postings"] < manifest["rows"], equality
    else:
        assert equality["old_index_reads"] > 0 and equality["correction_rows"] > 0, equality
        assert equality["index_postings"] == 0, equality
    # All fixture values ending in "tail" start with "hot". General LIKE is
    # eligible for INVERTED; its current policy declines the pure suffix op.
    pattern = run_case("unsupported_temp_like", 'name like "hot%tail"', manifest["counts"]["suffix"])
    assert pattern["old_index_reads"] > 0 and pattern["correction_rows"] > 0, pattern
    assert pattern["base_rows"] == pattern["value_rows"] == pattern["index_postings"] == 0, pattern
    # Numeric leaves precede string LIKE in the existing conjunction order.
    # The fixture's tail values have age=7, so the intersection is empty while
    # the string reader must visit exactly the age=42 candidates.
    candidates = run_case("candidate_mask", 'age == 42 and name like "%tail"', 0)
    assert 0 < candidates["value_rows"] < manifest["rows"], candidates
    assert candidates["value_rows"] == manifest["counts"]["eq"], candidates
    assert candidates["base_rows"] <= candidates["value_rows"], candidates
    LOG.info("Validated sparse storage and expression paths for %s: %s", instance.mode, evidence)
    return evidence


def check_reader_usage(instance):
    if instance.mode == "R2":
        return {}
    logs = instance.wait_logs(
        lambda logs: any(
            "PK-update POC reader usage" in line
            and (log_number(line, "valueReads") or 0) > 0
            and ((log_number(line, "tempIndexReads") or 0) > 0) == (instance.mode == "R1-ON")
            for line in logs.splitlines()
        ),
        "reader counters for the selected mode",
    )
    lines = [line for line in logs.splitlines() if "PK-update POC reader usage" in line]
    counters = {}
    for key in ("tempIndexReads", "valueReads"):
        values = [int(value) for line in lines for value in re.findall(key + r'["= :]+(\d+)', line)]
        if not values:
            raise AssertionError(f"Missing {key} evidence in server logs")
        counters[key] = max(values)
    if (counters["tempIndexReads"] > 0) != (instance.mode == "R1-ON"):
        raise AssertionError(f"Wrong execution path for {instance.mode}: {counters}")
    if counters["valueReads"] == 0:
        raise AssertionError("No UpdatesRecord value-reader execution observed")
    return counters


def report(results, path):
    lines = [
        "# PK-update POC comparison",
        "",
        "Single-client, static latest-snapshot SDK latency (including RPC and Strong-consistency wait).",
        "",
        "| Case | R1-ON median ms | R1-OFF median ms | R2 median ms | ON/R2 | OFF/R2 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name in results["R2"]["latencies"]:
        values = [results[mode]["latencies"][name]["median_ms"] for mode in MODES]
        lines.append(
            f"| {name} | {values[0]:.3f} | {values[1]:.3f} | {values[2]:.3f} | {values[0] / values[2]:.2f} | {values[1] / values[2]:.2f} |"
        )
    lines += [
        "",
        "Full PK-set correctness passed for all eight filters in all three modes. Equality also verified updated output values.",
        "",
        "Small runs validate plumbing; their latency ratios are not production performance conclusions. No GC, old-snapshot or concurrent-update benchmark is included.",
    ]
    path.write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--image",
        required=True,
        help="Image containing all four implementation phases; never use an unmodified release",
    )
    parser.add_argument(
        "--work-dir", required=True, type=Path, help="New directory for isolated data, logs and reports"
    )
    parser.add_argument("--fixture", type=Path, help="Existing workload.py output (otherwise generate locally)")
    parser.add_argument("--rows", type=int, default=2000)
    parser.add_argument("--insert-batch", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--port", type=int, default=19540)
    parser.add_argument("--health-port", type=int, default=9095)
    parser.add_argument("--cpus", type=int, default=4)
    parser.add_argument("--memory", default="8g")
    args = parser.parse_args()
    if args.repeats < 1 or args.warmup < 0 or args.insert_batch < 1:
        parser.error("repeats and insert-batch must be positive; warmup must be nonnegative")
    args.work_dir = args.work_dir.resolve()
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*", args.work_dir.name):
        parser.error("work-dir basename must be a valid Docker name component")
    args.work_dir.mkdir(parents=True, exist_ok=False)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(args.work_dir / "runner.log")],
    )
    fixture = args.fixture.resolve() if args.fixture else args.work_dir / "fixture"
    if not args.fixture:
        generate(fixture, args.rows)
    manifest = validate_fixture(fixture)
    results = {}
    image = json.loads(docker("image", "inspect", args.image))[0]
    metadata = {
        "image_id": image["Id"],
        "repo_digests": image.get("RepoDigests"),
        "fixture": manifest,
        "config": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
    }
    (args.work_dir / "run.json").write_text(json.dumps(metadata, indent=2) + "\n")
    for mode in MODES:
        instance = Instance(args, mode)
        try:
            instance.start()
            collection, identity = prepare(instance, fixture, manifest)
            principles = verify_principles(instance, collection, manifest)
            verify(collection, fixture, args.timeout)
            latencies = benchmark(collection, manifest, args)
            # Counters are sampled on the first/each 100th request, rate-limited.
            # Emit evidence outside the measured loop even for tiny smoke runs.
            if mode != "R2":
                time.sleep(6)
                for _ in range(100):
                    collection.query(
                        "age == 42",
                        output_fields=["pk", "age"],
                        limit=1,
                        timeout=args.timeout,
                        consistency_level="Strong",
                    )
            results[mode] = {
                **identity,
                "latencies": latencies,
                "principles": principles,
                "reader_usage": check_reader_usage(instance),
                "container_usage": docker("stats", "--no-stream", "--format", "{{json .}}", instance.name).strip(),
            }
            (args.work_dir / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        finally:
            instance.stop()
    report(results, args.work_dir / "report.md")
    LOG.info("All three modes passed; report: %s", args.work_dir / "report.md")


if __name__ == "__main__":
    main()
