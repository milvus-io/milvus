"""Bounded PyUDF workload with retained inputs and independent result checks.

Run through ``pytest system_tests/pyudf`` from tests/python_client.
Credentials are read from the environment and are never included in artifacts.
The caller owns deployment, faults, resource monitoring, and eventual cleanup.
"""

import hashlib
import io
import json
import time
from pathlib import Path

import numpy as np
import pymilvus
from common.pyudf_helpers import build_wheel
from pymilvus import DataType, FunctionChain, FunctionChainStage
from pymilvus.function_chain import col
from pymilvus.function_chain.chain import FunctionChainExpr

SEED = 20261105
FIELDS = ["ts", "label", "flag", "optional"]
MODULE = """import os
import threading
import pyarrow as pa

class UDF:
    def __init__(self, context):
        self.context = context
        self.count = 0
        self.lock = threading.Lock()

    def transform_query(self, params, columns):
        with self.lock:
            self.count += 1
            count = self.count
        values = columns[0].to_pylist()
        mode = params.get("verify_mode")
        if mode == "repeated":
            if any(column is not columns[0] for column in columns):
                raise AssertionError("Repeated input arrays lost their identity")
        elif mode == "unique":
            ids = [(value - 7) // 3 for value in values]
            if columns[1].to_pylist() != [f"row_{pk}" for pk in ids]:
                raise AssertionError("String input lost row alignment")
            if columns[2].to_pylist() != [pk % 2 == 0 for pk in ids]:
                raise AssertionError("Boolean input lost row alignment")
            if columns[3].to_pylist() != [None if pk % 7 == 0 else pk for pk in ids]:
                raise AssertionError("Nullable input lost row alignment")
        result = [pa.array([v * params["factor"] + params["bias"] for v in values], type=pa.float32())]
        if count == 1 or count % 1000 == 0:
            self.context.logger.warning("PYUDF_QA_COMPLETE resource=%s pid=%s calls=%s rows=%s",
                                        self.context.resource_name, os.getpid(), count, len(values))
        return result

def create_udf(context):
    return UDF(context)
"""


class CandidateDrift(AssertionError):
    """A changed ANN candidate set needs diagnosis before attributing a UDF bug."""


def check_stop_file(path):
    """Stop submitting load when the scenario's resource guard requests it."""
    if path.exists():
        raise RuntimeError("Resource guard requested stop")


class PyUDFSearchChecker:
    """A reusable oracle for immutable collections; no fault injection or retries."""

    def __init__(self, client, collection, resources, vectors, queries, evidence, index_type, stop_file=None):
        self.client = client
        self.collection = collection
        self.resources = resources
        self.vectors = vectors
        self.queries = queries
        self.evidence = evidence
        self.index_type = index_type
        self.stop_file = Path(stop_file) if stop_file is not None else evidence / "STOP"
        self.candidates = []
        self.baseline_distances = []

    @staticmethod
    def chain(resource, factor, bias, final_limit=20):
        return (
            FunctionChain(FunctionChainStage.L2_RERANK)
            .map(
                "$score",
                FunctionChainExpr(
                    "py_udf",
                    args=(col("ts"),),
                    params={"resource_name": resource, "udf_params": {"factor": factor, "bias": bias}},
                ),
            )
            .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            .limit(final_limit)
        )

    def search_params(self, limit):
        params = {"search_list": max(100, limit)} if self.index_type == "DISKANN" else {"ef": max(256, limit)}
        return {"metric_type": "L2", "params": params}

    def search(self, client, query, chain=None, limit=100, filter_expr=""):
        check_stop_file(self.stop_file)
        options = {"function_chains": chain} if chain is not None else {}
        return client.search(
            self.collection,
            data=[query.tolist()],
            anns_field="vector",
            limit=limit,
            filter=filter_expr,
            search_params=self.search_params(limit),
            output_fields=FIELDS,
            consistency_level="Strong",
            timeout=30,
            **options,
        )[0]

    def verify_fields(self, hits):
        ids = [hit["id"] for hit in hits]
        assert len(ids) == len(set(ids)), f"Duplicate result IDs: {ids}"
        for hit in hits:
            pk = hit["id"]
            assert isinstance(pk, int) and 0 <= pk < len(self.vectors), hit
            expected = {
                "ts": pk * 3 + 7,
                "label": f"row_{pk}",
                "flag": pk % 2 == 0,
                "optional": None if pk % 7 == 0 else pk,
            }
            entity = hit.get("entity", {})
            assert all(field in entity for field in FIELDS), hit
            assert {field: entity.get(field) for field in FIELDS} == expected, hit
        return ids

    def verify_udf(self, hits, candidates, factor, bias):
        ids = self.verify_fields(hits)
        expected = sorted(candidates, reverse=True)[:20]
        assert len(ids) == len(expected), (ids, expected)
        for hit in hits:
            assert hit["distance"] == (hit["id"] * 3 + 7) * factor + bias, hit
        assert ids == sorted(ids, reverse=True), ids
        if ids != expected:
            raise CandidateDrift(f"Expected candidate-derived IDs {expected}, received {ids}")

    def baseline(self):
        recalls = []
        distance_errors = []
        for number, query in enumerate(self.queries):
            hits = self.search(self.client, query)
            ids = self.verify_fields(hits)
            assert len(ids) == 100, len(ids)
            self.candidates.append(ids)
            self.baseline_distances.append({hit["id"]: hit["distance"] for hit in hits})
            if number < 10:
                distances = np.sum((self.vectors - query) ** 2, axis=1)
                exact = set(np.argsort(distances)[:100].tolist())
                recalls.append(len(exact.intersection(ids)) / 100)
                distance_errors.append(max(abs(hit["distance"] - float(distances[hit["id"]])) for hit in hits))
        (self.evidence / "candidates.json").write_text(json.dumps(self.candidates))
        (self.evidence / "baseline_distances.json").write_text(json.dumps(self.baseline_distances))
        # Exhaustive small candidate search makes this check independent of ANN recall.
        query = self.queries[0]
        small = self.search(self.client, query, limit=257, filter_expr="id < 257")
        assert set(self.verify_fields(small)) == set(range(257))
        exact = self.search(self.client, query, self.chain(self.resources[0], 2, 5), limit=257, filter_expr="id < 257")
        self.verify_udf(exact, list(range(257)), 2, 5)
        return {
            "recall_at_100_samples": recalls,
            "ann_distance_max_absolute_error_samples": distance_errors,
            "exact_subset_rows": 257,
        }

    def verify_control(self, hits, query_index):
        ids = self.verify_fields(hits)
        assert len(ids) == 100
        for hit in hits:
            pk = hit["id"]
            assert np.isfinite(hit["distance"]) and hit["distance"] >= 0, hit
            if self.index_type == "AUTOINDEX":
                # Cardinal HNSW build_level=1 uses RBQ4/RBQM4 distances.
                # Measure approximation separately; require stable control results here.
                if pk not in self.baseline_distances[query_index]:
                    raise CandidateDrift(f"Control candidate {pk} was absent from the retained baseline")
                distance = self.baseline_distances[query_index][pk]
            else:
                distance = float(np.sum((self.vectors[pk] - self.queries[query_index]) ** 2))
            assert np.isclose(hit["distance"], distance, rtol=1e-5, atol=1e-6), hit

    def audit_all_rows(self):
        """Check every stored scalar and original vector against the retained input ledger."""
        started = time.monotonic()
        checked = 0
        for start in range(0, len(self.vectors), 1000):
            check_stop_file(self.stop_file)
            assert time.monotonic() - started < 180, "Full ledger audit exceeded its observation budget"
            end = min(start + 1000, len(self.vectors))
            rows = self.client.query(
                self.collection,
                filter=f"id >= {start} and id < {end}",
                output_fields=["id", "vector", *FIELDS],
                limit=1000,
                consistency_level="Strong",
                timeout=30,
            )
            assert {row["id"] for row in rows} == set(range(start, end)), (start, len(rows))
            self.verify_fields([{"id": row["id"], "entity": row} for row in rows])
            for row in rows:
                assert np.array_equal(np.asarray(row["vector"], dtype=np.float32), self.vectors[row["id"]]), row["id"]
            checked += len(rows)
            time.sleep(0.05)
        result = {
            "event": "full_ledger_audit",
            "rows": checked,
            "elapsed": time.monotonic() - started,
            "outcome": "PASS",
        }
        (self.evidence / "full-ledger-audit.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result), flush=True)
        return result

    def once(self, client, sequence):
        query_index = sequence % len(self.queries)
        # Keep the query/operation schedules coprime so all queries exercise PyUDF.
        control = (sequence // len(self.queries) + sequence) % 10 == 0
        wheel = (sequence // 10) % 2
        factor, bias = [(2, 5), (3, 11)][wheel]
        chain = None if control else self.chain(self.resources[wheel], factor, bias)
        started = time.monotonic()
        hits = self.search(client, self.queries[query_index], chain)
        if control:
            self.verify_control(hits, query_index)
        else:
            self.verify_udf(hits, self.candidates[query_index], factor, bias)
        return {
            "sequence": sequence,
            "query": query_index,
            "kind": "control" if control else "pyudf",
            "wheel": None if control else wheel,
            "latency_ms": (time.monotonic() - started) * 1000,
        }


def prepare(client, storage, args, evidence, stop_file=None):
    stop_file = Path(stop_file) if stop_file is not None else evidence / "STOP"
    check_stop_file(stop_file)
    name = args.collection
    assert name.startswith("pyudf_"), "Use an exclusively owned pyudf_ collection"
    assert not client.has_collection(name), "Refuse to overwrite an existing collection"
    rng = np.random.default_rng(SEED)
    vectors = rng.random((args.rows, 128), dtype=np.float32)
    queries = vectors[rng.choice(args.rows, size=min(1000, args.rows), replace=False)].copy()
    np.save(evidence / "vectors.npy", vectors)
    np.save(evidence / "queries.npy", queries)
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("ts", DataType.INT64)
    schema.add_field("label", DataType.VARCHAR, max_length=64)
    schema.add_field("flag", DataType.BOOL)
    schema.add_field("optional", DataType.INT64, nullable=True)
    schema.add_field("vector", DataType.FLOAT_VECTOR, dim=128)
    check_stop_file(stop_file)
    client.create_collection(name, schema=schema, num_shards=2, consistency_level="Strong")
    for start in range(0, args.rows, 1000):
        check_stop_file(stop_file)
        rows = [
            {
                "id": pk,
                "ts": pk * 3 + 7,
                "label": f"row_{pk}",
                "flag": pk % 2 == 0,
                "optional": None if pk % 7 == 0 else pk,
                "vector": vectors[pk].tolist(),
            }
            for pk in range(start, min(start + 1000, args.rows))
        ]
        result = client.insert(name, rows, timeout=60)
        assert result["insert_count"] == len(rows), result
    check_stop_file(stop_file)
    client.flush(name, timeout=180)
    params = client.prepare_index_params()
    params.add_index(
        "vector",
        index_type=args.index_type,
        metric_type="L2",
        params={"M": 18, "efConstruction": 240} if args.index_type == "HNSW" else {},
    )
    check_stop_file(stop_file)
    client.create_index(name, params, timeout=300)
    check_stop_file(stop_file)
    client.load_collection(name, replica_number=1, timeout=300)
    check_stop_file(stop_file)
    index = client.describe_index(name, "vector")
    assert index["state"] == "Finished" and index["indexed_rows"] == args.rows, index
    assert client.query(name, filter="", output_fields=["count(*)"])[0]["count(*)"] == args.rows
    resources = []
    wheels = []
    if not storage.bucket_exists(args.bucket):
        storage.make_bucket(args.bucket)
    for number in range(2):
        check_stop_file(stop_file)
        resource = f"{name}_wheel_{number}"
        package = f"{name}_pkg_{number}"
        wheel = build_wheel(package, MODULE)
        path = f"pyudf_r1/{resource}.whl"
        (evidence / f"{resource}.whl").write_bytes(wheel)
        # Persist ownership before remote writes, including ambiguous RPC failures.
        wheels.append({"resource": resource, "path": path, "sha256": hashlib.sha256(wheel).hexdigest()})
        (evidence / "wheels.json").write_text(json.dumps(wheels, indent=2))
        storage.put_object(args.bucket, path, io.BytesIO(wheel), len(wheel))
        check_stop_file(stop_file)
        client.add_file_resource(resource, path, timeout=30)
        resources.append(resource)
    metadata = {
        "server": client.get_server_version(detail=True),
        "sdk": pymilvus.__version__,
        "seed": SEED,
        "rows": args.rows,
        "dim": 128,
        "queries": len(queries),
        "wheels": wheels,
        "index": index,
        "collection": name,
        "vectors_sha256": hashlib.sha256((evidence / "vectors.npy").read_bytes()).hexdigest(),
        "workload_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    (evidence / "metadata.json").write_text(json.dumps(metadata, indent=2))
    print(json.dumps({"event": "prepared", **metadata}), flush=True)
    return PyUDFSearchChecker(client, name, resources, vectors, queries, evidence, args.index_type, stop_file)


def resume(client, storage, args, evidence):
    """Reuse retained inputs and candidate baselines; never silently re-baseline."""
    metadata = json.loads((evidence / "metadata.json").read_text())
    assert metadata["collection"] == args.collection and metadata["rows"] == args.rows
    assert hashlib.sha256((evidence / "vectors.npy").read_bytes()).hexdigest() == metadata["vectors_sha256"]
    assert client.query(args.collection, filter="", output_fields=["count(*)"])[0]["count(*)"] == args.rows
    registered = {item.name: item.path for item in client.list_file_resources()}
    for wheel in metadata["wheels"]:
        assert registered[wheel["resource"]] == wheel["path"]
        response = storage.get_object(args.bucket, wheel["path"])
        try:
            assert hashlib.sha256(response.read()).hexdigest() == wheel["sha256"]
        finally:
            response.close()
            response.release_conn()
    checker = PyUDFSearchChecker(
        client,
        args.collection,
        [wheel["resource"] for wheel in metadata["wheels"]],
        np.load(evidence / "vectors.npy"),
        np.load(evidence / "queries.npy"),
        evidence,
        args.index_type,
    )
    checker.candidates = json.loads((evidence / "candidates.json").read_text())
    if args.index_type == "AUTOINDEX":
        checker.baseline_distances = [
            {int(key): value for key, value in row.items()}
            for row in json.loads((evidence / "baseline_distances.json").read_text())
        ]
    print(json.dumps({"event": "resumed", "metadata": metadata}), flush=True)
    return checker
