"""Retained D2 migration ledger and independent data/PyUDF checks.

The driver owns deployment, transfer, restart and credentials. This module never
copies a wheel implicitly during verification or silently re-baselines data.
"""

import hashlib
import io
import json
import time
from pathlib import Path

import numpy as np
from common.pyudf_helpers import build_wheel
from pymilvus import DataType, FunctionChain, FunctionChainStage
from pymilvus.client.prepare import Prepare
from pymilvus.client.utils import check_status
from pymilvus.function_chain import col
from pymilvus.function_chain.chain import FunctionChainExpr

SEED = 20261106
ROWS = 10000
DIM = 128


def live_server_identity(client):
    """Use the SDK's Connect wire path without its detail=True identity cache."""
    response = client._get_connection()._stub.Connect(Prepare.register_request("", ""), timeout=10)
    check_status(response.status)
    info = response.server_info
    return {
        "version": info.build_tags,
        "build_time": info.build_time,
        "git_commit": info.git_commit,
        "go_version": info.go_version,
        "deploy_mode": info.deploy_mode,
        "identity_source": "live_Connect_RPC",
    }


SCALARS = {
    "i8": DataType.INT8,
    "i16": DataType.INT16,
    "i32": DataType.INT32,
    "ts": DataType.INT64,
    "f32": DataType.FLOAT,
    "f64": DataType.DOUBLE,
    "flag": DataType.BOOL,
}
UDF_FIELDS = [*SCALARS, "label", "optional"]
OUTPUT_FIELDS = [*UDF_FIELDS, "payload", "items", "dynamic_tag", "vector"]
MODULE = """import os
import pyarrow as pa

class UDF:
    def __init__(self, context):
        self.context = context
        self.calls = 0
        context.logger.warning("PYUDF_R2_MIGRATION_FACTORY resource=%s pid=%s",
                               context.resource_name, os.getpid())

    def transform_query(self, params, columns):
        values = [column.to_pylist() for column in columns]
        for index, ts in enumerate(values[3]):
            pk = (ts - 7) // 3
            expected = [pk % 127, pk % 30000, pk * 11, ts,
                        pk * 0.25, pk * 0.125, pk % 2 == 0,
                        "row_%s" % pk, None if pk % 7 == 0 else pk]
            actual = [column[index] for column in values]
            if actual != expected:
                raise AssertionError("Migration input columns differ: %r != %r" % (actual, expected))
        self.calls += 1
        if self.calls == 1 or self.calls % 100 == 0:
            self.context.logger.warning("PYUDF_R2_MIGRATION_COMPLETE resource=%s pid=%s calls=%s",
                                       self.context.resource_name, os.getpid(), self.calls)
        return [pa.array([value * 2 + 5 for value in values[3]], type=pa.float32())]

def create_udf(context):
    return UDF(context)
"""


def digest(value):
    return hashlib.sha256(value).hexdigest()


def make_rows(vectors):
    """A migration profile of the shared comprehensive schema, with exact floats."""
    return [
        {
            "id": pk,
            "i8": pk % 127,
            "i16": pk % 30000,
            "i32": pk * 11,
            "ts": pk * 3 + 7,
            "f32": pk * 0.25,
            "f64": pk * 0.125,
            "flag": pk % 2 == 0,
            "label": f"row_{pk}",
            "optional": None if pk % 7 == 0 else pk,
            "payload": {"pk": pk, "nested": {"revision": 0}},
            "items": [pk, pk + 1],
            "dynamic_tag": f"dynamic_{pk}",
            "vector": vector.tolist(),
        }
        for pk, vector in enumerate(vectors)
    ]


def retained_ledger(directory, name, metric="L2"):
    """Write expected inputs before sending any request; refuse overwrite."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    assert name.startswith("pyudf_r2_"), "Migration collections require the owned prefix"
    assert metric in ("L2", "IP"), "Unsupported migration metric"
    vectors = np.random.default_rng(SEED).random((ROWS, DIM), dtype=np.float32)
    rows = make_rows(vectors)
    resource = name + "_wheel"
    wheel = build_wheel(name + "_pkg", MODULE)
    metadata = {
        "collection": name,
        "resource": resource,
        "wheel_path": f"pyudf_r2/{resource}.whl",
        "wheel_sha256": digest(wheel),
        "seed": SEED,
        "rows": ROWS,
        "dim": DIM,
        "metric": metric,
        "deleted_ids": list(range(0, ROWS, 101)),
        "updated_ids": [pk for pk in range(0, ROWS, 97) if pk % 101],
    }
    content = json.dumps(rows, separators=(",", ":")).encode()
    metadata["ledger_sha256"] = digest(content)
    with (directory / "rows.json").open("xb") as output:
        output.write(content)
    with (directory / "wheel.whl").open("xb") as output:
        output.write(wheel)
    with (directory / "ledger.json").open("x") as output:
        json.dump(metadata, output, indent=2)
    return metadata


class MigrationChecker:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.metadata = json.loads((self.directory / "ledger.json").read_text())
        self.metric = self.metadata.get("metric", "L2")
        assert self.metric in ("L2", "IP")
        raw = (self.directory / "rows.json").read_bytes()
        assert digest(raw) == self.metadata["ledger_sha256"], "Input ledger changed"
        self.rows = json.loads(raw)
        self.wheel = (self.directory / "wheel.whl").read_bytes()
        assert digest(self.wheel) == self.metadata["wheel_sha256"], "Wheel changed"

    def receipt(self, operation, **details):
        with (self.directory / "write-receipts.jsonl").open("a") as output:
            output.write(json.dumps({"unix": time.time(), "operation": operation, **details}) + "\n")

    def expected(self, mutated, post_switch=False):
        rows = {row["id"]: dict(row) for row in self.rows}
        if mutated:
            for pk in self.metadata["deleted_ids"]:
                del rows[pk]
            for pk in self.metadata["updated_ids"]:
                rows[pk]["payload"] = {"pk": pk, "nested": {"revision": 1}}
                rows[pk]["dynamic_tag"] = f"updated_{pk}"
                # Vector updates are independent of the UDF scalar formula.
                rows[pk]["vector"] = [value * 0.5 for value in rows[pk]["vector"]]
        if post_switch:
            assert mutated, "SwitchOver mutation follows the original DML phase"
            rows[97]["payload"] = {"pk": 97, "nested": {"revision": 2}}
            rows[97]["dynamic_tag"] = "after_switchover_97"
            rows[97]["vector"] = [value * 0.25 for value in self.rows[97]["vector"]]
        return rows

    def seed(self, client, index_type):
        name = self.metadata["collection"]
        assert not client.has_collection(name), "Refuse to overwrite existing data"
        schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
        schema.add_field("id", DataType.INT64, is_primary=True)
        for field, dtype in SCALARS.items():
            schema.add_field(field, dtype)
        schema.add_field("label", DataType.VARCHAR, max_length=64)
        schema.add_field("optional", DataType.INT64, nullable=True)
        schema.add_field("payload", DataType.JSON)
        schema.add_field("items", DataType.ARRAY, element_type=DataType.INT64, max_capacity=4)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=DIM)
        client.create_collection(name, schema=schema, num_shards=2, consistency_level="Strong")
        for start in range(0, ROWS, 1000):
            result = client.insert(name, self.rows[start : start + 1000], timeout=60)
            assert result["insert_count"] == 1000, result
            self.receipt("insert_ack", start=start, end=start + 1000, count=result["insert_count"])
        client.flush(name, timeout=180)
        indexes = client.prepare_index_params()
        indexes.add_index(
            "vector",
            index_type=index_type,
            metric_type=self.metric,
            params={"M": 18, "efConstruction": 240} if index_type == "HNSW" else {},
        )
        client.create_index(name, indexes, timeout=300)
        client.load_collection(name, timeout=300)

    def mutate(self, client):
        name = self.metadata["collection"]
        expected = self.expected(True)
        updated = [expected[pk] for pk in self.metadata["updated_ids"]]
        result = client.upsert(name, updated, timeout=60)
        assert result["upsert_count"] == len(updated), result
        self.receipt("upsert_ack", ids=self.metadata["updated_ids"], count=result["upsert_count"])
        result = client.delete(name, ids=self.metadata["deleted_ids"], timeout=60)
        assert result["delete_count"] == len(self.metadata["deleted_ids"]), result
        self.receipt("delete_ack", ids=self.metadata["deleted_ids"], count=result["delete_count"])
        client.flush(name, timeout=180)

    def post_switch_write(self, client):
        row = self.expected(True, post_switch=True)[97]
        result = client.upsert(self.metadata["collection"], [row], timeout=60)
        assert result["upsert_count"] == 1, result
        self.receipt("post_switch_upsert_ack", ids=[97], count=1)

    def prepare_resource(self, client, storage, bucket, register=True, copy=True):
        """Explicit control only; verification never invokes this method."""
        if copy:
            storage.put_object(bucket, self.metadata["wheel_path"], io.BytesIO(self.wheel), len(self.wheel))
        if register:
            client.add_file_resource(self.metadata["resource"], self.metadata["wheel_path"], timeout=30)
        return {"manual_wheel_copy": copy, "manual_registration": register}

    def resource_state(self, client, storage, bucket):
        registered = {item.name: item.path for item in client.list_file_resources(timeout=30)}
        result = {"registry_path": registered.get(self.metadata["resource"]), "object_sha256": None}
        try:
            response = storage.get_object(bucket, self.metadata["wheel_path"])
            try:
                result["object_sha256"] = digest(response.read())
            finally:
                response.close()
                response.release_conn()
        except Exception as error:
            # Keep missing-object distinct from unavailable/unauthorized storage.
            if getattr(error, "code", "") not in ("NoSuchKey", "NoSuchObject"):
                raise
            result["object_error_code"] = error.code
        result["registry_matches"] = result["registry_path"] == self.metadata["wheel_path"]
        result["bytes_match"] = result["object_sha256"] == self.metadata["wheel_sha256"]
        return result

    def audit(self, client, name=None, mutated=False, post_switch=False):
        name = name or self.metadata["collection"]
        expected = self.expected(mutated, post_switch)
        count = client.query(name, filter="", output_fields=["count(*)"], consistency_level="Strong", timeout=30)
        assert count[0]["count(*)"] == len(expected), count
        seen = set()
        for start in range(0, ROWS, 500):
            rows = client.query(
                name,
                filter=f"id >= {start} and id < {start + 500}",
                output_fields=["id", *OUTPUT_FIELDS],
                limit=500,
                consistency_level="Strong",
                timeout=30,
            )
            ids = [row["id"] for row in rows]
            assert len(ids) == len(set(ids)), "Duplicate PKs"
            assert set(ids) == {pk for pk in expected if start <= pk < start + 500}, (start, ids)
            for row in rows:
                self.verify_row(row, expected[row["id"]])
            seen.update(ids)
        assert seen == set(expected)
        return {
            "rows": len(seen),
            "fields": OUTPUT_FIELDS,
            "mutated": mutated,
            "post_switch": post_switch,
            "outcome": "PASS",
        }

    @staticmethod
    def verify_row(actual, expected):
        for field in ["id", *OUTPUT_FIELDS]:
            assert field in actual, (expected["id"], "missing field", field)
            if field == "vector":
                assert np.array_equal(np.asarray(actual[field], dtype=np.float32), expected[field]), expected["id"]
            else:
                assert actual[field] == expected[field], (expected["id"], field, actual[field], expected[field])

    def search_options(self, name=None):
        name = name or self.metadata["collection"]
        return dict(
            collection_name=name,
            data=[self.rows[11]["vector"]],
            anns_field="vector",
            filter="id < 257",
            limit=257,
            search_params={"metric_type": self.metric, "params": {"ef": 512}},
            output_fields=["id", *OUTPUT_FIELDS],
            consistency_level="Strong",
            timeout=30,
        )

    def ordinary(self, client, name=None, mutated=False, post_switch=False):
        expected = self.expected(mutated, post_switch)
        candidates = [pk for pk in expected if pk < 257]
        control = client.search(**self.search_options(name))[0]
        assert len(control) == len(candidates)
        assert {hit["id"] for hit in control} == set(candidates)
        query = np.asarray(self.rows[11]["vector"], dtype=np.float32)
        metric_failures = []
        for hit in control:
            vector = np.asarray(expected[hit["id"]]["vector"], dtype=np.float32)
            score = np.dot(query, vector) if self.metric == "IP" else np.sum((query - vector) ** 2)
            actual = float(hit["distance"])
            if (
                not np.isfinite(actual)
                or (self.metric == "L2" and actual < 0)
                or not np.isclose(actual, score, rtol=1e-5, atol=1e-6)
            ):
                metric_failures.append(
                    {
                        "id": hit["id"],
                        "actual": actual if np.isfinite(actual) else str(actual),
                        "expected": float(score),
                    }
                )
        scores = [float(hit["distance"]) for hit in control]
        ordered = all(
            left >= right if self.metric == "IP" else left <= right for left, right in zip(scores, scores[1:])
        )
        outcome = "FAIL" if metric_failures or not ordered else "PASS"
        return {
            "outcome": outcome,
            "metric": self.metric,
            "metric_control_outcome": outcome,
            "ordering_outcome": "PASS" if ordered else "FAIL",
            "metric_failures": metric_failures,
            "metric_tolerance": {"rtol": 1e-5, "atol": 1e-6},
            "control_candidates": len(control),
            "candidate_ids": [hit["id"] for hit in control],
            "candidate_scores": [
                float(hit["distance"]) if np.isfinite(hit["distance"]) else str(hit["distance"]) for hit in control
            ],
        }

    def udf(self, client, name=None, mutated=False, post_switch=False):
        expected = self.expected(mutated, post_switch)
        candidates = [pk for pk in expected if pk < 257]
        chain = (
            FunctionChain(FunctionChainStage.L2_RERANK)
            .map(
                "$score",
                FunctionChainExpr(
                    "py_udf",
                    args=tuple(col(field) for field in UDF_FIELDS),
                    params={"resource_name": self.metadata["resource"], "udf_params": {}},
                ),
            )
            .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            .limit(20)
        )
        control = self.ordinary(client, name, mutated, post_switch)
        hits = client.search(**self.search_options(name), function_chains=chain)[0]
        assert [hit["id"] for hit in hits] == sorted(candidates, reverse=True)[:20], hits
        for hit in hits:
            assert hit["distance"] == expected[hit["id"]]["ts"] * 2 + 5, hit
            self.verify_row({**hit["entity"], "id": hit["id"]}, expected[hit["id"]])
        return {
            **control,
            "pyudf_outcome": "PASS",
            "pyudf_hits": len(hits),
            "pyudf_ids": [hit["id"] for hit in hits],
            "pyudf_scores": [hit["distance"] for hit in hits],
        }

    def window(self, clients, commits, emit, seconds=600, mutated=False):
        """Direct each request to a fixed Proxy; a lost old endpoint aborts."""
        assert len(clients) == len(commits) >= 2
        assert seconds >= 600, "Migration acceptance requires the full 600-second window"
        started = time.monotonic()
        counts = [0] * len(clients)
        metric_failure_counts = [0] * len(clients)
        first_success = [None] * len(clients)
        last_success = [None] * len(clients)
        while any(
            first is None or last - first < seconds for first, last in zip(first_success, last_success, strict=True)
        ):
            for number, (client, commit) in enumerate(zip(clients, commits, strict=True)):
                version = live_server_identity(client)
                assert version["git_commit"].startswith(commit[:7]), version
                result = self.udf(client, mutated=mutated)
                last_success[number] = time.monotonic()
                if first_success[number] is None:
                    first_success[number] = last_success[number]
                counts[number] += 1
                metric_failure_counts[number] += result["metric_control_outcome"] == "FAIL"
                emit("window_request", proxy=number, server=version, **result)
            time.sleep(1)
        return {
            "seconds": time.monotonic() - started,
            "requests_by_proxy": counts,
            "commits": commits,
            "verified_seconds_by_proxy": [
                last - first for first, last in zip(first_success, last_success, strict=True)
            ],
            "pyudf_outcome": "PASS",
            "metric_failure_counts": metric_failure_counts,
            "outcome": "FAIL" if any(metric_failure_counts) else "PASS",
        }
