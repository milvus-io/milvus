import io
import json
import math
import time
from contextlib import ExitStack, contextmanager

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from common.pyudf_helpers import build_wheel
from minio import Minio
from pymilvus import DataType, FunctionChain, FunctionChainStage
from pymilvus.client.embedding_list import EmbeddingList
from pymilvus.function_chain import col
from pymilvus.function_chain.chain import FunctionChainExpr
from pymilvus.grpc_gen.common_pb2 import SegmentState
from utils.util_log import test_log as log

ROWS = [
    {"id": 1, "ts": 10, "vectors": [[1.0, 0.0], [0.0, 1.0]]},
    {"id": 2, "ts": 20, "vectors": [[0.8, 0.6], [0.6, 0.8]]},
    {"id": 3, "ts": 30, "vectors": [[0.3, math.sqrt(0.91)]]},
]
QUERIES = [[[1.0, 0.0], [0.0, 1.0]], [[-1.0, 0.0]]]
MODULE = """import pyarrow as pa

class UDF:
    def transform_query(self, params, columns):
        mode = params.get("mode", "valid")
        if mode == "exception":
            raise RuntimeError("controlled EmbeddingList PyUDF failure")
        values = [float(score.as_py()) + 2 * value.as_py()
                  for score, value in zip(columns[0], columns[1])]
        if mode == "wrong_rows":
            values = values[:-1]
        return [pa.array(values, type=pa.float32())]

def create_udf(context):
    return UDF()
"""


class TestMilvusClientPyUDFEmbeddingList(TestMilvusClientV2Base):
    """Exercise L2 PyUDF on document-level EmbeddingList results."""

    skip_global_role_cleanup = True

    @staticmethod
    @contextmanager
    def _measure(timings, phase):
        started = time.perf_counter()
        try:
            yield
        finally:
            timings[phase] = time.perf_counter() - started

    @staticmethod
    def _remove_resource(client, resource):
        deadline = time.monotonic() + 30
        while True:
            try:
                client.remove_file_resource(resource)
                return
            except Exception as exc:
                if "not found" in str(exc) and resource in str(exc):
                    return
                if "is still in use" not in str(exc) or time.monotonic() >= deadline:
                    raise
                time.sleep(0.2)

    @contextmanager
    def _environment(self, minio_host, minio_bucket, case_id, element_level=False, source_rows=None):
        timings = {}
        stack = ExitStack()
        started = time.perf_counter()
        try:
            with self._measure(timings, "connect"):
                client = self._client()
                stack.callback(client.close)
                storage = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
            package = cf.gen_unique_str("pyudf_embedding").lower()
            resource = cf.gen_unique_str("pyudf_embedding_resource")
            name = cf.gen_unique_str("pyudf_embedding")
            remote = f"pyudf/embedding/{resource}.whl"
            cleanup_state = {"resource_attempted": False, "resource_removed": False}

            def cleanup_wheel():
                if cleanup_state["resource_attempted"] and not cleanup_state["resource_removed"]:
                    raise AssertionError("wheel retained because FileResource cleanup did not complete")
                storage.remove_object(minio_bucket, remote)

            def cleanup_resource():
                self._remove_resource(client, resource)
                cleanup_state["resource_removed"] = True

            def cleanup_collection():
                if client.has_collection(name):
                    self.drop_collection(client, name)

            with self._measure(timings, "wheel_upload"):
                content = build_wheel(package, MODULE)
                if not storage.bucket_exists(minio_bucket):
                    storage.make_bucket(minio_bucket)
                storage.put_object(minio_bucket, remote, io.BytesIO(content), len(content))
                stack.callback(cleanup_wheel)
            with self._measure(timings, "resource_register"):
                cleanup_state["resource_attempted"] = True
                stack.callback(cleanup_resource)
                self.add_file_resource(client, resource, remote)
            with self._measure(timings, "schema_collection"):
                schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
                schema.add_field("id", DataType.INT64, is_primary=True)
                schema.add_field("ts", DataType.INT64)
                schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
                struct = self.create_struct_field_schema(client)[0]
                struct.add_field("embedding", DataType.FLOAT_VECTOR, dim=2)
                if element_level:
                    struct.add_field("ordinal", DataType.INT64)
                schema.add_field(
                    "events", DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=struct, max_capacity=2
                )
                indexes = self.prepare_index_params(client)[0]
                indexes.add_index("vector", index_type="FLAT", metric_type="L2")
                indexes.add_index(
                    "events[embedding]",
                    index_type="HNSW",
                    metric_type="COSINE" if element_level else "MAX_SIM_COSINE",
                    params={"M": 8, "efConstruction": 64},
                )
                stack.callback(cleanup_collection)
                self.create_collection(
                    client,
                    name,
                    schema=schema,
                    index_params=indexes,
                    force_teardown=False,
                    num_shards=1,
                    consistency_level="Strong",
                )
            with self._measure(timings, "insert_flush_load"):
                rows = [
                    {
                        "id": row["id"],
                        "ts": row["ts"],
                        "vector": [float(row["id"]), 0.0],
                        "events": [
                            {"embedding": vector, **({"ordinal": index} if element_level else {})}
                            for index, vector in enumerate(row["vectors"])
                        ],
                    }
                    for row in (ROWS if source_rows is None else source_rows)
                ]
                result = self.insert(client, name, rows)[0]
                assert result["insert_count"] == len(rows)
                self.flush(client, name)
                self.load_collection(client, name)
            with self._measure(timings, "body_search_and_assertions"):
                yield client, name, resource
        finally:
            try:
                with self._measure(timings, "cleanup"):
                    stack.close()
            finally:
                log.info(
                    "PYUDF_EMBEDDING_TIMING="
                    + json.dumps(
                        {"case": case_id, "phases_seconds": timings, "total_seconds": time.perf_counter() - started}
                    ),
                )

    @staticmethod
    def _chain(resource, mode="valid", page=False):
        chain = (
            FunctionChain(FunctionChainStage.L2_RERANK)
            .map(
                "$score",
                FunctionChainExpr(
                    "py_udf",
                    args=(col("$score"), col("ts")),
                    params={"resource_name": resource, "udf_params": {"mode": mode}},
                ),
            )
            .sort(col("$score"), desc=True, tie_break_col=col("$id"))
        )
        return chain.limit(2, offset=1) if page else chain

    @staticmethod
    def _request(name, nq, filter_expr="", chain=None):
        args = {
            "collection_name": name,
            "data": [EmbeddingList(vectors) for vectors in QUERIES[:nq]],
            "anns_field": "events[embedding]",
            "limit": 3,
            "output_fields": ["ts"],
            "filter": filter_expr,
            "search_params": {
                "metric_type": "MAX_SIM_COSINE",
                "params": {"ef": 64, "retrieval_ann_ratio": 3.0, "emb_list_rerank": True},
            },
        }
        if chain is not None:
            args["function_chains"] = chain
        return args

    @staticmethod
    def _expected(query, ids, reranked=False, page=False):
        def cosine(left, right):
            return sum(a * b for a, b in zip(left, right)) / math.sqrt(
                sum(a * a for a in left) * sum(b * b for b in right)
            )

        expected = []
        for row in ROWS:
            if row["id"] not in ids:
                continue
            score = sum(max(cosine(vector, document) for document in row["vectors"]) for vector in query)
            expected.append({"id": row["id"], "ts": row["ts"], "score": score + 2 * row["ts"] if reranked else score})
        expected.sort(key=lambda row: (-row["score"], row["id"]))
        return expected[1:3] if page else expected

    def _assert_results(self, results, nq, ids, reranked=False, page=False):
        assert len(results) == nq
        for query, hits in zip(QUERIES[:nq], results):
            expected = self._expected(query, ids, reranked, page)
            assert [hit["id"] for hit in hits] == [row["id"] for row in expected]
            assert [hit["distance"] for hit in hits] == pytest.approx(
                [row["score"] for row in expected], rel=1e-5, abs=1e-5
            )
            assert [hit.get("entity", hit)["ts"] for hit in hits] == [row["ts"] for row in expected]
            assert all("events" not in hit.get("entity", hit) for hit in hits)

    def _synced_search(self, client, args):
        # Probe the real API while only retrying known FileResource synchronization states.
        deadline = time.monotonic() + 30
        while True:
            try:
                return client.search(timeout=15, **args)
            except Exception as exc:
                message = str(exc)
                transient = "file resource snapshot is not ready" in message or (
                    "file resource" in message and "not found" in message
                )
                if not transient or time.monotonic() >= deadline:
                    raise
                time.sleep(0.2)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [1, 2])
    @pytest.mark.parametrize("scenario", ["all", "filter", "empty", "chain_page"])
    def test_pyudf_embedding_list_preserves_scores_and_fields(self, minio_host, minio_bucket, request, nq, scenario):
        """target: EmbeddingList L2 rerank; method: exact MaxSim oracle; expected: query/row/score alignment."""
        filter_expr = {"filter": "id != 2", "empty": "id == 999"}.get(scenario, "")
        ids = [] if scenario == "empty" else [1, 3] if scenario == "filter" else [1, 2, 3]
        with self._environment(minio_host, minio_bucket, request.node.nodeid) as (client, name, resource):
            ordinary = self.search(client, **self._request(name, nq, filter_expr))[0]
            self._assert_results(ordinary, nq, ids)
            chain = self._chain(resource, page=scenario == "chain_page")
            result = self._synced_search(client, self._request(name, nq, filter_expr, chain))
            self._assert_results(result, nq, ids, reranked=True, page=scenario == "chain_page")
            control = self._synced_search(client, self._request(name, nq, chain=self._chain(resource)))
            self._assert_results(control, nq, [1, 2, 3], reranked=True)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("element_level", [False, True], ids=["embedding_list", "element"])
    def test_pyudf_indexed_array_search_preserves_scores_and_fields(
        self, minio_host, minio_bucket, request, record_property, element_level
    ):
        """target: built HNSW paths; method: verify threshold/build/load and exact oracle; expected: complete aligned hits."""
        # L3: proving the index-build precondition requires the coordinator's
        # management endpoint, which the ordinary CI runner does not supply.
        import requests

        endpoint = request.config.getoption("--pyudf-management-endpoint")
        assert endpoint, "requires the coordinator --pyudf-management-endpoint"
        key = "indexCoord.segment.minSegmentNumRowsToEnableIndex"
        response = requests.get(endpoint.rstrip("/") + "/management/config/get", params={"keys": key}, timeout=10)
        response.raise_for_status()
        config = response.json()["configs"][0]
        assert config["key"] == key, config
        threshold = int(config["value"])
        assert 0 < threshold <= 2048, config
        record_property("min_index_rows", threshold)
        # Both document and element counts exceed the checked build threshold.
        # Negative filler scores keep the final oracle small while the baseline
        # checks every candidate without relying on approximate topK recall.
        rows = ROWS + [{"id": i, "ts": -i, "vectors": [[-0.6, -0.8]]} for i in range(4, 2049)]
        with self._environment(
            minio_host, minio_bucket, request.node.nodeid, element_level=element_level, source_rows=rows
        ) as (client, name, resource):
            deadline = time.monotonic() + 120
            while True:
                info = self.describe_index(client, name, "events[embedding]")[0]
                if info["state"] == "Finished" and int(info["indexed_rows"]) == len(rows):
                    break
                assert time.monotonic() < deadline, info
                time.sleep(0.5)
            assert info["index_type"] == "HNSW", info
            self.release_collection(client, name)
            self.load_collection(client, name)
            segments = client.list_loaded_segments(name)
            assert len(segments) == 1, segments
            assert segments[0].num_rows == len(rows) >= threshold, segments
            assert segments[0].state == SegmentState.Sealed and segments[0].index_id > 0, segments
            record_property("indexed_array_rows", len(rows))
            record_property("loaded_index_segments", str(segments))

            args = self._element_request(name, 2) if element_level else self._request(name, 2)
            args["limit"] = sum(len(row["vectors"]) for row in rows) if element_level else len(rows)
            args["search_params"]["params"]["ef"] = 4096
            baseline = self.search(client, **args)[0]
            assert len(baseline) == 2
            ledger = {row["id"]: row for row in rows}
            for query_vectors, hits in zip(QUERIES, baseline):
                identities = [(h["id"], h["offset"]) for h in hits] if element_level else [h["id"] for h in hits]
                expected = (
                    {(row["id"], offset) for row in rows for offset in range(len(row["vectors"]))}
                    if element_level
                    else set(ledger)
                )
                assert len(identities) == len(expected) and set(identities) == expected
                for hit in hits:
                    row = ledger[hit["id"]]
                    vectors = [row["vectors"][hit["offset"]]] if element_level else row["vectors"]
                    query = query_vectors[:1] if element_level else query_vectors
                    score = sum(
                        max(
                            sum(a * b for a, b in zip(q, v)) / math.sqrt(sum(a * a for a in q) * sum(b * b for b in v))
                            for v in vectors
                        )
                        for q in query
                    )
                    assert hit["distance"] == pytest.approx(score, abs=1e-5)
                    assert hit.get("entity", hit)["ts"] == row["ts"]
            args["function_chains"] = self._chain(resource).limit(5 if element_level else 3)
            result = self._synced_search(client, args)
            if element_level:
                self._assert_element_results(result, 2, reranked=True)
            else:
                self._assert_results(result, 2, [1, 2, 3], reranked=True)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [1, 2])
    @pytest.mark.parametrize("failure", ["exception", "wrong_rows", "missing_resource"])
    def test_pyudf_embedding_list_rejects_errors_and_recovers(self, minio_host, minio_bucket, request, nq, failure):
        """target: failed EmbeddingList UDF; method: exact error and recovery; expected: no partial success."""
        with self._environment(minio_host, minio_bucket, request.node.nodeid) as (client, name, resource):
            valid = self._request(name, nq, chain=self._chain(resource))
            self._assert_results(self._synced_search(client, valid), nq, [1, 2, 3], reranked=True)
            failed_resource = cf.gen_unique_str("pyudf_missing") if failure == "missing_resource" else resource
            chain = self._chain(failed_resource, mode=failure)
            code, message = {
                "exception": (2400, "controlled EmbeddingList PyUDF failure"),
                "wrong_rows": (2400, "rows, expected 3"),
                "missing_resource": (1100, "not found"),
            }[failure]
            error = self.search(
                client,
                **self._request(name, nq, chain=chain),
                check_task=CheckTasks.err_res,
                check_items={"err_code": code, "err_msg": message},
            )[0]
            assert error.code == code
            if failure == "missing_resource":
                assert failed_resource in str(error)
            self._assert_results(self.search(client, **self._request(name, nq))[0], nq, [1, 2, 3])
            self._assert_results(self._synced_search(client, valid), nq, [1, 2, 3], reranked=True)

    @staticmethod
    def _element_request(name, nq, scenario="all", chain=None):
        args = {
            "collection_name": name,
            "data": [vectors[0] for vectors in QUERIES[:nq]],
            "anns_field": "events[embedding]",
            "limit": 5,
            "output_fields": ["ts", "vector"] if scenario == "output_vector" else ["ts"],
            "filter": {
                "element_filter": "element_filter(events, $[ordinal] == 1)",
                "row_filter": "id != 2",
                "empty": "element_filter(events, $[ordinal] == 99)",
            }.get(scenario, ""),
            "search_params": {"metric_type": "COSINE", "params": {"ef": 64}},
        }
        if chain is not None:
            args["function_chains"] = chain
        return args

    @staticmethod
    def _assert_element_results(results, nq, scenario="all", reranked=False):
        assert len(results) == nq
        for query_vectors, hits in zip(QUERIES[:nq], results):
            query = query_vectors[0]
            expected = []
            for row in ROWS:
                if scenario == "empty" or (scenario == "row_filter" and row["id"] == 2):
                    continue
                for index, vector in enumerate(row["vectors"]):
                    if scenario == "element_filter" and index != 1:
                        continue
                    cosine = sum(a * b for a, b in zip(query, vector)) / math.sqrt(
                        sum(a * a for a in query) * sum(b * b for b in vector)
                    )
                    expected.append((row["id"], index, cosine + (2 * row["ts"] if reranked else 0), row["ts"]))
            expected.sort(key=lambda item: (-item[2], item[0]))
            if scenario == "chain_page" and reranked:
                expected = expected[1:3]
            assert [(hit["id"], hit["offset"]) for hit in hits] == [(item[0], item[1]) for item in expected]
            assert [hit["distance"] for hit in hits] == pytest.approx([item[2] for item in expected], abs=1e-5)
            assert [hit.get("entity", hit)["ts"] for hit in hits] == [item[3] for item in expected]
            for hit in hits:
                entity = hit.get("entity", hit)
                assert not any(key.startswith("$") for key in entity)
                if scenario == "output_vector":
                    assert entity["vector"] == pytest.approx([float(hit["id"]), 0.0])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [1, 2])
    @pytest.mark.parametrize(
        "scenario", ["all", "element_filter", "row_filter", "empty", "chain_page", "output_vector"]
    )
    def test_pyudf_element_search_preserves_duplicate_pk_identity(
        self, minio_host, minio_bucket, request, nq, scenario
    ):
        """target: element L2 rerank; method: exact cosine and (PK, offset) oracle; expected: no merged/misaligned hits."""
        with self._environment(minio_host, minio_bucket, request.node.nodeid, element_level=True) as (
            client,
            name,
            resource,
        ):
            ordinary = self.search(client, **self._element_request(name, nq, scenario))[0]
            self._assert_element_results(ordinary, nq, scenario)
            chain = self._chain(resource, page=scenario == "chain_page")
            result = self._synced_search(client, self._element_request(name, nq, scenario, chain))
            self._assert_element_results(result, nq, scenario, reranked=True)
            control = self._synced_search(client, self._element_request(name, nq, chain=self._chain(resource)))
            self._assert_element_results(control, nq, reranked=True)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [1, 2])
    @pytest.mark.parametrize("failure", ["exception", "wrong_rows", "missing_resource"])
    def test_pyudf_element_search_rejects_errors_and_recovers(self, minio_host, minio_bucket, request, nq, failure):
        """target: failed element rerank; method: rejection then exact recovery; expected: no partial result or lost offsets."""
        with self._environment(minio_host, minio_bucket, request.node.nodeid, element_level=True) as (
            client,
            name,
            resource,
        ):
            valid = self._element_request(name, nq, chain=self._chain(resource))
            self._assert_element_results(self._synced_search(client, valid), nq, reranked=True)
            failed_resource = cf.gen_unique_str("pyudf_missing") if failure == "missing_resource" else resource
            code, message = {
                "exception": (2400, "controlled EmbeddingList PyUDF failure"),
                "wrong_rows": (2400, "rows, expected 5"),
                "missing_resource": (1100, "not found"),
            }[failure]
            error = self.search(
                client,
                **self._element_request(name, nq, chain=self._chain(failed_resource, mode=failure)),
                check_task=CheckTasks.err_res,
                check_items={"err_code": code, "err_msg": message},
            )[0]
            assert error.code == code
            if failure == "missing_resource":
                assert failed_resource in str(error)
            self._assert_element_results(self.search(client, **self._element_request(name, nq))[0], nq)
            self._assert_element_results(self._synced_search(client, valid), nq, reranked=True)
