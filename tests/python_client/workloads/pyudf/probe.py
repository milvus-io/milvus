"""Small owned PyUDF fixture for runtime E2E and system recovery probes."""

import io
import time
import uuid
from contextlib import contextmanager

import numpy as np
from common.pyudf_helpers import build_wheel
from pymilvus import DataType, FunctionChain, FunctionChainStage
from pymilvus.function_chain import col
from pymilvus.function_chain.chain import FunctionChainExpr

PROBE_MODULE = """import os
import threading
import time
import pyarrow as pa

class UDF:
    def __init__(self, context):
        self.context = context
        self.calls = 0
        self.completed = 0
        self.lock = threading.Lock()

    def transform_query(self, params, columns):
        with self.lock:
            self.calls += 1
            call = self.calls
        mode = params.get("mode", "normal")
        self.context.logger.warning("PYUDF_PROBE_START pid=%s call=%s mode=%s", os.getpid(), call, mode)
        try:
            if mode == "system_exit":
                raise SystemExit("qa_synthetic_system_exit")
            if mode == "os_error":
                raise OSError("qa_synthetic_os_error")
            if mode == "memory_error":
                raise MemoryError("qa_synthetic_memory_error")
            if mode == "delayed":
                time.sleep(2)
            if mode == "server_timeout":
                time.sleep(8)
            with self.lock:
                completed = self.completed
            if mode == "wide_output":
                return [pa.array(["x" * params["size"]] * len(columns[0]), type=pa.string())]
            if mode == "wide_input":
                for column in columns[:-1]:
                    if any(value.as_py() != "x" * params["size"] for value in column):
                        raise AssertionError("Wide intermediate content changed")
                return [pa.array([value.as_py() * 2 for value in columns[-1]], type=pa.float32())]
            if mode == "state":
                values = [100 * completed + call] * len(columns[0])
            else:
                values = [value.as_py() * 2 for value in columns[0]]
            return [pa.array(values, type=pa.float32())]
        finally:
            with self.lock:
                self.completed += 1
            self.context.logger.warning("PYUDF_PROBE_END pid=%s call=%s mode=%s", os.getpid(), call, mode)

def create_udf(context):
    return UDF(context)
"""


class PyUDFProbe:
    def __init__(self, client, name, resource, string_pk=False, query=None, metric="L2", *, distances):
        self.client = client
        self.name = name
        self.resource = resource
        self.ids = ["甲", "乙", "丙"] if string_pk else [1, 2, 3]
        self.query = [0.0, 0.0] if query is None else query
        self.metric = metric
        self.distances = dict(zip(self.ids, distances, strict=True))

    def chain(self, mode="normal"):
        return (
            FunctionChain(FunctionChainStage.L2_RERANK)
            .map(
                "$score",
                FunctionChainExpr(
                    "py_udf", args=(col("ts"),), params={"resource_name": self.resource, "udf_params": {"mode": mode}}
                ),
            )
            .sort(col("$score"), desc=True, tie_break_col=col("$id"))
        )

    def search(self, mode="normal", timeout=10, with_udf=True, nq=1):
        return self.client.search(
            self.name,
            data=[self.query] * nq,
            anns_field="vector",
            limit=3,
            search_params={"metric_type": self.metric},
            output_fields=["ts", "text"],
            timeout=timeout,
            **({"function_chains": self.chain(mode)} if with_udf else {}),
        )

    def check(self, with_udf=True, nq=1):
        results = self.search(with_udf=with_udf, nq=nq)
        assert len(results) == nq
        for hits in results:
            ids = [hit["id"] for hit in hits]
            assert len(ids) == len(self.ids) and set(ids) == set(self.ids), hits
            order = [self.ids.index(pk) for pk in ids]
            assert [hit["entity"]["ts"] for hit in hits] == [(index + 1) * 10 for index in order], hits
            assert [hit["entity"]["text"] for hit in hits] == [f"你好_{index}" for index in order], hits
            if with_udf:
                assert ids == list(reversed(self.ids)), hits
                assert [hit["distance"] for hit in hits] == [60, 40, 20], hits
            else:
                scores = [hit["distance"] for hit in hits]
                assert all(np.isfinite(score) for score in scores), hits
                assert np.allclose(scores, [self.distances[pk] for pk in ids], rtol=1e-5, atol=1e-6), hits
                assert scores == sorted(scores, reverse=self.metric in ("IP", "COSINE")), hits
        return results


def vector_fixture(kind):
    """Use small, exactly representable data; vector columns never enter the UDF."""
    if kind == DataType.BINARY_VECTOR:
        return [bytes([1 << i, 0]) for i in range(3)], bytes([1, 0]), "BIN_FLAT", {"dim": 16}
    if kind == DataType.SPARSE_FLOAT_VECTOR:
        return [{0: float(i + 1), i + 1: 1.0} for i in range(3)], {0: 1.0}, "SPARSE_INVERTED_INDEX", {}
    vectors = [np.array([float(i + 1)] + [0.0] * 15, dtype=np.float32) for i in range(3)]
    if kind == DataType.FLOAT16_VECTOR:
        values = [v.astype(np.float16).tobytes() for v in vectors]
    elif kind == DataType.BFLOAT16_VECTOR:
        values = [(v.view(np.uint32) >> 16).astype(np.uint16).tobytes() for v in vectors]
    elif kind == DataType.INT8_VECTOR:
        values = [v.astype(np.int8).tobytes() for v in vectors]
    else:
        values = [v.tolist() for v in vectors]
    return values, values[0], "HNSW", {"dim": 16}


def expected_distances(vectors, query, kind, metric):
    """Compute recovery scores from retained inputs, independent of Search results."""
    if kind == DataType.BINARY_VECTOR:
        query_bits = np.unpackbits(np.frombuffer(query, dtype=np.uint8))
        bits = [np.unpackbits(np.frombuffer(vector, dtype=np.uint8)) for vector in vectors]
        if metric == "HAMMING":
            return [float(np.count_nonzero(vector != query_bits)) for vector in bits]
        assert metric == "JACCARD", metric
        return [1 - np.count_nonzero(vector & query_bits) / np.count_nonzero(vector | query_bits) for vector in bits]
    if kind == DataType.SPARSE_FLOAT_VECTOR:
        assert metric == "IP", metric
        return [sum(value * vector.get(key, 0) for key, value in query.items()) for vector in vectors]

    def decode(vector):
        if kind == DataType.FLOAT16_VECTOR:
            return np.frombuffer(vector, dtype=np.float16).astype(np.float32)
        if kind == DataType.BFLOAT16_VECTOR:
            return (np.frombuffer(vector, dtype=np.uint16).astype(np.uint32) << 16).view(np.float32)
        if kind == DataType.INT8_VECTOR:
            return np.frombuffer(vector, dtype=np.int8).astype(np.float32)
        return np.asarray(vector, dtype=np.float32)

    query = decode(query)
    values = [decode(vector) for vector in vectors]
    if metric == "L2":
        return [float(np.sum((vector - query) ** 2)) for vector in values]
    if metric == "IP":
        return [float(np.dot(vector, query)) for vector in values]
    assert metric == "COSINE", metric
    return [float(np.dot(vector, query) / (np.linalg.norm(vector) * np.linalg.norm(query))) for vector in values]


@contextmanager
def pyudf_probe(client, storage, bucket, string_pk=False, vector_kind=None, metric="L2"):
    name = "pyudf_r1_probe_" + uuid.uuid4().hex[:12]
    resource = name + "_resource"
    path = f"pyudf_r1/{resource}.whl"
    wheel = build_wheel(name + "_pkg", PROBE_MODULE)
    collection_owned = resource_owned = object_owned = False
    failure = None
    try:
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(
            "id",
            DataType.VARCHAR if string_pk else DataType.INT64,
            is_primary=True,
            **({"max_length": 64} if string_pk else {}),
        )
        schema.add_field("ts", DataType.INT64)
        schema.add_field("text", DataType.VARCHAR, max_length=64)
        vectors, query, index_type, dimensions = (
            ([[float(i), 0.0] for i in range(3)], [0.0, 0.0], "FLAT", {"dim": 2})
            if vector_kind is None
            else vector_fixture(vector_kind)
        )
        schema.add_field("vector", vector_kind or DataType.FLOAT_VECTOR, **dimensions)
        index = client.prepare_index_params()
        index.add_index(
            "vector",
            index_type=index_type,
            metric_type=metric,
            params={"M": 8, "efConstruction": 64} if index_type == "HNSW" else {},
        )
        # Record ownership before each write: a timeout may hide a successful RPC.
        collection_owned = True
        client.create_collection(name, schema=schema, index_params=index, consistency_level="Strong")
        probe = PyUDFProbe(
            client,
            name,
            resource,
            string_pk,
            query=query,
            metric=metric,
            distances=expected_distances(vectors, query, vector_kind, metric),
        )
        client.insert(
            name,
            [
                {"id": pk, "ts": (index + 1) * 10, "text": f"你好_{index}", "vector": vectors[index]}
                for index, pk in enumerate(probe.ids)
            ],
        )
        client.flush(name, timeout=60)
        client.load_collection(name, timeout=60)
        if not storage.bucket_exists(bucket):
            storage.make_bucket(bucket)
        object_owned = True
        storage.put_object(bucket, path, io.BytesIO(wheel), len(wheel))
        resource_owned = True
        client.add_file_resource(resource, path, timeout=30)
        deadline = time.monotonic() + 30
        while True:
            try:
                probe.check()
                break
            except Exception as error:
                if time.monotonic() >= deadline or not (
                    "file resource snapshot is not ready" in str(error)
                    or "file resource" in str(error)
                    and "not found" in str(error)
                ):
                    raise
                time.sleep(0.2)
        yield probe
    except BaseException as error:
        failure = error
        raise
    finally:
        # Callers must drain any timed-out UDF before leaving this context.
        errors = []
        for owned, operation, remove in (
            (collection_owned, "drop collection", lambda: client.drop_collection(name, timeout=30)),
            (resource_owned, "remove resource", lambda: client.remove_file_resource(resource, timeout=30)),
            (object_owned, "remove wheel object", lambda: storage.remove_object(bucket, path)),
        ):
            if owned:
                try:
                    remove()
                except Exception as error:
                    error.add_note(operation)
                    errors.append(error)
        if errors:
            raise BaseExceptionGroup("PyUDF probe cleanup failed", ([failure] if failure else []) + errors)
