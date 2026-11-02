import io
import math
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager

import grpc
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from google.protobuf.message import DecodeError
from pymilvus import (
    AnnSearchRequest,
    DataType,
    Function,
    FunctionChain,
    FunctionChainStage,
    FunctionType,
    MilvusClient,
    MilvusException,
    RRFRanker,
)
from pymilvus.exceptions import ParamError
from pymilvus.function_chain import col, fn
from pymilvus.function_chain.chain import FunctionChainExpr
from pymilvus.grpc_gen.common_pb2 import SegmentState

prefix = "pyudf"


class TestMilvusClientPyUDF(TestMilvusClientV2Base):
    """End-to-end tests for PyUDF-backed FunctionChain reranking."""

    dim = 2
    vector_field = "vector"
    scalar_field = "ts"

    @staticmethod
    def _pyudf_management_config(request, key):
        import requests

        endpoint = request.config.getoption("--pyudf-management-endpoint")
        assert endpoint, "requires --pyudf-management-endpoint pointing to the coordinator management service"
        response = requests.get(
            endpoint.rstrip("/") + "/management/config/get",
            params={"keys": key},
            timeout=10,
        )
        response.raise_for_status()
        entry = response.json()["configs"][0]
        assert entry["key"] == key and "value" in entry, entry
        return entry["value"]

    @staticmethod
    def _build_wheel(tmp_path, package_name, module=None, entry_points=None):
        wheel_path = tmp_path / f"{package_name}-0.0.1-py3-none-any.whl"
        if module is None:
            module = """import pyarrow as pa


class UDF:
    def __init__(self, context):
        self.context = context

    def transform_query(self, params, columns):
        factor = float(params.get("factor", 1.0))
        values = [float(value.as_py()) * factor for value in columns[1]]
        return [pa.array(values, type=pa.float32())]


def create_udf(context):
    return UDF(context)
"""
        dist_info = f"{package_name}-0.0.1.dist-info"
        with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_STORED) as wheel:
            wheel.writestr(f"{package_name}/__init__.py", module)
            wheel.writestr(
                f"{dist_info}/entry_points.txt",
                f"[milvus.pyudf]\nmain = {package_name}:create_udf\n" if entry_points is None else entry_points,
            )
            wheel.writestr(
                f"{dist_info}/METADATA",
                f"Metadata-Version: 2.1\nName: {package_name}\nVersion: 0.0.1\n",
            )
        return wheel_path

    @staticmethod
    def _hit_field(hit, field):
        if field in hit:
            return hit[field]
        return hit.get("entity", {}).get(field)

    @staticmethod
    def _assert_intermediate_field_absent(hits, field):
        for hit in hits:
            assert field not in hit, hit
            assert field not in hit.get("entity", {}), hit

    @staticmethod
    def _remove_file_resource(client, resource_name, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                client.remove_file_resource(name=resource_name)
                return
            except Exception as exc:
                if "not found" in str(exc):
                    return
                time.sleep(1)
        client.remove_file_resource(name=resource_name)

    def _search_with_synced_resource(
        self, client, collection_name, chain, timeout=30, data=None, limit=3, search_params=None, **kwargs
    ):
        deadline = time.time() + timeout
        queries = [[0.0, 0.0]] if data is None else data
        while True:
            try:
                return client.search(
                    collection_name=collection_name,
                    data=queries,
                    anns_field=self.vector_field,
                    search_params={"metric_type": "L2"} if search_params is None else search_params,
                    limit=limit,
                    output_fields=[self.scalar_field],
                    function_chains=chain,
                    **kwargs,
                )
            except Exception as exc:
                message = str(exc)
                transient = (
                    "file resource snapshot is not ready" in message
                    or "file resource" in message
                    and "not found" in message
                )
                if not transient or time.time() >= deadline:
                    raise
                time.sleep(1)

    def _wait_for_file_resource_absent(self, client, resource_name, timeout=30):
        deadline = time.time() + timeout
        while True:
            resources, listed = self.list_file_resources(client)
            assert listed
            if all(resource.name != resource_name for resource in resources):
                return
            if time.time() >= deadline:
                raise AssertionError(f"file resource {resource_name!r} was not removed")
            time.sleep(1)

    def _wait_for_removed_pyudf(self, client, collection_name, chain, resource_name, timeout=30):
        deadline = time.time() + timeout
        while True:
            try:
                client.search(
                    collection_name=collection_name,
                    data=[[0.0, 0.0]],
                    anns_field=self.vector_field,
                    search_params={"metric_type": "L2"},
                    limit=3,
                    output_fields=[self.scalar_field],
                    function_chains=chain,
                )
            except Exception as exc:
                message = str(exc)
                if "py_udf: file resource" in message and resource_name in message and "not found" in message:
                    return
                raise
            if time.time() >= deadline:
                raise AssertionError(f"removed PyUDF resource {resource_name!r} remains executable")
            time.sleep(1)

    @contextmanager
    def _pyudf_test_environment(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
        module,
        entry_points=None,
        wheel_bytes=None,
        rows=None,
        nullable=False,
        index_type="FLAT",
    ):
        from minio import Minio

        client = self._client()
        package_name = cf.gen_unique_str("pyudf_contract_e2e").lower()
        resource_name = cf.gen_unique_str("pyudf_contract_resource")
        collection_name = cf.gen_unique_str(prefix)
        remote_path = f"pyudf/{resource_name}.whl"
        wheel_path = self._build_wheel(tmp_path, package_name, module=module, entry_points=entry_points)
        if wheel_bytes is not None:
            wheel_path.write_bytes(wheel_bytes)
        minio_client = Minio(
            f"{minio_host}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        resource_added = False
        resource_removed = False
        collection_created = False
        object_uploaded = False

        try:
            if not minio_client.bucket_exists(minio_bucket):
                minio_client.make_bucket(minio_bucket)
            wheel_data = wheel_path.read_bytes()
            minio_client.put_object(
                minio_bucket,
                remote_path,
                io.BytesIO(wheel_data),
                len(wheel_data),
            )
            object_uploaded = True

            _, added = self.add_file_resource(client, resource_name, remote_path)
            assert added
            resource_added = True

            schema, schema_created = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
            assert schema_created
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field(self.scalar_field, DataType.INT64, nullable=nullable)
            schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)
            index_params, index_params_created = self.prepare_index_params(client)
            assert index_params_created
            index_params.add_index(
                field_name=self.vector_field,
                index_type=index_type,
                metric_type="L2",
                params={"M": 8, "efConstruction": 64} if index_type == "HNSW" else {},
            )
            _, collection_created = self.create_collection(
                client,
                collection_name,
                schema=schema,
                index_params=index_params,
                consistency_level="Strong",
            )
            assert collection_created
            rows = (
                rows
                if rows is not None
                else [
                    {"id": 1, self.scalar_field: 10, self.vector_field: [0.0, 0.0]},
                    {"id": 2, self.scalar_field: 20, self.vector_field: [0.01, 0.0]},
                    {"id": 3, self.scalar_field: 30, self.vector_field: [0.02, 0.0]},
                ]
            )
            _, inserted = self.insert(client, collection_name, rows)
            assert inserted
            _, flushed = self.flush(client, collection_name)
            assert flushed
            _, loaded = self.load_collection(client, collection_name)
            assert loaded

            yield client, collection_name, resource_name
        finally:
            if client is not None:
                if collection_created:
                    try:
                        self.drop_collection(client, collection_name)
                    except Exception:
                        pass
                if resource_added:
                    try:
                        self._remove_file_resource(client, resource_name)
                        resource_removed = True
                    except Exception:
                        pass
                try:
                    client.close()
                except Exception:
                    pass
            if object_uploaded and (not resource_added or resource_removed):
                try:
                    minio_client.remove_object(minio_bucket, remote_path)
                except Exception:
                    pass

    @staticmethod
    def _pyudf_chain(resource_name, udf_params=None):
        return (
            FunctionChain(FunctionChainStage.L2_RERANK)
            .map(
                "$score",
                FunctionChainExpr(
                    "py_udf",
                    args=(col("$score"), col("ts")),
                    params={
                        "resource_name": resource_name,
                        "udf_params": {"factor": 2.0} if udf_params is None else udf_params,
                    },
                ),
            )
            .sort(col("$score"), desc=True, tie_break_col=col("$id"))
        )

    def _assert_pyudf_recovery(self, client, collection_name, resource_name):
        baseline, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            output_fields=[self.scalar_field],
        )
        assert [hit["id"] for hit in baseline[0]] == [1, 2, 3]
        assert [self._hit_field(hit, self.scalar_field) for hit in baseline[0]] == [10, 20, 30]
        result = self._search_with_synced_resource(client, collection_name, self._pyudf_chain(resource_name))
        assert [hit["id"] for hit in result[0]] == [3, 2, 1]
        assert [hit["distance"] for hit in result[0]] == pytest.approx([60.0, 40.0, 20.0], rel=1e-5, abs=1e-5)
        assert [self._hit_field(hit, self.scalar_field) for hit in result[0]] == [30, 20, 10]

    def _assert_pyudf_error(self, client, collection_name, chain, message, code=1100, **kwargs):
        # Preserve the suite's error checker instead of accepting any exception.
        error, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            output_fields=[self.scalar_field],
            function_chains=chain,
            check_task=CheckTasks.err_res,
            check_items={"err_code": code, "err_msg": message},
            **kwargs,
        )
        # The shared err_res checker currently matches the message only.
        assert error.code == code, error

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_pyudf_file_resource_reranks_results(self, tmp_path, minio_host, minio_bucket):
        """
        target: verify a wheel registered as FileResource can rerank a real Search through PyUDF
        method: generate and upload a wheel, register it, then rewrite and sort L2 scores with FunctionChain
        expected: Search returns the PyUDF-produced scores and the corresponding descending result order
        """
        from minio import Minio

        client = self._client()
        package_name = cf.gen_unique_str("pyudf_e2e").lower()
        resource_name = cf.gen_unique_str("pyudf_resource")
        collection_name = cf.gen_unique_str(prefix)
        remote_path = f"pyudf/{resource_name}.whl"
        wheel_path = self._build_wheel(tmp_path, package_name)
        minio_client = Minio(
            f"{minio_host}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        resource_added = False
        resource_removed = False
        collection_created = False
        object_uploaded = False

        try:
            if not minio_client.bucket_exists(minio_bucket):
                minio_client.make_bucket(minio_bucket)
            wheel_data = wheel_path.read_bytes()
            minio_client.put_object(
                minio_bucket,
                remote_path,
                io.BytesIO(wheel_data),
                len(wheel_data),
            )
            object_uploaded = True

            _, added = self.add_file_resource(client, resource_name, remote_path)
            assert added
            resource_added = True
            resources, listed = self.list_file_resources(client)
            assert listed
            assert any(resource.name == resource_name and resource.path == remote_path for resource in resources)

            schema, schema_created = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
            assert schema_created
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field(self.scalar_field, DataType.INT64)
            schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)
            index_params, index_params_created = self.prepare_index_params(client)
            assert index_params_created
            index_params.add_index(field_name=self.vector_field, index_type="FLAT", metric_type="L2")
            _, collection_created = self.create_collection(
                client,
                collection_name,
                schema=schema,
                index_params=index_params,
                consistency_level="Strong",
            )
            assert collection_created
            rows = [
                {"id": 1, self.scalar_field: 10, self.vector_field: [0.0, 0.0]},
                {"id": 2, self.scalar_field: 20, self.vector_field: [0.01, 0.0]},
                {"id": 3, self.scalar_field: 30, self.vector_field: [0.02, 0.0]},
            ]
            _, inserted = self.insert(client, collection_name, rows)
            assert inserted
            _, flushed = self.flush(client, collection_name)
            assert flushed
            _, loaded = self.load_collection(client, collection_name)
            assert loaded

            baseline, baseline_searched = self.search(
                client,
                collection_name,
                data=[[0.0, 0.0]],
                anns_field=self.vector_field,
                search_params={"metric_type": "L2"},
                limit=3,
                output_fields=[self.scalar_field],
            )
            assert baseline_searched
            assert [hit["id"] for hit in baseline[0]] == [1, 2, 3]

            chain = (
                FunctionChain(FunctionChainStage.L2_RERANK, name="pyudf_e2e")
                .map(
                    "$score",
                    FunctionChainExpr(
                        "py_udf",
                        args=(col("$score"), col(self.scalar_field)),
                        params={
                            "resource_name": resource_name,
                            "udf_params": {"factor": 2.0},
                        },
                    ),
                )
                .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            )
            result = self._search_with_synced_resource(client, collection_name, chain)

            assert [hit["id"] for hit in result[0]] == [3, 2, 1]
            assert [hit["distance"] for hit in result[0]] == pytest.approx([60.0, 40.0, 20.0])
            assert [self._hit_field(hit, self.scalar_field) for hit in result[0]] == [
                30,
                20,
                10,
            ]
        finally:
            if client is not None:
                if collection_created:
                    try:
                        self.drop_collection(client, collection_name)
                    except Exception:
                        pass
                if resource_added:
                    try:
                        self._remove_file_resource(client, resource_name)
                        resource_removed = True
                    except Exception:
                        pass
                try:
                    client.close()
                except Exception:
                    pass
            if object_uploaded and (not resource_added or resource_removed):
                try:
                    minio_client.remove_object(minio_bucket, remote_path)
                except Exception:
                    pass

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_returns_pyudf_exception_without_disrupting_milvus(self, tmp_path, minio_host, minio_bucket):
        """
        target: verify a user PyUDF exception is returned without disrupting Milvus
        method: execute a wheel whose transform_query raises, then run a normal Search
        expected: the first Search returns code 2400 and the exception, and the next Search succeeds
        """
        from minio import Minio

        client = self._client()
        package_name = cf.gen_unique_str("pyudf_failure_e2e").lower()
        resource_name = cf.gen_unique_str("pyudf_failure_resource")
        collection_name = cf.gen_unique_str(prefix)
        remote_path = f"pyudf/{resource_name}.whl"
        failure_message = "intentional user PyUDF failure"
        wheel_path = self._build_wheel(
            tmp_path,
            package_name,
            module=f"""
class UDF:
    def transform_query(self, params, columns):
        raise RuntimeError({failure_message!r})

def create_udf(context):
    return UDF()
""",
        )
        minio_client = Minio(
            f"{minio_host}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        resource_added = False
        resource_removed = False
        collection_created = False
        object_uploaded = False

        try:
            if not minio_client.bucket_exists(minio_bucket):
                minio_client.make_bucket(minio_bucket)
            wheel_data = wheel_path.read_bytes()
            minio_client.put_object(
                minio_bucket,
                remote_path,
                io.BytesIO(wheel_data),
                len(wheel_data),
            )
            object_uploaded = True

            _, added = self.add_file_resource(client, resource_name, remote_path)
            assert added
            resource_added = True

            schema, schema_created = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
            assert schema_created
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field(self.scalar_field, DataType.INT64)
            schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)
            index_params, index_params_created = self.prepare_index_params(client)
            assert index_params_created
            index_params.add_index(field_name=self.vector_field, index_type="FLAT", metric_type="L2")
            _, collection_created = self.create_collection(
                client,
                collection_name,
                schema=schema,
                index_params=index_params,
                consistency_level="Strong",
            )
            assert collection_created
            rows = [
                {"id": 1, self.scalar_field: 10, self.vector_field: [0.0, 0.0]},
                {"id": 2, self.scalar_field: 20, self.vector_field: [0.01, 0.0]},
                {"id": 3, self.scalar_field: 30, self.vector_field: [0.02, 0.0]},
            ]
            _, inserted = self.insert(client, collection_name, rows)
            assert inserted
            _, flushed = self.flush(client, collection_name)
            assert flushed
            _, loaded = self.load_collection(client, collection_name)
            assert loaded

            chain = FunctionChain(FunctionChainStage.L2_RERANK, name="pyudf_failure_e2e").map(
                "$score",
                FunctionChainExpr(
                    "py_udf",
                    args=(col("$score"), col(self.scalar_field)),
                    params={"resource_name": resource_name},
                ),
            )
            with pytest.raises(MilvusException) as captured:
                self._search_with_synced_resource(client, collection_name, chain)

            assert captured.value.code == 2400
            assert failure_message in str(captured.value)
            assert "UDF_FAILED" in str(captured.value)

            result = client.search(
                collection_name=collection_name,
                data=[[0.0, 0.0]],
                anns_field=self.vector_field,
                search_params={"metric_type": "L2"},
                limit=3,
                output_fields=[self.scalar_field],
            )
            assert [hit["id"] for hit in result[0]] == [1, 2, 3]
        finally:
            if client is not None:
                if collection_created:
                    try:
                        self.drop_collection(client, collection_name)
                    except Exception:
                        pass
                if resource_added:
                    try:
                        self._remove_file_resource(client, resource_name)
                        resource_removed = True
                    except Exception:
                        pass
                try:
                    client.close()
                except Exception:
                    pass
            if object_uploaded and (not resource_added or resource_removed):
                try:
                    minio_client.remove_object(minio_bucket, remote_path)
                except Exception:
                    pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_pyudf_repeated_input_columns(self, tmp_path, minio_host, minio_bucket):
        """
        target: preserve repeated column arguments through the complete Search path
        method: pass (ts, $score, ts), check shared arrays in the UDF, and rerank two queries
        expected: all three argument positions survive; both queries return scores 90, 60, 30
        """
        module = """import pyarrow as pa


class UDF:
    def transform_query(self, params, columns):
        if len(columns) != 3:
            raise AssertionError("repeated argument position was lost")
        if columns[0] is not columns[2]:
            raise AssertionError("repeated fields must share the same Arrow array")
        if columns[0] is columns[1]:
            raise AssertionError("distinct fields must remain distinct")
        if not pa.types.is_int64(columns[0].type) or not pa.types.is_floating(columns[1].type):
            raise AssertionError("input column order changed")
        values = [
            float(first.as_py()) + 2.0 * float(repeated.as_py())
            for first, repeated in zip(columns[0], columns[2])
        ]
        return [pa.array(values, type=pa.float32())]


def create_udf(context):
    return UDF()
"""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as environment:
            client, collection_name, resource_name = environment
            chain = (
                FunctionChain(FunctionChainStage.L2_RERANK, name="pyudf_repeated_inputs")
                .map(
                    "$score",
                    FunctionChainExpr(
                        "py_udf",
                        args=(col(self.scalar_field), col("$score"), col(self.scalar_field)),
                        params={"resource_name": resource_name},
                    ),
                )
                .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            )
            queries = [[0.0, 0.0], [0.1, 0.1]]
            result = self._search_with_synced_resource(client, collection_name, chain, data=queries)
            assert len(result) == len(queries)
            for hits in result:
                assert [hit["id"] for hit in hits] == [3, 2, 1]
                assert [hit["distance"] for hit in hits] == pytest.approx([90.0, 60.0, 30.0])
                assert [self._hit_field(hit, self.scalar_field) for hit in hits] == [30, 20, 10]

    @pytest.mark.tags(CaseLabel.L1)
    def test_pyudf_parameter_and_output_contracts(self, tmp_path, minio_host, minio_bucket):
        """
        target: verify recursive parameters and output-contract failures across the complete Search path
        method: run one UDF in success and failure modes, including multi-query output consistency failures
        expected: recursive values arrive intact; every output-contract failure returns code 2400; later runs recover
        """
        module = """import pyarrow as pa


class UDF:
    def __init__(self, context):
        self.context = context
        self.last_mode = None
        self.chunk_index = 0

    def _next_chunk(self, mode):
        if self.last_mode != mode:
            self.last_mode = mode
            self.chunk_index = 0
        index = self.chunk_index
        self.chunk_index += 1
        return index

    def _require(self, condition, message):
        if not condition:
            raise AssertionError(message)

    def _validate_nested_params(self, params):
        self._require(params["enabled"] is True, "bool parameter changed")
        self._require(params["count"] == 7, "int parameter changed")
        self._require(params["ratio"] == 1.25, "double parameter changed")
        self._require(params["label"] == "中文", "string parameter changed")
        self._require(params["payload"] == b"\\x00\\xff", "bytes parameter changed")
        self._require(params["items"][0] == 1, "array parameter changed")
        self._require(params["items"][1]["nested"] == ("a", b"\\x80"), "nested parameter changed")
        try:
            params["count"] = 8
        except TypeError:
            pass
        else:
            raise AssertionError("top-level params are mutable")
        try:
            params["items"][1]["nested"] = ()
        except TypeError:
            pass
        else:
            raise AssertionError("nested params are mutable")

    def transform_query(self, params, columns):
        mode = params["mode"]
        if mode == "decimal32":
            return [pa.array([1] * len(columns[0]), type=pa.decimal32(9, 0))]
        size = len(columns[0])
        if mode == "nested_params":
            self._validate_nested_params(params)
            factor = float(params["factor"])
            values = [float(value.as_py()) * factor for value in columns[1]]
            return [pa.array(values, type=pa.float32())]
        if mode == "non_arrow":
            return [[1.0] * size]
        if mode == "wrong_rows":
            return [pa.array([1.0] * (size - 1), type=pa.float32())]
        if mode == "unsupported_type":
            return [pa.array([b"x"] * size, type=pa.binary())]
        if mode == "two_outputs":
            return [
                pa.array([1.0] * size, type=pa.float32()),
                pa.array([2.0] * size, type=pa.float32()),
            ]
        if mode == "null_score":
            return [pa.array([None] * size, type=pa.float32())]
        if mode == "count_changes":
            values = pa.array([1.0] * size, type=pa.float32())
            if self._next_chunk(mode) == 0:
                return [values]
            return [values, pa.array([2.0] * size, type=pa.float32())]
        if mode == "type_changes":
            if self._next_chunk(mode) == 0:
                return [pa.array([1.0] * size, type=pa.float32())]
            return [pa.array([1] * size, type=pa.int64())]
        raise ValueError(f"unknown test mode: {mode}")


def create_udf(context):
    return UDF(context)
"""

        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as environment:
            client, collection_name, resource_name = environment

            def chain(udf_params):
                return (
                    FunctionChain(FunctionChainStage.L2_RERANK, name=f"pyudf_{udf_params['mode']}")
                    .map(
                        "$score",
                        FunctionChainExpr(
                            "py_udf",
                            args=(col("$score"), col(self.scalar_field)),
                            params={"resource_name": resource_name, "udf_params": udf_params},
                        ),
                    )
                    .sort(col("$score"), desc=True, tie_break_col=col("$id"))
                )

            nested_params = {
                "mode": "nested_params",
                "factor": 2.0,
                "enabled": True,
                "count": 7,
                "ratio": 1.25,
                "label": "中文",
                "payload": b"\x00\xff",
                "items": [1, {"nested": ["a", b"\x80"]}],
            }
            result = self._search_with_synced_resource(client, collection_name, chain(nested_params))
            assert [hit["id"] for hit in result[0]] == [3, 2, 1]
            assert [hit["distance"] for hit in result[0]] == pytest.approx([60.0, 40.0, 20.0])

            two_queries = [[0.0, 0.0], [0.1, 0.1]]
            failure_cases = [
                ("non_arrow", "outputs must be pyarrow.Array objects", None),
                ("wrong_rows", "has 2 rows, expected 3", None),
                ("decimal32", "unsupported type decimal32", None),
                ("unsupported_type", "unsupported type binary", None),
                ("two_outputs", "function returned 2 outputs, expected 1", None),
                ("null_score", "$score contains null", None),
                ("count_changes", "output column count/type differs between query batches", two_queries),
                ("type_changes", "output column count/type differs between query batches", two_queries),
            ]
            for mode, expected_message, queries in failure_cases:
                with pytest.raises(MilvusException) as captured:
                    self._search_with_synced_resource(
                        client,
                        collection_name,
                        chain({"mode": mode}),
                        data=queries,
                    )
                assert captured.value.code == 2400, mode
                assert expected_message in str(captured.value), mode

            recovered = self._search_with_synced_resource(client, collection_name, chain(nested_params))
            assert [hit["distance"] for hit in recovered[0]] == pytest.approx([60.0, 40.0, 20.0])

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_pyudf_intermediate_column_and_decay(self, tmp_path, minio_host, minio_bucket):
        """
        target: verify a built-in operator can consume a PyUDF-generated intermediate column
        method: map a temporary feature with PyUDF, apply linear decay to $score, then sort descending
        expected: Search returns deterministic decay scores and does not expose the temporary column
        """
        from minio import Minio

        client = self._client()
        package_name = cf.gen_unique_str("pyudf_decay_e2e").lower()
        resource_name = cf.gen_unique_str("pyudf_decay_resource")
        collection_name = cf.gen_unique_str(prefix)
        remote_path = f"pyudf/{resource_name}.whl"
        wheel_path = self._build_wheel(tmp_path, package_name)
        minio_client = Minio(
            f"{minio_host}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        resource_added = False
        resource_removed = False
        collection_created = False
        object_uploaded = False

        try:
            if not minio_client.bucket_exists(minio_bucket):
                minio_client.make_bucket(minio_bucket)
            wheel_data = wheel_path.read_bytes()
            minio_client.put_object(
                minio_bucket,
                remote_path,
                io.BytesIO(wheel_data),
                len(wheel_data),
            )
            object_uploaded = True

            _, added = self.add_file_resource(client, resource_name, remote_path)
            assert added
            resource_added = True
            resources, listed = self.list_file_resources(client)
            assert listed
            assert any(resource.name == resource_name and resource.path == remote_path for resource in resources)

            schema, schema_created = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
            assert schema_created
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field(self.scalar_field, DataType.INT64)
            schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)
            index_params, index_params_created = self.prepare_index_params(client)
            assert index_params_created
            index_params.add_index(field_name=self.vector_field, index_type="FLAT", metric_type="L2")
            _, collection_created = self.create_collection(
                client,
                collection_name,
                schema=schema,
                index_params=index_params,
                consistency_level="Strong",
            )
            assert collection_created
            rows = [
                {"id": 1, self.scalar_field: 10, self.vector_field: [0.0, 0.0]},
                {"id": 2, self.scalar_field: 20, self.vector_field: [0.01, 0.0]},
                {"id": 3, self.scalar_field: 30, self.vector_field: [0.02, 0.0]},
            ]
            _, inserted = self.insert(client, collection_name, rows)
            assert inserted
            _, flushed = self.flush(client, collection_name)
            assert flushed
            _, loaded = self.load_collection(client, collection_name)
            assert loaded

            baseline, baseline_searched = self.search(
                client,
                collection_name,
                data=[[0.0, 0.0]],
                anns_field=self.vector_field,
                search_params={"metric_type": "L2"},
                limit=3,
                output_fields=[self.scalar_field],
            )
            assert baseline_searched
            assert [hit["id"] for hit in baseline[0]] == [1, 2, 3]

            intermediate_column = "pyudf_feature"
            chain = (
                FunctionChain(FunctionChainStage.L2_RERANK, name="pyudf_decay_e2e")
                .map(
                    intermediate_column,
                    FunctionChainExpr(
                        "py_udf",
                        args=(col("$score"), col(self.scalar_field)),
                        params={
                            "resource_name": resource_name,
                            "udf_params": {"factor": 2.0},
                        },
                    ),
                )
                .map(
                    "$score",
                    fn.decay(
                        col(intermediate_column),
                        function="linear",
                        origin=60,
                        scale=40,
                        offset=0,
                        decay=0.5,
                    ),
                )
                .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            )
            result = self._search_with_synced_resource(client, collection_name, chain)

            assert [hit["id"] for hit in result[0]] == [3, 2, 1]
            assert [hit["distance"] for hit in result[0]] == pytest.approx([1.0, 0.75, 0.5])
            assert [self._hit_field(hit, self.scalar_field) for hit in result[0]] == [
                30,
                20,
                10,
            ]
            self._assert_intermediate_field_absent(result[0], intermediate_column)
        finally:
            if client is not None:
                if collection_created:
                    try:
                        self.drop_collection(client, collection_name)
                    except Exception:
                        pass
                if resource_added:
                    try:
                        self._remove_file_resource(client, resource_name)
                        resource_removed = True
                    except Exception:
                        pass
                try:
                    client.close()
                except Exception:
                    pass
            if object_uploaded and (not resource_added or resource_removed):
                try:
                    minio_client.remove_object(minio_bucket, remote_path)
                except Exception:
                    pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_multiple_pyudf_resources(self, tmp_path, minio_host, minio_bucket):
        """
        target: verify multiple PyUDF resources can execute sequentially in one FunctionChain
        method: map ts with one PyUDF, consume that temporary column with another PyUDF, then sort
        expected: Search returns scores composed by both PyUDFs and hides the temporary column
        """
        from minio import Minio

        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        first_package = cf.gen_unique_str("pyudf_first_e2e").lower()
        second_package = cf.gen_unique_str("pyudf_second_e2e").lower()
        first_resource = cf.gen_unique_str("pyudf_first_resource")
        second_resource = cf.gen_unique_str("pyudf_second_resource")
        resources = [
            (
                first_resource,
                f"pyudf/{first_resource}.whl",
                self._build_wheel(tmp_path, first_package),
            ),
            (
                second_resource,
                f"pyudf/{second_resource}.whl",
                self._build_wheel(tmp_path, second_package),
            ),
        ]
        minio_client = Minio(
            f"{minio_host}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        uploaded_resources = set()
        added_resources = set()
        removed_resources = set()
        collection_created = False

        try:
            if not minio_client.bucket_exists(minio_bucket):
                minio_client.make_bucket(minio_bucket)
            for resource_name, remote_path, wheel_path in resources:
                wheel_data = wheel_path.read_bytes()
                minio_client.put_object(
                    minio_bucket,
                    remote_path,
                    io.BytesIO(wheel_data),
                    len(wheel_data),
                )
                uploaded_resources.add(resource_name)

                _, added = self.add_file_resource(client, resource_name, remote_path)
                assert added
                added_resources.add(resource_name)

            listed_resources, listed = self.list_file_resources(client)
            assert listed
            listed_paths = {resource.name: resource.path for resource in listed_resources}
            assert listed_paths[first_resource] == resources[0][1]
            assert listed_paths[second_resource] == resources[1][1]

            schema, schema_created = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
            assert schema_created
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field(self.scalar_field, DataType.INT64)
            schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)
            index_params, index_params_created = self.prepare_index_params(client)
            assert index_params_created
            index_params.add_index(field_name=self.vector_field, index_type="FLAT", metric_type="L2")
            _, collection_created = self.create_collection(
                client,
                collection_name,
                schema=schema,
                index_params=index_params,
                consistency_level="Strong",
            )
            assert collection_created
            rows = [
                {"id": 1, self.scalar_field: 10, self.vector_field: [0.0, 0.0]},
                {"id": 2, self.scalar_field: 20, self.vector_field: [0.01, 0.0]},
                {"id": 3, self.scalar_field: 30, self.vector_field: [0.02, 0.0]},
            ]
            _, inserted = self.insert(client, collection_name, rows)
            assert inserted
            _, flushed = self.flush(client, collection_name)
            assert flushed
            _, loaded = self.load_collection(client, collection_name)
            assert loaded

            baseline, baseline_searched = self.search(
                client,
                collection_name,
                data=[[0.0, 0.0]],
                anns_field=self.vector_field,
                search_params={"metric_type": "L2"},
                limit=3,
                output_fields=[self.scalar_field],
            )
            assert baseline_searched
            assert [hit["id"] for hit in baseline[0]] == [1, 2, 3]

            intermediate_column = "first_pyudf_feature"
            chain = (
                FunctionChain(FunctionChainStage.L2_RERANK, name="multi_pyudf_e2e")
                .map(
                    intermediate_column,
                    FunctionChainExpr(
                        "py_udf",
                        args=(col("$score"), col(self.scalar_field)),
                        params={
                            "resource_name": first_resource,
                            "udf_params": {"factor": 2.0},
                        },
                    ),
                )
                .map(
                    "$score",
                    FunctionChainExpr(
                        "py_udf",
                        args=(col("$score"), col(intermediate_column)),
                        params={
                            "resource_name": second_resource,
                            "udf_params": {"factor": 3.0},
                        },
                    ),
                )
                .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            )
            result = self._search_with_synced_resource(client, collection_name, chain)

            assert [hit["id"] for hit in result[0]] == [3, 2, 1]
            assert [hit["distance"] for hit in result[0]] == pytest.approx([180.0, 120.0, 60.0])
            assert [self._hit_field(hit, self.scalar_field) for hit in result[0]] == [
                30,
                20,
                10,
            ]
            self._assert_intermediate_field_absent(result[0], intermediate_column)
        finally:
            if client is not None:
                if collection_created:
                    try:
                        self.drop_collection(client, collection_name)
                    except Exception:
                        pass
                for resource_name in reversed([first_resource, second_resource]):
                    if resource_name not in added_resources:
                        continue
                    try:
                        self._remove_file_resource(client, resource_name)
                        removed_resources.add(resource_name)
                    except Exception:
                        pass
                try:
                    client.close()
                except Exception:
                    pass
            for resource_name, remote_path, _ in resources:
                if resource_name not in uploaded_resources or (
                    resource_name in added_resources and resource_name not in removed_resources
                ):
                    continue
                try:
                    minio_client.remove_object(minio_bucket, remote_path)
                except Exception:
                    pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_remove_pyudf_resource_invalidates_only_removed_udf(self, tmp_path, minio_host, minio_bucket):
        """
        target: verify deleting a PyUDF invalidates its Runtime cache without affecting another PyUDF
        method: execute two resources, remove one, retry it until not found, then execute the other again
        expected: the removed PyUDF fails while the remaining PyUDF keeps returning correct scores
        """
        from minio import Minio

        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        removed_package = cf.gen_unique_str("pyudf_removed_e2e").lower()
        retained_package = cf.gen_unique_str("pyudf_retained_e2e").lower()
        removed_resource = cf.gen_unique_str("pyudf_removed_resource")
        retained_resource = cf.gen_unique_str("pyudf_retained_resource")
        resources = [
            (
                removed_resource,
                f"pyudf/{removed_resource}.whl",
                self._build_wheel(tmp_path, removed_package),
            ),
            (
                retained_resource,
                f"pyudf/{retained_resource}.whl",
                self._build_wheel(tmp_path, retained_package),
            ),
        ]
        minio_client = Minio(
            f"{minio_host}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        uploaded_resources = set()
        added_resources = set()
        removed_resources = set()
        collection_created = False

        def pyudf_chain(name, resource_name, factor):
            return (
                FunctionChain(FunctionChainStage.L2_RERANK, name=name)
                .map(
                    "$score",
                    FunctionChainExpr(
                        "py_udf",
                        args=(col("$score"), col(self.scalar_field)),
                        params={
                            "resource_name": resource_name,
                            "udf_params": {"factor": factor},
                        },
                    ),
                )
                .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            )

        try:
            if not minio_client.bucket_exists(minio_bucket):
                minio_client.make_bucket(minio_bucket)
            for resource_name, remote_path, wheel_path in resources:
                wheel_data = wheel_path.read_bytes()
                minio_client.put_object(
                    minio_bucket,
                    remote_path,
                    io.BytesIO(wheel_data),
                    len(wheel_data),
                )
                uploaded_resources.add(resource_name)

                _, added = self.add_file_resource(client, resource_name, remote_path)
                assert added
                added_resources.add(resource_name)

            schema, schema_created = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
            assert schema_created
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field(self.scalar_field, DataType.INT64)
            schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)
            index_params, index_params_created = self.prepare_index_params(client)
            assert index_params_created
            index_params.add_index(field_name=self.vector_field, index_type="FLAT", metric_type="L2")
            _, collection_created = self.create_collection(
                client,
                collection_name,
                schema=schema,
                index_params=index_params,
                consistency_level="Strong",
            )
            assert collection_created
            rows = [
                {"id": 1, self.scalar_field: 10, self.vector_field: [0.0, 0.0]},
                {"id": 2, self.scalar_field: 20, self.vector_field: [0.01, 0.0]},
                {"id": 3, self.scalar_field: 30, self.vector_field: [0.02, 0.0]},
            ]
            _, inserted = self.insert(client, collection_name, rows)
            assert inserted
            _, flushed = self.flush(client, collection_name)
            assert flushed
            _, loaded = self.load_collection(client, collection_name)
            assert loaded

            removed_chain = pyudf_chain("removed_pyudf_e2e", removed_resource, 2.0)
            retained_chain = pyudf_chain("retained_pyudf_e2e", retained_resource, 3.0)
            removed_result = self._search_with_synced_resource(client, collection_name, removed_chain)
            retained_result = self._search_with_synced_resource(client, collection_name, retained_chain)
            assert [hit["distance"] for hit in removed_result[0]] == pytest.approx([60.0, 40.0, 20.0])
            assert [hit["distance"] for hit in retained_result[0]] == pytest.approx([90.0, 60.0, 30.0])

            self._remove_file_resource(client, removed_resource)
            removed_resources.add(removed_resource)
            self._wait_for_file_resource_absent(client, removed_resource)
            self._wait_for_removed_pyudf(
                client,
                collection_name,
                removed_chain,
                removed_resource,
            )

            retained_result = self._search_with_synced_resource(client, collection_name, retained_chain)
            assert [hit["id"] for hit in retained_result[0]] == [3, 2, 1]
            assert [hit["distance"] for hit in retained_result[0]] == pytest.approx([90.0, 60.0, 30.0])
        finally:
            if client is not None:
                if collection_created:
                    try:
                        self.drop_collection(client, collection_name)
                    except Exception:
                        pass
                for resource_name in reversed([removed_resource, retained_resource]):
                    if resource_name not in added_resources or resource_name in removed_resources:
                        continue
                    try:
                        self._remove_file_resource(client, resource_name)
                        removed_resources.add(resource_name)
                    except Exception:
                        pass
                try:
                    client.close()
                except Exception:
                    pass
            for resource_name, remote_path, _ in resources:
                if resource_name not in uploaded_resources or (
                    resource_name in added_resources and resource_name not in removed_resources
                ):
                    continue
                try:
                    minio_client.remove_object(minio_bucket, remote_path)
                except Exception:
                    pass

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "params, arguments, message",
        [
            pytest.param({}, ("ts",), "resource_name", id="missing_resource"),
            pytest.param({"resource_name": ""}, ("ts",), "resource_name", id="empty_resource"),
            pytest.param({"resource_name": "  "}, ("ts",), "resource_name", id="blank_resource"),
            pytest.param({"resource_name": 7}, ("ts",), "must be a string", id="resource_type"),
            pytest.param({"udf_params": "bad"}, ("ts",), "udf_params", id="params_string"),
            pytest.param({"udf_params": []}, ("ts",), "udf_params", id="params_array"),
            pytest.param({"unexpected": True}, ("ts",), "unknown parameter", id="unknown_parameter"),
            pytest.param({}, (), "at least one input", id="no_inputs"),
            pytest.param({}, (1,), "literal", id="literal_input"),
        ],
    )
    def test_pyudf_invalid_expression_is_rejected(self, tmp_path, minio_host, minio_bucket, params, arguments, message):
        """
        target: reject invalid PyUDF expression contracts without harming valid requests
        method: submit one malformed expression against a synchronized control resource
        expected: code 1100 and a focused error; ordinary and valid UDF Search remain correct
        """
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as environment:
            client, name, resource = environment
            self._assert_pyudf_recovery(client, name, resource)
            values = {"resource_name": resource, **params}
            if message == "resource_name" and not params:
                values = {}
            args = tuple(col(arg) if isinstance(arg, str) else arg for arg in arguments)
            chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
                "$score", FunctionChainExpr("py_udf", args=args, params=values)
            )
            self._assert_pyudf_error(client, name, chain, message)
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "stage",
        [
            FunctionChainStage.L0_RERANK,
            FunctionChainStage.L1_RERANK,
            FunctionChainStage.INGESTION,
            FunctionChainStage.PRE_PROCESS,
            FunctionChainStage.POST_PROCESS,
        ],
        ids=["l0", "l1", "ingestion", "pre_process", "post_process"],
    )
    def test_pyudf_unsupported_stages_are_rejected(self, tmp_path, minio_host, minio_bucket, stage):
        """
        target: keep Python UDF execution restricted to Proxy L2
        method: submit the same UDF at an unsupported stage, then at L2
        expected: unsupported stage returns code 1100; ordinary and L2 Search remain correct
        """
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as environment:
            client, name, resource = environment
            self._assert_pyudf_recovery(client, name, resource)
            chain = FunctionChain(stage).map(
                "$score", FunctionChainExpr("py_udf", args=(col("ts"),), params={"resource_name": resource})
            )
            self._assert_pyudf_error(client, name, chain, "stage")
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "dtype, expected_error",
        [
            pytest.param(dtype, None, id=dtype)
            for dtype in ["int8", "int16", "int32", "int64", "float32", "float64", "bool", "string"]
        ]
        + [
            pytest.param("bool_score", "must be numeric", id="bool_score"),
            pytest.param("string_score", "must be numeric", id="string_score"),
            pytest.param("uint64", "unsupported type", id="uint64"),
            pytest.param("binary", "unsupported type", id="binary"),
            pytest.param("decimal128", "unsupported type", id="decimal128"),
        ],
    )
    def test_pyudf_output_types_and_shape_contracts(self, tmp_path, minio_host, minio_bucket, dtype, expected_error):
        """
        target: validate supported outputs, score conversion and nonnumeric score rejection
        method: return typed scores and inspect intermediate Arrow types/boundary values in a second UDF
        expected: valid results preserve IDs/fields; rejected outputs return 2400 and valid calls recover
        """
        module = """import pyarrow as pa

NUMERIC_VALUES = {
    "int8": [-128, -1, 127],
    "int16": [-32768, -1, 32767],
    "int32": [-2147483648, -1, 2147483647],
    "int64": [-9223372036854775808, -1, 9223372036854775807],
    "float32": [-1.25, 2.0 ** -20, 16777216.0],
    "float64": [-1.25, 2.0 ** -40, 9007199254740991.0],
}

class UDF:
    def transform_query(self, params, columns):
        mode = params.get("mode", "float32")
        if mode in ("produce_numeric", "consume_numeric"):
            dtype = params["dtype"]
            expected = [NUMERIC_VALUES[dtype][v.as_py() // 10 - 1] for v in columns[1]]
            if mode == "produce_numeric":
                return [pa.array(expected, type=getattr(pa, dtype)())]
            if columns[0].type != getattr(pa, dtype)():
                raise AssertionError("numeric intermediate output type changed")
            if columns[0].to_pylist() != expected:
                raise AssertionError("numeric intermediate output values changed")
            return [pa.array([v.as_py() * 2.0 for v in columns[1]], type=pa.float32())]
        if mode == "consume":
            expected = pa.bool_() if params["dtype"] == "bool" else pa.string()
            if columns[0].type != expected:
                raise AssertionError("intermediate output type changed")
            values = [float(value.as_py()) * 2 for value in columns[1]]
            expected_values = [value > 30 for value in values] if params["dtype"] == "bool" else [str(int(value)) for value in values]
            if columns[0].to_pylist() != expected_values:
                raise AssertionError("intermediate output values changed")
            return [pa.array(values, type=pa.float32())]
        numbers = [int(value.as_py()) * 2 for value in columns[1]]
        if mode in ("bool", "bool_score"):
            return [pa.array([value > 30 for value in numbers], type=pa.bool_())]
        if mode in ("string", "string_score"):
            return [pa.array([str(value) for value in numbers], type=pa.string())]
        if mode == "binary":
            return [pa.array([b"x"] * len(numbers), type=pa.binary())]
        if mode == "decimal128":
            return [pa.array(numbers, type=pa.decimal128(9, 0))]
        return [pa.array(numbers, type=getattr(pa, mode)())]

def create_udf(context):
    return UDF()
"""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as environment:
            client, name, resource = environment
            self._assert_pyudf_recovery(client, name, resource)
            if dtype in ("bool", "string"):
                chain = (
                    FunctionChain(FunctionChainStage.L2_RERANK)
                    .map(
                        "typed_value",
                        FunctionChainExpr(
                            "py_udf",
                            args=(col("$score"), col("ts")),
                            params={"resource_name": resource, "udf_params": {"mode": dtype}},
                        ),
                    )
                    .map(
                        "$score",
                        FunctionChainExpr(
                            "py_udf",
                            args=(col("typed_value"), col("ts")),
                            params={"resource_name": resource, "udf_params": {"mode": "consume", "dtype": dtype}},
                        ),
                    )
                    .sort(col("$score"), desc=True, tie_break_col=col("$id"))
                )
            else:
                chain = self._pyudf_chain(resource, {"mode": dtype})
            if expected_error:
                self._assert_pyudf_error(client, name, chain, expected_error, code=2400)
            else:
                result = self._search_with_synced_resource(client, name, chain)
                assert [hit["id"] for hit in result[0]] == [3, 2, 1]
                assert [hit["distance"] for hit in result[0]] == pytest.approx([60, 40, 20], rel=1e-5, abs=1e-5)
                assert [self._hit_field(hit, "ts") for hit in result[0]] == [30, 20, 10]
                self._assert_intermediate_field_absent(result[0], "typed_value")
                if dtype not in ("bool", "string"):
                    # Keep the direct numeric-to-score assertion above, and
                    # separately check precision before the final Float32 cast.
                    typed_chain = FunctionChain(FunctionChainStage.L2_RERANK)
                    for output, input_column, mode in [
                        ("typed_value", "$score", "produce_numeric"),
                        ("$score", "typed_value", "consume_numeric"),
                    ]:
                        typed_chain.map(
                            output,
                            FunctionChainExpr(
                                "py_udf",
                                args=(col(input_column), col("ts")),
                                params={"resource_name": resource, "udf_params": {"mode": mode, "dtype": dtype}},
                            ),
                        )
                    typed_chain.sort(col("$score"), desc=True, tie_break_col=col("$id"))
                    typed = self._search_with_synced_resource(client, name, typed_chain, data=[[0.0, 0.0], [0.02, 0.0]])
                    assert len(typed) == 2
                    for hits in typed:
                        assert [hit["id"] for hit in hits] == [3, 2, 1]
                        assert [hit["distance"] for hit in hits] == pytest.approx([60, 40, 20])
                        assert [self._hit_field(hit, "ts") for hit in hits] == [30, 20, 10]
                        self._assert_intermediate_field_absent(hits, "typed_value")
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mode", ["null", "nan", "positive_inf", "negative_inf", "overflow"])
    @pytest.mark.parametrize("repair", [False, True], ids=["final_invalid", "intermediate_replaced"])
    def test_pyudf_nonfinite_score_boundaries(self, tmp_path, minio_host, minio_bucket, mode, repair):
        """
        target: enforce finite final scores without forbidding replaceable intermediate values
        method: return invalid float64 scores and optionally overwrite them before export
        expected: invalid final scores fail with 2400; replacement and later valid calls are correct
        """
        module = """import math
import pyarrow as pa

class UDF:
    def transform_query(self, params, columns):
        mode = params.get("mode", "valid")
        if mode in ("valid", "repair"):
            if mode == "repair":
                invalid = params["invalid_mode"]
                for value in columns[0].to_pylist():
                    if invalid == "null":
                        matches = value is None
                    elif invalid == "nan":
                        matches = value is not None and math.isnan(value)
                    elif invalid == "negative_inf":
                        matches = value == -float("inf")
                    else:
                        matches = value == float("inf")
                    if not matches:
                        raise AssertionError("invalid intermediate score was not observed before replacement")
            return [pa.array([float(value.as_py()) * 2 for value in columns[1]], type=pa.float32())]
        value = {"null": None, "nan": float("nan"), "positive_inf": float("inf"),
                 "negative_inf": -float("inf"), "overflow": 1e40}[mode]
        return [pa.array([value] * len(columns[0]), type=pa.float64())]

def create_udf(context):
    return UDF()
"""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as environment:
            client, name, resource = environment
            self._assert_pyudf_recovery(client, name, resource)
            chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
                "$score",
                FunctionChainExpr(
                    "py_udf",
                    args=(col("$score"), col("ts")),
                    params={"resource_name": resource, "udf_params": {"mode": mode}},
                ),
            )
            if repair:
                chain.map(
                    "$score",
                    FunctionChainExpr(
                        "py_udf",
                        args=(col("$score"), col("ts")),
                        params={"resource_name": resource, "udf_params": {"mode": "repair", "invalid_mode": mode}},
                    ),
                )
            chain.sort(col("$score"), desc=True, tie_break_col=col("$id"))
            if repair:
                result = self._search_with_synced_resource(client, name, chain)
                assert [hit["id"] for hit in result[0]] == [3, 2, 1]
                assert [hit["distance"] for hit in result[0]] == pytest.approx([60, 40, 20], rel=1e-5, abs=1e-5)
                assert [self._hit_field(hit, "ts") for hit in result[0]] == [30, 20, 10]
            else:
                self._assert_pyudf_error(client, name, chain, "$score", code=2400)
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    def test_pyudf_empty_search_validation_boundary(self, tmp_path, minio_host, minio_bucket):
        """
        target: distinguish empty-result optimization from static expression validation
        method: filter out all rows for valid and invalid expressions after a nonempty control
        expected: valid empty Search returns no hits; malformed static parameters still fail with 1100
        """
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as environment:
            client, name, resource = environment
            self._assert_pyudf_recovery(client, name, resource)
            result, _ = self.search(
                client,
                name,
                data=[[0.0, 0.0]],
                anns_field=self.vector_field,
                search_params={"metric_type": "L2"},
                limit=3,
                filter="id < 0",
                function_chains=self._pyudf_chain(resource),
            )
            assert len(result) == 1 and len(result[0]) == 0
            invalid = FunctionChain(FunctionChainStage.L2_RERANK).map(
                "$score", FunctionChainExpr("py_udf", args=(col("ts"),), params={})
            )
            self._assert_pyudf_error(client, name, invalid, "resource_name", filter="id < 0")
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("output", ["$id", "$group_size"], ids=["id", "group_size"])
    def test_pyudf_readonly_system_outputs_are_rejected(self, tmp_path, minio_host, minio_bucket, output):
        """
        target: preserve readonly system fields when a UDF writes outputs
        method: target a protected system column, then run builtin and valid UDF controls
        expected: code 1100; IDs remain unchanged and existing reranking remains usable
        """
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as environment:
            client, name, resource = environment
            self._assert_pyudf_recovery(client, name, resource)
            chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
                output, FunctionChainExpr("py_udf", args=(col("$score"), col("ts")), params={"resource_name": resource})
            )
            self._assert_pyudf_error(client, name, chain, "system output")
            builtin = (
                FunctionChain(FunctionChainStage.L2_RERANK)
                .map("$score", fn.num_combine(col("ts"), col("ts"), mode="sum"))
                .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            )
            result = self._search_with_synced_resource(client, name, builtin)
            assert [hit["id"] for hit in result[0]] == [3, 2, 1]
            assert [hit["distance"] for hit in result[0]] == pytest.approx([60, 40, 20], rel=1e-5, abs=1e-5)
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    def test_pyudf_missing_resource_does_not_fallback(self, tmp_path, minio_host, minio_bucket):
        """
        target: distinguish a missing resource in a ready snapshot from successful ANN fallback
        method: warm a valid UDF, request an unregistered resource, and retry the control
        expected: code 1100 names the missing resource; both control paths preserve content
        """
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as environment:
            client, name, resource = environment
            self._assert_pyudf_recovery(client, name, resource)
            missing = cf.gen_unique_str("pyudf_missing")
            self._assert_pyudf_error(client, name, self._pyudf_chain(missing), missing)
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "module, entry_points, wheel_bytes, message",
        [
            pytest.param(None, None, b"invalid wheel bytes", "cannot open local wheel", id="invalid_zip"),
            pytest.param(None, "", None, "exactly one milvus.pyudf entry point", id="missing_entry"),
            pytest.param(
                None,
                "[milvus.pyudf]\na = x:f\nb = x:f\n",
                None,
                "exactly one milvus.pyudf entry point",
                id="multiple_entries",
            ),
            pytest.param("other_factory = 1\n", None, None, "cannot resolve PyUDF entry factory", id="missing_factory"),
            pytest.param("create_udf = 1\n", None, None, "is not callable", id="noncallable_factory"),
            pytest.param(
                "def create_udf(context):\n    raise RuntimeError('factory failure')\n",
                None,
                None,
                "factory raised an exception",
                id="factory_exception",
            ),
            pytest.param(
                "def create_udf(context):\n    return object()\n",
                None,
                None,
                "exactly one callable",
                id="invalid_instance",
            ),
            pytest.param(
                "class UDF:\n    def transform(self, params, columns):\n        return []\n"
                "def create_udf(context):\n    return UDF()\n",
                None,
                None,
                "does not implement transform_query",
                id="transform_only",
            ),
            pytest.param(
                "import missing_pyudf_dependency_52001\n",
                None,
                None,
                "cannot import PyUDF entry module",
                id="missing_dependency",
            ),
        ],
    )
    def test_pyudf_wheel_loading_failures(
        self, tmp_path, minio_host, minio_bucket, module, entry_points, wheel_bytes, message
    ):
        """
        target: reject real wheel load failures while preserving another resource
        method: upload each malformed wheel and invoke it beside a synchronized valid control
        expected: code 2400 carries UDF_FAILED and the cause; the control UDF and plain Search recover
        """
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as control:
            client, name, resource = control
            self._assert_pyudf_recovery(client, name, resource)
            with self._pyudf_test_environment(
                tmp_path, minio_host, minio_bucket, module, entry_points=entry_points, wheel_bytes=wheel_bytes
            ) as invalid:
                invalid_client, invalid_name, invalid_resource = invalid
                # Poll only the known asynchronous resource-visibility phase.
                with pytest.raises(MilvusException) as captured:
                    self._search_with_synced_resource(invalid_client, invalid_name, self._pyudf_chain(invalid_resource))
                assert captured.value.code == 2400
                assert "UDF_FAILED" in str(captured.value)
                assert message in str(captured.value)
                baseline, _ = self.search(
                    invalid_client,
                    invalid_name,
                    data=[[0.0, 0.0]],
                    anns_field=self.vector_field,
                    search_params={"metric_type": "L2"},
                    limit=3,
                )
                assert [hit["id"] for hit in baseline[0]] == [1, 2, 3]
                self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "bad_value, message",
        [
            pytest.param(None, "None", id="null_params"),
            pytest.param({"key": None}, "None", id="nested_null"),
            pytest.param({"key": [None]}, "None", id="null_array_item"),
            pytest.param({"": 1}, "non-empty strings", id="empty_key"),
            pytest.param({1: "bad"}, "non-empty strings", id="nonstring_key"),
        ],
    )
    def test_pyudf_parameter_boundary_errors(self, tmp_path, minio_host, minio_bucket, bad_value, message):
        """
        target: identify unsupported SDK parameter values at serialization rather than worker execution
        method: serialize an invalid expression, then issue a valid Search against the same control
        expected: ParamError identifies the invalid value and subsequent ordinary/UDF results remain correct
        """
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as environment:
            client, name, resource = environment
            self._assert_pyudf_recovery(client, name, resource)
            chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
                "$score",
                FunctionChainExpr(
                    "py_udf", args=(col("ts"),), params={"resource_name": resource, "udf_params": bad_value}
                ),
            )
            with pytest.raises(ParamError, match=message):
                chain.to_proto()
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("arrow_type", ["timestamp", "date32", "duration", "list", "struct"])
    def test_pyudf_rejects_complex_arrow_outputs(self, tmp_path, minio_host, minio_bucket, arrow_type):
        """target: reject unsupported Arrow outputs; method: execute then recover; expected: 2400 and intact results."""
        module = """import pyarrow as pa
class UDF:
    def transform_query(self, params, columns):
        mode = params.get("mode")
        if mode:
            types = {"timestamp": pa.timestamp("ms"), "date32": pa.date32(),
                     "duration": pa.duration("ms"), "list": pa.list_(pa.int64()),
                     "struct": pa.struct([("v", pa.int64())])}
            value = [1] if mode == "list" else {"v": 1} if mode == "struct" else 1
            return [pa.array([value] * len(columns[0]), type=types[mode])]
        return [pa.array([v.as_py() * 2.0 for v in columns[1]], type=pa.float32())]
def create_udf(context):
    return UDF()
"""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as (client, name, resource):
            self._assert_pyudf_recovery(client, name, resource)
            self._assert_pyudf_error(
                client, name, self._pyudf_chain(resource, {"mode": arrow_type}), "unsupported type", code=2400
            )
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "payload",
        [b"", b"\x00\xff\x80", "中文\x00🙂", [[], {"中文": b"\x00"}]],
        ids=["empty_bytes", "binary", "unicode", "nested"],
    )
    def test_pyudf_parameter_values_round_trip(self, tmp_path, minio_host, minio_bucket, payload):
        """target: preserve parameter values; method: compare inside worker; expected: exact values and correct scores."""
        expected = tuple(payload) if isinstance(payload, list) else payload
        module = f"""import pyarrow as pa
class UDF:
    def transform_query(self, params, columns):
        value = params["payload"]
        expected = {expected!r}
        if isinstance(expected, tuple):
            if value[0] != () or dict(value[1]) != expected[1]:
                raise AssertionError("nested parameter changed")
        elif value != expected or type(value) is not type(expected):
            raise AssertionError("parameter bytes/type changed")
        return [pa.array([v.as_py() * 2.0 for v in columns[1]], type=pa.float32())]
def create_udf(context):
    return UDF()
"""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as (client, name, resource):
            result = self._search_with_synced_resource(client, name, self._pyudf_chain(resource, {"payload": payload}))
            assert [hit["id"] for hit in result[0]] == [3, 2, 1]
            assert [hit["distance"] for hit in result[0]] == pytest.approx([60, 40, 20])

    @pytest.mark.tags(CaseLabel.L1)
    def test_pyudf_empty_collection_still_validates_expression(self, tmp_path, minio_host, minio_bucket):
        """target: empty collection boundary; method: delete all rows then search; expected: empty or static 1100."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (client, name, resource):
            self._assert_pyudf_recovery(client, name, resource)
            self.delete(client, name, ids=[1, 2, 3])
            self.flush(client, name)
            rows, _ = self.query(client, name, filter="", output_fields=["count(*)"])
            assert rows == [{"count(*)": 0}]
            result = self._search_with_synced_resource(client, name, self._pyudf_chain(resource))
            assert len(result) == 1 and list(result[0]) == []
            bad = FunctionChain(FunctionChainStage.L2_RERANK).map(
                "$score", FunctionChainExpr("py_udf", args=(col("ts"),), params={})
            )
            self._assert_pyudf_error(client, name, bad, "resource_name")
            self.insert(client, name, [{"id": 4, "ts": 40, "vector": [0.0, 0.0]}])
            restored = self._search_with_synced_resource(client, name, self._pyudf_chain(resource))
            assert [hit["id"] for hit in restored[0]] == [4]
            assert [hit["distance"] for hit in restored[0]] == pytest.approx([80])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index_type", ["FLAT", "HNSW"])
    @pytest.mark.parametrize("mmap", [False, True], ids=["memory", "mmap"])
    def test_pyudf_indexes_reload_and_mmap(self, tmp_path, minio_host, minio_bucket, index_type, mmap):
        """target: indexed/reloaded inputs; method: verify index metadata and complete candidate set; expected: exact top3."""
        rows = [{"id": i, "ts": i * 10, "vector": [i / 10000.0, 0.0]} for i in range(1, 1101)]
        with self._pyudf_test_environment(
            tmp_path, minio_host, minio_bucket, None, rows=rows, index_type=index_type
        ) as (client, name, resource):
            if index_type == "HNSW":
                deadline = time.monotonic() + 90
                while True:
                    info = self.describe_index(client, name, "vector")[0]
                    if int(info.get("indexed_rows", 0)) >= len(rows):
                        break
                    assert time.monotonic() < deadline, info
                    time.sleep(0.2)
            self.release_collection(client, name)
            self.alter_index_properties(client, name, "vector", properties={"mmap.enabled": mmap})
            info = self.describe_index(client, name, "vector")[0]
            assert str(info.get("mmap.enabled")).lower() == str(mmap).lower(), info
            chain = self._pyudf_chain(resource).limit(3)
            for _ in range(2):
                self.load_collection(client, name)
                params = {"metric_type": "L2", "params": {"ef": len(rows)}}
                baseline, _ = self.search(
                    client,
                    name,
                    data=[[0.0, 0.0]],
                    anns_field="vector",
                    search_params=params,
                    limit=len(rows),
                    output_fields=["ts"],
                )
                assert {hit["id"] for hit in baseline[0]} == {row["id"] for row in rows}
                result = self._search_with_synced_resource(client, name, chain, limit=len(rows), search_params=params)
                assert [hit["id"] for hit in result[0]] == [1100, 1099, 1098]
                assert [hit["distance"] for hit in result[0]] == pytest.approx([22000, 21980, 21960])
                assert [self._hit_field(hit, "ts") for hit in result[0]] == [11000, 10990, 10980]
                self.release_collection(client, name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("added_field", [False, True], ids=["nullable", "add_field"])
    def test_pyudf_nullable_and_added_field_inputs(self, tmp_path, minio_host, minio_bucket, added_field):
        """target: nullable/added fields; method: assert Arrow nulls in worker; expected: explicit policy and recovery."""
        module = """import pyarrow as pa
class UDF:
    def transform_query(self, params, columns):
        if not pa.types.is_int64(columns[0].type):
            raise AssertionError("nullable input type changed")
        values = columns[0].to_pylist()
        if params.get("reject_null") and any(v is None for v in values):
            raise ValueError("controlled null input rejection")
        return [pa.array([-1.0 if v is None else float(v) for v in values], type=pa.float32())]
def create_udf(context):
    return UDF()
"""
        rows = [
            {"id": 1, "ts": None, "vector": [0.0, 0.0]},
            {"id": 2, "vector": [0.01, 0.0]},
            {"id": 3, "ts": 30, "vector": [0.02, 0.0]},
        ]
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module, rows=rows, nullable=True) as (
            client,
            name,
            resource,
        ):
            field = "added_ts" if added_field else "ts"
            if added_field:
                self.add_collection_field(client, name, field, DataType.INT64, nullable=True)

            def chain(reject_null=False):
                return (
                    FunctionChain(FunctionChainStage.L2_RERANK)
                    .map(
                        "$score",
                        FunctionChainExpr(
                            "py_udf",
                            args=(col(field),),
                            params={"resource_name": resource, "udf_params": {"reject_null": reject_null}},
                        ),
                    )
                    .sort(col("$score"), desc=True, tie_break_col=col("$id"))
                )

            stored = self.query(client, name, filter="id >= 1", output_fields=["id", field], limit=3)[0]
            assert {r["id"]: r[field] for r in stored} == {1: None, 2: None, 3: None if added_field else 30}
            good = self._search_with_synced_resource(client, name, chain())
            expected_ids = [1, 2, 3] if added_field else [3, 1, 2]
            assert [h["id"] for h in good[0]] == expected_ids
            assert [h["distance"] for h in good[0]] == pytest.approx([-1, -1, -1] if added_field else [30, -1, -1])
            self._assert_pyudf_error(client, name, chain(True), "controlled null input rejection", code=2400)
            ordinary = self.search(
                client, name, data=[[0.0, 0.0]], anns_field="vector", limit=3, search_params={"metric_type": "L2"}
            )[0]
            assert [h["id"] for h in ordinary[0]] == [1, 2, 3]
            assert [h["distance"] for h in ordinary[0]] == pytest.approx([0, 0.0001, 0.0004])
            restored = self._search_with_synced_resource(client, name, chain())
            assert [h["id"] for h in restored[0]] == expected_ids
            assert [h["distance"] for h in restored[0]] == pytest.approx([-1, -1, -1] if added_field else [30, -1, -1])
            if added_field:
                self.insert(client, name, [{"id": 4, "ts": 40, "added_ts": 55, "vector": [0.03, 0.0]}])
                updated = self._search_with_synced_resource(client, name, chain(), limit=4)
                assert [h["id"] for h in updated[0]] == [4, 1, 2, 3]
                assert [h["distance"] for h in updated[0]] == pytest.approx([55, -1, -1, -1])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mode", ["filter", "range", "group", "chain_page"])
    def test_pyudf_query_interactions(self, tmp_path, minio_host, minio_bucket, mode):
        """target: candidate selection versus UDF ordering; method: exact three-row oracle; expected: aligned scores/fields."""
        rows = [
            {"id": 1, "ts": 10, "vector": [0.0, 0.0]},
            {"id": 2, "ts": 10 if mode == "group" else 20, "vector": [0.01, 0.0]},
            {"id": 3, "ts": 30, "vector": [0.02, 0.0]},
        ]
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None, rows=rows) as (
            client,
            name,
            resource,
        ):
            chain = self._pyudf_chain(resource)
            kwargs = {}
            expected = {"filter": [3, 2], "range": [2], "group": [3, 1], "chain_page": [2]}[mode]
            if mode == "filter":
                kwargs["filter"] = "ts >= 20"
            elif mode == "range":
                kwargs["search_params"] = {"metric_type": "L2", "params": {"radius": 0.00025, "range_filter": 0.00005}}
            elif mode == "group":
                kwargs["group_by_field"] = "ts"
            else:
                chain.limit(1, offset=1)
            result = self._search_with_synced_resource(client, name, chain, **kwargs)
            ledger = {row["id"]: row["ts"] for row in rows}
            assert [h["id"] for h in result[0]] == expected
            assert [h["distance"] for h in result[0]] == pytest.approx([2 * ledger[i] for i in expected])
            assert [self._hit_field(h, "ts") for h in result[0]] == [ledger[i] for i in expected]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [1, 2, 4, 8])
    def test_pyudf_ragged_query_batches(self, tmp_path, minio_host, minio_bucket, nq):
        """target: unequal query chunk sizes; method: independent range ledger; expected: no cross-query leakage."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (client, name, resource):
            # Range membership produces lengths [1, 3, 0, 2], including a
            # nonempty batch after an empty one. Avoid exact vector matches.
            queries = [[-0.01, 0.0], [0.009, 0.0], [10.0, 10.0], [0.019, 0.0]]
            queries = [queries[i % 4] for i in range(nq)]
            result = self._search_with_synced_resource(
                client,
                name,
                self._pyudf_chain(resource),
                data=queries,
                search_params={"metric_type": "L2", "params": {"radius": 0.00015, "range_filter": 0.0}},
            )
            assert len(result) == nq
            for query, hits in zip(queries, result):
                ids = [i for i in (1, 2, 3) if 0 < (query[0] - (i - 1) * 0.01) ** 2 + query[1] ** 2 < 0.00015]
                ids.sort(reverse=True)
                assert [h["id"] for h in hits] == ids
                assert [h["distance"] for h in hits] == pytest.approx([20 * i for i in ids])
                assert [self._hit_field(h, "ts") for h in hits] == [10 * i for i in ids]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mode", ["valid", "missing_merge", "late_merge", "duplicate_merge", "ranker_conflict"])
    def test_pyudf_hybrid_merge_contract(self, tmp_path, minio_host, minio_bucket, mode):
        """target: hybrid fusion then PyUDF; method: verify RRF control and invalid chains; expected: explicit contracts."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (client, name, resource):
            self._assert_pyudf_recovery(client, name, resource)
            reqs = [
                AnnSearchRequest(data=[q], anns_field="vector", param={"metric_type": "L2"}, limit=3)
                for q in ([0.0, 0.0], [0.02, 0.0])
            ]
            baseline_chain = (
                FunctionChain(FunctionChainStage.L2_RERANK)
                .merge("rrf", k=60)
                .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            )
            baseline = self.hybrid_search(
                client, name, reqs=reqs, ranker=None, limit=3, output_fields=["ts"], function_chains=baseline_chain
            )[0]
            assert [h["id"] for h in baseline[0]] == [1, 3, 2]
            assert [h["distance"] for h in baseline[0]] == pytest.approx([1 / 61 + 1 / 63, 1 / 61 + 1 / 63, 2 / 62])
            chain = FunctionChain(FunctionChainStage.L2_RERANK)
            if mode in ("valid", "duplicate_merge", "ranker_conflict"):
                chain.merge("rrf", k=60)
            chain.map(
                "$score",
                FunctionChainExpr(
                    "py_udf",
                    args=(col("$score"), col("ts")),
                    params={"resource_name": resource, "udf_params": {"factor": 2.0}},
                ),
            )
            if mode in ("late_merge", "duplicate_merge"):
                chain.merge("rrf", k=60)
            chain.sort(col("$score"), desc=True, tie_break_col=col("$id"))
            if mode == "valid":
                result = self.hybrid_search(
                    client, name, reqs=reqs, ranker=None, limit=3, output_fields=["ts"], function_chains=chain
                )[0]
                assert [h["id"] for h in result[0]] == [3, 2, 1]
                assert [h["distance"] for h in result[0]] == pytest.approx([60, 40, 20])
                assert [self._hit_field(h, "ts") for h in result[0]] == [30, 20, 10]
            else:
                message = "must be first" if mode == "late_merge" else "exactly one merge"
                code = 1100
                kwargs = {"ranker": None}
                if mode == "ranker_conflict":
                    message, code, kwargs = (
                        "function_chains and ranker cannot be used together",
                        1,
                        {"ranker": RRFRanker()},
                    )
                error = self.hybrid_search(
                    client,
                    name,
                    reqs=reqs,
                    limit=3,
                    function_chains=chain,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": code, "err_msg": message},
                    **kwargs,
                )[0]
                assert error.code == code, error
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mode", ["iterator", "order_by"])
    def test_pyudf_rejects_conflicting_query_modes(self, tmp_path, minio_host, minio_bucket, mode):
        """target: incompatible query modes; method: reject then retry supported Search; expected: 1100 and recovery."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (client, name, resource):
            self._assert_pyudf_recovery(client, name, resource)
            if mode == "order_by":
                self._assert_pyudf_error(
                    client,
                    name,
                    self._pyudf_chain(resource),
                    "order_by and function rerank cannot be used together",
                    order_by_fields=[{"field": "ts", "order": "asc"}],
                )
            else:
                error = self.search_iterator(
                    client,
                    name,
                    data=[[0.0, 0.0]],
                    anns_field="vector",
                    batch_size=2,
                    limit=3,
                    search_params={"metric_type": "L2"},
                    function_chains=self._pyudf_chain(resource),
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100, "err_msg": "function rerank is not supported with search iterator"},
                )[0]
                assert error.code == 1100, error
            self._assert_pyudf_recovery(client, name, resource)

    @staticmethod
    def _wait_for_pyudf_permission(operation, denied=False, timeout=30):
        deadline = time.monotonic() + timeout
        while True:
            try:
                value = operation()
                if not denied:
                    return value
            except grpc.RpcError as exc:
                assert exc.code() == grpc.StatusCode.PERMISSION_DENIED, exc
                assert "permission deny" in exc.details(), exc
                if denied:
                    return exc
            assert time.monotonic() < deadline, "privilege cache did not converge"
            time.sleep(0.2)

    @pytest.mark.xdist_group(name="rbac_serial")
    @pytest.mark.tags(CaseLabel.L3)
    def test_pyudf_search_privileges_follow_grant_and_revoke(self, tmp_path, minio_host, minio_bucket):
        """target: Search and resource privileges; method: grant/revoke on same user; expected: isolation and recovery."""
        # Preconditions: function.pyUDF.enabled=true, common.fileResource.mode.proxy=sync,
        # MinIO wheel storage, common.security.authorizationEnabled=true, and root credentials.
        # Run with --dist loadgroup to share the existing RBAC suite's worker.
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (root, name, resource):
            self._assert_pyudf_recovery(root, name, resource)
            user, role, password = (
                cf.gen_unique_str("pyudf_u"),
                cf.gen_unique_str("pyudf_r"),
                cf.gen_str_by_length(contain_numbers=True),
            )
            grants = set()
            with ExitStack() as cleanup:
                self.create_user(root, user_name=user, password=password)
                cleanup.callback(root.drop_user, user)
                self.create_role(root, role_name=role)
                cleanup.callback(root.drop_role, role)
                self.grant_role(root, user_name=user, role_name=role)
                cleanup.callback(root.revoke_role, user, role)

                def revoke(privilege):
                    if privilege in grants:
                        root.revoke_privilege_v2(role, privilege, name, "default")
                        grants.remove(privilege)

                def grant(privilege):
                    root.grant_privilege_v2(role, privilege, collection_name=name, db_name="default")
                    grants.add(privilege)
                    cleanup.callback(revoke, privilege)

                grant("DescribeCollection")
                uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
                actor = MilvusClient(uri=uri, token=f"{user}:{password}")
                cleanup.callback(actor.close)

                def search(with_udf=True):
                    return actor.search(
                        collection_name=name,
                        data=[[0.0, 0.0]],
                        anns_field="vector",
                        limit=3,
                        search_params={"metric_type": "L2"},
                        output_fields=["ts"],
                        **({"function_chains": self._pyudf_chain(resource)} if with_udf else {}),
                    )

                self._wait_for_pyudf_permission(search, denied=True)
                grant("Search")
                allowed = self._wait_for_pyudf_permission(search)
                assert [h["id"] for h in allowed[0]] == [3, 2, 1]
                assert [h["distance"] for h in allowed[0]] == pytest.approx([60, 40, 20])
                self._wait_for_pyudf_permission(actor.list_file_resources, denied=True)
                self._wait_for_pyudf_permission(lambda: actor.remove_file_resource(resource), denied=True)
                assert any(r.name == resource for r in root.list_file_resources())
                revoke("Search")
                self._wait_for_pyudf_permission(search, denied=True)
                self._wait_for_pyudf_permission(lambda: search(False), denied=True)
                self._assert_pyudf_recovery(root, name, resource)
                grant("Search")
                allowed = self._wait_for_pyudf_permission(search)
                assert [h["id"] for h in allowed[0]] == [3, 2, 1]
                assert [h["distance"] for h in allowed[0]] == pytest.approx([60, 40, 20])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("depth", [1, 16, 24, 32, 64, 65])
    def test_pyudf_nested_parameter_serialization_boundary(self, tmp_path, minio_host, minio_bucket, depth):
        """
        target: distinguish protobuf nesting limits from worker validation
        method: verify nested values inside UDF or assert the precise SDK serialization failure
        expected: supported depths preserve immutable values; oversized values fail before RPC; controls recover
        """
        module = """import pyarrow as pa
from types import MappingProxyType
class UDF:
    def transform_query(self, params, columns):
        if "payload" in params:
            value = params["payload"]
            for _ in range(params["depth"]):
                if not isinstance(value, MappingProxyType):
                    raise AssertionError("nested parameter is not immutable")
                value = value["next"]
            if value != 7:
                raise AssertionError("nested parameter leaf changed")
        return [pa.array([v.as_py() * 2.0 for v in columns[1]], type=pa.float32())]
def create_udf(context):
    return UDF()
"""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as (client, name, resource):
            self._assert_pyudf_recovery(client, name, resource)
            value = 7
            for _ in range(depth):
                value = {"next": value}
            chain = self._pyudf_chain(resource, {"payload": value, "depth": depth})
            if depth >= 32:
                # Current SDK/protobuf rejects these before the worker's depth-64 check.
                # Protobuf versions expose either a depth detail or the failed message type.
                message_type = "FunctionChainOp" if depth == 32 else "FunctionParamObject"
                with pytest.raises(
                    DecodeError,
                    match=rf"MaxDepth|Error parsing message with type 'milvus\.proto\.schema\.{message_type}'",
                ):
                    chain.to_proto()
            else:
                result = self._search_with_synced_resource(client, name, chain)
                assert [h["id"] for h in result[0]] == [3, 2, 1]
                assert [h["distance"] for h in result[0]] == pytest.approx([60, 40, 20])
                assert [self._hit_field(h, "ts") for h in result[0]] == [30, 20, 10]
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "input_name,message",
        [
            pytest.param("vector", "unsupported field type FloatVector", id="vector_root"),
            pytest.param("payload", "complete JSON root input is not supported", id="json_root"),
            pytest.param('payload["boost"]', "JSON path input requires an explicit data_type", id="json_without_hint"),
        ],
    )
    def test_pyudf_rejects_unsupported_input_columns(self, tmp_path, minio_host, minio_bucket, input_name, message):
        """
        target: reject unsupported input declarations before execution
        method: submit vector/JSON roots or an untyped JSON path beside valid scalar controls
        expected: exact 1100 source message; ordinary Search and valid PyUDF remain correct
        """
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (client, name, resource):
            if input_name.startswith("payload"):
                self.add_collection_field(client, name, "payload", DataType.JSON, nullable=True)
            self._assert_pyudf_recovery(client, name, resource)
            chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
                "$score",
                FunctionChainExpr("py_udf", args=(col(input_name),), params={"resource_name": resource}),
            )
            self._assert_pyudf_error(client, name, chain, message)
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    def test_pyudf_never_written_collection_validation(self, tmp_path, minio_host, minio_bucket):
        """
        target: separate a never-written collection from delete-all state
        method: search fresh empty data with valid/invalid chains, then insert a controlled row
        expected: empty valid result, static rejection 1100, and exact nonempty UDF result after insert
        """
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (client, control, resource):
            self._assert_pyudf_recovery(client, control, resource)
            with ExitStack() as cleanup:
                name = cf.gen_unique_str("pyudf_empty")
                schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
                schema.add_field("id", DataType.INT64, is_primary=True)
                schema.add_field("ts", DataType.INT64)
                schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
                indexes = self.prepare_index_params(client)[0]
                indexes.add_index("vector", index_type="FLAT", metric_type="L2")
                self.create_collection(client, name, schema=schema, index_params=indexes, consistency_level="Strong")
                cleanup.callback(self.drop_collection, client, name)
                self.load_collection(client, name)
                count = self.query(client, name, filter="", output_fields=["count(*)"])[0]
                assert count == [{"count(*)": 0}]
                empty = self._search_with_synced_resource(client, name, self._pyudf_chain(resource))
                assert len(empty) == 1 and list(empty[0]) == []
                invalid = FunctionChain(FunctionChainStage.L2_RERANK).map(
                    "$score", FunctionChainExpr("py_udf", args=(col("ts"),), params={})
                )
                self._assert_pyudf_error(client, name, invalid, "resource_name")
                self.insert(client, name, [{"id": 4, "ts": 40, "vector": [0.0, 0.0]}])
                recovered = self._search_with_synced_resource(client, name, self._pyudf_chain(resource))
                assert [h["id"] for h in recovered[0]] == [4]
                assert [h["distance"] for h in recovered[0]] == pytest.approx([80])
                assert [self._hit_field(h, "ts") for h in recovered[0]] == [40]
            self._assert_pyudf_recovery(client, control, resource)

    @contextmanager
    def _pyudf_security_actor(self, root):
        """Own each identity/grant; avoid tracing credential-bearing create calls."""
        user = cf.gen_unique_str("pyudf_sec_user")
        role = cf.gen_unique_str("pyudf_sec_role")
        password = cf.gen_str_by_length(contain_numbers=True)
        uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        grants = set()
        with ExitStack() as cleanup:
            root.create_user(user, password)
            cleanup.callback(root.drop_user, user)
            root.create_role(role)
            cleanup.callback(root.drop_role, role)
            root.grant_role(user, role)
            cleanup.callback(root.revoke_role, user, role)

            def revoke(privilege, collection="*", db="*"):
                key = (privilege, collection, db)
                if key in grants:
                    root.revoke_privilege_v2(role, privilege, collection, db)
                    grants.remove(key)

            def grant(privilege, collection="*", db="*"):
                root.grant_privilege_v2(role, privilege, collection_name=collection, db_name=db)
                grants.add((privilege, collection, db))
                cleanup.callback(revoke, privilege, collection, db)

            token = f"{user}:{password}"
            actor = MilvusClient(uri=uri, token=token)
            cleanup.callback(actor.close)
            yield actor, token, grant, revoke

    def _pyudf_security_wire_request(self, name, resource):
        from pymilvus.client.prepare import Prepare

        # Raw public RPC bypasses SDK connection authentication and retry behavior.
        return Prepare.search_requests_with_expr(
            name,
            "vector",
            {"metric_type": "L2"},
            3,
            data=[[0.0, 0.0]],
            output_fields=["ts"],
            function_chains=self._pyudf_chain(resource),
            use_default_consistency=False,
            consistency_level=0,
        )

    @staticmethod
    def _pyudf_security_metadata(token):
        import base64

        return [("authorization", base64.b64encode(token.encode()).decode())]

    @staticmethod
    def _assert_pyudf_security_hits(hits, scale=1):
        assert [h["id"] for h in hits] == [3, 2, 1]
        assert [h["distance"] for h in hits] == pytest.approx([60 * scale, 40 * scale, 20 * scale])
        assert [TestMilvusClientPyUDF._hit_field(h, "ts") for h in hits] == [30 * scale, 20 * scale, 10 * scale]

    @pytest.mark.xdist_group(name="rbac_serial")
    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("credential", ["missing", "malformed", "wrong_password"])
    def test_pyudf_security_rpc_authentication(self, tmp_path, minio_host, minio_bucket, credential):
        """target: four public RPC auth gates; method: raw requests; expected: denial and intact assets."""
        # Preconditions: function.pyUDF.enabled=true, common.fileResource.mode.proxy=sync,
        # MinIO wheel storage, common.security.authorizationEnabled=true, and root credentials.
        # Run with --dist loadgroup to share the existing RBAC suite's worker.
        from urllib.parse import urlparse

        from pymilvus.grpc_gen import milvus_pb2, milvus_pb2_grpc

        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (root, name, resource):
            self._assert_pyudf_recovery(root, name, resource)
            path = next(r.path for r in root.list_file_resources() if r.name == resource)
            candidate = cf.gen_unique_str("pyudf_sec_candidate")
            uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
            root_token = cf.param_info.param_token or f"{cf.param_info.param_user}:{cf.param_info.param_password}"
            metadata = (
                []
                if credential == "missing"
                else (
                    [("authorization", "%%%not-base64%%%")]
                    if credential == "malformed"
                    else self._pyudf_security_metadata("root:qa_synthetic_wrong_password")
                )
            )
            with grpc.insecure_channel(urlparse(uri).netloc) as channel:
                stub = milvus_pb2_grpc.MilvusServiceStub(channel)
                allowed = self._pyudf_security_metadata(root_token)
                response = stub.Search(self._pyudf_security_wire_request(name, resource), metadata=allowed, timeout=15)
                assert response.status.code == 0 and response.status.error_code == 0
                assert list(response.results.ids.int_id.data) == [3, 2, 1]
                assert list(response.results.scores) == pytest.approx([60, 40, 20])
                added = stub.AddFileResource(
                    milvus_pb2.AddFileResourceRequest(name=candidate, path=path), metadata=allowed, timeout=15
                )
                assert added.code == 0 and added.error_code == 0
                try:
                    listed = stub.ListFileResources(milvus_pb2.ListFileResourcesRequest(), metadata=allowed, timeout=15)
                    assert listed.status.code == 0 and any(r.name == candidate for r in listed.resources)
                finally:
                    removed = stub.RemoveFileResource(
                        milvus_pb2.RemoveFileResourceRequest(name=candidate), metadata=allowed, timeout=15
                    )
                    assert removed.code == 0 and removed.error_code == 0
                for method, request in [
                    (stub.Search, self._pyudf_security_wire_request(name, resource)),
                    (stub.AddFileResource, milvus_pb2.AddFileResourceRequest(name=candidate, path=path)),
                    (stub.RemoveFileResource, milvus_pb2.RemoveFileResourceRequest(name=resource)),
                    (stub.ListFileResources, milvus_pb2.ListFileResourcesRequest()),
                ]:
                    with pytest.raises(grpc.RpcError) as error:
                        method(request, metadata=metadata, timeout=15)
                    assert error.value.code() == grpc.StatusCode.UNAUTHENTICATED
                    resources = {r.name: r.path for r in root.list_file_resources()}
                    assert resources[resource] == path and candidate not in resources
                    self._assert_pyudf_recovery(root, name, resource)

    @pytest.mark.xdist_group(name="rbac_serial")
    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("surface", ["sdk", "rest"])
    def test_pyudf_security_collection_scope_and_revocation(self, tmp_path, minio_host, minio_bucket, surface):
        """target: collection-scoped Search; method: A/B and revoke; expected: exact A, denied B, intact peer."""
        # Preconditions: function.pyUDF.enabled=true, common.fileResource.mode.proxy=sync,
        # MinIO wheel storage, common.security.authorizationEnabled=true, and root credentials.
        # Run with --dist loadgroup to share the existing RBAC suite's worker.
        from urllib.parse import urlparse

        import requests
        from pymilvus.grpc_gen import milvus_pb2_grpc

        rows_b = [{"id": i, "ts": i * 100, "vector": [(i - 1) * 0.01, 0.0]} for i in (1, 2, 3)]
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (root, a, resource_a):
            with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None, rows=rows_b) as (
                owner,
                b,
                resource_b,
            ):
                self._assert_pyudf_recovery(root, a, resource_a)
                self._assert_pyudf_security_hits(
                    self._search_with_synced_resource(owner, b, self._pyudf_chain(resource_b))[0], scale=10
                )
                with self._pyudf_security_actor(root) as (actor, token, grant, revoke):
                    for collection in (a, b):
                        grant("DescribeCollection", collection, "default")
                    grant("Search", a, "default")
                    uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"

                    def sdk_search(collection, resource):
                        return self._search_with_synced_resource(actor, collection, self._pyudf_chain(resource))

                    def selected_search(collection, resource, denied=False):
                        if surface == "sdk":
                            value = self._wait_for_pyudf_permission(
                                lambda: sdk_search(collection, resource), denied=denied
                            )
                            if not denied:
                                self._assert_pyudf_security_hits(value[0])
                            return
                        # This cross-surface security case uses the same principal and policy.
                        payload = {
                            "collectionName": collection,
                            "data": [[0.0, 0.0]],
                            "annsField": "vector",
                            "limit": 3,
                            "outputFields": ["ts"],
                            "searchParams": {"metricType": "L2"},
                            "consistencyLevel": "Strong",
                            "functionChains": [
                                {
                                    "stage": "FunctionChainStageL2Rerank",
                                    "ops": [
                                        {
                                            "op": "map",
                                            "outputs": ["$score"],
                                            "expr": {
                                                "name": "py_udf",
                                                "args": [{"column": "$score"}, {"column": "ts"}],
                                                "params": {"resource_name": resource, "udf_params": {"factor": 2.0}},
                                            },
                                        },
                                        {
                                            "op": "sort",
                                            "inputs": ["$score"],
                                            "params": {"column": "$score", "desc": True},
                                        },
                                    ],
                                }
                            ],
                        }
                        response = requests.post(
                            uri + "/v2/vectordb/entities/search",
                            headers={"Authorization": "Bearer " + token},
                            json=payload,
                            timeout=15,
                        )
                        body = response.json()
                        if denied:
                            assert response.status_code == 403 and body["code"] == 65535, body
                            assert "permission deny" in body.get("message", "") and not body.get("data"), body
                        else:
                            assert body["code"] == 0, body
                            self._assert_pyudf_security_hits(body["data"])

                    self._wait_for_pyudf_permission(lambda: sdk_search(a, resource_a))
                    selected_search(a, resource_a)
                    for resource in (resource_a, resource_b):
                        selected_search(b, resource, denied=True)
                    self._wait_for_pyudf_permission(actor.list_file_resources, denied=True)
                    self._wait_for_pyudf_permission(lambda: actor.remove_file_resource(resource_a), denied=True)
                    path = next(r.path for r in root.list_file_resources() if r.name == resource_a)
                    candidate = cf.gen_unique_str("pyudf_sec_denied_add")
                    self._wait_for_pyudf_permission(lambda: actor.add_file_resource(candidate, path), denied=True)
                    assert candidate not in {r.name for r in root.list_file_resources()}
                    revoke("Search", a, "default")
                    self._wait_for_pyudf_permission(lambda: sdk_search(a, resource_a), denied=True)
                    selected_search(a, resource_a, denied=True)
                    # A new raw channel verifies revocation without SDK handler reuse.
                    with grpc.insecure_channel(urlparse(uri).netloc) as channel:
                        with pytest.raises(grpc.RpcError) as error:
                            milvus_pb2_grpc.MilvusServiceStub(channel).Search(
                                self._pyudf_security_wire_request(a, resource_a),
                                metadata=self._pyudf_security_metadata(token),
                                timeout=15,
                            )
                        assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
                    self._assert_pyudf_recovery(root, a, resource_a)
                    grant("Search", a, "default")
                    self._wait_for_pyudf_permission(lambda: sdk_search(a, resource_a))
                    selected_search(a, resource_a)
                    protected = self.query(owner, b, filter="id >= 0", output_fields=["id", "ts"])[0]
                    assert sorted(protected, key=lambda row: row["id"]) == [{"id": i, "ts": i * 100} for i in (1, 2, 3)]
                    self._assert_pyudf_security_hits(
                        self._search_with_synced_resource(owner, b, self._pyudf_chain(resource_b))[0], scale=10
                    )

    @pytest.mark.xdist_group(name="rbac_serial")
    @pytest.mark.tags(CaseLabel.L3)
    def test_pyudf_security_resource_manager_has_no_search_privilege(self, tmp_path, minio_host, minio_bucket):
        """target: management/execution separation; method: grant only resources; expected: Search denied."""
        # Preconditions: function.pyUDF.enabled=true, common.fileResource.mode.proxy=sync,
        # MinIO wheel storage, common.security.authorizationEnabled=true, and root credentials.
        # Run with --dist loadgroup to share the existing RBAC suite's worker.
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (root, name, resource):
            self._assert_pyudf_recovery(root, name, resource)
            path = next(r.path for r in root.list_file_resources() if r.name == resource)
            candidate = cf.gen_unique_str("pyudf_sec_managed")
            with self._pyudf_security_actor(root) as (actor, _, grant, _):
                grant("DescribeCollection", name, "default")
                grant("ListFileResources")
                grant("AddFileResource")
                grant("RemoveFileResource")
                listed = self._wait_for_pyudf_permission(actor.list_file_resources)
                assert any(r.name == resource for r in listed)
                try:
                    self._wait_for_pyudf_permission(lambda: actor.add_file_resource(candidate, path))
                    assert candidate in {r.name for r in root.list_file_resources()}
                    self._wait_for_pyudf_permission(
                        lambda: self._search_with_synced_resource(actor, name, self._pyudf_chain(resource)), denied=True
                    )
                    self._wait_for_pyudf_permission(lambda: actor.remove_file_resource(candidate))
                    assert candidate not in {r.name for r in root.list_file_resources()}
                finally:
                    self._remove_file_resource(root, candidate)
                self._assert_pyudf_recovery(root, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("resource_form", ["relative_path", "absolute_path", "uri", "expression"])
    def test_pyudf_security_resource_name_is_registry_key(self, tmp_path, minio_host, minio_bucket, resource_form):
        """target: resource lookup boundary; method: path/URI-like names; expected: 1100 without fallback."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (root, name, resource):
            self._assert_pyudf_recovery(root, name, resource)
            candidate = {
                "relative_path": "../" + resource,
                "absolute_path": "/tmp/" + resource + ".whl",
                "uri": "s3://qa-controlled/" + resource + ".whl",
                "expression": "__import__('" + resource + "')",
            }[resource_form]
            self._assert_pyudf_error(root, name, self._pyudf_chain(candidate), "not found")
            self._assert_pyudf_recovery(root, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("factory", ["create_udf()", "create_udf.attribute"])
    def test_pyudf_security_entrypoint_is_identifier(self, tmp_path, minio_host, minio_bucket, factory):
        """target: entrypoint parsing; method: inert non-identifiers; expected: loader rejection and peer recovery."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (root, control, valid):
            self._assert_pyudf_recovery(root, control, valid)
            package = "qa_inert_entrypoint"
            bad_wheel = self._build_wheel(
                tmp_path, package, entry_points=f"[milvus.pyudf]\nmain = {package}:{factory}\n"
            ).read_bytes()
            with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None, wheel_bytes=bad_wheel) as (
                actor,
                name,
                resource,
            ):
                self._assert_pyudf_error(actor, name, self._pyudf_chain(resource), "entry point", code=2400)
                self._assert_pyudf_recovery(root, control, valid)

    @pytest.mark.tags(CaseLabel.L1)
    def test_pyudf_security_extra_import_root_rejected(self, tmp_path, minio_host, minio_bucket):
        """target: wheel import-root declaration; method: inert extra root; expected: reject before import."""
        package = cf.gen_unique_str("pyudf_sec_root").lower()
        wheel = self._build_wheel(tmp_path, package)
        with zipfile.ZipFile(wheel, "a") as archive:
            archive.writestr("qa_inert_extra_root/__init__.py", "VALUE = 7\n")
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (root, control, valid):
            self._assert_pyudf_recovery(root, control, valid)
            with self._pyudf_test_environment(
                tmp_path, minio_host, minio_bucket, None, wheel_bytes=wheel.read_bytes()
            ) as (actor, name, resource):
                self._assert_pyudf_error(
                    actor, name, self._pyudf_chain(resource), "exactly one top-level Python import root", code=2400
                )
                self._assert_pyudf_recovery(root, control, valid)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("literal", ["__import__('os').getpid()", "../../qa_canary", "a'\";\\quoted"])
    def test_pyudf_security_parameters_remain_literal(self, tmp_path, minio_host, minio_bucket, literal):
        """target: parameter data/code boundary; method: exact inert strings; expected: unchanged and correct scores."""
        module = (
            "EXPECTED = "
            + repr(literal)
            + "\n"
            + """import pyarrow as pa
class UDF:
    def transform_query(self, params, columns):
        if params["literal"] != EXPECTED:
            raise AssertionError("literal changed")
        return [pa.array([v.as_py() * 2.0 for v in columns[1]], type=pa.float32())]
def create_udf(context):
    return UDF()
"""
        )
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as (root, name, resource):
            result = self._search_with_synced_resource(root, name, self._pyudf_chain(resource, {"literal": literal}))
            self._assert_pyudf_security_hits(result[0])

    @pytest.mark.xdist_group(name="rbac_serial")
    @pytest.mark.tags(CaseLabel.L3)
    def test_pyudf_security_exception_disclosure_characterization(
        self, tmp_path, minio_host, minio_bucket, record_property
    ):
        """target: error disclosure observation; method: synthetic canary; expected: classified error and peer recovery."""
        # Preconditions: function.pyUDF.enabled=true, common.fileResource.mode.proxy=sync,
        # MinIO wheel storage, common.security.authorizationEnabled=true, and root credentials.
        # Run with --dist loadgroup to share the existing RBAC suite's worker.
        canary = cf.gen_unique_str("qa_synthetic_sensitive")
        module = (
            "CANARY = "
            + repr(canary)
            + "\n"
            + """import pyarrow as pa
class UDF:
    def transform_query(self, params, columns):
        if params.get("raise_canary"):
            raise RuntimeError(CANARY)
        return [pa.array([v.as_py() * 2.0 for v in columns[1]], type=pa.float32())]
def create_udf(context):
    return UDF()
"""
        )
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as (root, name, resource):
            self._assert_pyudf_recovery(root, name, resource)
            with self._pyudf_security_actor(root) as (actor, _, grant, _):
                grant("DescribeCollection", name, "default")
                grant("Search", name, "default")
                # First prove this Search-only principal can execute the reviewed resource.
                allowed = self._wait_for_pyudf_permission(
                    lambda: self._search_with_synced_resource(actor, name, self._pyudf_chain(resource))
                )
                self._assert_pyudf_security_hits(allowed[0])
                # Record exposure separately: the runtime documents chained exception text.
                with pytest.raises(MilvusException) as error:
                    self._search_with_synced_resource(actor, name, self._pyudf_chain(resource, {"raise_canary": True}))
                assert error.value.code == 2400 and "UDF_FAILED" in str(error.value)
                record_property("security_canary_returned_to_caller", str(canary in str(error.value)))
                record_property("caller_permissions", "DescribeCollection and Search only")
                record_property("security_verdict", "EXPOSURE_OBSERVATION_NOT_SECURITY_PASS")
                self._assert_pyudf_recovery(root, name, resource)

    @pytest.mark.tags(CaseLabel.L3)
    def test_pyudf_security_worker_endpoint_not_peer_accessible(self, record_property):
        """target: supplied endpoint exposure; method: bounded peer TCP probes; expected: API reachable, worker closed."""
        # Preconditions: enabled PyUDF on 127.0.0.1:19090 and a runner outside its Pod;
        # point --uri at the actual Proxy/standalone Pod IP, with public port 19530 reachable.
        # A Service address alone does not establish the worker network boundary.
        from urllib.parse import urlparse

        from common.pyudf_helpers import probe_endpoint

        client = self._client()
        try:
            assert client.get_server_version(detail=True).get("git_commit")
        finally:
            client.close()
        uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        target = urlparse(uri)
        result = probe_endpoint(target.hostname, public_port=target.port or 19530)
        record_property("peer_target", target.hostname)
        record_property("public_tcp_code", result["observation"]["public_api"]["tcp_connect_code"])
        record_property("worker_tcp_code", result["observation"]["worker_rpc"]["tcp_connect_code"])
        record_property("scope", "supplied endpoint; bind it to a recorded Pod IP to certify the Pod boundary")

    @pytest.mark.xdist_group(name="rbac_serial")
    @pytest.mark.tags(CaseLabel.L3)
    def test_pyudf_security_database_scope_with_colliding_names(self, tmp_path, minio_host, minio_bucket):
        """target: database-scoped Search; method: identical names and separate grants; expected: exact scoped data."""
        # Preconditions: function.pyUDF.enabled=true, common.fileResource.mode.proxy=sync,
        # MinIO wheel storage, common.security.authorizationEnabled=true, and root credentials.
        # Run with --dist loadgroup to share the existing RBAC suite's worker.
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (root, name, resource):
            self._assert_pyudf_recovery(root, name, resource)
            db = cf.gen_unique_str("pyudf_sec_db")
            uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
            token = cf.param_info.param_token or f"{cf.param_info.param_user}:{cf.param_info.param_password}"
            with ExitStack() as cleanup:
                self.create_database(root, db_name=db)
                cleanup.callback(root.drop_database, db)
                owner = MilvusClient(uri=uri, token=token, db_name=db)
                cleanup.callback(owner.close)
                schema = self.create_schema(owner, auto_id=False, enable_dynamic_field=False)[0]
                schema.add_field("id", DataType.INT64, is_primary=True)
                schema.add_field("ts", DataType.INT64)
                schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
                indexes = self.prepare_index_params(owner)[0]
                indexes.add_index("vector", index_type="FLAT", metric_type="L2")
                self.create_collection(owner, name, schema=schema, index_params=indexes, consistency_level="Strong")
                cleanup.callback(self.drop_collection, owner, name)
                rows = [{"id": i, "ts": i * 100, "vector": [(i - 1) * 0.01, 0.0]} for i in (1, 2, 3)]
                self.insert(owner, name, rows)
                self.flush(owner, name)
                self.load_collection(owner, name)
                # FileResource is global; the same reviewed wheel may serve both databases.
                self._assert_pyudf_security_hits(
                    self._search_with_synced_resource(owner, name, self._pyudf_chain(resource))[0], scale=10
                )
                with self._pyudf_security_actor(root) as (actor, _, grant, revoke):
                    for database in ("default", db):
                        grant("DescribeCollection", name, database)
                        # SDK use_database first calls DescribeDatabase, independently of Search.
                        grant("DescribeDatabase", "*", database)
                    self._wait_for_pyudf_permission(lambda: actor.describe_database(db))
                    grant("Search", name, "default")

                    def search():
                        return self._search_with_synced_resource(actor, name, self._pyudf_chain(resource))

                    self._assert_pyudf_security_hits(self._wait_for_pyudf_permission(search)[0])
                    actor.use_database(db)
                    self._wait_for_pyudf_permission(search, denied=True)
                    grant("Search", name, db)
                    self._assert_pyudf_security_hits(self._wait_for_pyudf_permission(search)[0], scale=10)
                    revoke("Search", name, db)
                    self._wait_for_pyudf_permission(search, denied=True)
                    actor.use_database("default")
                    self._assert_pyudf_security_hits(self._wait_for_pyudf_permission(search)[0])
                self._assert_pyudf_recovery(root, name, resource)
                observed = self.query(owner, name, filter="id >= 0", output_fields=["id", "ts"])[0]
                assert sorted(observed, key=lambda row: row["id"]) == [{"id": i, "ts": i * 100} for i in (1, 2, 3)]

    @pytest.mark.tags(CaseLabel.L3)
    def test_pyudf_mixed_segment_states_preserve_results(
        self, tmp_path, minio_host, minio_bucket, record_property, request
    ):
        """target: three segment paths; method: prove topology and rerank each PK group; expected: exact ledger."""
        # Preconditions: enabled PyUDF with Proxy FileResource sync and MinIO;
        # --pyudf-management-endpoint must address this deployment's coordinator
        # (not its Proxy), with common.security.adminAuthEnabled=false.
        # Require 500 < indexCoord.segment.minSegmentNumRowsToEnableIndex <= 3000.
        import json

        threshold = int(self._pyudf_management_config(request, "indexCoord.segment.minSegmentNumRowsToEnableIndex"))
        assert 500 < threshold <= 3000, f"mixed segment fixture requires compatible build threshold, got {threshold}"
        record_property("min_index_rows", threshold)

        ledger = {i: i * 10 for i in range(1, 4001)}
        # The combined top three must include indexed sealed, small sealed,
        # and growing data, in that order.
        ledger.update({1500: 90000, 3250: 80000, 3750: 70000})
        rows = [{"id": i, "ts": ledger[i], "vector": [i / 10000.0, 0.0]} for i in ledger]
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (client, control, resource):
            self._assert_pyudf_recovery(client, control, resource)
            with ExitStack() as cleanup:
                name = cf.gen_unique_str("pyudf_segments")
                schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
                schema.add_field("id", DataType.INT64, is_primary=True)
                schema.add_field("ts", DataType.INT64)
                schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
                self.create_collection(client, name, schema=schema, consistency_level="Strong", num_shards=1)
                cleanup.callback(self.drop_collection, client, name)
                self.alter_collection_properties(
                    client, name, properties={"collection.autocompaction.enabled": "false"}
                )
                self.insert(client, name, rows[:3000])
                self.flush(client, name)
                self.insert(client, name, rows[3000:3500])
                self.flush(client, name)
                indexes = self.prepare_index_params(client)[0]
                indexes.add_index("vector", index_type="HNSW", metric_type="L2", params={"M": 8, "efConstruction": 64})
                self.create_index(client, name, index_params=indexes)
                self.load_collection(client, name)
                deadline = time.monotonic() + 120
                while True:
                    segments = client.list_loaded_segments(name)
                    if (
                        len(segments) == 2
                        and sorted(s.num_rows for s in segments) == [500, 3000]
                        and all(s.state == SegmentState.Sealed for s in segments)
                        and all(s.index_id > 0 for s in segments)
                    ):
                        break
                    assert time.monotonic() < deadline, segments
                    time.sleep(0.5)
                sealed_ids = {s.segment_id for s in segments}
                persistent_ids = {s.segment_id for s in client.list_persistent_segments(name)}
                topology = [{"id": s.segment_id, "rows": s.num_rows, "index_id": s.index_id} for s in segments]
                record_property("sealed_topology", json.dumps(topology))
                # index_id names the collection index even when a below-threshold
                # segment is fake-finished without building vector index files.
                record_property("small_segment_rows_below_build_threshold", 500 < threshold)
                self.insert(client, name, rows[3500:])
                growing = self.query(client, name, filter="id > 3500", output_fields=["id"], limit=500)[0]
                assert {r["id"] for r in growing} == set(range(3501, 4001))
                for expression, ids in [
                    ("id <= 3000", range(1, 3001)),
                    ("id > 3000 and id <= 3500", range(3001, 3501)),
                    ("id > 3500", range(3501, 4001)),
                    ("id > 0", range(1, 4001)),
                ]:
                    selected = list(ids)
                    result = self._search_with_synced_resource(
                        client,
                        name,
                        self._pyudf_chain(resource).limit(3),
                        limit=len(selected),
                        search_params={"metric_type": "L2", "params": {"ef": 4000}},
                        filter=expression,
                    )
                    expected = sorted(selected, key=lambda i: (-ledger[i], i))[:3]
                    assert [h["id"] for h in result[0]] == expected
                    assert [h["distance"] for h in result[0]] == pytest.approx([ledger[i] * 2 for i in expected])
                    assert [self._hit_field(h, "ts") for h in result[0]] == [ledger[i] for i in expected]
                complete = self._search_with_synced_resource(
                    client,
                    name,
                    self._pyudf_chain(resource),
                    limit=len(ledger),
                    search_params={"metric_type": "L2", "params": {"ef": 4000}},
                )[0]
                expected = sorted(ledger, key=lambda i: (-ledger[i], i))
                assert expected[:3] == [1500, 3250, 3750]
                assert [h["id"] for h in complete] == expected
                assert [h["distance"] for h in complete] == pytest.approx([ledger[i] * 2 for i in expected])
                assert [self._hit_field(h, "ts") for h in complete] == [ledger[i] for i in expected]
                assert {s.segment_id for s in client.list_loaded_segments(name)} == sealed_ids
                assert {s.segment_id for s in client.list_persistent_segments(name)} == persistent_ids
                record_property("growing_rows", "500 visible rows without an added persistent segment")
            self._assert_pyudf_recovery(client, control, resource)

    @pytest.mark.tags(CaseLabel.L2)
    def test_pyudf_compaction_preserves_mutated_ledger(self, tmp_path, minio_host, minio_bucket, record_property):
        """target: compaction and mutations; method: retain ledger across real plans/reload; expected: exact contents."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (client, name, resource):
            self.alter_collection_properties(client, name, properties={"collection.autocompaction.enabled": "false"})
            ledger = {i: i * 10 for i in (1, 2, 3)}
            for start in (1000, 2000, 3000):
                rows = [{"id": i, "ts": i * 10, "vector": [i / 10000.0, 0.0]} for i in range(start, start + 1200)]
                # Disjoint PKs are required for a deterministic snapshot oracle.
                rows = [r for r in rows if r["id"] not in ledger]
                self.insert(client, name, rows)
                ledger.update({r["id"]: r["ts"] for r in rows})
                self.flush(client, name)
            self.delete(client, name, filter="id in [1, 2] or (id >= 1000 and id < 1100)")
            ledger = {i: v for i, v in ledger.items() if i not in (1, 2) and not 1000 <= i < 1100}
            self.upsert(client, name, [{"id": 3, "ts": 99999, "vector": [0.02, 0.0]}])
            ledger[3] = 99999
            self.flush(client, name)

            def verify():
                stored = self.query(client, name, filter="id >= 0", output_fields=["id", "ts"], limit=5000)[0]
                assert len(stored) == len(ledger)
                assert {r["id"]: r["ts"] for r in stored} == ledger
                result = self._search_with_synced_resource(client, name, self._pyudf_chain(resource), limit=5000)
                expected = sorted(ledger, key=lambda i: (-ledger[i], i))
                assert [h["id"] for h in result[0]] == expected
                assert [h["distance"] for h in result[0]] == pytest.approx([ledger[i] * 2 for i in expected])
                assert [self._hit_field(h, "ts") for h in result[0]] == [ledger[i] for i in expected]
                # Target tombstones and the old upsert version through Search,
                # independently of the complete Query ledger above.
                stale = self._search_with_synced_resource(
                    client,
                    name,
                    self._pyudf_chain(resource),
                    limit=5000,
                    filter="id in [1, 2] or (id >= 1000 and id < 1100) or (id == 3 and ts == 30)",
                )
                assert len(stale) == 1 and list(stale[0]) == []

            verify()
            job = self.compact(client, name)[0]
            deadline = time.monotonic() + 180
            while True:
                state = self.get_compaction_state(client, job)[0]
                if state == "Completed":
                    break
                assert state != "Failed" and time.monotonic() < deadline, state
                time.sleep(0.5)
            plans = client.get_compaction_plans(job)
            assert plans.plans, "a completed no-op is not compaction coverage"
            record_property("compaction_plans", str(plans))
            self.release_collection(client, name)
            self.load_collection(client, name)
            verify()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mode", ["text_match", "bm25", "hybrid_rrf", "hybrid_weighted"])
    @pytest.mark.parametrize("nq", [1, 2])
    def test_pyudf_text_and_sparse_queries(self, tmp_path, minio_host, minio_bucket, mode, nq):
        """target: lexical/dense fusion reaches UDF; method: independent branch oracle; expected: exact scores/IDs."""
        module = """import pyarrow as pa
class UDF:
    def transform_query(self, params, columns):
        factor = float(params.get("factor", 1.0))
        score_factor = float(params.get("score_factor", 0.0))
        values = [float(score.as_py()) * score_factor + float(ts.as_py()) * factor
                  for score, ts in zip(columns[0], columns[1])]
        return [pa.array(values, type=pa.float32())]
def create_udf(context):
    return UDF()
"""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as (client, control, resource):
            self._assert_pyudf_recovery(client, control, resource)
            with ExitStack() as cleanup:
                name = cf.gen_unique_str("pyudf_text")
                schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
                schema.add_field("id", DataType.INT64, is_primary=True)
                schema.add_field("ts", DataType.INT64)
                schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
                schema.add_field(
                    "text",
                    DataType.VARCHAR,
                    max_length=256,
                    enable_analyzer=True,
                    enable_match=True,
                    analyzer_params={"tokenizer": "standard"},
                )
                schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
                schema.add_function(
                    Function(
                        name="text_bm25",
                        function_type=FunctionType.BM25,
                        input_field_names=["text"],
                        output_field_names=["sparse"],
                    )
                )
                indexes = self.prepare_index_params(client)[0]
                indexes.add_index("vector", index_type="FLAT", metric_type="L2")
                indexes.add_index("sparse", index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
                self.create_collection(client, name, schema=schema, index_params=indexes, consistency_level="Strong")
                cleanup.callback(self.drop_collection, client, name)
                rows = [
                    {"id": 1, "ts": 10, "vector": [0.0, 0.0], "text": "milvus vector database"},
                    {"id": 2, "ts": 20, "vector": [0.01, 0.0], "text": "milvus search"},
                    {"id": 3, "ts": 30, "vector": [0.02, 0.0], "text": "postgres relational"},
                    {"id": 4, "ts": 40, "vector": [1.0, 0.0], "text": "milvus relational"},
                ]
                self.insert(client, name, rows)
                self.flush(client, name)
                self.load_collection(client, name)
                dense = [[0.0, 0.0], [0.02, 0.0]][:nq]
                texts = ["milvus", "relational"][:nq]
                chain = self._pyudf_chain(resource)
                if mode.startswith("hybrid"):
                    # The far-away row 4 must enter through the sparse branch for every query.
                    dense_hits = self.search(
                        client,
                        name,
                        data=dense,
                        anns_field="vector",
                        search_params={"metric_type": "L2"},
                        limit=2,
                    )[0]
                    sparse_hits = self.search(
                        client,
                        name,
                        data=texts,
                        anns_field="sparse",
                        search_params={"metric_type": "BM25"},
                        limit=3,
                    )[0]
                    assert len(dense_hits) == len(sparse_hits) == nq
                    expected_scores = []
                    for query, (dense_branch, sparse_branch) in enumerate(zip(dense_hits, sparse_hits)):
                        assert [hit["id"] for hit in dense_branch] == ([1, 2] if query == 0 else [3, 2])
                        assert {hit["id"] for hit in sparse_branch} == ({1, 2, 4} if query == 0 else {3, 4})
                        fused = {}
                        for branch, weight, is_dense in [(dense_branch, 0.3, True), (sparse_branch, 0.7, False)]:
                            for rank, hit in enumerate(branch, start=1):
                                if mode == "hybrid_rrf":
                                    contribution = 1.0 / (60 + rank)
                                else:
                                    normalized = 2 * math.atan(hit["distance"]) / math.pi
                                    contribution = weight * (1 - normalized if is_dense else normalized)
                                fused[hit["id"]] = fused.get(hit["id"], 0.0) + contribution
                        expected_scores.append({pk: 100 * score + pk * 20 for pk, score in fused.items()})
                    reqs = [
                        AnnSearchRequest(data=dense, anns_field="vector", param={"metric_type": "L2"}, limit=2),
                        AnnSearchRequest(data=texts, anns_field="sparse", param={"metric_type": "BM25"}, limit=3),
                    ]
                    merged = FunctionChain(FunctionChainStage.L2_RERANK)
                    merged.merge(
                        "weighted", weights=[0.3, 0.7], norm_score=True
                    ) if mode == "hybrid_weighted" else merged.merge("rrf", k=60)
                    merged.map(
                        "$score",
                        FunctionChainExpr(
                            "py_udf",
                            args=(col("$score"), col("ts")),
                            params={"resource_name": resource, "udf_params": {"factor": 2.0, "score_factor": 100.0}},
                        ),
                    )
                    merged.sort(col("$score"), desc=True, tie_break_col=col("$id"))
                    result = self.hybrid_search(
                        client, name, reqs=reqs, ranker=None, limit=3, output_fields=["ts"], function_chains=merged
                    )[0]
                else:
                    kwargs = {"filter": 'text_match(text, "milvus")'} if mode == "text_match" else {}
                    field, data, metric = ("vector", dense, "L2") if mode == "text_match" else ("sparse", texts, "BM25")
                    result = self.search(
                        client,
                        name,
                        data=data,
                        anns_field=field,
                        search_params={"metric_type": metric},
                        limit=3,
                        output_fields=["ts"],
                        function_chains=chain,
                        **kwargs,
                    )[0]
                assert len(result) == nq
                for index, hits in enumerate(result):
                    if mode.startswith("hybrid"):
                        scores = expected_scores[index]
                        expected = sorted(scores, key=lambda pk: (-scores[pk], pk))
                    else:
                        expected = [4, 3] if mode == "bm25" and index == 1 else [4, 2, 1]
                        scores = {pk: pk * 20 for pk in expected}
                    assert [h["id"] for h in hits] == expected
                    assert [h["distance"] for h in hits] == pytest.approx(
                        [scores[pk] for pk in expected], rel=1e-5, abs=1e-5
                    )
                    assert [self._hit_field(h, "ts") for h in hits] == [i * 10 for i in expected]
                if mode.startswith("hybrid"):
                    # Each union has three candidates. Requesting two makes
                    # ignored pagination observable without changing the fusion oracle.
                    for request_limit, request_offset, chain_page in [(2, 0, False), (2, 1, False), (1, 2, True)]:
                        page_chain = merged.limit(2, offset=0) if chain_page else merged
                        paged = self.hybrid_search(
                            client,
                            name,
                            reqs=reqs,
                            ranker=None,
                            limit=request_limit,
                            offset=request_offset,
                            output_fields=["ts"],
                            function_chains=page_chain,
                        )[0]
                        assert len(paged) == nq
                        for scores, hits in zip(expected_scores, paged):
                            ordered = sorted(scores, key=lambda pk: (-scores[pk], pk))
                            expected = (
                                ordered[:2] if chain_page else ordered[request_offset : request_offset + request_limit]
                            )
                            assert [h["id"] for h in hits] == expected
                            assert [h["distance"] for h in hits] == pytest.approx(
                                [scores[pk] for pk in expected], rel=1e-5, abs=1e-5
                            )
                            assert [self._hit_field(h, "ts") for h in hits] == [pk * 10 for pk in expected]
            self._assert_pyudf_recovery(client, control, resource)

    @pytest.mark.tags(CaseLabel.L1)
    def test_pyudf_new_package_replaces_removed_resource_name(self, tmp_path, minio_host, minio_bucket):
        """target: resource rebinding; method: remove then bind a new package/path; expected: no stale score."""
        module = """import pyarrow as pa
class UDF:
    def transform_query(self, params, columns):
        return [pa.array([v.as_py() * 2.0 + 100 for v in columns[1]], type=pa.float32())]
def create_udf(context):
    return UDF()
"""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None) as (client, name, resource):
            self._assert_pyudf_recovery(client, name, resource)
            with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as (
                peer,
                other,
                new_resource,
            ):
                replacement_path = next(r.path for r in peer.list_file_resources() if r.name == new_resource)
                self._remove_file_resource(client, resource)
                self._wait_for_removed_pyudf(client, name, self._pyudf_chain(resource), resource)
                self.add_file_resource(client, resource, replacement_path)
                # A distinct package is intentional: hot replacement of an imported package is unsupported.
                try:
                    result = self._search_with_synced_resource(client, name, self._pyudf_chain(resource))
                    assert [h["id"] for h in result[0]] == [3, 2, 1]
                    assert [h["distance"] for h in result[0]] == pytest.approx([160, 140, 120])
                    assert [self._hit_field(h, "ts") for h in result[0]] == [30, 20, 10]
                finally:
                    self._remove_file_resource(client, resource)

    @pytest.mark.tags(CaseLabel.L3)
    def test_pyudf_package_collision_preserves_original(self, tmp_path, minio_host, minio_bucket, request):
        """target: package ownership; method: same package in two wheel paths; expected: rejection and intact control."""
        # Preconditions: one standalone instance with PyUDF enabled, Proxy resource
        # sync, MinIO, and function.pyUDF.server.workerCount=1 applied at startup.
        # --pyudf-management-endpoint must address that same standalone instance,
        # with common.security.adminAuthEnabled=false. Do not route through multiple replicas.
        probe = self._client()
        try:
            assert probe.get_server_version(detail=True)["deploy_mode"] == "STANDALONE"
        finally:
            probe.close()
        workers = int(self._pyudf_management_config(request, "function.pyUDF.server.workerCount"))
        assert workers == 1, f"package ownership is process-local; requires one worker, got {workers}"
        package = cf.gen_unique_str("pyudf_shared_package").lower()
        wheel = self._build_wheel(tmp_path, package).read_bytes()
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None, wheel_bytes=wheel) as first:
            client, name, resource = first
            self._assert_pyudf_recovery(client, name, resource)
            with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None, wheel_bytes=wheel) as second:
                peer, other, colliding = second
                self._assert_pyudf_error(peer, other, self._pyudf_chain(colliding), "already claimed", code=2400)
                self._assert_pyudf_recovery(client, name, resource)
                ordinary = self.search(
                    peer, other, data=[[0.0, 0.0]], anns_field="vector", limit=3, search_params={"metric_type": "L2"}
                )[0]
                assert [h["id"] for h in ordinary[0]] == [1, 2, 3]

    @pytest.mark.tags(CaseLabel.L2)
    def test_pyudf_concurrent_first_load_creates_one_instance(self, tmp_path, minio_host, minio_bucket):
        """target: concurrent initialization; method: cold fan-in and factory guard; expected: exact results per call."""
        module = """import time
import pyarrow as pa
FACTORY_CALLS = 0
class UDF:
    def transform_query(self, params, columns):
        if FACTORY_CALLS != 1:
            raise AssertionError("factory ran more than once in this worker")
        factor = float(params.get("factor", 2.0))
        return [pa.array([v.as_py() * factor for v in columns[1]], type=pa.float32())]
def create_udf(context):
    global FACTORY_CALLS
    FACTORY_CALLS += 1
    time.sleep(0.2)
    return UDF()
"""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as (client, name, resource):
            # Resource registration does not execute the factory; fan-in starts before any UDF warmup.
            with ThreadPoolExecutor(max_workers=6) as pool:
                futures = [
                    (
                        factor,
                        pool.submit(
                            self._search_with_synced_resource,
                            client,
                            name,
                            self._pyudf_chain(resource, {"factor": factor}),
                        ),
                    )
                    for factor in range(1, 13)
                ]
                for factor, future in futures:
                    result = future.result(timeout=40)
                    assert [h["id"] for h in result[0]] == [3, 2, 1]
                    assert [h["distance"] for h in result[0]] == pytest.approx([30 * factor, 20 * factor, 10 * factor])
                    assert [self._hit_field(h, "ts") for h in result[0]] == [30, 20, 10]
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    def test_pyudf_explicit_memory_error_is_classified_and_recovers(self, tmp_path, minio_host, minio_bucket):
        """target: MemoryError mapping; method: explicit exception without allocation pressure; expected: code 3/recovery."""
        module = """import pyarrow as pa
class UDF:
    def transform_query(self, params, columns):
        if params.get("fail"):
            raise MemoryError("qa controlled MemoryError without resource exhaustion")
        return [pa.array([v.as_py() * 2.0 for v in columns[1]], type=pa.float32())]
def create_udf(context):
    return UDF()
"""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as (client, name, resource):
            self._assert_pyudf_recovery(client, name, resource)
            self._assert_pyudf_error(client, name, self._pyudf_chain(resource, {"fail": True}), "OUT_OF_MEMORY", code=3)
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "offset,chain_page",
        [(0, False), (1, False), (5, False), (6, False), (1, True), (-1, False)],
        ids=["first", "middle", "short", "empty", "chain-page", "negative"],
    )
    def test_pyudf_search_offset_selects_candidates_before_rerank(
        self, tmp_path, minio_host, minio_bucket, offset, chain_page
    ):
        """target: Search offset; method: distinct ANN/UDF orders; expected: candidate page then chain page."""
        values = [10, 60, 20, 50, 30, 40]
        rows = [{"id": i + 1, "ts": ts, "vector": [i * 0.01, 0.0]} for i, ts in enumerate(values)]
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None, rows=rows) as (
            client,
            name,
            resource,
        ):

            def control():
                hits = self._search_with_synced_resource(client, name, self._pyudf_chain(resource), limit=6)[0]
                assert [hit["id"] for hit in hits] == [2, 4, 6, 5, 3, 1]
                assert [hit["distance"] for hit in hits] == pytest.approx([120, 100, 80, 60, 40, 20])

            control()
            chain = self._pyudf_chain(resource)
            if chain_page:
                chain.limit(2, offset=1)
            if offset < 0:
                with pytest.raises((MilvusException, ParamError)) as caught:
                    self._search_with_synced_resource(client, name, chain, offset=offset, limit=3)
                assert "offset" in str(caught.value).lower()
                control()
                return
            candidates = rows[offset : offset + 3]
            ordinary = self.search(
                client,
                name,
                data=[[0.0, 0.0]],
                anns_field="vector",
                limit=3,
                search_params={"metric_type": "L2"},
                offset=offset,
                output_fields=["ts"],
            )[0]
            assert [hit["id"] for hit in ordinary[0]] == [row["id"] for row in candidates]
            expected = sorted(candidates, key=lambda row: (-row["ts"], row["id"]))
            if chain_page:
                expected = expected[1:3]
            result = self._search_with_synced_resource(client, name, chain, offset=offset, limit=3)
            assert [hit["id"] for hit in result[0]] == [row["id"] for row in expected]
            assert [hit["distance"] for hit in result[0]] == pytest.approx([row["ts"] * 2 for row in expected])
            assert [self._hit_field(hit, "ts") for hit in result[0]] == [row["ts"] for row in expected]
