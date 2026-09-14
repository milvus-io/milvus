import io
import time
import zipfile
from contextlib import contextmanager

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import DataType, FunctionChain, FunctionChainStage, MilvusException
from pymilvus.function_chain import col, fn
from pymilvus.function_chain.chain import FunctionChainExpr

prefix = "pyudf"


class TestMilvusClientPyUDF(TestMilvusClientV2Base):
    """End-to-end tests for PyUDF-backed FunctionChain reranking."""

    dim = 2
    vector_field = "vector"
    scalar_field = "ts"

    @staticmethod
    def _build_wheel(tmp_path, package_name, module=None):
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
                f"[milvus.pyudf]\nmain = {package_name}:create_udf\n",
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

    def _search_with_synced_resource(self, client, collection_name, chain, timeout=30, data=None):
        deadline = time.time() + timeout
        queries = [[0.0, 0.0]] if data is None else data
        while True:
            try:
                return client.search(
                    collection_name=collection_name,
                    data=queries,
                    anns_field=self.vector_field,
                    search_params={"metric_type": "L2"},
                    limit=3,
                    output_fields=[self.scalar_field],
                    function_chains=chain,
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
    def _pyudf_test_environment(self, tmp_path, minio_host, minio_bucket, module):
        from minio import Minio

        client = self._client()
        package_name = cf.gen_unique_str("pyudf_contract_e2e").lower()
        resource_name = cf.gen_unique_str("pyudf_contract_resource")
        collection_name = cf.gen_unique_str(prefix)
        remote_path = f"pyudf/{resource_name}.whl"
        wheel_path = self._build_wheel(tmp_path, package_name, module=module)
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
            assert "RuntimeError" in str(captured.value)

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

    @pytest.mark.tags(CaseLabel.L0)
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
                ("unsupported_type", "unsupported type binary", None),
                ("two_outputs", "function returned 2 outputs, expected 1", None),
                ("null_score", "contains 3 null value(s)", None),
                ("count_changes", "output count changed", two_queries),
                ("type_changes", "type changed", two_queries),
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

    @pytest.mark.tags(CaseLabel.L0)
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
            assert all(self._hit_field(hit, intermediate_column) is None for hit in result[0])
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
            assert all(self._hit_field(hit, intermediate_column) is None for hit in result[0])
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

    @pytest.mark.tags(CaseLabel.L0)
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
