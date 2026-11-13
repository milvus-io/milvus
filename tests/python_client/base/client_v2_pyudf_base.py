"""Owned wheel, collection and permission lifecycles for PyUDF SDK cases."""

import io
import time
from contextlib import ExitStack, contextmanager

import grpc
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CheckTasks
from common.pyudf_helpers import build_wheel
from pymilvus import DataType, FunctionChain, FunctionChainStage, MilvusClient
from pymilvus.function_chain import col
from pymilvus.function_chain.chain import FunctionChainExpr


class PyUDFClientBase(TestMilvusClientV2Base):
    """Share fixture ownership without importing or inheriting collected tests."""

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
        wheel_path.write_bytes(build_wheel(package_name, module, entry_points=entry_points))
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
        metric_type="L2",
    ):
        from minio import Minio

        client = self._client()
        package_name = cf.gen_unique_str("pyudf_contract_e2e").lower()
        resource_name = cf.gen_unique_str("pyudf_contract_resource")
        collection_name = cf.gen_unique_str("pyudf")
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
                metric_type=metric_type,
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
