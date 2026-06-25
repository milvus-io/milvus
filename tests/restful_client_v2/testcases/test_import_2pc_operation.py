import base64
import csv
import json
import os
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from api.milvus import CollectionClient, ImportJobClient, StorageClient, VectorClient
from base.testbase import TestBase
from pymilvus import MilvusClient
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

IMPORT_2PC_TIMEOUT = 360


@pytest.mark.tags(CaseLabel.L0)
class TestImport2PCRestOperation(TestBase):
    def _kubectl_get_release_pods(self, release_name, namespace="chaos-testing"):
        if not shutil.which("kubectl"):
            pytest.skip("kubectl is required for import 2PC environment precondition checks")

        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        cmd = [
            "kubectl",
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            f"app.kubernetes.io/instance={release_name},app.kubernetes.io/name=milvus",
            "-o",
            "json",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=15, check=False)
        if result.returncode != 0:
            pytest.skip(f"cannot inspect release {release_name} pods in namespace {namespace}: {result.stderr.strip()}")
        return json.loads(result.stdout).get("items", [])

    def _skip_if_release_has_no_component(self, release_name, component, case_id):
        pods = self._kubectl_get_release_pods(release_name)
        components = sorted(
            {pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component", "") for pod in pods}
        )
        if component not in components:
            pytest.skip(
                f"{case_id} requires a release with separate {component} pods; "
                f"{release_name} exposes components={components or ['<none>']}"
            )

    def _kubectl_delete_one_component_pod_and_wait_ready(
        self,
        release_name,
        component,
        namespace="chaos-testing",
        timeout=300,
    ):
        pods = self._kubectl_get_release_pods(release_name, namespace=namespace)
        component_pods = [
            pod
            for pod in pods
            if pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component") == component
            and pod.get("status", {}).get("phase") == "Running"
        ]
        if not component_pods:
            pytest.skip(f"release {release_name} has no running {component} pod")

        pod_name = component_pods[0]["metadata"]["name"]
        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        delete_cmd = [
            "kubectl",
            "delete",
            "pod",
            pod_name,
            "-n",
            namespace,
            "--wait=false",
        ]
        delete_result = subprocess.run(
            delete_cmd,
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
            check=False,
        )
        assert delete_result.returncode == 0, {
            "cmd": delete_cmd,
            "stdout": delete_result.stdout,
            "stderr": delete_result.stderr,
        }

        deadline = time.time() + timeout
        last_pods = []
        while time.time() < deadline:
            last_pods = self._kubectl_get_release_pods(release_name, namespace=namespace)
            for pod in last_pods:
                labels = pod.get("metadata", {}).get("labels", {})
                if labels.get("app.kubernetes.io/component") != component:
                    continue
                if pod.get("metadata", {}).get("name") == pod_name:
                    continue
                conditions = pod.get("status", {}).get("conditions", [])
                ready = any(
                    condition.get("type") == "Ready" and condition.get("status") == "True" for condition in conditions
                )
                if pod.get("status", {}).get("phase") == "Running" and ready:
                    return pod_name, pod.get("metadata", {}).get("name")
            time.sleep(3)

        observed = [
            {
                "name": pod.get("metadata", {}).get("name"),
                "component": pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component"),
                "phase": pod.get("status", {}).get("phase"),
                "conditions": pod.get("status", {}).get("conditions", []),
            }
            for pod in last_pods
        ]
        raise TimeoutError(f"{component} pod for {release_name} did not become ready: {observed}")

    def _wait_milvus_cr_healthy(self, release_name, namespace="chaos-testing", timeout=300):
        if not shutil.which("kubectl"):
            pytest.skip("kubectl is required for import 2PC fault-tolerance tests")

        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        deadline = time.time() + timeout
        last_result = None
        while time.time() < deadline:
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "milvus",
                    "-n",
                    namespace,
                    release_name,
                    "-o",
                    "json",
                ],
                capture_output=True,
                text=True,
                env=env,
                timeout=15,
                check=False,
            )
            last_result = result
            if result.returncode == 0:
                status = json.loads(result.stdout).get("status", {})
                if status.get("status") == "Healthy":
                    return status, True
            time.sleep(3)
        return {
            "returncode": None if last_result is None else last_result.returncode,
            "stdout": "" if last_result is None else last_result.stdout,
            "stderr": "" if last_result is None else last_result.stderr,
        }, False

    def _skip_cdc_case_on_single_cluster_instance(self, case_id):
        pytest.skip(
            f"{case_id} requires a primary/secondary CDC deployment. "
            "The current endpoint is a single standalone Milvus instance."
        )

    def _require_cdc_rest_env(
        self,
        case_id,
        secondary_endpoint,
        secondary_minio_host,
        secondary_bucket_name,
        source_cluster_id,
        target_cluster_id,
    ):
        required = {
            "secondary_endpoint": secondary_endpoint,
            "secondary_minio_host": secondary_minio_host,
            "secondary_bucket_name": secondary_bucket_name,
            "source_cluster_id": source_cluster_id,
            "target_cluster_id": target_cluster_id,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            pytest.skip(f"{case_id} requires CDC REST options: {', '.join(missing)}")

    def _apply_cdc_topology(
        self,
        secondary_endpoint,
        secondary_token,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        config = {
            "clusters": [
                {
                    "cluster_id": source_cluster_id,
                    "connection_param": {
                        "uri": self.endpoint,
                        "token": self.api_key,
                    },
                    "pchannels": [f"{source_cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)],
                },
                {
                    "cluster_id": target_cluster_id,
                    "connection_param": {
                        "uri": secondary_endpoint,
                        "token": secondary_token,
                    },
                    "pchannels": [f"{target_cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)],
                },
            ],
            "cross_cluster_topology": [
                {
                    "source_cluster_id": source_cluster_id,
                    "target_cluster_id": target_cluster_id,
                }
            ],
        }

        primary_client = MilvusClient(uri=self.endpoint, token=self.api_key)
        secondary_client = MilvusClient(uri=secondary_endpoint, token=secondary_token)
        try:
            if not hasattr(primary_client, "update_replicate_configuration") or not hasattr(
                secondary_client, "update_replicate_configuration"
            ):
                return
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(primary_client.update_replicate_configuration, timeout=600, **config),
                    executor.submit(secondary_client.update_replicate_configuration, timeout=600, **config),
                ]
                wait(futures)
            for future in futures:
                future.result()
        finally:
            primary_client.close()
            secondary_client.close()
        time.sleep(5)

    def _build_secondary_cdc_clients(
        self,
        secondary_endpoint,
        secondary_token,
        secondary_minio_host,
        secondary_bucket_name,
        secondary_root_path,
    ):
        return {
            "collection": CollectionClient(secondary_endpoint, secondary_token),
            "vector": VectorClient(secondary_endpoint, secondary_token),
            "import_job": ImportJobClient(secondary_endpoint, secondary_token),
            "storage": StorageClient(
                f"{secondary_minio_host}:9000",
                "minioadmin",
                "minioadmin",
                secondary_bucket_name,
                secondary_root_path,
            ),
        }

    def _wait_collection_exists_with_client(self, collection_client, collection_name, timeout=180):
        t0 = time.time()
        last_rsp = None
        while time.time() - t0 < timeout:
            last_rsp = collection_client.collection_list()
            if last_rsp.get("code") == 0 and collection_name in last_rsp.get("data", []):
                return last_rsp, True
            time.sleep(2)
        return last_rsp, False

    def _wait_collection_loaded_with_client(self, collection_client, collection_name, timeout=180):
        t0 = time.time()
        last_rsp = None
        while time.time() - t0 < timeout:
            last_rsp = collection_client.collection_describe(collection_name)
            if last_rsp.get("code") == 0 and last_rsp.get("data", {}).get("load") == "LoadStateLoaded":
                return last_rsp, True
            time.sleep(2)
        return last_rsp, False

    def _query_imported_ids_with_client(self, vector_client, collection_name, ids):
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {ids}",
            "outputFields": ["id"],
            "limit": len(ids),
        }
        rsp = vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _query_count_with_clients(self, vector_client, collection_name, filter_expr=" "):
        payload = {
            "collectionName": collection_name,
            "filter": filter_expr,
            "limit": 0,
            "outputFields": ["count(*)"],
        }
        rsp = vector_client.vector_query(payload, timeout=30)
        assert rsp["code"] == 0, rsp
        assert len(rsp.get("data", [])) == 1, rsp
        return rsp["data"][0]["count(*)"]

    def _wait_count_with_clients(
        self,
        collection_client,
        vector_client,
        collection_name,
        expected_count,
        timeout=180,
    ):
        t0 = time.time()
        last_count = None
        while time.time() - t0 < timeout:
            collection_client.refresh_load(collection_name)
            last_count = self._query_count_with_clients(vector_client, collection_name)
            if last_count == expected_count:
                return last_count, True
            time.sleep(2)
        return last_count, False

    def _wait_imported_ids_visible_with_clients(
        self,
        collection_client,
        vector_client,
        collection_name,
        expected_ids,
        timeout=180,
    ):
        t0 = time.time()
        expected = set(expected_ids)
        last_seen = set()
        while time.time() - t0 < timeout:
            collection_client.refresh_load(collection_name)
            last_seen = self._query_imported_ids_with_client(vector_client, collection_name, sorted(expected))
            if last_seen == expected:
                return last_seen, True
            time.sleep(2)
        return last_seen, False

    def _wait_imported_ids_absent_with_clients(
        self,
        collection_client,
        vector_client,
        collection_name,
        absent_ids,
        duration=12,
        interval=3,
    ):
        t0 = time.time()
        absent = set(absent_ids)
        last_seen = set()
        while time.time() - t0 < duration:
            collection_client.refresh_load(collection_name)
            last_seen = self._query_imported_ids_with_client(vector_client, collection_name, sorted(absent))
            if last_seen:
                return last_seen, False
            time.sleep(interval)
        return last_seen, True

    def _wait_imported_ids_eventually_absent_with_clients(
        self,
        collection_client,
        vector_client,
        collection_name,
        absent_ids,
        timeout=180,
        interval=2,
    ):
        t0 = time.time()
        absent = set(absent_ids)
        last_seen = set(absent)
        while time.time() - t0 < timeout:
            collection_client.refresh_load(collection_name)
            last_seen = self._query_imported_ids_with_client(vector_client, collection_name, sorted(absent))
            if not last_seen:
                return last_seen, True
            time.sleep(interval)
        return last_seen, False

    def _upload_file_or_fail(self, storage_client, file_path, object_name):
        storage_client.client.fput_object(storage_client.bucket_name, object_name, file_path)
        storage_client.client.stat_object(storage_client.bucket_name, object_name)

    def _write_parquet_and_upload_to_both_clusters(self, rows, file_name, secondary_storage_client):
        schema = pa.schema(
            [
                ("id", pa.int64(), False),
                ("tag", pa.string(), False),
                ("phase", pa.int64(), False),
                ("vector", pa.list_(pa.float32()), False),
            ]
        )
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, file_path)
        self._upload_file_or_fail(self.storage_client, file_path, file_name)
        self._upload_file_or_fail(secondary_storage_client, file_path, file_name)
        return file_path

    def _create_base_collection(
        self,
        name,
        dim=8,
        shards_num=None,
        ttl_seconds=None,
        db_name="default",
        consistency_level=None,
    ):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        params = {}
        if shards_num is not None:
            params["shardsNum"] = shards_num
        if ttl_seconds is not None:
            params["ttlSeconds"] = ttl_seconds
        if consistency_level is not None:
            params["consistencyLevel"] = consistency_level
        if params:
            payload["params"] = params
        if db_name != "default":
            payload["dbName"] = db_name
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, db_name=db_name, timeout=120)

    def _create_hnsw_collection(self, name, metric_type, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [
                {
                    "fieldName": "vector",
                    "indexName": f"vector_hnsw_{metric_type.lower()}",
                    "indexType": "HNSW",
                    "metricType": metric_type,
                    "params": {"M": 8, "efConstruction": 64},
                }
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_ivf_collection(self, name, index_type, index_params, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [
                {
                    "fieldName": "vector",
                    "indexName": f"vector_{index_type.lower()}",
                    "indexType": index_type,
                    "metricType": "L2",
                    "params": index_params,
                }
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_diskann_collection(self, name, dim=16):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [
                {
                    "fieldName": "vector",
                    "indexName": "vector_diskann_l2",
                    "indexType": "DISKANN",
                    "metricType": "L2",
                    "params": {"search_list_size": 100},
                }
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_multi_vector_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "vector_a", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "vector_b", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [
                {
                    "fieldName": "vector_a",
                    "indexName": "vector_a_hnsw_l2",
                    "indexType": "HNSW",
                    "metricType": "L2",
                    "params": {"M": 8, "efConstruction": 64},
                },
                {
                    "fieldName": "vector_b",
                    "indexName": "vector_b_hnsw_cosine",
                    "indexType": "HNSW",
                    "metricType": "COSINE",
                    "params": {"M": 8, "efConstruction": 64},
                },
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_binary_collection(self, name, index_type, index_params, metric_type="HAMMING", dim=128):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {
                        "fieldName": "binary_vector",
                        "dataType": "BinaryVector",
                        "elementTypeParams": {"dim": f"{dim}"},
                    },
                ],
            },
            "indexParams": [
                {
                    "fieldName": "binary_vector",
                    "indexName": f"binary_vector_{index_type.lower()}",
                    "indexType": index_type,
                    "metricType": metric_type,
                    "params": index_params,
                }
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_half_vector_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {
                        "fieldName": "float16_vector",
                        "dataType": "Float16Vector",
                        "elementTypeParams": {"dim": f"{dim}"},
                    },
                    {
                        "fieldName": "bfloat16_vector",
                        "dataType": "BFloat16Vector",
                        "elementTypeParams": {"dim": f"{dim}"},
                    },
                ],
            },
            "indexParams": [
                {
                    "fieldName": "float16_vector",
                    "indexName": "float16_vector_idx",
                    "indexType": "AUTOINDEX",
                    "metricType": "L2",
                    "params": {},
                },
                {
                    "fieldName": "bfloat16_vector",
                    "indexName": "bfloat16_vector_idx",
                    "indexType": "AUTOINDEX",
                    "metricType": "L2",
                    "params": {},
                },
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_int8_vector_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {
                        "fieldName": "int8_vector",
                        "dataType": "Int8Vector",
                        "elementTypeParams": {"dim": f"{dim}"},
                    },
                ],
            },
            "indexParams": [
                {
                    "fieldName": "int8_vector",
                    "indexName": "int8_vector_hnsw_l2",
                    "indexType": "HNSW",
                    "metricType": "L2",
                    "params": {"M": 8, "efConstruction": 64},
                }
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_array_of_vector_collection(self, name, dim=8, max_capacity=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "vec", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
                "structFields": [
                    {
                        "fieldName": "my_struct",
                        "fields": [
                            {
                                "fieldName": "sub_int",
                                "dataType": "Array",
                                "elementDataType": "Int32",
                                "elementTypeParams": {"max_capacity": f"{max_capacity}"},
                            },
                            {
                                "fieldName": "sub_vec",
                                "dataType": "ArrayOfVector",
                                "elementDataType": "FloatVector",
                                "elementTypeParams": {"dim": f"{dim}", "max_capacity": f"{max_capacity}"},
                            },
                        ],
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "vec", "indexName": "vec_idx", "metricType": "L2"},
                {
                    "fieldName": "my_struct[sub_vec]",
                    "indexName": "sub_vec_idx",
                    "metricType": "COSINE",
                },
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_geometry_timestamptz_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "event_time", "dataType": "Timestamptz", "elementTypeParams": {}},
                    {"fieldName": "geo", "dataType": "Geometry", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_sparse_collection(self, name, index_type, index_params, metric_type="IP"):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                ],
            },
            "indexParams": [
                {
                    "fieldName": "sparse_vector",
                    "indexName": f"sparse_vector_{index_type.lower()}",
                    "indexType": index_type,
                    "metricType": metric_type,
                    "params": index_params,
                }
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_bm25_function_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {
                        "fieldName": "document_content",
                        "dataType": "VarChar",
                        "elementTypeParams": {
                            "max_length": "1000",
                            "enable_analyzer": True,
                            "analyzer_params": {"tokenizer": "standard"},
                            "enable_match": True,
                        },
                    },
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                ],
                "functions": [
                    {
                        "name": "bm25_fn",
                        "type": "BM25",
                        "inputFieldNames": ["document_content"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {},
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "dense_vector", "indexName": "dense_vector_idx", "metricType": "L2"},
                {
                    "fieldName": "sparse_vector",
                    "indexName": "sparse_vector_bm25_idx",
                    "indexType": "SPARSE_INVERTED_INDEX",
                    "metricType": "BM25",
                    "params": {},
                },
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_auto_id_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": True,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_partition_key_collection(self, name, dim=8, partitions_num=8):
        payload = {
            "collectionName": name,
            "params": {"partitionsNum": f"{partitions_num}"},
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {
                        "fieldName": "phase",
                        "dataType": "Int64",
                        "isPartitionKey": True,
                        "elementTypeParams": {},
                    },
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_clustering_key_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {
                        "fieldName": "phase",
                        "dataType": "Int64",
                        "isClusteringKey": True,
                        "elementTypeParams": {},
                    },
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_scalar_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "bool_field", "dataType": "Bool", "elementTypeParams": {}},
                    {"fieldName": "int8_field", "dataType": "Int8", "elementTypeParams": {}},
                    {"fieldName": "int16_field", "dataType": "Int16", "elementTypeParams": {}},
                    {"fieldName": "int32_field", "dataType": "Int32", "elementTypeParams": {}},
                    {"fieldName": "int64_field", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "float_field", "dataType": "Float", "elementTypeParams": {}},
                    {"fieldName": "double_field", "dataType": "Double", "elementTypeParams": {}},
                    {"fieldName": "varchar_field", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_scalar_index_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "flag", "dataType": "Bool", "elementTypeParams": {}},
                    {"fieldName": "score", "dataType": "Double", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [
                {"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"},
                {"fieldName": "phase", "indexName": "phase_inverted_idx", "indexType": "INVERTED", "params": {}},
                {"fieldName": "flag", "indexName": "flag_bitmap_idx", "indexType": "BITMAP", "params": {}},
                {"fieldName": "score", "indexName": "score_stl_sort_idx", "indexType": "STL_SORT", "params": {}},
                {"fieldName": "tag", "indexName": "tag_trie_idx", "indexType": "TRIE", "params": {}},
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_nullable_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "required_field", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "nullable_int", "dataType": "Int64", "nullable": True, "elementTypeParams": {}},
                    {"fieldName": "nullable_float", "dataType": "Float", "nullable": True, "elementTypeParams": {}},
                    {
                        "fieldName": "nullable_varchar",
                        "dataType": "VarChar",
                        "nullable": True,
                        "elementTypeParams": {"max_length": "64"},
                    },
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_default_value_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "required_field", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "default_int", "dataType": "Int64", "defaultValue": 999, "elementTypeParams": {}},
                    {"fieldName": "default_float", "dataType": "Float", "defaultValue": 8.5, "elementTypeParams": {}},
                    {
                        "fieldName": "default_varchar",
                        "dataType": "VarChar",
                        "defaultValue": "default_text",
                        "elementTypeParams": {"max_length": "64"},
                    },
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_json_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "category", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "json", "dataType": "JSON", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_array_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {
                        "fieldName": "int_array",
                        "dataType": "Array",
                        "elementDataType": "Int64",
                        "elementTypeParams": {"max_capacity": "16"},
                    },
                    {
                        "fieldName": "varchar_array",
                        "dataType": "Array",
                        "elementDataType": "VarChar",
                        "elementTypeParams": {"max_capacity": "16", "max_length": "64"},
                    },
                    {
                        "fieldName": "bool_array",
                        "dataType": "Array",
                        "elementDataType": "Bool",
                        "elementTypeParams": {"max_capacity": "16"},
                    },
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_dynamic_collection(self, name, dim=8):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _write_parquet_and_upload(self, rows, file_name):
        schema = pa.schema(
            [
                ("id", pa.int64(), False),
                ("tag", pa.string(), False),
                ("phase", pa.int64(), False),
                ("vector", pa.list_(pa.float32()), False),
            ]
        )
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_binary_parquet_and_upload(self, rows, file_name, dim=128):
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("tag", pa.string(), nullable=False),
                pa.field("phase", pa.int64(), nullable=False),
                pa.field("binary_vector", pa.list_(pa.uint8()), nullable=False),
            ]
        )
        file_path = f"/tmp/{file_name}"
        parquet_rows = []
        for row in rows:
            parquet_row = dict(row)
            parquet_row["binary_vector"] = list(row["binary_vector"])
            parquet_rows.append(parquet_row)
        table = pa.Table.from_pylist(parquet_rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_half_vector_parquet_and_upload(self, rows, file_name):
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("tag", pa.string(), nullable=False),
                pa.field("phase", pa.int64(), nullable=False),
                pa.field("float16_vector", pa.list_(pa.uint8()), nullable=False),
                pa.field("bfloat16_vector", pa.list_(pa.uint8()), nullable=False),
            ]
        )
        file_path = f"/tmp/{file_name}"
        parquet_rows = []
        for row in rows:
            parquet_row = dict(row)
            parquet_row["float16_vector"] = list(row["float16_vector"])
            parquet_row["bfloat16_vector"] = list(row["bfloat16_vector"])
            parquet_rows.append(parquet_row)
        table = pa.Table.from_pylist(parquet_rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_int8_vector_parquet_and_upload(self, rows, file_name):
        file_path = f"/tmp/{file_name}"
        vectors = np.array([row["int8_vector"] for row in rows], dtype=np.int8)
        table = pa.table(
            {
                "id": pa.array([row["id"] for row in rows], type=pa.int64()),
                "tag": pa.array([row["tag"] for row in rows], type=pa.string()),
                "phase": pa.array([row["phase"] for row in rows], type=pa.int64()),
                "int8_vector": pa.FixedSizeListArray.from_arrays(
                    pa.array(vectors.flatten(), type=pa.int8()),
                    list_size=vectors.shape[1],
                ),
            }
        )
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_array_of_vector_parquet_and_upload(self, rows, file_name):
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("tag", pa.string(), nullable=False),
                pa.field("phase", pa.int64(), nullable=False),
                pa.field("vec", pa.list_(pa.float32()), nullable=False),
                pa.field(
                    "my_struct",
                    pa.list_(
                        pa.struct(
                            [
                                pa.field("sub_int", pa.int32(), nullable=False),
                                pa.field("sub_vec", pa.list_(pa.float32()), nullable=False),
                            ]
                        )
                    ),
                    nullable=False,
                ),
            ]
        )
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_geometry_timestamptz_parquet_and_upload(self, rows, file_name):
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("tag", pa.string(), nullable=False),
                pa.field("event_time", pa.string(), nullable=False),
                pa.field("geo", pa.string(), nullable=False),
                pa.field("vector", pa.list_(pa.float32()), nullable=False),
            ]
        )
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_sparse_parquet_and_upload(self, rows, file_name):
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("tag", pa.string(), nullable=False),
                pa.field("phase", pa.int64(), nullable=False),
                pa.field("sparse_vector", pa.string(), nullable=False),
            ]
        )
        file_path = f"/tmp/{file_name}"
        parquet_rows = []
        for row in rows:
            parquet_row = dict(row)
            parquet_row["sparse_vector"] = json.dumps(row["sparse_vector"])
            parquet_rows.append(parquet_row)
        table = pa.Table.from_pylist(parquet_rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_sparse_struct_parquet_and_upload(self, rows, file_name):
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("tag", pa.string(), nullable=False),
                pa.field("phase", pa.int64(), nullable=False),
                pa.field(
                    "sparse_vector",
                    pa.struct(
                        [
                            pa.field("indices", pa.list_(pa.uint32()), nullable=False),
                            pa.field("values", pa.list_(pa.float32()), nullable=False),
                        ]
                    ),
                    nullable=False,
                ),
            ]
        )
        file_path = f"/tmp/{file_name}"
        parquet_rows = []
        for row in rows:
            sparse_items = sorted((int(index), float(value)) for index, value in row["sparse_vector"].items())
            parquet_rows.append(
                {
                    "id": row["id"],
                    "tag": row["tag"],
                    "phase": row["phase"],
                    "sparse_vector": {
                        "indices": [index for index, _ in sparse_items],
                        "values": [value for _, value in sparse_items],
                    },
                }
            )
        table = pa.Table.from_pylist(parquet_rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_multi_vector_parquet_and_upload(self, rows, file_name):
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("tag", pa.string(), nullable=False),
                pa.field("phase", pa.int64(), nullable=False),
                pa.field("vector_a", pa.list_(pa.float32()), nullable=False),
                pa.field("vector_b", pa.list_(pa.float32()), nullable=False),
            ]
        )
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_auto_id_parquet_and_upload(self, rows, file_name):
        schema = pa.schema(
            [
                ("tag", pa.string(), False),
                ("phase", pa.int64(), False),
                ("vector", pa.list_(pa.float32()), False),
            ]
        )
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_scalar_parquet_and_upload(self, rows, file_name):
        schema = pa.schema(
            [
                ("id", pa.int64(), False),
                ("bool_field", pa.bool_(), False),
                ("int8_field", pa.int8(), False),
                ("int16_field", pa.int16(), False),
                ("int32_field", pa.int32(), False),
                ("int64_field", pa.int64(), False),
                ("float_field", pa.float32(), False),
                ("double_field", pa.float64(), False),
                ("varchar_field", pa.string(), False),
                ("vector", pa.list_(pa.float32()), False),
            ]
        )
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_scalar_index_parquet_and_upload(self, rows, file_name):
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("tag", pa.string(), nullable=False),
                pa.field("phase", pa.int64(), nullable=False),
                pa.field("flag", pa.bool_(), nullable=False),
                pa.field("score", pa.float64(), nullable=False),
                pa.field("vector", pa.list_(pa.float32()), nullable=False),
            ]
        )
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_nullable_parquet_and_upload(self, rows, file_name):
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("required_field", pa.int64(), nullable=False),
                pa.field("nullable_int", pa.int64(), nullable=True),
                pa.field("nullable_float", pa.float32(), nullable=True),
                pa.field("nullable_varchar", pa.string(), nullable=True),
                pa.field("vector", pa.list_(pa.float32()), nullable=False),
            ]
        )
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_default_missing_parquet_and_upload(self, rows, file_name):
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("required_field", pa.int64(), nullable=False),
                pa.field("vector", pa.list_(pa.float32()), nullable=False),
            ]
        )
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_json_and_upload(self, rows, file_name):
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(rows, f)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_jsonl_and_upload(self, rows, file_name):
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_csv_and_upload(self, rows, file_name, sep="|"):
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["id", "tag", "phase", "vector"],
                delimiter=sep,
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "id": row["id"],
                        "tag": row["tag"],
                        "phase": row["phase"],
                        "vector": "[" + ",".join(str(value) for value in row["vector"]) + "]",
                    }
                )
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_nullable_csv_and_upload(self, rows, file_name, sep="|", nullkey="NULL"):
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "id",
                    "required_field",
                    "nullable_int",
                    "nullable_float",
                    "nullable_varchar",
                    "vector",
                ],
                delimiter=sep,
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "id": row["id"],
                        "required_field": row["required_field"],
                        "nullable_int": nullkey if row["nullable_int"] is None else row["nullable_int"],
                        "nullable_float": nullkey if row["nullable_float"] is None else row["nullable_float"],
                        "nullable_varchar": nullkey if row["nullable_varchar"] is None else row["nullable_varchar"],
                        "vector": "[" + ",".join(str(value) for value in row["vector"]) + "]",
                    }
                )
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_numpy_field_files_and_upload(self, rows, file_dir):
        base_path = Path("/tmp") / file_dir
        base_path.mkdir(parents=True, exist_ok=True)
        arrays = {
            "id": np.array([row["id"] for row in rows], dtype=np.int64),
            "tag": np.array([row["tag"] for row in rows]),
            "phase": np.array([row["phase"] for row in rows], dtype=np.int64),
            "vector": np.array([row["vector"] for row in rows], dtype=np.float32),
        }

        file_names = []
        for field_name, values in arrays.items():
            local_path = base_path / f"{field_name}.npy"
            object_name = f"{file_dir}/{field_name}.npy"
            np.save(local_path, values)
            self.storage_client.upload_file(str(local_path), object_name)
            file_names.append(object_name)
        return file_names, str(base_path)

    def _write_text_and_upload(self, content, file_name):
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            f.write(content)
        self.storage_client.upload_file(file_path, file_name)
        return file_path

    def _write_text_object_and_upload(self, content, object_name):
        file_path = f"/tmp/import_2pc_object_{uuid4().hex}.txt"
        with open(file_path, "w") as f:
            f.write(content)
        self.storage_client.upload_file(file_path, object_name)
        return file_path

    def _make_rows(self, start_id, count, phase, dim=8):
        rows = []
        for i in range(count):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "tag": f"phase_{phase}_{pk}",
                    "phase": phase,
                    "vector": [float(pk + j) / 1000 for j in range(dim)],
                }
            )
        return rows

    def _make_binary_rows(self, start_id, count, phase, dim=128):
        byte_width = dim // 8
        rows = []
        for i in range(count):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "tag": f"phase_{phase}_{pk}",
                    "phase": phase,
                    "binary_vector": bytes((pk + j) % 256 for j in range(byte_width)),
                }
            )
        return rows

    def _make_half_vector_rows(self, start_id, count, phase, dim=8):
        rows = []
        for i in range(count):
            pk = start_id + i
            raw = np.array([float((pk % 100) + j) / 100 for j in range(dim)], dtype=np.float32)
            float16_vector = raw.astype(np.float16).view(np.uint8).tobytes()
            bfloat16_vector = (raw.view(np.uint32) >> 16).astype(np.uint16).view(np.uint8).tobytes()
            rows.append(
                {
                    "id": pk,
                    "tag": f"phase_{phase}_{pk}",
                    "phase": phase,
                    "float16_vector": float16_vector,
                    "bfloat16_vector": bfloat16_vector,
                }
            )
        return rows

    def _make_int8_vector_rows(self, start_id, count, phase, dim=8):
        rows = []
        for i in range(count):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "tag": f"phase_{phase}_{pk}",
                    "phase": phase,
                    "int8_vector": [((pk + j) % 255) - 128 for j in range(dim)],
                }
            )
        return rows

    def _make_multi_vector_rows(self, start_id, count, phase, dim=8):
        rows = []
        for i in range(count):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "tag": f"multi_vec_{phase}_{pk}",
                    "phase": phase,
                    "vector_a": [float((pk % 100) + j) / 100 for j in range(dim)],
                    "vector_b": [float(((pk + 17) % 100) + j) / 100 for j in range(dim)],
                }
            )
        return rows

    def _make_array_of_vector_rows(self, start_id, count, phase, dim=8):
        rows = []
        for i in range(count):
            pk = start_id + i
            elem_count = 1 + (i % 3)
            struct_values = []
            for j in range(elem_count):
                struct_values.append(
                    {
                        "sub_int": pk * 10 + j,
                        "sub_vec": [float((pk % 100) + j + d) / 100 for d in range(dim)],
                    }
                )
            rows.append(
                {
                    "id": pk,
                    "tag": f"phase_{phase}_{pk}",
                    "phase": phase,
                    "vec": [float((pk % 100) + d) / 1000 for d in range(dim)],
                    "my_struct": struct_values,
                }
            )
        return rows

    def _make_geometry_timestamptz_rows(self, start_id, dim=8):
        specs = [
            ("alpha", "2025-05-30T23:46:05Z", "POINT (10.00 10.00)"),
            ("beta", "2025-05-31T00:46:05Z", "POINT (20.00 20.00)"),
            ("gamma", "2025-06-01T08:00:00Z", "POINT (30.00 30.00)"),
            ("delta", "2025-06-02T12:30:00Z", "POINT (40.00 40.00)"),
            ("epsilon", "2025-06-03T23:59:59Z", "POINT (50.00 50.00)"),
            ("zeta", "2025-06-04T01:02:03Z", "POINT (60.00 60.00)"),
        ]
        rows = []
        for i, (label, event_time, geo) in enumerate(specs):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "tag": f"geo_ts_{label}_{pk}",
                    "event_time": event_time,
                    "geo": geo,
                    "vector": [float((pk % 100) + j) / 1000 for j in range(dim)],
                }
            )
        return rows

    def _make_sparse_rows(self, start_id, count, phase):
        rows = []
        for i in range(count):
            pk = start_id + i
            base_dim = (i % 7) * 10
            rows.append(
                {
                    "id": pk,
                    "tag": f"phase_{phase}_{pk}",
                    "phase": phase,
                    "sparse_vector": {
                        base_dim + 1: 1.0 + i / 100,
                        base_dim + 3: 0.5 + i / 200,
                        base_dim + 7: 0.25 + i / 300,
                    },
                }
            )
        return rows

    def _make_bm25_function_rows(self, start_id, count, dim=8):
        rows = []
        topics = [
            "import two phase commit alpha document",
            "replication commit timestamp beta document",
            "manual import bm25 gamma document",
        ]
        for i in range(count):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "document_content": f"{topics[i % len(topics)]} row {i}",
                    "dense_vector": [float((pk % 100) + j) / 1000 for j in range(dim)],
                }
            )
        return rows

    def _make_json_rows(self, start_id, count, dim=8):
        rows = []
        for i in range(count):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "category": i % 3,
                    "json": {
                        "key": [i],
                        "nested": {"score": i, "label": f"json_{pk}"},
                        "flag": i % 2 == 0,
                    },
                    "vector": [float(pk + j) / 1000 for j in range(dim)],
                }
            )
        return rows

    def _make_array_rows(self, start_id, count, dim=8):
        rows = []
        for i in range(count):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "int_array": [i, i + 100],
                    "varchar_array": [f"tag_{i}", f"row_{pk}"],
                    "bool_array": [i % 2 == 0, True],
                    "vector": [float(pk + j) / 1000 for j in range(dim)],
                }
            )
        return rows

    def _make_dynamic_rows(self, start_id, count, dim=8):
        rows = []
        for i in range(count):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "vector": [float(pk + j) / 1000 for j in range(dim)],
                    "extra_str": f"dynamic_value_{i}",
                    "extra_int": 1000 + i,
                    "extra_bool": i % 2 == 0,
                }
            )
        return rows

    def _make_default_missing_rows(self, start_id, count, dim=8):
        rows = []
        for i in range(count):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "required_field": 3000 + i,
                    "vector": [float(pk + j) / 1000 for j in range(dim)],
                }
            )
        return rows

    def _make_scalar_index_rows(self, start_id, count, dim=8):
        rows = []
        for i in range(count):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "tag": f"scalar_tag_{i % 4}_{pk}",
                    "phase": i % 5,
                    "flag": i % 2 == 0,
                    "score": float(i) + 0.25,
                    "vector": [float((pk % 100) + j) / 100 for j in range(dim)],
                }
            )
        return rows

    def _make_nullable_rows(self, start_id, count, dim=8):
        rows = []
        for i in range(count):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "required_field": 1000 + i,
                    "nullable_int": None if i % 2 == 0 else 2000 + i,
                    "nullable_float": None if i % 3 == 0 else float(i) + 0.5,
                    "nullable_varchar": None if i % 2 == 0 else f"nullable_{pk}",
                    "vector": [float(pk + j) / 1000 for j in range(dim)],
                }
            )
        return rows

    def _make_scalar_rows(self, start_id, count, dim=8):
        rows = []
        for i in range(count):
            pk = start_id + i
            rows.append(
                {
                    "id": pk,
                    "bool_field": i % 2 == 0,
                    "int8_field": -64 + i,
                    "int16_field": -32000 + i,
                    "int32_field": 100000 + i,
                    "int64_field": 900000000000 + i,
                    "float_field": float(i) + 0.25,
                    "double_field": float(i) + 0.125,
                    "varchar_field": f"scalar_{pk}",
                    "vector": [float(pk + j) / 1000 for j in range(dim)],
                }
            )
        return rows

    def _make_auto_id_rows(self, start_tag_id, count, phase, dim=8):
        rows = []
        for i in range(count):
            tag_id = start_tag_id + i
            rows.append(
                {
                    "tag": f"auto_phase_{phase}_{tag_id}",
                    "phase": phase,
                    "vector": [float(tag_id + j) / 1000 for j in range(dim)],
                }
            )
        return rows

    def _normalize_array_value(self, value):
        if not isinstance(value, dict):
            return value

        data = value.get("Data", value.get("data"))
        if not isinstance(data, dict):
            return value

        for field_name in ("LongData", "StringData", "BoolData", "FloatData", "DoubleData", "IntData"):
            typed_data = data.get(field_name)
            if isinstance(typed_data, dict) and "data" in typed_data:
                return typed_data["data"]
        return value

    def _create_manual_import_job(self, collection_name, file_name, partition_name=None, db_name="default"):
        payload = {
            "collectionName": collection_name,
            "files": [[file_name]],
            "options": {"auto_commit": "false"},
        }
        if partition_name is not None:
            payload["partitionName"] = partition_name
        rsp = self.import_job_client.create_import_jobs(payload, db_name=db_name)
        assert rsp["code"] == 0, rsp
        return rsp["data"]["jobId"]

    def _create_manual_import_job_with_files(self, collection_name, file_names, partition_name=None, db_name="default"):
        payload = {
            "collectionName": collection_name,
            "files": [[file_name] for file_name in file_names],
            "options": {"auto_commit": "false"},
        }
        if partition_name is not None:
            payload["partitionName"] = partition_name
        rsp = self.import_job_client.create_import_jobs(payload, db_name=db_name)
        assert rsp["code"] == 0, rsp
        return rsp["data"]["jobId"]

    def _create_import_job(self, collection_name, file_name, options=None, partition_name=None):
        payload = {
            "collectionName": collection_name,
            "files": [[file_name]],
        }
        if partition_name is not None:
            payload["partitionName"] = partition_name
        if options is not None:
            payload["options"] = options
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp["code"] == 0, rsp
        return rsp["data"]["jobId"]

    def _create_import_job_with_file_groups(self, collection_name, file_groups, options=None, partition_name=None):
        payload = {
            "collectionName": collection_name,
            "files": file_groups,
        }
        if partition_name is not None:
            payload["partitionName"] = partition_name
        if options is not None:
            payload["options"] = options
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp["code"] == 0, rsp
        return rsp["data"]["jobId"]

    def _query_imported_ids(self, collection_name, ids, partition_names=None, db_name="default"):
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {ids}",
            "outputFields": ["id"],
            "limit": len(ids),
        }
        if partition_names is not None:
            payload["partitionNames"] = partition_names
        rsp = self.vector_client.vector_query(payload, db_name=db_name, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _query_base_rows_by_ids(self, collection_name, ids):
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {sorted(ids)}",
            "outputFields": ["id", "tag", "phase"],
            "limit": len(ids),
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    def _query_array_of_vector_rows_by_ids(self, collection_name, ids):
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {ids}",
            "outputFields": ["id", "tag", "phase", "my_struct"],
            "limit": len(ids),
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    def _query_geometry_timestamptz_rows_by_ids(self, collection_name, ids):
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {ids}",
            "outputFields": ["id", "tag", "event_time", "geo"],
            "limit": len(ids),
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    def _query_scalar_rows_by_ids(self, collection_name, ids):
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {ids}",
            "outputFields": [
                "id",
                "bool_field",
                "int8_field",
                "int16_field",
                "int32_field",
                "int64_field",
                "float_field",
                "double_field",
                "varchar_field",
            ],
            "limit": len(ids),
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    def _query_scalar_index_rows(self, collection_name, filter_expr, limit):
        payload = {
            "collectionName": collection_name,
            "filter": filter_expr,
            "outputFields": ["id", "tag", "phase", "flag", "score"],
            "limit": limit,
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    def _query_rows_by_tags(self, collection_name, tags):
        quoted_tags = ", ".join(json.dumps(tag) for tag in sorted(tags))
        payload = {
            "collectionName": collection_name,
            "filter": f"tag in [{quoted_tags}]",
            "outputFields": ["id", "tag", "phase"],
            "limit": len(tags),
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    def _query_nullable_rows_by_ids(self, collection_name, ids):
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {ids}",
            "outputFields": [
                "id",
                "required_field",
                "nullable_int",
                "nullable_float",
                "nullable_varchar",
            ],
            "limit": len(ids),
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    def _query_default_rows_by_ids(self, collection_name, ids):
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {ids}",
            "outputFields": [
                "id",
                "required_field",
                "default_int",
                "default_float",
                "default_varchar",
            ],
            "limit": len(ids),
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    def _query_ids_by_filter(self, collection_name, filter_expr, limit=100):
        payload = {
            "collectionName": collection_name,
            "filter": filter_expr,
            "outputFields": ["id"],
            "limit": limit,
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _query_json_rows_by_ids(self, collection_name, ids):
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {ids}",
            "outputFields": ["id", "category", "json"],
            "limit": len(ids),
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    def _query_array_rows_by_ids(self, collection_name, ids):
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {ids}",
            "outputFields": ["id", "int_array", "varchar_array", "bool_array"],
            "limit": len(ids),
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    def _query_dynamic_rows_by_ids(self, collection_name, ids):
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {ids}",
            "outputFields": ["id", "extra_str", "extra_int", "extra_bool"],
            "limit": len(ids),
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    def _query_count(self, collection_name, filter_expr=" ", db_name="default"):
        payload = {
            "collectionName": collection_name,
            "filter": filter_expr,
            "limit": 0,
            "outputFields": ["count(*)"],
        }
        rsp = self.vector_client.vector_query(payload, db_name=db_name, timeout=30)
        assert rsp["code"] == 0, rsp
        assert len(rsp.get("data", [])) == 1, rsp
        return rsp["data"][0]["count(*)"]

    def _wait_count(self, collection_name, expected_count, timeout=120, db_name="default"):
        t0 = time.time()
        last_count = None
        while time.time() - t0 < timeout:
            self.collection_client.refresh_load(collection_name, db_name=db_name)
            last_count = self._query_count(collection_name, db_name=db_name)
            if last_count == expected_count:
                return last_count, True
            time.sleep(2)
        return last_count, False

    def _wait_imported_ids_visible(self, collection_name, expected_ids, timeout=120, db_name="default"):
        t0 = time.time()
        expected = set(expected_ids)
        last_seen = set()
        while time.time() - t0 < timeout:
            self.collection_client.refresh_load(collection_name, db_name=db_name)
            last_seen = self._query_imported_ids(collection_name, sorted(expected), db_name=db_name)
            if last_seen == expected:
                return last_seen, True
            time.sleep(2)
        return last_seen, False

    def _wait_tags_visible(self, collection_name, expected_tags, timeout=120):
        t0 = time.time()
        expected = set(expected_tags)
        last_rows = []
        while time.time() - t0 < timeout:
            self.collection_client.refresh_load(collection_name)
            last_rows = self._query_rows_by_tags(collection_name, expected)
            if {row["tag"] for row in last_rows} == expected:
                return last_rows, True
            time.sleep(2)
        return last_rows, False

    def _wait_base_rows_by_ids_match(self, collection_name, expected_by_id, timeout=120):
        t0 = time.time()
        expected_ids = set(expected_by_id.keys())
        last_rows = []
        while time.time() - t0 < timeout:
            self.collection_client.refresh_load(collection_name)
            last_rows = self._query_base_rows_by_ids(collection_name, expected_ids)
            actual_by_id = {row["id"]: row for row in last_rows}
            if len(last_rows) == len(expected_by_id) and set(actual_by_id.keys()) == expected_ids:
                matched = True
                for pk, expected in expected_by_id.items():
                    actual = actual_by_id[pk]
                    if actual["tag"] != expected["tag"] or actual["phase"] != expected["phase"]:
                        matched = False
                        break
                if matched:
                    return last_rows, True
            time.sleep(2)
        return last_rows, False

    def _wait_imported_ids_absent(self, collection_name, absent_ids, duration=12, interval=3, db_name="default"):
        t0 = time.time()
        absent = set(absent_ids)
        last_seen = set()
        while time.time() - t0 < duration:
            self.collection_client.refresh_load(collection_name, db_name=db_name)
            last_seen = self._query_imported_ids(collection_name, sorted(absent), db_name=db_name)
            if last_seen:
                return last_seen, False
            time.sleep(interval)
        return last_seen, True

    def _wait_tags_absent(self, collection_name, absent_tags, duration=12, interval=3):
        t0 = time.time()
        absent = set(absent_tags)
        last_rows = []
        while time.time() - t0 < duration:
            self.collection_client.refresh_load(collection_name)
            last_rows = self._query_rows_by_tags(collection_name, absent)
            if last_rows:
                return last_rows, False
            time.sleep(interval)
        return last_rows, True

    def _wait_imported_ids_eventually_absent(self, collection_name, absent_ids, timeout=120, interval=2):
        t0 = time.time()
        absent = set(absent_ids)
        last_seen = set(absent)
        while time.time() - t0 < timeout:
            self.collection_client.refresh_load(collection_name)
            last_seen = self._query_imported_ids(collection_name, sorted(absent))
            if not last_seen:
                return last_seen, True
            time.sleep(interval)
        return last_seen, False

    def _wait_compaction_completed(self, compaction_id, timeout=180, interval=3):
        t0 = time.time()
        last_rsp = None
        while time.time() - t0 < timeout:
            last_rsp = self.collection_client.get_compaction_state(compaction_id)
            if last_rsp["code"] == 0:
                state = last_rsp.get("data", {}).get("state")
                if state == "Completed":
                    return last_rsp, True
            time.sleep(interval)
        return last_rsp, False

    def _flush_collection_with_retry(self, collection_name, timeout=90, interval=11):
        t0 = time.time()
        last_rsp = None
        while time.time() - t0 < timeout:
            last_rsp = self.collection_client.flush(collection_name)
            if last_rsp["code"] == 0:
                return last_rsp, True
            if last_rsp["code"] != 1807:
                return last_rsp, False
            time.sleep(interval)
        return last_rsp, False

    def _search_imported_ids(self, collection_name, vector, limit):
        payload = {
            "collectionName": collection_name,
            "data": [vector],
            "annsField": "vector",
            "limit": limit,
            "outputFields": ["id"],
        }
        rsp = self.vector_client.vector_search(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _search_half_vector_imported_ids(self, collection_name, anns_field, vector, limit):
        payload = {
            "collectionName": collection_name,
            "data": [base64.b64encode(vector).decode("utf-8")],
            "annsField": anns_field,
            "limit": limit,
            "outputFields": ["id"],
            "searchParams": {"metricType": "L2"},
        }
        rsp = self.vector_client.vector_search(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _search_array_of_vector_imported_ids(self, collection_name, vector, limit):
        payload = {
            "collectionName": collection_name,
            "data": [vector],
            "annsField": "my_struct[sub_vec]",
            "limit": limit,
            "outputFields": ["id"],
            "searchParams": {"metricType": "COSINE"},
        }
        rsp = self.vector_client.vector_search(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _search_int8_vector_imported_ids(self, collection_name, vector, limit):
        payload = {
            "collectionName": collection_name,
            "data": [vector],
            "annsField": "int8_vector",
            "limit": limit,
            "outputFields": ["id"],
            "searchParams": {"metricType": "L2", "params": {"ef": 64}},
        }
        rsp = self.vector_client.vector_search(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _search_sparse_imported_ids(self, collection_name, vector, limit):
        payload = {
            "collectionName": collection_name,
            "data": [vector],
            "annsField": "sparse_vector",
            "limit": limit,
            "outputFields": ["id"],
            "searchParams": {
                "metricType": "IP",
                "params": {"drop_ratio_search": "0.2"},
            },
        }
        rsp = self.vector_client.vector_search(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _search_diskann_imported_ids(self, collection_name, vector, limit):
        payload = {
            "collectionName": collection_name,
            "data": [vector],
            "annsField": "vector",
            "limit": limit,
            "outputFields": ["id"],
            "searchParams": {"metricType": "L2", "params": {"search_list": max(limit, 128)}},
        }
        rsp = self.vector_client.vector_search(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _search_named_float_vector_imported_ids(self, collection_name, anns_field, vector, limit, metric_type):
        payload = {
            "collectionName": collection_name,
            "data": [vector],
            "annsField": anns_field,
            "limit": limit,
            "outputFields": ["id"],
            "searchParams": {"metricType": metric_type, "params": {"ef": 64}},
        }
        rsp = self.vector_client.vector_search(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _search_bm25_imported_ids(self, collection_name, query_text, limit):
        payload = {
            "collectionName": collection_name,
            "data": [query_text],
            "annsField": "sparse_vector",
            "limit": limit,
            "outputFields": ["id", "document_content"],
            "searchParams": {"metric_type": "BM25"},
        }
        rsp = self.vector_client.vector_search(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _wait_bm25_search_hits(self, collection_name, query_text, expected_any_ids, timeout=120, limit=5):
        t0 = time.time()
        expected = set(expected_any_ids)
        last_seen = set()
        while time.time() - t0 < timeout:
            self.collection_client.refresh_load(collection_name)
            last_seen = self._search_bm25_imported_ids(collection_name, query_text, limit)
            if last_seen & expected:
                return last_seen, True
            time.sleep(2)
        return last_seen, False

    def _search_imported_tags(self, collection_name, vector, limit):
        payload = {
            "collectionName": collection_name,
            "data": [vector],
            "annsField": "vector",
            "limit": limit,
            "outputFields": ["id", "tag"],
        }
        rsp = self.vector_client.vector_search(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["tag"] for row in rsp.get("data", [])}

    def _search_binary_imported_ids(self, collection_name, vector, limit, metric_type="HAMMING"):
        payload = {
            "collectionName": collection_name,
            "data": [base64.b64encode(vector).decode("utf-8")],
            "annsField": "binary_vector",
            "limit": limit,
            "outputFields": ["id"],
            "searchParams": {"metricType": metric_type},
        }
        rsp = self.vector_client.vector_search(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _insert_one_row(self, collection_name, row):
        payload = {
            "collectionName": collection_name,
            "data": [row],
        }
        rsp = self.vector_client.vector_insert(payload)
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == 1, rsp

    def _insert_rows(self, collection_name, rows):
        payload = {
            "collectionName": collection_name,
            "data": rows,
        }
        rsp = self.vector_client.vector_insert(payload)
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == len(rows), rsp

    def _upsert_rows(self, collection_name, rows):
        payload = {
            "collectionName": collection_name,
            "data": rows,
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp["code"] == 0, rsp
        if "data" in rsp and "upsertCount" in rsp["data"]:
            assert rsp["data"]["upsertCount"] == len(rows), rsp
        return rsp

    def _delete_ids(self, collection_name, ids):
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {sorted(ids)}",
        }
        rsp = self.vector_client.vector_delete(payload)
        assert rsp["code"] == 0, rsp
        return rsp

    def _delete_tags(self, collection_name, tags):
        quoted_tags = ", ".join(json.dumps(tag) for tag in sorted(tags))
        payload = {
            "collectionName": collection_name,
            "filter": f"tag in [{quoted_tags}]",
        }
        rsp = self.vector_client.vector_delete(payload)
        assert rsp["code"] == 0, rsp
        return rsp

    def _assert_import_create_rejected(self, payload, expected_reason_part):
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp["code"] != 0, rsp
        readable_reason = str(rsp.get("message", rsp.get("reason", rsp)))
        assert readable_reason, rsp
        expected_reason_parts = (
            (expected_reason_part,) if isinstance(expected_reason_part, str) else tuple(expected_reason_part)
        )
        assert any(reason_part.lower() in str(rsp).lower() for reason_part in expected_reason_parts), rsp
        assert "jobId" not in str(rsp), rsp
        return rsp

    def _assert_import_job_endpoint_rejected(self, endpoint_name, call_endpoint, expected_reason_part):
        rsp = call_endpoint()
        assert rsp["code"] != 0, {"endpoint": endpoint_name, "rsp": rsp}
        readable_reason = str(rsp.get("message", rsp.get("reason", rsp)))
        assert readable_reason, {"endpoint": endpoint_name, "rsp": rsp}
        expected_reason_parts = (
            (expected_reason_part,) if isinstance(expected_reason_part, str) else tuple(expected_reason_part)
        )
        assert any(reason_part.lower() in str(rsp).lower() for reason_part in expected_reason_parts), {
            "endpoint": endpoint_name,
            "rsp": rsp,
        }
        return rsp

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_rest_api_contract_smoke(self):
        """
        target: import 2PC REST API contract
        method: create manual import jobs, then verify progress/list/describe/commit/abort paths
        expected: valid paths return success, invalid jobId is rejected, commit reaches Completed and abort reaches Failed
        """
        collection_name = gen_collection_name(prefix="import_2pc_rest")
        self._create_base_collection(collection_name)

        commit_file = f"import_2pc_commit_{uuid4()}.parquet"
        abort_file = f"import_2pc_abort_{uuid4()}.parquet"
        self._write_parquet_and_upload(self._make_rows(1000, 4, phase=1), commit_file)
        self._write_parquet_and_upload(self._make_rows(2000, 4, phase=2), abort_file)

        commit_job_id = self._create_manual_import_job(collection_name, commit_file)
        rsp, ok = self.import_job_client.wait_import_job_state(commit_job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        describe_rsp = self.import_job_client.describe_import_job(commit_job_id)
        assert describe_rsp["code"] == 0, describe_rsp
        assert describe_rsp["data"]["jobId"] == commit_job_id
        assert describe_rsp["data"]["state"] == "Uncommitted"

        list_rsp = self.import_job_client.list_import_jobs({"collectionName": collection_name})
        assert list_rsp["code"] == 0, list_rsp
        listed_job_ids = {record["jobId"] for record in list_rsp["data"]["records"]}
        assert commit_job_id in listed_job_ids

        commit_rsp = self.import_job_client.commit_import_job(commit_job_id)
        assert commit_rsp["code"] == 0, commit_rsp

        duplicate_commit_rsp = self.import_job_client.commit_import_job(commit_job_id)
        assert duplicate_commit_rsp["code"] == 0, duplicate_commit_rsp

        rsp, ok = self.import_job_client.wait_import_job_state(commit_job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        abort_job_id = self._create_manual_import_job(collection_name, abort_file)
        rsp, ok = self.import_job_client.wait_import_job_state(abort_job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        abort_rsp = self.import_job_client.abort_import_job(abort_job_id)
        assert abort_rsp["code"] == 0, abort_rsp

        rsp, ok = self.import_job_client.wait_import_job_state(abort_job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        invalid_commit_rsp = self.import_job_client.commit_import_job("not-a-number")
        assert invalid_commit_rsp["code"] == 1100, invalid_commit_rsp

        invalid_abort_rsp = self.import_job_client.abort_import_job("not-a-number")
        assert invalid_abort_rsp["code"] == 1100, invalid_abort_rsp

        for file_path in [f"/tmp/{commit_file}", f"/tmp/{abort_file}"]:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_list_filters_by_collection_name_without_cross_collection_leak(self):
        """
        target: REST import list collectionName filter isolation
        method: create one manual import job in each of two collections and list them separately by collectionName
        expected: each filtered list contains only the target collection job and never returns the other collection job
        """
        first_collection = gen_collection_name(prefix="import_2pc_list_filter_a")
        second_collection = gen_collection_name(prefix="import_2pc_list_filter_b")
        self._create_base_collection(first_collection)
        self._create_base_collection(second_collection)

        first_rows = self._make_rows(9281000, 3, phase=9281)
        second_rows = self._make_rows(9282000, 3, phase=9282)
        first_file = f"import_2pc_list_filter_a_{uuid4()}.parquet"
        second_file = f"import_2pc_list_filter_b_{uuid4()}.parquet"
        self._write_parquet_and_upload(first_rows, first_file)
        self._write_parquet_and_upload(second_rows, second_file)

        first_job_id = None
        second_job_id = None
        try:
            first_job_id = self._create_manual_import_job(first_collection, first_file)
            first_rsp, first_ready = self.import_job_client.wait_import_job_state(
                first_job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert first_ready, first_rsp

            second_job_id = self._create_manual_import_job(second_collection, second_file)
            second_rsp, second_ready = self.import_job_client.wait_import_job_state(
                second_job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert second_ready, second_rsp

            first_list_rsp = self.import_job_client.list_import_jobs({"collectionName": first_collection})
            assert first_list_rsp["code"] == 0, first_list_rsp
            first_records = first_list_rsp.get("data", {}).get("records", [])
            first_job_ids = {record.get("jobId") for record in first_records}
            assert first_job_id in first_job_ids, first_list_rsp
            assert second_job_id not in first_job_ids, {
                "first_collection": first_collection,
                "second_collection": second_collection,
                "first_list_rsp": first_list_rsp,
            }
            assert all(record.get("collectionName") == first_collection for record in first_records), first_list_rsp

            second_list_rsp = self.import_job_client.list_import_jobs({"collectionName": second_collection})
            assert second_list_rsp["code"] == 0, second_list_rsp
            second_records = second_list_rsp.get("data", {}).get("records", [])
            second_job_ids = {record.get("jobId") for record in second_records}
            assert second_job_id in second_job_ids, second_list_rsp
            assert first_job_id not in second_job_ids, {
                "first_collection": first_collection,
                "second_collection": second_collection,
                "second_list_rsp": second_list_rsp,
            }
            assert all(record.get("collectionName") == second_collection for record in second_records), second_list_rsp
        finally:
            for job_id in (first_job_id, second_job_id):
                if job_id is not None:
                    abort_rsp = self.import_job_client.abort_import_job(job_id)
                    if abort_rsp["code"] == 0:
                        self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
            for file_name in (first_file, second_file):
                file_path = f"/tmp/{file_name}"
                if os.path.exists(file_path):
                    os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_list_empty_collection_filter_returns_no_records(self):
        """
        target: REST import list empty collectionName filter
        method: create a collection without any import jobs and list import jobs by collectionName
        expected: list succeeds and returns an empty records list for the target collection
        """
        collection_name = gen_collection_name(prefix="import_2pc_list_empty")
        self._create_base_collection(collection_name)

        list_rsp = self.import_job_client.list_import_jobs({"collectionName": collection_name})
        assert list_rsp["code"] == 0, list_rsp
        records = list_rsp.get("data", {}).get("records", [])
        assert records == [], list_rsp

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_list_rejects_numeric_collection_name(self):
        """
        target: REST import list collectionName type validation
        method: call import job list with numeric collectionName
        expected: request is rejected synchronously with a readable type error and no records are returned
        """
        list_rsp = self.import_job_client.list_import_jobs({"collectionName": 12345})
        assert list_rsp["code"] != 0, list_rsp
        readable_reason = str(list_rsp.get("message", list_rsp.get("reason", list_rsp))).lower()
        assert readable_reason, list_rsp
        assert any(keyword in readable_reason for keyword in ("collection", "string", "type", "number")), list_rsp
        records = list_rsp.get("data", {}).get("records", [])
        assert records == [], list_rsp

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_list_without_collection_name_includes_current_job(self):
        """
        target: REST import list without collectionName filter
        method: create one manual import job and call list with no collectionName filter
        expected: unfiltered list succeeds and includes the current job summary
        """
        collection_name = gen_collection_name(prefix="import_2pc_list_unfiltered")
        self._create_base_collection(collection_name)

        rows = self._make_rows(9290000, 3, phase=9290)
        file_name = f"import_2pc_list_unfiltered_{uuid4()}.parquet"
        file_path = self._write_parquet_and_upload(rows, file_name)
        job_id = None

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ready = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ready, rsp

            list_rsp = self.import_job_client.list_import_jobs({})
            assert list_rsp["code"] == 0, list_rsp
            records = list_rsp.get("data", {}).get("records", [])
            matching_records = [record for record in records if record.get("jobId") == job_id]
            assert len(matching_records) == 1, {"job_id": job_id, "list_rsp": list_rsp}
            record = matching_records[0]
            assert record["collectionName"] == collection_name, list_rsp
            assert record["state"] == "Uncommitted", list_rsp
            assert 0 <= record["progress"] <= 100, list_rsp
        finally:
            if job_id is not None:
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                if abort_rsp["code"] == 0:
                    self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_missing_required_fields(self):
        """
        target: import create REST required field validation
        method: submit create requests missing collectionName/files or with empty files
        expected: each invalid request is rejected synchronously without creating a job
        """
        collection_name = gen_collection_name(prefix="import_2pc_rest_missing_fields")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "files": [["missing_collection.parquet"]],
                "options": {"auto_commit": "false"},
            },
            "collection",
        )

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "options": {"auto_commit": "false"},
            },
            "files",
        )

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [],
                "options": {"auto_commit": "false"},
            },
            ("files", "empty"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_numeric_collection_name(self):
        """
        target: import create collectionName type validation
        method: submit create request with numeric collectionName
        expected: request is rejected synchronously with a readable type error and no jobId
        """
        self._assert_import_create_rejected(
            {
                "collectionName": 12345,
                "files": [["import_2pc_numeric_collection.parquet"]],
                "options": {"auto_commit": "false"},
            },
            ("collection", "string", "type", "number"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_string_files_value(self):
        """
        target: import create files type validation
        method: submit create request with files as a string instead of a list of file groups
        expected: request is rejected synchronously with a readable type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_files_type")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": "import_2pc_bad_files_type.parquet",
                "options": {"auto_commit": "false"},
            },
            ("files", "array", "list", "type", "string"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_string_options_value(self):
        """
        target: import create options type validation
        method: submit create request with options as a string instead of a map/object
        expected: request is rejected synchronously with a readable type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_options_type")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_bad_options_type.parquet"]],
                "options": "auto_commit=false",
            },
            ("options", "object", "map", "type", "string"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_numeric_partition_name(self):
        """
        target: import create partitionName type validation
        method: submit create request with numeric partitionName
        expected: request is rejected synchronously with a readable type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_partition_type")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "partitionName": 12345,
                "files": [["import_2pc_bad_partition_type.parquet"]],
                "options": {"auto_commit": "false"},
            },
            ("partition", "string", "type", "number"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_numeric_file_path(self):
        """
        target: import create file path type validation
        method: submit create request with numeric file path inside files
        expected: request is rejected synchronously with a readable type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_file_path_type")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [[12345]],
                "options": {"auto_commit": "false"},
            },
            ("files", "string", "type", "number"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_flat_files_list(self):
        """
        target: import create files nested shape validation
        method: submit create request with files as a flat list instead of a list of file groups
        expected: request is rejected synchronously with a readable type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_flat_files")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": ["import_2pc_bad_flat_files.parquet"],
                "options": {"auto_commit": "false"},
            },
            ("files", "array", "list", "type", "string"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_empty_file_group(self):
        """
        target: import create empty file group validation
        method: submit create request with files containing one empty file group
        expected: request is rejected synchronously with a readable files error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_empty_group")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [[]],
                "options": {"auto_commit": "false"},
            },
            ("files", "file", "empty"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_numeric_timeout_option(self):
        """
        target: import create timeout option type validation
        method: submit create request with numeric timeout option value
        expected: request is rejected synchronously with a readable timeout/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_timeout_type")
        self._create_base_collection(collection_name)

        rows = self._make_rows(9345000, 2, phase=9345)
        file_name = f"import_2pc_bad_timeout_type_{uuid4()}.parquet"
        file_path = self._write_parquet_and_upload(rows, file_name)

        try:
            self._assert_import_create_rejected(
                {
                    "collectionName": collection_name,
                    "files": [[file_name]],
                    "options": {"auto_commit": "false", "timeout": 12345},
                },
                ("timeout", "string", "type", "number"),
            )
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_read_job_endpoints_reject_boolean_job_id(self):
        """
        target: import read job endpoint jobId type validation
        method: call describe/get_progress with boolean jobId
        expected: both endpoints reject the request synchronously with a readable jobId/type error
        """
        self._assert_import_job_endpoint_rejected(
            "describe",
            lambda: self.import_job_client.describe_import_job(True),
            ("job", "id", "type", "bool"),
        )
        self._assert_import_job_endpoint_rejected(
            "get_progress",
            lambda: self.import_job_client.get_import_job_progress(True),
            ("job", "id", "type", "bool"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_write_job_endpoints_reject_boolean_job_id(self):
        """
        target: import write job endpoint jobId type validation
        method: call commit/abort with boolean jobId
        expected: both endpoints reject the request synchronously with a readable jobId/type error
        """
        self._assert_import_job_endpoint_rejected(
            "commit",
            lambda: self.import_job_client.commit_import_job(True),
            ("job", "id", "type", "bool"),
        )
        self._assert_import_job_endpoint_rejected(
            "abort",
            lambda: self.import_job_client.abort_import_job(True),
            ("job", "id", "type", "bool"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_null_collection_name(self):
        """
        target: import create collectionName null validation
        method: submit create request with collectionName as null
        expected: request is rejected synchronously with a readable collection/type error and no jobId
        """
        self._assert_import_create_rejected(
            {
                "collectionName": None,
                "files": [["import_2pc_null_collection.parquet"]],
                "options": {"auto_commit": "false"},
            },
            ("collection", "name", "string", "null"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_null_files(self):
        """
        target: import create files null validation
        method: submit create request with files as null
        expected: request is rejected synchronously with a readable files/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_null_files")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": None,
                "options": {"auto_commit": "false"},
            },
            ("files", "array", "list", "null"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_array_partition_name(self):
        """
        target: import create partitionName array validation
        method: submit create request with partitionName as an array
        expected: request is rejected synchronously with a readable partition/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_array_partition")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "partitionName": ["_default"],
                "files": [["import_2pc_array_partition.parquet"]],
                "options": {"auto_commit": "false"},
            },
            ("partition", "string", "array", "type"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_read_job_endpoints_reject_array_job_id(self):
        """
        target: import read job endpoint jobId array validation
        method: call describe/get_progress with array jobId
        expected: both endpoints reject the request synchronously with a readable jobId/type error
        """
        self._assert_import_job_endpoint_rejected(
            "describe",
            lambda: self.import_job_client.describe_import_job(["1"]),
            ("job", "id", "array", "type", "unmarshal", "parse"),
        )
        self._assert_import_job_endpoint_rejected(
            "get_progress",
            lambda: self.import_job_client.get_import_job_progress(["1"]),
            ("job", "id", "array", "type", "unmarshal", "parse"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_write_job_endpoints_reject_array_job_id(self):
        """
        target: import write job endpoint jobId array validation
        method: call commit/abort with array jobId
        expected: both endpoints reject the request synchronously with a readable jobId/type error
        """
        self._assert_import_job_endpoint_rejected(
            "commit",
            lambda: self.import_job_client.commit_import_job(["1"]),
            ("job", "id", "array", "type", "unmarshal", "parse"),
        )
        self._assert_import_job_endpoint_rejected(
            "abort",
            lambda: self.import_job_client.abort_import_job(["1"]),
            ("job", "id", "array", "type", "unmarshal", "parse"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_job_endpoints_reject_object_job_id(self):
        """
        target: import job endpoint jobId object validation
        method: call describe/get_progress/commit/abort with object jobId
        expected: all endpoints reject the request synchronously with a readable jobId/type error
        """
        invalid_job_id = {"id": "1"}
        endpoint_calls = {
            "describe": lambda: self.import_job_client.describe_import_job(invalid_job_id),
            "get_progress": lambda: self.import_job_client.get_import_job_progress(invalid_job_id),
            "commit": lambda: self.import_job_client.commit_import_job(invalid_job_id),
            "abort": lambda: self.import_job_client.abort_import_job(invalid_job_id),
        }

        for endpoint_name, call_endpoint in endpoint_calls.items():
            self._assert_import_job_endpoint_rejected(
                endpoint_name,
                call_endpoint,
                ("job", "id", "object", "type", "unmarshal", "parse"),
            )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_array_collection_name(self):
        """
        target: import create collectionName array validation
        method: submit create request with collectionName as an array
        expected: request is rejected synchronously with a readable collection/type error and no jobId
        """
        self._assert_import_create_rejected(
            {
                "collectionName": ["import_2pc_array_collection"],
                "files": [["import_2pc_array_collection.parquet"]],
                "options": {"auto_commit": "false"},
            },
            ("collection", "name", "array", "type", "unmarshal", "string"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_object_collection_name(self):
        """
        target: import create collectionName object validation
        method: submit create request with collectionName as an object
        expected: request is rejected synchronously with a readable collection/type error and no jobId
        """
        self._assert_import_create_rejected(
            {
                "collectionName": {"name": "import_2pc_object_collection"},
                "files": [["import_2pc_object_collection.parquet"]],
                "options": {"auto_commit": "false"},
            },
            ("collection", "name", "object", "type", "unmarshal", "string"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_object_files_value(self):
        """
        target: import create files object validation
        method: submit create request with files as an object
        expected: request is rejected synchronously with a readable files/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_object_files")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": {"path": "import_2pc_object_files.parquet"},
                "options": {"auto_commit": "false"},
            },
            ("files", "object", "array", "list", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_object_file_path(self):
        """
        target: import create nested file path object validation
        method: submit create request with object file path inside files
        expected: request is rejected synchronously with a readable files/path type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_object_file_path")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [[{"path": "import_2pc_object_file_path.parquet"}]],
                "options": {"auto_commit": "false"},
            },
            ("files", "path", "object", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_array_options_value(self):
        """
        target: import create options array validation
        method: submit create request with options as an array
        expected: request is rejected synchronously with a readable options/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_array_options")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_array_options.parquet"]],
                "options": ["auto_commit=false"],
            },
            ("options", "array", "object", "map", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_numeric_options_value(self):
        """
        target: import create options numeric validation
        method: submit create request with options as a number
        expected: request is rejected synchronously with a readable options/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_numeric_options")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_numeric_options.parquet"]],
                "options": 12345,
            },
            ("options", "number", "object", "map", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_boolean_auto_commit_option(self):
        """
        target: import create auto_commit option value type validation
        method: submit create request with options.auto_commit as a boolean
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_bool_auto_commit")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_bool_auto_commit.parquet"]],
                "options": {"auto_commit": False},
            },
            ("auto_commit", "bool", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_null_auto_commit_option(self):
        """
        target: import create auto_commit option null validation
        method: submit create request with options.auto_commit as null
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_null_auto_commit")
        self._create_base_collection(collection_name)

        payload = {
            "collectionName": collection_name,
            "files": [["import_2pc_null_auto_commit.parquet"]],
            "options": {"auto_commit": None},
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        if rsp.get("code") == 0:
            job_id = rsp.get("data", {}).get("jobId")
            if job_id is not None:
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                if abort_rsp.get("code") == 0:
                    self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
        assert rsp["code"] != 0, rsp
        assert any(
            reason_part in str(rsp).lower() for reason_part in ("auto_commit", "null", "string", "type", "unmarshal")
        ), rsp
        assert "jobId" not in str(rsp), rsp

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_array_auto_commit_option(self):
        """
        target: import create auto_commit option array validation
        method: submit create request with options.auto_commit as an array
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_array_auto_commit")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_array_auto_commit.parquet"]],
                "options": {"auto_commit": ["false"]},
            },
            ("auto_commit", "array", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_boolean_backup_option(self):
        """
        target: import create backup option value type validation
        method: submit create request with options.backup as a boolean
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_bool_backup")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "partitionName": "_default",
                "files": [["import_2pc_bool_backup"]],
                "options": {"auto_commit": "false", "backup": True},
            },
            ("backup", "bool", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_boolean_l0_import_option(self):
        """
        target: import create l0_import option value type validation
        method: submit create request with options.l0_import as a boolean
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_bool_l0_import")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_bool_l0_import"]],
                "options": {"auto_commit": "false", "l0_import": True},
            },
            ("l0_import", "bool", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_boolean_skip_disk_quota_check_option(self):
        """
        target: import create skip_disk_quota_check option value type validation
        method: submit create request with options.skip_disk_quota_check as a boolean
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_bool_skip_quota")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_bool_skip_quota.parquet"]],
                "options": {"auto_commit": "false", "skip_disk_quota_check": True},
            },
            ("skip_disk_quota_check", "bool", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_array_backup_option(self):
        """
        target: import create backup option array validation
        method: submit create request with options.backup as an array
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_array_backup")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "partitionName": "_default",
                "files": [["import_2pc_array_backup"]],
                "options": {"auto_commit": "false", "backup": ["true"]},
            },
            ("backup", "array", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_object_backup_option(self):
        """
        target: import create backup option object validation
        method: submit create request with options.backup as an object
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_object_backup")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "partitionName": "_default",
                "files": [["import_2pc_object_backup"]],
                "options": {"auto_commit": "false", "backup": {"enabled": "true"}},
            },
            ("backup", "object", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_array_l0_import_option(self):
        """
        target: import create l0_import option array validation
        method: submit create request with options.l0_import as an array
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_array_l0_import")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_array_l0_import"]],
                "options": {"auto_commit": "false", "l0_import": ["true"]},
            },
            ("l0_import", "array", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_object_l0_import_option(self):
        """
        target: import create l0_import option object validation
        method: submit create request with options.l0_import as an object
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_object_l0_import")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_object_l0_import"]],
                "options": {"auto_commit": "false", "l0_import": {"enabled": "true"}},
            },
            ("l0_import", "object", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_array_skip_disk_quota_check_option(self):
        """
        target: import create skip_disk_quota_check option array validation
        method: submit create request with options.skip_disk_quota_check as an array
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_array_skip_quota")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_array_skip_quota.parquet"]],
                "options": {"auto_commit": "false", "skip_disk_quota_check": ["true"]},
            },
            ("skip_disk_quota_check", "array", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_object_skip_disk_quota_check_option(self):
        """
        target: import create skip_disk_quota_check option object validation
        method: submit create request with options.skip_disk_quota_check as an object
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_object_skip_quota")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_object_skip_quota.parquet"]],
                "options": {"auto_commit": "false", "skip_disk_quota_check": {"enabled": "true"}},
            },
            ("skip_disk_quota_check", "object", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_numeric_storage_version_option(self):
        """
        target: import create storage_version option value type validation
        method: submit create request with options.storage_version as a number
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_numeric_storage_version")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "partitionName": "_default",
                "files": [["import_2pc_numeric_storage_version"]],
                "options": {"auto_commit": "false", "backup": "true", "storage_version": 2},
            },
            ("storage_version", "number", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_array_start_ts_option(self):
        """
        target: import create start_ts option value type validation
        method: submit create request with options.start_ts as an array
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_array_start_ts")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "partitionName": "_default",
                "files": [["import_2pc_array_start_ts"]],
                "options": {"auto_commit": "false", "backup": "true", "start_ts": ["123"]},
            },
            ("start_ts", "array", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_object_end_ts_option(self):
        """
        target: import create end_ts option value type validation
        method: submit create request with options.end_ts as an object
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_object_end_ts")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "partitionName": "_default",
                "files": [["import_2pc_object_end_ts"]],
                "options": {"auto_commit": "false", "backup": "true", "end_ts": {"value": "123"}},
            },
            ("end_ts", "object", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_boolean_sep_option(self):
        """
        target: import create CSV sep option value type validation
        method: submit create request with options.sep as a boolean
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_bool_sep")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_bool_sep.csv"]],
                "options": {"auto_commit": "false", "sep": True},
            },
            ("sep", "bool", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_array_nullkey_option(self):
        """
        target: import create CSV nullkey option value type validation
        method: submit create request with options.nullkey as an array
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_array_nullkey")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_array_nullkey.csv"]],
                "options": {"auto_commit": "false", "nullkey": ["NULL"]},
            },
            ("nullkey", "array", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_object_ezk_option(self):
        """
        target: import create ezk option value type validation
        method: submit create request with options.ezk as an object
        expected: request is rejected synchronously with a readable option/type error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_object_ezk")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "partitionName": "_default",
                "files": [["import_2pc_object_ezk"]],
                "options": {"auto_commit": "false", "backup": "true", "ezk": {"key": "abc"}},
            },
            ("ezk", "object", "string", "type", "unmarshal"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_invalid_timeout_option(self):
        """
        target: import create timeout option validation
        method: submit a syntactically valid import request with an invalid Go duration timeout option
        expected: create is rejected synchronously without creating a job or visible rows
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_timeout")
        self._create_base_collection(collection_name)

        rows = self._make_rows(9340000, 2, phase=9340)
        file_name = f"import_2pc_bad_timeout_{uuid4()}.parquet"
        file_path = self._write_parquet_and_upload(rows, file_name)

        payload = {
            "collectionName": collection_name,
            "files": [[file_name]],
            "options": {"auto_commit": "false", "timeout": "not-a-duration"},
        }
        create_rsp = self._assert_import_create_rejected(payload, ("timeout", "duration", "parse"))

        list_rsp = self.import_job_client.list_import_jobs({"collectionName": collection_name})
        assert list_rsp["code"] == 0, list_rsp
        assert list_rsp.get("data", {}).get("records", []) == [], {"create_rsp": create_rsp, "list_rsp": list_rsp}

        self.collection_client.refresh_load(collection_name)
        assert self._query_count(collection_name) == 0

        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_file_count_over_limit(self):
        """
        target: import create max file count validation
        method: submit 1025 file groups while maxImportFileNumPerReq defaults to 1024
        expected: request is rejected synchronously without creating a job
        """
        collection_name = gen_collection_name(prefix="import_2pc_file_count_limit")
        self._create_base_collection(collection_name)

        payload = {
            "collectionName": collection_name,
            "files": [[f"import_2pc_file_count_limit_{index:04d}.parquet"] for index in range(1025)],
            "options": {"auto_commit": "false"},
        }
        create_rsp = self.import_job_client.create_import_jobs(payload)
        if create_rsp["code"] != 0:
            readable_reason = str(create_rsp.get("message", create_rsp.get("reason", create_rsp))).lower()
            assert readable_reason, create_rsp
            assert any(keyword in readable_reason for keyword in ("file", "1024", "max", "limit", "too many")), (
                create_rsp
            )
            assert "jobId" not in str(create_rsp), create_rsp
            count, count_ok = self._wait_count(collection_name, 0, timeout=20)
            assert count_ok, {"expected_count": 0, "last_count": count, "create_rsp": create_rsp}
            return

        job_id = create_rsp["data"]["jobId"]
        progress_before_cleanup = self.import_job_client.get_import_job_progress(job_id)
        abort_rsp = self.import_job_client.abort_import_job(job_id)
        progress_after_cleanup = None
        if abort_rsp["code"] == 0:
            progress_after_cleanup, _ = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=60)
        else:
            progress_after_cleanup = self.import_job_client.get_import_job_progress(job_id)
        assert False, {
            "expected_sync_rejection": True,
            "actual_create_rsp": create_rsp,
            "progress_before_cleanup": progress_before_cleanup,
            "abort_rsp": abort_rsp,
            "progress_after_cleanup": progress_after_cleanup,
        }

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_job_id_string_is_canonical_and_numeric_behavior_is_explicit(self):
        """
        target: import REST jobId JSON type contract
        method: use string jobId as canonical path and probe numeric JSON jobId compatibility
        expected: string jobId works; numeric jobId is either accepted consistently or rejected with a readable error
        """
        collection_name = gen_collection_name(prefix="import_2pc_rest_jobid")
        self._create_base_collection(collection_name)

        rows = self._make_rows(7600, 4, phase=76)
        file_name = f"import_2pc_rest_jobid_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        string_describe_rsp = self.import_job_client.describe_import_job(job_id)
        assert string_describe_rsp["code"] == 0, string_describe_rsp
        assert string_describe_rsp["data"]["jobId"] == job_id
        assert string_describe_rsp["data"]["state"] == "Uncommitted"

        numeric_job_id = int(job_id)
        numeric_describe_rsp = self.import_job_client.describe_import_job(numeric_job_id)
        if numeric_describe_rsp["code"] == 0:
            assert str(numeric_describe_rsp["data"]["jobId"]) == job_id, numeric_describe_rsp
            assert numeric_describe_rsp["data"]["state"] == "Uncommitted", numeric_describe_rsp
        else:
            assert str(numeric_describe_rsp.get("message", numeric_describe_rsp)), numeric_describe_rsp

        numeric_commit_rsp = self.import_job_client.commit_import_job(numeric_job_id)
        if numeric_commit_rsp["code"] != 0:
            assert str(numeric_commit_rsp.get("message", numeric_commit_rsp)), numeric_commit_rsp
            string_commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert string_commit_rsp["code"] == 0, string_commit_rsp

        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        expected_ids = {row["id"] for row in rows}
        seen_ids, visible = self._wait_imported_ids_visible(collection_name, expected_ids, timeout=120)
        assert visible, {"expected": expected_ids, "seen": seen_ids}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_job_endpoints_reject_missing_job_id(self):
        """
        target: import REST jobId required field validation
        method: call describe/get_progress/commit/abort without jobId
        expected: each endpoint rejects the request synchronously with a readable error
        """
        endpoint_calls = {
            "describe": lambda: self.import_job_client.describe_import_job(None),
            "get_progress": lambda: self.import_job_client.get_import_job_progress(None),
            "commit": lambda: self.import_job_client.commit_import_job(None),
            "abort": lambda: self.import_job_client.abort_import_job(None),
        }

        for endpoint_name, call_endpoint in endpoint_calls.items():
            rsp = call_endpoint()
            assert rsp["code"] != 0, {"endpoint": endpoint_name, "rsp": rsp}
            readable_reason = str(rsp.get("message", rsp.get("reason", rsp))).strip()
            assert readable_reason, {"endpoint": endpoint_name, "rsp": rsp}
            assert "jobId" not in str(rsp.get("data", {})), {"endpoint": endpoint_name, "rsp": rsp}

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_describe_progress_is_monotonic_until_uncommitted(self):
        """
        target: import job describe observability
        method: poll /describe while a manual import job moves toward Uncommitted
        expected: observed state/progress do not regress and final describe reports Uncommitted
        """
        collection_name = gen_collection_name(prefix="import_2pc_describe_progress")
        self._create_base_collection(collection_name)

        rows = self._make_rows(7700, 128, phase=77)
        file_name = f"import_2pc_describe_progress_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        state_order = {
            "Pending": 0,
            "PreImporting": 1,
            "Importing": 2,
            "Sorting": 3,
            "IndexBuilding": 4,
            "Uncommitted": 5,
        }
        observed = []
        t0 = time.time()
        last_rsp = None
        while time.time() - t0 < IMPORT_2PC_TIMEOUT:
            last_rsp = self.import_job_client.describe_import_job(job_id)
            assert last_rsp["code"] == 0, last_rsp
            data = last_rsp["data"]
            assert data["jobId"] == job_id, last_rsp
            assert data["state"] in state_order, last_rsp
            assert 0 <= data["progress"] <= 100, last_rsp
            observed.append((data["state"], data["progress"], data.get("importedRows"), data.get("totalRows")))
            if data["state"] == "Uncommitted":
                break
            time.sleep(1)

        assert last_rsp is not None, job_id
        assert last_rsp["data"]["state"] == "Uncommitted", {"last_rsp": last_rsp, "observed": observed}
        assert last_rsp["data"]["importedRows"] == len(rows), last_rsp
        assert last_rsp["data"]["totalRows"] == len(rows), last_rsp

        for previous, current in zip(observed, observed[1:]):
            previous_state, previous_progress = previous[0], previous[1]
            current_state, current_progress = current[0], current[1]
            assert state_order[current_state] >= state_order[previous_state], observed
            assert current_progress >= previous_progress, observed

        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, {row["id"] for row in rows}, duration=6
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit, "observed": observed}

        abort_rsp = self.import_job_client.abort_import_job(job_id)
        assert abort_rsp["code"] == 0, abort_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_describe_matches_get_progress_for_uncommitted_job(self):
        """
        target: REST import describe/get_progress parity
        method: create a manual import job, wait for Uncommitted, then compare both endpoint responses
        expected: describe and get_progress report matching job identity, state, progress, and row counters
        """
        collection_name = gen_collection_name(prefix="import_2pc_describe_parity")
        self._create_base_collection(collection_name)

        rows = self._make_rows(7710, 4, phase=771)
        file_name = f"import_2pc_describe_parity_{uuid4()}.parquet"
        file_path = self._write_parquet_and_upload(rows, file_name)
        job_id = None

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            progress_rsp, ok = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ok, progress_rsp

            describe_rsp = self.import_job_client.describe_import_job(job_id)
            assert describe_rsp["code"] == 0, describe_rsp
            progress_rsp = self.import_job_client.get_import_job_progress(job_id)
            assert progress_rsp["code"] == 0, progress_rsp

            describe_data = describe_rsp["data"]
            progress_data = progress_rsp["data"]
            assert describe_data["collectionName"] == collection_name, describe_rsp
            for field_name in ("jobId", "state", "progress", "importedRows", "totalRows"):
                assert describe_data[field_name] == progress_data[field_name], {
                    "field": field_name,
                    "describe_rsp": describe_rsp,
                    "progress_rsp": progress_rsp,
                }
            assert describe_data["jobId"] == job_id, describe_rsp
            assert describe_data["state"] == "Uncommitted", describe_rsp
            assert 0 <= describe_data["progress"] <= 100, describe_rsp
            assert describe_data["importedRows"] == len(rows), describe_rsp
            assert describe_data["totalRows"] == len(rows), describe_rsp

            imported_ids = {row["id"] for row in rows}
            seen_ids, absent = self._wait_imported_ids_absent(collection_name, imported_ids, duration=6)
            assert absent, {"unexpected_visible_ids": seen_ids}

            abort_rsp = self.import_job_client.abort_import_job(job_id)
            assert abort_rsp["code"] == 0, abort_rsp
            rsp, failed = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
            assert failed, rsp
        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                if progress_rsp["code"] == 0 and progress_rsp["data"]["state"] == "Uncommitted":
                    self.import_job_client.abort_import_job(job_id)
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_completed_job_observability_fields_are_reported(self):
        """
        target: completed import job REST observability
        method: complete a manual import job and inspect /describe plus /list records
        expected: /describe reports detailed counters and timestamps; /list reports the completed job summary
        """
        collection_name = gen_collection_name(prefix="import_2pc_obs_completed")
        self._create_base_collection(collection_name)

        rows = self._make_rows(9380000, 6, phase=938)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_obs_completed_{uuid4()}.parquet"
        file_path = self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        describe_rsp = self.import_job_client.describe_import_job(job_id)
        assert describe_rsp["code"] == 0, describe_rsp
        describe_data = describe_rsp["data"]
        assert describe_data["jobId"] == job_id, describe_rsp
        assert describe_data["collectionName"] == collection_name, describe_rsp
        assert describe_data["state"] == "Completed", describe_rsp
        assert describe_data["progress"] == 100, describe_rsp
        assert describe_data["importedRows"] == len(rows), describe_rsp
        assert describe_data["totalRows"] == len(rows), describe_rsp
        assert describe_data.get("createTime"), describe_rsp
        assert describe_data.get("completeTime"), describe_rsp

        list_rsp = self.import_job_client.list_import_jobs({"collectionName": collection_name})
        assert list_rsp["code"] == 0, list_rsp
        records = list_rsp.get("data", {}).get("records", [])
        matching_records = [record for record in records if record.get("jobId") == job_id]
        assert len(matching_records) == 1, {"job_id": job_id, "list_rsp": list_rsp}
        list_record = matching_records[0]
        assert list_record["collectionName"] == collection_name, list_rsp
        assert list_record["state"] == "Completed", list_rsp
        assert list_record["progress"] == 100, list_rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {
            "expected_ids": sorted(expected_ids),
            "seen_ids": sorted(seen_after_commit),
            "describe_rsp": describe_rsp,
            "list_rsp": list_rsp,
        }

        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_completed_import_survives_release_and_reload(self):
        """
        target: completed import visibility after reload/target refresh
        method: commit a manual import, release and reload the collection, then query the imported PK set
        expected: imported rows remain fully visible after collection reload
        """
        collection_name = gen_collection_name(prefix="import_2pc_reload_after_commit")
        self._create_base_collection(collection_name)

        rows = self._make_rows(9390000, 8, phase=939)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_reload_after_commit_{uuid4()}.parquet"
        file_path = self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_before_reload, visible_before_reload = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_before_reload, {
            "expected_ids": sorted(expected_ids),
            "seen_ids": sorted(seen_before_reload),
            "progress": rsp,
        }

        release_rsp = self.collection_client.collection_release(collection_name=collection_name)
        assert release_rsp["code"] == 0, release_rsp

        load_state_rsp = None
        t0 = time.time()
        while time.time() - t0 < 60:
            load_state_rsp = self.collection_client.collection_load_state(collection_name=collection_name)
            assert load_state_rsp["code"] == 0, load_state_rsp
            if load_state_rsp.get("data", {}).get("loadState") == "LoadStateNotLoad":
                break
            time.sleep(1)
        assert load_state_rsp is not None, collection_name
        assert load_state_rsp.get("data", {}).get("loadState") == "LoadStateNotLoad", load_state_rsp

        load_rsp = self.collection_client.collection_load(collection_name=collection_name)
        assert load_rsp["code"] == 0, load_rsp
        self.wait_load_completed(collection_name, timeout=120)

        seen_after_reload, visible_after_reload = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_reload, {
            "expected_ids": sorted(expected_ids),
            "seen_ids": sorted(seen_after_reload),
            "progress": rsp,
            "load_state_rsp": load_state_rsp,
        }

        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_db_name_body_and_header_route_same_job_deterministically(self):
        """
        target: import REST database routing
        method: create same collection name in default and custom DB, import via body dbName, inspect via DB-Name header
        expected: body and header routes address the same custom-DB job and default DB remains isolated
        """
        db_name = f"imp2pc_db_{uuid4().hex[:8]}"
        create_db_rsp = self.database_client.database_create({"dbName": db_name})
        assert create_db_rsp["code"] == 0, create_db_rsp

        collection_name = gen_collection_name(prefix="import_2pc_db_route")
        self._create_base_collection(collection_name)
        self._create_base_collection(collection_name, db_name=db_name)

        rows = self._make_rows(7800, 4, phase=78)
        file_name = f"import_2pc_db_route_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        create_payload = {
            "collectionName": collection_name,
            "files": [[file_name]],
            "options": {"auto_commit": "false"},
        }
        create_rsp = self.import_job_client.create_import_jobs(create_payload, db_name=db_name)
        assert create_rsp["code"] == 0, create_rsp
        job_id = create_rsp["data"]["jobId"]

        rsp, ok = self.import_job_client.wait_import_job_state(
            job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT, db_name=db_name
        )
        assert ok, rsp
        assert rsp["data"]["collectionName"] == collection_name, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp

        describe_body_rsp = self.import_job_client.describe_import_job(job_id, db_name=db_name)
        assert describe_body_rsp["code"] == 0, describe_body_rsp
        assert describe_body_rsp["data"]["jobId"] == job_id
        assert describe_body_rsp["data"]["state"] == "Uncommitted"

        header = self.import_job_client.update_headers()
        header["DB-Name"] = db_name
        describe_url = f"{self.endpoint}/v2/vectordb/jobs/import/describe"
        describe_header_rsp = self.import_job_client.post(
            describe_url,
            headers=header,
            data={"jobId": job_id},
        ).json()
        assert describe_header_rsp["code"] == 0, describe_header_rsp
        assert describe_header_rsp["data"]["jobId"] == job_id
        assert describe_header_rsp["data"]["state"] == describe_body_rsp["data"]["state"]
        assert describe_header_rsp["data"]["collectionName"] == collection_name

        commit_url = f"{self.endpoint}/v2/vectordb/jobs/import/commit"
        commit_rsp = self.import_job_client.post(
            commit_url,
            headers=header,
            data={"jobId": job_id},
        ).json()
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(
            job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT, db_name=db_name
        )
        assert ok, rsp

        target_count, target_count_ok = self._wait_count(collection_name, len(rows), timeout=120, db_name=db_name)
        assert target_count_ok, {"expected": len(rows), "last_count": target_count}
        default_count, default_count_ok = self._wait_count(collection_name, 0, timeout=20)
        assert default_count_ok, {"expected_default_count": 0, "last_count": default_count}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.xfail(
        reason="milvus-io/milvus#50458: REST import commit/abort currently bypass PrivilegeImport authorization",
        strict=True,
    )
    def test_import_2pc_user_without_import_privilege_cannot_operate_jobs(self):
        """
        target: import REST RBAC guardrails
        method: call import create/progress/list/commit/abort with a user that has no import privilege
        expected: unauthorized user cannot create, inspect, commit, or abort import jobs and cannot see job IDs in list
        """
        collection_name = gen_collection_name(prefix="import_2pc_rbac")
        self._create_base_collection(collection_name)

        create_rows = self._make_rows(9320000, 4, phase=9320)
        commit_rows = self._make_rows(9321000, 4, phase=9321)
        abort_rows = self._make_rows(9322000, 4, phase=9322)
        create_file = f"import_2pc_rbac_create_{uuid4()}.parquet"
        commit_file = f"import_2pc_rbac_commit_{uuid4()}.parquet"
        abort_file = f"import_2pc_rbac_abort_{uuid4()}.parquet"
        local_files = [
            self._write_parquet_and_upload(create_rows, create_file),
            self._write_parquet_and_upload(commit_rows, commit_file),
            self._write_parquet_and_upload(abort_rows, abort_file),
        ]

        user_name = f"imp2pc_user_{uuid4().hex[:8]}"
        password = "Import2pc123"
        unauthorized_create_job_id = None
        commit_job_id = None
        abort_job_id = None

        try:
            create_user_rsp = self.user_client.user_create({"userName": user_name, "password": password})
            assert create_user_rsp["code"] == 0, create_user_rsp

            unauthorized_import_client = self.import_job_client.__class__(self.endpoint, f"{user_name}:{password}")

            commit_job_id = self._create_manual_import_job(collection_name, commit_file)
            rsp, ok = self.import_job_client.wait_import_job_state(
                commit_job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ok, rsp

            abort_job_id = self._create_manual_import_job(collection_name, abort_file)
            rsp, ok = self.import_job_client.wait_import_job_state(
                abort_job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ok, rsp

            create_rsp = unauthorized_import_client.create_import_jobs(
                {
                    "collectionName": collection_name,
                    "files": [[create_file]],
                    "options": {"auto_commit": "false"},
                }
            )
            if create_rsp.get("code") == 0:
                unauthorized_create_job_id = create_rsp["data"]["jobId"]

            progress_rsp = unauthorized_import_client.get_import_job_progress(commit_job_id)
            describe_rsp = unauthorized_import_client.describe_import_job(commit_job_id)
            list_rsp = unauthorized_import_client.list_import_jobs({"collectionName": collection_name})
            commit_rsp = unauthorized_import_client.commit_import_job(commit_job_id)
            abort_rsp = unauthorized_import_client.abort_import_job(abort_job_id)

            list_records = list_rsp.get("data", {}).get("records", []) if list_rsp.get("code") == 0 else []
            listed_job_ids = {record.get("jobId") for record in list_records}
            observed = {
                "create": create_rsp,
                "progress": progress_rsp,
                "describe": describe_rsp,
                "list": list_rsp,
                "commit": commit_rsp,
                "abort": abort_rsp,
                "listed_job_ids": sorted(job_id for job_id in listed_job_ids if job_id is not None),
            }

            assert create_rsp.get("code") != 0, observed
            assert "jobId" not in create_rsp.get("data", {}), observed
            assert progress_rsp.get("code") != 0, observed
            assert describe_rsp.get("code") != 0, observed
            assert commit_job_id not in listed_job_ids, observed
            assert abort_job_id not in listed_job_ids, observed
            assert commit_rsp.get("code") != 0, observed
            assert abort_rsp.get("code") != 0, observed

            commit_progress_rsp = self.import_job_client.get_import_job_progress(commit_job_id)
            abort_progress_rsp = self.import_job_client.get_import_job_progress(abort_job_id)
            assert commit_progress_rsp["data"]["state"] == "Uncommitted", {
                "observed": observed,
                "progress": commit_progress_rsp,
            }
            assert abort_progress_rsp["data"]["state"] == "Uncommitted", {
                "observed": observed,
                "progress": abort_progress_rsp,
            }
        finally:
            for job_id in (unauthorized_create_job_id, commit_job_id, abort_job_id):
                if job_id is None:
                    continue
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                if progress_rsp.get("code") != 0:
                    continue
                state = progress_rsp.get("data", {}).get("state")
                if state == "Uncommitted":
                    self.import_job_client.abort_import_job(job_id)
            self.user_client.user_drop({"userName": user_name})
            for file_path in local_files:
                if os.path.exists(file_path):
                    os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_same_collection_name_two_dbs_isolates_jobs_and_data(self):
        """
        target: import job and data isolation across databases
        method: create the same collection name in default and a custom DB, import different PK sets into both
        expected: each DB sees only its own import job result and same collection names do not leak data across DBs
        """
        db_name = f"imp2pc_iso_{uuid4().hex[:8]}"
        create_db_rsp = self.database_client.database_create({"dbName": db_name})
        assert create_db_rsp["code"] == 0, create_db_rsp

        collection_name = gen_collection_name(prefix="import_2pc_db_isolation")
        self._create_base_collection(collection_name)
        self._create_base_collection(collection_name, db_name=db_name)

        default_rows = self._make_rows(91800, 4, phase=918)
        custom_rows = self._make_rows(91900, 5, phase=919)
        default_file = f"import_2pc_db_isolation_default_{uuid4()}.parquet"
        custom_file = f"import_2pc_db_isolation_custom_{uuid4()}.parquet"
        local_files = [default_file, custom_file]

        try:
            self._write_parquet_and_upload(default_rows, default_file)
            self._write_parquet_and_upload(custom_rows, custom_file)

            default_job_id = self._create_manual_import_job(collection_name, default_file)
            custom_job_id = self._create_manual_import_job(collection_name, custom_file, db_name=db_name)

            default_rsp, default_ready = self.import_job_client.wait_import_job_state(
                default_job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert default_ready, default_rsp
            assert default_rsp["data"]["importedRows"] == len(default_rows), default_rsp

            custom_rsp, custom_ready = self.import_job_client.wait_import_job_state(
                custom_job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT, db_name=db_name
            )
            assert custom_ready, custom_rsp
            assert custom_rsp["data"]["importedRows"] == len(custom_rows), custom_rsp

            default_ids = {row["id"] for row in default_rows}
            custom_ids = {row["id"] for row in custom_rows}

            seen_default_before_commit, default_absent = self._wait_imported_ids_absent(
                collection_name, default_ids, duration=6
            )
            assert default_absent, {"unexpected_default_visible": seen_default_before_commit}
            seen_custom_before_commit, custom_absent = self._wait_imported_ids_absent(
                collection_name, custom_ids, duration=6, db_name=db_name
            )
            assert custom_absent, {"unexpected_custom_visible": seen_custom_before_commit}

            custom_commit_rsp = self.import_job_client.commit_import_job(custom_job_id, db_name=db_name)
            assert custom_commit_rsp["code"] == 0, custom_commit_rsp
            custom_rsp, custom_completed = self.import_job_client.wait_import_job_state(
                custom_job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT, db_name=db_name
            )
            assert custom_completed, custom_rsp

            seen_custom_after_commit, custom_visible = self._wait_imported_ids_visible(
                collection_name, custom_ids, timeout=120, db_name=db_name
            )
            assert custom_visible, {"expected": custom_ids, "seen": seen_custom_after_commit}

            default_count, default_count_ok = self._wait_count(collection_name, 0, timeout=20)
            assert default_count_ok, {"expected_default_count": 0, "last_count": default_count}
            assert self._query_imported_ids(collection_name, sorted(custom_ids)) == set()
            assert self._query_imported_ids(collection_name, sorted(default_ids), db_name=db_name) == set()

            default_commit_rsp = self.import_job_client.commit_import_job(default_job_id)
            assert default_commit_rsp["code"] == 0, default_commit_rsp
            default_rsp, default_completed = self.import_job_client.wait_import_job_state(
                default_job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert default_completed, default_rsp

            seen_default_after_commit, default_visible = self._wait_imported_ids_visible(
                collection_name, default_ids, timeout=120
            )
            assert default_visible, {"expected": default_ids, "seen": seen_default_after_commit}

            default_count, default_count_ok = self._wait_count(collection_name, len(default_rows), timeout=60)
            assert default_count_ok, {"expected_default_count": len(default_rows), "last_count": default_count}
            custom_count, custom_count_ok = self._wait_count(
                collection_name, len(custom_rows), timeout=60, db_name=db_name
            )
            assert custom_count_ok, {"expected_custom_count": len(custom_rows), "last_count": custom_count}
            assert self._query_imported_ids(collection_name, sorted(custom_ids)) == set()
            assert self._query_imported_ids(collection_name, sorted(default_ids), db_name=db_name) == set()
        finally:
            for file_name in local_files:
                file_path = f"/tmp/{file_name}"
                if os.path.exists(file_path):
                    os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_consistency_levels_respect_commit_visibility(self):
        """
        target: import commit_timestamp visibility under different collection consistency levels
        method: run manual import on Strong, Bounded, and Eventually collections
        expected: import rows stay invisible at Uncommitted and become eventually visible after explicit commit
        """
        scenarios = [
            ("Strong", 91400, 914),
            ("Bounded", 91420, 915),
            ("Eventually", 91440, 916),
        ]
        local_files = []
        try:
            for consistency_level, start_id, phase in scenarios:
                collection_name = gen_collection_name(prefix=f"import_2pc_mvcc_{consistency_level.lower()}")
                self._create_base_collection(collection_name, consistency_level=consistency_level)

                rows = self._make_rows(start_id, 4, phase=phase)
                file_name = f"import_2pc_mvcc_{consistency_level.lower()}_{uuid4()}.parquet"
                local_files.append(file_name)
                self._write_parquet_and_upload(rows, file_name)

                job_id = self._create_manual_import_job(collection_name, file_name)
                rsp, ok = self.import_job_client.wait_import_job_state(
                    job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
                )
                assert ok, {"consistency_level": consistency_level, "last_rsp": rsp}
                assert rsp["data"]["importedRows"] == len(rows), rsp
                assert rsp["data"]["totalRows"] == len(rows), rsp

                expected_ids = {row["id"] for row in rows}
                seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                    collection_name, expected_ids, duration=6
                )
                assert absent_before_commit, {
                    "consistency_level": consistency_level,
                    "unexpected_visible_ids": seen_before_commit,
                }

                commit_rsp = self.import_job_client.commit_import_job(job_id)
                assert commit_rsp["code"] == 0, {
                    "consistency_level": consistency_level,
                    "commit_rsp": commit_rsp,
                }
                rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
                assert ok, {"consistency_level": consistency_level, "last_rsp": rsp}

                seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                    collection_name, expected_ids, timeout=120
                )
                assert visible_after_commit, {
                    "consistency_level": consistency_level,
                    "expected": expected_ids,
                    "seen": seen_after_commit,
                }
        finally:
            for file_name in local_files:
                file_path = f"/tmp/{file_name}"
                if os.path.exists(file_path):
                    os.remove(file_path)

    def test_import_2pc_default_auto_commit_completed_and_visible(self):
        """
        target: default import compatibility
        method: create import job without auto_commit option and verify it completes automatically
        expected: job reaches Completed and imported rows are eventually visible to query and search
        """
        collection_name = gen_collection_name(prefix="import_2pc_auto")
        self._create_base_collection(collection_name)

        rows = self._make_rows(3000, 6, phase=3)
        file_name = f"import_2pc_auto_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        seen_ids, visible = self._wait_imported_ids_visible(collection_name, expected_ids, timeout=120)
        assert visible, {"expected": expected_ids, "seen": seen_ids}

        search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
        assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_create_rejects_invalid_auto_commit_option(self):
        """
        target: auto_commit option contract
        method: create import job with auto_commit set to a value other than true or false
        expected: request is rejected synchronously with a readable auto_commit validation error and no jobId
        """
        collection_name = gen_collection_name(prefix="import_2pc_auto_invalid")
        self._create_base_collection(collection_name)

        self._assert_import_create_rejected(
            {
                "collectionName": collection_name,
                "files": [["import_2pc_auto_invalid.parquet"]],
                "options": {"auto_commit": "not-a-bool"},
            },
            ("auto_commit", "true", "false"),
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_auto_commit_uppercase_false_stops_at_uncommitted(self):
        """
        target: auto_commit option contract
        method: create import job with auto_commit set to uppercase FALSE
        expected: option is treated as false, job stops at Uncommitted, rows stay invisible, and abort cleans it up
        """
        collection_name = gen_collection_name(prefix="import_2pc_auto_upper_false")
        self._create_base_collection(collection_name)

        rows = self._make_rows(3010, 4, phase=3010)
        file_name = f"import_2pc_auto_upper_false_{uuid4()}.parquet"
        file_path = self._write_parquet_and_upload(rows, file_name)
        job_id = None

        try:
            job_id = self._create_import_job(collection_name, file_name, options={"auto_commit": "FALSE"})
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["state"] == "Uncommitted", rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            seen_before_abort, absent_before_abort = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=8, interval=2
            )
            assert absent_before_abort, {"unexpected_visible_ids": seen_before_abort}

            abort_rsp = self.import_job_client.abort_import_job(job_id)
            assert abort_rsp["code"] == 0, abort_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_abort, absent_after_abort = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=8, interval=2
            )
            assert absent_after_abort, {"unexpected_visible_ids": seen_after_abort}
        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                if progress_rsp["code"] == 0 and progress_rsp["data"]["state"] == "Uncommitted":
                    self.import_job_client.abort_import_job(job_id)
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_empty_parquet_auto_commit_completes_with_zero_rows(self):
        """
        target: empty import file lifecycle
        method: import a parquet file with the collection schema and zero rows using default auto_commit
        expected: job reaches Completed with zero rows and the collection remains empty
        """
        collection_name = gen_collection_name(prefix="import_2pc_empty")
        self._create_base_collection(collection_name)

        file_name = f"import_2pc_empty_{uuid4()}.parquet"
        self._write_parquet_and_upload([], file_name)

        job_id = self._create_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == 0, rsp
        assert rsp["data"]["totalRows"] == 0, rsp

        self.collection_client.refresh_load(collection_name)
        assert self._query_count(collection_name) == 0
        assert self._query_count(collection_name, "id >= 0") == 0

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_nonexistent_numeric_job_commit_and_abort_rejected(self):
        """
        target: nonexistent import job handling
        method: call commit and abort with a syntactically valid numeric job id that does not exist
        expected: both APIs return non-zero business errors with a not-found reason
        """
        nonexistent_job_id = "922337203685477001"

        commit_rsp = self.import_job_client.commit_import_job(nonexistent_job_id)
        assert commit_rsp["code"] != 0, commit_rsp
        assert "not found" in str(commit_rsp).lower(), commit_rsp

        abort_rsp = self.import_job_client.abort_import_job(nonexistent_job_id)
        assert abort_rsp["code"] != 0, abort_rsp
        assert "not found" in str(abort_rsp).lower(), abort_rsp

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_consecutive_manual_imports_accumulate_rows(self):
        """
        target: consecutive import jobs in one collection
        method: commit two manual import jobs serially in the same collection
        expected: each job is isolated, previous rows stay visible, and final row count is cumulative
        """
        collection_name = gen_collection_name(prefix="import_2pc_multi")
        self._create_base_collection(collection_name)

        first_rows = self._make_rows(8400, 5, phase=84)
        second_rows = self._make_rows(8500, 7, phase=85)
        first_file = f"import_2pc_multi_first_{uuid4()}.parquet"
        second_file = f"import_2pc_multi_second_{uuid4()}.parquet"
        self._write_parquet_and_upload(first_rows, first_file)
        self._write_parquet_and_upload(second_rows, second_file)

        first_job_id = self._create_manual_import_job(collection_name, first_file)
        rsp, ok = self.import_job_client.wait_import_job_state(first_job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(first_rows), rsp
        first_ids = {row["id"] for row in first_rows}
        seen_first_before_commit, first_absent = self._wait_imported_ids_absent(collection_name, first_ids, duration=12)
        assert first_absent, {"unexpected_visible_ids": seen_first_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(first_job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(first_job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_first_ids, first_visible = self._wait_imported_ids_visible(collection_name, first_ids, timeout=120)
        assert first_visible, {"expected": first_ids, "seen": seen_first_ids}
        assert self._query_count(collection_name) == len(first_rows)

        second_job_id = self._create_manual_import_job(collection_name, second_file)
        rsp, ok = self.import_job_client.wait_import_job_state(second_job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(second_rows), rsp

        second_ids = {row["id"] for row in second_rows}
        seen_second_before_commit, second_absent = self._wait_imported_ids_absent(
            collection_name, second_ids, duration=12
        )
        assert second_absent, {"unexpected_visible_ids": seen_second_before_commit}

        seen_first_during_second = self._query_imported_ids(collection_name, sorted(first_ids))
        assert seen_first_during_second == first_ids, {
            "expected_first_ids": first_ids,
            "seen": seen_first_during_second,
        }

        commit_rsp = self.import_job_client.commit_import_job(second_job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(second_job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        all_ids = first_ids | second_ids
        seen_all_ids, all_visible = self._wait_imported_ids_visible(collection_name, all_ids, timeout=120)
        assert all_visible, {"expected": all_ids, "seen": seen_all_ids}
        assert self._query_count(collection_name) == len(all_ids)

        for file_path in [f"/tmp/{first_file}", f"/tmp/{second_file}"]:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_manual_multi_file_import_accumulates_rows(self):
        """
        target: multi-file import request
        method: create one manual import job with two parquet file batches, then commit it
        expected: total rows are accumulated, rows stay invisible before commit, and all rows are visible after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_multi_file")
        self._create_base_collection(collection_name)

        first_rows = self._make_rows(90500, 4, phase=905)
        second_rows = self._make_rows(90600, 5, phase=906)
        first_file = f"import_2pc_multi_file_first_{uuid4()}.parquet"
        second_file = f"import_2pc_multi_file_second_{uuid4()}.parquet"
        self._write_parquet_and_upload(first_rows, first_file)
        self._write_parquet_and_upload(second_rows, second_file)

        job_id = self._create_manual_import_job_with_files(collection_name, [first_file, second_file])
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        all_rows = first_rows + second_rows
        expected_ids = {row["id"] for row in all_rows}
        assert rsp["data"]["importedRows"] == len(all_rows), rsp
        assert rsp["data"]["totalRows"] == len(all_rows), rsp

        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

        final_count, final_count_ok = self._wait_count(collection_name, len(expected_ids), timeout=60)
        assert final_count_ok, {"expected_count": len(expected_ids), "last_count": final_count}

        search_first_ids = self._search_imported_ids(collection_name, first_rows[0]["vector"], limit=len(all_rows))
        assert search_first_ids & {row["id"] for row in first_rows}, {
            "expected_any": {row["id"] for row in first_rows},
            "search_ids": search_first_ids,
        }

        search_second_ids = self._search_imported_ids(collection_name, second_rows[0]["vector"], limit=len(all_rows))
        assert search_second_ids & {row["id"] for row in second_rows}, {
            "expected_any": {row["id"] for row in second_rows},
            "search_ids": search_second_ids,
        }

        for file_path in [f"/tmp/{first_file}", f"/tmp/{second_file}"]:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_json_file_manual_import_preserves_rows(self):
        """
        target: JSON file import with 2PC
        method: import one row-based JSON file with auto_commit=false, then commit it
        expected: rows stay invisible before commit and scalar/vector fields are queryable after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_json_file")
        self._create_base_collection(collection_name)

        rows = self._make_rows(91350, 6, phase=913)
        file_name = f"import_2pc_json_file_{uuid4()}.json"
        self._write_json_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        expected_tags = {row["tag"] for row in rows}
        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

        tag_rows, tags_visible = self._wait_tags_visible(collection_name, expected_tags, timeout=60)
        assert tags_visible, {"expected_tags": expected_tags, "seen_rows": tag_rows}
        assert len(tag_rows) == len(rows), tag_rows
        expected_by_id = {row["id"]: row for row in rows}
        for actual in tag_rows:
            expected = expected_by_id[actual["id"]]
            assert actual["tag"] == expected["tag"], actual
            assert actual["phase"] == expected["phase"], actual

        final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
        assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

        search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
        assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_jsonl_file_manual_import_preserves_rows(self):
        """
        target: JSONL file import with 2PC
        method: import one row-based JSONL file with auto_commit=false, then commit it
        expected: rows stay invisible before commit and scalar/vector fields are queryable after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_jsonl_file")
        self._create_base_collection(collection_name)

        rows = self._make_rows(92800, 6, phase=928)
        file_name = f"import_2pc_jsonl_file_{uuid4()}.jsonl"
        self._write_jsonl_and_upload(rows, file_name)

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            expected_tags = {row["tag"] for row in rows}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=120
            )
            assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

            tag_rows, tags_visible = self._wait_tags_visible(collection_name, expected_tags, timeout=60)
            assert tags_visible, {"expected_tags": expected_tags, "seen_rows": tag_rows}
            assert len(tag_rows) == len(rows), tag_rows
            expected_by_id = {row["id"]: row for row in rows}
            for actual in tag_rows:
                expected = expected_by_id[actual["id"]]
                assert actual["tag"] == expected["tag"], actual
                assert actual["phase"] == expected["phase"], actual

            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
            assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

            search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
            assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}
        finally:
            file_path = f"/tmp/{file_name}"
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_csv_file_with_custom_separator_manual_import_preserves_rows(self):
        """
        target: CSV file import with custom separator and 2PC
        method: import one pipe-separated CSV file with auto_commit=false, then commit it
        expected: rows stay invisible before commit and scalar/vector fields are queryable after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_csv_file")
        self._create_base_collection(collection_name)

        rows = self._make_rows(91450, 6, phase=914)
        file_name = f"import_2pc_csv_file_{uuid4()}.csv"
        self._write_csv_and_upload(rows, file_name, sep="|")

        job_id = self._create_import_job(
            collection_name,
            file_name,
            options={"auto_commit": "false", "sep": "|"},
        )
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        expected_tags = {row["tag"] for row in rows}
        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

        tag_rows, tags_visible = self._wait_tags_visible(collection_name, expected_tags, timeout=60)
        assert tags_visible, {"expected_tags": expected_tags, "seen_rows": tag_rows}
        assert len(tag_rows) == len(rows), tag_rows
        expected_by_id = {row["id"]: row for row in rows}
        for actual in tag_rows:
            expected = expected_by_id[actual["id"]]
            assert actual["tag"] == expected["tag"], actual
            assert actual["phase"] == expected["phase"], actual

        final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
        assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

        search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
        assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_csv_nullkey_manual_import_preserves_nulls(self):
        """
        target: CSV nullkey import with 2PC
        method: import nullable fields from a pipe-separated CSV file using NULL as nullkey
        expected: rows stay invisible before commit and nullable values are preserved after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_csv_nullkey")
        self._create_nullable_collection(collection_name)

        rows = self._make_nullable_rows(92900, 6)
        file_name = f"import_2pc_csv_nullkey_{uuid4()}.csv"
        self._write_nullable_csv_and_upload(rows, file_name, sep="|", nullkey="NULL")

        try:
            job_id = self._create_import_job(
                collection_name,
                file_name,
                options={"auto_commit": "false", "sep": "|", "nullkey": "NULL"},
            )
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=120
            )
            assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

            nullable_rows = self._query_nullable_rows_by_ids(collection_name, sorted(expected_ids))
            assert len(nullable_rows) == len(rows), nullable_rows
            expected_by_id = {row["id"]: row for row in rows}
            saw_null_int = False
            saw_non_null_int = False
            saw_null_float = False
            saw_non_null_float = False
            saw_null_varchar = False
            saw_non_null_varchar = False
            for actual in nullable_rows:
                expected = expected_by_id[actual["id"]]
                assert actual["required_field"] == expected["required_field"], actual
                assert actual.get("nullable_int") == expected["nullable_int"], actual
                assert actual.get("nullable_varchar") == expected["nullable_varchar"], actual
                if expected["nullable_float"] is None:
                    assert actual.get("nullable_float") is None, actual
                else:
                    assert abs(actual["nullable_float"] - expected["nullable_float"]) < 1e-6, actual

                saw_null_int = saw_null_int or expected["nullable_int"] is None
                saw_non_null_int = saw_non_null_int or expected["nullable_int"] is not None
                saw_null_float = saw_null_float or expected["nullable_float"] is None
                saw_non_null_float = saw_non_null_float or expected["nullable_float"] is not None
                saw_null_varchar = saw_null_varchar or expected["nullable_varchar"] is None
                saw_non_null_varchar = saw_non_null_varchar or expected["nullable_varchar"] is not None

            assert saw_null_int and saw_non_null_int, nullable_rows
            assert saw_null_float and saw_non_null_float, nullable_rows
            assert saw_null_varchar and saw_non_null_varchar, nullable_rows

            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
            assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

            search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
            assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}
        finally:
            file_path = f"/tmp/{file_name}"
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_numpy_field_per_file_manual_import_preserves_rows(self):
        """
        target: NumPy field-per-file import with 2PC
        method: import one .npy file per field in a single REST file group
        expected: rows stay invisible before commit and preserve scalar/vector data after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_numpy")
        self._create_base_collection(collection_name)

        rows = self._make_rows(93000, 6, phase=93)
        file_dir = f"import_2pc_numpy_{uuid4()}"
        file_names, local_dir = self._write_numpy_field_files_and_upload(rows, file_dir)

        try:
            job_id = self._create_import_job_with_file_groups(
                collection_name,
                [file_names],
                options={"auto_commit": "false"},
            )
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=120
            )
            assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

            actual_rows = self._query_base_rows_by_ids(collection_name, sorted(expected_ids))
            assert len(actual_rows) == len(rows), actual_rows
            expected_by_id = {row["id"]: row for row in rows}
            for actual in actual_rows:
                expected = expected_by_id[actual["id"]]
                assert actual["tag"] == expected["tag"], actual
                assert actual["phase"] == expected["phase"], actual

            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
            assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

            search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
            assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}
        finally:
            shutil.rmtree(local_dir, ignore_errors=True)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_invalid_file_suffix_fails_with_reason_and_no_visible_rows(self):
        """
        target: invalid import file suffix handling
        method: upload a .txt object and import it with auto_commit=false
        expected: job reaches Failed with readable reason and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_invalid_suffix")
        self._create_base_collection(collection_name)

        file_name = f"import_2pc_invalid_suffix_{uuid4()}.txt"
        file_path = self._write_text_and_upload(
            "id,tag,phase,vector\n1,invalid,1,[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]\n",
            file_name,
        )

        payload = {
            "collectionName": collection_name,
            "files": [[file_name]],
            "options": {"auto_commit": "false"},
        }
        create_rsp = self.import_job_client.create_import_jobs(payload)
        if create_rsp["code"] == 0:
            job_id = create_rsp["data"]["jobId"]
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["state"] == "Failed", rsp
            reason_text = str(rsp.get("data", {}).get("reason", rsp))
            error_context = rsp
        else:
            assert "jobId" not in str(create_rsp), create_rsp
            reason_text = str(create_rsp.get("message", create_rsp))
            error_context = create_rsp
        assert reason_text, error_context
        assert any(keyword in reason_text.lower() for keyword in ("txt", "format", "suffix", "unsupported")), (
            error_context
        )

        self.collection_client.refresh_load(collection_name)
        assert self._query_count(collection_name) == 0

        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_csv_invalid_separator_rejected_with_reason_and_no_visible_rows(self):
        """
        target: invalid CSV separator validation
        method: import a valid CSV file with an invalid newline separator option
        expected: create is rejected or job reaches Failed with readable reason, and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_invalid_sep")
        self._create_base_collection(collection_name)

        rows = self._make_rows(91650, 3, phase=916)
        file_name = f"import_2pc_invalid_sep_{uuid4()}.csv"
        file_path = self._write_csv_and_upload(rows, file_name, sep="|")

        payload = {
            "collectionName": collection_name,
            "files": [[file_name]],
            "options": {"auto_commit": "false", "sep": "\n"},
        }
        create_rsp = self.import_job_client.create_import_jobs(payload)
        if create_rsp["code"] == 0:
            job_id = create_rsp["data"]["jobId"]
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=120)
            assert ok, rsp
            assert rsp["data"]["state"] == "Failed", rsp
            reason_text = str(rsp.get("data", {}).get("reason", rsp))
            error_context = rsp
        else:
            assert "jobId" not in str(create_rsp), create_rsp
            reason_text = str(create_rsp.get("message", create_rsp))
            error_context = create_rsp
        assert reason_text, error_context
        assert any(
            keyword in reason_text.lower()
            for keyword in ("sep", "separator", "delimiter", "invalid", "line", "newline")
        ), error_context

        self.collection_client.refresh_load(collection_name)
        assert self._query_count(collection_name) == 0

        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_mixed_file_formats_across_file_groups_commits_all_rows(self):
        """
        target: mixed import file format support
        method: submit one manual import job containing parquet and csv files in separate file groups
        expected: the job reaches Uncommitted, stays invisible before commit, and all rows become visible after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_mixed_format")
        self._create_base_collection(collection_name)

        parquet_rows = self._make_rows(91750, 3, phase=917)
        csv_rows = self._make_rows(91850, 3, phase=918)
        parquet_file = f"import_2pc_mixed_format_{uuid4()}.parquet"
        csv_file = f"import_2pc_mixed_format_{uuid4()}.csv"
        parquet_path = self._write_parquet_and_upload(parquet_rows, parquet_file)
        csv_path = self._write_csv_and_upload(csv_rows, csv_file, sep="|")

        payload = {
            "collectionName": collection_name,
            "files": [[parquet_file], [csv_file]],
            "options": {"auto_commit": "false", "sep": "|"},
        }
        create_rsp = self.import_job_client.create_import_jobs(payload)
        assert create_rsp["code"] == 0, create_rsp
        job_id = create_rsp["data"]["jobId"]

        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        all_rows = parquet_rows + csv_rows
        expected_ids = {row["id"] for row in all_rows}
        assert rsp["data"]["importedRows"] == len(all_rows), rsp
        assert rsp["data"]["totalRows"] == len(all_rows), rsp

        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}
        assert self._query_count(collection_name) == len(all_rows)

        for file_path in [parquet_path, csv_path]:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_backup_invalid_storage_version_fails_with_reason_and_no_visible_rows(self):
        """
        target: backup import storage_version parse contract
        method: submit a backup import with a non-integer storage_version option
        expected: job reaches Failed with a storage_version parse reason and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_storage_version")
        self._create_base_collection(collection_name)

        object_prefix = f"import_2pc_bad_storage_version_{uuid4().hex}"
        fake_binlog_object = f"{object_prefix}/123456789/fake-binlog"
        file_path = self._write_text_object_and_upload("not a real binlog", fake_binlog_object)

        payload = {
            "collectionName": collection_name,
            "partitionName": "_default",
            "files": [[object_prefix]],
            "options": {"auto_commit": "false", "backup": "true", "storage_version": "abc"},
        }
        create_rsp = self.import_job_client.create_import_jobs(payload)
        if create_rsp["code"] == 0:
            job_id = create_rsp["data"]["jobId"]
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["state"] == "Failed", rsp
            reason_text = str(rsp.get("data", {}).get("reason", rsp))
            detail_text = " ".join(str(detail) for detail in rsp.get("data", {}).get("details", []))
            error_context = rsp
        else:
            assert "jobId" not in str(create_rsp), create_rsp
            reason_text = str(create_rsp.get("message", create_rsp))
            detail_text = ""
            error_context = create_rsp

        combined_reason = f"{reason_text} {detail_text}"
        assert combined_reason.strip(), error_context
        assert "storage_version" in combined_reason.lower(), error_context
        assert any(keyword in combined_reason.lower() for keyword in ("parse", "invalid", "syntax")), error_context

        self.collection_client.refresh_load(collection_name)
        assert self._query_count(collection_name) == 0

        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_backup_storage_version_supported_values_reach_reader_validation(self):
        """
        target: backup import storage_version supported string values
        method: submit backup imports with storage_version unset/0/1/2/3 against invalid backup objects
        expected: supported values are accepted by option parsing, then fail later with reader/input reasons and no rows
        """
        collection_name = gen_collection_name(prefix="import_2pc_storage_version_values")
        self._create_base_collection(collection_name)

        scenarios = [
            ("unset", None),
            ("0", "0"),
            ("1", "1"),
            ("2", "2"),
            ("3", "3"),
        ]
        local_files = []

        try:
            for label, storage_version in scenarios:
                object_prefix = f"import_2pc_storage_version_{label}_{uuid4().hex}"
                fake_binlog_object = f"{object_prefix}/123456789/fake-binlog"
                local_files.append(self._write_text_object_and_upload("not a real binlog", fake_binlog_object))

                options = {"auto_commit": "false", "backup": "true"}
                if storage_version is not None:
                    options["storage_version"] = storage_version
                payload = {
                    "collectionName": collection_name,
                    "partitionName": "_default",
                    "files": [[object_prefix]],
                    "options": options,
                }

                create_rsp = self.import_job_client.create_import_jobs(payload)
                if create_rsp["code"] == 0:
                    job_id = create_rsp["data"]["jobId"]
                    rsp, failed = self.import_job_client.wait_import_job_state(
                        job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT
                    )
                    assert failed, {"label": label, "create_rsp": create_rsp, "last_rsp": rsp}
                    reason_text = str(rsp.get("data", {}).get("reason", rsp))
                    detail_text = " ".join(str(detail) for detail in rsp.get("data", {}).get("details", []))
                    error_context = rsp
                else:
                    assert "jobId" not in str(create_rsp), {"label": label, "create_rsp": create_rsp}
                    reason_text = str(create_rsp.get("message", create_rsp))
                    detail_text = ""
                    error_context = create_rsp

                combined_reason = f"{reason_text} {detail_text}".strip()
                lower_reason = combined_reason.lower()
                assert combined_reason, {"label": label, "rsp": error_context}
                assert "parse storage_version" not in lower_reason, {"label": label, "rsp": error_context}
                assert "invalid storage_version" not in lower_reason, {"label": label, "rsp": error_context}

                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"label": label, "expected_count": 0, "last_count": count, "rsp": error_context}
        finally:
            for file_path in local_files:
                if os.path.exists(file_path):
                    os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_backup_reversed_time_range_fails_with_reason_and_no_visible_rows(self):
        """
        target: backup import start_ts/end_ts guardrail
        method: submit a backup import with start_ts greater than end_ts
        expected: job reaches Failed with a time range reason and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_time_range")
        self._create_base_collection(collection_name)

        object_prefix = f"import_2pc_bad_time_range_{uuid4().hex}"
        fake_binlog_object = f"{object_prefix}/123456789/fake-binlog"
        file_path = self._write_text_object_and_upload("not a real binlog", fake_binlog_object)

        end_ts = int(time.time() * 1000) << 18
        start_ts = end_ts + (1000 << 18)
        payload = {
            "collectionName": collection_name,
            "partitionName": "_default",
            "files": [[object_prefix]],
            "options": {
                "auto_commit": "false",
                "backup": "true",
                "start_ts": str(start_ts),
                "end_ts": str(end_ts),
            },
        }
        create_rsp = self.import_job_client.create_import_jobs(payload)
        if create_rsp["code"] == 0:
            job_id = create_rsp["data"]["jobId"]
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["state"] == "Failed", rsp
            reason_text = str(rsp.get("data", {}).get("reason", rsp))
            detail_text = " ".join(str(detail) for detail in rsp.get("data", {}).get("details", []))
            error_context = rsp
        else:
            assert "jobId" not in str(create_rsp), create_rsp
            reason_text = str(create_rsp.get("message", create_rsp))
            detail_text = ""
            error_context = create_rsp

        combined_reason = f"{reason_text} {detail_text}"
        assert combined_reason.strip(), error_context
        assert "start_ts" in combined_reason.lower() or "end_ts" in combined_reason.lower(), error_context
        assert any(keyword in combined_reason.lower() for keyword in ("larger", "range", "invalid")), error_context

        self.collection_client.refresh_load(collection_name)
        assert self._query_count(collection_name) == 0

        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_backup_camelcase_time_range_alias_fails_with_reason_and_no_visible_rows(self):
        """
        target: backup import startTs/endTs alias guardrail
        method: submit a backup import with camelCase startTs greater than endTs
        expected: job reaches Failed with a time range reason and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_time_alias")
        self._create_base_collection(collection_name)

        object_prefix = f"import_2pc_bad_time_alias_{uuid4().hex}"
        fake_binlog_object = f"{object_prefix}/123456789/fake-binlog"
        file_path = self._write_text_object_and_upload("not a real binlog", fake_binlog_object)

        end_ts = int(time.time() * 1000) << 18
        start_ts = end_ts + (1000 << 18)
        payload = {
            "collectionName": collection_name,
            "partitionName": "_default",
            "files": [[object_prefix]],
            "options": {
                "auto_commit": "false",
                "backup": "true",
                "startTs": str(start_ts),
                "endTs": str(end_ts),
            },
        }
        create_rsp = self.import_job_client.create_import_jobs(payload)
        if create_rsp["code"] == 0:
            job_id = create_rsp["data"]["jobId"]
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["state"] == "Failed", rsp
            reason_text = str(rsp.get("data", {}).get("reason", rsp))
            detail_text = " ".join(str(detail) for detail in rsp.get("data", {}).get("details", []))
            error_context = rsp
        else:
            assert "jobId" not in str(create_rsp), create_rsp
            reason_text = str(create_rsp.get("message", create_rsp))
            detail_text = ""
            error_context = create_rsp

        combined_reason = f"{reason_text} {detail_text}"
        assert combined_reason.strip(), error_context
        assert "start_ts" in combined_reason.lower() or "end_ts" in combined_reason.lower(), error_context
        assert any(keyword in combined_reason.lower() for keyword in ("larger", "range", "invalid")), error_context

        self.collection_client.refresh_load(collection_name)
        assert self._query_count(collection_name) == 0

        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_backup_invalid_start_ts_fails_with_reason_and_no_visible_rows(self):
        """
        target: backup import start_ts parse guardrail
        method: submit a backup import with a non-numeric start_ts option
        expected: job reaches Failed with a start_ts parse reason and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_start_ts")
        self._create_base_collection(collection_name)

        object_prefix = f"import_2pc_bad_start_ts_{uuid4().hex}"
        fake_binlog_object = f"{object_prefix}/123456789/fake-binlog"
        file_path = self._write_text_object_and_upload("not a real binlog", fake_binlog_object)

        end_ts = int(time.time() * 1000) << 18
        payload = {
            "collectionName": collection_name,
            "partitionName": "_default",
            "files": [[object_prefix]],
            "options": {
                "auto_commit": "false",
                "backup": "true",
                "start_ts": "not-a-hybrid-ts",
                "end_ts": str(end_ts),
            },
        }
        create_rsp = self.import_job_client.create_import_jobs(payload)
        if create_rsp["code"] == 0:
            job_id = create_rsp["data"]["jobId"]
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["state"] == "Failed", rsp
            reason_text = str(rsp.get("data", {}).get("reason", rsp))
            detail_text = " ".join(str(detail) for detail in rsp.get("data", {}).get("details", []))
            error_context = rsp
        else:
            assert "jobId" not in str(create_rsp), create_rsp
            reason_text = str(create_rsp.get("message", create_rsp))
            detail_text = ""
            error_context = create_rsp

        combined_reason = f"{reason_text} {detail_text}"
        assert combined_reason.strip(), error_context
        assert "start_ts" in combined_reason.lower(), error_context
        assert any(keyword in combined_reason.lower() for keyword in ("parse", "invalid", "syntax")), error_context

        self.collection_client.refresh_load(collection_name)
        assert self._query_count(collection_name) == 0

        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_backup_invalid_end_ts_fails_with_reason_and_no_visible_rows(self):
        """
        target: backup import end_ts parse guardrail
        method: submit a backup import with a non-numeric end_ts option
        expected: job reaches Failed with an end_ts parse reason and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_bad_end_ts")
        self._create_base_collection(collection_name)

        object_prefix = f"import_2pc_bad_end_ts_{uuid4().hex}"
        fake_binlog_object = f"{object_prefix}/123456789/fake-binlog"
        file_path = self._write_text_object_and_upload("not a real binlog", fake_binlog_object)

        start_ts = int(time.time() * 1000) << 18
        payload = {
            "collectionName": collection_name,
            "partitionName": "_default",
            "files": [[object_prefix]],
            "options": {
                "auto_commit": "false",
                "backup": "true",
                "start_ts": str(start_ts),
                "end_ts": "not-a-hybrid-ts",
            },
        }
        create_rsp = self.import_job_client.create_import_jobs(payload)
        if create_rsp["code"] == 0:
            job_id = create_rsp["data"]["jobId"]
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["state"] == "Failed", rsp
            reason_text = str(rsp.get("data", {}).get("reason", rsp))
            detail_text = " ".join(str(detail) for detail in rsp.get("data", {}).get("details", []))
            error_context = rsp
        else:
            assert "jobId" not in str(create_rsp), create_rsp
            reason_text = str(create_rsp.get("message", create_rsp))
            detail_text = ""
            error_context = create_rsp

        combined_reason = f"{reason_text} {detail_text}"
        assert combined_reason.strip(), error_context
        assert "end_ts" in combined_reason.lower(), error_context
        assert any(keyword in combined_reason.lower() for keyword in ("parse", "invalid", "syntax")), error_context

        self.collection_client.refresh_load(collection_name)
        assert self._query_count(collection_name) == 0

        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_backup_requires_partition_name_without_creating_job(self):
        """
        target: backup import partitionName contract
        method: submit backup=true without partitionName
        expected: create is rejected synchronously without creating a job or visible rows
        """
        collection_name = gen_collection_name(prefix="import_2pc_backup_no_partition")
        self._create_base_collection(collection_name)

        payload = {
            "collectionName": collection_name,
            "files": [[f"import_2pc_backup_no_partition_{uuid4().hex}"]],
            "options": {"auto_commit": "false", "backup": "true"},
        }
        create_rsp = self._assert_import_create_rejected(payload, ("partition", "specified"))

        list_rsp = self.import_job_client.list_import_jobs({"collectionName": collection_name})
        assert list_rsp["code"] == 0, list_rsp
        assert list_rsp.get("data", {}).get("records", []) == [], {"create_rsp": create_rsp, "list_rsp": list_rsp}

        self.collection_client.refresh_load(collection_name)
        assert self._query_count(collection_name) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_skip_disk_quota_check_is_ignored_for_regular_import(self):
        """
        target: skip_disk_quota_check regular import contract
        method: submit a normal parquet import with skip_disk_quota_check=true but without backup/L0 mode
        expected: import follows normal 2PC; data is invisible before commit and visible after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_skip_dqc_regular")
        self._create_base_collection(collection_name)

        rows = self._make_rows(9360000, 5, phase=936)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_skip_dqc_regular_{uuid4()}.parquet"
        file_path = self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_import_job(
            collection_name,
            file_name,
            options={"auto_commit": "false", "skip_disk_quota_check": "true"},
        )
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert self._query_count(collection_name) == 0

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {
            "expected_ids": sorted(expected_ids),
            "seen_ids": sorted(seen_after_commit),
            "progress": rsp,
        }

        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_l0_import_rejects_loaded_collection_without_creating_job(self):
        """
        target: L0 import loaded-collection guardrail
        method: submit l0_import=true against a loaded collection
        expected: create is rejected synchronously without creating a job or visible rows
        """
        collection_name = gen_collection_name(prefix="import_2pc_l0_loaded")
        self._create_base_collection(collection_name)

        payload = {
            "collectionName": collection_name,
            "files": [[f"import_2pc_l0_loaded_{uuid4().hex}"]],
            "options": {"auto_commit": "false", "l0_import": "true"},
        }
        create_rsp = self.import_job_client.create_import_jobs(payload)
        assert create_rsp["code"] != 0, create_rsp
        assert "jobId" not in str(create_rsp), create_rsp

        reason_text = str(create_rsp.get("message", create_rsp.get("reason", create_rsp))).lower()
        assert "l0" in reason_text, create_rsp
        assert "loaded" in reason_text or "release" in reason_text, create_rsp

        list_rsp = self.import_job_client.list_import_jobs({"collectionName": collection_name})
        assert list_rsp["code"] == 0, list_rsp
        assert list_rsp.get("data", {}).get("records", []) == [], {"create_rsp": create_rsp, "list_rsp": list_rsp}

        self.collection_client.refresh_load(collection_name)
        assert self._query_count(collection_name) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_multi_vchannel_manual_commit_eventually_visible(self):
        """
        target: multi-vchannel manual import commit
        method: create a three-shard collection, import enough rows to span vchannels, then commit it
        expected: rows stay invisible before commit and all imported rows become visible after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_multi_vchannel")
        self._create_base_collection(collection_name, shards_num=3)

        rows = self._make_rows(91550, 60, phase=915)
        file_name = f"import_2pc_multi_vchannel_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        describe_rsp = self.collection_client.collection_describe(collection_name)
        assert describe_rsp["code"] == 0, describe_rsp
        assert describe_rsp["data"]["shardsNum"] == 3, describe_rsp

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        expected_tags = {row["tag"] for row in rows}
        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=180
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

        tag_rows, tags_visible = self._wait_tags_visible(collection_name, expected_tags, timeout=120)
        assert tags_visible, {"expected_tags": expected_tags, "seen_rows": tag_rows}
        assert len(tag_rows) == len(rows), tag_rows

        final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=120)
        assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

        first_search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
        last_search_ids = self._search_imported_ids(collection_name, rows[-1]["vector"], limit=len(rows))
        assert first_search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": first_search_ids}
        assert last_search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": last_search_ids}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_ttl_starts_from_commit_timestamp(self):
        """
        target: TTL calculation for manually committed import rows
        method: keep imported rows Uncommitted past collection TTL, then commit and observe visibility window
        expected: rows are visible after commit and expire from commit timestamp instead of import row timestamp
        """
        collection_name = gen_collection_name(prefix="import_2pc_ttl_commit_ts")
        ttl_seconds = 30
        self._create_base_collection(collection_name, ttl_seconds=ttl_seconds)

        describe_rsp = self.collection_client.collection_describe(collection_name)
        assert describe_rsp["code"] == 0, describe_rsp
        ttl_property = None
        for prop in describe_rsp["data"].get("properties", []):
            if prop["key"] == "collection.ttl.seconds":
                ttl_property = int(prop["value"])
        assert ttl_property == ttl_seconds, describe_rsp

        rows = self._make_rows(91650, 4, phase=916)
        file_name = f"import_2pc_ttl_commit_ts_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        time.sleep(ttl_seconds + 5)
        seen_after_ttl_while_uncommitted, still_absent = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=6, interval=2
        )
        assert still_absent, {"unexpected_visible_ids": seen_after_ttl_while_uncommitted}

        commit_start = time.time()
        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=20
        )
        assert visible_after_commit, {
            "expected": expected_ids,
            "seen": seen_after_commit,
            "elapsed_since_commit": time.time() - commit_start,
        }

        seconds_until_midpoint = 15 - (time.time() - commit_start)
        if seconds_until_midpoint > 0:
            time.sleep(seconds_until_midpoint)
        visible_ids_at_midpoint = self._query_imported_ids(collection_name, sorted(expected_ids))
        assert visible_ids_at_midpoint == expected_ids, {
            "expected": expected_ids,
            "seen": visible_ids_at_midpoint,
            "elapsed_since_commit": time.time() - commit_start,
        }
        assert self._query_count(collection_name) == len(rows)

        seconds_until_expiry_probe = ttl_seconds + 5 - (time.time() - commit_start)
        if seconds_until_expiry_probe > 0:
            time.sleep(seconds_until_expiry_probe)
        seen_after_expiry, expired = self._wait_imported_ids_eventually_absent(
            collection_name, expected_ids, timeout=30, interval=2
        )
        assert expired, {
            "expected_absent": expected_ids,
            "last_seen": seen_after_expiry,
            "elapsed_since_commit": time.time() - commit_start,
        }

        final_count, final_count_ok = self._wait_count(collection_name, 0, timeout=20)
        assert final_count_ok, {"expected_count": 0, "last_count": final_count}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(
        reason=(
            "milvus-io/milvus#50464: manual MixCompaction may remain Executing after compacting "
            "committed import segments with normal segments"
        )
    )
    def test_import_2pc_manual_compaction_after_commit_preserves_rows(self):
        """
        target: manual compaction after committed import rows
        method: insert and flush baseline rows, commit a manual import job, then compact the collection
        expected: compaction completes and both inserted plus imported rows remain queryable/searchable
        """
        collection_name = gen_collection_name(prefix="import_2pc_compact_after_commit")
        dim = 128
        batch_size = 5000
        batch_count = 4
        self._create_base_collection(collection_name, dim=dim)

        baseline_rows = []
        for batch in range(batch_count):
            batch_rows = self._make_rows(9170000 + batch * batch_size, batch_size, phase=9170 + batch, dim=dim)
            baseline_rows.extend(batch_rows)
            self._insert_rows(collection_name, batch_rows)
            baseline_count, baseline_count_ok = self._wait_count(collection_name, len(baseline_rows), timeout=60)
            assert baseline_count_ok, {"expected_count": len(baseline_rows), "last_count": baseline_count}
            flush_rsp, flush_ok = self._flush_collection_with_retry(collection_name)
            assert flush_ok, flush_rsp
        baseline_ids = {row["id"] for row in baseline_rows}

        import_rows = self._make_rows(9195000, batch_size, phase=9180, dim=dim)
        file_name = f"import_2pc_compact_after_commit_{uuid4()}.parquet"
        self._write_parquet_and_upload(import_rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(import_rows), rsp
        assert rsp["data"]["totalRows"] == len(import_rows), rsp

        imported_ids = {row["id"] for row in import_rows}
        sample_baseline_ids = {row["id"] for row in baseline_rows[:5] + baseline_rows[-5:]}
        sample_imported_ids = {row["id"] for row in import_rows[:5] + import_rows[-5:]}
        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, sample_imported_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_import_ids": seen_before_commit}
        assert self._query_imported_ids(collection_name, sorted(sample_baseline_ids)) == sample_baseline_ids
        assert self._query_count(collection_name) == len(baseline_rows)

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        sample_all_ids = sample_baseline_ids | sample_imported_ids
        expected_total_rows = len(baseline_rows) + len(import_rows)
        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, sample_all_ids, timeout=120
        )
        assert visible_after_commit, {"expected": sample_all_ids, "seen": seen_after_commit}
        count_after_commit, count_after_commit_ok = self._wait_count(collection_name, expected_total_rows, timeout=120)
        assert count_after_commit_ok, {"expected_count": expected_total_rows, "last_count": count_after_commit}

        time.sleep(5)
        compact_rsp = self.collection_client.compact(collection_name)
        assert compact_rsp["code"] == 0, compact_rsp
        compaction_id = compact_rsp.get("data", {}).get("compactionID")
        if compaction_id == -1:
            seen_after_noop_compact, visible_after_noop_compact = self._wait_imported_ids_visible(
                collection_name, sample_all_ids, timeout=120
            )
            assert visible_after_noop_compact, {
                "expected": sample_all_ids,
                "seen": seen_after_noop_compact,
            }
            count_after_noop_compact, count_after_noop_compact_ok = self._wait_count(
                collection_name, expected_total_rows, timeout=120
            )
            assert count_after_noop_compact_ok, {
                "expected_count": expected_total_rows,
                "last_count": count_after_noop_compact,
            }
            pytest.skip("Manual compaction did not schedule a task on this instance: compactionID=-1")
        assert isinstance(compaction_id, int) and compaction_id > 0, compact_rsp
        compaction_rsp, compaction_completed = self._wait_compaction_completed(compaction_id, timeout=600)
        assert compaction_completed, compaction_rsp

        seen_after_compaction, visible_after_compaction = self._wait_imported_ids_visible(
            collection_name, sample_all_ids, timeout=120
        )
        assert visible_after_compaction, {"expected": sample_all_ids, "seen": seen_after_compaction}
        count_after_compaction, count_after_compaction_ok = self._wait_count(
            collection_name, expected_total_rows, timeout=120
        )
        assert count_after_compaction_ok, {
            "expected_count": expected_total_rows,
            "last_count": count_after_compaction,
        }

        imported_search_ids = self._search_imported_ids(collection_name, import_rows[0]["vector"], limit=20)
        baseline_search_ids = self._search_imported_ids(collection_name, baseline_rows[0]["vector"], limit=20)
        assert imported_search_ids & imported_ids, {
            "expected_any_imported_sample": sorted(sample_imported_ids),
            "search_ids": imported_search_ids,
        }
        assert baseline_search_ids & baseline_ids, {
            "expected_any_baseline_sample": sorted(sample_baseline_ids),
            "search_ids": baseline_search_ids,
        }

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_manual_compaction_during_uncommitted_keeps_import_invisible(self):
        """
        target: manual compaction while an import job is Uncommitted
        method: compact already-flushed baseline rows while a manual import job is waiting for commit
        expected: compaction does not expose Uncommitted import rows, and commit makes them visible later
        """
        collection_name = gen_collection_name(prefix="import_2pc_compact_uncommitted")
        batch_size = 2000
        batch_count = 2
        self._create_base_collection(collection_name)

        baseline_rows = []
        for batch in range(batch_count):
            batch_rows = self._make_rows(9280000 + batch * batch_size, batch_size, phase=9280 + batch)
            baseline_rows.extend(batch_rows)
            self._insert_rows(collection_name, batch_rows)
            baseline_count, baseline_count_ok = self._wait_count(collection_name, len(baseline_rows), timeout=60)
            assert baseline_count_ok, {"expected_count": len(baseline_rows), "last_count": baseline_count}
            flush_rsp, flush_ok = self._flush_collection_with_retry(collection_name)
            assert flush_ok, flush_rsp

        import_rows = self._make_rows(9290000, 256, phase=9290)
        file_name = f"import_2pc_compact_uncommitted_{uuid4()}.parquet"
        file_path = self._write_parquet_and_upload(import_rows, file_name)

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(import_rows), rsp
            assert rsp["data"]["totalRows"] == len(import_rows), rsp

            import_sample_ids = {row["id"] for row in import_rows[:8] + import_rows[-8:]}
            baseline_sample_ids = {row["id"] for row in baseline_rows[:8] + baseline_rows[-8:]}
            seen_before_compact, absent_before_compact = self._wait_imported_ids_absent(
                collection_name, import_sample_ids, duration=12
            )
            assert absent_before_compact, {"unexpected_visible_import_ids": seen_before_compact}
            assert self._query_imported_ids(collection_name, sorted(baseline_sample_ids)) == baseline_sample_ids
            assert self._query_count(collection_name) == len(baseline_rows)

            compact_rsp = self.collection_client.compact(collection_name)
            assert compact_rsp["code"] == 0, compact_rsp
            compaction_id = compact_rsp.get("data", {}).get("compactionID")
            assert isinstance(compaction_id, int), compact_rsp
            if compaction_id > 0:
                compaction_rsp, compaction_completed = self._wait_compaction_completed(compaction_id, timeout=360)
                assert compaction_completed, compaction_rsp
            else:
                assert compaction_id == -1, compact_rsp

            seen_after_compact, absent_after_compact = self._wait_imported_ids_absent(
                collection_name, import_sample_ids, duration=12
            )
            assert absent_after_compact, {
                "unexpected_visible_import_ids": seen_after_compact,
                "compaction_id": compaction_id,
            }
            assert self._query_imported_ids(collection_name, sorted(baseline_sample_ids)) == baseline_sample_ids
            baseline_count_after_compact, baseline_count_after_compact_ok = self._wait_count(
                collection_name, len(baseline_rows), timeout=60
            )
            assert baseline_count_after_compact_ok, {
                "expected_count": len(baseline_rows),
                "last_count": baseline_count_after_compact,
                "compaction_id": compaction_id,
            }

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            all_sample_ids = baseline_sample_ids | import_sample_ids
            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, all_sample_ids, timeout=120
            )
            assert visible_after_commit, {"expected": all_sample_ids, "seen": seen_after_commit}

            final_count, final_count_ok = self._wait_count(
                collection_name, len(baseline_rows) + len(import_rows), timeout=120
            )
            assert final_count_ok, {
                "expected_count": len(baseline_rows) + len(import_rows),
                "last_count": final_count,
            }

            imported_search_ids = self._search_imported_ids(collection_name, import_rows[0]["vector"], limit=20)
            assert imported_search_ids & import_sample_ids, {
                "expected_any_import_ids": import_sample_ids,
                "search_ids": imported_search_ids,
            }
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_aborted_import_does_not_pollute_later_committed_import(self):
        """
        target: aborted import cleanup visibility
        method: abort one Uncommitted import, then commit a second import in the same collection
        expected: aborted PKs never become visible and the later committed import has exact rows/count/search results
        """
        collection_name = gen_collection_name(prefix="import_2pc_abort_cleanup")
        self._create_base_collection(collection_name)

        baseline_rows = self._make_rows(9310000, 4, phase=9310)
        self._insert_rows(collection_name, baseline_rows)
        baseline_count, baseline_count_ok = self._wait_count(collection_name, len(baseline_rows), timeout=60)
        assert baseline_count_ok, {"expected_count": len(baseline_rows), "last_count": baseline_count}

        aborted_rows = self._make_rows(9311000, 16, phase=9311)
        committed_rows = self._make_rows(9312000, 12, phase=9312)
        aborted_file_name = f"import_2pc_abort_cleanup_aborted_{uuid4()}.parquet"
        committed_file_name = f"import_2pc_abort_cleanup_committed_{uuid4()}.parquet"
        aborted_file_path = self._write_parquet_and_upload(aborted_rows, aborted_file_name)
        committed_file_path = self._write_parquet_and_upload(committed_rows, committed_file_name)

        try:
            aborted_job_id = self._create_manual_import_job(collection_name, aborted_file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(
                aborted_job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(aborted_rows), rsp
            assert rsp["data"]["totalRows"] == len(aborted_rows), rsp

            aborted_ids = {row["id"] for row in aborted_rows}
            seen_before_abort, absent_before_abort = self._wait_imported_ids_absent(
                collection_name, aborted_ids, duration=12
            )
            assert absent_before_abort, {"unexpected_visible_aborted_ids": seen_before_abort}

            abort_rsp = self.import_job_client.abort_import_job(aborted_job_id)
            assert abort_rsp["code"] == 0, abort_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(aborted_job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_abort, absent_after_abort = self._wait_imported_ids_absent(
                collection_name, aborted_ids, duration=30
            )
            assert absent_after_abort, {"unexpected_visible_aborted_ids": seen_after_abort}
            assert self._query_count(collection_name) == len(baseline_rows)

            committed_job_id = self._create_manual_import_job(collection_name, committed_file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(
                committed_job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(committed_rows), rsp
            assert rsp["data"]["totalRows"] == len(committed_rows), rsp

            committed_ids = {row["id"] for row in committed_rows}
            seen_aborted_during_second_import, aborted_still_absent = self._wait_imported_ids_absent(
                collection_name, aborted_ids, duration=12
            )
            assert aborted_still_absent, {
                "unexpected_visible_aborted_ids": seen_aborted_during_second_import,
            }
            seen_committed_before_commit, committed_absent_before_commit = self._wait_imported_ids_absent(
                collection_name, committed_ids, duration=12
            )
            assert committed_absent_before_commit, {
                "unexpected_visible_committed_ids": seen_committed_before_commit,
            }

            commit_rsp = self.import_job_client.commit_import_job(committed_job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(
                committed_job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ok, rsp

            seen_committed_after_commit, committed_visible = self._wait_imported_ids_visible(
                collection_name, committed_ids, timeout=120
            )
            assert committed_visible, {
                "expected_committed_ids": committed_ids,
                "seen": seen_committed_after_commit,
            }
            seen_aborted_after_commit, aborted_absent_after_commit = self._wait_imported_ids_absent(
                collection_name, aborted_ids, duration=30
            )
            assert aborted_absent_after_commit, {
                "unexpected_visible_aborted_ids": seen_aborted_after_commit,
            }

            final_count, final_count_ok = self._wait_count(
                collection_name, len(baseline_rows) + len(committed_rows), timeout=120
            )
            assert final_count_ok, {
                "expected_count": len(baseline_rows) + len(committed_rows),
                "last_count": final_count,
            }

            progress_rsp = self.import_job_client.get_import_job_progress(aborted_job_id)
            assert progress_rsp["code"] == 0, progress_rsp
            assert progress_rsp["data"]["state"] == "Failed", progress_rsp

            search_ids = self._search_imported_ids(
                collection_name, committed_rows[0]["vector"], limit=len(committed_rows)
            )
            assert search_ids & committed_ids, {
                "expected_any_committed_ids": committed_ids,
                "search_ids": search_ids,
            }
            assert not search_ids & aborted_ids, {
                "unexpected_aborted_search_ids": search_ids & aborted_ids,
                "search_ids": search_ids,
            }
        finally:
            for file_path in (aborted_file_path, committed_file_path):
                if os.path.exists(file_path):
                    os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_hnsw_l2_ip_cosine_manual_import_searches_after_commit(self):
        """
        target: HNSW index import with L2/IP/COSINE metrics
        method: create one HNSW collection per metric and run manual 2PC parquet import
        expected: rows stay invisible before commit and search returns imported PKs after commit
        """
        local_files = []
        metric_types = ["L2", "IP", "COSINE"]

        try:
            for metric_index, metric_type in enumerate(metric_types):
                collection_name = gen_collection_name(prefix=f"import_2pc_hnsw_{metric_type.lower()}")
                self._create_hnsw_collection(collection_name, metric_type)

                rows = self._make_rows(94000 + metric_index * 100, 6, phase=940 + metric_index)
                file_name = f"import_2pc_hnsw_{metric_type.lower()}_{uuid4()}.parquet"
                file_path = self._write_parquet_and_upload(rows, file_name)
                local_files.append(file_path)

                job_id = self._create_manual_import_job(collection_name, file_name)
                rsp, ok = self.import_job_client.wait_import_job_state(
                    job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
                )
                assert ok, {"metric_type": metric_type, "response": rsp}
                assert rsp["data"]["importedRows"] == len(rows), rsp
                assert rsp["data"]["totalRows"] == len(rows), rsp

                expected_ids = {row["id"] for row in rows}
                seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                    collection_name, expected_ids, duration=12
                )
                assert absent_before_commit, {
                    "metric_type": metric_type,
                    "unexpected_visible_ids": seen_before_commit,
                }

                commit_rsp = self.import_job_client.commit_import_job(job_id)
                assert commit_rsp["code"] == 0, {"metric_type": metric_type, "response": commit_rsp}
                rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
                assert ok, {"metric_type": metric_type, "response": rsp}

                seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                    collection_name, expected_ids, timeout=120
                )
                assert visible_after_commit, {
                    "metric_type": metric_type,
                    "expected": expected_ids,
                    "seen": seen_after_commit,
                }

                actual_rows = self._query_base_rows_by_ids(collection_name, sorted(expected_ids))
                assert len(actual_rows) == len(rows), {"metric_type": metric_type, "rows": actual_rows}
                expected_by_id = {row["id"]: row for row in rows}
                for actual in actual_rows:
                    expected = expected_by_id[actual["id"]]
                    assert actual["tag"] == expected["tag"], {"metric_type": metric_type, "row": actual}
                    assert actual["phase"] == expected["phase"], {"metric_type": metric_type, "row": actual}

                final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
                assert final_count_ok, {
                    "metric_type": metric_type,
                    "expected_count": len(rows),
                    "last_count": final_count,
                }

                search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
                assert search_ids & expected_ids, {
                    "metric_type": metric_type,
                    "expected_any": expected_ids,
                    "search_ids": search_ids,
                }
        finally:
            for file_path in local_files:
                if os.path.exists(file_path):
                    os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_ivf_flat_sq8_pq_manual_import_searches_after_commit(self):
        """
        target: IVF index import with IVF_FLAT/IVF_SQ8/IVF_PQ
        method: create one IVF collection per index type and run manual 2PC parquet import
        expected: rows stay invisible before commit and search returns imported PKs after commit
        """
        local_files = []
        index_cases = [
            ("IVF_FLAT", {"nlist": 1}),
            ("IVF_SQ8", {"nlist": 1}),
            ("IVF_PQ", {"nlist": 1, "m": 2, "nbits": 4}),
        ]

        try:
            for index_index, (index_type, index_params) in enumerate(index_cases):
                collection_name = gen_collection_name(prefix=f"import_2pc_{index_type.lower()}")
                self._create_ivf_collection(collection_name, index_type, index_params)

                rows = self._make_rows(95000 + index_index * 1000, 32, phase=950 + index_index)
                file_name = f"import_2pc_{index_type.lower()}_{uuid4()}.parquet"
                file_path = self._write_parquet_and_upload(rows, file_name)
                local_files.append(file_path)

                job_id = self._create_manual_import_job(collection_name, file_name)
                rsp, ok = self.import_job_client.wait_import_job_state(
                    job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
                )
                assert ok, {"index_type": index_type, "response": rsp}
                assert rsp["data"]["importedRows"] == len(rows), rsp
                assert rsp["data"]["totalRows"] == len(rows), rsp

                expected_ids = {row["id"] for row in rows}
                seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                    collection_name, expected_ids, duration=12
                )
                assert absent_before_commit, {
                    "index_type": index_type,
                    "unexpected_visible_ids": seen_before_commit,
                }

                commit_rsp = self.import_job_client.commit_import_job(job_id)
                assert commit_rsp["code"] == 0, {"index_type": index_type, "response": commit_rsp}
                rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
                assert ok, {"index_type": index_type, "response": rsp}

                seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                    collection_name, expected_ids, timeout=120
                )
                assert visible_after_commit, {
                    "index_type": index_type,
                    "expected": expected_ids,
                    "seen": seen_after_commit,
                }

                actual_rows = self._query_base_rows_by_ids(collection_name, sorted(expected_ids))
                assert len(actual_rows) == len(rows), {"index_type": index_type, "rows": actual_rows}
                expected_by_id = {row["id"]: row for row in rows}
                for actual in actual_rows:
                    expected = expected_by_id[actual["id"]]
                    assert actual["tag"] == expected["tag"], {"index_type": index_type, "row": actual}
                    assert actual["phase"] == expected["phase"], {"index_type": index_type, "row": actual}

                final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
                assert final_count_ok, {
                    "index_type": index_type,
                    "expected_count": len(rows),
                    "last_count": final_count,
                }

                search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=10)
                assert search_ids & expected_ids, {
                    "index_type": index_type,
                    "expected_any": expected_ids,
                    "search_ids": search_ids,
                }
        finally:
            for file_path in local_files:
                if os.path.exists(file_path):
                    os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_diskann_manual_import_searches_after_commit(self):
        """
        target: DISKANN index import with manual 2PC
        method: create a DISKANN collection and run manual 2PC parquet import
        expected: rows stay invisible before commit and DISKANN search returns imported PKs after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_diskann")
        self._create_diskann_collection(collection_name)

        rows = self._make_rows(97000, 256, phase=970, dim=16)
        file_name = f"import_2pc_diskann_{uuid4()}.parquet"
        file_path = self._write_parquet_and_upload(rows, file_name)

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            sampled_ids = {row["id"] for row in rows[:8] + rows[-8:]}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, sampled_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, sampled_ids, timeout=120
            )
            assert visible_after_commit, {
                "expected": sampled_ids,
                "seen": seen_after_commit,
            }

            actual_rows = self._query_base_rows_by_ids(collection_name, sorted(sampled_ids))
            assert len(actual_rows) == len(sampled_ids), {"rows": actual_rows}
            expected_by_id = {row["id"]: row for row in rows}
            for actual in actual_rows:
                expected = expected_by_id[actual["id"]]
                assert actual["tag"] == expected["tag"], {"row": actual}
                assert actual["phase"] == expected["phase"], {"row": actual}

            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=120)
            assert final_count_ok, {
                "expected_count": len(rows),
                "last_count": final_count,
            }

            search_ids = self._search_diskann_imported_ids(collection_name, rows[0]["vector"], limit=20)
            assert search_ids & expected_ids, {
                "expected_any": sorted(sampled_ids),
                "search_ids": search_ids,
            }
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_flat_scann_manual_import_searches_after_commit(self):
        """
        target: FLAT and SCANN index import
        method: create one collection per index type and run manual 2PC parquet import
        expected: rows stay invisible before commit and search returns imported PKs after commit
        """
        local_files = []
        index_cases = [
            ("FLAT", {}),
            ("SCANN", {"nlist": 1}),
        ]

        try:
            for index_index, (index_type, index_params) in enumerate(index_cases):
                collection_name = gen_collection_name(prefix=f"import_2pc_{index_type.lower()}")
                self._create_ivf_collection(collection_name, index_type, index_params)

                rows = self._make_rows(98000 + index_index * 1000, 32, phase=980 + index_index)
                file_name = f"import_2pc_{index_type.lower()}_{uuid4()}.parquet"
                file_path = self._write_parquet_and_upload(rows, file_name)
                local_files.append(file_path)

                job_id = self._create_manual_import_job(collection_name, file_name)
                rsp, ok = self.import_job_client.wait_import_job_state(
                    job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
                )
                assert ok, {"index_type": index_type, "response": rsp}
                assert rsp["data"]["importedRows"] == len(rows), rsp
                assert rsp["data"]["totalRows"] == len(rows), rsp

                expected_ids = {row["id"] for row in rows}
                seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                    collection_name, expected_ids, duration=12
                )
                assert absent_before_commit, {
                    "index_type": index_type,
                    "unexpected_visible_ids": seen_before_commit,
                }

                commit_rsp = self.import_job_client.commit_import_job(job_id)
                assert commit_rsp["code"] == 0, {"index_type": index_type, "response": commit_rsp}
                rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
                assert ok, {"index_type": index_type, "response": rsp}

                seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                    collection_name, expected_ids, timeout=120
                )
                assert visible_after_commit, {
                    "index_type": index_type,
                    "expected": expected_ids,
                    "seen": seen_after_commit,
                }

                actual_rows = self._query_base_rows_by_ids(collection_name, sorted(expected_ids))
                assert len(actual_rows) == len(rows), {"index_type": index_type, "rows": actual_rows}
                expected_by_id = {row["id"]: row for row in rows}
                for actual in actual_rows:
                    expected = expected_by_id[actual["id"]]
                    assert actual["tag"] == expected["tag"], {"index_type": index_type, "row": actual}
                    assert actual["phase"] == expected["phase"], {"index_type": index_type, "row": actual}

                final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
                assert final_count_ok, {
                    "index_type": index_type,
                    "expected_count": len(rows),
                    "last_count": final_count,
                }

                search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=10)
                assert search_ids & expected_ids, {
                    "index_type": index_type,
                    "expected_any": expected_ids,
                    "search_ids": search_ids,
                }
        finally:
            for file_path in local_files:
                if os.path.exists(file_path):
                    os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_binary_flat_ivf_manual_import_searches_after_commit(self):
        """
        target: binary vector index import with BIN_FLAT/BIN_IVF_FLAT
        method: create one BinaryVector collection per index type and run manual 2PC parquet import
        expected: rows stay invisible before commit and binary vector search returns imported PKs after commit
        """
        local_files = []
        index_cases = [
            ("BIN_FLAT", {}),
            ("BIN_IVF_FLAT", {"nlist": 1}),
        ]

        try:
            for index_index, (index_type, index_params) in enumerate(index_cases):
                collection_name = gen_collection_name(prefix=f"import_2pc_{index_type.lower()}")
                self._create_binary_collection(collection_name, index_type, index_params)

                rows = self._make_binary_rows(99000 + index_index * 1000, 32, phase=990 + index_index)
                file_name = f"import_2pc_{index_type.lower()}_{uuid4()}.parquet"
                file_path = self._write_binary_parquet_and_upload(rows, file_name)
                local_files.append(file_path)

                job_id = self._create_manual_import_job(collection_name, file_name)
                rsp, ok = self.import_job_client.wait_import_job_state(
                    job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
                )
                assert ok, {"index_type": index_type, "response": rsp}
                assert rsp["data"]["importedRows"] == len(rows), rsp
                assert rsp["data"]["totalRows"] == len(rows), rsp

                expected_ids = {row["id"] for row in rows}
                seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                    collection_name, expected_ids, duration=12
                )
                assert absent_before_commit, {
                    "index_type": index_type,
                    "unexpected_visible_ids": seen_before_commit,
                }

                commit_rsp = self.import_job_client.commit_import_job(job_id)
                assert commit_rsp["code"] == 0, {"index_type": index_type, "response": commit_rsp}
                rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
                assert ok, {"index_type": index_type, "response": rsp}

                seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                    collection_name, expected_ids, timeout=120
                )
                assert visible_after_commit, {
                    "index_type": index_type,
                    "expected": expected_ids,
                    "seen": seen_after_commit,
                }

                actual_rows = self._query_base_rows_by_ids(collection_name, sorted(expected_ids))
                assert len(actual_rows) == len(rows), {"index_type": index_type, "rows": actual_rows}
                expected_by_id = {row["id"]: row for row in rows}
                for actual in actual_rows:
                    expected = expected_by_id[actual["id"]]
                    assert actual["tag"] == expected["tag"], {"index_type": index_type, "row": actual}
                    assert actual["phase"] == expected["phase"], {"index_type": index_type, "row": actual}

                final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
                assert final_count_ok, {
                    "index_type": index_type,
                    "expected_count": len(rows),
                    "last_count": final_count,
                }

                search_ids = self._search_binary_imported_ids(collection_name, rows[0]["binary_vector"], limit=10)
                assert search_ids & expected_ids, {
                    "index_type": index_type,
                    "expected_any": expected_ids,
                    "search_ids": search_ids,
                }
        finally:
            for file_path in local_files:
                if os.path.exists(file_path):
                    os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_sparse_inverted_wand_manual_import_searches_after_commit(self):
        """
        target: sparse vector index import with SPARSE_INVERTED_INDEX/SPARSE_WAND
        method: create one SparseFloatVector collection per index type and run manual 2PC parquet import
        expected: rows stay invisible before commit and sparse vector search returns imported PKs after commit
        """
        local_files = []
        index_cases = [
            ("SPARSE_INVERTED_INDEX", {"drop_ratio_build": "0.2"}),
            ("SPARSE_WAND", {"drop_ratio_build": "0.2"}),
        ]

        try:
            for index_index, (index_type, index_params) in enumerate(index_cases):
                collection_name = gen_collection_name(prefix=f"import_2pc_{index_type.lower()}")
                self._create_sparse_collection(collection_name, index_type, index_params)

                rows = self._make_sparse_rows(100000 + index_index * 1000, 32, phase=1000 + index_index)
                file_name = f"import_2pc_{index_type.lower()}_{uuid4()}.parquet"
                file_path = self._write_sparse_parquet_and_upload(rows, file_name)
                local_files.append(file_path)

                job_id = self._create_manual_import_job(collection_name, file_name)
                rsp, ok = self.import_job_client.wait_import_job_state(
                    job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
                )
                assert ok, {"index_type": index_type, "response": rsp}
                assert rsp["data"]["importedRows"] == len(rows), rsp
                assert rsp["data"]["totalRows"] == len(rows), rsp

                expected_ids = {row["id"] for row in rows}
                seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                    collection_name, expected_ids, duration=12
                )
                assert absent_before_commit, {
                    "index_type": index_type,
                    "unexpected_visible_ids": seen_before_commit,
                }

                commit_rsp = self.import_job_client.commit_import_job(job_id)
                assert commit_rsp["code"] == 0, {"index_type": index_type, "response": commit_rsp}
                rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
                assert ok, {"index_type": index_type, "response": rsp}

                seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                    collection_name, expected_ids, timeout=120
                )
                assert visible_after_commit, {
                    "index_type": index_type,
                    "expected": expected_ids,
                    "seen": seen_after_commit,
                }

                actual_rows = self._query_base_rows_by_ids(collection_name, sorted(expected_ids))
                assert len(actual_rows) == len(rows), {"index_type": index_type, "rows": actual_rows}
                expected_by_id = {row["id"]: row for row in rows}
                for actual in actual_rows:
                    expected = expected_by_id[actual["id"]]
                    assert actual["tag"] == expected["tag"], {"index_type": index_type, "row": actual}
                    assert actual["phase"] == expected["phase"], {"index_type": index_type, "row": actual}

                final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
                assert final_count_ok, {
                    "index_type": index_type,
                    "expected_count": len(rows),
                    "last_count": final_count,
                }

                search_ids = self._search_sparse_imported_ids(collection_name, rows[0]["sparse_vector"], limit=10)
                assert search_ids & expected_ids, {
                    "index_type": index_type,
                    "expected_any": expected_ids,
                    "search_ids": search_ids,
                }
        finally:
            for file_path in local_files:
                if os.path.exists(file_path):
                    os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_sparse_struct_parquet_manual_import_searches_after_commit(self):
        """
        target: SparseFloatVector parquet struct encoding with manual 2PC
        method: import a parquet file whose sparse vector column is struct<indices:list<uint32>, values:list<float32>>
        expected: rows stay invisible before commit and sparse vector search returns imported PKs after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_sparse_struct")
        self._create_sparse_collection(collection_name, "SPARSE_INVERTED_INDEX", {"drop_ratio_build": "0.2"})

        rows = self._make_sparse_rows(103000, 24, phase=1030)
        file_name = f"import_2pc_sparse_struct_{uuid4()}.parquet"
        file_path = self._write_sparse_struct_parquet_and_upload(rows, file_name)

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=120
            )
            assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

            actual_rows = self._query_base_rows_by_ids(collection_name, sorted(expected_ids))
            assert len(actual_rows) == len(rows), actual_rows
            expected_by_id = {row["id"]: row for row in rows}
            for actual in actual_rows:
                expected = expected_by_id[actual["id"]]
                assert actual["tag"] == expected["tag"], actual
                assert actual["phase"] == expected["phase"], actual

            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
            assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

            search_ids = self._search_sparse_imported_ids(collection_name, rows[0]["sparse_vector"], limit=10)
            assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_bm25_function_input_only_manual_import_searches_after_commit(self):
        """
        target: BM25 function output generation with manual 2PC import
        method: import JSON rows that include only BM25 input text and dense vector fields
        expected: rows stay invisible before commit and BM25 search returns imported PKs after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_bm25_fn")
        self._create_bm25_function_collection(collection_name)

        rows = self._make_bm25_function_rows(101000, 18)
        file_name = f"import_2pc_bm25_fn_{uuid4()}.json"
        file_path = self._write_json_and_upload(rows, file_name)

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=120
            )
            assert visible_after_commit, {
                "expected": expected_ids,
                "seen": seen_after_commit,
            }

            payload = {
                "collectionName": collection_name,
                "filter": f"id in {sorted(expected_ids)}",
                "outputFields": ["id", "document_content"],
                "limit": len(rows),
            }
            query_rsp = self.vector_client.vector_query(payload, timeout=1)
            assert query_rsp["code"] == 0, query_rsp
            actual_by_id = {row["id"]: row for row in query_rsp.get("data", [])}
            assert set(actual_by_id.keys()) == expected_ids, query_rsp
            expected_by_id = {row["id"]: row for row in rows}
            for pk, expected in expected_by_id.items():
                assert actual_by_id[pk]["document_content"] == expected["document_content"]

            alpha_ids = {
                row["id"] for row in rows if "import two phase commit alpha document" in row["document_content"]
            }
            search_ids, search_ok = self._wait_bm25_search_hits(
                collection_name,
                "import two phase commit alpha document",
                alpha_ids,
                timeout=120,
                limit=8,
            )
            assert search_ok, {
                "expected_any": alpha_ids,
                "search_ids": search_ids,
            }
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_bm25_function_output_field_rejected_and_no_visible_rows(self):
        """
        target: BM25 function output field guardrail with manual 2PC import
        method: import JSON rows that include the generated BM25 SparseFloatVector output field
        expected: request is rejected or job fails with an actionable reason and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_bm25_output")
        self._create_bm25_function_collection(collection_name)

        rows = self._make_bm25_function_rows(102000, 8)
        invalid_rows = []
        for i, row in enumerate(rows):
            invalid_row = dict(row)
            invalid_row["sparse_vector"] = {
                "indices": [1, 10 + i, 100 + i],
                "values": [0.5, 0.25, 0.125],
            }
            invalid_rows.append(invalid_row)

        file_name = f"import_2pc_fn_invalid_{uuid4()}.json"
        file_path = self._write_json_and_upload(invalid_rows, file_name)
        expected_reason_parts = ("function output", "bm25", "unexpected field", "sparse_vector")

        try:
            payload = {
                "collectionName": collection_name,
                "files": [[file_name]],
                "options": {"auto_commit": "false"},
            }
            create_rsp = self.import_job_client.create_import_jobs(payload)

            if create_rsp["code"] != 0:
                readable_reason = str(create_rsp).lower()
                assert any(part in readable_reason for part in expected_reason_parts), create_rsp
            else:
                job_id = create_rsp["data"]["jobId"]
                progress_rsp, failed = self.import_job_client.wait_import_job_state(
                    job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT
                )
                assert failed, progress_rsp
                reason = str(progress_rsp.get("data", {}).get("reason", progress_rsp)).lower()
                assert any(part in reason for part in expected_reason_parts), progress_rsp

            expected_ids = {row["id"] for row in rows}
            count_after_rejection, count_ok = self._wait_count(collection_name, 0, timeout=60)
            assert count_ok, {"expected_count": 0, "last_count": count_after_rejection}
            seen_after_rejection = self._query_imported_ids(collection_name, sorted(expected_ids))
            assert not seen_after_rejection, {"unexpected_visible_ids": seen_after_rejection}
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_scalar_indexes_manual_import_filters_after_commit(self):
        """
        target: scalar indexes with manual 2PC import
        method: create INVERTED/BITMAP/STL_SORT/TRIE scalar indexes and run manual 2PC parquet import
        expected: rows stay invisible before commit and indexed scalar filters return expected rows after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_scalar_idx")
        self._create_scalar_index_collection(collection_name)

        rows = self._make_scalar_index_rows(101000, 32)
        file_name = f"import_2pc_scalar_idx_{uuid4()}.parquet"
        file_path = self._write_scalar_index_parquet_and_upload(rows, file_name)

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=120
            )
            assert visible_after_commit, {
                "expected": expected_ids,
                "seen": seen_after_commit,
            }

            filter_cases = [
                ("phase == 3", {row["id"] for row in rows if row["phase"] == 3}, "INVERTED"),
                ("flag == true", {row["id"] for row in rows if row["flag"]}, "BITMAP"),
                (
                    "score > 20.0 and score < 24.0",
                    {row["id"] for row in rows if 20.0 < row["score"] < 24.0},
                    "STL_SORT",
                ),
                (
                    'tag like "scalar_tag_1_%"',
                    {row["id"] for row in rows if row["tag"].startswith("scalar_tag_1_")},
                    "TRIE",
                ),
            ]
            for filter_expr, expected_filter_ids, index_type in filter_cases:
                actual_rows = self._query_scalar_index_rows(collection_name, filter_expr, limit=len(rows))
                actual_ids = {row["id"] for row in actual_rows}
                assert actual_ids == expected_filter_ids, {
                    "index_type": index_type,
                    "filter": filter_expr,
                    "expected": expected_filter_ids,
                    "actual": actual_rows,
                }

            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
            assert final_count_ok, {
                "expected_count": len(rows),
                "last_count": final_count,
            }
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_half_vector_types_manual_import_searches_after_commit(self):
        """
        target: Float16Vector and BFloat16Vector import with 2PC
        method: import half-vector parquet with auto_commit=false and search both vector fields after commit
        expected: rows stay invisible before commit and half-vector searches return imported PKs after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_half_vec")
        self._create_half_vector_collection(collection_name)

        rows = self._make_half_vector_rows(102000, 16, phase=1020)
        file_name = f"import_2pc_half_vec_{uuid4()}.parquet"
        file_path = self._write_half_vector_parquet_and_upload(rows, file_name)

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=120
            )
            assert visible_after_commit, {
                "expected": expected_ids,
                "seen": seen_after_commit,
            }

            actual_rows = self._query_base_rows_by_ids(collection_name, sorted(expected_ids))
            assert len(actual_rows) == len(rows), actual_rows
            expected_by_id = {row["id"]: row for row in rows}
            for actual in actual_rows:
                expected = expected_by_id[actual["id"]]
                assert actual["tag"] == expected["tag"], actual
                assert actual["phase"] == expected["phase"], actual

            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
            assert final_count_ok, {
                "expected_count": len(rows),
                "last_count": final_count,
            }

            for anns_field in ("float16_vector", "bfloat16_vector"):
                search_ids = self._search_half_vector_imported_ids(
                    collection_name, anns_field, rows[0][anns_field], limit=10
                )
                assert search_ids & expected_ids, {
                    "anns_field": anns_field,
                    "expected_any": expected_ids,
                    "search_ids": search_ids,
                }
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_int8_vector_manual_import_searches_after_commit(self):
        """
        target: Int8Vector import with 2PC
        method: import Int8Vector parquet with auto_commit=false and search after explicit commit
        expected: rows stay invisible before commit and Int8Vector search returns imported PKs after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_int8_vec")
        self._create_int8_vector_collection(collection_name)

        rows = self._make_int8_vector_rows(103000, 16, phase=1030)
        file_name = f"import_2pc_int8_vec_{uuid4()}.parquet"
        file_path = self._write_int8_vector_parquet_and_upload(rows, file_name)

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=120
            )
            assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

            actual_rows = self._query_base_rows_by_ids(collection_name, sorted(expected_ids))
            assert len(actual_rows) == len(rows), actual_rows
            expected_by_id = {row["id"]: row for row in rows}
            for actual in actual_rows:
                expected = expected_by_id[actual["id"]]
                assert actual["tag"] == expected["tag"], actual
                assert actual["phase"] == expected["phase"], actual

            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
            assert final_count_ok, {
                "expected_count": len(rows),
                "last_count": final_count,
            }

            search_ids = self._search_int8_vector_imported_ids(collection_name, rows[0]["int8_vector"], limit=10)
            assert search_ids & expected_ids, {
                "expected_any": expected_ids,
                "search_ids": search_ids,
            }
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_array_of_vector_manual_import_searches_after_commit(self):
        """
        target: ArrayOfVector import with 2PC
        method: import StructArray parquet with ArrayOfVector sub-field and search after explicit commit
        expected: rows stay invisible before commit and ArrayOfVector search returns imported PKs after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_array_vec")
        self._create_array_of_vector_collection(collection_name)

        rows = self._make_array_of_vector_rows(104000, 12, phase=1040)
        file_name = f"import_2pc_array_vec_{uuid4()}.parquet"
        file_path = self._write_array_of_vector_parquet_and_upload(rows, file_name)

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=120
            )
            assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

            actual_rows = self._query_array_of_vector_rows_by_ids(collection_name, sorted(expected_ids))
            assert len(actual_rows) == len(rows), actual_rows
            expected_by_id = {row["id"]: row for row in rows}
            for actual in actual_rows:
                expected = expected_by_id[actual["id"]]
                assert actual["tag"] == expected["tag"], actual
                assert actual["phase"] == expected["phase"], actual
                actual_struct = actual["my_struct"]
                expected_struct = expected["my_struct"]
                assert len(actual_struct) == len(expected_struct), actual
                for actual_elem, expected_elem in zip(actual_struct, expected_struct):
                    assert int(actual_elem["sub_int"]) == expected_elem["sub_int"], actual_elem
                    np.testing.assert_allclose(actual_elem["sub_vec"], expected_elem["sub_vec"], rtol=1e-5, atol=1e-5)

            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
            assert final_count_ok, {
                "expected_count": len(rows),
                "last_count": final_count,
            }

            search_ids = self._search_array_of_vector_imported_ids(
                collection_name, rows[0]["my_struct"][0]["sub_vec"], limit=10
            )
            assert search_ids & expected_ids, {
                "expected_any": expected_ids,
                "search_ids": search_ids,
            }
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_geometry_timestamptz_manual_import_filters_after_commit(self):
        """
        target: Geometry and Timestamptz import with 2PC
        method: import WKT Geometry and ISO8601 Timestamptz parquet with auto_commit=false
        expected: rows stay invisible before commit and geo/time filters return expected PKs after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_geo_ts")
        self._create_geometry_timestamptz_collection(collection_name)

        rows = self._make_geometry_timestamptz_rows(105000)
        file_name = f"import_2pc_geo_ts_{uuid4()}.parquet"
        file_path = self._write_geometry_timestamptz_parquet_and_upload(rows, file_name)

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=120
            )
            assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

            actual_rows = self._query_geometry_timestamptz_rows_by_ids(collection_name, sorted(expected_ids))
            assert len(actual_rows) == len(rows), actual_rows
            expected_by_id = {row["id"]: row for row in rows}
            for actual in actual_rows:
                expected = expected_by_id[actual["id"]]
                assert actual["tag"] == expected["tag"], actual
                assert str(actual["event_time"]).startswith(expected["event_time"].rstrip("Z")), actual
                assert "POINT" in str(actual["geo"]).upper(), actual

            geo_ids = self._query_ids_by_filter(
                collection_name,
                "ST_EQUALS(geo, 'POINT (10.00 10.00)')",
                limit=len(rows),
            )
            assert geo_ids == {rows[0]["id"]}, {"geo_ids": geo_ids, "expected": rows[0]["id"]}

            time_ids = self._query_ids_by_filter(
                collection_name,
                "event_time >= ISO '2025-06-02T00:00:00Z'",
                limit=len(rows),
            )
            assert time_ids == {row["id"] for row in rows[3:]}, time_ids

            search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
            assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}

            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
            assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_scalar_types_manual_import_preserves_values(self):
        """
        target: scalar data type import with 2PC
        method: import Bool/Int8/Int16/Int32/Int64/Float/Double/VarChar fields with auto_commit=false
        expected: rows stay invisible before commit and all scalar values match exactly after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_scalar")
        self._create_scalar_collection(collection_name)

        rows = self._make_scalar_rows(90750, 6)
        file_name = f"import_2pc_scalar_{uuid4()}.parquet"
        self._write_scalar_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

        scalar_rows = self._query_scalar_rows_by_ids(collection_name, sorted(expected_ids))
        assert len(scalar_rows) == len(rows), scalar_rows
        expected_by_id = {row["id"]: row for row in rows}
        for actual in scalar_rows:
            expected = expected_by_id[actual["id"]]
            assert actual["bool_field"] == expected["bool_field"], actual
            assert actual["int8_field"] == expected["int8_field"], actual
            assert actual["int16_field"] == expected["int16_field"], actual
            assert actual["int32_field"] == expected["int32_field"], actual
            assert actual["int64_field"] == expected["int64_field"], actual
            assert abs(actual["float_field"] - expected["float_field"]) < 1e-6, actual
            assert abs(actual["double_field"] - expected["double_field"]) < 1e-12, actual
            assert actual["varchar_field"] == expected["varchar_field"], actual

        final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
        assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

        search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
        assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_json_field_manual_import_supports_json_path_filter(self):
        """
        target: JSON field import with 2PC
        method: import JSON rows with auto_commit=false, then query JSON values and JSON path filter after commit
        expected: rows stay invisible before commit and JSON field data is filterable after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_json")
        self._create_json_collection(collection_name)

        rows = self._make_json_rows(91050, 6)
        file_name = f"import_2pc_json_{uuid4()}.json"
        self._write_json_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        json_filter_ids_before_commit = self._query_ids_by_filter(
            collection_name, "json_contains(json['key'] , 1)", limit=len(rows)
        )
        assert json_filter_ids_before_commit == set(), {"unexpected_visible_ids": json_filter_ids_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

        json_rows = self._query_json_rows_by_ids(collection_name, sorted(expected_ids))
        assert len(json_rows) == len(rows), json_rows
        expected_by_id = {row["id"]: row for row in rows}
        for actual in json_rows:
            expected = expected_by_id[actual["id"]]
            assert actual["category"] == expected["category"], actual
            actual_json = actual["json"]
            if isinstance(actual_json, str):
                actual_json = json.loads(actual_json)
            assert actual_json == expected["json"], actual

        json_filter_ids = self._query_ids_by_filter(collection_name, "json_contains(json['key'] , 1)", limit=len(rows))
        assert json_filter_ids == {rows[1]["id"]}, {
            "expected": {rows[1]["id"]},
            "seen": json_filter_ids,
        }

        final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
        assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

        search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
        assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_array_fields_manual_import_supports_array_filter(self):
        """
        target: Array field import with 2PC
        method: import Int64/VarChar/Bool array fields with auto_commit=false and query array_contains after commit
        expected: rows stay invisible before commit and array values are filterable after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_array")
        self._create_array_collection(collection_name)

        rows = self._make_array_rows(91150, 6)
        file_name = f"import_2pc_array_{uuid4()}.json"
        self._write_json_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        array_filter_ids_before_commit = self._query_ids_by_filter(
            collection_name, "array_contains(int_array, 3)", limit=len(rows)
        )
        assert array_filter_ids_before_commit == set(), {"unexpected_visible_ids": array_filter_ids_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

        array_rows = self._query_array_rows_by_ids(collection_name, sorted(expected_ids))
        assert len(array_rows) == len(rows), array_rows
        expected_by_id = {row["id"]: row for row in rows}
        for actual in array_rows:
            expected = expected_by_id[actual["id"]]
            assert self._normalize_array_value(actual["int_array"]) == expected["int_array"], actual
            assert self._normalize_array_value(actual["varchar_array"]) == expected["varchar_array"], actual
            assert self._normalize_array_value(actual["bool_array"]) == expected["bool_array"], actual

        array_filter_ids = self._query_ids_by_filter(collection_name, "array_contains(int_array, 3)", limit=len(rows))
        assert array_filter_ids == {rows[3]["id"]}, {
            "expected": {rows[3]["id"]},
            "seen": array_filter_ids,
        }

        final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
        assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

        search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
        assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_dynamic_fields_manual_import_stores_and_filters_extra_keys(self):
        """
        target: dynamic field import with 2PC
        method: import JSON rows containing extra keys into a collection with enableDynamicField=true
        expected: rows stay invisible before commit and dynamic fields are queryable/filterable after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_dynamic")
        self._create_dynamic_collection(collection_name)

        rows = self._make_dynamic_rows(91250, 6)
        file_name = f"import_2pc_dynamic_{uuid4()}.json"
        self._write_json_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        dynamic_filter_ids_before_commit = self._query_ids_by_filter(
            collection_name, "extra_int == 1003", limit=len(rows)
        )
        assert dynamic_filter_ids_before_commit == set(), {"unexpected_visible_ids": dynamic_filter_ids_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

        dynamic_rows = self._query_dynamic_rows_by_ids(collection_name, sorted(expected_ids))
        assert len(dynamic_rows) == len(rows), dynamic_rows
        expected_by_id = {row["id"]: row for row in rows}
        for actual in dynamic_rows:
            expected = expected_by_id[actual["id"]]
            assert actual["extra_str"] == expected["extra_str"], actual
            assert actual["extra_int"] == expected["extra_int"], actual
            assert actual["extra_bool"] == expected["extra_bool"], actual

        dynamic_filter_ids = self._query_ids_by_filter(collection_name, "extra_int == 1003", limit=len(rows))
        assert dynamic_filter_ids == {rows[3]["id"]}, {
            "expected": {rows[3]["id"]},
            "seen": dynamic_filter_ids,
        }

        final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
        assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

        search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
        assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_nullable_fields_manual_import_preserves_nulls(self):
        """
        target: nullable field import with 2PC
        method: import nullable Int64/Float/VarChar fields containing mixed null and non-null values
        expected: rows stay invisible before commit and nullable values are preserved after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_nullable")
        self._create_nullable_collection(collection_name)

        rows = self._make_nullable_rows(90850, 6)
        file_name = f"import_2pc_nullable_{uuid4()}.parquet"
        self._write_nullable_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

        nullable_rows = self._query_nullable_rows_by_ids(collection_name, sorted(expected_ids))
        assert len(nullable_rows) == len(rows), nullable_rows
        expected_by_id = {row["id"]: row for row in rows}
        saw_null_int = False
        saw_non_null_int = False
        saw_null_varchar = False
        saw_non_null_varchar = False
        for actual in nullable_rows:
            expected = expected_by_id[actual["id"]]
            assert actual["required_field"] == expected["required_field"], actual
            assert actual.get("nullable_int") == expected["nullable_int"], actual
            assert actual.get("nullable_varchar") == expected["nullable_varchar"], actual
            if expected["nullable_float"] is None:
                assert actual.get("nullable_float") is None, actual
            else:
                assert abs(actual["nullable_float"] - expected["nullable_float"]) < 1e-6, actual

            saw_null_int = saw_null_int or expected["nullable_int"] is None
            saw_non_null_int = saw_non_null_int or expected["nullable_int"] is not None
            saw_null_varchar = saw_null_varchar or expected["nullable_varchar"] is None
            saw_non_null_varchar = saw_non_null_varchar or expected["nullable_varchar"] is not None

        assert saw_null_int and saw_non_null_int, nullable_rows
        assert saw_null_varchar and saw_non_null_varchar, nullable_rows

        final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
        assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

        search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
        assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_default_value_fields_manual_import_fills_missing_columns(self):
        """
        target: default_value field import with 2PC
        method: import a parquet file missing default_value Int64/Float/VarChar columns
        expected: rows stay invisible before commit and missing fields are filled with defaults after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_default_value")
        self._create_default_value_collection(collection_name)

        rows = self._make_default_missing_rows(90950, 6)
        file_name = f"import_2pc_default_value_{uuid4()}.parquet"
        self._write_default_missing_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

        default_rows = self._query_default_rows_by_ids(collection_name, sorted(expected_ids))
        assert len(default_rows) == len(rows), default_rows
        expected_by_id = {row["id"]: row for row in rows}
        for actual in default_rows:
            expected = expected_by_id[actual["id"]]
            assert actual["required_field"] == expected["required_field"], actual
            assert actual["default_int"] == 999, actual
            assert abs(actual["default_float"] - 8.5) < 1e-6, actual
            assert actual["default_varchar"] == "default_text", actual

        final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
        assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

        search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
        assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_auto_id_manual_import_assigns_ids_after_commit(self):
        """
        target: auto_id primary key import with 2PC
        method: import a parquet file without the primary key column into an autoId collection, then commit
        expected: rows stay invisible before commit and become visible with unique generated IDs after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_auto_id")
        self._create_auto_id_collection(collection_name)

        rows = self._make_auto_id_rows(90700, 6, phase=907)
        file_name = f"import_2pc_auto_id_{uuid4()}.parquet"
        self._write_auto_id_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_tags = {row["tag"] for row in rows}
        seen_before_commit, absent_before_commit = self._wait_tags_absent(collection_name, expected_tags, duration=12)
        assert absent_before_commit, {"unexpected_visible_rows": seen_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        tag_rows, visible_after_commit = self._wait_tags_visible(collection_name, expected_tags, timeout=120)
        assert visible_after_commit, {"expected_tags": expected_tags, "seen_rows": tag_rows}
        assert len(tag_rows) == len(rows), tag_rows
        assert {row["tag"] for row in tag_rows} == expected_tags, tag_rows
        assert {row["phase"] for row in tag_rows} == {907}, tag_rows

        generated_ids = [str(row["id"]) for row in tag_rows]
        assert len(generated_ids) == len(set(generated_ids)) == len(rows), tag_rows
        assert all(generated_id.isdigit() for generated_id in generated_ids), tag_rows

        final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
        assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

        search_tags = self._search_imported_tags(collection_name, rows[0]["vector"], limit=len(rows))
        assert search_tags & expected_tags, {"expected_any": expected_tags, "search_tags": search_tags}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_manual_import_to_specified_partition(self):
        """
        target: import into a specified partition
        method: create two partitions, import rows into one partition with auto_commit=false, then commit
        expected: rows are visible in the target partition only and collection count matches imported rows
        """
        collection_name = gen_collection_name(prefix="import_2pc_partition")
        self._create_base_collection(collection_name)

        target_partition = "p_import_2pc"
        empty_partition = "p_import_2pc_empty"
        rsp = self.partition_client.partition_create(collection_name=collection_name, partition_name=target_partition)
        assert rsp["code"] == 0, rsp
        rsp = self.partition_client.partition_create(collection_name=collection_name, partition_name=empty_partition)
        assert rsp["code"] == 0, rsp

        rows = self._make_rows(90400, 6, phase=904)
        file_name = f"import_2pc_partition_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name, partition_name=target_partition)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        seen_before_commit = self._query_imported_ids(
            collection_name, sorted(expected_ids), partition_names=[target_partition]
        )
        assert seen_before_commit == set(), {"unexpected_visible_ids": seen_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_all_ids, all_visible = self._wait_imported_ids_visible(collection_name, expected_ids, timeout=120)
        assert all_visible, {"expected": expected_ids, "seen": seen_all_ids}

        seen_target_ids = self._query_imported_ids(
            collection_name, sorted(expected_ids), partition_names=[target_partition]
        )
        assert seen_target_ids == expected_ids, {"expected": expected_ids, "seen": seen_target_ids}

        seen_empty_partition_ids = self._query_imported_ids(
            collection_name, sorted(expected_ids), partition_names=[empty_partition]
        )
        assert seen_empty_partition_ids == set(), {"unexpected_visible_ids": seen_empty_partition_ids}

        final_count, final_count_ok = self._wait_count(collection_name, len(expected_ids), timeout=60)
        assert final_count_ok, {"expected_count": len(expected_ids), "last_count": final_count}

        target_stats = self.partition_client.partition_stats(
            collection_name=collection_name, partition_name=target_partition
        )
        assert target_stats["code"] == 0, target_stats
        assert target_stats["data"]["rowCount"] == len(rows), target_stats

        empty_stats = self.partition_client.partition_stats(
            collection_name=collection_name, partition_name=empty_partition
        )
        assert empty_stats["code"] == 0, empty_stats
        assert empty_stats["data"]["rowCount"] == 0, empty_stats

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_partition_key_collection_routes_and_filters_rows(self):
        """
        target: import into a partition-key collection
        method: import rows with multiple partition-key values and query each key after commit
        expected: rows remain invisible before commit and phase filters return exactly the rows for that partition key
        """
        collection_name = gen_collection_name(prefix="import_2pc_partition_key")
        partitions_num = 8
        self._create_partition_key_collection(collection_name, partitions_num=partitions_num)

        desc_rsp = self.collection_client.collection_describe(collection_name)
        assert desc_rsp["code"] == 0, desc_rsp
        assert desc_rsp["data"]["partitionsNum"] == partitions_num, desc_rsp
        phase_fields = [field for field in desc_rsp["data"]["fields"] if field["name"] == "phase"]
        assert len(phase_fields) == 1, desc_rsp
        assert phase_fields[0]["partitionKey"] is True, phase_fields[0]

        rows = []
        rows.extend(self._make_rows(92000, 3, phase=920))
        rows.extend(self._make_rows(92100, 4, phase=921))
        rows.extend(self._make_rows(92200, 5, phase=922))
        file_name = f"import_2pc_partition_key_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

        expected_by_phase = {}
        for row in rows:
            expected_by_phase.setdefault(row["phase"], set()).add(row["id"])

        for phase, phase_ids in expected_by_phase.items():
            actual_ids = self._query_ids_by_filter(collection_name, f"phase == {phase}", limit=len(rows))
            assert actual_ids == phase_ids, {
                "phase": phase,
                "expected_ids": phase_ids,
                "actual_ids": actual_ids,
            }
            phase_count = self._query_count(collection_name, f"phase == {phase}")
            assert phase_count == len(phase_ids), {
                "phase": phase,
                "expected_count": len(phase_ids),
                "actual_count": phase_count,
            }

        final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
        assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_partition_key_collection_rejects_explicit_partition_name(self):
        """
        target: explicit partitionName guardrail on partition-key collection import
        method: create a partition-key collection and submit import with partitionName set
        expected: request is rejected synchronously or the job fails asynchronously without making data visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_partition_key_negative")
        self._create_partition_key_collection(collection_name, partitions_num=8)

        rows = self._make_rows(92300, 4, phase=923)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_partition_key_negative_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        try:
            payload = {
                "collectionName": collection_name,
                "partitionName": "_default",
                "files": [[file_name]],
                "options": {"auto_commit": "false"},
            }
            create_rsp = self.import_job_client.create_import_jobs(payload)
            if create_rsp["code"] != 0:
                readable_reason = str(create_rsp.get("message", create_rsp.get("reason", create_rsp))).lower()
                assert "partition" in readable_reason, create_rsp
                assert "jobId" not in str(create_rsp), create_rsp
                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"expected_count": 0, "last_count": count, "create_rsp": create_rsp}
                return

            job_id = create_rsp["data"]["jobId"]
            last_rsp = None
            observed_states = []
            t0 = time.time()
            while time.time() - t0 < IMPORT_2PC_TIMEOUT:
                last_rsp = self.import_job_client.get_import_job_progress(job_id)
                if last_rsp.get("code") == 0:
                    state = last_rsp.get("data", {}).get("state")
                    if state:
                        observed_states.append(state)
                    if state in {"Failed", "Uncommitted", "Completed"}:
                        break
                time.sleep(2)
            else:
                assert False, {"unexpected_unfinished_job": last_rsp, "observed_states": observed_states}

            state = last_rsp["data"]["state"]
            if state == "Failed":
                reason = str(last_rsp["data"].get("reason", last_rsp)).lower()
                assert reason, last_rsp
                assert "partition" in reason or "import" in reason, last_rsp
                seen_ids, absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=6)
                assert absent, {"unexpected_visible_ids": seen_ids, "last_rsp": last_rsp}
                return

            if state == "Uncommitted":
                seen_ids, absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=6)
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                assert abort_rsp["code"] == 0, abort_rsp
                self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
                if not absent:
                    assert False, {
                        "unexpected_visible_ids": seen_ids,
                        "observed_states": observed_states,
                        "last_rsp": last_rsp,
                    }
                assert False, {
                    "expected_reject_or_failed_job": True,
                    "actual_state": state,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                }

            if state == "Completed":
                visible_ids = self._query_imported_ids(collection_name, sorted(expected_ids))
                assert False, {
                    "expected_reject_or_failed_job": True,
                    "actual_state": state,
                    "visible_ids": visible_ids,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                }

            assert False, {
                "expected_reject_or_failed_job": True,
                "actual_state": state,
                "observed_states": observed_states,
                "last_rsp": last_rsp,
            }
        finally:
            file_path = f"/tmp/{file_name}"
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_clustering_key_collection_manual_import_survives_clustering_compaction(self):
        """
        target: clustering-key collection import and clustering compaction with manual 2PC
        method: import rows into a collection with an Int64 clustering key, commit, then run clustering compaction
        expected: rows stay invisible before commit and remain queryable/searchable after clustering compaction
        """
        collection_name = gen_collection_name(prefix="import_2pc_clustering")
        self._create_clustering_key_collection(collection_name)

        desc_rsp = self.collection_client.collection_describe(collection_name)
        assert desc_rsp["code"] == 0, desc_rsp
        phase_fields = [field for field in desc_rsp["data"]["fields"] if field["name"] == "phase"]
        assert len(phase_fields) == 1, desc_rsp
        assert phase_fields[0]["clusteringKey"] is True, phase_fields[0]

        rows = []
        rows.extend(self._make_rows(92600, 512, phase=926))
        rows.extend(self._make_rows(93600, 512, phase=927))
        file_name = f"import_2pc_clustering_{uuid4()}.parquet"
        file_path = self._write_parquet_and_upload(rows, file_name)

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            sample_ids = {row["id"] for row in rows[:8] + rows[-8:]}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, sample_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, sample_ids, timeout=120
            )
            assert visible_after_commit, {"expected": sample_ids, "seen": seen_after_commit}

            for phase, expected_count in ((926, 512), (927, 512)):
                phase_count = self._query_count(collection_name, f"phase == {phase}")
                assert phase_count == expected_count, {
                    "phase": phase,
                    "expected_count": expected_count,
                    "actual_count": phase_count,
                }

            compact_rsp = self.collection_client.compact(collection_name, is_clustering=True)
            assert compact_rsp["code"] == 0, compact_rsp
            compaction_id = compact_rsp.get("data", {}).get("compactionID")
            assert isinstance(compaction_id, int) and compaction_id > 0, compact_rsp

            compaction_rsp, compaction_completed = self._wait_compaction_completed(compaction_id, timeout=600)
            assert compaction_completed, compaction_rsp

            seen_after_compaction, visible_after_compaction = self._wait_imported_ids_visible(
                collection_name, sample_ids, timeout=120
            )
            assert visible_after_compaction, {
                "expected": sample_ids,
                "seen": seen_after_compaction,
            }

            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=120)
            assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

            actual_rows = self._query_base_rows_by_ids(collection_name, sorted(sample_ids))
            actual_by_id = {row["id"]: row for row in actual_rows}
            expected_by_id = {row["id"]: row for row in rows}
            assert set(actual_by_id.keys()) == sample_ids, actual_rows
            for pk in sample_ids:
                assert actual_by_id[pk]["tag"] == expected_by_id[pk]["tag"], actual_by_id[pk]
                assert actual_by_id[pk]["phase"] == expected_by_id[pk]["phase"], actual_by_id[pk]

            search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=20)
            assert search_ids & expected_ids, {
                "expected_any": sorted(sample_ids),
                "search_ids": search_ids,
            }
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_multi_vector_collection_manual_import_searches_each_vector_field(self):
        """
        target: multi-vector collection import with manual 2PC
        method: import rows into a collection with two FloatVector fields and two vector indexes
        expected: rows stay invisible before commit and each vector field is searchable after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_multi_vector")
        self._create_multi_vector_collection(collection_name)

        rows = self._make_multi_vector_rows(92500, 12, phase=925)
        file_name = f"import_2pc_multi_vector_{uuid4()}.parquet"
        file_path = self._write_multi_vector_parquet_and_upload(rows, file_name)

        try:
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp
            assert rsp["data"]["totalRows"] == len(rows), rsp

            expected_ids = {row["id"] for row in rows}
            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp

            seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=120
            )
            assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

            actual_rows = self._query_base_rows_by_ids(collection_name, sorted(expected_ids))
            assert len(actual_rows) == len(rows), actual_rows
            expected_by_id = {row["id"]: row for row in rows}
            for actual in actual_rows:
                expected = expected_by_id[actual["id"]]
                assert actual["tag"] == expected["tag"], actual
                assert actual["phase"] == expected["phase"], actual

            vector_a_hits = self._search_named_float_vector_imported_ids(
                collection_name,
                "vector_a",
                rows[0]["vector_a"],
                limit=len(rows),
                metric_type="L2",
            )
            assert vector_a_hits & expected_ids, {
                "field": "vector_a",
                "expected_any": expected_ids,
                "search_ids": vector_a_hits,
            }

            vector_b_hits = self._search_named_float_vector_imported_ids(
                collection_name,
                "vector_b",
                rows[-1]["vector_b"],
                limit=len(rows),
                metric_type="COSINE",
            )
            assert vector_b_hits & expected_ids, {
                "field": "vector_b",
                "expected_any": expected_ids,
                "search_ids": vector_b_hits,
            }

            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=60)
            assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_vector_dimension_mismatch_fails_with_reason_and_no_visible_rows(self):
        """
        target: vector dimension validation for manual import
        method: import parquet rows whose FloatVector length is smaller than the collection schema dim
        expected: create is rejected or the job reaches Failed with readable reason, and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_dim_mismatch")
        self._create_base_collection(collection_name, dim=8)

        rows = self._make_rows(92400, 3, phase=924, dim=7)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_dim_mismatch_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        try:
            payload = {
                "collectionName": collection_name,
                "files": [[file_name]],
                "options": {"auto_commit": "false"},
            }
            create_rsp = self.import_job_client.create_import_jobs(payload)
            if create_rsp["code"] != 0:
                reason_text = str(create_rsp.get("message", create_rsp.get("reason", create_rsp))).lower()
                assert any(keyword in reason_text for keyword in ("dimension", "dim", "vector", "schema", "field")), (
                    create_rsp
                )
                assert "jobId" not in str(create_rsp), create_rsp
                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"expected_count": 0, "last_count": count, "create_rsp": create_rsp}
                return

            job_id = create_rsp["data"]["jobId"]
            observed_states = []
            last_rsp = None
            t0 = time.time()
            while time.time() - t0 < IMPORT_2PC_TIMEOUT:
                last_rsp = self.import_job_client.get_import_job_progress(job_id)
                if last_rsp.get("code") == 0:
                    state = last_rsp.get("data", {}).get("state")
                    if state:
                        observed_states.append(state)
                    if state in {"Failed", "Uncommitted", "Completed"}:
                        break
                time.sleep(2)
            else:
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                if abort_rsp["code"] == 0:
                    self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=60)
                assert False, {
                    "expected_failed_job": True,
                    "actual_state": last_rsp.get("data", {}).get("state") if last_rsp else None,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                    "abort_rsp": abort_rsp,
                }

            state = last_rsp["data"]["state"]
            if state == "Failed":
                reason_text = str(last_rsp.get("data", {}).get("reason", last_rsp)).lower()
                detail_text = " ".join(str(detail) for detail in last_rsp["data"].get("details", [])).lower()
                combined_reason = f"{reason_text} {detail_text}".strip()
                assert combined_reason, last_rsp
                assert any(
                    keyword in combined_reason for keyword in ("dimension", "dim", "vector", "schema", "field")
                ), last_rsp
                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"expected_count": 0, "last_count": count, "last_rsp": last_rsp}
                seen_ids, absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=6)
                assert absent, {"unexpected_visible_ids": seen_ids, "last_rsp": last_rsp}
                return

            if state == "Uncommitted":
                seen_ids, absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=6)
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                assert abort_rsp["code"] == 0, abort_rsp
                self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=60)
                assert False, {
                    "expected_reject_or_failed_job": True,
                    "actual_state": state,
                    "absent_before_cleanup": absent,
                    "unexpected_visible_ids": seen_ids,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                }

            if state == "Completed":
                visible_ids = self._query_imported_ids(collection_name, sorted(expected_ids))
                count_after_completion = self._query_count(collection_name)
                assert False, {
                    "expected_reject_or_failed_job": True,
                    "actual_state": state,
                    "visible_ids": visible_ids,
                    "count_after_completion": count_after_completion,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                }

            assert False, {
                "expected_reject_or_failed_job": True,
                "actual_state": state,
                "observed_states": observed_states,
                "last_rsp": last_rsp,
            }
        finally:
            file_path = f"/tmp/{file_name}"
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_binary_vector_byte_length_mismatch_fails_with_reason_and_no_visible_rows(self):
        """
        target: BinaryVector byte-length validation for manual import
        method: import parquet rows whose BinaryVector byte length is smaller than schema dim/8
        expected: create is rejected or the job reaches Failed with readable reason, and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_binary_len_mismatch")
        self._create_binary_collection(collection_name, "BIN_FLAT", {}, dim=128)

        rows = self._make_binary_rows(92700, 3, phase=927, dim=128)
        for row in rows:
            row["binary_vector"] = row["binary_vector"][:-1]
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_binary_len_mismatch_{uuid4()}.parquet"
        self._write_binary_parquet_and_upload(rows, file_name, dim=128)

        try:
            payload = {
                "collectionName": collection_name,
                "files": [[file_name]],
                "options": {"auto_commit": "false"},
            }
            create_rsp = self.import_job_client.create_import_jobs(payload)
            expected_reason_keywords = ("binary", "dimension", "dim", "length", "byte", "vector", "schema", "field")
            if create_rsp["code"] != 0:
                reason_text = str(create_rsp.get("message", create_rsp.get("reason", create_rsp))).lower()
                assert any(keyword in reason_text for keyword in expected_reason_keywords), create_rsp
                assert "jobId" not in str(create_rsp), create_rsp
                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"expected_count": 0, "last_count": count, "create_rsp": create_rsp}
                return

            job_id = create_rsp["data"]["jobId"]
            observed_states = []
            last_rsp = None
            t0 = time.time()
            while time.time() - t0 < IMPORT_2PC_TIMEOUT:
                last_rsp = self.import_job_client.get_import_job_progress(job_id)
                if last_rsp.get("code") == 0:
                    state = last_rsp.get("data", {}).get("state")
                    if state:
                        observed_states.append(state)
                    if state in {"Failed", "Uncommitted", "Completed"}:
                        break
                time.sleep(2)
            else:
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                if abort_rsp["code"] == 0:
                    self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=60)
                assert False, {
                    "expected_failed_job": True,
                    "actual_state": last_rsp.get("data", {}).get("state") if last_rsp else None,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                    "abort_rsp": abort_rsp,
                }

            state = last_rsp["data"]["state"]
            if state == "Failed":
                reason_text = str(last_rsp.get("data", {}).get("reason", last_rsp)).lower()
                detail_text = " ".join(str(detail) for detail in last_rsp["data"].get("details", [])).lower()
                combined_reason = f"{reason_text} {detail_text}".strip()
                assert combined_reason, last_rsp
                assert any(keyword in combined_reason for keyword in expected_reason_keywords), last_rsp
                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"expected_count": 0, "last_count": count, "last_rsp": last_rsp}
                seen_ids, absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=6)
                assert absent, {"unexpected_visible_ids": seen_ids, "last_rsp": last_rsp}
                return

            if state == "Uncommitted":
                seen_ids, absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=6)
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                assert abort_rsp["code"] == 0, abort_rsp
                self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=60)
                assert False, {
                    "expected_reject_or_failed_job": True,
                    "actual_state": state,
                    "absent_before_cleanup": absent,
                    "unexpected_visible_ids": seen_ids,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                }

            if state == "Completed":
                visible_ids = self._query_imported_ids(collection_name, sorted(expected_ids))
                count_after_completion = self._query_count(collection_name)
                assert False, {
                    "expected_reject_or_failed_job": True,
                    "actual_state": state,
                    "visible_ids": visible_ids,
                    "count_after_completion": count_after_completion,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                }

            assert False, {
                "expected_reject_or_failed_job": True,
                "actual_state": state,
                "observed_states": observed_states,
                "last_rsp": last_rsp,
            }
        finally:
            file_path = f"/tmp/{file_name}"
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_varchar_overflow_fails_with_reason_and_no_visible_rows(self):
        """
        target: varchar max_length validation for manual import
        method: import parquet rows whose tag value exceeds the collection schema max_length
        expected: create is rejected or the job reaches Failed with readable reason, and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_varchar_overflow")
        self._create_base_collection(collection_name)

        rows = self._make_rows(92500, 3, phase=925)
        for row in rows:
            row["tag"] = f"overflow_{row['id']}_" + ("x" * 80)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_varchar_overflow_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        try:
            payload = {
                "collectionName": collection_name,
                "files": [[file_name]],
                "options": {"auto_commit": "false"},
            }
            create_rsp = self.import_job_client.create_import_jobs(payload)
            if create_rsp["code"] != 0:
                reason_text = str(create_rsp.get("message", create_rsp.get("reason", create_rsp))).lower()
                assert any(
                    keyword in reason_text
                    for keyword in ("varchar", "string", "length", "max", "tag", "overflow", "exceed")
                ), create_rsp
                assert "jobId" not in str(create_rsp), create_rsp
                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"expected_count": 0, "last_count": count, "create_rsp": create_rsp}
                return

            job_id = create_rsp["data"]["jobId"]
            observed_states = []
            last_rsp = None
            t0 = time.time()
            while time.time() - t0 < IMPORT_2PC_TIMEOUT:
                last_rsp = self.import_job_client.get_import_job_progress(job_id)
                if last_rsp.get("code") == 0:
                    state = last_rsp.get("data", {}).get("state")
                    if state:
                        observed_states.append(state)
                    if state in {"Failed", "Uncommitted", "Completed"}:
                        break
                time.sleep(2)
            else:
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                if abort_rsp["code"] == 0:
                    self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=60)
                assert False, {
                    "expected_failed_job": True,
                    "actual_state": last_rsp.get("data", {}).get("state") if last_rsp else None,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                    "abort_rsp": abort_rsp,
                }

            state = last_rsp["data"]["state"]
            if state == "Failed":
                reason_text = str(last_rsp.get("data", {}).get("reason", last_rsp)).lower()
                detail_text = " ".join(str(detail) for detail in last_rsp["data"].get("details", [])).lower()
                combined_reason = f"{reason_text} {detail_text}".strip()
                assert combined_reason, last_rsp
                assert any(
                    keyword in combined_reason
                    for keyword in ("varchar", "string", "length", "max", "tag", "overflow", "exceed")
                ), last_rsp
                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"expected_count": 0, "last_count": count, "last_rsp": last_rsp}
                seen_ids, absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=6)
                assert absent, {"unexpected_visible_ids": seen_ids, "last_rsp": last_rsp}
                return

            if state == "Uncommitted":
                seen_ids, absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=6)
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                assert abort_rsp["code"] == 0, abort_rsp
                self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=60)
                assert False, {
                    "expected_reject_or_failed_job": True,
                    "actual_state": state,
                    "absent_before_cleanup": absent,
                    "unexpected_visible_ids": seen_ids,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                }

            if state == "Completed":
                visible_ids = self._query_imported_ids(collection_name, sorted(expected_ids))
                count_after_completion = self._query_count(collection_name)
                assert False, {
                    "expected_reject_or_failed_job": True,
                    "actual_state": state,
                    "visible_ids": visible_ids,
                    "count_after_completion": count_after_completion,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                }

            assert False, {
                "expected_reject_or_failed_job": True,
                "actual_state": state,
                "observed_states": observed_states,
                "last_rsp": last_rsp,
            }
        finally:
            file_path = f"/tmp/{file_name}"
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_field_type_mismatch_fails_with_reason_and_no_visible_rows(self):
        """
        target: field type validation for manual import
        method: import a parquet file whose phase column is string while collection schema expects Int64
        expected: create is rejected or the job reaches Failed with readable reason, and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_type_mismatch")
        self._create_base_collection(collection_name)

        rows = self._make_rows(92600, 3, phase=926)
        for row in rows:
            row["phase"] = f"bad_phase_{row['id']}"
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_type_mismatch_{uuid4()}.parquet"
        file_path = f"/tmp/{file_name}"
        schema = pa.schema(
            [
                ("id", pa.int64(), False),
                ("tag", pa.string(), False),
                ("phase", pa.string(), False),
                ("vector", pa.list_(pa.float32()), False),
            ]
        )
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)

        try:
            payload = {
                "collectionName": collection_name,
                "files": [[file_name]],
                "options": {"auto_commit": "false"},
            }
            create_rsp = self.import_job_client.create_import_jobs(payload)
            if create_rsp["code"] != 0:
                reason_text = str(create_rsp.get("message", create_rsp.get("reason", create_rsp))).lower()
                assert any(
                    keyword in reason_text
                    for keyword in ("phase", "int", "string", "type", "schema", "parquet", "field")
                ), create_rsp
                assert "jobId" not in str(create_rsp), create_rsp
                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"expected_count": 0, "last_count": count, "create_rsp": create_rsp}
                return

            job_id = create_rsp["data"]["jobId"]
            observed_states = []
            last_rsp = None
            t0 = time.time()
            while time.time() - t0 < IMPORT_2PC_TIMEOUT:
                last_rsp = self.import_job_client.get_import_job_progress(job_id)
                if last_rsp.get("code") == 0:
                    state = last_rsp.get("data", {}).get("state")
                    if state:
                        observed_states.append(state)
                    if state in {"Failed", "Uncommitted", "Completed"}:
                        break
                time.sleep(2)
            else:
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                if abort_rsp["code"] == 0:
                    self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=60)
                assert False, {
                    "expected_failed_job": True,
                    "actual_state": last_rsp.get("data", {}).get("state") if last_rsp else None,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                    "abort_rsp": abort_rsp,
                }

            state = last_rsp["data"]["state"]
            if state == "Failed":
                reason_text = str(last_rsp.get("data", {}).get("reason", last_rsp)).lower()
                detail_text = " ".join(str(detail) for detail in last_rsp["data"].get("details", [])).lower()
                combined_reason = f"{reason_text} {detail_text}".strip()
                assert combined_reason, last_rsp
                assert any(
                    keyword in combined_reason
                    for keyword in ("phase", "int", "string", "type", "schema", "parquet", "field")
                ), last_rsp
                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"expected_count": 0, "last_count": count, "last_rsp": last_rsp}
                seen_ids, absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=6)
                assert absent, {"unexpected_visible_ids": seen_ids, "last_rsp": last_rsp}
                return

            if state == "Uncommitted":
                seen_ids, absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=6)
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                assert abort_rsp["code"] == 0, abort_rsp
                self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=60)
                assert False, {
                    "expected_reject_or_failed_job": True,
                    "actual_state": state,
                    "absent_before_cleanup": absent,
                    "unexpected_visible_ids": seen_ids,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                }

            if state == "Completed":
                visible_ids = self._query_imported_ids(collection_name, sorted(expected_ids))
                count_after_completion = self._query_count(collection_name)
                assert False, {
                    "expected_reject_or_failed_job": True,
                    "actual_state": state,
                    "visible_ids": visible_ids,
                    "count_after_completion": count_after_completion,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                }

            assert False, {
                "expected_reject_or_failed_job": True,
                "actual_state": state,
                "observed_states": observed_states,
                "last_rsp": last_rsp,
            }
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(
        reason="milvus-io/milvus#50459: import currently accepts NaN/Inf FloatVector values",
        strict=True,
    )
    def test_import_2pc_vector_nan_inf_fails_with_reason_and_no_visible_rows(self):
        """
        target: FloatVector finite-value validation for manual import
        method: import parquet rows containing NaN and Inf inside FloatVector values
        expected: create is rejected or the job reaches Failed with readable reason, and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_vector_nan_inf")
        self._create_base_collection(collection_name)

        rows = self._make_rows(92700, 3, phase=927)
        rows[0]["vector"][0] = float("nan")
        rows[1]["vector"][1] = float("inf")
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_vector_nan_inf_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        try:
            payload = {
                "collectionName": collection_name,
                "files": [[file_name]],
                "options": {"auto_commit": "false"},
            }
            create_rsp = self.import_job_client.create_import_jobs(payload)
            if create_rsp["code"] != 0:
                reason_text = str(create_rsp.get("message", create_rsp.get("reason", create_rsp))).lower()
                assert any(
                    keyword in reason_text
                    for keyword in ("nan", "inf", "finite", "float", "vector", "invalid", "schema", "field")
                ), create_rsp
                assert "jobId" not in str(create_rsp), create_rsp
                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"expected_count": 0, "last_count": count, "create_rsp": create_rsp}
                return

            job_id = create_rsp["data"]["jobId"]
            observed_states = []
            last_rsp = None
            t0 = time.time()
            while time.time() - t0 < IMPORT_2PC_TIMEOUT:
                last_rsp = self.import_job_client.get_import_job_progress(job_id)
                if last_rsp.get("code") == 0:
                    state = last_rsp.get("data", {}).get("state")
                    if state:
                        observed_states.append(state)
                    if state in {"Failed", "Uncommitted", "Completed"}:
                        break
                time.sleep(2)
            else:
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                if abort_rsp["code"] == 0:
                    self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=60)
                assert False, {
                    "expected_failed_job": True,
                    "actual_state": last_rsp.get("data", {}).get("state") if last_rsp else None,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                    "abort_rsp": abort_rsp,
                }

            state = last_rsp["data"]["state"]
            if state == "Failed":
                reason_text = str(last_rsp.get("data", {}).get("reason", last_rsp)).lower()
                detail_text = " ".join(str(detail) for detail in last_rsp["data"].get("details", [])).lower()
                combined_reason = f"{reason_text} {detail_text}".strip()
                assert combined_reason, last_rsp
                assert any(
                    keyword in combined_reason
                    for keyword in ("nan", "inf", "finite", "float", "vector", "invalid", "schema", "field")
                ), last_rsp
                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"expected_count": 0, "last_count": count, "last_rsp": last_rsp}
                seen_ids, absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=6)
                assert absent, {"unexpected_visible_ids": seen_ids, "last_rsp": last_rsp}
                return

            if state == "Uncommitted":
                seen_ids, absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=6)
                abort_rsp = self.import_job_client.abort_import_job(job_id)
                assert abort_rsp["code"] == 0, abort_rsp
                self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=60)
                assert False, {
                    "expected_reject_or_failed_job": True,
                    "actual_state": state,
                    "absent_before_cleanup": absent,
                    "unexpected_visible_ids": seen_ids,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                }

            if state == "Completed":
                visible_ids = self._query_imported_ids(collection_name, sorted(expected_ids))
                count_after_completion = self._query_count(collection_name)
                assert False, {
                    "expected_reject_or_failed_job": True,
                    "actual_state": state,
                    "visible_ids": visible_ids,
                    "count_after_completion": count_after_completion,
                    "observed_states": observed_states,
                    "last_rsp": last_rsp,
                }

            assert False, {
                "expected_reject_or_failed_job": True,
                "actual_state": state,
                "observed_states": observed_states,
                "last_rsp": last_rsp,
            }
        finally:
            file_path = f"/tmp/{file_name}"
            if os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_rest_insert_interleaves_with_manual_import(self):
        """
        target: import and normal DML interleaving
        method: insert rows before import, during Uncommitted import, and after import commit
        expected: import does not block REST insert and imported/inserted PK sets remain independent
        """
        collection_name = gen_collection_name(prefix="import_2pc_mix_insert")
        self._create_base_collection(collection_name)

        before_rows = self._make_rows(8600, 2, phase=86)
        import_rows = self._make_rows(8700, 6, phase=87)
        during_rows = self._make_rows(8800, 2, phase=88)
        after_rows = self._make_rows(8900, 2, phase=89)

        self._insert_rows(collection_name, before_rows)
        before_ids = {row["id"] for row in before_rows}
        seen_before_ids, before_visible = self._wait_imported_ids_visible(collection_name, before_ids, timeout=60)
        assert before_visible, {"expected": before_ids, "seen": seen_before_ids}

        file_name = f"import_2pc_mix_insert_{uuid4()}.parquet"
        self._write_parquet_and_upload(import_rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(import_rows), rsp
        assert rsp["data"]["totalRows"] == len(import_rows), rsp

        import_ids = {row["id"] for row in import_rows}
        seen_import_before_commit, import_absent = self._wait_imported_ids_absent(
            collection_name, import_ids, duration=12
        )
        assert import_absent, {"unexpected_visible_import_ids": seen_import_before_commit}

        self._insert_rows(collection_name, during_rows)
        during_ids = {row["id"] for row in during_rows}
        dml_ids = before_ids | during_ids
        seen_dml_ids, dml_visible = self._wait_imported_ids_visible(collection_name, dml_ids, timeout=60)
        assert dml_visible, {"expected": dml_ids, "seen": seen_dml_ids}

        seen_import_during_dml = self._query_imported_ids(collection_name, sorted(import_ids))
        assert seen_import_during_dml == set(), {"unexpected_visible_import_ids": seen_import_during_dml}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp

        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        committed_ids = dml_ids | import_ids
        seen_committed_ids, committed_visible = self._wait_imported_ids_visible(
            collection_name, committed_ids, timeout=120
        )
        assert committed_visible, {"expected": committed_ids, "seen": seen_committed_ids}
        count_after_commit, count_ok = self._wait_count(collection_name, len(committed_ids), timeout=60)
        assert count_ok, {"expected_count": len(committed_ids), "last_count": count_after_commit}

        self._insert_rows(collection_name, after_rows)
        after_ids = {row["id"] for row in after_rows}
        all_ids = committed_ids | after_ids
        seen_all_ids, all_visible = self._wait_imported_ids_visible(collection_name, all_ids, timeout=60)
        assert all_visible, {"expected": all_ids, "seen": seen_all_ids}

        final_count, final_count_ok = self._wait_count(collection_name, len(all_ids), timeout=60)
        assert final_count_ok, {"expected_count": len(all_ids), "last_count": final_count}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_upsert_pk_conflict_respects_commit_timestamp(self):
        """
        target: upsert conflicts with imported primary keys around import commit
        method: upsert the same PKs before commit, then upsert a subset again after commit
        expected: pre-commit upserts are superseded by import commit, while post-commit upserts supersede import rows
        """
        collection_name = gen_collection_name(prefix="import_2pc_mix_upsert")
        self._create_base_collection(collection_name)

        import_rows = self._make_rows(91300, 6, phase=913)
        pre_commit_upsert_rows = []
        post_commit_upsert_rows = []
        for row in import_rows:
            pre_commit_upsert_rows.append(
                {
                    "id": row["id"],
                    "tag": f"pre_upsert_{row['id']}",
                    "phase": 914,
                    "vector": [value + 0.1 for value in row["vector"]],
                }
            )
        for row in import_rows[:3]:
            post_commit_upsert_rows.append(
                {
                    "id": row["id"],
                    "tag": f"post_upsert_{row['id']}",
                    "phase": 915,
                    "vector": [value + 0.2 for value in row["vector"]],
                }
            )

        file_name = f"import_2pc_mix_upsert_{uuid4()}.parquet"
        self._write_parquet_and_upload(import_rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(import_rows), rsp
        assert rsp["data"]["totalRows"] == len(import_rows), rsp

        import_ids = {row["id"] for row in import_rows}
        seen_before_upsert, import_absent = self._wait_imported_ids_absent(collection_name, import_ids, duration=12)
        assert import_absent, {"unexpected_visible_import_ids": seen_before_upsert}

        self._upsert_rows(collection_name, pre_commit_upsert_rows)
        pre_commit_expected = {row["id"]: {"tag": row["tag"], "phase": row["phase"]} for row in pre_commit_upsert_rows}
        pre_upsert_rows, pre_upsert_visible = self._wait_base_rows_by_ids_match(
            collection_name, pre_commit_expected, timeout=60
        )
        assert pre_upsert_visible, {"expected": pre_commit_expected, "seen_rows": pre_upsert_rows}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        import_expected = {row["id"]: {"tag": row["tag"], "phase": row["phase"]} for row in import_rows}
        import_rows_seen, import_version_visible = self._wait_base_rows_by_ids_match(
            collection_name, import_expected, timeout=120
        )
        assert import_version_visible, {"expected": import_expected, "seen_rows": import_rows_seen}

        self._upsert_rows(collection_name, post_commit_upsert_rows)
        post_commit_expected = dict(import_expected)
        for row in post_commit_upsert_rows:
            post_commit_expected[row["id"]] = {"tag": row["tag"], "phase": row["phase"]}

        final_rows_seen, final_versions_visible = self._wait_base_rows_by_ids_match(
            collection_name, post_commit_expected, timeout=120
        )
        assert final_versions_visible, {"expected": post_commit_expected, "seen_rows": final_rows_seen}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_flush_during_uncommitted_keeps_import_invisible(self):
        """
        target: manual flush while import is Uncommitted
        method: flush the collection after a manual import job reaches Uncommitted, then commit it
        expected: flush does not expose uncommitted import rows and commit still makes them visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_flush")
        self._create_base_collection(collection_name)

        baseline_rows = self._make_rows(90000, 2, phase=900)
        import_rows = self._make_rows(90100, 6, phase=901)
        self._insert_rows(collection_name, baseline_rows)

        baseline_ids = {row["id"] for row in baseline_rows}
        seen_baseline_ids, baseline_visible = self._wait_imported_ids_visible(collection_name, baseline_ids, timeout=60)
        assert baseline_visible, {"expected": baseline_ids, "seen": seen_baseline_ids}

        file_name = f"import_2pc_flush_{uuid4()}.parquet"
        self._write_parquet_and_upload(import_rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(import_rows), rsp
        assert rsp["data"]["totalRows"] == len(import_rows), rsp

        import_ids = {row["id"] for row in import_rows}
        seen_before_flush, absent_before_flush = self._wait_imported_ids_absent(
            collection_name, import_ids, duration=12
        )
        assert absent_before_flush, {"unexpected_visible_import_ids": seen_before_flush}

        flush_rsp = self.collection_client.flush(collection_name)
        assert flush_rsp["code"] == 0, flush_rsp

        seen_after_flush, absent_after_flush = self._wait_imported_ids_absent(collection_name, import_ids, duration=12)
        assert absent_after_flush, {"unexpected_visible_import_ids": seen_after_flush}

        seen_baseline_after_flush = self._query_imported_ids(collection_name, sorted(baseline_ids))
        assert seen_baseline_after_flush == baseline_ids, {
            "expected_baseline_ids": baseline_ids,
            "seen": seen_baseline_after_flush,
        }

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        all_ids = baseline_ids | import_ids
        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(collection_name, all_ids, timeout=120)
        assert visible_after_commit, {"expected": all_ids, "seen": seen_after_commit}

        final_count, final_count_ok = self._wait_count(collection_name, len(all_ids), timeout=60)
        assert final_count_ok, {"expected_count": len(all_ids), "last_count": final_count}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_delete_before_commit_is_noop_for_import_rows(self):
        """
        target: delete timestamp before import commit timestamp
        method: delete imported PKs while the manual import job is Uncommitted, then commit the job
        expected: delete is a no-op for those import rows and all rows become visible after commit
        """
        collection_name = gen_collection_name(prefix="import_2pc_delete_before_commit")
        self._create_base_collection(collection_name)

        rows = self._make_rows(8000, 6, phase=8)
        file_name = f"import_2pc_delete_before_commit_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        seen_before_delete, absent_before_delete = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_before_delete, {"unexpected_visible_ids": seen_before_delete}

        self._delete_ids(collection_name, expected_ids)

        seen_after_delete, absent_after_delete = self._wait_imported_ids_absent(
            collection_name, expected_ids, duration=12
        )
        assert absent_after_delete, {"unexpected_visible_ids": seen_after_delete}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp

        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_after_commit, {"expected": expected_ids, "seen": seen_after_commit}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_delete_after_commit_removes_import_rows(self):
        """
        target: delete timestamp after import commit timestamp
        method: commit a manual import job, then delete a subset of imported PKs
        expected: deleted import rows disappear and untouched import rows remain visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_delete_after_commit")
        self._create_base_collection(collection_name)

        rows = self._make_rows(8200, 8, phase=82)
        file_name = f"import_2pc_delete_after_commit_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp

        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        imported_ids = {row["id"] for row in rows}
        seen_before_delete, visible_before_delete = self._wait_imported_ids_visible(
            collection_name, imported_ids, timeout=120
        )
        assert visible_before_delete, {"expected": imported_ids, "seen": seen_before_delete}

        deleted_ids = {row["id"] for row in rows[:3]}
        remaining_ids = imported_ids - deleted_ids
        self._delete_ids(collection_name, deleted_ids)

        seen_deleted_ids, delete_visible = self._wait_imported_ids_eventually_absent(
            collection_name, deleted_ids, timeout=120
        )
        assert delete_visible, {"unexpected_visible_deleted_ids": seen_deleted_ids}

        seen_remaining_ids, remaining_visible = self._wait_imported_ids_visible(
            collection_name, remaining_ids, timeout=60
        )
        assert remaining_visible, {"expected": remaining_ids, "seen": seen_remaining_ids}

        seen_all_after_delete = self._query_imported_ids(collection_name, sorted(imported_ids))
        assert seen_all_after_delete == remaining_ids, {
            "expected_remaining": remaining_ids,
            "seen": seen_all_after_delete,
        }

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_mixed_delete_across_commit_boundary(self):
        """
        target: mixed delete timestamps around import commit timestamp
        method: delete one import PK subset before commit and another subset after commit
        expected: pre-commit delete is ignored, post-commit delete applies, and untouched rows remain visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_delete_mixed")
        self._create_base_collection(collection_name)

        rows = self._make_rows(90300, 9, phase=903)
        file_name = f"import_2pc_delete_mixed_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        all_import_ids = {row["id"] for row in rows}
        pre_commit_delete_ids = {row["id"] for row in rows[:3]}
        post_commit_delete_ids = {row["id"] for row in rows[3:6]}
        never_delete_ids = all_import_ids - pre_commit_delete_ids - post_commit_delete_ids

        seen_before_delete, absent_before_delete = self._wait_imported_ids_absent(
            collection_name, all_import_ids, duration=12
        )
        assert absent_before_delete, {"unexpected_visible_ids": seen_before_delete}

        self._delete_ids(collection_name, pre_commit_delete_ids)

        seen_after_pre_delete, absent_after_pre_delete = self._wait_imported_ids_absent(
            collection_name, all_import_ids, duration=12
        )
        assert absent_after_pre_delete, {"unexpected_visible_ids": seen_after_pre_delete}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_after_commit, visible_after_commit = self._wait_imported_ids_visible(
            collection_name, all_import_ids, timeout=120
        )
        assert visible_after_commit, {"expected": all_import_ids, "seen": seen_after_commit}

        self._delete_ids(collection_name, post_commit_delete_ids)

        seen_post_deleted, post_deleted_absent = self._wait_imported_ids_eventually_absent(
            collection_name, post_commit_delete_ids, timeout=120
        )
        assert post_deleted_absent, {"unexpected_visible_deleted_ids": seen_post_deleted}

        expected_visible_ids = pre_commit_delete_ids | never_delete_ids
        seen_expected_visible, expected_visible = self._wait_imported_ids_visible(
            collection_name, expected_visible_ids, timeout=60
        )
        assert expected_visible, {"expected": expected_visible_ids, "seen": seen_expected_visible}

        seen_all_after_delete = self._query_imported_ids(collection_name, sorted(all_import_ids))
        assert seen_all_after_delete == expected_visible_ids, {
            "expected_visible_ids": expected_visible_ids,
            "seen": seen_all_after_delete,
        }

        final_count, final_count_ok = self._wait_count(collection_name, len(expected_visible_ids), timeout=60)
        assert final_count_ok, {"expected_count": len(expected_visible_ids), "last_count": final_count}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_auto_id_scalar_delete_respects_commit_timestamp(self):
        """
        target: auto_id collection delete timestamp boundary with scalar filter
        method: delete one tag subset before commit and another tag subset after commit
        expected: pre-commit scalar delete is ignored, post-commit scalar delete applies, and generated IDs remain valid
        """
        collection_name = gen_collection_name(prefix="import_2pc_auto_id_delete")
        self._create_auto_id_collection(collection_name)

        rows = self._make_auto_id_rows(91200, 8, phase=912)
        file_name = f"import_2pc_auto_id_delete_{uuid4()}.parquet"
        self._write_auto_id_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        all_tags = {row["tag"] for row in rows}
        pre_commit_delete_tags = {row["tag"] for row in rows[:3]}
        post_commit_delete_tags = {row["tag"] for row in rows[3:6]}
        never_delete_tags = all_tags - pre_commit_delete_tags - post_commit_delete_tags

        seen_before_delete, absent_before_delete = self._wait_tags_absent(collection_name, all_tags, duration=12)
        assert absent_before_delete, {"unexpected_visible_rows": seen_before_delete}

        self._delete_tags(collection_name, pre_commit_delete_tags)

        seen_after_pre_delete, absent_after_pre_delete = self._wait_tags_absent(collection_name, all_tags, duration=12)
        assert absent_after_pre_delete, {"unexpected_visible_rows": seen_after_pre_delete}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        tag_rows, visible_after_commit = self._wait_tags_visible(collection_name, all_tags, timeout=120)
        assert visible_after_commit, {"expected_tags": all_tags, "seen_rows": tag_rows}
        assert {row["tag"] for row in tag_rows} == all_tags, tag_rows
        assert {row["phase"] for row in tag_rows} == {912}, tag_rows
        generated_ids = [str(row["id"]) for row in tag_rows]
        assert len(generated_ids) == len(set(generated_ids)) == len(rows), tag_rows
        assert all(generated_id.isdigit() for generated_id in generated_ids), tag_rows

        self._delete_tags(collection_name, post_commit_delete_tags)

        seen_post_deleted, post_deleted_absent = self._wait_tags_absent(
            collection_name, post_commit_delete_tags, duration=120, interval=2
        )
        assert post_deleted_absent, {"unexpected_visible_deleted_rows": seen_post_deleted}

        expected_visible_tags = pre_commit_delete_tags | never_delete_tags
        expected_rows, expected_visible = self._wait_tags_visible(collection_name, expected_visible_tags, timeout=60)
        assert expected_visible, {"expected_tags": expected_visible_tags, "seen_rows": expected_rows}

        final_rows = self._query_rows_by_tags(collection_name, all_tags)
        assert {row["tag"] for row in final_rows} == expected_visible_tags, {
            "expected_visible_tags": expected_visible_tags,
            "final_rows": final_rows,
        }
        final_count, final_count_ok = self._wait_count(collection_name, len(expected_visible_tags), timeout=60)
        assert final_count_ok, {"expected_count": len(expected_visible_tags), "last_count": final_count}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_abort_before_commit_keeps_import_invisible(self):
        """
        target: abort manual import before commit
        method: abort an Uncommitted import job while a normal DML row is visible, then try CommitImport
        expected: aborted import moves to Failed, later CommitImport is rejected, imported rows stay invisible,
                  and normal DML remains visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_abort_before_commit")
        self._create_base_collection(collection_name)

        baseline_row = self._make_rows(9100, 1, phase=91)[0]
        self._insert_one_row(collection_name, baseline_row)
        baseline_seen, baseline_visible = self._wait_imported_ids_visible(
            collection_name, {baseline_row["id"]}, timeout=60
        )
        assert baseline_visible, {"expected_baseline_id": baseline_row["id"], "seen": baseline_seen}

        rows = self._make_rows(6000, 6, phase=6)
        file_name = f"import_2pc_abort_before_commit_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        imported_ids = {row["id"] for row in rows}
        seen_before_abort, absent_before_abort = self._wait_imported_ids_absent(
            collection_name, imported_ids, duration=12
        )
        assert absent_before_abort, {"unexpected_visible_ids": seen_before_abort}

        abort_rsp = self.import_job_client.abort_import_job(job_id)
        assert abort_rsp["code"] == 0, abort_rsp

        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        commit_after_abort_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_after_abort_rsp["code"] != 0, commit_after_abort_rsp

        progress_after_commit_rsp = self.import_job_client.get_import_job_progress(job_id)
        assert progress_after_commit_rsp["code"] == 0, progress_after_commit_rsp
        assert progress_after_commit_rsp["data"]["state"] == "Failed", progress_after_commit_rsp

        seen_after_abort, absent_after_abort = self._wait_imported_ids_absent(
            collection_name, imported_ids, duration=18
        )
        assert absent_after_abort, {"unexpected_visible_ids": seen_after_abort}

        baseline_after_abort = self._query_imported_ids(collection_name, [baseline_row["id"]])
        assert baseline_after_abort == {baseline_row["id"]}, {
            "expected_baseline_id": baseline_row["id"],
            "seen": baseline_after_abort,
        }

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_abort_is_idempotent_for_failed_job(self):
        """
        target: abort retry idempotency for terminal failed import job
        method: abort an Uncommitted import job, wait for Failed, then abort the same job again
        expected: repeated abort returns success, job remains Failed, and imported rows stay invisible
        """
        collection_name = gen_collection_name(prefix="import_2pc_abort_idempotent")
        self._create_base_collection(collection_name)

        rows = self._make_rows(90200, 6, phase=902)
        file_name = f"import_2pc_abort_idempotent_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        imported_ids = {row["id"] for row in rows}
        seen_before_abort, absent_before_abort = self._wait_imported_ids_absent(
            collection_name, imported_ids, duration=12
        )
        assert absent_before_abort, {"unexpected_visible_ids": seen_before_abort}

        abort_rsp = self.import_job_client.abort_import_job(job_id)
        assert abort_rsp["code"] == 0, abort_rsp

        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        retry_abort_rsp = self.import_job_client.abort_import_job(job_id)
        assert retry_abort_rsp["code"] == 0, retry_abort_rsp

        progress_rsp = self.import_job_client.get_import_job_progress(job_id)
        assert progress_rsp["code"] == 0, progress_rsp
        assert progress_rsp["data"]["state"] == "Failed", progress_rsp

        seen_after_retry, absent_after_retry = self._wait_imported_ids_absent(
            collection_name, imported_ids, duration=18
        )
        assert absent_after_retry, {"unexpected_visible_ids": seen_after_retry}

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_commit_rejects_invalid_states(self):
        """
        target: invalid commit state handling
        method: commit a failed manual import job and a nonexistent numeric job
        expected: commit returns business errors instead of moving either job to committed states
        """
        collection_name = gen_collection_name(prefix="import_2pc_commit_invalid")
        self._create_base_collection(collection_name)

        missing_file = f"import_2pc_missing_{uuid4()}.parquet"
        failed_job_id = self._create_import_job(
            collection_name,
            missing_file,
            options={"auto_commit": "false"},
        )
        rsp, ok = self.import_job_client.wait_import_job_state(failed_job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        failed_commit_rsp = self.import_job_client.commit_import_job(failed_job_id)
        assert failed_commit_rsp["code"] != 0, failed_commit_rsp
        assert "expected Uncommitted" in str(failed_commit_rsp), failed_commit_rsp

        progress_rsp = self.import_job_client.get_import_job_progress(failed_job_id)
        assert progress_rsp["code"] == 0, progress_rsp
        assert progress_rsp["data"]["state"] == "Failed", progress_rsp

        nonexistent_commit_rsp = self.import_job_client.commit_import_job("922337203685477000")
        assert nonexistent_commit_rsp["code"] != 0, nonexistent_commit_rsp
        assert "not found" in str(nonexistent_commit_rsp).lower(), nonexistent_commit_rsp

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_failed_job_exposes_actionable_reason_and_no_visible_rows(self):
        """
        target: failed import job observability
        method: import a missing parquet object and inspect progress/detail after Failed
        expected: progress exposes an actionable reason and no rows become visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_failed_reason")
        self._create_base_collection(collection_name)

        missing_file = f"import_2pc_missing_reason_{uuid4()}.parquet"
        job_id = self._create_import_job(
            collection_name,
            missing_file,
            options={"auto_commit": "false"},
        )
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["jobId"] == job_id, rsp
        assert rsp["data"]["state"] == "Failed", rsp

        reason_text = str(rsp["data"].get("reason", ""))
        detail_text = " ".join(str(detail) for detail in rsp["data"].get("details", []))
        combined_reason = f"{reason_text} {detail_text}"
        assert combined_reason.strip(), rsp
        assert missing_file in combined_reason, rsp
        assert any(
            keyword in combined_reason.lower()
            for keyword in ("not found", "no such", "missing", "stat", "exist", "failed")
        ), rsp

        progress_rsp = self.import_job_client.get_import_job_progress(job_id)
        assert progress_rsp["code"] == 0, progress_rsp
        assert progress_rsp["data"]["state"] == "Failed", progress_rsp
        assert str(progress_rsp["data"].get("reason", "") or progress_rsp["data"].get("details", "")), progress_rsp

        self.collection_client.refresh_load(collection_name)
        assert self._query_count(collection_name) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_manual_import_stops_at_uncommitted_and_invisible(self):
        """
        target: manual import lifecycle
        method: create import with auto_commit=false and keep polling before explicit commit
        expected: job remains Uncommitted, imported PKs stay invisible, and normal DML remains available
        """
        collection_name = gen_collection_name(prefix="import_2pc_manual")
        self._create_base_collection(collection_name)

        rows = self._make_rows(4000, 6, phase=4)
        file_name = f"import_2pc_manual_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        imported_ids = {row["id"] for row in rows}
        for _ in range(4):
            progress_rsp = self.import_job_client.get_import_job_progress(job_id)
            assert progress_rsp["code"] == 0, progress_rsp
            assert progress_rsp["data"]["state"] == "Uncommitted", progress_rsp

            seen_ids = self._query_imported_ids(collection_name, sorted(imported_ids))
            assert seen_ids == set(), {"unexpected_visible_ids": seen_ids}
            time.sleep(3)

        dml_row = self._make_rows(9000, 1, phase=90)[0]
        self._insert_one_row(collection_name, dml_row)
        seen_dml_ids, visible = self._wait_imported_ids_visible(collection_name, {dml_row["id"]}, timeout=60)
        assert visible, {"expected_dml_id": dml_row["id"], "seen": seen_dml_ids}

        progress_rsp = self.import_job_client.get_import_job_progress(job_id)
        assert progress_rsp["code"] == 0, progress_rsp
        assert progress_rsp["data"]["state"] == "Uncommitted", progress_rsp
        seen_ids = self._query_imported_ids(collection_name, sorted(imported_ids))
        assert seen_ids == set(), {"unexpected_visible_ids": seen_ids}

        abort_rsp = self.import_job_client.abort_import_job(job_id)
        assert abort_rsp["code"] == 0, abort_rsp

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_manual_commit_completed_visible_and_idempotent(self):
        """
        target: manual import commit lifecycle
        method: create manual import, commit it repeatedly, and verify final query/search visibility
        expected: commit is idempotent, job reaches Completed, and imported rows are eventually visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_commit")
        self._create_base_collection(collection_name)

        rows = self._make_rows(5000, 6, phase=5)
        file_name = f"import_2pc_manual_commit_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp
        assert rsp["data"]["importedRows"] == len(rows), rsp
        assert rsp["data"]["totalRows"] == len(rows), rsp

        expected_ids = {row["id"] for row in rows}
        seen_before_commit = self._query_imported_ids(collection_name, sorted(expected_ids))
        assert seen_before_commit == set(), {"unexpected_visible_ids": seen_before_commit}

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp

        duplicate_commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert duplicate_commit_rsp["code"] == 0, duplicate_commit_rsp

        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        seen_ids, visible = self._wait_imported_ids_visible(collection_name, expected_ids, timeout=120)
        assert visible, {"expected": expected_ids, "seen": seen_ids}

        search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=len(rows))
        assert search_ids & expected_ids, {"expected_any": expected_ids, "search_ids": search_ids}

        post_completed_commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert post_completed_commit_rsp["code"] == 0, post_completed_commit_rsp

        seen_after_idempotent_commit = self._query_imported_ids(collection_name, sorted(expected_ids))
        assert seen_after_idempotent_commit == expected_ids, {
            "expected": expected_ids,
            "seen": seen_after_idempotent_commit,
        }

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_abort_rejects_completed_job(self):
        """
        target: abort terminal committed import job
        method: complete a manual import job, then call abort on the Completed job
        expected: abort is rejected, job remains Completed, and committed rows stay visible
        """
        collection_name = gen_collection_name(prefix="import_2pc_abort_completed")
        self._create_base_collection(collection_name)

        rows = self._make_rows(7000, 6, phase=7)
        file_name = f"import_2pc_abort_completed_{uuid4()}.parquet"
        self._write_parquet_and_upload(rows, file_name)

        job_id = self._create_manual_import_job(collection_name, file_name)
        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        commit_rsp = self.import_job_client.commit_import_job(job_id)
        assert commit_rsp["code"] == 0, commit_rsp

        rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
        assert ok, rsp

        expected_ids = {row["id"] for row in rows}
        seen_before_abort, visible_before_abort = self._wait_imported_ids_visible(
            collection_name, expected_ids, timeout=120
        )
        assert visible_before_abort, {"expected": expected_ids, "seen": seen_before_abort}

        abort_rsp = self.import_job_client.abort_import_job(job_id)
        assert abort_rsp["code"] != 0, abort_rsp

        progress_rsp = self.import_job_client.get_import_job_progress(job_id)
        assert progress_rsp["code"] == 0, progress_rsp
        assert progress_rsp["data"]["state"] == "Completed", progress_rsp

        seen_after_abort = self._query_imported_ids(collection_name, sorted(expected_ids))
        assert seen_after_abort == expected_ids, {
            "expected": expected_ids,
            "seen": seen_after_abort,
        }

        file_path = f"/tmp/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_cdc_manual_commit_primary_secondary_consistent(
        self,
        secondary_endpoint,
        secondary_token,
        secondary_minio_host,
        secondary_bucket_name,
        secondary_root_path,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """
        target: IMP-REP-001 CDC 2PC happy path
        method: run REST manual import on primary with the same file staged in both object stores, then commit
        expected: primary and secondary stay invisible at Uncommitted and expose the same PK set after Completed
        """
        self._require_cdc_rest_env(
            "IMP-REP-001",
            secondary_endpoint,
            secondary_minio_host,
            secondary_bucket_name,
            source_cluster_id,
            target_cluster_id,
        )
        self._apply_cdc_topology(
            secondary_endpoint,
            secondary_token,
            source_cluster_id,
            target_cluster_id,
            pchannel_num,
        )

        secondary_collection_client = CollectionClient(secondary_endpoint, secondary_token)
        secondary_vector_client = VectorClient(secondary_endpoint, secondary_token)
        secondary_import_job_client = ImportJobClient(secondary_endpoint, secondary_token)
        secondary_storage_client = StorageClient(
            f"{secondary_minio_host}:9000",
            "minioadmin",
            "minioadmin",
            secondary_bucket_name,
            secondary_root_path,
        )

        collection_name = gen_collection_name(prefix="import_2pc_cdc_commit")
        rows = self._make_rows(12000, 12, phase=12)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_cdc_manual_commit_{uuid4()}.parquet"
        file_path = None
        job_id = None

        try:
            self._create_base_collection(collection_name, shards_num=4)

            rsp, synced = self._wait_collection_exists_with_client(
                secondary_collection_client, collection_name, timeout=180
            )
            assert synced, rsp

            rsp, loaded = self._wait_collection_loaded_with_client(
                secondary_collection_client, collection_name, timeout=180
            )
            assert loaded, rsp

            file_path = self._write_parquet_and_upload_to_both_clusters(rows, file_name, secondary_storage_client)

            job_id = self._create_manual_import_job(collection_name, file_name)

            primary_rsp, primary_uncommitted = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_uncommitted, primary_rsp
            assert primary_rsp["data"]["importedRows"] == len(rows), primary_rsp

            secondary_rsp, secondary_uncommitted = secondary_import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_uncommitted, secondary_rsp
            assert secondary_rsp["data"]["importedRows"] == len(rows), secondary_rsp

            primary_seen, primary_absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=12)
            assert primary_absent, {"unexpected_primary_ids": primary_seen}

            secondary_seen, secondary_absent = self._wait_imported_ids_absent_with_clients(
                secondary_collection_client,
                secondary_vector_client,
                collection_name,
                expected_ids,
                duration=12,
            )
            assert secondary_absent, {"unexpected_secondary_ids": secondary_seen}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp

            primary_rsp, primary_completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_completed, primary_rsp

            secondary_rsp, secondary_completed = secondary_import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_completed, secondary_rsp

            primary_visible_ids, primary_visible = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=180
            )
            assert primary_visible, {"expected": expected_ids, "seen": primary_visible_ids}

            secondary_visible_ids, secondary_visible = self._wait_imported_ids_visible_with_clients(
                secondary_collection_client,
                secondary_vector_client,
                collection_name,
                expected_ids,
                timeout=180,
            )
            assert secondary_visible, {"expected": expected_ids, "seen": secondary_visible_ids}
            assert primary_visible_ids == secondary_visible_ids == expected_ids

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_cdc_delete_across_commit_boundary_primary_secondary_consistent(
        self,
        secondary_endpoint,
        secondary_token,
        secondary_minio_host,
        secondary_bucket_name,
        secondary_root_path,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """
        target: IMP-REP-002 CDC delete semantics around import commit timestamp
        method: delete one import PK subset before commit and another subset after commit on primary
        expected: pre-commit delete is ignored, post-commit delete applies, and primary/secondary PK sets match
        """
        self._require_cdc_rest_env(
            "IMP-REP-002",
            secondary_endpoint,
            secondary_minio_host,
            secondary_bucket_name,
            source_cluster_id,
            target_cluster_id,
        )
        self._apply_cdc_topology(
            secondary_endpoint,
            secondary_token,
            source_cluster_id,
            target_cluster_id,
            pchannel_num,
        )

        secondary_collection_client = CollectionClient(secondary_endpoint, secondary_token)
        secondary_vector_client = VectorClient(secondary_endpoint, secondary_token)
        secondary_import_job_client = ImportJobClient(secondary_endpoint, secondary_token)
        secondary_storage_client = StorageClient(
            f"{secondary_minio_host}:9000",
            "minioadmin",
            "minioadmin",
            secondary_bucket_name,
            secondary_root_path,
        )

        collection_name = gen_collection_name(prefix="import_2pc_cdc_delete")
        rows = self._make_rows(12100, 9, phase=121)
        all_import_ids = {row["id"] for row in rows}
        pre_commit_delete_ids = {row["id"] for row in rows[:3]}
        post_commit_delete_ids = {row["id"] for row in rows[3:6]}
        expected_after_post_delete = all_import_ids - post_commit_delete_ids
        file_name = f"import_2pc_cdc_delete_{uuid4()}.parquet"
        file_path = None
        job_id = None

        try:
            self._create_base_collection(collection_name, shards_num=4)

            rsp, synced = self._wait_collection_exists_with_client(
                secondary_collection_client, collection_name, timeout=180
            )
            assert synced, rsp
            rsp, loaded = self._wait_collection_loaded_with_client(
                secondary_collection_client, collection_name, timeout=180
            )
            assert loaded, rsp

            file_path = self._write_parquet_and_upload_to_both_clusters(rows, file_name, secondary_storage_client)
            job_id = self._create_manual_import_job(collection_name, file_name)

            primary_rsp, primary_uncommitted = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_uncommitted, primary_rsp
            secondary_rsp, secondary_uncommitted = secondary_import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_uncommitted, secondary_rsp

            self._delete_ids(collection_name, pre_commit_delete_ids)

            primary_seen, primary_absent = self._wait_imported_ids_absent(collection_name, all_import_ids, duration=12)
            assert primary_absent, {"unexpected_primary_ids": primary_seen}
            secondary_seen, secondary_absent = self._wait_imported_ids_absent_with_clients(
                secondary_collection_client,
                secondary_vector_client,
                collection_name,
                all_import_ids,
                duration=12,
            )
            assert secondary_absent, {"unexpected_secondary_ids": secondary_seen}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            primary_rsp, primary_completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_completed, primary_rsp
            secondary_rsp, secondary_completed = secondary_import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_completed, secondary_rsp

            primary_visible_ids, primary_visible = self._wait_imported_ids_visible(
                collection_name, all_import_ids, timeout=180
            )
            assert primary_visible, {"expected": all_import_ids, "seen": primary_visible_ids}
            secondary_visible_ids, secondary_visible = self._wait_imported_ids_visible_with_clients(
                secondary_collection_client,
                secondary_vector_client,
                collection_name,
                all_import_ids,
                timeout=180,
            )
            assert secondary_visible, {"expected": all_import_ids, "seen": secondary_visible_ids}
            assert pre_commit_delete_ids <= primary_visible_ids
            assert pre_commit_delete_ids <= secondary_visible_ids

            self._delete_ids(collection_name, post_commit_delete_ids)

            primary_deleted_seen, primary_deleted_absent = self._wait_imported_ids_eventually_absent(
                collection_name, post_commit_delete_ids, timeout=180
            )
            assert primary_deleted_absent, {"unexpected_primary_deleted_ids": primary_deleted_seen}
            secondary_deleted_seen, secondary_deleted_absent = self._wait_imported_ids_eventually_absent_with_clients(
                secondary_collection_client,
                secondary_vector_client,
                collection_name,
                post_commit_delete_ids,
                timeout=180,
            )
            assert secondary_deleted_absent, {"unexpected_secondary_deleted_ids": secondary_deleted_seen}

            primary_final_ids, primary_final_visible = self._wait_imported_ids_visible(
                collection_name, expected_after_post_delete, timeout=180
            )
            assert primary_final_visible, {"expected": expected_after_post_delete, "seen": primary_final_ids}
            secondary_final_ids, secondary_final_visible = self._wait_imported_ids_visible_with_clients(
                secondary_collection_client,
                secondary_vector_client,
                collection_name,
                expected_after_post_delete,
                timeout=180,
            )
            assert secondary_final_visible, {"expected": expected_after_post_delete, "seen": secondary_final_ids}
            assert primary_final_ids == secondary_final_ids == expected_after_post_delete

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_cdc_ttl_expires_from_commit_timestamp_primary_secondary_consistent(
        self,
        secondary_endpoint,
        secondary_token,
        secondary_minio_host,
        secondary_bucket_name,
        secondary_root_path,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """
        target: IMP-TTL-002 CDC TTL calculation for manually committed import rows
        method: keep import Uncommitted longer than collection TTL, commit on primary, then observe both clusters
        expected: primary and secondary expose rows after commit and expire them from commit timestamp, not row write time
        """
        self._require_cdc_rest_env(
            "IMP-TTL-002",
            secondary_endpoint,
            secondary_minio_host,
            secondary_bucket_name,
            source_cluster_id,
            target_cluster_id,
        )
        self._apply_cdc_topology(
            secondary_endpoint,
            secondary_token,
            source_cluster_id,
            target_cluster_id,
            pchannel_num,
        )
        secondary = self._build_secondary_cdc_clients(
            secondary_endpoint,
            secondary_token,
            secondary_minio_host,
            secondary_bucket_name,
            secondary_root_path,
        )

        collection_name = gen_collection_name(prefix="import_2pc_cdc_ttl")
        ttl_seconds = 30
        rows = self._make_rows(12750, 6, phase=127)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_cdc_ttl_{uuid4()}.parquet"
        file_path = None
        job_id = None

        try:
            self._create_base_collection(collection_name, shards_num=4, ttl_seconds=ttl_seconds)
            rsp, synced = self._wait_collection_exists_with_client(
                secondary["collection"], collection_name, timeout=180
            )
            assert synced, rsp
            rsp, loaded = self._wait_collection_loaded_with_client(
                secondary["collection"], collection_name, timeout=180
            )
            assert loaded, rsp

            file_path = self._write_parquet_and_upload_to_both_clusters(rows, file_name, secondary["storage"])
            job_id = self._create_manual_import_job(collection_name, file_name)

            primary_rsp, primary_uncommitted = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_uncommitted, primary_rsp
            secondary_rsp, secondary_uncommitted = secondary["import_job"].wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_uncommitted, secondary_rsp

            time.sleep(ttl_seconds + 5)
            primary_seen, primary_absent = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=8, interval=2
            )
            assert primary_absent, {"unexpected_primary_ids": primary_seen}
            secondary_seen, secondary_absent = self._wait_imported_ids_absent_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                expected_ids,
                duration=8,
                interval=2,
            )
            assert secondary_absent, {"unexpected_secondary_ids": secondary_seen}

            commit_start = time.time()
            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp

            primary_rsp, primary_completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_completed, primary_rsp
            secondary_rsp, secondary_completed = secondary["import_job"].wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_completed, secondary_rsp

            primary_seen, primary_visible = self._wait_imported_ids_visible(collection_name, expected_ids, timeout=180)
            assert primary_visible, {"expected": expected_ids, "seen": primary_seen}
            secondary_seen, secondary_visible = self._wait_imported_ids_visible_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                expected_ids,
                timeout=180,
            )
            assert secondary_visible, {"expected": expected_ids, "seen": secondary_seen}
            assert primary_seen == secondary_seen == expected_ids

            seconds_until_midpoint = 15 - (time.time() - commit_start)
            if seconds_until_midpoint > 0:
                time.sleep(seconds_until_midpoint)
            assert self._query_imported_ids(collection_name, sorted(expected_ids)) == expected_ids
            assert (
                self._query_imported_ids_with_client(secondary["vector"], collection_name, sorted(expected_ids))
                == expected_ids
            )

            seconds_until_expiry_probe = ttl_seconds + 5 - (time.time() - commit_start)
            if seconds_until_expiry_probe > 0:
                time.sleep(seconds_until_expiry_probe)
            primary_seen, primary_expired = self._wait_imported_ids_eventually_absent(
                collection_name, expected_ids, timeout=60, interval=2
            )
            assert primary_expired, {"expected_absent": expected_ids, "last_primary_seen": primary_seen}
            secondary_seen, secondary_expired = self._wait_imported_ids_eventually_absent_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                expected_ids,
                timeout=60,
                interval=2,
            )
            assert secondary_expired, {"expected_absent": expected_ids, "last_secondary_seen": secondary_seen}

            primary_count, primary_count_ok = self._wait_count(collection_name, 0, timeout=30)
            assert primary_count_ok, {"expected_count": 0, "last_count": primary_count}
            secondary_count, secondary_count_ok = self._wait_count_with_clients(
                secondary["collection"], secondary["vector"], collection_name, 0, timeout=60
            )
            assert secondary_count_ok, {"expected_count": 0, "last_count": secondary_count}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_cdc_abort_before_commit_primary_secondary_cleanup(
        self,
        secondary_endpoint,
        secondary_token,
        secondary_minio_host,
        secondary_bucket_name,
        secondary_root_path,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """
        target: IMP-REP-004 CDC replicated abort cleanup before import commit
        method: abort a replicated manual import from primary after both clusters reach Uncommitted
        expected: both jobs reach Failed and import rows stay invisible on primary and secondary
        """
        self._require_cdc_rest_env(
            "IMP-REP-004",
            secondary_endpoint,
            secondary_minio_host,
            secondary_bucket_name,
            source_cluster_id,
            target_cluster_id,
        )
        self._apply_cdc_topology(
            secondary_endpoint,
            secondary_token,
            source_cluster_id,
            target_cluster_id,
            pchannel_num,
        )
        secondary = self._build_secondary_cdc_clients(
            secondary_endpoint,
            secondary_token,
            secondary_minio_host,
            secondary_bucket_name,
            secondary_root_path,
        )

        collection_name = gen_collection_name(prefix="import_2pc_cdc_abort")
        rows = self._make_rows(12200, 10, phase=122)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_cdc_abort_{uuid4()}.parquet"
        file_path = None
        job_id = None

        try:
            self._create_base_collection(collection_name, shards_num=4)
            rsp, synced = self._wait_collection_exists_with_client(
                secondary["collection"], collection_name, timeout=180
            )
            assert synced, rsp
            rsp, loaded = self._wait_collection_loaded_with_client(
                secondary["collection"], collection_name, timeout=180
            )
            assert loaded, rsp

            file_path = self._write_parquet_and_upload_to_both_clusters(rows, file_name, secondary["storage"])
            job_id = self._create_manual_import_job(collection_name, file_name)

            primary_rsp, primary_uncommitted = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_uncommitted, primary_rsp
            secondary_rsp, secondary_uncommitted = secondary["import_job"].wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_uncommitted, secondary_rsp

            primary_seen, primary_absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=12)
            assert primary_absent, {"unexpected_primary_ids": primary_seen}
            secondary_seen, secondary_absent = self._wait_imported_ids_absent_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                expected_ids,
                duration=12,
            )
            assert secondary_absent, {"unexpected_secondary_ids": secondary_seen}

            abort_rsp = self.import_job_client.abort_import_job(job_id)
            assert abort_rsp["code"] == 0, abort_rsp

            primary_rsp, primary_failed = self.import_job_client.wait_import_job_state(
                job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_failed, primary_rsp
            secondary_rsp, secondary_failed = secondary["import_job"].wait_import_job_state(
                job_id, "Failed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_failed, secondary_rsp

            primary_seen, primary_absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=18)
            assert primary_absent, {"unexpected_primary_ids": primary_seen}
            secondary_seen, secondary_absent = self._wait_imported_ids_absent_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                expected_ids,
                duration=18,
            )
            assert secondary_absent, {"unexpected_secondary_ids": secondary_seen}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_cdc_dml_interleaving_during_uncommitted_primary_secondary_consistent(
        self,
        secondary_endpoint,
        secondary_token,
        secondary_minio_host,
        secondary_bucket_name,
        secondary_root_path,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """
        target: IMP-REP-005 CDC DML availability while import is Uncommitted
        method: insert DML rows before and during a replicated manual import, then commit import
        expected: DML rows replicate normally before commit, import rows appear only after commit, and PK sets match
        """
        self._require_cdc_rest_env(
            "IMP-REP-005",
            secondary_endpoint,
            secondary_minio_host,
            secondary_bucket_name,
            source_cluster_id,
            target_cluster_id,
        )
        self._apply_cdc_topology(
            secondary_endpoint,
            secondary_token,
            source_cluster_id,
            target_cluster_id,
            pchannel_num,
        )
        secondary = self._build_secondary_cdc_clients(
            secondary_endpoint,
            secondary_token,
            secondary_minio_host,
            secondary_bucket_name,
            secondary_root_path,
        )

        collection_name = gen_collection_name(prefix="import_2pc_cdc_dml")
        baseline_rows = self._make_rows(12300, 3, phase=123)
        import_rows = self._make_rows(12350, 10, phase=124)
        during_rows = self._make_rows(12400, 3, phase=125)
        baseline_ids = {row["id"] for row in baseline_rows}
        import_ids = {row["id"] for row in import_rows}
        during_ids = {row["id"] for row in during_rows}
        final_ids = baseline_ids | import_ids | during_ids
        file_name = f"import_2pc_cdc_dml_{uuid4()}.parquet"
        file_path = None
        job_id = None

        try:
            self._create_base_collection(collection_name, shards_num=4)
            rsp, synced = self._wait_collection_exists_with_client(
                secondary["collection"], collection_name, timeout=180
            )
            assert synced, rsp
            rsp, loaded = self._wait_collection_loaded_with_client(
                secondary["collection"], collection_name, timeout=180
            )
            assert loaded, rsp

            self._insert_rows(collection_name, baseline_rows)
            primary_seen, primary_baseline_visible = self._wait_imported_ids_visible(
                collection_name, baseline_ids, timeout=120
            )
            assert primary_baseline_visible, {"expected": baseline_ids, "seen": primary_seen}
            secondary_seen, secondary_baseline_visible = self._wait_imported_ids_visible_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                baseline_ids,
                timeout=180,
            )
            assert secondary_baseline_visible, {"expected": baseline_ids, "seen": secondary_seen}

            file_path = self._write_parquet_and_upload_to_both_clusters(import_rows, file_name, secondary["storage"])
            job_id = self._create_manual_import_job(collection_name, file_name)

            primary_rsp, primary_uncommitted = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_uncommitted, primary_rsp
            secondary_rsp, secondary_uncommitted = secondary["import_job"].wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_uncommitted, secondary_rsp

            self._insert_rows(collection_name, during_rows)

            primary_seen, primary_during_visible = self._wait_imported_ids_visible(
                collection_name, baseline_ids | during_ids, timeout=120
            )
            assert primary_during_visible, {"expected": baseline_ids | during_ids, "seen": primary_seen}
            secondary_seen, secondary_during_visible = self._wait_imported_ids_visible_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                baseline_ids | during_ids,
                timeout=180,
            )
            assert secondary_during_visible, {"expected": baseline_ids | during_ids, "seen": secondary_seen}

            primary_import_seen, primary_import_absent = self._wait_imported_ids_absent(
                collection_name, import_ids, duration=12
            )
            assert primary_import_absent, {"unexpected_primary_import_ids": primary_import_seen}
            secondary_import_seen, secondary_import_absent = self._wait_imported_ids_absent_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                import_ids,
                duration=12,
            )
            assert secondary_import_absent, {"unexpected_secondary_import_ids": secondary_import_seen}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            primary_rsp, primary_completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_completed, primary_rsp
            secondary_rsp, secondary_completed = secondary["import_job"].wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_completed, secondary_rsp

            primary_final_ids, primary_final_visible = self._wait_imported_ids_visible(
                collection_name, final_ids, timeout=180
            )
            assert primary_final_visible, {"expected": final_ids, "seen": primary_final_ids}
            secondary_final_ids, secondary_final_visible = self._wait_imported_ids_visible_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                final_ids,
                timeout=180,
            )
            assert secondary_final_visible, {"expected": final_ids, "seen": secondary_final_ids}
            assert primary_final_ids == secondary_final_ids == final_ids

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_cdc_commit_idempotent_primary_secondary_no_duplicate_rows(
        self,
        secondary_endpoint,
        secondary_token,
        secondary_minio_host,
        secondary_bucket_name,
        secondary_root_path,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """
        target: IMP-REP-006 CDC CommitImport idempotency
        method: call primary commit repeatedly before and after Completed
        expected: duplicate commits succeed without duplicating rows, and primary/secondary counts and PK sets match
        """
        self._require_cdc_rest_env(
            "IMP-REP-006",
            secondary_endpoint,
            secondary_minio_host,
            secondary_bucket_name,
            source_cluster_id,
            target_cluster_id,
        )
        self._apply_cdc_topology(
            secondary_endpoint,
            secondary_token,
            source_cluster_id,
            target_cluster_id,
            pchannel_num,
        )
        secondary = self._build_secondary_cdc_clients(
            secondary_endpoint,
            secondary_token,
            secondary_minio_host,
            secondary_bucket_name,
            secondary_root_path,
        )

        collection_name = gen_collection_name(prefix="import_2pc_cdc_idempotent")
        rows = self._make_rows(12500, 10, phase=126)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_cdc_idempotent_{uuid4()}.parquet"
        file_path = None
        job_id = None

        try:
            self._create_base_collection(collection_name, shards_num=4)
            rsp, synced = self._wait_collection_exists_with_client(
                secondary["collection"], collection_name, timeout=180
            )
            assert synced, rsp
            rsp, loaded = self._wait_collection_loaded_with_client(
                secondary["collection"], collection_name, timeout=180
            )
            assert loaded, rsp

            file_path = self._write_parquet_and_upload_to_both_clusters(rows, file_name, secondary["storage"])
            job_id = self._create_manual_import_job(collection_name, file_name)

            primary_rsp, primary_uncommitted = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_uncommitted, primary_rsp
            secondary_rsp, secondary_uncommitted = secondary["import_job"].wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_uncommitted, secondary_rsp

            first_commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert first_commit_rsp["code"] == 0, first_commit_rsp
            duplicate_commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert duplicate_commit_rsp["code"] == 0, duplicate_commit_rsp

            primary_rsp, primary_completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_completed, primary_rsp
            secondary_rsp, secondary_completed = secondary["import_job"].wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_completed, secondary_rsp

            completed_commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert completed_commit_rsp["code"] == 0, completed_commit_rsp

            primary_seen, primary_visible = self._wait_imported_ids_visible(collection_name, expected_ids, timeout=180)
            assert primary_visible, {"expected": expected_ids, "seen": primary_seen}
            secondary_seen, secondary_visible = self._wait_imported_ids_visible_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                expected_ids,
                timeout=180,
            )
            assert secondary_visible, {"expected": expected_ids, "seen": secondary_seen}
            assert primary_seen == secondary_seen == expected_ids

            primary_count, primary_count_ok = self._wait_count(collection_name, len(expected_ids), timeout=120)
            assert primary_count_ok, {"expected_count": len(expected_ids), "last_count": primary_count}
            secondary_count, secondary_count_ok = self._wait_count_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                len(expected_ids),
                timeout=180,
            )
            assert secondary_count_ok, {"expected_count": len(expected_ids), "last_count": secondary_count}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_cdc_secondary_direct_import_commit_abort_rejected(
        self,
        secondary_endpoint,
        secondary_token,
        secondary_minio_host,
        secondary_bucket_name,
        secondary_root_path,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """
        target: IMP-REP-003/009 standby write-role guard for import 2PC REST APIs
        method: create a replicated manual import, then call create/commit/abort directly on secondary
        expected: secondary direct writes are rejected, primary commit still replicates, and final PK sets match
        """
        self._require_cdc_rest_env(
            "IMP-REP-003",
            secondary_endpoint,
            secondary_minio_host,
            secondary_bucket_name,
            source_cluster_id,
            target_cluster_id,
        )
        self._apply_cdc_topology(
            secondary_endpoint,
            secondary_token,
            source_cluster_id,
            target_cluster_id,
            pchannel_num,
        )
        secondary = self._build_secondary_cdc_clients(
            secondary_endpoint,
            secondary_token,
            secondary_minio_host,
            secondary_bucket_name,
            secondary_root_path,
        )

        collection_name = gen_collection_name(prefix="import_2pc_cdc_secondary_guard")
        primary_rows = self._make_rows(12600, 8, phase=126)
        secondary_direct_rows = self._make_rows(12650, 4, phase=127)
        expected_ids = {row["id"] for row in primary_rows}
        secondary_direct_ids = {row["id"] for row in secondary_direct_rows}
        primary_file_name = f"import_2pc_cdc_secondary_guard_primary_{uuid4()}.parquet"
        secondary_file_name = f"import_2pc_cdc_secondary_guard_direct_{uuid4()}.parquet"
        local_files = []
        job_id = None
        unexpected_secondary_job_id = None

        try:
            self._create_base_collection(collection_name, shards_num=4)
            rsp, synced = self._wait_collection_exists_with_client(
                secondary["collection"], collection_name, timeout=180
            )
            assert synced, rsp
            rsp, loaded = self._wait_collection_loaded_with_client(
                secondary["collection"], collection_name, timeout=180
            )
            assert loaded, rsp

            local_files.append(
                self._write_parquet_and_upload_to_both_clusters(primary_rows, primary_file_name, secondary["storage"])
            )
            local_files.append(
                self._write_parquet_and_upload_to_both_clusters(
                    secondary_direct_rows, secondary_file_name, secondary["storage"]
                )
            )

            job_id = self._create_manual_import_job(collection_name, primary_file_name)
            primary_rsp, primary_uncommitted = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_uncommitted, primary_rsp
            secondary_rsp, secondary_uncommitted = secondary["import_job"].wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_uncommitted, secondary_rsp

            secondary_create_rsp = secondary["import_job"].create_import_jobs(
                {
                    "collectionName": collection_name,
                    "files": [[secondary_file_name]],
                    "options": {"auto_commit": "false"},
                }
            )
            if secondary_create_rsp.get("code") == 0:
                unexpected_secondary_job_id = secondary_create_rsp["data"]["jobId"]
            assert secondary_create_rsp.get("code") != 0, secondary_create_rsp
            assert "jobId" not in str(secondary_create_rsp), secondary_create_rsp

            secondary_commit_rsp = secondary["import_job"].commit_import_job(job_id)
            assert secondary_commit_rsp.get("code") != 0, secondary_commit_rsp
            secondary_abort_rsp = secondary["import_job"].abort_import_job(job_id)
            assert secondary_abort_rsp.get("code") != 0, secondary_abort_rsp

            primary_rsp = self.import_job_client.get_import_job_progress(job_id)
            secondary_rsp = secondary["import_job"].get_import_job_progress(job_id)
            assert primary_rsp["code"] == 0 and primary_rsp["data"]["state"] == "Uncommitted", {
                "primary_rsp": primary_rsp,
                "secondary_commit_rsp": secondary_commit_rsp,
                "secondary_abort_rsp": secondary_abort_rsp,
            }
            assert secondary_rsp["code"] == 0 and secondary_rsp["data"]["state"] == "Uncommitted", {
                "secondary_rsp": secondary_rsp,
                "secondary_commit_rsp": secondary_commit_rsp,
                "secondary_abort_rsp": secondary_abort_rsp,
            }

            primary_seen, primary_absent = self._wait_imported_ids_absent(collection_name, expected_ids, duration=12)
            assert primary_absent, {"unexpected_primary_ids": primary_seen}
            secondary_seen, secondary_absent = self._wait_imported_ids_absent_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                expected_ids | secondary_direct_ids,
                duration=12,
            )
            assert secondary_absent, {"unexpected_secondary_ids": secondary_seen}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            primary_rsp, primary_completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_completed, primary_rsp
            secondary_rsp, secondary_completed = secondary["import_job"].wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_completed, secondary_rsp

            primary_final_ids, primary_visible = self._wait_imported_ids_visible(
                collection_name, expected_ids, timeout=180
            )
            assert primary_visible, {"expected": expected_ids, "seen": primary_final_ids}
            secondary_final_ids, secondary_visible = self._wait_imported_ids_visible_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                expected_ids,
                timeout=180,
            )
            assert secondary_visible, {"expected": expected_ids, "seen": secondary_final_ids}
            assert primary_final_ids == secondary_final_ids == expected_ids
            assert self._query_imported_ids(collection_name, sorted(secondary_direct_ids)) == set()
            assert (
                self._query_imported_ids_with_client(secondary["vector"], collection_name, sorted(secondary_direct_ids))
                == set()
            )
        finally:
            if unexpected_secondary_job_id is not None:
                secondary["import_job"].abort_import_job(unexpected_secondary_job_id)
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            for file_path in local_files:
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_cdc_commit_while_secondary_import_still_building_requires_cdc_env(self):
        """
        target: IMP-REP-101 environment precondition
        method: verify the current endpoint is a primary/secondary CDC deployment before running the CDC scenario
        expected: current standalone instance is skipped, not treated as a Milvus failure
        """
        self._skip_cdc_case_on_single_cluster_instance("IMP-REP-101")

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_cdc_commit_message_replay_requires_cdc_env(self):
        """
        target: IMP-REP-102 environment precondition
        method: verify the current endpoint is a primary/secondary CDC deployment before pausing secondary CDC delivery
        expected: current standalone instance is skipped, not treated as a Milvus failure
        """
        self._skip_cdc_case_on_single_cluster_instance("IMP-REP-102")

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_cdc_abort_replication_requires_cdc_env(self):
        """
        target: IMP-REP-103 environment precondition
        method: verify the current endpoint is a primary/secondary CDC deployment before validating replicated abort cleanup
        expected: current standalone instance is skipped, not treated as a Milvus failure
        """
        self._skip_cdc_case_on_single_cluster_instance("IMP-REP-103")

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_mixcoord_restart_in_uncommitted_allows_commit(self, release_name):
        """
        target: IMP-FT-001 DataCoord metadata recovery while import job is Uncommitted
        method: restart the MixCoord pod after manual import reaches Uncommitted, then commit the job
        expected: job state recovers after restart, commit succeeds, and imported rows become visible
        """
        self._skip_if_release_has_no_component(release_name, "mixcoord", "IMP-FT-001")

        collection_name = gen_collection_name(prefix="import_2pc_mixcoord_restart")
        self._create_base_collection(collection_name)

        rows = self._make_rows(13000, 12, phase=13)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_mixcoord_restart_{uuid4()}.parquet"
        file_path = None
        job_id = None

        try:
            file_path = self._write_parquet_and_upload(rows, file_name)
            job_id = self._create_manual_import_job(collection_name, file_name)

            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp

            seen_before_restart, absent_before_restart = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_restart, {"unexpected_visible_ids": seen_before_restart}

            old_pod, new_pod = self._kubectl_delete_one_component_pod_and_wait_ready(
                release_name, "mixcoord", timeout=300
            )
            assert old_pod != new_pod
            cr_status, healthy = self._wait_milvus_cr_healthy(release_name, timeout=300)
            assert healthy, cr_status

            rsp, recovered = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert recovered, rsp

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp

            rsp, completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert completed, rsp

            seen_ids, visible = self._wait_imported_ids_visible(collection_name, expected_ids, timeout=180)
            assert visible, {"expected": expected_ids, "seen": seen_ids}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    @pytest.mark.tags(CaseLabel.L0)
    def test_import_2pc_datanode_restart_during_importing_recovers_to_uncommitted(self, release_name):
        """
        target: IMP-FT-003 DataNode recovery while a manual import is being processed
        method: start a large manual import, restart a DataNode pod during scheduling/importing, then commit
        expected: job recovers to Uncommitted, commit succeeds, and imported rows become visible
        """
        self._skip_if_release_has_no_component(release_name, "datanode", "IMP-FT-003")

        collection_name = gen_collection_name(prefix="import_2pc_datanode_restart")
        self._create_base_collection(collection_name, shards_num=4)

        rows = self._make_rows(14000, 20000, phase=140)
        expected_ids = {row["id"] for row in rows[:12] + rows[-12:]}
        file_name = f"import_2pc_datanode_restart_{uuid4()}.parquet"
        file_path = None
        job_id = None

        try:
            file_path = self._write_parquet_and_upload(rows, file_name)
            job_id = self._create_manual_import_job(collection_name, file_name)

            old_pod, new_pod = self._kubectl_delete_one_component_pod_and_wait_ready(
                release_name, "datanode", timeout=300
            )
            assert old_pod != new_pod
            cr_status, healthy = self._wait_milvus_cr_healthy(release_name, timeout=300)
            assert healthy, cr_status

            rsp, ok = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ok, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp

            seen_before_commit, absent_before_commit = self._wait_imported_ids_absent(
                collection_name, expected_ids, duration=12
            )
            assert absent_before_commit, {"unexpected_visible_ids": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp

            rsp, completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert completed, rsp

            seen_ids, visible = self._wait_imported_ids_visible(collection_name, expected_ids, timeout=180)
            assert visible, {"expected": expected_ids, "seen": seen_ids}
            final_count, final_count_ok = self._wait_count(collection_name, len(rows), timeout=180)
            assert final_count_ok, {"expected_count": len(rows), "last_count": final_count}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
