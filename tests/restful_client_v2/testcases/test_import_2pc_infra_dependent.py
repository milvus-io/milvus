import json
import os
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, wait
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from api.milvus import CollectionClient, ImportJobClient, StorageClient, VectorClient
from base.testbase import TestBase
from pymilvus import MilvusClient
from utils.utils import gen_collection_name


IMPORT_2PC_TIMEOUT = 360


class Import2PCInfraBase(TestBase):
    def _make_rows(self, start_id, count, phase, dim=8):
        return [
            {
                "id": start_id + i,
                "tag": f"import_2pc_infra_{phase}_{i}",
                "phase": phase,
                "vector": [float((start_id + i + j) % 17) / 17 for j in range(dim)],
            }
            for i in range(count)
        ]

    def _create_base_collection(self, name, dim=8, shards_num=None, ttl_seconds=None):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "96"}},
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
        if params:
            payload["params"] = params
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=120)

    def _create_collection_from_fixture(self, collection_name, fixture):
        payload = fixture.get("collectionPayload")
        if payload is None:
            self._create_base_collection(collection_name)
            return
        payload = dict(payload)
        payload["collectionName"] = collection_name
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(collection_name, timeout=120)

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
        self._upload_file_or_fail(self.storage_client, file_path, file_name)
        return file_path

    def _write_parquet_and_upload_to_both_clusters(self, rows, file_name, secondary_storage_client):
        file_path = self._write_parquet_and_upload(rows, file_name)
        self._upload_file_or_fail(secondary_storage_client, file_path, file_name)
        return file_path

    def _upload_file_or_fail(self, storage_client, file_path, object_name):
        storage_client.client.fput_object(storage_client.bucket_name, object_name, file_path)
        storage_client.client.stat_object(storage_client.bucket_name, object_name)

    def _insert_rows(self, collection_name, rows):
        rsp = self.vector_client.vector_insert({"collectionName": collection_name, "data": rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == len(rows), rsp

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

    def _get_collection_id(self, collection_name):
        rsp = self.collection_client.collection_describe(collection_name)
        assert rsp["code"] == 0, rsp
        collection_id = rsp.get("data", {}).get("collectionID")
        assert collection_id is not None, rsp
        return collection_id

    def _list_storage_objects(self, prefix):
        return sorted(
            obj.object_name
            for obj in self.storage_client.client.list_objects(
                self.storage_client.bucket_name,
                prefix=prefix,
                recursive=True,
            )
        )

    def _wait_storage_prefix_non_empty(self, prefix, timeout=120):
        t0 = time.time()
        objects = []
        while time.time() - t0 < timeout:
            objects = self._list_storage_objects(prefix)
            if objects:
                return objects, True
            time.sleep(2)
        return objects, False

    def _wait_storage_log_prefix_non_empty(self, log_kind, collection_id, timeout=120):
        roots = []
        configured_root = (self.storage_client.root_path or "").strip("/")
        for root in (configured_root, "files", ""):
            if root not in roots:
                roots.append(root)

        t0 = time.time()
        last = {}
        while time.time() - t0 < timeout:
            for root in roots:
                prefix = f"{root}/{log_kind}/{collection_id}/" if root else f"{log_kind}/{collection_id}/"
                objects = self._list_storage_objects(prefix)
                last[prefix] = objects[:5]
                if objects:
                    return prefix, objects, True
            time.sleep(2)
        return None, last, False

    def _select_insert_partition_prefix(self, insert_objects):
        partition_segments = {}
        for object_name in insert_objects:
            parts = object_name.split("/")
            if "insert_log" not in parts:
                continue
            idx = parts.index("insert_log")
            if len(parts) < idx + 6:
                continue
            partition_prefix = "/".join(parts[: idx + 3]) + "/"
            segment_prefix = "/".join(parts[: idx + 4]) + "/"
            group_id = parts[idx + 4]
            partition_segments.setdefault(partition_prefix, {}).setdefault(segment_prefix, set()).add(group_id)

        complete_partitions = []
        for partition_prefix, segments in partition_segments.items():
            complete_segments = [
                segment_prefix
                for segment_prefix, groups in segments.items()
                if "0" in groups and len(groups) >= 2
            ]
            if complete_segments:
                complete_partitions.append((partition_prefix, len(complete_segments)))

        assert complete_partitions, {
            "reason": "no partition with complete insert segment found",
            "partition_segments": {
                partition: {segment: sorted(groups) for segment, groups in segments.items()}
                for partition, segments in partition_segments.items()
            },
        }
        return sorted(complete_partitions)[-1][0]

    def _select_insert_segment_prefix(self, insert_objects):
        segment_groups = {}
        for object_name in insert_objects:
            parts = object_name.split("/")
            if "insert_log" not in parts:
                continue
            idx = parts.index("insert_log")
            if len(parts) < idx + 6:
                continue
            segment_prefix = "/".join(parts[: idx + 4]) + "/"
            group_id = parts[idx + 4]
            segment_groups.setdefault(segment_prefix, set()).add(group_id)

        complete_segments = [
            segment_prefix
            for segment_prefix, groups in segment_groups.items()
            if "0" in groups and len(groups) >= 2
        ]
        assert complete_segments, {
            "reason": "no complete insert segment found",
            "segment_groups": {segment: sorted(groups) for segment, groups in segment_groups.items()},
        }
        return sorted(complete_segments)[-1]

    def _select_manifest_segment_prefix(self, insert_objects):
        segment_groups = {}
        for object_name in insert_objects:
            parts = object_name.split("/")
            if "insert_log" not in parts:
                continue
            idx = parts.index("insert_log")
            if len(parts) < idx + 6:
                continue
            segment_prefix = "/".join(parts[: idx + 4]) + "/"
            group_id = parts[idx + 4]
            segment_groups.setdefault(segment_prefix, set()).add(group_id)

        manifest_segments = [
            segment_prefix
            for segment_prefix, groups in segment_groups.items()
            if "_data" in groups and "_metadata" in groups
        ]
        assert manifest_segments, {
            "reason": "no manifest-layout insert segment found",
            "segment_groups": {segment: sorted(groups) for segment, groups in segment_groups.items()},
        }
        return sorted(manifest_segments)[-1]

    def _build_generated_backup_fixture(self, source_collection_name, rows):
        self._create_base_collection(source_collection_name)
        self._insert_rows(source_collection_name, rows)
        flush_rsp, flushed = self._flush_collection_with_retry(source_collection_name)
        assert flushed, flush_rsp

        source_collection_id = self._get_collection_id(source_collection_name)
        insert_prefix, insert_objects, has_insert = self._wait_storage_log_prefix_non_empty(
            "insert_log", source_collection_id
        )
        assert has_insert, {"collection_id": source_collection_id, "checked_prefixes": insert_objects}
        partition_prefix = self._select_insert_partition_prefix(insert_objects)
        storage_version = int(os.getenv("IMPORT_2PC_GENERATED_STORAGE_VERSION", "2"))
        return {
            "files": [[partition_prefix]],
            "storageVersion": storage_version,
            "partitionName": "_default",
            "expectedIds": [row["id"] for row in rows],
            "expectedCount": None,
            "expectedCountFromJob": True,
        }

    def _build_generated_l0_fixture(self, source_collection_name, seed_rows, delete_ids):
        self._create_base_collection(source_collection_name)
        self._insert_rows(source_collection_name, seed_rows)
        flush_rsp, flushed = self._flush_collection_with_retry(source_collection_name)
        assert flushed, flush_rsp

        delete_rsp = self.vector_client.vector_delete(
            {
                "collectionName": source_collection_name,
                "filter": f"id in {sorted(delete_ids)}",
            }
        )
        assert delete_rsp["code"] == 0, delete_rsp
        flush_rsp, flushed = self._flush_collection_with_retry(source_collection_name)
        assert flushed, flush_rsp

        source_collection_id = self._get_collection_id(source_collection_name)
        delta_prefix, delta_objects, has_delta = self._wait_storage_log_prefix_non_empty(
            "delta_log", source_collection_id
        )
        assert has_delta, {"collection_id": source_collection_id, "checked_prefixes": delta_objects}
        return {
            "files": [[delta_prefix]],
            "storageVersion": 2,
            "seedRows": seed_rows,
            "deleteIds": sorted(delete_ids),
            "partitionName": None,
        }

    def _create_manual_import_job(self, collection_name, file_name, options=None, partition_name=None):
        payload = {
            "collectionName": collection_name,
            "files": [[file_name]],
            "options": {"auto_commit": "false"},
        }
        if options:
            payload["options"].update(options)
        if partition_name is not None:
            payload["partitionName"] = partition_name
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp["code"] == 0, rsp
        return rsp["data"]["jobId"]

    def _create_manual_import_job_with_files(self, collection_name, files, options, partition_name=None):
        payload = {
            "collectionName": collection_name,
            "files": self._normalize_files(files),
            "options": {"auto_commit": "false", **options},
        }
        if partition_name is not None:
            payload["partitionName"] = partition_name
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp["code"] == 0, rsp
        return rsp["data"]["jobId"]

    def _normalize_files(self, files):
        assert files, "fixture files must not be empty"
        if all(isinstance(item, str) for item in files):
            return [[item] for item in files]
        return files

    def _query_imported_ids(self, collection_name, ids):
        if not ids:
            return set()
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {sorted(ids)}",
            "outputFields": ["id"],
            "limit": len(ids),
        }
        rsp = self.vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _query_imported_ids_with_client(self, vector_client, collection_name, ids):
        if not ids:
            return set()
        payload = {
            "collectionName": collection_name,
            "filter": f"id in {sorted(ids)}",
            "outputFields": ["id"],
            "limit": len(ids),
        }
        rsp = vector_client.vector_query(payload, timeout=1)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _query_count(self, collection_name, filter_expr=" "):
        payload = {
            "collectionName": collection_name,
            "filter": filter_expr,
            "limit": 0,
            "outputFields": ["count(*)"],
        }
        rsp = self.vector_client.vector_query(payload, timeout=30)
        assert rsp["code"] == 0, rsp
        assert len(rsp.get("data", [])) == 1, rsp
        return rsp["data"][0]["count(*)"]

    def _query_count_with_client(self, vector_client, collection_name, filter_expr=" "):
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

    def _search_imported_ids(self, collection_name, vector, limit):
        payload = {
            "collectionName": collection_name,
            "data": [vector],
            "annsField": "vector",
            "limit": limit,
            "outputFields": ["id"],
        }
        rsp = self.vector_client.vector_search(payload, timeout=30)
        assert rsp["code"] == 0, rsp
        return {row["id"] for row in rsp.get("data", [])}

    def _wait_imported_ids_visible(self, collection_name, expected_ids, timeout=180):
        expected = set(expected_ids)
        t0 = time.time()
        last_seen = set()
        while time.time() - t0 < timeout:
            self.collection_client.refresh_load(collection_name)
            last_seen = self._query_imported_ids(collection_name, expected)
            if last_seen == expected:
                return last_seen, True
            time.sleep(2)
        return last_seen, False

    def _wait_imported_ids_visible_with_clients(
        self,
        collection_client,
        vector_client,
        collection_name,
        expected_ids,
        timeout=180,
    ):
        expected = set(expected_ids)
        t0 = time.time()
        last_seen = set()
        while time.time() - t0 < timeout:
            collection_client.refresh_load(collection_name)
            last_seen = self._query_imported_ids_with_client(vector_client, collection_name, expected)
            if last_seen == expected:
                return last_seen, True
            time.sleep(2)
        return last_seen, False

    def _wait_imported_ids_absent(self, collection_name, absent_ids, timeout=120):
        absent = set(absent_ids)
        t0 = time.time()
        last_seen = set(absent)
        while time.time() - t0 < timeout:
            self.collection_client.refresh_load(collection_name)
            last_seen = self._query_imported_ids(collection_name, absent)
            if not last_seen:
                return last_seen, True
            time.sleep(2)
        return last_seen, False

    def _wait_count(self, collection_name, expected_count, timeout=180):
        t0 = time.time()
        last_count = None
        while time.time() - t0 < timeout:
            self.collection_client.refresh_load(collection_name)
            last_count = self._query_count(collection_name)
            if last_count == expected_count:
                return last_count, True
            time.sleep(2)
        return last_count, False

    def _wait_count_with_clients(self, collection_client, vector_client, collection_name, expected_count, timeout=180):
        t0 = time.time()
        last_count = None
        while time.time() - t0 < timeout:
            collection_client.refresh_load(collection_name)
            last_count = self._query_count_with_client(vector_client, collection_name)
            if last_count == expected_count:
                return last_count, True
            time.sleep(2)
        return last_count, False

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

    def _wait_disk_quota_write_denied(self, collection_name, timeout=120):
        t0 = time.time()
        last_rsp = None
        attempt = 0
        while time.time() - t0 < timeout:
            row = self._make_rows(-251000 - attempt, 1, phase=251)[0]
            last_rsp = self.vector_client.vector_insert({"collectionName": collection_name, "data": [row]})
            if last_rsp.get("code") != 0:
                reason = str(last_rsp.get("message", last_rsp)).lower()
                if "quota" in reason or "disk" in reason:
                    return last_rsp, True
                return last_rsp, False
            attempt += 1
            time.sleep(3)
        return last_rsp, False

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
        assert not missing, f"{case_id} requires CDC REST options: {', '.join(missing)}"

    def _apply_cdc_topology(self, secondary_endpoint, secondary_token, source_cluster_id, target_cluster_id, pchannel_num):
        config = {
            "clusters": [
                {
                    "cluster_id": source_cluster_id,
                    "connection_param": {"uri": self.endpoint, "token": self.api_key},
                    "pchannels": [f"{source_cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)],
                },
                {
                    "cluster_id": target_cluster_id,
                    "connection_param": {"uri": secondary_endpoint, "token": secondary_token},
                    "pchannels": [f"{target_cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)],
                },
            ],
            "cross_cluster_topology": [
                {"source_cluster_id": source_cluster_id, "target_cluster_id": target_cluster_id}
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

    def _build_fixture_source_clients(self, endpoint, token, minio_host, bucket_name, root_path):
        minio_endpoint = minio_host if ":" in minio_host else f"{minio_host}:9000"
        return {
            "collection": CollectionClient(endpoint, token),
            "vector": VectorClient(endpoint, token),
            "storage": StorageClient(
                minio_endpoint,
                "minioadmin",
                "minioadmin",
                bucket_name,
                root_path,
            ),
        }

    def _create_base_collection_with_clients(self, collection_client, collection_name, dim=8):
        payload = {
            "collectionName": collection_name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "96"}},
                    {"fieldName": "phase", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        rsp = collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        collection_client.wait_load_completed(collection_name, timeout=120)

    def _insert_rows_with_client(self, vector_client, collection_name, rows):
        rsp = vector_client.vector_insert({"collectionName": collection_name, "data": rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == len(rows), rsp

    def _flush_collection_with_client_retry(self, collection_client, collection_name, timeout=90, interval=11):
        t0 = time.time()
        last_rsp = None
        while time.time() - t0 < timeout:
            last_rsp = collection_client.flush(collection_name)
            if last_rsp["code"] == 0:
                return last_rsp, True
            if last_rsp["code"] != 1807:
                return last_rsp, False
            time.sleep(interval)
        return last_rsp, False

    def _get_collection_id_with_client(self, collection_client, collection_name):
        rsp = collection_client.collection_describe(collection_name)
        assert rsp["code"] == 0, rsp
        collection_id = rsp.get("data", {}).get("collectionID")
        assert collection_id is not None, rsp
        return collection_id

    def _list_storage_objects_with_client(self, storage_client, prefix):
        return sorted(
            obj.object_name
            for obj in storage_client.client.list_objects(
                storage_client.bucket_name,
                prefix=prefix,
                recursive=True,
            )
        )

    def _wait_storage_log_prefix_non_empty_with_client(self, storage_client, log_kind, collection_id, timeout=120):
        roots = []
        configured_root = (storage_client.root_path or "").strip("/")
        for root in (configured_root, "files", "file", ""):
            if root not in roots:
                roots.append(root)

        t0 = time.time()
        last = {}
        while time.time() - t0 < timeout:
            for root in roots:
                prefix = f"{root}/{log_kind}/{collection_id}/" if root else f"{log_kind}/{collection_id}/"
                objects = self._list_storage_objects_with_client(storage_client, prefix)
                last[prefix] = objects[:5]
                if objects:
                    return prefix, objects, True
            time.sleep(2)
        return None, last, False

    def _copy_storage_prefix_between_clients(self, source_storage, target_storage, source_prefix, target_prefix):
        source_prefix = source_prefix.rstrip("/") + "/"
        target_prefix = target_prefix.rstrip("/") + "/"
        objects = list(
            source_storage.client.list_objects(
                source_storage.bucket_name,
                prefix=source_prefix,
                recursive=True,
            )
        )
        assert objects, {"source_bucket": source_storage.bucket_name, "source_prefix": source_prefix}
        if not target_storage.client.bucket_exists(target_storage.bucket_name):
            target_storage.client.make_bucket(target_storage.bucket_name)
        copied = []
        for obj in objects:
            relative_path = obj.object_name[len(source_prefix) :]
            target_object = f"{target_prefix}{relative_path}"
            response = source_storage.client.get_object(source_storage.bucket_name, obj.object_name)
            try:
                target_storage.client.put_object(
                    target_storage.bucket_name,
                    target_object,
                    response,
                    length=obj.size,
                )
            finally:
                response.close()
                response.release_conn()
            copied.append(target_object)
        return sorted(copied)

    def _delete_storage_prefix(self, storage_client, prefix):
        objects = list(
            storage_client.client.list_objects(
                storage_client.bucket_name,
                prefix=prefix.rstrip("/") + "/",
                recursive=True,
            )
        )
        for obj in objects:
            storage_client.client.remove_object(storage_client.bucket_name, obj.object_name)

    def _kubectl_json(self, args, namespace="chaos-testing", timeout=30, fail_message="kubectl command failed"):
        if not shutil.which("kubectl"):
            pytest.fail("kubectl is required for import 2PC infra-dependent tests")
        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        result = subprocess.run(
            ["kubectl", *args],
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout,
            check=False,
        )
        if result.returncode != 0:
            pytest.fail(f"{fail_message}: stdout={result.stdout.strip()} stderr={result.stderr.strip()}")
        if not result.stdout.strip():
            return {}
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            pytest.fail(f"{fail_message}: non-json output={result.stdout!r}, error={exc}")

    def _kubectl_get_milvus_cr_or_none(self, release_name, namespace="chaos-testing"):
        if not shutil.which("kubectl"):
            pytest.fail("kubectl is required for import 2PC infra-dependent tests")
        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        result = subprocess.run(
            ["kubectl", "get", "milvus", "-n", namespace, release_name, "-o", "json"],
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)

    def _kubectl_apply_manifest(self, manifest, timeout=60):
        if not shutil.which("kubectl"):
            pytest.fail("kubectl is required for import 2PC infra-dependent tests")
        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as manifest_file:
            json.dump(manifest, manifest_file)
            manifest_path = manifest_file.name
        try:
            result = subprocess.run(
                ["kubectl", "apply", "-f", manifest_path],
                capture_output=True,
                text=True,
                env=env,
                timeout=timeout,
                check=False,
            )
        finally:
            os.remove(manifest_path)
        assert result.returncode == 0, {"stdout": result.stdout, "stderr": result.stderr, "manifest": manifest}
        return result

    def _kubectl_wait_service_lb_ip(self, service_name, namespace="chaos-testing", timeout=600):
        deadline = time.time() + timeout
        last = {}
        while time.time() < deadline:
            service = self._kubectl_json(
                ["get", "svc", "-n", namespace, service_name, "-o", "json"],
                namespace=namespace,
                timeout=30,
                fail_message=f"cannot inspect service {service_name}",
            )
            ingress = service.get("status", {}).get("loadBalancer", {}).get("ingress", [])
            if ingress:
                host = ingress[0].get("ip") or ingress[0].get("hostname")
                if host:
                    return host
            last = service
            time.sleep(5)
        pytest.fail(f"service {service_name} did not get a LoadBalancer address: {last}")

    def _kubectl_get_release_image(self, release_name, namespace="chaos-testing"):
        cr = self._kubectl_get_milvus_cr_or_none(release_name, namespace=namespace)
        assert cr is not None, f"target release {release_name} does not exist in namespace {namespace}"
        components = cr.get("spec", {}).get("components", {})
        image = components.get("image")
        version = components.get("version") or cr.get("status", {}).get("currentVersion") or "2.7.0"
        assert image, cr
        return image, version

    def _kubectl_deploy_fixture_source_release(
        self,
        release_name,
        image,
        version,
        namespace="chaos-testing",
        use_loon_ffi=None,
    ):
        config = {"common": {"security": {"authorizationEnabled": True}}}
        if use_loon_ffi is not None:
            config["common"]["storage"] = {"useLoonFFI": bool(use_loon_ffi)}
        manifest = {
            "apiVersion": "milvus.io/v1beta1",
            "kind": "Milvus",
            "metadata": {
                "name": release_name,
                "namespace": namespace,
                "labels": {"app": "milvus", "purpose": "import-2pc-storage-fixture"},
            },
            "spec": {
                "mode": "standalone",
                "components": {
                    "image": image,
                    "version": version,
                    "standalone": {
                        "replicas": 1,
                        "serviceType": "LoadBalancer",
                        "resources": {
                            "requests": {"cpu": "2", "memory": "8Gi"},
                            "limits": {"cpu": "2", "memory": "8Gi"},
                        },
                    },
                },
                "dependencies": {
                    "etcd": {
                        "inCluster": {
                            "deletionPolicy": "Delete",
                            "pvcDeletion": True,
                            "values": {"replicaCount": 1, "service": {"type": "LoadBalancer"}},
                        }
                    },
                    "storage": {
                        "inCluster": {
                            "deletionPolicy": "Delete",
                            "pvcDeletion": True,
                            "values": {
                                "mode": "standalone",
                                "service": {"type": "LoadBalancer"},
                                "persistence": {"size": "20Gi"},
                                "resources": {"requests": {"memory": "100Mi"}},
                            },
                        }
                    },
                },
                "config": config,
            },
        }
        self._kubectl_apply_manifest(manifest)
        cr_status, healthy = self._wait_milvus_cr_healthy(release_name, namespace=namespace, timeout=900)
        assert healthy, cr_status

    def _restart_release_for_config(self, release_name, namespace="chaos-testing"):
        pods = self._kubectl_get_release_pods(release_name, namespace=namespace)
        components = sorted(
            {
                pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component", "")
                for pod in pods
            }
        )
        component = "mixcoord" if "mixcoord" in components else "standalone" if "standalone" in components else None
        assert component is not None, {"release": release_name, "components": components}
        old_pod, new_pod = self._kubectl_delete_one_component_pod_and_wait_ready(
            release_name, component, namespace=namespace, timeout=300
        )
        assert old_pod != new_pod
        cr_status, healthy = self._wait_milvus_cr_healthy(release_name, namespace=namespace, timeout=300)
        assert healthy, cr_status
        return old_pod, new_pod

    def _discover_fixture_source_bucket(self, minio_host, release_name):
        storage_probe = StorageClient(
            f"{minio_host}:9000",
            "minioadmin",
            "minioadmin",
            release_name,
            "file",
        )
        buckets = sorted(bucket.name for bucket in storage_probe.client.list_buckets())
        for candidate in (release_name, "milvus-bucket"):
            if candidate in buckets:
                return candidate
        assert len(buckets) == 1, {"reason": "cannot infer source bucket", "release": release_name, "buckets": buckets}
        return buckets[0]

    def _ensure_storage_fixture_source(self, storage_version, target_release_name):
        namespace = os.environ.get("IMPORT_2PC_FIXTURE_NAMESPACE", "chaos-testing")
        version_label = "V1" if storage_version == 0 else f"V{storage_version}"
        endpoint = os.environ.get(f"IMPORT_2PC_FIXTURE_{version_label}_ENDPOINT")
        minio_host = os.environ.get(f"IMPORT_2PC_FIXTURE_{version_label}_MINIO_HOST")
        token = os.environ.get(f"IMPORT_2PC_FIXTURE_{version_label}_TOKEN", "root:Milvus")
        bucket_name = os.environ.get(f"IMPORT_2PC_FIXTURE_{version_label}_BUCKET")
        root_path = os.environ.get(f"IMPORT_2PC_FIXTURE_{version_label}_ROOT_PATH", "file")
        if endpoint or minio_host:
            assert endpoint and minio_host, (
                f"IMPORT_2PC_FIXTURE_{version_label}_ENDPOINT and "
                f"IMPORT_2PC_FIXTURE_{version_label}_MINIO_HOST must be set together"
            )
            if bucket_name is None:
                release_hint = os.environ.get(f"IMPORT_2PC_FIXTURE_{version_label}_RELEASE", "")
                assert release_hint, f"IMPORT_2PC_FIXTURE_{version_label}_BUCKET is required without release hint"
                bucket_name = self._discover_fixture_source_bucket(minio_host, release_hint)
            return self._build_fixture_source_clients(endpoint, token, minio_host, bucket_name, root_path)

        release_name = os.environ.get(
            f"IMPORT_2PC_FIXTURE_{version_label}_RELEASE",
            "import-2pc-fixture-v1" if storage_version == 0 else f"import-2pc-fixture-v{storage_version}",
        )
        if storage_version == 0:
            image = os.environ.get("IMPORT_2PC_FIXTURE_V1_IMAGE")
            version = os.environ.get("IMPORT_2PC_FIXTURE_V1_VERSION", "2.4.0")
            assert image, (
                "IMPORT_2PC_FIXTURE_V1_IMAGE is required to deploy a real StorageV1 source; "
                "use an older Milvus image that still writes legacy V1 binlogs"
            )
            use_loon_ffi = None
        else:
            image = os.environ.get("IMPORT_2PC_FIXTURE_MASTER_IMAGE")
            version = os.environ.get("IMPORT_2PC_FIXTURE_MASTER_VERSION")
            if not image:
                image, target_version = self._kubectl_get_release_image(target_release_name, namespace=namespace)
                version = version or target_version
            version = version or "2.7.0"
            use_loon_ffi = storage_version == 3

        cr = self._kubectl_get_milvus_cr_or_none(release_name, namespace=namespace)
        if cr is None:
            self._kubectl_deploy_fixture_source_release(
                release_name,
                image,
                version,
                namespace=namespace,
                use_loon_ffi=use_loon_ffi,
            )
        else:
            cr_status, healthy = self._wait_milvus_cr_healthy(release_name, namespace=namespace, timeout=600)
            assert healthy, cr_status
            if use_loon_ffi is not None:
                observed = (
                    cr.get("spec", {})
                    .get("config", {})
                    .get("common", {})
                    .get("storage", {})
                    .get("useLoonFFI")
                )
                if observed is not bool(use_loon_ffi):
                    self._kubectl_patch_milvus_config(
                        release_name,
                        {"common": {"storage": {"useLoonFFI": bool(use_loon_ffi)}}},
                        namespace=namespace,
                    )
                    self._restart_release_for_config(release_name, namespace=namespace)

        milvus_host = self._kubectl_wait_service_lb_ip(f"{release_name}-milvus", namespace=namespace, timeout=600)
        minio_host = self._kubectl_wait_service_lb_ip(f"{release_name}-minio", namespace=namespace, timeout=600)
        bucket_name = self._discover_fixture_source_bucket(minio_host, release_name)
        return self._build_fixture_source_clients(
            f"http://{milvus_host}:19530",
            token,
            minio_host,
            bucket_name,
            root_path,
        )

    def _build_copied_backup_fixture_from_source(self, source, storage_version, rows):
        source_collection_name = gen_collection_name(prefix=f"import_2pc_storage_v{storage_version}_src")
        self._create_base_collection_with_clients(source["collection"], source_collection_name)
        self._insert_rows_with_client(source["vector"], source_collection_name, rows)
        flush_rsp, flushed = self._flush_collection_with_client_retry(source["collection"], source_collection_name)
        assert flushed, flush_rsp

        source_collection_id = self._get_collection_id_with_client(source["collection"], source_collection_name)
        _, insert_objects, has_insert = self._wait_storage_log_prefix_non_empty_with_client(
            source["storage"], "insert_log", source_collection_id
        )
        assert has_insert, {"collection_id": source_collection_id, "checked_prefixes": insert_objects}
        if storage_version == 3:
            source_segment_prefix = self._select_manifest_segment_prefix(insert_objects)
        else:
            source_segment_prefix = self._select_insert_segment_prefix(insert_objects)
        source_segment_id = source_segment_prefix.rstrip("/").split("/")[-1]
        target_partition_prefix = (
            f"{(self.storage_client.root_path or 'file').strip('/')}/"
            f"import_2pc_storage_fixture/{uuid4().hex}/insert_log/{source_collection_id}/_default"
        )
        target_segment_prefix = (
            f"{target_partition_prefix}/{source_segment_id}"
        )
        copied = self._copy_storage_prefix_between_clients(
            source["storage"],
            self.storage_client,
            source_segment_prefix,
            target_segment_prefix,
        )
        assert copied, {"source_prefix": source_segment_prefix, "target_prefix": target_segment_prefix}
        return {
            "files": [[target_partition_prefix + "/"]],
            "storageVersion": storage_version,
            "partitionName": "_default",
            "expectedIds": [row["id"] for row in rows],
            "expectedCount": len(rows),
            "copiedPrefix": target_partition_prefix,
        }

    def _build_copied_l0_fixture_from_source(self, source, storage_version, seed_rows, delete_ids):
        source_collection_name = gen_collection_name(prefix=f"import_2pc_l0_storage_v{storage_version}_src")
        self._create_base_collection_with_clients(source["collection"], source_collection_name)
        self._insert_rows_with_client(source["vector"], source_collection_name, seed_rows)
        flush_rsp, flushed = self._flush_collection_with_client_retry(source["collection"], source_collection_name)
        assert flushed, flush_rsp

        delete_rsp = source["vector"].vector_delete(
            {
                "collectionName": source_collection_name,
                "filter": f"id in {sorted(delete_ids)}",
            }
        )
        assert delete_rsp["code"] == 0, delete_rsp
        flush_rsp, flushed = self._flush_collection_with_client_retry(source["collection"], source_collection_name)
        assert flushed, flush_rsp

        source_collection_id = self._get_collection_id_with_client(source["collection"], source_collection_name)
        source_delta_prefix, delta_objects, has_delta = self._wait_storage_log_prefix_non_empty_with_client(
            source["storage"], "delta_log", source_collection_id
        )
        assert has_delta, {"collection_id": source_collection_id, "checked_prefixes": delta_objects}
        target_prefix = (
            f"{(self.storage_client.root_path or 'file').strip('/')}/"
            f"import_2pc_storage_fixture/{uuid4().hex}/delta_log/{source_collection_id}"
        )
        copied = self._copy_storage_prefix_between_clients(
            source["storage"],
            self.storage_client,
            source_delta_prefix,
            target_prefix,
        )
        assert copied, {"source_prefix": source_delta_prefix, "target_prefix": target_prefix}
        return {
            "files": [[target_prefix + "/"]],
            "storageVersion": storage_version,
            "seedRows": seed_rows,
            "deleteIds": sorted(delete_ids),
            "partitionName": None,
            "copiedPrefix": target_prefix,
        }

    def _kubectl_get_release_pods(self, release_name, namespace="chaos-testing"):
        if not shutil.which("kubectl"):
            pytest.fail("kubectl is required for import 2PC infra-dependent tests")
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
            pytest.fail(f"cannot inspect release {release_name} pods: {result.stderr.strip()}")
        return json.loads(result.stdout).get("items", [])

    def _skip_if_release_has_no_component(self, release_name, component, case_id):
        pods = self._kubectl_get_release_pods(release_name)
        components = sorted(
            {pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component", "") for pod in pods}
        )
        assert component in components, (
            f"{case_id} requires separate {component} pods; "
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
            pytest.fail(f"release {release_name} has no running {component} pod")
        pod_name = component_pods[0]["metadata"]["name"]
        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        result = subprocess.run(
            ["kubectl", "delete", "pod", pod_name, "-n", namespace, "--wait=false"],
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
            check=False,
        )
        assert result.returncode == 0, {"stdout": result.stdout, "stderr": result.stderr}

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
        raise TimeoutError(f"{component} pod for {release_name} did not become ready: {last_pods}")

    def _wait_milvus_cr_healthy(self, release_name, namespace="chaos-testing", timeout=300):
        if not shutil.which("kubectl"):
            pytest.fail("kubectl is required for import 2PC infra-dependent tests")
        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        deadline = time.time() + timeout
        last_result = None
        while time.time() < deadline:
            last_result = subprocess.run(
                ["kubectl", "get", "milvus", "-n", namespace, release_name, "-o", "json"],
                capture_output=True,
                text=True,
                env=env,
                timeout=15,
                check=False,
            )
            if last_result.returncode == 0:
                status = json.loads(last_result.stdout).get("status", {})
                if status.get("status") == "Healthy":
                    return status, True
            time.sleep(3)
        return {
            "returncode": None if last_result is None else last_result.returncode,
            "stdout": "" if last_result is None else last_result.stdout,
            "stderr": "" if last_result is None else last_result.stderr,
        }, False

    def _kubectl_patch_milvus_config(self, release_name, config_patch, namespace="chaos-testing", timeout=300):
        if not shutil.which("kubectl"):
            pytest.fail("kubectl is required for import 2PC infra-dependent tests")
        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        payload = {"spec": {"config": config_patch}}
        result = subprocess.run(
            [
                "kubectl",
                "patch",
                "milvus",
                "-n",
                namespace,
                release_name,
                "--type",
                "merge",
                "-p",
                json.dumps(payload),
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
            check=False,
        )
        assert result.returncode == 0, {"stdout": result.stdout, "stderr": result.stderr, "payload": payload}
        cr_status, healthy = self._wait_milvus_cr_healthy(release_name, namespace=namespace, timeout=timeout)
        assert healthy, cr_status
        return result

    def _restart_mixcoord_for_config(self, release_name, namespace="chaos-testing"):
        old_pod, new_pod = self._kubectl_delete_one_component_pod_and_wait_ready(
            release_name, "mixcoord", namespace=namespace, timeout=300
        )
        assert old_pod != new_pod
        cr_status, healthy = self._wait_milvus_cr_healthy(release_name, namespace=namespace, timeout=300)
        assert healthy, cr_status
        return old_pod, new_pod

    def _load_json_fixture_from_env(self, env_name):
        raw = os.environ.get(env_name)
        assert raw, f"{env_name} is required for this infra-dependent import 2PC fixture case"
        if os.path.exists(raw):
            with open(raw, encoding="utf-8") as fixture_file:
                return json.load(fixture_file)
        return json.loads(raw)

    def _load_json_fixture_from_env_or_none(self, env_name):
        raw = os.environ.get(env_name)
        if not raw:
            return None
        if os.path.exists(raw):
            with open(raw, encoding="utf-8") as fixture_file:
                return json.load(fixture_file)
        return json.loads(raw)


@pytest.mark.BulkInsert
@pytest.mark.L3
class TestImport2PCInfraDependent(Import2PCInfraBase):
    def test_import_2pc_cdc_multiple_manual_jobs_same_collection_primary_secondary_consistent(
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
        target: verify two manual import jobs in one CDC-replicated collection commit independently and converge
        method: hold two manual import jobs Uncommitted, commit them independently, and compare primary/secondary
        expected: each job has independent visibility and final primary/secondary PK sets and counts match
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

        collection_name = gen_collection_name(prefix="import_2pc_cdc_multi_job")
        first_rows = self._make_rows(21000, 8, phase=210)
        second_rows = self._make_rows(21100, 8, phase=211)
        first_ids = {row["id"] for row in first_rows}
        second_ids = {row["id"] for row in second_rows}
        first_file = f"import_2pc_cdc_multi_first_{uuid4()}.parquet"
        second_file = f"import_2pc_cdc_multi_second_{uuid4()}.parquet"
        local_files = []
        job_ids = []

        try:
            self._create_base_collection(collection_name, shards_num=4)
            rsp, synced = self._wait_collection_exists_with_client(secondary["collection"], collection_name)
            assert synced, rsp
            rsp, loaded = self._wait_collection_loaded_with_client(secondary["collection"], collection_name)
            assert loaded, rsp

            local_files.append(
                self._write_parquet_and_upload_to_both_clusters(first_rows, first_file, secondary["storage"])
            )
            local_files.append(
                self._write_parquet_and_upload_to_both_clusters(second_rows, second_file, secondary["storage"])
            )

            first_job_id = self._create_manual_import_job(collection_name, first_file)
            second_job_id = self._create_manual_import_job(collection_name, second_file)
            job_ids.extend([first_job_id, second_job_id])

            for job_id, rows in ((first_job_id, first_rows), (second_job_id, second_rows)):
                primary_rsp, primary_ready = self.import_job_client.wait_import_job_state(
                    job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
                )
                assert primary_ready, primary_rsp
                assert primary_rsp["data"]["importedRows"] == len(rows), primary_rsp
                secondary_rsp, secondary_ready = secondary["import_job"].wait_import_job_state(
                    job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
                )
                assert secondary_ready, secondary_rsp

            all_ids = first_ids | second_ids
            seen, absent = self._wait_imported_ids_absent(collection_name, all_ids, timeout=20)
            assert absent, {"unexpected_primary_ids": seen}

            commit_rsp = self.import_job_client.commit_import_job(first_job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            for client in (self.import_job_client, secondary["import_job"]):
                rsp, completed = client.wait_import_job_state(
                    first_job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
                )
                assert completed, rsp

            seen, visible = self._wait_imported_ids_visible(collection_name, first_ids, timeout=180)
            assert visible, {"expected": first_ids, "seen": seen}
            seen, absent = self._wait_imported_ids_absent(collection_name, second_ids, timeout=20)
            assert absent, {"unexpected_second_job_ids": seen}

            commit_rsp = self.import_job_client.commit_import_job(second_job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            for client in (self.import_job_client, secondary["import_job"]):
                rsp, completed = client.wait_import_job_state(
                    second_job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
                )
                assert completed, rsp

            primary_seen, primary_visible = self._wait_imported_ids_visible(collection_name, all_ids, timeout=180)
            assert primary_visible, {"expected": all_ids, "seen": primary_seen}
            secondary_seen, secondary_visible = self._wait_imported_ids_visible_with_clients(
                secondary["collection"], secondary["vector"], collection_name, all_ids, timeout=180
            )
            assert secondary_visible, {"expected": all_ids, "seen": secondary_seen}
            assert primary_seen == secondary_seen == all_ids

            primary_count, primary_count_ok = self._wait_count(collection_name, len(all_ids), timeout=120)
            assert primary_count_ok, {"expected_count": len(all_ids), "last_count": primary_count}
            secondary_count, secondary_count_ok = self._wait_count_with_clients(
                secondary["collection"], secondary["vector"], collection_name, len(all_ids), timeout=180
            )
            assert secondary_count_ok, {"expected_count": len(all_ids), "last_count": secondary_count}

        finally:
            for job_id in job_ids:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            for file_path in local_files:
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)

    def test_import_2pc_mixcoord_restart_during_committing_recovers_to_completed(self, release_name):
        """
        target: verify a manual import can finish after MixCoord restarts immediately after CommitImport
        method: commit a manual import and immediately restart MixCoord
        expected: checker recovers the committing job to Completed and imported rows become visible
        """
        self._skip_if_release_has_no_component(release_name, "mixcoord", "IMP-FT-002")

        collection_name = gen_collection_name(prefix="import_2pc_committing_restart")
        self._create_base_collection(collection_name, shards_num=4)
        rows = self._make_rows(22000, 5000, phase=220)
        sample_ids = {row["id"] for row in rows[:12] + rows[-12:]}
        file_name = f"import_2pc_committing_restart_{uuid4()}.parquet"
        file_path = None
        job_id = None

        try:
            file_path = self._write_parquet_and_upload(rows, file_name)
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ready = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ready, rsp

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            old_pod, new_pod = self._kubectl_delete_one_component_pod_and_wait_ready(
                release_name, "mixcoord", timeout=300
            )
            assert old_pod != new_pod
            cr_status, healthy = self._wait_milvus_cr_healthy(release_name, timeout=300)
            assert healthy, cr_status

            rsp, completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert completed, rsp
            seen, visible = self._wait_imported_ids_visible(collection_name, sample_ids, timeout=180)
            assert visible, {"expected": sample_ids, "seen": seen}
            count, count_ok = self._wait_count(collection_name, len(rows), timeout=180)
            assert count_ok, {"expected_count": len(rows), "last_count": count}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    def test_import_2pc_uncommitted_segment_survives_short_gc_then_commit_visible(self, release_name):
        """
        target: verify DataCoord garbage collection does not remove segment files or metadata for an Uncommitted import
        method: set dataCoord.gc.interval to a short value in the Milvus CR, restart MixCoord/DataCoord,
                keep an import job Uncommitted past several GC cycles, then CommitImport
        expected: rows stay invisible before commit and become queryable/searchable after commit
        """
        gc_interval_seconds = int(os.environ.get("IMPORT_2PC_GC_INTERVAL_SECONDS", "10"))
        gc_wait_seconds = int(os.environ.get("IMPORT_2PC_GC_WAIT_SECONDS", str(max(35, gc_interval_seconds * 3 + 5))))
        assert 1 <= gc_interval_seconds <= 60, "IMPORT_2PC_GC_INTERVAL_SECONDS must be 1..60 for this L3 test"
        assert gc_wait_seconds >= gc_interval_seconds * 3, "GC wait must cover at least three short GC cycles"

        collection_name = gen_collection_name(prefix="import_2pc_uncommitted_gc")
        rows = self._make_rows(28000, 1000, phase=280)
        expected_ids = {row["id"] for row in rows}
        sample_ids = {row["id"] for row in rows[:10] + rows[-10:]}
        file_name = f"import_2pc_uncommitted_gc_{uuid4()}.parquet"
        file_path = None
        job_id = None
        gc_config_applied = False

        try:
            self._kubectl_patch_milvus_config(release_name, {"dataCoord": {"gc": {"interval": gc_interval_seconds}}})
            gc_config_applied = True
            self._restart_mixcoord_for_config(release_name)

            self._create_base_collection(collection_name, shards_num=4)
            file_path = self._write_parquet_and_upload(rows, file_name)
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ready = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ready, rsp
            assert rsp["data"]["importedRows"] == len(rows), rsp

            seen, absent = self._wait_imported_ids_absent(collection_name, sample_ids, timeout=20)
            assert absent, {"unexpected_visible_ids_before_gc_wait": seen}

            deadline = time.time() + gc_wait_seconds
            last_progress = rsp
            while time.time() < deadline:
                time.sleep(min(10, max(1, deadline - time.time())))
                last_progress = self.import_job_client.get_import_job_progress(job_id)
                assert last_progress.get("code") == 0, last_progress
                assert last_progress["data"]["state"] == "Uncommitted", last_progress

            seen, absent = self._wait_imported_ids_absent(collection_name, sample_ids, timeout=20)
            assert absent, {"unexpected_visible_ids_after_gc_wait": seen, "progress": last_progress}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert completed, rsp

            seen, visible = self._wait_imported_ids_visible(collection_name, sample_ids, timeout=180)
            assert visible, {"expected": sample_ids, "seen": seen}
            count, count_ok = self._wait_count(collection_name, len(expected_ids), timeout=180)
            assert count_ok, {"expected_count": len(expected_ids), "last_count": count}
            search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=10)
            assert search_ids & expected_ids, {"search_ids": search_ids, "expected_any": list(sample_ids)[:5]}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
            if gc_config_applied:
                self._kubectl_patch_milvus_config(release_name, {"dataCoord": {"gc": None}})
                self._restart_mixcoord_for_config(release_name)

    def test_import_2pc_streamingnode_restart_during_commit_replays_commit_message(self, release_name):
        """
        target: verify CommitImport WAL consumption is replayed or completed after StreamingNode restarts
        method: commit a manual import and immediately restart one StreamingNode pod
        expected: WAL message is replayed or consumed idempotently and job/data reach Completed/visible
        """
        self._skip_if_release_has_no_component(release_name, "streamingnode", "IMP-FT-004")

        collection_name = gen_collection_name(prefix="import_2pc_streamingnode_restart")
        self._create_base_collection(collection_name, shards_num=4)
        rows = self._make_rows(23000, 5000, phase=230)
        sample_ids = {row["id"] for row in rows[:12] + rows[-12:]}
        file_name = f"import_2pc_streamingnode_restart_{uuid4()}.parquet"
        file_path = None
        job_id = None

        try:
            file_path = self._write_parquet_and_upload(rows, file_name)
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ready = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ready, rsp

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            old_pod, new_pod = self._kubectl_delete_one_component_pod_and_wait_ready(
                release_name, "streamingnode", timeout=300
            )
            assert old_pod != new_pod
            cr_status, healthy = self._wait_milvus_cr_healthy(release_name, timeout=300)
            assert healthy, cr_status

            rsp, completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert completed, rsp
            seen, visible = self._wait_imported_ids_visible(collection_name, sample_ids, timeout=180)
            assert visible, {"expected": sample_ids, "seen": seen}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    def test_import_2pc_external_backup_fixture_manual_commit_restores_rows(self):
        """
        target: verify backup/binlog import stays invisible before CommitImport and restores rows after CommitImport
        method: import real backup files with auto_commit=false, then commit; generate binlog fixture if env is absent
        expected: restore stays invisible before commit and exposes expected count/PK set after commit
        """
        fixture = self._load_json_fixture_from_env_or_none("IMPORT_2PC_BACKUP_FIXTURE")
        if fixture is None:
            fixture = self._build_generated_backup_fixture(
                gen_collection_name(prefix="import_2pc_backup_fixture_src"),
                self._make_rows(26000, 12, phase=260),
            )
        collection_name = gen_collection_name(prefix="import_2pc_backup_fixture")
        self._create_collection_from_fixture(collection_name, fixture)

        expected_ids = set(fixture.get("expectedIds", []))
        expected_count = fixture.get("expectedCount", len(expected_ids) if expected_ids else None)
        options = {"backup": "true"}
        if fixture.get("storageVersion") is not None:
            options["storage_version"] = str(fixture["storageVersion"])
        for key in ("start_ts", "end_ts", "skip_disk_quota_check", "ezk"):
            if key in fixture:
                options[key] = str(fixture[key])

        job_id = None
        try:
            job_id = self._create_manual_import_job_with_files(
                collection_name,
                fixture["files"],
                options,
                partition_name=fixture.get("partitionName", "_default"),
            )
            rsp, ready = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ready, rsp
            if expected_count is None and fixture.get("expectedCountFromJob"):
                expected_count = rsp["data"].get("importedRows")
            if expected_count is not None:
                assert rsp["data"]["importedRows"] == expected_count, rsp

            if expected_ids:
                seen, absent = self._wait_imported_ids_absent(collection_name, expected_ids, timeout=20)
                assert absent, {"unexpected_visible_ids": seen}
            elif expected_count is not None:
                count, count_ok = self._wait_count(collection_name, 0, timeout=20)
                assert count_ok, {"expected_count": 0, "last_count": count}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert completed, rsp

            if expected_ids:
                seen, visible = self._wait_imported_ids_visible(collection_name, expected_ids, timeout=180)
                assert visible, {"expected": expected_ids, "seen": seen}
            if expected_count is not None:
                count, count_ok = self._wait_count(collection_name, expected_count, timeout=180)
                assert count_ok, {"expected_count": expected_count, "last_count": count}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)

    def test_import_2pc_external_l0_fixture_manual_commit_applies_delete_after_commit(self):
        """
        target: verify L0 delete import does not apply before CommitImport and deletes matching rows after CommitImport
        method: seed target rows, release collection, import real L0 files with auto_commit=false, then commit;
                generate delete-binlog fixture if env is absent
        expected: delete rows remain before commit and disappear only after CommitImport
        """
        fixture = self._load_json_fixture_from_env_or_none("IMPORT_2PC_L0_FIXTURE")
        collection_name = gen_collection_name(prefix="import_2pc_l0_fixture")
        if fixture is None:
            self._create_base_collection(collection_name)
            seed_rows = self._make_rows(27000, 12, phase=270)
            delete_ids = {row["id"] for row in seed_rows[:5]}
        else:
            self._create_collection_from_fixture(collection_name, fixture)
            seed_rows = fixture["seedRows"]
            delete_ids = set(fixture["deleteIds"])
        seed_ids = {row["id"] for row in seed_rows}
        survivor_ids = seed_ids - delete_ids

        job_id = None
        try:
            self._insert_rows(collection_name, seed_rows)
            seen, visible = self._wait_imported_ids_visible(collection_name, seed_ids, timeout=120)
            assert visible, {"expected": seed_ids, "seen": seen}
            flush_rsp, flushed = self._flush_collection_with_retry(collection_name)
            assert flushed, flush_rsp
            if fixture is None:
                fixture = self._build_generated_l0_fixture(
                    gen_collection_name(prefix="import_2pc_l0_fixture_src"),
                    seed_rows,
                    delete_ids,
                )

            options = {"l0_import": "true"}
            if fixture.get("storageVersion") is not None:
                options["storage_version"] = str(fixture["storageVersion"])
            if fixture.get("skipDiskQuotaCheck") is not None:
                options["skip_disk_quota_check"] = str(fixture["skipDiskQuotaCheck"]).lower()

            release_rsp = self.collection_client.collection_release(collection_name=collection_name)
            assert release_rsp["code"] == 0, release_rsp
            job_id = self._create_manual_import_job_with_files(
                collection_name,
                fixture["files"],
                options,
                partition_name=fixture.get("partitionName"),
            )
            rsp, ready = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ready, rsp

            load_rsp = self.collection_client.collection_load(collection_name=collection_name)
            assert load_rsp["code"] == 0, load_rsp
            self.wait_load_completed(collection_name, timeout=120)

            seen_before_commit = self._query_imported_ids(collection_name, delete_ids)
            assert seen_before_commit == delete_ids, {"expected_still_visible": delete_ids, "seen": seen_before_commit}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert completed, rsp

            seen, deleted = self._wait_imported_ids_absent(collection_name, delete_ids, timeout=180)
            assert deleted, {"expected_deleted": delete_ids, "last_seen": seen}
            seen, visible = self._wait_imported_ids_visible(collection_name, survivor_ids, timeout=180)
            assert visible, {"expected_survivors": survivor_ids, "seen": seen}
            count, count_ok = self._wait_count(collection_name, len(survivor_ids), timeout=180)
            assert count_ok, {"expected_count": len(survivor_ids), "last_count": count}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)

    @pytest.mark.parametrize("storage_version", [0, 2], ids=["storage_v1", "storage_v2"])
    def test_import_2pc_real_source_backup_storage_version_manual_commit_restores_rows(
        self,
        release_name,
        storage_version,
    ):
        """
        target: verify backup import can restore real binlogs generated by a separate source Milvus instance
        method: deploy or reuse a source instance for the requested storage version, insert and flush rows there,
                copy its MinIO insert_log objects into the target MinIO, import with backup=true and auto_commit=false
        expected: rows are invisible while Uncommitted and become queryable/searchable only after CommitImport
        """
        source = self._ensure_storage_fixture_source(storage_version, release_name)
        base_id = {0: 30000, 2: 30200, 3: 30300}[storage_version]
        rows = self._make_rows(base_id, 12, phase=300 + storage_version)
        fixture = self._build_copied_backup_fixture_from_source(source, storage_version, rows)
        collection_name = gen_collection_name(prefix=f"import_2pc_real_backup_v{storage_version}")
        self._create_base_collection(collection_name)

        expected_ids = set(fixture["expectedIds"])
        job_id = None
        try:
            job_id = self._create_manual_import_job_with_files(
                collection_name,
                fixture["files"],
                {"backup": "true", "storage_version": str(storage_version)},
                partition_name=fixture["partitionName"],
            )
            rsp, ready = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ready, rsp
            assert rsp["data"]["importedRows"] == fixture["expectedCount"], rsp

            seen, absent = self._wait_imported_ids_absent(collection_name, expected_ids, timeout=20)
            assert absent, {"unexpected_visible_ids": seen, "storage_version": storage_version}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert completed, rsp

            seen, visible = self._wait_imported_ids_visible(collection_name, expected_ids, timeout=180)
            assert visible, {"expected": expected_ids, "seen": seen, "storage_version": storage_version}
            count, count_ok = self._wait_count(collection_name, len(expected_ids), timeout=180)
            assert count_ok, {"expected_count": len(expected_ids), "last_count": count}
            search_ids = self._search_imported_ids(collection_name, rows[0]["vector"], limit=5)
            assert search_ids & expected_ids, {"search_ids": search_ids, "expected_any": list(expected_ids)[:5]}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            self._delete_storage_prefix(self.storage_client, fixture["copiedPrefix"])

    def test_import_2pc_real_source_backup_storage_v3_manifest_prefix_fails_closed(self, release_name):
        """
        target: verify REST backup=true prefix import does not silently misread StorageV3 manifest-layout objects
        method: generate a real StorageV3 segment in a source instance, copy its _data/_metadata/_stats objects
                into target MinIO, then submit backup=true with storage_version=3 through the REST import API
        expected: the job fails with a reader/manifest-layout reason and no imported rows become visible
        """
        storage_version = 3
        source = self._ensure_storage_fixture_source(storage_version, release_name)
        rows = self._make_rows(30350, 12, phase=350)
        fixture = self._build_copied_backup_fixture_from_source(source, storage_version, rows)
        collection_name = gen_collection_name(prefix="import_2pc_real_backup_v3_guardrail")
        self._create_base_collection(collection_name)

        expected_ids = set(fixture["expectedIds"])
        job_id = None
        try:
            job_id = self._create_manual_import_job_with_files(
                collection_name,
                fixture["files"],
                {"backup": "true", "storage_version": "3"},
                partition_name=fixture["partitionName"],
            )
            deadline = time.time() + min(IMPORT_2PC_TIMEOUT, 120)
            rsp = None
            failed = False
            while time.time() < deadline:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                assert rsp.get("code") == 0, rsp
                state = rsp.get("data", {}).get("state")
                if state == "Failed":
                    failed = True
                    break
                if state in ("Uncommitted", "Committing", "Completed"):
                    pytest.fail(f"StorageV3 manifest prefix became commit-capable through REST backup reader: {rsp}")
                time.sleep(2)
            assert failed, rsp
            reason = str(rsp.get("data", {}).get("reason", rsp)).lower()
            assert any(
                token in reason
                for token in ("field id", "system fields", "manifest", "failed to create reader", "import failed")
            ), {"reason": reason, "rsp": rsp}

            seen, absent = self._wait_imported_ids_absent(collection_name, expected_ids, timeout=20)
            assert absent, {"unexpected_visible_ids": seen, "reason": reason}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            self._delete_storage_prefix(self.storage_client, fixture["copiedPrefix"])

    @pytest.mark.parametrize("storage_version", [0, 2, 3], ids=["storage_v1", "storage_v2", "storage_v3"])
    def test_import_2pc_real_source_l0_storage_version_manual_commit_applies_delete(
        self,
        release_name,
        storage_version,
    ):
        """
        target: verify L0 import can replay real delete logs generated by a separate source Milvus instance
        method: seed matching rows in the target, generate delete logs in a source instance for the requested
                storage version, copy source delta_log objects into target MinIO, then import with l0_import=true
        expected: matching rows remain visible before CommitImport and disappear only after the L0 import is committed
        """
        source = self._ensure_storage_fixture_source(storage_version, release_name)
        base_id = {0: 31000, 2: 31200, 3: 31300}[storage_version]
        seed_rows = self._make_rows(base_id, 12, phase=310 + storage_version)
        delete_ids = {row["id"] for row in seed_rows[:5]}
        seed_ids = {row["id"] for row in seed_rows}
        survivor_ids = seed_ids - delete_ids
        collection_name = gen_collection_name(prefix=f"import_2pc_real_l0_v{storage_version}")
        self._create_base_collection(collection_name)

        job_id = None
        fixture = None
        try:
            self._insert_rows(collection_name, seed_rows)
            seen, visible = self._wait_imported_ids_visible(collection_name, seed_ids, timeout=120)
            assert visible, {"expected": seed_ids, "seen": seen}
            flush_rsp, flushed = self._flush_collection_with_retry(collection_name)
            assert flushed, flush_rsp

            fixture = self._build_copied_l0_fixture_from_source(source, storage_version, seed_rows, delete_ids)

            release_rsp = self.collection_client.collection_release(collection_name=collection_name)
            assert release_rsp["code"] == 0, release_rsp
            job_id = self._create_manual_import_job_with_files(
                collection_name,
                fixture["files"],
                {"l0_import": "true", "storage_version": str(storage_version)},
                partition_name=fixture["partitionName"],
            )
            rsp, ready = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert ready, rsp

            load_rsp = self.collection_client.collection_load(collection_name=collection_name)
            assert load_rsp["code"] == 0, load_rsp
            self.wait_load_completed(collection_name, timeout=120)

            seen_before_commit = self._query_imported_ids(collection_name, delete_ids)
            assert seen_before_commit == delete_ids, {
                "expected_still_visible": delete_ids,
                "seen": seen_before_commit,
                "storage_version": storage_version,
            }

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert completed, rsp

            seen, deleted = self._wait_imported_ids_absent(collection_name, delete_ids, timeout=180)
            assert deleted, {"expected_deleted": delete_ids, "last_seen": seen, "storage_version": storage_version}
            seen, visible = self._wait_imported_ids_visible(collection_name, survivor_ids, timeout=180)
            assert visible, {"expected_survivors": survivor_ids, "seen": seen, "storage_version": storage_version}
            count, count_ok = self._wait_count(collection_name, len(survivor_ids), timeout=180)
            assert count_ok, {"expected_count": len(survivor_ids), "last_count": count}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if fixture is not None:
                self._delete_storage_prefix(self.storage_client, fixture["copiedPrefix"])

    def test_import_2pc_max_active_job_limit_on_configured_instance(self, release_name):
        """
        target: verify DataCoord rejects a new import when active manual import jobs reach maxImportJobNum
        method: set dataCoord.import.maxImportJobNum in the Milvus CR, create limit Uncommitted jobs,
                then create one more manual import job
        expected: extra job is rejected while active Uncommitted jobs count toward the configured limit
        """
        max_jobs = int(os.environ.get("IMPORT_2PC_MAX_ACTIVE_JOBS", "1"))
        if max_jobs <= 0 or max_jobs > 50:
            pytest.fail("IMPORT_2PC_MAX_ACTIVE_JOBS must be 1..50 for this L3 test")

        collection_name = gen_collection_name(prefix="import_2pc_max_jobs")
        rows = self._make_rows(24000, 2, phase=240)
        file_name = f"import_2pc_max_jobs_{uuid4()}.parquet"
        file_path = None
        job_ids = []
        max_jobs_config_applied = False

        try:
            self._kubectl_patch_milvus_config(
                release_name,
                {"dataCoord": {"import": {"maxImportJobNum": max_jobs}}},
            )
            max_jobs_config_applied = True
            self._restart_mixcoord_for_config(release_name)

            self._create_base_collection(collection_name)
            file_path = self._write_parquet_and_upload(rows, file_name)
            rejected_rsp = None
            for _ in range(max_jobs + 1):
                rsp = self.import_job_client.create_import_jobs(
                    {
                        "collectionName": collection_name,
                        "files": [[file_name]],
                        "options": {"auto_commit": "false"},
                    }
                )
                if rsp.get("code") == 0:
                    job_id = rsp["data"]["jobId"]
                    observed_rsp, observed = self.import_job_client.wait_import_job_state(
                        job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
                    )
                    assert observed, observed_rsp
                    job_ids.append(job_id)
                else:
                    rejected_rsp = rsp
                    break

            assert len(job_ids) == max_jobs, {"created_job_ids": job_ids, "rejected_rsp": rejected_rsp}
            assert rejected_rsp is not None and rejected_rsp.get("code") != 0, rejected_rsp
            reason = str(rejected_rsp.get("message", rejected_rsp)).lower()
            assert "import" in reason and ("job" in reason or "limit" in reason or "max" in reason), rejected_rsp

        finally:
            for job_id in job_ids:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
            if max_jobs_config_applied:
                self._kubectl_patch_milvus_config(release_name, {"dataCoord": {"import": None}})
                self._restart_mixcoord_for_config(release_name)

    def test_import_2pc_disk_quota_failure_on_configured_instance(self, release_name):
        """
        target: verify import is rejected or fails when cluster and collection disk quota are already exhausted
        method: set quotaAndLimits.limitWriting.diskProtection quota to a tiny value in the Milvus CR,
                wait until normal DML is denied by disk quota, then submit a manual import
        expected: import create or job execution fails with quota reason and no rows become visible
        """
        disk_quota_mb = int(os.environ.get("IMPORT_2PC_DISK_QUOTA_MB", "1"))
        assert 1 <= disk_quota_mb <= 100, "IMPORT_2PC_DISK_QUOTA_MB must be 1..100 for this L3 test"
        collection_name = gen_collection_name(prefix="import_2pc_disk_quota")
        rows = self._make_rows(25000, 1000, phase=250)
        expected_ids = {row["id"] for row in rows[:10]}
        file_name = f"import_2pc_disk_quota_{uuid4()}.parquet"
        file_path = None
        job_id = None
        disk_quota_config_applied = False

        try:
            self._kubectl_patch_milvus_config(
                release_name,
                {
                    "quotaAndLimits": {
                        "limitWriting": {
                            "diskProtection": {
                                "enabled": True,
                                "diskQuota": disk_quota_mb,
                                "diskQuotaPerCollection": disk_quota_mb,
                            }
                        }
                    }
                },
            )
            disk_quota_config_applied = True

            self._create_base_collection(collection_name)
            quota_rsp, quota_active = self._wait_disk_quota_write_denied(collection_name)
            assert quota_active, {
                "reason": "disk quota precondition did not become active before import",
                "last_rsp": quota_rsp,
            }
            file_path = self._write_parquet_and_upload(rows, file_name)
            create_rsp = self.import_job_client.create_import_jobs(
                {
                    "collectionName": collection_name,
                    "files": [[file_name]],
                    "options": {"auto_commit": "false"},
                }
            )
            if create_rsp.get("code") == 0:
                job_id = create_rsp["data"]["jobId"]
                deadline = time.time() + min(IMPORT_2PC_TIMEOUT, 120)
                rsp = None
                failed = False
                while time.time() < deadline:
                    rsp = self.import_job_client.get_import_job_progress(job_id)
                    state = rsp.get("data", {}).get("state") if rsp.get("code") == 0 else None
                    if state == "Failed":
                        failed = True
                        break
                    if state in ("Uncommitted", "Committing", "Completed"):
                        pytest.fail(
                            "disk quota exhausted import job became commit-capable instead of failing: "
                            f"{rsp}"
                        )
                    time.sleep(2)
                assert failed, rsp
                reason = str(rsp.get("data", {}).get("reason", rsp)).lower()
            else:
                reason = str(create_rsp.get("message", create_rsp)).lower()
            assert "quota" in reason or "disk" in reason, {"create_rsp": create_rsp, "reason": reason}
            seen, absent = self._wait_imported_ids_absent(collection_name, expected_ids, timeout=20)
            assert absent, {"unexpected_visible_ids": seen}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
            if disk_quota_config_applied:
                self._kubectl_patch_milvus_config(release_name, {"quotaAndLimits": None})
