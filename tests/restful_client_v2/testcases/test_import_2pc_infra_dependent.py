import json
import os
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, wait
from urllib.parse import urlparse
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from api.milvus import CollectionClient, ImportJobClient, StorageClient, VectorClient
from base.testbase import TestBase
from pymilvus import MilvusClient
from utils.constant import CaseLabel
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
                segment_prefix for segment_prefix, groups in segments.items() if "0" in groups and len(groups) >= 2
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
            segment_prefix for segment_prefix, groups in segment_groups.items() if "0" in groups and len(groups) >= 2
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
        source_segment_prefix = self._select_insert_segment_prefix(insert_objects)
        source_segment_id = source_segment_prefix.rstrip("/").split("/")[-1]
        target_partition_prefix = (
            f"{(self.storage_client.root_path or 'file').strip('/')}/"
            f"import_2pc_backup_fixture/{uuid4().hex}/insert_log/{source_collection_id}/_default"
        )
        target_segment_prefix = f"{target_partition_prefix}/{source_segment_id}"
        copied = self._copy_storage_prefix_between_clients(
            self.storage_client,
            self.storage_client,
            source_segment_prefix,
            target_segment_prefix,
        )
        assert copied, {"source_prefix": source_segment_prefix, "target_prefix": target_segment_prefix}
        storage_version = int(os.getenv("IMPORT_2PC_GENERATED_STORAGE_VERSION", "2"))
        return {
            "files": [[target_partition_prefix + "/"]],
            "storageVersion": storage_version,
            "partitionName": "_default",
            "expectedIds": [row["id"] for row in rows],
            "expectedCount": len(rows),
            "copiedPrefixes": [target_partition_prefix],
        }

    def _hybrid_ts_now(self, offset_ms=0):
        return str((int(time.time() * 1000) + offset_ms) << 18)

    def _build_generated_backup_window_fixture(self, source_collection_name, old_rows, included_rows, late_rows):
        def build_source_prefix(suffix, rows):
            collection_name = f"{source_collection_name}_{suffix}"
            self._create_base_collection(collection_name)
            self._insert_rows(collection_name, rows)
            flush_rsp, flushed = self._flush_collection_with_retry(collection_name)
            assert flushed, flush_rsp
            collection_id = self._get_collection_id(collection_name)
            _, insert_objects, has_insert = self._wait_storage_log_prefix_non_empty("insert_log", collection_id)
            assert has_insert, {"collection_id": collection_id, "checked_prefixes": insert_objects}
            source_segment_prefix = self._select_insert_segment_prefix(insert_objects)
            source_segment_id = source_segment_prefix.rstrip("/").split("/")[-1]
            target_partition_prefix = (
                f"{(self.storage_client.root_path or 'file').strip('/')}/"
                f"import_2pc_backup_window_fixture/{uuid4().hex}/insert_log/{collection_id}/_default"
            )
            target_segment_prefix = f"{target_partition_prefix}/{source_segment_id}"
            copied = self._copy_storage_prefix_between_clients(
                self.storage_client,
                self.storage_client,
                source_segment_prefix,
                target_segment_prefix,
            )
            assert copied, {"source_prefix": source_segment_prefix, "target_prefix": target_segment_prefix}
            return target_partition_prefix + "/", target_partition_prefix

        old_prefix, old_copied_prefix = build_source_prefix("old", old_rows)

        time.sleep(2)
        start_ts = self._hybrid_ts_now(-500)
        included_prefix, included_copied_prefix = build_source_prefix("included", included_rows)

        end_ts = self._hybrid_ts_now(500)
        time.sleep(2)
        late_prefix, late_copied_prefix = build_source_prefix("late", late_rows)

        storage_version = int(os.getenv("IMPORT_2PC_GENERATED_STORAGE_VERSION", "2"))
        return {
            "files": [[old_prefix], [included_prefix], [late_prefix]],
            "storageVersion": storage_version,
            "partitionName": "_default",
            "start_ts": start_ts,
            "end_ts": end_ts,
            "expectedIds": [row["id"] for row in included_rows],
            "excludedIds": [row["id"] for row in old_rows + late_rows],
            "expectedCount": len(included_rows),
            "copiedPrefixes": [old_copied_prefix, included_copied_prefix, late_copied_prefix],
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

    def _build_generated_l0_window_fixture(
        self,
        source_collection_name,
        seed_rows,
        old_delete_ids,
        included_delete_ids,
        late_delete_ids,
    ):
        def build_delete_prefix(suffix, ids):
            collection_name = f"{source_collection_name}_{suffix}"
            self._create_base_collection(collection_name)
            self._insert_rows(collection_name, seed_rows)
            flush_rsp, flushed = self._flush_collection_with_retry(collection_name)
            assert flushed, flush_rsp
            delete_rsp = self.vector_client.vector_delete(
                {
                    "collectionName": collection_name,
                    "filter": f"id in {sorted(ids)}",
                }
            )
            assert delete_rsp["code"] == 0, delete_rsp
            flush_rsp, flushed = self._flush_collection_with_retry(collection_name)
            assert flushed, flush_rsp
            collection_id = self._get_collection_id(collection_name)
            delta_prefix, delta_objects, has_delta = self._wait_storage_log_prefix_non_empty("delta_log", collection_id)
            assert has_delta, {"collection_id": collection_id, "checked_prefixes": delta_objects}
            return delta_prefix

        old_delta_prefix = build_delete_prefix("old", old_delete_ids)

        time.sleep(2)
        start_ts = self._hybrid_ts_now(-500)
        included_delta_prefix = build_delete_prefix("included", included_delete_ids)

        end_ts = self._hybrid_ts_now(500)
        time.sleep(2)
        late_delta_prefix = build_delete_prefix("late", late_delete_ids)

        storage_version = int(os.getenv("IMPORT_2PC_GENERATED_STORAGE_VERSION", "2"))
        return {
            "files": [[old_delta_prefix], [included_delta_prefix], [late_delta_prefix]],
            "storageVersion": storage_version,
            "seedRows": seed_rows,
            "deleteIds": sorted(included_delete_ids),
            "nonDeleteIds": sorted(set(old_delete_ids) | set(late_delete_ids)),
            "start_ts": start_ts,
            "end_ts": end_ts,
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

    def _apply_cdc_topology(
        self, secondary_endpoint, secondary_token, source_cluster_id, target_cluster_id, pchannel_num
    ):
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
            "endpoint": endpoint,
            "token": token,
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
            {pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component", "") for pod in pods}
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
                observed = cr.get("spec", {}).get("config", {}).get("common", {}).get("storage", {}).get("useLoonFFI")
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
        target_segment_prefix = f"{target_partition_prefix}/{source_segment_id}"
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

    def _resolve_secondary_release_name(self, secondary_release_name, case_id):
        release_name = secondary_release_name or os.environ.get("IMPORT_2PC_SECONDARY_RELEASE_NAME")
        assert release_name, (
            f"{case_id} requires --secondary_release_name or IMPORT_2PC_SECONDARY_RELEASE_NAME "
            "so the test can pause and restore the secondary cluster component"
        )
        return release_name

    def _resolve_primary_release_name(self, release_name, case_id, namespace="chaos-testing"):
        resolved = os.environ.get("IMPORT_2PC_PRIMARY_RELEASE_NAME") or release_name
        pods = self._kubectl_get_release_pods(resolved, namespace=namespace)
        assert pods, (
            f"{case_id} requires --release_name or IMPORT_2PC_PRIMARY_RELEASE_NAME "
            "to select the primary cluster pods for Chaos Mesh network partition"
        )
        return resolved

    def _skip_if_release_has_no_component(self, release_name, component, case_id, namespace="chaos-testing"):
        pods = self._kubectl_get_release_pods(release_name, namespace=namespace)
        components = sorted(
            {pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component", "") for pod in pods}
        )
        assert component in components, (
            f"{case_id} requires separate {component} pods; "
            f"{release_name} exposes components={components or ['<none>']}"
        )

    def _kubectl_get_component_workloads(self, release_name, component, namespace="chaos-testing"):
        if not shutil.which("kubectl"):
            pytest.fail("kubectl is required for import 2PC infra-dependent tests")
        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        selector = (
            f"app.kubernetes.io/instance={release_name},"
            f"app.kubernetes.io/name=milvus,"
            f"app.kubernetes.io/component={component}"
        )
        result = subprocess.run(
            ["kubectl", "get", "deploy,statefulset", "-n", namespace, "-l", selector, "-o", "json"],
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            pytest.fail(
                f"cannot inspect {component} workloads for release {release_name}: "
                f"stdout={result.stdout.strip()} stderr={result.stderr.strip()}"
            )
        workloads = []
        for item in json.loads(result.stdout).get("items", []):
            kind = item.get("kind", "").lower()
            name = item.get("metadata", {}).get("name")
            replicas = item.get("spec", {}).get("replicas", 1)
            if kind and name:
                workloads.append({"kind": kind, "name": name, "replicas": replicas})
        assert workloads, f"release {release_name} has no {component} deployment/statefulset workloads"
        return workloads

    def _kubectl_wait_workloads_replicas(self, workloads, replicas, namespace="chaos-testing", timeout=300):
        if not shutil.which("kubectl"):
            pytest.fail("kubectl is required for import 2PC infra-dependent tests")
        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        deadline = time.time() + timeout
        last = {}
        while time.time() < deadline:
            all_ready = True
            for workload in workloads:
                result = subprocess.run(
                    [
                        "kubectl",
                        "get",
                        f"{workload['kind']}/{workload['name']}",
                        "-n",
                        namespace,
                        "-o",
                        "json",
                    ],
                    capture_output=True,
                    text=True,
                    env=env,
                    timeout=30,
                    check=False,
                )
                if result.returncode != 0:
                    all_ready = False
                    last[workload["name"]] = {"stdout": result.stdout, "stderr": result.stderr}
                    continue
                item = json.loads(result.stdout)
                status = item.get("status", {})
                observed_replicas = status.get("replicas", 0) or 0
                ready_replicas = status.get("readyReplicas", status.get("availableReplicas", 0)) or 0
                last[workload["name"]] = {
                    "replicas": observed_replicas,
                    "readyReplicas": ready_replicas,
                    "desired": item.get("spec", {}).get("replicas"),
                }
                if replicas == 0:
                    all_ready = all_ready and observed_replicas == 0 and ready_replicas == 0
                else:
                    all_ready = all_ready and ready_replicas >= replicas
            if all_ready:
                return last, True
            time.sleep(3)
        return last, False

    def _kubectl_scale_workloads(self, workloads, replicas, namespace="chaos-testing", timeout=300):
        if not shutil.which("kubectl"):
            pytest.fail("kubectl is required for import 2PC infra-dependent tests")
        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        for workload in workloads:
            result = subprocess.run(
                [
                    "kubectl",
                    "scale",
                    f"{workload['kind']}/{workload['name']}",
                    "-n",
                    namespace,
                    "--replicas",
                    str(replicas),
                ],
                capture_output=True,
                text=True,
                env=env,
                timeout=30,
                check=False,
            )
            assert result.returncode == 0, {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "workload": workload,
                "replicas": replicas,
            }
        observed, ready = self._kubectl_wait_workloads_replicas(
            workloads,
            replicas,
            namespace=namespace,
            timeout=timeout,
        )
        assert ready, {"workloads": workloads, "replicas": replicas, "last": observed}
        return observed

    def _kubectl_restore_workloads(self, workloads, namespace="chaos-testing", timeout=300):
        if not workloads:
            return
        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        for workload in workloads:
            result = subprocess.run(
                [
                    "kubectl",
                    "scale",
                    f"{workload['kind']}/{workload['name']}",
                    "-n",
                    namespace,
                    "--replicas",
                    str(workload["replicas"]),
                ],
                capture_output=True,
                text=True,
                env=env,
                timeout=30,
                check=False,
            )
            assert result.returncode == 0, {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "workload": workload,
            }
        deadline = time.time() + timeout
        last = {}
        while time.time() < deadline:
            all_ready = True
            for workload in workloads:
                observed, ready = self._kubectl_wait_workloads_replicas(
                    [workload],
                    workload["replicas"],
                    namespace=namespace,
                    timeout=10,
                )
                last.update(observed)
                all_ready = all_ready and ready
            if all_ready:
                return
        assert False, {"workloads": workloads, "last": last}

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
            and pod.get("metadata", {}).get("deletionTimestamp") is None
            and any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in pod.get("status", {}).get("conditions", [])
            )
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
                if pod.get("metadata", {}).get("deletionTimestamp") is not None:
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

    def _restart_config_components(self, release_name, requested_components, namespace="chaos-testing"):
        pods = self._kubectl_get_release_pods(release_name, namespace=namespace)
        available_components = {
            pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component") for pod in pods
        }
        if "standalone" in available_components:
            restart_components = ["standalone"]
        else:
            restart_components = [component for component in requested_components if component in available_components]
        if not restart_components:
            pytest.fail(
                f"release {release_name} has none of {list(requested_components)} and no standalone pod for config restart"
            )
        restarted = []
        for component in restart_components:
            old_pod, new_pod = self._kubectl_delete_one_component_pod_and_wait_ready(
                release_name, component, namespace=namespace, timeout=300
            )
            assert old_pod != new_pod
            restarted.append((old_pod, new_pod))
        cr_status, healthy = self._wait_milvus_cr_healthy(release_name, namespace=namespace, timeout=300)
        assert healthy, cr_status
        return restarted

    def _restart_mixcoord_for_config(self, release_name, namespace="chaos-testing"):
        restarted = self._restart_config_components(release_name, ("mixcoord",), namespace=namespace)
        return restarted[0]

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

    def _require_networkchaos_crd(self):
        self._kubectl_json(
            ["get", "crd", "networkchaos.chaos-mesh.org", "-o", "json"],
            timeout=30,
            fail_message="Chaos Mesh NetworkChaos CRD is required",
        )

    def _build_cdc_proxy_network_partition_manifest(
        self,
        chaos_name,
        namespace,
        primary_release,
        secondary_release,
        external_targets=None,
        duration="10m",
    ):
        spec = {
            "action": "partition",
            "mode": "all",
            "selector": {
                "namespaces": [namespace],
                "labelSelectors": {
                    "app.kubernetes.io/instance": primary_release,
                    "app.kubernetes.io/name": "milvus",
                },
                "expressionSelectors": [
                    {
                        "key": "app.kubernetes.io/component",
                        "operator": "In",
                        "values": ["cdc", "streamingnode"],
                    }
                ],
            },
            "direction": "both",
            "duration": duration,
        }
        if external_targets:
            spec["externalTargets"] = list(external_targets)
        else:
            spec["target"] = {
                "mode": "all",
                "selector": {
                    "namespaces": [namespace],
                    "labelSelectors": {
                        "app.kubernetes.io/instance": secondary_release,
                        "app.kubernetes.io/name": "milvus",
                        "app.kubernetes.io/component": "proxy",
                    },
                },
            }

        return {
            "apiVersion": "chaos-mesh.org/v1alpha1",
            "kind": "NetworkChaos",
            "metadata": {
                "name": chaos_name,
                "namespace": namespace,
                "labels": {
                    "app.kubernetes.io/part-of": "import-2pc",
                    "import-2pc-case": "cdc-commit-replay",
                },
            },
            "spec": spec,
        }

    def _kubectl_delete_resource(self, kind, name, namespace="chaos-testing", timeout=60):
        if not shutil.which("kubectl"):
            pytest.fail("kubectl is required for import 2PC infra-dependent tests")
        env = os.environ.copy()
        env.setdefault("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        result = subprocess.run(
            ["kubectl", "delete", kind, name, "-n", namespace, "--ignore-not-found=true"],
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout,
            check=False,
        )
        assert result.returncode == 0, {"stdout": result.stdout, "stderr": result.stderr, "kind": kind, "name": name}
        return result

    def _wait_networkchaos_injected(self, chaos_name, namespace="chaos-testing", timeout=90):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            chaos = self._kubectl_json(
                ["get", "networkchaos", chaos_name, "-n", namespace, "-o", "json"],
                namespace=namespace,
                timeout=30,
                fail_message=f"cannot inspect NetworkChaos {chaos_name}",
            )
            status = chaos.get("status", {})
            experiment = status.get("experiment", {})
            records = experiment.get("containerRecords", [])
            injected = sum(int(record.get("injectedCount", 0) or 0) for record in records)
            instances = status.get("instances", {})
            last = {
                "desiredPhase": experiment.get("desiredPhase"),
                "injectedCount": injected,
                "instances": instances,
                "conditions": status.get("conditions", []),
            }
            if injected > 0 or instances:
                return chaos, True
            time.sleep(3)
        return last, False

    def _assert_secondary_not_committed_during_partition(
        self,
        secondary,
        collection_name,
        job_id,
        expected_ids,
        timeout=20,
    ):
        deadline = time.time() + timeout
        last_progress = None
        last_seen = set()
        while time.time() < deadline:
            last_progress = secondary["import_job"].get_import_job_progress(job_id)
            state = last_progress.get("data", {}).get("state") if last_progress.get("code") == 0 else None
            assert state != "Completed", {
                "reason": "secondary completed while CDC NetworkChaos partition was still active",
                "progress": last_progress,
            }
            last_seen = self._query_imported_ids_with_client(
                secondary["vector"],
                collection_name,
                sorted(expected_ids),
            )
            assert not last_seen, {
                "reason": "secondary exposed import rows while CDC NetworkChaos partition was still active",
                "seen": last_seen,
                "progress": last_progress,
            }
            time.sleep(2)
        return last_progress, last_seen


@pytest.mark.tags(CaseLabel.L3)
class TestImport2PCInfraDependent(Import2PCInfraBase):
    def test_import_2pc_cdc_multiple_manual_jobs_same_collection_primary_secondary_consistent(
        self,
        secondary_endpoint,
        secondary_token,
        secondary_release_name,
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
                rsp, completed = client.wait_import_job_state(first_job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
                assert completed, rsp

            seen, visible = self._wait_imported_ids_visible(collection_name, first_ids, timeout=180)
            assert visible, {"expected": first_ids, "seen": seen}
            seen, absent = self._wait_imported_ids_absent(collection_name, second_ids, timeout=20)
            assert absent, {"unexpected_second_job_ids": seen}

            commit_rsp = self.import_job_client.commit_import_job(second_job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            for client in (self.import_job_client, secondary["import_job"]):
                rsp, completed = client.wait_import_job_state(second_job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT)
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

    def test_import_2pc_cdc_commit_replays_after_secondary_proxy_outage(
        self,
        secondary_endpoint,
        secondary_token,
        secondary_release_name,
        secondary_minio_host,
        secondary_bucket_name,
        secondary_root_path,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """
        target: verify CDC replays CommitImportMessage when secondary proxy is unavailable during primary commit
        method: wait until both clusters hold the import job at Uncommitted, scale secondary proxy workloads to zero,
                commit on primary, then restore secondary proxy workloads
        expected: primary completes while secondary misses the live commit delivery; after proxy restore,
                  CDC replay advances the same job to Completed and primary/secondary PK sets match
        """
        self._require_cdc_rest_env(
            "IMP-REP-102",
            secondary_endpoint,
            secondary_minio_host,
            secondary_bucket_name,
            source_cluster_id,
            target_cluster_id,
        )
        secondary_release = self._resolve_secondary_release_name(secondary_release_name, "IMP-REP-102")
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

        collection_name = gen_collection_name(prefix="import_2pc_cdc_commit_replay")
        rows = self._make_rows(32000, 24, phase=320)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_cdc_commit_replay_{uuid4()}.parquet"
        file_path = None
        job_id = None
        secondary_proxy_workloads = []

        try:
            self._create_base_collection(collection_name, shards_num=4)
            rsp, synced = self._wait_collection_exists_with_client(secondary["collection"], collection_name)
            assert synced, rsp
            rsp, loaded = self._wait_collection_loaded_with_client(secondary["collection"], collection_name)
            assert loaded, rsp
            file_path = self._write_parquet_and_upload_to_both_clusters(rows, file_name, secondary["storage"])

            job_id = self._create_manual_import_job(collection_name, file_name)
            primary_rsp, primary_ready = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_ready, primary_rsp
            secondary_rsp, secondary_ready = secondary["import_job"].wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_ready, secondary_rsp

            secondary_proxy_workloads = self._kubectl_get_component_workloads(secondary_release, "proxy")
            self._kubectl_scale_workloads(secondary_proxy_workloads, 0, timeout=300)

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            primary_rsp, primary_completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_completed, primary_rsp
            primary_seen, primary_visible = self._wait_imported_ids_visible(
                collection_name,
                expected_ids,
                timeout=180,
            )
            assert primary_visible, {"expected": expected_ids, "seen": primary_seen}

            self._kubectl_restore_workloads(secondary_proxy_workloads, timeout=300)
            secondary_proxy_workloads = []
            cr_status, healthy = self._wait_milvus_cr_healthy(secondary_release, timeout=300)
            assert healthy, cr_status

            secondary_rsp, secondary_completed = secondary["import_job"].wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_completed, secondary_rsp
            secondary_seen, secondary_visible = self._wait_imported_ids_visible_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                expected_ids,
                timeout=180,
            )
            assert secondary_visible, {"expected": expected_ids, "seen": secondary_seen}
            assert primary_seen == secondary_seen == expected_ids

        finally:
            if secondary_proxy_workloads:
                self._kubectl_restore_workloads(secondary_proxy_workloads, timeout=300)
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    def test_import_2pc_cdc_commit_replays_after_chaos_mesh_network_partition(
        self,
        release_name,
        secondary_endpoint,
        secondary_token,
        secondary_release_name,
        secondary_minio_host,
        secondary_bucket_name,
        secondary_root_path,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """
        target: verify CDC replays CommitImportMessage after a Chaos Mesh partition blocks primary replication traffic to secondary
        method: hold a manual import job at Uncommitted on both clusters, apply Chaos Mesh NetworkChaos partition
                from primary cdc/streamingnode pods to the secondary Milvus endpoint IP, commit on primary, then remove partition
        expected: primary completes while secondary remains Uncommitted/invisible during the partition; after partition removal,
                  CDC reconnects from checkpoint, replays CommitImportMessage, and primary/secondary PK sets match
        """
        case_id = "IMP-REP-103"
        self._require_cdc_rest_env(
            case_id,
            secondary_endpoint,
            secondary_minio_host,
            secondary_bucket_name,
            source_cluster_id,
            target_cluster_id,
        )
        self._require_networkchaos_crd()
        namespace = os.environ.get("IMPORT_2PC_CHAOS_NAMESPACE", "chaos-testing")
        primary_release = self._resolve_primary_release_name(release_name, case_id, namespace=namespace)
        secondary_release = self._resolve_secondary_release_name(secondary_release_name, case_id)
        self._skip_if_release_has_no_component(primary_release, "streamingnode", case_id, namespace=namespace)
        self._skip_if_release_has_no_component(secondary_release, "proxy", case_id, namespace=namespace)
        secondary_partition_target = os.environ.get("IMPORT_2PC_CHAOS_SECONDARY_TARGET")
        if not secondary_partition_target:
            secondary_partition_target = urlparse(secondary_endpoint).hostname
        assert secondary_partition_target, {
            "reason": "cannot infer secondary endpoint host for Chaos Mesh externalTargets",
            "secondary_endpoint": secondary_endpoint,
        }

        chaos_duration = os.environ.get("IMPORT_2PC_CHAOS_DURATION", "10m")
        chaos_settle_seconds = int(os.environ.get("IMPORT_2PC_CHAOS_SETTLE_SECONDS", "15"))
        chaos_hold_seconds = int(os.environ.get("IMPORT_2PC_CHAOS_HOLD_SECONDS", "20"))
        chaos_name = f"import-2pc-cdc-net-{uuid4().hex[:10]}"
        chaos_applied = False

        primary_status, primary_healthy = self._wait_milvus_cr_healthy(
            primary_release, namespace=namespace, timeout=120
        )
        assert primary_healthy, primary_status
        secondary_status, secondary_healthy = self._wait_milvus_cr_healthy(
            secondary_release, namespace=namespace, timeout=120
        )
        assert secondary_healthy, secondary_status

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

        collection_name = gen_collection_name(prefix="import_2pc_cdc_net_partition")
        rows = self._make_rows(32200, 24, phase=322)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_cdc_net_partition_{uuid4()}.parquet"
        file_path = None
        job_id = None

        try:
            self._create_base_collection(collection_name, shards_num=4)
            rsp, synced = self._wait_collection_exists_with_client(secondary["collection"], collection_name)
            assert synced, rsp
            rsp, loaded = self._wait_collection_loaded_with_client(secondary["collection"], collection_name)
            assert loaded, rsp
            file_path = self._write_parquet_and_upload_to_both_clusters(rows, file_name, secondary["storage"])

            job_id = self._create_manual_import_job(collection_name, file_name)
            primary_rsp, primary_ready = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_ready, primary_rsp
            secondary_rsp, secondary_ready = secondary["import_job"].wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_ready, secondary_rsp

            chaos_manifest = self._build_cdc_proxy_network_partition_manifest(
                chaos_name,
                namespace,
                primary_release,
                secondary_release,
                external_targets=[secondary_partition_target],
                duration=chaos_duration,
            )
            self._kubectl_apply_manifest(chaos_manifest)
            chaos_applied = True
            chaos_status, injected = self._wait_networkchaos_injected(
                chaos_name,
                namespace=namespace,
                timeout=90,
            )
            assert injected, {"networkchaos": chaos_name, "last_status": chaos_status}
            time.sleep(chaos_settle_seconds)

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            primary_rsp, primary_completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_completed, primary_rsp
            primary_seen, primary_visible = self._wait_imported_ids_visible(
                collection_name,
                expected_ids,
                timeout=180,
            )
            assert primary_visible, {"expected": expected_ids, "seen": primary_seen}

            self._assert_secondary_not_committed_during_partition(
                secondary,
                collection_name,
                job_id,
                expected_ids,
                timeout=chaos_hold_seconds,
            )

            self._kubectl_delete_resource("networkchaos", chaos_name, namespace=namespace, timeout=60)
            chaos_applied = False
            time.sleep(10)

            secondary_rsp, secondary_completed = secondary["import_job"].wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_completed, secondary_rsp
            secondary_seen, secondary_visible = self._wait_imported_ids_visible_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                expected_ids,
                timeout=180,
            )
            assert secondary_visible, {"expected": expected_ids, "seen": secondary_seen}
            assert primary_seen == secondary_seen == expected_ids

        finally:
            if chaos_applied:
                self._kubectl_delete_resource("networkchaos", chaos_name, namespace=namespace, timeout=60)
                time.sleep(10)
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    def test_import_2pc_cdc_commit_before_secondary_import_ready_recovers_after_datanode_restore(
        self,
        secondary_endpoint,
        secondary_token,
        secondary_release_name,
        secondary_minio_host,
        secondary_bucket_name,
        secondary_root_path,
        source_cluster_id,
        target_cluster_id,
        pchannel_num,
    ):
        """
        target: verify primary CommitImport does not permanently lose secondary import data when secondary import is not ready
        method: pause secondary datanode workloads before creating a manual import, wait for primary Uncommitted,
                commit primary while secondary cannot finish import, then restore secondary datanode workloads
        expected: primary data becomes visible; secondary does not expose partial import data while datanode is paused,
                  and eventually reaches Completed with the same PK set after datanode restore
        """
        self._require_cdc_rest_env(
            "IMP-REP-101",
            secondary_endpoint,
            secondary_minio_host,
            secondary_bucket_name,
            source_cluster_id,
            target_cluster_id,
        )
        secondary_release = self._resolve_secondary_release_name(secondary_release_name, "IMP-REP-101")
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

        collection_name = gen_collection_name(prefix="import_2pc_cdc_secondary_not_ready")
        rows = self._make_rows(32100, 24, phase=321)
        expected_ids = {row["id"] for row in rows}
        file_name = f"import_2pc_cdc_secondary_not_ready_{uuid4()}.parquet"
        file_path = None
        job_id = None
        secondary_datanode_workloads = []

        try:
            self._create_base_collection(collection_name, shards_num=4)
            rsp, synced = self._wait_collection_exists_with_client(secondary["collection"], collection_name)
            assert synced, rsp
            rsp, loaded = self._wait_collection_loaded_with_client(secondary["collection"], collection_name)
            assert loaded, rsp
            file_path = self._write_parquet_and_upload_to_both_clusters(rows, file_name, secondary["storage"])

            secondary_datanode_workloads = self._kubectl_get_component_workloads(secondary_release, "datanode")
            self._kubectl_scale_workloads(secondary_datanode_workloads, 0, timeout=300)

            job_id = self._create_manual_import_job(collection_name, file_name)
            primary_rsp, primary_ready = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_ready, primary_rsp

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            primary_rsp, primary_completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert primary_completed, primary_rsp
            primary_seen, primary_visible = self._wait_imported_ids_visible(
                collection_name,
                expected_ids,
                timeout=180,
            )
            assert primary_visible, {"expected": expected_ids, "seen": primary_seen}

            secondary_seen = self._query_imported_ids_with_client(
                secondary["vector"],
                collection_name,
                sorted(expected_ids),
            )
            assert not secondary_seen, {
                "reason": "secondary exposed import rows before local import completed",
                "seen": secondary_seen,
            }

            self._kubectl_restore_workloads(secondary_datanode_workloads, timeout=300)
            secondary_datanode_workloads = []
            cr_status, healthy = self._wait_milvus_cr_healthy(secondary_release, timeout=300)
            assert healthy, cr_status

            secondary_rsp, secondary_completed = secondary["import_job"].wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert secondary_completed, secondary_rsp
            secondary_seen, secondary_visible = self._wait_imported_ids_visible_with_clients(
                secondary["collection"],
                secondary["vector"],
                collection_name,
                expected_ids,
                timeout=180,
            )
            assert secondary_visible, {"expected": expected_ids, "seen": secondary_seen}
            assert primary_seen == secondary_seen == expected_ids

        finally:
            if secondary_datanode_workloads:
                self._kubectl_restore_workloads(secondary_datanode_workloads, timeout=300)
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
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
            rsp, ready = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
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
            rsp, ready = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
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
            rsp, ready = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
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
            rsp, ready = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
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

    def test_import_2pc_backup_start_end_ts_filters_real_insert_binlog_window(self):
        """
        target: verify backup import start_ts/end_ts filters real insert binlog rows by timestamp window
        method: generate old, in-window, and late insert binlogs in a source collection, then import the whole
                partition prefix with backup=true, auto_commit=false, and start_ts/end_ts covering only the middle batch
        expected: only in-window PKs are counted and visible after CommitImport; old and late PKs remain absent
        """
        old_rows = self._make_rows(33000, 6, phase=330)
        included_rows = self._make_rows(33100, 6, phase=331)
        late_rows = self._make_rows(33200, 6, phase=332)
        fixture = self._build_generated_backup_window_fixture(
            gen_collection_name(prefix="import_2pc_backup_window_src"),
            old_rows,
            included_rows,
            late_rows,
        )
        collection_name = gen_collection_name(prefix="import_2pc_backup_window")
        self._create_base_collection(collection_name)

        expected_ids = set(fixture["expectedIds"])
        excluded_ids = set(fixture["excludedIds"])
        job_id = None
        try:
            job_id = self._create_manual_import_job_with_files(
                collection_name,
                fixture["files"],
                {
                    "backup": "true",
                    "storage_version": str(fixture["storageVersion"]),
                    "start_ts": fixture["start_ts"],
                    "end_ts": fixture["end_ts"],
                },
                partition_name=fixture["partitionName"],
            )
            rsp, ready = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ready, rsp
            assert rsp["data"]["importedRows"] == fixture["expectedCount"], rsp

            seen, absent = self._wait_imported_ids_absent(collection_name, expected_ids, timeout=20)
            assert absent, {"unexpected_visible_ids_before_commit": seen}

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert completed, rsp

            seen, visible = self._wait_imported_ids_visible(collection_name, expected_ids, timeout=180)
            assert visible, {"expected": expected_ids, "seen": seen, "fixture": fixture}
            excluded_seen = self._query_imported_ids(collection_name, sorted(excluded_ids))
            assert not excluded_seen, {"excluded_ids": excluded_ids, "seen": excluded_seen, "fixture": fixture}
            count, count_ok = self._wait_count(collection_name, len(expected_ids), timeout=180)
            assert count_ok, {"expected_count": len(expected_ids), "last_count": count}

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            for prefix in fixture.get("copiedPrefixes", []):
                self._delete_storage_prefix(self.storage_client, prefix)

    def test_import_2pc_encrypted_backup_ezk_fixture_manual_commit_restores_rows(self):
        """
        target: verify encrypted backup import with ezk follows 2PC visibility and restores rows
        method: load an encrypted backup fixture from IMPORT_2PC_ENCRYPTED_BACKUP_FIXTURE, submit backup=true
                with ezk and auto_commit=false, then CommitImport
        expected: rows are invisible while Uncommitted and become visible only after CommitImport
        """
        fixture = self._load_json_fixture_from_env("IMPORT_2PC_ENCRYPTED_BACKUP_FIXTURE")
        assert fixture.get("ezk"), "IMPORT_2PC_ENCRYPTED_BACKUP_FIXTURE must contain ezk"
        collection_name = gen_collection_name(prefix="import_2pc_encrypted_backup_ezk")
        self._create_collection_from_fixture(collection_name, fixture)

        expected_ids = set(fixture.get("expectedIds", []))
        expected_count = fixture.get("expectedCount", len(expected_ids) if expected_ids else None)
        options = {"backup": "true", "ezk": str(fixture["ezk"])}
        if fixture.get("storageVersion") is not None:
            options["storage_version"] = str(fixture["storageVersion"])
        for key in ("start_ts", "end_ts", "skip_disk_quota_check"):
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
            rsp, ready = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ready, rsp
            if expected_count is not None:
                assert rsp["data"]["importedRows"] == expected_count, rsp

            if expected_ids:
                seen, absent = self._wait_imported_ids_absent(collection_name, expected_ids, timeout=20)
                assert absent, {"unexpected_visible_ids_before_commit": seen}

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
            rsp, ready = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
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

    def test_import_2pc_l0_start_end_ts_filters_real_delta_binlog_window(self):
        """
        target: verify L0 import start_ts/end_ts filters real delete binlog rows by timestamp window
        method: generate old, in-window, and late delete logs in a source collection, seed all rows into the target,
                then import the whole delta prefix with l0_import=true, auto_commit=false, and start_ts/end_ts
        expected: only in-window delete PKs disappear after CommitImport; old and late delete PKs remain visible
        """
        seed_rows = self._make_rows(33300, 18, phase=333)
        old_delete_ids = {row["id"] for row in seed_rows[0:4]}
        included_delete_ids = {row["id"] for row in seed_rows[4:10]}
        late_delete_ids = {row["id"] for row in seed_rows[10:14]}
        survivor_ids = {row["id"] for row in seed_rows} - included_delete_ids
        collection_name = gen_collection_name(prefix="import_2pc_l0_window")
        self._create_base_collection(collection_name)

        job_id = None
        try:
            self._insert_rows(collection_name, seed_rows)
            seen, visible = self._wait_imported_ids_visible(
                collection_name,
                {row["id"] for row in seed_rows},
                timeout=120,
            )
            assert visible, {"expected": {row["id"] for row in seed_rows}, "seen": seen}
            flush_rsp, flushed = self._flush_collection_with_retry(collection_name)
            assert flushed, flush_rsp

            fixture = self._build_generated_l0_window_fixture(
                gen_collection_name(prefix="import_2pc_l0_window_src"),
                seed_rows,
                old_delete_ids,
                included_delete_ids,
                late_delete_ids,
            )

            release_rsp = self.collection_client.collection_release(collection_name=collection_name)
            assert release_rsp["code"] == 0, release_rsp
            job_id = self._create_manual_import_job_with_files(
                collection_name,
                fixture["files"],
                {
                    "l0_import": "true",
                    "storage_version": str(fixture["storageVersion"]),
                    "start_ts": fixture["start_ts"],
                    "end_ts": fixture["end_ts"],
                },
                partition_name=fixture["partitionName"],
            )
            rsp, ready = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
            assert ready, rsp

            load_rsp = self.collection_client.collection_load(collection_name=collection_name)
            assert load_rsp["code"] == 0, load_rsp
            self.wait_load_completed(collection_name, timeout=120)

            seen_before_commit = self._query_imported_ids(collection_name, included_delete_ids)
            assert seen_before_commit == included_delete_ids, {
                "expected_still_visible_before_commit": included_delete_ids,
                "seen": seen_before_commit,
            }

            commit_rsp = self.import_job_client.commit_import_job(job_id)
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            assert completed, rsp

            seen, deleted = self._wait_imported_ids_absent(collection_name, included_delete_ids, timeout=180)
            assert deleted, {"expected_deleted": included_delete_ids, "last_seen": seen, "fixture": fixture}
            seen, visible = self._wait_imported_ids_visible(collection_name, survivor_ids, timeout=180)
            assert visible, {"expected_survivors": survivor_ids, "seen": seen, "fixture": fixture}
            non_window_seen = self._query_imported_ids(collection_name, fixture["nonDeleteIds"])
            assert non_window_seen == set(fixture["nonDeleteIds"]), {
                "expected_non_window_delete_ids_to_survive": fixture["nonDeleteIds"],
                "seen": non_window_seen,
                "fixture": fixture,
            }

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
            rsp, ready = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
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

    def test_import_2pc_storage_v3_manifest_snapshot_restore_happy_path(self, release_name):
        """
        target: verify StorageV3 manifest data can be restored through the manifest-aware copy-segment path
        method: deploy or reuse a StorageV3 fixture source with common.storage.useLoonFFI=true, insert and flush rows,
                create a snapshot, restore it to a new collection, and query the restored rows
        expected: snapshot restore completes and restored collection exposes the exact source PK set
        """
        source = self._ensure_storage_fixture_source(3, release_name)
        source_collection_name = gen_collection_name(prefix="import_2pc_v3_snapshot_src")
        restored_collection_name = gen_collection_name(prefix="import_2pc_v3_snapshot_dst")
        snapshot_name = f"import_2pc_v3_snapshot_{uuid4().hex}"
        rows = self._make_rows(30400, 12, phase=304)
        expected_ids = {row["id"] for row in rows}
        source_client = MilvusClient(uri=source["endpoint"], token=source["token"])

        try:
            self._create_base_collection_with_clients(source["collection"], source_collection_name)
            self._insert_rows_with_client(source["vector"], source_collection_name, rows)
            flush_rsp, flushed = self._flush_collection_with_client_retry(
                source["collection"],
                source_collection_name,
            )
            assert flushed, flush_rsp

            source_client.create_snapshot(
                snapshot_name,
                source_collection_name,
                description="import 2pc storage v3 manifest restore coverage",
                compaction_protection_seconds=0,
                timeout=IMPORT_2PC_TIMEOUT,
            )
            snapshot = source_client.describe_snapshot(
                snapshot_name,
                source_collection_name,
                timeout=IMPORT_2PC_TIMEOUT,
            )
            assert snapshot.name == snapshot_name, snapshot
            assert snapshot.collection_name == source_collection_name, snapshot
            assert snapshot.s3_location, snapshot

            restore_job_id = source_client.restore_snapshot(
                snapshot_name,
                source_collection_name,
                restored_collection_name,
                timeout=IMPORT_2PC_TIMEOUT,
            )
            deadline = time.time() + IMPORT_2PC_TIMEOUT
            restore_info = None
            while time.time() < deadline:
                restore_info = source_client.get_restore_snapshot_state(
                    restore_job_id,
                    timeout=30,
                )
                if restore_info.state == "RestoreSnapshotCompleted":
                    break
                if restore_info.state == "RestoreSnapshotFailed":
                    pytest.fail(f"StorageV3 snapshot restore failed: {restore_info}")
                time.sleep(2)
            assert restore_info is not None and restore_info.state == "RestoreSnapshotCompleted", restore_info

            rsp, exists = self._wait_collection_exists_with_client(
                source["collection"],
                restored_collection_name,
                timeout=180,
            )
            assert exists, rsp
            rsp = source["collection"].collection_load(collection_name=restored_collection_name)
            assert rsp["code"] == 0, rsp
            rsp, loaded = self._wait_collection_loaded_with_client(
                source["collection"],
                restored_collection_name,
                timeout=180,
            )
            assert loaded, rsp
            seen, visible = self._wait_imported_ids_visible_with_clients(
                source["collection"],
                source["vector"],
                restored_collection_name,
                expected_ids,
                timeout=180,
            )
            assert visible, {"expected": expected_ids, "seen": seen, "restore_info": restore_info}
            count, count_ok = self._wait_count_with_clients(
                source["collection"],
                source["vector"],
                restored_collection_name,
                len(expected_ids),
                timeout=180,
            )
            assert count_ok, {"expected_count": len(expected_ids), "last_count": count}

        finally:
            try:
                source_client.drop_snapshot(snapshot_name, source_collection_name, timeout=60)
            except Exception:
                pass
            for collection_name in (restored_collection_name, source_collection_name):
                try:
                    source["collection"].collection_drop({"collectionName": collection_name})
                except Exception:
                    pass
            source_client.close()

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
            rsp, ready = self.import_job_client.wait_import_job_state(job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT)
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

    def test_import_2pc_long_stability_fresh_and_accumulate_manual_commit(self):
        """
        target: verify Import 2PC remains stable over a long fresh+accumulate loop
        method: for IMPORT_2PC_STABILITY_SECONDS seconds, repeatedly create manual import jobs on one collection,
                hold each at Uncommitted, verify invisibility, commit, and verify cumulative visibility/count
        expected: every iteration preserves Uncommitted invisibility, Completed visibility, and cumulative row count
        """
        duration_seconds = int(os.environ.get("IMPORT_2PC_STABILITY_SECONDS", "1800"))
        rows_per_job = int(os.environ.get("IMPORT_2PC_STABILITY_ROWS_PER_JOB", "20"))
        assert duration_seconds > 0, "IMPORT_2PC_STABILITY_SECONDS must be positive"
        assert rows_per_job > 0, "IMPORT_2PC_STABILITY_ROWS_PER_JOB must be positive"

        collection_name = gen_collection_name(prefix="import_2pc_long_stability")
        self._create_base_collection(collection_name, shards_num=4)
        deadline = time.time() + duration_seconds
        cumulative_ids = set()
        local_files = []
        job_ids = []
        iteration = 0

        try:
            while time.time() < deadline or iteration == 0:
                base_id = 34000 + iteration * 1000
                rows = self._make_rows(base_id, rows_per_job, phase=340 + iteration)
                ids = {row["id"] for row in rows}
                file_name = f"import_2pc_long_stability_{iteration}_{uuid4()}.parquet"
                local_files.append(self._write_parquet_and_upload(rows, file_name))

                job_id = self._create_manual_import_job(collection_name, file_name)
                job_ids.append(job_id)
                rsp, ready = self.import_job_client.wait_import_job_state(
                    job_id, "Uncommitted", timeout=IMPORT_2PC_TIMEOUT
                )
                assert ready, {"iteration": iteration, "rsp": rsp}
                assert rsp["data"]["importedRows"] == rows_per_job, rsp

                seen, absent = self._wait_imported_ids_absent(collection_name, ids, timeout=20)
                assert absent, {"iteration": iteration, "unexpected_visible_ids": seen}

                commit_rsp = self.import_job_client.commit_import_job(job_id)
                assert commit_rsp["code"] == 0, {"iteration": iteration, "rsp": commit_rsp}
                rsp, completed = self.import_job_client.wait_import_job_state(
                    job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
                )
                assert completed, {"iteration": iteration, "rsp": rsp}

                cumulative_ids.update(ids)
                seen, visible = self._wait_imported_ids_visible(collection_name, ids, timeout=180)
                assert visible, {"iteration": iteration, "expected": ids, "seen": seen}
                count, count_ok = self._wait_count(collection_name, len(cumulative_ids), timeout=180)
                assert count_ok, {
                    "iteration": iteration,
                    "expected_count": len(cumulative_ids),
                    "last_count": count,
                }
                iteration += 1

            assert iteration > 0

        finally:
            for job_id in job_ids:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            for file_path in local_files:
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)

    def test_import_2pc_throughput_baseline_records_import_index_commit_latency(self):
        """
        target: record Import 2PC throughput and phase latency baseline on a real instance
        method: import IMPORT_2PC_PERF_ROWS rows with auto_commit=false and record create->Uncommitted,
                commit->Completed, commit->query-visible, and total elapsed durations
        expected: job completes, all rows become visible, and timing metrics are emitted to a JSON result file
        """
        row_count = int(os.environ.get("IMPORT_2PC_PERF_ROWS", "5000"))
        assert row_count > 0, "IMPORT_2PC_PERF_ROWS must be positive"
        result_path = os.environ.get(
            "IMPORT_2PC_PERF_RESULT_PATH",
            f"/tmp/import_2pc_perf_baseline_{uuid4().hex}.json",
        )
        collection_name = gen_collection_name(prefix="import_2pc_perf_baseline")
        rows = self._make_rows(36000, row_count, phase=360)
        sample_ids = {row["id"] for row in rows[: min(20, row_count)]}
        file_name = f"import_2pc_perf_baseline_{uuid4()}.parquet"
        file_path = None
        job_id = None

        try:
            self._create_base_collection(collection_name, shards_num=4)
            file_path = self._write_parquet_and_upload(rows, file_name)

            t_create = time.time()
            job_id = self._create_manual_import_job(collection_name, file_name)
            rsp, ready = self.import_job_client.wait_import_job_state(
                job_id, "Uncommitted", timeout=max(IMPORT_2PC_TIMEOUT, 900)
            )
            t_uncommitted = time.time()
            assert ready, rsp
            assert rsp["data"]["importedRows"] == row_count, rsp

            seen, absent = self._wait_imported_ids_absent(collection_name, sample_ids, timeout=20)
            assert absent, {"unexpected_visible_ids_before_commit": seen}

            t_commit_start = time.time()
            commit_rsp = self.import_job_client.commit_import_job(job_id)
            t_commit_return = time.time()
            assert commit_rsp["code"] == 0, commit_rsp
            rsp, completed = self.import_job_client.wait_import_job_state(
                job_id, "Completed", timeout=IMPORT_2PC_TIMEOUT
            )
            t_completed = time.time()
            assert completed, rsp

            seen, visible = self._wait_imported_ids_visible(collection_name, sample_ids, timeout=300)
            t_visible = time.time()
            assert visible, {"expected_sample": sample_ids, "seen": seen}
            count, count_ok = self._wait_count(collection_name, row_count, timeout=300)
            assert count_ok, {"expected_count": row_count, "last_count": count}

            metrics = {
                "collection": collection_name,
                "job_id": job_id,
                "rows": row_count,
                "create_to_uncommitted_seconds": round(t_uncommitted - t_create, 3),
                "commit_rpc_seconds": round(t_commit_return - t_commit_start, 3),
                "commit_to_completed_seconds": round(t_completed - t_commit_return, 3),
                "commit_to_visible_seconds": round(t_visible - t_commit_return, 3),
                "total_seconds": round(t_visible - t_create, 3),
                "rows_per_second_to_visible": round(row_count / max(t_visible - t_create, 0.001), 3),
            }
            with open(result_path, "w", encoding="utf-8") as result_file:
                json.dump(metrics, result_file, indent=2, sort_keys=True)
            print(json.dumps(metrics, sort_keys=True))

        finally:
            if job_id is not None:
                progress_rsp = self.import_job_client.get_import_job_progress(job_id)
                state = progress_rsp.get("data", {}).get("state") if progress_rsp.get("code") == 0 else None
                if state in ("Pending", "PreImporting", "Importing", "Sorting", "IndexBuilding", "Uncommitted"):
                    self.import_job_client.abort_import_job(job_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

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
        target: verify import is rejected or fails when the requested import file size exceeds disk quota
        method: set quotaAndLimits.limitWriting.diskProtection quota to a tiny value in the Milvus CR,
                generate and upload a real parquet file larger than that quota, then submit a manual import
        expected: import create or job execution fails with quota/disk reason and no imported rows become visible
        """
        disk_quota_mb = int(os.environ.get("IMPORT_2PC_DISK_QUOTA_MB", "1"))
        row_count = int(os.environ.get("IMPORT_2PC_DISK_QUOTA_ROWS", "50000"))
        assert 1 <= disk_quota_mb <= 100, "IMPORT_2PC_DISK_QUOTA_MB must be 1..100 for this L3 test"
        assert row_count > 0, "IMPORT_2PC_DISK_QUOTA_ROWS must be positive"
        collection_name = gen_collection_name(prefix="import_2pc_disk_quota")
        rows = self._make_rows(25000, row_count, phase=250)
        for row in rows:
            row["tag"] = f"quota_{row['id']}_{uuid4().hex}_{uuid4().hex}"
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
                        "enabled": True,
                        "limitWriting": {
                            "diskProtection": {
                                "enabled": True,
                                "diskQuota": disk_quota_mb,
                                "diskQuotaPerCollection": disk_quota_mb,
                            }
                        },
                    }
                },
            )
            disk_quota_config_applied = True
            self._restart_config_components(release_name, ("mixcoord", "proxy"))

            self._create_base_collection(collection_name)
            file_path = self._write_parquet_and_upload(rows, file_name)
            file_size = os.path.getsize(file_path)
            assert file_size > disk_quota_mb * 1024 * 1024, {
                "reason": "generated import file must exceed configured disk quota",
                "file_size": file_size,
                "disk_quota_mb": disk_quota_mb,
                "row_count": row_count,
            }
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
                        pytest.fail(f"disk quota exhausted import job became commit-capable instead of failing: {rsp}")
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
                self._restart_config_components(release_name, ("mixcoord", "proxy"))
