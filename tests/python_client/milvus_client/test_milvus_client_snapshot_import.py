"""Snapshot Import E2E: smoke, functionality, errors, security and compatibility.

Each test method has one level shared by all its parameterized instances.
PR CI runs L0/L1; the daily regression runs L2.
"""

import base64
import hashlib
import json
import time
from copy import deepcopy
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import ml_dtypes
import numpy as np
import pytest
import urllib3
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from common.external_table_common import build_external_spec, get_minio_config, new_minio_client
from common.snapshot_import_common import (
    FUNCTION_CASE_IDS,
    FUNCTION_CASES,
    FunctionMockClient,
    IndexEvidencePending,
    SnapshotBatchBase,
    _data_key,
    _legacy_delete_records,
    _segment_header,
    _snapshot_read_path,
    assert_external_credentials_rejected,
    assert_external_snapshot_search,
    assert_hnsw_definition,
    assert_owned_hnsw_search,
    assert_scalar_rows,
    assert_snapshot_objects_unchanged,
    assert_source_readonly,
    assert_target_snapshot_storage,
    avro_manifest,
    build_legacy_storage_fixture,
    bundle_fingerprints,
    copy_legacy_descriptors,
    export_snapshot,
    external_snapshot_bucket,
    foreign_export_uri,
    function_rows,
    function_spec,
    inspect_snapshot_ownership,
    inspect_snapshot_text_lobs,
    int64_partition_slot,
    normalize_output,
    object_bytes,
    owned_bundle_fingerprints,
    ownership_rows,
    ownership_runtime_config,
    plain_rows,
    put_target_bucket_decoys,
    read_referenced_snapshot,
    read_segment_rows,
    read_snapshot_inventory,
    read_snapshot_objects,
    read_snapshot_partition_inventory,
    read_snapshot_partition_rows,
    relocate_snapshot_bundle,
    remove_snapshot_export,
    scalar_fields,
    scalar_filters,
    scalar_rows,
    snapshot_file_fingerprints,
    snapshot_manifest_versions,
    snapshot_text_rows,
    wait_for_snapshot_export,
)
from minio.credentials import StaticProvider
from minio.minioadmin import MinioAdmin
from pymilvus import DataType
from pymilvus.bulk_writer import abort_import, bulk_import, commit_import, get_import_progress
from pymilvus.exceptions import MilvusException
from utils.util_log import test_log as log

FIELDS = ["id", "source_uid", "score", "flag", "tag", "vec"]
DIM = 32


class TestMilvusClientSnapshotImportIndependent(TestMilvusClientV2Base):
    """Tests requiring private snapshot producers and Import destinations.

    When to add tests here: snapshot Import cases that modify data or schema.
    Fixture: snapshot_resources owns only this test's client, collections and snapshot.
    """

    skip_global_role_cleanup = True

    def setup_method(self, method):
        super().setup_method(method)
        self.tear_down_collection_names = []
        self.resource_group_list = []

    @pytest.mark.tags(CaseLabel.L0)
    def test_referenced_snapshot_auto_commit(self, snapshot_resources, minio_host, minio_bucket, db_name):
        """
        target: SI-SM-01, automatically import a V3 Referenced snapshot into an existing target
        method: insert 128 source rows and one target sentinel; snapshot; Import; query/search; reload
        expected: Completed without CommitImport; 129 exact rows; unchanged target definitions and source references
        """
        client, source, target, snapshot = snapshot_resources

        # Step 1: Create matching schemas and verify actual FieldIDs.
        self._create_collection(client, source, "src_vec_idx")
        self._create_collection(client, target, "dst_vec_idx")
        source_fields = self.describe_collection(client, source)[0]["fields"]
        target_fields = self.describe_collection(client, target)[0]["fields"]
        assert {f["name"]: f["field_id"] for f in source_fields} == {f["name"]: f["field_id"] for f in target_fields}
        assert {f["name"] for f in source_fields} == set(FIELDS)

        # Step 2: Insert source rows and a target sentinel; freeze the input truth.
        rows = [
            {
                "id": i,
                "source_uid": f"src_{i}",
                "score": i * 10,
                "flag": i % 2 == 0,
                "tag": f"tag_{i}",
                "vec": [(i * DIM + j) / 4096 for j in range(DIM)],
            }
            for i in range(128)
        ]
        sentinel = {
            "id": 10000,
            "source_uid": "target_existing",
            "score": -1,
            "flag": True,
            "tag": "sentinel",
            "vec": [2 + j / 4096 for j in range(DIM)],
        }
        expected = {row["id"]: row for row in deepcopy(rows + [sentinel])}
        self.insert(client, source, rows)
        self.insert(client, target, [deepcopy(sentinel)])
        self.flush(client, source)
        self.flush(client, target)
        self.load_collection(client, target)
        self._assert_rows(client, target, {sentinel["id"]: sentinel})
        definitions_before = self._definitions(client, target)

        # Step 3: Create a Referenced snapshot and verify its source is Storage V3.
        self.create_snapshot(client, snapshot, source, db_name=db_name)
        metadata_uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location
        store = new_minio_client(get_minio_config(minio_host, minio_bucket))
        metadata, segments, source_objects = read_referenced_snapshot(store, minio_bucket, metadata_uri)
        assert metadata["layout"] in ("SnapshotLayoutReferenced", 1)
        data_segments = [segment for segment in segments if segment["num_of_rows"] > 0]
        assert data_segments and all(segment["storage_version"] == 3 for segment in data_segments)
        assert sum(segment["num_of_rows"] for segment in data_segments) == len(rows)

        # Step 4: Import the original URI with auto_commit=true; do not call CommitImport.
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[metadata_uri]],
            options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
            api_key=cf.param_info.param_token,
            timeout=30,
        ).json()
        assert response["code"] == 0, response
        job_id = response["data"]["jobId"]
        assert job_id
        log.info(f"SI-SM-01 Import job_id={job_id}, source={source}, target={target}")

        # Step 5: Wait for a terminal job state and verify automatic completion.
        progress = self._wait_for_import(url, job_id, db_name)
        self._retain_import_resources = False
        assert progress["state"] == "Completed", progress
        assert progress["importedRows"] == len(rows), progress

        # Step 6: Wait for the target index, then verify every row and FLAT/L2 top-k.
        assert self.wait_for_index_ready(client, target, index_name="dst_vec_idx", timeout=120)
        self._assert_rows(client, target, expected)
        self._assert_search(client, target, expected)

        # Step 7: Release/load and repeat the complete data checks.
        self.release_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
        self.load_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
        self._assert_rows(client, target, expected)
        self._assert_search(client, target, expected)

        # Step 8: Import must not replace target definitions or modify snapshot objects.
        assert self._definitions(client, target) == definitions_before
        assert self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location == metadata_uri
        assert read_snapshot_objects(store, minio_bucket, metadata_uri) == source_objects

    @pytest.mark.tags(CaseLabel.L0)
    def test_self_contained_snapshot_relocated_manual_commit(self, snapshot_resources, snapshot_exports, db_name):
        """
        target: SI-SM-02, import a relocated SelfContained snapshot with explicit commit
        method: export; copy the whole bundle; remove the old copy; Import; check invisibility; Commit; reload
        expected: only the sentinel before Commit; 129 exact rows afterwards; no dependence on the old export prefix
        """
        client, source, target, snapshot = snapshot_resources
        store, bucket, export_a, export_b = snapshot_exports
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        api_key = cf.param_info.param_token

        # Step 1: Prepare the same schema/data as SI-SM-01 and one existing target row.
        self._create_collection(client, source, "src_vec_idx")
        self._create_collection(client, target, "dst_vec_idx")
        source_fields = self.describe_collection(client, source)[0]["fields"]
        target_fields = self.describe_collection(client, target)[0]["fields"]
        assert {f["name"]: f["field_id"] for f in source_fields} == {f["name"]: f["field_id"] for f in target_fields}
        rows = [
            {
                "id": i,
                "source_uid": f"src_{i}",
                "score": i * 10,
                "flag": i % 2 == 0,
                "tag": f"tag_{i}",
                "vec": [(i * DIM + j) / 4096 for j in range(DIM)],
            }
            for i in range(128)
        ]
        sentinel = {
            "id": 10000,
            "source_uid": "target_existing",
            "score": -1,
            "flag": True,
            "tag": "sentinel",
            "vec": [2 + j / 4096 for j in range(DIM)],
        }
        expected = {row["id"]: row for row in deepcopy(rows + [sentinel])}
        self.insert(client, source, rows)
        self.insert(client, target, [deepcopy(sentinel)])
        self.flush(client, source)
        self.flush(client, target)
        self.load_collection(client, target)
        self._assert_rows(client, target, {sentinel["id"]: sentinel})

        # Step 2: Export a snapshot into this test's private export_a.
        self.create_snapshot(client, snapshot, source, db_name=db_name)
        self._retain_import_resources = True
        export_id = export_snapshot(url, source, snapshot, export_a, db_name, api_key)
        exported = wait_for_snapshot_export(url, export_id, db_name, api_key)
        self._retain_import_resources = False
        assert exported["state"] == "ExportSnapshotCompleted", exported
        assert exported["collectionName"] == source and exported["snapshotName"] == snapshot
        assert int(exported["totalBytes"]) > 0, exported

        # Step 3: Copy all objects unchanged, verify their bytes, then remove only export_a.
        moved_uri = relocate_snapshot_bundle(
            store, bucket, exported["snapshotMetadataURI"], export_a, export_b, expected_rows=len(rows)
        )
        assert not list(store.list_objects(bucket, prefix=export_a, recursive=True))

        # Step 4: Import from export_b and wait for Uncommitted without publishing rows.
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[moved_uri]],
            options={"backup": "true", "source_type": "snapshot", "auto_commit": "false"},
            api_key=api_key,
            timeout=30,
        ).json()
        assert response["code"] == 0, response
        job_id = response["data"]["jobId"]
        assert job_id
        log.info(f"SI-SM-02 Import job_id={job_id}, source={source}, target={target}")
        progress = self._wait_for_import(url, job_id, db_name, expected_state="Uncommitted")
        self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
        assert progress["state"] == "Uncommitted", progress
        assert progress["importedRows"] == len(rows), progress
        self._assert_rows(client, target, {sentinel["id"]: sentinel})

        # Step 5: Explicitly commit the same job, then verify all fields and vector search.
        response = commit_import(url=url, job_id=job_id, db_name=db_name, api_key=api_key, timeout=30).json()
        assert response["code"] == 0, response
        progress = self._wait_for_import(url, job_id, db_name)
        self._retain_import_resources = False
        assert progress["state"] == "Completed", progress
        assert progress["importedRows"] == len(rows), progress
        assert self.wait_for_index_ready(client, target, index_name="dst_vec_idx", timeout=120)
        self._assert_rows(client, target, expected)
        self._assert_search(client, target, expected)

        # Step 6: Release/load and check again while the original export_a remains absent.
        self.release_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
        self.load_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
        self._assert_rows(client, target, expected)
        self._assert_search(client, target, expected)
        assert not list(store.list_objects(bucket, prefix=export_a, recursive=True))

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    def test_snapshot_import_uses_captured_content(self, snapshot_pair_resources, layout, db_name, record_property):
        """
        target: SI-FN-01, import each snapshot's captured content after the source changes
        method: capture S1; insert/delete; capture S2; import both into empty targets; compare all fields and reload
        expected: S1 has PK 0..127; S2 has PK 16..143; record manifest-version advancement separately
        """
        client, source, targets, snapshots, store, bucket, export_prefixes = snapshot_pair_resources
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        api_key = cf.param_info.param_token

        # Step 1: Create one source and two empty targets; freeze both independent oracles.
        self._create_collection(client, source, "src_vec_idx", shards_num=1)
        source_info = self.describe_collection(client, source)[0]
        assert source_info["num_shards"] == 1
        source_fields = source_info["fields"]
        for target in targets:
            self._create_collection(client, target, "dst_vec_idx", shards_num=1)
            target_fields = self.describe_collection(client, target)[0]["fields"]
            assert {f["name"]: f["field_id"] for f in source_fields} == {
                f["name"]: f["field_id"] for f in target_fields
            }
            self.load_collection(client, target)
            self._assert_rows(client, target, {})
        rows = [
            {
                "id": i,
                "source_uid": f"src_{i}",
                "score": i * 10,
                "flag": i % 2 == 0,
                "tag": f"tag_{i}",
                "vec": [(i * DIM + j) / 4096 for j in range(DIM)],
            }
            for i in range(144)
        ]
        expected_s1 = {row["id"]: row for row in deepcopy(rows[:128])}
        expected_s2 = {row["id"]: row for row in deepcopy(rows[16:])}

        # Step 2: Capture S1 before changing the source.
        self.insert(client, source, deepcopy(rows[:128]))
        self.flush(client, source)
        s1_uri, s1_meta, s1_objects = self._capture_snapshot_input(
            client, source, snapshots[0], store, bucket, db_name, inserted_rows=128, deleted_pks=set()
        )

        # Step 3: Add 16 rows and delete 16 old rows, then verify S2 really captured both.
        self.insert(client, source, deepcopy(rows[128:]))
        self.delete(client, source, ids=list(range(16)))
        self.flush(client, source)
        self.load_collection(client, source)
        self._assert_rows(client, source, expected_s2)  # Validate setup, never derive the oracle from query.
        s2_uri, s2_meta, s2_objects = self._capture_snapshot_input(
            client, source, snapshots[1], store, bucket, db_name, inserted_rows=144, deleted_pks=set(range(16))
        )
        assert_snapshot_objects_unchanged(store, bucket, s1_objects)

        # Step 4: Record whether the same source segment actually advanced its manifest version.
        versions_s1, versions_s2 = snapshot_manifest_versions(s1_meta), snapshot_manifest_versions(s2_meta)
        advanced = [
            segment_id
            for segment_id in versions_s1.keys() & versions_s2.keys()
            if versions_s1[segment_id][0] == versions_s2[segment_id][0]
            and versions_s2[segment_id][1] > versions_s1[segment_id][1]
        ]
        record_property("snapshot_layout", layout)
        record_property("same_segment_manifest_version_advance", "Observed" if advanced else "NotCovered")
        log.info(f"SI-FN-01 manifest versions: S1={versions_s1}, S2={versions_s2}")
        if not advanced:
            log.warning("SI-FN-01: manifest-version advancement not observed; only content isolation is exercised")

        # Step 5: For SelfContained, export both fixed snapshots AFTER the source changed.
        input_uris = [s1_uri, s2_uri]
        captured_objects = [s1_objects, s2_objects]
        if layout == "self_contained":
            for index, snapshot in enumerate(snapshots):
                self._retain_import_resources = True
                export_id = export_snapshot(url, source, snapshot, export_prefixes[index], db_name, api_key)
                exported = wait_for_snapshot_export(url, export_id, db_name, api_key)
                self._retain_import_resources = False
                assert exported["state"] == "ExportSnapshotCompleted", exported
                assert exported["collectionName"] == source and exported["snapshotName"] == snapshot
                input_uris[index] = exported["snapshotMetadataURI"]
                _, segments, objects, deleted = read_snapshot_inventory(store, bucket, input_uris[index], layout)
                assert sum(s["num_of_rows"] for s in segments if s["segment_level"] != 1) == (128, 144)[index]
                assert deleted == (set(), set(range(16)))[index]
                captured_objects.append(objects)

        # Step 6: Import S1 and S2 separately, then compare exact rows before and after reload.
        for target, metadata_uri, expected in zip(targets, input_uris, (expected_s1, expected_s2)):
            self._retain_import_resources = True
            response = bulk_import(
                url=url,
                collection_name=target,
                db_name=db_name,
                files=[[metadata_uri]],
                options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
                api_key=api_key,
                timeout=30,
            ).json()
            assert response["code"] == 0, response
            job_id = response["data"]["jobId"]
            assert job_id
            log.info(f"SI-FN-01 layout={layout}, target={target}, job_id={job_id}")
            progress = self._wait_for_import(url, job_id, db_name)
            self._retain_import_resources = False
            assert progress["state"] == "Completed", progress
            assert progress["importedRows"] == len(expected), progress
            assert self.wait_for_index_ready(client, target, index_name="dst_vec_idx", timeout=120)
            self._assert_rows(client, target, expected)
            self.release_collection(client, target)
            assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
            self.load_collection(client, target)
            assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
            self._assert_rows(client, target, expected)
        for objects in captured_objects:
            assert_snapshot_objects_unchanged(store, bucket, objects)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    @pytest.mark.parametrize(
        "pk_type", [pytest.param(DataType.INT64, id="int64"), pytest.param(DataType.VARCHAR, id="varchar")]
    )
    def test_snapshot_import_applies_l0_deletes(self, snapshot_l0_resources, layout, pk_type, db_name):
        """
        target: SI-FN-02, apply captured L0 deletes for Int64/VarChar PKs in both snapshot layouts
        method: insert 128; delete 32; require real applicable L0; import; compare all rows and reload
        expected: exactly the remaining 96 rows, deleted PKs absent; missing L0 is not a pass
        """
        client, source, target, snapshot, store, bucket, export_prefix = snapshot_l0_resources
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        api_key = cf.param_info.param_token

        # Step 1: Create matching schemas and freeze the input truth before any API call mutates it.
        self._create_collection(client, source, "src_vec_idx", pk_type=pk_type, shards_num=1)
        self._create_collection(client, target, "dst_vec_idx", pk_type=pk_type, shards_num=1)
        source_info = self.describe_collection(client, source)[0]
        target_info = self.describe_collection(client, target)[0]
        assert source_info["num_shards"] == target_info["num_shards"] == 1
        assert source_info["fields"] == target_info["fields"]
        self.load_collection(client, target)
        self._assert_rows(client, target, {}, filter_expr="")
        rows = [
            {
                "id": i if pk_type == DataType.INT64 else f"pk_{i:03d}",
                "source_uid": f"src_{i}",
                "score": i * 10,
                "flag": i % 2 == 0,
                "tag": f"tag_{i}",
                "vec": [(i * DIM + j) / 4096 for j in range(DIM)],
            }
            for i in range(128)
        ]
        deleted_pks = {row["id"] for row in rows[:32]}
        expected = {row["id"]: row for row in deepcopy(rows[32:])}

        # Step 2: Require 128 captured raw rows plus the 32 actual applicable L0 deletes.
        assert self.insert(client, source, deepcopy(rows))[0]["insert_count"] == len(rows)
        self.flush(client, source)
        assert self.delete(client, source, ids=sorted(deleted_pks))[0]["delete_count"] == len(deleted_pks)
        self.flush(client, source)
        metadata_uri, _, source_objects = self._capture_snapshot_input(
            client,
            source,
            snapshot,
            store,
            bucket,
            db_name,
            len(rows),
            deleted_pks,
            pk_type=pk_type,
            case_id="SI-FN-02",
        )

        # Step 3: SelfContained must preserve the same data and L0 inventory in the exported bundle.
        captured_objects = [source_objects]
        if layout == "self_contained":
            self._retain_import_resources = True
            export_id = export_snapshot(url, source, snapshot, export_prefix, db_name, api_key)
            exported = wait_for_snapshot_export(url, export_id, db_name, api_key)
            self._retain_import_resources = False
            assert exported["state"] == "ExportSnapshotCompleted", exported
            assert exported["collectionName"] == source and exported["snapshotName"] == snapshot
            metadata_uri = exported["snapshotMetadataURI"]
            _, segments, objects, deleted = read_snapshot_inventory(store, bucket, metadata_uri, layout, pk_type)
            assert sum(s["num_of_rows"] for s in segments if s["segment_level"] != 1) == len(rows)
            assert deleted == deleted_pks
            captured_objects.append(objects)

        # Step 4: Automatically commit one Import into the empty target.
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[metadata_uri]],
            options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
            api_key=api_key,
            timeout=30,
        ).json()
        assert response["code"] == 0, response
        job_id = response["data"]["jobId"]
        assert job_id
        log.info(f"SI-FN-02 layout={layout}, pk_type={pk_type.name}, target={target}, job_id={job_id}")
        progress = self._wait_for_import(url, job_id, db_name)
        self._retain_import_resources = False
        assert progress["state"] == "Completed", progress
        assert progress["importedRows"] == len(expected), progress
        assert self.wait_for_index_ready(client, target, index_name="dst_vec_idx", timeout=120)

        # Step 5: Check every surviving value and explicitly check deleted IDs, before and after reload.
        self._assert_rows(client, target, expected, filter_expr="")
        assert (
            self.get(client, target, ids=sorted(deleted_pks), output_fields=FIELDS, consistency_level="Strong")[0] == []
        )
        self.release_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
        self.load_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
        self._assert_rows(client, target, expected, filter_expr="")
        assert (
            self.get(client, target, ids=sorted(deleted_pks), output_fields=FIELDS, consistency_level="Strong")[0] == []
        )
        for objects in captured_objects:
            assert_snapshot_objects_unchanged(store, bucket, objects)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    @pytest.mark.parametrize(
        "pk_type", [pytest.param(DataType.INT64, id="int64"), pytest.param(DataType.VARCHAR, id="varchar")]
    )
    def test_snapshot_import_preserves_reinserts_and_target_rows(
        self, snapshot_reinsert_resources, layout, pk_type, db_name
    ):
        """
        target: SI-FN-03, keep source reinserts and isolate existing target rows from source deletes
        method: seed target; source insert/delete/reinsert; capture; manual Import; Commit; compare and reload
        expected: 16 target rows before Commit; 128 exact rows after; importedRows=112, not a physical-file audit
        """
        client, source, target, snapshot, store, bucket, export_prefix = snapshot_reinsert_resources
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        api_key = cf.param_info.param_token

        # Step 1: Prepare independent truth for original, reinserted and target-existing rows.
        self._create_collection(client, source, "src_vec_idx", pk_type=pk_type, shards_num=1)
        self._create_collection(client, target, "dst_vec_idx", pk_type=pk_type, shards_num=1)
        source_info = self.describe_collection(client, source)[0]
        target_info = self.describe_collection(client, target)[0]
        assert source_info["num_shards"] == target_info["num_shards"] == 1
        assert source_info["fields"] == target_info["fields"]
        rows = [
            {
                "id": i if pk_type == DataType.INT64 else f"pk_{i:03d}",
                "source_uid": f"src_{i}",
                "score": i * 10,
                "flag": i % 2 == 0,
                "tag": f"tag_{i}",
                "vec": [(i * DIM + j) / 4096 for j in range(DIM)],
            }
            for i in range(128)
        ]
        reinserted = deepcopy(rows[16:32])
        for row in reinserted:
            row.update(source_uid=f"new_{row['id']}", score=-100, flag=not row["flag"], tag="reinserted")
            row["vec"] = [value + 2 for value in row["vec"]]
        sentinels = deepcopy(rows[:16])
        for row in sentinels:
            row.update(source_uid=f"target_{row['id']}", score=-200, flag=not row["flag"], tag="target_existing")
            row["vec"] = [value + 4 for value in row["vec"]]
        deleted_pks = {row["id"] for row in rows[:32]}
        expected_source = {row["id"]: row for row in deepcopy(reinserted + rows[32:])}
        expected_before = {row["id"]: row for row in deepcopy(sentinels)}
        expected_after = {**deepcopy(expected_before), **deepcopy(expected_source)}

        # Step 2: Seed target BEFORE source deletes so a wrongly replayed delete is observable.
        assert self.insert(client, target, deepcopy(sentinels))[0]["insert_count"] == len(sentinels)
        self.flush(client, target)
        self.load_collection(client, target)
        self._assert_rows(client, target, expected_before, filter_expr="")
        assert self.insert(client, source, deepcopy(rows))[0]["insert_count"] == len(rows)
        self.flush(client, source)
        assert self.delete(client, source, ids=sorted(deleted_pks))[0]["delete_count"] == len(deleted_pks)
        assert self.insert(client, source, deepcopy(reinserted))[0]["insert_count"] == len(reinserted)
        self.flush(client, source)

        # Step 3: Require old + new source rows and real applicable L0, not just a live query result.
        raw_rows = len(rows) + len(reinserted)
        metadata_uri, _, source_objects = self._capture_snapshot_input(
            client,
            source,
            snapshot,
            store,
            bucket,
            db_name,
            raw_rows,
            deleted_pks,
            pk_type=pk_type,
            case_id="SI-FN-03",
        )
        self.load_collection(client, source)
        self._assert_rows(client, source, expected_source, filter_expr="")
        captured_objects = [source_objects]
        if layout == "self_contained":
            self._retain_import_resources = True
            export_id = export_snapshot(url, source, snapshot, export_prefix, db_name, api_key)
            exported = wait_for_snapshot_export(url, export_id, db_name, api_key)
            self._retain_import_resources = False
            assert exported["state"] == "ExportSnapshotCompleted", exported
            assert exported["collectionName"] == source and exported["snapshotName"] == snapshot
            metadata_uri = exported["snapshotMetadataURI"]
            _, segments, objects, deleted = read_snapshot_inventory(store, bucket, metadata_uri, layout, pk_type)
            assert sum(s["num_of_rows"] for s in segments if s["segment_level"] != 1) == raw_rows
            assert deleted == deleted_pks
            captured_objects.append(objects)

        # Step 4: Import without publishing; existing target rows must remain the only visible rows.
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[metadata_uri]],
            options={"backup": "true", "source_type": "snapshot", "auto_commit": "false"},
            api_key=api_key,
            timeout=30,
        ).json()
        assert response["code"] == 0, response
        job_id = response["data"]["jobId"]
        assert job_id
        log.info(f"SI-FN-03 layout={layout}, pk_type={pk_type.name}, target={target}, job_id={job_id}")
        progress = self._wait_for_import(url, job_id, db_name, expected_state="Uncommitted")
        self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
        assert progress["state"] == "Uncommitted", progress
        assert progress["importedRows"] == len(expected_source), progress
        self._assert_rows(client, target, expected_before, filter_expr="")

        # Step 5: Commit the same job and compare all three PK groups, then repeat after reload.
        response = commit_import(url=url, job_id=job_id, db_name=db_name, api_key=api_key, timeout=30).json()
        assert response["code"] == 0, response
        progress = self._wait_for_import(url, job_id, db_name)
        self._retain_import_resources = False
        assert progress["state"] == "Completed", progress
        assert progress["importedRows"] == len(expected_source), progress
        assert self.wait_for_index_ready(client, target, index_name="dst_vec_idx", timeout=120)
        self._assert_rows(client, target, expected_after, filter_expr="")
        self.release_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
        self.load_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
        self._assert_rows(client, target, expected_after, filter_expr="")
        for objects in captured_objects:
            assert_snapshot_objects_unchanged(store, bucket, objects)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    def test_snapshot_import_isolates_partition_deletes(self, snapshot_partition_resources, layout, db_name):
        """
        target: SI-FN-05, preserve source L0 partition scope when target routing changes
        method: delete A/10 only; require captured L0; import the same snapshot with split and merged mappings
        expected: A/10 absent, B/10 and all controls intact; 129 exact rows after Import and reload
        """
        client, source, targets, snapshot, store, bucket, export_prefix = snapshot_partition_resources
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        api_key = cf.param_info.param_token

        # Step 1: Two source shards, three target shards, and explicit named partitions.
        self._create_collection(client, source, "src_vec_idx", shards_num=2)
        for partition in ("A", "B"):
            self.create_partition(client, source, partition)
        source_info = self.describe_collection(client, source)[0]
        assert source_info["num_shards"] == 2
        for target in targets:
            self._create_collection(client, target, "dst_vec_idx", shards_num=3)
            info = self.describe_collection(client, target)[0]
            assert info["num_shards"] == 3 and info["fields"] == source_info["fields"]
            for partition in ("X", "Y"):
                self.create_partition(client, target, partition)

        # Step 2: A and B share PK=10, but every payload differs; controls do not overlap.
        rows = {}
        for partition, base in (("A", 100), ("B", 200)):
            rows[partition] = [
                {
                    "id": pk,
                    "source_uid": f"{partition}_{pk}",
                    "score": base * 1000 + pk,
                    "flag": partition == "A",
                    "tag": partition,
                    "vec": [(base + pk + j) / 4096 for j in range(DIM)],
                }
                for pk in [10, *range(base, base + 64)]
            ]
        expected_a = {r["id"]: deepcopy(r) for r in rows["A"] if r["id"] != 10}
        expected_b = {r["id"]: deepcopy(r) for r in rows["B"]}
        expected_all = {**expected_a, **expected_b}
        for partition in ("A", "B"):
            result = self.insert(client, source, deepcopy(rows[partition]), partition_name=partition)[0]
            assert result["insert_count"] == 65
        self.flush(client, source)
        self.load_collection(client, source)
        for partition in ("A", "B"):
            self._assert_rows(client, source, {r["id"]: r for r in rows[partition]}, partition_names=[partition])
        assert self.delete(client, source, ids=[10], partition_name="A")[0]["delete_count"] == 1
        self.flush(client, source)
        self._assert_rows(client, source, expected_a, partition_names=["A"])
        self._assert_rows(client, source, expected_b, partition_names=["B"])

        # Step 3: Require 65 raw rows in each partition and actual A-only L0 for PK=10.
        metadata_uri, metadata, data, deletes, objects = self._capture_partition_delete_snapshot(
            client, source, snapshot, store, bucket, db_name
        )
        captured_objects = [objects]
        if layout == "self_contained":
            self._retain_import_resources = True
            export_id = export_snapshot(url, source, snapshot, export_prefix, db_name, api_key)
            exported = wait_for_snapshot_export(url, export_id, db_name, api_key)
            self._retain_import_resources = False
            assert exported["state"] == "ExportSnapshotCompleted", exported
            assert exported["collectionName"] == source and exported["snapshotName"] == snapshot
            metadata_uri = exported["snapshotMetadataURI"]
            exported_meta, exported_data, exported_deletes, objects = read_snapshot_partition_inventory(
                store, bucket, metadata_uri, layout
            )
            assert exported_meta["collection"]["partitions"] == metadata["collection"]["partitions"]
            assert self._partition_delete_fixture_ready(exported_meta, exported_data, exported_deletes)
            assert exported_deletes == deletes
            source_scopes = {(s["segment_id"], s["partition_id"], s["channel_name"], s["num_of_rows"]) for s in data}
            assert source_scopes == {
                (s["segment_id"], s["partition_id"], s["channel_name"], s["num_of_rows"]) for s in exported_data
            }
            captured_objects.append(objects)

        # Step 4: Reuse this exact snapshot for separate and many-to-one destinations.
        for target, b_destination in zip(targets, ("Y", "X")):
            mapping = {name: "X" for name in metadata["collection"]["partitions"]}
            mapping["B"] = b_destination  # Includes the empty source _default partition.
            self._retain_import_resources = True
            response = bulk_import(
                url=url,
                collection_name=target,
                db_name=db_name,
                files=[[metadata_uri]],
                options={
                    "backup": "true",
                    "source_type": "snapshot",
                    "auto_commit": "true",
                    "partition_mapping": json.dumps(mapping),
                },
                api_key=api_key,
                timeout=30,
            ).json()
            assert response["code"] == 0, response
            job_id = response["data"]["jobId"]
            assert job_id
            log.info(f"SI-FN-05 layout={layout}, target={target}, mapping={mapping}, job_id={job_id}")
            progress = self._wait_for_import(url, job_id, db_name)
            self._retain_import_resources = False
            assert progress["state"] == "Completed" and progress["importedRows"] == 129, progress
            assert self.wait_for_index_ready(client, target, index_name="dst_vec_idx", timeout=120)
            expected_partitions = {
                "X": expected_a if b_destination == "Y" else expected_all,
                "Y": expected_b if b_destination == "Y" else {},
                "_default": {},
            }

            # Step 5: Full payload/count checks by partition, then repeat after release/load.
            self.load_collection(client, target)
            for reload in (False, True):
                if reload:
                    self.release_collection(client, target)
                    assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
                    self.load_collection(client, target)
                    assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
                for partition, expected in expected_partitions.items():
                    self._assert_rows(client, target, expected, partition_names=[partition])
                self._assert_rows(client, target, expected_all)
        for objects in captured_objects:
            assert_snapshot_objects_unchanged(store, bucket, objects)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    @pytest.mark.parametrize(
        "target_shards",
        [
            pytest.param(None, id="default_shards"),
            pytest.param(1, id="fewer_shards"),
            pytest.param(3, id="more_shards"),
        ],
    )
    def test_snapshot_import_routes_all_source_partitions(
        self, snapshot_routing_resources, layout, target_shards, db_name
    ):
        """
        target: SI-FN-06, import every source partition into the default or named destination without mapping
        method: snapshot A/B/C plus empty partitions; import twice under default/fewer/more target shards
        expected: exactly 96 unique rows in the selected partition; other partitions empty; no source partitions created
        """
        client, source, targets, snapshot, store, bucket, export_prefix = snapshot_routing_resources
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        api_key = cf.param_info.param_token

        # Step 1: Freeze 96 unique rows across A/B/C; leave _default and empty_source empty.
        self._create_collection(client, source, "src_vec_idx", shards_num=2)
        for partition in ("A", "B", "C", "empty_source"):
            self.create_partition(client, source, partition)
        source_info = self.describe_collection(client, source)[0]
        assert source_info["num_shards"] == 2
        expected = {}
        for partition, start in (("A", 0), ("B", 32), ("C", 64)):
            rows = [
                {
                    "id": pk,
                    "source_uid": f"{partition}_{pk}",
                    "score": pk * 10,
                    "flag": pk % 2 == 0,
                    "tag": partition,
                    "vec": [(pk * DIM + j) / 4096 for j in range(DIM)],
                }
                for pk in range(start, start + 32)
            ]
            expected.update({row["id"]: deepcopy(row) for row in rows})
            assert self.insert(client, source, deepcopy(rows), partition_name=partition)[0]["insert_count"] == 32
        self.flush(client, source)

        # Step 2: Prove the snapshot captures every data partition and both empty partitions.
        self.create_snapshot(client, snapshot, source, db_name=db_name, compaction_protection_seconds=3600)
        metadata_uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location
        metadata, data, deletes, objects = read_snapshot_partition_inventory(
            store, bucket, metadata_uri, case_id="SI-FN-06"
        )
        self._assert_routing_snapshot_input(metadata, data, deletes)
        captured_objects = [objects]
        if layout == "self_contained":
            self._retain_import_resources = True
            export_id = export_snapshot(url, source, snapshot, export_prefix, db_name, api_key)
            exported = wait_for_snapshot_export(url, export_id, db_name, api_key)
            self._retain_import_resources = False
            assert exported["state"] == "ExportSnapshotCompleted", exported
            assert exported["collectionName"] == source and exported["snapshotName"] == snapshot
            metadata_uri = exported["snapshotMetadataURI"]
            exported_meta, exported_data, exported_deletes, objects = read_snapshot_partition_inventory(
                store, bucket, metadata_uri, layout, case_id="SI-FN-06"
            )
            self._assert_routing_snapshot_input(exported_meta, exported_data, exported_deletes)
            assert exported_meta["collection"]["partitions"] == metadata["collection"]["partitions"]
            assert {(s["segment_id"], s["partition_id"], s["num_of_rows"]) for s in exported_data} == {
                (s["segment_id"], s["partition_id"], s["num_of_rows"]) for s in data
            }
            captured_objects.append(objects)

        # Step 3: Default-shard variant genuinely omits shards_num; record the actual value.
        shard_options = {} if target_shards is None else {"shards_num": target_shards}
        for target, partition_name in zip(targets, (None, "X")):
            self._create_collection(client, target, "dst_vec_idx", **shard_options)
            target_info = self.describe_collection(client, target)[0]
            assert target_info["fields"] == source_info["fields"]
            actual_shards = target_info["num_shards"]
            assert actual_shards > 0
            if target_shards is not None:
                assert actual_shards == target_shards
            for partition in ("X", "Y"):
                self.create_partition(client, target, partition)
            assert set(self.list_partitions(client, target)[0]) == {"_default", "X", "Y"}

            # Step 4: No partition_mapping. Omitted SDK partition_name serializes as an empty name.
            destination = "_default" if partition_name is None else partition_name
            partition_options = {} if partition_name is None else {"partition_name": partition_name}
            self._retain_import_resources = True
            response = bulk_import(
                url=url,
                collection_name=target,
                db_name=db_name,
                files=[[metadata_uri]],
                options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
                api_key=api_key,
                timeout=30,
                **partition_options,
            ).json()
            assert response["code"] == 0, response
            job_id = response["data"]["jobId"]
            assert job_id
            log.info(
                f"SI-FN-06 layout={layout}, requested_shards={target_shards}, actual_shards={actual_shards}, "
                f"destination={destination}, target={target}, job_id={job_id}"
            )
            progress = self._wait_for_import(url, job_id, db_name)
            self._retain_import_resources = False
            assert progress["state"] == "Completed" and progress["importedRows"] == len(expected), progress
            assert self.wait_for_index_ready(client, target, index_name="dst_vec_idx", timeout=120)

            # Step 5: All fields/count by partition and collection, repeated after release/load.
            self.load_collection(client, target)
            for reload in (False, True):
                if reload:
                    self.release_collection(client, target)
                    assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
                    self.load_collection(client, target)
                    assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
                assert set(self.list_partitions(client, target)[0]) == {"_default", "X", "Y"}
                for partition in ("_default", "X", "Y"):
                    partition_rows = expected if partition == destination else {}
                    self._assert_rows(client, target, partition_rows, partition_names=[partition])
                self._assert_rows(client, target, expected)
            assert self.describe_collection(client, target)[0]["num_shards"] == actual_shards
        for objects in captured_objects:
            assert_snapshot_objects_unchanged(store, bucket, objects)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    @pytest.mark.parametrize("source_kind", ["values", "missing", "null"])
    def test_snapshot_import_partition_key_defaults(
        self, snapshot_key_resources, source_kind, layout, db_name, tmp_path
    ):
        """
        target: SI-FN-07, hash stored/default-filled partition keys into the correct physical partitions
        method: snapshot real/missing/NULL key; Import; query/reload; inspect target snapshot physical rows
        expected: original 0..3 or default 7; all 128 PKs/payloads intact; each physical row in its expected hash slot
        """
        client, source, target, control, snapshot, output_snapshots, store, bucket, export_prefix = (
            snapshot_key_resources
        )
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        api_key = cf.param_info.param_token

        # Step 1: Key is last in the schema, so missing-source fields cannot shift common FieldIDs.
        self._create_key_fixture_collection(client, source, source_kind)
        self._create_key_fixture_collection(client, target, "partitioned_target")
        source_fields = {f["name"]: f for f in self.describe_collection(client, source)[0]["fields"]}
        target_fields = {f["name"]: f for f in self.describe_collection(client, target)[0]["fields"]}
        assert set(target_fields) == set(FIELDS) | {"route_key"}
        assert set(source_fields) == (set(FIELDS) if source_kind == "missing" else set(target_fields))
        for name in FIELDS:
            assert source_fields[name]["field_id"] == target_fields[name]["field_id"]
        if source_kind == "missing":
            assert target_fields["route_key"]["field_id"] not in {f["field_id"] for f in source_fields.values()}
        else:
            assert source_fields["route_key"]["field_id"] == target_fields["route_key"]["field_id"]
            assert not source_fields["route_key"].get("is_partition_key", False)
            assert source_fields["route_key"].get("nullable", False) == (source_kind == "null")
        assert target_fields["route_key"].get("is_partition_key")
        assert not target_fields["route_key"].get("nullable", False)
        assert target_fields["route_key"]["default_value"].long_data == 7
        assert len(self.list_partitions(client, target)[0]) == 8

        # Step 2: Freeze input truth; source NULL has no default and is not a partition key.
        rows = [
            {
                "id": i,
                "source_uid": f"src_{i}",
                "score": i * 10,
                "flag": i % 2 == 0,
                "tag": f"tag_{i}",
                "vec": [(i * DIM + j) / 4096 for j in range(DIM)],
            }
            for i in range(128)
        ]
        for row in rows:
            if source_kind != "missing":
                row["route_key"] = row["id"] % 4 if source_kind == "values" else None
        expected = {r["id"]: {**deepcopy(r), "route_key": r["id"] % 4 if source_kind == "values" else 7} for r in rows}
        assert self.insert(client, source, deepcopy(rows))[0]["insert_count"] == len(rows)
        self.flush(client, source)
        self.create_snapshot(client, snapshot, source, db_name=db_name, compaction_protection_seconds=3600)
        metadata_uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location
        source_ids = {"id": source_fields["id"]["field_id"], "vector": source_fields["vec"]["field_id"]}
        if source_kind != "missing":
            source_ids["route_key"] = source_fields["route_key"]["field_id"]
        _, captured, objects = read_snapshot_partition_rows(store, bucket, metadata_uri, source_ids, tmp_path)
        assert len(captured) == len(rows) and {r["id"] for r in captured} == expected.keys()
        for row in captured:
            if source_kind != "missing":
                assert row["route_key"] == (row["id"] % 4 if source_kind == "values" else None)
            np.testing.assert_array_equal(row["vector"], expected[row["id"]]["vec"])
        captured_objects = [objects]
        if layout == "self_contained":
            self._retain_import_resources = True
            export_id = export_snapshot(url, source, snapshot, export_prefix, db_name, api_key)
            exported = wait_for_snapshot_export(url, export_id, db_name, api_key)
            self._retain_import_resources = False
            assert exported["state"] == "ExportSnapshotCompleted", exported
            assert exported["collectionName"] == source and exported["snapshotName"] == snapshot
            metadata_uri = exported["snapshotMetadataURI"]
            _, copied, objects = read_snapshot_partition_rows(store, bucket, metadata_uri, source_ids, tmp_path, layout)
            assert sorted(copied, key=lambda row: row["id"]) == sorted(captured, key=lambda row: row["id"])
            captured_objects.append(objects)

        # Step 3: Missing-key case also imports into an ordinary single destination as a control.
        destinations = [target]
        if source_kind == "missing":
            self._create_key_fixture_collection(client, control, "plain_target")
            destinations.append(control)
        for destination in destinations:
            fields = {f["name"]: f for f in self.describe_collection(client, destination)[0]["fields"]}
            assert {n: f["field_id"] for n, f in fields.items()} == {n: f["field_id"] for n, f in target_fields.items()}
            assert fields["route_key"]["default_value"].long_data == 7
            self._retain_import_resources = True
            response = bulk_import(
                url=url,
                collection_name=destination,
                db_name=db_name,
                files=[[metadata_uri]],
                options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
                api_key=api_key,
                timeout=30,
            ).json()
            assert response["code"] == 0, response
            job_id = response["data"]["jobId"]
            assert job_id
            log.info(f"SI-FN-07 source_kind={source_kind}, layout={layout}, target={destination}, job_id={job_id}")
            progress = self._wait_for_import(url, job_id, db_name)
            self._retain_import_resources = False
            assert progress["state"] == "Completed" and progress["importedRows"] == len(expected), progress
            assert self.wait_for_index_ready(client, destination, index_name="vec_idx", timeout=120)

            # Step 4: Query values independently of the physical routing check; repeat after reload.
            self.load_collection(client, destination)
            for reload in (False, True):
                if reload:
                    self.release_collection(client, destination)
                    assert self.get_load_state(client, destination)[0]["state"].name == "NotLoad"
                    self.load_collection(client, destination)
                    assert self.get_load_state(client, destination)[0]["state"].name == "Loaded"
                self._assert_rows(client, destination, expected)
                self._assert_key_values(client, destination, expected)

            # Step 5: Snapshot the completed target, then inspect actual rows per segment/partition.
            output_snapshot = output_snapshots[destination]
            self.create_snapshot(
                client, output_snapshot, destination, db_name=db_name, compaction_protection_seconds=3600
            )
            output_uri = self.describe_snapshot(client, output_snapshot, destination, db_name=db_name)[0].s3_location
            physical_ids = {
                "id": fields["id"]["field_id"],
                "vector": fields["vec"]["field_id"],
                "route_key": fields["route_key"]["field_id"],
            }
            metadata, physical, objects = read_snapshot_partition_rows(
                store, bucket, output_uri, physical_ids, tmp_path
            )
            self._assert_key_placement(metadata, physical, expected, destination == target)
            captured_objects.append(objects)
        for objects in captured_objects:
            assert_snapshot_objects_unchanged(store, bucket, objects)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    @pytest.mark.parametrize("with_delete", [False, True], ids=["no_delete", "with_delete"])
    def test_snapshot_import_explicit_partition_mapping(self, snapshot_mapping_resources, layout, with_delete, db_name):
        """
        target: SI-FN-08, complete explicit partition permutation and many-to-one mapping
        method: snapshot A/B/C plus empty partitions; map into two targets; manually commit each job
        expected: exact partition PKs/payloads, no rows before commit; deleted A rows stay absent after both mappings
        """
        client, source, targets, snapshot, export_prefix = snapshot_mapping_resources
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        api_key = cf.param_info.param_token

        # Step 1: Ordinary collections with matching FieldIDs and precreated target partitions.
        self._create_collection(client, source, "src_vec_idx", shards_num=2)
        for partition in ("A", "B", "C", "Empty"):
            self.create_partition(client, source, partition)
        source_fields = self.describe_collection(client, source)[0]["fields"]
        for target in targets:
            self._create_collection(client, target, "dst_vec_idx", shards_num=3)
            assert self.describe_collection(client, target)[0]["fields"] == source_fields
            for partition in ("X", "Y", "Z"):
                self.create_partition(client, target, partition)
            self.load_collection(client, target)

        # Step 2: Freeze unique PK/payload truth; delete only A/0..7 in the deletion variant.
        rows = {
            partition: [
                {
                    "id": pk,
                    "source_uid": f"{partition}_{pk}",
                    "score": pk * 10,
                    "flag": pk % 2 == 0,
                    "tag": partition,
                    "vec": [(pk * DIM + j) / 4096 for j in range(DIM)],
                }
                for pk in range(start, start + 32)
            ]
            for partition, start in (("A", 0), ("B", 32), ("C", 64))
        }
        deleted = set(range(8)) if with_delete else set()
        expected_source = {
            name: {r["id"]: deepcopy(r) for r in values if r["id"] not in deleted} for name, values in rows.items()
        }
        expected_all = {pk: row for values in expected_source.values() for pk, row in values.items()}
        for partition, values in rows.items():
            assert self.insert(client, source, deepcopy(values), partition_name=partition)[0]["insert_count"] == 32
        self.flush(client, source)
        self.load_collection(client, source)
        for partition, values in rows.items():
            self._assert_rows(client, source, {r["id"]: r for r in values}, partition_names=[partition])
        if with_delete:
            assert self.delete(client, source, ids=sorted(deleted), partition_name="A")[0]["delete_count"] == 8
            self.flush(client, source)
        for partition, expected in expected_source.items():
            self._assert_rows(client, source, expected, partition_names=[partition])

        # Step 3: Verify source through public queries, then snapshot once; no physical-file inspection.
        source_partitions = set(self.list_partitions(client, source)[0])
        assert source_partitions == {"A", "B", "C", "Empty", "_default"}
        for partition in ("Empty", "_default"):
            self._assert_rows(client, source, {}, partition_names=[partition])
        self._assert_rows(client, source, expected_all)
        self.create_snapshot(client, snapshot, source, db_name=db_name)
        metadata_uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location
        assert metadata_uri
        if layout == "self_contained":
            self._retain_import_resources = True
            export_id = export_snapshot(url, source, snapshot, export_prefix, db_name, api_key)
            exported = wait_for_snapshot_export(url, export_id, db_name, api_key)
            self._retain_import_resources = False
            assert exported["state"] == "ExportSnapshotCompleted", exported
            assert exported["collectionName"] == source and exported["snapshotName"] == snapshot
            metadata_uri = exported["snapshotMetadataURI"]
            assert metadata_uri

        # Step 4: Two independent destinations; all source names, including empty ones, are mapped.
        permuted = {"A": "Y", "B": "Z", "C": "X", "Empty": "X", "_default": "X"}
        merged = {name: "X" for name in source_partitions}
        expected_permuted = {
            "X": expected_source["C"],
            "Y": expected_source["A"],
            "Z": expected_source["B"],
            "_default": {},
        }
        expected_merged = {"X": expected_all, "Y": {}, "Z": {}, "_default": {}}
        for target, mapping, expected_partitions in zip(
            targets, (permuted, merged), (expected_permuted, expected_merged)
        ):
            assert set(mapping) == set(source_partitions)
            assert set(self.list_partitions(client, target)[0]) == set(expected_partitions)
            self._retain_import_resources = True
            response = bulk_import(
                url=url,
                collection_name=target,
                db_name=db_name,
                files=[[metadata_uri]],
                options={
                    "backup": "true",
                    "source_type": "snapshot",
                    "auto_commit": "false",
                    "partition_mapping": json.dumps(mapping),
                },
                api_key=api_key,
                timeout=30,
            ).json()
            assert response["code"] == 0, response
            job_id = response["data"]["jobId"]
            assert job_id
            log.info(
                f"SI-FN-08 layout={layout}, with_delete={with_delete}, target={target}, mapping={mapping}, job_id={job_id}"
            )
            progress = self._wait_for_import(url, job_id, db_name, expected_state="Uncommitted")
            self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
            assert progress["state"] == "Uncommitted" and progress["importedRows"] == len(expected_all), progress
            for partition in expected_partitions:
                self._assert_rows(client, target, {}, partition_names=[partition])
            self._assert_rows(client, target, {})

            # Step 5: Commit this same job once, then check every destination before and after reload.
            response = commit_import(url=url, job_id=job_id, db_name=db_name, api_key=api_key, timeout=30).json()
            assert response["code"] == 0, response
            progress = self._wait_for_import(url, job_id, db_name)
            self._retain_import_resources = False
            assert progress["state"] == "Completed" and progress["importedRows"] == len(expected_all), progress
            assert self.wait_for_index_ready(client, target, index_name="dst_vec_idx", timeout=120)
            for reload in (False, True):
                if reload:
                    self.release_collection(client, target)
                    assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
                    self.load_collection(client, target)
                    assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
                assert set(self.list_partitions(client, target)[0]) == set(expected_partitions)
                for partition, expected in expected_partitions.items():
                    self._assert_rows(client, target, expected, partition_names=[partition])
                self._assert_rows(client, target, expected_all)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    @pytest.mark.parametrize("field_change", ["renamed", "swapped"])
    def test_snapshot_import_field_id_correspondence(self, snapshot_field_id_resources, layout, field_change, db_name):
        """
        target: SI-FN-09, source columns follow actual FieldIDs rather than field names
        method: rename age or swap age/score declarations; verify IDs via describe; Import and query/reload
        expected: renamed age_copy=i; swapped score=i and age=1000+i; all 128 PKs and vectors preserved
        """
        client, source, target, snapshot, export_prefix = snapshot_field_id_resources
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        api_key = cf.param_info.param_token
        target_scalars = ("age_copy", "score") if field_change == "renamed" else ("score", "age")
        target_to_source = (
            {"age_copy": "age", "score": "score"} if field_change == "renamed" else {"score": "age", "age": "score"}
        )

        # Step 1: Request the schema shapes, then verify real IDs instead of assuming allocation order.
        self._create_field_id_collection(client, source, ("age", "score"))
        self._create_field_id_collection(client, target, target_scalars)
        source_fields = {f["name"]: f for f in self.describe_collection(client, source)[0]["fields"]}
        target_fields = {f["name"]: f for f in self.describe_collection(client, target)[0]["fields"]}
        assert set(source_fields) == {"id", "age", "score", "vec"}
        assert set(target_fields) == {"id", *target_scalars, "vec"}
        source_ids = {name: f["field_id"] for name, f in source_fields.items()}
        target_ids = {name: f["field_id"] for name, f in target_fields.items()}
        assert len(set(source_ids.values())) == len(set(target_ids.values())) == 4
        assert source_fields["id"]["is_primary"] and target_fields["id"]["is_primary"]
        for name, dtype in (("id", DataType.INT64), ("vec", DataType.FLOAT_VECTOR)):
            assert source_ids[name] == target_ids[name], (source_ids, target_ids)
            assert source_fields[name]["type"] == target_fields[name]["type"] == dtype
        assert int(source_fields["vec"]["params"]["dim"]) == int(target_fields["vec"]["params"]["dim"]) == DIM
        for target_name, source_name in target_to_source.items():
            assert target_ids[target_name] == source_ids[source_name], (
                "FN-09 fixture FieldID correspondence not established",
                source_ids,
                target_ids,
            )
            assert target_fields[target_name]["type"] == source_fields[source_name]["type"] == DataType.INT64
        log.info(f"SI-FN-09 field_change={field_change}, source_ids={source_ids}, target_ids={target_ids}")

        # Step 2: Distinct scalar value ranges make a name-based remap visible in the query result.
        rows = [
            {
                "id": i,
                "age": i,
                "score": 1000 + i,
                "vec": [(i * DIM + j) / 4096 for j in range(DIM)],
            }
            for i in range(128)
        ]
        expected_source = {r["id"]: deepcopy(r) for r in rows}
        expected = {
            r["id"]: {
                "id": r["id"],
                "vec": deepcopy(r["vec"]),
                **{target_name: r[source_name] for target_name, source_name in target_to_source.items()},
            }
            for r in rows
        }
        assert self.insert(client, source, deepcopy(rows))[0]["insert_count"] == len(rows)
        self.flush(client, source)
        self.load_collection(client, source)
        self._assert_field_id_rows(client, source, expected_source, ("age", "score"))
        self.create_snapshot(client, snapshot, source, db_name=db_name)
        metadata_uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location
        assert metadata_uri
        if layout == "self_contained":
            self._retain_import_resources = True
            export_id = export_snapshot(url, source, snapshot, export_prefix, db_name, api_key)
            exported = wait_for_snapshot_export(url, export_id, db_name, api_key)
            self._retain_import_resources = False
            assert exported["state"] == "ExportSnapshotCompleted", exported
            assert exported["collectionName"] == source and exported["snapshotName"] == snapshot
            metadata_uri = exported["snapshotMetadataURI"]
            assert metadata_uri

        # Step 3: Import without a field-name mapping; the schema's actual IDs define correspondence.
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[metadata_uri]],
            options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
            api_key=api_key,
            timeout=30,
        ).json()
        assert response["code"] == 0, response
        job_id = response["data"]["jobId"]
        assert job_id
        log.info(f"SI-FN-09 layout={layout}, field_change={field_change}, target={target}, job_id={job_id}")
        progress = self._wait_for_import(url, job_id, db_name)
        self._retain_import_resources = False
        assert progress["state"] == "Completed" and progress["importedRows"] == len(expected), progress
        assert self.wait_for_index_ready(client, target, index_name="vec_idx", timeout=120)

        # Step 4: Verify every PK/scalar/vector and total count, then repeat after reload.
        self.load_collection(client, target)
        self._assert_field_id_rows(client, target, expected, target_scalars)
        self.release_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
        self.load_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
        self._assert_field_id_rows(client, target, expected, target_scalars)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    def test_snapshot_import_combined_defaults(self, snapshot_default_resources, layout, db_name):
        """
        target: SI-FN-10, combine the four valid missing/NULL rules in different fields
        method: import mixed NULL/0/5 and false/empty controls into default/nullable target fields
        expected: missing and stored-NULL values follow target rules; all non-NULL values and vectors survive reload
        """
        client, source, target, snapshot, export_prefix = snapshot_default_resources
        source_specs = [
            ("null_to_default", DataType.INT64, {"nullable": True}),
            ("keep_null", DataType.INT64, {"nullable": True}),
            ("bool_value", DataType.BOOL, {}),
            ("text_value", DataType.VARCHAR, {"max_length": 64}),
            ("array_value", DataType.ARRAY, {"element_type": DataType.INT64, "max_capacity": 8}),
        ]
        target_specs = [
            ("null_to_default", DataType.INT64, {"default_value": 42}),
            ("keep_null", DataType.INT64, {"nullable": True}),
            ("bool_value", DataType.BOOL, {"default_value": True}),
            ("text_value", DataType.VARCHAR, {"max_length": 64, "default_value": "fallback"}),
            ("array_value", DataType.ARRAY, {"element_type": DataType.INT64, "max_capacity": 8}),
            ("missing_default", DataType.INT64, {"default_value": 42}),
            ("missing_nullable", DataType.INT64, {"nullable": True}),
        ]

        # Step 1: Missing fields are appended after all common fields; verify actual IDs.
        source_fields = self._create_default_fields_collection(client, source, source_specs)
        target_fields = self._create_default_fields_collection(client, target, target_specs)
        source_ids = {f["field_id"] for f in source_fields.values()}
        assert len(source_ids) == len(source_fields)
        assert len({f["field_id"] for f in target_fields.values()}) == len(target_fields)
        for name, field in source_fields.items():
            assert target_fields[name]["field_id"] == field["field_id"]
            assert target_fields[name]["type"] == field["type"]
        for name in ("missing_default", "missing_nullable"):
            assert name not in source_fields and target_fields[name]["field_id"] not in source_ids
        for name in ("null_to_default", "keep_null"):
            assert source_fields[name].get("nullable") and "default_value" not in source_fields[name]
        for name in ("null_to_default", "missing_default"):
            assert target_fields[name]["default_value"].long_data == 42
            assert not target_fields[name].get("nullable", False)
        for name in ("keep_null", "missing_nullable"):
            assert target_fields[name].get("nullable") and "default_value" not in target_fields[name]
        assert target_fields["bool_value"]["default_value"].bool_data is True
        assert target_fields["text_value"]["default_value"].string_data == "fallback"

        # Step 2: NULL is explicit; zero/false/empty values must not be replaced by defaults.
        rows = [
            {
                "id": i,
                "vec": [(i * DIM + j) / 4096 for j in range(DIM)],
                "null_to_default": (None, 0, 5)[i % 3],
                "keep_null": (None, 0, 5)[i % 3],
                "bool_value": i % 2 != 0,
                "text_value": "" if i % 2 == 0 else f"text_{i}",
                "array_value": [] if i % 2 == 0 else [i, i + 1],
            }
            for i in range(128)
        ]
        expected = {
            row["id"]: {
                **deepcopy(row),
                "null_to_default": 42 if row["null_to_default"] is None else row["null_to_default"],
                "missing_default": 42,
                "missing_nullable": None,
            }
            for row in rows
        }
        source_scalars = tuple(spec[0] for spec in source_specs)
        target_scalars = tuple(spec[0] for spec in target_specs)
        assert self.insert(client, source, deepcopy(rows))[0]["insert_count"] == len(rows)
        self.flush(client, source)
        self.load_collection(client, source)
        self._assert_field_id_rows(client, source, {r["id"]: r for r in rows}, source_scalars, case_id="SI-FN-10")
        uri = self._default_snapshot_uri(client, source, snapshot, export_prefix, layout, db_name)

        # Step 3: One Import checks all valid rules in the same target collection.
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[uri]],
            options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
            api_key=cf.param_info.param_token,
            timeout=30,
        ).json()
        assert response["code"] == 0, response
        job_id = response["data"]["jobId"]
        assert job_id
        log.info(f"SI-FN-10 scenario=combined_success, layout={layout}, target={target}, job_id={job_id}")
        progress = self._wait_for_import(url, job_id, db_name)
        self._retain_import_resources = False
        assert progress["state"] == "Completed" and progress["importedRows"] == len(expected), progress
        assert self.wait_for_index_ready(client, target, index_name="vec_idx", timeout=120)
        self.load_collection(client, target)
        self._assert_field_id_rows(client, target, expected, target_scalars, case_id="SI-FN-10")
        self.release_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
        self.load_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
        self._assert_field_id_rows(client, target, expected, target_scalars, case_id="SI-FN-10")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    @pytest.mark.parametrize("error_kind", ["missing_required", "null_required"])
    def test_snapshot_import_required_field_rejection(self, snapshot_default_resources, layout, error_kind, db_name):
        """
        target: SI-FN-10, isolate missing-required and stored-NULL/non-nullable rejection
        method: import one invalid source into a required-field target containing a sentinel
        expected: matching schema error, no partial publication; sentinel and target schema unchanged after reload
        """
        client, source, target, snapshot, export_prefix = snapshot_default_resources
        source_specs = (
            [] if error_kind == "missing_required" else [("required_value", DataType.INT64, {"nullable": True})]
        )
        source_fields = self._create_default_fields_collection(client, source, source_specs)
        target_fields = self._create_default_fields_collection(client, target, [("required_value", DataType.INT64, {})])
        for name, field in source_fields.items():
            assert target_fields[name]["field_id"] == field["field_id"]
            assert target_fields[name]["type"] == field["type"]
        assert not target_fields["required_value"].get("nullable", False)
        assert "default_value" not in target_fields["required_value"]
        if error_kind == "missing_required":
            assert "required_value" not in source_fields
            assert target_fields["required_value"]["field_id"] not in {
                field["field_id"] for field in source_fields.values()
            }
        else:
            assert source_fields["required_value"].get("nullable")
            assert "default_value" not in source_fields["required_value"]

        # Step 1: Snapshot input is independently queryable in its legal source schema.
        rows = [{"id": i, "vec": [(i * DIM + j) / 4096 for j in range(DIM)]} for i in range(128)]
        if error_kind == "null_required":
            for row in rows:
                row["required_value"] = None if row["id"] == 127 else (0, 5)[row["id"] % 2]
        assert self.insert(client, source, deepcopy(rows))[0]["insert_count"] == len(rows)
        self.flush(client, source)
        self.load_collection(client, source)
        self._assert_field_id_rows(
            client, source, {r["id"]: r for r in rows}, tuple(s[0] for s in source_specs), case_id="SI-FN-10"
        )
        uri = self._default_snapshot_uri(client, source, snapshot, export_prefix, layout, db_name)

        # Step 2: A disjoint existing row must survive rejection; none of the new PKs may become visible.
        sentinel = {"id": 10000, "vec": [0.5] * DIM, "required_value": 777}
        assert self.insert(client, target, [deepcopy(sentinel)])[0]["insert_count"] == 1
        self.flush(client, target)
        self.load_collection(client, target)
        expected = {sentinel["id"]: sentinel}
        self._assert_field_id_rows(client, target, expected, ("required_value",), case_id="SI-FN-10")
        schema_before = self.describe_collection(client, target)[0]["fields"]
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"

        # Step 3: SDK raises MilvusException on admission errors; admitted jobs must reach Failed.
        self._retain_import_resources = True
        try:
            response = bulk_import(
                url=url,
                collection_name=target,
                db_name=db_name,
                files=[[uri]],
                options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
                api_key=cf.param_info.param_token,
                timeout=30,
            ).json()
        except MilvusException as exc:
            # Match only the intended schema error; network/auth/timeout failures cannot pass.
            self._assert_default_import_error(str(exc), error_kind)
            self._retain_import_resources = False
            log.info(f"SI-FN-10 scenario={error_kind}, layout={layout}, target={target}, admission_rejected=True")
        else:
            assert response["code"] == 0, response
            job_id = response["data"]["jobId"]
            assert job_id
            log.info(f"SI-FN-10 scenario={error_kind}, layout={layout}, target={target}, job_id={job_id}")
            progress = self._wait_for_import(url, job_id, db_name, expected_state="Failed")
            self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
            assert progress["state"] == "Failed", progress
            self._assert_default_import_error(progress.get("reason", ""), error_kind)

        # Step 4: Check the whole target, not just the sentinel PK; repeat after reload.
        self._assert_field_id_rows(client, target, expected, ("required_value",), case_id="SI-FN-10")
        self.release_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
        self.load_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
        self._assert_field_id_rows(client, target, expected, ("required_value",), case_id="SI-FN-10")
        assert self.describe_collection(client, target)[0]["fields"] == schema_before

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    @pytest.mark.parametrize("scenario", ["source_extra", "missing_dynamic", "stored_dynamic"])
    def test_snapshot_import_dynamic_fields(self, snapshot_dynamic_resources, layout, scenario, db_name):
        """
        target: SI-FN-11, ignore source-only columns and fill/preserve dynamic JSON
        method: snapshot three source schemas; import into dynamic targets; query full JSON and selected keys; reload
        expected: extra ordinary column is not promoted; missing dynamic data is empty; stored values/types survive
        """
        client, source, target, snapshot, export_prefix = snapshot_dynamic_resources

        # Step 1: Matching creation order preserves $meta correspondence; add source_extra afterwards.
        # Public DescribeCollection hides $meta, so only inspect ordinary fields and the dynamic flag here.
        self._create_dynamic_import_collection(client, source, dynamic=scenario != "missing_dynamic")
        self._create_dynamic_import_collection(client, target, dynamic=True)
        if scenario == "source_extra":
            self.add_collection_field(client, source, "source_extra", DataType.INT64, nullable=True)
        source_schema = self.describe_collection(client, source)[0]
        target_schema = self.describe_collection(client, target)[0]
        source_fields = {f["name"]: f for f in source_schema["fields"]}
        target_fields = {f["name"]: f for f in target_schema["fields"]}
        assert target_schema["enable_dynamic_field"] is True
        assert source_schema["enable_dynamic_field"] == (scenario != "missing_dynamic")
        assert set(target_fields) == {"id", "vec"}
        expected_source_fields = {"id", "vec", "source_extra"} if scenario == "source_extra" else {"id", "vec"}
        assert set(source_fields) == expected_source_fields
        for fields in (source_fields, target_fields):
            assert len({f["field_id"] for f in fields.values()}) == len(fields)
        for name in ("id", "vec"):
            assert source_fields[name]["field_id"] == target_fields[name]["field_id"]
            assert source_fields[name]["type"] == target_fields[name]["type"]
        if scenario == "source_extra":
            extra = source_fields["source_extra"]
            assert extra["type"] == DataType.INT64 and extra.get("nullable") is True
            assert not extra.get("is_dynamic", False)
            assert extra["field_id"] not in {f["field_id"] for f in target_fields.values()}

        # Step 2: Freeze row-by-row truth; zero/false/empty arrays are intentional controls.
        rows = [{"id": i, "vec": [(i * DIM + j) / 4096 for j in range(DIM)]} for i in range(128)]
        expected_meta = {row["id"]: {} for row in rows}
        if scenario == "source_extra":
            for row in rows:
                row["source_extra"] = 1000 + row["id"]
        elif scenario == "stored_dynamic":
            expected_meta = {
                i: {
                    "color": "red" if i % 2 == 0 else "blue",
                    "score": i,
                    "enabled": i % 2 != 0,
                    "tags": [] if i % 2 == 0 else ["keep", f"tag_{i}"],
                    "nested": {"ok": i % 3 == 0, "seq": i},
                }
                for i in range(len(rows))
            }
            for row in rows:
                row.update(deepcopy(expected_meta[row["id"]]))
        source_truth = {row["id"]: deepcopy(row) for row in rows}
        target_truth = {pk: {k: v for k, v in row.items() if k != "source_extra"} for pk, row in source_truth.items()}
        assert self.insert(client, source, deepcopy(rows))[0]["insert_count"] == len(rows)
        self.flush(client, source)
        self.load_collection(client, source)
        source_scalars = ("source_extra",) if scenario == "source_extra" else ()
        self._assert_field_id_rows(client, source, source_truth, source_scalars, case_id="SI-FN-11")
        if scenario != "missing_dynamic":
            self._assert_snapshot_dynamic_data(client, source, expected_meta)
        uri = self._default_snapshot_uri(client, source, snapshot, export_prefix, layout, db_name)

        # Step 3: Submit one automatically committed Import for this source/layout.
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[uri]],
            options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
            api_key=cf.param_info.param_token,
            timeout=30,
        ).json()
        assert response["code"] == 0, response
        job_id = response["data"]["jobId"]
        assert job_id
        log.info(f"SI-FN-11 scenario={scenario}, layout={layout}, target={target}, job_id={job_id}")
        progress = self._wait_for_import(url, job_id, db_name)
        self._retain_import_resources = False
        assert progress["state"] == "Completed" and progress["importedRows"] == len(rows), progress
        assert self.wait_for_index_ready(client, target, index_name="vec_idx", timeout=120)

        # Step 4: Query all PKs/vectors/count, full dynamic JSON, selected keys and filters; repeat after reload.
        self.load_collection(client, target)
        self._assert_field_id_rows(client, target, target_truth, (), case_id="SI-FN-11")
        self._assert_snapshot_dynamic_data(client, target, expected_meta)
        self.release_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
        self.load_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
        self._assert_field_id_rows(client, target, target_truth, (), case_id="SI-FN-11")
        self._assert_snapshot_dynamic_data(client, target, expected_meta)
        assert self.describe_collection(client, target)[0]["fields"] == target_schema["fields"]

    def _create_dynamic_import_collection(self, client, name, dynamic):
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=dynamic)[0]
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=DIM)
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index(field_name="vec", index_name="vec_idx", index_type="FLAT", metric_type="L2")
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=1,
            consistency_level="Strong",
            force_teardown=False,
        )

    def _assert_snapshot_dynamic_data(self, client, name, expected_meta):
        # PyMilvus flattens $meta into top-level keys. Request the entire column, not a whitelist.
        projections = [["id", "$meta"]]
        if any(expected_meta.values()):
            projections.append(["id", "color", "score", "enabled", "tags", "nested"])
        for output_fields in projections:
            rows = self.query(
                client,
                name,
                filter="id >= 0",
                output_fields=output_fields,
                limit=len(expected_meta) + 1,
                consistency_level="Strong",
                timeout=30,
            )[0]
            actual = {row["id"]: {k: v for k, v in row.items() if k != "id"} for row in rows}
            assert len(rows) == len(actual) == len(expected_meta)
            assert actual.keys() == expected_meta.keys()
            for pk, truth in expected_meta.items():
                # Python equality alone would accept True == 1; JSON serialization preserves these types.
                assert json.dumps(actual[pk], sort_keys=True, allow_nan=False) == json.dumps(
                    truth, sort_keys=True, allow_nan=False
                ), (pk, output_fields, actual[pk], truth)

        if any(expected_meta.values()):
            filters = [
                (
                    'color == "red" and score >= 64',
                    {pk for pk, v in expected_meta.items() if v["color"] == "red" and v["score"] >= 64},
                ),
                ("enabled == false", {pk for pk, v in expected_meta.items() if v["enabled"] is False}),
                ('nested["ok"] == true', {pk for pk, v in expected_meta.items() if v["nested"]["ok"] is True}),
                ('json_contains(tags, "keep")', {pk for pk, v in expected_meta.items() if "keep" in v["tags"]}),
            ]
        else:
            filters = [('exists $meta["source_extra"]', set())]
        for expression, expected_ids in filters:
            rows = self.query(
                client,
                name,
                filter=expression,
                output_fields=["id"],
                limit=len(expected_meta) + 1,
                consistency_level="Strong",
                timeout=30,
            )[0]
            assert len(rows) == len(expected_ids), (expression, rows, expected_ids)
            assert {row["id"] for row in rows} == expected_ids, (expression, rows, expected_ids)

    @pytest.fixture
    def snapshot_dynamic_resources(self, snapshot_resources, minio_host, minio_bucket):
        client, source, target, snapshot = snapshot_resources
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn11_{uuid4().hex}/export_a/"
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, target, snapshot, prefix
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {prefix}")
            else:
                remove_snapshot_export(store, bucket, prefix)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    def test_snapshot_import_schema_rejections(self, snapshot_schema_resources, layout, db_name):
        """
        target: SI-FN-12, reject missing target PK, vector dimension and incompatible scalar decoding
        method: use one legal snapshot for three isolated invalid targets and one compatible control per layout
        expected: specific rejection leaves only the sentinel; the identical snapshot imports 128 rows into the control
        """
        client, source, targets, snapshot, export_prefix = snapshot_schema_resources

        # Step 1: The source itself is legal and independently queryable.
        source_fields = self._create_schema_import_collection(client, source, "compatible")
        rows = [
            {"id": i, "vec": [(i * DIM + j) / 4096 for j in range(DIM)], "value": f"not_a_number_{i}"}
            for i in range(128)
        ]
        expected = {r["id"]: deepcopy(r) for r in rows}
        assert self.insert(client, source, deepcopy(rows))[0]["insert_count"] == len(rows)
        self.flush(client, source)
        self.load_collection(client, source)
        self._assert_field_id_rows(client, source, expected, ("value",), case_id="SI-FN-12")
        uri = self._default_snapshot_uri(client, source, snapshot, export_prefix, layout, db_name)
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"

        for error_kind in ("missing_pk", "vector_dim", "scalar_type"):
            target = targets[error_kind]
            target_fields = self._create_schema_import_collection(client, target, error_kind)
            self._assert_schema_import_fields(source_fields, target_fields, error_kind)

            # Step 2: A valid existing row, with the target's own type/dimension, must survive rejection.
            sentinel = {
                "id": 10000,
                "vec": [0.5] * (16 if error_kind == "vector_dim" else DIM),
                "value": 777 if error_kind == "scalar_type" else "target_sentinel",
            }
            scalars = ("value",)
            if error_kind == "missing_pk":
                sentinel["source_pk"] = 9000
                scalars = ("source_pk", "value")
            assert self.insert(client, target, [deepcopy(sentinel)])[0]["insert_count"] == 1
            self.flush(client, target)
            self.load_collection(client, target)
            sentinel_truth = {sentinel["id"]: sentinel}
            self._assert_field_id_rows(client, target, sentinel_truth, scalars, case_id="SI-FN-12")
            definitions_before = self._definitions(client, target)

            # Step 3: Only the matching schema failure is accepted, at admission or in an admitted job.
            self._retain_import_resources = True
            try:
                response = bulk_import(
                    url=url,
                    collection_name=target,
                    db_name=db_name,
                    files=[[uri]],
                    options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
                    api_key=cf.param_info.param_token,
                    timeout=30,
                ).json()
            except MilvusException as exc:
                self._assert_schema_import_error(str(exc), error_kind)
                self._retain_import_resources = False
                log.info(f"SI-FN-12 scenario={error_kind}, layout={layout}, target={target}, admission_rejected=True")
            else:
                assert response["code"] == 0, response
                job_id = response["data"]["jobId"]
                assert job_id
                log.info(f"SI-FN-12 scenario={error_kind}, layout={layout}, target={target}, job_id={job_id}")
                progress = self._wait_for_import(url, job_id, db_name, expected_state="Failed")
                self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
                assert progress["state"] == "Failed", progress
                self._assert_schema_import_error(progress.get("reason", ""), error_kind)

            # Step 4: Scan the whole target, not just the sentinel PK; repeat after reload.
            self._assert_field_id_rows(client, target, sentinel_truth, scalars, case_id="SI-FN-12")
            self.release_collection(client, target)
            assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
            self.load_collection(client, target)
            assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
            self._assert_field_id_rows(client, target, sentinel_truth, scalars, case_id="SI-FN-12")
            assert self._definitions(client, target) == definitions_before

        # Step 5: Import the exact same URI into an empty compatible target.
        target = targets["compatible"]
        target_fields = self._create_schema_import_collection(client, target, "compatible")
        self._assert_schema_import_fields(source_fields, target_fields, "compatible")
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[uri]],
            options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
            api_key=cf.param_info.param_token,
            timeout=30,
        ).json()
        assert response["code"] == 0, response
        job_id = response["data"]["jobId"]
        assert job_id
        log.info(f"SI-FN-12 scenario=compatible, layout={layout}, target={target}, job_id={job_id}")
        progress = self._wait_for_import(url, job_id, db_name)
        self._retain_import_resources = False
        assert progress["state"] == "Completed" and progress["importedRows"] == len(expected), progress
        assert self.wait_for_index_ready(client, target, index_name="vec_idx", timeout=120)
        self.load_collection(client, target)
        self._assert_field_id_rows(client, target, expected, ("value",), case_id="SI-FN-12")
        self.release_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
        self.load_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
        self._assert_field_id_rows(client, target, expected, ("value",), case_id="SI-FN-12")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    @pytest.mark.parametrize("overflow", [False, True], ids=["within_limit", "over_limit"])
    def test_snapshot_import_varchar_max_length(self, snapshot_varchar_resources, layout, overflow, db_name):
        """
        target: SI-FN-12, enforce the target VarChar byte limit without requiring equal source/target limits
        method: import the same max64 snapshot into max64 control and max6 target; compare all rows and reload
        expected: six-byte values survive unchanged; a seven-byte value is rejected without changing the sentinel
        """
        client, source, target, control, snapshot, export_prefix = snapshot_varchar_resources
        source_fields = self._create_schema_import_collection(client, source, "compatible")
        values = ["a", "abcdef", "中文", "中文a" if overflow else "abc"]
        assert [len(v.encode("utf-8")) for v in values] == [1, 6, 6, 7 if overflow else 3]
        rows = [
            {"id": i, "value": value, "vec": [(i * DIM + j) / 4096 for j in range(DIM)]}
            for i, value in enumerate(values)
        ]
        truth = {row["id"]: deepcopy(row) for row in rows}
        assert self.insert(client, source, deepcopy(rows))[0]["insert_count"] == len(rows)
        self.flush(client, source)
        self.load_collection(client, source)
        self._assert_field_id_rows(client, source, truth, ("value",), case_id="SI-FN-12")
        uri = self._default_snapshot_uri(client, source, snapshot, export_prefix, layout, db_name)
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"

        # Run the same-URI positive control first, so a rejection cannot be blamed on a broken snapshot.
        for name, max_length in ((control, 64), (target, 6)):
            fields = self._create_schema_import_collection(client, name, "compatible", max_length=max_length)
            self._assert_schema_import_fields(source_fields, fields, "compatible")
            assert int(source_fields["value"]["params"]["max_length"]) == 64
            assert int(fields["value"]["params"]["max_length"]) == max_length
            sentinel = {"id": 10000, "value": "keep", "vec": [0.5] * DIM}
            assert self.insert(client, name, [deepcopy(sentinel)])[0]["insert_count"] == 1
            self.flush(client, name)
            self.load_collection(client, name)
            self._assert_field_id_rows(client, name, {10000: sentinel}, ("value",), case_id="SI-FN-12")
            definitions_before = self._definitions(client, name)
            rejected = overflow and max_length == 6
            expected_state = "Failed" if rejected else "Completed"

            self._retain_import_resources = True
            response = bulk_import(
                url=url,
                collection_name=name,
                db_name=db_name,
                files=[[uri]],
                options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
                api_key=cf.param_info.param_token,
                timeout=30,
            ).json()
            assert response["code"] == 0, response
            job_id = response["data"]["jobId"]
            assert job_id
            log.info(
                f"SI-FN-12 varchar layout={layout}, overflow={overflow}, max_length={max_length}, "
                f"target={name}, job_id={job_id}"
            )
            progress = self._wait_for_import(url, job_id, db_name, expected_state=expected_state)
            self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
            assert progress["state"] == expected_state, progress
            if not rejected:
                assert progress["importedRows"] == len(rows), progress
            reason = progress.get("reason", "")

            if rejected:
                text = reason.lower()
                assert "value" in text and "max_length" in text, reason
                assert "exceed" in text or "too long" in text, reason
                expected = {10000: sentinel}
            else:
                expected = {**truth, 10000: sentinel}
                assert self.wait_for_index_ready(client, name, index_name="vec_idx", timeout=120)
            self._assert_field_id_rows(client, name, expected, ("value",), case_id="SI-FN-12")
            self.release_collection(client, name)
            assert self.get_load_state(client, name)[0]["state"].name == "NotLoad"
            self.load_collection(client, name)
            assert self.get_load_state(client, name)[0]["state"].name == "Loaded"
            self._assert_field_id_rows(client, name, expected, ("value",), case_id="SI-FN-12")
            assert self._definitions(client, name) == definitions_before

    @pytest.fixture
    def snapshot_varchar_resources(self, snapshot_resources, minio_host, minio_bucket):
        client, source, target, snapshot = snapshot_resources
        control = f"{target}_length_control"
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn12_{uuid4().hex}/export_a/"
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, target, control, snapshot, prefix
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {control}, {prefix}")
            else:
                remove_snapshot_export(store, bucket, prefix)
                if self.has_collection(client, control)[0]:
                    self.drop_collection(client, control)
                    assert not self.has_collection(client, control)[0]

    def _create_schema_import_collection(self, client, name, role, max_length=64):
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        if role == "missing_pk":
            self.add_field(schema, "source_pk", DataType.INT64)
        else:
            self.add_field(schema, "id", DataType.INT64, is_primary=True)
        self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=16 if role == "vector_dim" else DIM)
        if role == "scalar_type":
            self.add_field(schema, "value", DataType.INT64)
        else:
            self.add_field(schema, "value", DataType.VARCHAR, max_length=max_length)
        if role == "missing_pk":
            self.add_field(schema, "id", DataType.INT64, is_primary=True)
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index(field_name="vec", index_name="vec_idx", index_type="FLAT", metric_type="L2")
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=1,
            consistency_level="Strong",
            force_teardown=False,
        )
        described = self.describe_collection(client, name)[0]
        assert not described["enable_dynamic_field"] and not described["auto_id"]
        return {field["name"]: field for field in described["fields"]}

    @staticmethod
    def _assert_schema_import_fields(source, target, role):
        assert set(source) == {"id", "vec", "value"}
        assert source["id"]["type"] == DataType.INT64 and source["id"]["is_primary"]
        assert source["vec"]["type"] == DataType.FLOAT_VECTOR and int(source["vec"]["params"]["dim"]) == DIM
        assert source["value"]["type"] == DataType.VARCHAR
        assert target["id"]["type"] == DataType.INT64 and target["id"]["is_primary"]
        assert not target["id"].get("nullable", False) and "default_value" not in target["id"]
        for fields in (source, target):
            assert len({f["field_id"] for f in fields.values()}) == len(fields)
        if role == "missing_pk":
            assert set(target) == {"source_pk", "vec", "value", "id"}
            assert target["id"]["field_id"] not in {f["field_id"] for f in source.values()}
            assert target["source_pk"]["field_id"] == source["id"]["field_id"]
            assert target["source_pk"]["type"] == source["id"]["type"]
        else:
            assert set(target) == set(source)
            assert target["id"]["field_id"] == source["id"]["field_id"]
        for name in ("vec", "value"):
            assert target[name]["field_id"] == source[name]["field_id"]
            if name != "value" or role != "scalar_type":
                assert target[name]["type"] == source[name]["type"]
        assert target["vec"]["type"] == DataType.FLOAT_VECTOR
        assert int(target["vec"]["params"]["dim"]) == (16 if role == "vector_dim" else DIM)
        assert target["value"]["type"] == (DataType.INT64 if role == "scalar_type" else DataType.VARCHAR)
        assert not target["value"].get("nullable", False) and "default_value" not in target["value"]

    @staticmethod
    def _assert_schema_import_error(reason, error_kind):
        assert isinstance(reason, str) and reason, "Missing schema rejection reason"
        text = reason.lower()
        if error_kind == "missing_pk":
            assert "no binlog for field:id" in text, reason
        elif error_kind == "vector_dim":
            row_error = "wrong row type" in text and "[]float32" in text
            arrow_error = (
                "fixed_size_binary" in text
                and "128" in text
                and "64" in text
                and any(token in text for token in ("mismatch", "expected", "incompatible"))
            )
            assert row_error or arrow_error, reason
        else:
            assert error_kind == "scalar_type"
            reader_error = "expected *array.int64" in text and any(
                token in text for token in ("got *array.string", "got *array.largestring")
            )
            schema_error = (
                "int64" in text
                and any(token in text for token in ("string", "utf8", "varchar", "not_a_number_"))
                and any(token in text for token in ("mismatch", "expected", "invalid", "parse"))
            )
            assert reader_error or schema_error, reason
        log.info(f"SI-FN-12 expected schema rejection scenario={error_kind}, reason={reason}")

    @pytest.fixture
    def snapshot_schema_resources(self, snapshot_resources, minio_host, minio_bucket):
        client, source, target, snapshot = snapshot_resources
        targets = {kind: f"{target}_{kind}" for kind in ("missing_pk", "vector_dim", "scalar_type")}
        targets["compatible"] = target
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn12_{uuid4().hex}/export_a/"
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, targets, snapshot, prefix
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {list(targets.values())}, {prefix}")
            else:
                remove_snapshot_export(store, bucket, prefix)
                for kind in ("missing_pk", "vector_dim", "scalar_type"):
                    name = targets[kind]
                    if self.has_collection(client, name)[0]:
                        self.drop_collection(client, name)
                        assert not self.has_collection(client, name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("pk_type", [DataType.INT64, DataType.VARCHAR], ids=["int64", "varchar"])
    @pytest.mark.parametrize(
        "source_auto_id,target_auto_id",
        [
            pytest.param(False, False, id="manual_to_manual"),
            pytest.param(False, True, id="manual_to_auto"),
            pytest.param(True, False, id="auto_to_manual"),
            pytest.param(True, True, id="auto_to_auto"),
        ],
    )
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    def test_snapshot_import_preserves_pks_with_auto_id(
        self, snapshot_auto_id_resources, pk_type, source_auto_id, target_auto_id, layout, db_name
    ):
        """
        target: SI-FN-13, backup Import preserves source PKs across source/target AutoID settings
        method: import 128 rows; compare all PK/payload/vector values; insert one row normally and reload
        expected: original PKs stay unchanged; the new row follows target AutoID and never overwrites them
        """
        client, source, target, snapshot, export_prefix = snapshot_auto_id_resources

        # Step 1: AutoID is the only schema difference; verify actual flags and physical FieldIDs.
        for name, auto_id in ((source, source_auto_id), (target, target_auto_id)):
            self._create_collection(client, name, "vec_idx", pk_type=pk_type, auto_id=auto_id, shards_num=1)
            described = self.describe_collection(client, name)[0]
            assert described["auto_id"] == auto_id and not described["enable_dynamic_field"]
        source_fields = {f["name"]: f for f in self.describe_collection(client, source)[0]["fields"]}
        target_fields = {f["name"]: f for f in self.describe_collection(client, target)[0]["fields"]}
        assert set(source_fields) == set(target_fields) == set(FIELDS)
        assert source_fields["id"]["type"] == target_fields["id"]["type"] == pk_type
        assert source_fields["id"]["is_primary"] and target_fields["id"]["is_primary"]
        for field in FIELDS:
            for key in ("field_id", "type", "params"):
                assert source_fields[field][key] == target_fields[field][key], (field, key)
        definitions_before = self._definitions(client, target)

        # Step 2: Bind each returned PK to its original row; do not assume generated IDs are consecutive.
        if pk_type == DataType.INT64:
            manual_ids = [-(2**63), -1, 0, *range(1, 125), 2**63 - 1]
        else:
            manual_ids = ["0007", "7", "主键_中文", "x" * 64] + [f"pk_{i:03d}" for i in range(124)]
        rows = [
            {
                "source_uid": f"source_{i:03d}",
                "score": i * 10,
                "flag": i % 2 == 0,
                "tag": f"tag_{i}",
                "vec": [(i * DIM + j) / 4096 for j in range(DIM)],
            }
            for i in range(128)
        ]
        if not source_auto_id:
            for row, pk in zip(rows, manual_ids):
                row["id"] = pk
        inserted = self.insert(client, source, deepcopy(rows))[0]
        source_ids = inserted["ids"]
        assert inserted["insert_count"] == len(source_ids) == len(set(source_ids)) == len(rows)
        expected_pk_type = int if pk_type == DataType.INT64 else str
        assert all(type(pk) is expected_pk_type for pk in source_ids)
        if not source_auto_id:
            assert source_ids == manual_ids
        expected = {pk: {**deepcopy(row), "id": pk} for pk, row in zip(source_ids, rows)}
        self.flush(client, source)
        self.load_collection(client, source)
        self._assert_rows(client, source, expected, filter_expr="")
        uri = self._default_snapshot_uri(client, source, snapshot, export_prefix, layout, db_name)

        # Step 3: Backup Import must retain PKs, including when the target has AutoID enabled.
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[uri]],
            options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
            api_key=cf.param_info.param_token,
            timeout=30,
        ).json()
        assert response["code"] == 0, response
        job_id = response["data"]["jobId"]
        assert job_id
        log.info(
            f"SI-FN-13 layout={layout}, pk_type={pk_type.name}, source_auto_id={source_auto_id}, "
            f"target_auto_id={target_auto_id}, target={target}, job_id={job_id}"
        )
        progress = self._wait_for_import(url, job_id, db_name)
        self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
        assert progress["state"] == "Completed" and progress["importedRows"] == len(expected), progress
        assert self.wait_for_index_ready(client, target, index_name="vec_idx", timeout=120)
        self.load_collection(client, target)
        self._assert_rows(client, target, expected, filter_expr="")

        # Step 4: A normal insert still follows the target's own AutoID setting.
        new_row = {
            "source_uid": "target_new",
            "score": 9999,
            "flag": False,
            "tag": "after_import",
            "vec": [0.75 + j / 4096 for j in range(DIM)],
        }
        if not target_auto_id:
            new_pk = 1000 if pk_type == DataType.INT64 else "target_new"
            while new_pk in expected:
                new_pk = new_pk + 1 if pk_type == DataType.INT64 else new_pk + "_"
            new_row["id"] = new_pk
        inserted = self.insert(client, target, [deepcopy(new_row)])[0]
        assert inserted["insert_count"] == len(inserted["ids"]) == 1
        new_pk = inserted["ids"][0]
        assert type(new_pk) is expected_pk_type and new_pk not in expected
        if not target_auto_id:
            assert new_pk == new_row["id"]
        new_row["id"] = new_pk
        expected_after = {**expected, new_pk: new_row}
        self._assert_rows(client, target, expected_after, filter_expr="")

        # Step 5: Persist/reload, then recheck all 129 rows and the target definition.
        self.flush(client, target)
        self.release_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
        self.load_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
        self._assert_rows(client, target, expected_after, filter_expr="")
        assert self._definitions(client, target) == definitions_before

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("family,input_type", FUNCTION_CASES, ids=FUNCTION_CASE_IDS)
    def test_snapshot_import_preserves_function_outputs(
        self,
        snapshot_resources,
        snapshot_function_mock,
        snapshot_function_reader,
        family,
        input_type,
        db_name,
    ):
        """
        target: SI-FN-14, preserve every stored function output and its PK association
        method: 1. Insert/Referenced snapshot 32 source rows; 2. Import and compare outputs; 3. search and reload
        expected: Completed/32 rows; exact output values; no embedding calls during Import; same data after reload
        """
        client, source, target, snapshot = snapshot_resources
        mock = snapshot_function_mock
        source_fields = self._create_function_collection(client, source, family, input_type, mock)
        target_fields = self._create_function_collection(client, target, family, input_type, mock)
        assert source_fields == target_fields
        before_insert = mock.counters() if mock else None
        rows = function_rows()
        self.insert(client, source, deepcopy(rows))
        self.flush(client, source)
        self.load_collection(client, source)
        if mock:
            provider = "tei" if family == "embedding_float" else "voyageai"
            assert mock.counters()[provider] > before_insert[provider], "Normal Insert did not reach the mock"
        self._assert_function_payload(client, source, rows)
        self.create_snapshot(client, snapshot, source, db_name=db_name)
        uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location
        expected_outputs = self._read_function_outputs(
            client, source, family, source_fields, snapshot_function_reader, uri=uri
        )
        assert set(expected_outputs) == {row["id"] for row in rows}
        definitions = self._definitions(client, target)
        before_import = mock.counters() if mock else None

        # Import the stored output; a second provider call must not generate the truth.
        progress = self._import_function_snapshot(target, uri, db_name)
        assert progress["state"] == "Completed" and progress["importedRows"] == len(rows), progress
        if mock:
            assert mock.counters() == before_import, "Snapshot Import recomputed embedding output"
        self.load_collection(client, target)
        for reload in (False, True):
            if reload:
                self.release_collection(client, target)
                self.load_collection(client, target)
            self._assert_function_payload(client, target, rows)
            actual_outputs = self._read_function_outputs(
                client, target, family, target_fields, snapshot_function_reader
            )
            assert actual_outputs == expected_outputs
            self._assert_function_search(client, target, family, rows, expected_outputs)
        self._assert_function_search(client, source, family, rows, expected_outputs)
        assert self._definitions(client, target) == definitions

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("family,input_type", FUNCTION_CASES, ids=FUNCTION_CASE_IDS)
    def test_snapshot_import_rejects_missing_function_output(
        self,
        snapshot_resources,
        snapshot_function_mock,
        snapshot_function_reader,
        family,
        input_type,
        db_name,
    ):
        """
        target: SI-FN-14, reject a snapshot missing the target's required function output
        method: 1. Referenced snapshot of input-only source; 2. Import into function target with sentinel; 3. reload
        expected: Failed with missing-output reason; no input-based regeneration; only the unchanged sentinel visible
        """
        client, source, target, snapshot = snapshot_resources
        mock = snapshot_function_mock
        source_fields = self._create_function_collection(client, source, family, input_type, mock, with_output=False)
        target_fields = self._create_function_collection(client, target, family, input_type, mock)
        assert {name: target_fields[name] for name in source_fields} == source_fields
        assert target_fields["output"]["field_id"] not in {field["field_id"] for field in source_fields.values()}
        rows, sentinel = function_rows(), function_rows([-1])
        self.insert(client, source, rows)
        self.flush(client, source)
        self.insert(client, target, deepcopy(sentinel))
        self.flush(client, target)
        self.load_collection(client, target)
        self._assert_function_payload(client, target, sentinel)
        sentinel_output = self._read_function_outputs(client, target, family, target_fields, snapshot_function_reader)
        assert set(sentinel_output) == {-1}
        self.create_snapshot(client, snapshot, source, db_name=db_name)
        uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location
        definitions = self._definitions(client, target)
        before_import = mock.counters() if mock else None

        progress = self._import_function_snapshot(target, uri, db_name)
        assert progress["state"] == "Failed", progress
        assert "no binlog for field:output" in progress.get("reason", "").lower(), progress
        if mock:
            assert mock.counters() == before_import, "Missing output was recomputed from input"
        for reload in (False, True):
            if reload:
                self.release_collection(client, target)
                self.load_collection(client, target)
            self._assert_function_payload(client, target, sentinel)
            assert (
                self._read_function_outputs(client, target, family, target_fields, snapshot_function_reader)
                == sentinel_output
            )
        assert self._definitions(client, target) == definitions

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_import_text_lob_projection(
        self, snapshot_text_resources, minio_host, minio_bucket, tmp_path, db_name
    ):
        """
        target: SI-FN-15, preserve inline/LOB TEXT values and respect target column projection
        method: 1. Snapshot 32 rows with real LOB evidence; 2. Import into three schemas; 3. compare and reload
        expected: three Completed/32 jobs; exact kept text; omitted text_b absent; new text_c NULL after reload
        """
        client, source, snapshot, targets = snapshot_text_resources
        rows, threshold = snapshot_text_rows()
        source_fields = self._create_snapshot_text_collection(client, source, ("text_a", "text_b"))
        self.insert(client, source, deepcopy(rows))
        self.flush(client, source)
        self.load_collection(client, source)
        self._assert_snapshot_text_rows(client, source, rows, ("text_a", "text_b"))
        self.create_snapshot(client, snapshot, source, db_name=db_name)
        uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location

        # Prove the source really contains inline data and out-of-line LOBs before any Import.
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        evidence = inspect_snapshot_text_lobs(store, bucket, uri, source_fields, rows, threshold, tmp_path)
        log.info(f"SI-FN-15 source={source}, threshold_hint={threshold}, LOB evidence={evidence}")
        scenarios = {
            "keep_both": ("text_a", "text_b"),
            "project_b": ("text_a",),
            "append_nullable": ("text_a", "text_b", "text_c"),
        }
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        for scenario, text_fields in scenarios.items():
            target = targets[scenario]
            fields = self._create_snapshot_text_collection(client, target, text_fields)
            for name in set(source_fields) & set(fields):
                assert fields[name]["field_id"] == source_fields[name]["field_id"]
                assert fields[name]["type"] == source_fields[name]["type"]
            if scenario == "project_b":
                assert "text_b" not in fields
            if scenario == "append_nullable":
                assert fields["text_c"].get("nullable") is True
                assert fields["text_c"]["field_id"] not in {f["field_id"] for f in source_fields.values()}
            definitions = self._definitions(client, target)

            # Reuse exactly the same Referenced URI for all three targets.
            self._retain_import_resources = True
            response = bulk_import(
                url=url,
                collection_name=target,
                db_name=db_name,
                files=[[uri]],
                options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
                api_key=cf.param_info.param_token,
                timeout=30,
            ).json()
            assert response["code"] == 0 and response["data"]["jobId"], response
            job_id = response["data"]["jobId"]
            log.info(f"SI-FN-15 scenario={scenario}, target={target}, job_id={job_id}, uri={uri}")
            progress = self._wait_for_import(url, job_id, db_name)
            self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
            assert progress["state"] == "Completed" and progress["importedRows"] == len(rows), progress
            self.load_collection(client, target)
            self._assert_snapshot_text_rows(client, target, rows, text_fields)
            self.release_collection(client, target)
            assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
            self.load_collection(client, target)
            self._assert_snapshot_text_rows(client, target, rows, text_fields)
            assert self._definitions(client, target) == definitions

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("scenario", ["plain", "l0", "mapping"])
    def test_snapshot_import_external_bucket(self, snapshot_external_resources, scenario, db_name):
        """
        target: SI-FN-16, external source credentials affect reads while writes remain in target storage
        method: 1. export 128 rows across buckets 2. Import with a read-only identity 3. query/search/reload
        expected: 128/96/128 exact rows, active L0 and mapping preserved; source unchanged; target-owned output
        """
        client, source, target, snapshot, external, target_bucket, target_snapshot = snapshot_external_resources
        store = external.store
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        api_key = cf.param_info.param_token

        # Step 1: Match physical fields and prepare source truth independently of Import.
        self._create_collection(client, source, "src_vec_idx", shards_num=1)
        self._create_collection(client, target, "dst_vec_idx", shards_num=2)
        assert (
            self.describe_collection(client, source)[0]["fields"]
            == self.describe_collection(client, target)[0]["fields"]
        )
        if scenario == "mapping":
            for partition in ("A", "B"):
                self.create_partition(client, source, partition)
            for partition in ("X", "Y"):
                self.create_partition(client, target, partition)
        rows = [
            {
                "id": pk,
                "source_uid": f"external_{pk}",
                "score": pk * 10,
                "flag": pk % 2 == 0,
                "tag": ("A" if pk < 64 else "B") if scenario == "mapping" else "external",
                "vec": [(pk * DIM + j) / 4096 for j in range(DIM)],
            }
            for pk in range(128)
        ]
        if scenario == "mapping":
            self.insert(client, source, deepcopy(rows[:64]), partition_name="A")
            self.insert(client, source, deepcopy(rows[64:]), partition_name="B")
        else:
            self.insert(client, source, deepcopy(rows))
        self.flush(client, source)
        self.load_collection(client, source)
        self._assert_rows(client, source, {row["id"]: row for row in rows})
        deleted = set(range(32)) if scenario == "l0" else set()
        if deleted:
            assert self.delete(client, source, ids=sorted(deleted))[0]["delete_count"] == len(deleted)
            self.flush(client, source)
        expected = {row["id"]: deepcopy(row) for row in rows if row["id"] not in deleted}
        self._assert_rows(client, source, expected)
        self.load_collection(client, target)
        definitions = self._definitions(client, target)

        # Step 2: Require captured raw rows plus actual applicable L0, then export to a new bucket.
        uri, _, _ = self._capture_snapshot_input(
            client, source, snapshot, store, target_bucket, db_name, 128, deleted, case_id="SI-FN-16"
        )
        export_uri = foreign_export_uri(uri, external.bucket, external.prefix)
        self._retain_import_resources = True
        export_id = export_snapshot(
            url, source, snapshot, export_uri, db_name, api_key, external_spec=external.write_spec
        )
        exported = wait_for_snapshot_export(url, export_id, db_name, api_key)
        self._retain_import_resources = False
        assert exported["state"] == "ExportSnapshotCompleted" and int(exported["totalBytes"]) > 0, exported
        uri = exported["snapshotMetadataURI"]
        metadata, segments, _, captured_deletes = read_snapshot_inventory(
            store, external.bucket, uri, layout="self_contained"
        )
        assert sum(s["num_of_rows"] for s in segments if s["segment_level"] != 1) == 128
        assert captured_deletes == deleted, (captured_deletes, deleted)
        fingerprints = bundle_fingerprints(store, external.bucket, external.prefix)
        put_target_bucket_decoys(store, target_bucket, external.prefix, fingerprints)
        assert_source_readonly(external, target_bucket, fingerprints)

        # Step 3: Only this request's source reader receives the read-only credentials.
        options = {
            "backup": "true",
            "source_type": "snapshot",
            "auto_commit": "true",
            "external_spec": external.read_spec,
        }
        if scenario == "mapping":
            mapping = {"_default": "_default", "A": "Y", "B": "X"}
            assert set(mapping) == set(metadata["collection"]["partitions"])
            options["partition_mapping"] = json.dumps(mapping)
        if scenario == "plain":
            self._retain_import_resources = True
            assert_external_credentials_rejected(url, target, uri, options, db_name, api_key)
            self._retain_import_resources = False
            self._assert_rows(client, target, {})
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[uri]],
            options=options,
            api_key=api_key,
            timeout=30,
        ).json()
        assert response["code"] == 0, response
        job_id = response["data"]["jobId"]
        assert job_id
        log.info(
            f"SI-FN-16 scenario={scenario}, target={target}, source_bucket={external.bucket}, job_id={job_id}, uri={uri}"
        )
        progress = self._wait_for_import(url, job_id, db_name)
        self._retain_import_resources = False
        assert progress["state"] == "Completed" and progress["importedRows"] == len(expected), progress

        # Step 4: Exact values, count, mapped partition membership and top-k before/after reload.
        assert self.wait_for_index_ready(client, target, index_name="dst_vec_idx", timeout=120)
        queries = [expected[pk]["vec"] for pk in (min(expected), 63, 127)]
        for reload in (False, True):
            if reload:
                self.release_collection(client, target)
                self.load_collection(client, target)
            self._assert_rows(client, target, expected)
            if scenario == "mapping":
                for partition, tag in (("X", "B"), ("Y", "A"), ("_default", None)):
                    truth = {pk: row for pk, row in expected.items() if row["tag"] == tag}
                    self._assert_rows(client, target, truth, partition_names=[partition])
            hits = self.search(
                client,
                target,
                data=queries,
                anns_field="vec",
                limit=5,
                output_fields=["source_uid"],
                search_params={"metric_type": "L2"},
                consistency_level="Strong",
            )[0]
            assert_external_snapshot_search(hits, queries, expected)
        assert self._definitions(client, target) == definitions

        # Step 5: Source is unchanged; exact target manifests/data exist in instance-owned storage.
        assert bundle_fingerprints(store, external.bucket, external.prefix) == fingerprints
        self.create_snapshot(client, target_snapshot, target, db_name=db_name, compaction_protection_seconds=3600)
        target_uri = self.describe_snapshot(client, target_snapshot, target, db_name=db_name)[0].s3_location
        target_id = self.describe_collection(client, target)[0]["collection_id"]
        assert_target_snapshot_storage(store, target_bucket, target_uri, target_id, len(expected))

    @pytest.fixture
    def snapshot_external_resources(self, snapshot_resources, minio_host, minio_bucket, tmp_path, db_name):
        client, source, target, snapshot = snapshot_resources
        target_snapshot = f"{snapshot}_target"
        cfg = get_minio_config(minio_host, minio_bucket)
        with external_snapshot_bucket(cfg, tmp_path, lambda: self._retain_import_resources) as external:
            assert not list(external.store.list_objects(cfg["bucket"], prefix=external.prefix, recursive=True))
            try:
                yield client, source, target, snapshot, external, cfg["bucket"], target_snapshot
            finally:
                if not self._retain_import_resources:
                    remove_snapshot_export(external.store, cfg["bucket"], external.prefix)
                    if self.has_collection(client, target)[0]:
                        if target_snapshot in self.list_snapshots(client, target, db_name=db_name)[0]:
                            self.drop_snapshot(client, target_snapshot, target, db_name=db_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_import_v3_owned_data_and_indexes(self, snapshot_ownership_resources, tmp_path, db_name):
        """
        target: SI-FN-17 V3, rewritten data/LOB/index ownership and target definitions survive deleting the bundle
        method: 1. export indexed TEXT source 2. Import to preindexed/unindexed targets 3. delete bundle; reload
        expected: two Completed/4096 jobs; target HNSW/mmap retained; actual target-owned artifacts and exact values
        """
        client, source, targets, snapshot, store, bucket, prefix, capture = snapshot_ownership_resources
        settings = ownership_runtime_config()
        minimum, threshold = settings["index_min_segment_rows"], settings["text_inline_threshold"]
        rows = ownership_rows(threshold)
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        api_key = cf.param_info.param_token

        # Step 1: Source HNSW differs from the two target definitions; T2 really starts unindexed.
        fields = self._create_owned_snapshot_collection(client, source, mmap=False)
        self._create_owned_hnsw(client, source, "source_hnsw", 8, 64)
        initial = {}
        for scenario, target in targets.items():
            assert self._create_owned_snapshot_collection(client, target, mmap=True) == fields
            if scenario == "preindexed":
                self._create_owned_hnsw(client, target, "target_hnsw", 16, 128)
            initial[target] = self._definitions(client, target)
            assert str(initial[target]["collection"]["properties"]["mmap.enabled"]).lower() == "true"
            if scenario == "preindexed":
                assert_hnsw_definition(initial[target], "target_hnsw", 16, 128, mmap=True)
            else:
                assert initial[target]["indexes"] == {}
        for start in range(0, len(rows), 128):
            batch = rows[start : start + 128]
            assert self.insert(client, source, batch)[0]["insert_count"] == len(batch)
        self.flush(client, source)
        assert self.wait_for_index_ready(client, source, "source_hnsw", timeout=300)
        self.load_collection(client, source)
        self._assert_owned_snapshot_rows(client, source, rows)
        source_definitions = self._definitions(client, source)
        assert_hnsw_definition(source_definitions, "source_hnsw", 8, 64, mmap=False)

        # Step 2: Capture real source data/LOB/HNSW evidence, then export exactly one private bundle.
        source_uri, source_evidence = capture(source, snapshot, "source_hnsw", 8, 64, minimum)
        source_lobs = inspect_snapshot_text_lobs(
            store, bucket, source_uri, fields, rows, threshold, tmp_path, case_id="SI-FN-17"
        )
        log.info(f"SI-FN-17 source inline/LOB evidence={source_lobs}")
        source_files = [
            key
            for kind in ("metadata_files", "data_files", "lob_files", "index_files")
            for key in source_evidence[kind]
        ]
        source_fingerprints = snapshot_file_fingerprints(store, bucket, source_files)
        self._retain_import_resources = True
        export_id = export_snapshot(url, source, snapshot, prefix, db_name, api_key)
        exported = wait_for_snapshot_export(url, export_id, db_name, api_key, timeout=600)
        self._retain_import_resources = False
        assert exported["state"] == "ExportSnapshotCompleted" and int(exported["totalBytes"]) > 0, exported
        uri = exported["snapshotMetadataURI"]
        inspect_snapshot_ownership(
            store,
            bucket,
            uri,
            source_definitions["collection"]["collection_id"],
            fields["vec"]["field_id"],
            "source_hnsw",
            8,
            64,
            minimum,
            layout="self_contained",
        )
        bundle = owned_bundle_fingerprints(store, bucket, prefix)

        # Step 3: Same immutable snapshot, exactly two Imports; never load the unindexed target yet.
        final_definitions, target_evidence = {}, {}
        for scenario, target in targets.items():
            self._retain_import_resources = True
            response = bulk_import(
                url=url,
                collection_name=target,
                db_name=db_name,
                files=[[uri]],
                options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
                api_key=api_key,
                timeout=30,
            ).json()
            assert response["code"] == 0, response
            job_id = response["data"]["jobId"]
            assert job_id
            log.info(f"SI-FN-17 scenario={scenario}, target={target}, job_id={job_id}, uri={uri}")
            progress = self._wait_for_import(url, job_id, db_name)
            self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
            assert progress["state"] == "Completed" and progress["importedRows"] == len(rows), progress
            assert self._definitions(client, target) == initial[target]
            if scenario == "unindexed":
                assert self.list_indexes(client, target)[0] == []
                self._create_owned_hnsw(client, target, "target_hnsw", 16, 128)
            assert self.wait_for_index_ready(client, target, "target_hnsw", timeout=300)
            final_definitions[target] = self._definitions(client, target)
            assert final_definitions[target]["collection"] == initial[target]["collection"]
            assert final_definitions[target]["partitions"] == initial[target]["partitions"]
            assert_hnsw_definition(final_definitions[target], "target_hnsw", 16, 128, mmap=True)
            self.load_collection(client, target)
            self._assert_owned_snapshot_rows(client, target, rows)
            self._assert_owned_snapshot_search(client, target, rows)
            target_uri, evidence = capture(target, snapshot + "_" + scenario, "target_hnsw", 16, 128, minimum)
            target_evidence[target] = evidence
            target_lobs = inspect_snapshot_text_lobs(
                store, bucket, target_uri, fields, rows, threshold, tmp_path, case_id="SI-FN-17"
            )
            log.info(f"SI-FN-17 {scenario} inline/LOB evidence={target_lobs}")
            assert set(evidence["segment_ids"]).isdisjoint(source_evidence["segment_ids"])
            for kind in ("data_files", "lob_files", "index_files"):
                assert set(evidence[kind]).isdisjoint(source_evidence[kind])
                assert set(evidence[kind]).isdisjoint(bundle)
        first, second = target_evidence.values()
        for kind in ("data_files", "lob_files", "index_files"):
            assert set(first[kind]).isdisjoint(second[kind])

        # Step 4: Both Imports are terminal; delete ONLY the verified private export copy.
        assert not self._retain_import_resources
        assert owned_bundle_fingerprints(store, bucket, prefix) == bundle
        remove_snapshot_export(store, bucket, prefix)
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True))

        # Step 5: Reload without that bundle and repeat complete values/search/definition checks.
        for target in targets.values():
            self.release_collection(client, target)
            assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
            self.load_collection(client, target)
            assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
            self._assert_owned_snapshot_rows(client, target, rows)
            self._assert_owned_snapshot_search(client, target, rows)
            assert self._definitions(client, target) == final_definitions[target]
        assert self._definitions(client, source) == source_definitions
        assert snapshot_file_fingerprints(store, bucket, source_files) == source_fingerprints
        self.release_collection(client, source)
        self.load_collection(client, source)
        self._assert_owned_snapshot_rows(client, source, rows)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("source_case", ["normal", "all_deleted"])
    @pytest.mark.parametrize("action", ["auto", "commit", "abort"])
    def test_snapshot_import_commit_abort_and_zero_rows(
        self, snapshot_lifecycle_resources, source_case, action, minio_host, minio_bucket, db_name
    ):
        """
        target: SI-FN-18, commit/abort normal and fully deleted Referenced snapshots into a nonempty target
        method: 1. prove source fixture 2. auto/explicit commit or abort 3. repeat/reject actions 4. reload and reuse
        expected: committed normal129 rows; zero-row/aborted1 sentinel; three followups17 rows; terminal states stable
        """
        client, source, target, snapshot, next_source, next_snapshot = snapshot_lifecycle_resources
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        store = new_minio_client(get_minio_config(minio_host, minio_bucket))
        rows = self._snapshot_lifecycle_rows(range(128), "src")
        source_expected = {row["id"]: deepcopy(row) for row in rows}
        sentinel = self._snapshot_lifecycle_rows([10000], "sentinel")[0]
        expected = {sentinel["id"]: deepcopy(sentinel)}

        # Step 1: Real source rows/deletes; the target already has one independent row.
        self._create_collection(client, source, "src_vec_idx", shards_num=1)
        self._create_collection(client, target, "dst_vec_idx", shards_num=1)
        assert (
            self.describe_collection(client, source)[0]["fields"]
            == self.describe_collection(client, target)[0]["fields"]
        )
        before = self._definitions(client, target)
        assert self.insert(client, source, deepcopy(rows))[0]["insert_count"] == len(rows)
        assert self.insert(client, target, [deepcopy(sentinel)])[0]["insert_count"] == 1
        self.flush(client, source)
        self.flush(client, target)
        self.load_collection(client, source)
        self.load_collection(client, target)
        self._assert_rows(client, source, source_expected)
        self._assert_rows(client, target, expected)
        deleted = set(source_expected) if source_case == "all_deleted" else set()
        if deleted:
            assert self.delete(client, source, ids=sorted(deleted))[0]["delete_count"] == len(deleted)
            self.flush(client, source)
            source_expected = {}
            self._assert_rows(client, source, source_expected)
        uri, objects = self._capture_lifecycle_snapshot(
            client, source, snapshot, store, minio_bucket, db_name, len(rows), deleted
        )

        # Step 2: A manual job must park without exposing any source row, even when its output is zero.
        job_id = self._submit_lifecycle_import(
            url, target, uri, db_name, action == "auto", f"{source_case}/{action}/main"
        )
        state = "Completed" if action == "auto" else "Uncommitted"
        progress = self._wait_for_import(url, job_id, db_name, expected_state=state)
        self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
        assert str(progress["jobId"]) == str(job_id) and progress["state"] == state, progress
        assert progress["importedRows"] == len(source_expected), progress
        job_args = {
            "url": url,
            "job_id": job_id,
            "db_name": db_name,
            "api_key": cf.param_info.param_token,
            "timeout": 30,
        }
        terminal = "Failed" if action == "abort" else "Completed"
        if action != "auto":
            self._assert_rows(client, target, expected)
            self._assert_lifecycle_search(client, target, expected)
            operation = commit_import if action == "commit" else abort_import
            assert operation(**job_args).json()["code"] == 0
            progress = self._wait_for_import(url, job_id, db_name, expected_state=terminal)
            self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
        assert progress["state"] == terminal, progress
        if action == "abort":
            assert progress["reason"] == "aborted by user", progress
        else:
            assert progress["importedRows"] == len(source_expected), progress
            expected.update(deepcopy(source_expected))
        assert self.wait_for_index_ready(client, target, "dst_vec_idx", timeout=120)
        self._assert_rows(client, target, expected)
        self._assert_lifecycle_search(client, target, expected)

        # Step 3: Repeating the chosen action is harmless; the opposite terminal action is rejected.
        if action != "auto":
            self._retain_import_resources = True
        if action == "commit":
            assert commit_import(**job_args).json()["code"] == 0
            self._assert_rows(client, target, expected)
            with pytest.raises(MilvusException, match=r"state Completed.*abort not allowed"):
                abort_import(**job_args)
        elif action == "abort":
            assert abort_import(**job_args).json()["code"] == 0
            self._assert_rows(client, target, expected)
            with pytest.raises(MilvusException, match=r"state Failed.*expected Uncommitted"):
                commit_import(**job_args)
        progress = self._wait_for_import(url, job_id, db_name, expected_state=terminal)
        self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
        assert progress["state"] == terminal, progress
        if action == "abort":
            assert progress["reason"] == "aborted by user", progress
        self._reload_lifecycle_target(client, target, expected, before)

        # Step 4: The zero-row auto target and both aborted targets must accept a subsequent normal Import.
        if action == "abort" or (source_case == "all_deleted" and action == "auto"):
            followup = self._snapshot_lifecycle_rows(range(1000, 1016), "followup")
            self._create_collection(client, next_source, "next_vec_idx", shards_num=1)
            assert self.describe_collection(client, next_source)[0]["fields"] == before["collection"]["fields"]
            assert self.insert(client, next_source, deepcopy(followup))[0]["insert_count"] == len(followup)
            self.flush(client, next_source)
            next_uri, next_objects = self._capture_lifecycle_snapshot(
                client, next_source, next_snapshot, store, minio_bucket, db_name, len(followup), set()
            )
            next_job = self._submit_lifecycle_import(
                url, target, next_uri, db_name, True, f"{source_case}/{action}/followup"
            )
            progress = self._wait_for_import(url, next_job, db_name)
            self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
            assert str(progress["jobId"]) == str(next_job) and progress["state"] == "Completed", progress
            assert progress["importedRows"] == len(followup), progress
            expected.update({row["id"]: deepcopy(row) for row in followup})
            assert self.wait_for_index_ready(client, target, "dst_vec_idx", timeout=120)
            self._assert_rows(client, target, expected)
            self._assert_lifecycle_search(client, target, expected)
            self._reload_lifecycle_target(client, target, expected, before)
            self._retain_import_resources = True
            previous = self._wait_for_import(url, job_id, db_name, expected_state=terminal)
            self._retain_import_resources = previous["state"] not in ("Completed", "Failed")
            assert previous["state"] == terminal, previous
            if action == "abort":
                assert previous["reason"] == "aborted by user", previous
            assert_snapshot_objects_unchanged(store, minio_bucket, next_objects)
        self._assert_rows(client, source, source_expected)
        assert_snapshot_objects_unchanged(store, minio_bucket, objects)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("group", ["scalar", "array"], ids=["12-scalar-types", "8-array-element-types"])
    def test_snapshot_import_all_scalar_types(self, snapshot_resources, group, db_name):
        """
        target: SI-FN-19, preserve all scalar/Array values, NULLs and explicit output_fields
        method: 1. insert 32 boundary rows 2. snapshot Import 3. assert projections/filters 4. reload and repeat
        expected: one Completed/32-row Import per group; exact typed values and PK sets before/after reload
        """
        client, source, target, snapshot = snapshot_resources
        fields = scalar_fields(group)
        rows = scalar_rows(fields)
        expected = {row["id"]: deepcopy(row) for row in rows}

        # Step 1: Required/nullable pairs coexist; source output must match input truth.
        self._create_scalar_collection(client, source, fields)
        self._create_scalar_collection(client, target, fields)
        assert (
            self.describe_collection(client, source)[0]["fields"]
            == self.describe_collection(client, target)[0]["fields"]
        )
        assert self.insert(client, source, deepcopy(rows))[0]["insert_count"] == 32
        self.flush(client, source)
        self.load_collection(client, source)
        self._assert_scalar_collection(client, source, fields, expected)
        definitions = self._definitions(client, target)

        # Step 2: Fixed Referenced + auto_commit; exactly one Import for this schema.
        self.create_snapshot(client, snapshot, source, db_name=db_name, compaction_protection_seconds=3600)
        uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[uri]],
            options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
            api_key=cf.param_info.param_token,
            timeout=30,
        ).json()
        assert response["code"] == 0 and response["data"]["jobId"], response
        job_id = response["data"]["jobId"]
        log.info(f"SI-FN-19 group={group}, target={target}, job_id={job_id}, uri={uri}")
        progress = self._wait_for_import(url, job_id, db_name)
        self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
        assert str(progress["jobId"]) == str(job_id) and progress["state"] == "Completed", progress
        assert progress["importedRows"] == 32, progress

        # Step 3: Check every field separately, then all fields; repeat after release/load.
        assert self.wait_for_index_ready(client, target, index_name="scalar_vec_idx", timeout=120)
        self.load_collection(client, target)
        for reload in (False, True):
            if reload:
                self.release_collection(client, target)
                self.load_collection(client, target)
            self._assert_scalar_collection(client, target, fields, expected)
            assert self._definitions(client, target) == definitions
        self._assert_scalar_collection(client, source, fields, expected)

    def _create_scalar_collection(self, client, name, fields):
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=DIM)
        for field in fields:
            self.add_field(schema, field.name, field.dtype, **field.options)
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index(field_name="vec", index_name="scalar_vec_idx", index_type="FLAT", metric_type="L2")
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=1,
            consistency_level="Strong",
            force_teardown=False,
        )
        actual = {field["name"]: field for field in self.describe_collection(client, name)[0]["fields"]}
        assert set(actual) == {"id", "vec", *(field.name for field in fields)}
        for field in fields:
            stored = actual[field.name]
            assert stored["type"] == field.dtype and stored.get("nullable", False) == field.nullable, stored
            if field.element_type is not None:
                assert stored["element_type"] == field.element_type, stored
            for key, value in field.options.items():
                if key in ("max_length", "max_capacity"):
                    assert int(stored["params"][key]) == value, stored

    def _assert_scalar_collection(self, client, name, fields, expected):
        projection = ["id", "vec", *(field.name for field in fields)]
        deadline = time.monotonic() + 120
        actual = []
        # Completed can precede QueryNode visibility. Retry only missing rows, never value errors.
        while (remaining := deadline - time.monotonic()) > 0:
            actual = self.query(
                client,
                name,
                filter="",
                output_fields=projection,
                limit=33,
                consistency_level="Strong",
                timeout=min(30, remaining),
            )[0]
            if len(actual) >= len(expected):
                break
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        assert_scalar_rows(actual, expected, fields, projection)
        assert self.query(client, name, filter="", output_fields=["count(*)"])[0] == [{"count(*)": 32}]
        for field in fields:
            selected = ["id", field.name, "vec"]
            actual = self.query(client, name, filter="", output_fields=selected, limit=33)[0]
            assert_scalar_rows(actual, expected, fields, selected)
        for expression, wanted in scalar_filters(fields, expected):
            actual = self.query(client, name, filter=expression, output_fields=["id"], limit=33)[0]
            assert len(actual) == len(wanted), (name, expression, actual, wanted)
            assert {row["id"] for row in actual} == wanted, (name, expression, actual, wanted)

    @pytest.fixture
    def snapshot_lifecycle_resources(self, snapshot_resources, db_name):
        """The parent owns the primary source/target; this fixture adds only the optional followup source."""
        client, source, target, snapshot = snapshot_resources
        next_source, next_snapshot = source + "_followup", snapshot + "_followup"
        try:
            yield client, source, target, snapshot, next_source, next_snapshot
        finally:
            if not self._retain_import_resources and self.has_collection(client, next_source)[0]:
                if next_snapshot in self.list_snapshots(client, next_source, db_name=db_name)[0]:
                    self.drop_snapshot(client, next_snapshot, next_source, db_name=db_name)
                self.drop_collection(client, next_source)
                assert not self.has_collection(client, next_source)[0]

    @staticmethod
    def _snapshot_lifecycle_rows(pks, label):
        """Small, exact float32 input truth shared by initial, sentinel and followup rows."""
        return [
            {
                "id": pk,
                "source_uid": f"{label}_{pk}",
                "score": pk * 10,
                "flag": pk % 2 == 0,
                "tag": label,
                "vec": [(pk * DIM + j) / 4096 for j in range(DIM)],
            }
            for pk in pks
        ]

    def _capture_lifecycle_snapshot(self, client, source, snapshot, store, bucket, db_name, raw_rows, deleted_pks):
        """Missing raw data/L0 is a bounded fixture failure; API/storage/decoder errors propagate."""
        assert raw_rows > 0
        for attempt in range(3):
            self.create_snapshot(client, snapshot, source, db_name=db_name, compaction_protection_seconds=3600)
            uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location
            uri_bucket, key = unquote(urlsplit(uri).path.lstrip("/")).split("/", 1)
            assert uri_bucket == bucket
            metadata = json.loads(object_bytes(store, bucket, key))
            assert metadata["layout"] in ("SnapshotLayoutReferenced", 1)
            # An empty/compacted snapshot is a missing fixture, never a successful zero-row input.
            objects = read_snapshot_objects(store, bucket, uri) if metadata.get("manifest_list") else {}
            segments = [
                _segment_header(objects[path], int(metadata["format_version"]))
                for path in metadata.get("manifest_list", [])
            ]
            data = [s for s in segments if s["segment_level"] != 1 and s["num_of_rows"] > 0]
            count = sum(s["num_of_rows"] for s in data)
            assert count <= raw_rows, (count, raw_rows)
            actual_deleted = set()
            if count == raw_rows:
                _, _, objects, actual_deleted = read_snapshot_inventory(store, bucket, uri)
                assert actual_deleted <= deleted_pks, actual_deleted
                if actual_deleted == deleted_pks:
                    log.info(
                        f"SI-FN-18 fixture={snapshot}, raw_rows={count}, applicable_deleted_pks={sorted(actual_deleted)}"
                    )
                    return uri, objects
            log.warning(
                f"SI-FN-18 incomplete fixture attempt={attempt + 1}, raw_rows={count}, deleted={len(actual_deleted)}"
            )
            self.drop_snapshot(client, snapshot, source, db_name=db_name)
            if attempt < 2:
                time.sleep(2)
        pytest.fail(
            f"SI-FN-18 Blocked: real data/delete fixture unavailable after 3 captures: rows={count}, deletes={actual_deleted}"
        )

    def _submit_lifecycle_import(self, url, target, uri, db_name, auto_commit, phase):
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[uri]],
            options={"backup": "true", "source_type": "snapshot", "auto_commit": str(auto_commit).lower()},
            api_key=cf.param_info.param_token,
            timeout=30,
        ).json()
        assert response["code"] == 0, response
        job_id = response["data"]["jobId"]
        assert job_id
        log.info(f"SI-FN-18 phase={phase}, target={target}, job_id={job_id}")
        return job_id

    def _reload_lifecycle_target(self, client, target, expected, before):
        self._assert_rows(client, target, expected)
        assert self._definitions(client, target) == before
        self.release_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "NotLoad"
        self.load_collection(client, target)
        assert self.get_load_state(client, target)[0]["state"].name == "Loaded"
        self._assert_rows(client, target, expected)
        self._assert_lifecycle_search(client, target, expected)
        assert self._definitions(client, target) == before

    def _assert_lifecycle_search(self, client, target, expected):
        """FLAT/L2 checks also work when the sentinel is the only visible row."""
        keys = sorted(expected)
        queries = [expected[pk]["vec"] for pk in keys[:: max(1, len(keys) // 3)][:3]]
        limit = min(5, len(expected))
        results = self.search(
            client,
            target,
            data=queries,
            anns_field="vec",
            limit=limit,
            output_fields=FIELDS[1:-1],
            search_params={"metric_type": "L2"},
            consistency_level="Strong",
            timeout=30,
        )[0]
        assert len(results) == len(queries)
        for vector, hits in zip(queries, results):
            distances = {
                pk: float(np.sum((np.asarray(row["vec"], dtype=np.float64) - vector) ** 2))
                for pk, row in expected.items()
            }
            cutoff = sorted(distances.values())[limit - 1]
            strict = {pk for pk, value in distances.items() if value < cutoff}
            allowed = {pk for pk, value in distances.items() if value <= cutoff}
            ids = {hit["id"] for hit in hits}
            assert len(hits) == len(ids) == limit and strict <= ids <= allowed
            for hit in hits:
                assert hit["entity"] == {name: expected[hit["id"]][name] for name in FIELDS[1:-1]}
                assert hit["distance"] == pytest.approx(distances[hit["id"]], rel=1e-5, abs=1e-6)
            assert [hit["distance"] for hit in hits] == sorted(hit["distance"] for hit in hits)

    @pytest.fixture
    def snapshot_ownership_resources(self, snapshot_resources, minio_host, minio_bucket, db_name):
        client, source, target, snapshot = snapshot_resources
        targets = {"preindexed": target, "unindexed": target + "_unindexed"}
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn17_{uuid4().hex}/export_a/"
        owned_snapshots = []
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True))

        def capture(name, snap, index_name, m, ef_construction, minimum):
            deadline = time.monotonic() + 120
            owned_snapshots.append((name, snap))
            fields = {f["name"]: f for f in self.describe_collection(client, name)[0]["fields"]}
            collection_id = self.describe_collection(client, name)[0]["collection_id"]
            while True:
                self.create_snapshot(client, snap, name, db_name=db_name, compaction_protection_seconds=3600)
                uri = self.describe_snapshot(client, snap, name, db_name=db_name)[0].s3_location
                try:
                    evidence = inspect_snapshot_ownership(
                        store,
                        bucket,
                        uri,
                        collection_id,
                        fields["vec"]["field_id"],
                        index_name,
                        m,
                        ef_construction,
                        minimum,
                    )
                    return uri, evidence
                except IndexEvidencePending:
                    self.drop_snapshot(client, snap, name, db_name=db_name)
                    assert time.monotonic() < deadline, "FN-17 real index artifact NotCovered after 120s"
                    time.sleep(min(2, max(0, deadline - time.monotonic())))

        try:
            yield client, source, targets, snapshot, store, bucket, prefix, capture
        finally:
            if not self._retain_import_resources:
                remove_snapshot_export(store, bucket, prefix)
                for name, snap in reversed(owned_snapshots):
                    if (
                        self.has_collection(client, name)[0]
                        and snap in self.list_snapshots(client, name, db_name=db_name)[0]
                    ):
                        self.drop_snapshot(client, snap, name, db_name=db_name)
                if self.has_collection(client, targets["unindexed"])[0]:
                    self.drop_collection(client, targets["unindexed"])
            else:
                log.warning(f"Export/Import may be active; retaining FN-17 targets and private bundle={prefix}")

    def _create_owned_snapshot_collection(self, client, name, mmap):
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        for field, dtype, options in (
            ("id", DataType.INT64, {"is_primary": True}),
            ("marker", DataType.INT64, {}),
            ("vec", DataType.FLOAT_VECTOR, {"dim": DIM}),
            ("text_a", DataType.TEXT, {}),
            ("text_b", DataType.TEXT, {}),
        ):
            self.add_field(schema, field, dtype, **options)
        self.create_collection(
            client, name, schema=schema, shards_num=1, consistency_level="Strong", force_teardown=False
        )
        self.alter_collection_properties(client, name, {"mmap.enabled": mmap})
        return {f["name"]: f for f in self.describe_collection(client, name)[0]["fields"]}

    def _create_owned_hnsw(self, client, name, index_name, m, ef_construction):
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index(
            field_name="vec",
            index_name=index_name,
            index_type="HNSW",
            metric_type="L2",
            params={"M": m, "efConstruction": ef_construction},
        )
        self.create_index(client, name, indexes, timeout=300)

    def _assert_owned_snapshot_rows(self, client, name, expected_rows):
        """Bound TEXT response sizes; count and all batches share one 300s budget."""
        expected = {row["id"]: row for row in expected_rows}
        assert set(expected) == set(range(len(expected_rows)))
        deadline = time.monotonic() + 300

        def query(**kwargs):
            remaining = deadline - time.monotonic()
            assert remaining > 0, f"FN-17 {name} full-row verification exceeded 300s"
            return self.query(client, name, consistency_level="Strong", timeout=min(30, remaining), **kwargs)[0]

        while True:
            count = query(filter="", output_fields=["count(*)"])[0]["count(*)"]
            if count >= len(expected):
                break
            assert time.monotonic() < deadline
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        assert count == len(expected), count
        for start in range(0, len(expected_rows), 256):
            stop = min(start + 256, len(expected_rows))
            actual = query(
                filter=f"id >= {start} and id < {stop}",
                output_fields=["id", "marker", "vec", "text_a", "text_b"],
                limit=stop - start + 1,
            )
            assert len(actual) == stop - start and {row["id"] for row in actual} == set(range(start, stop))
            for row in actual:
                assert set(row) == {"id", "marker", "vec", "text_a", "text_b"}
                truth = expected[row["id"]]
                assert row["marker"] == truth["marker"]
                np.testing.assert_array_equal(np.asarray(row["vec"], dtype=np.float32), truth["vec"])
                for field in ("text_a", "text_b"):
                    assert row[field] == truth[field], (name, row["id"], field)
                    assert len(row[field].encode("utf-8")) == len(truth[field].encode("utf-8"))

    def _assert_owned_snapshot_search(self, client, name, rows):
        queries = [rows[pk]["vec"] for pk in (0, 2048, 4095)]
        results = self.search(
            client,
            name,
            data=queries,
            anns_field="vec",
            limit=5,
            output_fields=["marker"],
            search_params={"metric_type": "L2", "params": {"ef": 128}},
            consistency_level="Strong",
            timeout=60,
        )[0]
        assert_owned_hnsw_search(results, queries, rows)

    @pytest.fixture
    def snapshot_text_resources(self, snapshot_resources):
        client, source, target, snapshot = snapshot_resources
        targets = {"keep_both": target, "project_b": target + "_project", "append_nullable": target + "_nullable"}
        try:
            yield client, source, snapshot, targets
        finally:
            if not self._retain_import_resources:
                # snapshot_resources owns the source, snapshot and keep_both target.
                for name in (targets["project_b"], targets["append_nullable"]):
                    if self.has_collection(client, name)[0]:
                        self.drop_collection(client, name)
                        assert not self.has_collection(client, name)[0]

    def _create_snapshot_text_collection(self, client, name, text_fields):
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        self.add_field(schema, "marker", DataType.INT64)
        self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=DIM)
        for field in text_fields:
            self.add_field(schema, field, DataType.TEXT, nullable=field == "text_c")
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index(field_name="vec", index_name="vec_idx", index_type="FLAT", metric_type="L2")
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=1,
            consistency_level="Strong",
            force_teardown=False,
        )
        fields = {f["name"]: f for f in self.describe_collection(client, name)[0]["fields"]}
        assert set(fields) == {"id", "marker", "vec", *text_fields}
        return fields

    def _assert_snapshot_text_rows(self, client, name, expected_rows, text_fields):
        expected = {row["id"]: row for row in expected_rows}
        wanted = {"id", "marker", "vec", *text_fields}
        deadline, actual = time.monotonic() + 120, []
        while (remaining := deadline - time.monotonic()) > 0:
            actual = self.query(
                client,
                name,
                filter="",
                output_fields=sorted(wanted),
                limit=len(expected) + 1,
                consistency_level="Strong",
                timeout=min(30, remaining),
            )[0]
            if len(actual) >= len(expected):
                break
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        assert len(actual) == len(expected), (name, len(actual), len(expected))
        assert {row["id"] for row in actual} == set(expected)
        for row in actual:
            assert set(row) == wanted, (name, row["id"], set(row))
            original = expected[row["id"]]
            assert row["marker"] == original["marker"]
            np.testing.assert_array_equal(np.asarray(row["vec"], dtype=np.float32), original["vec"])
            for field in text_fields:
                if field == "text_c":
                    assert row[field] is None, (row["id"], field)
                else:
                    assert row[field] == original[field], (row["id"], field, "TEXT differs")
                    assert len(row[field].encode("utf-8")) == len(original[field].encode("utf-8"))
        remaining = deadline - time.monotonic()
        assert remaining > 0, (name, "TEXT verification exhausted its 120s budget")
        count = self.query(
            client,
            name,
            filter="",
            output_fields=["count(*)"],
            consistency_level="Strong",
            timeout=min(30, remaining),
        )[0]
        assert count[0]["count(*)"] == len(expected)

    @pytest.fixture
    def snapshot_function_mock(self, request, family):
        if not family.startswith("embedding_"):
            return None
        return FunctionMockClient(
            request.config.getoption("--snapshot_function_mock_endpoint"),
            request.config.getoption("--snapshot_function_mock_control_endpoint"),
        )

    @pytest.fixture
    def snapshot_function_reader(self, snapshot_resources, minio_host, minio_bucket, tmp_path, db_name):
        client, _, _, _ = snapshot_resources
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        owned = []

        def read(name, fields, uri=None, layout="referenced"):
            if uri is None:
                # Capture the current target again after reload, rather than reread an old snapshot.
                self.flush(client, name)
                snap = f"fn14_oracle_{uuid4().hex}"
                owned.append((name, snap))
                self.create_snapshot(client, snap, name, db_name=db_name)
                uri = self.describe_snapshot(client, snap, name, db_name=db_name)[0].s3_location
            _, rows, _ = read_snapshot_partition_rows(
                store,
                bucket,
                uri,
                {
                    "id": fields["id"]["field_id"],
                    "vector": fields["vec"]["field_id"],
                    "output": fields["output"]["field_id"],
                },
                tmp_path,
                layout=layout,
                case_id="SI-FN-14",
            )
            assert len(rows) == len({row["id"] for row in rows}), "Duplicate physical PKs"
            return {row["id"]: normalize_output("bm25", row["output"]) for row in rows}

        try:
            yield read
        finally:
            if not self._retain_import_resources:
                for name, snap in reversed(owned):
                    if (
                        self.has_collection(client, name)[0]
                        and snap in self.list_snapshots(client, name, db_name=db_name)[0]
                    ):
                        self.drop_snapshot(client, snap, name, db_name=db_name)

    def _create_function_collection(self, client, name, family, input_type, mock, with_output=True):
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        options = {"enable_analyzer": True, "analyzer_params": {"tokenizer": "standard"}}
        if input_type == DataType.VARCHAR:
            options["max_length"] = 65535
        self.add_field(schema, "doc", input_type, **options)
        self.add_field(schema, "marker", DataType.INT64)
        self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=DIM)
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index(field_name="vec", index_name="vec_idx", index_type="FLAT", metric_type="L2")
        if with_output:
            dtype, field, function, index_type, metric, params = function_spec(family, mock)
            self.add_field(schema, "output", dtype, **field)
            schema.add_function(function)
            indexes.add_index(
                field_name="output", index_name="output_idx", index_type=index_type, metric_type=metric, params=params
            )
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=1,
            consistency_level="Strong",
            force_teardown=False,
        )
        return {f["name"]: f for f in self.describe_collection(client, name)[0]["fields"]}

    def _import_function_snapshot(self, target, uri, db_name):
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        self._retain_import_resources = True
        response = bulk_import(
            url=url,
            collection_name=target,
            db_name=db_name,
            files=[[uri]],
            options={"backup": "true", "source_type": "snapshot", "auto_commit": "true"},
            api_key=cf.param_info.param_token,
            timeout=30,
        ).json()
        assert response["code"] == 0 and response["data"]["jobId"], response
        job_id = response["data"]["jobId"]
        log.info(f"SI-FN-14 Import job_id={job_id}, target={target}, uri={uri}")
        progress = self._wait_for_import(url, job_id, db_name)
        self._retain_import_resources = progress["state"] not in ("Completed", "Failed")
        return progress

    def _assert_function_payload(self, client, name, rows):
        expected = {row["id"]: row for row in rows}
        deadline = time.monotonic() + 120
        actual = []
        # Completed does not guarantee that the loaded target already sees the new segments.
        while (remaining := deadline - time.monotonic()) > 0:
            actual = self.query(
                client,
                name,
                filter="",
                output_fields=["id", "doc", "marker", "vec"],
                limit=len(expected) + 1,
                consistency_level="Strong",
                timeout=min(30, remaining),
            )[0]
            if len(actual) >= len(expected):
                break
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        assert len(actual) == len(expected), (name, len(actual), len(expected))
        assert {row["id"] for row in actual} == set(expected)
        for row in actual:
            original = expected[row["id"]]
            assert row["doc"] == original["doc"] and row["marker"] == original["marker"]
            np.testing.assert_array_equal(np.asarray(row["vec"], dtype=np.float32), original["vec"])

    def _read_function_outputs(self, client, name, family, fields, reader, uri=None, layout="referenced"):
        if family == "bm25":
            return reader(name, fields, uri=uri, layout=layout)
        rows = self.query(client, name, filter="", output_fields=["id", "output"], limit=100)[0]
        assert len(rows) == len({row["id"] for row in rows}), "Duplicate query PKs"
        return {row["id"]: normalize_output(family, row["output"]) for row in rows}

    def _assert_function_search(self, client, name, family, rows, outputs):
        if family in ("bm25", "minhash"):
            data = [rows[0]["doc"]]
            metric = "BM25" if family == "bm25" else "MHJACCARD"
            params = {}
        else:
            vector = outputs[rows[0]["id"]]
            data = [np.asarray(vector, dtype=np.int8)] if family == "embedding_int8" else [list(vector)]
            metric = "L2"
            params = {"ef": 64} if family == "embedding_int8" else {}
        results = self.search(
            client,
            name,
            data=data,
            anns_field="output",
            limit=5,
            search_params={"metric_type": metric, "params": params},
            output_fields=["id", "marker"],
        )[0]
        assert len(results) == 1
        hits = results[0]
        assert (1 <= len(hits) <= 5) if family == "minhash" else len(hits) == 5
        ids = [hit["id"] for hit in hits]
        expected = {row["id"]: row for row in rows}
        assert len(ids) == len(set(ids)) and set(ids) <= set(expected)
        distances = [hit["distance"] for hit in hits]
        assert all(np.isfinite(distance) for distance in distances)
        assert distances == sorted(distances, reverse=metric in ("BM25", "MHJACCARD"))
        for hit in hits:
            assert set(hit["entity"]) == {"id", "marker"}
            assert hit["entity"]["id"] == hit["id"]
            assert hit["entity"]["marker"] == expected[hit["id"]]["marker"]

    @pytest.fixture
    def snapshot_auto_id_resources(self, snapshot_resources, minio_host, minio_bucket):
        client, source, target, snapshot = snapshot_resources
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn13_{uuid4().hex}/export_a/"
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, target, snapshot, prefix
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {prefix}")
            else:
                remove_snapshot_export(store, bucket, prefix)

    @staticmethod
    def _assert_default_import_error(reason, error_kind):
        assert isinstance(reason, str) and reason, "Missing schema rejection reason"
        if error_kind == "missing_required":
            assert "no binlog for field:required_value" in reason.lower(), reason
        else:
            assert error_kind == "null_required"
            assert all(token in reason.lower() for token in ("wrong row type", "int64", "<nil>")), reason
        log.info(f"SI-FN-10 expected schema rejection scenario={error_kind}, reason={reason}")

    def _create_default_fields_collection(self, client, name, field_specs):
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=DIM)
        for field, dtype, options in field_specs:
            self.add_field(schema, field, dtype, **options)
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index(field_name="vec", index_name="vec_idx", index_type="FLAT", metric_type="L2")
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=1,
            consistency_level="Strong",
            force_teardown=False,
        )
        fields = {f["name"]: f for f in self.describe_collection(client, name)[0]["fields"]}
        assert set(fields) == {"id", "vec", *(s[0] for s in field_specs)}
        return fields

    def _default_snapshot_uri(self, client, source, snapshot, export_prefix, layout, db_name):
        self.create_snapshot(client, snapshot, source, db_name=db_name)
        uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location
        assert uri
        if layout == "self_contained":
            url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
            self._retain_import_resources = True
            export_id = export_snapshot(url, source, snapshot, export_prefix, db_name, cf.param_info.param_token)
            exported = wait_for_snapshot_export(url, export_id, db_name, cf.param_info.param_token)
            self._retain_import_resources = False
            assert exported["state"] == "ExportSnapshotCompleted", exported
            assert exported["collectionName"] == source and exported["snapshotName"] == snapshot
            uri = exported["snapshotMetadataURI"]
            assert uri
        return uri

    @pytest.fixture
    def snapshot_default_resources(self, snapshot_resources, minio_host, minio_bucket):
        client, source, target, snapshot = snapshot_resources
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn10_{uuid4().hex}/export_a/"
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, target, snapshot, prefix
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {prefix}")
            else:
                remove_snapshot_export(store, bucket, prefix)

    def _create_field_id_collection(self, client, name, scalar_names):
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        for field in scalar_names:
            self.add_field(schema, field, DataType.INT64)
        self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=DIM)
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index(field_name="vec", index_name="vec_idx", index_type="FLAT", metric_type="L2")
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=1,
            consistency_level="Strong",
            force_teardown=False,
        )

    def _assert_field_id_rows(self, client, name, expected, scalar_names, case_id="SI-FN-09"):
        started = time.monotonic()
        deadline = started + 120
        initial_rows = None
        polls = 0
        while True:
            rows = self.query(
                client,
                name,
                filter="id >= 0",
                output_fields=["id", *scalar_names, "vec"],
                limit=len(expected) + 1,
                consistency_level="Strong",
                timeout=30,
            )[0]
            polls += 1
            if initial_rows is None:
                initial_rows = len(rows)
            if len(rows) >= len(expected) or time.monotonic() >= deadline:
                break
            time.sleep(1)
        log.info(
            f"{case_id} visibility collection={name}, initial_rows={initial_rows}, rows={len(rows)}, "
            f"polls={polls}, elapsed_seconds={time.monotonic() - started:.3f}"
        )
        assert len(rows) == len(expected), (len(rows), len(expected))
        actual = {row["id"]: row for row in rows}
        assert len(actual) == len(rows) and actual.keys() == expected.keys()
        for pk, truth in expected.items():
            for field in ("id", *scalar_names):
                assert actual[pk][field] == truth[field], (pk, field, actual[pk][field], truth[field])
            np.testing.assert_array_equal(actual[pk]["vec"], truth["vec"], err_msg=f"PK={pk}")
        count = self.query(client, name, filter="", output_fields=["count(*)"], consistency_level="Strong", timeout=30)[
            0
        ][0]["count(*)"]
        assert count == len(expected), count

    @pytest.fixture
    def snapshot_field_id_resources(self, snapshot_resources, minio_host, minio_bucket):
        client, source, target, snapshot = snapshot_resources
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn09_{uuid4().hex}/export_a/"
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, target, snapshot, prefix
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {prefix}")
            else:
                remove_snapshot_export(store, bucket, prefix)

    @pytest.fixture
    def snapshot_mapping_resources(self, snapshot_resources, minio_host, minio_bucket):
        client, source, target, snapshot = snapshot_resources
        merged = f"{target}_merged"
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn08_{uuid4().hex}/export_a/"
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, (target, merged), snapshot, prefix
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {merged}, {prefix}")
            else:
                remove_snapshot_export(store, bucket, prefix)
                if self.has_collection(client, merged)[0]:
                    self.drop_collection(client, merged)
                    assert not self.has_collection(client, merged)[0]

    def _create_key_fixture_collection(self, client, name, role):
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        for field, dtype, options in (
            ("id", DataType.INT64, {"is_primary": True}),
            ("source_uid", DataType.VARCHAR, {"max_length": 64}),
            ("score", DataType.INT64, {}),
            ("flag", DataType.BOOL, {}),
            ("tag", DataType.VARCHAR, {"max_length": 64}),
            ("vec", DataType.FLOAT_VECTOR, {"dim": DIM}),
        ):
            self.add_field(schema, field, dtype, **options)
        if role != "missing":
            options = {"nullable": role == "null"}
            if role in ("partitioned_target", "plain_target"):
                options["default_value"] = 7
            if role == "partitioned_target":
                options["is_partition_key"] = True
            self.add_field(schema, "route_key", DataType.INT64, **options)
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index(field_name="vec", index_name="vec_idx", index_type="FLAT", metric_type="L2")
        options = {"num_partitions": 8} if role == "partitioned_target" else {}
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=1,
            consistency_level="Strong",
            force_teardown=False,
            **options,
        )

    def _assert_key_values(self, client, name, expected):
        rows = self.query(
            client,
            name,
            filter="id >= 0",
            output_fields=["id", "route_key"],
            limit=len(expected) + 1,
            consistency_level="Strong",
        )[0]
        assert len(rows) == len(expected)
        assert {r["id"]: r["route_key"] for r in rows} == {pk: r["route_key"] for pk, r in expected.items()}
        for key in (0, 1, 2, 3, 7):
            rows = self.query(
                client,
                name,
                filter=f"route_key == {key}",
                output_fields=["id", "route_key"],
                limit=len(expected) + 1,
                consistency_level="Strong",
            )[0]
            ids = {pk for pk, r in expected.items() if r["route_key"] == key}
            assert len(rows) == len(ids) and {r["id"] for r in rows} == ids
            assert all(r["route_key"] == key for r in rows)

    @staticmethod
    def _assert_key_placement(metadata, rows, expected, partition_key):
        partitions = {name: int(pid) for name, pid in metadata["collection"]["partitions"].items()}
        assert len(set(partitions.values())) == len(partitions)
        if partition_key:
            slots = {int(name.rsplit("_", 1)[1]): pid for name, pid in partitions.items()}
            assert len(partitions) == 8 and set(slots) == set(range(8)), partitions
        else:
            assert set(partitions) == {"_default"}, partitions
        assert len(rows) == len(expected) and {r["id"] for r in rows} == expected.keys()
        counts = {pid: 0 for pid in partitions.values()}
        for row in rows:
            truth = expected[row["id"]]
            assert row["route_key"] == truth["route_key"]
            expected_partition = (
                slots[int64_partition_slot(truth["route_key"], 8)] if partition_key else partitions["_default"]
            )
            assert row["partition_id"] == expected_partition, (row["id"], row["partition_id"], expected_partition)
            np.testing.assert_array_equal(row["vector"], truth["vec"])
            counts[expected_partition] += 1
        log.info(f"SI-FN-07 physical routing verified, partitions={partitions}, row_counts={counts}")

    @pytest.fixture
    def snapshot_key_resources(self, snapshot_resources, minio_host, minio_bucket, db_name):
        client, source, target, snapshot = snapshot_resources
        control = f"{target}_control"
        output_snapshots = {target: f"{snapshot}_routed", control: f"{snapshot}_control"}
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn07_{uuid4().hex}/export_a/"
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, target, control, snapshot, output_snapshots, store, bucket, prefix
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {control}, {prefix}, {output_snapshots}")
            else:
                for name, output_snapshot in output_snapshots.items():
                    if self.has_collection(client, name)[0]:
                        if output_snapshot in self.list_snapshots(client, name, db_name=db_name)[0]:
                            self.drop_snapshot(client, output_snapshot, name, db_name=db_name)
                remove_snapshot_export(store, bucket, prefix)
                if self.has_collection(client, control)[0]:
                    self.drop_collection(client, control)
                    assert not self.has_collection(client, control)[0]

    @staticmethod
    def _assert_routing_snapshot_input(metadata, data, deletes):
        partitions = {name: int(pid) for name, pid in metadata["collection"]["partitions"].items()}
        expected_counts = {"_default": 0, "A": 32, "B": 32, "C": 32, "empty_source": 0}
        assert set(partitions) == set(expected_counts), partitions
        assert len(set(partitions.values())) == len(partitions) and all(pid > 0 for pid in partitions.values())
        assert int(metadata["collection"]["num_shards"]) == 2
        assert not deletes, "FN-06 fixture must not contain L0"
        assert all(s["partition_id"] in partitions.values() for s in data)
        counts = {
            name: sum(s["num_of_rows"] for s in data if s["partition_id"] == pid) for name, pid in partitions.items()
        }
        assert counts == expected_counts, f"FN-06 snapshot input not fully captured: {counts}"

    @pytest.fixture
    def snapshot_routing_resources(self, snapshot_resources, minio_host, minio_bucket):
        """Own FN-06's second target and optional export, preserving resources for active jobs."""
        client, source, target, snapshot = snapshot_resources
        named_target = f"{target}_named"
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn06_{uuid4().hex}/export_a/"
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, (target, named_target), snapshot, store, bucket, prefix
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {named_target}, {prefix}")
            else:
                remove_snapshot_export(store, bucket, prefix)
                if self.has_collection(client, named_target)[0]:
                    self.drop_collection(client, named_target)
                    assert not self.has_collection(client, named_target)[0]

    @staticmethod
    def _partition_delete_fixture_ready(metadata, data, deletes):
        partitions = {name: int(pid) for name, pid in metadata["collection"]["partitions"].items()}
        assert set(partitions) == {"_default", "A", "B"}, partitions
        assert len(set(partitions.values())) == 3 and all(pid > 0 for pid in partitions.values())
        counts = {
            name: sum(s["num_of_rows"] for s in data if s["partition_id"] == pid) for name, pid in partitions.items()
        }
        assert counts["_default"] == 0 and counts["A"] <= 65 and counts["B"] <= 65, counts
        assert all(s["partition_id"] in (partitions["A"], partitions["B"]) for s in data)
        channels = {
            name: {s["channel_name"] for s in data if s["partition_id"] == partitions[name]} for name in ("A", "B")
        }
        active = {scope: records for scope, records in deletes.items() if records}
        for (channel, partition), records in active.items():
            assert partition == partitions["A"] and {pk for pk, _ in records} == {10}, active
            if channel not in channels["A"] & channels["B"]:
                return False
        return counts["A"] == counts["B"] == 65 and len(channels["A"]) == len(channels["B"]) == 2 and bool(active)

    def _capture_partition_delete_snapshot(self, client, source, snapshot, store, bucket, db_name):
        for attempt in range(3):
            self.create_snapshot(client, snapshot, source, db_name=db_name, compaction_protection_seconds=3600)
            uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location
            inventory = read_snapshot_partition_inventory(store, bucket, uri)
            if self._partition_delete_fixture_ready(*inventory[:3]):
                return uri, *inventory
            log.warning(f"SI-FN-05 incomplete captured fixture attempt={attempt + 1}")
            self.drop_snapshot(client, snapshot, source, db_name=db_name)
            if attempt < 2:
                time.sleep(2)
        pytest.fail("SI-FN-05 fixture not captured after 3 attempts: require A/B raw rows, two channels and A-only L0")

    @pytest.fixture
    def snapshot_partition_resources(self, snapshot_resources, minio_host, minio_bucket):
        client, source, target, snapshot = snapshot_resources
        merged = f"{target}_merged"
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn05_{uuid4().hex}/export_a/"
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, (target, merged), snapshot, store, bucket, prefix
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {merged}, {prefix}")
            else:
                remove_snapshot_export(store, bucket, prefix)
                if self.has_collection(client, merged)[0]:
                    self.drop_collection(client, merged)
                    assert not self.has_collection(client, merged)[0]

    @pytest.fixture
    def snapshot_reinsert_resources(self, snapshot_resources, minio_host, minio_bucket):
        """Own the FN-03 export prefix; the parent fixture owns collections and the snapshot."""
        client, source, target, snapshot = snapshot_resources
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn03_{uuid4().hex}/export_a/"
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, target, snapshot, store, bucket, prefix
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {prefix}")
            else:
                remove_snapshot_export(store, bucket, prefix)

    def _capture_snapshot_input(
        self,
        client,
        source,
        snapshot,
        store,
        bucket,
        db_name,
        inserted_rows,
        deleted_pks,
        pk_type=DataType.INT64,
        case_id="SI-FN-01",
    ):
        """Retry only an incomplete capture, never an API/storage/decoder error."""
        for attempt in range(3):
            self.create_snapshot(client, snapshot, source, db_name=db_name, compaction_protection_seconds=3600)
            uri = self.describe_snapshot(client, snapshot, source, db_name=db_name)[0].s3_location
            metadata, segments, objects, deleted = read_snapshot_inventory(store, bucket, uri, pk_type=pk_type)
            row_count = sum(s["num_of_rows"] for s in segments if s["segment_level"] != 1)
            assert row_count <= inserted_rows and deleted <= deleted_pks, (row_count, deleted)
            if row_count == inserted_rows and deleted == deleted_pks:
                log.info(
                    f"{case_id} snapshot={snapshot}, captured_rows={row_count}, captured_deletes={sorted(deleted)}"
                )
                return uri, metadata, objects
            log.warning(
                f"{case_id} incomplete fixture attempt={attempt + 1}: rows={row_count}, deletes={sorted(deleted)}"
            )
            self.drop_snapshot(client, snapshot, source, db_name=db_name)
            if attempt < 2:
                time.sleep(2)
        pytest.fail(f"{case_id} fixture not ready after 3 captures: rows={row_count}, deletes={sorted(deleted)}")

    @pytest.fixture
    def snapshot_l0_resources(self, snapshot_resources, minio_host, minio_bucket):
        """Add one private export prefix; the parent fixture owns collections and the snapshot."""
        client, source, target, snapshot = snapshot_resources
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        prefix = f"snapshot_import_fn02_{uuid4().hex}/export_a/"
        assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, target, snapshot, store, bucket, prefix
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {prefix}")
            else:
                remove_snapshot_export(store, bucket, prefix)

    @pytest.fixture
    def snapshot_pair_resources(self, snapshot_resources, minio_host, minio_bucket, db_name):
        """Extend the existing fixture with a second snapshot/target and private export roots."""
        client, source, target_s1, snapshot_s1 = snapshot_resources
        target_s2, snapshot_s2 = f"{target_s1}_s2", f"{snapshot_s1}_s2"
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        root = f"snapshot_import_fn01_{uuid4().hex}"
        export_prefixes = (f"{root}/export_a/", f"{root}/export_b/")
        for prefix in export_prefixes:
            assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield client, source, (target_s1, target_s2), (snapshot_s1, snapshot_s2), store, bucket, export_prefixes
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {target_s2}, {snapshot_s2}, {root}")
            else:
                for prefix in export_prefixes:
                    remove_snapshot_export(store, bucket, prefix)
                if self.has_collection(client, source)[0]:
                    if snapshot_s2 in self.list_snapshots(client, source, db_name=db_name)[0]:
                        self.drop_snapshot(client, snapshot_s2, source, db_name=db_name)
                if self.has_collection(client, target_s2)[0]:
                    self.drop_collection(client, target_s2)
                    assert not self.has_collection(client, target_s2)[0]

    @pytest.fixture
    def snapshot_exports(self, snapshot_resources, minio_host, minio_bucket):
        """Own two export prefixes; never delete them while export/Import may be active."""
        cfg = get_minio_config(minio_host, minio_bucket)
        store, bucket = new_minio_client(cfg), cfg["bucket"]
        root = f"snapshot_import_sm02_{uuid4().hex}"
        export_a, export_b = f"{root}/export_a/", f"{root}/export_b/"
        for prefix in (export_a, export_b):
            assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix
        try:
            yield store, bucket, export_a, export_b
        finally:
            if self._retain_import_resources:
                log.warning(f"Export/Import may be active; retaining {export_a}, {export_b}")
            else:
                for prefix in (export_a, export_b):
                    remove_snapshot_export(store, bucket, prefix)

    @pytest.fixture
    def snapshot_resources(self, db_name):
        """Clean up test-owned resources unless an Import may still be using them."""
        client = self._client(db_name=db_name)
        prefix = f"{self.__class__.__name__[:40]}_{uuid4().hex[:10]}"
        source, target, snapshot = f"{prefix}_src", f"{prefix}_dst", f"{prefix}_snapshot"
        self._retain_import_resources = False
        try:
            yield client, source, target, snapshot
        finally:
            try:
                if self._retain_import_resources:
                    log.warning(f"Import terminal state unknown; retaining {source}, {target}, {snapshot}")
                else:
                    if self.has_collection(client, source)[0]:
                        if snapshot in self.list_snapshots(client, source, db_name=db_name)[0]:
                            self.drop_snapshot(client, snapshot, source, db_name=db_name)
                    for name in (target, source):
                        if self.has_collection(client, name)[0]:
                            self.drop_collection(client, name)
                            assert not self.has_collection(client, name)[0]
            finally:
                self.close(client)

    def _create_collection(self, client, name, index_name, pk_type=DataType.INT64, auto_id=False, **kwargs):
        assert pk_type in (DataType.INT64, DataType.VARCHAR), pk_type
        pk_options = {"is_primary": True}
        if pk_type == DataType.VARCHAR:
            pk_options["max_length"] = 64
        schema = self.create_schema(client, auto_id=auto_id, enable_dynamic_field=False)[0]
        for field, dtype, options in (
            ("id", pk_type, pk_options),
            ("source_uid", DataType.VARCHAR, {"max_length": 64}),
            ("score", DataType.INT64, {}),
            ("flag", DataType.BOOL, {}),
            ("tag", DataType.VARCHAR, {"max_length": 64}),
            ("vec", DataType.FLOAT_VECTOR, {"dim": DIM}),
        ):
            self.add_field(schema, field, dtype, **options)
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index(field_name="vec", index_name=index_name, index_type="FLAT", metric_type="L2")
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            consistency_level="Strong",
            force_teardown=False,
            **kwargs,
        )

    @staticmethod
    def _wait_for_import(url, job_id, db_name, timeout=600, expected_state="Completed"):
        deadline = time.monotonic() + timeout
        last_data = None
        last_state = None
        while time.monotonic() < deadline:
            response = get_import_progress(
                url=url,
                job_id=job_id,
                db_name=db_name,
                api_key=cf.param_info.param_token,
                timeout=30,
            ).json()
            assert response["code"] == 0, response
            last_data = response["data"]
            if last_data["state"] != last_state:
                last_state = last_data["state"]
                log.info(f"Import job_id={job_id}, state={last_state}")
            if last_state in (expected_state, "Completed", "Failed"):
                return last_data
            time.sleep(1)
        pytest.fail(f"Import {job_id} did not reach {expected_state} within {timeout}s: {last_data}")

    def _definitions(self, client, name):
        indexes = {}
        for index in self.list_indexes(client, name)[0]:
            detail = self.describe_index(client, name, index)[0]
            indexes[index] = {
                k: v
                for k, v in detail.items()
                if k not in {"state", "indexed_rows", "total_rows", "pending_index_rows"}
            }
        return {
            "collection": self.describe_collection(client, name)[0],
            "partitions": sorted(self.list_partitions(client, name)[0]),
            "indexes": indexes,
        }

    def _assert_rows(self, client, name, expected, filter_expr="id >= 0", partition_names=None):
        scope = {} if partition_names is None else {"partition_names": partition_names}
        deadline = time.monotonic() + 120
        while True:
            rows = self.query(
                client,
                name,
                filter=filter_expr,
                output_fields=FIELDS,
                limit=len(expected) + 1,
                consistency_level="Strong",
                timeout=30,
                **scope,
            )[0]
            if len(rows) >= len(expected) or time.monotonic() >= deadline:
                break
            time.sleep(1)
        assert len(rows) == len(expected), (len(rows), len(expected))
        actual = {row["id"]: row for row in rows}
        assert len(actual) == len(rows) and actual.keys() == expected.keys()
        for pk, truth in expected.items():
            for field in FIELDS[:-1]:
                assert actual[pk][field] == truth[field], (pk, field, actual[pk][field], truth[field])
            # This fixture uses binary-exact float32 values, so exact comparison is intentional.
            np.testing.assert_array_equal(actual[pk]["vec"], truth["vec"], err_msg=f"PK={pk}")
        count = self.query(
            client,
            name,
            filter="",
            output_fields=["count(*)"],
            consistency_level="Strong",
            timeout=30,
            **scope,
        )[0][0]["count(*)"]
        assert count == len(expected), count

    def _assert_search(self, client, name, expected):
        queries = [expected[pk]["vec"] for pk in (0, 63, 127, 10000)]
        results = self.search(
            client,
            name,
            data=queries,
            anns_field="vec",
            limit=5,
            output_fields=["source_uid"],
            search_params={"metric_type": "L2"},
            consistency_level="Strong",
        )[0]
        assert len(results) == len(queries)
        for vector, hits in zip(queries, results):
            distances = {
                pk: float(np.sum((np.array(row["vec"], dtype=np.float64) - vector) ** 2))
                for pk, row in expected.items()
            }
            threshold = sorted(distances.values())[4]
            strict = {pk for pk, distance in distances.items() if distance < threshold}
            allowed = {pk for pk, distance in distances.items() if distance <= threshold}
            ids = {hit["id"] for hit in hits}
            assert len(hits) == len(ids) == 5 and strict <= ids <= allowed
            # Equal-distance boundary ties may return either eligible PK.
            for hit in hits:
                assert hit["entity"]["source_uid"] == expected[hit["id"]]["source_uid"]
                assert hit["distance"] == pytest.approx(distances[hit["id"]], rel=1e-5, abs=1e-6)
            assert [hit["distance"] for hit in hits] == sorted(hit["distance"] for hit in hits)


# Additional admission controls with real external tables and self-contained fixtures.


class TestSnapshotImportBoundaries(SnapshotBatchBase):
    @pytest.mark.tags(CaseLabel.L2)
    def test_batch_er03_real_external_collection(self, batch, minio_host, minio_bucket):
        """
        target: SI-ER-03, a real external collection snapshot is not an Import source
        method: 1. create snapshot-backed external collection 2. refresh/query 3. snapshot and reject Import
        expected: actual external identity rejected; target sentinel and producer rows unchanged
        """
        source = batch.create("source")
        rows = plain_rows(range(32))
        batch.client.insert(source, rows)
        batch.client.flush(source)
        uri = batch.snapshot(source)
        cfg = get_minio_config(minio_host, minio_bucket)
        external = batch.name + "_external"
        schema = batch.client.create_schema(
            auto_id=False,
            enable_dynamic_field=False,
            external_source=uri,
            external_spec=build_external_spec(cfg, fmt="milvus-table"),
        )
        schema.add_field("id", DataType.INT64, is_primary=True, external_field="id")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=16, external_field="vec")
        schema.add_field("marker", DataType.INT64, external_field="marker")
        batch.collections.append(external)
        batch.client.create_collection(external, schema=schema, shards_num=1, consistency_level="Strong")
        indexes = batch.client.prepare_index_params()
        indexes.add_index("vec", index_type="FLAT", metric_type="L2")
        batch.client.create_index(external, indexes)
        info = batch.client.describe_collection(external)
        assert info["external_source"] == uri
        refresh = batch.client.refresh_external_collection(external)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            state = batch.client.get_refresh_external_collection_progress(refresh)
            value = state.get("state") if isinstance(state, dict) else state.state
            if value in ("RefreshCompleted", "RefreshFailed"):
                break
            time.sleep(1)
        assert value == "RefreshCompleted", value
        batch.assert_rows(external, rows)
        foreign_snapshot = batch.snapshot(external)
        target = batch.create("target")
        batch.client.insert(target, plain_rows([-1]))
        batch.import_rejected(target, foreign_snapshot, label="SI-ER-03/real_external", reason=["external collection"])
        batch.reload_assert(target, plain_rows([-1]))
        batch.assert_rows(source, rows)

    @pytest.mark.tags(CaseLabel.L2)
    def test_batch_er03_compatible_future_ocf(self, batch):
        """
        target: SI-ER-03, compatible future OCF version is not incorrectly treated as unsupported
        method: 1. export actual V6 OCF fixture 2. change only format identifier 3. Import/reload
        expected: compatible self-describing descriptors still import 32 exact rows
        """
        source, target = batch.create("source"), batch.create("target")
        rows = plain_rows(range(32))
        batch.client.insert(source, rows)
        batch.client.flush(source)
        uri = batch.snapshot(source, "self_contained")
        metadata = batch.metadata(uri)
        assert int(metadata["format_version"]) >= 6
        metadata["format_version"] = 999
        key = urlsplit(uri).path.split("/" + batch.bucket + "/", 1)[1]
        batch.put(key, metadata)
        batch.import_ok(target, uri, 32, label="SI-ER-03/future_OCF")
        batch.reload_assert(target, rows)

    @pytest.mark.tags(CaseLabel.L2)
    def test_batch_sec03_cmke_text_admission(self, batch):
        """
        target: SI-SEC-03, encrypted-source metadata with TEXT is rejected at admission
        method: 1. private exported TEXT fixture 2. declare EZ identity and matching key 3. require TEXT-specific rejection
        expected: nonzero create code/no job and TEXT-specific reason; synthetic fixture does not prove actual KMS
        """
        extra = [("text", DataType.TEXT, {})]
        source, target = batch.create("source", extra), batch.create("target", extra)
        rows = [{**row, "text": "private"} for row in plain_rows(range(32))]
        batch.client.insert(source, rows)
        batch.client.flush(source)
        uri = batch.snapshot(source, "self_contained")
        metadata = batch.metadata(uri)
        metadata["collection"]["schema"].setdefault("properties", []).append({"key": "cipher.ezID", "value": "42"})
        key = urlsplit(uri).path.split("/" + batch.bucket + "/", 1)[1]
        batch.put(key, metadata)
        base, version = next(iter(snapshot_manifest_versions(metadata).values()))
        base = _snapshot_read_path(base, metadata, key, batch.bucket, "self_contained")
        physical_manifest = base + f"/_metadata/manifest-{version}.avro"
        assert physical_manifest.startswith(batch.prefix)
        assert batch.store.stat_object(batch.bucket, physical_manifest).size > 0
        batch.store.remove_object(batch.bucket, physical_manifest)
        sentinel = [{**row, "text": "sentinel"} for row in plain_rows([-1])]
        batch.client.insert(target, sentinel)
        response = batch.request(
            target,
            uri,
            options={
                "backup": "true",
                "source_type": "snapshot",
                "auto_commit": "true",
                "ezk": base64.b64encode(b'{"ez_id":42}').decode(),
            },
            label="SI-SEC-03/CMEK_TEXT_admission",
        )
        assert response["code"] != 0 and not response.get("data", {}).get("jobId"), response
        assert "does not support TEXT/LOB" in response["message"], response
        batch.record("admission_only", real_encrypted_data=False, kms_tested=False, expected_guard="CMEK+TEXT")
        batch.reload_assert(target, sentinel)


# SI-ER-01..04 static public request and private snapshot corruption cases.

BASE_OPTIONS = {"backup": "true", "source_type": "snapshot", "auto_commit": "true", "timeout": "60s"}
OPTION_ERRORS = {
    "missing_backup": ("backup", None),
    "backup_false": ("backup", "false"),
    "unknown_source": ("source_type", "not-a-source"),
    "storage_version": ("storage_version", "3"),
    "l0_import": ("l0_import", "true"),
    "source_partition": ("source_partition_name", "_default"),
    "reserved_uri": ("_snapshot_source_uri", "internal"),
    "reserved_layout": ("_snapshot_layout", "referenced"),
    "bad_timestamp": ("start_ts", "not-a-timestamp"),
    "reverse_range": ("start_ts", "18446744073709551615"),
}
FILE_ERRORS = ("empty_files", "two_entries", "two_uris", "empty_uri", "bare_key", "segment_manifest")
MAPPING_ERRORS = (
    "missing_A",
    "missing_empty",
    "unknown_source",
    "unknown_target",
    "empty_source",
    "empty_target",
    "non_string",
    "invalid_json",
    "duplicate_key",
    "oversized_utf8",
    "partition_conflict",
    "partition_key",
    "missing_collection",
)


class TestSnapshotImportErrors(SnapshotBatchBase):
    @staticmethod
    def _source(batch, partitions=()):
        source = batch.create("source", partitions=partitions)
        rows = plain_rows(range(32))
        if partitions:
            batch.client.insert(source, rows[:16], partition_name="A")
            batch.client.insert(source, rows[16:], partition_name="B")
        else:
            batch.client.insert(source, rows)
        batch.client.flush(source)
        uri = batch.snapshot(source)
        return source, uri, rows

    @staticmethod
    def _target(batch, suffix="target", partitions=()):
        target = batch.create(suffix, partitions=partitions)
        batch.client.insert(target, plain_rows([-1]))
        batch.client.flush(target)
        return target

    @staticmethod
    def _unchanged(batch, target, before):
        batch.reload_assert(target, plain_rows([-1]))
        assert batch.client.describe_collection(target) == before

    @pytest.mark.tags(CaseLabel.L2)
    def test_batch_er01_valid_control(self, batch):
        """
        target: SI-ER-01, known-good request remains valid before/after negative variants
        method: snapshot 32 rows and Import to sentinel target; compare/reload
        expected: Completed/32, exact 33 rows
        """
        _, uri, rows = self._source(batch)
        target = self._target(batch)
        batch.import_ok(target, uri, 32, label="SI-ER-01/control")
        batch.reload_assert(target, plain_rows([-1]) + rows)

    @pytest.mark.tags(CaseLabel.L2)
    def test_batch_er02_valid_mapping_control(self, batch):
        """
        target: SI-ER-02, complete mapping including empty partitions remains valid
        method: 1. Import A/B/C/Empty source using complete mapping 2. verify exact partition PKs/reload
        expected: 32 source rows plus sentinel, X/Y contain their mapped source rows
        """
        _, uri, rows = self._source(batch, partitions=["A", "B", "C", "Empty"])
        target = self._target(batch, partitions=["X", "Y", "Z"])
        mapping = {"_default": "_default", "A": "X", "B": "Y", "C": "Z", "Empty": "Z"}
        batch.import_ok(
            target, uri, 32, {**BASE_OPTIONS, "partition_mapping": json.dumps(mapping)}, label="SI-ER-02/control"
        )
        batch.reload_assert(target, plain_rows([-1]) + rows)
        for name, expected in (("X", set(range(16))), ("Y", set(range(16, 32))), ("Z", set())):
            actual = batch.client.query(target, filter="", partition_names=[name], output_fields=["id"], limit=33)
            assert {row["id"] for row in actual} == expected and len(actual) == len(expected)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("variant", [*OPTION_ERRORS, *FILE_ERRORS])
    def test_batch_er01_invalid_request(self, batch, variant):
        """
        target: SI-ER-01, invalid options/file cardinality/source URI cannot silently import
        method: change one known-good request field; require rejection/Failed; check sentinel/schema/reload
        expected: nonempty failure reason and unchanged target
        """
        source, uri, rows = self._source(batch)
        target = self._target(batch)
        before = batch.client.describe_collection(target)
        options, files = deepcopy(BASE_OPTIONS), None
        if variant in OPTION_ERRORS:
            key, value = OPTION_ERRORS[variant]
            if value is None:
                options.pop(key)
            else:
                options[key] = value
            if variant == "reverse_range":
                options["start_ts"] = str(1700000001000 << 18)
                options["end_ts"] = str(1700000000000 << 18)
        else:
            metadata = batch.metadata(uri)
            files = {
                "empty_files": [],
                "two_entries": [[uri], [uri]],
                "two_uris": [[uri, uri]],
                "empty_uri": [[""]],
                "bare_key": [[urlsplit(uri).path.split("/" + batch.bucket + "/", 1)[1]]],
                "segment_manifest": [
                    [urlsplit(uri)._replace(path=f"/{batch.bucket}/{metadata['manifest_list'][0]}").geturl()]
                ],
            }[variant]
        reason = (
            ["import request is empty"]
            if variant == "empty_files"
            else ["source", "snapshot", "uri", "path", "file"]
            if variant in FILE_ERRORS
            else ["timestamp", "start", "end", "time"]
            if variant in ("bad_timestamp", "reverse_range")
            else [OPTION_ERRORS[variant][0], "reserved", "backup", "source_type"]
        )
        batch.import_rejected(target, uri, options=options, files=files, label="SI-ER-01/" + variant, reason=reason)
        self._unchanged(batch, target, before)
        batch.assert_rows(source, rows)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("variant", MAPPING_ERRORS)
    def test_batch_er02_invalid_mapping(self, batch, variant):
        """
        target: SI-ER-02, reject incomplete/malformed/conflicting routing without partial data
        method: capture A/B/C/Empty source; alter one mapping condition; check target schema/partitions/sentinel
        expected: rejection/Failed; no auto-created collection or partition
        """
        source, uri, rows = self._source(batch, partitions=["A", "B", "C", "Empty"])
        target = self._target(batch, partitions=["X", "Y", "Z"])
        before = batch.client.describe_collection(target)
        partitions = sorted(batch.client.list_partitions(target))
        mapping = {"_default": "_default", "A": "X", "B": "Y", "C": "Z", "Empty": "Z"}
        partition = None
        if variant == "missing_A":
            mapping.pop("A")
        elif variant == "missing_empty":
            mapping.pop("Empty")
        elif variant == "unknown_source":
            mapping["Unknown"] = mapping.pop("A")
        elif variant == "unknown_target":
            mapping["A"] = "Unknown"
        elif variant == "empty_source":
            mapping[""] = "X"
        elif variant == "empty_target":
            mapping["A"] = ""
        elif variant == "non_string":
            mapping["A"] = 1
        elif variant == "partition_conflict":
            partition = "X"
        elif variant == "missing_collection":
            missing = target + "_absent"
            batch.collections.append(missing)
            batch.import_rejected(
                missing,
                uri,
                options={**BASE_OPTIONS, "partition_mapping": json.dumps(mapping)},
                label="SI-ER-02/" + variant,
                reason=["collection", "not exist", "not found"],
            )
            assert not batch.client.has_collection(missing)
            self._unchanged(batch, target, before)
            batch.assert_rows(source, rows)
            return
        elif variant == "partition_key":
            # Existing target uses a real partition key; Import cannot accept explicit mapping.
            schema = batch.client.create_schema(auto_id=False, enable_dynamic_field=False)
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field("vec", DataType.FLOAT_VECTOR, dim=16)
            schema.add_field("marker", DataType.INT64, is_partition_key=True)
            keyed = target + "_keyed"
            batch.collections.append(keyed)
            index = batch.client.prepare_index_params()
            index.add_index("vec", index_type="FLAT", metric_type="L2")
            batch.client.create_collection(
                keyed, schema=schema, index_params=index, num_partitions=2, consistency_level="Strong"
            )
            batch.client.insert(keyed, plain_rows([-1]))
            mapping = {name: batch.client.list_partitions(keyed)[0] for name in mapping}
            batch.import_rejected(
                keyed,
                uri,
                options={**BASE_OPTIONS, "partition_mapping": json.dumps(mapping)},
                label="SI-ER-02/" + variant,
                reason=["partition key", "partition-key", "partition_key"],
            )
            batch.reload_assert(keyed, plain_rows([-1]))
            self._unchanged(batch, target, before)
            batch.assert_rows(source, rows)
            return
        raw = json.dumps(mapping, ensure_ascii=False)
        if variant == "invalid_json":
            raw = "{not json"
        elif variant == "duplicate_key":
            raw = raw[:-1] + ',"A":"Y"}'
        elif variant == "oversized_utf8":
            raw = " " * 65537 + raw
            assert len(raw.encode()) > 65536
        batch.import_rejected(
            target,
            uri,
            options={**BASE_OPTIONS, "partition_mapping": raw},
            partition=partition,
            label="SI-ER-02/" + variant,
            reason=["64", "KiB", "size"]
            if variant == "oversized_utf8"
            else ["duplicate"]
            if variant == "duplicate_key"
            else ["source partition Unknown does not exist"]
            if variant == "unknown_source"
            else ["partition", "mapping", "JSON"],
        )
        self._unchanged(batch, target, before)
        assert sorted(batch.client.list_partitions(target)) == partitions
        batch.assert_rows(source, rows)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "variant", ["unknown_layout", "invalid_format", "missing_schema", "no_data", "empty_snapshot", "l0_only"]
    )
    def test_batch_er03_unsupported_source(self, batch, variant):
        """
        target: SI-ER-03, invalid metadata and empty snapshot differ from deleted real data
        method: copy only private metadata or snapshot real empty source; require failure; preserve sentinel
        expected: unsupported metadata/input rejected, target unchanged
        """
        source, uri, _ = self._source(batch)
        if variant == "empty_snapshot":
            empty = batch.create("empty_source")
            uri = batch.snapshot(empty)
        else:
            if variant == "l0_only":
                batch.client.delete(source, ids=list(range(8)))
                batch.client.flush(source)
            uri = batch.snapshot(source, "self_contained")

            def mutate(metadata):
                if variant == "unknown_layout":
                    metadata["layout"] = 99
                elif variant == "invalid_format":
                    metadata["format_version"] = -1
                elif variant == "missing_schema":
                    metadata["collection"].pop("schema", None)
                elif variant == "l0_only":
                    key = urlsplit(uri).path.split("/" + batch.bucket + "/", 1)[1]
                    keep = []
                    ids = []
                    for descriptor in metadata["manifest_list"]:
                        path = _snapshot_read_path(descriptor, metadata, key, batch.bucket, "self_contained")
                        segment = _segment_header(
                            object_bytes(batch.store, batch.bucket, path), int(metadata["format_version"])
                        )
                        if segment["segment_level"] == 1:
                            keep.append(descriptor)
                            ids.append(str(segment["segment_id"]))
                    assert keep, "Real L0-only input unavailable"
                    metadata["manifest_list"], metadata["segment_ids"] = keep, ids
                    metadata["storagev2_manifest_list"] = []
                else:
                    metadata["manifest_list"] = []
                    metadata["segment_ids"] = []
                    metadata["storagev2_manifest_list"] = []

            metadata = batch.metadata(uri)
            mutate(metadata)
            key = urlsplit(uri).path.split("/" + batch.bucket + "/", 1)[1]
            batch.put(key, metadata)
        target = self._target(batch)
        before = batch.client.describe_collection(target)
        batch.import_rejected(
            target,
            uri,
            label="SI-ER-03/" + variant,
            reason=["layout"]
            if variant == "unknown_layout"
            else ["version", "format"]
            if variant == "invalid_format"
            else ["schema"]
            if variant == "missing_schema"
            else ["segment", "empty", "data", "manifest"],
        )
        self._unchanged(batch, target, before)
        batch.assert_rows(source, plain_rows(range(8, 32) if variant == "l0_only" else range(32)))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "variant",
        [
            "missing_metadata",
            "corrupt_metadata",
            "missing_descriptor",
            "corrupt_descriptor",
            "missing_data",
            "corrupt_data",
            "escape_descriptor",
            "foreign_descriptor",
            "missing_physical_manifest",
            "corrupt_physical_manifest",
            "missing_l0",
            "corrupt_l0",
            "missing_lob",
            "corrupt_lob",
            "unprojected_lob",
        ],
    )
    def test_batch_er04_static_object_fault(self, batch, variant):
        """
        target: SI-ER-04, missing/corrupt private bundle objects never become empty success
        method: export self-contained copy; damage one required object before Import; check unchanged sentinel
        expected: rejection/Failed; no writes outside private bundle or source mutation
        """
        extra = [("text", DataType.TEXT, {})] if "lob" in variant else []
        source = batch.create("source", extra, properties={"collection.autocompaction.enabled": "false"})
        rows = plain_rows(range(32))
        if extra:
            for row in rows:
                row["text"] = "长文本🙂" * 20000 if row["id"] == 1 else "small"
        batch.client.insert(source, deepcopy(rows))
        batch.client.flush(source)
        if "l0" in variant:
            batch.client.delete(source, ids=list(range(8)))
            batch.client.flush(source)
        uri = batch.snapshot(source, "self_contained")
        metadata = batch.metadata(uri)
        key = urlsplit(uri).path.split("/" + batch.bucket + "/", 1)[1]
        root = key.rsplit("/snapshots/", 1)[0]
        descriptor = _snapshot_read_path(metadata["manifest_list"][0], metadata, key, batch.bucket, "self_contained")
        assert descriptor.startswith(batch.prefix)
        if variant.endswith("physical_manifest"):
            base, version = next(iter(snapshot_manifest_versions(metadata).values()))
            base = _snapshot_read_path(base, metadata, key, batch.bucket, "self_contained")
            damaged = base + f"/_metadata/manifest-{version}.avro"
        elif variant.endswith("l0"):
            paths = []
            for item in metadata["manifest_list"]:
                path = _snapshot_read_path(item, metadata, key, batch.bucket, "self_contained")
                segment = _segment_header(
                    object_bytes(batch.store, batch.bucket, path), int(metadata["format_version"]), include_logs=True
                )
                if segment["segment_level"] == 1:
                    for field in segment["deltalog_files"]:
                        for entry in field["binlogs"]:
                            paths.append(
                                _snapshot_read_path(entry["log_path"], metadata, key, batch.bucket, "self_contained")
                            )
            assert paths, "Actual L0 dependency unavailable"
            damaged = paths[0]
        elif "lob" in variant:
            base, version = next(iter(snapshot_manifest_versions(metadata).values()))
            base = _snapshot_read_path(base, metadata, key, batch.bucket, "self_contained")
            manifest = avro_manifest(batch.store, batch.bucket, base, version)
            assert manifest.get("lob_files"), "Actual LOB dependency unavailable"
            path = manifest["lob_files"][0]["path"]
            lob_root = base.rsplit("/", 1)[0] + "/lobs/"
            damaged = path if path.startswith(lob_root) else lob_root + path
        elif variant.endswith("metadata"):
            damaged = key
        elif variant.endswith("descriptor") and variant not in ("escape_descriptor", "foreign_descriptor"):
            damaged = descriptor
        elif variant.endswith("data"):
            base, version = next(iter(snapshot_manifest_versions(metadata).values()))
            base = _snapshot_read_path(base, metadata, key, batch.bucket, "self_contained")
            manifest = avro_manifest(batch.store, batch.bucket, base, version)
            files = [file for group in manifest["column_groups"] for file in group["files"]]
            assert files
            damaged = _data_key(base, files[0]["path"])
            assert batch.store.stat_object(batch.bucket, damaged).size > 0
        else:
            metadata["manifest_list"][0] = (
                "../escape.avro" if variant == "escape_descriptor" else "s3://foreign-bucket/foreign.avro"
            )
            batch.put(key, metadata)
            damaged = None
        if damaged:
            assert damaged.startswith(batch.prefix)
            assert batch.store.stat_object(batch.bucket, damaged).size > 0
            if variant.startswith("missing") or variant == "unprojected_lob":
                batch.store.remove_object(batch.bucket, damaged)
            else:
                batch.put(damaged, b"deliberately corrupt private test object")
        if variant == "unprojected_lob":
            target = self._target(batch)
            batch.import_ok(target, uri, 32, label="SI-ER-04/unprojected_LOB_control")
            batch.reload_assert(target, plain_rows([-1, *range(32)]))
            batch.assert_rows(source, rows)
            return
        if extra:
            target = batch.create("target", extra)
            sentinel = plain_rows([-1])
            sentinel[0]["text"] = "sentinel"
            batch.client.insert(target, sentinel)
        else:
            target = self._target(batch)
            sentinel = plain_rows([-1])
        before = batch.client.describe_collection(target)
        batch.import_rejected(
            target,
            uri,
            label="SI-ER-04/" + variant,
            reason=["read", "decode", "manifest", "path", "file", "metadata", "EOF", "json", "exist", "invalid"],
        )
        batch.reload_assert(target, sentinel)
        assert batch.client.describe_collection(target) == before
        batch.assert_rows(source, [row for row in rows if "l0" not in variant or row["id"] >= 8])


# Self-constructed physical legacy fixtures with positive readability and timestamp proofs.


class TestSnapshotImportLegacyAssets(SnapshotBatchBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("kind", ["storage_v1", "storage_v2"])
    def test_batch_er03_real_source_assets(self, request, kind):
        """
        target: SI-ER-03, real unsupported source assets must fail without partial data
        method: 1. construct real V1/V2 files 2. require exact backup Import control 3. snapshot Import to sentinel target
        expected: physical files readable by ordinary backup Import; snapshot source rejected; sentinel unchanged
        """
        batch = request.getfixturevalue("batch")
        fixture = build_legacy_storage_fixture(batch, 1 if kind == "storage_v1" else 2)
        uri = fixture["uri"]
        metadata = batch.metadata(uri)
        key = uri.split("/" + batch.bucket + "/", 1)[1]
        segments = [
            _segment_header(
                object_bytes(
                    batch.store,
                    batch.bucket,
                    _snapshot_read_path(path, metadata, key, batch.bucket, fixture.get("layout", "referenced")),
                ),
                int(metadata["format_version"]),
                include_logs=True,
            )
            for path in metadata["manifest_list"]
        ]
        expected = 0 if kind == "storage_v1" else 2
        assert any(segment["num_of_rows"] > 0 and segment["storage_version"] == expected for segment in segments)
        checksums = fixture.get("physical_file_checksums")
        assert isinstance(checksums, dict) and checksums, "Authentic data-file evidence required"
        dependencies = {
            _snapshot_read_path(entry["log_path"], metadata, key, batch.bucket, fixture.get("layout", "referenced"))
            for segment in segments
            if segment["num_of_rows"] > 0
            for field in segment.get("binlog_files", [])
            for entry in field["binlogs"]
        }
        assert dependencies and dependencies <= set(checksums), "Actual referenced data checksums missing"
        for path in dependencies:
            data = object_bytes(batch.store, batch.bucket, path)
            assert hashlib.sha256(data).hexdigest() == checksums[path]
            assert data[:4] == (b"\xbc\xfa\xff\x00" if expected == 0 else b"PAR1"), "Not actual legacy data encoding"
        target = batch.create("target")
        batch.client.insert(target, plain_rows([-1]))
        batch.import_rejected(
            target,
            uri,
            label="SI-ER-03/real_" + kind,
            reason=["snapshot-source import only supports StorageV3 segments"],
        )
        batch.reload_assert(target, plain_rows([-1]))


# Snapshot time windows, mixed-schema input and post-Import DML/compaction.


class TestSnapshotImportMutations(SnapshotBatchBase):
    @pytest.mark.tags(CaseLabel.L2)
    def test_batch_fn24_time_windows(self, batch, tmp_path):
        """
        target: SI-FN-24, inclusive HybridTS filtering applies to both rows and deletes
        method: 1. capture five mutation timestamps 2. prove raw input 3. Import seven windows 4. reload
        expected: 80/88/56/64/32/32/0 source rows plus sentinel; out-of-window deletes have no effect
        """
        source = batch.create("source", properties={"collection.autocompaction.enabled": "false"})
        connection = batch.client._get_connection()
        context = batch.client._generate_call_context()
        timestamps = []
        for action, values in (
            ("insert", range(32)),
            ("insert", range(32, 64)),
            ("delete", range(8)),
            ("insert", range(64, 96)),
            ("delete", [*range(8), *range(32, 40)]),
        ):
            mutation = (
                connection.insert_rows(source, plain_rows(values), context=context, timeout=30)
                if action == "insert"
                else connection.delete(source, f"id in {list(values)}", context=context, timeout=30)
            )
            timestamps.append(mutation.timestamp)
            batch.client.flush(source)
        assert all(a < b for a, b in zip(timestamps, timestamps[1:]))
        uri = batch.snapshot(source)
        metadata, segments, _, _ = read_snapshot_inventory(batch.store, batch.bucket, uri)
        data = [s for s in segments if s["segment_level"] != 1 and s["num_of_rows"] > 0]
        assert sum(s["num_of_rows"] for s in data) == 96, "Missing raw history: fixture blocked"
        manifests = snapshot_manifest_versions(metadata)
        fields = {f["name"]: f["field_id"] for f in batch.client.describe_collection(source)["fields"]}
        physical = []
        for segment in data:
            base, version = manifests[segment["segment_id"]]
            physical.extend(
                read_segment_rows(
                    batch.store,
                    batch.bucket,
                    json.dumps({"base_path": base, "ver": version}),
                    {"id": fields["id"], "vector": fields["vec"], "timestamp": 1},
                    segment["num_of_rows"],
                    16,
                    tmp_path,
                )
            )
        assert len(physical) == 96
        for row in physical:
            assert row["timestamp"] == timestamps[0 if row["id"] < 32 else 1 if row["id"] < 64 else 3]
        deletes = []
        metadata_key = uri.split("/" + batch.bucket + "/", 1)[1]
        for segment in segments:
            if segment["segment_level"] == 1:
                for field in segment["deltalog_files"]:
                    for entry in field["binlogs"]:
                        key = _snapshot_read_path(entry["log_path"], metadata, metadata_key, batch.bucket, "referenced")
                        deletes.extend(_legacy_delete_records(object_bytes(batch.store, batch.bucket, key)))
        assert {(pk, ts) for pk, ts in deletes} >= {
            *((pk, timestamps[2]) for pk in range(8)),
            *((pk, timestamps[4]) for pk in [*range(8), *range(32, 40)]),
        }, "Missing duplicate-delete history: fixture blocked"
        batch.record("time_fixture", timestamps=timestamps, raw_rows=len(physical), deletes=deletes)
        windows = [
            ("all", None, None, [*range(8, 32), *range(40, 96)]),
            ("T1-T4", timestamps[0], timestamps[3], range(8, 96)),
            ("T1-T3", timestamps[0], timestamps[2], range(8, 64)),
            ("T2-T4", timestamps[1], timestamps[3], range(32, 96)),
            ("T2plus1-T4", timestamps[1] + 1, timestamps[3], range(64, 96)),
            ("T2-T2", timestamps[1], timestamps[1], range(32, 64)),
            ("T3-T3", timestamps[2], timestamps[2], []),
        ]
        for label, start, end, pks in windows:
            target = batch.create("target_" + label.replace("-", "_"))
            batch.client.insert(target, plain_rows([-1]))
            options = {"backup": "true", "source_type": "snapshot", "auto_commit": "true"}
            if start is not None:
                options.update(start_ts=str(start), end_ts=str(end))
            batch.import_ok(target, uri, len(pks), options, label="SI-FN-24/" + label)
            batch.reload_assert(target, plain_rows([-1, *pks]))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default", [None, 7], ids=["nullable", "default7"])
    def test_batch_fn25_mixed_schema_segments(self, batch, default, tmp_path):
        """
        target: SI-FN-25, each input manifest independently projects missing/new columns
        method: 1. flush old rows 2. add score and write NULL/0/value 3. inspect manifests 4. Import/reload
        expected: 96 rows; missing and NULL score fill target default; explicit 0/value retained
        """
        source = batch.create("source", properties={"collection.autocompaction.enabled": "false"})
        rows = plain_rows(range(96))
        batch.client.insert(source, deepcopy(rows[:32]))
        batch.client.flush(source)
        batch.client.add_collection_field(source, "score", DataType.INT64, nullable=True)
        for row in rows[32:]:
            row["score"] = None if row["id"] < 64 and row["id"] % 2 == 0 else 0 if row["id"] < 64 else 1000 + row["id"]
        batch.client.insert(source, deepcopy(rows[32:64]))
        batch.client.flush(source)
        batch.client.insert(source, deepcopy(rows[64:]))
        batch.client.flush(source)
        uri = batch.snapshot(source)
        metadata, segments, _, deleted = read_snapshot_inventory(batch.store, batch.bucket, uri)
        assert not deleted
        score_id = next(
            f["field_id"] for f in batch.client.describe_collection(source)["fields"] if f["name"] == "score"
        )
        present = []
        physical_ids = {f["name"]: f["field_id"] for f in batch.client.describe_collection(source)["fields"]}
        observed = set()
        segment_rows = {s["segment_id"]: s["num_of_rows"] for s in segments}
        for segment_id, (base, version) in snapshot_manifest_versions(metadata).items():
            manifest = avro_manifest(batch.store, batch.bucket, base, version)
            columns = {column for group in manifest["column_groups"] for column in group["columns"]}
            present.append(str(score_id) in columns)
            selected = {"id": physical_ids["id"], "vector": physical_ids["vec"]}
            if present[-1]:
                selected["score"] = score_id
            physical = read_segment_rows(
                batch.store,
                batch.bucket,
                json.dumps({"base_path": base, "ver": version}),
                selected,
                segment_rows[segment_id],
                16,
                tmp_path,
            )
            for row in physical:
                pk = row["id"]
                assert pk not in observed
                observed.add(pk)
                assert present[-1] == (pk >= 32), (pk, present[-1])
                if pk >= 32:
                    assert row["score"] == rows[pk]["score"], row
        assert any(present) and not all(present), "No physically mixed schema: fixture blocked"
        assert observed == set(range(96))
        assert sum(s["num_of_rows"] for s in segments if s["segment_level"] != 1) == 96
        batch.record("mixed_schema_fixture", score_field_id=score_id, manifests_have_score=present)
        options = {"nullable": True}
        if default is not None:
            options["default_value"] = default
        target = batch.create("target", [("score", DataType.INT64, options)])
        assert (
            batch.client.describe_collection(source)["fields"][-1]["field_id"]
            == batch.client.describe_collection(target)["fields"][-1]["field_id"]
        )
        expected = [{**row, "score": default if row.get("score") is None else row["score"]} for row in rows]
        batch.import_ok(target, uri, 96, label="SI-FN-25")
        batch.reload_assert(target, expected)

    @pytest.mark.tags(CaseLabel.L2)
    def test_batch_fn26_mutations_and_compaction(self, batch):
        """
        target: SI-FN-26, imported rows remain mutable through real compaction
        method: 1. Import 128+sentinel 2. delete/upsert/insert 3. require compaction input IDs 4. reload
        expected: 129 exact rows; deleted absent, updates/new rows preserved; plans include Import segment
        """
        source = batch.create("source")
        target = batch.create("target", properties={"collection.autocompaction.enabled": "false"})
        batch.client.insert(source, plain_rows(range(128)))
        batch.client.flush(source)
        batch.client.insert(target, plain_rows([-1]))
        uri = batch.snapshot(source)
        batch.import_ok(target, uri, 128, label="SI-FN-26")
        batch.assert_rows(target, plain_rows([-1, *range(128)]))
        captured = batch.snapshot(target, protect=0)
        metadata, segments, _, _ = read_snapshot_inventory(batch.store, batch.bucket, captured)
        imported_ids = {s["segment_id"] for s in segments if s["segment_level"] != 1 and s["num_of_rows"] == 128}
        assert imported_ids, "Missing exact imported input segment"
        snapshot_source, snapshot_name = batch.snapshots.pop()
        batch.client.drop_snapshot(snapshot_name, snapshot_source, db_name=batch.db)
        batch.client.delete(target, ids=list(range(16)))
        updates = plain_rows(range(16, 32), marker_offset=10000)
        for row in updates:
            row["vec"] = [2 + value for value in row["vec"]]
        batch.client.upsert(target, deepcopy(updates))
        batch.client.insert(target, plain_rows(range(128, 144)))
        batch.client.flush(target)
        expected = plain_rows([-1]) + updates + plain_rows(range(32, 144))
        batch.assert_rows(target, expected)
        compact_id = batch.client.compact(target, timeout=30)
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            state = batch.client.get_compaction_state(compact_id, timeout=min(30, max(1, deadline - time.monotonic())))
            if "completed" in str(state).lower():
                break
            time.sleep(2)
        else:
            pytest.fail("Compaction did not complete within 600s")
        plans = batch.client.get_compaction_plans(compact_id, timeout=30)
        sources = {segment for plan in plans.plans for segment in plan.sources}
        assert sources & imported_ids, ("Compaction did not process imported rows", sources, imported_ids)
        batch.record(
            "compaction", compact_id=compact_id, imported_ids=sorted(imported_ids), processed_ids=sorted(sources)
        )
        batch.reload_assert(target, expected)
        assert (
            batch.client.query(target, filter="id in [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]", output_fields=["id"])
            == []
        )
        hits = batch.client.search(
            target, [updates[0]["vec"]], anns_field="vec", filter="id == 16", limit=1, output_fields=["marker"]
        )[0]
        assert len(hits) == 1 and hits[0]["id"] == 16 and hits[0]["entity"]["marker"] == 10016
        assert abs(hits[0]["distance"]) < 1e-6
        batch.assert_rows(source, plain_rows(range(128)))


# External source identity rejection, secret handling and plaintext EZK behavior.


class TestSnapshotImportSecurity(SnapshotBatchBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "variant", ["no_spec", "bad_credentials", "deny_metadata", "deny_data", "deny_l0", "deny_lob"]
    )
    def test_batch_er05_external_identity(self, batch, variant, minio_host, minio_bucket, tmp_path):
        """
        target: SI-ER-05, request-scoped source identity must not fall back or ignore denied files
        method: export private bucket; change one permission/credential; require failure; restore control
        expected: failed target retains sentinel; restored fresh job imports exact survivors
        """
        cfg = get_minio_config(minio_host, minio_bucket)
        extra = [("text", DataType.TEXT, {})] if variant == "deny_lob" else []
        source = batch.create("source", extra, properties={"collection.autocompaction.enabled": "false"})
        rows = plain_rows(range(32))
        if extra:
            for row in rows:
                row["text"] = "small" if row["id"] else "长文本🙂" * 20000
        batch.client.insert(source, deepcopy(rows))
        batch.client.flush(source)
        if variant == "deny_l0":
            batch.client.delete(source, ids=list(range(8)))
            batch.client.flush(source)
        uri = batch.snapshot(source)
        with external_snapshot_bucket(cfg, tmp_path, lambda: bool(batch.jobs or batch.uncertain)) as external:
            spec = json.loads(external.read_spec)
            batch.secret_tokens.extend([spec["extfs"]["access_key_value"]])
            export_uri = foreign_export_uri(uri, external.bucket, external.prefix)
            batch.uncertain = True
            export = export_snapshot(
                batch.url,
                source,
                batch.snapshots[-1][1],
                export_uri,
                batch.db,
                cf.param_info.param_token,
                external_spec=external.write_spec,
            )
            exported = wait_for_snapshot_export(batch.url, export, batch.db, cf.param_info.param_token)
            batch.uncertain = False
            assert exported["state"] == "ExportSnapshotCompleted"
            foreign_uri = exported["snapshotMetadataURI"]
            keys = [
                obj.object_name
                for obj in external.store.list_objects(external.bucket, prefix=external.prefix, recursive=True)
            ]
            metadata_key = urlsplit(foreign_uri).path.split("/" + external.bucket + "/", 1)[1]
            original_policy = json.loads((tmp_path / "source-readonly-policy.json").read_text())
            policy_name = "fn16-ro-" + external.bucket.removeprefix("snapshot-import-fn16-")
            admin = MinioAdmin(
                cfg["address"],
                StaticProvider(cfg["access_key"], cfg["secret_key"]),
                secure=cfg["secure"],
                http_client=urllib3.PoolManager(timeout=urllib3.Timeout(connect=10, read=30), retries=False),
            )
            if variant.startswith("deny_"):
                if variant == "deny_metadata":
                    denied = [metadata_key]
                elif variant == "deny_data":
                    denied = [key for key in keys if "/_data/" in key]
                elif variant == "deny_l0":
                    denied = [key for key in keys if "/delta_log/" in key or "/deltalog/" in key]
                else:
                    denied = [key for key in keys if "/lobs/" in key]
                assert denied, "Denied object class missing from real exported fixture"
                policy = deepcopy(original_policy)
                policy["Statement"].append(
                    {
                        "Effect": "Deny",
                        "Action": ["s3:GetObject"],
                        "Resource": [f"arn:aws:s3:::{external.bucket}/{key}" for key in denied],
                    }
                )
                path = tmp_path / "deny-policy.json"
                path.write_text(json.dumps(policy))
                admin.policy_add(policy_name, str(path))
                batch.record("denied_objects", variant=variant, keys=denied)
            options = {"backup": "true", "source_type": "snapshot", "auto_commit": "true", "timeout": "45s"}
            if variant != "no_spec":
                if variant == "bad_credentials":
                    bad = "canary_" + uuid4().hex
                    batch.secret_tokens.append(bad)
                    spec["extfs"]["access_key_value"] = bad
                options["external_spec"] = json.dumps(spec)
            target = batch.create("failed_target", extra)
            sentinel = plain_rows([-1])
            if extra:
                sentinel[0]["text"] = "sentinel"
            batch.client.insert(target, sentinel)
            batch.import_rejected(
                target,
                foreign_uri,
                options=options,
                label="SI-ER-05/" + variant,
                reason=["source", "read", "access", "credential", "denied", "timeout", "deadline"],
            )
            batch.reload_assert(target, sentinel)
            # Restore the same owned identity, then prove a new job succeeds.
            admin.policy_add(policy_name, str(tmp_path / "source-readonly-policy.json"))
            control = batch.create("valid_target", extra)
            expected = [row for row in rows if variant != "deny_l0" or row["id"] >= 8]
            batch.import_ok(
                control,
                foreign_uri,
                len(expected),
                {**options, "external_spec": str(external.read_spec)},
                label="SI-ER-05/restored",
            )
            batch.reload_assert(control, expected)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    @pytest.mark.parametrize("ezk", ["absent", "valid_unused", "malformed_unused"])
    def test_batch_sec03_plaintext_ignores_ezk(self, batch, layout, ezk, tmp_path):
        """
        target: SI-SEC-03, explicitly plaintext TEXT/LOB source ignores unused EZK
        method: use cipher.enabled=false private DB; prove LOB; Import both layouts with three EZK variants
        expected: all six jobs Complete with exact TEXT and reload; no encryption success claim
        """
        db = batch.name + "_db"
        batch.client.create_database(db, properties={"cipher.enabled": "false"})
        batch.client.use_database(db)
        batch.db = db
        try:
            properties = batch.client.describe_database(db).get("properties", {})
            assert str(properties.get("cipher.enabled", "false")).lower() == "false"
            assert "cipher.ezID" not in properties
            extra = [("text", DataType.TEXT, {})]
            source, target = batch.create("source", extra), batch.create("target", extra)
            rows = plain_rows(range(32))
            for row in rows:
                row["text"] = (
                    "长文本🙂" * 20000 if row["id"] == 1 else "" if row["id"] == 0 else f"short中文-{row['id']}"
                )
            batch.client.insert(source, deepcopy(rows))
            batch.client.flush(source)
            referenced = batch.snapshot(source)
            metadata = batch.metadata(referenced)
            assert not any(
                item["key"] == "cipher.ezID" for item in metadata["collection"]["schema"].get("properties", [])
            )
            lob_keys = []
            for base, version in snapshot_manifest_versions(metadata).values():
                manifest = avro_manifest(batch.store, batch.bucket, base, version)
                for item in manifest.get("lob_files", []):
                    key = item["path"]
                    if not key.startswith(base.rsplit("/", 1)[0] + "/lobs/"):
                        key = base.rsplit("/", 1)[0] + "/lobs/" + key
                    assert batch.store.stat_object(batch.bucket, key).size > 0
                    lob_keys.append(key)
            assert lob_keys, "TEXT fixture did not capture a real LOB"
            batch.record("plaintext_lob", database=db, cipher_enabled=False, lob_keys=lob_keys)
            uri = referenced if layout == "referenced" else batch.snapshot(source, layout)
            options = {"backup": "true", "source_type": "snapshot", "auto_commit": "true"}
            if ezk != "absent":
                options["ezk"] = base64.b64encode(b'{"ez_id":1}').decode() if ezk == "valid_unused" else "not-base64"
            batch.import_ok(target, uri, 32, options, label="SI-SEC-03/" + layout + "/" + ezk)
            batch.reload_assert(target, rows)
        finally:
            if not batch.jobs and not batch.uncertain:
                batch._cleanup()
                batch.collections.clear()
                batch.snapshots.clear()
                batch.client.use_database("default")
                batch.client.drop_database(db)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("layout", ["referenced", "self_contained"])
    @pytest.mark.parametrize("origin", ["ordinary", "imported"])
    def test_batch_cp02_legacy_descriptor_without_commit(self, batch, layout, origin):
        """
        target: SI-CP-02, genuine V2 descriptors omit commit timestamp and decode as zero
        method: 1. copy captured data 2. encode producer V2 descriptor 3. Import and reload
        expected: simple 32-row data retained; no commit field in fixture; real historical ambiguity unverified
        """
        rows = plain_rows(range(32))
        source = batch.create("source")
        batch.client.insert(source, rows)
        batch.client.flush(source)
        if origin == "imported":
            middle = batch.create("middle")
            batch.import_ok(middle, batch.snapshot(source), 32, label="SI-CP-02/prepare_imported")
            source = middle
        uri = batch.snapshot(source, layout)
        legacy, commits = copy_legacy_descriptors(batch, uri, layout)
        if origin == "imported":
            assert commits and all(value > 0 for value in commits)
        target = batch.create("target")
        batch.import_ok(target, legacy, 32, label="SI-CP-02/" + origin + "/" + layout)
        batch.reload_assert(target, rows)
        batch.record(
            "compatibility_limit",
            real_historical_fixture=False,
            ambiguous_delete_history="NotCovered",
            data_fidelity="KnownLimitation" if origin == "imported" else "simple_rows_verified",
        )


# SI-FN-20 ordinary vector Import, typed output and NULL-aware search.

VECTOR_TYPES = (
    DataType.FLOAT_VECTOR,
    DataType.FLOAT16_VECTOR,
    DataType.BFLOAT16_VECTOR,
    DataType.INT8_VECTOR,
    DataType.BINARY_VECTOR,
    DataType.SPARSE_FLOAT_VECTOR,
)


def vector_value(dtype, pk):
    if dtype == DataType.BINARY_VECTOR:
        return (
            bytes([0] * 16)
            if pk // 4 == 0
            else bytes([255] * 16)
            if pk // 4 == 1
            else bytes((pk * 13 + j * 17) % 256 for j in range(16))
        )
    if dtype == DataType.SPARSE_FLOAT_VECTOR:
        return {0: (pk + 1) / 32, pk + 1: (pk + 3) / 16, 100 + pk: 0.5}
    if dtype == DataType.INT8_VECTOR:
        return np.asarray(
            [[-128, -1, 0, 127][j % 4] if j < 4 else (pk * 7 + j) % 255 - 128 for j in range(16)], dtype=np.int8
        )
    values = [0.0] * 16 if pk == 1 else [(pk * 16 + j - 256) / 1003 for j in range(16)]
    if dtype == DataType.FLOAT16_VECTOR:
        return np.asarray(values, dtype=np.float16)
    if dtype == DataType.BFLOAT16_VECTOR:
        return np.asarray(values, dtype=ml_dtypes.bfloat16)
    return np.asarray(values, dtype=np.float32)


def decoded(dtype, value):
    if value is None:
        return None
    if dtype == DataType.SPARSE_FLOAT_VECTOR:
        return {int(key): float(weight) for key, weight in value.items()}
    if isinstance(value, (list, tuple)) and len(value) == 1 and isinstance(value[0], bytes):
        value = value[0]
    if dtype == DataType.BINARY_VECTOR:
        result = bytes(value)
        assert len(result) == 16
        return result
    precision = {
        DataType.FLOAT_VECTOR: np.float32,
        DataType.FLOAT16_VECTOR: np.float16,
        DataType.BFLOAT16_VECTOR: ml_dtypes.bfloat16,
        DataType.INT8_VECTOR: np.int8,
    }[dtype]
    if isinstance(value, bytes):
        result = np.frombuffer(value, dtype=precision).astype(np.float64)
    else:
        result = np.asarray(value).astype(np.float64)
    assert result.shape == (16,)
    return result


def distance(dtype, query, vector):
    if dtype == DataType.SPARSE_FLOAT_VECTOR:
        return sum(weight * vector.get(key, 0) for key, weight in query.items())
    if dtype == DataType.BINARY_VECTOR:
        return sum((a ^ b).bit_count() for a, b in zip(query, vector, strict=True))
    return float(np.sum((query - vector) ** 2))


class TestSnapshotImportVectors(SnapshotBatchBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", VECTOR_TYPES, ids=lambda dtype: dtype.name)
    def test_batch_fn20_vector_values(self, batch, dtype):
        """
        target: SI-FN-20, preserve six ordinary vector types and nullable vectors
        method: 1. insert encoded truth 2. Import 32 rows 3. verify values/search 4. reload and repeat
        expected: exact PK/value/NULL associations and valid distances for both vector fields
        """
        source = batch.create(
            "source",
            [
                (
                    "required",
                    dtype,
                    {}
                    if dtype == DataType.SPARSE_FLOAT_VECTOR
                    else {"dim": 128 if dtype == DataType.BINARY_VECTOR else 16},
                ),
                (
                    "optional",
                    dtype,
                    {
                        "nullable": True,
                        **(
                            {}
                            if dtype == DataType.SPARSE_FLOAT_VECTOR
                            else {"dim": 128 if dtype == DataType.BINARY_VECTOR else 16}
                        ),
                    },
                ),
            ],
        )
        target = batch.create(
            "target",
            [
                (
                    "required",
                    dtype,
                    {}
                    if dtype == DataType.SPARSE_FLOAT_VECTOR
                    else {"dim": 128 if dtype == DataType.BINARY_VECTOR else 16},
                ),
                (
                    "optional",
                    dtype,
                    {
                        "nullable": True,
                        **(
                            {}
                            if dtype == DataType.SPARSE_FLOAT_VECTOR
                            else {"dim": 128 if dtype == DataType.BINARY_VECTOR else 16}
                        ),
                    },
                ),
            ],
        )
        assert batch.client.describe_collection(source)["fields"] == batch.client.describe_collection(target)["fields"]
        rows = plain_rows(range(32))
        for row in rows:
            row["required"] = vector_value(dtype, row["id"])
            row["optional"] = None if row["id"] % 4 == 0 else vector_value(dtype, row["id"])
        truth = {row["id"]: deepcopy(row) for row in rows}
        batch.client.insert(source, deepcopy(rows))
        batch.client.flush(source)
        for name in (source, target):
            indexes = batch.client.prepare_index_params()
            for field in ("required", "optional"):
                index, metric, params = (
                    ("BIN_FLAT", "HAMMING", {})
                    if dtype == DataType.BINARY_VECTOR
                    else (
                        ("SPARSE_INVERTED_INDEX", "IP", {})
                        if dtype == DataType.SPARSE_FLOAT_VECTOR
                        else (
                            ("HNSW", "L2", {"M": 8, "efConstruction": 64})
                            if dtype == DataType.INT8_VECTOR
                            else ("FLAT", "L2", {})
                        )
                    )
                )
                indexes.add_index(field, index_name=field + "_idx", index_type=index, metric_type=metric, params=params)
            batch.client.create_index(name, indexes)
        self._assert_vectors(batch, source, dtype, truth)
        uri = batch.snapshot(source)
        batch.import_ok(target, uri, 32, label="SI-FN-20/" + dtype.name)
        for reload in (False, True):
            if reload:
                batch.client.release_collection(target)
            self._assert_vectors(batch, target, dtype, truth)

    @staticmethod
    def _assert_vectors(batch, name, dtype, truth):
        batch.assert_rows(name, list(truth.values()), fields=["id", "marker", "vec"])
        for field in ("required", "optional"):
            actual = batch.client.query(name, filter="", output_fields=["id", field], limit=33)
            assert len(actual) == 32 and {row["id"] for row in actual} == set(truth)
            for row in actual:
                assert set(row) == {"id", field}
                expected, value = decoded(dtype, truth[row["id"]][field]), decoded(dtype, row[field])
                if expected is None:
                    assert value is None
                elif dtype in (DataType.BINARY_VECTOR, DataType.SPARSE_FLOAT_VECTOR):
                    assert value == expected, (row["id"], field)
                else:
                    np.testing.assert_array_equal(value, expected)
            valid = {pk for pk, row in truth.items() if row[field] is not None}
            for pk in (1, 15, 31):
                hits = batch.client.search(
                    name,
                    [truth[pk][field]],
                    anns_field=field,
                    limit=5,
                    search_params={
                        "metric_type": "HAMMING"
                        if dtype == DataType.BINARY_VECTOR
                        else "IP"
                        if dtype == DataType.SPARSE_FLOAT_VECTOR
                        else "L2",
                        "params": {"ef": 64} if dtype == DataType.INT8_VECTOR else {},
                    },
                    output_fields=["marker"],
                )[0]
                assert len(hits) == 5 and len({hit["id"] for hit in hits}) == 5
                assert {hit["id"] for hit in hits} <= valid
                oracle = {
                    candidate: distance(
                        dtype, decoded(dtype, truth[pk][field]), decoded(dtype, truth[candidate][field])
                    )
                    for candidate in valid
                }
                ordered = sorted(oracle.values(), reverse=dtype == DataType.SPARSE_FLOAT_VECTOR)
                cutoff = ordered[4]
                tolerance = 1e-5 * max(1, abs(cutoff))
                better = (
                    {candidate for candidate, score in oracle.items() if score > cutoff + tolerance}
                    if dtype == DataType.SPARSE_FLOAT_VECTOR
                    else {candidate for candidate, score in oracle.items() if score < cutoff - tolerance}
                )
                assert better <= {hit["id"] for hit in hits}
                assert all(
                    oracle[hit["id"]] >= cutoff - tolerance
                    if dtype == DataType.SPARSE_FLOAT_VECTOR
                    else oracle[hit["id"]] <= cutoff + tolerance
                    for hit in hits
                )
                scores = []
                for hit in hits:
                    assert hit["entity"]["marker"] == truth[hit["id"]]["marker"]
                    expected = distance(
                        dtype, decoded(dtype, truth[pk][field]), decoded(dtype, truth[hit["id"]][field])
                    )
                    assert np.isclose(hit["distance"], expected, rtol=1e-5, atol=1e-5), (hit, expected)
                    scores.append(hit["distance"])
                assert scores == sorted(scores, reverse=dtype == DataType.SPARSE_FLOAT_VECTOR)
