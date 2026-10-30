"""Offline snapshot inspector regression checks; no Milvus/MinIO/etcd access."""

import io
import json
import socket
from copy import deepcopy
from unittest.mock import Mock

import pytest
from common import common_func as cf
from common import snapshot_import_common as snapshot
from fastavro import writer
from milvus_client.test_milvus_client_snapshot_import import (
    TestMilvusClientSnapshotImportIndependent as SnapshotImportCases,
)


def test_vortex_dimension_rejection_matches_ci_error_without_accepting_other_failures():
    check = SnapshotImportCases._assert_schema_import_error
    check("Cannot convert FixedSizeList with list size 128 to Arrow array with list size 64", "vector_dim")
    check("fixed_size_binary expected 64, got 128: mismatch", "vector_dim")
    for reason in [
        "request timeout",
        "Cannot convert FixedSizeList with list size 12 to Arrow array with list size 8",
        "Cannot convert FixedSizeList with list size 1280 to Arrow array with list size 640",
        "Registry missing encoding",
    ]:
        with pytest.raises(AssertionError):
            check(reason, "vector_dim")


def test_vortex_scalar_rejection_matches_ci_type_without_accepting_unrelated_failures():
    check = SnapshotImportCases._assert_schema_import_error
    check("No CastKernel to cast canonical array vortex.varbinview from utf8 to i64?", "scalar_type")
    for reason in [
        "request timeout",
        "S3 access denied",
        "No CastKernel to cast canonical array vortex.varbinview from utf8 to i32?",
        "No CastKernel to cast canonical array vortex.varbinview from binary to i64?",
    ]:
        with pytest.raises(AssertionError):
            check(reason, "scalar_type")


@pytest.mark.parametrize(
    "fault", [None, "missing_field", "missing_object", "empty", "invalid_rows", "wrong_namespace", "duplicate"]
)
def test_lob_inventory_requires_real_registered_files_for_each_text_field(monkeypatch, fault):
    base = "files/insert_log/100/101/300"
    lob_root = "files/insert_log/100/101/lobs/"
    fields = {"text_a": {"field_id": 103}, "text_b": {"field_id": 104}}
    entries = [
        {"field_id": field_id, "path": f"{field_id}/_data/{field_id}.vx", "valid_rows": 1, "total_rows": 1}
        for field_id in (103, 104)
    ]
    if fault == "missing_field":
        entries.pop()
    elif fault == "invalid_rows":
        entries[0]["valid_rows"] = 0
    elif fault == "wrong_namespace":
        entries[0]["path"] = "999/_data/103.vx"
    elif fault == "duplicate":
        entries.append(entries[0])
    schema = {
        "type": "record",
        "name": "Manifest",
        "fields": [
            {
                "name": "lob_files",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "Lob",
                        "fields": [
                            {"name": name, "type": "string" if name == "path" else "long"}
                            for name in ("field_id", "path", "valid_rows", "total_rows")
                        ],
                    },
                },
            }
        ],
    }
    data = io.BytesIO()
    writer(data, schema, [{"lob_files": entries}])
    objects = {base + "/_metadata/manifest-1.avro": data.getvalue()}
    for field_id in (103, 104):
        objects[f"{lob_root}{field_id}/_data/{field_id}.vx"] = b"" if fault == "empty" else b"actual LOB data"
    if fault == "missing_object":
        objects.pop(f"{lob_root}103/_data/103.vx")
    store = MemoryStore(objects)
    monkeypatch.setattr(
        snapshot, "read_snapshot_partition_inventory", Mock(return_value=({"layout": 1}, [{"segment_id": 300}], {}, {}))
    )
    monkeypatch.setattr(snapshot, "snapshot_manifest_versions", Mock(return_value={300: (base, 1)}))
    if fault == "missing_object":
        with pytest.raises(KeyError):
            snapshot.inspect_snapshot_lob_inventory(store, "test-bucket", "uri", fields)
    elif fault:
        with pytest.raises(AssertionError):
            snapshot.inspect_snapshot_lob_inventory(store, "test-bucket", "uri", fields)
    else:
        assert snapshot.inspect_snapshot_lob_inventory(store, "test-bucket", "uri", fields) == {
            name: [lob_root + str(field["field_id"]) + "/_data/" + str(field["field_id"]) + ".vx"]
            for name, field in fields.items()
        }
        assert len(store.stats) == 2


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def reject(*args, **kwargs):
        raise AssertionError("Offline helper tests must not open network connections")

    monkeypatch.setattr(socket.socket, "connect", reject)
    monkeypatch.setattr(socket.socket, "connect_ex", reject)


class MemoryStore:
    def __init__(self, objects):
        self.objects = objects
        self.reads = []
        self.stats = []

    def stat_object(self, bucket, key):
        assert bucket == "test-bucket"
        self.stats.append(key)
        return Mock(size=len(self.objects[key]))

    def get_object(self, bucket, key):
        assert bucket == "test-bucket"
        self.reads.append(key)
        response = Mock()
        response.read.return_value = self.objects[key]
        return response


def bundle(layout="self_contained", old_root="export_a/", new_root="export_b/", location_uri=True):
    metadata_suffix = "snapshots/100/metadata/200.json"
    descriptor_suffix = "snapshots/100/manifests/200/300.avro"
    old_descriptor = old_root + descriptor_suffix
    actual_descriptor = (new_root if layout == "self_contained" else old_root) + descriptor_suffix
    metadata_key = new_root + metadata_suffix
    location = old_root + metadata_suffix
    if location_uri:
        location = "s3://minio:9000/test-bucket/" + location
    metadata = {
        "layout": 2 if layout == "self_contained" else 1,
        "snapshot_info": {"s3_location": location},
        "format_version": 6,
        "manifest_list": [old_descriptor],
        "segment_ids": [300],
        "storagev2_manifest_list": [
            {"segment_id": 300, "manifest": json.dumps({"base_path": old_root + "files/data", "ver": 1})}
        ],
    }
    segment = {
        "segment_id": 300,
        "partition_id": 101,
        "segment_level": 2,
        "channel_name": "source-channel",
        "num_of_rows": 128,
        "storage_version": 3,
        "commit_timestamp": 470023031874912273,
    }
    schema = {
        "type": "record",
        "name": "Segment",
        "fields": [{"name": k, "type": "string" if isinstance(v, str) else "long"} for k, v in segment.items()],
    }
    data = io.BytesIO()
    writer(data, schema, [segment])
    store = MemoryStore({metadata_key: json.dumps(metadata).encode(), actual_descriptor: data.getvalue()})
    uri = "s3://minio:9000/test-bucket/" + metadata_key
    return store, uri, metadata_key, old_descriptor, actual_descriptor


@pytest.mark.parametrize(
    "layout,old_root,new_root,location_uri",
    [
        ("referenced", "export_a/", "export_b/", True),
        ("self_contained", "export_a/", "export_a/", True),
        ("self_contained", "export_a/", "export_b/", True),
        ("self_contained", "export_a/", "export_b/", False),
        ("self_contained", "", "export_b/", True),
        ("self_contained", "export_a/", "", True),
        ("self_contained", "nested/snapshots/export_a/", "nested/snapshots/export_b/", True),
    ],
)
def test_commit_inspector_reads_actual_paths_without_changing_bytes(layout, old_root, new_root, location_uri):
    store, uri, metadata_key, _, descriptor = bundle(layout, old_root, new_root, location_uri)
    original = deepcopy(store.objects)

    timestamps, objects = snapshot.read_snapshot_commit_timestamps(store, "test-bucket", uri, layout)

    assert timestamps == {300: (128, 470023031874912273)}
    assert store.reads == [metadata_key, descriptor]
    assert objects == original == store.objects
    snapshot.assert_snapshot_objects_unchanged(store, "test-bucket", objects)
    assert store.reads == [metadata_key, descriptor] * 2

    store.objects[descriptor] += b"changed"
    with pytest.raises(AssertionError, match="Captured snapshot object changed"):
        snapshot.assert_snapshot_objects_unchanged(store, "test-bucket", objects)


def test_missing_moved_descriptor_does_not_fall_back_to_old_copy():
    store, uri, metadata_key, old_descriptor, descriptor = bundle()
    store.objects[old_descriptor] = store.objects.pop(descriptor)

    with pytest.raises(KeyError) as error:
        snapshot.read_snapshot_commit_timestamps(store, "test-bucket", uri, "self_contained")

    assert error.value.args == (descriptor,)
    assert store.reads == [metadata_key, descriptor]


def test_relocation_rejects_lookalike_root():
    store, uri, metadata_key, _, _ = bundle()
    metadata = json.loads(store.objects[metadata_key])
    metadata["manifest_list"] = ["export_ab/snapshots/100/manifests/200/300.avro"]
    store.objects[metadata_key] = json.dumps(metadata).encode()

    with pytest.raises(AssertionError, match="outside the original bundle root"):
        snapshot.read_snapshot_objects(store, "test-bucket", uri, "self_contained")
    assert store.reads == [metadata_key]


def test_relocation_rejects_missing_metadata_anchor():
    store, uri, metadata_key, _, _ = bundle()
    metadata = json.loads(store.objects[metadata_key])
    metadata["snapshot_info"]["s3_location"] = "export_a/not-a-snapshot.json"
    store.objects[metadata_key] = json.dumps(metadata).encode()

    with pytest.raises(AssertionError, match="metadata anchor"):
        snapshot.read_snapshot_objects(store, "test-bucket", uri, "self_contained")
    assert store.reads == [metadata_key]


def test_inventory_uses_relocated_l0_read_path(monkeypatch):
    store, uri, metadata_key, _, descriptor = bundle()
    old_delta = "export_a/snapshots/100/manifests/200/301.avro"
    new_delta = "export_b/snapshots/100/manifests/200/301.avro"
    new_delete = "export_b/files/deletes/301"
    metadata = json.loads(store.objects[metadata_key])
    metadata["manifest_list"].append(old_delta)
    metadata["segment_ids"].append(301)
    store.objects[metadata_key] = json.dumps(metadata).encode()
    store.objects[new_delta] = b"delta descriptor"
    store.objects[new_delete] = b"delete payload"
    original = deepcopy(store.objects)
    decode_segment = snapshot._segment_header

    def decode(data, version, include_logs=False):
        if data != b"delta descriptor":
            return decode_segment(data, version, include_logs)
        return {
            "segment_id": 301,
            "partition_id": 101,
            "segment_level": 1,
            "channel_name": "source-channel",
            "num_of_rows": 0,
            "storage_version": 0,
            "deltalog_files": [{"binlogs": [{"log_path": "export_a/files/deletes/301"}]}],
        }

    # Isolate path lookup; the legacy decoder itself is not under test here.
    decode_delete = Mock(return_value=[(7, 470023031874912274)])
    monkeypatch.setattr(snapshot, "_segment_header", decode)
    monkeypatch.setattr(snapshot, "_legacy_delete_records", decode_delete)
    returned, _, objects, deleted = snapshot.read_snapshot_inventory(store, "test-bucket", uri, "self_contained")

    assert returned == metadata
    assert deleted == {7}
    decode_delete.assert_called_once_with(b"delete payload", snapshot.DataType.INT64)
    assert store.reads == [metadata_key, descriptor, new_delta, new_delete]
    assert objects == original == store.objects
    snapshot.assert_snapshot_objects_unchanged(store, "test-bucket", objects)


@pytest.mark.parametrize(
    "override,uri,host,expected",
    [
        ("http://localhost:19092/", "http://proxy:19530", "ignored", "http://localhost:19092"),
        (None, "http://milvus-proxy:19530", "ignored", "http://milvus-proxy:9091"),
        (None, "", "milvus-proxy", "http://milvus-proxy:9091"),
        (None, "http://[::1]:19530", "ignored", "http://[::1]:9091"),
    ],
)
def test_ownership_config_uses_ci_proxy_without_extra_cli_flags(monkeypatch, override, uri, host, expected):
    if override:
        monkeypatch.setenv("MILVUS_MANAGEMENT_URI", override)
    else:
        monkeypatch.delenv("MILVUS_MANAGEMENT_URI", raising=False)
    params = Mock(param_uri=uri, param_host=host, param_port=19530)
    monkeypatch.setattr(cf, "param_info", params)
    response = Mock()
    response.json.return_value = {
        "common.storage.useloonffi": "true",
        "datanode.text.inlinethreshold": "32768",
        "indexcoord.segment.minsegmentnumrowstoenableindex": "1024",
    }
    get = Mock(return_value=response)
    monkeypatch.setattr(snapshot.requests, "get", get)

    assert snapshot.ownership_runtime_config() == {
        "text_inline_threshold": 32768,
        "index_min_segment_rows": 1024,
        "target_storage_version": 3,
    }
    get.assert_called_once_with(expected + "/api/v1/_cluster/configs", timeout=30)
    response.raise_for_status.assert_called_once_with()
