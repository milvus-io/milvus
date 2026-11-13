"""Offline assertions for fixture integrity, publication and independent GT."""

import hashlib
import json
import zipfile

import pytest
from common.paimon_fixture import FixtureBundle, PaimonOracle, PaimonSource, snapshot_expectation


def test_committed_fixture_fits_ci_source_budget_and_validates_all_snapshots():
    """
    target: TCUS source distribution budget and offline archive readability
    method: validate the real fixture package, including every blob and pinned oracle
    expected: <=26 MiB leaves 6 MiB of the 32 MiB CI limit for other test sources; all 28 versions retained
    """
    bundle = FixtureBundle()
    assert bundle.path.stat().st_size <= 26 * 1024**2, "fixture exceeds its share of the TCUS source bundle budget"
    assert len(bundle.fixtures) == 11
    assert sum(len(fixture["versions"]) for fixture in bundle.fixtures.values()) == 28


def make_bundle(path, *, source_path="schema/schema-0", corrupt=False):
    data = b'{"id":0,"fields":[]}'
    digest = hashlib.sha256(data).hexdigest()
    oracle = {
        "snapshot_id": 1,
        "reader": "Paimon Java 1.2.0 pinned batch reader",
        "rows": [
            {"id": 1, "label": "one", "embedding": [1.0, 0.0]},
        ],
    }
    encoded = json.dumps(oracle).encode()
    oracle_digest = hashlib.sha256(encoded).hexdigest()
    snapshot_data = b'{"id":1}'
    snapshot_digest = hashlib.sha256(snapshot_data).hexdigest()
    metadata = {
        "version": 1,
        "producer": "PaimonFixtures.java",
        "fixtures": {
            "append": {
                "dimension": 2,
                "fields": ["id", "label", "embedding"],
                "versions": {
                    "s1": {
                        "snapshot_id": 1,
                        "files": {source_path: digest, "snapshot/snapshot-1": snapshot_digest},
                        "oracle": oracle_digest,
                    }
                },
            }
        },
    }
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("manifest.json", json.dumps(metadata))
        archive.writestr(f"blobs/{digest}", b"wrong" if corrupt else data)
        archive.writestr(f"blobs/{oracle_digest}", encoded)
        archive.writestr(f"blobs/{snapshot_digest}", snapshot_data)
    return path


@pytest.mark.parametrize("value", [-1, "-1", None])
def test_snapshot_latest_sentinel(value):
    """
    target: snapshot latest sentinel
    method: exercise the documented boundary with positive and negative controls
    expected: only the specified result passes
    """
    assert snapshot_expectation(value, {1, 2}) == "latest"


@pytest.mark.parametrize("value", [-2, "-2", -(2**63), str(-(2**63))])
def test_negative_snapshot_rejected_at_property_boundary(value):
    """
    target: negative snapshot rejected at property boundary
    method: exercise the documented boundary with positive and negative controls
    expected: only the specified result passes
    """
    assert snapshot_expectation(value, {1}) == "invalid-property"


@pytest.mark.parametrize("value", [0, "0", 3, "3"])
def test_missing_nonnegative_snapshot_never_means_latest(value):
    """
    target: missing nonnegative snapshot never means latest
    method: exercise the documented boundary with positive and negative controls
    expected: only the specified result passes
    """
    assert snapshot_expectation(value, {1, 2}) == "missing"


@pytest.mark.parametrize("value", [True, 1.5, "1.5", [], {}, 2**63])
def test_malformed_snapshot_is_not_coerced(value):
    """
    target: malformed snapshot is not coerced
    method: exercise the documented boundary with positive and negative controls
    expected: only the specified result passes
    """
    with pytest.raises(ValueError):
        snapshot_expectation(value, {1})


@pytest.mark.parametrize("source_path", ["../escape", "/absolute", "a/../../b", "a//b", "a\\b"])
def test_package_cannot_escape_test_prefix(tmp_path, source_path):
    """
    target: package cannot escape test prefix
    method: exercise the documented boundary with positive and negative controls
    expected: only the specified result passes
    """
    with pytest.raises(ValueError, match="path"):
        FixtureBundle(make_bundle(tmp_path / "bad.zip", source_path=source_path))


def test_corruption_fails_before_any_upload(tmp_path):
    """
    target: corruption fails before any upload
    method: exercise the documented boundary with positive and negative controls
    expected: only the specified result passes
    """
    with pytest.raises(ValueError, match="digest"):
        FixtureBundle(make_bundle(tmp_path / "bad.zip", corrupt=True))


def test_oracle_detects_missing_and_duplicate_rows():
    """
    target: oracle detects missing and duplicate rows
    method: exercise the documented boundary with positive and negative controls
    expected: only the specified result passes
    """
    rows = [{"id": 1, "label": "one", "embedding": [1.0, 0.0]}, {"id": 2, "label": "two", "embedding": [0.0, 1.0]}]
    oracle = PaimonOracle(rows)
    oracle.assert_rows(list(reversed(rows)))
    for bad in [rows[:1], [rows[0], rows[0]]]:
        with pytest.raises(AssertionError):
            oracle.assert_rows(bad)


def test_oracle_detects_old_vector_even_with_same_row_count():
    """
    target: oracle detects old vector even with same row count
    method: exercise the documented boundary with positive and negative controls
    expected: only the specified result passes
    """
    oracle = PaimonOracle([{"id": 1, "embedding": [1.0, 0.0]}])
    with pytest.raises(AssertionError):
        oracle.assert_rows([{"id": 1, "embedding": [0.0, 1.0]}])


@pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
def test_exact_search_checks_distances_and_ids(metric):
    """
    target: metric GT
    method: hand-calculated ranking
    expected: distinct correct top2.
    """
    oracle = PaimonOracle(
        [{"id": 1, "embedding": [1.0, 0.0]}, {"id": 2, "embedding": [0.0, 3.0]}, {"id": 3, "embedding": [3.0, 3.0]}]
    )
    # Query [2,1]: L2 -> 1,3; IP -> 3,2; cosine -> 3,1.
    expected = {
        "L2": [(1, 2.0), (3, 5.0)],
        "IP": [(3, 9.0), (2, 3.0)],
        "COSINE": [(3, 3 / (10**0.5)), (1, 2 / (5**0.5))],
    }[metric]
    ids, scores = oracle.topk([2.0, 1.0], metric, 2)
    assert ids.tolist() == [row_id for row_id, _ in expected]
    assert scores.tolist() == pytest.approx([score for _, score in expected])
    hits = [
        {"id": 100 + row_id, "distance": score, "entity": dict(next(row for row in oracle.rows if row["id"] == row_id))}
        for row_id, score in expected
    ]
    oracle.assert_search([hits], [[2.0, 1.0]], metric, 2, output_fields=["id", "embedding"])
    for bad in [
        [dict(hits[0], distance=123), hits[1]],
        list(reversed(hits)),
        [hits[0], hits[0]],
        [dict(hits[0], entity={"id": expected[0][0]}), hits[1]],
    ]:
        with pytest.raises(AssertionError):
            oracle.assert_search([bad], [[2.0, 1.0]], metric, 2, output_fields=["id", "embedding"])


class MemoryMinio:
    def __init__(self):
        self.uploads = []
        self.objects = {}
        self.fail_key = None
        self.fail_after_key = None

    def put_object(self, bucket, key, stream, length, **kwargs):
        if key == self.fail_key:
            raise OSError("injected publication failure")
        value = stream.read()
        assert len(value) == length
        self.uploads.append((key, value))
        self.objects[key] = value
        if key == self.fail_after_key:
            raise OSError("remote PUT succeeded but response was lost")

    def remove_object(self, bucket, key):
        del self.objects[key]


def test_publish_validated_files_under_owned_prefix(tmp_path):
    """
    target: publish validated files under owned prefix
    method: exercise the documented boundary with positive and negative controls
    expected: only the specified result passes
    """
    bundle = FixtureBundle(make_bundle(tmp_path / "ok.zip"))
    store = MemoryMinio()
    source = PaimonSource(
        bundle,
        "append",
        store,
        {"bucket": "test", "address": "minio:9000", "secure": False, "access_key": "key", "secret_key": "secret"},
        "case-owned/table",
    )
    source.publish("s1")
    assert [key for key, _ in store.uploads] == [
        "case-owned/table/schema/schema-0",
        "case-owned/table/snapshot/snapshot-1",
    ]
    assert source.snapshot("s1") == 1
    assert source.source == "s3://minio:9000/test/case-owned/table"
    source.publish("s1")
    assert len(store.uploads) == 2, "no-op publication must not overwrite immutable files"


def test_publication_rejects_empty_or_absolute_prefix(tmp_path):
    """
    target: publication rejects empty or absolute prefix
    method: exercise the documented boundary with positive and negative controls
    expected: only the specified result passes
    """
    bundle = FixtureBundle(make_bundle(tmp_path / "ok.zip"))
    for prefix in ["", "/table", "../table", "bucket//table"]:
        with pytest.raises(ValueError, match="path"):
            PaimonSource(bundle, "append", MemoryMinio(), {"bucket": "test"}, prefix)


def staged_source(tmp_path):
    bundle = FixtureBundle(make_bundle(tmp_path / "staged.zip"))
    for number in [1, 2]:
        files = {}
        for key in [
            "schema/schema-0",
            "manifest/base",
            f"bucket-0/data-{number}.parquet",
            f"snapshot/snapshot-{number}",
            "snapshot/LATEST",
        ]:
            data = (str(number) if key.endswith("LATEST") else key).encode()
            digest = hashlib.sha256(data).hexdigest()
            bundle.blobs[digest] = data
            files[key] = digest
        if number == 2:
            files.update({k: v for k, v in bundle.metadata("append", "s1")["files"].items() if k not in files})
        bundle.fixtures["append"]["versions"][f"s{number}"] = {"snapshot_id": number, "files": files}
    return PaimonSource(bundle, "append", MemoryMinio(), {"bucket": "test"}, "owned/table")


@pytest.mark.parametrize("failure", ["bucket-0/data-2.parquet", "snapshot/snapshot-2", "snapshot/LATEST"])
def test_commit_order_partial_failure_retry_and_no_rollback(tmp_path, failure):
    """
    target: atomic publication
    method: fail each phase
    expected: safe retry, no rollback.
    """
    source = staged_source(tmp_path)
    source.publish("s1")
    source.client.fail_key = f"owned/table/{failure}"
    with pytest.raises(OSError):
        source.publish("s2")
    committed = "owned/table/snapshot/snapshot-2" in source.client.objects
    assert committed == (failure == "snapshot/LATEST")
    if committed:
        with pytest.raises(ValueError, match="earlier snapshot"):
            source.publish("s1")
    source.client.fail_key = None
    source.publish("s2")
    keys = [key.removeprefix("owned/table/") for key, _ in source.client.uploads]
    assert keys.index("bucket-0/data-2.parquet") < keys.index("snapshot/snapshot-2")
    assert keys.index("snapshot/snapshot-2") < len(keys) - 1
    assert keys[-1] == "snapshot/LATEST"
    assert keys.count("bucket-0/data-2.parquet") == 1
    assert source.version == "s2"


def test_immutable_overwrite_rejected_before_writes(tmp_path):
    """
    target: immutable files
    method: alter an existing digest
    expected: no writes.
    """
    source = staged_source(tmp_path)
    source.publish("s1")
    count = len(source.client.uploads)
    source.bundle.metadata("append", "s2")["files"]["schema/schema-0"] = hashlib.sha256(b"bad").hexdigest()
    with pytest.raises(ValueError, match="immutable"):
        source.publish("s2")
    assert len(source.client.uploads) == count


def test_deleted_owned_dependency_can_be_repaired(tmp_path):
    """
    target: repair after negative test
    method: delete and republish
    expected: restored.
    """
    source = staged_source(tmp_path)
    source.publish("s1")
    source.delete_owned("manifest/base")
    assert "owned/table/manifest/base" not in source.client.objects
    source.publish("s1")
    assert source.client.objects["owned/table/manifest/base"] == b"manifest/base"
    with pytest.raises(ValueError, match="does not belong"):
        source.delete_owned("not-owned")


def test_remote_snapshot_commit_with_lost_response_cannot_roll_back(tmp_path):
    """
    target: ambiguous snapshot PUT
    method: commit in the object store then raise a client transport error
    expected: old publication is refused and retrying the same new epoch succeeds
    """
    source = staged_source(tmp_path)
    source.publish("s1")
    source.client.fail_after_key = "owned/table/snapshot/snapshot-2"
    with pytest.raises(OSError, match="response was lost"):
        source.publish("s2")
    assert "owned/table/snapshot/snapshot-2" in source.client.objects
    with pytest.raises(ValueError, match="earlier snapshot"):
        source.publish("s1")
    source.client.fail_after_key = None
    source.publish("s2")
    assert source.version == "s2"
    assert source.client.objects["owned/table/snapshot/LATEST"] == b"2"


@pytest.mark.parametrize("mismatch", ["source-missing", "source-id", "oracle-missing", "oracle-id"])
def test_package_requires_source_manifest_oracle_snapshot_binding(tmp_path, mismatch):
    """
    target: snapshot provenance
    method: construct hash-valid packages with one missing or mismatched snapshot binding
    expected: package validation rejects every inconsistency before upload
    """
    path = make_bundle(tmp_path / "binding.zip")
    with zipfile.ZipFile(path) as archive:
        entries = {name: archive.read(name) for name in archive.namelist()}
    manifest = json.loads(entries["manifest.json"])
    metadata = manifest["fixtures"]["append"]["versions"]["s1"]
    if mismatch == "source-missing":
        del metadata["files"]["snapshot/snapshot-1"]
    else:
        if mismatch == "source-id":
            value = {"id": 2}
        else:
            value = json.loads(entries["blobs/" + metadata["oracle"]])
            if mismatch == "oracle-missing":
                del value["snapshot_id"]
            else:
                value["snapshot_id"] = 2
        data = json.dumps(value).encode()
        digest = hashlib.sha256(data).hexdigest()
        entries["blobs/" + digest] = data
        if mismatch == "source-id":
            metadata["files"]["snapshot/snapshot-1"] = digest
        else:
            metadata["oracle"] = digest
    entries["manifest.json"] = json.dumps(manifest).encode()
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in entries.items():
            archive.writestr(name, data)
    with pytest.raises(ValueError, match="snapshot"):
        FixtureBundle(path)


def test_cutoff_ties_cannot_replace_strictly_better_neighbor():
    """
    target: exact recall
    method: swap best hit for extra cutoff tie
    expected: reject false full recall.
    """
    oracle = PaimonOracle(
        [{"id": 1, "embedding": [0.0, 0.0]}, {"id": 2, "embedding": [1.0, 0.0]}, {"id": 3, "embedding": [0.0, 1.0]}]
    )
    bad = [{"id": key + 10, "entity": {"id": key}, "distance": 1.0} for key in [2, 3]]
    with pytest.raises(AssertionError, match="recall"):
        oracle.assert_search([bad], [[0.0, 0.0]], "L2", 2, output_fields=["id"])
    good = [{"id": 11, "entity": {"id": 1}, "distance": 0.0}, bad[1]]
    oracle.assert_search([good], [[0.0, 0.0]], "L2", 2, output_fields=["id"])


@pytest.mark.parametrize("metric,expected", [("HAMMING", [0.0, 2.0]), ("JACCARD", [0.0, 0.5])])
def test_binary_oracle_uses_bits_and_ascending_distances(metric, expected):
    """
    target: packed binary metrics
    method: hand-count bits
    expected: exact Hamming/Jaccard.
    """
    oracle = PaimonOracle([{"id": 1, "binary_vec": b"\x0f"}, {"id": 2, "binary_vec": b"\x03"}])
    ids, scores = oracle.topk(b"\x0f", metric, 2, field="binary_vec")
    assert ids.tolist() == [1, 2]
    assert scores.tolist() == expected
    hits = [
        {"id": key + 10, "entity": {"id": key}, "distance": score} for key, score in zip([1, 2], expected, strict=True)
    ]
    oracle.assert_search([hits], [b"\x0f"], metric, 2, field="binary_vec", output_fields=["id"])


def test_sparse_oracle_uses_independent_dot_product():
    """
    target: sparse IP
    method: unordered/disjoint hand-calculated keys
    expected: exact ranking and score.
    """
    oracle = PaimonOracle([{"id": 1, "sparse": {99: 5.0, 3: 2.0}}, {"id": 2, "sparse": {3: -1.0, 7: 4.0}}])
    ids, scores = oracle.topk({7: 2.0, 3: 3.0}, "IP", 2, field="sparse")
    assert ids.tolist() == [1, 2]
    assert scores.tolist() == [6.0, 5.0]


def test_timestamp_output_compares_exact_instant_not_nonempty_string():
    """
    target: timestamp GT
    method: equivalent offsets and one-microsecond mutation
    expected: exact UTC instant.
    """
    oracle = PaimonOracle([{"id": 1, "ts": 1700000000123456}])
    oracle.assert_rows([{"id": 1, "ts": "2023-11-15T06:13:20.123456+08:00"}])
    with pytest.raises(AssertionError):
        oracle.assert_rows([{"id": 1, "ts": "2023-11-14T22:13:20.123457Z"}])


@pytest.mark.parametrize("query", [{}, {9999: 1.0}, {3: 0.0}])
def test_sparse_query_without_nonzero_overlap_returns_no_candidates(query):
    """
    target: sparse candidate contract
    method: empty/disjoint/zero query
    expected: zero hits.
    """
    oracle = PaimonOracle([{"id": 1, "sparse": {3: 1.0}}])
    ids, scores = oracle.topk(query, "IP", 10, field="sparse")
    assert ids.tolist() == scores.tolist() == []
    oracle.assert_search([[]], [query], "IP", 10, field="sparse", output_fields=["id"])


@pytest.mark.parametrize(
    "field,dtype,payload,expected",
    [
        ("half_vec", 102, b"\x00\x3c\x00\xc0", [1.0, -2.0]),
        ("bf16_vec", 103, b"\x80\x3f\x00\xc0", [1.0, -2.0]),
        ("int8_vec", 105, b"\xff\x02", [-1, 2]),
        ("binary_vec", 100, b"\xa5", b"\xa5"),
        ("mh", 100, b"\xa5", b"\xa5"),
    ],
)
def test_actual_sdk_byte_vector_query_output(field, dtype, payload, expected):
    """
    target: SDK raw output
    method: real protobuf lazy decoder
    expected: wrapper decoded and mutation rejected.
    """
    from pymilvus.client.types import HybridExtraList
    from pymilvus.grpc_gen import schema_pb2

    columns = {102: "float16_vector", 103: "bfloat16_vector", 105: "int8_vector", 100: "binary_vector"}
    data = schema_pb2.FieldData(type=dtype, field_name=field)
    data.vectors.dim = 8 if field in {"binary_vec", "mh"} else 2
    setattr(data.vectors, columns[dtype], payload)
    result = HybridExtraList([data], [{"id": 1}])
    oracle = PaimonOracle([{"id": 1, field: expected}])
    oracle.assert_rows(list(result), ["id", field])
    bad = dict(result[0])
    bad[field] = [b"\x00" * len(payload)]
    with pytest.raises(AssertionError):
        oracle.assert_rows([bad], ["id", field])


@pytest.mark.parametrize("variant", ["equal", "different", "wrong-snapshot"])
def test_shared_row_blob_keeps_snapshot_validation_and_distinct_ground_truth(tmp_path, variant):
    """
    target: multi-worker fixture memory
    method: two independently pinned oracle records sharing or differing in row content
    expected: equal payloads reuse decoded GT; distinct payloads and invalid pins remain distinguishable
    """
    path = make_bundle(tmp_path / "base.zip")
    with zipfile.ZipFile(path) as archive:
        blobs = {name: archive.read(name) for name in archive.namelist() if name != "manifest.json"}
        manifest = json.loads(archive.read("manifest.json"))

    def put(value):
        data = json.dumps(value).encode()
        digest = hashlib.sha256(data).hexdigest()
        blobs["blobs/" + digest] = data
        return digest

    versions = manifest["fixtures"]["append"]["versions"]
    rows = [{"id": 1, "label": "one", "embedding": [1.0, 0.0]}]
    for version, snapshot in [("s1", 1), ("s2", 2)]:
        payload = [{**rows[0], "label": "two"}] if version == "s2" and variant == "different" else rows
        rows_blob = put(payload)
        oracle = put(
            {
                "snapshot_id": 1 if variant == "wrong-snapshot" else snapshot,
                "reader": "Paimon Java 1.2.0 pinned batch reader",
                "rows_blob": rows_blob,
            }
        )
        versions[version] = {
            "snapshot_id": snapshot,
            "oracle": oracle,
            "files": {f"snapshot/snapshot-{snapshot}": put({"id": snapshot})},
        }
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("manifest.json", json.dumps(manifest))
        for name, data in blobs.items():
            archive.writestr(name, data)
    if variant == "wrong-snapshot":
        with pytest.raises(ValueError, match="oracle snapshot"):
            FixtureBundle(path)
        return
    bundle = FixtureBundle(path)
    first, second = bundle.oracle("append", "s1"), bundle.oracle("append", "s2")
    first.assert_rows(rows)
    if variant == "equal":
        assert first is second
        second.assert_rows(rows)
    else:
        second.assert_rows([{**rows[0], "label": "two"}])
        with pytest.raises(AssertionError):
            first.assert_rows(second.rows)
