import json

import pytest

from spark_backfill.contracts import (
    BundleContractError,
    ConnectorBundleManifest,
    GroundTruthError,
    build_ground_truth,
    extract_read_probe_result,
    storage_kind,
)


def _valid_manifest():
    return {
        "connectorRevision": "abc123",
        "sparkVersion": "4.0.1",
        "scalaBinaryVersion": "2.13",
        "javaMajor": 21,
        "os": "linux",
        "arch": "amd64",
        "assemblyJar": "connector-assembly.jar",
        "backfillMainClass": "com.zilliz.spark.connector.operations.backfill.BackfillApp",
        "files": {
            "connector-assembly.jar": "a" * 64,
            "lib/libmilvus-storage.so": "b" * 64,
            "lib/libmilvus-storage-jni.so": "c" * 64,
        },
    }


def test_connector_bundle_manifest_accepts_expected_runtime():
    manifest = ConnectorBundleManifest.from_dict(_valid_manifest())

    manifest.validate_runtime(
        spark_version="4.0.1",
        scala_binary_version="2.13",
        java_major=21,
        os_name="linux",
        arch="amd64",
    )

    assert manifest.assembly_jar == "connector-assembly.jar"
    assert manifest.connector_revision == "abc123"


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("sparkVersion", "3.5.0", "Spark version"),
        ("scalaBinaryVersion", "2.12", "Scala binary version"),
        ("javaMajor", 17, "Java major version"),
        ("os", "darwin", "operating system"),
        ("arch", "arm64", "architecture"),
    ],
)
def test_connector_bundle_manifest_rejects_incompatible_runtime(key, value, message):
    raw = _valid_manifest()
    raw[key] = value
    manifest = ConnectorBundleManifest.from_dict(raw)

    with pytest.raises(BundleContractError, match=message):
        manifest.validate_runtime(
            spark_version="4.0.1",
            scala_binary_version="2.13",
            java_major=21,
            os_name="linux",
            arch="amd64",
        )


def test_connector_bundle_manifest_requires_all_bundle_files():
    raw = _valid_manifest()
    del raw["files"]["lib/libmilvus-storage-jni.so"]

    with pytest.raises(BundleContractError, match="libmilvus-storage-jni.so"):
        ConnectorBundleManifest.from_dict(raw)


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        (
            "coalesce",
            {
                0: {"bf_score": 1000.0, "bf_label": "from-file"},
                1: {"bf_score": 11.0, "bf_label": "source"},
                2: {"bf_score": None, "bf_label": None},
            },
        ),
        (
            "overwrite",
            {
                0: {"bf_score": -1.0, "bf_label": "from-file"},
                1: {"bf_score": None, "bf_label": None},
                2: {"bf_score": None, "bf_label": None},
            },
        ),
        (
            "replace",
            {
                0: {"bf_score": -1.0, "bf_label": "from-file"},
                1: {"bf_score": None, "bf_label": None},
                2: {"bf_score": None, "bf_label": None},
            },
        ),
    ],
)
def test_build_ground_truth_implements_backfill_modes(mode, expected):
    source = {
        0: {"bf_score": 1000.0, "bf_label": None},
        1: {"bf_score": 11.0, "bf_label": "source"},
        2: {"bf_score": None, "bf_label": None},
    }
    parquet = {
        0: {"bf_score": -1.0, "bf_label": "from-file"},
        1: {"bf_score": None, "bf_label": None},
        100: {"bf_score": 100.0, "bf_label": "outside-snapshot"},
    }

    actual = build_ground_truth(source, parquet, ["bf_score", "bf_label"], mode)

    assert actual == expected
    assert 100 not in actual


def test_build_ground_truth_rejects_unknown_mode():
    with pytest.raises(GroundTruthError, match="unknown backfill mode"):
        build_ground_truth({1: {"f": None}}, {1: {"f": 1}}, ["f"], "merge")


@pytest.mark.parametrize(
    ("segment", "expected"),
    [
        ({"version": 7, "manifestPaths": ["s3a://bucket/manifest-7.avro"]}, "v3"),
        (
            {
                "version": -1,
                "storage_version": 2,
                "column_groups": [{"field_ids": [104], "binlog_files": ["1"], "row_count": 30}],
            },
            "v2",
        ),
    ],
)
def test_storage_kind_uses_payload_not_historical_field_names(segment, expected):
    assert storage_kind(segment) == expected


def test_extract_read_probe_result_parses_last_sentinel():
    first = {"count": 1}
    final = {"count": 30, "primaryKeys": list(range(30))}
    logs = "\n".join(
        [
            "spark log",
            "SPARK_BACKFILL_READ_RESULT=" + json.dumps(first),
            "more spark log",
            "SPARK_BACKFILL_READ_RESULT=" + json.dumps(final),
        ]
    )

    assert extract_read_probe_result(logs) == final
