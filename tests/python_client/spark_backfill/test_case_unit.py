from types import SimpleNamespace

from spark_backfill.case import BackfillCase, infer_root_path


class FakeRunner:
    def __init__(self):
        self.requests = []

    def run(self, request):
        self.requests.append(request)
        return SimpleNamespace(succeeded=True, logs="ok")


class FakeObject:
    def __init__(self, object_name):
        self.object_name = object_name


class FakeMinio:
    def list_objects(self, bucket, prefix, recursive):
        assert bucket == "bucket"
        assert prefix == "files/spark-backfill/run/explicit-null-coalesce/"
        assert recursive is True
        return [
            FakeObject("files/spark-backfill/run/explicit-null-coalesce/part-1"),
            FakeObject("files/spark-backfill/run/explicit-null-coalesce/result.json"),
        ]


def test_infer_root_path_from_snapshot_location():
    assert infer_root_path("s3a://bucket/files/snapshots/100/metadata/1.json") == "files"
    assert infer_root_path("s3a://bucket/custom/root/snapshots/100/metadata/1.json") == "custom/root"


def test_backfill_case_builds_remote_job_and_result_under_milvus_root(tmp_path):
    runner = FakeRunner()
    case = BackfillCase(
        client=None,
        minio_client=None,
        runner=runner,
        settings=SimpleNamespace(
            minio_bucket="bucket",
            spark_minio_endpoint="minio.ns:9000",
            management_endpoint="http://milvus:9091",
            spark_milvus_uri="http://milvus.ns:19530",
            milvus_token="root:Milvus",
        ),
        tmp_path=tmp_path,
        collection_name="collection",
        snapshot_names=["snapshot"],
        snapshot_location="s3a://bucket/files/snapshots/100/metadata/1.json",
        snapshot=None,
        prefix="files/spark-backfill/run",
        source_rows=[],
    )

    result, result_uri = case.run_backfill(
        case_id="coalesce",
        parquet_uri="s3a://bucket/files/spark-backfill/run/input.parquet",
        mode="coalesce",
    )

    assert result.succeeded is True
    assert result_uri == "s3a://bucket/files/spark-backfill/run/coalesce/result.json"
    request = runner.requests[0]
    assert request.operation == "backfill"
    assert request.payload["arguments"][-4:] == ["--mode", "coalesce", "--batch-size", "1024"]


def test_backfill_case_lists_objects_from_result_parent_without_truncating_case_id(tmp_path):
    case = BackfillCase(
        client=None,
        minio_client=FakeMinio(),
        runner=FakeRunner(),
        settings=SimpleNamespace(minio_bucket="bucket"),
        tmp_path=tmp_path,
        collection_name="collection",
        snapshot_names=[],
        snapshot_location="s3a://bucket/files/snapshots/100/metadata/1.json",
        snapshot=None,
        prefix="files/spark-backfill/run",
        source_rows=[],
    )

    objects = case.list_result_objects("s3a://bucket/files/spark-backfill/run/explicit-null-coalesce/result.json")

    assert objects[-1].endswith("explicit-null-coalesce/result.json")
