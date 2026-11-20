"""External Parquet collections: actual Search/Query cancellation and controls."""

import base64
import io
import json
import os
from time import monotonic, sleep
from uuid import uuid4

import numpy as np
import pytest
from pymilvus import DataType
from running_requests_topology import ProxyAuthorizationTopology, forward, run
from test_running_requests_t13_extended import cancel_inflight


@pytest.fixture
def external_data(deployment, request):
    if dependency_path := os.getenv("MILVUS_EXTERNAL_DEPS"):
        request.getfixturevalue("monkeypatch").syspath_prepend(dependency_path)
    minio = pytest.importorskip("minio", reason="External fixture requires MinIO Python client")
    import pyarrow as pa
    import pyarrow.parquet as pq

    kube = ProxyAuthorizationTopology().kube
    release = os.environ.get("MILVUS_L1_RELEASE", "requests-l0")
    service = os.environ.get("MILVUS_L1_MINIO_SERVICE", f"{release}-minio")
    service_spec = json.loads(run([*kube, "get", "service", service, "-o", "json"]))["spec"]
    selector = service_spec.get("selector")
    assert selector, f"MinIO Service {service} must select its Pods"
    label_query = ",".join(f"{key}={value}" for key, value in selector.items())
    pods = json.loads(run([*kube, "get", "pods", "-l", label_query, "-o", "json"]))["items"]
    containers = [
        container
        for pod in pods
        if not pod["metadata"].get("deletionTimestamp")
        and any(item["type"] == "Ready" and item["status"] == "True" for item in pod["status"].get("conditions", []))
        for container in pod["spec"]["containers"]
        if container["name"] == "minio"
    ]
    assert containers, f"MinIO Service {service} has no Ready MinIO Pod"
    env = {item["name"]: item for item in containers[0]["env"]}

    def credential(name):
        item = env[name]
        if "value" in item:
            return item["value"]
        ref = item["valueFrom"]["secretKeyRef"]
        secret = json.loads(run([*kube, "get", "secret", ref["name"], "-o", "json"]))
        return base64.b64decode(secret["data"][ref["key"]]).decode()

    access, secret = (
        (credential("MINIO_ROOT_USER"), credential("MINIO_ROOT_PASSWORD"))
        if "MINIO_ROOT_USER" in env
        else (credential("MINIO_ACCESS_KEY"), credential("MINIO_SECRET_KEY"))
    )
    uris, _, _, connect = deployment
    app = connect(next(iter(uris)))
    name = "rr_external_" + uuid4().hex[:12]
    bucket = name.replace("_", "-")
    dim = 512 if request.node.callspec.params["kind"] == "Query" else 128
    vectors = np.random.default_rng(1314).random((40000, dim), dtype=np.float32)
    with forward(kube, f"service/{service}", 9000) as uri:
        storage = minio.Minio(uri.removeprefix("http://"), access_key=access, secret_key=secret, secure=False)
        storage.make_bucket(bucket)
        created = False
        try:
            buf = io.BytesIO()
            table = pa.table(
                {
                    "id": pa.array(np.arange(len(vectors))),
                    "embedding": pa.array(vectors.tolist(), type=pa.list_(pa.float32())),
                }
            )
            pq.write_table(table, buf)
            payload = buf.getvalue()
            storage.put_object(bucket, "data/batch.parquet", io.BytesIO(payload), len(payload))
            ext = json.dumps(
                {
                    "format": "parquet",
                    "extfs": {
                        "cloud_provider": "minio",
                        "region": "us-east-1",
                        "access_key_id": access,
                        "access_key_value": secret,
                        "use_ssl": "false",
                    },
                }
            )
            schema = app.create_schema(external_source=f"s3://{service}:9000/{bucket}/data/", external_spec=ext)
            schema.add_field("id", DataType.INT64, external_field="id")
            schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim, external_field="embedding")
            app.create_collection(name, schema=schema, timeout=60)
            created = True
            job = app.refresh_external_collection(name, timeout=30)
            deadline = monotonic() + 180
            while monotonic() < deadline:
                status = app.get_refresh_external_collection_progress(job, timeout=10)
                if status.state == "RefreshFailed":
                    pytest.fail("External refresh failed; inspect server job logs (credentials excluded)")
                if status.state == "RefreshCompleted":
                    break
                sleep(1)
            else:
                pytest.fail("External refresh did not complete within 180 seconds")
            indexes = app.prepare_index_params()
            indexes.add_index("embedding", index_type="FLAT", metric_type="L2")
            app.create_index(name, indexes, timeout=120)
            app.load_collection(name, timeout=120)
            yield app, name, vectors
        finally:
            if created:
                app.drop_collection(name, timeout=60)
            storage.remove_object(bucket, "data/batch.parquet")
            storage.remove_bucket(bucket)


@pytest.mark.parametrize("kind", ["Search", "Query"])
def test_external_cancel(kind, deployment, external_data):
    check_external_cancel(kind, deployment, external_data)


def check_external_cancel(kind, deployment, external_data, *, operator=None):
    uris, _, user, connect = deployment
    a, b = uris
    app, name, vectors = external_data
    control = connect(a)
    operator = connect(b) if operator is None else operator
    expr = "id >= 0"

    def invoke(client, nq):
        if kind == "Search":
            return client.search(
                name,
                vectors[:nq].tolist(),
                anns_field="embedding",
                filter=expr,
                output_fields=["id"],
                limit=1,
                search_params={"metric_type": "L2"},
                timeout=90,
            )
        return client.query(name, filter=expr, output_fields=["id", "embedding"], limit=16000, timeout=90)

    baseline = invoke(control, 1)
    if kind == "Search":
        assert baseline[0][0]["entity"]["id"] == 0
        assert abs(baseline[0][0]["distance"]) < 1e-5
    else:
        assert len(baseline) == 16000
        assert len({r["id"] for r in baseline}) == 16000
        assert all(np.allclose(r["embedding"], vectors[r["id"]]) for r in baseline)
    cancel_inflight(operator, set(uris), name, user, expr, lambda: invoke(app, 16384), kind, a)
    actual = invoke(control, 1)
    assert len(actual) == len(baseline)
    if kind == "Query":
        assert {r["id"] for r in actual} == {r["id"] for r in baseline}
    else:
        assert actual[0][0]["entity"]["id"] == 0
