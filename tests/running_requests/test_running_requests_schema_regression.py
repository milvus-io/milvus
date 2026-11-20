"""Cancellation preserves independent Search with MMAP, nullable and added fields."""

from uuid import uuid4

import numpy as np
import pytest
from pymilvus import DataType
from test_running_requests_t13_extended import cancel_inflight


@pytest.mark.parametrize("variant", ["mmap", "nullable", "add_field"])
def test_cancel_schema_regression(variant, deployment, request):
    check_schema_cancel(variant, deployment, request)


def check_schema_cancel(variant, deployment, request, *, operator=None):
    uris, _, user, connect = deployment
    a, b = uris
    app, control = connect(a), connect(a)
    operator = connect(b) if operator is None else operator
    name, vectors = prepare_schema_search(variant, app, request)
    expr = "extra is null" if variant == "nullable" else "id >= 0"
    fields = ["id"] if variant == "mmap" else ["id", "extra"]

    def invoke(client, nq):
        return client.search(
            name,
            vectors[:nq].tolist(),
            anns_field="vector",
            filter=expr,
            limit=1,
            output_fields=fields,
            search_params={"metric_type": "L2"},
            timeout=90,
        )

    def check(result):
        assert result[0][0]["id"] == 0
        assert abs(result[0][0]["distance"]) < 1e-5
        if variant == "nullable":
            assert result[0][0]["entity"]["extra"] is None
        if variant == "add_field":
            assert result[0][0]["entity"]["extra"] == 7

    check(invoke(control, 1))
    cancel_inflight(operator, set(uris), name, user, expr, lambda: invoke(app, 16384), "Search", a)
    check(invoke(control, 1))


def prepare_schema_search(variant, app, request):
    name = "rr_schema_" + uuid4().hex[:12]
    schema = app.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("vector", DataType.FLOAT_VECTOR, dim=128)
    if variant == "nullable":
        schema.add_field("extra", DataType.INT64, nullable=True)
    app.create_collection(name, schema=schema, consistency_level="Strong", timeout=60)
    request.addfinalizer(lambda: app.drop_collection(name, timeout=60))
    vectors = np.random.default_rng(1315).random((40000, 128), dtype=np.float32)
    for start in range(0, len(vectors), 1000):
        rows = [dict(id=i, vector=vectors[i].tolist()) for i in range(start, start + 1000)]
        if variant == "nullable":
            for row in rows:
                row["extra"] = None if row["id"] % 2 == 0 else row["id"]
        app.insert(name, rows, timeout=60)
    app.flush(name, timeout=120)
    if variant == "add_field":
        app.add_collection_field(name, "extra", DataType.INT64, nullable=True, default_value=7, timeout=60)
    if variant == "mmap":
        app.alter_collection_properties(name, {"mmap.enabled": True}, timeout=30)
        properties = app.describe_collection(name)["properties"]
        assert str(properties.get("mmap.enabled")).lower() == "true", properties
    index = app.prepare_index_params()
    index.add_index("vector", index_type="FLAT", metric_type="L2")
    app.create_index(name, index, timeout=120)
    app.load_collection(name, timeout=120)
    return name, vectors
