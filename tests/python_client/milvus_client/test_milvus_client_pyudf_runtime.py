"""Search-level runtime contracts on a single enabled PyUDF worker."""

import time

import grpc
import pytest
from common import common_func as cf
from minio import Minio
from pymilvus import DataType, FunctionChain, FunctionChainStage, MilvusClient, MilvusException
from pymilvus.function_chain import col, fn
from pymilvus.function_chain.chain import FunctionChainExpr
from workloads.pyudf.probe import pyudf_probe


@pytest.fixture
def probe_connection(minio_host, minio_bucket):
    uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
    token = cf.param_info.param_token or f"{cf.param_info.param_user}:{cf.param_info.param_password}"
    client = MilvusClient(uri, token=token)
    storage = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
    try:
        yield client, storage, minio_bucket
    finally:
        client.close()


@pytest.fixture
def owned_probe(probe_connection, request):
    with pyudf_probe(*probe_connection, string_pk=True, metric=getattr(request, "param", "L2")) as probe:
        yield probe


@pytest.mark.tags("L1")
def test_pyudf_unicode_primary_key_multiple_queries(owned_probe):
    """Unicode primary keys and scalar payloads stay aligned for every query."""
    owned_probe.check(with_udf=False, nq=4)
    owned_probe.check(nq=4)


@pytest.mark.tags("L1")
def test_pyudf_map_preserves_candidate_order_without_sort(owned_probe):
    chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
        "$score", FunctionChainExpr("py_udf", args=(col("ts"),), params={"resource_name": owned_probe.resource})
    )
    results = owned_probe.client.search(
        owned_probe.name,
        data=[[0.0, 0.0]],
        anns_field="vector",
        limit=3,
        search_params={"metric_type": "L2"},
        function_chains=chain,
    )
    assert [hit["id"] for hit in results[0]] == owned_probe.ids
    assert [hit["distance"] for hit in results[0]] == [20, 40, 60]
    owned_probe.check()


@pytest.mark.tags("L2")
@pytest.mark.parametrize("owned_probe", ["L2", "IP"], indirect=True)
@pytest.mark.parametrize("order", [(0, 1, 2), (2, 0, 1), (1, 2, 0)])
def test_pyudf_composes_with_builtin_l0_l1_in_stage_order(owned_probe, order):
    l0 = FunctionChain(FunctionChainStage.L0_RERANK).map("$score", fn.num_combine(col("ts"), col("ts"), mode="sum"))
    l1 = FunctionChain(FunctionChainStage.L1_RERANK).map("$score", fn.num_combine(col("$score"), col("ts"), mode="sum"))
    l2 = (
        FunctionChain(FunctionChainStage.L2_RERANK)
        .map(
            "$score", FunctionChainExpr("py_udf", args=(col("$score"),), params={"resource_name": owned_probe.resource})
        )
        .sort(col("$score"), desc=True, tie_break_col=col("$id"))
    )
    stages = [l0, l1, l2]
    results = owned_probe.client.search(
        owned_probe.name,
        data=[[0.0, 0.0]] * 2,
        anns_field="vector",
        limit=3,
        search_params={"metric_type": owned_probe.metric},
        output_fields=["ts"],
        function_chains=[stages[index] for index in order],
    )
    for hits in results:
        assert [hit["id"] for hit in hits] == list(reversed(owned_probe.ids))
        assert [hit["distance"] for hit in hits] == [180, 120, 60]
        assert [hit["entity"]["ts"] for hit in hits] == [30, 20, 10]
    owned_probe.check()


@pytest.mark.tags("L1")
@pytest.mark.parametrize("mode,code", [("system_exit", 5), ("os_error", 2400), ("memory_error", 3)])
def test_pyudf_exception_origin_and_recovery(owned_probe, mode, code):
    """User exception origins survive the entire worker-to-Search error path."""
    with pytest.raises(MilvusException) as caught:
        owned_probe.search(mode)
    assert caught.value.code == code, caught.value
    assert "qa_synthetic_" + mode in str(caught.value), caught.value
    owned_probe.check(with_udf=False)
    owned_probe.check()


@pytest.mark.tags("L2")
@pytest.mark.parametrize(
    "kind,metric",
    [
        *[
            (kind, metric)
            for kind in (DataType.FLOAT_VECTOR, DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR, DataType.INT8_VECTOR)
            for metric in ("L2", "IP", "COSINE")
        ],
        (DataType.BINARY_VECTOR, "HAMMING"),
        (DataType.BINARY_VECTOR, "JACCARD"),
        (DataType.SPARSE_FLOAT_VECTOR, "IP"),
    ],
)
def test_pyudf_vector_recall_types_preserve_scalar_rerank(probe_connection, kind, metric, record_property):
    """All candidates are retained; vector encoding must not alter scalar/ID alignment."""
    with pyudf_probe(*probe_connection, vector_kind=kind, metric=metric) as probe:
        ordinary = probe.search(with_udf=False, nq=2)
        for hits in ordinary:
            assert sorted((hit["id"], hit["entity"]["ts"]) for hit in hits) == [(1, 10), (2, 20), (3, 30)]
        probe.check(nq=2)
        probe.client.release_collection(probe.name)
        probe.client.load_collection(probe.name)
        probe.check(nq=2)
        record_property("vector_tuple", f"{kind.name}/{metric}/3 exhaustive candidates/reload")


@pytest.mark.tags("L3", "pyudf")
def test_pyudf_timeout_completes_without_replay(owned_probe, request, record_property):
    """A deadline ends the RPC; the bounded user call completes exactly once."""
    import requests

    endpoint = request.config.getoption("--pyudf-management-endpoint")
    assert endpoint, "Requires one standalone Pod and management endpoint"
    assert owned_probe.client.get_server_version(detail=True)["deploy_mode"] == "STANDALONE"
    response = requests.get(
        endpoint.rstrip("/") + "/management/config/get",
        params={"keys": "function.pyUDF.server.workerCount"},
        timeout=10,
    )
    response.raise_for_status()
    assert response.json()["configs"][0]["value"] == "1"
    # Fixture warmup completed call 1. This observation is call 2.
    assert {hit["distance"] for hit in owned_probe.search("state")[0]} == {102}
    started = time.monotonic()
    try:
        with pytest.raises((MilvusException, grpc.RpcError)) as caught:
            owned_probe.search("delayed", timeout=0.3)
        if isinstance(caught.value, grpc.RpcError):
            assert caught.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED, caught.value
            record_property("deadline_origin", "client RPC deadline")
        else:
            assert caught.value.code == 10001, caught.value
            record_property("deadline_origin", "server deadline status")
        assert time.monotonic() - started < 1.5
    finally:
        # Wait for the actual two-second UDF to finish before cleanup or recovery.
        time.sleep(max(0, 2.5 - (time.monotonic() - started)))
    # Call 3 completed after its deadline; no replay made extra calls.
    state = {hit["distance"] for hit in owned_probe.search("state")[0]}
    assert state == {304}, state
    record_property("timeout_invocations", "one timed-out call; completed=3 at call=4")
    owned_probe.check(with_udf=False)
    owned_probe.check()


@pytest.mark.tags("L1")
@pytest.mark.parametrize("descending", [False, True])
@pytest.mark.parametrize("tied", [False, True])
def test_pyudf_explicit_sort_direction_and_unicode_ties(owned_probe, descending, tied):
    """Sort the transformed scores and use ascending Unicode IDs to break ties."""
    timestamps = [10, 10, 10] if tied else [10, 20, 30]
    if tied:
        owned_probe.client.upsert(
            owned_probe.name,
            [
                {"id": pk, "ts": timestamps[index], "text": f"你好_{index}", "vector": [float(index), 0.0]}
                for index, pk in enumerate(owned_probe.ids)
            ],
        )
    chain = (
        FunctionChain(FunctionChainStage.L2_RERANK)
        .map("$score", FunctionChainExpr("py_udf", args=(col("ts"),), params={"resource_name": owned_probe.resource}))
        .sort(col("$score"), desc=descending, tie_break_col=col("$id"))
    )
    expected = sorted(
        zip(owned_probe.ids, timestamps, strict=True),
        key=lambda item: ((-1 if descending else 1) * item[1], item[0]),
    )
    results = owned_probe.client.search(
        owned_probe.name,
        data=[[0.0, 0.0]] * 2,
        anns_field="vector",
        limit=3,
        search_params={"metric_type": "L2"},
        output_fields=["ts", "text"],
        function_chains=chain,
    )
    for hits in results:
        assert [hit["id"] for hit in hits] == [pk for pk, _ in expected]
        assert [hit["distance"] for hit in hits] == [2 * ts for _, ts in expected]
        assert [hit["entity"]["ts"] for hit in hits] == [ts for _, ts in expected]
        assert [hit["entity"]["text"] for hit in hits] == [f"你好_{owned_probe.ids.index(pk)}" for pk, _ in expected]
