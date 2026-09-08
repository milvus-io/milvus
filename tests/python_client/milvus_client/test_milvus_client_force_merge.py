"""
ForceMerge Compaction Test Cases

L3 tests require Milvus configuration changes to trigger actual force merge:
    dataCoord:
      segment:
        maxSize: 64  # MB, default is 1024
      compaction:
        enableAutoCompaction: false  # Disable auto compaction

With maxSize=64MB and auto compaction disabled, small data volumes can trigger
force merge compaction manually without interference from auto compaction.
"""

import inspect
import json
import os
import time
from collections import Counter

import grpc
import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.constants import *  # noqa: F403
from minio import Minio
from pymilvus import DataType
from pymilvus.client.call_context import _api_level_md
from pymilvus.client.utils import check_status
from pymilvus.decorators import IGNORE_RETRY_CODES
from pymilvus.exceptions import ErrorCode, MilvusException
from pymilvus.grpc_gen import common_pb2
from pymilvus.grpc_gen import milvus_pb2 as milvus_types
from utils.util_log import test_log as log
from utils.util_pymilvus import *  # noqa: F403

prefix = "client_force_merge"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name

# ForceMerge specific constants
max_int64 = (1 << 63) - 1
auto_target_size_mb = max_int64 // (1024 * 1024) + 1  # Triggers server auto target-size mode.
actual_output_size_tolerance = 0.10
force_merge_target_size_tolerance = 0.05
default_pooling_datanode_memory = 32 * 1024 * 1024 * 1024
initial_query_retry_backoff = 0.01
max_query_retry_backoff = 3
query_retry_backoff_multiplier = 3
system_info_request = json.dumps({"metric_type": "system_info"})


def minio_endpoint(minio_host):
    endpoint = (minio_host or "localhost").strip()
    if "://" in endpoint:
        endpoint = endpoint.split("://", 1)[1]
    endpoint = endpoint.split("/", 1)[0]
    if ":" not in endpoint:
        endpoint = f"{endpoint}:9000"
    return endpoint


def new_minio_client(minio_host):
    return Minio(
        minio_endpoint(minio_host),
        access_key=os.getenv("MILVUS_MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MILVUS_MINIO_SECRET_KEY", "minioadmin"),
        secure=os.getenv("MILVUS_MINIO_SECURE", "false").lower() in ["1", "true", "yes"],
    )


def get_insert_log_sizes(minio_client, bucket, collection_id, segment_ids):
    root_path = os.getenv("MILVUS_MINIO_ROOT_PATH", "files").strip("/")
    prefix = f"{root_path}/insert_log/{collection_id}/"
    sizes = {str(segment_id): 0 for segment_id in segment_ids}

    for item in minio_client.list_objects(bucket, prefix=prefix, recursive=True):
        relative_parts = item.object_name[len(prefix) :].split("/")
        if len(relative_parts) < 3:
            continue
        segment_id = relative_parts[1]
        if segment_id in sizes:
            sizes[segment_id] += item.size

    missing = [segment_id for segment_id, size in sizes.items() if size == 0]
    assert not missing, f"No insert-log objects found for segments {missing} under {prefix}"
    return {int(segment_id): size for segment_id, size in sizes.items()}


def get_system_info_nodes_once(client, timeout=30):
    handler = client._get_connection()
    response = handler._stub.GetMetrics(
        milvus_types.GetMetricsRequest(request=system_info_request),
        wait_for_ready=True,
        timeout=timeout,
        metadata=_api_level_md(client._generate_call_context()),
    )
    check_status(response.status)
    return json.loads(response.response).get("nodes_info", [])


def get_segment_max_size_mb(client):
    for node in get_system_info_nodes_once(client):
        if node.get("infos", {}).get("type", "").lower() != "datacoord":
            continue
        configurations = node.get("infos", {}).get("system_configurations", {})
        max_size = configurations.get("segment_max_size")
        if max_size is not None:
            return int(max_size)
    return None


def force_merge_machine_safe_size_mb(nodes, querynode_memory_factor, datanode_memory_factor):
    """Mirror the server's memory ceiling using the same system-info metrics."""
    # Failed node metrics may have no component type, so conservatively reject
    # every untyped error as well as explicit QueryNode/DataNode errors.
    if any(
        node.get("infos", {}).get("has_error", False)
        and node.get("infos", {}).get("type", "").lower() in {"", "querynode", "datanode"}
        for node in nodes
    ):
        return None
    query_nodes = [node for node in nodes if node.get("infos", {}).get("type", "").lower() == "querynode"]
    data_nodes = [node for node in nodes if node.get("infos", {}).get("type", "").lower() == "datanode"]
    if not query_nodes or not data_nodes:
        return None

    query_memory = [int(node.get("infos", {}).get("hardware_infos", {}).get("memory", 0)) for node in query_nodes]
    raw_data_memory = [int(node.get("infos", {}).get("hardware_infos", {}).get("memory", 0)) for node in data_nodes]
    is_pooling = any(memory == 0 for memory in raw_data_memory)
    data_memory = [memory or default_pooling_datanode_memory for memory in raw_data_memory]
    querynode_safe_size = min(query_memory) / querynode_memory_factor
    datanode_safe_size = min(data_memory) / datanode_memory_factor
    safe_size = min(querynode_safe_size, datanode_safe_size)

    deploy_modes = {node.get("infos", {}).get("system_info", {}).get("deploy_mode", "").upper() for node in nodes}
    if "STANDALONE" in deploy_modes and not is_pooling:
        safe_size *= 0.5
    return safe_size / (1024 * 1024)


def get_force_merge_machine_safe_size_mb(client):
    # Unlike MilvusSys.nodes, retain has_error=true records so an unavailable
    # topology member cannot make the test overestimate the server ceiling.
    nodes = get_system_info_nodes_once(client)
    datacoord_nodes = [node for node in nodes if node.get("infos", {}).get("type", "").lower() == "datacoord"]
    if not datacoord_nodes:
        return None
    configurations = datacoord_nodes[0].get("infos", {}).get("system_configurations", {})
    querynode_memory_factor = configurations.get("force_merge_querynode_memory_factor")
    datanode_memory_factor = configurations.get("force_merge_datanode_memory_factor")
    if querynode_memory_factor is None or datanode_memory_factor is None:
        return None
    return force_merge_machine_safe_size_mb(
        nodes,
        float(querynode_memory_factor),
        float(datanode_memory_factor),
    )


@pytest.mark.tags(CaseLabel.L0)
def test_get_system_info_nodes_retains_error_records():
    nodes = [
        {"infos": {"type": "querynode", "has_error": False}},
        {"infos": {"type": "", "has_error": True, "error_reason": "metrics unavailable"}},
    ]

    class FakeHandler:
        def __init__(self):
            self._stub = self

        def GetMetrics(self, request, wait_for_ready, timeout, metadata):
            assert json.loads(request.request) == {"metric_type": "system_info"}
            assert wait_for_ready
            assert timeout == 7
            assert metadata is None
            return milvus_types.GetMetricsResponse(
                status=common_pb2.Status(error_code=common_pb2.Success),
                response=json.dumps({"nodes_info": nodes}),
            )

    class FakeClient:
        def _get_connection(self):
            return FakeHandler()

        def _generate_call_context(self):
            return None

    assert get_system_info_nodes_once(FakeClient(), timeout=7) == nodes


@pytest.mark.tags(CaseLabel.L0)
def test_force_merge_machine_safe_size_wrapper_rejects_untyped_error_node():
    nodes = [
        {
            "infos": {
                "type": "datacoord",
                "has_error": False,
                "system_configurations": {
                    "force_merge_querynode_memory_factor": 4,
                    "force_merge_datanode_memory_factor": 4,
                },
            }
        },
        {
            "infos": {
                "type": "querynode",
                "has_error": False,
                "hardware_infos": {"memory": 16 * 1024 * 1024 * 1024},
            }
        },
        {
            "infos": {
                "type": "datanode",
                "has_error": False,
                "hardware_infos": {"memory": 32 * 1024 * 1024 * 1024},
            }
        },
        {"infos": {"type": "", "has_error": True, "error_reason": "metrics unavailable"}},
    ]

    class FakeHandler:
        def __init__(self):
            self._stub = self

        def GetMetrics(self, request, wait_for_ready, timeout, metadata):
            assert json.loads(request.request) == {"metric_type": "system_info"}
            assert wait_for_ready
            assert timeout == 30
            assert metadata is None
            return milvus_types.GetMetricsResponse(
                status=common_pb2.Status(error_code=common_pb2.Success),
                response=json.dumps({"nodes_info": nodes}),
            )

    class FakeClient:
        def _get_connection(self):
            return FakeHandler()

        def _generate_call_context(self):
            return None

    assert get_force_merge_machine_safe_size_mb(FakeClient()) is None


def query_ids_page_once(client, collection_name, page_start, page_end, timeout):
    """Run one query RPC without the SDK retry decorator reusing a stale timeout."""
    handler = client._get_connection()
    raw_query = inspect.unwrap(handler.query)
    query_options = client._with_cluster_id({"consistency_level": "Strong"})
    kwargs = {
        "collection_name": collection_name,
        "expr": f"{default_primary_key_field_name} >= {page_start} && {default_primary_key_field_name} < {page_end}",
        "output_fields": [default_primary_key_field_name],
        "timeout": timeout,
        "context": client._generate_call_context(),
        **query_options,
    }
    if inspect.ismethod(raw_query):
        return raw_query(**kwargs)
    return raw_query(handler, **kwargs)


def collect_query_ids(
    client,
    collection_name,
    total_rows,
    timeout=180,
    rpc_timeout=30,
    page_size=4096,
    monotonic=time.monotonic,
    sleep=time.sleep,
):
    deadline = monotonic() + timeout
    ids = []
    for page_start in range(0, total_rows, page_size):
        backoff = initial_query_retry_backoff
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Query pagination for {collection_name} exceeded {timeout} seconds")
            try:
                rows = query_ids_page_once(
                    client,
                    collection_name,
                    page_start,
                    min(page_start + page_size, total_rows),
                    timeout=min(rpc_timeout, remaining),
                )
                if monotonic() >= deadline:
                    raise TimeoutError(f"Query pagination for {collection_name} exceeded {timeout} seconds")
                break
            except grpc.RpcError as error:
                if error.code() in IGNORE_RETRY_CODES:
                    raise
                if error.code() == grpc.StatusCode.UNAVAILABLE:
                    handler = client._get_connection()
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise TimeoutError(
                            f"Query pagination for {collection_name} exceeded {timeout} seconds"
                        ) from error
                    handler.reconnect(timeout=remaining)
                    if monotonic() >= deadline:
                        raise TimeoutError(
                            f"Query pagination for {collection_name} exceeded {timeout} seconds"
                        ) from error
            except MilvusException as error:
                is_rate_limit = error.code == ErrorCode.RATE_LIMIT or error.compatible_code == common_pb2.RateLimit
                if not is_rate_limit:
                    raise

            remaining = deadline - monotonic()
            if remaining <= backoff:
                raise TimeoutError(f"Query pagination for {collection_name} exceeded {timeout} seconds")
            sleep(backoff)
            backoff = min(backoff * query_retry_backoff_multiplier, max_query_retry_backoff)
        ids.extend(row[default_primary_key_field_name] for row in rows)
    return ids


@pytest.mark.tags(CaseLabel.L0)
def test_collect_query_ids_bounds_every_page_rpc():
    now = 0.0

    def monotonic():
        return now

    class FakeHandler:
        def __init__(self):
            self.rpc_timeouts = []
            self.expressions = []

        def raw_query(self, collection_name, expr, output_fields, timeout, context, **kwargs):
            nonlocal now
            assert collection_name == "deadline_collection"
            assert output_fields == [default_primary_key_field_name]
            assert context == "deadline-context"
            assert kwargs["consistency_level"] == "Strong"
            assert kwargs["cluster_id"] == "cluster-a"
            self.rpc_timeouts.append(timeout)
            self.expressions.append(expr)
            now += 2
            return [{default_primary_key_field_name: 1}]

        def query(self, *args, **kwargs):
            raise AssertionError("the SDK retry wrapper must not be called")

    FakeHandler.query.__wrapped__ = FakeHandler.raw_query
    handler = FakeHandler()

    class FakeClient:
        def _get_connection(self):
            return handler

        def _generate_call_context(self):
            return "deadline-context"

        def _with_cluster_id(self, kwargs):
            kwargs["cluster_id"] = "cluster-a"
            return kwargs

    with pytest.raises(TimeoutError, match="exceeded 3 seconds"):
        collect_query_ids(
            FakeClient(),
            "deadline_collection",
            total_rows=4,
            timeout=3,
            rpc_timeout=10,
            page_size=2,
            monotonic=monotonic,
        )

    assert handler.rpc_timeouts == [3, 1]
    assert handler.expressions == ["id >= 0 && id < 2", "id >= 2 && id < 4"]


@pytest.mark.tags(CaseLabel.L0)
@pytest.mark.parametrize("failure", ["unavailable", "rate-limit", "legacy-rate-limit"])
def test_collect_query_ids_retries_transient_page_failures(failure):
    now = 0.0

    def monotonic():
        return now

    def sleep(delay):
        nonlocal now
        now += delay

    class RetryableRpcError(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNAVAILABLE

    class FakeHandler:
        def __init__(self):
            self.calls = 0
            self.rpc_timeouts = []
            self.reconnect_timeouts = []

        def raw_query(self, collection_name, expr, output_fields, timeout, context, **kwargs):
            nonlocal now
            self.calls += 1
            self.rpc_timeouts.append(timeout)
            assert kwargs["cluster_id"] == "cluster-a"
            if self.calls == 1:
                now += 0.25
                if failure == "unavailable":
                    raise RetryableRpcError()
                if failure == "rate-limit":
                    raise MilvusException(code=ErrorCode.RATE_LIMIT, message="limited")
                try:
                    check_status(
                        common_pb2.Status(
                            error_code=common_pb2.RateLimit,
                            reason="legacy limited",
                        )
                    )
                except MilvusException as error:
                    assert error.code == common_pb2.Success
                    assert error.compatible_code == common_pb2.RateLimit
                    raise
            return [{default_primary_key_field_name: 0}, {default_primary_key_field_name: 1}]

        def query(self, *args, **kwargs):
            raise AssertionError("the SDK retry wrapper must not be called")

        def reconnect(self, timeout):
            self.reconnect_timeouts.append(timeout)

    FakeHandler.query.__wrapped__ = FakeHandler.raw_query
    handler = FakeHandler()

    class FakeClient:
        def _get_connection(self):
            return handler

        def _generate_call_context(self, **kwargs):
            return "deadline-context"

        def _with_cluster_id(self, kwargs):
            kwargs["cluster_id"] = "cluster-a"
            return kwargs

    assert collect_query_ids(
        FakeClient(),
        "retry_collection",
        total_rows=2,
        timeout=1,
        rpc_timeout=1,
        page_size=2,
        monotonic=monotonic,
        sleep=sleep,
    ) == [0, 1]
    assert handler.calls == 2
    assert handler.rpc_timeouts == pytest.approx([1, 0.74])
    if failure == "unavailable":
        assert handler.reconnect_timeouts == pytest.approx([0.75])
    else:
        assert handler.reconnect_timeouts == []


@pytest.mark.tags(CaseLabel.L0)
@pytest.mark.parametrize("failure", ["deadline-exceeded", "unexpected-status"])
def test_collect_query_ids_fails_fast_for_non_retryable_error(failure):
    class DeadlineExceededRpcError(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.DEADLINE_EXCEEDED

    if failure == "deadline-exceeded":
        error = DeadlineExceededRpcError()
    else:
        with pytest.raises(MilvusException) as generated_error:
            check_status(
                common_pb2.Status(
                    error_code=common_pb2.UnexpectedError,
                    reason="unexpected business failure",
                )
            )
        error = generated_error.value
        assert error.code == common_pb2.Success
        assert error.compatible_code == common_pb2.UnexpectedError

    class FakeHandler:
        def __init__(self):
            self.calls = 0
            self.reconnect_timeouts = []

        def raw_query(self, collection_name, expr, output_fields, timeout, context, **kwargs):
            self.calls += 1
            raise error

        def query(self, *args, **kwargs):
            raise AssertionError("the SDK retry wrapper must not be called")

        def reconnect(self, timeout):
            self.reconnect_timeouts.append(timeout)

    FakeHandler.query.__wrapped__ = FakeHandler.raw_query
    handler = FakeHandler()

    class FakeClient:
        def _get_connection(self):
            return handler

        def _generate_call_context(self, **kwargs):
            return "deadline-context"

        def _with_cluster_id(self, kwargs):
            kwargs["cluster_id"] = "cluster-a"
            return kwargs

    with pytest.raises(type(error)) as error_info:
        collect_query_ids(
            FakeClient(),
            "fail_fast_collection",
            total_rows=2,
            timeout=1,
            rpc_timeout=1,
            page_size=2,
        )

    assert error_info.value is error
    assert handler.calls == 1
    assert handler.reconnect_timeouts == []


@pytest.mark.tags(CaseLabel.L0)
def test_collect_query_ids_stops_when_reconnect_exhausts_deadline():
    now = 0.0

    def monotonic():
        return now

    class RetryableRpcError(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNAVAILABLE

    class FakeHandler:
        def __init__(self):
            self.calls = 0
            self.reconnect_timeouts = []

        def raw_query(self, collection_name, expr, output_fields, timeout, context, **kwargs):
            nonlocal now
            self.calls += 1
            assert timeout == pytest.approx(1)
            now += 0.25
            raise RetryableRpcError()

        def query(self, *args, **kwargs):
            raise AssertionError("the SDK retry wrapper must not be called")

        def reconnect(self, timeout):
            nonlocal now
            self.reconnect_timeouts.append(timeout)
            now += timeout

    FakeHandler.query.__wrapped__ = FakeHandler.raw_query
    handler = FakeHandler()

    class FakeClient:
        def _get_connection(self):
            return handler

        def _generate_call_context(self, **kwargs):
            return "deadline-context"

        def _with_cluster_id(self, kwargs):
            kwargs["cluster_id"] = "cluster-a"
            return kwargs

    with pytest.raises(TimeoutError, match="exceeded 1 seconds"):
        collect_query_ids(
            FakeClient(),
            "reconnect_deadline_collection",
            total_rows=2,
            timeout=1,
            rpc_timeout=1,
            page_size=2,
            monotonic=monotonic,
        )

    assert handler.calls == 1
    assert handler.reconnect_timeouts == pytest.approx([0.75])


@pytest.mark.tags(CaseLabel.L0)
def test_force_merge_machine_safe_size_uses_server_formula():
    def node(node_type, memory, deploy_mode="DISTRIBUTED", has_error=False):
        return {
            "infos": {
                "type": node_type,
                "has_error": has_error,
                "hardware_infos": {"memory": memory},
                "system_info": {"deploy_mode": deploy_mode},
            }
        }

    gib = 1024 * 1024 * 1024
    distributed = [node("querynode", 2 * gib), node("datanode", 4 * gib)]
    standalone = [node("querynode", 2 * gib, "STANDALONE"), node("datanode", 4 * gib, "STANDALONE")]
    pooling = [node("querynode", 2 * gib, "STANDALONE"), node("datanode", 0, "STANDALONE")]

    assert force_merge_machine_safe_size_mb(distributed, 4, 4) == 512
    assert force_merge_machine_safe_size_mb(distributed, 8, 4) == 256
    assert force_merge_machine_safe_size_mb(standalone, 4, 4) == 256
    assert force_merge_machine_safe_size_mb(pooling, 4, 4) == 512
    assert force_merge_machine_safe_size_mb([node("querynode", 2 * gib)], 4, 4) is None
    assert force_merge_machine_safe_size_mb(distributed + [node("querynode", 0, has_error=True)], 4, 4) is None


class TestMilvusClientForceMergeInvalid(TestMilvusClientV2Base):
    """Test cases for ForceMerge with invalid parameters"""

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target_size", [-1, -100])
    def test_force_merge_invalid_target_size_negative(self, target_size):
        """
        target: test ForceMerge with negative target_size
        method: create collection, call compact with negative target_size
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. compact with invalid target_size
        error = {ct.err_code: 1, ct.err_msg: "target_size"}
        self.compact(
            client,
            collection_name,
            target_size=target_size,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "target_size_ratio",
        [
            pytest.param(0.1, id="low"),
            pytest.param(0.5, id="half"),
            pytest.param(0.99, id="near-max"),
        ],
    )
    def test_force_merge_target_size_less_than_max_size(self, target_size_ratio):
        """
        target: test ForceMerge with target_size less than config maxSize
        method: read the server maxSize and call compact with a strictly smaller target_size
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. compact with target_size less than the active server maxSize
        segment_max_size_mb = get_segment_max_size_mb(client)
        assert segment_max_size_mb is not None and segment_max_size_mb > 1
        target_size = max(
            1,
            min(segment_max_size_mb - 1, int(segment_max_size_mb * target_size_ratio)),
        )
        error = {ct.err_code: 1100, ct.err_msg: "targetSize"}
        self.compact(
            client,
            collection_name,
            target_size=target_size,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_force_merge_nonexistent_collection(self):
        """
        target: test ForceMerge with non-existent collection
        method: call compact on a collection that doesn't exist
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 100, ct.err_msg: "collection not found"}
        self.compact(
            client,
            collection_name,
            target_size=2048,
            check_task=CheckTasks.err_res,
            check_items=error,
        )


class TestMilvusClientForceMergeValid(TestMilvusClientV2Base):
    """Test cases for ForceMerge with valid parameters"""

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L3)
    def test_manual_compaction_without_target_size(self):
        """
        target: test ordinary manual compaction when target_size is omitted
        method: create collection, insert data, flush, compact without target_size
        expected: Ordinary manual compaction completes successfully
        note: Omitting target_size does not select Force Merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
            }
            for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. compact without target_size; this is ordinary manual compaction
        compact_id = self.compact(client, collection_name)[0]
        assert self.wait_for_compaction_ready(client, compact_id, timeout=180)
        log.info("Manual compaction without target_size completed successfully")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_explicit_target_size(self):
        """
        target: test ForceMerge with explicit target_size (2048 MB)
        method: create collection, insert data, flush, compact with target_size=2048
        expected: Compaction completes successfully
        note: L3 - requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
            }
            for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. compact with explicit target_size
        target_size = 2048  # 2GB
        compact_id = self.compact(client, collection_name, target_size=target_size)[0]
        # 4. wait for compaction to complete
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info(f"ForceMerge with target_size={target_size}MB completed successfully")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_auto_target_size(self):
        """
        target: test ForceMerge with automatic target_size
        method: create collection, insert data, flush, compact with target_size above int64-safe MB threshold
        expected: Compaction completes successfully with auto-calculated optimal size
        note: L3 - requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
            }
            for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. compact with auto target_size
        compact_id = self.compact(client, collection_name, target_size=auto_target_size_mb)[0]
        # 4. wait for compaction to complete
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info("ForceMerge with auto target_size (max_int64) completed successfully")

    @pytest.mark.tags(CaseLabel.L1)
    def test_force_merge_empty_collection(self):
        """
        target: test ForceMerge on empty collection
        method: create collection, compact with target_size
        expected: Compaction completes successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection (empty)
        self.create_collection(client, collection_name, dim)
        # 2. compact with target_size on empty collection
        compact_id = self.compact(client, collection_name, target_size=2048)[0]
        # 3. wait for compaction to complete
        cost = 60
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info("ForceMerge on empty collection completed successfully")

    @pytest.mark.tags(CaseLabel.L1)
    def test_force_merge_with_multiple_segments(self):
        """
        target: verify ForceMerge rewrites a non-empty collection without losing data
        method: write five flush batches, force merge at least three active segments, then inspect plans and data
        expected: every input is replaced by a new target and all inserted primary keys remain queryable
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        self.create_collection(
            client,
            collection_name,
            dim,
            consistency_level="Strong",
            properties={"collection.autocompaction.enabled": "false"},
        )
        rng = np.random.default_rng()
        batch_size = default_nb
        num_batches = 5
        total_rows = batch_size * num_batches
        for batch in range(num_batches):
            vectors = rng.random((batch_size, dim), dtype=np.float32)
            rows = [
                {
                    default_primary_key_field_name: batch * batch_size + i,
                    default_vector_field_name: vectors[i].tolist(),
                }
                for i in range(batch_size)
            ]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)

        segments_before = self.wait_for_compaction_eligible_segments(
            client,
            collection_name,
            minimum_segment_count=num_batches,
        )
        source_ids = {segment.segment_id for segment in segments_before}
        assert len(source_ids) >= num_batches, (
            f"Expected at least {num_batches} sealed inputs after flushes, got {len(source_ids)}: {segments_before}"
        )

        compact_id = self.compact(client, collection_name, target_size=auto_target_size_mb)[0]
        self.wait_for_compaction_ready(client, compact_id, timeout=300)

        plans = client.get_compaction_plans(compact_id).plans
        assert plans, f"ForceMerge {compact_id} completed without a compaction plan"
        planned_source_counts = Counter(segment_id for plan in plans for segment_id in plan.sources)
        target_ids = {plan.target for plan in plans}
        assert set(planned_source_counts) == source_ids, (
            f"ForceMerge plans did not cover every source: expected={source_ids}, actual={set(planned_source_counts)}"
        )
        assert all(count == 1 for count in planned_source_counts.values())
        assert target_ids.isdisjoint(source_ids), (
            f"ForceMerge targets must be new segments: sources={source_ids}, targets={target_ids}"
        )

        segments_after = self.list_persistent_segments(client, collection_name)[0]
        output_ids = {segment.segment_id for segment in segments_after}
        assert output_ids == target_ids, (
            f"Active segments do not match ForceMerge targets: active={output_ids}, targets={target_ids}"
        )
        assert sum(segment.num_rows for segment in segments_after) == total_rows

        query_res = self.query(
            client,
            collection_name,
            filter=f"{default_primary_key_field_name} >= 0",
            output_fields=[default_primary_key_field_name],
            consistency_level="Strong",
        )[0]
        query_ids = [row[default_primary_key_field_name] for row in query_res]
        assert len(query_ids) == total_rows
        assert len(set(query_ids)) == total_rows
        assert set(query_ids) == set(range(total_rows))

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_search_after_merge(self):
        """
        target: test search works correctly after ForceMerge
        method: create collection, insert data, flush, compact, load, search
        expected: Search returns correct results
        note: L3 - requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data
        rng = np.random.default_rng(seed=19530)
        nb = default_nb
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
            }
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. compact with target_size
        target_size = 2048
        compact_id = self.compact(client, collection_name, target_size=target_size)[0]
        # 4. wait for compaction to complete
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        # 5. search
        search_vectors = rng.random((1, dim))
        search_res = self.search(
            client,
            collection_name,
            list(search_vectors),
            limit=10,
            output_fields=[default_primary_key_field_name],
        )[0]
        log.info(f"Search results: {search_res}")
        assert len(search_res) == 1
        assert len(search_res[0]) == 10
        log.info(f"Search after ForceMerge completed successfully with {len(search_res[0])} results")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_with_clustering_key(self):
        """
        target: test ForceMerge with clustering key
        method: create collection with clustering key, insert data, compact with is_clustering=True
        expected: Compaction completes successfully
        note: L3 - requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection with clustering key
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(
            default_primary_key_field_name,
            DataType.INT64,
            is_primary=True,
            auto_id=False,
        )
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(
            default_string_field_name,
            DataType.VARCHAR,
            max_length=64,
            is_clustering_key=True,
        )
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(
            client,
            collection_name,
            dimension=dim,
            schema=schema,
            index_params=index_params,
        )
        # 2. insert data
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
                default_string_field_name: f"str_{i}",
            }
            for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. compact with is_clustering=True and target_size
        target_size = 2048
        compact_id = self.compact(client, collection_name, is_clustering=True, target_size=target_size)[0]
        # 4. wait for compaction to complete
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id, is_clustering=True)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info("ForceMerge with clustering key completed successfully")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_verify_segment_count(self):
        """
        target: test ForceMerge reduces segment count
        method: create collection, insert data in batches, verify segment count before/after
        expected: Fewer segments after ForceMerge
        note: L3 - requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data in multiple batches
        rng = np.random.default_rng(seed=19530)
        batch_size = default_nb
        num_batches = 5
        for batch in range(num_batches):
            rows = [
                {
                    default_primary_key_field_name: batch * batch_size + i,
                    default_vector_field_name: list(rng.random((1, dim))[0]),
                }
                for i in range(batch_size)
            ]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)

        # 3. reload and get stable segment count before compaction
        assert self.wait_for_index_ready(client, collection_name, default_vector_field_name, timeout=300)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        segments_before = client.list_loaded_segments(collection_name)
        segment_count_before = len(segments_before)
        log.info(f"Segment count before ForceMerge: {segment_count_before}")

        # 4. compact with target_size
        target_size = 2048
        compact_id = self.compact(client, collection_name, target_size=target_size)[0]
        # 5. wait for compaction to complete
        cost = 300
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")

        # 6. wait for the compacted segment index, then reload to get updated segment info
        assert self.wait_for_index_ready(client, collection_name, default_vector_field_name, timeout=300)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        segments_after = client.list_loaded_segments(collection_name)
        segment_count_after = len(segments_after)
        log.info(f"Segment count after ForceMerge: {segment_count_after}")

        # 7. verify segment count reduced
        assert segment_count_after < segment_count_before, (
            f"Expected fewer segments after ForceMerge, got {segment_count_after} >= {segment_count_before}"
        )
        log.info(f"ForceMerge reduced segments from {segment_count_before} to {segment_count_after}")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_with_target_size_preserves_topology_and_data(self, minio_host, minio_bucket):
        """
        target: prove Force Merge honors explicit target_size without losing topology or data
        method: compact equivalent data with two valid target sizes and compare output topology
        expected: the smaller target creates more outputs; every plan target is active and every PK survives
        note: L3 - requires segment.maxSize == 64MB and MinIO access
        """
        client = self._client()
        dim = 1024
        batch_size = 2000
        num_batches = 18
        total_rows = batch_size * num_batches
        minio_client = new_minio_client(minio_host)
        assert minio_client.bucket_exists(minio_bucket), f"MinIO bucket {minio_bucket!r} does not exist"
        segment_max_size_mb = get_segment_max_size_mb(client)
        assert segment_max_size_mb is not None and segment_max_size_mb > 0
        if segment_max_size_mb != 64:
            pytest.skip(
                f"comparative target-size fixture requires segment.maxSize == 64MB, got {segment_max_size_mb}MB"
            )

        # With this fixture's ~140MiB MemorySize, 64/128MiB targets produce
        # distinct output counts in the PR-gate topology (at most two
        # QueryNodes per replica/shard). The machine-safe-size check below
        # prevents the server from legally shrinking the larger target.
        small_target_mb = segment_max_size_mb
        large_target_mb = segment_max_size_mb * 2
        required_machine_safe_size_mb = large_target_mb * (1 + force_merge_target_size_tolerance)
        machine_safe_size_mb = get_force_merge_machine_safe_size_mb(client)
        if machine_safe_size_mb is None or machine_safe_size_mb < required_machine_safe_size_mb:
            pytest.skip(
                f"comparative target-size fixture requires machineSafeSize >= "
                f"{required_machine_safe_size_mb}MB, got {machine_safe_size_mb}MB"
            )

        def run_force_merge(target_size_mb):
            collection_name = cf.gen_unique_str(f"{prefix}_{target_size_mb}")
            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(
                default_vector_field_name,
                index_type="FLAT",
                metric_type="COSINE",
            )
            self.create_collection(
                client,
                collection_name,
                dim,
                index_params=index_params,
                properties={"collection.autocompaction.enabled": "false"},
            )
            rng = np.random.default_rng(seed=19530)
            for batch in range(num_batches):
                vectors = rng.random((batch_size, dim), dtype=np.float32)
                rows = [
                    {
                        default_primary_key_field_name: batch * batch_size + index,
                        default_vector_field_name: vectors[index].tolist(),
                    }
                    for index in range(batch_size)
                ]
                self.insert(client, collection_name, rows)
                self.flush(client, collection_name)

            segments_before = self.wait_for_compaction_eligible_segments(
                client,
                collection_name,
                minimum_segment_count=2,
                timeout=180,
            )
            raw_source_ids = [segment.segment_id for segment in segments_before]
            source_ids = set(raw_source_ids)
            assert len(raw_source_ids) == len(source_ids), f"Duplicate ForceMerge input IDs: {segments_before}"
            description = self.describe_collection(client, collection_name)[0]
            collection_id = description["collection_id"]
            input_sizes = get_insert_log_sizes(minio_client, minio_bucket, collection_id, source_ids)
            total_input_size = sum(input_sizes.values())

            latest_machine_safe_size_mb = get_force_merge_machine_safe_size_mb(client)
            if latest_machine_safe_size_mb is None or latest_machine_safe_size_mb < required_machine_safe_size_mb:
                pytest.skip(
                    f"machineSafeSize became insufficient before compaction: required >= "
                    f"{required_machine_safe_size_mb}MB, got {latest_machine_safe_size_mb}MB"
                )
            compact_id = self.compact(client, collection_name, target_size=target_size_mb)[0]
            assert self.wait_for_compaction_ready(client, compact_id, timeout=600)

            plans = client.get_compaction_plans(compact_id).plans
            assert plans, f"ForceMerge {compact_id} completed without plans"
            assert all(len(plan.sources) == len(set(plan.sources)) for plan in plans), plans
            planned_source_counts = Counter(segment_id for plan in plans for segment_id in plan.sources)
            assert set(planned_source_counts) == source_ids, (
                f"Force Merge plans must cover every input: expected={source_ids}, "
                f"actual={set(planned_source_counts)}, plans={plans}"
            )
            assert all(count == 1 for count in planned_source_counts.values()), (
                f"Force Merge source IDs must occur in exactly one plan: {planned_source_counts}"
            )
            raw_target_ids = [plan.target for plan in plans]
            target_ids = set(raw_target_ids)
            assert len(raw_target_ids) == len(target_ids), f"ForceMerge targets must be unique: {plans}"
            assert target_ids.isdisjoint(source_ids), (
                f"ForceMerge targets must be new segments: sources={source_ids}, targets={target_ids}"
            )

            segments_after = self.list_persistent_segments(client, collection_name)[0]
            raw_output_ids = [segment.segment_id for segment in segments_after]
            output_ids = set(raw_output_ids)
            assert len(raw_output_ids) == len(output_ids), f"Duplicate active ForceMerge outputs: {segments_after}"
            assert output_ids.isdisjoint(source_ids), (
                f"Completed ForceMerge must retire every source: sources={source_ids}, outputs={output_ids}"
            )
            # CompactionMergeInfo has one legacy `target` field, while one
            # ForceMerge task may split into several result segments. The API
            # therefore exposes only ResultSegments[0] for a split task.
            assert target_ids.issubset(output_ids), (
                f"Every target exposed by the plan must be active: active={output_ids}, targets={target_ids}"
            )
            assert sum(segment.num_rows for segment in segments_after) == total_rows

            query_ids = collect_query_ids(client, collection_name, total_rows)
            assert len(query_ids) == total_rows
            assert len(set(query_ids)) == total_rows
            assert set(query_ids) == set(range(total_rows))

            output_sizes = get_insert_log_sizes(minio_client, minio_bucket, collection_id, output_ids)
            total_output_size = sum(output_sizes.values())
            rewrite_delta = abs(total_output_size - total_input_size) / total_input_size
            assert rewrite_delta <= actual_output_size_tolerance, (
                f"Rewrite changed total insert-log bytes by {rewrite_delta:.2%}: "
                f"before={total_input_size}, after={total_output_size}"
            )
            log.info(
                f"ForceMerge target={target_size_mb}MB: inputs={input_sizes}, outputs={output_sizes}, plans={plans}"
            )
            return output_ids, target_ids

        small_output_ids, _ = run_force_merge(small_target_mb)
        large_output_ids, _ = run_force_merge(large_target_mb)
        assert len(small_output_ids) == 3, (
            f"64MB target should produce 3 outputs from the fixed fixture, got {small_output_ids}"
        )
        assert len(large_output_ids) == 2, (
            f"128MB target should produce 2 outputs from the fixed fixture, got {large_output_ids}"
        )
        assert len(small_output_ids) > len(large_output_ids), (
            f"Explicit target_size did not change output topology: "
            f"target={small_target_mb}MB produced {len(small_output_ids)}, "
            f"target={large_target_mb}MB produced {len(large_output_ids)}"
        )

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_max_int64_overflow(self):
        """
        target: test ForceMerge with huge targetSize doesn't cause overflow
        method: create collection, insert data, compact with target_size above int64-safe MB threshold
        expected: Compaction completes successfully (auto-calculate mode)
        note: L3 - This verifies PR #47327 fix for integer overflow when
              targetSizeBytes = targetSize * 1024 * 1024
              Requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data in batches to create segments
        rng = np.random.default_rng(seed=19530)
        for batch in range(3):
            rows = [
                {
                    default_primary_key_field_name: batch * 1000 + i,
                    default_vector_field_name: list(rng.random((1, dim))[0]),
                }
                for i in range(1000)
            ]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            log.info(f"Inserted batch {batch + 1}/3")
        # 3. compact with huge target_size (should trigger auto-calculate mode)
        # Before fix: would fail with overflow error
        # After fix: should succeed
        compact_id = self.compact(client, collection_name, target_size=auto_target_size_mb)[0]
        # 4. wait for compaction to complete
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info("ForceMerge with max_int64 target_size completed (overflow fix verified)")
