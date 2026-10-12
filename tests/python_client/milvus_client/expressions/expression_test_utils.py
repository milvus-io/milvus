import math
import time

import pytest
from common.common_type import CheckTasks
from pymilvus import DataType
from pymilvus.client.types import IndexState
from pymilvus.grpc_gen.common_pb2 import SegmentState


def query_ids(testcase, client, collection_name, expr, pk_field="id"):
    res = testcase.query(
        client,
        collection_name,
        filter=expr,
        output_fields=[pk_field],
        check_task=CheckTasks.check_nothing,
    )[0]
    if hasattr(res, "message") and res.message:
        pytest.fail(f"query errored for expression `{expr}`: {res.message}")
    return sorted(row[pk_field] for row in res)


def query_id_offsets(testcase, client, collection_name, expr, pk_field="id"):
    res = testcase.query(
        client,
        collection_name,
        filter=expr,
        output_fields=[pk_field],
        check_task=CheckTasks.check_nothing,
    )[0]
    if hasattr(res, "message") and res.message:
        pytest.fail(f"query errored for expression `{expr}`: {res.message}")
    assert all("offset" in row for row in res), f"{expr} returned rows without element offsets: {res}"
    return sorted((row[pk_field], row["offset"]) for row in res)


def assert_query_ids(testcase, client, collection_name, expr, expected_ids, pk_field="id"):
    actual_ids = query_ids(testcase, client, collection_name, expr, pk_field)
    assert actual_ids == sorted(expected_ids), f"{expr} got {actual_ids}, expected {expected_ids}"


def assert_query_id_offsets(testcase, client, collection_name, expr, expected_id_offsets, pk_field="id"):
    actual_id_offsets = query_id_offsets(testcase, client, collection_name, expr, pk_field)
    assert actual_id_offsets == sorted(expected_id_offsets), (
        f"{expr} got id/offset pairs {actual_id_offsets}, expected {expected_id_offsets}"
    )


def assert_same_query_result(
    testcase,
    client,
    collection_name,
    left_expr,
    right_expr,
    expected_ids,
    pk_field="id",
):
    left_ids = query_ids(testcase, client, collection_name, left_expr, pk_field)
    right_ids = query_ids(testcase, client, collection_name, right_expr, pk_field)
    expected_ids = sorted(expected_ids)
    assert left_ids == expected_ids, f"{left_expr} got {left_ids}, expected {expected_ids}"
    assert right_ids == expected_ids, f"{right_expr} got {right_ids}, expected {expected_ids}"
    assert left_ids == right_ids, f"permutation mismatch: left={left_ids}, right={right_ids}"


def add_minimal_query_vector_field(schema, vector_field="vector", dim=8):
    schema.add_field(vector_field, DataType.FLOAT_VECTOR, dim=dim, nullable=True)


def create_minimal_vector_index(testcase, client, collection_name, vector_field="vector"):
    index_params = testcase.prepare_index_params(client)[0]
    index_params.add_index(vector_field, index_type="FLAT", metric_type="COSINE")
    testcase.create_index(client, collection_name, index_params=index_params)


def assert_search_result_shape(result, metric_type):
    assert len(result) == 1, f"expected one nq result group, got {len(result)}"
    hits = result[0]
    distances = [hit["distance"] for hit in hits]
    assert all(math.isfinite(distance) for distance in distances), f"non-finite distances: {distances}"

    metric_type = metric_type.upper()
    if metric_type == "L2":
        assert all(distance >= 0 for distance in distances), f"L2 distances must be non-negative: {distances}"
        descending = False
    elif metric_type == "COSINE":
        tolerance = 1e-6
        assert all(-1 - tolerance <= distance <= 1 + tolerance for distance in distances), (
            f"COSINE distances out of range: {distances}"
        )
        descending = True
    elif metric_type == "RANKER":
        assert all(0 <= distance <= 1 for distance in distances), f"ranker scores out of range: {distances}"
        descending = True
    else:
        raise AssertionError(f"unsupported metric oracle: {metric_type}")

    assert distances == sorted(distances, reverse=descending), f"unexpected distance order: {distances}"
    return hits


def wait_for_materialized_index(
    testcase,
    client,
    collection_name,
    index_name,
    expected_total_rows,
    expected_field_name,
    expected_index_type,
    timeout=240,
    interval=2,
):
    deadline = time.time() + timeout
    last_index_info = None
    while time.time() < deadline:
        index_info = testcase.describe_index(client, collection_name, index_name=index_name)[0]
        last_index_info = index_info
        state = index_info.get("state")
        total_rows = index_info.get("total_rows")
        indexed_rows = index_info.get("indexed_rows")
        pending_index_rows = index_info.get("pending_index_rows")
        assert index_info.get("index_name") == index_name, (
            f"{collection_name}/{index_name}: unexpected index identity {index_info}"
        )
        assert index_info.get("field_name") == expected_field_name, (
            f"{collection_name}/{index_name}: expected field {expected_field_name}, got {index_info}"
        )
        assert index_info.get("index_type") == expected_index_type, (
            f"{collection_name}/{index_name}: expected type {expected_index_type}, got {index_info}"
        )
        if state in {"Failed", "Deleted"}:
            raise AssertionError(f"{collection_name}/{index_name}: index entered terminal state {state}: {index_info}")
        if (
            state == IndexState.Finished.name
            and pending_index_rows is not None
            and pending_index_rows == 0
            and total_rows == expected_total_rows
            and indexed_rows == total_rows
        ):
            return index_info
        time.sleep(interval)

    raise AssertionError(
        f"{collection_name}/{index_name}: index was not fully materialized within {timeout}s; "
        f"expected state={IndexState.Finished.name}, total_rows=indexed_rows={expected_total_rows}, "
        f"and zero pending rows, got {last_index_info}"
    )


def register_collection_cleanup(testcase, request, alias, collection_name):
    def teardown():
        client = testcase._client(alias=alias)
        if client is not None and client.has_collection(collection_name):
            client.drop_collection(collection_name)

    request.addfinalizer(teardown)


def prepare_loaded_empty_collection_for_segment(
    testcase,
    client,
    collection_name,
    schema,
    vector_field="vector",
):
    testcase.create_collection(client, collection_name, schema=schema, consistency_level="Strong")
    create_minimal_vector_index(testcase, client, collection_name, vector_field=vector_field)
    testcase.load_collection(client, collection_name)


def insert_by_segment_mode(testcase, client, collection_name, first_batch, second_batch, segment_mode):
    if segment_mode == "growing":
        testcase.insert(client, collection_name, data=first_batch + second_batch)
    elif segment_mode == "sealed":
        testcase.insert(client, collection_name, data=first_batch + second_batch)
        testcase.flush(client, collection_name)
    elif segment_mode == "mixed":
        testcase.insert(client, collection_name, data=first_batch)
        testcase.flush(client, collection_name)
        wait_for_segment_mode(
            client,
            collection_name,
            "sealed",
            expected_row_count=len(first_batch),
        )
        testcase.insert(client, collection_name, data=second_batch)
    else:
        raise ValueError(f"unsupported segment mode: {segment_mode}")


def get_query_segment_states(client, collection_name):
    segments = client._get_connection().get_query_segment_info(collection_name)
    return [(segment.segmentID, segment.state, segment.num_rows) for segment in segments]


def get_visible_row_count(client, collection_name):
    result = client.query(collection_name, filter="", output_fields=["count(*)"])
    return result[0]["count(*)"]


def wait_for_segment_mode(
    client,
    collection_name,
    segment_mode,
    expected_row_count,
    expected_sealed_rows=None,
    timeout=60,
    interval=0.5,
):
    deadline = time.time() + timeout
    last_states = []
    last_visible_count = None

    while time.time() < deadline:
        states = get_query_segment_states(client, collection_name)
        last_states = states
        last_visible_count = get_visible_row_count(client, collection_name)
        sealed_rows = sum(num_rows for _, state, num_rows in states if state == SegmentState.Sealed)

        if segment_mode == "sealed" and states and last_visible_count == expected_row_count:
            if all(state == SegmentState.Sealed for _, state, _ in states) and sealed_rows == expected_row_count:
                return states

        if segment_mode == "growing" and last_visible_count == expected_row_count:
            # get_query_segment_info exposes sealed segments but not growing ones.
            # Visible rows with zero sealed rows therefore prove the growing path.
            if sealed_rows == 0:
                return states

        if segment_mode == "mixed" and last_visible_count == expected_row_count:
            assert expected_sealed_rows is not None, "mixed mode requires expected_sealed_rows"
            if sealed_rows == expected_sealed_rows and sealed_rows < expected_row_count:
                return states

        if segment_mode not in {"sealed", "growing", "mixed"}:
            raise ValueError(f"unsupported segment mode: {segment_mode}")

        time.sleep(interval)

    raise AssertionError(
        f"{collection_name}: timed out waiting for segment_mode={segment_mode}; "
        f"expected_row_count={expected_row_count}, expected_sealed_rows={expected_sealed_rows}, "
        f"last_visible_count={last_visible_count}, last query segment states={last_states}"
    )
