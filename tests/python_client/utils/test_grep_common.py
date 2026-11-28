"""Offline checks for the independent Grep oracle and acceptance assertions."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from common.grep_common import (
    assert_results,
    basic_rows,
    context_line,
    detail,
    expected_literal,
    literal_details,
    projected_rows,
    sparse_rows,
)
from milvus_client import test_milvus_client_grep_acceptance as grep_e2e
from pymilvus.grpc_gen.common_pb2 import SegmentState


@pytest.mark.parametrize(
    "text,expected",
    [
        (None, []),
        ("", []),
        ("\n", [detail("", 1, 0, [(0, 0)])]),
        ("\n\n", [detail("", 1, 0, [(0, 0)]), detail("", 2, 1, [(0, 0)])]),
        ("é中\n", [detail("é中", 1, 0, [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)])]),
    ],
)
def test_empty_pattern_byte_offsets(text, expected):
    assert literal_details(text, "") == expected


def test_manually_reviewed_coordinates():
    assert literal_details("info\n中文 error error\nend", "error") == [
        detail("中文 error error", 2, 5, [(7, 12), (13, 18)]),
    ]
    assert literal_details("x\r\nerror\r\n", "error") == [detail("error\r", 2, 3, [(0, 5)])]
    assert literal_details("error\nerror", "error") == [
        detail("error", 1, 0, [(0, 5)]),
        detail("error", 2, 6, [(0, 5)]),
    ]
    assert literal_details("aaa", "aa") == [detail("aaa", 1, 0, [(0, 2)])]
    assert literal_details("foo\nbar", "foobar") == []


def test_reviewed_hit_sets():
    assert set(expected_literal(basic_rows(), "error")) == {1, 3, 4, 5, 6, 11, 12}
    assert set(expected_literal(sparse_rows(), "error")) == {128, 255, 256, 383, 384}


@pytest.mark.parametrize("mutation", ["extra_spans", "duplicate_context", "wrong_offset", "wrong_type", "missing_type"])
def test_context_line_response_contract(mutation):
    rows = [{"id": 1, "content": "错🙂\n\nerr"}]
    expected = {1: [context_line("", 2, 8), detail("err", 3, 9, [(0, 3)])]}
    actual = deepcopy(expected[1])
    response = [{"id": 1, "matches": actual}]
    assert_results(response, rows, expected, ["id"])
    if mutation == "extra_spans":
        actual[0].update(starts=[], ends=[])
    elif mutation == "duplicate_context":
        actual.insert(0, deepcopy(actual[0]))
    elif mutation == "wrong_offset":
        actual[0]["line_start_offset"] = 3
    elif mutation == "wrong_type":
        actual[0]["type"] = "match"
    else:
        del actual[1]["type"]
    with pytest.raises(AssertionError):
        assert_results(response, rows, expected, ["id"])


@pytest.fixture
def segment_state_client():
    segment = SimpleNamespace(segment_id=7, state=SegmentState.Sealed, num_rows=2)
    client = Mock()
    client.list_loaded_segments.return_value = [segment]
    client.list_persistent_segments.return_value = [segment]
    client.query.return_value = [{"id": 1}, {"id": 2}]
    return client


def test_confirm_state_waits_for_sealed_segment_loading(monkeypatch, segment_state_client):
    loaded = segment_state_client.list_loaded_segments.return_value
    segment_state_client.list_loaded_segments.side_effect = [[], loaded]
    clock = SimpleNamespace(monotonic=Mock(return_value=0), sleep=Mock())
    monkeypatch.setattr(grep_e2e, "time", clock)

    grep_e2e.confirm_state(segment_state_client, "test", [{"id": 1}, {"id": 2}], {1, 2}, wait=True)

    clock.sleep.assert_called_once()
    assert [call.kwargs["ignore_growing"] for call in segment_state_client.query.call_args_list] == [False, True]


def test_confirm_state_fails_when_ready_sealed_query_omits_rows(segment_state_client):
    segment_state_client.query.side_effect = [[{"id": 1}, {"id": 2}], []]

    with pytest.raises(pytest.fail.Exception, match=r"Sealed query result mismatch.*missing IDs=\[1, 2\]"):
        grep_e2e.confirm_state(segment_state_client, "test", [{"id": 1}, {"id": 2}], {1, 2})


def test_confirm_state_retries_post_check_during_segment_replacement(monkeypatch, segment_state_client):
    old = segment_state_client.list_loaded_segments.return_value
    new = [SimpleNamespace(segment_id=8, state=SegmentState.Sealed, num_rows=2)]
    segment_state_client.list_loaded_segments.side_effect = [old, new]
    segment_state_client.list_persistent_segments.return_value = new
    clock = SimpleNamespace(monotonic=Mock(return_value=0), sleep=Mock())
    monkeypatch.setattr(grep_e2e, "time", clock)

    grep_e2e.confirm_state(segment_state_client, "test", [{"id": 1}, {"id": 2}], {1, 2})

    clock.sleep.assert_called_once()


def test_confirm_state_fails_when_expected_sealed_segment_never_loads(monkeypatch, segment_state_client):
    segment_state_client.list_loaded_segments.return_value = []
    clock = SimpleNamespace(monotonic=Mock(side_effect=[0, grep_e2e.TIMEOUT]), sleep=Mock())
    monkeypatch.setattr(grep_e2e, "time", clock)

    with pytest.raises(pytest.fail.Exception, match="Target segment layout unavailable"):
        grep_e2e.confirm_state(segment_state_client, "test", [{"id": 1}, {"id": 2}], {1, 2}, wait=True)


def test_confirm_state_skips_when_growing_data_is_already_sealed(segment_state_client):
    with pytest.raises(pytest.skip.Exception, match="Growing data was sealed"):
        grep_e2e.confirm_state(segment_state_client, "test", [{"id": 1}, {"id": 2}], set())


def response_fixture():
    rows = basic_rows()
    expected = expected_literal(rows, "error")
    response = [
        {"id": row["id"], "path": row["path"], "matches": deepcopy(expected[row["id"]])}
        for row in rows
        if row["id"] in expected
    ]
    return rows, expected, response


def test_page_does_not_assume_order_or_selected_ids():
    rows, expected, response = response_fixture()
    assert_results(response[::-1], rows, expected, ["id", "path"])
    assert_results(response[-2:], rows, expected, ["id", "path"], count=2)
    assert_results([], rows, expected, ["id", "path"], count=0)


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "wrong_field", "offset", "span", "string_json"])
def test_assertion_rejects_corrupt_responses(mutation):
    rows, expected, response = response_fixture()
    if mutation == "missing":
        response.pop()
    elif mutation == "duplicate":
        response.append(deepcopy(response[0]))
    elif mutation == "wrong_field":
        response[0]["path"] = "src/wrong.py"
    elif mutation == "offset":
        response[0]["matches"][0]["line_start_offset"] = 1
    elif mutation == "span":
        response[0]["matches"][0]["starts"] = [1]
    else:
        response[0]["matches"] = "[]"
    with pytest.raises(AssertionError):
        assert_results(response, rows, expected, ["id", "path"])


@pytest.mark.parametrize("mutation", ["vector_swap", "boolean_scalar", "float_line_number", "float_offset"])
def test_projection_or_coordinate_type_corruption(mutation):
    rows = projected_rows()
    expected = expected_literal(rows, "error")
    fields = ["id", "score", "vector"]
    response = [
        {**{field: deepcopy(row[field]) for field in fields}, "matches": deepcopy(expected[row["id"]])}
        for row in rows
        if row["id"] in expected
    ]
    assert_results(response, rows, expected, fields)
    if mutation == "vector_swap":
        response[0]["vector"], response[1]["vector"] = response[1]["vector"], response[0]["vector"]
    elif mutation == "boolean_scalar":
        response[1]["score"] = True  # Python's True == 1 must not mask a wrong SDK type.
    elif mutation == "float_line_number":
        response[0]["matches"][0]["line_number"] = 2.0
    else:
        response[0]["matches"][0]["line_start_offset"] = 7.0
    with pytest.raises(AssertionError):
        assert_results(response, rows, expected, fields)


def test_projection_names_and_filter_only_export():
    rows, expected, response = response_fixture()
    custom = [{"id": row["id"], "path": row["path"], "text_hits": row["matches"]} for row in response]
    assert_results(custom, rows, expected, ["id", "path"], match_field="text_hits")
    no_details = [{"id": row["id"], "path": row["path"]} for row in response]
    assert_results(no_details, rows, expected, ["id", "path"], match_field=None)
    with pytest.raises(AssertionError):
        assert_results(response, rows, expected, ["id", "path"], match_field=None)


@pytest.mark.parametrize("mutation", ["short_page", "unknown_pk", "missing_field", "extra_field"])
def test_page_and_projection_corruption(mutation):
    rows, expected, response = response_fixture()
    response = response[:2]
    if mutation == "short_page":
        response.pop()
    elif mutation == "unknown_pk":
        response[0]["id"] = 99999
    elif mutation == "missing_field":
        del response[0]["path"]
    else:
        response[0]["unexpected"] = True
    with pytest.raises(AssertionError):
        assert_results(response, rows, expected, ["id", "path"], count=2)
