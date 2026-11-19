"""Server-independent regressions for compaction candidate index readiness."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from common import compaction_utils
from pymilvus import MilvusException
from pymilvus.grpc_gen import common_pb2

pytestmark = pytest.mark.tags("CompactionIntegrityUnit")


def _compaction_candidate_response(segment_ids=(1, 2, 3)):
    return SimpleNamespace(
        status=common_pb2.Status(),
        infos=[
            SimpleNamespace(
                segmentID=segment_id,
                collectionID=1,
                num_rows=100,
                is_sorted=True,
                state=common_pb2.SegmentState.Flushed,
                level=common_pb2.SegmentLevel.L1,
                storage_version=2,
            )
            for segment_id in segment_ids
        ],
    )


def _compaction_index_response(state=common_pb2.Finished, total_rows=300, indexed_rows=300, pending_rows=0):
    return SimpleNamespace(
        status=common_pb2.Status(),
        index_descriptions=[
            SimpleNamespace(
                index_name="vector",
                state=state,
                total_rows=total_rows,
                indexed_rows=indexed_rows,
                pending_index_rows=pending_rows,
                index_state_fail_reason="index worker failed",
            )
        ],
    )


@pytest.fixture
def compaction_index_wait(monkeypatch):
    clock = SimpleNamespace(now=0.0)

    def advance(seconds):
        clock.now += seconds

    monkeypatch.setattr(compaction_utils, "time", SimpleNamespace(monotonic=lambda: clock.now, sleep=advance))
    stub = SimpleNamespace(
        GetPersistentSegmentInfo=Mock(side_effect=lambda *args, **kwargs: _compaction_candidate_response()),
        DescribeIndex=Mock(side_effect=lambda *args, **kwargs: _compaction_index_response()),
    )
    handler = SimpleNamespace(_stub=stub)

    def wait(**kwargs):
        return compaction_utils.wait_for_compaction_eligible_segments(
            handler, "candidate_collection", minimum_segment_count=3, timeout=5, poll_interval=1, **kwargs
        )

    return clock, stub, wait


@pytest.mark.parametrize(
    "not_ready",
    [
        pytest.param(_compaction_index_response(state=common_pb2.InProgress, pending_rows=300), id="building"),
        pytest.param(_compaction_index_response(pending_rows=300), id="finished-ancestor-has-pending-sources"),
        pytest.param(_compaction_index_response(total_rows=0, indexed_rows=0), id="stale-empty-progress"),
        pytest.param(_compaction_index_response(total_rows=600, indexed_rows=600), id="stale-row-overcount"),
        pytest.param(_compaction_index_response(indexed_rows=200), id="incomplete-row-coverage"),
        pytest.param(SimpleNamespace(status=common_pb2.Status(), index_descriptions=[]), id="missing-description"),
    ],
)
def test_compaction_candidates_wait_for_index_coverage(compaction_index_wait, not_ready):
    clock, stub, wait = compaction_index_wait
    stub.DescribeIndex.side_effect = [not_ready, _compaction_index_response(), _compaction_index_response()]

    result = wait(index_name="vector")

    assert [segment.segment_id for segment in result] == [1, 2, 3]
    assert clock.now == 2
    assert stub.DescribeIndex.call_count == 3
    for call in stub.DescribeIndex.call_args_list:
        assert call.args[0].collection_name == "candidate_collection"
        assert call.args[0].index_name == "vector"


def test_compaction_candidates_restart_stability_after_index_regresses(compaction_index_wait):
    clock, stub, wait = compaction_index_wait
    stub.DescribeIndex.side_effect = [
        _compaction_index_response(),
        _compaction_index_response(pending_rows=100),
        _compaction_index_response(),
        _compaction_index_response(),
    ]

    wait(index_name="vector")

    assert stub.DescribeIndex.call_count == 4
    assert clock.now == 3


def test_compaction_candidates_restart_stability_for_replaced_sources(compaction_index_wait):
    clock, stub, wait = compaction_index_wait
    stub.GetPersistentSegmentInfo.side_effect = [
        _compaction_candidate_response(),
        _compaction_candidate_response((4, 5, 6)),
        _compaction_candidate_response((4, 5, 6)),
    ]

    result = wait(index_name="vector")

    assert [segment.segment_id for segment in result] == [4, 5, 6]
    assert stub.DescribeIndex.call_count == 3
    assert clock.now == 2


def test_compaction_candidates_index_failure_is_immediate(compaction_index_wait):
    clock, stub, wait = compaction_index_wait
    stub.DescribeIndex.side_effect = [_compaction_index_response(state=common_pb2.Failed)]

    with pytest.raises(AssertionError, match="Index vector.*failed: index worker failed"):
        wait(index_name="vector")

    assert stub.DescribeIndex.call_count == 1
    assert clock.now == 0


def test_compaction_candidates_pending_index_times_out_with_progress(compaction_index_wait):
    clock, stub, wait = compaction_index_wait
    stub.DescribeIndex.side_effect = lambda *args, **kwargs: _compaction_index_response(
        state=common_pb2.InProgress, indexed_rows=100, pending_rows=200
    )

    with pytest.raises(AssertionError, match="index vector:.*InProgress.*pending_index_rows.*200"):
        wait(index_name="vector")

    assert clock.now == 5
    assert stub.DescribeIndex.call_count == 5


def test_compaction_candidates_reject_late_index_completion(compaction_index_wait):
    clock, stub, wait = compaction_index_wait
    timeouts = []

    def describe(request, timeout, metadata):
        timeouts.append(timeout)
        clock.now += 1 if len(timeouts) == 1 else timeout + 0.01
        return _compaction_index_response()

    stub.DescribeIndex.side_effect = describe

    with pytest.raises(AssertionError, match="did not become stable compaction candidates.*5 seconds"):
        wait(index_name="vector")

    assert timeouts == [5, 3]
    assert clock.now == pytest.approx(5.01)


def test_compaction_candidates_do_not_retry_unrelated_index_errors(compaction_index_wait):
    clock, stub, wait = compaction_index_wait
    stub.DescribeIndex.side_effect = MilvusException(code=1100, message="invalid index request")

    with pytest.raises(MilvusException, match="invalid index request"):
        wait(index_name="vector")

    assert stub.DescribeIndex.call_count == 1
    assert clock.now == 0


def test_compaction_candidates_index_gate_is_opt_in(compaction_index_wait):
    clock, stub, wait = compaction_index_wait
    stub.DescribeIndex.side_effect = AssertionError("legacy caller must not describe indexes")

    assert len(wait()) == 3
    stub.DescribeIndex.assert_not_called()
    assert clock.now == 1
