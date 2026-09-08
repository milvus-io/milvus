from types import SimpleNamespace

import grpc
import pytest
from base import client_v2_base
from common import compaction_utils
from common.common_type import CaseLabel
from common.compaction_utils import (
    CompactionStateInfo,
    get_compaction_state_info,
    wait_for_compaction_completed,
)
from pymilvus.client.call_context import CallContext
from pymilvus.grpc_gen import common_pb2
from pymilvus.grpc_gen import milvus_pb2 as milvus_types


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class RetryableRpcError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "transient unavailable"


@pytest.mark.tags(CaseLabel.L0)
class TestWaitForCompactionCompleted:
    def test_noop_id_completes_without_rpc(self):
        rpc_called = False

        def get_state(compact_id, rpc_timeout):
            nonlocal rpc_called
            rpc_called = True
            return CompactionStateInfo(state="Completed")

        assert wait_for_compaction_completed(get_state, compact_id=-1, timeout=10)
        assert rpc_called is False

    def test_returns_after_completed_state(self):
        clock = FakeClock()
        states = iter(
            [
                CompactionStateInfo(state="Executing", executing_plan_no=1),
                CompactionStateInfo(state="Executing", executing_plan_no=1),
                CompactionStateInfo(state="Completed", completed_plan_no=1),
            ]
        )

        result = wait_for_compaction_completed(
            get_state=lambda compact_id, rpc_timeout: next(states),
            compact_id=42,
            timeout=10,
            poll_interval=1,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

        assert result is True
        assert clock.now == 2

    def test_accepts_completed_target_without_materialized_plan(self):
        assert wait_for_compaction_completed(
            get_state=lambda compact_id, rpc_timeout: CompactionStateInfo(state="Completed"),
            compact_id=42,
            timeout=10,
        )

    def test_fails_with_last_state_on_timeout(self):
        clock = FakeClock()
        rpc_timeouts = []

        def get_state(compact_id, rpc_timeout):
            rpc_timeouts.append(rpc_timeout)
            return CompactionStateInfo(state="Executing", executing_plan_no=1)

        with pytest.raises(
            AssertionError,
            match=r"compaction 42.*3 seconds.*last state: Executing.*"
            r"state history: \[CompactionStateInfo\(state='Executing'.*CompactionStateInfo\(state='Executing'.*"
            r"CompactionStateInfo\(state='Executing'",
        ):
            wait_for_compaction_completed(
                get_state=get_state,
                compact_id=42,
                timeout=3,
                poll_interval=1,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )

        assert clock.now == 3
        assert rpc_timeouts == [3, 2, 1]

    def test_rejects_completed_state_returned_after_deadline(self):
        clock = FakeClock()

        def get_state(compact_id, rpc_timeout):
            clock.now += rpc_timeout + 1
            return CompactionStateInfo(state="Completed", completed_plan_no=1)

        with pytest.raises(AssertionError, match=r"compaction 42.*10 seconds.*last state: Completed"):
            wait_for_compaction_completed(
                get_state=get_state,
                compact_id=42,
                timeout=10,
                poll_interval=1,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )

    @pytest.mark.parametrize("terminal_state", ["UndefiedState", "UndefinedState"])
    def test_fails_immediately_for_undefined_state(self, terminal_state):
        clock = FakeClock()

        with pytest.raises(
            AssertionError,
            match=(
                rf"compaction 42.*unsuccessful terminal state: {terminal_state}.*"
                rf"state history: \[CompactionStateInfo\(state='{terminal_state}'"
            ),
        ):
            wait_for_compaction_completed(
                get_state=lambda compact_id, rpc_timeout: CompactionStateInfo(state=terminal_state),
                compact_id=42,
                timeout=3,
                poll_interval=1,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )

        assert clock.now == 0

    @pytest.mark.parametrize(
        "terminal_state",
        [
            CompactionStateInfo(state="Completed", failed_plan_no=1),
            CompactionStateInfo(state="Completed", timeout_plan_no=1),
            CompactionStateInfo(state="Completed", failed_plan_no=2, timeout_plan_no=3),
            CompactionStateInfo(state="Executing", executing_plan_no=1, failed_plan_no=1),
        ],
        ids=["failed-plan", "timeout-plan", "failed-and-timeout-plans", "executing-with-failed-plan"],
    )
    def test_rejects_state_with_unsuccessful_plans(self, terminal_state):
        clock = FakeClock()

        with pytest.raises(
            AssertionError,
            match=rf"compaction 42.*failed plans: {terminal_state.failed_plan_no}.*"
            rf"timed out plans: {terminal_state.timeout_plan_no}",
        ):
            wait_for_compaction_completed(
                get_state=lambda compact_id, rpc_timeout: terminal_state,
                compact_id=42,
                timeout=3,
                poll_interval=1,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )

        assert clock.now == 0

    @pytest.mark.parametrize("timeout,poll_interval", [(0, 1), (-1, 1), (1, 0), (1, -1)])
    def test_rejects_non_positive_timing(self, timeout, poll_interval):
        with pytest.raises(ValueError):
            wait_for_compaction_completed(
                get_state=lambda compact_id, rpc_timeout: CompactionStateInfo(state="Completed"),
                compact_id=42,
                timeout=timeout,
                poll_interval=poll_interval,
            )


@pytest.mark.tags(CaseLabel.L0)
class TestGetCompactionStateInfo:
    def test_maps_all_counts_and_propagates_context(self):
        response = milvus_types.GetCompactionStateResponse(
            status=common_pb2.Status(error_code=common_pb2.Success),
            state=common_pb2.CompactionState.Completed,
            executingPlanNo=1,
            completedPlanNo=2,
            failedPlanNo=3,
            timeoutPlanNo=4,
        )

        class Stub:
            def __init__(self):
                self.calls = []

            def GetCompactionState(self, request, timeout, metadata):
                self.calls.append((request, timeout, metadata))
                return response

        stub = Stub()
        context = CallContext(db_name="review_db", client_request_id="review-request")

        state = get_compaction_state_info(SimpleNamespace(_stub=stub), compact_id=42, timeout=5, context=context)

        assert state == CompactionStateInfo(
            state="Completed",
            executing_plan_no=1,
            completed_plan_no=2,
            failed_plan_no=3,
            timeout_plan_no=4,
        )
        request, rpc_timeout, metadata = stub.calls[0]
        assert request.compactionID == 42
        assert 0 < rpc_timeout <= 5
        assert ("dbname", "review_db") in metadata
        assert ("client-request-id", "review-request") in metadata

    def test_retries_transient_rpc_error(self):
        response = milvus_types.GetCompactionStateResponse(
            status=common_pb2.Status(error_code=common_pb2.Success),
            state=common_pb2.CompactionState.Executing,
            executingPlanNo=1,
        )

        class Handler:
            def __init__(self):
                self._stub = self
                self.call_count = 0
                self.reconnect_timeouts = []

            def GetCompactionState(self, request, timeout, metadata):
                self.call_count += 1
                if self.call_count == 1:
                    raise RetryableRpcError()
                return response

            def reconnect(self, timeout):
                self.reconnect_timeouts.append(timeout)

        handler = Handler()
        state = get_compaction_state_info(handler, compact_id=42, timeout=1)

        assert state.state == "Executing"
        assert handler.call_count == 2
        assert len(handler.reconnect_timeouts) == 1
        assert 0 < handler.reconnect_timeouts[0] <= 1

    def test_recovery_uses_remaining_deadline_and_cannot_continue_after_it(self, monkeypatch):
        clock = FakeClock()

        class Handler:
            def __init__(self):
                self._stub = self
                self.call_count = 0
                self.reconnect_timeouts = []

            def GetCompactionState(self, request, timeout, metadata):
                self.call_count += 1
                clock.now += 0.06
                raise RetryableRpcError()

            def reconnect(self, timeout):
                self.reconnect_timeouts.append(timeout)
                clock.now += timeout

        handler = Handler()
        monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)

        with pytest.raises(TimeoutError, match=r"timed out getting state for compaction 42"):
            get_compaction_state_info(handler, compact_id=42, timeout=0.1)

        assert handler.call_count == 1
        assert handler.reconnect_timeouts == pytest.approx([0.04])

    def test_retries_rate_limit_status(self, monkeypatch):
        clock = FakeClock()
        responses = iter(
            [
                milvus_types.GetCompactionStateResponse(
                    status=common_pb2.Status(error_code=common_pb2.RateLimit, reason="limited")
                ),
                milvus_types.GetCompactionStateResponse(
                    status=common_pb2.Status(error_code=common_pb2.Success),
                    state=common_pb2.CompactionState.Executing,
                    executingPlanNo=1,
                ),
            ]
        )

        class Stub:
            def __init__(self):
                self.call_count = 0

            def GetCompactionState(self, request, timeout, metadata):
                self.call_count += 1
                return next(responses)

        stub = Stub()
        monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(compaction_utils.time, "sleep", clock.sleep)

        state = get_compaction_state_info(SimpleNamespace(_stub=stub), compact_id=42, timeout=1)

        assert state.state == "Executing"
        assert stub.call_count == 2
        assert clock.now == pytest.approx(0.01)

    def test_non_rate_limit_status_fails_without_background_recovery(self):
        class Handler:
            def __init__(self):
                self._stub = self
                self.call_count = 0
                self.recovery_errors = []

            def GetCompactionState(self, request, timeout, metadata):
                self.call_count += 1
                return milvus_types.GetCompactionStateResponse(
                    status=common_pb2.Status(error_code=common_pb2.UnexpectedError, reason="recoverable")
                )

            def _on_rpc_error(self, error):
                self.recovery_errors.append(error)
                return True

        handler = Handler()
        with pytest.raises(compaction_utils.MilvusException, match="recoverable"):
            get_compaction_state_info(handler, compact_id=42, timeout=1)

        assert handler.call_count == 1
        assert handler.recovery_errors == []

    def test_retries_with_remaining_rpc_timeout(self, monkeypatch):
        clock = FakeClock()
        rpc_timeouts = []
        response = milvus_types.GetCompactionStateResponse(
            status=common_pb2.Status(error_code=common_pb2.Success),
            state=common_pb2.CompactionState.Executing,
            executingPlanNo=1,
        )

        class Stub:
            def GetCompactionState(self, request, timeout, metadata):
                rpc_timeouts.append(timeout)
                if len(rpc_timeouts) == 1:
                    clock.now += 0.06
                    raise RetryableRpcError()
                return response

        monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(compaction_utils.time, "sleep", clock.sleep)

        state = get_compaction_state_info(SimpleNamespace(_stub=Stub()), compact_id=42, timeout=0.1)

        assert state.state == "Executing"
        assert rpc_timeouts == pytest.approx([0.1, 0.03])

    def test_does_not_backoff_past_deadline(self, monkeypatch):
        clock = FakeClock()

        class Stub:
            def GetCompactionState(self, request, timeout, metadata):
                clock.now += 0.095
                raise RetryableRpcError()

        monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(compaction_utils.time, "sleep", clock.sleep)

        with pytest.raises(TimeoutError, match=r"timed out getting state for compaction 42"):
            get_compaction_state_info(SimpleNamespace(_stub=Stub()), compact_id=42, timeout=0.1)

        assert clock.now == pytest.approx(0.095)

    def test_rejects_rpc_result_returned_after_deadline(self, monkeypatch):
        clock = FakeClock()
        response = milvus_types.GetCompactionStateResponse(
            status=common_pb2.Status(error_code=common_pb2.Success),
            state=common_pb2.CompactionState.Completed,
            completedPlanNo=1,
        )

        class Stub:
            def GetCompactionState(self, request, timeout, metadata):
                clock.now += timeout + 0.01
                return response

        monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)

        with pytest.raises(TimeoutError, match=r"timed out getting state for compaction 42"):
            get_compaction_state_info(SimpleNamespace(_stub=Stub()), compact_id=42, timeout=0.1)


@pytest.mark.tags(CaseLabel.L0)
class TestWaitForCompactionEligibleSegments:
    def test_bounds_rpc_and_rejects_late_stable_snapshot(self, monkeypatch):
        clock = FakeClock()
        rpc_timeouts = []
        segment = SimpleNamespace(
            segment_id=1,
            num_rows=100,
            state_name="Flushed",
            level_name="L1",
            is_sorted=True,
        )

        class FakeClient:
            def __init__(self):
                self._stub = self

            def _get_connection(self):
                return self

            def _generate_call_context(self):
                return None

            def GetPersistentSegmentInfo(self, request, timeout=None, metadata=None):
                rpc_timeouts.append(timeout)
                clock.now += 1 if len(rpc_timeouts) == 1 else 8
                return SimpleNamespace(
                    status=common_pb2.Status(error_code=common_pb2.Success),
                    infos=[
                        SimpleNamespace(
                            segmentID=segment.segment_id,
                            collectionID=1,
                            num_rows=segment.num_rows,
                            is_sorted=segment.is_sorted,
                            state=common_pb2.SegmentState.Flushed,
                            level=common_pb2.SegmentLevel.L1,
                            storage_version=0,
                        )
                    ],
                )

        monkeypatch.setattr(client_v2_base.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(client_v2_base.time, "sleep", clock.sleep)

        with pytest.raises(AssertionError, match=r"did not become stable compaction candidates.*10 seconds"):
            client_v2_base.TestMilvusClientV2Base.wait_for_compaction_eligible_segments(
                object(),
                client=FakeClient(),
                collection_name="review_collection",
                minimum_segment_count=1,
                timeout=10,
                poll_interval=2,
            )

        assert rpc_timeouts == pytest.approx([10, 7])

    @pytest.mark.parametrize(
        "transient_status",
        [
            pytest.param(
                common_pb2.Status(code=600, reason="segment not found[segment=1]"),
                id="current-code",
            ),
            pytest.param(
                common_pb2.Status(
                    error_code=common_pb2.SegmentNotFound,
                    reason="segment not found[segment=1]",
                ),
                id="legacy-error-code",
            ),
        ],
    )
    def test_retries_transient_segment_not_found(self, monkeypatch, transient_status):
        clock = FakeClock()
        segment = SimpleNamespace(
            segment_id=1,
            num_rows=100,
            state_name="Flushed",
            level_name="L1",
            is_sorted=True,
        )

        class FakeClient:
            def __init__(self):
                self.calls = 0
                self._stub = self

            def _get_connection(self):
                return self

            def _generate_call_context(self):
                return None

            def GetPersistentSegmentInfo(self, request, timeout=None, metadata=None):
                self.calls += 1
                if self.calls == 1:
                    return SimpleNamespace(
                        status=transient_status,
                        infos=[],
                    )
                return SimpleNamespace(
                    status=common_pb2.Status(error_code=common_pb2.Success),
                    infos=[
                        SimpleNamespace(
                            segmentID=segment.segment_id,
                            collectionID=1,
                            num_rows=segment.num_rows,
                            is_sorted=segment.is_sorted,
                            state=common_pb2.SegmentState.Flushed,
                            level=common_pb2.SegmentLevel.L1,
                            storage_version=0,
                        )
                    ],
                )

        client = FakeClient()
        monkeypatch.setattr(client_v2_base.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(client_v2_base.time, "sleep", clock.sleep)

        result = client_v2_base.TestMilvusClientV2Base.wait_for_compaction_eligible_segments(
            object(),
            client=client,
            collection_name="review_collection",
            minimum_segment_count=1,
            timeout=10,
            poll_interval=1,
        )

        assert [item.segment_id for item in result] == [segment.segment_id]
        assert result[0].state_name == segment.state_name
        assert result[0].level_name == segment.level_name
        assert result[0].is_sorted == segment.is_sorted
        assert client.calls == 3
        assert clock.now == 2

    @pytest.mark.parametrize("failure", ["unavailable", "rate-limit"])
    def test_retries_transient_transport_and_rate_limit_failures(self, monkeypatch, failure):
        clock = FakeClock()
        rpc_timeouts = []

        class FakeClient:
            def __init__(self):
                self.calls = 0
                self.reconnect_timeouts = []
                self._stub = self

            def _get_connection(self):
                return self

            def _generate_call_context(self):
                return None

            def reconnect(self, timeout):
                self.reconnect_timeouts.append(timeout)

            def GetPersistentSegmentInfo(self, request, timeout=None, metadata=None):
                self.calls += 1
                rpc_timeouts.append(timeout)
                if self.calls == 1:
                    if failure == "unavailable":
                        raise RetryableRpcError()
                    return SimpleNamespace(
                        status=common_pb2.Status(error_code=common_pb2.RateLimit, reason="limited"),
                        infos=[],
                    )
                return SimpleNamespace(
                    status=common_pb2.Status(error_code=common_pb2.Success),
                    infos=[
                        SimpleNamespace(
                            segmentID=1,
                            collectionID=1,
                            num_rows=100,
                            is_sorted=True,
                            state=common_pb2.SegmentState.Flushed,
                            level=common_pb2.SegmentLevel.L1,
                            storage_version=0,
                        )
                    ],
                )

        client = FakeClient()
        monkeypatch.setattr(client_v2_base.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(client_v2_base.time, "sleep", clock.sleep)

        result = client_v2_base.TestMilvusClientV2Base.wait_for_compaction_eligible_segments(
            object(),
            client=client,
            collection_name="review_collection",
            minimum_segment_count=1,
            timeout=10,
            poll_interval=1,
        )

        assert [item.segment_id for item in result] == [1]
        assert client.calls == 3
        assert rpc_timeouts == pytest.approx([10, 9.99, 8.99])
        if failure == "unavailable":
            assert client.reconnect_timeouts == pytest.approx([10])
        else:
            assert client.reconnect_timeouts == []
