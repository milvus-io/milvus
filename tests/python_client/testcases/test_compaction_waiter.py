import threading
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
from pymilvus import MilvusClient
from pymilvus.client.call_context import CallContext
from pymilvus.client.grpc_handler import GrpcHandler
from pymilvus.client.types import Status
from pymilvus.exceptions import MilvusException
from pymilvus.grpc_gen import common_pb2, schema_pb2
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

    def test_retries_reconnect_handshake_failure_within_deadline(self, monkeypatch):
        clock = FakeClock()
        response = milvus_types.GetCompactionStateResponse(
            status=common_pb2.Status(error_code=common_pb2.Success),
            state=common_pb2.CompactionState.Executing,
            executingPlanNo=1,
        )

        class Handler:
            def __init__(self):
                self._stub = self
                self.call_count = 0
                self.reconnect_count = 0
                self.reconnect_timeouts = []

            def GetCompactionState(self, request, timeout, metadata):
                self.call_count += 1
                if self.call_count <= 2:
                    raise RetryableRpcError()
                return response

            def reconnect(self, timeout):
                self.reconnect_count += 1
                self.reconnect_timeouts.append(timeout)
                if self.reconnect_count == 1:
                    clock.now += 0.02
                    raise compaction_utils.MilvusException(
                        code=Status.CONNECT_FAILED,
                        message="transient handshake failure",
                    )

        monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(compaction_utils.time, "sleep", clock.sleep)

        handler = Handler()
        state = get_compaction_state_info(handler, compact_id=42, timeout=1)

        assert state.state == "Executing"
        assert handler.call_count == 3
        assert handler.reconnect_count == 2
        assert handler.reconnect_timeouts[0] == pytest.approx(1)
        assert handler.reconnect_timeouts[1] == pytest.approx(0.97)

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


@pytest.mark.tags(CaseLabel.L0)
@pytest.mark.parametrize("helper", ["segments", "query"])
@pytest.mark.parametrize("handshake", ["recovers", "exhausts-deadline", "permanent-error"])
def test_helpers_handle_real_sdk_reconnect_handshake(monkeypatch, helper, handshake):
    """Use the SDK reconnect/readiness path, including its CONNECT_FAILED mapping."""
    from milvus_client.test_milvus_client_force_merge import collect_query_ids

    clock = FakeClock()
    rpc_timeouts = []
    handshake_timeouts = []
    permanent_error = MilvusException(code=65535, message="non-retryable handshake error")

    class Stub:
        def observe(self, request, timeout, metadata):
            name = request.collectionName if helper == "segments" else request.collection_name
            assert name == "review_collection"
            assert ("dbname", "review_db") in metadata
            rpc_timeouts.append(timeout)
            if len(rpc_timeouts) <= 2:
                raise RetryableRpcError()

        def GetPersistentSegmentInfo(self, request, timeout, metadata):
            self.observe(request, timeout, metadata)
            return SimpleNamespace(
                status=common_pb2.Status(),
                infos=[
                    SimpleNamespace(
                        segmentID=1,
                        collectionID=1,
                        num_rows=2,
                        is_sorted=True,
                        state=common_pb2.SegmentState.Flushed,
                        level=common_pb2.SegmentLevel.L1,
                        storage_version=0,
                    )
                ],
            )

        def Query(self, request, timeout, metadata):
            self.observe(request, timeout, metadata)
            return milvus_types.QueryResults(
                status=common_pb2.Status(),
                fields_data=[
                    schema_pb2.FieldData(
                        type=schema_pb2.Int64,
                        field_name="id",
                        scalars=schema_pb2.ScalarField(long_data=schema_pb2.LongArray(data=[0, 1])),
                    )
                ],
            )

    # Keep reconnect(), _wait_for_channel_ready(), and query() real. Only replace
    # channel construction and the handshake RPC so this needs no live server.
    handler = GrpcHandler.__new__(GrpcHandler)
    handler._address = "localhost:19530"
    handler._user = ""
    handler._channel = SimpleNamespace(close=lambda: None)
    handler._channel_swap_lock = threading.Lock()
    handler.callbacks = []
    handler._stub = Stub()
    monkeypatch.setattr(handler, "_create_grpc_channel", lambda address: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(handler, "_setup_grpc_channel", lambda channel: (channel, handler._stub))

    def handshake_rpc(user, timeout, final_channel, stub):
        handshake_timeouts.append(timeout)
        if handshake == "permanent-error":
            raise permanent_error
        if handshake == "exhausts-deadline":
            clock.sleep(timeout)
            raise RetryableRpcError()
        if len(handshake_timeouts) == 1:
            clock.sleep(0.02)
            raise RetryableRpcError()
        return final_channel, stub

    monkeypatch.setattr(handler, "_setup_identifier_interceptor", handshake_rpc)
    monkeypatch.setattr(client_v2_base.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(client_v2_base.time, "sleep", clock.sleep)
    client = SimpleNamespace(
        _get_connection=lambda: handler,
        _generate_call_context=lambda: CallContext(db_name="review_db"),
        _with_cluster_id=lambda kwargs: {**kwargs, "cluster_id": "review-cluster"},
    )

    def invoke():
        if helper == "query":
            return collect_query_ids(
                client,
                "review_collection",
                total_rows=2,
                timeout=1,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )
        return client_v2_base.TestMilvusClientV2Base.wait_for_compaction_eligible_segments(
            object(), client, "review_collection", minimum_segment_count=1, timeout=1, poll_interval=0.1
        )

    if handshake == "recovers":
        result = invoke()
        if helper == "query":
            assert result == [0, 1]
            assert rpc_timeouts == pytest.approx([1, 0.97, 0.94])
        else:
            assert [segment.segment_id for segment in result] == [1]
            assert rpc_timeouts == pytest.approx([1, 0.97, 0.94, 0.84])
        assert handshake_timeouts == pytest.approx([1, 0.97])
    elif handshake == "exhausts-deadline":
        with pytest.raises((TimeoutError, AssertionError)):
            invoke()
        assert rpc_timeouts == [1]
        assert handshake_timeouts == [1]
        assert clock.now == 1
    else:
        with pytest.raises(MilvusException) as error:
            invoke()
        assert error.value is permanent_error
        assert rpc_timeouts == [1]
        assert handshake_timeouts == [1]


class LoadedTopologyClient:
    def __init__(self, snapshots):
        self.snapshots = iter(snapshots)
        self.events = []
        self.handler = GrpcHandler.__new__(GrpcHandler)
        self.handler._stub = self
        self.queried = False

    def _get_connection(self):
        return self.handler

    def _generate_call_context(self):
        return CallContext(db_name="review_db", client_request_id="topology-request")

    # Keep the real high-level SDK entry points available for regression runs
    # against the old implementation, as well as the real handler underneath.
    release_collection = MilvusClient.release_collection
    load_collection = MilvusClient.load_collection

    def observe(self, operation, request, timeout, metadata):
        assert request.collection_name == "review_collection"
        assert ("dbname", "review_db") in metadata
        assert ("client-request-id", "topology-request") in metadata
        self.events.append((operation, timeout))

    def ReleaseCollection(self, request, timeout, metadata):
        self.observe("release", request, timeout, metadata)
        return common_pb2.Status()

    def LoadCollection(self, request, timeout, metadata):
        self.observe("load", request, timeout, metadata)
        return common_pb2.Status()

    def GetLoadingProgress(self, request, timeout, metadata):
        self.observe("progress", request, timeout, metadata)
        return milvus_types.GetLoadingProgressResponse(progress=100)

    def GetQuerySegmentInfo(self, request, timeout, metadata):
        assert request.collectionName == "review_collection"
        assert ("dbname", "review_db") in metadata
        assert ("client-request-id", "topology-request") in metadata
        self.events.append(("topology", timeout))
        self.latest = next(self.snapshots, getattr(self, "latest", []))
        return milvus_types.GetQuerySegmentInfoResponse(
            status=common_pb2.Status(),
            infos=[
                milvus_types.QuerySegmentInfo(segmentID=segment_id, nodeIds=node_ids)
                for segment_id, node_ids in self.latest
            ],
        )

    def query_ids(self):
        # Old inputs can still return all PKs: this must not make integrity pass.
        self.queried = True
        return [0, 1]


@pytest.mark.tags(CaseLabel.L0)
class TestReloadCompactionOutputs:
    def test_waits_for_exact_loaded_outputs_before_reading_pks(self, monkeypatch):
        clock = FakeClock()
        client = LoadedTopologyClient(
            [
                [(1, [10]), (2, [10])],  # Old inputs still readable.
                [(1, [10]), (3, [10]), (4, [10])],  # Outputs loaded, old input remains.
                [(3, [10])],  # A split ForceMerge output is missing.
                [(3, [10]), (4, [10])],
            ]
        )
        monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(compaction_utils.time, "sleep", clock.sleep)
        compaction_utils.reload_compaction_outputs(
            client, "review_collection", {3, 4}, {1, 2}, timeout=5, poll_interval=1
        )
        assert client.query_ids() == [0, 1]
        assert client.events == [
            ("release", 5),
            ("load", 5),
            ("progress", 5),
            ("topology", 5),
            ("topology", 4),
            ("topology", 3),
            ("topology", 2),
        ]

    def test_mix_topology_keeps_unselected_sources(self, monkeypatch):
        clock = FakeClock()
        client = LoadedTopologyClient([[(2, [10]), (3, [10])]])
        monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)
        compaction_utils.reload_compaction_outputs(client, "review_collection", {2, 3}, {1})
        assert client.query_ids() == [0, 1]

    def test_reload_and_poll_share_remaining_budget(self, monkeypatch):
        clock = FakeClock()
        client = LoadedTopologyClient([[(3, [10]), (4, [10])]])
        release = client.ReleaseCollection
        load = client.LoadCollection

        def slow_release(*args, **kwargs):
            release(*args, **kwargs)
            clock.sleep(0.25)
            return common_pb2.Status()

        def slow_load(*args, **kwargs):
            load(*args, **kwargs)
            clock.sleep(0.25)
            return common_pb2.Status()

        monkeypatch.setattr(client, "ReleaseCollection", slow_release)
        monkeypatch.setattr(client, "LoadCollection", slow_load)
        monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)
        compaction_utils.reload_compaction_outputs(client, "review_collection", {3, 4}, {1, 2}, timeout=1)
        assert client.events == [("release", 1), ("load", 0.75), ("progress", 0.5), ("topology", 0.5)]

    def test_late_matching_topology_cannot_reach_pk_validation(self, monkeypatch):
        clock = FakeClock()
        client = LoadedTopologyClient([[(3, [10]), (4, [10])]])
        observe = client.GetQuerySegmentInfo

        def late_observation(request, timeout, metadata):
            response = observe(request, timeout, metadata)
            clock.sleep(timeout)
            return response

        monkeypatch.setattr(client, "GetQuerySegmentInfo", late_observation)
        monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)
        with pytest.raises(AssertionError, match="loaded topology"):
            compaction_utils.reload_compaction_outputs(client, "review_collection", {3, 4}, {1, 2}, timeout=1)
            client.query_ids()
        assert not client.queried
        assert client.events == [("release", 1), ("load", 1), ("progress", 1), ("topology", 1)]

    @pytest.mark.parametrize(
        "snapshot",
        [
            [(1, [10]), (2, [10])],
            [(1, [10]), (3, [10]), (4, [10])],
            [(3, [10])],
            [(3, [10]), (4, [])],
            [],
        ],
        ids=["old-inputs", "old-and-new", "partial-output", "unassigned-output", "empty"],
    )
    def test_rejects_incomplete_handoff_even_if_old_data_has_all_pks(self, monkeypatch, snapshot):
        clock = FakeClock()
        client = LoadedTopologyClient([snapshot])
        monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(compaction_utils.time, "sleep", clock.sleep)
        with pytest.raises(AssertionError, match="loaded topology"):
            compaction_utils.reload_compaction_outputs(
                client, "review_collection", {3, 4}, {1, 2}, timeout=2, poll_interval=1
            )
            client.query_ids()
        assert not client.queried
        assert clock.now == 2

    @pytest.mark.parametrize("operation", ["ReleaseCollection", "LoadCollection", "GetLoadingProgress"])
    def test_failed_reload_never_reaches_pk_validation(self, monkeypatch, operation):
        client = LoadedTopologyClient([[(3, [10]), (4, [10])]])
        error = MilvusException(code=65535, message="injected reload failure")

        def fail(*args, **kwargs):
            raise error

        monkeypatch.setattr(client, operation, fail)
        with pytest.raises(MilvusException) as failure:
            compaction_utils.reload_compaction_outputs(client, "review_collection", {3, 4}, {1, 2})
            client.query_ids()
        assert failure.value is error
        assert not client.queried


@pytest.mark.tags(CaseLabel.L0)
@pytest.mark.parametrize("progress", [50, 100])
def test_reload_real_sdk_load_does_not_restart_deadline(monkeypatch, progress):
    clock = FakeClock()
    client = LoadedTopologyClient([[(3, [10])]])
    load = client.LoadCollection
    observe_progress = client.GetLoadingProgress
    monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(compaction_utils.time, "time", clock.monotonic)
    monkeypatch.setattr(compaction_utils.time, "sleep", clock.sleep)

    def slow_load(*args, **kwargs):
        response = load(*args, **kwargs)
        clock.sleep(4)
        return response

    def slow_progress(request, timeout, metadata):
        observe_progress(request, timeout, metadata)
        # Model an RPC capped by the supplied timeout. The old SDK nested wait
        # supplies 5 here and returns at t=8; the fixed helper supplies only 1.
        clock.sleep(min(4, timeout))
        return milvus_types.GetLoadingProgressResponse(progress=progress)

    monkeypatch.setattr(client, "LoadCollection", slow_load)
    monkeypatch.setattr(client, "GetLoadingProgress", slow_progress)
    with pytest.raises((AssertionError, MilvusException)):
        compaction_utils.reload_compaction_outputs(client, "review_collection", {3}, {1, 2}, timeout=5)
        client.query_ids()
    assert clock.now == 5
    assert client.events == [("release", 5), ("load", 5), ("progress", 1)]
    assert not client.queried


@pytest.mark.tags(CaseLabel.L0)
def test_reload_real_sdk_progress_polls_use_remaining_budget(monkeypatch):
    clock = FakeClock()
    client = LoadedTopologyClient([[(3, [10])]])
    responses = iter([0, 50, 100])
    observe_progress = client.GetLoadingProgress
    monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(compaction_utils.time, "sleep", clock.sleep)

    def progress(request, timeout, metadata):
        observe_progress(request, timeout, metadata)
        clock.sleep(0.25)
        return milvus_types.GetLoadingProgressResponse(progress=next(responses))

    monkeypatch.setattr(client, "GetLoadingProgress", progress)
    compaction_utils.reload_compaction_outputs(client, "review_collection", {3}, {1, 2}, timeout=5)
    assert client.events == [
        ("release", 5),
        ("load", 5),
        ("progress", 5),
        ("progress", 3.75),
        ("progress", 2.5),
        ("topology", 2.25),
    ]


@pytest.mark.tags(CaseLabel.L0)
@pytest.mark.parametrize("operation", ["ReleaseCollection", "LoadCollection", "GetLoadingProgress"])
def test_reload_transient_rpc_is_single_attempt(monkeypatch, operation):
    client = LoadedTopologyClient([[(3, [10])]])
    calls = []

    def fail(*args, **kwargs):
        calls.append(1)
        raise RetryableRpcError()

    monkeypatch.setattr(client, operation, fail)
    with pytest.raises(RetryableRpcError):
        compaction_utils.reload_compaction_outputs(client, "review_collection", {3}, {1, 2}, timeout=1)
    assert calls == [1]
    assert not client.queried


@pytest.mark.tags(CaseLabel.L0)
class TestCompactionWorkload:
    @pytest.mark.parametrize("observe_executing", [True, False])
    def test_completion_after_valid_overlap_is_not_a_failure(self, observe_executing):
        entered = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        samples = []

        def operation(timeout):
            entered.set()
            assert release.wait(timeout)
            finished.set()

        def trigger(timeout):
            assert entered.wait(timeout)

        def get_state(timeout):
            samples.append(1)
            if observe_executing and len(samples) == 1:
                return CompactionStateInfo("Executing")
            release.set()
            assert finished.wait(timeout)
            return CompactionStateInfo("Completed")

        try:
            observed = compaction_utils.run_compaction_with_workload(
                trigger, get_state, operation, timeout=3, poll_interval=0.001
            )
        finally:
            release.set()
        assert observed is observe_executing
        assert finished.is_set()

    def test_different_calls_on_each_side_of_state_rpc_do_not_prove_overlap(self):
        first = threading.Event()
        release_first = threading.Event()
        second = threading.Event()
        release_second = threading.Event()
        samples = []

        def operation(timeout):
            if not first.is_set():
                first.set()
                assert release_first.wait(timeout)
            else:
                second.set()
                assert release_second.wait(timeout)

        def get_state(timeout):
            samples.append(1)
            if len(samples) == 1:
                release_first.set()
                assert second.wait(timeout)
                return CompactionStateInfo("Executing")
            release_second.set()
            return CompactionStateInfo("Completed")

        try:
            observed = compaction_utils.run_compaction_with_workload(
                lambda timeout: first.wait(timeout), get_state, operation, timeout=3, poll_interval=0.001
            )
        finally:
            release_first.set()
            release_second.set()
        assert not observed

    @pytest.mark.parametrize("failure_at", [1, 3])
    def test_every_worker_failure_propagates(self, failure_at):
        failed = threading.Event()
        calls = []
        error = RuntimeError("injected workload failure")

        def operation(timeout):
            calls.append(1)
            if len(calls) == failure_at:
                failed.set()
                raise error

        def get_state(timeout):
            assert failed.wait(timeout)
            return CompactionStateInfo("Completed")

        with pytest.raises(RuntimeError) as failure:
            compaction_utils.run_compaction_with_workload(
                lambda timeout: None, get_state, operation, timeout=3, poll_interval=0.001
            )
        assert failure.value is error
        assert len(calls) == failure_at

    def test_last_inflight_failure_after_completed_propagates(self):
        entered = threading.Event()
        release = threading.Event()
        error = RuntimeError("last in-flight request failed")

        def operation(timeout):
            entered.set()
            assert release.wait(timeout)
            raise error

        def get_state(timeout):
            release.set()
            return CompactionStateInfo("Completed")

        with pytest.raises(RuntimeError) as failure:
            compaction_utils.run_compaction_with_workload(
                lambda timeout: entered.wait(timeout), get_state, operation, timeout=3, poll_interval=0.001
            )
        assert failure.value is error

    @pytest.mark.parametrize("main_failure", ["trigger", "state", "failed-plan"])
    def test_main_failure_stops_and_joins_worker(self, main_failure):
        entered = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def operation(timeout):
            entered.set()
            assert release.wait(timeout)
            finished.set()

        def trigger(timeout):
            assert entered.wait(timeout)
            if main_failure == "trigger":
                release.set()
                raise ValueError("trigger failed")

        def get_state(timeout):
            release.set()
            if main_failure == "state":
                raise ValueError("state failed")
            return CompactionStateInfo("Completed", failed_plan_no=1)

        with pytest.raises((ValueError, AssertionError)):
            compaction_utils.run_compaction_with_workload(trigger, get_state, operation, timeout=3, poll_interval=0.001)
        assert finished.is_set()


@pytest.mark.tags(CaseLabel.L0)
def test_orm_waits_for_sorted_stable_inputs_with_call_context(monkeypatch):
    from base.collection_wrapper import ApiCollectionWrapper

    clock = FakeClock()
    calls = []
    # Sorting rewrites segment 2 to 3; do not snapshot either the unsorted
    # inputs or the first observation of the new topology.
    snapshots = iter([[(1, True), (2, False)], [(1, True), (3, True)], [(1, True), (3, True)]])

    def observe(request, timeout, metadata):
        assert request.collectionName == "review_collection"
        assert ("dbname", "review_db") in metadata
        calls.append(timeout)
        return milvus_types.GetPersistentSegmentInfoResponse(
            infos=[
                milvus_types.PersistentSegmentInfo(
                    segmentID=segment_id,
                    collectionID=1,
                    num_rows=3000,
                    is_sorted=sorted_,
                    state=common_pb2.Flushed,
                    level=common_pb2.L1,
                )
                for segment_id, sorted_ in next(snapshots)
            ]
        )

    handler = SimpleNamespace(_stub=SimpleNamespace(GetPersistentSegmentInfo=observe))
    wrapper = ApiCollectionWrapper()
    wrapper.collection = SimpleNamespace(
        name="review_collection", _get_connection=lambda: (handler, CallContext(db_name="review_db"))
    )
    monkeypatch.setattr(compaction_utils.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(compaction_utils.time, "sleep", clock.sleep)
    segments = wrapper.wait_for_compaction_eligible_segments(2, timeout=10, poll_interval=1)
    assert [segment.segment_id for segment in segments] == [1, 3]
    assert calls == [10, 9, 8]


@pytest.mark.tags(CaseLabel.L0)
@pytest.mark.parametrize(
    "topology",
    [
        "partial-plan",
        "all-selected",
        "selected-source-remains",
        "missing-target",
        "missing-unselected",
        "unindexed",
        "unassigned",
        "unexpected",
        "missing-pk",
    ],
)
def test_index_compaction_case_preserves_unselected_sources(monkeypatch, topology):
    """Run the actual E2E case with injected observations of the review failure."""
    from testcases import test_compaction as case

    clock = FakeClock()
    events = []
    selected = list(range(1, 10 if topology == "all-selected" else 9))
    loaded_ids = [10] if topology == "all-selected" else [9, 10]
    if topology == "selected-source-remains":
        loaded_ids.append(1)
    elif topology == "missing-target":
        loaded_ids = [9]
    elif topology == "missing-unselected":
        loaded_ids = [10]
    elif topology == "unexpected":
        loaded_ids.append(11)

    class Collection:
        name = "review_collection"

        def wait_for_compaction_eligible_segments(self, **kwargs):
            events.append("stable-inputs")
            assert kwargs["minimum_segment_count"] == 9
            return [SimpleNamespace(segment_id=i, num_rows=case.ct.default_nb) for i in range(1, 10)]

        def compact(self, **kwargs):
            events.append("compact")

        def create_index(self, *args, **kwargs):
            events.append("create-index")

        def get_detailed_compaction_state(self, **kwargs):
            return CompactionStateInfo("Completed")

        def index(self, **kwargs):
            return SimpleNamespace(params=case.ct.default_index), True

        def get_compaction_plans(self, **kwargs):
            return SimpleNamespace(plans=[SimpleNamespace(sources=selected, target=10)]), True

        def load(self, **kwargs):
            events.append("load")

        def search(self, *args, **kwargs):
            events.append("search")
            return [list(range(case.ct.default_limit))], True

        def query(self, expr, **kwargs):
            import re

            start, end = map(int, re.findall(r"[<>]=? (\d+)", expr))
            events.append("query")
            if topology == "missing-pk":
                end -= 1
            return [{case.ct.default_int64_field_name: pk} for pk in range(start, end)], True

    def workload(trigger, get_state, operation, **kwargs):
        operation(60)
        trigger(60)
        get_state(60)
        return True

    def loaded(*args, **kwargs):
        events.append("loaded")
        # Advance fake time so rejected topologies deterministically time out.
        clock.sleep(1)
        return [
            SimpleNamespace(
                segmentID=i,
                indexID=0 if topology == "unindexed" and i == 10 else 42,
                index_name="index",
                nodeIds=[] if topology == "unassigned" and i == 10 else [1],
            )
            for i in loaded_ids
        ], True

    monkeypatch.setattr(case, "monotonic", clock.monotonic)
    monkeypatch.setattr(case, "sleep", clock.sleep)
    monkeypatch.setattr(case, "run_compaction_with_workload", workload)
    collection = Collection()
    test = SimpleNamespace(
        collection_insert_multi_segments_one_shard=lambda *args, **kwargs: collection,
        utility_wrap=SimpleNamespace(
            wait_for_index_building_complete=lambda *args, **kwargs: (True, True),
            get_query_segment_info=loaded,
        ),
    )
    if topology in {"partial-plan", "all-selected"}:
        case.TestCompactionOperation.test_compact_during_index(test)
        assert events[:3] == ["stable-inputs", "create-index", "compact"]
        assert events.count("loaded") == 1
        assert "search" in events
        assert events.count("query") == (9 * case.ct.default_nb + 999) // 1000
    elif topology == "missing-pk":
        with pytest.raises(AssertionError):
            case.TestCompactionOperation.test_compact_during_index(test)
    else:
        with pytest.raises(TimeoutError):
            case.TestCompactionOperation.test_compact_during_index(test)
        assert "search" not in events
        assert "query" not in events
