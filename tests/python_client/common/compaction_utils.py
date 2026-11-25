import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from inspect import unwrap

import grpc
from pymilvus.client.call_context import _api_level_md
from pymilvus.client.grpc_handler import GrpcHandler
from pymilvus.client.prepare import Prepare
from pymilvus.client.types import SegmentInfo, Status
from pymilvus.client.utils import check_status
from pymilvus.decorators import IGNORE_RETRY_CODES
from pymilvus.exceptions import ErrorCode, MilvusException
from pymilvus.grpc_gen import common_pb2, milvus_pb2

UNSUCCESSFUL_COMPACTION_STATES = frozenset({"UndefiedState", "UndefinedState"})
INITIAL_RETRY_BACKOFF = 0.01
MAX_RETRY_BACKOFF = 3
RETRY_BACKOFF_MULTIPLIER = 3


@dataclass(frozen=True)
class CompactionStateInfo:
    state: str
    executing_plan_no: int = 0
    completed_plan_no: int = 0
    failed_plan_no: int = 0
    timeout_plan_no: int = 0


def submit_compaction_once(handler, collection_name, *, timeout, context=None, segment_ids=None):
    """Submit a collection-wide or source-restricted manual compaction in one RPC."""
    request = milvus_pb2.ManualCompactionRequest(collection_name=collection_name)
    if segment_ids is not None:
        request.segment_ids.extend(sorted(set(segment_ids)))
        if not request.segment_ids:
            raise ValueError("segment_ids must be nonempty for a targeted compaction")
    response = handler._stub.ManualCompaction(request, timeout=timeout, metadata=_api_level_md(context))
    # DataCoord rejects the selection if a requested segment was claimed or retired after our snapshot.
    if (
        segment_ids is not None
        and response.status.code == 5
        and "not all segment ids provided could be compacted" in response.status.reason
    ):
        return -1
    check_status(response.status)
    return response.compactionID


def reload_collection_with_deadline(client, collection_name, remaining, poll_interval=1):
    """Release, start loading asynchronously, and poll using the caller's remaining budget."""
    handler = client._get_connection()
    context = client._generate_call_context()
    # Single-attempt SDK calls: decorators and synchronous loading polls reuse
    # the supplied timeout, so neither may own this helper's total deadline.
    unwrap(GrpcHandler.release_collection)(handler, collection_name, timeout=remaining(), context=context)
    unwrap(GrpcHandler.load_collection)(handler, collection_name, timeout=remaining(), context=context, _async=True)
    while True:
        progress = unwrap(GrpcHandler.get_loading_progress)(
            handler, collection_name, timeout=remaining(), context=context
        )
        budget = remaining()
        if progress >= 100:
            return
        time.sleep(min(poll_interval, budget))


def reload_compaction_outputs(
    client,
    collection_name,
    expected_segment_ids,
    retired_segment_ids,
    timeout=180,
    poll_interval=1,
):
    """Reload a quiescent collection and verify its output topology before querying PKs.

    DataCoord completion does not imply QueryNode handoff. ReleaseCollection
    waits for the old segment/channel distribution to drain; a fresh load then
    cannot serve the old readable snapshot. GetQuerySegmentInfo alone is not
    this barrier: QueryCoord filters its result against the current target.
    """
    expected_ids = set(expected_segment_ids)
    retired_ids = set(retired_segment_ids)
    if not expected_ids or expected_ids & retired_ids:
        raise ValueError("expected segments must be nonempty and disjoint from retired segments")
    if timeout <= 0 or poll_interval <= 0:
        raise ValueError("timeout and poll_interval must be positive")

    deadline = time.monotonic() + timeout
    loaded_ids = set()
    unassigned_ids = set()

    def remaining():
        budget = deadline - time.monotonic()
        if budget <= 0:
            raise AssertionError(
                f"{collection_name} loaded topology did not match compaction outputs within {timeout:g} seconds; "
                f"expected={sorted(expected_ids)}, loaded={sorted(loaded_ids)}, "
                f"retired={sorted(retired_ids)}, unassigned={sorted(unassigned_ids)}"
            )
        return budget

    reload_collection_with_deadline(client, collection_name, remaining, poll_interval)
    while True:
        # Bypass the SDK retry decorator so each observation has a fresh budget.
        response = client._get_connection()._stub.GetQuerySegmentInfo(
            Prepare.get_query_segment_info_request(collection_name),
            timeout=remaining(),
            metadata=_api_level_md(client._generate_call_context()),
        )
        check_status(response.status)
        loaded_ids = {segment.segmentID for segment in response.infos}
        unassigned_ids = {segment.segmentID for segment in response.infos if not segment.nodeIds}
        budget = remaining()  # A late matching response must not pass.
        if loaded_ids == expected_ids and loaded_ids.isdisjoint(retired_ids) and not unassigned_ids:
            return
        time.sleep(min(poll_interval, budget))


def call_compaction_rpc(
    handler, method, *args, deadline, context=None, retry_transport=True, timeout_message=None, **kwargs
):
    """Run single-attempt handler calls under one deadline, including retries and reconnects."""
    message = timeout_message or f"timed out calling {method.__name__} for {args}"

    def remaining():
        budget = deadline - time.monotonic()
        if budget <= 0:
            raise TimeoutError(message)
        return budget

    attempt = unwrap(method)
    backoff = INITIAL_RETRY_BACKOFF
    while True:
        try:
            result = attempt(handler, *args, timeout=remaining(), context=context, **kwargs)
            remaining()  # A late success cannot start the next phase with a fresh budget.
            return result
        except grpc.RpcError as error:
            if not retry_transport or error.code() in IGNORE_RETRY_CODES:
                raise
            if error.code() == grpc.StatusCode.UNAVAILABLE and hasattr(handler, "reconnect"):
                try:
                    handler.reconnect(timeout=remaining())
                except MilvusException as reconnect_error:
                    if reconnect_error.code != Status.CONNECT_FAILED:
                        raise
        except MilvusException as error:
            if error.code != ErrorCode.RATE_LIMIT and error.compatible_code != common_pb2.RateLimit:
                raise
        if remaining() <= backoff:
            raise TimeoutError(message)
        time.sleep(backoff)
        backoff = min(backoff * RETRY_BACKOFF_MULTIPLIER, MAX_RETRY_BACKOFF)


def get_compaction_state_info(handler, compact_id: int, timeout: float, context=None) -> CompactionStateInfo:
    return call_compaction_rpc(
        handler,
        _get_compaction_state_info,
        compact_id,
        deadline=time.monotonic() + timeout,
        context=context,
        timeout_message=f"timed out getting state for compaction {compact_id}",
    )


def _get_compaction_state_info(handler, compact_id: int, *, timeout: float, context=None) -> CompactionStateInfo:
    # The high-level PySDK API returns only state_name and drops failedPlanNo.
    request = Prepare.get_compaction_state(compact_id)
    response = handler._stub.GetCompactionState(
        request,
        timeout=timeout,
        metadata=_api_level_md(context),
    )
    check_status(response.status)
    return CompactionStateInfo(
        state=common_pb2.CompactionState.Name(response.state),
        executing_plan_no=response.executingPlanNo,
        completed_plan_no=response.completedPlanNo,
        failed_plan_no=response.failedPlanNo,
        timeout_plan_no=response.timeoutPlanNo,
    )


def wait_for_compaction_completed(
    get_state: Callable[[int, float], CompactionStateInfo],
    compact_id: int,
    timeout: float,
    poll_interval: float = 2,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """Poll a compaction job and fail with useful context when it does not finish."""
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero")
    if poll_interval <= 0:
        raise ValueError("poll_interval must be greater than zero")
    if compact_id == -1:
        return True

    deadline = monotonic() + timeout
    last_state = None
    state_history = []
    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            break

        last_state = get_state(compact_id, remaining)
        state_history.append(last_state)
        # Enforce the caller's deadline even when the RPC returns a terminal
        # response after consuming its entire timeout budget.
        if monotonic() >= deadline:
            break
        if last_state.failed_plan_no or last_state.timeout_plan_no:
            raise AssertionError(
                f"compaction {compact_id} reported unsuccessful plans in state {last_state.state}; "
                f"failed plans: {last_state.failed_plan_no}; timed out plans: {last_state.timeout_plan_no}; "
                f"state history: {state_history}"
            )
        if last_state.state == "Completed":
            return True
        if last_state.state in UNSUCCESSFUL_COMPACTION_STATES:
            raise AssertionError(
                f"compaction {compact_id} entered unsuccessful terminal state: {last_state.state}; "
                f"state history: {state_history}"
            )

        remaining = deadline - monotonic()
        if remaining > 0:
            sleep(min(poll_interval, remaining))

    raise AssertionError(
        f"compaction {compact_id} did not complete within {timeout:g} seconds; "
        f"last state: {last_state.state if last_state else None}; state history: {state_history}"
    )


def run_compaction_with_workload(trigger, get_state, operation, timeout=300, poll_interval=0.01):
    """Sustain a checked workload until compaction completes; return overlap evidence.

    Each callback accepts its remaining timeout. An Executing observation counts
    only if the same workload call spans the state RPC. Completed without such
    an observation is insufficient coverage, not evidence of a product failure.
    The caller must check final results before reporting that coverage gap.
    """
    if timeout <= 0 or poll_interval <= 0:
        raise ValueError("timeout and poll_interval must be positive")
    deadline = time.monotonic() + timeout
    stop = threading.Event()
    started = threading.Event()
    lock = threading.Lock()
    active_call = None
    observed_overlap = False

    def remaining():
        budget = deadline - time.monotonic()
        if budget <= 0:
            raise TimeoutError("compaction workload exceeded its deadline")
        return budget

    def worker():
        nonlocal active_call
        while not stop.is_set():
            with lock:
                active_call = object()
            started.set()
            try:
                operation(remaining())
            finally:
                with lock:
                    active_call = None
            stop.wait(min(poll_interval, remaining()))

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(worker)
        try:
            while not started.wait(min(poll_interval, remaining())):
                if future.done():
                    future.result()
            if future.done():
                future.result()
            trigger(remaining())

            def observe(_compact_id, rpc_timeout):
                nonlocal observed_overlap
                if future.done():
                    future.result()
                with lock:
                    before = active_call
                state = get_state(rpc_timeout)
                with lock:
                    if before is not None and active_call is before and state.state == "Executing":
                        observed_overlap = True
                if future.done():
                    future.result()
                return state

            wait_for_compaction_completed(observe, 0, remaining(), poll_interval=poll_interval)
        finally:
            stop.set()
            # Also surface failures from the last in-flight call after the
            # terminal observation, including during cleanup of a main failure.
            future.result()
    return observed_overlap


def is_segment_not_found(error):
    return (error.code == 600 or error.compatible_code == common_pb2.SegmentNotFound) and (
        "segment not found" in error.message.lower()
    )


def list_persistent_segments_once(handler, collection_name, timeout, context=None):
    """Issue exactly one persistent-segment RPC with the supplied budget."""
    request = Prepare.get_persistent_segment_info_request(collection_name)
    response = handler._stub.GetPersistentSegmentInfo(
        request,
        timeout=timeout,
        metadata=_api_level_md(context),
    )
    check_status(response.status)
    return [
        SegmentInfo(
            info.segmentID,
            info.collectionID,
            collection_name,
            info.num_rows,
            info.is_sorted,
            info.state,
            info.level,
            info.storage_version,
        )
        for info in response.infos
    ]


def _segment_snapshot(segments):
    return tuple(sorted((s.segment_id, s.num_rows, s.state_name, s.level_name, s.is_sorted) for s in segments))


def _index_coverage(descriptions, names, required_rows, collection_name):
    progress = {}
    ready = True
    for index in descriptions:
        name = index.index_name
        if name not in names:
            continue
        if index.state == common_pb2.Failed:
            raise AssertionError(f"Index {name} for {collection_name} failed: {index.index_state_fail_reason}")
        ready = ready and (
            name not in progress
            and index.state == common_pb2.Finished
            and required_rows > 0
            and index.total_rows == required_rows
            and index.indexed_rows >= required_rows
            and index.pending_index_rows == 0
        )
        progress[name] = {
            "state": common_pb2.IndexState.Name(index.state),
            "indexed_rows": index.indexed_rows,
            "total_rows": index.total_rows,
            "pending_index_rows": index.pending_index_rows,
        }
    return ready and progress.keys() == names, progress


def wait_for_compaction_eligible_segments(
    handler,
    collection_name,
    minimum_segment_count,
    timeout=30,
    poll_interval=2,
    context=None,
    index_name=None,
    *,
    index_names=(),
    expected_rows=None,
):
    """Wait for stable sorted sources, optionally including their index readiness.

    A manual compaction may skip sorted sources while their vector index is still
    building. The opt-in index check shares this waiter's deadline and requires
    row coverage for the latest source snapshot before counting a stable poll.
    """
    deadline = time.monotonic() + timeout
    required_indexes = set(index_names)
    if index_name is not None:
        required_indexes.add(index_name)
    # DescribeIndex with an empty name returns all indexes for the same DataCoord view.
    request_name = next(iter(required_indexes)) if len(required_indexes) == 1 else ""
    previous_snapshot = None
    stable_observations = 0
    latest_segments = []
    latest_index = None
    retry_backoff = INITIAL_RETRY_BACKOFF

    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        try:
            latest_segments = list_persistent_segments_once(
                handler, collection_name, timeout=remaining, context=context
            )
            snapshot = _segment_snapshot(latest_segments)
            required_rows = sum(segment.num_rows for segment in latest_segments)
            all_segments_eligible = (
                len(snapshot) >= minimum_segment_count
                and (expected_rows is None or required_rows == expected_rows)
                and all(s.state_name == "Flushed" and s.level_name == "L1" and s.is_sorted for s in latest_segments)
            )
            if all_segments_eligible and required_indexes:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                response = handler._stub.DescribeIndex(
                    Prepare.describe_index_request(collection_name, request_name),
                    timeout=remaining,
                    metadata=_api_level_md(context),
                )
                check_status(response.status)
                all_segments_eligible, latest_index = _index_coverage(
                    response.index_descriptions, required_indexes, required_rows, collection_name
                )
                if all_segments_eligible and len(required_indexes) > 1:
                    # Equal row totals alone cannot distinguish a replacement segment.
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    after = list_persistent_segments_once(handler, collection_name, timeout=remaining, context=context)
                    all_segments_eligible = snapshot == _segment_snapshot(after)
        except grpc.RpcError as error:
            if error.code() in IGNORE_RETRY_CODES:
                raise
            if error.code() == grpc.StatusCode.UNAVAILABLE and hasattr(handler, "reconnect"):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    handler.reconnect(timeout=remaining)
                except MilvusException as reconnect_error:
                    if reconnect_error.code != Status.CONNECT_FAILED:
                        raise
                if time.monotonic() >= deadline:
                    break
            previous_snapshot = None
            stable_observations = 0
            remaining = deadline - time.monotonic()
            if remaining <= retry_backoff:
                break
            time.sleep(retry_backoff)
            retry_backoff = min(
                retry_backoff * RETRY_BACKOFF_MULTIPLIER,
                MAX_RETRY_BACKOFF,
            )
            continue
        except MilvusException as error:
            segment_disappeared = is_segment_not_found(error)
            is_rate_limit = error.code == ErrorCode.RATE_LIMIT or error.compatible_code == common_pb2.RateLimit
            if not (segment_disappeared or is_rate_limit):
                raise
            previous_snapshot = None
            stable_observations = 0
            remaining = deadline - time.monotonic()
            delay = poll_interval if segment_disappeared else retry_backoff
            if remaining <= delay:
                break
            time.sleep(delay)
            if is_rate_limit:
                retry_backoff = min(
                    retry_backoff * RETRY_BACKOFF_MULTIPLIER,
                    MAX_RETRY_BACKOFF,
                )
            continue
        if time.monotonic() >= deadline:
            break
        retry_backoff = INITIAL_RETRY_BACKOFF
        if all_segments_eligible and snapshot == previous_snapshot:
            stable_observations += 1
            if stable_observations >= 2:
                return latest_segments
        else:
            stable_observations = 1 if all_segments_eligible else 0
        previous_snapshot = snapshot

        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(poll_interval, remaining))

    raise AssertionError(
        f"Segments for {collection_name} did not become stable compaction candidates "
        f"(Flushed, L1, sorted) within {timeout} seconds; "
        f"last snapshot: {previous_snapshot}"
        + (f"; expected rows: {expected_rows}" if expected_rows is not None else "")
        + (f"; index {request_name or sorted(required_indexes)}: {latest_index}" if required_indexes else "")
    )
