import time
from collections.abc import Callable
from dataclasses import dataclass

import grpc
from pymilvus.client.call_context import _api_level_md
from pymilvus.client.prepare import Prepare
from pymilvus.client.utils import check_status
from pymilvus.decorators import IGNORE_RETRY_CODES
from pymilvus.exceptions import ErrorCode, MilvusException
from pymilvus.grpc_gen import common_pb2

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


def get_compaction_state_info(handler, compact_id: int, timeout: float, context=None) -> CompactionStateInfo:
    deadline = time.monotonic() + timeout
    backoff = INITIAL_RETRY_BACKOFF
    while True:
        try:
            state = _get_compaction_state_info(handler, compact_id, deadline=deadline, context=context)
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out getting state for compaction {compact_id}")
            return state
        except grpc.RpcError as error:
            if error.code() in IGNORE_RETRY_CODES:
                raise

            # GrpcHandler.reconnect() is a synchronous, timeout-aware SDK
            # interface. It replaces a shutdown/stale channel before the next
            # raw RPC without leaving a recovery worker behind after timeout.
            if error.code() == grpc.StatusCode.UNAVAILABLE and hasattr(handler, "reconnect"):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"timed out getting state for compaction {compact_id}") from error
                handler.reconnect(timeout=remaining)
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out getting state for compaction {compact_id}") from error

            remaining = deadline - time.monotonic()
            # Never start a backoff that consumes the remaining RPC budget.
            if remaining <= backoff:
                raise TimeoutError(f"timed out getting state for compaction {compact_id}") from error
            time.sleep(backoff)
            backoff = min(backoff * RETRY_BACKOFF_MULTIPLIER, MAX_RETRY_BACKOFF)
        except MilvusException as error:
            should_retry = error.code == ErrorCode.RATE_LIMIT or error.compatible_code == common_pb2.RateLimit
            if not should_retry:
                raise

            remaining = deadline - time.monotonic()
            if remaining <= backoff:
                raise TimeoutError(f"timed out getting state for compaction {compact_id}") from error
            time.sleep(backoff)
            backoff = min(backoff * RETRY_BACKOFF_MULTIPLIER, MAX_RETRY_BACKOFF)


def _get_compaction_state_info(
    handler,
    compact_id: int,
    *,
    deadline: float,
    context=None,
) -> CompactionStateInfo:
    # The high-level PySDK API returns only state_name and drops failedPlanNo.
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError(f"timed out getting state for compaction {compact_id}")
    request = Prepare.get_compaction_state(compact_id)
    response = handler._stub.GetCompactionState(
        request,
        timeout=remaining,
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
