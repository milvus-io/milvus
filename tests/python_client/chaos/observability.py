import json
import os
import time
import uuid

LOG_PREFIX = "[chaos-checker]"
_LOCAL_RUN_ID = f"local-{uuid.uuid4().hex[:8]}"


def get_run_id():
    """Return one correlation ID shared by the current test run when possible."""
    return (
        os.getenv("CHAOS_CHECKER_RUN_ID")
        or os.getenv("BUILD_TAG")
        or os.getenv("PYTEST_XDIST_TESTRUNUID")
        or _LOCAL_RUN_ID
    )


def get_worker_id():
    """Return the xdist worker ID, or main for a non-xdist process."""
    return os.getenv("PYTEST_XDIST_WORKER", "main")


def format_event(event, **fields):
    """Format a structured single-line event for Jenkins and Loki."""
    payload = {
        "event": event,
        "run_id": get_run_id(),
        "worker": get_worker_id(),
    }
    payload.update({key: value for key, value in fields.items() if value is not None})
    return f"{LOG_PREFIX} {json.dumps(payload, default=str, sort_keys=True, separators=(',', ':'))}"


def checker_snapshot(operation, checker, now_monotonic=None, now_wall_time=None):
    """Build a compact snapshot without invoking Milvus or mutating checker state."""
    now_monotonic = time.monotonic() if now_monotonic is None else now_monotonic
    now_wall_time = time.time() if now_wall_time is None else now_wall_time
    operation_started = getattr(checker, "current_operation_started_at", None)
    in_flight_seconds = None
    if operation_started is not None:
        in_flight_seconds = round(max(0, now_monotonic - operation_started), 2)

    success = getattr(checker, "_succ", 0)
    failure = getattr(checker, "_fail", 0)
    total = success + failure
    snapshot = {
        "operation": getattr(operation, "value", str(operation)),
        "checker": type(checker).__name__,
        "collection": getattr(checker, "c_name", None),
        "success": success,
        "failure": failure,
        "success_rate": round(success / total, 4) if total else None,
        "average_elapsed_seconds": round(getattr(checker, "average_time", 0), 4),
        "in_flight": getattr(checker, "current_operation", None),
        "in_flight_seconds": in_flight_seconds,
        "last_operation": getattr(checker, "last_operation", None),
        "last_result": getattr(checker, "last_operation_result", None),
        "last_elapsed_seconds": getattr(checker, "last_operation_elapsed", None),
        "error_types": len(getattr(checker, "error_messages", ())),
    }
    for field_name in ("last_operation_completed_at", "last_success_at", "last_failure_at"):
        timestamp = getattr(checker, field_name, None)
        snapshot[f"{field_name.removesuffix('_at')}_seconds_ago"] = (
            round(max(0, now_wall_time - timestamp), 2) if timestamp is not None else None
        )
    return snapshot
