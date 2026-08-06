import pytest
from chaos import checker as checker_module


@pytest.mark.parametrize(
    "checker_class",
    (
        checker_module.FlushChecker,
        checker_module.AddFieldChecker,
        checker_module.AddVectorFieldChecker,
        checker_module.SnapshotChecker,
        checker_module.SnapshotRestoreChecker,
    ),
)
def test_heavy_checker_waits_between_operations(monkeypatch, checker_class):
    checker = object.__new__(checker_class)
    checker.c_name = "schedule_test_collection"
    checker._keep_running = True
    checker.configure_operation_schedule(
        interval_seconds=checker_module.HEAVY_OP_WAIT_SECONDS,
        initial_jitter_seconds=checker_module.HEAVY_OP_WAIT_SECONDS,
    )

    def run_once():
        checker._keep_running = False

    checker.run_task = run_once
    wait_calls = []
    monkeypatch.setattr(
        checker_module,
        "_wait_for_next_operation",
        lambda checker, seconds: wait_calls.append(seconds),
    )
    monkeypatch.setattr(checker_module, "_get_initial_operation_jitter", lambda checker, seconds: 17)

    checker.keep_running()

    assert wait_calls == [17, checker_module.HEAVY_OP_WAIT_SECONDS]


def test_initial_operation_jitter_is_stable_and_operation_specific():
    add_vector = object.__new__(checker_module.AddVectorFieldChecker)
    add_vector.c_name = "shared_collection"
    flush = object.__new__(checker_module.FlushChecker)
    flush.c_name = "shared_collection"

    first = checker_module._get_initial_operation_jitter(add_vector, 120)
    second = checker_module._get_initial_operation_jitter(add_vector, 120)
    flush_jitter = checker_module._get_initial_operation_jitter(flush, 120)

    assert first == second
    assert 0 <= first < 120
    assert first != flush_jitter


def test_initial_operation_jitter_uses_distinct_xdist_worker_slots(monkeypatch):
    checker = object.__new__(checker_module.AddVectorFieldChecker)
    checker.c_name = "shared_collection"
    monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", "5")

    delays = []
    for worker_index in range(5):
        monkeypatch.setenv("PYTEST_XDIST_WORKER", f"gw{worker_index}")
        delays.append(checker_module._get_initial_operation_jitter(checker, 120))

    assert len(set(delays)) == 5
    assert all(right - left == 24 for left, right in zip(delays, delays[1:]))


def test_configure_heavy_operation_schedules():
    checkers = {
        op: object.__new__(checker_class)
        for op, checker_class in (
            (checker_module.Op.flush, checker_module.FlushChecker),
            (checker_module.Op.add_field, checker_module.AddFieldChecker),
            (checker_module.Op.snapshot, checker_module.SnapshotChecker),
            (checker_module.Op.restore_snapshot, checker_module.SnapshotRestoreChecker),
            (checker_module.Op.add_vector_field, checker_module.AddVectorFieldChecker),
        )
    }
    insert_checker = object()
    checkers[checker_module.Op.insert] = insert_checker

    checker_module.configure_heavy_operation_schedules(checkers)

    for operation, checker in checkers.items():
        if operation == checker_module.Op.insert:
            assert checker is insert_checker
            continue
        assert checker.operation_interval_seconds == checker_module.HEAVY_OP_WAIT_SECONDS
        assert checker.initial_jitter_seconds == checker_module.HEAVY_OP_WAIT_SECONDS
