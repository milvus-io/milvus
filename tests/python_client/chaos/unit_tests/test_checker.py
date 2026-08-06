import pytest
from chaos import checker as checker_module


@pytest.mark.parametrize(
    "checker_class",
    (
        checker_module.AddFieldChecker,
        checker_module.AddVectorFieldChecker,
        checker_module.SnapshotChecker,
        checker_module.SnapshotRestoreChecker,
    ),
)
def test_heavy_checker_waits_between_operations(monkeypatch, checker_class):
    checker = object.__new__(checker_class)
    checker._keep_running = True

    def run_once():
        checker._keep_running = False

    checker.run_task = run_once
    wait_calls = []
    monkeypatch.setattr(
        checker_module,
        "_wait_for_next_operation",
        lambda checker, seconds: wait_calls.append(seconds),
    )

    checker.keep_running()

    assert wait_calls == [checker_module.HEAVY_OP_WAIT_SECONDS]
