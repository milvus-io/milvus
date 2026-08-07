import threading
import time

import pytest
from chaos import chaos_commons as cc


class _LoopChecker:
    def __init__(self):
        self._keep_running = False
        self.started = threading.Event()
        self.iterations = 0

    def keep_running(self):
        self.started.set()
        while self._keep_running:
            self.iterations += 1
            time.sleep(0.001)


class _StuckChecker:
    def __init__(self):
        self._keep_running = False
        self.started = threading.Event()
        self.release = threading.Event()

    def keep_running(self):
        self.started.set()
        self.release.wait(timeout=1)


def test_monitor_threads_stops_workers_when_test_body_fails():
    checker = _LoopChecker()
    tasks = []

    with pytest.raises(RuntimeError, match="test failure"):
        with cc.monitor_threads({"loop": checker}, join_timeout=1) as tasks:
            assert checker.started.wait(timeout=1)
            raise RuntimeError("test failure")

    assert tasks
    assert all(not task.is_alive() for task in tasks)
    completed_iterations = checker.iterations
    time.sleep(0.01)
    assert checker.iterations == completed_iterations


def test_monitor_threads_fails_when_worker_ignores_shutdown():
    checker = _StuckChecker()
    tasks = []

    with pytest.raises(AssertionError, match="did not stop within 0s"):
        try:
            with cc.monitor_threads({"stuck": checker}, join_timeout=0) as tasks:
                assert checker.started.wait(timeout=1)
        finally:
            checker.release.set()

    for task in tasks:
        task.join(timeout=1)
