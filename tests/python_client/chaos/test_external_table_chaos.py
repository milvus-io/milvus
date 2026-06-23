import time
from time import sleep

import pytest
from chaos import chaos_commons as cc
from chaos.checker import ExternalTableChecker, Op
from common.common_type import CaseLabel


def _duration_seconds(duration):
    duration = duration.replace("h", "*3600+").replace("m", "*60+").replace("s", "")
    if duration[-1] == "+":
        duration = duration[:-1]
    return eval(duration)


class TestExternalTableChaosChecker:
    @pytest.mark.tags(CaseLabel.L3)
    def test_external_table_checker_basic_lifecycle(self, minio_host, minio_bucket, request_duration):
        checker = ExternalTableChecker(minio_host=minio_host, minio_bucket=minio_bucket, max_files=2)
        try:
            task = cc.start_monitor_threads({Op.external_table: checker})[0]
            deadline = time.time() + _duration_seconds(request_duration)
            while time.time() < deadline and task.is_alive():
                sleep(min(30, max(0, deadline - time.time())))
            assert task.is_alive(), "ExternalTableChecker thread exited unexpectedly"
            checker._keep_running = False
            task.join(timeout=checker.refresh_timeout + 60)
            assert not task.is_alive(), "ExternalTableChecker thread did not stop before final consistency check"
            checker.check_result()
            res, result = checker.verify_consistency()
            assert result, res
            assert not checker.consistency_errors, f"external table dirty data detected: {checker.consistency_errors}"
            assert checker.total() > 0, "ExternalTableChecker did not complete any operation"
        finally:
            checker.terminate()
