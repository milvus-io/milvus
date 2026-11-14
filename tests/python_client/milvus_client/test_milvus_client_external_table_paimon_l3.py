"""PAI-L3-13: phase-specific refresh timeout contracts on a dedicated deployment.

Requires a real deployment driver implementing the documented control protocol.
Missing configuration fails setup; it can never become a passing/empty test.
"""

import json
import time

import pytest
from base.client_v2_base import TestMilvusClientV2Base as _ClientBase
from common import common_func as cf
from common.common_type import CaseLabel
from common.paimon_fixture import progress_field, wait_actual_index, wait_refresh
from common.paimon_l3_control import PaimonL3Control
from milvus_client import test_milvus_client_external_table_paimon as _paimon

paimon_bundle = _paimon.paimon_bundle
minio_env = _paimon.minio_env
external_prefix = _paimon.external_prefix
paimon_case = _paimon.paimon_case


@pytest.fixture
def phase_control(request, paimon_case):
    path = request.config.getoption("--paimon-l3-profile")
    if not path:
        raise pytest.UsageError("Blocked: PAI-L3-13 needs --paimon-l3-profile and real keyed phase hooks")
    uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
    control = PaimonL3Control(path, uri)
    original = {}

    def restore():
        evidence = control.call("restore")
        assert evidence.get("hooks_cleared") is True
        assert evidence.get("healthy") is True
        if original:
            assert evidence.get("effective_config") == original, "L3 did not restore original configuration"

    request.addfinalizer(restore)  # Also runs if prepare/arm fails halfway through.
    prepared = control.call("prepare")
    assert prepared.get("milvus_uri", "").rstrip("/") == control.uri.rstrip("/")
    assert prepared.get("resolved_target") == control.target
    assert prepared.get("exclusive_deployment") is True
    original.update(prepared["original_config"])
    assert "dataCoord.externalCollection.refreshWaitForIndex" in original
    assert "dataCoord.externalCollectionJobTimeout" in original
    return control


@pytest.mark.paimon_refresh_serial
class TestPaimonRefreshIndexTimeoutIndependent(_ClientBase):
    """When to add tests here: keyed timeout phase controls. Fixtures: phase_control, paimon_case."""

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("phase", ["after-apply", "before-apply", "wait-disabled"])
    def test_refresh_timeout_publication_contract(self, paimon_case, phase_control, phase, record_property):
        """
        target: PAI-L3-13
        method: real apply/index keyed hooks
        expected: phase-correct view, retry and actual ANN.
        """
        case, control = paimon_case, phase_control
        source = case.source("dv")
        name = case.create(source, spec=source.spec("s1"))
        case.refresh(name)
        case.index_load(name, index_type="IVF_FLAT", params={"nlist": 32})
        wait_actual_index(case.client, name, "embedding", len(source.oracle("s1").rows))
        case.verify(name, source.oracle("s1"), search_params={"nprobe": 32})
        old_persisted = case.persisted(name)
        source.publish("s2")
        armed = control.call(
            "arm",
            phase=phase,
            collection=name,
            snapshot_id=source.snapshot("s2"),
            expected_rows=len(source.oracle("s2").rows),
        )
        assert armed.get("keyed_collection") == name
        assert armed.get("keyed_snapshot_id") == source.snapshot("s2")
        assert armed.get("hook_ready") is True
        config = armed["effective_config"]
        assert config["dataCoord.externalCollection.refreshWaitForIndex"] == (phase != "wait-disabled")
        assert armed.get("effective_job_timeout_seconds", 0) > 0
        job = case.client.refresh_external_collection(
            collection_name=name, external_source=source.source, external_spec=source.spec("s2"), timeout=30
        )
        expected = "RefreshCompleted" if phase == "wait-disabled" else "RefreshFailed"
        result = wait_refresh(case.client, job, timeout=180, expected=expected)
        if expected == "RefreshFailed":
            assert "timeout" in progress_field(result, "reason", "").lower()
        evidence = control.call("inspect", collection=name, job_id=job)
        assert evidence.get("job_id") == job
        assert evidence.get("raw_job_state") == expected
        if phase == "after-apply":
            assert evidence.get("segments_applied") is True
            assert evidence.get("index_wait_started_time", 0) > 0, "timeout occurred before index wait; invalid setup"
            assert evidence.get("observed_in_progress_with_index_wait") is True
            assert evidence.get("s2_indexes_blocked") is True
        elif phase == "before-apply":
            assert evidence.get("segments_applied") is False
            assert evidence.get("apply_blocked_before_publication") is True
            assert case.persisted(name) == old_persisted
        else:
            assert evidence.get("segments_applied") is True
            assert evidence.get("s2_indexes_blocked") is True
            stats = case.client.describe_index(collection_name=name, index_name="embedding", timeout=30)
            assert int(stats["pending_index_rows"]) > 0, "Completed alone does not prove delayed ANN setup"
        version = "s1" if phase == "before-apply" else "s2"
        # Allow serving convergence, but never release/load to hide an incorrect first view.
        deadline = time.monotonic() + 60
        while True:
            try:
                source.oracle(version).assert_rows(case.rows(name), _paimon.CORE)
                break
            except AssertionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        case.verify(name, source.oracle(version), search_params={"nprobe": 32})
        case.reload(name)
        case.verify(name, source.oracle(version), search_params={"nprobe": 32})
        if version == "s2":
            url, spec = case.persisted(name)
            assert url == source.source and int(spec["snapshot_id"]) == source.snapshot("s2")
        resumed = control.call("unblock", collection=name, job_id=job)
        assert resumed.get("hook_cleared") is True
        assert resumed.get("timeout_restored_for_retry") is True
        # The after-apply failure must not cancel background index construction.
        if phase == "after-apply":
            wait_actual_index(case.client, name, "embedding", len(source.oracle("s2").rows))
            background = control.call("inspect", collection=name, job_id=job)
            assert background.get("background_index_completed_without_refresh_retry") is True
        case.refresh(name, source, source.spec("s2"))
        wait_actual_index(case.client, name, "embedding", len(source.oracle("s2").rows))
        assert (
            progress_field(case.client.get_refresh_external_collection_progress(job_id=job, timeout=30), "state")
            == expected
        )
        for reload in [False, True]:
            if reload:
                case.reload(name)
            case.verify(name, source.oracle("s2"), search_params={"nprobe": 32}, min_recall=0.95)
        final = control.call("inspect", collection=name, job_id=job)
        assert final.get("duplicate_logical_application") is False
        record_property("paimon_l3_phase_evidence", json.dumps(control.events, sort_keys=True))
        assert progress_field(result, "state") == expected
