"""Complete, independently runnable system scenarios on a reserved deployment."""

import pytest
from workloads.pyudf import stress
from workloads.pyudf.runner import WorkloadConfig, soak

pytestmark = pytest.mark.tags("L3", "pyudf")


def test_soak_and_replay_preserve_rows_and_scores(pyudf_system):
    config = WorkloadConfig.read(pyudf_system)
    soak(pyudf_system, config.duration, config.drain, config.replay)


@pytest.mark.usefixtures("single_worker_proxies")
@pytest.mark.parametrize("pyudf_system", ["HNSW", "DISKANN"], indirect=True)
def test_abba_and_batches_preserve_semantics_and_worker_counts(pyudf_system):
    """Measure equivalent operations and batches; no latency acceptance threshold."""
    stress.performance(pyudf_system)


def test_concurrent_search_reaches_requested_rpc_concurrency(pyudf_system):
    stress.concurrency(pyudf_system)


def test_cpu_and_wait_load_preserve_peer_search(pyudf_system):
    stress.interference(pyudf_system)


def test_package_reuse_and_removal_preserve_search(pyudf_system):
    """Wheel reuse and invalidation, without claiming tiered-storage/cache hits."""
    stress.package_cache(pyudf_system)
