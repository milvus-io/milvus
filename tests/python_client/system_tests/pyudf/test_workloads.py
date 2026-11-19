"""Complete, independently runnable system scenarios on a reserved deployment."""

import pytest
from workloads.pyudf import stress
from workloads.pyudf.runner import soak

pytestmark = pytest.mark.tags("L3", "pyudf")


def test_soak_and_replay_preserve_rows_and_scores(pyudf_system):
    soak(pyudf_system)


@pytest.mark.usefixtures("single_worker_proxies")
@pytest.mark.parametrize("pyudf_system", ["HNSW", "DISKANN"], indirect=True)
def test_abba_preserves_semantics_and_worker_counts(pyudf_system):
    """Measure paired operations and worker counts; no latency acceptance threshold."""
    stress.abba(pyudf_system)


@pytest.mark.parametrize("pyudf_system", ["HNSW", "DISKANN"], indirect=True)
def test_batches_preserve_scores_and_input_columns(pyudf_system):
    stress.batches(pyudf_system)


def test_concurrent_search_reaches_requested_rpc_concurrency(pyudf_system):
    stress.concurrency(pyudf_system)


def test_cpu_and_wait_load_preserve_peer_search(pyudf_system):
    stress.interference(pyudf_system)


def test_package_reuse_and_removal_preserve_search(pyudf_system):
    """RAG document policies survive wheel reuse; removed resources stop serving."""
    stress.package_cache(pyudf_system)
