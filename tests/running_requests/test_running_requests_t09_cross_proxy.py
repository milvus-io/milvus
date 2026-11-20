"""TC-L1 functional scenario T09."""

import pytest
from running_requests_l1_support import (
    run_batch,
)

pytestmark = pytest.mark.tags("L1")


def test_t09_cross_proxy_batch_cancel(deployment, workload, workload_baseline):
    """List both targets and controls together, then cancel across Proxies."""
    uris, _, _, _ = deployment
    run_batch(
        "T09", deployment, workload, control_proxies=tuple(uris), expected_missing=[], baseline=workload_baseline()
    )
