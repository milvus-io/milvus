"""Load an environment-owned checkpoint provider for white-box L1 tests."""

import importlib
import os

import pytest


@pytest.fixture
def checkpoint_provider():
    name = os.environ.get("MILVUS_L1_CHECKPOINT_PROVIDER")
    if not name:
        pytest.skip("MILVUS_L1_CHECKPOINT_PROVIDER is required for server-side path evidence")
    return importlib.import_module(name)
