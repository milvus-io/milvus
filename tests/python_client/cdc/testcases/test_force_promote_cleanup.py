import importlib
import importlib.util
import sys
import types

if importlib.util.find_spec("yaml") is None:
    yaml = types.ModuleType("yaml")
    yaml.safe_dump = lambda *args, **kwargs: None
    sys.modules.setdefault("yaml", yaml)

_TestCDCForcePromote = importlib.import_module("cdc.testcases.test_force_promote").TestCDCForcePromote


class FakeClient:
    def __init__(self):
        self.collections = {"downstream_only"}
        self.dropped = []

    def has_collection(self, collection_name):
        return collection_name in self.collections

    def drop_collection(self, collection_name):
        self.dropped.append(collection_name)
        self.collections.discard(collection_name)


def test_cleanup_downstream_only_collection_before_topology_restore():
    test = _TestCDCForcePromote()
    test.resources_to_cleanup = [
        ("collection", "normal_collection"),
        ("collection", "downstream_only"),
    ]
    test._downstream_client = FakeClient()

    test.cleanup_downstream_only_collection("downstream_only")

    assert test._downstream_client.dropped == ["downstream_only"]
    assert ("collection", "downstream_only") not in test.resources_to_cleanup
    assert ("collection", "normal_collection") in test.resources_to_cleanup
