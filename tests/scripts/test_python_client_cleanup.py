"""Server-independent regressions for Python E2E collection cleanup.

Run explicitly with: python3 -m pytest tests/scripts/test_python_client_cleanup.py
The tests use the E2E Python dependencies and a local gRPC fault injector.
"""

import importlib
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call

import grpc
import numpy as np
import pytest
from pymilvus import MilvusClient
from pymilvus.client.grpc_handler import GrpcHandler
from pymilvus.grpc_gen import common_pb2, milvus_pb2_grpc

pytest_plugins = ["pytester"]


@pytest.fixture
def client_base(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "python_client"))
    # Standalone runs and pytester workers do not load python_client/conftest.py.
    # Match its NumPy 2 compatibility bootstrap before importing legacy wrappers.
    if not hasattr(np, "NaN"):
        monkeypatch.setattr(np, "NaN", np.nan, raising=False)
    # Match conftest's import order: common_func and the wrappers depend on
    # each other during import.
    common = importlib.import_module("common.common_func")
    module = importlib.import_module("base.client_base")
    for name, value in {
        "param_uri": "http://localhost:19530",
        "param_token": "",
        "param_user": "",
        "param_password": "",
    }.items():
        monkeypatch.setattr(common.param_info, name, value)
    return module


@pytest.fixture
def case(client_base):
    case = client_base.Base()
    case._setup_objects()
    case.skip_global_role_cleanup = True
    case.connection_wrap = Mock(spec=client_base.ApiConnectionsWrapper)
    case.connection_wrap.has_connection.return_value = (True, True)
    case.connection_wrap.list_connections.return_value = ([("default", None), ("extra", None)], True)
    case.collection_wrap = Mock(spec=client_base.ApiCollectionWrapper)
    case.collection_wrap.collection = None
    case.utility_wrap = Mock(spec=client_base.ApiUtilityWrapper)
    case.utility_wrap.list_collections.return_value = (["one", "two", "unowned"], True)
    case.utility_wrap.list_aliases.return_value = ([], True)
    case.utility_wrap.drop_alias.return_value = (None, True)
    case.utility_wrap.drop_collection.return_value = (None, True)
    case.utility_wrap.list_resource_groups.return_value = ([], True)
    return case


@pytest.fixture
def client_v2_base(client_base):
    return importlib.import_module("base.client_v2_base")


@pytest.fixture
def client_v2_case(client_v2_base):
    case = client_v2_base.TestMilvusClientV2Base()
    case._setup_objects()
    return case


@pytest.fixture
def stalled_drop():
    canceled = threading.Event()
    observed_deadlines = []

    class StalledDrop(milvus_pb2_grpc.MilvusServiceServicer):
        def DropCollection(self, request, context):
            observed_deadlines.append(context.time_remaining())
            context.add_callback(canceled.set)
            canceled.wait(20)
            return common_pb2.Status()

    with ThreadPoolExecutor(max_workers=1) as executor:
        server = grpc.server(executor)
        milvus_pb2_grpc.add_MilvusServiceServicer_to_server(StalledDrop(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        try:
            grpc.channel_ready_future(channel).result(timeout=5)
            handler = GrpcHandler(channel=channel)
            client = object.__new__(MilvusClient)
            client._get_connection = lambda: handler
            client._generate_call_context = lambda **kwargs: None
            yield SimpleNamespace(
                handler=handler, client=client, canceled=canceled, observed_deadlines=observed_deadlines
            )
        finally:
            canceled.set()
            channel.close()
            server.stop(0).wait(timeout=5)


def test_registries_are_isolated_and_reset_with_wrappers(client_base):
    first, second = client_base.Base(), client_base.Base()
    first._setup_objects()
    second._setup_objects()
    first.tear_down_collection_names.append("first")
    assert second.tear_down_collection_names == []
    assert client_base.Base.tear_down_collection_names == ()
    first._setup_objects()
    assert first.tear_down_collection_names == []


def test_success_drops_only_owned_collections_and_aliases_once(case):
    case.tear_down_collection_names.extend(["one", None, "one", "missing", "two"])
    case.utility_wrap.list_aliases.side_effect = [(["one_alias"], True), ([], True)]
    case._teardown_objects()
    assert [c.args for c in case.utility_wrap.drop_collection.call_args_list] == [("one",), ("two",)]
    assert case.utility_wrap.drop_alias.call_args.args == ("one_alias",)
    for operation in [
        case.utility_wrap.list_collections,
        case.utility_wrap.list_aliases,
        case.utility_wrap.drop_alias,
        case.utility_wrap.drop_collection,
    ]:
        for invocation in operation.call_args_list:
            assert 0 < invocation.kwargs["timeout"] <= 120
    assert case.tear_down_collection_names == []
    assert case.connection_wrap.remove_connection.call_args_list == [call("default"), call("extra")]
    case.connection_wrap.add_connection.assert_called_once()


@pytest.mark.parametrize("raises", [False, True])
def test_drop_failure_reports_reason_after_other_cleanup(case, raises):
    case.tear_down_collection_names.extend(["one", "two"])

    def drop(name, **kwargs):
        if name == "one":
            if raises:
                raise TimeoutError("drop deadline expired")
            return "drop deadline expired", False
        return None, True

    case.utility_wrap.drop_collection.side_effect = drop
    with pytest.raises(AssertionError, match="drop collection one.*drop deadline expired") as failure:
        case._teardown_objects()
    assert failure.value.__cause__ is not None
    assert case.utility_wrap.drop_collection.call_count == 2
    case.utility_wrap.list_resource_groups.assert_called_once()
    case.connection_wrap.add_connection.assert_called_once()
    assert case.tear_down_collection_names == []


def test_alias_failure_does_not_hide_drop_or_skip_remaining_collections(case):
    case.tear_down_collection_names.extend(["one", "two"])
    case.utility_wrap.list_aliases.side_effect = [("alias lookup failed", False), ([], True)]
    with pytest.raises(AssertionError, match="list aliases for one.*alias lookup failed"):
        case._teardown_objects()
    assert case.utility_wrap.drop_collection.call_count == 2
    case.connection_wrap.add_connection.assert_called_once()


def test_listing_failure_is_reported_and_connections_still_reset(case):
    case.tear_down_collection_names.append("one")
    case.utility_wrap.list_collections.return_value = ("list failed", False)
    with pytest.raises(AssertionError, match="list collections.*list failed"):
        case._teardown_objects()
    case.utility_wrap.drop_collection.assert_not_called()
    case.connection_wrap.add_connection.assert_called_once()
    assert case.tear_down_collection_names == []


def test_connection_failure_is_reported_after_connection_reset(case):
    case.tear_down_collection_names.append("one")
    case.connection_wrap.has_connection.return_value = (False, True)
    case.connection_wrap.connect.side_effect = TimeoutError("connect deadline expired")
    with pytest.raises(AssertionError, match="prepare collection cleanup.*connect deadline expired"):
        case._teardown_objects()
    assert case.connection_wrap.connect.call_args.kwargs["timeout"] == 120
    case.connection_wrap.add_connection.assert_called_once()
    assert case.tear_down_collection_names == []


def test_wrapped_collection_failure_is_reported_without_duplicate_drop(case):
    case.collection_wrap.collection = SimpleNamespace(name="one", _using="default")
    case.collection_wrap.drop.return_value = ("wrapped drop failed", False)
    case.tear_down_collection_names.extend(["one", "two"])
    with pytest.raises(AssertionError, match="drop collection one.*wrapped drop failed"):
        case._teardown_objects()
    assert [c.args for c in case.utility_wrap.drop_collection.call_args_list] == [("two",)]
    assert 0 < case.collection_wrap.drop.call_args.kwargs["timeout"] <= 120
    case.connection_wrap.add_connection.assert_called_once()


def test_alias_collection_wrapper_is_not_dropped(case):
    case.collection_wrap.collection = SimpleNamespace(name="alias_example")
    case._teardown_objects()
    case.collection_wrap.drop.assert_not_called()
    case.utility_wrap.list_collections.assert_not_called()


@pytest.mark.parametrize("using", ["default", "other"])
def test_wrapped_collection_aliases_are_removed_before_drop(case, using):
    case.collection_wrap.collection = SimpleNamespace(name="one", _using=using)
    if using == "default":
        case.tear_down_collection_names.extend(["one", "one"])
    case.utility_wrap.list_aliases.return_value = (["one_alias"], True)
    case.collection_wrap.drop.return_value = (None, True)

    def drop(**kwargs):
        case.utility_wrap.drop_alias.assert_called_once()
        assert case.utility_wrap.drop_alias.call_args.kwargs["using"] == using
        return None, True

    case.collection_wrap.drop.side_effect = drop
    case._teardown_objects()
    assert case.utility_wrap.list_collections.call_args.kwargs["using"] == using
    assert case.utility_wrap.list_aliases.call_args.kwargs["using"] == using
    case.utility_wrap.drop_collection.assert_not_called()


def test_already_dropped_wrapped_collection_is_skipped(case):
    case.collection_wrap.collection = SimpleNamespace(name="one", _using="default")
    case.utility_wrap.list_collections.return_value = ([], True)
    case.tear_down_collection_names.append("one")
    case._teardown_objects()
    case.collection_wrap.drop.assert_not_called()
    case.utility_wrap.drop_collection.assert_not_called()


def test_same_name_in_other_connection_does_not_suppress_owned_default_collection(case):
    case.collection_wrap.collection = SimpleNamespace(name="one", _using="other")
    case.collection_wrap.drop.return_value = (None, True)
    case.tear_down_collection_names.append("one")
    case._teardown_objects()
    case.collection_wrap.drop.assert_called_once()
    case.utility_wrap.drop_collection.assert_called_once()
    assert case.utility_wrap.list_collections.call_count == 2
    assert [c.kwargs["using"] for c in case.utility_wrap.list_aliases.call_args_list] == ["other", "default"]


def test_late_success_is_reported_even_for_the_last_drop(case, client_base, monkeypatch):
    clock = SimpleNamespace(now=0)
    monkeypatch.setattr(client_base.time, "monotonic", lambda: clock.now)
    case.tear_down_collection_names.append("one")

    def late_drop(*args, **kwargs):
        clock.now = 121
        return None, True

    case.utility_wrap.drop_collection.side_effect = late_drop
    with pytest.raises(AssertionError, match="drop collection one.*cleanup deadline exceeded"):
        case._teardown_objects()
    case.connection_wrap.add_connection.assert_called_once()


def test_collection_budget_shrinks_and_is_not_renewed(case, client_base, monkeypatch):
    clock = SimpleNamespace(now=0)
    monkeypatch.setattr(client_base.time, "monotonic", lambda: clock.now)
    case.tear_down_collection_names.extend(["one", "two"])

    def listing(**kwargs):
        assert kwargs["timeout"] == 120
        clock.now += 10
        return ["one", "two"], True

    def aliases(name, **kwargs):
        assert name == "one"
        assert kwargs["timeout"] == 110
        clock.now += 20
        return [], True

    def drop(name, **kwargs):
        assert name == "one"
        assert kwargs["timeout"] == 90
        clock.now += 90
        return None, True

    case.utility_wrap.list_collections.side_effect = listing
    case.utility_wrap.list_aliases.side_effect = aliases
    case.utility_wrap.drop_collection.side_effect = drop
    with pytest.raises(AssertionError, match="drop collection two.*cleanup deadline exceeded"):
        case._teardown_objects()
    assert case.utility_wrap.drop_collection.call_count == 1
    case.connection_wrap.add_connection.assert_called_once()


def test_legacy_class_scope_keeps_registry_until_class_teardown(client_base, case):
    class Legacy(client_base.TestCaseClassBase):
        pass

    Legacy.setup_class(Legacy)
    first, second = Legacy(), Legacy()
    first.tear_down_collection_names.append("one")
    second.setup_method(lambda: None)
    assert second.tear_down_collection_names == ["one"]
    for attr in ["connection_wrap", "collection_wrap", "utility_wrap", "skip_global_role_cleanup"]:
        setattr(Legacy, attr, getattr(case, attr))
    Legacy.teardown_class(Legacy)
    assert Legacy.tear_down_collection_names == []
    assert Legacy.utility_wrap.drop_collection.call_args.args == ("one",)


@pytest.mark.parametrize("timeout,expected", [(None, 20), (17.5, 17.5)])
def test_utility_drop_passes_default_and_explicit_deadlines(client_base, monkeypatch, timeout, expected):
    wrapper = client_base.ApiUtilityWrapper()
    handler = Mock()
    handler.drop_collection.return_value = None
    monkeypatch.setattr(wrapper.ut, "_get_connection", lambda using: handler)
    _, success = wrapper.drop_collection("one", timeout=timeout)
    assert success
    assert handler.drop_collection.call_args.kwargs["timeout"] == expected


@pytest.mark.parametrize(
    "kwargs,expected", [({}, 120), ({"timeout": None}, 120), ({"timeout": 17.5}, 17.5), ({"timeout": 0}, 0)]
)
def test_milvus_client_drop_passes_default_and_explicit_deadlines(client_v2_case, client_v2_base, kwargs, expected):
    handler = Mock()
    handler.drop_collection.return_value = None
    client = object.__new__(MilvusClient)
    client._get_connection = lambda: handler
    client._generate_call_context = lambda **kwargs: None
    client_v2_case.tear_down_collection_names.extend(["one", "two"])
    # Keep the existing positional check_task/check_items calling convention.
    _, success = client_v2_case.drop_collection(
        client, "one", client_v2_base.ct.CheckTasks.check_nothing, None, **kwargs
    )
    assert success
    handler.drop_collection.assert_called_once_with("one", timeout=expected, context=None)
    assert client_v2_case.tear_down_collection_names == ["two"]


@pytest.mark.parametrize("check_nothing", [False, True])
def test_milvus_client_body_drop_deadline_preserves_failure_and_cleanup_registration(
    client_v2_case, client_v2_base, stalled_drop, monkeypatch, check_nothing
):
    monkeypatch.setattr(client_v2_base, "TIMEOUT", 0.2)
    client_v2_case.tear_down_collection_names.append("one")
    start = time.monotonic()
    if check_nothing:
        result, success = client_v2_case.drop_collection(
            stalled_drop.client, "one", check_task=client_v2_base.ct.CheckTasks.check_nothing
        )
        assert not success
        assert "DEADLINE_EXCEEDED" in result.message
    else:
        with pytest.raises(AssertionError, match="Response of API drop_collection"):
            client_v2_case.drop_collection(stalled_drop.client, "one")
    assert time.monotonic() - start < 5
    assert stalled_drop.canceled.wait(1)
    assert stalled_drop.observed_deadlines and all(0 < timeout < 1 for timeout in stalled_drop.observed_deadlines)
    assert client_v2_case.tear_down_collection_names == ["one"]


def test_stalled_body_drop_does_not_kill_xdist_worker(pytester):
    pytester.makeconftest(
        f"""
        import sys
        sys.path.insert(0, {str(Path(__file__).resolve().parent)!r})
        from test_python_client_cleanup import (
            client_base, client_v2_base, client_v2_case, stalled_drop,
        )
        import pytest

        @pytest.fixture
        def body_drop(client_v2_case, client_v2_base, stalled_drop, monkeypatch):
            monkeypatch.setattr(client_v2_base, "TIMEOUT", 0.2)
            client_v2_case.tear_down_collection_names.append("one")
            yield client_v2_case, stalled_drop.client
            assert client_v2_case.tear_down_collection_names == ["one"]
            assert stalled_drop.canceled.wait(1)
            assert stalled_drop.observed_deadlines
            assert all(0 < value < 1 for value in stalled_drop.observed_deadlines)
        """
    )
    pytester.makepyfile(
        """
        import os
        from pathlib import Path
        import pytest

        pytestmark = pytest.mark.xdist_group("body_drop_deadline")

        def test_stalled_body_drop(body_drop):
            Path("drop-worker").write_text(str(os.getpid()))
            case, client = body_drop
            case.drop_collection(client, "one")

        def test_worker_continues_after_drop_failure():
            assert Path("drop-worker").read_text() == str(os.getpid())
        """
    )
    result = pytester.runpytest_subprocess(
        "-q",
        "-n",
        "2",
        "--dist",
        "loadgroup",
        "--reruns",
        "3",
        "--timeout",
        "10",
        "--timeout-method",
        "thread",
        "--junitxml=results.xml",
        timeout=45,
    )
    result.assert_outcomes(failed=1, passed=1)
    assert result.parseoutcomes()["rerun"] == 3
    assert result.ret == pytest.ExitCode.TESTS_FAILED
    output = result.stdout.str()
    assert "DEADLINE_EXCEEDED" in output
    assert "INTERNALERROR" not in output
    assert "node down" not in output
    report = ET.parse(pytester.path / "results.xml")
    assert len(report.findall(".//failure")) == 1
    assert report.find(".//error") is None


@pytest.mark.parametrize("body_fails", [False, True])
def test_pytest_reports_cleanup_error_and_preserves_body_failure(pytester, monkeypatch, body_fails):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parent))
    pytester.makeconftest(
        "from test_python_client_cleanup import case, client_base\n"
        "import pytest\n"
        "@pytest.fixture\n"
        "def cleanup_case(case):\n"
        "    case.tear_down_collection_names.append('one')\n"
        "    case.utility_wrap.drop_collection.return_value = ('drop deadline expired', False)\n"
        "    yield case\n"
        "    case._teardown_objects()\n"
    )
    pytester.makepyfile(
        "def test_probe(cleanup_case):\n"
        + ("    assert False, 'primary body failure'\n" if body_fails else "    pass\n")
    )
    result = pytester.runpytest_inprocess("-q", "--junitxml=results.xml")
    result.assert_outcomes(passed=not body_fails, failed=int(body_fails), errors=1)
    report = ET.parse(pytester.path / "results.xml")
    assert "Collection cleanup failed" in report.find(".//error").attrib["message"]
    if body_fails:
        assert "primary body failure" in report.find(".//failure").attrib["message"]
    assert "INTERNALERROR" not in result.stdout.str()


def test_real_grpc_deadline_reaches_sdk_and_becomes_teardown_error(case, client_base, stalled_drop, monkeypatch):
    wrapper = client_base.ApiUtilityWrapper()
    monkeypatch.setattr(wrapper.ut, "_get_connection", lambda using: stalled_drop.handler)
    monkeypatch.setattr(client_base, "COLLECTION_CLEANUP_TIMEOUT", 0.2)
    case.utility_wrap.drop_collection.side_effect = wrapper.drop_collection
    case.tear_down_collection_names.append("one")
    start = time.monotonic()
    with pytest.raises(AssertionError, match="(?s)drop collection one.*DEADLINE_EXCEEDED"):
        case._teardown_objects()
    assert time.monotonic() - start < 5
    assert stalled_drop.canceled.wait(1)
    assert stalled_drop.observed_deadlines and all(0 < timeout < 1 for timeout in stalled_drop.observed_deadlines)
    case.connection_wrap.add_connection.assert_called_once()
    assert case.tear_down_collection_names == []
