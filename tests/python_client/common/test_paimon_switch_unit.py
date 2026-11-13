"""Run the actual switch scenarios against deliberately broken source backends."""

import json
from copy import deepcopy
from types import SimpleNamespace

import conftest as _bootstrap  # noqa: F401 -- legacy wrapper/NumPy bootstrap, as in real E2E
import pytest
from common.paimon_fixture import PaimonOracle
from milvus_client import test_milvus_client_external_table_paimon as scenario


class Source:
    def __init__(self, kind, version):
        self.kind, self.version = kind, version
        self.source = f"s3://own/{kind}"

    def spec(self, version=None, scan_mode="auto", **extra):
        result = {"format": "paimon-table", "scan_mode": scan_mode}
        if version:
            result["snapshot_id"] = str(int(version[1:]))
        return json.dumps(dict(result, **extra))

    def publish(self, version):
        self.version = version

    def oracle(self, version=None):
        value = int((version or self.version)[1:]) + (10 if self.kind == "mor" else 0)
        return PaimonOracle(
            [{"id": value, "label": str(value), "value": float(value), "embedding": [float(value), 0.0], "group_id": 0}]
        )


class Backend:
    """Public API model; mutations independently break metadata or execution."""

    def __init__(self, mutation=None):
        self.mutation, self.sources, self.plain = mutation, {}, {}
        self.client = SimpleNamespace(refresh_external_collection=self.rpc)

    def source(self, name="smoke", version="s1"):
        source = Source(name, version)
        self.sources[source.source] = source
        return source

    def create(self, source, spec=None):
        self.url, self.spec = source.source, json.loads(spec or source.spec())
        return "collection"

    def rpc(self, collection_name, external_source=None, external_spec=None, **kwargs):
        url = external_source or self.url
        spec = json.loads(external_spec) if external_spec else deepcopy(self.spec)
        if spec.get("snapshot_id") == 999999:
            raise ValueError("missing snapshot")
        if self.mutation == "ignore-format" and spec["format"] != self.spec["format"]:
            return 1
        if self.mutation == "keep-direct-mode" and spec["format"] == "paimon-table":
            spec["scan_mode"] = "direct-file"
        if spec["format"] == "paimon-table":
            source = self.sources[url]
            if self.mutation == "force-direct-execution" and source.kind == "mor":
                raise AssertionError("direct-file cannot read native-only MOR S2")
            version = "s" + str(spec["snapshot_id"]) if "snapshot_id" in spec else None
            rows = source.oracle(version).rows
        else:
            rows = self.plain[url]
        if not (self.mutation == "format-metadata-only" and spec["format"] != "paimon-table"):
            self.rows = deepcopy(rows)
        self.url, self.spec = url, spec
        return 1

    def refresh(self, name, source=None, spec=None):
        return self.rpc(name, source.source if source else None, spec or (source.spec() if source else None))

    def index_load(self, name):
        pass

    def reload(self, name):
        pass

    def verify(self, name, oracle):
        oracle.assert_rows(self.rows)

    def persisted(self, name):
        return self.url, deepcopy(self.spec)

    def assert_source_spec(self, name, url, spec):
        scenario.PaimonCase.assert_source_spec(self, name, url, spec)

    def plain_source(self, source, fmt):
        url = "s3://own/" + fmt
        self.plain[url] = deepcopy(source.oracle().rows)
        return url, json.dumps({"format": fmt})

    def rejected_refresh(self, name, source, spec, terms):
        with pytest.raises(ValueError, match="snapshot"):
            self.refresh(name, source, spec)


def run_switch(monkeypatch, kind, mutation=None):
    monkeypatch.setattr(scenario, "wait_refresh", lambda *args, **kwargs: None)
    case = Backend(mutation)
    tests = scenario.TestMilvusClientPaimonIndependent
    if kind == "mode":
        tests.test_paimon_same_collection_mode_switch(None, case)
    else:
        tests.test_paimon_format_switch_and_failed_override(None, case, kind)


@pytest.mark.parametrize("kind", ["mode", "parquet", "lance-table"])
def test_switch_scenarios_accept_correct_backend(monkeypatch, kind):
    run_switch(monkeypatch, kind)


@pytest.mark.parametrize("fmt", ["parquet", "lance-table"])
@pytest.mark.parametrize("mutation", ["ignore-format", "format-metadata-only"])
def test_format_scenario_rejects_ignored_switch(monkeypatch, fmt, mutation):
    with pytest.raises(AssertionError):
        run_switch(monkeypatch, fmt, mutation)


@pytest.mark.parametrize("mutation", ["keep-direct-mode", "force-direct-execution"])
def test_mode_scenario_rejects_ignored_switch(monkeypatch, mutation):
    with pytest.raises(AssertionError):
        run_switch(monkeypatch, "mode", mutation)
