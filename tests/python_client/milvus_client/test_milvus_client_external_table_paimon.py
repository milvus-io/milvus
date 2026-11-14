"""Paimon external collections: default-deployment L0/L1/L2 API coverage.

SealedOnly contains immutable read checks; Independent changes source, schema or
indexes. Source data and GT come from the offline Java writer/batch-reader bundle.
No test changes server configuration or requires a JVM in the pytest runner.
"""

import io
import json
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
from base.client_v2_base import TestMilvusClientV2Base as _ClientBase
from common import common_func as cf
from common.common_type import CaseLabel
from common.external_table_common import build_external_source, build_external_spec
from common.paimon_fixture import (
    FixtureBundle,
    PaimonOracle,
    PaimonSource,
    progress_field,
    wait_actual_index,
    wait_refresh,
)
from milvus_client import test_milvus_client_external_table as _external
from pymilvus import DataType, Function, FunctionType, MilvusException

CORE = ("id", "label", "value", "embedding", "group_id")
minio_env = _external.minio_env
external_prefix = _external.external_prefix
SCALAR_TYPES = {
    "id": (DataType.INT64, {}),
    "label": (DataType.VARCHAR, {"max_length": 256}),
    "value": (DataType.DOUBLE, {}),
    "group_id": (DataType.INT32, {}),
    "score": (DataType.DOUBLE, {}),
    "doc": (DataType.VARCHAR, {"max_length": 1024}),
    "evolved": (DataType.VARCHAR, {"max_length": 256}),
    "bool_value": (DataType.BOOL, {}),
    "int8_value": (DataType.INT8, {}),
    "int16_value": (DataType.INT16, {}),
    "int32_value": (DataType.INT32, {}),
    "long_value": (DataType.INT64, {}),
    "geo": (DataType.GEOMETRY, {}),
    "float_value": (DataType.FLOAT, {}),
    "json_value": (DataType.JSON, {}),
    "arr": (DataType.ARRAY, {"element_type": DataType.INT64, "max_capacity": 16}),
    "ts": (DataType.TIMESTAMPTZ, {}),
    "all_null": (DataType.DOUBLE, {}),
}
VECTOR_TYPES = {
    "embedding": DataType.FLOAT_VECTOR,
    "new_embedding": DataType.FLOAT_VECTOR,
    "half_vec": DataType.FLOAT16_VECTOR,
    "bf16_vec": DataType.BFLOAT16_VECTOR,
    "int8_vec": DataType.INT8_VECTOR,
    "binary_vec": DataType.BINARY_VECTOR,
    "sparse": DataType.SPARSE_FLOAT_VECTOR,
}


@pytest.fixture(scope="module")
def paimon_bundle():
    return FixtureBundle()


class PaimonCase:
    """One case owns every collection and object prefix it creates."""

    def __init__(self, client, bundle, store, cfg, prefix, name_prefix="PaimonIndependent"):
        self.client, self.bundle, self.store, self.cfg = client, bundle, store, cfg
        self.prefix = prefix
        self.name_prefix = name_prefix
        self.collections = []
        self.sources = []

    def source(self, name="smoke", version="s1", suffix=None):
        source = PaimonSource(
            self.bundle,
            name,
            self.store,
            self.cfg,
            f"{self.prefix}/{suffix or name + '-' + str(len(self.sources))}",
        )
        self.sources.append(source)
        source.publish(version)
        return source

    def schema(self, source, fields=CORE, spec=None, **overrides):
        schema = self.client.create_schema(
            external_source=source.source,
            external_spec=spec if spec is not None else source.spec(),
            enable_dynamic_field=False,
        )
        for name in fields:
            options = {"external_field": name, "nullable": True}
            if name in VECTOR_TYPES:
                dtype = VECTOR_TYPES[name]
                if name != "sparse":
                    options["dim"] = source.dimension
            else:
                dtype, extra = SCALAR_TYPES[name]
                options.update(extra)
            options.update(overrides.get(name, {}))
            dtype = options.pop("datatype", dtype)
            schema.add_field(name, datatype=dtype, **options)
        return schema

    def create(self, source, fields=CORE, spec=None, schema=None):
        name = f"{self.name_prefix}_{uuid4().hex[:10]}"
        self.collections.append(name)  # Register before RPC so partial creation is cleaned up.
        self.client.create_collection(
            collection_name=name,
            schema=schema or self.schema(source, fields, spec),
            consistency_level="Strong",
            timeout=60,
        )
        return name

    def refresh(self, name, source=None, spec=None, expected="RefreshCompleted"):
        kwargs = {}
        if source is not None:
            kwargs.update(external_source=source.source, external_spec=spec if spec is not None else source.spec())
        elif spec is not None:
            raise ValueError("refresh overrides must pass the complete source/spec pair")
        job = self.client.refresh_external_collection(collection_name=name, timeout=30, **kwargs)
        return wait_refresh(self.client, job, expected=expected)

    def index_load(self, name, field="embedding", index_type="FLAT", metric="L2", params=None):
        indexes = self.client.prepare_index_params()
        indexes.add_index(
            field_name=field, index_name=field, index_type=index_type, metric_type=metric, params=params or {}
        )
        self.client.create_index(collection_name=name, index_params=indexes, timeout=180)
        self.client.load_collection(collection_name=name, timeout=180)

    def rows(self, name, fields=CORE, expression="id >= 0"):
        iterator = self.client.query_iterator(
            collection_name=name,
            filter=expression,
            output_fields=list(fields),
            batch_size=257,
            consistency_level="Strong",
            timeout=60,
        )
        rows = []
        try:
            while True:
                batch = iterator.next()
                if not batch:
                    return rows
                rows.extend(batch)
                assert len(rows) <= 100000, "iterator did not terminate within fixture row bound"
        finally:
            iterator.close()

    def verify(
        self,
        name,
        oracle,
        fields=CORE,
        field="embedding",
        metric="L2",
        min_recall=1.0,
        queries=None,
        expression="id >= 0",
        search_params=None,
        limit=10,
    ):
        rows = self.rows(name, fields, expression)
        oracle.assert_rows(rows, fields)
        count = self.client.query(collection_name=name, filter=expression, output_fields=["count(*)"], timeout=60)
        assert count[0]["count(*)"] == len(oracle.rows)
        assert oracle.rows, "empty-read behavior must be tested with explicit failure expectations"
        queries = queries or [oracle.rows[i][field] for i in (0, len(oracle.rows) // 2, -1)]
        wire_queries = [np.asarray(query, dtype=np.int8) for query in queries] if field == "int8_vec" else queries
        results = self.client.search(
            collection_name=name,
            data=wire_queries,
            anns_field=field,
            filter=expression,
            limit=min(limit, len(oracle.rows)),
            output_fields=list(fields),
            search_params={"metric_type": metric, "params": search_params or {}},
            timeout=60,
        )
        oracle.assert_search(
            results,
            queries,
            metric,
            min(limit, len(oracle.rows)),
            field=field,
            min_recall=min_recall,
            output_fields=fields,
        )
        return results

    def reload(self, name):
        self.client.release_collection(collection_name=name, timeout=60)
        self.client.load_collection(collection_name=name, timeout=180)

    def expect_rejected(self, action, terms):
        # Assertions/timeouts in our own verifier must never be mistaken for a server rejection.
        with pytest.raises(MilvusException) as error:
            action()
        assert any(term in str(error.value).lower() for term in terms), str(error.value)
        return str(error.value)

    def rejected_refresh(self, name, source, spec, terms):
        try:
            job = self.client.refresh_external_collection(
                collection_name=name,
                external_source=source.source,
                external_spec=spec,
                timeout=30,
            )
        except MilvusException as error:
            reason = str(error)
        else:
            progress = wait_refresh(self.client, job, expected="RefreshFailed")
            reason = progress_field(progress, "reason", "")
        assert any(term in reason.lower() for term in terms), reason

    def rejected_read_pipeline(self, name, source, terms):
        """Bad source/type/dim can fail at exploration, index build or first read."""
        try:
            job = self.client.refresh_external_collection(collection_name=name, timeout=30)
            progress = wait_refresh(self.client, job, expected=("RefreshCompleted", "RefreshFailed"))
            if progress_field(progress, "state") == "RefreshFailed":
                reason = progress_field(progress, "reason")
            else:
                self.index_load(name)
                self.rows(name)
                raise AssertionError("invalid source/mapping was successfully refreshed, loaded and read")
        except MilvusException as error:
            reason = str(error)
        assert any(term in reason.lower() for term in terms), reason

    def persisted(self, name):
        description = self.client.describe_collection(collection_name=name, timeout=30)
        return description["external_source"], json.loads(description["external_spec"])

    def assert_source_spec(self, name, source, spec):
        """Check durable routing properties without comparing redacted credentials."""
        spec = json.loads(spec)
        actual_source, actual = self.persisted(name)
        assert actual_source == source, "external_source did not switch"
        for key in ["format", "scan_mode", "snapshot_id"]:
            assert actual.get(key) == spec.get(key), f"persisted {key} did not switch"

    def plain_source(self, source, fmt):
        """Write exactly the independent reader's logical rows into Parquet/Lance."""
        rows = [{key: row[key] for key in CORE} for row in source.oracle().rows]
        schema = pa.schema(
            [
                ("id", pa.int64()),
                ("label", pa.string()),
                ("value", pa.float64()),
                ("embedding", pa.list_(pa.float32(), source.dimension)),
                ("group_id", pa.int32()),
            ]
        )
        table = pa.Table.from_pylist(rows, schema=schema)
        prefix = f"{self.prefix}/comparison-{fmt}"
        if fmt == "parquet":
            stream = io.BytesIO()
            pq.write_table(table, stream)
            self.store.put_object(
                self.cfg["bucket"], prefix + "/data.parquet", io.BytesIO(stream.getvalue()), len(stream.getvalue())
            )
        elif fmt == "lance-table":
            import lance

            with tempfile.TemporaryDirectory(prefix="paimon-compare-") as temporary:
                path = Path(temporary) / "table.lance"
                lance.write_dataset(table, str(path))
                for file in path.rglob("*"):
                    if file.is_file():
                        self.store.fput_object(
                            self.cfg["bucket"], f"{prefix}/{file.relative_to(path).as_posix()}", str(file)
                        )
        else:
            raise ValueError(fmt)
        return build_external_source(self.cfg, prefix), build_external_spec(self.cfg, fmt=fmt)

    def close(self):
        errors = []
        for name in reversed(self.collections):
            try:
                if self.client.has_collection(collection_name=name, timeout=30):
                    self.client.drop_collection(collection_name=name, timeout=60)
            except Exception as error:
                errors.append(f"{name}: {error}")
        try:
            self.client.close()
        except Exception as error:
            errors.append(f"close client: {error}")
        assert not errors, f"Paimon collection cleanup failed: {errors}"


@pytest.fixture
def paimon_case(request, paimon_bundle, minio_env, external_prefix):
    store, cfg = minio_env
    case = PaimonCase(
        request.instance._client(),
        paimon_bundle,
        store,
        cfg,
        external_prefix["key"],
        request.cls.__name__[:35] + "_" + request.function.__name__[-65:],
    )
    # This fixture depends on external_prefix: drop collections BEFORE source cleanup.
    request.addfinalizer(case.close)
    return case


class TestMilvusClientPaimonIndependent(_ClientBase):
    """When to add tests here: source/schema/index mutation. Fixture: paimon_case."""

    @pytest.mark.tags(CaseLabel.L0)
    def test_paimon_append_auto(self, paimon_case):
        """
        target: PAI-L0-01
        method: refresh/query/search/reload/drop
        expected: exact source GT.
        """
        case = paimon_case
        source = case.source()
        name = case.create(source)
        case.refresh(name)
        case.index_load(name)
        case.verify(name, source.oracle())
        filtered = source.oracle().filtered(lambda row: row["group_id"] == 2 and row["id"] >= 20)
        case.verify(name, filtered, expression="group_id == 2 and id >= 20")
        case.reload(name)
        case.verify(name, source.oracle())
        keys = set(source._published)
        case.client.drop_collection(collection_name=name, timeout=60)
        assert {
            obj.object_name.removeprefix(source.prefix + "/")
            for obj in case.store.list_objects(case.cfg["bucket"], prefix=source.prefix + "/", recursive=True)
        } == keys

    @pytest.mark.tags(CaseLabel.L0)
    def test_paimon_mor_refresh_and_pinned_snapshot(self, paimon_case):
        """
        target: PAI-L0-02
        method: MOR S1→S2 with pinned control
        expected: update/delete/add GT.
        """
        case = paimon_case
        source = case.source("mor")
        name, pinned = case.create(source), case.create(source, spec=source.spec("s1"))
        for collection in [name, pinned]:
            case.refresh(collection)
            case.index_load(collection)
            case.verify(collection, source.oracle("s1"))
        source.publish("s2")
        case.refresh(name)
        case.verify(name, source.oracle("s2"))
        case.refresh(pinned)
        case.verify(pinned, source.oracle("s1"))
        case.reload(name)
        case.verify(name, source.oracle("s2"))

    @pytest.mark.tags(CaseLabel.L1)
    def test_paimon_auto_mixed_snapshot(self, paimon_case):
        """
        target: PAI-L1-01B
        method: one schema-evolved auto snapshot
        expected: full mixed GT.
        """
        case = paimon_case
        source = case.source("mixed", "s2")
        name = case.create(source)
        case.refresh(name)
        case.index_load(name)
        case.verify(name, source.oracle())
        case.reload(name)
        case.verify(name, source.oracle())

    @pytest.mark.tags(CaseLabel.L1)
    def test_paimon_same_collection_mode_switch(self, paimon_case):
        """
        target: PAI-L1-01C
        method: direct→split→auto on S1, then native-only MOR S2 and append S2
        expected: durable source/spec/mode and exact GT before/after every reload.
        """
        case = paimon_case
        source = case.source("append")
        name = case.create(source, spec=source.spec("s1", scan_mode="direct-file"))
        case.refresh(name)
        case.index_load(name)
        for mode in ["direct-file", "data-split", "auto"]:
            spec = source.spec("s1", scan_mode=mode)
            case.refresh(name, source, spec)
            case.assert_source_spec(name, source.source, spec)
            case.verify(name, source.oracle("s1"))
            case.reload(name)
            case.assert_source_spec(name, source.source, spec)
            case.verify(name, source.oracle("s1"))
        native = case.source("mor", "s2")
        assert native.oracle().rows != source.oracle("s1").rows
        spec = native.spec("s2", scan_mode="data-split")
        case.refresh(name, native, spec)
        case.assert_source_spec(name, native.source, spec)
        case.verify(name, native.oracle())
        case.reload(name)
        case.assert_source_spec(name, native.source, spec)
        case.verify(name, native.oracle())
        source.publish("s2")
        assert source.oracle("s2").rows != native.oracle().rows
        spec = source.spec("s2")
        case.refresh(name, source, spec)
        case.assert_source_spec(name, source.source, spec)
        case.verify(name, source.oracle("s2"))
        case.reload(name)
        case.assert_source_spec(name, source.source, spec)
        case.verify(name, source.oracle("s2"))

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mode", ["auto", "data-split"])
    def test_paimon_vortex_source(self, paimon_case, mode):
        """
        target: PAI-L1-01A
        method: certified converted Vortex table
        expected: independent logical GT.
        """
        case = paimon_case
        source = case.source("vortex")
        name = case.create(source, spec=source.spec(scan_mode=mode))
        case.refresh(name)
        case.index_load(name)
        case.verify(name, source.oracle())
        case.reload(name)
        case.verify(name, source.oracle())

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mode", ["auto", "data-split"])
    def test_paimon_bitmap32_delete_refresh(self, paimon_case, mode):
        """
        target: PAI-L1-02
        method: bitmap32 S1→S2
        expected: deleted rows absent, raw rows aligned.
        """
        case = paimon_case
        source = case.source("dv")
        name = case.create(source, spec=source.spec(scan_mode=mode))
        case.refresh(name)
        case.index_load(name)
        case.verify(name, source.oracle("s1"))
        source.publish("s2")
        case.refresh(name)
        case.verify(name, source.oracle("s2"))

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("as_string", [False, True], ids=["number", "string"])
    def test_paimon_pinned_and_manual_latest(self, paimon_case, as_string):
        """
        target: PAI-L1-03
        method: pin S1, publish S2 then manual refresh
        expected: no auto drift.
        """
        case = paimon_case
        source = case.source("append")
        snapshot = source.snapshot("s1")
        pinned_spec = source.spec(snapshot_id=str(snapshot) if as_string else snapshot)
        latest, pinned = case.create(source), case.create(source, spec=pinned_spec)
        for name in [latest, pinned]:
            case.refresh(name)
            case.index_load(name)
        source.publish("s2")
        for name in [latest, pinned]:
            case.verify(name, source.oracle("s1"))
            case.refresh(name)
        case.verify(latest, source.oracle("s2"))
        case.verify(pinned, source.oracle("s1"))

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("fixture,mode", [("append", "auto"), ("mor", "data-split")])
    def test_paimon_noop_and_incremental_refresh(self, paimon_case, fixture, mode):
        """
        target: PAI-L1-04
        method: repeated refresh around S1/S2
        expected: stable complete multiset.
        """
        case = paimon_case
        source = case.source(fixture)
        name = case.create(source, spec=source.spec(scan_mode=mode))
        case.refresh(name)
        case.index_load(name)
        for version in ["s1", "s1", "s2", "s2"] + (["s3"] if fixture == "mor" else []):
            source.publish(version)
            case.refresh(name)
            case.verify(name, source.oracle(version))
        case.reload(name)
        case.verify(name, source.oracle())

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mode", ["auto", "data-split"])
    def test_paimon_multi_bucket(self, paimon_case, mode):
        """
        target: PAI-L1-05
        method: multi-bucket native iteration/groups
        expected: no omissions/duplicates.
        """
        case = paimon_case
        source = case.source("multi_bucket", "s2")
        name = case.create(source, spec=source.spec(scan_mode=mode))
        case.refresh(name)
        case.index_load(name)
        case.verify(name, source.oracle())
        for group in range(5):
            oracle = source.oracle().filtered(lambda row: row["group_id"] == group)
            oracle.assert_rows(case.rows(name, expression=f"group_id == {group}"), CORE)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "fixture,mode,index_type,metric,params,search_params",
        [
            ("dv", "direct-file", "HNSW", "COSINE", {"M": 16, "efConstruction": 200}, {"ef": 200}),
            ("mor", "data-split", "IVF_FLAT", "L2", {"nlist": 32}, {"nprobe": 32}),
        ],
    )
    def test_paimon_ann_built_before_incremental_refresh(
        self,
        paimon_case,
        fixture,
        mode,
        index_type,
        metric,
        params,
        search_params,
    ):
        """
        target: PAI-L1-06
        method: actual ANN S1→loaded S2→reload
        expected: coverage/recall/new raw GT.
        """
        case = paimon_case
        source = case.source(fixture)
        name = case.create(source, spec=source.spec(scan_mode=mode))
        case.refresh(name)
        case.index_load(name, index_type=index_type, metric=metric, params=params)
        for version in ["s1", "s2"]:
            if version == "s2":
                source.publish(version)
                case.refresh(name)  # Keep loaded collection and existing index definition.
                source.oracle(version).assert_rows(case.rows(name), CORE)
            stats = wait_actual_index(case.client, name, "embedding", len(source.oracle(version).rows))
            assert int(stats["indexed_rows"]) >= 1024
            case.verify(name, source.oracle(version), metric=metric, min_recall=0.95, search_params=search_params)
        # Old-vector probes must rank against S2; an updated business key can still be nearest.
        old_queries = [source.oracle("s1").rows[i]["embedding"] for i in [0, 2048, 4095]]
        for reload in [False, True]:
            if reload:
                case.reload(name)
            case.verify(
                name,
                source.oracle("s2"),
                metric=metric,
                min_recall=0.95,
                search_params=search_params,
                queries=old_queries,
            )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mode", ["direct-file", "data-split"])
    def test_paimon_add_external_fields(self, paimon_case, mode):
        """
        target: PAI-L1-08
        method: add source scalar/vector mappings then refresh
        expected: new fields GT.
        """
        case = paimon_case
        source = case.source("append")
        name = case.create(source, spec=source.spec(scan_mode=mode))
        case.refresh(name)
        case.index_load(name)
        case.client.release_collection(collection_name=name, timeout=60)
        case.client.add_collection_field(
            collection_name=name,
            field_name="score",
            data_type=DataType.DOUBLE,
            nullable=True,
            external_field="score",
            timeout=60,
        )
        case.client.add_collection_field(
            collection_name=name,
            field_name="new_embedding",
            data_type=DataType.FLOAT_VECTOR,
            dim=source.dimension,
            nullable=True,
            external_field="new_embedding",
            timeout=60,
        )
        case.refresh(name)
        case.index_load(name, field="new_embedding")
        fields = (*CORE, "score", "new_embedding")
        case.verify(name, source.oracle(), fields, field="new_embedding")
        case.reload(name)
        case.verify(name, source.oracle(), fields, field="new_embedding")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mode", ["direct-file", "data-split"])
    def test_paimon_parquet_lance_normal_parity(self, paimon_case, mode):
        """
        target: PAI-L1-10
        method: same logical data in four sources
        expected: query/raw/search parity.
        """
        case = paimon_case
        source = case.source("append")
        names = [case.create(source, spec=source.spec(scan_mode=mode))]
        for fmt in ["parquet", "lance-table"]:
            url, spec = case.plain_source(source, fmt)
            schema = case.schema(source, spec=spec)
            schema.external_source = url
            names.append(case.create(source, schema=schema))
        schema = case.client.create_schema(enable_dynamic_field=False)
        for field in CORE:
            dtype, options = (
                (DataType.FLOAT_VECTOR, {"dim": source.dimension}) if field == "embedding" else SCALAR_TYPES[field]
            )
            schema.add_field(
                field, datatype=dtype, **options, **({"is_primary": True} if field == "id" else {"nullable": True})
            )
        normal = case.create(source, schema=schema)
        case.client.insert(
            collection_name=normal, data=[{f: row[f] for f in CORE} for row in source.oracle().rows], timeout=60
        )
        case.client.flush(collection_name=normal, timeout=180)
        for name in names:
            case.refresh(name)
        for name in [*names, normal]:
            case.index_load(name)
            case.verify(name, source.oracle())
            case.reload(name)
            case.verify(name, source.oracle())

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("fixture,mode", [("dv", "direct-file"), ("mor", "data-split")])
    def test_paimon_bm25_text_match_refresh(self, paimon_case, fixture, mode):
        """
        target: PAI-L1-09
        method: identical analyzer/corpus normal control S1→S2
        expected: BM25/text/raw parity.
        """
        case = paimon_case
        source = case.source(fixture)
        fields = ("id", "doc", "embedding")

        def schema(external):
            result = case.client.create_schema(
                enable_dynamic_field=False,
                **(
                    {"external_source": source.source, "external_spec": source.spec(scan_mode=mode)} if external else {}
                ),
            )
            result.add_field("id", DataType.INT64, **({"external_field": "id"} if external else {"is_primary": True}))
            result.add_field(
                "doc",
                DataType.VARCHAR,
                max_length=1024,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params={"tokenizer": "standard"},
                **({"external_field": "doc"} if external else {}),
            )
            result.add_field(
                "embedding",
                DataType.FLOAT_VECTOR,
                dim=source.dimension,
                **({"external_field": "embedding"} if external else {}),
            )
            result.add_field("bm25", DataType.SPARSE_FLOAT_VECTOR)
            result.add_function(
                Function(
                    name="doc_bm25",
                    function_type=FunctionType.BM25,
                    input_field_names=["doc"],
                    output_field_names=["bm25"],
                )
            )
            return result

        external = case.create(source, schema=schema(True))
        for version in ["s1", "s2"]:
            if version == "s2":
                source.publish(version)
            # A fresh normal collection avoids tombstoned S1 segments polluting BM25 IDF statistics.
            normal = case.create(source, schema=schema(False))
            oracle = source.oracle(version)
            case.client.insert(
                collection_name=normal, data=[{f: row[f] for f in fields} for row in oracle.rows], timeout=60
            )
            case.client.flush(collection_name=normal, timeout=180)
            case.refresh(external)
            for name in [external, normal]:
                if name == normal or version == "s1":
                    indexes = case.client.prepare_index_params()
                    indexes.add_index(field_name="embedding", index_type="FLAT", metric_type="L2")
                    indexes.add_index(field_name="bm25", index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
                    case.client.create_index(collection_name=name, index_params=indexes, timeout=180)
                    case.client.load_collection(collection_name=name, timeout=180)
            for reload in [False, True]:
                if reload:
                    case.reload(external)
                for name in [external, normal]:
                    oracle.assert_rows(case.rows(name, fields, "TEXT_MATCH(doc, 'topic')"), fields)
                searches = []
                for name in [external, normal]:
                    results = case.client.search(
                        collection_name=name,
                        data=["record 2048"],
                        anns_field="bm25",
                        output_fields=["id", "doc"],
                        limit=len(oracle.rows),
                        search_params={"metric_type": "BM25", "params": {}},
                        timeout=60,
                    )
                    assert len(results) == 1
                    hits = results[0]
                    assert len(hits) == len(oracle.rows), "every source document contains record"
                    scores = [hit["distance"] for hit in hits]
                    assert scores == sorted(scores, reverse=True)
                    expected = {row["id"]: row["doc"] for row in oracle.rows}
                    assert len({hit["entity"]["id"] for hit in hits}) == len(hits)
                    for hit in hits:
                        assert hit["entity"]["doc"] == expected[hit["entity"]["id"]]
                    searches.append({hit["entity"]["id"]: hit["distance"] for hit in hits})
                assert searches[0] == pytest.approx(searches[1], rel=1e-4, abs=1e-5)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("entry", ["create", "refresh"])
    @pytest.mark.parametrize(
        "value,valid",
        [
            ("__missing__", True),
            ("", True),
            (None, True),
            ("auto", True),
            ("direct-file", True),
            ("data-split", True),
            ("native", False),
            ("AUTO", False),
            (1, False),
            (True, False),
            ({}, False),
            ([], False),
        ],
    )
    def test_paimon_scan_mode_validation(self, paimon_case, entry, value, valid):
        """
        target: PAI-L2-01 mode
        method: create/refresh typed inputs
        expected: one fixed outcome per input.
        """
        case = paimon_case
        source = case.source()
        spec = json.loads(source.spec())
        if value == "__missing__":
            spec.pop("scan_mode")
        else:
            spec["scan_mode"] = value
        encoded = json.dumps(spec)
        if entry == "create":
            if not valid:
                case.expect_rejected(lambda: case.create(source, spec=encoded), ["scan_mode", "scan mode", "unmarshal"])
                return
            name = case.create(source, spec=encoded)
        else:
            name = case.create(source)
            case.refresh(name)
            case.index_load(name)
            before = case.persisted(name)
            if not valid:
                case.expect_rejected(
                    lambda: case.client.refresh_external_collection(
                        collection_name=name, external_source=source.source, external_spec=encoded, timeout=30
                    ),
                    ["scan_mode", "scan mode", "unmarshal"],
                )
                assert case.persisted(name) == before
                case.verify(name, source.oracle())
                return
            case.refresh(name, source, encoded)
        if entry == "create":
            case.refresh(name)
            case.index_load(name)
        case.verify(name, source.oracle())

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("entry", ["create", "refresh"])
    @pytest.mark.parametrize(
        "value,outcome",
        [
            (None, "latest"),
            (-1, "latest"),
            ("-1", "latest"),
            ("__s1_number__", "s1"),
            ("__s1_string__", "s1"),
            (-2, "property"),
            ("-2", "property"),
            (0, "missing"),
            ("0", "missing"),
            (999999, "missing"),
            ("999999", "missing"),
            ("invalid", "parser"),
            (1.5, "parser"),
            (True, "parser"),
            ({}, "parser"),
            ([], "parser"),
            (2**63, "parser"),
        ],
    )
    def test_paimon_snapshot_validation(self, paimon_case, entry, value, outcome):
        """
        target: PAI-L2-01 snapshot
        method: S1 retained/latest S2
        expected: -1 latest, lower invalid, missing fails.
        """
        case = paimon_case
        source = case.source("smoke", "s2")
        if value == "__s1_number__":
            value = source.snapshot("s1")
        elif value == "__s1_string__":
            value = str(source.snapshot("s1"))
        spec = source.spec(snapshot_id=value)
        failure = outcome in {"property", "missing", "parser"}
        if entry == "create":
            if outcome == "parser":
                case.expect_rejected(
                    lambda: case.create(source, spec=spec), ["snapshot", "unmarshal", "parse", "int64"]
                )
                return
            # Property validation is allowed at create or at refresh, never require the former.
            try:
                name = case.create(source, spec=spec)
            except MilvusException as error:
                assert failure and "snapshot" in str(error).lower(), str(error)
                return
        else:
            name = case.create(source, spec=source.spec("s1"))
            case.refresh(name)
            case.index_load(name)
            before = case.persisted(name)
        if failure:
            case.rejected_refresh(name, source, spec, ["snapshot", "int64", "unmarshal", "parse"])
            if entry == "refresh":
                assert case.persisted(name) == before
                case.verify(name, source.oracle("s1"))
                case.reload(name)
                case.verify(name, source.oracle("s1"))
        else:
            case.refresh(name, source, spec)
            if entry == "create":
                case.index_load(name)
            case.verify(name, source.oracle("s1" if outcome == "s1" else "s2"))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("variant", ["case-order", "duplicate-mode", "duplicate-null"])
    def test_paimon_spec_normalization(self, paimon_case, variant):
        """
        target: PAI-L2-01 normalization
        method: Go typed decoder inputs
        expected: accepted and exact snapshot GT.
        """
        case = paimon_case
        source = case.source("mixed", "s2")
        spec = json.loads(source.spec("s2"))
        if variant == "case-order":
            encoded = json.dumps({key.upper(): value for key, value in reversed(list(spec.items()))})
        else:
            spec.pop("scan_mode")
            tail = (
                '"scan_mode":"direct-file","scan_mode":"data-split"'
                if variant == "duplicate-mode"
                else '"scan_mode":"data-split","scan_mode":null'
            )
            encoded = json.dumps(spec)[:-1] + "," + tail + "}"
        name = case.create(source, spec=encoded)
        case.refresh(name)
        case.index_load(name)
        case.verify(name, source.oracle())

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mode,accepted", [("__missing__", True), ("", True), (None, True), ("auto", False)])
    def test_non_paimon_scan_mode(self, paimon_case, mode, accepted):
        """
        target: PAI-L2-01 format restriction
        method: Parquet mode input
        expected: only missing/empty/null accepted.
        """
        case = paimon_case
        source = case.source()
        url, spec = case.plain_source(source, "parquet")
        spec = json.loads(spec)
        if mode != "__missing__":
            spec["scan_mode"] = mode
        schema = case.schema(source, spec=json.dumps(spec))
        schema.external_source = url
        if not accepted:
            case.expect_rejected(lambda: case.create(source, schema=schema), ["scan_mode", "paimon", "scan mode"])
            return
        name = case.create(source, schema=schema)
        case.refresh(name)
        case.index_load(name)
        case.verify(name, source.oracle())

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric", ["IP", "COSINE"])
    @pytest.mark.parametrize("limit,nq,offset", [(1, 1, 0), (10, 3, 7), (200, 2, 0)])
    def test_paimon_search_limit_nq_offset(self, paimon_case, metric, limit, nq, offset):
        """
        target: PAI-L2-05
        method: exact IP/cosine with nq/limit/offset
        expected: independent distance slice and raw rows.
        """
        case = paimon_case
        source = case.source()
        name = case.create(source)
        case.refresh(name)
        case.index_load(name, metric=metric)
        oracle = source.oracle()
        queries = [row["embedding"] for row in oracle.rows[:nq]]
        hits = case.client.search(
            collection_name=name,
            data=queries,
            anns_field="embedding",
            limit=limit,
            output_fields=list(CORE),
            search_params={"metric_type": metric, "offset": offset},
            timeout=60,
        )
        assert len(hits) == nq
        for query, result in zip(queries, hits, strict=True):
            _, scores = oracle.topk(query, metric, len(oracle.rows))
            np.testing.assert_allclose(
                [hit["distance"] for hit in result], scores[offset : offset + limit], rtol=1e-4, atol=1e-5
            )
            ids = [hit["entity"]["id"] for hit in result]
            assert len(set(ids)) == len(ids)
            oracle.filtered(lambda row: row["id"] in ids).assert_rows([hit["entity"] for hit in result], CORE)
            for hit in result:
                _, score = oracle.filtered(lambda row: row["id"] == hit["entity"]["id"]).topk(query, metric, 1)
                assert hit["distance"] == pytest.approx(score[0], rel=1e-4, abs=1e-5)

    @pytest.mark.tags(CaseLabel.L2)
    def test_paimon_range_search(self, paimon_case):
        """
        target: PAI-L2-05
        method: exact L2 radius from independent GT
        expected: complete range result and scores.
        """
        case = paimon_case
        source = case.source()
        name = case.create(source)
        case.refresh(name)
        case.index_load(name)
        oracle = source.oracle()
        query = oracle.rows[0]["embedding"]
        _, all_distances = oracle.topk(query, "L2", len(oracle.rows))
        radius = float((all_distances[49] + all_distances[50]) / 2)
        subset = oracle.filtered(lambda row: np.square(np.asarray(row["embedding"]) - query).sum() < radius)
        results = case.client.search(
            collection_name=name,
            data=[query],
            anns_field="embedding",
            limit=100,
            output_fields=list(CORE),
            search_params={"metric_type": "L2", "params": {"radius": radius, "range_filter": -1}},
            timeout=60,
        )
        subset.assert_search(results, [query], "L2", len(subset.rows), output_fields=CORE)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mode", ["auto", "direct-file", "data-split"])
    def test_paimon_one_row_succeeds(self, paimon_case, mode):
        """
        target: PAI-L2-08A
        method: real one-row snapshot
        expected: success and exact top1/raw output.
        """
        case = paimon_case
        source = case.source("one")
        name = case.create(source, spec=source.spec(scan_mode=mode))
        case.refresh(name)
        case.index_load(name)
        case.verify(name, source.oracle(), limit=1)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("first_fill", [False, True])
    @pytest.mark.parametrize("mode", ["auto", "data-split"])
    def test_paimon_all_deleted_empty_exploration(self, paimon_case, first_fill, mode):
        """
        target: PAI-L2-08B/C
        method: genuine all-deleted source
        expected: failed exploration, old view intact.
        """
        case = paimon_case
        source = case.source("mor", "empty" if first_fill else "s1")
        name = case.create(source, spec=source.spec(scan_mode=mode))
        if not first_fill:
            case.refresh(name)
            case.index_load(name)
            before = case.persisted(name)
            source.publish("empty")
        progress = case.refresh(name, expected="RefreshFailed")
        reason = progress_field(progress, "reason", "").lower()
        assert any(
            term in reason for term in ["no files", "no data", "empty", "at least one explored file", "no readable"]
        ), reason
        if not first_fill:
            assert case.persisted(name) == before
            case.verify(name, source.oracle("s1"))
            case.reload(name)
            case.verify(name, source.oracle("s1"))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("kind", ["missing", "case", "dimension", "type"])
    def test_paimon_bad_mapping(self, paimon_case, kind):
        """
        target: PAI-L2-02
        method: invalid column/type/dim mapping
        expected: explicit failure, never successful empty data.
        """
        case = paimon_case
        source = case.source()
        options = (
            {"id": {"external_field": "absent" if kind == "missing" else "ID"}}
            if kind in {"missing", "case"}
            else {"embedding": {"dim": 127} if kind == "dimension" else {"datatype": DataType.INT64, "dim": None}}
        )
        if kind == "type":
            options = {"label": {"datatype": DataType.INT64, "max_length": 256}}
        name = case.create(source, schema=case.schema(source, **options))
        case.rejected_read_pipeline(name, source, ["field", "column", "dim", "type", "cast", "schema"])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mode", ["direct-file", "data-split"])
    @pytest.mark.parametrize("scalar_index", [False, True])
    def test_paimon_scalar_null_array_json_timestamp(self, paimon_case, mode, scalar_index):
        """
        target: PAI-L2-03
        method: full Java scalar oracle with/without scalar index
        expected: raw/null/filter equality.
        """
        case = paimon_case
        source = case.source("wide")
        fields = tuple(field for field in source.fields if field in SCALAR_TYPES) + ("embedding",)
        name = case.create(source, fields=fields, spec=source.spec(scan_mode=mode))
        case.refresh(name)
        indexes = case.client.prepare_index_params()
        indexes.add_index(field_name="embedding", index_type="FLAT", metric_type="L2")
        if scalar_index:
            indexes.add_index(field_name="int32_value", index_type="INVERTED")
            indexes.add_index(field_name="bool_value", index_type="BITMAP")
        case.client.create_index(collection_name=name, index_params=indexes, timeout=180)
        case.client.load_collection(collection_name=name, timeout=180)
        for reload in [False, True]:
            if reload:
                case.reload(name)
            case.verify(name, source.oracle(), fields)
            for expression, predicate in [
                ("bool_value is null", lambda row: row["bool_value"] is None),
                ("float_value is null", lambda row: row["float_value"] is None),
                ("all_null is null", lambda row: row["all_null"] is None),
                ("arr is null", lambda row: row["arr"] is None),
                (
                    "json_value['g'] == 2 and int32_value >= 70",
                    lambda row: row["json_value"]["g"] == 2 and row["int32_value"] >= 70,
                ),
            ]:
                expected = source.oracle().filtered(predicate)
                expected.assert_rows(case.rows(name, fields, expression), fields)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mode", ["direct-file", "data-split"])
    @pytest.mark.parametrize(
        "field,index,metric",
        [
            ("half_vec", "FLAT", "L2"),
            ("bf16_vec", "FLAT", "COSINE"),
            ("int8_vec", "HNSW", "IP"),
            ("binary_vec", "BIN_FLAT", "HAMMING"),
            ("sparse", "SPARSE_INVERTED_INDEX", "IP"),
        ],
    )
    def test_paimon_vector_physical_encodings(self, paimon_case, mode, field, index, metric):
        """
        target: PAI-L2-04
        method: independent decoded bytes/MAP oracle
        expected: quantized raw and exact distances.
        """
        case = paimon_case
        source = case.source("wide")
        fields = ("id", "value", field)
        name = case.create(source, fields=fields, spec=source.spec(scan_mode=mode))
        case.refresh(name)
        params = {"M": 8, "efConstruction": 200} if index == "HNSW" else {}
        search_params = {"ef": 200} if index == "HNSW" else {}
        case.index_load(name, field=field, index_type=index, metric=metric, params=params)
        case.verify(name, source.oracle(), fields, field=field, metric=metric, search_params=search_params)
        case.reload(name)
        case.verify(name, source.oracle(), fields, field=field, metric=metric, search_params=search_params)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "fixture,mode,outcome",
        [
            ("partitioned", "auto", "success"),
            ("partitioned", "data-split", "success"),
            ("mixed", "data-split", "success"),
            ("mixed", "direct-file", "schema"),
            ("mor", "direct-file", "merge"),
        ],
    )
    def test_paimon_partition_schema_and_unsafe_direct(self, paimon_case, fixture, mode, outcome):
        """
        target: PAI-L2-06
        method: partition/schema-evolved/MOR fixtures
        expected: batch semantics or explicit unsafe-route rejection.
        """
        case = paimon_case
        source = case.source(fixture, "s2")
        fields = (*CORE, "evolved") if fixture == "mixed" else CORE
        name = case.create(source, fields=fields, spec=source.spec(scan_mode=mode))
        if outcome != "success":
            case.rejected_refresh(name, source, source.spec(scan_mode=mode), [outcome, "direct", "split"])
            return
        case.refresh(name)
        case.index_load(name)
        case.verify(name, source.oracle(), fields)

    @pytest.mark.tags(CaseLabel.L2)
    def test_paimon_bitmap64_direct_rejected(self, paimon_case):
        """
        target: PAI-L2-10
        method: Java bitmap64 source in direct mode
        expected: explicit bitmap64 limitation.
        """
        case = paimon_case
        source = case.source("bitmap64", "s2")
        spec = source.spec(scan_mode="direct-file")
        name = case.create(source, spec=spec)
        case.rejected_refresh(name, source, spec, ["bitmap64"])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dependency", ["schema", "snapshot", "manifest", "data"])
    def test_paimon_truncated_source_dependency(self, paimon_case, dependency):
        """
        target: PAI-L2-02
        method: deterministically truncate own source
        expected: explicit read/refresh failure.
        """
        case = paimon_case
        source = case.source()
        patterns = {"schema": "schema/schema-", "snapshot": "snapshot/snapshot-", "manifest": "manifest/manifest-"}
        path = next(
            path
            for path in source._published
            if (path.endswith(".parquet") if dependency == "data" else path.startswith(patterns[dependency]))
        )
        case.store.put_object(case.cfg["bucket"], f"{source.prefix}/{path}", io.BytesIO(b"broken"), 6)
        name = case.create(source)
        case.rejected_read_pipeline(
            name, source, ["error", "invalid", "parquet", "schema", "snapshot", "manifest", "eof", "decode", "json"]
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_paimon_missing_snapshot_retry(self, paimon_case):
        """
        target: PAI-L2-13/14
        method: remove own metadata before first read then repair
        expected: failure and successful retry.
        """
        case = paimon_case
        source = case.source()
        snapshot = f"snapshot/snapshot-{source.snapshot('s1')}"
        source.delete_owned(snapshot)
        name = case.create(source, spec=source.spec("s1"))
        case.rejected_refresh(name, source, source.spec("s1"), ["snapshot", "not found", "exist", "404"])
        source.publish("s1")
        case.refresh(name)
        case.index_load(name)
        case.verify(name, source.oracle())
        case.reload(name)
        case.verify(name, source.oracle())

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mode", ["direct-file", "data-split"])
    def test_paimon_text_embedding_independent_model_gt(self, paimon_case, mode, tei_endpoint, record_property):
        """
        target: PAI-L2-12
        method: direct same-model HTTP GT
        expected: generated vectors/search/raw match after reload.
        """
        case = paimon_case
        if not tei_endpoint:
            pytest.skip("Blocked: existing --tei_endpoint is required for the TextEmbedding profile")
        source = case.source()
        rows = source.oracle().rows
        try:
            info = requests.get(tei_endpoint.rstrip("/") + "/info", timeout=30)
            info.raise_for_status()
            vectors = []
            for start in range(0, len(rows), 16):
                response = requests.post(
                    tei_endpoint.rstrip("/") + "/embed",
                    json={"inputs": [row["doc"] for row in rows[start : start + 16]]},
                    timeout=30,
                )
                response.raise_for_status()
                batch = response.json()
                assert len(batch) == len(rows[start : start + 16])
                vectors.extend(batch)
        except requests.RequestException as error:
            pytest.skip(f"Blocked: configured TEI is unavailable to the runner: {type(error).__name__}")
        assert len(vectors) == len(rows) and vectors and len(vectors[0]) > 0
        dim = len(vectors[0])
        assert all(len(vector) == dim and np.isfinite(vector).all() for vector in vectors)
        record_property("paimon_tei_model", json.dumps(info.json(), sort_keys=True))
        oracle = PaimonOracle([{**row, "dense": vector} for row, vector in zip(rows, vectors, strict=True)])
        schema = case.client.create_schema(
            external_source=source.source, external_spec=source.spec(scan_mode=mode), enable_dynamic_field=False
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("doc", DataType.VARCHAR, max_length=1024, external_field="doc")
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_function(
            Function(
                name="doc_embedding",
                function_type=FunctionType.TEXTEMBEDDING,
                input_field_names=["doc"],
                output_field_names=["dense"],
                params={"provider": "TEI", "endpoint": tei_endpoint},
            )
        )
        name = case.create(source, schema=schema)
        case.refresh(name)
        case.index_load(name, field="dense")
        for reload in [False, True]:
            if reload:
                case.reload(name)
            case.verify(name, oracle, fields=("id", "doc", "dense"), field="dense")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mode", ["direct-file", "data-split"])
    def test_paimon_minhash_normal_control(self, paimon_case, mode):
        """
        target: PAI-L2-12 MinHash
        method: identical function parameters and corpus in a normal collection
        expected: complete raw function output and searchable signatures survive reload
        """
        case = paimon_case
        source = case.source("append")
        names = []
        for external in [True, False]:
            schema = case.client.create_schema(
                enable_dynamic_field=False,
                **(
                    {"external_source": source.source, "external_spec": source.spec(scan_mode=mode)} if external else {}
                ),
            )
            schema.add_field("id", DataType.INT64, **({"external_field": "id"} if external else {"is_primary": True}))
            schema.add_field(
                "doc", DataType.VARCHAR, max_length=1024, **({"external_field": "doc"} if external else {})
            )
            schema.add_field("mh", DataType.BINARY_VECTOR, dim=512)
            schema.add_function(
                Function(
                    name="doc_minhash",
                    function_type=FunctionType.MINHASH,
                    input_field_names=["doc"],
                    output_field_names=["mh"],
                    params={"num_hashes": 16, "shingle_size": 3},
                )
            )
            name = case.create(source, schema=schema)
            names.append(name)
            if external:
                case.refresh(name)
            else:
                case.client.insert(
                    collection_name=name,
                    data=[{"id": row["id"], "doc": row["doc"]} for row in source.oracle().rows],
                    timeout=60,
                )
                case.client.flush(collection_name=name, timeout=180)
            case.index_load(
                name,
                field="mh",
                index_type="MINHASH_LSH",
                metric="MHJACCARD",
                params={"mh_lsh_band": 8, "with_raw_data": True, "mh_element_bit_width": 32},
            )
            wait_actual_index(case.client, name, "mh", len(source.oracle().rows))
        normal_rows = case.rows(names[1], ("id", "doc", "mh"))
        for row in normal_rows:
            assert isinstance(row["mh"], list) and len(row["mh"]) == 1 and isinstance(row["mh"][0], bytes)
            row["mh"] = row["mh"][0]
        oracle = PaimonOracle(normal_rows)
        source.oracle().assert_rows(normal_rows, ("id", "doc"))
        query_signature = np.frombuffer(next(row["mh"] for row in normal_rows if row["id"] == 1), dtype="<u4")
        assert len(query_signature) == 16
        expected_scores = {
            row["id"]: float(np.mean(np.frombuffer(row["mh"], dtype="<u4") == query_signature)) for row in normal_rows
        }
        for reload in [False, True]:
            if reload:
                case.reload(names[0])
            oracle.assert_rows(case.rows(names[0], ("id", "doc", "mh")), ("id", "doc", "mh"))
            for name in names:
                result = case.client.search(
                    collection_name=name,
                    data=[source.oracle().rows[1]["doc"]],
                    anns_field="mh",
                    limit=10,
                    output_fields=["id", "doc", "mh"],
                    search_params={"metric_type": "MHJACCARD", "params": {"mh_search_with_jaccard": True}},
                    timeout=60,
                )
                assert len(result) == 1 and result[0]
                ids = [hit["entity"]["id"] for hit in result[0]]
                assert len(ids) == len(set(ids))
                scores = [hit["distance"] for hit in result[0]]
                assert scores == sorted(scores, reverse=True)
                assert scores[0] == pytest.approx(1.0)
                assert any(expected_scores[key] == 1.0 for key in ids)
                for hit in result[0]:
                    assert hit["distance"] == pytest.approx(expected_scores[hit["entity"]["id"]], abs=1e-6)
                oracle.filtered(lambda row: row["id"] in ids).assert_rows(
                    [hit["entity"] for hit in result[0]], ("id", "doc", "mh")
                )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("as_string", [False, True], ids=["number", "string"])
    def test_paimon_expired_snapshot_does_not_fall_back(self, paimon_case, as_string):
        """
        target: PAI-L2-01/14 expired snapshot
        method: remove the retained old snapshot metadata from this case's own source
        expected: explicit failure preserves S2 and never silently selects latest
        """
        case = paimon_case
        source = case.source("smoke", "s2")
        name = case.create(source, spec=source.spec("s2"))
        case.refresh(name)
        case.index_load(name)
        before = case.persisted(name)
        source.delete_owned(f"snapshot/snapshot-{source.snapshot('s1')}")
        value = str(source.snapshot("s1")) if as_string else source.snapshot("s1")
        case.rejected_refresh(name, source, source.spec(snapshot_id=value), ["snapshot", "exist", "not found"])
        assert case.persisted(name) == before
        case.verify(name, source.oracle("s2"))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("fmt", ["parquet", "lance-table"])
    def test_paimon_format_switch_and_failed_override(self, paimon_case, fmt):
        """
        target: PAI-L2-07
        method: pinned Paimon S1→plain S2→pinned Paimon S1→S2, then missing snapshot
        expected: distinct GT and durable source/spec before/after reload; failed override preserves both.
        """
        case = paimon_case
        source = case.source("append")
        name = case.create(source, spec=source.spec("s1"))
        case.refresh(name)
        case.index_load(name)
        source.publish("s2")
        assert source.oracle("s2").rows != source.oracle("s1").rows
        url, spec = case.plain_source(source, fmt)
        job = case.client.refresh_external_collection(
            collection_name=name, external_source=url, external_spec=spec, timeout=30
        )
        wait_refresh(case.client, job)
        case.assert_source_spec(name, url, spec)
        case.verify(name, source.oracle("s2"))
        case.reload(name)
        case.assert_source_spec(name, url, spec)
        case.verify(name, source.oracle("s2"))
        for version in ["s1", "s2"]:
            spec = source.spec(version, scan_mode="data-split")
            case.refresh(name, source, spec)
            case.assert_source_spec(name, source.source, spec)
            case.verify(name, source.oracle(version))
            case.reload(name)
            case.assert_source_spec(name, source.source, spec)
            case.verify(name, source.oracle(version))
        before = case.persisted(name)
        case.rejected_refresh(name, source, source.spec(snapshot_id=999999), ["snapshot"])
        assert case.persisted(name) == before
        case.verify(name, source.oracle("s2"))
        case.reload(name)
        assert case.persisted(name) == before
        case.verify(name, source.oracle("s2"))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("suffix", ["literal%25percent", "space key", "中文表"])
    def test_paimon_object_key(self, paimon_case, suffix):
        """
        target: PAI-L2-11
        method: own percent/space/Unicode prefix
        expected: no duplicate URI decoding.
        """
        case = paimon_case
        source = case.source(suffix=suffix)
        name = case.create(source)
        case.refresh(name)
        case.index_load(name)
        case.verify(name, source.oracle())

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("credential", ["access_key_id", "access_key_value"])
    def test_paimon_bad_credentials_preserve_view(self, paimon_case, credential):
        """
        target: PAI-L2-11
        method: override own credentials
        expected: exploration failure retains old data/spec.
        """
        case = paimon_case
        source = case.source()
        name = case.create(source)
        case.refresh(name)
        case.index_load(name)
        before = case.persisted(name)
        spec = json.loads(source.spec())
        spec["extfs"][credential] = "paimon-e2e-intentionally-invalid"
        case.rejected_refresh(
            name, source, json.dumps(spec), ["access", "credential", "signature", "403", "denied", "key"]
        )
        assert case.persisted(name) == before
        case.verify(name, source.oracle())

    @pytest.mark.tags(CaseLabel.L2)
    def test_paimon_concurrent_refresh_and_reads(self, paimon_case):
        """
        target: PAI-L2-13
        method: concurrent same-snapshot refresh/query
        expected: complete old/new views and terminal jobs.
        """
        case = paimon_case
        source = case.source("mor")
        name = case.create(source)
        case.refresh(name)
        case.index_load(name)
        source.publish("s2")
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [
                pool.submit(case.client.refresh_external_collection, collection_name=name, timeout=30) for _ in range(2)
            ]
            # One RPC has a committed view. A multi-page iterator is not promised
            # to pin an external-data view across a concurrent refresh.
            rows = case.client.query(
                collection_name=name, filter="id >= 0", output_fields=list(CORE), limit=10000, timeout=60
            )
            # Public API does not promise which side of the publication the concurrent read sees.
            matches = []
            for version in ["s1", "s2"]:
                try:
                    source.oracle(version).assert_rows(rows, CORE)
                    matches.append(version)
                except AssertionError:
                    pass
            assert matches, "concurrent read returned neither complete committed snapshot"
            query = source.oracle("s1").rows[0]["embedding"]
            results = case.client.search(
                collection_name=name,
                data=[query],
                anns_field="embedding",
                limit=10,
                output_fields=list(CORE),
                search_params={"metric_type": "L2"},
                timeout=60,
            )
            search_matches = []
            for version in ["s1", "s2"]:
                try:
                    source.oracle(version).assert_search(results, [query], "L2", 10, output_fields=CORE)
                    search_matches.append(version)
                except AssertionError:
                    pass
            assert search_matches, "concurrent search returned neither committed snapshot"
            for future in futures:
                try:
                    job = future.result()
                except MilvusException as error:
                    assert any(word in str(error).lower() for word in ["refresh", "progress", "running"]), str(error)
                else:
                    wait_refresh(case.client, job)
        case.refresh(name)
        case.verify(name, source.oracle("s2"))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("operation", ["insert", "upsert", "delete", "truncate_collection"])
    def test_paimon_reject_dml(self, paimon_case, operation):
        """
        target: PAI-L2-15
        method: DML on loaded external table
        expected: rejection and unchanged source GT.
        """
        case = paimon_case
        source = case.source()
        name = case.create(source)
        case.refresh(name)
        case.index_load(name)
        args = (
            {"filter": "id >= 0"}
            if operation == "delete"
            else {"data": [{f: row[f] for f in CORE} for row in source.oracle().rows[:1]]}
        )
        if operation == "truncate_collection":
            args = {}
        elif operation == "upsert":
            primary = case.client.query(
                collection_name=name, filter="id == 0", output_fields=["__virtual_pk__"], limit=1, timeout=30
            )
            assert len(primary) == 1
            args["data"][0]["__virtual_pk__"] = primary[0]["__virtual_pk__"]
        case.expect_rejected(
            lambda: getattr(case.client, operation)(collection_name=name, timeout=30, **args),
            ["external", "not support", "unsupported"],
        )
        case.verify(name, source.oracle())

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "variant,terms",
        [
            ("primary", ["primary key"]),
            ("dynamic", ["dynamic"]),
            ("partition", ["partition key"]),
            ("clustering", ["clustering"]),
        ],
    )
    def test_paimon_reject_unsupported_collection_definition(self, paimon_case, variant, terms):
        """
        target: PAI-L2-15 collection restrictions
        method: supply an explicitly forbidden external collection definition
        expected: create rejects the specific constraint before serving any data
        """
        case = paimon_case
        source = case.source()
        schema = case.client.create_schema(
            external_source=source.source, external_spec=source.spec(), enable_dynamic_field=variant == "dynamic"
        )
        schema.add_field("id", DataType.INT64, external_field="id", is_primary=variant == "primary")
        schema.add_field(
            "group_id",
            DataType.INT64,
            external_field="group_id",
            is_partition_key=variant == "partition",
            is_clustering_key=variant == "clustering",
        )
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=source.dimension, external_field="embedding")
        reason = case.expect_rejected(lambda: case.create(source, schema=schema), terms)
        assert "external" in reason.lower(), "rejection must come from the external collection restriction"

    @pytest.mark.tags(CaseLabel.L2)
    def test_paimon_job_progress_list_and_redaction(self, paimon_case):
        """
        target: PAI-L2-16
        method: progress/list/describe
        expected: terminal job traceability and masked credentials.
        """
        case = paimon_case
        source = case.source()
        name = case.create(source)
        progress = case.refresh(name)
        jobs = case.client.list_refresh_external_collection_jobs(collection_name=name, timeout=30)
        assert any(progress_field(job, "job_id") == progress_field(progress, "job_id") for job in jobs)
        assert progress_field(progress, "state") == "RefreshCompleted"
        assert progress_field(progress, "collection_name") == name
        _, spec = case.persisted(name)
        for key in ["access_key_id", "access_key_value"]:
            assert spec["extfs"][key] == "***"


@pytest.fixture(scope="class")
def paimon_readonly(request, paimon_bundle, minio_env, tmp_path_factory):
    """Class owns its immutable prefix; keep it until all read-only checks finish."""
    from common.external_table_common import cleanup_minio_prefix

    store, cfg = minio_env
    client = request.cls()._client()
    prefix = "paimon-readonly/" + tmp_path_factory.mktemp("paimon").name + "-" + cf.gen_unique_str("data")
    case = PaimonCase(client, paimon_bundle, store, cfg, prefix, request.cls.__name__[:60])
    request.addfinalizer(lambda: cleanup_minio_prefix(store, cfg["bucket"], prefix + "/"))
    request.addfinalizer(case.close)
    source = case.source("append")
    collections = {}
    for mode in ["direct-file", "data-split"]:
        name = case.create(source, spec=source.spec(scan_mode=mode))
        case.refresh(name)
        case.index_load(name)
        collections[mode] = name
    return case, source, collections


@pytest.mark.xdist_group("PaimonSealedOnly")
class TestMilvusClientPaimonSealedOnly(_ClientBase):
    """When to add tests here: immutable query/projection checks. Fixture: paimon_readonly."""

    def teardown_method(self, method):
        """The class fixture owns cleanup; Base teardown would disconnect its shared client."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mode", ["direct-file", "data-split"])
    def test_paimon_explicit_modes(self, paimon_readonly, mode):
        """
        target: PAI-L1-01A
        method: explicit modes and reload
        expected: complete source multiset/search GT.
        """
        case, source, names = paimon_readonly
        case.verify(names[mode], source.oracle())
        case.reload(names[mode])
        case.verify(names[mode], source.oracle())

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mode", ["direct-file", "data-split"])
    def test_paimon_query_get_projection_iterator(self, paimon_readonly, mode):
        """
        target: PAI-L1-07
        method: query/filter/get via current virtual PK
        expected: first/middle/tail fields aligned.
        """
        case, source, names = paimon_readonly
        name = names[mode]
        for predicate, expression in [
            (lambda row: row["id"] in [0, 2048, 4095], "id in [0, 2048, 4095]"),
            (lambda row: 19 <= row["id"] < 500 and row["group_id"] == 2, "id >= 19 and id < 500 and group_id == 2"),
            (lambda row: row["label"] is None, "label is null"),
        ]:
            oracle = source.oracle().filtered(predicate)
            case.verify(name, oracle, expression=expression)
        rows = case.client.query(
            collection_name=name,
            filter="id in [0, 2048, 4095]",
            output_fields=[*CORE, "__virtual_pk__"],
            limit=10,
            timeout=60,
        )
        got = case.client.get(
            collection_name=name, ids=[row["__virtual_pk__"] for row in rows], output_fields=list(CORE), timeout=60
        )
        source.oracle().filtered(lambda row: row["id"] in [0, 2048, 4095]).assert_rows(got, CORE)
