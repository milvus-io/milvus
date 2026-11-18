"""Paimon-specific source publication, lifecycle fixtures and independent oracle checks."""

import io
import json
import re
import tempfile
from pathlib import Path
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from check.func_check import Error
from common import common_func as cf
from common.common_type import CheckTasks
from common.external_table_common import build_external_source, build_external_spec
from common.paimon_fixture import (
    PaimonSource,
    progress_field,
    wait_refresh,
)
from common.paimon_runtime import generate_fixture_bundle
from pymilvus import DataType, MilvusException

CORE = ("id", "label", "value", "embedding", "group_id")
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


def milvus_error_reason(error):
    """Reject local adapter/assertion failures before matching an expected API error."""
    assert isinstance(error, Error), error
    assert issubclass(error.exception_type, MilvusException), (
        f"expected MilvusException, got {error.exception_type.__name__}: {error}"
    )
    # merr.ErrCollectionNotFound is unrelated to source/property validation.
    assert error.code != 100, f"unexpected collection-not-found error: {error}"
    return str(error.message)


@pytest.fixture(scope="session")
def paimon_bundle(tmp_path_factory, testrun_uid, request):
    base = tmp_path_factory.getbasetemp()
    if hasattr(request.config, "workerinput"):
        base = base.parent
    return generate_fixture_bundle(base / f"paimon-{testrun_uid}")


class PaimonCase:
    """One case owns every collection and object prefix it creates."""

    def __init__(self, api, client, bundle, store, cfg, prefix, name_prefix="PaimonIndependent"):
        self.api = api
        self.client, self.bundle, self.store, self.cfg = client, bundle, store, cfg
        self.prefix = prefix
        self.name_prefix = name_prefix
        self.sources = []
        self._source_identifiers = {prefix}

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
        schema = self.api.create_schema(
            self.client,
            external_source=source.source,
            external_spec=spec if spec is not None else source.spec(),
            enable_dynamic_field=False,
        )[0]
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

    def create(self, source, fields=CORE, spec=None, schema=None, **checks):
        name = f"{self.name_prefix}_{uuid4().hex[:10]}"
        self.api.tear_down_collection_names.append(name)  # Also clean ambiguous create responses.
        result = self.api.create_collection(
            self.client,
            collection_name=name,
            schema=schema or self.schema(source, fields, spec),
            consistency_level="Strong",
            timeout=60,
            force_teardown=False,
            **checks,
        )[0]
        return result if isinstance(result, Error) else name

    def refresh(self, name, source=None, spec=None, expected="RefreshCompleted"):
        kwargs = {}
        if source is not None:
            kwargs.update(external_source=source.source, external_spec=spec if spec is not None else source.spec())
        elif spec is not None:
            raise ValueError("refresh overrides must pass the complete source/spec pair")
        job = self.api.refresh_external_collection(self.client, collection_name=name, timeout=30, **kwargs)[0]
        return wait_refresh(self.api, self.client, job, expected=expected)

    def index_load(self, name, field="embedding", index_type="FLAT", metric="L2", params=None):
        indexes = self.api.prepare_index_params(self.client)[0]
        indexes.add_index(
            field_name=field, index_name=field, index_type=index_type, metric_type=metric, params=params or {}
        )
        self.api.create_index(self.client, collection_name=name, index_params=indexes, timeout=180)[0]
        self.api.load_collection(self.client, collection_name=name, timeout=180)[0]

    def rows(self, name, fields=CORE, expression="id >= 0"):
        iterator = self.api.query_iterator(
            self.client,
            collection_name=name,
            filter=expression,
            output_fields=list(fields),
            batch_size=257,
            consistency_level="Strong",
            timeout=60,
        )[0]
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
        count = self.api.query(
            self.client, collection_name=name, filter=expression, output_fields=["count(*)"], timeout=60
        )[0]
        assert count[0]["count(*)"] == len(oracle.rows)
        assert oracle.rows, "empty-read behavior must be tested with explicit failure expectations"
        queries = queries or [oracle.rows[i][field] for i in (0, len(oracle.rows) // 2, -1)]
        wire_queries = [np.asarray(query, dtype=np.int8) for query in queries] if field == "int8_vec" else queries
        results = self.api.search(
            self.client,
            collection_name=name,
            data=wire_queries,
            anns_field=field,
            filter=expression,
            limit=min(limit, len(oracle.rows)),
            output_fields=list(fields),
            search_params={"metric_type": metric, "params": search_params or {}},
            timeout=60,
        )[0]
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
        self.api.release_collection(self.client, collection_name=name, timeout=60)[0]
        self.api.load_collection(self.client, collection_name=name, timeout=180)[0]

    def rejection_reason(self, error, terms):
        """Match failure semantics without credit from echoed request identifiers."""
        raw = milvus_error_reason(error) if isinstance(error, Error) else error
        assert isinstance(raw, str) and raw, f"missing rejection reason: {raw!r}"
        identifiers = set(self.api.tear_down_collection_names) | self._source_identifiers
        for source in self.sources:
            identifiers.update((source.source, source.prefix))
        reason = raw
        for identifier in sorted(identifiers, key=len, reverse=True):
            if identifier:
                reason = re.sub(re.escape(identifier), "<?>", reason, flags=re.IGNORECASE)
        assert any(term.lower() in reason.lower() for term in terms), (
            f"unexpected rejection reason: {reason}; original: {raw}"
        )
        return reason

    def expect_rejected(self, action, terms):
        # The standard checker rejects successful RPCs; preserve the SDK error origin too.
        error = action(check_task=CheckTasks.err_res, check_items={"err_msg": ""})
        return self.rejection_reason(error, terms)

    def rejected_refresh(self, name, source, spec, terms):
        job, success = self.api.refresh_external_collection(
            self.client,
            collection_name=name,
            external_source=source.source,
            external_spec=spec,
            timeout=30,
            check_task=CheckTasks.check_nothing,
        )
        if success:
            progress = wait_refresh(self.api, self.client, job, expected="RefreshFailed")
            reason = progress_field(progress, "reason", "")
        else:
            reason = job
        self.rejection_reason(reason, terms)

    def rejected_read_pipeline(self, name, source, terms):
        """Bad source/type/dim can fail at exploration, index build or first read."""
        result, success = self.api.refresh_external_collection(
            self.client,
            collection_name=name,
            timeout=30,
            check_task=CheckTasks.check_nothing,
        )
        if success:
            progress = wait_refresh(self.api, self.client, result, expected=("RefreshCompleted", "RefreshFailed"))
            if progress_field(progress, "state") == "RefreshFailed":
                self.rejection_reason(progress_field(progress, "reason"), terms)
                return
            indexes = self.api.prepare_index_params(self.client)[0]
            indexes.add_index(field_name="embedding", index_name="embedding", index_type="FLAT", metric_type="L2")
            for operation, options in [
                (self.api.create_index, {"index_params": indexes, "timeout": 180}),
                (self.api.load_collection, {"timeout": 180}),
                (self.api.query_iterator, {"filter": "id >= 0", "output_fields": list(CORE), "timeout": 60}),
            ]:
                result, success = operation(
                    self.client,
                    collection_name=name,
                    check_task=CheckTasks.check_nothing,
                    **options,
                )
                if not success:
                    break
                if operation == self.api.query_iterator:
                    # Iterator.next has no client_v2 wrapper; errors may be deferred to the first page.
                    try:
                        while result.next():
                            pass
                    except MilvusException as error:
                        result_error = Error(error)
                        success = False
                    finally:
                        result.close()
                    if not success:
                        result = result_error
        assert not success, "invalid source/mapping was successfully refreshed, loaded and read"
        self.rejection_reason(result, terms)

    def persisted(self, name):
        description = self.api.describe_collection(self.client, collection_name=name, timeout=30)[0]
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
        url = build_external_source(self.cfg, prefix)
        self._source_identifiers.update((url, prefix))
        return url, build_external_spec(self.cfg, fmt=fmt)

    def close(self):
        errors = []
        for name in reversed(list(dict.fromkeys(self.api.tear_down_collection_names))):
            try:
                if self.api.has_collection(self.client, collection_name=name, timeout=30)[0]:
                    self.api.drop_collection(self.client, collection_name=name, timeout=60)[0]
                else:
                    self.api.tear_down_collection_names.remove(name)
            except Exception as error:
                errors.append(f"{name}: {error}")
        try:
            self.api.close(self.client)[0]
        except Exception as error:
            errors.append(f"close client: {error}")
        assert not errors, f"Paimon collection cleanup failed: {errors}"


@pytest.fixture
def paimon_case(request, paimon_bundle, minio_env, external_prefix):
    store, cfg = minio_env
    case = PaimonCase(
        request.instance,
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


@pytest.fixture(scope="class")
def paimon_readonly(request, paimon_bundle, minio_env, tmp_path_factory):
    """Class owns its immutable prefix; keep it until all read-only checks finish."""
    from common.external_table_common import cleanup_minio_prefix

    store, cfg = minio_env
    api = request.cls()
    api._setup_objects()
    client = api._client()
    prefix = "paimon-readonly/" + tmp_path_factory.mktemp("paimon").name + "-" + cf.gen_unique_str("data")
    case = PaimonCase(api, client, paimon_bundle, store, cfg, prefix, request.cls.__name__[:60])
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
