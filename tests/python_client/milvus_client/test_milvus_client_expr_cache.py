"""Sequential functional E2E coverage for the expression *result* cache.

Run this file separately against OFF, MEMORY, MEMORY-GROWING and DISK servers:
    python -m pytest milvus_client/test_milvus_client_expr_cache.py --uri http://HOST:19530

Parallel runs support -n 6 --dist=loadgroup. Only the immutable sealed tests
share an xdist group to reuse their collections. Independent and growing-data
cases are ungrouped, so each parameterized invocation can run on an available
worker with fresh collections. Ordered steps within each case stay sequential.

Enabled profiles require minEvalDurationUs=0, admissionThreshold=1 and normal
cache budgets. Boundary cases assume exprEvalBatchSize=8192, one replica and the
default minimum index row count (1024). Enable JSON shredding externally for
the stats-enabled run. EXPR_CACHE_TEST_PROFILE optionally labels JUnit results;
it is an operator-supplied label, never a configuration or capability probe.

No metrics, configuration updates, performance assertions or concurrent work.
Compaction checks warmed reads before reload and observes SDK segment topology;
the SDK cannot attribute individual reads to physical segments. L0/L1 selection
excludes the L2 lifecycle, iterator and larger-boundary cases.
Repeated calls exercise reuse opportunities but do not prove cache hits. The
JSON stats toggle verifies request semantics, not execution-path selection.
Source: https://milvus-expr-cache-e2e-guide.lu-zhang943306.chatgpt.site/
"""

import copy
import json
import operator
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import reduce

import numpy as np
import pymilvus
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from common.compaction_utils import reload_compaction_outputs
from milvus_client.expressions.expression_test_utils import (
    get_query_segment_states,
    wait_for_materialized_index,
    wait_for_segment_mode,
)
from pymilvus import AnnSearchRequest, DataType, MilvusException, RRFRanker

ROWS = 2049
BATCH = 8192
REPEATS = 6  # Initial evaluation plus five repetitions; not a hit assertion.
TIMEOUT = 120
MISSING = object()


@dataclass(frozen=True)
class FilterCase:
    name: str
    expr: str
    predicate: Callable
    params: dict = field(default_factory=dict)

    def expected(self, rows):
        # None represents UNKNOWN, not Python's False. Only TRUE selects a row.
        return [row for row in rows if self.predicate(row) is True]


@dataclass
class Dataset:
    name: str
    rows: list


class ExpectedResult:
    """Reuse independent calculations only within a block with no writes.

    Never attach this to a Dataset or a shared fixture: membership and vector
    distances must be rebuilt after every write, including vector-only upserts.
    """

    def __init__(self, rows):
        self.rows = rows
        self.by_id = {r["id"]: r for r in rows}
        assert len(self.by_id) == len(rows), "oracle requires unique primary keys"
        self._vectors = None
        self._distances = {}

    def l2(self, query_vector):
        q = np.asarray(query_vector, dtype=np.float32)
        key = q.tobytes()
        if key not in self._distances:
            if self._vectors is None:
                self._vectors = np.asarray([r["vec"] for r in self.rows], dtype=np.float32).reshape(-1, 8)
            distances = np.sum((self._vectors - q) ** 2, axis=1)
            self._distances[key] = (dict(zip(self.by_id, distances)), np.sort(distances))
        return self._distances[key]


def tri_not(value):
    return None if value is None else not value


def tri_and(left, right):
    if left is False or right is False:
        return False
    return None if left is None or right is None else True


def tri_or(left, right):
    if left is True or right is True:
        return True
    return None if left is None or right is None else False


def nullable_compare(value, op, other):
    return None if value is None or other is None else bool(op(value, other))


def vector_for_id(row_id):
    # Exactly representable float32 inputs; the first coordinate ensures unique
    # vectors, the others avoid all-zero/collinear fixtures.
    return [row_id / 64.0] + [((row_id * (i + 3) + i) % 31) / 32.0 for i in range(7)]


JSON_VALUES = [
    {"a": 7, "tags": ["red", "blue"], "arr": [7, 2], "user": {"age": 21}, "label": "red", "enabled": True},
    {"a": 7.0, "tags": ["red", "red"], "arr": [2, 7], "user": {"age": 18}, "label": "blue", "enabled": False},
    {"a": "7", "tags": [], "arr": [], "user": {}},
    {"a": True, "tags": [7, "red", True], "arr": [7], "user": {"age": "21"}},
    {"a": None, "tags": None, "arr": "wrong", "user": None},
    {},
    {"a": -2, "tags": [[1, 2], {"k": 1}], "arr": [8], "user": "wrong"},
    None,
    {"a": 0, "tags": [False, 0], "arr": [0], "user": {"age": 30}},
    {"a": 9, "tags": ["blue"], "arr": [9, 7], "user": {"age": None}},
    {"a": [], "tags": 5},
    {"a": {}, "tags": {}},
]
STRINGS = [None, "", "abc", "abcd", "xabc", "xabcy", "中文", 'a"b', "a\\b", "zzz", "a" * 240, "m"]
ARRAYS = [None, [], [1], [1, 2], [2, 1], [1, 1], [7], [1, 7], [7, 1], [2, 7, 7], [10, 11], [-1, 0]]
STRING_ARRAYS = [None, [], ["red"], ["red", "blue"], ["blue", "red"], ["red", "red"], ["中文"]]
TEXTS = [
    "vector database ready",
    "vector fast database ready",
    "vector fast fast database ready",
    "vector ready",
    "database ready",
    "milvus ready",
    "milvuz ready",
    "milvax ready",
    "other ready",
    "ready",
    None,
]


def make_rows(count=ROWS, start=0, profile="raw"):
    rows = []
    for i in range(start, start + count):
        row = {
            "id": i,
            "vec": vector_for_id(i),
            "n": None if i % 9 == 0 else i % 21 - 10,
            "m": None if i % 9 == 1 else 10 - i % 21,
            "group": i % 7,
        }
        if profile == "minimal":
            rows.append(row)
            continue
        if profile == "text":
            row.update(text=TEXTS[i % len(TEXTS)], text2=TEXTS[(i + 3) % len(TEXTS)])
        elif profile == "timestamp":
            dates = [
                None,
                "2025-12-31T00:00:00Z",
                "2026-01-01T00:00:00Z",
                "2026-01-31T00:00:00Z",
                "2026-02-28T00:00:00Z",
                "2026-12-31T00:00:00Z",
            ]
            row["ts"] = dates[i % len(dates)]
        else:
            row.update(
                b=[None, False, True][i % 3],
                s=STRINGS[i % len(STRINGS)],
                ai=copy.deepcopy(ARRAYS[i % len(ARRAYS)]),
                astr=copy.deepcopy(STRING_ARRAYS[i % len(STRING_ARRAYS)]),
                j=copy.deepcopy(JSON_VALUES[i % len(JSON_VALUES)]),
                j2=copy.deepcopy(JSON_VALUES[(i + 3) % len(JSON_VALUES)]),
            )
            for bits in (8, 16, 32, 64):
                row[f"i{bits}"] = [-(2 ** (bits - 1)), -1, 0, 1, 2 ** (bits - 1) - 1, None][i % 6]
            row["f"] = [None, -0.5, 0.0, 0.25, float(np.nextafter(np.float32(0.25), np.float32(1))), 1.25][i % 6]
            row["d"] = [None, -0.5, 0.0, 0.25, float(np.nextafter(0.25, 1.0)), 1.25][i % 6]
            if profile == "mixed":
                row.update(text=TEXTS[i % len(TEXTS)], text2=TEXTS[(i + 3) % len(TEXTS)])
            if profile == "indexed":
                row.update(s_inv=row["s"], s_ngram=row["s"])
            if profile == "dynamic" and i % 3:
                row.update(dyn_score=i % 11, dyn_meta={"score": i % 5})
        rows.append(row)
    return rows


def json_path(row, *path, field_name="j"):
    value = row[field_name]
    for part in path:
        if isinstance(part, int):
            if not isinstance(value, list) or part >= len(value):
                return MISSING
        elif not isinstance(value, dict) or part not in value:
            return MISSING
        value = value[part]
    return value


def json_number(row, *path, field_name="j"):
    value = json_path(row, *path, field_name=field_name)
    # bool is an int subclass in Python, but is a distinct JSON type in Milvus.
    return value if type(value) in (int, float) else None


def json_exists(value):
    """EXISTS requires a non-NULL scalar at or below the selected JSON path."""
    if value is MISSING or value is None:
        return False
    if isinstance(value, dict):
        return any(json_exists(child) for child in value.values())
    if isinstance(value, list):
        return any(json_exists(child) for child in value)
    # False, zero and the empty string are present scalar values. Containers
    # containing only NULLs/empty containers are absent under Json::exist().
    return True


def case_params(cases):
    return [pytest.param(case, id=case.name) for case in cases]


def scalar_cases():
    cases = []
    for symbol, op in [
        ("==", operator.eq),
        ("!=", operator.ne),
        (">", operator.gt),
        (">=", operator.ge),
        ("<", operator.lt),
        ("<=", operator.le),
    ]:
        cases.append(FilterCase(f"n_{op.__name__}", f"n {symbol} 0", lambda r, op=op: nullable_compare(r["n"], op, 0)))
    for low, high in [("<", "<"), ("<", "<="), ("<=", "<"), ("<=", "<=")]:
        cases.append(
            FilterCase(
                f"range_{low}_{high}",
                f"-2 {low} n {high} 2",
                lambda r, low=low, high=high: (
                    None
                    if r["n"] is None
                    else (-2 <= r["n"] if low == "<=" else -2 < r["n"])
                    and (r["n"] <= 2 if high == "<=" else r["n"] < 2)
                ),
            )
        )
    cases += [
        FilterCase("equal_bounds", "0 <= n <= 0", lambda r: nullable_compare(r["n"], operator.eq, 0)),
        FilterCase("empty_range", "0 < n < 1", lambda r: False),
        FilterCase("nonnullable", "group >= 2", lambda r: r["group"] >= 2),
    ]
    for values in [[], [1], [1, 1, 7], [7, 1], list(range(1000)), list(range(100_000, 109_999)) + [-7]]:
        for negate in (False, True):
            cases.append(
                FilterCase(
                    f"{'not_in' if negate else 'in'}_{len(values)}",
                    f"n {'not in' if negate else 'in'} {values}",
                    lambda r, values=values, negate=negate: (
                        None if r["n"] is None else (r["n"] not in values if negate else r["n"] in values)
                    ),
                )
            )
    for name, expr, func in [
        ("add", "n + 2 > 3", lambda n: n + 2 > 3),
        ("subtract", "n - 2 <= 3", lambda n: n - 2 <= 3),
        ("multiply", "n * 2 == 4", lambda n: n * 2 == 4),
        ("divide", "n / 2 > 1", lambda n: int(n / 2) > 1),
        ("modulo", "n % 3 == 0", lambda n: n % 3 == 0),
    ]:
        cases.append(FilterCase(name, expr, lambda r, func=func: None if r["n"] is None else func(r["n"])))
    return cases


def string_cases(field_name="s"):
    cases = []
    for name, expr, func in [
        ("empty", '== ""', lambda s: s == ""),
        ("unicode", '== "中文"', lambda s: s == "中文"),
        ("quote", f"== {json.dumps('a' + chr(34) + 'b')}", lambda s: s == 'a"b'),
        ("backslash", f"== {json.dumps('a' + chr(92) + 'b')}", lambda s: s == "a\\b"),
        ("long", f"== {json.dumps('a' * 240)}", lambda s: s == "a" * 240),
        ("not_equal", '!= "abc"', lambda s: s != "abc"),
        ("ordering", '> "abc"', lambda s: s > "abc"),
        ("in", 'in ["", "abc", "中文", "abc"]', lambda s: s in {"", "abc", "中文"}),
        ("not_in", 'not in ["", "abc", "中文", "abc"]', lambda s: s not in {"", "abc", "中文"}),
        ("prefix", 'like "abc%"', lambda s: s.startswith("abc")),
        ("suffix", 'like "%abc"', lambda s: s.endswith("abc")),
        ("infix", 'like "%abc%"', lambda s: "abc" in s),
        ("all", 'like "%"', lambda s: True),
    ]:
        cases.append(
            FilterCase(
                f"{field_name}_{name}",
                f"{field_name} {expr}",
                lambda r, func=func: None if r[field_name] is None else func(r[field_name]),
            )
        )
    cases.append(
        FilterCase(
            f"{field_name}_range",
            f'"a" <= {field_name} < "m"',
            lambda r: None if r[field_name] is None else "a" <= r[field_name] < "m",
        )
    )
    return cases


def json_cases():
    return [
        FilterCase("number_eq", 'j["a"] == 7', lambda r: json_number(r, "a") == 7),
        FilterCase(
            "not_number_eq",
            'not (j["a"] == 7)',
            lambda r: (v := json_number(r, "a")) is not None and v != 7,
        ),
        FilterCase("number_range", '1 <= j["a"] < 9', lambda r: (v := json_number(r, "a")) is not None and 1 <= v < 9),
        FilterCase("string", 'j["a"] == "7"', lambda r: json_path(r, "a") == "7"),
        FilterCase("bool", 'j["a"] == true', lambda r: json_path(r, "a") is True),
        FilterCase("term", 'j["a"] in [0, 7]', lambda r: json_number(r, "a") in (0, 7)),
        FilterCase(
            "not_term", 'j["a"] not in [0, 7]', lambda r: (v := json_number(r, "a")) is not None and v not in (0, 7)
        ),
        FilterCase(
            "nested", 'j["user"]["age"] > 18', lambda r: (v := json_number(r, "user", "age")) is not None and v > 18
        ),
        FilterCase("offset", 'j["arr"][0] == 7', lambda r: json_number(r, "arr", 0) == 7),
        FilterCase("offset_oob", 'j["arr"][9] == 7', lambda r: False),
        FilterCase("exists", 'exists j["a"]', lambda r: json_exists(json_path(r, "a"))),
        FilterCase("not_exists", 'not exists j["a"]', lambda r: not json_exists(json_path(r, "a"))),
        FilterCase("typed_string_index", 'j["label"] == "red"', lambda r: json_path(r, "label") == "red"),
        FilterCase("typed_bool_index", 'j["enabled"] == true', lambda r: json_path(r, "enabled") is True),
        FilterCase("contains", 'json_contains(j["tags"], "red")', lambda r: r["id"] % 12 in (0, 1, 3)),
        FilterCase("contains_number", 'json_contains(j["tags"], 7)', lambda r: r["id"] % 12 == 3),
        FilterCase("contains_bool", 'json_contains(j["tags"], true)', lambda r: r["id"] % 12 == 3),
        FilterCase("contains_nested", 'json_contains(j["tags"], [1, 2])', lambda r: r["id"] % 12 == 6),
        FilterCase(
            "contains_any",
            'json_contains_any(j["tags"], ["blue", "red", "red"])',
            lambda r: r["id"] % 12 in (0, 1, 3, 9),
        ),
        FilterCase("contains_all", 'json_contains_all(j["tags"], ["red", "blue"])', lambda r: r["id"] % 12 == 0),
        FilterCase("empty_any", 'json_contains_any(j["tags"], [])', lambda r: False),
        # JSON's empty-argument fast path is constant TRUE, including field NULL.
        # Nullable ARRAY has a separate validity-preserving fast path.
        FilterCase("empty_all", 'json_contains_all(j["tags"], [])', lambda r: True),
        FilterCase("arithmetic", 'j["a"] + 2 >= 10', lambda r: (v := json_number(r, "a")) is not None and v + 2 >= 10),
        FilterCase(
            "length",
            'array_length(j["tags"]) == 2',
            lambda r: isinstance(v := json_path(r, "tags"), list) and len(v) == 2,
        ),
        FilterCase("missing", 'j["absent"] == 7', lambda r: False),
    ]


def array_cases():
    cases = []
    for name, expr, func in [
        ("null", "ai is null", lambda a: a is None),
        ("empty", "array_length(ai) == 0", lambda a: a is not None and len(a) == 0),
        ("length", "array_length(ai) >= 2", lambda a: a is not None and len(a) >= 2),
        ("contains", "array_contains(ai, 7)", lambda a: a is not None and 7 in a),
        ("any", "array_contains_any(ai, [7, 1, 1])", lambda a: a is not None and any(x in a for x in (7, 1))),
        ("all", "array_contains_all(ai, [1, 7])", lambda a: a is not None and all(x in a for x in (1, 7))),
        ("empty_any", "array_contains_any(ai, [])", lambda a: False),
        ("empty_all", "array_contains_all(ai, [])", lambda a: a is not None),
        ("equal", "ai == [1, 2]", lambda a: a == [1, 2]),
        ("offset", "ai[0] > 6", lambda a: a is not None and len(a) > 0 and a[0] > 6),
        ("offset_arithmetic", "ai[0] + 1 == 8", lambda a: a is not None and len(a) > 0 and a[0] + 1 == 8),
        ("offset_oob", "ai[9] == 7", lambda a: False),
    ]:
        cases.append(FilterCase(name, expr, lambda r, func=func: func(r["ai"])))
    for suffix, values, func in [
        ("", '"red"', lambda a: "red" in a),
        ("_any", '["red", "blue"]', lambda a: any(x in a for x in ("red", "blue"))),
        ("_all", '["blue", "red", "red"]', lambda a: all(x in a for x in ("red", "blue"))),
    ]:
        cases.append(
            FilterCase(
                f"strings{suffix}",
                f"array_contains{suffix}(astr, {values})",
                lambda r, func=func: r["astr"] is not None and func(r["astr"]),
            )
        )
    return cases


def assert_fields(actual, expected, fields):
    # Milvus may return the primary key implicitly, but must not leak fields
    # from a previous request with a different projection.
    extra = set(actual) - set(fields) - {"id"}
    assert not extra, f"unexpected output fields: {sorted(extra)}"
    if "id" in actual:
        assert actual["id"] == expected["id"], "incorrect implicit primary key"
    for name in fields:
        assert name in actual, f"missing output field {name}: {actual}"
        value = expected.get(name)
        if name == "vec" or isinstance(value, float):
            np.testing.assert_allclose(actual[name], value, rtol=1e-5, atol=1e-6, err_msg=f"field={name}")
        else:
            assert actual[name] == value, f"field={name}: actual={actual[name]!r}, expected={value!r}"


def ranked_ids(rows, query_vector, limit):
    """Local FLAT/L2 ranks; ID only breaks ties for fixture-sensitivity checks."""
    distances, _ = ExpectedResult(rows).l2(query_vector)
    ids = list(distances)
    order = np.lexsort((ids, list(distances.values())))[:limit]
    return [ids[i] for i in order]


def assert_exact_hits(hits, rows, query_vector, limit, output_fields=("n",)):
    expected = rows if isinstance(rows, ExpectedResult) else ExpectedResult(rows)
    by_id = expected.by_id
    ids = [h["id"] for h in hits]
    assert len(ids) == min(limit, len(by_id)), f"wrong hit count: {len(ids)} vs {min(limit, len(by_id))}"
    assert len(ids) == len(set(ids)), f"duplicate search IDs: {ids}"
    assert set(ids) <= set(by_id), f"search returned nonmatching IDs: {set(ids) - set(by_id)}"
    if not hits:
        return
    distances, ranked = expected.l2(query_vector)
    actual_distances = [h["distance"] for h in hits]
    np.testing.assert_allclose(actual_distances, ranked[:limit], rtol=1e-5, atol=1e-6)
    np.testing.assert_allclose(actual_distances, [distances[i] for i in ids], rtol=1e-5, atol=1e-6)
    for hit in hits:
        assert_fields(hit["entity"], by_id[hit["id"]], output_fields)


def wait_for_text_rows(client, data):
    """Wait for the independent sentinel in the same text reader as the target.

    Every non-NULL document contains "ready". Check IDs and document values,
    not just cardinality: same-count upserts and vector-only updates must not
    mistake the previous snapshot for readiness. Never poll a target predicate.
    Growing text publishes committed prefixes (TantivyGrowingTextIndex), so a
    visible sentinel on each updated document establishes reader publication.
    """
    fields = ("id", "text", "vec")
    expected = {r["id"]: {f: r[f] for f in fields} for r in data.rows if r["text"] is not None}
    assert all("ready" in r["text"].split() for r in expected.values()), "missing readiness sentinel"
    deadline = time.monotonic() + TIMEOUT
    while True:
        remaining = deadline - time.monotonic()
        assert remaining > 0, f"{data.name}: text reader did not publish current sentinel documents"
        result = client.query(
            data.name,
            filter='TEXT_MATCH(text, "ready")',
            output_fields=list(fields),
            consistency_level="Strong",
            timeout=remaining,
        )
        assert time.monotonic() < deadline, f"{data.name}: text readiness response exceeded deadline"
        actual = {r["id"]: r for r in result}
        assert len(actual) == len(result), "duplicate IDs in text readiness response"
        if actual == expected:
            return
        time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))


def assert_single_sealed_segment(client, data):
    """A row-count boundary must occur within a segment, not across fragments."""
    states = get_query_segment_states(client, data.name)
    assert len(states) == 1 and states[0][2] == len(data.rows), (
        f"{data.name}: boundary-test prerequisite unmet: expected one sealed "
        f"segment containing {len(data.rows)} rows, got {states}"
    )


class ExprCacheTestBase(TestMilvusClientV2Base):
    """Fixture-owned clients/collections; never run Base's global role cleanup."""

    def teardown_method(self, method):
        # Base teardown removes all SDK connections and unrelated roles. These
        # tests own their resources via finalizers, including shared clients.
        pass

    @pytest.fixture(scope="class")
    def ec_client(self, request):
        client = self._client(alias=request.cls.__name__)
        assert client is not None, "MilvusClient initialization failed"
        request.addfinalizer(client.close)
        return client

    @pytest.fixture(autouse=True)
    def profile_metadata(self, request, ec_client):
        request.node.user_properties.extend(
            [
                ("expr_cache_profile_supplied", os.getenv("EXPR_CACHE_TEST_PROFILE", "unspecified")),
                ("pymilvus_version", pymilvus.__version__),
                ("milvus_version", str(ec_client.get_server_version())),
                ("cache_hit_verification", "not available through SDK-only assertions"),
            ]
        )

    def create_dataset(
        self,
        client,
        request,
        rows=None,
        profile="raw",
        sealed=True,
        name=None,
        properties=None,
        index_targets=None,
        defer_load=False,
        extra_indexes=(),
    ):
        rows = make_rows(profile=profile) if rows is None else copy.deepcopy(rows)
        assert not defer_load or (sealed and not rows), "deferred loading requires an empty sealed fixture"
        name = name or cf.gen_unique_str(f"expr_cache_{profile}")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=profile == "dynamic")[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)
        schema.add_field("n", DataType.INT64, nullable=True)
        schema.add_field("m", DataType.INT64, nullable=True)
        schema.add_field("group", DataType.INT64)
        if profile in ("text", "mixed"):
            for f in ("text", "text2"):
                schema.add_field(
                    f,
                    DataType.VARCHAR,
                    max_length=512,
                    nullable=True,
                    enable_analyzer=True,
                    enable_match=True,
                    analyzer_params={"tokenizer": "standard"},
                )
        if profile == "timestamp":
            schema.add_field("ts", DataType.TIMESTAMPTZ, nullable=True)
        elif profile == "retyped":
            schema.add_field("changed", DataType.VARCHAR, max_length=64)
        elif profile not in ("minimal", "text"):
            for bits in (8, 16, 32, 64):
                schema.add_field(f"i{bits}", getattr(DataType, f"INT{bits}"), nullable=True)
            schema.add_field("f", DataType.FLOAT, nullable=True)
            schema.add_field("d", DataType.DOUBLE, nullable=True)
            schema.add_field("b", DataType.BOOL, nullable=True)
            for f in ("s", "s_inv", "s_ngram") if profile == "indexed" else ("s",):
                schema.add_field(f, DataType.VARCHAR, max_length=512, nullable=True)
            for f in ("j", "j2"):
                schema.add_field(f, DataType.JSON, nullable=True)
            schema.add_field("ai", DataType.ARRAY, element_type=DataType.INT64, max_capacity=16, nullable=True)
            schema.add_field(
                "astr", DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=16, max_length=64, nullable=True
            )
        kwargs = {} if properties is None else {"properties": properties}
        self.create_collection(
            client, name, schema=schema, shards_num=1, consistency_level="Strong", force_teardown=False, **kwargs
        )

        def cleanup():
            if client.has_collection(name, timeout=TIMEOUT):
                client.drop_collection(name, timeout=TIMEOUT)

        request.addfinalizer(cleanup)
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index("vec", index_type="FLAT", metric_type="L2")
        scalar_indexes = []
        built_targets = set()
        if profile == "indexed":
            for f, kind in [
                ("n", "INVERTED"),
                ("m", "STL_SORT"),
                ("b", "BITMAP"),
                ("s", "TRIE"),
                ("s_inv", "INVERTED"),
                ("s_ngram", "NGRAM"),
            ]:
                if index_targets is not None and f not in index_targets:
                    continue
                params = {"min_gram": 2, "max_gram": 3} if kind == "NGRAM" else {}
                indexes.add_index(f, index_type=kind, index_name=f"idx_{f}", params=params)
                scalar_indexes.append((f"idx_{f}", f, kind))
                built_targets.add(f)
        if profile in ("json_path", "json_flat", "mixed"):
            paths = (
                [("a", "DOUBLE"), ("label", "VARCHAR"), ("enabled", "BOOL")]
                if profile in ("json_path", "mixed")
                else [("", "JSON")]
            )
            for field_name in ("j", "j2") if profile == "json_flat" else ("j",):
                for key, cast in paths:
                    path = f'{field_name}["{key}"]' if key else field_name
                    if index_targets is not None and path not in index_targets:
                        continue
                    index_name = f"idx_json_{key or 'flat'}_{field_name}"
                    indexes.add_index(
                        field_name,
                        index_type="INVERTED",
                        index_name=index_name,
                        params={
                            "json_path": path,
                            "json_cast_type": cast,
                        },
                    )
                    scalar_indexes.append((index_name, field_name, "INVERTED"))
                    built_targets.add(path)
        for f, kind in extra_indexes:
            assert f not in built_targets, f"duplicate index target: {f}"
            indexes.add_index(f, index_type=kind, index_name=f"idx_{f}")
            scalar_indexes.append((f"idx_{f}", f, kind))
        if index_targets is not None:
            assert built_targets == set(index_targets), (
                f"unconfigured index targets: {set(index_targets) - built_targets}"
            )
        self.create_index(client, name, index_params=indexes)
        # Growing tests must load before insertion. Sealed tests prepare rows
        # and indexes first, avoiding an unnecessary empty load and handoff.
        # No target predicate is used for readiness.
        if not sealed:
            self.load_collection(client, name)
        if rows:
            self.insert(client, name, data=rows)
            if sealed:
                self.flush(client, name)
                for index_name, f, kind in scalar_indexes:
                    wait_for_materialized_index(self, client, name, index_name, len(rows), f, kind)
        if sealed and not defer_load:
            self.load_collection(client, name)
        if rows:
            wait_for_segment_mode(client, name, "sealed" if sealed else "growing", len(rows), timeout=TIMEOUT)
        return Dataset(name, rows)

    @pytest.fixture
    def make_dataset(self, request, ec_client):
        return lambda **kwargs: self.create_dataset(ec_client, request, **kwargs)

    def query_rows(self, client, data, case, output_fields=("id", "n"), limit=None, expected_count=None, **kwargs):
        params = dict(case.params)
        params.update(kwargs.pop("filter_params", {}))
        args = dict(
            filter=case.expr,
            filter_params=params,
            output_fields=list(output_fields),
            consistency_level="Strong",
            **kwargs,
        )
        if limit is not None:
            args["limit"] = limit
        if expected_count is None:
            expected_count = len(case.expected(data.rows))
        # The result cardinality, not collection size, determines whether the
        # complete response needs an iterator. Never cap a complete query.
        if expected_count > 16384 and limit is None:
            iterator = self.query_iterator(client, data.name, batch_size=BATCH, **args)[0]
            rows = []
            try:
                while batch := iterator.next():
                    rows.extend(batch)
                    assert len(rows) <= len(data.rows), f"{data.name}: iterator exceeded the complete fixture row count"
            finally:
                iterator.close()
            return rows
        return self.query(client, data.name, **args)[0]

    def verify_query(self, client, data, case, output_fields=("id", "n"), limit=None, oracle=None, **kwargs):
        oracle = ExpectedResult(case.expected(data.rows)) if oracle is None else oracle
        expected = oracle.by_id
        actual = self.query_rows(client, data, case, output_fields, limit, expected_count=len(expected), **kwargs)
        ids = [r["id"] for r in actual]
        context = f"{data.name}: {case.name}: {case.expr}, params={case.params}"
        assert len(ids) == len(set(ids)), f"{context}: duplicate query IDs"
        if limit is None:
            assert set(ids) == set(expected), (
                f"{context}: missing={set(expected) - set(ids)}, extra={set(ids) - set(expected)}"
            )
        else:
            assert len(ids) == min(limit, len(expected)), context
            assert set(ids) <= set(expected), context
        for row in actual:
            assert_fields(row, expected[row["id"]], output_fields)
        params = dict(case.params)
        params.update(kwargs.get("filter_params", {}))
        count = self.query(
            client,
            data.name,
            filter=case.expr,
            filter_params=params,
            output_fields=["count(*)"],
            consistency_level="Strong",
            **{k: v for k, v in kwargs.items() if k != "filter_params"},
        )[0]
        assert count == [{"count(*)": len(expected)}], f"{context}: count={count}"

    def verify_search(
        self,
        client,
        data,
        case,
        vectors=None,
        limit=10,
        output_fields=("n",),
        iterative=False,
        oracle=None,
        timeout=TIMEOUT,
        **kwargs,
    ):
        vectors = [vector_for_id(23)] if vectors is None else vectors
        params = dict(case.params)
        params.update(kwargs.pop("filter_params", {}))
        search_params = {"metric_type": "L2", "params": {}}
        if iterative:
            search_params["hints"] = "iterative_filter"
        # The shared search wrapper currently consumes timeout without forwarding
        # it. Call the SDK here to retain a bounded RPC without changing it.
        results = client.search(
            data.name,
            data=vectors,
            anns_field="vec",
            filter=case.expr,
            filter_params=params,
            search_params=search_params,
            limit=limit,
            output_fields=list(output_fields),
            consistency_level="Strong",
            timeout=timeout,
            **kwargs,
        )
        assert len(results) == len(vectors), f"{case.name}: wrong nq"
        oracle = ExpectedResult(case.expected(data.rows)) if oracle is None else oracle
        for hits, vector in zip(results, vectors):
            assert_exact_hits(hits, oracle, vector, limit, output_fields)
        return results

    def verify_compaction_transition(self, client, data, case, expected_ids, retired_ids):
        """Assert warmed reads while waiting for the SDK's new segment view.

        GetQuerySegmentInfo is target-filtered metadata, not a per-read segment
        trace. Keep this check before reload so stale results in the ordinary
        transition cannot be erased by our own ReleaseCollection call.
        """
        deadline = time.monotonic() + TIMEOUT
        observed = set()
        oracle = ExpectedResult(case.expected(data.rows))

        def remaining():
            budget = deadline - time.monotonic()
            assert budget > 0, (
                f"{data.name}: compaction topology not observed before deadline; "
                f"expected={expected_ids}, observed={observed}, retired={retired_ids}"
            )
            return budget

        while True:
            # Any incorrect response fails immediately, even before topology
            # converges; readiness polling must never consume assertion failures.
            self.verify_query(client, data, case, oracle=oracle, timeout=remaining())
            self.verify_search(client, data, case, oracle=oracle, timeout=remaining())
            segments = client._get_connection().get_query_segment_info(
                data.name, timeout=remaining(), context=client._generate_call_context()
            )
            observed = {segment.segmentID for segment in segments}
            budget = remaining()
            if observed == expected_ids and observed.isdisjoint(retired_ids) and all(s.nodeIds for s in segments):
                break
            time.sleep(min(0.5, budget))
        self.repeat_case(client, data, case)

    def repeat_case(self, client, data, case, search=True, **kwargs):
        oracle = ExpectedResult(case.expected(data.rows))
        for _ in range(REPEATS):
            self.verify_query(client, data, case, oracle=oracle, **kwargs)
            if search:
                self.verify_search(client, data, case, oracle=oracle, **kwargs)

    def alternate(self, client, data, left, right, **kwargs):
        left_oracle = ExpectedResult(left.expected(data.rows))
        right_oracle = ExpectedResult(right.expected(data.rows))
        assert left_oracle.by_id.keys() != right_oracle.by_id.keys(), "isolation fixtures need distinct results"
        for case, oracle in [(left, left_oracle), (right, right_oracle)] * 5:
            self.verify_query(client, data, case, oracle=oracle, **kwargs)
            self.verify_search(client, data, case, oracle=oracle, **kwargs)


@pytest.mark.xdist_group("ExprCacheSealedOnly")
class TestExprCacheSealedOnly(ExprCacheTestBase):
    """When to add tests here: immutable expression/type coverage on sealed data.

    Fixtures: raw/indexed/json_data are shared only within this class.
    """

    @pytest.fixture(scope="class")
    def raw(self, request, ec_client):
        return self.create_dataset(ec_client, request)

    @pytest.fixture(scope="class")
    def indexed(self, request, ec_client):
        return self.create_dataset(ec_client, request, profile="indexed")

    @pytest.fixture(scope="class", params=["raw", "json_path", "json_flat"])
    def json_data(self, request, ec_client):
        return self.create_dataset(ec_client, request, profile=request.param)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("fixture_name", ["raw", "indexed"])
    def test_repeated_query_count(self, request, ec_client, fixture_name):
        """target: repeated scalar results; method: range reads; expected: exact IDs/fields/count."""
        data = request.getfixturevalue(fixture_name)
        case = FilterCase("range", "n >= 0", lambda r: nullable_compare(r["n"], operator.ge, 0))
        self.repeat_case(ec_client, data, case)

    @pytest.mark.tags(CaseLabel.L0)
    def test_filter_reuse_with_different_vectors(self, ec_client, raw):
        """target: final results are not cached; method: change vector; expected: fresh exact top-K."""
        case = FilterCase("range", "n > 0", lambda r: nullable_compare(r["n"], operator.gt, 0))
        oracle = ExpectedResult(case.expected(raw.rows))
        for _ in range(REPEATS):
            for row_id in (23, 1800, 23):
                self.verify_search(ec_client, raw, case, vectors=[vector_for_id(row_id)], oracle=oracle)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("case", case_params(scalar_cases()))
    def test_scalar_operators(self, ec_client, raw, case):
        """target: scalar predicates; method: repeat each operator; expected: independent oracle."""
        self.repeat_case(ec_client, raw, case)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field_name", ["i8", "i16", "i32", "i64", "f", "d", "b"])
    def test_scalar_types_and_boundaries(self, ec_client, raw, field_name):
        """target: typed literal identity; method: legal extrema; expected: typed results."""
        values = list(dict.fromkeys(r[field_name] for r in raw.rows if r[field_name] is not None))
        for value in values:
            literal = json.dumps(value)
            for symbol, op in [("==", operator.eq), ("!=", operator.ne)]:
                case = FilterCase(
                    f"{field_name}_{symbol}_{literal}",
                    f"{field_name} {symbol} {literal}",
                    lambda r, value=value, op=op: nullable_compare(r[field_name], op, value),
                )
                self.repeat_case(ec_client, raw, case)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "case",
        case_params(
            [
                FilterCase("inverted_range", "n > 2", lambda r: nullable_compare(r["n"], operator.gt, 2)),
                FilterCase("sorted_range", "-2 <= m < 4", lambda r: None if r["m"] is None else -2 <= r["m"] < 4),
                FilterCase("bitmap_bool", "b == true", lambda r: r["b"] is True),
                FilterCase(
                    "mixed_index_raw",
                    'n > 2 and j["a"] == 7',
                    lambda r: r["n"] is not None and r["n"] > 2 and json_number(r, "a") == 7,
                ),
            ]
        ),
    )
    def test_scalar_index_families(self, ec_client, indexed, case):
        """target: index-backed reuse; method: indexed/mixed predicates; expected: exact results."""
        self.repeat_case(ec_client, indexed, case, filter_params={"expr_use_json_stats": False})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field_name", ["f", "d"])
    def test_float_ranges_and_terms(self, ec_client, raw, field_name):
        """target: fractional range/IN boundaries; method: adjacent floats; expected: exact membership."""
        adjacent = next(r[field_name] for r in raw.rows if r[field_name] is not None and 0.25 < r[field_name] < 1)
        for name, expr, predicate in [
            ("exclusive", f"0.25 < {field_name} < 1.0", lambda v: 0.25 < v < 1.0),
            ("inclusive", f"0.25 <= {field_name} <= {adjacent}", lambda v: 0.25 <= v <= adjacent),
            ("in", f"{field_name} in [0.25, {adjacent}]", lambda v: v in (0.25, adjacent)),
            ("not_in", f"{field_name} not in [0.25, {adjacent}]", lambda v: v not in (0.25, adjacent)),
        ]:
            case = FilterCase(
                name, expr, lambda r, pred=predicate: None if r[field_name] is None else pred(r[field_name])
            )
            self.repeat_case(ec_client, raw, case)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("case", case_params(string_cases()))
    def test_raw_strings(self, ec_client, raw, case):
        """target: raw string semantics; method: repeat LIKE/comparison; expected: literal oracle."""
        self.repeat_case(ec_client, raw, case)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "case",
        case_params(
            string_cases("s")
            + string_cases("s_inv")
            + [
                FilterCase(
                    "ngram_infix", 's_ngram like "%abc%"', lambda r: r["s_ngram"] is not None and "abc" in r["s_ngram"]
                ),
            ]
        ),
    )
    def test_indexed_strings(self, ec_client, indexed, case):
        """target: TRIE/INVERTED/NGRAM compatibility; method: repeat; expected: exact membership."""
        self.repeat_case(ec_client, indexed, case)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("case", case_params(json_cases()))
    def test_json_semantics(self, ec_client, json_data, case):
        """target: JSON types/paths; method: raw/path/flat configurations; expected: typed oracle."""
        self.repeat_case(ec_client, json_data, case, filter_params={"expr_use_json_stats": False})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "case",
        case_params(
            [
                c
                for c in json_cases()
                if c.name
                in {
                    "number_eq",
                    "number_range",
                    "term",
                    "nested",
                    "offset",
                    "exists",
                    "contains",
                    "missing",
                }
            ]
        ),
    )
    def test_json_stats_request_option(self, ec_client, json_data, case):
        """target: stats option compatibility; method: alternate toggle; expected: unchanged semantics."""
        for enabled in (False, True, False, True, False, True):
            self.verify_query(ec_client, json_data, case, filter_params={"expr_use_json_stats": enabled})
            self.verify_search(ec_client, json_data, case, filter_params={"expr_use_json_stats": enabled})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("case", case_params(array_cases()))
    def test_arrays(self, ec_client, raw, case):
        """target: ARRAY null/empty/offsets; method: repeat predicates; expected: exact oracle."""
        self.repeat_case(ec_client, raw, case)


class TestExprCacheIndependent(ExprCacheTestBase):
    """When to add tests here: ordered requests, writes or lifecycle transitions.

    Fixture: make_dataset creates fresh collections for every test invocation.
    No xdist_group: loadgroup may distribute these invocations across workers;
    cache warmup, mutations and verification stay inside the same invocation.
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_type", ["INVERTED", "STL_SORT", "BITMAP"])
    def test_numeric_index_operator_matrix(self, ec_client, make_dataset, index_type):
        """target: legal numeric index/operator combinations; method: representative leaves/NOT; expected: oracle."""
        data = make_dataset(profile="minimal", extra_indexes=(("n", index_type),))
        cases = [c for c in scalar_cases() if c.name in {"n_eq", "in_3", "not_in_3", "range_<=_<"}]
        cases += [
            FilterCase("null", "n is null", lambda r: r["n"] is None),
            FilterCase("not_range", "not (-2 <= n < 2)", lambda r: None if r["n"] is None else not (-2 <= r["n"] < 2)),
        ]
        for case in cases:
            self.repeat_case(ec_client, data, case)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_type", ["INVERTED", "BITMAP"])
    def test_indexed_arrays(self, ec_client, make_dataset, index_type):
        """target: numeric/string ARRAY indexes; method: contains/NULL/NOT; expected: independent array oracle."""
        data = make_dataset(extra_indexes=(("ai", index_type), ("astr", index_type)))
        cases = [
            c for c in array_cases() if c.name in {"contains", "any", "all", "strings", "strings_any", "strings_all"}
        ]
        cases += [
            FilterCase("null", "ai is null", lambda r: r["ai"] is None),
            FilterCase(
                "not_contains", "not array_contains(ai, 1)", lambda r: None if r["ai"] is None else 1 not in r["ai"]
            ),
        ]
        for case in cases:
            self.repeat_case(ec_client, data, case)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("profile", ["raw", "json_path", "json_flat"])
    def test_json_integer_precision(self, ec_client, make_dataset, profile):
        """target: JSON literals around +/-2^53; method: adjacent int64 bounds/terms; expected: no double aliasing."""
        boundary = 2**53
        values = [
            None,
            -boundary - 2,
            -boundary - 1,
            -boundary,
            -boundary + 1,
            -1,
            0,
            boundary - 1,
            boundary,
            boundary + 1,
            boundary + 2,
            str(boundary + 1),
            True,
        ]
        rows = make_rows(profile=profile)
        for row in rows:
            row["j"] = {"a": values[row["id"] % len(values)]}
        targets = {"json_path": ('j["a"]',), "json_flat": ("j",)}.get(profile)
        data = make_dataset(rows=rows, profile=profile, index_targets=targets)
        for sign in (-1, 1):
            adjacent = [sign * boundary, sign * (boundary + 1)]
            equality = [
                FilterCase(
                    f"equal_{v}", f'j["a"] == {v}', lambda r, v=v: nullable_compare(json_number(r, "a"), operator.eq, v)
                )
                for v in adjacent
            ]
            self.alternate(ec_client, data, *equality, filter_params={"expr_use_json_stats": False})
            value = adjacent[1]
            for expr, pred in [
                (f'j["a"] in [{value}]', lambda v, value=value: v == value),
                (f'{value - 1} < j["a"] < {value + 1}', lambda v, value=value: value - 1 < v < value + 1),
            ]:
                case = FilterCase(
                    "precise_int64",
                    expr,
                    lambda r, pred=pred: None if json_number(r, "a") is None else pred(json_number(r, "a")),
                )
                # Both settings request identical semantics; neither proves stats use.
                for enabled in (False, True):
                    self.repeat_case(ec_client, data, case, filter_params={"expr_use_json_stats": enabled})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("leaf_count", [4, 8, 16])
    def test_mixed_expression_tree(self, ec_client, make_dataset, leaf_count):
        """target: larger mixed expression trees; method: indexed/raw/JSON/text leaves; expected: tri-valued oracle."""
        data = make_dataset(profile="mixed", index_targets=('j["a"]',), extra_indexes=(("n", "INVERTED"),))
        leaves = []
        for threshold, number, element, term in zip(
            (-7, -1, 3, 7), (-1, 1, 7, 9), (1, 2, 7, 10), ("vector", "database", "milvus", "other")
        ):
            leaves.extend(
                [
                    FilterCase(
                        "indexed", f"n > {threshold}", lambda r, v=threshold: nullable_compare(r["n"], operator.gt, v)
                    ),
                    FilterCase(
                        "json",
                        f'j["a"] >= {number}',
                        lambda r, v=number: nullable_compare(json_number(r, "a"), operator.ge, v),
                    ),
                    FilterCase(
                        "raw_array",
                        f"array_contains(ai, {element})",
                        lambda r, v=element: None if r["ai"] is None else v in r["ai"],
                    ),
                    FilterCase(
                        "text",
                        f'TEXT_MATCH(text, "{term}")',
                        lambda r, v=term: None if r["text"] is None else v in r["text"].split(),
                    ),
                ]
            )
        blocks = [leaves[i : i + 4] for i in range(0, leaf_count, 4)]
        expr = " and ".join("(" + " or ".join(c.expr for c in block) + ")" for block in blocks)

        def predicate(row):
            return reduce(tri_and, (reduce(tri_or, (c.predicate(row) for c in block)) for block in blocks))

        positive = FilterCase("mixed", expr, predicate)
        negative = FilterCase("not_mixed", f"not ({expr})", lambda r: tri_not(predicate(r)))
        assert 0 < len(positive.expected(data.rows)) < len(data.rows)
        for enabled in (False, True):
            self.alternate(ec_client, data, positive, negative, filter_params={"expr_use_json_stats": enabled})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("family", ["text", "phrase", "fuzzy"])
    def test_text_parameters(self, ec_client, make_dataset, family):
        """target: text parameter keys; method: A/B/A and reverse; expected: labelled corpus IDs."""
        text_data = make_dataset(profile="text")
        if family == "text":
            a = FilterCase(
                "msm_1", 'TEXT_MATCH(text, "vector database", minimum_should_match=1)', lambda r: r["text"] in TEXTS[:5]
            )
            b = FilterCase(
                "msm_2", 'TEXT_MATCH(text, "vector database", minimum_should_match=2)', lambda r: r["text"] in TEXTS[:3]
            )
        elif family == "phrase":
            a = FilterCase("slop_0", 'PHRASE_MATCH(text, "vector database", 0)', lambda r: r["text"] == TEXTS[0])
            b = FilterCase("slop_1", 'PHRASE_MATCH(text, "vector database", 1)', lambda r: r["text"] in TEXTS[:2])
            self.repeat_case(
                ec_client,
                text_data,
                FilterCase("slop_2", 'PHRASE_MATCH(text, "vector database", 2)', lambda r: r["text"] in TEXTS[:3]),
            )
        else:
            a = FilterCase(
                "distance_0", 'TEXT_MATCH_FUZZY(text, "milvus", max_edit_distance=0)', lambda r: r["text"] == TEXTS[5]
            )
            b = FilterCase(
                "distance_1", 'TEXT_MATCH_FUZZY(text, "milvus", max_edit_distance=1)', lambda r: r["text"] in TEXTS[5:7]
            )
            self.repeat_case(
                ec_client,
                text_data,
                FilterCase(
                    "distance_2",
                    'TEXT_MATCH_FUZZY(text, "milvus", max_edit_distance=2)',
                    lambda r: r["text"] in TEXTS[5:8],
                ),
            )
        self.alternate(ec_client, text_data, a, b)

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_fields_terms_and_not(self, ec_client, make_dataset):
        """target: text field/term/null isolation; method: alternate leaves/NOT; expected: corpus oracle."""
        text_data = make_dataset(profile="text")
        a = FilterCase("text_vector", 'TEXT_MATCH(text, "vector")', lambda r: r["text"] in TEXTS[:4])
        b = FilterCase("text2_vector", 'TEXT_MATCH(text2, "vector")', lambda r: r["text2"] in TEXTS[:4])
        self.alternate(ec_client, text_data, a, b)
        self.alternate(
            ec_client,
            text_data,
            a,
            FilterCase("text_milvus", 'TEXT_MATCH(text, "milvus")', lambda r: r["text"] == TEXTS[5]),
        )
        for case in [
            FilterCase("text_absent", 'TEXT_MATCH(text, "nonexistent")', lambda r: False),
            FilterCase(
                "text_not",
                'not TEXT_MATCH(text, "vector")',
                lambda r: r["text"] is not None and r["text"] not in TEXTS[:4],
            ),
            FilterCase(
                "mixed_text",
                'TEXT_MATCH(text, "vector") and n > 0',
                lambda r: a.predicate(r) and r["n"] is not None and r["n"] > 0,
            ),
        ]:
            self.repeat_case(ec_client, text_data, case)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("profile", ["raw", "indexed"])
    @pytest.mark.parametrize(
        "dimension", ["field", "operator", "literal", "inclusive", "arithmetic", "path", "json_field", "offset"]
    )
    def test_key_isolation(self, ec_client, make_dataset, dimension, profile):
        """target: result-affecting key parts; method: alternate A/B; expected: distinct correct sets."""
        data = make_dataset(profile=profile)
        a = FilterCase("n_gt_0", "n > 0", lambda r: nullable_compare(r["n"], operator.gt, 0))
        pairs = {
            "field": (a, FilterCase("m_gt_0", "m > 0", lambda r: nullable_compare(r["m"], operator.gt, 0))),
            "operator": (a, FilterCase("n_lt_0", "n < 0", lambda r: nullable_compare(r["n"], operator.lt, 0))),
            "literal": (a, FilterCase("n_gt_5", "n > 5", lambda r: nullable_compare(r["n"], operator.gt, 5))),
            "inclusive": (
                FilterCase("exclusive", "0 < n < 5", lambda r: r["n"] is not None and 0 < r["n"] < 5),
                FilterCase("inclusive", "0 <= n <= 5", lambda r: r["n"] is not None and 0 <= r["n"] <= 5),
            ),
            "arithmetic": (
                FilterCase("plus_1", "n + 1 > 5", lambda r: r["n"] is not None and r["n"] + 1 > 5),
                FilterCase("plus_2", "n + 2 > 5", lambda r: r["n"] is not None and r["n"] + 2 > 5),
            ),
            "path": (
                FilterCase("a", 'j["a"] == 7', lambda r: json_number(r, "a") == 7),
                FilterCase("arr", 'j["arr"][0] == 7', lambda r: json_number(r, "arr", 0) == 7),
            ),
            "json_field": (
                FilterCase("j", 'j["a"] == 7', lambda r: json_number(r, "a") == 7),
                FilterCase("j2", 'j2["a"] == 7', lambda r: json_number(r, "a", field_name="j2") == 7),
            ),
            "offset": (
                FilterCase("offset_0", "ai[0] == 7", lambda r: bool(r["ai"]) and r["ai"][0] == 7),
                FilterCase(
                    "offset_1", "ai[1] == 7", lambda r: r["ai"] is not None and len(r["ai"]) > 1 and r["ai"][1] == 7
                ),
            ),
        }
        self.alternate(ec_client, data, *pairs[dimension], filter_params={"expr_use_json_stats": False})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("template", ["value", "list", "empty_list"])
    def test_template_parameters(self, ec_client, make_dataset, template):
        """target: template values enter identity; method: alternate and literal controls; expected: oracle."""
        data = make_dataset(profile="minimal")
        if template == "value":
            values, expr = [0, 500], "n == {v}"

            def predicate(r, v):
                return nullable_compare(r["n"], operator.eq, v)
        else:
            values, expr = ([[1, 2], [3, 4]] if template == "list" else [[], [1, 2]]), "n in {v}"

            def predicate(r, v):
                return None if r["n"] is None else r["n"] in v

        cases = [
            FilterCase(f"template_{i}", expr, lambda r, v=v: predicate(r, v), {"v": v}) for i, v in enumerate(values)
        ]
        self.alternate(ec_client, data, *cases)
        for case in cases:
            literal = FilterCase("literal", expr.replace("{v}", json.dumps(case.params["v"])), case.predicate)
            self.repeat_case(ec_client, data, literal)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("reverse", [False, True], ids=["positive_first", "negative_first"])
    def test_three_valued_composition(self, ec_client, make_dataset, reverse):
        """target: validity survives NOT; method: all T/F/U pairs; expected: SQL truth table."""
        rows = make_rows(profile="minimal")
        for i, row in enumerate(rows):
            row["n"], row["m"] = [None, -1, 1][i % 3], [None, -1, 1][(i // 3) % 3]
        data = make_dataset(rows=rows, profile="minimal")

        def a(r):
            return nullable_compare(r["n"], operator.gt, 0)

        def b(r):
            return nullable_compare(r["m"], operator.gt, 0)

        cases = [
            FilterCase("A", "n > 0", a),
            FilterCase("NOT_A", "not (n > 0)", lambda r: tri_not(a(r))),
            FilterCase("AND", "n > 0 and m > 0", lambda r: tri_and(a(r), b(r))),
            FilterCase("OR", "n > 0 or m > 0", lambda r: tri_or(a(r), b(r))),
            FilterCase("NOT_AND", "not (n > 0 and m > 0)", lambda r: tri_not(tri_and(a(r), b(r)))),
            FilterCase("NOT_OR", "not (n > 0 or m > 0)", lambda r: tri_not(tri_or(a(r), b(r)))),
            FilterCase("double_not", "not not (n > 0)", a),
            FilterCase("null_or_A", "m is null or n > 0", lambda r: tri_or(r["m"] is None, a(r))),
            FilterCase("field_compare", "n > m", lambda r: nullable_compare(r["n"], operator.gt, r["m"])),
            FilterCase(
                "nested", "(n > 0 and m > 0) or not (n > 0)", lambda r: tri_or(tri_and(a(r), b(r)), tri_not(a(r)))
            ),
        ]
        # Start with the requested A/NOT-A sequence on untouched expressions.
        sequence = [cases[1], cases[0], cases[1]] if reverse else [cases[0], cases[1], cases[0]]
        sequence += [cases[0], cases[2], cases[3], cases[0]]
        sequence += cases[4:]
        oracles = {case.name: ExpectedResult(case.expected(data.rows)) for case in cases}
        for _ in range(REPEATS):
            for case in sequence:
                self.verify_query(ec_client, data, case, oracle=oracles[case.name])
                self.verify_search(ec_client, data, case, oracle=oracles[case.name])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("reverse", [False, True], ids=["positive_first", "negative_first"])
    @pytest.mark.parametrize("expr", ["n > 10", "10 <= n <= 20", "n in [10, 20]"], ids=["unary", "range", "term"])
    def test_skipped_nullable_values_under_not(self, ec_client, make_dataset, reverse, expr):
        """target: null-rejection key isolation; method: all-skip values then NOT; expected: NULL stays unknown."""
        rows = make_rows(profile="minimal")
        for i, row in enumerate(rows):
            row["n"] = None if i % 3 == 0 else 5
        data = make_dataset(rows=rows, profile="minimal")
        # No valid value can match: min/max metadata may skip every raw chunk.
        # A null-rejecting result must not supply invented validity beneath NOT.
        positive = FilterCase("all_skip", expr, lambda r: None if r["n"] is None else False)
        negative = FilterCase("not_all_skip", f"not ({expr})", lambda r: tri_not(positive.predicate(r)))
        first, second = (negative, positive) if reverse else (positive, negative)
        assert not positive.expected(rows)
        assert 0 < len(negative.expected(rows)) < len(rows)
        oracles = {case.name: ExpectedResult(case.expected(data.rows)) for case in (positive, negative)}
        for _ in range(REPEATS):
            for case in (first, second, first):
                self.verify_query(ec_client, data, case, oracle=oracles[case.name])
                self.verify_search(ec_client, data, case, oracle=oracles[case.name])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("reverse", [False, True], ids=["positive_first", "negative_first"])
    def test_json_flat_validity_isolation(self, ec_client, make_dataset, reverse):
        """target: flat-index validity keys; method: alternate fields/types/NOT; expected: typed truth tables."""
        samples = [{"a": v} for v in (7, 8, "7", "8", True, False, None, [], {"nested": 7}, {"other": 8})] + [
            {},
            None,
            {"other": 7},
        ]
        rows = make_rows()
        for i, row in enumerate(rows):
            row["j"] = copy.deepcopy(samples[i % len(samples)])
            row["j2"] = copy.deepcopy(samples[(i + 6) % len(samples)])
        data = make_dataset(rows=rows, profile="json_flat")
        pairs = []
        for value, types in [(7, (int, float)), ("7", (str,)), (True, (bool,))]:
            for field_name in ("j", "j2"):

                def predicate(row, field_name=field_name, value=value, types=types):
                    actual = json_path(row, "a", field_name=field_name)
                    return actual == value if type(actual) in types else None

                positive = FilterCase(
                    f"{field_name}_{types[0].__name__}", f'{field_name}["a"] == {json.dumps(value)}', predicate
                )
                negative = FilterCase(
                    f"not_{positive.name}", f"not ({positive.expr})", lambda r, p=predicate: tri_not(p(r))
                )
                assert positive.expected(rows) and negative.expected(rows)
                pairs.append((negative, positive) if reverse else (positive, negative))
        if reverse:
            pairs.reverse()
        # Interleave value families and fields before returning to each leaf;
        # both JSON fields have materialized flat indexes on this fresh collection.
        cases = [case for pair in pairs for case in pair]
        assert len({frozenset(r["id"] for r in case.expected(rows)) for case in cases}) == len(cases), (
            "field/type/NOT isolation requires distinct expected ID sets"
        )
        oracles = {case.name: ExpectedResult(case.expected(data.rows)) for case in cases}
        for _ in range(REPEATS):
            for case in cases:
                self.verify_query(
                    ec_client,
                    data,
                    case,
                    output_fields=("id", "j", "j2"),
                    filter_params={"expr_use_json_stats": False},
                    oracle=oracles[case.name],
                )
                self.verify_search(
                    ec_client,
                    data,
                    case,
                    output_fields=("j", "j2"),
                    filter_params={"expr_use_json_stats": False},
                    oracle=oracles[case.name],
                )

    @pytest.mark.tags(CaseLabel.L1)
    def test_dynamic_fields(self, ec_client, make_dataset):
        """target: dynamic keys/missing paths; method: alternate predicates; expected: oracle."""
        data = make_dataset(profile="dynamic")
        a = FilterCase("dynamic_score", "dyn_score >= 7", lambda r: r.get("dyn_score", -1) >= 7)
        b = FilterCase(
            "dynamic_nested", 'dyn_meta["score"] >= 3', lambda r: r.get("dyn_meta", {}).get("score", -1) >= 3
        )
        self.alternate(ec_client, data, a, b, filter_params={"expr_use_json_stats": False})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "profile,sealed",
        [
            pytest.param("minimal", False, id="raw_growing"),
            pytest.param("minimal", True, id="raw_sealed"),
            pytest.param("indexed", True, id="scalar_indexed"),
            pytest.param("json_path", True, id="json_path_indexed"),
            pytest.param("json_flat", True, id="json_flat_indexed"),
            pytest.param("text", True, id="text_sealed"),
        ],
    )
    def test_write_visibility(self, ec_client, make_dataset, profile, sealed):
        """target: writes supersede membership; method: insert/delete/upsert; expected: current rows."""
        q = vector_for_id(23)

        def set_value(row, value):
            row["n"] = value
            if profile in ("json_path", "json_flat"):
                row["j"] = None if value is None else {"a": value}
            elif profile == "text":
                row["text"] = None if value is None else ("vector ready" if value > 0 else "other ready")

        rows = make_rows(profile=profile)
        for row in rows:
            set_value(row, row["n"])
            row["vec"][0] += 64
        # Controlled nearby rows make every membership transition affect top-K.
        for i, value in enumerate([5, -5, 5, -5, None, 5, None, -5]):
            set_value(rows[i], value)
            rows[i]["vec"] = [q[0] + (i + 1) / 64] + q[1:]
        # This family mutates one indexed field/path. The shared expression
        # fixtures retain the full index matrix for operator/type coverage.
        index_targets = {"indexed": ("n",), "json_path": ('j["a"]',), "json_flat": ("j",)}.get(profile)
        data = make_dataset(rows=rows, profile=profile, sealed=sealed, index_targets=index_targets)
        if profile in ("json_path", "json_flat"):
            expr = 'j["a"] > 0'

            def predicate(r):
                return nullable_compare(json_number(r, "a"), operator.gt, 0)

        elif profile == "text":
            expr = 'TEXT_MATCH(text, "vector")'

            def predicate(r):
                return None if r["text"] is None else "vector" in r["text"].split()

        else:
            expr = "n > 0"

            def predicate(r):
                return nullable_compare(r["n"], operator.gt, 0)

        cases = [
            FilterCase("positive", expr, predicate),
            FilterCase("negative", f"not ({expr})", lambda r: tri_not(predicate(r))),
        ]

        def checkpoint(previous=None, changed=("positive", "negative")):
            ranks = {case.name: ranked_ids(case.expected(data.rows), q, 10) for case in cases}
            if previous is not None:
                for name in changed:
                    assert ranks[name] != previous[name], f"fixture write must change {name} top-K"
            if profile == "text" and previous is not None:
                wait_for_text_rows(ec_client, data)
            for case in cases:
                self.repeat_case(ec_client, data, case, filter_params={"expr_use_json_stats": False})
            return ranks

        ranks = checkpoint()
        added = make_rows(2, start=ROWS, profile=profile)
        for row, value, delta in zip(added, (5, -5), (1 / 1024, 0)):
            set_value(row, value)
            row["vec"] = [q[0] + delta] + q[1:]
        self.insert(ec_client, data.name, data=added)
        data.rows.extend(added)
        ranks = checkpoint(ranks)
        deleted = [0, 1]
        assert deleted[0] in ranks["positive"] and deleted[1] in ranks["negative"]
        self.delete(ec_client, data.name, ids=deleted)
        data.rows = [r for r in data.rows if r["id"] not in deleted]
        ranks = checkpoint(ranks)
        updates = []
        for row_id, new in [(2, -5), (3, 5), (4, 5), (5, None), (6, -5), (7, None)]:
            row = copy.deepcopy(next(r for r in data.rows if r["id"] == row_id))
            set_value(row, new)
            updates.append(row)
        self.upsert(ec_client, data.name, data=updates)
        replacement = {r["id"]: r for r in updates}
        data.rows = [replacement.get(r["id"], r) for r in data.rows]
        ranks = checkpoint(ranks)
        # Same membership, different vector: the moved row becomes nearest.
        moved = copy.deepcopy(next(r for r in data.rows if r["id"] == 3))
        moved["vec"] = q
        self.upsert(ec_client, data.name, data=[moved])
        data.rows = [moved if r["id"] == moved["id"] else r for r in data.rows]
        checkpoint(ranks, changed=("positive",))

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_and_partition_isolation(self, ec_client, make_dataset):
        """target: scope isolation; method: same filter/PKs in different scopes; expected: local data."""
        first = make_dataset(profile="minimal")
        rows = make_rows(profile="minimal")
        for r in rows:
            r["n"] = None if r["n"] is None else -r["n"]
        second = make_dataset(rows=rows, profile="minimal")
        case = FilterCase("scope", "n > 0", lambda r: nullable_compare(r["n"], operator.gt, 0))
        assert {r["id"] for r in case.expected(first.rows)} != {r["id"] for r in case.expected(second.rows)}
        for data in [first, second, first, second, first, second, first, second, first, second]:
            self.verify_query(ec_client, data, case)
            self.verify_search(ec_client, data, case)
        partitioned = make_dataset(rows=[], profile="minimal", defer_load=True)
        for partition in ("p_a", "p_b"):
            self.create_partition(ec_client, partitioned.name, partition)
        part_rows = [make_rows(1025, profile="minimal"), make_rows(1025, start=2000, profile="minimal")]
        for partition, rows in zip(("p_a", "p_b"), part_rows):
            self.insert(ec_client, partitioned.name, data=rows, partition_name=partition)
        self.flush(ec_client, partitioned.name)
        self.load_collection(ec_client, partitioned.name)
        wait_for_segment_mode(ec_client, partitioned.name, "sealed", sum(map(len, part_rows)), timeout=TIMEOUT)
        for _ in range(REPEATS):
            for rows, names in [(part_rows[0], ["p_a"]), (part_rows[1], ["p_b"]), (sum(part_rows, []), ["p_a", "p_b"])]:
                scoped = Dataset(partitioned.name, rows)
                self.verify_query(ec_client, scoped, case, partition_names=names)
                self.verify_search(ec_client, scoped, case, partition_names=names)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("leaf_first", [False, True], ids=["partial_first", "full_first"])
    @pytest.mark.parametrize("logical", ["and", "or"])
    @pytest.mark.parametrize("layout", ["skip_prefix", "skip_middle", "mask_middle"])
    def test_partial_evaluation_does_not_pollute_leaf(self, ec_client, make_dataset, leaf_first, logical, layout):
        """target: incomplete captures; method: short-circuit across batches then leaf; expected: full results."""
        rows = make_rows(2 * BATCH + 1, profile="minimal")
        active, inactive = (1, -1) if logical == "and" else (-1, 1)
        for i, row in enumerate(rows):
            row["n"] = active
            if layout == "skip_prefix" and i < BATCH:
                row["n"] = inactive
            elif BATCH <= i < 2 * BATCH:
                if layout == "skip_middle":
                    row["n"] = inactive
                elif layout == "mask_middle":
                    row["n"] = [active, inactive, None][i % 3]
            row["m"] = [None, -2, 1][i % 3]
        rows[-1]["m"] = 1
        data = make_dataset(rows=rows, profile="minimal")
        assert_single_sealed_segment(ec_client, data)

        def a(r):
            return nullable_compare(r["n"], operator.gt, 0)

        def b(r):
            return None if r["m"] is None else r["m"] + 1 > 0

        combine = tri_and if logical == "and" else tri_or
        leaf = FilterCase("leaf", "m + 1 > 0", b)
        compound = FilterCase("short_circuit", f"n > 0 {logical} m + 1 > 0", lambda r: combine(a(r), b(r)))
        # Middle layouts offer a full first batch, then a broken capture, then
        # an active matching tail. The prefix layout covers failure to start.
        # These are execution opportunities, not assertions about cache hits.
        first, second = (leaf, compound) if leaf_first else (compound, leaf)
        self.repeat_case(ec_client, data, first)
        self.repeat_case(ec_client, data, second)
        if leaf_first:
            # Recheck after the compound; partial-first already checked the
            # leaf six times immediately after the compound with no writes.
            self.repeat_case(ec_client, data, leaf)
        self.repeat_case(ec_client, data, FilterCase("not_leaf", f"not ({leaf.expr})", lambda r: tri_not(b(r))))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("count", [0, 1, 63, 64, 65, 127, 128, 129, BATCH - 1, BATCH, BATCH + 1])
    def test_bitmap_boundaries(self, ec_client, make_dataset, count):
        """target: bitmap words/tails; method: extreme/selective predicates; expected: no missing tail bits."""
        rows = make_rows(count, profile="minimal")
        for i, row in enumerate(rows):
            row["n"], row["m"] = i, None
        data = make_dataset(rows=rows, profile="minimal")
        if count:
            assert_single_sealed_segment(ec_client, data)
        cases = [
            FilterCase("empty", "n < 0", lambda r: False),
            FilterCase("full", "n >= 0", lambda r: True),
            FilterCase("tail", f"n == {count - 1}", lambda r: r["n"] == count - 1),
            FilterCase("all_null", "m is null", lambda r: True),
            FilterCase("not_unknown", "not (m > 0)", lambda r: None),
        ]
        for fraction in (0.029, 0.03, 0.031, 0.969, 0.97, 0.971):
            threshold = int(count * fraction)
            cases.append(FilterCase(f"selectivity_{fraction}", f"n < {threshold}", lambda r, t=threshold: r["n"] < t))
        seen = set()
        for case in cases:
            # Integer rounding collapses density thresholds for small fixtures.
            # Keep six evaluations of every distinct expression, including
            # distinct expressions that happen to select the same IDs.
            if case.expr in seen:
                continue
            seen.add(case.expr)
            self.repeat_case(ec_client, data, case)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("layout", ["clustered", "scattered"])
    @pytest.mark.parametrize(
        "valid_count,match_count",
        [
            pytest.param(0, 0, id="all_null"),
            pytest.param(290, 100, id="valid_below_3pct"),
            pytest.param(300, 100, id="valid_at_3pct"),
            pytest.param(310, 100, id="valid_above_3pct"),
            pytest.param(5000, 2500, id="half_valid_half_matches"),
            pytest.param(9690, 5000, id="valid_below_97pct"),
            pytest.param(9700, 100, id="valid_at_97pct_sparse_matches"),
            pytest.param(9710, 5000, id="valid_above_97pct"),
            pytest.param(9900, 9800, id="dense_valid_dense_matches"),
            pytest.param(10000, 100, id="all_valid_sparse_matches"),
        ],
    )
    def test_result_and_validity_densities(self, ec_client, make_dataset, layout, valid_count, match_count):
        """target: independent result/validity bitmaps; method: NULL densities and NOT; expected: exact membership."""
        rows = make_rows(10000, profile="minimal")
        for i, row in enumerate(rows):
            # Multiplication coprime to 10000 permutes positions without RNG.
            position = i if layout == "clustered" else (i * 7919) % len(rows)
            row["n"] = None if position >= valid_count else int(position < match_count)
        data = make_dataset(rows=rows, profile="minimal")
        assert_single_sealed_segment(ec_client, data)
        positive = FilterCase("positive", "n + 1 > 1", lambda r: None if r["n"] is None else r["n"] + 1 > 1)
        negative = FilterCase("negative", f"not ({positive.expr})", lambda r: tri_not(positive.predicate(r)))
        assert len(positive.expected(rows)) == match_count
        assert len(negative.expected(rows)) == valid_count - match_count
        # Start under NOT so preserving validity is essential from the first
        # request. Memory may encode each bitmap differently; disk stays raw.
        oracles = {case.name: ExpectedResult(case.expected(data.rows)) for case in (positive, negative)}
        for _ in range(REPEATS):
            for case in (negative, positive, negative):
                self.verify_query(ec_client, data, case, oracle=oracles[case.name])
                self.verify_search(ec_client, data, case, oracle=oracles[case.name])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("count", [65535, 65536, 65537])
    def test_bitmap_container_boundaries(self, ec_client, make_dataset, count):
        """target: 16-bit container edges/tails; method: sparse/dense results across 65536; expected: complete IDs."""
        rows = make_rows(count, profile="minimal")
        edges = [i for i in (0, 63, 64, 65534, 65535, 65536) if i < count]
        for row in rows:
            row["n"] = row["id"]
        data = make_dataset(rows=rows, profile="minimal")
        assert_single_sealed_segment(ec_client, data)
        sparse = FilterCase("sparse_edges", f"n in {edges}", lambda r: r["n"] in edges)
        dense = FilterCase("dense_edges", f"not ({sparse.expr})", lambda r: r["n"] not in edges)
        # Query helper fully drains an iterator above the query result window.
        # The same correctness cases also run with OFF and raw disk storage.
        for case in (sparse, dense):
            self.repeat_case(ec_client, data, case)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("iterative_first", [False, True], ids=["full_first", "offset_first"])
    def test_iterative_filter_interleaving(self, ec_client, make_dataset, iterative_first):
        """target: full/offset reuse; method: alternate execution modes; expected: exact filtered top-K."""
        data = make_dataset()
        case = FilterCase("raw_arithmetic", "n + 2 > 5", lambda r: r["n"] is not None and r["n"] + 2 > 5)
        oracle = ExpectedResult(case.expected(data.rows))
        for mode in [iterative_first, not iterative_first, iterative_first, not iterative_first]:
            for limit in (1, 10, 100):
                self.verify_search(
                    ec_client,
                    data,
                    case,
                    vectors=[vector_for_id(23), vector_for_id(1800)],
                    limit=limit,
                    iterative=mode,
                    oracle=oracle,
                )
        self.repeat_case(ec_client, data, case)

    @pytest.mark.tags(CaseLabel.L2)
    def test_request_shape_changes(self, ec_client, make_dataset):
        """target: filter reuse across requests; method: vary nq/topK/projection/limit; expected: exact responses."""
        data = make_dataset(profile="minimal")
        case = FilterCase("few_matches", "m > 0", lambda r: nullable_compare(r["m"], operator.gt, 0))
        # Keep fewer than 100 matches, so top-K also verifies underfilled results.
        narrow = FilterCase(
            "few_matches", "m > 0 and id < 50", lambda r: r["m"] is not None and r["m"] > 0 and r["id"] < 50
        )
        oracles = [ExpectedResult(current.expected(data.rows)) for current in (case, narrow)]
        for _ in range(REPEATS):
            for current, oracle in zip((case, narrow), oracles):
                for limit in (1, 10, 100):
                    for fields in (("id",), ("id", "n", "m", "vec")):
                        self.verify_query(ec_client, data, current, output_fields=fields, limit=limit, oracle=oracle)
                    for nq in (1, 3):
                        vectors = [vector_for_id(i) for i in (23, 1800, 1000)[:nq]]
                        self.verify_search(
                            ec_client,
                            data,
                            current,
                            vectors=vectors,
                            limit=limit,
                            output_fields=("n", "m", "vec"),
                            oracle=oracle,
                        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_large_search_request(self, ec_client, make_dataset):
        """target: larger nq/topK reuse; method: alternate 1/1 and 16/512; expected: fresh exact results."""
        data = make_dataset(profile="minimal")
        case = FilterCase("large_request", "n >= 0", lambda r: nullable_compare(r["n"], operator.ge, 0))
        oracle = ExpectedResult(case.expected(data.rows))
        assert len(oracle.rows) > 512
        for _ in range(REPEATS):
            for nq, limit in ((1, 1), (16, 512)):
                self.verify_search(
                    ec_client,
                    data,
                    case,
                    vectors=[vector_for_id(23 + i * 113) for i in range(nq)],
                    limit=limit,
                    oracle=oracle,
                )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("api", ["query", "search"])
    @pytest.mark.parametrize("early_close", [False, True], ids=["consume", "close_after_page"])
    def test_iterator_lifecycle(self, ec_client, make_dataset, api, early_close):
        """target: iterator consumption; method: exhaust or close after a page then reread; expected: complete results."""
        # Stopping consumption is not in-flight server cancellation: filtering
        # may finish before the first page. Partial capture has separate tests.
        data = make_dataset(profile="minimal")
        case = FilterCase("iterator", "n + 1 > 0", lambda r: r["n"] is not None and r["n"] + 1 > 0)
        expected = case.expected(data.rows)
        if api == "query":
            iterator = self.query_iterator(
                ec_client,
                data.name,
                filter=case.expr,
                batch_size=17,
                output_fields=["id", "n"],
                consistency_level="Strong",
            )[0]
        else:
            iterator = self.search_iterator(
                ec_client,
                data.name,
                data=[vector_for_id(23)],
                anns_field="vec",
                filter=case.expr,
                batch_size=17,
                limit=len(expected),
                output_fields=["n"],
                search_params={"metric_type": "L2", "params": {}},
                consistency_level="Strong",
            )[0]
        collected = []
        try:
            while batch := iterator.next():
                collected.extend(batch)
                assert len(collected) <= len(expected), "iterator returned more rows than the complete filtered dataset"
                if early_close:
                    break
        finally:
            iterator.close()
        assert collected, "iterator must exercise at least one nonempty page"
        assert len(collected) == (17 if early_close else len(expected))
        if api == "query":
            ids = [r["id"] for r in collected]
            assert len(ids) == len(set(ids))
            by_id = {r["id"]: r for r in expected}
            assert set(ids) <= set(by_id)
            if not early_close:
                assert set(ids) == set(by_id)
            for row in collected:
                assert_fields(row, by_id[row["id"]], ("id", "n"))
        else:
            # The synchronous SDK search iterator returns Hits/Hit objects,
            # whereas MilvusClient.search returns dictionaries.
            hits = [{"id": h.id, "distance": h.distance, "entity": {"n": h.entity.get("n")}} for h in collected]
            assert_exact_hits(hits, expected, vector_for_id(23), len(collected))
        self.repeat_case(ec_client, data, case)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("filter_relation", ["disjoint", "same", "overlap"])
    def test_hybrid_search(self, ec_client, make_dataset, filter_relation):
        """target: per-route filters; method: repeat two FLAT routes and RRF; expected: local rank fusion."""
        rows = make_rows(129, profile="minimal")
        for row in rows:
            row["vec"] = [float(row["id"])] + [0.0] * 7
        data = make_dataset(rows=rows, profile="minimal")
        a = FilterCase("positive", "n > 0", lambda r: r["n"] is not None and r["n"] > 0)
        b = {
            "same": a,
            "disjoint": FilterCase("negative", "n < 0", lambda r: r["n"] is not None and r["n"] < 0),
            "overlap": FilterCase("below_five", "n < 5", lambda r: r["n"] is not None and r["n"] < 5),
        }[filter_relation]
        vectors = [[-1.0] + [0.0] * 7, [130.0] + [0.0] * 7]
        reqs, scores, route_ids = [], {}, []
        for case, q in zip((a, b), vectors):
            reqs.append(
                AnnSearchRequest(
                    data=[q], anns_field="vec", param={"metric_type": "L2", "params": {}}, limit=50, expr=case.expr
                )
            )
            # Opposite ends of the fixture give disagreeing, tie-free ranks.
            ids = ranked_ids(case.expected(rows), q, 50)
            route_ids.append(ids)
            for rank, row_id in enumerate(ids, 1):
                scores[row_id] = scores.get(row_id, 0.0) + 1.0 / (60 + rank)
        common = set(route_ids[0]) & set(route_ids[1])
        assert route_ids[0] != route_ids[1]
        if filter_relation == "overlap":
            assert common and set(route_ids[0]) - common and set(route_ids[1]) - common
        elif filter_relation == "same":
            assert common
        else:
            assert not common
        expected_scores = sorted(scores.values(), reverse=True)[:10]
        by_id = {r["id"]: r for r in rows}
        for _ in range(REPEATS):
            result = self.hybrid_search(
                ec_client,
                data.name,
                reqs=reqs,
                ranker=RRFRanker(k=60),
                limit=10,
                output_fields=["n"],
                consistency_level="Strong",
            )[0]
            assert len(result) == 1 and len(result[0]) == len(expected_scores)
            hits = result[0]
            assert len({h["id"] for h in hits}) == len(hits)
            np.testing.assert_allclose([h["distance"] for h in hits], expected_scores, rtol=1e-5, atol=1e-6)
            for hit in hits:
                assert hit["id"] in scores
                np.testing.assert_allclose(hit["distance"], scores[hit["id"]], rtol=1e-5, atol=1e-6)
                assert_fields(hit["entity"], by_id[hit["id"]], ("n",))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("group_size", [1, 2])
    def test_grouped_search(self, ec_client, make_dataset, group_size):
        """target: grouped filter reuse; method: strict groups including a rare group; expected: exact members."""
        rows = make_rows(129, profile="minimal")
        for row in rows:
            i = row["id"]
            excluded_group = i % 11 == 3
            row["n"] = None if i % 7 == 2 else (-1 if i % 3 == 1 or excluded_group else 1)
            row["group"] = 99 if i == 0 else (98 if excluded_group else i % 4)
            row["vec"] = [float(row["id"])] + [0.0] * 7
        data = make_dataset(rows=rows, profile="minimal")
        case = FilterCase("positive", "n > 0", lambda r: r["n"] is not None and r["n"] > 0)
        matching = case.expected(rows)
        assert 0 < len(matching) < len(rows)
        assert {r["group"] for r in matching} == {0, 1, 2, 3, 99}
        # Groups rank by their best hit; hits within each group rank by L2.
        # The fixture has unique distances (id + 1)^2, including the rare group.
        expected = {}
        for row in sorted(matching, key=lambda r: (r["id"] + 1) ** 2):
            expected.setdefault(row["group"], [])
            if len(expected[row["group"]]) < group_size:
                expected[row["group"]].append(row["id"])
        for _ in range(REPEATS):
            hits = ec_client.search(
                data.name,
                data=[[-1.0] + [0.0] * 7],
                anns_field="vec",
                filter="n > 0",
                search_params={"metric_type": "L2", "params": {}},
                limit=5,
                group_by_field="group",
                group_size=group_size,
                strict_group_size=True,
                output_fields=["n", "group"],
                consistency_level="Strong",
                timeout=TIMEOUT,
            )[0]
            assert len({h["id"] for h in hits}) == len(hits)
            grouped = {}
            for hit in hits:
                row = rows[hit["id"]]
                assert case.predicate(row), f"grouped search returned excluded row {hit['id']}"
                assert_fields(hit["entity"], row, ("n", "group"))
                np.testing.assert_allclose(hit["distance"], (row["id"] + 1) ** 2, rtol=1e-5, atol=1e-6)
                grouped.setdefault(row["group"], []).append(row["id"])
            assert grouped == expected
            assert list(grouped) == list(expected), "groups are not ranked by their nearest hit"
            assert [h["id"] for h in hits] == [i for ids in expected.values() for i in ids], (
                "group members must be contiguous and ordered by distance"
            )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index_type", [None, "STL_SORT"], ids=["raw", "sorted"])
    def test_timestamp_signatures(self, ec_client, make_dataset, index_type):
        """target: timestamp/interval identity; method: alternate instants/intervals; expected: explicit dates."""
        data = make_dataset(profile="timestamp", extra_indexes=() if index_type is None else (("ts", index_type),))
        instant = datetime(2026, 1, 1, tzinfo=UTC)

        def parsed(row):
            return datetime.fromisoformat(row["ts"].replace("Z", "+00:00")) if row["ts"] is not None else None

        a = FilterCase(
            "utc", "ts >= ISO '2026-01-01T00:00:00Z'", lambda r: parsed(r) is not None and parsed(r) >= instant
        )
        b = FilterCase(
            "later", "ts > ISO '2026-01-01T00:00:00Z'", lambda r: parsed(r) is not None and parsed(r) > instant
        )
        self.alternate(ec_client, data, a, b)
        cases = [
            FilterCase("null", "ts is null", lambda r: r["ts"] is None),
            FilterCase(
                "not_bound",
                "not (ts >= ISO '2026-01-01T00:00:00Z')",
                lambda r: None if parsed(r) is None else parsed(r) < instant,
            ),
            FilterCase("same_instant", "ts >= ISO '2026-01-01T08:00:00+08:00'", a.predicate),
            FilterCase(
                "plus_day",
                "ts + INTERVAL 'P1D' > ISO '2026-01-01T00:00:00Z'",
                lambda r: parsed(r) is not None and parsed(r) + timedelta(days=1) > instant,
            ),
            FilterCase(
                "minus_day",
                "ts - INTERVAL 'P1D' >= ISO '2026-01-01T00:00:00Z'",
                lambda r: parsed(r) is not None and parsed(r) - timedelta(days=1) >= instant,
            ),
            # Milvus normalizes calendar fields (Jan 31 + one month = Mar 3),
            # rather than clamping to the final day of February.
            FilterCase(
                "month_boundary",
                "ts + INTERVAL 'P1M' == ISO '2026-03-03T00:00:00Z'",
                lambda r: r["ts"] == "2026-01-31T00:00:00Z",
            ),
            FilterCase(
                "year_boundary",
                "ts + INTERVAL 'P1Y' == ISO '2027-12-31T00:00:00Z'",
                lambda r: r["ts"] == "2026-12-31T00:00:00Z",
            ),
        ]
        for case in cases:
            self.repeat_case(ec_client, data, case)

        for unit, cutoff, matched in [
            ("D", "2026-01-02T00:00:00Z", ["2026-01-01T00:00:00Z", "2025-12-31T00:00:00Z"]),
            ("M", "2026-03-03T00:00:00Z", ["2026-01-31T00:00:00Z", "2025-12-31T00:00:00Z"]),
            ("Y", "2027-12-31T00:00:00Z", ["2026-12-31T00:00:00Z", "2025-12-31T00:00:00Z"]),
        ]:
            intervals = [
                FilterCase(
                    f"interval_{amount}{unit}",
                    f"ts + INTERVAL 'P{amount}{unit}' == ISO '{cutoff}'",
                    lambda r, expected=expected: expected is not None and r["ts"] == expected,
                )
                for amount, expected in enumerate(matched, 1)
            ]
            self.alternate(ec_client, data, *intervals)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_reload_and_recreate(self, ec_client, make_dataset):
        """target: lifecycle identity; method: reload/recreate same name; expected: new data/schema."""
        data = make_dataset(profile="minimal")
        case = FilterCase("positive", "n > 0", lambda r: r["n"] is not None and r["n"] > 0)
        self.repeat_case(ec_client, data, case)
        self.release_collection(ec_client, data.name)
        self.load_collection(ec_client, data.name)
        self.repeat_case(ec_client, data, case)
        self.drop_collection(ec_client, data.name)
        new_rows = make_rows(profile="minimal")
        for row in new_rows:
            row["n"] = -1 if row["n"] is None else -row["n"]
        data = make_dataset(rows=new_rows, name=data.name, profile="minimal")
        self.repeat_case(ec_client, data, case)
        # Recreate the same *referenced* field with another type. A positive
        # numeric filter on "changed" must now fail instead of using old state.
        name = data.name
        self.drop_collection(ec_client, name)
        schema = self.create_schema(ec_client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)
        schema.add_field("changed", DataType.DOUBLE)
        self.create_collection(ec_client, name, schema=schema, force_teardown=False, consistency_level="Strong")
        params = self.prepare_index_params(ec_client)[0]
        params.add_index("vec", index_type="FLAT", metric_type="L2")
        self.create_index(ec_client, name, index_params=params)
        self.insert(ec_client, name, data=[{"id": 1, "vec": vector_for_id(1), "changed": 7.0}])
        self.flush(ec_client, name)
        self.load_collection(ec_client, name)
        for _ in range(REPEATS):
            assert self.query(ec_client, name, filter="changed > 0", output_fields=["id"])[0] == [{"id": 1}]
        self.drop_collection(ec_client, name)
        rows = make_rows(1, profile="minimal")
        rows[0]["changed"] = "seven"
        data = make_dataset(rows=rows, name=name, profile="retyped")
        for _ in range(REPEATS):
            with pytest.raises(MilvusException, match="(?i)(type|cast|compar)") as error:
                ec_client.query(name, filter="changed > 0", output_fields=["id"], timeout=TIMEOUT)
            assert error.value.code == 1100
            self.verify_query(ec_client, data, FilterCase("new_string", 'changed == "seven"', lambda r: True))

    @pytest.mark.tags(CaseLabel.L2)
    def test_compaction_replaces_segments(self, ec_client, make_dataset):
        """target: replacement segment cache identity; method: compact warmed segments; expected: survivors."""
        data = make_dataset(
            rows=[], profile="minimal", properties={"collection.autocompaction.enabled": "false"}, defer_load=True
        )
        for batch in range(3):
            rows = make_rows(ROWS, start=batch * ROWS, profile="minimal")
            self.insert(ec_client, data.name, data=rows)
            self.flush(ec_client, data.name)
            data.rows.extend(rows)
        self.load_collection(ec_client, data.name)
        segments = self.wait_for_compaction_eligible_segments(
            ec_client, data.name, minimum_segment_count=3, timeout=TIMEOUT
        )
        source_ids = {s.segment_id for s in segments}
        case = FilterCase("compaction", "n > 0", lambda r: r["n"] is not None and r["n"] > 0)
        self.repeat_case(ec_client, data, case)
        removed = {r["id"] for r in data.rows if r["id"] % 5 == 0}
        self.delete(ec_client, data.name, ids=sorted(removed))
        data.rows = [r for r in data.rows if r["id"] not in removed]
        self.flush(ec_client, data.name)
        self.repeat_case(ec_client, data, case)
        job = self.compact(ec_client, data.name)[0]
        assert job != -1, "compaction prerequisite unmet: server scheduled no work"
        self.wait_for_compaction_ready(ec_client, job, timeout=240)
        plans = ec_client.get_compaction_plans(job, timeout=TIMEOUT).plans
        assert plans, "compaction completed without rewriting a segment"
        sources = {s for p in plans for s in p.sources}
        targets = {p.target for p in plans}
        assert sources and sources <= source_ids, f"unexpected compaction sources: {sources}, before={source_ids}"
        assert targets and targets.isdisjoint(source_ids), f"compaction did not replace segments: {plans}"
        expected_ids = (source_ids - sources) | targets
        self.verify_compaction_transition(ec_client, data, case, expected_ids, sources)
        # Additional physical replacement check; normal-transition assertions
        # above must pass before this explicit cache-clearing lifecycle action.
        reload_compaction_outputs(ec_client, data.name, expected_ids, sources)
        self.repeat_case(ec_client, data, case)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "expr,params,message",
        [
            pytest.param("n >", {}, "(?i)(parse|syntax|invalid)", id="syntax"),
            pytest.param("0 < n < 0", {}, "invalid range", id="empty_exclusive_range"),
            pytest.param("n == {missing}", {}, "(?i)(template|missing)", id="missing_template"),
            pytest.param('n > "wrong"', {}, "(?i)(type|cast|compar)", id="operand_type"),
            pytest.param(
                'TEXT_MATCH_FUZZY(text, "milvus", max_edit_distance=3)', {}, "max_edit_distance", id="fuzzy_distance"
            ),
            pytest.param('PHRASE_MATCH(text, "vector database", -1)', {}, "slop", id="negative_slop"),
            pytest.param("n / {divisor} > 0", {"divisor": 0}, "by zero", id="division_by_zero"),
            pytest.param("n % {divisor} == 0", {"divisor": 0}, "by zero", id="modulo_by_zero"),
        ],
    )
    def test_errors_do_not_reuse_success(self, ec_client, make_dataset, expr, params, message):
        """target: errors are not hidden by cache; method: valid/error alternation; expected: error then recovery."""
        data = make_dataset(profile="text")
        good = FilterCase(
            "valid_arithmetic",
            "n / {divisor} > 0",
            lambda r: r["n"] is not None and int(r["n"] / 2) > 0,
            {"divisor": 2},
        )
        for _ in range(REPEATS):
            self.verify_query(ec_client, data, good)
            self.verify_search(ec_client, data, good)
            for api in ("query", "search"):
                with pytest.raises(MilvusException, match=message) as error:
                    kwargs = dict(filter=expr, filter_params=params, timeout=TIMEOUT, consistency_level="Strong")
                    if api == "query":
                        ec_client.query(data.name, output_fields=["id"], **kwargs)
                    else:
                        ec_client.search(
                            data.name,
                            data=[vector_for_id(23)],
                            anns_field="vec",
                            search_params={"metric_type": "L2", "params": {}},
                            **kwargs,
                        )
                # Proxy WrapPlanCreationError deliberately projects parse/fill
                # errors to ParameterInvalid (1100) for both Query and Search.
                # Assert that public contract, not a code learned from a reply.
                assert error.value.code == 1100
        self.repeat_case(ec_client, data, good)


class TestExprCacheSealedAndGrowing(ExprCacheTestBase):
    """When to add tests here: the same predicates across segment transitions.

    Fixture: make_dataset, with no flush before the initial growing checkpoint.
    No xdist_group: each invocation owns its collection and all transitions,
    so loadgroup can schedule separate invocations on different workers.
    """

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("initial_count", [63, BATCH - 1], ids=["word_boundary", "batch_boundary"])
    def test_growing_append_across_bitmap_boundaries(self, ec_client, make_dataset, initial_count):
        """target: growing snapshot lengths; method: warm and append across boundaries; expected: new tail matches."""
        rows = make_rows(initial_count, profile="minimal")
        for row in rows:
            row["n"] = None if row["id"] % 7 == 0 else -1
            row["vec"] = [row["id"] / 64.0] + [0.0] * 7
        rows[-1]["n"] = 1
        data = make_dataset(rows=rows, profile="minimal", sealed=False)
        positive = FilterCase("positive", "n + 1 > 1", lambda r: None if r["n"] is None else r["n"] + 1 > 1)
        negative = FilterCase("negative", f"not ({positive.expr})", lambda r: tri_not(positive.predicate(r)))
        q = [(initial_count + 1) / 64.0] + [0.0] * 7
        for step in range(3):
            if step:
                added = make_rows(1, start=len(data.rows), profile="minimal")
                added[0]["n"] = 1
                added[0]["vec"] = [added[0]["id"] / 64.0] + [0.0] * 7
                self.insert(ec_client, data.name, data=added)
                data.rows.extend(added)
                wait_for_segment_mode(ec_client, data.name, "growing", len(data.rows), timeout=TIMEOUT)
            # Every append must add a match and replace the nearest result.
            assert len(positive.expected(data.rows)) == step + 1
            assert ranked_ids(positive.expected(data.rows), q, 1) == [len(data.rows) - 1]
            for _ in range(REPEATS):
                for case in (positive, negative):
                    self.verify_query(ec_client, data, case)
                    self.verify_search(ec_client, data, case, vectors=[q], limit=3)

    @pytest.mark.tags(CaseLabel.L1)
    def test_growing_text_publication_without_writes(self, ec_client, make_dataset, request):
        """target: text publication at unchanged rows; method: sample/wait/reread without writes; expected: full corpus."""
        data = make_dataset(profile="text", sealed=False)
        cases = [
            FilterCase("text", 'TEXT_MATCH(text, "vector")', lambda r: r["text"] in TEXTS[:4]),
            FilterCase("phrase", 'PHRASE_MATCH(text, "vector database", 1)', lambda r: r["text"] in TEXTS[:2]),
            FilterCase(
                "fuzzy", 'TEXT_MATCH_FUZZY(text, "milvus", max_edit_distance=1)', lambda r: r["text"] in TEXTS[5:7]
            ),
        ]
        initial_counts = {}
        partial = []
        for case in cases:
            expected = {r["id"]: r for r in case.expected(data.rows)}
            initial = ec_client.query(
                data.name, filter=case.expr, output_fields=["id", "text"], consistency_level="Strong", timeout=TIMEOUT
            )
            ids = [r["id"] for r in initial]
            # Before reader publication, missing matches are allowed; false
            # positives, duplicate IDs and incorrect values never are.
            assert len(ids) == len(set(ids)) and set(ids) <= set(expected)
            for row in initial:
                assert_fields(row, expected[row["id"]], ("id", "text"))
            initial_counts[case.name] = len(ids)
            if len(ids) < len(expected):
                partial.append(case.name)
        # Publication timing is uncontrolled. Report when the initial reader
        # was already complete rather than claiming an observed transition.
        request.node.user_properties.extend(
            [
                ("text_initial_match_counts", json.dumps(initial_counts, sort_keys=True)),
                ("text_publication_transition_observed", ",".join(partial) or "none; reader already complete"),
            ]
        )
        wait_for_text_rows(ec_client, data)
        wait_for_segment_mode(ec_client, data.name, "growing", len(data.rows), timeout=TIMEOUT)
        for case in cases:
            self.repeat_case(ec_client, data, case)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("profile", ["raw", "text"])
    def test_growing_append_flush_and_mixed(self, ec_client, make_dataset, profile):
        """target: row snapshots/text visibility; method: append/flush/append; expected: current complete rows."""
        data = make_dataset(profile=profile, sealed=False)
        if profile == "text":
            cases = [
                FilterCase("text", 'TEXT_MATCH(text, "vector")', lambda r: r["text"] in TEXTS[:4]),
                FilterCase("phrase_0", 'PHRASE_MATCH(text, "vector database", 0)', lambda r: r["text"] == TEXTS[0]),
                FilterCase("phrase_2", 'PHRASE_MATCH(text, "vector database", 2)', lambda r: r["text"] in TEXTS[:3]),
                FilterCase(
                    "fuzzy_0", 'TEXT_MATCH_FUZZY(text, "milvus", max_edit_distance=0)', lambda r: r["text"] == TEXTS[5]
                ),
                FilterCase(
                    "fuzzy_2",
                    'TEXT_MATCH_FUZZY(text, "milvus", max_edit_distance=2)',
                    lambda r: r["text"] in TEXTS[5:8],
                ),
            ]
        else:
            cases = [
                FilterCase("scalar", "n > 0", lambda r: r["n"] is not None and r["n"] > 0),
                json_cases()[0],
                next(c for c in array_cases() if c.name == "contains"),
            ]

        def checkpoint():
            if profile == "text":
                wait_for_text_rows(ec_client, data)
            for case in cases:
                self.repeat_case(ec_client, data, case, filter_params={"expr_use_json_stats": False})

        checkpoint()
        added = make_rows(129, start=len(data.rows), profile=profile)
        self.insert(ec_client, data.name, data=added)
        data.rows.extend(added)
        wait_for_segment_mode(ec_client, data.name, "growing", len(data.rows), timeout=TIMEOUT)
        checkpoint()
        self.flush(ec_client, data.name)
        wait_for_segment_mode(ec_client, data.name, "sealed", len(data.rows), timeout=TIMEOUT)
        checkpoint()
        sealed_rows = len(data.rows)
        added = make_rows(65, start=sealed_rows, profile=profile)
        self.insert(ec_client, data.name, data=added)
        data.rows.extend(added)
        wait_for_segment_mode(
            ec_client, data.name, "mixed", len(data.rows), expected_sealed_rows=sealed_rows, timeout=TIMEOUT
        )
        checkpoint()
