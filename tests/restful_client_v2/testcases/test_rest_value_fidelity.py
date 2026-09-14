import json
import time

import pytest
import requests
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

# Verify PR #52261: the REST layer no longer rewrites the values a caller sends.
# Covers value fidelity (write == read == filter hit), rejection cases, exprParams,
# partial update, id lists, vector types, and gRPC/REST consistency.
#
# Tags:
#   L0 - positive value-fidelity paths (fast, run every time)
#   L1 - negative / rejection paths
#   L2 - special vector types (BinaryVector / FP16 / BF16 / Sparse)
#
# Config-switch behaviors (proxy.http.compatibilityMode, toggling
# proxy.http.nativeJSONResponse, proxy.http.maxExprParamsDepth) and legacy
# non-document JSON bytes are covered by a separate upgrade-compatibility test,
# not here, because they require changing runtime configuration on a live server.

VECTOR = [0.1, 0.2]


class TestRestValueFidelity(TestBase):
    # ---- shared collections (class-scoped, built once) ----

    _scalar_coll = None  # Int64 PK, all scalar types + JSON + dynamic + FloatVector
    _varchar_coll = None  # VarChar PK, same scalar schema
    _array_coll = None  # Array(Bool) field
    _next_pk = 0

    @classmethod
    def _new_pk(cls):
        cls._next_pk += 1
        return cls._next_pk

    def _rest(self, endpoint, token, path, payload=None, raw=None):
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        if raw is not None:
            return requests.post(f"{endpoint}{path}", headers=headers, data=raw).json()
        return requests.post(f"{endpoint}{path}", headers=headers, data=json.dumps(payload)).json()

    @pytest.fixture(scope="class", autouse=True)
    def _shared_collections(self, endpoint, token):
        def build(fields, index_params=None):
            name = gen_collection_name("rvf")
            payload = {
                "collectionName": name,
                "schema": {"autoId": False, "enableDynamicField": True, "fields": fields},
            }
            if index_params is None:
                vec = next((f for f in fields if "Vector" in f["dataType"]), None)
                if vec is not None:
                    payload["indexParams"] = [
                        {"fieldName": vec["fieldName"], "indexName": vec["fieldName"], "metricType": "L2"}
                    ]
            else:
                payload["indexParams"] = index_params
            rsp = self._rest(endpoint, token, "/v2/vectordb/collections/create", payload=payload)
            assert rsp["code"] == 0, f"create failed: {rsp}"
            self._rest(endpoint, token, "/v2/vectordb/collections/load", payload={"collectionName": name})
            return name

        def wait_loaded(name):
            t0 = time.time()
            while time.time() - t0 < 60:
                rsp = self._rest(endpoint, token, "/v2/vectordb/collections/describe", payload={"collectionName": name})
                if rsp.get("data", {}).get("load") == "LoadStateLoaded":
                    return
                time.sleep(2)

        # Class-scoped and function-scoped fixtures run on different class
        # instances, so store the shared collection names on the class, not on
        # self, so every test method can read them.
        cls = type(self)
        cls._next_pk = 0
        scalar = build(self._scalar_fields())
        varchar = build(self._scalar_fields(pk="VarChar"))
        array = build(
            [
                {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                {
                    "fieldName": "arr",
                    "dataType": "Array",
                    "elementDataType": "Bool",
                    "elementTypeParams": {"max_capacity": "10"},
                },
                {"fieldName": "vec", "dataType": "FloatVector", "elementTypeParams": {"dim": "2"}},
            ]
        )
        cls._scalar_coll = scalar
        cls._varchar_coll = varchar
        cls._array_coll = array
        for c in (scalar, varchar, array):
            wait_loaded(c)

        yield

        for c in (scalar, varchar, array):
            try:
                self._rest(endpoint, token, "/v2/vectordb/collections/drop", payload={"collectionName": c})
            except Exception:
                pass

    # ---- helpers (function-scoped, use self.vector_client / collection_client) ----

    def _headers(self):
        return {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

    def _raw_post(self, path, raw_body):
        return requests.post(f"{self.endpoint}{path}", headers=self._headers(), data=raw_body).json()

    def _raw_insert(self, coll, data_json):
        raw = '{"collectionName":"%s","data":%s}' % (coll, data_json)
        return self._raw_post("/v2/vectordb/entities/insert", raw)

    def _insert(self, coll, rows):
        rsp = self.vector_client.vector_insert({"collectionName": coll, "data": rows})
        assert rsp["code"] == 0, f"insert failed: {rsp}"
        self.collection_client.flush(coll)
        time.sleep(1)
        return rsp

    def _query(self, coll, filter_expr, output_fields=None, limit=10):
        payload = {"collectionName": coll, "filter": filter_expr, "limit": limit}
        if output_fields:
            payload["outputFields"] = output_fields
        return self.vector_client.vector_query(payload)

    def _get(self, coll, ids, timeout=30):
        # vector_get has no empty-result retry, so wait here until the flushed
        # (sealed) data is loaded on the query node, which can lag in CI.
        t0 = time.time()
        rsp = {}
        while time.time() - t0 < timeout:
            rsp = self.vector_client.vector_get({"collectionName": coll, "id": ids})
            if rsp.get("data"):
                return rsp
            time.sleep(1)
        return rsp

    def _create_load(self, fields, index_params=None, enable_dynamic=True):
        name = gen_collection_name("rvf")
        payload = {
            "collectionName": name,
            "schema": {"autoId": False, "enableDynamicField": enable_dynamic, "fields": fields},
        }
        if index_params is None:
            vec = next((f for f in fields if "Vector" in f["dataType"]), None)
            if vec is not None:
                payload["indexParams"] = [
                    {"fieldName": vec["fieldName"], "indexName": vec["fieldName"], "metricType": "L2"}
                ]
        else:
            payload["indexParams"] = index_params
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, f"create failed: {rsp}"
        self.collection_client.collection_load(collection_name=name)
        self.wait_collection_load_completed(name)
        return name

    def _scalar_fields(self, pk="Int64"):
        return [
            {
                "fieldName": "id",
                "dataType": pk,
                "isPrimary": True,
                "elementTypeParams": {"max_length": "256"} if pk == "VarChar" else {},
            },
            {"fieldName": "i8", "dataType": "Int8", "elementTypeParams": {}},
            {"fieldName": "i16", "dataType": "Int16", "elementTypeParams": {}},
            {"fieldName": "i32", "dataType": "Int32", "elementTypeParams": {}},
            {"fieldName": "i64", "dataType": "Int64", "elementTypeParams": {}},
            {"fieldName": "f32", "dataType": "Float", "elementTypeParams": {}},
            {"fieldName": "f64", "dataType": "Double", "elementTypeParams": {}},
            {"fieldName": "b", "dataType": "Bool", "elementTypeParams": {}},
            {"fieldName": "s", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
            {"fieldName": "j", "dataType": "JSON", "elementTypeParams": {}},
            {"fieldName": "vec", "dataType": "FloatVector", "elementTypeParams": {"dim": "2"}},
        ]

    def _row(self, pk, **extra):
        row = {
            "id": pk,
            "i8": 1,
            "i16": 1,
            "i32": 1,
            "i64": 1,
            "f32": 1.0,
            "f64": 1.0,
            "b": True,
            "s": "x",
            "j": {"k": 1},
            "vec": VECTOR,
        }
        row.update(extra)
        return row

    # ================= L0: A. Value fidelity =================

    @pytest.mark.tags(CaseLabel.L0)
    def test_int8_int16_int32_precision(self):
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, i8=42, i16=1234, i32=123456)])
        d = self._query(self._scalar_coll, f"id == {pk}", ["i8", "i16", "i32"])["data"][0]
        assert d["i8"] == 42 and d["i16"] == 1234 and d["i32"] == 123456

    @pytest.mark.tags(CaseLabel.L0)
    def test_int64_bigint_roundtrip(self):
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, i64=9007199254740993)])
        d = self._query(self._scalar_coll, f"id == {pk}", ["i64"])["data"][0]
        assert d["i64"] == 9007199254740993
        # filter on the exact value hits; the adjacent rounded value does not
        hit = self.vector_client.vector_query(
            {
                "collectionName": self._scalar_coll,
                "filter": f"id == {pk} && i64 == {{v}}",
                "exprParams": {"v": 9007199254740993},
                "limit": 10,
            }
        )
        assert hit["code"] == 0 and len(hit["data"]) == 1
        miss = self.vector_client.vector_query(
            {
                "collectionName": self._scalar_coll,
                "filter": f"id == {pk} && i64 == {{v}}",
                "exprParams": {"v": 9007199254740992},
                "limit": 10,
            }
        )
        assert len(miss["data"]) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_int64_scientific_integer_forms(self):
        # 1e3 is an integer value and must be accepted exactly.
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":1e3,"f32":1,"f64":1,"b":true,"s":"x","j":{"k":1},"vec":[0.1,0.2]}]'
            % pk,
        )
        assert rsp["code"] == 0, rsp
        self.collection_client.flush(self._scalar_coll)
        time.sleep(1)
        d = self._query(self._scalar_coll, f"id == {pk}", ["i64"])["data"][0]
        assert d["i64"] == 1000

    @pytest.mark.tags(CaseLabel.L0)
    def test_int64_decimal_that_is_integer(self):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":9007199254740993.0,"f32":1,"f64":1,"b":true,"s":"x","j":{"k":1},"vec":[0.1,0.2]}]'
            % pk,
        )
        assert rsp["code"] == 0, rsp
        self.collection_client.flush(self._scalar_coll)
        time.sleep(1)
        d = self._query(self._scalar_coll, f"id == {pk}", ["i64"])["data"][0]
        assert d["i64"] == 9007199254740993

    @pytest.mark.tags(CaseLabel.L0)
    def test_varchar_number_literal_kept(self):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":1,"f32":1,"f64":1,"b":true,"s":1e300,"j":{"k":1},"vec":[0.1,0.2]}]'
            % pk,
        )
        assert rsp["code"] == 0, rsp
        self.collection_client.flush(self._scalar_coll)
        time.sleep(1)
        d = self._query(self._scalar_coll, f"id == {pk}", ["s"])["data"][0]
        assert d["s"] == "1e300"

    @pytest.mark.tags(CaseLabel.L0)
    def test_varchar_decimal_literal_kept(self):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":1,"f32":1,"f64":1,"b":true,"s":1.50,"j":{"k":1},"vec":[0.1,0.2]}]'
            % pk,
        )
        assert rsp["code"] == 0, rsp
        self.collection_client.flush(self._scalar_coll)
        time.sleep(1)
        d = self._query(self._scalar_coll, f"id == {pk}", ["s"])["data"][0]
        assert d["s"] == "1.50"

    @pytest.mark.tags(CaseLabel.L0)
    def test_json_object_reads_back_native(self):
        # On master latest nativeJSONResponse defaults to true, so a JSON object
        # reads back as a native object.
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, j={"j": "hello"})])
        d = self._query(self._scalar_coll, f"id == {pk}", ["j"])["data"][0]
        assert d["j"] == {"j": "hello"}, f"got {d['j']!r}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_json_string_reads_back_as_string(self):
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, j="hello")])
        d = self._query(self._scalar_coll, f"id == {pk}", ["j"])["data"][0]
        assert d["j"] == "hello", f"got {d['j']!r}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_dynamic_field_bigint_top_level(self):
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, dyn_big=9007199254740993)])
        d = self._query(self._scalar_coll, f"id == {pk}", ["dyn_big"])["data"][0]
        assert d["dyn_big"] == 9007199254740993

    @pytest.mark.tags(CaseLabel.L0)
    def test_dynamic_field_bigint_nested(self):
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, dyn_obj={"nested": {"x": 9007199254740993}})])
        d = self._query(self._scalar_coll, f"id == {pk}", ["dyn_obj"])["data"][0]
        assert d["dyn_obj"]["nested"]["x"] == 9007199254740993

    @pytest.mark.tags(CaseLabel.L0)
    def test_dynamic_field_float_not_zero(self):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":1,"f32":1,"f64":1,"b":true,"s":"x","j":{"k":1},"dyn_f":1e19,"vec":[0.1,0.2]}]'
            % pk,
        )
        assert rsp["code"] == 0, rsp
        self.collection_client.flush(self._scalar_coll)
        time.sleep(1)
        d = self._query(self._scalar_coll, f"id == {pk}", ["dyn_f"])["data"][0]
        assert d["dyn_f"] == 1e19 and d["dyn_f"] != 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_double_precision(self):
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, f64=1.5)])
        d = self._query(self._scalar_coll, f"id == {pk}", ["f64"])["data"][0]
        assert d["f64"] == 1.5

    # ================= L0: C. exprParams (positive) =================

    @pytest.mark.tags(CaseLabel.L0)
    def test_exprparams_bigint_exact_match(self):
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, i64=9007199254740993)])
        # Scope by id so the shared collection's other rows (with the same i64
        # value) do not affect the hit count.
        hit = self.vector_client.vector_query(
            {
                "collectionName": self._scalar_coll,
                "filter": f"id == {pk} && i64 == {{v}}",
                "exprParams": {"v": 9007199254740993},
                "limit": 10,
            }
        )
        assert hit["code"] == 0 and len(hit["data"]) == 1
        miss = self.vector_client.vector_query(
            {
                "collectionName": self._scalar_coll,
                "filter": f"id == {pk} && i64 == {{v}}",
                "exprParams": {"v": 9007199254740992},
                "limit": 10,
            }
        )
        assert len(miss["data"]) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_exprparams_int64_max_ok(self):
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, i64=9223372036854775807)])
        hit = self.vector_client.vector_query(
            {
                "collectionName": self._scalar_coll,
                "filter": f"id == {pk} && i64 == {{v}}",
                "exprParams": {"v": 9223372036854775807},
                "limit": 10,
            }
        )
        assert hit["code"] == 0 and len(hit["data"]) == 1
        miss = self.vector_client.vector_query(
            {
                "collectionName": self._scalar_coll,
                "filter": f"id == {pk} && i64 == {{v}}",
                "exprParams": {"v": 9223372036854775806},
                "limit": 10,
            }
        )
        assert len(miss["data"]) == 0

    # ================= L0: P. Partial update =================

    @pytest.mark.tags(CaseLabel.L0)
    def test_partial_update_null_clears_dynamic_field(self):
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, tag="old", keep=9007199254740993)])
        rsp = self.vector_client.vector_upsert(
            {"collectionName": self._scalar_coll, "data": [{"id": pk, "tag": None}], "partialUpdate": True}
        )
        assert rsp["code"] == 0, rsp
        self.collection_client.flush(self._scalar_coll)
        time.sleep(1)
        d = self._query(self._scalar_coll, f"id == {pk}", ["tag", "keep"])["data"][0]
        assert d["tag"] is None, f"tag should be cleared, got {d.get('tag')!r}"
        assert d["keep"] == 9007199254740993, "untouched key must not be rewritten"

    @pytest.mark.tags(CaseLabel.L0)
    def test_partial_update_preserves_untouched_keys(self):
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, da=9007199254740993, db="keep")])
        rsp = self.vector_client.vector_upsert(
            {"collectionName": self._scalar_coll, "data": [{"id": pk, "dc": "new"}], "partialUpdate": True}
        )
        assert rsp["code"] == 0, rsp
        self.collection_client.flush(self._scalar_coll)
        time.sleep(1)
        d = self._query(self._scalar_coll, f"id == {pk}", ["da", "db", "dc"])["data"][0]
        assert d["da"] == 9007199254740993
        assert d["db"] == "keep" and d["dc"] == "new"

    # ================= L0: D. id list =================

    @pytest.mark.tags(CaseLabel.L0)
    def test_get_multiple_ids_exact(self):
        self._insert(self._varchar_coll, [self._row("alice"), self._row("carol")])
        rsp = self._get(self._varchar_coll, ["alice", "bob"])
        assert rsp["code"] == 0, rsp
        ids = [d["id"] for d in rsp["data"]]
        assert ids == ["alice"], f"should hit only alice, got {ids}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_get_id_with_quote(self):
        self._insert(self._varchar_coll, [self._row('a"b')])
        rsp = self._get(self._varchar_coll, ['a"b'])
        assert rsp["code"] == 0 and len(rsp["data"]) == 1, rsp

    @pytest.mark.tags(CaseLabel.L0)
    def test_varchar_pk_numeric_id(self):
        self._insert(self._varchar_coll, [self._row("1000000")])
        rsp = self._get(self._varchar_coll, [1000000])
        assert rsp["code"] == 0 and len(rsp["data"]) == 1, f"numeric id should hit, got {rsp}"

    # ================= L0: G. gRPC/REST consistency =================

    @pytest.mark.tags(CaseLabel.L0)
    def test_json_grpc_write_rest_read(self):
        from pymilvus import Collection

        pk = self._new_pk()
        col = Collection(self._scalar_coll)
        col.insert(
            [
                {
                    "id": pk,
                    "i8": 1,
                    "i16": 1,
                    "i32": 1,
                    "i64": 1,
                    "f32": 1.0,
                    "f64": 1.0,
                    "b": True,
                    "s": "x",
                    "j": {"j": "hello"},
                    "vec": VECTOR,
                }
            ]
        )
        col.flush()
        time.sleep(2)
        d = self._query(self._scalar_coll, f"id == {pk}", ["j"])["data"][0]
        assert d["j"] == {"j": "hello"}, f"REST read of gRPC-written JSON mismatch: {d['j']!r}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_dynamic_bigint_grpc_write_rest_read(self):
        from pymilvus import Collection

        pk = self._new_pk()
        col = Collection(self._scalar_coll)
        col.insert(
            [
                {
                    "id": pk,
                    "i8": 1,
                    "i16": 1,
                    "i32": 1,
                    "i64": 1,
                    "f32": 1.0,
                    "f64": 1.0,
                    "b": True,
                    "s": "x",
                    "j": {"k": 1},
                    "vec": VECTOR,
                    "dyn_big": 9007199254740993,
                }
            ]
        )
        col.flush()
        time.sleep(2)
        d = self._query(self._scalar_coll, f"id == {pk}", ["dyn_big"])["data"][0]
        assert d["dyn_big"] == 9007199254740993

    # ================= L1: B. Rejection =================

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("val", [128, -129])
    def test_int8_overflow_rejected(self, val):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":%d,"i16":1,"i32":1,"i64":1,"f32":1,"f64":1,"b":true,"s":"x","j":{"k":1},"vec":[0.1,0.2]}]'
            % (pk, val),
        )
        assert rsp["code"] != 0, f"int8={val} should be rejected: {rsp}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("val", [32768, -32769])
    def test_int16_overflow_rejected(self, val):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":%d,"i32":1,"i64":1,"f32":1,"f64":1,"b":true,"s":"x","j":{"k":1},"vec":[0.1,0.2]}]'
            % (pk, val),
        )
        assert rsp["code"] != 0, f"int16={val} should be rejected: {rsp}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("val", [2147483648, -2147483649])
    def test_int32_overflow_rejected(self, val):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":%d,"i64":1,"f32":1,"f64":1,"b":true,"s":"x","j":{"k":1},"vec":[0.1,0.2]}]'
            % (pk, val),
        )
        assert rsp["code"] != 0, f"int32={val} should be rejected: {rsp}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("val", [9223372036854775808, -9223372036854775809])
    def test_int64_overflow_rejected(self, val):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":%d,"f32":1,"f64":1,"b":true,"s":"x","j":{"k":1},"vec":[0.1,0.2]}]'
            % (pk, val),
        )
        assert rsp["code"] != 0, f"int64={val} should be rejected: {rsp}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_int64_fraction_rejected(self):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":1.5,"f32":1,"f64":1,"b":true,"s":"x","j":{"k":1},"vec":[0.1,0.2]}]'
            % pk,
        )
        assert rsp["code"] != 0, rsp

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("val", ["3.5e38", "3.4028236e38", "-3.5e38"])
    def test_float32_overflow_rejected(self, val):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":1,"f32":%s,"f64":1,"b":true,"s":"x","j":{"k":1},"vec":[0.1,0.2]}]'
            % (pk, val),
        )
        assert rsp["code"] != 0, f"f32={val} should be rejected: {rsp}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_varchar_object_rejected(self):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":1,"f32":1,"f64":1,"b":true,"s":{"a":1},"j":{"k":1},"vec":[0.1,0.2]}]'
            % pk,
        )
        assert rsp["code"] != 0, rsp

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_duplicate_key_rejected(self):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":1,"f32":1,"f64":1,"b":true,"s":"x","j":{"a":1,"a":2},"vec":[0.1,0.2]}]'
            % pk,
        )
        assert rsp["code"] != 0, rsp

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_oversized_integer_rejected(self):
        # 2^64 exceeds the 64-bit range and is rejected; uint64 max (2^64-1) is
        # within range and accepted (see the L0 positive case).
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":1,"f32":1,"f64":1,"b":true,"s":"x","j":{"big":18446744073709551616},"vec":[0.1,0.2]}]'
            % pk,
        )
        assert rsp["code"] != 0, rsp

    @pytest.mark.tags(CaseLabel.L0)
    def test_json_uint64_max_accepted(self):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":1,"f32":1,"f64":1,"b":true,"s":"x","j":{"big":18446744073709551615},"vec":[0.1,0.2]}]'
            % pk,
        )
        assert rsp["code"] == 0, rsp

    @pytest.mark.tags(CaseLabel.L1)
    def test_missing_required_field_rejected(self):
        # A missing non-nullable field without a default must be rejected (it
        # used to be stored as an empty value).
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":1,"f32":1,"f64":1,"b":true,"vec":[0.1,0.2]}]' % pk,
        )
        assert rsp["code"] != 0, f"missing required field should be rejected: {rsp}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_float_vector_string_rejected(self):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":1,"f32":1,"f64":1,"b":true,"s":"x","j":{"k":1},"vec":"[0.1,0.2]"}]'
            % pk,
        )
        assert rsp["code"] != 0, rsp

    @pytest.mark.tags(CaseLabel.L1)
    def test_leading_zero_rejected(self):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":1,"i16":1,"i32":1,"i64":010,"f32":1,"f64":1,"b":true,"s":"x","j":{"k":1},"vec":[0.1,0.2]}]'
            % pk,
        )
        assert rsp["code"] != 0, rsp

    @pytest.mark.tags(CaseLabel.L1)
    def test_array_null_element_rejected(self):
        pk = self._new_pk()
        rsp = self._raw_insert(self._array_coll, '[{"id":%d,"arr":[true,null],"vec":[0.1,0.2]}]' % pk)
        assert rsp["code"] != 0, rsp

    # ================= L1: C. exprParams (negative) =================

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("val", [9223372036854775808, 9223372036854775809, 18446744073709551615])
    def test_exprparams_beyond_int64_rejected(self, val):
        rsp = self.vector_client.vector_query(
            {"collectionName": self._scalar_coll, "filter": "i64 == {v}", "exprParams": {"v": val}, "limit": 10}
        )
        assert rsp["code"] != 0, f"exprParams v={val} should be rejected: {rsp}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("val", [None, [], {}])
    def test_exprparams_null_array_object_rejected(self, val):
        rsp = self.vector_client.vector_query(
            {"collectionName": self._scalar_coll, "filter": "i64 == {v}", "exprParams": {"v": val}, "limit": 10}
        )
        assert rsp["code"] != 0, f"exprParams v={val!r} should be rejected: {rsp}"

    # ================= L2: E. Vectors =================

    @pytest.mark.tags(CaseLabel.L2)
    def test_binary_vector_base64(self):
        name = gen_collection_name("rvf")
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "bv", "dataType": "BinaryVector", "elementTypeParams": {"dim": "8"}},
                ],
            },
            "indexParams": [
                {
                    "fieldName": "bv",
                    "indexName": "bv",
                    "metricType": "HAMMING",
                    "params": {"index_type": "BIN_IVF_FLAT", "nlist": "16"},
                }
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.collection_client.collection_load(collection_name=name)
        self.wait_collection_load_completed(name)
        raw = '{"collectionName":"%s","data":[{"id":1,"bv":"AQ=="}]}' % name
        rsp = self._raw_post("/v2/vectordb/entities/insert", raw)
        assert rsp["code"] == 0, rsp

    @pytest.mark.tags(CaseLabel.L2)
    def test_int8vector_quoted_null_rejected(self):
        # #52261 removed Int8Vector from vectorAcceptsBase64: a quoted "null"
        # used to decode to an empty vector (nil slice) and is now rejected.
        name = self._create_load(
            [
                {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                {"fieldName": "iv", "dataType": "Int8Vector", "elementTypeParams": {"dim": "2"}},
            ],
            index_params=[
                {
                    "fieldName": "iv",
                    "indexName": "iv",
                    "indexType": "HNSW",
                    "metricType": "L2",
                    "params": {"M": 8, "efConstruction": 64},
                }
            ],
        )
        rsp = self._raw_insert(name, '[{"id":1,"iv":"null"}]')
        assert rsp["code"] != 0, f"Int8Vector quoted null should be rejected: {rsp}"
        rsp = self._raw_insert(name, '[{"id":1,"iv":[1,2]}]')
        assert rsp["code"] == 0, rsp

    # ================= L0: additional coverage =================

    @pytest.mark.tags(CaseLabel.L0)
    def test_float_precision_roundtrip(self):
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, f32=1.5)])
        d = self._query(self._scalar_coll, f"id == {pk}", ["f32"])["data"][0]
        assert d["f32"] == 1.5

    @pytest.mark.tags(CaseLabel.L0)
    def test_bool_quoted_and_numeric(self):
        pk = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk, b="true")])
        d = self._query(self._scalar_coll, f"id == {pk}", ["b"])["data"][0]
        assert d["b"] is True
        pk2 = self._new_pk()
        self._insert(self._scalar_coll, [self._row(pk2, b=1)])
        d2 = self._query(self._scalar_coll, f"id == {pk2}", ["b"])["data"][0]
        assert d2["b"] is True

    @pytest.mark.tags(CaseLabel.L0)
    def test_growing_vs_sealed_consistency(self):
        pk = self._new_pk()
        rsp = self.vector_client.vector_insert(
            {"collectionName": self._scalar_coll, "data": [self._row(pk, dyn_big=9007199254740993, j={"x": 1})]}
        )
        assert rsp["code"] == 0, rsp
        # growing read (before flush): retry until the growing data reaches the
        # query node, which can take longer in a distributed deployment
        d_g = None
        deadline = time.time() + 60
        while time.time() < deadline:
            rsp = self.vector_client.vector_query(
                {
                    "collectionName": self._scalar_coll,
                    "filter": f"id == {pk}",
                    "outputFields": ["dyn_big", "j"],
                    "limit": 10,
                }
            )
            if rsp.get("data"):
                d_g = rsp["data"][0]
                break
            time.sleep(2)
        assert d_g is not None, "growing data not visible within 60s"
        # sealed read (after flush)
        self.collection_client.flush(self._scalar_coll)
        time.sleep(1)
        d_s = self._query(self._scalar_coll, f"id == {pk}", ["dyn_big", "j"])["data"][0]
        assert d_g["dyn_big"] == d_s["dyn_big"] == 9007199254740993
        assert d_g["j"] == d_s["j"] == {"x": 1}

    @pytest.mark.tags(CaseLabel.L0)
    def test_nullable_scalar_null_roundtrip(self):
        name = self._create_load(
            [
                {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                {"fieldName": "ni", "dataType": "Int64", "nullable": True, "elementTypeParams": {}},
                {
                    "fieldName": "ns",
                    "dataType": "VarChar",
                    "nullable": True,
                    "elementTypeParams": {"max_length": "256"},
                },
                {"fieldName": "vec", "dataType": "FloatVector", "elementTypeParams": {"dim": "2"}},
            ]
        )
        rsp = self.vector_client.vector_insert(
            {"collectionName": name, "data": [{"id": 1, "ni": None, "ns": None, "vec": VECTOR}]}
        )
        assert rsp["code"] == 0, rsp
        self.collection_client.flush(name)
        time.sleep(1)
        d = self._query(name, "id == 1", ["ni", "ns"])["data"][0]
        assert d.get("ni") is None and d.get("ns") is None

    @pytest.mark.tags(CaseLabel.L0)
    def test_array_other_element_types(self):
        name = self._create_load(
            [
                {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                {
                    "fieldName": "ai",
                    "dataType": "Array",
                    "elementDataType": "Int64",
                    "elementTypeParams": {"max_capacity": "10"},
                },
                {
                    "fieldName": "as",
                    "dataType": "Array",
                    "elementDataType": "VarChar",
                    "elementTypeParams": {"max_capacity": "10", "max_length": "256"},
                },
                {"fieldName": "vec", "dataType": "FloatVector", "elementTypeParams": {"dim": "2"}},
            ]
        )
        rsp = self.vector_client.vector_insert(
            {"collectionName": name, "data": [{"id": 1, "ai": [1, 2], "as": ["a", "b"], "vec": VECTOR}]}
        )
        assert rsp["code"] == 0, rsp
        self.collection_client.flush(name)
        time.sleep(1)
        d = self._query(name, "id == 1", ["ai", "as"])["data"][0]
        assert d["ai"] == [1, 2] and d["as"] == ["a", "b"]

    # ================= L1: additional rejection =================

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_null_vector_rejected(self):
        # #52261 refuses a whole-value null query vector on the search path; it
        # used to decode to an empty vector and pass the required-field check.
        rsp = self.vector_client.vector_search(
            {"collectionName": self._scalar_coll, "data": [None], "annsField": "vec", "limit": 10}
        )
        assert rsp["code"] != 0, rsp

    @pytest.mark.tags(CaseLabel.L1)
    def test_non_nullable_explicit_null_rejected(self):
        pk = self._new_pk()
        rsp = self._raw_insert(
            self._scalar_coll,
            '[{"id":%d,"i8":null,"i16":1,"i32":1,"i64":1,"f32":1,"f64":1,"b":true,"s":"x","j":{"k":1},"vec":[0.1,0.2]}]'
            % pk,
        )
        assert rsp["code"] != 0, rsp

    # ================= Struct Array sub-field value handling =================

    @pytest.mark.tags(CaseLabel.L1)
    def test_struct_array_subfield_value_handling(self):
        # Struct Array sub-fields flow through parseStructArrayRow ->
        # buildStructSubArrayScalar -> parseScalarArrayElements, the same value
        # checks as a plain Array column: big integers round-trip exactly, and
        # out-of-range or null elements are rejected.
        name = gen_collection_name("rvf")
        payload = {
            "collectionName": name,
            "schema": {
                "autoID": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "vec", "dataType": "FloatVector", "elementTypeParams": {"dim": "2"}},
                ],
                "structFields": [
                    {
                        "fieldName": "profile",
                        "typeParams": {"max_capacity": "4"},
                        "fields": [
                            {
                                "fieldName": "p_int",
                                "dataType": "Array",
                                "elementDataType": "Int64",
                                "elementTypeParams": {"max_capacity": "4"},
                            },
                            {
                                "fieldName": "p_tag",
                                "dataType": "Array",
                                "elementDataType": "VarChar",
                                "elementTypeParams": {"max_capacity": "4", "max_length": "128"},
                            },
                        ],
                    }
                ],
            },
            "indexParams": [{"fieldName": "vec", "indexName": "vec", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.collection_client.collection_load(collection_name=name)
        self.wait_collection_load_completed(name)

        # bigint sub-field element round-trips exactly
        rsp = self._raw_insert(name, '[{"id":1,"profile":[{"p_int":9007199254740993,"p_tag":"a"}],"vec":[0.1,0.2]}]')
        assert rsp["code"] == 0, rsp
        self.collection_client.flush(name)
        time.sleep(1)
        d = self._query(name, "id == 1", ["profile"])["data"][0]
        assert d["profile"] == [{"p_int": 9007199254740993, "p_tag": "a"}], f"got {d['profile']!r}"

        # out-of-range sub-field element rejected
        rsp = self._raw_insert(name, '[{"id":2,"profile":[{"p_int":9223372036854775808,"p_tag":"a"}],"vec":[0.1,0.2]}]')
        assert rsp["code"] != 0, rsp

        # null sub-field element rejected
        rsp = self._raw_insert(name, '[{"id":3,"profile":[{"p_int":null,"p_tag":"a"}],"vec":[0.1,0.2]}]')
        assert rsp["code"] != 0, rsp
