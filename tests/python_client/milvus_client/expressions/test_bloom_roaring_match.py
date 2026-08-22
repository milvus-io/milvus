"""
bloom_match / roaring_match membership filter expressions via the public pymilvus SDK.

Covers the two client-built membership expressions against a live Milvus:

- bloom_match (#51140): approximate membership, zero false negatives, strict
  JSON-path typing (single key / nested / whole-doc / dynamic field), domain
  mismatch rejection, empty blob, fpr bounds, delete rejection, INT type matrix,
  scalar-index interaction (STL_SORT / INVERTED / AUTOINDEX), growing+sealed mix.
- roaring_match (#51968): exact membership, negative values, empty bitmap,
  negation, delete (positive/negated/empty), INT type matrix, INT64 bounds,
  invalid-input rejection, scalar-index interaction, growing+sealed mix.
- struct-array sub-field rejection for both bloom_match and roaring_match.

The client-side builders (`build_bloom_filter` / `build_roaring_bitmap`) are
required by pymilvus >= 3.1.0rc83 (roaring landed in pymilvus#3764).

Performance and capacity are out of scope.
"""

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import DataType, build_bloom_filter, build_roaring_bitmap
from pymilvus.exceptions import MilvusException

pk_field = "id"
vector_field = "vector"
creator_field = "creator_id"
dim = 8
nb = 1000
domain = 50


class _MembershipBase(TestMilvusClientV2Base):
    def _build_int_collection(self, client):
        """Create an INT64 membership collection with a flat vector field, insert,
        flush, index and load. Returns the collection name."""
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(creator_field, datatype=DataType.INT64)
        schema.add_field(vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            row[creator_field] = i % domain
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field)
        self.load_collection(client, collection_name)
        return collection_name

    def _query_ids(self, client, collection_name, expr, **kwargs):
        res = self.query(client, collection_name, filter=expr, output_fields=[pk_field], **kwargs)[0]
        return sorted(r[pk_field] for r in res)

    def _build_indexed_int_collection(self, client, index_type, total_rows=2000, val_mod=domain):
        """Like _build_int_collection but builds a scalar index on creator_field
        BEFORE load. total_rows must stay >= indexCoord.segment
        .minSegmentNumRowsToEnableIndex (1024) so the scalar index is really
        built, not fake-finished."""
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(creator_field, datatype=DataType.INT64)
        schema.add_field(vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        rows = cf.gen_row_data_by_schema(nb=total_rows, schema=schema, start=0)
        for i, row in enumerate(rows):
            row[creator_field] = i % val_mod
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=creator_field, index_type=index_type)
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=creator_field)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field)
        self.load_collection(client, collection_name)
        return collection_name

    def _build_struct_array_collection(self, client):
        """Build a small struct-array collection with INT64/VARCHAR/FLOAT
        sub-fields, used to exercise parser-level rejection of bloom_match /
        roaring_match on struct-array sub-fields."""
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("doc_int", datatype=DataType.INT64)
        schema.add_field(vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)

        struct_schema = self.create_struct_field_schema(client)[0]
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("str_val", DataType.VARCHAR, max_length=128)
        struct_schema.add_field("float_val", DataType.FLOAT)
        schema.add_field(
            "structA",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=8,
        )

        self.create_collection(client, collection_name, schema=schema)

        rows = [
            {
                pk_field: i,
                "doc_int": i,
                vector_field: [float(i)] * dim,
                "structA": [{"int_val": i, "str_val": f"row_{i}", "float_val": float(i)}],
            }
            for i in range(200)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field)
        self.load_collection(client, collection_name)
        return collection_name

    def _build_custom_collection(self, client, fields, row_mutator, enable_dynamic_field=False):
        """Create + insert + flush + index + load a collection with a custom set of
        scalar fields.

        `fields` is a list of (name, DataType) tuples; the int64 primary key is
        added first and the float-vector field last. `row_mutator(i, row)` fills
        the non-default values for the i-th row. Returns the collection name.
        """
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)
        schema.add_field(pk_field, datatype=DataType.INT64, is_primary=True, auto_id=False)
        for name, datatype in fields:
            schema.add_field(name, datatype=datatype)
        schema.add_field(vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            row_mutator(i, row)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field)
        self.load_collection(client, collection_name)
        return collection_name

    def _build_nullable_int_collection(self, client):
        """Create a nullable INT64 membership collection: rows with i % 8 == 7
        carry NULL creator_id, the rest cycle 0..domain-1. Index + load."""
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(creator_field, datatype=DataType.INT64, nullable=True)
        schema.add_field(vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            row[creator_field] = None if i % 8 == 7 else i % domain
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field)
        self.load_collection(client, collection_name)
        return collection_name


class TestBloomMatch(_MembershipBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_zero_false_negatives(self):
        """bloom_match must return a superset of exact `in` (no false negatives)."""
        client = self._client()
        collection_name = self._build_int_collection(client)

        members = list(range(10))
        blob = build_bloom_filter(members, fpr=0.001)

        exact = self._query_ids(client, collection_name, f"{creator_field} in [0,1,2,3,4,5,6,7,8,9]")
        got = self._query_ids(
            client, collection_name, f"bloom_match({creator_field}, {{bf}})", filter_params={"bf": blob}
        )
        assert set(exact) <= set(got), "bloom_match dropped true members"

        # not bloom_match must never leak a true member
        not_got = self._query_ids(
            client, collection_name, f"not bloom_match({creator_field}, {{bf}})", filter_params={"bf": blob}
        )
        creators = [i % domain for i in not_got]
        assert all(c >= 10 for c in creators), "not bloom_match leaked a true member"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_int_type_matrix(self):
        """One int64 blob probes every integer width (widened to int64 before hashing)."""
        client = self._client()

        def mutate(i, row):
            v = i % domain
            row["i8"], row["i16"], row["i32"], row["i64"] = v, v, v, v

        fields = [("i8", DataType.INT8), ("i16", DataType.INT16), ("i32", DataType.INT32), ("i64", DataType.INT64)]
        collection_name = self._build_custom_collection(client, fields, mutate)

        blob = build_bloom_filter([0, 1, 2, 3, 4], fpr=0.001)
        for field in ["i8", "i16", "i32", "i64"]:
            exact = self._query_ids(client, collection_name, f"{field} in [0,1,2,3,4]")
            got = self._query_ids(client, collection_name, f"bloom_match({field}, {{bf}})", filter_params={"bf": blob})
            assert set(exact) <= set(got), f"bloom_match dropped true members on {field}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_json_path_strict_typing(self):
        """JSON path membership is strictly typed: int matches, float (5.0) and
        missing key never match an int64 member, diverging from exact `in`."""
        client = self._client()

        def mutate(i, row):
            if i % 11 == 0:
                row["meta"] = {"other": 1}  # missing key
            elif i % 3 == 0:
                row["meta"] = {"uid": float(i % 10)}  # float-encoded
            else:
                row["meta"] = {"uid": i % 10}  # int-encoded

        collection_name = self._build_custom_collection(client, [("meta", DataType.JSON)], mutate)

        blob = build_bloom_filter(list(range(10)), fpr=0.001)
        res = self.query(
            client,
            collection_name,
            filter='bloom_match(meta["uid"], {bf})',
            output_fields=[pk_field],
            filter_params={"bf": blob},
        )[0]
        ids = [r[pk_field] for r in res]
        assert ids, "bloom_match(json) returned no rows"
        for i in ids:
            assert i % 11 != 0, f"missing-key row {i} matched"
            assert i % 3 != 0, f"float-encoded row {i} matched"

        # exact `in` returns the float-encoded rows too (5.0 == 5); bloom does not.
        exact = self._query_ids(client, collection_name, 'meta["uid"] in [0,1,2,3,4,5,6,7,8,9]')
        bloom_ids = set(ids)
        diverged = [i for i in exact if i % 3 == 0 and i % 11 != 0 and i not in bloom_ids]
        assert diverged, "expected float-encoded rows in exact `in` but not bloom_match"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_domain_mismatch_rejected(self):
        """A blob built from the wrong value domain is rejected at the proxy."""
        client = self._client()
        collection_name = self._build_int_collection(client)

        # utf8-domain blob on an int64 field
        str_blob = build_bloom_filter(["0", "1", "2"], fpr=0.001)
        with pytest.raises(MilvusException) as e:
            client.query(
                collection_name,
                filter=f"bloom_match({creator_field}, {{bf}})",
                output_fields=[pk_field],
                filter_params={"bf": str_blob},
            )
        assert "value domain" in str(e.value), str(e.value)

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_rejected_in_delete(self):
        """bloom_match is approximate and must be rejected in delete expressions."""
        client = self._client()
        collection_name = self._build_int_collection(client)
        blob = build_bloom_filter([0, 1, 2], fpr=0.001)
        with pytest.raises(MilvusException) as e:
            client.delete(
                collection_name,
                filter=f"bloom_match({creator_field}, {{bf}})",
                filter_params={"bf": blob},
            )
        assert "cannot be used in delete" in str(e.value), str(e.value)

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_empty_blob(self):
        """An empty membership blob matches nothing and is not rejected."""
        client = self._client()
        collection_name = self._build_int_collection(client)
        blob = build_bloom_filter([], fpr=0.001)
        got = self._query_ids(
            client, collection_name, f"bloom_match({creator_field}, {{bf}})", filter_params={"bf": blob}
        )
        assert got == []

    def test_bloom_match_fpr_out_of_range(self):
        """The client builder rejects an out-of-range fpr."""
        with pytest.raises(Exception):
            build_bloom_filter([0, 1, 2], fpr=0.00001)
        with pytest.raises(Exception):
            build_bloom_filter([0, 1, 2], fpr=0.06)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_type", ["STL_SORT", "INVERTED"])
    def test_bloom_match_scalar_index_type_matrix(self, index_type):
        """bloom_match stays zero-false-negative when a scalar index is built on
        the field before load: STL_SORT drops the raw column, INVERTED keeps it."""
        client = self._client()
        collection_name = self._build_indexed_int_collection(client, index_type)

        members = list(range(10))
        blob = build_bloom_filter(members, fpr=0.001)
        exact = self._query_ids(client, collection_name, f"{creator_field} in [0,1,2,3,4,5,6,7,8,9]")
        got = self._query_ids(
            client, collection_name, f"bloom_match({creator_field}, {{bf}})", filter_params={"bf": blob}
        )
        assert set(exact) <= set(got), f"bloom_match dropped true members under {index_type}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("val_mod", [200, 50])
    def test_bloom_match_auto_index(self, val_mod):
        """bloom_match stays zero-false-negative under AUTOINDEX (HYBRID selects
        STLSORT for high-cardinality data, BITMAP for low-cardinality data).

        The 200/50 split assumes HYBRID's internal ~100 cardinality cutoff; if
        that changes, the test still passes but its two-path coverage silently
        degrades.
        """
        client = self._client()
        collection_name = self._build_indexed_int_collection(client, "AUTOINDEX", val_mod=val_mod)

        members = list(range(10))
        blob = build_bloom_filter(members, fpr=0.001)
        exact = self._query_ids(client, collection_name, f"{creator_field} in [0,1,2,3,4,5,6,7,8,9]")
        got = self._query_ids(
            client, collection_name, f"bloom_match({creator_field}, {{bf}})", filter_params={"bf": blob}
        )
        assert set(exact) <= set(got), f"bloom_match dropped true members under AUTOINDEX domain {val_mod}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_json_nested_and_whole_doc(self):
        """JSON-path forms beyond a single key: nested path and whole document.
        Strict typing means float-encoded values never match an int64 member."""
        client = self._client()

        def mutate(i, row):
            if i % 3 == 0:
                row["meta"] = {"a": {"b": i % 10}}  # nested int
            elif i % 3 == 1:
                row["meta"] = {"a": {"b": float(i % 10)}}  # nested float
            else:
                row["meta"] = i % 10  # bare scalar int

        collection_name = self._build_custom_collection(client, [("meta", DataType.JSON)], mutate)

        blob = build_bloom_filter(list(range(10)), fpr=0.001)

        nested = self._query_ids(
            client, collection_name, 'bloom_match(meta["a"]["b"], {bf})', filter_params={"bf": blob}
        )
        assert nested, "bloom_match(nested) returned no rows"
        for i in nested:
            assert i % 3 == 0, f"bloom_match(nested) matched a non-int row {i}"

        whole = self._query_ids(client, collection_name, "bloom_match(meta, {bf})", filter_params={"bf": blob})
        assert whole, "bloom_match(whole doc) returned no rows"
        for i in whole:
            assert i % 3 == 2, f"bloom_match(whole doc) matched a non-bare-scalar row {i}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_dynamic_field_path(self):
        """bloom_match over a dynamic field: an unknown identifier resolves to a
        $meta path, strictly typed per row value."""
        client = self._client()

        def mutate(i, row):
            row["uid"] = i % 10

        collection_name = self._build_custom_collection(client, [], mutate, enable_dynamic_field=True)

        blob = build_bloom_filter(list(range(10)), fpr=0.001)
        got = self._query_ids(client, collection_name, "bloom_match(uid, {bf})", filter_params={"bf": blob})
        assert len(got) == nb, "all ten values present, every row must match"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_growing_and_sealed_mixed(self):
        """bloom_match evaluates both sealed and growing segments in one query."""
        client = self._client()
        collection_name = self._build_int_collection(client)

        growing_n = 200
        growing = [
            {
                pk_field: nb + i,
                creator_field: 500 + i % 10,
                vector_field: [float(nb + i)] * dim,
            }
            for i in range(growing_n)
        ]
        self.insert(client, collection_name, growing)

        blob = build_bloom_filter([0, 1, 2, 3, 4, 500, 501, 502, 503, 504], fpr=0.001)
        got = self._query_ids(
            client, collection_name, f"bloom_match({creator_field}, {{bf}})", filter_params={"bf": blob}
        )
        saw_sealed = any(i < nb for i in got)
        saw_growing = any(i >= nb for i in got)
        assert saw_sealed, "bloom_match missed sealed-segment members"
        assert saw_growing, "bloom_match missed growing-segment members"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_null_rows_fold_to_false(self):
        """NULL rows never match bloom_match nor its negation (NULL folds to
        FALSE on both sides)."""
        client = self._client()
        collection_name = self._build_nullable_int_collection(client)

        blob = build_bloom_filter(list(range(domain)), fpr=0.001)
        got = self._query_ids(
            client, collection_name, f"bloom_match({creator_field}, {{bf}})", filter_params={"bf": blob}
        )
        for i in got:
            assert i % 8 != 7, f"bloom_match matched a NULL row id={i}"

        not_got = self._query_ids(
            client, collection_name, f"not bloom_match({creator_field}, {{bf}})", filter_params={"bf": blob}
        )
        for i in not_got:
            assert i % 8 != 7, f"not bloom_match matched a NULL row id={i}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_varchar_zero_false_negatives(self):
        """VARCHAR membership has no false negatives (exact `in` ⊆ bloom_match)."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("tag", datatype=DataType.VARCHAR, max_length=64)
        schema.add_field(vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            row["tag"] = f"tag{i % domain}"
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field)
        self.load_collection(client, collection_name)

        str_blob = build_bloom_filter([f"tag{v}" for v in range(5)], fpr=0.001)
        exact = self._query_ids(client, collection_name, 'tag in ["tag0","tag1","tag2","tag3","tag4"]')
        got = self._query_ids(client, collection_name, "bloom_match(tag, {bf})", filter_params={"bf": str_blob})
        assert set(exact) <= set(got), "bloom_match(varchar) dropped true members"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_malformed_blob_and_literal_rejected(self):
        """A malformed blob and a literal array argument are rejected rather than
        silently unfiltered."""
        client = self._client()
        collection_name = self._build_int_collection(client)

        with pytest.raises(MilvusException):
            client.query(
                collection_name,
                filter=f"bloom_match({creator_field}, {{bf}})",
                output_fields=[pk_field],
                filter_params={"bf": b"not-a-real-blob"},
            )
        with pytest.raises(MilvusException):
            client.query(
                collection_name,
                filter=f"bloom_match({creator_field}, [1,2,3])",
                output_fields=[pk_field],
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_false_positive_rate_sanity(self):
        """The measured false-positive count over the disjoint (non-member) rows
        is bounded and in line with the configured fpr (loose upper bound)."""
        client = self._client()
        collection_name = self._build_int_collection(client)

        blob = build_bloom_filter(list(range(10)), fpr=0.05)
        got = self._query_ids(
            client, collection_name, f"bloom_match({creator_field}, {{bf}})", filter_params={"bf": blob}
        )
        fp = sum(1 for i in got if i % domain >= 10)
        non_member_rows = (domain - 10) * nb // domain
        assert fp < non_member_rows * 3 // 10, f"false-positive count {fp} far exceeds fpr=0.05"


class TestRoaringMatch(_MembershipBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_exact_membership(self):
        """roaring_match selects exactly the requested ids — no false positives."""
        client = self._client()
        collection_name = self._build_int_collection(client)

        blob = build_roaring_bitmap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000000])
        got = self._query_ids(
            client, collection_name, f"roaring_match({creator_field}, {{rb}})", filter_params={"rb": blob}
        )
        creators = sorted({i % domain for i in got})
        assert creators == list(range(10)), f"roaring_match returned {creators}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_negative_values(self):
        """Negative ids round-trip through two's-complement; a zero-extending
        build/probe would still pass all-positive tests, so this pins negatives."""
        client = self._client()

        shift = 50
        d = 100

        def mutate(i, row):
            row[creator_field] = i % d - shift

        collection_name = self._build_custom_collection(client, [(creator_field, DataType.INT64)], mutate)

        members = [-shift, -30, -1, 0, 1, 17, 49, 1 << 20, -(1 << 20)]
        blob = build_roaring_bitmap(members)
        got = self._query_ids(
            client, collection_name, f"roaring_match({creator_field}, {{rb}})", filter_params={"rb": blob}
        )
        present = {m for m in members if -shift <= m <= d - shift - 1}
        creators = {i % d - shift for i in got}
        assert creators == present, f"roaring_match got {creators}, want {present}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_not_and_empty_bitmap(self):
        """not roaring_match is the exact complement; negating an empty bitmap
        selects every non-NULL row."""
        client = self._client()
        collection_name = self._build_int_collection(client)

        blob = build_roaring_bitmap([0, 1, 2, 3, 4])
        got = self._query_ids(
            client, collection_name, f"not roaring_match({creator_field}, {{rb}})", filter_params={"rb": blob}
        )
        creators = [i % domain for i in got]
        assert all(c >= 5 for c in creators), "not roaring_match returned a member"

        empty = build_roaring_bitmap([])
        all_rows = self._query_ids(
            client, collection_name, f"not roaring_match({creator_field}, {{rb}})", filter_params={"rb": empty}
        )
        assert len(all_rows) == nb, "negating an empty bitmap must select every non-NULL row"

    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_delete(self):
        """roaring_match is exact and allowed in delete; a positive empty set
        deletes nothing."""
        client = self._client()
        collection_name = self._build_int_collection(client)

        victims = [0, 1, 2]
        blob = build_roaring_bitmap(victims)
        res = self.delete(
            client,
            collection_name,
            filter=f"roaring_match({creator_field}, {{rb}})",
            filter_params={"rb": blob},
        )[0]
        # Fixture couples pk == row index i and creator == i % domain, so each
        # creator value appears nb // domain times; deleting len(victims) distinct
        # values removes len(victims) * nb // domain rows.
        assert res["delete_count"] == len(victims) * nb // domain

        remaining = self._query_ids(
            client, collection_name, f"roaring_match({creator_field}, {{rb}})", filter_params={"rb": blob}
        )
        assert remaining == [], "victim rows must be deleted"

        empty = build_roaring_bitmap([])
        res = self.delete(
            client,
            collection_name,
            filter=f"roaring_match({creator_field}, {{rb}})",
            filter_params={"rb": empty},
        )[0]
        assert res["delete_count"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_int_type_matrix(self):
        """One int64 bitmap probes every integer width including sign-extended
        narrow negatives."""
        client = self._client()

        shift = 50
        d = 100

        def mutate(i, row):
            v = i % d - shift
            row["i8"], row["i16"], row["i32"], row["i64"] = v, v, v, v

        fields = [("i8", DataType.INT8), ("i16", DataType.INT16), ("i32", DataType.INT32), ("i64", DataType.INT64)]
        collection_name = self._build_custom_collection(client, fields, mutate)

        members = [-shift, -30, -1, 0, 1, 17, 49]
        blob = build_roaring_bitmap(members)
        present = {m for m in members if -shift <= m <= d - shift - 1}
        for field in ["i8", "i16", "i32", "i64"]:
            got = self._query_ids(
                client, collection_name, f"roaring_match({field}, {{rb}})", filter_params={"rb": blob}
            )
            creators = {i % d - shift for i in got}
            assert creators == present, f"roaring_match got {creators} on {field}, want {present}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_int64_bounds(self):
        """INT64_MIN/MAX round-trip through the two's-complement mapping."""
        client = self._client()

        vals = [(1 << 63) - 1, -(1 << 63), -1, 42]

        def mutate(i, row):
            row[creator_field] = vals[i % len(vals)]

        collection_name = self._build_custom_collection(client, [(creator_field, DataType.INT64)], mutate)

        blob = build_roaring_bitmap(vals)
        got = self._query_ids(
            client, collection_name, f"roaring_match({creator_field}, {{rb}})", filter_params={"rb": blob}
        )
        assert len(got) == nb, "all four values present, every row must match"

        absent = build_roaring_bitmap([(1 << 63) - 2])
        got = self._query_ids(
            client, collection_name, f"roaring_match({creator_field}, {{rb}})", filter_params={"rb": absent}
        )
        assert got == []

    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_invalid_input_rejected(self):
        """Literal-list and malformed-blob arguments are rejected."""
        client = self._client()
        collection_name = self._build_int_collection(client)

        with pytest.raises(MilvusException):
            client.query(
                collection_name,
                filter=f"roaring_match({creator_field}, [1,2,3])",
                output_fields=[pk_field],
            )
        with pytest.raises(MilvusException):
            client.query(
                collection_name,
                filter=f"roaring_match({creator_field}, {{rb}})",
                output_fields=[pk_field],
                filter_params={"rb": b"not-an-mrb1-blob"},
            )

        valid = build_roaring_bitmap([1, 2, 3])
        with pytest.raises(MilvusException):
            client.query(
                collection_name,
                filter=f"roaring_match({creator_field}, {{rb}})",
                output_fields=[pk_field],
                filter_params={"rb": valid[:-1]},
            )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_type", ["STL_SORT", "INVERTED"])
    def test_roaring_match_scalar_index_type_matrix(self, index_type):
        """roaring_match stays exact when a scalar index is built on the field
        before load: STL_SORT drops the raw column, INVERTED keeps it."""
        client = self._client()
        collection_name = self._build_indexed_int_collection(client, index_type)

        blob = build_roaring_bitmap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000000])
        got = self._query_ids(
            client, collection_name, f"roaring_match({creator_field}, {{rb}})", filter_params={"rb": blob}
        )
        creators = sorted({i % domain for i in got})
        assert creators == list(range(10)), f"roaring_match returned {creators} under {index_type}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("val_mod", [200, 50])
    def test_roaring_match_auto_index(self, val_mod):
        """roaring_match stays exact under AUTOINDEX (HYBRID selects STLSORT for
        high-cardinality data, BITMAP for low-cardinality data).

        The 200/50 split assumes HYBRID's internal ~100 cardinality cutoff; if
        that changes, the test still passes but its two-path coverage silently
        degrades.
        """
        client = self._client()
        collection_name = self._build_indexed_int_collection(client, "AUTOINDEX", val_mod=val_mod)

        blob = build_roaring_bitmap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000000])
        got = self._query_ids(
            client, collection_name, f"roaring_match({creator_field}, {{rb}})", filter_params={"rb": blob}
        )
        creators = sorted({i % val_mod for i in got})
        assert creators == list(range(10)), f"roaring_match returned {creators} under AUTOINDEX domain {val_mod}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_delete_negated(self):
        """`not roaring_match` in delete removes everything outside the set."""
        client = self._client()
        collection_name = self._build_int_collection(client)

        keep = [0, 1, 2]
        blob = build_roaring_bitmap(keep)
        res = self.delete(
            client,
            collection_name,
            filter=f"not roaring_match({creator_field}, {{rb}})",
            filter_params={"rb": blob},
        )[0]
        # Negating the set deletes every creator value except len(keep), i.e.
        # (domain - len(keep)) values; each value spans nb // domain rows.
        assert res["delete_count"] == (domain - len(keep)) * nb // domain

        remaining = self._query_ids(
            client, collection_name, f"roaring_match({creator_field}, {{rb}})", filter_params={"rb": blob}
        )
        assert sorted({i % domain for i in remaining}) == keep, "kept rows must be exactly the set"

    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_growing_and_sealed_mixed(self):
        """roaring_match evaluates both sealed and growing segments exactly."""
        client = self._client()
        collection_name = self._build_int_collection(client)

        growing_n = 200
        growing = [
            {
                pk_field: nb + i,
                creator_field: 500 + i % 10,
                vector_field: [float(nb + i)] * dim,
            }
            for i in range(growing_n)
        ]
        self.insert(client, collection_name, growing)

        blob = build_roaring_bitmap([0, 1, 2, 3, 4, 500, 501, 502, 503, 504])
        got = self._query_ids(
            client, collection_name, f"roaring_match({creator_field}, {{rb}})", filter_params={"rb": blob}
        )
        saw_sealed = any(i < nb for i in got)
        saw_growing = any(i >= nb for i in got)
        assert saw_sealed, "roaring_match missed sealed-segment members"
        assert saw_growing, "roaring_match missed growing-segment members"

    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_null_rows_fold_to_false(self):
        """NULL rows never match roaring_match nor its negation."""
        client = self._client()
        collection_name = self._build_nullable_int_collection(client)

        blob = build_roaring_bitmap(list(range(domain)))
        got = self._query_ids(
            client, collection_name, f"roaring_match({creator_field}, {{rb}})", filter_params={"rb": blob}
        )
        for i in got:
            assert i % 8 != 7, f"roaring_match matched a NULL row id={i}"

        not_got = self._query_ids(
            client, collection_name, f"not roaring_match({creator_field}, {{rb}})", filter_params={"rb": blob}
        )
        for i in not_got:
            assert i % 8 != 7, f"not roaring_match matched a NULL row id={i}"


class TestMembershipStructArrayRejected(_MembershipBase):
    """bloom_match / roaring_match must be rejected on struct-array sub-fields."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_membership_filter_struct_subfield_rejected(self):
        client = self._client()
        collection_name = self._build_struct_array_collection(client)

        bloom_blob = build_bloom_filter([0, 1, 2], fpr=0.001)
        roaring_blob = build_roaring_bitmap([0, 1, 2])
        str_blob = build_bloom_filter(["a", "b"], fpr=0.001)

        with pytest.raises(MilvusException) as e:
            client.query(
                collection_name,
                filter="bloom_match(structA[int_val], {bf})",
                output_fields=[pk_field],
                filter_params={"bf": bloom_blob},
            )
        assert "only supports" in str(e.value), str(e.value)

        with pytest.raises(MilvusException) as e:
            client.query(
                collection_name,
                filter="roaring_match(structA[int_val], {rb})",
                output_fields=[pk_field],
                filter_params={"rb": roaring_blob},
            )
        assert "only supports" in str(e.value), str(e.value)

        with pytest.raises(MilvusException) as e:
            client.query(
                collection_name,
                filter="bloom_match(structA[str_val], {bf})",
                output_fields=[pk_field],
                filter_params={"bf": str_blob},
            )
        assert "only supports" in str(e.value), str(e.value)

        with pytest.raises(MilvusException) as e:
            client.query(
                collection_name,
                filter="bloom_match(structA[float_val], {bf})",
                output_fields=[pk_field],
                filter_params={"bf": bloom_blob},
            )
        assert "only supports" in str(e.value), str(e.value)

        with pytest.raises(MilvusException) as e:
            client.query(
                collection_name,
                filter="roaring_match(structA[str_val], {rb})",
                output_fields=[pk_field],
                filter_params={"rb": roaring_blob},
            )
        assert "only supports" in str(e.value), str(e.value)
