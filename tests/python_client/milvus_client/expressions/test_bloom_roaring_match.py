"""
Unified membership_match expressions via the public PyMilvus MilvusClient API.

Covers the two client-built membership expressions against a live Milvus:

- membership_match(..., type=bloom) (#51140): approximate membership, zero false negatives, strict
  JSON-path typing (single key / nested / whole-doc / dynamic field), domain
  mismatch rejection, empty blob, fpr bounds, delete rejection, INT type matrix,
  scalar-index interaction (STL_SORT / INVERTED / BITMAP / AUTOINDEX), growing+sealed mix.
- membership_match(..., type=roaring) (#51968): exact membership, negative values, empty bitmap,
  negation, delete (positive/negated/empty), INT type matrix, INT64 bounds,
  invalid-input rejection, scalar-index interaction, growing+sealed mix.
- unified auto-dispatch, explicit type/magic mismatch, unknown magic, and
  rejection of the unreleased predecessor names.
- struct-array sub-field rejection for both membership kinds.

The client-side builders (`build_bloom_filter` / `build_roaring_bitmap`) are
required by pymilvus >= 3.1.0rc83 (roaring landed in pymilvus#3764).

Performance and capacity are out of scope.
"""

import json
import math
from pathlib import Path

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import DataType, build_bloom_filter, build_roaring_bitmap
from pymilvus.exceptions import MilvusException, ParamError

pk_field = "id"
vector_field = "vector"
creator_field = "creator_id"
dim = 8
nb = 2000
domain = 50

# Go and PyMilvus consume one static wire manifest while keeping collection
# lifecycle language-local. A simultaneous builder change therefore cannot
# hide wire drift behind duplicated constants.
MEMBERSHIP_FIXTURE_PATH = Path(__file__).resolve().parents[3] / "fixtures" / "membership_filter" / "manifest.json"
MEMBERSHIP_FIXTURE = json.loads(MEMBERSHIP_FIXTURE_PATH.read_text(encoding="utf-8"))
assert MEMBERSHIP_FIXTURE["schema_version"] == 1


class _MembershipBase(TestMilvusClientV2Base):
    def _build_int_collection(self, client, collection_name=None, force_teardown=True):
        """Create an INT64 membership collection with a flat vector field, insert,
        flush, index and load. Returns the collection name."""
        collection_name = collection_name or cf.gen_collection_name_by_testcase_name(module_index=2)
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(creator_field, datatype=DataType.INT64)
        schema.add_field(vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            consistency_level="Strong",
            force_teardown=force_teardown,
        )

        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            row[creator_field] = i % domain
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self._assert_index_ready(client, collection_name, vector_field, expected_rows=nb)
        self.load_collection(client, collection_name)
        return collection_name

    @pytest.fixture(scope="class")
    def shared_int_collection(self, request):
        """Build one immutable membership fixture for read-only cases in a class.

        Query, Search, malformed-input, and golden-wire checks do not mutate the
        collection, so they can share the same wide-enough PK/int64/vector table.
        Mutation, nullable, JSON, and scalar-index matrix cases continue to use
        their own fixtures.
        """
        alias = f"{request.cls.__name__}Shared"
        request.cls.shared_alias = alias
        client = self._client(alias=alias)
        collection_name = f"{request.cls.__name__}_{cf.gen_unique_str('_')}"

        def teardown():
            cleanup_client = self._client(alias=alias)
            if self.has_collection(cleanup_client, collection_name)[0]:
                self.drop_collection(cleanup_client, collection_name)

        request.addfinalizer(teardown)
        self._build_int_collection(client, collection_name=collection_name, force_teardown=False)
        return collection_name

    def _query_ids(self, client, collection_name, expr, **kwargs):
        res = self.query(client, collection_name, filter=expr, output_fields=[pk_field], **kwargs)[0]
        return sorted(r[pk_field] for r in res)

    def _search_ids(self, client, collection_name, expr, **kwargs):
        result_sets = client.search(
            collection_name,
            data=[[0.0] * dim],
            anns_field=vector_field,
            search_params={"metric_type": "L2", "params": {}},
            limit=nb,
            filter=expr,
            output_fields=[creator_field],
            **kwargs,
        )
        assert len(result_sets) == 1, f"expected exactly one query result, got {len(result_sets)}"
        hits = result_sets[0]
        ids = [hit[pk_field] for hit in hits]
        assert len(ids) == len(set(ids)), "Search returned duplicate primary keys"
        distances = [hit["distance"] for hit in hits]
        assert distances == sorted(distances), "L2 Search distances must be nondecreasing"
        for hit in hits:
            assert hit.get("entity", {}).get(creator_field) == hit[pk_field] % domain, (
                f"creator_id projection does not match PK {hit[pk_field]}"
            )
        return sorted(ids)

    def _assert_index_ready(self, client, collection_name, index_name, expected_rows=None, expected_index_type=None):
        assert self.wait_for_index_ready(client, collection_name, index_name=index_name), (
            f"index {index_name} on {collection_name} did not become ready"
        )
        info = self.describe_index(client, collection_name, index_name)[0]
        assert info.get("pending_index_rows") == 0, info
        if expected_rows is not None:
            assert info.get("total_rows") == expected_rows, info
            assert info.get("indexed_rows") == expected_rows, info
        if expected_index_type is not None:
            assert info.get("index_type") == expected_index_type, info

    @staticmethod
    def _expected_ids(total_rows, value_mod, members):
        member_set = set(members)
        return [i for i in range(total_rows) if i % value_mod in member_set]

    @staticmethod
    def _assert_bloom_result(got, expected, total_rows, context):
        """Require Bloom's zero-false-negative contract and reject an ignored filter.

        The 30% false-positive ceiling is intentionally far above the configured
        0.1%/5% FPR, making the assertion stable while still rejecting a full
        unfiltered result.
        """
        got_set = set(got)
        expected_set = set(expected)
        assert len(got) == len(got_set), f"Bloom Query returned duplicate PKs: {context}"
        assert expected_set <= got_set, f"Bloom result dropped true members: {context}"
        non_members = total_rows - len(expected_set)
        assert non_members >= 0, f"invalid Bloom ground truth: {context}"
        false_positives = len(got_set - expected_set)
        if non_members == 0:
            assert false_positives == 0, f"all rows are true members: {context}"
        else:
            assert false_positives * 10 < non_members * 3, (
                f"Bloom result has {false_positives} false positives over "
                f"{non_members} non-members; filter may be ignored: {context}"
            )

    @staticmethod
    def _assert_bloom_fpr_bound(got, expected, total_rows, configured_fpr):
        """Bound false positives using p + six standard deviations.

        This assertion is for fixtures with one row per unique probe value.
        The generous six-sigma binomial bound avoids flakes while still tying
        the accepted false-positive count to the configured FPR.
        """
        assert 0 < configured_fpr < 1
        got_set = set(got)
        expected_set = set(expected)
        assert len(got) == len(got_set), "Bloom Query returned duplicate PKs"
        assert expected_set <= got_set, "Bloom result dropped true members"
        non_members = total_rows - len(expected_set)
        assert non_members >= 0
        false_positives = len(got_set - expected_set)
        mean = non_members * configured_fpr
        standard_deviation = math.sqrt(non_members * configured_fpr * (1 - configured_fpr))
        upper_bound = math.ceil(mean + 6 * standard_deviation)
        assert false_positives <= upper_bound, (
            f"Bloom result has {false_positives} false positives over {non_members} unique non-members; "
            f"configured fpr={configured_fpr:.4f}, six-sigma upper bound={upper_bound}"
        )

    def _build_indexed_int_collection(self, client, index_type, total_rows=2000, val_mod=domain):
        """Like _build_int_collection but builds a scalar index on creator_field
        BEFORE load. total_rows must stay >= indexCoord.segment
        .minSegmentNumRowsToEnableIndex (1024) so the scalar index is really
        built, not fake-finished."""
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=2)
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(creator_field, datatype=DataType.INT64)
        schema.add_field(vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rows = cf.gen_row_data_by_schema(nb=total_rows, schema=schema, start=0)
        for i, row in enumerate(rows):
            row[creator_field] = i % val_mod
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=creator_field, index_type=index_type)
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self._assert_index_ready(
            client,
            collection_name,
            creator_field,
            expected_rows=total_rows,
            expected_index_type=index_type,
        )
        self._assert_index_ready(client, collection_name, vector_field, expected_rows=total_rows)
        self.load_collection(client, collection_name)
        return collection_name

    def _build_struct_array_collection(self, client):
        """Build a small struct-array collection with INT64/VARCHAR/FLOAT
        sub-fields, used to exercise parser-level rejection of bloom_match /
        roaring_match on struct-array sub-fields."""
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=2)
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

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

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
        self._assert_index_ready(client, collection_name, vector_field)
        self.load_collection(client, collection_name)
        return collection_name

    def _build_custom_collection(self, client, fields, row_mutator, enable_dynamic_field=False):
        """Create + insert + flush + index + load a collection with a custom set of
        scalar fields.

        `fields` is a list of (name, DataType) tuples; the int64 primary key is
        added first and the float-vector field last. `row_mutator(i, row)` fills
        the non-default values for the i-th row. Returns the collection name.
        """
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=2)
        schema, _ = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)
        schema.add_field(pk_field, datatype=DataType.INT64, is_primary=True, auto_id=False)
        for name, datatype in fields:
            schema.add_field(name, datatype=datatype)
        schema.add_field(vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            row_mutator(i, row)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self._assert_index_ready(client, collection_name, vector_field, expected_rows=nb)
        self.load_collection(client, collection_name)
        return collection_name

    def _build_nullable_int_collection(self, client):
        """Create a nullable INT64 membership collection: rows with i % 8 == 7
        carry NULL creator_id, the rest cycle 0..domain-1. Index + load."""
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=2)
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(creator_field, datatype=DataType.INT64, nullable=True)
        schema.add_field(vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            row[creator_field] = None if i % 8 == 7 else i % domain
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self._assert_index_ready(client, collection_name, vector_field, expected_rows=nb)
        self.load_collection(client, collection_name)
        return collection_name


class TestBloomMatch(_MembershipBase):
    @pytest.mark.xdist_group("TestBloomMatchShared")
    @pytest.mark.tags(CaseLabel.L0)
    def test_bloom_match_zero_false_negatives(self, shared_int_collection):
        """bloom_match must return a superset of exact `in` (no false negatives)."""
        client = self._client(alias=self.shared_alias)
        collection_name = shared_int_collection

        members = list(range(10))
        blob = build_bloom_filter(members, fpr=0.001)

        exact = self._query_ids(client, collection_name, f"{creator_field} in [0,1,2,3,4,5,6,7,8,9]")
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{bf}}, type=bloom)",
            filter_params={"bf": blob},
            consistency_level="Strong",
        )
        self._assert_bloom_result(got, exact, nb, "sealed INT64 Query")

        # Query and Search intentionally share this loaded collection and blob.
        # The FLAT vector index plus limit=nb makes both filtered result sets
        # complete, so Search must agree exactly with Query, including any
        # deterministic Bloom false positives.
        search_got = self._search_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{bf}}, type=bloom)",
            filter_params={"bf": blob},
        )
        assert search_got == got, "Search membership_match must return the same Bloom-filtered PK set as Query"

        # not bloom_match must never leak a true member
        not_got = self._query_ids(
            client,
            collection_name,
            f"not membership_match({creator_field}, {{bf}}, type=bloom)",
            filter_params={"bf": blob},
        )
        assert set(got).isdisjoint(not_got), "bloom_match and its negation must be disjoint"
        assert set(got) | set(not_got) == set(range(nb)), "bloom_match and its negation must partition all rows"

    @pytest.mark.xdist_group("TestBloomMatchShared")
    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_python_golden_bytes(self, shared_int_collection):
        """A fixed pymilvus-built blob remains executable by the server."""
        client = self._client(alias=self.shared_alias)
        collection_name = shared_int_collection
        fixture = MEMBERSHIP_FIXTURE["bloom"]
        blob = bytes.fromhex(fixture["hex"])
        assert blob == build_bloom_filter(fixture["members"], fpr=fixture["fpr"])
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{bf}}, type=bloom)",
            filter_params={"bf": blob},
        )
        expected = self._expected_ids(nb, domain, fixture["present_members"])
        self._assert_bloom_result(got, expected, nb, "pymilvus golden Bloom blob")

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
            got = self._query_ids(
                client, collection_name, f"membership_match({field}, {{bf}}, type=bloom)", filter_params={"bf": blob}
            )
            self._assert_bloom_result(got, exact, nb, f"integer field {field}")

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
            filter='membership_match(meta["uid"], {bf}, type=bloom)',
            output_fields=[pk_field],
            filter_params={"bf": blob},
        )[0]
        ids = sorted(r[pk_field] for r in res)
        expected = [i for i in range(nb) if i % 11 != 0 and i % 3 != 0]
        assert ids == expected, "JSON membership must return every int row and no missing/float row"

        negated_ids = self._query_ids(
            client,
            collection_name,
            'not membership_match(meta["uid"], {bf}, type=bloom)',
            filter_params={"bf": blob},
        )
        expected_negated = [i for i in range(nb) if i % 11 != 0 and i % 3 == 0]
        assert negated_ids == expected_negated, (
            "negation must select float-encoded rows while missing-key rows remain invalid"
        )

        # exact `in` returns the float-encoded rows too (5.0 == 5); bloom does not.
        exact = self._query_ids(client, collection_name, 'meta["uid"] in [0,1,2,3,4,5,6,7,8,9]')
        bloom_ids = set(ids)
        diverged = [i for i in exact if i % 3 == 0 and i % 11 != 0 and i not in bloom_ids]
        assert diverged, "expected float-encoded rows in exact `in` but not bloom_match"

    @pytest.mark.xdist_group("TestBloomMatchShared")
    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_domain_mismatch_rejected(self, shared_int_collection):
        """A blob built from the wrong value domain is rejected at the proxy."""
        client = self._client(alias=self.shared_alias)
        collection_name = shared_int_collection

        # utf8-domain blob on an int64 field
        str_blob = build_bloom_filter(["0", "1", "2"], fpr=0.001)
        with pytest.raises(MilvusException) as e:
            client.query(
                collection_name,
                filter=f"membership_match({creator_field}, {{bf}}, type=bloom)",
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
                filter=f"membership_match({creator_field}, {{bf}}, type=bloom)",
                filter_params={"bf": blob},
            )
        assert "cannot be used in delete" in str(e.value), str(e.value)

    @pytest.mark.xdist_group("TestBloomMatchShared")
    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_empty_blob(self, shared_int_collection):
        """An empty membership blob matches nothing and is not rejected."""
        client = self._client(alias=self.shared_alias)
        collection_name = shared_int_collection
        blob = build_bloom_filter([], fpr=0.001)
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{bf}}, type=bloom)",
            filter_params={"bf": blob},
        )
        assert got == []

    @pytest.mark.tags(CaseLabel.L2)
    def test_bloom_match_fpr_out_of_range(self):
        """The client builder rejects an out-of-range fpr."""
        with pytest.raises(ParamError, match="fpr must be in"):
            build_bloom_filter([0, 1, 2], fpr=0.00001)
        with pytest.raises(ParamError, match="fpr must be in"):
            build_bloom_filter([0, 1, 2], fpr=0.06)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_type", ["STL_SORT", "INVERTED", "BITMAP"])
    def test_bloom_match_scalar_index_type_matrix(self, index_type):
        """bloom_match stays zero-false-negative with each supported explicit
        scalar index entry point: STL_SORT, INVERTED, and BITMAP."""
        client = self._client()
        collection_name = self._build_indexed_int_collection(client, index_type)

        members = list(range(10))
        blob = build_bloom_filter(members, fpr=0.001)
        exact = self._query_ids(client, collection_name, f"{creator_field} in [0,1,2,3,4,5,6,7,8,9]")
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{bf}}, type=bloom)",
            filter_params={"bf": blob},
        )
        self._assert_bloom_result(got, exact, nb, f"explicit scalar index {index_type}")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("val_mod", [200, 50])
    def test_bloom_match_auto_index(self, val_mod):
        """AUTOINDEX smoke coverage at two data cardinalities.

        Explicit STL_SORT, INVERTED, and BITMAP execution is covered separately;
        this test does not assume AUTOINDEX's internal resolution policy.
        """
        client = self._client()
        collection_name = self._build_indexed_int_collection(client, "AUTOINDEX", val_mod=val_mod)

        members = list(range(10))
        blob = build_bloom_filter(members, fpr=0.001)
        exact = self._query_ids(client, collection_name, f"{creator_field} in [0,1,2,3,4,5,6,7,8,9]")
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{bf}}, type=bloom)",
            filter_params={"bf": blob},
        )
        self._assert_bloom_result(got, exact, nb, f"AUTOINDEX domain {val_mod}")

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
            client, collection_name, 'membership_match(meta["a"]["b"], {bf}, type=bloom)', filter_params={"bf": blob}
        )
        nested_expected = [i for i in range(nb) if i % 3 == 0]
        assert nested == nested_expected, "membership_match(nested) must return every nested int row and no float row"

        whole = self._query_ids(
            client, collection_name, "membership_match(meta, {bf}, type=bloom)", filter_params={"bf": blob}
        )
        whole_expected = [i for i in range(nb) if i % 3 == 2]
        assert whole == whole_expected, "membership_match(whole doc) must return every bare int row and no nested row"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_dynamic_field_path(self):
        """bloom_match over a dynamic field: an unknown identifier resolves to a
        $meta path, strictly typed per row value."""
        client = self._client()

        def mutate(i, row):
            row["uid"] = i % 10

        collection_name = self._build_custom_collection(client, [], mutate, enable_dynamic_field=True)

        blob = build_bloom_filter(list(range(10)), fpr=0.001)
        got = self._query_ids(
            client, collection_name, "membership_match(uid, {bf}, type=bloom)", filter_params={"bf": blob}
        )
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
            client,
            collection_name,
            f"membership_match({creator_field}, {{bf}}, type=bloom)",
            filter_params={"bf": blob},
            consistency_level="Strong",
        )
        sealed_expected = [i for i in range(nb) if i % domain < 5]
        growing_expected = [nb + i for i in range(growing_n) if i % 10 < 5]
        expected = sealed_expected + growing_expected
        self._assert_bloom_result(got, expected, nb + growing_n, "sealed and growing segments")

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_null_rows_fold_to_false(self):
        """NULL rows never match bloom_match nor its negation (NULL folds to
        FALSE on both sides)."""
        client = self._client()
        collection_name = self._build_nullable_int_collection(client)

        blob = build_bloom_filter(list(range(domain)), fpr=0.001)
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{bf}}, type=bloom)",
            filter_params={"bf": blob},
        )
        expected = [i for i in range(nb) if i % 8 != 7]
        assert got == expected, "all and only non-NULL rows must match when every value is a member"

        not_got = self._query_ids(
            client,
            collection_name,
            f"not membership_match({creator_field}, {{bf}}, type=bloom)",
            filter_params={"bf": blob},
        )
        assert not_got == [], "NULL folds to false and every non-NULL value matches"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_varchar_zero_false_negatives(self):
        """VARCHAR membership has no false negatives (exact `in` ⊆ bloom_match)."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("tag", datatype=DataType.VARCHAR, max_length=64)
        schema.add_field(vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            row["tag"] = f"tag{i % domain}"
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params)
        self._assert_index_ready(client, collection_name, vector_field, expected_rows=nb)
        self.load_collection(client, collection_name)

        str_blob = build_bloom_filter([f"tag{v}" for v in range(5)], fpr=0.001)
        exact = self._query_ids(client, collection_name, 'tag in ["tag0","tag1","tag2","tag3","tag4"]')
        got = self._query_ids(
            client, collection_name, "membership_match(tag, {bf}, type=bloom)", filter_params={"bf": str_blob}
        )
        self._assert_bloom_result(got, exact, nb, "VARCHAR Query")

    @pytest.mark.xdist_group("TestBloomMatchShared")
    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_malformed_blob_and_literal_rejected(self, shared_int_collection):
        """A malformed blob and a literal array argument are rejected rather than
        silently unfiltered."""
        client = self._client(alias=self.shared_alias)
        collection_name = shared_int_collection

        with pytest.raises(MilvusException) as exc:
            client.query(
                collection_name,
                filter=f"membership_match({creator_field}, {{bf}}, type=bloom)",
                output_fields=[pk_field],
                filter_params={"bf": b"not-a-real-blob"},
            )
        assert "unknown format magic" in str(exc.value), str(exc.value)
        with pytest.raises(MilvusException) as exc:
            client.query(
                collection_name,
                filter=f"membership_match({creator_field}, [1,2,3], type=bloom)",
                output_fields=[pk_field],
            )
        assert "must be a {template} placeholder" in str(exc.value), str(exc.value)

    @pytest.mark.tags(CaseLabel.L1)
    def test_bloom_match_false_positive_rate_sanity(self):
        """The measured FPR over unique probes stays within a six-sigma upper
        bound derived from the configured 5% FPR."""
        client = self._client()

        def mutate(i, row):
            row[creator_field] = i

        collection_name = self._build_custom_collection(client, [(creator_field, DataType.INT64)], mutate)
        configured_fpr = 0.05
        members = list(range(10))
        blob = build_bloom_filter(members, fpr=configured_fpr)
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{bf}}, type=bloom)",
            filter_params={"bf": blob},
        )
        self._assert_bloom_fpr_bound(got, members, nb, configured_fpr)


class TestRoaringMatch(_MembershipBase):
    @pytest.mark.xdist_group("TestRoaringMatchShared")
    @pytest.mark.tags(CaseLabel.L0)
    def test_roaring_match_exact_membership(self, shared_int_collection):
        """roaring_match selects exactly the requested ids — no false positives."""
        client = self._client(alias=self.shared_alias)
        collection_name = shared_int_collection

        blob = build_roaring_bitmap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000000])
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
            consistency_level="Strong",
        )
        expected = self._expected_ids(nb, domain, range(10))
        assert got == expected, "roaring_match must return the exact matching row PK set"

        # Query and Search intentionally share this loaded collection and blob.
        search_got = self._search_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
        )
        assert search_got == expected, "Search roaring_match must return the exact row PK set"

    @pytest.mark.xdist_group("TestRoaringMatchShared")
    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_python_golden_bytes(self, shared_int_collection):
        """A fixed pymilvus-built blob remains byte-compatible and exact."""
        client = self._client(alias=self.shared_alias)
        collection_name = shared_int_collection
        fixture = MEMBERSHIP_FIXTURE["roaring"]
        blob = bytes.fromhex(fixture["hex"])
        assert blob == build_roaring_bitmap(fixture["members"])
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
        )
        expected = self._expected_ids(nb, domain, fixture["present_members"])
        assert got == expected, "golden roaring blob result mismatch"

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
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
        )
        present = {m for m in members if -shift <= m <= d - shift - 1}
        expected = [i for i in range(nb) if i % d - shift in present]
        assert got == expected, "roaring_match must return the exact negative-value row PK set"

    @pytest.mark.xdist_group("TestRoaringMatchShared")
    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_not_and_empty_bitmap(self, shared_int_collection):
        """not roaring_match is the exact complement; an empty bitmap selects
        no rows, while negating it selects every non-NULL row."""
        client = self._client(alias=self.shared_alias)
        collection_name = shared_int_collection

        blob = build_roaring_bitmap([0, 1, 2, 3, 4])
        got = self._query_ids(
            client,
            collection_name,
            f"not membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
        )
        expected = self._expected_ids(nb, domain, range(5, domain))
        assert got == expected, "not roaring_match must return the exact complement row PK set"

        empty = build_roaring_bitmap([])
        no_rows = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": empty},
        )
        assert no_rows == [], "an empty bitmap must match no rows"

        all_rows = self._query_ids(
            client,
            collection_name,
            f"not membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": empty},
        )
        assert len(all_rows) == nb, "negating an empty bitmap must select every non-NULL row"

    @pytest.mark.tags(CaseLabel.L0)
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
            filter=f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
        )[0]
        # Fixture couples pk == row index i and creator == i % domain, so each
        # creator value appears nb // domain times; deleting len(victims) distinct
        # values removes len(victims) * nb // domain rows.
        assert res["delete_count"] == len(victims) * nb // domain

        remaining = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
        )
        assert remaining == [], "victim rows must be deleted"

        empty = build_roaring_bitmap([])
        res = self.delete(
            client,
            collection_name,
            filter=f"membership_match({creator_field}, {{rb}}, type=roaring)",
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
                client, collection_name, f"membership_match({field}, {{rb}}, type=roaring)", filter_params={"rb": blob}
            )
            expected = [i for i in range(nb) if i % d - shift in present]
            assert got == expected, f"roaring_match row set mismatch on {field}"

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
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
        )
        assert len(got) == nb, "all four values present, every row must match"

        absent = build_roaring_bitmap([(1 << 63) - 2])
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": absent},
        )
        assert got == []

    @pytest.mark.xdist_group("TestRoaringMatchShared")
    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_invalid_input_rejected(self, shared_int_collection):
        """Literal-list and malformed-blob arguments are rejected."""
        client = self._client(alias=self.shared_alias)
        collection_name = shared_int_collection

        with pytest.raises(MilvusException) as exc:
            client.query(
                collection_name,
                filter=f"membership_match({creator_field}, [1,2,3], type=roaring)",
                output_fields=[pk_field],
            )
        assert "must be a {template} placeholder" in str(exc.value), str(exc.value)
        with pytest.raises(MilvusException) as exc:
            client.query(
                collection_name,
                filter=f"membership_match({creator_field}, {{rb}}, type=roaring)",
                output_fields=[pk_field],
                filter_params={"rb": b"not-an-mrb1-blob"},
            )
        assert "unknown format magic" in str(exc.value), str(exc.value)

        valid = build_roaring_bitmap([1, 2, 3])
        with pytest.raises(MilvusException) as exc:
            client.query(
                collection_name,
                filter=f"membership_match({creator_field}, {{rb}}, type=roaring)",
                output_fields=[pk_field],
                filter_params={"rb": valid[:-1]},
            )
        assert "membership_match roaring bitmap blob is invalid" in str(exc.value), str(exc.value)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_type", ["STL_SORT", "INVERTED", "BITMAP"])
    def test_roaring_match_scalar_index_type_matrix(self, index_type):
        """roaring_match stays exact with each supported explicit scalar index
        entry point: STL_SORT, INVERTED, and BITMAP."""
        client = self._client()
        collection_name = self._build_indexed_int_collection(client, index_type)

        blob = build_roaring_bitmap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000000])
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
        )
        expected = self._expected_ids(2000, domain, range(10))
        assert got == expected, f"roaring_match row set mismatch under {index_type}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("val_mod", [200, 50])
    def test_roaring_match_auto_index(self, val_mod):
        """AUTOINDEX smoke coverage at two data cardinalities.

        Explicit STL_SORT, INVERTED, and BITMAP execution is covered separately;
        this test does not assume AUTOINDEX's internal resolution policy.
        """
        client = self._client()
        collection_name = self._build_indexed_int_collection(client, "AUTOINDEX", val_mod=val_mod)

        blob = build_roaring_bitmap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000000])
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
        )
        expected = self._expected_ids(2000, val_mod, range(10))
        assert got == expected, f"roaring_match row set mismatch under AUTOINDEX domain {val_mod}"

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
            filter=f"not membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
        )[0]
        # Negating the set deletes every creator value except len(keep), i.e.
        # (domain - len(keep)) values; each value spans nb // domain rows.
        assert res["delete_count"] == (domain - len(keep)) * nb // domain

        remaining = self._query_ids(client, collection_name, f"{pk_field} >= 0")
        expected = self._expected_ids(nb, domain, keep)
        assert remaining == expected, "negated delete must preserve exactly the PKs in the keep set"

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
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
            consistency_level="Strong",
        )
        sealed_expected = [i for i in range(nb) if i % domain < 5]
        growing_expected = [nb + i for i in range(growing_n) if i % 10 < 5]
        expected = sorted(sealed_expected + growing_expected)
        assert got == expected, "roaring_match must return every exact member across sealed and growing segments"

    @pytest.mark.tags(CaseLabel.L1)
    def test_roaring_match_null_rows_fold_to_false(self):
        """NULL rows never match roaring_match nor its negation."""
        client = self._client()
        collection_name = self._build_nullable_int_collection(client)

        blob = build_roaring_bitmap(list(range(domain)))
        got = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
        )
        expected = [i for i in range(nb) if i % 8 != 7]
        assert got == expected, "all and only non-NULL rows must match when every value is a member"

        not_got = self._query_ids(
            client,
            collection_name,
            f"not membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": blob},
        )
        assert not_got == [], "NULL folds to false and every non-NULL value matches"


class TestMembershipMatchContract(_MembershipBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_membership_match_unsupported_top_level_fields(self):
        """
        target: verify each membership kind rejects unsupported top-level scalar fields
        method: execute Bloom and Roaring blobs against BOOL and FLOAT fields
        expected: every query fails with code 1100 and the kind-specific supported-type contract
        """
        client = self._client()

        def mutate(i, row):
            row["flag"] = i % 2 == 0
            row["score"] = float(i % domain)

        collection_name = self._build_custom_collection(
            client,
            [("flag", DataType.BOOL), ("score", DataType.FLOAT)],
            mutate,
        )
        cases = [
            ("bloom", "flag", build_bloom_filter([0, 1], fpr=0.001), "INT8/INT16/INT32/INT64/VARCHAR"),
            ("bloom", "score", build_bloom_filter([0, 1], fpr=0.001), "INT8/INT16/INT32/INT64/VARCHAR"),
            ("roaring", "flag", build_roaring_bitmap([0, 1]), "INT8/INT16/INT32/INT64"),
            ("roaring", "score", build_roaring_bitmap([0, 1]), "INT8/INT16/INT32/INT64"),
        ]
        for kind, field, blob, supported_types in cases:
            with pytest.raises(MilvusException) as exc:
                client.query(
                    collection_name,
                    filter=f"membership_match({field}, {{blob}}, type={kind})",
                    output_fields=[pk_field],
                    filter_params={"blob": blob},
                )
            assert exc.value.code == 1100, str(exc.value)
            assert f"only supports {supported_types}" in str(exc.value), str(exc.value)

    @pytest.mark.xdist_group("TestMembershipMatchContractShared")
    @pytest.mark.tags(CaseLabel.L0)
    def test_membership_match_unified_contract(self, shared_int_collection):
        """Pin auto-dispatch, explicit type validation, fail-closed unknown
        formats, and rejection of the two predecessor function names."""
        client = self._client(alias=self.shared_alias)
        collection_name = shared_int_collection
        bloom_blob = build_bloom_filter([0, 1, 2, 42], fpr=0.001)
        roaring_blob = build_roaring_bitmap([0, 1, 2, 42])

        bloom_auto = self._query_ids(
            client, collection_name, f"membership_match({creator_field}, {{bf}})", filter_params={"bf": bloom_blob}
        )
        bloom_pinned = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{bf}}, type=bloom)",
            filter_params={"bf": bloom_blob},
        )
        expected = self._expected_ids(nb, domain, [0, 1, 2, 42])
        self._assert_bloom_result(bloom_auto, expected, nb, "auto-dispatched Bloom")
        self._assert_bloom_result(bloom_pinned, expected, nb, "explicit Bloom")
        assert bloom_auto == bloom_pinned

        roaring_auto = self._query_ids(
            client, collection_name, f"membership_match({creator_field}, {{rb}})", filter_params={"rb": roaring_blob}
        )
        roaring_pinned = self._query_ids(
            client,
            collection_name,
            f"membership_match({creator_field}, {{rb}}, type=roaring)",
            filter_params={"rb": roaring_blob},
        )
        assert roaring_auto == expected
        assert roaring_pinned == expected
        assert roaring_auto == roaring_pinned

        mismatches = [
            (f"membership_match({creator_field}, {{bf}}, type=roaring)", "bf", bloom_blob),
            (f"membership_match({creator_field}, {{rb}}, type=bloom)", "rb", roaring_blob),
        ]
        for expression, key, blob in mismatches:
            with pytest.raises(MilvusException) as exc:
                client.query(
                    collection_name,
                    filter=expression,
                    output_fields=[pk_field],
                    filter_params={key: blob},
                )
            assert "does not match filter blob format" in str(exc.value), str(exc.value)

        with pytest.raises(MilvusException) as exc:
            client.query(
                collection_name,
                filter=f"membership_match({creator_field}, {{blob}})",
                output_fields=[pk_field],
                filter_params={"blob": b"UNKNOWN-MEMBERSHIP-FORMAT"},
            )
        assert "unknown format magic" in str(exc.value), str(exc.value)

        predecessor_calls = [
            (f"bloom_match({creator_field}, {{bf}})", "bf", bloom_blob),
            (f"roaring_match({creator_field}, {{rb}})", "rb", roaring_blob),
        ]
        for expression, key, blob in predecessor_calls:
            with pytest.raises(MilvusException) as exc:
                client.query(
                    collection_name,
                    filter=expression,
                    output_fields=[pk_field],
                    filter_params={key: blob},
                )
            message = str(exc.value)
            assert "is not supported" in message, message
            assert "membership_match" in message, message


class TestMembershipStructArrayRejected(_MembershipBase):
    """Both membership kinds must be rejected on struct-array sub-fields."""

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
                filter="membership_match(structA[int_val], {bf}, type=bloom)",
                output_fields=[pk_field],
                filter_params={"bf": bloom_blob},
            )
        assert "only supports" in str(e.value), str(e.value)

        with pytest.raises(MilvusException) as e:
            client.query(
                collection_name,
                filter="membership_match(structA[int_val], {rb}, type=roaring)",
                output_fields=[pk_field],
                filter_params={"rb": roaring_blob},
            )
        assert "only supports" in str(e.value), str(e.value)

        with pytest.raises(MilvusException) as e:
            client.query(
                collection_name,
                filter="membership_match(structA[str_val], {bf}, type=bloom)",
                output_fields=[pk_field],
                filter_params={"bf": str_blob},
            )
        assert "only supports" in str(e.value), str(e.value)

        with pytest.raises(MilvusException) as e:
            client.query(
                collection_name,
                filter="membership_match(structA[float_val], {bf}, type=bloom)",
                output_fields=[pk_field],
                filter_params={"bf": bloom_blob},
            )
        assert "only supports" in str(e.value), str(e.value)

        with pytest.raises(MilvusException) as e:
            client.query(
                collection_name,
                filter="membership_match(structA[str_val], {rb}, type=roaring)",
                output_fields=[pk_field],
                filter_params={"rb": roaring_blob},
            )
        assert "only supports" in str(e.value), str(e.value)
