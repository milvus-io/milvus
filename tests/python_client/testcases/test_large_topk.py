"""
Large TopK Feature E2E Tests
Feature: collection-level property `query_mode: large_topk`
         - Backend index auto-switches to IVF RBQ2
         - Supports topk up to 1M level
         - alter/drop property requires dropping vector index first

Test Plan: tests/python_client/docs/test-plan-large-topk.md
Issue: https://github.com/milvus-io/milvus/issues/48725
"""
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType, MilvusException

prefix = "large_topk"
default_nb = 3000          # > 1024 to trigger IVF index build
default_dim = ct.default_dim   # 128
default_nq = ct.default_nq     # 2
default_limit = ct.default_limit  # 10
vec_field = ct.default_float_vec_field_name  # "float_vector"
large_topk_first = 16385   # first topk above the normal 16384 limit
large_topk_total = 21000   # total rows in col_large_topk (> large_topk_first + default_nb for headroom)


@pytest.mark.xdist_group("TestLargeTopkShared")
class TestLargeTopkShared(TestMilvusClientV2Base):
    """
    L0 + L1 read-only tests. Two shared collections are prepared once:
      - col_large_topk: query_mode=large_topk, 6×default_nb vectors, FLAT index
      - col_normal:     no query_mode set,      default_nb vectors,   FLAT index
    All tests are read-only; no data modification or index changes.
    """

    def setup_class(self):
        super().setup_class(self)
        self.col_large_topk = "TestLargeTopkSharedLargeTopk" + cf.gen_unique_str("_")
        self.col_normal = "TestLargeTopkSharedNormal" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collections(self, request):
        client = self._client()

        def _create(col_name, enable_large_topk, batches=1):
            schema = self.create_schema(client)[0]
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
            schema.add_field(vec_field, DataType.FLOAT_VECTOR, dim=default_dim)
            query_mode_props = {"query_mode": "large_topk"} if enable_large_topk else None
            self.create_collection(client, col_name, schema=schema,
                                   properties=query_mode_props, force_teardown=False)
            index_params = self.prepare_index_params(client)[0]
            # FLAT: 100% recall, simplifies assertions
            index_params.add_index(vec_field, index_type="FLAT", metric_type="L2")
            self.create_index(client, col_name, index_params)
            self.load_collection(client, col_name)
            rows = [{vec_field: cf.gen_vectors(1, default_dim)[0]} for _ in range(default_nb)]
            for _ in range(batches):
                self.insert(client, col_name, rows)
            self.flush(client, col_name)

        # large_topk_total rows total; topk tests use large_topk_total - default_nb for ~1 batch headroom
        _create(self.col_large_topk, enable_large_topk=True, batches=large_topk_total // default_nb)
        _create(self.col_normal, enable_large_topk=False)

        def teardown():
            client.drop_collection(self.col_large_topk)
            client.drop_collection(self.col_normal)

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_with_large_topk_property(self):
        """
        target: verify query_mode=large_topk property is correctly set at create time
        method:
            1. describe_collection and check properties dict contains query_mode=large_topk
            2. search with limit=100, check nq/limit/metric
        expected: property present; search returns 100 results with ascending L2 distances
        """
        client = self._client()
        desc = client.describe_collection(self.col_large_topk)
        props = desc.get("properties", {})
        assert props.get("query_mode") == "large_topk", f"property not set: {props}"

        vectors = cf.gen_vectors(default_nq, default_dim)
        self.search(client, self.col_large_topk, data=vectors,
                    anns_field=vec_field, limit=100,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "limit": 100,
                                 "metric": "L2"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("topk", [1, 100, 16384])
    def test_search_various_topk(self, topk):
        """
        target: verify search with various topk values all return correct counts
        method: search col_with_prop with topk in [1, 100, 16384], check nq/limit/metric
        expected: each search returns exactly min(topk, default_nb) hits per query
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, default_dim)
        expected_limit = min(topk, large_topk_total)
        self.search(client, self.col_large_topk, data=vectors,
                    anns_field=vec_field, limit=topk,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "limit": expected_limit,
                                 "metric": "L2"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_result_consistency(self):
        """
        target: verify repeated searches return identical results (IDs and distances)
        method: search same query vector twice, compare id list and distance list
        expected: ids and distances are identical across two calls
        """
        client = self._client()
        vectors = cf.gen_vectors(1, default_dim)

        res1 = client.search(self.col_large_topk, data=vectors,
                             limit=50, anns_field=vec_field)
        res2 = client.search(self.col_large_topk, data=vectors,
                             limit=50, anns_field=vec_field)

        ids1 = [r["id"] for r in res1[0]]
        ids2 = [r["id"] for r in res2[0]]
        dist1 = [r["distance"] for r in res1[0]]
        dist2 = [r["distance"] for r in res2[0]]
        assert ids1 == ids2, f"Inconsistent IDs: {ids1[:5]} vs {ids2[:5]}"
        assert dist1 == dist2, f"Inconsistent distances: {dist1[:5]} vs {dist2[:5]}"

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("topk", [large_topk_first, large_topk_total - default_nb])
    def test_large_topk_above_normal_limit(self, topk):
        """
        target: verify query_mode=large_topk allows topk above 16384 without error (core MVP)
        method: search col_large_topk (large_topk_total vectors) with topk in [large_topk_first, large_topk_total - default_nb]
        expected: search completes without exception, returns results with ascending L2 distances.
                  Note: exact result count is not asserted — large_topk forces IVF index which
                  does not guarantee 100% recall, so returned count may be < topk.
        """
        client = self._client()
        results = client.search(self.col_large_topk, data=cf.gen_vectors(default_nq, default_dim),
                                anns_field=vec_field, limit=topk)
        for hits in results:
            assert len(hits) > 0, "Expected non-empty results"
            distances = [h["distance"] for h in hits]
            assert distances == sorted(distances), "L2 distances should be ascending"

    @pytest.mark.tags(CaseLabel.L2)
    def test_topk_without_large_topk_property(self):
        """
        target: verify topk>16384 is rejected when query_mode=large_topk is not set
        method:
            1. search col_without_prop with limit=16384 — should succeed
            2. search col_without_prop with limit=large_topk_first — should raise MilvusException
        expected: limit=16384 OK; limit=large_topk_first raises error with message about invalid topk
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, default_dim)

        # Normal topk limit works fine
        self.search(client, self.col_normal, data=vectors,
                    anns_field=vec_field, limit=16384,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "limit": default_nb,
                                 "metric": "L2"})

        # Above limit must be rejected
        error = {ct.err_code: 65535,
                 ct.err_msg: f"topk [{large_topk_first}] is invalid, it should be in range [1, 16384]"}
        self.search(client, self.col_normal, data=vectors,
                    anns_field=vec_field, limit=large_topk_first,
                    check_task=CheckTasks.err_res,
                    check_items=error)


# ---------------------------------------------------------------------------
# Independent Tests (each test owns its own collection)
# ---------------------------------------------------------------------------
class TestLargeTopkIndependent(TestMilvusClientV2Base):
    """
    Tests that require modifying index or property — each test gets its own collection.
    force_teardown=True (default) ensures cleanup even on failure.
    """

    def _setup_col(self, client, enable_large_topk=True, nb=default_nb):
        """Create collection with optional query_mode=large_topk, FLAT index, insert nb rows."""
        col = cf.gen_collection_name_by_testcase_name(module_index=2)
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(vec_field, DataType.FLOAT_VECTOR, dim=default_dim)
        query_mode_props = {"query_mode": "large_topk"} if enable_large_topk else None
        self.create_collection(client, col, schema=schema,
                               properties=query_mode_props, force_teardown=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(vec_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, col, index_params)
        self.load_collection(client, col)
        if nb > 0:
            rows = [{vec_field: cf.gen_vectors(1, default_dim)[0]} for _ in range(nb)]
            self.insert(client, col, rows)
            self.flush(client, col)
        return col

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_collection_add_property(self):
        """
        target: verify alter_collection_properties correctly adds query_mode=large_topk
        method:
            1. create collection without property, build FLAT index, insert data
            2. release → drop_index → alter_collection_properties
            3. describe_collection to verify property
            4. rebuild index, load, search with limit=100
        expected: property set; search returns 100 results with ascending L2 distances
        """
        client = self._client()
        col = self._setup_col(client, enable_large_topk=False)

        self.release_collection(client, col)
        self.drop_index(client, col, vec_field)

        self.alter_collection_properties(client, col,
                                         properties={"query_mode": "large_topk"})

        desc = client.describe_collection(col)
        assert desc.get("properties", {}).get("query_mode") == "large_topk"

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(vec_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, col, index_params)
        self.load_collection(client, col)

        self.search(client, col, data=cf.gen_vectors(default_nq, default_dim),
                    anns_field=vec_field, limit=100,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq, "limit": 100, "metric": "L2"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_collection_property(self):
        """
        target: verify drop_collection_properties removes query_mode and restores normal behavior
        method:
            1. create collection with property, build FLAT index, insert data
            2. release → drop_index → drop_collection_properties
            3. describe_collection to verify property absent
            4. rebuild index, load, search with limit=default_limit
        expected: property absent; normal search returns default_limit results
        """
        client = self._client()
        col = self._setup_col(client, enable_large_topk=True)

        self.release_collection(client, col)
        self.drop_index(client, col, vec_field)

        self.drop_collection_properties(client, col, property_keys=["query_mode"])

        desc = client.describe_collection(col)
        assert "query_mode" not in desc.get("properties", {}), \
            f"property still present: {desc.get('properties')}"

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(vec_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, col, index_params)
        self.load_collection(client, col)

        self.search(client, col, data=cf.gen_vectors(default_nq, default_dim),
                    anns_field=vec_field, limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "limit": default_limit,
                                 "metric": "L2"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_property_without_dropping_index_fails(self):
        """
        target: verify alter_collection_properties is rejected when vector index exists
        method: create collection with index, call alter_collection_properties directly
        expected: MilvusException with error code 702 and message containing "vector index"
        """
        client = self._client()
        col = self._setup_col(client, enable_large_topk=False, nb=0)

        error = {ct.err_code: 702,
                 ct.err_msg: "can not alter query_mode if the collection already has a vector index"}
        self.alter_collection_properties(client, col,
                                         properties={"query_mode": "large_topk"},
                                         check_task=CheckTasks.err_res,
                                         check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_property_without_dropping_index_fails(self):
        """
        target: verify drop_collection_properties is rejected when vector index exists
        method: create collection with large_topk property and index, call drop directly
        expected: MilvusException with error code 702 and message containing "vector index"
        """
        client = self._client()
        col = self._setup_col(client, enable_large_topk=True, nb=0)

        error = {ct.err_code: 702,
                 ct.err_msg: "can not alter query_mode if the collection already has a vector index"}
        self.drop_collection_properties(client, col,
                                        property_keys=["query_mode"],
                                        check_task=CheckTasks.err_res,
                                        check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_empty_collection_search(self):
        """
        target: verify search on empty large_topk collection returns 0 results
        method: create collection with property, build FLAT index, load, search without inserting
        expected: 0 results returned, no exception
        """
        client = self._client()
        col = self._setup_col(client, enable_large_topk=True, nb=0)

        res = client.search(col, data=cf.gen_vectors(default_nq, default_dim),
                            limit=default_limit, anns_field=vec_field)
        for hits in res:
            assert len(hits) == 0, f"Empty collection should return 0 results, got {len(hits)}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_then_drop_property_roundtrip(self):
        """
        target: verify adding then dropping query_mode property restores normal behavior
        method:
            1. create plain collection, build FLAT index, insert data
            2. drop index → alter_collection_properties (add) → rebuild index → search
            3. drop index → drop_collection_properties → rebuild index → search
            4. verify property absent after final drop
        expected: both searches return default_limit results; property absent after drop
        """
        client = self._client()
        col = self._setup_col(client, enable_large_topk=False)
        vectors = cf.gen_vectors(default_nq, default_dim)

        # Phase 1: add property
        self.release_collection(client, col)
        self.drop_index(client, col, vec_field)
        self.alter_collection_properties(client, col,
                                         properties={"query_mode": "large_topk"})
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(vec_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, col, index_params)
        self.load_collection(client, col)
        self.search(client, col, data=vectors, anns_field=vec_field,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "limit": default_limit,
                                 "metric": "L2"})

        # Phase 2: drop property
        self.release_collection(client, col)
        self.drop_index(client, col, vec_field)
        self.drop_collection_properties(client, col, property_keys=["query_mode"])
        self.create_index(client, col, index_params)
        self.load_collection(client, col)
        self.search(client, col, data=vectors, anns_field=vec_field,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "limit": default_limit,
                                 "metric": "L2"})

        desc = client.describe_collection(col)
        assert "query_mode" not in desc.get("properties", {})

    @pytest.mark.tags(CaseLabel.L1)
    def test_property_persistence_after_reload(self):
        """
        target: verify query_mode=large_topk persists after release + load
        method:
            1. create collection with property, insert total > large_topk_first rows, flush
            2. release → load
            3. describe_collection to verify property present
            4. search with limit=large_topk_first, verify large_topk_first results returned
        expected: property present; topk=large_topk_first returns exactly large_topk_first results
        """
        client = self._client()
        nb_total = large_topk_first + 1000
        col = self._setup_col(client, enable_large_topk=True, nb=nb_total)

        self.release_collection(client, col)
        self.load_collection(client, col)

        desc = client.describe_collection(col)
        assert desc.get("properties", {}).get("query_mode") == "large_topk", \
            f"property lost after reload: {desc.get('properties')}"

        self.search(client, col, data=cf.gen_vectors(default_nq, default_dim),
                    anns_field=vec_field, limit=large_topk_first,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "limit": large_topk_first,
                                 "metric": "L2"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_large_topk_growing_segment(self):
        """
        target: verify large_topk works on growing segments (before flush)
        method:
            1. create collection with property, build FLAT index, load
            2. insert default_nb rows WITHOUT flush (growing segment)
            3. search with limit=100 — should return results from growing segment
        expected: search succeeds and returns hits; no error about topk limit
        """
        client = self._client()
        col = self._setup_col(client, enable_large_topk=True, nb=0)

        # Insert without flush → growing segment
        rows = [{vec_field: cf.gen_vectors(1, default_dim)[0]} for _ in range(default_nb)]
        self.insert(client, col, rows)
        # No flush — data stays in growing segment

        self.search(client, col, data=cf.gen_vectors(default_nq, default_dim),
                    anns_field=vec_field, limit=100,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "limit": 100,
                                 "metric": "L2"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_invalid_property_value(self):
        """
        target: verify invalid query_mode value is rejected at collection creation
        method: create_collection with properties={"query_mode": "invalid_mode"}
        expected: MilvusException with message containing valid values hint
        """
        client = self._client()
        col = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(vec_field, DataType.FLOAT_VECTOR, dim=default_dim)

        error = {ct.err_code: 65535,
                 ct.err_msg: 'invalid query_mode value "invalid_mode", valid values: [large_topk]'}
        self.create_collection(client, col, schema=schema,
                               properties={"query_mode": "invalid_mode"},
                               check_task=CheckTasks.err_res,
                               check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("value", ["LARGE_TOPK", "Large_TopK", "large_TOPK"])
    def test_query_mode_value_case_insensitive(self, value):
        """
        target: document that query_mode value is case-insensitive (known inconsistency)
        method:
            1. create collection with properties={"query_mode": value} (non-lowercase value)
            2. describe_collection to verify stored value equals input (original casing preserved)
            3. search with limit=large_topk_first to verify large_topk is actually enabled
        expected: value accepted, stored as-is, large_topk functionality enabled
        note: inconsistency — error message says "valid values: [large_topk]" but uppercase works
              tracked in https://github.com/milvus-io/milvus/issues/48725
        """
        client = self._client()
        col = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(vec_field, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, col, schema=schema,
                               properties={"query_mode": value}, force_teardown=True)

        desc = client.describe_collection(col)
        stored = desc.get("properties", {}).get("query_mode")
        assert stored == value, f"stored={stored!r}, expected={value!r}"

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(vec_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, col, index_params)
        self.load_collection(client, col)
        nb = ct.default_nb  # > 2048 to trigger index build
        rows = [{vec_field: cf.gen_vectors(1, default_dim)[0]} for _ in range(nb)]
        self.insert(client, col, rows)
        self.flush(client, col)

        self.search(client, col, data=cf.gen_vectors(default_nq, default_dim),
                    anns_field=vec_field, limit=large_topk_first,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "limit": nb,
                                 "metric": "L2"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("key", ["QUERY_MODE", "Query_Mode", "query_MODE"])
    def test_query_mode_key_case_sensitive(self, key):
        """
        target: document that query_mode key is case-sensitive
        method:
            1. create collection with properties={key: "large_topk"} (wrong-cased key)
            2. verify the wrong-cased key is stored as unknown custom property
            3. verify topk=large_topk_first is rejected (large_topk not enabled)
        expected: wrong-cased key stored; topk>16384 rejected with error
        note: inconsistency with value case-insensitivity
              tracked in https://github.com/milvus-io/milvus/issues/48725
        """
        client = self._client()
        col = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(vec_field, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, col, schema=schema,
                               properties={key: "large_topk"}, force_teardown=True)

        desc = client.describe_collection(col)
        props = desc.get("properties", {})
        assert key in props, f"key {key!r} not stored: {props}"
        assert "query_mode" not in props, \
            f"query_mode should not be recognized: {props}"

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(vec_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, col, index_params)
        self.load_collection(client, col)
        rows = [{vec_field: cf.gen_vectors(1, default_dim)[0]} for _ in range(default_nb)]
        self.insert(client, col, rows)
        self.flush(client, col)

        error = {ct.err_code: 65535,
                 ct.err_msg: f"topk [{large_topk_first}] is invalid, it should be in range [1, 16384]"}
        self.search(client, col, data=cf.gen_vectors(default_nq, default_dim),
                    anns_field=vec_field, limit=large_topk_first,
                    check_task=CheckTasks.err_res,
                    check_items=error)

