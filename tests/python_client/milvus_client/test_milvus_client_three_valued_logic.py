"""
Test cases for three-valued logic with nullable fields.

Related PR: https://github.com/milvus-io/milvus/pull/47333
Related Issue: https://github.com/milvus-io/milvus/issues/46820

These tests verify correct behavior of:
1. IS NULL / IS NOT NULL expressions
2. NOT operator with NULL values
3. AND/OR short-circuit logic with NULL values
4. De Morgan's law equivalences with NULL
5. Search with filter expressions involving NULL
"""

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType


prefix = "three_valued_logic"
default_dim = ct.default_dim


@pytest.mark.xdist_group("TestMilvusClientThreeValuedLogic")
class TestMilvusClientThreeValuedLogic(TestMilvusClientV2Base):
    """
    Test cases for three-valued logic with nullable fields.

    Uses shared collection pattern - collection created once in prepare_collection
    and reused by all tests (read-only verification).
    """

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = cf.gen_unique_str(prefix)
        self.pk_field = "id"
        self.vector_field = "vec"
        self.nullable_a_field = "nullable_a"
        self.nullable_b_field = "nullable_b"
        self.non_null_field = "non_null_int"
        self.dim = default_dim
        self.nb = 40
        self.datas = []

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        """
        Initialize collection with nullable fields before test class runs.

        Data pattern:
        - nullable_a: NULL for IDs 0-19, NOT NULL (id*10) for IDs 20-39
        - nullable_b: NULL for IDs 0-9 and 20-29, NOT NULL (id*100) for IDs 10-19 and 30-39
        """
        client = self._client()

        # Create schema with nullable fields
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(self.pk_field, DataType.INT64, is_primary=True)
        schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)
        schema.add_field(self.nullable_a_field, DataType.INT64, nullable=True)
        schema.add_field(self.nullable_b_field, DataType.INT64, nullable=True)
        schema.add_field(self.non_null_field, DataType.INT64)

        # Create collection
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)
        log.info(f"Created shared collection: {self.collection_name}")

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(self.vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, self.collection_name, index_params=index_params)

        # Generate and insert test data
        vectors = cf.gen_vectors(self.nb, self.dim)
        for i in range(self.nb):
            row = {
                self.pk_field: i,
                self.vector_field: vectors[i],
                self.non_null_field: i,
                # nullable_a: NULL for IDs 0-19, NOT NULL (id*10) for IDs 20-39
                self.nullable_a_field: None if i < 20 else i * 10,
                # nullable_b: NULL for IDs 0-9 and 20-29, NOT NULL (id*100) for IDs 10-19 and 30-39
                self.nullable_b_field: None if (i < 10 or 20 <= i < 30) else i * 100,
            }
            self.datas.append(row)

        self.insert(client, self.collection_name, self.datas)
        self.flush(client, self.collection_name)
        self.load_collection(client, self.collection_name)
        log.info(f"Inserted {self.nb} rows into shared collection")

        def teardown():
            self.drop_collection(self._client(), self.collection_name)
            log.info(f"Dropped shared collection: {self.collection_name}")

        request.addfinalizer(teardown)

    # ========================================================================
    # Helper Methods - Calculate Expected Results from Data
    # ========================================================================

    def get_ids_where(self, predicate):
        """Get sorted list of IDs where predicate(row) is True."""
        return sorted([row[self.pk_field] for row in self.datas if predicate(row)])

    def ids_a_is_null(self):
        """IDs where nullable_a IS NULL."""
        return self.get_ids_where(lambda r: r[self.nullable_a_field] is None)

    def ids_a_is_not_null(self):
        """IDs where nullable_a IS NOT NULL."""
        return self.get_ids_where(lambda r: r[self.nullable_a_field] is not None)

    def ids_b_is_null(self):
        """IDs where nullable_b IS NULL."""
        return self.get_ids_where(lambda r: r[self.nullable_b_field] is None)

    def ids_b_is_not_null(self):
        """IDs where nullable_b IS NOT NULL."""
        return self.get_ids_where(lambda r: r[self.nullable_b_field] is not None)

    def ids_both_null(self):
        """IDs where both nullable_a AND nullable_b are NULL."""
        return self.get_ids_where(
            lambda r: r[self.nullable_a_field] is None and r[self.nullable_b_field] is None
        )

    def ids_either_null(self):
        """IDs where nullable_a OR nullable_b is NULL."""
        return self.get_ids_where(
            lambda r: r[self.nullable_a_field] is None or r[self.nullable_b_field] is None
        )

    def ids_both_not_null(self):
        """IDs where both nullable_a AND nullable_b are NOT NULL."""
        return self.get_ids_where(
            lambda r: r[self.nullable_a_field] is not None and r[self.nullable_b_field] is not None
        )

    def ids_a_not_null_and_gt(self, value):
        """IDs where nullable_a IS NOT NULL AND nullable_a > value."""
        return self.get_ids_where(
            lambda r: r[self.nullable_a_field] is not None and r[self.nullable_a_field] > value
        )

    def ids_a_null_or_a_gte(self, value):
        """IDs where nullable_a IS NULL OR nullable_a >= value."""
        return self.get_ids_where(
            lambda r: r[self.nullable_a_field] is None or r[self.nullable_a_field] >= value
        )

    def ids_a_null_or_a_gt(self, value):
        """IDs where nullable_a IS NULL OR nullable_a > value."""
        return self.get_ids_where(
            lambda r: r[self.nullable_a_field] is None or r[self.nullable_a_field] > value
        )

    def ids_complex_nested(self):
        """IDs for NOT (((A IS NOT NULL) AND (A > 200)) AND (B IS NOT NULL))."""
        # Inner: (A NOT NULL AND A > 200) AND B NOT NULL
        inner = self.get_ids_where(
            lambda r: (r[self.nullable_a_field] is not None and r[self.nullable_a_field] > 200)
                      and r[self.nullable_b_field] is not None
        )
        # NOT of inner = all IDs except inner
        all_ids = set(row[self.pk_field] for row in self.datas)
        return sorted(all_ids - set(inner))

    def ids_pk_lt_and_a_null(self, pk_limit):
        """IDs where id < pk_limit AND nullable_a IS NULL."""
        return self.get_ids_where(
            lambda r: r[self.pk_field] < pk_limit and r[self.nullable_a_field] is None
        )

    # ========================================================================
    # Basic IS NULL / IS NOT NULL Tests
    # ========================================================================

    @pytest.mark.tags(CaseLabel.L1)
    def test_is_null_basic(self):
        """
        target: test basic IS NULL expression
        method: query with "nullable_a IS NULL" filter
        expected: return rows where nullable_a is NULL
        """
        client = self._client()
        res = self.query(client, self.collection_name,
                        filter=f"{self.nullable_a_field} IS NULL",
                        output_fields=[self.pk_field])[0]

        expected_ids = self.ids_a_is_null()
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_is_not_null_basic(self):
        """
        target: test basic IS NOT NULL expression
        method: query with "nullable_a IS NOT NULL" filter
        expected: return rows where nullable_a is NOT NULL
        """
        client = self._client()
        res = self.query(client, self.collection_name,
                        filter=f"{self.nullable_a_field} IS NOT NULL",
                        output_fields=[self.pk_field])[0]

        expected_ids = self.ids_a_is_not_null()
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    # ========================================================================
    # NOT Equivalence Tests (Core Bug Fix Verification)
    # ========================================================================

    @pytest.mark.tags(CaseLabel.L1)
    def test_not_is_not_null_equivalence(self):
        """
        target: test NOT (field IS NOT NULL) equals field IS NULL
        method: compare results of both expressions
        expected: identical results

        This is the critical bug fixed in PR #47333.
        """
        client = self._client()
        res1 = self.query(client, self.collection_name,
                         filter=f"{self.nullable_a_field} IS NULL",
                         output_fields=[self.pk_field])[0]

        res2 = self.query(client, self.collection_name,
                         filter=f"NOT ({self.nullable_a_field} IS NOT NULL)",
                         output_fields=[self.pk_field])[0]

        expected_ids = self.ids_a_is_null()
        ids1 = sorted([r[self.pk_field] for r in res1])
        ids2 = sorted([r[self.pk_field] for r in res2])
        assert ids1 == ids2 == expected_ids, f"Expected {expected_ids}, got {ids1} and {ids2}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_not_is_null_equivalence(self):
        """
        target: test NOT (field IS NULL) equals field IS NOT NULL
        method: compare results of both expressions
        expected: identical results
        """
        client = self._client()
        res1 = self.query(client, self.collection_name,
                         filter=f"{self.nullable_a_field} IS NOT NULL",
                         output_fields=[self.pk_field])[0]

        res2 = self.query(client, self.collection_name,
                         filter=f"NOT ({self.nullable_a_field} IS NULL)",
                         output_fields=[self.pk_field])[0]

        expected_ids = self.ids_a_is_not_null()
        ids1 = sorted([r[self.pk_field] for r in res1])
        ids2 = sorted([r[self.pk_field] for r in res2])
        assert ids1 == ids2 == expected_ids, f"Expected {expected_ids}, got {ids1} and {ids2}"

    # ========================================================================
    # Multiple NOT Tests
    # ========================================================================

    @pytest.mark.tags(CaseLabel.L2)
    def test_double_not_cancellation(self):
        """
        target: test NOT (NOT (expr)) equals expr
        method: query with NOT (NOT (nullable_a IS NULL))
        expected: same as IS NULL
        """
        client = self._client()
        res = self.query(client, self.collection_name,
                        filter=f"NOT (NOT ({self.nullable_a_field} IS NULL))",
                        output_fields=[self.pk_field])[0]

        expected_ids = self.ids_a_is_null()
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_triple_not(self):
        """
        target: test triple NOT operation
        method: query with NOT (NOT (NOT (nullable_a IS NULL)))
        expected: same as NOT (IS NULL) = IS NOT NULL
        """
        client = self._client()
        res = self.query(client, self.collection_name,
                        filter=f"NOT (NOT (NOT ({self.nullable_a_field} IS NULL)))",
                        output_fields=[self.pk_field])[0]

        expected_ids = self.ids_a_is_not_null()
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    # ========================================================================
    # Multi-Field NULL Tests
    # ========================================================================

    @pytest.mark.tags(CaseLabel.L1)
    def test_and_both_null_fields(self):
        """
        target: test AND with both fields being NULL
        method: query (nullable_a IS NULL) AND (nullable_b IS NULL)
        expected: rows where both fields are NULL
        """
        client = self._client()
        res = self.query(client, self.collection_name,
                        filter=f"({self.nullable_a_field} IS NULL) AND ({self.nullable_b_field} IS NULL)",
                        output_fields=[self.pk_field])[0]

        expected_ids = self.ids_both_null()
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_or_either_null_field(self):
        """
        target: test OR with either field being NULL
        method: query (nullable_a IS NULL) OR (nullable_b IS NULL)
        expected: rows where at least one field is NULL
        """
        client = self._client()
        res = self.query(client, self.collection_name,
                        filter=f"({self.nullable_a_field} IS NULL) OR ({self.nullable_b_field} IS NULL)",
                        output_fields=[self.pk_field])[0]

        expected_ids = self.ids_either_null()
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    # ========================================================================
    # De Morgan's Law Tests
    # ========================================================================

    @pytest.mark.tags(CaseLabel.L1)
    def test_de_morgan_not_and_to_or(self):
        """
        target: test De Morgan's law: NOT (A AND B) = (NOT A) OR (NOT B)
        method: NOT ((A IS NOT NULL) AND (B IS NOT NULL))
        expected: equivalent to (A IS NULL) OR (B IS NULL)

        Tests AND short-circuit fix in PR #47333.
        """
        client = self._client()
        res = self.query(client, self.collection_name,
                        filter=f"NOT (({self.nullable_a_field} IS NOT NULL) AND ({self.nullable_b_field} IS NOT NULL))",
                        output_fields=[self.pk_field])[0]

        # NOT (both NOT NULL) = at least one is NULL
        expected_ids = self.ids_either_null()
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_de_morgan_not_or_to_and(self):
        """
        target: test De Morgan's law: NOT (A OR B) = (NOT A) AND (NOT B)
        method: NOT ((A IS NULL) OR (B IS NULL))
        expected: equivalent to (A IS NOT NULL) AND (B IS NOT NULL)
        """
        client = self._client()
        res = self.query(client, self.collection_name,
                        filter=f"NOT (({self.nullable_a_field} IS NULL) OR ({self.nullable_b_field} IS NULL))",
                        output_fields=[self.pk_field])[0]

        # NOT (either NULL) = both NOT NULL
        expected_ids = self.ids_both_not_null()
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    # ========================================================================
    # NULL with Value Comparison Tests
    # ========================================================================

    @pytest.mark.tags(CaseLabel.L2)
    def test_null_with_value_comparison(self):
        """
        target: test NULL check combined with value comparison
        method: (nullable_a IS NOT NULL) AND (nullable_a > 250)
        expected: rows where nullable_a is not null and > 250
        """
        client = self._client()
        threshold = 250
        res = self.query(client, self.collection_name,
                        filter=f"({self.nullable_a_field} IS NOT NULL) AND ({self.nullable_a_field} > {threshold})",
                        output_fields=[self.pk_field])[0]

        expected_ids = self.ids_a_not_null_and_gt(threshold)
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_not_combined_null_and_value(self):
        """
        target: test NOT on combined NULL and value comparison
        method: NOT ((nullable_a IS NOT NULL) AND (nullable_a < 250))
        expected: (A IS NULL) OR (A >= 250)
        """
        client = self._client()
        threshold = 250
        res = self.query(client, self.collection_name,
                        filter=f"NOT (({self.nullable_a_field} IS NOT NULL) AND ({self.nullable_a_field} < {threshold}))",
                        output_fields=[self.pk_field])[0]

        expected_ids = self.ids_a_null_or_a_gte(threshold)
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_complex_nested_not(self):
        """
        target: test complex nested NOT expression
        method: NOT (((A IS NOT NULL) AND (A > 200)) AND (B IS NOT NULL))
        expected: complement of rows matching inner condition
        """
        client = self._client()
        res = self.query(client, self.collection_name,
                        filter=f"NOT ((({self.nullable_a_field} IS NOT NULL) AND ({self.nullable_a_field} > 200)) AND ({self.nullable_b_field} IS NOT NULL))",
                        output_fields=[self.pk_field])[0]

        expected_ids = self.ids_complex_nested()
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    # ========================================================================
    # Mixed Filter Tests
    # ========================================================================

    @pytest.mark.tags(CaseLabel.L1)
    def test_and_with_id_filter(self):
        """
        target: test AND with id filter and NULL check
        method: (id < 15) AND (nullable_a IS NULL)
        expected: rows where id < 15 and nullable_a is NULL
        """
        client = self._client()
        pk_limit = 15
        res = self.query(client, self.collection_name,
                        filter=f"({self.pk_field} < {pk_limit}) AND ({self.nullable_a_field} IS NULL)",
                        output_fields=[self.pk_field])[0]

        expected_ids = self.ids_pk_lt_and_a_null(pk_limit)
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_or_with_null_and_value(self):
        """
        target: test OR with NULL check and value comparison
        method: (nullable_a IS NULL) OR (nullable_a > 350)
        expected: rows where nullable_a is NULL or > 350
        """
        client = self._client()
        threshold = 350
        res = self.query(client, self.collection_name,
                        filter=f"({self.nullable_a_field} IS NULL) OR ({self.nullable_a_field} > {threshold})",
                        output_fields=[self.pk_field])[0]

        expected_ids = self.ids_a_null_or_a_gt(threshold)
        actual_ids = sorted([r[self.pk_field] for r in res])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    # ========================================================================
    # Search with NULL Filter Tests
    # ========================================================================

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_not_is_not_null_filter(self):
        """
        target: test search with NOT (field IS NOT NULL) filter
        method: search with filter="NOT (nullable_a IS NOT NULL)"
        expected: results only from rows where nullable_a IS NULL
        """
        client = self._client()
        vectors = cf.gen_vectors(1, self.dim)
        search_res = self.search(client, self.collection_name, vectors,
                                 filter=f"NOT ({self.nullable_a_field} IS NOT NULL)",
                                 anns_field=self.vector_field,
                                 limit=100,
                                 output_fields=[self.pk_field])[0]

        expected_ids = set(self.ids_a_is_null())
        # search_res[0] is the hits for the first query vector
        actual_ids = set(hit.get("id") for hit in search_res[0])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_complex_null_filter(self):
        """
        target: test search with complex NULL filter
        method: search with NOT ((A IS NOT NULL) AND (B IS NOT NULL))
        expected: results from rows where at least one field is NULL
        """
        client = self._client()
        vectors = cf.gen_vectors(1, self.dim)
        search_res = self.search(client, self.collection_name, vectors,
                                 filter=f"NOT (({self.nullable_a_field} IS NOT NULL) AND ({self.nullable_b_field} IS NOT NULL))",
                                 anns_field=self.vector_field,
                                 limit=100,
                                 output_fields=[self.pk_field])[0]

        expected_ids = set(self.ids_either_null())
        # search_res[0] is the hits for the first query vector
        actual_ids = set(hit.get("id") for hit in search_res[0])
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"
