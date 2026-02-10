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


@pytest.mark.xdist_group("TestMilvusClientThreeValuedLogicIssue46972")
class TestMilvusClientThreeValuedLogicIssue46972(TestMilvusClientV2Base):
    """
    Test cases for issue #46972: False OR False = True bug in complex JSON expressions.

    Related Issue: https://github.com/milvus-io/milvus/issues/46972

    This tests the critical logic error where complex OR expressions with
    nullable fields and JSON fields incorrectly evaluate False OR False as True.
    """

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = cf.gen_unique_str(f"{prefix}_issue46972")
        self.pk_field = "id"
        self.vector_field = "vector"
        self.dim = 128
        self.datas = []

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        """
        Initialize collection with nullable fields and JSON field for issue #46972.
        """
        client = self._client()

        # Create schema matching the issue reproduction case
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(self.pk_field, DataType.INT64, is_primary=True)
        schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)
        schema.add_field("c0", DataType.DOUBLE, nullable=True)
        schema.add_field("c1", DataType.BOOL, nullable=True)
        schema.add_field("c2", DataType.INT64, nullable=True)
        schema.add_field("c3", DataType.DOUBLE, nullable=True)
        schema.add_field("c4", DataType.DOUBLE, nullable=True)
        schema.add_field("c5", DataType.BOOL, nullable=True)
        schema.add_field("c6", DataType.INT64, nullable=True)
        schema.add_field("c7", DataType.BOOL, nullable=True)
        schema.add_field("c8", DataType.VARCHAR, max_length=512, nullable=True)
        schema.add_field("c9", DataType.DOUBLE, nullable=True)
        schema.add_field("c10", DataType.VARCHAR, max_length=512, nullable=True)
        schema.add_field("c11", DataType.INT64, nullable=True)
        schema.add_field("c12", DataType.BOOL, nullable=True)
        schema.add_field("c13", DataType.INT64, nullable=True)
        schema.add_field("c14", DataType.DOUBLE, nullable=True)
        schema.add_field("c15", DataType.DOUBLE, nullable=True)
        schema.add_field("c16", DataType.DOUBLE, nullable=True)
        schema.add_field("c17", DataType.BOOL, nullable=True)
        schema.add_field("meta_json", DataType.JSON, nullable=True)
        schema.add_field("tags_array", DataType.ARRAY, element_type=DataType.INT64,
                         max_capacity=50, nullable=True)

        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)
        log.info(f"Created collection for issue #46972: {self.collection_name}")

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(self.vector_field, index_type="HNSW", metric_type="L2",
                               params={"M": 32, "efConstruction": 256})
        self.create_index(client, self.collection_name, index_params=index_params)

        # Insert the exact test data from the issue
        self.datas = [{
            'id': 1051,
            'vector': [0.1] * self.dim,
            'c0': None, 'c1': True, 'c2': 55943, 'c3': 2379.7519128726576, 'c4': None, 'c5': True,
            'c6': -57942, 'c7': False, 'c8': 'Vaxr5xzbWPHJazy9loD', 'c9': 1682.7331496087677,
            'c10': None, 'c11': -62195, 'c12': False, 'c13': 71210, 'c14': None,
            'c15': 2884.9004720171306, 'c16': 4866.809897346821, 'c17': True,
            'meta_json': {
                'price': 561, 'color': 'Green', 'active': False,
                'config': {'version': 7}, 'history': [7, 20, 88], 'random_payload': 12168
            },
            'tags_array': [73]
        }]

        self.insert(client, self.collection_name, self.datas)
        self.flush(client, self.collection_name)
        self.load_collection(client, self.collection_name)
        log.info(f"Inserted {len(self.datas)} rows for issue #46972 test")

        def teardown():
            self.drop_collection(self._client(), self.collection_name)
            log.info(f"Dropped collection: {self.collection_name}")

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    def test_issue_46972_false_or_false_complex_json(self):
        """
        target: test that False OR False = False (not True) with complex JSON expressions
        method: use the exact expressions from issue #46972
        expected: left expr is False, right expr is False, combined is False

        This is the critical bug fixed in PR #47333: complex expressions with
        JSON fields and nullable fields incorrectly evaluated (False OR False) as True.
        """
        client = self._client()

        # Block A: Left Expression (should be False for test data)
        expr_left = """
(((meta_json["config"]["version"] == 9 or (meta_json["history"][0] > 49 or (meta_json["history"][0] > 47 and (meta_json["price"] > 294 and meta_json["price"] < 379)))) and (((meta_json["config"]["version"] == 7 or meta_json["history"][0] > 27) or (tags_array is not null or null is null)) or c17 == false)) or ((((c14 <= 863.28694295187 or (c10 != "kaKmPWPbAaEFnHzX" and c10 != "ZmBmv")) and ((c4 > 1113.2711377477458 or c13 >= -76839) or (meta_json["config"]["version"] == 3 or (meta_json["price"] > 150 and meta_json["price"] < 237)))) and c7 == false) and ((((c1 is not null and null is null) or (c12 == false and meta_json["config"]["version"] == 4)) or (((meta_json["active"] == true and meta_json["color"] == "Blue") and c12 == true) and (c6 is not null or c13 == 180192))) or (c4 < 105376.953125 or ((c15 <= 3484.876420871185 or c1 == false) or (meta_json["config"]["version"] == 9 and c10 != "OjcbvwxZ5LfV2PZNPgS7"))))))
"""

        # Block B: Right Expression (should be False for test data)
        expr_right = """
((((((c17 == false or exists(meta_json["non_exist"])) or c14 <= 5363.7872388701035) or meta_json["history"][0] > 72) or (((c4 is not null and c17 == true) and ((c2 < 60835 or c1 is null) or (c5 == false or meta_json["history"][0] > 64))) and ((c6 >= -56376 or c16 is not null) or ((c4 >= 812.9318963654309 and c16 >= 2907.7772038042867) and (meta_json["price"] > 449 and meta_json["price"] < 624))))) or (((((null is not null and c13 < 180192) or (c13 < -71746 or c17 == true)) and ((meta_json["price"] > 407 and meta_json["price"] < 521) and (c8 like "w%" or c14 > 3021.9496428003613))) and (((c12 == false or (meta_json["price"] > 412 and meta_json["price"] < 571)) or (c9 < 1264.220988053753 or c6 != 2063)) or c5 == false)) and (meta_json["active"] == true and meta_json["color"] == "Red"))) or ((((c13 != 54759 and (c7 is null and (c17 == true or c9 <= 1067.5165787120216))) and (((c11 != 36936 or meta_json["history"][0] > 59) or ((meta_json["price"] > 354 and meta_json["price"] < 528) and c9 >= 135.84002581554114)) and ((c0 > 105381.9375 or exists(meta_json["non_exist"])) or (c12 == true or meta_json is null)))) or ((((c14 >= 2547.2285277180335 and c1 == false) and (meta_json["history"][0] > 37 or json_contains(meta_json["k_11"], "o"))) and ((c9 < 2869.0647504243566 and c4 > 449.2640493406897) or (meta_json["config"]["version"] == 9 and c16 < 3626.0660282789318))) and (((c11 <= -57792 or (meta_json["price"] > 320 and meta_json["price"] < 488)) and (meta_json["active"] == false and c3 >= 105383.3671875)) or ((c13 >= -32496 and c5 == true) and (meta_json["active"] == true and meta_json["color"] == "Blue"))))) and c13 <= 180192))
"""

        # Combined expression
        expr_combined = f"({expr_left}) OR ({expr_right})"

        # Query with left expression
        res_left = self.query(client, self.collection_name, filter=expr_left,
                              output_fields=[self.pk_field])[0]
        is_left_true = len(res_left) > 0

        # Query with right expression
        res_right = self.query(client, self.collection_name, filter=expr_right,
                               output_fields=[self.pk_field])[0]
        is_right_true = len(res_right) > 0

        # Query with combined expression
        res_combined = self.query(client, self.collection_name, filter=expr_combined,
                                  output_fields=[self.pk_field])[0]
        is_combined_true = len(res_combined) > 0

        log.info(f"Expr Left result: {is_left_true} (hits: {len(res_left)})")
        log.info(f"Expr Right result: {is_right_true} (hits: {len(res_right)})")
        log.info(f"Combined result: {is_combined_true} (hits: {len(res_combined)})")

        # Verify: For this data, both expressions should be False
        assert not is_left_true, "Left expression should be False for test data"
        assert not is_right_true, "Right expression should be False for test data"

        # The critical check: False OR False should NOT be True
        assert not is_combined_true, \
            f"BUG: (False OR False) evaluated to True! Combined result has {len(res_combined)} hits"

    @pytest.mark.tags(CaseLabel.L2)
    def test_issue_46972_simplified_false_or_false(self):
        """
        target: test simplified False OR False scenario with nullable fields
        method: create conditions that are both False due to NULL comparisons
        expected: combined OR expression is also False

        Verifies the fix handles NULL comparisons correctly in OR expressions.
        """
        client = self._client()

        # For the test data: c0=NULL, c4=NULL, c10=NULL, c14=NULL
        # Expression 1: c0 > 100 (NULL > 100 = UNKNOWN, treated as False)
        # Expression 2: c4 > 100 (NULL > 100 = UNKNOWN, treated as False)
        expr_left = "c0 > 100"
        expr_right = "c4 > 100"
        expr_combined = f"({expr_left}) OR ({expr_right})"

        res_left = self.query(client, self.collection_name, filter=expr_left,
                              output_fields=[self.pk_field])[0]
        res_right = self.query(client, self.collection_name, filter=expr_right,
                               output_fields=[self.pk_field])[0]
        res_combined = self.query(client, self.collection_name, filter=expr_combined,
                                  output_fields=[self.pk_field])[0]

        log.info(f"Simplified test - Left: {len(res_left)}, Right: {len(res_right)}, Combined: {len(res_combined)}")

        # Both should be empty (NULL comparisons are UNKNOWN)
        assert len(res_left) == 0, "NULL > 100 should return 0 results"
        assert len(res_right) == 0, "NULL > 100 should return 0 results"
        # Combined should also be empty
        assert len(res_combined) == 0, \
            "Combined (NULL > 100) OR (NULL > 100) should return 0 results"
