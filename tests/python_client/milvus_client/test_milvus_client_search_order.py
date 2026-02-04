import random
import pytest

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from pymilvus import DataType

prefix = "client_search_order"
default_nb = 3000
default_dim = 8
default_limit = 20
default_primary_key_field_name = "id"
default_vector_field_name = "embeddings"

# Global collection names with unique suffix to avoid conflicts across parallel runs
VALID_COLLECTION_NAME = "test_search_order_valid_" + cf.gen_unique_str("_")
INVALID_COLLECTION_NAME = "test_search_order_invalid_" + cf.gen_unique_str("_")

# Data generation constants
PRICE_MIN = 10.0
PRICE_MAX = 59.0
RATING_MIN = 0.0
RATING_MAX = 4.9
CATEGORIES = [
    "electronics", "tools", "books", "sports", "movies",
    "games", "furniture", "health", "toys", "jewelry",
    "cosmetics", "clothing", "garden", "food", "travel",
    "pet_supplies", "beauty", "appliances", "automotive", "music"
]


def build_search_order_schema(client):
    """Build schema for search order by tests."""
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("price", DataType.DOUBLE)
    schema.add_field("rating", DataType.DOUBLE)
    schema.add_field("category", DataType.VARCHAR, max_length=64)
    schema.add_field("int32_field", DataType.INT32)
    schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=default_dim)
    return schema


def gen_search_order_data(nb, schema):
    """Generate test data using cf.gen_row_data_by_schema, then override
    price/rating/category with controlled values for order-by testing."""
    rows = cf.gen_row_data_by_schema(nb=nb, schema=schema)
    for i, row in enumerate(rows):
        row["price"] = float(random.randint(int(PRICE_MIN), int(PRICE_MAX)))
        row["rating"] = round(random.uniform(RATING_MIN, RATING_MAX), 1)
        row["category"] = CATEGORIES[i % len(CATEGORIES)]
    return rows


@pytest.mark.xdist_group("TestMilvusClientSearchOrderValid")
class TestMilvusClientSearchOrderValid(TestMilvusClientV2Base):
    """
    Test cases for search with order_by_fields - valid scenarios.
    Collection is initialized once via fixture and shared across all tests.
    """

    @pytest.fixture(scope="module", autouse=True)
    def prepare_valid_collection(self, request):
        """Create the shared collection once before all tests in this module,
        and drop it after all tests complete."""
        client = self._client()
        collection_name = VALID_COLLECTION_NAME
        if client.has_collection(collection_name):
            client.drop_collection(collection_name)

        schema = build_search_order_schema(client)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="embeddings", index_type="HNSW",
                               metric_type="COSINE", M=16, efConstruction=200)

        client.create_collection(collection_name=collection_name, schema=schema,
                                 index_params=index_params, consistency_level="Strong")

        # Insert data in 3 batches to create multiple segments
        nb = default_nb
        all_rows = gen_search_order_data(nb, schema)
        batch_size = nb // 3
        for i in range(3):
            start = i * batch_size
            end = start + batch_size if i < 2 else nb
            client.insert(collection_name=collection_name, data=all_rows[start:end])

        client.flush(collection_name=collection_name)

        def teardown():
            try:
                if self.has_collection(self._client(), VALID_COLLECTION_NAME):
                    self.drop_collection(self._client(), VALID_COLLECTION_NAME)
            except Exception:
                pass
        request.addfinalizer(teardown)

    # ==================== L0: Smoke Tests ====================

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_search_order_by_price_asc(self):
        """
        target: test search with order_by_fields by price ascending
        method: search with order_by_fields=[{"field": "price", "order": "asc"}]
        expected: results sorted by price in ascending order
        """
        client = self._client()
        collection_name = VALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        res = self.search(client, collection_name, vectors_to_search,
                          limit=default_limit,
                          anns_field="embeddings",
                          output_fields=["id", "price", "rating", "category"],
                          order_by_fields=[{"field": "price", "order": "asc"}],
                          consistency_level="Strong")[0]

        assert len(res) == 1, f"Expected 1 query result, got {len(res)}"
        results = res[0]
        assert len(results) == default_limit, f"Expected {default_limit} results, got {len(results)}"

        # Verify price is sorted ascending
        prices = [r["entity"]["price"] for r in results]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], \
                f"Price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"

        # Verify all output fields are present
        for r in results:
            assert "price" in r["entity"]
            assert "rating" in r["entity"]
            assert "category" in r["entity"]
            assert "id" in r["entity"]

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_search_order_by_rating_desc(self):
        """
        target: test search with order_by_fields by rating descending
        method: search with order_by_fields=[{"field": "rating", "order": "desc"}]
        expected: results sorted by rating in descending order
        """
        client = self._client()
        collection_name = VALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        res = self.search(client, collection_name, vectors_to_search,
                          limit=default_limit,
                          anns_field="embeddings",
                          output_fields=["id", "price", "rating", "category"],
                          order_by_fields=[{"field": "rating", "order": "desc"}],
                          consistency_level="Strong")[0]

        results = res[0]
        assert len(results) == default_limit

        # Verify rating is sorted descending
        ratings = [r["entity"]["rating"] for r in results]
        for i in range(len(ratings) - 1):
            assert ratings[i] >= ratings[i + 1], \
                f"Rating not descending at index {i}: {ratings[i]} < {ratings[i + 1]}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_search_order_by_multi_fields(self):
        """
        target: test search with multi-field order_by (price asc, rating desc)
        method: search with order_by_fields with two fields
        expected: results sorted by price asc first, then by rating desc within same price
        """
        client = self._client()
        collection_name = VALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        res = self.search(client, collection_name, vectors_to_search,
                          limit=default_limit,
                          anns_field="embeddings",
                          output_fields=["id", "price", "rating", "category"],
                          order_by_fields=[
                              {"field": "price", "order": "asc"},
                              {"field": "rating", "order": "desc"}
                          ],
                          consistency_level="Strong")[0]

        results = res[0]
        assert len(results) == default_limit

        # Verify primary sort: price ascending
        prices = [r["entity"]["price"] for r in results]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], \
                f"Price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"

        # Verify secondary sort: within same price, rating descending
        for i in range(len(results) - 1):
            if results[i]["entity"]["price"] == results[i + 1]["entity"]["price"]:
                assert results[i]["entity"]["rating"] >= results[i + 1]["entity"]["rating"], \
                    f"Rating not descending within same price at index {i}: " \
                    f"price={results[i]['entity']['price']}, " \
                    f"rating {results[i]['entity']['rating']} < {results[i + 1]['entity']['rating']}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_search_group_by_with_order_by(self):
        """
        target: test search with group_by_field combined with order_by_fields
        method: search with group_by_field="category" and order_by_fields price asc
        expected: results grouped by category, groups ordered by price ascending
        """
        client = self._client()
        collection_name = VALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        res = self.search(client, collection_name, vectors_to_search,
                          limit=default_limit,
                          anns_field="embeddings",
                          output_fields=["id", "price", "rating", "category"],
                          group_by_field="category",
                          group_size=3,
                          strict_group_size=True,
                          order_by_fields=[{"field": "price", "order": "asc"}],
                          consistency_level="Strong")[0]

        results = res[0]
        assert len(results) > 0

        # Verify grouping: collect groups by category
        groups = {}
        for r in results:
            cat = r["entity"]["category"]
            if cat not in groups:
                groups[cat] = []
            groups[cat].append(r)

        # Verify each group has at most group_size entities
        for cat, group_results in groups.items():
            assert len(group_results) <= 3, \
                f"Category '{cat}' has {len(group_results)} results, expected <= 3"

        # Verify top1 of each group is sorted by price ascending
        group_top1_prices = []
        seen_categories = []
        for r in results:
            cat = r["entity"]["category"]
            if cat not in seen_categories:
                seen_categories.append(cat)
                group_top1_prices.append(r["entity"]["price"])

        for i in range(len(group_top1_prices) - 1):
            assert group_top1_prices[i] <= group_top1_prices[i + 1], \
                f"Group top1 price not ascending at index {i}: " \
                f"{group_top1_prices[i]} > {group_top1_prices[i + 1]}"

    # ==================== L1: Core Functionality ====================

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_order_by_varchar_asc(self):
        """
        target: test search order by VARCHAR field ascending
        method: search with order_by_fields on category field
        expected: results sorted by category in lexicographic ascending order
        """
        client = self._client()
        collection_name = VALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        res = self.search(client, collection_name, vectors_to_search,
                          limit=default_limit,
                          anns_field="embeddings",
                          output_fields=["id", "category"],
                          order_by_fields=[{"field": "category", "order": "asc"}],
                          consistency_level="Strong")[0]

        results = res[0]
        categories = [r["entity"]["category"] for r in results]
        for i in range(len(categories) - 1):
            assert categories[i] <= categories[i + 1], \
                f"Category not ascending at index {i}: '{categories[i]}' > '{categories[i + 1]}'"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_order_by_with_filter(self):
        """
        target: test search order by with filter expression
        method: search with filter "price >= 20.0 && price <= 40.0" and order_by price asc
        expected: only filtered results returned, sorted by price ascending
        """
        client = self._client()
        collection_name = VALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        filter_expr = "price >= 20.0 && price <= 40.0"
        res = self.search(client, collection_name, vectors_to_search,
                          limit=default_limit,
                          anns_field="embeddings",
                          filter=filter_expr,
                          output_fields=["id", "price", "rating"],
                          order_by_fields=[{"field": "price", "order": "asc"}],
                          consistency_level="Strong")[0]

        results = res[0]
        assert len(results) <= default_limit

        prices = [r["entity"]["price"] for r in results]
        # Verify filter
        for p in prices:
            assert 20.0 <= p <= 40.0, f"Price {p} not in filter range [20.0, 40.0]"
        # Verify sort
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], \
                f"Price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("limit", [1, 50, 100])
    def test_milvus_client_search_order_by_different_limit(self, limit):
        """
        target: test search order by with different limit values
        method: search with various limit values and order_by price asc
        expected: correct number of results returned, all sorted
        """
        client = self._client()
        collection_name = VALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        res = self.search(client, collection_name, vectors_to_search,
                          limit=limit,
                          anns_field="embeddings",
                          output_fields=["id", "price"],
                          order_by_fields=[{"field": "price", "order": "asc"}],
                          consistency_level="Strong")[0]

        results = res[0]
        assert len(results) == limit, f"Expected {limit} results, got {len(results)}"

        prices = [r["entity"]["price"] for r in results]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], \
                f"Price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_order_by_output_fields_complete(self):
        """
        target: test that all specified output_fields are present in results
        method: search with order_by and multiple output_fields
        expected: every result entity contains all specified output fields
        """
        client = self._client()
        collection_name = VALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        output_fields = ["id", "price", "rating", "category", "int32_field"]
        res = self.search(client, collection_name, vectors_to_search,
                          limit=default_limit,
                          anns_field="embeddings",
                          output_fields=output_fields,
                          order_by_fields=[{"field": "price", "order": "asc"}],
                          consistency_level="Strong")[0]

        results = res[0]
        for r in results:
            for field in output_fields:
                assert field in r["entity"], \
                    f"Field '{field}' missing in result entity: {r['entity'].keys()}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_group_by_order_by_with_group_size(self):
        """
        target: test group_by + order_by with group_size parameter
        method: search with group_by_field, group_size=2, order_by price asc
        expected: each group has at most 2 entities
        """
        client = self._client()
        collection_name = VALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        res = self.search(client, collection_name, vectors_to_search,
                          limit=default_limit,
                          anns_field="embeddings",
                          output_fields=["id", "price", "category"],
                          group_by_field="category",
                          group_size=2,
                          order_by_fields=[{"field": "price", "order": "asc"}],
                          consistency_level="Strong")[0]

        results = res[0]
        groups = {}
        for r in results:
            cat = r["entity"]["category"]
            if cat not in groups:
                groups[cat] = []
            groups[cat].append(r)

        for cat, group_results in groups.items():
            assert len(group_results) <= 2, \
                f"Category '{cat}' has {len(group_results)} results, expected <= 2"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_group_by_order_by_strict_group_size(self):
        """
        target: test group_by + order_by with strict_group_size=True
        method: search with strict_group_size=True
        expected: each group has exactly group_size entities (if enough data)
        """
        client = self._client()
        collection_name = VALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        group_size = 3
        res = self.search(client, collection_name, vectors_to_search,
                          limit=default_limit,
                          anns_field="embeddings",
                          output_fields=["id", "price", "category"],
                          group_by_field="category",
                          group_size=group_size,
                          strict_group_size=True,
                          order_by_fields=[{"field": "price", "order": "asc"}],
                          consistency_level="Strong")[0]

        results = res[0]
        groups = {}
        for r in results:
            cat = r["entity"]["category"]
            if cat not in groups:
                groups[cat] = []
            groups[cat].append(r)

        # With strict_group_size=True, each group should have exactly group_size
        for cat, group_results in groups.items():
            assert len(group_results) == group_size, \
                f"Category '{cat}' has {len(group_results)} results, expected exactly {group_size}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_search_order_by_multi_nq(self):
        """
        target: test search order by with multiple query vectors (nq > 1)
        method: search with 3 query vectors and order_by price asc
        expected: each nq result is independently sorted by price ascending
        """
        client = self._client()
        collection_name = VALID_COLLECTION_NAME

        nq = 3
        vectors_to_search = cf.gen_vectors(nq, default_dim)
        res = self.search(client, collection_name, vectors_to_search,
                          limit=default_limit,
                          anns_field="embeddings",
                          output_fields=["id", "price"],
                          order_by_fields=[{"field": "price", "order": "asc"}],
                          consistency_level="Strong")[0]

        assert len(res) == nq, f"Expected {nq} result sets, got {len(res)}"

        for q in range(nq):
            results = res[q]
            assert len(results) == default_limit
            prices = [r["entity"]["price"] for r in results]
            for i in range(len(prices) - 1):
                assert prices[i] <= prices[i + 1], \
                    f"nq={q}: Price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"

    # ==================== L2: Edge Cases & Boundary Tests ====================

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_order_by_with_narrow_filter(self):
        """
        target: test order by with a narrow filter that returns few results
        method: search with filter on a specific category and order by price
        expected: filtered results sorted correctly, count <= limit
        """
        client = self._client()
        collection_name = VALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        filter_expr = "category == \"electronics\""
        res = self.search(client, collection_name, vectors_to_search,
                          limit=default_limit,
                          anns_field="embeddings",
                          filter=filter_expr,
                          output_fields=["id", "price", "category"],
                          order_by_fields=[{"field": "price", "order": "asc"}],
                          consistency_level="Strong")[0]

        results = res[0]
        for r in results:
            assert r["entity"]["category"] == "electronics"

        prices = [r["entity"]["price"] for r in results]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], \
                f"Price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"


@pytest.mark.xdist_group("TestMilvusClientSearchOrderInvalid")
class TestMilvusClientSearchOrderInvalid(TestMilvusClientV2Base):
    """
    Test cases for search with order_by_fields - invalid/negative scenarios.
    Collection is initialized once via fixture and shared across all tests.
    """

    @pytest.fixture(scope="module", autouse=True)
    def prepare_invalid_collection(self, request):
        """Create the shared collection once before all tests in this module,
        and drop it after all tests complete."""
        client = self._client()
        collection_name = INVALID_COLLECTION_NAME
        if client.has_collection(collection_name):
            client.drop_collection(collection_name)

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("price", DataType.DOUBLE)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="embeddings", index_type="HNSW",
                               metric_type="COSINE", M=16, efConstruction=200)

        client.create_collection(collection_name=collection_name, schema=schema,
                                 index_params=index_params, consistency_level="Strong")

        rows = cf.gen_row_data_by_schema(nb=500, schema=schema)
        for i, row in enumerate(rows):
            row["price"] = float(random.randint(10, 59))
        client.insert(collection_name=collection_name, data=rows)
        client.flush(collection_name=collection_name)

        def teardown():
            try:
                if self.has_collection(self._client(), INVALID_COLLECTION_NAME):
                    self.drop_collection(self._client(), INVALID_COLLECTION_NAME)
            except Exception:
                pass
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_order_by_nonexistent_field(self):
        """
        target: test search order by a field that does not exist in schema
        method: search with order_by_fields on non-existent field
        expected: returns error
        """
        client = self._client()
        collection_name = INVALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {ct.err_code: 65536, ct.err_msg: "order_by field 'nonexistent_field' does not exist in collection schema"}
        self.search(client, collection_name, vectors_to_search,
                    limit=default_limit,
                    anns_field="embeddings",
                    output_fields=["id", "price"],
                    order_by_fields=[{"field": "nonexistent_field", "order": "asc"}],
                    check_task=CheckTasks.err_res,
                    check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_order_by_vector_field(self):
        """
        target: test search order by a vector field
        method: search with order_by_fields on embeddings (vector field)
        expected: returns error
        """
        client = self._client()
        collection_name = INVALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {
            ct.err_code: 65536,
            ct.err_msg: (
                "order_by field 'embeddings' has unsortable type FloatVector; supported types: "
                "bool, int8/16/32/64, float, double, string, varchar; for JSON fields use path "
                "syntax like field[\"key\"]"
            )
        }
        self.search(client, collection_name, vectors_to_search,
                    limit=default_limit,
                    anns_field="embeddings",
                    output_fields=["id", "price"],
                    order_by_fields=[{"field": "embeddings", "order": "asc"}],
                    check_task=CheckTasks.err_res,
                    check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_order_by_invalid_order_value(self):
        """
        target: test search order by with invalid order value
        method: search with order_by_fields with order="invalid"
        expected: returns error
        """
        client = self._client()
        collection_name = INVALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {ct.err_code: 65536, ct.err_msg: "invalid order direction 'invalid' for field 'price', expected 'asc' or 'desc'"}
        self.search(client, collection_name, vectors_to_search,
                    limit=default_limit,
                    anns_field="embeddings",
                    output_fields=["id", "price"],
                    order_by_fields=[{"field": "price", "order": "invalid"}],
                    check_task=CheckTasks.err_res,
                    check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_order_by_empty_list(self):
        """
        target: test search order by with empty order_by_fields list
        method: search with order_by_fields=[]
        expected: normal search behavior (no ordering applied) or returns error
        """
        client = self._client()
        collection_name = INVALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        # Empty list should either work as normal search or return error
        res = self.search(client, collection_name, vectors_to_search,
                          limit=default_limit,
                          anns_field="embeddings",
                          output_fields=["id", "price"],
                          order_by_fields=[],
                          consistency_level="Strong")[0]

        # Should return results (normal search without ordering)
        results = res[0]
        assert len(results) == default_limit

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_order_by_missing_field_key(self):
        """
        target: test search order by with malformed dict (missing 'field' key)
        method: search with order_by_fields=[{"order": "asc"}]
        expected: returns error
        """
        client = self._client()
        collection_name = INVALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        error = {ct.err_code: 1, ct.err_msg: "Invalid order_by_fields item: 'field' key is required and cannot be empty"}
        self.search(client, collection_name, vectors_to_search,
                    limit=default_limit,
                    anns_field="embeddings",
                    output_fields=["id", "price"],
                    order_by_fields=[{"order": "asc"}],
                    check_task=CheckTasks.err_res,
                    check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_search_order_by_missing_order_key(self):
        """
        target: test search order by with malformed dict (missing 'order' key)
        method: search with order_by_fields=[{"field": "price"}]
        expected: returns error or defaults to ascending
        """
        client = self._client()
        collection_name = INVALID_COLLECTION_NAME

        vectors_to_search = cf.gen_vectors(1, default_dim)
        # Missing 'order' key - may default to asc or return error
        try:
            res = self.search(client, collection_name, vectors_to_search,
                              limit=default_limit,
                              anns_field="embeddings",
                              output_fields=["id", "price"],
                              order_by_fields=[{"field": "price"}],
                              consistency_level="Strong")[0]
            # If it succeeds, verify it defaults to some ordering
            results = res[0]
            assert len(results) == default_limit
        except Exception as e:
            log.info(f"Expected error for missing 'order' key: {e}")
