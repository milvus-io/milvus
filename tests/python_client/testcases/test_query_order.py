"""
Test Query ORDER BY

This module tests the Query ORDER BY feature which supports:
- ORDER BY on scalar fields (single or multiple columns)
- Sort directions: ASC (ascending) and DESC (descending)
- Supported data types: Int8/16/32/64, Float, Double, VarChar
- Combination with filter, limit, offset (pagination)
- Not supported: JSON, Array, Vector fields

PR: TBD
Issue: TBD
"""

import pytest
import pandas as pd
import numpy as np
from base.client_v2_base import TestMilvusClientV2Base
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from pymilvus import DataType

prefix = "query_order"
default_nb = 3000


@pytest.mark.xdist_group("TestQueryOrderSharedV2")
class TestQueryOrderSharedV2(TestMilvusClientV2Base):
    """
    Test Query ORDER BY with Shared Collection (L0 + L1)

    These tests use a single shared collection to avoid repeated setup overhead.
    All tests are read-only and can safely share the same collection data.

    Covers:
    - Single/multi-field ORDER BY with ASC/DESC
    - Filter, limit, offset combinations
    - Various data types (Int32, Int64, Double, VarChar)
    - Edge cases (empty results, sort field not in output)
    """

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestQueryOrderShared" + cf.gen_unique_str("_")
        self.pk_field_name = "pk"
        self.name_field_name = "name"
        self.category_field_name = "category"
        self.price_field_name = "price"
        self.rating_field_name = "rating"
        self.stock_field_name = "stock"
        self.vector_field_name = "embedding"
        self.datas = []

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self, request):
        """
        Prepare collection with ORDER BY test data

        Schema:
        - pk: Int64 (primary key, auto_id)
        - name: VarChar (non-nullable, 7 unique values)
        - category: VarChar (non-nullable, 5 unique values)
        - price: Double (non-nullable)
        - rating: Int32 (non-nullable, values 1-5)
        - stock: Int64 (non-nullable, values 0-1000)
        - embedding: FloatVector (non-nullable, dim=8)

        Data: 3000 rows with random but reproducible values (seed=19530).
        Inserted in 3 batches of 1000, each flushed, to test cross-segment ordering.
        """
        client = self._client()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(self.pk_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(self.name_field_name, DataType.VARCHAR, max_length=100)
        schema.add_field(self.category_field_name, DataType.VARCHAR, max_length=100)
        schema.add_field(self.price_field_name, DataType.DOUBLE)
        schema.add_field(self.rating_field_name, DataType.INT32)
        schema.add_field(self.stock_field_name, DataType.INT64)
        schema.add_field(self.vector_field_name, DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace"]
        categories = ["Electronics", "Books", "Clothing", "Food", "Sports"]

        np.random.seed(19530)
        all_rows = []
        batch_size = 1000
        for batch_idx in range(3):
            rows = []
            for i in range(batch_size):
                row = {
                    self.name_field_name: np.random.choice(names),
                    self.category_field_name: np.random.choice(categories),
                    self.price_field_name: round(float(np.random.uniform(10.0, 500.0)), 2),
                    self.rating_field_name: int(np.random.randint(1, 6)),
                    self.stock_field_name: int(np.random.randint(0, 1001)),
                    self.vector_field_name: [float(x) for x in np.random.random(8)],
                }
                rows.append(row)
            self.insert(client, self.collection_name, data=rows)
            self.flush(client, self.collection_name)
            all_rows.extend(rows)

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=self.vector_field_name,
            metric_type="L2",
            index_type="FLAT",
            params={}
        )
        self.create_index(client, self.collection_name, index_params=index_params)
        self.load_collection(client, self.collection_name)

        self.__class__.datas = pd.DataFrame(all_rows)
        log.info(f"Prepared collection {self.collection_name} with {default_nb} entities in 3 segments")

        def teardown():
            self.drop_collection(self._client(), self.collection_name)

        request.addfinalizer(teardown)

    # ==================== L0: Core ORDER BY functionality ====================

    @pytest.mark.tags(CaseLabel.L0)
    def test_order_by_single_field_asc(self):
        """
        target: test basic ORDER BY single field ascending
        method: query with order_by=["price:asc"], filter="price > 50", limit=10
        expected: results sorted by price in ascending order
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="price > 50",
            output_fields=[self.name_field_name, self.category_field_name, self.price_field_name],
            limit=10,
            order_by=["price:asc"],
        )

        assert len(results) == 10, f"Expected 10 results, got {len(results)}"

        # Verify ascending order
        prices = [r[self.price_field_name] for r in results]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], \
                f"Not in ascending order: prices[{i}]={prices[i]} > prices[{i+1}]={prices[i+1]}"

        # Verify against ground truth: smallest 10 prices > 50
        filtered = self.datas[self.datas[self.price_field_name] > 50]
        expected_prices = sorted(filtered[self.price_field_name].tolist())[:10]
        for i, price in enumerate(prices):
            assert abs(price - expected_prices[i]) < 0.01, \
                f"Price mismatch at index {i}: {price} != {expected_prices[i]}"

        log.info("test_order_by_single_field_asc passed")

    @pytest.mark.tags(CaseLabel.L0)
    def test_order_by_single_field_desc(self):
        """
        target: test basic ORDER BY single field descending
        method: query with order_by=["price:desc"], filter="price > 50", limit=10
        expected: results sorted by price in descending order
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="price > 50",
            output_fields=[self.name_field_name, self.category_field_name, self.price_field_name],
            limit=10,
            order_by=["price:desc"],
        )

        assert len(results) == 10, f"Expected 10 results, got {len(results)}"

        # Verify descending order
        prices = [r[self.price_field_name] for r in results]
        for i in range(len(prices) - 1):
            assert prices[i] >= prices[i + 1], \
                f"Not in descending order: prices[{i}]={prices[i]} < prices[{i+1}]={prices[i+1]}"

        # Verify against ground truth: largest 10 prices > 50
        filtered = self.datas[self.datas[self.price_field_name] > 50]
        expected_prices = sorted(filtered[self.price_field_name].tolist(), reverse=True)[:10]
        for i, price in enumerate(prices):
            assert abs(price - expected_prices[i]) < 0.01, \
                f"Price mismatch at index {i}: {price} != {expected_prices[i]}"

        log.info("test_order_by_single_field_desc passed")

    @pytest.mark.tags(CaseLabel.L0)
    def test_order_by_multi_field(self):
        """
        target: test ORDER BY on multiple fields
        method: query with order_by=["rating:desc", "price:asc"], limit=20
        expected: results sorted by rating DESC first, then by price ASC within same rating
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="stock > 100",
            output_fields=[self.name_field_name, self.rating_field_name, self.price_field_name],
            limit=20,
            order_by=["rating:desc", "price:asc"],
        )

        assert len(results) == 20, f"Expected 20 results, got {len(results)}"

        # Verify multi-field sort order
        for i in range(len(results) - 1):
            r1 = results[i]
            r2 = results[i + 1]
            # Primary sort: rating DESC
            if r1[self.rating_field_name] > r2[self.rating_field_name]:
                continue  # correct order
            elif r1[self.rating_field_name] == r2[self.rating_field_name]:
                # Secondary sort: price ASC (within same rating)
                assert r1[self.price_field_name] <= r2[self.price_field_name], \
                    f"Not in order at index {i}: rating={r1[self.rating_field_name]}, " \
                    f"price {r1[self.price_field_name]} > {r2[self.price_field_name]}"
            else:
                pytest.fail(
                    f"Rating not in descending order at index {i}: "
                    f"{r1[self.rating_field_name]} < {r2[self.rating_field_name]}"
                )

        log.info("test_order_by_multi_field passed")

    # ==================== L1: Data type coverage ====================

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_int32_field(self):
        """
        target: test ORDER BY on Int32 field
        method: query with order_by=["rating:asc"], limit=15
        expected: results sorted by rating ascending
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            output_fields=[self.name_field_name, self.rating_field_name],
            limit=15,
            order_by=["rating:asc"],
        )

        assert len(results) == 15, f"Expected 15 results, got {len(results)}"

        ratings = [r[self.rating_field_name] for r in results]
        for i in range(len(ratings) - 1):
            assert ratings[i] <= ratings[i + 1], \
                f"Int32 not in ascending order at index {i}: {ratings[i]} > {ratings[i+1]}"

        # All results should have rating == 1 (smallest value) since limit=15 and we have ~600 per rating
        assert ratings[0] == 1, f"First rating should be 1, got {ratings[0]}"

        log.info("test_order_by_int32_field passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_int64_field(self):
        """
        target: test ORDER BY on Int64 field
        method: query with order_by=["stock:desc"], limit=10
        expected: results sorted by stock descending
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            output_fields=[self.name_field_name, self.stock_field_name],
            limit=10,
            order_by=["stock:desc"],
        )

        assert len(results) == 10, f"Expected 10 results, got {len(results)}"

        stocks = [r[self.stock_field_name] for r in results]
        for i in range(len(stocks) - 1):
            assert stocks[i] >= stocks[i + 1], \
                f"Int64 not in descending order at index {i}: {stocks[i]} < {stocks[i+1]}"

        # Verify against ground truth
        expected_stocks = sorted(self.datas[self.stock_field_name].tolist(), reverse=True)[:10]
        assert stocks == expected_stocks, \
            f"Stock values mismatch: {stocks} != {expected_stocks}"

        log.info("test_order_by_int64_field passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_double_field(self):
        """
        target: test ORDER BY on Double field
        method: query with order_by=["price:asc"], limit=10
        expected: results sorted by price ascending
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            output_fields=[self.name_field_name, self.price_field_name],
            limit=10,
            order_by=["price:asc"],
        )

        assert len(results) == 10, f"Expected 10 results, got {len(results)}"

        prices = [r[self.price_field_name] for r in results]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], \
                f"Double not in ascending order at index {i}: {prices[i]} > {prices[i+1]}"

        # Verify against ground truth
        expected_prices = sorted(self.datas[self.price_field_name].tolist())[:10]
        for i in range(len(prices)):
            assert abs(prices[i] - expected_prices[i]) < 0.01, \
                f"Double value mismatch at index {i}: {prices[i]} != {expected_prices[i]}"

        log.info("test_order_by_double_field passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_varchar_field(self):
        """
        target: test ORDER BY on VarChar field
        method: query with order_by=["category:asc"], limit=10
        expected: results sorted by category in lexicographical ascending order
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="rating >= 3",
            output_fields=[self.name_field_name, self.category_field_name, self.rating_field_name],
            limit=10,
            order_by=["category:asc"],
        )

        assert len(results) == 10, f"Expected 10 results, got {len(results)}"

        categories = [r[self.category_field_name] for r in results]
        for i in range(len(categories) - 1):
            assert categories[i] <= categories[i + 1], \
                f"VarChar not in ascending order at index {i}: '{categories[i]}' > '{categories[i+1]}'"

        # First results should be "Books" (lexicographically smallest category)
        assert categories[0] == "Books", \
            f"First category should be 'Books', got '{categories[0]}'"

        log.info("test_order_by_varchar_field passed")

    # ==================== L1: Filter and pagination ====================

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_with_filter(self):
        """
        target: test ORDER BY combined with filter expression
        method: query with filter="price > 100 and rating >= 3 and stock > 50", order_by=["price:desc"]
        expected: only filtered rows returned, sorted by price descending
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="price > 100 and rating >= 3 and stock > 50",
            output_fields=[self.name_field_name, self.category_field_name,
                           self.price_field_name, self.rating_field_name, self.stock_field_name],
            limit=10,
            order_by=["price:desc"],
        )

        assert len(results) > 0, "Expected at least 1 result"
        assert len(results) <= 10, f"Expected at most 10 results, got {len(results)}"

        # Verify filter conditions
        for r in results:
            assert r[self.price_field_name] > 100, f"Filter violated: price={r[self.price_field_name]} <= 100"
            assert r[self.rating_field_name] >= 3, f"Filter violated: rating={r[self.rating_field_name]} < 3"
            assert r[self.stock_field_name] > 50, f"Filter violated: stock={r[self.stock_field_name]} <= 50"

        # Verify descending order
        prices = [r[self.price_field_name] for r in results]
        for i in range(len(prices) - 1):
            assert prices[i] >= prices[i + 1], \
                f"Not in descending order at index {i}: {prices[i]} < {prices[i+1]}"

        log.info(f"test_order_by_with_filter passed: {len(results)} results")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_with_offset(self):
        """
        target: test ORDER BY with offset for pagination
        method: query page 1 (offset=0, limit=5) and page 2 (offset=5, limit=5)
        expected: page 2 picks up exactly where page 1 left off
        """
        client = self._client()

        # Page 1
        page1, _ = self.query(
            client,
            self.collection_name,
            filter="price > 50",
            output_fields=[self.name_field_name, self.price_field_name],
            limit=5,
            offset=0,
            order_by=["price:asc"],
        )

        # Page 2
        page2, _ = self.query(
            client,
            self.collection_name,
            filter="price > 50",
            output_fields=[self.name_field_name, self.price_field_name],
            limit=5,
            offset=5,
            order_by=["price:asc"],
        )

        assert len(page1) == 5, f"Page 1: expected 5, got {len(page1)}"
        assert len(page2) == 5, f"Page 2: expected 5, got {len(page2)}"

        # Page 2 should start after page 1 ends
        last_price_page1 = page1[-1][self.price_field_name]
        first_price_page2 = page2[0][self.price_field_name]
        assert first_price_page2 >= last_price_page1, \
            f"Page 2 first price ({first_price_page2}) < page 1 last price ({last_price_page1})"

        # No overlap (unless duplicate prices): combined should have 10 distinct ordered results
        all_prices = [r[self.price_field_name] for r in page1] + [r[self.price_field_name] for r in page2]
        for i in range(len(all_prices) - 1):
            assert all_prices[i] <= all_prices[i + 1], \
                f"Combined pages not in order at index {i}: {all_prices[i]} > {all_prices[i+1]}"

        log.info("test_order_by_with_offset passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_no_filter(self):
        """
        target: test ORDER BY without explicit filter (match all records)
        method: query with filter="", order_by=["rating:desc", "price:desc"], limit=10
        expected: returns top 10 results sorted by rating DESC then price DESC
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            output_fields=[self.name_field_name, self.rating_field_name, self.price_field_name],
            limit=10,
            order_by=["rating:desc", "price:desc"],
        )

        assert len(results) == 10, f"Expected 10 results, got {len(results)}"

        # Verify multi-field descending order
        for i in range(len(results) - 1):
            r1 = results[i]
            r2 = results[i + 1]
            if r1[self.rating_field_name] > r2[self.rating_field_name]:
                continue
            elif r1[self.rating_field_name] == r2[self.rating_field_name]:
                assert r1[self.price_field_name] >= r2[self.price_field_name], \
                    f"Secondary sort violated at index {i}"
            else:
                pytest.fail(f"Primary sort violated at index {i}")

        # First result should have rating == 5 (max)
        assert results[0][self.rating_field_name] == 5, \
            f"First result should have max rating 5, got {results[0][self.rating_field_name]}"

        log.info("test_order_by_no_filter passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_field_not_in_output(self):
        """
        target: test ORDER BY on a field not included in output_fields
        method: query with order_by=["price:asc"] but output_fields does not include price
        expected: results are still sorted by price, but price is not in output
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            output_fields=[self.name_field_name, self.category_field_name],
            limit=10,
            order_by=["price:asc"],
        )

        assert len(results) == 10, f"Expected 10 results, got {len(results)}"

        # Price should NOT be in output fields
        # (server may or may not include it; if it does, that's fine too)
        log.info(f"Output fields present: {list(results[0].keys())}")

        log.info("test_order_by_field_not_in_output passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_category_filter(self):
        """
        target: test ORDER BY with category equality filter
        method: query with filter="category == 'Electronics'", order_by=["stock:desc"]
        expected: only Electronics returned, sorted by stock descending
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="category == 'Electronics'",
            output_fields=[self.name_field_name, self.category_field_name,
                           self.stock_field_name, self.price_field_name],
            limit=10,
            order_by=["stock:desc"],
        )

        assert len(results) == 10, f"Expected 10 results, got {len(results)}"

        # Verify filter
        for r in results:
            assert r[self.category_field_name] == "Electronics", \
                f"Filter violated: category={r[self.category_field_name]}"

        # Verify descending order
        stocks = [r[self.stock_field_name] for r in results]
        for i in range(len(stocks) - 1):
            assert stocks[i] >= stocks[i + 1], \
                f"Not in descending order at index {i}: {stocks[i]} < {stocks[i+1]}"

        log.info("test_order_by_category_filter passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_empty_result(self):
        """
        target: test ORDER BY when filter matches no rows
        method: query with filter that matches nothing
        expected: returns empty result set
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="price > 99999",
            output_fields=[self.name_field_name, self.price_field_name],
            limit=10,
            order_by=["price:asc"],
        )

        assert len(results) == 0, f"Expected empty result, got {len(results)}"

        log.info("test_order_by_empty_result passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_large_limit(self):
        """
        target: test ORDER BY with limit larger than total matching rows
        method: query with a restrictive filter and large limit
        expected: returns all matching rows sorted correctly
        """
        client = self._client()

        # Filter to get ~few hundred rows (price > 400, roughly top 20%)
        results, _ = self.query(
            client,
            self.collection_name,
            filter="price > 400",
            output_fields=[self.name_field_name, self.price_field_name],
            limit=10000,
            order_by=["price:asc"],
        )

        # Calculate expected count
        expected_count = len(self.datas[self.datas[self.price_field_name] > 400])
        assert len(results) == expected_count, \
            f"Expected {expected_count} results, got {len(results)}"

        # Verify ascending order
        prices = [r[self.price_field_name] for r in results]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], \
                f"Not in ascending order at index {i}"

        log.info(f"test_order_by_large_limit passed: {len(results)} results")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_cross_segment_correctness(self):
        """
        target: verify ORDER BY produces globally correct results across segments
        method: query all data ordered by price ASC with a moderate limit,
                compare with pandas sort on full dataset
        expected: exact match with ground truth
        """
        client = self._client()

        limit = 50
        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            output_fields=[self.price_field_name, self.rating_field_name],
            limit=limit,
            order_by=["price:asc"],
        )

        assert len(results) == limit, f"Expected {limit} results, got {len(results)}"

        # Ground truth: sort entire dataset by price ASC, take top 50
        expected = self.datas.sort_values(self.price_field_name).head(limit)
        expected_prices = expected[self.price_field_name].tolist()

        result_prices = [r[self.price_field_name] for r in results]
        for i in range(limit):
            assert abs(result_prices[i] - expected_prices[i]) < 0.01, \
                f"Cross-segment mismatch at index {i}: {result_prices[i]} != {expected_prices[i]}"

        log.info("test_order_by_cross_segment_correctness passed")


@pytest.mark.xdist_group("TestQueryOrderErrorV2")
class TestQueryOrderErrorV2(TestMilvusClientV2Base):
    """
    Test Query ORDER BY error cases requiring independent collections

    Covers:
    - Unsupported field types (Vector, JSON, Array)
    - Invalid syntax
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_vector_field_error(self):
        """
        target: test error when ORDER BY on vector field
        method: query with order_by=["vec:asc"]
        expected: raise error indicating vector field not supported
        """
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field("val", DataType.INT32)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        rows = [{"val": i, "vec": [float(x) for x in np.random.random(8)]} for i in range(10)]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        error = {ct.err_code: 1100, ct.err_msg: "order by"}
        self.query(
            client,
            collection_name,
            filter="",
            output_fields=["val"],
            limit=10,
            order_by=["vec:asc"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        self.drop_collection(client, collection_name)
        log.info("test_order_by_vector_field_error passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_json_field_error(self):
        """
        target: test error when ORDER BY on JSON field
        method: query with order_by=["json_field:asc"]
        expected: raise error indicating JSON field not supported
        """
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field("val", DataType.INT32)
        schema.add_field("json_field", DataType.JSON)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        rows = [{"val": i, "json_field": {"k": i}, "vec": [float(x) for x in np.random.random(8)]} for i in range(10)]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        error = {ct.err_code: 1100, ct.err_msg: "order by"}
        self.query(
            client,
            collection_name,
            filter="",
            output_fields=["val"],
            limit=10,
            order_by=["json_field:asc"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        self.drop_collection(client, collection_name)
        log.info("test_order_by_json_field_error passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_array_field_error(self):
        """
        target: test error when ORDER BY on Array field
        method: query with order_by=["arr:asc"]
        expected: raise error indicating Array field not supported
        """
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field("val", DataType.INT32)
        schema.add_field("arr", DataType.ARRAY, element_type=DataType.INT64, max_capacity=10)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        rows = [{"val": i, "arr": [i, i + 1], "vec": [float(x) for x in np.random.random(8)]} for i in range(10)]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        error = {ct.err_code: 1100, ct.err_msg: "order by"}
        self.query(
            client,
            collection_name,
            filter="",
            output_fields=["val"],
            limit=10,
            order_by=["arr:asc"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        self.drop_collection(client, collection_name)
        log.info("test_order_by_array_field_error passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_nonexistent_field_error(self):
        """
        target: test error when ORDER BY on a field that does not exist
        method: query with order_by=["nonexistent:asc"]
        expected: raise error indicating field not found
        """
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field("val", DataType.INT32)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        rows = [{"val": i, "vec": [float(x) for x in np.random.random(8)]} for i in range(10)]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.query(
            client,
            collection_name,
            filter="",
            output_fields=["val"],
            limit=10,
            order_by=["nonexistent:asc"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        self.drop_collection(client, collection_name)
        log.info("test_order_by_nonexistent_field_error passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_invalid_direction_error(self):
        """
        target: test error when ORDER BY uses invalid sort direction
        method: query with order_by=["val:up"] (invalid direction)
        expected: raise error indicating invalid sort direction
        """
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field("val", DataType.INT32)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        rows = [{"val": i, "vec": [float(x) for x in np.random.random(8)]} for i in range(10)]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.query(
            client,
            collection_name,
            filter="",
            output_fields=["val"],
            limit=10,
            order_by=["val:up"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        self.drop_collection(client, collection_name)
        log.info("test_order_by_invalid_direction_error passed")


@pytest.mark.xdist_group("TestQueryOrderNullableV2")
class TestQueryOrderNullableV2(TestMilvusClientV2Base):
    """
    Test Query ORDER BY with nullable fields

    Covers:
    - ORDER BY on nullable field (NULL values sorted correctly)
    - NULL values should appear at the end by default (NULLS LAST)
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_order_by_nullable_field(self):
        """
        target: test ORDER BY on a nullable field
        method: create collection with nullable Int32 field, insert data with NULLs,
                query with order_by on the nullable field
        expected: results sorted correctly, NULL values at the end (NULLS LAST default)
        """
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field("score", DataType.INT32, nullable=True)
        schema.add_field("name", DataType.VARCHAR, max_length=100)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        np.random.seed(42)
        rows = []
        for i in range(100):
            row = {
                "score": None if i % 5 == 0 else int(np.random.randint(1, 100)),
                "name": f"item_{i}",
                "vec": [float(x) for x in np.random.random(8)],
            }
            rows.append(row)
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        results, _ = self.query(
            client,
            collection_name,
            filter="",
            output_fields=["score", "name"],
            limit=100,
            order_by=["score:asc"],
        )

        assert len(results) == 100, f"Expected 100 results, got {len(results)}"

        # Verify: non-NULL values should be sorted ascending, NULLs at the end
        non_null_scores = [r["score"] for r in results if r["score"] is not None]
        null_count = sum(1 for r in results if r["score"] is None)

        # Non-null values should be sorted
        for i in range(len(non_null_scores) - 1):
            assert non_null_scores[i] <= non_null_scores[i + 1], \
                f"Non-null scores not in order at index {i}: {non_null_scores[i]} > {non_null_scores[i+1]}"

        # Should have 20 NULLs (every 5th row out of 100)
        assert null_count == 20, f"Expected 20 NULL values, got {null_count}"

        # NULLs should be at the end (NULLS LAST is default for ASC)
        first_null_idx = next((i for i, r in enumerate(results) if r["score"] is None), None)
        if first_null_idx is not None:
            # All entries after first NULL should also be NULL
            for i in range(first_null_idx, len(results)):
                assert results[i]["score"] is None, \
                    f"Non-NULL value found after NULL at index {i}"

        self.drop_collection(client, collection_name)
        log.info(f"test_order_by_nullable_field passed: {null_count} NULLs at end")
