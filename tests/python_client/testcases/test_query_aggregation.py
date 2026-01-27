"""
Test Query Aggregation (GROUP BY + Aggregation Functions)

This module tests the Query Aggregation feature which supports:
- GROUP BY on scalar fields (single or multiple columns)
- Aggregation functions: COUNT, SUM, MIN, MAX, AVG
- Supported data types: Int8/16/32/64, Float, Double, VarChar, Timestamptz
- Not supported: JSON, Array, Vector fields

Test Plan: /Users/yanliang/fork/milvus/docs/test-plans/2026-01-26-query-aggregation-test-plan.md
PR: #44394
Issue: #36380
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

prefix = "query_aggregation"
default_nb = 3000


@pytest.mark.xdist_group("TestQueryAggregationSharedV2")
class TestQueryAggregationSharedV2(TestMilvusClientV2Base):
    """
    Test Query Aggregation with Shared Collection (L0 + L1)

    These tests use a single shared collection to avoid repeated setup overhead.
    All tests are read-only and can safely share the same collection data.

    Covers:
    - Single/multiple column GROUP BY with all aggregation functions
    - Global aggregation (no GROUP BY)
    - Filter, limit combinations
    - Various data types (Int, Double, VarChar, Timestamp)
    - Edge cases (empty results, case sensitivity)
    """

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestQueryAggregationShared" + cf.gen_unique_str("_")
        self.pk_field_name = "pk"
        self.c1_field_name = "c1"
        self.c2_field_name = "c2"
        self.c3_field_name = "c3"
        self.c4_field_name = "c4"
        self.ts_field_name = "ts"
        self.vector_field_name = "c5"
        self.c6_field_name = "c6"
        self.c7_field_name = "c7_int8"
        self.c8_field_name = "c8_int64"
        self.c9_field_name = "c9_float"
        self.datas = []

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self, request):
        """
        Prepare collection with aggregation test data

        Schema (with nullable aggregation fields to test NULL handling in aggregations):
        - pk: VarChar (primary key, non-nullable)
        - c1: VarChar (non-nullable, grouping field, 7 unique values)
        - c2: Int16 (nullable, aggregation field - tests COUNT/SUM with NULL)
        - c3: Int32 (non-nullable, aggregation field)
        - c4: Double (nullable, aggregation field - tests AVG/MIN/MAX with NULL)
        - ts: Int64 (non-nullable, timestamp field)
        - c5: FloatVector (non-nullable, dim=8)
        - c6: VarChar (non-nullable, grouping field, 7 unique values)
        - c7_int8: Int8 (non-nullable, grouping field, 5 unique values)
        - c8_int64: Int64 (non-nullable, grouping field, 5 unique values)
        - c9_float: Float (nullable, aggregation field - tests aggregations with NULL)

        Note: Nullable fields (c2, c4, c9_float) contain ~10-15% NULL values to test
              that aggregation functions correctly ignore NULL values.

        IMPORTANT: GROUP BY fields (c1, c6, c7_int8, c8_int64) are non-nullable
                   because nullable GROUP BY fields have a bug (issue #47350) where
                   Milvus returns NULL for all group values instead of actual values.
        """
        client = self._client()

        # Create schema with nullable aggregation fields (but non-nullable GROUP BY fields due to bug #47350)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(self.pk_field_name, DataType.VARCHAR, is_primary=True, max_length=100)
        # GROUP BY fields - all non-nullable (nullable GROUP BY has bug #47350)
        schema.add_field(self.c1_field_name, DataType.VARCHAR, max_length=100)
        schema.add_field(self.c6_field_name, DataType.VARCHAR, max_length=100)
        schema.add_field(self.c7_field_name, DataType.INT8)
        schema.add_field(self.c8_field_name, DataType.INT64)
        # Aggregation fields - some nullable to test NULL handling in aggregations
        schema.add_field(self.c2_field_name, DataType.INT16, nullable=True)
        schema.add_field(self.c3_field_name, DataType.INT32)
        schema.add_field(self.c4_field_name, DataType.DOUBLE, nullable=True)
        schema.add_field(self.c9_field_name, DataType.FLOAT, nullable=True)
        # Other fields - non-nullable
        schema.add_field(self.ts_field_name, DataType.INT64)
        schema.add_field(self.vector_field_name, DataType.FLOAT_VECTOR, dim=8)

        # Create collection
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        # Generate test data (3000 rows) with nullable aggregation fields containing ~10-15% NULL values
        unique_values_c1 = ["A", "B", "C", "D", "E", "F", "G"]
        unique_values_c6 = ["X", "Y", "Z", "W", "V", "U", "T"]
        unique_values_c7 = [1, 2, 3, 4, 5]  # INT8 grouping values
        unique_values_c8 = [100, 200, 300, 400, 500]  # INT64 grouping values
        unique_values_c9 = [1.0, 2.0, 3.0, 4.0, 5.0]  # FLOAT aggregation values

        np.random.seed(19530)
        rows = []
        for i in range(default_nb):
            # Helper function to randomly insert NULL for nullable aggregation fields (~15% probability)
            def maybe_null(value):
                return None if np.random.random() < 0.15 else value

            row = {
                self.pk_field_name: f"pk_{i}",
                # GROUP BY fields - all non-nullable (due to bug #47350)
                self.c1_field_name: np.random.choice(unique_values_c1),
                self.c6_field_name: np.random.choice(unique_values_c6),
                self.c7_field_name: int(np.random.choice(unique_values_c7)),
                self.c8_field_name: int(np.random.choice(unique_values_c8)),
                # Aggregation fields - some nullable to test NULL handling
                self.c2_field_name: maybe_null(int(np.random.randint(0, 100, dtype=np.int16))),
                self.c4_field_name: maybe_null(float(np.random.uniform(0, 100))),
                self.c9_field_name: maybe_null(float(np.random.choice(unique_values_c9))),
                # Other non-nullable fields
                self.c3_field_name: int(np.random.randint(0, 1000, dtype=np.int32)),
                self.ts_field_name: int(np.random.randint(1000000, 2000000, dtype=np.int64)),
                self.vector_field_name: [float(x) for x in np.random.random(8)],
            }
            rows.append(row)

        # Insert data
        self.insert(client, self.collection_name, data=rows)
        self.flush(client, self.collection_name)

        # Create index on vectowr field
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=self.vector_field_name,
            metric_type="L2",
            index_type="FLAT",
            params={}
        )
        self.create_index(client, self.collection_name, index_params=index_params)

        # Load collection
        self.load_collection(client, self.collection_name)

        # Store data for ground truth verification (on class, not instance)
        self.__class__.datas = pd.DataFrame(rows)

        log.info(f"Prepared collection {self.collection_name} with {default_nb} entities")

        def teardown():
            self.drop_collection(self._client(), self.collection_name)

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="Bug: GROUP BY requires limit when filter is empty. Tracked in issue #47329")
    def test_basic_group_by_count_no_filter_no_limit(self):
        """
        target: test the most basic GROUP BY with COUNT without any filter or limit
        method: query with only group_by_fields and output_fields, no filter/limit parameters
        expected: should return all groups with correct count values
        Note: Currently fails with 'empty expression should be used with limit' (issue #47329)
        """
        client = self._client()

        # Most basic aggregation: no filter, no limit - just group and count
        # This should work but currently fails with "empty expression should be used with limit"
        results, _ = self.query(
            client,
            self.collection_name,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "count(c2)"]
        )

        # Should return all 7 groups
        assert len(results) == 7, f"Expected 7 groups, got {len(results)}"

        # Calculate ground truth
        ground_truth = self.datas.groupby(self.c1_field_name).agg(
            count_c2=(self.c2_field_name, "count")
        ).reset_index()

        # Verify each group's count
        for result in results:
            c1_value = result[self.c1_field_name]
            expected = ground_truth[ground_truth[self.c1_field_name] == c1_value].iloc[0]
            assert result["count(c2)"] == expected["count_c2"], \
                f"COUNT mismatch for c1={c1_value}: {result['count(c2)']} != {expected['count_c2']}"

        log.info(f"test_basic_group_by_count_no_filter_no_limit passed: {len(results)} groups verified")

    @pytest.mark.tags(CaseLabel.L0)
    def test_single_column_group_by_count_sum(self):
        """
        target: test basic single column GROUP BY with COUNT and SUM
        method: query with group_by_fields=["c1"], output_fields=["c1", "count(c2)", "sum(c3)"]
        expected: returns 7 groups with correct count and sum values
        """
        client = self._client()

        # Execute query
        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "count(c2)", "sum(c3)"]
        )

        # Verify number of groups
        assert len(results) == 7, f"Expected 7 groups, got {len(results)}"

        # Calculate ground truth
        ground_truth = self.datas.groupby(self.c1_field_name).agg(
            count_c2=(self.c2_field_name, "count"),
            sum_c3=(self.c3_field_name, "sum")
        ).reset_index()

        # Verify each group's aggregation values
        for result in results:
            c1_value = result[self.c1_field_name]
            expected = ground_truth[ground_truth[self.c1_field_name] == c1_value].iloc[0]

            assert result["count(c2)"] == expected["count_c2"], \
                f"COUNT mismatch for c1={c1_value}: {result['count(c2)']} != {expected['count_c2']}"
            assert result["sum(c3)"] == expected["sum_c3"], \
                f"SUM mismatch for c1={c1_value}: {result['sum(c3)']} != {expected['sum_c3']}"

        log.info(f"test_single_column_group_by_count_sum passed: {len(results)} groups verified")

    @pytest.mark.tags(CaseLabel.L0)
    def test_multi_column_group_by_min_max(self):
        """
        target: test multi-column GROUP BY with MIN and MAX
        method: query with group_by_fields=["c1", "c6"], output_fields=["c1", "c6", "min(c2)", "max(c2)"]
        expected: returns correct groups with correct min and max values
        """
        client = self._client()

        # Execute query
        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name, self.c6_field_name],
            output_fields=[self.c1_field_name, self.c6_field_name, "min(c2)", "max(c2)"]
        )

        # Verify number of groups (should be up to 49, but actual may be less)
        assert len(results) > 0, "Expected at least 1 group"
        log.info(f"Got {len(results)} groups from multi-column GROUP BY")

        # Calculate ground truth
        ground_truth = self.datas.groupby([self.c1_field_name, self.c6_field_name]).agg(
            min_c2=(self.c2_field_name, "min"),
            max_c2=(self.c2_field_name, "max")
        ).reset_index()

        # Verify each group's aggregation values
        for result in results:
            c1_value = result[self.c1_field_name]
            c6_value = result[self.c6_field_name]
            expected = ground_truth[
                (ground_truth[self.c1_field_name] == c1_value) &
                (ground_truth[self.c6_field_name] == c6_value)
            ].iloc[0]

            assert result["min(c2)"] == expected["min_c2"], \
                f"MIN mismatch for c1={c1_value}, c6={c6_value}"
            assert result["max(c2)"] == expected["max_c2"], \
                f"MAX mismatch for c1={c1_value}, c6={c6_value}"

        log.info(f"test_multi_column_group_by_min_max passed: {len(results)} groups verified")

    @pytest.mark.tags(CaseLabel.L1)
    def test_single_group_by_multiple_aggregations(self):
        """
        target: test single GROUP BY field with multiple AVG aggregations
        method: query with group_by_fields=["c1"], output_fields=["c1", "avg(c2)", "avg(c3)", "avg(c4)"]
        expected: returns groups with 3 AVG values, all of type double
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "avg(c2)", "avg(c3)", "avg(c4)"]
        )

        assert len(results) == 7, f"Expected 7 groups, got {len(results)}"

        # Calculate ground truth
        ground_truth = self.datas.groupby(self.c1_field_name).agg(
            avg_c2=(self.c2_field_name, "mean"),
            avg_c3=(self.c3_field_name, "mean"),
            avg_c4=(self.c4_field_name, "mean")
        ).reset_index()

        # Verify AVG values and check they are float/double type
        for result in results:
            c1_value = result[self.c1_field_name]
            expected = ground_truth[ground_truth[self.c1_field_name] == c1_value].iloc[0]

            # AVG should return float type
            assert isinstance(result["avg(c2)"], (float, np.floating)), \
                f"avg(c2) should be float type, got {type(result['avg(c2)'])}"
            assert isinstance(result["avg(c3)"], (float, np.floating)), \
                f"avg(c3) should be float type"
            assert isinstance(result["avg(c4)"], (float, np.floating)), \
                f"avg(c4) should be float type"

            # Verify values (allow small floating point errors)
            assert abs(result["avg(c2)"] - expected["avg_c2"]) < 0.01, \
                f"AVG(c2) mismatch for c1={c1_value}"
            assert abs(result["avg(c3)"] - expected["avg_c3"]) < 0.01, \
                f"AVG(c3) mismatch for c1={c1_value}"
            assert abs(result["avg(c4)"] - expected["avg_c4"]) < 0.01, \
                f"AVG(c4) mismatch for c1={c1_value}"

        log.info("test_single_group_by_multiple_aggregations passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_group_by_with_filter(self):
        """
        target: test GROUP BY with filter expression
        method: query with filter="c2 < 10", group_by_fields=["c1"], output_fields=["c1", "count(c2)", "max(c3)"]
        expected: only rows with c2 < 10 participate in grouping
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="c2 < 10",
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "count(c2)", "max(c3)"]
        )

        # Filter data first
        filtered_data = self.datas[self.datas[self.c2_field_name] < 10]
        ground_truth = filtered_data.groupby(self.c1_field_name).agg(
            count_c2=(self.c2_field_name, "count"),
            max_c3=(self.c3_field_name, "max")
        ).reset_index()

        # Number of groups may be less than 7 if some groups have no data after filter
        assert len(results) <= 7, f"Expected at most 7 groups, got {len(results)}"
        assert len(results) == len(ground_truth), \
            f"Group count mismatch: {len(results)} != {len(ground_truth)}"

        for result in results:
            c1_value = result[self.c1_field_name]
            expected = ground_truth[ground_truth[self.c1_field_name] == c1_value].iloc[0]

            assert result["count(c2)"] == expected["count_c2"], \
                f"COUNT mismatch for c1={c1_value} with filter"
            assert result["max(c3)"] == expected["max_c3"], \
                f"MAX mismatch for c1={c1_value} with filter"

        log.info(f"test_group_by_with_filter passed: {len(results)} groups after filtering")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="limit parameter on GROUP BY queries not fully supported in Milvus 2.6.6")
    def test_group_by_with_limit(self):
        """
        target: test GROUP BY without filter but with limit
        method: query with filter="", group_by_fields=["c1"], output_fields=["c1", "avg(c2)"], limit=3
        expected: returns at most 3 groups
        NOTE: This feature may not be fully implemented in Milvus 2.6.6 - limit may apply to rows before grouping
        """
        client = self._client()

        limit = 3
        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "avg(c2)"],
            limit=limit
        )

        assert len(results) <= limit, f"Expected at most {limit} groups, got {len(results)}"
        log.info(f"test_group_by_with_limit passed: {len(results)} groups returned")

    @pytest.mark.tags(CaseLabel.L1)
    def test_varchar_min_max(self):
        """
        target: test MIN/MAX on VarChar field
        method: query with group_by_fields=["c1"], output_fields=["c1", "min(c6)", "max(c6)"]
        expected: MIN/MAX returns VarChar type, sorted by lexicographical order
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "min(c6)", "max(c6)"]
        )

        assert len(results) == 7, f"Expected 7 groups, got {len(results)}"

        # Calculate ground truth
        ground_truth = self.datas.groupby(self.c1_field_name).agg(
            min_c6=(self.c6_field_name, "min"),
            max_c6=(self.c6_field_name, "max")
        ).reset_index()

        for result in results:
            c1_value = result[self.c1_field_name]
            expected = ground_truth[ground_truth[self.c1_field_name] == c1_value].iloc[0]

            # Verify values (lexicographical order)
            assert result["min(c6)"] == expected["min_c6"], \
                f"MIN(c6) mismatch for c1={c1_value}"
            assert result["max(c6)"] == expected["max_c6"], \
                f"MAX(c6) mismatch for c1={c1_value}"

            # Verify type is string
            assert isinstance(result["min(c6)"], str), "min(c6) should return string"
            assert isinstance(result["max(c6)"], str), "max(c6) should return string"

        log.info("test_varchar_min_max passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_timestamp_aggregation(self):
        """
        target: test aggregation on timestamp field
        method: query with group_by_fields=["c1"], output_fields=["c1", "count(ts)", "max(ts)"]
        expected: timestamp grouping and aggregation work correctly
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "count(ts)", "max(ts)"]
        )

        assert len(results) == 7, f"Expected 7 groups, got {len(results)}"

        ground_truth = self.datas.groupby(self.c1_field_name).agg(
            count_ts=(self.ts_field_name, "count"),
            max_ts=(self.ts_field_name, "max")
        ).reset_index()

        for result in results:
            c1_value = result[self.c1_field_name]
            expected = ground_truth[ground_truth[self.c1_field_name] == c1_value].iloc[0]

            assert result["count(ts)"] == expected["count_ts"], \
                f"COUNT(ts) mismatch for c1={c1_value}"
            assert result["max(ts)"] == expected["max_ts"], \
                f"MAX(ts) mismatch for c1={c1_value}"

        log.info("test_timestamp_aggregation passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_different_sum_return_types(self):
        """
        target: test SUM return types for different numeric types
        method: verify sum(c2) (Int16) -> int64, sum(c3) (Int32) -> int64, sum(c4) (Double) -> double
        expected: integer SUM returns int64, float SUM returns double
        """
        client = self._client()

        # Test integer SUM (Int16)
        results_int16, _ = self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "sum(c2)"]
        )

        # Test integer SUM (Int32)
        results_int32, _ = self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "sum(c3)"]
        )

        # Test float SUM (Double)
        results_double, _ = self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "sum(c4)"]
        )

        # Verify return types
        for result in results_int16:
            # Int16 SUM should return int64
            assert isinstance(result["sum(c2)"], (int, np.integer)), \
                f"sum(c2) should return int type, got {type(result['sum(c2)'])}"

        for result in results_int32:
            # Int32 SUM should return int64
            assert isinstance(result["sum(c3)"], (int, np.integer)), \
                f"sum(c3) should return int type, got {type(result['sum(c3)'])}"

        for result in results_double:
            # Double SUM should return double
            assert isinstance(result["sum(c4)"], (float, np.floating)), \
                f"sum(c4) should return float type, got {type(result['sum(c4)'])}"

        log.info("test_different_sum_return_types passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_avg_return_type(self):
        """
        target: test AVG always returns double regardless of input type
        method: verify avg(c2) (Int16), avg(c3) (Int32), avg(c4) (Double) all return double
        expected: all AVG results are double type
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "avg(c2)", "avg(c3)", "avg(c4)"]
        )

        for result in results:
            # All AVG should return float/double type
            assert isinstance(result["avg(c2)"], (float, np.floating)), \
                f"avg(c2) should return float, got {type(result['avg(c2)'])}"
            assert isinstance(result["avg(c3)"], (float, np.floating)), \
                f"avg(c3) should return float, got {type(result['avg(c3)'])}"
            assert isinstance(result["avg(c4)"], (float, np.floating)), \
                f"avg(c4) should return float, got {type(result['avg(c4)'])}"

        log.info("test_avg_return_type passed")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="Skipped due to Milvus crash bug, tracked in issue #47316")
    def test_empty_result_aggregation(self):
        """
        target: test aggregation when filter matches no rows
        method: query with filter that matches nothing, e.g., "c2 > 10000"
        expected: returns empty result set
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="c2 > 10000",
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "count(c2)"]
        )

        # Should return empty result set
        assert len(results) == 0, f"Expected empty result, got {len(results)} groups"
        log.info("test_empty_result_aggregation passed")

    @pytest.mark.tags(CaseLabel.L0)
    def test_global_aggregation_no_filter_no_limit(self):
        """
        target: test global aggregation without GROUP BY, filter, or limit
        method: query with only group_by_fields=[], output_fields=["count(c2)", "sum(c2)", "avg(c3)"]
        expected: returns 1 row with global aggregation values for all data
        Note: Unlike GROUP BY aggregation, global aggregation works without filter/limit
              c2 is nullable, so COUNT(c2) excludes NULL values
        """
        client = self._client()

        # Most basic global aggregation: no GROUP BY, no filter, no limit
        results, _ = self.query(
            client,
            self.collection_name,
            group_by_fields=[],
            output_fields=["count(c2)", "sum(c2)", "avg(c3)"]
        )

        # Should return exactly 1 row
        assert len(results) == 1, f"Expected 1 global aggregation row, got {len(results)}"

        # Calculate ground truth for all data
        # Note: c2 is nullable, so pandas count() excludes NULL, matching SQL behavior
        expected_count = self.datas[self.c2_field_name].count()  # COUNT excludes NULL
        expected_sum = self.datas[self.c2_field_name].sum()  # SUM excludes NULL
        expected_avg = self.datas[self.c3_field_name].mean()

        result = results[0]
        assert result["count(c2)"] == expected_count, \
            f"Global count mismatch: {result['count(c2)']} != {expected_count}"
        assert result["sum(c2)"] == expected_sum, \
            f"Global sum mismatch: {result['sum(c2)']} != {expected_sum}"
        assert abs(result["avg(c3)"] - expected_avg) < 0.01, \
            f"Global avg mismatch: {result['avg(c3)']} != {expected_avg}"

        log.info("test_global_aggregation_no_filter_no_limit passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_global_aggregation_no_group_by(self):
        """
        target: test global aggregation without GROUP BY with filter
        method: query with group_by_fields=[], output_fields=["count(c2)", "sum(c2)", "avg(c3)"]
        expected: returns 1 row with global aggregation values
        Note: c2 is nullable, filter "c2 >= 0" excludes NULL rows (SQL three-valued logic)
              COUNT(c2) also excludes NULL values in the aggregation
        """
        client = self._client()

        # Note: Milvus requires count(field_name), count(*) is not supported
        # filter "c2 >= 0" excludes rows where c2 is NULL (SQL three-valued logic)
        results, _ = self.query(
            client,
            self.collection_name,
            filter="c2 >= 0",
            group_by_fields=[],
            output_fields=["count(c2)", "sum(c2)", "avg(c3)"]
        )

        # Should return 1 row
        assert len(results) == 1, f"Expected 1 global aggregation row, got {len(results)}"

        # Calculate ground truth
        # Filter excludes NULL c2 rows, then COUNT(c2) counts non-NULL c2 values
        filtered_data = self.datas[self.datas[self.c2_field_name] >= 0]  # Excludes NULL
        expected_count = filtered_data[self.c2_field_name].count()  # COUNT excludes NULL
        expected_sum = filtered_data[self.c2_field_name].sum()  # SUM excludes NULL
        expected_avg = filtered_data[self.c3_field_name].mean()

        result = results[0]
        assert result["count(c2)"] == expected_count, \
            f"Global count mismatch: {result['count(c2)']} != {expected_count}"
        assert result["sum(c2)"] == expected_sum, \
            f"Global sum mismatch: {result['sum(c2)']} != {expected_sum}"
        assert abs(result["avg(c3)"] - expected_avg) < 0.01, \
            f"Global avg mismatch: {result['avg(c3)']} != {expected_avg}"

        log.info("test_global_aggregation_no_group_by passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_star_without_group_by(self):
        """
        target: test Milvus original count(*) functionality (without GROUP BY)
        method: query with group_by_fields=[], output_fields=["count(*)"]
        expected: returns 1 row with total count of all entities
        Note: This tests the original Milvus count(*) feature, which is different from
              aggregation count(field). count(*) counts all entities regardless of NULL values.
        """
        client = self._client()

        # count(*) without GROUP BY should return total entity count
        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            group_by_fields=[],
            output_fields=["count(*)"]
        )

        # Should return 1 row with total count
        assert len(results) == 1, f"Expected 1 row for count(*), got {len(results)}"

        # count(*) should equal total number of entities (3000)
        expected_count = len(self.datas)  # 3000
        assert results[0]["count(*)"] == expected_count, \
            f"count(*) mismatch: {results[0]['count(*)']} != {expected_count}"

        log.info("test_count_star_without_group_by passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_star_vs_count_field(self):
        """
        target: test difference between count(*) and count(field) for nullable fields
        method: compare count(*) result with count(field) result on nullable field
        expected: count(*) counts all entities, count(field) excludes NULL values
        Note: c2 is nullable with ~15% NULL values
              - count(*) should return 3000 (all entities)
              - count(c2) should return ~2550 (non-NULL entities)
        """
        client = self._client()

        # Get count(*) - counts all entities
        results_star, _ = self.query(
            client,
            self.collection_name,
            filter="",
            group_by_fields=[],
            output_fields=["count(*)"]
        )
        count_star = results_star[0]["count(*)"]

        # Get count(c2) - excludes NULL values
        results_field, _ = self.query(
            client,
            self.collection_name,
            filter="",
            group_by_fields=[],
            output_fields=["count(c2)"]
        )
        count_field = results_field[0]["count(c2)"]

        # Verify count(*) equals total entities
        expected_total = len(self.datas)  # 3000
        assert count_star == expected_total, \
            f"count(*) should equal total entities: {count_star} != {expected_total}"

        # Verify count(c2) excludes NULL (should be less than count(*))
        expected_non_null = self.datas[self.c2_field_name].count()  # ~2550
        assert count_field == expected_non_null, \
            f"count(c2) should exclude NULL: {count_field} != {expected_non_null}"

        # Verify count(*) > count(field) for nullable field
        assert count_star > count_field, \
            f"count(*) should be greater than count(nullable_field): {count_star} <= {count_field}"

        log.info(f"count(*) = {count_star}, count(c2) = {count_field}, difference = {count_star - count_field}")
        log.info("test_count_star_vs_count_field passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_star_with_group_by_error(self):
        """
        target: test error when using count(*) with GROUP BY
        method: query with group_by_fields=["c1"], output_fields=["c1", "count(*)"]
        expected: raise error indicating count(*) not allowed with pagination/GROUP BY
        Note: Milvus original count(*) feature does not support GROUP BY.
              For grouped counting, use count(field) instead.
        """
        client = self._client()

        # count(*) with GROUP BY should fail
        error = {ct.err_code: 1100, ct.err_msg: "count entities with pagination is not allowed"}
        self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "count(*)"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        log.info("test_count_star_with_group_by_error passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_aggregation_function_case_insensitive(self):
        """
        target: test that aggregation functions are case-insensitive
        method: query with aggregation functions in different cases (COUNT, count, Count, etc.)
        expected: all variations work and return same results
        """
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field("c1", DataType.VARCHAR, max_length=100)
        schema.add_field("c2", DataType.INT32)
        schema.add_field("c3", DataType.DOUBLE)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        # Insert test data
        rows = [
            {"pk": "pk_1", "c1": "A", "c2": 10, "c3": 1.5, "vec": [0.1] * 8},
            {"pk": "pk_2", "c1": "A", "c2": 20, "c3": 2.5, "vec": [0.2] * 8},
            {"pk": "pk_3", "c1": "B", "c2": 30, "c3": 3.5, "vec": [0.3] * 8},
        ]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # Test different case variations
        test_cases = [
            # lowercase
            ["c1", "count(c2)", "sum(c2)", "min(c2)", "max(c2)", "avg(c3)"],
            # UPPERCASE
            ["c1", "COUNT(c2)", "SUM(c2)", "MIN(c2)", "MAX(c2)", "AVG(c3)"],
            # Mixed case
            ["c1", "Count(c2)", "Sum(c2)", "Min(c2)", "Max(c2)", "Avg(c3)"],
            # Random mixed
            ["c1", "CoUnT(c2)", "sUm(c2)", "mIn(c2)", "MaX(c2)", "AvG(c3)"],
        ]

        expected_group_a = {
            "c1": "A",
            "count": 2,
            "sum": 30,
            "min": 10,
            "max": 20,
            "avg": 2.0
        }

        expected_group_b = {
            "c1": "B",
            "count": 1,
            "sum": 30,
            "min": 30,
            "max": 30,
            "avg": 3.5
        }

        for idx, output_fields in enumerate(test_cases):
            results, _ = self.query(
                client,
                collection_name,
                filter="",
                limit=100,
                group_by_fields=["c1"],
                output_fields=output_fields
            )

            # Verify results
            assert len(results) == 2, f"Test case {idx}: Expected 2 groups, got {len(results)}"

            # Find groups A and B
            group_a = [r for r in results if r["c1"] == "A"][0]
            group_b = [r for r in results if r["c1"] == "B"][0]

            # The result field names will match the case used in output_fields
            count_key = output_fields[1]  # e.g., "count(c2)" or "COUNT(c2)"
            sum_key = output_fields[2]
            min_key = output_fields[3]
            max_key = output_fields[4]
            avg_key = output_fields[5]

            # Verify Group A
            assert group_a[count_key] == expected_group_a["count"], \
                f"Test case {idx}: Group A count mismatch"
            assert group_a[sum_key] == expected_group_a["sum"], \
                f"Test case {idx}: Group A sum mismatch"
            assert group_a[min_key] == expected_group_a["min"], \
                f"Test case {idx}: Group A min mismatch"
            assert group_a[max_key] == expected_group_a["max"], \
                f"Test case {idx}: Group A max mismatch"
            assert abs(group_a[avg_key] - expected_group_a["avg"]) < 0.01, \
                f"Test case {idx}: Group A avg mismatch"

            # Verify Group B
            assert group_b[count_key] == expected_group_b["count"], \
                f"Test case {idx}: Group B count mismatch"
            assert group_b[sum_key] == expected_group_b["sum"], \
                f"Test case {idx}: Group B sum mismatch"
            assert group_b[min_key] == expected_group_b["min"], \
                f"Test case {idx}: Group B min mismatch"
            assert group_b[max_key] == expected_group_b["max"], \
                f"Test case {idx}: Group B max mismatch"
            assert abs(group_b[avg_key] - expected_group_b["avg"]) < 0.01, \
                f"Test case {idx}: Group B avg mismatch"

            log.info(f"Test case {idx} with {output_fields[1]} passed")

        self.drop_collection(client, collection_name)
        log.info("test_aggregation_function_case_insensitive passed")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="limit parameter is ignored in GROUP BY aggregation queries. Tracked in issue #47326")
    def test_filter_and_limit_with_aggregation(self):
        """
        target: test filter + limit + aggregation combination behavior
        method: query with both filter and limit parameters combined with GROUP BY aggregation
        expected: should work correctly (filter input data, group, aggregate, limit output groups)
                  This is standard SQL behavior and common user expectation.
        Note: Currently limit parameter is ignored in aggregation queries (issue #47326)
        """
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field("c1", DataType.VARCHAR, max_length=100)  # group by field
        schema.add_field("c2", DataType.INT16)  # filter field
        schema.add_field("c3", DataType.DOUBLE)  # aggregation field
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        # Insert data: 3 groups (A, B, C) with c2 values ranging from -15 to 14
        rows = []
        groups = ["A", "B", "C"]
        for i in range(30):
            rows.append({
                "pk": f"pk_{i}",
                "c1": groups[i % 3],  # Rotate through A, B, C
                "c2": i - 15,  # Values from -15 to 14
                "c3": float(i) * 1.5,
                "vec": [0.1 * i] * 8
            })

        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # Scenario: filter (c2 >= 0) + GROUP BY c1 + aggregation + limit 2
        # Expected: Filter keeps only rows where c2 >= 0 (15 rows),
        #           Group by c1 (3 groups: A, B, C),
        #           Return only first 2 groups
        results, _ = self.query(
            client,
            collection_name,
            filter="c2 >= 0",  # Filter input data
            limit=2,  # Limit output groups
            group_by_fields=["c1"],
            output_fields=["c1", "count(c2)", "sum(c3)", "avg(c3)"]
        )

        log.info(f"Results: {results}")

        # Verify results
        assert len(results) <= 2, f"Expected at most 2 groups due to limit, got {len(results)}"

        # Each group should have aggregated values from filtered data only (c2 >= 0)
        for result in results:
            assert result["c1"] in ["A", "B", "C"]
            # Verify aggregation is computed from filtered data
            # For c2 >= 0, we have values 0-14 (15 values), distributed across 3 groups
            # Group A: c2 = 0, 3, 6, 9, 12 (5 values)
            # Group B: c2 = 1, 4, 7, 10, 13 (5 values)
            # Group C: c2 = 2, 5, 8, 11, 14 (5 values)
            assert result["count(c2)"] == 5, f"Each group should have 5 filtered rows, got {result['count(c2)']}"

        self.drop_collection(client, collection_name)
        log.info("test_filter_and_limit_with_aggregation passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_group_by_int8_field(self):
        """
        target: test GROUP BY on INT8 field with aggregation functions
        method: query with group_by_fields=[INT8 field], apply COUNT and SUM aggregations
        expected: return correct aggregation results for each INT8 group value, validate against pandas ground truth
        """
        client = self._client()

        # Query with GROUP BY on INT8 field
        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c7_field_name],
            output_fields=[self.c7_field_name, "count(c2)", "sum(c3)"]
        )

        # Verify against ground truth
        ground_truth = self.datas.groupby(self.c7_field_name).agg(
            count_c2=(self.c2_field_name, "count"),
            sum_c3=(self.c3_field_name, "sum")
        ).reset_index()

        # Should have 5 groups (unique INT8 values: 1, 2, 3, 4, 5)
        assert len(results) == 5, f"Expected 5 groups for INT8 field, got {len(results)}"

        # Verify each group's aggregation values
        for result in results:
            c7_value = result[self.c7_field_name]
            expected = ground_truth[ground_truth[self.c7_field_name] == c7_value].iloc[0]
            assert result["count(c2)"] == expected["count_c2"], \
                f"INT8 group {c7_value}: count mismatch, expected {expected['count_c2']}, got {result['count(c2)']}"
            assert result["sum(c3)"] == expected["sum_c3"], \
                f"INT8 group {c7_value}: sum mismatch, expected {expected['sum_c3']}, got {result['sum(c3)']}"

        log.info("test_group_by_int8_field passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_group_by_int64_field(self):
        """
        target: test GROUP BY on INT64 field (non-timestamp) with aggregation functions
        method: query with group_by_fields=[INT64 field], apply COUNT and AVG aggregations
        expected: return correct aggregation results for each INT64 group value, validate against pandas ground truth
        """
        client = self._client()

        # Query with GROUP BY on INT64 field
        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c8_field_name],
            output_fields=[self.c8_field_name, "count(c2)", "avg(c4)"]
        )

        # Verify against ground truth
        ground_truth = self.datas.groupby(self.c8_field_name).agg(
            count_c2=(self.c2_field_name, "count"),
            avg_c4=(self.c4_field_name, "mean")
        ).reset_index()

        # Should have 5 groups (unique INT64 values: 100, 200, 300, 400, 500)
        assert len(results) == 5, f"Expected 5 groups for INT64 field, got {len(results)}"

        # Verify each group's aggregation values
        for result in results:
            c8_value = result[self.c8_field_name]
            expected = ground_truth[ground_truth[self.c8_field_name] == c8_value].iloc[0]
            assert result["count(c2)"] == expected["count_c2"], \
                f"INT64 group {c8_value}: count mismatch, expected {expected['count_c2']}, got {result['count(c2)']}"
            assert abs(result["avg(c4)"] - expected["avg_c4"]) < 0.01, \
                f"INT64 group {c8_value}: avg mismatch, expected {expected['avg_c4']}, got {result['avg(c4)']}"

        log.info("test_group_by_int64_field passed")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="GROUP BY field not in output_fields should error but succeeds. Tracked in issue #47334")
    def test_group_by_field_not_in_output_fields(self):
        """
        target: test error when group_by field is not in output_fields
        method: query with group_by_fields=["c1"] but output_fields=["count(c2)"] (missing c1)
        expected: Should raise error indicating group_by field must be in output_fields
        Note: Design requirement states "分组字段必须包含在 output_fields 中，否则应该报错"
              Currently Milvus allows this query but returns useless results without group keys (issue #47334)
        """
        client = self._client()

        # Query with missing group_by field in output_fields
        # According to design, this should raise an error
        error = {ct.err_code: 1, ct.err_msg: ""}  # Expected error
        self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=["count(c2)"],  # Missing c1 - should error
            check_task=CheckTasks.err_res,
            check_items=error
        )

        log.info("test_group_by_field_not_in_output_fields passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_unsupported_vector_field(self):
        """
        target: test error for Vector field in GROUP BY
        method: query with group_by_fields=["vector_field"]
        expected: raise error indicating Vector not supported
        """
        client = self._client()

        # Try to group by vector field - should fail with invalid parameter error
        error = {ct.err_code: 1100, ct.err_msg: f"group by field {self.vector_field_name} has unsupported data type FloatVector"}
        self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.vector_field_name],
            output_fields=[self.vector_field_name, "count(c1)"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        log.info("test_unsupported_vector_field passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_unsupported_aggregation_function_varchar(self):
        """
        target: test error for SUM on VarChar field
        method: query with output_fields=["sum(varchar_field)"]
        expected: raise error indicating VarChar doesn't support SUM
        """
        client = self._client()

        # Try SUM on VarChar field
        error = {ct.err_code: 65535, ct.err_msg: f"aggregation operator sum does not support data type VarChar"}
        self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, f"sum({self.c6_field_name})"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        log.info("test_unsupported_aggregation_function_varchar passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_mixed_aggregation_and_non_aggregation_fields(self):
        """
        target: test error when output_fields mixes aggregation and non-aggregation fields
        method: query with output_fields containing both regular field (not in group_by) and aggregation
        expected: raise error indicating only group_by fields and aggregation fields allowed
        """
        client = self._client()

        # Try to mix regular field (c3) with aggregation - c3 is not in group_by_fields
        # Note: The error message is not precise enough, should be improved (see GitHub issue)
        error = {ct.err_code: 65535, ct.err_msg: "no indices found for output field"}
        self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, self.c3_field_name, "count(c2)"],  # c3 is neither group_by field nor aggregation
            check_task=CheckTasks.err_res,
            check_items=error
        )

        log.info("test_mixed_aggregation_and_non_aggregation_fields passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_invalid_aggregation_function_syntax(self):
        """
        target: test error for invalid aggregation function syntax
        method: query with invalid function names or syntax
        expected: raise error indicating syntax error
        Note: Milvus aggregation functions are case-insensitive (COUNT, count, Count all work)
        """
        client = self._client()

        # Test case 1: Unknown aggregation function
        error = {ct.err_code: 1, ct.err_msg: ""}
        self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "median(c2)"],  # Unsupported function
            check_task=CheckTasks.err_res,
            check_items=error
        )

        # Test case 2: Malformed syntax (missing closing parenthesis)
        self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "count(c2"],  # Missing closing parenthesis
            check_task=CheckTasks.err_res,
            check_items=error
        )

        # Test case 3: Empty function call
        self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "count()"],  # Empty parameter
            check_task=CheckTasks.err_res,
            check_items=error
        )

        log.info("test_invalid_aggregation_function_syntax passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_unsupported_float_type_for_groupby(self):
        """
        target: test error for FLOAT field in GROUP BY
        method: query with group_by_fields=[FLOAT field]
        expected: raise error indicating FLOAT type not supported for GROUP BY
        Note: Floating point types are not suitable for GROUP BY due to precision issues
        """
        client = self._client()

        # Try to group by FLOAT field - should fail
        error = {ct.err_code: 1100, ct.err_msg: f"group by field {self.c9_field_name} has unsupported data type Float"}
        self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c9_field_name],
            output_fields=[self.c9_field_name, "count(c1)"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        log.info("test_unsupported_float_type_for_groupby passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_unsupported_double_type_for_groupby(self):
        """
        target: test error for DOUBLE field in GROUP BY
        method: query with group_by_fields=[DOUBLE field]
        expected: raise error indicating DOUBLE type not supported for GROUP BY
        Note: Floating point types are not suitable for GROUP BY due to precision issues
        """
        client = self._client()

        # Try to group by DOUBLE field - should fail
        error = {ct.err_code: 1100, ct.err_msg: f"group by field {self.c4_field_name} has unsupported data type Double"}
        self.query(
            client,
            self.collection_name,
            filter="",
            limit=100,
            group_by_fields=[self.c4_field_name],
            output_fields=[self.c4_field_name, "count(c1)"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        log.info("test_unsupported_double_type_for_groupby passed")


@pytest.mark.xdist_group("TestQueryAggregationIndependentV2")
class TestQueryAggregationIndependentV2(TestMilvusClientV2Base):
    """
    Test Query Aggregation scenarios requiring special schemas

    These tests need independent collections because they require:
    - JSON fields
    - Array fields

    Note: Nullable field aggregation is now covered by the shared collection tests.
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_unsupported_json_type(self):
        """
        target: test error for JSON field in GROUP BY or aggregation
        method: query with JSON field in group_by_fields or aggregation functions
        expected: raise error indicating JSON type not supported
        """
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field("c1", DataType.VARCHAR, max_length=100)
        schema.add_field("json_field", DataType.JSON)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        rows = [{"pk": "pk_1", "c1": "A", "json_field": {"key": "value"}, "vec": [0.1] * 8}]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # Try to group by JSON field
        error = {ct.err_code: 1100, ct.err_msg: "group by field json_field has unsupported data type JSON"}
        self.query(
            client,
            collection_name,
            filter="",
            limit=100,
            group_by_fields=["json_field"],
            output_fields=["json_field", "count(c1)"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        self.drop_collection(client, collection_name)
        log.info("test_unsupported_json_type passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_unsupported_array_type(self):
        """
        target: test error for Array field in GROUP BY or aggregation
        method: query with Array field in group_by_fields or aggregation functions
        expected: raise error indicating Array type not supported
        """
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field("c1", DataType.VARCHAR, max_length=100)
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=10)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        rows = [{"pk": "pk_1", "c1": "A", "array_field": [1, 2, 3], "vec": [0.1] * 8}]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # Try to group by Array field
        error = {ct.err_code: 1100, ct.err_msg: "group by field array_field has unsupported data type Array"}
        self.query(
            client,
            collection_name,
            filter="",
            limit=100,
            group_by_fields=["array_field"],
            output_fields=["array_field", "count(c1)"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        self.drop_collection(client, collection_name)
        log.info("test_unsupported_array_type passed")
