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


@pytest.mark.xdist_group("TestQueryAggregationL0V2")
@pytest.mark.tags(CaseLabel.L0)
class TestQueryAggregationL0V2(TestMilvusClientV2Base):
    """
    Test basic Query Aggregation functionality (L0 priority)

    These tests cover the most common scenarios:
    - Single column GROUP BY with COUNT/SUM
    - Multiple columns GROUP BY with MIN/MAX
    """

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestQueryAggregationL0" + cf.gen_unique_str("_")
        self.pk_field_name = "pk"
        self.c1_field_name = "c1"
        self.c2_field_name = "c2"
        self.c3_field_name = "c3"
        self.c4_field_name = "c4"
        self.ts_field_name = "ts"
        self.vector_field_name = "c5"
        self.c6_field_name = "c6"
        self.datas = []

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self, request):
        """
        Prepare collection with aggregation test data

        Schema:
        - pk: VarChar (primary key)
        - c1: VarChar (grouping field, 7 unique values)
        - c2: Int16 (aggregation field)
        - c3: Int32 (aggregation field)
        - c4: Double (aggregation field)
        - ts: Int64 (timestamp field, simulating Timestamptz)
        - c5: FloatVector (dim=8)
        - c6: VarChar (grouping field, 7 unique values)
        """
        client = self._client()

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(self.pk_field_name, DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field(self.c1_field_name, DataType.VARCHAR, max_length=100)
        schema.add_field(self.c2_field_name, DataType.INT16)
        schema.add_field(self.c3_field_name, DataType.INT32)
        schema.add_field(self.c4_field_name, DataType.DOUBLE)
        schema.add_field(self.ts_field_name, DataType.INT64)
        schema.add_field(self.vector_field_name, DataType.FLOAT_VECTOR, dim=8)
        schema.add_field(self.c6_field_name, DataType.VARCHAR, max_length=100)

        # Create collection
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        # Generate test data (3000 rows)
        unique_values_c1 = ["A", "B", "C", "D", "E", "F", "G"]
        unique_values_c6 = ["X", "Y", "Z", "W", "V", "U", "T"]

        np.random.seed(19530)
        rows = []
        for i in range(default_nb):
            row = {
                self.pk_field_name: f"pk_{i}",
                self.c1_field_name: np.random.choice(unique_values_c1),
                self.c2_field_name: int(np.random.randint(0, 100, dtype=np.int16)),
                self.c3_field_name: int(np.random.randint(0, 1000, dtype=np.int32)),
                self.c4_field_name: float(np.random.uniform(0, 100)),
                self.ts_field_name: int(np.random.randint(1000000, 2000000, dtype=np.int64)),
                self.vector_field_name: [float(x) for x in np.random.random(8)],
                self.c6_field_name: np.random.choice(unique_values_c6),
            }
            rows.append(row)

        # Insert data
        self.insert(client, self.collection_name, data=rows)
        self.flush(client, self.collection_name)

        # Create index on vector field
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

        # Store data for ground truth verification
        self.datas = pd.DataFrame(rows)

        log.info(f"Prepared collection {self.collection_name} with {default_nb} entities")

        def teardown():
            self.drop_collection(self._client(), self.collection_name)

        request.addfinalizer(teardown)

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


@pytest.mark.xdist_group("TestQueryAggregationL1V2")
@pytest.mark.tags(CaseLabel.L1)
class TestQueryAggregationL1V2(TestMilvusClientV2Base):
    """
    Test common Query Aggregation scenarios (L1 priority)

    These tests cover:
    - Multiple aggregation functions in one query
    - GROUP BY with filter expressions
    - GROUP BY with limit
    - Different data types (VarChar, Timestamptz)
    - Return type validations (SUM, AVG)
    - Empty result sets
    - Global aggregation (no GROUP BY)
    """

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestQueryAggregationL1" + cf.gen_unique_str("_")
        self.pk_field_name = "pk"
        self.c1_field_name = "c1"
        self.c2_field_name = "c2"
        self.c3_field_name = "c3"
        self.c4_field_name = "c4"
        self.ts_field_name = "ts"
        self.vector_field_name = "c5"
        self.c6_field_name = "c6"
        self.datas = []

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self, request):
        """
        Prepare collection with aggregation test data (same as L0)
        """
        client = self._client()

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(self.pk_field_name, DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field(self.c1_field_name, DataType.VARCHAR, max_length=100)
        schema.add_field(self.c2_field_name, DataType.INT16)
        schema.add_field(self.c3_field_name, DataType.INT32)
        schema.add_field(self.c4_field_name, DataType.DOUBLE)
        schema.add_field(self.ts_field_name, DataType.INT64)
        schema.add_field(self.vector_field_name, DataType.FLOAT_VECTOR, dim=8)
        schema.add_field(self.c6_field_name, DataType.VARCHAR, max_length=100)

        # Create collection
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        # Generate test data
        unique_values_c1 = ["A", "B", "C", "D", "E", "F", "G"]
        unique_values_c6 = ["X", "Y", "Z", "W", "V", "U", "T"]

        np.random.seed(19530)
        rows = []
        for i in range(default_nb):
            row = {
                self.pk_field_name: f"pk_{i}",
                self.c1_field_name: np.random.choice(unique_values_c1),
                self.c2_field_name: int(np.random.randint(0, 100, dtype=np.int16)),
                self.c3_field_name: int(np.random.randint(0, 1000, dtype=np.int32)),
                self.c4_field_name: float(np.random.uniform(0, 100)),
                self.ts_field_name: int(np.random.randint(1000000, 2000000, dtype=np.int64)),
                self.vector_field_name: [float(x) for x in np.random.random(8)],
                self.c6_field_name: np.random.choice(unique_values_c6),
            }
            rows.append(row)

        self.insert(client, self.collection_name, data=rows)
        self.flush(client, self.collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=self.vector_field_name,
            metric_type="L2",
            index_type="FLAT",
            params={}
        )
        self.create_index(client, self.collection_name, index_params=index_params)
        self.load_collection(client, self.collection_name)

        self.datas = pd.DataFrame(rows)
        log.info(f"Prepared collection {self.collection_name} with {default_nb} entities")

        def teardown():
            self.drop_collection(self._client(), self.collection_name)

        request.addfinalizer(teardown)

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
    def test_group_by_with_limit(self):
        """
        target: test GROUP BY without filter but with limit
        method: query with filter="", group_by_fields=["c1"], output_fields=["c1", "avg(c2)"], limit=3
        expected: returns at most 3 groups
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
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "sum(c2)"]
        )

        # Test integer SUM (Int32)
        results_int32, _ = self.query(
            client,
            self.collection_name,
            filter="",
            group_by_fields=[self.c1_field_name],
            output_fields=[self.c1_field_name, "sum(c3)"]
        )

        # Test float SUM (Double)
        results_double, _ = self.query(
            client,
            self.collection_name,
            filter="",
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_global_aggregation_no_group_by(self):
        """
        target: test global aggregation without GROUP BY
        method: query with group_by_fields=[], output_fields=["count(*)", "sum(c2)", "avg(c3)"]
        expected: returns 1 row with global aggregation values
        """
        client = self._client()

        results, _ = self.query(
            client,
            self.collection_name,
            filter="",
            group_by_fields=[],
            output_fields=["count(*)", "sum(c2)", "avg(c3)"]
        )

        # Should return 1 row
        assert len(results) == 1, f"Expected 1 global aggregation row, got {len(results)}"

        # Calculate ground truth
        expected_count = len(self.datas)
        expected_sum = self.datas[self.c2_field_name].sum()
        expected_avg = self.datas[self.c3_field_name].mean()

        result = results[0]
        assert result["count(*)"] == expected_count, \
            f"Global count mismatch: {result['count(*)']} != {expected_count}"
        assert result["sum(c2)"] == expected_sum, \
            f"Global sum mismatch: {result['sum(c2)']} != {expected_sum}"
        assert abs(result["avg(c3)"] - expected_avg) < 0.01, \
            f"Global avg mismatch: {result['avg(c3)']} != {expected_avg}"

        log.info("test_global_aggregation_no_group_by passed")


@pytest.mark.tags(CaseLabel.L1)
class TestQueryAggregationNegativeV2(TestMilvusClientV2Base):
    """
    Test negative scenarios for Query Aggregation

    These tests verify error handling for:
    - Missing group_by fields in output_fields
    - Unsupported data types (Vector)
    - Invalid aggregation function combinations
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_group_by_field_not_in_output_fields(self):
        """
        target: test error when group_by field is not in output_fields
        method: query with group_by_fields=["c1"] but output_fields=["count(c2)"] (missing c1)
        expected: raise error indicating group_by fields must be in output_fields
        """
        client = self._client()

        # Create simple collection
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field("c1", DataType.VARCHAR, max_length=100)
        schema.add_field("c2", DataType.INT16)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        # Insert minimal data
        rows = [
            {"pk": "pk_1", "c1": "A", "c2": 10, "vec": [0.1] * 8},
            {"pk": "pk_2", "c1": "B", "c2": 20, "vec": [0.2] * 8},
        ]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # Query with missing group_by field in output_fields
        error = {ct.err_code: 65535, ct.err_msg: "group by field"}
        self.query(
            client,
            collection_name,
            filter="",
            group_by_fields=["c1"],
            output_fields=["count(c2)"],  # Missing c1
            check_task=CheckTasks.err_res,
            check_items=error
        )

        self.drop_collection(client, collection_name)
        log.info("test_group_by_field_not_in_output_fields passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_unsupported_vector_field(self):
        """
        target: test error for Vector field in GROUP BY
        method: query with group_by_fields=["vector_field"]
        expected: raise error indicating Vector not supported
        """
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field("c1", DataType.VARCHAR, max_length=100)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        rows = [{"pk": "pk_1", "c1": "A", "vec": [0.1] * 8}]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # Try to group by vector field
        error = {ct.err_code: 65535, ct.err_msg: "vector"}
        self.query(
            client,
            collection_name,
            filter="",
            group_by_fields=["vec"],
            output_fields=["vec", "count(c1)"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        self.drop_collection(client, collection_name)
        log.info("test_unsupported_vector_field passed")

    @pytest.mark.tags(CaseLabel.L1)
    def test_unsupported_aggregation_function_varchar(self):
        """
        target: test error for SUM on VarChar field
        method: query with output_fields=["sum(varchar_field)"]
        expected: raise error indicating VarChar doesn't support SUM
        """
        client = self._client()

        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field("c1", DataType.VARCHAR, max_length=100)
        schema.add_field("c6", DataType.VARCHAR, max_length=100)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        rows = [{"pk": "pk_1", "c1": "A", "c6": "X", "vec": [0.1] * 8}]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", metric_type="L2", index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # Try SUM on VarChar field
        error = {ct.err_code: 65535, ct.err_msg: "sum"}
        self.query(
            client,
            collection_name,
            filter="",
            group_by_fields=["c1"],
            output_fields=["c1", "sum(c6)"],
            check_task=CheckTasks.err_res,
            check_items=error
        )

        self.drop_collection(client, collection_name)
        log.info("test_unsupported_aggregation_function_varchar passed")
