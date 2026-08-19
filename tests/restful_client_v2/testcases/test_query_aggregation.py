import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

DIM = 4
NB = 200
SELECTIVE_FILTER = 'category in ["category_1", "category_3"] && id >= 20 && id < 90'


def _rows():
    return [
        {
            "id": i,
            "price": 10 + ((i // 5) % 10),
            "score": (i * 17 + 11) % NB,
            "nullable_value": None if i % 4 == 0 else (i * 7) % 31,
            "category": f"category_{i % 5}",
            "vector": [float(i), 0.0, 0.0, 0.0],
        }
        for i in range(NB)
    ]


def _expected_grouped_rows(group_by_fields, aggregate_fields=("count(*)", "sum(score)"), row_filter=None):
    groups = {}
    for row in _rows():
        if row_filter is not None and not row_filter(row):
            continue
        key = tuple(row[field] for field in group_by_fields)
        groups.setdefault(key, []).append(row)

    expected_rows = []
    for key, grouped_rows in groups.items():
        expected = dict(zip(group_by_fields, key))
        for aggregate_field in aggregate_fields:
            if aggregate_field == "count(*)":
                expected[aggregate_field] = len(grouped_rows)
                continue

            aggregate_function, field_name = aggregate_field[:-1].split("(", 1)
            values = [row[field_name] for row in grouped_rows if row[field_name] is not None]
            if aggregate_function == "count":
                expected[aggregate_field] = len(values)
            elif aggregate_function == "sum":
                expected[aggregate_field] = sum(values)
            elif aggregate_function == "min":
                expected[aggregate_field] = min(values)
            elif aggregate_function == "max":
                expected[aggregate_field] = max(values)
            elif aggregate_function == "avg":
                expected[aggregate_field] = sum(values) / len(values)
            else:
                raise AssertionError(f"Unsupported aggregate expression: {aggregate_field}")
        expected_rows.append(expected)
    return expected_rows


class TestQueryAggregation(TestBase):
    @pytest.fixture(scope="class", autouse=True)
    def prepare_shared_query_aggregation_collection(self, request, init_class_config):
        collection_name = gen_collection_name(prefix=request.cls.__name__)
        request.cls.collection_name = collection_name
        collection_client, vector_client = self._class_scope_clients()

        def teardown():
            collection_client.collection_drop({"collectionName": collection_name})

        request.addfinalizer(teardown)
        payload = {
            "collectionName": collection_name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "price", "dataType": "Int64"},
                    {"fieldName": "score", "dataType": "Int64"},
                    {"fieldName": "nullable_value", "dataType": "Int64", "nullable": True},
                    {"fieldName": "category", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(DIM)}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_index", "metricType": "L2"}],
        }
        rsp = collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        collection_client.wait_load_completed(collection_name, timeout=60)

        rows = _rows()
        rsp = vector_client.vector_insert({"collectionName": collection_name, "data": rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == len(rows)
        rsp = collection_client.flush(collection_name)
        assert rsp["code"] == 0, rsp

    def _query(self, payload, timeout=1):
        rsp = self.vector_client.vector_query(payload, timeout=timeout)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_group_by_single_field_count_sum(self):
        """
        target: verify REST query supports groupByFields with aggregation expressions
        method: selectively filter IDs and categories, then group by category and calculate count(*) and sum(score)
        expected: only filtered categories return their exact filtered count and sum
        """
        rows = self._query(
            {
                "collectionName": self.collection_name,
                "filter": SELECTIVE_FILTER,
                "limit": 10,
                "outputFields": ["category", "count(*)", "sum(score)"],
                "groupByFields": ["category"],
                "orderByFields": ["category:asc"],
            }
        )

        expected = sorted(
            _expected_grouped_rows(
                ["category"],
                row_filter=lambda row: row["category"] in {"category_1", "category_3"} and 20 <= row["id"] < 90,
            ),
            key=lambda row: row["category"],
        )
        assert [row["category"] for row in rows] == ["category_1", "category_3"]
        assert rows == expected

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_group_by_omits_group_key_from_output_fields(self):
        """
        target: verify REST query remaps grouped aggregate results without injecting omitted group keys
        method: group by category while requesting only count(*) and sum(score) in outputFields
        expected: each result contains exactly the requested aggregates and matches the category-group oracle order
        """
        rows = self._query(
            {
                "collectionName": self.collection_name,
                "filter": SELECTIVE_FILTER,
                "limit": 10,
                "outputFields": ["count(*)", "sum(score)"],
                "groupByFields": ["category"],
                "orderByFields": ["category:asc"],
            }
        )
        expected = sorted(
            _expected_grouped_rows(
                ["category"],
                row_filter=lambda row: row["category"] in {"category_1", "category_3"} and 20 <= row["id"] < 90,
            ),
            key=lambda row: row["category"],
        )
        assert [set(row) for row in rows] == [{"count(*)", "sum(score)"}] * len(expected)
        assert rows == [{key: row[key] for key in ("count(*)", "sum(score)")} for row in expected]

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_group_by_nullable_count_min_max_avg(self):
        """
        target: verify REST query supports count(field), min, max, and avg aggregation expressions
        method: filter an uneven prefix, group nullable numeric values by category, and request four aggregate functions
        expected: non-uniform group counts and each aggregate match the filtered non-NULL source values
        """

        def row_filter(row):
            return row["id"] < 97

        aggregate_fields = (
            "count(*)",
            "count(nullable_value)",
            "min(nullable_value)",
            "max(nullable_value)",
            "avg(nullable_value)",
        )
        rows = self._query(
            {
                "collectionName": self.collection_name,
                "filter": "id < 97",
                "limit": 10,
                "outputFields": ["category", *aggregate_fields],
                "groupByFields": ["category"],
                "orderByFields": ["category:asc"],
            }
        )

        expected = sorted(
            _expected_grouped_rows(
                ["category"],
                aggregate_fields=aggregate_fields,
                row_filter=row_filter,
            ),
            key=lambda row: row["category"],
        )
        assert len(rows) == len(expected)
        for actual, expected_row in zip(rows, expected):
            assert set(actual) == {"category", *aggregate_fields}
            assert actual["category"] == expected_row["category"]
            assert actual["count(*)"] == expected_row["count(*)"]
            assert actual["count(nullable_value)"] == expected_row["count(nullable_value)"]
            assert actual["count(nullable_value)"] < actual["count(*)"]
            assert actual["min(nullable_value)"] == expected_row["min(nullable_value)"]
            assert actual["max(nullable_value)"] == expected_row["max(nullable_value)"]
            assert actual["avg(nullable_value)"] == pytest.approx(expected_row["avg(nullable_value)"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_group_by_nullable_field(self):
        """
        target: verify REST query returns a group for a nullable groupByFields key
        method: group all rows by nullable_value and calculate count(*) and sum(score)
        expected: every numeric key and the NULL key return the exact aggregate values
        """
        rows = self._query(
            {
                "collectionName": self.collection_name,
                "filter": "id >= 0",
                "limit": 40,
                "outputFields": ["nullable_value", "count(*)", "sum(score)"],
                "groupByFields": ["nullable_value"],
            }
        )

        expected = _expected_grouped_rows(["nullable_value"])
        actual_by_key = {row["nullable_value"]: row for row in rows}
        expected_by_key = {row["nullable_value"]: row for row in expected}
        assert len(rows) == len(expected)
        assert None in actual_by_key
        assert actual_by_key == expected_by_key

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_group_by_without_order_by(self):
        """
        target: verify REST query supports groupByFields without orderByFields
        method: group by category and calculate count(*) and sum(score) without requesting group ordering
        expected: every group and aggregate value matches the source rows regardless of response order
        """
        rows = self._query(
            {
                "collectionName": self.collection_name,
                "filter": "id >= 0",
                "limit": 10,
                "outputFields": ["category", "count(*)", "sum(score)"],
                "groupByFields": ["category"],
            }
        )

        expected = _expected_grouped_rows(["category"])
        assert sorted(rows, key=lambda row: row["category"]) == sorted(expected, key=lambda row: row["category"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_group_by_multi_fields_with_order_by(self):
        """
        target: verify REST query forwards multiple groupByFields with orderByFields
        method: group by category and price, then order groups by category asc and price desc
        expected: group keys, aggregate values, and ordering all match the source rows
        """
        rows = self._query(
            {
                "collectionName": self.collection_name,
                "filter": "id >= 0",
                "limit": 20,
                "outputFields": ["category", "price", "count(*)", "sum(score)"],
                "groupByFields": ["category", "price"],
                "orderByFields": ["category:asc", "price:desc"],
            }
        )

        expected = sorted(
            _expected_grouped_rows(["category", "price"]),
            key=lambda row: (row["category"], -row["price"]),
        )[:20]
        assert rows == expected

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_group_by_count_with_limit_offset(self):
        """
        target: verify grouped aggregates apply limit and offset after explicit group ordering
        method: order category groups descending, skip the first group, and return two count/sum rows
        expected: REST returns exact category_3 and category_2 aggregate rows in that order
        """
        rows = self._query(
            {
                "collectionName": self.collection_name,
                "filter": "id >= 0",
                "limit": 2,
                "offset": 1,
                "outputFields": ["category", "count(*)", "sum(score)"],
                "groupByFields": ["category"],
                "orderByFields": ["category:desc"],
            }
        )

        assert rows == [
            {"category": "category_3", "count(*)": 40, "sum(score)": 3980},
            {"category": "category_2", "count(*)": 40, "sum(score)": 3900},
        ]

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_global_count_star_keeps_legacy_limit_behavior(self):
        """
        target: verify global count(*) behavior remains unchanged after adding groupByFields
        method: query count(*) without groupByFields while sending limit and offset
        expected: one global row returns the full collection count
        """
        rows = self._query(
            {
                "collectionName": self.collection_name,
                "filter": "id >= 0",
                "limit": 1,
                "offset": 10,
                "outputFields": ["count(*)"],
            }
        )
        assert rows == [{"count(*)": NB}]

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_global_mixed_count_aggregates_keep_legacy_limit_behavior(self):
        """
        target: verify mixed global count aggregates retain the legacy REST default-limit behavior
        method: query count(*) and sum(score) without groupByFields or an explicit limit
        expected: Proxy rejects the REST default limit because global count(*) cannot use pagination
        """
        rsp = self.vector_client.vector_query(
            {
                "collectionName": self.collection_name,
                "filter": "id >= 0",
                "outputFields": ["count(*)", "sum(score)"],
            }
        )
        assert rsp["code"] == 1100, rsp
        assert "count entities with pagination is not allowed" in rsp["message"], rsp

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_group_by_invalid_field(self):
        """
        target: verify REST query reports server validation for an invalid groupByFields entry
        method: group count(*) by a field absent from the collection schema
        expected: request fails with parameter error code 1100
        """
        rsp = self.vector_client.vector_query(
            {
                "collectionName": self.collection_name,
                "filter": "id >= 0",
                "limit": 10,
                "outputFields": ["count(*)"],
                "groupByFields": ["unknown_group_field"],
            }
        )
        assert rsp["code"] == 1100, rsp
        assert "unknown_group_field" in rsp["message"], rsp
