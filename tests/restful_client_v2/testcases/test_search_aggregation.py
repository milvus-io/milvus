import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

ROWS = [
    {"id": 1, "category": "A", "brand": "X", "price": 30, "vector": [1.0, 0.0]},
    {"id": 2, "category": "A", "brand": "Y", "price": 10, "vector": [0.9, 0.0]},
    {"id": 3, "category": "B", "brand": "X", "price": 20, "vector": [0.8, 0.0]},
    {"id": 4, "category": "C", "brand": "Y", "price": 40, "vector": [0.7, 0.0]},
    {"id": 5, "category": "A", "brand": "X", "price": 5, "vector": [0.6, 0.0]},
]


class TestSearchAggregation(TestBase):
    @pytest.fixture(scope="class", autouse=True)
    def prepare_shared_search_aggregation_collection(self, request, init_class_config):
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
                    {"fieldName": "category", "dataType": "VarChar", "elementTypeParams": {"max_length": "16"}},
                    {"fieldName": "brand", "dataType": "VarChar", "elementTypeParams": {"max_length": "16"}},
                    {"fieldName": "price", "dataType": "Int64"},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": "2"}},
                ],
            },
            "indexParams": [
                {
                    "fieldName": "vector",
                    "indexName": "vector_index",
                    "indexType": "FLAT",
                    "metricType": "IP",
                }
            ],
        }
        rsp = collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        collection_client.wait_load_completed(collection_name, timeout=60)

        rsp = vector_client.vector_insert({"collectionName": collection_name, "data": ROWS})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == len(ROWS)
        rsp = collection_client.flush(collection_name)
        assert rsp["code"] == 0, rsp

    @staticmethod
    def _bucket_key(bucket, field_name):
        for key in bucket["key"]:
            if key["fieldName"] == field_name:
                return key["value"]
        raise AssertionError(f"field {field_name} not found in bucket key {bucket['key']}")

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_single_field_with_all_metrics(self):
        """
        target: verify aggregation size and metrics are independent of the ordinary search limit
        method: filter five rows, request limit 1, and aggregate three category buckets with three topHits per bucket
        expected: all three buckets and every filtered row contribute to the exact metrics despite limit 1
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "limit": 1,
                "filter": "id < 10",
                "outputFields": ["category", "price"],
                "searchAggregation": {
                    "fields": ["category"],
                    "size": 3,
                    "metrics": {
                        "item_count": {"op": "count", "fieldName": "*"},
                        "total_price": {"op": "sum", "fieldName": "price"},
                        "average_price": {"op": "avg", "fieldName": "price"},
                        "minimum_price": {"op": "min", "fieldName": "price"},
                        "maximum_price": {"op": "max", "fieldName": "price"},
                    },
                    "order": [{"key": "_key", "direction": "asc"}],
                    "topHits": {"size": 3, "sort": [{"fieldName": "price", "direction": "asc"}]},
                },
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["aggTopks"] == [3]

        buckets = rsp["data"][0]["buckets"]
        assert [self._bucket_key(bucket, "category") for bucket in buckets] == ["A", "B", "C"]
        for bucket in buckets:
            category = self._bucket_key(bucket, "category")
            expected_rows = [row for row in ROWS if row["category"] == category]
            expected_prices = [row["price"] for row in expected_rows]
            metrics = bucket["metrics"]
            assert int(bucket["count"]) == len(expected_prices)
            assert int(metrics["item_count"]) == len(expected_prices)
            assert int(metrics["total_price"]) == sum(expected_prices)
            assert metrics["average_price"] == pytest.approx(sum(expected_prices) / len(expected_prices))
            assert int(metrics["minimum_price"]) == min(expected_prices)
            assert int(metrics["maximum_price"]) == max(expected_prices)
            expected_hits = sorted(expected_rows, key=lambda row: row["price"])
            assert [int(hit["id"]) for hit in bucket["hits"]] == [row["id"] for row in expected_hits]
            assert [int(hit["price"]) for hit in bucket["hits"]] == [row["price"] for row in expected_hits]

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_composite_fields_ordered_by_metric(self):
        """
        target: verify composite grouping fields and metric-based bucket ordering
        method: group by category and brand, then order the buckets by total price descending
        expected: composite keys and total prices follow the requested order
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "limit": 3,
                "filter": "id <= 3",
                "outputFields": ["category", "brand", "price"],
                "searchAggregation": {
                    "fields": ["category", "brand"],
                    "size": 3,
                    "metrics": {"total_price": {"op": "sum", "fieldName": "price"}},
                    "order": [{"key": "total_price", "direction": "desc"}],
                    "topHits": {"size": 2, "sort": [{"fieldName": "price", "direction": "asc"}]},
                },
            }
        )
        assert rsp["code"] == 0, rsp

        candidate_rows = [row for row in ROWS if row["id"] <= 3]
        expected_groups = {}
        for row in candidate_rows:
            expected_groups.setdefault((row["category"], row["brand"]), []).append(row)
        assert rsp["aggTopks"] == [len(expected_groups)]

        buckets = rsp["data"][0]["buckets"]
        actual = [
            (
                self._bucket_key(bucket, "category"),
                self._bucket_key(bucket, "brand"),
                int(bucket["metrics"]["total_price"]),
                int(bucket["count"]),
            )
            for bucket in buckets
        ]
        assert all([key["fieldName"] for key in bucket["key"]] == ["category", "brand"] for bucket in buckets)
        expected = sorted(
            [
                (category, brand, sum(row["price"] for row in grouped_rows), len(grouped_rows))
                for (category, brand), grouped_rows in expected_groups.items()
            ],
            key=lambda item: item[2],
            reverse=True,
        )
        assert actual == expected
        for bucket in buckets:
            group_key = (self._bucket_key(bucket, "category"), self._bucket_key(bucket, "brand"))
            expected_prices = sorted(row["price"] for row in expected_groups[group_key])
            assert [int(hit["price"]) for hit in bucket["hits"]] == expected_prices

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_nested_groups_with_top_hits(self):
        """
        target: verify nested grouping and topHits through the REST request and response
        method: retain two rows per composite key through child topHits, but request only one parent topHit
        expected: parent metrics cover all bucket rows while parent topHits is truncated to its requested size
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "limit": 3,
                "filter": "id <= 3",
                "outputFields": ["category", "brand", "price"],
                "searchAggregation": {
                    "fields": ["category"],
                    "size": 3,
                    "metrics": {"item_count": {"op": "count", "fieldName": "*"}},
                    "order": [{"key": "_key", "direction": "asc"}],
                    "topHits": {"size": 1, "sort": [{"fieldName": "price", "direction": "asc"}]},
                    "subAggregation": {
                        "fields": ["brand"],
                        "size": 2,
                        "metrics": {"total_price": {"op": "sum", "fieldName": "price"}},
                        "order": [{"key": "_key", "direction": "asc"}],
                        "topHits": {"size": 2, "sort": [{"fieldName": "price", "direction": "asc"}]},
                    },
                },
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["aggTopks"] == [2]

        buckets = rsp["data"][0]["buckets"]
        assert [self._bucket_key(bucket, "category") for bucket in buckets] == ["A", "B"]
        candidate_rows = [row for row in ROWS if row["id"] <= 3]
        for bucket in buckets:
            category = self._bucket_key(bucket, "category")
            category_rows = [row for row in candidate_rows if row["category"] == category]
            assert int(bucket["count"]) == len(category_rows)
            assert int(bucket["metrics"]["item_count"]) == len(category_rows)
            expected_parent_hits = sorted(category_rows, key=lambda row: row["price"])[:1]
            assert [int(hit["id"]) for hit in bucket["hits"]] == [row["id"] for row in expected_parent_hits]
            assert [int(hit["price"]) for hit in bucket["hits"]] == [row["price"] for row in expected_parent_hits]
            assert all(hit["category"] == category for hit in bucket["hits"])

            sub_groups = bucket["subGroups"]
            expected_brands = sorted({row["brand"] for row in category_rows})
            assert [self._bucket_key(sub_group, "brand") for sub_group in sub_groups] == expected_brands
            for sub_group in sub_groups:
                brand = self._bucket_key(sub_group, "brand")
                expected_rows = [row for row in category_rows if row["brand"] == brand]
                assert int(sub_group["count"]) == len(expected_rows)
                assert int(sub_group["metrics"]["total_price"]) == sum(row["price"] for row in expected_rows)
                expected_child_hits = sorted(expected_rows, key=lambda row: row["price"])
                assert [int(hit["id"]) for hit in sub_group["hits"]] == [row["id"] for row in expected_child_hits]
                assert [int(hit["price"]) for hit in sub_group["hits"]] == [row["price"] for row in expected_child_hits]
                assert all(hit["category"] == category and hit["brand"] == brand for hit in sub_group["hits"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_with_top_level_order_by_rejected(self):
        """
        target: verify searchAggregation cannot be combined with top-level orderByFields
        method: send both new REST parameters in one search request
        expected: REST rejects the unsupported combination with code 1100
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "limit": 3,
                "orderByFields": ["price:asc"],
                "searchAggregation": {"fields": ["category"], "size": 3},
            }
        )
        assert rsp["code"] == 1100, rsp
        assert "orderByFields and searchAggregation cannot be used simultaneously" in rsp["message"], rsp

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_with_legacy_order_by_compatible(self):
        """
        target: verify searchAggregation remains compatible with legacy searchParams.order_by_fields
        method: use the legacy order_by_fields location with an exact two-ID candidate filter
        expected: request succeeds and returns the exact aggregation buckets for the filtered candidates
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "limit": 2,
                "filter": "id in [1, 3]",
                "searchParams": {"order_by_fields": "price:asc"},
                "searchAggregation": {
                    "fields": ["category"],
                    "size": 3,
                    "order": [{"key": "_key", "direction": "asc"}],
                },
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["aggTopks"] == [2]
        buckets = rsp["data"][0]["buckets"]
        assert [bucket["key"][0]["value"] for bucket in buckets] == ["A", "B"]
        candidate_rows = [row for row in ROWS if row["id"] in {1, 3}]
        expected_counts = {
            category: sum(row["category"] == category for row in candidate_rows) for category in {"A", "B"}
        }
        assert {bucket["key"][0]["value"]: int(bucket["count"]) for bucket in buckets} == expected_counts
