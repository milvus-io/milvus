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
    {"id": 6, "category": "A", "brand": "Z", "price": -100, "vector": [-1.0, 0.0]},
    {"id": 7, "category": "Z", "brand": "Q", "price": 50, "vector": [0.5, 0.0]},
    {"id": 8, "category": "A", "brand": "X", "price": 15, "vector": [0.4, 0.0]},
    {"id": 9, "category": "Z", "brand": "R", "price": 60, "vector": [1.5, 0.0]},
    {"id": 10, "category": "A", "brand": "W", "price": -1000, "vector": [2.0, 0.0]},
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

    @staticmethod
    def _ip_score(row, query_vector):
        return sum(left * right for left, right in zip(query_vector, row["vector"]))

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_single_field_with_all_metrics(self):
        """
        target: verify filtering precedes aggregation and the ordinary search limit does not cap aggregation results
        method: exclude the highest-IP row by filter, request limit 1, and retain three ANN rows per category
        expected: exact buckets, metrics, topHits, fields, and scores come only from filtered candidates
        """
        query_vector = [1.0, 0.0]
        retained_size = 3
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [query_vector],
                "annsField": "vector",
                "limit": 1,
                "filter": "id <= 6",
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
                        "score_sum": {"op": "sum", "fieldName": "_score"},
                        "score_avg": {"op": "avg", "fieldName": "_score"},
                        "score_min": {"op": "min", "fieldName": "_score"},
                        "score_max": {"op": "max", "fieldName": "_score"},
                    },
                    "order": [{"key": "_key", "direction": "asc"}],
                    "topHits": {"size": retained_size, "sort": [{"fieldName": "price", "direction": "asc"}]},
                },
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["aggTopks"] == [3]

        buckets = rsp["data"][0]["buckets"]
        assert [self._bucket_key(bucket, "category") for bucket in buckets] == ["A", "B", "C"]
        eligible_rows = [row for row in ROWS if row["id"] <= 6]
        ranked_rows = sorted(
            eligible_rows,
            key=lambda row: (-self._ip_score(row, query_vector), row["id"]),
        )
        expected_by_category = {}
        for row in ranked_rows:
            retained_rows = expected_by_category.setdefault(row["category"], [])
            if len(retained_rows) < retained_size:
                retained_rows.append(row)

        assert [row["id"] for row in expected_by_category["A"]] == [1, 2, 5]
        for bucket in buckets:
            category = self._bucket_key(bucket, "category")
            expected_rows = expected_by_category[category]
            expected_prices = [row["price"] for row in expected_rows]
            expected_scores = [self._ip_score(row, query_vector) for row in expected_rows]
            metrics = bucket["metrics"]
            assert int(bucket["count"]) == len(expected_prices)
            assert int(metrics["item_count"]) == len(expected_prices)
            assert int(metrics["total_price"]) == sum(expected_prices)
            assert metrics["average_price"] == pytest.approx(sum(expected_prices) / len(expected_prices))
            assert int(metrics["minimum_price"]) == min(expected_prices)
            assert int(metrics["maximum_price"]) == max(expected_prices)
            assert metrics["score_sum"] == pytest.approx(sum(expected_scores))
            assert metrics["score_avg"] == pytest.approx(sum(expected_scores) / len(expected_scores))
            assert metrics["score_min"] == pytest.approx(min(expected_scores))
            assert metrics["score_max"] == pytest.approx(max(expected_scores))
            expected_hits = sorted(expected_rows, key=lambda row: row["price"])
            assert [int(hit["id"]) for hit in bucket["hits"]] == [row["id"] for row in expected_hits]
            assert [int(hit["price"]) for hit in bucket["hits"]] == [row["price"] for row in expected_hits]
            assert [hit["distance"] for hit in bucket["hits"]] == pytest.approx(
                [self._ip_score(row, query_vector) for row in expected_hits]
            )
            assert all(set(hit) == {"id", "distance", "category", "price"} for hit in bucket["hits"])
            assert all(hit["category"] == category for hit in bucket["hits"])

        all_hit_ids = {int(hit["id"]) for bucket in buckets for hit in bucket["hits"]}
        assert all_hit_ids == {1, 2, 3, 4, 5}
        assert {6, 10}.isdisjoint(all_hit_ids)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_size_search_size_and_key_order(self):
        """
        target: verify REST searchAggregation applies searchSize, final size, and bucket-key ordering independently
        method: collect four distinct candidate buckets, keep three, and order their keys descending
        expected: the exact retained buckets are Z, C, and B with their corresponding hits and scores
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "limit": 1,
                "filter": "id in [1, 3, 4, 7]",
                "outputFields": ["category", "price"],
                "searchAggregation": {
                    "fields": ["category"],
                    "size": 3,
                    "searchSize": 4,
                    "order": [{"key": "_key", "direction": "desc"}],
                    "topHits": {"size": 1},
                },
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["aggTopks"] == [3]

        buckets = rsp["data"][0]["buckets"]
        assert [self._bucket_key(bucket, "category") for bucket in buckets] == ["Z", "C", "B"]
        expected = [(7, 0.5), (4, 0.7), (3, 0.8)]
        for bucket, (expected_id, expected_score) in zip(buckets, expected):
            assert int(bucket["count"]) == 1
            assert len(bucket["hits"]) == 1
            hit = bucket["hits"][0]
            assert int(hit["id"]) == expected_id
            assert hit["distance"] == pytest.approx(expected_score)
            assert hit["category"] == self._bucket_key(bucket, "category")
            assert set(hit) == {"id", "distance", "category", "price"}

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_composite_fields_ordered_by_metric(self):
        """
        target: verify composite grouping fields and metric-based bucket ordering
        method: group four exact candidates by category and brand, then order buckets by total price descending
        expected: the repeated A/X key merges two rows and every bucket exposes exact metrics, hits, fields, and scores
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "limit": 4,
                "filter": "id in [1, 2, 3, 5]",
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

        assert rsp["aggTopks"] == [3]

        buckets = rsp["data"][0]["buckets"]
        expected = [
            ("A", "X", 35, [5, 1], [5, 30], [0.6, 1.0]),
            ("B", "X", 20, [3], [20], [0.8]),
            ("A", "Y", 10, [2], [10], [0.9]),
        ]
        assert all([key["fieldName"] for key in bucket["key"]] == ["category", "brand"] for bucket in buckets)
        assert len(buckets) == len(expected)
        for bucket, (category, brand, total_price, hit_ids, hit_prices, hit_scores) in zip(buckets, expected):
            assert self._bucket_key(bucket, "category") == category
            assert self._bucket_key(bucket, "brand") == brand
            assert int(bucket["count"]) == len(hit_ids)
            assert int(bucket["metrics"]["total_price"]) == total_price
            assert [int(hit["id"]) for hit in bucket["hits"]] == hit_ids
            assert [int(hit["price"]) for hit in bucket["hits"]] == hit_prices
            assert [hit["distance"] for hit in bucket["hits"]] == pytest.approx(hit_scores)
            assert all(set(hit) == {"id", "distance", "category", "brand", "price"} for hit in bucket["hits"])
            assert all(hit["category"] == category and hit["brand"] == brand for hit in bucket["hits"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_nested_groups_with_top_hits(self):
        """
        target: verify nested bucket final size, ordering, metrics, and topHits are applied per level
        method: aggregate three categories and three A-brand candidates, then retain only two A-brand subgroups
        expected: parent buckets are C/B/A; A children are Z/Y and the X subgroup is truncated
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "limit": 6,
                "filter": "id <= 6",
                "outputFields": ["category", "brand", "price"],
                "searchAggregation": {
                    "fields": ["category"],
                    "size": 3,
                    "searchSize": 3,
                    "metrics": {"item_count": {"op": "count", "fieldName": "*"}},
                    "order": [{"key": "_key", "direction": "desc"}],
                    "topHits": {"size": 1, "sort": [{"fieldName": "price", "direction": "asc"}]},
                    "subAggregation": {
                        "fields": ["brand"],
                        "size": 2,
                        "searchSize": 3,
                        "metrics": {"total_price": {"op": "sum", "fieldName": "price"}},
                        "order": [{"key": "_key", "direction": "desc"}],
                        "topHits": {"size": 2, "sort": [{"fieldName": "price", "direction": "asc"}]},
                    },
                },
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["aggTopks"] == [3]

        buckets = rsp["data"][0]["buckets"]
        assert [self._bucket_key(bucket, "category") for bucket in buckets] == ["C", "B", "A"]
        expected_parents = {
            "C": (1, 4, 40, 0.7, [("Y", 1, 40, [4], [40], [0.7])]),
            "B": (1, 3, 20, 0.8, [("X", 1, 20, [3], [20], [0.8])]),
            "A": (
                4,
                6,
                -100,
                -1.0,
                [
                    ("Z", 1, -100, [6], [-100], [-1.0]),
                    ("Y", 1, 10, [2], [10], [0.9]),
                ],
            ),
        }
        for bucket in buckets:
            category = self._bucket_key(bucket, "category")
            expected_count, parent_id, parent_price, parent_score, expected_children = expected_parents[category]
            assert int(bucket["count"]) == expected_count
            assert int(bucket["metrics"]["item_count"]) == expected_count
            assert [int(hit["id"]) for hit in bucket["hits"]] == [parent_id]
            assert [int(hit["price"]) for hit in bucket["hits"]] == [parent_price]
            assert [hit["distance"] for hit in bucket["hits"]] == pytest.approx([parent_score])
            assert all(hit["category"] == category for hit in bucket["hits"])

            sub_groups = bucket["subGroups"]
            assert [self._bucket_key(sub_group, "brand") for sub_group in sub_groups] == [
                child[0] for child in expected_children
            ]
            for sub_group, (brand, count, total_price, hit_ids, hit_prices, hit_scores) in zip(
                sub_groups, expected_children
            ):
                assert int(sub_group["count"]) == count
                assert int(sub_group["metrics"]["total_price"]) == total_price
                assert [int(hit["id"]) for hit in sub_group["hits"]] == hit_ids
                assert [int(hit["price"]) for hit in sub_group["hits"]] == hit_prices
                assert [hit["distance"] for hit in sub_group["hits"]] == pytest.approx(hit_scores)
                assert all(hit["category"] == category and hit["brand"] == brand for hit in sub_group["hits"])

        a_bucket = next(bucket for bucket in buckets if self._bucket_key(bucket, "category") == "A")
        assert "X" not in {self._bucket_key(sub_group, "brand") for sub_group in a_bucket["subGroups"]}

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_nested_child_search_size(self):
        """
        target: verify child searchSize expands the nested ANN candidate window before child size and ordering
        method: retain three A-brand candidates by child searchSize, then return two brand keys in descending order
        expected: Z and Y are returned; defaulting searchSize to size would instead return Y and X
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "limit": 1,
                "filter": "id in [1, 2, 6]",
                "outputFields": ["category", "brand", "price"],
                "searchAggregation": {
                    "fields": ["category"],
                    "size": 1,
                    "searchSize": 1,
                    "subAggregation": {
                        "fields": ["brand"],
                        "size": 2,
                        "searchSize": 3,
                        "metrics": {"item_count": {"op": "count", "fieldName": "*"}},
                        "order": [{"key": "_key", "direction": "desc"}],
                        "topHits": {"size": 1},
                    },
                },
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["aggTopks"] == [1]

        bucket = rsp["data"][0]["buckets"][0]
        assert self._bucket_key(bucket, "category") == "A"
        assert int(bucket["count"]) == 3
        sub_groups = bucket["subGroups"]
        assert [self._bucket_key(sub_group, "brand") for sub_group in sub_groups] == ["Z", "Y"]
        expected = [("Z", 6, -1.0), ("Y", 2, 0.9)]
        for sub_group, (brand, hit_id, score) in zip(sub_groups, expected):
            assert int(sub_group["count"]) == 1
            assert int(sub_group["metrics"]["item_count"]) == 1
            assert len(sub_group["hits"]) == 1
            hit = sub_group["hits"][0]
            assert int(hit["id"]) == hit_id
            assert hit["distance"] == pytest.approx(score)
            assert hit["brand"] == brand

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_child_top_hits_size(self):
        """
        target: verify child topHits.size truncates hits without changing the child bucket metrics
        method: aggregate three rows in the same A/X composite bucket and request only two child top hits
        expected: the child count and sum cover all rows while its sorted hits contain only IDs 5 and 8
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "limit": 3,
                "filter": "id in [1, 5, 8]",
                "outputFields": ["category", "brand", "price"],
                "searchAggregation": {
                    "fields": ["category"],
                    "size": 1,
                    "searchSize": 1,
                    "topHits": {"size": 3, "sort": [{"fieldName": "price", "direction": "asc"}]},
                    "subAggregation": {
                        "fields": ["brand"],
                        "size": 1,
                        "searchSize": 1,
                        "metrics": {"total_price": {"op": "sum", "fieldName": "price"}},
                        "topHits": {"size": 2, "sort": [{"fieldName": "price", "direction": "asc"}]},
                    },
                },
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["aggTopks"] == [1]

        bucket = rsp["data"][0]["buckets"][0]
        assert self._bucket_key(bucket, "category") == "A"
        assert int(bucket["count"]) == 3
        assert [int(hit["id"]) for hit in bucket["hits"]] == [5, 8, 1]
        assert [int(hit["price"]) for hit in bucket["hits"]] == [5, 15, 30]

        assert len(bucket["subGroups"]) == 1
        sub_group = bucket["subGroups"][0]
        assert self._bucket_key(sub_group, "brand") == "X"
        assert int(sub_group["count"]) == 3
        assert int(sub_group["metrics"]["total_price"]) == 50
        assert [int(hit["id"]) for hit in sub_group["hits"]] == [5, 8]
        assert [int(hit["price"]) for hit in sub_group["hits"]] == [5, 15]
        assert [hit["distance"] for hit in sub_group["hits"]] == pytest.approx([0.6, 0.4])
        assert 1 not in {int(hit["id"]) for hit in sub_group["hits"]}

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_multiple_query_vectors(self):
        """
        target: verify REST searchAggregation returns independent aggregation results for every query vector
        method: search the same filtered rows with opposite IP query vectors and retain one category and hit per query
        expected: the first query returns category A/ID 1 and the second returns category C/ID 4
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0], [-1.0, 0.0]],
                "annsField": "vector",
                "limit": 1,
                "filter": "id <= 4",
                "outputFields": ["category", "price"],
                "searchAggregation": {
                    "fields": ["category"],
                    "size": 1,
                    "searchSize": 1,
                    "topHits": {"size": 1},
                },
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["aggTopks"] == [1, 1]
        assert len(rsp["data"]) == 2

        expected = [("A", 1, 1.0), ("C", 4, -0.7)]
        for result, (category, hit_id, score) in zip(rsp["data"], expected):
            assert len(result["buckets"]) == 1
            bucket = result["buckets"][0]
            assert self._bucket_key(bucket, "category") == category
            assert int(bucket["count"]) == 1
            assert len(bucket["hits"]) == 1
            hit = bucket["hits"][0]
            assert int(hit["id"]) == hit_id
            assert hit["distance"] == pytest.approx(score)
            assert hit["category"] == category

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
