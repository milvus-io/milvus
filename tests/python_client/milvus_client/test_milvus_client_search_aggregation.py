import math

import pytest
from pymilvus import DataType, SearchAggregation, TopHits

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel


@pytest.mark.xdist_group("TestSearchAggregation")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchAggregation(TestMilvusClientV2Base):
    """Shared collection for MilvusClient search_aggregation tests.
    Schema: pk, float_vector, brand, color, category, price, rating, in_stock.
    Data: 2400 deterministic rows with predictable vector distance and scalar groups.
    Index: FLAT/L2 on float_vector.
    """

    shared_alias = "TestSearchAggregation"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchAggregation" + cf.gen_unique_str("_")
        self.vector_field = ct.default_float_vec_field_name
        self.primary_field = ct.default_primary_field_name
        self.brand_field = "brand"
        self.color_field = "color"
        self.category_field = "category"
        self.price_field = "price"
        self.rating_field = "rating"
        self.in_stock_field = "in_stock"
        self.dim = 4
        self.metric_type = "L2"
        self.nb = 2400

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(field_name=self.primary_field, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=self.vector_field, datatype=DataType.FLOAT_VECTOR, dim=self.dim)
        schema.add_field(field_name=self.brand_field, datatype=DataType.VARCHAR, max_length=64)
        schema.add_field(field_name=self.color_field, datatype=DataType.VARCHAR, max_length=64)
        schema.add_field(field_name=self.category_field, datatype=DataType.VARCHAR, max_length=64)
        schema.add_field(field_name=self.price_field, datatype=DataType.INT64)
        schema.add_field(field_name=self.rating_field, datatype=DataType.DOUBLE)
        schema.add_field(field_name=self.in_stock_field, datatype=DataType.BOOL)

        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)
        self.insert(client, self.collection_name, data=self._rows())
        self.flush(client, self.collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.vector_field, index_type="FLAT", metric_type=self.metric_type)
        self.create_index(client, self.collection_name, index_params=index_params)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)

        request.addfinalizer(teardown)

    def _rows(self):
        brands = ["brand_a", "brand_b", "brand_c", "brand_d"]
        colors = ["red", "blue", "green", "black"]
        categories = ["cat_a", "cat_b", "cat_c"]
        rows = []
        for i in range(self.nb):
            rows.append(
                {
                    self.primary_field: i,
                    self.vector_field: [i / 1000.0, 0.0, 0.0, 0.0],
                    self.brand_field: brands[i % len(brands)],
                    self.color_field: colors[(i // len(brands)) % len(colors)],
                    self.category_field: categories[i % len(categories)],
                    self.price_field: i + 10,
                    self.rating_field: (i % 50) / 10.0,
                    self.in_stock_field: i % 5 != 0,
                }
            )
        return rows

    def _query_vectors(self):
        return [[0.0, 0.0, 0.0, 0.0], [0.12, 0.0, 0.0, 0.0]]

    def _search_params(self):
        return {"metric_type": self.metric_type}

    @staticmethod
    def _key_value(bucket, field_name):
        for entry in bucket.key:
            if entry["field_name"] == field_name:
                return entry["value"]
        raise AssertionError(f"field {field_name} not found in bucket key {bucket.key}")

    @staticmethod
    def _assert_scores_ascending(hits):
        scores = [hit.score for hit in hits]
        assert scores == sorted(scores)

    def _assert_hit_prices_ascending(self, hits):
        prices = [hit.fields[self.price_field] for hit in hits]
        assert prices == sorted(prices)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_single_field_top_hits(self):
        """
        target: verify basic search_aggregation with one group field and top_hits.
        method: 1. search with brand buckets, doc_count/sum(price), and top_hits sorted by _score asc
                2. verify bucket count, key field names, metrics, hit fields, and L2 score order
                3. verify q0 exact pks and metric values against deterministic ground truth
        expected: each query returns 4 brand buckets, each bucket has 2 hits from the same brand with exact metrics.
        """
        nq = 2
        top_hit_size = 2
        client = self._client(alias=self.shared_alias)
        res, _ = self.search(
            client,
            self.collection_name,
            data=self._query_vectors(),
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[self.brand_field, self.price_field],
            search_aggregation=SearchAggregation(
                fields=[self.brand_field],
                size=4,
                metrics={"doc_count": {"count": "*"}, "total_price": {"sum": self.price_field}},
                top_hits=TopHits(size=top_hit_size, sort=[{"_score": "asc"}]),
            ),
        )

        assert len(res.agg_buckets) == nq
        for buckets in res.agg_buckets:
            assert len(buckets) == 4
            for bucket in buckets:
                brand = self._key_value(bucket, self.brand_field)
                assert len(bucket.key) == 1
                assert bucket.key[0]["field_name"] == self.brand_field
                assert bucket.key[0]["value"] == brand
                assert bucket.count == top_hit_size
                assert bucket.metrics["doc_count"] == top_hit_size
                assert len(bucket.hits) == top_hit_size
                self._assert_scores_ascending(bucket.hits)
                assert bucket.metrics["total_price"] == sum(hit.fields[self.price_field] for hit in bucket.hits)
                for hit in bucket.hits:
                    assert hit.fields[self.brand_field] == brand

        expected_q0 = {
            "brand_a": ([0, 4], 24),
            "brand_b": ([1, 5], 26),
            "brand_c": ([2, 6], 28),
            "brand_d": ([3, 7], 30),
        }
        actual_q0 = {
            self._key_value(bucket, self.brand_field): (
                [hit.pk for hit in bucket.hits],
                bucket.metrics["total_price"],
            )
            for bucket in res.agg_buckets[0]
        }
        assert actual_q0 == expected_q0

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_composite_key_metrics_order(self):
        """
        target: verify search_aggregation with composite group fields, metrics, bucket order, and sorted top_hits.
        method: 1. search with (brand, color) buckets, avg_price/doc_count metrics, order by avg_price desc
                2. verify composite key field names, metric arithmetic, bucket ordering, and top_hits price order
                3. verify q0 exact keys, pks, and avg_price against deterministic ground truth
        expected: each query returns 4 composite buckets ordered by avg_price desc with exact per-bucket metrics.
        """
        nq = 2
        top_hit_size = 2
        client = self._client(alias=self.shared_alias)
        res, _ = self.search(
            client,
            self.collection_name,
            data=self._query_vectors(),
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[self.brand_field, self.color_field, self.price_field],
            search_aggregation=SearchAggregation(
                fields=[self.brand_field, self.color_field],
                size=4,
                metrics={"avg_price": {"avg": self.price_field}, "doc_count": {"count": "*"}},
                order=[{"avg_price": "desc"}],
                top_hits=TopHits(size=top_hit_size, sort=[{self.price_field: "asc"}]),
            ),
        )

        assert len(res.agg_buckets) == nq
        for buckets in res.agg_buckets:
            assert len(buckets) == 4
            avg_prices = [bucket.metrics["avg_price"] for bucket in buckets]
            assert avg_prices == sorted(avg_prices, reverse=True)
            for bucket in buckets:
                assert [entry["field_name"] for entry in bucket.key] == [self.brand_field, self.color_field]
                assert bucket.count == top_hit_size
                assert bucket.metrics["doc_count"] == top_hit_size
                assert len(bucket.hits) == top_hit_size
                self._assert_hit_prices_ascending(bucket.hits)
                expected_avg = sum(hit.fields[self.price_field] for hit in bucket.hits) / top_hit_size
                assert math.isclose(bucket.metrics["avg_price"], expected_avg, rel_tol=0, abs_tol=ct.epsilon)
                for hit in bucket.hits:
                    assert hit.fields[self.brand_field] == self._key_value(bucket, self.brand_field)
                    assert hit.fields[self.color_field] == self._key_value(bucket, self.color_field)

        expected_q0 = [
            (("brand_d", "red"), [3, 19], 21.0),
            (("brand_c", "red"), [2, 18], 20.0),
            (("brand_b", "red"), [1, 17], 19.0),
            (("brand_a", "red"), [0, 16], 18.0),
        ]
        actual_q0 = [
            (
                (self._key_value(bucket, self.brand_field), self._key_value(bucket, self.color_field)),
                [hit.pk for hit in bucket.hits],
                bucket.metrics["avg_price"],
            )
            for bucket in res.agg_buckets[0]
        ]
        assert actual_q0 == expected_q0

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_two_level_nested_with_filter(self):
        """
        target: verify two-level search_aggregation with filter, parent buckets, child buckets, metrics, and top_hits.
        method: 1. search with filter in_stock == true and category -> brand nested aggregation
                2. verify field names, parent/child key consistency, top_hits sort rules, and filter effectiveness
                3. verify child avg_rating metrics from returned hits when child count equals hit count
        expected: all returned hits satisfy the filter and match their parent/child bucket keys with consistent metrics.
        """
        nq = 2
        client = self._client(alias=self.shared_alias)
        res, _ = self.search(
            client,
            self.collection_name,
            data=self._query_vectors(),
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            filter=f"{self.in_stock_field} == true",
            output_fields=[
                self.category_field,
                self.brand_field,
                self.price_field,
                self.rating_field,
                self.in_stock_field,
            ],
            search_aggregation=SearchAggregation(
                fields=[self.category_field],
                size=2,
                metrics={"total_price": {"sum": self.price_field}, "item_count": {"count": "*"}},
                order=[{"total_price": "desc"}],
                top_hits=TopHits(size=2, sort=[{"_score": "asc"}]),
                sub_aggregation=SearchAggregation(
                    fields=[self.brand_field],
                    size=2,
                    metrics={"avg_rating": {"avg": self.rating_field}},
                    order=[{"avg_rating": "desc"}],
                    top_hits=TopHits(size=2, sort=[{self.price_field: "asc"}]),
                ),
            ),
        )

        assert len(res.agg_buckets) == nq
        for buckets in res.agg_buckets:
            assert len(buckets) == 2
            total_prices = [bucket.metrics["total_price"] for bucket in buckets]
            assert total_prices == sorted(total_prices, reverse=True)
            for bucket in buckets:
                category = self._key_value(bucket, self.category_field)
                assert [entry["field_name"] for entry in bucket.key] == [self.category_field]
                assert bucket.metrics["item_count"] == bucket.count
                assert len(bucket.hits) == 2
                assert 1 <= len(bucket.sub_groups) <= 2
                self._assert_scores_ascending(bucket.hits)
                for hit in bucket.hits:
                    assert hit.fields[self.category_field] == category
                    assert hit.fields[self.in_stock_field] is True

                for sub_bucket in bucket.sub_groups:
                    brand = self._key_value(sub_bucket, self.brand_field)
                    assert [entry["field_name"] for entry in sub_bucket.key] == [self.brand_field]
                    self._assert_hit_prices_ascending(sub_bucket.hits)
                    for hit in sub_bucket.hits:
                        assert hit.fields[self.category_field] == category
                        assert hit.fields[self.brand_field] == brand
                        assert hit.fields[self.in_stock_field] is True
                    assert sub_bucket.count == len(sub_bucket.hits)
                    expected_avg = sum(hit.fields[self.rating_field] for hit in sub_bucket.hits) / sub_bucket.count
                    assert math.isclose(
                        sub_bucket.metrics["avg_rating"],
                        expected_avg,
                        rel_tol=0,
                        abs_tol=ct.epsilon,
                    )
