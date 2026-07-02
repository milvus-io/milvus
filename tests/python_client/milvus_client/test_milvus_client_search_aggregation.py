import math

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import AnnSearchRequest, DataType, Function, FunctionType, RRFRanker, SearchAggregation, TopHits
from pymilvus.client.embedding_list import EmbeddingList
from pymilvus.exceptions import ParamError


@pytest.mark.xdist_group("TestSearchAggregation")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchAggregation(TestMilvusClientV2Base):
    """Shared collection for MilvusClient search_aggregation tests.
    Schema: pk, float_vector, brand, color, category, price, rating, in_stock.
    Data: default rows with generated vectors, scalar groups, and nullable vector/scalar fields.
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
        self.int8_field = "int8_key"
        self.int16_field = "int16_key"
        self.int32_field = "int32_key"
        self.price_field = "price"
        self.rating_field = "rating"
        self.in_stock_field = "in_stock"
        self.dim = ct.default_dim
        self.metric_type = "L2"
        self.nb = ct.default_nb
        self.null_field_mod = 20
        self.null_vector_remainder = 9
        self.null_scalar_remainders = {
            self.brand_field: 3,
            self.color_field: 4,
            self.category_field: 6,
            self.int8_field: 12,
            self.int16_field: 13,
            self.int32_field: 14,
            self.price_field: 7,
            self.rating_field: 8,
            self.in_stock_field: 11,
        }
        self.vectors = cf.gen_vectors(self.nb, dim=self.dim, vector_data_type=DataType.FLOAT_VECTOR)

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(field_name=self.primary_field, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=self.vector_field, datatype=DataType.FLOAT_VECTOR, dim=self.dim, nullable=True)
        schema.add_field(field_name=self.brand_field, datatype=DataType.VARCHAR, max_length=64, nullable=True)
        schema.add_field(field_name=self.color_field, datatype=DataType.VARCHAR, max_length=64, nullable=True)
        schema.add_field(field_name=self.category_field, datatype=DataType.VARCHAR, max_length=64, nullable=True)
        schema.add_field(field_name=self.int8_field, datatype=DataType.INT8, nullable=True)
        schema.add_field(field_name=self.int16_field, datatype=DataType.INT16, nullable=True)
        schema.add_field(field_name=self.int32_field, datatype=DataType.INT32, nullable=True)
        schema.add_field(field_name=self.price_field, datatype=DataType.INT64, nullable=True)
        schema.add_field(field_name=self.rating_field, datatype=DataType.DOUBLE, nullable=True)
        schema.add_field(field_name=self.in_stock_field, datatype=DataType.BOOL, nullable=True)

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
                    self.vector_field: None if self._is_null_vector_row(i) else self.vectors[i],
                    self.brand_field: (
                        None if self._is_null_scalar_row(i, self.brand_field) else brands[i % len(brands)]
                    ),
                    self.color_field: (
                        None
                        if self._is_null_scalar_row(i, self.color_field)
                        else colors[(i // len(brands)) % len(colors)]
                    ),
                    self.category_field: (
                        None if self._is_null_scalar_row(i, self.category_field) else categories[i % len(categories)]
                    ),
                    self.int8_field: None if self._is_null_scalar_row(i, self.int8_field) else i % 11 - 5,
                    self.int16_field: None if self._is_null_scalar_row(i, self.int16_field) else i % 101 - 50,
                    self.int32_field: None if self._is_null_scalar_row(i, self.int32_field) else i % 257,
                    self.price_field: None if self._is_null_scalar_row(i, self.price_field) else i + 10,
                    self.rating_field: None if self._is_null_scalar_row(i, self.rating_field) else (i % 50) / 10.0,
                    self.in_stock_field: None if self._is_null_scalar_row(i, self.in_stock_field) else i % 5 != 0,
                }
            )
        return rows

    def _query_vectors(self):
        return [self.vectors[0], self.vectors[120]]

    def _search_params(self):
        return {"metric_type": self.metric_type}

    def _is_null_vector_row(self, pk):
        return pk % self.null_field_mod == self.null_vector_remainder

    def _is_null_scalar_row(self, pk, field_name):
        return pk % self.null_field_mod == self.null_scalar_remainders[field_name]

    def _non_null_expr(self, *field_names):
        exprs = []
        for field_name in field_names:
            if field_name in [self.brand_field, self.color_field, self.category_field]:
                exprs.append(f'{field_name} != ""')
            elif field_name in [
                self.int8_field,
                self.int16_field,
                self.int32_field,
                self.price_field,
                self.rating_field,
            ]:
                exprs.append(f"{field_name} >= 0")
            elif field_name == self.in_stock_field:
                exprs.append(f"{field_name} in [true, false]")
            else:
                raise AssertionError(f"unsupported nullable field {field_name}")
        return " and ".join(exprs)

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
    def test_search_aggregation_minimal_parameters(self):
        """
        target: verify minimal SDK-supported search_aggregation parameters.
        method: search with group-by fields and size, omitting metrics/order/top_hits.
        expected: search returns one bucket per query; SDK requires size in the constructor.
        """
        nq = 2
        client = self._client(alias=self.shared_alias)
        with pytest.raises(TypeError, match="missing 1 required positional argument: 'size'"):
            SearchAggregation(fields=[self.brand_field])

        res, _ = self.search(
            client,
            self.collection_name,
            data=self._query_vectors(),
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[self.brand_field],
            search_aggregation=SearchAggregation(fields=[self.brand_field], size=1),
        )

        assert len(res.agg_buckets) == nq
        for buckets in res.agg_buckets:
            assert len(buckets) == 1
            assert [entry["field_name"] for entry in buckets[0].key] == [self.brand_field]
            assert buckets[0].count >= 1

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_range_search(self):
        """
        target: verify range search supports search_aggregation on a regular vector field.
        method: use L2 radius/range_filter with brand buckets and top_hits.
        expected: aggregation succeeds and every returned hit distance is inside the range.
        """
        client = self._client(alias=self.shared_alias)
        radius = 2.1
        range_filter = 0.0
        res, _ = self.search(
            client,
            self.collection_name,
            data=[self._query_vectors()[0]],
            anns_field=self.vector_field,
            search_params={"metric_type": self.metric_type, "params": {"radius": radius, "range_filter": range_filter}},
            limit=ct.default_limit,
            output_fields=[self.brand_field],
            search_aggregation=SearchAggregation(
                fields=[self.brand_field],
                size=4,
                metrics={"doc_count": {"count": "*"}},
                top_hits=TopHits(size=2, sort=[{"_score": "asc"}]),
            ),
        )

        assert len(res.agg_buckets) == 1
        assert len(res.agg_buckets[0]) == 4
        for bucket in res.agg_buckets[0]:
            brand = self._key_value(bucket, self.brand_field)
            assert bucket.metrics["doc_count"] == bucket.count
            for hit in bucket.hits:
                assert range_filter <= hit.score < radius
                assert hit.fields.get(self.brand_field) == brand

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_max_four_levels(self):
        """
        target: verify nesting depth limit allows up to four aggregation levels.
        method: search with category -> brand -> color -> price sub-aggregations.
        expected: four-level aggregation succeeds and returns nested buckets.
        """
        client = self._client(alias=self.shared_alias)
        res, _ = self.search(
            client,
            self.collection_name,
            data=[self._query_vectors()[0]],
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[self.category_field, self.brand_field, self.color_field, self.price_field],
            search_aggregation=SearchAggregation(
                fields=[self.category_field],
                size=1,
                sub_aggregation=SearchAggregation(
                    fields=[self.brand_field],
                    size=1,
                    sub_aggregation=SearchAggregation(
                        fields=[self.color_field],
                        size=1,
                        sub_aggregation=SearchAggregation(
                            fields=[self.price_field],
                            size=1,
                            top_hits=TopHits(size=1, sort=[{"_score": "asc"}]),
                        ),
                    ),
                ),
            ),
        )

        level1 = res.agg_buckets[0]
        assert len(level1) == 1
        level2 = level1[0].sub_groups
        assert len(level2) == 1
        level3 = level2[0].sub_groups
        assert len(level3) == 1
        level4 = level3[0].sub_groups
        assert len(level4) == 1
        assert len(level4[0].hits) == 1

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_reject_more_than_four_levels(self):
        """
        target: verify nesting depth rejects five aggregation levels.
        method: search with category -> brand -> color -> price -> in_stock.
        expected: proxy rejects the request with max-depth validation error.
        """
        client = self._client(alias=self.shared_alias)
        error = {ct.err_code: 999, ct.err_msg: "search_aggregation nesting exceeds max 4 levels"}
        self.search(
            client,
            self.collection_name,
            data=[self._query_vectors()[0]],
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[self.category_field, self.brand_field, self.color_field, self.price_field],
            search_aggregation=SearchAggregation(
                fields=[self.category_field],
                size=1,
                sub_aggregation=SearchAggregation(
                    fields=[self.brand_field],
                    size=1,
                    sub_aggregation=SearchAggregation(
                        fields=[self.color_field],
                        size=1,
                        sub_aggregation=SearchAggregation(
                            fields=[self.price_field],
                            size=1,
                            sub_aggregation=SearchAggregation(fields=[self.in_stock_field], size=1),
                        ),
                    ),
                ),
            ),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_reject_oversized_result_entries(self):
        """
        target: verify size/top_hits are bounded by proxy.maxSearchAggregationResultEntries.
        method: request nq=1, derived topK=50*50 and derived group size=5.
        expected: default proxy limit 10000 rejects 12500 derived result entries.
        """
        client = self._client(alias=self.shared_alias)
        error = {ct.err_code: 999, ct.err_msg: "number of search_aggregation result entries is too large"}
        self.search(
            client,
            self.collection_name,
            data=[self._query_vectors()[0]],
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[self.brand_field, self.color_field],
            search_aggregation=SearchAggregation(
                fields=[self.brand_field],
                size=50,
                sub_aggregation=SearchAggregation(
                    fields=[self.color_field],
                    size=50,
                    top_hits=TopHits(size=5),
                ),
            ),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_max_result_entries_boundary(self):
        """
        target: verify proxy.maxSearchAggregationResultEntries allows the exact boundary.
        method: request derived result entries 50 * 50 * 4 = 10000.
        expected: request succeeds at the default max boundary.
        """
        client = self._client(alias=self.shared_alias)
        res, _ = self.search(
            client,
            self.collection_name,
            data=[self._query_vectors()[0]],
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[self.brand_field, self.color_field],
            search_aggregation=SearchAggregation(
                fields=[self.brand_field],
                size=50,
                sub_aggregation=SearchAggregation(
                    fields=[self.color_field],
                    size=50,
                    top_hits=TopHits(size=4),
                ),
            ),
        )

        assert len(res.agg_buckets) == 1
        assert len(res.agg_buckets[0]) > 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "factory, err_msg",
        [
            (
                lambda self: SearchAggregation(fields=[], size=1),
                "SearchAggregation.fields must be a non-empty list of str",
            ),
            (
                lambda self: SearchAggregation(fields=[self.brand_field], size=0),
                "SearchAggregation.size must be a positive int",
            ),
            (
                lambda self: SearchAggregation(fields=[self.brand_field], size=-1),
                "SearchAggregation.size must be a positive int",
            ),
            (
                lambda self: SearchAggregation(fields=[self.brand_field], size=1, top_hits=TopHits(size=0)),
                "TopHits.size must be a positive int",
            ),
            (
                lambda self: SearchAggregation(fields=[self.brand_field], size=1, top_hits=TopHits(size=-1)),
                "TopHits.size must be a positive int",
            ),
            (
                lambda self: SearchAggregation(
                    fields=[self.brand_field], size=1, metrics={"m": {"median": self.price_field}}
                ),
                "op must be one of",
            ),
            (
                lambda self: SearchAggregation(fields=[self.brand_field], size=1, order=[{"unknown_metric": "desc"}]),
                "must be a metric alias",
            ),
        ],
    )
    def test_search_aggregation_reject_invalid_sdk_parameters(self, factory, err_msg):
        """
        target: verify SDK-side search_aggregation parameter ranges and names.
        method: construct invalid SearchAggregation/TopHits objects.
        expected: constructor rejects invalid fields, size, top_hits.size, metrics, and order.
        """
        with pytest.raises(ParamError, match=err_msg):
            factory(self)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_reject_unknown_group_field(self):
        """
        target: verify unknown aggregation fields are rejected by server validation.
        method: search with a group field that does not exist in schema.
        expected: request fails with field-not-found error.
        """
        client = self._client(alias=self.shared_alias)
        error = {ct.err_code: 999, ct.err_msg: "not found in schema"}
        self.search(
            client,
            self.collection_name,
            data=[self._query_vectors()[0]],
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[self.brand_field],
            search_aggregation=SearchAggregation(fields=["unknown_group_field"], size=1),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "kwargs, err_msg",
        [
            ({"group_by_field": "brand"}, "search_aggregation and group_by_field are mutually exclusive"),
            pytest.param(
                {"group_by_fields": ["brand"]},
                "group_by_fields and search_aggregation cannot be used simultaneously",
                marks=pytest.mark.xfail(
                    reason=(
                        "Milvus issue #50960: Python MilvusClient.search currently ignores group_by_fields "
                        "kwargs, so the proxy conflict validation is not reached"
                    ),
                    strict=True,
                ),
            ),
            (
                {"search_params": {"metric_type": "L2", "offset": 1}},
                "offset is not supported with search_aggregation",
            ),
        ],
    )
    def test_search_aggregation_reject_mutually_exclusive_search_params(self, kwargs, err_msg):
        """
        target: verify search_aggregation rejects incompatible legacy search parameters.
        method: combine aggregation with group_by_field/group_by_fields/offset.
        expected: request fails with deterministic validation error.
        """
        client = self._client(alias=self.shared_alias)
        search_params = kwargs.pop("search_params", self._search_params())
        error = {ct.err_code: 999, ct.err_msg: err_msg}
        self.search(
            client,
            self.collection_name,
            data=[self._query_vectors()[0]],
            anns_field=self.vector_field,
            search_params=search_params,
            limit=ct.default_limit,
            output_fields=[self.brand_field],
            search_aggregation=SearchAggregation(fields=[self.brand_field], size=2),
            check_task=CheckTasks.err_res,
            check_items=error,
            **kwargs,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_reject_float_group_key(self):
        """
        target: verify unsupported group-by key types are rejected.
        method: use DOUBLE field as search_aggregation group key.
        expected: proxy rejects FLOAT/DOUBLE group-by keys.
        """
        client = self._client(alias=self.shared_alias)
        error = {ct.err_code: 999, ct.err_msg: "FLOAT / DOUBLE fields are not supported with search_aggregation"}
        self.search(
            client,
            self.collection_name,
            data=[self._query_vectors()[0]],
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[self.rating_field],
            search_aggregation=SearchAggregation(fields=[self.rating_field], size=2),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_nullable_vector(self):
        """
        target: verify nullable vector fields support search_aggregation.
        method: search around a range containing null vector rows and aggregate by brand.
        expected: aggregation succeeds and top_hits never return rows whose vector is null.
        """
        client = self._client(alias=self.shared_alias)
        res, _ = self.search(
            client,
            self.collection_name,
            data=[self.vectors[2000]],
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[self.brand_field],
            search_aggregation=SearchAggregation(
                fields=[self.brand_field],
                size=4,
                top_hits=TopHits(size=3, sort=[{"_score": "asc"}]),
            ),
        )

        assert len(res.agg_buckets) == 1
        assert len(res.agg_buckets[0]) == 4
        for bucket in res.agg_buckets[0]:
            brand = self._key_value(bucket, self.brand_field)
            for hit in bucket.hits:
                assert not self._is_null_vector_row(hit.pk)
                assert hit.fields.get(self.brand_field) == brand

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_nullable_scalar_fields(self):
        """
        target: verify nullable scalar fields are covered by search_aggregation.
        method: query near a row whose brand is NULL and aggregate by nullable brand.
        expected: aggregation returns a NULL bucket, and top hits in that bucket also carry NULL brand values.
        """
        client = self._client(alias=self.shared_alias)
        null_brand_pk = next(
            i
            for i in range(self.nb)
            if self._is_null_scalar_row(i, self.brand_field) and not self._is_null_vector_row(i)
        )
        res, _ = self.search(
            client,
            self.collection_name,
            data=[self.vectors[null_brand_pk]],
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[self.brand_field],
            search_aggregation=SearchAggregation(
                fields=[self.brand_field],
                size=5,
                metrics={"doc_count": {"count": "*"}},
                top_hits=TopHits(size=2, sort=[{"_score": "asc"}]),
            ),
        )

        assert len(res.agg_buckets) == 1
        null_buckets = [bucket for bucket in res.agg_buckets[0] if self._key_value(bucket, self.brand_field) is None]
        assert len(null_buckets) == 1
        null_bucket = null_buckets[0]
        assert null_bucket.metrics["doc_count"] == null_bucket.count
        assert len(null_bucket.hits) >= 1
        for hit in null_bucket.hits:
            assert hit.fields.get(self.brand_field) is None

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_nullable_int_and_bool_group_keys(self):
        """
        target: verify nullable INT64 and BOOL scalar fields can be aggregation group keys.
        method: search near rows where price/in_stock are NULL and aggregate by those fields.
        expected: each aggregation returns a NULL bucket with matching top_hits.
        """
        client = self._client(alias=self.shared_alias)
        for field_name in [self.price_field, self.in_stock_field]:
            null_pk = next(
                i for i in range(self.nb) if self._is_null_scalar_row(i, field_name) and not self._is_null_vector_row(i)
            )
            res, _ = self.search(
                client,
                self.collection_name,
                data=[self.vectors[null_pk]],
                anns_field=self.vector_field,
                search_params=self._search_params(),
                limit=ct.default_limit,
                output_fields=[field_name],
                search_aggregation=SearchAggregation(
                    fields=[field_name],
                    size=3,
                    metrics={"doc_count": {"count": "*"}},
                    top_hits=TopHits(size=2, sort=[{"_score": "asc"}]),
                ),
            )

            assert len(res.agg_buckets) == 1
            null_buckets = [bucket for bucket in res.agg_buckets[0] if self._key_value(bucket, field_name) is None]
            assert len(null_buckets) == 1
            for hit in null_buckets[0].hits:
                assert hit.fields.get(field_name) is None

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("field_name", ["int8_key", "int16_key", "int32_key"])
    def test_search_aggregation_integer_group_key_types(self, field_name):
        """
        target: verify smaller integer scalar types can be search_aggregation group keys.
        method: aggregate by nullable INT8, INT16, and INT32 fields.
        expected: buckets use the requested integer field and top_hits match each bucket key.
        """
        client = self._client(alias=self.shared_alias)
        res, _ = self.search(
            client,
            self.collection_name,
            data=[self._query_vectors()[0]],
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[field_name],
            search_aggregation=SearchAggregation(
                fields=[field_name],
                size=4,
                metrics={"doc_count": {"count": "*"}},
                top_hits=TopHits(size=2, sort=[{"_score": "asc"}]),
            ),
        )

        assert len(res.agg_buckets) == 1
        assert 1 <= len(res.agg_buckets[0]) <= 4
        for bucket in res.agg_buckets[0]:
            key = self._key_value(bucket, field_name)
            assert [entry["field_name"] for entry in bucket.key] == [field_name]
            assert bucket.metrics["doc_count"] == bucket.count
            for hit in bucket.hits:
                assert hit.fields.get(field_name) == key

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_nullable_metric_field(self):
        """
        target: verify metrics on nullable scalar fields skip NULL values.
        method: aggregate rows with price values and NULL price values in the same brand bucket.
        expected: count(price)/sum(price)/min(price)/max(price) match the non-NULL top_hits only.
        """
        client = self._client(alias=self.shared_alias)
        res, _ = self.search(
            client,
            self.collection_name,
            data=[self._query_vectors()[0]],
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            output_fields=[self.brand_field, self.price_field],
            search_aggregation=SearchAggregation(
                fields=[self.brand_field],
                size=5,
                metrics={
                    "doc_count": {"count": "*"},
                    "price_count": {"count": self.price_field},
                    "price_sum": {"sum": self.price_field},
                    "price_min": {"min": self.price_field},
                    "price_max": {"max": self.price_field},
                },
                top_hits=TopHits(size=10, sort=[{"_score": "asc"}]),
            ),
        )

        assert len(res.agg_buckets) == 1
        checked_nullable_metric = False
        for bucket in res.agg_buckets[0]:
            prices = [hit.fields.get(self.price_field) for hit in bucket.hits]
            non_null_prices = [price for price in prices if price is not None]
            assert bucket.metrics["doc_count"] == bucket.count
            assert bucket.metrics["price_count"] == len(non_null_prices)
            if non_null_prices:
                assert bucket.metrics["price_sum"] == sum(non_null_prices)
                assert bucket.metrics["price_min"] == min(non_null_prices)
                assert bucket.metrics["price_max"] == max(non_null_prices)
            if len(non_null_prices) < len(prices):
                checked_nullable_metric = True
        assert checked_nullable_metric

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_single_field_top_hits(self):
        """
        target: verify basic search_aggregation with one group field and top_hits.
        method: 1. search with brand buckets, doc_count/sum(price), and top_hits sorted by _score asc
                2. verify bucket count, key field names, metrics, hit fields, and L2 score order
                3. verify metric values against returned top_hits
        expected: each query returns 4 brand buckets, each bucket has 2 hits from the same brand with consistent metrics.
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
            filter=self._non_null_expr(self.brand_field, self.price_field),
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_aggregation_composite_key_metrics_order(self):
        """
        target: verify search_aggregation with composite group fields, metrics, bucket order, and sorted top_hits.
        method: 1. search with (brand, color) buckets, avg_price/doc_count metrics, order by avg_price desc
                2. verify composite key field names, metric arithmetic, bucket ordering, and top_hits price order
                3. verify metrics against returned top_hits
        expected: each query returns 4 composite buckets ordered by avg_price desc with consistent per-bucket metrics.
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
            filter=self._non_null_expr(self.brand_field, self.color_field, self.price_field),
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_order_by_key_and_multi_criteria(self):
        """
        target: verify bucket order supports _key and multiple order criteria.
        method: aggregate by brand with _key asc, then with _count desc + _key asc.
        expected: buckets are sorted by key and by the multi-criteria tuple.
        """
        client = self._client(alias=self.shared_alias)
        res, _ = self.search(
            client,
            self.collection_name,
            data=[self._query_vectors()[0]],
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            filter=self._non_null_expr(self.brand_field),
            output_fields=[self.brand_field],
            search_aggregation=SearchAggregation(
                fields=[self.brand_field],
                size=4,
                metrics={"doc_count": {"count": "*"}},
                order=[{"_key": "asc"}],
                top_hits=TopHits(size=1),
            ),
        )

        buckets = res.agg_buckets[0]
        keys = [self._key_value(bucket, self.brand_field) for bucket in buckets]
        assert keys == sorted(keys)
        for bucket in buckets:
            assert bucket.metrics["doc_count"] == bucket.count

        res, _ = self.search(
            client,
            self.collection_name,
            data=[self._query_vectors()[0]],
            anns_field=self.vector_field,
            search_params=self._search_params(),
            limit=ct.default_limit,
            filter=self._non_null_expr(self.brand_field),
            output_fields=[self.brand_field],
            search_aggregation=SearchAggregation(
                fields=[self.brand_field],
                size=4,
                metrics={"doc_count": {"count": "*"}},
                order=[{"_count": "desc"}, {"_key": "asc"}],
                top_hits=TopHits(size=1),
            ),
        )

        buckets = res.agg_buckets[0]
        order_pairs = [(bucket.count, self._key_value(bucket, self.brand_field)) for bucket in buckets]
        assert order_pairs == sorted(order_pairs, key=lambda item: (-item[0], item[1]))
        for bucket in buckets:
            assert bucket.metrics["doc_count"] == bucket.count

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
            filter=(
                f"{self.in_stock_field} == true and "
                f"{self._non_null_expr(self.category_field, self.brand_field, self.price_field, self.rating_field)}"
            ),
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


class TestSearchAggregationIndependent(TestMilvusClientV2Base):
    nb = ct.default_nb

    @staticmethod
    def _vector_dim(vector_type):
        if vector_type == DataType.SPARSE_FLOAT_VECTOR:
            return 1000
        if vector_type == DataType.BINARY_VECTOR:
            return ct.default_dim
        return ct.default_dim

    @staticmethod
    def _vector_field_name(vector_type):
        return ct.default_field_name_map.get(vector_type, ct.default_float_vec_field_name)

    @staticmethod
    def _index_type(vector_type):
        if vector_type == DataType.SPARSE_FLOAT_VECTOR:
            return "SPARSE_INVERTED_INDEX"
        if vector_type == DataType.BINARY_VECTOR:
            return "BIN_FLAT"
        if vector_type == DataType.INT8_VECTOR:
            return "HNSW"
        return "FLAT"

    @staticmethod
    def _index_params(vector_type):
        if vector_type == DataType.INT8_VECTOR:
            return {"M": 8, "efConstruction": 64}
        return {}

    def _prepare_dynamic_field_collection(self, client, collection_name, controlled_vectors=False):
        dim = ct.default_dim
        vector_field = ct.default_float_vec_field_name
        static_group_field = "brand"
        dynamic_group_field = "dynamic_brand"
        dynamic_metric_field = "dynamic_price"

        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(field_name=ct.default_primary_field_name, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name=static_group_field, datatype=DataType.VARCHAR, max_length=64)
        self.create_collection(client, collection_name, schema=schema)

        if controlled_vectors:
            vectors = []
            for i in range(self.nb):
                vector = [0.0] * dim
                vector[0] = i / 1000.0
                vectors.append(vector)
        else:
            vectors = cf.gen_vectors(self.nb, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)

        rows = []
        for i in range(self.nb):
            rows.append(
                {
                    ct.default_primary_field_name: i,
                    vector_field: vectors[i],
                    static_group_field: "brand_0" if controlled_vectors else f"brand_{i % 3}",
                    dynamic_group_field: f"dynamic_brand_{i % 3}",
                    dynamic_metric_field: 100 - i if controlled_vectors and i < 5 else i,
                }
            )
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)
        return vectors, vector_field, static_group_field, dynamic_group_field, dynamic_metric_field

    def _prepare_json_field_collection(self, client, collection_name, enable_dynamic_field=False):
        dim = ct.default_dim
        vector_field = ct.default_float_vec_field_name
        static_group_field = "brand"
        json_field = "meta"
        dynamic_group_field = "dynamic_brand"

        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(field_name=ct.default_primary_field_name, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name=static_group_field, datatype=DataType.VARCHAR, max_length=64)
        schema.add_field(field_name=json_field, datatype=DataType.JSON)
        self.create_collection(client, collection_name, schema=schema)

        vectors = cf.gen_vectors(self.nb, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows = []
        for i in range(self.nb):
            row = {
                ct.default_primary_field_name: i,
                vector_field: vectors[i],
                static_group_field: f"brand_{i % 3}",
                json_field: {"score": i, "tag": f"tag_{i % 5}"},
            }
            if enable_dynamic_field:
                row[dynamic_group_field] = f"dynamic_brand_{i % 3}"
            rows.append(row)
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)
        return vectors, vector_field, static_group_field, json_field, dynamic_group_field

    def _prepare_struct_array_embedding_list_collection(self, client, collection_name):
        dim = ct.default_dim

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_group", datatype=DataType.VARCHAR, max_length=32)
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("label", DataType.VARCHAR, max_length=32)
        schema.add_field(
            "structA",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=4,
        )

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="structA[embedding]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params={"M": 8, "efConstruction": 64},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        vectors = cf.gen_vectors(self.nb * 2, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows = [
            {
                "id": i,
                "doc_group": f"group_{i % 3}",
                "structA": [
                    {"embedding": vectors[i * 2], "label": f"label_{i}_0"},
                    {"embedding": vectors[i * 2 + 1], "label": f"label_{i}_1"},
                ],
            }
            for i in range(self.nb)
        ]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return vectors

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_type", ct.all_vector_types)
    def test_search_aggregation_all_vector_types(self, vector_type):
        """
        target: verify search_aggregation works with every top-level vector field type.
        method: create one default-sized collection per vector type, search, and aggregate by scalar bucket.
        expected: aggregation succeeds for all regular Milvus vector types with valid index/metric combinations.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = self._vector_dim(vector_type)
        vector_field = self._vector_field_name(vector_type)
        metric_type = ct.default_metric_for_vector_type[vector_type]

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(field_name=ct.default_primary_field_name, datatype=DataType.INT64, is_primary=True)
        if vector_type == DataType.SPARSE_FLOAT_VECTOR:
            schema.add_field(field_name=vector_field, datatype=vector_type)
        else:
            schema.add_field(field_name=vector_field, datatype=vector_type, dim=dim)
        schema.add_field(field_name=ct.default_int64_field_name, datatype=DataType.INT64)
        self.create_collection(client, collection_name, schema=schema)

        vectors = cf.gen_vectors(self.nb, dim=dim, vector_data_type=vector_type)
        rows = [
            {
                ct.default_primary_field_name: i,
                vector_field: vectors[i],
                ct.default_int64_field_name: i % 3,
            }
            for i in range(self.nb)
        ]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=vector_field,
            index_type=self._index_type(vector_type),
            metric_type=metric_type,
            params=self._index_params(vector_type),
        )
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        res, _ = self.search(
            client,
            collection_name,
            data=[vectors[0]],
            anns_field=vector_field,
            search_params={
                "metric_type": metric_type,
                "params": {"ef": 32} if vector_type == DataType.INT8_VECTOR else {},
            },
            limit=20,
            output_fields=[ct.default_int64_field_name],
            search_aggregation=SearchAggregation(
                fields=[ct.default_int64_field_name],
                size=3,
                metrics={"doc_count": {"count": "*"}},
                top_hits=TopHits(size=1),
            ),
        )

        assert len(res.agg_buckets) == 1
        assert 1 <= len(res.agg_buckets[0]) <= 3
        for bucket in res.agg_buckets[0]:
            assert bucket.metrics["doc_count"] == bucket.count
            assert len(bucket.hits) == 1

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_reject_dynamic_group_field(self):
        """
        target: clarify search_aggregation compatibility with dynamic fields.
        method: create a dynamic-field-enabled collection and group by an inserted dynamic scalar field.
        expected: request is rejected because JSON / dynamic group-by plumbing is not supported yet.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        vectors, vector_field, _, dynamic_group_field, _ = self._prepare_dynamic_field_collection(
            client, collection_name
        )

        error = {ct.err_code: 999, ct.err_msg: "JSON / dynamic fields are not yet supported"}
        self.search(
            client,
            collection_name,
            data=[vectors[0]],
            anns_field=vector_field,
            search_params={"metric_type": "L2"},
            limit=20,
            output_fields=[dynamic_group_field],
            search_aggregation=SearchAggregation(fields=[dynamic_group_field], size=2),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_reject_dynamic_metric_field_with_order(self):
        """
        target: clarify search_aggregation metric/order compatibility with dynamic fields.
        method: aggregate by a static scalar field, compute sum(dynamic_price), and order by that metric alias.
        expected: request is rejected because JSON / dynamic metric fields are not yet supported with search_aggregation.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        vectors, vector_field, static_group_field, _, dynamic_metric_field = self._prepare_dynamic_field_collection(
            client, collection_name
        )

        error = {ct.err_code: 999, ct.err_msg: "JSON / dynamic fields are not yet supported"}
        self.search(
            client,
            collection_name,
            data=[vectors[0]],
            anns_field=vector_field,
            search_params={"metric_type": "L2"},
            limit=20,
            output_fields=[static_group_field],
            search_aggregation=SearchAggregation(
                fields=[static_group_field],
                size=2,
                metrics={"dynamic_total": {"sum": dynamic_metric_field}},
                order=[{"dynamic_total": "desc"}],
            ),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_top_hits_sort_by_dynamic_field(self):
        """
        target: verify top_hits.sort rejects dynamic scalar fields.
        method: request top_hits sort by an inserted dynamic scalar field.
        expected: request is rejected because JSON / dynamic top_hits sort is not supported yet.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        vectors, vector_field, static_group_field, _, dynamic_metric_field = self._prepare_dynamic_field_collection(
            client, collection_name, controlled_vectors=True
        )

        query_vector = [0.0] * ct.default_dim
        error = {
            ct.err_code: 999,
            ct.err_msg: "JSON / dynamic fields are not yet supported",
        }
        self.search(
            client,
            collection_name,
            data=[query_vector],
            anns_field=vector_field,
            search_params={"metric_type": "L2"},
            limit=20,
            output_fields=[static_group_field],
            search_aggregation=SearchAggregation(
                fields=[static_group_field],
                size=1,
                top_hits=TopHits(size=5, sort=[{dynamic_metric_field: "asc"}]),
            ),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        assert vectors[0] == query_vector

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric_field", ["meta", "dynamic_brand"])
    def test_search_aggregation_count_json_or_dynamic_metric_reject(self, metric_field):
        """
        target: verify count(JSON/dynamic field) is explicitly rejected.
        method: aggregate by a static scalar field and compute count(meta) or count(dynamic_brand).
        expected: request is rejected because JSON / dynamic metric fields are not supported yet.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        vectors, vector_field, static_group_field, _, _ = self._prepare_json_field_collection(
            client, collection_name, enable_dynamic_field=True
        )

        metric_alias = "field_count"
        error = {
            ct.err_code: 999,
            ct.err_msg: "JSON / dynamic fields are not yet supported",
        }
        self.search(
            client,
            collection_name,
            data=[vectors[0]],
            anns_field=vector_field,
            search_params={"metric_type": "L2"},
            limit=20,
            output_fields=[static_group_field],
            search_aggregation=SearchAggregation(
                fields=[static_group_field],
                size=2,
                metrics={metric_alias: {"count": metric_field}},
                order=[{metric_alias: "desc"}],
            ),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_reject_json_group_field(self):
        """
        target: clarify search_aggregation compatibility with explicit JSON group fields.
        method: group by a JSON field and construct a JSON-path group key.
        expected: JSON field group-by is rejected; JSON path group-by is rejected by SDK validation.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        vectors, vector_field, _, json_field, _ = self._prepare_json_field_collection(client, collection_name)

        error = {ct.err_code: 999, ct.err_msg: "JSON / dynamic fields are not yet supported"}
        self.search(
            client,
            collection_name,
            data=[vectors[0]],
            anns_field=vector_field,
            search_params={"metric_type": "L2"},
            limit=20,
            output_fields=[json_field],
            search_aggregation=SearchAggregation(fields=[json_field], size=2),
            check_task=CheckTasks.err_res,
            check_items=error,
        )
        with pytest.raises(ParamError, match="bracketed JSON path expressions"):
            SearchAggregation(fields=[f"{json_field}['tag']"], size=2)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_reject_json_metric_field_and_sort_path(self):
        """
        target: clarify search_aggregation metric and top_hits.sort compatibility with explicit JSON fields.
        method: use sum(meta) as a metric source and meta['score'] as a top_hits sort key.
        expected: unsupported JSON metric and JSON-path sort are rejected with deterministic errors.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        vectors, vector_field, static_group_field, json_field, _ = self._prepare_json_field_collection(
            client, collection_name
        )

        metric_error = {
            ct.err_code: 999,
            ct.err_msg: "JSON / dynamic fields are not yet supported",
        }
        self.search(
            client,
            collection_name,
            data=[vectors[0]],
            anns_field=vector_field,
            search_params={"metric_type": "L2"},
            limit=20,
            output_fields=[static_group_field],
            search_aggregation=SearchAggregation(
                fields=[static_group_field],
                size=2,
                metrics={"json_total": {"sum": json_field}},
                order=[{"json_total": "desc"}],
            ),
            check_task=CheckTasks.err_res,
            check_items=metric_error,
        )

        sort_error = {ct.err_code: 999, ct.err_msg: "JSON path is not yet supported"}
        self.search(
            client,
            collection_name,
            data=[vectors[0]],
            anns_field=vector_field,
            search_params={"metric_type": "L2"},
            limit=20,
            output_fields=[static_group_field],
            search_aggregation=SearchAggregation(
                fields=[static_group_field],
                size=2,
                top_hits=TopHits(size=2, sort=[{f"{json_field}['score']": "asc"}]),
            ),
            check_task=CheckTasks.err_res,
            check_items=sort_error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_aggregation_on_growing_segment(self):
        """
        target: verify search_aggregation works on growing segment results.
        method: load a collection with sealed data, insert unflushed rows, then aggregate growing-only results.
        expected: aggregation returns the growing-only bucket and top_hits from the unflushed rows.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = ct.default_dim
        vector_field = ct.default_float_vec_field_name
        brand_field = "brand"
        growing_brand = "growing_brand"

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(field_name=ct.default_primary_field_name, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=vector_field, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name=brand_field, datatype=DataType.VARCHAR, max_length=64)
        self.create_collection(client, collection_name, schema=schema)

        vectors = cf.gen_vectors(self.nb + 20, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        sealed_rows = [
            {
                ct.default_primary_field_name: i,
                vector_field: vectors[i],
                brand_field: f"brand_{i % 4}",
            }
            for i in range(self.nb)
        ]
        self.insert(client, collection_name, data=sealed_rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field, index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        growing_rows = []
        for i in range(20):
            pk = self.nb + i
            growing_rows.append(
                {
                    ct.default_primary_field_name: pk,
                    vector_field: vectors[i],
                    brand_field: growing_brand,
                }
            )
        self.insert(client, collection_name, data=growing_rows)

        res, _ = self.search(
            client,
            collection_name,
            data=[vectors[0]],
            anns_field=vector_field,
            search_params={"metric_type": "L2"},
            limit=ct.default_limit,
            filter=f'{brand_field} == "{growing_brand}"',
            output_fields=[brand_field],
            search_aggregation=SearchAggregation(
                fields=[brand_field],
                size=1,
                metrics={"doc_count": {"count": "*"}},
                top_hits=TopHits(size=3, sort=[{"_score": "asc"}]),
            ),
        )

        assert len(res.agg_buckets) == 1
        assert len(res.agg_buckets[0]) == 1
        bucket = res.agg_buckets[0][0]
        assert bucket.key[0]["value"] == growing_brand
        assert bucket.metrics["doc_count"] == bucket.count
        assert len(bucket.hits) == 3
        for hit in bucket.hits:
            assert hit.pk >= self.nb
            assert hit.fields[brand_field] == growing_brand

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_iterator_with_search_aggregation_not_silent_empty(self):
        """
        target: verify search_iterator explicitly rejects search_aggregation.
        method: pass search_aggregation to MilvusClient.search_iterator.
        expected: request is rejected instead of silently dropping search_aggregation.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = ct.default_dim
        self.create_collection(client, collection_name, dimension=dim, metric_type="L2")
        vectors = cf.gen_vectors(self.nb, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows = [{"id": i, "vector": vectors[i]} for i in range(self.nb)]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        normal_iterator, _ = self.search_iterator(
            client,
            collection_name,
            data=[vectors[0]],
            batch_size=10,
            limit=10,
            search_params={"metric_type": "L2"},
            output_fields=["id"],
        )
        normal_batch = normal_iterator.next()
        normal_iterator.close()
        assert len(normal_batch) > 0

        error = {ct.err_code: 999, ct.err_msg: "not supported with search_aggregation"}
        self.search_iterator(
            client,
            collection_name,
            data=[vectors[0]],
            batch_size=10,
            limit=10,
            search_params={"metric_type": "L2"},
            output_fields=["id"],
            search_aggregation=SearchAggregation(fields=["id"], size=2),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_struct_array_element_search_reject_non_primary_key_aggregation(self):
        """
        target: clarify StructArray element-vector search compatibility with non-primary-key aggregation.
        method: search structA[embedding] and request aggregation by non-primary-key fields.
        expected: request is rejected when any aggregation field is not the primary key.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = ct.default_dim
        group_field = "doc_group"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=group_field, datatype=DataType.VARCHAR, max_length=32)
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("label", DataType.VARCHAR, max_length=32)
        schema.add_field(
            "structA",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=4,
        )

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="structA[embedding]",
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 8, "efConstruction": 64},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        vectors = cf.gen_vectors(self.nb * 2, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows = []
        for i in range(self.nb):
            rows.append(
                {
                    "id": i,
                    group_field: f"group_{i % 2}",
                    "structA": [
                        {"embedding": vectors[i * 2], "label": f"label_{i}_0"},
                        {"embedding": vectors[i * 2 + 1], "label": f"label_{i}_1"},
                    ],
                }
            )
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        error = {ct.err_code: 1100, ct.err_msg: "only group by primary key is supported for element-level search"}
        self.search(
            client,
            collection_name,
            data=[vectors[0]],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE", "params": {"ef": 32}},
            limit=10,
            output_fields=["id", group_field],
            search_aggregation=SearchAggregation(fields=[group_field], size=2),
            check_task=CheckTasks.err_res,
            check_items=error,
        )
        self.search(
            client,
            collection_name,
            data=[vectors[0]],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE", "params": {"ef": 32}},
            limit=10,
            output_fields=["id", group_field],
            search_aggregation=SearchAggregation(fields=["id", group_field], size=2),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_struct_array_embedding_list_search_reject_search_aggregation(self):
        """
        target: clarify StructArray embedding-list-level search compatibility with search_aggregation.
        method: search structA[embedding] with EmbeddingList input and request aggregation by primary key.
        expected: request is rejected because group-by is not supported for multi-search-multi on embedding list fields.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        vectors = self._prepare_struct_array_embedding_list_collection(client, collection_name)

        tensor = EmbeddingList()
        tensor.add(vectors[0])
        tensor.add(vectors[1])

        error = {ct.err_code: 1100, ct.err_msg: "group by is not supported for multi-search-multi"}
        self.search(
            client,
            collection_name,
            data=[tensor],
            anns_field="structA[embedding]",
            search_params={"metric_type": "MAX_SIM_COSINE", "params": {"ef": 32}},
            limit=10,
            output_fields=["id"],
            search_aggregation=SearchAggregation(fields=["id"], size=2),
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_struct_array_element_search_with_primary_key_aggregation(self):
        """
        target: clarify StructArray element-vector search support when grouped by primary key.
        method: search structA[embedding] and request aggregation by the primary key field.
        expected: request succeeds because element-level StructArray search only supports primary-key grouping.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = ct.default_dim

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("label", DataType.VARCHAR, max_length=32)
        schema.add_field(
            "structA",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=4,
        )

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="structA[embedding]",
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 8, "efConstruction": 64},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        vectors = cf.gen_vectors(self.nb * 2, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows = [
            {
                "id": i,
                "structA": [
                    {"embedding": vectors[i * 2], "label": f"label_{i}_0"},
                    {"embedding": vectors[i * 2 + 1], "label": f"label_{i}_1"},
                ],
            }
            for i in range(self.nb)
        ]
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        res, _ = self.search(
            client,
            collection_name,
            data=[vectors[0]],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE", "params": {"ef": 32}},
            limit=10,
            output_fields=["id"],
            search_aggregation=SearchAggregation(
                fields=["id"],
                size=3,
                metrics={"doc_count": {"count": "*"}},
                top_hits=TopHits(size=1),
            ),
        )

        assert len(res.agg_buckets) == 1
        assert 1 <= len(res.agg_buckets[0]) <= 3
        for bucket in res.agg_buckets[0]:
            assert [entry["field_name"] for entry in bucket.key] == ["id"]
            assert bucket.metrics["doc_count"] == bucket.count
            assert len(bucket.hits) == 1


@pytest.mark.xdist_group("TestSearchAggregationTextAndBM25")
@pytest.mark.tags(CaseLabel.L1)
class TestSearchAggregationTextAndBM25(TestMilvusClientV2Base):
    shared_alias = "TestSearchAggregationTextAndBM25"
    nb = ct.default_nb

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchAggregationTextAndBM25" + cf.gen_unique_str("_")
        self.primary_field = ct.default_primary_field_name
        self.vector_field = ct.default_float_vec_field_name
        self.text_field = "document"
        self.bm25_field = "bm25_sparse"
        self.topic_field = "topic"
        self.dim = ct.default_dim
        self.vectors = cf.gen_vectors(self.nb, dim=self.dim, vector_data_type=DataType.FLOAT_VECTOR)

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=self.primary_field, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=self.vector_field, datatype=DataType.FLOAT_VECTOR, dim=self.dim)
        schema.add_field(
            field_name=self.text_field,
            datatype=DataType.VARCHAR,
            max_length=2048,
            enable_analyzer=True,
            enable_match=True,
        )
        schema.add_field(field_name=self.topic_field, datatype=DataType.VARCHAR, max_length=64)
        schema.add_field(field_name=self.bm25_field, datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_function(
            Function(
                name="document_bm25",
                function_type=FunctionType.BM25,
                input_field_names=[self.text_field],
                output_field_names=[self.bm25_field],
                params={},
            )
        )

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.vector_field, index_type="FLAT", metric_type="L2")
        index_params.add_index(field_name=self.bm25_field, index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
        self.create_collection(
            client, self.collection_name, schema=schema, index_params=index_params, force_teardown=False
        )

        rows = []
        topics = ["database", "search", "storage"]
        for i in range(self.nb):
            topic = topics[i % len(topics)]
            if topic == "database":
                text = f"milvus vector database search aggregation document {i}"
            elif topic == "search":
                text = f"vector search relevance ranking document {i}"
            else:
                text = f"distributed storage compaction segment document {i}"
            rows.append(
                {
                    self.primary_field: i,
                    self.vector_field: self.vectors[i],
                    self.text_field: text,
                    self.topic_field: topic,
                }
            )
        self.insert(client, self.collection_name, data=rows)
        self.flush(client, self.collection_name)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_match_filter_with_search_aggregation(self):
        """
        target: verify TextMatch filter supports search_aggregation.
        method: dense vector search with text_match(document, 'database') and aggregate by topic.
        expected: only matching documents are returned and bucket key is database.
        """
        client = self._client(alias=self.shared_alias)
        res, _ = self.search(
            client,
            self.collection_name,
            data=[self.vectors[0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=20,
            filter=f"text_match({self.text_field}, 'database')",
            output_fields=[self.text_field, self.topic_field],
            search_aggregation=SearchAggregation(
                fields=[self.topic_field],
                size=2,
                metrics={"doc_count": {"count": "*"}},
                top_hits=TopHits(size=3, sort=[{"_score": "asc"}]),
            ),
        )

        assert len(res.agg_buckets) == 1
        assert len(res.agg_buckets[0]) == 1
        bucket = res.agg_buckets[0][0]
        assert bucket.key[0]["value"] == "database"
        assert bucket.metrics["doc_count"] == bucket.count
        for hit in bucket.hits:
            assert "database" in hit.fields[self.text_field]
            assert hit.fields[self.topic_field] == "database"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bm25_search_with_search_aggregation(self):
        """
        target: verify single-field BM25 search supports search_aggregation.
        method: BM25 search by text query and aggregate returned hits by topic.
        expected: BM25 search succeeds and aggregation buckets contain top hits.
        """
        client = self._client(alias=self.shared_alias)
        res, _ = self.search(
            client,
            self.collection_name,
            data=["database"],
            anns_field=self.bm25_field,
            search_params={"metric_type": "BM25"},
            limit=20,
            output_fields=[self.text_field, self.topic_field],
            search_aggregation=SearchAggregation(
                fields=[self.topic_field],
                size=3,
                metrics={"doc_count": {"count": "*"}},
                order=[{"_count": "desc"}],
                top_hits=TopHits(size=2, sort=[{"_score": "desc"}]),
            ),
        )

        assert len(res.agg_buckets) == 1
        assert len(res.agg_buckets[0]) >= 1
        counts = [bucket.count for bucket in res.agg_buckets[0]]
        assert counts == sorted(counts, reverse=True)
        for bucket in res.agg_buckets[0]:
            assert bucket.metrics["doc_count"] == bucket.count
            assert len(bucket.hits) <= 2

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_reject_search_aggregation(self):
        """
        target: verify hybrid_search is not supported with search_aggregation.
        method: combine dense and BM25 AnnSearchRequest with search_aggregation.
        expected: proxy rejects the advanced search request.
        """
        client = self._client(alias=self.shared_alias)
        dense_req = AnnSearchRequest(
            data=[self.vectors[0]],
            anns_field=self.vector_field,
            param={"metric_type": "L2"},
            limit=10,
        )
        bm25_req = AnnSearchRequest(
            data=["database"],
            anns_field=self.bm25_field,
            param={"metric_type": "BM25"},
            limit=10,
        )
        error = {ct.err_code: 1, ct.err_msg: "search_aggregation is not supported in hybrid_search"}
        self.hybrid_search(
            client,
            self.collection_name,
            reqs=[dense_req, bm25_req],
            ranker=RRFRanker(),
            limit=10,
            output_fields=[self.topic_field],
            search_aggregation=SearchAggregation(fields=[self.topic_field], size=2),
            check_task=CheckTasks.err_res,
            check_items=error,
        )
