import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from milvus_client.expressions.expression_test_utils import (
    add_minimal_query_vector_field,
    assert_query_ids,
    query_ids,
    register_collection_cleanup,
    wait_for_materialized_index,
)
from milvus_client.expressions.filtering_case_matrix import (
    GEOMETRY_INDEX_CONSISTENCY_CASES,
    REAL_INDEX_ROW_COUNT,
    TEXT_FILTER_CASES,
    TEXT_ROWS,
    TIMESTAMPTZ_FILTER_CASES,
    TIMESTAMPTZ_INDEX_CONSISTENCY_CASES,
    TIMESTAMPTZ_INTERVAL_51538_CASES,
    build_geometry_rows,
    build_timestamptz_rows,
)
from pymilvus import DataType
from pymilvus.client.types import IndexState

default_pk = "id"
default_vec = "vector"
default_dim = 8

TIMESTAMPTZ_INDEX_NAME = "idx_event_time_stl_sort"
GEOMETRY_INDEX_NAME = "idx_geometry_rtree"


def timestamptz_filter_params():
    case_names = ["eq_utc", "range_same_day", "or_two_instants", "is_null", "is_not_null"]
    return [
        pytest.param(
            expr,
            expected_ids,
            marks=pytest.mark.tags(CaseLabel.L1 if case_name == "eq_utc" else CaseLabel.L2),
            id=case_name,
        )
        for case_name, (expr, expected_ids) in zip(case_names, TIMESTAMPTZ_FILTER_CASES)
    ]


def timestamptz_interval_params():
    params = []
    for case_name, expr, expected_ids in TIMESTAMPTZ_INTERVAL_51538_CASES:
        for path_name, field_name in (("plain", "event_time_plain"), ("indexed", "event_time_indexed")):
            params.append(
                pytest.param(
                    f"{case_name}_{path_name}",
                    expr.replace("event_time_plain", field_name),
                    expected_ids,
                    id=f"{case_name}-{path_name}",
                )
            )
    return params


@pytest.mark.xdist_group("TestFilteringSpecialTypes")
class TestFilteringSpecialTypes(TestMilvusClientV2Base):
    shared_alias = "TestFilteringSpecialTypes"

    @pytest.fixture(scope="class")
    def timestamptz_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_timestamptz" + cf.gen_unique_str("_")
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        add_minimal_query_vector_field(schema, vector_field=default_vec, dim=default_dim)
        schema.add_field("event_time_plain", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field("event_time_indexed", DataType.TIMESTAMPTZ, nullable=True)
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            force_teardown=False,
            consistency_level="Strong",
        )
        register_collection_cleanup(self, request, self.shared_alias, collection_name)
        self.insert(
            client,
            collection_name,
            data=build_timestamptz_rows(
                default_pk,
                use_dual_fields=True,
                row_count=REAL_INDEX_ROW_COUNT,
            ),
        )
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vec, index_type="FLAT", metric_type="COSINE")
        index_params.add_index(
            "event_time_indexed",
            index_type="STL_SORT",
            index_name=TIMESTAMPTZ_INDEX_NAME,
        )
        self.create_index(client, collection_name, index_params=index_params)
        self.__class__.timestamptz_index_info = wait_for_materialized_index(
            self,
            client,
            collection_name,
            TIMESTAMPTZ_INDEX_NAME,
            expected_total_rows=REAL_INDEX_ROW_COUNT,
            expected_field_name="event_time_indexed",
            expected_index_type="STL_SORT",
        )
        self.load_collection(client, collection_name)
        yield collection_name

    @pytest.fixture(scope="class")
    def text_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_text" + cf.gen_unique_str("_")
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        add_minimal_query_vector_field(schema, vector_field=default_vec, dim=default_dim)
        schema.add_field(
            "content",
            DataType.VARCHAR,
            max_length=512,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params={"tokenizer": "standard"},
        )
        schema.add_field("topic", DataType.VARCHAR, max_length=64)
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            force_teardown=False,
            consistency_level="Strong",
        )
        register_collection_cleanup(self, request, self.shared_alias, collection_name)
        self.insert(client, collection_name, data=TEXT_ROWS)
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vec, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)
        yield collection_name

    @pytest.fixture(scope="class")
    def geometry_collection(self, request):
        client = self._client(alias=self.shared_alias)
        collection_name = "filter_geometry" + cf.gen_unique_str("_")
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        add_minimal_query_vector_field(schema, vector_field=default_vec, dim=default_dim)
        schema.add_field("geo_plain", DataType.GEOMETRY)
        schema.add_field("geo_indexed", DataType.GEOMETRY)
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            force_teardown=False,
            consistency_level="Strong",
        )
        register_collection_cleanup(self, request, self.shared_alias, collection_name)
        self.insert(client, collection_name, data=build_geometry_rows(REAL_INDEX_ROW_COUNT))
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vec, index_type="FLAT", metric_type="COSINE")
        index_params.add_index(
            "geo_indexed",
            index_type="RTREE",
            index_name=GEOMETRY_INDEX_NAME,
        )
        self.create_index(client, collection_name, index_params=index_params)
        self.__class__.geometry_index_info = wait_for_materialized_index(
            self,
            client,
            collection_name,
            GEOMETRY_INDEX_NAME,
            expected_total_rows=REAL_INDEX_ROW_COUNT,
            expected_field_name="geo_indexed",
            expected_index_type="RTREE",
        )
        self.load_collection(client, collection_name)
        yield collection_name

    @pytest.mark.parametrize(
        "expr, expected_ids",
        timestamptz_filter_params(),
    )
    def test_timestamptz_filter_cases(self, timestamptz_collection, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, timestamptz_collection, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("case_name, expr, expected_ids", timestamptz_interval_params())
    def test_timestamptz_interval_null_regression_51538(self, timestamptz_collection, case_name, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, timestamptz_collection, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "case_name, plain_expr, indexed_expr, expected_ids",
        [pytest.param(*case, id=case[0]) for case in TIMESTAMPTZ_INDEX_CONSISTENCY_CASES],
    )
    def test_timestamptz_index_consistency_cases(
        self,
        timestamptz_collection,
        case_name,
        plain_expr,
        indexed_expr,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        plain_ids = query_ids(self, client, timestamptz_collection, plain_expr, pk_field=default_pk)
        indexed_ids = query_ids(self, client, timestamptz_collection, indexed_expr, pk_field=default_pk)
        assert plain_ids == expected_ids, f"{case_name} plain got {plain_ids}, expected {expected_ids}"
        assert indexed_ids == expected_ids, f"{case_name} indexed got {indexed_ids}, expected {expected_ids}"
        assert plain_ids == indexed_ids, f"{case_name} plain={plain_ids}, indexed={indexed_ids}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_timestamptz_index_is_materialized(self, timestamptz_collection):
        assert self.timestamptz_index_info["field_name"] == "event_time_indexed"
        assert self.timestamptz_index_info["index_type"] == "STL_SORT"
        assert self.timestamptz_index_info["state"] == IndexState.Finished.name
        assert self.timestamptz_index_info["indexed_rows"] == self.timestamptz_index_info["total_rows"]
        assert self.timestamptz_index_info["total_rows"] == REAL_INDEX_ROW_COUNT
        assert self.timestamptz_index_info["pending_index_rows"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expr, expected_ids",
        TEXT_FILTER_CASES,
        ids=["text_match", "phrase_match", "text_and_scalar", "text_or_scalar"],
    )
    def test_text_filter_cases(self, text_collection, expr, expected_ids):
        client = self._client(alias=self.shared_alias)
        assert_query_ids(self, client, text_collection, expr, expected_ids, pk_field=default_pk)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "case_name, plain_expr, indexed_expr, expected_ids",
        [pytest.param(*case, id=case[0]) for case in GEOMETRY_INDEX_CONSISTENCY_CASES],
    )
    def test_geometry_filter_cases(
        self,
        geometry_collection,
        case_name,
        plain_expr,
        indexed_expr,
        expected_ids,
    ):
        client = self._client(alias=self.shared_alias)
        plain_ids = query_ids(self, client, geometry_collection, plain_expr, pk_field=default_pk)
        indexed_ids = query_ids(self, client, geometry_collection, indexed_expr, pk_field=default_pk)
        assert plain_ids == expected_ids, f"{case_name} plain got {plain_ids}, expected {expected_ids}"
        assert indexed_ids == expected_ids, f"{case_name} indexed got {indexed_ids}, expected {expected_ids}"
        assert plain_ids == indexed_ids, f"{case_name} plain={plain_ids}, indexed={indexed_ids}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_geometry_index_is_materialized(self, geometry_collection):
        assert self.geometry_index_info["field_name"] == "geo_indexed"
        assert self.geometry_index_info["index_type"] == "RTREE"
        assert self.geometry_index_info["state"] == IndexState.Finished.name
        assert self.geometry_index_info["indexed_rows"] == self.geometry_index_info["total_rows"]
        assert self.geometry_index_info["total_rows"] == REAL_INDEX_ROW_COUNT
        assert self.geometry_index_info["pending_index_rows"] == 0
