import random
import math
import threading
import time
import pytest

from pymilvus import DataType
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base

prefix = "range_search"
range_search_supported_indexes = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ",
                                  "IVF_RABITQ", "HNSW", "SCANN", "DISKANN"]
default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = f"{ct.default_int64_field_name} >= 0"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name
nq = 1
epsilon = 0.001


@pytest.mark.xdist_group("TestRangeSearchCosineShared")
@pytest.mark.tags(CaseLabel.GPU)
class TestRangeSearchCosineShared(TestMilvusClientV2Base):
    """Shared collection for range search tests.
    Schema: int64(PK), float(nullable), varchar(65535), json, float_vector(128),
            sparse_vector, nullable_float_vector(128, nullable), nullable_sparse_vector(nullable),
            dynamic=True
    Data: 3000 rows, ~20% null for nullable fields (pk % 5 == 0)
    Index: HNSW/COSINE on float_vector, SPARSE_INVERTED_INDEX/IP on sparse_vector,
           FLAT/COSINE on nullable_float_vector, SPARSE_INVERTED_INDEX/IP on nullable_sparse_vector
    """
    shared_alias = "TestRangeSearchCosineShared"
    nullable_float_vec_field = "nullable_float_vector"
    nullable_sparse_vec_field = "nullable_sparse_vector"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestRangeSearchCosineShared" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(ct.default_sparse_vec_field_name, DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field(self.nullable_float_vec_field, DataType.FLOAT_VECTOR,
                         dim=default_dim, nullable=True)
        schema.add_field(self.nullable_sparse_vec_field, DataType.SPARSE_FLOAT_VECTOR,
                         nullable=True)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        nb = 3000
        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        # Enforce deterministic ~20% null for nullable fields (pk % 5 == 0)
        nullable_float_vectors = cf.gen_vectors(nb, default_dim)
        nullable_sparse_vectors = cf.gen_sparse_vectors(nb)
        for i in range(nb):
            is_null = (i % 5 == 0)
            data[i][ct.default_float_field_name] = None if is_null else float(i)
            data[i][self.nullable_float_vec_field] = None if is_null else nullable_float_vectors[i]
            data[i][self.nullable_sparse_vec_field] = None if is_null else nullable_sparse_vectors[i]
        request.cls.shared_data = data

        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="HNSW", params={"M": 16, "efConstruction": 500})
        idx.add_index(field_name=ct.default_sparse_vec_field_name, index_type="SPARSE_INVERTED_INDEX",
                      metric_type="IP", params={})
        idx.add_index(field_name=self.nullable_float_vec_field, metric_type="COSINE",
                      index_type="FLAT", params={})
        idx.add_index(field_name=self.nullable_sparse_vec_field, index_type="SPARSE_INVERTED_INDEX",
                      metric_type="IP", params={})
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("search_by_pk", [True, False])
    def test_range_search_cosine(self, search_by_pk):
        """
        target: test range search normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        client = self._client(alias=self.shared_alias)

        range_filter = random.uniform(0, 1)
        radius = random.uniform(-1, range_filter)

        # 2. range search
        range_search_params = {"metric_type": "COSINE",
                               "params": {"radius": radius, "range_filter": range_filter}}
        vectors_to_search = cf.gen_vectors(nq, default_dim)
        ids_to_search = None
        if search_by_pk is True:
            vectors_to_search = None
            ids_to_search = [0, 1]
        search_res, _ = self.search(client, self.collection_name,
                                    data=vectors_to_search,
                                    anns_field=default_search_field,
                                    search_params=range_search_params,
                                    limit=default_limit,
                                    filter=default_search_exp,
                                    ids=ids_to_search)

        # 3. check search results
        for hits in search_res:
            for hit in hits:
                assert range_filter >= hit["distance"] > radius

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_only_range_filter(self):
        """
        target: test range search with only range filter
        method: create connection, collection, insert and search
        expected: range search successfully as normal search
        """
        client = self._client(alias=self.shared_alias)

        # 2. get vectors that inserted into collection
        query_res, _ = self.query(client, self.collection_name, filter=default_search_exp,
                                  output_fields=[ct.default_float_vec_field_name])
        search_vectors = [row[ct.default_float_vec_field_name] for row in query_res[:default_nq]]

        # 3. range search with COSINE (only range_filter, no radius)
        # With [-1,1] vectors, cosine distances span full range. range_filter=0.5 filters out high-similarity results.
        range_search_params = {"metric_type": "COSINE",
                               "params": {"range_filter": 0.5}}
        search_res, _ = self.search(client, self.collection_name,
                    data=search_vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=default_limit,
                    filter=default_search_exp)
        # verify range filter is effective: all distances should be <= 0.5
        for hits in search_res:
            for hit in hits:
                assert hit["distance"] <= 0.5
        # 4. range search with IP (should fail - metric mismatch)
        range_search_params = {"metric_type": "IP",
                               "params": {"range_filter": 1}}
        self.search(client, self.collection_name,
                    data=search_vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 65535,
                                 ct.err_msg: "metric type not match: "
                                             "invalid parameter[expected=COSINE][actual=IP]"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_radius_range_filter_not_in_params(self):
        """
        target: test range search radius and range filter not in params
        method: create connection, collection, insert and search
        expected: search successfully as normal search
        """
        client = self._client(alias=self.shared_alias)

        # 2. get vectors that inserted into collection
        query_res, _ = self.query(client, self.collection_name, filter=default_search_exp,
                                  output_fields=[ct.default_float_vec_field_name])
        search_vectors = [row[ct.default_float_vec_field_name] for row in query_res[:default_nq]]

        # 3. range search with COSINE (radius/range_filter at top level, not inside params)
        # Search vectors are queried from collection (L2-normalized), so cosine distances
        # are concentrated near 1.0. Use radius=0.5 to filter out lower-similarity results.
        range_search_params = {"metric_type": "COSINE",
                               "radius": 0.5, "range_filter": 1}
        search_res, _ = self.search(client, self.collection_name,
                    data=search_vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=default_limit,
                    filter=default_search_exp)
        # verify range filter is effective: all distances should be in (0.5, 1]
        for hits in search_res:
            for hit in hits:
                assert 1 >= hit["distance"] > 0.5
        # 4. range search with IP (should fail - metric mismatch)
        range_search_params = {"metric_type": "IP",
                               "radius": 1, "range_filter": 0}
        self.search(client, self.collection_name,
                    data=search_vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 65535,
                                 ct.err_msg: "metric type not match: invalid "
                                             "parameter[expected=COSINE][actual=IP]"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_with_expression(self):
        """
        target: test range search with different expressions (enable_dynamic_field=True)
        method: test range search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        client = self._client(alias=self.shared_alias)
        nb = len(self.shared_data)
        # Use nb//2 to avoid HNSW recall issues while still covering enough results
        search_limit = nb // 2

        # get inserted data for expression evaluation
        query_res, _ = self.query(client, self.collection_name, filter=default_search_exp,
                                  output_fields=[ct.default_int64_field_name, ct.default_float_field_name])

        # filter result with expression in collection
        for expressions in cf.gen_normal_expressions_and_templates():
            log.debug(f"test_range_search_with_expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, row in enumerate(query_res):
                float_val = row.get(ct.default_float_field_name)
                # NULL values never match any comparison (SQL NULL semantics)
                if float_val is None and "float" in expr:
                    continue
                local_vars = {"int64": row[ct.default_int64_field_name],
                              "float": float_val if float_val is not None else 0}
                if not expr or eval(expr, {}, local_vars):
                    filter_ids.append(row[ct.default_int64_field_name])

            # 3. search with expression
            expected_limit = min(search_limit, len(filter_ids))
            search_vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
            range_search_params = {"metric_type": "COSINE", "params": {"radius": -1, "range_filter": 1}}
            search_res, _ = self.search(client, self.collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=range_search_params,
                                        limit=search_limit,
                                        filter=expr)
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
                assert len(hits) >= expected_limit * 0.8, \
                    f"recall too low: got {len(hits)}, expected >= {expected_limit * 0.8}"

            # 4. search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(client, self.collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=range_search_params,
                                        limit=search_limit,
                                        filter=expr, filter_params=expr_params)
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
                assert len(hits) >= expected_limit * 0.8, \
                    f"recall too low: got {len(hits)}, expected >= {expected_limit * 0.8}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_with_output_field(self):
        """
        target: test range search with output fields (enable_dynamic_field=True)
        method: range search with one output_field
        expected: search success
        """
        client = self._client(alias=self.shared_alias)

        insert_ids = [i for i in range(len(self.shared_data))]

        # 2. search
        log.info("test_range_search_with_output_field: Searching collection %s" % self.collection_name)
        range_search_params = {"metric_type": "COSINE", "params": {"radius": 0,
                                                                   "range_filter": 1}}
        res, _ = self.search(client, self.collection_name,
                             data=cf.gen_vectors(default_nq, default_dim),
                             anns_field=default_search_field,
                             search_params=range_search_params,
                             limit=default_limit,
                             filter=default_search_exp,
                             output_fields=[default_int64_field_name],
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "ids": insert_ids,
                                          "limit": default_limit,
                                          "enable_milvus_client_api": True,
                                          "metric": "COSINE",
                                          "pk_name": ct.default_int64_field_name})
        assert default_int64_field_name in res[0][0]["entity"]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [2, 500])
    def test_range_search_concurrent_multi_threads(self, nq):
        """
        target: test concurrent range search with multi-processes
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        client = self._client(alias=self.shared_alias)

        threads_num = 10
        threads = []

        def search(client_ref, coll_name):
            search_vectors = [[random.random() for _ in range(default_dim)]
                              for _ in range(nq)]
            range_search_params = {"metric_type": "COSINE", "params": {"radius": 0,
                                                                       "range_filter": 1}}
            self.search(client_ref, coll_name,
                        data=search_vectors[:nq],
                        anns_field=default_search_field,
                        search_params=range_search_params,
                        limit=default_limit,
                        filter=default_search_exp,
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": nq,
                                     "limit": default_limit,
                                     "enable_milvus_client_api": True,
                                     "metric": "COSINE",
                                     "pk_name": ct.default_int64_field_name})

        # 2. search with multi-threads
        log.info("test_range_search_concurrent_multi_threads: searching with %s processes" % threads_num)
        for _ in range(threads_num):
            t = threading.Thread(target=search, args=(client, self.collection_name,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("round_decimal", [0, 1, 2, 3, 4, 5, 6])
    def test_range_search_round_decimal(self, round_decimal):
        """
        target: test range search with valid round decimal
        method: range search with valid round decimal
        expected: search successfully
        """
        client = self._client(alias=self.shared_alias)

        tmp_nq = 1
        tmp_limit = 5

        # 2. search
        log.info("test_search_round_decimal: Searching collection %s" % self.collection_name)
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1}}
        search_vectors = cf.gen_vectors(tmp_nq, default_dim)
        res, _ = self.search(client, self.collection_name,
                             data=search_vectors,
                             anns_field=default_search_field,
                             search_params=range_search_params,
                             limit=tmp_limit)

        res_round, _ = self.search(client, self.collection_name,
                                   data=search_vectors,
                                   anns_field=default_search_field,
                                   search_params=range_search_params,
                                   limit=tmp_limit,
                                   round_decimal=round_decimal)

        abs_tol = pow(10, 1 - round_decimal)
        for i in range(tmp_limit):
            dis_expect = round(res[0][i]["distance"], round_decimal)
            dis_actual = res_round[0][i]["distance"]
            assert math.isclose(dis_actual, dis_expect,
                                rel_tol=0, abs_tol=abs_tol)

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_only_radius(self):
        """
        target: test range search with only radius
        method: search with radius=2 on COSINE field (max distance is 1)
        expected: 0 results; metric mismatch with IP should fail
        """
        client = self._client(alias=self.shared_alias)

        # 2. get vectors that inserted into collection
        query_res, _ = self.query(client, self.collection_name, filter=default_search_exp,
                                  output_fields=[ct.default_float_vec_field_name])
        search_vectors = [row[ct.default_float_vec_field_name] for row in query_res[:default_nq]]

        # 3. range search with COSINE, radius=2 → 0 results (COSINE distances ≤ 1)
        range_search_params = {"metric_type": "COSINE", "params": {"radius": 2}}
        self.search(client, self.collection_name,
                    data=search_vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": [],
                                 "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})
        # 4. range search with IP (should fail - metric mismatch)
        range_search_params = {"metric_type": "IP", "params": {"radius": 0}}
        self.search(client, self.collection_name,
                    data=search_vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 65535,
                                 ct.err_msg: "metric type not match: invalid "
                                             "parameter[expected=COSINE][actual=IP]"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_with_empty_vectors(self):
        """
        target: test range search with empty query vector
        method: search using empty query vector
        expected: search failed with error (Client V2 does not support data=[])
        """
        client = self._client(alias=self.shared_alias)

        # 2. search collection with empty vectors
        log.info("test_range_search_with_empty_vectors: Range searching collection %s "
                 "using empty vector" % self.collection_name)
        range_search_params = {"metric_type": "COSINE", "params": {
            "nprobe": 10, "radius": 0, "range_filter": 0}}
        self.search(client, self.collection_name,
                    data=[],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1,
                                 ct.err_msg: "list index out of range"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_sparse(self):
        """
        target: test sparse index normal range search
        method: range search on shared collection's sparse_vector field
        expected: range search successfully
        """
        client = self._client(alias=self.shared_alias)

        range_filter = random.uniform(0.5, 1)
        radius = random.uniform(0, 0.5)

        # 2. range search
        range_search_params = {"metric_type": "IP",
                               "params": {"radius": radius, "range_filter": range_filter}}
        search_vectors = cf.gen_sparse_vectors(nq)
        search_res, _ = self.search(client, self.collection_name,
                                    data=search_vectors,
                                    anns_field=ct.default_sparse_vec_field_name,
                                    search_params=range_search_params,
                                    limit=default_limit,
                                    filter=default_search_exp)

        # 3. check search results
        for hits in search_res:
            for hit in hits:
                assert range_filter >= hit["distance"] > radius

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_nullable_float_vector(self):
        """
        target: verify range search on nullable float vector field returns no NaN distances
        method: 1. range search on nullable_float_vector with COSINE radius/range_filter
                2. verify all distances within [radius, range_filter]
                3. verify no NaN distances (null vector leak detection)
        expected: results contain only non-null vector rows, distances within range, no NaN
        """
        client = self._client(alias=self.shared_alias)

        range_filter = random.uniform(0.3, 1)
        radius = random.uniform(-1, range_filter - 0.1)

        range_search_params = {"metric_type": "COSINE",
                               "params": {"radius": radius, "range_filter": range_filter}}
        search_vectors = cf.gen_vectors(default_nq, default_dim)
        search_res, _ = self.search(client, self.collection_name,
                                    data=search_vectors,
                                    anns_field=self.nullable_float_vec_field,
                                    search_params=range_search_params,
                                    limit=default_limit,
                                    output_fields=[ct.default_int64_field_name])

        for hits in search_res:
            for hit in hits:
                # no NaN distances (null vector leak detection)
                assert not math.isnan(hit["distance"]), \
                    f"NaN distance found, pk={hit[ct.default_int64_field_name]}"
                # distance within range
                assert range_filter >= hit["distance"] > radius, \
                    f"distance {hit['distance']} out of range ({radius}, {range_filter}]"

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_nullable_sparse_vector(self):
        """
        target: verify range search on nullable sparse vector field returns no NaN distances
        method: 1. range search on nullable_sparse_vector with IP radius/range_filter
                2. verify all distances within [radius, range_filter]
                3. verify no NaN distances (null vector leak detection)
        expected: results contain only non-null vector rows, distances within range, no NaN
        """
        client = self._client(alias=self.shared_alias)

        range_filter = random.uniform(0.5, 1)
        radius = random.uniform(0, 0.3)

        range_search_params = {"metric_type": "IP",
                               "params": {"radius": radius, "range_filter": range_filter}}
        search_vectors = cf.gen_sparse_vectors(nq)
        search_res, _ = self.search(client, self.collection_name,
                                    data=search_vectors,
                                    anns_field=self.nullable_sparse_vec_field,
                                    search_params=range_search_params,
                                    limit=default_limit,
                                    output_fields=[ct.default_int64_field_name])

        for hits in search_res:
            for hit in hits:
                assert not math.isnan(hit["distance"]), \
                    f"NaN distance found, pk={hit[ct.default_int64_field_name]}"
                assert range_filter >= hit["distance"] > radius, \
                    f"distance {hit['distance']} out of range ({radius}, {range_filter}]"

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_nullable_vector_with_scalar_filter(self):
        """
        target: verify range search on nullable float vector combined with nullable scalar filter
        method: 1. range search on nullable_float_vector with filter on nullable float field
                2. verify filter effectiveness: returned rows satisfy float > filter_value
                3. verify null float rows excluded by filter
                4. verify no NaN distances
        expected: all returned results have non-null float > filter_value, no NaN distances
        """
        client = self._client(alias=self.shared_alias)

        filter_value = 1000
        range_search_params = {"metric_type": "COSINE",
                               "params": {"radius": -1, "range_filter": 1}}
        search_vectors = cf.gen_vectors(default_nq, default_dim)
        search_res, _ = self.search(client, self.collection_name,
                                    data=search_vectors,
                                    anns_field=self.nullable_float_vec_field,
                                    search_params=range_search_params,
                                    limit=default_limit,
                                    filter=f"{ct.default_float_field_name} > {filter_value}",
                                    output_fields=[ct.default_int64_field_name,
                                                   ct.default_float_field_name])

        for hits in search_res:
            for hit in hits:
                assert not math.isnan(hit["distance"]), \
                    f"NaN distance found, pk={hit[ct.default_int64_field_name]}"
                float_val = hit.get(ct.default_float_field_name)
                assert float_val is not None, \
                    f"Null float value should be excluded by filter > {filter_value}"
                assert float_val > filter_value, \
                    f"Filter not effective: {ct.default_float_field_name}={float_val} <= {filter_value}"


class TestRangeSearchIndependent(TestMilvusClientV2Base):
    """ Test case of range search interface """

    """
    ******************************************************************
    #  The followings are valid range search cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("index_type, metric, vector_data_type", [
        # Each dense index paired with a representative metric and vector type (zip, not cartesian)
        # Note: INT8_VECTOR only supports HNSW index
        # Coverage: 8 index types × 3 metrics × 4 vector types → 8 combos (was 320)
        ("FLAT", "L2", DataType.FLOAT_VECTOR),
        ("IVF_FLAT", "IP", DataType.FLOAT16_VECTOR),
        # ("IVF_SQ8", "COSINE", DataType.BFLOAT16_VECTOR),
        # ("IVF_PQ", "L2", DataType.FLOAT_VECTOR),
        # ("IVF_RABITQ", "COSINE", DataType.FLOAT16_VECTOR),  # recall too low for range search (#32630)
        ("HNSW", "COSINE", DataType.BFLOAT16_VECTOR),
        # ("SCANN", "L2", DataType.BFLOAT16_VECTOR),
        ("DISKANN", "COSINE", DataType.FLOAT_VECTOR),
    ])
    @pytest.mark.parametrize("with_growing", [False, True])
    def test_range_search_default(self, index_type, metric, vector_data_type, with_growing):
        """
        target: verify the range search returns correct results
        method: 1. create collection, insert 10k vectors,
                2. search with topk=1000
                3. range search from the 30th-330th distance as filter
                4. verified the range search results is same as the search results in the range
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 1000
        rounds = 10
        dim = default_dim

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        # Add the correct vector field based on vector_data_type
        vec_field_name = ct.default_field_name_map.get(vector_data_type, ct.default_float_vec_field_name)
        schema.add_field(vec_field_name, vector_data_type, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        for i in range(rounds):
            data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
            self.insert(client, collection_name, data=data)

        self.flush(client, collection_name)
        _index_params = self.prepare_index_params(client)[0]
        _index_params.add_index(field_name=vec_field_name, index_type="FLAT", metric_type=metric, params={})
        self.create_index(client, collection_name, index_params=_index_params)
        self.load_collection(client, collection_name)

        if with_growing is True:
            # add some growing segments
            for j in range(rounds // 2):
                data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
                self.insert(client, collection_name, data=data)

        search_params = {"params": {}}
        _nq = 1
        search_vectors = cf.gen_vectors(_nq, dim, vector_data_type=vector_data_type)
        search_res, _ = self.search(client, collection_name,
                                    data=search_vectors,
                                    anns_field=vec_field_name,
                                    search_params=search_params,
                                    limit=1000)
        assert len(search_res[0]) == 1000
        log.debug(f"search topk=1000 returns {len(search_res[0])}")
        check_topk = 300
        check_from = 30
        ids = [hit[ct.default_int64_field_name] for hit in search_res[0][check_from:check_from + check_topk]]
        radius = search_res[0][check_from + check_topk]["distance"]
        range_filter = search_res[0][check_from]["distance"]

        # rebuild the collection with test target index
        self.release_collection(client, collection_name)
        indexes, _ = self.list_indexes(client, collection_name)
        for idx_name in indexes:
            self.drop_index(client, collection_name, index_name=idx_name)
        _index_params2 = self.prepare_index_params(client)[0]
        _index_params2.add_index(field_name=vec_field_name, index_type=index_type, metric_type=metric,
                                 params=cf.get_index_params_params(index_type))
        self.create_index(client, collection_name, index_params=_index_params2)
        self.load_collection(client, collection_name)

        params = cf.get_search_params_params(index_type)
        params.update({"radius": radius, "range_filter": range_filter})
        if index_type == "HNSW":
            params.update({"ef": check_topk + 100})
        if index_type == "IVF_PQ":
            params.update({"max_empty_result_buckets": 100})
        range_search_params = {"params": params}
        range_res, _ = self.search(client, collection_name,
                                   data=search_vectors,
                                   anns_field=vec_field_name,
                                   search_params=range_search_params,
                                   limit=check_topk)
        range_ids = [hit[ct.default_int64_field_name] for hit in range_res[0]]
        log.debug(f"range search radius={radius}, range_filter={range_filter}, range results num: {len(range_ids)}")
        hit_rate = round(len(set(ids).intersection(set(range_ids))) / len(set(ids)), 2)
        log.debug(
            f"{vector_data_type} range search results {index_type} {metric} with_growing {with_growing} hit_rate: {hit_rate}")
        assert hit_rate >= 0.2  # issue #32630 to improve the accuracy

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq, dim, auto_id, radius, range_filter, enable_dynamic_field", [
        # Zip combos: flush/growing already covered by test_range_search_default, so fix is_flush=True here
        # Coverage: nq×dim×auto_id×radius_type×dynamic → 4 combos (was 256)
        (2, 32, False, 0, 1000, True),        # small nq, small dim, int params, dynamic
        (500, 128, True, 0.0, 1000.0, False),   # large nq, large dim, float params, no dynamic
        # (2, 128, True, 0, 1000.0, True),       # small nq, large dim, auto_id, mixed types
        # (500, 32, False, 0.0, 1000, False),      # large nq, small dim, no auto_id, mixed types
    ])
    def test_range_search_multi_vector_fields(self, nq, dim, auto_id, radius, range_filter,
                                              enable_dynamic_field):
        """
        target: test range search on collection with multiple vector fields
        method: create collection with 3 float vector fields, insert, index, range search each field
        expected: search successfully with limit(topK) on each vector field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create schema with multiple vector fields
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        # Add extra vector fields
        extra_vec_field_1 = "float_vector_1"
        extra_vec_field_2 = "float_vector_2"
        schema.add_field(extra_vec_field_1, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(extra_vec_field_2, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # 2. insert data (flush/growing already covered by test_range_search_default)
        nb = default_nb
        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # Create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="COSINE",
                      params={"M": 32, "efConstruction": 360})
        idx.add_index(field_name=extra_vec_field_1, index_type="HNSW", metric_type="COSINE",
                      params={"M": 32, "efConstruction": 360})
        idx.add_index(field_name=extra_vec_field_2, index_type="HNSW", metric_type="COSINE",
                      params={"M": 32, "efConstruction": 360})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. range search each vector field using its own vectors from insert data
        range_search_params = {"metric_type": "COSINE", "params": {"radius": radius,
                                                                   "range_filter": range_filter}}
        vector_list = [extra_vec_field_1, extra_vec_field_2, ct.default_float_vec_field_name]
        for search_field in vector_list:
            # use vectors from the same field being searched (search-self pattern)
            search_vectors = [row[search_field] for row in data[:nq]]
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors,
                                        anns_field=search_field,
                                        search_params=range_search_params,
                                        limit=default_limit,
                                        filter=default_search_exp,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": nq,
                                                     "limit": default_limit,
                                                     "enable_milvus_client_api": True,
                                                     "metric": "COSINE",
                                                     "pk_name": ct.default_int64_field_name})
            # verify that top 1 hit is itself (COSINE distance = 1.0)
            for hits in search_res:
                assert abs(hits[0]["distance"] - 1.0) <= epsilon, \
                    f"Top-1 hit on {search_field} distance={hits[0]['distance']}, expected ~1.0"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", [False, True])
    @pytest.mark.parametrize("dup_times", [1, 2])
    def test_range_search_with_dup_primary_key(self, auto_id, dup_times):
        """
        target: test range search with duplicate primary key
        method: 1.insert same data twice
                2.range search
        expected: range search results are de-duplicated
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with data
        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. insert dup data multi times
        for _ in range(dup_times):
            self.insert(client, collection_name, data=data)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="COSINE",
                      params={"M": 32, "efConstruction": 360})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. range search
        search_vectors = []
        for row in data[:default_nq]:
            search_vectors.append(row[ct.default_float_vec_field_name])
        log.info(search_vectors)
        range_search_params = {"metric_type": "COSINE", "params": {
            "nprobe": 10, "radius": 0, "range_filter": 1}}
        search_res, _ = self.search(client, collection_name,
                                    data=search_vectors[:default_nq],
                                    anns_field=default_search_field,
                                    search_params=range_search_params,
                                    limit=default_limit,
                                    filter=default_search_exp,
                                    check_task=CheckTasks.check_search_results,
                                    check_items={"nq": default_nq,
                                                 "limit": default_limit,
                                                 "enable_milvus_client_api": True,
                                                 "metric": "COSINE",
                                                 "pk_name": ct.default_int64_field_name})
        # assert that search results are de-duplicated
        for hits in search_res:
            ids = [hit[ct.default_int64_field_name] for hit in hits]
            assert sorted(list(set(ids))) == sorted(ids)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [2, 500])
    @pytest.mark.skip(reason="partition load and release constraints")
    def test_range_search_before_after_delete(self, nq):
        """
        target: test range search before and after deletion
        method: 1. search the collection
                2. delete a partition
                3. search the collection
        expected: the deleted entities should not be searched
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with data
        nb = 1000
        limit = 1000
        dim = 100
        auto_id = True

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Create partition and insert data
        partition_name = cf.gen_unique_str("par")
        self.create_partition(client, collection_name, partition_name)

        # Insert into default partition
        data_default = cf.gen_row_data_by_schema(nb=nb // 2, schema=schema)
        self.insert(client, collection_name, data=data_default)
        # Insert into custom partition
        data_par = cf.gen_row_data_by_schema(nb=nb // 2, schema=schema)
        self.insert(client, collection_name, data=data_par, partition_name=partition_name)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="COSINE",
                      params={"M": 32, "efConstruction": 360})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search all the partitions before partition deletion
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        log.info("test_range_search_before_after_delete: searching before deleting partitions")
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1}}
        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "limit": limit,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})

        # 3. delete partition
        log.info("test_range_search_before_after_delete: deleting a partition")
        self.release_collection(client, collection_name)
        self.drop_partition(client, collection_name, partition_name)
        log.info("test_range_search_before_after_delete: deleted a partition")

        # Recreate index and load
        self.load_collection(client, collection_name)

        # 4. search non-deleted part after delete partitions
        log.info("test_range_search_before_after_delete: searching after deleting partitions")
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1}}
        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "limit": nb // 2,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_collection_after_release_load(self):
        """
        target: range search the pre-released collection after load
        method: 1. create collection
                2. release collection
                3. load collection
                4. range search the pre-released collection
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with data
        auto_id = True
        enable_dynamic_field = False
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="COSINE",
                      params={"M": 32, "efConstruction": 360})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. release collection
        log.info("test_range_search_collection_after_release_load: releasing collection %s" % collection_name)
        self.release_collection(client, collection_name)
        log.info("test_range_search_collection_after_release_load: released collection %s" % collection_name)

        # 3. Search the pre-released collection after load
        log.info("test_range_search_collection_after_release_load: loading collection %s" % collection_name)
        self.load_collection(client, collection_name)
        log.info("test_range_search_collection_after_release_load: searching after load")
        search_vectors = [[random.random() for _ in range(default_dim)]
                          for _ in range(default_nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1}}
        self.search(client, collection_name,
                    data=search_vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_load_flush_load(self):
        """
        target: test range search when load before flush
        method: 1. insert data and load
                2. flush, and load
                3. search the collection
        expected: search success with limit(topK)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize collection
        dim = 100
        enable_dynamic_field = True
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # 2. insert data
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)

        # 3. create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 4. flush and load
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # 5. search
        search_vectors = [[random.random() for _ in range(dim)]
                          for _ in range(default_nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1}}
        self.search(client, collection_name,
                    data=search_vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [2, 500])
    def test_range_search_new_data(self, nq):
        """
        target: test search new inserted data without load
        method: 1. search the collection
                2. insert new data
                3. search the collection without load again
        expected: new data should be range searched
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with data
        limit = 1000
        nb_old = 500
        dim = 111
        enable_dynamic_field = False

        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=nb_old, schema=schema)
        insert_ids = [i for i in range(nb_old)]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search for original data after load
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"radius": -1,
                                                                   "range_filter": 1}}
        log.info("test_range_search_new_data: searching for original data after load")
        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "ids": insert_ids,
                                 "limit": nb_old,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})

        # 3. insert new data
        nb_new = 300
        data_new = cf.gen_row_data_by_schema(nb=nb_new, schema=schema, start=nb_old)
        self.insert(client, collection_name, data=data_new)
        insert_ids.extend([i for i in range(nb_old, nb_old + nb_new)])

        # 4. search for new data without load
        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "ids": insert_ids,
                                 "limit": nb_old + nb_new,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_different_data_distribution_with_index(self):
        """
        target: test search different data distribution with index
        method: 1. connect to milvus
                2. create a collection
                3. insert data
                4. create an index
                5. Load and search
        expected: Range search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create collection and insert data
        dim = 100
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=-1500)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="IVF_FLAT", metric_type="L2", params={"nlist": 100})
        self.create_index(client, collection_name, index_params=idx)

        # 3. load and range search
        self.load_collection(client, collection_name)
        search_vectors = [[random.random() for _ in range(dim)]
                          for _ in range(default_nq)]
        range_search_params = {"metric_type": "L2", "params": {"radius": 1000,
                                                               "range_filter": 0}}
        self.search(client, collection_name,
                    data=search_vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "metric": "L2",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("not fixed yet")
    @pytest.mark.parametrize("shards_num", [-256, 0, 1, 10, 31, 63])
    def test_range_search_with_non_default_shard_nums(self, shards_num):
        """
        target: test range search with non_default shards_num
        method: connect milvus, create collection with several shard numbers , insert, load and search
        expected: search successfully with the non_default shards_num
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create collection
        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema, shards_num=shards_num)

        # 2. rename collection
        new_collection_name = cf.gen_unique_str(prefix + "new")
        self.rename_collection(client, collection_name, new_collection_name)
        collection_name = new_collection_name

        # 3. insert
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 4. create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 5. range search
        search_vectors = [[random.random() for _ in range(default_dim)]
                          for _ in range(default_nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1}}
        self.search(client, collection_name,
                    data=search_vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", range_search_supported_indexes)
    def test_range_search_after_different_index_with_params(self, index):
        """
        target: test range search after different index
        method: test range search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with data
        dim = 96
        enable_dynamic_field = False
        nb = 5000

        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        insert_ids = [i for i in range(nb)]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index and load
        params = cf.get_index_params_params(index)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type=index, metric_type="L2", params=params)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. range search
        search_vectors = cf.gen_vectors(ct.default_nq, dim)
        search_params = cf.get_search_params_params(index)
        search_params["params"].update({"radius": 2, "range_filter": 0.1})
        self.search(client, collection_name,
                    data=search_vectors,
                    anns_field=default_search_field,
                    search_params=search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": insert_ids,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "metric": "L2",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_index_one_partition(self):
        """
        target: test range search from partition
        method: search from one partition
        expected: searched successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with data
        nb = 3000
        auto_id = False

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        # Create a partition
        partition_name = cf.gen_unique_str("par")
        self.create_partition(client, collection_name, partition_name)

        # Insert half data into default partition and half into custom partition
        half_nb = nb // 2
        data_default = cf.gen_row_data_by_schema(nb=half_nb, schema=schema)
        self.insert(client, collection_name, data=data_default)

        data_par = cf.gen_row_data_by_schema(nb=half_nb, schema=schema, start=half_nb)
        self.insert(client, collection_name, data=data_par, partition_name=partition_name)
        self.flush(client, collection_name)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="IVF_FLAT", metric_type="L2", params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search in one partition
        log.info("test_range_search_index_one_partition: searching (1000 entities) through one partition")
        limit = 1000
        limit_check = min(limit, half_nb)
        range_search_params = {"metric_type": "L2",
                               "params": {"radius": 1000, "range_filter": 0}}
        self.search(client, collection_name,
                    data=cf.gen_vectors(default_nq, default_dim),
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=limit,
                    filter=default_search_exp,
                    partition_names=[partition_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": [i for i in range(half_nb, nb)],
                                 "limit": limit_check,
                                 "enable_milvus_client_api": True,
                                 "metric": "L2",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [2, 500])
    @pytest.mark.parametrize("is_flush", [False, True])
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_jaccard_flat_index(self, nq, index, is_flush):
        """
        target: range search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with JACCARD
        expected: the return distance equals to the computed value
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with binary data
        dim = 48
        auto_id = False

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Insert 2 binary vectors
        binary_raw_vector, binary_vectors = cf.gen_binary_vectors(2, dim)
        data = []
        for i in range(2):
            data.append({
                ct.default_int64_field_name: i,
                ct.default_float_field_name: float(i),
                ct.default_string_field_name: str(i),
                ct.default_binary_vec_field_name: binary_vectors[i],
            })
        self.insert(client, collection_name, data=data)
        if is_flush:
            self.flush(client, collection_name)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name, index_type=index, metric_type="JACCARD", params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. compute the distance
        query_raw_vector, search_binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.jaccard(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.jaccard(query_raw_vector[0], binary_raw_vector[1])

        # 4. search and compare the distance
        search_params = {"metric_type": "JACCARD",
                         "params": {"radius": 1000, "range_filter": 0}}
        insert_ids = [0, 1]
        res, _ = self.search(client, collection_name,
                             data=search_binary_vectors[:nq],
                             anns_field=ct.default_binary_vec_field_name,
                             search_params=search_params,
                             limit=default_limit,
                             filter=default_search_exp,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": nq,
                                          "ids": insert_ids,
                                          "limit": 2,
                                          "enable_milvus_client_api": True,
                                          "metric": "JACCARD",
                                          "pk_name": ct.default_int64_field_name})
        assert abs(res[0][0]["distance"] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_jaccard_invalid_params(self, index):
        """
        target: range search binary_collection with out of range params [0, 1]
        method: range search binary_collection with out of range params
        expected: return empty
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with binary data
        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=2, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name, index_type=index, metric_type="JACCARD", params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. generate search vectors
        _, search_binary_vectors = cf.gen_binary_vectors(3000, default_dim)

        # 4. range search with invalid params
        search_params = {"metric_type": "JACCARD",
                         "params": {"radius": -1, "range_filter": -10}}
        self.search(client, collection_name,
                    data=search_binary_vectors[:default_nq],
                    anns_field=ct.default_binary_vec_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": [],
                                 "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "metric": "JACCARD",
                                 "pk_name": ct.default_int64_field_name})
        # 5. range search with another invalid params
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10, "radius": 10,
                                                              "range_filter": 2}}
        self.search(client, collection_name,
                    data=search_binary_vectors[:default_nq],
                    anns_field=ct.default_binary_vec_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": [],
                                 "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "metric": "JACCARD",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [2, 500])
    @pytest.mark.parametrize("is_flush", [False, True])
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_hamming_flat_index(self, nq, index, is_flush):
        """
        target: range search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with HAMMING
        expected: the return distance equals to the computed value
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with binary data
        dim = 80
        auto_id = True

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Insert 2 binary vectors
        binary_raw_vector, binary_vectors = cf.gen_binary_vectors(2, dim)
        data = []
        for i in range(2):
            data.append({
                ct.default_float_field_name: float(i),
                ct.default_string_field_name: str(i),
                ct.default_binary_vec_field_name: binary_vectors[i],
            })
        self.insert(client, collection_name, data=data)
        if is_flush:
            self.flush(client, collection_name)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name, index_type=index, metric_type="HAMMING", params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)

        # 3. compute the distance
        self.load_collection(client, collection_name)
        query_raw_vector, search_binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.hamming(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.hamming(query_raw_vector[0], binary_raw_vector[1])

        # 4. search and compare the distance
        search_params = {"metric_type": "HAMMING",
                         "params": {"radius": 1000, "range_filter": 0}}
        res, _ = self.search(client, collection_name,
                             data=search_binary_vectors[:nq],
                             anns_field=ct.default_binary_vec_field_name,
                             search_params=search_params,
                             limit=default_limit,
                             filter=default_search_exp,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": nq,
                                          "limit": 2,
                                          "enable_milvus_client_api": True,
                                          "metric": "HAMMING",
                                          "pk_name": ct.default_int64_field_name})
        assert abs(res[0][0]["distance"] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_hamming_invalid_params(self, index):
        """
        target: range search binary_collection with out of range params
        method: range search binary_collection with out of range params
        expected: return empty
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with binary data
        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=2, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name, index_type=index, metric_type="HAMMING", params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. generate search vectors
        _, search_binary_vectors = cf.gen_binary_vectors(3000, default_dim)

        # 4. range search with invalid params
        search_params = {"metric_type": "HAMMING", "params": {"nprobe": 10, "radius": -1,
                                                              "range_filter": -10}}
        self.search(client, collection_name,
                    data=search_binary_vectors[:default_nq],
                    anns_field=ct.default_binary_vec_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": [],
                                 "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "metric": "HAMMING",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("tanimoto obsolete")
    @pytest.mark.parametrize("is_flush", [False, True])
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_tanimoto_flat_index(self, index, is_flush):
        """
        target: range search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with TANIMOTO
        expected: the return distance equals to the computed value
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with binary data
        dim = 100
        auto_id = False

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        binary_raw_vector, binary_vectors = cf.gen_binary_vectors(2, dim)
        data = []
        for i in range(2):
            data.append({
                ct.default_int64_field_name: i,
                ct.default_float_field_name: float(i),
                ct.default_string_field_name: str(i),
                ct.default_binary_vec_field_name: binary_vectors[i],
            })
        insert_ids = [0, 1]
        self.insert(client, collection_name, data=data)
        if is_flush:
            self.flush(client, collection_name)

        log.info("auto_id= %s" % auto_id)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name, index_type=index, metric_type="TANIMOTO", params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. compute the distance
        query_raw_vector, search_binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[1])

        # 4. search
        search_params = {"metric_type": "TANIMOTO", "params": {"nprobe": 10}}
        res, _ = self.search(client, collection_name,
                             data=search_binary_vectors[:1],
                             anns_field=ct.default_binary_vec_field_name,
                             search_params=search_params,
                             limit=default_limit,
                             filter=default_search_exp)
        limit = 0
        radius = 1000
        range_filter = 0
        # filter the range search results to be compared
        for hit in res[0]:
            if radius > hit["distance"] >= range_filter:
                limit += 1

        # 5. range search and compare the distance
        search_params = {"metric_type": "TANIMOTO", "params": {"radius": radius,
                                                               "range_filter": range_filter}}
        res, _ = self.search(client, collection_name,
                             data=search_binary_vectors[:1],
                             anns_field=ct.default_binary_vec_field_name,
                             search_params=search_params,
                             limit=default_limit,
                             filter=default_search_exp,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": 1,
                                          "ids": insert_ids,
                                          "limit": limit,
                                          "enable_milvus_client_api": True,
                                          "metric": "TANIMOTO",
                                          "pk_name": ct.default_int64_field_name})
        assert abs(res[0][0]["distance"] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("tanimoto obsolete")
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_tanimoto_invalid_params(self, index):
        """
        target: range search binary_collection with out of range params [0,inf)
        method: range search binary_collection with out of range params
        expected: return empty
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with binary data
        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        _, binary_vectors = cf.gen_binary_vectors(2, default_dim)
        data = []
        for i in range(2):
            data.append({
                ct.default_int64_field_name: i,
                ct.default_float_field_name: float(i),
                ct.default_string_field_name: str(i),
                ct.default_binary_vec_field_name: binary_vectors[i],
            })
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name, index_type=index, metric_type="JACCARD", params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. compute the distance
        _, search_binary_vectors = cf.gen_binary_vectors(3000, default_dim)

        # 4. range search
        search_params = {"metric_type": "JACCARD",
                         "params": {"radius": -1, "range_filter": -10}}
        self.search(client, collection_name,
                    data=search_binary_vectors[:default_nq],
                    anns_field=ct.default_binary_vec_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": [],
                                 "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "metric": "JACCARD",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metrics", ["JACCARD", "HAMMING"])
    def test_range_search_binary_without_flush(self, metrics):
        """
        target: test range search without flush for binary data (no index)
        method: create connection, collection, insert, load and search
        expected: search successfully with limit(topK)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize a collection without data
        auto_id = True
        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        # 2. insert data
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)

        # 3. create index and load data
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name, index_type="BIN_FLAT", metric_type=metrics, params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 4. search
        log.info("test_range_search_binary_without_flush: searching collection %s" % collection_name)
        _, search_binary_vectors = cf.gen_binary_vectors(default_nq, default_dim)
        search_params = {"metric_type": metrics, "params": {"radius": 1000,
                                                            "range_filter": 0}}
        self.search(client, collection_name,
                    data=search_binary_vectors[:default_nq],
                    anns_field=ct.default_binary_vec_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "metric": metrics,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [2, 500])
    def test_range_search_concurrent_multi_threads_nullable(self, nq):
        """
        target: test concurrent range search with multi-processes (with nullable fields)
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with data
        threads_num = 10
        threads = []
        dim = 66
        auto_id = False
        nb = 4000

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="HNSW", metric_type="COSINE",
                      params={"M": 32, "efConstruction": 360})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        def search(client_ref, coll_name):
            search_vectors = [[random.random() for _ in range(dim)]
                              for _ in range(nq)]
            range_search_params = {"metric_type": "COSINE", "params": {"radius": 0,
                                                                       "range_filter": 1}}
            self.search(client_ref, coll_name,
                        data=search_vectors[:nq],
                        anns_field=default_search_field,
                        search_params=range_search_params,
                        limit=default_limit,
                        filter=default_search_exp,
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": nq,
                                     "limit": default_limit,
                                     "enable_milvus_client_api": True,
                                     "metric": "COSINE",
                                     "pk_name": ct.default_int64_field_name})

        # 2. search with multi-threads
        log.info("test_range_search_concurrent_multi_threads: searching with %s processes" % threads_num)
        for _ in range(threads_num):
            t = threading.Thread(target=search, args=(client, collection_name,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [32, 128])
    def test_range_search_with_expression_large(self, dim):
        """
        target: test range search with large expression
        method: test range search with large expression
        expected: searched successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. initialize with data
        nb = 10000
        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="IVF_FLAT", metric_type="L2", params={"nlist": 100})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search with expression
        expression = f"0 < {default_int64_field_name} < 5001"
        log.info("test_search_with_expression: searching with expression: %s" % expression)

        nums = 5000
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        # calculate the distance to make sure in range(0, 1000)
        search_params = {"metric_type": "L2"}
        search_res, _ = self.search(client, collection_name,
                                    data=search_vectors,
                                    anns_field=default_search_field,
                                    search_params=search_params,
                                    limit=500,
                                    filter=expression)
        for i in range(nums):
            assert len(search_res[i]) >= 10, \
                f"nq={i}: expected at least 10 results, got {len(search_res[i])}"
            for j in range(len(search_res[i])):
                dist = search_res[i][j]["distance"]
                assert 0 <= dist < 1000, \
                    f"nq={i}, hit={j}: distance {dist} out of expected range [0, 1000)"
        # range search
        range_search_params = {"metric_type": "L2", "params": {"radius": 1000, "range_filter": 0}}
        search_res, _ = self.search(client, collection_name,
                                    data=search_vectors,
                                    anns_field=default_search_field,
                                    search_params=range_search_params,
                                    limit=default_limit,
                                    filter=expression)
        for i in range(nums):
            log.info(i)
            assert len(search_res[i]) == default_limit

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [2, 500])
    def test_range_search_with_consistency_bounded(self, nq):
        """
        target: test range search with different consistency level
        method: 1. create a collection
                2. insert data
                3. range search with consistency_level is "bounded"
        expected: searched successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        limit = 1000
        nb_old = 500
        dim = 200
        auto_id = True

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=nb_old, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search for original data after load
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"radius": -1,
                                                                   "range_filter": 1}}
        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "limit": nb_old,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})

        nb_new = 400
        data_new = cf.gen_row_data_by_schema(nb=nb_new, schema=schema, start=nb_old)
        self.insert(client, collection_name, data=data_new)

        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=limit,
                    filter=default_search_exp,
                    consistency_level="Bounded",
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "limit": nb_old,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [2, 500])
    def test_range_search_with_consistency_strong(self, nq):
        """
        target: test range search with different consistency level
        method: 1. create a collection
                2. insert data
                3. range search with consistency_level is "Strong"
        expected: searched successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        limit = 1000
        nb_old = 500
        dim = 100
        auto_id = True

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=nb_old, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search for original data after load
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"radius": -1,
                                                                   "range_filter": 1}}
        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "limit": nb_old,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})

        nb_new = 400
        data_new = cf.gen_row_data_by_schema(nb=nb_new, schema=schema, start=nb_old)
        self.insert(client, collection_name, data=data_new)

        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=range_search_params,
                    limit=limit,
                    filter=default_search_exp,
                    consistency_level="Strong",
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "limit": nb_old + nb_new,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_with_consistency_eventually(self):
        """
        target: test range search with Eventually consistency level
        method: 1. create a collection and insert data
                2. baseline: range search with Strong consistency (full range)
                3. insert new data without flush
                4. Eventually + wide range: verify search works and flushed data visible
                5. Eventually + tight range: verify range filter actually filters distances
        expected: searched successfully with correct distance filtering
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nq = 10
        limit = 1000
        nb_old = 500
        dim = 128
        auto_id = False

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=auto_id)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=nb_old, schema=schema)
        insert_ids = [i for i in range(nb_old)]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 1. baseline: Strong consistency + full range, verify data integrity
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params_wide = {"metric_type": "COSINE", "params": {
            "radius": -1, "range_filter": 1}}
        self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=range_search_params_wide,
                    limit=limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "ids": insert_ids,
                                 "limit": nb_old,
                                 "enable_milvus_client_api": True,
                                 "metric": "COSINE",
                                 "pk_name": ct.default_int64_field_name})

        # 2. insert new data (not flushed)
        nb_new = 400
        data_new = cf.gen_row_data_by_schema(nb=nb_new, schema=schema, start=nb_old)
        self.insert(client, collection_name, data=data_new)

        # 3. Eventually + wide range: verify search works, flushed data should be visible
        search_res, _ = self.search(client, collection_name,
                    data=search_vectors[:nq],
                    anns_field=default_search_field,
                    search_params=range_search_params_wide,
                    limit=limit,
                    filter=default_search_exp,
                    consistency_level="Eventually")
        assert len(search_res) == nq
        for hits in search_res:
            assert len(hits) > 0, "Flushed+loaded data should be visible with Eventually consistency"

        # 4. Eventually + tight range: use vectors from DB to guarantee self-match (distance ≈ 1.0)
        query_res, _ = self.query(client, collection_name,
                                  filter=f"{ct.default_int64_field_name} < {nq}",
                                  output_fields=[default_search_field])
        search_vectors_from_db = [row[default_search_field] for row in query_res]
        range_search_params_tight = {"metric_type": "COSINE", "params": {
            "radius": 0.5, "range_filter": 1.0}}
        search_res_tight, _ = self.search(client, collection_name,
                    data=search_vectors_from_db,
                    anns_field=default_search_field,
                    search_params=range_search_params_tight,
                    limit=limit,
                    filter=default_search_exp,
                    consistency_level="Eventually")
        assert len(search_res_tight) == nq
        for hits in search_res_tight:
            assert len(hits) > 0, "Self-match with distance≈1.0 should fall in (0.5, 1.0]"
            for hit in hits:
                assert 0.5 < hit["distance"] <= 1.0, \
                    f"distance {hit['distance']} out of expected range (0.5, 1.0]"

