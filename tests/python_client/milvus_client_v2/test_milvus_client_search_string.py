from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_base import TestcaseBase
import random

import pytest
import pandas as pd
from faker import Faker

Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")

# patch faker to generate text with specific distribution
cf.patch_faker_text(fake_en, cf.en_vocabularies_distribution)
cf.patch_faker_text(fake_zh, cf.zh_vocabularies_distribution)

pd.set_option("expand_frame_repr", False)

prefix = "search_collection"
search_num = 10
max_dim = ct.max_dim
min_dim = ct.min_dim
epsilon = ct.epsilon
hybrid_search_epsilon = 0.01
gracefulTime = ct.gracefulTime
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
max_limit = ct.max_limit
default_search_exp = "int64 >= 0"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_json_field_name = ct.default_json_field_name
default_index_params = ct.default_index
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
uid = "test_search"
nq = 1
epsilon = 0.001
field_name = default_float_vec_field_name
binary_field_name = default_binary_vec_field_name
search_param = {"nprobe": 1}
entity = gen_entities(1, is_normal=True)
entities = gen_entities(default_nb, is_normal=True)
raw_vectors, binary_entities = gen_binary_entities(default_nb)
default_query, _ = gen_search_vectors_params(field_name, entities, default_top_k, nq)
index_name1 = cf.gen_unique_str("float")
index_name2 = cf.gen_unique_str("varhar")
half_nb = ct.default_nb // 2
max_hybrid_search_req_num = ct.max_hybrid_search_req_num


class TestSearchString(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test search about string
    ******************************************************************
    """

    @pytest.fixture(scope="function",
                    params=[default_nb, default_nb_medium])
    def nb(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[2, 500])
    def nq(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[32, 128])
    def dim(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def _async(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_not_primary(self, _async):
        """
        target: test search with string expr and string field is not primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field, string field is not primary
        expected: Search successfully
        """
        # 1. initialize with data
        auto_id = True
        enable_dynamic_field = False
        collection_w, insert_data, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=default_dim, nb=1000,
                                         enable_dynamic_field=enable_dynamic_field, language="Chinese")[0:4]
        search_str = insert_data[0][default_string_field_name][1]
        search_exp = f"{default_string_field_name} == '{search_str}'"
        # 2. search
        log.info("test_search_string_field_not_primary: searching collection %s" % collection_w.name)
        log.info("search expr: %s" % search_exp)
        output_fields = [default_string_field_name, default_float_field_name]
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     default_search_params, default_limit, search_exp,
                                     output_fields=output_fields,
                                     _async=_async,
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"nq": default_nq,
                                                  "ids": insert_ids,
                                                  "pk_name": default_int64_field_name,
                                                  "limit": 1,
                                                  "_async": _async})
        if _async:
            res.done()
            res = res.result()
        assert res[0][0].entity.varchar == search_str

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_true(self, _async):
        """
        target: test search with string expr and string field is primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field ,string field is primary
        expected: Search successfully
        """
        # 1. initialize with data
        dim = 64
        enable_dynamic_field = True
        collection_w, insert_data, _, insert_ids = \
            self.init_collection_general(prefix, True, dim=dim, primary_field=ct.default_string_field_name,
                                         enable_dynamic_field=enable_dynamic_field, language="English", nb=1000)[0:4]
        search_str = insert_data[0][1][default_string_field_name]
        search_exp = f"{default_string_field_name} == '{search_str}'"
        # 2. search
        log.info("test_search_string_field_is_primary_true: searching collection %s" % collection_w.name)
        log.info("search expr: %s" % search_exp)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     default_search_params, default_limit, search_exp,
                                     output_fields=output_fields,
                                     _async=_async,
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"nq": default_nq,
                                                  "ids": insert_ids,
                                                  "pk_name": default_int64_field_name,
                                                  "limit": 1,
                                                  "_async": _async})
        if _async:
            res.done()
            res = res.result()
        assert res[0][0].entity.varchar == search_str

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_true_multi_vector_fields(self, _async):
        """
        target: test search with string expr and string field is primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field ,string field is primary
        expected: Search successfully
        """
        # 1. initialize with data
        dim = 64
        enable_dynamic_field = False
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, dim=dim, primary_field=ct.default_string_field_name,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array, language="German")[0:4]
        # 2. search
        log.info("test_search_string_field_is_primary_true: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        vector_list = cf.extract_vector_field_name_list(collection_w)
        for search_field in vector_list:
            collection_w.search(vectors[:default_nq], search_field,
                                default_search_params, default_limit,
                                default_search_string_exp,
                                output_fields=output_fields,
                                _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "pk_name": ct.default_string_field_name,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_string_field_is_primary_true(self, _async):
        """
        target: test range search with string expr and string field is primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field ,string field is primary
        expected: Search successfully
        """
        # 1. initialize with data
        dim = 64
        enable_dynamic_field = True
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, dim=dim, primary_field=ct.default_string_field_name,
                                         enable_dynamic_field=enable_dynamic_field, is_index=False,
                                         multiple_dim_array=multiple_dim_array)[0:4]
        vector_list = cf.extract_vector_field_name_list(collection_w)
        collection_w.create_index(field_name, {"metric_type": "L2"})
        for vector_field_name in vector_list:
            collection_w.create_index(vector_field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. search
        log.info("test_search_string_field_is_primary_true: searching collection %s" %
                 collection_w.name)
        range_search_params = {"metric_type": "L2",
                               "params": {"radius": 1000, "range_filter": 0}}
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        for search_field in vector_list:
            collection_w.search(vectors[:default_nq], search_field,
                                range_search_params, default_limit,
                                default_search_string_exp,
                                output_fields=output_fields,
                                _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "pk_name": ct.default_string_field_name,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_mix_expr(self, _async):
        """
        target: test search with mix string and int expr
        method: create collection and insert data
                create index and collection load
                collection search uses mix expr
        expected: Search successfully
        """
        # 1. initialize with data
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        log.info("test_search_string_mix_expr: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_mix_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "pk_name": default_int64_field_name,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_with_invalid_expr(self):
        """
        target: test search data
        method: create collection and insert data
                create index and collection load
                collection search uses invalid string expr
        expected: Raise exception
        """
        # 1. initialize with data
        auto_id = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=default_dim)[0:4]
        # 2. search
        log.info("test_search_string_with_invalid_expr: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_invaild_string_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1100,
                                         "err_msg": "failed to create query plan: cannot "
                                                    "parse expression: varchar >= 0"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expression", cf.gen_normal_string_expressions([ct.default_string_field_name]))
    def test_search_with_different_string_expr(self, expression, _async):
        """
        target: test search with different string expressions
        method: test search with different string expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        dim = 64
        nb = 1000
        enable_dynamic_field = True
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, dim=dim,
                                         is_index=False, enable_dynamic_field=enable_dynamic_field)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        filter_ids = []
        expression = expression.replace("&&", "and").replace("||", "or")
        for i, _id in enumerate(insert_ids):
            if enable_dynamic_field:
                int64 = _vectors[i][ct.default_int64_field_name]
                varchar = _vectors[i][ct.default_string_field_name]
            else:
                int64 = _vectors.int64[i]
                varchar = _vectors.varchar[i]
            if not expression or eval(expression):
                filter_ids.append(_id)

        # 2. create index
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "pk_name": default_int64_field_name,
                                                         "limit": min(nb, len(filter_ids)),
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_binary(self, _async):
        """
        target: test search with string expr and string field is primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field ,string field is primary
        expected: Search successfully
        """
        dim = 64
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids = \
            self.init_collection_general(prefix, True, 2, is_binary=True, dim=dim,
                                         is_index=False, primary_field=ct.default_string_field_name)[0:4]
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. search with exception
        binary_vectors = cf.gen_binary_vectors(3000, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        output_fields = [default_string_field_name]
        collection_w.search(binary_vectors[:default_nq], "binary_vector", search_params,
                            default_limit, default_search_string_exp, output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 2,
                                         "pk_name": ct.default_string_field_name,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_binary(self, _async):
        """
        target: test search with string expr and string field is not primary
        method: create an binary collection and insert data
                create index and collection load
                collection search uses string expr in string field, string field is not primary
        expected: Search successfully
        """
        # 1. initialize with binary data
        dim = 128
        auto_id = True
        collection_w, _, binary_raw_vector, insert_ids = \
            self.init_collection_general(prefix, True, 2, is_binary=True, auto_id=auto_id,
                                         dim=dim, is_index=False)[0:4]
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 2. search with exception
        binary_vectors = cf.gen_binary_vectors(3000, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector", search_params,
                            default_limit, default_search_string_exp,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 2,
                                         "pk_name": default_int64_field_name,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_mix_expr_with_binary(self, _async):
        """
        target: test search with mix string and int expr
        method: create an binary collection and insert data
                create index and collection load
                collection search uses mix expr
        expected: Search successfully
        """
        # 1. initialize with data
        dim = 128
        auto_id = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(
                prefix, True, auto_id=auto_id, dim=dim, is_binary=True, is_index=False)[0:4]
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 2. search
        log.info("test_search_mix_expr_with_binary: searching collection %s" %
                 collection_w.name)
        binary_vectors = cf.gen_binary_vectors(3000, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            default_search_mix_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "pk_name": default_int64_field_name,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_not_primary_prefix(self, _async):
        """
        target: test search with string expr and string field is not primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field, string field is not primary
        expected: Search successfully
        """
        # 1. initialize with data
        auto_id = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(
                prefix, True, auto_id=auto_id, dim=default_dim, is_index=False)[0:4]
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param, index_name="a")
        index_param_two = {}
        collection_w.create_index("varchar", index_param_two, index_name="b")
        collection_w.load()
        # 2. search
        log.info("test_search_string_field_not_primary: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        output_fields = [default_float_field_name, default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            # search all buckets
                            {"metric_type": "L2", "params": {
                                "nprobe": 100}}, default_limit,
                            perfix_expr,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 1,
                                         "pk_name": default_int64_field_name,
                                         "_async": _async}
                            )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_index(self, _async):
        """
        target: test search with string expr and string field is not primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field, string field is not primary
        expected: Search successfully
        """
        # 1. initialize with data
        auto_id = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(
                prefix, True, auto_id=auto_id, dim=default_dim, is_index=False)[0:4]
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param, index_name="a")
        index_param = {"index_type": "Trie", "params": {}}
        collection_w.create_index("varchar", index_param, index_name="b")
        collection_w.load()
        # 2. search
        log.info("test_search_string_field_not_primary: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        output_fields = [default_float_field_name, default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            # search all buckets
                            {"metric_type": "L2", "params": {
                                "nprobe": 100}}, default_limit,
                            perfix_expr,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 1,
                                         "pk_name": default_int64_field_name,
                                         "_async": _async}
                            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_all_index_with_compare_expr(self, _async):
        """
        target: test delete after creating index
        method: 1.create collection , insert data, primary_field is string field
                2.create string and float index ,delete entities, query
                3.search
        expected: assert index and deleted id not in search result
        """
        # create collection, insert tmp_nb, flush and load
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, insert_data=True,
                                                                            primary_field=ct.default_string_field_name,
                                                                            is_index=False)[0:4]

        # create index
        index_params_one = {"index_type": "IVF_SQ8",
                            "metric_type": "COSINE", "params": {"nlist": 64}}
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params_one, index_name=index_name1)
        index_params_two = {}
        collection_w.create_index(
            ct.default_string_field_name, index_params=index_params_two, index_name=index_name2)
        assert collection_w.has_index(index_name=index_name2)

        collection_w.release()
        collection_w.load()
        # delete entity
        expr = 'float >= int64'
        # search with id 0 vectors
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        output_fields = [default_int64_field_name,
                         default_float_field_name, default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            expr,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "pk_name": ct.default_string_field_name,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_insert_empty(self, _async):
        """
        target: test search with string expr and string field is primary
        method: create collection ,string field is primary
                collection load and insert data
                collection search uses string expr in string field
        expected: Search successfully
        """
        # 1. initialize with data
        collection_w, _, _, _ = \
            self.init_collection_general(
                prefix, False, primary_field=ct.default_string_field_name)[0:4]

        nb = 3000
        data = cf.gen_default_list_data(nb)
        data[2] = ["" for _ in range(nb)]
        collection_w.insert(data=data)

        collection_w.load()

        search_string_exp = "varchar >= \"\""
        limit = 1

        # 2. search
        log.info("test_search_string_field_is_primary_true: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit,
                            search_string_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_not_primary_is_empty(self, _async):
        """
        target: test search with string expr and string field is not primary
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field, string field is not primary
        expected: Search successfully
        """
        # 1. initialize with data
        collection_w, _, _, _ = \
            self.init_collection_general(
                prefix, False, primary_field=ct.default_int64_field_name, is_index=False)[0:4]

        nb = 3000
        data = cf.gen_default_list_data(nb)
        insert_ids = data[0]
        data[2] = ["" for _ in range(nb)]

        collection_w.insert(data)
        assert collection_w.num_entities == nb

        # 2. create index
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        search_string_exp = "varchar >= \"\""

        # 3. search
        log.info("test_search_string_field_not_primary: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            search_string_exp,
                            output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "pk_name": default_int64_field_name,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_different_language(self):
        """
        target: test search with string expr using different language
        method: create collection and insert data
                create index and collection load
                collection search uses string expr in string field
        expected: Search successfully
        """
        # 1. initialize with data
        _async = random.choice([True, False])
        auto_id = random.choice([True, False])
        enable_dynamic_field = random.choice([True, False])
        all_language = ["English", "French", "Spanish", "German", "Italian", "Portuguese", "Russian", "Chinese",
                        "Japanese", "Arabic", "Hindi"]
        language = random.choice(all_language)
        log.info(f"_async: {_async}, auto_id: {auto_id}, enable_dynamic_field: {enable_dynamic_field},"
                 f"language: {language}")
        collection_w, insert_data, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, nb=100,
                                         enable_dynamic_field=enable_dynamic_field, language=language)[0:4]
        search_str = insert_data[0][default_string_field_name][1] if not enable_dynamic_field \
            else insert_data[0][1][default_string_field_name]
        search_exp = f"{default_string_field_name} == '{search_str}'"
        # 2. search
        log.info("test_search_string_field_not_primary: searching collection %s" % collection_w.name)
        log.info("search expr: %s" % search_exp)
        output_fields = [default_string_field_name, default_float_field_name]
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     default_search_params, default_limit, search_exp,
                                     output_fields=output_fields,
                                     _async=_async,
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"nq": default_nq,
                                                  "ids": insert_ids,
                                                  "limit": 1,
                                                  "pk_name": default_int64_field_name,
                                                  "_async": _async})
        if _async:
            res.done()
            res = res.result()
        assert res[0][0].entity.varchar == search_str
