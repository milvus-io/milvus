import numpy as np
from pymilvus.orm.types import CONSISTENCY_STRONG, CONSISTENCY_BOUNDED, CONSISTENCY_SESSION, CONSISTENCY_EVENTUALLY
from pymilvus import AnnSearchRequest, RRFRanker, WeightedRanker, Function, FunctionType
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    Collection
)
from common.constants import *
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_base import TestcaseBase
import heapq
from time import sleep
from decimal import Decimal, getcontext
import decimal
import multiprocessing
import numbers
import random
import math
import numpy
import threading
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
binary_field_name = default_binary_vec_field_name
search_param = {"nprobe": 1}
entity = gen_entities(1, is_normal=True)
entities = gen_entities(default_nb, is_normal=True)
raw_vectors, binary_entities = gen_binary_entities(default_nb)
index_name1 = cf.gen_unique_str("float")
index_name2 = cf.gen_unique_str("varhar")
half_nb = ct.default_nb // 2
max_hybrid_search_req_num = ct.max_hybrid_search_req_num



class TestCollectionSearch(TestcaseBase):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[default_nb_medium])
    def nb(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[200])
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

    @pytest.fixture(scope="function", params=["JACCARD", "HAMMING"])
    def metrics(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def is_flush(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["IP", "COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def random_primary_key(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=ct.all_dense_vector_types)
    def vector_data_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["STL_SORT", "INVERTED"])
    def scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[0, 0.5, 1])
    def null_data_percent(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression(self, null_data_percent):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 2000
        dim = 64
        enable_dynamic_field = False
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, dim=dim, is_index=False,
                                         enable_dynamic_field=enable_dynamic_field,
                                         nullable_fields={ct.default_float_field_name: null_data_percent})[0:4]
        # 2. create index
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # filter result with expression in collection
        _vectors = _vectors[0]
        for _async in [False, True]:
            for expressions in cf.gen_normal_expressions_and_templates():
                log.debug(f"test_search_with_expression: {expressions}")
                expr = expressions[0].replace("&&", "and").replace("||", "or")
                filter_ids = []
                for i, _id in enumerate(insert_ids):
                    if enable_dynamic_field:
                        int64 = _vectors[i][ct.default_int64_field_name]
                        float = _vectors[i][ct.default_float_field_name]
                    else:
                        int64 = _vectors.int64[i]
                        float = _vectors.float[i]
                    if float is None and "float <=" in expr:
                        continue
                    if null_data_percent == 1 and "and float" in expr:
                        continue
                    if not expr or eval(expr):
                        filter_ids.append(_id)

                # 3. search with expression
                vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    default_search_params, nb,
                                                    expr=expr, _async=_async,
                                                    check_task=CheckTasks.check_search_results,
                                                    check_items={"nq": default_nq,
                                                                 "ids": insert_ids,
                                                                 "limit": min(nb, len(filter_ids)),
                                                                 "_async": _async})
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                filter_ids_set = set(filter_ids)
                for hits in search_res:
                    ids = hits.ids
                    assert set(ids).issubset(filter_ids_set)

                # 4. search again with expression template
                expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
                expr_params = cf.get_expr_params_from_template(expressions[1])
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    default_search_params, nb,
                                                    expr=expr, expr_params=expr_params, _async=_async,
                                                    check_task=CheckTasks.check_search_results,
                                                    check_items={"nq": default_nq,
                                                                 "ids": insert_ids,
                                                                 "limit": min(nb, len(filter_ids)),
                                                                 "_async": _async})
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                filter_ids_set = set(filter_ids)
                for hits in search_res:
                    ids = hits.ids
                    assert set(ids).issubset(filter_ids_set)

                # 5. search again with expression template and search hints
                search_param = default_search_params.copy()
                search_param.update({"hints": "iterative_filter"})
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    search_param, nb,
                                                    expr=expr, expr_params=expr_params, _async=_async,
                                                    check_task=CheckTasks.check_search_results,
                                                    check_items={"nq": default_nq,
                                                                 "ids": insert_ids,
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
    @pytest.mark.parametrize("bool_type", [True, False, "true", "false"])
    def test_search_with_expression_bool(self, _async, bool_type, null_data_percent):
        """
        target: test search with different bool expressions
        method: search with different bool expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, is_all_data_type=True, auto_id=auto_id,
                                         dim=dim, is_index=False, enable_dynamic_field=enable_dynamic_field,
                                         nullable_fields={ct.default_bool_field_name: null_data_percent})[0:4]
        # 2. create index and load
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, index_param)
        collection_w.load()

        # 3. filter result with expression in collection
        filter_ids = []
        bool_type_cmp = bool_type
        if bool_type == "true":
            bool_type_cmp = True
        if bool_type == "false":
            bool_type_cmp = False
        if enable_dynamic_field:
            for i, _id in enumerate(insert_ids):
                if _vectors[0][i][f"{ct.default_bool_field_name}"] == bool_type_cmp:
                    filter_ids.append(_id)
        else:
            for i in range(len(_vectors[0])):
                if _vectors[0][i].dtype == bool:
                    num = i
                    break
            for i, _id in enumerate(insert_ids):
                if _vectors[0][num][i] == bool_type_cmp:
                    filter_ids.append(_id)

        # 4. search with different expressions
        expression = f"{default_bool_field_name} == {bool_type}"
        log.info("test_search_with_expression_bool: searching with bool expression: %s" % expression)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]

        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
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
    def test_search_with_expression_array(self, null_data_percent):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        enable_dynamic_field = False
        # 1. create a collection
        nb = ct.default_nb
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema, enable_dynamic_field=enable_dynamic_field)

        # 2. insert data
        array_length = 10
        data = []
        for i in range(int(nb * (1 - null_data_percent))):
            arr = {ct.default_int64_field_name: i,
                   ct.default_float_vec_field_name: cf.gen_vectors(1, ct.default_dim)[0],
                   ct.default_int32_array_field_name: [np.int32(i) for i in range(array_length)],
                   ct.default_float_array_field_name: [np.float32(i) for i in range(array_length)],
                   ct.default_string_array_field_name: [str(i) for i in range(array_length)]}
            data.append(arr)
        for i in range(int(nb * (1 - null_data_percent)), nb):
            arr = {ct.default_int64_field_name: i,
                   ct.default_float_vec_field_name: cf.gen_vectors(1, ct.default_dim)[0],
                   ct.default_int32_array_field_name: [np.int32(i) for i in range(array_length)],
                   ct.default_float_array_field_name: [np.float32(i) for i in range(array_length)],
                   ct.default_string_array_field_name: None}
            data.append(arr)
        collection_w.insert(data)

        # 3. create index
        collection_w.create_index("float_vector", ct.default_index)
        collection_w.load()

        # 4. filter result with expression in collection
        for _async in [False, True]:
            for expressions in cf.gen_array_field_expressions_and_templates():
                log.debug(f"search with expression: {expressions} with async={_async}")
                expr = expressions[0].replace("&&", "and").replace("||", "or")
                filter_ids = []
                for i in range(nb):
                    int32_array = data[i][ct.default_int32_array_field_name]
                    float_array = data[i][ct.default_float_array_field_name]
                    string_array = data[i][ct.default_string_array_field_name]
                    if ct.default_string_array_field_name in expr and string_array is None:
                        continue
                    if not expr or eval(expr):
                        filter_ids.append(i)

                # 5. search with expression
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    default_search_params, limit=nb,
                                                    expr=expr, _async=_async)
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                for hits in search_res:
                    ids = hits.ids
                    assert set(ids) == set(filter_ids)

                # 6. search again with expression template
                expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
                expr_params = cf.get_expr_params_from_template(expressions[1])
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    default_search_params, limit=nb,
                                                    expr=expr, expr_params=expr_params,
                                                    _async=_async)
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                for hits in search_res:
                    ids = hits.ids
                    assert set(ids) == set(filter_ids)

                # 7. search again with expression template and hints
                search_params = default_search_params.copy()
                search_params.update({"hints": "iterative_filter"})
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    search_params, limit=nb,
                                                    expr=expr, expr_params=expr_params,
                                                    _async=_async)
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                for hits in search_res:
                    ids = hits.ids
                    assert set(ids) == set(filter_ids)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("exists", ["exists"])
    @pytest.mark.parametrize("json_field_name", ["json_field", "json_field['number']", "json_field['name']",
                                                 "float_array", "not_exist_field", "new_added_field"])
    def test_search_with_expression_exists(self, exists, json_field_name, _async):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        enable_dynamic_field = True
        if not enable_dynamic_field:
            pytest.skip("not allowed")
        # 1. initialize with data
        nb = 100
        schema = cf.gen_array_collection_schema(with_json=True, enable_dynamic_field=enable_dynamic_field)
        collection_w = self.init_collection_wrap(schema=schema, enable_dynamic_field=enable_dynamic_field)
        log.info(schema.fields)
        if enable_dynamic_field:
            data = cf.gen_row_data_by_schema(nb, schema=schema)
            for i in range(nb):
                data[i]["new_added_field"] = i
            log.info(data[0])
        else:
            data = cf.gen_array_dataframe_data(nb, with_json=True)
            log.info(data.head(1))
        collection_w.insert(data)

        # 2. create index
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        expression = exists + " " + json_field_name
        if enable_dynamic_field:
            limit = nb if json_field_name in data[0].keys() else 0
        else:
            limit = nb if json_field_name in data.columns.to_list() else 0
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "limit": limit,
                                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_expression_auto_id(self, _async):
        """
        target: test search with different expressions
        method: test search with different expressions with auto id
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        enable_dynamic_field = True
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, auto_id=True, dim=dim,
                                         is_index=False, enable_dynamic_field=enable_dynamic_field)[0:4]

        # 2. create index
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # filter result with expression in collection
        search_vectors = [[random.random() for _ in range(dim)]
                          for _ in range(default_nq)]
        _vectors = _vectors[0]
        for expressions in cf.gen_normal_expressions_and_templates_field(default_float_field_name):
            log.debug(f"search with expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                if enable_dynamic_field:
                    exec(
                        f"{default_float_field_name} = _vectors[i][f'{default_float_field_name}']")
                else:
                    exec(
                        f"{default_float_field_name} = _vectors.{default_float_field_name}[i]")
                if not expr or eval(expr):
                    filter_ids.append(_id)
            # 3. search expressions
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                default_search_params,
                                                limit=nb, expr=expr,
                                                _async=_async,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids)),
                                                             "_async": _async})
            if _async:
                search_res.done()
                search_res = search_res.result()
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)

            # search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                default_search_params,
                                                limit=nb, expr=expr, expr_params=expr_params,
                                                _async=_async,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids)),
                                                             "_async": _async})
            if _async:
                search_res.done()
                search_res = search_res.result()
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_expr_json_field(self):
        """
        target: test delete entities using normal expression
        method: delete using normal expression
        expected: delete successfully
        """
        # init collection with nb default data
        nb = 2000
        dim = 64
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, nb=nb, dim=dim, enable_dynamic_field=True)[0:4]

        # filter result with expression in collection
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        _vectors = _vectors[0]
        for expressions in cf.gen_json_field_expressions_and_templates():
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            json_field = {}
            for i, _id in enumerate(insert_ids):
                json_field['number'] = _vectors[i][ct.default_json_field_name]['number']
                json_field['float'] = _vectors[i][ct.default_json_field_name]['float']
                if not expr or eval(expr):
                    filter_ids.append(_id)

            # 3. search expressions
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                default_search_params,
                                                limit=nb, expr=expr,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)

            # 4. search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                default_search_params,
                                                limit=nb, expr=expr, expr_params=expr_params,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)

            # 5. search again with expression template and hint
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                search_params,
                                                limit=nb, expr=expr, expr_params=expr_params,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)
            # 6. search again with expression template and hint
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                search_params,
                                                limit=nb, expr=expr, expr_params=expr_params,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)
            # 7. create json index
            default_json_path_index = {"index_type": "INVERTED",
                                       "params": {"json_cast_type": "double",
                                                  "json_path": f"{ct.default_json_field_name}['number']"}}
            collection_w.create_index(ct.default_json_field_name, default_json_path_index,
                                      index_name=f"{ct.default_json_field_name}_0")
            default_json_path_index = {"index_type": "AUTOINDEX",
                                       "params": {"json_cast_type": "double",
                                                  "json_path": f"{ct.default_json_field_name}['float']"}}
            collection_w.create_index(ct.default_json_field_name, default_json_path_index,
                                      index_name=f"{ct.default_json_field_name}_1")
            # 8. release and load to make sure the new index is loaded
            collection_w.release()
            collection_w.load()
            # 9. search expressions after json path index
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                default_search_params,
                                                limit=nb, expr=expr,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)

            # 10. search again with expression template after json path index
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                default_search_params,
                                                limit=nb, expr=expr, expr_params=expr_params,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)

            # 11. search again with expression template and hint after json path index
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                search_params,
                                                limit=nb, expr=expr, expr_params=expr_params,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                # log.info(ids)
                # log.info(filter_ids_set)
                assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_all_data_type(self, nq, _async, null_data_percent):
        """
        target: test search using all supported data types
        method: search using different supported data types
        expected: search success
        """
        # 1. initialize with data
        nb = 3000
        dim = 64
        auto_id = False
        nullable_fields = {ct.default_int32_field_name: null_data_percent,
                           ct.default_int16_field_name: null_data_percent,
                           ct.default_int8_field_name: null_data_percent,
                           ct.default_bool_field_name: null_data_percent,
                           ct.default_float_field_name: null_data_percent,
                           ct.default_double_field_name: null_data_percent,
                           ct.default_string_field_name: null_data_percent}
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, is_all_data_type=True,
                                         auto_id=auto_id, dim=dim, multiple_dim_array=[dim, dim],
                                         nullable_fields=nullable_fields)[0:4]
        # 2. search
        search_exp = "int64 >= 0 && int32 >= 0 && int16 >= 0 " \
                     "&& int8 >= 0 && float >= 0 && double >= 0"
        limit = default_limit
        if null_data_percent == 1:
            limit = 0
            insert_ids = []
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        for vector_field_name in vector_name_list:
            vector_data_type = cf.get_field_dtype_by_field_name(collection_w, vector_field_name)
            vectors = cf.gen_vectors(nq, dim, vector_data_type)
            res = collection_w.search(vectors[:nq], vector_field_name,
                                      default_search_params, default_limit,
                                      search_exp, _async=_async,
                                      output_fields=[default_int64_field_name,
                                                     default_float_field_name,
                                                     default_bool_field_name],
                                      check_task=CheckTasks.check_search_results,
                                      check_items={"nq": nq,
                                                   "ids": insert_ids,
                                                   "limit": limit,
                                                   "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        if limit:
            assert (default_int64_field_name and default_float_field_name
                    and default_bool_field_name) in res[0][0].fields

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field", ct.all_scalar_data_types[:3])
    def test_search_expression_different_data_type(self, field, null_data_percent):
        """
        target: test search expression using different supported data types
        method: search using different supported data types
        expected: search success
        """
        # 1. initialize with data
        field_name = field.name.lower()
        num = int(field_name[3:])
        offset = 2 ** (num - 1)
        nullable_fields = {field_name: null_data_percent}
        default_schema = cf.gen_collection_schema_all_datatype(nullable_fields=nullable_fields)
        collection_w = self.init_collection_wrap(schema=default_schema)
        collection_w = cf.insert_data(collection_w, is_all_data_type=True, insert_offset=offset - 1000,
                                      nullable_fields=nullable_fields)[0]

        # 2. create index and load
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, index_param)
        collection_w.load()

        # 3. search using expression which field value is out of bound
        expression = f"{field_name} >= {offset}"
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, expression, output_fields=[field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq, "limit": 0})
        # 4. search normal using all the scalar type as output fields
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, output_fields=[field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "output_fields": [field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_comparative_expression(self, _async):
        """
        target: test search with expression comparing two fields
        method: create a collection, insert data and search with comparative expression
        expected: search successfully
        """
        # 1. create a collection
        nb = 10
        dim = 2
        fields = [cf.gen_int64_field("int64_1"), cf.gen_int64_field("int64_2"),
                  cf.gen_float_vec_field(dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, primary_field="int64_1")
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str("comparison"), schema=schema)

        # 2. inset data
        values = pd.Series(data=[i for i in range(0, nb)])
        dataframe = pd.DataFrame({"int64_1": values, "int64_2": values,
                                  ct.default_float_vec_field_name: cf.gen_vectors(nb, dim)})
        insert_res = collection_w.insert(dataframe)[0]

        insert_ids = []
        filter_ids = []
        insert_ids.extend(insert_res.primary_keys)
        for _id in enumerate(insert_ids):
            filter_ids.extend(_id)

        # 3. search with expression
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        expression = "int64_1 <= int64_2"
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        res = collection_w.search(vectors[:nq], default_search_field,
                                  default_search_params, default_limit,
                                  expression, _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        filter_ids_set = set(filter_ids)
        for hits in res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_with_double_quotes(self):
        """
        target: test search with expressions with double quotes
        method: test search with expressions with double quotes
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        string_value = [(f"'{cf.gen_str_by_length(3)}'{cf.gen_str_by_length(3)}\""
                         f"{cf.gen_str_by_length(3)}\"") for _ in range(default_nb)]
        data = cf.gen_default_dataframe_data()
        data[default_string_field_name] = string_value
        insert_ids = data[default_int64_field_name]
        collection_w.insert(data)

        # 2. create index
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        _id = random.randint(0, default_nb)
        string_value[_id] = string_value[_id].replace("\"", "\\\"")
        expression = f"{default_string_field_name} == \"{string_value[_id]}\""
        log.debug("test_search_with_expression: searching with expression: %s" % expression)
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, default_limit, expression,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": 1})
        assert search_res[0].ids == [_id]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue 37113")
    def test_search_concurrent_two_collections_nullable(self, nq, _async):
        """
        target: test concurrent load/search with multi-processes between two collections with null data in json field
        method: concurrent load, and concurrent search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        # 1. initialize with data
        nb = 3000
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        threads_num = 10
        threads = []
        collection_w_1, _, _, insert_ids = \
            self.init_collection_general(prefix, False, nb, auto_id=True, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field,
                                         nullable_fields={ct.default_json_field_name: 1})[0:4]
        collection_w_2, _, _, insert_ids = \
            self.init_collection_general(prefix, False, nb, auto_id=True, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field,
                                         nullable_fields={ct.default_json_field_name: 1})[0:4]
        collection_w_1.release()
        collection_w_2.release()
        # insert data
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nb)]
        data = [[np.float32(i) for i in range(default_nb)], [str(i) for i in range(default_nb)], [], vectors]
        collection_w_1.insert(data)
        collection_w_2.insert(data)
        collection_w_1.num_entities
        collection_w_2.num_entities
        collection_w_1.load(_async=True)
        collection_w_2.load(_async=True)
        res = {'loading_progress': '0%'}
        res_1 = {'loading_progress': '0%'}
        while ((res['loading_progress'] != '100%') or (res_1['loading_progress'] != '100%')):
            res = self.utility_wrap.loading_progress(collection_w_1.name)[0]
            log.info("collection %s: loading progress: %s " % (collection_w_1.name, res))
            res_1 = self.utility_wrap.loading_progress(collection_w_2.name)[0]
            log.info("collection %s: loading progress: %s " % (collection_w_1.name, res_1))

        def search(collection_w):
            vectors = [[random.random() for _ in range(dim)]
                       for _ in range(nq)]
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

        # 2. search with multi-processes
        log.info("test_search_concurrent_two_collections_nullable: searching with %s processes" % threads_num)
        for i in range(threads_num):
            t = threading.Thread(target=search, args=(collection_w_1))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.skip(reason="Not running for now")
    @pytest.mark.tags(CaseLabel.L2)
    def test_search_insert_in_parallel(self):
        """
        target: test search and insert in parallel
        method: One process do search while other process do insert
        expected: No exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index(ct.default_float_vec_field_name, default_index)
        collection_w.load()

        def do_insert():
            df = cf.gen_default_dataframe_data(10000)
            for i in range(11):
                collection_w.insert(df)
                log.info(f'Collection num entities is : {collection_w.num_entities}')

        def do_search():
            while True:
                results, _ = collection_w.search(cf.gen_vectors(nq, ct.default_dim), default_search_field,
                                                 default_search_params, default_limit, default_search_exp, timeout=30)
                ids = []
                for res in results:
                    ids.extend(res.ids)
                expr = f'{ct.default_int64_field_name} in {ids}'
                collection_w.query(expr, output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
                                   timeout=30)

        p_insert = multiprocessing.Process(target=do_insert, args=())
        p_search = multiprocessing.Process(target=do_search, args=(), daemon=True)

        p_insert.start()
        p_search.start()

        p_insert.join()

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("round_decimal", [0, 1, 2, 3, 4, 5, 6])
    def test_search_round_decimal(self, round_decimal):
        """
        target: test search with valid round decimal
        method: search with valid round decimal
        expected: search successfully
        """
        import math
        tmp_nb = 500
        tmp_nq = 1
        tmp_limit = 5
        enable_dynamic_field = False
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, nb=tmp_nb,
                                                    enable_dynamic_field=enable_dynamic_field)[0]
        # 2. search
        log.info("test_search_round_decimal: Searching collection %s" % collection_w.name)
        res, _ = collection_w.search(vectors[:tmp_nq], default_search_field,
                                     default_search_params, tmp_limit)

        res_round, _ = collection_w.search(vectors[:tmp_nq], default_search_field,
                                           default_search_params, tmp_limit, round_decimal=round_decimal)

        abs_tol = pow(10, 1 - round_decimal)
        for i in range(tmp_limit):
            dis_expect = round(res[0][i].distance, round_decimal)
            dis_actual = res_round[0][i].distance
            # log.debug(f'actual: {dis_actual}, expect: {dis_expect}')
            # abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)
            assert math.isclose(dis_actual, dis_expect, rel_tol=0, abs_tol=abs_tol)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large(self):
        """
        target: test search with large expression
        method: test search with large expression
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 10000
        dim = 64
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, dim=dim, is_index=False,
                                         enable_dynamic_field=enable_dynamic_field,
                                         with_json=False)[0:4]

        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        expression = f"0 < {default_int64_field_name} < 5001"
        log.info("test_search_with_expression: searching with expression: %s" % expression)

        nums = 5000
        vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        search_res, _ = collection_w.search(vectors, default_search_field,
                                            default_search_params, default_limit, expression,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": nums,
                                                         "ids": insert_ids,
                                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large_two(self):
        """
        target: test search with large expression
        method: test one of the collection ids to another collection search for it, with the large expression
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 10000
        dim = 64
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, dim=dim, is_index=False,
                                         enable_dynamic_field=enable_dynamic_field,
                                         with_json=False)[0:4]

        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        nums = 5000
        vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        vectors_id = [random.randint(0, nums) for _ in range(nums)]
        expression = f"{default_int64_field_name} in {vectors_id}"
        search_res, _ = collection_w.search(vectors, default_search_field,
                                            default_search_params, default_limit, expression,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={
                                                "nq": nums,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                            })

   