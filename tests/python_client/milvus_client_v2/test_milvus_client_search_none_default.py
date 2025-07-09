
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


class TestCollectionSearchNoneAndDefaultData(TestcaseBase):
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
    def numeric_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["TRIE", "INVERTED", "BITMAP"])
    def varchar_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[200, 600])
    def batch_size(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[0, 0.5, 1])
    def null_data_percent(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_normal_none_data(self, nq, dim, auto_id, is_flush, enable_dynamic_field, vector_data_type,
                                     null_data_percent):
        """
        target: test search normal case with none data inserted
        method: create connection, collection with nullable fields, insert data including none, and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field,
                                         vector_data_type=vector_data_type,
                                         nullable_fields={ct.default_float_field_name: null_data_percent})[0:5]
        # 2. generate search data
        vectors = cf.gen_vectors(nq, dim, vector_data_type)
        # 3. search after insert
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=[default_int64_field_name,
                                           default_float_field_name],
                            guarantee_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "pk_name": ct.default_int64_field_name,
                                         "limit": default_limit,
                                         "output_fields": [default_int64_field_name,
                                                           default_float_field_name]})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_after_none_data_all_field_datatype(self, varchar_scalar_index, numeric_scalar_index,
                                                       null_data_percent, _async):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        nullable_fields = {ct.default_int32_field_name: null_data_percent,
                           ct.default_int16_field_name: null_data_percent,
                           ct.default_int8_field_name: null_data_percent,
                           ct.default_bool_field_name: null_data_percent,
                           ct.default_float_field_name: null_data_percent,
                           ct.default_double_field_name: null_data_percent,
                           ct.default_string_field_name: null_data_percent}
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, 5000, partition_num=1,
                                         is_all_data_type=True, dim=default_dim,
                                         is_index=False, nullable_fields=nullable_fields)[0:4]
        # 2. create index on vector field and load
        index = "HNSW"
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 3. create index on scalar field with None data
        scalar_index_params = {"index_type": varchar_scalar_index, "params": {}}
        collection_w.create_index(ct.default_string_field_name, scalar_index_params)
        # 4. create index on scalar field with default data
        scalar_index_params = {"index_type": numeric_scalar_index, "params": {}}
        collection_w.create_index(ct.default_int64_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int32_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int16_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int8_field_name, scalar_index_params)
        collection_w.create_index(ct.default_float_field_name, scalar_index_params)
        scalar_index_params = {"index_type": "INVERTED", "params": {}}
        collection_w.create_index(ct.default_bool_field_name, scalar_index_params)
        collection_w.load()
        # 5. search
        search_params = cf.gen_search_param(index, "COSINE")
        limit = search_params[0]["params"]["ef"]
        log.info("Searching with search params: {}".format(search_params[0]))
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_param, limit, default_search_exp, _async=_async,
                            output_fields=[ct.default_string_field_name, ct.default_float_field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "pk_name": ct.default_int64_field_name,
                                         "_async": _async,
                                         "output_fields": [ct.default_string_field_name,
                                                           ct.default_float_field_name]})

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_default_value_with_insert(self, nq, dim, auto_id, is_flush, enable_dynamic_field, vector_data_type):
        """
        target: test search normal case with default value set
        method: create connection, collection with default value set, insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field,
                                         vector_data_type=vector_data_type,
                                         default_value_fields={ct.default_float_field_name: np.float32(10.0)})[0:5]
        # 2. generate search data
        vectors = cf.gen_vectors(nq, dim, vector_data_type)
        # 3. search after insert
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=[default_int64_field_name,
                                           default_float_field_name],
                            guarantee_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "pk_name": ct.default_int64_field_name,
                                         "limit": default_limit,
                                         "output_fields": [default_int64_field_name,
                                                           default_float_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_default_value_without_insert(self, enable_dynamic_field):
        """
        target: test search normal case with default value set
        method: create connection, collection with default value set, no insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, False, dim=default_dim,
                                                    enable_dynamic_field=enable_dynamic_field,
                                                    nullable_fields={ct.default_float_field_name: 0},
                                                    default_value_fields={
                                                        ct.default_float_field_name: np.float32(10.0)})[0]
        # 2. generate search data
        vectors = cf.gen_vectors(default_nq, default_dim, vector_data_type=DataType.FLOAT_VECTOR)
        # 3. search after insert
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            guarantee_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_after_default_data_all_field_datatype(self, varchar_scalar_index, numeric_scalar_index, _async):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        default_value_fields = {ct.default_int32_field_name: np.int32(1),
                                ct.default_int16_field_name: np.int32(2),
                                ct.default_int8_field_name: np.int32(3),
                                ct.default_bool_field_name: True,
                                ct.default_float_field_name: np.float32(10.0),
                                ct.default_double_field_name: 10.0,
                                ct.default_string_field_name: "1"}
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 5000, partition_num=1,
                                                                      is_all_data_type=True, dim=default_dim,
                                                                      is_index=False,
                                                                      default_value_fields=default_value_fields)[0:4]
        # 2. create index on vector field and load
        index = "HNSW"
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 3. create index on scalar field with None data
        scalar_index_params = {"index_type": varchar_scalar_index, "params": {}}
        collection_w.create_index(ct.default_string_field_name, scalar_index_params)
        # 4. create index on scalar field with default data
        scalar_index_params = {"index_type": numeric_scalar_index, "params": {}}
        collection_w.create_index(ct.default_int64_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int32_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int16_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int8_field_name, scalar_index_params)
        if numeric_scalar_index != "STL_SORT":
            collection_w.create_index(ct.default_bool_field_name, scalar_index_params)
        collection_w.create_index(ct.default_float_field_name, scalar_index_params)
        collection_w.load()
        # 5. search
        search_params = cf.gen_search_param(index, "L2")
        limit = search_params[0]["params"]["ef"]
        log.info("Searching with search params: {}".format(search_params[0]))
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        output_fields = [ct.default_int64_field_name, ct.default_int32_field_name,
                         ct.default_int16_field_name, ct.default_int8_field_name,
                         ct.default_bool_field_name, ct.default_float_field_name,
                         ct.default_double_field_name, ct.default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_param, limit, default_search_exp, _async=_async,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "pk_name": ct.default_int64_field_name,
                                         "limit": limit,
                                         "_async": _async,
                                         "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_both_default_value_non_data(self, nq, dim, auto_id, is_flush, enable_dynamic_field,
                                                vector_data_type):
        """
        target: test search normal case with default value set
        method: create connection, collection with default value set, insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field,
                                         vector_data_type=vector_data_type,
                                         nullable_fields={ct.default_float_field_name: 1},
                                         default_value_fields={ct.default_float_field_name: np.float32(10.0)})[0:5]
        # 2. generate search data
        vectors = cf.gen_vectors(nq, dim, vector_data_type)
        # 3. search after insert
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=[default_int64_field_name,
                                           default_float_field_name],
                            guarantee_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "pk_name": ct.default_int64_field_name,
                                         "limit": default_limit,
                                         "output_fields": [default_int64_field_name,
                                                           default_float_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_collection_with_non_default_data_after_release_load(self, nq, _async, null_data_percent):
        """
        target: search the pre-released collection after load
        method: 1. create collection
                2. release collection
                3. load collection
                4. search the pre-released collection
        expected: search successfully
        """
        # 1. initialize without data
        nb = 2000
        dim = 64
        auto_id = True
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, nb, 1, auto_id=auto_id, dim=dim,
                                         nullable_fields={ct.default_string_field_name: null_data_percent},
                                         default_value_fields={ct.default_float_field_name: np.float32(10.0)})[0:5]
        # 2. release collection
        collection_w.release()
        # 3. Search the pre-released collection after load
        collection_w.load()
        log.info("test_search_collection_awith_non_default_data_after_release_load: searching after load")
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, _async=_async,
                            output_fields=[ct.default_float_field_name, ct.default_string_field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "pk_name": ct.default_int64_field_name,
                                         "limit": default_limit,
                                         "_async": _async,
                                         "output_fields": [ct.default_float_field_name,
                                                           ct.default_string_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.tags(CaseLabel.GPU)
    def test_search_after_different_index_with_params_none_default_data(self, varchar_scalar_index,
                                                                        numeric_scalar_index,
                                                                        null_data_percent, _async):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, 5000, partition_num=1, is_all_data_type=True,
                                         dim=default_dim, is_index=False,
                                         nullable_fields={ct.default_string_field_name: null_data_percent},
                                         default_value_fields={ct.default_float_field_name: np.float32(10.0)})[0:4]
        # 2. create index on vector field and load
        index = "HNSW"
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 3. create index on scalar field with None data
        scalar_index_params = {"index_type": varchar_scalar_index, "params": {}}
        collection_w.create_index(ct.default_string_field_name, scalar_index_params)
        # 4. create index on scalar field with default data
        scalar_index_params = {"index_type": numeric_scalar_index, "params": {}}
        collection_w.create_index(ct.default_float_field_name, scalar_index_params)
        collection_w.load()
        # 5. search
        search_params = cf.gen_search_param(index, "COSINE")
        limit = search_params[0]["params"]["ef"]
        log.info("Searching with search params: {}".format(search_params[0]))
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_param, limit, default_search_exp, _async=_async,
                            output_fields=[ct.default_string_field_name, ct.default_float_field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "pk_name": ct.default_int64_field_name,
                                         "_async": _async,
                                         "output_fields": [ct.default_string_field_name,
                                                           ct.default_float_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_iterator_with_none_data(self, batch_size, null_data_percent):
        """
        target: test search iterator normal
        method: 1. search iterator
                2. check the result, expect pk
        expected: search successfully
        """
        # 1. initialize with data
        dim = 64
        collection_w = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False,
                                         nullable_fields={ct.default_string_field_name: null_data_percent})[0]
        collection_w.create_index(field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. search iterator
        search_params = {"metric_type": "L2"}
        vectors = cf.gen_vectors(1, dim, vector_data_type=DataType.FLOAT_VECTOR)
        collection_w.search_iterator(vectors[:1], field_name, search_params, batch_size,
                                     check_task=CheckTasks.check_search_iterator,
                                     check_items={"batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_none_data_partial_load(self, is_flush, enable_dynamic_field, null_data_percent):
        """
        target: test search normal case with none data inserted
        method: create connection, collection with nullable fields, insert data including none, and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field,
                                         nullable_fields={ct.default_float_field_name: null_data_percent})[0:5]
        # 2. release and partial load again
        collection_w.release()
        loaded_fields = [default_int64_field_name, ct.default_float_vec_field_name]
        if not enable_dynamic_field:
            loaded_fields.append(default_float_field_name)
        collection_w.load(load_fields=loaded_fields)
        # 3. generate search data
        vectors = cf.gen_vectors(default_nq, default_dim)
        # 4. search after partial load field with None data
        output_fields = [default_int64_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "pk_name": ct.default_int64_field_name,
                                         "limit": default_limit,
                                         "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #37547")
    def test_search_none_data_expr_cache(self, is_flush):
        """
        target: test search case with none data to test expr cache
        method: 1. create collection with double datatype as nullable field
                2. search with expr "nullableFid == 0"
                3. drop this collection
                4. create collection with same collection name and same field name but modify the type of nullable field
                   as varchar datatype
                5. search with expr "nullableFid == 0" again
        expected: 1. search successfully with limit(topK) for the first collection
                  2. report error for the second collection with the same name
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, is_flush=is_flush,
                                         nullable_fields={ct.default_float_field_name: 0.5})[0:5]
        collection_name = collection_w.name
        # 2. generate search data
        vectors = cf.gen_vectors(default_nq, default_dim)
        # 3. search with expr "nullableFid == 0"
        search_exp = f"{ct.default_float_field_name} == 0"
        output_fields = [default_int64_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 1,
                                         "pk_name": ct.default_int64_field_name,
                                         "output_fields": output_fields})
        # 4. drop collection
        collection_w.drop()
        # 5. create the same collection name with same field name but varchar field type
        int64_field = cf.gen_int64_field(is_primary=True)
        string_field = cf.gen_string_field(ct.default_float_field_name, nullable=True)
        json_field = cf.gen_json_field()
        float_vector_field = cf.gen_float_vec_field()
        fields = [int64_field, string_field, json_field, float_vector_field]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(name=collection_name, schema=schema)
        int64_values = pd.Series(data=[i for i in range(default_nb)])
        string_values = pd.Series(data=[str(i) for i in range(default_nb)], dtype="string")
        json_values = [{"number": i, "string": str(i), "bool": bool(i),
                        "list": [j for j in range(i, i + ct.default_json_list_length)]} for i in range(default_nb)]
        float_vec_values = cf.gen_vectors(default_nb, default_dim)
        df = pd.DataFrame({
            ct.default_int64_field_name: int64_values,
            ct.default_float_field_name: None,
            ct.default_json_field_name: json_values,
            ct.default_float_vec_field_name: float_vec_values
        })
        collection_w.insert(df)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        collection_w.load()
        collection_w.flush()
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1100,
                                         "err_msg": "failed to create query plan: cannot parse expression: float == 0, "
                                                    "error: comparisons between VarChar and Int64 are not supported: "
                                                    "invalid parameter"})
