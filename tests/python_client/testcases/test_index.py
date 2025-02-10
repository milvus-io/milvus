import random
from time import sleep

import numpy as np
import pytest
import copy

from base.client_base import TestcaseBase
from base.index_wrapper import ApiIndexWrapper
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.code_mapping import CollectionErrorMessage as clem
from common.code_mapping import IndexErrorMessage as iem

from utils.util_pymilvus import *
from common.constants import *
from pymilvus.exceptions import MilvusException

prefix = "index"
default_schema = cf.gen_default_collection_schema()
default_field_name = ct.default_float_vec_field_name
default_index_params = ct.default_index
default_autoindex_params = {"index_type": "AUTOINDEX", "metric_type": "COSINE"}
default_sparse_autoindex_params = {"index_type": "AUTOINDEX", "metric_type": "IP"}

# copied from pymilvus
uid = "test_index"
# BUILD_TIMEOUT = 300
field_name = default_float_vec_field_name
binary_field_name = default_binary_vec_field_name
default_string_field_name = ct.default_string_field_name
index_name1 = cf.gen_unique_str("float")
index_name2 = cf.gen_unique_str("varhar")
index_name3 = cf.gen_unique_str("binary")
default_string_index_params = {}
default_binary_schema = cf.gen_default_binary_collection_schema()
default_binary_index_params = ct.default_binary_index
# query = gen_search_vectors_params(field_name, default_entities, default_top_k, 1)
default_ivf_flat_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
default_ip_index_params = {"index_type": "IVF_FLAT", "metric_type": "IP", "params": {"nlist": 64}}
default_nq = ct.default_nq
default_limit = ct.default_limit
default_search_exp = "int64 >= 0"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_search_ip_params = ct.default_search_ip_params
default_search_binary_params = ct.default_search_binary_params
default_nb = ct.default_nb


@pytest.mark.tags(CaseLabel.GPU)
class TestIndexParams(TestcaseBase):
    """ Test case of index interface """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("collection", [None, "coll"])
    def test_index_non_collection(self, collection):
        """
        target: test index with None collection
        method: input none collection object
        expected: raise exception
        """
        self._connect()
        self.index_wrap.init_index(collection, default_field_name, default_index_params, check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 0, ct.err_msg: clem.CollectionType})

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_field_name_not_existed(self):
        """
        target: test index on non_existing field
        method: input field name
        expected: raise exception
        """
        collection_name = cf.gen_unique_str(prefix)

        collection_w = self.init_collection_wrap(name=collection_name)
        fieldname = "non_existing"
        self.index_wrap.init_index(collection_w.collection, fieldname, default_index_params,
                                   check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 999,
                                                ct.err_msg: "cannot create index on non-existed field"})

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("index_type", ["non_exiting_type", 100])
    def test_index_type_invalid(self, index_type):
        """
        target: test index with error index type
        method: input invalid index type
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        index_params = copy.deepcopy(default_index_params)
        index_params["index_type"] = index_type
        if not isinstance(index_params["index_type"], str):
            msg = "must be str"
        else:
            msg = "invalid index type"
        self.index_wrap.init_index(collection_w.collection, default_field_name, index_params,
                                   check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 1100, ct.err_msg: msg})

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_type_not_supported(self):
        """
        target: test index with error index type
        method: input unsupported index type
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        index_params = copy.deepcopy(default_index_params)
        index_params["index_type"] = "IVFFFFFFF"
        self.index_wrap.init_index(collection_w.collection, default_field_name, index_params,
                                   check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 999, ct.err_msg: ""})

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_params_invalid(self, get_invalid_index_params):
        """
        target: test index with error index params
        method: input invalid index params
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        index_params = get_invalid_index_params
        self.index_wrap.init_index(collection_w.collection, default_field_name, index_params,
                                   check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 1, ct.err_msg: ""})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_name", ["_1ndeX", "In_t0"])
    def test_index_naming_rules(self, index_name):
        """
        target: test index naming rules
        method: 1. connect milvus
                2. Create a collection
                3. Create an index with an index_name which uses all the supported elements in the naming rules
        expected: Index create successfully
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        collection_w.create_index(default_field_name, default_index_params, index_name=index_name)
        assert len(collection_w.indexes) == 1
        assert collection_w.indexes[0].index_name == index_name

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_name", ["_1ndeX", "In_0"])
    def test_index_same_index_name_two_fields(self, index_name):
        """
        target: test index naming rules
        method: 1. connect milvus
                2. Create a collection with more than 3 fields
                3. Create two indexes on two fields with the same index name
        expected: raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params,
                                   index_name=index_name)
        self.index_wrap.init_index(collection_w.collection, ct.default_int64_field_name, default_index_params,
                                   index_name=index_name,
                                   check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 1,
                                                ct.err_msg: "invalid parameter"})

    @pytest.mark.tags(CaseLabel.L1)
    # @pytest.mark.xfail(reason="issue 19181")
    @pytest.mark.parametrize("get_invalid_index_name", ["1nDex", "$in4t", "12 s", "(中文)"])
    def test_index_name_invalid(self, get_invalid_index_name):
        """
        target: test index with error index name
        method: input invalid index name
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        index_name = get_invalid_index_name
        collection_w = self.init_collection_wrap(name=c_name)
        self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params,
                                   index_name=get_invalid_index_name,
                                   check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 1,
                                                ct.err_msg: "Invalid index name"})


@pytest.mark.tags(CaseLabel.GPU)
class TestIndexOperation(TestcaseBase):
    """ Test case of index interface """

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_create_with_different_indexes(self):
        """
        target: test create index on one field, with two different type of index
        method: create two different indexes
        expected: only latest index can be created for a collection
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params)
        error = {ct.err_code: 65535, ct.err_msg: "CreateIndex failed: at most one "
                                                 "distinct index is allowed per field"}
        self.index_wrap.init_index(collection_w.collection, default_field_name, default_ivf_flat_index,
                                   check_task=CheckTasks.err_res, check_items=error)

        assert len(collection_w.indexes) == 1
        assert collection_w.indexes[0].params["index_type"] == default_index_params["index_type"]

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_create_indexes_for_different_fields(self):
        """
        target: Test create indexes for different fields
        method: create two different indexes with default index name
        expected: create successfully, and the default index name equals to field name
        """
        collection_w = self.init_collection_general(prefix, True, nb=200, is_index=False)[0]
        default_index = ct.default_index
        collection_w.create_index(default_field_name, default_index)
        collection_w.create_index(ct.default_int64_field_name, {})
        assert len(collection_w.indexes) == 2
        for index in collection_w.indexes:
            assert index.field_name == index.index_name

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_create_on_scalar_field(self):
        """
        target: Test create index on scalar field
        method: create index on scalar field and load
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, True, nb=200, is_index=False)[0]
        collection_w.create_index(ct.default_int64_field_name, {})
        collection_w.load(check_task=CheckTasks.err_res,
                          check_items={ct.err_code: 65535,
                                       ct.err_msg: f"there is no vector index on field: [float_vector], "
                                                   f"please create index firstly: collection={collection_w.name}: index not found"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index_param", [{}, {"index_type": "INVERTED"}])
    def test_index_create_on_array_field(self, index_param):
        """
        target: Test create index on array field
        method: create index on array field
        expected: raise exception
        """
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)
        collection_w.create_index(ct.default_string_array_field_name, index_param)
        assert collection_w.index()[0].params == index_param

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_collection_empty(self):
        """
        target: test index with empty collection
        method: Index on empty collection
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        index, _ = self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params)
        cf.assert_equal_index(index, collection_w.collection.indexes[0])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_param", [default_index_params])
    def test_index_params(self, index_param):
        """
        target: test index with all index type/params
        method: input valid params
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        index_params = index_param
        index, _ = self.index_wrap.init_index(collection_w.collection, default_field_name, index_params)
        cf.assert_equal_index(index, collection_w.collection.indexes[0])

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_params_flush(self):
        """
        target: test index with all index type/params
        method: input valid params
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        # flush
        collection_w.num_entities
        index, _ = self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params)
        # TODO: assert index
        cf.assert_equal_index(index, collection_w.collection.indexes[0])
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_name_dup(self):
        """
        target: test index with duplicate index name
        method: create index with existed index name and different index params
        expected: raise exception
        create index with the same index name and same index params
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        index_name = ct.default_index_name
        collection_w = self.init_collection_wrap(name=c_name)
        params = cf.get_index_params_params("HNSW")
        index_params = {"index_type": "HNSW", "metric_type": "L2", "params": params}
        params2 = cf.get_index_params_params("HNSW")
        params2.update({"M": 16, "efConstruction": 200})
        index_params2 = {"index_type": "HNSW", "metric_type": "L2", "params": params2}
        collection_w.collection.create_index(default_field_name, index_params, index_name=index_name)

        # create index with the same index name and different index params
        error = {ct.err_code: 999, ct.err_msg: "at most one distinct index is allowed per field"}
        self.index_wrap.init_index(collection_w.collection, default_field_name, index_params2, index_name=index_name,
                                   check_task=CheckTasks.err_res,
                                   check_items=error)
        # create index with the same index name and same index params
        self.index_wrap.init_index(collection_w.collection, default_field_name, index_params)

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_same_name_on_diff_fields(self):
        """
        target: verify index with the same name on different fields is not supported
        method: create index with index name A on fieldA, create index with index name A on fieldB
        expected: raise exception
        """
        # collection_w, _ = self.init_collection_general(prefix, dim=64, insert_data=False, is_index=False,
        #                                                multiple_dim_array=[32])
        id_field = cf.gen_int64_field(name="id", is_primary=True)
        vec_field = cf.gen_float_vec_field(name="vec_field", dim=64)
        vec_field2 = cf.gen_float_vec_field(name="vec_field2", dim=32)
        str_field = cf.gen_string_field(name="str_field")
        str_field2 = cf.gen_string_field(name="str_field2")
        schema, _ = self.collection_schema_wrap.init_collection_schema(
            [id_field, vec_field, vec_field2, str_field, str_field2])
        collection_w = self.init_collection_wrap(schema=schema)
        vec_index = ct.default_index
        vec_index_name = "my_index"

        # create same index name on different vector fields
        error = {ct.err_code: 999, ct.err_msg: "at most one distinct index is allowed per field"}
        collection_w.create_index(vec_field.name, vec_index, index_name=vec_index_name)
        collection_w.create_index(vec_field2.name, vec_index, index_name=vec_index_name,
                                  check_task=CheckTasks.err_res,
                                  check_items=error)

        # create same index name on different scalar fields
        collection_w.create_index(str_field.name, index_name=vec_index_name,
                                  check_task=CheckTasks.err_res,
                                  check_items=error)

        # create same salar index nae on different scalar fields
        index_name = "scalar_index"
        collection_w.create_index(str_field.name, index_name=index_name)
        collection_w.create_index(str_field2.name, index_name=index_name,
                                  check_task=CheckTasks.err_res,
                                  check_items=error)
        all_indexes = collection_w.indexes
        assert len(all_indexes) == 2
        assert all_indexes[0].index_name != all_indexes[1].index_name
        for index in all_indexes:
            assert index.index_name in [vec_index_name, index_name]

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_drop_index(self):
        """
        target: test index.drop
        method: create index by `index`, and then drop it
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        index, _ = self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params)
        cf.assert_equal_index(index, collection_w.collection.indexes[0])
        self.index_wrap.drop()
        assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_drop_repeatedly(self):
        """
        target: test index.drop
        method: create index by `index`, and then drop it twice
        expected: exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        _, _ = self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params)
        self.index_wrap.drop()
        self.index_wrap.drop()


@pytest.mark.tags(CaseLabel.GPU)
class TestIndexAdvanced(TestcaseBase):
    """ Test case of index interface """

    @pytest.mark.tags(CaseLabel.L2)
    def test_index_drop_multi_collections(self):
        """
        target: test index.drop
        method: create indexes by `index`, and then drop it, assert there is one index left
        expected: exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        c_name_2 = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        cw2 = self.init_collection_wrap(name=c_name_2)
        iw_2 = ApiIndexWrapper()
        self.index_wrap.init_index(cw.collection, default_field_name, default_index_params)
        index_2, _ = iw_2.init_index(cw2.collection, default_field_name, default_index_params)
        self.index_wrap.drop()
        assert cf.assert_equal_index(index_2, cw2.collection.indexes[0])
        assert len(cw.collection.indexes) == 0


@pytest.mark.tags(CaseLabel.GPU)
class TestNewIndexBase(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request):
        log.info(request.param)
        return copy.deepcopy(request.param)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_new(self, get_simple_index):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, shards_num=1)
        data = cf.gen_default_list_data(nb=5000)
        collection_w.insert(data=data)
        log.debug(collection_w.num_entities)
        if get_simple_index["index_type"] != "FLAT":
            collection_w.create_index(ct.default_float_vec_field_name, get_simple_index,
                                      index_name=ct.default_index_name)
            assert len(collection_w.indexes) == 1

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="The scenario in this case is not existed for each RPC is limited to 64 MB")
    def test_annoy_index(self):
        # The strange thing is that the indexnode crash is only reproduced when nb is 50000 and dim is 512
        nb = 50000
        dim = 512

        fields = [cf.gen_int64_field(), cf.gen_float_vec_field(dim=dim)]
        schema = cf.gen_collection_schema(fields, primary_field=ct.default_int64_field_name)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(), schema=schema)

        # use python random to generate the data as usual doesn't reproduce
        data = [[i for i in range(nb)], np.random.random([nb, dim]).tolist()]
        collection_w.insert(data)
        log.debug(collection_w.num_entities)

        index_params = {"index_type": "ANNOY", "metric_type": "IP", "params": {"n_trees": 10}}
        index_wrapper = ApiIndexWrapper()
        index, _ = index_wrapper.init_index(collection_w.collection, ct.default_float_vec_field_name, index_params)
        assert index.params == index_params

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_non_existed_field(self):
        """
        target: test create index interface
        method: create collection and add entities in it, create index on other field
        expected: error raised
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        collection_w.create_index(ct.default_int8_field_name, default_index_params,
                                  index_name=ct.default_index_name,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 999,
                                               ct.err_msg: "cannot create index on non-existed field: int8"}
                                  )

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_partition(self):
        """
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        """
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]
        data = cf.gen_default_list_data()
        ins_res, _ = partition_w.insert(data)
        assert len(ins_res.primary_keys) == len(data[0])
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_partition_flush(self):
        """
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        """
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]
        data = cf.gen_default_list_data(default_nb)
        partition_w.insert(data)
        assert collection_w.num_entities == default_nb
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_without_connect(self):
        """
        target: test create index without connection
        method: create collection and add entities in it, check if added successfully
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_all_indexes_params,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 999, ct.err_msg: "should create connection first"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_search_with_query_vectors(self):
        """
        target: test create index interface, search with more query vectors
        method: create collection and add entities in it, create index
        expected: return search success
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(default_nb)
        collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_multithread(self):
        """
        target: test create index interface with multiprocess
        method: create collection and add entities in it, create index
        expected: return search success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)

        def build(collection_w):
            data = cf.gen_default_list_data(default_nb)
            collection_w.insert(data=data)
            collection_w.create_index(ct.default_float_vec_field_name, default_index_params)

        threads_num = 8
        threads = []
        for i in range(threads_num):
            t = MyThread(target=build, args=(collection_w,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_insert_flush(self, get_simple_index):
        """
        target: test create index
        method: create collection and create index, add entities in it
        expected: create index ok, and count correct
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(default_nb)
        collection_w.insert(data=data)
        assert collection_w.num_entities == default_nb
        collection_w.create_index(ct.default_float_vec_field_name, get_simple_index)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_same_index_repeatedly(self):
        """
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: return code success, and search ok
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(default_nb)
        collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        assert len(collection_w.indexes) == 1

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_different_name(self):
        """
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: raise error
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(default_nb)
        collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params, index_name="a")
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params, index_name="b",
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 999,
                                               ct.err_msg: "CreateIndex failed: creating multiple indexes on same field is not supported"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_repeatedly_new(self):
        """
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: drop index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        index_prams = [default_ivf_flat_index,
                       {"metric_type": "L2", "index_type": "IVF_SQ8", "params": {"nlist": 1024}}]
        for index in index_prams:
            index_name = cf.gen_unique_str("name")
            collection_w.create_index(default_float_vec_field_name, index, index_name=index_name)
            collection_w.load()
            collection_w.release()
            collection_w.drop_index(index_name=index_name)
        assert len(collection_w.collection.indexes) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_ip(self):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(default_nb)
        collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, default_ip_index_params)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_partition_ip(self):
        """
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        """
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]
        data = cf.gen_default_list_data(default_nb)
        ins_res, _ = partition_w.insert(data)
        assert len(ins_res.primary_keys) == len(data[0])
        collection_w.create_index(ct.default_float_vec_field_name, default_ip_index_params)

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_partition_flush_ip(self):
        """
        target: test create index
        method: create collection and create index, add entities in it
        expected: create index ok, and count correct
        """
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]
        data = cf.gen_default_list_data(default_nb)
        partition_w.insert(data)
        assert collection_w.num_entities == default_nb
        collection_w.create_index(ct.default_float_vec_field_name, default_ip_index_params)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_search_with_query_vectors_ip(self):
        """
        target: test create index interface, search with more query vectors
        method: create collection and add entities in it, create index
        expected: return search success
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(default_nb)
        collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, default_ip_index_params)
        collection_w.load()
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_ip_params, default_limit,
                            default_search_exp)

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_multithread_ip(self):
        """
        target: test create index interface with multiprocess
        method: create collection and add entities in it, create index
        expected: return success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)

        def build(collection_w):
            data = cf.gen_default_list_data(default_nb)
            collection_w.insert(data=data)
            collection_w.create_index(ct.default_float_vec_field_name, default_ip_index_params)

        threads_num = 8
        threads = []
        for i in range(threads_num):
            t = MyThread(target=build, args=(collection_w,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_no_vectors_insert_ip(self):
        """
        target: test create index interface when there is no vectors in collection,
                and does not affect the subsequent process
        method: create collection and add no vectors in it, and then create index,
                add entities in it
        expected: return insert suceess
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.create_index(ct.default_float_vec_field_name, default_ip_index_params)
        collection_w.insert(data=data)
        assert collection_w.num_entities == default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_same_index_repeatedly_ip(self):
        """
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: return code success, and search ok
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(default_nb)
        collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, default_ip_index_params)
        collection_w.create_index(ct.default_float_vec_field_name, default_ip_index_params)
        assert len(collection_w.indexes) == 1

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_different_index_repeatedly_ip(self):
        """
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: drop index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        index_prams = [default_ip_index_params,
                       {"metric_type": "IP", "index_type": "IVF_SQ8", "params": {"nlist": 1024}}]
        for index in index_prams:
            index_name = cf.gen_unique_str("name")
            collection_w.create_index(default_float_vec_field_name, index, index_name=index_name)
            collection_w.release()
            collection_w.drop_index(index_name=index_name)
        assert len(collection_w.collection.indexes) == 0

    """
       ******************************************************************
         The following cases are used to test `drop_index` function
       ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_index(self, get_simple_index):
        """
        target: test drop index interface
        method: create collection and add entities in it, create index, call drop index
        expected: return code 0, and default index param
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        if get_simple_index["index_type"] != "FLAT":
            collection_w.create_index(ct.default_float_vec_field_name, get_simple_index,
                                      index_name=ct.default_index_name)
            assert len(collection_w.indexes) == 1
            collection_w.drop_index(index_name=ct.default_index_name)
            assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index_repeatedly(self, get_simple_index):
        """
        target: test drop index repeatedly
        method: create index, call drop index, and drop again
        expected: return code 0
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        if get_simple_index["index_type"] != "FLAT":
            collection_w.create_index(ct.default_float_vec_field_name, get_simple_index,
                                      index_name=ct.default_index_name)
            assert len(collection_w.indexes) == 1
            collection_w.drop_index(index_name=ct.default_index_name)
            assert len(collection_w.indexes) == 0
            collection_w.drop_index(index_name=ct.default_index_name)
            assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index_without_connect(self):
        """
        target: test drop index without connection
        method: drop index, and check if drop successfully
        expected: raise exception
        """

        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name=ct.default_index_name)
        self.connection_wrap.remove_connection(ct.default_alias)
        collection_w.drop_index(index_name=ct.default_index_name, check_task=CheckTasks.err_res,
                                check_items={ct.err_code: 999, ct.err_msg: "should create connection first."})

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_drop_index_repeatedly(self, get_simple_index):
        """
        target: test create / drop index repeatedly, use the same index params
        method: create index, drop index, four times
        expected: return code 0
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        if get_simple_index["index_type"] != "FLAT":
            for i in range(4):
                collection_w.create_index(ct.default_float_vec_field_name, get_simple_index,
                                          index_name=ct.default_index_name)
                assert len(collection_w.indexes) == 1
                collection_w.drop_index(index_name=ct.default_index_name)
                assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_PQ_without_nbits(self):
        """
        target: test create PQ index
        method: create PQ index without nbits
        expected: create successfully
        """
        PQ_index = {"index_type": "IVF_PQ", "params": {"nlist": 128, "m": 16}, "metric_type": "L2"}
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name)
        collection_w.create_index(ct.default_float_vec_field_name, PQ_index, index_name=ct.default_index_name)
        assert len(collection_w.indexes) == 1

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index_collection_not_create_ip(self):
        """
        target: test drop index interface when index not created
        method: create collection and add entities in it, create index
        expected: return code not equals to 0, drop index failed
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        collection_w.drop_index(index_name=default_field_name)
        assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_collection_with_after_load(self):
        """
        target: Test that index files are not lost after loading
        method: create collection and add entities in it, create index, flush and load
        expected: load and search successfully
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        nums = 5
        tmp_nb = 1000
        for i in range(nums):
            df = cf.gen_default_dataframe_data(nb=tmp_nb, start=i * tmp_nb)
            insert_res, _ = collection_w.insert(df)
            assert collection_w.num_entities == (i + 1) * tmp_nb
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors, default_search_field, default_search_params, default_limit)
        assert len(search_res[0]) == ct.default_limit

    @pytest.mark.tags(CaseLabel.L1)
    def test_turn_off_index_mmap(self):
        """
        target: disabling and re-enabling mmap for index
        method: disabling and re-enabling mmap for index
        expected: search success
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name, schema=default_schema)
        collection_w.insert(cf.gen_default_list_data())
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name=ct.default_index_name)
        collection_w.alter_index(ct.default_index_name, {'mmap.enabled': True})
        assert collection_w.index()[0].params["mmap.enabled"] == 'True'
        collection_w.load()
        collection_w.release()
        collection_w.alter_index(ct.default_index_name, {'mmap.enabled': False})
        collection_w.load()
        assert collection_w.index()[0].params["mmap.enabled"] == 'False'
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp)
        collection_w.release()
        collection_w.alter_index(ct.default_index_name, {'mmap.enabled': True})
        assert collection_w.index()[0].params["mmap.enabled"] == 'True'
        collection_w.load()
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index, params", zip(ct.all_index_types[:6], ct.default_all_indexes_params[:6]))
    def test_drop_mmap_index(self, index, params):
        """
        target: disabling and re-enabling mmap for index
        method: disabling and re-enabling mmap for index
        expected: search success
        """
        self._connect()
        collection_w = self.init_collection_general(prefix, insert_data=True, is_index=False)[0]
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index(field_name, default_index, index_name=f"mmap_index_{index}")
        collection_w.alter_index(f"mmap_index_{index}", {'mmap.enabled': True})
        assert collection_w.index()[0].params["mmap.enabled"] == 'True'
        collection_w.drop_index(index_name=f"mmap_index_{index}")
        collection_w.create_index(field_name, default_index, index_name=f"index_{index}")
        collection_w.load()
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_rebuild_mmap_index(self):
        """
        target: reconstructing an index after an alter index
        method: reconstructing an index after an alter index
        expected: build indexes normally , index contains mmap information
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_general(c_name, insert_data=True, is_index=False)[0]
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name=ct.default_index_name)
        collection_w.set_properties({'mmap.enabled': True})
        pro = collection_w.describe()[0].get("properties")
        assert pro["mmap.enabled"] == 'True'
        collection_w.alter_index(ct.default_index_name, {'mmap.enabled': True})
        assert collection_w.index()[0].params["mmap.enabled"] == 'True'
        collection_w.insert(cf.gen_default_list_data())
        collection_w.flush()

        # check if mmap works after rebuild index
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name=ct.default_index_name)
        assert collection_w.index()[0].params["mmap.enabled"] == 'True'

        collection_w.load()
        collection_w.release()

        # check if mmap works after reloading and rebuilding index.
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name=ct.default_index_name)
        assert collection_w.index()[0].params["mmap.enabled"] == 'True'
        pro = collection_w.describe()[0].get("properties")
        assert pro["mmap.enabled"] == 'True'

        collection_w.load()
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})


@pytest.mark.tags(CaseLabel.GPU)
class TestNewIndexBinary(TestcaseBase):
    """
        ******************************************************************
          The following cases are used to test `create_index` function
        ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_binary_index_on_scalar_field(self):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        collection_w = self.init_collection_general(prefix, True, is_binary=True, is_index=False)[0]
        collection_w.create_index(default_string_field_name, default_string_index_params, index_name=binary_field_name)
        assert collection_w.has_index(index_name=binary_field_name)[0] is True

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_index_partition(self):
        """
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]
        df, _ = cf.gen_default_binary_dataframe_data()
        ins_res, _ = partition_w.insert(df)
        assert len(ins_res.primary_keys) == len(df)
        collection_w.create_index(default_binary_vec_field_name, default_binary_index_params,
                                  index_name=binary_field_name)
        assert collection_w.has_index(index_name=binary_field_name)[0] is True
        assert len(collection_w.indexes) == 1

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_index_search_with_query_vectors(self):
        """
        target: test create index interface, search with more query vectors
        method: create collection and add entities in it, create index
        expected: return search success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data()
        collection_w.insert(data=df)
        collection_w.create_index(default_binary_vec_field_name, default_binary_index_params,
                                  index_name=binary_field_name)
        collection_w.load()
        _, vectors = cf.gen_binary_vectors(default_nq, default_dim)
        collection_w.search(vectors[:default_nq], binary_field_name,
                            default_search_binary_params, default_limit,
                            default_search_exp)

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_invalid_metric_type_binary(self):
        """
        target: test create index interface with invalid metric type
        method: add entities into binary collection, flush, create index with L2 metric type.
        expected: return create_index failure
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        binary_index_params = {'index_type': 'BIN_IVF_FLAT', 'metric_type': 'L2', 'params': {'nlist': 64}}
        collection_w.create_index(default_binary_vec_field_name, binary_index_params,
                                  index_name=binary_field_name, check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1100,
                                               ct.err_msg: "metric type L2 not found or not supported, supported: "
                                                           "[HAMMING JACCARD SUBSTRUCTURE SUPERSTRUCTURE]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric_type", ["L2", "IP", "COSINE", "JACCARD", "HAMMING"])
    def test_create_binary_index_HNSW(self, metric_type):
        """
        target: test create binary index hnsw
        method: create binary index hnsw
        expected: succeed
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        binary_index_params = {'index_type': 'HNSW', "M": '18', "efConstruction": '240', 'metric_type': metric_type}
        collection_w.create_index(default_binary_vec_field_name, binary_index_params,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1100,
                                               ct.err_msg: "HNSW only support float vector data type: invalid "
                                                           "parameter[expected=valid index params][actual=invalid "
                                                           "index params]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric", ct.binary_metrics)
    def test_create_binary_index_all_metrics(self, metric):
        """
        target: test create binary index using all supported metrics
        method: create binary using all supported metrics
        expected: succeed
        """
        collection_w = self.init_collection_general(prefix, True, is_binary=True, is_index=False)[0]
        binary_index_params = {"index_type": "BIN_FLAT", "metric_type": metric, "params": {"nlist": 64}}
        collection_w.create_index(binary_field_name, binary_index_params)
        assert collection_w.has_index()[0] is True

    """
        ******************************************************************
          The following cases are used to test `drop_index` function
        ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index(self):
        """
        target: test drop index interface
        method: create collection and add entities in it, create index, call drop index
        expected: return code 0, and default index param
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data()
        collection_w.insert(data=df)
        collection_w.create_index(default_binary_vec_field_name, default_binary_index_params,
                                  index_name=binary_field_name)
        assert len(collection_w.indexes) == 1
        collection_w.drop_index(index_name=binary_field_name)
        assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_index_partition(self):
        """
        target: test drop index interface
        method: create collection, create partition and add entities in it,
                create index on collection, call drop collection index
        expected: return code 0, and default index param
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]
        df, _ = cf.gen_default_binary_dataframe_data()
        ins_res, _ = partition_w.insert(df)
        assert len(ins_res.primary_keys) == len(df)
        collection_w.create_index(default_binary_vec_field_name, default_binary_index_params,
                                  index_name=binary_field_name)
        assert collection_w.has_index(index_name=binary_field_name)[0] is True
        assert len(collection_w.indexes) == 1
        collection_w.drop_index(index_name=binary_field_name)
        assert collection_w.has_index(index_name=binary_field_name)[0] is False
        assert len(collection_w.indexes) == 0


@pytest.mark.tags(CaseLabel.GPU)
class TestIndexInvalid(TestcaseBase):
    """
    Test create / describe / drop index interfaces with invalid collection names
    """

    @pytest.fixture(scope="function", params=["Trie", "STL_SORT", "INVERTED"])
    def scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["FLOAT_VECTOR", "FLOAT16_VECTOR", "BFLOAT16_VECTOR"])
    def vector_data_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=ct.invalid_resource_names)
    def invalid_index_name(self, request):
        if request.param in [None, "", " "]:
            pytest.skip("None and empty is valid for there is a default index name")
        yield request.param

    @pytest.mark.tags(CaseLabel.L0)
    def test_index_with_invalid_index_name(self, invalid_index_name):
        """
        target: test create index interface for invalid scenario
        method:
        1. create index with invalid collection name
        expected: raise exception
        2. drop index with an invalid index name
        expected: succeed
        """
        collection_w = self.init_collection_wrap()
        error = {ct.err_code: 999, ct.err_msg: f"Invalid index name: {invalid_index_name}"}
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params, index_name=invalid_index_name,
                                  check_task=CheckTasks.err_res, check_items=error)

        # drop index with an invalid index name
        collection_w.drop_index(index_name=invalid_index_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_index_without_release(self):
        """
        target: test drop index after load without release
        method: 1. create a collection and build an index then load
                2. drop the index
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, True, nb=100, is_index=False)[0]
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w.load()
        collection_w.drop_index(check_task=CheckTasks.err_res,
                                check_items={"err_code": 999,
                                             "err_msg": "index cannot be dropped, collection is "
                                                        "loaded, please release it first"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("n_trees", [-1, 1025, 'a'])
    def test_annoy_index_with_invalid_params(self, n_trees):
        """
        target: test create index with invalid params
        method: 1. set annoy index param n_trees out of range [1, 1024]
                2. set annoy index param n_trees type invalid(not int)
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, True, nb=100, is_index=False)[0]
        index_annoy = {"index_type": "ANNOY", "params": {"n_trees": n_trees}, "metric_type": "L2"}
        collection_w.create_index("float_vector", index_annoy,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 1100,
                                               "err_msg": "invalid index type: ANNOY"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_json(self):
        """
        target: test create index on json fields
        method: 1.create collection, and create index
        expected: create index raise an error
        """
        collection_w = self.init_collection_general(prefix, True, nb=100, is_index=False)[0]
        # create index on JSON/Array field is not supported
        collection_w.create_index(ct.default_json_field_name,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1100,
                                               ct.err_msg: "create index on JSON field is not supported"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_scalar_index_on_vector_field(self, scalar_index, vector_data_type):
        """
        target: test create scalar index on vector field
        method: 1.create collection, and create index
        expected: Raise exception
        """
        collection_w = self.init_collection_general(prefix, True, nb=100,
                                                    is_index=False, vector_data_type=vector_data_type)[0]
        scalar_index_params = {"index_type": scalar_index}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=scalar_index_params,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1100,
                                               ct.err_msg: f"invalid index params"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_scalar_index_on_binary_vector_field(self, scalar_index):
        """
        target: test create scalar index on binary vector field
        method: 1.create collection, and create index
        expected: Raise exception
        """
        collection_w = self.init_collection_general(prefix, is_binary=True, is_index=False)[0]
        scalar_index_params = {"index_type": scalar_index}
        collection_w.create_index(ct.default_binary_vec_field_name, index_params=scalar_index_params,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1100,
                                               ct.err_msg: f"invalid index type: {scalar_index}"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_inverted_index_on_json_field(self, vector_data_type):
        """
        target: test create scalar index on json field
        method: 1.create collection, and create index
        expected: Raise exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False, vector_data_type=vector_data_type)[0]
        scalar_index_params = {"index_type": "INVERTED"}
        collection_w.create_index(ct.default_json_field_name, index_params=scalar_index_params,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1100,
                                               ct.err_msg: "create index on JSON field is not supported"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_inverted_index_on_array_field(self):
        """
        target: test create scalar index on array field
        method: 1.create collection, and create index
        expected: supported create inverted index on array since 2.4.x
        """
        # 1. create a collection
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)
        # 2. create index
        scalar_index_params = {"index_type": "INVERTED"}
        collection_w.create_index(ct.default_int32_array_field_name, index_params=scalar_index_params)
        res, _ = self.utility_wrap.index_building_progress(collection_w.name, ct.default_int32_array_field_name)
        exp_res = {'total_rows': 0, 'indexed_rows': 0, 'pending_index_rows': 0, 'state': 'Finished'}
        assert res == exp_res

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_inverted_index_no_vector_index(self):
        """
        target: test create scalar index on array field
        method: 1.create collection, and create index
        expected: Raise exception
        """
        # 1. create a collection
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        # 2. create index
        scalar_index_params = {"index_type": "INVERTED"}
        collection_w.create_index(ct.default_float_field_name, index_params=scalar_index_params)
        collection_w.load(check_task=CheckTasks.err_res,
                          check_items={ct.err_code: 65535,
                                       ct.err_msg: "there is no vector index on field: [float_vector], "
                                                   "please create index firstly"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("scalar_index", ["STL_SORT", "INVERTED"])
    def test_create_inverted_index_no_all_vector_index(self, scalar_index):
        """
        target: test create scalar index on array field
        method: 1.create collection, and create index
        expected: Raise exception
        """
        # 1. create a collection
        multiple_dim_array = [ct.default_dim, ct.default_dim]
        collection_w = self.init_collection_general(prefix, is_index=False, multiple_dim_array=multiple_dim_array)[0]
        # 2. create index
        scalar_index_params = {"index_type": scalar_index}
        collection_w.create_index(ct.default_float_field_name, index_params=scalar_index_params)
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "L2"}
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load(check_task=CheckTasks.err_res,
                          check_items={ct.err_code: 65535,
                                       ct.err_msg: f"there is no vector index on field: "
                                                   f"[{vector_name_list[0]} {vector_name_list[1]}], "
                                                   f"please create index firstly"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_set_non_exist_index_mmap(self):
        """
        target: enabling mmap for non-existent indexes
        method: enabling mmap for non-existent indexes
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name, schema=default_schema)
        collection_w.insert(cf.gen_default_list_data())
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index,
                                  index_name=ct.default_index_name)
        collection_w.alter_index("random_index_345", {'mmap.enabled': True},
                                 check_task=CheckTasks.err_res,
                                 check_items={ct.err_code: 65535,
                                              ct.err_msg: f"index not found"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_mmap_index(self):
        """
        target: after loading, enable mmap for the index
        method: 1. data preparation and create index
        2. load collection
        3. enable mmap on index
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name, schema=default_schema)
        collection_w.insert(cf.gen_default_list_data())
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index,
                                  index_name=ct.default_index_name)
        collection_w.load()
        collection_w.alter_index(binary_field_name, {'mmap.enabled': True},
                                 check_task=CheckTasks.err_res,
                                 check_items={ct.err_code: 104,
                                              ct.err_msg: f"can't alter index on loaded collection"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_turning_on_mmap_for_scalar_index(self):
        """
        target: turn on mmap for scalar indexes
        method: turn on mmap for scalar indexes
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False, is_all_data_type=True)[0]
        scalar_index = ["Trie", "STL_SORT"]
        scalar_fields = [ct.default_string_field_name, ct.default_int16_field_name]
        for i in range(len(scalar_fields)):
            index_name = f"scalar_index_name_{i}"
            scalar_index_params = {"index_type": f"{scalar_index[i]}"}
            collection_w.create_index(scalar_fields[i], index_params=scalar_index_params, index_name=index_name)
            assert collection_w.has_index(index_name=index_name)[0] is True
            collection_w.alter_index(index_name, {'mmap.enabled': True},
                                     check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 65535,
                                                  ct.err_msg: f"index type {scalar_index[i]} does not support mmap"})
            collection_w.drop_index(index_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_alter_index_invalid(self):
        """
        target: the alter index of the error parameter
        method: the alter index of the error parameter
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name, schema=default_schema)
        collection_w.insert(cf.gen_default_list_data())
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name=ct.default_index_name)
        collection_w.alter_index(ct.default_index_name, {'mmap.enabled': "error_value"},
                                 check_task=CheckTasks.err_res,
                                 check_items={ct.err_code: 65535,
                                              ct.err_msg: f"invalid mmap.enabled value"})
        collection_w.alter_index(ct.default_index_name, {"error_param_key": 123},
                                 check_task=CheckTasks.err_res,
                                 check_items={ct.err_code: 1100,
                                              ct.err_msg: f"error_param is not configable index param"})
        collection_w.alter_index(ct.default_index_name, ["error_param_type"],
                                 check_task=CheckTasks.err_res,
                                 check_items={ct.err_code: 1,
                                              ct.err_msg: f"Unexpected error"})
        collection_w.alter_index(ct.default_index_name, None,
                                 check_task=CheckTasks.err_res,
                                 check_items={ct.err_code: 1,
                                              ct.err_msg: f"extra_params should not be None"})
        collection_w.alter_index(ct.default_index_name, 1000,
                                 check_task=CheckTasks.err_res,
                                 check_items={ct.err_code: 1,
                                              ct.err_msg: f"<'int' object has no attribute 'items'"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric_type", ["L2", "COSINE", "   ", "invalid"])
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_invalid_sparse_metric_type(self, metric_type, index):
        """
        target: unsupported metric_type create index
        method: unsupported metric_type creates an index
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema()
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        data = cf.gen_default_list_sparse_data()
        collection_w.insert(data=data)
        param = cf.get_index_params_params(index)
        params = {"index_type": index, "metric_type": metric_type, "params": param}
        error = {ct.err_code: 65535, ct.err_msg: "only IP is the supported metric type for sparse index"}
        index, _ = self.index_wrap.init_index(collection_w.collection, ct.default_sparse_vec_field_name, params,
                            check_task=CheckTasks.err_res,
                            check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("ratio", [-0.5, 1, 3])
    @pytest.mark.parametrize("index ", ct.all_index_types[9:11])
    def test_invalid_sparse_ratio(self, ratio, index):
        """
        target: index creation for unsupported ratio parameter
        method: indexing of unsupported ratio parameters
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema()
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        data = cf.gen_default_list_sparse_data()
        collection_w.insert(data=data)
        params = {"index_type": index, "metric_type": "IP", "params": {"drop_ratio_build": ratio}}
        error = {ct.err_code: 1100, ct.err_msg: f"invalid drop_ratio_build: {ratio}, must be in range [0, 1): invalid parameter[expected=valid index params"}
        index, _ = self.index_wrap.init_index(collection_w.collection, ct.default_sparse_vec_field_name, params,
                                              check_task=CheckTasks.err_res,
                                              check_items=error)


@pytest.mark.tags(CaseLabel.GPU)
class TestNewIndexAsync(TestcaseBase):
    @pytest.fixture(scope="function", params=[False, True])
    def _async(self, request):
        yield request.param

    def call_back(self):
        assert True

    """
       ******************************************************************
         The following cases are used to test `create_index` function
       ******************************************************************
    """

    # @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, _async):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        res, _ = collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                           index_name=ct.default_index_name, _async=_async)
        if _async:
            res.done()
            assert len(collection_w.indexes) == 1

    @pytest.mark.tags(CaseLabel.L0)
    # @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_drop(self, _async):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        res, _ = collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                           index_name=ct.default_index_name, _async=_async)

        # load and search
        if _async:
            res.done()
            assert len(collection_w.indexes) == 1
        collection_w.load()
        vectors_s = [[random.random() for _ in range(ct.default_dim)] for _ in range(ct.default_nq)]
        search_res, _ = collection_w.search(vectors_s[:ct.default_nq], ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        assert len(search_res) == ct.default_nq
        assert len(search_res[0]) == ct.default_limit

        collection_w.release()
        if _async:
            res.done()
            assert collection_w.indexes[0].params == default_index_params
        collection_w.drop_index(index_name=ct.default_index_name)
        assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L0)
    # @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_callback(self):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        res, _ = collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                           index_name=ct.default_index_name, _async=True,
                                           _callback=self.call_back())


@pytest.mark.tags(CaseLabel.GPU)
class TestIndexString(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test create index about string
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_with_string_field(self):
        """
        target: test create index with string field is not primary
        method: 1.create collection and insert data
                2.only create an index with string field is not primary
        expected: create index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        index, _ = self.index_wrap.init_index(collection_w.collection, default_string_field_name,
                                              default_string_index_params)
        cf.assert_equal_index(index, collection_w.indexes[0])

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_with_string_before_load(self):
        """
        target: test create index with string field before load
        method: 1.create collection and insert data
                2.create an index with string field before load
        expected: create index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(ct.default_nb)
        collection_w.insert(data=data)
        index, _ = self.index_wrap.init_index(collection_w.collection, default_string_field_name,
                                              default_string_index_params)
        cf.assert_equal_index(index, collection_w.indexes[0])
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index,
                                  index_name="vector_flat")
        collection_w.load()
        assert collection_w.num_entities == default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_after_create_index_with_string(self):
        """
        target: test load after create index with string field
        method: 1.create collection and insert data
                2.collection load after create index with string field
        expected: create index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(ct.default_nb)
        collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index,
                                  index_name="vector_flat")
        index, _ = self.index_wrap.init_index(collection_w.collection, default_string_field_name,
                                              default_string_index_params)
        collection_w.load()
        cf.assert_equal_index(index, collection_w.indexes[0])
        assert collection_w.num_entities == default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_with_string_field_is_primary(self):
        """
        target: test create index with string field is primary
        method: 1.create collection
                2.insert data
                3.only create an index with string field is primary
        expected: create index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        index, _ = self.index_wrap.init_index(collection_w.collection, default_string_field_name,
                                              default_string_index_params)
        cf.assert_equal_index(index, collection_w.indexes[0])

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_or_not_with_string_field(self):
        """
        target: test create index, half of the string fields are indexed and half are not
        method: 1.create collection
                2.insert data
                3.half of the indexes are created and half are not in the string fields
        expected: create index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        string_fields = [cf.gen_string_field(name="test_string")]
        schema = cf.gen_schema_multi_string_fields(string_fields)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        df = cf.gen_dataframe_multi_string_fields(string_fields=string_fields)
        collection_w.insert(df)
        self.index_wrap.init_index(collection_w.collection, default_string_field_name, default_string_index_params)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_with_same_index_name(self):
        """
        target: test create index with different fields use same index name
        method: 1.create collection
                2.insert data
                3.only create index with different fields use same index name
        expected: create index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        collection_w.create_index(default_string_field_name, default_string_index_params, index_name=index_name2)
        collection_w.create_index(default_float_vec_field_name, default_index_params,
                                  index_name=index_name2,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1, ct.err_msg: "CreateIndex failed"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_different_index_fields(self):
        """
        target: test create index with different fields
        method: 1.create collection
                2.insert data
                3.create different indexes with string and float vector field
        expected: create index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        collection_w.create_index(default_float_vec_field_name, default_index_params, index_name=index_name1)
        assert collection_w.has_index(index_name=index_name1)[0] == True
        collection_w.create_index(default_string_field_name, default_string_index_params, index_name=index_name2)
        assert collection_w.has_index(index_name=index_name2)[0] == True
        assert len(collection_w.indexes) == 2

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_different_index_binary_fields(self):
        """
        target: testing the creation of indexes with string and binary fields
        method: 1.create collection
                2.insert data
                3.create different indexes with string and binary vector field
        expected: create index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data()
        collection_w.insert(data=df)
        collection_w.create_index(default_string_field_name, default_string_index_params, index_name=index_name2)
        assert collection_w.has_index(index_name=index_name2)[0] == True
        collection_w.create_index(default_binary_vec_field_name, default_binary_index_params, index_name=index_name3)
        assert collection_w.has_index(index_name=index_name3)[0] == True
        assert len(collection_w.indexes) == 2

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_index_with_string_field(self):
        """
        target: test drop index with string field
        method: 1.create collection and insert data
                2.create index and use index.drop() drop index
        expected: drop index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        index, _ = self.index_wrap.init_index(collection_w.collection, default_string_field_name,
                                              default_string_index_params)
        cf.assert_equal_index(index, collection_w.indexes[0])
        self.index_wrap.drop()
        assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_drop_index_with_string(self):
        """
        target: test drop index with string field
        method: 1.create collection and insert data
                2.create index and uses collection.drop_index () drop index
        expected: drop index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        collection_w.create_index(default_string_field_name, default_string_index_params, index_name=index_name2)
        collection_w.drop_index(index_name=index_name2)
        assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_with_string_field_empty(self):
        """
        target: test drop index with string field
        method: 1.create collection and insert data
                2.create index and uses collection.drop_index () drop index
        expected: drop index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)

        nb = 3000
        data = cf.gen_default_list_data(nb)
        data[2] = ["" for _ in range(nb)]
        collection_w.insert(data=data)

        collection_w.create_index(default_string_field_name, default_string_index_params, index_name=index_name2)
        assert collection_w.has_index(index_name=index_name2)[0] == True
        collection_w.drop_index(index_name=index_name2)
        assert len(collection_w.indexes) == 0


@pytest.mark.tags(CaseLabel.GPU)
class TestIndexDiskann(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test create index about diskann
    ******************************************************************
    """

    @pytest.fixture(scope="function", params=[False, True])
    def _async(self, request):
        yield request.param

    def call_back(self):
        assert True

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_with_diskann_normal(self):
        """
        target: test create index with diskann
        method: 1.create collection and insert data
                2.create diskann index , then load data
                3.search successfully
        expected: create index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        assert collection_w.num_entities == default_nb
        index, _ = self.index_wrap.init_index(collection_w.collection, default_float_vec_field_name,
                                              ct.default_diskann_index)
        log.info(self.index_wrap.params)
        cf.assert_equal_index(index, collection_w.indexes[0])
        collection_w.load()
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            ct.default_diskann_search_params, default_limit,
                                            default_search_exp,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [ct.min_dim, ct.max_dim])
    def test_create_index_diskann_with_max_min_dim(self, dim):
        """
        target: test create index with diskann
        method: 1.create collection, when the max dim of the vector is 32768
                2.create diskann index
        expected: create index raise an error
        """
        collection_w = self.init_collection_general(prefix, False, dim=dim, is_index=False)[0]
        collection_w.create_index(default_float_vec_field_name, ct.default_diskann_index)
        assert len(collection_w.indexes) == 1

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_with_diskann_callback(self, _async):
        """
        target: test create index with diskann
        method: 1.create collection and insert data
                2.create diskann index ,then load
                3.search
        expected: create index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        assert collection_w.num_entities == default_nb
        res, _ = collection_w.create_index(ct.default_float_vec_field_name, ct.default_diskann_index,
                                           index_name=ct.default_index_name, _async=_async,
                                           _callback=self.call_back())

        if _async:
            res.done()
            assert len(collection_w.indexes) == 1
        collection_w.load()
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            ct.default_diskann_search_params, default_limit,
                                            default_search_exp,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_diskann_index_drop_with_async(self, _async):
        """
        target: test create index interface
        method: create collection and add entities in it, create diskann index
        expected: return search success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        assert collection_w.num_entities == default_nb
        res, _ = collection_w.create_index(ct.default_float_vec_field_name, ct.default_diskann_index,
                                           index_name=ct.default_index_name, _async=_async)
        if _async:
            res.done()
            assert len(collection_w.indexes) == 1
        collection_w.release()
        collection_w.drop_index(index_name=ct.default_index_name)
        assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_diskann_index_with_partition(self):
        """
        target: test create index with diskann
        method: 1.create collection , partition and insert data to partition
                2.create diskann index ,then load
        expected: create index successfully
        """

        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema)
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]
        df = cf.gen_default_list_data(ct.default_nb)
        ins_res, _ = partition_w.insert(df)
        assert len(ins_res.primary_keys) == ct.default_nb
        collection_w.create_index(default_float_vec_field_name, ct.default_diskann_index,
                                  index_name=field_name)
        collection_w.load()
        assert collection_w.has_index(index_name=field_name)[0] is True
        assert len(collection_w.indexes) == 1
        collection_w.release()
        collection_w.drop_index(index_name=field_name)
        assert collection_w.has_index(index_name=field_name)[0] is False
        assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_diskann_index_with_normal(self):
        """
        target: test drop diskann index normal
        method: 1.create collection and insert data
                2.create index and uses collection.drop_index () drop index
        expected: drop index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        assert collection_w.num_entities == default_nb
        collection_w.create_index(default_float_vec_field_name, ct.default_diskann_index, index_name=index_name1)
        collection_w.load()
        assert len(collection_w.indexes) == 1
        collection_w.release()
        collection_w.drop_index(index_name=index_name1)
        assert collection_w.has_index(index_name=index_name1)[0] is False

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_diskann_index_and_create_again(self):
        """
        target: test drop diskann index normal
        method: 1.create collection and insert data
                2.create index and uses collection.drop_index () drop index
        expected: drop index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        collection_w.create_index(default_field_name, ct.default_diskann_index)
        collection_w.load()
        assert len(collection_w.indexes) == 1
        collection_w.release()
        collection_w.drop_index()
        assert len(collection_w.indexes) == 0
        collection_w.create_index(default_field_name, ct.default_diskann_index)
        assert len(collection_w.indexes) == 1

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_more_than_three_index(self):
        """
        target: test create diskann index
        method: 1.create collection and insert data
                2.create different index
        expected: drop index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data()
        collection_w.insert(data=data)
        assert collection_w.num_entities == default_nb
        collection_w.create_index(default_float_vec_field_name, ct.default_diskann_index, index_name="a")
        assert collection_w.has_index(index_name="a")[0] == True
        collection_w.create_index(default_string_field_name, default_string_index_params, index_name="b")
        assert collection_w.has_index(index_name="b")[0] == True
        default_params = {}
        collection_w.create_index("float", default_params, index_name="c")
        assert collection_w.has_index(index_name="c")[0] == True

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_diskann_index_with_partition(self):
        """
        target: test drop diskann index normal
        method: 1.create collection and insert data
                2.create diskann index and uses collection.drop_index () drop index
        expected: drop index successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema)
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]
        df = cf.gen_default_list_data()
        ins_res, _ = partition_w.insert(df)
        collection_w.create_index(default_float_vec_field_name, ct.default_diskann_index)
        collection_w.load()
        assert len(collection_w.indexes) == 1
        collection_w.release()
        collection_w.drop_index()
        assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_diskann_index_with_binary(self):
        """
        target: test create diskann index with binary
        method: 1.create collection and insert binary data
                2.create diskann index
        expected: report an error
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data()
        collection_w.insert(data=df)
        collection_w.create_index(default_binary_vec_field_name, ct.default_diskann_index, index_name=binary_field_name,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1100,
                                               ct.err_msg: "float or float16 vector are only supported"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_diskann_index_multithread(self):
        """
        target: test create index interface with multiprocess
        method: create collection and add entities in it, create diskann index
        expected: return search success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(default_nb)
        collection_w.insert(data=data)
        assert collection_w.num_entities == default_nb

        def build(collection_w):

            collection_w.create_index(ct.default_float_vec_field_name, ct.default_diskann_index)

        threads_num = 10
        threads = []
        for i in range(threads_num):
            t = MyThread(target=build, args=(collection_w,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    def test_diskann_enable_mmap(self):
        """
        target: enable mmap for unsupported indexes
        method: diskann index enable mmap
        expected: unsupported
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name, schema=default_schema)
        collection_w.insert(cf.gen_default_list_data())
        collection_w.create_index(default_float_vec_field_name, ct.default_diskann_index,
                                  index_name=ct.default_index_name)
        collection_w.set_properties({'mmap.enabled': True})
        desc, _ = collection_w.describe()
        pro = desc.get("properties")
        assert pro["mmap.enabled"] == 'True'
        collection_w.alter_index(ct.default_index_name, {'mmap.enabled': True},
                                 check_task=CheckTasks.err_res,
                                 check_items={ct.err_code: 104,
                                              ct.err_msg: f"index type DISKANN does not support mmap"})


@pytest.mark.tags(CaseLabel.GPU)
class TestAutoIndex(TestcaseBase):
    """ Test case of Auto index """

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_autoindex_with_no_params(self):
        """
        target: test create auto index with no params
        method: create index with only one field name
        expected: create successfully
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(field_name)
        actual_index_params = collection_w.index()[0].params
        assert default_autoindex_params == actual_index_params

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_params", cf.gen_autoindex_params())
    def test_create_autoindex_with_params(self, index_params):
        """
        target: test create auto index with params
        method: create index with different params
        expected: create successfully
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(field_name, index_params)
        actual_index_params = collection_w.index()[0].params
        log.info(collection_w.index()[0].params)
        expect_autoindex_params = copy.copy(default_autoindex_params)
        if index_params.get("index_type"):
            if index_params["index_type"] != 'AUTOINDEX':
                expect_autoindex_params = index_params
        if index_params.get("metric_type"):
            expect_autoindex_params["metric_type"] = index_params["metric_type"]
        assert actual_index_params == expect_autoindex_params

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_autoindex_with_invalid_params(self):
        """
        target: test create auto index with invalid params
        method: create index with invalid params
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        index_params = {"metric_type": "L2", "nlist": "1024", "M": "100"}
        collection_w.create_index(field_name, index_params,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 1,
                                               "err_msg": "only metric type can be "
                                                          "passed when use AutoIndex"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_autoindex_on_binary_vectors(self):
        """
        target: test create auto index on binary vectors
        method: create index on binary vectors
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, is_binary=True, is_index=False)[0]
        collection_w.create_index(binary_field_name, {})
        assert collection_w.index()[0].params == {'index_type': 'AUTOINDEX', 'metric_type': 'HAMMING'}


@pytest.mark.tags(CaseLabel.GPU)
class TestScaNNIndex(TestcaseBase):
    """ Test case of Auto index """

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_scann_index(self):
        """
        target: test create scann index
        method: create index with only one field name
        expected: create successfully
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        index_params = {"index_type": "SCANN", "metric_type": "L2",
                        "params": {"nlist": 1024, "with_raw_data": True}}
        collection_w.create_index(default_field_name, index_params)
        assert collection_w.has_index()[0] is True

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nlist", [0, 65537])
    def test_create_scann_index_nlist_invalid(self, nlist):
        """
        target: test create scann index invalid
        method: create index with invalid nlist
        expected: report error
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        index_params = {"index_type": "SCANN", "metric_type": "L2", "params": {"nlist": nlist}}
        error = {ct.err_code: 1100, ct.err_msg: "nlist out of range: [1, 65536]"}
        collection_w.create_index(default_field_name, index_params,
                                  check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [3, 127])
    def test_create_scann_index_dim_invalid(self, dim):
        """
        target: test create scann index invalid
        method: create index on vector dim % 2 == 1
        expected: report error
        """
        collection_w = self.init_collection_general(prefix, is_index=False, dim=dim)[0]
        index_params = {"index_type": "SCANN", "metric_type": "L2", "params": {"nlist": 1024}}
        error = {ct.err_code: 1100,
                 ct.err_msg: f"dimension must be able to be divided by 2, dimension: {dim}"}
        collection_w.create_index(default_field_name, index_params,
                                  check_task=CheckTasks.err_res, check_items=error)


@pytest.mark.tags(CaseLabel.GPU)
class TestInvertedIndexValid(TestcaseBase):
    """
    Test create / describe / drop index interfaces with inverted index
    """

    @pytest.fixture(scope="function", params=["Trie", "STL_SORT", "INVERTED"])
    def scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["FLOAT_VECTOR", "FLOAT16_VECTOR", "BFLOAT16_VECTOR"])
    def vector_data_type(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("scalar_field_name", [ct.default_int8_field_name, ct.default_int16_field_name,
                                                   ct.default_int32_field_name, ct.default_int64_field_name,
                                                   ct.default_float_field_name, ct.default_double_field_name,
                                                   ct.default_string_field_name, ct.default_bool_field_name])
    def test_create_inverted_index_on_all_supported_scalar_field(self, scalar_field_name):
        """
        target: test create scalar index all supported scalar field
        method: 1.create collection, and create index
        expected: create index successfully
        """
        collection_w = self.init_collection_general(prefix, insert_data=True, is_index=False, is_all_data_type=True)[0]
        scalar_index_params = {"index_type": "INVERTED"}
        index_name = "scalar_index_name"
        collection_w.create_index(scalar_field_name, index_params=scalar_index_params, index_name=index_name)
        assert collection_w.has_index(index_name=index_name)[0] is True
        index_list = self.utility_wrap.list_indexes(collection_w.name)[0]
        assert index_name in index_list
        collection_w.flush()
        result = self.utility_wrap.index_building_progress(collection_w.name, index_name)[0]
        # assert False
        start = time.time()
        while True:
            time.sleep(1)
            res, _ = self.utility_wrap.index_building_progress(collection_w.name, index_name)
            if 0 < res['indexed_rows'] <= default_nb:
                break
            if time.time() - start > 5:
                raise MilvusException(1, f"Index build completed in more than 5s")

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_multiple_inverted_index(self):
        """
        target: test create multiple scalar index
        method: 1.create collection, and create index
        expected: create index successfully
        """
        collection_w = self.init_collection_general(prefix, is_index=False, is_all_data_type=True)[0]
        scalar_index_params = {"index_type": "INVERTED"}
        index_name = "scalar_index_name_0"
        collection_w.create_index(ct.default_int8_field_name, index_params=scalar_index_params, index_name=index_name)
        assert collection_w.has_index(index_name=index_name)[0] is True
        index_name = "scalar_index_name_1"
        collection_w.create_index(ct.default_int32_field_name, index_params=scalar_index_params, index_name=index_name)
        assert collection_w.has_index(index_name=index_name)[0] is True

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_all_inverted_index(self):
        """
        target: test create multiple scalar index
        method: 1.create collection, and create index
        expected: create index successfully
        """
        collection_w = self.init_collection_general(prefix, is_index=False, is_all_data_type=True)[0]
        scalar_index_params = {"index_type": "INVERTED"}
        scalar_fields = [ct.default_int8_field_name, ct.default_int16_field_name,
                         ct.default_int32_field_name, ct.default_int64_field_name,
                         ct.default_float_field_name, ct.default_double_field_name,
                         ct.default_string_field_name, ct.default_bool_field_name]
        for i in range(len(scalar_fields)):
            index_name = f"scalar_index_name_{i}"
            collection_w.create_index(scalar_fields[i], index_params=scalar_index_params, index_name=index_name)
            assert collection_w.has_index(index_name=index_name)[0] is True

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_all_scalar_index(self):
        """
        target: test create multiple scalar index
        method: 1.create collection, and create index
        expected: create index successfully
        """
        collection_w = self.init_collection_general(prefix, is_index=False, is_all_data_type=True)[0]
        scalar_index = ["Trie", "STL_SORT", "INVERTED"]
        scalar_fields = [ct.default_string_field_name, ct.default_int16_field_name,
                         ct.default_int32_field_name]
        for i in range(len(scalar_fields)):
            index_name = f"scalar_index_name_{i}"
            scalar_index_params = {"index_type": f"{scalar_index[i]}"}
            collection_w.create_index(scalar_fields[i], index_params=scalar_index_params, index_name=index_name)
            assert collection_w.has_index(index_name=index_name)[0] is True