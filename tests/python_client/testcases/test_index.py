from time import sleep
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
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "COSINE", "params": {"nlist": 64}}
default_autoindex_params = {"index_type": "AUTOINDEX", "metric_type": "IP"}

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
default_binary_index_params = {"index_type": "BIN_IVF_FLAT", "metric_type": "JACCARD", "params": {"nlist": 64}}
# query = gen_search_vectors_params(field_name, default_entities, default_top_k, 1)
default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
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
    @pytest.mark.parametrize("field_name", ct.get_invalid_strs)
    def test_index_field_name_invalid(self, field_name):
        """
        target: test index with error field name
        method: input field name
        expected: raise exception
        """
        collection_name = cf.gen_unique_str(prefix)

        collection_w = self.init_collection_wrap(name=collection_name)

        log.error(iem.WrongFieldName % str(field_name))
        self.index_wrap.init_index(collection_w.collection, field_name, default_index_params,
                                   check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 1,
                                                ct.err_msg: iem.WrongFieldName % str(field_name)})

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_field_name_not_existed(self):
        """
        target: test index with error field name
        method: input field name not created
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        f_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        self.index_wrap.init_index(collection_w.collection, f_name, default_index_params, check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 1,
                                                ct.err_msg: f"cannot create index on non-existed field: {f_name}"})

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("index_type", ct.get_invalid_strs)
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
            msg = "Invalid index_type"
        self.index_wrap.init_index(collection_w.collection, default_field_name, index_params,
                                   check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 1, ct.err_msg: msg})

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
                                   check_items={ct.err_code: 1, ct.err_msg: ""})

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
                                                ct.err_msg: "CreateIndex failed: index already exist, "
                                                            "but parameters are inconsistent"})

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
        error = {ct.err_code: 1, ct.err_msg: f"CreateIndex failed: index already exists"}
        self.index_wrap.init_index(collection_w.collection, default_field_name, default_index,
                                   check_task=CheckTasks.err_res, check_items=error)

        assert len(collection_w.indexes) == 1
        assert collection_w.indexes[0].params["index_type"] == default_index_params["index_type"]

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_create_indexes_for_different_fields(self):
        """
        target: Test create indexes for different fields
        method: create two different indexes with default index name
        expected: create successfully
        """
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index(default_field_name, default_index)
        collection_w.create_index(ct.default_int64_field_name, {})

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_create_on_scalar_field(self):
        """
        target: Test create index on scalar field
        method: create index on scalar field and load
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        collection_w.create_index(ct.default_int64_field_name, {})
        collection_w.load(check_task=CheckTasks.err_res,
                          check_items={ct.err_code: 1, ct.err_msg: "there is no vector index on collection, "
                                                                   "please create index firstly"})

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
        # TODO: assert index
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
        # TODO: assert index
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

    # TODO: not support
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason='not supported')
    def test_index_name_dup(self):
        """
        target: test index with duplicate index name
        method: create index with existed index name create by `collection.create_index`
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        index_name = ct.default_index_name
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.collection.create_index(default_field_name, default_index_params, index_name=index_name)
        self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params,
                                   check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 1, ct.err_msg: ""})

    # TODO: server not supported
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason='not supported')
    def test_index_field_names(self):
        """
        target: test index on one field, with two indexes
        method: create index with two different indexes
        expected: no exception raised
        """
        pass

    # TODO: server not supported
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason='not supported')
    def test_index_fields(self):
        """
        target: test index on two fields, with the same name
        method: create the same index name with two different fields
        expected: exception raised
        """
        pass

    # TODO: server not supported
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason='not supported')
    def test_index_fields_B(self):
        """
        target: test index on two fields, with the different name
        method: create the different index with two different fields
        expected: no exception raised
        """
        pass

    # TODO: server not supported
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason='not supported')
    def test_index_field_names_eq_maximum(self):
        """
        target: test index on one field, with the different names, num of the names equal to the maximum num supported
        method: create the different indexes
        expected: no exception raised
        """
        pass

    # TODO: server not supported
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason='not supported')
    def test_index_field_names_more_maximum(self):
        """
        target: test index on one field, with the different names, num of the names more than the maximum num supported
        method: create the different indexes
        expected: exception raised
        """
        pass
    
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
    # TODO #7372
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason='TODO')
    def test_index_drop_during_inserting(self):
        """
        target: test index.drop during inserting
        method: create indexes by `index`, and then drop it during inserting entities, make sure async insert
        expected: no exception raised, insert success
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason='TODO')
    def test_index_drop_during_searching(self):
        """
        target: test index.drop during searching
        method: create indexes by `index`, and then drop it during searching, make sure async search
        expected: no exception raised, search success
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason='TODO')
    def test_index_recovery_after_restart(self):
        """
        target: test index still existed after server restart
        method: create index by `index`, and then restart server, assert index existed
        expected: index in collection.indexes
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason='TODO')
    def test_index_building_after_restart(self):
        """
        target: index can still build if not finished before server restart
        method: create index by `index`, and then restart server, assert server is indexing
        expected: index build finished after server restart
        """
        pass

    """
    ******************************************************************
      The following classes are copied from pymilvus test
    ******************************************************************
    """


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
                                  check_items={ct.err_code: 1,
                                               ct.err_msg: "cannot create index on non-existed field: int8"}
                                  )

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_no_vectors(self):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name=ct.default_index_name)

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
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index_params,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1, ct.err_msg: "should create connect first"})

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
                                  check_items={ct.err_code: 1,
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
        index_prams = [default_index, {"metric_type": "L2", "index_type": "IVF_SQ8", "params": {"nlist": 1024}}]
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
    def test_create_index_no_vectors_ip(self):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.create_index(ct.default_float_vec_field_name, default_ip_index_params,
                                  index_name=ct.default_index_name)

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

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_different_name_ip(self):
        """
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: raise error
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(default_nb)
        collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, default_ip_index_params, index_name="a")
        collection_w.create_index(ct.default_float_vec_field_name, default_ip_index_params, index_name="b",
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1,
                                               ct.err_msg: "CreateIndex failed: creating multiple indexes on same field is not supported"})

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
    # TODO #7372
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
                                check_items={ct.err_code: 0, ct.err_msg: "should create connect first."})

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

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index_ip(self, get_simple_index):
        """
        target: test drop index interface
        method: create collection and add entities in it, create index, call drop index
        expected: return code 0, and default index param
        """
        get_simple_index["metric_type"] = "IP"
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        if get_simple_index["index_type"] != "FLAT":
            collection_w.create_index(ct.default_float_vec_field_name, get_simple_index,
                                      index_name=ct.default_index_name)
            assert len(collection_w.indexes) == 1
            collection_w.drop_index(index_name=ct.default_index_name)
            assert len(collection_w.indexes) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index_repeatedly_ip(self, get_simple_index):
        """
        target: test drop index repeatedly
        method: create index, call drop index, and drop again
        expected: return code 0
        """
        get_simple_index["metric_type"] = "IP"
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
    def test_drop_index_without_connect_ip(self):
        """
        target: test drop index without connection
        method: drop index, and check if drop successfully
        expected: raise exception
        """

        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name)
        collection_w.create_index(ct.default_float_vec_field_name, default_ip_index_params,
                                  index_name=ct.default_index_name)
        self.connection_wrap.remove_connection(ct.default_alias)
        collection_w.drop_index(index_name=ct.default_index_name, check_task=CheckTasks.err_res,
                                check_items={ct.err_code: 0, ct.err_msg: "should create connect first."})

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_drop_index_repeatedly_ip(self, get_simple_index):
        """
        target: test create / drop index repeatedly, use the same index params
        method: create index, drop index, four times
        expected: return code 0
        """
        get_simple_index["metric_type"] = "IP"
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name)
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
        nums = 20
        tmp_nb = 5000
        for i in range(nums):
            df = cf.gen_default_dataframe_data(nb=tmp_nb, start=i * tmp_nb)
            insert_res, _ = collection_w.insert(df)
            assert collection_w.num_entities == (i + 1) * tmp_nb
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors, default_search_field, default_search_params, default_limit)
        assert len(search_res[0]) == ct.default_limit


@pytest.mark.tags(CaseLabel.GPU)
class TestNewIndexBinary(TestcaseBase):

    def get_simple_index(self, request):
        log.info(request.param)
        return copy.deepcopy(request.param)

    """
        ******************************************************************
          The following cases are used to test `create_index` function
        ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    # @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data()
        collection_w.insert(data=df)
        collection_w.create_index(default_string_field_name, default_string_index_params, index_name=binary_field_name)
        assert collection_w.has_index(index_name=binary_field_name)[0] == True

    @pytest.mark.tags(CaseLabel.L0)
    # @pytest.mark.timeout(BUILD_TIMEOUT)
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
        assert collection_w.has_index(index_name=binary_field_name)[0] == True
        assert len(collection_w.indexes) == 1

    @pytest.mark.tags(CaseLabel.L0)
    # @pytest.mark.timeout(BUILD_TIMEOUT)
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

    # @pytest.mark.timeout(BUILD_TIMEOUT)
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
                                  check_items={ct.err_code: 1,
                                               ct.err_msg: "Invalid metric_type: L2, which does not match the index type: BIN_IVF_FLAT"})

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
        assert collection_w.has_index(index_name=binary_field_name)[0] == True
        assert len(collection_w.indexes) == 1
        collection_w.drop_index(index_name=binary_field_name)
        assert collection_w.has_index(index_name=binary_field_name)[0] == False
        assert len(collection_w.indexes) == 0


@pytest.mark.tags(CaseLabel.GPU)
class TestIndexInvalid(TestcaseBase):
    """
    Test create / describe / drop index interfaces with invalid collection names
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_index_with_invalid_collection_name(self, connect, get_collection_name):
        """
        target: test create index interface for invalid scenario
        method: create index with invalid collection name
        expected: raise exception
        """
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.create_index(collection_name, field_name, default_index)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index_with_invalid_collection_name(self, connect, get_collection_name):
        """
        target: test drop index interface for invalid scenario
        method: drop index with invalid collection name
        expected: raise exception
        """
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.drop_index(collection_name)

    @pytest.fixture(
        scope="function",
        params=gen_invalid_index()
    )
    def get_index(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_with_invalid_index_params(self, connect, collection, get_index):
        """
        target: test create index interface for invalid scenario
        method: create index with invalid index params
        expected: raise exception
        """
        log.info(get_index)
        with pytest.raises(Exception) as e:
            connect.create_index(collection, field_name, get_index)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_index_without_release(self):
        """
        target: test drop index after load without release
        method: 1. create a collection and build an index then load
                2. drop the index
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        collection_w.drop_index(check_task=CheckTasks.err_res,
                                check_items={"err_code": 1,
                                             "err_msg": "index cannot be dropped, collection is "
                                                        "loaded, please release it first"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("n_trees", [-1, 1025, 'a', {34}])
    def test_annoy_index_with_invalid_params(self, n_trees):
        """
        target: test create index with invalid params
        method: 1. set annoy index param n_trees out of range [1, 1024]
                2. set annoy index param n_trees type invalid(not int)
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        index_annoy = {"index_type": "ANNOY", "params": {"n_trees": n_trees}, "metric_type": "L2"}
        collection_w.create_index("float_vector", index_annoy,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 1,
                                               "err_msg": "invalid index params"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_json(self):
        """
        target: test create index on json fields
        method: 1.create collection, and create index
        expected: create index raise an error
        """
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      dim=ct.default_dim, is_index=False)[0:4]
        collection_w.create_index(ct.default_json_field_name, index_params=ct.default_flat_index,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1,
                                               ct.err_msg: "create index on json field is not supported"})


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
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index, index_name="vector_flat")
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
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index, index_name="vector_flat")
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
        data[2] = [""for _ in range(nb)] 
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
        index, _ = self.index_wrap.init_index(collection_w.collection, default_float_vec_field_name, ct.default_diskann_index)
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
    def test_create_index_with_max_dim(self):
        """
        target: test create index with diskann
        method: 1.create collection, when the max dim of the vector is 32768
                2.create diskann index
        expected: create index raise an error
        """
        dim = 32768
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(), cf.gen_string_field(),
                  cf.gen_float_vec_field(dim=dim)]
        schema = cf.gen_collection_schema(fields=fields)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
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
        assert collection_w.has_index(index_name=index_name1)[0] == False
    
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
                2.create  diskann index and uses collection.drop_index () drop index
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
                                  check_items={ct.err_code: 1,
                                               ct.err_msg: "field data type BinaryVector don't support the index build type DISKANN"})

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

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [1, 2, 8, 16, 24, 31])
    def test_create_index_with_small_dim(self, dim):
        """
        target: test create index with diskann
        method: 1.create collection, when the dim of the vector Less than 32
                2.create diskann index
        expected: create index raise an error
        """
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(), cf.gen_string_field(),
                  cf.gen_float_vec_field(dim=dim)]
        schema = cf.gen_collection_schema(fields=fields)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        collection_w.create_index(default_float_vec_field_name, ct.default_diskann_index,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1,
                                               ct.err_msg: "invalid index params"})


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
        actual_index_params = collection_w.index()[0].params
        assert default_autoindex_params == actual_index_params
