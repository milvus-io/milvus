import pytest

from base.client_base import TestcaseBase
from base.index_wrapper import ApiIndexWrapper
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.code_mapping import CollectionErrorMessage as clem
from common.code_mapping import IndexErrorMessage as iem

from utils.utils import *
from common.constants import *

prefix = "index"
default_schema = cf.gen_default_collection_schema()
default_field_name = ct.default_float_vec_field_name
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}

# copied from pymilvus
uid = "test_index"
BUILD_TIMEOUT = 300
field_name = default_float_vec_field_name
binary_field_name = default_binary_vec_field_name
query, query_vecs = gen_query_vectors(field_name, default_entities, default_top_k, 1)
default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}


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
    # TODO (reason="pymilvus issue #677", raises=TypeError)
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

    # TODO: not supported
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason='not supported')
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
                                   check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 1, ct.err_msg: ""})


class TestIndexOperation(TestcaseBase):
    """ Test case of index interface """

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
        data = cf.gen_default_list_data(ct.default_nb)
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
        data = cf.gen_default_list_data(ct.default_nb)
        collection_w.insert(data=data)
        self._connect().flush([collection_w.name])
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
        assert len(collection_w.collection.indexes) == 0

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
        self.index_wrap.drop(check_task=CheckTasks.err_res,
                             check_items={ct.err_code: 1, ct.err_msg: "Index doesn't exist"})


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


class TestIndexBase:
    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        logging.getLogger().info(request.param)
        # if str(connect._cmd("mode")) == "CPU":
        #     if request.param["index_type"] in index_cpu_not_support():
        #         pytest.skip("sq8h not support in CPU mode")
        return copy.deepcopy(request.param)

    @pytest.fixture(
        scope="function",
        params=[
            1,
            10,
            1111
        ],
    )
    def get_nq(self, request):
        yield request.param

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, collection, get_simple_index):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        result = connect.insert(collection, default_entities)
        connect.create_index(collection, field_name, get_simple_index)
        if get_simple_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            create_target_index(get_simple_index, field_name)
            assert index == get_simple_index

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(reason="Repeat with test_index_field_name_not_existed")
    def test_create_index_on_field_not_existed(self, connect, collection, get_simple_index):
        """
        target: test create index interface
        method: create collection and add entities in it, create index on field not existed
        expected: error raised
        """
        tmp_field_name = gen_unique_str()
        result = connect.insert(collection, default_entities)
        with pytest.raises(Exception) as e:
            connect.create_index(collection, tmp_field_name, get_simple_index)

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_on_field(self, connect, collection, get_simple_index):
        """
        target: test create index interface
        method: create collection and add entities in it, create index on other field
        expected: error raised
        """
        tmp_field_name = "int64"
        result = connect.insert(collection, default_entities)
        with pytest.raises(Exception) as e:
            connect.create_index(collection, tmp_field_name, get_simple_index)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors(self, connect, collection, get_simple_index):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        connect.create_index(collection, field_name, get_simple_index)
        if get_simple_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            create_target_index(get_simple_index, field_name)
            assert index == get_simple_index

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition(self, connect, collection, get_simple_index):
        """
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        """
        connect.create_partition(collection, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        connect.create_index(collection, field_name, get_simple_index)
        if get_simple_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            create_target_index(get_simple_index, field_name)
            assert index == get_simple_index

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition_flush(self, connect, collection, get_simple_index):
        """
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        """
        connect.create_partition(collection, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        connect.flush([collection])
        connect.create_index(collection, field_name, get_simple_index)
        if get_simple_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            create_target_index(get_simple_index, field_name)
            assert index == get_simple_index

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_without_connect(self, dis_connect, collection):
        """
        target: test create index without connection
        method: create collection and add entities in it, check if added successfully
        expected: raise exception
        """
        with pytest.raises(Exception) as e:
            dis_connect.create_index(collection, field_name, get_simple_index)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, collection, get_simple_index, get_nq):
        """
        target: test create index interface, search with more query vectors
        method: create collection and add entities in it, create index
        expected: return search success
        """
        result = connect.insert(collection, default_entities)
        connect.flush([collection])
        connect.create_index(collection, field_name, get_simple_index)
        logging.getLogger().info(connect.describe_index(collection, ""))
        nq = get_nq
        index_type = get_simple_index["index_type"]
        search_param = get_search_param(index_type)
        query, vecs = gen_query_vectors(field_name, default_entities, default_top_k, nq, search_params=search_param)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq

    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_multithread(self, connect, collection, args):
        """
        target: test create index interface with multiprocess
        method: create collection and add entities in it, create index
        expected: return search success
        """
        connect.insert(collection, default_entities)

        def build(connect):
            connect.create_index(collection, field_name, default_index)
            if default_index["index_type"] != "FLAT":
                index = connect.describe_index(collection, "")
                create_target_index(default_index, field_name)
                assert index == default_index

        threads_num = 8
        threads = []
        for i in range(threads_num):
            m = get_milvus(host=args["ip"], port=args["port"], handler=args["handler"])
            t = MyThread(target=build, args=(m,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_index_collection_not_existed(self, connect):
        """
        target: test create index interface when collection name not existed
        method: create collection and add entities in it, create index
            , make sure the collection name not in index
        expected: create index failed
        """
        collection_name = gen_unique_str(uid)
        with pytest.raises(Exception) as e:
            connect.create_index(collection_name, field_name, default_index)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_insert_flush(self, connect, collection, get_simple_index):
        """
        target: test create index
        method: create collection and create index, add entities in it
        expected: create index ok, and count correct
        """
        connect.create_index(collection, field_name, get_simple_index)
        result = connect.insert(collection, default_entities)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats["row_count"] == default_nb
        if get_simple_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            create_target_index(get_simple_index, field_name)
            assert index == get_simple_index

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_same_index_repeatedly(self, connect, collection, get_simple_index):
        """
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: return code success, and search ok
        """
        connect.create_index(collection, field_name, get_simple_index)
        connect.create_index(collection, field_name, get_simple_index)
        if get_simple_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            create_target_index(get_simple_index, field_name)
            assert index == get_simple_index

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_different_index_repeatedly(self, connect, collection):
        """
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: return code 0, and describe index result equals with the second index params
        """
        result = connect.insert(collection, default_entities)
        connect.flush([collection])
        indexs = [default_index, {"metric_type":"L2", "index_type": "FLAT", "params":{"nlist": 1024}}]
        for index in indexs:
            connect.create_index(collection, field_name, index)
            connect.release_collection(collection)
            connect.load_collection(collection)
        index = connect.describe_index(collection, "")
        # assert index == indexs[-1]
        assert not index    # FLAT is the last index_type, drop all indexes in server

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_different_index_repeatedly_B(self, connect, collection):
        """
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: return code 0, and describe index result equals with the second index params
        """
        result = connect.insert(collection, default_entities)
        connect.flush([collection])
        indexs = [default_index, {"metric_type": "L2", "index_type": "IVF_SQ8", "params": {"nlist": 1024}}]
        for index in indexs:
            connect.create_index(collection, field_name, index)
            connect.release_collection(collection)
            connect.load_collection(collection)
        index = connect.describe_index(collection, "")
        create_target_index(indexs[-1], field_name)
        assert index == indexs[-1]
        # assert not index  # FLAT is the last index_type, drop all indexes in server

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_ip(self, connect, collection, get_simple_index):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        result = connect.insert(collection, default_entities)
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)
        if get_simple_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            create_target_index(get_simple_index, field_name)
            assert index == get_simple_index

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors_ip(self, connect, collection, get_simple_index):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)
        if get_simple_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            create_target_index(get_simple_index, field_name)
            assert index == get_simple_index

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition_ip(self, connect, collection, get_simple_index):
        """
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        """
        connect.create_partition(collection, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)
        if get_simple_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            create_target_index(get_simple_index, field_name)
            assert index == get_simple_index

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition_flush_ip(self, connect, collection, get_simple_index):
        """
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        """
        connect.create_partition(collection, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        connect.flush([collection])
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)
        if get_simple_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            create_target_index(get_simple_index, field_name)
            assert index == get_simple_index

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors_ip(self, connect, collection, get_simple_index, get_nq):
        """
        target: test create index interface, search with more query vectors
        method: create collection and add entities in it, create index
        expected: return search success
        """
        metric_type = "IP"
        result = connect.insert(collection, default_entities)
        connect.flush([collection])
        get_simple_index["metric_type"] = metric_type
        connect.create_index(collection, field_name, get_simple_index)
        connect.load_collection(collection)
        logging.getLogger().info(connect.describe_index(collection, ""))
        nq = get_nq
        index_type = get_simple_index["index_type"]
        search_param = get_search_param(index_type)
        query, vecs = gen_query_vectors(field_name, default_entities, default_top_k, nq, metric_type=metric_type, search_params=search_param)
        res = connect.search(collection, query)
        assert len(res) == nq

    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_multithread_ip(self, connect, collection, args):
        """
        target: test create index interface with multiprocess
        method: create collection and add entities in it, create index
        expected: return search success
        """
        connect.insert(collection, default_entities)

        def build(connect):
            default_index["metric_type"] = "IP"
            connect.create_index(collection, field_name, default_index)
            if default_index["index_type"] != "FLAT":
                index = connect.describe_index(collection, "")
                create_target_index(default_index, field_name)
                assert index == default_index

        threads_num = 8
        threads = []
        for i in range(threads_num):
            m = get_milvus(host=args["ip"], port=args["port"], handler=args["handler"])
            t = MyThread(target=build, args=(m,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_collection_not_existed_ip(self, connect, collection):
        """
        target: test create index interface when collection name not existed
        method: create collection and add entities in it, create index
            , make sure the collection name not in index
        expected: return code not equals to 0, create index failed
        """
        collection_name = gen_unique_str(uid)
        default_index["metric_type"] = "IP"
        with pytest.raises(Exception) as e:
            connect.create_index(collection_name, field_name, default_index)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors_insert_ip(self, connect, collection):
        """
        target: test create index interface when there is no vectors in collection, and does not affect the subsequent process
        method: create collection and add no vectors in it, and then create index, add entities in it
        expected: return code equals to 0
        """
        default_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, default_index)
        result = connect.insert(collection, default_entities)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats["row_count"] == default_nb
        if default_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            create_target_index(default_index, field_name)
            assert index == default_index

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_same_index_repeatedly_ip(self, connect, collection):
        """
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: return code success, and search ok
        """
        default_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, default_index)
        connect.create_index(collection, field_name, default_index)
        if default_index["index_type"] != "FLAT":
            index = connect.describe_index(collection, "")
            create_target_index(default_index, field_name)
            assert index == default_index

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_different_index_repeatedly_ip(self, connect, collection):
        """
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: return code 0, and describe index result equals with the second index params
        """
        result = connect.insert(collection, default_entities)
        connect.flush([collection])
        connect.load_collection(collection)
        stats = connect.get_collection_stats(collection)
        assert stats["row_count"] == default_nb
        default_index["metric_type"] = "IP"
        indexs = [default_index, {"index_type": "FLAT", "params": {"nlist": 1024}, "metric_type": "IP"}]
        for index in indexs:
            connect.create_index(collection, field_name, index)
            connect.release_collection(collection)
            connect.load_collection(collection)
        index = connect.describe_index(collection, "")
        # assert index == indexs[-1]
        assert not index

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_index(self, connect, collection, get_simple_index):
        """
        target: test drop index interface
        method: create collection and add entities in it, create index, call drop index
        expected: return code 0, and default index param
        """
        # result = connect.insert(collection, entities)
        connect.create_index(collection, field_name, get_simple_index)
        connect.drop_index(collection, field_name)
        index = connect.describe_index(collection, "")
        assert not index

    @pytest.mark.tags(CaseLabel.L2)
    # TODO #7372
    def test_drop_index_repeatedly(self, connect, collection, get_simple_index):
        """
        target: test drop index repeatedly
        method: create index, call drop index, and drop again
        expected: return code 0
        """
        connect.create_index(collection, field_name, get_simple_index)
        connect.drop_index(collection, field_name)
        connect.drop_index(collection, field_name)
        index = connect.describe_index(collection, "")
        assert not index

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index_without_connect(self, dis_connect, collection):
        """
        target: test drop index without connection
        method: drop index, and check if drop successfully
        expected: raise exception
        """
        with pytest.raises(Exception) as e:
            dis_connect.drop_index(collection, field_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_index_collection_not_existed(self, connect):
        """
        target: test drop index interface when collection name not existed
        method: create collection and add entities in it, create index
            , make sure the collection name not in index, and then drop it
        expected: return code not equals to 0, drop index failed
        """
        collection_name = gen_unique_str(uid)
        with pytest.raises(Exception) as e:
            connect.drop_index(collection_name, field_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_index_collection_not_create(self, connect, collection):
        """
        target: test drop index interface when index not created
        method: create collection and add entities in it, create index
        expected: return code not equals to 0, drop index failed
        """
        # no create index
        connect.drop_index(collection, field_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_drop_index_repeatedly(self, connect, collection, get_simple_index):
        """
        target: test create / drop index repeatedly, use the same index params
        method: create index, drop index, four times
        expected: return code 0
        """
        for i in range(4):
            connect.create_index(collection, field_name, get_simple_index)
            connect.drop_index(collection, field_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index_ip(self, connect, collection, get_simple_index):
        """
        target: test drop index interface
        method: create collection and add entities in it, create index, call drop index
        expected: return code 0, and default index param
        """
        # result = connect.insert(collection, entities)
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)
        connect.drop_index(collection, field_name)
        index = connect.describe_index(collection, "")
        assert not index

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index_repeatedly_ip(self, connect, collection, get_simple_index):
        """
        target: test drop index repeatedly
        method: create index, call drop index, and drop again
        expected: return code 0
        """
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)
        connect.drop_index(collection, field_name)
        connect.drop_index(collection, field_name)
        index = connect.describe_index(collection, "")
        assert not index

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index_without_connect_ip(self, dis_connect, collection):
        """
        target: test drop index without connection
        method: drop index, and check if drop successfully
        expected: raise exception
        """
        with pytest.raises(Exception) as e:
            dis_connect.drop_index(collection, field_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index_collection_not_create_ip(self, connect, collection):
        """
        target: test drop index interface when index not created
        method: create collection and add entities in it, create index
        expected: return code not equals to 0, drop index failed
        """
        # result = connect.insert(collection, entities)
        # no create index
        connect.drop_index(collection, field_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_drop_index_repeatedly_ip(self, connect, collection, get_simple_index):
        """
        target: test create / drop index repeatedly, use the same index params
        method: create index, drop index, four times
        expected: return code 0
        """
        get_simple_index["metric_type"] = "IP"
        for i in range(4):
            connect.create_index(collection, field_name, get_simple_index)
            connect.drop_index(collection, field_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_PQ_without_nbits(self, connect, collection):
        """
        target: test create PQ index
        method: create PQ index without nbits
        expected: create successfully
        """
        PQ_index = {"index_type": "IVF_PQ", "params": {"nlist": 128, "m": 16}, "metric_type": "L2"}
        result = connect.insert(collection, default_entities)
        connect.create_index(collection, field_name, PQ_index)
        index = connect.describe_index(collection, "")
        create_target_index(PQ_index, field_name)
        assert index == PQ_index


class TestIndexBinary:
    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        # if str(connect._cmd("mode")) == "CPU":
        #     if request.param["index_type"] in index_cpu_not_support():
        #         pytest.skip("sq8h not support in CPU mode")
        return copy.deepcopy(request.param)

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_jaccard_index(self, request, connect):
        if request.param["index_type"] in binary_support():
            request.param["metric_type"] = "JACCARD"
            return request.param
        else:
            pytest.skip("Skip index")

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_l2_index(self, request, connect):
        request.param["metric_type"] = "L2"
        return request.param

    @pytest.fixture(
        scope="function",
        params=[
            1,
            10,
            1111
        ],
    )
    def get_nq(self, request):
        yield request.param

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, binary_collection, get_jaccard_index):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        result = connect.insert(binary_collection, default_binary_entities)
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)
        binary_index = connect.describe_index(binary_collection, "")
        create_target_index(get_jaccard_index, binary_field_name)
        assert binary_index == get_jaccard_index

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition(self, connect, binary_collection, get_jaccard_index):
        """
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        """
        connect.create_partition(binary_collection, default_tag)
        result = connect.insert(binary_collection, default_binary_entities, partition_name=default_tag)
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)
        binary_index = connect.describe_index(binary_collection, "")
        create_target_index(get_jaccard_index, binary_field_name)
        assert binary_index == get_jaccard_index

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, binary_collection, get_jaccard_index, get_nq):
        """
        target: test create index interface, search with more query vectors
        method: create collection and add entities in it, create index
        expected: return search success
        """
        nq = get_nq
        result = connect.insert(binary_collection, default_binary_entities)
        connect.flush([binary_collection])
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)
        connect.load_collection(binary_collection)
        query, vecs = gen_query_vectors(binary_field_name, default_binary_entities, default_top_k, nq, metric_type="JACCARD")
        search_param = get_search_param(get_jaccard_index["index_type"], metric_type="JACCARD")
        logging.getLogger().info(search_param)
        res = connect.search(binary_collection, query, search_params=search_param)
        assert len(res) == nq

    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_invalid_metric_type_binary(self, connect, binary_collection, get_l2_index):
        """
        target: test create index interface with invalid metric type
        method: add entitys into binary connection, flash, create index with L2 metric type.
        expected: return create_index failure
        """
        # insert 6000 vectors
        result = connect.insert(binary_collection, default_binary_entities)
        connect.flush([binary_collection])
        with pytest.raises(Exception) as e:
            res = connect.create_index(binary_collection, binary_field_name, get_l2_index)

    """
    ******************************************************************
      The following cases are used to test `describe_index` function
    ***************************************************************
    """
    @pytest.mark.skip("repeat with test_create_index binary")
    def _test_get_index_info(self, connect, binary_collection, get_jaccard_index):
        """
        target: test describe index interface
        method: create collection and add entities in it, create index, call describe index
        expected: return code 0, and index instructure
        """
        result = connect.insert(binary_collection, default_binary_entities)
        connect.flush([binary_collection])
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)
        stats = connect.get_collection_stats(binary_collection)
        assert stats["row_count"] == default_nb
        for partition in stats["partitions"]:
            segments = partition["segments"]
            if segments:
                for segment in segments:
                    for file in segment["files"]:
                        if "index_type" in file:
                            assert file["index_type"] == get_jaccard_index["index_type"]

    @pytest.mark.skip("repeat with test_create_index_partition binary")
    def _test_get_index_info_partition(self, connect, binary_collection, get_jaccard_index):
        """
        target: test describe index interface
        method: create collection, create partition and add entities in it, create index, call describe index
        expected: return code 0, and index instructure
        """
        connect.create_partition(binary_collection, default_tag)
        result = connect.insert(binary_collection, default_binary_entities, partition_name=default_tag)
        connect.flush([binary_collection])
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)
        stats = connect.get_collection_stats(binary_collection)
        logging.getLogger().info(stats)
        assert stats["row_count"] == default_nb
        assert len(stats["partitions"]) == 2
        for partition in stats["partitions"]:
            segments = partition["segments"]
            if segments:
                for segment in segments:
                    for file in segment["files"]:
                        if "index_type" in file:
                            assert file["index_type"] == get_jaccard_index["index_type"]

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index(self, connect, binary_collection, get_jaccard_index):
        """
        target: test drop index interface
        method: create collection and add entities in it, create index, call drop index
        expected: return code 0, and default index param
        """
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)
        stats = connect.get_collection_stats(binary_collection)
        logging.getLogger().info(stats)
        connect.drop_index(binary_collection, binary_field_name)
        binary_index = connect.describe_index(binary_collection, "")
        assert not binary_index

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_index_partition(self, connect, binary_collection, get_jaccard_index):
        """
        target: test drop index interface
        method: create collection, create partition and add entities in it, create index on collection, call drop collection index
        expected: return code 0, and default index param
        """
        connect.create_partition(binary_collection, default_tag)
        result = connect.insert(binary_collection, default_binary_entities, partition_name=default_tag)
        connect.flush([binary_collection])
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)
        connect.drop_index(binary_collection, binary_field_name)
        binary_index = connect.describe_index(binary_collection, "")
        assert not binary_index


class TestIndexInvalid(object):
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
        logging.getLogger().info(get_index)
        with pytest.raises(Exception) as e:
            connect.create_index(collection, field_name, get_index)


class TestIndexAsync:
    @pytest.fixture(scope="function", autouse=True)
    def skip_http_check(self, args):
        if args["handler"] == "HTTP":
            pytest.skip("skip in http mode")

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        # if str(connect._cmd("mode")) == "CPU":
        #     if request.param["index_type"] in index_cpu_not_support():
        #         pytest.skip("sq8h not support in CPU mode")
        return copy.deepcopy(request.param)

    def check_result(self, res):
        logging.getLogger().info("In callback check search result")
        logging.getLogger().info(res)

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, collection, get_simple_index):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        result = connect.insert(collection, default_entities)
        logging.getLogger().info("start index")
        future = connect.create_index(collection, field_name, get_simple_index, _async=True)
        logging.getLogger().info("before result")
        res = future.result()
        # TODO:
        logging.getLogger().info(res)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_drop(self, connect, collection):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        result = connect.insert(collection, default_entities)
        connect.create_index(collection, field_name, default_index, _async=True)
        connect.drop_collection(collection)
        with pytest.raises(Exception, match=f'DescribeIndex failed, error = collection {collection} not found'):
            connect.describe_index(collection, "")

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_index_with_invalid_collection_name(self, connect):
        collection_name = " "
        with pytest.raises(Exception) as e:
            future = connect.create_index(collection_name, field_name, default_index, _async=True)
            res = future.result()

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_callback(self, connect, collection, get_simple_index):
        """
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        """
        result = connect.insert(collection, default_entities)
        logging.getLogger().info("start index")
        future = connect.create_index(collection, field_name, get_simple_index, _async=True,
                                      _callback=self.check_result)
        logging.getLogger().info("before result")
        res = future.result()
        # TODO:
        logging.getLogger().info(res)