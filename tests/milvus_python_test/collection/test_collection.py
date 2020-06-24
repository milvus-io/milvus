import pdb
import pytest
import logging
import itertools
from time import sleep
from multiprocessing import Process
from milvus import IndexType, MetricType
from utils import *

dim = 128
default_segment_size = 1024
drop_collection_interval_time = 3
segment_size = 10
vectors = gen_vectors(100, dim)
default_fields = gen_default_fields() 


class TestCollection:

    """
    ******************************************************************
      The following cases are used to test `create_collection` function
    ******************************************************************
    """
    @pytest.fixture(
        scope="function",
        params=gen_single_filter_fields()
    )
    def get_filter_field(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_single_vector_fields()
    )
    def get_vector_field(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_segment_sizes()
    )
    def get_segment_size(self, request):
        yield request.param

    """
    ******************************************************************
      The following cases are used to test `get_collection_info` function
    ******************************************************************
    """

    def test_collection_describe_result(self, connect):
        '''
        target: test describe collection created with correct params 
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'segment_size': segment_size,
                 'metric_type': MetricType.L2}
        connect.create_collection(param)
        status, res = connect.get_collection_info(collection_name)
        assert res.collection_name == collection_name
        assert res.metric_type == MetricType.L2

    @pytest.mark.level(2)
    def test_collection_get_collection_info_name_ip(self, connect):
        '''
        target: test describe collection created with correct params 
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'segment_size': segment_size,
                 'metric_type': MetricType.IP}
        connect.create_collection(param)
        status, res = connect.get_collection_info(collection_name)
        assert res.collection_name == collection_name
        assert res.metric_type == MetricType.IP

    @pytest.mark.level(2)
    def test_collection_get_collection_info_name_jaccard(self, connect):
        '''
        target: test describe collection created with correct params 
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'segment_size': segment_size,
                 'metric_type': MetricType.JACCARD}
        connect.create_collection(param)
        status, res = connect.get_collection_info(collection_name)
        assert res.collection_name == collection_name
        assert res.metric_type == MetricType.JACCARD

    @pytest.mark.level(2)
    def test_collection_get_collection_info_name_hamming(self, connect):
        '''
        target: test describe collection created with correct params
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'segment_size': segment_size,
                 'metric_type': MetricType.HAMMING}
        connect.create_collection(param)
        status, res = connect.get_collection_info(collection_name)
        assert res.collection_name == collection_name
        assert res.metric_type == MetricType.HAMMING

    def test_collection_get_collection_info_name_substructure(self, connect):
        '''
        target: test describe collection created with correct params
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'segment_size': segment_size,
                 'metric_type': MetricType.SUBSTRUCTURE}
        connect.create_collection(param)
        status, res = connect.get_collection_info(collection_name)
        assert res.collection_name == collection_name
        assert res.metric_type == MetricType.SUBSTRUCTURE

    def test_collection_get_collection_info_name_superstructure(self, connect):
        '''
        target: test describe collection created with correct params
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'segment_size': segment_size,
                 'metric_type': MetricType.SUPERSTRUCTURE}
        connect.create_collection(param)
        status, res = connect.get_collection_info(collection_name)
        assert res.collection_name == collection_name
        assert res.metric_type == MetricType.SUPERSTRUCTURE

    # TODO: enable
    @pytest.mark.level(2)
    def _test_collection_get_collection_info_name_multiprocessing(self, connect, args):
        '''
        target: test describe collection created with multiprocess 
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'segment_size': segment_size, 
                 'metric_type': MetricType.L2}
        connect.create_collection(param)

        def describecollection(milvus):
            status, res = milvus.get_collection_info(collection_name)
            assert res.collection_name == collection_name

        process_num = 4
        processes = []
        for i in range(process_num):
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            p = Process(target=describecollection, args=(milvus,))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()
    
    def test_collection_describe_dimension(self, connect):
        '''
        target: test describe collection created with correct params 
        method: create collection, assert the dimention value returned by describe method
        expected: dimention equals with dimention when created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim+1,
                 'segment_size': segment_size,
                 'metric_type': MetricType.L2}
        connect.create_collection(param)
        status, res = connect.get_collection_info(collection_name)
        assert res.dimension == dim+1

    """
    ******************************************************************
      The following cases are used to test `drop_collection` function
    ******************************************************************
    """

    def test_drop_collection(self, connect, collection):
        '''
        target: test delete collection created with correct params 
        method: create collection and then delete, 
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        status = connect.drop_collection(collection)
        assert not assert_has_collection(connect, collection)

    @pytest.mark.level(2)
    def test_drop_collection_ip(self, connect, ip_collection):
        '''
        target: test delete collection created with correct params 
        method: create collection and then delete, 
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        status = connect.drop_collection(ip_collection)
        assert not assert_has_collection(connect, ip_collection)

    @pytest.mark.level(2)
    def test_drop_collection_jaccard(self, connect, jac_collection):
        '''
        target: test delete collection created with correct params 
        method: create collection and then delete, 
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        status = connect.drop_collection(jac_collection)
        assert not assert_has_collection(connect, jac_collection)

    @pytest.mark.level(2)
    def test_drop_collection_hamming(self, connect, ham_collection):
        '''
        target: test delete collection created with correct params
        method: create collection and then delete,
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        status = connect.drop_collection(ham_collection)
        assert not assert_has_collection(connect, ham_collection)

    # @pytest.mark.level(2)
    # def test_collection_delete_without_connection(self, collection, dis_connect):
    #     '''
    #     target: test describe collection, without connection
    #     method: describe collection with correct params, with a disconnected instance
    #     expected: describe raise exception
    #     '''
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.drop_collection(collection)

    def test_drop_collection_not_existed(self, connect):
        '''
        target: test delete collection not in index
        method: delete all collections, and delete collection again, 
            assert the value returned by delete method
        expected: status not ok
        '''
        collection_name = gen_unique_str("test_collection")
        status = connect.drop_collection(collection_name)
        assert not status.OK()

    def test_delete_create_collection_repeatedly(self, connect):
        '''
        target: test delete and create the same collection repeatedly
        method: try to create the same collection and delete repeatedly,
            assert the value returned by delete method
        expected: create ok and delete ok
        '''
        loops = 2 
        timeout = 5
        for i in range(loops):
            collection_name = "test_collection"
            param = {'collection_name': collection_name,
                 'dimension': dim,
                 'segment_size': segment_size,
                 'metric_type': MetricType.L2}
            connect.create_collection(param)
            status = None
            while i < timeout:
                status = connect.drop_collection(collection_name)
                time.sleep(1)
                i += 1
                if status.OK():
                    break
            if i > timeout:
                assert False

    # TODO: enable
    @pytest.mark.level(2)
    def _test_drop_collection_multiprocessing(self, args):
        '''
        target: test delete collection with multiprocess 
        method: create collection and then delete, 
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        process_num = 6
        processes = []
        def deletecollection(milvus):
            status = milvus.drop_collection(collection)
            # assert not status.code==0
            assert assert_has_collection(milvus, collection)
            assert status.OK()

        for i in range(process_num):
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            p = Process(target=deletecollection, args=(milvus,))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

    # TODO: enable
    @pytest.mark.level(2)
    def _test_drop_collection_multiprocessing_multicollection(self, connect):
        '''
        target: test delete collection with multiprocess 
        method: create collection and then delete, 
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        process_num = 5
        loop_num = 2
        processes = []

        collection = []
        j = 0
        while j < (process_num*loop_num):
            collection_name = gen_unique_str("test_drop_collection_with_multiprocessing")
            collection.append(collection_name)
            param = {'collection_name': collection_name,
                 'dimension': dim,
                 'segment_size': segment_size,
                 'metric_type': MetricType.L2}
            connect.create_collection(param)
            j = j + 1

        def delete(connect,ids):
            i = 0
            while i < loop_num:
                status = connect.drop_collection(collection[ids*process_num+i])
                time.sleep(2)
                assert status.OK()
                assert not assert_has_collection(connect, collection[ids*process_num+i])
                i = i + 1

        for i in range(process_num):
            ids = i
            p = Process(target=delete, args=(connect,ids))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

    """
    ******************************************************************
      The following cases are used to test `load_collection` function
    ******************************************************************
    """

    """
    generate valid create_index params
    """
    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in cpu mode")
        if request.param["index_type"] == IndexType.IVF_PQ:
            pytest.skip("Skip PQ Temporary")
        return request.param

    @pytest.mark.level(1)
    def test_load_collection(self, connect, collection, get_simple_index):
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status, ids = connect.insert(collection, vectors)
        status = connect.create_index(collection, index_type, index_param)
        status = connect.load_collection(collection)
        assert status.OK()

    @pytest.mark.level(1)
    def test_load_collection_ip(self, connect, ip_collection, get_simple_index):
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status, ids = connect.insert(ip_collection, vectors)
        status = connect.create_index(ip_collection, index_type, index_param)
        status = connect.load_collection(ip_collection)
        assert status.OK()

    @pytest.mark.level(1)
    def test_load_collection_jaccard(self, connect, jac_collection, get_simple_index):
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status, ids = connect.insert(jac_collection, vectors)
        status = connect.create_index(jac_collection, index_type, index_param)
        status = connect.load_collection(jac_collection)
        assert status.OK()

    @pytest.mark.level(1)
    def test_load_collection_hamming(self, connect, ham_collection, get_simple_index):
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status, ids = connect.insert(ham_collection, vectors)
        status = connect.create_index(ham_collection, index_type, index_param)
        status = connect.load_collection(ham_collection)
        assert status.OK()

    @pytest.mark.level(2)
    def test_load_collection_not_existed(self, connect, collection, get_simple_index):
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        collection_name = gen_unique_str()
        status, ids = connect.insert(collection, vectors)
        status = connect.create_index(collection, index_type, index_param)
        status = connect.load_collection(collection_name)
        assert not status.OK()

    @pytest.mark.level(2)
    def test_load_collection_not_existed_ip(self, connect, ip_collection, get_simple_index):
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        collection_name = gen_unique_str()
        status, ids = connect.insert(ip_collection, vectors)
        status = connect.create_index(ip_collection, index_type, index_param)
        status = connect.load_collection(collection_name)
        assert not status.OK()

    @pytest.mark.level(1)
    def test_load_collection_no_vectors(self, connect, collection):
        status = connect.load_collection(collection)
        assert status.OK()

    @pytest.mark.level(2)
    def test_load_collection_no_vectors_ip(self, connect, ip_collection):
        status = connect.load_collection(ip_collection)
        assert status.OK()

    # TODO: psutils get memory usage
    @pytest.mark.level(1)
    def test_load_collection_memory_usage(self, connect, collection):
        pass


def create_collection(connect, **params):
    param = {'collection_name': params["collection_name"],
             'dimension': params["dimension"],
             'segment_size': segment_size,
             'metric_type': MetricType.L2}
    status = connect.create_collection(param)
    return status

def search_collection(connect, **params):
    status, result = connect.search(
        params["collection_name"], 
        params["top_k"], 
        params["query_vectors"],
        params={"nprobe": params["nprobe"]})
    return status

def load_collection(connect, **params):
    status = connect.load_collection(params["collection_name"])
    return status

def has(connect, **params):
    status, result = connect.has_collection(params["collection_name"])
    return status

def show(connect, **params):
    status, result = connect.list_collections()
    return status

def delete(connect, **params):
    status = connect.drop_collection(params["collection_name"])
    return status

def describe(connect, **params):
    status, result = connect.get_collection_info(params["collection_name"])
    return status

def rowcount(connect, **params):
    status, result = connect.count_entities(params["collection_name"])
    return status

def create_index(connect, **params):
    status = connect.create_index(params["collection_name"], params["index_type"], params["index_param"])
    return status

func_map = { 
    # 0:has, 
    1:show,
    10:create_collection, 
    11:describe,
    12:rowcount,
    13:search_collection,
    14:load_collection,
    15:create_index,
    30:delete
}

def gen_sequence():
    raw_seq = func_map.keys()
    result = itertools.permutations(raw_seq)
    for x in result:
        yield x

class TestCollectionLogic(object):
    @pytest.mark.parametrize("logic_seq", gen_sequence())
    @pytest.mark.level(2)
    def test_logic(self, connect, logic_seq, args):
        if args["handler"] == "HTTP":
            pytest.skip("Skip in http mode")
        if self.is_right(logic_seq):
            self.execute(logic_seq, connect)
        else:
            self.execute_with_error(logic_seq, connect)
        self.tear_down(connect)

    def is_right(self, seq):
        if sorted(seq) == seq:
            return True

        not_created = True
        has_deleted = False
        for i in range(len(seq)):
            if seq[i] > 10 and not_created:
                return False
            elif seq [i] > 10 and has_deleted:
                return False
            elif seq[i] == 10:
                not_created = False
            elif seq[i] == 30:
                has_deleted = True

        return True

    def execute(self, logic_seq, connect):
        basic_params = self.gen_params()
        for i in range(len(logic_seq)):
            # logging.getLogger().info(logic_seq[i])
            f = func_map[logic_seq[i]]
            status = f(connect, **basic_params)
            assert status.OK()

    def execute_with_error(self, logic_seq, connect):
        basic_params = self.gen_params()

        error_flag = False
        for i in range(len(logic_seq)):
            f = func_map[logic_seq[i]]
            status = f(connect, **basic_params)
            if not status.OK():
                # logging.getLogger().info(logic_seq[i])
                error_flag = True
                break
        assert error_flag == True

    def tear_down(self, connect):
        names = connect.list_collections()[1]
        for name in names:
            connect.drop_collection(name)

    def gen_params(self):
        collection_name = gen_unique_str("test_collection")
        top_k = 1
        vectors = gen_vectors(2, dim)
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'metric_type': MetricType.L2,
                 'nprobe': 1,
                 'top_k': top_k,
                 'index_type': IndexType.IVF_SQ8,
                 'index_param': {
                        'nlist': 16384
                 },
                 'query_vectors': vectors}
        return param
