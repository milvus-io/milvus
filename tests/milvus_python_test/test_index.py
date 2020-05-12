"""
   For testing index operations, including `create_index`, `get_index_info` and `drop_index` interfaces
"""
import logging
import pytest
import time
import pdb
import threading
from multiprocessing import Pool, Process
import numpy
import sklearn.preprocessing
from milvus import IndexType, MetricType
from utils import *

nb = 6000
dim = 128
index_file_size = 10
vectors = gen_vectors(nb, dim)
vectors = sklearn.preprocessing.normalize(vectors, axis=1, norm='l2')
vectors = vectors.tolist()
BUILD_TIMEOUT = 300
nprobe = 1
tag = "1970-01-01"
NLIST = 4046
INVALID_NLIST = 100000000


class TestIndexBase:
    @pytest.fixture(
        scope="function",
        params=gen_index()
    )
    def get_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        return request.param

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status, ids = connect.insert(collection, vectors)
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection, create partition, and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(collection, tag)
        status, ids = connect.insert(collection, vectors, partition_tag=tag)
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition_flush(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection, create partition, and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(collection, tag)
        status, ids = connect.insert(collection, vectors, partition_tag=tag)
        connect.flush()
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()

    # @pytest.mark.level(2)
    # def test_create_index_without_connect(self, dis_connect, collection):
    #     '''
    #     target: test create index without connection
    #     method: create collection and add vectors in it, check if added successfully
    #     expected: raise exception
    #     '''
    #     nlist = NLIST
    #     index_type = IndexType.IVF_SQ8
    #     index_param = {"nlist": nlist}
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.create_index(collection, index_type, index_param)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, collection, get_simple_index):
        '''
        target: test create index interface, search with more query vectors
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status, ids = connect.insert(collection, vectors)
        status = connect.create_index(collection, index_type, index_param)
        logging.getLogger().info(connect.get_index_info(collection))
        query_vecs = [vectors[0], vectors[1], vectors[2]]
        top_k = 5
        search_param = get_search_param(index_type)
        status, result = connect.search(collection, top_k, query_vecs, params=search_param)
        assert status.OK()
        assert len(result) == len(query_vecs)
        logging.getLogger().info(result)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.level(2)
    def test_create_index_multithread(self, connect, collection, args):
        '''
        target: test create index interface with multiprocess
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        status, ids = connect.insert(collection, vectors)

        def build(connect):
            status = connect.create_index(collection, IndexType.IVFLAT, {"nlist": NLIST})
            assert status.OK()

        threads_num = 8
        threads = []
        for i in range(threads_num):
            m = get_milvus(host=args["ip"], port=args["port"], handler=args["handler"])
            t = threading.Thread(target=build, args=(m,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

        query_vec = [vectors[0]]
        top_k = 1
        search_param = {"nprobe": nprobe}
        status, result = connect.search(collection, top_k, query_vec, params=search_param)
        assert len(result) == 1
        assert len(result[0]) == top_k
        assert result[0][0].distance == 0.0

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_multithread_multicollection(self, connect, args):
        '''
        target: test create index interface with multiprocess
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        threads_num = 8
        loop_num = 8
        threads = []
        collection = []
        j = 0
        while j < (threads_num*loop_num):
            collection_name = gen_unique_str("test_create_index_multiprocessing")
            collection.append(collection_name)
            param = {'collection_name': collection_name,
                     'dimension': dim,
                     'index_type': IndexType.FLAT,
                     'store_raw_vector': False}
            connect.create_collection(param)
            j = j + 1

        def create_index():
            i = 0
            while i < loop_num:
                # assert connect.has_collection(collection[ids*process_num+i])
                status, ids = connect.insert(collection[ids*threads_num+i], vectors)
                status = connect.create_index(collection[ids*threads_num+i], IndexType.IVFLAT, {"nlist": NLIST})
                assert status.OK()
                query_vec = [vectors[0]]
                top_k = 1
                search_param = {"nprobe": nprobe}
                status, result = connect.search(collection[ids*threads_num+i], top_k, query_vec, params=search_param)
                assert len(result) == 1
                assert len(result[0]) == top_k
                assert result[0][0].distance == 0.0
                i = i + 1
        for i in range(threads_num):
            m = get_milvus(host=args["ip"], port=args["port"], handler=args["handler"])
            ids = i
            t = threading.Thread(target=create_index, args=(m, ids))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.level(2)
    def test_create_index_a_multithreads(self, connect, collection, args):
        status, ids = connect.insert(collection, vectors)
        def build(connect):
            status = connect.create_index(collection, IndexType.IVFLAT, {"nlist": NLIST})
            assert status.OK()
        def count(connect):
            status, count = connect.count_entities(collection)
            assert status.OK()
            assert count == nb

        threads_num = 8
        threads = []
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        for i in range(threads_num):
            m = get_milvus(host=args["ip"], port=args["port"], handler=args["handler"])
            if(i % 2 == 0):
                p = threading.Thread(target=build, args=(m,))
            else:
                p = threading.Thread(target=count, args=(m,))
            threads.append(p)
            p.start()
            time.sleep(0.2)
        for p in threads:
            p.join()


    # TODO: enable
    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.level(2)
    def _test_create_index_multiprocessing(self, connect, collection, args):
        '''
        target: test create index interface with multiprocess
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        status, ids = connect.insert(collection, vectors)

        def build(connect):
            status = connect.create_index(collection, IndexType.IVFLAT, {"nlist": NLIST})
            assert status.OK()

        process_num = 8
        processes = []
        for i in range(process_num):
            m = get_milvus(host=args["ip"], port=args["port"], handler=args["handler"])
            p = Process(target=build, args=(m,))
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()

        query_vec = [vectors[0]]
        top_k = 1
        search_param = {"nprobe": nprobe}
        status, result = connect.search(collection, top_k, query_vec, params=search_param)
        assert len(result) == 1
        assert len(result[0]) == top_k
        assert result[0][0].distance == 0.0

    # TODO: enable
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def _test_create_index_multiprocessing_multicollection(self, connect, args):
        '''
        target: test create index interface with multiprocess
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        process_num = 8
        loop_num = 8
        processes = []

        collection = []
        j = 0
        while j < (process_num*loop_num):
            collection_name = gen_unique_str("test_create_index_multiprocessing")
            collection.append(collection_name)
            param = {'collection_name': collection_name,
                    'dimension': dim,
                    'index_type': IndexType.FLAT,
                    'store_raw_vector': False}
            connect.create_collection(param)
            j = j + 1

        def create_index():
            i = 0
            while i < loop_num:
                # assert connect.has_collection(collection[ids*process_num+i])
                status, ids = connect.insert(collection[ids*process_num+i], vectors)

                status = connect.create_index(collection[ids*process_num+i], IndexType.IVFLAT, {"nlist": NLIST})
                assert status.OK()
                query_vec = [vectors[0]]
                top_k = 1
                search_param = {"nprobe": nprobe}
                status, result = connect.search(collection[ids*process_num+i], top_k, query_vec, params=search_param)
                assert len(result) == 1
                assert len(result[0]) == top_k
                assert result[0][0].distance == 0.0
                i = i + 1

        for i in range(process_num):
            m = get_milvus(host=args["ip"], port=args["port"], handler=args["handler"])
            ids = i
            p = Process(target=create_index, args=(m,ids))
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()

    def test_create_index_collection_not_existed(self, connect):
        '''
        target: test create index interface when collection name not existed
        method: create collection and add vectors in it, create index
            , make sure the collection name not in index
        expected: return code not equals to 0, create index failed
        '''
        collection_name = gen_unique_str(self.__class__.__name__)
        nlist = NLIST
        index_type = IndexType.IVF_SQ8
        index_param = {"nlist": nlist}
        status = connect.create_index(collection_name, index_type, index_param)
        assert not status.OK()

    def test_create_index_collection_None(self, connect):
        '''
        target: test create index interface when collection name is None
        method: create collection and add vectors in it, create index with an collection_name: None
        expected: return code not equals to 0, create index failed
        '''
        collection_name = None
        nlist = NLIST
        index_type = IndexType.IVF_SQ8
        index_param = {"nlist": nlist}
        with pytest.raises(Exception) as e:
            status = connect.create_index(collection_name, index_type, index_param)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors_then_insert(self, connect, collection, get_simple_index):
        '''
        target: test create index interface when there is no vectors in collection, and does not affect the subsequent process
        method: create collection and add no vectors in it, and then create index, add vectors in it
        expected: return code equals to 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(collection, index_type, index_param)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_same_index_repeatedly(self, connect, collection, get_simple_index):
        '''
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: return code success, and search ok
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(collection, index_type, index_param)
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_different_index_repeatedly(self, connect, collection):
        '''
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: return code 0, and describe index result equals with the second index params
        '''
        nlist = NLIST
        status, ids = connect.insert(collection, vectors)
        index_type_1 = IndexType.IVF_SQ8
        index_type_2 = IndexType.IVFLAT
        indexs = [{"index_type": index_type_1, "index_param": {"nlist": nlist}}, {"index_type": index_type_2, "index_param": {"nlist": nlist}}]
        logging.getLogger().info(indexs)
        for index in indexs:
            status = connect.create_index(collection, index["index_type"], index["index_param"])
            assert status.OK()
        status, result = connect.get_index_info(collection)
        assert result._params["nlist"] == nlist
        assert result._collection_name == collection
        assert result._index_type == index_type_2

    """
    ******************************************************************
      The following cases are used to test `get_index_info` function
    ******************************************************************
    """

    def test_get_index_info(self, connect, collection, get_index):
        '''
        target: test describe index interface
        method: create collection and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_index["index_param"]
        index_type = get_index["index_type"]
        logging.getLogger().info(get_index)
        # status, ids = connect.insert(collection, vectors)
        status = connect.create_index(collection, index_type, index_param)
        if status.OK():
            status, result = connect.get_index_info(collection)
            logging.getLogger().info(result)
            assert result._params == index_param
            assert result._collection_name == collection
            assert result._index_type == index_type

    def test_describe_and_drop_index_multi_collections(self, connect, get_simple_index):
        '''
        target: test create, describe and drop index interface with multiple collections of L2
        method: create collections and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        collection_list = []
        for i in range(10):
            collection_name = gen_unique_str()
            collection_list.append(collection_name)
            param = {'collection_name': collection_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.L2}
            connect.create_collection(param)
            index_param = get_simple_index["index_param"]
            index_type = get_simple_index["index_type"]
            logging.getLogger().info(get_simple_index)
            status, ids = connect.insert(collection_name=collection_name, records=vectors)
            status = connect.create_index(collection_name, index_type, index_param)
            assert status.OK()

        for i in range(10):
            status, result = connect.get_index_info(collection_list[i])
            logging.getLogger().info(result)
            assert result._params == index_param
            assert result._collection_name == collection_list[i]
            assert result._index_type == index_type

        for i in range(10):
            status = connect.drop_index(collection_list[i])
            assert status.OK()
            status, result = connect.get_index_info(collection_list[i])
            logging.getLogger().info(result)
            assert result._collection_name == collection_list[i]
            assert result._index_type == IndexType.FLAT

    # @pytest.mark.level(2)
    # def test_get_index_info_without_connect(self, dis_connect, collection):
    #     '''
    #     target: test describe index without connection
    #     method: describe index, and check if describe successfully
    #     expected: raise exception
    #     '''
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.get_index_info(collection)

    def test_get_index_info_collection_not_existed(self, connect):
        '''
        target: test describe index interface when collection name not existed
        method: create collection and add vectors in it, create index
            , make sure the collection name not in index
        expected: return code not equals to 0, describe index failed
        '''
        collection_name = gen_unique_str(self.__class__.__name__)
        status, result = connect.get_index_info(collection_name)
        assert not status.OK()

    def test_get_index_info_collection_None(self, connect):
        '''
        target: test describe index interface when collection name is None
        method: create collection and add vectors in it, create index with an collection_name: None
        expected: return code not equals to 0, describe index failed
        '''
        collection_name = None
        with pytest.raises(Exception) as e:
            status = connect.get_index_info(collection_name)

    def test_get_index_info_not_create(self, connect, collection):
        '''
        target: test describe index interface when index not created
        method: create collection and add vectors in it, create index
            , make sure the collection name not in index
        expected: return code not equals to 0, describe index failed
        '''
        status, ids = connect.insert(collection, vectors)
        status, result = connect.get_index_info(collection)
        logging.getLogger().info(result)
        assert status.OK()
        # assert result._params["nlist"] == index_params["nlist"]
        # assert result._collection_name == collection
        # assert result._index_type == index_params["index_type"]

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """

    def test_drop_index(self, connect, collection, get_simple_index):
        '''
        target: test drop index interface
        method: create collection and add vectors in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        # status, ids = connect.insert(collection, vectors)
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()
        status, result = connect.get_index_info(collection)
        logging.getLogger().info(result)
        status = connect.drop_index(collection)
        assert status.OK()
        status, result = connect.get_index_info(collection)
        logging.getLogger().info(result)
        assert result._collection_name == collection
        assert result._index_type == IndexType.FLAT

    @pytest.mark.level(2)
    def test_drop_index_repeatly(self, connect, collection, get_simple_index):
        '''
        target: test drop index repeatly
        method: create index, call drop index, and drop again
        expected: return code 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        # status, ids = connect.insert(collection, vectors)
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()
        status, result = connect.get_index_info(collection)
        logging.getLogger().info(result)
        status = connect.drop_index(collection)
        assert status.OK()
        status = connect.drop_index(collection)
        assert status.OK()
        status, result = connect.get_index_info(collection)
        logging.getLogger().info(result)
        assert result._collection_name == collection
        assert result._index_type == IndexType.FLAT

    # @pytest.mark.level(2)
    # def test_drop_index_without_connect(self, dis_connect, collection):
    #     '''
    #     target: test drop index without connection
    #     method: drop index, and check if drop successfully
    #     expected: raise exception
    #     '''
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.drop_index(collection)

    def test_drop_index_collection_not_existed(self, connect):
        '''
        target: test drop index interface when collection name not existed
        method: create collection and add vectors in it, create index
            , make sure the collection name not in index, and then drop it
        expected: return code not equals to 0, drop index failed
        '''
        collection_name = gen_unique_str(self.__class__.__name__)
        status = connect.drop_index(collection_name)
        assert not status.OK()

    def test_drop_index_collection_None(self, connect):
        '''
        target: test drop index interface when collection name is None
        method: create collection and add vectors in it, create index with an collection_name: None
        expected: return code not equals to 0, drop index failed
        '''
        collection_name = None
        with pytest.raises(Exception) as e:
            status = connect.drop_index(collection_name)

    def test_drop_index_collection_not_create(self, connect, collection):
        '''
        target: test drop index interface when index not created
        method: create collection and add vectors in it, create index
        expected: return code not equals to 0, drop index failed
        '''
        status, ids = connect.insert(collection, vectors)
        status, result = connect.get_index_info(collection)
        logging.getLogger().info(result)
        # no create index
        status = connect.drop_index(collection)
        logging.getLogger().info(status)
        assert status.OK()

    @pytest.mark.level(2)
    def test_create_drop_index_repeatly(self, connect, collection, get_simple_index):
        '''
        target: test create / drop index repeatly, use the same index params
        method: create index, drop index, four times
        expected: return code 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        # status, ids = connect.insert(collection, vectors)
        for i in range(2):
            status = connect.create_index(collection, index_type, index_param)
            assert status.OK()
            status, result = connect.get_index_info(collection)
            logging.getLogger().info(result)
            status = connect.drop_index(collection)
            assert status.OK()
            status, result = connect.get_index_info(collection)
            logging.getLogger().info(result)
            assert result._collection_name == collection
            assert result._index_type == IndexType.FLAT

    def test_create_drop_index_repeatly_different_index_params(self, connect, collection):
        '''
        target: test create / drop index repeatly, use the different index params
        method: create index, drop index, four times, each tme use different index_params to create index
        expected: return code 0
        '''
        nlist = NLIST
        indexs = [{"index_type": IndexType.IVFLAT, "index_param": {"nlist": nlist}}, {"index_type": IndexType.IVF_SQ8, "index_param": {"nlist": nlist}}]
        # status, ids = connect.insert(collection, vectors)
        for i in range(2):
            status = connect.create_index(collection, indexs[i]["index_type"], indexs[i]["index_param"])
            assert status.OK()
            status, result = connect.get_index_info(collection)
            logging.getLogger().info(result)
            status = connect.drop_index(collection)
            assert status.OK()
            status, result = connect.get_index_info(collection)
            logging.getLogger().info(result)
            assert result._collection_name == collection
            assert result._index_type == IndexType.FLAT


class TestIndexIP:
    @pytest.fixture(
        scope="function",
        params=gen_index()
    )
    def get_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        if request.param["index_type"] == IndexType.RNSG:
            pytest.skip("rnsg not support in ip")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        if request.param["index_type"] == IndexType.RNSG:
            pytest.skip("rnsg not support in ip")
        return request.param
    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """
    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, ip_collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status, ids = connect.insert(ip_collection, vectors)
        status = connect.create_index(ip_collection, index_type, index_param)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_collection(self, connect, ip_collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection, create partition, and add vectors in it, create index on collection
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(ip_collection, tag)
        status, ids = connect.insert(ip_collection, vectors, partition_tag=tag)
        status = connect.create_index(ip_collection, index_type, index_param)
        assert status.OK()

    # @pytest.mark.level(2)
    # def test_create_index_without_connect(self, dis_connect, ip_collection):
    #     '''
    #     target: test create index without connection
    #     method: create collection and add vectors in it, check if added successfully
    #     expected: raise exception
    #     '''
    #     nlist = NLIST
    #     index_type = IndexType.IVF_SQ8
    #     index_param = {"nlist": nlist}
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.create_index(ip_collection, index_type, index_param)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, ip_collection, get_simple_index):
        '''
        target: test create index interface, search with more query vectors
        method: create collection and add vectors in it, create index, with no manual flush 
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status, ids = connect.insert(ip_collection, vectors)
        status = connect.create_index(ip_collection, index_type, index_param)
        logging.getLogger().info(connect.get_index_info(ip_collection))
        query_vecs = [vectors[0], vectors[1], vectors[2]]
        top_k = 5
        search_param = get_search_param(index_type)
        status, result = connect.search(ip_collection, top_k, query_vecs, params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == len(query_vecs)

    # TODO: enable
    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.level(2)
    def _test_create_index_multiprocessing(self, connect, ip_collection, args):
        '''
        target: test create index interface with multiprocess
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        status, ids = connect.insert(ip_collection, vectors)
        def build(connect):
            status = connect.create_index(ip_collection, IndexType.IVFLAT, {"nlist": NLIST})
            assert status.OK()

        process_num = 8
        processes = []

        for i in range(process_num):
            m = get_milvus(args["ip"], args["port"], handler=args["handler"])
            p = Process(target=build, args=(m,))
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()

        query_vec = [vectors[0]]
        top_k = 1
        search_param = {"nprobe": nprobe}
        status, result = connect.search(ip_collection, top_k, query_vec, params=search_param)
        assert len(result) == 1
        assert len(result[0]) == top_k
        assert result[0][0].distance == 0.0

    # TODO: enable
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def _test_create_index_multiprocessing_multicollection(self, connect, args):
        '''
        target: test create index interface with multiprocess
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        process_num = 8
        loop_num = 8
        processes = []

        collection = []
        j = 0
        while j < (process_num*loop_num):
            collection_name = gen_unique_str("test_create_index_multiprocessing")
            collection.append(collection_name)
            param = {'collection_name': collection_name,
                    'dimension': dim}
            connect.create_collection(param)
            j = j + 1

        def create_index():
            i = 0
            while i < loop_num:
                # assert connect.has_collection(collection[ids*process_num+i])
                status, ids = connect.insert(collection[ids*process_num+i], vectors)

                status = connect.create_index(collection[ids*process_num+i], IndexType.IVFLAT, {"nlist": NLIST})
                assert status.OK()
                query_vec = [vectors[0]]
                top_k = 1
                search_param = {"nprobe": nprobe}
                status, result = connect.search(collection[ids*process_num+i], top_k, query_vec, params=search_param)
                assert len(result) == 1
                assert len(result[0]) == top_k
                assert result[0][0].distance == 0.0
                i = i + 1

        for i in range(process_num):
            m = get_milvus(args["ip"], args["port"], handler=args["handler"])
            ids = i
            p = Process(target=create_index, args=(m,ids))
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()

    def test_create_index_no_vectors(self, connect, ip_collection):
        '''
        target: test create index interface when there is no vectors in collection
        method: create collection and add no vectors in it, and then create index
        expected: return code equals to 0
        '''
        nlist = NLIST
        index_type = IndexType.IVF_SQ8
        index_param = {"nlist": nlist}
        status = connect.create_index(ip_collection, index_type, index_param)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors_then_insert(self, connect, ip_collection, get_simple_index):
        '''
        target: test create index interface when there is no vectors in collection, and does not affect the subsequent process
        method: create collection and add no vectors in it, and then create index, add vectors in it
        expected: return code equals to 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(ip_collection, index_type, index_param)
        status, ids = connect.insert(ip_collection, vectors)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_same_index_repeatedly(self, connect, ip_collection):
        '''
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: return code success, and search ok
        '''
        nlist = NLIST
        status, ids = connect.insert(ip_collection, vectors)
        index_type = IndexType.IVF_SQ8
        index_param = {"nlist": nlist}
        status = connect.create_index(ip_collection, index_type, index_param)
        status = connect.create_index(ip_collection, index_type, index_param)
        assert status.OK()
        query_vec = [vectors[0]]
        top_k = 1
        search_param = {"nprobe": nprobe}
        status, result = connect.search(ip_collection, top_k, query_vec, params=search_param)
        assert len(result) == 1
        assert len(result[0]) == top_k

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_different_index_repeatedly(self, connect, ip_collection):
        '''
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: return code 0, and describe index result equals with the second index params
        '''
        nlist = NLIST
        status, ids = connect.insert(ip_collection, vectors)
        index_type_1 = IndexType.IVF_SQ8
        index_type_2 = IndexType.IVFLAT
        indexs = [{"index_type": index_type_1, "index_param": {"nlist": nlist}}, {"index_type": index_type_2, "index_param": {"nlist": nlist}}]
        logging.getLogger().info(indexs)
        for index in indexs:
            status = connect.create_index(ip_collection, index["index_type"], index["index_param"])
            assert status.OK()
        status, result = connect.get_index_info(ip_collection)
        assert result._params["nlist"] == nlist
        assert result._collection_name == ip_collection
        assert result._index_type == index_type_2

    """
    ******************************************************************
      The following cases are used to test `get_index_info` function
    ******************************************************************
    """

    def test_get_index_info(self, connect, ip_collection, get_simple_index):
        '''
        target: test describe index interface
        method: create collection and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        # status, ids = connect.insert(ip_collection, vectors[:5000])
        status = connect.create_index(ip_collection, index_type, index_param)
        status, result = connect.get_index_info(ip_collection)
        logging.getLogger().info(result)
        assert result._collection_name == ip_collection
        status, mode = connect._cmd("mode")
        if str(mode) == "GPU" and index_type == IndexType.IVF_PQ:
            assert result._index_type == IndexType.FLAT
            assert result._params["nlist"] == NLIST
        else:
            assert result._index_type == index_type
            assert result._params == index_param

    def test_get_index_info_partition(self, connect, ip_collection, get_simple_index):
        '''
        target: test describe index interface
        method: create collection, create partition and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(ip_collection, tag)
        status, ids = connect.insert(ip_collection, vectors, partition_tag=tag)
        status = connect.create_index(ip_collection, index_type, index_param)
        status, result = connect.get_index_info(ip_collection)
        logging.getLogger().info(result)
        assert result._params == index_param
        assert result._collection_name == ip_collection
        assert result._index_type == index_type

    def test_get_index_info_partition_A(self, connect, ip_collection, get_simple_index):
        '''
        target: test describe index interface
        method: create collection, create partitions and add vectors in it, create index on partitions, call describe index
        expected: return code 0, and index instructure
        '''
        new_tag = "new_tag"
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(ip_collection, tag)
        status = connect.create_partition(ip_collection, new_tag)
        # status, ids = connect.insert(ip_collection, vectors, partition_tag=tag)
        # status, ids = connect.insert(ip_collection, vectors, partition_tag=new_tag)
        status = connect.create_index(ip_collection, index_type, index_param)
        status, result = connect.get_index_info(ip_collection)
        logging.getLogger().info(result)
        assert result._params == index_param
        assert result._collection_name == ip_collection
        assert result._index_type == index_type

    def test_describe_and_drop_index_multi_collections(self, connect, get_simple_index):
        '''
        target: test create, describe and drop index interface with multiple collections of IP
        method: create collections and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        collection_list = []
        for i in range(10):
            collection_name = gen_unique_str()
            collection_list.append(collection_name)
            param = {'collection_name': collection_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.IP}
            connect.create_collection(param)
            index_param = get_simple_index["index_param"]
            index_type = get_simple_index["index_type"]
            logging.getLogger().info(get_simple_index)
            status, ids = connect.insert(collection_name=collection_name, records=vectors)
            status = connect.create_index(collection_name, index_type, index_param)
            assert status.OK()
        for i in range(10):
            status, result = connect.get_index_info(collection_list[i])
            logging.getLogger().info(result)
            assert result._params == index_param
            assert result._collection_name == collection_list[i]
            assert result._index_type == index_type
        for i in range(10):
            status = connect.drop_index(collection_list[i])
            assert status.OK()
            status, result = connect.get_index_info(collection_list[i])
            logging.getLogger().info(result)
            assert result._collection_name == collection_list[i]
            assert result._index_type == IndexType.FLAT

    # @pytest.mark.level(2)
    # def test_get_index_info_without_connect(self, dis_connect, ip_collection):
    #     '''
    #     target: test describe index without connection
    #     method: describe index, and check if describe successfully
    #     expected: raise exception
    #     '''
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.get_index_info(ip_collection)

    def test_get_index_info_not_create(self, connect, ip_collection):
        '''
        target: test describe index interface when index not created
        method: create collection and add vectors in it, create index
            , make sure the collection name not in index
        expected: return code not equals to 0, describe index failed
        '''
        status, ids = connect.insert(ip_collection, vectors)
        status, result = connect.get_index_info(ip_collection)
        logging.getLogger().info(result)
        assert status.OK()
        # assert result._params["nlist"] == index_params["nlist"]
        # assert result._collection_name == collection
        # assert result._index_type == index_params["index_type"]

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """

    def test_drop_index(self, connect, ip_collection, get_simple_index):
        '''
        target: test drop index interface
        method: create collection and add vectors in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status, mode = connect._cmd("mode")
        assert status.OK()
        # status, ids = connect.insert(ip_collection, vectors)
        status = connect.create_index(ip_collection, index_type, index_param)
        if str(mode) == "GPU" and (index_type == IndexType.IVF_PQ):
            assert not status.OK()
        else:
            assert status.OK()
        status, result = connect.get_index_info(ip_collection)
        logging.getLogger().info(result)
        status = connect.drop_index(ip_collection)
        assert status.OK()
        status, result = connect.get_index_info(ip_collection)
        logging.getLogger().info(result)
        assert result._collection_name == ip_collection
        assert result._index_type == IndexType.FLAT

    def test_drop_index_partition(self, connect, ip_collection, get_simple_index):
        '''
        target: test drop index interface
        method: create collection, create partition and add vectors in it, create index on collection, call drop collection index
        expected: return code 0, and default index param
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_partition(ip_collection, tag)
        status, ids = connect.insert(ip_collection, vectors, partition_tag=tag)
        status = connect.create_index(ip_collection, index_type, index_param)
        assert status.OK()
        status, result = connect.get_index_info(ip_collection)
        logging.getLogger().info(result)
        status = connect.drop_index(ip_collection)
        assert status.OK()
        status, result = connect.get_index_info(ip_collection)
        logging.getLogger().info(result)
        assert result._collection_name == ip_collection
        assert result._index_type == IndexType.FLAT

    def test_drop_index_partition_C(self, connect, ip_collection, get_simple_index):
        '''
        target: test drop index interface
        method: create collection, create partitions and add vectors in it, create index on partitions, call drop partition index
        expected: return code 0, and default index param
        '''
        new_tag = "new_tag"
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_partition(ip_collection, tag)
        status = connect.create_partition(ip_collection, new_tag)
        status, ids = connect.insert(ip_collection, vectors)
        status = connect.create_index(ip_collection, index_type, index_param)
        assert status.OK()
        status = connect.drop_index(ip_collection)
        assert status.OK()
        status, result = connect.get_index_info(ip_collection)
        logging.getLogger().info(result)
        assert result._collection_name == ip_collection
        assert result._index_type == IndexType.FLAT

    @pytest.mark.level(2)
    def test_drop_index_repeatly(self, connect, ip_collection, get_simple_index):
        '''
        target: test drop index repeatly
        method: create index, call drop index, and drop again
        expected: return code 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        # status, ids = connect.insert(ip_collection, vectors)
        status, mode = connect._cmd("mode")
        assert status.OK()
        # status, ids = connect.insert(ip_collection, vectors)
        status = connect.create_index(ip_collection, index_type, index_param)
        if str(mode) == "GPU" and (index_type == IndexType.IVF_PQ):
            assert not status.OK()
        else:
            assert status.OK()        
        status, result = connect.get_index_info(ip_collection)
        logging.getLogger().info(result)
        status = connect.drop_index(ip_collection)
        assert status.OK()
        status = connect.drop_index(ip_collection)
        assert status.OK()
        status, result = connect.get_index_info(ip_collection)
        logging.getLogger().info(result)
        assert result._collection_name == ip_collection
        assert result._index_type == IndexType.FLAT

    # @pytest.mark.level(2)
    # def test_drop_index_without_connect(self, dis_connect, ip_collection):
    #     '''
    #     target: test drop index without connection
    #     method: drop index, and check if drop successfully
    #     expected: raise exception
    #     '''
    #     nlist = NLIST
    #     index_type = IndexType.IVFLAT
    #     index_param = {"nlist": nlist}
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.drop_index(ip_collection, index_type, index_param)

    def test_drop_index_collection_not_create(self, connect, ip_collection):
        '''
        target: test drop index interface when index not created
        method: create collection and add vectors in it, create index
        expected: return code not equals to 0, drop index failed
        '''
        status, ids = connect.insert(ip_collection, vectors)
        status, result = connect.get_index_info(ip_collection)
        logging.getLogger().info(result)
        # no create index
        status = connect.drop_index(ip_collection)
        logging.getLogger().info(status)
        assert status.OK()

    @pytest.mark.level(2)
    def test_create_drop_index_repeatly(self, connect, ip_collection, get_simple_index):
        '''
        target: test create / drop index repeatly, use the same index params
        method: create index, drop index, four times
        expected: return code 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status, ids = connect.insert(ip_collection, vectors)
        for i in range(2):
            status = connect.create_index(ip_collection, index_type, index_param)
            assert status.OK()
            status, result = connect.get_index_info(ip_collection)
            logging.getLogger().info(result)
            status = connect.drop_index(ip_collection)
            assert status.OK()
            status, result = connect.get_index_info(ip_collection)
            logging.getLogger().info(result)
            assert result._collection_name == ip_collection
            assert result._index_type == IndexType.FLAT

    def test_create_drop_index_repeatly_different_index_params(self, connect, ip_collection):
        '''
        target: test create / drop index repeatly, use the different index params
        method: create index, drop index, four times, each tme use different index_params to create index
        expected: return code 0
        '''
        nlist = NLIST
        indexs = [{"index_type": IndexType.IVFLAT, "index_param": {"nlist": nlist}}, {"index_type": IndexType.IVF_SQ8, "index_param": {"nlist": nlist}}]
        status, ids = connect.insert(ip_collection, vectors)
        for i in range(2):
            status = connect.create_index(ip_collection, indexs[i]["index_type"], indexs[i]["index_param"])
            assert status.OK()
            status, result = connect.get_index_info(ip_collection)
            assert result._params == indexs[i]["index_param"]
            assert result._collection_name == ip_collection
            assert result._index_type == indexs[i]["index_type"]
            status, result = connect.get_index_info(ip_collection)
            logging.getLogger().info(result)
            status = connect.drop_index(ip_collection)
            assert status.OK()
            status, result = connect.get_index_info(ip_collection)
            logging.getLogger().info(result)
            assert result._collection_name == ip_collection
            assert result._index_type == IndexType.FLAT


class TestIndexJAC:
    tmp, vectors = gen_binary_vectors(nb, dim)

    @pytest.fixture(
        scope="function",
        params=gen_index()
    )
    def get_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_jaccard_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] == IndexType.IVFLAT or request.param["index_type"] == IndexType.FLAT:
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, jac_collection, get_jaccard_index):
        '''
        target: test create index interface
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        logging.getLogger().info(get_jaccard_index)
        status, ids = connect.insert(jac_collection, self.vectors)
        status = connect.create_index(jac_collection, index_type, index_param)
        if index_type != IndexType.FLAT and index_type != IndexType.IVFLAT:
            assert not status.OK()
        else:
            assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition(self, connect, jac_collection, get_jaccard_index):
        '''
        target: test create index interface
        method: create collection, create partition, and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        logging.getLogger().info(get_jaccard_index)
        status = connect.create_partition(jac_collection, tag)
        status, ids = connect.insert(jac_collection, self.vectors, partition_tag=tag)
        status = connect.create_index(jac_collection, index_type, index_param)
        assert status.OK()

    # @pytest.mark.level(2)
    # def test_create_index_without_connect(self, dis_connect, jac_collection):
    #     '''
    #     target: test create index without connection
    #     method: create collection and add vectors in it, check if added successfully
    #     expected: raise exception
    #     '''
    #     nlist = NLIST
    #     index_param = {"nlist": nlist}
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.create_index(jac_collection, IndexType.IVF_SQ8, index_param)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, jac_collection, get_jaccard_index):
        '''
        target: test create index interface, search with more query vectors
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        logging.getLogger().info(get_jaccard_index)
        status, ids = connect.insert(jac_collection, self.vectors)
        status = connect.create_index(jac_collection, index_type, index_param)
        logging.getLogger().info(connect.get_index_info(jac_collection))
        query_vecs = [self.vectors[0], self.vectors[1], self.vectors[2]]
        top_k = 5
        search_param = get_search_param(index_type)
        status, result = connect.search(jac_collection, top_k, query_vecs, params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == len(query_vecs)

    """
    ******************************************************************
      The following cases are used to test `get_index_info` function
    ******************************************************************
    """

    def test_get_index_info(self, connect, jac_collection, get_jaccard_index):
        '''
        target: test describe index interface
        method: create collection and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        logging.getLogger().info(get_jaccard_index)
        # status, ids = connect.insert(jac_collection, vectors[:5000])
        status = connect.create_index(jac_collection, index_type, index_param)
        status, result = connect.get_index_info(jac_collection)
        logging.getLogger().info(result)
        assert result._collection_name == jac_collection
        assert result._index_type == index_type
        assert result._params == index_param

    def test_get_index_info_partition(self, connect, jac_collection, get_jaccard_index):
        '''
        target: test describe index interface
        method: create collection, create partition and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        logging.getLogger().info(get_jaccard_index)
        status = connect.create_partition(jac_collection, tag)
        status, ids = connect.insert(jac_collection, vectors, partition_tag=tag)
        status = connect.create_index(jac_collection, index_type, index_param)
        status, result = connect.get_index_info(jac_collection)
        logging.getLogger().info(result)
        assert result._params == index_param
        assert result._collection_name == jac_collection
        assert result._index_type == index_type

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """

    def test_drop_index(self, connect, jac_collection, get_jaccard_index):
        '''
        target: test drop index interface
        method: create collection and add vectors in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        status, mode = connect._cmd("mode")
        assert status.OK()
        # status, ids = connect.insert(ip_collection, vectors)
        status = connect.create_index(jac_collection, index_type, index_param)
        assert status.OK()
        status, result = connect.get_index_info(jac_collection)
        logging.getLogger().info(result)
        status = connect.drop_index(jac_collection)
        assert status.OK()
        status, result = connect.get_index_info(jac_collection)
        logging.getLogger().info(result)
        assert result._collection_name == jac_collection
        assert result._index_type == IndexType.FLAT

    def test_drop_index_partition(self, connect, jac_collection, get_jaccard_index):
        '''
        target: test drop index interface
        method: create collection, create partition and add vectors in it, create index on collection, call drop collection index
        expected: return code 0, and default index param
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        status = connect.create_partition(jac_collection, tag)
        status, ids = connect.insert(jac_collection, vectors, partition_tag=tag)
        status = connect.create_index(jac_collection, index_type, index_param)
        assert status.OK()
        status, result = connect.get_index_info(jac_collection)
        logging.getLogger().info(result)
        status = connect.drop_index(jac_collection)
        assert status.OK()
        status, result = connect.get_index_info(jac_collection)
        logging.getLogger().info(result)
        assert result._collection_name == jac_collection
        assert result._index_type == IndexType.FLAT


class TestIndexBinary:
    tmp, vectors = gen_binary_vectors(nb, dim)

    @pytest.fixture(
        scope="function",
        params=gen_index()
    )
    def get_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if request.param["index_type"] == IndexType.IVF_PQ or request.param["index_type"] == IndexType.HNSW:
            pytest.skip("Skip PQ Temporary")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if request.param["index_type"] == IndexType.IVF_PQ or request.param["index_type"] == IndexType.HNSW:
            pytest.skip("Skip PQ Temporary")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_hamming_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] == IndexType.IVFLAT or request.param["index_type"] == IndexType.FLAT:
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_substructure_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] == IndexType.FLAT:
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_superstructure_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] == IndexType.FLAT:
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, ham_collection, get_hamming_index):
        '''
        target: test create index interface
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        logging.getLogger().info(get_hamming_index)
        status, ids = connect.insert(ham_collection, self.vectors)
        status = connect.create_index(ham_collection, index_type, index_param)
        if index_type != IndexType.FLAT and index_type != IndexType.IVFLAT:
            assert not status.OK()
        else:
            assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition(self, connect, ham_collection, get_hamming_index):
        '''
        target: test create index interface
        method: create collection, create partition, and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        logging.getLogger().info(get_hamming_index)
        status = connect.create_partition(ham_collection, tag)
        status, ids = connect.insert(ham_collection, self.vectors, partition_tag=tag)
        status = connect.create_index(ham_collection, index_type, index_param)
        assert status.OK()
        status, res = connect.count_entities(ham_collection)
        assert res == len(self.vectors)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition_structure(self, connect, substructure_collection, get_substructure_index):
        '''
        target: test create index interface
        method: create collection, create partition, and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_substructure_index["index_param"]
        index_type = get_substructure_index["index_type"]
        logging.getLogger().info(get_substructure_index)
        status = connect.create_partition(substructure_collection, tag)
        status, ids = connect.insert(substructure_collection, self.vectors, partition_tag=tag)
        status = connect.create_index(substructure_collection, index_type, index_param)
        assert status.OK()
        status, res = connect.count_entities(substructure_collection,)
        assert res == len(self.vectors)

    # @pytest.mark.level(2)
    # def test_create_index_without_connect(self, dis_connect, ham_collection):
    #     '''
    #     target: test create index without connection
    #     method: create collection and add vectors in it, check if added successfully
    #     expected: raise exception
    #     '''
    #     nlist = NLIST
    #     index_param = {"nlist": nlist}
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.create_index(ham_collection, IndexType.IVF_SQ8, index_param)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, ham_collection, get_hamming_index):
        '''
        target: test create index interface, search with more query vectors
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        logging.getLogger().info(get_hamming_index)
        status, ids = connect.insert(ham_collection, self.vectors)
        status = connect.create_index(ham_collection,  index_type, index_param)
        logging.getLogger().info(connect.get_index_info(ham_collection))
        query_vecs = [self.vectors[0], self.vectors[1], self.vectors[2]]
        top_k = 5
        search_param = get_search_param(index_type)
        status, result = connect.search(ham_collection, top_k, query_vecs, params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == len(query_vecs)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors_superstructure(self, connect, superstructure_collection, get_superstructure_index):
        '''
        target: test create index interface, search with more query vectors
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_superstructure_index["index_param"]
        index_type = get_superstructure_index["index_type"]
        logging.getLogger().info(get_superstructure_index)
        status, ids = connect.insert(superstructure_collection, self.vectors)
        status = connect.create_index(superstructure_collection, index_type, index_param)
        logging.getLogger().info(connect.get_index_info(superstructure_collection))
        query_vecs = [self.vectors[0], self.vectors[1], self.vectors[2]]
        top_k = 5
        search_param = get_search_param(index_type)
        status, result = connect.search(superstructure_collection, top_k, query_vecs, params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == len(query_vecs)

    """
    ******************************************************************
      The following cases are used to test `get_index_info` function
    ******************************************************************
    """

    def test_get_index_info(self, connect, ham_collection, get_hamming_index):
        '''
        target: test describe index interface
        method: create collection and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        logging.getLogger().info(get_hamming_index)
        # status, ids = connect.insert(jac_collection, vectors[:5000])
        status = connect.create_index(ham_collection, index_type, index_param)
        status, result = connect.get_index_info(ham_collection)
        logging.getLogger().info(result)
        assert result._collection_name == ham_collection
        assert result._index_type == index_type
        assert result._params == index_param

    def test_get_index_info_partition(self, connect, ham_collection, get_hamming_index):
        '''
        target: test describe index interface
        method: create collection, create partition and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        logging.getLogger().info(get_hamming_index)
        status = connect.create_partition(ham_collection, tag)
        status, ids = connect.insert(ham_collection, vectors, partition_tag=tag)
        status = connect.create_index(ham_collection, index_type, index_param)
        status, result = connect.get_index_info(ham_collection)
        logging.getLogger().info(result)
        assert result._params == index_param
        assert result._collection_name == ham_collection
        assert result._index_type == index_type

    def test_get_index_info_partition_superstructrue(self, connect, superstructure_collection, get_superstructure_index):
        '''
        target: test describe index interface
        method: create collection, create partition and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_superstructure_index["index_param"]
        index_type = get_superstructure_index["index_type"]
        logging.getLogger().info(get_superstructure_index)
        status = connect.create_partition(superstructure_collection, tag)
        status, ids = connect.insert(superstructure_collection, vectors, partition_tag=tag)
        status = connect.create_index(superstructure_collection, index_type, index_param)
        status, result = connect.get_index_info(superstructure_collection)
        logging.getLogger().info(result)
        assert result._params == index_param
        assert result._collection_name == superstructure_collection
        assert result._index_type == index_type

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """

    def test_drop_index(self, connect, ham_collection, get_hamming_index):
        '''
        target: test drop index interface
        method: create collection and add vectors in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        status, mode = connect._cmd("mode")
        assert status.OK()
        # status, ids = connect.insert(ip_collection, vectors)
        status = connect.create_index(ham_collection, index_type, index_param)
        assert status.OK()
        status, result = connect.get_index_info(ham_collection)
        logging.getLogger().info(result)
        status = connect.drop_index(ham_collection)
        assert status.OK()
        status, result = connect.get_index_info(ham_collection)
        logging.getLogger().info(result)
        assert result._collection_name == ham_collection
        assert result._index_type == IndexType.FLAT

    def test_drop_index_substructure(self, connect, substructure_collection, get_substructure_index):
        '''
        target: test drop index interface
        method: create collection and add vectors in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        index_param = get_substructure_index["index_param"]
        index_type = get_substructure_index["index_type"]
        status, mode = connect._cmd("mode")
        assert status.OK()
        status = connect.create_index(substructure_collection, index_type, index_param)
        assert status.OK()
        status, result = connect.get_index_info(substructure_collection)
        logging.getLogger().info(result)
        status = connect.drop_index(substructure_collection)
        assert status.OK()
        status, result = connect.get_index_info(substructure_collection)
        logging.getLogger().info(result)
        assert result._collection_name == substructure_collection
        assert result._index_type == IndexType.FLAT

    def test_drop_index_partition(self, connect, ham_collection, get_hamming_index):
        '''
        target: test drop index interface
        method: create collection, create partition and add vectors in it, create index on collection, call drop collection index
        expected: return code 0, and default index param
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        status = connect.create_partition(ham_collection, tag)
        status, ids = connect.insert(ham_collection, vectors, partition_tag=tag)
        status = connect.create_index(ham_collection, index_type, index_param)
        assert status.OK()
        status, result = connect.get_index_info(ham_collection)
        logging.getLogger().info(result)
        status = connect.drop_index(ham_collection)
        assert status.OK()
        status, result = connect.get_index_info(ham_collection)
        logging.getLogger().info(result)
        assert result._collection_name == ham_collection
        assert result._index_type == IndexType.FLAT

class TestIndexCollectionInvalid(object):
    """
    Test create / describe / drop index interfaces with invalid collection names
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_collection_names()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.level(1)
    def test_create_index_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        nlist = NLIST
        index_param = {"nlist": nlist}
        status = connect.create_index(collection_name, IndexType.IVF_SQ8, index_param)
        assert not status.OK()

    @pytest.mark.level(1)
    def test_get_index_info_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        status, result = connect.get_index_info(collection_name)
        assert not status.OK()   

    @pytest.mark.level(1)
    def test_drop_index_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        status = connect.drop_index(collection_name)
        assert not status.OK()


class TestCreateIndexParamsInvalid(object):
    """
    Test Building index with invalid collection names, collection names not in db
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_index()
    )
    def get_index(self, request):
        yield request.param

    @pytest.mark.level(1)
    def test_create_index_with_invalid_index_params(self, connect, collection, get_index):
        index_param = get_index["index_param"]
        index_type = get_index["index_type"]
        logging.getLogger().info(get_index)
        # status, ids = connect.insert(collection, vectors)
        if (not index_type) or (not isinstance(index_type, IndexType)):
            with pytest.raises(Exception) as e:
                status = connect.create_index(collection, index_type, index_param)
        else:
            status = connect.create_index(collection, index_type, index_param)
            assert not status.OK()

    """
    Test Building index with invalid nlist
    """
    @pytest.fixture(
        scope="function",
        params=[IndexType.FLAT,IndexType.IVFLAT,IndexType.IVF_SQ8,IndexType.IVF_SQ8H]
    )
    def get_index_type(self, request):
        yield request.param

    def test_create_index_with_invalid_nlist(self, connect, collection, get_index_type):
        status, ids = connect.insert(collection, vectors)
        status = connect.create_index(collection, get_index_type, {"nlist": INVALID_NLIST})
        if get_index_type != IndexType.FLAT:
            assert not status.OK()

    '''
    Test Building index with empty params
    '''
    def test_create_index_with_empty_param(self, connect, collection, get_index_type):
        logging.getLogger().info(get_index_type)
        status = connect.create_index(collection, get_index_type, {})
        if get_index_type != IndexType.FLAT :
            assert not status.OK()
        status, result = connect.get_index_info(collection)
        logging.getLogger().info(result)
        assert result._collection_name == collection
        assert result._index_type == IndexType.FLAT

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
        params=gen_index()
    )
    def get_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            # if request.param["index_type"] == IndexType.IVF_PQ:
            if request.param["index_type"] not in [IndexType.IVF_FLAT]:
                # pytest.skip("ivfpq not support in GPU mode")
                pytest.skip("debug ivf_flat in GPU mode")
        return request.param

    def check_status(self, status):
        logging.getLogger().info("In callback check status")
        assert status.OK()

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        vectors = gen_vectors(nb, dim)
        status, ids = connect.insert(collection, vectors)
        logging.getLogger().info("start index")
        # future = connect.create_index(collection, index_type, index_param, _async=True, _callback=self.check_status) 
        future = connect.create_index(collection, index_type, index_param, _async=True) 
        logging.getLogger().info("before result")
        status = future.result()
        assert status.OK()

    def test_create_index_with_invalid_collectionname(self, connect):
        collection_name = " "
        nlist = NLIST
        index_param = {"nlist": nlist}
        future = connect.create_index(collection_name, IndexType.IVF_SQ8, index_param, _async=True)
        status = future.result()
        assert not status.OK()

