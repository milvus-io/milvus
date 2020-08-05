import logging
import time
import pdb
import threading
from multiprocessing import Pool, Process
import numpy
import pytest
import sklearn.preprocessing
from utils import *

nb = 6000
dim = 128
index_file_size = 10
BUILD_TIMEOUT = 300
nprobe = 1
top_k = 5
tag = "1970-01-01"
NLIST = 4046
INVALID_NLIST = 100000000
field_name = "float_vector"
binary_field_name = "binary_vector"
collection_id = "index"
default_index_type = "FLAT"
entity = gen_entities(1)
entities = gen_entities(nb)
raw_vector, binary_entity = gen_binary_entities(1)
raw_vectors, binary_entities = gen_binary_entities(nb)
query, query_vecs = gen_query_vectors(field_name, entities, top_k, 1)
default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 1024}, "metric_type": "L2"}


class TestIndexBase:
    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        logging.getLogger().info(request.param)
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("sq8h not support in CPU mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=[
            1,
            10,
            1500
        ],
    )
    def get_nq(self, request):
        yield request.param

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        ids = connect.insert(collection, entities)
        connect.create_index(collection, field_name, get_simple_index)

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        connect.create_index(collection, field_name, get_simple_index)

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        '''
        connect.create_partition(collection, tag)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush([collection])
        connect.create_index(collection, field_name, get_simple_index)

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition_flush(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        '''
        connect.create_partition(collection, tag)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush()
        connect.create_index(collection, field_name, get_simple_index)

    @pytest.mark.level(2)
    @pytest.mark.level(2)
    def test_create_index_without_connect(self, dis_connect, collection):
        '''
        target: test create index without connection
        method: create collection and add entities in it, check if added successfully
        expected: raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.create_index(collection, field_name, get_simple_index)

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, collection, get_simple_index, get_nq):
        '''
        target: test create index interface, search with more query vectors
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        ids = connect.insert(collection, entities)
        connect.create_index(collection, field_name, get_simple_index)
        logging.getLogger().info(connect.get_collection_stats(collection))
        nq = get_nq
        index_type = get_simple_index["index_type"]
        search_param = get_search_param(index_type)
        query, vecs = gen_query_vectors(field_name, entities, top_k, nq, search_params=search_param)
        res = connect.search(collection, query)
        assert len(res) == nq

    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.level(2)
    def test_create_index_multithread(self, connect, collection, args):
        '''
        target: test create index interface with multiprocess
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        ids = connect.insert(collection, entities)

        def build(connect):
            connect.create_index(collection, field_name, default_index)

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

    @pytest.mark.level(2)
    def test_create_index_collection_not_existed(self, connect):
        '''
        target: test create index interface when collection name not existed
        method: create collection and add entities in it, create index
            , make sure the collection name not in index
        expected: return code not equals to 0, create index failed
        '''
        collection_name = gen_unique_str(collection_id)
        with pytest.raises(Exception) as e:
            connect.create_index(collection, field_name, default_index)

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors_insert(self, connect, collection, get_simple_index):
        '''
        target: test create index interface when there is no vectors in collection, and does not affect the subsequent process
        method: create collection and add no vectors in it, and then create index, add entities in it
        expected: return code equals to 0
        '''
        connect.create_index(collection, field_name, get_simple_index)
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        count = connect.count_entities(collection)
        assert count == nb

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_same_index_repeatedly(self, connect, collection, get_simple_index):
        '''
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: return code success, and search ok
        '''
        connect.create_index(collection, field_name, get_simple_index)
        connect.create_index(collection, field_name, get_simple_index)

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_different_index_repeatedly(self, connect, collection):
        '''
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: return code 0, and describe index result equals with the second index params
        '''
        ids = connect.insert(collection, entities)
        indexs = [default_index, {"index_type": "FLAT", "nlist": 1024}]
        for index in indexs:
            connect.create_index(collection, field_name, index)
            stats = connect.get_collection_stats(collection)
            assert stats["partitions"][0]["segments"][0]["index_name"] == index["index_type"]
            assert stats["row_count"] == nb

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_ip(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        ids = connect.insert(collection, entities)
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors_ip(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition_ip(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        '''
        connect.create_partition(collection, tag)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush([collection])
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition_flush_ip(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        '''
        connect.create_partition(collection, tag)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush()
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors_ip(self, connect, collection, get_simple_index, get_nq):
        '''
        target: test create index interface, search with more query vectors
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        metric_type = "IP"
        ids = connect.insert(collection, entities)
        get_simple_index["metric_type"] = metric_type
        connect.create_index(collection, field_name, get_simple_index)
        logging.getLogger().info(connect.get_collection_stats(collection))
        nq = get_nq
        index_type = get_simple_index["index_type"]
        search_param = get_search_param(index_type)
        query, vecs = gen_query_vectors(field_name, entities, top_k, nq, metric_type=metric_type, search_params=search_param)
        res = connect.search(collection, query)
        assert len(res) == nq

    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.level(2)
    def test_create_index_multithread_ip(self, connect, collection, args):
        '''
        target: test create index interface with multiprocess
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        ids = connect.insert(collection, entities)

        def build(connect):
            default_index["metric_type"] = "IP"
            connect.create_index(collection, field_name, default_index)

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

    # TODO
    @pytest.mark.level(2)
    def test_create_index_collection_not_existed_ip(self, connect, collection):
        '''
        target: test create index interface when collection name not existed
        method: create collection and add entities in it, create index
            , make sure the collection name not in index
        expected: return code not equals to 0, create index failed
        '''
        collection_name = gen_unique_str(collection_id)
        default_index["metric_type"] = "IP"
        with pytest.raises(Exception) as e:
            connect.create_index(collection, field_name, default_index)

    # TODO
    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors_insert_ip(self, connect, collection, get_simple_index):
        '''
        target: test create index interface when there is no vectors in collection, and does not affect the subsequent process
        method: create collection and add no vectors in it, and then create index, add entities in it
        expected: return code equals to 0
        '''
        default_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        count = connect.count_entities(collection)
        assert count == nb

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_same_index_repeatedly_ip(self, connect, collection, get_simple_index):
        '''
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: return code success, and search ok
        '''
        default_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)
        connect.create_index(collection, field_name, get_simple_index)

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_different_index_repeatedly_ip(self, connect, collection):
        '''
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: return code 0, and describe index result equals with the second index params
        '''
        ids = connect.insert(collection, entities)
        indexs = [default_index, {"index_type": "FLAT", "params": {"nlist": 1024}, "metric_type": "IP"}]
        for index in indexs:
            connect.create_index(collection, field_name, index)
            stats = connect.get_collection_stats(collection)
            assert stats["partitions"][0]["segments"][0]["index_name"] == index["index_type"]
            assert stats["row_count"] == nb

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """

    @pytest.mark.level(2)
    def test_drop_index(self, connect, collection, get_simple_index):
        '''
        target: test drop index interface
        method: create collection and add entities in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        # ids = connect.insert(collection, entities)
        connect.create_index(collection, field_name, get_simple_index)
        connect.drop_index(collection, field_name)
        stats = connect.get_collection_stats(collection)
        # assert stats["partitions"][0]["segments"][0]["index_name"] == default_index_type
        assert not stats["partitions"][0]["segments"]

    @pytest.mark.level(2)
    def test_drop_index_repeatly(self, connect, collection, get_simple_index):
        '''
        target: test drop index repeatly
        method: create index, call drop index, and drop again
        expected: return code 0
        '''
        connect.create_index(collection, field_name, get_simple_index)
        stats = connect.get_collection_stats(collection)
        connect.drop_index(collection, field_name)
        connect.drop_index(collection, field_name)
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        # assert stats["partitions"][0]["segments"][0]["index_name"] == default_index_type
        assert not stats["partitions"][0]["segments"]

    @pytest.mark.level(2)
    def test_drop_index_without_connect(self, dis_connect, collection):
        '''
        target: test drop index without connection
        method: drop index, and check if drop successfully
        expected: raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.drop_index(collection, field_name)

    @pytest.mark.level(2)
    def test_drop_index_collection_not_existed(self, connect):
        '''
        target: test drop index interface when collection name not existed
        method: create collection and add entities in it, create index
            , make sure the collection name not in index, and then drop it
        expected: return code not equals to 0, drop index failed
        '''
        collection_name = gen_unique_str(collection_id)
        with pytest.raises(Exception) as e:
            connect.drop_index(collection_name, field_name)

    @pytest.mark.level(2)
    def test_drop_index_collection_not_create(self, connect, collection):
        '''
        target: test drop index interface when index not created
        method: create collection and add entities in it, create index
        expected: return code not equals to 0, drop index failed
        '''
        # ids = connect.insert(collection, entities)
        # no create index
        connect.drop_index(collection, field_name)

    @pytest.mark.level(2)
    def test_create_drop_index_repeatly(self, connect, collection, get_simple_index):
        '''
        target: test create / drop index repeatly, use the same index params
        method: create index, drop index, four times
        expected: return code 0
        '''
        for i in range(4):
            connect.create_index(collection, field_name, get_simple_index)
            connect.drop_index(collection, field_name)

    @pytest.mark.level(2)
    def test_drop_index_ip(self, connect, collection, get_simple_index):
        '''
        target: test drop index interface
        method: create collection and add entities in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        # ids = connect.insert(collection, entities)
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)
        connect.drop_index(collection, field_name)
        stats = connect.get_collection_stats(collection)
        # assert stats["partitions"][0]["segments"][0]["index_name"] == default_index_type
        assert not stats["partitions"][0]["segments"]

    @pytest.mark.level(2)
    def test_drop_index_repeatly_ip(self, connect, collection, get_simple_index):
        '''
        target: test drop index repeatly
        method: create index, call drop index, and drop again
        expected: return code 0
        '''
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)
        stats = connect.get_collection_stats(collection)
        connect.drop_index(collection, field_name)
        connect.drop_index(collection, field_name)
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        # assert stats["partitions"][0]["segments"][0]["index_name"] == default_index_type
        assert not stats["partitions"][0]["segments"]

    @pytest.mark.level(2)
    def test_drop_index_without_connect_ip(self, dis_connect, collection):
        '''
        target: test drop index without connection
        method: drop index, and check if drop successfully
        expected: raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.drop_index(collection, field_name)

    @pytest.mark.level(2)
    def test_drop_index_collection_not_create_ip(self, connect, collection):
        '''
        target: test drop index interface when index not created
        method: create collection and add entities in it, create index
        expected: return code not equals to 0, drop index failed
        '''
        # ids = connect.insert(collection, entities)
        # no create index
        connect.drop_index(collection, field_name)

    @pytest.mark.level(2)
    def test_create_drop_index_repeatly_ip(self, connect, collection, get_simple_index):
        '''
        target: test create / drop index repeatly, use the same index params
        method: create index, drop index, four times
        expected: return code 0
        '''
        get_simple_index["metric_type"] = "IP"
        for i in range(4):
            connect.create_index(collection, field_name, get_simple_index)
            connect.drop_index(collection, field_name)


class TestIndexBinary:
    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("sq8h not support in CPU mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_jaccard_index(self, request, connect):
        if request.param["index_type"] in binary_support():
            request.param["metric_type"] = "JACCARD"
            return request.param
        else:
            pytest.skip("Skip index")

    @pytest.fixture(
        scope="function",
        params=[
            1,
            10,
            1500
        ],
    )
    def get_nq(self, request):
        yield request.param

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, binary_collection, get_jaccard_index):
        '''
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        ids = connect.insert(binary_collection, binary_entities)
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition(self, connect, binary_collection, get_jaccard_index):
        '''
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        '''
        connect.create_partition(binary_collection, tag)
        ids = connect.insert(binary_collection, binary_entities, partition_tag=tag)
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)

    # TODO:
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def _test_create_index_search_with_query_vectors(self, connect, binary_collection, get_jaccard_index, get_nq):
        '''
        target: test create index interface, search with more query vectors
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        nq = get_nq
        pdb.set_trace()
        ids = connect.insert(binary_collection, binary_entities)
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)
        query, vecs = gen_query_vectors(binary_field_name, binary_entities, top_k, nq)
        search_param = get_search_param(binary_collection["index_type"])
        res = connect.search(binary_collection, query, search_params=search_param)
        logging.getLogger().info(res)
        assert len(res) == nq

    """
    ******************************************************************
      The following cases are used to test `get_index_info` function
    ******************************************************************
    """

    def test_get_index_info(self, connect, binary_collection, get_jaccard_index):
        '''
        target: test describe index interface
        method: create collection and add entities in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        if get_jaccard_index["index_type"] == "BIN_FLAT":
            pytest.skip("GetCollectionStats skip BIN_FLAT")
        ids = connect.insert(binary_collection, binary_entities)
        connect.flush([binary_collection])
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)
        stats = connect.get_collection_stats(binary_collection)
        logging.getLogger().info(stats)
        assert stats['partitions'][0]['segments'][0]['index_name'] == get_jaccard_index['index_type']

    def test_get_index_info_partition(self, connect, binary_collection, get_jaccard_index):
        '''
        target: test describe index interface
        method: create collection, create partition and add entities in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        if get_jaccard_index["index_type"] == "BIN_FLAT":
            pytest.skip("GetCollectionStats skip BIN_FLAT")
        connect.create_partition(binary_collection, tag)
        ids = connect.insert(binary_collection, binary_entities, partition_tag=tag)
        connect.flush([binary_collection])
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)
        stats = connect.get_collection_stats(binary_collection)
        logging.getLogger().info(stats)
        assert stats['partitions'][1]['segments'][0]['index_name'] == get_jaccard_index['index_type']

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """

    def test_drop_index(self, connect, binary_collection, get_jaccard_index):
        '''
        target: test drop index interface
        method: create collection and add entities in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)
        stats = connect.get_collection_stats(binary_collection)
        logging.getLogger().info(stats)
        connect.drop_index(binary_collection, binary_field_name)
        stats = connect.get_collection_stats(binary_collection)
        # assert stats["partitions"][0]["segments"][0]["index_name"] == default_index_type
        assert not stats["partitions"][0]["segments"]

    def test_drop_index_partition(self, connect, binary_collection, get_jaccard_index):
        '''
        target: test drop index interface
        method: create collection, create partition and add entities in it, create index on collection, call drop collection index
        expected: return code 0, and default index param
        '''
        connect.create_partition(binary_collection, tag)
        ids = connect.insert(binary_collection, binary_entities, partition_tag=tag)
        connect.flush([binary_collection])
        connect.create_index(binary_collection, binary_field_name, get_jaccard_index)
        stats = connect.get_collection_stats(binary_collection)
        logging.getLogger().info(stats)
        connect.drop_index(binary_collection, binary_field_name)
        stats = connect.get_collection_stats(binary_collection)
        logging.getLogger().info(stats)
        assert stats["partitions"][1]["segments"][0]["index_name"] == default_index_type


class TestIndexMultiCollections(object):

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def _test_create_index_multithread_multicollection(self, connect, args):
        '''
        target: test create index interface with multiprocess
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        threads_num = 8
        loop_num = 8
        threads = []
        collection = []
        j = 0
        while j < (threads_num * loop_num):
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
                ids = connect.insert(collection[ids * threads_num + i], vectors)
                connect.create_index(collection[ids * threads_num + i], IndexType.IVFLAT, {"nlist": NLIST, "metric_type": "L2"})
                assert status.OK()
                query_vec = [vectors[0]]
                top_k = 1
                search_param = {"nprobe": nprobe}
                status, result = connect.search(collection[ids * threads_num + i], top_k, query_vec,
                                                params=search_param)
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

    @pytest.mark.level(1)
    def test_create_index_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.create_index(collection_name, field_name, default_index)

    @pytest.mark.level(1)
    def test_drop_index_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.drop_index(collection_name)

    @pytest.fixture(
        scope="function",
        params=gen_invalid_index()
    )
    def get_index(self, request):
        yield request.param

    @pytest.mark.level(1)
    def test_create_index_with_invalid_index_params(self, connect, collection, get_index):
        logging.getLogger().info(get_index)
        with pytest.raises(Exception) as e:
            connect.create_index(collection, field_name, get_simple_index)


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
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("sq8h not support in CPU mode")
        return request.param

    def check_result(self, res):
        logging.getLogger().info("In callback check search result")
        logging.getLogger().info(res)

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        ids = connect.insert(collection, entities)
        logging.getLogger().info("start index")
        future = connect.create_index(collection, field_name, get_simple_index, _async=True)
        logging.getLogger().info("before result")
        res = future.result()
        # TODO:
        logging.getLogger().info(res)

    def test_create_index_with_invalid_collectionname(self, connect):
        collection_name = " "
        future = connect.create_index(collection_name, field_name, default_index, _async=True)
        with pytest.raises(Exception) as e:
            res = future.result()

    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_callback(self, connect, collection, get_simple_index):
        '''
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        ids = connect.insert(collection, entities)
        logging.getLogger().info("start index")
        future = connect.create_index(collection, field_name, get_simple_index, _async=True,
                                      _callback=self.check_result)
        logging.getLogger().info("before result")
        res = future.result()
        # TODO:
        logging.getLogger().info(res)
