import time
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from milvus import IndexType, MetricType
from utils import *

dim = 128
index_file_size = 10
COMPACT_TIMEOUT = 180
nprobe = 1
top_k = 1
tag = "1970-01-01"
nb = 6000


class TestCompactBase:
    """
    ******************************************************************
      The following cases are used to test `compact` function
    ******************************************************************
    """
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_collection_name_None(self, connect, collection):
        '''
        target: compact collection where collection name is None
        method: compact with the collection_name: None
        expected: exception raised
        '''
        collection_name = None
        with pytest.raises(Exception) as e:
            status = connect.compact(collection_name)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_collection_name_not_existed(self, connect, collection):
        '''
        target: compact collection not existed
        method: compact with a random collection_name, which is not in db
        expected: status not ok
        '''
        collection_name = gen_unique_str("not_existed_collection")
        status = connect.compact(collection_name)
        assert not status.OK()
    
    @pytest.fixture(
        scope="function",
        params=gen_invalid_collection_names()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_collection_name_invalid(self, connect, get_collection_name):
        '''
        target: compact collection with invalid name
        method: compact with invalid collection_name
        expected: status not ok
        '''
        collection_name = get_collection_name
        status = connect.compact(collection_name)
        assert not status.OK()
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact(self, connect, collection):
        '''
        target: test add vector and compact 
        method: add vector and compact collection
        expected: status ok, vector added
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.insert(collection, vector)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        logging.getLogger().info(info)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_and_compact(self, connect, collection):
        '''
        target: test add vectors and compact 
        method: add vectors and compact collection
        expected: status ok, vectors added
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_delete_part_and_compact(self, connect, collection):
        '''
        target: test add vectors, delete part of them and compact 
        method: add vectors, delete a few and compact collection
        expected: status ok, data size is smaller after compact
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        logging.getLogger().info(info["partitions"])
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(size_before)
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        logging.getLogger().info(info["partitions"])
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(size_after)
        assert(size_before >= size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_delete_all_and_compact(self, connect, collection):
        '''
        target: test add vectors, delete them and compact 
        method: add vectors, delete all and compact collection
        expected: status ok, no data size in collection info because collection is empty
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.delete_entity_by_id(collection, ids)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        logging.getLogger().info(info["partitions"])
        assert not info["partitions"][0]["segments"]

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] not in [IndexType.IVF_SQ8, IndexType.IVFLAT, IndexType.FLAT, IndexType.IVF_PQ, IndexType.HNSW]:
                pytest.skip("Only support index_type: flat/ivf_flat/ivf_sq8")
        else:
            pytest.skip("Only support CPU mode")
        return request.param

    def test_compact_after_index_created(self, connect, collection, get_simple_index):
        '''
        target: test compact collection after index created
        method: add vectors, create index, delete part of vectors and compact
        expected: status ok, index description no change, data size smaller after compact
        '''
        count = 10
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vector(count, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.create_index(collection, index_type, index_param) 
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(info["partitions"])
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        logging.getLogger().info(info["partitions"])
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before > size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact_twice(self, connect, collection):
        '''
        target: test add vector and compact twice
        method: add vector and compact collection twice
        expected: status ok, data size no change
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.insert(collection, vector)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(collection)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact twice
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        size_after_twice = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_delete_part_and_compact_twice(self, connect, collection):
        '''
        target: test add vectors, delete part of them and compact twice
        method: add vectors, delete part and compact collection twice
        expected: status ok, data size smaller after first compact, no change after second
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before >= size_after)
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact twice
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        size_after_twice = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_multi_collections(self, connect):
        '''
        target: test compact works or not with multiple collections
        method: create 50 collections, add vectors into them and compact in turn
        expected: status ok
        '''
        nq = 100
        num_collections = 50
        vectors = gen_vectors(nq, dim)
        collection_list = []
        for i in range(num_collections):
            collection_name = gen_unique_str("test_compact_multi_collection_%d" % i)
            collection_list.append(collection_name)
            param = {'collection_name': collection_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.L2}
            connect.create_collection(param)
        time.sleep(6)
        for i in range(num_collections):
            status, ids = connect.insert(collection_name=collection_list[i], records=vectors)
            assert status.OK()
            status = connect.compact(collection_list[i])
            assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_after_compact(self, connect, collection):
        '''
        target: test add vector after compact 
        method: after compact operation, add vector
        expected: status ok, vector added
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
        vector = gen_single_vector(dim)
        status, ids = connect.insert(collection, vector)
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_index_creation_after_compact(self, connect, collection, get_simple_index):
        '''
        target: test index creation after compact
        method: after compact operation, create index
        expected: status ok, index description no change
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.delete_entity_by_id(collection, ids[:10])
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.compact(collection)
        assert status.OK()
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(collection, index_type, index_param) 
        assert status.OK()
        status, result = connect.get_index_info(collection)
        assert result._collection_name == collection
        assert result._index_type == index_type

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_delete_vectors_after_compact(self, connect, collection):
        '''
        target: test delete vectors after compact
        method: after compact operation, delete vectors
        expected: status ok, vectors deleted
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.compact(collection)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.delete_entity_by_id(collection, ids)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_search_after_compact(self, connect, collection):
        '''
        target: test search after compact
        method: after compact operation, search vector
        expected: status ok
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.compact(collection)
        assert status.OK()
        query_vecs = [vectors[0]]
        status, res = connect.search(collection, top_k, query_records=query_vecs) 
        logging.getLogger().info(res)
        assert status.OK()

    # TODO: enable
    def _test_compact_server_crashed_recovery(self, connect, collection):
        '''
        target: test compact when server crashed unexpectedly and restarted
        method: add vectors, delete and compact collection; server stopped and restarted during compact
        expected: status ok, request recovered
        '''
        vectors = gen_vector(nb * 100, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        delete_ids = ids[0:1000]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        # start to compact, kill and restart server
        logging.getLogger().info("compact starting...")
        status = connect.compact(collection)
        # pdb.set_trace()
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        assert info["partitions"][0].count == nb * 100 - 1000


class TestCompactJAC:
    """
    ******************************************************************
      The following cases are used to test `compact` function
    ******************************************************************
    """
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact(self, connect, jac_collection):
        '''
        target: test add vector and compact 
        method: add vector and compact collection
        expected: status ok, vector added
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.insert(jac_collection, vector)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(jac_collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_and_compact(self, connect, jac_collection):
        '''
        target: test add vectors and compact 
        method: add vectors and compact collection
        expected: status ok, vectors added
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.insert(jac_collection, vectors)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(jac_collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_delete_part_and_compact(self, connect, jac_collection):
        '''
        target: test add vectors, delete part of them and compact 
        method: add vectors, delete a few and compact collection
        expected: status ok, data size is smaller after compact
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.insert(jac_collection, vectors)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(jac_collection, delete_ids)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        logging.getLogger().info(info["partitions"])
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(size_before)
        status = connect.compact(jac_collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        logging.getLogger().info(info["partitions"])
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(size_after)
        assert(size_before >= size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_delete_all_and_compact(self, connect, jac_collection):
        '''
        target: test add vectors, delete them and compact 
        method: add vectors, delete all and compact collection
        expected: status ok, no data size in collection info because collection is empty
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.insert(jac_collection, vectors)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status = connect.delete_entity_by_id(jac_collection, ids)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        status = connect.compact(jac_collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        logging.getLogger().info(info["partitions"])
        assert not info["partitions"][0]["segments"]
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact_twice(self, connect, jac_collection):
        '''
        target: test add vector and compact twice
        method: add vector and compact collection twice
        expected: status ok
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.insert(jac_collection, vector)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(jac_collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
        status = connect.compact(jac_collection)
        assert status.OK()
        # get collection info after compact twice
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        size_after_twice = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_delete_part_and_compact_twice(self, connect, jac_collection):
        '''
        target: test add vectors, delete part of them and compact twice
        method: add vectors, delete part and compact collection twice
        expected: status ok, data size smaller after first compact, no change after second
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.insert(jac_collection, vectors)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(jac_collection, delete_ids)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(jac_collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before >= size_after)
        status = connect.compact(jac_collection)
        assert status.OK()
        # get collection info after compact twice
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        size_after_twice = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_multi_collections(self, connect):
        '''
        target: test compact works or not with multiple collections
        method: create 50 collections, add vectors into them and compact in turn
        expected: status ok
        '''
        nq = 100
        num_collections = 10
        tmp, vectors = gen_binary_vectors(nq, dim)
        collection_list = []
        for i in range(num_collections):
            collection_name = gen_unique_str("test_compact_multi_collection_%d" % i)
            collection_list.append(collection_name)
            param = {'collection_name': collection_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.JACCARD}
            connect.create_collection(param)
        time.sleep(6)
        for i in range(num_collections):
            status, ids = connect.insert(collection_name=collection_list[i], records=vectors)
            assert status.OK()
            status = connect.delete_entity_by_id(collection_list[i], [ids[0], ids[-1]])
            assert status.OK()
            status = connect.flush([collection_list[i]])
            assert status.OK()
            status = connect.compact(collection_list[i])
            assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_after_compact(self, connect, jac_collection):
        '''
        target: test add vector after compact 
        method: after compact operation, add vector
        expected: status ok, vector added
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.insert(jac_collection, vectors)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(jac_collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.insert(jac_collection, vector)
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_delete_vectors_after_compact(self, connect, jac_collection):
        '''
        target: test delete vectors after compact
        method: after compact operation, delete vectors
        expected: status ok, vectors deleted
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.insert(jac_collection, vectors)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status = connect.compact(jac_collection)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status = connect.delete_entity_by_id(jac_collection, ids)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_search_after_compact(self, connect, jac_collection):
        '''
        target: test search after compact
        method: after compact operation, search vector
        expected: status ok
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.insert(jac_collection, vectors)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status = connect.compact(jac_collection)
        assert status.OK()
        query_vecs = [vectors[0]]
        status, res = connect.search(jac_collection, top_k, query_records=query_vecs) 
        logging.getLogger().info(res)
        assert status.OK()


class TestCompactIP:
    """
    ******************************************************************
      The following cases are used to test `compact` function
    ******************************************************************
    """
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact(self, connect, ip_collection):
        '''
        target: test add vector and compact 
        method: add vector and compact collection
        expected: status ok, vector added
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.insert(ip_collection, vector)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(ip_collection)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_and_compact(self, connect, ip_collection):
        '''
        target: test add vectors and compact 
        method: add vectors and compact collection
        expected: status ok, vectors added
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(ip_collection, vectors)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(ip_collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_delete_part_and_compact(self, connect, ip_collection):
        '''
        target: test add vectors, delete part of them and compact 
        method: add vectors, delete a few and compact collection
        expected: status ok, data size is smaller after compact
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(ip_collection, vectors)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(ip_collection, delete_ids)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        logging.getLogger().info(info["partitions"])
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(size_before)
        status = connect.compact(ip_collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        logging.getLogger().info(info["partitions"])
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(size_after)
        assert(size_before >= size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_delete_all_and_compact(self, connect, ip_collection):
        '''
        target: test add vectors, delete them and compact 
        method: add vectors, delete all and compact collection
        expected: status ok, no data size in collection info because collection is empty
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(ip_collection, vectors)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        status = connect.delete_entity_by_id(ip_collection, ids)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        status = connect.compact(ip_collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        logging.getLogger().info(info["partitions"])
        assert not info["partitions"][0]["segments"]
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact_twice(self, connect, ip_collection):
        '''
        target: test add vector and compact twice
        method: add vector and compact collection twice
        expected: status ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.insert(ip_collection, vector)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(ip_collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
        status = connect.compact(ip_collection)
        assert status.OK()
        # get collection info after compact twice
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        size_after_twice = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_delete_part_and_compact_twice(self, connect, ip_collection):
        '''
        target: test add vectors, delete part of them and compact twice
        method: add vectors, delete part and compact collection twice
        expected: status ok, data size smaller after first compact, no change after second
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(ip_collection, vectors)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(ip_collection, delete_ids)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(ip_collection)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before >= size_after)
        status = connect.compact(ip_collection)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        # get collection info after compact twice
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        size_after_twice = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_multi_collections(self, connect):
        '''
        target: test compact works or not with multiple collections
        method: create 50 collections, add vectors into them and compact in turn
        expected: status ok
        '''
        nq = 100
        num_collections = 50
        vectors = gen_vectors(nq, dim)
        collection_list = []
        for i in range(num_collections):
            collection_name = gen_unique_str("test_compact_multi_collection_%d" % i)
            collection_list.append(collection_name)
            param = {'collection_name': collection_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.IP}
            connect.create_collection(param)
        time.sleep(6)
        for i in range(num_collections):
            status, ids = connect.insert(collection_name=collection_list[i], records=vectors)
            assert status.OK()
            status = connect.compact(collection_list[i])
            assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_after_compact(self, connect, ip_collection):
        '''
        target: test add vector after compact 
        method: after compact operation, add vector
        expected: status ok, vector added
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(ip_collection, vectors)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        # get collection info before compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(ip_collection)
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
        vector = gen_single_vector(dim)
        status, ids = connect.insert(ip_collection, vector)
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_delete_vectors_after_compact(self, connect, ip_collection):
        '''
        target: test delete vectors after compact
        method: after compact operation, delete vectors
        expected: status ok, vectors deleted
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(ip_collection, vectors)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        status = connect.compact(ip_collection)
        assert status.OK()
        status = connect.delete_entity_by_id(ip_collection, ids)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_search_after_compact(self, connect, ip_collection):
        '''
        target: test search after compact
        method: after compact operation, search vector
        expected: status ok
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(ip_collection, vectors)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        status = connect.compact(ip_collection)
        assert status.OK()
        query_vecs = [vectors[0]]
        status, res = connect.search(ip_collection, top_k, query_records=query_vecs) 
        logging.getLogger().info(res)
        assert status.OK()
