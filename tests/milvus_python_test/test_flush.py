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
collection_id = "test_flush"
DELETE_TIMEOUT = 60
nprobe = 1
tag = "1970-01-01"
top_k = 1
nb = 6000
tag = "partition_tag"


class TestFlushBase:
    """
    ******************************************************************
      The following cases are used to test `flush` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] not in [IndexType.IVF_SQ8, IndexType.IVFLAT, IndexType.FLAT, IndexType.IVF_PQ, IndexType.IVF_SQ8H]:
                pytest.skip("Only support index_type: idmap/flat")
        return request.param

    def test_flush_collection_not_existed(self, connect, collection):
        '''
        target: test delete vector, params collection_name not existed
        method: add vector and delete
        expected: status not ok
        '''
        collection_new = gen_unique_str()
        status = connect.flush([collection_new])
        assert not status.OK()

    def test_flush_empty_collection(self, connect, collection):
        '''
        method: flush collection with no vectors
        expected: status ok
        '''
        status = connect.flush([collection])
        assert status.OK()

    def test_add_partition_flush(self, connect, collection):
        '''
        method: add vectors into partition in collection, flush serveral times
        expected: status ok
        '''
        vectors = gen_vector(nb, dim)
        status = connect.create_partition(collection, tag)
        vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        status, ids = connect.insert(collection, vectors, ids)
        status = connect.flush([collection])
        result, res = connect.count_collection(collection)
        assert res == nb
        status, ids = connect.insert(collection, vectors, ids, partition_tag=tag)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        result, res = connect.count_collection(collection)
        assert res == 2 * nb

    def test_add_partitions_flush(self, connect, collection):
        '''
        method: add vectors into partitions in collection, flush one
        expected: status ok
        '''
        vectors = gen_vectors(nb, dim)
        tag_new = gen_unique_str()
        status = connect.create_partition(collection, tag)
        status = connect.create_partition(collection, tag_new)
        ids = [i for i in range(nb)]
        status, ids = connect.insert(collection, vectors, ids, partition_tag=tag)
        status = connect.flush([collection])
        assert status.OK()
        status, ids = connect.insert(collection, vectors, ids, partition_tag=tag_new)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        result, res = connect.count_collection(collection)
        assert res == 2 * nb

    def test_add_collections_flush(self, connect, collection):
        '''
        method: add vectors into collections, flush one
        expected: status ok
        '''
        vectors = gen_vectors(nb, dim)
        collection_new = gen_unique_str()
        param = {'collection_name': collection_new,
            'dimension': dim,
            'index_file_size': index_file_size,
            'metric_type': MetricType.L2}
        status = connect.create_collection(param)
        status = connect.create_partition(collection, tag)
        status = connect.create_partition(collection_new, tag)
        vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        status, ids = connect.insert(collection, vectors, ids, partition_tag=tag)
        status, ids = connect.insert(collection_new, vectors, ids, partition_tag=tag)
        assert status.OK()
        status = connect.flush([collection])
        status = connect.flush([collection_new])
        assert status.OK()
        result, res = connect.count_collection(collection)
        assert res == nb
        result, res = connect.count_collection(collection_new)
        assert res == nb
       
    def test_add_flush_multiable_times(self, connect, collection):
        '''
        method: add vectors, flush serveral times
        expected: status ok
        '''
        vectors = gen_vectors(nb, dim)
        status, ids = connect.add_vectors(collection, vectors)
        assert status.OK()
        for i in range(10):
            status = connect.flush([collection])
            assert status.OK()
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status, res = connect.search_vectors(collection, top_k, query_records=query_vecs)
        assert status.OK()

    def test_add_flush_auto(self, connect, collection):
        '''
        method: add vectors
        expected: status ok
        '''
        vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        status, ids = connect.add_vectors(collection, vectors, ids)
        assert status.OK()
        time.sleep(2)
        status, res = connect.count_collection(collection)
        assert status.OK()
        assert res == nb 

    @pytest.fixture(
        scope="function",
        params=[
            1,
            100
        ],
    )
    def same_ids(self, request):
        yield request.param

    # both autoflush / flush
    def test_add_flush_same_ids(self, connect, collection, same_ids):
        '''
        method: add vectors, with same ids, count(same ids) < 15, > 15
        expected: status ok
        '''
        vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        for i, item in enumerate(ids):
            if item <= same_ids:
                ids[i] = 0
        status, ids = connect.add_vectors(collection, vectors, ids)
        time.sleep(2)
        status = connect.flush([collection])
        assert status.OK()
        status, res = connect.count_collection(collection)
        assert status.OK()
        assert res == nb 

    def test_delete_flush_multiable_times(self, connect, collection):
        '''
        method: delete vectors, flush serveral times
        expected: status ok
        '''
        vectors = gen_vectors(nb, dim)
        status, ids = connect.add_vectors(collection, vectors)
        assert status.OK()
        status = connect.delete_by_id(collection, [ids[-1]])
        assert status.OK()
        for i in range(10):
            status = connect.flush([collection])
            assert status.OK()
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status, res = connect.search_vectors(collection, top_k, query_records=query_vecs)
        assert status.OK()

    # TODO: CI fail, LOCAL pass
    def _test_collection_count_during_flush(self, connect, args):
        '''
        method: flush collection at background, call `count_collection`
        expected: status ok
        '''
        collection = gen_unique_str() 
        param = {'collection_name': collection,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
        milvus.create_collection(param)
        vectors = gen_vector(nb, dim)
        status, ids = milvus.add_vectors(collection, vectors, ids=[i for i in range(nb)])
        def flush(collection_name):
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            status = milvus.delete_by_id(collection_name, [i for i in range(nb)])
            assert status.OK()
            status = milvus.flush([collection_name])
            assert status.OK()
        p = Process(target=flush, args=(collection, ))
        p.start()
        status, res = milvus.count_collection(collection)
        assert status.OK()
        p.join()
        status, res = milvus.count_collection(collection)
        assert status.OK()
        logging.getLogger().info(res)
        assert res == 0


class TestCollectionNameInvalid(object):
    """
    Test adding vectors with invalid collection names
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_collection_names()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_flush_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            status, result = connect.flush(collection_name)
