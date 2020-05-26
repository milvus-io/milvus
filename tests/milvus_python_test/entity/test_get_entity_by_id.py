import time
import random
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import concurrent.futures
import pytest
from milvus import IndexType, MetricType
from utils import *


dim = 128
index_file_size = 10
collection_id = "get_entity_by_id"
DELETE_TIMEOUT = 60
nprobe = 1
tag = "1970-01-01"
top_k = 1
nb = 6000
tag = "tag"

class TestGetBase:
    """
    ******************************************************************
      The following cases are used to test .get_entity_by_id` function
    ******************************************************************
    """
    def test_get_vector_A(self, connect, collection):
        '''
        target: test.get_entity_by_id
        method: add vector, and get
        expected: status ok, vector returned
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.insert(collection, vector)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, res = connect.get_entity_by_id(collection, ids) 
        assert status.OK()
        assert_equal_vector(res[0], vector[0])

    def test_get_vector_B(self, connect, collection):
        '''
        target: test.get_entity_by_id
        method: add vector, and get
        expected: status ok, vector returned
        '''
        vectors = gen_vectors(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        length = 100
        status, res = connect.get_entity_by_id(collection, ids[:length])
        assert status.OK()
        for i in range(length):
            assert_equal_vector(res[i], vectors[i])

    def test_get_vector_C_limit(self, connect, collection):
        '''
        target: test.get_entity_by_id
        method: add vector, and get, limit > 1000
        expected: status ok, vector returned
        '''
        vectors = gen_vectors(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, res = connect.get_entity_by_id(collection, ids)
        assert not status.OK()

    def test_get_vector_partition(self, connect, collection):
        '''
        target: test.get_entity_by_id
        method: add vector, and get
        expected: status ok, vector returned
        '''
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(collection, tag)
        assert status.OK()
        status, ids = connect.insert(collection, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        length = 100
        status, res = connect.get_entity_by_id(collection, ids[:length])
        assert status.OK()
        for i in range(length):
            assert_equal_vector(res[i], vectors[i])

    def test_get_vector_multi_same_ids(self, connect, collection):
        '''
        target: test.get_entity_by_id
        method: add vectors, with the same id, get vector by the given id
        expected: status ok, get one vector 
        '''
        vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        ids[1] = 0; ids[-1] = 0
        status, ids = connect.insert(collection, vectors, ids=ids)
        status = connect.flush([collection])
        assert status.OK()
        status, res = connect.get_entity_by_id(collection, [0]) 
        assert status.OK()
        assert_equal_vector(res[0], vectors[0])

    @pytest.fixture(
        scope="function",
        params=[
            1,
            10,
            100,
            1000,
            -1
        ],
    )
    def get_id(self, request):
        yield request.param

    def test_get_vector_after_delete(self, connect, collection, get_id):
        '''
        target: test.get_entity_by_id
        method: add vectors, and delete, get vector by the given id
        expected: status ok, get one vector
        '''
        vectors = gen_vectors(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        id = get_id
        status = connect.delete_entity_by_id(collection, [ids[id]])
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, res = connect.get_entity_by_id(collection, [ids[id]])
        assert status.OK()
        assert not len(res[0])

    def test_get_vector_after_delete_with_partition(self, connect, collection, get_id):
        '''
        target: test.get_entity_by_id
        method: add vectors into partition, and delete, get vector by the given id
        expected: status ok, get one vector
        '''
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(collection, tag)
        status, ids = connect.insert(collection, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        id = get_id
        status = connect.delete_entity_by_id(collection, [ids[id]])
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, res = connect.get_entity_by_id(collection, [ids[id]])
        assert status.OK()
        assert not len(res[0])

    def test_get_vector_id_not_exised(self, connect, collection):
        '''
        target: test get vector, params vector_id not existed
        method: add vector and get 
        expected: status ok, empty result
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.insert(collection, vector)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, res = connect.get_entity_by_id(collection, [1]) 
        assert status.OK()
        assert not len(res[0])

    def test_get_vector_collection_not_existed(self, connect, collection):
        '''
        target: test get vector, params collection_name not existed
        method: add vector and get
        expected: status not ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.insert(collection, vector)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        collection_new = gen_unique_str()
        status, res = connect.get_entity_by_id(collection_new, [1]) 
        assert not status.OK()

    @pytest.mark.timeout(60)
    def test_get_vector_by_id_multithreads(self, connect, collection):
        vectors = gen_vectors(nb, dim)
        status, ids = connect.insert(collection, vectors)
        status = connect.flush([collection])
        assert status.OK()
        get_id = ids[100:200]
        def get():
            status, res = connect.get_entity_by_id(collection, get_id)
            assert status.OK()
            assert len(res) == len(get_id)
            for i in range(len(res)):
                assert_equal_vector(res[i], vectors[100+i])
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_results = {executor.submit(
                get): i for i in range(10)}
            for future in concurrent.futures.as_completed(future_results):
                future.result()

    # TODO: autoflush
    def _test_get_vector_by_id_after_delete_no_flush(self, connect, collection):
        vectors = gen_vectors(nb, dim)
        status, ids = connect.insert(collection, vectors)
        status = connect.flush([collection])
        assert status.OK()
        get_id = ids[100:200]
        status = connect.delete_entity_by_id(collection, get_id)
        assert status.OK()
        status, res = connect.get_entity_by_id(collection, get_id)
        assert status.OK()
        assert len(res) == len(get_id)
        for i in range(len(res)):
            assert_equal_vector(res[i], vectors[100+i])


class TestGetIndexedVectors:
    """
    ******************************************************************
      The following cases are used to test .get_entity_by_id` function
    ******************************************************************
    """
    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] not in [IndexType.IVF_SQ8, IndexType.IVFLAT, IndexType.FLAT, IndexType.IVF_PQ, IndexType.IVF_SQ8H]:
                pytest.skip("Only support index_type: idmap/ivf")
        elif str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] in [IndexType.IVF_SQ8H]:
                pytest.skip("CPU not support index_type: ivf_sq8h")

        return request.param

    @pytest.fixture(
        scope="function",
        params=[
            1,
            10,
            100,
            1000,
            -1
        ],
    )
    def get_id(self, request):
        yield request.param

    def test_get_vectors_after_index_created(self, connect, collection, get_simple_index, get_id):
        '''
        target: test get vector after index created
        method: add vector, create index and get vector
        expected: status ok
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()
        id = get_id
        status, res = connect.get_entity_by_id(collection, [ids[id]])
        assert status.OK()
        assert_equal_vector(res[0], vectors[id])

    def test_get_vector_after_delete(self, connect, collection, get_simple_index, get_id):
        '''
        target: test.get_entity_by_id
        method: add vectors, and delete, get vector by the given id
        expected: status ok, get one vector
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vectors(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()
        id = get_id
        status = connect.delete_entity_by_id(collection, [ids[id]])
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, res = connect.get_entity_by_id(collection, [ids[id]])
        assert status.OK()
        assert not len(res[0])

    def test_get_vector_partition(self, connect, collection, get_simple_index, get_id):
        '''
        target: test.get_entity_by_id
        method: add vector, and get
        expected: status ok, vector returned
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(collection, tag)
        ids = [i for i in range(nb)] 
        status, ids = connect.insert(collection, vectors, ids, partition_tag=tag)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()
        id = get_id
        status, res = connect.get_entity_by_id(collection, [ids[id]])
        assert status.OK()
        assert_equal_vector(res[0], vectors[id])


class TestGetBinary:
    """
    ******************************************************************
      The following cases are used to test .get_entity_by_id` function
    ******************************************************************
    """
    def test_get_vector_A(self, connect, jac_collection):
        '''
        target: test.get_entity_by_id
        method: add vector, and get
        expected: status ok, vector returned
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.insert(jac_collection, vector)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status, res = connect.get_entity_by_id(jac_collection, [ids[0]]) 
        assert status.OK()
        assert_equal_vector(res[0], vector[0])

    def test_get_vector_B(self, connect, jac_collection):
        '''
        target: test.get_entity_by_id
        method: add vector, and get
        expected: status ok, vector returned
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.insert(jac_collection, vectors)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status, res = connect.get_entity_by_id(jac_collection, [ids[0]])
        assert status.OK()
        assert_equal_vector(res[0], vectors[0])

    def test_get_vector_multi_same_ids(self, connect, jac_collection):
        '''
        target: test.get_entity_by_id
        method: add vectors, with the same id, get vector by the given id
        expected: status ok, get one vector 
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        ids = [i for i in range(nb)]
        ids[0] = 0; ids[-1] = 0
        status, ids = connect.insert(jac_collection, vectors, ids=ids)
        status = connect.flush([jac_collection])
        assert status.OK()
        status, res = connect.get_entity_by_id(jac_collection, [0]) 
        assert status.OK()
        assert_equal_vector(res[0], vectors[0])

    def test_get_vector_id_not_exised(self, connect, jac_collection):
        '''
        target: test get vector, params vector_id not existed
        method: add vector and get 
        expected: status ok, empty result
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.insert(jac_collection, vector)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status, res = connect.get_entity_by_id(jac_collection, [1]) 
        assert status.OK()
        assert not len(res[0])

    def test_get_vector_collection_not_existed(self, connect, jac_collection):
        '''
        target: test get vector, params collection_name not existed
        method: add vector and get
        expected: status not ok
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.insert(jac_collection, vector)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        collection_new = gen_unique_str()
        status, res = connect.get_entity_by_id(collection_new, [1]) 
        assert not status.OK()

    def test_get_vector_partition(self, connect, jac_collection):
        '''
        target: test.get_entity_by_id
        method: add vector, and get
        expected: status ok, vector returned
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status = connect.create_partition(jac_collection, tag)
        status, ids = connect.insert(jac_collection, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status, res = connect.get_entity_by_id(jac_collection, [ids[0]])
        assert status.OK()
        assert_equal_vector(res[0], vectors[0])


class TestGetVectorIdIngalid(object):
    single_vector = gen_single_vector(dim)

    """
    Test adding vectors with invalid vectors
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_vector_ids()
    )
    def gen_invalid_id(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_get_vector_id_invalid(self, connect, collection, gen_invalid_id):
        invalid_id = gen_invalid_id
        with pytest.raises(Exception) as e:
            status = connect.get_entity_by_id(collection, [invalid_id])


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
    def test_get_vectors_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        vectors = gen_vectors(1, dim)
        status, result = connect.get_entity_by_id(collection_name, [1])
        assert not status.OK()
