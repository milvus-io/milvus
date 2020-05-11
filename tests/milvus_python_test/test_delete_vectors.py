import time
import random
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from milvus import IndexType, MetricType
from utils import *


dim = 128
index_file_size = 10
collection_id = "test_delete"
DELETE_TIMEOUT = 60
nprobe = 1
epsilon = 0.001
tag = "1970-01-01"
top_k = 1
nb = 6000

class TestDeleteBase:
    """
    ******************************************************************
      The following cases are used to test `delete_entity_by_id` function
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

    def test_delete_vector_search(self, connect, collection, get_simple_index):
        '''
        target: test delete vector
        method: add vector and delete
        expected: status ok, vector deleted
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(collection, vector)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.delete_entity_by_id(collection, ids)
        assert status.OK()
        status = connect.flush([collection])
        search_param = get_search_param(index_type)
        status = connect.flush([collection])
        search_param = get_search_param(index_type)
        status, res = connect.search_vectors(collection, top_k, vector, params=search_param)
        logging.getLogger().info(res)
        assert status.OK()
        assert len(res) == 0

    def test_delete_vector_multi_same_ids(self, connect, collection, get_simple_index):
        '''
        target: test delete vector, with some same ids
        method: add vector and delete
        expected: status ok, vector deleted
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vectors(nb, dim)
        connect.add_vectors(collection, vectors, ids=[1 for i in range(nb)])
        status = connect.flush([collection])
        # Bloom filter error
        assert status.OK()
        status = connect.delete_entity_by_id(collection, [1])
        assert status.OK()
        status = connect.flush([collection])
        search_param = get_search_param(index_type)
        status, res = connect.search_vectors(collection, top_k, [vectors[0]], params=search_param)
        logging.getLogger().info(res)
        assert status.OK()
        assert len(res) == 0

    def test_delete_vector_collection_count(self, connect, collection):
        '''
        target: test delete vector
        method: add vector and delete
        expected: status ok, vector deleted
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(collection, vector)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.delete_entity_by_id(collection, ids)
        assert status.OK()
        status = connect.flush([collection])
        status, res = connect.count_entities(collection)
        assert status.OK()
        assert res == 0

    def test_delete_vector_collection_count_no_flush(self, connect, collection):
        '''
        target: test delete vector
        method: add vector and delete, no flush(using auto flush)
        expected: status ok, vector deleted
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(collection, vector)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.delete_entity_by_id(collection, ids)
        assert status.OK()
        time.sleep(2)
        status, res = connect.count_entities(collection)
        assert status.OK()
        assert res == 0

    def test_delete_vector_id_not_exised(self, connect, collection, get_simple_index):
        '''
        target: test delete vector, params vector_id not existed
        method: add vector and delete
        expected: status ok, search with vector have result
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(collection, vector)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.delete_entity_by_id(collection, [0])
        assert status.OK()
        status = connect.flush([collection])
        search_param = get_search_param(index_type)
        status, res = connect.search_vectors(collection, top_k, vector, params=search_param)
        assert status.OK()
        assert res[0][0].id == ids[0]

    def test_delete_vector_collection_not_existed(self, connect, collection):
        '''
        target: test delete vector, params collection_name not existed
        method: add vector and delete
        expected: status not ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(collection, vector)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        collection_new = gen_unique_str()
        status = connect.delete_entity_by_id(collection_new, [0])
        assert not status.OK()

    def test_add_vectors_delete_vector(self, connect, collection, get_simple_index):
        '''
        method: add vectors and delete
        expected: status ok, vectors deleted
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        status = connect.flush([collection])
        search_param = get_search_param(index_type)
        status, res = connect.search_vectors(collection, top_k, query_vecs, params=search_param)
        assert status.OK()
        logging.getLogger().info(res)
        assert res[0][0].distance > epsilon
        assert res[1][0].distance < epsilon
        assert res[1][0].id == ids[1]
        assert res[2][0].distance > epsilon

    def test_create_index_after_delete(self, connect, collection, get_simple_index):
        '''
        method: add vectors and delete, then create index
        expected: status ok, vectors deleted, index created
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        status = connect.flush([collection])
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()
        search_param = get_search_param(index_type)
        status, res = connect.search_vectors(collection, top_k, query_vecs, params=search_param)
        assert status.OK()
        logging.getLogger().info(res)
        logging.getLogger().info(ids[0])
        logging.getLogger().info(ids[1])
        logging.getLogger().info(ids[-1])
        assert res[0][0].id != ids[0]
        assert res[1][0].id == ids[1]
        assert res[2][0].id != ids[-1]

    def test_add_vector_after_delete(self, connect, collection, get_simple_index):
        '''
        method: add vectors and delete, then add vector
        expected: status ok, vectors deleted, vector added
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        status = connect.flush([collection])
        status, tmp_ids = connect.add_vectors(collection, [vectors[0], vectors[-1]])
        assert status.OK()
        status = connect.flush([collection])
        search_param = get_search_param(index_type)
        status, res = connect.search_vectors(collection, top_k, query_vecs, params=search_param)
        assert status.OK()
        logging.getLogger().info(res)
        assert res[0][0].id == tmp_ids[0]
        assert res[0][0].distance < epsilon
        assert res[1][0].distance < epsilon
        assert res[2][0].id == tmp_ids[-1]
        assert res[2][0].distance < epsilon

    def test_delete_multiable_times(self, connect, collection):
        '''
        method: add vectors and delete id serveral times
        expected: status ok, vectors deleted, and status ok for next delete operation
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        status = connect.flush([collection])
        for i in range(10):
            status = connect.delete_entity_by_id(collection, delete_ids)
            assert status.OK()

    def test_delete_no_flush_multiable_times(self, connect, collection):
        '''
        method: add vectors and delete id serveral times
        expected: status ok, vectors deleted, and status ok for next delete operation
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        for i in range(10):
            status = connect.delete_entity_by_id(collection, delete_ids)
            assert status.OK()
            assert status.OK()


class TestDeleteIndexedVectors:
    """
    ******************************************************************
      The following cases are used to test `delete_entity_by_id` function
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

    def test_delete_vectors_after_index_created_search(self, connect, collection, get_simple_index):
        '''
        target: test delete vector after index created
        method: add vector, create index and delete vector
        expected: status ok, vector deleted
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(collection, vector)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()
        status = connect.delete_entity_by_id(collection, ids)
        assert status.OK()
        status = connect.flush([collection])
        search_param = get_search_param(index_type)
        status, res = connect.search_vectors(collection, top_k, vector, params=search_param)
        logging.getLogger().info(res)
        assert status.OK()
        assert len(res) == 0

    def test_add_vectors_delete_vector(self, connect, collection, get_simple_index):
        '''
        method: add vectors and delete
        expected: status ok, vectors deleted
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        status = connect.flush([collection])
        search_param = get_search_param(index_type)
        status, res = connect.search_vectors(collection, top_k, query_vecs, params=search_param)
        assert status.OK()
        logging.getLogger().info(ids[0])
        logging.getLogger().info(ids[1])
        logging.getLogger().info(ids[-1])
        logging.getLogger().info(res)
        assert res[0][0].id != ids[0]
        assert res[1][0].id == ids[1]
        assert res[2][0].id != ids[-1]


class TestDeleteBinary:
    """
    ******************************************************************
      The following cases are used to test `delete_entity_by_id` function
    ******************************************************************
    """
    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] == IndexType.IVFLAT or request.param["index_type"] == IndexType.FLAT:
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    def test_delete_vector_search(self, connect, jac_collection, get_simple_index):
        '''
        target: test delete vector
        method: add vector and delete
        expected: status ok, vector deleted
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_collection, vector)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status = connect.delete_entity_by_id(jac_collection, ids)
        assert status.OK()
        status = connect.flush([jac_collection])
        search_param = get_search_param(index_type)
        status, res = connect.search_vectors(jac_collection, top_k, vector, params=search_param)
        logging.getLogger().info(res)
        assert status.OK()
        assert len(res) == 0
        assert status.OK()
        assert len(res) == 0

    # TODO: soft delete
    def test_delete_vector_collection_count(self, connect, jac_collection):
        '''
        target: test delete vector
        method: add vector and delete
        expected: status ok, vector deleted
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_collection, vector)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status = connect.delete_entity_by_id(jac_collection, ids)
        assert status.OK()
        status = connect.flush([jac_collection])
        status, res = connect.count_entities(jac_collection)
        assert status.OK()
        assert res == 0

    def test_delete_vector_id_not_exised(self, connect, jac_collection, get_simple_index):
        '''
        target: test delete vector, params vector_id not existed
        method: add vector and delete
        expected: status ok, search with vector have result
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_collection, vector)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status = connect.delete_entity_by_id(jac_collection, [0])
        assert status.OK()
        status = connect.flush([jac_collection])
        status = connect.flush([jac_collection])
        search_param = get_search_param(index_type)
        status, res = connect.search_vectors(jac_collection, top_k, vector, params=search_param)
        assert status.OK()
        assert res[0][0].id == ids[0]

    def test_delete_vector_collection_not_existed(self, connect, jac_collection):
        '''
        target: test delete vector, params collection_name not existed
        method: add vector and delete
        expected: status not ok
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_collection, vector)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        collection_new = gen_unique_str()
        status = connect.delete_entity_by_id(collection_new, [0])
        collection_new = gen_unique_str()
        status = connect.delete_entity_by_id(collection_new, [0])
        assert not status.OK()

    def test_add_vectors_delete_vector(self, connect, jac_collection, get_simple_index):
        '''
        method: add vectors and delete
        expected: status ok, vectors deleted
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_collection, vectors)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_entity_by_id(jac_collection, delete_ids)
        assert status.OK()
        status = connect.flush([jac_collection])
        search_param = get_search_param(index_type)
        status, res = connect.search_vectors(jac_collection, top_k, query_vecs, params=search_param)
        assert status.OK()
        logging.getLogger().info(res)
        assert res[0][0].id != ids[0]
        assert res[1][0].id == ids[1]
        assert res[2][0].id != ids[-1]

    def test_add_after_delete_vector(self, connect, jac_collection, get_simple_index):
        '''
        method: add vectors and delete, add
        expected: status ok, vectors added
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_collection, vectors)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_entity_by_id(jac_collection, delete_ids)
        assert status.OK()
        status = connect.flush([jac_collection])
        status, tmp_ids = connect.add_vectors(jac_collection, [vectors[0], vectors[-1]])
        assert status.OK()
        status = connect.flush([jac_collection])
        search_param = get_search_param(index_type)
        status, res = connect.search_vectors(jac_collection, top_k, query_vecs, params=search_param)
        assert status.OK()
        logging.getLogger().info(res)
        assert res[0][0].id == tmp_ids[0]
        assert res[1][0].id == ids[1]
        assert res[2][0].id == tmp_ids[-1]
        assert res[2][0].id == tmp_ids[-1]


class TestDeleteIdsIngalid(object):
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

    @pytest.mark.level(1)
    def test_delete_vector_id_invalid(self, connect, collection, gen_invalid_id):
        invalid_id = gen_invalid_id
        with pytest.raises(Exception) as e:
            status = connect.delete_entity_by_id(collection, [invalid_id])

    @pytest.mark.level(2)
    def test_delete_vector_ids_invalid(self, connect, collection, gen_invalid_id):
        invalid_id = gen_invalid_id
        with pytest.raises(Exception) as e:
            status = connect.delete_entity_by_id(collection, [1, invalid_id])


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
    def test_delete_vectors_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        status = connect.delete_entity_by_id(collection_name, [1])
        assert not status.OK()

