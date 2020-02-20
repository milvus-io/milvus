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
table_id = "test_delete"
DELETE_TIMEOUT = 60
nprobe = 1
epsilon = 0.0001
tag = "1970-01-01"
top_k = 1
nb = 6000


class TestDeleteBase:
    """
    ******************************************************************
      The following cases are used to test `delete_by_id` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index_params()
    )
    def get_simple_index_params(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] not in [IndexType.IVF_SQ8, IndexType.IVFLAT, IndexType.FLAT]:
                pytest.skip("Only support index_type: flat/ivf_flat/ivf_sq8")
        else:
            pytest.skip("Only support CPU mode")
        return request.param

    def test_delete_vector_search(self, connect, table):
        '''
        target: test delete vector
        method: add vector and delete
        expected: status ok, vector deleted
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.delete_by_id(table, ids)
        assert status.OK()
        status = connect.flush([table])
        # pdb.set_trace()
        status, res = connect.search_vectors(table, top_k, nprobe, vector) 
        logging.getLogger().info(res)
        assert status.OK()
        assert len(res) == 0

    def test_delete_vector_multi_same_ids(self, connect, table):
        '''
        target: test delete vector, with some same ids
        method: add vector and delete
        expected: status ok, vector deleted
        '''
        vectors = gen_vectors(nb, dim)
        status, ids = connect.add_vectors(table, vectors, ids=[1 for i in range(nb)])
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.delete_by_id(table, [1])
        assert status.OK()
        status = connect.flush([table])
        status, res = connect.search_vectors(table, top_k, nprobe, [vectors[0]])
        logging.getLogger().info(res)
        assert status.OK()
        assert len(res) == 0

    def test_delete_vector_table_count(self, connect, table):
        '''
        target: test delete vector
        method: add vector and delete
        expected: status ok, vector deleted
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.delete_by_id(table, ids)
        assert status.OK()
        status = connect.flush([table])
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        assert res == 0 

    def test_delete_vector_table_count_no_flush(self, connect, table):
        '''
        target: test delete vector
        method: add vector and delete, no flush(using auto flush)
        expected: status ok, vector deleted
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.delete_by_id(table, ids)
        assert status.OK()
        time.sleep(2)
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        assert res == 0 

    def test_delete_vector_id_not_exised(self, connect, table):
        '''
        target: test delete vector, params vector_id not existed
        method: add vector and delete
        expected: status ok, search with vector have result
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.delete_by_id(table, [0])
        assert status.OK()
        status = connect.flush([table])
        status, res = connect.search_vectors(table, top_k, nprobe, vector)
        assert status.OK()
        assert res[0][0].id == ids[0]

    def test_delete_vector_table_not_existed(self, connect, table):
        '''
        target: test delete vector, params table_name not existed
        method: add vector and delete
        expected: status not ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        table_new = gen_unique_str()
        status = connect.delete_by_id(table_new, [0])
        assert not status.OK()

    def test_add_vectors_delete_vector(self, connect, table):
        '''
        method: add vectors and delete
        expected: status ok, vectors deleted
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_by_id(table, delete_ids)
        assert status.OK()
        status = connect.flush([table])
        status, res = connect.search_vectors(table, top_k, nprobe, query_vecs)
        assert status.OK()
        logging.getLogger().info(res)
        assert res[0][0].distance > epsilon
        assert res[1][0].distance < epsilon
        assert res[1][0].id == ids[1]
        assert res[2][0].distance > epsilon

    def test_create_index_after_delete(self, connect, table, get_simple_index_params):
        '''
        method: add vectors and delete, then create index
        expected: status ok, vectors deleted, index created
        '''
        index_params = get_simple_index_params
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_by_id(table, delete_ids)
        assert status.OK()
        status = connect.flush([table])
        status = connect.create_index(table, index_params)
        assert status.OK()
        status, res = connect.search_vectors(table, top_k, nprobe, query_vecs)
        assert status.OK()
        logging.getLogger().info(res)
        assert res[0][0].distance > epsilon
        assert res[1][0].distance < epsilon
        assert res[1][0].id == ids[1]
        assert res[2][0].distance > epsilon

    def test_add_vector_after_delete(self, connect, table):
        '''
        method: add vectors and delete, then add vector
        expected: status ok, vectors deleted, vector added
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_by_id(table, delete_ids)
        assert status.OK()
        status = connect.flush([table])
        status, tmp_ids = connect.add_vectors(table, [vectors[0], vectors[-1]])
        assert status.OK()
        status = connect.flush([table])
        status, res = connect.search_vectors(table, top_k, nprobe, query_vecs)
        assert status.OK()
        logging.getLogger().info(res)
        assert res[0][0].id == tmp_ids[0]
        assert res[0][0].distance < epsilon
        assert res[1][0].distance < epsilon
        assert res[2][0].id == tmp_ids[-1]
        assert res[2][0].distance < epsilon

    def test_delete_multiable_times(self, connect, table):
        '''
        method: add vectors and delete id serveral times
        expected: status ok, vectors deleted, and status ok for next delete operation
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_by_id(table, delete_ids)
        assert status.OK()
        status = connect.flush([table])
        for i in range(10):
            status = connect.delete_by_id(table, delete_ids)
            assert status.OK()

    def test_delete_no_flush_multiable_times(self, connect, table):
        '''
        method: add vectors and delete id serveral times
        expected: status ok, vectors deleted, and status ok for next delete operation
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_by_id(table, delete_ids)
        assert status.OK()
        for i in range(10):
            status = connect.delete_by_id(table, delete_ids)
            assert status.OK()
        

class TestDeleteIndexedVectors:
    """
    ******************************************************************
      The following cases are used to test `delete_by_id` function
    ******************************************************************
    """
    @pytest.fixture(
        scope="function",
        params=gen_simple_index_params()
    )
    def get_simple_index_params(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] not in [IndexType.IVF_SQ8, IndexType.IVFLAT, IndexType.FLAT]:
                pytest.skip("Only support index_type: flat/ivf_flat/ivf_sq8")
        else:
            pytest.skip("Only support CPU mode")
        return request.param

    def test_delete_vectors_after_index_created_search(self, connect, table, get_simple_index_params):
        '''
        target: test delete vector after index created
        method: add vector, create index and delete vector
        expected: status ok, vector deleted
        '''
        index_params = get_simple_index_params
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.create_index(table, index_params) 
        assert status.OK()
        status = connect.delete_by_id(table, ids)
        assert status.OK()
        status = connect.flush([table])
        # pdb.set_trace()
        status, res = connect.search_vectors(table, top_k, nprobe, vector) 
        logging.getLogger().info(res)
        assert status.OK()
        assert len(res) == 0

    def test_add_vectors_delete_vector(self, connect, table, get_simple_index_params):
        '''
        method: add vectors and delete
        expected: status ok, vectors deleted
        '''
        index_params = get_simple_index_params
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.create_index(table, index_params) 
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_by_id(table, delete_ids)
        assert status.OK()
        status = connect.flush([table])
        status, res = connect.search_vectors(table, top_k, nprobe, query_vecs)
        assert status.OK()
        logging.getLogger().info(res)
        assert res[0][0].distance > epsilon
        assert res[1][0].distance < epsilon
        assert res[1][0].id == ids[1]
        assert res[2][0].distance > epsilon


class TestDeleteBinary:
    """
    ******************************************************************
      The following cases are used to test `delete_by_id` function
    ******************************************************************
    """
    def test_delete_vector_search(self, connect, jac_table):
        '''
        target: test delete vector
        method: add vector and delete
        expected: status ok, vector deleted
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_table, vector)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status = connect.delete_by_id(jac_table, ids)
        assert status.OK()
        status = connect.flush([jac_table])
        status, res = connect.search_vectors(jac_table, top_k, nprobe, vector) 
        logging.getLogger().info(res)
        assert status.OK()
        assert len(res) == 0

    # TODO: soft delete
    def test_delete_vector_table_count(self, connect, jac_table):
        '''
        target: test delete vector
        method: add vector and delete
        expected: status ok, vector deleted
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_table, vector)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status = connect.delete_by_id(jac_table, ids)
        assert status.OK()
        status = connect.flush([jac_table])
        status, res = connect.get_table_row_count(jac_table)
        assert status.OK()
        assert res == 0

    def test_delete_vector_id_not_exised(self, connect, jac_table):
        '''
        target: test delete vector, params vector_id not existed
        method: add vector and delete
        expected: status ok, search with vector have result
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_table, vector)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status = connect.delete_by_id(jac_table, [0])
        assert status.OK()
        status = connect.flush([jac_table])
        status = connect.flush([jac_table])
        status, res = connect.search_vectors(jac_table, top_k, nprobe, vector)
        assert status.OK()
        assert res[0][0].id == ids[0]

    def test_delete_vector_table_not_existed(self, connect, jac_table):
        '''
        target: test delete vector, params table_name not existed
        method: add vector and delete
        expected: status not ok
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_table, vector)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        table_new = gen_unique_str()
        status = connect.delete_by_id(table_new, [0])
        assert not status.OK()

    def test_add_vectors_delete_vector(self, connect, jac_table):
        '''
        method: add vectors and delete
        expected: status ok, vectors deleted
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_by_id(jac_table, delete_ids)
        assert status.OK()
        status = connect.flush([jac_table])
        status, res = connect.search_vectors(jac_table, top_k, nprobe, query_vecs)
        assert status.OK()
        logging.getLogger().info(res)
        assert res[0][0].id != ids[0]
        assert res[1][0].id == ids[1]
        assert res[2][0].id != ids[-1]

    def test_add_after_delete_vector(self, connect, jac_table):
        '''
        method: add vectors and delete, add
        expected: status ok, vectors added
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status = connect.delete_by_id(jac_table, delete_ids)
        assert status.OK()
        status = connect.flush([jac_table])
        status, tmp_ids = connect.add_vectors(jac_table, [vectors[0], vectors[-1]])
        assert status.OK()
        status = connect.flush([jac_table])
        status, res = connect.search_vectors(jac_table, top_k, nprobe, query_vecs)
        assert status.OK()
        logging.getLogger().info(res)
        assert res[0][0].id == tmp_ids[0]
        assert res[1][0].id == ids[1]
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
    def test_delete_vector_id_invalid(self, connect, table, gen_invalid_id):
        invalid_id = gen_invalid_id
        with pytest.raises(Exception) as e:
            status = connect.delete_by_id(table, [invalid_id])

    @pytest.mark.level(2)
    def test_delete_vector_ids_invalid(self, connect, table, gen_invalid_id):
        invalid_id = gen_invalid_id
        with pytest.raises(Exception) as e:
            status = connect.delete_by_id(table, [1, invalid_id])


class TestTableNameInvalid(object):
    """
    Test adding vectors with invalid table names
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_table_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_delete_vectors_with_invalid_table_name(self, connect, get_table_name):
        table_name = get_table_name
        status = connect.delete_by_id(table_name, [1])
        assert not status.OK() 
