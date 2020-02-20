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
table_id = "test_get_vector_by_id"
DELETE_TIMEOUT = 60
nprobe = 1
epsilon = 0.0001
tag = "1970-01-01"
top_k = 1
nb = 6000


class TestGetBase:
    """
    ******************************************************************
      The following cases are used to test `get_vector_by_id` function
    ******************************************************************
    """
    def test_get_vector_A(self, connect, table):
        '''
        target: test get_vector_by_id
        method: add vector, and get
        expected: status ok, vector returned
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, res = connect.get_vector_by_id(table, ids[0]) 
        logging.getLogger().info(res)
        assert status.OK()
        assert_equal_vector(res, vector[0])

    def test_get_vector_B(self, connect, table):
        '''
        target: test get_vector_by_id
        method: add vector, and get
        expected: status ok, vector returned
        '''
        vectors = gen_vectors(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, res = connect.get_vector_by_id(table, ids[0])
        logging.getLogger().info(res)
        assert status.OK()
        assert_equal_vector(res, vectors[0])

    def test_get_vector_multi_same_ids(self, connect, table):
        '''
        target: test get_vector_by_id
        method: add vectors, with the same id, get vector by the given id
        expected: status ok, get one vector 
        '''
        vectors = gen_vectors(nb, dim)
        status, ids = connect.add_vectors(table, vectors, ids=[1 for i in range(nb)])
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, res = connect.get_vector_by_id(table, 1) 
        logging.getLogger().info(res)
        assert status.OK()
        assert_equal_vector(res, vectors[0])

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

    def test_get_vector_after_delete(self, connect, table, get_id):
        '''
        target: test get_vector_by_id
        method: add vectors, and delete, get vector by the given id
        expected: status ok, get one vector
        '''
        vectors = gen_vectors(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        id = get_id
        status = connect.delete_by_id(table, [ids[id]])
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, res = connect.get_vector_by_id(table, ids[id])
        logging.getLogger().info(res)
        assert status.OK()
        assert not res 

    def test_get_vector_id_not_exised(self, connect, table):
        '''
        target: test get vector, params vector_id not existed
        method: add vector and get 
        expected: status ok, empty result
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, res = connect.get_vector_by_id(table, 1) 
        assert status.OK()
        assert not res 

    def test_get_vector_table_not_existed(self, connect, table):
        '''
        target: test get vector, params table_name not existed
        method: add vector and get
        expected: status not ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        table_new = gen_unique_str()
        status, res = connect.get_vector_by_id(table_new, 1) 
        assert not status.OK()


class TestGetIndexedVectors:
    """
    ******************************************************************
      The following cases are used to test `get_vector_by_id` function
    ******************************************************************
    """
    @pytest.fixture(
        scope="function",
        params=gen_simple_index_params()
    )
    def get_simple_index_params(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] not in [IndexType.IVF_SQ8, IndexType.IVFLAT, IndexType.FLAT]:
                logging.getLogger().info(request.param["index_type"])
                pytest.skip("Only support index_type: flat/ivf_flat/ivf_sq8")
        else:
            pytest.skip("Only support CPU mode")
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

    def test_get_vectors_after_index_created(self, connect, table, get_simple_index_params, get_id):
        '''
        target: test get vector after index created
        method: add vector, create index and get vector
        expected: status ok
        '''
        index_params = get_simple_index_params
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.create_index(table, index_params) 
        assert status.OK()
        id = get_id
        status, res = connect.get_vector_by_id(table, ids[id])
        logging.getLogger().info(res)
        assert status.OK()
        assert_equal_vector(res, vectors[id])

    def test_get_vector_after_delete(self, connect, table, get_simple_index_params, get_id):
        '''
        target: test get_vector_by_id
        method: add vectors, and delete, get vector by the given id
        expected: status ok, get one vector
        '''
        index_params = get_simple_index_params
        vectors = gen_vectors(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.create_index(table, index_params)
        assert status.OK()
        id = get_id
        status = connect.delete_by_id(table, [ids[id]])
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, res = connect.get_vector_by_id(table, ids[id])
        logging.getLogger().info(res)
        assert status.OK()
        assert not res


class TestGetBinary:
    """
    ******************************************************************
      The following cases are used to test `get_vector_by_id` function
    ******************************************************************
    """
    def test_get_vector_A(self, connect, jac_table):
        '''
        target: test get_vector_by_id
        method: add vector, and get
        expected: status ok, vector returned
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_table, vector)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status, res = connect.get_vector_by_id(jac_table, ids[0]) 
        logging.getLogger().info(res)
        assert status.OK()
        assert res == vector[0]

    def test_get_vector_B(self, connect, jac_table):
        '''
        target: test get_vector_by_id
        method: add vector, and get
        expected: status ok, vector returned
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status, res = connect.get_vector_by_id(jac_table, ids[0])
        logging.getLogger().info(res)
        assert status.OK()
        assert res == vectors[0]

    def test_get_vector_multi_same_ids(self, connect, jac_table):
        '''
        target: test get_vector_by_id
        method: add vectors, with the same id, get vector by the given id
        expected: status ok, get one vector 
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_table, vectors, ids=[1 for i in range(nb)])
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status, res = connect.get_vector_by_id(jac_table, 1) 
        logging.getLogger().info(res)
        assert status.OK()
        assert res == vectors[0] 

    def test_get_vector_id_not_exised(self, connect, jac_table):
        '''
        target: test get vector, params vector_id not existed
        method: add vector and get 
        expected: status ok, empty result
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_table, vector)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status, res = connect.get_vector_by_id(jac_table, 1) 
        assert status.OK()
        assert not res 

    def test_get_vector_table_not_existed(self, connect, jac_table):
        '''
        target: test get vector, params table_name not existed
        method: add vector and get
        expected: status not ok
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_table, vector)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        table_new = gen_unique_str()
        status, res = connect.get_vector_by_id(table_new, 1) 
        assert not status.OK()


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
    def test_get_vector_id_invalid(self, connect, table, gen_invalid_id):
        invalid_id = gen_invalid_id
        with pytest.raises(Exception) as e:
            status = connect.get_vector_by_id(table, invalid_id)


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
    def test_get_vectors_with_invalid_table_name(self, connect, get_table_name):
        table_name = get_table_name
        vectors = gen_vectors(1, dim)
        status, result = connect.get_vector_by_id(table_name, 1)
        assert not status.OK()
