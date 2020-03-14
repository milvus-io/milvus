import pdb
import copy
import struct
from random import sample

import pytest
import threading
import datetime
import logging
from time import sleep
from multiprocessing import Process
import numpy
import sklearn.preprocessing
from milvus import IndexType, MetricType
from utils import *

dim = 128
collection_id = "test_search"
add_interval_time = 2
vectors = gen_vectors(6000, dim)
vectors = sklearn.preprocessing.normalize(vectors, axis=1, norm='l2')
vectors = vectors.tolist()
nprobe = 1
epsilon = 0.001
tag = "1970-01-01"
raw_vectors, binary_vectors = gen_binary_vectors(6000, dim)


class TestSearchBase:
    def init_data(self, connect, collection, nb=6000, partition_tags=None):
        '''
        Generate vectors and add it in collection, before search vectors
        '''
        global vectors
        if nb == 6000:
            add_vectors = vectors
        else:  
            add_vectors = gen_vectors(nb, dim)
            vectors = sklearn.preprocessing.normalize(vectors, axis=1, norm='l2')
            vectors = vectors.tolist()
        if partition_tags is None:
            status, ids = connect.add_vectors(collection, add_vectors)
            assert status.OK()
        else:
            status, ids = connect.add_vectors(collection, add_vectors, partition_tag=partition_tags)
            assert status.OK()
        sleep(add_interval_time)
        return add_vectors, ids

    def init_binary_data(self, connect, collection, nb=6000, insert=True, partition_tags=None):
        '''
        Generate vectors and add it in collection, before search vectors
        '''
        ids = []
        global binary_vectors
        global raw_vectors
        if nb == 6000:
            add_vectors = binary_vectors
            add_raw_vectors = raw_vectors
        else:  
            add_raw_vectors, add_vectors = gen_binary_vectors(nb, dim)
        if insert is True:
            if partition_tags is None:
                status, ids = connect.add_vectors(collection, add_vectors)
                assert status.OK()
            else:
                status, ids = connect.add_vectors(collection, add_vectors, partition_tag=partition_tags)
                assert status.OK()
            sleep(add_interval_time)
        return add_raw_vectors, add_vectors, ids

    """
    generate valid create_index params
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

    """
    generate top-k params
    """
    @pytest.fixture(
        scope="function",
        params=[1, 99, 1024, 2048, 2049]
    )
    def get_top_k(self, request):
        yield request.param


    def test_search_top_k_flat_index(self, connect, collection, get_top_k):
        '''
        target: test basic search fuction, all the search params is corrent, change top-k value
        method: search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        vectors, ids = self.init_data(connect, collection)
        query_vec = [vectors[0]]
        top_k = get_top_k
        status, result = connect.search_vectors(collection, top_k, query_vec)
        if top_k <= 2048:
            assert status.OK()
            assert len(result[0]) == min(len(vectors), top_k)
            assert result[0][0].distance <= epsilon
            assert check_result(result[0], ids[0])
        else:
            assert not status.OK()

    def test_search_l2_index_params(self, connect, collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        vectors, ids = self.init_data(connect, collection)
        status = connect.create_index(collection, index_type, index_param)
        query_vec = [vectors[0]]
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(collection, top_k, query_vec, params=search_param)
        logging.getLogger().info(result)
        if top_k <= 1024:
            assert status.OK()
            if index_type == IndexType.IVF_PQ:
                return
            assert len(result[0]) == min(len(vectors), top_k)
            assert check_result(result[0], ids[0])
            assert result[0][0].distance <= epsilon
        else:
            assert not status.OK()

    def test_search_l2_large_nq_index_params(self, connect, collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        vectors, ids = self.init_data(connect, collection)
        status = connect.create_index(collection, index_type, index_param)
        query_vec = []
        for i in range (1200):
            query_vec.append(vectors[i])
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(collection, top_k, query_vec, params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        if index_type == IndexType.IVF_PQ:
            return
        assert len(result[0]) == min(len(vectors), top_k)
        assert check_result(result[0], ids[0])
        assert result[0][0].distance <= epsilon

    def test_search_l2_index_params_partition(self, connect, collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: add vectors into collection, search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k, search collection with partition tag return empty
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(collection, tag)
        vectors, ids = self.init_data(connect, collection)
        status = connect.create_index(collection, index_type, index_param)
        query_vec = [vectors[0]]
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(collection, top_k, query_vec, params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        if(index_type != IndexType.IVF_PQ):
            assert len(result[0]) == min(len(vectors), top_k)
            assert check_result(result[0], ids[0])
            assert result[0][0].distance <= epsilon
        status, result = connect.search_vectors(collection, top_k, query_vec, partition_tags=[tag], params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == 0

    def test_search_l2_index_params_partition_A(self, connect, collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search partition with the given vectors, check the result
        expected: search status ok, and the length of the result is 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(collection, tag)
        vectors, ids = self.init_data(connect, collection)
        status = connect.create_index(collection, index_type, index_param)
        query_vec = [vectors[0]]
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(collection, top_k, query_vec, partition_tags=[tag], params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == 0

    def test_search_l2_index_params_partition_B(self, connect, collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(collection, tag)
        vectors, ids = self.init_data(connect, collection, partition_tags=tag)
        status = connect.create_index(collection, index_type, index_param)
        query_vec = [vectors[0]]
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(collection, top_k, query_vec, params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        if(index_type != IndexType.IVF_PQ):
            assert len(result[0]) == min(len(vectors), top_k)
            assert check_result(result[0], ids[0])
            assert result[0][0].distance <= epsilon
        status, result = connect.search_vectors(collection, top_k, query_vec, partition_tags=[tag], params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        if(index_type != IndexType.IVF_PQ):
            assert len(result[0]) == min(len(vectors), top_k)
            assert check_result(result[0], ids[0])
            assert result[0][0].distance <= epsilon

    def test_search_l2_index_params_partition_C(self, connect, collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors and tags (one of the tags not existed in collection), check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(collection, tag)
        vectors, ids = self.init_data(connect, collection, partition_tags=tag)
        status = connect.create_index(collection, index_type, index_param)
        query_vec = [vectors[0]]
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(collection, top_k, query_vec, partition_tags=[tag, "new_tag"], params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        if(index_type != IndexType.IVF_PQ):
            assert len(result[0]) == min(len(vectors), top_k)
            assert check_result(result[0], ids[0])
            assert result[0][0].distance <= epsilon

    def test_search_l2_index_params_partition_D(self, connect, collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors and tag (tag name not existed in collection), check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(collection, tag)
        vectors, ids = self.init_data(connect, collection, partition_tags=tag)
        status = connect.create_index(collection, index_type, index_param)
        query_vec = [vectors[0]]
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(collection, top_k, query_vec, partition_tags=["new_tag"], params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == 0

    def test_search_l2_index_params_partition_E(self, connect, collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search collection with the given vectors and tags, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        new_tag = "new_tag"
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(collection, tag)
        status = connect.create_partition(collection, new_tag)
        vectors, ids = self.init_data(connect, collection, partition_tags=tag)
        new_vectors, new_ids = self.init_data(connect, collection, nb=6001, partition_tags=new_tag)
        status = connect.create_index(collection, index_type, index_param)
        query_vec = [vectors[0], new_vectors[0]]
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(collection, top_k, query_vec, partition_tags=[tag, new_tag], params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        if(index_type != IndexType.IVF_PQ):
            assert len(result[0]) == min(len(vectors), top_k)
            assert check_result(result[0], ids[0])
            assert check_result(result[1], new_ids[0])
            assert result[0][0].distance <= epsilon
            assert result[1][0].distance <= epsilon
        status, result = connect.search_vectors(collection, top_k, query_vec, partition_tags=[new_tag], params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        if(index_type != IndexType.IVF_PQ):
            assert len(result[0]) == min(len(vectors), top_k)
            assert check_result(result[1], new_ids[0])
            assert result[1][0].distance <= epsilon

    def test_search_l2_index_params_partition_F(self, connect, collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search collection with the given vectors and tags with "re" expr, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        tag = "atag"
        new_tag = "new_tag"
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(collection, tag)
        status = connect.create_partition(collection, new_tag)
        vectors, ids = self.init_data(connect, collection, partition_tags=tag)
        new_vectors, new_ids = self.init_data(connect, collection, nb=6001, partition_tags=new_tag)
        status = connect.create_index(collection, index_type, index_param)
        query_vec = [vectors[0], new_vectors[0]]
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(collection, top_k, query_vec, partition_tags=["new(.*)"], params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        if(index_type != IndexType.IVF_PQ):
            assert result[0][0].distance > epsilon
            assert result[1][0].distance <= epsilon
        status, result = connect.search_vectors(collection, top_k, query_vec, partition_tags=["(.*)tag"], params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        if(index_type != IndexType.IVF_PQ):
            assert result[0][0].distance <= epsilon
            assert result[1][0].distance <= epsilon

    def test_search_ip_index_params(self, connect, ip_collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        vectors, ids = self.init_data(connect, ip_collection)
        status = connect.create_index(ip_collection, index_type, index_param)
        query_vec = [vectors[0]]
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(ip_collection, top_k, query_vec, params=search_param)
        logging.getLogger().info(result)

        if top_k <= 1024:
            assert status.OK()
            if(index_type != IndexType.IVF_PQ):
                assert len(result[0]) == min(len(vectors), top_k)
                assert check_result(result[0], ids[0])
                assert result[0][0].distance >= 1 - gen_inaccuracy(result[0][0].distance)
        else:
            assert not status.OK()

    def test_search_ip_large_nq_index_params(self, connect, ip_collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        vectors, ids = self.init_data(connect, ip_collection)
        status = connect.create_index(ip_collection, index_type, index_param)
        query_vec = []
        for i in range (1200):
            query_vec.append(vectors[i])
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(ip_collection, top_k, query_vec, params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        if(index_type != IndexType.IVF_PQ):
            assert len(result[0]) == min(len(vectors), top_k)
            assert check_result(result[0], ids[0])
            assert result[0][0].distance >= 1 - gen_inaccuracy(result[0][0].distance)

    def test_search_ip_index_params_partition(self, connect, ip_collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(index_param)
        status = connect.create_partition(ip_collection, tag)
        vectors, ids = self.init_data(connect, ip_collection)
        status = connect.create_index(ip_collection, index_type, index_param)
        query_vec = [vectors[0]]
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(ip_collection, top_k, query_vec, params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        if(index_type != IndexType.IVF_PQ):
            assert len(result[0]) == min(len(vectors), top_k)
            assert check_result(result[0], ids[0])
            assert result[0][0].distance >= 1 - gen_inaccuracy(result[0][0].distance)
        status, result = connect.search_vectors(ip_collection, top_k, query_vec, partition_tags=[tag], params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == 0

    def test_search_ip_index_params_partition_A(self, connect, ip_collection, get_simple_index):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors and tag, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(index_param)
        status = connect.create_partition(ip_collection, tag)
        vectors, ids = self.init_data(connect, ip_collection, partition_tags=tag)
        status = connect.create_index(ip_collection, index_type, index_param)
        query_vec = [vectors[0]]
        top_k = 10
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(ip_collection, top_k, query_vec, partition_tags=[tag], params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        if(index_type != IndexType.IVF_PQ):
            assert len(result[0]) == min(len(vectors), top_k)
            assert check_result(result[0], ids[0])
            assert result[0][0].distance >= 1 - gen_inaccuracy(result[0][0].distance)

    @pytest.mark.level(2)
    def test_search_vectors_without_connect(self, dis_connect, collection):
        '''
        target: test search vectors without connection
        method: use dis connected instance, call search method and check if search successfully
        expected: raise exception
        '''
        query_vectors = [vectors[0]]
        top_k = 1
        nprobe = 1
        with pytest.raises(Exception) as e:
            status, ids = dis_connect.search_vectors(collection, top_k, query_vectors)

    def test_search_collection_name_not_existed(self, connect, collection):
        '''
        target: search collection not existed
        method: search with the random collection_name, which is not in db
        expected: status not ok
        '''
        collection_name = gen_unique_str("not_existed_collection")
        top_k = 1
        nprobe = 1
        query_vecs = [vectors[0]]
        status, result = connect.search_vectors(collection_name, top_k, query_vecs)
        assert not status.OK()

    def test_search_collection_name_None(self, connect, collection):
        '''
        target: search collection that collection name is None
        method: search with the collection_name: None
        expected: status not ok
        '''
        collection_name = None
        top_k = 1
        nprobe = 1
        query_vecs = [vectors[0]]
        with pytest.raises(Exception) as e: 
            status, result = connect.search_vectors(collection_name, top_k, query_vecs)

    def test_search_top_k_query_records(self, connect, collection):
        '''
        target: test search fuction, with search params: query_records
        method: search with the given query_records, which are subarrays of the inserted vectors
        expected: status ok and the returned vectors should be query_records
        '''
        top_k = 10
        vectors, ids = self.init_data(connect, collection)
        query_vecs = [vectors[0],vectors[55],vectors[99]]
        status, result = connect.search_vectors(collection, top_k, query_vecs)
        assert status.OK()
        assert len(result) == len(query_vecs)
        for i in range(len(query_vecs)):
            assert len(result[i]) == top_k
            assert result[i][0].distance <= epsilon

    def test_search_distance_l2_flat_index(self, connect, collection):
        '''
        target: search collection, and check the result: distance
        method: compare the return distance value with value computed with Euclidean
        expected: the return distance equals to the computed value
        '''
        nb = 2
        top_k = 1
        vectors, ids = self.init_data(connect, collection, nb=nb)
        query_vecs = [[0.50 for i in range(dim)]]
        distance_0 = numpy.linalg.norm(numpy.array(query_vecs[0]) - numpy.array(vectors[0]))
        distance_1 = numpy.linalg.norm(numpy.array(query_vecs[0]) - numpy.array(vectors[1]))
        status, result = connect.search_vectors(collection, top_k, query_vecs)
        assert abs(numpy.sqrt(result[0][0].distance) - min(distance_0, distance_1)) <= gen_inaccuracy(result[0][0].distance)

    def test_search_distance_ip_flat_index(self, connect, ip_collection):
        '''
        target: search ip_collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        '''
        nb = 2
        top_k = 1
        nprobe = 1
        vectors, ids = self.init_data(connect, ip_collection, nb=nb)
        index_type = IndexType.FLAT
        index_param = {
            "nlist": 16384
        }
        connect.create_index(ip_collection, index_type, index_param)
        logging.getLogger().info(connect.describe_index(ip_collection))
        query_vecs = [[0.50 for i in range(dim)]]
        distance_0 = numpy.inner(numpy.array(query_vecs[0]), numpy.array(vectors[0]))
        distance_1 = numpy.inner(numpy.array(query_vecs[0]), numpy.array(vectors[1]))
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(ip_collection, top_k, query_vecs, params=search_param)
        assert abs(result[0][0].distance - max(distance_0, distance_1)) <= gen_inaccuracy(result[0][0].distance)

    def test_search_distance_jaccard_flat_index(self, connect, jac_collection):
        '''
        target: search ip_collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        '''
        # from scipy.spatial import distance
        top_k = 1
        nprobe = 512
        int_vectors, vectors, ids = self.init_binary_data(connect, jac_collection, nb=2)
        index_type = IndexType.FLAT
        index_param = {
            "nlist": 16384
        }
        connect.create_index(jac_collection, index_type, index_param)
        logging.getLogger().info(connect.describe_collection(jac_collection))
        logging.getLogger().info(connect.describe_index(jac_collection))
        query_int_vectors, query_vecs, tmp_ids = self.init_binary_data(connect, jac_collection, nb=1, insert=False)
        distance_0 = jaccard(query_int_vectors[0], int_vectors[0])
        distance_1 = jaccard(query_int_vectors[0], int_vectors[1])
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(jac_collection, top_k, query_vecs, params=search_param)
        logging.getLogger().info(status)
        logging.getLogger().info(result)
        assert abs(result[0][0].distance - min(distance_0, distance_1)) <= epsilon

    def test_search_distance_hamming_flat_index(self, connect, ham_collection):
        '''
        target: search ip_collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        '''
        # from scipy.spatial import distance
        top_k = 1
        nprobe = 512
        int_vectors, vectors, ids = self.init_binary_data(connect, ham_collection, nb=2)
        index_type = IndexType.FLAT
        index_param = {
            "nlist": 16384
        }
        connect.create_index(ham_collection, index_type, index_param)
        logging.getLogger().info(connect.describe_collection(ham_collection))
        logging.getLogger().info(connect.describe_index(ham_collection))
        query_int_vectors, query_vecs, tmp_ids = self.init_binary_data(connect, ham_collection, nb=1, insert=False)
        distance_0 = hamming(query_int_vectors[0], int_vectors[0])
        distance_1 = hamming(query_int_vectors[0], int_vectors[1])
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(ham_collection, top_k, query_vecs, params=search_param)
        logging.getLogger().info(status)
        logging.getLogger().info(result)
        assert abs(result[0][0].distance - min(distance_0, distance_1).astype(float)) <= epsilon

    def test_search_distance_tanimoto_flat_index(self, connect, tanimoto_collection):
        '''
        target: search ip_collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        '''
        # from scipy.spatial import distance
        top_k = 1
        nprobe = 512
        int_vectors, vectors, ids = self.init_binary_data(connect, tanimoto_collection, nb=2)
        index_type = IndexType.FLAT
        index_param = {
            "nlist": 16384
        }
        connect.create_index(tanimoto_collection, index_type, index_param)
        logging.getLogger().info(connect.describe_collection(tanimoto_collection))
        logging.getLogger().info(connect.describe_index(tanimoto_collection))
        query_int_vectors, query_vecs, tmp_ids = self.init_binary_data(connect, tanimoto_collection, nb=1, insert=False)
        distance_0 = tanimoto(query_int_vectors[0], int_vectors[0])
        distance_1 = tanimoto(query_int_vectors[0], int_vectors[1])
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(tanimoto_collection, top_k, query_vecs, params=search_param)
        logging.getLogger().info(status)
        logging.getLogger().info(result)
        assert abs(result[0][0].distance - min(distance_0, distance_1)) <= epsilon

    def test_search_distance_ip_index_params(self, connect, ip_collection, get_index):
        '''
        target: search collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        '''
        top_k = 2
        nprobe = 1
        vectors, ids = self.init_data(connect, ip_collection, nb=2)
        index_param = get_index["index_param"]
        index_type = get_index["index_type"]
        connect.create_index(ip_collection, index_type, index_param)
        logging.getLogger().info(connect.describe_index(ip_collection))
        query_vecs = [[0.50 for i in range(dim)]]
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(ip_collection, top_k, query_vecs, params=search_param)
        logging.getLogger().debug(status)
        logging.getLogger().debug(result)
        distance_0 = numpy.inner(numpy.array(query_vecs[0]), numpy.array(vectors[0]))
        distance_1 = numpy.inner(numpy.array(query_vecs[0]), numpy.array(vectors[1]))
        assert abs(result[0][0].distance - max(distance_0, distance_1)) <= gen_inaccuracy(result[0][0].distance)

    # TODO: enable
    # @pytest.mark.repeat(5)
    @pytest.mark.timeout(30)
    def _test_search_concurrent(self, connect, collection):
        vectors, ids = self.init_data(connect, collection)
        thread_num = 10
        nb = 100
        top_k = 10
        threads = []
        query_vecs = vectors[nb//2:nb]
        def search():
            status, result = connect.search_vectors(collection, top_k, query_vecs)
            assert len(result) == len(query_vecs)
            for i in range(len(query_vecs)):
                assert result[i][0].id in ids
                assert result[i][0].distance == 0.0
        for i in range(thread_num):
            x = threading.Thread(target=search, args=())
            threads.append(x)
            x.start()
        for th in threads:
            th.join()

    @pytest.mark.timeout(30)
    def test_search_concurrent_multithreads(self, args):
        '''
        target: test concurrent search with multiprocessess
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        '''
        nb = 100
        top_k = 10
        threads_num = 4
        threads = []
        collection = gen_unique_str("test_search_concurrent_multiprocessing")
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        param = {'collection_name': collection,
                 'dimension': dim,
                 'index_type': IndexType.FLAT,
                 'store_raw_vector': False}
        # create collection
        milvus = get_milvus(args["handler"])
        milvus.connect(uri=uri)
        milvus.create_collection(param)
        vectors, ids = self.init_data(milvus, collection, nb=nb)
        query_vecs = vectors[nb//2:nb]
        def search(milvus):
            status, result = milvus.search_vectors(collection, top_k, query_vecs)
            assert len(result) == len(query_vecs)
            for i in range(len(query_vecs)):
                assert result[i][0].id in ids
                assert result[i][0].distance == 0.0

        for i in range(threads_num):
            milvus = get_milvus(args["handler"])
            milvus.connect(uri=uri)
            t = threading.Thread(target=search, args=(milvus, ))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    # TODO: enable
    @pytest.mark.timeout(30)
    def _test_search_concurrent_multiprocessing(self, args):
        '''
        target: test concurrent search with multiprocessess
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        '''
        nb = 100
        top_k = 10
        process_num = 4
        processes = []
        collection = gen_unique_str("test_search_concurrent_multiprocessing")
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        param = {'collection_name': collection,
             'dimension': dim,
             'index_type': IndexType.FLAT,
             'store_raw_vector': False}
        # create collection
        milvus = get_milvus(args["handler"])
        milvus.connect(uri=uri)
        milvus.create_collection(param)
        vectors, ids = self.init_data(milvus, collection, nb=nb)
        query_vecs = vectors[nb//2:nb]
        def search(milvus):
            status, result = milvus.search_vectors(collection, top_k, query_vecs)
            assert len(result) == len(query_vecs)
            for i in range(len(query_vecs)):
                assert result[i][0].id in ids
                assert result[i][0].distance == 0.0

        for i in range(process_num):
            milvus = get_milvus(args["handler"])
            milvus.connect(uri=uri)
            p = Process(target=search, args=(milvus, ))
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()

    def test_search_multi_collection_L2(search, args):
        '''
        target: test search multi collections of L2
        method: add vectors into 10 collections, and search
        expected: search status ok, the length of result
        '''
        num = 10
        top_k = 10
        collections = []
        idx = []
        for i in range(num):
            collection = gen_unique_str("test_add_multicollection_%d" % i)
            uri = "tcp://%s:%s" % (args["ip"], args["port"])
            param = {'collection_name': collection,
                     'dimension': dim,
                     'index_file_size': 10,
                     'metric_type': MetricType.L2}
            # create collection
            milvus = get_milvus(args["handler"])
            milvus.connect(uri=uri)
            milvus.create_collection(param)
            status, ids = milvus.add_vectors(collection, vectors)
            assert status.OK()
            assert len(ids) == len(vectors)
            collections.append(collection)
            idx.append(ids[0])
            idx.append(ids[10])
            idx.append(ids[20])
        time.sleep(6)
        query_vecs = [vectors[0], vectors[10], vectors[20]]
        # start query from random collection
        for i in range(num):
            collection = collections[i]
            status, result = milvus.search_vectors(collection, top_k, query_vecs)
            assert status.OK()
            assert len(result) == len(query_vecs)
            for j in range(len(query_vecs)):
                assert len(result[j]) == top_k
            for j in range(len(query_vecs)):
                assert check_result(result[j], idx[3 * i + j])

    def test_search_multi_collection_IP(search, args):
        '''
        target: test search multi collections of IP
        method: add vectors into 10 collections, and search
        expected: search status ok, the length of result
        '''
        num = 10
        top_k = 10
        collections = []
        idx = []
        for i in range(num):
            collection = gen_unique_str("test_add_multicollection_%d" % i)
            uri = "tcp://%s:%s" % (args["ip"], args["port"])
            param = {'collection_name': collection,
                     'dimension': dim,
                     'index_file_size': 10,
                     'metric_type': MetricType.L2}
            # create collection
            milvus = get_milvus(args["handler"])
            milvus.connect(uri=uri)
            milvus.create_collection(param)
            status, ids = milvus.add_vectors(collection, vectors)
            assert status.OK()
            assert len(ids) == len(vectors)
            collections.append(collection)
            idx.append(ids[0])
            idx.append(ids[10])
            idx.append(ids[20])
        time.sleep(6)
        query_vecs = [vectors[0], vectors[10], vectors[20]]
        # start query from random collection
        for i in range(num):
            collection = collections[i]
            status, result = milvus.search_vectors(collection, top_k, query_vecs)
            assert status.OK()
            assert len(result) == len(query_vecs)
            for j in range(len(query_vecs)):
                assert len(result[j]) == top_k
            for j in range(len(query_vecs)):
                assert check_result(result[j], idx[3 * i + j])
"""
******************************************************************
#  The following cases are used to test `search_vectors` function 
#  with invalid collection_name top-k / nprobe / query_range
******************************************************************
"""

class TestSearchParamsInvalid(object):
    nlist = 16384
    index_type = IndexType.IVF_SQ8
    index_param = {"nlist": nlist}
    logging.getLogger().info(index_param)

    def init_data(self, connect, collection, nb=6000):
        '''
        Generate vectors and add it in collection, before search vectors
        '''
        global vectors
        if nb == 6000:
            add_vectors = vectors
        else:  
            add_vectors = gen_vectors(nb, dim)
        status, ids = connect.add_vectors(collection, add_vectors)
        sleep(add_interval_time)
        return add_vectors, ids

    """
    Test search collection with invalid collection names
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_collection_names()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_search_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        logging.getLogger().info(collection_name)
        top_k = 1
        nprobe = 1 
        query_vecs = gen_vectors(1, dim)
        status, result = connect.search_vectors(collection_name, top_k, query_vecs)
        assert not status.OK()

    @pytest.mark.level(1)
    def test_search_with_invalid_tag_format(self, connect, collection):
        top_k = 1
        nprobe = 1 
        query_vecs = gen_vectors(1, dim)
        with pytest.raises(Exception) as e:
            status, result = connect.search_vectors(collection, top_k, query_vecs, partition_tags="tag")

    """
    Test search collection with invalid top-k
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_top_ks()
    )
    def get_top_k(self, request):
        yield request.param

    @pytest.mark.level(1)
    def test_search_with_invalid_top_k(self, connect, collection, get_top_k):
        '''
        target: test search fuction, with the wrong top_k
        method: search with top_k
        expected: raise an error, and the connection is normal
        '''
        top_k = get_top_k
        logging.getLogger().info(top_k)
        nprobe = 1
        query_vecs = gen_vectors(1, dim)
        if isinstance(top_k, int):
            status, result = connect.search_vectors(collection, top_k, query_vecs)
            assert not status.OK()
        else:
            with pytest.raises(Exception) as e:
                status, result = connect.search_vectors(collection, top_k, query_vecs)

    @pytest.mark.level(2)
    def test_search_with_invalid_top_k_ip(self, connect, ip_collection, get_top_k):
        '''
        target: test search fuction, with the wrong top_k
        method: search with top_k
        expected: raise an error, and the connection is normal
        '''
        top_k = get_top_k
        logging.getLogger().info(top_k)
        nprobe = 1
        query_vecs = gen_vectors(1, dim)
        if isinstance(top_k, int):
            status, result = connect.search_vectors(ip_collection, top_k, query_vecs)
            assert not status.OK()
        else:
            with pytest.raises(Exception) as e:
                status, result = connect.search_vectors(ip_collection, top_k, query_vecs)
    """
    Test search collection with invalid nprobe
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_nprobes()
    )
    def get_nprobes(self, request):
        yield request.param

    @pytest.mark.level(1)
    def test_search_with_invalid_nprobe(self, connect, collection, get_nprobes):
        '''
        target: test search fuction, with the wrong nprobe
        method: search with nprobe
        expected: raise an error, and the connection is normal
        '''
        index_type = IndexType.IVF_SQ8
        index_param = {"nlist": 16384}
        connect.create_index(collection, index_type, index_param)

        top_k = 1
        nprobe = get_nprobes
        search_param = {"nprobe": nprobe}
        logging.getLogger().info(nprobe)
        query_vecs = gen_vectors(1, dim)
        # if isinstance(nprobe, int):
        status, result = connect.search_vectors(collection, top_k, query_vecs, params=search_param)
        assert not status.OK()
        # else:
        #     with pytest.raises(Exception) as e:
        #         status, result = connect.search_vectors(collection, top_k, query_vecs, params=search_param)

    @pytest.mark.level(2)
    def test_search_with_invalid_nprobe_ip(self, connect, ip_collection, get_nprobes):
        '''
        target: test search fuction, with the wrong top_k
        method: search with top_k
        expected: raise an error, and the connection is normal
        '''
        index_type = IndexType.IVF_SQ8
        index_param = {"nlist": 16384}
        connect.create_index(ip_collection, index_type, index_param)

        top_k = 1
        nprobe = get_nprobes
        search_param = {"nprobe": nprobe}
        logging.getLogger().info(nprobe)
        query_vecs = gen_vectors(1, dim)

        # if isinstance(nprobe, int):
        status, result = connect.search_vectors(ip_collection, top_k, query_vecs, params=search_param)
        assert not status.OK()
        # else:
        #     with pytest.raises(Exception) as e:
        #         status, result = connect.search_vectors(ip_collection, top_k, query_vecs, params=search_param)

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

    def test_search_with_empty_params(self, connect, collection, args, get_simple_index):
        '''
        target: test search fuction, with empty search params
        method: search with params
        expected: search status not ok, and the connection is normal
        '''
        if args["handler"] == "HTTP":
            pytest.skip("skip in http mode")
        index_type = get_simple_index["index_type"]
        index_param = get_simple_index["index_param"]
        connect.create_index(collection, index_type, index_param)

        top_k = 1
        query_vecs = gen_vectors(1, dim)
        status, result = connect.search_vectors(collection, top_k, query_vecs, params={})

        if index_type == IndexType.FLAT:
            assert status.OK()
        else:
            assert not status.OK()

    @pytest.fixture(
        scope="function",
        params=gen_invaild_search_params()
    )
    def get_invalid_searh_param(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        return request.param

    def test_search_with_invalid_params(self, connect, collection, get_invalid_searh_param):
        '''
        target: test search fuction, with invalid search params
        method: search with params
        expected: search status not ok, and the connection is normal
        '''
        index_type = get_invalid_searh_param["index_type"]
        search_param = get_invalid_searh_param["search_param"]

        if index_type in [IndexType.IVFLAT, IndexType.IVF_SQ8, IndexType.IVF_SQ8H]:
            connect.create_index(collection, index_type, {"nlist": 16384})
        if (index_type == IndexType.IVF_PQ):
            connect.create_index(collection, index_type, {"nlist": 16384, "m": 10})
        if(index_type == IndexType.HNSW):
            connect.create_index(collection, index_type, {"M": 16, "efConstruction": 500})
        if (index_type == IndexType.RNSG):
            connect.create_index(collection, index_type, {"search_length": 60, "out_degree": 50, "candidate_pool_size": 300, "knng": 100})

        top_k = 1
        query_vecs = gen_vectors(1, dim)
        status, result = connect.search_vectors(collection, top_k, query_vecs, params=search_param)
        assert not status.OK()

def check_result(result, id):
    if len(result) >= 5:
        return id in [result[0].id, result[1].id, result[2].id, result[3].id, result[4].id]
    else:
        return id in (i.id for i in result)
