import pdb
import copy
import pytest
import threading
import datetime
import logging
from time import sleep
from multiprocessing import Process
import numpy
from milvus import Milvus, IndexType, MetricType
from utils import *

dim = 128
table_id = "test_search"
add_interval_time = 2
vectors = gen_vectors(100, dim)
# vectors /= numpy.linalg.norm(vectors)
# vectors = vectors.tolist()
nprobe = 1
epsilon = 0.001
tag = "1970-01-01"


class TestSearchBase:
    def init_data(self, connect, table, nb=100):
        '''
        Generate vectors and add it in table, before search vectors
        '''
        global vectors
        if nb == 100:
            add_vectors = vectors
        else:  
            add_vectors = gen_vectors(nb, dim)
            # add_vectors /= numpy.linalg.norm(add_vectors)
            # add_vectors = add_vectors.tolist()
        status, ids = connect.add_vectors(table, add_vectors)
        sleep(add_interval_time)
        return add_vectors, ids

    """
    generate valid create_index params
    """
    @pytest.fixture(
        scope="function",
        params=gen_index_params()
    )
    def get_index_params(self, request, args):
        if "internal" not in args:
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in open source")
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("skip pq case temporary")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index_params()
    )
    def get_simple_index_params(self, request, args):
        if "internal" not in args:
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in open source")
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("skip pq case temporary")
        return request.param
    """
    generate top-k params
    """
    @pytest.fixture(
        scope="function",
        params=[1, 99, 1024, 2048, 2049]
    )
    def get_top_k(self, request):
        yield request.param


    def test_search_top_k_flat_index(self, connect, table, get_top_k):
        '''
        target: test basic search fuction, all the search params is corrent, change top-k value
        method: search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        vectors, ids = self.init_data(connect, table)
        query_vec = [vectors[0]]
        top_k = get_top_k
        nprobe = 1
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec)
        if top_k <= 2048:
            assert status.OK()
            assert len(result[0]) == min(len(vectors), top_k)
            assert result[0][0].distance <= epsilon
            assert check_result(result[0], ids[0])
        else:
            assert not status.OK()

    def test_search_l2_index_params(self, connect, table, get_simple_index_params):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_params = get_simple_index_params
        logging.getLogger().info(index_params)
        vectors, ids = self.init_data(connect, table)
        status = connect.create_index(table, index_params)
        query_vec = [vectors[0]]
        top_k = 10
        nprobe = 1
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec)
        logging.getLogger().info(result)
        if top_k <= 1024:
            assert status.OK()
            assert len(result[0]) == min(len(vectors), top_k)
            assert check_result(result[0], ids[0])
            assert result[0][0].distance <= epsilon
        else:
            assert not status.OK()

    def test_search_l2_index_params_partition(self, connect, table, get_simple_index_params):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: add vectors into table, search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k, search table with partition tag return empty
        '''
        index_params = get_simple_index_params
        logging.getLogger().info(index_params)
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        vectors, ids = self.init_data(connect, table)
        status = connect.create_index(table, index_params)
        query_vec = [vectors[0]]
        top_k = 10
        nprobe = 1
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result[0]) == min(len(vectors), top_k)
        assert check_result(result[0], ids[0])
        assert result[0][0].distance <= epsilon
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec, partition_tags=[tag])
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == 0

    def test_search_l2_index_params_partition_A(self, connect, table, get_simple_index_params):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search partition with the given vectors, check the result
        expected: search status ok, and the length of the result is 0
        '''
        index_params = get_simple_index_params
        logging.getLogger().info(index_params)
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        vectors, ids = self.init_data(connect, table)
        status = connect.create_index(table, index_params)
        query_vec = [vectors[0]]
        top_k = 10
        nprobe = 1
        status, result = connect.search_vectors(partition_name, top_k, nprobe, query_vec, partition_tags=[tag])
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == 0

    def test_search_l2_index_params_partition_B(self, connect, table, get_simple_index_params):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_params = get_simple_index_params
        logging.getLogger().info(index_params)
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        vectors, ids = self.init_data(connect, partition_name)
        status = connect.create_index(table, index_params)
        query_vec = [vectors[0]]
        top_k = 10
        nprobe = 1
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result[0]) == min(len(vectors), top_k)
        assert check_result(result[0], ids[0])
        assert result[0][0].distance <= epsilon
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec, partition_tags=[tag])
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result[0]) == min(len(vectors), top_k)
        assert check_result(result[0], ids[0])
        assert result[0][0].distance <= epsilon
        status, result = connect.search_vectors(partition_name, top_k, nprobe, query_vec, partition_tags=[tag])
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == 0

    def test_search_l2_index_params_partition_C(self, connect, table, get_simple_index_params):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors and tags (one of the tags not existed in table), check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_params = get_simple_index_params
        logging.getLogger().info(index_params)
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        vectors, ids = self.init_data(connect, partition_name)
        status = connect.create_index(table, index_params)
        query_vec = [vectors[0]]
        top_k = 10
        nprobe = 1
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec, partition_tags=[tag, "new_tag"])
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result[0]) == min(len(vectors), top_k)
        assert check_result(result[0], ids[0])
        assert result[0][0].distance <= epsilon

    def test_search_l2_index_params_partition_D(self, connect, table, get_simple_index_params):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors and tag (tag name not existed in table), check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_params = get_simple_index_params
        logging.getLogger().info(index_params)
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        vectors, ids = self.init_data(connect, partition_name)
        status = connect.create_index(table, index_params)
        query_vec = [vectors[0]]
        top_k = 10
        nprobe = 1
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec, partition_tags=["new_tag"])
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == 0

    def test_search_l2_index_params_partition_E(self, connect, table, get_simple_index_params):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search table with the given vectors and tags, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        new_tag = "new_tag"
        index_params = get_simple_index_params
        logging.getLogger().info(index_params)
        partition_name = gen_unique_str()
        new_partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        status = connect.create_partition(table, new_partition_name, new_tag)
        vectors, ids = self.init_data(connect, partition_name)
        new_vectors, new_ids = self.init_data(connect, new_partition_name, nb=1000)
        status = connect.create_index(table, index_params)
        query_vec = [vectors[0], new_vectors[0]]
        top_k = 10
        nprobe = 1
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec, partition_tags=[tag, new_tag])
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result[0]) == min(len(vectors), top_k)
        assert check_result(result[0], ids[0])
        assert check_result(result[1], new_ids[0])
        assert result[0][0].distance <= epsilon
        assert result[1][0].distance <= epsilon
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec, partition_tags=[new_tag])
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result[0]) == min(len(vectors), top_k)
        assert check_result(result[1], new_ids[0])
        assert result[1][0].distance <= epsilon

    def test_search_l2_index_params_partition_F(self, connect, table, get_simple_index_params):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search table with the given vectors and tags with "re" expr, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        tag = "atag"
        new_tag = "new_tag"
        index_params = get_simple_index_params
        logging.getLogger().info(index_params)
        partition_name = gen_unique_str()
        new_partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        status = connect.create_partition(table, new_partition_name, new_tag)
        vectors, ids = self.init_data(connect, partition_name)
        new_vectors, new_ids = self.init_data(connect, new_partition_name, nb=1000)
        status = connect.create_index(table, index_params)
        query_vec = [vectors[0], new_vectors[0]]
        top_k = 10
        nprobe = 1
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec, partition_tags=["new(.*)"])
        logging.getLogger().info(result)
        assert status.OK()
        assert result[0][0].distance > epsilon
        assert result[1][0].distance <= epsilon
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec, partition_tags=["(.*)tag"])
        logging.getLogger().info(result)
        assert status.OK()
        assert result[0][0].distance <= epsilon
        assert result[1][0].distance <= epsilon

    def test_search_ip_index_params(self, connect, ip_table, get_simple_index_params):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        
        index_params = get_simple_index_params
        logging.getLogger().info(index_params)
        vectors, ids = self.init_data(connect, ip_table)
        status = connect.create_index(ip_table, index_params)
        query_vec = [vectors[0]]
        top_k = 10
        nprobe = 1
        status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vec)
        logging.getLogger().info(result)

        if top_k <= 1024:
            assert status.OK()
            assert len(result[0]) == min(len(vectors), top_k)
            assert check_result(result[0], ids[0])
            assert abs(result[0][0].distance - numpy.inner(numpy.array(query_vec[0]), numpy.array(query_vec[0]))) <= gen_inaccuracy(result[0][0].distance)
        else:
            assert not status.OK()

    def test_search_ip_index_params_partition(self, connect, ip_table, get_simple_index_params):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_params = get_simple_index_params
        logging.getLogger().info(index_params)
        partition_name = gen_unique_str()
        status = connect.create_partition(ip_table, partition_name, tag)
        vectors, ids = self.init_data(connect, ip_table)
        status = connect.create_index(ip_table, index_params)
        query_vec = [vectors[0]]
        top_k = 10
        nprobe = 1
        status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vec)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result[0]) == min(len(vectors), top_k)
        assert check_result(result[0], ids[0])
        assert abs(result[0][0].distance - numpy.inner(numpy.array(query_vec[0]), numpy.array(query_vec[0]))) <= gen_inaccuracy(result[0][0].distance)
        status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vec, partition_tags=[tag])
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == 0

    def test_search_ip_index_params_partition_A(self, connect, ip_table, get_simple_index_params):
        '''
        target: test basic search fuction, all the search params is corrent, test all index params, and build
        method: search with the given vectors and tag, check the result
        expected: search status ok, and the length of the result is top_k
        '''
        index_params = get_simple_index_params
        logging.getLogger().info(index_params)
        partition_name = gen_unique_str()
        status = connect.create_partition(ip_table, partition_name, tag)
        vectors, ids = self.init_data(connect, partition_name)
        status = connect.create_index(ip_table, index_params)
        query_vec = [vectors[0]]
        top_k = 10
        nprobe = 1
        status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vec, partition_tags=[tag])
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result[0]) == min(len(vectors), top_k)
        assert check_result(result[0], ids[0])
        assert abs(result[0][0].distance - numpy.inner(numpy.array(query_vec[0]), numpy.array(query_vec[0]))) <= gen_inaccuracy(result[0][0].distance)
        status, result = connect.search_vectors(partition_name, top_k, nprobe, query_vec)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result[0]) == min(len(vectors), top_k)
        assert check_result(result[0], ids[0])

    @pytest.mark.level(2)
    def test_search_vectors_without_connect(self, dis_connect, table):
        '''
        target: test search vectors without connection
        method: use dis connected instance, call search method and check if search successfully
        expected: raise exception
        '''
        query_vectors = [vectors[0]]
        top_k = 1
        nprobe = 1
        with pytest.raises(Exception) as e:
            status, ids = dis_connect.search_vectors(table, top_k, nprobe, query_vectors)

    def test_search_table_name_not_existed(self, connect, table):
        '''
        target: search table not existed
        method: search with the random table_name, which is not in db
        expected: status not ok
        '''
        table_name = gen_unique_str("not_existed_table")
        top_k = 1
        nprobe = 1
        query_vecs = [vectors[0]]
        status, result = connect.search_vectors(table_name, top_k, nprobe, query_vecs)
        assert not status.OK()

    def test_search_table_name_None(self, connect, table):
        '''
        target: search table that table name is None
        method: search with the table_name: None
        expected: status not ok
        '''
        table_name = None
        top_k = 1
        nprobe = 1
        query_vecs = [vectors[0]]
        with pytest.raises(Exception) as e: 
            status, result = connect.search_vectors(table_name, top_k, nprobe, query_vecs)

    def test_search_top_k_query_records(self, connect, table):
        '''
        target: test search fuction, with search params: query_records
        method: search with the given query_records, which are subarrays of the inserted vectors
        expected: status ok and the returned vectors should be query_records
        '''
        top_k = 10
        nprobe = 1
        vectors, ids = self.init_data(connect, table)
        query_vecs = [vectors[0],vectors[55],vectors[99]]
        status, result = connect.search_vectors(table, top_k, nprobe, query_vecs)
        assert status.OK()
        assert len(result) == len(query_vecs)
        for i in range(len(query_vecs)):
            assert len(result[i]) == top_k
            assert result[i][0].distance <= epsilon

    """
    generate invalid query range params
    """
    @pytest.fixture(
        scope="function",
        params=[
            (get_current_day(), get_current_day()),
            (get_last_day(1), get_last_day(1)),
            (get_next_day(1), get_next_day(1))
        ]
    )
    def get_invalid_range(self, request):
        yield request.param

    # disable
    def _test_search_invalid_query_ranges(self, connect, table, get_invalid_range):
        '''
        target: search table with query ranges
        method: search with the same query ranges
        expected: status not ok
        '''
        top_k = 2
        nprobe = 1
        vectors, ids = self.init_data(connect, table)
        query_vecs = [vectors[0]]
        query_ranges = [get_invalid_range]
        status, result = connect.search_vectors(table, top_k, nprobe, query_vecs, query_ranges=query_ranges)
        assert not status.OK()
        assert len(result) == 0

    """
    generate valid query range params, no search result
    """
    @pytest.fixture(
        scope="function",
        params=[
            (get_last_day(2), get_last_day(1)),
            (get_next_day(1), get_next_day(2))
        ]
    )
    def get_valid_range_no_result(self, request):
        yield request.param

    # disable
    def _test_search_valid_query_ranges_no_result(self, connect, table, get_valid_range_no_result):
        '''
        target: search table with normal query ranges, but no data in db
        method: search with query ranges (low, low)
        expected: length of result is 0
        '''
        top_k = 2
        nprobe = 1
        vectors, ids = self.init_data(connect, table)
        query_vecs = [vectors[0]]
        query_ranges = [get_valid_range_no_result]
        status, result = connect.search_vectors(table, top_k, nprobe, query_vecs, query_ranges=query_ranges)
        assert status.OK()
        assert len(result) == 0

    """
    generate valid query range params, no search result
    """
    @pytest.fixture(
        scope="function",
        params=[
            (get_last_day(2), get_next_day(2)),
            (get_current_day(), get_next_day(2)),
        ]
    )
    def get_valid_range(self, request):
        yield request.param

    # disable
    def _test_search_valid_query_ranges(self, connect, table, get_valid_range):
        '''
        target: search table with normal query ranges, but no data in db
        method: search with query ranges (low, normal)
        expected: length of result is 0
        '''
        top_k = 2
        nprobe = 1
        vectors, ids = self.init_data(connect, table)
        query_vecs = [vectors[0]]
        query_ranges = [get_valid_range]
        status, result = connect.search_vectors(table, top_k, nprobe, query_vecs, query_ranges=query_ranges)
        assert status.OK()
        assert len(result) == 1
        assert result[0][0].distance <= epsilon

    def test_search_distance_l2_flat_index(self, connect, table):
        '''
        target: search table, and check the result: distance
        method: compare the return distance value with value computed with Euclidean
        expected: the return distance equals to the computed value
        '''
        nb = 2
        top_k = 1
        nprobe = 1
        vectors, ids = self.init_data(connect, table, nb=nb)
        query_vecs = [[0.50 for i in range(dim)]]
        distance_0 = numpy.linalg.norm(numpy.array(query_vecs[0]) - numpy.array(vectors[0]))
        distance_1 = numpy.linalg.norm(numpy.array(query_vecs[0]) - numpy.array(vectors[1]))
        status, result = connect.search_vectors(table, top_k, nprobe, query_vecs)
        assert abs(numpy.sqrt(result[0][0].distance) - min(distance_0, distance_1)) <= gen_inaccuracy(result[0][0].distance)

    def test_search_distance_ip_flat_index(self, connect, ip_table):
        '''
        target: search ip_table, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        '''
        nb = 2
        top_k = 1
        nprobe = 1
        vectors, ids = self.init_data(connect, ip_table, nb=nb)
        index_params = {
            "index_type": IndexType.FLAT,
            "nlist": 16384
        }
        connect.create_index(ip_table, index_params)
        logging.getLogger().info(connect.describe_index(ip_table))
        query_vecs = [[0.50 for i in range(dim)]]
        distance_0 = numpy.inner(numpy.array(query_vecs[0]), numpy.array(vectors[0]))
        distance_1 = numpy.inner(numpy.array(query_vecs[0]), numpy.array(vectors[1]))
        status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vecs)
        assert abs(result[0][0].distance - max(distance_0, distance_1)) <= gen_inaccuracy(result[0][0].distance)

    def test_search_distance_ip_index_params(self, connect, ip_table, get_index_params):
        '''
        target: search table, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        '''
        top_k = 2
        nprobe = 1
        vectors, ids = self.init_data(connect, ip_table, nb=2)
        index_params = get_index_params
        connect.create_index(ip_table, index_params)
        logging.getLogger().info(connect.describe_index(ip_table))
        query_vecs = [[0.50 for i in range(dim)]]
        status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vecs)
        distance_0 = numpy.inner(numpy.array(query_vecs[0]), numpy.array(vectors[0]))
        distance_1 = numpy.inner(numpy.array(query_vecs[0]), numpy.array(vectors[1]))
        assert abs(result[0][0].distance - max(distance_0, distance_1)) <= gen_inaccuracy(result[0][0].distance)

    # TODO: enable
    # @pytest.mark.repeat(5)
    @pytest.mark.timeout(30)
    def _test_search_concurrent(self, connect, table):
        vectors, ids = self.init_data(connect, table)
        thread_num = 10
        nb = 100
        top_k = 10
        threads = []
        query_vecs = vectors[nb//2:nb]
        def search():
            status, result = connect.search_vectors(table, top_k, query_vecs)
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
        table = gen_unique_str("test_search_concurrent_multiprocessing")
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        param = {'table_name': table,
             'dimension': dim,
             'index_type': IndexType.FLAT,
             'store_raw_vector': False}
        # create table
        milvus = Milvus()
        milvus.connect(uri=uri)
        milvus.create_table(param)
        vectors, ids = self.init_data(milvus, table, nb=nb)
        query_vecs = vectors[nb//2:nb]
        def search(milvus):
            status, result = milvus.search_vectors(table, top_k, query_vecs)
            assert len(result) == len(query_vecs)
            for i in range(len(query_vecs)):
                assert result[i][0].id in ids
                assert result[i][0].distance == 0.0

        for i in range(process_num):
            milvus = Milvus()
            milvus.connect(uri=uri)
            p = Process(target=search, args=(milvus, ))
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()

    def test_search_multi_table_L2(search, args):
        '''
        target: test search multi tables of L2
        method: add vectors into 10 tables, and search
        expected: search status ok, the length of result
        '''
        num = 10
        top_k = 10
        nprobe = 1
        tables = []
        idx = []
        for i in range(num):
            table = gen_unique_str("test_add_multitable_%d" % i)
            uri = "tcp://%s:%s" % (args["ip"], args["port"])
            param = {'table_name': table,
                     'dimension': dim,
                     'index_file_size': 10,
                     'metric_type': MetricType.L2}
            # create table
            milvus = Milvus()
            milvus.connect(uri=uri)
            milvus.create_table(param)
            status, ids = milvus.add_vectors(table, vectors)
            assert status.OK()
            assert len(ids) == len(vectors)
            tables.append(table)
            idx.append(ids[0])
            idx.append(ids[10])
            idx.append(ids[20])
        time.sleep(6)
        query_vecs = [vectors[0], vectors[10], vectors[20]]
        # start query from random table
        for i in range(num):
            table = tables[i]
            status, result = milvus.search_vectors(table, top_k, nprobe, query_vecs)
            assert status.OK()
            assert len(result) == len(query_vecs)
            for j in range(len(query_vecs)):
                assert len(result[j]) == top_k
            for j in range(len(query_vecs)):
                assert check_result(result[j], idx[3 * i + j])

    def test_search_multi_table_IP(search, args):
        '''
        target: test search multi tables of IP
        method: add vectors into 10 tables, and search
        expected: search status ok, the length of result
        '''
        num = 10
        top_k = 10
        nprobe = 1
        tables = []
        idx = []
        for i in range(num):
            table = gen_unique_str("test_add_multitable_%d" % i)
            uri = "tcp://%s:%s" % (args["ip"], args["port"])
            param = {'table_name': table,
                     'dimension': dim,
                     'index_file_size': 10,
                     'metric_type': MetricType.L2}
            # create table
            milvus = Milvus()
            milvus.connect(uri=uri)
            milvus.create_table(param)
            status, ids = milvus.add_vectors(table, vectors)
            assert status.OK()
            assert len(ids) == len(vectors)
            tables.append(table)
            idx.append(ids[0])
            idx.append(ids[10])
            idx.append(ids[20])
        time.sleep(6)
        query_vecs = [vectors[0], vectors[10], vectors[20]]
        # start query from random table
        for i in range(num):
            table = tables[i]
            status, result = milvus.search_vectors(table, top_k, nprobe, query_vecs)
            assert status.OK()
            assert len(result) == len(query_vecs)
            for j in range(len(query_vecs)):
                assert len(result[j]) == top_k
            for j in range(len(query_vecs)):
                assert check_result(result[j], idx[3 * i + j])
"""
******************************************************************
#  The following cases are used to test `search_vectors` function 
#  with invalid table_name top-k / nprobe / query_range
******************************************************************
"""

class TestSearchParamsInvalid(object):
    nlist = 16384
    index_param = {"index_type": IndexType.IVF_SQ8, "nlist": nlist}
    logging.getLogger().info(index_param)

    def init_data(self, connect, table, nb=100):
        '''
        Generate vectors and add it in table, before search vectors
        '''
        global vectors
        if nb == 100:
            add_vectors = vectors
        else:  
            add_vectors = gen_vectors(nb, dim)
        status, ids = connect.add_vectors(table, add_vectors)
        sleep(add_interval_time)
        return add_vectors, ids

    """
    Test search table with invalid table names
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_table_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_search_with_invalid_tablename(self, connect, get_table_name):
        table_name = get_table_name
        logging.getLogger().info(table_name)
        top_k = 1
        nprobe = 1 
        query_vecs = gen_vectors(1, dim)
        status, result = connect.search_vectors(table_name, top_k, nprobe, query_vecs)
        assert not status.OK()

    @pytest.mark.level(1)
    def test_search_with_invalid_tag_format(self, connect, table):
        top_k = 1
        nprobe = 1 
        query_vecs = gen_vectors(1, dim)
        with pytest.raises(Exception) as e:
            status, result = connect.search_vectors(table_name, top_k, nprobe, query_vecs, partition_tags="tag")

    """
    Test search table with invalid top-k
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_top_ks()
    )
    def get_top_k(self, request):
        yield request.param

    @pytest.mark.level(1)
    def test_search_with_invalid_top_k(self, connect, table, get_top_k):
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
            status, result = connect.search_vectors(table, top_k, nprobe, query_vecs)
            assert not status.OK()
        else:
            with pytest.raises(Exception) as e:
                status, result = connect.search_vectors(table, top_k, nprobe, query_vecs)

    @pytest.mark.level(2)
    def test_search_with_invalid_top_k_ip(self, connect, ip_table, get_top_k):
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
            status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vecs)
            assert not status.OK()
        else:
            with pytest.raises(Exception) as e:
                status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vecs)
    """
    Test search table with invalid nprobe
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_nprobes()
    )
    def get_nprobes(self, request):
        yield request.param

    @pytest.mark.level(1)
    def test_search_with_invalid_nprobe(self, connect, table, get_nprobes):
        '''
        target: test search fuction, with the wrong top_k
        method: search with top_k
        expected: raise an error, and the connection is normal
        '''
        top_k = 1
        nprobe = get_nprobes
        logging.getLogger().info(nprobe)
        query_vecs = gen_vectors(1, dim)
        if isinstance(nprobe, int):
            status, result = connect.search_vectors(table, top_k, nprobe, query_vecs)
            assert not status.OK()
        else:
            with pytest.raises(Exception) as e:
                status, result = connect.search_vectors(table, top_k, nprobe, query_vecs)

    @pytest.mark.level(2)
    def test_search_with_invalid_nprobe_ip(self, connect, ip_table, get_nprobes):
        '''
        target: test search fuction, with the wrong top_k
        method: search with top_k
        expected: raise an error, and the connection is normal
        '''
        top_k = 1
        nprobe = get_nprobes
        logging.getLogger().info(nprobe)
        query_vecs = gen_vectors(1, dim)
        if isinstance(nprobe, int):
            status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vecs)
            assert not status.OK()
        else:
            with pytest.raises(Exception) as e:
                status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vecs)

    """
    Test search table with invalid query ranges
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_query_ranges()
    )
    def get_query_ranges(self, request):
        yield request.param

    # disable
    @pytest.mark.level(1)
    def _test_search_flat_with_invalid_query_range(self, connect, table, get_query_ranges):
        '''
        target: test search fuction, with the wrong query_range
        method: search with query_range
        expected: raise an error, and the connection is normal
        '''
        top_k = 1
        nprobe = 1
        query_vecs = [vectors[0]]
        query_ranges = get_query_ranges
        logging.getLogger().info(query_ranges)
        with pytest.raises(Exception) as e:
            status, result = connect.search_vectors(table, 1, nprobe, query_vecs, query_ranges=query_ranges)

    # disable
    @pytest.mark.level(2)
    def _test_search_flat_with_invalid_query_range_ip(self, connect, ip_table, get_query_ranges):
        '''
        target: test search fuction, with the wrong query_range
        method: search with query_range
        expected: raise an error, and the connection is normal
        '''
        top_k = 1
        nprobe = 1
        query_vecs = [vectors[0]]
        query_ranges = get_query_ranges
        logging.getLogger().info(query_ranges)
        with pytest.raises(Exception) as e:
            status, result = connect.search_vectors(ip_table, 1, nprobe, query_vecs, query_ranges=query_ranges)


def check_result(result, id):
    if len(result) >= 5:
        return id in [result[0].id, result[1].id, result[2].id, result[3].id, result[4].id]
    else:
        return id in (i.id for i in result)