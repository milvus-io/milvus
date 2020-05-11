import pdb
import pytest
import logging
import itertools
from time import sleep
import threading
from multiprocessing import Process
from milvus import IndexType, MetricType
from utils import *

dim = 128
index_file_size = 10
add_time_interval = 3
tag = "1970-01-01"
nb = 6000

class TestCollectionCount:
    """
    params means different nb, the nb value may trigger merge, or not
    """
    @pytest.fixture(
        scope="function",
        params=[
            1,
            5000,
            20000,
        ],
    )
    def add_vectors_nb(self, request):
        yield request.param

    """
    generate valid create_index params
    """
    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in cpu mode")
        if request.param["index_type"] == IndexType.IVF_PQ:
            pytest.skip("Skip PQ Temporary")
        return request.param

    def test_collection_rows_count(self, connect, collection, add_vectors_nb):
        '''
        target: test collection rows_count is correct or not
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        vectors = gen_vectors(nb, dim)
        res = connect.add_vectors(collection_name=collection, records=vectors)
        connect.flush([collection])
        status, res = connect.count_entities(collection)
        assert res == nb

    def test_collection_rows_count_partition(self, connect, collection, add_vectors_nb):
        '''
        target: test collection rows_count is correct or not
        method: create collection, create partition and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(collection, tag)
        assert status.OK()
        res = connect.add_vectors(collection_name=collection, records=vectors, partition_tag=tag)
        connect.flush([collection])
        status, res = connect.count_entities(collection)
        assert res == nb

    def test_collection_rows_count_multi_partitions_A(self, connect, collection, add_vectors_nb):
        '''
        target: test collection rows_count is correct or not
        method: create collection, create partitions and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        new_tag = "new_tag"
        nb = add_vectors_nb
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(collection, tag)
        status = connect.create_partition(collection, new_tag)
        assert status.OK()
        res = connect.add_vectors(collection_name=collection, records=vectors)
        connect.flush([collection])
        status, res = connect.count_entities(collection)
        assert res == nb

    def test_collection_rows_count_multi_partitions_B(self, connect, collection, add_vectors_nb):
        '''
        target: test collection rows_count is correct or not
        method: create collection, create partitions and add vectors in one of the partitions,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        new_tag = "new_tag"
        nb = add_vectors_nb
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(collection, tag)
        status = connect.create_partition(collection, new_tag)
        assert status.OK()
        res = connect.add_vectors(collection_name=collection, records=vectors, partition_tag=tag)
        connect.flush([collection])
        status, res = connect.count_entities(collection)
        assert res == nb

    def test_collection_rows_count_multi_partitions_C(self, connect, collection, add_vectors_nb):
        '''
        target: test collection rows_count is correct or not
        method: create collection, create partitions and add vectors in one of the partitions,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the collection count is equal to the length of vectors
        '''
        new_tag = "new_tag"
        nb = add_vectors_nb
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(collection, tag)
        status = connect.create_partition(collection, new_tag)
        assert status.OK()
        res = connect.add_vectors(collection_name=collection, records=vectors, partition_tag=tag)
        res = connect.add_vectors(collection_name=collection, records=vectors, partition_tag=new_tag)
        connect.flush([collection])
        status, res = connect.count_entities(collection)
        assert res == nb * 2

    def test_collection_rows_count_after_index_created(self, connect, collection, get_simple_index):
        '''
        target: test count_entities, after index have been created
        method: add vectors in db, and create index, then calling count_entities with correct params 
        expected: count_entities raise exception
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        nb = 100
        vectors = gen_vectors(nb, dim)
        res = connect.add_vectors(collection_name=collection, records=vectors)
        connect.flush([collection])
        connect.create_index(collection, index_type, index_param)
        status, res = connect.count_entities(collection)
        assert res == nb

    # @pytest.mark.level(2)
    # def test_count_without_connection(self, collection, dis_connect):
    #     '''
    #     target: test count_entities, without connection
    #     method: calling count_entities with correct params, with a disconnected instance
    #     expected: count_entities raise exception
    #     '''
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.count_entities(collection)

    def test_collection_rows_count_no_vectors(self, connect, collection):
        '''
        target: test collection rows_count is correct or not, if collection is empty
        method: create collection and no vectors in it,
            assert the value returned by count_entities method is equal to 0
        expected: the count is equal to 0
        '''
        collection_name = gen_unique_str()
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size}
        connect.create_collection(param)        
        status, res = connect.count_entities(collection)
        assert res == 0

    # TODO: enable
    @pytest.mark.level(2)
    @pytest.mark.timeout(20)
    def _test_collection_rows_count_multiprocessing(self, connect, collection, args):
        '''
        target: test collection rows_count is correct or not with multiprocess
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nq = 2
        vectors = gen_vectors(nq, dim)
        res = connect.add_vectors(collection_name=collection, records=vectors)
        time.sleep(add_time_interval)

        def rows_count(milvus):
            status, res = milvus.count_entities(collection)
            logging.getLogger().info(status)
            assert res == nq

        process_num = 8
        processes = []
        for i in range(process_num):
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            p = Process(target=rows_count, args=(milvus, ))
            processes.append(p)
            p.start()
            logging.getLogger().info(p)
        for p in processes:
            p.join()

    def test_collection_rows_count_multi_collections(self, connect):
        '''
        target: test collection rows_count is correct or not with multiple collections of L2
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        collection_list = []
        for i in range(20):
            collection_name = gen_unique_str()
            collection_list.append(collection_name)
            param = {'collection_name': collection_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.L2}
            connect.create_collection(param)
            res = connect.add_vectors(collection_name=collection_name, records=vectors)
        connect.flush(collection_list)
        for i in range(20):
            status, res = connect.count_entities(collection_list[i])
            assert status.OK()
            assert res == nq


class TestCollectionCountIP:
    """
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.fixture(
        scope="function",
        params=[
            1,
            5000,
            20000,
        ],
    )
    def add_vectors_nb(self, request):
        yield request.param

    """
    generate valid create_index params
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if request.param["index_type"] == IndexType.IVF_PQ:
            pytest.skip("Skip PQ Temporary")
        return request.param

    def test_collection_rows_count(self, connect, ip_collection, add_vectors_nb):
        '''
        target: test collection rows_count is correct or not
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        vectors = gen_vectors(nb, dim)
        res = connect.add_vectors(collection_name=ip_collection, records=vectors)
        connect.flush([ip_collection])
        status, res = connect.count_entities(ip_collection)
        assert res == nb

    def test_collection_rows_count_after_index_created(self, connect, ip_collection, get_simple_index):
        '''
        target: test count_entities, after index have been created
        method: add vectors in db, and create index, then calling count_entities with correct params
        expected: count_entities raise exception
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        nb = 100
        vectors = gen_vectors(nb, dim)
        res = connect.add_vectors(collection_name=ip_collection, records=vectors)
        connect.flush([ip_collection])
        connect.create_index(ip_collection, index_type, index_param)
        status, res = connect.count_entities(ip_collection)
        assert res == nb

    # @pytest.mark.level(2)
    # def test_count_without_connection(self, ip_collection, dis_connect):
    #     '''
    #     target: test count_entities, without connection
    #     method: calling count_entities with correct params, with a disconnected instance
    #     expected: count_entities raise exception
    #     '''
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.count_entities(ip_collection)

    def test_collection_rows_count_no_vectors(self, connect, ip_collection):
        '''
        target: test collection rows_count is correct or not, if collection is empty
        method: create collection and no vectors in it,
            assert the value returned by count_entities method is equal to 0
        expected: the count is equal to 0
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size}
        connect.create_collection(param)
        status, res = connect.count_entities(ip_collection)
        assert res == 0

    # TODO: enable
    @pytest.mark.timeout(60)
    def _test_collection_rows_count_multiprocessing(self, connect, ip_collection, args):
        '''
        target: test collection rows_count is correct or not with multiprocess
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nq = 2
        vectors = gen_vectors(nq, dim)
        res = connect.add_vectors(collection_name=ip_collection, records=vectors)
        time.sleep(add_time_interval)

        def rows_count(milvus):
            status, res = milvus.count_entities(ip_collection)
            logging.getLogger().info(status)
            assert res == nq

        process_num = 8
        processes = []
        for i in range(process_num):
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            p = Process(target=rows_count, args=(milvus,))
            processes.append(p)
            p.start()
            logging.getLogger().info(p)
        for p in processes:
            p.join()

    def test_collection_rows_count_multi_collections(self, connect):
        '''
        target: test collection rows_count is correct or not with multiple collections of IP
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        collection_list = []
        for i in range(20):
            collection_name = gen_unique_str('test_collection_rows_count_multi_collections')
            collection_list.append(collection_name)
            param = {'collection_name': collection_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.IP}
            connect.create_collection(param)
            res = connect.add_vectors(collection_name=collection_name, records=vectors)
        connect.flush(collection_list)
        for i in range(20):
            status, res = connect.count_entities(collection_list[i])
            assert status.OK()
            assert res == nq


class TestCollectionCountJAC:
    """
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.fixture(
        scope="function",
        params=[
            1,
            5000,
            20000,
        ],
    )
    def add_vectors_nb(self, request):
        yield request.param

    """
    generate valid create_index params
    """

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

    def test_collection_rows_count(self, connect, jac_collection, add_vectors_nb):
        '''
        target: test collection rows_count is correct or not
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(collection_name=jac_collection, records=vectors)
        connect.flush([jac_collection])
        status, res = connect.count_entities(jac_collection)
        assert res == nb

    def test_collection_rows_count_after_index_created(self, connect, jac_collection, get_jaccard_index):
        '''
        target: test count_entities, after index have been created
        method: add vectors in db, and create index, then calling count_entities with correct params
        expected: count_entities raise exception
        '''
        nb = 100
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(collection_name=jac_collection, records=vectors)
        connect.flush([jac_collection])
        connect.create_index(jac_collection, index_type, index_param)
        status, res = connect.count_entities(jac_collection)
        assert res == nb

    # @pytest.mark.level(2)
    # def test_count_without_connection(self, jac_collection, dis_connect):
    #     '''
    #     target: test count_entities, without connection
    #     method: calling count_entities with correct params, with a disconnected instance
    #     expected: count_entities raise exception
    #     '''
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.count_entities(jac_collection)

    def test_collection_rows_count_no_vectors(self, connect, jac_collection):
        '''
        target: test collection rows_count is correct or not, if collection is empty
        method: create collection and no vectors in it,
            assert the value returned by count_entities method is equal to 0
        expected: the count is equal to 0
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size}
        connect.create_collection(param)
        status, res = connect.count_entities(jac_collection)
        assert res == 0

    def test_collection_rows_count_multi_collections(self, connect):
        '''
        target: test collection rows_count is correct or not with multiple collections of IP
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nq = 100
        tmp, vectors = gen_binary_vectors(nq, dim)
        collection_list = []
        for i in range(20):
            collection_name = gen_unique_str('test_collection_rows_count_multi_collections')
            collection_list.append(collection_name)
            param = {'collection_name': collection_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.JACCARD}
            connect.create_collection(param)
            res = connect.add_vectors(collection_name=collection_name, records=vectors)
        connect.flush(collection_list)
        for i in range(20):
            status, res = connect.count_entities(collection_list[i])
            assert status.OK()
            assert res == nq

class TestCollectionCountBinary:
    """
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.fixture(
        scope="function",
        params=[
            1,
            5000,
            20000,
        ],
    )
    def add_vectors_nb(self, request):
        yield request.param

    """
    generate valid create_index params
    """

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

    def test_collection_rows_count(self, connect, ham_collection, add_vectors_nb):
        '''
        target: test collection rows_count is correct or not
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(collection_name=ham_collection, records=vectors)
        connect.flush([ham_collection])
        status, res = connect.count_entities(ham_collection)
        assert res == nb

    def test_collection_rows_count_substructure(self, connect, substructure_collection, add_vectors_nb):
        '''
        target: test collection rows_count is correct or not
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(collection_name=substructure_collection, records=vectors)
        connect.flush([substructure_collection])
        status, res = connect.count_entities(substructure_collection)
        assert res == nb

    def test_collection_rows_count_superstructure(self, connect, superstructure_collection, add_vectors_nb):
        '''
        target: test collection rows_count is correct or not
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(collection_name=superstructure_collection, records=vectors)
        connect.flush([superstructure_collection])
        status, res = connect.count_entities(superstructure_collection)
        assert res == nb

    def test_collection_rows_count_after_index_created(self, connect, ham_collection, get_hamming_index):
        '''
        target: test count_entities, after index have been created
        method: add vectors in db, and create index, then calling count_entities with correct params
        expected: count_entities raise exception
        '''
        nb = 100
        index_type = get_hamming_index["index_type"]
        index_param = get_hamming_index["index_param"]
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(collection_name=ham_collection, records=vectors)
        connect.flush([ham_collection])
        connect.create_index(ham_collection, index_type, index_param)
        status, res = connect.count_entities(ham_collection)
        assert res == nb

    def test_collection_rows_count_after_index_created_substructure(self, connect, substructure_collection, get_substructure_index):
        '''
        target: test count_entities, after index have been created
        method: add vectors in db, and create index, then calling count_entities with correct params
        expected: count_entities raise exception
        '''
        nb = 100
        index_type = get_substructure_index["index_type"]
        index_param = get_substructure_index["index_param"]
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(collection_name=substructure_collection, records=vectors)
        connect.flush([substructure_collection])
        connect.create_index(substructure_collection, index_type, index_param)
        status, res = connect.count_entities(substructure_collection)
        assert res == nb

    def test_collection_rows_count_after_index_created_superstructure(self, connect, superstructure_collection, get_superstructure_index):
        '''
        target: test count_entities, after index have been created
        method: add vectors in db, and create index, then calling count_entities with correct params
        expected: count_entities raise exception
        '''
        nb = 100
        index_type = get_superstructure_index["index_type"]
        index_param = get_superstructure_index["index_param"]
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(collection_name=superstructure_collection, records=vectors)
        connect.flush([superstructure_collection])
        connect.create_index(superstructure_collection, index_type, index_param)
        status, res = connect.count_entities(superstructure_collection)
        assert res == nb

    # @pytest.mark.level(2)
    # def test_count_without_connection(self, ham_collection, dis_connect):
    #     '''
    #     target: test count_entities, without connection
    #     method: calling count_entities with correct params, with a disconnected instance
    #     expected: count_entities raise exception
    #     '''
    #     with pytest.raises(Exception) as e:
    #         status = dis_connect.count_entities(ham_collection)

    def test_collection_rows_count_no_vectors(self, connect, ham_collection):
        '''
        target: test collection rows_count is correct or not, if collection is empty
        method: create collection and no vectors in it,
            assert the value returned by count_entities method is equal to 0
        expected: the count is equal to 0
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size}
        connect.create_collection(param)
        status, res = connect.count_entities(ham_collection)
        assert res == 0

    def test_collection_rows_count_multi_collections(self, connect):
        '''
        target: test collection rows_count is correct or not with multiple collections of IP
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nq = 100
        tmp, vectors = gen_binary_vectors(nq, dim)
        collection_list = []
        for i in range(20):
            collection_name = gen_unique_str('test_collection_rows_count_multi_collections')
            collection_list.append(collection_name)
            param = {'collection_name': collection_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.HAMMING}
            connect.create_collection(param)
            res = connect.add_vectors(collection_name=collection_name, records=vectors)
        connect.flush(collection_list)
        for i in range(20):
            status, res = connect.count_entities(collection_list[i])
            assert status.OK()
            assert res == nq


class TestCollectionCountTANIMOTO:
    """
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.fixture(
        scope="function",
        params=[
            1,
            5000,
            20000,
        ],
    )
    def add_vectors_nb(self, request):
        yield request.param

    """
    generate valid create_index params
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_tanimoto_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] == IndexType.IVFLAT or request.param["index_type"] == IndexType.FLAT:
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    def test_collection_rows_count(self, connect, tanimoto_collection, add_vectors_nb):
        '''
        target: test collection rows_count is correct or not
        method: create collection and add vectors in it,
            assert the value returned by count_entities method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(collection_name=tanimoto_collection, records=vectors)
        connect.flush([tanimoto_collection])
        status, res = connect.count_entities(tanimoto_collection)
        assert status.OK()
        assert res == nb
