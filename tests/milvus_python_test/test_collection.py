import pdb
import pytest
import logging
import itertools
from time import sleep
from multiprocessing import Process
from milvus import IndexType, MetricType
from utils import *

dim = 128
drop_collection_interval_time = 3
index_file_size = 10
vectors = gen_vectors(100, dim)


class TestCollection:

    """
    ******************************************************************
      The following cases are used to test `create_collection` function
    ******************************************************************
    """

    def test_create_collection(self, connect):
        '''
        target: test create normal collection 
        method: create collection with corrent params
        expected: create status return ok
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.L2}
        status = connect.create_collection(param)
        assert status.OK()

    def test_create_collection_ip(self, connect):
        '''
        target: test create normal collection 
        method: create collection with corrent params
        expected: create status return ok
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.IP}
        status = connect.create_collection(param)
        assert status.OK()

    def test_create_collection_jaccard(self, connect):
        '''
        target: test create normal collection 
        method: create collection with corrent params
        expected: create status return ok
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.JACCARD}
        status = connect.create_collection(param)
        assert status.OK()

    def test_create_collection_hamming(self, connect):
        '''
        target: test create normal collection
        method: create collection with corrent params
        expected: create status return ok
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.HAMMING}
        status = connect.create_collection(param)
        assert status.OK()

    def test_create_collection_substructure(self, connect):
        '''
        target: test create normal collection
        method: create collection with corrent params
        expected: create status return ok
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.SUBSTRUCTURE}
        status = connect.create_collection(param)
        assert status.OK()

    def test_create_collection_superstructure(self, connect):
        '''
        target: test create normal collection
        method: create collection with corrent params
        expected: create status return ok
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.SUPERSTRUCTURE}
        status = connect.create_collection(param)
        assert status.OK()

    @pytest.mark.level(2)
    def test_create_collection_without_connection(self, dis_connect):
        '''
        target: test create collection, without connection
        method: create collection with correct params, with a disconnected instance
        expected: create raise exception
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.L2}
        with pytest.raises(Exception) as e:
            status = dis_connect.create_collection(param)

    def test_create_collection_existed(self, connect):
        '''
        target: test create collection but the collection name have already existed
        method: create collection with the same collection_name
        expected: create status return not ok
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.L2}
        status = connect.create_collection(param)
        status = connect.create_collection(param)
        assert not status.OK()

    @pytest.mark.level(2)
    def test_create_collection_existed_ip(self, connect):
        '''
        target: test create collection but the collection name have already existed
        method: create collection with the same collection_name
        expected: create status return not ok
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.IP}
        status = connect.create_collection(param)
        status = connect.create_collection(param)
        assert not status.OK()

    def test_create_collection_None(self, connect):
        '''
        target: test create collection but the collection name is None
        method: create collection, param collection_name is None
        expected: create raise error
        '''
        param = {'collection_name': None,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.L2}
        with pytest.raises(Exception) as e:
            status = connect.create_collection(param)

    def test_create_collection_no_dimension(self, connect):
        '''
        target: test create collection with no dimension params
        method: create collection with corrent params
        expected: create status return ok
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        with pytest.raises(Exception) as e:
            status = connect.create_collection(param)

    def test_create_collection_no_file_size(self, connect):
        '''
        target: test create collection with no index_file_size params
        method: create collection with corrent params
        expected: create status return ok, use default 1024
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'metric_type': MetricType.L2}
        status = connect.create_collection(param)
        logging.getLogger().info(status)
        status, result = connect.describe_collection(collection_name)
        logging.getLogger().info(result)
        assert result.index_file_size == 1024

    def test_create_collection_no_metric_type(self, connect):
        '''
        target: test create collection with no metric_type params
        method: create collection with corrent params
        expected: create status return ok, use default L2
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size}
        status = connect.create_collection(param)
        status, result = connect.describe_collection(collection_name)
        logging.getLogger().info(result)
        assert result.metric_type == MetricType.L2

    """
    ******************************************************************
      The following cases are used to test `describe_collection` function
    ******************************************************************
    """

    def test_collection_describe_result(self, connect):
        '''
        target: test describe collection created with correct params 
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        connect.create_collection(param)
        status, res = connect.describe_collection(collection_name)
        assert res.collection_name == collection_name
        assert res.metric_type == MetricType.L2

    @pytest.mark.level(2)
    def test_collection_describe_collection_name_ip(self, connect):
        '''
        target: test describe collection created with correct params 
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.IP}
        connect.create_collection(param)
        status, res = connect.describe_collection(collection_name)
        assert res.collection_name == collection_name
        assert res.metric_type == MetricType.IP

    @pytest.mark.level(2)
    def test_collection_describe_collection_name_jaccard(self, connect):
        '''
        target: test describe collection created with correct params 
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.JACCARD}
        connect.create_collection(param)
        status, res = connect.describe_collection(collection_name)
        assert res.collection_name == collection_name
        assert res.metric_type == MetricType.JACCARD

    @pytest.mark.level(2)
    def test_collection_describe_collection_name_hamming(self, connect):
        '''
        target: test describe collection created with correct params
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.HAMMING}
        connect.create_collection(param)
        status, res = connect.describe_collection(collection_name)
        assert res.collection_name == collection_name
        assert res.metric_type == MetricType.HAMMING

    def test_collection_describe_collection_name_substructure(self, connect):
        '''
        target: test describe collection created with correct params
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.SUBSTRUCTURE}
        connect.create_collection(param)
        status, res = connect.describe_collection(collection_name)
        assert res.collection_name == collection_name
        assert res.metric_type == MetricType.SUBSTRUCTURE

    def test_collection_describe_collection_name_superstructure(self, connect):
        '''
        target: test describe collection created with correct params
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.SUPERSTRUCTURE}
        connect.create_collection(param)
        status, res = connect.describe_collection(collection_name)
        assert res.collection_name == collection_name
        assert res.metric_type == MetricType.SUPERSTRUCTURE

    # TODO: enable
    @pytest.mark.level(2)
    def _test_collection_describe_collection_name_multiprocessing(self, connect, args):
        '''
        target: test describe collection created with multiprocess 
        method: create collection, assert the value returned by describe method
        expected: collection_name equals with the collection name created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.L2}
        connect.create_collection(param)

        def describecollection(milvus):
            status, res = milvus.describe_collection(collection_name)
            assert res.collection_name == collection_name

        process_num = 4
        processes = []
        for i in range(process_num):
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            p = Process(target=describecollection, args=(milvus,))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()
    
    @pytest.mark.level(2)
    def test_collection_describe_without_connection(self, collection, dis_connect):
        '''
        target: test describe collection, without connection
        method: describe collection with correct params, with a disconnected instance
        expected: describe raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.describe_collection(collection)

    def test_collection_describe_dimension(self, connect):
        '''
        target: test describe collection created with correct params 
        method: create collection, assert the dimention value returned by describe method
        expected: dimention equals with dimention when created
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim+1,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        connect.create_collection(param)
        status, res = connect.describe_collection(collection_name)
        assert res.dimension == dim+1

    """
    ******************************************************************
      The following cases are used to test `drop_collection` function
    ******************************************************************
    """

    def test_drop_collection(self, connect, collection):
        '''
        target: test delete collection created with correct params 
        method: create collection and then delete, 
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        status = connect.drop_collection(collection)
        assert not assert_has_collection(connect, collection)

    @pytest.mark.level(2)
    def test_drop_collection_ip(self, connect, ip_collection):
        '''
        target: test delete collection created with correct params 
        method: create collection and then delete, 
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        status = connect.drop_collection(ip_collection)
        assert not assert_has_collection(connect, ip_collection)

    @pytest.mark.level(2)
    def test_drop_collection_jaccard(self, connect, jac_collection):
        '''
        target: test delete collection created with correct params 
        method: create collection and then delete, 
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        status = connect.drop_collection(jac_collection)
        assert not assert_has_collection(connect, jac_collection)

    @pytest.mark.level(2)
    def test_drop_collection_hamming(self, connect, ham_collection):
        '''
        target: test delete collection created with correct params
        method: create collection and then delete,
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        status = connect.drop_collection(ham_collection)
        assert not assert_has_collection(connect, ham_collection)

    @pytest.mark.level(2)
    def test_collection_delete_without_connection(self, collection, dis_connect):
        '''
        target: test describe collection, without connection
        method: describe collection with correct params, with a disconnected instance
        expected: describe raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.drop_collection(collection)

    def test_drop_collection_not_existed(self, connect):
        '''
        target: test delete collection not in index
        method: delete all collections, and delete collection again, 
            assert the value returned by delete method
        expected: status not ok
        '''
        collection_name = gen_unique_str("test_collection")
        status = connect.drop_collection(collection_name)
        assert not status.OK()

    def test_drop_collection_repeatedly(self, connect):
        '''
        target: test delete collection created with correct params 
        method: create collection and delete new collection repeatedly, 
            assert the value returned by delete method
        expected: create ok and delete ok
        '''
        loops = 1
        for i in range(loops):
            collection_name = gen_unique_str("test_collection")
            param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
            connect.create_collection(param)
            status = connect.drop_collection(collection_name)
            time.sleep(1)
            assert not assert_has_collection(connect, collection_name)

    def test_delete_create_collection_repeatedly(self, connect):
        '''
        target: test delete and create the same collection repeatedly
        method: try to create the same collection and delete repeatedly,
            assert the value returned by delete method
        expected: create ok and delete ok
        '''
        loops = 2 
        for i in range(loops):
            collection_name = "test_collection"
            param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
            connect.create_collection(param)
            status = connect.drop_collection(collection_name)
            time.sleep(1)
            assert status.OK()

    def test_delete_create_collection_repeatedly_ip(self, connect):
        '''
        target: test delete and create the same collection repeatedly
        method: try to create the same collection and delete repeatedly,
            assert the value returned by delete method
        expected: create ok and delete ok
        '''
        loops = 2 
        for i in range(loops):
            collection_name = "test_collection"
            param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.IP}
            connect.create_collection(param)
            status = connect.drop_collection(collection_name)
            time.sleep(1)
            assert status.OK()

    # TODO: enable
    @pytest.mark.level(2)
    def _test_drop_collection_multiprocessing(self, args):
        '''
        target: test delete collection with multiprocess 
        method: create collection and then delete, 
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        process_num = 6
        processes = []
        def deletecollection(milvus):
            status = milvus.drop_collection(collection)
            # assert not status.code==0
            assert assert_has_collection(milvus, collection)
            assert status.OK()

        for i in range(process_num):
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            p = Process(target=deletecollection, args=(milvus,))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

    # TODO: enable
    @pytest.mark.level(2)
    def _test_drop_collection_multiprocessing_multicollection(self, connect):
        '''
        target: test delete collection with multiprocess 
        method: create collection and then delete, 
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        process_num = 5
        loop_num = 2
        processes = []

        collection = []
        j = 0
        while j < (process_num*loop_num):
            collection_name = gen_unique_str("test_drop_collection_with_multiprocessing")
            collection.append(collection_name)
            param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
            connect.create_collection(param)
            j = j + 1

        def delete(connect,ids):
            i = 0
            while i < loop_num:
                status = connect.drop_collection(collection[ids*process_num+i])
                time.sleep(2)
                assert status.OK()
                assert not assert_has_collection(connect, collection[ids*process_num+i])
                i = i + 1

        for i in range(process_num):
            ids = i
            p = Process(target=delete, args=(connect,ids))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

    """
    ******************************************************************
      The following cases are used to test `has_collection` function
    ******************************************************************
    """

    def test_has_collection(self, connect):
        '''
        target: test if the created collection existed
        method: create collection, assert the value returned by has_collection method
        expected: True
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        connect.create_collection(param)
        assert assert_has_collection(connect, collection_name)

    def test_has_collection_ip(self, connect):
        '''
        target: test if the created collection existed
        method: create collection, assert the value returned by has_collection method
        expected: True
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.IP}
        connect.create_collection(param)
        assert assert_has_collection(connect, collection_name)

    def test_has_collection_jaccard(self, connect):
        '''
        target: test if the created collection existed
        method: create collection, assert the value returned by has_collection method
        expected: True
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.JACCARD}
        connect.create_collection(param)
        assert assert_has_collection(connect, collection_name)

    def test_has_collection_hamming(self, connect):
        '''
        target: test if the created collection existed
        method: create collection, assert the value returned by has_collection method
        expected: True
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.HAMMING}
        connect.create_collection(param)
        assert assert_has_collection(connect, collection_name)

    @pytest.mark.level(2)
    def test_has_collection_without_connection(self, collection, dis_connect):
        '''
        target: test has collection, without connection
        method: calling has collection with correct params, with a disconnected instance
        expected: has collection raise exception
        '''
        with pytest.raises(Exception) as e:
            assert_has_collection(dis_connect, collection)

    def test_has_collection_not_existed(self, connect):
        '''
        target: test if collection not created
        method: random a collection name, which not existed in db, 
            assert the value returned by has_collection method
        expected: False
        '''
        collection_name = gen_unique_str("test_collection")
        assert not assert_has_collection(connect, collection_name)

    """
    ******************************************************************
      The following cases are used to test `show_collections` function
    ******************************************************************
    """

    def test_show_collections(self, connect):
        '''
        target: test show collections is correct or not, if collection created
        method: create collection, assert the value returned by show_collections method is equal to 0
        expected: collection_name in show collections   
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        connect.create_collection(param)    
        status, result = connect.show_collections()
        assert status.OK()
        assert collection_name in result

    def test_show_collections_ip(self, connect):
        '''
        target: test show collections is correct or not, if collection created
        method: create collection, assert the value returned by show_collections method is equal to 0
        expected: collection_name in show collections   
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.IP}
        connect.create_collection(param)    
        status, result = connect.show_collections()
        assert status.OK()
        assert collection_name in result

    def test_show_collections_jaccard(self, connect):
        '''
        target: test show collections is correct or not, if collection created
        method: create collection, assert the value returned by show_collections method is equal to 0
        expected: collection_name in show collections   
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.JACCARD}
        connect.create_collection(param)    
        status, result = connect.show_collections()
        assert status.OK()
        assert collection_name in result

    def test_show_collections_hamming(self, connect):
        '''
        target: test show collections is correct or not, if collection created
        method: create collection, assert the value returned by show_collections method is equal to 0
        expected: collection_name in show collections
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.HAMMING}
        connect.create_collection(param)
        status, result = connect.show_collections()
        assert status.OK()
        assert collection_name in result

    def test_show_collections_substructure(self, connect):
        '''
        target: test show collections is correct or not, if collection created
        method: create collection, assert the value returned by show_collections method is equal to 0
        expected: collection_name in show collections
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.SUBSTRUCTURE}
        connect.create_collection(param)
        status, result = connect.show_collections()
        assert status.OK()
        assert collection_name in result

    def test_show_collections_superstructure(self, connect):
        '''
        target: test show collections is correct or not, if collection created
        method: create collection, assert the value returned by show_collections method is equal to 0
        expected: collection_name in show collections
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.SUPERSTRUCTURE}
        connect.create_collection(param)
        status, result = connect.show_collections()
        assert status.OK()
        assert collection_name in result

    @pytest.mark.level(2)
    def test_show_collections_without_connection(self, dis_connect):
        '''
        target: test show_collections, without connection
        method: calling show_collections with correct params, with a disconnected instance
        expected: show_collections raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.show_collections()

    @pytest.mark.level(2)
    def test_show_collections_no_collection(self, connect):
        '''
        target: test show collections is correct or not, if no collection in db
        method: delete all collections,
            assert the value returned by show_collections method is equal to []
        expected: the status is ok, and the result is equal to []      
        '''
        status, result = connect.show_collections()
        if result:
            for collection_name in result:
                connect.drop_collection(collection_name)
        time.sleep(drop_collection_interval_time)
        status, result = connect.show_collections()
        assert status.OK()
        assert len(result) == 0

    # TODO: enable
    @pytest.mark.level(2)
    def _test_show_collections_multiprocessing(self, connect, args):
        '''
        target: test show collections is correct or not with processes
        method: create collection, assert the value returned by show_collections method is equal to 0
        expected: collection_name in show collections
        '''
        collection_name = gen_unique_str("test_collection")
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        connect.create_collection(param)
        def showcollections(milvus):
            status, result = milvus.show_collections()
            assert status.OK()
            assert collection_name in result

        process_num = 8
        processes = []

        for i in range(process_num):
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            p = Process(target=showcollections, args=(milvus,))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

    """
    ******************************************************************
      The following cases are used to test `preload_collection` function
    ******************************************************************
    """

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

    @pytest.mark.level(1)
    def test_preload_collection(self, connect, collection, get_simple_index):
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status, ids = connect.add_vectors(collection, vectors)
        status = connect.create_index(collection, index_type, index_param)
        status = connect.preload_collection(collection)
        assert status.OK()

    @pytest.mark.level(1)
    def test_preload_collection_ip(self, connect, ip_collection, get_simple_index):
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status, ids = connect.add_vectors(ip_collection, vectors)
        status = connect.create_index(ip_collection, index_type, index_param)
        status = connect.preload_collection(ip_collection)
        assert status.OK()

    @pytest.mark.level(1)
    def test_preload_collection_jaccard(self, connect, jac_collection, get_simple_index):
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status, ids = connect.add_vectors(jac_collection, vectors)
        status = connect.create_index(jac_collection, index_type, index_param)
        status = connect.preload_collection(jac_collection)
        assert status.OK()

    @pytest.mark.level(1)
    def test_preload_collection_hamming(self, connect, ham_collection, get_simple_index):
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status, ids = connect.add_vectors(ham_collection, vectors)
        status = connect.create_index(ham_collection, index_type, index_param)
        status = connect.preload_collection(ham_collection)
        assert status.OK()

    @pytest.mark.level(2)
    def test_preload_collection_not_existed(self, connect, collection, get_simple_index):
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        collection_name = gen_unique_str()
        status, ids = connect.add_vectors(collection, vectors)
        status = connect.create_index(collection, index_type, index_param)
        status = connect.preload_collection(collection_name)
        assert not status.OK()

    @pytest.mark.level(2)
    def test_preload_collection_not_existed_ip(self, connect, ip_collection, get_simple_index):
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        collection_name = gen_unique_str()
        status, ids = connect.add_vectors(ip_collection, vectors)
        status = connect.create_index(ip_collection, index_type, index_param)
        status = connect.preload_collection(collection_name)
        assert not status.OK()

    @pytest.mark.level(1)
    def test_preload_collection_no_vectors(self, connect, collection):
        status = connect.preload_collection(collection)
        assert status.OK()

    @pytest.mark.level(2)
    def test_preload_collection_no_vectors_ip(self, connect, ip_collection):
        status = connect.preload_collection(ip_collection)
        assert status.OK()

    # TODO: psutils get memory usage
    @pytest.mark.level(1)
    def test_preload_collection_memory_usage(self, connect, collection):
        pass


class TestCollectionInvalid(object):
    """
    Test creating collection with invalid collection names
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_collection_names()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_create_collection_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_collection(param)
        assert not status.OK()

    def test_create_collection_with_empty_collectionname(self, connect):
        collection_name = ''
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        with pytest.raises(Exception) as e:
            status = connect.create_collection(param)

    def test_preload_collection_with_invalid_collectionname(self, connect):
        collection_name = ''
        with pytest.raises(Exception) as e:
            status = connect.preload_collection(collection_name)


class TestCreateCollectionDimInvalid(object):
    """
    Test creating collection with invalid dimension
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_dims()
    )
    def get_dim(self, request):
        yield request.param

    @pytest.mark.level(2)
    @pytest.mark.timeout(5)
    def test_create_collection_with_invalid_dimension(self, connect, get_dim):
        dimension = get_dim
        collection = gen_unique_str("test_create_collection_with_invalid_dimension")
        param = {'collection_name': collection,
                 'dimension': dimension,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        if isinstance(dimension, int):
            status = connect.create_collection(param)
            assert not status.OK()
        else:
            with pytest.raises(Exception) as e:
                status = connect.create_collection(param)
            

# TODO: max / min index file size
class TestCreateCollectionIndexSizeInvalid(object):
    """
    Test creating collections with invalid index_file_size
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_file_sizes()
    )
    def get_file_size(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_create_collection_with_invalid_file_size(self, connect, collection, get_file_size):
        file_size = get_file_size
        param = {'collection_name': collection,
                 'dimension': dim,
                 'index_file_size': file_size,
                 'metric_type': MetricType.L2}
        if isinstance(file_size, int):
            status = connect.create_collection(param)
            assert not status.OK()
        else:
            with pytest.raises(Exception) as e:
                status = connect.create_collection(param)


class TestCreateMetricTypeInvalid(object):
    """
    Test creating collections with invalid metric_type
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_metric_types()
    )
    def get_metric_type(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_create_collection_with_invalid_file_size(self, connect, collection, get_metric_type):
        metric_type = get_metric_type
        param = {'collection_name': collection,
                 'dimension': dim,
                 'index_file_size': 10,
                 'metric_type': metric_type}
        with pytest.raises(Exception) as e:
            status = connect.create_collection(param)


def create_collection(connect, **params):
    param = {'collection_name': params["collection_name"],
             'dimension': params["dimension"],
             'index_file_size': index_file_size,
             'metric_type': MetricType.L2}
    status = connect.create_collection(param)
    return status

def search_collection(connect, **params):
    status, result = connect.search_vectors(
        params["collection_name"], 
        params["top_k"], 
        params["query_vectors"],
        params={"nprobe": params["nprobe"]})
    return status

def preload_collection(connect, **params):
    status = connect.preload_collection(params["collection_name"])
    return status

def has(connect, **params):
    status, result = connect.has_collection(params["collection_name"])
    return status

def show(connect, **params):
    status, result = connect.show_collections()
    return status

def delete(connect, **params):
    status = connect.drop_collection(params["collection_name"])
    return status

def describe(connect, **params):
    status, result = connect.describe_collection(params["collection_name"])
    return status

def rowcount(connect, **params):
    status, result = connect.count_collection(params["collection_name"])
    return status

def create_index(connect, **params):
    status = connect.create_index(params["collection_name"], params["index_type"], params["index_param"])
    return status

func_map = { 
    # 0:has, 
    1:show,
    10:create_collection, 
    11:describe,
    12:rowcount,
    13:search_collection,
    14:preload_collection,
    15:create_index,
    30:delete
}

def gen_sequence():
    raw_seq = func_map.keys()
    result = itertools.permutations(raw_seq)
    for x in result:
        yield x

class TestCollectionLogic(object):
    @pytest.mark.parametrize("logic_seq", gen_sequence())
    @pytest.mark.level(2)
    def test_logic(self, connect, logic_seq, args):
        if args["handler"] == "HTTP":
            pytest.skip("Skip in http mode")
        if self.is_right(logic_seq):
            self.execute(logic_seq, connect)
        else:
            self.execute_with_error(logic_seq, connect)

    def is_right(self, seq):
        if sorted(seq) == seq:
            return True

        not_created = True
        has_deleted = False
        for i in range(len(seq)):
            if seq[i] > 10 and not_created:
                return False
            elif seq [i] > 10 and has_deleted:
                return False
            elif seq[i] == 10:
                not_created = False
            elif seq[i] == 30:
                has_deleted = True

        return True

    def execute(self, logic_seq, connect):
        basic_params = self.gen_params()
        for i in range(len(logic_seq)):
            # logging.getLogger().info(logic_seq[i])
            f = func_map[logic_seq[i]]
            status = f(connect, **basic_params)
            assert status.OK()

    def execute_with_error(self, logic_seq, connect):
        basic_params = self.gen_params()

        error_flag = False
        for i in range(len(logic_seq)):
            f = func_map[logic_seq[i]]
            status = f(connect, **basic_params)
            if not status.OK():
                # logging.getLogger().info(logic_seq[i])
                error_flag = True
                break
        assert error_flag == True

    def gen_params(self):
        collection_name = gen_unique_str("test_collection")
        top_k = 1
        vectors = gen_vectors(2, dim)
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'metric_type': MetricType.L2,
                 'nprobe': 1,
                 'top_k': top_k,
                 'index_type': IndexType.IVF_SQ8,
                 'index_param': {
                        'nlist': 16384
                 },
                 'query_vectors': vectors}
        return param
