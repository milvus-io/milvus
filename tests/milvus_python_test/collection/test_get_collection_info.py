import pdb
import pytest
import logging
import itertools
from time import sleep
import threading
from multiprocessing import Process
from utils import *

collection_id = "info"
default_fields = gen_default_fields() 
segment_size = 10


class TestInfoBase:

    @pytest.fixture(
        scope="function",
        params=gen_single_filter_fields()
    )
    def get_filter_field(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_single_vector_fields()
    )
    def get_vector_field(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_segment_sizes()
    )
    def get_segment_size(self, request):
        yield request.param

    """
    ******************************************************************
      The following cases are used to test `get_collection_info` function, no data in collection
    ******************************************************************
    """
  
    # TODO
    def test_info_collection_fields(self, connect, get_filter_field, get_vector_field):
        '''
        target: test create normal collection with different fields, check info returned
        method: create collection with diff fields: metric/field_type/..., calling `get_collection_info`
        expected: no exception raised, and value returned correct
        '''
        filter_field = get_filter_field
        vector_field = get_vector_field
        collection_name = gen_unique_str(collection_id)
        fields = {
                "fields": [filter_field, vector_field],
                "segment_size": segment_size
        }
        connect.create_collection(collection_name, fields)
        res = connect.get_collection_info(collection_name)
        # assert field_name
        # assert field_type
        # assert vector field params
        # assert metric type
        # assert dimension

    # TODO
    def test_create_collection_segment_size(self, connect, get_segment_size):
        '''
        target: test create normal collection with different fields
        method: create collection with diff segment_size
        expected: no exception raised
        '''
        collection_name = gen_unique_str(collection_id)
        fields = copy.deepcopy(default_fields)
        fields["segment_size"] = get_segment_size
        connect.create_collection(collection_name, fields)
        # assert segment size


    @pytest.mark.level(2)
    def test_get_collection_info_without_connection(self, collection, dis_connect):
        '''
        target: test get collection info, without connection
        method: calling get collection info with correct params, with a disconnected instance
        expected: get collection info raise exception
        '''
        with pytest.raises(Exception) as e:
            assert connect.get_collection_info(dis_connect, collection)

    def test_get_collection_info_not_existed(self, connect):
        '''
        target: test if collection not created
        method: random a collection name, which not existed in db, 
            assert the value returned by get_collection_info method
        expected: False
        '''
        collection_name = gen_unique_str(collection_id)
        with pytest.raises(Exception) as e:
            res = connect.get_collection_info(connect, collection_name)

    # TODO
    @pytest.mark.level(2)
    def test_get_collection_info_multithread(self, connect):
        '''
        target: test create collection with multithread
        method: create collection using multithread, 
        expected: collections are created
        '''
        threads_num = 4 
        threads = []
        collection_name = gen_unique_str(collection_id)
        connect.create_collection(collection_name, default_fields)

        def get_info():
            res = connect.get_collection_info(connect, collection_name)
            # assert

        for i in range(threads_num):
            t = threading.Thread(target=get_info, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    """
    ******************************************************************
      The following cases are used to test `get_collection_info` function, and insert data in collection
    ******************************************************************
    """

    # TODO
    def test_info_collection_fields_after_insert(self, connect, get_filter_field, get_vector_field):
        '''
        target: test create normal collection with different fields, check info returned
        method: create collection with diff fields: metric/field_type/..., calling `get_collection_info`
        expected: no exception raised, and value returned correct
        '''
        filter_field = get_filter_field
        vector_field = get_vector_field
        collection_name = gen_unique_str(collection_id)
        fields = {
                "fields": [filter_field, vector_field],
                "segment_size": segment_size
        }
        connect.create_collection(collection_name, fields)
        # insert
        res = connect.get_collection_info(collection_name)
        # assert field_name
        # assert field_type
        # assert vector field params
        # assert metric type
        # assert dimension

    # TODO
    def test_create_collection_segment_size_after_insert(self, connect, get_segment_size):
        '''
        target: test create normal collection with different fields
        method: create collection with diff segment_size
        expected: no exception raised
        '''
        collection_name = gen_unique_str(collection_id)
        fields = copy.deepcopy(default_fields)
        fields["segment_size"] = get_segment_size
        connect.create_collection(collection_name, fields)
        # insert
        # assert segment size


class TestInfoInvalid(object):
    """
    Test get collection info with invalid params
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param


    @pytest.mark.level(2)
    def test_get_collection_info_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.get_collection_info(collection_name)

    @pytest.mark.level(2)
    def test_get_collection_info_with_empty_collectionname(self, connect):
        collection_name = ''
        with pytest.raises(Exception) as e:
            connect.get_collection_info(collection_name)

    @pytest.mark.level(2)
    def test_get_collection_info_with_none_collectionname(self, connect):
        collection_name = None
        with pytest.raises(Exception) as e:
            connect.get_collection_info(collection_name)

    def test_get_collection_info_None(self, connect):
        '''
        target: test create collection but the collection name is None
        method: create collection, param collection_name is None
        expected: create raise error
        '''
        with pytest.raises(Exception) as e:
            connect.get_collection_info(None)
