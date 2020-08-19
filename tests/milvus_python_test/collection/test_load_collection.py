import pdb
import pytest
import logging
import itertools
from time import sleep
from multiprocessing import Process
from utils import *

collection_id = "load_collection"
nb = 6000
default_fields = gen_default_fields() 
entities = gen_entities(nb)
field_name = default_float_vec_field_name
binary_field_name = default_binary_vec_field_name
raw_vectors, binary_entities = gen_binary_entities(nb)


class TestLoadBase:

    """
    ******************************************************************
      The following cases are used to test `load_collection` function
    ******************************************************************
    """
    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("sq8h not support in cpu mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_binary_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] in binary_support():
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    def test_load_collection_after_index(self, connect, collection, get_simple_index):
        '''
        target: test load collection, after index created
        method: insert and create index, load collection with correct params
        expected: no error raised
        ''' 
        connect.insert(collection, entities)
        connect.flush([collection])
        logging.getLogger().info(get_simple_index)
        connect.create_index(collection, field_name, get_simple_index)
        connect.load_collection(collection)

    @pytest.mark.level(2)
    def test_load_collection_after_index_binary(self, connect, binary_collection, get_binary_index):
        '''
        target: test load binary_collection, after index created
        method: insert and create index, load binary_collection with correct params
        expected: no error raised
        ''' 
        connect.insert(binary_collection, binary_entities)
        connect.flush([binary_collection])
        for metric_type in binary_metrics():
            logging.getLogger().info(metric_type)
            get_binary_index["metric_type"] = metric_type
            if metric_type in structure_metrics():
                with pytest.raises(Exception) as e:
                    connect.create_index(binary_collection, binary_field_name, get_binary_index)
            else:
                connect.create_index(binary_collection, binary_field_name, get_binary_index)
                connect.load_collection(binary_collection)

    def load_empty_collection(self, connect, collection):
        '''
        target: test load collection
        method: no entities in collection, load collection with correct params
        expected: load success
        '''
        connect.load_collection(collection)

    @pytest.mark.level(2)
    def test_load_collection_dis_connect(self, dis_connect, collection):
        '''
        target: test load collection, without connection
        method: load collection with correct params, with a disconnected instance
        expected: load raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.load_collection(collection)

    @pytest.mark.level(2)
    def test_load_collection_not_existed(self, connect, collection):
        collection_name = gen_unique_str(collection_id)
        with pytest.raises(Exception) as e:
            connect.load_collection(collection_name)

    # TODO:
    @pytest.mark.level(2)
    def test_load_collection_after_search(self, connect, collection):
        pass


class TestLoadCollectionInvalid(object):
    """
    Test load collection with invalid params
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_load_collection_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)
