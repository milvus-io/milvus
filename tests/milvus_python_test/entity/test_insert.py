import logging
import time
import pdb
import copy
import threading
from multiprocessing import Pool, Process
import pytest
from milvus import DataType
from utils import *
from constants import *

ADD_TIMEOUT = 60
uid = "test_insert"
field_name = default_float_vec_field_name
binary_field_name = default_binary_vec_field_name
default_single_query = {
    "bool": {
        "must": [
            {"vector": {field_name: {"topk": 10, "query": gen_vectors(1, default_dim), "metric_type": "L2",
                                     "params": {"nprobe": 10}}}}
        ]
    }
}


class TestInsertBase:
    """
    ******************************************************************
      The following cases are used to test `insert` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("CPU not support index_type: ivf_sq8h")
        return request.param

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

    def test_add_vector_with_empty_vector(self, connect, collection):
        '''
        target: test add vectors with empty vectors list
        method: set empty vectors list as add method params
        expected: raises a Exception
        '''
        vector = []
        with pytest.raises(Exception) as e:
            status, ids = connect.insert(collection, vector)

    def test_add_vector_with_None(self, connect, collection):
        '''
        target: test add vectors with None
        method: set None as add method params
        expected: raises a Exception
        '''
        vector = None
        with pytest.raises(Exception) as e:
            status, ids = connect.insert(collection, vector)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_collection_not_existed(self, connect):
        '''
        target: test insert, with collection not existed
        method: insert entity into a random named collection
        expected: error raised 
        '''
        collection_name = gen_unique_str(uid)
        with pytest.raises(Exception) as e:
            connect.insert(collection_name, default_entities_rows)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_drop_collection(self, connect, collection):
        '''
        target: test delete collection after insert vector
        method: insert vector and delete collection
        expected: no error raised
        '''
        ids = connect.insert(collection, default_entity_row)
        assert len(ids) == 1
        connect.drop_collection(collection)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_sleep_drop_collection(self, connect, collection):
        '''
        target: test delete collection after insert vector for a while
        method: insert vector, sleep, and delete collection
        expected: no error raised 
        '''
        ids = connect.insert(collection, default_entity_row)
        assert len(ids) == 1
        connect.flush([collection])
        connect.drop_collection(collection)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_create_index(self, connect, collection, get_simple_index):
        '''
        target: test build index insert after vector
        method: insert vector and build index
        expected: no error raised
        '''
        ids = connect.insert(collection, default_entities_rows)
        assert len(ids) == default_nb
        connect.flush([collection])
        connect.create_index(collection, field_name, get_simple_index)
        info = connect.get_collection_info(collection)
        fields = info["fields"]
        for field in fields:
            if field["name"] == field_name:
                assert field["indexes"][0] == get_simple_index

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_after_create_index(self, connect, collection, get_simple_index):
        '''
        target: test build index insert after vector
        method: insert vector and build index
        expected: no error raised
        '''
        connect.create_index(collection, field_name, get_simple_index)
        ids = connect.insert(collection, default_entities_rows)
        assert len(ids) == default_nb
        info = connect.get_collection_info(collection)
        fields = info["fields"]
        for field in fields:
            if field["name"] == field_name:
                assert field["indexes"][0] == get_simple_index

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_search(self, connect, collection):
        '''
        target: test search vector after insert vector after a while
        method: insert vector, sleep, and search collection
        expected: no error raised 
        '''
        ids = connect.insert(collection, default_entities_rows)
        connect.flush([collection])
        res = connect.search(collection, default_single_query)
        logging.getLogger().debug(res)
        assert res

    def test_insert_segment_row_count(self, connect, collection):
        nb = default_segment_row_limit + 1
        res_ids = connect.insert(collection, gen_entities_rows(nb))
        connect.flush([collection])
        assert len(res_ids) == nb
        stats = connect.get_collection_stats(collection)
        assert len(stats['partitions'][0]['segments']) == 2
        for segment in stats['partitions'][0]['segments']:
            assert segment['row_count'] in [default_segment_row_limit, 1]

    @pytest.fixture(
        scope="function",
        params=[
            1,
            2000
        ],
    )
    def insert_count(self, request):
        yield request.param

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_ids_not_match(self, connect, id_collection, insert_count):
        '''
        target: test insert vectors in collection, use customize ids
        method: create collection and insert vectors in it, check the ids returned and the collection length after vectors inserted
        expected: the length of ids and the collection row count
        '''
        nb = insert_count
        with pytest.raises(Exception) as e:
            res_ids = connect.insert(id_collection, gen_entities_rows(nb))

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_twice_ids_no_ids(self, connect, collection):
        '''
        target: check the result of insert, with params ids and no ids
        method: test insert vectors twice, use customize ids first, and then use no ids
        expected:  error raised
        '''
        with pytest.raises(Exception) as e:
            res_ids = connect.insert(collection, gen_entities_rows(default_nb, _id=False))

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_tag(self, connect, collection):
        '''
        target: test insert entities in collection created before
        method: create collection and insert entities in it, with the partition_tag param
        expected: the collection row count equals to nq
        '''
        connect.create_partition(collection, default_tag)
        ids = connect.insert(collection, default_entities_rows, partition_tag=default_tag)
        assert len(ids) == default_nb
        assert connect.has_partition(collection, default_tag)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_tag_with_ids(self, connect, id_collection):
        '''
        target: test insert entities in collection created before, insert with ids
        method: create collection and insert entities in it, with the partition_tag param
        expected: the collection row count equals to nq
        '''
        connect.create_partition(id_collection, default_tag)
        ids = [i for i in range(default_nb)]
        res_ids = connect.insert(id_collection, gen_entities_rows(default_nb, _id=False), partition_tag=default_tag)
        assert res_ids == ids

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_tag_not_existed(self, connect, collection):
        '''
        target: test insert entities in collection created before
        method: create collection and insert entities in it, with the not existed partition_tag param
        expected: error raised
        '''
        tag = gen_unique_str()
        with pytest.raises(Exception) as e:
            ids = connect.insert(collection, default_entities_rows, partition_tag=tag)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_tag_existed(self, connect, collection):
        '''
        target: test insert entities in collection created before
        method: create collection and insert entities in it repeatly, with the partition_tag param
        expected: the collection row count equals to nq
        '''
        connect.create_partition(collection, default_tag)
        ids = connect.insert(collection, default_entities_rows, partition_tag=default_tag)
        ids = connect.insert(collection, default_entities_rows, partition_tag=default_tag)
        connect.flush([collection])
        res_count = connect.count_entities(collection)
        assert res_count == 2 * default_nb

    @pytest.mark.level(2)
    def test_insert_collection_not_existed(self, connect):
        '''
        target: test insert entities in collection, which not existed before
        method: insert entities collection not existed, check the status
        expected: error raised
        '''
        with pytest.raises(Exception) as e:
            ids = connect.insert(gen_unique_str("not_exist_collection"), default_entities_rows)

    def test_insert_dim_not_matched(self, connect, collection):
        '''
        target: test insert entities, the vector dimension is not equal to the collection dimension
        method: the entities dimension is half of the collection dimension, check the status
        expected: error raised
        '''
        vectors = gen_vectors(default_nb, int(default_dim) // 2)
        insert_entities = copy.deepcopy(default_entities_rows)
        insert_entities[-1][default_float_vec_field_name] = vectors
        with pytest.raises(Exception) as e:
            ids = connect.insert(collection, insert_entities)


class TestInsertBinary:
    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_binary_index(self, request):
        request.param["metric_type"] = "JACCARD"
        return request.param

    def test_insert_binary_entities(self, connect, binary_collection):
        '''
        target: test insert entities in binary collection
        method: create collection and insert binary entities in it
        expected: the collection row count equals to nb
        '''
        ids = connect.insert(binary_collection, default_binary_entities_rows)
        assert len(ids) == default_nb
        connect.flush()
        assert connect.count_entities(binary_collection) == default_nb

    def test_insert_binary_tag(self, connect, binary_collection):
        '''
        target: test insert entities and create partition tag
        method: create collection and insert binary entities in it, with the partition_tag param
        expected: the collection row count equals to nb
        '''
        connect.create_partition(binary_collection, default_tag)
        ids = connect.insert(binary_collection, default_binary_entities_rows, partition_tag=default_tag)
        assert len(ids) == default_nb
        assert connect.has_partition(binary_collection, default_tag)

    # TODO
    @pytest.mark.level(2)
    def test_insert_binary_multi_times(self, connect, binary_collection):
        '''
        target: test insert entities multi times and final flush
        method: create collection and insert binary entity multi and final flush
        expected: the collection row count equals to nb
        '''
        for i in range(default_nb):
            ids = connect.insert(binary_collection, default_binary_entity_row)
            assert len(ids) == 1
        connect.flush([binary_collection])
        assert connect.count_entities(binary_collection) == default_nb

    def test_insert_binary_after_create_index(self, connect, binary_collection, get_binary_index):
        '''
        target: test insert binary entities after build index
        method: build index and insert entities
        expected: no error raised
        '''
        connect.create_index(binary_collection, binary_field_name, get_binary_index)
        ids = connect.insert(binary_collection, default_binary_entities_rows)
        assert len(ids) == default_nb
        connect.flush([binary_collection])
        info = connect.get_collection_info(binary_collection)
        fields = info["fields"]
        for field in fields:
            if field["name"] == binary_field_name:
                assert field["indexes"][0] == get_binary_index

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_insert_binary_create_index(self, connect, binary_collection, get_binary_index):
        '''
        target: test build index insert after vector
        method: insert vector and build index
        expected: no error raised
        '''
        ids = connect.insert(binary_collection, default_binary_entities_rows)
        assert len(ids) == default_nb
        connect.flush([binary_collection])
        connect.create_index(binary_collection, binary_field_name, get_binary_index)
        info = connect.get_collection_info(binary_collection)
        fields = info["fields"]
        for field in fields:
            if field["name"] == binary_field_name:
                assert field["indexes"][0] == get_binary_index


class TestInsertInvalid(object):
    """
    Test inserting vectors with invalid collection names
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_tag_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_field_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_field_type(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_field_int_value(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_ints()
    )
    def get_entity_id(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_vectors()
    )
    def get_field_vectors_value(self, request):
        yield request.param

    def test_insert_field_name_not_match(self, connect, collection):
        '''
        target: test insert, with field name not matched
        method: create collection and insert entities in it
        expected: raise an exception
        '''
        tmp_entity = copy.deepcopy(default_entity_row)
        tmp_entity[0]["string"] = "string"
        with pytest.raises(Exception):
            connect.insert(collection, tmp_entity)

    def test_insert_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception):
            connect.insert(collection_name, default_entity_row)

    def test_insert_with_invalid_tag_name(self, connect, collection, get_tag_name):
        tag_name = get_tag_name
        connect.create_partition(collection, default_tag)
        if tag_name is not None:
            with pytest.raises(Exception):
                connect.insert(collection, default_entity_row, partition_tag=tag_name)
        else:
            connect.insert(collection, default_entity_row, partition_tag=tag_name)

    def test_insert_with_less_field(self, connect, collection):
        tmp_entity = copy.deepcopy(default_entity_row)
        tmp_entity[0].pop(default_float_vec_field_name)
        with pytest.raises(Exception):
            connect.insert(collection, tmp_entity) 

    def test_insert_with_less_field_id(self, connect, id_collection):
        tmp_entity = copy.deepcopy(gen_entities_rows(default_nb, _id=False))
        tmp_entity[0].pop("_id")
        with pytest.raises(Exception):
            connect.insert(id_collection, tmp_entity) 

    def test_insert_with_more_field(self, connect, collection):
        tmp_entity = copy.deepcopy(default_entity_row)
        tmp_entity[0]["new_field"] = 1
        with pytest.raises(Exception):
            connect.insert(collection, tmp_entity) 

    def test_insert_with_more_field_id(self, connect, collection):
        tmp_entity = copy.deepcopy(default_entity_row)
        tmp_entity[0]["_id"] = 1
        with pytest.raises(Exception):
            connect.insert(collection, tmp_entity)

    def test_insert_with_invalid_field_vector_value(self, connect, collection, get_field_vectors_value):
        tmp_entity = copy.deepcopy(default_entity_row)
        tmp_entity[0][default_float_vec_field_name][1] = get_field_vectors_value
        with pytest.raises(Exception):
            connect.insert(collection, tmp_entity)
