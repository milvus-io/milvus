import time
import random
import pdb
import copy
import threading
import logging
from multiprocessing import Pool, Process
import concurrent.futures
import pytest
from utils import *


dim = 128
segment_row_count = 5000
collection_id = "test_get"
DELETE_TIMEOUT = 60
tag = "1970-01-01"
nb = 6000
entity = gen_entities(1)
binary_entity = gen_binary_entities(1)
entities = gen_entities(nb)
raw_vectors, binary_entities = gen_binary_entities(nb)
default_single_query = {
    "bool": {
        "must": [
            {"vector": {default_float_vec_field_name: {"topk": 10, "query": gen_vectors(1, dim), "params": {"nprobe": 10}}}}
        ]
    }
}

class TestGetBase:
    """
    ******************************************************************
      The following cases are used to test `get_entity_by_id` function
    ******************************************************************
    """
    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("sq8h not support in CPU mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=[
            1,
            10,
            100,
            500
        ],
    )
    def get_pos(self, request):
        yield request.param

    def test_get_entity(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id, get one
        method: add entity, and get
        expected: entity returned equals insert
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        res_count = connect.count_entities(collection)
        assert res_count == nb
        get_ids = [ids[get_pos]]
        res = connect.get_entity_by_id(collection, get_ids)
        assert_equal_vector(res[0].get(default_float_vec_field_name), entities[-1]["values"][get_pos])

    def test_get_entity_multi_ids(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id, get one
        method: add entity, and get
        expected: entity returned equals insert
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        get_ids = ids[:get_pos]
        res = connect.get_entity_by_id(collection, get_ids)
        for i in range(get_pos):
            assert_equal_vector(res[i].get(default_float_vec_field_name), entities[-1]["values"][i])

    def test_get_entity_parts_ids(self, connect, collection):
        '''
        target: test.get_entity_by_id, some ids in collection, some ids not
        method: add entity, and get
        expected: entity returned equals insert
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        get_ids = [ids[0], 1, ids[-1]]
        res = connect.get_entity_by_id(collection, get_ids)
        assert_equal_vector(res[0].get(default_float_vec_field_name), entities[-1]["values"][0])
        assert_equal_vector(res[-1].get(default_float_vec_field_name), entities[-1]["values"][-1])
        assert res[1] is None

    def test_get_entity_limit(self, connect, collection, args):
        '''
        target: test.get_entity_by_id
        method: add entity, and get, limit > 1000
        expected: entity returned
        '''
        if args["handler"] == "HTTP":
            pytest.skip("skip in http mode")

        ids = connect.insert(collection, entities)
        connect.flush([collection])
        with pytest.raises(Exception) as e:
            res = connect.get_entity_by_id(collection, ids)

    # TODO
    @pytest.mark.level(2)
    def test_get_entity_same_ids(self, connect, id_collection):
        '''
        target: test.get_entity_by_id, with the same ids
        method: add entity, and get one id
        expected: entity returned equals insert
        '''
        ids = [1 for i in range(nb)]
        res_ids = connect.insert(id_collection, entities, ids)
        connect.flush([id_collection])
        get_ids = [ids[0]]
        res = connect.get_entity_by_id(id_collection, get_ids)
        assert len(res) == 1
        assert_equal_vector(res[0].get(default_float_vec_field_name), entities[-1]["values"][0])

    # TODO
    @pytest.mark.level(2)
    def test_get_entity_params_same_ids(self, connect, collection):
        '''
        target: test.get_entity_by_id, with the same ids
        method: add entity, and get entity with the same ids
        expected: entity returned equals insert
        '''
        ids = [1]
        res_ids = connect.insert(collection, entity, ids)
        connect.flush([collection])
        get_ids = [1, 1]
        res = connect.get_entity_by_id(collection, get_ids)
        assert len(res) == len(get_ids)
        for i in range(len(get_ids)):
            logging.getLogger().info(i)
            assert_equal_vector(res[i].get(default_float_vec_field_name), entity[-1]["values"][0])

    def test_get_entities_params_same_ids(self, connect, collection):
        '''
        target: test.get_entity_by_id, with the same ids
        method: add entities, and get entity with the same ids
        expected: entity returned equals insert
        '''
        res_ids = connect.insert(collection, entities)
        connect.flush([collection])
        get_ids = [res_ids[0], res_ids[0]]
        res = connect.get_entity_by_id(collection, get_ids)
        assert len(res) == len(get_ids)
        for i in range(len(get_ids)):
            assert_equal_vector(res[i].get(default_float_vec_field_name), entities[-1]["values"][0])

    """
    ******************************************************************
      The following cases are used to test `get_entity_by_id` function, with different metric type
    ******************************************************************
    """

    def test_get_entity_parts_ids_binary(self, connect, binary_collection):
        '''
        target: test.get_entity_by_id, some ids in jac_collection, some ids not
        method: add entity, and get
        expected: entity returned equals insert
        '''
        ids = connect.insert(binary_collection, binary_entities)
        connect.flush([binary_collection])
        get_ids = [ids[0], 1, ids[-1]]
        res = connect.get_entity_by_id(binary_collection, get_ids)
        assert_equal_vector(res[0].get("binary_vector"), binary_entities[-1]["values"][0])
        assert_equal_vector(res[-1].get("binary_vector"), binary_entities[-1]["values"][-1])
        assert res[1] is None

    """
    ******************************************************************
      The following cases are used to test `get_entity_by_id` function, with tags
    ******************************************************************
    """
    def test_get_entities_tag(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id
        method: add entities with tag, get
        expected: entity returned
        '''
        connect.create_partition(collection, tag)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush([collection])
        get_ids = ids[:get_pos]
        res = connect.get_entity_by_id(collection, get_ids)
        for i in range(get_pos):
            assert_equal_vector(res[i].get(default_float_vec_field_name), entities[-1]["values"][i])

    def test_get_entities_tag_default(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id
        method: add entities with default tag, get
        expected: entity returned
        '''
        connect.create_partition(collection, tag)
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        get_ids = ids[:get_pos]
        res = connect.get_entity_by_id(collection, get_ids)
        for i in range(get_pos):
            assert_equal_vector(res[i].get(default_float_vec_field_name), entities[-1]["values"][i])

    def test_get_entities_tags_default(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id
        method: create partitions, add entities with default tag, get
        expected: entity returned
        '''
        tag_new = "tag_new"
        connect.create_partition(collection, tag)
        connect.create_partition(collection, tag_new)
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        get_ids = ids[:get_pos]
        res = connect.get_entity_by_id(collection, get_ids)
        for i in range(get_pos):
            assert_equal_vector(res[i].get(default_float_vec_field_name), entities[-1]["values"][i])

    def test_get_entities_tags_A(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id
        method: create partitions, add entities with default tag, get
        expected: entity returned
        '''
        tag_new = "tag_new"
        connect.create_partition(collection, tag)
        connect.create_partition(collection, tag_new)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush([collection])
        get_ids = ids[:get_pos]
        res = connect.get_entity_by_id(collection, get_ids)
        for i in range(get_pos):
            assert_equal_vector(res[i].get(default_float_vec_field_name), entities[-1]["values"][i])

    def test_get_entities_tags_B(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id
        method: create partitions, add entities with default tag, get
        expected: entity returned
        '''
        tag_new = "tag_new"
        connect.create_partition(collection, tag)
        connect.create_partition(collection, tag_new)
        new_entities = gen_entities(nb+1)
        ids = connect.insert(collection, entities, partition_tag=tag)
        ids_new = connect.insert(collection, new_entities, partition_tag=tag_new)
        connect.flush([collection])
        get_ids = ids[:get_pos]
        get_ids.extend(ids_new[:get_pos])
        res = connect.get_entity_by_id(collection, get_ids)
        for i in range(get_pos):
            assert_equal_vector(res[i].get(default_float_vec_field_name), entities[-1]["values"][i])
        for i in range(get_pos, get_pos*2):
            assert_equal_vector(res[i].get(default_float_vec_field_name), new_entities[-1]["values"][i-get_pos])

    @pytest.mark.level(2)
    def test_get_entities_indexed_tag(self, connect, collection, get_simple_index, get_pos):
        '''
        target: test.get_entity_by_id
        method: add entities with tag, get
        expected: entity returned
        '''
        connect.create_partition(collection, tag)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush([collection])
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        get_ids = ids[:get_pos]
        res = connect.get_entity_by_id(collection, get_ids)
        for i in range(get_pos):
            assert_equal_vector(res[i].get(default_float_vec_field_name), entities[-1]["values"][i])

    """
    ******************************************************************
      The following cases are used to test `get_entity_by_id` function, with fields params
    ******************************************************************
    """
    # TODO: 
    def test_get_entity_field(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id, get one
        method: add entity, and get
        expected: entity returned equals insert
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        get_ids = [ids[get_pos]]
        fields = ["int64"]
        res = connect.get_entity_by_id(collection, get_ids, fields = fields)
        # assert fields

    # TODO: 
    def test_get_entity_fields(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id, get one
        method: add entity, and get
        expected: entity returned equals insert
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        get_ids = [ids[get_pos]]
        fields = ["int64", "float", default_float_vec_field_name]
        res = connect.get_entity_by_id(collection, get_ids, fields = fields)
        # assert fields

    # TODO: assert exception
    def test_get_entity_field_not_match(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id, get one
        method: add entity, and get
        expected: entity returned equals insert
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        get_ids = [ids[get_pos]]
        fields = ["int1288"]
        with pytest.raises(Exception) as e:
            res = connect.get_entity_by_id(collection, get_ids, fields = fields)

    # TODO: assert exception
    def test_get_entity_fields_not_match(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id, get one
        method: add entity, and get
        expected: entity returned equals insert
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        get_ids = [ids[get_pos]]
        fields = ["int1288"]
        with pytest.raises(Exception) as e:
            res = connect.get_entity_by_id(collection, get_ids, fields = fields)

    def test_get_entity_id_not_exised(self, connect, collection):
        '''
        target: test get entity, params entity_id not existed
        method: add entity and get 
        expected: empty result
        '''
        ids = connect.insert(collection, entity)
        connect.flush([collection])
        res = connect.get_entity_by_id(collection, [1]) 
        assert res[0] is None

    def test_get_entity_collection_not_existed(self, connect, collection):
        '''
        target: test get entity, params collection_name not existed
        method: add entity and get
        expected: error raised
        '''
        ids = connect.insert(collection, entity)
        connect.flush([collection])
        collection_new = gen_unique_str()
        with pytest.raises(Exception) as e:
            res = connect.get_entity_by_id(collection_new, [ids[0]])

    """
    ******************************************************************
      The following cases are used to test `get_entity_by_id` function, after deleted
    ******************************************************************
    """
    def test_get_entity_after_delete(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id
        method: add entities, and delete, get entity by the given id
        expected: empty result
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        delete_ids = [ids[get_pos]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        get_ids = [ids[get_pos]]
        res = connect.get_entity_by_id(collection, get_ids)
        assert res[0] is None

    # TODO
    def test_get_entities_after_delete(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id
        method: add entities, and delete, get entity by the given id
        expected: empty result
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        delete_ids = ids[:get_pos]
        status = connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        get_ids = delete_ids
        res = connect.get_entity_by_id(collection, get_ids)
        for i in range(get_pos):
            assert res[i] is None

    # TODO
    @pytest.mark.level(2)
    def test_get_entities_after_delete_compact(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id
        method: add entities, and delete, get entity by the given id
        expected: empty result
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        delete_ids = ids[:get_pos]
        status = connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        connect.compact(collection)
        get_ids = ids[:get_pos]
        res = connect.get_entity_by_id(collection, get_ids)
        for i in range(get_pos):
            assert res[i] is None

    @pytest.mark.level(2)
    def test_get_entities_indexed_batch(self, connect, collection, get_simple_index, get_pos):
        '''
        target: test.get_entity_by_id
        method: add entities batch, create index, get
        expected: entity returned
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        get_ids = ids[:get_pos]
        res = connect.get_entity_by_id(collection, get_ids)
        for i in range(get_pos):
            assert_equal_vector(res[i].get(default_float_vec_field_name), entities[-1]["values"][i])

    @pytest.mark.level(2)
    def test_get_entities_indexed_single(self, connect, collection, get_simple_index, get_pos):
        '''
        target: test.get_entity_by_id
        method: add entities 1 entity/per request, create index, get
        expected: entity returned
        '''
        ids = []
        for i in range(nb):
            ids.append(connect.insert(collection, entity)[0])
        connect.flush([collection])
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        get_ids = ids[:get_pos]
        res = connect.get_entity_by_id(collection, get_ids)
        for i in range(get_pos):
            assert_equal_vector(res[i].get(default_float_vec_field_name), entity[-1]["values"][0])

    def test_get_entities_after_delete_disable_autoflush(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id
        method: disable autoflush, add entities, and delete, get entity by the given id
        expected: empty result
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        delete_ids = ids[:get_pos]
        try:
            disable_flush(connect)
            status = connect.delete_entity_by_id(collection, delete_ids)
            get_ids = ids[:get_pos]
            res = connect.get_entity_by_id(collection, get_ids)
            for i in range(get_pos):
                assert_equal_vector(res[i].get(default_float_vec_field_name), entities[-1]["values"][i])
        finally:
            enable_flush(connect)

    # TODO:
    @pytest.mark.level(2)
    def test_get_entities_after_delete_same_ids(self, connect, id_collection):
        '''
        target: test.get_entity_by_id
        method: add entities with the same ids, and delete, get entity by the given id
        expected: empty result
        '''
        ids = [i for i in range(nb)]
        ids[0] = 1
        res_ids = connect.insert(id_collection, entities, ids)
        connect.flush([id_collection])
        status = connect.delete_entity_by_id(id_collection, [1])
        connect.flush([id_collection])
        get_ids = [1]
        res = connect.get_entity_by_id(id_collection, get_ids)
        assert res[0] is None

    def test_get_entity_after_delete_with_partition(self, connect, collection, get_pos):
        '''
        target: test.get_entity_by_id
        method: add entities into partition, and delete, get entity by the given id
        expected: get one entity
        '''
        connect.create_partition(collection, tag)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush([collection])
        status = connect.delete_entity_by_id(collection, [ids[get_pos]])
        connect.flush([collection])
        res = connect.get_entity_by_id(collection, [ids[get_pos]])
        assert res[0] is None

    def test_get_entity_by_id_multithreads(self, connect, collection):
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        get_id = ids[100:200]
        def get():
            res = connect.get_entity_by_id(collection, get_id)
            assert len(res) == len(get_id)
            for i in range(len(res)):
                assert_equal_vector(res[i].get(default_float_vec_field_name), entities[-1]["values"][100+i])
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_results = {executor.submit(
                get): i for i in range(10)}
            for future in concurrent.futures.as_completed(future_results):
                future.result()


class TestGetInvalid(object):
    """
    Test get entities with invalid params
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
    def get_field_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_ints()
    )
    def get_entity_id(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_insert_ids_invalid(self, connect, collection, get_entity_id):
        '''
        target: test insert, with using customize ids, which are not int64
        method: create collection and insert entities in it
        expected: raise an exception
        '''
        entity_id = get_entity_id
        ids = [entity_id for _ in range(nb)]
        with pytest.raises(Exception):
            connect.get_entity_by_id(collection, ids)

    @pytest.mark.level(2)
    def test_insert_parts_ids_invalid(self, connect, collection, get_entity_id):
        '''
        target: test insert, with using customize ids, which are not int64
        method: create collection and insert entities in it
        expected: raise an exception
        '''
        entity_id = get_entity_id
        ids = [i for i in range(nb)]
        ids[-1] = entity_id
        with pytest.raises(Exception):
            connect.get_entity_by_id(collection, ids)

    @pytest.mark.level(2)
    def test_get_entities_with_invalid_collection_name(self, connect, get_collection_name):
        collection_name = get_collection_name
        ids = [1]
        with pytest.raises(Exception):
            res = connect.get_entity_by_id(collection_name, ids)

    @pytest.mark.level(2)
    def test_get_entities_with_invalid_field_name(self, connect, collection, get_field_name):
        field_name = get_field_name
        ids = [1]
        fields = [field_name]
        with pytest.raises(Exception):
            res = connect.get_entity_by_id(collection, ids, fields=fields)
