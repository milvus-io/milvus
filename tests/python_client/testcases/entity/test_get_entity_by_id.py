import time
import random
import pdb
import copy
import logging
from multiprocessing import Pool, Process
import concurrent.futures
from threading import current_thread
import pytest
# from utils.utils import *
from common.constants import *

# default_single_query = {
#     "bool": {
#         "must": [
#             {"vector": {
#                 default_float_vec_field_name: {"topk": 10, "query": gen_vectors(1, default_dim), "params": {"nprobe": 10}}}}
#         ]
#     }
# }

# class TestGetBase:
#     """
#     ******************************************************************
#       The following cases are used to test `get_entity_by_id` function
#     ******************************************************************
#     """
#
#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index()
#     )
#     def get_simple_index(self, request, connect):
#         if str(connect._cmd("mode")) == "CPU":
#             if request.param["index_type"] in index_cpu_not_support():
#                 pytest.skip("sq8h not support in CPU mode")
#         return request.param
#
#     @pytest.fixture(
#         scope="function",
#         params=[
#             1,
#             500
#         ],
#     )
#     def get_pos(self, request):
#         yield request.param
#
#     def test_get_entity(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id, get one
#         method: add entity, and get
#         expected: entity returned equals insert
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == default_nb
#         get_ids = [ids[get_pos]]
#         res = connect.get_entity_by_id(collection, get_ids)
#         assert_equal_vector(res[0].get(default_float_vec_field_name), default_entities[-1]["values"][get_pos])
#
#     def test_get_entity_multi_ids(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id, get one
#         method: add entity, and get
#         expected: entity returned equals insert
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         get_ids = ids[:get_pos]
#         res = connect.get_entity_by_id(collection, get_ids)
#         for i in range(get_pos):
#             assert_equal_vector(res[i].get(default_float_vec_field_name), default_entities[-1]["values"][i])
#
#     def test_get_entity_parts_ids(self, connect, collection):
#         '''
#         target: test.get_entity_by_id, some ids in collection, some ids not
#         method: add entity, and get
#         expected: entity returned equals insert
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         get_ids = [ids[0], 1, ids[-1]]
#         res = connect.get_entity_by_id(collection, get_ids)
#         assert_equal_vector(res[0].get(default_float_vec_field_name), default_entities[-1]["values"][0])
#         assert_equal_vector(res[-1].get(default_float_vec_field_name), default_entities[-1]["values"][-1])
#         assert res[1] is None
#
#     def test_get_entity_limit(self, connect, collection, args):
#         '''
#         target: test.get_entity_by_id
#         method: add entity, and get, limit > 1000
#         expected: entity returned
#         '''
#         if args["handler"] == "HTTP":
#             pytest.skip("skip in http mode")
#
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         with pytest.raises(Exception) as e:
#             res = connect.get_entity_by_id(collection, ids)
#
#     def test_get_entity_same_ids(self, connect, id_collection):
#         '''
#         target: test.get_entity_by_id, with the same ids
#         method: add entity, and get one id
#         expected: entity returned equals insert
#         '''
#         ids = [1 for i in range(default_nb)]
#         res_ids = connect.bulk_insert(id_collection, default_entities, ids)
#         connect.flush([id_collection])
#         get_ids = [ids[0]]
#         res = connect.get_entity_by_id(id_collection, get_ids)
#         assert len(res) == 1
#         assert_equal_vector(res[0].get(default_float_vec_field_name), default_entities[-1]["values"][0])
#
#     def test_get_entity_params_same_ids(self, connect, id_collection):
#         '''
#         target: test.get_entity_by_id, with the same ids
#         method: add entity, and get entity with the same ids
#         expected: entity returned equals insert
#         '''
#         ids = [1]
#         res_ids = connect.bulk_insert(id_collection, default_entity, ids)
#         connect.flush([id_collection])
#         get_ids = [1, 1]
#         res = connect.get_entity_by_id(id_collection, get_ids)
#         assert len(res) == len(get_ids)
#         for i in range(len(get_ids)):
#             logging.getLogger().info(i)
#             assert_equal_vector(res[i].get(default_float_vec_field_name), default_entity[-1]["values"][0])
#
#     def test_get_entities_params_same_ids(self, connect, collection):
#         '''
#         target: test.get_entity_by_id, with the same ids
#         method: add entities, and get entity with the same ids
#         expected: entity returned equals insert
#         '''
#         res_ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         get_ids = [res_ids[0], res_ids[0]]
#         res = connect.get_entity_by_id(collection, get_ids)
#         assert len(res) == len(get_ids)
#         for i in range(len(get_ids)):
#             assert_equal_vector(res[i].get(default_float_vec_field_name), default_entities[-1]["values"][0])
#
#     """
#     ******************************************************************
#       The following cases are used to test `get_entity_by_id` function, with different metric type
#     ******************************************************************
#     """
#
#     def test_get_entity_parts_ids_binary(self, connect, binary_collection):
#         '''
#         target: test.get_entity_by_id, some ids in jac_collection, some ids not
#         method: add entity, and get
#         expected: entity returned equals insert
#         '''
#         ids = connect.bulk_insert(binary_collection, default_binary_entities)
#         connect.flush([binary_collection])
#         get_ids = [ids[0], 1, ids[-1]]
#         res = connect.get_entity_by_id(binary_collection, get_ids)
#         assert_equal_vector(res[0].get("binary_vector"), default_binary_entities[-1]["values"][0])
#         assert_equal_vector(res[-1].get("binary_vector"), default_binary_entities[-1]["values"][-1])
#         assert res[1] is None
#
#     """
#     ******************************************************************
#       The following cases are used to test `get_entity_by_id` function, with tags
#     ******************************************************************
#     """
#
#     def test_get_entities_tag(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: add entities with tag, get
#         expected: entity returned
#         '''
#         connect.create_partition(collection, default_tag)
#         ids = connect.bulk_insert(collection, default_entities, partition_name = default_tag)
#         connect.flush([collection])
#         get_ids = ids[:get_pos]
#         res = connect.get_entity_by_id(collection, get_ids)
#         for i in range(get_pos):
#             assert_equal_vector(res[i].get(default_float_vec_field_name), default_entities[-1]["values"][i])
#
#     def test_get_entities_tag_default(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: add entities with default tag, get
#         expected: entity returned
#         '''
#         connect.create_partition(collection, default_tag)
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         get_ids = ids[:get_pos]
#         res = connect.get_entity_by_id(collection, get_ids)
#         for i in range(get_pos):
#             assert_equal_vector(res[i].get(default_float_vec_field_name), default_entities[-1]["values"][i])
#
#     def test_get_entities_tags_default(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: create partitions, add entities with default tag, get
#         expected: entity returned
#         '''
#         tag_new = "tag_new"
#         connect.create_partition(collection, default_tag)
#         connect.create_partition(collection, tag_new)
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         get_ids = ids[:get_pos]
#         res = connect.get_entity_by_id(collection, get_ids)
#         for i in range(get_pos):
#             assert_equal_vector(res[i].get(default_float_vec_field_name), default_entities[-1]["values"][i])
#
#     def test_get_entities_tags_A(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: create partitions, add entities with default tag, get
#         expected: entity returned
#         '''
#         tag_new = "tag_new"
#         connect.create_partition(collection, default_tag)
#         connect.create_partition(collection, tag_new)
#         ids = connect.bulk_insert(collection, default_entities, partition_name = default_tag)
#         connect.flush([collection])
#         get_ids = ids[:get_pos]
#         res = connect.get_entity_by_id(collection, get_ids)
#         for i in range(get_pos):
#             assert_equal_vector(res[i].get(default_float_vec_field_name), default_entities[-1]["values"][i])
#
#     def test_get_entities_tags_B(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: create partitions, add entities with default tag, get
#         expected: entity returned
#         '''
#         tag_new = "tag_new"
#         connect.create_partition(collection, default_tag)
#         connect.create_partition(collection, tag_new)
#         new_entities = gen_entities(default_nb + 1)
#         ids = connect.bulk_insert(collection, default_entities, partition_name = default_tag)
#         ids_new = connect.bulk_insert(collection, new_entities, partition_name = tag_new)
#         connect.flush([collection])
#         get_ids = ids[:get_pos]
#         get_ids.extend(ids_new[:get_pos])
#         res = connect.get_entity_by_id(collection, get_ids)
#         for i in range(get_pos):
#             assert_equal_vector(res[i].get(default_float_vec_field_name), default_entities[-1]["values"][i])
#         for i in range(get_pos, get_pos * 2):
#             assert_equal_vector(res[i].get(default_float_vec_field_name), new_entities[-1]["values"][i - get_pos])
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_entities_indexed_tag(self, connect, collection, get_simple_index, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: add entities with tag, get
#         expected: entity returned
#         '''
#         connect.create_partition(collection, default_tag)
#         ids = connect.bulk_insert(collection, default_entities, partition_name = default_tag)
#         connect.flush([collection])
#         connect.create_index(collection, default_float_vec_field_name, get_simple_index)
#         get_ids = ids[:get_pos]
#         res = connect.get_entity_by_id(collection, get_ids)
#         for i in range(get_pos):
#             assert_equal_vector(res[i].get(default_float_vec_field_name), default_entities[-1]["values"][i])
#
#     """
#     ******************************************************************
#       The following cases are used to test `get_entity_by_id` function, with fields params
#     ******************************************************************
#     """
#
#     def test_get_entity_field(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id, get one
#         method: add entity, and get
#         expected: entity returned equals insert
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         get_ids = [ids[get_pos]]
#         fields = ["int64"]
#         res = connect.get_entity_by_id(collection, get_ids, fields = fields)
#         # assert fields
#         res = res.dict()
#         assert res[0]["field"] == fields[0]
#         assert res[0]["values"] == [default_entities[0]["values"][get_pos]]
#         assert res[0]["type"] == DataType.INT64
#
#     def test_get_entity_fields(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id, get one
#         method: add entity, and get
#         expected: entity returned equals insert
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         get_ids = [ids[get_pos]]
#         fields = ["int64", "float", default_float_vec_field_name]
#         res = connect.get_entity_by_id(collection, get_ids, fields = fields)
#         # assert fields
#         res = res.dict()
#         assert len(res) == len(fields)
#         for field in res:
#             if field["field"] == fields[0]:
#                 assert field["values"] == [default_entities[0]["values"][get_pos]]
#             elif field["field"] == fields[1]:
#                 assert field["values"] == [default_entities[1]["values"][get_pos]]
#             else:
#                 assert_equal_vector(field["values"][0], default_entities[-1]["values"][get_pos])
#
#     # TODO: assert exception
#     def test_get_entity_field_not_match(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id, get one
#         method: add entity, and get
#         expected: entity returned equals insert
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         get_ids = [ids[get_pos]]
#         fields = ["int1288"]
#         with pytest.raises(Exception) as e:
#             res = connect.get_entity_by_id(collection, get_ids, fields = fields)
#
#     # TODO: assert exception
#     def test_get_entity_fields_not_match(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id, get one
#         method: add entity, and get
#         expected: entity returned equals insert
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         get_ids = [ids[get_pos]]
#         fields = ["int1288"]
#         with pytest.raises(Exception) as e:
#             res = connect.get_entity_by_id(collection, get_ids, fields = fields)
#
#     def test_get_entity_id_not_exised(self, connect, collection):
#         '''
#         target: test get entity, params entity_id not existed
#         method: add entity and get
#         expected: empty result
#         '''
#         ids = connect.bulk_insert(collection, default_entity)
#         connect.flush([collection])
#         res = connect.get_entity_by_id(collection, [1])
#         assert res[0] is None
#
#     def test_get_entity_collection_not_existed(self, connect, collection):
#         '''
#         target: test get entity, params collection_name not existed
#         method: add entity and get
#         expected: error raised
#         '''
#         ids = connect.bulk_insert(collection, default_entity)
#         connect.flush([collection])
#         collection_new = gen_unique_str()
#         with pytest.raises(Exception) as e:
#             res = connect.get_entity_by_id(collection_new, [ids[0]])
#
#     """
#     ******************************************************************
#       The following cases are used to test `get_entity_by_id` function, after deleted
#     ******************************************************************
#     """
#
#     def test_get_entity_after_delete(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: add entities, and delete, get entity by the given id
#         expected: empty result
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         delete_ids = [ids[get_pos]]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         connect.flush([collection])
#         get_ids = [ids[get_pos]]
#         res = connect.get_entity_by_id(collection, get_ids)
#         assert res[0] is None
#
#     def test_get_entities_after_delete(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: add entities, and delete, get entity by the given id
#         expected: empty result
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         delete_ids = ids[:get_pos]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         connect.flush([collection])
#         get_ids = delete_ids
#         res = connect.get_entity_by_id(collection, get_ids)
#         for i in range(get_pos):
#             assert res[i] is None
#
#     def test_get_entities_after_delete_compact(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: add entities, and delete, get entity by the given id
#         expected: empty result
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         delete_ids = ids[:get_pos]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         connect.flush([collection])
#         connect.compact(collection)
#         get_ids = ids[:get_pos]
#         res = connect.get_entity_by_id(collection, get_ids)
#         for i in range(get_pos):
#             assert res[i] is None
#
#     def test_get_entities_indexed_batch(self, connect, collection, get_simple_index, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: add entities batch, create index, get
#         expected: entity returned
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         connect.create_index(collection, default_float_vec_field_name, get_simple_index)
#         get_ids = ids[:get_pos]
#         res = connect.get_entity_by_id(collection, get_ids)
#         for i in range(get_pos):
#             assert_equal_vector(res[i].get(default_float_vec_field_name), default_entities[-1]["values"][i])
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_entities_indexed_single(self, connect, collection, get_simple_index, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: add entities 1 entity/per request, create index, get
#         expected: entity returned
#         '''
#         ids = []
#         for i in range(default_nb):
#             ids.append(connect.bulk_insert(collection, default_entity)[0])
#         connect.flush([collection])
#         connect.create_index(collection, default_float_vec_field_name, get_simple_index)
#         get_ids = ids[:get_pos]
#         res = connect.get_entity_by_id(collection, get_ids)
#         for i in range(get_pos):
#             assert_equal_vector(res[i].get(default_float_vec_field_name), default_entity[-1]["values"][0])
#
#     def test_get_entities_with_deleted_ids(self, connect, id_collection):
#         '''
#         target: test.get_entity_by_id
#         method: add entities ids, and delete part, get entity include the deleted id
#         expected:
#         '''
#         ids = [i for i in range(default_nb)]
#         res_ids = connect.bulk_insert(id_collection, default_entities, ids)
#         connect.flush([id_collection])
#         status = connect.delete_entity_by_id(id_collection, [res_ids[1]])
#         connect.flush([id_collection])
#         get_ids = res_ids[:2]
#         res = connect.get_entity_by_id(id_collection, get_ids)
#         assert len(res) == len(get_ids)
#         assert_equal_vector(res[0].get(default_float_vec_field_name), default_entities[-1]["values"][0])
#         assert res[1] is None
#
#     # TODO: unable to set config
#     def _test_get_entities_after_delete_disable_autoflush(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: disable autoflush, add entities, and delete, get entity by the given id
#         expected: empty result
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         delete_ids = ids[:get_pos]
#         try:
#             disable_flush(connect)
#             status = connect.delete_entity_by_id(collection, delete_ids)
#             get_ids = ids[:get_pos]
#             res = connect.get_entity_by_id(collection, get_ids)
#             for i in range(get_pos):
#                 assert_equal_vector(res[i].get(default_float_vec_field_name), default_entities[-1]["values"][i])
#         finally:
#             enable_flush(connect)
#
#     def test_get_entities_after_delete_same_ids(self, connect, id_collection):
#         '''
#         target: test.get_entity_by_id
#         method: add entities with the same ids, and delete, get entity by the given id
#         expected: empty result
#         '''
#         ids = [i for i in range(default_nb)]
#         ids[0] = 1
#         res_ids = connect.bulk_insert(id_collection, default_entities, ids)
#         connect.flush([id_collection])
#         status = connect.delete_entity_by_id(id_collection, [1])
#         connect.flush([id_collection])
#         get_ids = [1]
#         res = connect.get_entity_by_id(id_collection, get_ids)
#         assert res[0] is None
#
#     def test_get_entity_after_delete_with_partition(self, connect, collection, get_pos):
#         '''
#         target: test.get_entity_by_id
#         method: add entities into partition, and delete, get entity by the given id
#         expected: get one entity
#         '''
#         connect.create_partition(collection, default_tag)
#         ids = connect.bulk_insert(collection, default_entities, partition_name = default_tag)
#         connect.flush([collection])
#         status = connect.delete_entity_by_id(collection, [ids[get_pos]])
#         connect.flush([collection])
#         res = connect.get_entity_by_id(collection, [ids[get_pos]])
#         assert res[0] is None
#
#     def test_get_entity_by_id_multithreads(self, connect, collection):
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         get_id = ids[100:200]
#
#         def get():
#             res = connect.get_entity_by_id(collection, get_id)
#             assert len(res) == len(get_id)
#             for i in range(len(res)):
#                 assert_equal_vector(res[i].get(default_float_vec_field_name), default_entities[-1]["values"][100 + i])
#
#         with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
#             future_results = {executor.submit(
#                 get): i for i in range(10)}
#             for future in concurrent.futures.as_completed(future_results):
#                 future.result()
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_entity_by_id_insert_multi_threads(self, connect, collection):
#         '''
#         target: test.get_entity_by_id
#         method: thread do insert and get
#         expected:
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         get_id = ids[:1000]
#
#         def insert():
#             # logging.getLogger().info(current_thread().getName() + " insert")
#             step = 1000
#             for i in range(default_nb // step):
#                 group_entities = gen_entities(step, False)
#                 connect.bulk_insert(collection, group_entities)
#                 connect.flush([collection])
#
#         def get():
#             # logging.getLogger().info(current_thread().getName() + " get")
#             res = connect.get_entity_by_id(collection, get_id)
#             assert len(res) == len(get_id)
#             for i in range(len(res)):
#                 assert_equal_vector(res[i].get(default_float_vec_field_name), default_entities[-1]["values"][i])
#
#         with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
#             for i in range(20):
#                 fun = random.choices([get, insert])[0]
#                 future = executor.submit(fun)
#                 future.result()
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_entity_by_id_insert_multi_threads_2(self, connect, collection):
#         '''
#         target: test.get_entity_by_id
#         method: thread do insert and get
#         expected:
#         '''
#         with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
#             def get(group_ids, group_entities):
#                 # logging.getLogger().info(current_thread().getName() + " get")
#                 res = connect.get_entity_by_id(collection, group_ids)
#                 assert len(res) == len(group_ids)
#                 for i in range(len(res)):
#                     assert_equal_vector(res[i].get(default_float_vec_field_name), group_entities[-1]["values"][i])
#
#             def insert(group_vectors):
#                 # logging.getLogger().info(current_thread().getName() + " insert")
#                 for group_vector in group_vectors:
#                     group_entities = [
#                         {"name": "int64", "type": DataType.INT64, "values": [i for i in range(step)]},
#                         {"name": "float", "type": DataType.FLOAT, "values": [float(i) for i in range(step)]},
#                         {"name": default_float_vec_field_name, "type": DataType.FLOAT_VECTOR, "values": group_vector}
#                     ]
#                     group_ids = connect.bulk_insert(collection, group_entities)
#                     connect.flush([collection])
#                     executor.submit(get, group_ids, group_entities)
#
#             step = 100
#             vectors = gen_vectors(default_nb, default_dim, False)
#             group_vectors = [vectors[i:i + step] for i in range(0, len(vectors), step)]
#             task = executor.submit(insert, group_vectors)
#             task.result()
#
#
# class TestGetInvalid(object):
#     """
#     Test get entities with invalid params
#     """
#
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_strs()
#     )
#     def get_collection_name(self, request):
#         yield request.param
#
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_strs()
#     )
#     def get_field_name(self, request):
#         yield request.param
#
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_ints()
#     )
#     def get_entity_id(self, request):
#         yield request.param
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_insert_ids_invalid(self, connect, collection, get_entity_id):
#         '''
#         target: test insert, with using customize ids, which are not int64
#         method: create collection and insert entities in it
#         expected: raise an exception
#         '''
#         entity_id = get_entity_id
#         ids = [entity_id for _ in range(default_nb)]
#         with pytest.raises(Exception):
#             connect.get_entity_by_id(collection, ids)
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_insert_parts_ids_invalid(self, connect, collection, get_entity_id):
#         '''
#         target: test insert, with using customize ids, which are not int64
#         method: create collection and insert entities in it
#         expected: raise an exception
#         '''
#         entity_id = get_entity_id
#         ids = [i for i in range(default_nb)]
#         ids[-1] = entity_id
#         with pytest.raises(Exception):
#             connect.get_entity_by_id(collection, ids)
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_entities_with_invalid_collection_name(self, connect, get_collection_name):
#         collection_name = get_collection_name
#         ids = [1]
#         with pytest.raises(Exception):
#             res = connect.get_entity_by_id(collection_name, ids)
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_entities_with_invalid_field_name(self, connect, collection, get_field_name):
#         field_name = get_field_name
#         ids = [1]
#         fields = [field_name]
#         with pytest.raises(Exception):
#             res = connect.get_entity_by_id(collection, ids, fields = fields)
