# import time
# import random
# import pdb
# import copy
# import threading
# import logging
# from multiprocessing import Pool, Process
# import pytest
# from utils.utils import *
# from common.constants import *
#
# field_name = default_float_vec_field_name
# default_single_query = {
#     "bool": {
#         "must": [
#             {"vector": {field_name: {"topk": 10, "metric_type":"L2", "query": gen_vectors(1, default_dim), "params": {"nprobe": 10}}}}
#         ]
#     }
# }


# class TestDeleteBase:
#     """
#     ******************************************************************
#       The following cases are used to test `delete_entity_by_id` function
#     ******************************************************************
#     """
#
#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index()
#     )
#     def get_simple_index(self, request, connect):
#         if str(connect._cmd("mode")) == "GPU":
#             if not request.param["index_type"] not in ivf():
#                 pytest.skip("Only support index_type: idmap/ivf")
#         if str(connect._cmd("mode")) == "CPU":
#             if request.param["index_type"] in index_cpu_not_support():
#                 pytest.skip("CPU not support index_type: ivf_sq8h")
#         return request.param
#
#     @pytest.fixture(
#         scope="function",
#         params=[
#             1,
#             2000
#         ],
#     )
#     def insert_count(self, request):
#         yield request.param
#
#     def test_delete_entity_id_not_exised(self, connect, collection):
#         '''
#         target: test delete entity, params entity_id not existed
#         method: add entity and delete
#         expected: status DELETED
#         '''
#         ids = connect.bulk_insert(collection, default_entity)
#         connect.flush([collection])
#         status = connect.delete_entity_by_id(collection, [0])
#         assert status
#
#     def test_delete_empty_collection(self, connect, collection):
#         '''
#         target: test delete entity, params collection_name not existed
#         method: add entity and delete
#         expected: status DELETED
#         '''
#         status = connect.delete_entity_by_id(collection, [0])
#         assert status
#
#     def test_delete_entity_collection_not_existed(self, connect, collection):
#         '''
#         target: test delete entity, params collection_name not existed
#         method: add entity and delete
#         expected: error raised
#         '''
#         collection_new = gen_unique_str()
#         with pytest.raises(Exception) as e:
#             status = connect.delete_entity_by_id(collection_new, [0])
#
#     def test_delete_entity_collection_not_existed(self, connect, collection):
#         '''
#         target: test delete entity, params collection_name not existed
#         method: add entity and delete
#         expected: error raised
#         '''
#         ids = connect.bulk_insert(collection, default_entity)
#         connect.flush([collection])
#         collection_new = gen_unique_str()
#         with pytest.raises(Exception) as e:
#             status = connect.delete_entity_by_id(collection_new, [0])
#
#     def test_insert_delete(self, connect, collection, insert_count):
#         '''
#         target: test delete entity
#         method: add entities and delete
#         expected: no error raised
#         '''
#         entities = gen_entities(insert_count)
#         ids = connect.bulk_insert(collection, entities)
#         connect.flush([collection])
#         delete_ids = [ids[0]]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         assert status
#         connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == insert_count - 1
#
#     def test_insert_delete_A(self, connect, collection):
#         '''
#         target: test delete entity
#         method: add entities and delete one in collection, and one not in collection
#         expected: no error raised
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         delete_ids = [ids[0], 1]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         assert status
#         connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == default_nb - 1
#
#     def test_insert_delete_B(self, connect, id_collection):
#         '''
#         target: test delete entity
#         method: add entities with the same ids, and delete the id in collection
#         expected: no error raised, all entities deleted
#         '''
#         ids = [1 for i in range(default_nb)]
#         res_ids = connect.bulk_insert(id_collection, default_entities, ids)
#         connect.flush([id_collection])
#         delete_ids = [1]
#         status = connect.delete_entity_by_id(id_collection, delete_ids)
#         assert status
#         connect.flush([id_collection])
#         res_count = connect.count_entities(id_collection)
#         assert res_count == 0
#
#     def test_delete_exceed_limit(self, connect, collection):
#         '''
#         target: test delete entity
#         method: add one entity and delete two ids
#         expected: error raised
#         '''
#         ids = connect.bulk_insert(collection, default_entity)
#         connect.flush([collection])
#         delete_ids = [ids[0], 1]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == 0
#
#     def test_flush_after_delete(self, connect, collection):
#         '''
#         target: test delete entity
#         method: add entities and delete, then flush
#         expected: entity deleted and no error raised
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         delete_ids = [ids[0], ids[-1]]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         assert status
#         connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == default_nb - len(delete_ids)
#
#     def test_flush_after_delete_binary(self, connect, binary_collection):
#         '''
#         target: test delete entity
#         method: add entities and delete, then flush
#         expected: entity deleted and no error raised
#         '''
#         ids = connect.bulk_insert(binary_collection, default_binary_entities)
#         connect.flush([binary_collection])
#         delete_ids = [ids[0], ids[-1]]
#         status = connect.delete_entity_by_id(binary_collection, delete_ids)
#         assert status
#         connect.flush([binary_collection])
#         res_count = connect.count_entities(binary_collection)
#         assert res_count == default_nb - len(delete_ids)
#
#     def test_insert_delete_binary(self, connect, binary_collection):
#         '''
#         method: add entities and delete
#         expected: status DELETED
#         '''
#         ids = connect.bulk_insert(binary_collection, default_binary_entities)
#         connect.flush([binary_collection])
#         delete_ids = [ids[0], ids[-1]]
#         status = connect.delete_entity_by_id(binary_collection, delete_ids)
#
#     def test_insert_same_ids_after_delete(self, connect, id_collection):
#         '''
#         method: add entities and delete
#         expected: status DELETED
#         note: Not flush after delete
#         '''
#         insert_ids = [i for i in range(default_nb)]
#         ids = connect.bulk_insert(id_collection, default_entities, insert_ids)
#         connect.flush([id_collection])
#         delete_ids = [ids[0], ids[-1]]
#         status = connect.delete_entity_by_id(id_collection, delete_ids)
#         assert status
#         new_ids = connect.bulk_insert(id_collection, default_entity, [ids[0]])
#         assert new_ids == [ids[0]]
#         connect.flush([id_collection])
#         res_count = connect.count_entities(id_collection)
#         assert res_count == default_nb - 1
#
#     def test_insert_same_ids_after_delete_binary(self, connect, binary_id_collection):
#         '''
#         method: add entities, with the same id and delete the ids
#         expected: status DELETED, all id deleted
#         '''
#         insert_ids = [i for i in range(default_nb)]
#         ids = connect.bulk_insert(binary_id_collection, default_binary_entities, insert_ids)
#         connect.flush([binary_id_collection])
#         delete_ids = [ids[0], ids[-1]]
#         status = connect.delete_entity_by_id(binary_id_collection, delete_ids)
#         assert status
#         new_ids = connect.bulk_insert(binary_id_collection, default_binary_entity, [ids[0]])
#         assert new_ids == [ids[0]]
#         connect.flush([binary_id_collection])
#         res_count = connect.count_entities(binary_id_collection)
#         assert res_count == default_nb - 1
#
#     def test_search_after_delete(self, connect, collection):
#         '''
#         target: test delete entity
#         method: add entities and delete, then search
#         expected: entity deleted and no error raised
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         delete_ids = [ids[0], ids[-1]]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         assert status
#         connect.flush([collection])
#         query = copy.deepcopy(default_single_query)
#         query["bool"]["must"][0]["vector"][field_name]["query"] =\
#             [default_entity[-1]["values"][0], default_entities[-1]["values"][1], default_entities[-1]["values"][-1]]
#         res = connect.search(collection, query)
#         logging.getLogger().debug(res)
#         assert len(res) == len(query["bool"]["must"][0]["vector"][field_name]["query"])
#         assert res[0]._distances[0] > epsilon
#         assert res[1]._distances[0] < epsilon
#         assert res[2]._distances[0] > epsilon
#
#     def test_create_index_after_delete(self, connect, collection, get_simple_index):
#         '''
#         method: add entitys and delete, then create index
#         expected: vectors deleted, index created
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         delete_ids = [ids[0], ids[-1]]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         connect.create_index(collection, field_name, get_simple_index)
#         # assert index info
#
#     def test_delete_multiable_times(self, connect, collection):
#         '''
#         method: add entities and delete id serveral times
#         expected: entities deleted
#         '''
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         delete_ids = [ids[0], ids[-1]]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         assert status
#         connect.flush([collection])
#         for i in range(10):
#             status = connect.delete_entity_by_id(collection, delete_ids)
#             assert status
#
#     def test_index_insert_batch_delete_get(self, connect, collection, get_simple_index):
#         '''
#         method: create index, insert entities, and delete
#         expected: entities deleted
#         '''
#         connect.create_index(collection, field_name, get_simple_index)
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         delete_ids = [ids[0], ids[-1]]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         assert status
#         connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == default_nb - len(delete_ids)
#         res_get = connect.get_entity_by_id(collection, delete_ids)
#         assert res_get[0] is None
#
#     # TODO: disable
#     @pytest.mark.tags(CaseLabel.L2)
#     def _test_index_insert_single_delete_get(self, connect, id_collection):
#         '''
#         method: insert entities, and delete
#         expected: entities deleted
#         '''
#         ids = [i for i in range(default_nb)]
#         for i in range(default_nb):
#             connect.bulk_insert(id_collection, default_entity, [ids[i]])
#         connect.flush([id_collection])
#         delete_ids = [ids[0], ids[-1]]
#         status = connect.delete_entity_by_id(id_collection, delete_ids)
#         assert status
#         connect.flush([id_collection])
#         res_count = connect.count_entities(id_collection)
#         assert res_count == default_nb - len(delete_ids)
#
#     """
#     ******************************************************************
#       The following cases are used to test `delete_entity_by_id` function, with tags
#     ******************************************************************
#     """
#
#     def test_insert_tag_delete(self, connect, collection):
#         '''
#         method: add entitys with given tag, delete entities with the return ids
#         expected: entities deleted
#         '''
#         connect.create_partition(collection, default_tag)
#         ids = connect.bulk_insert(collection, default_entities, partition_name=default_tag)
#         connect.flush([collection])
#         delete_ids = [ids[0], ids[-1]]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         assert status
#         connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == default_nb - 2
#
#     def test_insert_default_tag_delete(self, connect, collection):
#         '''
#         method: add entitys, delete entities with the return ids
#         expected: entities deleted
#         '''
#         connect.create_partition(collection, default_tag)
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         delete_ids = [ids[0], ids[-1]]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         assert status
#         connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == default_nb - 2
#
#     def test_insert_tags_delete(self, connect, collection):
#         '''
#         method: add entitys with given two tags, delete entities with the return ids
#         expected: entities deleted
#         '''
#         tag_new = "tag_new"
#         connect.create_partition(collection, default_tag)
#         connect.create_partition(collection, tag_new)
#         ids = connect.bulk_insert(collection, default_entities, partition_name=default_tag)
#         ids_new = connect.bulk_insert(collection, default_entities, partition_name=tag_new)
#         connect.flush([collection])
#         delete_ids = [ids[0], ids_new[0]]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         assert status
#         connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == 2 * (default_nb - 1)
#
#     def test_insert_tags_index_delete(self, connect, collection, get_simple_index):
#         """
#         method: add entitys with given tag, create index, delete entities with the return ids
#         expected: entities deleted
#         """
#         tag_new = "tag_new"
#         connect.create_partition(collection, default_tag)
#         connect.create_partition(collection, tag_new)
#         ids = connect.bulk_insert(collection, default_entities, partition_name=default_tag)
#         ids_new = connect.bulk_insert(collection, default_entities, partition_name=tag_new)
#         connect.flush([collection])
#         connect.create_index(collection, field_name, get_simple_index)
#         delete_ids = [ids[0], ids_new[0]]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         assert status
#         connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == 2 * (default_nb - 1)
#
#     def test_insert_delete_loop(self, connect, collection):
#         """
#         target: test loop insert and delete entities
#         method: loop insert entities into two segments, and delete entities cross segments.
#         expected: count is correct
#         """
#         loop = 2
#         for i in range(loop):
#             ids = connect.bulk_insert(collection, default_entities)
#             connect.flush([collection])
#             status = connect.delete_entity_by_id(collection, ids[100:default_nb - 100])
#             connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == loop * 200
#
#     def test_search_delete_loop(self, connect, collection):
#         """
#         target: test loop search and delete entities
#         method: loop search and delete cross segments
#         expected: ok
#         """
#         loop = 2
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         ni = default_nb // loop
#         for i in range(loop):
#             res = connect.search(collection, default_single_query)
#             status = connect.delete_entity_by_id(collection, ids[i * ni:(i + 1) * ni])
#             assert status
#             connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == 0
#
#     def test_count_delete_loop(self, connect, collection):
#         """
#         target: test loop search and delete entities
#         method: loop search and delete cross segments
#         expected: ok
#         """
#         loop = 2
#         ids = connect.bulk_insert(collection, default_entities)
#         connect.flush([collection])
#         ni = default_nb // loop
#         for i in range(loop):
#             connect.count_entities(collection)
#             status = connect.delete_entity_by_id(collection, ids[i * ni:(i + 1) * ni])
#             assert status
#             connect.flush([collection])
#         res_count = connect.count_entities(collection)
#         assert res_count == 0
#
#
# class TestDeleteInvalid(object):
#     """
#     Test adding vectors with invalid vectors
#     """
#
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_ints()
#     )
#     def gen_entity_id(self, request):
#         yield request.param
#
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_strs()
#     )
#     def get_collection_name(self, request):
#         yield request.param
#
#     @pytest.mark.tags(CaseLabel.L1)
#     def test_delete_entity_id_invalid(self, connect, collection, gen_entity_id):
#         invalid_id = gen_entity_id
#         with pytest.raises(Exception) as e:
#             status = connect.delete_entity_by_id(collection, [invalid_id])
#
#     def test_delete_entity_ids_invalid(self, connect, collection, gen_entity_id):
#         invalid_id = gen_entity_id
#         with pytest.raises(Exception) as e:
#             status = connect.delete_entity_by_id(collection, [1, invalid_id])
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_delete_entity_with_invalid_collection_name(self, connect, get_collection_name):
#         collection_name = get_collection_name
#         with pytest.raises(Exception) as e:
#             status = connect.delete_entity_by_id(collection_name, [1])
