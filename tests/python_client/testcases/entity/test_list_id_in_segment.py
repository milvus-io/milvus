# import time
# import random
# import pdb
# import threading
# import logging
# from multiprocessing import Pool, Process
# import pytest
# from utils.utils import *
# from common.constants import *

# uid = "list_id_in_segment"

# def get_segment_id(connect, collection, nb=1, vec_type='float', index_params=None):
#     if vec_type != "float":
#         vectors, entities = gen_binary_entities(nb)
#     else:
#         entities = gen_entities(nb)
#     ids = connect.bulk_insert(collection, entities)
#     connect.flush([collection])
#     if index_params:
#         if vec_type == 'float':
#             connect.create_index(collection, default_float_vec_field_name, index_params)
#         else:
#             connect.create_index(collection, default_binary_vec_field_name, index_params)
#     stats = connect.get_collection_stats(collection)
#     return ids, stats["partitions"][0]["segments"][0]["id"]
#
#
# class TestListIdInSegmentBase:
#
#     """
#     ******************************************************************
#       The following cases are used to test `list_id_in_segment` function
#     ******************************************************************
#     """
#     def test_list_id_in_segment_collection_name_None(self, connect, collection):
#         '''
#         target: get vector ids where collection name is None
#         method: call list_id_in_segment with the collection_name: None
#         expected: exception raised
#         '''
#         collection_name = None
#         ids, segment_id = get_segment_id(connect, collection)
#         with pytest.raises(Exception) as e:
#             connect.list_id_in_segment(collection_name, segment_id)
#
#     def test_list_id_in_segment_collection_name_not_existed(self, connect, collection):
#         '''
#         target: get vector ids where collection name does not exist
#         method: call list_id_in_segment with a random collection_name, which is not in db
#         expected: status not ok
#         '''
#         collection_name = gen_unique_str(uid)
#         ids, segment_id = get_segment_id(connect, collection)
#         with pytest.raises(Exception) as e:
#             vector_ids = connect.list_id_in_segment(collection_name, segment_id)
#
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_strs()
#     )
#     def get_collection_name(self, request):
#         yield request.param
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_list_id_in_segment_collection_name_invalid(self, connect, collection, get_collection_name):
#         '''
#         target: get vector ids where collection name is invalid
#         method: call list_id_in_segment with invalid collection_name
#         expected: status not ok
#         '''
#         collection_name = get_collection_name
#         ids, segment_id = get_segment_id(connect, collection)
#         with pytest.raises(Exception) as e:
#             connect.list_id_in_segment(collection_name, segment_id)
#
#     def test_list_id_in_segment_name_None(self, connect, collection):
#         '''
#         target: get vector ids where segment name is None
#         method: call list_id_in_segment with the name: None
#         expected: exception raised
#         '''
#         ids, segment_id = get_segment_id(connect, collection)
#         segment = None
#         with pytest.raises(Exception) as e:
#             vector_ids = connect.list_id_in_segment(collection, segment)
#
#     def test_list_id_in_segment_name_not_existed(self, connect, collection):
#         '''
#         target: get vector ids where segment name does not exist
#         method: call list_id_in_segment with a random segment name
#         expected: status not ok
#         '''
#         ids, seg_id = get_segment_id(connect, collection)
#         # segment = gen_unique_str(uid)
#         with pytest.raises(Exception) as e:
#             vector_ids = connect.list_id_in_segment(collection, seg_id + 10000)
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_list_id_in_segment_without_index_A(self, connect, collection):
#         '''
#         target: get vector ids when there is no index
#         method: call list_id_in_segment and check if the segment contains vectors
#         expected: status ok
#         '''
#         nb = 1
#         ids, seg_id = get_segment_id(connect, collection, nb=nb)
#         vector_ids = connect.list_id_in_segment(collection, seg_id)
#         # vector_ids should match ids
#         assert len(vector_ids) == nb
#         assert vector_ids[0] == ids[0]
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_list_id_in_segment_without_index_B(self, connect, collection):
#         '''
#         target: get vector ids when there is no index but with partition
#         method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
#         expected: status ok
#         '''
#         nb = 10
#         entities = gen_entities(nb)
#         connect.create_partition(collection, default_tag)
#         ids = connect.bulk_insert(collection, entities, partition_name=default_tag)
#         connect.flush([collection])
#         stats = connect.get_collection_stats(collection)
#         assert stats["partitions"][1]["tag"] == default_tag
#         vector_ids = connect.list_id_in_segment(collection, stats["partitions"][1]["segments"][0]["id"])
#         # vector_ids should match ids
#         assert len(vector_ids) == nb
#         for i in range(nb):
#             assert vector_ids[i] == ids[i]
#
#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index()
#     )
#     def get_simple_index(self, request, connect):
#         if str(connect._cmd("mode")) == "CPU":
#             if request.param["index_type"] in index_cpu_not_support():
#                 pytest.skip("CPU not support index_type: ivf_sq8h")
#         return request.param
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_list_id_in_segment_with_index_A(self, connect, collection, get_simple_index):
#         '''
#         target: get vector ids when there is index
#         method: call list_id_in_segment and check if the segment contains vectors
#         expected: status ok
#         '''
#         ids, seg_id = get_segment_id(connect, collection, nb=default_nb, index_params=get_simple_index)
#         try:
#             connect.list_id_in_segment(collection, seg_id)
#         except Exception as e:
#             assert False, str(e)
#         # TODO:
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_list_id_in_segment_with_index_B(self, connect, collection, get_simple_index):
#         '''
#         target: get vector ids when there is index and with partition
#         method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
#         expected: status ok
#         '''
#         connect.create_partition(collection, default_tag)
#         ids = connect.bulk_insert(collection, default_entities, partition_name=default_tag)
#         connect.flush([collection])
#         stats = connect.get_collection_stats(collection)
#         assert stats["partitions"][1]["tag"] == default_tag
#         try:
#             connect.list_id_in_segment(collection, stats["partitions"][1]["segments"][0]["id"])
#         except Exception as e:
#             assert False, str(e)
#         # vector_ids should match ids
#         # TODO
#
#     def test_list_id_in_segment_after_delete_vectors(self, connect, collection):
#         '''
#         target: get vector ids after vectors are deleted
#         method: add vectors and delete a few, call list_id_in_segment
#         expected: status ok, vector_ids decreased after vectors deleted
#         '''
#         nb = 2
#         ids, seg_id = get_segment_id(connect, collection, nb=nb)
#         delete_ids = [ids[0]]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         connect.flush([collection])
#         stats = connect.get_collection_stats(collection)
#         vector_ids = connect.list_id_in_segment(collection, stats["partitions"][0]["segments"][0]["id"])
#         assert len(vector_ids) == 1
#         assert vector_ids[0] == ids[1]
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_list_id_in_segment_after_delete_vectors(self, connect, collection):
#         '''
#         target: get vector ids after vectors are deleted
#         method: add vectors and delete a few, call list_id_in_segment
#         expected: vector_ids decreased after vectors deleted
#         '''
#         nb = 60
#         delete_length = 10
#         ids, seg_id = get_segment_id(connect, collection, nb=nb)
#         delete_ids = ids[:delete_length]
#         status = connect.delete_entity_by_id(collection, delete_ids)
#         connect.flush([collection])
#         stats = connect.get_collection_stats(collection)
#         vector_ids = connect.list_id_in_segment(collection, stats["partitions"][0]["segments"][0]["id"])
#         assert len(vector_ids) == nb - delete_length
#         assert vector_ids[0] == ids[delete_length]
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_list_id_in_segment_with_index_ip(self, connect, collection, get_simple_index):
#         '''
#         target: get vector ids when there is index
#         method: call list_id_in_segment and check if the segment contains vectors
#         expected: ids returned in ids inserted
#         '''
#         get_simple_index["metric_type"] = "IP"
#         ids, seg_id = get_segment_id(connect, collection, nb=default_nb, index_params=get_simple_index)
#         vector_ids = connect.list_id_in_segment(collection, seg_id)
#         # TODO:
#         segment_row_limit = connect.get_collection_info(collection)["segment_row_limit"]
#         assert vector_ids[0:segment_row_limit] == ids[0:segment_row_limit]
#
# class TestListIdInSegmentBinary:
#     """
#     ******************************************************************
#       The following cases are used to test `list_id_in_segment` function
#     ******************************************************************
#     """
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_list_id_in_segment_without_index_A(self, connect, binary_collection):
#         '''
#         target: get vector ids when there is no index
#         method: call list_id_in_segment and check if the segment contains vectors
#         expected: status ok
#         '''
#         nb = 10
#         vectors, entities = gen_binary_entities(nb)
#         ids = connect.bulk_insert(binary_collection, entities)
#         connect.flush([binary_collection])
#         stats = connect.get_collection_stats(binary_collection)
#         vector_ids = connect.list_id_in_segment(binary_collection, stats["partitions"][0]["segments"][0]["id"])
#         # vector_ids should match ids
#         assert len(vector_ids) == nb
#         for i in range(nb):
#             assert vector_ids[i] == ids[i]
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_list_id_in_segment_without_index_B(self, connect, binary_collection):
#         '''
#         target: get vector ids when there is no index but with partition
#         method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
#         expected: status ok
#         '''
#         connect.create_partition(binary_collection, default_tag)
#         nb = 10
#         vectors, entities = gen_binary_entities(nb)
#         ids = connect.bulk_insert(binary_collection, entities, partition_name=default_tag)
#         connect.flush([binary_collection])
#         stats = connect.get_collection_stats(binary_collection)
#         vector_ids = connect.list_id_in_segment(binary_collection, stats["partitions"][1]["segments"][0]["id"])
#         # vector_ids should match ids
#         assert len(vector_ids) == nb
#         for i in range(nb):
#             assert vector_ids[i] == ids[i]
#
#     @pytest.fixture(
#         scope="function",
#         params=gen_binary_index()
#     )
#     def get_jaccard_index(self, request, connect):
#         logging.getLogger().info(request.param)
#         if request.param["index_type"] in binary_support():
#             request.param["metric_type"] = "JACCARD"
#             return request.param
#         else:
#             pytest.skip("not support")
#
#     def test_list_id_in_segment_with_index_A(self, connect, binary_collection, get_jaccard_index):
#         '''
#         target: get vector ids when there is index
#         method: call list_id_in_segment and check if the segment contains vectors
#         expected: status ok
#         '''
#         ids, seg_id = get_segment_id(connect, binary_collection, nb=default_nb, index_params=get_jaccard_index, vec_type='binary')
#         vector_ids = connect.list_id_in_segment(binary_collection, seg_id)
#         # TODO:
#
#     def test_list_id_in_segment_with_index_B(self, connect, binary_collection, get_jaccard_index):
#         '''
#         target: get vector ids when there is index and with partition
#         method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
#         expected: status ok
#         '''
#         connect.create_partition(binary_collection, default_tag)
#         ids = connect.bulk_insert(binary_collection, default_binary_entities, partition_name=default_tag)
#         connect.flush([binary_collection])
#         stats = connect.get_collection_stats(binary_collection)
#         assert stats["partitions"][1]["tag"] == default_tag
#         vector_ids = connect.list_id_in_segment(binary_collection, stats["partitions"][1]["segments"][0]["id"])
#         # vector_ids should match ids
#         # TODO
#
#     def test_list_id_in_segment_after_delete_vectors(self, connect, binary_collection, get_jaccard_index):
#         '''
#         target: get vector ids after vectors are deleted
#         method: add vectors and delete a few, call list_id_in_segment
#         expected: status ok, vector_ids decreased after vectors deleted
#         '''
#         nb = 2
#         ids, seg_id = get_segment_id(connect, binary_collection, nb=nb, vec_type='binary', index_params=get_jaccard_index)
#         delete_ids = [ids[0]]
#         status = connect.delete_entity_by_id(binary_collection, delete_ids)
#         connect.flush([binary_collection])
#         stats = connect.get_collection_stats(binary_collection)
#         vector_ids = connect.list_id_in_segment(binary_collection, stats["partitions"][0]["segments"][0]["id"])
#         assert len(vector_ids) == 1
#         assert vector_ids[0] == ids[1]
