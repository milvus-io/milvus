# import pdb
# import copy
# import struct
# import pytest
# import threading
# import datetime
# import logging
# from time import sleep
# from multiprocessing import Process
# import numpy
# import sklearn.preprocessing
# from milvus import Milvus, IndexType, MetricType
# from utils import *
# 
# dim = 128
# collection_id = "test_search_by_id"
# nb = 6000
# vectors = gen_vectors(nb, dim)
# vectors = sklearn.preprocessing.normalize(vectors, axis=1, norm='l2')
# vectors = vectors.tolist()
# nprobe = 1
# epsilon = 0.001
# tag = "overallpaper"
# top_k = 5
# nq = 10
# nprobe = 1
# non_exist_id = [9527]
# raw_vectors, binary_vectors = gen_binary_vectors(6000, dim)
# 
# 
# class TestSearchBase:
#     # @pytest.fixture(scope="function", autouse=True)
#     # def skip_check(self, connect):
#     #     if str(connect._cmd("mode")[1]) == "CPU":
#     #         if request.param["index_type"] == IndexType.IVF_SQ8H:
#     #             pytest.skip("sq8h not support in CPU mode")
#     #     if str(connect._cmd("mode")[1]) == "GPU":
#     #         if request.param["index_type"] == IndexType.IVF_PQ:
#     #             pytest.skip("ivfpq not support in GPU mode")
# 
#     def init_data(self, connect, collection, nb=6000):
#         '''
#         Generate vectors and add it in collection, before search vectors
#         '''
#         global vectors
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#         status, ids = connect.add_vectors(collection, add_vectors)
#         connect.flush([collection])
#         return add_vectors, ids
# 
#     def init_data_binary(self, connect, collection, nb=6000):
#         '''
#         Generate vectors and add it in collection, before search vectors
#         '''
#         global binary_vectors
#         if nb == 6000:
#             add_vectors = binary_vectors
#         else:
#             add_vectors = gen_binary_vectors(nb, dim)
#         status, ids = connect.add_vectors(collection, add_vectors)
#         connect.flush([collection])
#         return add_vectors, ids
# 
#     def init_data_no_flush(self, connect, collection, nb=6000):
#         global vectors
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#         status, ids = connect.add_vectors(collection, add_vectors)
#         return add_vectors, ids
# 
#     def init_data_ids(self, connect, collection, nb=6000):
#         global vectors
#         my_ids = [i for i in range(nb)]
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#         status, ids = connect.add_vectors(collection, add_vectors, my_ids)
#         connect.flush([collection])
#         return add_vectors, ids
# 
#     def init_data_partition(self, connect, collection, partition_tag, nb=6000):
#         '''
#         Generate vectors and add it in collection, before search vectors
#         '''
#         global vectors
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#             add_vectors = sklearn.preprocessing.normalize(add_vectors, axis=1, norm='l2')
#             add_vectors = add_vectors.tolist()
#         status, ids = connect.add_vectors(collection, add_vectors, partition_tag=partition_tag)
#         assert status.OK()
#         connect.flush([collection])
#         return add_vectors, ids
# 
#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index()
#     )
#     def get_simple_index(self, request, connect):
#         if str(connect._cmd("mode")[1]) == "CPU":
#             if request.param["index_type"] == IndexType.IVF_SQ8H:
#                 pytest.skip("sq8h not support in CPU mode")
#         if str(connect._cmd("mode")[1]) == "GPU":
#             if request.param["index_type"] == IndexType.IVF_PQ:
#                 pytest.skip("ivfpq not support in GPU mode")
#         return request.param
# 
#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index()
#     )
#     def get_jaccard_index(self, request, connect):
#         logging.getLogger().info(request.param)
#         if request.param["index_type"] == IndexType.IVFLAT or request.param["index_type"] == IndexType.FLAT:
#             return request.param
#         else:
#             pytest.skip("Skip index Temporary")
# 
#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index()
#     )
#     def get_hamming_index(self, request, connect):
#         logging.getLogger().info(request.param)
#         if request.param["index_type"] == IndexType.IVFLAT or request.param["index_type"] == IndexType.FLAT:
#             return request.param
#         else:
#             pytest.skip("Skip index Temporary")
# 
#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index()
#     )
#     def get_structure_index(self, request, connect):
#         logging.getLogger().info(request.param)
#         if request.param["index_type"] == IndexType.FLAT:
#             return request.param
#         else:
#             pytest.skip("Skip index Temporary")
# 
#     """
#     generate top-k params
#     """
#     @pytest.fixture(
#         scope="function",
#         params=[1, 2048]
#     )
#     def get_top_k(self, request):
#         yield request.param
# 
#     def test_search_flat_normal_topk(self, connect, collection, get_top_k):
#         '''
#         target: test basic search fuction, all the search params is corrent, change top-k value
#         method: search with the given vector id, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         top_k = get_top_k
#         vectors, ids = self.init_data(connect, collection)
#         query_ids = [ids[0]]
#         status, result = connect.search_by_id(collection, query_ids, top_k, params={})
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert result[0][0].distance <= epsilon
#         assert check_result(result[0], ids[0])
# 
#     def test_search_flat_same_ids(self, connect, collection):
#         '''
#         target: test basic search fuction, all the search params is corrent, change top-k value
#         method: search with the given vector id, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         vectors, ids = self.init_data(connect, collection)
#         query_ids = [ids[0], ids[0]]
#         status, result = connect.search_by_id(collection, query_ids, top_k, params={})
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert result[0][0].distance <= epsilon
#         assert result[1][0].distance <= epsilon
#         assert check_result(result[0], ids[0])
#         assert check_result(result[1], ids[0])
# 
#     def test_search_flat_max_topk(self, connect, collection):
#         '''
#         target: test basic search fuction, all the search params is corrent, change top-k value
#         method: search with the given vector id, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         top_k = 2049
#         vectors, ids = self.init_data(connect, collection)
#         query_ids = [ids[0]]
#         status, result = connect.search_by_id(collection, query_ids, top_k, params={})
#         assert not status.OK()
# 
#     def test_search_id_not_existed(self, connect, collection):
#         '''
#         target: test basic search fuction, all the search params is corrent, change top-k value
#         method: search with the given vector id, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         vectors, ids = self.init_data(connect, collection)
#         query_ids = non_exist_id
#         status, result = connect.search_by_id(collection, query_ids, top_k, params={})
#         assert status.OK()
#         assert len(result[0]) == 0
# 
#     def test_search_collection_empty(self, connect, collection):
#         '''
#         target: test basic search fuction, all the search params is corrent, change top-k value
#         method: search with the given vector id, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         query_ids = non_exist_id
#         logging.getLogger().info(query_ids)
#         logging.getLogger().info(collection)
#         logging.getLogger().info(connect.get_collection_info(collection))
#         status, result = connect.search_by_id(collection, query_ids, top_k, params={})
#         assert not status.OK()
# 
#     def test_search_index_l2(self, connect, collection, get_simple_index):
#         '''
#         target: test basic search fuction, all the search params is corrent, test all index params, and build
#         method: search with the given vectors, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         if index_type == IndexType.IVF_PQ:
#             pytest.skip("skip pq")
#         vectors, ids = self.init_data(connect, collection)
#         status = connect.create_index(collection, index_type, index_param)
#         query_ids = [ids[0]]
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, query_ids, top_k, params=search_param)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert result[0][0].distance <= epsilon
#         assert check_result(result[0], ids[0])
# 
#     def test_search_index_l2_B(self, connect, collection, get_simple_index):
#         '''
#         target: test basic search fuction, all the search params is corrent, test all index params, and build
#         method: search with the given vectors, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         if index_type == IndexType.IVF_PQ:
#             pytest.skip("skip pq")
#         vectors, ids = self.init_data(connect, collection)
#         status = connect.create_index(collection, index_type, index_param)
#         query_ids = ids[0:nq]
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, query_ids, top_k, params=search_param)
#         assert status.OK()
#         assert len(result) == nq
#         for i in range(nq):
#             assert len(result[i]) == min(len(vectors), top_k)
#             assert result[i][0].distance <= epsilon
#             assert check_result(result[i], ids[i])
# 
#     def test_search_index_l2_C(self, connect, collection, get_simple_index):
#         '''
#         target: test basic search fuction, all the search params is corrent, one id is not existed
#         method: search with the given vectors, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         if index_type == IndexType.IVF_PQ:
#             pytest.skip("skip pq")
#         vectors, ids = self.init_data(connect, collection)
#         status = connect.create_index(collection, index_type, index_param)
#         query_ids = ids[0:nq]
#         query_ids[0] = 1
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, query_ids, top_k, params=search_param)
#         assert status.OK()
#         assert len(result) == nq
#         for i in range(nq):
#             if i == 0:
#                 assert len(result[i]) == 0
#             else:
#                 assert len(result[i]) == min(len(vectors), top_k)
#                 assert result[i][0].distance <= epsilon
#                 assert check_result(result[i], ids[i])
# 
#     def test_search_index_delete(self, connect, collection):
#         vectors, ids = self.init_data(connect, collection)
#         query_ids = ids[0:nq]
#         status = connect.delete_entity_by_id(collection, [query_ids[0]])
#         assert status.OK()
#         status = connect.flush([collection])
#         status, result = connect.search_by_id(collection, query_ids, top_k, params={})
#         assert status.OK()
#         assert len(result) == nq 
#         assert len(result[0]) == 0
#         assert len(result[1]) == top_k 
#         assert result[1][0].distance <= epsilon
# 
#     def test_search_l2_partition_tag_not_existed(self, connect, collection):
#         '''
#         target: test basic search fuction, all the search params is corrent, test all index params, and build
#         method: add vectors into collection, search with the given vectors, check the result
#         expected: search status ok, and the length of the result is top_k, search collection with partition tag return empty
#         '''
#         status = connect.create_partition(collection, tag)
#         vectors, ids = self.init_data(connect, collection)
#         query_ids = [ids[0]]
#         new_tag = gen_unique_str()
#         status, result = connect.search_by_id(collection, query_ids, top_k, partition_tags=[new_tag], params={})
#         assert not status.OK() 
#         logging.getLogger().info(status)
#         assert len(result) == 0
# 
#     def test_search_l2_partition_empty(self, connect, collection):
#         status = connect.create_partition(collection, tag)
#         vectors, ids = self.init_data(connect, collection)
#         query_ids = [ids[0]]
#         status, result = connect.search_by_id(collection, query_ids, top_k, partition_tags=[tag], params={})
#         assert not status.OK()
#         logging.getLogger().info(status)
#         assert len(result) == 0
# 
#     def test_search_l2_partition(self, connect, collection):
#         status = connect.create_partition(collection, tag)
#         vectors, ids = self.init_data_partition(connect, collection, tag)
#         query_ids = ids[-1:]
#         status, result = connect.search_by_id(collection, query_ids, top_k, partition_tags=[tag])
#         assert status.OK() 
#         assert len(result) == 1
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert check_result(result[0], query_ids[-1])
# 
#     def test_search_l2_partition_B(self, connect, collection):
#         status = connect.create_partition(collection, tag)
#         vectors, ids = self.init_data_partition(connect, collection, tag)
#         query_ids = ids[0:nq]
#         status, result = connect.search_by_id(collection, query_ids, top_k, partition_tags=[tag])
#         assert status.OK()
#         assert len(result) == nq
#         for i in range(nq):
#             assert len(result[i]) == min(len(vectors), top_k)
#             assert result[i][0].distance <= epsilon
#             assert check_result(result[i], ids[i])
# 
#     def test_search_l2_index_partitions(self, connect, collection):
#         new_tag = "new_tag"
#         status = connect.create_partition(collection, tag)
#         status = connect.create_partition(collection, new_tag)
#         vectors, ids = self.init_data_partition(connect, collection, tag)
#         vectors, new_ids = self.init_data_partition(connect, collection, new_tag, nb=nb+1)
#         tmp = 2
#         query_ids = ids[0:tmp]
#         query_ids.extend(new_ids[tmp:nq])
#         status, result = connect.search_by_id(collection, query_ids, top_k, partition_tags=[tag, new_tag], params={})
#         assert status.OK()
#         assert len(result) == nq
#         for i in range(nq):
#             assert len(result[i]) == min(len(vectors), top_k)
#             assert result[i][0].distance <= epsilon
#             if i < tmp:
#                 assert result[i][0].id == ids[i]
#             else:
#                 assert result[i][0].id == new_ids[i]
# 
#     def test_search_l2_index_partitions_match_one_tag(self, connect, collection):
#         new_tag = "new_tag"
#         status = connect.create_partition(collection, tag)
#         status = connect.create_partition(collection, new_tag)
#         vectors, ids = self.init_data_partition(connect, collection, tag)
#         vectors, new_ids = self.init_data_partition(connect, collection, new_tag, nb=nb+1)
#         tmp = 2
#         query_ids = ids[0:tmp]
#         query_ids.extend(new_ids[tmp:nq])
#         status, result = connect.search_by_id(collection, query_ids, top_k, partition_tags=[new_tag], params={})
#         assert status.OK()
#         assert len(result) == nq
#         for i in range(nq):
#             if i < tmp:
#                 assert result[i][0].distance > epsilon
#                 assert result[i][0].id != ids[i]
#             else:
#                 assert len(result[i]) == min(len(vectors), top_k)
#                 assert result[i][0].distance <= epsilon
#                 assert result[i][0].id == new_ids[i]
#                 assert result[i][1].distance > epsilon
# 
#     # def test_search_by_id_without_connect(self, dis_connect, collection):
#     #     '''
#     #     target: test search vectors without connection
#     #     method: use dis connected instance, call search method and check if search successfully
#     #     expected: raise exception
#     #     '''
#     #     query_ids = [1]
#     #     with pytest.raises(Exception) as e:
#     #         status, ids = dis_connect.search_by_id(collection, query_ids, top_k, params={})
# 
#     def test_search_collection_name_not_existed(self, connect, collection):
#         '''
#         target: search collection not existed
#         method: search with the random collection_name, which is not in db
#         expected: status not ok
#         '''
#         collection_name = gen_unique_str("not_existed_collection")
#         query_ids = non_exist_id
#         status, result = connect.search_by_id(collection_name, query_ids, top_k, params={})
#         assert not status.OK()
# 
#     def test_search_collection_name_None(self, connect, collection):
#         '''
#         target: search collection that collection name is None
#         method: search with the collection_name: None
#         expected: status not ok
#         '''
#         collection_name = None
#         query_ids = non_exist_id
#         with pytest.raises(Exception) as e: 
#             status, result = connect.search_by_id(collection_name, query_ids, top_k, params={})
# 
#     def test_search_jac(self, connect, jac_collection, get_jaccard_index):
#         index_param = get_jaccard_index["index_param"]
#         index_type = get_jaccard_index["index_type"]
#         vectors, ids = self.init_data_binary(connect, jac_collection)
#         status = connect.create_index(jac_collection, index_type, index_param)
#         assert status.OK()
#         query_ids = ids[0:nq]
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(jac_collection, query_ids, top_k, params=search_param)
#         assert status.OK()
#         assert len(result) == nq
#         for i in range(nq):
#             assert len(result[i]) == min(len(vectors), top_k)
#             assert result[i][0].distance <= epsilon
#             assert check_result(result[i], ids[i])
# 
# 
# """
# ******************************************************************
# #  The following cases are used to test `search_by_id` function 
# #  with invalid collection_name top-k / ids / tags
# ******************************************************************
# """
# 
# class TestSearchParamsInvalid(object):
#     nlist = 16384
#     index_param = {"index_type": IndexType.IVF_SQ8, "nlist": nlist}
# 
#     """
#     Test search collection with invalid collection names
#     """
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_collection_names()
#     )
#     def get_collection_name(self, request):
#         yield request.param
# 
#     @pytest.mark.level(2)
#     def test_search_with_invalid_collectionname(self, connect, get_collection_name):
#         collection_name = get_collection_name
#         query_ids = non_exist_id
#         status, result = connect.search_by_id(collection_name, query_ids, top_k, params={})
#         assert not status.OK()
# 
#     @pytest.mark.level(1)
#     def test_search_with_invalid_tag_format(self, connect, collection):
#         query_ids = non_exist_id
#         with pytest.raises(Exception) as e:
#             status, result = connect.search_by_id(collection_name, query_ids, top_k, partition_tags="tag")
# 
#     """
#     Test search collection with invalid top-k
#     """
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_top_ks()
#     )
#     def get_top_k(self, request):
#         yield request.param
# 
#     @pytest.mark.level(1)
#     def test_search_with_invalid_top_k(self, connect, collection, get_top_k):
#         top_k = get_top_k
#         query_ids = non_exist_id
#         if isinstance(top_k, int):
#             status, result = connect.search_by_id(collection, query_ids, top_k)
#             assert not status.OK()
#         else:
#             with pytest.raises(Exception) as e:
#                 status, result = connect.search_by_id(collection, query_ids, top_k)
# 
#     """
#     Test search collection with invalid query ids 
#     """
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_vector_ids()
#     )
#     def get_ids(self, request):
#         yield request.param
# 
#     @pytest.mark.level(1)
#     def test_search_with_invalid_ids(self, connect, collection, get_ids):
#         id = get_ids
#         query_ids = [id]
#         if not isinstance(id, int):
#             with pytest.raises(Exception) as e:
#                 status, result = connect.search_by_id(collection, query_ids, top_k)
# 
#     @pytest.mark.level(2)
#     def test_search_with_part_invalid_ids(self, connect, collection, get_ids):
#         id = get_ids
#         query_ids = [1, id]
#         with pytest.raises(Exception) as e:
#             status, result = connect.search_by_id(collection, query_ids, top_k)
# 
# 
# def check_result(result, id):
#     if len(result) >= top_k:
#         return id in [x.id for x in result[:top_k]]
#     else:
#         return id in (i.id for i in result)
