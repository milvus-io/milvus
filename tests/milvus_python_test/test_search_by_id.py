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
# from milvus import Milvus, IndexType, MetricType
# from utils import *
# dim = 128
# collection_id = "test_search"
# add_interval_time = 2
# vectors = gen_vectors(6000, dim)
# # vectors /= numpy.linalg.norm(vectors)
# # vectors = vectors.tolist()
# nprobe = 1
# epsilon = 0.001
# tag = "overallpaper"
# top_k = 5
# nprobe = 1
# non_exist_id = 9527
# small_size = 6000
# raw_vectors, binary_vectors = gen_binary_vectors(6000, dim)


# class TestSearchBase:
#     @pytest.fixture(scope="function", autouse=True)
#     def skip_check(self, connect):
#         if str(connect._cmd("mode")[1]) == "GPU":
#             reason = "GPU mode not support"
#             logging.getLogger().info(reason)
#             pytest.skip(reason)

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
#         sleep(add_interval_time)
#         return add_vectors, ids

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
#         sleep(add_interval_time)
#         return add_vectors, ids

#     def init_data_no_flush(self, connect, collection, nb=6000):
#         global vectors
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#         status, ids = connect.add_vectors(collection, add_vectors)
#         # sleep(add_interval_time)
#         return add_vectors, ids

#     def init_data_no_flush_ids(self, connect, collection, nb=6000):
#         global vectors
#         my_ids = [i for i in range(nb)]
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#         status, ids = connect.add_vectors(collection, add_vectors, my_ids)
#         # sleep(add_interval_time)
#         return add_vectors, ids

#     def init_data_ids(self, connect, collection, nb=6000):
#         global vectors
#         my_ids = [i for i in range(nb)]
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#         status, ids = connect.add_vectors(collection, add_vectors, my_ids)
#         sleep(add_interval_time)
#         return add_vectors, ids

#     def add_data(self, connect, collection, vectors):
#         '''
#         Add specified vectors to collection
#         '''
#         status, ids = connect.add_vectors(collection, vectors)
#         # sleep(add_interval_time)
#         sleep(10)
#         return vectors, ids

#     def add_data_ids(self, connect, collection, vectors):
#         my_ids = [i for i in range(len(vectors))]
#         status, ids = connect.add_vectors(collection, vectors, my_ids)
#         sleep(add_interval_time)
#         return vectors, ids

#     def add_data_and_flush(self, connect, collection, vectors):
        
#         status, ids = connect.add_vectors(collection, vectors)
#         connect.flush([collection])
#         return vectors, ids

#     def add_data_and_flush_ids(self, connect, collection, vectors):
#         my_ids = [i for i in range(len(vectors))]
#         status, ids = connect.add_vectors(collection, vectors, my_ids)
#         connect.flush([collection])
#         return vectors, ids

#     def add_data_no_flush(self, connect, collection, vectors):
#         '''
#         Add specified vectors to collection
#         '''
#         status, ids = connect.add_vectors(collection, vectors)
#         return vectors, ids

#     def add_data_no_flush_ids(self, connect, collection, vectors):
#         my_ids = [i for i in range(len(vectors))]
#         status, ids = connect.add_vectors(collection, vectors, my_ids)
#         return vectors, ids

#     # delete data and auto flush - timeout due to the flush interval in config file
#     def delete_data(self, connect, collection, ids):
#         '''
#         delete vectors by id
#         '''
#         status = connect.delete_by_id(collection, ids)
#         sleep(add_interval_time)
#         return status

#     # delete data and auto flush - timeout due to the flush interval in config file
#     def delete_data_no_flush(self, connect, collection, ids):
#         '''
#         delete vectors by id
#         '''
#         status = connect.delete_by_id(collection, ids)
#         return status

#     # delete data and manual flush
#     def delete_data_and_flush(self, connect, collection, ids):
#         '''
#         delete vectors by id
#         '''
#         status = connect.delete_by_id(collection, ids)
#         connect.flush([collection])
#         return status

#     def check_no_result(self, results):
#         if len(results) == 0:
#             return True
#         flag = True
#         for r in results:
#             flag = flag and (r.id == -1)
#             if not flag:
#                 return False
#         return flag

#     def init_data_partition(self, connect, collection, partition_tag, nb=6000):
#         '''
#         Generate vectors and add it in collection, before search vectors
#         '''
#         global vectors
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#             # add_vectors /= numpy.linalg.norm(add_vectors)
#             # add_vectors = add_vectors.tolist()
#         status, ids = connect.add_vectors(collection, add_vectors, partition_tag=partition_tag)
#         sleep(add_interval_time)
#         return add_vectors, ids

#     def init_data_and_flush(self, connect, collection, nb=6000):
#         '''
#         Generate vectors and add it in collection, before search vectors
#         '''
#         global vectors
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#             # add_vectors /= numpy.linalg.norm(add_vectors)
#             # add_vectors = add_vectors.tolist()
#         status, ids = connect.add_vectors(collection, add_vectors)
#         connect.flush([collection])
#         return add_vectors, ids

#     def init_data_and_flush_ids(self, connect, collection, nb=6000):
#         global vectors
#         my_ids = [i for i in range(nb)]
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#         status, ids = connect.add_vectors(collection, add_vectors, my_ids)
#         connect.flush([collection])
#         return add_vectors, ids

#     def init_data_partition_and_flush(self, connect, collection, partition_tag, nb=6000):
#         '''
#         Generate vectors and add it in collection, before search vectors
#         '''
#         global vectors
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#             # add_vectors /= numpy.linalg.norm(add_vectors)
#             # add_vectors = add_vectors.tolist()
#         status, ids = connect.add_vectors(collection, add_vectors, partition_tag=partition_tag)
#         connect.flush([collection])
#         return add_vectors, ids

#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index()
#     )
#     def get_simple_index(self, request, connect):
#         if request.param["index_type"] not in [IndexType.FLAT, IndexType.IVF_FLAT, IndexType.IVF_SQ8]:
#             pytest.skip("Skip PQ Temporary")
#         return request.param

#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index()
#     )
#     def get_jaccard_index(self, request, connect):
# 
#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index_params()
#     )
#     def get_simple_index_params(self, request, connect):
#         if request.param["index_type"] not in [IndexType.FLAT, IndexType.IVF_FLAT, IndexType.IVF_SQ8]:
#             pytest.skip("Skip PQ Temporary")
#         return request.param
# 
#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index_params()
#     )
#     def get_jaccard_index_params(self, request, connect):
#         logging.getLogger().info(request.param)
#         if request.param["index_type"] == IndexType.IVFLAT or request.param["index_type"] == IndexType.FLAT:
#             return request.param
#         else:
#             pytest.skip("Skip index Temporary")

#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index()
#     )
#     def get_hamming_index(self, request, connect):
# 
#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index_params()
#     )
#     def get_hamming_index_params(self, request, connect):
#         logging.getLogger().info(request.param)
#         if request.param["index_type"] == IndexType.IVFLAT or request.param["index_type"] == IndexType.FLAT:
#             return request.param
#         else:
#             pytest.skip("Skip index Temporary")
#     """
#     generate top-k params
#     """
#     @pytest.fixture(
#         scope="function",
#         params=[1, 99, 1024, 2048]
#     )
#     def get_top_k(self, request):
#         yield request.param
#     # auto flush
#     def test_search_flat_normal_topk(self, connect, collection, get_top_k):
#         '''
#         target: test basic search fuction, all the search params is corrent, change top-k value
#         method: search with the given vector id, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         top_k = get_top_k
#         vectors, ids = self.init_data(connect, collection, nb=small_size)
#         query_id = ids[0]
#         status, result = connect.search_by_id(collection, top_k, query_id, params={})
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert result[0][0].distance <= epsilon
#         assert check_result(result[0], ids[0])

#     def test_search_flat_max_topk(self, connect, collection):
#         '''
#         target: test basic search fuction, all the search params is corrent, change top-k value
#         method: search with the given vector id, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         top_k = 2049
#         vectors, ids = self.init_data(connect, collection, nb=small_size)
#         query_id = ids[0]
#         status, result = connect.search_by_id(collection, top_k, query_id, params={})
#         assert not status.OK()
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert not status.OK()
# 
#     def test_search_id_not_existed(self, connect, collection):
#         '''
#         target: test basic search fuction, all the search params is corrent, change top-k value
#         method: search with the given vector id, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         vectors, ids = self.init_data_and_flush(connect, collection, nb=small_size)
#         query_id = non_exist_id
#         status, result = connect.search_by_id(collection, top_k, query_id, params={})
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
# 
#     # auto flush
#     def test_search_ids(self, connect, collection):
#         vectors, ids = self.init_data_ids(connect, collection, nb=small_size)
#         query_id = ids[0]
#         status, result = connect.search_by_id(collection, top_k, query_id, params={})
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert result[0][0].distance <= epsilon
#         assert check_result(result[0], ids[0])
#     # manual flush
#     def test_search_ids_flush(self, connect, collection):
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         query_id = non_exist_id
#         status, result = connect.search_by_id(collection, top_k, query_id, params={})
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert self.check_no_result(result[0])

#     # ------------------------------------------------------------- l2, add manual flush, delete, search ------------------------------------------------------------- #
#     # ids, manual flush, search collection, exist
#     def test_search_index_l2(self, connect, collection, get_simple_index):
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert self.check_no_result(result[0])
# 
#     # ------------------------------------------------------------- l2, add manual flush, delete, search ------------------------------------------------------------- #
#     # ids, manual flush, search collection, exist
#     def test_search_index_l2(self, connect, collection, get_simple_index_params):
#         '''
#         target: test basic search fuction, all the search params is corrent, test all index params, and build
#         method: search with the given vectors, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_type, index_param)
#         query_id = ids[0]
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, top_k, query_id, params=search_param)
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_params)
#         query_id = ids[0]
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert result[0][0].distance <= epsilon
#         assert check_result(result[0], ids[0])
    
#     # ids, manual flush, search collection, non exist
#     def test_search_index_l2_id_not_existed(self, connect, collection, get_simple_index):
#     
#     # ids, manual flush, search collection, non exist
#     def test_search_index_l2_id_not_existed(self, connect, collection, get_simple_index_params):
#         '''
#         target: test basic search fuction, all the search params is corrent, test all index params, and build
#         method: search with the given vectors, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_type, index_param)
#         query_id = non_exist_id
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, top_k, query_id, params=search_param)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)

#     # ids, manual flush, delete, manual flush, search collection, exist
#     def test_search_index_delete(self, connect, collection, get_simple_index):
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_type, index_param)
#         query_id = ids[0]
#         status = self.delete_data_and_flush(connect, collection, [query_id])
#         assert status.OK()
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, top_k, query_id, params=search_param)
#         assert status.OK()
#         assert self.check_no_result(result[0])

#     # ids, manual flush, delete, manual flush, search collection, non exist
#     def test_search_index_delete_id_not_existed(self, connect, collection, get_simple_index):
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_type, index_param)
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_params)
#         query_id = non_exist_id
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
# 
#     # ids, manual flush, delete, manual flush, search collection, exist
#     def test_search_index_delete(self, connect, collection, get_simple_index_params):
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_params)
#         query_id = ids[0]
#         status = self.delete_data_and_flush(connect, collection, [query_id])
#         assert status.OK()
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert status.OK()
#         assert self.check_no_result(result[0])
# 
#     # ids, manual flush, delete, manual flush, search collection, non exist
#     def test_search_index_delete_id_not_existed(self, connect, collection, get_simple_index_params):
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_params)
#         query_id = ids[0]
#         status = self.delete_data_and_flush(connect, collection, [query_id])
#         assert status.OK()
#         query_id = non_exist_id
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, top_k, query_id, params=search_param)
#         assert status.OK()
#         assert self.check_no_result(result[0])

#     def test_search_index_delete_no_flush(self, connect, collection, get_simple_index):
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_type, index_param)
#         query_id = ids[0]
#         status = self.delete_data_no_flush(connect, collection, [query_id])
#         assert status.OK()
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, top_k, query_id, params=search_param)
#         assert status.OK()
#         assert check_result(result[0], query_id)

#     # ids, manual flush, delete, no flush, search collection, non exist
#     def test_search_index_delete_no_flush_id_not_existed(self, connect, collection, get_simple_index):
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_type, index_param)
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert status.OK()
#         assert self.check_no_result(result[0])
# 
#     def test_search_index_delete_no_flush(self, connect, collection, get_simple_index_params):
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_params)
#         query_id = ids[0]
#         status = self.delete_data_no_flush(connect, collection, [query_id])
#         assert status.OK()
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert status.OK()
#         assert check_result(result[0], query_id)
# 
#     # ids, manual flush, delete, no flush, search collection, non exist
#     def test_search_index_delete_no_flush_id_not_existed(self, connect, collection, get_simple_index_params):
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_params)
#         query_id = ids[0]
#         status = self.delete_data_no_flush(connect, collection, [query_id])
#         assert status.OK()
#         query_id = non_exist_id
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, top_k, query_id, params=search_param)
#         assert status.OK()
#         assert self.check_no_result(result[0])

#     def test_search_index_delete_add(self, connect, collection, get_simple_index):
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_type, index_param)
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert status.OK()
#         assert self.check_no_result(result[0])
# 
#     def test_search_index_delete_add(self, connect, collection, get_simple_index_params):
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_params)
#         query_id = ids[0]
#         status = self.delete_data_no_flush(connect, collection, [query_id])
#         assert status.OK()
#         vectors, new_ids = self.add_data_and_flush_ids(connect, collection, vectors)
#         status = connect.create_index(collection, index_type, index_param)
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, top_k, query_id, params=search_param)
#         status = connect.create_index(collection, index_params)
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert result[0][0].distance <= epsilon
#         assert check_result(result[0], query_id)
#         status = self.delete_data_no_flush(connect, collection, [query_id])
#         assert status.OK()
#     # add to collection, auto flush, search collection, search partition exist
#     def test_search_l2_index_partition(self, connect, collection, get_simple_index):
# 
#     # add to collection, auto flush, search collection, search partition exist
#     def test_search_l2_index_partition(self, connect, collection, get_simple_index_params):
#         '''
#         target: test basic search fuction, all the search params is corrent, test all index params, and build
#         method: add vectors into collection, search with the given vectors, check the result
#         expected: search status ok, and the length of the result is top_k, search collection with partition tag return empty
#         '''
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         status = connect.create_partition(collection, tag)
#         vectors, ids = self.init_data(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_type, index_param)
#         query_id = ids[0]
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, top_k,  query_id, params=search_param)
#         index_params = get_simple_index_params
#         status = connect.create_partition(collection, tag)
#         vectors, ids = self.init_data(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_params)
#         query_id = ids[0]
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert check_result(result[0], ids[0])
#         assert result[0][0].distance <= epsilon
#         status, result = connect.search_by_id(collection, top_k, query_id, partition_tags=[tag], params=search_param)
#         assert status.OK() 
#         assert len(result) == 0

#     # add to partition, auto flush, search partition exist
#     def test_search_l2_index_params_partition_2(self, connect, collection, get_simple_index):
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         status = connect.create_partition(collection, tag)
#         vectors, ids = self.init_data_partition(connect, collection, tag, nb=small_size)
#         status = connect.create_index(collection, index_type, index_param)
#         query_id = ids[0]
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, top_k, query_id, partition_tags=[tag], params=search_param)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert check_result(result[0], query_id)

#     def test_search_l2_index_partition_id_not_existed(self, connect, collection, get_simple_index):
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         status = connect.create_partition(collection, tag)
#         vectors, ids = self.init_data(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_type, index_param)
#         query_id = non_exist_id
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, top_k, query_id, partition_tags=[tag], params=search_param)
#         assert status.OK()
#         assert len(result) == 0

#     # add to collection, manual flush, search non-existing partition non exist
#     def test_search_l2_index_partition_tag_not_existed(self, connect, collection, get_simple_index):
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         status = connect.create_partition(collection, tag)
#         vectors, ids = self.init_data_partition_and_flush(connect, collection, tag, nb=small_size)
#         status = connect.create_index(collection, index_type, index_param)
#         query_id = non_exist_id
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, top_k, query_id, partition_tags=['non_existing_tag'], params=search_param)
#         assert status.OK()
#         assert len(result) == 0
    
#     def test_search_l2_index_partitions(self, connect, collection, get_simple_index):
#         new_tag = "new_tag"
#         index_param = get_simple_index["index_param"]
#         index_type = get_simple_index["index_type"]
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id, partition_tag_array=[tag])
#         assert status.OK() 
#         assert len(result) == 0
# 
#     # add to partition, auto flush, search partition exist
#     def test_search_l2_index_params_partition_2(self, connect, collection, get_simple_index_params):
#         index_params = get_simple_index_params
#         status = connect.create_partition(collection, tag)
#         vectors, ids = self.init_data_partition(connect, collection, tag, nb=small_size)
#         status = connect.create_index(collection, index_params)
#         query_id = ids[0]
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id, partition_tag_array=[tag])
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert check_result(result[0], query_id)
# 
#     def test_search_l2_index_partition_id_not_existed(self, connect, collection, get_simple_index_params):
#         index_params = get_simple_index_params
#         status = connect.create_partition(collection, tag)
#         vectors, ids = self.init_data(connect, collection, nb=small_size)
#         status = connect.create_index(collection, index_params)
#         query_id = non_exist_id
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id, partition_tag_array=[tag])
#         assert status.OK()
#         assert len(result) == 0
# 
#     # add to collection, manual flush, search non-existing partition non exist
#     def test_search_l2_index_partition_tag_not_existed(self, connect, collection, get_simple_index_params):
#         index_params = get_simple_index_params
#         status = connect.create_partition(collection, tag)
#         vectors, ids = self.init_data_partition_and_flush(connect, collection, tag, nb=small_size)
#         status = connect.create_index(collection, index_params)
#         query_id = non_exist_id
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id, partition_tag_array=['non_existing_tag'])
#         assert status.OK()
#         assert len(result) == 0
#     
#     def test_search_l2_index_partitions(self, connect, collection, get_simple_index_params):
#         new_tag = "new_tag"
#         index_params = get_simple_index_params
#         status = connect.create_partition(collection, tag)
#         status = connect.create_partition(collection, new_tag)
#         vectors, ids = self.init_data_partition_and_flush(connect, collection, tag, nb=small_size)
#         vectors, new_ids = self.init_data_partition_and_flush(connect, collection, new_tag, nb=small_size)
#         status = connect.create_index(collection, index_type, index_param)
#         query_id = ids[0]
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(collection, top_k, query_id, partition_tags=[tag, new_tag], search_param)
#         status = connect.create_index(collection, index_params)
#         query_id = ids[0]
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id, partition_tag_array=[tag, new_tag])
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert check_result(result[0], ids[0])
#         assert result[0][0].distance <= epsilon
#         query_id = new_ids[0]
#         status, result = connect.search_by_id(collection, top_k, query_id, partition_tags=[tag, new_tag], search_param)
#         status, result = connect.search_by_id(collection, top_k, nprobe, query_id, partition_tag_array=[tag, new_tag])
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert check_result(result[0], new_ids[0])
#         assert result[0][0].distance <= epsilon

#     @pytest.mark.level(2)
#     def test_search_by_id_without_connect(self, dis_connect, collection):
#         '''
#         target: test search vectors without connection
#         method: use dis connected instance, call search method and check if search successfully
#         expected: raise exception
#         '''
#         query_idtors = 123
#         with pytest.raises(Exception) as e:
#             status, ids = dis_connect.search_by_id(collection, top_k, query_idtors, params={})
#             status, ids = dis_connect.search_by_id(collection, top_k, nprobe, query_idtors)
# 
#     def test_search_collection_name_not_existed(self, connect, collection):
#         '''
#         target: search collection not existed
#         method: search with the random collection_name, which is not in db
#         expected: status not ok
#         '''
#         collection_name = gen_unique_str("not_existed_collection")
#         query_id = non_exist_id
#         status, result = connect.search_by_id(collection_name, top_k, query_id, params={})
#         assert not status.OK()
#         status, result = connect.search_by_id(collection_name, top_k, nprobe, query_id)
#         assert not status.OK()

#     def test_search_collection_name_None(self, connect, collection):
#         '''
#         target: search collection that collection name is None
#         method: search with the collection_name: None
#         expected: status not ok
#         '''
#         collection_name = None
#         query_ids = non_exist_id
#         with pytest.raises(Exception) as e: 
#             status, result = connect.search_by_id(collection_name, top_k, query_id, params={})

#     def test_search_jac(self, connect, jac_collection, get_jaccard_index):
#         index_param = get_jaccard_index["index_param"]
#         index_type = get_jaccard_index["index_type"]
#         vectors, ids = self.init_data_binary(connect, jac_collection)
#         status = connect.create_index(jac_collection, index_type, index_param)
#         assert status.OK()
#         query_id = ids[0]
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(jac_collection, top_k, query_id, params=search_param)
#             status, result = connect.search_by_id(collection_name, top_k, nprobe, query_id)
# 
#     def test_search_jac(self, connect, jac_collection, get_jaccard_index_params):
#         index_params = get_jaccard_index_params
#         vectors, ids = self.init_data_binary(connect, jac_collection)
#         status = connect.create_index(jac_collection, index_params)
#         assert status.OK()
#         query_id = ids[0]
#         status, result = connect.search_by_id(jac_collection, top_k, nprobe, query_id)
#         logging.getLogger().info(status)
#         logging.getLogger().info(result)
#         assert status.OK()
#         assert check_result(result[0], ids[0])
#         assert result[0][0].distance <= epsilon

#     def test_search_ham(self, connect, ham_collection, get_hamming_index):
#         index_param = get_hamming_index["index_param"]
#         index_param = get_hamming_index["index_type"]
#         vectors, ids = self.init_data_binary(connect, ham_collection)
#         status = connect.create_index(ham_collection, index_type, index_param)
#         assert status.OK()
#         query_id = ids[0]
#         search_param = get_search_param(index_type)
#         status, result = connect.search_by_id(ham_collection, top_k, query_id, params=search_param)
# 
#     def test_search_ham(self, connect, ham_collection, get_hamming_index_params):
#         index_params = get_hamming_index_params
#         vectors, ids = self.init_data_binary(connect, ham_collection)
#         status = connect.create_index(ham_collection, index_params)
#         assert status.OK()
#         query_id = ids[0]
#         status, result = connect.search_by_id(ham_collection, top_k, nprobe, query_id)
#         logging.getLogger().info(status)
#         logging.getLogger().info(result)
#         assert status.OK()
#         assert check_result(result[0], ids[0])
#         assert result[0][0].distance <= epsilon 

# """
# ******************************************************************
# #  The following cases are used to test `search_by_id` function 
# #  with invalid collection_name top-k / nprobe / query_range
# ******************************************************************
# """

# class TestSearchParamsInvalid(object):
#     nlist = 16384
#     index_type = IndexType.IVF_SQ8
#     index_param = {"nlist": nlist}

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

#     @pytest.mark.level(2)
#     def test_search_with_invalid_collectionname(self, connect, get_collection_name):
#         collection_name = get_collection_name
#         query_id = non_exist_id
#         status, result = connect.search_by_id(collection_name, top_k, query_id)
#         assert not status.OK(
#         status, result = connect.search_by_id(collection_name, top_k, nprobe, query_id)
#         assert not status.OK()
# 
#     @pytest.mark.level(1)
#     def test_search_with_invalid_tag_format(self, connect, collection):
#         query_id = non_exist_id
#         with pytest.raises(Exception) as e:
#             status, result = connect.search_by_id(collection_name, top_k, query_id, partition_tags="tag")
#             status, result = connect.search_by_id(collection_name, top_k, nprobe, query_id, partition_tag_array="tag")
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

#     @pytest.mark.level(1)
#     def test_search_with_invalid_top_k(self, connect, collection, get_top_k):
#         top_k = get_top_k
#         query_id = non_exist_id
#         if isinstance(top_k, int):
#             status, result = connect.search_by_id(collection, top_k, query_id)
#             assert not status.OK()
#         else:
#             with pytest.raises(Exception) as e:
#                 status, result = connect.search_by_id(collection, top_k, query_id)
#             status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#             assert not status.OK()
#         else:
#             with pytest.raises(Exception) as e:
#                 status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
# 
#     @pytest.mark.level(2)
#     def test_search_with_invalid_top_k_ip(self, connect, ip_collection, get_top_k):
#         top_k = get_top_k
#         query_id = non_exist_id
#         if isinstance(top_k, int):

#             status, result = connect.search_by_id(ip_collection, top_k, query_id)
#             assert not status.OK()
#         else:
#             with pytest.raises(Exception) as e:
#                 status, result = connect.search_by_id(ip_collection, top_k, query_id)
#             status, result = connect.search_by_id(ip_collection, top_k, nprobe, query_id)
#             assert not status.OK()
#         else:
#             with pytest.raises(Exception) as e:
#                 status, result = connect.search_by_id(ip_collection, top_k, nprobe, query_id)
#     
#     """
#     Test search collection with invalid nprobe
#     """
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_nprobes()
#     )
#     def get_nprobes(self, request):
#         yield request.param

#     @pytest.mark.level(1)
#     def test_search_with_invalid_nprobe(self, connect, collection, get_nprobes):
#         nprobe = get_nprobes
#         logging.getLogger().info(nprobe)
#         query_id = non_exist_id
#         if isinstance(nprobe, int):
#             status, result = connect.search_by_id(collection, top_k, nprobe, query_id)
#             assert not status.OK()
#         else:
#             with pytest.raises(Exception) as e:
#                 status, result = connect.search_by_id(collection, top_k, nprobe, query_id)

#     @pytest.mark.level(2)
#     def test_search_with_invalid_nprobe_ip(self, connect, ip_collection, get_nprobes):
#         '''
#         target: test search fuction, with the wrong top_k
#         method: search with top_k
#         expected: raise an error, and the connection is normal
#         '''
#         nprobe = get_nprobes
#         logging.getLogger().info(nprobe)
#         query_id = non_exist_id
#         if isinstance(nprobe, int):
#             status, result = connect.search_by_id(ip_collection, top_k, nprobe, query_id)
#             assert not status.OK()
#         else:
#             with pytest.raises(Exception) as e:
#                 status, result = connect.search_by_id(ip_collection, top_k, nprobe, query_id)
#     """
#     Test search collection with invalid ids
#     """
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_vector_ids()
#     )
#     def get_vector_ids(self, request):
#         yield request.param

#     @pytest.mark.level(1)
#     def test_search_flat_with_invalid_vector_id(self, connect, collection, get_vector_ids):
#         '''
#         target: test search fuction, with the wrong query_range
#         method: search with query_range
#         expected: raise an error, and the connection is normal
#         '''
#         query_id = get_vector_ids
#         logging.getLogger().info(query_id)
#         with pytest.raises(Exception) as e:
#             status, result = connect.search_by_id(collection, top_k, nprobe, query_id)

#     @pytest.mark.level(2)
#     def test_search_flat_with_invalid_vector_id_ip(self, connect, ip_collection, get_vector_ids):
#         query_id = get_vector_ids
#         logging.getLogger().info(query_id)
#         with pytest.raises(Exception) as e:
#             status, result = connect.search_by_id(ip_collection, top_k, nprobe, query_id)

# def check_result(result, id):
#     if len(result) >= 5:
#         return id in [x.id for x in result[:5]]
#     else:
#         return id in (i.id for i in result)
