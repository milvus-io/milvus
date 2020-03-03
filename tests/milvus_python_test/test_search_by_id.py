# import pdb
# import copy
# import struct
# 
# import pytest
# import threading
# import datetime
# import logging
# from time import sleep
# from multiprocessing import Process
# import numpy
# from milvus import Milvus, IndexType, MetricType
# from utils import *
# 
# dim = 128
# table_id = "test_search"
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
# 
# 
# class TestSearchBase:
#     @pytest.fixture(scope="function", autouse=True)
#     def skip_check(self, connect):
#         if str(connect._cmd("mode")[1]) == "GPU":
#             reason = "GPU mode not support"
#             logging.getLogger().info(reason)
#             pytest.skip(reason)
#     
#     def init_data(self, connect, table, nb=6000):
#         '''
#         Generate vectors and add it in table, before search vectors
#         '''
#         global vectors
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#         status, ids = connect.add_vectors(table, add_vectors)
#         sleep(add_interval_time)
#         return add_vectors, ids
# 
#     def init_data_binary(self, connect, table, nb=6000):
#         '''
#         Generate vectors and add it in table, before search vectors
#         '''
#         global binary_vectors
#         if nb == 6000:
#             add_vectors = binary_vectors
#         else:
#             add_vectors = gen_binary_vectors(nb, dim)
#         status, ids = connect.add_vectors(table, add_vectors)
#         sleep(add_interval_time)
#         return add_vectors, ids
#     
#     def init_data_no_flush(self, connect, table, nb=6000):
#         global vectors
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#         status, ids = connect.add_vectors(table, add_vectors)
#         # sleep(add_interval_time)
#         return add_vectors, ids
# 
#     def init_data_no_flush_ids(self, connect, table, nb=6000):
#         global vectors
#         my_ids = [i for i in range(nb)]
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#         status, ids = connect.add_vectors(table, add_vectors, my_ids)
#         # sleep(add_interval_time)
#         return add_vectors, ids
# 
#     def init_data_ids(self, connect, table, nb=6000):
#         global vectors
#         my_ids = [i for i in range(nb)]
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#         status, ids = connect.add_vectors(table, add_vectors, my_ids)
#         sleep(add_interval_time)
#         return add_vectors, ids
# 
#     def add_data(self, connect, table, vectors):
#         '''
#         Add specified vectors to table
#         '''
#         status, ids = connect.add_vectors(table, vectors)
#         # sleep(add_interval_time)
#         sleep(10)
#         return vectors, ids
# 
#     def add_data_ids(self, connect, table, vectors):
#         my_ids = [i for i in range(len(vectors))]
#         status, ids = connect.add_vectors(table, vectors, my_ids)
#         sleep(add_interval_time)
#         return vectors, ids
# 
#     def add_data_and_flush(self, connect, table, vectors):
#         
#         status, ids = connect.add_vectors(table, vectors)
#         connect.flush([table])
#         return vectors, ids
# 
#     def add_data_and_flush_ids(self, connect, table, vectors):
#         my_ids = [i for i in range(len(vectors))]
#         status, ids = connect.add_vectors(table, vectors, my_ids)
#         connect.flush([table])
#         return vectors, ids
# 
#     def add_data_no_flush(self, connect, table, vectors):
#         '''
#         Add specified vectors to table
#         '''
#         status, ids = connect.add_vectors(table, vectors)
#         return vectors, ids
#     
#     def add_data_no_flush_ids(self, connect, table, vectors):
#         my_ids = [i for i in range(len(vectors))]
#         status, ids = connect.add_vectors(table, vectors, my_ids)
#         return vectors, ids
# 
#     # delete data and auto flush - timeout due to the flush interval in config file
#     def delete_data(self, connect, table, ids):
#         '''
#         delete vectors by id
#         '''
#         status = connect.delete_by_id(table, ids)
#         sleep(add_interval_time)
#         return status
# 
#     # delete data and auto flush - timeout due to the flush interval in config file
#     def delete_data_no_flush(self, connect, table, ids):
#         '''
#         delete vectors by id
#         '''
#         status = connect.delete_by_id(table, ids)
#         return status
# 
#     # delete data and manual flush
#     def delete_data_and_flush(self, connect, table, ids):
#         '''
#         delete vectors by id
#         '''
#         status = connect.delete_by_id(table, ids)
#         connect.flush([table])
#         return status
#     
#     def check_no_result(self, results):
#         if len(results) == 0:
#             return True
#         flag = True
#         for r in results:
#             flag = flag and (r.id == -1)
#             if not flag:
#                 return False
#         return flag
#     
#     def init_data_partition(self, connect, table, partition_tag, nb=6000):
#         '''
#         Generate vectors and add it in table, before search vectors
#         '''
#         global vectors
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#             # add_vectors /= numpy.linalg.norm(add_vectors)
#             # add_vectors = add_vectors.tolist()
#         status, ids = connect.add_vectors(table, add_vectors, partition_tag=partition_tag)
#         sleep(add_interval_time)
#         return add_vectors, ids
#     
#     def init_data_and_flush(self, connect, table, nb=6000):
#         '''
#         Generate vectors and add it in table, before search vectors
#         '''
#         global vectors
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#             # add_vectors /= numpy.linalg.norm(add_vectors)
#             # add_vectors = add_vectors.tolist()
#         status, ids = connect.add_vectors(table, add_vectors)
#         connect.flush([table])
#         return add_vectors, ids
# 
#     def init_data_and_flush_ids(self, connect, table, nb=6000):
#         global vectors
#         my_ids = [i for i in range(nb)]
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#         status, ids = connect.add_vectors(table, add_vectors, my_ids)
#         connect.flush([table])
#         return add_vectors, ids
# 
#     def init_data_partition_and_flush(self, connect, table, partition_tag, nb=6000):
#         '''
#         Generate vectors and add it in table, before search vectors
#         '''
#         global vectors
#         if nb == 6000:
#             add_vectors = vectors
#         else:  
#             add_vectors = gen_vectors(nb, dim)
#             # add_vectors /= numpy.linalg.norm(add_vectors)
#             # add_vectors = add_vectors.tolist()
#         status, ids = connect.add_vectors(table, add_vectors, partition_tag=partition_tag)
#         connect.flush([table])
#         return add_vectors, ids
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
# 
#     """
#     generate top-k params
#     """
#     @pytest.fixture(
#         scope="function",
#         params=[1, 99, 1024, 2048]
#     )
#     def get_top_k(self, request):
#         yield request.param
# 
#     # auto flush
#     def test_search_flat_normal_topk(self, connect, table, get_top_k):
#         '''
#         target: test basic search fuction, all the search params is corrent, change top-k value
#         method: search with the given vector id, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         top_k = get_top_k
#         vectors, ids = self.init_data(connect, table, nb=small_size)
#         query_id = ids[0]
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert result[0][0].distance <= epsilon
#         assert check_result(result[0], ids[0])
# 
#     def test_search_flat_max_topk(self, connect, table):
#         '''
#         target: test basic search fuction, all the search params is corrent, change top-k value
#         method: search with the given vector id, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         top_k = 2049
#         vectors, ids = self.init_data(connect, table, nb=small_size)
#         query_id = ids[0]
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert not status.OK()
# 
#     def test_search_id_not_existed(self, connect, table):
#         '''
#         target: test basic search fuction, all the search params is corrent, change top-k value
#         method: search with the given vector id, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         vectors, ids = self.init_data_and_flush(connect, table, nb=small_size)
#         query_id = non_exist_id
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
# 
#     # auto flush
#     def test_search_ids(self, connect, table):
#         vectors, ids = self.init_data_ids(connect, table, nb=small_size)
#         query_id = ids[0]
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert result[0][0].distance <= epsilon
#         assert check_result(result[0], ids[0])
# 
#     # manual flush
#     def test_search_ids_flush(self, connect, table):
#         vectors, ids = self.init_data_and_flush_ids(connect, table, nb=small_size)
#         query_id = non_exist_id
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert self.check_no_result(result[0])
# 
#     # ------------------------------------------------------------- l2, add manual flush, delete, search ------------------------------------------------------------- #
#     # ids, manual flush, search table, exist
#     def test_search_index_l2(self, connect, table, get_simple_index_params):
#         '''
#         target: test basic search fuction, all the search params is corrent, test all index params, and build
#         method: search with the given vectors, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, table, nb=small_size)
#         status = connect.create_index(table, index_params)
#         query_id = ids[0]
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert result[0][0].distance <= epsilon
#         assert check_result(result[0], ids[0])
#     
#     # ids, manual flush, search table, non exist
#     def test_search_index_l2_id_not_existed(self, connect, table, get_simple_index_params):
#         '''
#         target: test basic search fuction, all the search params is corrent, test all index params, and build
#         method: search with the given vectors, check the result
#         expected: search status ok, and the length of the result is top_k
#         '''
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, table, nb=small_size)
#         status = connect.create_index(table, index_params)
#         query_id = non_exist_id
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
# 
#     # ids, manual flush, delete, manual flush, search table, exist
#     def test_search_index_delete(self, connect, table, get_simple_index_params):
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, table, nb=small_size)
#         status = connect.create_index(table, index_params)
#         query_id = ids[0]
#         status = self.delete_data_and_flush(connect, table, [query_id])
#         assert status.OK()
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert status.OK()
#         assert self.check_no_result(result[0])
# 
#     # ids, manual flush, delete, manual flush, search table, non exist
#     def test_search_index_delete_id_not_existed(self, connect, table, get_simple_index_params):
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, table, nb=small_size)
#         status = connect.create_index(table, index_params)
#         query_id = ids[0]
#         status = self.delete_data_and_flush(connect, table, [query_id])
#         assert status.OK()
#         query_id = non_exist_id
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert status.OK()
#         assert self.check_no_result(result[0])
# 
#     def test_search_index_delete_no_flush(self, connect, table, get_simple_index_params):
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, table, nb=small_size)
#         status = connect.create_index(table, index_params)
#         query_id = ids[0]
#         status = self.delete_data_no_flush(connect, table, [query_id])
#         assert status.OK()
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert status.OK()
#         assert check_result(result[0], query_id)
# 
#     # ids, manual flush, delete, no flush, search table, non exist
#     def test_search_index_delete_no_flush_id_not_existed(self, connect, table, get_simple_index_params):
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, table, nb=small_size)
#         status = connect.create_index(table, index_params)
#         query_id = ids[0]
#         status = self.delete_data_no_flush(connect, table, [query_id])
#         assert status.OK()
#         query_id = non_exist_id
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert status.OK()
#         assert self.check_no_result(result[0])
# 
#     def test_search_index_delete_add(self, connect, table, get_simple_index_params):
#         index_params = get_simple_index_params
#         vectors, ids = self.init_data_and_flush_ids(connect, table, nb=small_size)
#         status = connect.create_index(table, index_params)
#         query_id = ids[0]
#         status = self.delete_data_no_flush(connect, table, [query_id])
#         assert status.OK()
#         vectors, new_ids = self.add_data_and_flush_ids(connect, table, vectors)
#         status = connect.create_index(table, index_params)
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert result[0][0].distance <= epsilon
#         assert check_result(result[0], query_id)
#         status = self.delete_data_no_flush(connect, table, [query_id])
#         assert status.OK()
# 
#     # add to table, auto flush, search table, search partition exist
#     def test_search_l2_index_partition(self, connect, table, get_simple_index_params):
#         '''
#         target: test basic search fuction, all the search params is corrent, test all index params, and build
#         method: add vectors into table, search with the given vectors, check the result
#         expected: search status ok, and the length of the result is top_k, search table with partition tag return empty
#         '''
#         index_params = get_simple_index_params
#         status = connect.create_partition(table, tag)
#         vectors, ids = self.init_data(connect, table, nb=small_size)
#         status = connect.create_index(table, index_params)
#         query_id = ids[0]
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert check_result(result[0], ids[0])
#         assert result[0][0].distance <= epsilon
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id, partition_tag_array=[tag])
#         assert status.OK() 
#         assert len(result) == 0
# 
#     # add to partition, auto flush, search partition exist
#     def test_search_l2_index_params_partition_2(self, connect, table, get_simple_index_params):
#         index_params = get_simple_index_params
#         status = connect.create_partition(table, tag)
#         vectors, ids = self.init_data_partition(connect, table, tag, nb=small_size)
#         status = connect.create_index(table, index_params)
#         query_id = ids[0]
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id, partition_tag_array=[tag])
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert check_result(result[0], query_id)
# 
#     def test_search_l2_index_partition_id_not_existed(self, connect, table, get_simple_index_params):
#         index_params = get_simple_index_params
#         status = connect.create_partition(table, tag)
#         vectors, ids = self.init_data(connect, table, nb=small_size)
#         status = connect.create_index(table, index_params)
#         query_id = non_exist_id
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id, partition_tag_array=[tag])
#         assert status.OK()
#         assert len(result) == 0
# 
#     # add to table, manual flush, search non-existing partition non exist
#     def test_search_l2_index_partition_tag_not_existed(self, connect, table, get_simple_index_params):
#         index_params = get_simple_index_params
#         status = connect.create_partition(table, tag)
#         vectors, ids = self.init_data_partition_and_flush(connect, table, tag, nb=small_size)
#         status = connect.create_index(table, index_params)
#         query_id = non_exist_id
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id, partition_tag_array=['non_existing_tag'])
#         assert status.OK()
#         assert len(result) == 0
#     
#     def test_search_l2_index_partitions(self, connect, table, get_simple_index_params):
#         new_tag = "new_tag"
#         index_params = get_simple_index_params
#         status = connect.create_partition(table, tag)
#         status = connect.create_partition(table, new_tag)
#         vectors, ids = self.init_data_partition_and_flush(connect, table, tag, nb=small_size)
#         vectors, new_ids = self.init_data_partition_and_flush(connect, table, new_tag, nb=small_size)
#         status = connect.create_index(table, index_params)
#         query_id = ids[0]
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id, partition_tag_array=[tag, new_tag])
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert check_result(result[0], ids[0])
#         assert result[0][0].distance <= epsilon
#         query_id = new_ids[0]
#         status, result = connect.search_by_id(table, top_k, nprobe, query_id, partition_tag_array=[tag, new_tag])
#         assert status.OK()
#         assert len(result[0]) == min(len(vectors), top_k)
#         assert check_result(result[0], new_ids[0])
#         assert result[0][0].distance <= epsilon
# 
#     @pytest.mark.level(2)
#     def test_search_by_id_without_connect(self, dis_connect, table):
#         '''
#         target: test search vectors without connection
#         method: use dis connected instance, call search method and check if search successfully
#         expected: raise exception
#         '''
#         query_idtors = 123
#         with pytest.raises(Exception) as e:
#             status, ids = dis_connect.search_by_id(table, top_k, nprobe, query_idtors)
# 
#     def test_search_table_name_not_existed(self, connect, table):
#         '''
#         target: search table not existed
#         method: search with the random table_name, which is not in db
#         expected: status not ok
#         '''
#         table_name = gen_unique_str("not_existed_table")
#         query_id = non_exist_id
#         status, result = connect.search_by_id(table_name, top_k, nprobe, query_id)
#         assert not status.OK()
# 
#     def test_search_table_name_None(self, connect, table):
#         '''
#         target: search table that table name is None
#         method: search with the table_name: None
#         expected: status not ok
#         '''
#         table_name = None
#         query_ids = non_exist_id
#         with pytest.raises(Exception) as e: 
#             status, result = connect.search_by_id(table_name, top_k, nprobe, query_id)
# 
#     def test_search_jac(self, connect, jac_table, get_jaccard_index_params):
#         index_params = get_jaccard_index_params
#         vectors, ids = self.init_data_binary(connect, jac_table)
#         status = connect.create_index(jac_table, index_params)
#         assert status.OK()
#         query_id = ids[0]
#         status, result = connect.search_by_id(jac_table, top_k, nprobe, query_id)
#         logging.getLogger().info(status)
#         logging.getLogger().info(result)
#         assert status.OK()
#         assert check_result(result[0], ids[0])
#         assert result[0][0].distance <= epsilon
# 
#     def test_search_ham(self, connect, ham_table, get_hamming_index_params):
#         index_params = get_hamming_index_params
#         vectors, ids = self.init_data_binary(connect, ham_table)
#         status = connect.create_index(ham_table, index_params)
#         assert status.OK()
#         query_id = ids[0]
#         status, result = connect.search_by_id(ham_table, top_k, nprobe, query_id)
#         logging.getLogger().info(status)
#         logging.getLogger().info(result)
#         assert status.OK()
#         assert check_result(result[0], ids[0])
#         assert result[0][0].distance <= epsilon 
# 
# 
# """
# ******************************************************************
# #  The following cases are used to test `search_by_id` function 
# #  with invalid table_name top-k / nprobe / query_range
# ******************************************************************
# """
# 
# class TestSearchParamsInvalid(object):
#     nlist = 16384
#     index_param = {"index_type": IndexType.IVF_SQ8, "nlist": nlist}
# 
#     """
#     Test search table with invalid table names
#     """
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_table_names()
#     )
#     def get_table_name(self, request):
#         yield request.param
# 
#     @pytest.mark.level(2)
#     def test_search_with_invalid_tablename(self, connect, get_table_name):
#         table_name = get_table_name
#         query_id = non_exist_id
#         status, result = connect.search_by_id(table_name, top_k, nprobe, query_id)
#         assert not status.OK()
# 
#     @pytest.mark.level(1)
#     def test_search_with_invalid_tag_format(self, connect, table):
#         query_id = non_exist_id
#         with pytest.raises(Exception) as e:
#             status, result = connect.search_by_id(table_name, top_k, nprobe, query_id, partition_tag_array="tag")
# 
#     """
#     Test search table with invalid top-k
#     """
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_top_ks()
#     )
#     def get_top_k(self, request):
#         yield request.param
# 
#     @pytest.mark.level(1)
#     def test_search_with_invalid_top_k(self, connect, table, get_top_k):
#         top_k = get_top_k
#         query_id = non_exist_id
#         if isinstance(top_k, int):
#             status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#             assert not status.OK()
#         else:
#             with pytest.raises(Exception) as e:
#                 status, result = connect.search_by_id(table, top_k, nprobe, query_id)
# 
#     @pytest.mark.level(2)
#     def test_search_with_invalid_top_k_ip(self, connect, ip_table, get_top_k):
#         top_k = get_top_k
#         query_id = non_exist_id
#         if isinstance(top_k, int):
#             status, result = connect.search_by_id(ip_table, top_k, nprobe, query_id)
#             assert not status.OK()
#         else:
#             with pytest.raises(Exception) as e:
#                 status, result = connect.search_by_id(ip_table, top_k, nprobe, query_id)
#     
#     """
#     Test search table with invalid nprobe
#     """
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_nprobes()
#     )
#     def get_nprobes(self, request):
#         yield request.param
# 
#     @pytest.mark.level(1)
#     def test_search_with_invalid_nprobe(self, connect, table, get_nprobes):
#         nprobe = get_nprobes
#         logging.getLogger().info(nprobe)
#         query_id = non_exist_id
#         if isinstance(nprobe, int):
#             status, result = connect.search_by_id(table, top_k, nprobe, query_id)
#             assert not status.OK()
#         else:
#             with pytest.raises(Exception) as e:
#                 status, result = connect.search_by_id(table, top_k, nprobe, query_id)
# 
#     @pytest.mark.level(2)
#     def test_search_with_invalid_nprobe_ip(self, connect, ip_table, get_nprobes):
#         '''
#         target: test search fuction, with the wrong top_k
#         method: search with top_k
#         expected: raise an error, and the connection is normal
#         '''
#         nprobe = get_nprobes
#         logging.getLogger().info(nprobe)
#         query_id = non_exist_id
#         if isinstance(nprobe, int):
#             status, result = connect.search_by_id(ip_table, top_k, nprobe, query_id)
#             assert not status.OK()
#         else:
#             with pytest.raises(Exception) as e:
#                 status, result = connect.search_by_id(ip_table, top_k, nprobe, query_id)
# 
#     """
#     Test search table with invalid ids
#     """
#     @pytest.fixture(
#         scope="function",
#         params=gen_invalid_vector_ids()
#     )
#     def get_vector_ids(self, request):
#         yield request.param
# 
#     @pytest.mark.level(1)
#     def test_search_flat_with_invalid_vector_id(self, connect, table, get_vector_ids):
#         '''
#         target: test search fuction, with the wrong query_range
#         method: search with query_range
#         expected: raise an error, and the connection is normal
#         '''
#         query_id = get_vector_ids
#         logging.getLogger().info(query_id)
#         with pytest.raises(Exception) as e:
#             status, result = connect.search_by_id(table, top_k, nprobe, query_id)
# 
#     @pytest.mark.level(2)
#     def test_search_flat_with_invalid_vector_id_ip(self, connect, ip_table, get_vector_ids):
#         query_id = get_vector_ids
#         logging.getLogger().info(query_id)
#         with pytest.raises(Exception) as e:
#             status, result = connect.search_by_id(ip_table, top_k, nprobe, query_id)
# 
# 
# def check_result(result, id):
#     if len(result) >= 5:
#         return id in [x.id for x in result[:5]]
#     else:
#         return id in (i.id for i in result)
