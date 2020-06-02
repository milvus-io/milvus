# import time
# import pdb
# import threading
# import logging
# import random
# import threading
# from multiprocessing import Pool, Process
# import pytest
# from milvus import IndexType, MetricType
# from utils import *
#
#
# dim = 128
# index_file_size = 10
# collection_id = "test_add"
# ADD_TIMEOUT = 60
# tag = "1970-01-01"
# add_interval_time = 1.5
# nb = 6000
#
# class TestInsertBase:
#     """
#     ******************************************************************
#       The following cases are used to test `insert` function
#     ******************************************************************
#     """
#
#     @pytest.fixture(
#         scope="function",
#         params=gen_simple_index()
#     )
#     def get_simple_index(self, request, connect):
#         if str(connect._cmd("mode")[1]) == "CPU":
#             if request.param["index_type"] == IndexType.IVFSQ8H:
#                 pytest.skip("sq8h not support in cpu mode")
#         if request.param["index_type"] == IndexType.IVF_PQ:
#             pytest.skip("Skip PQ Temporary")
#         return request.param
#
#     def test_insert_create_collection(self, connect, hybrid_collection):
#         '''
#         target: test insert entity, then create collection again
#         method: add vector and create collection
#         expected: status not ok
#         '''
#         attr_int_list = gen_int_attr(nb)
#         attr_float_list = gen_float_attr(nb)
#         vector = gen_single_vector(dim)
#         attr_records = [
#             {"field_name": "A", "field_values": attr_int_list},
#             {"field_name": "B", "field_values": attr_int_list},
#             {"field_name": "C", "field_values": attr_float_list},
#         ]
#         vector_records = [
#             {"field_name": "Vec", "field_values": vector}
#         ]
#         status, ids = connect.insert_hybrid(hybrid_collection, attr_records, vector_records)
#
#     def test_insert_entity_has_collection(self, connect, hybrid_collection):
#         '''
#         target: test insert entity, then check collection existence
#         method: add vector and call Hascollection
#         expected: collection exists, status ok
#         '''
#         attr_int_list = gen_int_attr(nb)
#         attr_float_list = gen_float_attr(nb)
#         vector = gen_single_vector(dim)
#         attr_records = [
#             {"field_name": "A", "field_values": attr_int_list},
#             {"field_name": "B", "field_values": attr_int_list},
#             {"field_name": "C", "field_values": attr_float_list},
#         ]
#         vector_records = [
#             {"field_name": "Vec", "field_values": vector}
#         ]
#         status, ids = connect.insert_hybrid(hybrid_collection, attr_records, vector_records)
#         assert assert_has_collection(connect, hybrid_collection)