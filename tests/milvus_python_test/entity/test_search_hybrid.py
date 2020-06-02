# import pdb
# import struct
# from random import sample
#
# import pytest
# import threading
# import datetime
# import logging
# from time import sleep
# from multiprocessing import Process
# import numpy
# import sklearn.preprocessing
# from milvus import IndexType, MetricType
# from utils import *
#
# dim = 128
# collection_id = "test_search"
# add_interval_time = 2
# vectors = gen_vectors(6000, dim)
# vectors = sklearn.preprocessing.normalize(vectors, axis=1, norm='l2')
# vectors = vectors.tolist()
# top_k = 1
# nprobe = 1
# epsilon = 0.001
# tag = "1970-01-01"
#
#
# class TestSearchBase:
#     def init_data(self, connect, hybrid_collection, nb=6000, partition_tags=None):
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
#
#         attr_int_list = gen_int_attr(nb)
#         attr_float_list = gen_float_attr(nb)
#         attr_records = [
#             {"field_name": "A", "field_values": attr_int_list},
#             {"field_name": "B", "field_values": attr_int_list},
#             {"field_name": "C", "field_values": attr_float_list},
#         ]
#         vector_records = [
#             {"field_name": "Vec", "field_values": vectors}
#         ]
#
#         if partition_tags is None:
#             status, ids = connect.insert_hybrid(hybrid_collection, attr_records, vector_records)
#             assert status.OK()
#         else:
#             status, ids = connect.insert_hybrid(hybrid_collection, attr_records, vector_records, partition_tag=partition_tags)
#             assert status.OK()
#         connect.flush([hybrid_collection])
#         return attr_int_list, attr_float_list, vector_records, ids
#
#