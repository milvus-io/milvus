import faiss
import numpy as np

class FaissSearch():
    def __init__(self, index, id_to_vector_map=None):
        self.__index = index
        if id_to_vector_map is None:
            self.__id_to_vector_map = []

    # def search_by_ids(self, id_list, k):
    #     pass

    def search_by_vectors(self, vector_list, k):
        id_list = [None] * len(vector_list)

        result = self.__search(id_list, vector_list, k)
        return result

    def __search(self, id_list, vector_list, k):
        D, I = self.__index.search(vector_list, k)
        return I


# class FaissIndex():
#     def build_index(self, vector_list, dimension):
#         # return index
#         pass
#
#     def build_index_cpu(self):
#         pass
#
#     def build_index_gpu(self):
#         pass