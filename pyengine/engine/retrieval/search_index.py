import faiss
import numpy as np


class SearchResult():
    def __init__(self, D, I):
        self.distance = D
        self.vectors = I

    def __add__(self, other):
        distance = self.distance + other.distance
        vectors = self.vectors + other.vectors
        return SearchResult(distance, vectors)


class FaissSearch():
    def __init__(self, index_data, id_to_vector_map=None):
        self.__index = index_data

        if id_to_vector_map is None:
            self.__id_to_vector_map = []

    # def search_by_ids(self, id_list, k):
    #     pass

    def search_by_vectors(self, query_vectors, k):
        id_list = [None] * len(query_vectors)

        result = self.__search(id_list, query_vectors, k)
        return result

    def __search(self, id_list, vector_list, k):
        D, I = self.__index.search(vector_list, k)
        return SearchResult(D, I)


# import heapq
def top_k(input, k):
    pass
    # sorted = heapq.nsmallest(k, input, key=np.sum(input.get()))
    # return sorted
