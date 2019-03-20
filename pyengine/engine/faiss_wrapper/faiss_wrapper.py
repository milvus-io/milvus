import faiss
import numpy as np

class FaissSearch():
    def __init__(self, index, id_to_vector_map):
        pass

    # def search_by_ids(self, id_list, k):
    #     pass

    def search_by_vectors(self, vector_list, k):
        # return both id and vector
        pass

    def __search__(self, id_list, vector_list, k):
        pass


class FaissIndex():
    def build_index(self, vector_list, dimension):
        # return index
        pass

    # def build_index_cpu(self):
    #     pass

    # def build_index_gpu(self):
    #     pass