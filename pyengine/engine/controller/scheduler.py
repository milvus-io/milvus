from engine.retrieval import search_index
from engine.ingestion import build_index
from engine.ingestion import serialize
import numpy as np


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Scheduler(metaclass=Singleton):
    def search(self, index_file_key, vectors, k):
        # assert index_file_key
        # assert vectors
        assert k != 0

        query_vectors = serialize.to_array(vectors)
        return self.__scheduler(index_file_key, query_vectors, k)

    def __scheduler(self, index_data_key, vectors, k):
        result_list = []

        if 'raw' in index_data_key:
            raw_vectors = index_data_key['raw']
            raw_vector_ids = index_data_key['raw_id']
            d = index_data_key['dimension']
            index_builder = build_index.FactoryIndex()
            index = index_builder().build(d, raw_vectors, raw_vector_ids)
            searcher = search_index.FaissSearch(index)
            result_list.append(searcher.search_by_vectors(vectors, k))

        if 'index' in index_data_key:
            index_data_list = index_data_key['index']
            for key in index_data_list:
                index = get_index_data(key)
                searcher = search_index.FaissSearch(index)
                result_list.append(searcher.search_by_vectors(vectors, k))

        if len(result_list) == 1:
            return result_list[0].vectors[0].tolist() # TODO(linxj): fix hard code

        return result_list;  # TODO(linxj): add topk

        # d_list = np.array([])
        # v_list = np.array([])
        # for result in result_list:
        #     rd = result.distance
        #     rv = result.vectors
        #
        #     td_list = np.array([])
        #     tv_list = np.array([])
        #     for d, v in zip(rd, rv):
        #         td_list = np.append(td_list, d)
        #         tv_list = np.append(tv_list, v)
        #     d_list = np.add(d_list, td_list)
        #     v_list = np.add(v_list, td_list)
        #
        # print(d_list)
        # print(v_list)
        # result_map = [d_list, v_list]
        # top_k_result = search_index.top_k(result_map, k)
        # return top_k_result


def get_index_data(key):
    return serialize.read_index(key)
