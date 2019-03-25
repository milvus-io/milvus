from engine.retrieval import search_index
from engine.ingestion import build_index
from engine.ingestion import serialize

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Scheduler(metaclass=Singleton):
    def Search(self, index_file_key, vectors, k):
        # assert index_file_key
        # assert vectors
        # assert k

        return self.__scheduler(index_file_key, vectors, k)


    def __scheduler(self, index_data_key, vectors, k):
        result_list = []

        if 'raw' in index_data_key:
            raw_vectors = index_data_key['raw']
            d = index_data_key['dimension']

        if 'raw' in index_data_key:
            index_builder = build_index.FactoryIndex()
            index = index_builder().build(d, raw_vectors)
            searcher = search_index.FaissSearch(index)
            result_list.append(searcher.search_by_vectors(vectors, k))

        index_data_list = index_data_key['index']
        for key in index_data_list:
            index = GetIndexData(key)
            searcher = search_index.FaissSearch(index)
            result_list.append(searcher.search_by_vectors(vectors, k))

        if len(result_list) == 1:
            return result_list[0].vectors


        # result = search_index.top_k(result_list, k)
        return result_list


def GetIndexData(key):
    return serialize.read_index(key)