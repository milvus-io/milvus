import faiss
from enum import Enum, unique


@unique
class INDEXDEVICES(Enum):
    CPU = 0
    GPU = 1
    MULTI_GPU = 2


def FactoryIndex(index_name="DefaultIndex"):
    cls = globals()[index_name]
    return cls  # invoke __init__() by user


class Index():
    def build(self, d, vectors, vector_ids, DEVICE=INDEXDEVICES.CPU):
        pass

    @staticmethod
    def increase(trained_index, vectors):
        trained_index.add_with_ids(vectors. vector_ids)

    @staticmethod
    def serialize(index):
        writer = faiss.VectorIOWriter()
        faiss.write_index(index, writer)
        array_data = faiss.vector_to_array(writer.data)
        return array_data


class DefaultIndex(Index):
    def __init__(self, *args, **kwargs):
        # maybe need to specif parameters
        pass

    def build(self, d, vectors, vector_ids, DEVICE=INDEXDEVICES.CPU):
        index = faiss.IndexFlatL2(d)
        index2 = faiss.IndexIDMap(index)
        index2.add_with_ids(vectors, vector_ids)
        return index2


class LowMemoryIndex(Index):
    def __init__(self, *args, **kwargs):
        self.__nlist = 100
        self.__bytes_per_vector = 8
        self.__bits_per_sub_vector = 8

    def build(d, vectors, vector_ids, DEVICE=INDEXDEVICES.CPU):
        # quantizer = faiss.IndexFlatL2(d)
        # index = faiss.IndexIVFPQ(quantizer, d, self.nlist,
        #                          self.__bytes_per_vector, self.__bits_per_sub_vector)
        # return index
        pass
