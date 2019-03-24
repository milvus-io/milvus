import faiss
from enum import Enum, unique


@unique
class INDEX_DEVICES(Enum):
    CPU = 0
    GPU = 1
    MULTI_GPU = 2


def FactoryIndex(index_name="DefaultIndex"):
    cls = globals()[index_name]
    return cls # invoke __init__() by user


class Index():
    def build(d, vectors, DEVICE=INDEX_DEVICES.CPU):
        pass

    @staticmethod
    def increase(trained_index, vectors):
        trained_index.add((vectors))

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

    def build(self, d, vectors, DEVICE=INDEX_DEVICES.CPU):
        index = faiss.IndexFlatL2(d) # trained
        index.add(vectors)
        return index


class LowMemoryIndex(Index):
    def __init__(self, *args, **kwargs):
        self.__nlist = 100
        self.__bytes_per_vector = 8
        self.__bits_per_sub_vector = 8

    def build(d, vectors, DEVICE=INDEX_DEVICES.CPU):
        # quantizer = faiss.IndexFlatL2(d)
        # index = faiss.IndexIVFPQ(quantizer, d, self.nlist,
        #                          self.__bytes_per_vector, self.__bits_per_sub_vector)
        # return index
        pass