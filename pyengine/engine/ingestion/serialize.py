import faiss
import numpy as np


def write_index(index, file_name):
    faiss.write_index(index, file_name)


def read_index(file_name):
    return faiss.read_index(file_name)


def to_array(vec):
    return np.asarray(vec).astype('float32')


def to_int_array(vec):
    return np.asarray(vec).astype('int64')
