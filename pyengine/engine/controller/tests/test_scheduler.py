from ..scheduler import *

import unittest
import faiss
import numpy as np


class TestScheduler(unittest.TestCase):
    def test_schedule(self):
        d = 64
        nb = 10000
        nq = 2
        nt = 5000
        xt, xb, xq = get_dataset(d, nb, nt, nq)
        file_name = "/tmp/tempfile_1"

        index = faiss.IndexFlatL2(d)
        print(index.is_trained)
        index.add(xb)
        faiss.write_index(index, file_name)
        Dref, Iref = index.search(xq, 5)

        index2 = faiss.read_index(file_name)

        scheduler_instance = Scheduler()

        # query args 1
        query_index = dict()
        query_index['index'] = [file_name]
        vectors = scheduler_instance.Search(query_index, vectors=xq, k=5)
        assert np.all(vectors == Iref)

        # query args 2
        query_index = dict()
        query_index['raw'] = xt
        # Xiaojun TODO: 'raw_id' part
        # query_index['raw_id'] = 
        query_index['dimension'] = d
        query_index['index'] = [file_name]

        # Xiaojun TODO: once 'raw_id' part added, open below
        # vectors = scheduler_instance.Search(query_index, vectors=xq, k=5)

        # print("success")


def get_dataset(d, nb, nt, nq):
    """A dataset that is not completely random but still challenging to
    index
    """
    d1 = 10  # intrinsic dimension (more or less)
    n = nb + nt + nq
    rs = np.random.RandomState(1338)
    x = rs.normal(size=(n, d1))
    x = np.dot(x, rs.rand(d1, d))
    # now we have a d1-dim ellipsoid in d-dimensional space
    # higher factor (>4) -> higher frequency -> less linear
    x = x * (rs.rand(d) * 4 + 0.1)
    x = np.sin(x)
    x = x.astype('float32')
    return x[:nt], x[nt:-nq], x[-nq:]


if __name__ == "__main__":
    unittest.main()
