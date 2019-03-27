from ..scheduler import *

import unittest
import faiss
import numpy as np


class TestScheduler(unittest.TestCase):
    def test_single_query(self):
        d = 64
        nb = 10000
        nq = 1
        nt = 5000
        xt, xb, xq = get_dataset(d, nb, nt, nq)
        ids_xb = np.arange(xb.shape[0])
        ids_xt = np.arange(xt.shape[0])
        file_name = "/tmp/tempfile_1"

        index = faiss.IndexFlatL2(d)
        index2 = faiss.IndexIDMap(index)
        index2.add_with_ids(xb, ids_xb)
        Dref, Iref = index.search(xq, 5)
        faiss.write_index(index, file_name)

        scheduler_instance = Scheduler()

        # query 1
        query_index = dict()
        query_index['index'] = [file_name]
        vectors = scheduler_instance.search(query_index, vectors=xq, k=5)
        assert np.all(vectors == Iref)

        # query 2
        query_index.clear()
        query_index['raw'] = xb
        query_index['raw_id'] = ids_xb
        query_index['dimension'] = d
        vectors = scheduler_instance.search(query_index, vectors=xq, k=5)
        assert np.all(vectors == Iref)

        # query 3
        # TODO(linxj): continue...
        # query_index.clear()
        # query_index['raw'] = xt
        # query_index['raw_id'] = ids_xt
        # query_index['dimension'] = d
        # query_index['index'] = [file_name]
        # vectors = scheduler_instance.search(query_index, vectors=xq, k=5)
        # assert np.all(vectors == Iref)


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
