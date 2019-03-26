from ..search_index import *

import unittest
import numpy as np


class TestSearchSingleThread(unittest.TestCase):
    def test_search_by_vectors(self):
        d = 64
        nb = 10000
        nq = 100
        _, xb, xq = get_dataset(d, nb, 500, nq)

        index = faiss.IndexFlatL2(d)
        index.add(xb)

        # expect result
        Dref, Iref = index.search(xq, 5)

        searcher = FaissSearch(index)
        result = searcher.search_by_vectors(xq, 5)

        assert np.all(result.distance == Dref) \
               and np.all(result.vectors == Iref)
        pass

    def test_top_k(selfs):
        pass


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
