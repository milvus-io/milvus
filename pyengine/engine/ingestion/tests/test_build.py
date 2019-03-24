from ..build_index import *

import faiss
import numpy as np
import unittest


class TestBuildIndex(unittest.TestCase):
    def test_factory_method(self):
        pass

    def test_default_index(self):
        d = 64
        nb = 10000
        nq = 100
        _, xb, xq = get_dataset(d, nb, 500, nq)

        # Expected result
        index = faiss.IndexFlatL2(d)
        index.add(xb)
        Dref, Iref = index.search(xq, 5)

        builder = DefaultIndex()
        index2 = builder.build(d, xb)
        Dnew, Inew = index2.search(xq, 5)

        assert np.all(Dnew == Dref) and np.all(Inew == Iref)

    def test_increase(self):
        d = 64
        nb = 10000
        nq = 100
        _, xb, xq = get_dataset(d, nb, 500, nq)

        index = faiss.IndexFlatL2(d)
        index.add(xb)

        pass

    def test_serialize(self):
        pass


def get_dataset(d, nb, nt, nq):
    """A dataset that is not completely random but still challenging to
    index
    """
    d1 = 10     # intrinsic dimension (more or less)
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