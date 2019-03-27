from ..build_index import *

import faiss
import numpy as np
import unittest


class TestBuildIndex(unittest.TestCase):
    def test_factory_method(self):
        index_builder = FactoryIndex()
        index = index_builder()
        self.assertIsInstance(index, DefaultIndex)

    def test_default_index(self):
        d = 64
        nb = 10000
        nq = 100
        _, xb, xq = get_dataset(d, nb, 500, nq)
        ids = np.arange(xb.shape[0])

        # Expected result
        index = faiss.IndexFlatL2(d)
        index2 = faiss.IndexIDMap(index)
        index2.add_with_ids(xb, ids)
        Dref, Iref = index.search(xq, 5)

        builder = DefaultIndex()
        index2 = builder.build(d, xb, ids)
        Dnew, Inew = index2.search(xq, 5)

        assert np.all(Dnew == Dref) and np.all(Inew == Iref)

    def test_increase(self):
        # d = 64
        # nb = 10000
        # nq = 100
        # nt = 500
        # xt, xb, xq = get_dataset(d, nb, nt, nq)
        #
        # index = faiss.IndexFlatL2(d)
        # index.add(xb)
        #
        # assert index.ntotal == nb
        #
        # Index.increase(index, xt)
        # assert index.ntotal == nb + nt
        pass

    def test_serialize(self):
        d = 64
        nb = 10000
        nq = 100
        nt = 500
        xt, xb, xq = get_dataset(d, nb, nt, nq)

        index = faiss.IndexFlatL2(d)
        index.add(xb)
        Dref, Iref = index.search(xq, 5)

        ar_data = Index.serialize(index)

        reader = faiss.VectorIOReader()
        faiss.copy_array_to_vector(ar_data, reader.data)
        index2 = faiss.read_index(reader)

        Dnew, Inew = index2.search(xq, 5)

        assert np.all(Dnew == Dref) and np.all(Inew == Iref)


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
