# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

"""this is a basic test script for simple indices work"""

import numpy as np
import unittest
import faiss


def make_binary_dataset(d, nt, nb, nq):
    assert d % 8 == 0
    rs = np.random.RandomState(123)
    x = rs.randint(256, size=(nb + nq + nt, int(d / 8))).astype('uint8')
    return x[:nt], x[nt:-nq], x[-nq:]


def binary_to_float(x):
    n, d = x.shape
    x8 = x.reshape(n * d, -1)
    c8 = 2 * ((x8 >> np.arange(8)) & 1).astype('int8') - 1
    return c8.astype('float32').reshape(n, d * 8)


def binary_dis(x, y):
    return sum(faiss.popcount64(int(xi ^ yi)) for xi, yi in zip(x, y))


class TestBinaryPQ(unittest.TestCase):
    """ Use a PQ that mimicks a binary encoder """

    def test_encode_to_binary(self):
        d = 256
        nt = 256
        nb = 1500
        nq = 500
        (xt, xb, xq) = make_binary_dataset(d, nt, nb, nq)
        pq = faiss.ProductQuantizer(d, int(d / 8), 8)

        centroids = binary_to_float(
            np.tile(np.arange(256), int(d / 8)).astype('uint8').reshape(-1, 1))

        faiss.copy_array_to_vector(centroids.ravel(), pq.centroids)
        pq.is_trained = True

        codes = pq.compute_codes(binary_to_float(xb))

        assert np.all(codes == xb)

        indexpq = faiss.IndexPQ(d, int(d / 8), 8)
        indexpq.pq = pq
        indexpq.is_trained = True

        indexpq.add(binary_to_float(xb))
        D, I = indexpq.search(binary_to_float(xq), 3)

        for i in range(nq):
            for j, dj in zip(I[i], D[i]):
                ref_dis = binary_dis(xq[i], xb[j])
                assert 4 * ref_dis == dj

        nlist = 32
        quantizer = faiss.IndexFlatL2(d)
        # pretext class for training
        iflat = faiss.IndexIVFFlat(quantizer, d, nlist)
        iflat.train(binary_to_float(xt))

        indexivfpq = faiss.IndexIVFPQ(quantizer, d, nlist, int(d / 8), 8)

        indexivfpq.pq = pq
        indexivfpq.is_trained = True
        indexivfpq.by_residual = False

        indexivfpq.add(binary_to_float(xb))
        indexivfpq.nprobe = 4

        D, I = indexivfpq.search(binary_to_float(xq), 3)

        for i in range(nq):
            for j, dj in zip(I[i], D[i]):
                ref_dis = binary_dis(xq[i], xb[j])
                assert 4 * ref_dis == dj


class TestBinaryFlat(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        d = 32
        nt = 0
        nb = 1500
        nq = 500

        (_, self.xb, self.xq) = make_binary_dataset(d, nt, nb, nq)

    def test_flat(self):
        d = self.xq.shape[1] * 8
        nq = self.xq.shape[0]

        index = faiss.IndexBinaryFlat(d)
        index.add(self.xb)
        D, I = index.search(self.xq, 3)

        for i in range(nq):
            for j, dj in zip(I[i], D[i]):
                ref_dis = binary_dis(self.xq[i], self.xb[j])
                assert dj == ref_dis

        # test reconstruction
        assert np.all(index.reconstruct(12) == self.xb[12])

    def test_empty_flat(self):
        d = self.xq.shape[1] * 8

        index = faiss.IndexBinaryFlat(d)

        for use_heap in [True, False]:
            index.use_heap = use_heap
            Dflat, Iflat = index.search(self.xq, 10)

            assert(np.all(Iflat == -1))
            assert(np.all(Dflat == 2147483647)) # NOTE(hoss): int32_t max


class TestBinaryIVF(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        d = 32
        nt = 200
        nb = 1500
        nq = 500

        (self.xt, self.xb, self.xq) = make_binary_dataset(d, nt, nb, nq)
        index = faiss.IndexBinaryFlat(d)
        index.add(self.xb)
        Dref, Iref = index.search(self.xq, 10)
        self.Dref = Dref

    def test_ivf_flat_exhaustive(self):
        d = self.xq.shape[1] * 8

        quantizer = faiss.IndexBinaryFlat(d)
        index = faiss.IndexBinaryIVF(quantizer, d, 8)
        index.cp.min_points_per_centroid = 5    # quiet warning
        index.nprobe = 8
        index.train(self.xt)
        index.add(self.xb)
        Divfflat, _ = index.search(self.xq, 10)

        np.testing.assert_array_equal(self.Dref, Divfflat)

    def test_ivf_flat2(self):
        d = self.xq.shape[1] * 8

        quantizer = faiss.IndexBinaryFlat(d)
        index = faiss.IndexBinaryIVF(quantizer, d, 8)
        index.cp.min_points_per_centroid = 5    # quiet warning
        index.nprobe = 4
        index.train(self.xt)
        index.add(self.xb)
        Divfflat, _ = index.search(self.xq, 10)

        self.assertEqual((self.Dref == Divfflat).sum(), 4122)

    def test_ivf_flat_empty(self):
        d = self.xq.shape[1] * 8

        index = faiss.IndexBinaryIVF(faiss.IndexBinaryFlat(d), d, 8)
        index.train(self.xt)

        for use_heap in [True, False]:
            index.use_heap = use_heap
            Divfflat, Iivfflat = index.search(self.xq, 10)

            assert(np.all(Iivfflat == -1))
            assert(np.all(Divfflat == 2147483647)) # NOTE(hoss): int32_t max

class TestHNSW(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        d = 32
        nt = 0
        nb = 1500
        nq = 500

        (_, self.xb, self.xq) = make_binary_dataset(d, nt, nb, nq)

    def test_hnsw_exact_distances(self):
        d = self.xq.shape[1] * 8
        nq = self.xq.shape[0]

        index = faiss.IndexBinaryHNSW(d, 16)
        index.add(self.xb)
        Dists, Ids = index.search(self.xq, 3)

        for i in range(nq):
            for j, dj in zip(Ids[i], Dists[i]):
                ref_dis = binary_dis(self.xq[i], self.xb[j])
                self.assertEqual(dj, ref_dis)

    def test_hnsw(self):
        d = self.xq.shape[1] * 8

        # NOTE(hoss): Ensure the HNSW construction is deterministic.
        nthreads = faiss.omp_get_max_threads()
        faiss.omp_set_num_threads(1)

        index_hnsw_float = faiss.IndexHNSWFlat(d, 16)
        index_hnsw_ref = faiss.IndexBinaryFromFloat(index_hnsw_float)

        index_hnsw_bin = faiss.IndexBinaryHNSW(d, 16)

        index_hnsw_ref.add(self.xb)
        index_hnsw_bin.add(self.xb)

        faiss.omp_set_num_threads(nthreads)

        Dref, Iref = index_hnsw_ref.search(self.xq, 3)
        Dbin, Ibin = index_hnsw_bin.search(self.xq, 3)

        self.assertTrue((Dref == Dbin).all())


def compare_binary_result_lists(D1, I1, D2, I2):
    """comparing result lists is difficult because there are many
    ties. Here we sort by (distance, index) pairs and ignore the largest
    distance of each result. Compatible result lists should pass this."""
    assert D1.shape == I1.shape == D2.shape == I2.shape
    n, k = D1.shape
    ndiff = (D1 != D2).sum()
    assert ndiff == 0, '%d differences in distance matrix %s' % (
        ndiff, D1.shape)

    def normalize_DI(D, I):
        norm = I.max() + 1.0
        Dr = D.astype('float64') + I / norm
        # ignore -1s and elements on last column
        Dr[I1 == -1] = 1e20
        Dr[D == D[:, -1:]] = 1e20
        Dr.sort(axis=1)
        return Dr
    ndiff = (normalize_DI(D1, I1) != normalize_DI(D2, I2)).sum()
    assert ndiff == 0, '%d differences in normalized D matrix' % ndiff


class TestReplicasAndShards(unittest.TestCase):

    def test_replicas(self):
        d = 32
        nq = 100
        nb = 200

        (_, xb, xq) = make_binary_dataset(d, 0, nb, nq)

        index_ref = faiss.IndexBinaryFlat(d)
        index_ref.add(xb)

        Dref, Iref = index_ref.search(xq, 10)

        nrep = 5
        index = faiss.IndexBinaryReplicas()
        for i in range(nrep):
            sub_idx = faiss.IndexBinaryFlat(d)
            sub_idx.add(xb)
            index.addIndex(sub_idx)

        D, I = index.search(xq, 10)

        self.assertTrue((Dref == D).all())
        self.assertTrue((Iref == I).all())

        index2 = faiss.IndexBinaryReplicas()
        for i in range(nrep):
            sub_idx = faiss.IndexBinaryFlat(d)
            index2.addIndex(sub_idx)

        index2.add(xb)
        D2, I2 = index2.search(xq, 10)

        self.assertTrue((Dref == D2).all())
        self.assertTrue((Iref == I2).all())

    def test_shards(self):
        d = 32
        nq = 100
        nb = 200

        (_, xb, xq) = make_binary_dataset(d, 0, nb, nq)

        index_ref = faiss.IndexBinaryFlat(d)
        index_ref.add(xb)

        Dref, Iref = index_ref.search(xq, 10)

        nrep = 5
        index = faiss.IndexBinaryShards(d)
        for i in range(nrep):
            sub_idx = faiss.IndexBinaryFlat(d)
            sub_idx.add(xb[i * nb // nrep : (i + 1) * nb // nrep])
            index.add_shard(sub_idx)

        D, I = index.search(xq, 10)

        compare_binary_result_lists(Dref, Iref, D, I)

        index2 = faiss.IndexBinaryShards(d)
        for i in range(nrep):
            sub_idx = faiss.IndexBinaryFlat(d)
            index2.add_shard(sub_idx)

        index2.add(xb)
        D2, I2 = index2.search(xq, 10)

        compare_binary_result_lists(Dref, Iref, D2, I2)


if __name__ == '__main__':
    unittest.main()
