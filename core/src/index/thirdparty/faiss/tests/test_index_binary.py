# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""this is a basic test script for simple indices work"""
from __future__ import absolute_import, division, print_function, unicode_literals

import numpy as np
import unittest
import faiss

from common import compare_binary_result_lists, make_binary_dataset



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

    def test_range_search(self):
        d = self.xq.shape[1] * 8

        index = faiss.IndexBinaryFlat(d)
        index.add(self.xb)
        D, I = index.search(self.xq, 10)
        thresh = int(np.median(D[:, -1]))

        lims, D2, I2 = index.range_search(self.xq, thresh)
        nt1 = nt2 = 0
        for i in range(len(self.xq)):
            range_res = I2[lims[i]:lims[i + 1]]
            if thresh > D[i, -1]:
                self.assertTrue(set(I[i]) <= set(range_res))
                nt1 += 1
            elif thresh < D[i, -1]:
                self.assertTrue(set(range_res) <= set(I[i]))
                nt2 += 1
            # in case of equality we have a problem with ties
        print('nb tests', nt1, nt2)
        # nb tests is actually low...
        self.assertTrue(nt1 > 19 and nt2 > 19)


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

    def test_ivf_range(self):
        d = self.xq.shape[1] * 8

        quantizer = faiss.IndexBinaryFlat(d)
        index = faiss.IndexBinaryIVF(quantizer, d, 8)
        index.cp.min_points_per_centroid = 5    # quiet warning
        index.nprobe = 4
        index.train(self.xt)
        index.add(self.xb)
        D, I = index.search(self.xq, 10)

        radius = int(np.median(D[:, -1]) + 1)
        Lr, Dr, Ir = index.range_search(self.xq, radius)

        for i in range(len(self.xq)):
            res = Ir[Lr[i]:Lr[i + 1]]
            if D[i, -1] < radius:
                self.assertTrue(set(I[i]) <= set(res))
            else:
                subset = I[i, D[i, :] < radius]
                self.assertTrue(set(subset) == set(res))


    def test_ivf_flat_empty(self):
        d = self.xq.shape[1] * 8

        index = faiss.IndexBinaryIVF(faiss.IndexBinaryFlat(d), d, 8)
        index.train(self.xt)

        for use_heap in [True, False]:
            index.use_heap = use_heap
            Divfflat, Iivfflat = index.search(self.xq, 10)

            assert(np.all(Iivfflat == -1))
            assert(np.all(Divfflat == 2147483647)) # NOTE(hoss): int32_t max

    def test_ivf_reconstruction(self):
        d = self.xq.shape[1] * 8
        quantizer = faiss.IndexBinaryFlat(d)
        index = faiss.IndexBinaryIVF(quantizer, d, 8)
        index.cp.min_points_per_centroid = 5    # quiet warning
        index.nprobe = 4
        index.train(self.xt)

        index.add(self.xb)
        index.set_direct_map_type(faiss.DirectMap.Array)

        for i in range(0, len(self.xb), 13):
            np.testing.assert_array_equal(
                index.reconstruct(i),
                self.xb[i]
            )

        # try w/ hashtable
        index = faiss.IndexBinaryIVF(quantizer, d, 8)
        rs = np.random.RandomState(123)
        ids = rs.choice(10000, size=len(self.xb), replace=False)
        index.add_with_ids(self.xb, ids)
        index.set_direct_map_type(faiss.DirectMap.Hashtable)

        for i in range(0, len(self.xb), 13):
            np.testing.assert_array_equal(
                index.reconstruct(int(ids[i])),
                self.xb[i]
            )


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
        for _i in range(nrep):
            sub_idx = faiss.IndexBinaryFlat(d)
            sub_idx.add(xb)
            index.addIndex(sub_idx)

        D, I = index.search(xq, 10)

        self.assertTrue((Dref == D).all())
        self.assertTrue((Iref == I).all())

        index2 = faiss.IndexBinaryReplicas()
        for _i in range(nrep):
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
        for _i in range(nrep):
            sub_idx = faiss.IndexBinaryFlat(d)
            index2.add_shard(sub_idx)

        index2.add(xb)
        D2, I2 = index2.search(xq, 10)

        compare_binary_result_lists(Dref, Iref, D2, I2)


if __name__ == '__main__':
    unittest.main()
