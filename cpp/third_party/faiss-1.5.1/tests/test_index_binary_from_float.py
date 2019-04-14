# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import absolute_import, division, print_function
import numpy as np
import unittest
import faiss


def make_binary_dataset(d, nb, nt, nq):
    assert d % 8 == 0
    rs = np.random.RandomState(123)
    x = rs.randint(256, size=(nb + nq + nt, int(d / 8))).astype('uint8')
    return x[:nt], x[nt:-nq], x[-nq:]


def binary_to_float(x):
    n, d = x.shape
    x8 = x.reshape(n * d, -1)
    c8 = 2 * ((x8 >> np.arange(8)) & 1).astype('int8') - 1
    return c8.astype('float32').reshape(n, d * 8)


class TestIndexBinaryFromFloat(unittest.TestCase):
    """Use a binary index backed by a float index"""

    def test_index_from_float(self):
        d = 256
        nt = 0
        nb = 1500
        nq = 500
        (xt, xb, xq) = make_binary_dataset(d, nb, nt, nq)

        index_ref = faiss.IndexFlatL2(d)
        index_ref.add(binary_to_float(xb))

        index = faiss.IndexFlatL2(d)
        index_bin = faiss.IndexBinaryFromFloat(index)
        index_bin.add(xb)

        D_ref, I_ref = index_ref.search(binary_to_float(xq), 10)
        D, I = index_bin.search(xq, 10)

        np.testing.assert_allclose((D_ref / 4.0).astype('int32'), D)

    def test_wrapped_quantizer(self):
        d = 256
        nt = 150
        nb = 1500
        nq = 500
        (xt, xb, xq) = make_binary_dataset(d, nb, nt, nq)

        nlist = 16
        quantizer_ref = faiss.IndexBinaryFlat(d)
        index_ref = faiss.IndexBinaryIVF(quantizer_ref, d, nlist)
        index_ref.train(xt)

        index_ref.add(xb)

        unwrapped_quantizer = faiss.IndexFlatL2(d)
        quantizer = faiss.IndexBinaryFromFloat(unwrapped_quantizer)
        index = faiss.IndexBinaryIVF(quantizer, d, nlist)

        index.train(xt)

        index.add(xb)

        D_ref, I_ref = index_ref.search(xq, 10)
        D, I = index.search(xq, 10)

        np.testing.assert_array_equal(D_ref, D)

    def test_wrapped_quantizer_IMI(self):
        d = 256
        nt = 3500
        nb = 10000
        nq = 500
        (xt, xb, xq) = make_binary_dataset(d, nb, nt, nq)

        index_ref = faiss.IndexBinaryFlat(d)

        index_ref.add(xb)

        nlist_exp = 6
        nlist = 2 ** (2 * nlist_exp)
        float_quantizer = faiss.MultiIndexQuantizer(d, 2, nlist_exp)
        wrapped_quantizer = faiss.IndexBinaryFromFloat(float_quantizer)
        wrapped_quantizer.train(xt)

        assert nlist == float_quantizer.ntotal

        index = faiss.IndexBinaryIVF(wrapped_quantizer, d,
                                     float_quantizer.ntotal)
        index.nprobe = 2048
        assert index.is_trained

        index.add(xb)

        D_ref, I_ref = index_ref.search(xq, 10)
        D, I = index.search(xq, 10)

        recall = sum(gti[0] in Di[:10] for gti, Di in zip(D_ref, D)) \
                 / float(D_ref.shape[0])

        assert recall > 0.82, "recall = %g" % recall

    def test_wrapped_quantizer_HNSW(self):
        faiss.omp_set_num_threads(1)

        def bin2float(v):
            def byte2float(byte):
                return np.array([-1.0 + 2.0 * (byte & (1 << b) != 0)
                                 for b in range(0, 8)])

            return np.hstack([byte2float(byte) for byte in v]).astype('float32')

        def floatvec2nparray(v):
            return np.array([np.float32(v.at(i)) for i in range(0, v.size())]) \
                     .reshape(-1, d)

        d = 256
        nt = 12800
        nb = 10000
        nq = 500
        (xt, xb, xq) = make_binary_dataset(d, nb, nt, nq)

        index_ref = faiss.IndexBinaryFlat(d)

        index_ref.add(xb)

        nlist = 256
        clus = faiss.Clustering(d, nlist)
        clus_index = faiss.IndexFlatL2(d)

        xt_f = np.array([bin2float(v) for v in xt])
        clus.train(xt_f, clus_index)

        centroids = floatvec2nparray(clus.centroids)
        hnsw_quantizer = faiss.IndexHNSWFlat(d, 32)
        hnsw_quantizer.add(centroids)
        hnsw_quantizer.is_trained = True
        wrapped_quantizer = faiss.IndexBinaryFromFloat(hnsw_quantizer)

        assert nlist == hnsw_quantizer.ntotal
        assert nlist == wrapped_quantizer.ntotal
        assert wrapped_quantizer.is_trained

        index = faiss.IndexBinaryIVF(wrapped_quantizer, d,
                                     hnsw_quantizer.ntotal)
        index.nprobe = 128

        assert index.is_trained

        index.add(xb)

        D_ref, I_ref = index_ref.search(xq, 10)
        D, I = index.search(xq, 10)

        recall = sum(gti[0] in Di[:10] for gti, Di in zip(D_ref, D)) \
                 / float(D_ref.shape[0])

        assert recall > 0.77, "recall = %g" % recall


class TestOverrideKmeansQuantizer(unittest.TestCase):

    def test_override(self):
        d = 256
        nt = 3500
        nb = 10000
        nq = 500
        (xt, xb, xq) = make_binary_dataset(d, nb, nt, nq)

        def train_and_get_centroids(override_kmeans_index):
            index = faiss.index_binary_factory(d, "BIVF10")
            index.verbose = True

            if override_kmeans_index is not None:
                index.clustering_index = override_kmeans_index

            index.train(xt)

            centroids = faiss.downcast_IndexBinary(index.quantizer).xb
            return faiss.vector_to_array(centroids).reshape(-1, d // 8)

        centroids_ref = train_and_get_centroids(None)

        # should do the exact same thing
        centroids_new = train_and_get_centroids(faiss.IndexFlatL2(d))

        assert np.all(centroids_ref == centroids_new)

        # will do less accurate assignment... Sanity check that the
        # index is indeed used by kmeans
        centroids_new = train_and_get_centroids(faiss.IndexLSH(d, 16))

        assert not np.all(centroids_ref == centroids_new)
