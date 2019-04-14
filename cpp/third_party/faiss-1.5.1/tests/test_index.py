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
import tempfile
import os
import re


from common import get_dataset, get_dataset_2

class TestModuleInterface(unittest.TestCase):

    def test_version_attribute(self):
        assert hasattr(faiss, '__version__')
        assert re.match('^\\d+\\.\\d+\\.\\d+$', faiss.__version__)



class EvalIVFPQAccuracy(unittest.TestCase):

    def test_IndexIVFPQ(self):
        d = 32
        nb = 1000
        nt = 1500
        nq = 200

        (xt, xb, xq) = get_dataset_2(d, nb, nt, nq)
        d = xt.shape[1]

        gt_index = faiss.IndexFlatL2(d)
        gt_index.add(xb)
        D, gt_nns = gt_index.search(xq, 1)

        coarse_quantizer = faiss.IndexFlatL2(d)
        index = faiss.IndexIVFPQ(coarse_quantizer, d, 32, 8, 8)
        index.cp.min_points_per_centroid = 5    # quiet warning
        index.train(xt)
        index.add(xb)
        index.nprobe = 4
        D, nns = index.search(xq, 10)
        n_ok = (nns == gt_nns).sum()
        nq = xq.shape[0]

        self.assertGreater(n_ok, nq * 0.66)

        # check that and Index2Layer gives the same reconstruction
        # this is a bit fragile: it assumes 2 runs of training give
        # the exact same result.
        index2 = faiss.Index2Layer(coarse_quantizer, 32, 8)
        if True:
            index2.train(xt)
        else:
            index2.pq = index.pq
            index2.is_trained = True
        index2.add(xb)
        ref_recons = index.reconstruct_n(0, nb)
        new_recons = index2.reconstruct_n(0, nb)
        self.assertTrue(np.all(ref_recons == new_recons))


    def test_IMI(self):
        d = 32
        nb = 1000
        nt = 1500
        nq = 200

        (xt, xb, xq) = get_dataset_2(d, nb, nt, nq)
        d = xt.shape[1]

        gt_index = faiss.IndexFlatL2(d)
        gt_index.add(xb)
        D, gt_nns = gt_index.search(xq, 1)

        nbits = 5
        coarse_quantizer = faiss.MultiIndexQuantizer(d, 2, nbits)
        index = faiss.IndexIVFPQ(coarse_quantizer, d, (1 << nbits) ** 2, 8, 8)
        index.quantizer_trains_alone = 1
        index.train(xt)
        index.add(xb)
        index.nprobe = 100
        D, nns = index.search(xq, 10)
        n_ok = (nns == gt_nns).sum()

        # Should return 166 on mac, and 170 on linux.
        self.assertGreater(n_ok, 165)

        ############# replace with explicit assignment indexes
        nbits = 5
        pq = coarse_quantizer.pq
        centroids = faiss.vector_to_array(pq.centroids)
        centroids = centroids.reshape(pq.M, pq.ksub, pq.dsub)
        ai0 = faiss.IndexFlatL2(pq.dsub)
        ai0.add(centroids[0])
        ai1 = faiss.IndexFlatL2(pq.dsub)
        ai1.add(centroids[1])

        coarse_quantizer_2 = faiss.MultiIndexQuantizer2(d, nbits, ai0, ai1)
        coarse_quantizer_2.pq = pq
        coarse_quantizer_2.is_trained = True

        index.quantizer = coarse_quantizer_2

        index.reset()
        index.add(xb)

        D, nns = index.search(xq, 10)
        n_ok = (nns == gt_nns).sum()

        # should return the same result
        self.assertGreater(n_ok, 165)


    def test_IMI_2(self):
        d = 32
        nb = 1000
        nt = 1500
        nq = 200

        (xt, xb, xq) = get_dataset_2(d, nb, nt, nq)
        d = xt.shape[1]

        gt_index = faiss.IndexFlatL2(d)
        gt_index.add(xb)
        D, gt_nns = gt_index.search(xq, 1)

        ############# redo including training
        nbits = 5
        ai0 = faiss.IndexFlatL2(int(d / 2))
        ai1 = faiss.IndexFlatL2(int(d / 2))

        coarse_quantizer = faiss.MultiIndexQuantizer2(d, nbits, ai0, ai1)
        index = faiss.IndexIVFPQ(coarse_quantizer, d, (1 << nbits) ** 2, 8, 8)
        index.quantizer_trains_alone = 1
        index.train(xt)
        index.add(xb)
        index.nprobe = 100
        D, nns = index.search(xq, 10)
        n_ok = (nns == gt_nns).sum()

        # should return the same result
        self.assertGreater(n_ok, 165)





class TestMultiIndexQuantizer(unittest.TestCase):

    def test_search_k1(self):

        # verify codepath for k = 1 and k > 1

        d = 64
        nb = 0
        nt = 1500
        nq = 200

        (xt, xb, xq) = get_dataset(d, nb, nt, nq)

        miq = faiss.MultiIndexQuantizer(d, 2, 6)

        miq.train(xt)

        D1, I1 = miq.search(xq, 1)

        D5, I5 = miq.search(xq, 5)

        self.assertEqual(np.abs(I1[:, :1] - I5[:, :1]).max(), 0)
        self.assertEqual(np.abs(D1[:, :1] - D5[:, :1]).max(), 0)


class TestScalarQuantizer(unittest.TestCase):

    def test_4variants_ivf(self):
        d = 32
        nt = 2500
        nq = 400
        nb = 5000

        (xt, xb, xq) = get_dataset_2(d, nb, nt, nq)

        # common quantizer
        quantizer = faiss.IndexFlatL2(d)

        ncent = 64

        index_gt = faiss.IndexFlatL2(d)
        index_gt.add(xb)
        D, I_ref = index_gt.search(xq, 10)

        nok = {}

        index = faiss.IndexIVFFlat(quantizer, d, ncent,
                                   faiss.METRIC_L2)
        index.cp.min_points_per_centroid = 5    # quiet warning
        index.nprobe = 4
        index.train(xt)
        index.add(xb)
        D, I = index.search(xq, 10)
        nok['flat'] = (I[:, 0] == I_ref[:, 0]).sum()

        for qname in "QT_4bit QT_4bit_uniform QT_8bit QT_8bit_uniform QT_fp16".split():
            qtype = getattr(faiss.ScalarQuantizer, qname)
            index = faiss.IndexIVFScalarQuantizer(quantizer, d, ncent,
                                                  qtype, faiss.METRIC_L2)

            index.nprobe = 4
            index.train(xt)
            index.add(xb)
            D, I = index.search(xq, 10)

            nok[qname] = (I[:, 0] == I_ref[:, 0]).sum()
        print(nok, nq)

        self.assertGreaterEqual(nok['flat'], nq * 0.6)
        # The tests below are a bit fragile, it happens that the
        # ordering between uniform and non-uniform are reverted,
        # probably because the dataset is small, which introduces
        # jitter
        self.assertGreaterEqual(nok['flat'], nok['QT_8bit'])
        self.assertGreaterEqual(nok['QT_8bit'], nok['QT_4bit'])
        self.assertGreaterEqual(nok['QT_8bit'], nok['QT_8bit_uniform'])
        self.assertGreaterEqual(nok['QT_4bit'], nok['QT_4bit_uniform'])
        self.assertGreaterEqual(nok['QT_fp16'], nok['QT_8bit'])

    def test_4variants(self):
        d = 32
        nt = 2500
        nq = 400
        nb = 5000

        (xt, xb, xq) = get_dataset(d, nb, nt, nq)

        index_gt = faiss.IndexFlatL2(d)
        index_gt.add(xb)
        D_ref, I_ref = index_gt.search(xq, 10)

        nok = {}

        for qname in "QT_4bit QT_4bit_uniform QT_8bit QT_8bit_uniform QT_fp16".split():
            qtype = getattr(faiss.ScalarQuantizer, qname)
            index = faiss.IndexScalarQuantizer(d, qtype, faiss.METRIC_L2)
            index.train(xt)
            index.add(xb)
            D, I = index.search(xq, 10)
            nok[qname] = (I[:, 0] == I_ref[:, 0]).sum()

        print(nok, nq)

        self.assertGreaterEqual(nok['QT_8bit'], nq * 0.9)
        self.assertGreaterEqual(nok['QT_8bit'], nok['QT_4bit'])
        self.assertGreaterEqual(nok['QT_8bit'], nok['QT_8bit_uniform'])
        self.assertGreaterEqual(nok['QT_4bit'], nok['QT_4bit_uniform'])
        self.assertGreaterEqual(nok['QT_fp16'], nok['QT_8bit'])


class TestRangeSearch(unittest.TestCase):

    def test_range_search(self):
        d = 4
        nt = 100
        nq = 10
        nb = 50

        (xt, xb, xq) = get_dataset(d, nb, nt, nq)

        index = faiss.IndexFlatL2(d)
        index.add(xb)

        Dref, Iref = index.search(xq, 5)

        thresh = 0.1   # *squared* distance
        lims, D, I = index.range_search(xq, thresh)

        for i in range(nq):
            Iline = I[lims[i]:lims[i + 1]]
            Dline = D[lims[i]:lims[i + 1]]
            for j, dis in zip(Iref[i], Dref[i]):
                if dis < thresh:
                    li, = np.where(Iline == j)
                    self.assertTrue(li.size == 1)
                    idx = li[0]
                    self.assertGreaterEqual(1e-4, abs(Dline[idx] - dis))


class TestSearchAndReconstruct(unittest.TestCase):

    def run_search_and_reconstruct(self, index, xb, xq, k=10, eps=None):
        n, d = xb.shape
        assert xq.shape[1] == d
        assert index.d == d

        D_ref, I_ref = index.search(xq, k)
        R_ref = index.reconstruct_n(0, n)
        D, I, R = index.search_and_reconstruct(xq, k)

        self.assertTrue((D == D_ref).all())
        self.assertTrue((I == I_ref).all())
        self.assertEqual(R.shape[:2], I.shape)
        self.assertEqual(R.shape[2], d)

        # (n, k, ..) -> (n * k, ..)
        I_flat = I.reshape(-1)
        R_flat = R.reshape(-1, d)
        # Filter out -1s when not enough results
        R_flat = R_flat[I_flat >= 0]
        I_flat = I_flat[I_flat >= 0]

        recons_ref_err = np.mean(np.linalg.norm(R_flat - R_ref[I_flat]))
        self.assertLessEqual(recons_ref_err, 1e-6)

        def norm1(x):
            return np.sqrt((x ** 2).sum(axis=1))

        recons_err = np.mean(norm1(R_flat - xb[I_flat]))

        print('Reconstruction error = %.3f' % recons_err)
        if eps is not None:
            self.assertLessEqual(recons_err, eps)

        return D, I, R

    def test_IndexFlat(self):
        d = 32
        nb = 1000
        nt = 1500
        nq = 200

        (xt, xb, xq) = get_dataset(d, nb, nt, nq)

        index = faiss.IndexFlatL2(d)
        index.add(xb)

        self.run_search_and_reconstruct(index, xb, xq, eps=0.0)

    def test_IndexIVFFlat(self):
        d = 32
        nb = 1000
        nt = 1500
        nq = 200

        (xt, xb, xq) = get_dataset(d, nb, nt, nq)

        quantizer = faiss.IndexFlatL2(d)
        index = faiss.IndexIVFFlat(quantizer, d, 32, faiss.METRIC_L2)
        index.cp.min_points_per_centroid = 5    # quiet warning
        index.nprobe = 4
        index.train(xt)
        index.add(xb)

        self.run_search_and_reconstruct(index, xb, xq, eps=0.0)

    def test_IndexIVFPQ(self):
        d = 32
        nb = 1000
        nt = 1500
        nq = 200

        (xt, xb, xq) = get_dataset(d, nb, nt, nq)

        quantizer = faiss.IndexFlatL2(d)
        index = faiss.IndexIVFPQ(quantizer, d, 32, 8, 8)
        index.cp.min_points_per_centroid = 5    # quiet warning
        index.nprobe = 4
        index.train(xt)
        index.add(xb)

        self.run_search_and_reconstruct(index, xb, xq, eps=1.0)

    def test_MultiIndex(self):
        d = 32
        nb = 1000
        nt = 1500
        nq = 200

        (xt, xb, xq) = get_dataset(d, nb, nt, nq)

        index = faiss.index_factory(d, "IMI2x5,PQ8np")
        faiss.ParameterSpace().set_index_parameter(index, "nprobe", 4)
        index.train(xt)
        index.add(xb)

        self.run_search_and_reconstruct(index, xb, xq, eps=1.0)

    def test_IndexTransform(self):
        d = 32
        nb = 1000
        nt = 1500
        nq = 200

        (xt, xb, xq) = get_dataset(d, nb, nt, nq)

        index = faiss.index_factory(d, "L2norm,PCA8,IVF32,PQ8np")
        faiss.ParameterSpace().set_index_parameter(index, "nprobe", 4)
        index.train(xt)
        index.add(xb)

        self.run_search_and_reconstruct(index, xb, xq)


class TestHNSW(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        d = 32
        nt = 0
        nb = 1500
        nq = 500

        (_, self.xb, self.xq) = get_dataset_2(d, nb, nt, nq)
        index = faiss.IndexFlatL2(d)
        index.add(self.xb)
        Dref, Iref = index.search(self.xq, 1)
        self.Iref = Iref

    def test_hnsw(self):
        d = self.xq.shape[1]

        index = faiss.IndexHNSWFlat(d, 16)
        index.add(self.xb)
        Dhnsw, Ihnsw = index.search(self.xq, 1)

        self.assertGreaterEqual((self.Iref == Ihnsw).sum(), 460)

        self.io_and_retest(index, Dhnsw, Ihnsw)

    def test_hnsw_unbounded_queue(self):
        d = self.xq.shape[1]

        index = faiss.IndexHNSWFlat(d, 16)
        index.add(self.xb)
        index.search_bounded_queue = False
        Dhnsw, Ihnsw = index.search(self.xq, 1)

        self.assertGreaterEqual((self.Iref == Ihnsw).sum(), 460)

        self.io_and_retest(index, Dhnsw, Ihnsw)

    def io_and_retest(self, index, Dhnsw, Ihnsw):
        _, tmpfile = tempfile.mkstemp()
        try:
            faiss.write_index(index, tmpfile)
            index2 = faiss.read_index(tmpfile)
        finally:
            if os.path.exists(tmpfile):
                os.unlink(tmpfile)

        Dhnsw2, Ihnsw2 = index2.search(self.xq, 1)

        self.assertTrue(np.all(Dhnsw2 == Dhnsw))
        self.assertTrue(np.all(Ihnsw2 == Ihnsw))

    def test_hnsw_2level(self):
        d = self.xq.shape[1]

        quant = faiss.IndexFlatL2(d)

        index = faiss.IndexHNSW2Level(quant, 256, 8, 8)
        index.train(self.xb)
        index.add(self.xb)
        Dhnsw, Ihnsw = index.search(self.xq, 1)

        self.assertGreaterEqual((self.Iref == Ihnsw).sum(), 310)

        self.io_and_retest(index, Dhnsw, Ihnsw)

    def test_add_0_vecs(self):
        index = faiss.IndexHNSWFlat(10, 16)
        zero_vecs = np.zeros((0, 10), dtype='float32')
        # infinite loop
        index.add(zero_vecs)


class TestIOError(unittest.TestCase):

    def test_io_error(self):
        d, n = 32, 1000
        x = np.random.uniform(size=(n, d)).astype('float32')
        index = faiss.IndexFlatL2(d)
        index.add(x)
        _, fname = tempfile.mkstemp()
        try:
            faiss.write_index(index, fname)

            # should be fine
            faiss.read_index(fname)

            # now damage file
            data = open(fname, 'rb').read()
            data = data[:int(len(data) / 2)]
            open(fname, 'wb').write(data)

            # should make a nice readable exception that mentions the
            try:
                faiss.read_index(fname)
            except RuntimeError as e:
                if fname not in str(e):
                    raise
            else:
                raise

        finally:
            if os.path.exists(fname):
                os.unlink(fname)


class TestDistancesPositive(unittest.TestCase):

    def test_l2_pos(self):
        """
        roundoff errors occur only with the L2 decomposition used
        with BLAS, ie. in IndexFlatL2 and with
        n > distance_compute_blas_threshold = 20
        """

        d = 128
        n = 100

        rs = np.random.RandomState(1234)
        x = rs.rand(n, d).astype('float32')

        index = faiss.IndexFlatL2(d)
        index.add(x)

        D, I = index.search(x, 10)

        assert np.all(D >= 0)


class TestReconsException(unittest.TestCase):

    def test_recons(self):

        d = 64                           # dimension
        nb = 1000
        rs = np.random.RandomState(1234)
        xb = rs.rand(nb, d).astype('float32')
        nlist = 10
        quantizer = faiss.IndexFlatL2(d)  # the other index
        index = faiss.IndexIVFFlat(quantizer, d, nlist)
        index.train(xb)
        index.add(xb)
        index.make_direct_map()

        index.reconstruct(9)

        try:
            index.reconstruct(100001)
        except RuntimeError:
            pass
        else:
            assert False, "should raise an exception"



if __name__ == '__main__':
    unittest.main()
