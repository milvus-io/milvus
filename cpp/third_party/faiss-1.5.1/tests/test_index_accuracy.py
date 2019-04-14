# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

# translation of test_knn.lua

import numpy as np
import unittest
import faiss

from common import Randu10k, get_dataset_2, Randu10kUnbalanced

ev = Randu10k()

d = ev.d

# Parameters inverted indexes
ncentroids = int(4 * np.sqrt(ev.nb))
kprobe = int(np.sqrt(ncentroids))

# Parameters for LSH
nbits = d

# Parameters for indexes involving PQ
M = int(d / 8)           # for PQ: #subquantizers
nbits_per_index = 8      # for PQ


class IndexAccuracy(unittest.TestCase):

    def test_IndexFlatIP(self):
        q = faiss.IndexFlatIP(d)  # Ask inner product
        res = ev.launch('FLAT / IP', q)
        e = ev.evalres(res)
        assert e[1] == 1.0

    def test_IndexFlatL2(self):
        q = faiss.IndexFlatL2(d)
        res = ev.launch('FLAT / L2', q)
        e = ev.evalres(res)
        assert e[1] == 1.0

    def test_ivf_kmeans(self):
        ivfk = faiss.IndexIVFFlat(faiss.IndexFlatL2(d), d, ncentroids)
        ivfk.nprobe = kprobe
        res = ev.launch('IVF K-means', ivfk)
        e = ev.evalres(res)
        # should give 0.260  0.260  0.260
        assert e[1] > 0.2

    def test_indexLSH(self):
        q = faiss.IndexLSH(d, nbits)
        res = ev.launch('FLAT / LSH Cosine', q)
        e = ev.evalres(res)
        # should give 0.070  0.250  0.580
        assert e[10] > 0.2

    def test_IndexLSH_32_48(self):
        # CHECK: the difference between 32 and 48 does not make much sense
        for nbits2 in 32, 48:
            q = faiss.IndexLSH(d, nbits2)
            res = ev.launch('LSH half size', q)
            e = ev.evalres(res)
            # should give 0.003  0.019  0.108
            assert e[10] > 0.018

    def test_IndexPQ(self):
        q = faiss.IndexPQ(d, M, nbits_per_index)
        res = ev.launch('FLAT / PQ L2', q)
        e = ev.evalres(res)
        # should give 0.070  0.230  0.260
        assert e[10] > 0.2

    # Approximate search module: PQ with inner product distance
    def test_IndexPQ_ip(self):
        q = faiss.IndexPQ(d, M, nbits_per_index, faiss.METRIC_INNER_PRODUCT)
        res = ev.launch('FLAT / PQ IP', q)
        e = ev.evalres(res)
        # should give 0.070  0.230  0.260
        #(same result as regular PQ on normalized distances)
        assert e[10] > 0.2

    def test_IndexIVFPQ(self):
        ivfpq = faiss.IndexIVFPQ(faiss.IndexFlatL2(d), d, ncentroids, M, 8)
        ivfpq.nprobe = kprobe
        res = ev.launch('IVF PQ', ivfpq)
        e = ev.evalres(res)
        # should give 0.070  0.230  0.260
        assert e[10] > 0.2

    # TODO: translate evaluation of nested

    # Approximate search: PQ with full vector refinement
    def test_IndexPQ_refined(self):
        q = faiss.IndexPQ(d, M, nbits_per_index)
        res = ev.launch('PQ non-refined', q)
        e = ev.evalres(res)
        q.reset()

        rq = faiss.IndexRefineFlat(q)
        res = ev.launch('PQ refined', rq)
        e2 = ev.evalres(res)
        assert e2[10] >= e[10]
        rq.k_factor = 4

        res = ev.launch('PQ refined*4', rq)
        e3 = ev.evalres(res)
        assert e3[10] >= e2[10]

    def test_polysemous(self):
        index = faiss.IndexPQ(d, M, nbits_per_index)
        index.do_polysemous_training = True
        # reduce nb iterations to speed up training for the test
        index.polysemous_training.n_iter = 50000
        index.polysemous_training.n_redo = 1
        res = ev.launch('normal PQ', index)
        e_baseline = ev.evalres(res)
        index.search_type = faiss.IndexPQ.ST_polysemous

        index.polysemous_ht = int(M / 16. * 58)

        stats = faiss.cvar.indexPQ_stats
        stats.reset()

        res = ev.launch('Polysemous ht=%d' % index.polysemous_ht,
                        index)
        e_polysemous = ev.evalres(res)
        print(e_baseline, e_polysemous, index.polysemous_ht)
        print(stats.n_hamming_pass, stats.ncode)
        # The randu dataset is difficult, so we are not too picky on
        # the results. Here we assert that we have < 10 % loss when
        # computing full PQ on fewer than 20% of the data.
        assert stats.n_hamming_pass < stats.ncode / 5
        # Test disabled because difference is 0.17 on aarch64
        # TODO check why???
        # assert e_polysemous[10] > e_baseline[10] - 0.1

    def test_ScalarQuantizer(self):
        quantizer = faiss.IndexFlatL2(d)
        ivfpq = faiss.IndexIVFScalarQuantizer(
            quantizer, d, ncentroids,
            faiss.ScalarQuantizer.QT_8bit)
        ivfpq.nprobe = kprobe
        res = ev.launch('IVF SQ', ivfpq)
        e = ev.evalres(res)
        # should give 0.234  0.236  0.236
        assert e[10] > 0.235



class TestSQFlavors(unittest.TestCase):
    """ tests IP in addition to L2, non multiple of 8 dimensions
    """

    def add2columns(self, x):
        return np.hstack((
            x, np.zeros((x.shape[0], 2), dtype='float32')
        ))

    def subtest_add2col(self, xb, xq, index, qname):
        """Test with 2 additional dimensions to take also the non-SIMD
        codepath. We don't retrain anything but add 2 dims to the
        queries, the centroids and the trained ScalarQuantizer.
        """
        nb, d = xb.shape

        d2 = d + 2
        xb2 = self.add2columns(xb)
        xq2 = self.add2columns(xq)

        nlist = index.nlist
        quantizer = faiss.downcast_index(index.quantizer)
        quantizer2 = faiss.IndexFlat(d2, index.metric_type)
        centroids = faiss.vector_to_array(quantizer.xb).reshape(nlist, d)
        centroids2 = self.add2columns(centroids)
        quantizer2.add(centroids2)
        index2 = faiss.IndexIVFScalarQuantizer(
            quantizer2, d2, index.nlist, index.sq.qtype,
            index.metric_type)
        index2.nprobe = 4
        if qname in ('8bit', '4bit'):
            trained = faiss.vector_to_array(index.sq.trained).reshape(2, -1)
            nt = trained.shape[1]
            # 2 lines: vmins and vdiffs
            new_nt = int(nt * d2 / d)
            trained2 = np.hstack((
                trained,
                np.zeros((2, new_nt - nt), dtype='float32')
            ))
            trained2[1, nt:] = 1.0   # set vdiff to 1 to avoid div by 0
            faiss.copy_array_to_vector(trained2.ravel(), index2.sq.trained)
        else:
            index2.sq.trained = index.sq.trained

        index2.is_trained = True
        index2.add(xb2)
        return index2.search(xq2, 10)

    # run on Sept 6, 2018 with nprobe=1
    ref_results_xx = {
        (1, '8bit'): 387,
        (1, '4bit'): 216,
        (1, '8bit_uniform'): 387,
        (1, '4bit_uniform'): 216,
        (1, 'fp16'): 387,
        (0, '8bit'): 364,
        (0, '4bit'): 187,
        (0, '8bit_uniform'): 364,
        (0, '4bit_uniform'): 186,
        (0, 'fp16'): 364,
    }

    # run on Sept 18, 2018 with nprobe=4 + 4 bit bugfix
    ref_results = {
        (0, '8bit'): 984,
        (0, '4bit'): 978,
        (0, '8bit_uniform'): 985,
        (0, '4bit_uniform'): 979,
        (0, 'fp16'): 985,
        (1, '8bit'): 979,
        (1, '4bit'): 973,
        (1, '8bit_uniform'): 979,
        (1, '4bit_uniform'): 972,
        (1, 'fp16'): 979,
    }


    def subtest(self, mt):
        d = 32
        xt, xb, xq = get_dataset_2(d, 1000, 2000, 200)
        nlist = 64

        gt_index = faiss.IndexFlat(d, mt)
        gt_index.add(xb)
        gt_D, gt_I = gt_index.search(xq, 10)
        quantizer = faiss.IndexFlat(d, mt)
        for qname in '8bit 4bit 8bit_uniform 4bit_uniform fp16'.split():
            qtype = getattr(faiss.ScalarQuantizer, 'QT_' + qname)
            index = faiss.IndexIVFScalarQuantizer(
                quantizer, d, nlist, qtype, mt)
            index.train(xt)
            index.add(xb)
            index.nprobe = 4   # hopefully more robust than 1
            D, I = index.search(xq, 10)
            ninter = faiss.eval_intersection(I, gt_I)
            print('(%d, %s): %d, ' % (mt, repr(qname), ninter))
            assert abs(ninter - self.ref_results[(mt, qname)]) <= 9

            D2, I2 = self.subtest_add2col(xb, xq, index, qname)

            assert np.all(I2 == I)

            # also test range search

            if mt == faiss.METRIC_INNER_PRODUCT:
                radius = float(D[:, -1].max())
            else:
                radius = float(D[:, -1].min())
            print('radius', radius)

            lims, D3, I3 = index.range_search(xq, radius)
            ntot = ndiff = 0
            for i in range(len(xq)):
                l0, l1 = lims[i], lims[i + 1]
                Inew = set(I3[l0:l1])
                if mt == faiss.METRIC_INNER_PRODUCT:
                    mask = D2[i] > radius
                else:
                    mask = D2[i] < radius
                Iref = set(I2[i, mask])
                ndiff += len(Inew ^ Iref)
                ntot += len(Iref)
            print('ndiff %d / %d' % (ndiff, ntot))
            assert ndiff < ntot * 0.01


    def test_SQ_IP(self):
        self.subtest(faiss.METRIC_INNER_PRODUCT)

    def test_SQ_L2(self):
        self.subtest(faiss.METRIC_L2)


class TestSQByte(unittest.TestCase):

    def subtest_8bit_direct(self, metric_type, d):
        xt, xb, xq = get_dataset_2(d, 1000, 500, 30)

        # rescale everything to get integer
        tmin, tmax = xt.min(), xt.max()

        def rescale(x):
            x = np.floor((x - tmin) * 256 / (tmax - tmin))
            x[x < 0] = 0
            x[x > 255] = 255
            return x

        xt = rescale(xt)
        xb = rescale(xb)
        xq = rescale(xq)

        gt_index = faiss.IndexFlat(d, metric_type)
        gt_index.add(xb)
        Dref, Iref = gt_index.search(xq, 10)

        index = faiss.IndexScalarQuantizer(
            d, faiss.ScalarQuantizer.QT_8bit_direct, metric_type)
        index.add(xb)
        D, I = index.search(xq, 10)

        assert np.all(I == Iref)
        assert np.all(D == Dref)

        # same, with IVF

        nlist = 64
        quantizer = faiss.IndexFlat(d, metric_type)

        gt_index = faiss.IndexIVFFlat(quantizer, d, nlist, metric_type)
        gt_index.nprobe = 4
        gt_index.train(xt)
        gt_index.add(xb)
        Dref, Iref = gt_index.search(xq, 10)

        index = faiss.IndexIVFScalarQuantizer(
            quantizer, d, nlist,
            faiss.ScalarQuantizer.QT_8bit_direct, metric_type)
        index.nprobe = 4
        index.by_residual = False
        index.train(xt)
        index.add(xb)
        D, I = index.search(xq, 10)

        assert np.all(I == Iref)
        assert np.all(D == Dref)

    def test_8bit_direct(self):
        for d in 13, 16, 24:
            for metric_type in faiss.METRIC_L2, faiss.METRIC_INNER_PRODUCT:
                self.subtest_8bit_direct(metric_type, d)



class TestPQFlavors(unittest.TestCase):

    # run on Dec 14, 2018
    ref_results = {
        (1, True): 800,
        (1, True, 20): 794,
        (1, False): 769,
        (0, True): 831,
        (0, True, 20): 828,
        (0, False): 829,
    }

    def test_IVFPQ_IP(self):
        self.subtest(faiss.METRIC_INNER_PRODUCT)

    def test_IVFPQ_L2(self):
        self.subtest(faiss.METRIC_L2)

    def subtest(self, mt):
        d = 32
        xt, xb, xq = get_dataset_2(d, 1000, 2000, 200)
        nlist = 64

        gt_index = faiss.IndexFlat(d, mt)
        gt_index.add(xb)
        gt_D, gt_I = gt_index.search(xq, 10)
        quantizer = faiss.IndexFlat(d, mt)
        for by_residual in True, False:

            index = faiss.IndexIVFPQ(
                quantizer, d, nlist, 4, 8)
            index.metric_type = mt
            index.by_residual = by_residual
            if by_residual:
                # perform cheap polysemous training
                index.do_polysemous_training = True
                pt = faiss.PolysemousTraining()
                pt.n_iter = 50000
                pt.n_redo = 1
                index.polysemous_training = pt

            index.train(xt)
            index.add(xb)
            index.nprobe = 4
            D, I = index.search(xq, 10)

            ninter = faiss.eval_intersection(I, gt_I)
            print('(%d, %s): %d, ' % (mt, by_residual, ninter))

            assert abs(ninter - self.ref_results[mt, by_residual]) <= 3

            index.use_precomputed_table = 0
            D2, I2 = index.search(xq, 10)
            assert np.all(I == I2)

            if by_residual:

                index.use_precomputed_table = 1
                index.polysemous_ht = 20
                D, I = index.search(xq, 10)
                ninter = faiss.eval_intersection(I, gt_I)
                print('(%d, %s, %d): %d, ' % (
                    mt, by_residual, index.polysemous_ht, ninter))

                # polysemous behaves bizarrely on ARM
                assert (ninter >= self.ref_results[
                    mt, by_residual, index.polysemous_ht] - 4)


class TestFlat1D(unittest.TestCase):

    def test_flat_1d(self):
        rs = np.random.RandomState(123545)
        k = 10
        xb = rs.uniform(size=(100, 1)).astype('float32')
        # make sure to test below and above
        xq = rs.uniform(size=(1000, 1)).astype('float32') * 1.1 - 0.05

        ref = faiss.IndexFlatL2(1)
        ref.add(xb)
        ref_D, ref_I = ref.search(xq, k)

        new = faiss.IndexFlat1D()
        new.add(xb)

        new_D, new_I = new.search(xq, 10)

        ndiff = (np.abs(ref_I - new_I) != 0).sum()

        assert(ndiff < 100)
        new_D = new_D ** 2
        max_diff_D = np.abs(ref_D - new_D).max()
        assert(max_diff_D < 1e-5)


class OPQRelativeAccuracy(unittest.TestCase):
    # translated from test_opq.lua

    def test_OPQ(self):

        M = 4

        ev = Randu10kUnbalanced()
        d = ev.d
        index = faiss.IndexPQ(d, M, 8)

        res = ev.launch('PQ', index)
        e_pq = ev.evalres(res)

        index_pq = faiss.IndexPQ(d, M, 8)
        opq_matrix = faiss.OPQMatrix(d, M)
        # opq_matrix.verbose = true
        opq_matrix.niter = 10
        opq_matrix.niter_pq = 4
        index = faiss.IndexPreTransform(opq_matrix, index_pq)

        res = ev.launch('OPQ', index)
        e_opq = ev.evalres(res)

        print('e_pq=%s' % e_pq)
        print('e_opq=%s' % e_opq)

        # verify that OPQ better than PQ
        assert(e_opq[10] > e_pq[10])

    def test_OIVFPQ(self):
        # Parameters inverted indexes
        ncentroids = 50
        M = 4

        ev = Randu10kUnbalanced()
        d = ev.d
        quantizer = faiss.IndexFlatL2(d)
        index = faiss.IndexIVFPQ(quantizer, d, ncentroids, M, 8)
        index.nprobe = 5

        res = ev.launch('IVFPQ', index)
        e_ivfpq = ev.evalres(res)

        index_ivfpq = faiss.IndexIVFPQ(quantizer, d, ncentroids, M, 8)
        index_ivfpq.nprobe = 5
        opq_matrix = faiss.OPQMatrix(d, M)
        opq_matrix.niter = 10
        index = faiss.IndexPreTransform(opq_matrix, index_ivfpq)

        res = ev.launch('O+IVFPQ', index)
        e_oivfpq = ev.evalres(res)

        # TODO(beauby): Fix and re-enable.
        # verify same on OIVFPQ
        # assert(e_oivfpq[1] > e_ivfpq[1])


class TestRoundoff(unittest.TestCase):

    def test_roundoff(self):
        # params that force use of BLAS implementation
        nb = 100
        nq = 25
        d = 4
        xb = np.zeros((nb, d), dtype='float32')

        xb[:, 0] = np.arange(nb) + 12345
        xq = xb[:nq] + 0.3

        index = faiss.IndexFlat(d)
        index.add(xb)

        D, I = index.search(xq, 1)

        # this does not work
        assert not np.all(I.ravel() == np.arange(nq))

        index = faiss.IndexPreTransform(
            faiss.CenteringTransform(d),
            faiss.IndexFlat(d))

        index.train(xb)
        index.add(xb)

        D, I = index.search(xq, 1)

        # this works
        assert np.all(I.ravel() == np.arange(nq))



if __name__ == '__main__':
    unittest.main()
