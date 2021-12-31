# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2
# noqa E741

import numpy as np

import faiss
import unittest

from common import get_dataset_2

import scipy.spatial.distance


class TestExtraDistances(unittest.TestCase):
    """ check wrt. the scipy implementation """

    def make_example(self):
        rs = np.random.RandomState(123)
        x = rs.rand(5, 32).astype('float32')
        y = rs.rand(3, 32).astype('float32')
        return x, y

    def run_simple_dis_test(self, ref_func, metric_type):
        xq, yb = self.make_example()
        ref_dis = np.array([
            [ref_func(x, y) for y in yb]
            for x in xq
        ])
        new_dis = faiss.pairwise_distances(xq, yb, metric_type)
        self.assertTrue(np.allclose(ref_dis, new_dis))

    def test_L1(self):
        self.run_simple_dis_test(scipy.spatial.distance.cityblock,
                                 faiss.METRIC_L1)

    def test_Linf(self):
        self.run_simple_dis_test(scipy.spatial.distance.chebyshev,
                                 faiss.METRIC_Linf)

    def test_L2(self):
        xq, yb = self.make_example()
        ref_dis = np.array([
            [scipy.spatial.distance.sqeuclidean(x, y) for y in yb]
            for x in xq
        ])
        new_dis = faiss.pairwise_distances(xq, yb, faiss.METRIC_L2)
        self.assertTrue(np.allclose(ref_dis, new_dis))

        ref_dis = np.array([
            [scipy.spatial.distance.euclidean(x, y) for y in yb]
            for x in xq
        ])
        new_dis = np.sqrt(new_dis)        # post processing
        self.assertTrue(np.allclose(ref_dis, new_dis))

    def test_Lp(self):
        p = 1.5
        xq, yb = self.make_example()
        ref_dis = np.array([
            [scipy.spatial.distance.minkowski(x, y, p) for y in yb]
            for x in xq
        ])
        new_dis = faiss.pairwise_distances(xq, yb, faiss.METRIC_Lp, p)
        new_dis = new_dis ** (1 / p)     # post processing
        self.assertTrue(np.allclose(ref_dis, new_dis))

    def test_canberra(self):
        self.run_simple_dis_test(scipy.spatial.distance.canberra,
                                 faiss.METRIC_Canberra)

    def test_braycurtis(self):
        self.run_simple_dis_test(scipy.spatial.distance.braycurtis,
                                 faiss.METRIC_BrayCurtis)

    def xx_test_jensenshannon(self):
        # this distance does not seem to be implemented in scipy
        # vectors should probably be L1 normalized
        self.run_simple_dis_test(scipy.spatial.distance.jensenshannon,
                                 faiss.METRIC_JensenShannon)


class TestKNN(unittest.TestCase):
    """ test that the knn search gives the same as distance matrix + argmin """

    def do_test_knn(self, mt):
        d = 10
        nb = 100
        nq = 50
        nt = 0
        xt, xb, xq = get_dataset_2(d, nt, nb, nq)

        index = faiss.IndexFlat(d, mt)
        index.add(xb)

        D, I = index.search(xq, 10)

        dis = faiss.pairwise_distances(xq, xb, mt)
        o = dis.argsort(axis=1)
        assert np.all(I == o[:, :10])

        for q in range(nq):
            assert np.all(D[q] == dis[q, I[q]])

        index2 = faiss.deserialize_index(faiss.serialize_index(index))

        D2, I2 = index2.search(xq, 10)

        self.assertTrue(np.all(I == I2))

    def test_L1(self):
        self.do_test_knn(faiss.METRIC_L1)

    def test_Linf(self):
        self.do_test_knn(faiss.METRIC_Linf)


class TestHNSW(unittest.TestCase):
    """ since it has a distance computer, HNSW should work """

    def test_hnsw(self):

        d = 10
        nb = 1000
        nq = 100
        nt = 0
        xt, xb, xq = get_dataset_2(d, nt, nb, nq)

        mt = faiss.METRIC_L1

        index = faiss.IndexHNSW(faiss.IndexFlat(d, mt))
        index.add(xb)

        D, I = index.search(xq, 10)

        dis = faiss.pairwise_distances(xq, xb, mt)

        for q in range(nq):
            assert np.all(D[q] == dis[q, I[q]])
