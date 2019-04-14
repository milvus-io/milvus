# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

# a few common functions for the tests

import numpy as np
import faiss

# reduce number of threads to avoid excessive nb of threads in opt
# mode (recuces runtime from 100s to 4s!)
faiss.omp_set_num_threads(4)


def random_unitary(n, d, seed):
    x = faiss.randn(n * d, seed).reshape(n, d)
    faiss.normalize_L2(x)
    return x


class Randu10k:

    def __init__(self):
        self.nb = 10000
        self.nq = 1000
        self.nt = 10000
        self.d = 128

        self.xb = random_unitary(self.nb, self.d, 1)
        self.xt = random_unitary(self.nt, self.d, 2)
        self.xq = random_unitary(self.nq, self.d, 3)

        dotprods = np.dot(self.xq, self.xb.T)
        self.gt = dotprods.argmax(1)
        self.k = 100

    def launch(self, name, index):
        if not index.is_trained:
            index.train(self.xt)
        index.add(self.xb)
        return index.search(self.xq, self.k)

    def evalres(self, DI):
        D, I = DI
        e = {}
        for rank in 1, 10, 100:
            e[rank] = ((I[:, :rank] == self.gt.reshape(-1, 1)).sum() /
                       float(self.nq))
        print("1-recalls: %s" % e)
        return e


class Randu10kUnbalanced(Randu10k):

    def __init__(self):
        Randu10k.__init__(self)

        weights = 0.95 ** np.arange(self.d)
        rs = np.random.RandomState(123)
        weights = weights[rs.permutation(self.d)]
        self.xb *= weights
        self.xq *= weights
        self.xt *= weights

        dotprods = np.dot(self.xq, self.xb.T)
        self.gt = dotprods.argmax(1)
        self.k = 100


def get_dataset(d, nb, nt, nq):
    rs = np.random.RandomState(123)
    xb = rs.rand(nb, d).astype('float32')
    xt = rs.rand(nt, d).astype('float32')
    xq = rs.rand(nq, d).astype('float32')

    return (xt, xb, xq)


def get_dataset_2(d, nb, nt, nq):
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
