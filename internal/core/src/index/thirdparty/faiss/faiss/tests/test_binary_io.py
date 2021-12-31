# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Binary indexes (de)serialization"""

from __future__ import absolute_import, division, print_function, unicode_literals

import numpy as np
import unittest
import faiss
import os
import tempfile

def make_binary_dataset(d, nb, nt, nq):
    assert d % 8 == 0
    x = np.random.randint(256, size=(nb + nq + nt, int(d / 8))).astype('uint8')
    return x[:nt], x[nt:-nq], x[-nq:]


class TestBinaryFlat(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        d = 32
        nt = 0
        nb = 1500
        nq = 500

        (_, self.xb, self.xq) = make_binary_dataset(d, nb, nt, nq)

    def test_flat(self):
        d = self.xq.shape[1] * 8

        index = faiss.IndexBinaryFlat(d)
        index.add(self.xb)
        D, I = index.search(self.xq, 3)

        _, tmpnam = tempfile.mkstemp()
        try:
            faiss.write_index_binary(index, tmpnam)

            index2 = faiss.read_index_binary(tmpnam)

            D2, I2 = index2.search(self.xq, 3)

            assert (I2 == I).all()
            assert (D2 == D).all()

        finally:
            os.remove(tmpnam)


class TestBinaryIVF(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        d = 32
        nt = 200
        nb = 1500
        nq = 500

        (self.xt, self.xb, self.xq) = make_binary_dataset(d, nb, nt, nq)

    def test_ivf_flat(self):
        d = self.xq.shape[1] * 8

        quantizer = faiss.IndexBinaryFlat(d)
        index = faiss.IndexBinaryIVF(quantizer, d, 8)
        index.cp.min_points_per_centroid = 5    # quiet warning
        index.nprobe = 4
        index.train(self.xt)
        index.add(self.xb)
        D, I = index.search(self.xq, 3)

        _, tmpnam = tempfile.mkstemp()

        try:
            faiss.write_index_binary(index, tmpnam)

            index2 = faiss.read_index_binary(tmpnam)

            D2, I2 = index2.search(self.xq, 3)

            assert (I2 == I).all()
            assert (D2 == D).all()

        finally:
            os.remove(tmpnam)


class TestObjectOwnership(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        d = 32
        nt = 200
        nb = 1500
        nq = 500

        (self.xt, self.xb, self.xq) = make_binary_dataset(d, nb, nt, nq)

    def test_read_index_ownership(self):
        d = self.xq.shape[1] * 8

        index = faiss.IndexBinaryFlat(d)
        index.add(self.xb)

        _, tmpnam = tempfile.mkstemp()
        try:
            faiss.write_index_binary(index, tmpnam)

            index2 = faiss.read_index_binary(tmpnam)

            assert index2.thisown
        finally:
            os.remove(tmpnam)


class TestBinaryFromFloat(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        d = 32
        nt = 200
        nb = 1500
        nq = 500

        (self.xt, self.xb, self.xq) = make_binary_dataset(d, nb, nt, nq)

    def test_binary_from_float(self):
        d = self.xq.shape[1] * 8

        float_index = faiss.IndexHNSWFlat(d, 16)
        index = faiss.IndexBinaryFromFloat(float_index)
        index.add(self.xb)
        D, I = index.search(self.xq, 3)

        _, tmpnam = tempfile.mkstemp()

        try:
            faiss.write_index_binary(index, tmpnam)

            index2 = faiss.read_index_binary(tmpnam)

            D2, I2 = index2.search(self.xq, 3)

            assert (I2 == I).all()
            assert (D2 == D).all()

        finally:
            os.remove(tmpnam)


class TestBinaryHNSW(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        d = 32
        nt = 200
        nb = 1500
        nq = 500

        (self.xt, self.xb, self.xq) = make_binary_dataset(d, nb, nt, nq)

    def test_hnsw(self):
        d = self.xq.shape[1] * 8

        index = faiss.IndexBinaryHNSW(d)
        index.add(self.xb)
        D, I = index.search(self.xq, 3)

        _, tmpnam = tempfile.mkstemp()

        try:
            faiss.write_index_binary(index, tmpnam)

            index2 = faiss.read_index_binary(tmpnam)

            D2, I2 = index2.search(self.xq, 3)

            assert (I2 == I).all()
            assert (D2 == D).all()

        finally:
            os.remove(tmpnam)

    def test_ivf_hnsw(self):
        d = self.xq.shape[1] * 8

        quantizer = faiss.IndexBinaryHNSW(d)
        index = faiss.IndexBinaryIVF(quantizer, d, 8)
        index.cp.min_points_per_centroid = 5    # quiet warning
        index.nprobe = 4
        index.train(self.xt)
        index.add(self.xb)
        D, I = index.search(self.xq, 3)

        _, tmpnam = tempfile.mkstemp()

        try:
            faiss.write_index_binary(index, tmpnam)

            index2 = faiss.read_index_binary(tmpnam)

            D2, I2 = index2.search(self.xq, 3)

            assert (I2 == I).all()
            assert (D2 == D).all()

        finally:
            os.remove(tmpnam)


if __name__ == '__main__':
    unittest.main()
