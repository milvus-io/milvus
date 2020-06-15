# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import absolute_import, division, print_function

import numpy as np
import unittest
import faiss


class TestFactory(unittest.TestCase):

    def test_factory_1(self):

        index = faiss.index_factory(12, "IVF10,PQ4")
        assert index.do_polysemous_training

        index = faiss.index_factory(12, "IVF10,PQ4np")
        assert not index.do_polysemous_training

        index = faiss.index_factory(12, "PQ4")
        assert index.do_polysemous_training

        index = faiss.index_factory(12, "PQ4np")
        assert not index.do_polysemous_training

        try:
            index = faiss.index_factory(10, "PQ4")
        except RuntimeError:
            pass
        else:
            assert False, "should do a runtime error"

    def test_factory_2(self):

        index = faiss.index_factory(12, "SQ8")
        assert index.code_size == 12

    def test_factory_3(self):

        index = faiss.index_factory(12, "IVF10,PQ4")
        faiss.ParameterSpace().set_index_parameter(index, "nprobe", 3)
        assert index.nprobe == 3

        index = faiss.index_factory(12, "PCAR8,IVF10,PQ4")
        faiss.ParameterSpace().set_index_parameter(index, "nprobe", 3)
        assert faiss.downcast_index(index.index).nprobe == 3

    def test_factory_4(self):
        index = faiss.index_factory(12, "IVF10,FlatDedup")
        assert index.instances is not None


class TestCloneSize(unittest.TestCase):

    def test_clone_size(self):
        index = faiss.index_factory(20, 'PCA10,Flat')
        xb = faiss.rand((100, 20))
        index.train(xb)
        index.add(xb)
        index2 = faiss.clone_index(index)
        assert index2.ntotal == 100
