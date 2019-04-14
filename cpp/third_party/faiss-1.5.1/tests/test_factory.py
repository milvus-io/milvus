# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

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
