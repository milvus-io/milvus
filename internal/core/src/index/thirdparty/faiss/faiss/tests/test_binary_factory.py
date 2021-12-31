# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import absolute_import, division, print_function

import unittest
import faiss


class TestBinaryFactory(unittest.TestCase):

    def test_factory_IVF(self):

        index = faiss.index_binary_factory(16, "BIVF10")
        assert index.invlists is not None
        assert index.nlist == 10
        assert index.code_size == 2

    def test_factory_Flat(self):

        index = faiss.index_binary_factory(16, "BFlat")
        assert index.code_size == 2

    def test_factory_HNSW(self):

        index = faiss.index_binary_factory(256, "BHNSW32")
        assert index.code_size == 32

    def test_factory_IVF_HNSW(self):

        index = faiss.index_binary_factory(256, "BIVF1024_BHNSW32")
        assert index.code_size == 32
        assert index.nlist == 1024
