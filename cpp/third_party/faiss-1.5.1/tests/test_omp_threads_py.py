# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import absolute_import, division, print_function, unicode_literals
import faiss
import unittest


class TestOpenMP(unittest.TestCase):

    def test_openmp(self):
        assert faiss.check_openmp()
