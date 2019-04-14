# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

import unittest
import faiss


class TestIVFlib(unittest.TestCase):

    def test_methods_exported(self):
        methods = ['check_compatible_for_merge', 'extract_index_ivf',
                   'merge_into', 'search_centroid',
                   'search_and_return_centroids', 'get_invlist_range',
                   'set_invlist_range', 'search_with_parameters']

        for method in methods:
            assert callable(getattr(faiss, method, None))
