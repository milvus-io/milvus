# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import absolute_import, division, print_function

import unittest
import faiss
import numpy as np

class TestIVFlib(unittest.TestCase):

    def test_methods_exported(self):
        methods = ['check_compatible_for_merge', 'extract_index_ivf',
                   'merge_into', 'search_centroid',
                   'search_and_return_centroids', 'get_invlist_range',
                   'set_invlist_range', 'search_with_parameters']

        for method in methods:
            assert callable(getattr(faiss, method, None))


def search_single_scan(index, xq, k, bs=128):
    """performs a search so that the inverted lists are accessed
    sequentially by blocks of size bs"""

    # handle pretransform
    if isinstance(index, faiss.IndexPreTransform):
        xq = index.apply_py(xq)
        index = faiss.downcast_index(index.index)

    # coarse assignment
    coarse_dis, assign = index.quantizer.search(xq, index.nprobe)
    nlist = index.nlist
    assign_buckets = assign // bs
    nq = len(xq)

    rh = faiss.ResultHeap(nq, k)
    index.parallel_mode |= index.PARALLEL_MODE_NO_HEAP_INIT

    for l0 in range(0, nlist, bs):
        bucket_no = l0 // bs
        skip_rows, skip_cols = np.where(assign_buckets != bucket_no)
        sub_assign = assign.copy()
        sub_assign[skip_rows, skip_cols] = -1

        index.search_preassigned(
            nq, faiss.swig_ptr(xq), k,
            faiss.swig_ptr(sub_assign), faiss.swig_ptr(coarse_dis),
            faiss.swig_ptr(rh.D), faiss.swig_ptr(rh.I),
            False, None
        )

    rh.finalize()

    return rh.D, rh.I


class TestSequentialScan(unittest.TestCase):

    def test_sequential_scan(self):
        d = 20
        index = faiss.index_factory(d, 'IVF100,SQ8')

        rs = np.random.RandomState(123)
        xt = rs.rand(5000, d).astype('float32')
        xb = rs.rand(10000, d).astype('float32')
        index.train(xt)
        index.add(xb)
        k = 15
        xq = rs.rand(200, d).astype('float32')

        ref_D, ref_I = index.search(xq, k)
        D, I = search_single_scan(index, xq, k, bs=10)

        assert np.all(D == ref_D)
        assert np.all(I == ref_I)
