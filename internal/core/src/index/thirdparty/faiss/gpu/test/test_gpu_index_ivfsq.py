# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python3

from __future__ import print_function
import unittest
import numpy as np
import faiss

def make_t(num, d, clamp=False):
    rs = np.random.RandomState(123)
    x = rs.rand(num, d).astype('float32')
    if clamp:
        x = (x * 255).astype('uint8').astype('float32')
    return x

def make_indices_copy_from_cpu(nlist, d, qtype, by_residual, metric, clamp):
    to_train = make_t(10000, d, clamp)

    quantizer_cp = faiss.IndexFlat(d, metric)
    idx_cpu = faiss.IndexIVFScalarQuantizer(quantizer_cp, d, nlist,
                                            qtype, metric, by_residual)

    idx_cpu.train(to_train)
    idx_cpu.add(to_train)

    res = faiss.StandardGpuResources()
    res.noTempMemory()
    idx_gpu = faiss.GpuIndexIVFScalarQuantizer(res, idx_cpu)

    return idx_cpu, idx_gpu


def make_indices_copy_from_gpu(nlist, d, qtype, by_residual, metric, clamp):
    to_train = make_t(10000, d, clamp)

    res = faiss.StandardGpuResources()
    res.noTempMemory()
    idx_gpu = faiss.GpuIndexIVFScalarQuantizer(res, d, nlist,
                                               qtype, metric, by_residual)
    idx_gpu.train(to_train)
    idx_gpu.add(to_train)

    quantizer_cp = faiss.IndexFlat(d, metric)
    idx_cpu = faiss.IndexIVFScalarQuantizer(quantizer_cp, d, nlist,
                                            qtype, metric, by_residual)
    idx_gpu.copyTo(idx_cpu)

    return idx_cpu, idx_gpu


def make_indices_train(nlist, d, qtype, by_residual, metric, clamp):
    to_train = make_t(10000, d, clamp)

    quantizer_cp = faiss.IndexFlat(d, metric)
    idx_cpu = faiss.IndexIVFScalarQuantizer(quantizer_cp, d, nlist,
                                            qtype, metric, by_residual)
    assert(by_residual == idx_cpu.by_residual)

    idx_cpu.train(to_train)
    idx_cpu.add(to_train)

    res = faiss.StandardGpuResources()
    res.noTempMemory()
    idx_gpu = faiss.GpuIndexIVFScalarQuantizer(res, d, nlist,
                                               qtype, metric, by_residual)
    assert(by_residual == idx_gpu.by_residual)

    idx_gpu.train(to_train)
    idx_gpu.add(to_train)

    return idx_cpu, idx_gpu

#
# Testing functions
#

def summarize_results(dist, idx):
    valid = []
    invalid = []
    for query in range(dist.shape[0]):
        valid_sub = {}
        invalid_sub = []

        for order, (d, i) in enumerate(zip(dist[query], idx[query])):
            if i == -1:
                invalid_sub.append(order)
            else:
                valid_sub[i] = [order, d]

        valid.append(valid_sub)
        invalid.append(invalid_sub)

    return valid, invalid

def compare_results(d1, i1, d2, i2):
    # Count number of index differences
    idx_diffs = {}
    idx_diffs_inf = 0
    idx_invalid = 0

    valid1, invalid1 = summarize_results(d1, i1)
    valid2, invalid2 = summarize_results(d2, i2)

    # Invalid results should be the same for both
    # (except if we happen to hit different centroids)
    for inv1, inv2 in zip(invalid1, invalid2):
        if (len(inv1) != len(inv2)):
            print('mismatch ', len(inv1), len(inv2), inv2[0])

        assert(len(inv1) == len(inv2))
        idx_invalid += len(inv2)
        for x1, x2 in zip(inv1, inv2):
            assert(x1 == x2)

    for _, (query1, query2) in enumerate(zip(valid1, valid2)):
        for idx1, order_d1 in query1.items():
            order_d2 = query2.get(idx1, None)
            if order_d2:
                idx_diff = order_d1[0] - order_d2[0]

                if idx_diff not in idx_diffs:
                    idx_diffs[idx_diff] = 1
                else:
                    idx_diffs[idx_diff] += 1
            else:
                idx_diffs_inf += 1

    return idx_diffs, idx_diffs_inf, idx_invalid

def check_diffs(total_num, in_window_thresh, diffs, diff_inf, invalid):
    # We require a certain fraction of results to be within +/- diff_window
    # index differences
    diff_window = 4
    in_window = 0

    for diff in sorted(diffs):
        if abs(diff) <= diff_window:
            in_window += diffs[diff] / total_num

    if (in_window < in_window_thresh):
        print('error {} {}'.format(in_window, in_window_thresh))
        assert(in_window >= in_window_thresh)

def do_test_with_index(ci, gi, nprobe, k, clamp, in_window_thresh):
    num_query = 11
    to_query = make_t(num_query, ci.d, clamp)

    ci.nprobe = ci.nprobe
    gi.nprobe = gi.nprobe

    total_num = num_query * k
    check_diffs(total_num, in_window_thresh,
                *compare_results(*ci.search(to_query, k),
                                 *gi.search(to_query, k)))

def do_test(nlist, d, qtype, by_residual, metric, nprobe, k):
    clamp = (qtype == faiss.ScalarQuantizer.QT_8bit_direct)
    ci, gi = make_indices_copy_from_cpu(nlist, d, qtype,
                                        by_residual, metric, clamp)
    # A direct copy should be much more closely in agreement
    # (except for fp accumulation order differences)
    do_test_with_index(ci, gi, nprobe, k, clamp, 0.99)

    ci, gi = make_indices_copy_from_gpu(nlist, d, qtype,
                                        by_residual, metric, clamp)
    # A direct copy should be much more closely in agreement
    # (except for fp accumulation order differences)
    do_test_with_index(ci, gi, nprobe, k, clamp, 0.99)

    ci, gi = make_indices_train(nlist, d, qtype,
                                by_residual, metric, clamp)
    # Separate training can produce a slightly different coarse quantizer
    # and residuals
    do_test_with_index(ci, gi, nprobe, k, clamp, 0.8)

def do_multi_test(qtype):
    nlist = 100
    nprobe = 10
    k = 50

    for d in [11, 64]:
        if (qtype != faiss.ScalarQuantizer.QT_8bit_direct):
            # residual doesn't make sense here
            do_test(nlist, d, qtype, True,
                    faiss.METRIC_L2, nprobe, k)
            do_test(nlist, d, qtype, True,
                    faiss.METRIC_INNER_PRODUCT, nprobe, k)
        do_test(nlist, d, qtype, False, faiss.METRIC_L2, nprobe, k)
        do_test(nlist, d, qtype, False, faiss.METRIC_INNER_PRODUCT, nprobe, k)

#
# Test
#

class TestSQ(unittest.TestCase):
    def test_fp16(self):
        do_multi_test(faiss.ScalarQuantizer.QT_fp16)

    def test_8bit(self):
        do_multi_test(faiss.ScalarQuantizer.QT_8bit)

    def test_8bit_uniform(self):
        do_multi_test(faiss.ScalarQuantizer.QT_8bit_uniform)

    def test_6bit(self):
        try:
            do_multi_test(faiss.ScalarQuantizer.QT_6bit)
            # should not reach here; QT_6bit is unimplemented
        except:
            print('QT_6bit exception thrown (is expected)')
        else:
            assert(False)

    def test_4bit(self):
        do_multi_test(faiss.ScalarQuantizer.QT_4bit)

    def test_4bit_uniform(self):
        do_multi_test(faiss.ScalarQuantizer.QT_4bit_uniform)

    def test_8bit_direct(self):
        do_multi_test(faiss.ScalarQuantizer.QT_8bit_direct)


if __name__ == '__main__':
    unittest.main()
