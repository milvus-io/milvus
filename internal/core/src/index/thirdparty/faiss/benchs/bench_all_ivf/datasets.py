# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

"""
Common functions to load datasets and compute their ground-truth
"""

from __future__ import print_function
import time
import numpy as np
import faiss
import sys

# set this to the directory that contains the datafiles.
# deep1b data should be at simdir + 'deep1b'
# bigann data should be at simdir + 'bigann'
simdir = '/mnt/vol/gfsai-east/ai-group/datasets/simsearch/'

#################################################################
# Small I/O functions
#################################################################


def ivecs_read(fname):
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def fvecs_read(fname):
    return ivecs_read(fname).view('float32')


def ivecs_mmap(fname):
    a = np.memmap(fname, dtype='int32', mode='r')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:]


def fvecs_mmap(fname):
    return ivecs_mmap(fname).view('float32')


def bvecs_mmap(fname):
    x = np.memmap(fname, dtype='uint8', mode='r')
    d = x[:4].view('int32')[0]
    return x.reshape(-1, d + 4)[:, 4:]


def ivecs_write(fname, m):
    n, d = m.shape
    m1 = np.empty((n, d + 1), dtype='int32')
    m1[:, 0] = d
    m1[:, 1:] = m
    m1.tofile(fname)


def fvecs_write(fname, m):
    m = m.astype('float32')
    ivecs_write(fname, m.view('int32'))



#################################################################
# Dataset
#################################################################

def sanitize(x):
    return np.ascontiguousarray(x, dtype='float32')


class ResultHeap:
    """ Combine query results from a sliced dataset """

    def __init__(self, nq, k):
        " nq: number of query vectors, k: number of results per query "
        self.I = np.zeros((nq, k), dtype='int64')
        self.D = np.zeros((nq, k), dtype='float32')
        self.nq, self.k = nq, k
        heaps = faiss.float_maxheap_array_t()
        heaps.k = k
        heaps.nh = nq
        heaps.val = faiss.swig_ptr(self.D)
        heaps.ids = faiss.swig_ptr(self.I)
        heaps.heapify()
        self.heaps = heaps

    def add_batch_result(self, D, I, i0):
        assert D.shape == (self.nq, self.k)
        assert I.shape == (self.nq, self.k)
        I += i0
        self.heaps.addn_with_ids(
            self.k, faiss.swig_ptr(D),
            faiss.swig_ptr(I), self.k)

    def finalize(self):
        self.heaps.reorder()


def compute_GT_sliced(xb, xq, k):
    print("compute GT")
    t0 = time.time()
    nb, d = xb.shape
    nq, d = xq.shape
    rh = ResultHeap(nq, k)
    bs = 10 ** 5

    xqs = sanitize(xq)

    db_gt = faiss.index_cpu_to_all_gpus(faiss.IndexFlatL2(d))

    # compute ground-truth by blocks of bs, and add to heaps
    for i0 in range(0, nb, bs):
        i1 = min(nb, i0 + bs)
        xsl = sanitize(xb[i0:i1])
        db_gt.add(xsl)
        D, I = db_gt.search(xqs, k)
        rh.add_batch_result(D, I, i0)
        db_gt.reset()
        print("\r   %d/%d, %.3f s" % (i0, nb, time.time() - t0), end=' ')
        sys.stdout.flush()
    print()
    rh.finalize()
    gt_I = rh.I

    print("GT time: %.3f s" % (time.time() - t0))
    return gt_I


def do_compute_gt(xb, xq, k):
    print("computing GT")
    nb, d = xb.shape
    index = faiss.index_cpu_to_all_gpus(faiss.IndexFlatL2(d))
    if nb < 100 * 1000:
        print("   add")
        index.add(np.ascontiguousarray(xb, dtype='float32'))
        print("   search")
        D, I = index.search(np.ascontiguousarray(xq, dtype='float32'), k)
    else:
        I = compute_GT_sliced(xb, xq, k)

    return I.astype('int32')


def load_data(dataset='deep1M', compute_gt=False):

    print("load data", dataset)

    if dataset == 'sift1M':
        basedir = simdir + 'sift1M/'

        xt = fvecs_read(basedir + "sift_learn.fvecs")
        xb = fvecs_read(basedir + "sift_base.fvecs")
        xq = fvecs_read(basedir + "sift_query.fvecs")
        gt = ivecs_read(basedir + "sift_groundtruth.ivecs")

    elif dataset.startswith('bigann'):
        basedir = simdir + 'bigann/'

        dbsize = 1000 if dataset == "bigann1B" else int(dataset[6:-1])
        xb = bvecs_mmap(basedir + 'bigann_base.bvecs')
        xq = bvecs_mmap(basedir + 'bigann_query.bvecs')
        xt = bvecs_mmap(basedir + 'bigann_learn.bvecs')
        # trim xb to correct size
        xb = xb[:dbsize * 1000 * 1000]
        gt = ivecs_read(basedir + 'gnd/idx_%dM.ivecs' % dbsize)

    elif dataset.startswith("deep"):
        basedir = simdir + 'deep1b/'
        szsuf = dataset[4:]
        if szsuf[-1] == 'M':
            dbsize = 10 ** 6 * int(szsuf[:-1])
        elif szsuf == '1B':
            dbsize = 10 ** 9
        elif szsuf[-1] == 'k':
            dbsize = 1000 * int(szsuf[:-1])
        else:
            assert False, "did not recognize suffix " + szsuf

        xt = fvecs_mmap(basedir + "learn.fvecs")
        xb = fvecs_mmap(basedir + "base.fvecs")
        xq = fvecs_read(basedir + "deep1B_queries.fvecs")

        xb = xb[:dbsize]

        gt_fname = basedir + "%s_groundtruth.ivecs" % dataset
        if compute_gt:
            gt = do_compute_gt(xb, xq, 100)
            print("store", gt_fname)
            ivecs_write(gt_fname, gt)

        gt = ivecs_read(gt_fname)

    else:
        assert False

    print("dataset %s sizes: B %s Q %s T %s" % (
        dataset, xb.shape, xq.shape, xt.shape))

    return xt, xb, xq, gt

#################################################################
# Evaluation
#################################################################


def evaluate_DI(D, I, gt):
    nq = gt.shape[0]
    k = I.shape[1]
    rank = 1
    while rank <= k:
        recall = (I[:, :rank] == gt[:, :1]).sum() / float(nq)
        print("R@%d: %.4f" % (rank, recall), end=' ')
        rank *= 10


def evaluate(xq, gt, index, k=100, endl=True):
    t0 = time.time()
    D, I = index.search(xq, k)
    t1 = time.time()
    nq = xq.shape[0]
    print("\t %8.4f ms per query, " % (
        (t1 - t0) * 1000.0 / nq), end=' ')
    rank = 1
    while rank <= k:
        recall = (I[:, :rank] == gt[:, :1]).sum() / float(nq)
        print("R@%d: %.4f" % (rank, recall), end=' ')
        rank *= 10
    if endl:
        print()
    return D, I
