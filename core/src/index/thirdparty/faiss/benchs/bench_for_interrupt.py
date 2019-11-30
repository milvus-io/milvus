# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python3

from __future__ import print_function
import numpy as np
import faiss
import time
import os
import argparse


parser = argparse.ArgumentParser()

def aa(*args, **kwargs):
    group.add_argument(*args, **kwargs)

group = parser.add_argument_group('dataset options')
aa('--dim', type=int, default=64)
aa('--nb', type=int, default=int(1e6))
aa('--subset_len', type=int, default=int(1e5))
aa('--key', default='IVF1000,Flat')
aa('--nprobe', type=int, default=640)
aa('--no_intcallback', default=False, action='store_true')
aa('--twostage', default=False, action='store_true')
aa('--nt', type=int, default=-1)


args = parser.parse_args()
print("args:", args)


d = args.dim  # dimension
nb = args.nb  # database size
nq = 1000  # nb of queries
nt = 100000
subset_len = args.subset_len


np.random.seed(1234)  # make reproducible
xb = np.random.random((nb, d)).astype('float32')
xq = np.random.random((nq, d)).astype('float32')
xt = np.random.random((nt, d)).astype('float32')
k = 100

if args.no_intcallback:
    faiss.InterruptCallback.clear_instance()

if args.nt != -1:
    faiss.omp_set_num_threads(args.nt)

nprobe = args.nprobe
key = args.key
#key = 'IVF1000,Flat'
# key = 'IVF1000,PQ64'
# key = 'IVF100_HNSW32,PQ64'

# faiss.omp_set_num_threads(1)

pf = 'dim%d_' % d
if d == 64:
    pf = ''

basename = '/tmp/base%s%s.index' % (pf, key)

if os.path.exists(basename):
    print('load', basename)
    index_1 = faiss.read_index(basename)
else:
    print('train + write', basename)
    index_1 = faiss.index_factory(d, key)
    index_1.train(xt)
    faiss.write_index(index_1, basename)

print('add')
index_1.add(xb)

print('set nprobe=', nprobe)
faiss.ParameterSpace().set_index_parameter(index_1, 'nprobe', nprobe)

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

stats = faiss.cvar.indexIVF_stats
stats.reset()

print('index size', index_1.ntotal,
      'imbalance', index_1.invlists.imbalance_factor())
start = time.time()
Dref, Iref = index_1.search(xq, k)
print('time of searching: %.3f s = %.3f + %.3f ms' % (
    time.time() - start, stats.quantization_time, stats.search_time))

indexes = {}
if args.twostage:

    for i in range(0, nb, subset_len):
        index = faiss.read_index(basename)
        faiss.ParameterSpace().set_index_parameter(index, 'nprobe', nprobe)
        print("add %d:%d" %(i, i+subset_len))
        index.add(xb[i:i + subset_len])
        indexes[i] = index

rh = ResultHeap(nq, k)
sum_time = tq = ts = 0
for i in range(0, nb, subset_len):
    if not args.twostage:
        index = faiss.read_index(basename)
        faiss.ParameterSpace().set_index_parameter(index, 'nprobe', nprobe)
        print("add %d:%d" %(i, i+subset_len))
        index.add(xb[i:i + subset_len])
    else:
        index = indexes[i]

    stats.reset()
    start = time.time()
    Di, Ii = index.search(xq, k)
    sum_time = sum_time + time.time() - start
    tq += stats.quantization_time
    ts += stats.search_time
    rh.add_batch_result(Di, Ii, i)

print('time of searching separately: %.3f s = %.3f + %.3f ms' %
      (sum_time, tq, ts))

rh.finalize()

print('diffs: %d / %d'  % ((Iref != rh.I).sum(), Iref.size))
