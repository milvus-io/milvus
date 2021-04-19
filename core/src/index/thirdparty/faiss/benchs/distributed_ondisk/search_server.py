# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import os
import time
import rpc
import combined_index

import argparse


############################################################
# Server implementation
############################################################


class MyServer(rpc.Server):
    """ Assign version that can be exposed via RPC """
    def __init__(self, s, index):
        rpc.Server.__init__(self, s)
        self.index = index

    def __getattr__(self, f):
        return getattr(self.index, f)

def main():
    parser = argparse.ArgumentParser()

    def aa(*args, **kwargs):
        group.add_argument(*args, **kwargs)

    group = parser.add_argument_group('server options')
    aa('--port', default=12012, type=int, help='server port')
    aa('--when_ready_dir', default=None,
       help='store host:port to this file when ready')
    aa('--ipv4', default=False, action='store_true', help='force ipv4')
    aa('--rank', default=0, type=int,
       help='rank used as index in the client table')

    args = parser.parse_args()

    when_ready = None
    if args.when_ready_dir:
        when_ready = '%s/%d' % (args.when_ready_dir, args.rank)

    print('loading index')

    index = combined_index.CombinedIndexDeep1B()

    print('starting server')
    rpc.run_server(
        lambda s: MyServer(s, index),
        args.port, report_to_file=when_ready,
        v6=not args.ipv4)

if __name__ == '__main__':
    main()


############################################################
# Client implementation
############################################################

from multiprocessing.dummy import Pool as ThreadPool
import faiss
import numpy as np



class ResultHeap:
    """ Combine query results from a sliced dataset (for k-nn search) """

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

def distribute_weights(weights, nbin):
    """ assign a set of weights to a smaller set of bins to balance them """
    nw = weights.size
    o = weights.argsort()
    bins = np.zeros(nbin)
    assign = np.ones(nw, dtype=int)
    for i in o[::-1]:
        b = bins.argmin()
        assign[i] = b
        bins[b] += weights[i]
    return bins, assign



class SplitPerListIndex:
    """manages a local index, that does the coarse quantization and a set
    of sub_indexes. The sub_indexes search a subset of the inverted
    lists. The SplitPerListIndex merges results from the sub-indexes"""

    def __init__(self, index, sub_indexes):
        self.index = index
        self.code_size = faiss.extract_index_ivf(index.index).code_size
        self.sub_indexes = sub_indexes
        self.ni = len(self.sub_indexes)
        # pool of threads. Each thread manages one sub-index.
        self.pool = ThreadPool(self.ni)
        self.verbose = False

    def set_nprobe(self, nprobe):
        self.index.set_nprobe(nprobe)
        self.pool.map(
            lambda i: self.sub_indexes[i].set_nprobe(nprobe),
            range(self.ni)
        )

    def set_omp_num_threads(self, nt):
        faiss.omp_set_num_threads(nt)
        self.pool.map(
            lambda idx: idx.set_omp_num_threads(nt),
            self.sub_indexes
        )

    def set_parallel_mode(self, pm):
        self.index.set_parallel_mode(pm)
        self.pool.map(
            lambda idx: idx.set_parallel_mode(pm),
            self.sub_indexes
        )

    def set_prefetch_nthread(self, nt):
        self.index.set_prefetch_nthread(nt)
        self.pool.map(
            lambda idx: idx.set_prefetch_nthread(nt),
            self.sub_indexes
        )

    def balance_lists(self, list_nos):
        big_il = self.index.big_il
        weights = np.array([big_il.list_size(int(i))
                            for i in list_nos.ravel()])
        bins, assign = distribute_weights(weights, self.ni)
        if self.verbose:
            print('bins weight range %d:%d total %d (%.2f MiB)' % (
                bins.min(), bins.max(), bins.sum(),
                bins.sum() * (self.code_size + 8) / 2 ** 20))
        self.nscan = bins.sum()
        return assign.reshape(list_nos.shape)

    def search(self, x, k):
        xqo, list_nos, coarse_dis = self.index.transform_and_assign(x)
        assign = self.balance_lists(list_nos)

        def do_query(i):
            sub_index = self.sub_indexes[i]
            list_nos_i = list_nos.copy()
            list_nos_i[assign != i] = -1
            t0 = time.time()
            Di, Ii = sub_index.ivf_search_preassigned(
                xqo, list_nos_i, coarse_dis, k)
            #print(list_nos_i, Ii)
            if self.verbose:
                print('client %d: %.3f s' % (i, time.time() - t0))
            return Di, Ii

        rh = ResultHeap(x.shape[0], k)

        for Di, Ii in self.pool.imap(do_query, range(self.ni)):
            #print("ADD", Ii, rh.I)
            rh.add_batch_result(Di, Ii, 0)
        rh.finalize()
        return rh.D, rh.I

    def range_search(self, x, radius):
        xqo, list_nos, coarse_dis = self.index.transform_and_assign(x)
        assign = self.balance_lists(list_nos)
        nq = len(x)

        def do_query(i):
            sub_index = self.sub_indexes[i]
            list_nos_i = list_nos.copy()
            list_nos_i[assign != i] = -1
            t0 = time.time()
            limi, Di, Ii = sub_index.ivf_range_search_preassigned(
                xqo, list_nos_i, coarse_dis, radius)
            if self.verbose:
                print('slice %d: %.3f s' % (i, time.time() - t0))
            return limi, Di, Ii

        D = [[] for i in range(nq)]
        I = [[] for i in range(nq)]

        sizes = np.zeros(nq, dtype=int)
        for lims, Di, Ii in self.pool.imap(do_query, range(self.ni)):
            for i in range(nq):
                l0, l1 = lims[i:i + 2]
                D[i].append(Di[l0:l1])
                I[i].append(Ii[l0:l1])
                sizes[i] += l1 - l0
        lims = np.zeros(nq + 1, dtype=int)
        lims[1:] = np.cumsum(sizes)
        D = np.hstack([j for i in D for j in i])
        I = np.hstack([j for i in I for j in i])
        return lims, D, I
