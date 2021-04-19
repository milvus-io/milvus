# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#!/usr/bin/env python3

import os
import faiss
import numpy as np


class CombinedIndex:
    """
    combines a set of inverted lists into a hstack
    masks part of those lists
    adds these inverted lists to an empty index that contains
    the info on how to perform searches
    """

    def __init__(self, invlist_fnames, empty_index_fname,
                 masked_index_fname=None):

        self.indexes = indexes = []
        ilv = faiss.InvertedListsPtrVector()

        for fname in invlist_fnames:
            if os.path.exists(fname):
                print('reading', fname, end='\r', flush=True)
                index = faiss.read_index(fname)
                indexes.append(index)
                il = faiss.extract_index_ivf(index).invlists
            else:
                raise AssertionError
            ilv.push_back(il)
        print()

        self.big_il = faiss.VStackInvertedLists(ilv.size(), ilv.data())
        if masked_index_fname:
            self.big_il_base = self.big_il
            print('loading', masked_index_fname)
            self.masked_index = faiss.read_index(
                masked_index_fname,
                faiss.IO_FLAG_MMAP | faiss.IO_FLAG_READ_ONLY)
            self.big_il = faiss.MaskedInvertedLists(
                faiss.extract_index_ivf(self.masked_index).invlists,
                self.big_il_base)

        print('loading empty index', empty_index_fname)
        self.index = faiss.read_index(empty_index_fname)
        ntotal = self.big_il.compute_ntotal()

        print('replace invlists')
        index_ivf = faiss.extract_index_ivf(self.index)
        index_ivf.replace_invlists(self.big_il, False)
        index_ivf.ntotal = self.index.ntotal = ntotal
        index_ivf.parallel_mode = 1   # seems reasonable to do this all the time

        quantizer = faiss.downcast_index(index_ivf.quantizer)
        quantizer.hnsw.efSearch = 1024

    ############################################################
    # Expose fields and functions of the index as methods so that they
    # can be called by RPC

    def search(self, x, k):
        return self.index.search(x, k)

    def range_search(self, x, radius):
        return self.index.range_search(x, radius)

    def transform_and_assign(self, xq):
        index = self.index

        if isinstance(index, faiss.IndexPreTransform):
            assert index.chain.size() == 1
            vt = index.chain.at(0)
            xq = vt.apply_py(xq)

        # perform quantization
        index_ivf = faiss.extract_index_ivf(index)
        quantizer = index_ivf.quantizer
        coarse_dis, list_nos = quantizer.search(xq, index_ivf.nprobe)
        return xq, list_nos, coarse_dis


    def ivf_search_preassigned(self, xq, list_nos, coarse_dis, k):
        index_ivf = faiss.extract_index_ivf(self.index)
        n, d = xq.shape
        assert d == index_ivf.d
        n2, d2 = list_nos.shape
        assert list_nos.shape == coarse_dis.shape
        assert n2 == n
        assert d2 == index_ivf.nprobe
        D = np.empty((n, k), dtype='float32')
        I = np.empty((n, k), dtype='int64')
        index_ivf.search_preassigned(
            n, faiss.swig_ptr(xq), k,
            faiss.swig_ptr(list_nos), faiss.swig_ptr(coarse_dis),
            faiss.swig_ptr(D), faiss.swig_ptr(I), False)
        return D, I


    def ivf_range_search_preassigned(self, xq, list_nos, coarse_dis, radius):
        index_ivf = faiss.extract_index_ivf(self.index)
        n, d = xq.shape
        assert d == index_ivf.d
        n2, d2 = list_nos.shape
        assert list_nos.shape == coarse_dis.shape
        assert n2 == n
        assert d2 == index_ivf.nprobe
        res = faiss.RangeSearchResult(n)

        index_ivf.range_search_preassigned(
            n, faiss.swig_ptr(xq), radius,
            faiss.swig_ptr(list_nos), faiss.swig_ptr(coarse_dis),
            res)

        lims = faiss.rev_swig_ptr(res.lims, n + 1).copy()
        nd = int(lims[-1])
        D = faiss.rev_swig_ptr(res.distances, nd).copy()
        I = faiss.rev_swig_ptr(res.labels, nd).copy()
        return lims, D, I

    def set_nprobe(self, nprobe):
        index_ivf = faiss.extract_index_ivf(self.index)
        index_ivf.nprobe = nprobe

    def set_parallel_mode(self, pm):
        index_ivf = faiss.extract_index_ivf(self.index)
        index_ivf.parallel_mode = pm

    def get_ntotal(self):
        return self.index.ntotal

    def set_prefetch_nthread(self, nt):
        for idx in self.indexes:
            il = faiss.downcast_InvertedLists(
                faiss.extract_index_ivf(idx).invlists)
            il.prefetch_nthread
            il.prefetch_nthread = nt

    def set_omp_num_threads(self, nt):
        faiss.omp_set_num_threads(nt)

class CombinedIndexDeep1B(CombinedIndex):
    """ loads a CombinedIndex with the data from the big photodna index """

    def __init__(self):
        # set some paths
        workdir = "/checkpoint/matthijs/ondisk_distributed/"

        # empty index with the proper quantizer
        indexfname = workdir + 'trained.faissindex'

        # index that has some invlists that override the big one
        masked_index_fname = None
        invlist_fnames = [
            '%s/hslices/slice%d.faissindex' % (workdir, i)
            for i in range(50)
        ]
        CombinedIndex.__init__(self, invlist_fnames, indexfname, masked_index_fname)


def ivecs_read(fname):
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def fvecs_read(fname):
    return ivecs_read(fname).view('float32')


if __name__ == '__main__':
    import time
    ci = CombinedIndexDeep1B()
    print('loaded index of size ', ci.index.ntotal)

    deep1bdir = "/datasets01_101/simsearch/041218/deep1b/"

    xq = fvecs_read(deep1bdir + "deep1B_queries.fvecs")
    gt_fname = deep1bdir + "deep1B_groundtruth.ivecs"
    gt = ivecs_read(gt_fname)

    for nprobe in 1, 10, 100, 1000:
        ci.set_nprobe(nprobe)
        t0 = time.time()
        D, I = ci.search(xq, 100)
        t1 = time.time()
        print('nprobe=%d 1-recall@1=%.4f t=%.2fs' % (
            nprobe, (I[:, 0] == gt[:, 0]).sum() / len(xq),
            t1 - t0
        ))
