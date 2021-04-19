# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import os
import time
import numpy as np
import faiss
import argparse
from multiprocessing.dummy import Pool as ThreadPool

def ivecs_mmap(fname):
    a = np.memmap(fname, dtype='int32', mode='r')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:]

def fvecs_mmap(fname):
    return ivecs_mmap(fname).view('float32')


def produce_batches(args):

    x = fvecs_mmap(args.input)

    if args.i1 == -1:
        args.i1 = len(x)

    print("Iterating on vectors %d:%d from %s by batches of size %d" % (
        args.i0, args.i1, args.input, args.bs))

    for j0 in range(args.i0, args.i1, args.bs):
        j1 = min(j0 + args.bs, args.i1)
        yield np.arange(j0, j1), x[j0:j1]


def rate_limited_iter(l):
    'a thread pre-processes the next element'
    pool = ThreadPool(1)
    res = None

    def next_or_None():
        try:
            return next(l)
        except StopIteration:
            return None

    while True:
        res_next = pool.apply_async(next_or_None)
        if res is not None:
            res = res.get()
            if res is None:
                return
            yield res
        res = res_next

deep1bdir = "/datasets01_101/simsearch/041218/deep1b/"
workdir = "/checkpoint/matthijs/ondisk_distributed/"

def main():
    parser = argparse.ArgumentParser(
        description='make index for a subset of the data')

    def aa(*args, **kwargs):
        group.add_argument(*args, **kwargs)

    group = parser.add_argument_group('index type')
    aa('--inputindex',
       default=workdir + 'trained.faissindex',
       help='empty input index to fill in')
    aa('--nt', default=-1, type=int, help='nb of openmp threads to use')

    group = parser.add_argument_group('db options')
    aa('--input', default=deep1bdir + "base.fvecs")
    aa('--bs', default=2**18, type=int,
       help='batch size for db access')
    aa('--i0', default=0, type=int, help='lower bound to index')
    aa('--i1', default=-1, type=int, help='upper bound of vectors to index')

    group = parser.add_argument_group('output')
    aa('-o', default='/tmp/x', help='output index')
    aa('--keepquantizer', default=False, action='store_true',
       help='by default we remove the data from the quantizer to save space')

    args = parser.parse_args()
    print('args=', args)

    print('start accessing data')
    src = produce_batches(args)

    print('loading index', args.inputindex)
    index = faiss.read_index(args.inputindex)

    if args.nt != -1:
        faiss.omp_set_num_threads(args.nt)

    t0 = time.time()
    ntot = 0
    for ids, x in rate_limited_iter(src):
        print('add %d:%d (%.3f s)' % (ntot, ntot + ids.size, time.time() - t0))
        index.add_with_ids(np.ascontiguousarray(x, dtype='float32'), ids)
        ntot += ids.size

    index_ivf = faiss.extract_index_ivf(index)
    print('invlists stats: imbalance %.3f' % index_ivf.invlists.imbalance_factor())
    index_ivf.invlists.print_stats()

    if not args.keepquantizer:
        print('resetting quantizer content')
        index_ivf = faiss.extract_index_ivf(index)
        index_ivf.quantizer.reset()

    print('store output', args.o)
    faiss.write_index(index, args.o)

if __name__ == '__main__':
    main()
