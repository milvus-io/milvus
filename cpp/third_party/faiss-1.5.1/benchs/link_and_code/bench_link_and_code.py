# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#!/usr/bin/env python2

import os
import sys
import time
import numpy as np
import re
import faiss
from multiprocessing.dummy import Pool as ThreadPool
import pdb
import argparse
import datasets
from datasets import sanitize
import neighbor_codec

######################################################
# Command-line parsing
######################################################


parser = argparse.ArgumentParser()

def aa(*args, **kwargs):
    group.add_argument(*args, **kwargs)

group = parser.add_argument_group('dataset options')

aa('--db', default='deep1M', help='dataset')
aa( '--compute_gt', default=False, action='store_true',
    help='compute and store the groundtruth')

group = parser.add_argument_group('index consturction')

aa('--indexkey', default='HNSW32', help='index_factory type')
aa('--efConstruction', default=200, type=int,
   help='HNSW construction factor')
aa('--M0', default=-1, type=int, help='size of base level')
aa('--maxtrain', default=256 * 256, type=int,
   help='maximum number of training points')
aa('--indexfile', default='', help='file to read or write index from')
aa('--add_bs', default=-1, type=int,
   help='add elements index by batches of this size')
aa('--link_singletons', default=False, action='store_true',
   help='do a pass to link in the singletons')

group = parser.add_argument_group(
    'searching (reconstruct_from_neighbors options)')

aa('--beta_centroids', default='',
   help='file with codebook')
aa('--neigh_recons_codes', default='',
   help='file with codes for reconstruction')
aa('--beta_ntrain', default=250000, type=int, help='')
aa('--beta_k', default=256, type=int, help='beta codebook size')
aa('--beta_nsq', default=1, type=int, help='number of beta sub-vectors')
aa('--beta_niter', default=10, type=int, help='')
aa('--k_reorder', default='-1', help='')

group = parser.add_argument_group('searching')

aa('--k', default=100, type=int, help='nb of nearest neighbors')
aa('--exhaustive', default=False, action='store_true',
    help='report the exhaustive search topline')
aa('--searchthreads', default=-1, type=int,
   help='nb of threads to use at search time')
aa('--efSearch', default='', type=str,
   help='comma-separated values of efSearch to try')

args = parser.parse_args()

print "args:", args


######################################################
# Load dataset
######################################################

xt, xb, xq, gt = datasets.load_data(
    dataset=args.db, compute_gt=args.compute_gt)

nq, d = xq.shape
nb, d = xb.shape


######################################################
# Make index
######################################################

if os.path.exists(args.indexfile):

    print "reading", args.indexfile
    index = faiss.read_index(args.indexfile)

    if isinstance(index, faiss.IndexPreTransform):
        index_hnsw = faiss.downcast_index(index.index)
        vec_transform = index.chain.at(0).apply_py
    else:
        index_hnsw = index
        vec_transform = lambda x:x

    hnsw = index_hnsw.hnsw
    hnsw_stats = faiss.cvar.hnsw_stats

else:

    print "build index, key=", args.indexkey

    index = faiss.index_factory(d, args.indexkey)

    if isinstance(index, faiss.IndexPreTransform):
        index_hnsw = faiss.downcast_index(index.index)
        vec_transform = index.chain.at(0).apply_py
    else:
        index_hnsw = index
        vec_transform = lambda x:x

    hnsw = index_hnsw.hnsw
    hnsw.efConstruction = args.efConstruction
    hnsw_stats = faiss.cvar.hnsw_stats
    index.verbose = True
    index_hnsw.verbose = True
    index_hnsw.storage.verbose = True

    if args.M0 != -1:
        print "set level 0 nb of neighbors to", args.M0
        hnsw.set_nb_neighbors(0, args.M0)

    xt2 = sanitize(xt[:args.maxtrain])
    assert np.all(np.isfinite(xt2))

    print "train, size", xt.shape
    t0 = time.time()
    index.train(xt2)
    print "  train in %.3f s" % (time.time() - t0)

    print "adding"
    t0 = time.time()
    if args.add_bs == -1:
        index.add(sanitize(xb))
    else:
        for i0 in range(0, nb, args.add_bs):
            i1 = min(nb, i0 + args.add_bs)
            print "  adding %d:%d / %d" % (i0, i1, nb)
            index.add(sanitize(xb[i0:i1]))

    print "  add in %.3f s" % (time.time() - t0)
    print "storing", args.indexfile
    faiss.write_index(index, args.indexfile)


######################################################
# Train beta centroids and encode dataset
######################################################

if args.beta_centroids:
    print "reordering links"
    index_hnsw.reorder_links()

    if os.path.exists(args.beta_centroids):
        print "load", args.beta_centroids
        beta_centroids = np.load(args.beta_centroids)
        nsq, k, M1 = beta_centroids.shape
        assert M1 == hnsw.nb_neighbors(0) + 1

        rfn = faiss.ReconstructFromNeighbors(index_hnsw, k, nsq)
    else:
        print "train beta centroids"
        rfn = faiss.ReconstructFromNeighbors(
            index_hnsw, args.beta_k, args.beta_nsq)

        xb_full = vec_transform(sanitize(xb[:args.beta_ntrain]))

        beta_centroids = neighbor_codec.train_beta_codebook(
            rfn, xb_full, niter=args.beta_niter)

        print "  storing", args.beta_centroids
        np.save(args.beta_centroids, beta_centroids)


    faiss.copy_array_to_vector(beta_centroids.ravel(),
                               rfn.codebook)
    index_hnsw.reconstruct_from_neighbors = rfn

    if rfn.k == 1:
        pass     # no codes to take care of
    elif os.path.exists(args.neigh_recons_codes):
        print "loading neigh codes", args.neigh_recons_codes
        codes = np.load(args.neigh_recons_codes)
        assert codes.size == rfn.code_size * index.ntotal
        faiss.copy_array_to_vector(codes.astype('uint8'),
                                   rfn.codes)
        rfn.ntotal = index.ntotal
    else:
        print "encoding neigh codes"
        t0 = time.time()

        bs = 1000000 if args.add_bs == -1 else args.add_bs

        for i0 in range(0, nb, bs):
            i1 = min(i0 + bs, nb)
            print "   encode %d:%d / %d [%.3f s]\r" % (
                i0, i1, nb, time.time() - t0),
            sys.stdout.flush()
            xbatch = vec_transform(sanitize(xb[i0:i1]))
            rfn.add_codes(i1 - i0, faiss.swig_ptr(xbatch))
        print

        print "storing %s" % args.neigh_recons_codes
        codes = faiss.vector_to_array(rfn.codes)
        np.save(args.neigh_recons_codes, codes)

######################################################
# Exhaustive evaluation
######################################################

if args.exhaustive:
    print "exhaustive evaluation"
    xq_tr = vec_transform(sanitize(xq))
    index2 = faiss.IndexFlatL2(index_hnsw.d)
    accu_recons_error = 0.0

    if faiss.get_num_gpus() > 0:
        print "do eval on GPU"
        co = faiss.GpuMultipleClonerOptions()
        co.shard = False
        index2 = faiss.index_cpu_to_all_gpus(index2, co)

    # process in batches in case the dataset does not fit in RAM
    rh = datasets.ResultHeap(xq_tr.shape[0], 100)
    t0 = time.time()
    bs = 500000
    for i0 in range(0, nb, bs):
        i1 = min(nb, i0 + bs)
        print '  handling batch %d:%d' % (i0, i1)

        xb_recons = np.empty(
            (i1 - i0, index_hnsw.d), dtype='float32')
        rfn.reconstruct_n(i0, i1 - i0, faiss.swig_ptr(xb_recons))

        accu_recons_error += (
            (vec_transform(sanitize(xb[i0:i1])) -
             xb_recons)**2).sum()

        index2.reset()
        index2.add(xb_recons)
        D, I = index2.search(xq_tr, 100)
        rh.add_batch_result(D, I, i0)

    rh.finalize()
    del index2
    t1 = time.time()
    print "done in %.3f s" % (t1 - t0)
    print "total reconstruction error: ", accu_recons_error
    print "eval retrieval:"
    datasets.evaluate_DI(rh.D, rh.I, gt)


def get_neighbors(hnsw, i, level):
    " list the neighbors for node i at level "
    assert i < hnsw.levels.size()
    assert level < hnsw.levels.at(i)
    be = np.empty(2, 'uint64')
    hnsw.neighbor_range(i, level, faiss.swig_ptr(be), faiss.swig_ptr(be[1:]))
    return [hnsw.neighbors.at(j) for j in range(be[0], be[1])]


#############################################################
# Index is ready
#############################################################

xq = sanitize(xq)

if args.searchthreads != -1:
    print "Setting nb of threads to", args.searchthreads
    faiss.omp_set_num_threads(args.searchthreads)


if gt is None:
    print "no valid groundtruth -- exit"
    sys.exit()


k_reorders = [int(x) for x in args.k_reorder.split(',')]
efSearchs = [int(x) for x in args.efSearch.split(',')]


for k_reorder in k_reorders:

    if index_hnsw.reconstruct_from_neighbors:
        print "setting k_reorder=%d" % k_reorder
        index_hnsw.reconstruct_from_neighbors.k_reorder = k_reorder

    for efSearch in efSearchs:
        print "efSearch=%-4d" % efSearch,
        hnsw.efSearch = efSearch
        hnsw_stats.reset()
        datasets.evaluate(xq, gt, index, k=args.k, endl=False)

        print "ndis %d nreorder %d" % (hnsw_stats.ndis, hnsw_stats.nreorder)
