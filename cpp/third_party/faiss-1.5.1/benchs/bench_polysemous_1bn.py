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

#################################################################
# I/O functions
#################################################################


def ivecs_read(fname):
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def fvecs_read(fname):
    return ivecs_read(fname).view('float32')

# we mem-map the biggest files to avoid having them in memory all at
# once


def mmap_fvecs(fname):
    x = np.memmap(fname, dtype='int32', mode='r')
    d = x[0]
    return x.view('float32').reshape(-1, d + 1)[:, 1:]


def mmap_bvecs(fname):
    x = np.memmap(fname, dtype='uint8', mode='r')
    d = x[:4].view('int32')[0]
    return x.reshape(-1, d + 4)[:, 4:]


#################################################################
# Bookkeeping
#################################################################


dbname        = sys.argv[1]
index_key     = sys.argv[2]
parametersets = sys.argv[3:]


tmpdir = '/tmp/bench_polysemous'

if not os.path.isdir(tmpdir):
    print "%s does not exist, creating it" % tmpdir
    os.mkdir(tmpdir)


#################################################################
# Prepare dataset
#################################################################


print "Preparing dataset", dbname

if dbname.startswith('SIFT'):
    # SIFT1M to SIFT1000M
    dbsize = int(dbname[4:-1])
    xb = mmap_bvecs('bigann/bigann_base.bvecs')
    xq = mmap_bvecs('bigann/bigann_query.bvecs')
    xt = mmap_bvecs('bigann/bigann_learn.bvecs')

    # trim xb to correct size
    xb = xb[:dbsize * 1000 * 1000]

    gt = ivecs_read('bigann/gnd/idx_%dM.ivecs' % dbsize)

elif dbname == 'Deep1B':
    xb = mmap_fvecs('deep1b/base.fvecs')
    xq = mmap_fvecs('deep1b/deep1B_queries.fvecs')
    xt = mmap_fvecs('deep1b/learn.fvecs')
    # deep1B's train is is outrageously big
    xt = xt[:10 * 1000 * 1000]
    gt = ivecs_read('deep1b/deep1B_groundtruth.ivecs')

else:
    print >> sys.stderr, 'unknown dataset', dbname
    sys.exit(1)


print "sizes: B %s Q %s T %s gt %s" % (
    xb.shape, xq.shape, xt.shape, gt.shape)

nq, d = xq.shape
nb, d = xb.shape
assert gt.shape[0] == nq


#################################################################
# Training
#################################################################


def choose_train_size(index_key):

    # some training vectors for PQ and the PCA
    n_train = 256 * 1000

    if "IVF" in index_key:
        matches = re.findall('IVF([0-9]+)', index_key)
        ncentroids = int(matches[0])
        n_train = max(n_train, 100 * ncentroids)
    elif "IMI" in index_key:
        matches = re.findall('IMI2x([0-9]+)', index_key)
        nbit = int(matches[0])
        n_train = max(n_train, 256 * (1 << nbit))
    return n_train


def get_trained_index():
    filename = "%s/%s_%s_trained.index" % (
        tmpdir, dbname, index_key)

    if not os.path.exists(filename):
        index = faiss.index_factory(d, index_key)

        n_train = choose_train_size(index_key)

        xtsub = xt[:n_train]
        print "Keeping %d train vectors" % xtsub.shape[0]
        # make sure the data is actually in RAM and in float
        xtsub = xtsub.astype('float32').copy()
        index.verbose = True

        t0 = time.time()
        index.train(xtsub)
        index.verbose = False
        print "train done in %.3f s" % (time.time() - t0)
        print "storing", filename
        faiss.write_index(index, filename)
    else:
        print "loading", filename
        index = faiss.read_index(filename)
    return index


#################################################################
# Adding vectors to dataset
#################################################################

def rate_limited_imap(f, l):
    'a thread pre-processes the next element'
    pool = ThreadPool(1)
    res = None
    for i in l:
        res_next = pool.apply_async(f, (i, ))
        if res:
            yield res.get()
        res = res_next
    yield res.get()


def matrix_slice_iterator(x, bs):
    " iterate over the lines of x in blocks of size bs"
    nb = x.shape[0]
    block_ranges = [(i0, min(nb, i0 + bs))
                    for i0 in range(0, nb, bs)]

    return rate_limited_imap(
        lambda (i0, i1): x[i0:i1].astype('float32').copy(),
        block_ranges)


def get_populated_index():

    filename = "%s/%s_%s_populated.index" % (
        tmpdir, dbname, index_key)

    if not os.path.exists(filename):
        index = get_trained_index()
        i0 = 0
        t0 = time.time()
        for xs in matrix_slice_iterator(xb, 100000):
            i1 = i0 + xs.shape[0]
            print '\radd %d:%d, %.3f s' % (i0, i1, time.time() - t0),
            sys.stdout.flush()
            index.add(xs)
            i0 = i1
        print
        print "Add done in %.3f s" % (time.time() - t0)
        print "storing", filename
        faiss.write_index(index, filename)
    else:
        print "loading", filename
        index = faiss.read_index(filename)
    return index


#################################################################
# Perform searches
#################################################################

index = get_populated_index()

ps = faiss.ParameterSpace()
ps.initialize(index)

# make sure queries are in RAM
xq = xq.astype('float32').copy()

# a static C++ object that collects statistics about searches
ivfpq_stats = faiss.cvar.indexIVFPQ_stats


if parametersets == ['autotune'] or parametersets == ['autotuneMT']:

    if parametersets == ['autotune']:
        faiss.omp_set_num_threads(1)

    # setup the Criterion object: optimize for 1-R@1
    crit = faiss.OneRecallAtRCriterion(nq, 1)
    # by default, the criterion will request only 1 NN
    crit.nnn = 100
    crit.set_groundtruth(None, gt.astype('int64'))

    # then we let Faiss find the optimal parameters by itself
    print "exploring operating points"

    t0 = time.time()
    op = ps.explore(index, xq, crit)
    print "Done in %.3f s, available OPs:" % (time.time() - t0)

    # opv is a C++ vector, so it cannot be accessed like a Python array
    opv = op.optimal_pts
    print "%-40s  1-R@1     time" % "Parameters"
    for i in range(opv.size()):
        opt = opv.at(i)
        print "%-40s  %.4f  %7.3f" % (opt.key, opt.perf, opt.t)

else:

    # we do queries in a single thread
    faiss.omp_set_num_threads(1)

    print ' ' * len(parametersets[0]), '\t', 'R@1    R@10   R@100     time    %pass'

    for param in parametersets:
        print param, '\t',
        sys.stdout.flush()
        ps.set_index_parameters(index, param)
        t0 = time.time()
        ivfpq_stats.reset()
        D, I = index.search(xq, 100)
        t1 = time.time()
        for rank in 1, 10, 100:
            n_ok = (I[:, :rank] == gt[:, :1]).sum()
            print "%.4f" % (n_ok / float(nq)),
        print "%8.3f  " % ((t1 - t0) * 1000.0 / nq),
        print "%5.2f" % (ivfpq_stats.n_hamming_pass * 100.0 / ivfpq_stats.ncode)
