# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#!/usr/bin/env python2

import time
import numpy as np

import faiss

#################################################################
# Small I/O functions
#################################################################


def ivecs_read(fname):
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def fvecs_read(fname):
    return ivecs_read(fname).view('float32')


#################################################################
#  Main program
#################################################################

print "load data"

xt = fvecs_read("sift1M/sift_learn.fvecs")
xb = fvecs_read("sift1M/sift_base.fvecs")
xq = fvecs_read("sift1M/sift_query.fvecs")

nq, d = xq.shape

print "load GT"

gt = ivecs_read("sift1M/sift_groundtruth.ivecs")


# index with 16 subquantizers, 8 bit each
index = faiss.IndexPQ(d, 16, 8)
index.do_polysemous_training = True
index.verbose = True

print "train"

index.train(xt)

print "add vectors to index"

index.add(xb)

nt = 1
faiss.omp_set_num_threads(1)


def evaluate():
    t0 = time.time()
    D, I = index.search(xq, 1)
    t1 = time.time()

    recall_at_1 = (I == gt[:, :1]).sum() / float(nq)
    print "\t %7.3f ms per query, R@1 %.4f" % (
        (t1 - t0) * 1000.0 / nq * nt, recall_at_1)


print "PQ baseline",
index.search_type = faiss.IndexPQ.ST_PQ
evaluate()

for ht in 64, 62, 58, 54, 50, 46, 42, 38, 34, 30:
    print "Polysemous", ht,
    index.search_type = faiss.IndexPQ.ST_polysemous
    index.polysemous_ht = ht
    evaluate()
