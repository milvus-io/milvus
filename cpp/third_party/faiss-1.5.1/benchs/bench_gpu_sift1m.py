# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#!/usr/bin/env python2

import os
import time
import numpy as np
import pdb

import faiss

#################################################################
# I/O functions
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

# we need only a StandardGpuResources per GPU
res = faiss.StandardGpuResources()


#################################################################
#  Exact search experiment
#################################################################

print "============ Exact search"

flat_config = faiss.GpuIndexFlatConfig()
flat_config.device = 0

index = faiss.GpuIndexFlatL2(res, d, flat_config)

print "add vectors to index"

index.add(xb)

print "warmup"

index.search(xq, 123)

print "benchmark"

for lk in range(11):
    k = 1 << lk
    t0 = time.time()
    D, I = index.search(xq, k)
    t1 = time.time()

    # the recall should be 1 at all times
    recall_at_1 = (I[:, :1] == gt[:, :1]).sum() / float(nq)
    print "k=%d %.3f s, R@1 %.4f" % (
        k, t1 - t0, recall_at_1)


#################################################################
#  Approximate search experiment
#################################################################

print "============ Approximate search"

index = faiss.index_factory(d, "IVF4096,PQ64")

# faster, uses more memory
# index = faiss.index_factory(d, "IVF16384,Flat")

co = faiss.GpuClonerOptions()

# here we are using a 64-byte PQ, so we must set the lookup tables to
# 16 bit float (this is due to the limited temporary memory).
co.useFloat16 = True

index = faiss.index_cpu_to_gpu(res, 0, index, co)

print "train"

index.train(xt)

print "add vectors to index"

index.add(xb)

print "warmup"

index.search(xq, 123)

print "benchmark"

for lnprobe in range(10):
    nprobe = 1 << lnprobe
    index.setNumProbes(nprobe)
    t0 = time.time()
    D, I = index.search(xq, 100)
    t1 = time.time()

    print "nprobe=%4d %.3f s recalls=" % (nprobe, t1 - t0),
    for rank in 1, 10, 100:
        n_ok = (I[:, :rank] == gt[:, :1]).sum()
        print "%.4f" % (n_ok / float(nq)),
    print
