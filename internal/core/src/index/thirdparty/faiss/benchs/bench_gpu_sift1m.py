# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#!/usr/bin/env python2

from __future__ import print_function
import os
import time
import numpy as np
import pdb

import faiss
from datasets import load_sift1M, evaluate


print("load data")

xb, xq, xt, gt = load_sift1M()
nq, d = xq.shape

# we need only a StandardGpuResources per GPU
res = faiss.StandardGpuResources()


#################################################################
#  Exact search experiment
#################################################################

print("============ Exact search")

flat_config = faiss.GpuIndexFlatConfig()
flat_config.device = 0

index = faiss.GpuIndexFlatL2(res, d, flat_config)

print("add vectors to index")

index.add(xb)

print("warmup")

index.search(xq, 123)

print("benchmark")

for lk in range(11):
    k = 1 << lk
    t, r = evaluate(index, xq, gt, k)

    # the recall should be 1 at all times
    print("k=%d %.3f ms, R@1 %.4f" % (k, t, r[1]))


#################################################################
#  Approximate search experiment
#################################################################

print("============ Approximate search")

index = faiss.index_factory(d, "IVF4096,PQ64")

# faster, uses more memory
# index = faiss.index_factory(d, "IVF16384,Flat")

co = faiss.GpuClonerOptions()

# here we are using a 64-byte PQ, so we must set the lookup tables to
# 16 bit float (this is due to the limited temporary memory).
co.useFloat16 = True

index = faiss.index_cpu_to_gpu(res, 0, index, co)

print("train")

index.train(xt)

print("add vectors to index")

index.add(xb)

print("warmup")

index.search(xq, 123)

print("benchmark")

for lnprobe in range(10):
    nprobe = 1 << lnprobe
    index.setNumProbes(nprobe)
    t, r = evaluate(index, xq, gt, 100)

    print("nprobe=%4d %.3f ms recalls= %.4f %.4f %.4f" % (nprobe, t, r[1], r[10], r[100]))
