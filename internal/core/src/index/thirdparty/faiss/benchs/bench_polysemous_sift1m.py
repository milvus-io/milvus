# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#!/usr/bin/env python2

from __future__ import print_function
import time
import numpy as np

import faiss
from datasets import load_sift1M, evaluate


print("load data")
xb, xq, xt, gt = load_sift1M()
nq, d = xq.shape

# index with 16 subquantizers, 8 bit each
index = faiss.IndexPQ(d, 16, 8)
index.do_polysemous_training = True
index.verbose = True

print("train")

index.train(xt)

print("add vectors to index")

index.add(xb)

nt = 1
faiss.omp_set_num_threads(1)


print("PQ baseline", end=' ')
index.search_type = faiss.IndexPQ.ST_PQ
t, r = evaluate(index, xq, gt, 1)
print("\t %7.3f ms per query, R@1 %.4f" % (t, r[1]))

for ht in 64, 62, 58, 54, 50, 46, 42, 38, 34, 30:
    print("Polysemous", ht, end=' ')
    index.search_type = faiss.IndexPQ.ST_polysemous
    index.polysemous_ht = ht
    t, r = evaluate(index, xq, gt, 1)
    print("\t %7.3f ms per query, R@1 %.4f" % (t, r[1]))
