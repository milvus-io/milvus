# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python3

"""small test script to benchmark the SIMD implementation of the
distance computations for the additional metrics. Call eg. with L1 to
get L1 distance computations.
"""

import faiss

import sys
import time

d = 64
nq = 4096
nb = 16384

print("sample")

xq = faiss.randn((nq, d), 123)
xb = faiss.randn((nb, d), 123)

mt_name = "L2" if len(sys.argv) < 2 else sys.argv[1]

mt = getattr(faiss, "METRIC_" + mt_name)

print("distances")
t0 = time.time()
dis = faiss.pairwise_distances(xq, xb, mt)
t1 = time.time()

print("nq=%d nb=%d d=%d %s: %.3f s" % (nq, nb, d, mt_name, t1 - t0))
