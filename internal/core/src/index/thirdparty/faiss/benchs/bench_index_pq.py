# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import print_function
import faiss
from datasets import load_sift1M, evaluate

xb, xq, xt, gt = load_sift1M()
nq, d = xq.shape

k = 32

for nbits in 4, 6, 8, 10, 12:
    index = faiss.IndexPQ(d, 8, nbits)
    index.train(xt)
    index.add(xb)

    t, r = evaluate(index, xq, gt, k)
    print("\t %7.3f ms per query, R@1 %.4f" % (t, r[1]))
    del index
