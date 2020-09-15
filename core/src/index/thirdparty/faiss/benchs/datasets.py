# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import print_function
import sys
import time
import numpy as np


def ivecs_read(fname):
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def fvecs_read(fname):
    return ivecs_read(fname).view('float32')


def load_sift1M():
    print("Loading sift1M...", end='', file=sys.stderr)
    xt = fvecs_read("sift1M/sift_learn.fvecs")
    xb = fvecs_read("sift1M/sift_base.fvecs")
    xq = fvecs_read("sift1M/sift_query.fvecs")
    gt = ivecs_read("sift1M/sift_groundtruth.ivecs")
    print("done", file=sys.stderr)

    return xb, xq, xt, gt


def evaluate(index, xq, gt, k):
    nq = xq.shape[0]
    t0 = time.time()
    D, I = index.search(xq, k)  # noqa: E741
    t1 = time.time()

    recalls = {}
    i = 1
    while i <= k:
        recalls[i] = (I[:, :i] == gt[:, :1]).sum() / float(nq)
        i *= 10

    return (t1 - t0) * 1000.0 / nq, recalls
