# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#!/usr/bin/env python2

from __future__ import print_function
import time
import sys
import numpy as np
import faiss
from datasets import load_sift1M


k = int(sys.argv[1])
todo = sys.argv[1:]

print("load data")
xb, xq, xt, gt = load_sift1M()
nq, d = xq.shape

if todo == []:
    todo = 'hnsw hnsw_sq ivf ivf_hnsw_quantizer kmeans kmeans_hnsw'.split()


def evaluate(index):
    # for timing with a single core
    # faiss.omp_set_num_threads(1)

    t0 = time.time()
    D, I = index.search(xq, k)
    t1 = time.time()

    missing_rate = (I == -1).sum() / float(k * nq)
    recall_at_1 = (I == gt[:, :1]).sum() / float(nq)
    print("\t %7.3f ms per query, R@1 %.4f, missing rate %.4f" % (
        (t1 - t0) * 1000.0 / nq, recall_at_1, missing_rate))


if 'hnsw' in todo:

    print("Testing HNSW Flat")

    index = faiss.IndexHNSWFlat(d, 32)

    # training is not needed

    # this is the default, higher is more accurate and slower to
    # construct
    index.hnsw.efConstruction = 40

    print("add")
    # to see progress
    index.verbose = True
    index.add(xb)

    print("search")
    for efSearch in 16, 32, 64, 128, 256:
        for bounded_queue in [True, False]:
            print("efSearch", efSearch, "bounded queue", bounded_queue, end=' ')
            index.hnsw.search_bounded_queue = bounded_queue
            index.hnsw.efSearch = efSearch
            evaluate(index)

if 'hnsw_sq' in todo:

    print("Testing HNSW with a scalar quantizer")
    # also set M so that the vectors and links both use 128 bytes per
    # entry (total 256 bytes)
    index = faiss.IndexHNSWSQ(d, faiss.ScalarQuantizer.QT_8bit, 16)

    print("training")
    # training for the scalar quantizer
    index.train(xt)

    # this is the default, higher is more accurate and slower to
    # construct
    index.hnsw.efConstruction = 40

    print("add")
    # to see progress
    index.verbose = True
    index.add(xb)

    print("search")
    for efSearch in 16, 32, 64, 128, 256:
        print("efSearch", efSearch, end=' ')
        index.hnsw.efSearch = efSearch
        evaluate(index)

if 'ivf' in todo:

    print("Testing IVF Flat (baseline)")
    quantizer = faiss.IndexFlatL2(d)
    index = faiss.IndexIVFFlat(quantizer, d, 16384)
    index.cp.min_points_per_centroid = 5   # quiet warning

    # to see progress
    index.verbose = True

    print("training")
    index.train(xt)

    print("add")
    index.add(xb)

    print("search")
    for nprobe in 1, 4, 16, 64, 256:
        print("nprobe", nprobe, end=' ')
        index.nprobe = nprobe
        evaluate(index)

if 'ivf_hnsw_quantizer' in todo:

    print("Testing IVF Flat with HNSW quantizer")
    quantizer = faiss.IndexHNSWFlat(d, 32)
    index = faiss.IndexIVFFlat(quantizer, d, 16384)
    index.cp.min_points_per_centroid = 5   # quiet warning
    index.quantizer_trains_alone = 2

    # to see progress
    index.verbose = True

    print("training")
    index.train(xt)

    print("add")
    index.add(xb)

    print("search")
    quantizer.hnsw.efSearch = 64
    for nprobe in 1, 4, 16, 64, 256:
        print("nprobe", nprobe, end=' ')
        index.nprobe = nprobe
        evaluate(index)

# Bonus: 2 kmeans tests

if 'kmeans' in todo:
    print("Performing kmeans on sift1M database vectors (baseline)")
    clus = faiss.Clustering(d, 16384)
    clus.verbose = True
    clus.niter = 10
    index = faiss.IndexFlatL2(d)
    clus.train(xb, index)


if 'kmeans_hnsw' in todo:
    print("Performing kmeans on sift1M using HNSW assignment")
    clus = faiss.Clustering(d, 16384)
    clus.verbose = True
    clus.niter = 10
    index = faiss.IndexHNSWFlat(d, 32)
    # increase the default efSearch, otherwise the number of empty
    # clusters is too high.
    index.hnsw.efSearch = 128
    clus.train(xb, index)
