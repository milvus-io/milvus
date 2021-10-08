# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#!/usr/bin/env python2

from __future__ import print_function
import os
import numpy as np
import faiss
import argparse
import datasets
from datasets import sanitize

######################################################
# Command-line parsing
######################################################

parser = argparse.ArgumentParser()


def aa(*args, **kwargs):
    group.add_argument(*args, **kwargs)


group = parser.add_argument_group('dataset options')

aa('--db', default='deep1M', help='dataset')
aa('--nt', default=65536, type=int)
aa('--nb', default=100000, type=int)
aa('--nt_sample', default=0, type=int)

group = parser.add_argument_group('kmeans options')
aa('--k', default=256, type=int)
aa('--seed', default=12345, type=int)
aa('--pcadim', default=-1, type=int, help='PCA to this dimension')
aa('--niter', default=25, type=int)
aa('--eval_freq', default=100, type=int)


args = parser.parse_args()

print("args:", args)

os.system('echo -n "nb processors "; '
          'cat /proc/cpuinfo | grep ^processor | wc -l; '
          'cat /proc/cpuinfo | grep ^"model name" | tail -1')

ngpu = faiss.get_num_gpus()
print("nb GPUs:", ngpu)

######################################################
# Load dataset
######################################################

xt, xb, xq, gt = datasets.load_data(dataset=args.db)


if args.nt_sample == 0:
    xt_pca = xt[args.nt:args.nt + 10000]
    xt = xt[:args.nt]
else:
    xt_pca = xt[args.nt_sample:args.nt_sample + 10000]
    rs = np.random.RandomState(args.seed)
    idx = rs.choice(args.nt_sample, size=args.nt, replace=False)
    xt = xt[idx]

xb = xb[:args.nb]

d = xb.shape[1]

if args.pcadim != -1:
    print("training PCA: %d -> %d" % (d, args.pcadim))
    pca = faiss.PCAMatrix(d, args.pcadim)
    pca.train(sanitize(xt_pca))
    xt = pca.apply_py(sanitize(xt))
    xb = pca.apply_py(sanitize(xb))
    d = xb.shape[1]


######################################################
# Run clustering
######################################################


index = faiss.IndexFlatL2(d)

if ngpu > 0:
    print("moving index to GPU")
    index = faiss.index_cpu_to_all_gpus(index)


clustering = faiss.Clustering(d, args.k)

clustering.verbose = True
clustering.seed = args.seed
clustering.max_points_per_centroid = 10**6
clustering.min_points_per_centroid = 1


for iter0 in range(0, args.niter, args.eval_freq):
    iter1 = min(args.niter, iter0 + args.eval_freq)
    clustering.niter = iter1 - iter0

    if iter0 > 0:
        faiss.copy_array_to_vector(centroids.ravel(), clustering.centroids)

    clustering.train(sanitize(xt), index)
    index.reset()
    centroids = faiss.vector_to_array(clustering.centroids).reshape(args.k, d)
    index.add(centroids)

    _, I = index.search(sanitize(xb), 1)

    error = ((xb - centroids[I.ravel()]) ** 2).sum()

    print("iter1=%d quantization error on test: %.4f" % (iter1, error))
