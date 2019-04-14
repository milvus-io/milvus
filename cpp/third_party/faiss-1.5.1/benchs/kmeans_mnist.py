# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

import numpy as np
import time
import faiss
import sys


# Get command-line arguments

k = int(sys.argv[1])
ngpu = int(sys.argv[2])

# Load Leon's file format

def load_mnist(fname):
    print "load", fname
    f = open(fname)

    header = np.fromfile(f, dtype='int8', count=4*4)
    header = header.reshape(4, 4)[:, ::-1].copy().view('int32')
    print header
    nim, xd, yd = [int(x) for x in header[1:]]

    data = np.fromfile(f, count=nim * xd * yd,
                       dtype='uint8')

    print data.shape, nim, xd, yd
    data = data.reshape(nim, xd, yd)
    return data

x = load_mnist(basedir + 'mnist8m/mnist8m-patterns-idx3-ubyte')

print "reshape"

x = x.reshape(x.shape[0], -1).astype('float32')


def train_kmeans(x, k, ngpu):
    "Runs kmeans on one or several GPUs"
    d = x.shape[1]
    clus = faiss.Clustering(d, k)
    clus.verbose = True
    clus.niter = 20

    # otherwise the kmeans implementation sub-samples the training set
    clus.max_points_per_centroid = 10000000

    res = [faiss.StandardGpuResources() for i in range(ngpu)]

    flat_config = []
    for i in range(ngpu):
        cfg = faiss.GpuIndexFlatConfig()
        cfg.useFloat16 = False
        cfg.device = i
        flat_config.append(cfg)

    if ngpu == 1:
        index = faiss.GpuIndexFlatL2(res[0], d, flat_config[0])
    else:
        indexes = [faiss.GpuIndexFlatL2(res[i], d, flat_config[i])
                   for i in range(ngpu)]
        index = faiss.IndexReplicas()
        for sub_index in indexes:
            index.addIndex(sub_index)

    # perform the training
    clus.train(x, index)
    centroids = faiss.vector_float_to_array(clus.centroids)

    obj = faiss.vector_float_to_array(clus.obj)
    print "final objective: %.4g" % obj[-1]

    return centroids.reshape(k, d)

print "run"
t0 = time.time()
train_kmeans(x, k, ngpu)
t1 = time.time()

print "total runtime: %.3f s" % (t1 - t0)
