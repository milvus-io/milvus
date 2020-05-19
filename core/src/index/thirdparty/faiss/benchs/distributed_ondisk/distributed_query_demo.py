# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import os
import faiss
import numpy as np
import time
import rpc
import sys

import combined_index
import search_server

hostnames = sys.argv[1:]

print("Load local index")
ci = combined_index.CombinedIndexDeep1B()

print("connect to clients")
clients = []
for host in hostnames:
    client = rpc.Client(host, 12012, v6=False)
    clients.append(client)

# check if all servers respond
print("sizes seen by servers:", [cl.get_ntotal() for cl in clients])


# aggregate all clients into a one that uses them all for speed
# note that it also requires a local index ci
sindex = search_server.SplitPerListIndex(ci, clients)
sindex.verbose = True

# set reasonable parameters
ci.set_parallel_mode(1)
ci.set_prefetch_nthread(0)
ci.set_omp_num_threads(64)

# initialize params
sindex.set_parallel_mode(1)
sindex.set_prefetch_nthread(0)
sindex.set_omp_num_threads(64)

def ivecs_read(fname):
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()

def fvecs_read(fname):
    return ivecs_read(fname).view('float32')


deep1bdir = "/datasets01_101/simsearch/041218/deep1b/"

xq = fvecs_read(deep1bdir + "deep1B_queries.fvecs")
gt_fname = deep1bdir + "deep1B_groundtruth.ivecs"
gt = ivecs_read(gt_fname)


for nprobe in 1, 10, 100, 1000:
    sindex.set_nprobe(nprobe)
    t0 = time.time()
    D, I = sindex.search(xq, 100)
    t1 = time.time()
    print('nprobe=%d 1-recall@1=%.4f t=%.2fs' % (
        nprobe, (I[:, 0] == gt[:, 0]).sum() / len(xq),
        t1 - t0
    ))
