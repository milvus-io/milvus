# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import numpy as np

d = 64                           # dimension
nb = 100000                      # database size
nq = 10000                       # nb of queries
np.random.seed(1234)             # make reproducible
xb = np.random.random((nb, d)).astype('float32')
xb[:, 0] += np.arange(nb) / 1000.
xq = np.random.random((nq, d)).astype('float32')
xq[:, 0] += np.arange(nq) / 1000.

import faiss                     # make faiss available

res = faiss.StandardGpuResources()  # use a single GPU

## Using a flat index

index_flat = faiss.IndexFlatL2(d)  # build a flat (CPU) index

# make it a flat GPU index
gpu_index_flat = faiss.index_cpu_to_gpu(res, 0, index_flat)

gpu_index_flat.add(xb)         # add vectors to the index
print(gpu_index_flat.ntotal)

k = 4                          # we want to see 4 nearest neighbors
D, I = gpu_index_flat.search(xq, k)  # actual search
print(I[:5])                   # neighbors of the 5 first queries
print(I[-5:])                  # neighbors of the 5 last queries


## Using an IVF index

nlist = 100
quantizer = faiss.IndexFlatL2(d)  # the other index
index_ivf = faiss.IndexIVFFlat(quantizer, d, nlist, faiss.METRIC_L2)
# here we specify METRIC_L2, by default it performs inner-product search

# make it an IVF GPU index
gpu_index_ivf = faiss.index_cpu_to_gpu(res, 0, index_ivf)

assert not gpu_index_ivf.is_trained
gpu_index_ivf.train(xb)        # add vectors to the index
assert gpu_index_ivf.is_trained

gpu_index_ivf.add(xb)          # add vectors to the index
print(gpu_index_ivf.ntotal)

k = 4                          # we want to see 4 nearest neighbors
D, I = gpu_index_ivf.search(xq, k)  # actual search
print(I[:5])                   # neighbors of the 5 first queries
print(I[-5:])                  # neighbors of the 5 last queries
