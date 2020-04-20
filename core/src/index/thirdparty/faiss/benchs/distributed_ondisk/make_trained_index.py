# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import numpy as np
import faiss

deep1bdir = "/datasets01_101/simsearch/041218/deep1b/"
workdir = "/checkpoint/matthijs/ondisk_distributed/"


print('Load centroids')
centroids = np.load(workdir + '1M_centroids.npy')
ncent, d = centroids.shape


print('apply random rotation')
rrot = faiss.RandomRotationMatrix(d, d)
rrot.init(1234)
centroids = rrot.apply_py(centroids)

print('make HNSW index as quantizer')
quantizer = faiss.IndexHNSWFlat(d, 32)
quantizer.hnsw.efSearch = 1024
quantizer.hnsw.efConstruction = 200
quantizer.add(centroids)

print('build index')
index = faiss.IndexPreTransform(
    rrot,
    faiss.IndexIVFScalarQuantizer(
        quantizer, d, ncent, faiss.ScalarQuantizer.QT_6bit
        )
    )

def ivecs_mmap(fname):
    a = np.memmap(fname, dtype='int32', mode='r')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:]

def fvecs_mmap(fname):
    return ivecs_mmap(fname).view('float32')


print('finish training index')
xt = fvecs_mmap(deep1bdir + 'learn.fvecs')
xt = np.ascontiguousarray(xt[:256 * 1000], dtype='float32')
index.train(xt)

print('write output')
faiss.write_index(index, workdir + 'trained.faissindex')
