# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python3
# this is a slow computation to test whether ctrl-C handling works
import faiss
import numpy as np

def test_slow():
    d = 256
    index = faiss.index_cpu_to_gpu(faiss.StandardGpuResources(),
                               0, faiss.IndexFlatL2(d))
    x = np.random.rand(10 ** 6, d).astype('float32')
    print('add')
    index.add(x)
    print('search')
    index.search(x, 10)
    print('done')


if __name__ == '__main__':
    test_slow()
