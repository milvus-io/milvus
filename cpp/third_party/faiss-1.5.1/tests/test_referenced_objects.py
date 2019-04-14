# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

"""make sure that the referenced objects are kept"""

import numpy as np
import unittest
import faiss
import sys
import gc

d = 10
xt = np.random.rand(100, d).astype('float32')
xb = np.random.rand(20, d).astype('float32')


class TestReferenced(unittest.TestCase):

    def test_IndexIVF(self):
        quantizer = faiss.IndexFlatL2(d)
        index = faiss.IndexIVFFlat(quantizer, d, 10)
        index.train(xt)
        index.add(xb)
        del quantizer
        gc.collect()
        index.add(xb)

    def test_count_refs(self):
        quantizer = faiss.IndexFlatL2(d)
        index = faiss.IndexIVFFlat(quantizer, d, 10)
        refc1 = sys.getrefcount(quantizer)
        del index
        gc.collect()
        refc2 = sys.getrefcount(quantizer)
        assert refc2 == refc1 - 1

    def test_IndexIVF_2(self):
        index = faiss.IndexIVFFlat(faiss.IndexFlatL2(d), d, 10)
        index.train(xt)
        index.add(xb)

    def test_IndexPreTransform(self):
        ltrans = faiss.NormalizationTransform(d)
        sub_index = faiss.IndexFlatL2(d)
        index = faiss.IndexPreTransform(ltrans, sub_index)
        index.add(xb)
        del ltrans
        gc.collect()
        index.add(xb)
        del sub_index
        gc.collect()
        index.add(xb)

    def test_IndexPreTransform_2(self):
        sub_index = faiss.IndexFlatL2(d)
        index = faiss.IndexPreTransform(sub_index)
        ltrans = faiss.NormalizationTransform(d)
        index.prepend_transform(ltrans)
        index.add(xb)
        del ltrans
        gc.collect()
        index.add(xb)
        del sub_index
        gc.collect()
        index.add(xb)

    def test_IDMap(self):
        sub_index = faiss.IndexFlatL2(d)
        index = faiss.IndexIDMap(sub_index)
        index.add_with_ids(xb, np.arange(len(xb)))
        del sub_index
        gc.collect()
        index.add_with_ids(xb, np.arange(len(xb)))

    def test_shards(self):
        index = faiss.IndexShards(d)
        for i in range(3):
            sub_index = faiss.IndexFlatL2(d)
            sub_index.add(xb)
            index.add_shard(sub_index)
        gc.collect()
        index.search(xb, 10)


dbin = 32
xtbin = np.random.randint(256, size=(100, int(dbin / 8))).astype('uint8')
xbbin = np.random.randint(256, size=(20, int(dbin / 8))).astype('uint8')


class TestReferencedBinary(unittest.TestCase):

    def test_binary_ivf(self):
        index = faiss.IndexBinaryIVF(faiss.IndexBinaryFlat(dbin), dbin, 10)
        gc.collect()
        index.train(xtbin)

    def test_wrap(self):
        index = faiss.IndexBinaryFromFloat(faiss.IndexFlatL2(dbin))
        gc.collect()
        index.add(xbbin)

if __name__ == '__main__':
    unittest.main()
