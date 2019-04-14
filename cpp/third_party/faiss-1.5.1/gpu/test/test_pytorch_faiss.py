# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

import numpy as np
import unittest
import faiss
import torch

def swig_ptr_from_FloatTensor(x):
    assert x.is_contiguous()
    assert x.dtype == torch.float32
    return faiss.cast_integer_to_float_ptr(x.storage().data_ptr())

def swig_ptr_from_LongTensor(x):
    assert x.is_contiguous()
    assert x.dtype == torch.int64, 'dtype=%s' % x.dtype
    return faiss.cast_integer_to_long_ptr(x.storage().data_ptr())



def search_index_pytorch(index, x, k, D=None, I=None):
    """call the search function of an index with pytorch tensor I/O (CPU
    and GPU supported)"""
    assert x.is_contiguous()
    n, d = x.size()
    assert d == index.d

    if D is None:
        D = torch.empty((n, k), dtype=torch.float32, device=x.device)
    else:
        assert D.size() == (n, k)

    if I is None:
        I = torch.empty((n, k), dtype=torch.int64, device=x.device)
    else:
        assert I.size() == (n, k)
    torch.cuda.synchronize()
    xptr = swig_ptr_from_FloatTensor(x)
    Iptr = swig_ptr_from_LongTensor(I)
    Dptr = swig_ptr_from_FloatTensor(D)
    index.search_c(n, xptr,
                   k, Dptr, Iptr)
    torch.cuda.synchronize()
    return D, I


def search_raw_array_pytorch(res, xb, xq, k, D=None, I=None,
                             metric=faiss.METRIC_L2):
    assert xb.device == xq.device

    xq_ptr = swig_ptr_from_FloatTensor(xq)
    nq, d = xq.size()

    xb_ptr = swig_ptr_from_FloatTensor(xb)
    nb, d2 = xb.size()
    assert d2 == d

    if D is None:
        D = torch.empty(nq, k, device=xb.device, dtype=torch.float32)
    else:
        assert D.shape == (nq, k)
        assert D.device == xb.device

    if I is None:
        I = torch.empty(nq, k, device=xb.device, dtype=torch.int64)
    else:
        assert I.shape == (nq, k)
        assert I.device == xb.device

    D_ptr = swig_ptr_from_FloatTensor(D)
    I_ptr = swig_ptr_from_LongTensor(I)

    faiss.bruteForceKnn(res, metric,
                        xb_ptr, nb,
                        xq_ptr, nq,
                        d, k, D_ptr, I_ptr)

    return D, I


class PytorchFaissInterop(unittest.TestCase):

    def test_interop(self):

        d = 16
        nq = 5
        nb = 20

        xq = faiss.randn(nq * d, 1234).reshape(nq, d)
        xb = faiss.randn(nb * d, 1235).reshape(nb, d)

        res = faiss.StandardGpuResources()
        index = faiss.GpuIndexFlatIP(res, d)
        index.add(xb)

        # reference CPU result
        Dref, Iref = index.search(xq, 5)

        # query is pytorch tensor (CPU)
        xq_torch = torch.FloatTensor(xq)

        D2, I2 = search_index_pytorch(index, xq_torch, 5)

        assert np.all(Iref == I2.numpy())

        # query is pytorch tensor (GPU)
        xq_torch = xq_torch.cuda()
        # no need for a sync here

        D3, I3 = search_index_pytorch(index, xq_torch, 5)

        # D3 and I3 are on torch tensors on GPU as well.
        # this does a sync, which is useful because faiss and
        # pytorch use different Cuda streams.
        res.syncDefaultStreamCurrentDevice()

        assert np.all(Iref == I3.cpu().numpy())

    def test_raw_array_search(self):
        d = 32
        nb = 1024
        nq = 128
        k = 10

        # make GT on Faiss CPU

        xq = faiss.randn(nq * d, 1234).reshape(nq, d)
        xb = faiss.randn(nb * d, 1235).reshape(nb, d)

        index = faiss.IndexFlatL2(d)
        index.add(xb)
        gt_D, gt_I = index.search(xq, k)

        # move to pytorch & GPU
        xq_t = torch.from_numpy(xq).cuda()
        xb_t = torch.from_numpy(xb).cuda()

        # resource object, can be re-used over calls
        res = faiss.StandardGpuResources()

        # put on same stream as pytorch to avoid synchronizing streams
        res.setDefaultNullStreamAllDevices()

        D, I = search_raw_array_pytorch(res, xb_t, xq_t, k)

        # back to CPU for verification
        D = D.cpu().numpy()
        I = I.cpu().numpy()

        assert np.all(I == gt_I)
        assert np.all(np.abs(D - gt_D).max() < 1e-4)







if __name__ == '__main__':
    unittest.main()
