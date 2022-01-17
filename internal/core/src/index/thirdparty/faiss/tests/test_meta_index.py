# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import absolute_import, division, print_function, unicode_literals

# translation of test_meta_index.lua

import numpy as np
import faiss
import unittest

from common import Randu10k

ru = Randu10k()

xb = ru.xb
xt = ru.xt
xq = ru.xq
nb, d = xb.shape
nq, d = xq.shape


class IDRemap(unittest.TestCase):

    def test_id_remap_idmap(self):
        # reference: index without remapping

        index = faiss.IndexPQ(d, 8, 8)
        k = 10
        index.train(xt)
        index.add(xb)
        _Dref, Iref = index.search(xq, k)

        # try a remapping
        ids = np.arange(nb)[::-1].copy()

        sub_index = faiss.IndexPQ(d, 8, 8)
        index2 = faiss.IndexIDMap(sub_index)

        index2.train(xt)
        index2.add_with_ids(xb, ids)

        _D, I = index2.search(xq, k)

        assert np.all(I == nb - 1 - Iref)

    def test_id_remap_ivf(self):
        # coarse quantizer in common
        coarse_quantizer = faiss.IndexFlatIP(d)
        ncentroids = 25

        # reference: index without remapping

        index = faiss.IndexIVFPQ(coarse_quantizer, d,
                                        ncentroids, 8, 8)
        index.nprobe = 5
        k = 10
        index.train(xt)
        index.add(xb)
        _Dref, Iref = index.search(xq, k)

        # try a remapping
        ids = np.arange(nb)[::-1].copy()

        index2 = faiss.IndexIVFPQ(coarse_quantizer, d,
                                        ncentroids, 8, 8)
        index2.nprobe = 5

        index2.train(xt)
        index2.add_with_ids(xb, ids)

        _D, I = index2.search(xq, k)
        assert np.all(I == nb - 1 - Iref)


class Shards(unittest.TestCase):

    def test_shards(self):
        k = 32
        ref_index = faiss.IndexFlatL2(d)

        print('ref search')
        ref_index.add(xb)
        _Dref, Iref = ref_index.search(xq, k)
        print(Iref[:5, :6])

        shard_index = faiss.IndexShards(d)
        shard_index_2 = faiss.IndexShards(d, True, False)

        ni = 3
        for i in range(ni):
            i0 = int(i * nb / ni)
            i1 = int((i + 1) * nb / ni)
            index = faiss.IndexFlatL2(d)
            index.add(xb[i0:i1])
            shard_index.add_shard(index)

            index_2 = faiss.IndexFlatL2(d)
            irm = faiss.IndexIDMap(index_2)
            shard_index_2.add_shard(irm)

        # test parallel add
        shard_index_2.verbose = True
        shard_index_2.add(xb)

        for test_no in range(3):
            with_threads = test_no == 1

            print('shard search test_no = %d' % test_no)
            if with_threads:
                remember_nt = faiss.omp_get_max_threads()
                faiss.omp_set_num_threads(1)
                shard_index.threaded = True
            else:
                shard_index.threaded = False

            if test_no != 2:
                _D, I = shard_index.search(xq, k)
            else:
                _D, I = shard_index_2.search(xq, k)

            print(I[:5, :6])

            if with_threads:
                faiss.omp_set_num_threads(remember_nt)

            ndiff = (I != Iref).sum()

            print('%d / %d differences' % (ndiff, nq * k))
            assert(ndiff < nq * k / 1000.)


class Merge(unittest.TestCase):

    def make_index_for_merge(self, quant, index_type, master_index):
        ncent = 40
        if index_type == 1:
            index = faiss.IndexIVFFlat(quant, d, ncent, faiss.METRIC_L2)
            if master_index:
                index.is_trained = True
        elif index_type == 2:
            index = faiss.IndexIVFPQ(quant, d, ncent, 4, 8)
            if master_index:
                index.pq = master_index.pq
                index.is_trained = True
        elif index_type == 3:
            index = faiss.IndexIVFPQR(quant, d, ncent, 4, 8, 8, 8)
            if master_index:
                index.pq = master_index.pq
                index.refine_pq = master_index.refine_pq
                index.is_trained = True
        elif index_type == 4:
            # quant used as the actual index
            index = faiss.IndexIDMap(quant)
        return index

    def do_test_merge(self, index_type):
        k = 16
        quant = faiss.IndexFlatL2(d)
        ref_index = self.make_index_for_merge(quant, index_type, False)

        # trains the quantizer
        ref_index.train(xt)

        print('ref search')
        ref_index.add(xb)
        _Dref, Iref = ref_index.search(xq, k)
        print(Iref[:5, :6])

        indexes = []
        ni = 3
        for i in range(ni):
            i0 = int(i * nb / ni)
            i1 = int((i + 1) * nb / ni)
            index = self.make_index_for_merge(quant, index_type, ref_index)
            index.is_trained = True
            index.add(xb[i0:i1])
            indexes.append(index)

        index = indexes[0]

        for i in range(1, ni):
            print('merge ntotal=%d other.ntotal=%d ' % (
                index.ntotal, indexes[i].ntotal))
            index.merge_from(indexes[i], index.ntotal)

        _D, I = index.search(xq, k)
        print(I[:5, :6])

        ndiff = (I != Iref).sum()
        print('%d / %d differences' % (ndiff, nq * k))
        assert(ndiff < nq * k / 1000.)

    def test_merge(self):
        self.do_test_merge(1)
        self.do_test_merge(2)
        self.do_test_merge(3)

    def do_test_remove(self, index_type):
        k = 16
        quant = faiss.IndexFlatL2(d)
        index = self.make_index_for_merge(quant, index_type, None)

        # trains the quantizer
        index.train(xt)

        if index_type < 4:
            index.add(xb)
        else:
            gen = np.random.RandomState(1234)
            id_list = gen.permutation(nb * 7)[:nb]
            index.add_with_ids(xb, id_list)


        print('ref search ntotal=%d' % index.ntotal)
        Dref, Iref = index.search(xq, k)

        toremove = np.zeros(nq * k, dtype=int)
        nr = 0
        for i in range(nq):
            for j in range(k):
                # remove all even results (it's ok if there are duplicates
                # in the list of ids)
                if Iref[i, j] % 2 == 0:
                    nr = nr + 1
                    toremove[nr] = Iref[i, j]

        print('nr=', nr)

        idsel = faiss.IDSelectorBatch(
            nr, faiss.swig_ptr(toremove))

        for i in range(nr):
            assert(idsel.is_member(int(toremove[i])))

        nremoved = index.remove_ids(idsel)

        print('nremoved=%d ntotal=%d' % (nremoved, index.ntotal))

        D, I = index.search(xq, k)

        # make sure results are in the same order with even ones removed
        for i in range(nq):
            j2 = 0
            for j in range(k):
                if Iref[i, j] % 2 != 0:
                    assert I[i, j2] == Iref[i, j]
                    assert abs(D[i, j2] - Dref[i, j]) < 1e-5
                    j2 += 1

    def test_remove(self):
        self.do_test_remove(1)
        self.do_test_remove(2)
        self.do_test_remove(4)






if __name__ == '__main__':
    unittest.main()
