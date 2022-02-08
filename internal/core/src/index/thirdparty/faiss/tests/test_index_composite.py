# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

""" more elaborate that test_index.py """
from __future__ import absolute_import, division, print_function

import numpy as np
import unittest
import faiss
import os
import shutil
import tempfile

from common import get_dataset_2

class TestRemove(unittest.TestCase):

    def do_merge_then_remove(self, ondisk):
        d = 10
        nb = 1000
        nq = 200
        nt = 200

        xt, xb, xq = get_dataset_2(d, nt, nb, nq)

        quantizer = faiss.IndexFlatL2(d)

        index1 = faiss.IndexIVFFlat(quantizer, d, 20)
        index1.train(xt)

        filename = None
        if ondisk:
            filename = tempfile.mkstemp()[1]
            invlists = faiss.OnDiskInvertedLists(
                index1.nlist, index1.code_size,
                filename)
            index1.replace_invlists(invlists)

        index1.add(xb[:int(nb / 2)])

        index2 = faiss.IndexIVFFlat(quantizer, d, 20)
        assert index2.is_trained
        index2.add(xb[int(nb / 2):])

        Dref, Iref = index1.search(xq, 10)
        index1.merge_from(index2, int(nb / 2))

        assert index1.ntotal == nb

        index1.remove_ids(faiss.IDSelectorRange(int(nb / 2), nb))

        assert index1.ntotal == int(nb / 2)
        Dnew, Inew = index1.search(xq, 10)

        assert np.all(Dnew == Dref)
        assert np.all(Inew == Iref)

        if filename is not None:
            os.unlink(filename)

    def test_remove_regular(self):
        self.do_merge_then_remove(False)

    def test_remove_ondisk(self):
        self.do_merge_then_remove(True)

    def test_remove(self):
        # only tests the python interface

        index = faiss.IndexFlat(5)
        xb = np.zeros((10, 5), dtype='float32')
        xb[:, 0] = np.arange(10) + 1000
        index.add(xb)
        index.remove_ids(np.arange(5) * 2)
        xb2 = faiss.vector_float_to_array(index.xb).reshape(5, 5)
        assert np.all(xb2[:, 0] == xb[np.arange(5) * 2 + 1, 0])

    def test_remove_id_map(self):
        sub_index = faiss.IndexFlat(5)
        xb = np.zeros((10, 5), dtype='float32')
        xb[:, 0] = np.arange(10) + 1000
        index = faiss.IndexIDMap2(sub_index)
        index.add_with_ids(xb, np.arange(10) + 100)
        assert index.reconstruct(104)[0] == 1004
        index.remove_ids(np.array([103]))
        assert index.reconstruct(104)[0] == 1004
        try:
            index.reconstruct(103)
        except RuntimeError:
            pass
        else:
            assert False, 'should have raised an exception'

    def test_remove_id_map_2(self):
        # from https://github.com/facebookresearch/faiss/issues/255
        rs = np.random.RandomState(1234)
        X = rs.randn(10, 10).astype(np.float32)
        idx = np.array([0, 10, 20, 30, 40, 5, 15, 25, 35, 45], np.int64)
        remove_set = np.array([10, 30], dtype=np.int64)
        index = faiss.index_factory(10, 'IDMap,Flat')
        index.add_with_ids(X[:5, :], idx[:5])
        index.remove_ids(remove_set)
        index.add_with_ids(X[5:, :], idx[5:])

        print (index.search(X, 1))

        for i in range(10):
            _, searchres = index.search(X[i:i + 1, :], 1)
            if idx[i] in remove_set:
                assert searchres[0] != idx[i]
            else:
                assert searchres[0] == idx[i]

    def test_remove_id_map_binary(self):
        sub_index = faiss.IndexBinaryFlat(40)
        xb = np.zeros((10, 5), dtype='uint8')
        xb[:, 0] = np.arange(10) + 100
        index = faiss.IndexBinaryIDMap2(sub_index)
        index.add_with_ids(xb, np.arange(10) + 1000)
        assert index.reconstruct(1004)[0] == 104
        index.remove_ids(np.array([1003]))
        assert index.reconstruct(1004)[0] == 104
        try:
            index.reconstruct(1003)
        except RuntimeError:
            pass
        else:
            assert False, 'should have raised an exception'

        # while we are there, let's test I/O as well...
        _, tmpnam = tempfile.mkstemp()
        try:
            faiss.write_index_binary(index, tmpnam)
            index = faiss.read_index_binary(tmpnam)
        finally:
            os.remove(tmpnam)

        assert index.reconstruct(1004)[0] == 104
        try:
            index.reconstruct(1003)
        except RuntimeError:
            pass
        else:
            assert False, 'should have raised an exception'



class TestRangeSearch(unittest.TestCase):

    def test_range_search_id_map(self):
        sub_index = faiss.IndexFlat(5, 1)  # L2 search instead of inner product
        xb = np.zeros((10, 5), dtype='float32')
        xb[:, 0] = np.arange(10) + 1000
        index = faiss.IndexIDMap2(sub_index)
        index.add_with_ids(xb, np.arange(10) + 100)
        dist = float(np.linalg.norm(xb[3] - xb[0])) * 0.99
        res_subindex = sub_index.range_search(xb[[0], :], dist)
        res_index = index.range_search(xb[[0], :], dist)
        assert len(res_subindex[2]) == 2
        np.testing.assert_array_equal(res_subindex[2] + 100, res_index[2])


class TestUpdate(unittest.TestCase):

    def test_update(self):
        d = 64
        nb = 1000
        nt = 1500
        nq = 100
        np.random.seed(123)
        xb = np.random.random(size=(nb, d)).astype('float32')
        xt = np.random.random(size=(nt, d)).astype('float32')
        xq = np.random.random(size=(nq, d)).astype('float32')

        index = faiss.index_factory(d, "IVF64,Flat")
        index.train(xt)
        index.add(xb)
        index.nprobe = 32
        D, I = index.search(xq, 5)

        index.make_direct_map()
        recons_before = np.vstack([index.reconstruct(i) for i in range(nb)])

        # revert order of the 200 first vectors
        nu = 200
        index.update_vectors(np.arange(nu), xb[nu - 1::-1].copy())

        recons_after = np.vstack([index.reconstruct(i) for i in range(nb)])

        # make sure reconstructions remain the same
        diff_recons = recons_before[:nu] - recons_after[nu - 1::-1]
        assert np.abs(diff_recons).max() == 0

        D2, I2 = index.search(xq, 5)

        assert np.all(D == D2)

        gt_map = np.arange(nb)
        gt_map[:nu] = np.arange(nu, 0, -1) - 1
        eqs = I.ravel() == gt_map[I2.ravel()]

        assert np.all(eqs)


class TestPCAWhite(unittest.TestCase):

    def test_white(self):

        # generate data
        d = 4
        nt = 1000
        nb = 200
        nq = 200

        # normal distribition
        x = faiss.randn((nt + nb + nq) * d, 1234).reshape(nt + nb + nq, d)

        index = faiss.index_factory(d, 'Flat')

        xt = x[:nt]
        xb = x[nt:-nq]
        xq = x[-nq:]

        # NN search on normal distribution
        index.add(xb)
        Do, Io = index.search(xq, 5)

        # make distribution very skewed
        x *= [10, 4, 1, 0.5]
        rr, _ = np.linalg.qr(faiss.randn(d * d).reshape(d, d))
        x = np.dot(x, rr).astype('float32')

        xt = x[:nt]
        xb = x[nt:-nq]
        xq = x[-nq:]

        # L2 search on skewed distribution
        index = faiss.index_factory(d, 'Flat')

        index.add(xb)
        Dl2, Il2 = index.search(xq, 5)

        # whiten + L2 search on L2 distribution
        index = faiss.index_factory(d, 'PCAW%d,Flat' % d)

        index.train(xt)
        index.add(xb)
        Dw, Iw = index.search(xq, 5)

        # make sure correlation of whitened results with original
        # results is much better than simple L2 distances
        # should be 961 vs. 264
        assert (faiss.eval_intersection(Io, Iw) >
                2 * faiss.eval_intersection(Io, Il2))


class TestTransformChain(unittest.TestCase):

    def test_chain(self):

        # generate data
        d = 4
        nt = 1000
        nb = 200
        nq = 200

        # normal distribition
        x = faiss.randn((nt + nb + nq) * d, 1234).reshape(nt + nb + nq, d)

        # make distribution very skewed
        x *= [10, 4, 1, 0.5]
        rr, _ = np.linalg.qr(faiss.randn(d * d).reshape(d, d))
        x = np.dot(x, rr).astype('float32')

        xt = x[:nt]
        xb = x[nt:-nq]
        xq = x[-nq:]

        index = faiss.index_factory(d, "L2norm,PCA2,L2norm,Flat")

        assert index.chain.size() == 3
        l2_1 = faiss.downcast_VectorTransform(index.chain.at(0))
        assert l2_1.norm == 2
        pca = faiss.downcast_VectorTransform(index.chain.at(1))
        assert not pca.is_trained
        index.train(xt)
        assert pca.is_trained

        index.add(xb)
        D, I = index.search(xq, 5)

        # do the computation manually and check if we get the same result
        def manual_trans(x):
            x = x.copy()
            faiss.normalize_L2(x)
            x = pca.apply_py(x)
            faiss.normalize_L2(x)
            return x

        index2 = faiss.IndexFlatL2(2)
        index2.add(manual_trans(xb))
        D2, I2 = index2.search(manual_trans(xq), 5)

        assert np.all(I == I2)

class TestRareIO(unittest.TestCase):

    def compare_results(self, index1, index2, xq):

        Dref, Iref = index1.search(xq, 5)
        Dnew, Inew = index2.search(xq, 5)

        assert np.all(Dref == Dnew)
        assert np.all(Iref == Inew)

    def do_mmappedIO(self, sparse, in_pretransform=False):
        d = 10
        nb = 1000
        nq = 200
        nt = 200
        xt, xb, xq = get_dataset_2(d, nt, nb, nq)

        quantizer = faiss.IndexFlatL2(d)
        index1 = faiss.IndexIVFFlat(quantizer, d, 20)
        if sparse:
            # makes the inverted lists sparse because all elements get
            # assigned to the same invlist
            xt += (np.ones(10) * 1000).astype('float32')

        if in_pretransform:
            # make sure it still works when wrapped in an IndexPreTransform
            index1 = faiss.IndexPreTransform(index1)

        index1.train(xt)
        index1.add(xb)

        _, fname = tempfile.mkstemp()
        try:

            faiss.write_index(index1, fname)

            index2 = faiss.read_index(fname)
            self.compare_results(index1, index2, xq)

            index3 = faiss.read_index(fname, faiss.IO_FLAG_MMAP)
            self.compare_results(index1, index3, xq)
        finally:
            if os.path.exists(fname):
                os.unlink(fname)

    def test_mmappedIO_sparse(self):
        self.do_mmappedIO(True)

    def test_mmappedIO_full(self):
        self.do_mmappedIO(False)

    def test_mmappedIO_pretrans(self):
        self.do_mmappedIO(False, True)


class TestIVFFlatDedup(unittest.TestCase):

    def normalize_res(self, D, I):
        dmax = D[-1]
        res = [(d, i) for d, i in zip(D, I) if d < dmax]
        res.sort()
        return res

    def test_dedup(self):
        d = 10
        nb = 1000
        nq = 200
        nt = 500
        xt, xb, xq = get_dataset_2(d, nt, nb, nq)

        # introduce duplicates
        xb[500:900:2] = xb[501:901:2]
        xb[901::4] = xb[900::4]
        xb[902::4] = xb[900::4]
        xb[903::4] = xb[900::4]

        # also in the train set
        xt[201::2] = xt[200::2]

        quantizer = faiss.IndexFlatL2(d)
        index_new = faiss.IndexIVFFlatDedup(quantizer, d, 20)

        index_new.verbose = True
        # should display
        # IndexIVFFlatDedup::train: train on 350 points after dedup (was 500 points)
        index_new.train(xt)

        index_ref = faiss.IndexIVFFlat(quantizer, d, 20)
        assert index_ref.is_trained

        index_ref.nprobe = 5
        index_ref.add(xb)
        index_new.nprobe = 5
        index_new.add(xb)

        Dref, Iref = index_ref.search(xq, 20)
        Dnew, Inew = index_new.search(xq, 20)

        for i in range(nq):
            ref = self.normalize_res(Dref[i], Iref[i])
            new = self.normalize_res(Dnew[i], Inew[i])
            assert ref == new

        # test I/O
        _, tmpfile = tempfile.mkstemp()
        try:
            faiss.write_index(index_new, tmpfile)
            index_st = faiss.read_index(tmpfile)
        finally:
            if os.path.exists(tmpfile):
                os.unlink(tmpfile)
        Dst, Ist = index_st.search(xq, 20)

        for i in range(nq):
            new = self.normalize_res(Dnew[i], Inew[i])
            st = self.normalize_res(Dst[i], Ist[i])
            assert st == new

        # test remove
        toremove = np.hstack((np.arange(3, 1000, 5), np.arange(850, 950)))
        index_ref.remove_ids(toremove)
        index_new.remove_ids(toremove)

        Dref, Iref = index_ref.search(xq, 20)
        Dnew, Inew = index_new.search(xq, 20)

        for i in range(nq):
            ref = self.normalize_res(Dref[i], Iref[i])
            new = self.normalize_res(Dnew[i], Inew[i])
            assert ref == new


class TestSerialize(unittest.TestCase):

    def test_serialize_to_vector(self):
        d = 10
        nb = 1000
        nq = 200
        nt = 500
        xt, xb, xq = get_dataset_2(d, nt, nb, nq)

        index = faiss.IndexFlatL2(d)
        index.add(xb)

        Dref, Iref = index.search(xq, 5)

        writer = faiss.VectorIOWriter()
        faiss.write_index(index, writer)

        ar_data = faiss.vector_to_array(writer.data)

        # direct transfer of vector
        reader = faiss.VectorIOReader()
        reader.data.swap(writer.data)

        index2 = faiss.read_index(reader)

        Dnew, Inew = index2.search(xq, 5)
        assert np.all(Dnew == Dref) and np.all(Inew == Iref)

        # from intermediate numpy array
        reader = faiss.VectorIOReader()
        faiss.copy_array_to_vector(ar_data, reader.data)

        index3 = faiss.read_index(reader)

        Dnew, Inew = index3.search(xq, 5)
        assert np.all(Dnew == Dref) and np.all(Inew == Iref)


class TestRenameOndisk(unittest.TestCase):

    def test_rename(self):
        d = 10
        nb = 500
        nq = 100
        nt = 100

        xt, xb, xq = get_dataset_2(d, nt, nb, nq)

        quantizer = faiss.IndexFlatL2(d)

        index1 = faiss.IndexIVFFlat(quantizer, d, 20)
        index1.train(xt)

        dirname = tempfile.mkdtemp()

        try:

            # make an index with ondisk invlists
            invlists = faiss.OnDiskInvertedLists(
                index1.nlist, index1.code_size,
                dirname + '/aa.ondisk')
            index1.replace_invlists(invlists)
            index1.add(xb)
            D1, I1 = index1.search(xq, 10)
            faiss.write_index(index1, dirname + '/aa.ivf')

            # move the index elsewhere
            os.mkdir(dirname + '/1')
            for fname in 'aa.ondisk', 'aa.ivf':
                os.rename(dirname + '/' + fname,
                          dirname + '/1/' + fname)

            # try to read it: fails!
            try:
                index2 = faiss.read_index(dirname + '/1/aa.ivf')
            except RuntimeError:
                pass   # normal
            else:
                assert False

            # read it with magic flag
            index2 = faiss.read_index(dirname + '/1/aa.ivf',
                                      faiss.IO_FLAG_ONDISK_SAME_DIR)
            D2, I2 = index2.search(xq, 10)
            assert np.all(I1 == I2)

        finally:
            shutil.rmtree(dirname)


class TestInvlistMeta(unittest.TestCase):

    def test_slice_vstack(self):
        d = 10
        nb = 1000
        nq = 100
        nt = 200

        xt, xb, xq = get_dataset_2(d, nt, nb, nq)

        quantizer = faiss.IndexFlatL2(d)
        index = faiss.IndexIVFFlat(quantizer, d, 30)

        index.train(xt)
        index.add(xb)
        Dref, Iref = index.search(xq, 10)

        # faiss.wait()

        il0 = index.invlists
        ils = []
        ilv = faiss.InvertedListsPtrVector()
        for sl in 0, 1, 2:
            il = faiss.SliceInvertedLists(il0, sl * 10, sl * 10 + 10)
            ils.append(il)
            ilv.push_back(il)

        il2 = faiss.VStackInvertedLists(ilv.size(), ilv.data())

        index2 = faiss.IndexIVFFlat(quantizer, d, 30)
        index2.replace_invlists(il2)
        index2.ntotal = index.ntotal

        D, I = index2.search(xq, 10)
        assert np.all(D == Dref)
        assert np.all(I == Iref)




if __name__ == '__main__':
    unittest.main()
