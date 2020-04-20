# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

import numpy as np

import faiss
import unittest


class TestClustering(unittest.TestCase):

    def test_clustering(self):
        d = 64
        n = 1000
        rs = np.random.RandomState(123)
        x = rs.uniform(size=(n, d)).astype('float32')

        x *= 10

        km = faiss.Kmeans(d, 32, niter=10)
        err32 = km.train(x)

        # check that objective is decreasing
        prev = 1e50
        for o in km.obj:
            self.assertGreater(prev, o)
            prev = o

        km = faiss.Kmeans(d, 64, niter=10)
        err64 = km.train(x)

        # check that 64 centroids give a lower quantization error than 32
        self.assertGreater(err32, err64)

        km = faiss.Kmeans(d, 32, niter=10, int_centroids=True)
        err_int = km.train(x)

        # check that integer centoids are not as good as float ones
        self.assertGreater(err_int, err32)
        self.assertTrue(np.all(km.centroids == np.floor(km.centroids)))


    def test_nasty_clustering(self):
        d = 2
        rs = np.random.RandomState(123)
        x = np.zeros((100, d), dtype='float32')
        for i in range(5):
            x[i * 20:i * 20 + 20] = rs.uniform(size=d)

        # we have 5 distinct points but ask for 10 centroids...
        km = faiss.Kmeans(d, 10, niter=10, verbose=True)
        km.train(x)

    def test_redo(self):
        d = 64
        n = 1000

        rs = np.random.RandomState(123)
        x = rs.uniform(size=(n, d)).astype('float32')

        clus = faiss.Clustering(d, 20)
        clus.nredo = 1
        clus.train(x, faiss.IndexFlatL2(d))
        obj1 = faiss.vector_to_array(clus.obj)

        clus = faiss.Clustering(d, 20)
        clus.nredo = 10
        clus.train(x, faiss.IndexFlatL2(d))
        obj10 = faiss.vector_to_array(clus.obj)

        self.assertGreater(obj1[-1], obj10[-1])

    def test_1ptpercluster(self):
        # https://github.com/facebookresearch/faiss/issues/842
        X = np.random.randint(0, 1, (5, 10)).astype('float32')
        k = 5
        niter = 10
        verbose = True
        kmeans = faiss.Kmeans(X.shape[1], k, niter=niter, verbose=verbose)
        kmeans.train(X)
        l2_distances, I = kmeans.index.search(X, 1)


class TestPCA(unittest.TestCase):

    def test_pca(self):
        d = 64
        n = 1000
        np.random.seed(123)
        x = np.random.random(size=(n, d)).astype('float32')

        pca = faiss.PCAMatrix(d, 10)
        pca.train(x)
        y = pca.apply_py(x)

        # check that energy per component is decreasing
        column_norm2 = (y**2).sum(0)

        prev = 1e50
        for o in column_norm2:
            self.assertGreater(prev, o)
            prev = o


class TestProductQuantizer(unittest.TestCase):

    def test_pq(self):
        d = 64
        n = 2000
        cs = 4
        np.random.seed(123)
        x = np.random.random(size=(n, d)).astype('float32')
        pq = faiss.ProductQuantizer(d, cs, 8)
        pq.train(x)
        codes = pq.compute_codes(x)
        x2 = pq.decode(codes)
        diff = ((x - x2)**2).sum()

        # print "diff=", diff
        # diff= 4418.0562
        self.assertGreater(5000, diff)

        pq10 = faiss.ProductQuantizer(d, cs, 10)
        assert pq10.code_size == 5
        pq10.verbose = True
        pq10.cp.verbose = True
        pq10.train(x)
        codes = pq10.compute_codes(x)

        x10 = pq10.decode(codes)
        diff10 = ((x - x10)**2).sum()
        self.assertGreater(diff, diff10)

    def do_test_codec(self, nbit):
        pq = faiss.ProductQuantizer(16, 2, nbit)

        # simulate training
        rs = np.random.RandomState(123)
        centroids = rs.rand(2, 1 << nbit, 8).astype('float32')
        faiss.copy_array_to_vector(centroids.ravel(), pq.centroids)

        idx = rs.randint(1 << nbit, size=(100, 2))
        # can be encoded exactly
        x = np.hstack((
            centroids[0, idx[:, 0]],
            centroids[1, idx[:, 1]]
        ))

        # encode / decode
        codes = pq.compute_codes(x)
        xr = pq.decode(codes)
        assert np.all(xr == x)

        # encode w/ external index
        assign_index = faiss.IndexFlatL2(8)
        pq.assign_index = assign_index
        codes2 = np.empty((100, pq.code_size), dtype='uint8')
        pq.compute_codes_with_assign_index(
            faiss.swig_ptr(x), faiss.swig_ptr(codes2), 100)
        assert np.all(codes == codes2)

    def test_codec(self):
        for i in range(16):
            print("Testing nbits=%d" % (i + 1))
            self.do_test_codec(i + 1)


class TestRevSwigPtr(unittest.TestCase):

    def test_rev_swig_ptr(self):

        index = faiss.IndexFlatL2(4)
        xb0 = np.vstack([
            i * 10 + np.array([1, 2, 3, 4], dtype='float32')
            for i in range(5)])
        index.add(xb0)
        xb = faiss.rev_swig_ptr(index.xb.data(), 4 * 5).reshape(5, 4)
        self.assertEqual(np.abs(xb0 - xb).sum(), 0)


class TestException(unittest.TestCase):

    def test_exception(self):

        index = faiss.IndexFlatL2(10)

        a = np.zeros((5, 10), dtype='float32')
        b = np.zeros(5, dtype='int64')

        try:
            # an unsupported operation for IndexFlat
            index.add_with_ids(a, b)
        except RuntimeError as e:
            assert 'add_with_ids not implemented' in str(e)
        else:
            assert False, 'exception did not fire???'

    def test_exception_2(self):

        try:
            faiss.index_factory(12, 'IVF256,Flat,PQ8')
        except RuntimeError as e:
            assert 'could not parse' in str(e)
        else:
            assert False, 'exception did not fire???'

class TestMapLong2Long(unittest.TestCase):

    def test_maplong2long(self):
        keys = np.array([13, 45, 67])
        vals = np.array([3, 8, 2])

        m = faiss.MapLong2Long()
        m.add(keys, vals)

        assert np.all(m.search_multiple(keys) == vals)

        assert m.search(12343) == -1


class TestOrthognalReconstruct(unittest.TestCase):

    def test_recons_orthonormal(self):
        lt = faiss.LinearTransform(20, 10, True)
        rs = np.random.RandomState(10)
        A, _ = np.linalg.qr(rs.randn(20, 20))
        A = A[:10].astype('float32')
        faiss.copy_array_to_vector(A.ravel(), lt.A)
        faiss.copy_array_to_vector(rs.randn(10).astype('float32'), lt.b)

        lt.set_is_orthonormal()
        lt.is_trained = True
        assert lt.is_orthonormal

        x = rs.rand(30, 20).astype('float32')
        xt = lt.apply_py(x)
        xtt = lt.reverse_transform(xt)
        xttt = lt.apply_py(xtt)

        err = ((xt - xttt)**2).sum()

        self.assertGreater(1e-5, err)

    def test_recons_orthogona_impossible(self):
        lt = faiss.LinearTransform(20, 10, True)
        rs = np.random.RandomState(10)
        A = rs.randn(10 * 20).astype('float32')
        faiss.copy_array_to_vector(A.ravel(), lt.A)
        faiss.copy_array_to_vector(rs.randn(10).astype('float32'), lt.b)
        lt.is_trained = True

        lt.set_is_orthonormal()
        assert not lt.is_orthonormal

        x = rs.rand(30, 20).astype('float32')
        xt = lt.apply_py(x)
        try:
            lt.reverse_transform(xt)
        except Exception:
            pass
        else:
            self.assertFalse('should do an exception')


class TestMAdd(unittest.TestCase):

    def test_1(self):
        # try with dimensions that are multiples of 16 or not
        rs = np.random.RandomState(123)
        swig_ptr = faiss.swig_ptr
        for dim in 16, 32, 20, 25:
            for repeat in 1, 2, 3, 4, 5:
                a = rs.rand(dim).astype('float32')
                b = rs.rand(dim).astype('float32')
                c = np.zeros(dim, dtype='float32')
                bf = rs.uniform(5.0) - 2.5
                idx = faiss.fvec_madd_and_argmin(
                    dim, swig_ptr(a), bf, swig_ptr(b),
                    swig_ptr(c))
                ref_c = a + b * bf
                assert np.abs(c - ref_c).max() < 1e-5
                assert idx == ref_c.argmin()


class TestNyFuncs(unittest.TestCase):

    def test_l2(self):
        rs = np.random.RandomState(123)
        swig_ptr = faiss.swig_ptr
        for d in 1, 2, 4, 8, 12, 16:
            x = rs.rand(d).astype('float32')
            for ny in 128, 129, 130:
                print("d=%d ny=%d" % (d, ny))
                y = rs.rand(ny, d).astype('float32')
                ref = ((x - y) ** 2).sum(1)
                new = np.zeros(ny, dtype='float32')
                faiss.fvec_L2sqr_ny(swig_ptr(new), swig_ptr(x),
                                    swig_ptr(y), d, ny)
                assert np.abs(ref - new).max() < 1e-4

    def test_IP(self):
        # this one is not optimized with SIMD but just in case
        rs = np.random.RandomState(123)
        swig_ptr = faiss.swig_ptr
        for d in 1, 2, 4, 8, 12, 16:
            x = rs.rand(d).astype('float32')
            for ny in 128, 129, 130:
                print("d=%d ny=%d" % (d, ny))
                y = rs.rand(ny, d).astype('float32')
                ref = (x * y).sum(1)
                new = np.zeros(ny, dtype='float32')
                faiss.fvec_inner_products_ny(
                    swig_ptr(new), swig_ptr(x), swig_ptr(y), d, ny)
                assert np.abs(ref - new).max() < 1e-4


class TestMatrixStats(unittest.TestCase):

    def test_0s(self):
        rs = np.random.RandomState(123)
        m = rs.rand(40, 20).astype('float32')
        m[5:10] = 0
        comments = faiss.MatrixStats(m).comments
        print comments
        assert 'has 5 copies' in comments
        assert '5 null vectors' in comments

    def test_copies(self):
        rs = np.random.RandomState(123)
        m = rs.rand(40, 20).astype('float32')
        m[::2] = m[1::2]
        comments = faiss.MatrixStats(m).comments
        print comments
        assert '20 vectors are distinct' in comments

    def test_dead_dims(self):
        rs = np.random.RandomState(123)
        m = rs.rand(40, 20).astype('float32')
        m[:, 5:10] = 0
        comments = faiss.MatrixStats(m).comments
        print comments
        assert '5 dimensions are constant' in comments

    def test_rogue_means(self):
        rs = np.random.RandomState(123)
        m = rs.rand(40, 20).astype('float32')
        m[:, 5:10] += 12345
        comments = faiss.MatrixStats(m).comments
        print comments
        assert '5 dimensions are too large wrt. their variance' in comments

    def test_normalized(self):
        rs = np.random.RandomState(123)
        m = rs.rand(40, 20).astype('float32')
        faiss.normalize_L2(m)
        comments = faiss.MatrixStats(m).comments
        print comments
        assert 'vectors are normalized' in comments


class TestScalarQuantizer(unittest.TestCase):

    def test_8bit_equiv(self):
        rs = np.random.RandomState(123)
        for it in range(20):
            for d in 13, 16, 24:
                x = np.floor(rs.rand(5, d) * 256).astype('float32')
                x[0] = 0
                x[1] = 255

                # make sure to test extreme cases
                x[2, 0] = 0
                x[3, 0] = 255
                x[2, 1] = 255
                x[3, 1] = 0

                ref_index = faiss.IndexScalarQuantizer(
                    d, faiss.ScalarQuantizer.QT_8bit)
                ref_index.train(x[:2])
                ref_index.add(x[2:3])

                index = faiss.IndexScalarQuantizer(
                    d, faiss.ScalarQuantizer.QT_8bit_direct)
                assert index.is_trained
                index.add(x[2:3])

                assert np.all(
                    faiss.vector_to_array(ref_index.codes) ==
                    faiss.vector_to_array(index.codes))

                # Note that distances are not the same because ref_index
                # reconstructs x as x + 0.5
                D, I = index.search(x[3:], 1)

                # assert D[0, 0] == Dref[0, 0]
                print(D[0, 0], ((x[3] - x[2]) ** 2).sum())
                assert D[0, 0] == ((x[3] - x[2]) ** 2).sum()

    def test_6bit_equiv(self):
        rs = np.random.RandomState(123)
        for d in 3, 6, 8, 16, 36:
            trainset = np.zeros((2, d), dtype='float32')
            trainset[0, :] = 0
            trainset[0, :] = 63

            index = faiss.IndexScalarQuantizer(
                d, faiss.ScalarQuantizer.QT_6bit)
            index.train(trainset)

            print('cs=', index.code_size)

            x = rs.randint(64, size=(100, d)).astype('float32')

            # verify encoder / decoder
            index.add(x)
            x2 = index.reconstruct_n(0, x.shape[0])
            assert np.all(x == x2 - 0.5)

            # verify AVX decoder (used only for search)
            y = 63 * rs.rand(20, d).astype('float32')

            D, I = index.search(y, 10)
            for i in range(20):
                for j in range(10):
                    dis = ((y[i] - x2[I[i, j]]) ** 2).sum()
                    print(dis, D[i, j])
                    assert abs(D[i, j] - dis) / dis < 1e-5

class TestRandom(unittest.TestCase):

    def test_rand(self):
        x = faiss.rand(2000)
        assert np.all(x >= 0) and np.all(x < 1)
        h, _ = np.histogram(x, np.arange(0, 1, 0.1))
        assert h.min() > 160 and h.max() < 240

    def test_randint(self):
        x = faiss.randint(20000, vmax=100)
        assert np.all(x >= 0) and np.all(x < 100)
        c = np.bincount(x, minlength=100)
        print(c)
        assert c.max() - c.min() < 50 * 2


class TestPairwiseDis(unittest.TestCase):

    def test_L2(self):
        swig_ptr = faiss.swig_ptr
        x = faiss.rand((100, 10), seed=1)
        y = faiss.rand((200, 10), seed=2)
        ix = faiss.randint(50, vmax=100)
        iy = faiss.randint(50, vmax=200)
        dis = np.empty(50, dtype='float32')
        faiss.pairwise_indexed_L2sqr(
            10, 50,
            swig_ptr(x), swig_ptr(ix),
            swig_ptr(y), swig_ptr(iy),
            swig_ptr(dis))

        for i in range(50):
            assert np.allclose(
                dis[i], ((x[ix[i]] - y[iy[i]]) ** 2).sum())

    def test_IP(self):
        swig_ptr = faiss.swig_ptr
        x = faiss.rand((100, 10), seed=1)
        y = faiss.rand((200, 10), seed=2)
        ix = faiss.randint(50, vmax=100)
        iy = faiss.randint(50, vmax=200)
        dis = np.empty(50, dtype='float32')
        faiss.pairwise_indexed_inner_product(
            10, 50,
            swig_ptr(x), swig_ptr(ix),
            swig_ptr(y), swig_ptr(iy),
            swig_ptr(dis))

        for i in range(50):
            assert np.allclose(
                dis[i], np.dot(x[ix[i]], y[iy[i]]))




if __name__ == '__main__':
    unittest.main()
