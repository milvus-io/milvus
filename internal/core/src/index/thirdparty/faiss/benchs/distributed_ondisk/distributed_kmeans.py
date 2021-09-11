# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python3

"""
Simple distributed kmeans implementation Relies on an abstraction
for the training matrix, that can be sharded over several machines.
"""

import faiss
import time
import numpy as np
import sys
import pdb
import argparse

from scipy.sparse import csc_matrix

from multiprocessing.dummy import Pool as ThreadPool

import rpc




class DatasetAssign:
    """Wrapper for a matrix that offers a function to assign the vectors
    to centroids. All other implementations offer the same interface"""

    def __init__(self, x):
        self.x = np.ascontiguousarray(x, dtype='float32')

    def count(self):
        return self.x.shape[0]

    def dim(self):
        return self.x.shape[1]

    def get_subset(self, indices):
        return self.x[indices]

    def perform_search(self, centroids):
        index = faiss.IndexFlatL2(self.x.shape[1])
        index.add(centroids)
        return index.search(self.x, 1)

    def assign_to(self, centroids, weights=None):
        D, I = self.perform_search(centroids)

        I = I.ravel()
        D = D.ravel()
        n = len(self.x)
        if weights is None:
            weights = np.ones(n, dtype='float32')
        nc = len(centroids)
        m = csc_matrix((weights, I, np.arange(n + 1)),
                       shape=(nc, n))
        sum_per_centroid = m * self.x

        return I, D, sum_per_centroid


class DatasetAssignGPU(DatasetAssign):
    """ GPU version of the previous """

    def __init__(self, x, gpu_id, verbose=False):
        DatasetAssign.__init__(self, x)
        index = faiss.IndexFlatL2(x.shape[1])
        if gpu_id >= 0:
            self.index = faiss.index_cpu_to_gpu(
                faiss.StandardGpuResources(),
                gpu_id, index)
        else:
            # -1 -> assign to all GPUs
            self.index = faiss.index_cpu_to_all_gpus(index)


    def perform_search(self, centroids):
        self.index.reset()
        self.index.add(centroids)
        return self.index.search(self.x, 1)


class DatasetAssignDispatch:
    """dispatches to several other DatasetAssigns and combines the
    results"""

    def __init__(self, xes, in_parallel):
        self.xes = xes
        self.d = xes[0].dim()
        if not in_parallel:
            self.imap = map
        else:
            self.pool = ThreadPool(len(self.xes))
            self.imap = self.pool.imap
        self.sizes = list(map(lambda x: x.count(), self.xes))
        self.cs = np.cumsum([0] + self.sizes)

    def count(self):
        return self.cs[-1]

    def dim(self):
        return self.d

    def get_subset(self, indices):
        res = np.zeros((len(indices), self.d), dtype='float32')
        nos = np.searchsorted(self.cs[1:], indices, side='right')

        def handle(i):
            mask = nos == i
            sub_indices = indices[mask] - self.cs[i]
            subset = self.xes[i].get_subset(sub_indices)
            res[mask] = subset

        list(self.imap(handle, range(len(self.xes))))
        return res

    def assign_to(self, centroids, weights=None):
        src = self.imap(
            lambda x: x.assign_to(centroids, weights),
            self.xes
        )
        I = []
        D = []
        sum_per_centroid = None
        for Ii, Di, sum_per_centroid_i in src:
            I.append(Ii)
            D.append(Di)
            if sum_per_centroid is None:
                sum_per_centroid = sum_per_centroid_i
            else:
                sum_per_centroid += sum_per_centroid_i
        return np.hstack(I), np.hstack(D), sum_per_centroid


def imbalance_factor(k , assign):
    return faiss.imbalance_factor(len(assign), k, faiss.swig_ptr(assign))


def reassign_centroids(hassign, centroids, rs=None):
    """ reassign centroids when some of them collapse """
    if rs is None:
        rs = np.random
    k, d = centroids.shape
    nsplit = 0
    empty_cents = np.where(hassign == 0)[0]

    if empty_cents.size == 0:
        return 0

    fac = np.ones(d)
    fac[::2] += 1 / 1024.
    fac[1::2] -= 1 / 1024.

    # this is a single pass unless there are more than k/2
    # empty centroids
    while empty_cents.size > 0:
        # choose which centroids to split
        probas = hassign.astype('float') - 1
        probas[probas < 0] = 0
        probas /= probas.sum()
        nnz = (probas > 0).sum()

        nreplace = min(nnz, empty_cents.size)
        cjs = rs.choice(k, size=nreplace, p=probas)

        for ci, cj in zip(empty_cents[:nreplace], cjs):

            c = centroids[cj]
            centroids[ci] = c * fac
            centroids[cj] = c / fac

            hassign[ci] = hassign[cj] // 2
            hassign[cj] -= hassign[ci]
            nsplit += 1

        empty_cents = empty_cents[nreplace:]

    return nsplit


def kmeans(k, data, niter=25, seed=1234, checkpoint=None):
    """Pure python kmeans implementation. Follows the Faiss C++ version
    quite closely, but takes a DatasetAssign instead of a training data
    matrix. Also redo is not implemented. """
    n, d = data.count(), data.dim()

    print(("Clustering %d points in %dD to %d clusters, " +
            "%d iterations seed %d") % (n, d, k, niter, seed))

    rs = np.random.RandomState(seed)
    print("preproc...")
    t0 = time.time()
    # initialization
    perm = rs.choice(n, size=k, replace=False)
    centroids = data.get_subset(perm)

    print("  done")
    t_search_tot = 0
    obj = []
    for i in range(niter):
        t0s = time.time()

        print('assigning', end='\r', flush=True)
        assign, D, sums = data.assign_to(centroids)

        print('compute centroids', end='\r', flush=True)

        # pdb.set_trace()

        t_search_tot += time.time() - t0s;

        err = D.sum()
        obj.append(err)

        hassign = np.bincount(assign, minlength=k)

        fac = hassign.reshape(-1, 1).astype('float32')
        fac[fac == 0] = 1 # quiet warning

        centroids = sums / fac

        nsplit = reassign_centroids(hassign, centroids, rs)

        print(("  Iteration %d (%.2f s, search %.2f s): "
               "objective=%g imbalance=%.3f nsplit=%d") % (
                   i, (time.time() - t0), t_search_tot,
                   err, imbalance_factor (k, assign),
                   nsplit)
        )

        if checkpoint is not None:
            print('storing centroids in', checkpoint)
            np.save(checkpoint, centroids)

    return centroids


class AssignServer(rpc.Server):
    """ Assign version that can be exposed via RPC """

    def __init__(self, s, assign, log_prefix=''):
        rpc.Server.__init__(self, s, log_prefix=log_prefix)
        self.assign = assign

    def __getattr__(self, f):
        return getattr(self.assign, f)



def bvecs_mmap(fname):
    x = np.memmap(fname, dtype='uint8', mode='r')
    d = x[:4].view('int32')[0]
    return x.reshape(-1, d + 4)[:, 4:]


def ivecs_mmap(fname):
    a = np.memmap(fname, dtype='int32', mode='r')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:]

def fvecs_mmap(fname):
    return ivecs_mmap(fname).view('float32')


def do_test(todo):
    testdata = '/datasets01_101/simsearch/041218/bigann/bigann_learn.bvecs'

    x = bvecs_mmap(testdata)

    # bad distribution to stress-test split code
    xx = x[:100000].copy()
    xx[:50000] = x[0]

    todo = sys.argv[1:]

    if "0" in todo:
        # reference C++ run
        km = faiss.Kmeans(x.shape[1], 1000, niter=20, verbose=True)
        km.train(xx.astype('float32'))

    if "1" in todo:
        # using the Faiss c++ implementation
        data = DatasetAssign(xx)
        kmeans(1000, data, 20)

    if "2" in todo:
        # use the dispatch object (on local datasets)
        data = DatasetAssignDispatch([
            DatasetAssign(xx[20000 * i : 20000 * (i + 1)])
            for i in range(5)
            ], False
        )
        kmeans(1000, data, 20)

    if "3" in todo:
        # same, with GPU
        ngpu = faiss.get_num_gpus()
        print('using %d GPUs' % ngpu)
        data = DatasetAssignDispatch([
            DatasetAssignGPU(xx[100000 * i // ngpu: 100000 * (i + 1) // ngpu], i)
            for i in range(ngpu)
            ], True
        )
        kmeans(1000, data, 20)


def main():
    parser = argparse.ArgumentParser()

    def aa(*args, **kwargs):
        group.add_argument(*args, **kwargs)

    group = parser.add_argument_group('general options')
    aa('--test', default='', help='perform tests (comma-separated numbers)')

    aa('--k', default=0, type=int, help='nb centroids')
    aa('--seed', default=1234, type=int, help='random seed')
    aa('--niter', default=20, type=int, help='nb iterations')
    aa('--gpu', default=-2, type=int, help='GPU to use (-2:none, -1: all)')

    group = parser.add_argument_group('I/O options')
    aa('--indata', default='',
       help='data file to load (supported formats fvecs, bvecs, npy')
    aa('--i0', default=0, type=int, help='first vector to keep')
    aa('--i1', default=-1, type=int, help='last vec to keep + 1')
    aa('--out', default='', help='file to store centroids')
    aa('--store_each_iteration', default=False, action='store_true',
       help='store centroid checkpoints')

    group = parser.add_argument_group('server options')
    aa('--server', action='store_true', default=False, help='run server')
    aa('--port', default=12345, type=int, help='server port')
    aa('--when_ready', default=None, help='store host:port to this file when ready')
    aa('--ipv4', default=False, action='store_true', help='force ipv4')

    group = parser.add_argument_group('client options')
    aa('--client', action='store_true', default=False, help='run client')
    aa('--servers', default='', help='list of server:port separated by spaces')

    args = parser.parse_args()

    if args.test:
        do_test(args.test.split(','))
        return

    # prepare data matrix (either local or remote)
    if args.indata:
        print('loading ', args.indata)
        if args.indata.endswith('.bvecs'):
            x = bvecs_mmap(args.indata)
        elif args.indata.endswith('.fvecs'):
            x = fvecs_mmap(args.indata)
        elif args.indata.endswith('.npy'):
            x = np.load(args.indata, mmap_mode='r')
        else:
            raise AssertionError

        if args.i1 == -1:
            args.i1 = len(x)
        x = x[args.i0:args.i1]
        if args.gpu == -2:
            data = DatasetAssign(x)
        else:
            print('moving to GPU')
            data = DatasetAssignGPU(x, args.gpu)

    elif args.client:
        print('connecting to servers')

        def connect_client(hostport):
            host, port = hostport.split(':')
            port = int(port)
            print('connecting %s:%d' % (host, port))
            client = rpc.Client(host, port, v6=not args.ipv4)
            print('client %s:%d ready' % (host, port))
            return client

        hostports = args.servers.strip().split(' ')
        # pool = ThreadPool(len(hostports))

        data = DatasetAssignDispatch(
            list(map(connect_client, hostports)),
            True
        )
    else:
        raise AssertionError


    if args.server:
        print('starting server')
        log_prefix = f"{rpc.socket.gethostname()}:{args.port}"
        rpc.run_server(
            lambda s: AssignServer(s, data, log_prefix=log_prefix),
            args.port, report_to_file=args.when_ready,
            v6=not args.ipv4)

    else:
        print('running kmeans')
        centroids = kmeans(args.k, data, niter=args.niter, seed=args.seed,
                           checkpoint=args.out if args.store_each_iteration else None)
        if args.out != '':
            print('writing centroids to', args.out)
            np.save(args.out, centroids)


if __name__ == '__main__':
    main()
