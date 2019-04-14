# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

"""
This is the training code for the link and code. Especially the
neighbors_kmeans function implements the EM-algorithm to find the
appropriate weightings and cluster them.
"""

import time
import numpy as np
import faiss

#----------------------------------------------------------
# Utils
#----------------------------------------------------------

def sanitize(x):
    return np.ascontiguousarray(x, dtype='float32')


def train_kmeans(x, k, ngpu, max_points_per_centroid=256):
    "Runs kmeans on one or several GPUs"
    d = x.shape[1]
    clus = faiss.Clustering(d, k)
    clus.verbose = True
    clus.niter = 20
    clus.max_points_per_centroid = max_points_per_centroid

    if ngpu == 0:
        index = faiss.IndexFlatL2(d)
    else:
        res = [faiss.StandardGpuResources() for i in range(ngpu)]

        flat_config = []
        for i in range(ngpu):
            cfg = faiss.GpuIndexFlatConfig()
            cfg.useFloat16 = False
            cfg.device = i
            flat_config.append(cfg)

        if ngpu == 1:
            index = faiss.GpuIndexFlatL2(res[0], d, flat_config[0])
        else:
            indexes = [faiss.GpuIndexFlatL2(res[i], d, flat_config[i])
                       for i in range(ngpu)]
            index = faiss.IndexReplicas()
            for sub_index in indexes:
                index.addIndex(sub_index)

    # perform the training
    clus.train(x, index)
    centroids = faiss.vector_float_to_array(clus.centroids)

    obj = faiss.vector_float_to_array(clus.obj)
    print "final objective: %.4g" % obj[-1]

    return centroids.reshape(k, d)


#----------------------------------------------------------
# Learning the codebook from neighbors
#----------------------------------------------------------


# works with both a full Inn table and dynamically generated neighbors

def get_Inn_shape(Inn):
    if type(Inn) != tuple:
        return Inn.shape
    return Inn[:2]

def get_neighbor_table(x_coded, Inn, i):
    if type(Inn) != tuple:
        return x_coded[Inn[i,:],:]
    rfn = x_coded
    M, d = rfn.M, rfn.index.d
    out = np.zeros((M + 1, d), dtype='float32')
    rfn.get_neighbor_table(i, faiss.swig_ptr(out))
    _, _, sq = Inn
    return out[:, sq * rfn.dsub : (sq + 1) * rfn.dsub]


# Function that produces the best regression values from the vector
# and its neighbors
def regress_from_neighbors (x, x_coded, Inn):
    (N, knn) = get_Inn_shape(Inn)
    betas = np.zeros((N,knn))
    t0 = time.time()
    for i in xrange (N):
        xi = x[i,:]
        NNi = get_neighbor_table(x_coded, Inn, i)
        betas[i,:] = np.linalg.lstsq(NNi.transpose(), xi, rcond=0.01)[0]
        if i % (N / 10) == 0:
            print ("[%d:%d]  %6.3fs" % (i, i + N / 10, time.time() - t0))
    return betas



# find the best beta minimizing ||x-x_coded[Inn,:]*beta||^2
def regress_opt_beta (x, x_coded, Inn):
    (N, knn) = get_Inn_shape(Inn)
    d = x.shape[1]

    # construct the linear system to be solved
    X = np.zeros ((d*N))
    Y = np.zeros ((d*N, knn))
    for i in xrange (N):
        X[i*d:(i+1)*d] = x[i,:]
        neighbor_table = get_neighbor_table(x_coded, Inn, i)
        Y[i*d:(i+1)*d, :] = neighbor_table.transpose()
    beta_opt = np.linalg.lstsq(Y, X, rcond=0.01)[0]
    return beta_opt


# Find the best encoding by minimizing the reconstruction error using
# a set of pre-computed beta values
def assign_beta (beta_centroids, x, x_coded, Inn, verbose=True):
    if type(Inn) == tuple:
        return assign_beta_2(beta_centroids, x, x_coded, Inn)
    (N, knn) = Inn.shape
    x_ibeta = np.zeros ((N), dtype='int32')
    t0= time.time()
    for i in xrange (N):
        NNi = x_coded[Inn[i,:]]
        # Consider all possible betas for the encoding and compute the
        # encoding error
        x_reg_all = np.dot (beta_centroids, NNi)
        err = ((x_reg_all - x[i,:]) ** 2).sum(axis=1)
        x_ibeta[i] = err.argmin()
        if verbose:
            if i % (N / 10) == 0:
                print ("[%d:%d]  %6.3fs" % (i, i + N / 10, time.time() - t0))
    return x_ibeta


# Reconstruct a set of vectors using the beta_centroids, the
# assignment, the encoded neighbors identified by the list Inn (which
# includes the vector itself)
def recons_from_neighbors (beta_centroids, x_ibeta, x_coded, Inn):
    (N, knn) = Inn.shape
    x_rec = np.zeros(x_coded.shape)
    t0= time.time()
    for i in xrange (N):
        NNi = x_coded[Inn[i,:]]
        x_rec[i, :] = np.dot (beta_centroids[x_ibeta[i]], NNi)
        if i % (N / 10) == 0:
            print ("[%d:%d]  %6.3fs" % (i, i + N / 10, time.time() - t0))
    return x_rec


# Compute a EM-like algorithm trying at optimizing the beta such as they
# minimize the reconstruction error from the neighbors
def neighbors_kmeans (x, x_coded, Inn, K, ngpus=1, niter=5):
    # First compute centroids using a regular k-means algorithm
    betas = regress_from_neighbors (x, x_coded, Inn)
    beta_centroids = train_kmeans(
        sanitize(betas), K, ngpus, max_points_per_centroid=1000000)
    _, knn = get_Inn_shape(Inn)
    d = x.shape[1]

    rs = np.random.RandomState()
    for iter in range(niter):
        print 'iter', iter
        idx = assign_beta (beta_centroids, x, x_coded, Inn, verbose=False)

        hist = np.bincount(idx)
        for cl0 in np.where(hist == 0)[0]:
            print "  cluster %d empty, split" % cl0,
            cl1 = idx[np.random.randint(idx.size)]
            pos = np.nonzero (idx == cl1)[0]
            pos = rs.choice(pos, pos.size / 2)
            print "   cl %d -> %d + %d" % (cl1, len(pos), hist[cl1] - len(pos))
            idx[pos] = cl0
            hist = np.bincount(idx)

        tot_err = 0
        for k in range (K):
            pos = np.nonzero (idx == k)[0]
            npos = pos.shape[0]

            X = np.zeros (d*npos)
            Y = np.zeros ((d*npos, knn))

            for i in range(npos):
                X[i*d:(i+1)*d] = x[pos[i],:]
                neighbor_table = get_neighbor_table(x_coded, Inn, pos[i])
                Y[i*d:(i+1)*d, :] = neighbor_table.transpose()
            sol, residuals, _, _ = np.linalg.lstsq(Y, X, rcond=0.01)
            if residuals.size > 0:
                tot_err += residuals.sum()
            beta_centroids[k, :] = sol
        print '  err=%g' % tot_err
    return beta_centroids


# assign the betas in C++
def assign_beta_2(beta_centroids, x, rfn, Inn):
    _, _, sq = Inn
    if rfn.k == 1:
        return np.zeros(x.shape[0], dtype=int)
    # add dummy dimensions to beta_centroids and x
    all_beta_centroids = np.zeros(
        (rfn.nsq, rfn.k, rfn.M + 1), dtype='float32')
    all_beta_centroids[sq] = beta_centroids
    all_x = np.zeros((len(x), rfn.d), dtype='float32')
    all_x[:, sq * rfn.dsub : (sq + 1) * rfn.dsub] = x
    rfn.codes.clear()
    rfn.ntotal = 0
    faiss.copy_array_to_vector(
        all_beta_centroids.ravel(), rfn.codebook)
    rfn.add_codes(len(x), faiss.swig_ptr(all_x))
    codes = faiss.vector_to_array(rfn.codes)
    codes = codes.reshape(-1, rfn.nsq)
    return codes[:, sq]


#######################################################
# For usage from bench_storages.py

def train_beta_codebook(rfn, xb_full, niter=10):
    beta_centroids = []
    for sq in range(rfn.nsq):
        d0, d1 = sq * rfn.dsub, (sq + 1) * rfn.dsub
        print "training subquantizer %d/%d on dimensions %d:%d" % (
            sq, rfn.nsq, d0, d1)
        beta_centroids_i = neighbors_kmeans(
            xb_full[:, d0:d1], rfn, (xb_full.shape[0], rfn.M + 1, sq),
            rfn.k,
            ngpus=0, niter=niter)
        beta_centroids.append(beta_centroids_i)
        rfn.ntotal = 0
        rfn.codes.clear()
        rfn.codebook.clear()
    return np.stack(beta_centroids)
