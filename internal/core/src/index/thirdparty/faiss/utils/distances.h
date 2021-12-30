/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

/* All distance functions for L2 and IP distances.
 * The actual functions are implemented in distances.cpp and distances_simd.cpp */

#pragma once

#include <stdint.h>

#include <faiss/utils/Heap.h>
#include <faiss/utils/BitsetView.h>
#include <faiss/utils/distances_range.h>
#include <faiss/utils/distances_simd.h>


namespace faiss {

 /*********************************************************
 * Optimized distance/norm/inner prod computations
 *********************************************************/

/** Compute pairwise distances between sets of vectors
 *
 * @param d     dimension of the vectors
 * @param nq    nb of query vectors
 * @param nb    nb of database vectors
 * @param xq    query vectors (size nq * d)
 * @param xb    database vectros (size nb * d)
 * @param dis   output distances (size nq * nb)
 * @param ldq,ldb, ldd strides for the matrices
 */
void pairwise_L2sqr (int64_t d,
                     int64_t nq, const float *xq,
                     int64_t nb, const float *xb,
                     float *dis,
                     int64_t ldq = -1, int64_t ldb = -1, int64_t ldd = -1);

/* compute the inner product between nx vectors x and one y */
void fvec_inner_products_ny (
        float * ip,         /* output inner product */
        const float * x,
        const float * y,
        size_t d, size_t ny);

/** compute the L2 norms for a set of vectors
 *
 * @param  ip       output norms, size nx
 * @param  x        set of vectors, size nx * d
 */
void fvec_norms_L2 (float * ip, const float * x, size_t d, size_t nx);

/// same as fvec_norms_L2, but computes square norms
void fvec_norms_L2sqr (float * ip, const float * x, size_t d, size_t nx);

/* L2-renormalize a set of vector. Nothing done if the vector is 0-normed */
void fvec_renorm_L2 (size_t d, size_t nx, float * x);


/* This function exists because the Torch counterpart is extremly slow
   (not multi-threaded + unexpected overhead even in single thread).
   It is here to implement the usual property |x-y|^2=|x|^2+|y|^2-2<x|y>  */
void inner_product_to_L2sqr (float * dis,
                             const float * nr1,
                             const float * nr2,
                             size_t n1, size_t n2);

/***************************************************************************
 * Compute a subset of  distances
 ***************************************************************************/

 /* compute the inner product between x and a subset y of ny vectors,
   whose indices are given by idy.  */
void fvec_inner_products_by_idx (
        float * ip,
        const float * x,
        const float * y,
        const int64_t *ids,
        size_t d, size_t nx, size_t ny);

/* same but for a subset in y indexed by idsy (ny vectors in total) */
void fvec_L2sqr_by_idx (
        float * dis,
        const float * x,
        const float * y,
        const int64_t *ids, /* ids of y vecs */
        size_t d, size_t nx, size_t ny);


/** compute dis[j] = L2sqr(x[ix[j]], y[iy[j]]) forall j=0..n-1
 *
 * @param x  size (max(ix) + 1, d)
 * @param y  size (max(iy) + 1, d)
 * @param ix size n
 * @param iy size n
 * @param dis size n
 */
void pairwise_indexed_L2sqr (
        size_t d, size_t n,
        const float * x, const int64_t *ix,
        const float * y, const int64_t *iy,
        float *dis);

/* same for inner product */
void pairwise_indexed_inner_product (
        size_t d, size_t n,
        const float * x, const int64_t *ix,
        const float * y, const int64_t *iy,
        float *dis);

/***************************************************************************
 * KNN functions
 ***************************************************************************/

// threshold on nx above which we switch to BLAS to compute distances
extern int distance_compute_blas_threshold;

// threshold on nx above which we switch to compute parallel on ny
extern int parallel_policy_threshold;

/** Return the k nearest neighors of each of the nx vectors x among the ny
 *  vector y, w.r.t to max inner product
 *
 * @param x    query vectors, size nx * d
 * @param y    database vectors, size ny * d
 * @param res  result array, which also provides k. Sorted on output
 */
void knn_inner_product (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        float_minheap_array_t * res,
        const BitsetView bitset = nullptr);

/** Same as knn_inner_product, for the L2 distance */
void knn_L2sqr (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        float_maxheap_array_t * res,
        const BitsetView bitset = nullptr);

void knn_jaccard (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        float_maxheap_array_t * res,
        const BitsetView bitset = nullptr);
        
/** same as knn_L2sqr, but base_shift[bno] is subtracted to all
 * computed distances.
 *
 * @param base_shift   size ny
 */
void knn_L2sqr_base_shift (
         const float * x,
         const float * y,
         size_t d, size_t nx, size_t ny,
         float_maxheap_array_t * res,
         const float *base_shift);

/* Find the nearest neighbors for nx queries in a set of ny vectors
 * indexed by ids. May be useful for re-ranking a pre-selected vector list
 */
void knn_inner_products_by_idx (
        const float * x,
        const float * y,
        const int64_t *  ids,
        size_t d, size_t nx, size_t ny,
        float_minheap_array_t * res);

void knn_L2sqr_by_idx (const float * x,
                       const float * y,
                       const int64_t * ids,
                       size_t d, size_t nx, size_t ny,
                       float_maxheap_array_t * res);


/***************************************************************************
 * elkan
 ***************************************************************************/

/** Return the nearest neighors of each of the nx vectors x among the ny
 *
 * @param x    query vectors, size nx * d
 * @param y    database vectors, size ny * d
 * @param ids  result array ids
 * @param val  result array value
 */
void elkan_L2_sse (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        int64_t *ids, float *val);

} // namespace faiss
