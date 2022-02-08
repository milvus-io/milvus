/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <cstdio>
#include <cassert>
#include <cstring>
#include <cmath>
#include <omp.h>

#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/distances.h>
#include <faiss/FaissHook.h>


#ifndef FINTEGER
#define FINTEGER long
#endif


extern "C" {

/* declare BLAS functions, see http://www.netlib.org/clapack/cblas/ */

int sgemm_ (const char *transa, const char *transb, FINTEGER *m, FINTEGER *
            n, FINTEGER *k, const float *alpha, const float *a,
            FINTEGER *lda, const float *b, FINTEGER *
            ldb, float *beta, float *c, FINTEGER *ldc);

/* Lapack functions, see http://www.netlib.org/clapack/old/single/sgeqrf.c */

int sgeqrf_ (FINTEGER *m, FINTEGER *n, float *a, FINTEGER *lda,
                 float *tau, float *work, FINTEGER *lwork, FINTEGER *info);

int sgemv_(const char *trans, FINTEGER *m, FINTEGER *n, float *alpha,
           const float *a, FINTEGER *lda, const float *x, FINTEGER *incx,
           float *beta, float *y, FINTEGER *incy);

}


namespace faiss {


/***************************************************************************
 * Matrix/vector ops
 ***************************************************************************/



/* Compute the inner product between a vector x and
   a set of ny vectors y.
   These functions are not intended to replace BLAS matrix-matrix, as they
   would be significantly less efficient in this case. */
void fvec_inner_products_ny (float * ip,
                             const float * x,
                             const float * y,
                             size_t d, size_t ny)
{
    // Not sure which one is fastest
#if 0
    {
        FINTEGER di = d;
        FINTEGER nyi = ny;
        float one = 1.0, zero = 0.0;
        FINTEGER onei = 1;
        sgemv_ ("T", &di, &nyi, &one, y, &di, x, &onei, &zero, ip, &onei);
    }
#endif
    for (size_t i = 0; i < ny; i++) {
        ip[i] = fvec_inner_product (x, y, d);
        y += d;
    }
}





/* Compute the L2 norm of a set of nx vectors */
void fvec_norms_L2 (float * __restrict nr,
                    const float * __restrict x,
                    size_t d, size_t nx)
{

#pragma omp parallel for
    for (size_t i = 0; i < nx; i++) {
        nr[i] = sqrtf (fvec_norm_L2sqr (x + i * d, d));
    }
}

void fvec_norms_L2sqr (float * __restrict nr,
                       const float * __restrict x,
                       size_t d, size_t nx)
{
#pragma omp parallel for
    for (size_t i = 0; i < nx; i++)
        nr[i] = fvec_norm_L2sqr (x + i * d, d);
}



void fvec_renorm_L2 (size_t d, size_t nx, float * __restrict x)
{
#pragma omp parallel for
    for (size_t i = 0; i < nx; i++) {
        float * __restrict xi = x + i * d;

        float nr = fvec_norm_L2sqr (xi, d);

        if (nr > 0) {
            size_t j;
            const float inv_nr = 1.0 / sqrtf (nr);
            for (j = 0; j < d; j++)
                xi[j] *= inv_nr;
        }
    }
}




/***************************************************************************
 * KNN functions
 ***************************************************************************/

int parallel_policy_threshold = 65535;

/* Find the nearest neighbors for nx queries in a set of ny vectors */
static void knn_inner_product_sse (const float * x,
                        const float * y,
                        size_t d, size_t nx, size_t ny,
                        float_minheap_array_t * res,
                        const BitsetView bitset = nullptr)
{
    size_t k = res->k;
    size_t thread_max_num = omp_get_max_threads();

    if (ny > parallel_policy_threshold || (nx < thread_max_num / 2 && ny >= thread_max_num * 32)) {
        size_t block_x = std::min(
                get_L3_Size() / (d * sizeof(float) + thread_max_num * k * (sizeof(float) + sizeof(int64_t))),
                nx);
        if (block_x == 0) {
            block_x = 1;
        }

        size_t all_heap_size = block_x * k * thread_max_num;
        float *value = new float[all_heap_size];
        int64_t *labels = new int64_t[all_heap_size];

        for (size_t x_from = 0, x_to; x_from < nx; x_from = x_to) {
            x_to = std::min(nx, x_from + block_x);
            int size = x_to - x_from;
            int thread_heap_size = size * k;

            // init heap
            for (size_t i = 0; i < all_heap_size; i++) {
                value[i] = -1.0 / 0.0;
                labels[i] = -1;
            }

#pragma omp parallel for schedule(static)
            for (size_t j = 0; j < ny; j++) {
                if(!bitset || !bitset.test(j)) {
                    size_t thread_no = omp_get_thread_num();
                    const float *y_j = y + j * d;
                    const float *x_i = x + x_from * d;
                    for (size_t i = 0; i < size; i++) {
                        float disij = fvec_inner_product (x_i, y_j, d);
                        float * val_ = value + thread_no * thread_heap_size + i * k;
                        int64_t * ids_ = labels + thread_no * thread_heap_size + i * k;
                        if (disij > val_[0]) {
                            minheap_swap_top (k, val_, ids_, disij, j);
                        }
                        x_i += d;
                    }
                }
            }

            // merge heap
            for (size_t t = 1; t < thread_max_num; t++) {
                for (size_t i = 0; i < size; i++) {
                    float * __restrict value_x = value + i * k;
                    int64_t * __restrict labels_x = labels + i * k;
                    float *value_x_t = value_x + t * thread_heap_size;
                    int64_t *labels_x_t = labels_x + t * thread_heap_size;
                    for (size_t j = 0; j < k; j++) {
                        if (value_x_t[j] > value_x[0]) {
                            minheap_swap_top (k, value_x, labels_x, value_x_t[j], labels_x_t[j]);
                        }
                    }
                }
            }

            // sort
            for (size_t i = 0; i < size; i++) {
                float * value_x = value + i * k;
                int64_t * labels_x = labels + i * k;
                minheap_reorder (k, value_x, labels_x);
            }

            // copy result
            memcpy(res->val + x_from * k, value, thread_heap_size * sizeof(float));
            memcpy(res->ids + x_from * k, labels, thread_heap_size * sizeof(int64_t));
        }
        delete[] value;
        delete[] labels;

    } else {
        float * value = res->val;
        int64_t * labels = res->ids;

#pragma omp parallel for
        for (size_t i = 0; i < nx; i++) {
            const float *x_i = x + i * d;
            const float *y_j = y;

            float * __restrict val_ = value  + i * k;
            int64_t * __restrict ids_ = labels  + i * k;

            for (size_t j = 0; j < k; j++) {
                val_[j] = -1.0 / 0.0;
                ids_[j] = -1;
            }

            for (size_t j = 0; j < ny; j++) {
                if (!bitset || !bitset.test(j)) {
                    float disij = fvec_inner_product (x_i, y_j, d);
                    if (disij > val_[0]) {
                        minheap_swap_top (k, val_, ids_, disij, j);
                    }
                }
                y_j += d;
            }

            minheap_reorder (k, val_, ids_);
        }
    }
}

static void knn_L2sqr_sse (
                const float * x,
                const float * y,
                size_t d, size_t nx, size_t ny,
                float_maxheap_array_t * res,
                const BitsetView bitset = nullptr)
{
    size_t k = res->k;
    size_t thread_max_num = omp_get_max_threads();

    if (ny > parallel_policy_threshold || (nx < thread_max_num / 2 && ny >= thread_max_num * 32)) {
        size_t block_x = std::min(
                get_L3_Size() / (d * sizeof(float) + thread_max_num * k * (sizeof(float) + sizeof(int64_t))),
                nx);
        if (block_x == 0) {
            block_x = 1;
        }

        size_t all_heap_size = block_x * k * thread_max_num;
        float *value = new float[all_heap_size];
        int64_t *labels = new int64_t[all_heap_size];

        for (size_t x_from = 0, x_to; x_from < nx; x_from = x_to) {
            x_to = std::min(nx, x_from + block_x);
            int size = x_to - x_from;
            int thread_heap_size = size * k;

            // init heap
            for (size_t i = 0; i < all_heap_size; i++) {
                value[i] = 1.0 / 0.0;
                labels[i] = -1;
            }

#pragma omp parallel for schedule(static)
            for (size_t j = 0; j < ny; j++) {
                if(!bitset || !bitset.test(j)) {
                    size_t thread_no = omp_get_thread_num();
                    const float *y_j = y + j * d;
                    const float *x_i = x + x_from * d;
                    for (size_t i = 0; i < size; i++) {
                        float disij = fvec_L2sqr (x_i, y_j, d);
                        float * val_ = value + thread_no * thread_heap_size + i * k;
                        int64_t * ids_ = labels + thread_no * thread_heap_size + i * k;
                        if (disij < val_[0]) {
                            maxheap_swap_top (k, val_, ids_, disij, j);
                        }
                        x_i += d;
                    }
                }
            }

            // merge heap
            for (size_t t = 1; t < thread_max_num; t++) {
                for (size_t i = 0; i < size; i++) {
                    float * __restrict value_x = value + i * k;
                    int64_t * __restrict labels_x = labels + i * k;
                    float *value_x_t = value_x + t * thread_heap_size;
                    int64_t *labels_x_t = labels_x + t * thread_heap_size;
                    for (size_t j = 0; j < k; j++) {
                        if (value_x_t[j] < value_x[0]) {
                            maxheap_swap_top (k, value_x, labels_x, value_x_t[j], labels_x_t[j]);
                        }
                    }
                }
            }

            // sort
            for (size_t i = 0; i < size; i++) {
                float * value_x = value + i * k;
                int64_t * labels_x = labels + i * k;
                maxheap_reorder (k, value_x, labels_x);
            }

            // copy result
            memcpy(res->val + x_from * k, value, thread_heap_size * sizeof(float));
            memcpy(res->ids + x_from * k, labels, thread_heap_size * sizeof(int64_t));
        }
        delete[] value;
        delete[] labels;

    } else {

        float * value = res->val;
        int64_t * labels = res->ids;

#pragma omp parallel for
        for (size_t i = 0; i < nx; i++) {
            const float *x_i = x + i * d;
            const float *y_j = y;

            float * __restrict val_ = value  + i * k;
            int64_t * __restrict ids_ = labels  + i * k;

            for (size_t j = 0; j < k; j++) {
                val_[j] = 1.0 / 0.0;
                ids_[j] = -1;
            }

            for (size_t j = 0; j < ny; j++) {
                if (!bitset || !bitset.test(j)) {
                    float disij = fvec_L2sqr (x_i, y_j, d);
                    if (disij < val_[0]) {
                        maxheap_swap_top (k, val_, ids_, disij, j);
                    }
                }
                y_j += d;
            }

            maxheap_reorder (k, val_, ids_);
        }
    }
}

/** Find the nearest neighbors for nx queries in a set of ny vectors */
static void knn_inner_product_blas (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        float_minheap_array_t * res,
        const BitsetView bitset = nullptr)
{
    res->heapify ();

    // BLAS does not like empty matrices
    if (nx == 0 || ny == 0) return;

    size_t k = res->k;

    /* block sizes */
    const size_t bs_x = 4096, bs_y = 1024;
    // const size_t bs_x = 16, bs_y = 16;
    float *ip_block = new float[bs_x * bs_y];
    ScopeDeleter<float> del1(ip_block);;

    for (size_t i0 = 0; i0 < nx; i0 += bs_x) {
        size_t i1 = i0 + bs_x;
        if(i1 > nx) i1 = nx;

        for (size_t j0 = 0; j0 < ny; j0 += bs_y) {
            size_t j1 = j0 + bs_y;
            if (j1 > ny) j1 = ny;
            /* compute the actual dot products */
            {
                float one = 1, zero = 0;
                FINTEGER nyi = j1 - j0, nxi = i1 - i0, di = d;
                sgemm_ ("Transpose", "Not transpose", &nyi, &nxi, &di, &one,
                        y + j0 * d, &di,
                        x + i0 * d, &di, &zero,
                        ip_block, &nyi);
            }

            /* collect maxima */
#pragma omp parallel for
            for(size_t i = i0; i < i1; i++){
                float * __restrict simi = res->get_val(i);
                int64_t * __restrict idxi = res->get_ids (i);
                const float *ip_line = ip_block + (i - i0) * (j1 - j0);

                for(size_t j = j0; j < j1; j++){
                    if(!bitset || !bitset.test(j)){
                        float dis = *ip_line;

                        if(dis > simi[0]){
                            minheap_swap_top(k, simi, idxi, dis, j);
                        }
                    }
                    ip_line++;
                }
            }
        }
        InterruptCallback::check ();
    }
    res->reorder ();
}

// distance correction is an operator that can be applied to transform
// the distances
template<class DistanceCorrection>
static void knn_L2sqr_blas (const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        float_maxheap_array_t * res,
        const DistanceCorrection &corr,
        const BitsetView bitset = nullptr)
{
    res->heapify ();

    // BLAS does not like empty matrices
    if (nx == 0 || ny == 0) return;

    size_t k = res->k;

    /* block sizes */
    const size_t bs_x = 4096, bs_y = 1024;
    // const size_t bs_x = 16, bs_y = 16;
    float *ip_block = new float[bs_x * bs_y];
    float *x_norms = new float[nx];
    float *y_norms = new float[ny];
    ScopeDeleter<float> del1(ip_block), del3(x_norms), del2(y_norms);

    fvec_norms_L2sqr (x_norms, x, d, nx);
    fvec_norms_L2sqr (y_norms, y, d, ny);


    for (size_t i0 = 0; i0 < nx; i0 += bs_x) {
        size_t i1 = i0 + bs_x;
        if(i1 > nx) i1 = nx;

        for (size_t j0 = 0; j0 < ny; j0 += bs_y) {
            size_t j1 = j0 + bs_y;
            if (j1 > ny) j1 = ny;
            /* compute the actual dot products */
            {
                float one = 1, zero = 0;
                FINTEGER nyi = j1 - j0, nxi = i1 - i0, di = d;
                sgemm_ ("Transpose", "Not transpose", &nyi, &nxi, &di, &one,
                        y + j0 * d, &di,
                        x + i0 * d, &di, &zero,
                        ip_block, &nyi);
            }

            /* collect minima */
#pragma omp parallel for
            for (size_t i = i0; i < i1; i++) {
                float * __restrict simi = res->get_val(i);
                int64_t * __restrict idxi = res->get_ids (i);
                const float *ip_line = ip_block + (i - i0) * (j1 - j0);

                for (size_t j = j0; j < j1; j++) {
                    if(!bitset || !bitset.test(j)){
                        float ip = *ip_line;
                        float dis = x_norms[i] + y_norms[j] - 2 * ip;

                        // negative values can occur for identical vectors
                        // due to roundoff errors
                        if (dis < 0) dis = 0;

                        dis = corr (dis, i, j);

                        if (dis < simi[0]) {
                            maxheap_swap_top (k, simi, idxi, dis, j);
                        }
                    }
                    ip_line++;
                }
            }
        }
        InterruptCallback::check ();
    }
    res->reorder ();

}

template<class DistanceCorrection>
static void knn_jaccard_blas (const float * x,
                              const float * y,
                              size_t d, size_t nx, size_t ny,
                              float_maxheap_array_t * res,
                              const DistanceCorrection &corr,
                              const BitsetView bitset = nullptr)
{
    res->heapify ();

    // BLAS does not like empty matrices
    if (nx == 0 || ny == 0) return;

    size_t k = res->k;

    /* block sizes */
    const size_t bs_x = 4096, bs_y = 1024;
    // const size_t bs_x = 16, bs_y = 16;
    float *ip_block = new float[bs_x * bs_y];
    float *x_norms = new float[nx];
    float *y_norms = new float[ny];
    ScopeDeleter<float> del1(ip_block), del3(x_norms), del2(y_norms);

    fvec_norms_L2sqr (x_norms, x, d, nx);
    fvec_norms_L2sqr (y_norms, y, d, ny);


    for (size_t i0 = 0; i0 < nx; i0 += bs_x) {
        size_t i1 = i0 + bs_x;
        if(i1 > nx) i1 = nx;

        for (size_t j0 = 0; j0 < ny; j0 += bs_y) {
            size_t j1 = j0 + bs_y;
            if (j1 > ny) j1 = ny;
            /* compute the actual dot products */
            {
                float one = 1, zero = 0;
                FINTEGER nyi = j1 - j0, nxi = i1 - i0, di = d;
                sgemm_ ("Transpose", "Not transpose", &nyi, &nxi, &di, &one,
                        y + j0 * d, &di,
                        x + i0 * d, &di, &zero,
                        ip_block, &nyi);
            }

            /* collect minima */
#pragma omp parallel for
            for (size_t i = i0; i < i1; i++) {
                float * __restrict simi = res->get_val(i);
                int64_t * __restrict idxi = res->get_ids (i);
                const float *ip_line = ip_block + (i - i0) * (j1 - j0);

                for (size_t j = j0; j < j1; j++) {
                    if(!bitset || !bitset.test(j)){
                        float ip = *ip_line;
                        float dis = 1.0 - ip / (x_norms[i] + y_norms[j] - ip);

                        // negative values can occur for identical vectors
                        // due to roundoff errors
                        if (dis < 0) dis = 0;

                        dis = corr (dis, i, j);

                        if (dis < simi[0]) {
                            maxheap_swap_top (k, simi, idxi, dis, j);
                        }
                    }
                    ip_line++;
                }
            }
        }
        InterruptCallback::check ();
    }
    res->reorder ();
}







/*******************************************************
 * KNN driver functions
 *******************************************************/

int distance_compute_blas_threshold = 20;

void knn_inner_product (const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        float_minheap_array_t * res,
        const BitsetView bitset)
{
    if (nx < distance_compute_blas_threshold) {
        knn_inner_product_sse (x, y, d, nx, ny, res, bitset);
    } else {
        knn_inner_product_blas (x, y, d, nx, ny, res, bitset);
    }
}



struct NopDistanceCorrection {
  float operator()(float dis, size_t /*qno*/, size_t /*bno*/) const {
    return dis;
    }
};

void knn_L2sqr (const float * x,
                const float * y,
                size_t d, size_t nx, size_t ny,
                float_maxheap_array_t * res,
                const BitsetView bitset)
{
    if (nx < distance_compute_blas_threshold) {
        knn_L2sqr_sse (x, y, d, nx, ny, res, bitset);
    } else {
        NopDistanceCorrection nop;
        knn_L2sqr_blas (x, y, d, nx, ny, res, nop, bitset);
    }
}

void knn_jaccard (const float * x,
                  const float * y,
                  size_t d, size_t nx, size_t ny,
                  float_maxheap_array_t * res,
                  const BitsetView bitset)
{
    if (d % 4 != 0) {
//        knn_jaccard_sse (x, y, d, nx, ny, res);
        printf("dimension is not a multiple of 4!\n");
    } else {
        NopDistanceCorrection nop;
        knn_jaccard_blas (x, y, d, nx, ny, res, nop, bitset);
    }
}

struct BaseShiftDistanceCorrection {
    const float *base_shift;
    float operator()(float dis, size_t /*qno*/, size_t bno) const {
      return dis - base_shift[bno];
    }
};

void knn_L2sqr_base_shift (
         const float * x,
         const float * y,
         size_t d, size_t nx, size_t ny,
         float_maxheap_array_t * res,
         const float *base_shift)
{
    BaseShiftDistanceCorrection corr = {base_shift};
    knn_L2sqr_blas (x, y, d, nx, ny, res, corr);
}



/***************************************************************************
 * compute a subset of  distances
 ***************************************************************************/

/* compute the inner product between x and a subset y of ny vectors,
   whose indices are given by idy.  */
void fvec_inner_products_by_idx (float * __restrict ip,
                                 const float * x,
                                 const float * y,
                                 const int64_t * __restrict ids, /* for y vecs */
                                 size_t d, size_t nx, size_t ny)
{
#pragma omp parallel for
    for (size_t j = 0; j < nx; j++) {
        const int64_t * __restrict idsj = ids + j * ny;
        const float * xj = x + j * d;
        float * __restrict ipj = ip + j * ny;
        for (size_t i = 0; i < ny; i++) {
            if (idsj[i] < 0)
                continue;
            ipj[i] = fvec_inner_product (xj, y + d * idsj[i], d);
        }
    }
}



/* compute the inner product between x and a subset y of ny vectors,
   whose indices are given by idy.  */
void fvec_L2sqr_by_idx (float * __restrict dis,
                        const float * x,
                        const float * y,
                        const int64_t * __restrict ids, /* ids of y vecs */
                        size_t d, size_t nx, size_t ny)
{
#pragma omp parallel for
    for (size_t j = 0; j < nx; j++) {
        const int64_t * __restrict idsj = ids + j * ny;
        const float * xj = x + j * d;
        float * __restrict disj = dis + j * ny;
        for (size_t i = 0; i < ny; i++) {
            if (idsj[i] < 0)
                continue;
            disj[i] = fvec_L2sqr (xj, y + d * idsj[i], d);
        }
    }
}

void pairwise_indexed_L2sqr (
        size_t d, size_t n,
        const float * x, const int64_t *ix,
        const float * y, const int64_t *iy,
        float *dis)
{
#pragma omp parallel for
    for (size_t j = 0; j < n; j++) {
        if (ix[j] >= 0 && iy[j] >= 0) {
            dis[j] = fvec_L2sqr (x + d * ix[j], y + d * iy[j], d);
        }
    }
}

void pairwise_indexed_inner_product (
        size_t d, size_t n,
        const float * x, const int64_t *ix,
        const float * y, const int64_t *iy,
        float *dis)
{
#pragma omp parallel for
    for (size_t j = 0; j < n; j++) {
        if (ix[j] >= 0 && iy[j] >= 0) {
            dis[j] = fvec_inner_product (x + d * ix[j], y + d * iy[j], d);
        }
    }
}


/* Find the nearest neighbors for nx queries in a set of ny vectors
   indexed by ids. May be useful for re-ranking a pre-selected vector list */
void knn_inner_products_by_idx (const float * x,
                                const float * y,
                                const int64_t * ids,
                                size_t d, size_t nx, size_t ny,
                                float_minheap_array_t * res)
{
    size_t k = res->k;

#pragma omp parallel for
    for (size_t i = 0; i < nx; i++) {
        const float * x_ = x + i * d;
        const int64_t * idsi = ids + i * ny;
        size_t j;
        float * __restrict simi = res->get_val(i);
        int64_t * __restrict idxi = res->get_ids (i);
        minheap_heapify (k, simi, idxi);

        for (j = 0; j < ny; j++) {
            if (idsi[j] < 0) break;
            float ip = fvec_inner_product (x_, y + d * idsi[j], d);

            if (ip > simi[0]) {
                minheap_swap_top (k, simi, idxi, ip, idsi[j]);
            }
        }
        minheap_reorder (k, simi, idxi);
    }

}

void knn_L2sqr_by_idx (const float * x,
                       const float * y,
                       const int64_t * __restrict ids,
                       size_t d, size_t nx, size_t ny,
                       float_maxheap_array_t * res)
{
    size_t k = res->k;

#pragma omp parallel for
    for (size_t i = 0; i < nx; i++) {
        const float * x_ = x + i * d;
        const int64_t * __restrict idsi = ids + i * ny;
        float * __restrict simi = res->get_val(i);
        int64_t * __restrict idxi = res->get_ids (i);
        maxheap_heapify (res->k, simi, idxi);
        for (size_t j = 0; j < ny; j++) {
            float disij = fvec_L2sqr (x_, y + d * idsi[j], d);

            if (disij < simi[0]) {
                maxheap_swap_top (k, simi, idxi, disij, idsi[j]);
            }
        }
        maxheap_reorder (res->k, simi, idxi);
    }

}


void pairwise_L2sqr (int64_t d,
                     int64_t nq, const float *xq,
                     int64_t nb, const float *xb,
                     float *dis,
                     int64_t ldq, int64_t ldb, int64_t ldd)
{
    if (nq == 0 || nb == 0) return;
    if (ldq == -1) ldq = d;
    if (ldb == -1) ldb = d;
    if (ldd == -1) ldd = nb;

    // store in beginning of distance matrix to avoid malloc
    float *b_norms = dis;

#pragma omp parallel for
    for (int64_t i = 0; i < nb; i++)
        b_norms [i] = fvec_norm_L2sqr (xb + i * ldb, d);

#pragma omp parallel for
    for (int64_t i = 1; i < nq; i++) {
        float q_norm = fvec_norm_L2sqr (xq + i * ldq, d);
        for (int64_t j = 0; j < nb; j++)
            dis[i * ldd + j] = q_norm + b_norms [j];
    }

    {
        float q_norm = fvec_norm_L2sqr (xq, d);
        for (int64_t j = 0; j < nb; j++)
            dis[j] += q_norm;
    }

    {
        FINTEGER nbi = nb, nqi = nq, di = d, ldqi = ldq, ldbi = ldb, lddi = ldd;
        float one = 1.0, minus_2 = -2.0;

        sgemm_ ("Transposed", "Not transposed",
                &nbi, &nqi, &di,
                &minus_2,
                xb, &ldbi,
                xq, &ldqi,
                &one, dis, &lddi);
    }

}

/***************************************************************************
 * elkan
 ***************************************************************************/

void elkan_L2_sse (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        int64_t *ids, float *val) {

    if (nx == 0 || ny == 0) {
        return;
    }

    const size_t bs_y = 1024;
    float *data = (float *) malloc((bs_y * (bs_y - 1) / 2) * sizeof (float));

    for (size_t j0 = 0; j0 < ny; j0 += bs_y) {
        size_t j1 = j0 + bs_y;
        if (j1 > ny) j1 = ny;

        auto Y = [&](size_t i, size_t j) -> float& {
            assert(i != j);
            i -= j0, j -= j0;
            return (i > j) ? data[j + i * (i - 1) / 2] : data[i + j * (j - 1) / 2];
        };

#pragma omp parallel
        {
            int nt = omp_get_num_threads();
            int rank = omp_get_thread_num();
            for (size_t i = j0 + 1 + rank; i < j1; i += nt) {
                const float *y_i = y + i * d;
                for (size_t j = j0; j < i; j++) {
                    const float *y_j = y + j * d;
                    Y(i, j) = fvec_L2sqr(y_i, y_j, d);
                }
            }
        }

#pragma omp parallel for
        for (size_t i = 0; i < nx; i++) {
            const float *x_i = x + i * d;

            int64_t ids_i = j0;
            float val_i = fvec_L2sqr(x_i, y + j0 * d, d);
            float val_i_time_4 = val_i * 4;
            for (size_t j = j0 + 1; j < j1; j++) {
                if (val_i_time_4 <= Y(ids_i, j)) {
                    continue;
                }
                const float *y_j = y + j * d;
                float disij = fvec_L2sqr(x_i, y_j, d / 2);
                if (disij >= val_i) {
                    continue;
                }
                disij += fvec_L2sqr(x_i + d / 2, y_j + d / 2, d - d / 2);
                if (disij < val_i) {
                    ids_i = j;
                    val_i = disij;
                    val_i_time_4 = val_i * 4;
                }
            }

            if (j0 == 0 || val[i] > val_i) {
                val[i] = val_i;
                ids[i] = ids_i;
            }
        }
    }

    free(data);
}


} // namespace faiss
