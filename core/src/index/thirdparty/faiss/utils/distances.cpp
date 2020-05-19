/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/utils/distances.h>

#include <cstdio>
#include <cassert>
#include <cstring>
#include <cmath>

#include <omp.h>

#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/impl/FaissAssert.h>



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



/* Find the nearest neighbors for nx queries in a set of ny vectors */
static void knn_inner_product_sse (const float * x,
                        const float * y,
                        size_t d, size_t nx, size_t ny,
                        float_minheap_array_t * res)
{
    size_t k = res->k;
    size_t check_period = InterruptCallback::get_period_hint (ny * d);

    check_period *= omp_get_max_threads();

    for (size_t i0 = 0; i0 < nx; i0 += check_period) {
        size_t i1 = std::min(i0 + check_period, nx);

#pragma omp parallel for
        for (size_t i = i0; i < i1; i++) {
            const float * x_i = x + i * d;
            const float * y_j = y;

            float * __restrict simi = res->get_val(i);
            int64_t * __restrict idxi = res->get_ids (i);

            minheap_heapify (k, simi, idxi);

            for (size_t j = 0; j < ny; j++) {
                float ip = fvec_inner_product (x_i, y_j, d);

                if (ip > simi[0]) {
                    minheap_pop (k, simi, idxi);
                    minheap_push (k, simi, idxi, ip, j);
                }
                y_j += d;
            }
            minheap_reorder (k, simi, idxi);
        }
        InterruptCallback::check ();
    }

}

static void knn_L2sqr_sse (
                const float * x,
                const float * y,
                size_t d, size_t nx, size_t ny,
                float_maxheap_array_t * res)
{
    size_t k = res->k;

    size_t check_period = InterruptCallback::get_period_hint (ny * d);
    check_period *= omp_get_max_threads();

    for (size_t i0 = 0; i0 < nx; i0 += check_period) {
        size_t i1 = std::min(i0 + check_period, nx);

#pragma omp parallel for
        for (size_t i = i0; i < i1; i++) {
            const float * x_i = x + i * d;
            const float * y_j = y;
            size_t j;
            float * simi = res->get_val(i);
            int64_t * idxi = res->get_ids (i);

            maxheap_heapify (k, simi, idxi);
            for (j = 0; j < ny; j++) {
                float disij = fvec_L2sqr (x_i, y_j, d);

                if (disij < simi[0]) {
                    maxheap_pop (k, simi, idxi);
                    maxheap_push (k, simi, idxi, disij, j);
                }
                y_j += d;
            }
            maxheap_reorder (k, simi, idxi);
        }
        InterruptCallback::check ();
    }

}


/** Find the nearest neighbors for nx queries in a set of ny vectors */
static void knn_inner_product_blas (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        float_minheap_array_t * res)
{
    res->heapify ();

    // BLAS does not like empty matrices
    if (nx == 0 || ny == 0) return;

    /* block sizes */
    const size_t bs_x = 4096, bs_y = 1024;
    // const size_t bs_x = 16, bs_y = 16;
    std::unique_ptr<float[]> ip_block(new float[bs_x * bs_y]);

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
                        ip_block.get(), &nyi);
            }

            /* collect maxima */
            res->addn (j1 - j0, ip_block.get(), j0, i0, i1 - i0);
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
        const DistanceCorrection &corr)
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
                    float ip = *ip_line++;
                    float dis = x_norms[i] + y_norms[j] - 2 * ip;

                    // negative values can occur for identical vectors
                    // due to roundoff errors
                    if (dis < 0) dis = 0;

                    dis = corr (dis, i, j);

                    if (dis < simi[0]) {
                        maxheap_pop (k, simi, idxi);
                        maxheap_push (k, simi, idxi, dis, j);
                    }
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
        float_minheap_array_t * res)
{
    if (nx < distance_compute_blas_threshold) {
        knn_inner_product_sse (x, y, d, nx, ny, res);
    } else {
        knn_inner_product_blas (x, y, d, nx, ny, res);
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
                float_maxheap_array_t * res)
{
    if (nx < distance_compute_blas_threshold) {
        knn_L2sqr_sse (x, y, d, nx, ny, res);
    } else {
        NopDistanceCorrection nop;
        knn_L2sqr_blas (x, y, d, nx, ny, res, nop);
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
                minheap_pop (k, simi, idxi);
                minheap_push (k, simi, idxi, ip, idsi[j]);
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
                maxheap_pop (k, simi, idxi);
                maxheap_push (k, simi, idxi, disij, idsi[j]);
            }
        }
        maxheap_reorder (res->k, simi, idxi);
    }

}





/***************************************************************************
 * Range search
 ***************************************************************************/

/** Find the nearest neighbors for nx queries in a set of ny vectors
 * compute_l2 = compute pairwise squared L2 distance rather than inner prod
 */
template <bool compute_l2>
static void range_search_blas (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        float radius,
        RangeSearchResult *result)
{

    // BLAS does not like empty matrices
    if (nx == 0 || ny == 0) return;

    /* block sizes */
    const size_t bs_x = 4096, bs_y = 1024;
    // const size_t bs_x = 16, bs_y = 16;
    float *ip_block = new float[bs_x * bs_y];
    ScopeDeleter<float> del0(ip_block);

    float *x_norms = nullptr, *y_norms = nullptr;
    ScopeDeleter<float> del1, del2;
    if (compute_l2) {
        x_norms = new float[nx];
        del1.set (x_norms);
        fvec_norms_L2sqr (x_norms, x, d, nx);

        y_norms = new float[ny];
        del2.set (y_norms);
        fvec_norms_L2sqr (y_norms, y, d, ny);
    }

    std::vector <RangeSearchPartialResult *> partial_results;

    for (size_t j0 = 0; j0 < ny; j0 += bs_y) {
        size_t j1 = j0 + bs_y;
        if (j1 > ny) j1 = ny;
        RangeSearchPartialResult * pres = new RangeSearchPartialResult (result);
        partial_results.push_back (pres);

        for (size_t i0 = 0; i0 < nx; i0 += bs_x) {
            size_t i1 = i0 + bs_x;
            if(i1 > nx) i1 = nx;

            /* compute the actual dot products */
            {
                float one = 1, zero = 0;
                FINTEGER nyi = j1 - j0, nxi = i1 - i0, di = d;
                sgemm_ ("Transpose", "Not transpose", &nyi, &nxi, &di, &one,
                        y + j0 * d, &di,
                        x + i0 * d, &di, &zero,
                        ip_block, &nyi);
            }


            for (size_t i = i0; i < i1; i++) {
                const float *ip_line = ip_block + (i - i0) * (j1 - j0);

                RangeQueryResult & qres = pres->new_result (i);

                for (size_t j = j0; j < j1; j++) {
                    float ip = *ip_line++;
                    if (compute_l2) {
                        float dis =  x_norms[i] + y_norms[j] - 2 * ip;
                        if (dis < radius) {
                            qres.add (dis, j);
                        }
                    } else {
                        if (ip > radius) {
                            qres.add (ip, j);
                        }
                    }
                }
            }
        }
        InterruptCallback::check ();
    }

    RangeSearchPartialResult::merge (partial_results);
}


template <bool compute_l2>
static void range_search_sse (const float * x,
                const float * y,
                size_t d, size_t nx, size_t ny,
                float radius,
                RangeSearchResult *res)
{

#pragma omp parallel
    {
        RangeSearchPartialResult pres (res);

#pragma omp for
        for (size_t i = 0; i < nx; i++) {
            const float * x_ = x + i * d;
            const float * y_ = y;
            size_t j;

            RangeQueryResult & qres = pres.new_result (i);

            for (j = 0; j < ny; j++) {
                if (compute_l2) {
                    float disij = fvec_L2sqr (x_, y_, d);
                    if (disij < radius) {
                        qres.add (disij, j);
                    }
                } else {
                    float ip = fvec_inner_product (x_, y_, d);
                    if (ip > radius) {
                        qres.add (ip, j);
                    }
                }
                y_ += d;
            }

        }
        pres.finalize ();
    }

    // check just at the end because the use case is typically just
    // when the nb of queries is low.
    InterruptCallback::check();
}





void range_search_L2sqr (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        float radius,
        RangeSearchResult *res)
{

    if (nx < distance_compute_blas_threshold) {
        range_search_sse<true> (x, y, d, nx, ny, radius, res);
    } else {
        range_search_blas<true> (x, y, d, nx, ny, radius, res);
    }
}

void range_search_inner_product (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        float radius,
        RangeSearchResult *res)
{

    if (nx < distance_compute_blas_threshold) {
        range_search_sse<false> (x, y, d, nx, ny, radius, res);
    } else {
        range_search_blas<false> (x, y, d, nx, ny, radius, res);
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


} // namespace faiss
