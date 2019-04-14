/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "utils.h"

#include <cstdio>
#include <cassert>
#include <cstring>
#include <cmath>

#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <omp.h>

#include <immintrin.h>

#include <algorithm>
#include <vector>

#include "AuxIndexStructures.h"
#include "FaissAssert.h"



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

int sorgqr_(FINTEGER *m, FINTEGER *n, FINTEGER *k, float *a,
            FINTEGER *lda, float *tau, float *work,
            FINTEGER *lwork, FINTEGER *info);

int sgemv_(const char *trans, FINTEGER *m, FINTEGER *n, float *alpha,
           const float *a, FINTEGER *lda, const float *x, FINTEGER *incx,
           float *beta, float *y, FINTEGER *incy);

}


/**************************************************
 * Get some stats about the system
 **************************************************/

namespace faiss {

double getmillisecs () {
    struct timeval tv;
    gettimeofday (&tv, nullptr);
    return tv.tv_sec * 1e3 + tv.tv_usec * 1e-3;
}


#ifdef __linux__

size_t get_mem_usage_kb ()
{
    int pid = getpid ();
    char fname[256];
    snprintf (fname, 256, "/proc/%d/status", pid);
    FILE * f = fopen (fname, "r");
    FAISS_THROW_IF_NOT_MSG (f, "cannot open proc status file");
    size_t sz = 0;
    for (;;) {
        char buf [256];
        if (!fgets (buf, 256, f)) break;
        if (sscanf (buf, "VmRSS: %ld kB", &sz) == 1) break;
    }
    fclose (f);
    return sz;
}

#elif __APPLE__

size_t get_mem_usage_kb ()
{
    fprintf(stderr, "WARN: get_mem_usage_kb not implemented on the mac\n");
    return 0;
}

#endif



/**************************************************
 * Random data generation functions
 **************************************************/

RandomGenerator::RandomGenerator (long seed)
    : mt((unsigned int)seed) {}

int RandomGenerator::rand_int ()
{
    return mt() & 0x7fffffff;
}

long RandomGenerator::rand_long ()
{
    return long(rand_int()) | long(rand_int()) << 31;
}

int RandomGenerator::rand_int (int max)
{
    return mt() % max;
}

float RandomGenerator::rand_float ()
{
    return mt() / float(mt.max());
}

double RandomGenerator::rand_double ()
{
    return mt() / double(mt.max());
}


/***********************************************************************
 * Random functions in this C file only exist because Torch
 *  counterparts are slow and not multi-threaded.  Typical use is for
 *  more than 1-100 billion values. */


/* Generate a set of random floating point values such that x[i] in [0,1]
   multi-threading. For this reason, we rely on re-entreant functions.  */
void float_rand (float * x, size_t n, long seed)
{
    // only try to parallelize on large enough arrays
    const size_t nblock = n < 1024 ? 1 : 1024;

    RandomGenerator rng0 (seed);
    int a0 = rng0.rand_int (), b0 = rng0.rand_int ();

#pragma omp parallel for
    for (size_t j = 0; j < nblock; j++) {

        RandomGenerator rng (a0 + j * b0);

        const size_t istart = j * n / nblock;
        const size_t iend = (j + 1) * n / nblock;

        for (size_t i = istart; i < iend; i++)
            x[i] = rng.rand_float ();
    }
}


void float_randn (float * x, size_t n, long seed)
{
    // only try to parallelize on large enough arrays
    const size_t nblock = n < 1024 ? 1 : 1024;

    RandomGenerator rng0 (seed);
    int a0 = rng0.rand_int (), b0 = rng0.rand_int ();

#pragma omp parallel for
    for (size_t j = 0; j < nblock; j++) {
        RandomGenerator rng (a0 + j * b0);

        double a = 0, b = 0, s = 0;
        int state = 0;  /* generate two number per "do-while" loop */

        const size_t istart = j * n / nblock;
        const size_t iend = (j + 1) * n / nblock;

        for (size_t i = istart; i < iend; i++) {
            /* Marsaglia's method (see Knuth) */
            if (state == 0) {
                do {
                    a = 2.0 * rng.rand_double () - 1;
                    b = 2.0 * rng.rand_double () - 1;
                    s = a * a + b * b;
                } while (s >= 1.0);
                x[i] = a * sqrt(-2.0 * log(s) / s);
            }
            else
                x[i] = b * sqrt(-2.0 * log(s) / s);
            state = 1 - state;
        }
    }
}


/* Integer versions */
void long_rand (long * x, size_t n, long seed)
{
    // only try to parallelize on large enough arrays
    const size_t nblock = n < 1024 ? 1 : 1024;

    RandomGenerator rng0 (seed);
    int a0 = rng0.rand_int (), b0 = rng0.rand_int ();

#pragma omp parallel for
    for (size_t j = 0; j < nblock; j++) {

        RandomGenerator rng (a0 + j * b0);

        const size_t istart = j * n / nblock;
        const size_t iend = (j + 1) * n / nblock;
        for (size_t i = istart; i < iend; i++)
            x[i] = rng.rand_long ();
    }
}



void rand_perm (int *perm, size_t n, long seed)
{
    for (size_t i = 0; i < n; i++) perm[i] = i;

    RandomGenerator rng (seed);

    for (size_t i = 0; i + 1 < n; i++) {
        int i2 = i + rng.rand_int (n - i);
        std::swap(perm[i], perm[i2]);
    }
}




void byte_rand (uint8_t * x, size_t n, long seed)
{
    // only try to parallelize on large enough arrays
    const size_t nblock = n < 1024 ? 1 : 1024;

    RandomGenerator rng0 (seed);
    int a0 = rng0.rand_int (), b0 = rng0.rand_int ();

#pragma omp parallel for
    for (size_t j = 0; j < nblock; j++) {

        RandomGenerator rng (a0 + j * b0);

        const size_t istart = j * n / nblock;
        const size_t iend = (j + 1) * n / nblock;

        size_t i;
        for (i = istart; i < iend; i++)
            x[i] = rng.rand_long ();
    }
}



void reflection (const float * __restrict u,
                 float * __restrict x,
                 size_t n, size_t d, size_t nu)
{
    size_t i, j, l;
    for (i = 0; i < n; i++) {
        const float * up = u;
        for (l = 0; l < nu; l++) {
            float ip1 = 0, ip2 = 0;

            for (j = 0; j < d; j+=2) {
                ip1 += up[j] * x[j];
                ip2 += up[j+1] * x[j+1];
            }
            float ip = 2 * (ip1 + ip2);

            for (j = 0; j < d; j++)
                x[j] -= ip * up[j];
            up += d;
        }
        x += d;
    }
}


/* Reference implementation (slower) */
void reflection_ref (const float * u, float * x, size_t n, size_t d, size_t nu)
{
    size_t i, j, l;
    for (i = 0; i < n; i++) {
        const float * up = u;
        for (l = 0; l < nu; l++) {
            double ip = 0;

            for (j = 0; j < d; j++)
                ip += up[j] * x[j];
            ip *= 2;

            for (j = 0; j < d; j++)
                x[j] -= ip * up[j];

            up += d;
        }
        x += d;
    }
}





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

    for (size_t i0 = 0; i0 < nx; i0 += check_period) {
        size_t i1 = std::min(i0 + check_period, nx);

#pragma omp parallel for
        for (size_t i = i0; i < i1; i++) {
            const float * x_i = x + i * d;
            const float * y_j = y;

            float * __restrict simi = res->get_val(i);
            long * __restrict idxi = res->get_ids (i);

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

    for (size_t i0 = 0; i0 < nx; i0 += check_period) {
        size_t i1 = std::min(i0 + check_period, nx);

#pragma omp parallel for
        for (size_t i = i0; i < i1; i++) {
            const float * x_i = x + i * d;
            const float * y_j = y;
            size_t j;
            float * simi = res->get_val(i);
            long * idxi = res->get_ids (i);

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
                long * __restrict idxi = res->get_ids (i);
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
    if (d % 4 == 0 && nx < distance_compute_blas_threshold) {
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
    if (d % 4 == 0 && nx < distance_compute_blas_threshold) {
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
                                 const long * __restrict ids, /* for y vecs */
                                 size_t d, size_t nx, size_t ny)
{
#pragma omp parallel for
    for (size_t j = 0; j < nx; j++) {
        const long * __restrict idsj = ids + j * ny;
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
                        const long * __restrict ids, /* ids of y vecs */
                        size_t d, size_t nx, size_t ny)
{
#pragma omp parallel for
    for (size_t j = 0; j < nx; j++) {
        const long * __restrict idsj = ids + j * ny;
        const float * xj = x + j * d;
        float * __restrict disj = dis + j * ny;
        for (size_t i = 0; i < ny; i++) {
            if (idsj[i] < 0)
                continue;
            disj[i] = fvec_L2sqr (xj, y + d * idsj[i], d);
        }
    }
}





/* Find the nearest neighbors for nx queries in a set of ny vectors
   indexed by ids. May be useful for re-ranking a pre-selected vector list */
void knn_inner_products_by_idx (const float * x,
                                const float * y,
                                const long * ids,
                                size_t d, size_t nx, size_t ny,
                                float_minheap_array_t * res)
{
    size_t k = res->k;

#pragma omp parallel for
    for (size_t i = 0; i < nx; i++) {
        const float * x_ = x + i * d;
        const long * idsi = ids + i * ny;
        size_t j;
        float * __restrict simi = res->get_val(i);
        long * __restrict idxi = res->get_ids (i);
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
                       const long * __restrict ids,
                       size_t d, size_t nx, size_t ny,
                       float_maxheap_array_t * res)
{
    size_t k = res->k;

#pragma omp parallel for
    for (size_t i = 0; i < nx; i++) {
        const float * x_ = x + i * d;
        const long * __restrict idsi = ids + i * ny;
        float * __restrict simi = res->get_val(i);
        long * __restrict idxi = res->get_ids (i);
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

    { // merge the partial results
        int npres = partial_results.size();
        // count
        for (size_t i = 0; i < nx; i++) {
            for (int j = 0; j < npres; j++)
                result->lims[i] += partial_results[j]->queries[i].nres;
        }
        result->do_allocation ();
        for (int j = 0; j < npres; j++) {
            partial_results[j]->set_result (true);
            delete partial_results[j];
        }

        // reset the limits
        for (size_t i = nx; i > 0; i--) {
            result->lims [i] = result->lims [i - 1];
        }
        result->lims [0] = 0;
    }
}


template <bool compute_l2>
static void range_search_sse (const float * x,
                const float * y,
                size_t d, size_t nx, size_t ny,
                float radius,
                RangeSearchResult *res)
{
    FAISS_THROW_IF_NOT (d % 4 == 0);

    size_t check_period = InterruptCallback::get_period_hint (ny * d);
    bool interrupted = false;

#pragma omp parallel
    {
        RangeSearchPartialResult pres (res);

        for (size_t i0 = 0; i0 < nx; i0 += check_period) {
            size_t i1 = std::min(i0 + check_period, nx);

#pragma omp for
            for (size_t i = i0; i < i1; i++) {
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

            if (InterruptCallback::is_interrupted ()) {
                interrupted = true;
            }

#pragma omp barrier
            if (interrupted) {
                break;
            }
        }
        pres.finalize ();
    }

    if (interrupted) {
        FAISS_THROW_MSG ("computation interrupted");
    }
}





void range_search_L2sqr (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        float radius,
        RangeSearchResult *res)
{

    if (d % 4 == 0 && nx < distance_compute_blas_threshold) {
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

    if (d % 4 == 0 && nx < distance_compute_blas_threshold) {
        range_search_sse<false> (x, y, d, nx, ny, radius, res);
    } else {
        range_search_blas<false> (x, y, d, nx, ny, radius, res);
    }
}



/***************************************************************************
 * Some matrix manipulation functions
 ***************************************************************************/


/* This function exists because the Torch counterpart is extremly slow
   (not multi-threaded + unexpected overhead even in single thread).
   It is here to implement the usual property |x-y|^2=|x|^2+|y|^2-2<x|y>  */
void inner_product_to_L2sqr (float * __restrict dis,
                             const float * nr1,
                             const float * nr2,
                             size_t n1, size_t n2)
{

#pragma omp parallel for
    for (size_t j = 0 ; j < n1 ; j++) {
        float * disj = dis + j * n2;
        for (size_t i = 0 ; i < n2 ; i++)
            disj[i] = nr1[j] + nr2[i] - 2 * disj[i];
    }
}


void matrix_qr (int m, int n, float *a)
{
    FAISS_THROW_IF_NOT (m >= n);
    FINTEGER mi = m, ni = n, ki = mi < ni ? mi : ni;
    std::vector<float> tau (ki);
    FINTEGER lwork = -1, info;
    float work_size;

    sgeqrf_ (&mi, &ni, a, &mi, tau.data(),
             &work_size, &lwork, &info);
    lwork = size_t(work_size);
    std::vector<float> work (lwork);

    sgeqrf_ (&mi, &ni, a, &mi,
             tau.data(), work.data(), &lwork, &info);

    sorgqr_ (&mi, &ni, &ki, a, &mi, tau.data(),
             work.data(), &lwork, &info);

}


void pairwise_L2sqr (long d,
                     long nq, const float *xq,
                     long nb, const float *xb,
                     float *dis,
                     long ldq, long ldb, long ldd)
{
    if (nq == 0 || nb == 0) return;
    if (ldq == -1) ldq = d;
    if (ldb == -1) ldb = d;
    if (ldd == -1) ldd = nb;

    // store in beginning of distance matrix to avoid malloc
    float *b_norms = dis;

#pragma omp parallel for
    for (long i = 0; i < nb; i++)
        b_norms [i] = fvec_norm_L2sqr (xb + i * ldb, d);

#pragma omp parallel for
    for (long i = 1; i < nq; i++) {
        float q_norm = fvec_norm_L2sqr (xq + i * ldq, d);
        for (long j = 0; j < nb; j++)
            dis[i * ldd + j] = q_norm + b_norms [j];
    }

    {
        float q_norm = fvec_norm_L2sqr (xq, d);
        for (long j = 0; j < nb; j++)
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
 * Kmeans subroutine
 ***************************************************************************/

// a bit above machine epsilon for float16

#define EPS (1 / 1024.)

/* For k-means, compute centroids given assignment of vectors to centroids */
int km_update_centroids (const float * x,
                         float * centroids,
                         long * assign,
                         size_t d, size_t k, size_t n,
                         size_t k_frozen)
{
    k -= k_frozen;
    centroids += k_frozen * d;

    std::vector<size_t> hassign(k);
    memset (centroids, 0, sizeof(*centroids) * d * k);

#pragma omp parallel
    {
        int nt = omp_get_num_threads();
        int rank = omp_get_thread_num();
        // this thread is taking care of centroids c0:c1
        size_t c0 = (k * rank) / nt;
        size_t c1 = (k * (rank + 1)) / nt;
        const float *xi = x;
        size_t nacc = 0;

        for (size_t i = 0; i < n; i++) {
            long ci = assign[i];
            assert (ci >= 0 && ci < k + k_frozen);
            ci -= k_frozen;
            if (ci >= c0 && ci < c1)  {
                float * c = centroids + ci * d;
                hassign[ci]++;
                for (size_t j = 0; j < d; j++)
                    c[j] += xi[j];
                nacc++;
            }
            xi += d;
        }

    }

#pragma omp parallel for
    for (size_t ci = 0; ci < k; ci++) {
        float * c = centroids + ci * d;
        float ni = (float) hassign[ci];
        if (ni != 0) {
            for (size_t j = 0; j < d; j++)
                c[j] /= ni;
        }
    }

    /* Take care of void clusters */
    size_t nsplit = 0;
    RandomGenerator rng (1234);
    for (size_t ci = 0; ci < k; ci++) {
        if (hassign[ci] == 0) { /* need to redefine a centroid */
            size_t cj;
            for (cj = 0; 1; cj = (cj + 1) % k) {
                /* probability to pick this cluster for split */
                float p = (hassign[cj] - 1.0) / (float) (n - k);
                float r = rng.rand_float ();
                if (r < p) {
                    break; /* found our cluster to be split */
                }
            }
            memcpy (centroids+ci*d, centroids+cj*d, sizeof(*centroids) * d);

            /* small symmetric pertubation. Much better than  */
            for (size_t j = 0; j < d; j++) {
                if (j % 2 == 0) {
                    centroids[ci * d + j] *= 1 + EPS;
                    centroids[cj * d + j] *= 1 - EPS;
                } else {
                    centroids[ci * d + j] *= 1 - EPS;
                    centroids[cj * d + j] *= 1 + EPS;
                }
            }

            /* assume even split of the cluster */
            hassign[ci] = hassign[cj] / 2;
            hassign[cj] -= hassign[ci];
            nsplit++;
        }
    }

    return nsplit;
}

#undef EPS



/***************************************************************************
 * Result list routines
 ***************************************************************************/


void ranklist_handle_ties (int k, long *idx, const float *dis)
{
    float prev_dis = -1e38;
    int prev_i = -1;
    for (int i = 0; i < k; i++) {
        if (dis[i] != prev_dis) {
            if (i > prev_i + 1) {
                // sort between prev_i and i - 1
                std::sort (idx + prev_i, idx + i);
            }
            prev_i = i;
            prev_dis = dis[i];
        }
    }
}

size_t merge_result_table_with (size_t n, size_t k,
                                long *I0, float *D0,
                                const long *I1, const float *D1,
                                bool keep_min,
                                long translation)
{
    size_t n1 = 0;

#pragma omp parallel reduction(+:n1)
    {
        std::vector<long> tmpI (k);
        std::vector<float> tmpD (k);

#pragma omp for
        for (size_t i = 0; i < n; i++) {
            long *lI0 = I0 + i * k;
            float *lD0 = D0 + i * k;
            const long *lI1 = I1 + i * k;
            const float *lD1 = D1 + i * k;
            size_t r0 = 0;
            size_t r1 = 0;

            if (keep_min) {
                for (size_t j = 0; j < k; j++) {

                    if (lI0[r0] >= 0 && lD0[r0] < lD1[r1]) {
                        tmpD[j] = lD0[r0];
                        tmpI[j] = lI0[r0];
                        r0++;
                    } else if (lD1[r1] >= 0) {
                        tmpD[j] = lD1[r1];
                        tmpI[j] = lI1[r1] + translation;
                        r1++;
                    } else { // both are NaNs
                        tmpD[j] = NAN;
                        tmpI[j] = -1;
                    }
                }
            } else {
                for (size_t j = 0; j < k; j++) {
                    if (lI0[r0] >= 0 && lD0[r0] > lD1[r1]) {
                        tmpD[j] = lD0[r0];
                        tmpI[j] = lI0[r0];
                        r0++;
                    } else if (lD1[r1] >= 0) {
                        tmpD[j] = lD1[r1];
                        tmpI[j] = lI1[r1] + translation;
                        r1++;
                    } else { // both are NaNs
                        tmpD[j] = NAN;
                        tmpI[j] = -1;
                    }
                }
            }
            n1 += r1;
            memcpy (lD0, tmpD.data(), sizeof (lD0[0]) * k);
            memcpy (lI0, tmpI.data(), sizeof (lI0[0]) * k);
        }
    }

    return n1;
}



size_t ranklist_intersection_size (size_t k1, const long *v1,
                                   size_t k2, const long *v2_in)
{
    if (k2 > k1) return ranklist_intersection_size (k2, v2_in, k1, v1);
    long *v2 = new long [k2];
    memcpy (v2, v2_in, sizeof (long) * k2);
    std::sort (v2, v2 + k2);
    { // de-dup v2
        long prev = -1;
        size_t wp = 0;
        for (size_t i = 0; i < k2; i++) {
            if (v2 [i] != prev) {
                v2[wp++] = prev = v2 [i];
            }
        }
        k2 = wp;
    }
    const long seen_flag = 1L << 60;
    size_t count = 0;
    for (size_t i = 0; i < k1; i++) {
        long q = v1 [i];
        size_t i0 = 0, i1 = k2;
        while (i0 + 1 < i1) {
            size_t imed = (i1 + i0) / 2;
            long piv = v2 [imed] & ~seen_flag;
            if (piv <= q) i0 = imed;
            else          i1 = imed;
        }
        if (v2 [i0] == q) {
            count++;
            v2 [i0] |= seen_flag;
        }
    }
    delete [] v2;

    return count;
}

double imbalance_factor (int k, const int *hist) {
    double tot = 0, uf = 0;

    for (int i = 0 ; i < k ; i++) {
        tot += hist[i];
        uf += hist[i] * (double) hist[i];
    }
    uf = uf * k / (tot * tot);

    return uf;
}


double imbalance_factor (int n, int k, const long *assign) {
    std::vector<int> hist(k, 0);
    for (int i = 0; i < n; i++) {
        hist[assign[i]]++;
    }

    return imbalance_factor (k, hist.data());
}



int ivec_hist (size_t n, const int * v, int vmax, int *hist) {
    memset (hist, 0, sizeof(hist[0]) * vmax);
    int nout = 0;
    while (n--) {
        if (v[n] < 0 || v[n] >= vmax) nout++;
        else hist[v[n]]++;
    }
    return nout;
}


void bincode_hist(size_t n, size_t nbits, const uint8_t *codes, int *hist)
{
    FAISS_THROW_IF_NOT (nbits % 8 == 0);
    size_t d = nbits / 8;
    std::vector<int> accu(d * 256);
    const uint8_t *c = codes;
    for (size_t i = 0; i < n; i++)
        for(int j = 0; j < d; j++)
            accu[j * 256 + *c++]++;
    memset (hist, 0, sizeof(*hist) * nbits);
    for (int i = 0; i < d; i++) {
        const int *ai = accu.data() + i * 256;
        int * hi = hist + i * 8;
        for (int j = 0; j < 256; j++)
            for (int k = 0; k < 8; k++)
                if ((j >> k) & 1)
                    hi[k] += ai[j];
    }

}



size_t ivec_checksum (size_t n, const int *a)
{
    size_t cs = 112909;
    while (n--) cs = cs * 65713 + a[n] * 1686049;
    return cs;
}


namespace {
    struct ArgsortComparator {
        const float *vals;
        bool operator() (const size_t a, const size_t b) const {
            return vals[a] < vals[b];
        }
    };

    struct SegmentS {
        size_t i0; // begin pointer in the permutation array
        size_t i1; // end
        size_t len() const {
            return i1 - i0;
        }
    };

    // see https://en.wikipedia.org/wiki/Merge_algorithm#Parallel_merge
    // extended to > 1 merge thread

    // merges 2 ranges that should be consecutive on the source into
    // the union of the two on the destination
    template<typename T>
    void parallel_merge (const T *src, T *dst,
                         SegmentS &s1, SegmentS & s2, int nt,
                         const ArgsortComparator & comp) {
        if (s2.len() > s1.len()) { // make sure that s1 larger than s2
            std::swap(s1, s2);
        }

        // compute sub-ranges for each thread
        SegmentS s1s[nt], s2s[nt], sws[nt];
        s2s[0].i0 = s2.i0;
        s2s[nt - 1].i1 = s2.i1;

        // not sure parallel actually helps here
#pragma omp parallel for num_threads(nt)
        for (int t = 0; t < nt; t++) {
            s1s[t].i0 = s1.i0 + s1.len() * t / nt;
            s1s[t].i1 = s1.i0 + s1.len() * (t + 1) / nt;

            if (t + 1 < nt) {
                T pivot = src[s1s[t].i1];
                size_t i0 = s2.i0, i1 = s2.i1;
                while (i0 + 1 < i1) {
                    size_t imed = (i1 + i0) / 2;
                    if (comp (pivot, src[imed])) {i1 = imed; }
                    else                         {i0 = imed; }
                }
                s2s[t].i1 = s2s[t + 1].i0 = i1;
            }
        }
        s1.i0 = std::min(s1.i0, s2.i0);
        s1.i1 = std::max(s1.i1, s2.i1);
        s2 = s1;
        sws[0].i0 = s1.i0;
        for (int t = 0; t < nt; t++) {
            sws[t].i1 = sws[t].i0 + s1s[t].len() + s2s[t].len();
            if (t + 1 < nt) {
                sws[t + 1].i0 = sws[t].i1;
            }
        }
        assert(sws[nt - 1].i1 == s1.i1);

        // do the actual merging
#pragma omp parallel for num_threads(nt)
        for (int t = 0; t < nt; t++) {
            SegmentS sw = sws[t];
            SegmentS s1t = s1s[t];
            SegmentS s2t = s2s[t];
            if (s1t.i0 < s1t.i1 && s2t.i0 < s2t.i1) {
                for (;;) {
                    // assert (sw.len() == s1t.len() + s2t.len());
                    if (comp(src[s1t.i0], src[s2t.i0])) {
                        dst[sw.i0++] = src[s1t.i0++];
                        if (s1t.i0 == s1t.i1) break;
                    } else {
                        dst[sw.i0++] = src[s2t.i0++];
                        if (s2t.i0 == s2t.i1) break;
                    }
                }
            }
            if (s1t.len() > 0) {
                assert(s1t.len() == sw.len());
                memcpy(dst + sw.i0, src + s1t.i0, s1t.len() * sizeof(dst[0]));
            } else if (s2t.len() > 0) {
                assert(s2t.len() == sw.len());
                memcpy(dst + sw.i0, src + s2t.i0, s2t.len() * sizeof(dst[0]));
            }
        }
    }

};

void fvec_argsort (size_t n, const float *vals,
                    size_t *perm)
{
    for (size_t i = 0; i < n; i++) perm[i] = i;
    ArgsortComparator comp = {vals};
    std::sort (perm, perm + n, comp);
}

void fvec_argsort_parallel (size_t n, const float *vals,
                            size_t *perm)
{
    size_t * perm2 = new size_t[n];
    // 2 result tables, during merging, flip between them
    size_t *permB = perm2, *permA = perm;

    int nt = omp_get_max_threads();
    { // prepare correct permutation so that the result ends in perm
      // at final iteration
        int nseg = nt;
        while (nseg > 1) {
            nseg = (nseg + 1) / 2;
            std::swap (permA, permB);
        }
    }

#pragma omp parallel
    for (size_t i = 0; i < n; i++) permA[i] = i;

    ArgsortComparator comp = {vals};

    SegmentS segs[nt];

    // independent sorts
#pragma omp parallel for
    for (int t = 0; t < nt; t++) {
        size_t i0 = t * n / nt;
        size_t i1 = (t + 1) * n / nt;
        SegmentS seg = {i0, i1};
        std::sort (permA + seg.i0, permA + seg.i1, comp);
        segs[t] = seg;
    }
    int prev_nested = omp_get_nested();
    omp_set_nested(1);

    int nseg = nt;
    while (nseg > 1) {
        int nseg1 = (nseg + 1) / 2;
        int sub_nt = nseg % 2 == 0 ? nt : nt - 1;
        int sub_nseg1 = nseg / 2;

#pragma omp parallel for num_threads(nseg1)
        for (int s = 0; s < nseg; s += 2) {
            if (s + 1 == nseg) { // otherwise isolated segment
                memcpy(permB + segs[s].i0, permA + segs[s].i0,
                       segs[s].len() * sizeof(size_t));
            } else {
                int t0 = s * sub_nt / sub_nseg1;
                int t1 = (s + 1) * sub_nt / sub_nseg1;
                printf("merge %d %d, %d threads\n", s, s + 1, t1 - t0);
                parallel_merge(permA, permB, segs[s], segs[s + 1],
                               t1 - t0, comp);
            }
        }
        for (int s = 0; s < nseg; s += 2)
            segs[s / 2] = segs[s];
        nseg = nseg1;
        std::swap (permA, permB);
    }
    assert (permA == perm);
    omp_set_nested(prev_nested);
    delete [] perm2;
}


















const float *fvecs_maybe_subsample (
          size_t d, size_t *n, size_t nmax, const float *x,
          bool verbose, long seed)
{

    if (*n <= nmax) return x; // nothing to do

    size_t n2 = nmax;
    if (verbose) {
        printf ("  Input training set too big (max size is %ld), sampling "
                "%ld / %ld vectors\n", nmax, n2, *n);
    }
    std::vector<int> subset (*n);
    rand_perm (subset.data (), *n, seed);
    float *x_subset = new float[n2 * d];
    for (long i = 0; i < n2; i++)
        memcpy (&x_subset[i * d],
                &x[subset[i] * size_t(d)],
                sizeof (x[0]) * d);
    *n = n2;
    return x_subset;
}


void binary_to_real(size_t d, const uint8_t *x_in, float *x_out) {
    for (size_t i = 0; i < d; ++i) {
        x_out[i] = 2 * ((x_in[i >> 3] >> (i & 7)) & 1) - 1;
    }
}

void real_to_binary(size_t d, const float *x_in, uint8_t *x_out) {
  for (size_t i = 0; i < d / 8; ++i) {
    uint8_t b = 0;
    for (int j = 0; j < 8; ++j) {
      if (x_in[8 * i + j] > 0) {
        b |= (1 << j);
      }
    }
    x_out[i] = b;
  }
}


// from Python's stringobject.c
uint64_t hash_bytes (const uint8_t *bytes, long n) {
    const uint8_t *p = bytes;
    uint64_t x = (uint64_t)(*p) << 7;
    long len = n;
    while (--len >= 0) {
        x = (1000003*x) ^ *p++;
    }
    x ^= n;
    return x;
}


bool check_openmp() {
  omp_set_num_threads(10);

  if (omp_get_max_threads() != 10) {
    return false;
  }

  std::vector<int> nt_per_thread(10);
  size_t sum = 0;
  bool in_parallel = true;
#pragma omp parallel reduction(+: sum)
  {
    if (!omp_in_parallel()) {
      in_parallel = false;
    }

    int nt = omp_get_num_threads();
    int rank = omp_get_thread_num();

    nt_per_thread[rank] = nt;
#pragma omp for
    for(int i = 0; i < 1000 * 1000 * 10; i++) {
      sum += i;
    }
  }

  if (!in_parallel) {
    return false;
  }
  if (nt_per_thread[0] != 10) {
    return false;
  }
  if (sum == 0) {
    return false;
  }

  return true;
}

} // namespace faiss
