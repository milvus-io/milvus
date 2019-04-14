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

#ifdef __SSE__
#include <immintrin.h>
#endif

#ifdef __aarch64__
#include  <arm_neon.h>
#endif

#include <omp.h>



/**************************************************
 * Get some stats about the system
 **************************************************/

namespace faiss {

#ifdef __AVX__
#define USE_AVX
#endif


/*********************************************************
 * Optimized distance computations
 *********************************************************/


/* Functions to compute:
   - L2 distance between 2 vectors
   - inner product between 2 vectors
   - L2 norm of a vector

   The functions should probably not be invoked when a large number of
   vectors are be processed in batch (in which case Matrix multiply
   is faster), but may be useful for comparing vectors isolated in
   memory.

   Works with any vectors of any dimension, even unaligned (in which
   case they are slower).

*/


/*********************************************************
 * Reference implementations
 */

/* same without SSE */
float fvec_L2sqr_ref (const float * x,
                     const float * y,
                     size_t d)
{
    size_t i;
    float res = 0;
    for (i = 0; i < d; i++) {
        const float tmp = x[i] - y[i];
       res += tmp * tmp;
    }
    return res;
}

float fvec_inner_product_ref (const float * x,
                             const float * y,
                             size_t d)
{
    size_t i;
    float res = 0;
    for (i = 0; i < d; i++)
       res += x[i] * y[i];
    return res;
}

float fvec_norm_L2sqr_ref (const float *x, size_t d)
{
    size_t i;
    double res = 0;
    for (i = 0; i < d; i++)
       res += x[i] * x[i];
    return res;
}


void fvec_L2sqr_ny_ref (float * dis,
                    const float * x,
                    const float * y,
                    size_t d, size_t ny)
{
    for (size_t i = 0; i < ny; i++) {
        dis[i] = fvec_L2sqr (x, y, d);
        y += d;
    }
}




/*********************************************************
 * SSE and AVX implementations
 */

#ifdef __SSE__

// reads 0 <= d < 4 floats as __m128
static inline __m128 masked_read (int d, const float *x)
{
    assert (0 <= d && d < 4);
    __attribute__((__aligned__(16))) float buf[4] = {0, 0, 0, 0};
    switch (d) {
      case 3:
        buf[2] = x[2];
      case 2:
        buf[1] = x[1];
      case 1:
        buf[0] = x[0];
    }
    return _mm_load_ps (buf);
    // cannot use AVX2 _mm_mask_set1_epi32
}

float fvec_norm_L2sqr (const float *  x,
                      size_t d)
{
    __m128 mx;
    __m128 msum1 = _mm_setzero_ps();

    while (d >= 4) {
        mx = _mm_loadu_ps (x); x += 4;
        msum1 = _mm_add_ps (msum1, _mm_mul_ps (mx, mx));
        d -= 4;
    }

    mx = masked_read (d, x);
    msum1 = _mm_add_ps (msum1, _mm_mul_ps (mx, mx));

    msum1 = _mm_hadd_ps (msum1, msum1);
    msum1 = _mm_hadd_ps (msum1, msum1);
    return  _mm_cvtss_f32 (msum1);
}

namespace {

float sqr (float x) {
    return x * x;
}


void fvec_L2sqr_ny_D1 (float * dis, const float * x,
                       const float * y, size_t ny)
{
    float x0s = x[0];
    __m128 x0 = _mm_set_ps (x0s, x0s, x0s, x0s);

    size_t i;
    for (i = 0; i + 3 < ny; i += 4) {
        __m128 tmp, accu;
        tmp = x0 - _mm_loadu_ps (y); y += 4;
        accu = tmp * tmp;
        dis[i] = _mm_cvtss_f32 (accu);
        tmp = _mm_shuffle_ps (accu, accu, 1);
        dis[i + 1] = _mm_cvtss_f32 (tmp);
        tmp = _mm_shuffle_ps (accu, accu, 2);
        dis[i + 2] = _mm_cvtss_f32 (tmp);
        tmp = _mm_shuffle_ps (accu, accu, 3);
        dis[i + 3] = _mm_cvtss_f32 (tmp);
    }
    while (i < ny) { // handle non-multiple-of-4 case
        dis[i++] = sqr(x0s - *y++);
    }
}


void fvec_L2sqr_ny_D2 (float * dis, const float * x,
                       const float * y, size_t ny)
{
    __m128 x0 = _mm_set_ps (x[1], x[0], x[1], x[0]);

    size_t i;
    for (i = 0; i + 1 < ny; i += 2) {
        __m128 tmp, accu;
        tmp = x0 - _mm_loadu_ps (y); y += 4;
        accu = tmp * tmp;
        accu = _mm_hadd_ps (accu, accu);
        dis[i] = _mm_cvtss_f32 (accu);
        accu = _mm_shuffle_ps (accu, accu, 3);
        dis[i + 1] = _mm_cvtss_f32 (accu);
    }
    if (i < ny) { // handle odd case
        dis[i] = sqr(x[0] - y[0]) + sqr(x[1] - y[1]);
    }
}



void fvec_L2sqr_ny_D4 (float * dis, const float * x,
                        const float * y, size_t ny)
{
    __m128 x0 = _mm_loadu_ps(x);

    for (size_t i = 0; i < ny; i++) {
        __m128 tmp, accu;
        tmp = x0 - _mm_loadu_ps (y); y += 4;
        accu = tmp * tmp;
        accu = _mm_hadd_ps (accu, accu);
        accu = _mm_hadd_ps (accu, accu);
        dis[i] = _mm_cvtss_f32 (accu);
    }
}


void fvec_L2sqr_ny_D8 (float * dis, const float * x,
                        const float * y, size_t ny)
{
    __m128 x0 = _mm_loadu_ps(x);
    __m128 x1 = _mm_loadu_ps(x + 4);

    for (size_t i = 0; i < ny; i++) {
        __m128 tmp, accu;
        tmp = x0 - _mm_loadu_ps (y); y += 4;
        accu = tmp * tmp;
        tmp = x1 - _mm_loadu_ps (y); y += 4;
        accu += tmp * tmp;
        accu = _mm_hadd_ps (accu, accu);
        accu = _mm_hadd_ps (accu, accu);
        dis[i] = _mm_cvtss_f32 (accu);
    }
}


void fvec_L2sqr_ny_D12 (float * dis, const float * x,
                        const float * y, size_t ny)
{
    __m128 x0 = _mm_loadu_ps(x);
    __m128 x1 = _mm_loadu_ps(x + 4);
    __m128 x2 = _mm_loadu_ps(x + 8);

    for (size_t i = 0; i < ny; i++) {
        __m128 tmp, accu;
        tmp = x0 - _mm_loadu_ps (y); y += 4;
        accu = tmp * tmp;
        tmp = x1 - _mm_loadu_ps (y); y += 4;
        accu += tmp * tmp;
        tmp = x2 - _mm_loadu_ps (y); y += 4;
        accu += tmp * tmp;
        accu = _mm_hadd_ps (accu, accu);
        accu = _mm_hadd_ps (accu, accu);
        dis[i] = _mm_cvtss_f32 (accu);
    }
}


} // anonymous namespace

void fvec_L2sqr_ny (float * dis, const float * x,
                        const float * y, size_t d, size_t ny) {
    // optimized for a few special cases
    switch(d) {
    case 1:
        fvec_L2sqr_ny_D1 (dis, x, y, ny);
        return;
    case 2:
        fvec_L2sqr_ny_D2 (dis, x, y, ny);
        return;
    case 4:
        fvec_L2sqr_ny_D4 (dis, x, y, ny);
        return;
    case 8:
        fvec_L2sqr_ny_D8 (dis, x, y, ny);
        return;
    case 12:
        fvec_L2sqr_ny_D12 (dis, x, y, ny);
        return;
    default:
        fvec_L2sqr_ny_ref (dis, x, y, d, ny);
        return;
    }
}



#endif

#ifdef USE_AVX

// reads 0 <= d < 8 floats as __m256
static inline __m256 masked_read_8 (int d, const float *x)
{
    assert (0 <= d && d < 8);
    if (d < 4) {
        __m256 res = _mm256_setzero_ps ();
        res = _mm256_insertf128_ps (res, masked_read (d, x), 0);
        return res;
    } else {
        __m256 res = _mm256_setzero_ps ();
        res = _mm256_insertf128_ps (res, _mm_loadu_ps (x), 0);
        res = _mm256_insertf128_ps (res, masked_read (d - 4, x + 4), 1);
        return res;
    }
}

float fvec_inner_product (const float * x,
                          const float * y,
                          size_t d)
{
    __m256 msum1 = _mm256_setzero_ps();

    while (d >= 8) {
        __m256 mx = _mm256_loadu_ps (x); x += 8;
        __m256 my = _mm256_loadu_ps (y); y += 8;
        msum1 = _mm256_add_ps (msum1, _mm256_mul_ps (mx, my));
        d -= 8;
    }

    __m128 msum2 = _mm256_extractf128_ps(msum1, 1);
    msum2 +=       _mm256_extractf128_ps(msum1, 0);

    if (d >= 4) {
        __m128 mx = _mm_loadu_ps (x); x += 4;
        __m128 my = _mm_loadu_ps (y); y += 4;
        msum2 = _mm_add_ps (msum2, _mm_mul_ps (mx, my));
        d -= 4;
    }

    if (d > 0) {
        __m128 mx = masked_read (d, x);
        __m128 my = masked_read (d, y);
        msum2 = _mm_add_ps (msum2, _mm_mul_ps (mx, my));
    }

    msum2 = _mm_hadd_ps (msum2, msum2);
    msum2 = _mm_hadd_ps (msum2, msum2);
    return  _mm_cvtss_f32 (msum2);
}

float fvec_L2sqr (const float * x,
                 const float * y,
                 size_t d)
{
    __m256 msum1 = _mm256_setzero_ps();

    while (d >= 8) {
        __m256 mx = _mm256_loadu_ps (x); x += 8;
        __m256 my = _mm256_loadu_ps (y); y += 8;
        const __m256 a_m_b1 = mx - my;
        msum1 += a_m_b1 * a_m_b1;
        d -= 8;
    }

    __m128 msum2 = _mm256_extractf128_ps(msum1, 1);
    msum2 +=       _mm256_extractf128_ps(msum1, 0);

    if (d >= 4) {
        __m128 mx = _mm_loadu_ps (x); x += 4;
        __m128 my = _mm_loadu_ps (y); y += 4;
        const __m128 a_m_b1 = mx - my;
        msum2 += a_m_b1 * a_m_b1;
        d -= 4;
    }

    if (d > 0) {
        __m128 mx = masked_read (d, x);
        __m128 my = masked_read (d, y);
        __m128 a_m_b1 = mx - my;
        msum2 += a_m_b1 * a_m_b1;
    }

    msum2 = _mm_hadd_ps (msum2, msum2);
    msum2 = _mm_hadd_ps (msum2, msum2);
    return  _mm_cvtss_f32 (msum2);
}

#elif defined(__SSE__)

/* SSE-implementation of L2 distance */
float fvec_L2sqr (const float * x,
                 const float * y,
                 size_t d)
{
    __m128 msum1 = _mm_setzero_ps();

    while (d >= 4) {
        __m128 mx = _mm_loadu_ps (x); x += 4;
        __m128 my = _mm_loadu_ps (y); y += 4;
        const __m128 a_m_b1 = mx - my;
        msum1 += a_m_b1 * a_m_b1;
        d -= 4;
    }

    if (d > 0) {
        // add the last 1, 2 or 3 values
        __m128 mx = masked_read (d, x);
        __m128 my = masked_read (d, y);
        __m128 a_m_b1 = mx - my;
        msum1 += a_m_b1 * a_m_b1;
    }

    msum1 = _mm_hadd_ps (msum1, msum1);
    msum1 = _mm_hadd_ps (msum1, msum1);
    return  _mm_cvtss_f32 (msum1);
}


float fvec_inner_product (const float * x,
                         const float * y,
                         size_t d)
{
    __m128 mx, my;
    __m128 msum1 = _mm_setzero_ps();

    while (d >= 4) {
        mx = _mm_loadu_ps (x); x += 4;
        my = _mm_loadu_ps (y); y += 4;
        msum1 = _mm_add_ps (msum1, _mm_mul_ps (mx, my));
        d -= 4;
    }

    // add the last 1, 2, or 3 values
    mx = masked_read (d, x);
    my = masked_read (d, y);
    __m128 prod = _mm_mul_ps (mx, my);

    msum1 = _mm_add_ps (msum1, prod);

    msum1 = _mm_hadd_ps (msum1, msum1);
    msum1 = _mm_hadd_ps (msum1, msum1);
    return  _mm_cvtss_f32 (msum1);
}

#elif defined(__aarch64__)


float fvec_L2sqr (const float * x,
                  const float * y,
                  size_t d)
{
    if (d & 3) return fvec_L2sqr_ref (x, y, d);
    float32x4_t accu = vdupq_n_f32 (0);
    for (size_t i = 0; i < d; i += 4) {
        float32x4_t xi = vld1q_f32 (x + i);
        float32x4_t yi = vld1q_f32 (y + i);
        float32x4_t sq = vsubq_f32 (xi, yi);
        accu = vfmaq_f32 (accu, sq, sq);
    }
    float32x4_t a2 = vpaddq_f32 (accu, accu);
    return vdups_laneq_f32 (a2, 0) + vdups_laneq_f32 (a2, 1);
}

float fvec_inner_product (const float * x,
                          const float * y,
                          size_t d)
{
    if (d & 3) return fvec_inner_product_ref (x, y, d);
    float32x4_t accu = vdupq_n_f32 (0);
    for (size_t i = 0; i < d; i += 4) {
        float32x4_t xi = vld1q_f32 (x + i);
        float32x4_t yi = vld1q_f32 (y + i);
        accu = vfmaq_f32 (accu, xi, yi);
    }
    float32x4_t a2 = vpaddq_f32 (accu, accu);
    return vdups_laneq_f32 (a2, 0) + vdups_laneq_f32 (a2, 1);
}

float fvec_norm_L2sqr (const float *x, size_t d)
{
    if (d & 3) return fvec_norm_L2sqr_ref (x, d);
    float32x4_t accu = vdupq_n_f32 (0);
    for (size_t i = 0; i < d; i += 4) {
        float32x4_t xi = vld1q_f32 (x + i);
        accu = vfmaq_f32 (accu, xi, xi);
    }
    float32x4_t a2 = vpaddq_f32 (accu, accu);
    return vdups_laneq_f32 (a2, 0) + vdups_laneq_f32 (a2, 1);
}

// not optimized for ARM
void fvec_L2sqr_ny (float * dis, const float * x,
                        const float * y, size_t d, size_t ny) {
    fvec_L2sqr_ny_ref (dis, x, y, d, ny);
}


#else
// scalar implementation

float fvec_L2sqr (const float * x,
                  const float * y,
                  size_t d)
{
    return fvec_L2sqr_ref (x, y, d);
}

float fvec_inner_product (const float * x,
                             const float * y,
                             size_t d)
{
    return fvec_inner_product_ref (x, y, d);
}

float fvec_norm_L2sqr (const float *x, size_t d)
{
    return fvec_norm_L2sqr_ref (x, d);
}

void fvec_L2sqr_ny (float * dis, const float * x,
                        const float * y, size_t d, size_t ny) {
    fvec_L2sqr_ny_ref (dis, x, y, d, ny);
}


#endif




















/***************************************************************************
 * heavily optimized table computations
 ***************************************************************************/


static inline void fvec_madd_ref (size_t n, const float *a,
                           float bf, const float *b, float *c) {
    for (size_t i = 0; i < n; i++)
        c[i] = a[i] + bf * b[i];
}

#ifdef __SSE__

static inline void fvec_madd_sse (size_t n, const float *a,
                                  float bf, const float *b, float *c) {
    n >>= 2;
    __m128 bf4 = _mm_set_ps1 (bf);
    __m128 * a4 = (__m128*)a;
    __m128 * b4 = (__m128*)b;
    __m128 * c4 = (__m128*)c;

    while (n--) {
        *c4 = _mm_add_ps (*a4, _mm_mul_ps (bf4, *b4));
        b4++;
        a4++;
        c4++;
    }
}

void fvec_madd (size_t n, const float *a,
                float bf, const float *b, float *c)
{
    if ((n & 3) == 0 &&
        ((((long)a) | ((long)b) | ((long)c)) & 15) == 0)
        fvec_madd_sse (n, a, bf, b, c);
    else
        fvec_madd_ref (n, a, bf, b, c);
}

#else

void fvec_madd (size_t n, const float *a,
                float bf, const float *b, float *c)
{
    fvec_madd_ref (n, a, bf, b, c);
}

#endif

static inline int fvec_madd_and_argmin_ref (size_t n, const float *a,
                                         float bf, const float *b, float *c) {
    float vmin = 1e20;
    int imin = -1;

    for (size_t i = 0; i < n; i++) {
        c[i] = a[i] + bf * b[i];
        if (c[i] < vmin) {
            vmin = c[i];
            imin = i;
        }
    }
    return imin;
}

#ifdef __SSE__

static inline int fvec_madd_and_argmin_sse (
        size_t n, const float *a,
        float bf, const float *b, float *c) {
    n >>= 2;
    __m128 bf4 = _mm_set_ps1 (bf);
    __m128 vmin4 = _mm_set_ps1 (1e20);
    __m128i imin4 = _mm_set1_epi32 (-1);
    __m128i idx4 = _mm_set_epi32 (3, 2, 1, 0);
    __m128i inc4 = _mm_set1_epi32 (4);
    __m128 * a4 = (__m128*)a;
    __m128 * b4 = (__m128*)b;
    __m128 * c4 = (__m128*)c;

    while (n--) {
        __m128 vc4 = _mm_add_ps (*a4, _mm_mul_ps (bf4, *b4));
        *c4 = vc4;
        __m128i mask = (__m128i)_mm_cmpgt_ps (vmin4, vc4);
        // imin4 = _mm_blendv_epi8 (imin4, idx4, mask); // slower!

        imin4 = _mm_or_si128 (_mm_and_si128 (mask, idx4),
                              _mm_andnot_si128 (mask, imin4));
        vmin4 = _mm_min_ps (vmin4, vc4);
        b4++;
        a4++;
        c4++;
        idx4 = _mm_add_epi32 (idx4, inc4);
    }

    // 4 values -> 2
    {
        idx4 = _mm_shuffle_epi32 (imin4, 3 << 2 | 2);
        __m128 vc4 = _mm_shuffle_ps (vmin4, vmin4, 3 << 2 | 2);
        __m128i mask = (__m128i)_mm_cmpgt_ps (vmin4, vc4);
        imin4 = _mm_or_si128 (_mm_and_si128 (mask, idx4),
                              _mm_andnot_si128 (mask, imin4));
        vmin4 = _mm_min_ps (vmin4, vc4);
    }
    // 2 values -> 1
    {
        idx4 = _mm_shuffle_epi32 (imin4, 1);
        __m128 vc4 = _mm_shuffle_ps (vmin4, vmin4, 1);
        __m128i mask = (__m128i)_mm_cmpgt_ps (vmin4, vc4);
        imin4 = _mm_or_si128 (_mm_and_si128 (mask, idx4),
                              _mm_andnot_si128 (mask, imin4));
        // vmin4 = _mm_min_ps (vmin4, vc4);
    }
    return _mm_cvtsi128_si32 (imin4);
}


int fvec_madd_and_argmin (size_t n, const float *a,
                          float bf, const float *b, float *c)
{
    if ((n & 3) == 0 &&
        ((((long)a) | ((long)b) | ((long)c)) & 15) == 0)
        return fvec_madd_and_argmin_sse (n, a, bf, b, c);
    else
        return fvec_madd_and_argmin_ref (n, a, bf, b, c);
}

#else

int fvec_madd_and_argmin (size_t n, const float *a,
                          float bf, const float *b, float *c)
{
  return fvec_madd_and_argmin_ref (n, a, bf, b, c);
}

#endif




} // namespace faiss
