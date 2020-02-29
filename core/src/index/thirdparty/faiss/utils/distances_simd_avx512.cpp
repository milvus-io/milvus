
// -*- c++ -*-

#include <faiss/utils/distances_avx512.h>
#include <faiss/utils/distances.h>
#include <faiss/impl/FaissAssert.h>

#include <cstdio>
#include <cassert>
#include <cstring>
#include <cmath>

#include <immintrin.h>

namespace faiss {

#ifdef __SSE__
// reads 0 <= d < 4 floats as __m128
static inline __m128 masked_read (int d, const float *x) {
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
    return _mm_load_ps(buf);
    // cannot use AVX2 _mm_mask_set1_epi32
}
#endif

#if (defined(__AVX512F__) && defined(__AVX512DQ__))

float fvec_inner_product_avx512 (const float * x,
                          const float * y,
                          size_t d)
{
    __m512 msum0 = _mm512_setzero_ps();

    while (d >= 16) {
        __m512 mx = _mm512_loadu_ps (x); x += 16;
        __m512 my = _mm512_loadu_ps (y); y += 16;
        msum0 = _mm512_add_ps (msum0, _mm512_mul_ps (mx, my));
        d -= 16;
    }

    __m256 msum1 = _mm512_extractf32x8_ps(msum0, 1);
    msum1 +=       _mm512_extractf32x8_ps(msum0, 0);

    if (d >= 8) {
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

float fvec_L2sqr_avx512 (const float * x,
                  const float * y,
                  size_t d)
{
    __m512 msum0 = _mm512_setzero_ps();

    while (d >= 16) {
        __m512 mx = _mm512_loadu_ps (x); x += 16;
        __m512 my = _mm512_loadu_ps (y); y += 16;
        const __m512 a_m_b1 = mx - my;
        msum0 += a_m_b1 * a_m_b1;
        d -= 16;
    }

    __m256 msum1 = _mm512_extractf32x8_ps(msum0, 1);
    msum1 +=       _mm512_extractf32x8_ps(msum0, 0);

    if (d >= 8) {
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

float fvec_L1_avx512 (const float * x, const float * y, size_t d)
{
    __m512 msum0 = _mm512_setzero_ps();
    __m512 signmask0 = __m512(_mm512_set1_epi32 (0x7fffffffUL));

    while (d >= 16) {
        __m512 mx = _mm512_loadu_ps (x); x += 16;
        __m512 my = _mm512_loadu_ps (y); y += 16;
        const __m512 a_m_b = mx - my;
        msum0 += _mm512_and_ps(signmask0, a_m_b);
        d -= 16;
    }

    __m256 msum1 = _mm512_extractf32x8_ps(msum0, 1);
    msum1 +=       _mm512_extractf32x8_ps(msum0, 0);
    __m256 signmask1 = __m256(_mm256_set1_epi32 (0x7fffffffUL));

    if (d >= 8) {
        __m256 mx = _mm256_loadu_ps (x); x += 8;
        __m256 my = _mm256_loadu_ps (y); y += 8;
        const __m256 a_m_b = mx - my;
        msum1 += _mm256_and_ps(signmask1, a_m_b);
        d -= 8;
    }

    __m128 msum2 = _mm256_extractf128_ps(msum1, 1);
    msum2 +=       _mm256_extractf128_ps(msum1, 0);
    __m128 signmask2 = __m128(_mm_set1_epi32 (0x7fffffffUL));

    if (d >= 4) {
        __m128 mx = _mm_loadu_ps (x); x += 4;
        __m128 my = _mm_loadu_ps (y); y += 4;
        const __m128 a_m_b = mx - my;
        msum2 += _mm_and_ps(signmask2, a_m_b);
        d -= 4;
    }

    if (d > 0) {
        __m128 mx = masked_read (d, x);
        __m128 my = masked_read (d, y);
        __m128 a_m_b = mx - my;
        msum2 += _mm_and_ps(signmask2, a_m_b);
    }

    msum2 = _mm_hadd_ps (msum2, msum2);
    msum2 = _mm_hadd_ps (msum2, msum2);
    return  _mm_cvtss_f32 (msum2);
}

float fvec_Linf_avx512 (const float * x, const float * y, size_t d)
{
    __m512 msum0 = _mm512_setzero_ps();
    __m512 signmask0 = __m512(_mm512_set1_epi32 (0x7fffffffUL));

    while (d >= 16) {
        __m512 mx = _mm512_loadu_ps (x); x += 16;
        __m512 my = _mm512_loadu_ps (y); y += 16;
        const __m512 a_m_b = mx - my;
        msum0 = _mm512_max_ps(msum0, _mm512_and_ps(signmask0, a_m_b));
        d -= 16;
    }

    __m256 msum1 = _mm512_extractf32x8_ps(msum0, 1);
    msum1 = _mm256_max_ps (msum1, _mm512_extractf32x8_ps(msum0, 0));
    __m256 signmask1 = __m256(_mm256_set1_epi32 (0x7fffffffUL));

    if (d >= 8) {
        __m256 mx = _mm256_loadu_ps (x); x += 8;
        __m256 my = _mm256_loadu_ps (y); y += 8;
        const __m256 a_m_b = mx - my;
        msum1 = _mm256_max_ps(msum1, _mm256_and_ps(signmask1, a_m_b));
        d -= 8;
    }

    __m128 msum2 = _mm256_extractf128_ps(msum1, 1);
    msum2 = _mm_max_ps (msum2, _mm256_extractf128_ps(msum1, 0));
    __m128 signmask2 = __m128(_mm_set1_epi32 (0x7fffffffUL));

    if (d >= 4) {
        __m128 mx = _mm_loadu_ps (x); x += 4;
        __m128 my = _mm_loadu_ps (y); y += 4;
        const __m128 a_m_b = mx - my;
        msum2 = _mm_max_ps(msum2, _mm_and_ps(signmask2, a_m_b));
        d -= 4;
    }

    if (d > 0) {
        __m128 mx = masked_read (d, x);
        __m128 my = masked_read (d, y);
        __m128 a_m_b = mx - my;
        msum2 = _mm_max_ps(msum2, _mm_and_ps(signmask2, a_m_b));
    }

    msum2 = _mm_max_ps(_mm_movehl_ps(msum2, msum2), msum2);
    msum2 = _mm_max_ps(msum2, _mm_shuffle_ps (msum2, msum2, 1));
    return  _mm_cvtss_f32 (msum2);
}

#else

float fvec_inner_product_avx512 (const float * x,
                          const float * y,
                          size_t d)
{
    FAISS_ASSERT(false);
    return 0.0;
}

float fvec_L2sqr_avx512 (const float * x,
                  const float * y,
                  size_t d)
{
    FAISS_ASSERT(false);
    return 0.0;
}

float fvec_L1_avx512 (const float * x, const float * y, size_t d)
{
    FAISS_ASSERT(false);
    return 0.0;
}

float fvec_Linf_avx512 (const float * x, const float * y, size_t d)
{
    FAISS_ASSERT(false);
    return 0.0;
}

#endif

} // namespace faiss
