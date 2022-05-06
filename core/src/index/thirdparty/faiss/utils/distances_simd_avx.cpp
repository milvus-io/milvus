
// -*- c++ -*-

#include <faiss/utils/distances_avx.h>
#include <faiss/impl/FaissAssert.h>

#include <cstdio>
#include <cassert>
#include <cstring>
#include <cmath>

#include <immintrin.h>

namespace faiss {

extern const uint8_t lookup8bit[256];

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

#ifdef __AVX__

// reads 0 <= d < 8 floats as __m256
static inline __m256 masked_read_8 (int d, const float* x) {
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

float fvec_inner_product_avx (const float* x, const float* y, size_t d) {
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

float fvec_L2sqr_avx (const float* x, const float* y, size_t d) {
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

float fvec_L1_avx (const float * x, const float * y, size_t d)
{
    __m256 msum1 = _mm256_setzero_ps();
    __m256 signmask = __m256(_mm256_set1_epi32 (0x7fffffffUL));

    while (d >= 8) {
        __m256 mx = _mm256_loadu_ps (x); x += 8;
        __m256 my = _mm256_loadu_ps (y); y += 8;
        const __m256 a_m_b = mx - my;
        msum1 += _mm256_and_ps(signmask, a_m_b);
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

float fvec_Linf_avx (const float* x, const float* y, size_t d) {
    __m256 msum1 = _mm256_setzero_ps();
    __m256 signmask = __m256(_mm256_set1_epi32 (0x7fffffffUL));

    while (d >= 8) {
        __m256 mx = _mm256_loadu_ps (x); x += 8;
        __m256 my = _mm256_loadu_ps (y); y += 8;
        const __m256 a_m_b = mx - my;
        msum1 = _mm256_max_ps(msum1, _mm256_and_ps(signmask, a_m_b));
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

#define DECLARE_LOOKUP \
const __m256i lookup = _mm256_setr_epi8( \
        /* 0 */ 0, /* 1 */ 1, /* 2 */ 1, /* 3 */ 2, \
        /* 4 */ 1, /* 5 */ 2, /* 6 */ 2, /* 7 */ 3, \
        /* 8 */ 1, /* 9 */ 2, /* a */ 2, /* b */ 3, \
        /* c */ 2, /* d */ 3, /* e */ 3, /* f */ 4, \
        /* 0 */ 0, /* 1 */ 1, /* 2 */ 1, /* 3 */ 2, \
        /* 4 */ 1, /* 5 */ 2, /* 6 */ 2, /* 7 */ 3, \
        /* 8 */ 1, /* 9 */ 2, /* a */ 2, /* b */ 3, \
        /* c */ 2, /* d */ 3, /* e */ 3, /* f */ 4 \
);

int popcnt_AVX2_lookup(const uint8_t* data, const size_t n) {
    size_t i = 0;

    const __m256i low_mask = _mm256_set1_epi8(0x0f);

    __m256i acc = _mm256_setzero_si256();

#define ITER { \
        const __m256i vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i)); \
        const __m256i lo  = _mm256_and_si256(vec, low_mask); \
        const __m256i hi  = _mm256_and_si256(_mm256_srli_epi16(vec, 4), low_mask); \
        const __m256i popcnt1 = _mm256_shuffle_epi8(lookup, lo); \
        const __m256i popcnt2 = _mm256_shuffle_epi8(lookup, hi); \
        local = _mm256_add_epi8(local, popcnt1); \
        local = _mm256_add_epi8(local, popcnt2); \
        i += 32; \
    }

    DECLARE_LOOKUP
    while (i + 8*32 <= n) {
        __m256i local = _mm256_setzero_si256();
        ITER ITER ITER ITER
        ITER ITER ITER ITER
        acc = _mm256_add_epi64(acc, _mm256_sad_epu8(local, _mm256_setzero_si256()));
    }

    __m256i local = _mm256_setzero_si256();

    while (i + 32 <= n) {
        ITER;
    }

    acc = _mm256_add_epi64(acc, _mm256_sad_epu8(local, _mm256_setzero_si256()));

#undef ITER

    int result = 0;

    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 0));
    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 1));
    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 2));
    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 3));

    for (/**/; i < n; i++) {
        result += lookup8bit[data[i]];
    }

    return result;
}

int xor_popcnt_AVX2_lookup(const uint8_t* data1, const uint8_t* data2, const size_t n) {

    size_t i = 0;


    const __m256i low_mask = _mm256_set1_epi8(0x0f);

    __m256i acc = _mm256_setzero_si256();

#define ITER { \
        const __m256i s1  = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data1 + i)); \
        const __m256i s2  = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data2 + i)); \
        const __m256i vec = _mm256_xor_si256(s1, s2);\
        const __m256i lo  = _mm256_and_si256(vec, low_mask); \
        const __m256i hi  = _mm256_and_si256(_mm256_srli_epi16(vec, 4), low_mask); \
        const __m256i popcnt1 = _mm256_shuffle_epi8(lookup, lo); \
        const __m256i popcnt2 = _mm256_shuffle_epi8(lookup, hi); \
        local = _mm256_add_epi8(local, popcnt1); \
        local = _mm256_add_epi8(local, popcnt2); \
        i += 32; \
    }

    DECLARE_LOOKUP
    while (i + 8*32 <= n) {
        __m256i local = _mm256_setzero_si256();
        ITER ITER ITER ITER
        ITER ITER ITER ITER
        acc = _mm256_add_epi64(acc, _mm256_sad_epu8(local, _mm256_setzero_si256()));
    }

    __m256i local = _mm256_setzero_si256();

    while (i + 32 <= n) {
        ITER;
    }

    acc = _mm256_add_epi64(acc, _mm256_sad_epu8(local, _mm256_setzero_si256()));

#undef ITER

    int result = 0;

    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 0));
    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 1));
    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 2));
    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 3));

    for (/**/; i < n; i++) {
        result += lookup8bit[data1[i]^data2[i]];
    }

    return result;
}

int or_popcnt_AVX2_lookup(const uint8_t* data1, const uint8_t* data2, const size_t n) {

    size_t i = 0;


    const __m256i low_mask = _mm256_set1_epi8(0x0f);

    __m256i acc = _mm256_setzero_si256();

#define ITER { \
        const __m256i s1  = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data1 + i)); \
        const __m256i s2  = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data2 + i)); \
        const __m256i vec = _mm256_or_si256(s1, s2);\
        const __m256i lo  = _mm256_and_si256(vec, low_mask); \
        const __m256i hi  = _mm256_and_si256(_mm256_srli_epi16(vec, 4), low_mask); \
        const __m256i popcnt1 = _mm256_shuffle_epi8(lookup, lo); \
        const __m256i popcnt2 = _mm256_shuffle_epi8(lookup, hi); \
        local = _mm256_add_epi8(local, popcnt1); \
        local = _mm256_add_epi8(local, popcnt2); \
        i += 32; \
    }

    DECLARE_LOOKUP
    while (i + 8*32 <= n) {
        __m256i local = _mm256_setzero_si256();
        ITER ITER ITER ITER
        ITER ITER ITER ITER
        acc = _mm256_add_epi64(acc, _mm256_sad_epu8(local, _mm256_setzero_si256()));
    }

    __m256i local = _mm256_setzero_si256();

    while (i + 32 <= n) {
        ITER;
    }

    acc = _mm256_add_epi64(acc, _mm256_sad_epu8(local, _mm256_setzero_si256()));

#undef ITER

    int result = 0;

    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 0));
    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 1));
    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 2));
    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 3));

    for (/**/; i < n; i++) {
        result += lookup8bit[data1[i]|data2[i]];
    }

    return result;
}

int and_popcnt_AVX2_lookup(const uint8_t* data1, const uint8_t* data2, const size_t n) {

    size_t i = 0;


    const __m256i low_mask = _mm256_set1_epi8(0x0f);

    __m256i acc = _mm256_setzero_si256();

#define ITER { \
        const __m256i s1  = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data1 + i)); \
        const __m256i s2  = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data2 + i)); \
        const __m256i vec = _mm256_and_si256(s1, s2);\
        const __m256i lo  = _mm256_and_si256(vec, low_mask); \
        const __m256i hi  = _mm256_and_si256(_mm256_srli_epi16(vec, 4), low_mask); \
        const __m256i popcnt1 = _mm256_shuffle_epi8(lookup, lo); \
        const __m256i popcnt2 = _mm256_shuffle_epi8(lookup, hi); \
        local = _mm256_add_epi8(local, popcnt1); \
        local = _mm256_add_epi8(local, popcnt2); \
        i += 32; \
    }

    DECLARE_LOOKUP
    while (i + 8*32 <= n) {
        __m256i local = _mm256_setzero_si256();
        ITER ITER ITER ITER
        ITER ITER ITER ITER
        acc = _mm256_add_epi64(acc, _mm256_sad_epu8(local, _mm256_setzero_si256()));
    }

    __m256i local = _mm256_setzero_si256();

    while (i + 32 <= n) {
        ITER;
    }

    acc = _mm256_add_epi64(acc, _mm256_sad_epu8(local, _mm256_setzero_si256()));

#undef ITER

    int result = 0;

    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 0));
    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 1));
    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 2));
    result += static_cast<uint64_t>(_mm256_extract_epi64(acc, 3));

    for (/**/; i < n; i++) {
        result += lookup8bit[(data1[i]&data2[i])];
    }

    return result;
}

float
jaccard__AVX2(const uint8_t * a, const uint8_t * b, size_t n) {
    int accu_num = and_popcnt_AVX2_lookup(a,b,n);
    int accu_den = or_popcnt_AVX2_lookup(a,b,n);
    return (accu_den == 0) ? 1.0 : ((float)(accu_den - accu_num) / (float)(accu_den));
}

#else

float fvec_inner_product_avx(const float* x, const float* y, size_t d) {
    FAISS_ASSERT(false);
    return 0.0;
}

float fvec_L2sqr_avx(const float* x, const float* y, size_t d) {
    FAISS_ASSERT(false);
    return 0.0;
}

float fvec_L1_avx(const float* x, const float* y, size_t d) {
    FAISS_ASSERT(false);
    return 0.0;
}

float fvec_Linf_avx (const float* x, const float* y, size_t d) {
    FAISS_ASSERT(false);
    return 0.0;
}

#endif

} // namespace faiss
