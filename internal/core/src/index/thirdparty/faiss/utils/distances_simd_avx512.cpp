
// -*- c++ -*-

#ifdef __AVX__
#include <faiss/utils/distances_simd_avx512.h>

#include <cstdio>
#include <cassert>
#include <immintrin.h>
#include <string>

namespace faiss {

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


extern uint8_t lookup8bit[256];

float
fvec_inner_product_avx512(const float* x, const float* y, size_t d) {
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

float
fvec_L2sqr_avx512(const float* x, const float* y, size_t d) {
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

float
fvec_L1_avx512(const float* x, const float* y, size_t d) {
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

float
fvec_Linf_avx512(const float* x, const float* y, size_t d) {
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

std::uint64_t
_mm256_hsum_epi64(__m256i v) {
    return _mm256_extract_epi64(v, 0)
    + _mm256_extract_epi64(v, 1)
    + _mm256_extract_epi64(v, 2)
    + _mm256_extract_epi64(v, 3);
}

std::uint64_t _mm512_hsum_epi64(__m512i v) {
    const __m256i t0 = _mm512_extracti64x4_epi64(v, 0);
    const __m256i t1 = _mm512_extracti64x4_epi64(v, 1);

    return _mm256_hsum_epi64(t0)
    + _mm256_hsum_epi64(t1);
}


int
popcnt_AVX512VBMI_lookup(const uint8_t* data, const size_t n) {

    size_t i = 0;

    const __m512i lookup = _mm512_setr_epi64(
        0x0302020102010100llu, 0x0403030203020201llu,
        0x0302020102010100llu, 0x0403030203020201llu,
        0x0302020102010100llu, 0x0403030203020201llu,
        0x0302020102010100llu, 0x0403030203020201llu
    );

    const __m512i low_mask = _mm512_set1_epi8(0x0f);

    __m512i acc = _mm512_setzero_si512();

    while (i + 64 < n) {

        __m512i local = _mm512_setzero_si512();

        for (int k=0; k < 255/8 && i + 64 < n; k++, i += 64) {
            const __m512i vec = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data + i));
            const __m512i lo  = _mm512_and_si512(vec, low_mask);
            const __m512i hi  = _mm512_and_si512(_mm512_srli_epi32(vec, 4), low_mask);

            const __m512i popcnt1 = _mm512_shuffle_epi8(lookup, lo);
            const __m512i popcnt2 = _mm512_shuffle_epi8(lookup, hi);

            local = _mm512_add_epi8(local, popcnt1);
            local = _mm512_add_epi8(local, popcnt2);
        }

        acc = _mm512_add_epi64(acc, _mm512_sad_epu8(local, _mm512_setzero_si512()));
    }


    int result = _mm512_hsum_epi64(acc);
    for (/**/; i < n; i++) {
        result += lookup8bit[data[i]];
    }

    return result;
}

int
xor_popcnt_AVX512VBMI_lookup(const uint8_t* data1, const uint8_t* data2, const size_t n) {

    size_t i = 0;

    const __m512i lookup = _mm512_setr_epi64(
        0x0302020102010100llu, 0x0403030203020201llu,
        0x0302020102010100llu, 0x0403030203020201llu,
        0x0302020102010100llu, 0x0403030203020201llu,
        0x0302020102010100llu, 0x0403030203020201llu
    );

    const __m512i low_mask = _mm512_set1_epi8(0x0f);

    __m512i acc = _mm512_setzero_si512();

    while (i + 64 < n) {

        __m512i local = _mm512_setzero_si512();

        for (int k=0; k < 255/8 && i + 64 < n; k++, i += 64) {
            const __m512i s1 = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data1 + i));
            const __m512i s2 = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data2 + i));
            const __m512i vec = _mm512_xor_si512(s1, s2);
            const __m512i lo  = _mm512_and_si512(vec, low_mask);
            const __m512i hi  = _mm512_and_si512(_mm512_srli_epi32(vec, 4), low_mask);

            const __m512i popcnt1 = _mm512_shuffle_epi8(lookup, lo);
            const __m512i popcnt2 = _mm512_shuffle_epi8(lookup, hi);

            local = _mm512_add_epi8(local, popcnt1);
            local = _mm512_add_epi8(local, popcnt2);
        }

        acc = _mm512_add_epi64(acc, _mm512_sad_epu8(local, _mm512_setzero_si512()));
    }


    int result = _mm512_hsum_epi64(acc);
    for (/**/; i < n; i++) {
        result += lookup8bit[data1[i]^data2[i]];
    }

    return result;
}

int
or_popcnt_AVX512VBMI_lookup(const uint8_t* data1, const uint8_t* data2, const size_t n) {

    size_t i = 0;

    const __m512i lookup = _mm512_setr_epi64(
        0x0302020102010100llu, 0x0403030203020201llu,
        0x0302020102010100llu, 0x0403030203020201llu,
        0x0302020102010100llu, 0x0403030203020201llu,
        0x0302020102010100llu, 0x0403030203020201llu
    );

    const __m512i low_mask = _mm512_set1_epi8(0x0f);

    __m512i acc = _mm512_setzero_si512();

    while (i + 64 < n) {

        __m512i local = _mm512_setzero_si512();

        for (int k=0; k < 255/8 && i + 64 < n; k++, i += 64) {
            const __m512i s1 = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data1 + i));
            const __m512i s2 = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data2 + i));
            const __m512i vec = _mm512_or_si512(s1, s2);
            const __m512i lo  = _mm512_and_si512(vec, low_mask);
            const __m512i hi  = _mm512_and_si512(_mm512_srli_epi32(vec, 4), low_mask);

            const __m512i popcnt1 = _mm512_shuffle_epi8(lookup, lo);
            const __m512i popcnt2 = _mm512_shuffle_epi8(lookup, hi);

            local = _mm512_add_epi8(local, popcnt1);
            local = _mm512_add_epi8(local, popcnt2);
        }

        acc = _mm512_add_epi64(acc, _mm512_sad_epu8(local, _mm512_setzero_si512()));
    }


    int result = _mm512_hsum_epi64(acc);
    for (/**/; i < n; i++) {
        result += lookup8bit[data1[i]|data2[i]];
    }

    return result;
}

int
and_popcnt_AVX512VBMI_lookup(const uint8_t* data1, const uint8_t* data2, const size_t n) {

    size_t i = 0;

    const __m512i lookup = _mm512_setr_epi64(
        0x0302020102010100llu, 0x0403030203020201llu,
        0x0302020102010100llu, 0x0403030203020201llu,
        0x0302020102010100llu, 0x0403030203020201llu,
        0x0302020102010100llu, 0x0403030203020201llu
    );

    const __m512i low_mask = _mm512_set1_epi8(0x0f);

    __m512i acc = _mm512_setzero_si512();

    while (i + 64 < n) {

        __m512i local = _mm512_setzero_si512();

        for (int k=0; k < 255/8 && i + 64 < n; k++, i += 64) {
            const __m512i s1 = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data1 + i));
            const __m512i s2 = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data2 + i));
            const __m512i vec = _mm512_and_si512(s1, s2);
            const __m512i lo  = _mm512_and_si512(vec, low_mask);
            const __m512i hi  = _mm512_and_si512(_mm512_srli_epi32(vec, 4), low_mask);

            const __m512i popcnt1 = _mm512_shuffle_epi8(lookup, lo);
            const __m512i popcnt2 = _mm512_shuffle_epi8(lookup, hi);

            local = _mm512_add_epi8(local, popcnt1);
            local = _mm512_add_epi8(local, popcnt2);
        }

        acc = _mm512_add_epi64(acc, _mm512_sad_epu8(local, _mm512_setzero_si512()));
    }


    int result = _mm512_hsum_epi64(acc);
    for (/**/; i < n; i++) {
        result += lookup8bit[data1[i]&data2[i]];
    }

    return result;
}

float
jaccard__AVX512(const uint8_t * a, const uint8_t * b, size_t n) {
    int accu_num = and_popcnt_AVX512VBMI_lookup(a,b,n);
    int accu_den = or_popcnt_AVX512VBMI_lookup(a,b,n);
    return (accu_den == 0) ? 1.0 : ((float)(accu_den - accu_num) / (float)(accu_den));
}

} // namespace faiss
#endif
