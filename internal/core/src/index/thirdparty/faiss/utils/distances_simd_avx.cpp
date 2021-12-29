
// -*- c++ -*-

#include <faiss/utils/distances_simd_avx.h>

#include <cstdio>
#include <cassert>
#include <immintrin.h>
#include <string>

namespace faiss {

uint8_t lookup8bit[256] = {
    /* 0 */ 0, /* 1 */ 1, /* 2 */ 1, /* 3 */ 2,
    /* 4 */ 1, /* 5 */ 2, /* 6 */ 2, /* 7 */ 3,
    /* 8 */ 1, /* 9 */ 2, /* a */ 2, /* b */ 3,
    /* c */ 2, /* d */ 3, /* e */ 3, /* f */ 4,
    /* 10 */ 1, /* 11 */ 2, /* 12 */ 2, /* 13 */ 3,
    /* 14 */ 2, /* 15 */ 3, /* 16 */ 3, /* 17 */ 4,
    /* 18 */ 2, /* 19 */ 3, /* 1a */ 3, /* 1b */ 4,
    /* 1c */ 3, /* 1d */ 4, /* 1e */ 4, /* 1f */ 5,
    /* 20 */ 1, /* 21 */ 2, /* 22 */ 2, /* 23 */ 3,
    /* 24 */ 2, /* 25 */ 3, /* 26 */ 3, /* 27 */ 4,
    /* 28 */ 2, /* 29 */ 3, /* 2a */ 3, /* 2b */ 4,
    /* 2c */ 3, /* 2d */ 4, /* 2e */ 4, /* 2f */ 5,
    /* 30 */ 2, /* 31 */ 3, /* 32 */ 3, /* 33 */ 4,
    /* 34 */ 3, /* 35 */ 4, /* 36 */ 4, /* 37 */ 5,
    /* 38 */ 3, /* 39 */ 4, /* 3a */ 4, /* 3b */ 5,
    /* 3c */ 4, /* 3d */ 5, /* 3e */ 5, /* 3f */ 6,
    /* 40 */ 1, /* 41 */ 2, /* 42 */ 2, /* 43 */ 3,
    /* 44 */ 2, /* 45 */ 3, /* 46 */ 3, /* 47 */ 4,
    /* 48 */ 2, /* 49 */ 3, /* 4a */ 3, /* 4b */ 4,
    /* 4c */ 3, /* 4d */ 4, /* 4e */ 4, /* 4f */ 5,
    /* 50 */ 2, /* 51 */ 3, /* 52 */ 3, /* 53 */ 4,
    /* 54 */ 3, /* 55 */ 4, /* 56 */ 4, /* 57 */ 5,
    /* 58 */ 3, /* 59 */ 4, /* 5a */ 4, /* 5b */ 5,
    /* 5c */ 4, /* 5d */ 5, /* 5e */ 5, /* 5f */ 6,
    /* 60 */ 2, /* 61 */ 3, /* 62 */ 3, /* 63 */ 4,
    /* 64 */ 3, /* 65 */ 4, /* 66 */ 4, /* 67 */ 5,
    /* 68 */ 3, /* 69 */ 4, /* 6a */ 4, /* 6b */ 5,
    /* 6c */ 4, /* 6d */ 5, /* 6e */ 5, /* 6f */ 6,
    /* 70 */ 3, /* 71 */ 4, /* 72 */ 4, /* 73 */ 5,
    /* 74 */ 4, /* 75 */ 5, /* 76 */ 5, /* 77 */ 6,
    /* 78 */ 4, /* 79 */ 5, /* 7a */ 5, /* 7b */ 6,
    /* 7c */ 5, /* 7d */ 6, /* 7e */ 6, /* 7f */ 7,
    /* 80 */ 1, /* 81 */ 2, /* 82 */ 2, /* 83 */ 3,
    /* 84 */ 2, /* 85 */ 3, /* 86 */ 3, /* 87 */ 4,
    /* 88 */ 2, /* 89 */ 3, /* 8a */ 3, /* 8b */ 4,
    /* 8c */ 3, /* 8d */ 4, /* 8e */ 4, /* 8f */ 5,
    /* 90 */ 2, /* 91 */ 3, /* 92 */ 3, /* 93 */ 4,
    /* 94 */ 3, /* 95 */ 4, /* 96 */ 4, /* 97 */ 5,
    /* 98 */ 3, /* 99 */ 4, /* 9a */ 4, /* 9b */ 5,
    /* 9c */ 4, /* 9d */ 5, /* 9e */ 5, /* 9f */ 6,
    /* a0 */ 2, /* a1 */ 3, /* a2 */ 3, /* a3 */ 4,
    /* a4 */ 3, /* a5 */ 4, /* a6 */ 4, /* a7 */ 5,
    /* a8 */ 3, /* a9 */ 4, /* aa */ 4, /* ab */ 5,
    /* ac */ 4, /* ad */ 5, /* ae */ 5, /* af */ 6,
    /* b0 */ 3, /* b1 */ 4, /* b2 */ 4, /* b3 */ 5,
    /* b4 */ 4, /* b5 */ 5, /* b6 */ 5, /* b7 */ 6,
    /* b8 */ 4, /* b9 */ 5, /* ba */ 5, /* bb */ 6,
    /* bc */ 5, /* bd */ 6, /* be */ 6, /* bf */ 7,
    /* c0 */ 2, /* c1 */ 3, /* c2 */ 3, /* c3 */ 4,
    /* c4 */ 3, /* c5 */ 4, /* c6 */ 4, /* c7 */ 5,
    /* c8 */ 3, /* c9 */ 4, /* ca */ 4, /* cb */ 5,
    /* cc */ 4, /* cd */ 5, /* ce */ 5, /* cf */ 6,
    /* d0 */ 3, /* d1 */ 4, /* d2 */ 4, /* d3 */ 5,
    /* d4 */ 4, /* d5 */ 5, /* d6 */ 5, /* d7 */ 6,
    /* d8 */ 4, /* d9 */ 5, /* da */ 5, /* db */ 6,
    /* dc */ 5, /* dd */ 6, /* de */ 6, /* df */ 7,
    /* e0 */ 3, /* e1 */ 4, /* e2 */ 4, /* e3 */ 5,
    /* e4 */ 4, /* e5 */ 5, /* e6 */ 5, /* e7 */ 6,
    /* e8 */ 4, /* e9 */ 5, /* ea */ 5, /* eb */ 6,
    /* ec */ 5, /* ed */ 6, /* ee */ 6, /* ef */ 7,
    /* f0 */ 4, /* f1 */ 5, /* f2 */ 5, /* f3 */ 6,
    /* f4 */ 5, /* f5 */ 6, /* f6 */ 6, /* f7 */ 7,
    /* f8 */ 5, /* f9 */ 6, /* fa */ 6, /* fb */ 7,
    /* fc */ 6, /* fd */ 7, /* fe */ 7, /* ff */ 8
};


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
                /* c */ 2, /* d */ 3, /* e */ 3, /* f */ 4  \
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
jaccard_AVX2(const uint8_t * a, const uint8_t * b, size_t n) {
    int accu_num = and_popcnt_AVX2_lookup(a,b,n);
    int accu_den = or_popcnt_AVX2_lookup(a,b,n);
    return (accu_den == 0) ? 1.0 : ((float)(accu_den - accu_num) / (float)(accu_den));
}

} // namespace faiss
