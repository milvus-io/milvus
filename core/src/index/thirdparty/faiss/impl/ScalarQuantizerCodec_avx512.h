/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#include <cstdio>
#include <algorithm>

#include <omp.h>

#ifdef __SSE__
#include <immintrin.h>
#endif

#include <faiss/utils/utils.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/ScalarQuantizerOp.h>

namespace faiss {

/*******************************************************************
 * ScalarQuantizer implementation
 *
 * The main source of complexity is to support combinations of 4
 * variants without incurring runtime tests or virtual function calls:
 *
 * - 4 / 8 bits per code component
 * - uniform / non-uniform
 * - IP / L2 distance search
 * - scalar / AVX distance computation
 *
 * The appropriate Quantizer object is returned via select_quantizer
 * that hides the template mess.
 ********************************************************************/

#ifdef __AVX__
#define USE_AVX
#endif

#if (defined(__AVX512F__) && defined(__AVX512DQ__) && defined(__AVX512BW__))
#define USE_AVX_512
#endif


/*******************************************************************
 * Codec: converts between values in [0, 1] and an index in a code
 * array. The "i" parameter is the vector component index (not byte
 * index).
 */

struct Codec8bit_avx512 {
    static void encode_component (float x, uint8_t *code, int i) {
        code[i] = (int)(255 * x);
    }

    static float decode_component (const uint8_t *code, int i) {
        return (code[i] + 0.5f) / 255.0f;
    }

#ifdef USE_AVX
    static __m256 decode_8_components (const uint8_t *code, int i) {
        uint64_t c8 = *(uint64_t*)(code + i);
        __m128i c4lo = _mm_cvtepu8_epi32 (_mm_set1_epi32(c8));
        __m128i c4hi = _mm_cvtepu8_epi32 (_mm_set1_epi32(c8 >> 32));
        // __m256i i8 = _mm256_set_m128i(c4lo, c4hi);
        __m256i i8 = _mm256_castsi128_si256 (c4lo);
        i8 = _mm256_insertf128_si256 (i8, c4hi, 1);
        __m256 f8 = _mm256_cvtepi32_ps (i8);
        __m256 half = _mm256_set1_ps (0.5f);
        f8 += half;
        __m256 one_255 = _mm256_set1_ps (1.f / 255.f);
        return f8 * one_255;
    }
#endif

#ifdef USE_AVX_512
    static __m512 decode_16_components (const uint8_t *code, int i) {
        uint64_t c8 = *(uint64_t*)(code + i);
        __m256i c8lo = _mm256_cvtepu8_epi32 (_mm_set1_epi64x(c8));
        c8 = *(uint64_t*)(code + i + 8);
        __m256i c8hi = _mm256_cvtepu8_epi32 (_mm_set1_epi64x(c8));
        // __m256i i8 = _mm256_set_m128i(c4lo, c4hi);
        __m512i i16 = _mm512_castsi256_si512 (c8lo);
        i16 = _mm512_inserti32x8 (i16, c8hi, 1);
        __m512 f16 = _mm512_cvtepi32_ps (i16);
        __m512 half = _mm512_set1_ps (0.5f);
        f16 += half;
        __m512 one_255 = _mm512_set1_ps (1.f / 255.f);
        return f16 * one_255;
    }
#endif
};


struct Codec4bit_avx512 {
    static void encode_component (float x, uint8_t *code, int i) {
        code [i / 2] |= (int)(x * 15.0) << ((i & 1) << 2);
    }

    static float decode_component (const uint8_t *code, int i) {
        return (((code[i / 2] >> ((i & 1) << 2)) & 0xf) + 0.5f) / 15.0f;
    }

#ifdef USE_AVX
    static __m256 decode_8_components (const uint8_t *code, int i) {
        uint32_t c4 = *(uint32_t*)(code + (i >> 1));
        uint32_t mask = 0x0f0f0f0f;
        uint32_t c4ev = c4 & mask;
        uint32_t c4od = (c4 >> 4) & mask;

        // the 8 lower bytes of c8 contain the values
        __m128i c8 = _mm_unpacklo_epi8 (_mm_set1_epi32(c4ev),
                                        _mm_set1_epi32(c4od));
        __m128i c4lo = _mm_cvtepu8_epi32 (c8);
        __m128i c4hi = _mm_cvtepu8_epi32 (_mm_srli_si128(c8, 4));
        __m256i i8 = _mm256_castsi128_si256 (c4lo);
        i8 = _mm256_insertf128_si256 (i8, c4hi, 1);
        __m256 f8 = _mm256_cvtepi32_ps (i8);
        __m256 half = _mm256_set1_ps (0.5f);
        f8 += half;
        __m256 one_255 = _mm256_set1_ps (1.f / 15.f);
        return f8 * one_255;
    }
#endif

#ifdef USE_AVX_512
    static __m512 decode_16_components (const uint8_t *code, int i) {
        uint64_t c8 = *(uint64_t*)(code + (i >> 1));
        uint64_t mask = 0x0f0f0f0f0f0f0f0f;
        uint64_t c8ev = c8 & mask;
        uint64_t c8od = (c8 >> 4) & mask;

        // the 8 lower bytes of c8 contain the values
        __m128i c16 = _mm_unpacklo_epi8 (_mm_set1_epi64x(c8ev),
                                         _mm_set1_epi64x(c8od));
        __m256i c8lo = _mm256_cvtepu8_epi32 (c16);
        __m256i c8hi = _mm256_cvtepu8_epi32 (_mm_srli_si128(c16, 4));
        __m512i i16 = _mm512_castsi256_si512 (c8lo);
        i16 = _mm512_inserti32x8 (i16, c8hi, 1);
        __m512 f16 = _mm512_cvtepi32_ps (i16);
        __m512 half = _mm512_set1_ps (0.5f);
        f16 += half;
        __m512 one_255 = _mm512_set1_ps (1.f / 15.f);
        return f16 * one_255;
    }
#endif
};

struct Codec6bit_avx512 {
    static void encode_component (float x, uint8_t *code, int i) {
        int bits = (int)(x * 63.0);
        code += (i >> 2) * 3;
        switch(i & 3) {
        case 0:
            code[0] |= bits;
            break;
        case 1:
            code[0] |= bits << 6;
            code[1] |= bits >> 2;
            break;
        case 2:
            code[1] |= bits << 4;
            code[2] |= bits >> 4;
            break;
        case 3:
            code[2] |= bits << 2;
            break;
        }
    }

    static float decode_component (const uint8_t *code, int i) {
        uint8_t bits;
        code += (i >> 2) * 3;
        switch(i & 3) {
        case 0:
            bits = code[0] & 0x3f;
            break;
        case 1:
            bits = code[0] >> 6;
            bits |= (code[1] & 0xf) << 2;
            break;
        case 2:
            bits = code[1] >> 4;
            bits |= (code[2] & 3) << 4;
            break;
        case 3:
            bits = code[2] >> 2;
            break;
        }
        return (bits + 0.5f) / 63.0f;
    }

#ifdef USE_AVX
    static __m256 decode_8_components (const uint8_t *code, int i) {
        return _mm256_set_ps
            (decode_component(code, i + 7),
             decode_component(code, i + 6),
             decode_component(code, i + 5),
             decode_component(code, i + 4),
             decode_component(code, i + 3),
             decode_component(code, i + 2),
             decode_component(code, i + 1),
             decode_component(code, i + 0));
    }
#endif

#ifdef USE_AVX_512
    static __m512 decode_16_components (const uint8_t *code, int i) {
        return _mm512_set_ps
            (decode_component(code, i + 15),
             decode_component(code, i + 14),
             decode_component(code, i + 13),
             decode_component(code, i + 12),
             decode_component(code, i + 11),
             decode_component(code, i + 10),
             decode_component(code, i + 9),
             decode_component(code, i + 8),
             decode_component(code, i + 7),
             decode_component(code, i + 6),
             decode_component(code, i + 5),
             decode_component(code, i + 4),
             decode_component(code, i + 3),
             decode_component(code, i + 2),
             decode_component(code, i + 1),
             decode_component(code, i + 0));
    }
#endif
};



/*******************************************************************
 * Quantizer: normalizes scalar vector components, then passes them
 * through a codec
 *******************************************************************/


template<class Codec, bool uniform, int SIMD>
struct QuantizerTemplate_avx512 {};


template<class Codec>
struct QuantizerTemplate_avx512<Codec, true, 1>: Quantizer {
    const size_t d;
    const float vmin, vdiff;

    QuantizerTemplate_avx512(size_t d, const std::vector<float> &trained):
        d(d), vmin(trained[0]), vdiff(trained[1])
    {
    }

    void encode_vector(const float* x, uint8_t* code) const final {
        for (size_t i = 0; i < d; i++) {
            float xi = (x[i] - vmin) / vdiff;
            if (xi < 0) {
                xi = 0;
            }
            if (xi > 1.0) {
                xi = 1.0;
            }
            Codec::encode_component(xi, code, i);
        }
    }

    void decode_vector(const uint8_t* code, float* x) const final {
        for (size_t i = 0; i < d; i++) {
            float xi = Codec::decode_component(code, i);
            x[i] = vmin + xi * vdiff;
        }
    }

    float reconstruct_component (const uint8_t * code, int i) const
    {
        float xi = Codec::decode_component (code, i);
        return vmin + xi * vdiff;
    }
};


#ifdef USE_AVX
template<class Codec>
struct QuantizerTemplate_avx512<Codec, true, 8>: QuantizerTemplate_avx512<Codec, true, 1> {
    QuantizerTemplate_avx512 (size_t d, const std::vector<float> &trained):
        QuantizerTemplate_avx512<Codec, true, 1> (d, trained) {}

    __m256 reconstruct_8_components (const uint8_t * code, int i) const
    {
        __m256 xi = Codec::decode_8_components (code, i);
        return _mm256_set1_ps(this->vmin) + xi * _mm256_set1_ps (this->vdiff);
    }
};
#endif

#ifdef USE_AVX_512
template<class Codec>
struct QuantizerTemplate_avx512<Codec, true, 16>: QuantizerTemplate_avx512<Codec, true, 1> {
    QuantizerTemplate_avx512 (size_t d, const std::vector<float> &trained):
        QuantizerTemplate_avx512<Codec, true, 1> (d, trained) {}

    __m512 reconstruct_16_components (const uint8_t * code, int i) const
    {
        __m512 xi = Codec::decode_16_components (code, i);
        return _mm512_set1_ps(this->vmin) + xi * _mm512_set1_ps (this->vdiff);
    }
};
#endif


template<class Codec>
struct QuantizerTemplate_avx512<Codec, false, 1>: Quantizer {
    const size_t d;
    const float *vmin, *vdiff;

    QuantizerTemplate_avx512 (size_t d, const std::vector<float> &trained):
        d(d), vmin(trained.data()), vdiff(trained.data() + d) {}

    void encode_vector(const float* x, uint8_t* code) const final {
        for (size_t i = 0; i < d; i++) {
            float xi = (x[i] - vmin[i]) / vdiff[i];
            if (xi < 0)
                xi = 0;
            if (xi > 1.0)
                xi = 1.0;
            Codec::encode_component(xi, code, i);
        }
    }

    void decode_vector(const uint8_t* code, float* x) const final {
        for (size_t i = 0; i < d; i++) {
            float xi = Codec::decode_component(code, i);
            x[i] = vmin[i] + xi * vdiff[i];
        }
    }

    float reconstruct_component (const uint8_t * code, int i) const
    {
        float xi = Codec::decode_component (code, i);
        return vmin[i] + xi * vdiff[i];
    }
};


#ifdef USE_AVX
template<class Codec>
struct QuantizerTemplate_avx512<Codec, false, 8>: QuantizerTemplate_avx512<Codec, false, 1> {
    QuantizerTemplate_avx512 (size_t d, const std::vector<float> &trained):
        QuantizerTemplate_avx512<Codec, false, 1> (d, trained) {}

    __m256 reconstruct_8_components (const uint8_t * code, int i) const
    {
        __m256 xi = Codec::decode_8_components (code, i);
        return _mm256_loadu_ps (this->vmin + i) + xi * _mm256_loadu_ps (this->vdiff + i);
    }
};
#endif

#ifdef USE_AVX_512
template<class Codec>
struct QuantizerTemplate_avx512<Codec, false, 16>: QuantizerTemplate_avx512<Codec, false, 1> {

    QuantizerTemplate_avx512 (size_t d, const std::vector<float> &trained):
        QuantizerTemplate_avx512<Codec, false, 1> (d, trained) {}

    __m512 reconstruct_16_components (const uint8_t * code, int i) const
    {
        __m512 xi = Codec::decode_16_components (code, i);
        return _mm512_loadu_ps (this->vmin + i) + xi * _mm512_loadu_ps (this->vdiff + i);
    }
};
#endif

/*******************************************************************
 * FP16 quantizer
 *******************************************************************/

template<int SIMDWIDTH>
struct QuantizerFP16_avx512 {};

template<>
struct QuantizerFP16_avx512<1>: Quantizer {
    const size_t d;

    QuantizerFP16_avx512(size_t d, const std::vector<float> & /* unused */):
        d(d) {}

    void encode_vector(const float* x, uint8_t* code) const final {
        for (size_t i = 0; i < d; i++) {
            ((uint16_t*)code)[i] = encode_fp16(x[i]);
        }
    }

    void decode_vector(const uint8_t* code, float* x) const final {
        for (size_t i = 0; i < d; i++) {
            x[i] = decode_fp16(((uint16_t*)code)[i]);
        }
    }

    float reconstruct_component (const uint8_t * code, int i) const
    {
        return decode_fp16(((uint16_t*)code)[i]);
    }
};

#ifdef USE_AVX
template<>
struct QuantizerFP16_avx512<8>: QuantizerFP16_avx512<1> {
    QuantizerFP16_avx512 (size_t d, const std::vector<float> &trained):
        QuantizerFP16_avx512<1> (d, trained) {}

    __m256 reconstruct_8_components (const uint8_t * code, int i) const
    {
        __m128i codei = _mm_loadu_si128 ((const __m128i*)(code + 2 * i));
        return _mm256_cvtph_ps (codei);
    }
};
#endif

#ifdef USE_AVX_512
template<>
struct QuantizerFP16_avx512<16>: QuantizerFP16_avx512<1> {
    QuantizerFP16_avx512 (size_t d, const std::vector<float> &trained):
        QuantizerFP16_avx512<1> (d, trained) {}

    __m512 reconstruct_16_components (const uint8_t * code, int i) const
    {
        __m256i codei = _mm256_loadu_si256 ((const __m256i*)(code + 2 * i));
        return _mm512_cvtph_ps (codei);
    }
};
#endif

/*******************************************************************
 * 8bit_direct quantizer
 *******************************************************************/

template<int SIMDWIDTH>
struct Quantizer8bitDirect_avx512 {};

template<>
struct Quantizer8bitDirect_avx512<1>: Quantizer {
    const size_t d;

    Quantizer8bitDirect_avx512(size_t d, const std::vector<float> & /* unused */):
        d(d) {}


    void encode_vector(const float* x, uint8_t* code) const final {
        for (size_t i = 0; i < d; i++) {
            code[i] = (uint8_t)x[i];
        }
    }

    void decode_vector(const uint8_t* code, float* x) const final {
        for (size_t i = 0; i < d; i++) {
            x[i] = code[i];
        }
    }

    float reconstruct_component (const uint8_t * code, int i) const
    {
        return code[i];
    }
};

#ifdef USE_AVX
template<>
struct Quantizer8bitDirect_avx512<8>: Quantizer8bitDirect_avx512<1> {
    Quantizer8bitDirect_avx512 (size_t d, const std::vector<float> &trained):
        Quantizer8bitDirect_avx512<1> (d, trained) {}

    __m256 reconstruct_8_components (const uint8_t * code, int i) const
    {
        __m128i x8 = _mm_loadl_epi64((__m128i*)(code + i)); // 8 * int8
        __m256i y8 = _mm256_cvtepu8_epi32 (x8);  // 8 * int32
        return _mm256_cvtepi32_ps (y8); // 8 * float32
    }
};
#endif

#ifdef USE_AVX_512
template<>
struct Quantizer8bitDirect_avx512<16>: Quantizer8bitDirect_avx512<1> {

    Quantizer8bitDirect_avx512 (size_t d, const std::vector<float> &trained):
        Quantizer8bitDirect_avx512<1> (d, trained) {}

    __m512 reconstruct_16_components (const uint8_t * code, int i) const
    {
        __m128i x8 = _mm_load_si128((__m128i*)(code + i)); // 16 * int8
        __m512i y8 = _mm512_cvtepu8_epi32 (x8);  // 16 * int32
        return _mm512_cvtepi32_ps (y8); // 16 * float32
    }
};
#endif


template<int SIMDWIDTH>
Quantizer *select_quantizer_1_avx512 (
          QuantizerType qtype,
          size_t d, const std::vector<float> & trained)
{
    switch(qtype) {
    case QuantizerType::QT_8bit:
        return new QuantizerTemplate_avx512<Codec8bit_avx512, false, SIMDWIDTH>(d, trained);
    case QuantizerType::QT_6bit:
        return new QuantizerTemplate_avx512<Codec6bit_avx512, false, SIMDWIDTH>(d, trained);
    case QuantizerType::QT_4bit:
        return new QuantizerTemplate_avx512<Codec4bit_avx512, false, SIMDWIDTH>(d, trained);
    case QuantizerType::QT_8bit_uniform:
        return new QuantizerTemplate_avx512<Codec8bit_avx512, true, SIMDWIDTH>(d, trained);
    case QuantizerType::QT_4bit_uniform:
        return new QuantizerTemplate_avx512<Codec4bit_avx512, true, SIMDWIDTH>(d, trained);
    case QuantizerType::QT_fp16:
        return new QuantizerFP16_avx512<SIMDWIDTH> (d, trained);
    case QuantizerType::QT_8bit_direct:
        return new Quantizer8bitDirect_avx512<SIMDWIDTH> (d, trained);
    }
    FAISS_THROW_MSG ("unknown qtype");
}



/*******************************************************************
 * Similarity: gets vector components and computes a similarity wrt. a
 * query vector stored in the object. The data fields just encapsulate
 * an accumulator.
 */

template<int SIMDWIDTH>
struct SimilarityL2_avx512 {};


template<>
struct SimilarityL2_avx512<1> {
    static constexpr int simdwidth = 1;
    static constexpr MetricType metric_type = METRIC_L2;

    const float *y, *yi;

    explicit SimilarityL2_avx512 (const float * y): y(y) {}

    /******* scalar accumulator *******/

    float accu;

    void begin () {
        accu = 0;
        yi = y;
    }

    void add_component (float x) {
        float tmp = *yi++ - x;
        accu += tmp * tmp;
    }

    void add_component_2 (float x1, float x2) {
        float tmp = x1 - x2;
        accu += tmp * tmp;
    }

    float result () {
        return accu;
    }
};


#ifdef USE_AVX
template<>
struct SimilarityL2_avx512<8> {
    static constexpr int simdwidth = 8;
    static constexpr MetricType metric_type = METRIC_L2;

    const float *y, *yi;

    explicit SimilarityL2_avx512 (const float * y): y(y) {}
    __m256 accu8;

    void begin_8 () {
        accu8 = _mm256_setzero_ps();
        yi = y;
    }

    void add_8_components (__m256 x) {
        __m256 yiv = _mm256_loadu_ps (yi);
        yi += 8;
        __m256 tmp = yiv - x;
        accu8 += tmp * tmp;
    }

    void add_8_components_2 (__m256 x, __m256 y) {
        __m256 tmp = y - x;
        accu8 += tmp * tmp;
    }

    float result_8 () {
        __m256 sum = _mm256_hadd_ps(accu8, accu8);
        __m256 sum2 = _mm256_hadd_ps(sum, sum);
        // now add the 0th and 4th component
        return
            _mm_cvtss_f32 (_mm256_castps256_ps128(sum2)) +
            _mm_cvtss_f32 (_mm256_extractf128_ps(sum2, 1));
    }
};
#endif

#ifdef USE_AVX_512
template<>
struct SimilarityL2_avx512<16> {
    static constexpr int simdwidth = 16;
    static constexpr MetricType metric_type = METRIC_L2;

    const float *y, *yi;

    explicit SimilarityL2_avx512 (const float * y): y(y) {}
    __m512 accu16;

    void begin_16 () {
        accu16 = _mm512_setzero_ps();
        yi = y;
    }

    void add_16_components (__m512 x) {
        __m512 yiv = _mm512_loadu_ps (yi);
        yi += 16;
        __m512 tmp = yiv - x;
        accu16 += tmp * tmp;
    }

    void add_16_components_2 (__m512 x, __m512 y) {
        __m512 tmp = y - x;
        accu16 += tmp * tmp;
    }

    float result_16 () {
        __m256 sum0 = _mm512_extractf32x8_ps(accu16, 1) + _mm512_extractf32x8_ps(accu16, 0);
        __m256 sum1 = _mm256_hadd_ps(sum0, sum0);
        __m256 sum2 = _mm256_hadd_ps(sum1, sum1);
        // now add the 0th and 4th component
        return
            _mm_cvtss_f32 (_mm256_castps256_ps128(sum2)) +
            _mm_cvtss_f32 (_mm256_extractf128_ps(sum2, 1));
    }
};
#endif


template<int SIMDWIDTH>
struct SimilarityIP_avx512 {};


template<>
struct SimilarityIP_avx512<1> {
    static constexpr int simdwidth = 1;
    static constexpr MetricType metric_type = METRIC_INNER_PRODUCT;
    const float *y, *yi;

    float accu;

    explicit SimilarityIP_avx512 (const float * y):
        y (y) {}

    void begin () {
        accu = 0;
        yi = y;
    }

    void add_component (float x) {
        accu +=  *yi++ * x;
    }

    void add_component_2 (float x1, float x2) {
        accu +=  x1 * x2;
    }

    float result () {
        return accu;
    }
};

#ifdef USE_AVX
template<>
struct SimilarityIP_avx512<8> {
    static constexpr int simdwidth = 8;
    static constexpr MetricType metric_type = METRIC_INNER_PRODUCT;

    const float *y, *yi;

    float accu;

    explicit SimilarityIP_avx512 (const float * y):
        y (y) {}

    __m256 accu8;

    void begin_8 () {
        accu8 = _mm256_setzero_ps();
        yi = y;
    }

    void add_8_components (__m256 x) {
        __m256 yiv = _mm256_loadu_ps (yi);
        yi += 8;
        accu8 += yiv * x;
    }

    void add_8_components_2 (__m256 x1, __m256 x2) {
        accu8 += x1 * x2;
    }

    float result_8 () {
        __m256 sum = _mm256_hadd_ps(accu8, accu8);
        __m256 sum2 = _mm256_hadd_ps(sum, sum);
        // now add the 0th and 4th component
        return
            _mm_cvtss_f32 (_mm256_castps256_ps128(sum2)) +
            _mm_cvtss_f32 (_mm256_extractf128_ps(sum2, 1));
    }
};
#endif

#ifdef USE_AVX_512
template<>
struct SimilarityIP_avx512<16> {
    static constexpr int simdwidth = 16;
    static constexpr MetricType metric_type = METRIC_INNER_PRODUCT;

    const float *y, *yi;

    float accu;

    explicit SimilarityIP_avx512 (const float * y):
        y (y) {}

    __m512 accu16;

    void begin_16 () {
        accu16 = _mm512_setzero_ps();
        yi = y;
    }

    void add_16_components (__m512 x) {
        __m512 yiv = _mm512_loadu_ps (yi);
        yi += 16;
        accu16 += yiv * x;
    }

    void add_16_components_2 (__m512 x1, __m512 x2) {
        accu16 += x1 * x2;
    }

    float result_16 () {
        __m256 sum0 = _mm512_extractf32x8_ps(accu16, 1) + _mm512_extractf32x8_ps(accu16, 0);
        __m256 sum1 = _mm256_hadd_ps(sum0, sum0);
        __m256 sum2 = _mm256_hadd_ps(sum1, sum1);
        // now add the 0th and 4th component
        return
            _mm_cvtss_f32 (_mm256_castps256_ps128(sum2)) +
            _mm_cvtss_f32 (_mm256_extractf128_ps(sum2, 1));
    }
};
#endif


/*******************************************************************
 * DistanceComputer: combines a similarity and a quantizer to do
 * code-to-vector or code-to-code comparisons
 *******************************************************************/

template<class Quantizer, class Similarity, int SIMDWIDTH>
struct DCTemplate_avx512 : SQDistanceComputer {};

template<class Quantizer, class Similarity>
struct DCTemplate_avx512<Quantizer, Similarity, 1> : SQDistanceComputer
{
    using Sim = Similarity;

    Quantizer quant;

    DCTemplate_avx512(size_t d, const std::vector<float> &trained):
        quant(d, trained)
    {}

    float compute_distance(const float* x, const uint8_t* code) const {
        Similarity sim(x);
        sim.begin();
        for (size_t i = 0; i < quant.d; i++) {
            float xi = quant.reconstruct_component(code, i);
            sim.add_component(xi);
        }
        return sim.result();
    }

    float compute_code_distance(const uint8_t* code1, const uint8_t* code2)
        const {
        Similarity sim(nullptr);
        sim.begin();
        for (size_t i = 0; i < quant.d; i++) {
            float x1 = quant.reconstruct_component(code1, i);
            float x2 = quant.reconstruct_component(code2, i);
                sim.add_component_2(x1, x2);
        }
        return sim.result();
    }

    void set_query (const float *x) final {
        q = x;
    }

    /// compute distance of vector i to current query
    float operator () (idx_t i) final {
        return compute_distance (q, codes + i * code_size);
    }

    float symmetric_dis (idx_t i, idx_t j) override {
        return compute_code_distance (codes + i * code_size,
                                      codes + j * code_size);
    }

    float query_to_code (const uint8_t * code) const {
        return compute_distance (q, code);
    }
};

#ifdef USE_AVX
template<class Quantizer, class Similarity>
struct DCTemplate_avx512<Quantizer, Similarity, 8> : SQDistanceComputer
{
    using Sim = Similarity;

    Quantizer quant;

    DCTemplate_avx512(size_t d, const std::vector<float> &trained):
        quant(d, trained)
    {}

    float compute_distance(const float* x, const uint8_t* code) const {

        Similarity sim(x);
        sim.begin_8();
        for (size_t i = 0; i < quant.d; i += 8) {
            __m256 xi = quant.reconstruct_8_components(code, i);
            sim.add_8_components(xi);
        }
        return sim.result_8();
    }

    float compute_code_distance(const uint8_t* code1, const uint8_t* code2)
        const {
        Similarity sim(nullptr);
        sim.begin_8();
        for (size_t i = 0; i < quant.d; i += 8) {
            __m256 x1 = quant.reconstruct_8_components(code1, i);
            __m256 x2 = quant.reconstruct_8_components(code2, i);
            sim.add_8_components_2(x1, x2);
        }
        return sim.result_8();
    }

    void set_query (const float *x) final {
        q = x;
    }

    /// compute distance of vector i to current query
    float operator () (idx_t i) final {
        return compute_distance (q, codes + i * code_size);
    }

    float symmetric_dis (idx_t i, idx_t j) override {
        return compute_code_distance (codes + i * code_size,
                                      codes + j * code_size);
    }

    float query_to_code (const uint8_t * code) const {
        return compute_distance (q, code);
    }
};
#endif

#ifdef USE_AVX_512
template<class Quantizer, class Similarity>
struct DCTemplate_avx512<Quantizer, Similarity, 16> : SQDistanceComputer
{
    using Sim = Similarity;

    Quantizer quant;

    DCTemplate_avx512(size_t d, const std::vector<float> &trained):
        quant(d, trained)
    {}

    float compute_distance(const float* x, const uint8_t* code) const {
        Similarity sim(x);
        sim.begin_16();
        for (size_t i = 0; i < quant.d; i += 16) {
            __m512 xi = quant.reconstruct_16_components(code, i);
            sim.add_16_components(xi);
        }
        return sim.result_16();
    }

    float compute_code_distance(const uint8_t* code1, const uint8_t* code2)
        const {
        Similarity sim(nullptr);
        sim.begin_16();
        for (size_t i = 0; i < quant.d; i += 16) {
            __m512 x1 = quant.reconstruct_16_components(code1, i);
            __m512 x2 = quant.reconstruct_16_components(code2, i);
            sim.add_16_components_2(x1, x2);
        }
        return sim.result_16();
    }

    void set_query (const float *x) final {
        q = x;
    }

    /// compute distance of vector i to current query
    float operator () (idx_t i) final {
        return compute_distance (q, codes + i * code_size);
    }

    float symmetric_dis (idx_t i, idx_t j) override {
        return compute_code_distance (codes + i * code_size,
                                      codes + j * code_size);
    }

    float query_to_code (const uint8_t * code) const {
        return compute_distance (q, code);
    }
};
#endif


/*******************************************************************
 * DistanceComputerByte: computes distances in the integer domain
 *******************************************************************/

template<class Similarity, int SIMDWIDTH>
struct DistanceComputerByte_avx512 : SQDistanceComputer {};

template<class Similarity>
struct DistanceComputerByte_avx512<Similarity, 1> : SQDistanceComputer {
    using Sim = Similarity;

    int d;
    std::vector<uint8_t> tmp;

    DistanceComputerByte_avx512(int d, const std::vector<float> &): d(d), tmp(d) {
    }

    int compute_code_distance(const uint8_t* code1, const uint8_t* code2)
        const {
        int accu = 0;
        for (int i = 0; i < d; i++) {
            if (Sim::metric_type == METRIC_INNER_PRODUCT) {
                accu += int(code1[i]) * code2[i];
            } else {
                int diff = int(code1[i]) - code2[i];
                accu += diff * diff;
            }
        }
        return accu;
    }

    void set_query (const float *x) final {
        for (int i = 0; i < d; i++) {
            tmp[i] = int(x[i]);
        }
    }

    int compute_distance(const float* x, const uint8_t* code) {
        set_query(x);
        return compute_code_distance(tmp.data(), code);
    }

    /// compute distance of vector i to current query
    float operator () (idx_t i) final {
        return compute_distance (q, codes + i * code_size);
    }

    float symmetric_dis (idx_t i, idx_t j) override {
        return compute_code_distance (codes + i * code_size,
                                      codes + j * code_size);
    }

    float query_to_code (const uint8_t * code) const {
        return compute_code_distance (tmp.data(), code);
    }
};

#ifdef USE_AVX
template<class Similarity>
struct DistanceComputerByte_avx512<Similarity, 8> : SQDistanceComputer {
    using Sim = Similarity;

    int d;
    std::vector<uint8_t> tmp;

    DistanceComputerByte_avx512(int d, const std::vector<float> &): d(d), tmp(d) {
    }

    int compute_code_distance(const uint8_t* code1, const uint8_t* code2)
        const {
        // __m256i accu = _mm256_setzero_ps ();
        __m256i accu = _mm256_setzero_si256 ();
        for (int i = 0; i < d; i += 16) {
            // load 16 bytes, convert to 16 uint16_t
            __m256i c1 = _mm256_cvtepu8_epi16
                (_mm_loadu_si128((__m128i*)(code1 + i)));
            __m256i c2 = _mm256_cvtepu8_epi16
                (_mm_loadu_si128((__m128i*)(code2 + i)));
            __m256i prod32;
            if (Sim::metric_type == METRIC_INNER_PRODUCT) {
                prod32 = _mm256_madd_epi16(c1, c2);
            } else {
                __m256i diff = _mm256_sub_epi16(c1, c2);
                prod32 = _mm256_madd_epi16(diff, diff);
            }
            accu = _mm256_add_epi32 (accu, prod32);
        }
        __m128i sum = _mm256_extractf128_si256(accu, 0);
        sum = _mm_add_epi32 (sum, _mm256_extractf128_si256(accu, 1));
        sum = _mm_hadd_epi32 (sum, sum);
        sum = _mm_hadd_epi32 (sum, sum);
        return _mm_cvtsi128_si32 (sum);
    }

    void set_query (const float *x) final {
        /*
        for (int i = 0; i < d; i += 8) {
            __m256 xi = _mm256_loadu_ps (x + i);
            __m256i ci = _mm256_cvtps_epi32(xi);
        */
        for (int i = 0; i < d; i++) {
            tmp[i] = int(x[i]);
        }
    }

    int compute_distance(const float* x, const uint8_t* code) {
        set_query(x);
        return compute_code_distance(tmp.data(), code);
    }

    /// compute distance of vector i to current query
    float operator () (idx_t i) final {
        return compute_distance (q, codes + i * code_size);
    }

    float symmetric_dis (idx_t i, idx_t j) override {
        return compute_code_distance (codes + i * code_size,
                                      codes + j * code_size);
    }

    float query_to_code (const uint8_t * code) const {
        return compute_code_distance (tmp.data(), code);
    }
};
#endif

#ifdef USE_AVX_512
template<class Similarity>
struct DistanceComputerByte_avx512<Similarity, 16> : SQDistanceComputer {
    using Sim = Similarity;

    int d;
    std::vector<uint8_t> tmp;

    DistanceComputerByte_avx512(int d, const std::vector<float> &): d(d), tmp(d) {
    }

    int compute_code_distance(const uint8_t* code1, const uint8_t* code2)
        const {
        // __m256i accu = _mm256_setzero_ps ();
        __m512i accu = _mm512_setzero_si512 ();
        for (int i = 0; i < d; i += 32) {
            // load 32 bytes, convert to 16 uint16_t
            __m512i c1 = _mm512_cvtepu8_epi16
                (_mm256_loadu_si256((__m256i*)(code1 + i)));
            __m512i c2 = _mm512_cvtepu8_epi16
                (_mm256_loadu_si256((__m256i*)(code2 + i)));
            __m512i prod32;
            if (Sim::metric_type == METRIC_INNER_PRODUCT) {
                prod32 = _mm512_madd_epi16(c1, c2);
            } else {
                __m512i diff = _mm512_sub_epi16(c1, c2);
                prod32 = _mm512_madd_epi16(diff, diff);
            }
            accu = _mm512_add_epi32 (accu, prod32);
        }
        __m128i sum = _mm512_extracti32x4_epi32(accu, 0);
        sum = _mm_add_epi32 (sum, _mm512_extracti32x4_epi32(accu, 1));
        sum = _mm_add_epi32 (sum, _mm512_extracti32x4_epi32(accu, 2));
        sum = _mm_add_epi32 (sum, _mm512_extracti32x4_epi32(accu, 3));
        sum = _mm_hadd_epi32 (sum, sum);
        sum = _mm_hadd_epi32 (sum, sum);
        return _mm_cvtsi128_si32 (sum);
    }

    void set_query (const float *x) final {
        /*
        for (int i = 0; i < d; i += 8) {
            __m256 xi = _mm256_loadu_ps (x + i);
            __m256i ci = _mm256_cvtps_epi32(xi);
        */
        for (int i = 0; i < d; i++) {
            tmp[i] = int(x[i]);
        }
    }

    int compute_distance(const float* x, const uint8_t* code) {
        set_query(x);
        return compute_code_distance(tmp.data(), code);
    }

    /// compute distance of vector i to current query
    float operator () (idx_t i) final {
        return compute_distance (q, codes + i * code_size);
    }

    float symmetric_dis (idx_t i, idx_t j) override {
        return compute_code_distance (codes + i * code_size,
                                      codes + j * code_size);
    }

    float query_to_code (const uint8_t * code) const {
        return compute_code_distance (tmp.data(), code);
    }
};
#endif


/*******************************************************************
 * select_distance_computer: runtime selection of template
 * specialization
 *******************************************************************/


template<class Sim>
SQDistanceComputer *select_distance_computer_avx512 (
          QuantizerType qtype,
          size_t d, const std::vector<float> & trained)
{
    constexpr int SIMDWIDTH = Sim::simdwidth;
    switch(qtype) {
    case QuantizerType::QT_8bit_uniform:
        return new DCTemplate_avx512<QuantizerTemplate_avx512<Codec8bit_avx512, true, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case QuantizerType::QT_4bit_uniform:
        return new DCTemplate_avx512<QuantizerTemplate_avx512<Codec4bit_avx512, true, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case QuantizerType::QT_8bit:
        return new DCTemplate_avx512<QuantizerTemplate_avx512<Codec8bit_avx512, false, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case QuantizerType::QT_6bit:
        return new DCTemplate_avx512<QuantizerTemplate_avx512<Codec6bit_avx512, false, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case QuantizerType::QT_4bit:
        return new DCTemplate_avx512<QuantizerTemplate_avx512<Codec4bit_avx512, false, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case QuantizerType::QT_fp16:
        return new DCTemplate_avx512
            <QuantizerFP16_avx512<SIMDWIDTH>, Sim, SIMDWIDTH>(d, trained);

    case QuantizerType::QT_8bit_direct:
        if (d % 16 == 0) {
            return new DistanceComputerByte_avx512<Sim, SIMDWIDTH>(d, trained);
        } else {
            return new DCTemplate_avx512
                <Quantizer8bitDirect_avx512<SIMDWIDTH>, Sim, SIMDWIDTH>(d, trained);
        }
    }
    FAISS_THROW_MSG ("unknown qtype");
    return nullptr;
}


} // namespace faiss
