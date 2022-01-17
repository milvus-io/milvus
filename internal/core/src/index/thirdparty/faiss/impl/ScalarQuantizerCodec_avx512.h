/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#ifdef __AVX__
#include <cstdio>
#include <algorithm>
#include <omp.h>
#include <immintrin.h>

#include <faiss/utils/utils.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/ScalarQuantizerCodec_avx.h>

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


/*******************************************************************
 * Codec: converts between values in [0, 1] and an index in a code
 * array. The "i" parameter is the vector component index (not byte
 * index).
 */

struct Codec8bit_avx512 : public Codec8bit_avx {
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
};

struct Codec4bit_avx512 : public Codec4bit_avx {
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
};

struct Codec6bit_avx512 : public Codec6bit_avx {
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
};


/*******************************************************************
 * Quantizer: normalizes scalar vector components, then passes them
 * through a codec
 *******************************************************************/

template<class Codec, bool uniform, int SIMD>
struct QuantizerTemplate_avx512 {};

template<class Codec>
struct QuantizerTemplate_avx512<Codec, true, 1> : public QuantizerTemplate_avx<Codec, true, 1> {
    QuantizerTemplate_avx512(size_t d, const std::vector<float> &trained) :
        QuantizerTemplate_avx<Codec, true, 1> (d, trained) {}
};

template<class Codec>
struct QuantizerTemplate_avx512<Codec, true, 8> : public QuantizerTemplate_avx<Codec, true, 8> {
    QuantizerTemplate_avx512 (size_t d, const std::vector<float> &trained) :
        QuantizerTemplate_avx<Codec, true, 8> (d, trained) {}
};

template<class Codec>
struct QuantizerTemplate_avx512<Codec, true, 16> : public QuantizerTemplate_avx<Codec, true, 8> {
    QuantizerTemplate_avx512 (size_t d, const std::vector<float> &trained) :
        QuantizerTemplate_avx<Codec, true, 8> (d, trained) {}

    __m512 reconstruct_16_components (const uint8_t * code, int i) const {
        __m512 xi = Codec::decode_16_components (code, i);
        return _mm512_set1_ps(this->vmin) + xi * _mm512_set1_ps (this->vdiff);
    }
};


template<class Codec>
struct QuantizerTemplate_avx512<Codec, false, 1> : public QuantizerTemplate_avx<Codec, false, 1> {
    QuantizerTemplate_avx512 (size_t d, const std::vector<float> &trained) :
        QuantizerTemplate_avx<Codec, false, 1> (d, trained) {}
};

template<class Codec>
struct QuantizerTemplate_avx512<Codec, false, 8> : public QuantizerTemplate_avx<Codec, false, 8> {
    QuantizerTemplate_avx512 (size_t d, const std::vector<float> &trained):
        QuantizerTemplate_avx<Codec, false, 8> (d, trained) {}
};

template<class Codec>
struct QuantizerTemplate_avx512<Codec, false, 16>: public QuantizerTemplate_avx<Codec, false, 8> {
    QuantizerTemplate_avx512 (size_t d, const std::vector<float> &trained):
        QuantizerTemplate_avx<Codec, false, 8> (d, trained) {}

    __m512 reconstruct_16_components (const uint8_t * code, int i) const {
        __m512 xi = Codec::decode_16_components (code, i);
        return _mm512_loadu_ps (this->vmin + i) + xi * _mm512_loadu_ps (this->vdiff + i);
    }
};

/*******************************************************************
 * FP16 quantizer
 *******************************************************************/

template<int SIMDWIDTH>
struct QuantizerFP16_avx512 {};

template<>
struct QuantizerFP16_avx512<1> : public QuantizerFP16_avx<1> {
    QuantizerFP16_avx512(size_t d, const std::vector<float> &unused) :
        QuantizerFP16_avx<1> (d, unused) {}
};

template<>
struct QuantizerFP16_avx512<8> : public QuantizerFP16_avx<8> {
    QuantizerFP16_avx512 (size_t d, const std::vector<float> &trained) :
        QuantizerFP16_avx<8> (d, trained) {}
};

template<>
struct QuantizerFP16_avx512<16>: public QuantizerFP16_avx<8> {
    QuantizerFP16_avx512 (size_t d, const std::vector<float> &trained):
        QuantizerFP16_avx<8> (d, trained) {}

    __m512 reconstruct_16_components (const uint8_t * code, int i) const {
        __m256i codei = _mm256_loadu_si256 ((const __m256i*)(code + 2 * i));
        return _mm512_cvtph_ps (codei);
    }
};

/*******************************************************************
 * 8bit_direct quantizer
 *******************************************************************/

template<int SIMDWIDTH>
struct Quantizer8bitDirect_avx512 {};

template<>
struct Quantizer8bitDirect_avx512<1> : public Quantizer8bitDirect_avx<1> {
    Quantizer8bitDirect_avx512(size_t d, const std::vector<float> &unused) :
        Quantizer8bitDirect_avx<1> (d, unused) {}
};

template<>
struct Quantizer8bitDirect_avx512<8> : public Quantizer8bitDirect_avx<8> {
    Quantizer8bitDirect_avx512 (size_t d, const std::vector<float> &trained):
        Quantizer8bitDirect_avx<8> (d, trained) {}
};

template<>
struct Quantizer8bitDirect_avx512<16> : public Quantizer8bitDirect_avx<8> {
    Quantizer8bitDirect_avx512 (size_t d, const std::vector<float> &trained):
        Quantizer8bitDirect_avx<8> (d, trained) {}

    __m512 reconstruct_16_components (const uint8_t * code, int i) const {
        __m128i x8 = _mm_load_si128((__m128i*)(code + i)); // 16 * int8
        __m512i y8 = _mm512_cvtepu8_epi32 (x8);  // 16 * int32
        return _mm512_cvtepi32_ps (y8); // 16 * float32
    }
};


template<int SIMDWIDTH>
Quantizer *select_quantizer_1_avx512 (QuantizerType qtype, size_t d,
                                      const std::vector<float> & trained)
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
            return new QuantizerFP16_avx512<SIMDWIDTH>(d, trained);
        case QuantizerType::QT_8bit_direct:
            return new Quantizer8bitDirect_avx512<SIMDWIDTH>(d, trained);
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
struct SimilarityL2_avx512<1> : public SimilarityL2_avx<1> {
    static constexpr int simdwidth = 1;
    static constexpr MetricType metric_type = METRIC_L2;

    explicit SimilarityL2_avx512 (const float * y) : SimilarityL2_avx<1> (y) {}
};

template<>
struct SimilarityL2_avx512<8> : public SimilarityL2_avx<8> {
    static constexpr int simdwidth = 8;
    static constexpr MetricType metric_type = METRIC_L2;

    explicit SimilarityL2_avx512 (const float * y) : SimilarityL2_avx<8> (y) {}
};

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


template<int SIMDWIDTH>
struct SimilarityIP_avx512 {};


template<>
struct SimilarityIP_avx512<1> : public SimilarityIP_avx<1> {
    static constexpr int simdwidth = 1;
    static constexpr MetricType metric_type = METRIC_INNER_PRODUCT;

    explicit SimilarityIP_avx512 (const float * y) : SimilarityIP_avx<1> (y) {}
};

template<>
struct SimilarityIP_avx512<8> : public SimilarityIP_avx<8> {
    static constexpr int simdwidth = 8;
    static constexpr MetricType metric_type = METRIC_INNER_PRODUCT;

    explicit SimilarityIP_avx512 (const float * y) : SimilarityIP_avx<8> (y) {}
};

template<>
struct SimilarityIP_avx512<16> {
    static constexpr int simdwidth = 16;
    static constexpr MetricType metric_type = METRIC_INNER_PRODUCT;

    const float *y, *yi;

    float accu;

    explicit SimilarityIP_avx512 (const float * y) : y (y) {}

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


/*******************************************************************
 * DistanceComputer: combines a similarity and a quantizer to do
 * code-to-vector or code-to-code comparisons
 *******************************************************************/

template<class Quantizer, class Similarity, int SIMDWIDTH>
struct DCTemplate_avx512 : SQDistanceComputer {};

template<class Quantizer, class Similarity>
struct DCTemplate_avx512<Quantizer, Similarity, 1> : public DCTemplate_avx<Quantizer, Similarity, 1> {
    DCTemplate_avx512(size_t d, const std::vector<float> &trained) :
        DCTemplate_avx<Quantizer, Similarity, 1> (d, trained) {}
};

template<class Quantizer, class Similarity>
struct DCTemplate_avx512<Quantizer, Similarity, 8> : public DCTemplate_avx<Quantizer, Similarity, 8> {
    DCTemplate_avx512(size_t d, const std::vector<float> &trained) :
        DCTemplate_avx<Quantizer, Similarity, 8> (d, trained) {}
};

template<class Quantizer, class Similarity>
struct DCTemplate_avx512<Quantizer, Similarity, 16> : SQDistanceComputer {
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

    float compute_code_distance(const uint8_t* code1, const uint8_t* code2) const {
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


/*******************************************************************
 * DistanceComputerByte: computes distances in the integer domain
 *******************************************************************/

template<class Similarity, int SIMDWIDTH>
struct DistanceComputerByte_avx512 : SQDistanceComputer {};

template<class Similarity>
struct DistanceComputerByte_avx512<Similarity, 1> : public DistanceComputerByte_avx<Similarity, 1> {
    DistanceComputerByte_avx512(int d, const std::vector<float> &unused) :
        DistanceComputerByte_avx<Similarity, 1> (d, unused) {}
};

template<class Similarity>
struct DistanceComputerByte_avx512<Similarity, 8> : public DistanceComputerByte_avx<Similarity, 8> {
    DistanceComputerByte_avx512(int d, const std::vector<float> &unused) :
        DistanceComputerByte_avx<Similarity, 8> (d, unused) {}
};

template<class Similarity>
struct DistanceComputerByte_avx512<Similarity, 16> : SQDistanceComputer {
    using Sim = Similarity;

    int d;
    std::vector<uint8_t> tmp;

    DistanceComputerByte_avx512(int d, const std::vector<float> &): d(d), tmp(d) {}

    int compute_code_distance(const uint8_t* code1, const uint8_t* code2) const {
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


template<class DCClass>
InvertedListScanner* sel2_InvertedListScanner_avx512 (
        const ScalarQuantizer *sq,
        const Index *quantizer, bool store_pairs, bool r)
{
    return sel2_InvertedListScanner<DCClass> (sq, quantizer, store_pairs, r);
}

template<class Similarity, class Codec, bool uniform>
InvertedListScanner* sel12_InvertedListScanner_avx512 (
        const ScalarQuantizer *sq,
        const Index *quantizer, bool store_pairs, bool r)
{
    constexpr int SIMDWIDTH = Similarity::simdwidth;
    using QuantizerClass = QuantizerTemplate_avx512<Codec, uniform, SIMDWIDTH>;
    using DCClass = DCTemplate_avx512<QuantizerClass, Similarity, SIMDWIDTH>;
    return sel2_InvertedListScanner_avx512<DCClass> (sq, quantizer, store_pairs, r);
}


template<class Similarity>
InvertedListScanner* sel1_InvertedListScanner_avx512 (
        const ScalarQuantizer *sq, const Index *quantizer,
        bool store_pairs, bool r)
{
    constexpr int SIMDWIDTH = Similarity::simdwidth;
    switch(sq->qtype) {
    case QuantizerType::QT_8bit_uniform:
        return sel12_InvertedListScanner_avx512
            <Similarity, Codec8bit_avx512, true>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_4bit_uniform:
        return sel12_InvertedListScanner_avx512
            <Similarity, Codec4bit_avx512, true>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_8bit:
        return sel12_InvertedListScanner_avx512
            <Similarity, Codec8bit_avx512, false>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_4bit:
        return sel12_InvertedListScanner_avx512
            <Similarity, Codec4bit_avx512, false>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_6bit:
        return sel12_InvertedListScanner_avx512
            <Similarity, Codec6bit_avx512, false>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_fp16:
        return sel2_InvertedListScanner_avx512
            <DCTemplate_avx512<QuantizerFP16_avx512<SIMDWIDTH>, Similarity, SIMDWIDTH> >
            (sq, quantizer, store_pairs, r);
    case QuantizerType::QT_8bit_direct:
        if (sq->d % 16 == 0) {
            return sel2_InvertedListScanner_avx512
                <DistanceComputerByte_avx512<Similarity, SIMDWIDTH> >
                (sq, quantizer, store_pairs, r);
        } else {
            return sel2_InvertedListScanner_avx512
                <DCTemplate_avx512<Quantizer8bitDirect_avx512<SIMDWIDTH>,
                            Similarity, SIMDWIDTH> >
                (sq, quantizer, store_pairs, r);
        }
    }

    FAISS_THROW_MSG ("unknown qtype");
    return nullptr;
}

template<int SIMDWIDTH>
InvertedListScanner* sel0_InvertedListScanner_avx512 (
        MetricType mt, const ScalarQuantizer *sq,
        const Index *quantizer, bool store_pairs, bool by_residual)
{
    if (mt == METRIC_L2) {
        return sel1_InvertedListScanner_avx512<SimilarityL2_avx512<SIMDWIDTH> >
            (sq, quantizer, store_pairs, by_residual);
    } else if (mt == METRIC_INNER_PRODUCT) {
        return sel1_InvertedListScanner_avx512<SimilarityIP_avx512<SIMDWIDTH> >
            (sq, quantizer, store_pairs, by_residual);
    } else {
        FAISS_THROW_MSG("unsupported metric type");
    }
}

} // namespace faiss
#endif
