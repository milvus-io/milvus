/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "IndexScalarQuantizer.h"

#include <cstdio>
#include <algorithm>

#include <omp.h>

#ifdef __SSE__
#include <immintrin.h>
#endif

#include "utils.h"
#include "FaissAssert.h"
#include "AuxIndexStructures.h"

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


struct SQDistanceComputer: DistanceComputer {

    const float *q;
    const uint8_t *codes;
    size_t code_size;

    SQDistanceComputer (): q(nullptr), codes (nullptr), code_size (0)
    {}

};


namespace {

typedef Index::idx_t idx_t;
typedef ScalarQuantizer::QuantizerType QuantizerType;
typedef ScalarQuantizer::RangeStat RangeStat;



/*******************************************************************
 * Codec: converts between values in [0, 1] and an index in a code
 * array. The "i" parameter is the vector component index (not byte
 * index).
 */

struct Codec8bit {

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
};


struct Codec4bit {

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
};


#ifdef USE_AVX


uint16_t encode_fp16 (float x) {
    __m128 xf = _mm_set1_ps (x);
    __m128i xi = _mm_cvtps_ph (
         xf, _MM_FROUND_TO_NEAREST_INT |_MM_FROUND_NO_EXC);
    return _mm_cvtsi128_si32 (xi) & 0xffff;
}


float decode_fp16 (uint16_t x) {
    __m128i xi = _mm_set1_epi16 (x);
    __m128 xf = _mm_cvtph_ps (xi);
    return _mm_cvtss_f32 (xf);
}

#else

// non-intrinsic FP16 <-> FP32 code adapted from
// https://github.com/ispc/ispc/blob/master/stdlib.ispc

float floatbits (uint32_t x) {
    void *xptr = &x;
    return *(float*)xptr;
}

uint32_t intbits (float f) {
    void *fptr = &f;
    return *(uint32_t*)fptr;
}


uint16_t encode_fp16 (float f) {

    // via Fabian "ryg" Giesen.
    // https://gist.github.com/2156668
    uint32_t sign_mask = 0x80000000u;
    int32_t o;

    uint32_t fint = intbits(f);
    uint32_t sign = fint & sign_mask;
    fint ^= sign;

    // NOTE all the integer compares in this function can be safely
    // compiled into signed compares since all operands are below
    // 0x80000000. Important if you want fast straight SSE2 code (since
    // there's no unsigned PCMPGTD).

    // Inf or NaN (all exponent bits set)
    // NaN->qNaN and Inf->Inf
    // unconditional assignment here, will override with right value for
    // the regular case below.
    uint32_t f32infty = 255u << 23;
    o = (fint > f32infty) ? 0x7e00u : 0x7c00u;

    // (De)normalized number or zero
    // update fint unconditionally to save the blending; we don't need it
    // anymore for the Inf/NaN case anyway.

    const uint32_t round_mask = ~0xfffu;
    const uint32_t magic = 15u << 23;

    // Shift exponent down, denormalize if necessary.
    // NOTE This represents half-float denormals using single
    // precision denormals.  The main reason to do this is that
    // there's no shift with per-lane variable shifts in SSE*, which
    // we'd otherwise need. It has some funky side effects though:
    // - This conversion will actually respect the FTZ (Flush To Zero)
    //   flag in MXCSR - if it's set, no half-float denormals will be
    //   generated. I'm honestly not sure whether this is good or
    //   bad. It's definitely interesting.
    // - If the underlying HW doesn't support denormals (not an issue
    //   with Intel CPUs, but might be a problem on GPUs or PS3 SPUs),
    //   you will always get flush-to-zero behavior. This is bad,
    //   unless you're on a CPU where you don't care.
    // - Denormals tend to be slow. FP32 denormals are rare in
    //   practice outside of things like recursive filters in DSP -
    //   not a typical half-float application. Whether FP16 denormals
    //   are rare in practice, I don't know. Whatever slow path your
    //   HW may or may not have for denormals, this may well hit it.
    float fscale = floatbits(fint & round_mask) * floatbits(magic);
    fscale = std::min(fscale, floatbits((31u << 23) - 0x1000u));
    int32_t fint2 = intbits(fscale) - round_mask;

    if (fint < f32infty)
        o = fint2 >> 13; // Take the bits!

    return (o | (sign >> 16));
}

float decode_fp16 (uint16_t h) {

    // https://gist.github.com/2144712
    // Fabian "ryg" Giesen.

    const uint32_t shifted_exp = 0x7c00u << 13; // exponent mask after shift

    int32_t o = ((int32_t)(h & 0x7fffu)) << 13;     // exponent/mantissa bits
    int32_t exp = shifted_exp & o;   // just the exponent
    o += (int32_t)(127 - 15) << 23;        // exponent adjust

    int32_t infnan_val = o + ((int32_t)(128 - 16) << 23);
    int32_t zerodenorm_val = intbits(
                 floatbits(o + (1u<<23)) - floatbits(113u << 23));
    int32_t reg_val = (exp == 0) ? zerodenorm_val : o;

    int32_t sign_bit = ((int32_t)(h & 0x8000u)) << 16;
    return floatbits(((exp == shifted_exp) ? infnan_val : reg_val) | sign_bit);
}

#endif



/*******************************************************************
 * Quantizer: normalizes scalar vector components, then passes them
 * through a codec
 *******************************************************************/



struct Quantizer {
    // encodes one vector. Assumes code is filled with 0s on input!
    virtual void encode_vector(const float *x, uint8_t *code) const = 0;
    virtual void decode_vector(const uint8_t *code, float *x) const = 0;

    virtual ~Quantizer() {}
};


template<class Codec, bool uniform, int SIMD>
struct QuantizerTemplate {};


template<class Codec>
struct QuantizerTemplate<Codec, true, 1>: Quantizer {
    const size_t d;
    const float vmin, vdiff;

    QuantizerTemplate(size_t d, const std::vector<float> &trained):
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
struct QuantizerTemplate<Codec, true, 8>: QuantizerTemplate<Codec, true, 1> {

    QuantizerTemplate (size_t d, const std::vector<float> &trained):
        QuantizerTemplate<Codec, true, 1> (d, trained) {}

    __m256 reconstruct_8_components (const uint8_t * code, int i) const
    {
        __m256 xi = Codec::decode_8_components (code, i);
        return _mm256_set1_ps(this->vmin) + xi * _mm256_set1_ps (this->vdiff);
    }

};

#endif



template<class Codec>
struct QuantizerTemplate<Codec, false, 1>: Quantizer {
    const size_t d;
    const float *vmin, *vdiff;

    QuantizerTemplate (size_t d, const std::vector<float> &trained):
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
struct QuantizerTemplate<Codec, false, 8>: QuantizerTemplate<Codec, false, 1> {

    QuantizerTemplate (size_t d, const std::vector<float> &trained):
        QuantizerTemplate<Codec, false, 1> (d, trained) {}

    __m256 reconstruct_8_components (const uint8_t * code, int i) const
    {
        __m256 xi = Codec::decode_8_components (code, i);
        return _mm256_loadu_ps (this->vmin + i) + xi * _mm256_loadu_ps (this->vdiff + i);
    }


};

#endif

/*******************************************************************
 * FP16 quantizer
 *******************************************************************/

template<int SIMDWIDTH>
struct QuantizerFP16 {};

template<>
struct QuantizerFP16<1>: Quantizer {
    const size_t d;

    QuantizerFP16(size_t d, const std::vector<float> & /* unused */):
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
struct QuantizerFP16<8>: QuantizerFP16<1> {

    QuantizerFP16 (size_t d, const std::vector<float> &trained):
        QuantizerFP16<1> (d, trained) {}

    __m256 reconstruct_8_components (const uint8_t * code, int i) const
    {
        __m128i codei = _mm_loadu_si128 ((const __m128i*)(code + 2 * i));
        return _mm256_cvtph_ps (codei);
    }

};

#endif

/*******************************************************************
 * 8bit_direct quantizer
 *******************************************************************/

template<int SIMDWIDTH>
struct Quantizer8bitDirect {};

template<>
struct Quantizer8bitDirect<1>: Quantizer {
    const size_t d;

    Quantizer8bitDirect(size_t d, const std::vector<float> & /* unused */):
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
struct Quantizer8bitDirect<8>: Quantizer8bitDirect<1> {

    Quantizer8bitDirect (size_t d, const std::vector<float> &trained):
        Quantizer8bitDirect<1> (d, trained) {}

    __m256 reconstruct_8_components (const uint8_t * code, int i) const
    {
        __m128i x8 = _mm_loadl_epi64((__m128i*)(code + i)); // 8 * int8
        __m256i y8 = _mm256_cvtepu8_epi32 (x8);  // 8 * int32
        return _mm256_cvtepi32_ps (y8); // 8 * float32
    }

};

#endif


template<int SIMDWIDTH>
Quantizer *select_quantizer (
          QuantizerType qtype,
          size_t d, const std::vector<float> & trained)
{
    switch(qtype) {
    case ScalarQuantizer::QT_8bit:
        return new QuantizerTemplate<Codec8bit, false, SIMDWIDTH>(d, trained);
    case ScalarQuantizer::QT_4bit:
        return new QuantizerTemplate<Codec4bit, false, SIMDWIDTH>(d, trained);
    case ScalarQuantizer::QT_8bit_uniform:
        return new QuantizerTemplate<Codec8bit, true, SIMDWIDTH>(d, trained);
    case ScalarQuantizer::QT_4bit_uniform:
        return new QuantizerTemplate<Codec4bit, true, SIMDWIDTH>(d, trained);
    case ScalarQuantizer::QT_fp16:
        return new QuantizerFP16<SIMDWIDTH> (d, trained);
    case ScalarQuantizer::QT_8bit_direct:
        return new Quantizer8bitDirect<SIMDWIDTH> (d, trained);
    }
    FAISS_THROW_MSG ("unknown qtype");
}



Quantizer *select_quantizer (const ScalarQuantizer &sq)
{
#ifdef USE_AVX
    if (sq.d % 8 == 0) {
        return select_quantizer<8> (sq.qtype, sq.d, sq.trained);
    } else
#endif
    {
        return select_quantizer<1> (sq.qtype, sq.d, sq.trained);
    }
}




/*******************************************************************
 * Quantizer range training
 */

static float sqr (float x) {
    return x * x;
}


void train_Uniform(RangeStat rs, float rs_arg,
                   idx_t n, int k, const float *x,
                   std::vector<float> & trained)
{
    trained.resize (2);
    float & vmin = trained[0];
    float & vmax = trained[1];

    if (rs == ScalarQuantizer::RS_minmax) {
        vmin = HUGE_VAL; vmax = -HUGE_VAL;
        for (size_t i = 0; i < n; i++) {
            if (x[i] < vmin) vmin = x[i];
            if (x[i] > vmax) vmax = x[i];
        }
        float vexp = (vmax - vmin) * rs_arg;
        vmin -= vexp;
        vmax += vexp;
    } else if (rs == ScalarQuantizer::RS_meanstd) {
        double sum = 0, sum2 = 0;
        for (size_t i = 0; i < n; i++) {
            sum += x[i];
            sum2 += x[i] * x[i];
        }
        float mean = sum / n;
        float var = sum2 / n - mean * mean;
        float std = var <= 0 ? 1.0 : sqrt(var);

        vmin = mean - std * rs_arg ;
        vmax = mean + std * rs_arg ;
    } else if (rs == ScalarQuantizer::RS_quantiles) {
        std::vector<float> x_copy(n);
        memcpy(x_copy.data(), x, n * sizeof(*x));
        // TODO just do a qucikselect
        std::sort(x_copy.begin(), x_copy.end());
        int o = int(rs_arg * n);
        if (o < 0) o = 0;
        if (o > n - o) o = n / 2;
        vmin = x_copy[o];
        vmax = x_copy[n - 1 - o];

    } else if (rs == ScalarQuantizer::RS_optim) {
        float a, b;
        float sx = 0;
        {
            vmin = HUGE_VAL, vmax = -HUGE_VAL;
            for (size_t i = 0; i < n; i++) {
                if (x[i] < vmin) vmin = x[i];
                if (x[i] > vmax) vmax = x[i];
                sx += x[i];
            }
            b = vmin;
            a = (vmax - vmin) / (k - 1);
        }
        int verbose = false;
        int niter = 2000;
        float last_err = -1;
        int iter_last_err = 0;
        for (int it = 0; it < niter; it++) {
            float sn = 0, sn2 = 0, sxn = 0, err1 = 0;

            for (idx_t i = 0; i < n; i++) {
                float xi = x[i];
                float ni = floor ((xi - b) / a + 0.5);
                if (ni < 0) ni = 0;
                if (ni >= k) ni = k - 1;
                err1 += sqr (xi - (ni * a + b));
                sn  += ni;
                sn2 += ni * ni;
                sxn += ni * xi;
            }

            if (err1 == last_err) {
                iter_last_err ++;
                if (iter_last_err == 16) break;
            } else {
                last_err = err1;
                iter_last_err = 0;
            }

            float det = sqr (sn) - sn2 * n;

            b = (sn * sxn - sn2 * sx) / det;
            a = (sn * sx - n * sxn) / det;
            if (verbose) {
                printf ("it %d, err1=%g            \r", it, err1);
                fflush(stdout);
            }
        }
        if (verbose) printf("\n");

        vmin = b;
        vmax = b + a * (k - 1);

    } else {
        FAISS_THROW_MSG ("Invalid qtype");
    }
    vmax -= vmin;
}

void train_NonUniform(RangeStat rs, float rs_arg,
                      idx_t n, int d, int k, const float *x,
                      std::vector<float> & trained)
{

    trained.resize (2 * d);
    float * vmin = trained.data();
    float * vmax = trained.data() + d;
    if (rs == ScalarQuantizer::RS_minmax) {
        memcpy (vmin, x, sizeof(*x) * d);
        memcpy (vmax, x, sizeof(*x) * d);
        for (size_t i = 1; i < n; i++) {
            const float *xi = x + i * d;
            for (size_t j = 0; j < d; j++) {
                if (xi[j] < vmin[j]) vmin[j] = xi[j];
                if (xi[j] > vmax[j]) vmax[j] = xi[j];
            }
        }
        float *vdiff = vmax;
        for (size_t j = 0; j < d; j++) {
            float vexp = (vmax[j] - vmin[j]) * rs_arg;
            vmin[j] -= vexp;
            vmax[j] += vexp;
            vdiff [j] = vmax[j] - vmin[j];
        }
    } else {
        // transpose
        std::vector<float> xt(n * d);
        for (size_t i = 1; i < n; i++) {
            const float *xi = x + i * d;
            for (size_t j = 0; j < d; j++) {
                xt[j * n + i] = xi[j];
            }
        }
        std::vector<float> trained_d(2);
#pragma omp parallel for
        for (size_t j = 0; j < d; j++) {
            train_Uniform(rs, rs_arg,
                          n, k, xt.data() + j * n,
                          trained_d);
            vmin[j] = trained_d[0];
            vmax[j] = trained_d[1];
        }
    }
}



/*******************************************************************
 * Similarity: gets vector components and computes a similarity wrt. a
 * query vector stored in the object. The data fields just encapsulate
 * an accumulator.
 */

template<int SIMDWIDTH>
struct SimilarityL2 {};


template<>
struct SimilarityL2<1> {
    static constexpr int simdwidth = 1;
    static constexpr MetricType metric_type = METRIC_L2;

    const float *y, *yi;

    explicit SimilarityL2 (const float * y): y(y) {}

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
struct SimilarityL2<8> {
    static constexpr int simdwidth = 8;
    static constexpr MetricType metric_type = METRIC_L2;

    const float *y, *yi;

    explicit SimilarityL2 (const float * y): y(y) {}
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


template<int SIMDWIDTH>
struct SimilarityIP {};


template<>
struct SimilarityIP<1> {
    static constexpr int simdwidth = 1;
    static constexpr MetricType metric_type = METRIC_INNER_PRODUCT;
    const float *y, *yi;

    float accu;

    explicit SimilarityIP (const float * y):
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
struct SimilarityIP<8> {
    static constexpr int simdwidth = 8;
    static constexpr MetricType metric_type = METRIC_INNER_PRODUCT;

    const float *y, *yi;

    float accu;

    explicit SimilarityIP (const float * y):
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


/*******************************************************************
 * DistanceComputer: combines a similarity and a quantizer to do
 * code-to-vector or code-to-code comparisons
 *******************************************************************/

template<class Quantizer, class Similarity, int SIMDWIDTH>
struct DCTemplate : SQDistanceComputer {};

template<class Quantizer, class Similarity>
struct DCTemplate<Quantizer, Similarity, 1> : SQDistanceComputer
{
    using Sim = Similarity;

    Quantizer quant;

    DCTemplate(size_t d, const std::vector<float> &trained):
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
struct DCTemplate<Quantizer, Similarity, 8> : SQDistanceComputer
{
    using Sim = Similarity;

    Quantizer quant;

    DCTemplate(size_t d, const std::vector<float> &trained):
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



/*******************************************************************
 * DistanceComputerByte: computes distances in the integer domain
 *******************************************************************/

template<class Similarity, int SIMDWIDTH>
struct DistanceComputerByte : SQDistanceComputer {};

template<class Similarity>
struct DistanceComputerByte<Similarity, 1> : SQDistanceComputer {
    using Sim = Similarity;

    int d;
    std::vector<uint8_t> tmp;

    DistanceComputerByte(int d, const std::vector<float> &): d(d), tmp(d) {
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
struct DistanceComputerByte<Similarity, 8> : SQDistanceComputer {
    using Sim = Similarity;

    int d;
    std::vector<uint8_t> tmp;

    DistanceComputerByte(int d, const std::vector<float> &): d(d), tmp(d) {
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

/*******************************************************************
 * select_distance_computer: runtime selection of template
 * specialization
 *******************************************************************/


template<class Sim>
SQDistanceComputer *select_distance_computer (
          QuantizerType qtype,
          size_t d, const std::vector<float> & trained)
{
    constexpr int SIMDWIDTH = Sim::simdwidth;
    switch(qtype) {
    case ScalarQuantizer::QT_8bit_uniform:
        return new DCTemplate<QuantizerTemplate<Codec8bit, true, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case ScalarQuantizer::QT_4bit_uniform:
        return new DCTemplate<QuantizerTemplate<Codec4bit, true, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case ScalarQuantizer::QT_8bit:
        return new DCTemplate<QuantizerTemplate<Codec8bit, false, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case ScalarQuantizer::QT_4bit:
        return new DCTemplate<QuantizerTemplate<Codec4bit, false, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case ScalarQuantizer::QT_fp16:
        return new DCTemplate
            <QuantizerFP16<SIMDWIDTH>, Sim, SIMDWIDTH>(d, trained);

    case ScalarQuantizer::QT_8bit_direct:
        if (d % 16 == 0) {
            return new DistanceComputerByte<Sim, SIMDWIDTH>(d, trained);
        } else {
            return new DCTemplate
                <Quantizer8bitDirect<SIMDWIDTH>, Sim, SIMDWIDTH>(d, trained);
        }
    }
    FAISS_THROW_MSG ("unknown qtype");
    return nullptr;
}



} // anonymous namespace



/*******************************************************************
 * ScalarQuantizer implementation
 ********************************************************************/

ScalarQuantizer::ScalarQuantizer
          (size_t d, QuantizerType qtype):
              qtype (qtype), rangestat(RS_minmax), rangestat_arg(0), d (d)
{
    switch (qtype) {
    case QT_8bit:
    case QT_8bit_uniform:
    case QT_8bit_direct:
        code_size = d;
        break;
    case QT_4bit:
    case QT_4bit_uniform:
        code_size = (d + 1) / 2;
        break;
    case QT_fp16:
        code_size = d * 2;
        break;
    }

}

ScalarQuantizer::ScalarQuantizer ():
    qtype(QT_8bit),
    rangestat(RS_minmax), rangestat_arg(0), d (0), code_size(0)
{}

void ScalarQuantizer::train (size_t n, const float *x)
{
    int bit_per_dim =
        qtype == QT_4bit_uniform ? 4 :
        qtype == QT_4bit ? 4 :
        qtype == QT_8bit_uniform ? 8 :
        qtype == QT_8bit ? 8 : -1;

    switch (qtype) {
    case QT_4bit_uniform: case QT_8bit_uniform:
        train_Uniform (rangestat, rangestat_arg,
                       n * d, 1 << bit_per_dim, x, trained);
        break;
    case QT_4bit: case QT_8bit:
        train_NonUniform (rangestat, rangestat_arg,
                          n, d, 1 << bit_per_dim, x, trained);
        break;
    case QT_fp16:
    case QT_8bit_direct:
        // no training necessary
        break;
    }
}

void ScalarQuantizer::compute_codes (const float * x,
                                     uint8_t * codes,
                                     size_t n) const
{
    Quantizer *squant = select_quantizer (*this);
    ScopeDeleter1<Quantizer> del(squant);
    memset (codes, 0, code_size * n);
#pragma omp parallel for
    for (size_t i = 0; i < n; i++)
        squant->encode_vector (x + i * d, codes + i * code_size);
}

void ScalarQuantizer::decode (const uint8_t *codes, float *x, size_t n) const
{
    Quantizer *squant = select_quantizer (*this);
    ScopeDeleter1<Quantizer> del(squant);
#pragma omp parallel for
    for (size_t i = 0; i < n; i++)
        squant->decode_vector (codes + i * code_size, x + i * d);
}


SQDistanceComputer *
ScalarQuantizer::get_distance_computer (MetricType metric) const
{
#ifdef USE_AVX
    if (d % 8 == 0) {
        if (metric == METRIC_L2) {
            return select_distance_computer<SimilarityL2<8> >
                (qtype, d, trained);
        } else {
            return select_distance_computer<SimilarityIP<8> >
                (qtype, d, trained);
        }
    } else
#endif
    {
        if (metric == METRIC_L2) {
            return select_distance_computer<SimilarityL2<1> >
                (qtype, d, trained);
        } else {
            return select_distance_computer<SimilarityIP<1> >
                (qtype, d, trained);
        }
    }
}


/*******************************************************************
 * IndexScalarQuantizer/IndexIVFScalarQuantizer scanner object
 *
 * It is an InvertedListScanner, but is designed to work with
 * IndexScalarQuantizer as well.
 ********************************************************************/

namespace {


template<bool store_pairs, class DCClass>
struct IVFSQScannerIP: InvertedListScanner {
    DCClass dc;
    bool by_residual;

    size_t code_size;

    idx_t list_no;  /// current list (set to 0 for Flat index
    float accu0;    /// added to all distances

    IVFSQScannerIP(int d, const std::vector<float> & trained,
                   size_t code_size, bool by_residual=false):
        dc(d, trained), by_residual(by_residual),
        code_size(code_size), list_no(0), accu0(0)
    {}


    void set_query (const float *query) override {
        dc.set_query (query);
    }

    void set_list (idx_t list_no, float coarse_dis) override {
        this->list_no = list_no;
        accu0 = by_residual ? coarse_dis : 0;
    }

    float distance_to_code (const uint8_t *code) const final {
        return accu0 + dc.query_to_code (code);
    }

    size_t scan_codes (size_t list_size,
                       const uint8_t *codes,
                       const idx_t *ids,
                       float *simi, idx_t *idxi,
                       size_t k) const override
    {
        size_t nup = 0;

        for (size_t j = 0; j < list_size; j++) {

            float accu = accu0 + dc.query_to_code (codes);

            if (accu > simi [0]) {
                minheap_pop (k, simi, idxi);
                long id = store_pairs ? (list_no << 32 | j) : ids[j];
                minheap_push (k, simi, idxi, accu, id);
                nup++;
            }
            codes += code_size;
        }
        return nup;
    }

    void scan_codes_range (size_t list_size,
                           const uint8_t *codes,
                           const idx_t *ids,
                           float radius,
                           RangeQueryResult & res) const override
    {
        for (size_t j = 0; j < list_size; j++) {
            float accu = accu0 + dc.query_to_code (codes);
            if (accu > radius) {
                long id = store_pairs ? (list_no << 32 | j) : ids[j];
                res.add (accu, id);
            }
            codes += code_size;
        }
    }


};


template<bool store_pairs, class DCClass>
struct IVFSQScannerL2: InvertedListScanner {

    DCClass dc;

    bool by_residual;
    size_t code_size;
    const Index *quantizer;
    idx_t list_no;    /// current inverted list
    const float *x;   /// current query

    std::vector<float> tmp;

    IVFSQScannerL2(int d, const std::vector<float> & trained,
                   size_t code_size, const Index *quantizer,
                   bool by_residual):
        dc(d, trained), by_residual(by_residual),
        code_size(code_size), quantizer(quantizer),
        list_no (0), x (nullptr), tmp (d)
    {
    }


    void set_query (const float *query) override {
        x = query;
        if (!quantizer) {
            dc.set_query (query);
        }
    }


    void set_list (idx_t list_no, float /*coarse_dis*/) override {
        if (by_residual) {
            this->list_no = list_no;
            // shift of x_in wrt centroid
            quantizer->compute_residual (x, tmp.data(), list_no);
            dc.set_query (tmp.data ());
        } else {
            dc.set_query (x);
        }
    }

    float distance_to_code (const uint8_t *code) const final {
        return dc.query_to_code (code);
    }

    size_t scan_codes (size_t list_size,
                       const uint8_t *codes,
                       const idx_t *ids,
                       float *simi, idx_t *idxi,
                       size_t k) const override
    {
        size_t nup = 0;
        for (size_t j = 0; j < list_size; j++) {

            float dis = dc.query_to_code (codes);

            if (dis < simi [0]) {
                maxheap_pop (k, simi, idxi);
                long id = store_pairs ? (list_no << 32 | j) : ids[j];
                maxheap_push (k, simi, idxi, dis, id);
                nup++;
            }
            codes += code_size;
        }
        return nup;
    }

    void scan_codes_range (size_t list_size,
                           const uint8_t *codes,
                           const idx_t *ids,
                           float radius,
                           RangeQueryResult & res) const override
    {
        for (size_t j = 0; j < list_size; j++) {
            float dis = dc.query_to_code (codes);
            if (dis < radius) {
                long id = store_pairs ? (list_no << 32 | j) : ids[j];
                res.add (dis, id);
            }
            codes += code_size;
        }
    }


};

template<class DCClass>
InvertedListScanner* sel2_InvertedListScanner
      (const ScalarQuantizer *sq,
       const Index *quantizer, bool store_pairs, bool r)
{
    if (DCClass::Sim::metric_type == METRIC_L2) {
        if (store_pairs) {
            return new IVFSQScannerL2<true, DCClass>
                (sq->d, sq->trained, sq->code_size, quantizer, r);
        } else {
            return new IVFSQScannerL2<false, DCClass>
                (sq->d, sq->trained, sq->code_size, quantizer, r);
        }
    } else {
        if (store_pairs) {
            return new IVFSQScannerIP<true, DCClass>
                (sq->d, sq->trained, sq->code_size, r);
        } else {
            return new IVFSQScannerIP<false, DCClass>
                (sq->d, sq->trained, sq->code_size, r);
        }
    }
}

template<class Similarity, class Codec, bool uniform>
InvertedListScanner* sel12_InvertedListScanner
        (const ScalarQuantizer *sq,
         const Index *quantizer, bool store_pairs, bool r)
{
    constexpr int SIMDWIDTH = Similarity::simdwidth;
    using QuantizerClass = QuantizerTemplate<Codec, uniform, SIMDWIDTH>;
    using DCClass = DCTemplate<QuantizerClass, Similarity, SIMDWIDTH>;
    return sel2_InvertedListScanner<DCClass> (sq, quantizer, store_pairs, r);
}



template<class Similarity>
InvertedListScanner* sel1_InvertedListScanner
        (const ScalarQuantizer *sq, const Index *quantizer,
         bool store_pairs, bool r)
{
    constexpr int SIMDWIDTH = Similarity::simdwidth;
    switch(sq->qtype) {
    case ScalarQuantizer::QT_8bit_uniform:
        return sel12_InvertedListScanner
            <Similarity, Codec8bit, true>(sq, quantizer, store_pairs, r);
    case ScalarQuantizer::QT_4bit_uniform:
        return sel12_InvertedListScanner
            <Similarity, Codec4bit, true>(sq, quantizer, store_pairs, r);
    case ScalarQuantizer::QT_8bit:
        return sel12_InvertedListScanner
            <Similarity, Codec8bit, false>(sq, quantizer, store_pairs, r);
    case ScalarQuantizer::QT_4bit:
        return sel12_InvertedListScanner
            <Similarity, Codec4bit, false>(sq, quantizer, store_pairs, r);
    case ScalarQuantizer::QT_fp16:
        return sel2_InvertedListScanner
            <DCTemplate<QuantizerFP16<SIMDWIDTH>, Similarity, SIMDWIDTH> >
            (sq, quantizer, store_pairs, r);
    case ScalarQuantizer::QT_8bit_direct:
        if (sq->d % 16 == 0) {
            return sel2_InvertedListScanner
                <DistanceComputerByte<Similarity, SIMDWIDTH> >
                (sq, quantizer, store_pairs, r);
        } else {
            return sel2_InvertedListScanner
                <DCTemplate<Quantizer8bitDirect<SIMDWIDTH>,
                            Similarity, SIMDWIDTH> >
                (sq, quantizer, store_pairs, r);
        }

    }

    FAISS_THROW_MSG ("unknown qtype");
    return nullptr;
}

template<int SIMDWIDTH>
InvertedListScanner* sel0_InvertedListScanner
        (MetricType mt, const ScalarQuantizer *sq,
         const Index *quantizer, bool store_pairs, bool by_residual)
{
    if (mt == METRIC_L2) {
        return sel1_InvertedListScanner<SimilarityL2<SIMDWIDTH> >
            (sq, quantizer, store_pairs, by_residual);
    } else {
        return sel1_InvertedListScanner<SimilarityIP<SIMDWIDTH> >
            (sq, quantizer, store_pairs, by_residual);
    }
}


InvertedListScanner* select_InvertedListScanner
        (MetricType mt, const ScalarQuantizer *sq,
         const Index *quantizer, bool store_pairs, bool by_residual=false)
{
#ifdef USE_AVX
    if (sq->d % 8 == 0) {
        return sel0_InvertedListScanner<8>
            (mt, sq, quantizer, store_pairs, by_residual);
    } else
#endif
    {
        return sel0_InvertedListScanner<1>
            (mt, sq, quantizer, store_pairs, by_residual);
    }
}


} // anonymous namespace


/*******************************************************************
 * IndexScalarQuantizer implementation
 ********************************************************************/

IndexScalarQuantizer::IndexScalarQuantizer
                      (int d, ScalarQuantizer::QuantizerType qtype,
                       MetricType metric):
          Index(d, metric),
          sq (d, qtype)
{
    is_trained =
        qtype == ScalarQuantizer::QT_fp16 ||
        qtype == ScalarQuantizer::QT_8bit_direct;
    code_size = sq.code_size;
}


IndexScalarQuantizer::IndexScalarQuantizer ():
    IndexScalarQuantizer(0, ScalarQuantizer::QT_8bit)
{}

void IndexScalarQuantizer::train(idx_t n, const float* x)
{
    sq.train(n, x);
    is_trained = true;
}

void IndexScalarQuantizer::add(idx_t n, const float* x)
{
    FAISS_THROW_IF_NOT (is_trained);
    codes.resize ((n + ntotal) * code_size);
    sq.compute_codes (x, &codes[ntotal * code_size], n);
    ntotal += n;
}


void IndexScalarQuantizer::search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const
{
    FAISS_THROW_IF_NOT (is_trained);

#pragma omp parallel
    {
        InvertedListScanner* scanner = select_InvertedListScanner
            (metric_type, &sq, nullptr, true);
        ScopeDeleter1<InvertedListScanner> del(scanner);

#pragma omp for
        for (size_t i = 0; i < n; i++) {
            float * D = distances + k * i;
            idx_t * I = labels + k * i;
            // re-order heap
            if (metric_type == METRIC_L2) {
                maxheap_heapify (k, D, I);
            } else {
                minheap_heapify (k, D, I);
            }
            scanner->set_query (x + i * d);
            scanner->scan_codes (ntotal, codes.data(),
                                 nullptr, D, I, k);

            // re-order heap
            if (metric_type == METRIC_L2) {
                maxheap_reorder (k, D, I);
            } else {
                minheap_reorder (k, D, I);
            }
        }
    }

}


DistanceComputer *IndexScalarQuantizer::get_distance_computer () const
{
    SQDistanceComputer *dc = sq.get_distance_computer (metric_type);
    dc->code_size = sq.code_size;
    dc->codes = codes.data();
    return dc;
}


void IndexScalarQuantizer::reset()
{
    codes.clear();
    ntotal = 0;
}

void IndexScalarQuantizer::reconstruct_n(
             idx_t i0, idx_t ni, float* recons) const
{
    Quantizer *squant = select_quantizer (sq);
    ScopeDeleter1<Quantizer> del (squant);
    for (size_t i = 0; i < ni; i++) {
        squant->decode_vector(&codes[(i + i0) * code_size], recons + i * d);
    }
}

void IndexScalarQuantizer::reconstruct(idx_t key, float* recons) const
{
    reconstruct_n(key, 1, recons);
}


/*******************************************************************
 * IndexIVFScalarQuantizer implementation
 ********************************************************************/

IndexIVFScalarQuantizer::IndexIVFScalarQuantizer
          (Index *quantizer, size_t d, size_t nlist,
           QuantizerType qtype, MetricType metric):
              IndexIVF (quantizer, d, nlist, 0, metric),
              sq (d, qtype)
{
    code_size = sq.code_size;
    // was not known at construction time
    invlists->code_size = code_size;
    is_trained = false;
    by_residual = true;
}

IndexIVFScalarQuantizer::IndexIVFScalarQuantizer ():
      IndexIVF ()
{
    by_residual = true;
}

void IndexIVFScalarQuantizer::train_residual (idx_t n, const float *x)
{
    const float * x_in = x;

    // 100k points more than enough
    x = fvecs_maybe_subsample (
         d, (size_t*)&n, 100000,
         x, verbose, 1234);

    ScopeDeleter<float> del_x (x_in == x ? nullptr : x);

    if (by_residual) {
        long * idx = new long [n];
        ScopeDeleter<long> del (idx);
        quantizer->assign (n, x, idx);
        float *residuals = new float [n * d];
        ScopeDeleter<float> del2 (residuals);

#pragma omp parallel for
        for (idx_t i = 0; i < n; i++) {
            quantizer->compute_residual (x + i * d, residuals + i * d, idx[i]);
        }
        sq.train (n, residuals);
    } else {
        sq.train (n, x);
    }

}

void IndexIVFScalarQuantizer::encode_vectors(idx_t n, const float* x,
                                             const idx_t *list_nos,
                                             uint8_t * codes) const
{
    Quantizer *squant = select_quantizer (sq);
    ScopeDeleter1<Quantizer> del (squant);
    memset(codes, 0, code_size * n);

#pragma omp parallel
    {
        std::vector<float> residual (d);

        // each thread takes care of a subset of lists
#pragma omp for
        for (size_t i = 0; i < n; i++) {
            long list_no = list_nos [i];
            if (list_no >= 0) {
                const float *xi = x + i * d;
                if (by_residual) {
                    quantizer->compute_residual (
                          xi, residual.data(), list_no);
                    xi = residual.data ();
                }
                squant->encode_vector (xi, codes + i * code_size);
            } else {
                memset (codes + i * code_size, 0, code_size);
            }
        }
    }
}



void IndexIVFScalarQuantizer::add_with_ids
       (idx_t n, const float * x, const long *xids)
{
    FAISS_THROW_IF_NOT (is_trained);
    long * idx = new long [n];
    ScopeDeleter<long> del (idx);
    quantizer->assign (n, x, idx);
    size_t nadd = 0;
    Quantizer *squant = select_quantizer (sq);
    ScopeDeleter1<Quantizer> del2 (squant);

#pragma omp parallel reduction(+: nadd)
    {
        std::vector<float> residual (d);
        std::vector<uint8_t> one_code (code_size);
        int nt = omp_get_num_threads();
        int rank = omp_get_thread_num();

        // each thread takes care of a subset of lists
        for (size_t i = 0; i < n; i++) {
            long list_no = idx [i];
            if (list_no >= 0 && list_no % nt == rank) {
                long id = xids ? xids[i] : ntotal + i;

                const float * xi = x + i * d;
                if (by_residual) {
                    quantizer->compute_residual (xi, residual.data(), list_no);
                    xi = residual.data();
                }

                memset (one_code.data(), 0, code_size);
                squant->encode_vector (xi, one_code.data());

                invlists->add_entry (list_no, id, one_code.data());

                nadd++;

            }
        }
    }
    ntotal += n;
}





InvertedListScanner* IndexIVFScalarQuantizer::get_InvertedListScanner
    (bool store_pairs) const
{
    return select_InvertedListScanner (metric_type, &sq, quantizer, store_pairs,
                                       by_residual);
}


void IndexIVFScalarQuantizer::reconstruct_from_offset (long list_no,
                                                       long offset,
                                                       float* recons) const
{
    std::vector<float> centroid(d);
    quantizer->reconstruct (list_no, centroid.data());

    const uint8_t* code = invlists->get_single_code (list_no, offset);
    sq.decode (code, recons, 1);
    for (int i = 0; i < d; ++i) {
        recons[i] += centroid[i];
    }
}

} // namespace faiss
