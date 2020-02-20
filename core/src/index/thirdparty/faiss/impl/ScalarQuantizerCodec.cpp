/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/impl/ScalarQuantizerCodec.h>

#include <cstdio>
#include <algorithm>

#include <omp.h>

#ifdef __SSE__
#include <immintrin.h>
#endif

#include <faiss/utils/utils.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/Similarity.h>

namespace faiss {

#ifdef __AVX__
#define USE_AVX
#endif


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

struct Codec6bit {

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
Quantizer *select_quantizer_1 (
          QuantizerType qtype,
          size_t d, const std::vector<float> & trained)
{
    switch(qtype) {
    case QuantizerType::QT_8bit:
        return new QuantizerTemplate<Codec8bit, false, SIMDWIDTH>(d, trained);
    case QuantizerType::QT_6bit:
        return new QuantizerTemplate<Codec6bit, false, SIMDWIDTH>(d, trained);
    case QuantizerType::QT_4bit:
        return new QuantizerTemplate<Codec4bit, false, SIMDWIDTH>(d, trained);
    case QuantizerType::QT_8bit_uniform:
        return new QuantizerTemplate<Codec8bit, true, SIMDWIDTH>(d, trained);
    case QuantizerType::QT_4bit_uniform:
        return new QuantizerTemplate<Codec4bit, true, SIMDWIDTH>(d, trained);
    case QuantizerType::QT_fp16:
        return new QuantizerFP16<SIMDWIDTH> (d, trained);
    case QuantizerType::QT_8bit_direct:
        return new Quantizer8bitDirect<SIMDWIDTH> (d, trained);
    }
    FAISS_THROW_MSG ("unknown qtype");
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

    if (rs == RangeStat::RS_minmax) {
        vmin = HUGE_VAL; vmax = -HUGE_VAL;
        for (size_t i = 0; i < n; i++) {
            if (x[i] < vmin) vmin = x[i];
            if (x[i] > vmax) vmax = x[i];
        }
        float vexp = (vmax - vmin) * rs_arg;
        vmin -= vexp;
        vmax += vexp;
    } else if (rs == RangeStat::RS_meanstd) {
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
    } else if (rs == RangeStat::RS_quantiles) {
        std::vector<float> x_copy(n);
        memcpy(x_copy.data(), x, n * sizeof(*x));
        // TODO just do a qucikselect
        std::sort(x_copy.begin(), x_copy.end());
        int o = int(rs_arg * n);
        if (o < 0) o = 0;
        if (o > n - o) o = n / 2;
        vmin = x_copy[o];
        vmax = x_copy[n - 1 - o];

    } else if (rs == RangeStat::RS_optim) {
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
    if (rs == RangeStat::RS_minmax) {
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
    case QuantizerType::QT_8bit_uniform:
        return new DCTemplate<QuantizerTemplate<Codec8bit, true, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case QuantizerType::QT_4bit_uniform:
        return new DCTemplate<QuantizerTemplate<Codec4bit, true, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case QuantizerType::QT_8bit:
        return new DCTemplate<QuantizerTemplate<Codec8bit, false, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case QuantizerType::QT_6bit:
        return new DCTemplate<QuantizerTemplate<Codec6bit, false, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case QuantizerType::QT_4bit:
        return new DCTemplate<QuantizerTemplate<Codec4bit, false, SIMDWIDTH>,
                              Sim, SIMDWIDTH>(d, trained);

    case QuantizerType::QT_fp16:
        return new DCTemplate
            <QuantizerFP16<SIMDWIDTH>, Sim, SIMDWIDTH>(d, trained);

    case QuantizerType::QT_8bit_direct:
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


template<class DCClass>
struct IVFSQScannerIP: InvertedListScanner {
    DCClass dc;
    bool store_pairs, by_residual;

    size_t code_size;

    idx_t list_no;  /// current list (set to 0 for Flat index
    float accu0;    /// added to all distances

    IVFSQScannerIP(int d, const std::vector<float> & trained,
                   size_t code_size, bool store_pairs,
                   bool by_residual):
        dc(d, trained), store_pairs(store_pairs),
        by_residual(by_residual),
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
                int64_t id = store_pairs ? (list_no << 32 | j) : ids[j];
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
                int64_t id = store_pairs ? (list_no << 32 | j) : ids[j];
                res.add (accu, id);
            }
            codes += code_size;
        }
    }


};


template<class DCClass>
struct IVFSQScannerL2: InvertedListScanner {

    DCClass dc;

    bool store_pairs, by_residual;
    size_t code_size;
    const Index *quantizer;
    idx_t list_no;    /// current inverted list
    const float *x;   /// current query

    std::vector<float> tmp;

    IVFSQScannerL2(int d, const std::vector<float> & trained,
                   size_t code_size, const Index *quantizer,
                   bool store_pairs, bool by_residual):
        dc(d, trained), store_pairs(store_pairs), by_residual(by_residual),
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
            quantizer->Index::compute_residual (x, tmp.data(), list_no);
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
                int64_t id = store_pairs ? (list_no << 32 | j) : ids[j];
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
                int64_t id = store_pairs ? (list_no << 32 | j) : ids[j];
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
        return new IVFSQScannerL2<DCClass>(sq->d, sq->trained, sq->code_size,
                                           quantizer, store_pairs, r);
    } else if (DCClass::Sim::metric_type == METRIC_INNER_PRODUCT) {
        return new IVFSQScannerIP<DCClass>(sq->d, sq->trained, sq->code_size,
                                           store_pairs, r);
    } else {
        FAISS_THROW_MSG("unsupported metric type");
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
    case QuantizerType::QT_8bit_uniform:
        return sel12_InvertedListScanner
            <Similarity, Codec8bit, true>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_4bit_uniform:
        return sel12_InvertedListScanner
            <Similarity, Codec4bit, true>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_8bit:
        return sel12_InvertedListScanner
            <Similarity, Codec8bit, false>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_4bit:
        return sel12_InvertedListScanner
            <Similarity, Codec4bit, false>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_6bit:
        return sel12_InvertedListScanner
            <Similarity, Codec6bit, false>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_fp16:
        return sel2_InvertedListScanner
            <DCTemplate<QuantizerFP16<SIMDWIDTH>, Similarity, SIMDWIDTH> >
            (sq, quantizer, store_pairs, r);
    case QuantizerType::QT_8bit_direct:
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
    } else if (mt == METRIC_INNER_PRODUCT) {
        return sel1_InvertedListScanner<SimilarityIP<SIMDWIDTH> >
            (sq, quantizer, store_pairs, by_residual);
    } else {
        FAISS_THROW_MSG("unsupported metric type");
    }
}


} // namespace faiss
