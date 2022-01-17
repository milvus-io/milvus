/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

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

#ifdef __AVX__
#define USE_AVX
#endif


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


} // namespace faiss
