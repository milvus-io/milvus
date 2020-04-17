// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>
#include <cassert>
#include <chrono>
#include <immintrin.h>
#include <vector>

typedef float (*metric_func_ptr)(const float*, const float*, size_t);

constexpr int64_t DIM = 512;
constexpr int64_t NB = 10000;
constexpr int64_t NQ = 1;

void
GenerateData(const int64_t dim, const int64_t n, float* x) {
    for (int64_t i = 0; i < n; ++i) {
        for (int64_t j = 0; j < dim; ++j) {
            x[i * dim + j] = drand48();
        }
    }
}

void
TestMetricAlg(const std::string& header, metric_func_ptr func_ptr, float* distance,
              const int64_t nb, const float* xb, const int64_t nq, const float* xq, const int64_t dim) {
    auto t0 = std::chrono::system_clock::now();
    for (int64_t i = 0; i < nb; i++) {
        for (int64_t j = 0; j < nq; j++) {
            distance[i * NQ + j] = func_ptr(xb + i * dim, xq + j * dim, dim);
        }
    }
    auto t1 = std::chrono::system_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    std::cout << header << " takes " << diff << "ms" << std::endl;
}

/* from faiss/utils/distances_simd.cpp */
namespace FAISS {

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

static inline __m256 masked_read_8 (int d, const float *x) {
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

float fvec_inner_product_avx (const float * x, const float * y, size_t d) {
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

float fvec_L2sqr_avx (const float * x, const float * y, size_t d) {
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

}  // namespace FAISS

TEST(METRICTEST, BENCHMARK) {
    std::vector<float> xb(NB * DIM);
    std::vector<float> xq(NQ * DIM);
    GenerateData(DIM, NB, xb.data());
    GenerateData(DIM, NQ, xq.data());

    std::vector<float> distance(NB * NQ);

    TestMetricAlg("FAISS::L2", FAISS::fvec_L2sqr_avx, distance.data(), NB, xb.data(), NQ, xq.data(), DIM);
    TestMetricAlg("FAISS::IP", FAISS::fvec_inner_product_avx, distance.data(), NB, xb.data(), NQ, xq.data(), DIM);
}
