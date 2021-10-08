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
#include <immintrin.h>
#include <cassert>
#include <chrono>
#include <unordered_map>
#include <vector>

typedef float (*metric_func_ptr)(const float*, const float*, size_t);

constexpr int64_t DIM = 512;
constexpr int64_t NB = 10000;
constexpr int64_t NQ = 5;
constexpr int64_t LOOP = 5;

void
GenerateData(const int64_t dim, const int64_t n, float* x) {
    for (int64_t i = 0; i < n; ++i) {
        for (int64_t j = 0; j < dim; ++j) {
            x[i * dim + j] = drand48();
        }
    }
}

void
TestMetricAlg(std::unordered_map<std::string, metric_func_ptr>& func_map,
              const std::string& key,
              int64_t loop,
              float* distance,
              const int64_t nb,
              const float* xb,
              const int64_t nq,
              const float* xq,
              const int64_t dim) {
    int64_t diff = 0;
    for (int64_t i = 0; i < loop; i++) {
        auto t0 = std::chrono::system_clock::now();
        for (int64_t i = 0; i < nb; i++) {
            for (int64_t j = 0; j < nq; j++) {
                distance[i * NQ + j] = func_map[key](xb + i * dim, xq + j * dim, dim);
            }
        }
        auto t1 = std::chrono::system_clock::now();
        diff += std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    }
    std::cout << key << " takes average " << diff / loop << "ms" << std::endl;
}

void
CheckResult(const float* result1, const float* result2, const size_t size) {
    for (size_t i = 0; i < size; i++) {
        ASSERT_FLOAT_EQ(result1[i], result2[i]);
    }
}

///////////////////////////////////////////////////////////////////////////////
/* from faiss/utils/distances_simd.cpp */
namespace FAISS {
// reads 0 <= d < 4 floats as __m128
static inline __m128
masked_read(int d, const float* x) {
    assert(0 <= d && d < 4);
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

static inline __m256
masked_read_8(int d, const float* x) {
    assert(0 <= d && d < 8);
    if (d < 4) {
        __m256 res = _mm256_setzero_ps();
        res = _mm256_insertf128_ps(res, masked_read(d, x), 0);
        return res;
    } else {
        __m256 res = _mm256_setzero_ps();
        res = _mm256_insertf128_ps(res, _mm_loadu_ps(x), 0);
        res = _mm256_insertf128_ps(res, masked_read(d - 4, x + 4), 1);
        return res;
    }
}

float
fvec_inner_product_avx(const float* x, const float* y, size_t d) {
    __m256 msum1 = _mm256_setzero_ps();

    while (d >= 8) {
        __m256 mx = _mm256_loadu_ps(x);
        x += 8;
        __m256 my = _mm256_loadu_ps(y);
        y += 8;
        msum1 = _mm256_add_ps(msum1, _mm256_mul_ps(mx, my));
        d -= 8;
    }

    __m128 msum2 = _mm256_extractf128_ps(msum1, 1);
    msum2 += _mm256_extractf128_ps(msum1, 0);

    if (d >= 4) {
        __m128 mx = _mm_loadu_ps(x);
        x += 4;
        __m128 my = _mm_loadu_ps(y);
        y += 4;
        msum2 = _mm_add_ps(msum2, _mm_mul_ps(mx, my));
        d -= 4;
    }

    if (d > 0) {
        __m128 mx = masked_read(d, x);
        __m128 my = masked_read(d, y);
        msum2 = _mm_add_ps(msum2, _mm_mul_ps(mx, my));
    }

    msum2 = _mm_hadd_ps(msum2, msum2);
    msum2 = _mm_hadd_ps(msum2, msum2);
    return _mm_cvtss_f32(msum2);
}

float
fvec_L2sqr_avx(const float* x, const float* y, size_t d) {
    __m256 msum1 = _mm256_setzero_ps();

    while (d >= 8) {
        __m256 mx = _mm256_loadu_ps(x);
        x += 8;
        __m256 my = _mm256_loadu_ps(y);
        y += 8;
        const __m256 a_m_b1 = mx - my;
        msum1 += a_m_b1 * a_m_b1;
        d -= 8;
    }

    __m128 msum2 = _mm256_extractf128_ps(msum1, 1);
    msum2 += _mm256_extractf128_ps(msum1, 0);

    if (d >= 4) {
        __m128 mx = _mm_loadu_ps(x);
        x += 4;
        __m128 my = _mm_loadu_ps(y);
        y += 4;
        const __m128 a_m_b1 = mx - my;
        msum2 += a_m_b1 * a_m_b1;
        d -= 4;
    }

    if (d > 0) {
        __m128 mx = masked_read(d, x);
        __m128 my = masked_read(d, y);
        __m128 a_m_b1 = mx - my;
        msum2 += a_m_b1 * a_m_b1;
    }

    msum2 = _mm_hadd_ps(msum2, msum2);
    msum2 = _mm_hadd_ps(msum2, msum2);
    return _mm_cvtss_f32(msum2);
}
}  // namespace FAISS

///////////////////////////////////////////////////////////////////////////////
/* from knowhere/index/vector_index/impl/nsg/Distance.cpp */
namespace NSG {
float
DistanceL2_Compare(const float* a, const float* b, size_t size) {
    float result = 0;

#define AVX_L2SQR(addr1, addr2, dest, tmp1, tmp2) \
    tmp1 = _mm256_loadu_ps(addr1);                \
    tmp2 = _mm256_loadu_ps(addr2);                \
    tmp1 = _mm256_sub_ps(tmp1, tmp2);             \
    tmp1 = _mm256_mul_ps(tmp1, tmp1);             \
    dest = _mm256_add_ps(dest, tmp1);

    __m256 sum;
    __m256 l0, l1;
    __m256 r0, r1;
    unsigned D = (size + 7) & ~7U;
    unsigned DR = D % 16;
    unsigned DD = D - DR;
    const float* l = a;
    const float* r = b;
    const float* e_l = l + DD;
    const float* e_r = r + DD;
    float unpack[8] __attribute__((aligned(32))) = {0, 0, 0, 0, 0, 0, 0, 0};

    sum = _mm256_loadu_ps(unpack);
    if (DR) {
        AVX_L2SQR(e_l, e_r, sum, l0, r0);
    }

    for (unsigned i = 0; i < DD; i += 16, l += 16, r += 16) {
        AVX_L2SQR(l, r, sum, l0, r0);
        AVX_L2SQR(l + 8, r + 8, sum, l1, r1);
    }
    _mm256_storeu_ps(unpack, sum);
    result = unpack[0] + unpack[1] + unpack[2] + unpack[3] + unpack[4] + unpack[5] + unpack[6] + unpack[7];

    return result;
}

float
DistanceIP_Compare(const float* a, const float* b, size_t size) {
    float result = 0;

#define AVX_DOT(addr1, addr2, dest, tmp1, tmp2) \
    tmp1 = _mm256_loadu_ps(addr1);              \
    tmp2 = _mm256_loadu_ps(addr2);              \
    tmp1 = _mm256_mul_ps(tmp1, tmp2);           \
    dest = _mm256_add_ps(dest, tmp1);

    __m256 sum;
    __m256 l0, l1;
    __m256 r0, r1;
    unsigned D = (size + 7) & ~7U;
    unsigned DR = D % 16;
    unsigned DD = D - DR;
    const float* l = a;
    const float* r = b;
    const float* e_l = l + DD;
    const float* e_r = r + DD;
    float unpack[8] __attribute__((aligned(32))) = {0, 0, 0, 0, 0, 0, 0, 0};

    sum = _mm256_loadu_ps(unpack);
    if (DR) {
        AVX_DOT(e_l, e_r, sum, l0, r0);
    }

    for (unsigned i = 0; i < DD; i += 16, l += 16, r += 16) {
        AVX_DOT(l, r, sum, l0, r0);
        AVX_DOT(l + 8, r + 8, sum, l1, r1);
    }
    _mm256_storeu_ps(unpack, sum);
    result = unpack[0] + unpack[1] + unpack[2] + unpack[3] + unpack[4] + unpack[5] + unpack[6] + unpack[7];

    return result;
}
}  // namespace NSG

///////////////////////////////////////////////////////////////////////////////
/* from index/thirdparty/annoy/src/annoylib.h */
namespace ANNOY {
inline float
hsum256_ps_avx(__m256 v) {
    const __m128 x128 = _mm_add_ps(_mm256_extractf128_ps(v, 1), _mm256_castps256_ps128(v));
    const __m128 x64 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128));
    const __m128 x32 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    return _mm_cvtss_f32(x32);
}

inline float
euclidean_distance(const float* x, const float* y, size_t f) {
    float result = 0;
    if (f > 7) {
        __m256 d = _mm256_setzero_ps();
        for (; f > 7; f -= 8) {
            const __m256 diff = _mm256_sub_ps(_mm256_loadu_ps(x), _mm256_loadu_ps(y));
            d = _mm256_add_ps(d, _mm256_mul_ps(diff, diff));  // no support for fmadd in AVX...
            x += 8;
            y += 8;
        }
        // Sum all floats in dot register.
        result = hsum256_ps_avx(d);
    }
    // Don't forget the remaining values.
    for (; f > 0; f--) {
        float tmp = *x - *y;
        result += tmp * tmp;
        x++;
        y++;
    }
    return result;
}

inline float
dot(const float* x, const float* y, size_t f) {
    float result = 0;
    if (f > 7) {
        __m256 d = _mm256_setzero_ps();
        for (; f > 7; f -= 8) {
            d = _mm256_add_ps(d, _mm256_mul_ps(_mm256_loadu_ps(x), _mm256_loadu_ps(y)));
            x += 8;
            y += 8;
        }
        // Sum all floats in dot register.
        result += hsum256_ps_avx(d);
    }
    // Don't forget the remaining values.
    for (; f > 0; f--) {
        result += *x * *y;
        x++;
        y++;
    }
    return result;
}
}  // namespace ANNOY

namespace HNSW {
#define PORTABLE_ALIGN32 __attribute__((aligned(32)))

static float
L2SqrSIMD16Ext(const float* pVect1v, const float* pVect2v, size_t qty) {
    float* pVect1 = (float*)pVect1v;
    float* pVect2 = (float*)pVect2v;
    // size_t qty = *((size_t *) qty_ptr);
    float PORTABLE_ALIGN32 TmpRes[8];
    size_t qty16 = qty >> 4;

    const float* pEnd1 = pVect1 + (qty16 << 4);

    __m256 diff, v1, v2;
    __m256 sum = _mm256_set1_ps(0);

    while (pVect1 < pEnd1) {
        v1 = _mm256_loadu_ps(pVect1);
        pVect1 += 8;
        v2 = _mm256_loadu_ps(pVect2);
        pVect2 += 8;
        diff = _mm256_sub_ps(v1, v2);
        sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));

        v1 = _mm256_loadu_ps(pVect1);
        pVect1 += 8;
        v2 = _mm256_loadu_ps(pVect2);
        pVect2 += 8;
        diff = _mm256_sub_ps(v1, v2);
        sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));
    }

    _mm256_store_ps(TmpRes, sum);
    float res = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3] + TmpRes[4] + TmpRes[5] + TmpRes[6] + TmpRes[7];

    return (res);
}

static float
InnerProductSIMD16Ext(const float* pVect1v, const float* pVect2v, size_t qty) {
    float PORTABLE_ALIGN32 TmpRes[8];
    float* pVect1 = (float*)pVect1v;
    float* pVect2 = (float*)pVect2v;
    // size_t qty = *((size_t *) qty_ptr);

    size_t qty16 = qty / 16;

    const float* pEnd1 = pVect1 + 16 * qty16;

    __m256 sum256 = _mm256_set1_ps(0);

    while (pVect1 < pEnd1) {
        //_mm_prefetch((char*)(pVect2 + 16), _MM_HINT_T0);

        __m256 v1 = _mm256_loadu_ps(pVect1);
        pVect1 += 8;
        __m256 v2 = _mm256_loadu_ps(pVect2);
        pVect2 += 8;
        sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v1, v2));

        v1 = _mm256_loadu_ps(pVect1);
        pVect1 += 8;
        v2 = _mm256_loadu_ps(pVect2);
        pVect2 += 8;
        sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v1, v2));
    }

    _mm256_store_ps(TmpRes, sum256);
    float sum = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3] + TmpRes[4] + TmpRes[5] + TmpRes[6] + TmpRes[7];

    return sum;
}
}  // namespace HNSW

TEST(METRICTEST, BENCHMARK) {
    std::unordered_map<std::string, metric_func_ptr> func_map;
    func_map["FAISS::L2"] = FAISS::fvec_L2sqr_avx;
    func_map["NSG::L2"] = NSG::DistanceL2_Compare;
    func_map["HNSW::L2"] = HNSW::L2SqrSIMD16Ext;
    func_map["ANNOY::L2"] = ANNOY::euclidean_distance;

    func_map["FAISS::IP"] = FAISS::fvec_inner_product_avx;
    func_map["NSG::IP"] = NSG::DistanceIP_Compare;
    func_map["HNSW::IP"] = HNSW::InnerProductSIMD16Ext;
    func_map["ANNOY::IP"] = ANNOY::dot;

    std::vector<float> xb(NB * DIM);
    std::vector<float> xq(NQ * DIM);
    GenerateData(DIM, NB, xb.data());
    GenerateData(DIM, NQ, xq.data());

    std::vector<float> distance_faiss(NB * NQ);
    // std::vector<float> distance_nsg(NB * NQ);
    std::vector<float> distance_annoy(NB * NQ);
    // std::vector<float> distance_hnsw(NB * NQ);

    std::cout << "==========" << std::endl;
    TestMetricAlg(func_map, "FAISS::L2", LOOP, distance_faiss.data(), NB, xb.data(), NQ, xq.data(), DIM);

    TestMetricAlg(func_map, "ANNOY::L2", LOOP, distance_annoy.data(), NB, xb.data(), NQ, xq.data(), DIM);
    CheckResult(distance_faiss.data(), distance_annoy.data(), NB * NQ);

    std::cout << "==========" << std::endl;
    TestMetricAlg(func_map, "FAISS::IP", LOOP, distance_faiss.data(), NB, xb.data(), NQ, xq.data(), DIM);

    TestMetricAlg(func_map, "ANNOY::IP", LOOP, distance_annoy.data(), NB, xb.data(), NQ, xq.data(), DIM);
    CheckResult(distance_faiss.data(), distance_annoy.data(), NB * NQ);
}
