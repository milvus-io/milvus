// Copyright (C) 2019-2023 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "avx512.h"
#include <cassert>

#if defined(__x86_64__)
#include <immintrin.h>

namespace milvus {
namespace simd {

template <>
bool
FindTermAVX512(const bool* src, size_t vec_size, bool val) {
    __m512i zmm_target = _mm512_set1_epi8(val);
    __m512i zmm_data;
    size_t num_chunks = vec_size / 64;

    for (size_t i = 0; i < 64 * num_chunks; i += 64) {
        zmm_data =
            _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + i));
        __mmask64 mask = _mm512_cmpeq_epi8_mask(zmm_data, zmm_target);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 64 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermAVX512(const int8_t* src, size_t vec_size, int8_t val) {
    __m512i zmm_target = _mm512_set1_epi8(val);
    __m512i zmm_data;
    size_t num_chunks = vec_size / 64;

    for (size_t i = 0; i < 64 * num_chunks; i += 64) {
        zmm_data =
            _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + i));
        __mmask64 mask = _mm512_cmpeq_epi8_mask(zmm_data, zmm_target);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 64 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermAVX512(const int16_t* src, size_t vec_size, int16_t val) {
    __m512i zmm_target = _mm512_set1_epi16(val);
    __m512i zmm_data;
    size_t num_chunks = vec_size / 32;

    for (size_t i = 0; i < 32 * num_chunks; i += 32) {
        zmm_data =
            _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + i));
        __mmask32 mask = _mm512_cmpeq_epi16_mask(zmm_data, zmm_target);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 32 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermAVX512(const int32_t* src, size_t vec_size, int32_t val) {
    __m512i zmm_target = _mm512_set1_epi32(val);
    __m512i zmm_data;
    size_t num_chunks = vec_size / 16;

    for (size_t i = 0; i < 16 * num_chunks; i += 16) {
        zmm_data =
            _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + i));
        __mmask16 mask = _mm512_cmpeq_epi32_mask(zmm_data, zmm_target);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 16 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermAVX512(const int64_t* src, size_t vec_size, int64_t val) {
    __m512i zmm_target = _mm512_set1_epi64(val);
    __m512i zmm_data;
    size_t num_chunks = vec_size / 8;

    for (size_t i = 0; i < 8 * num_chunks; i += 8) {
        zmm_data =
            _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + i));
        __mmask8 mask = _mm512_cmpeq_epi64_mask(zmm_data, zmm_target);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 8 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermAVX512(const float* src, size_t vec_size, float val) {
    __m512 zmm_target = _mm512_set1_ps(val);
    __m512 zmm_data;
    size_t num_chunks = vec_size / 16;

    for (size_t i = 0; i < 16 * num_chunks; i += 16) {
        zmm_data = _mm512_loadu_ps(src + i);
        __mmask16 mask = _mm512_cmp_ps_mask(zmm_data, zmm_target, _CMP_EQ_OQ);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 16 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermAVX512(const double* src, size_t vec_size, double val) {
    __m512d zmm_target = _mm512_set1_pd(val);
    __m512d zmm_data;
    size_t num_chunks = vec_size / 8;

    for (size_t i = 0; i < 8 * num_chunks; i += 8) {
        zmm_data = _mm512_loadu_pd(src + i);
        __mmask8 mask = _mm512_cmp_pd_mask(zmm_data, zmm_target, _CMP_EQ_OQ);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 8 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

enum class CompareType { EQ = 0, NEQ, LT, GT, LE, GE };

template <typename T, CompareType type>
struct CompareOperator;

template <typename T>
struct CompareOperator<T, CompareType::EQ> {
    static constexpr int ComparePredicate =
        std::is_floating_point_v<T> ? _CMP_EQ_OQ : _MM_CMPINT_EQ;
    static constexpr bool
    Op(T a, T b) {
        return a == b;
    }
};

template <typename T>
struct CompareOperator<T, CompareType::NEQ> {
    static constexpr int ComparePredicate =
        std::is_floating_point_v<T> ? _CMP_NEQ_OQ : _MM_CMPINT_NE;
    static constexpr bool
    Op(T a, T b) {
        return a != b;
    }
};

template <typename T>
struct CompareOperator<T, CompareType::LT> {
    static constexpr int ComparePredicate =
        std::is_floating_point_v<T> ? _CMP_LT_OQ : _MM_CMPINT_LT;
    static constexpr bool
    Op(T a, T b) {
        return a < b;
    }
};

template <typename T>
struct CompareOperator<T, CompareType::LE> {
    static constexpr int ComparePredicate =
        std::is_floating_point_v<T> ? _CMP_LE_OQ : _MM_CMPINT_LE;
    static constexpr bool
    Op(T a, T b) {
        return a <= b;
    }
};

template <typename T>
struct CompareOperator<T, CompareType::GT> {
    static constexpr int ComparePredicate =
        std::is_floating_point_v<T> ? _CMP_GT_OQ : _MM_CMPINT_NLE;
    static constexpr bool
    Op(T a, T b) {
        return a > b;
    }
};

template <typename T>
struct CompareOperator<T, CompareType::GE> {
    static constexpr int ComparePredicate =
        std::is_floating_point_v<T> ? _CMP_GE_OQ : _MM_CMPINT_NLT;
    static constexpr bool
    Op(T a, T b) {
        return a >= b;
    }
};

template <typename T, CompareType type>
struct CompareValAVX512Impl {
    static void
    Compare(const T* src, size_t size, T val, bool* res) {
        static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                      "T must be integral or float/double type");
    }
};

template <CompareType type>
struct CompareValAVX512Impl<int8_t, type> {
    static void
    Compare(const int8_t* src, size_t size, int8_t val, bool* res) {
        __m512i target = _mm512_set1_epi8(val);

        int middle = size / 64 * 64;

        for (size_t i = 0; i < middle; i += 64) {
            __m512i data =
                _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + i));

            __mmask64 cmp_res_mask = _mm512_cmp_epi8_mask(
                data, target, CompareOperator<int8_t, type>::ComparePredicate);
            __m512i cmp_res = _mm512_maskz_set1_epi8(cmp_res_mask, 0x01);
            _mm512_storeu_si512(res + i, cmp_res);
        }

        for (size_t i = middle; i < size; ++i) {
            res[i] = CompareOperator<int8_t, type>::Op(src[i], val);
        }
    }
};

template <CompareType type>
struct CompareValAVX512Impl<int16_t, type> {
    static void
    Compare(const int16_t* src, size_t size, int16_t val, bool* res) {
        __m512i target = _mm512_set1_epi16(val);

        int middle = size / 32 * 32;

        for (size_t i = 0; i < middle; i += 32) {
            __m512i data =
                _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + i));

            __mmask32 cmp_res_mask = _mm512_cmp_epi16_mask(
                data, target, CompareOperator<int16_t, type>::ComparePredicate);
            __m256i cmp_res = _mm256_maskz_set1_epi8(cmp_res_mask, 0x01);
            _mm256_storeu_si256((__m256i*)(res + i), cmp_res);
        }

        for (size_t i = middle; i < size; ++i) {
            res[i] = CompareOperator<int16_t, type>::Op(src[i], val);
        }
    }
};

template <CompareType type>
struct CompareValAVX512Impl<int32_t, type> {
    static void
    Compare(const int32_t* src, size_t size, int32_t val, bool* res) {
        __m512i target = _mm512_set1_epi32(val);

        int middle = size / 16 * 16;

        for (size_t i = 0; i < middle; i += 16) {
            __m512i data =
                _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + i));

            __mmask16 cmp_res_mask = _mm512_cmp_epi32_mask(
                data, target, CompareOperator<int32_t, type>::ComparePredicate);
            __m128i cmp_res = _mm_maskz_set1_epi8(cmp_res_mask, 0x01);
            _mm_storeu_si128((__m128i*)(res + i), cmp_res);
        }

        for (size_t i = middle; i < size; ++i) {
            res[i] = CompareOperator<int32_t, type>::Op(src[i], val);
        }
    }
};

template <CompareType type>
struct CompareValAVX512Impl<int64_t, type> {
    static void
    Compare(const int64_t* src, size_t size, int64_t val, bool* res) {
        __m512i target = _mm512_set1_epi64(val);
        int middle = size / 8 * 8;
        int index = 0;
        for (size_t i = 0; i < middle; i += 8) {
            __m512i data =
                _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + i));
            __mmask8 mask = _mm512_cmp_epi64_mask(
                data, target, CompareOperator<int64_t, type>::ComparePredicate);
            __m128i cmp_res = _mm_maskz_set1_epi8(mask, 0x01);
            _mm_storeu_si64((__m128i*)(res + i), cmp_res);
        }

        for (size_t i = middle; i < size; ++i) {
            res[i] = CompareOperator<int64_t, type>::Op(src[i], val);
        }
    }
};

template <CompareType type>
struct CompareValAVX512Impl<float, type> {
    static void
    Compare(const float* src, size_t size, float val, bool* res) {
        __m512 target = _mm512_set1_ps(val);

        int middle = size / 16 * 16;

        for (size_t i = 0; i < middle; i += 16) {
            __m512 data = _mm512_loadu_ps(src + i);

            __mmask16 cmp_res_mask = _mm512_cmp_ps_mask(
                data, target, CompareOperator<float, type>::ComparePredicate);
            __m128i cmp_res = _mm_maskz_set1_epi8(cmp_res_mask, 0x01);
            _mm_storeu_si128((__m128i*)(res + i), cmp_res);
        }

        for (size_t i = middle; i < size; ++i) {
            res[i] = CompareOperator<float, type>::Op(src[i], val);
        }
    }
};

template <CompareType type>
struct CompareValAVX512Impl<double, type> {
    static void
    Compare(const double* src, size_t size, double val, bool* res) {
        __m512d target = _mm512_set1_pd(val);

        int middle = size / 8 * 8;

        for (size_t i = 0; i < middle; i += 8) {
            __m512d data = _mm512_loadu_pd(src + i);

            __mmask8 cmp_res_mask = _mm512_cmp_pd_mask(
                data, target, CompareOperator<double, type>::ComparePredicate);
            __m128i cmp_res = _mm_maskz_set1_epi8(cmp_res_mask, 0x01);
            _mm_storeu_si64((res + i), cmp_res);
        }

        for (size_t i = middle; i < size; ++i) {
            res[i] = CompareOperator<double, type>::Op(src[i], val);
        }
    }
};

template <typename T>
void
EqualValAVX512(const T* src, size_t size, T val, bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
    CompareValAVX512Impl<T, CompareType::EQ>::Compare(src, size, val, res);
};
template void
EqualValAVX512(const int8_t* src, size_t size, int8_t val, bool* res);
template void
EqualValAVX512(const int16_t* src, size_t size, int16_t val, bool* res);
template void
EqualValAVX512(const int32_t* src, size_t size, int32_t val, bool* res);
template void
EqualValAVX512(const int64_t* src, size_t size, int64_t val, bool* res);
template void
EqualValAVX512(const float* src, size_t size, float val, bool* res);
template void
EqualValAVX512(const double* src, size_t size, double val, bool* res);

template <typename T>
void
LessValAVX512(const T* src, size_t size, T val, bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
    CompareValAVX512Impl<T, CompareType::LT>::Compare(src, size, val, res);
};
template void
LessValAVX512(const int8_t* src, size_t size, int8_t val, bool* res);
template void
LessValAVX512(const int16_t* src, size_t size, int16_t val, bool* res);
template void
LessValAVX512(const int32_t* src, size_t size, int32_t val, bool* res);
template void
LessValAVX512(const int64_t* src, size_t size, int64_t val, bool* res);
template void
LessValAVX512(const float* src, size_t size, float val, bool* res);
template void
LessValAVX512(const double* src, size_t size, double val, bool* res);

template <typename T>
void
GreaterValAVX512(const T* src, size_t size, T val, bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
    CompareValAVX512Impl<T, CompareType::GT>::Compare(src, size, val, res);
};
template void
GreaterValAVX512(const int8_t* src, size_t size, int8_t val, bool* res);
template void
GreaterValAVX512(const int16_t* src, size_t size, int16_t val, bool* res);
template void
GreaterValAVX512(const int32_t* src, size_t size, int32_t val, bool* res);
template void
GreaterValAVX512(const int64_t* src, size_t size, int64_t val, bool* res);
template void
GreaterValAVX512(const float* src, size_t size, float val, bool* res);
template void
GreaterValAVX512(const double* src, size_t size, double val, bool* res);

template <typename T>
void
NotEqualValAVX512(const T* src, size_t size, T val, bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
    CompareValAVX512Impl<T, CompareType::NEQ>::Compare(src, size, val, res);
};
template void
NotEqualValAVX512(const int8_t* src, size_t size, int8_t val, bool* res);
template void
NotEqualValAVX512(const int16_t* src, size_t size, int16_t val, bool* res);
template void
NotEqualValAVX512(const int32_t* src, size_t size, int32_t val, bool* res);
template void
NotEqualValAVX512(const int64_t* src, size_t size, int64_t val, bool* res);
template void
NotEqualValAVX512(const float* src, size_t size, float val, bool* res);
template void
NotEqualValAVX512(const double* src, size_t size, double val, bool* res);

template <typename T>
void
LessEqualValAVX512(const T* src, size_t size, T val, bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
    CompareValAVX512Impl<T, CompareType::LE>::Compare(src, size, val, res);
};
template void
LessEqualValAVX512(const int8_t* src, size_t size, int8_t val, bool* res);
template void
LessEqualValAVX512(const int16_t* src, size_t size, int16_t val, bool* res);
template void
LessEqualValAVX512(const int32_t* src, size_t size, int32_t val, bool* res);
template void
LessEqualValAVX512(const int64_t* src, size_t size, int64_t val, bool* res);
template void
LessEqualValAVX512(const float* src, size_t size, float val, bool* res);
template void
LessEqualValAVX512(const double* src, size_t size, double val, bool* res);

template <typename T>
void
GreaterEqualValAVX512(const T* src, size_t size, T val, bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
    CompareValAVX512Impl<T, CompareType::GE>::Compare(src, size, val, res);
};
template void
GreaterEqualValAVX512(const int8_t* src, size_t size, int8_t val, bool* res);
template void
GreaterEqualValAVX512(const int16_t* src, size_t size, int16_t val, bool* res);
template void
GreaterEqualValAVX512(const int32_t* src, size_t size, int32_t val, bool* res);
template void
GreaterEqualValAVX512(const int64_t* src, size_t size, int64_t val, bool* res);
template void
GreaterEqualValAVX512(const float* src, size_t size, float val, bool* res);
template void
GreaterEqualValAVX512(const double* src, size_t size, double val, bool* res);

template <typename T>
void
CompareColumnAVX512(const T* left, const T* right, size_t size, bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
}

template <typename T, CompareType type>
struct CompareColumnAVX512Impl {
    static void
    Compare(const T* left, const T* right, size_t size, bool* res) {
        static_assert(std::is_integral_v<T>, "T must be integral type");

        int batch_size = 512 / (sizeof(T) * 8);
        int middle = size / batch_size * batch_size;

        for (size_t i = 0; i < middle; i += batch_size) {
            __m512i left_reg =
                _mm512_loadu_si512(reinterpret_cast<const __m512i*>(left + i));
            __m512i right_reg =
                _mm512_loadu_si512(reinterpret_cast<const __m512i*>(right + i));

            if constexpr (std::is_same_v<T, int8_t>) {
                __mmask64 cmp_res_mask = _mm512_cmp_epi8_mask(
                    left_reg,
                    right_reg,
                    CompareOperator<T, type>::ComparePredicate);

                __m512i cmp_res = _mm512_maskz_set1_epi8(cmp_res_mask, 0x01);
                _mm512_storeu_si512(res + i, cmp_res);
            } else if constexpr (std::is_same_v<T, int16_t>) {
                __mmask32 cmp_res_mask = _mm512_cmp_epi16_mask(
                    left_reg,
                    right_reg,
                    CompareOperator<T, type>::ComparePredicate);

                __m256i cmp_res = _mm256_maskz_set1_epi8(cmp_res_mask, 0x01);
                _mm256_storeu_si256((__m256i*)(res + i), cmp_res);
            } else if constexpr (std::is_same_v<T, int32_t>) {
                __mmask16 cmp_res_mask = _mm512_cmp_epi32_mask(
                    left_reg,
                    right_reg,
                    CompareOperator<T, type>::ComparePredicate);

                __m128i cmp_res = _mm_maskz_set1_epi8(cmp_res_mask, 0x01);
                _mm_storeu_si128((__m128i*)(res + i), cmp_res);
            } else if constexpr (std::is_same_v<T, int64_t>) {
                __mmask8 mask = _mm512_cmp_epi64_mask(
                    left_reg,
                    right_reg,
                    CompareOperator<T, type>::ComparePredicate);

                __m128i cmp_res = _mm_maskz_set1_epi8(mask, 0x01);
                _mm_storeu_si64((__m128i*)(res + i), cmp_res);
            }
        }

        for (size_t i = middle; i < size; ++i) {
            res[i] = CompareOperator<T, type>::Op(left[i], right[i]);
        }
    }
};

template <CompareType type>
struct CompareColumnAVX512Impl<float, type> {
    static void
    Compare(const float* left, const float* right, size_t size, bool* res) {
        int batch_size = 512 / (sizeof(float) * 8);
        int middle = size / batch_size * batch_size;

        for (size_t i = 0; i < middle; i += batch_size) {
            __m512 left_reg =
                _mm512_loadu_ps(reinterpret_cast<const __m512*>(left + i));
            __m512 right_reg =
                _mm512_loadu_ps(reinterpret_cast<const __m512*>(right + i));

            __mmask16 cmp_res_mask = _mm512_cmp_ps_mask(
                left_reg,
                right_reg,
                CompareOperator<float, type>::ComparePredicate);

            __m128i cmp_res = _mm_maskz_set1_epi8(cmp_res_mask, 0x01);
            _mm_storeu_si128((__m128i*)(res + i), cmp_res);
        }

        for (size_t i = middle; i < size; ++i) {
            res[i] = CompareOperator<float, type>::Op(left[i], right[i]);
        }
    }
};

template <CompareType type>
struct CompareColumnAVX512Impl<double, type> {
    static void
    Compare(const double* left, const double* right, size_t size, bool* res) {
        int batch_size = 512 / (sizeof(double) * 8);
        int middle = size / batch_size * batch_size;

        for (size_t i = 0; i < middle; i += batch_size) {
            __m512d left_reg =
                _mm512_loadu_pd(reinterpret_cast<const __m512d*>(left + i));
            __m512d right_reg =
                _mm512_loadu_pd(reinterpret_cast<const __m512d*>(right + i));

            __mmask8 cmp_res_mask = _mm512_cmp_pd_mask(
                left_reg,
                right_reg,
                CompareOperator<double, type>::ComparePredicate);

            __m128i cmp_res = _mm_maskz_set1_epi8(cmp_res_mask, 0x01);
            _mm_storeu_si64((res + i), cmp_res);
        }

        for (size_t i = middle; i < size; ++i) {
            res[i] = CompareOperator<double, type>::Op(left[i], right[i]);
        }
    }
};

template <typename T>
void
EqualColumnAVX512(const T* left, const T* right, size_t size, bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
    CompareColumnAVX512Impl<T, CompareType::EQ>::Compare(
        left, right, size, res);
};

template void
EqualColumnAVX512(const int8_t* left,
                  const int8_t* right,
                  size_t size,
                  bool* res);
template void
EqualColumnAVX512(const int16_t* left,
                  const int16_t* right,
                  size_t size,
                  bool* res);
template void
EqualColumnAVX512(const int32_t* left,
                  const int32_t* right,
                  size_t size,
                  bool* res);
template void
EqualColumnAVX512(const int64_t* left,
                  const int64_t* right,
                  size_t size,
                  bool* res);
template void
EqualColumnAVX512(const float* left,
                  const float* right,
                  size_t size,
                  bool* res);
template void
EqualColumnAVX512(const double* left,
                  const double* right,
                  size_t size,
                  bool* res);

template <typename T>
void
LessColumnAVX512(const T* left, const T* right, size_t size, bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
    CompareColumnAVX512Impl<T, CompareType::LT>::Compare(
        left, right, size, res);
};
template void
LessColumnAVX512(const int8_t* left,
                 const int8_t* right,
                 size_t size,
                 bool* res);
template void
LessColumnAVX512(const int16_t* left,
                 const int16_t* right,
                 size_t size,
                 bool* res);
template void
LessColumnAVX512(const int32_t* left,
                 const int32_t* right,
                 size_t size,
                 bool* res);
template void
LessColumnAVX512(const int64_t* left,
                 const int64_t* right,
                 size_t size,
                 bool* res);
template void
LessColumnAVX512(const float* left, const float* right, size_t size, bool* res);
template void
LessColumnAVX512(const double* left,
                 const double* right,
                 size_t size,
                 bool* res);

template <typename T>
void
GreaterColumnAVX512(const T* left, const T* right, size_t size, bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
    CompareColumnAVX512Impl<T, CompareType::GT>::Compare(
        left, right, size, res);
};
template void
GreaterColumnAVX512(const int8_t* left,
                    const int8_t* right,
                    size_t size,
                    bool* res);
template void
GreaterColumnAVX512(const int16_t* left,
                    const int16_t* right,
                    size_t size,
                    bool* res);
template void
GreaterColumnAVX512(const int32_t* left,
                    const int32_t* right,
                    size_t size,
                    bool* res);
template void
GreaterColumnAVX512(const int64_t* left,
                    const int64_t* right,
                    size_t size,
                    bool* res);
template void
GreaterColumnAVX512(const float* left,
                    const float* right,
                    size_t size,
                    bool* res);
template void
GreaterColumnAVX512(const double* left,
                    const double* right,
                    size_t size,
                    bool* res);

template <typename T>
void
LessEqualColumnAVX512(const T* left, const T* right, size_t size, bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
    CompareColumnAVX512Impl<T, CompareType::LE>::Compare(
        left, right, size, res);
};
template void
LessEqualColumnAVX512(const int8_t* left,
                      const int8_t* right,
                      size_t size,
                      bool* res);
template void
LessEqualColumnAVX512(const int16_t* left,
                      const int16_t* right,
                      size_t size,
                      bool* res);
template void
LessEqualColumnAVX512(const int32_t* left,
                      const int32_t* right,
                      size_t size,
                      bool* res);
template void
LessEqualColumnAVX512(const int64_t* left,
                      const int64_t* right,
                      size_t size,
                      bool* res);
template void
LessEqualColumnAVX512(const float* left,
                      const float* right,
                      size_t size,
                      bool* res);
template void
LessEqualColumnAVX512(const double* left,
                      const double* right,
                      size_t size,
                      bool* res);

template <typename T>
void
GreaterEqualColumnAVX512(const T* left,
                         const T* right,
                         size_t size,
                         bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
    CompareColumnAVX512Impl<T, CompareType::GE>::Compare(
        left, right, size, res);
};
template void
GreaterEqualColumnAVX512(const int8_t* left,
                         const int8_t* right,
                         size_t size,
                         bool* res);
template void
GreaterEqualColumnAVX512(const int16_t* left,
                         const int16_t* right,
                         size_t size,
                         bool* res);
template void
GreaterEqualColumnAVX512(const int32_t* left,
                         const int32_t* right,
                         size_t size,
                         bool* res);
template void
GreaterEqualColumnAVX512(const int64_t* left,
                         const int64_t* right,
                         size_t size,
                         bool* res);
template void
GreaterEqualColumnAVX512(const float* left,
                         const float* right,
                         size_t size,
                         bool* res);
template void
GreaterEqualColumnAVX512(const double* left,
                         const double* right,
                         size_t size,
                         bool* res);

template <typename T>
void
NotEqualColumnAVX512(const T* left, const T* right, size_t size, bool* res) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                  "T must be integral or float/double type");
    CompareColumnAVX512Impl<T, CompareType::NEQ>::Compare(
        left, right, size, res);
};
template void
NotEqualColumnAVX512(const int8_t* left,
                     const int8_t* right,
                     size_t size,
                     bool* res);
template void
NotEqualColumnAVX512(const int16_t* left,
                     const int16_t* right,
                     size_t size,
                     bool* res);
template void
NotEqualColumnAVX512(const int32_t* left,
                     const int32_t* right,
                     size_t size,
                     bool* res);
template void
NotEqualColumnAVX512(const int64_t* left,
                     const int64_t* right,
                     size_t size,
                     bool* res);
template void
NotEqualColumnAVX512(const float* left,
                     const float* right,
                     size_t size,
                     bool* res);
template void
NotEqualColumnAVX512(const double* left,
                     const double* right,
                     size_t size,
                     bool* res);

}  // namespace simd
}  // namespace milvus
#endif
