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

#pragma once

#include <string>
#include <string_view>

#include "common.h"
namespace milvus {
namespace simd {

extern BitsetBlockType (*get_bitset_block)(const bool* src);

template <typename T>
using FindTermPtr = bool (*)(const T* src, size_t size, T val);
#define EXTERN_FIND_TERM_PTR(type) extern FindTermPtr<type> find_term_##type;

EXTERN_FIND_TERM_PTR(bool)
EXTERN_FIND_TERM_PTR(int8_t)
EXTERN_FIND_TERM_PTR(int16_t)
EXTERN_FIND_TERM_PTR(int32_t)
EXTERN_FIND_TERM_PTR(int64_t)
EXTERN_FIND_TERM_PTR(float)
EXTERN_FIND_TERM_PTR(double)

// Compare val function register
// Such as A == 10, A < 10...
template <typename T>
using CompareValPtr = void (*)(const T* src, size_t size, T val, bool* res);
#define EXTERN_COMPARE_VAL_PTR(prefix, type) \
    extern CompareValPtr<type> prefix##_##type;

// Compare column function register
// Such as A == B, A < B...
template <typename T>
using CompareColPtr =
    void (*)(const T* left, const T* right, size_t size, bool* res);
#define EXTERN_COMPARE_COL_PTR(prefix, type) \
    extern CompareColPtr<type> prefix##_##type;

EXTERN_COMPARE_VAL_PTR(equal_val, bool)
EXTERN_COMPARE_VAL_PTR(equal_val, int8_t)
EXTERN_COMPARE_VAL_PTR(equal_val, int16_t)
EXTERN_COMPARE_VAL_PTR(equal_val, int32_t)
EXTERN_COMPARE_VAL_PTR(equal_val, int64_t)
EXTERN_COMPARE_VAL_PTR(equal_val, float)
EXTERN_COMPARE_VAL_PTR(equal_val, double)

EXTERN_COMPARE_VAL_PTR(less_val, bool)
EXTERN_COMPARE_VAL_PTR(less_val, int8_t)
EXTERN_COMPARE_VAL_PTR(less_val, int16_t)
EXTERN_COMPARE_VAL_PTR(less_val, int32_t)
EXTERN_COMPARE_VAL_PTR(less_val, int64_t)
EXTERN_COMPARE_VAL_PTR(less_val, float)
EXTERN_COMPARE_VAL_PTR(less_val, double)

EXTERN_COMPARE_VAL_PTR(greater_val, bool)
EXTERN_COMPARE_VAL_PTR(greater_val, int8_t)
EXTERN_COMPARE_VAL_PTR(greater_val, int16_t)
EXTERN_COMPARE_VAL_PTR(greater_val, int32_t)
EXTERN_COMPARE_VAL_PTR(greater_val, int64_t)
EXTERN_COMPARE_VAL_PTR(greater_val, float)
EXTERN_COMPARE_VAL_PTR(greater_val, double)

EXTERN_COMPARE_VAL_PTR(less_equal_val, bool)
EXTERN_COMPARE_VAL_PTR(less_equal_val, int8_t)
EXTERN_COMPARE_VAL_PTR(less_equal_val, int16_t)
EXTERN_COMPARE_VAL_PTR(less_equal_val, int32_t)
EXTERN_COMPARE_VAL_PTR(less_equal_val, int64_t)
EXTERN_COMPARE_VAL_PTR(less_equal_val, float)
EXTERN_COMPARE_VAL_PTR(less_equal_val, double)

EXTERN_COMPARE_VAL_PTR(greater_equal_val, bool)
EXTERN_COMPARE_VAL_PTR(greater_equal_val, int8_t)
EXTERN_COMPARE_VAL_PTR(greater_equal_val, int16_t)
EXTERN_COMPARE_VAL_PTR(greater_equal_val, int32_t)
EXTERN_COMPARE_VAL_PTR(greater_equal_val, int64_t)
EXTERN_COMPARE_VAL_PTR(greater_equal_val, float)
EXTERN_COMPARE_VAL_PTR(greater_equal_val, double)

EXTERN_COMPARE_VAL_PTR(not_equal_val, bool)
EXTERN_COMPARE_VAL_PTR(not_equal_val, int8_t)
EXTERN_COMPARE_VAL_PTR(not_equal_val, int16_t)
EXTERN_COMPARE_VAL_PTR(not_equal_val, int32_t)
EXTERN_COMPARE_VAL_PTR(not_equal_val, int64_t)
EXTERN_COMPARE_VAL_PTR(not_equal_val, float)
EXTERN_COMPARE_VAL_PTR(not_equal_val, double)

EXTERN_COMPARE_COL_PTR(equal_col, bool)
EXTERN_COMPARE_COL_PTR(equal_col, int8_t)
EXTERN_COMPARE_COL_PTR(equal_col, int16_t)
EXTERN_COMPARE_COL_PTR(equal_col, int32_t)
EXTERN_COMPARE_COL_PTR(equal_col, int64_t)
EXTERN_COMPARE_COL_PTR(equal_col, float)
EXTERN_COMPARE_COL_PTR(equal_col, double)

EXTERN_COMPARE_COL_PTR(less_col, bool)
EXTERN_COMPARE_COL_PTR(less_col, int8_t)
EXTERN_COMPARE_COL_PTR(less_col, int16_t)
EXTERN_COMPARE_COL_PTR(less_col, int32_t)
EXTERN_COMPARE_COL_PTR(less_col, int64_t)
EXTERN_COMPARE_COL_PTR(less_col, float)
EXTERN_COMPARE_COL_PTR(less_col, double)

EXTERN_COMPARE_COL_PTR(greater_col, bool)
EXTERN_COMPARE_COL_PTR(greater_col, int8_t)
EXTERN_COMPARE_COL_PTR(greater_col, int16_t)
EXTERN_COMPARE_COL_PTR(greater_col, int32_t)
EXTERN_COMPARE_COL_PTR(greater_col, int64_t)
EXTERN_COMPARE_COL_PTR(greater_col, float)
EXTERN_COMPARE_COL_PTR(greater_col, double)

EXTERN_COMPARE_COL_PTR(less_equal_col, bool)
EXTERN_COMPARE_COL_PTR(less_equal_col, int8_t)
EXTERN_COMPARE_COL_PTR(less_equal_col, int16_t)
EXTERN_COMPARE_COL_PTR(less_equal_col, int32_t)
EXTERN_COMPARE_COL_PTR(less_equal_col, int64_t)
EXTERN_COMPARE_COL_PTR(less_equal_col, float)
EXTERN_COMPARE_COL_PTR(less_equal_col, double)

EXTERN_COMPARE_COL_PTR(greater_equal_col, bool)
EXTERN_COMPARE_COL_PTR(greater_equal_col, int8_t)
EXTERN_COMPARE_COL_PTR(greater_equal_col, int16_t)
EXTERN_COMPARE_COL_PTR(greater_equal_col, int32_t)
EXTERN_COMPARE_COL_PTR(greater_equal_col, int64_t)
EXTERN_COMPARE_COL_PTR(greater_equal_col, float)
EXTERN_COMPARE_COL_PTR(greater_equal_col, double)

EXTERN_COMPARE_COL_PTR(not_equal_col, bool)
EXTERN_COMPARE_COL_PTR(not_equal_col, int8_t)
EXTERN_COMPARE_COL_PTR(not_equal_col, int16_t)
EXTERN_COMPARE_COL_PTR(not_equal_col, int32_t)
EXTERN_COMPARE_COL_PTR(not_equal_col, int64_t)
EXTERN_COMPARE_COL_PTR(not_equal_col, float)
EXTERN_COMPARE_COL_PTR(not_equal_col, double)
#if defined(__x86_64__)
// Flags that indicate whether runtime can choose
// these simd type or not when hook starts.
extern bool use_avx512;
extern bool use_avx2;
extern bool use_sse4_2;
extern bool use_sse2;

#endif

#if defined(__x86_64__)
bool
cpu_support_avx512();
bool
cpu_support_avx2();
bool
cpu_support_sse4_2();
#endif

#define DISPATCH_FIND_TERM_SIMD_FUNC(type)                      \
    if constexpr (std::is_same_v<T, type>) {                    \
        return milvus::simd::find_term_##type(data, size, val); \
    }

#define DISPATCH_COMPARE_VAL_SIMD_FUNC(prefix, type)                \
    if constexpr (std::is_same_v<T, type>) {                        \
        return milvus::simd::prefix##_##type(data, size, val, res); \
    }

#define DISPATCH_COMPARE_COL_SIMD_FUNC(prefix, type)                  \
    if constexpr (std::is_same_v<T, type>) {                          \
        return milvus::simd::prefix##_##type(left, right, size, res); \
    }

template <typename T>
bool
find_term_func(const T* data, size_t size, T val) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_FIND_TERM_SIMD_FUNC(bool)
    DISPATCH_FIND_TERM_SIMD_FUNC(int8_t)
    DISPATCH_FIND_TERM_SIMD_FUNC(int16_t)
    DISPATCH_FIND_TERM_SIMD_FUNC(int32_t)
    DISPATCH_FIND_TERM_SIMD_FUNC(int64_t)
    DISPATCH_FIND_TERM_SIMD_FUNC(float)
    DISPATCH_FIND_TERM_SIMD_FUNC(double)
}

template <typename T>
void
equal_val_func(const T* data, int64_t size, T val, bool* res) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_COMPARE_VAL_SIMD_FUNC(equal_val, bool)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(equal_val, int8_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(equal_val, int16_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(equal_val, int32_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(equal_val, int64_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(equal_val, float)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(equal_val, double)
}

template <typename T>
void
less_val_func(const T* data, int64_t size, T val, bool* res) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_val, bool)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_val, int8_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_val, int16_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_val, int32_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_val, int64_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_val, float)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_val, double)
}

template <typename T>
void
greater_val_func(const T* data, int64_t size, T val, bool* res) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_val, bool)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_val, int8_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_val, int16_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_val, int32_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_val, int64_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_val, float)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_val, double)
}

template <typename T>
void
less_equal_val_func(const T* data, int64_t size, T val, bool* res) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_equal_val, bool)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_equal_val, int8_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_equal_val, int16_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_equal_val, int32_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_equal_val, int64_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_equal_val, float)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(less_equal_val, double)
}

template <typename T>
void
greater_equal_val_func(const T* data, int64_t size, T val, bool* res) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_equal_val, bool)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_equal_val, int8_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_equal_val, int16_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_equal_val, int32_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_equal_val, int64_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_equal_val, float)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(greater_equal_val, double)
}

template <typename T>
void
not_equal_val_func(const T* data, int64_t size, T val, bool* res) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_COMPARE_VAL_SIMD_FUNC(not_equal_val, bool)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(not_equal_val, int8_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(not_equal_val, int16_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(not_equal_val, int32_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(not_equal_val, int64_t)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(not_equal_val, float)
    DISPATCH_COMPARE_VAL_SIMD_FUNC(not_equal_val, double)
}

template <typename T>
void
equal_col_func(const T* left, const T* right, int64_t size, bool* res) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_COMPARE_COL_SIMD_FUNC(equal_col, bool)
    DISPATCH_COMPARE_COL_SIMD_FUNC(equal_col, int8_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(equal_col, int16_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(equal_col, int32_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(equal_col, int64_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(equal_col, float)
    DISPATCH_COMPARE_COL_SIMD_FUNC(equal_col, double)
}

template <typename T>
void
less_col_func(const T* left, const T* right, int64_t size, bool* res) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_COMPARE_COL_SIMD_FUNC(less_col, bool)
    DISPATCH_COMPARE_COL_SIMD_FUNC(less_col, int8_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(less_col, int16_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(less_col, int32_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(less_col, int64_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(less_col, float)
    DISPATCH_COMPARE_COL_SIMD_FUNC(less_col, double)
}

template <typename T>
void
greater_col_func(const T* left, const T* right, int64_t size, bool* res) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_col, bool)
    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_col, int8_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_col, int16_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_col, int32_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_col, int64_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_col, float)
    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_col, double)
}

template <typename T>
void
less_equal_col_func(const T* left, const T* right, int64_t size, bool* res) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_COMPARE_COL_SIMD_FUNC(less_equal_col, bool)
    DISPATCH_COMPARE_COL_SIMD_FUNC(less_equal_col, int8_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(less_equal_col, int16_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(less_equal_col, int32_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(less_equal_col, int64_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(less_equal_col, float)
    DISPATCH_COMPARE_COL_SIMD_FUNC(less_equal_col, double)
}

template <typename T>
void
greater_equal_col_func(const T* left, const T* right, int64_t size, bool* res) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_equal_col, bool)
    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_equal_col, int8_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_equal_col, int16_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_equal_col, int32_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_equal_col, int64_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_equal_col, float)
    DISPATCH_COMPARE_COL_SIMD_FUNC(greater_equal_col, double)
}

template <typename T>
void
not_equal_col_func(const T* left, const T* right, int64_t size, bool* res) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    DISPATCH_COMPARE_COL_SIMD_FUNC(not_equal_col, bool)
    DISPATCH_COMPARE_COL_SIMD_FUNC(not_equal_col, int8_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(not_equal_col, int16_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(not_equal_col, int32_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(not_equal_col, int64_t)
    DISPATCH_COMPARE_COL_SIMD_FUNC(not_equal_col, float)
    DISPATCH_COMPARE_COL_SIMD_FUNC(not_equal_col, double)
}

}  // namespace simd
}  // namespace milvus
