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

#include "hook.h"
namespace milvus {
namespace simd {

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

template <typename T>
void
compare_col_func(CompareType cmp_type,
                 const T* left,
                 const T* right,
                 int64_t size,
                 bool* res) {
    if (cmp_type == CompareType::EQ) {
        equal_col_func(left, right, size, res);
    } else if (cmp_type == CompareType::NEQ) {
        not_equal_col_func(left, right, size, res);
    } else if (cmp_type == CompareType::GE) {
        greater_equal_col_func(left, right, size, res);
    } else if (cmp_type == CompareType::GT) {
        greater_col_func(left, right, size, res);
    } else if (cmp_type == CompareType::LE) {
        less_equal_col_func(left, right, size, res);
    } else if (cmp_type == CompareType::LT) {
        less_col_func(left, right, size, res);
    }
}

}  // namespace simd
}  // namespace milvus
