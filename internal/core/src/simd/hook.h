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

extern FindTermPtr<bool> find_term_bool;
extern FindTermPtr<int8_t> find_term_int8;
extern FindTermPtr<int16_t> find_term_int16;
extern FindTermPtr<int32_t> find_term_int32;
extern FindTermPtr<int64_t> find_term_int64;
extern FindTermPtr<float> find_term_float;
extern FindTermPtr<double> find_term_double;

#if defined(__x86_64__)
// Flags that indicate whether runtime can choose
// these simd type or not when hook starts.
extern bool use_avx512;
extern bool use_avx2;
extern bool use_sse4_2;
extern bool use_sse2;

// Flags that indicate which kind of simd for
// different function when hook ends.
extern bool use_bitset_sse2;
extern bool use_find_term_sse2;
extern bool use_find_term_sse4_2;
extern bool use_find_term_avx2;
extern bool use_find_term_avx512;
#endif

#if defined(__x86_64__)
bool
cpu_support_avx512();
bool
cpu_support_avx2();
bool
cpu_support_sse4_2();
#endif

void
bitset_hook();

void
find_term_hook();

template <typename T>
bool
find_term_func(const T* data, size_t size, T val) {
    static_assert(
        std::is_integral<T>::value || std::is_floating_point<T>::value,
        "T must be integral or float/double type");

    if constexpr (std::is_same_v<T, bool>) {
        return milvus::simd::find_term_bool(data, size, val);
    }
    if constexpr (std::is_same_v<T, int8_t>) {
        return milvus::simd::find_term_int8(data, size, val);
    }
    if constexpr (std::is_same_v<T, int16_t>) {
        return milvus::simd::find_term_int16(data, size, val);
    }
    if constexpr (std::is_same_v<T, int32_t>) {
        return milvus::simd::find_term_int32(data, size, val);
    }
    if constexpr (std::is_same_v<T, int64_t>) {
        return milvus::simd::find_term_int64(data, size, val);
    }
    if constexpr (std::is_same_v<T, float>) {
        return milvus::simd::find_term_float(data, size, val);
    }
    if constexpr (std::is_same_v<T, double>) {
        return milvus::simd::find_term_double(data, size, val);
    }
}

}  // namespace simd
}  // namespace milvus
