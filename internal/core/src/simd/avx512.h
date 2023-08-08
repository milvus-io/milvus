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

#include <cstddef>
#include <cstdint>
#include <stdexcept>

#include "common.h"

#if defined(__x86_64__)
#include <immintrin.h>
#endif

namespace milvus {
namespace simd {

template <typename T>
bool
FindTermAVX512(const T* src, size_t vec_size, T va) {
    CHECK_SUPPORTED_TYPE(T, "unsupported type for FindTermAVX512");
    return false;
}

template <>
bool
FindTermAVX512(const bool* src, size_t vec_size, bool val);

template <>
bool
FindTermAVX512(const int8_t* src, size_t vec_size, int8_t val);

template <>
bool
FindTermAVX512(const int16_t* src, size_t vec_size, int16_t val);

template <>
bool
FindTermAVX512(const int32_t* src, size_t vec_size, int32_t val);

template <>
bool
FindTermAVX512(const int64_t* src, size_t vec_size, int64_t val);

template <>
bool
FindTermAVX512(const float* src, size_t vec_size, float val);

template <>
bool
FindTermAVX512(const double* src, size_t vec_size, double val);

template <typename T>
void
EqualValAVX512(const T* src, size_t size, T val, bool* res);

template <typename T>
void
LessValAVX512(const T* src, size_t size, T val, bool* res);

template <typename T>
void
GreaterValAVX512(const T* src, size_t size, T val, bool* res);

template <typename T>
void
NotEqualValAVX512(const T* src, size_t size, T val, bool* res);

template <typename T>
void
LessEqualValAVX512(const T* src, size_t size, T val, bool* res);

template <typename T>
void
GreaterEqualValAVX512(const T* src, size_t size, T val, bool* res);

template <typename T>
void
EqualColumnAVX512(const T* left, const T* right, size_t size, bool* res);

template <typename T>
void
LessColumnAVX512(const T* left, const T* right, size_t size, bool* res);

template <typename T>
void
LessEqualColumnAVX512(const T* left, const T* right, size_t size, bool* res);

template <typename T>
void
GreaterColumnAVX512(const T* left, const T* right, size_t size, bool* res);

template <typename T>
void
GreaterEqualColumnAVX512(const T* left, const T* right, size_t size, bool* res);

template <typename T>
void
NotEqualColumnAVX512(const T* left, const T* right, size_t size, bool* res);

}  // namespace simd
}  // namespace milvus
