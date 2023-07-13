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

#include <emmintrin.h>
#include <iostream>

#include "common.h"
#include "sse2.h"
namespace milvus {
namespace simd {

template <typename T>
bool
FindTermSSE4(const T* src, size_t vec_size, T val) {
    CHECK_SUPPORTED_TYPE(T, "unsupported type for FindTermSSE2");
    // SSE4 still hava 128bit, using same code with SSE2
    return FindTermSSE2<T>(src, vec_size, val);
}

template <>
bool
FindTermSSE4(const int64_t* src, size_t vec_size, int64_t val);

int
StrCmpSSE4(const char* s1, const char* s2);

}  // namespace simd
}  // namespace milvus
