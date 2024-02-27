// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <immintrin.h>

#include <type_traits>

#include "bitset/common.h"

namespace milvus {
namespace bitset {
namespace detail {
namespace x86 {

//
template <typename T, CompareOpType type>
struct ComparePredicate {};

template <typename T>
struct ComparePredicate<T, CompareOpType::EQ> {
    static inline constexpr int value =
        std::is_floating_point_v<T> ? _CMP_EQ_OQ : _MM_CMPINT_EQ;
};

template <typename T>
struct ComparePredicate<T, CompareOpType::LT> {
    static inline constexpr int value =
        std::is_floating_point_v<T> ? _CMP_LT_OQ : _MM_CMPINT_LT;
};

template <typename T>
struct ComparePredicate<T, CompareOpType::LE> {
    static inline constexpr int value =
        std::is_floating_point_v<T> ? _CMP_LE_OQ : _MM_CMPINT_LE;
};

template <typename T>
struct ComparePredicate<T, CompareOpType::GT> {
    static inline constexpr int value =
        std::is_floating_point_v<T> ? _CMP_GT_OQ : _MM_CMPINT_NLE;
};

template <typename T>
struct ComparePredicate<T, CompareOpType::GE> {
    static inline constexpr int value =
        std::is_floating_point_v<T> ? _CMP_GE_OQ : _MM_CMPINT_NLT;
};

template <typename T>
struct ComparePredicate<T, CompareOpType::NE> {
    static inline constexpr int value =
        std::is_floating_point_v<T> ? _CMP_NEQ_OQ : _MM_CMPINT_NE;
};

}  // namespace x86
}  // namespace detail
}  // namespace bitset
}  // namespace milvus
