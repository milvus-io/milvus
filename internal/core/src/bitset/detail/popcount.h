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

#include <cstdint>

namespace milvus {
namespace bitset {
namespace detail {

//
template <typename T>
struct PopCountHelper {};

//
template <>
struct PopCountHelper<unsigned long long> {
    static inline unsigned long long
    count(const unsigned long long v) {
        return __builtin_popcountll(v);
    }
};

template <>
struct PopCountHelper<unsigned long> {
    static inline unsigned long
    count(const unsigned long v) {
        return __builtin_popcountl(v);
    }
};

template <>
struct PopCountHelper<unsigned int> {
    static inline unsigned int
    count(const unsigned int v) {
        return __builtin_popcount(v);
    }
};

template <>
struct PopCountHelper<uint8_t> {
    static inline uint8_t
    count(const uint8_t v) {
        return __builtin_popcount(v);
    }
};

}  // namespace detail
}  // namespace bitset
}  // namespace milvus
