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

// returns 8 * sizeof(T) for 0
// returns 1 for 0b10
// returns 2 for 0b100
template <typename T>
struct CtzHelper {};

template <>
struct CtzHelper<uint8_t> {
    static inline auto
    ctz(const uint8_t value) {
        return __builtin_ctz(value);
    }
};

template <>
struct CtzHelper<unsigned int> {
    static inline auto
    ctz(const unsigned int value) {
        return __builtin_ctz(value);
    }
};

template <>
struct CtzHelper<unsigned long> {
    static inline auto
    ctz(const unsigned long value) {
        return __builtin_ctzl(value);
    }
};

template <>
struct CtzHelper<unsigned long long> {
    static inline auto
    ctz(const unsigned long long value) {
        return __builtin_ctzll(value);
    }
};

}  // namespace detail
}  // namespace bitset
}  // namespace milvus
