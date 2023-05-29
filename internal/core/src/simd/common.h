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
#include <type_traits>

namespace milvus {
namespace simd {

using BitsetBlockType = unsigned long;
constexpr size_t BITSET_BLOCK_SIZE = sizeof(unsigned long);

/*
* For term size less than TERM_EXPR_IN_SIZE_THREAD,
* using simd search better for all numberic type.
* For term size bigger than TERM_EXPR_IN_SIZE_THREAD, 
* using set search better for all numberic type.
* 50 is experimental value, using dynamic plan to support modify it
* in different situation.
*/
const int TERM_EXPR_IN_SIZE_THREAD = 50;

#define CHECK_SUPPORTED_TYPE(T, Message)                                     \
    static_assert(                                                           \
        std::is_same<T, bool>::value || std::is_same<T, int8_t>::value ||    \
            std::is_same<T, int16_t>::value ||                               \
            std::is_same<T, int32_t>::value ||                               \
            std::is_same<T, int64_t>::value ||                               \
            std::is_same<T, float>::value || std::is_same<T, double>::value, \
        Message);

}  // namespace simd
}  // namespace milvus
