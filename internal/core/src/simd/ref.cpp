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

#include "ref.h"

namespace milvus {
namespace simd {

BitsetBlockType
GetBitsetBlockRef(const bool* src) {
    BitsetBlockType val = 0;
    uint8_t vals[BITSET_BLOCK_SIZE] = {0};
    for (size_t j = 0; j < 8; ++j) {
        for (size_t k = 0; k < BITSET_BLOCK_SIZE; ++k) {
            vals[k] |= uint8_t(*(src + k * 8 + j)) << j;
        }
    }
    for (size_t j = 0; j < BITSET_BLOCK_SIZE; ++j) {
        val |= (BitsetBlockType)(vals[j]) << (8 * j);
    }
    return val;
}

}  // namespace simd
}  // namespace milvus
