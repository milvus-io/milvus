// Copyright (C) 2019-2025 Zilliz. All rights reserved.
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

#include <cstdint>
#include <cstddef>

namespace milvus {
namespace minhash {

// Constants used in hash computation
inline constexpr uint64_t MERSENNE_PRIME = 0x1FFFFFFFFFFFFFFFULL;  // 2^61 - 1
inline constexpr uint64_t MAX_HASH_MASK = 0xFFFFFFFFULL;           // 2^32 - 1
inline constexpr uint32_t MAX_HASH_32 = 0xFFFFFFFF;

// Native (non-SIMD) implementations - used as fallback
static inline uint32_t
linear_and_find_min_native(const uint64_t* base,
                           size_t shingle_count,
                           const uint64_t perm_a,
                           const uint64_t perm_b) {
    uint32_t sig = UINT32_MAX;
    for (size_t j = 0; j < shingle_count; j++) {
        uint64_t temp = perm_a * base[j] + perm_b;
        // Fast Mersenne modulo
        uint64_t low = temp & MERSENNE_PRIME;
        uint64_t high = temp >> 61;
        temp = low + high;
        if (temp >= MERSENNE_PRIME) {
            temp -= MERSENNE_PRIME;
        }

        uint32_t permuted_hash = (uint32_t)(temp & MAX_HASH_MASK);
        if (permuted_hash < sig) {
            sig = permuted_hash;
        }
    }
    return sig;
}

static inline void
linear_and_find_min_batch8_native(const uint64_t* base,
                                  size_t shingle_count,
                                  const uint64_t* perm_a,
                                  const uint64_t* perm_b,
                                  uint32_t* sig) {
#pragma unroll
    for (int k = 0; k < 8; k++) {
        for (size_t j = 0; j < shingle_count; j++) {
            uint64_t temp = perm_a[k] * base[j] + perm_b[k];

            uint64_t low = temp & MERSENNE_PRIME;
            uint64_t high = temp >> 61;
            temp = low + high;
            if (temp >= MERSENNE_PRIME) {
                temp -= MERSENNE_PRIME;
            }

            uint32_t permuted_hash = (uint32_t)(temp & MAX_HASH_MASK);
            if (permuted_hash < sig[k]) {
                sig[k] = permuted_hash;
            }
        }
    }
}

}  // namespace minhash
}  // namespace milvus
