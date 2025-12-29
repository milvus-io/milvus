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

#include <arm_neon.h>
#include "fusion_compute_native.h"

namespace milvus {
namespace minhash {

// NEON optimized rotation-based batch MinHash computation
// Processes 8 hash functions at once (in 4 batches of 2) using rotation optimization
// NEON has 128-bit registers, so we process 2 x 64-bit elements per batch
// Note: perm_a, perm_b, and sig pointers should already point to the correct offset
void
linear_and_find_min_batch8_neon(const uint64_t* base,
                                size_t shingle_count,
                                const uint64_t* perm_a,
                                const uint64_t* perm_b,
                                uint32_t* sig);

// NEON optimized linear_and_find_min for single hash function
// Processes base hashes in chunks of 2 using SIMD
uint32_t
linear_and_find_min_neon(const uint64_t* base,
                         size_t shingle_count,
                         const uint64_t perm_a,
                         const uint64_t perm_b);

}  // namespace minhash
}  // namespace milvus
