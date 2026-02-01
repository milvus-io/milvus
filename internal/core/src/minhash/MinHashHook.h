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

#include <cstddef>
#include <cstdint>

namespace milvus {
namespace minhash {

// Function pointer types for SIMD implementations
using LinearAndFindMinFunc = uint32_t (*)(const uint64_t* base,
                                          size_t shingle_count,
                                          uint64_t perm_a,
                                          uint64_t perm_b);

using LinearAndFindMinBatch8Func = void (*)(const uint64_t* base,
                                            size_t shingle_count,
                                            const uint64_t* perm_a,
                                            const uint64_t* perm_b,
                                            uint32_t* sig);

// Global function pointers - initialized at runtime to appropriate implementations
extern LinearAndFindMinFunc linear_and_find_min_impl;
extern LinearAndFindMinBatch8Func linear_and_find_min_batch8_impl;

// Initialize SIMD level based on runtime CPU detection
// This function sets the global function pointers
void
minhash_hook_init();

}  // namespace minhash
}  // namespace milvus
