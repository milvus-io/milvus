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

#include <stddef.h>
#include <stdint.h>

namespace milvus {
namespace minhash {

void
linear_and_find_min_batch8_avx512(const uint64_t* base,
                                  size_t shingle_count,
                                  const uint64_t* perm_a,
                                  const uint64_t* perm_b,
                                  uint32_t* sig);

uint32_t
linear_and_find_min_avx512(const uint64_t* base,
                           size_t shingle_count,
                           const uint64_t perm_a,
                           const uint64_t perm_b);

}  // namespace minhash
}  // namespace milvus
