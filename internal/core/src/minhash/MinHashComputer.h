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
#include <vector>
#include <memory>

namespace milvus {
namespace minhash {
// Hash function types
enum class HashFunction { SHA1 = 0, XXHASH64 = 1 };

void
InitPermutations(int32_t num_hashes,
                 uint64_t seed,
                 uint64_t* perm_a,
                 uint64_t* perm_b);

void
HashNGramWindow(const char** texts,
                const int32_t* text_lengths,
                int32_t num_texts,
                void* tokenizer_ptr,
                int32_t shingle_size,
                HashFunction hash_func_type,
                std::vector<uint64_t>& all_base_hashes,
                std::vector<int32_t>& hash_counts);

void
ComputeFromTextsDirectly(const char** texts,
                         const int32_t* text_lengths,
                         int32_t num_texts,
                         void* tokenizer_ptr,
                         int32_t shingle_size,
                         const uint64_t* perm_a,
                         const uint64_t* perm_b,
                         HashFunction hash_func_type,
                         int32_t num_hashes,
                         uint32_t* signatures);

}  // namespace minhash
}  // namespace milvus
