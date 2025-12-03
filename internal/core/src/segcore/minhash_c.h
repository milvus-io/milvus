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

#ifndef MILVUS_MINHASH_MINHASH_C_H
#define MILVUS_MINHASH_MINHASH_C_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

void
InitPermutations(int32_t num_hashes,
                 uint64_t seed,
                 uint64_t* perm_a,
                 uint64_t* perm_b);

/**
 * ComputeMinHashFromTexts - Complete end-to-end MinHash computation in C++
 * Eliminates all CGO overhead by doing everything in C++:
 * - Tokenization via C API (token_stream_c.h)
 * - Shingle generation
 * - Base hash computation
 * - MinHash signature computation with rotation-based SIMD
 *
 * @param texts Array of raw text strings
 * @param text_lengths Length of each text
 * @param num_texts Number of texts
 * @param tokenizer_ptr Analyzer handle (void* - CAnalyzer from analyzer_c.h), nullptr for char-level
 * @param shingle_size N-gram size
 * @param perm_a Permutation parameter 'a' array
 * @param perm_b Permutation parameter 'b' array
 * @param num_hashes Number of hash functions (signature dimension)
 * @param signatures Output buffer (num_texts * num_hashes)
 */
void
ComputeMinHashFromTexts(const char** texts,
                        const int32_t* text_lengths,
                        int32_t num_texts,
                        void* tokenizer_ptr,
                        int32_t shingle_size,
                        const uint64_t* perm_a,
                        const uint64_t* perm_b,
                        int hash_func,
                        int32_t num_hashes,
                        uint32_t* signatures);

#ifdef __cplusplus
}
#endif

#endif  // MILVUS_MINHASH_MINHASH_C_H
