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

#include "segcore/minhash_c.h"
#include "minhash/MinHashComputer.h"
#include "minhash/MinHashHook.h"
#include <vector>
#include <cstring>
#include <random>

void
InitPermutations(int32_t num_hashes,
                 uint64_t seed,
                 uint64_t* perm_a,
                 uint64_t* perm_b) {
    milvus::minhash::InitPermutations(num_hashes, seed, perm_a, perm_b);
}

// ComputeMinHashFromTexts implementation
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
                        uint32_t* signatures) {
    // Initialize SIMD hooks based on runtime CPU detection
    milvus::minhash::minhash_hook_init();

    milvus::minhash::ComputeFromTextsDirectly(
        texts,
        text_lengths,
        num_texts,
        tokenizer_ptr,
        shingle_size,
        perm_a,
        perm_b,
        (milvus::minhash::HashFunction)hash_func,
        num_hashes,
        signatures);
}
