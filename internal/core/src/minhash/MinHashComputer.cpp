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

#include "MinHashComputer.h"

#include <cstdint>
#include <cstring>
#include <cstdio>
#include <algorithm>
#include <limits>
#include <memory>
#include <vector>
#include <random>
// xxHash from conan dependencies (includes XXH3)
#include "xxhash.h"

// SHA1 from OpenSSL (conan dependencies)
#include <openssl/sha.h>

// Tokenizer C API for end-to-end MinHash computation
#include "segcore/tokenizer_c.h"

// Direct C++ TokenStream API (preferred for zero-copy)
#include "tantivy/tokenizer.h"
#include "tantivy/token-stream.h"

#if defined(__x86_64__) || defined(_M_X64)
#ifdef __AVX512F__
#define MINHASH_USE_AVX512
#include "x86/fusion_compute_avx512.h"
#endif
#ifdef __AVX2__
#define MINHASH_USE_AVX2
#include "x86/fusion_compute_avx2.h"
#endif
#elif defined(__aarch64__) || defined(_M_ARM64)
#if defined(__ARM_FEATURE_SVE)
#define MINHASH_USE_SVE
#include "arm/fusion_compute_sve.h"
#endif
#ifdef __ARM_NEON
#define MINHASH_USE_NEON
#include "arm/fusion_compute_neon.h"
#endif
#endif

namespace milvus {
namespace minhash {
namespace {
using HashFuncPtr = uint64_t (*)(const char*, int32_t);
HashFuncPtr
SelectHashFunction(HashFunction hash_type) {
    if (hash_type == HashFunction::SHA1) {
        return [](const char* shingle, const int32_t shingle_size) {
            unsigned char digest[SHA_DIGEST_LENGTH];
            SHA1(reinterpret_cast<const unsigned char*>(shingle),
                 shingle_size,
                 digest);
            // Take first 4 bytes as little-endian uint32
            return static_cast<uint64_t>(digest[0] | (digest[1] << 8) |
                                         (digest[2] << 16) | (digest[3] << 24));
        };
    } else {
        return [](const char* shingle, const int32_t shingle_size) {
            // todo: use 32bit value to return
            return static_cast<uint64_t>(XXH3_64bits(shingle, shingle_size));
        };
    }
}

// ComputeBatchRotation implementation
void
ComputeBatchRotation(const uint64_t* base_hashes,
                     const int32_t* shingle_counts,
                     int32_t num_texts,
                     const uint64_t* perm_a,
                     const uint64_t* perm_b,
                     int32_t num_hashes,
                     uint32_t* signatures) {
    int32_t base_hash_offset = 0;

    for (int32_t text_idx = 0; text_idx < num_texts; text_idx++) {
        uint32_t* sig = signatures + text_idx * num_hashes;
        int32_t shingle_count = shingle_counts[text_idx];

        for (int32_t i = 0; i < num_hashes; i++) {
            sig[i] = MAX_HASH_32;
        }

        int32_t i = 0;
        for (; i + 8 <= num_hashes; i += 8) {
            // Dispatch to platform-specific implementation
#if defined(MINHASH_USE_AVX512)
            linear_and_find_min_batch8_avx512(base_hashes + base_hash_offset,
                                              shingle_count,
                                              perm_a + i,
                                              perm_b + i,
                                              sig + i);
#elif defined(MINHASH_USE_AVX2)
            linear_and_find_min_batch8_avx2(base_hashes + base_hash_offset,
                                            shingle_count,
                                            perm_a + i,
                                            perm_b + i,
                                            sig + i);
#elif defined(MINHASH_USE_SVE)

            linear_and_find_min_batch8_sve(base_hashes + base_hash_offset,
                                           shingle_count,
                                           perm_a + i,
                                           perm_b + i,
                                           sig + i);
#elif defined(MINHASH_USE_NEON)
            linear_and_find_min_batch8_neon(base_hashes + base_hash_offset,
                                            shingle_count,
                                            perm_a + i,
                                            perm_b + i,
                                            sig + i);
#else
            // Fallback to native implementation
            linear_and_find_min_batch8_native(base_hashes + base_hash_offset,
                                              shingle_count,
                                              perm_a + i,
                                              perm_b + i,
                                              sig + i);
#endif
        }
        for (; i < num_hashes; i++) {
#if defined(MINHASH_USE_AVX512)
            sig[i] = linear_and_find_min_avx512(base_hashes + base_hash_offset,
                                                shingle_count,
                                                perm_a[i],
                                                perm_b[i]);
#elif defined(MINHASH_USE_AVX2)
            sig[i] = linear_and_find_min_avx2(base_hashes + base_hash_offset,
                                              shingle_count,
                                              perm_a[i],
                                              perm_b[i]);
#elif defined(MINHASH_USE_SVE)
            sig[i] = linear_and_find_min_sve(base_hashes + base_hash_offset,
                                             shingle_count,
                                             perm_a[i],
                                             perm_b[i]);
#elif defined(MINHASH_USE_NEON)
            sig[i] = linear_and_find_min_neon(base_hashes + base_hash_offset,
                                              shingle_count,
                                              perm_a[i],
                                              perm_b[i]);
#else
            sig[i] = linear_and_find_min_native(base_hashes + base_hash_offset,
                                                shingle_count,
                                                perm_a[i],
                                                perm_b[i]);
#endif
        }

        base_hash_offset += shingle_count;
    }
}
}  // namespace

void
HashNGramWindow(const char** texts,
                const int32_t* text_lengths,
                int32_t num_texts,
                void* tokenizer_ptr,
                int32_t shingle_size,
                HashFunction hash_func_type,
                std::vector<uint64_t>& all_base_hashes,
                std::vector<int32_t>& hash_counts) {
    auto hash_func = SelectHashFunction(hash_func_type);
    all_base_hashes.clear();
    all_base_hashes.reserve(num_texts * 100);
    hash_counts.resize(num_texts);

    if (tokenizer_ptr == nullptr) {
        // char-level token
        for (int32_t text_idx = 0; text_idx < num_texts; text_idx++) {
            const char* text = texts[text_idx];
            int32_t text_len = text_lengths[text_idx];

            if (text_len == 0) {
                hash_counts[text_idx] = 0;
                continue;
            }

            if (text_len < shingle_size) {
                all_base_hashes.push_back(hash_func(text, text_len));
                hash_counts[text_idx] = 1;
            } else {
                int32_t num_shingles = text_len - shingle_size + 1;
                hash_counts[text_idx] = num_shingles;

                for (int32_t i = 0; i < num_shingles; i++) {
                    all_base_hashes.push_back(
                        hash_func(text + i, shingle_size));
                }
            }
        }
    } else {
        // word-level token - Direct C++ TokenStream API (zero-copy optimization)
        CTokenizer tokenizer = static_cast<CTokenizer>(tokenizer_ptr);
        std::vector<char> shingle_buffer;
        shingle_buffer.reserve(1000);

        for (int32_t text_idx = 0; text_idx < num_texts; text_idx++) {
            CTokenStream stream = create_token_stream(
                tokenizer, texts[text_idx], text_lengths[text_idx]);

            auto* ts = static_cast<milvus::tantivy::TokenStream*>(stream);
            std::vector<const char*> tokens;
            tokens.reserve(128);
            while (ts->advance()) {
                tokens.push_back(ts->get_token_no_copy());
            }

            int32_t token_count = static_cast<int32_t>(tokens.size());

            if (token_count == 0) {
                hash_counts[text_idx] = 0;
                free_token_stream(stream);
                continue;
            }

            if (token_count < shingle_size) {
                shingle_buffer.clear();
                for (int32_t i = 0; i < token_count; i++) {
                    const char* token = tokens[i];
                    size_t len = std::strlen(token);
                    shingle_buffer.insert(
                        shingle_buffer.end(), token, token + len);
                }
                all_base_hashes.push_back(
                    hash_func(shingle_buffer.data(), shingle_buffer.size()));
                hash_counts[text_idx] = 1;
            } else {
                int32_t num_shingles = token_count - shingle_size + 1;
                hash_counts[text_idx] = num_shingles;

                for (int32_t i = 0; i < num_shingles; i++) {
                    shingle_buffer.clear();
                    for (int32_t j = 0; j < shingle_size; j++) {
                        const char* token = tokens[i + j];
                        size_t len = std::strlen(token);
                        shingle_buffer.insert(
                            shingle_buffer.end(), token, token + len);
                    }
                    all_base_hashes.push_back(hash_func(shingle_buffer.data(),
                                                        shingle_buffer.size()));
                }
            }

            // Free individual token strings (from Rust)
            for (auto token : tokens) {
                free_rust_string(token);
            }
            free_token_stream(stream);
        }
    }
}

void
InitPermutations(int32_t num_hashes,
                 uint64_t seed,
                 uint64_t* perm_a,
                 uint64_t* perm_b) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<uint64_t> dist(
        1, std::numeric_limits<uint32_t>::max());

    for (int32_t i = 0; i < num_hashes; i++) {
        perm_a[i] = dist(rng) % (MERSENNE_PRIME - 1) + 1;
        perm_b[i] = dist(rng) % MERSENNE_PRIME;
    }
}

// ComputeFromTextsDirectly implementation
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
                         uint32_t* signatures) {
    // Compute base hashes from ngrams
    std::vector<uint64_t> all_base_hashes;
    std::vector<int32_t> hash_counts;
    HashNGramWindow(texts,
                    text_lengths,
                    num_texts,
                    tokenizer_ptr,
                    shingle_size,
                    hash_func_type,
                    all_base_hashes,
                    hash_counts);

    // Call rotation-based SIMD optimized MinHash computation
    ComputeBatchRotation(all_base_hashes.data(),
                         hash_counts.data(),
                         num_texts,
                         perm_a,
                         perm_b,
                         num_hashes,
                         signatures);
}

}  // namespace minhash
}  // namespace milvus
