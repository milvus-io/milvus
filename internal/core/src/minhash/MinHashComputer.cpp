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

// SHA1 from OpenSSL (conan dependencies)
#include <openssl/sha.h>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iosfwd>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "MinHashHook.h"
#include "fusion_compute_native.h"
#include "tantivy/token-stream.h"
#include "tantivy/tokenizer.h"
// xxHash from conan dependencies (includes XXH3)
#include "xxhash.h"

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
            // Use lower 32 bits of XXH3_64bits for better performance
            return static_cast<uint64_t>(
                static_cast<uint32_t>(XXH3_64bits(shingle, shingle_size)));
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
            // Use function pointer for SIMD dispatch
            linear_and_find_min_batch8_impl(base_hashes + base_hash_offset,
                                            shingle_count,
                                            perm_a + i,
                                            perm_b + i,
                                            sig + i);
        }
        for (; i < num_hashes; i++) {
            // Use function pointer for SIMD dispatch
            sig[i] = linear_and_find_min_impl(base_hashes + base_hash_offset,
                                              shingle_count,
                                              perm_a[i],
                                              perm_b[i]);
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
        auto* tokenizer =
            static_cast<milvus::tantivy::Tokenizer*>(tokenizer_ptr);
        std::vector<char> shingle_buffer;
        shingle_buffer.reserve(1000);

        for (int32_t text_idx = 0; text_idx < num_texts; text_idx++) {
            // TODO: optimize to avoid copying text into std::string
            auto token_stream = tokenizer->CreateTokenStream(
                std::string(texts[text_idx], text_lengths[text_idx]));

            auto* ts = token_stream.get();
            std::vector<const char*> tokens;
            tokens.reserve(128);
            while (ts->advance()) {
                tokens.push_back(ts->get_token_no_copy());
            }

            int32_t token_count = static_cast<int32_t>(tokens.size());

            if (token_count == 0) {
                hash_counts[text_idx] = 0;
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
        }
    }
}

void
InitPermutations(int32_t num_hashes,
                 uint64_t seed,
                 uint64_t* perm_a,
                 uint64_t* perm_b) {
    std::mt19937_64 rng(seed);

    for (int32_t i = 0; i < num_hashes; i++) {
        // use raw mt19937_64 output
        // (guaranteed consistent across platforms && different compilers)
        uint64_t raw_a = rng();
        uint64_t raw_b = rng();

        perm_a[i] = (raw_a % (MERSENNE_PRIME - 1)) + 1;
        perm_b[i] = raw_b % MERSENNE_PRIME;
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
