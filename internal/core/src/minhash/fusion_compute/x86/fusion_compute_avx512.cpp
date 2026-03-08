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

// This file is compiled with -mavx512f -mavx512dq -mavx512bw -mavx512vl flags
// It provides the AVX512 implementations defined in the header

#include "fusion_compute_avx512.h"

#include <immintrin.h>
#include <xmmintrin.h>

#include "fusion_compute_native.h"

namespace milvus {
namespace minhash {

// AVX512 optimized rotation-based batch MinHash computation
// Processes 8 hash functions at once using rotation optimization
// find min((a* base + b) % MERSENNE_PRIME & MAX_HASH_MASK) for each base value
// batch process 8 functions in parallel
void
linear_and_find_min_batch8_avx512(const uint64_t* base,
                                  size_t shingle_count,
                                  const uint64_t* perm_a,
                                  const uint64_t* perm_b,
                                  uint32_t* sig) {
    __m512i vec_a =
        _mm512_loadu_si512((__m512i*)perm_a);  // params a of 8 linear functions
    __m512i vec_b =
        _mm512_loadu_si512((__m512i*)perm_b);  // params b of 8 linear functions
    __m512i vec_min = _mm512_cvtepu32_epi64(_mm256_loadu_si256((__m256i*)sig));

    __m512i vec_mersenne = _mm512_set1_epi64(MERSENNE_PRIME);
    __m512i vec_mask = _mm512_set1_epi64(MAX_HASH_MASK);
    size_t j = 0;

    // Process 16 base hashes at a time
    for (; j + 16 <= shingle_count; j += 16) {
        if (j + 16 <= shingle_count) {
            _mm_prefetch((const char*)&base[j + 16], _MM_HINT_T0);
        }

        __m512i vec_hashes1 = _mm512_loadu_si512((__m512i*)&base[j]);
        __m512i vec_hashes2 = _mm512_loadu_si512((__m512i*)&base[j + 8]);

        // Rotation: process each of 8 hash functions against each base hash
        for (int rot = 0; rot < 8; rot++) {
            // Process first 8 hashes
            {
                __m512i vec_result = _mm512_mullo_epi64(vec_a, vec_hashes1);
                vec_result = _mm512_add_epi64(vec_result, vec_b);

                __m512i low_bits = _mm512_and_si512(vec_result, vec_mersenne);
                __m512i high_bits = _mm512_srli_epi64(vec_result, 61);
                vec_result = _mm512_add_epi64(low_bits, high_bits);
                __mmask8 mask_ge =
                    _mm512_cmpge_epu64_mask(vec_result, vec_mersenne);
                vec_result = _mm512_mask_sub_epi64(
                    vec_result, mask_ge, vec_result, vec_mersenne);

                vec_result = _mm512_and_si512(vec_result, vec_mask);
                __m256i vec_perm_32 = _mm512_cvtepi64_epi32(vec_result);
                __m512i vec_perm_64 = _mm512_cvtepu32_epi64(vec_perm_32);

                vec_min = _mm512_min_epu64(vec_min, vec_perm_64);
                vec_hashes1 = _mm512_alignr_epi64(vec_hashes1, vec_hashes1, 1);
            }
            // Process next 8 hashes
            {
                __m512i vec_result = _mm512_mullo_epi64(vec_a, vec_hashes2);
                vec_result = _mm512_add_epi64(vec_result, vec_b);

                __m512i low_bits = _mm512_and_si512(vec_result, vec_mersenne);
                __m512i high_bits = _mm512_srli_epi64(vec_result, 61);
                vec_result = _mm512_add_epi64(low_bits, high_bits);
                __mmask8 mask_ge =
                    _mm512_cmpge_epu64_mask(vec_result, vec_mersenne);
                vec_result = _mm512_mask_sub_epi64(
                    vec_result, mask_ge, vec_result, vec_mersenne);

                vec_result = _mm512_and_si512(vec_result, vec_mask);
                __m256i vec_perm_32 = _mm512_cvtepi64_epi32(vec_result);
                __m512i vec_perm_64 = _mm512_cvtepu32_epi64(vec_perm_32);

                vec_min = _mm512_min_epu64(vec_min, vec_perm_64);
                vec_hashes2 = _mm512_alignr_epi64(vec_hashes2, vec_hashes2, 1);
            }
        }
    }

    // Process 8 base hashes at a time
    for (; j + 8 <= shingle_count; j += 8) {
        if (j + 16 <= shingle_count) {
            _mm_prefetch((const char*)&base[j + 16], _MM_HINT_T0);
        }
        __m512i vec_hashes = _mm512_loadu_si512((__m512i*)&base[j]);

        for (int rot = 0; rot < 8; rot++) {
            __m512i vec_result = _mm512_mullo_epi64(vec_a, vec_hashes);
            vec_result = _mm512_add_epi64(vec_result, vec_b);

            __m512i low_bits = _mm512_and_si512(vec_result, vec_mersenne);
            __m512i high_bits = _mm512_srli_epi64(vec_result, 61);
            vec_result = _mm512_add_epi64(low_bits, high_bits);
            __mmask8 mask_ge =
                _mm512_cmpge_epu64_mask(vec_result, vec_mersenne);
            vec_result = _mm512_mask_sub_epi64(
                vec_result, mask_ge, vec_result, vec_mersenne);

            vec_result = _mm512_and_si512(vec_result, vec_mask);
            __m256i vec_perm_32 = _mm512_cvtepi64_epi32(vec_result);
            __m512i vec_perm_64 = _mm512_cvtepu32_epi64(vec_perm_32);

            vec_min = _mm512_min_epu64(vec_min, vec_perm_64);
            vec_hashes = _mm512_alignr_epi64(vec_hashes, vec_hashes, 1);
        }
    }

    // Process remaining base hashes one at a time
    for (; j < shingle_count; j++) {
        __m512i vec_hash = _mm512_set1_epi64(base[j]);
        __m512i vec_result = _mm512_mullo_epi64(vec_a, vec_hash);
        vec_result = _mm512_add_epi64(vec_result, vec_b);

        __m512i low_bits = _mm512_and_si512(vec_result, vec_mersenne);
        __m512i high_bits = _mm512_srli_epi64(vec_result, 61);
        vec_result = _mm512_add_epi64(low_bits, high_bits);
        __mmask8 mask_ge = _mm512_cmpge_epu64_mask(vec_result, vec_mersenne);
        vec_result = _mm512_mask_sub_epi64(
            vec_result, mask_ge, vec_result, vec_mersenne);

        vec_result = _mm512_and_si512(vec_result, vec_mask);
        __m256i vec_perm_32 = _mm512_cvtepi64_epi32(vec_result);
        __m512i vec_perm_64 = _mm512_cvtepu32_epi64(vec_perm_32);

        vec_min = _mm512_min_epu64(vec_min, vec_perm_64);
    }

    // Store results back to signature
    __m256i result_32 = _mm512_cvtepi64_epi32(vec_min);
    _mm256_storeu_si256((__m256i*)sig, result_32);
}

// AVX512 optimized linear_and_find_min for single hash function
// Processes base hashes in chunks of 8 using SIMD
uint32_t
linear_and_find_min_avx512(const uint64_t* base,
                           size_t shingle_count,
                           const uint64_t perm_a,
                           const uint64_t perm_b) {
    __m512i vec_a = _mm512_set1_epi64(perm_a);
    __m512i vec_b = _mm512_set1_epi64(perm_b);
    __m512i vec_mersenne = _mm512_set1_epi64(MERSENNE_PRIME);
    __m512i vec_mask = _mm512_set1_epi64(MAX_HASH_MASK);

    // Initialize minimum to max value
    __m512i vec_min = _mm512_set1_epi64(MAX_HASH_32);

    size_t j = 0;

    // Process 8 base hashes at a time
    for (; j + 8 <= shingle_count; j += 8) {
        // Prefetch next block
        if (j + 16 <= shingle_count) {
            _mm_prefetch((const char*)&base[j + 16], _MM_HINT_T0);
        }

        // Load 8 base hashes
        __m512i vec_hashes = _mm512_loadu_si512((__m512i*)&base[j]);

        // Compute: (a * hash + b) % MERSENNE_PRIME & MAX_HASH_MASK
        __m512i vec_result = _mm512_mullo_epi64(vec_a, vec_hashes);
        vec_result = _mm512_add_epi64(vec_result, vec_b);

        // Fast Mersenne modulo using Barrett reduction
        __m512i low_bits = _mm512_and_si512(vec_result, vec_mersenne);
        __m512i high_bits = _mm512_srli_epi64(vec_result, 61);
        vec_result = _mm512_add_epi64(low_bits, high_bits);

        // Conditional subtraction
        __mmask8 mask_ge = _mm512_cmpge_epu64_mask(vec_result, vec_mersenne);
        vec_result = _mm512_mask_sub_epi64(
            vec_result, mask_ge, vec_result, vec_mersenne);

        // Mask to 32-bit
        vec_result = _mm512_and_si512(vec_result, vec_mask);

        // Update minimum
        vec_min = _mm512_min_epu64(vec_min, vec_result);
    }

    // Horizontal minimum reduction across vector lanes
    uint64_t min_vals[8];
    _mm512_storeu_si512((__m512i*)min_vals, vec_min);
    uint32_t min_val = (uint32_t)min_vals[0];
    for (int k = 1; k < 8; k++) {
        if ((uint32_t)min_vals[k] < min_val) {
            min_val = (uint32_t)min_vals[k];
        }
    }

    // Process remaining base hashes with scalar code
    for (; j < shingle_count; j++) {
        uint64_t temp = perm_a * base[j] + perm_b;

        // Fast Mersenne modulo
        uint64_t low = temp & MERSENNE_PRIME;
        uint64_t high = temp >> 61;
        temp = low + high;
        if (temp >= MERSENNE_PRIME) {
            temp -= MERSENNE_PRIME;
        }

        uint32_t permuted_hash = (uint32_t)(temp & MAX_HASH_MASK);
        if (permuted_hash < min_val) {
            min_val = permuted_hash;
        }
    }

    return min_val;
}

}  // namespace minhash
}  // namespace milvus
