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

// This file is compiled with -mavx2 -mfma flags
// It provides the AVX2 implementations defined in the header

#include "fusion_compute_avx2.h"

namespace milvus {
namespace minhash {

// AVX2-compatible minimum function for 64-bit unsigned integers
// AVX2 doesn't have _mm256_min_epu64, so we implement it using comparison and selection
__m256i
_mm256_min_epu64_avx2(__m256i a, __m256i b) {
    // Use signed comparison since for comparison purposes, the bit patterns are equivalent
    // For unsigned min, we can use the fact that comparison works correctly
    __m256i cmp = _mm256_cmpgt_epi64(a, b);  // a > b? (element-wise)
    // If a > b, select b; otherwise select a
    __m256i result = _mm256_blendv_epi8(a, b, cmp);
    return result;
}

// AVX2 optimized rotation-based batch MinHash computation
// Processes 8 hash functions at once (in 2 batches of 4) using rotation optimization
// AVX2 has 256-bit registers, so we process 4 x 64-bit elements per batch
// Note: perm_a, perm_b, and sig pointers should already point to the correct offset
void
linear_and_find_min_batch8_avx2(const uint64_t* base,
                                size_t shingle_count,
                                const uint64_t* perm_a,
                                const uint64_t* perm_b,
                                uint32_t* sig) {
    // Process in two batches of 4 to match the batch8 interface
    for (int batch = 0; batch < 2; batch++) {
        int32_t offset = batch * 4;

        __m256i vec_a = _mm256_loadu_si256((__m256i*)&perm_a[offset]);
        __m256i vec_b = _mm256_loadu_si256((__m256i*)&perm_b[offset]);

        // Load current signature values (32-bit) and extend to 64-bit
        __m128i sig_128 = _mm_loadu_si128((__m128i*)&sig[offset]);
        __m256i vec_min = _mm256_cvtepu32_epi64(sig_128);

        __m256i vec_mersenne = _mm256_set1_epi64x(MERSENNE_PRIME);
        __m256i vec_mersenne_minus_1 = _mm256_set1_epi64x(MERSENNE_PRIME - 1);
        __m256i vec_mask = _mm256_set1_epi64x(MAX_HASH_MASK);
        size_t j = 0;

        // Process 8 base hashes at a time
        for (; j + 8 <= shingle_count; j += 8) {
            // Prefetch next block
            if (j + 16 <= shingle_count) {
                _mm_prefetch((const char*)&base[j + 16], _MM_HINT_T0);
            }

            // Load 8 base hashes in two 256-bit vectors
            __m256i vec_hashes1 = _mm256_loadu_si256((__m256i*)&base[j]);
            __m256i vec_hashes2 = _mm256_loadu_si256((__m256i*)&base[j + 4]);

            // Process each of 4 hash functions against each base hash using rotation
            for (int rot = 0; rot < 4; rot++) {
                // Process first 4 hashes
                {
                    // Compute: (a * hash + b) % MERSENNE_PRIME & MAX_HASH_MASK
                    // For a = a0 + a1*2^32, h = h0 + h1*2^32:
                    // (a * h) mod 2^64 = a0*h0 + (a0*h1 + a1*h0) * 2^32
                    __m256i vec_result =
                        _mm256_mul_epu32(vec_a, vec_hashes1);  // a0 * h0

                    // Compute cross products for full 64-bit multiplication
                    __m256i a_high = _mm256_srli_epi64(vec_a, 32);
                    __m256i h_high = _mm256_srli_epi64(vec_hashes1, 32);
                    __m256i cross1 =
                        _mm256_mul_epu32(vec_a, h_high);  // a0 * h1
                    __m256i cross2 =
                        _mm256_mul_epu32(a_high, vec_hashes1);  // a1 * h0
                    cross1 = _mm256_slli_epi64(cross1, 32);
                    cross2 = _mm256_slli_epi64(cross2, 32);
                    vec_result = _mm256_add_epi64(vec_result, cross1);
                    vec_result = _mm256_add_epi64(vec_result, cross2);

                    vec_result = _mm256_add_epi64(vec_result, vec_b);

                    // Fast Mersenne modulo using Barrett reduction
                    __m256i low_bits =
                        _mm256_and_si256(vec_result, vec_mersenne);
                    __m256i high_bits = _mm256_srli_epi64(vec_result, 61);
                    vec_result = _mm256_add_epi64(low_bits, high_bits);

                    // Conditional subtraction: if result >= MERSENNE_PRIME, subtract it
                    // Use > MERSENNE_PRIME-1 to implement >= MERSENNE_PRIME
                    __m256i cmp =
                        _mm256_cmpgt_epi64(vec_result, vec_mersenne_minus_1);
                    __m256i sub_val = _mm256_and_si256(cmp, vec_mersenne);
                    vec_result = _mm256_sub_epi64(vec_result, sub_val);

                    // Mask to 32-bit
                    vec_result = _mm256_and_si256(vec_result, vec_mask);

                    // Update minimum
                    vec_min = _mm256_min_epu64_avx2(vec_min, vec_result);

                    // Rotate hashes for next iteration
                    // Rotate: [0,1,2,3] -> [1,2,3,0]
                    vec_hashes1 = _mm256_permute4x64_epi64(
                        vec_hashes1, _MM_SHUFFLE(0, 3, 2, 1));
                }

                // Process next 4 hashes
                {
                    // (a * h) mod 2^64 = a0*h0 + (a0*h1 + a1*h0) * 2^32
                    __m256i vec_result =
                        _mm256_mul_epu32(vec_a, vec_hashes2);  // a0 * h0

                    __m256i a_high = _mm256_srli_epi64(vec_a, 32);
                    __m256i h_high = _mm256_srli_epi64(vec_hashes2, 32);
                    __m256i cross1 =
                        _mm256_mul_epu32(vec_a, h_high);  // a0 * h1
                    __m256i cross2 =
                        _mm256_mul_epu32(a_high, vec_hashes2);  // a1 * h0
                    cross1 = _mm256_slli_epi64(cross1, 32);
                    cross2 = _mm256_slli_epi64(cross2, 32);
                    vec_result = _mm256_add_epi64(vec_result, cross1);
                    vec_result = _mm256_add_epi64(vec_result, cross2);

                    vec_result = _mm256_add_epi64(vec_result, vec_b);

                    __m256i low_bits =
                        _mm256_and_si256(vec_result, vec_mersenne);
                    __m256i high_bits = _mm256_srli_epi64(vec_result, 61);
                    vec_result = _mm256_add_epi64(low_bits, high_bits);

                    // Use > MERSENNE_PRIME-1 to implement >= MERSENNE_PRIME
                    __m256i cmp =
                        _mm256_cmpgt_epi64(vec_result, vec_mersenne_minus_1);
                    __m256i sub_val = _mm256_and_si256(cmp, vec_mersenne);
                    vec_result = _mm256_sub_epi64(vec_result, sub_val);

                    vec_result = _mm256_and_si256(vec_result, vec_mask);
                    vec_min = _mm256_min_epu64_avx2(vec_min, vec_result);

                    vec_hashes2 = _mm256_permute4x64_epi64(
                        vec_hashes2, _MM_SHUFFLE(0, 3, 2, 1));
                }
            }
        }

        // Process 4 base hashes at a time
        for (; j + 4 <= shingle_count; j += 4) {
            if (j + 16 <= shingle_count) {
                _mm_prefetch((const char*)&base[j + 16], _MM_HINT_T0);
            }

            __m256i vec_hashes = _mm256_loadu_si256((__m256i*)&base[j]);

            for (int rot = 0; rot < 4; rot++) {
                // (a * h) mod 2^64 = a0*h0 + (a0*h1 + a1*h0) * 2^32
                __m256i vec_result =
                    _mm256_mul_epu32(vec_a, vec_hashes);  // a0 * h0

                __m256i a_high = _mm256_srli_epi64(vec_a, 32);
                __m256i h_high = _mm256_srli_epi64(vec_hashes, 32);
                __m256i cross1 = _mm256_mul_epu32(vec_a, h_high);  // a0 * h1
                __m256i cross2 =
                    _mm256_mul_epu32(a_high, vec_hashes);  // a1 * h0
                cross1 = _mm256_slli_epi64(cross1, 32);
                cross2 = _mm256_slli_epi64(cross2, 32);
                vec_result = _mm256_add_epi64(vec_result, cross1);
                vec_result = _mm256_add_epi64(vec_result, cross2);

                vec_result = _mm256_add_epi64(vec_result, vec_b);

                __m256i low_bits = _mm256_and_si256(vec_result, vec_mersenne);
                __m256i high_bits = _mm256_srli_epi64(vec_result, 61);
                vec_result = _mm256_add_epi64(low_bits, high_bits);

                // Use > MERSENNE_PRIME-1 to implement >= MERSENNE_PRIME
                __m256i cmp =
                    _mm256_cmpgt_epi64(vec_result, vec_mersenne_minus_1);
                __m256i sub_val = _mm256_and_si256(cmp, vec_mersenne);
                vec_result = _mm256_sub_epi64(vec_result, sub_val);

                vec_result = _mm256_and_si256(vec_result, vec_mask);
                vec_min = _mm256_min_epu64_avx2(vec_min, vec_result);

                vec_hashes = _mm256_permute4x64_epi64(vec_hashes,
                                                      _MM_SHUFFLE(0, 3, 2, 1));
            }
        }

        // Process remaining base hashes one at a time
        for (; j < shingle_count; j++) {
            __m256i vec_hash = _mm256_set1_epi64x(base[j]);

            // (a * h) mod 2^64 = a0*h0 + (a0*h1 + a1*h0) * 2^32
            __m256i vec_result = _mm256_mul_epu32(vec_a, vec_hash);  // a0 * h0

            __m256i a_high = _mm256_srli_epi64(vec_a, 32);
            __m256i h_high = _mm256_srli_epi64(vec_hash, 32);
            __m256i cross1 = _mm256_mul_epu32(vec_a, h_high);     // a0 * h1
            __m256i cross2 = _mm256_mul_epu32(a_high, vec_hash);  // a1 * h0
            cross1 = _mm256_slli_epi64(cross1, 32);
            cross2 = _mm256_slli_epi64(cross2, 32);
            vec_result = _mm256_add_epi64(vec_result, cross1);
            vec_result = _mm256_add_epi64(vec_result, cross2);

            vec_result = _mm256_add_epi64(vec_result, vec_b);

            __m256i low_bits = _mm256_and_si256(vec_result, vec_mersenne);
            __m256i high_bits = _mm256_srli_epi64(vec_result, 61);
            vec_result = _mm256_add_epi64(low_bits, high_bits);

            // Use > MERSENNE_PRIME-1 to implement >= MERSENNE_PRIME
            __m256i cmp = _mm256_cmpgt_epi64(vec_result, vec_mersenne_minus_1);
            __m256i sub_val = _mm256_and_si256(cmp, vec_mersenne);
            vec_result = _mm256_sub_epi64(vec_result, sub_val);

            vec_result = _mm256_and_si256(vec_result, vec_mask);
            vec_min = _mm256_min_epu64_avx2(vec_min, vec_result);
        }

        // Store results back to signature (convert 64-bit to 32-bit)
        // Extract lower 32 bits from each 64-bit element
        __m256i shuffled =
            _mm256_shuffle_epi32(vec_min, _MM_SHUFFLE(2, 0, 2, 0));
        __m128i result_32 = _mm256_castsi256_si128(shuffled);
        __m128i result_high = _mm256_extracti128_si256(shuffled, 1);
        result_32 = _mm_unpacklo_epi64(result_32, result_high);

        _mm_storeu_si128((__m128i*)&sig[offset], result_32);
    }
}

// AVX2 optimized linear_and_find_min for single hash function
// Processes base hashes in chunks of 4 using SIMD
uint32_t
linear_and_find_min_avx2(const uint64_t* base,
                         size_t shingle_count,
                         const uint64_t perm_a,
                         const uint64_t perm_b) {
    __m256i vec_a = _mm256_set1_epi64x(perm_a);
    __m256i vec_b = _mm256_set1_epi64x(perm_b);
    __m256i vec_mersenne = _mm256_set1_epi64x(MERSENNE_PRIME);
    __m256i vec_mersenne_minus_1 = _mm256_set1_epi64x(MERSENNE_PRIME - 1);
    __m256i vec_mask = _mm256_set1_epi64x(MAX_HASH_MASK);

    // Initialize minimum to max value
    __m256i vec_min = _mm256_set1_epi64x(MAX_HASH_32);

    size_t j = 0;

    // Process 4 base hashes at a time
    for (; j + 4 <= shingle_count; j += 4) {
        // Prefetch next block
        if (j + 16 <= shingle_count) {
            _mm_prefetch((const char*)&base[j + 16], _MM_HINT_T0);
        }

        // Load 4 base hashes
        __m256i vec_hashes = _mm256_loadu_si256((__m256i*)&base[j]);

        // Compute: (a * hash + b) using manual 64-bit multiplication
        // For a = a0 + a1*2^32, h = h0 + h1*2^32:
        // (a * h) mod 2^64 = a0*h0 + (a0*h1 + a1*h0) * 2^32
        __m256i vec_result = _mm256_mul_epu32(vec_a, vec_hashes);  // a0 * h0

        // Compute cross products for full 64-bit multiplication
        __m256i a_high = _mm256_srli_epi64(vec_a, 32);
        __m256i h_high = _mm256_srli_epi64(vec_hashes, 32);
        __m256i cross1 = _mm256_mul_epu32(vec_a, h_high);       // a0 * h1
        __m256i cross2 = _mm256_mul_epu32(a_high, vec_hashes);  // a1 * h0
        cross1 = _mm256_slli_epi64(cross1, 32);
        cross2 = _mm256_slli_epi64(cross2, 32);
        vec_result = _mm256_add_epi64(vec_result, cross1);
        vec_result = _mm256_add_epi64(vec_result, cross2);

        vec_result = _mm256_add_epi64(vec_result, vec_b);

        // Fast Mersenne modulo using Barrett reduction
        __m256i low_bits = _mm256_and_si256(vec_result, vec_mersenne);
        __m256i high_bits = _mm256_srli_epi64(vec_result, 61);
        vec_result = _mm256_add_epi64(low_bits, high_bits);

        // Conditional subtraction: if result >= MERSENNE_PRIME, subtract it
        // Use > MERSENNE_PRIME-1 to implement >= MERSENNE_PRIME
        __m256i cmp = _mm256_cmpgt_epi64(vec_result, vec_mersenne_minus_1);
        __m256i sub_val = _mm256_and_si256(cmp, vec_mersenne);
        vec_result = _mm256_sub_epi64(vec_result, sub_val);

        // Mask to 32-bit
        vec_result = _mm256_and_si256(vec_result, vec_mask);

        // Update minimum
        vec_min = _mm256_min_epu64_avx2(vec_min, vec_result);
    }

    // Horizontal minimum reduction across vector lanes
    uint64_t min_vals[4];
    _mm256_storeu_si256((__m256i*)min_vals, vec_min);
    uint32_t min_val = (uint32_t)min_vals[0];
    for (int k = 1; k < 4; k++) {
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
