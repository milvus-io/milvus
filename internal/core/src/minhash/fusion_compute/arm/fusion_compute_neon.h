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

// Helper function for 64-bit multiplication in NEON
inline uint64x2_t
vmull_u64_neon(uint64x2_t a, uint64x2_t b) {
    // NEON doesn't have native 64-bit multiplication, so we emulate it
    // Split into high and low 32-bit parts
    uint32x2_t a_lo = vmovn_u64(a);
    uint32x2_t a_hi = vshrn_n_u64(a, 32);
    uint32x2_t b_lo = vmovn_u64(b);
    uint32x2_t b_hi = vshrn_n_u64(b, 32);

    // Compute partial products
    uint64x2_t lo_lo = vmull_u32(a_lo, b_lo);
    uint64x2_t lo_hi = vmull_u32(a_lo, b_hi);
    uint64x2_t hi_lo = vmull_u32(a_hi, b_lo);
    uint64x2_t hi_hi = vmull_u32(a_hi, b_hi);

    // Combine: (hi_hi << 64) + ((lo_hi + hi_lo) << 32) + lo_lo
    // We only keep lower 64 bits (natural overflow)
    uint64x2_t mid = vaddq_u64(lo_hi, hi_lo);
    uint64x2_t mid_shifted = vshlq_n_u64(mid, 32);
    uint64x2_t result = vaddq_u64(lo_lo, mid_shifted);

    // Add high bits contribution
    uint64x2_t hi_shifted = vshlq_n_u64(hi_hi, 32);
    result = vaddq_u64(result, hi_shifted);

    return result;
}

// NEON optimized rotation-based batch MinHash computation
// Processes 8 hash functions at once (in 4 batches of 2) using rotation optimization
// NEON has 128-bit registers, so we process 2 x 64-bit elements per batch
// Note: perm_a, perm_b, and sig pointers should already point to the correct offset
inline void
linear_and_find_min_batch8_neon(const uint64_t* base,
                                size_t shingle_count,
                                const uint64_t* perm_a,
                                const uint64_t* perm_b,
                                uint32_t* sig) {
    // Process in four batches of 2 to match the batch8 interface
    for (int batch = 0; batch < 4; batch++) {
        int32_t offset = batch * 2;

        uint64x2_t vec_a = vld1q_u64(&perm_a[offset]);
        uint64x2_t vec_b = vld1q_u64(&perm_b[offset]);

        // Load current signature values (32-bit) and extend to 64-bit
        uint32x2_t sig_32 = vld1_u32(&sig[offset]);
        uint64x2_t vec_min = vmovl_u32(sig_32);

        uint64x2_t vec_mersenne = vdupq_n_u64(MERSENNE_PRIME);
        uint64x2_t vec_mask = vdupq_n_u64(MAX_HASH_MASK);
        size_t j = 0;

        // Process 4 base hashes at a time
        for (; j + 4 <= shingle_count; j += 4) {
            // Prefetch next block
            if (j + 16 <= shingle_count) {
                __builtin_prefetch(&base[j + 16], 0, 0);
            }

            // Load 4 base hashes in two 128-bit vectors
            uint64x2_t vec_hashes1 = vld1q_u64(&base[j]);
            uint64x2_t vec_hashes2 = vld1q_u64(&base[j + 2]);

            // Process each of 2 hash functions against each base hash using rotation
            for (int rot = 0; rot < 2; rot++) {
                // Process first 2 hashes
                {
                    // Compute: (a * hash + b) % MERSENNE_PRIME & MAX_HASH_MASK
                    uint64x2_t vec_result = vmull_u64_neon(vec_a, vec_hashes1);
                    vec_result = vaddq_u64(vec_result, vec_b);

                    // Fast Mersenne modulo using Barrett reduction
                    uint64x2_t low_bits = vandq_u64(vec_result, vec_mersenne);
                    uint64x2_t high_bits = vshrq_n_u64(vec_result, 61);
                    vec_result = vaddq_u64(low_bits, high_bits);

                    // Conditional subtraction: if result >= MERSENNE_PRIME, subtract it
                    uint64x2_t cmp = vcgeq_u64(vec_result, vec_mersenne);
                    uint64x2_t sub_val = vandq_u64(cmp, vec_mersenne);
                    vec_result = vsubq_u64(vec_result, sub_val);

                    // Mask to 32-bit
                    vec_result = vandq_u64(vec_result, vec_mask);

                    // Update minimum
                    vec_min = vminq_u64(vec_min, vec_result);

                    // Rotate hashes: [0,1] -> [1,0]
                    vec_hashes1 = vextq_u64(vec_hashes1, vec_hashes1, 1);
                }

                // Process next 2 hashes
                {
                    uint64x2_t vec_result = vmull_u64_neon(vec_a, vec_hashes2);
                    vec_result = vaddq_u64(vec_result, vec_b);

                    uint64x2_t low_bits = vandq_u64(vec_result, vec_mersenne);
                    uint64x2_t high_bits = vshrq_n_u64(vec_result, 61);
                    vec_result = vaddq_u64(low_bits, high_bits);

                    uint64x2_t cmp = vcgeq_u64(vec_result, vec_mersenne);
                    uint64x2_t sub_val = vandq_u64(cmp, vec_mersenne);
                    vec_result = vsubq_u64(vec_result, sub_val);

                    vec_result = vandq_u64(vec_result, vec_mask);
                    vec_min = vminq_u64(vec_min, vec_result);

                    vec_hashes2 = vextq_u64(vec_hashes2, vec_hashes2, 1);
                }
            }
        }

        // Process 2 base hashes at a time
        for (; j + 2 <= shingle_count; j += 2) {
            if (j + 16 <= shingle_count) {
                __builtin_prefetch(&base[j + 16], 0, 0);
            }

            uint64x2_t vec_hashes = vld1q_u64(&base[j]);

            for (int rot = 0; rot < 2; rot++) {
                uint64x2_t vec_result = vmull_u64_neon(vec_a, vec_hashes);
                vec_result = vaddq_u64(vec_result, vec_b);

                uint64x2_t low_bits = vandq_u64(vec_result, vec_mersenne);
                uint64x2_t high_bits = vshrq_n_u64(vec_result, 61);
                vec_result = vaddq_u64(low_bits, high_bits);

                uint64x2_t cmp = vcgeq_u64(vec_result, vec_mersenne);
                uint64x2_t sub_val = vandq_u64(cmp, vec_mersenne);
                vec_result = vsubq_u64(vec_result, sub_val);

                vec_result = vandq_u64(vec_result, vec_mask);
                vec_min = vminq_u64(vec_min, vec_result);

                vec_hashes = vextq_u64(vec_hashes, vec_hashes, 1);
            }
        }

        // Process remaining base hashes one at a time
        for (; j < shingle_count; j++) {
            uint64x2_t vec_hash = vdupq_n_u64(base[j]);

            uint64x2_t vec_result = vmull_u64_neon(vec_a, vec_hash);
            vec_result = vaddq_u64(vec_result, vec_b);

            uint64x2_t low_bits = vandq_u64(vec_result, vec_mersenne);
            uint64x2_t high_bits = vshrq_n_u64(vec_result, 61);
            vec_result = vaddq_u64(low_bits, high_bits);

            uint64x2_t cmp = vcgeq_u64(vec_result, vec_mersenne);
            uint64x2_t sub_val = vandq_u64(cmp, vec_mersenne);
            vec_result = vsubq_u64(vec_result, sub_val);

            vec_result = vandq_u64(vec_result, vec_mask);
            vec_min = vminq_u64(vec_min, vec_result);
        }

        // Store results back to signature (convert 64-bit to 32-bit)
        uint32x2_t result_32 = vmovn_u64(vec_min);
        vst1_u32(&sig[offset], result_32);
    }
}

// NEON optimized linear_and_find_min for single hash function
// Processes base hashes in chunks of 2 using SIMD
inline uint32_t
linear_and_find_min_neon(const uint64_t* base,
                         size_t shingle_count,
                         const uint64_t perm_a,
                         const uint64_t perm_b) {
    uint64x2_t vec_a = vdupq_n_u64(perm_a);
    uint64x2_t vec_b = vdupq_n_u64(perm_b);
    uint64x2_t vec_mersenne = vdupq_n_u64(MERSENNE_PRIME);
    uint64x2_t vec_mask = vdupq_n_u64(MAX_HASH_MASK);

    // Initialize minimum to max value
    uint64x2_t vec_min = vdupq_n_u64(MAX_HASH_32);

    size_t j = 0;

    // Process 2 base hashes at a time
    for (; j + 2 <= shingle_count; j += 2) {
        // Prefetch next block
        if (j + 16 <= shingle_count) {
            __builtin_prefetch(&base[j + 16], 0, 0);
        }

        // Load 2 base hashes
        uint64x2_t vec_hashes = vld1q_u64(&base[j]);

        // Compute: (a * hash + b) % MERSENNE_PRIME & MAX_HASH_MASK
        uint64x2_t vec_result = vmull_u64_neon(vec_a, vec_hashes);
        vec_result = vaddq_u64(vec_result, vec_b);

        // Fast Mersenne modulo using Barrett reduction
        uint64x2_t low_bits = vandq_u64(vec_result, vec_mersenne);
        uint64x2_t high_bits = vshrq_n_u64(vec_result, 61);
        vec_result = vaddq_u64(low_bits, high_bits);

        // Conditional subtraction
        uint64x2_t cmp = vcgeq_u64(vec_result, vec_mersenne);
        uint64x2_t sub_val = vandq_u64(cmp, vec_mersenne);
        vec_result = vsubq_u64(vec_result, sub_val);

        // Mask to 32-bit
        vec_result = vandq_u64(vec_result, vec_mask);

        // Update minimum
        vec_min = vminq_u64(vec_min, vec_result);
    }

    // Horizontal minimum reduction across vector lanes
    uint64_t min_vals[2];
    vst1q_u64(min_vals, vec_min);
    uint32_t min_val = (uint32_t)min_vals[0];
    if ((uint32_t)min_vals[1] < min_val) {
        min_val = (uint32_t)min_vals[1];
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
