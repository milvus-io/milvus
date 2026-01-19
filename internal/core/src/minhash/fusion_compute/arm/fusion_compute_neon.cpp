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

// This file provides the NEON implementations defined in the header

#include "fusion_compute_neon.h"

namespace milvus {
namespace minhash {
inline void
rotate_left_u64x2x4_inplace(uint64x2x4_t& x) {
    uint64x2_t first = x.val[0];
    x.val[0] = x.val[1];
    x.val[1] = x.val[2];
    x.val[2] = x.val[3];
    x.val[3] = first;
}
inline void
fast_mersenne_modulo(uint64x2x4_t& x,
                     uint64x2_t vec_mersenne,
                     uint64x2_t vec_mask) {
    for (int k = 0; k < 4; k++) {
        uint64x2_t low_bits = vandq_u64(x.val[k], vec_mersenne);
        uint64x2_t high_bits = vshrq_n_u64(x.val[k], 61);
        x.val[k] = vaddq_u64(low_bits, high_bits);

        // Conditional subtraction
        uint64x2_t cmp = vcgeq_u64(x.val[k], vec_mersenne);
        uint64x2_t sub_val = vandq_u64(cmp, vec_mersenne);
        x.val[k] = vsubq_u64(x.val[k], sub_val);

        // Mask to 32-bit
        x.val[k] = vandq_u64(x.val[k], vec_mask);
    }
}
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
    // For a*b where a = (a_hi << 32) + a_lo and b = (b_hi << 32) + b_lo:
    // a*b = a_lo*b_lo + (a_lo*b_hi << 32) + (a_hi*b_lo << 32) + (a_hi*b_hi << 64)
    // We only keep lower 64 bits, so (a_hi*b_hi << 64) doesn't contribute
    uint64x2_t lo_lo = vmull_u32(a_lo, b_lo);
    uint64x2_t lo_hi = vmull_u32(a_lo, b_hi);
    uint64x2_t hi_lo = vmull_u32(a_hi, b_lo);

    // Shift the middle terms and add
    // Note: lo_hi and hi_lo are already 64-bit results from 32x32 multiplication
    // We need to shift them by 32 bits: (lo_hi << 32) and (hi_lo << 32)
    uint64x2_t lo_hi_shifted = vshlq_n_u64(lo_hi, 32);
    uint64x2_t hi_lo_shifted = vshlq_n_u64(hi_lo, 32);

    // result = lo_lo + (lo_hi << 32) + (hi_lo << 32)
    uint64x2_t result = vaddq_u64(lo_lo, lo_hi_shifted);
    result = vaddq_u64(result, hi_lo_shifted);

    return result;
}

// NEON optimized rotation-based batch MinHash computation
// Processes 8 hash functions at once (in 4 batches of 2) using rotation optimization
void
linear_and_find_min_batch8_neon(const uint64_t* base,
                                size_t shingle_count,
                                const uint64_t* perm_a,
                                const uint64_t* perm_b,
                                uint32_t* sig) {
    uint64x2_t vec_mersenne = vdupq_n_u64(MERSENNE_PRIME);
    uint64x2_t vec_mask = vdupq_n_u64(MAX_HASH_MASK);
    // NEON processes 2 hash functions at a time, so we need 4 batches for 8 functions
    uint64x2x4_t a = vld4q_u64(perm_a);
    uint64x2x4_t b = vld4q_u64(perm_b);
    uint64x2x4_t min = {vdupq_n_u64(UINT64_MAX),
                        vdupq_n_u64(UINT64_MAX),
                        vdupq_n_u64(UINT64_MAX),
                        vdupq_n_u64(UINT64_MAX)};

    size_t i = 0;
    for (; i + 8 <= shingle_count; i += 8) {
        auto x = vld4q_u64(base + i);
        // Process 8 base hashes in parallel
        for (auto j = 0; j < 4; j++) {
            uint64x2x4_t cmp_min;
            // Compute: (a * x + b) % MERSENNE_PRIME & MAX_HASH_MASK
            uint64x2x4_t res;
            res.val[0] = vmull_u64_neon(a.val[0], x.val[0]);
            res.val[1] = vmull_u64_neon(a.val[1], x.val[1]);
            res.val[2] = vmull_u64_neon(a.val[2], x.val[2]);
            res.val[3] = vmull_u64_neon(a.val[3], x.val[3]);

            // Add b
            res.val[0] = vaddq_u64(res.val[0], b.val[0]);
            res.val[1] = vaddq_u64(res.val[1], b.val[1]);
            res.val[2] = vaddq_u64(res.val[2], b.val[2]);
            res.val[3] = vaddq_u64(res.val[3], b.val[3]);

            // Fast Mersenne modulo for all 4 pairs
            fast_mersenne_modulo(res, vec_mersenne, vec_mask);
            // Update minimum
            cmp_min.val[0] = vcltq_u64(res.val[0], min.val[0]);
            cmp_min.val[1] = vcltq_u64(res.val[1], min.val[1]);
            cmp_min.val[2] = vcltq_u64(res.val[2], min.val[2]);
            cmp_min.val[3] = vcltq_u64(res.val[3], min.val[3]);

            min.val[0] = vbslq_u64(cmp_min.val[0], res.val[0], min.val[0]);
            min.val[1] = vbslq_u64(cmp_min.val[1], res.val[1], min.val[1]);
            min.val[2] = vbslq_u64(cmp_min.val[2], res.val[2], min.val[2]);
            min.val[3] = vbslq_u64(cmp_min.val[3], res.val[3], min.val[3]);

            // Rotate and compute again with shifted a and b
            uint64x2x4_t x_shift = {vextq_u64(x.val[0], x.val[0], 1),
                                    vextq_u64(x.val[1], x.val[1], 1),
                                    vextq_u64(x.val[2], x.val[2], 1),
                                    vextq_u64(x.val[3], x.val[3], 1)};

            res.val[0] = vmull_u64_neon(a.val[0], x_shift.val[0]);
            res.val[1] = vmull_u64_neon(a.val[1], x_shift.val[1]);
            res.val[2] = vmull_u64_neon(a.val[2], x_shift.val[2]);
            res.val[3] = vmull_u64_neon(a.val[3], x_shift.val[3]);

            // Add b
            res.val[0] = vaddq_u64(res.val[0], b.val[0]);
            res.val[1] = vaddq_u64(res.val[1], b.val[1]);
            res.val[2] = vaddq_u64(res.val[2], b.val[2]);
            res.val[3] = vaddq_u64(res.val[3], b.val[3]);

            // Fast Mersenne modulo for shifted values
            fast_mersenne_modulo(res, vec_mersenne, vec_mask);

            // Update minimum again with shifted results
            cmp_min.val[0] = vcltq_u64(res.val[0], min.val[0]);
            cmp_min.val[1] = vcltq_u64(res.val[1], min.val[1]);
            cmp_min.val[2] = vcltq_u64(res.val[2], min.val[2]);
            cmp_min.val[3] = vcltq_u64(res.val[3], min.val[3]);

            min.val[0] = vbslq_u64(cmp_min.val[0], res.val[0], min.val[0]);
            min.val[1] = vbslq_u64(cmp_min.val[1], res.val[1], min.val[1]);
            min.val[2] = vbslq_u64(cmp_min.val[2], res.val[2], min.val[2]);
            min.val[3] = vbslq_u64(cmp_min.val[3], res.val[3], min.val[3]);

            rotate_left_u64x2x4_inplace(x);
        }
    }

    for (; i < shingle_count; i++) {
        uint64x2x4_t x = {vdupq_n_u64(base[i]),
                          vdupq_n_u64(base[i]),
                          vdupq_n_u64(base[i]),
                          vdupq_n_u64(base[i])};
        uint64x2x4_t cmp_min;
        // Compute: (a * x + b) % MERSENNE_PRIME & MAX_HASH_MASK
        uint64x2x4_t res;
        res.val[0] = vmull_u64_neon(a.val[0], x.val[0]);
        res.val[1] = vmull_u64_neon(a.val[1], x.val[1]);
        res.val[2] = vmull_u64_neon(a.val[2], x.val[2]);
        res.val[3] = vmull_u64_neon(a.val[3], x.val[3]);

        res.val[0] = vaddq_u64(res.val[0], b.val[0]);
        res.val[1] = vaddq_u64(res.val[1], b.val[1]);
        res.val[2] = vaddq_u64(res.val[2], b.val[2]);
        res.val[3] = vaddq_u64(res.val[3], b.val[3]);

        // Fast Mersenne modulo for all 4 pairs
        fast_mersenne_modulo(res, vec_mersenne, vec_mask);
        // Update minimum
        cmp_min.val[0] = vcltq_u64(res.val[0], min.val[0]);
        cmp_min.val[1] = vcltq_u64(res.val[1], min.val[1]);
        cmp_min.val[2] = vcltq_u64(res.val[2], min.val[2]);
        cmp_min.val[3] = vcltq_u64(res.val[3], min.val[3]);

        min.val[0] = vbslq_u64(cmp_min.val[0], res.val[0], min.val[0]);
        min.val[1] = vbslq_u64(cmp_min.val[1], res.val[1], min.val[1]);
        min.val[2] = vbslq_u64(cmp_min.val[2], res.val[2], min.val[2]);
        min.val[3] = vbslq_u64(cmp_min.val[3], res.val[3], min.val[3]);
    }
    uint32x2x4_t result_32 = {vmovn_u64(min.val[0]),
                              vmovn_u64(min.val[1]),
                              vmovn_u64(min.val[2]),
                              vmovn_u64(min.val[3])};
    vst4_u32(sig, result_32);
}

// NEON optimized linear_and_find_min for single hash function
// Processes base hashes in chunks of 2 using SIMD
uint32_t
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

        // Update minimum (manual implementation since vminq_u64 doesn't exist)
        uint64x2_t cmp_min = vcltq_u64(vec_result, vec_min);
        vec_min = vbslq_u64(cmp_min, vec_result, vec_min);
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
