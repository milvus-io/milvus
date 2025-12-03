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

#include <arm_sve.h>
#include "fusion_compute_native.h"
namespace milvus {
namespace minhash {

// SVE optimized rotation-based batch MinHash computation
// Processes 8 hash functions at once using scalable vector extension
// SVE supports variable vector lengths, but we process 8 x 64-bit elements to match the interface
// Note: perm_a, perm_b, and sig pointers should already point to the correct offset
inline void
linear_and_find_min_batch8_sve(const uint64_t* base,
                               size_t shingle_count,
                               const uint64_t* perm_a,
                               const uint64_t* perm_b,
                               uint32_t* sig) {
    // Get vector length in 64-bit elements
    const uint64_t vl = svcntd();

    // Check if we have enough vector length for batch processing
    if (vl >= 8) {
        // Can process all 8 elements at once
        svbool_t pg8 = svwhilelt_b64(0UL, 8UL);

        svuint64_t vec_a = svld1_u64(pg8, perm_a);
        svuint64_t vec_b = svld1_u64(pg8, perm_b);

        // Load current signature values (32-bit) and extend to 64-bit
        svbool_t pg8_32 = svwhilelt_b32(0UL, 8UL);
        svuint32_t sig_32 = svld1_u32(pg8_32, sig);
        svuint64_t vec_min = svunpklo_u64(svunpklo_u32(sig_32));

        // For upper 4 elements
        svuint64_t vec_min_hi = svunpkhi_u64(svunpklo_u32(sig_32));

        svuint64_t vec_mersenne = svdup_u64(MERSENNE_PRIME);
        svuint64_t vec_mask = svdup_u64(MAX_HASH_MASK);
        size_t j = 0;

        // Process base hashes in chunks that fit in SVE vectors
        uint64_t chunk_size = vl;

        for (; j + chunk_size <= shingle_count; j += chunk_size) {
            // Prefetch next block
            if (j + chunk_size * 2 <= shingle_count) {
                svprfd(pg8, &base[j + chunk_size], SV_PLDL1KEEP);
            }

            svbool_t pg_chunk = svwhilelt_b64(0UL, chunk_size);
            svuint64_t vec_hashes = svld1_u64(pg_chunk, &base[j]);

            // Process each of 8 hash functions against loaded base hashes
            // Use rotation to align each hash function with base hashes
            for (int rot = 0; rot < 8 && rot < chunk_size; rot++) {
                // Extract rotated hash value for this iteration
                svuint64_t vec_hash =
                    svext_u64(vec_hashes, vec_hashes, rot % chunk_size);

                // Broadcast to match vector size
                for (int k = 0; k < 8; k++) {
                    uint64_t hash_val =
                        svlastb_u64(svwhilelt_b64(k, k + 1), vec_hash);
                    svuint64_t vec_hash_broadcast = svdup_u64(hash_val);

                    // Compute: (a * hash + b) % MERSENNE_PRIME & MAX_HASH_MASK
                    // SVE doesn't have direct 64-bit multiply, use scalar multiply in vector
                    svuint64_t vec_result =
                        svmul_u64_z(pg8, vec_a, vec_hash_broadcast);
                    vec_result = svadd_u64_z(pg8, vec_result, vec_b);

                    // Fast Mersenne modulo using Barrett reduction
                    svuint64_t low_bits =
                        svand_u64_z(pg8, vec_result, vec_mersenne);
                    svuint64_t high_bits = svlsr_n_u64_z(pg8, vec_result, 61);
                    vec_result = svadd_u64_z(pg8, low_bits, high_bits);

                    // Conditional subtraction: if result >= MERSENNE_PRIME, subtract it
                    svbool_t cmp = svcmpge_u64(pg8, vec_result, vec_mersenne);
                    vec_result = svsub_u64_m(cmp, vec_result, vec_mersenne);

                    // Mask to 32-bit
                    vec_result = svand_u64_z(pg8, vec_result, vec_mask);

                    // Update minimum - process lower and upper parts separately
                    svbool_t pg4 = svwhilelt_b64(0UL, 4UL);
                    svuint64_t vec_result_lo =
                        svext_u64(vec_result, vec_result, 0);
                    svuint64_t vec_result_hi =
                        svext_u64(vec_result, vec_result, 4);

                    vec_min = svmin_u64_m(pg4, vec_min, vec_result_lo);
                    vec_min_hi = svmin_u64_m(pg4, vec_min_hi, vec_result_hi);
                }
            }
        }

        // Process remaining base hashes one at a time
        for (; j < shingle_count; j++) {
            svuint64_t vec_hash = svdup_u64(base[j]);

            svuint64_t vec_result = svmul_u64_z(pg8, vec_a, vec_hash);
            vec_result = svadd_u64_z(pg8, vec_result, vec_b);

            svuint64_t low_bits = svand_u64_z(pg8, vec_result, vec_mersenne);
            svuint64_t high_bits = svlsr_n_u64_z(pg8, vec_result, 61);
            vec_result = svadd_u64_z(pg8, low_bits, high_bits);

            svbool_t cmp = svcmpge_u64(pg8, vec_result, vec_mersenne);
            vec_result = svsub_u64_m(cmp, vec_result, vec_mersenne);

            vec_result = svand_u64_z(pg8, vec_result, vec_mask);

            // Update minimum
            svbool_t pg4 = svwhilelt_b64(0UL, 4UL);
            svuint64_t vec_result_lo = svext_u64(vec_result, vec_result, 0);
            svuint64_t vec_result_hi = svext_u64(vec_result, vec_result, 4);

            vec_min = svmin_u64_m(pg4, vec_min, vec_result_lo);
            vec_min_hi = svmin_u64_m(pg4, vec_min_hi, vec_result_hi);
        }

        // Store results back to signature (convert 64-bit to 32-bit)
        svbool_t pg4_32 = svwhilelt_b32(0UL, 4UL);
        svuint32_t result_lo_32 = svuzp1_u32(svreinterpret_u32_u64(vec_min),
                                             svreinterpret_u32_u64(vec_min));
        svuint32_t result_hi_32 = svuzp1_u32(svreinterpret_u32_u64(vec_min_hi),
                                             svreinterpret_u32_u64(vec_min_hi));

        svst1_u32(pg4_32, sig, result_lo_32);
        svst1_u32(pg4_32, &sig[4], result_hi_32);
    } else {
        // Fallback: process in smaller batches if vector length is insufficient
        // Process 4 elements at a time using two passes
        for (int batch = 0; batch < 2; batch++) {
            int32_t offset = batch * 4;
            svbool_t pg4 = svwhilelt_b64(0UL, 4UL);

            svuint64_t vec_a = svld1_u64(pg4, &perm_a[offset]);
            svuint64_t vec_b = svld1_u64(pg4, &perm_b[offset]);

            svbool_t pg4_32 = svwhilelt_b32(0UL, 4UL);
            svuint32_t sig_32 = svld1_u32(pg4_32, &sig[offset]);
            svuint64_t vec_min = svunpklo_u64(svunpklo_u32(sig_32));

            svuint64_t vec_mersenne = svdup_u64(MERSENNE_PRIME);
            svuint64_t vec_mask = svdup_u64(MAX_HASH_MASK);

            for (size_t j = 0; j < shingle_count; j++) {
                svuint64_t vec_hash = svdup_u64(base[j]);

                svuint64_t vec_result = svmul_u64_z(pg4, vec_a, vec_hash);
                vec_result = svadd_u64_z(pg4, vec_result, vec_b);

                svuint64_t low_bits =
                    svand_u64_z(pg4, vec_result, vec_mersenne);
                svuint64_t high_bits = svlsr_n_u64_z(pg4, vec_result, 61);
                vec_result = svadd_u64_z(pg4, low_bits, high_bits);

                svbool_t cmp = svcmpge_u64(pg4, vec_result, vec_mersenne);
                vec_result = svsub_u64_m(cmp, vec_result, vec_mersenne);

                vec_result = svand_u64_z(pg4, vec_result, vec_mask);
                vec_min = svmin_u64_m(pg4, vec_min, vec_result);
            }

            // Store results
            svuint32_t result_32 = svuzp1_u32(svreinterpret_u32_u64(vec_min),
                                              svreinterpret_u32_u64(vec_min));
            svst1_u32(pg4_32, &sig[offset], result_32);
        }
    }
}

// SVE optimized linear_and_find_min for single hash function
// Uses scalable vector extension for variable-width SIMD
inline uint32_t
linear_and_find_min_sve(const uint64_t* base,
                        size_t shingle_count,
                        const uint64_t perm_a,
                        const uint64_t perm_b) {
    // Get vector length in 64-bit elements
    const uint64_t vl = svcntd();

    svuint64_t vec_a = svdup_u64(perm_a);
    svuint64_t vec_b = svdup_u64(perm_b);
    svuint64_t vec_mersenne = svdup_u64(MERSENNE_PRIME);
    svuint64_t vec_mask = svdup_u64(MAX_HASH_MASK);

    // Initialize minimum to max value
    svuint64_t vec_min = svdup_u64(MAX_HASH_32);

    size_t j = 0;

    // Process chunks that fit in vector length
    for (; j + vl <= shingle_count; j += vl) {
        // Create predicate for full vector
        svbool_t pg = svwhilelt_b64(0UL, vl);

        // Load base hashes
        svuint64_t vec_hashes = svld1_u64(pg, &base[j]);

        // Compute: (a * hash + b) % MERSENNE_PRIME & MAX_HASH_MASK
        svuint64_t vec_result = svmul_u64_z(pg, vec_a, vec_hashes);
        vec_result = svadd_u64_z(pg, vec_result, vec_b);

        // Fast Mersenne modulo using Barrett reduction
        svuint64_t low_bits = svand_u64_z(pg, vec_result, vec_mersenne);
        svuint64_t high_bits = svlsr_n_u64_z(pg, vec_result, 61);
        vec_result = svadd_u64_z(pg, low_bits, high_bits);

        // Conditional subtraction
        svbool_t cmp = svcmpge_u64(pg, vec_result, vec_mersenne);
        vec_result = svsub_u64_m(cmp, vec_result, vec_mersenne);

        // Mask to 32-bit
        vec_result = svand_u64_z(pg, vec_result, vec_mask);

        // Update minimum
        vec_min = svmin_u64_m(pg, vec_min, vec_result);
    }

    // Horizontal minimum reduction
    svbool_t pg_all = svptrue_b64();
    uint64_t min_val = svminv_u64(pg_all, vec_min);

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
        if (permuted_hash < (uint32_t)min_val) {
            min_val = permuted_hash;
        }
    }

    return (uint32_t)min_val;
}

}  // namespace minhash
}  // namespace milvus
