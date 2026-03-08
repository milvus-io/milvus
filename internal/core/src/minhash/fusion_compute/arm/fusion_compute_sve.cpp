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

// This file is compiled with -march=armv8-a+sve flag
// It provides the SVE implementations defined in the header

#include "fusion_compute_sve.h"

namespace milvus {
namespace minhash {

// SVE optimized rotation-based batch MinHash computation
void
linear_and_find_min_batch8_sve(const uint64_t* base,
                               size_t shingle_count,
                               const uint64_t* perm_a,
                               const uint64_t* perm_b,
                               uint32_t* sig) {
    // Process each of the 8 hash functions individually using SVE, as SVE does not support
    // the same kind of batch processing as NEON due to its variable vector length.
    sig[0] = linear_and_find_min_sve(base, shingle_count, perm_a[0], perm_b[0]);
    sig[1] = linear_and_find_min_sve(base, shingle_count, perm_a[1], perm_b[1]);
    sig[2] = linear_and_find_min_sve(base, shingle_count, perm_a[2], perm_b[2]);
    sig[3] = linear_and_find_min_sve(base, shingle_count, perm_a[3], perm_b[3]);
    sig[4] = linear_and_find_min_sve(base, shingle_count, perm_a[4], perm_b[4]);
    sig[5] = linear_and_find_min_sve(base, shingle_count, perm_a[5], perm_b[5]);
    sig[6] = linear_and_find_min_sve(base, shingle_count, perm_a[6], perm_b[6]);
    sig[7] = linear_and_find_min_sve(base, shingle_count, perm_a[7], perm_b[7]);
}

// SVE optimized linear_and_find_min for single hash function
uint32_t
linear_and_find_min_sve(const uint64_t* base,
                        size_t shingle_count,
                        const uint64_t perm_a,
                        const uint64_t perm_b) {
    // Use SVE to process multiple base hashes in parallel
    svuint64_t vec_a = svdup_u64(perm_a);
    svuint64_t vec_b = svdup_u64(perm_b);
    svuint64_t vec_mersenne = svdup_u64(MERSENNE_PRIME);
    svuint64_t vec_mask = svdup_u64(MAX_HASH_MASK);
    svbool_t pg = svptrue_b64();

    uint32_t min_val = MAX_HASH_32;
    size_t j = 0;

    // Process vl base hashes at a time (SVE vector length)
    uint64_t vl = svcntd();  // Get the number of 64-bit elements per vector

    for (; j + vl <= shingle_count; j += vl) {
        svuint64_t vec_hashes = svld1(pg, &base[j]);

        // Compute: (a * hash + b) using SVE multiplication
        svuint64_t vec_result = svmul_u64_x(pg, vec_a, vec_hashes);
        vec_result = svadd_u64_x(pg, vec_result, vec_b);

        // Fast Mersenne modulo using Barrett reduction
        svuint64_t low_bits = svand_u64_x(pg, vec_result, vec_mersenne);
        svuint64_t high_bits = svlsr_n_u64_x(pg, vec_result, 61);
        vec_result = svadd_u64_x(pg, low_bits, high_bits);

        // Conditional subtraction
        svbool_t cmp = svcmpge_u64(pg, vec_result, vec_mersenne);
        vec_result = svsub_u64_m(cmp, vec_result, vec_mersenne);

        // Mask to 32-bit
        vec_result = svand_u64_x(pg, vec_result, vec_mask);

        // Horizontal minimum reduction for this batch
        uint64_t batch_min = svminv_u64(pg, vec_result);
        if ((uint32_t)batch_min < min_val) {
            min_val = (uint32_t)batch_min;
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
