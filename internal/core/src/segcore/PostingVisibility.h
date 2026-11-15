// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#include <immintrin.h>
#endif

namespace milvus::segcore::experimental::posting {

struct Masks {
    uint8_t visible = 0;
    uint8_t history = 0;
};

inline Masks
Scalar(const uint64_t* begins,
       const uint64_t* ends,
       uint64_t query_ts,
       size_t count = 8) {
    Masks masks;
    for (size_t i = 0; i < count; ++i) {
        if (query_ts < begins[i]) {
            masks.history |= uint8_t(1u << i);
        } else if (query_ts < ends[i] ||
                   ends[i] == std::numeric_limits<uint64_t>::max()) {
            masks.visible |= uint8_t(1u << i);
        }
    }
    return masks;
}

inline Masks
ScalarEight(const uint64_t* begins, const uint64_t* ends, uint64_t query_ts) {
    return Scalar(begins, ends, query_ts);
}

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
__attribute__((target("avx512f"))) inline Masks
Avx512(const uint64_t* begins, const uint64_t* ends, uint64_t query_ts) {
    const auto b = _mm512_loadu_si512(begins);
    const auto e = _mm512_loadu_si512(ends);
    const auto q = _mm512_set1_epi64(static_cast<int64_t>(query_ts));
    const auto infinity = _mm512_set1_epi64(-1);
    const auto started = _mm512_cmp_epu64_mask(b, q, _MM_CMPINT_LE);
    const auto unended = _mm512_cmp_epu64_mask(q, e, _MM_CMPINT_LT) |
                         _mm512_cmp_epu64_mask(e, infinity, _MM_CMPINT_EQ);
    return {static_cast<uint8_t>(started & unended),
            static_cast<uint8_t>(~started)};
}

__attribute__((target("avx2"))) inline Masks
Avx2(const uint64_t* begins, const uint64_t* ends, uint64_t query_ts) {
    Masks masks;
    const auto sign = _mm256_set1_epi64x(std::numeric_limits<int64_t>::min());
    const auto q = _mm256_xor_si256(
        _mm256_set1_epi64x(static_cast<int64_t>(query_ts)), sign);
    const auto infinity = _mm256_set1_epi64x(-1);
    for (size_t i = 0; i < 8; i += 4) {
        const auto b =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(begins + i));
        const auto e =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ends + i));
        // Flip the sign bit to preserve unsigned timestamp ordering.
        const auto future = _mm256_cmpgt_epi64(_mm256_xor_si256(b, sign), q);
        const auto unended =
            _mm256_or_si256(_mm256_cmpgt_epi64(_mm256_xor_si256(e, sign), q),
                            _mm256_cmpeq_epi64(e, infinity));
        const auto visible = _mm256_andnot_si256(future, unended);
        masks.visible |= static_cast<uint8_t>(
            _mm256_movemask_pd(_mm256_castsi256_pd(visible)) << i);
        masks.history |= static_cast<uint8_t>(
            _mm256_movemask_pd(_mm256_castsi256_pd(future)) << i);
    }
    return masks;
}
#endif

using Kernel = Masks (*)(const uint64_t*, const uint64_t*, uint64_t);

inline Kernel
SelectKernel() {
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
    if (__builtin_cpu_supports("avx512f"))
        return Avx512;
    if (__builtin_cpu_supports("avx2"))
        return Avx2;
#endif
    return ScalarEight;
}

}  // namespace milvus::segcore::experimental::posting
