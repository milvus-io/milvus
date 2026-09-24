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
#include <cstring>

#include "count_bulk.h"

#if defined(BITSET_HEADER_ONLY)
#define BITSET_COUNT_INLINE inline
#else
#define BITSET_COUNT_INLINE
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#endif
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#include <immintrin.h>
#endif

namespace milvus::bitset::detail {

// Byte-addressed: supports both uint8_t and uint64_t policies, with no
// extra alignment/padding requirement or type-punning scalar loads.
BITSET_COUNT_INLINE size_t
CountBytesScalar(const uint8_t* data, size_t bytes) {
    size_t count = 0;
    size_t i = 0;
    for (; i + sizeof(uint64_t) <= bytes; i += sizeof(uint64_t)) {
        uint64_t word;
        std::memcpy(&word, data + i, sizeof(word));
        count += __builtin_popcountll(word);
    }
    for (; i < bytes; ++i) {
        count += __builtin_popcount(data[i]);
    }
    return count;
}

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
__attribute__((target("popcnt"), noinline)) BITSET_COUNT_INLINE size_t
CountBytesPopcnt(const uint8_t* data, size_t bytes) {
    size_t a = 0, b = 0, c = 0, d = 0;
    size_t i = 0;
    for (; i + 32 <= bytes; i += 32) {
        uint64_t words[4];
        std::memcpy(words, data + i, sizeof(words));
        a += __builtin_popcountll(words[0]);
        b += __builtin_popcountll(words[1]);
        c += __builtin_popcountll(words[2]);
        d += __builtin_popcountll(words[3]);
    }
    for (; i + 8 <= bytes; i += 8) {
        uint64_t word;
        std::memcpy(&word, data + i, sizeof(word));
        a += __builtin_popcountll(word);
    }
    for (; i < bytes; ++i) a += __builtin_popcount(data[i]);
    return a + b + c + d;
}

__attribute__((target("avx512f,avx512vl,avx512vpopcntdq"), noinline))
BITSET_COUNT_INLINE size_t
CountBytesVpopcnt256(const uint8_t* data, size_t bytes) {
    size_t prefix = (32 - (reinterpret_cast<uintptr_t>(data) & 31)) & 31;
    if (prefix > bytes)
        prefix = bytes;
    const size_t leading = CountBytesScalar(data, prefix);
    data += prefix;
    bytes -= prefix;
    auto a = _mm256_setzero_si256();
    auto b = a;
    auto c = a;
    auto d = a;
    size_t i = 0;
    for (; i + 128 <= bytes; i += 128) {
        a = _mm256_add_epi64(a,
                             _mm256_popcnt_epi64(_mm256_loadu_si256(
                                 reinterpret_cast<const __m256i*>(data + i))));
        b = _mm256_add_epi64(
            b,
            _mm256_popcnt_epi64(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(data + i + 32))));
        c = _mm256_add_epi64(
            c,
            _mm256_popcnt_epi64(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(data + i + 64))));
        d = _mm256_add_epi64(
            d,
            _mm256_popcnt_epi64(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(data + i + 96))));
    }
    a = _mm256_add_epi64(_mm256_add_epi64(a, b), _mm256_add_epi64(c, d));
    for (; i + 32 <= bytes; i += 32) {
        a = _mm256_add_epi64(a,
                             _mm256_popcnt_epi64(_mm256_loadu_si256(
                                 reinterpret_cast<const __m256i*>(data + i))));
    }
    uint64_t lanes[4];
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(lanes), a);
    return leading + lanes[0] + lanes[1] + lanes[2] + lanes[3] +
           CountBytesScalar(data + i, bytes - i);
}
#endif

// Isolate SIMD prologues and CPU dispatch from short range_count calls.
__attribute__((noinline)) BITSET_COUNT_INLINE size_t
CountBytesBulk(const uint8_t* data, size_t bytes) {
#if defined(__aarch64__) && defined(__ARM_NEON)
    size_t prefix = (16 - (reinterpret_cast<uintptr_t>(data) & 15)) & 15;
    if (prefix > bytes)
        prefix = bytes;
    size_t total = CountBytesScalar(data, prefix);
    data += prefix;
    bytes -= prefix;
    size_t i = 0;
    for (; i + 1024 <= bytes; i += 1024) {
        auto a = vdupq_n_u16(0);
        auto b = a;
        auto c = a;
        auto d = a;
        for (size_t j = 0; j < 1024; j += 64) {
            a = vpadalq_u8(a, vcntq_u8(vld1q_u8(data + i + j)));
            b = vpadalq_u8(b, vcntq_u8(vld1q_u8(data + i + j + 16)));
            c = vpadalq_u8(c, vcntq_u8(vld1q_u8(data + i + j + 32)));
            d = vpadalq_u8(d, vcntq_u8(vld1q_u8(data + i + j + 48)));
        }
        // At most 1024 in any combined uint16 lane per 1 KiB chunk.
        total += vaddlvq_u16(vaddq_u16(vaddq_u16(a, b), vaddq_u16(c, d)));
    }
    auto tail = vdupq_n_u16(0);
    auto b = tail;
    auto c = tail;
    auto d = tail;
    for (; i + 64 <= bytes; i += 64) {
        tail = vpadalq_u8(tail, vcntq_u8(vld1q_u8(data + i)));
        b = vpadalq_u8(b, vcntq_u8(vld1q_u8(data + i + 16)));
        c = vpadalq_u8(c, vcntq_u8(vld1q_u8(data + i + 32)));
        d = vpadalq_u8(d, vcntq_u8(vld1q_u8(data + i + 48)));
    }
    tail = vaddq_u16(vaddq_u16(tail, b), vaddq_u16(c, d));
    for (; i + 16 <= bytes; i += 16) {
        tail = vpadalq_u8(tail, vcntq_u8(vld1q_u8(data + i)));
    }
    return total + vaddlvq_u16(tail) + CountBytesScalar(data + i, bytes - i);
#elif defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
    static const bool use_vpopcnt = [] {
        __builtin_cpu_init();
        return __builtin_cpu_supports("avx512f") &&
               __builtin_cpu_supports("avx512vl") &&
               __builtin_cpu_supports("avx512vpopcntdq");
    }();
    if (use_vpopcnt) {
        return CountBytesVpopcnt256(data, bytes);
    }
    if (__builtin_cpu_supports("popcnt")) {
        return CountBytesPopcnt(data, bytes);
    }
    return CountBytesScalar(data, bytes);
#else
    return CountBytesScalar(data, bytes);
#endif
}

}  // namespace milvus::bitset::detail

#undef BITSET_COUNT_INLINE
