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

#include <cstdint>
#include <type_traits>
#include <xsimd/xsimd.hpp>
#include "common/BitUtil.h"

namespace milvus {

// Generic fallback implementation for toBitMask when no architecture-specific
// implementation is available
template <typename T, typename A>
uint64_t
genericToBitMask(xsimd::batch_bool<T, A> mask) {
    constexpr size_t size = xsimd::batch_bool<T, A>::size;
    uint64_t result = 0;
    auto ones = xsimd::batch<T, A>(T(1));
    auto zeros = xsimd::batch<T, A>(T(0));
    auto int_batch = xsimd::select(mask, ones, zeros);
    alignas(A::alignment()) T values[size];
    int_batch.store_aligned(values);
    for (size_t i = 0; i < size; ++i) {
        if (values[i] != T(0)) {
            result |= (uint64_t(1) << i);
        }
    }
    return result;
}

template <typename T, typename A, size_t kSizeT = sizeof(T)>
struct BitMask;

// ═══════════════════════════════════════════════════════════════════════════
// sizeof(T) == 1  (int8_t / uint8_t)
// AVX512BW: __mmask64 — already a bitmask, zero-cost extract.
// AVX2:     _mm256_movemask_epi8 → 32 bits, 1 per byte lane.
// NEON:     shift-and-add per 8-byte half.
// ═══════════════════════════════════════════════════════════════════════════
template <typename T, typename A>
struct BitMask<T, A, 1> {
    static constexpr uint64_t kAllSet =
        milvus::bits::lowMask(xsimd::batch_bool<T, A>::size);

#if XSIMD_WITH_AVX512BW
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx512bw&) {
        return static_cast<uint64_t>(mask.data);
    }
#endif

#if XSIMD_WITH_AVX2
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx2&) {
        return static_cast<uint32_t>(_mm256_movemask_epi8(mask));
    }
#endif

#if XSIMD_WITH_SSE2
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::sse2&) {
        return static_cast<uint16_t>(_mm_movemask_epi8(mask));
    }
#endif

#if XSIMD_WITH_NEON
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::neon&) {
        alignas(16) static const int8_t kShift[] = {
            -7, -6, -5, -4, -3, -2, -1, 0, -7, -6, -5, -4, -3, -2, -1, 0};
        int8x16_t vshift = vld1q_s8(kShift);
        uint8x16_t vmask = vshlq_u8(vandq_u8(mask, vdupq_n_u8(0x80)), vshift);
        return (vaddv_u8(vget_high_u8(vmask)) << 8) |
               vaddv_u8(vget_low_u8(vmask));
    }
#endif

    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::generic&) {
        return genericToBitMask(mask);
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// sizeof(T) == 2  (int16_t)
// AVX512BW: __mmask32 — direct extract.
// AVX2:     movemask_epi8 → extract every 2nd bit.
// NEON:     narrow 16→8, then shift-and-add.
// ═══════════════════════════════════════════════════════════════════════════
template <typename T, typename A>
struct BitMask<T, A, 2> {
    static constexpr uint64_t kAllSet =
        milvus::bits::lowMask(xsimd::batch_bool<T, A>::size);

#if XSIMD_WITH_AVX512BW
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx512bw&) {
        return static_cast<uint64_t>(mask.data);
    }
#endif

#if XSIMD_WITH_AVX2
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx2&) {
        return bits::extractBits<uint32_t>(_mm256_movemask_epi8(mask),
                                           0xAAAAAAAA);
    }
#endif

#if XSIMD_WITH_SSE2
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::sse2&) {
        return milvus::bits::extractBits<uint32_t>(_mm_movemask_epi8(mask),
                                                   0xAAAA);
    }
#endif

#if XSIMD_WITH_NEON
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::neon&) {
        // Narrow: top byte of each 16-bit all-1s/all-0s lane → 0xFF/0x00
        uint8x16_t raw = vreinterpretq_u8_u16(mask.data);
        uint16x8_t u16 = vreinterpretq_u16_u8(raw);
        uint8x8_t narrowed = vshrn_n_u16(u16, 8);
        // 8 bytes → 8 bits via shift-and-add
        alignas(8) static const int8_t kShift[] = {
            -7, -6, -5, -4, -3, -2, -1, 0};
        int8x8_t vshift = vld1_s8(kShift);
        uint8x8_t bits = vshl_u8(vand_u8(narrowed, vdup_n_u8(0x80)), vshift);
        return vaddv_u8(bits);
    }
#endif

    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::generic&) {
        return genericToBitMask(mask);
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// sizeof(T) == 4  (int32_t / float)
// AVX512F:  __mmask16 — direct extract (only needs Foundation, not BW).
// AVX:      _mm256_movemask_ps → 8 bits, 1 per float lane.
// NEON:     high-bit shift + positional weighting + horizontal add.
// ═══════════════════════════════════════════════════════════════════════════
template <typename T, typename A>
struct BitMask<T, A, 4> {
    static constexpr uint64_t kAllSet =
        milvus::bits::lowMask(xsimd::batch_bool<T, A>::size);

#if XSIMD_WITH_AVX512F
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx512f&) {
        return static_cast<uint64_t>(mask.data);
    }
#endif

#if XSIMD_WITH_AVX
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx&) {
        if constexpr (std::is_same_v<decltype(mask.data), __m256>) {
            return _mm256_movemask_ps(mask.data);
        } else {
            return _mm256_movemask_ps(_mm256_castsi256_ps(mask.data));
        }
    }
#endif

#if XSIMD_WITH_SSE2
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::sse2&) {
        if constexpr (std::is_same_v<decltype(mask.data), __m128>) {
            return _mm_movemask_ps(mask.data);
        } else {
            return _mm_movemask_ps(_mm_castsi128_ps(mask.data));
        }
    }
#endif

#if XSIMD_WITH_NEON
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::neon&) {
        uint8x16_t raw = vreinterpretq_u8_u32(mask.data);
        uint32x4_t u32 = vreinterpretq_u32_u8(raw);
        uint32x4_t bits = vshrq_n_u32(u32, 31);
        alignas(16) static const int32_t kShift[] = {0, 1, 2, 3};
        int32x4_t shifts = vld1q_s32(kShift);
        uint32x4_t weighted = vshlq_u32(bits, shifts);
        return vaddvq_u32(weighted);
    }
#endif

    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::generic&) {
        return genericToBitMask(mask);
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// sizeof(T) == 8  (int64_t / double)
// AVX512F:  __mmask8 — direct extract (only needs Foundation, not BW).
// AVX:      _mm256_movemask_pd → 4 bits, 1 per double lane.
// NEON:     extract high bit of each 64-bit lane (only 2 lanes).
// ═══════════════════════════════════════════════════════════════════════════
template <typename T, typename A>
struct BitMask<T, A, 8> {
    static constexpr uint64_t kAllSet =
        milvus::bits::lowMask(xsimd::batch_bool<T, A>::size);

#if XSIMD_WITH_AVX512F
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx512f&) {
        return static_cast<uint64_t>(mask.data);
    }
#endif

#if XSIMD_WITH_AVX
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx&) {
        if constexpr (std::is_same_v<decltype(mask.data), __m256d>) {
            return _mm256_movemask_pd(mask.data);
        } else {
            return _mm256_movemask_pd(_mm256_castsi256_pd(mask.data));
        }
    }
#endif

#if XSIMD_WITH_SSE2
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::sse2&) {
        if constexpr (std::is_same_v<decltype(mask.data), __m128d>) {
            return _mm_movemask_pd(mask.data);
        } else {
            return _mm_movemask_pd(_mm_castsi128_pd(mask.data));
        }
    }
#endif

#if XSIMD_WITH_NEON
    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::neon&) {
        uint8x16_t raw = vreinterpretq_u8_u64(mask.data);
        uint64x2_t u64 = vreinterpretq_u64_u8(raw);
        return (vgetq_lane_u64(u64, 0) >> 63) |
               ((vgetq_lane_u64(u64, 1) >> 63) << 1);
    }
#endif

    static uint64_t
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::generic&) {
        return genericToBitMask(mask);
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Free-function dispatcher — selects best overload for the default arch.
// ═══════════════════════════════════════════════════════════════════════════
template <typename T, typename A = xsimd::default_arch>
uint64_t
toBitMask(xsimd::batch_bool<T, A> mask, const A& arch = {}) {
    return BitMask<T, A>::toBitMask(mask, arch);
}
}  // namespace milvus
