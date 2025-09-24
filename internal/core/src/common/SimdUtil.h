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

#include <xsimd/xsimd.hpp>
#include "common/BitUtil.h"

namespace milvus {
template <typename T, typename A, size_t kSizeT = sizeof(T)>
struct BitMask;

template <typename T, typename A>
struct BitMask<T, A, 1> {
    static constexpr int kAllSet =
        milvus::bits::lowMask(xsimd::batch_bool<T, A>::size);

#if XSIMD_WITH_AVX2
    static int
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx2&) {
        return _mm256_movemask_epi8(mask);
    }
#endif

#if XSIMD_WITH_SSE2
    static int
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::sse2&) {
        return _mm_movemask_epi8(mask);
    }
#endif

#if XSIMD_WITH_NEON
    static int
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::neon&) {
        alignas(A::alignment()) static const int8_t kShift[] = {
            -7, -6, -5, -4, -3, -2, -1, 0, -7, -6, -5, -4, -3, -2, -1, 0};
        int8x16_t vshift = vld1q_s8(kShift);
        uint8x16_t vmask = vshlq_u8(vandq_u8(mask, vdupq_n_u8(0x80)), vshift);
        return (vaddv_u8(vget_high_u8(vmask)) << 8) |
               vaddv_u8(vget_low_u8(vmask));
    }
#endif
};

template <typename T, typename A>
struct BitMask<T, A, 2> {
    static constexpr int kAllSet =
        milvus::bits::lowMask(xsimd::batch_bool<T, A>::size);

#if XSIMD_WITH_AVX2
    static int
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx2&) {
        // There is no intrinsic for extracting high bits of a 16x16
        // vector.  Hence take every second bit of the high bits of a 32x1
        // vector.
        //
        // NOTE: TVL might have a more efficient implementation for this.
        return bits::extractBits<uint32_t>(_mm256_movemask_epi8(mask),
                                           0xAAAAAAAA);
    }
#endif

#if XSIMD_WITH_SSE2
    static int
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::sse2&) {
        return milvus::bits::extractBits<uint32_t>(_mm_movemask_epi8(mask),
                                                   0xAAAA);
    }
#endif

    static int
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::generic&) {
        return genericToBitMask(mask);
    }
};

template <typename T, typename A>
struct BitMask<T, A, 4> {
    static constexpr int kAllSet =
        milvus::bits::lowMask(xsimd::batch_bool<T, A>::size);

#if XSIMD_WITH_AVX
    static int
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx&) {
        return _mm256_movemask_ps(reinterpret_cast<__m256>(mask.data));
    }
#endif

#if XSIMD_WITH_SSE2
    static int
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::sse2&) {
        return _mm_movemask_ps(reinterpret_cast<__m128>(mask.data));
    }
#endif

    static int
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::generic&) {
        return genericToBitMask(mask);
    }
};

template <typename T, typename A>
struct BitMask<T, A, 8> {
    static constexpr int kAllSet =
        milvus::bits::lowMask(xsimd::batch_bool<T, A>::size);

#if XSIMD_WITH_AVX
    static int
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx&) {
        return _mm256_movemask_pd(reinterpret_cast<__m256d>(mask.data));
    }
#endif

#if XSIMD_WITH_SSE2
    static int
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::sse2&) {
        return _mm_movemask_pd(reinterpret_cast<__m128d>(mask.data));
    }
#endif

    static int
    toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::generic&) {
        return genericToBitMask(mask);
    }
};

template <typename T, typename A = xsimd::default_arch>
auto
toBitMask(xsimd::batch_bool<T, A> mask, const A& arch = {}) {
    return BitMask<T, A>::toBitMask(mask, arch);
}
}  // namespace milvus
