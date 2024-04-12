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

// ARM NEON implementation

#pragma once

#include <arm_neon.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "neon-decl.h"

#include "bitset/common.h"

namespace milvus {
namespace bitset {
namespace detail {
namespace arm {
namespace neon {

namespace {

// this function is missing somewhy
inline uint64x2_t
vmvnq_u64(const uint64x2_t value) {
    const uint64x2_t m1 = vreinterpretq_u64_u32(vdupq_n_u32(0xFFFFFFFF));
    return veorq_u64(value, m1);
}

// draft: movemask functions from sse2neon library.
// todo: can this be made better?

// todo: optimize
inline uint8_t
movemask(const uint8x8_t cmp) {
    static const int8_t shifts[8] = {0, 1, 2, 3, 4, 5, 6, 7};
    // shift right by 7, leaving 1 bit
    const uint8x8_t sh = vshr_n_u8(cmp, 7);
    // load shifts
    const int8x8_t shifts_v = vld1_s8(shifts);
    // shift each of 8 lanes with 1 bit values differently
    const uint8x8_t shifted_bits = vshl_u8(sh, shifts_v);
    // horizontal sum of bits on different positions
    return vaddv_u8(shifted_bits);
}

// todo: optimize
// https://lemire.me/blog/2017/07/10/pruning-spaces-faster-on-arm-processors-with-vector-table-lookups/ (?)
inline uint16_t
movemask(const uint8x16_t cmp) {
    uint16x8_t high_bits = vreinterpretq_u16_u8(vshrq_n_u8(cmp, 7));
    uint32x4_t paired16 =
        vreinterpretq_u32_u16(vsraq_n_u16(high_bits, high_bits, 7));
    uint64x2_t paired32 =
        vreinterpretq_u64_u32(vsraq_n_u32(paired16, paired16, 14));
    uint8x16_t paired64 =
        vreinterpretq_u8_u64(vsraq_n_u64(paired32, paired32, 28));
    return vgetq_lane_u8(paired64, 0) | ((int)vgetq_lane_u8(paired64, 8) << 8);
}

// todo: optimize
inline uint32_t
movemask(const uint8x16x2_t cmp) {
    return (uint32_t)(movemask(cmp.val[0])) |
           ((uint32_t)(movemask(cmp.val[1])) << 16);
}

// todo: optimize
inline uint8_t
movemask(const uint16x8_t cmp) {
    static const int16_t shifts[8] = {0, 1, 2, 3, 4, 5, 6, 7};
    // shift right by 15, leaving 1 bit
    const uint16x8_t sh = vshrq_n_u16(cmp, 15);
    // load shifts
    const int16x8_t shifts_v = vld1q_s16(shifts);
    // shift each of 8 lanes with 1 bit values differently
    const uint16x8_t shifted_bits = vshlq_u16(sh, shifts_v);
    // horizontal sum of bits on different positions
    return vaddvq_u16(shifted_bits);
}

// todo: optimize
inline uint16_t
movemask(const uint16x8x2_t cmp) {
    return (uint16_t)(movemask(cmp.val[0])) |
           ((uint16_t)(movemask(cmp.val[1])) << 8);
}

// todo: optimize
inline uint32_t
movemask(const uint32x4_t cmp) {
    static const int32_t shifts[4] = {0, 1, 2, 3};
    // shift right by 31, leaving 1 bit
    const uint32x4_t sh = vshrq_n_u32(cmp, 31);
    // load shifts
    const int32x4_t shifts_v = vld1q_s32(shifts);
    // shift each of 4 lanes with 1 bit values differently
    const uint32x4_t shifted_bits = vshlq_u32(sh, shifts_v);
    // horizontal sum of bits on different positions
    return vaddvq_u32(shifted_bits);
}

// todo: optimize
inline uint32_t
movemask(const uint32x4x2_t cmp) {
    return movemask(cmp.val[0]) | (movemask(cmp.val[1]) << 4);
}

// todo: optimize
inline uint8_t
movemask(const uint64x2_t cmp) {
    // shift right by 63, leaving 1 bit
    const uint64x2_t sh = vshrq_n_u64(cmp, 63);
    return vgetq_lane_u64(sh, 0) | (vgetq_lane_u64(sh, 1) << 1);
}

// todo: optimize
inline uint8_t
movemask(const uint64x2x4_t cmp) {
    return movemask(cmp.val[0]) | (movemask(cmp.val[1]) << 2) |
           (movemask(cmp.val[2]) << 4) | (movemask(cmp.val[3]) << 6);
}

//
template <CompareOpType Op>
struct CmpHelper {};

template <>
struct CmpHelper<CompareOpType::EQ> {
    static inline uint8x8_t
    compare(const int8x8_t a, const int8x8_t b) {
        return vceq_s8(a, b);
    }

    static inline uint8x16x2_t
    compare(const int8x16x2_t a, const int8x16x2_t b) {
        return {vceqq_s8(a.val[0], b.val[0]), vceqq_s8(a.val[1], b.val[1])};
    }

    static inline uint16x8_t
    compare(const int16x8_t a, const int16x8_t b) {
        return vceqq_s16(a, b);
    }

    static inline uint16x8x2_t
    compare(const int16x8x2_t a, const int16x8x2_t b) {
        return {vceqq_s16(a.val[0], b.val[0]), vceqq_s16(a.val[1], b.val[1])};
    }

    static inline uint32x4x2_t
    compare(const int32x4x2_t a, const int32x4x2_t b) {
        return {vceqq_s32(a.val[0], b.val[0]), vceqq_s32(a.val[1], b.val[1])};
    }

    static inline uint64x2x4_t
    compare(const int64x2x4_t a, const int64x2x4_t b) {
        return {vceqq_s64(a.val[0], b.val[0]),
                vceqq_s64(a.val[1], b.val[1]),
                vceqq_s64(a.val[2], b.val[2]),
                vceqq_s64(a.val[3], b.val[3])};
    }

    static inline uint32x4x2_t
    compare(const float32x4x2_t a, const float32x4x2_t b) {
        return {vceqq_f32(a.val[0], b.val[0]), vceqq_f32(a.val[1], b.val[1])};
    }

    static inline uint64x2x4_t
    compare(const float64x2x4_t a, const float64x2x4_t b) {
        return {vceqq_f64(a.val[0], b.val[0]),
                vceqq_f64(a.val[1], b.val[1]),
                vceqq_f64(a.val[2], b.val[2]),
                vceqq_f64(a.val[3], b.val[3])};
    }
};

template <>
struct CmpHelper<CompareOpType::GE> {
    static inline uint8x8_t
    compare(const int8x8_t a, const int8x8_t b) {
        return vcge_s8(a, b);
    }

    static inline uint8x16x2_t
    compare(const int8x16x2_t a, const int8x16x2_t b) {
        return {vcgeq_s8(a.val[0], b.val[0]), vcgeq_s8(a.val[1], b.val[1])};
    }

    static inline uint16x8_t
    compare(const int16x8_t a, const int16x8_t b) {
        return vcgeq_s16(a, b);
    }

    static inline uint16x8x2_t
    compare(const int16x8x2_t a, const int16x8x2_t b) {
        return {vcgeq_s16(a.val[0], b.val[0]), vcgeq_s16(a.val[1], b.val[1])};
    }

    static inline uint32x4x2_t
    compare(const int32x4x2_t a, const int32x4x2_t b) {
        return {vcgeq_s32(a.val[0], b.val[0]), vcgeq_s32(a.val[1], b.val[1])};
    }

    static inline uint64x2x4_t
    compare(const int64x2x4_t a, const int64x2x4_t b) {
        return {vcgeq_s64(a.val[0], b.val[0]),
                vcgeq_s64(a.val[1], b.val[1]),
                vcgeq_s64(a.val[2], b.val[2]),
                vcgeq_s64(a.val[3], b.val[3])};
    }

    static inline uint32x4x2_t
    compare(const float32x4x2_t a, const float32x4x2_t b) {
        return {vcgeq_f32(a.val[0], b.val[0]), vcgeq_f32(a.val[1], b.val[1])};
    }

    static inline uint64x2x4_t
    compare(const float64x2x4_t a, const float64x2x4_t b) {
        return {vcgeq_f64(a.val[0], b.val[0]),
                vcgeq_f64(a.val[1], b.val[1]),
                vcgeq_f64(a.val[2], b.val[2]),
                vcgeq_f64(a.val[3], b.val[3])};
    }
};

template <>
struct CmpHelper<CompareOpType::GT> {
    static inline uint8x8_t
    compare(const int8x8_t a, const int8x8_t b) {
        return vcgt_s8(a, b);
    }

    static inline uint8x16x2_t
    compare(const int8x16x2_t a, const int8x16x2_t b) {
        return {vcgtq_s8(a.val[0], b.val[0]), vcgtq_s8(a.val[1], b.val[1])};
    }

    static inline uint16x8_t
    compare(const int16x8_t a, const int16x8_t b) {
        return vcgtq_s16(a, b);
    }

    static inline uint16x8x2_t
    compare(const int16x8x2_t a, const int16x8x2_t b) {
        return {vcgtq_s16(a.val[0], b.val[0]), vcgtq_s16(a.val[1], b.val[1])};
    }

    static inline uint32x4x2_t
    compare(const int32x4x2_t a, const int32x4x2_t b) {
        return {vcgtq_s32(a.val[0], b.val[0]), vcgtq_s32(a.val[1], b.val[1])};
    }

    static inline uint64x2x4_t
    compare(const int64x2x4_t a, const int64x2x4_t b) {
        return {vcgtq_s64(a.val[0], b.val[0]),
                vcgtq_s64(a.val[1], b.val[1]),
                vcgtq_s64(a.val[2], b.val[2]),
                vcgtq_s64(a.val[3], b.val[3])};
    }

    static inline uint32x4x2_t
    compare(const float32x4x2_t a, const float32x4x2_t b) {
        return {vcgtq_f32(a.val[0], b.val[0]), vcgtq_f32(a.val[1], b.val[1])};
    }

    static inline uint64x2x4_t
    compare(const float64x2x4_t a, const float64x2x4_t b) {
        return {vcgtq_f64(a.val[0], b.val[0]),
                vcgtq_f64(a.val[1], b.val[1]),
                vcgtq_f64(a.val[2], b.val[2]),
                vcgtq_f64(a.val[3], b.val[3])};
    }
};

template <>
struct CmpHelper<CompareOpType::LE> {
    static inline uint8x8_t
    compare(const int8x8_t a, const int8x8_t b) {
        return vcle_s8(a, b);
    }

    static inline uint8x16x2_t
    compare(const int8x16x2_t a, const int8x16x2_t b) {
        return {vcleq_s8(a.val[0], b.val[0]), vcleq_s8(a.val[1], b.val[1])};
    }

    static inline uint16x8_t
    compare(const int16x8_t a, const int16x8_t b) {
        return vcleq_s16(a, b);
    }

    static inline uint16x8x2_t
    compare(const int16x8x2_t a, const int16x8x2_t b) {
        return {vcleq_s16(a.val[0], b.val[0]), vcleq_s16(a.val[1], b.val[1])};
    }

    static inline uint32x4x2_t
    compare(const int32x4x2_t a, const int32x4x2_t b) {
        return {vcleq_s32(a.val[0], b.val[0]), vcleq_s32(a.val[1], b.val[1])};
    }

    static inline uint64x2x4_t
    compare(const int64x2x4_t a, const int64x2x4_t b) {
        return {vcleq_s64(a.val[0], b.val[0]),
                vcleq_s64(a.val[1], b.val[1]),
                vcleq_s64(a.val[2], b.val[2]),
                vcleq_s64(a.val[3], b.val[3])};
    }

    static inline uint32x4x2_t
    compare(const float32x4x2_t a, const float32x4x2_t b) {
        return {vcleq_f32(a.val[0], b.val[0]), vcleq_f32(a.val[1], b.val[1])};
    }

    static inline uint64x2x4_t
    compare(const float64x2x4_t a, const float64x2x4_t b) {
        return {vcleq_f64(a.val[0], b.val[0]),
                vcleq_f64(a.val[1], b.val[1]),
                vcleq_f64(a.val[2], b.val[2]),
                vcleq_f64(a.val[3], b.val[3])};
    }
};

template <>
struct CmpHelper<CompareOpType::LT> {
    static inline uint8x8_t
    compare(const int8x8_t a, const int8x8_t b) {
        return vclt_s8(a, b);
    }

    static inline uint8x16x2_t
    compare(const int8x16x2_t a, const int8x16x2_t b) {
        return {vcltq_s8(a.val[0], b.val[0]), vcltq_s8(a.val[1], b.val[1])};
    }

    static inline uint16x8_t
    compare(const int16x8_t a, const int16x8_t b) {
        return vcltq_s16(a, b);
    }

    static inline uint16x8x2_t
    compare(const int16x8x2_t a, const int16x8x2_t b) {
        return {vcltq_s16(a.val[0], b.val[0]), vcltq_s16(a.val[1], b.val[1])};
    }

    static inline uint32x4x2_t
    compare(const int32x4x2_t a, const int32x4x2_t b) {
        return {vcltq_s32(a.val[0], b.val[0]), vcltq_s32(a.val[1], b.val[1])};
    }

    static inline uint64x2x4_t
    compare(const int64x2x4_t a, const int64x2x4_t b) {
        return {vcltq_s64(a.val[0], b.val[0]),
                vcltq_s64(a.val[1], b.val[1]),
                vcltq_s64(a.val[2], b.val[2]),
                vcltq_s64(a.val[3], b.val[3])};
    }

    static inline uint32x4x2_t
    compare(const float32x4x2_t a, const float32x4x2_t b) {
        return {vcltq_f32(a.val[0], b.val[0]), vcltq_f32(a.val[1], b.val[1])};
    }

    static inline uint64x2x4_t
    compare(const float64x2x4_t a, const float64x2x4_t b) {
        return {vcltq_f64(a.val[0], b.val[0]),
                vcltq_f64(a.val[1], b.val[1]),
                vcltq_f64(a.val[2], b.val[2]),
                vcltq_f64(a.val[3], b.val[3])};
    }
};

template <>
struct CmpHelper<CompareOpType::NE> {
    static inline uint8x8_t
    compare(const int8x8_t a, const int8x8_t b) {
        return vmvn_u8(vceq_s8(a, b));
    }

    static inline uint8x16x2_t
    compare(const int8x16x2_t a, const int8x16x2_t b) {
        return {vmvnq_u8(vceqq_s8(a.val[0], b.val[0])),
                vmvnq_u8(vceqq_s8(a.val[1], b.val[1]))};
    }

    static inline uint16x8_t
    compare(const int16x8_t a, const int16x8_t b) {
        return vmvnq_u16(vceqq_s16(a, b));
    }

    static inline uint16x8x2_t
    compare(const int16x8x2_t a, const int16x8x2_t b) {
        return {vmvnq_u16(vceqq_s16(a.val[0], b.val[0])),
                vmvnq_u16(vceqq_s16(a.val[1], b.val[1]))};
    }

    static inline uint32x4x2_t
    compare(const int32x4x2_t a, const int32x4x2_t b) {
        return {vmvnq_u32(vceqq_s32(a.val[0], b.val[0])),
                vmvnq_u32(vceqq_s32(a.val[1], b.val[1]))};
    }

    static inline uint64x2x4_t
    compare(const int64x2x4_t a, const int64x2x4_t b) {
        return {vmvnq_u64(vceqq_s64(a.val[0], b.val[0])),
                vmvnq_u64(vceqq_s64(a.val[1], b.val[1])),
                vmvnq_u64(vceqq_s64(a.val[2], b.val[2])),
                vmvnq_u64(vceqq_s64(a.val[3], b.val[3]))};
    }

    static inline uint32x4x2_t
    compare(const float32x4x2_t a, const float32x4x2_t b) {
        return {vmvnq_u32(vceqq_f32(a.val[0], b.val[0])),
                vmvnq_u32(vceqq_f32(a.val[1], b.val[1]))};
    }

    static inline uint64x2x4_t
    compare(const float64x2x4_t a, const float64x2x4_t b) {
        return {vmvnq_u64(vceqq_f64(a.val[0], b.val[0])),
                vmvnq_u64(vceqq_f64(a.val[1], b.val[1])),
                vmvnq_u64(vceqq_f64(a.val[2], b.val[2])),
                vmvnq_u64(vceqq_f64(a.val[3], b.val[3]))};
    }
};

}  // namespace

///////////////////////////////////////////////////////////////////////////

//
template <CompareOpType Op>
bool
OpCompareValImpl<int8_t, Op>::op_compare_val(uint8_t* const __restrict res_u8,
                                             const int8_t* const __restrict src,
                                             const size_t size,
                                             const int8_t& val) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint32_t* const __restrict res_u32 = reinterpret_cast<uint32_t*>(res_u8);
    const int8x16x2_t target = {vdupq_n_s8(val), vdupq_n_s8(val)};

    // todo: aligned reads & writes

    const size_t size32 = (size / 32) * 32;
    for (size_t i = 0; i < size32; i += 32) {
        const int8x16x2_t v0 = {vld1q_s8(src + i), vld1q_s8(src + i + 16)};
        const uint8x16x2_t cmp = CmpHelper<Op>::compare(v0, target);
        const uint32_t mmask = movemask(cmp);

        res_u32[i / 32] = mmask;
    }

    for (size_t i = size32; i < size; i += 8) {
        const int8x8_t v0 = vld1_s8(src + i);
        const uint8x8_t cmp = CmpHelper<Op>::compare(v0, vdup_n_s8(val));
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareValImpl<int16_t, Op>::op_compare_val(
    uint8_t* const __restrict res_u8,
    const int16_t* const __restrict src,
    const size_t size,
    const int16_t& val) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint16_t* const __restrict res_u16 = reinterpret_cast<uint16_t*>(res_u8);
    const int16x8x2_t target = {vdupq_n_s16(val), vdupq_n_s16(val)};

    // todo: aligned reads & writes

    const size_t size16 = (size / 16) * 16;
    for (size_t i = 0; i < size16; i += 16) {
        const int16x8x2_t v0 = {vld1q_s16(src + i), vld1q_s16(src + i + 8)};
        const uint16x8x2_t cmp = CmpHelper<Op>::compare(v0, target);
        const uint16_t mmask = movemask(cmp);

        res_u16[i / 16] = mmask;
    }

    if (size16 != size) {
        // 8 elements to process
        const int16x8_t v0 = vld1q_s16(src + size16);
        const uint16x8_t cmp = CmpHelper<Op>::compare(v0, target.val[0]);
        const uint8_t mmask = movemask(cmp);

        res_u8[size16 / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareValImpl<int32_t, Op>::op_compare_val(
    uint8_t* const __restrict res_u8,
    const int32_t* const __restrict src,
    const size_t size,
    const int32_t& val) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const int32x4x2_t target = {vdupq_n_s32(val), vdupq_n_s32(val)};

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const int32x4x2_t v0 = {vld1q_s32(src + i), vld1q_s32(src + i + 4)};
        const uint32x4x2_t cmp = CmpHelper<Op>::compare(v0, target);
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareValImpl<int64_t, Op>::op_compare_val(
    uint8_t* const __restrict res_u8,
    const int64_t* const __restrict src,
    const size_t size,
    const int64_t& val) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const int64x2x4_t target = {
        vdupq_n_s64(val), vdupq_n_s64(val), vdupq_n_s64(val), vdupq_n_s64(val)};

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const int64x2x4_t v0 = {vld1q_s64(src + i),
                                vld1q_s64(src + i + 2),
                                vld1q_s64(src + i + 4),
                                vld1q_s64(src + i + 6)};
        const uint64x2x4_t cmp = CmpHelper<Op>::compare(v0, target);
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareValImpl<float, Op>::op_compare_val(uint8_t* const __restrict res_u8,
                                            const float* const __restrict src,
                                            const size_t size,
                                            const float& val) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const float32x4x2_t target = {vdupq_n_f32(val), vdupq_n_f32(val)};

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const float32x4x2_t v0 = {vld1q_f32(src + i), vld1q_f32(src + i + 4)};
        const uint32x4x2_t cmp = CmpHelper<Op>::compare(v0, target);
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareValImpl<double, Op>::op_compare_val(uint8_t* const __restrict res_u8,
                                             const double* const __restrict src,
                                             const size_t size,
                                             const double& val) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const float64x2x4_t target = {
        vdupq_n_f64(val), vdupq_n_f64(val), vdupq_n_f64(val), vdupq_n_f64(val)};

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const float64x2x4_t v0 = {vld1q_f64(src + i),
                                  vld1q_f64(src + i + 2),
                                  vld1q_f64(src + i + 4),
                                  vld1q_f64(src + i + 6)};
        const uint64x2x4_t cmp = CmpHelper<Op>::compare(v0, target);
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

///////////////////////////////////////////////////////////////////////////

//
template <CompareOpType Op>
bool
OpCompareColumnImpl<int8_t, int8_t, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const int8_t* const __restrict left,
    const int8_t* const __restrict right,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint32_t* const __restrict res_u32 = reinterpret_cast<uint32_t*>(res_u8);

    // todo: aligned reads & writes

    const size_t size32 = (size / 32) * 32;
    for (size_t i = 0; i < size32; i += 32) {
        const int8x16x2_t v0l = {vld1q_s8(left + i), vld1q_s8(left + i + 16)};
        const int8x16x2_t v0r = {vld1q_s8(right + i), vld1q_s8(right + i + 16)};
        const uint8x16x2_t cmp = CmpHelper<Op>::compare(v0l, v0r);
        const uint32_t mmask = movemask(cmp);

        res_u32[i / 32] = mmask;
    }

    for (size_t i = size32; i < size; i += 8) {
        const int8x8_t v0l = vld1_s8(left + i);
        const int8x8_t v0r = vld1_s8(right + i);
        const uint8x8_t cmp = CmpHelper<Op>::compare(v0l, v0r);
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<int16_t, int16_t, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const int16_t* const __restrict left,
    const int16_t* const __restrict right,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint16_t* const __restrict res_u16 = reinterpret_cast<uint16_t*>(res_u8);

    // todo: aligned reads & writes

    const size_t size16 = (size / 16) * 16;
    for (size_t i = 0; i < size16; i += 16) {
        const int16x8x2_t v0l = {vld1q_s16(left + i), vld1q_s16(left + i + 8)};
        const int16x8x2_t v0r = {vld1q_s16(right + i),
                                 vld1q_s16(right + i + 8)};
        const uint16x8x2_t cmp = CmpHelper<Op>::compare(v0l, v0r);
        const uint16_t mmask = movemask(cmp);

        res_u16[i / 16] = mmask;
    }

    if (size16 != size) {
        // 8 elements to process
        const int16x8_t v0l = vld1q_s16(left + size16);
        const int16x8_t v0r = vld1q_s16(right + size16);
        const uint16x8_t cmp = CmpHelper<Op>::compare(v0l, v0r);
        const uint8_t mmask = movemask(cmp);

        res_u8[size16 / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<int32_t, int32_t, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const int32_t* const __restrict left,
    const int32_t* const __restrict right,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const int32x4x2_t v0l = {vld1q_s32(left + i), vld1q_s32(left + i + 4)};
        const int32x4x2_t v0r = {vld1q_s32(right + i),
                                 vld1q_s32(right + i + 4)};
        const uint32x4x2_t cmp = CmpHelper<Op>::compare(v0l, v0r);
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<int64_t, int64_t, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const int64_t* const __restrict left,
    const int64_t* const __restrict right,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const int64x2x4_t v0l = {vld1q_s64(left + i),
                                 vld1q_s64(left + i + 2),
                                 vld1q_s64(left + i + 4),
                                 vld1q_s64(left + i + 6)};
        const int64x2x4_t v0r = {vld1q_s64(right + i),
                                 vld1q_s64(right + i + 2),
                                 vld1q_s64(right + i + 4),
                                 vld1q_s64(right + i + 6)};
        const uint64x2x4_t cmp = CmpHelper<Op>::compare(v0l, v0r);
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<float, float, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const float* const __restrict left,
    const float* const __restrict right,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const float32x4x2_t v0l = {vld1q_f32(left + i),
                                   vld1q_f32(left + i + 4)};
        const float32x4x2_t v0r = {vld1q_f32(right + i),
                                   vld1q_f32(right + i + 4)};
        const uint32x4x2_t cmp = CmpHelper<Op>::compare(v0l, v0r);
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<double, double, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const double* const __restrict left,
    const double* const __restrict right,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const float64x2x4_t v0l = {vld1q_f64(left + i),
                                   vld1q_f64(left + i + 2),
                                   vld1q_f64(left + i + 4),
                                   vld1q_f64(left + i + 6)};
        const float64x2x4_t v0r = {vld1q_f64(right + i),
                                   vld1q_f64(right + i + 2),
                                   vld1q_f64(right + i + 4),
                                   vld1q_f64(right + i + 6)};
        const uint64x2x4_t cmp = CmpHelper<Op>::compare(v0l, v0r);
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

///////////////////////////////////////////////////////////////////////////

//
template <RangeType Op>
bool
OpWithinRangeColumnImpl<int8_t, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const int8_t* const __restrict lower,
    const int8_t* const __restrict upper,
    const int8_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint32_t* const __restrict res_u32 = reinterpret_cast<uint32_t*>(res_u8);

    // todo: aligned reads & writes

    const size_t size32 = (size / 32) * 32;
    for (size_t i = 0; i < size32; i += 32) {
        const int8x16x2_t v0l = {vld1q_s8(lower + i), vld1q_s8(lower + i + 16)};
        const int8x16x2_t v0u = {vld1q_s8(upper + i), vld1q_s8(upper + i + 16)};
        const int8x16x2_t v0v = {vld1q_s8(values + i),
                                 vld1q_s8(values + i + 16)};
        const uint8x16x2_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const uint8x16x2_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const uint8x16x2_t cmp = {vandq_u8(cmp0l.val[0], cmp0u.val[0]),
                                  vandq_u8(cmp0l.val[1], cmp0u.val[1])};
        const uint32_t mmask = movemask(cmp);

        res_u32[i / 32] = mmask;
    }

    for (size_t i = size32; i < size; i += 8) {
        const int8x8_t v0l = vld1_s8(lower + i);
        const int8x8_t v0u = vld1_s8(upper + i);
        const int8x8_t v0v = vld1_s8(values + i);
        const uint8x8_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const uint8x8_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const uint8x8_t cmp = vand_u8(cmp0l, cmp0u);
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<int16_t, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const int16_t* const __restrict lower,
    const int16_t* const __restrict upper,
    const int16_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint16_t* const __restrict res_u16 = reinterpret_cast<uint16_t*>(res_u8);

    // todo: aligned reads & writes

    const size_t size16 = (size / 16) * 16;
    for (size_t i = 0; i < size16; i += 16) {
        const int16x8x2_t v0l = {vld1q_s16(lower + i),
                                 vld1q_s16(lower + i + 8)};
        const int16x8x2_t v0u = {vld1q_s16(upper + i),
                                 vld1q_s16(upper + i + 8)};
        const int16x8x2_t v0v = {vld1q_s16(values + i),
                                 vld1q_s16(values + i + 8)};
        const uint16x8x2_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const uint16x8x2_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const uint16x8x2_t cmp = {vandq_u16(cmp0l.val[0], cmp0u.val[0]),
                                  vandq_u16(cmp0l.val[1], cmp0u.val[1])};
        const uint16_t mmask = movemask(cmp);

        res_u16[i / 16] = mmask;
    }

    if (size16 != size) {
        // 8 elements to process
        const int16x8_t v0l = vld1q_s16(lower + size16);
        const int16x8_t v0u = vld1q_s16(upper + size16);
        const int16x8_t v0v = vld1q_s16(values + size16);
        const uint16x8_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const uint16x8_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const uint16x8_t cmp = vandq_u16(cmp0l, cmp0u);
        const uint8_t mmask = movemask(cmp);

        res_u8[size16 / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<int32_t, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const int32_t* const __restrict lower,
    const int32_t* const __restrict upper,
    const int32_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const int32x4x2_t v0l = {vld1q_s32(lower + i),
                                 vld1q_s32(lower + i + 4)};
        const int32x4x2_t v0u = {vld1q_s32(upper + i),
                                 vld1q_s32(upper + i + 4)};
        const int32x4x2_t v0v = {vld1q_s32(values + i),
                                 vld1q_s32(values + i + 4)};
        const uint32x4x2_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const uint32x4x2_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const uint32x4x2_t cmp = {vandq_u32(cmp0l.val[0], cmp0u.val[0]),
                                  vandq_u32(cmp0l.val[1], cmp0u.val[1])};
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<int64_t, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const int64_t* const __restrict lower,
    const int64_t* const __restrict upper,
    const int64_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const int64x2x4_t v0l = {vld1q_s64(lower + i),
                                 vld1q_s64(lower + i + 2),
                                 vld1q_s64(lower + i + 4),
                                 vld1q_s64(lower + i + 6)};
        const int64x2x4_t v0u = {vld1q_s64(upper + i),
                                 vld1q_s64(upper + i + 2),
                                 vld1q_s64(upper + i + 4),
                                 vld1q_s64(upper + i + 6)};
        const int64x2x4_t v0v = {vld1q_s64(values + i),
                                 vld1q_s64(values + i + 2),
                                 vld1q_s64(values + i + 4),
                                 vld1q_s64(values + i + 6)};
        const uint64x2x4_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const uint64x2x4_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const uint64x2x4_t cmp = {vandq_u64(cmp0l.val[0], cmp0u.val[0]),
                                  vandq_u64(cmp0l.val[1], cmp0u.val[1]),
                                  vandq_u64(cmp0l.val[2], cmp0u.val[2]),
                                  vandq_u64(cmp0l.val[3], cmp0u.val[3])};
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<float, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const float* const __restrict lower,
    const float* const __restrict upper,
    const float* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const float32x4x2_t v0l = {vld1q_f32(lower + i),
                                   vld1q_f32(lower + i + 4)};
        const float32x4x2_t v0u = {vld1q_f32(upper + i),
                                   vld1q_f32(upper + i + 4)};
        const float32x4x2_t v0v = {vld1q_f32(values + i),
                                   vld1q_f32(values + i + 4)};
        const uint32x4x2_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const uint32x4x2_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const uint32x4x2_t cmp = {vandq_u32(cmp0l.val[0], cmp0u.val[0]),
                                  vandq_u32(cmp0l.val[1], cmp0u.val[1])};
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<double, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const double* const __restrict lower,
    const double* const __restrict upper,
    const double* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const float64x2x4_t v0l = {vld1q_f64(lower + i),
                                   vld1q_f64(lower + i + 2),
                                   vld1q_f64(lower + i + 4),
                                   vld1q_f64(lower + i + 6)};
        const float64x2x4_t v0u = {vld1q_f64(upper + i),
                                   vld1q_f64(upper + i + 2),
                                   vld1q_f64(upper + i + 4),
                                   vld1q_f64(upper + i + 6)};
        const float64x2x4_t v0v = {vld1q_f64(values + i),
                                   vld1q_f64(values + i + 2),
                                   vld1q_f64(values + i + 4),
                                   vld1q_f64(values + i + 6)};
        const uint64x2x4_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const uint64x2x4_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const uint64x2x4_t cmp = {vandq_u64(cmp0l.val[0], cmp0u.val[0]),
                                  vandq_u64(cmp0l.val[1], cmp0u.val[1]),
                                  vandq_u64(cmp0l.val[2], cmp0u.val[2]),
                                  vandq_u64(cmp0l.val[3], cmp0u.val[3])};
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

///////////////////////////////////////////////////////////////////////////

//
template <RangeType Op>
bool
OpWithinRangeValImpl<int8_t, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const int8_t& lower,
    const int8_t& upper,
    const int8_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const int8x16x2_t lower_v = {vdupq_n_s8(lower), vdupq_n_s8(lower)};
    const int8x16x2_t upper_v = {vdupq_n_s8(upper), vdupq_n_s8(upper)};
    uint32_t* const __restrict res_u32 = reinterpret_cast<uint32_t*>(res_u8);

    // todo: aligned reads & writes

    const size_t size32 = (size / 32) * 32;
    for (size_t i = 0; i < size32; i += 32) {
        const int8x16x2_t v0v = {vld1q_s8(values + i),
                                 vld1q_s8(values + i + 16)};
        const uint8x16x2_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(lower_v, v0v);
        const uint8x16x2_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, upper_v);
        const uint8x16x2_t cmp = {vandq_u8(cmp0l.val[0], cmp0u.val[0]),
                                  vandq_u8(cmp0l.val[1], cmp0u.val[1])};
        const uint32_t mmask = movemask(cmp);

        res_u32[i / 32] = mmask;
    }

    for (size_t i = size32; i < size; i += 8) {
        const int8x8_t lower_v1 = vdup_n_s8(lower);
        const int8x8_t upper_v1 = vdup_n_s8(upper);
        const int8x8_t v0v = vld1_s8(values + i);
        const uint8x8_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(lower_v1, v0v);
        const uint8x8_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, upper_v1);
        const uint8x8_t cmp = vand_u8(cmp0l, cmp0u);
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeValImpl<int16_t, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const int16_t& lower,
    const int16_t& upper,
    const int16_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const int16x8x2_t lower_v = {vdupq_n_s16(lower), vdupq_n_s16(lower)};
    const int16x8x2_t upper_v = {vdupq_n_s16(upper), vdupq_n_s16(upper)};
    uint16_t* const __restrict res_u16 = reinterpret_cast<uint16_t*>(res_u8);

    // todo: aligned reads & writes

    const size_t size16 = (size / 16) * 16;
    for (size_t i = 0; i < size16; i += 16) {
        const int16x8x2_t v0v = {vld1q_s16(values + i),
                                 vld1q_s16(values + i + 8)};
        const uint16x8x2_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(lower_v, v0v);
        const uint16x8x2_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, upper_v);
        const uint16x8x2_t cmp = {vandq_u16(cmp0l.val[0], cmp0u.val[0]),
                                  vandq_u16(cmp0l.val[1], cmp0u.val[1])};
        const uint16_t mmask = movemask(cmp);

        res_u16[i / 16] = mmask;
    }

    if (size16 != size) {
        // 8 elements to process
        const int16x8_t v0v = vld1q_s16(values + size16);
        const uint16x8_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(lower_v.val[0], v0v);
        const uint16x8_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, upper_v.val[0]);
        const uint16x8_t cmp = vandq_u16(cmp0l, cmp0u);
        const uint8_t mmask = movemask(cmp);

        res_u8[size16 / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeValImpl<int32_t, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const int32_t& lower,
    const int32_t& upper,
    const int32_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const int32x4x2_t lower_v = {vdupq_n_s32(lower), vdupq_n_s32(lower)};
    const int32x4x2_t upper_v = {vdupq_n_s32(upper), vdupq_n_s32(upper)};

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const int32x4x2_t v0v = {vld1q_s32(values + i),
                                 vld1q_s32(values + i + 4)};
        const uint32x4x2_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(lower_v, v0v);
        const uint32x4x2_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, upper_v);
        const uint32x4x2_t cmp = {vandq_u32(cmp0l.val[0], cmp0u.val[0]),
                                  vandq_u32(cmp0l.val[1], cmp0u.val[1])};
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeValImpl<int64_t, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const int64_t& lower,
    const int64_t& upper,
    const int64_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const int64x2x4_t lower_v = {vdupq_n_s64(lower),
                                 vdupq_n_s64(lower),
                                 vdupq_n_s64(lower),
                                 vdupq_n_s64(lower)};
    const int64x2x4_t upper_v = {vdupq_n_s64(upper),
                                 vdupq_n_s64(upper),
                                 vdupq_n_s64(upper),
                                 vdupq_n_s64(upper)};

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const int64x2x4_t v0v = {vld1q_s64(values + i),
                                 vld1q_s64(values + i + 2),
                                 vld1q_s64(values + i + 4),
                                 vld1q_s64(values + i + 6)};
        const uint64x2x4_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(lower_v, v0v);
        const uint64x2x4_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, upper_v);
        const uint64x2x4_t cmp = {vandq_u64(cmp0l.val[0], cmp0u.val[0]),
                                  vandq_u64(cmp0l.val[1], cmp0u.val[1]),
                                  vandq_u64(cmp0l.val[2], cmp0u.val[2]),
                                  vandq_u64(cmp0l.val[3], cmp0u.val[3])};
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeValImpl<float, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const float& lower,
    const float& upper,
    const float* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const float32x4x2_t lower_v = {vdupq_n_f32(lower), vdupq_n_f32(lower)};
    const float32x4x2_t upper_v = {vdupq_n_f32(upper), vdupq_n_f32(upper)};

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const float32x4x2_t v0v = {vld1q_f32(values + i),
                                   vld1q_f32(values + i + 4)};
        const uint32x4x2_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(lower_v, v0v);
        const uint32x4x2_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, upper_v);
        const uint32x4x2_t cmp = {vandq_u32(cmp0l.val[0], cmp0u.val[0]),
                                  vandq_u32(cmp0l.val[1], cmp0u.val[1])};
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeValImpl<double, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const double& lower,
    const double& upper,
    const double* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const float64x2x4_t lower_v = {vdupq_n_f64(lower),
                                   vdupq_n_f64(lower),
                                   vdupq_n_f64(lower),
                                   vdupq_n_f64(lower)};
    const float64x2x4_t upper_v = {vdupq_n_f64(upper),
                                   vdupq_n_f64(upper),
                                   vdupq_n_f64(upper),
                                   vdupq_n_f64(upper)};

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const float64x2x4_t v0v = {vld1q_f64(values + i),
                                   vld1q_f64(values + i + 2),
                                   vld1q_f64(values + i + 4),
                                   vld1q_f64(values + i + 6)};
        const uint64x2x4_t cmp0l =
            CmpHelper<Range2Compare<Op>::lower>::compare(lower_v, v0v);
        const uint64x2x4_t cmp0u =
            CmpHelper<Range2Compare<Op>::upper>::compare(v0v, upper_v);
        const uint64x2x4_t cmp = {vandq_u64(cmp0l.val[0], cmp0u.val[0]),
                                  vandq_u64(cmp0l.val[1], cmp0u.val[1]),
                                  vandq_u64(cmp0l.val[2], cmp0u.val[2]),
                                  vandq_u64(cmp0l.val[3], cmp0u.val[3])};
        const uint8_t mmask = movemask(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

///////////////////////////////////////////////////////////////////////////

namespace {

//
template <ArithOpType AOp, CompareOpType CmpOp>
struct ArithHelperI64 {};

template <CompareOpType CmpOp>
struct ArithHelperI64<ArithOpType::Add, CmpOp> {
    static inline uint64x2x4_t
    op(const int64x2x4_t left,
       const int64x2x4_t right,
       const int64x2x4_t value) {
        // left + right == value
        const int64x2x4_t lr = {vaddq_s64(left.val[0], right.val[0]),
                                vaddq_s64(left.val[1], right.val[1]),
                                vaddq_s64(left.val[2], right.val[2]),
                                vaddq_s64(left.val[3], right.val[3])};
        return CmpHelper<CmpOp>::compare(lr, value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperI64<ArithOpType::Sub, CmpOp> {
    static inline uint64x2x4_t
    op(const int64x2x4_t left,
       const int64x2x4_t right,
       const int64x2x4_t value) {
        // left - right == value
        const int64x2x4_t lr = {vsubq_s64(left.val[0], right.val[0]),
                                vsubq_s64(left.val[1], right.val[1]),
                                vsubq_s64(left.val[2], right.val[2]),
                                vsubq_s64(left.val[3], right.val[3])};
        return CmpHelper<CmpOp>::compare(lr, value);
    }
};

// template<CompareOpType CmpOp>
// struct ArithHelperI64<ArithOpType::Mul, CmpOp> {
//     // todo draft: https://stackoverflow.com/questions/60236627/facing-problem-in-implementing-multiplication-of-64-bit-variables-using-arm-neon
//     inline int64x2_t arm_vmulq_s64(const int64x2_t a, const int64x2_t b)
//     {
//         const auto ac = vmovn_s64(a);
//         const auto pr = vmovn_s64(b);

//         const auto hi = vmulq_s32(b, vrev64q_s32(a));

//         return vmlal_u32(vshlq_n_s64(vpaddlq_u32(hi), 32), ac, pr);
//     }

//     static inline uint64x2x4_t op(const int64x2x4_t left, const int64x2x4_t right, const int64x2x4_t value) {
//         // left * right == value
//         const int64x2x4_t lr = {
//             arm_vmulq_s64(left.val[0], right.val[0]),
//             arm_vmulq_s64(left.val[1], right.val[1]),
//             arm_vmulq_s64(left.val[2], right.val[2]),
//             arm_vmulq_s64(left.val[3], right.val[3])
//         };
//         return CmpHelper<CmpOp>::compare(lr, value);
//     }
// };

//
template <ArithOpType AOp, CompareOpType CmpOp>
struct ArithHelperF32 {};

template <CompareOpType CmpOp>
struct ArithHelperF32<ArithOpType::Add, CmpOp> {
    static inline uint32x4x2_t
    op(const float32x4x2_t left,
       const float32x4x2_t right,
       const float32x4x2_t value) {
        // left + right == value
        const float32x4x2_t lr = {vaddq_f32(left.val[0], right.val[0]),
                                  vaddq_f32(left.val[1], right.val[1])};
        return CmpHelper<CmpOp>::compare(lr, value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF32<ArithOpType::Sub, CmpOp> {
    static inline uint32x4x2_t
    op(const float32x4x2_t left,
       const float32x4x2_t right,
       const float32x4x2_t value) {
        // left - right == value
        const float32x4x2_t lr = {vsubq_f32(left.val[0], right.val[0]),
                                  vsubq_f32(left.val[1], right.val[1])};
        return CmpHelper<CmpOp>::compare(lr, value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF32<ArithOpType::Mul, CmpOp> {
    static inline uint32x4x2_t
    op(const float32x4x2_t left,
       const float32x4x2_t right,
       const float32x4x2_t value) {
        // left * right == value
        const float32x4x2_t lr = {vmulq_f32(left.val[0], right.val[0]),
                                  vmulq_f32(left.val[1], right.val[1])};
        return CmpHelper<CmpOp>::compare(lr, value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF32<ArithOpType::Div, CmpOp> {
    static inline uint32x4x2_t
    op(const float32x4x2_t left,
       const float32x4x2_t right,
       const float32x4x2_t value) {
        // left == right * value
        const float32x4x2_t rv = {vmulq_f32(right.val[0], value.val[0]),
                                  vmulq_f32(right.val[1], value.val[1])};
        return CmpHelper<CmpOp>::compare(left, rv);
    }
};

//
template <ArithOpType AOp, CompareOpType CmpOp>
struct ArithHelperF64 {};

template <CompareOpType CmpOp>
struct ArithHelperF64<ArithOpType::Add, CmpOp> {
    static inline uint64x2x4_t
    op(const float64x2x4_t left,
       const float64x2x4_t right,
       const float64x2x4_t value) {
        // left + right == value
        const float64x2x4_t lr = {vaddq_f64(left.val[0], right.val[0]),
                                  vaddq_f64(left.val[1], right.val[1]),
                                  vaddq_f64(left.val[2], right.val[2]),
                                  vaddq_f64(left.val[3], right.val[3])};
        return CmpHelper<CmpOp>::compare(lr, value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF64<ArithOpType::Sub, CmpOp> {
    static inline uint64x2x4_t
    op(const float64x2x4_t left,
       const float64x2x4_t right,
       const float64x2x4_t value) {
        // left - right == value
        const float64x2x4_t lr = {vsubq_f64(left.val[0], right.val[0]),
                                  vsubq_f64(left.val[1], right.val[1]),
                                  vsubq_f64(left.val[2], right.val[2]),
                                  vsubq_f64(left.val[3], right.val[3])};
        return CmpHelper<CmpOp>::compare(lr, value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF64<ArithOpType::Mul, CmpOp> {
    static inline uint64x2x4_t
    op(const float64x2x4_t left,
       const float64x2x4_t right,
       const float64x2x4_t value) {
        // left * right == value
        const float64x2x4_t lr = {vmulq_f64(left.val[0], right.val[0]),
                                  vmulq_f64(left.val[1], right.val[1]),
                                  vmulq_f64(left.val[2], right.val[2]),
                                  vmulq_f64(left.val[3], right.val[3])};
        return CmpHelper<CmpOp>::compare(lr, value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF64<ArithOpType::Div, CmpOp> {
    static inline uint64x2x4_t
    op(const float64x2x4_t left,
       const float64x2x4_t right,
       const float64x2x4_t value) {
        // left == right * value
        const float64x2x4_t rv = {vmulq_f64(right.val[0], value.val[0]),
                                  vmulq_f64(right.val[1], value.val[1]),
                                  vmulq_f64(right.val[2], value.val[2]),
                                  vmulq_f64(right.val[3], value.val[3])};
        return CmpHelper<CmpOp>::compare(left, rv);
    }
};

}  // namespace

// todo: Mul, Div, Mod

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<int8_t, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const int8_t* const __restrict src,
    const ArithHighPrecisionType<int8_t>& right_operand,
    const ArithHighPrecisionType<int8_t>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Mul || AOp == ArithOpType::Div ||
                  AOp == ArithOpType::Mod) {
        return false;
    } else {
        // the restriction of the API
        assert((size % 8) == 0);
        static_assert(std::is_same_v<int64_t, ArithHighPrecisionType<int64_t>>);

        //
        const int64x2x4_t right_v = {vdupq_n_s64(right_operand),
                                     vdupq_n_s64(right_operand),
                                     vdupq_n_s64(right_operand),
                                     vdupq_n_s64(right_operand)};
        const int64x2x4_t value_v = {vdupq_n_s64(value),
                                     vdupq_n_s64(value),
                                     vdupq_n_s64(value),
                                     vdupq_n_s64(value)};

        // todo: aligned reads & writes

        const size_t size8 = (size / 8) * 8;
        for (size_t i = 0; i < size8; i += 8) {
            const int8x8_t v0v_i8 = vld1_s8(src + i);
            const int16x8_t v0v_i16 = vmovl_s8(v0v_i8);
            const int32x4x2_t v0v_i32 = {vmovl_s16(vget_low_s16(v0v_i16)),
                                         vmovl_s16(vget_high_s16(v0v_i16))};
            const int64x2x4_t v0v_i64 = {
                vmovl_s32(vget_low_s32(v0v_i32.val[0])),
                vmovl_s32(vget_high_s32(v0v_i32.val[0])),
                vmovl_s32(vget_low_s32(v0v_i32.val[1])),
                vmovl_s32(vget_high_s32(v0v_i32.val[1]))};

            const uint64x2x4_t cmp =
                ArithHelperI64<AOp, CmpOp>::op(v0v_i64, right_v, value_v);

            const uint8_t mmask = movemask(cmp);
            res_u8[i / 8] = mmask;
        }

        return true;
    }
}

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<int16_t, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const int16_t* const __restrict src,
    const ArithHighPrecisionType<int16_t>& right_operand,
    const ArithHighPrecisionType<int16_t>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Mul || AOp == ArithOpType::Div ||
                  AOp == ArithOpType::Mod) {
        return false;
    } else {
        // the restriction of the API
        assert((size % 8) == 0);
        static_assert(std::is_same_v<int64_t, ArithHighPrecisionType<int64_t>>);

        //
        const int64x2x4_t right_v = {vdupq_n_s64(right_operand),
                                     vdupq_n_s64(right_operand),
                                     vdupq_n_s64(right_operand),
                                     vdupq_n_s64(right_operand)};
        const int64x2x4_t value_v = {vdupq_n_s64(value),
                                     vdupq_n_s64(value),
                                     vdupq_n_s64(value),
                                     vdupq_n_s64(value)};

        // todo: aligned reads & writes

        const size_t size8 = (size / 8) * 8;
        for (size_t i = 0; i < size8; i += 8) {
            const int16x8_t v0v_i16 = vld1q_s16(src + i);
            const int32x4x2_t v0v_i32 = {vmovl_s16(vget_low_s16(v0v_i16)),
                                         vmovl_s16(vget_high_s16(v0v_i16))};
            const int64x2x4_t v0v_i64 = {
                vmovl_s32(vget_low_s32(v0v_i32.val[0])),
                vmovl_s32(vget_high_s32(v0v_i32.val[0])),
                vmovl_s32(vget_low_s32(v0v_i32.val[1])),
                vmovl_s32(vget_high_s32(v0v_i32.val[1]))};

            const uint64x2x4_t cmp =
                ArithHelperI64<AOp, CmpOp>::op(v0v_i64, right_v, value_v);

            const uint8_t mmask = movemask(cmp);
            res_u8[i / 8] = mmask;
        }

        return true;
    }
}

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<int32_t, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const int32_t* const __restrict src,
    const ArithHighPrecisionType<int32_t>& right_operand,
    const ArithHighPrecisionType<int32_t>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Mul || AOp == ArithOpType::Div ||
                  AOp == ArithOpType::Mod) {
        return false;
    } else {
        // the restriction of the API
        assert((size % 8) == 0);
        static_assert(std::is_same_v<int64_t, ArithHighPrecisionType<int64_t>>);

        //
        const int64x2x4_t right_v = {vdupq_n_s64(right_operand),
                                     vdupq_n_s64(right_operand),
                                     vdupq_n_s64(right_operand),
                                     vdupq_n_s64(right_operand)};
        const int64x2x4_t value_v = {vdupq_n_s64(value),
                                     vdupq_n_s64(value),
                                     vdupq_n_s64(value),
                                     vdupq_n_s64(value)};

        // todo: aligned reads & writes

        const size_t size8 = (size / 8) * 8;
        for (size_t i = 0; i < size8; i += 8) {
            const int32x4x2_t v0v_i32 = {vld1q_s32(src + i),
                                         vld1q_s32(src + i + 4)};
            const int64x2x4_t v0v_i64 = {
                vmovl_s32(vget_low_s32(v0v_i32.val[0])),
                vmovl_s32(vget_high_s32(v0v_i32.val[0])),
                vmovl_s32(vget_low_s32(v0v_i32.val[1])),
                vmovl_s32(vget_high_s32(v0v_i32.val[1]))};

            const uint64x2x4_t cmp =
                ArithHelperI64<AOp, CmpOp>::op(v0v_i64, right_v, value_v);

            const uint8_t mmask = movemask(cmp);
            res_u8[i / 8] = mmask;
        }

        return true;
    }
}

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<int64_t, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const int64_t* const __restrict src,
    const ArithHighPrecisionType<int64_t>& right_operand,
    const ArithHighPrecisionType<int64_t>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Mul || AOp == ArithOpType::Div ||
                  AOp == ArithOpType::Mod) {
        return false;
    } else {
        // the restriction of the API
        assert((size % 8) == 0);
        static_assert(std::is_same_v<int64_t, ArithHighPrecisionType<int64_t>>);

        //
        const int64x2x4_t right_v = {vdupq_n_s64(right_operand),
                                     vdupq_n_s64(right_operand),
                                     vdupq_n_s64(right_operand),
                                     vdupq_n_s64(right_operand)};
        const int64x2x4_t value_v = {vdupq_n_s64(value),
                                     vdupq_n_s64(value),
                                     vdupq_n_s64(value),
                                     vdupq_n_s64(value)};

        // todo: aligned reads & writes

        const size_t size8 = (size / 8) * 8;
        for (size_t i = 0; i < size8; i += 8) {
            const int64x2x4_t v0v = {vld1q_s64(src + i),
                                     vld1q_s64(src + i + 2),
                                     vld1q_s64(src + i + 4),
                                     vld1q_s64(src + i + 6)};
            const uint64x2x4_t cmp =
                ArithHelperI64<AOp, CmpOp>::op(v0v, right_v, value_v);

            const uint8_t mmask = movemask(cmp);
            res_u8[i / 8] = mmask;
        }

        return true;
    }
}

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<float, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const float* const __restrict src,
    const ArithHighPrecisionType<float>& right_operand,
    const ArithHighPrecisionType<float>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Mod) {
        return false;
    } else {
        // the restriction of the API
        assert((size % 8) == 0);

        //
        const float32x4x2_t right_v = {vdupq_n_f32(right_operand),
                                       vdupq_n_f32(right_operand)};
        const float32x4x2_t value_v = {vdupq_n_f32(value), vdupq_n_f32(value)};

        // todo: aligned reads & writes

        const size_t size8 = (size / 8) * 8;
        for (size_t i = 0; i < size8; i += 8) {
            const float32x4x2_t v0v = {vld1q_f32(src + i),
                                       vld1q_f32(src + i + 4)};
            const uint32x4x2_t cmp =
                ArithHelperF32<AOp, CmpOp>::op(v0v, right_v, value_v);

            const uint8_t mmask = movemask(cmp);
            res_u8[i / 8] = mmask;
        }

        return true;
    }
}

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<double, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const double* const __restrict src,
    const ArithHighPrecisionType<double>& right_operand,
    const ArithHighPrecisionType<double>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Mod) {
        return false;
    } else {
        // the restriction of the API
        assert((size % 8) == 0);

        //
        const float64x2x4_t right_v = {vdupq_n_f64(right_operand),
                                       vdupq_n_f64(right_operand),
                                       vdupq_n_f64(right_operand),
                                       vdupq_n_f64(right_operand)};
        const float64x2x4_t value_v = {vdupq_n_f64(value),
                                       vdupq_n_f64(value),
                                       vdupq_n_f64(value),
                                       vdupq_n_f64(value)};

        // todo: aligned reads & writes

        const size_t size8 = (size / 8) * 8;
        for (size_t i = 0; i < size8; i += 8) {
            const float64x2x4_t v0v = {vld1q_f64(src + i),
                                       vld1q_f64(src + i + 2),
                                       vld1q_f64(src + i + 4),
                                       vld1q_f64(src + i + 6)};
            const uint64x2x4_t cmp =
                ArithHelperF64<AOp, CmpOp>::op(v0v, right_v, value_v);

            const uint8_t mmask = movemask(cmp);
            res_u8[i / 8] = mmask;
        }

        return true;
    }
}

///////////////////////////////////////////////////////////////////////////

}  // namespace neon
}  // namespace arm
}  // namespace detail
}  // namespace bitset
}  // namespace milvus
