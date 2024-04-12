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

// ARM SVE implementation

#pragma once

#include <arm_sve.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "sve-decl.h"

#include "bitset/common.h"

// #include <stdio.h>

namespace milvus {
namespace bitset {
namespace detail {
namespace arm {
namespace sve {

namespace {

//
constexpr size_t MAX_SVE_WIDTH = 2048;

constexpr uint8_t SVE_LANES_8[MAX_SVE_WIDTH / 8] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
    0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
    0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
    0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B,
    0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
    0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F,

    0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A,
    0x4B, 0x4C, 0x4D, 0x4E, 0x4F, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55,
    0x56, 0x57, 0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F, 0x60,
    0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6A, 0x6B,
    0x6C, 0x6D, 0x6E, 0x6F, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76,
    0x77, 0x78, 0x79, 0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F,

    0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A,
    0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95,
    0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0,
    0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB,
    0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6,
    0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF,

    0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA,
    0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5,
    0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0,
    0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB,
    0xEC, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6,
    0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF};

constexpr uint16_t SVE_LANES_16[MAX_SVE_WIDTH / 16] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
    0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
    0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
    0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B,
    0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
    0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F,

    0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A,
    0x4B, 0x4C, 0x4D, 0x4E, 0x4F, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55,
    0x56, 0x57, 0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F, 0x60,
    0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6A, 0x6B,
    0x6C, 0x6D, 0x6E, 0x6F, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76,
    0x77, 0x78, 0x79, 0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F};

constexpr uint32_t SVE_LANES_32[MAX_SVE_WIDTH / 32] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
    0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
    0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
    0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B,
    0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
    0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F};

constexpr uint64_t SVE_LANES_64[MAX_SVE_WIDTH / 64] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
    0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
    0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F};

/*
// debugging facilities

//
void print_svbool_t(const svbool_t value) {
    // 2048 bits, 256 bytes => 256 bits bitmask, 32 bytes
    uint8_t v[MAX_SVE_WIDTH / 64];
    *((svbool_t*)v) = value;

    const size_t sve_width = svcntb();
    for (size_t i = 0; i < sve_width / 8; i++) {
        printf("%d ", int(v[i]));
    }
    printf("\n");
}

//
void print_svuint8_t(const svuint8_t value) {
    uint8_t v[MAX_SVE_WIDTH / 8];
    *((svuint8_t*)v) = value;

    const size_t sve_width = svcntb();
    for (size_t i = 0; i < sve_width; i++) {
        printf("%d ", int(v[i]));
    }
    printf("\n");
}

*/

///////////////////////////////////////////////////////////////////////////

// todo: replace with pext whenever available

// generate 16-bit bitmask from 8 serialized 16-bit svbool_t values
void
write_bitmask_16_8x(uint8_t* const __restrict res_u8,
                    const svbool_t pred_op,
                    const svbool_t pred_write,
                    const uint8_t* const __restrict pred_buf) {
    // perform parallel pext
    // 2048b -> 32 bytes mask -> 256 bytes total, 128 uint16_t values
    // 512b -> 8 bytes mask -> 64 bytes total, 32 uint16_t values
    // 256b -> 4 bytes mask -> 32 bytes total, 16 uint16_t values
    // 128b -> 2 bytes mask -> 16 bytes total, 8 uint16_t values

    // this code does reduction of 16-bit 0b0A0B0C0D0E0F0G0H words into
    //   uint8_t values 0bABCDEFGH, then writes ones to the memory

    // we need to operate in uint8_t
    const svuint8_t mask_8b = svld1_u8(pred_op, pred_buf);

    const svuint8_t mask_04_8b = svand_n_u8_z(pred_op, mask_8b, 0x01);
    const svuint8_t mask_15_8b = svand_n_u8_z(pred_op, mask_8b, 0x04);
    const svuint8_t mask_15s_8b = svlsr_n_u8_z(pred_op, mask_15_8b, 1);
    const svuint8_t mask_26_8b = svand_n_u8_z(pred_op, mask_8b, 0x10);
    const svuint8_t mask_26s_8b = svlsr_n_u8_z(pred_op, mask_26_8b, 2);
    const svuint8_t mask_37_8b = svand_n_u8_z(pred_op, mask_8b, 0x40);
    const svuint8_t mask_37s_8b = svlsr_n_u8_z(pred_op, mask_37_8b, 3);

    const svuint8_t mask_0347_8b = svorr_u8_z(pred_op, mask_04_8b, mask_37s_8b);
    const svuint8_t mask_1256_8b =
        svorr_u8_z(pred_op, mask_15s_8b, mask_26s_8b);
    const svuint8_t mask_cmb_8b =
        svorr_u8_z(pred_op, mask_0347_8b, mask_1256_8b);

    //
    const svuint16_t shifts_16b = svdup_u16(0x0400UL);
    const svuint8_t shifts_8b = svreinterpret_u8_u16(shifts_16b);
    const svuint8_t shifted_8b_m0 = svlsl_u8_z(pred_op, mask_cmb_8b, shifts_8b);

    const svuint8_t zero_8b = svdup_n_u8(0);

    const svuint8_t shifted_8b_m3 =
        svorr_u8_z(pred_op,
                   svuzp1_u8(shifted_8b_m0, zero_8b),
                   svuzp2_u8(shifted_8b_m0, zero_8b));

    // write a finished bitmask
    svst1_u8(pred_write, res_u8, shifted_8b_m3);
}

// generate 32-bit bitmask from 8 serialized 32-bit svbool_t values
void
write_bitmask_32_8x(uint8_t* const __restrict res_u8,
                    const svbool_t pred_op,
                    const svbool_t pred_write,
                    const uint8_t* const __restrict pred_buf) {
    // perform parallel pext
    // 2048b -> 32 bytes mask -> 256 bytes total, 64 uint32_t values
    // 512b -> 8 bytes mask -> 64 bytes total, 16 uint32_t values
    // 256b -> 4 bytes mask -> 32 bytes total, 8 uint32_t values
    // 128b -> 2 bytes mask -> 16 bytes total, 4 uint32_t values

    // this code does reduction of 32-bit 0b000A000B000C000D... dwords into
    //   uint8_t values 0bABCDEFGH, then writes ones to the memory

    // we need to operate in uint8_t
    const svuint8_t mask_8b = svld1_u8(pred_op, pred_buf);

    const svuint8_t mask_024_8b = svand_n_u8_z(pred_op, mask_8b, 0x01);
    const svuint8_t mask_135s_8b = svlsr_n_u8_z(pred_op, mask_8b, 3);
    const svuint8_t mask_cmb_8b =
        svorr_u8_z(pred_op, mask_024_8b, mask_135s_8b);

    //
    const svuint32_t shifts_32b = svdup_u32(0x06040200UL);
    const svuint8_t shifts_8b = svreinterpret_u8_u32(shifts_32b);
    const svuint8_t shifted_8b_m0 = svlsl_u8_z(pred_op, mask_cmb_8b, shifts_8b);

    const svuint8_t zero_8b = svdup_n_u8(0);

    const svuint8_t shifted_8b_m2 =
        svorr_u8_z(pred_op,
                   svuzp1_u8(shifted_8b_m0, zero_8b),
                   svuzp2_u8(shifted_8b_m0, zero_8b));
    const svuint8_t shifted_8b_m3 =
        svorr_u8_z(pred_op,
                   svuzp1_u8(shifted_8b_m2, zero_8b),
                   svuzp2_u8(shifted_8b_m2, zero_8b));

    // write a finished bitmask
    svst1_u8(pred_write, res_u8, shifted_8b_m3);
}

// generate 64-bit bitmask from 8 serialized 64-bit svbool_t values
void
write_bitmask_64_8x(uint8_t* const __restrict res_u8,
                    const svbool_t pred_op,
                    const svbool_t pred_write,
                    const uint8_t* const __restrict pred_buf) {
    // perform parallel pext
    // 2048b -> 32 bytes mask -> 256 bytes total, 32 uint64_t values
    // 512b -> 8 bytes mask -> 64 bytes total, 4 uint64_t values
    // 256b -> 4 bytes mask -> 32 bytes total, 2 uint64_t values
    // 128b -> 2 bytes mask -> 16 bytes total, 1 uint64_t values

    // this code does reduction of 64-bit 0b0000000A0000000B... qwords into
    //   uint8_t values 0bABCDEFGH, then writes ones to the memory

    // we need to operate in uint8_t
    const svuint8_t mask_8b = svld1_u8(pred_op, pred_buf);
    const svuint64_t shifts_64b = svdup_u64(0x706050403020100ULL);
    const svuint8_t shifts_8b = svreinterpret_u8_u64(shifts_64b);
    const svuint8_t shifted_8b_m0 = svlsl_u8_z(pred_op, mask_8b, shifts_8b);

    const svuint8_t zero_8b = svdup_n_u8(0);

    const svuint8_t shifted_8b_m1 =
        svorr_u8_z(pred_op,
                   svuzp1_u8(shifted_8b_m0, zero_8b),
                   svuzp2_u8(shifted_8b_m0, zero_8b));
    const svuint8_t shifted_8b_m2 =
        svorr_u8_z(pred_op,
                   svuzp1_u8(shifted_8b_m1, zero_8b),
                   svuzp2_u8(shifted_8b_m1, zero_8b));
    const svuint8_t shifted_8b_m3 =
        svorr_u8_z(pred_op,
                   svuzp1_u8(shifted_8b_m2, zero_8b),
                   svuzp2_u8(shifted_8b_m2, zero_8b));

    // write a finished bitmask
    svst1_u8(pred_write, res_u8, shifted_8b_m3);
}

///////////////////////////////////////////////////////////////////////////

//
inline svbool_t
get_pred_op_8(const size_t n_elements) {
    const svbool_t pred_all_8 = svptrue_b8();
    const svuint8_t lanes_8 = svld1_u8(pred_all_8, SVE_LANES_8);
    const svuint8_t leftovers_op = svdup_n_u8(n_elements);
    const svbool_t pred_op = svcmpgt_u8(pred_all_8, leftovers_op, lanes_8);
    return pred_op;
}

//
inline svbool_t
get_pred_op_16(const size_t n_elements) {
    const svbool_t pred_all_16 = svptrue_b16();
    const svuint16_t lanes_16 = svld1_u16(pred_all_16, SVE_LANES_16);
    const svuint16_t leftovers_op = svdup_n_u16(n_elements);
    const svbool_t pred_op = svcmpgt_u16(pred_all_16, leftovers_op, lanes_16);
    return pred_op;
}

//
inline svbool_t
get_pred_op_32(const size_t n_elements) {
    const svbool_t pred_all_32 = svptrue_b32();
    const svuint32_t lanes_32 = svld1_u32(pred_all_32, SVE_LANES_32);
    const svuint32_t leftovers_op = svdup_n_u32(n_elements);
    const svbool_t pred_op = svcmpgt_u32(pred_all_32, leftovers_op, lanes_32);
    return pred_op;
}

//
inline svbool_t
get_pred_op_64(const size_t n_elements) {
    const svbool_t pred_all_64 = svptrue_b64();
    const svuint64_t lanes_64 = svld1_u64(pred_all_64, SVE_LANES_64);
    const svuint64_t leftovers_op = svdup_n_u64(n_elements);
    const svbool_t pred_op = svcmpgt_u64(pred_all_64, leftovers_op, lanes_64);
    return pred_op;
}

//
template <typename T>
struct GetPredHelper {};

template <>
struct GetPredHelper<int8_t> {
    inline static svbool_t
    get_pred_op(const size_t n_elements) {
        return get_pred_op_8(n_elements);
    }
};

template <>
struct GetPredHelper<int16_t> {
    inline static svbool_t
    get_pred_op(const size_t n_elements) {
        return get_pred_op_16(n_elements);
    }
};

template <>
struct GetPredHelper<int32_t> {
    inline static svbool_t
    get_pred_op(const size_t n_elements) {
        return get_pred_op_32(n_elements);
    }
};

template <>
struct GetPredHelper<int64_t> {
    inline static svbool_t
    get_pred_op(const size_t n_elements) {
        return get_pred_op_64(n_elements);
    }
};

template <>
struct GetPredHelper<float> {
    inline static svbool_t
    get_pred_op(const size_t n_elements) {
        return get_pred_op_32(n_elements);
    }
};

template <>
struct GetPredHelper<double> {
    inline static svbool_t
    get_pred_op(const size_t n_elements) {
        return get_pred_op_64(n_elements);
    }
};

template <typename T>
inline svbool_t
get_pred_op(const size_t n_elements) {
    return GetPredHelper<T>::get_pred_op(n_elements);
}

///////////////////////////////////////////////////////////////////////////

//
template <CompareOpType Op>
struct CmpHelper {};

template <>
struct CmpHelper<CompareOpType::EQ> {
    static inline svbool_t
    compare(const svbool_t pred, const svint8_t a, const svint8_t b) {
        return svcmpeq_s8(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint16_t a, const svint16_t b) {
        return svcmpeq_s16(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint32_t a, const svint32_t b) {
        return svcmpeq_s32(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint64_t a, const svint64_t b) {
        return svcmpeq_s64(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svfloat32_t a, const svfloat32_t b) {
        return svcmpeq_f32(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svfloat64_t a, const svfloat64_t b) {
        return svcmpeq_f64(pred, a, b);
    }
};

template <>
struct CmpHelper<CompareOpType::GE> {
    static inline svbool_t
    compare(const svbool_t pred, const svint8_t a, const svint8_t b) {
        return svcmpge_s8(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint16_t a, const svint16_t b) {
        return svcmpge_s16(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint32_t a, const svint32_t b) {
        return svcmpge_s32(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint64_t a, const svint64_t b) {
        return svcmpge_s64(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svfloat32_t a, const svfloat32_t b) {
        return svcmpge_f32(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svfloat64_t a, const svfloat64_t b) {
        return svcmpge_f64(pred, a, b);
    }
};

template <>
struct CmpHelper<CompareOpType::GT> {
    static inline svbool_t
    compare(const svbool_t pred, const svint8_t a, const svint8_t b) {
        return svcmpgt_s8(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint16_t a, const svint16_t b) {
        return svcmpgt_s16(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint32_t a, const svint32_t b) {
        return svcmpgt_s32(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint64_t a, const svint64_t b) {
        return svcmpgt_s64(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svfloat32_t a, const svfloat32_t b) {
        return svcmpgt_f32(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svfloat64_t a, const svfloat64_t b) {
        return svcmpgt_f64(pred, a, b);
    }
};

template <>
struct CmpHelper<CompareOpType::LE> {
    static inline svbool_t
    compare(const svbool_t pred, const svint8_t a, const svint8_t b) {
        return svcmple_s8(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint16_t a, const svint16_t b) {
        return svcmple_s16(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint32_t a, const svint32_t b) {
        return svcmple_s32(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint64_t a, const svint64_t b) {
        return svcmple_s64(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svfloat32_t a, const svfloat32_t b) {
        return svcmple_f32(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svfloat64_t a, const svfloat64_t b) {
        return svcmple_f64(pred, a, b);
    }
};

template <>
struct CmpHelper<CompareOpType::LT> {
    static inline svbool_t
    compare(const svbool_t pred, const svint8_t a, const svint8_t b) {
        return svcmplt_s8(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint16_t a, const svint16_t b) {
        return svcmplt_s16(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint32_t a, const svint32_t b) {
        return svcmplt_s32(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint64_t a, const svint64_t b) {
        return svcmplt_s64(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svfloat32_t a, const svfloat32_t b) {
        return svcmplt_f32(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svfloat64_t a, const svfloat64_t b) {
        return svcmplt_f64(pred, a, b);
    }
};

template <>
struct CmpHelper<CompareOpType::NE> {
    static inline svbool_t
    compare(const svbool_t pred, const svint8_t a, const svint8_t b) {
        return svcmpne_s8(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint16_t a, const svint16_t b) {
        return svcmpne_s16(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint32_t a, const svint32_t b) {
        return svcmpne_s32(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svint64_t a, const svint64_t b) {
        return svcmpne_s64(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svfloat32_t a, const svfloat32_t b) {
        return svcmpne_f32(pred, a, b);
    }

    static inline svbool_t
    compare(const svbool_t pred, const svfloat64_t a, const svfloat64_t b) {
        return svcmpne_f64(pred, a, b);
    }
};

///////////////////////////////////////////////////////////////////////////

template <typename T>
struct SVEVector {};

template <>
struct SVEVector<int8_t> {
    using data_type = int8_t;
    using sve_type = svint8_t;

    // measured in the number of elements that an SVE register can hold
    static inline size_t
    width() {
        return svcntb();
    }

    static inline svbool_t
    pred_all() {
        return svptrue_b8();
    }

    inline static sve_type
    set1(const data_type value) {
        return svdup_n_s8(value);
    }

    inline static sve_type
    load(const svbool_t pred, const data_type* value) {
        return svld1_s8(pred, value);
    }
};

template <>
struct SVEVector<int16_t> {
    using data_type = int16_t;
    using sve_type = svint16_t;

    // measured in the number of elements that an SVE register can hold
    static inline size_t
    width() {
        return svcnth();
    }

    static inline svbool_t
    pred_all() {
        return svptrue_b16();
    }

    inline static sve_type
    set1(const data_type value) {
        return svdup_n_s16(value);
    }

    inline static sve_type
    load(const svbool_t pred, const data_type* value) {
        return svld1_s16(pred, value);
    }
};

template <>
struct SVEVector<int32_t> {
    using data_type = int32_t;
    using sve_type = svint32_t;

    // measured in the number of elements that an SVE register can hold
    static inline size_t
    width() {
        return svcntw();
    }

    static inline svbool_t
    pred_all() {
        return svptrue_b32();
    }

    inline static sve_type
    set1(const data_type value) {
        return svdup_n_s32(value);
    }

    inline static sve_type
    load(const svbool_t pred, const data_type* value) {
        return svld1_s32(pred, value);
    }
};

template <>
struct SVEVector<int64_t> {
    using data_type = int64_t;
    using sve_type = svint64_t;

    // measured in the number of elements that an SVE register can hold
    static inline size_t
    width() {
        return svcntd();
    }

    static inline svbool_t
    pred_all() {
        return svptrue_b64();
    }

    inline static sve_type
    set1(const data_type value) {
        return svdup_n_s64(value);
    }

    inline static sve_type
    load(const svbool_t pred, const data_type* value) {
        return svld1_s64(pred, value);
    }
};

template <>
struct SVEVector<float> {
    using data_type = float;
    using sve_type = svfloat32_t;

    // measured in the number of elements that an SVE register can hold
    static inline size_t
    width() {
        return svcntw();
    }

    static inline svbool_t
    pred_all() {
        return svptrue_b32();
    }

    inline static sve_type
    set1(const data_type value) {
        return svdup_n_f32(value);
    }

    inline static sve_type
    load(const svbool_t pred, const data_type* value) {
        return svld1_f32(pred, value);
    }
};

template <>
struct SVEVector<double> {
    using data_type = double;
    using sve_type = svfloat64_t;

    // measured in the number of elements that an SVE register can hold
    static inline size_t
    width() {
        return svcntd();
    }

    static inline svbool_t
    pred_all() {
        return svptrue_b64();
    }

    inline static sve_type
    set1(const data_type value) {
        return svdup_n_f64(value);
    }

    inline static sve_type
    load(const svbool_t pred, const data_type* value) {
        return svld1_f64(pred, value);
    }
};

///////////////////////////////////////////////////////////////////////////

// an interesting discussion here:
// https://stackoverflow.com/questions/77834169/what-is-a-fast-fallback-algorithm-which-emulates-pdep-and-pext-in-software

// SVE2 has bitperm, which contains the implementation of pext

// todo: replace with pext whenever available

//
template <size_t NBYTES>
struct MaskHelper {};

template <>
struct MaskHelper<1> {
    static inline void
    write(uint8_t* const __restrict bitmask,
          const size_t size,
          const svbool_t pred0,
          const svbool_t pred1,
          const svbool_t pred2,
          const svbool_t pred3,
          const svbool_t pred4,
          const svbool_t pred5,
          const svbool_t pred6,
          const svbool_t pred7) {
        const size_t sve_width = svcntb();
        if (sve_width == 8 * sve_width) {
            // perform a full write
            *((svbool_t*)(bitmask + 0 * sve_width / 8)) = pred0;
            *((svbool_t*)(bitmask + 1 * sve_width / 8)) = pred1;
            *((svbool_t*)(bitmask + 2 * sve_width / 8)) = pred2;
            *((svbool_t*)(bitmask + 3 * sve_width / 8)) = pred3;
            *((svbool_t*)(bitmask + 4 * sve_width / 8)) = pred4;
            *((svbool_t*)(bitmask + 5 * sve_width / 8)) = pred5;
            *((svbool_t*)(bitmask + 6 * sve_width / 8)) = pred6;
            *((svbool_t*)(bitmask + 7 * sve_width / 8)) = pred7;
        } else {
            // perform a partial write

            // this is the buffer for the maximum possible case of 2048 bits
            uint8_t pred_buf[MAX_SVE_WIDTH / 8];
            *((volatile svbool_t*)(pred_buf + 0 * sve_width / 8)) = pred0;
            *((volatile svbool_t*)(pred_buf + 1 * sve_width / 8)) = pred1;
            *((volatile svbool_t*)(pred_buf + 2 * sve_width / 8)) = pred2;
            *((volatile svbool_t*)(pred_buf + 3 * sve_width / 8)) = pred3;
            *((volatile svbool_t*)(pred_buf + 4 * sve_width / 8)) = pred4;
            *((volatile svbool_t*)(pred_buf + 5 * sve_width / 8)) = pred5;
            *((volatile svbool_t*)(pred_buf + 6 * sve_width / 8)) = pred6;
            *((volatile svbool_t*)(pred_buf + 7 * sve_width / 8)) = pred7;

            // make the write mask
            const svbool_t pred_write = get_pred_op_8(size / 8);

            // load the buffer
            const svuint8_t mask_u8 = svld1_u8(pred_write, pred_buf);
            // write it to the bitmask
            svst1_u8(pred_write, bitmask, mask_u8);
        }
    }
};

template <>
struct MaskHelper<2> {
    static inline void
    write(uint8_t* const __restrict bitmask,
          const size_t size,
          const svbool_t pred0,
          const svbool_t pred1,
          const svbool_t pred2,
          const svbool_t pred3,
          const svbool_t pred4,
          const svbool_t pred5,
          const svbool_t pred6,
          const svbool_t pred7) {
        const size_t sve_width = svcnth();

        // this is the buffer for the maximum possible case of 2048 bits
        uint8_t pred_buf[MAX_SVE_WIDTH / 8];
        *((volatile svbool_t*)(pred_buf + 0 * sve_width / 4)) = pred0;
        *((volatile svbool_t*)(pred_buf + 1 * sve_width / 4)) = pred1;
        *((volatile svbool_t*)(pred_buf + 2 * sve_width / 4)) = pred2;
        *((volatile svbool_t*)(pred_buf + 3 * sve_width / 4)) = pred3;
        *((volatile svbool_t*)(pred_buf + 4 * sve_width / 4)) = pred4;
        *((volatile svbool_t*)(pred_buf + 5 * sve_width / 4)) = pred5;
        *((volatile svbool_t*)(pred_buf + 6 * sve_width / 4)) = pred6;
        *((volatile svbool_t*)(pred_buf + 7 * sve_width / 4)) = pred7;

        const svbool_t pred_op_8 = get_pred_op_8(size / 4);
        const svbool_t pred_write_8 = get_pred_op_8(size / 8);
        write_bitmask_16_8x(bitmask, pred_op_8, pred_write_8, pred_buf);
    }
};

template <>
struct MaskHelper<4> {
    static inline void
    write(uint8_t* const __restrict bitmask,
          const size_t size,
          const svbool_t pred0,
          const svbool_t pred1,
          const svbool_t pred2,
          const svbool_t pred3,
          const svbool_t pred4,
          const svbool_t pred5,
          const svbool_t pred6,
          const svbool_t pred7) {
        const size_t sve_width = svcntw();

        // this is the buffer for the maximum possible case of 2048 bits
        uint8_t pred_buf[MAX_SVE_WIDTH / 8];
        *((volatile svbool_t*)(pred_buf + 0 * sve_width / 2)) = pred0;
        *((volatile svbool_t*)(pred_buf + 1 * sve_width / 2)) = pred1;
        *((volatile svbool_t*)(pred_buf + 2 * sve_width / 2)) = pred2;
        *((volatile svbool_t*)(pred_buf + 3 * sve_width / 2)) = pred3;
        *((volatile svbool_t*)(pred_buf + 4 * sve_width / 2)) = pred4;
        *((volatile svbool_t*)(pred_buf + 5 * sve_width / 2)) = pred5;
        *((volatile svbool_t*)(pred_buf + 6 * sve_width / 2)) = pred6;
        *((volatile svbool_t*)(pred_buf + 7 * sve_width / 2)) = pred7;

        const svbool_t pred_op_8 = get_pred_op_8(size / 2);
        const svbool_t pred_write_8 = get_pred_op_8(size / 8);
        write_bitmask_32_8x(bitmask, pred_op_8, pred_write_8, pred_buf);
    }
};

template <>
struct MaskHelper<8> {
    static inline void
    write(uint8_t* const __restrict bitmask,
          const size_t size,
          const svbool_t pred0,
          const svbool_t pred1,
          const svbool_t pred2,
          const svbool_t pred3,
          const svbool_t pred4,
          const svbool_t pred5,
          const svbool_t pred6,
          const svbool_t pred7) {
        const size_t sve_width = svcntd();

        // this is the buffer for the maximum possible case of 2048 bits
        uint8_t pred_buf[MAX_SVE_WIDTH / 8];
        *((volatile svbool_t*)(pred_buf + 0 * sve_width)) = pred0;
        *((volatile svbool_t*)(pred_buf + 1 * sve_width)) = pred1;
        *((volatile svbool_t*)(pred_buf + 2 * sve_width)) = pred2;
        *((volatile svbool_t*)(pred_buf + 3 * sve_width)) = pred3;
        *((volatile svbool_t*)(pred_buf + 4 * sve_width)) = pred4;
        *((volatile svbool_t*)(pred_buf + 5 * sve_width)) = pred5;
        *((volatile svbool_t*)(pred_buf + 6 * sve_width)) = pred6;
        *((volatile svbool_t*)(pred_buf + 7 * sve_width)) = pred7;

        const svbool_t pred_op_8 = get_pred_op_8(size / 1);
        const svbool_t pred_write_8 = get_pred_op_8(size / 8);
        write_bitmask_64_8x(bitmask, pred_op_8, pred_write_8, pred_buf);
    }
};

///////////////////////////////////////////////////////////////////////////

// the facility that handles all bitset processing for SVE
template <typename T, typename Func>
bool
op_mask_helper(uint8_t* const __restrict res_u8, const size_t size, Func func) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    using sve_t = SVEVector<T>;

    // SVE width in elements
    const size_t sve_width = sve_t::width();
    assert((sve_width % 8) == 0);

    // process large blocks
    const size_t size_sve8 = (size / (8 * sve_width)) * (8 * sve_width);
    {
        for (size_t i = 0; i < size_sve8; i += 8 * sve_width) {
            const svbool_t pred_all = sve_t::pred_all();

            const svbool_t cmp0 = func(pred_all, i + 0 * sve_width);
            const svbool_t cmp1 = func(pred_all, i + 1 * sve_width);
            const svbool_t cmp2 = func(pred_all, i + 2 * sve_width);
            const svbool_t cmp3 = func(pred_all, i + 3 * sve_width);
            const svbool_t cmp4 = func(pred_all, i + 4 * sve_width);
            const svbool_t cmp5 = func(pred_all, i + 5 * sve_width);
            const svbool_t cmp6 = func(pred_all, i + 6 * sve_width);
            const svbool_t cmp7 = func(pred_all, i + 7 * sve_width);

            MaskHelper<sizeof(T)>::write(res_u8 + i / 8,
                                         sve_width * 8,
                                         cmp0,
                                         cmp1,
                                         cmp2,
                                         cmp3,
                                         cmp4,
                                         cmp5,
                                         cmp6,
                                         cmp7);
        }
    }

    // process leftovers
    if (size_sve8 != size) {
        auto get_partial_pred = [sve_width, size, size_sve8](const size_t j) {
            const size_t start = size_sve8 + j * sve_width;
            const size_t end = size_sve8 + (j + 1) * sve_width;

            const size_t amount = (end < size) ? sve_width : (size - start);
            const svbool_t pred_op = get_pred_op<T>(amount);

            return pred_op;
        };

        const svbool_t pred_none = svpfalse_b();
        svbool_t cmp0 = pred_none;
        svbool_t cmp1 = pred_none;
        svbool_t cmp2 = pred_none;
        svbool_t cmp3 = pred_none;
        svbool_t cmp4 = pred_none;
        svbool_t cmp5 = pred_none;
        svbool_t cmp6 = pred_none;
        svbool_t cmp7 = pred_none;

        const size_t jcount = (size - size_sve8 + sve_width - 1) / sve_width;
        if (jcount > 0) {
            cmp0 = func(get_partial_pred(0), size_sve8 + 0 * sve_width);
        }
        if (jcount > 1) {
            cmp1 = func(get_partial_pred(1), size_sve8 + 1 * sve_width);
        }
        if (jcount > 2) {
            cmp2 = func(get_partial_pred(2), size_sve8 + 2 * sve_width);
        }
        if (jcount > 3) {
            cmp3 = func(get_partial_pred(3), size_sve8 + 3 * sve_width);
        }
        if (jcount > 4) {
            cmp4 = func(get_partial_pred(4), size_sve8 + 4 * sve_width);
        }
        if (jcount > 5) {
            cmp5 = func(get_partial_pred(5), size_sve8 + 5 * sve_width);
        }
        if (jcount > 6) {
            cmp6 = func(get_partial_pred(6), size_sve8 + 6 * sve_width);
        }
        if (jcount > 7) {
            cmp7 = func(get_partial_pred(7), size_sve8 + 7 * sve_width);
        }

        MaskHelper<sizeof(T)>::write(res_u8 + size_sve8 / 8,
                                     size - size_sve8,
                                     cmp0,
                                     cmp1,
                                     cmp2,
                                     cmp3,
                                     cmp4,
                                     cmp5,
                                     cmp6,
                                     cmp7);
    }

    return true;
}

}  // namespace

///////////////////////////////////////////////////////////////////////////

namespace {

template <typename T, CompareOpType CmpOp>
bool
op_compare_val_impl(uint8_t* const __restrict res_u8,
                    const T* const __restrict src,
                    const size_t size,
                    const T& val) {
    auto handler = [src, val](const svbool_t pred, const size_t idx) {
        using sve_t = SVEVector<T>;

        const auto target = sve_t::set1(val);
        const auto v = sve_t::load(pred, src + idx);
        const svbool_t cmp = CmpHelper<CmpOp>::compare(pred, v, target);
        return cmp;
    };

    return op_mask_helper<T, decltype(handler)>(res_u8, size, handler);
}

}  // namespace

//
template <CompareOpType Op>
bool
OpCompareValImpl<int8_t, Op>::op_compare_val(uint8_t* const __restrict res_u8,
                                             const int8_t* const __restrict src,
                                             const size_t size,
                                             const int8_t& val) {
    return op_compare_val_impl<int8_t, Op>(res_u8, src, size, val);
}

template <CompareOpType Op>
bool
OpCompareValImpl<int16_t, Op>::op_compare_val(
    uint8_t* const __restrict res_u8,
    const int16_t* const __restrict src,
    const size_t size,
    const int16_t& val) {
    return op_compare_val_impl<int16_t, Op>(res_u8, src, size, val);
}

template <CompareOpType Op>
bool
OpCompareValImpl<int32_t, Op>::op_compare_val(
    uint8_t* const __restrict res_u8,
    const int32_t* const __restrict src,
    const size_t size,
    const int32_t& val) {
    return op_compare_val_impl<int32_t, Op>(res_u8, src, size, val);
}

template <CompareOpType Op>
bool
OpCompareValImpl<int64_t, Op>::op_compare_val(
    uint8_t* const __restrict res_u8,
    const int64_t* const __restrict src,
    const size_t size,
    const int64_t& val) {
    return op_compare_val_impl<int64_t, Op>(res_u8, src, size, val);
}

template <CompareOpType Op>
bool
OpCompareValImpl<float, Op>::op_compare_val(uint8_t* const __restrict res_u8,
                                            const float* const __restrict src,
                                            const size_t size,
                                            const float& val) {
    return op_compare_val_impl<float, Op>(res_u8, src, size, val);
}

template <CompareOpType Op>
bool
OpCompareValImpl<double, Op>::op_compare_val(uint8_t* const __restrict res_u8,
                                             const double* const __restrict src,
                                             const size_t size,
                                             const double& val) {
    return op_compare_val_impl<double, Op>(res_u8, src, size, val);
}

///////////////////////////////////////////////////////////////////////////

namespace {

template <typename T, CompareOpType CmpOp>
bool
op_compare_column_impl(uint8_t* const __restrict res_u8,
                       const T* const __restrict left,
                       const T* const __restrict right,
                       const size_t size) {
    auto handler = [left, right](const svbool_t pred, const size_t idx) {
        using sve_t = SVEVector<T>;

        const auto left_v = sve_t::load(pred, left + idx);
        const auto right_v = sve_t::load(pred, right + idx);
        const svbool_t cmp = CmpHelper<CmpOp>::compare(pred, left_v, right_v);
        return cmp;
    };

    return op_mask_helper<T, decltype(handler)>(res_u8, size, handler);
}

}  // namespace

//
template <CompareOpType Op>
bool
OpCompareColumnImpl<int8_t, int8_t, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const int8_t* const __restrict left,
    const int8_t* const __restrict right,
    const size_t size) {
    return op_compare_column_impl<int8_t, Op>(res_u8, left, right, size);
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<int16_t, int16_t, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const int16_t* const __restrict left,
    const int16_t* const __restrict right,
    const size_t size) {
    return op_compare_column_impl<int16_t, Op>(res_u8, left, right, size);
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<int32_t, int32_t, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const int32_t* const __restrict left,
    const int32_t* const __restrict right,
    const size_t size) {
    return op_compare_column_impl<int32_t, Op>(res_u8, left, right, size);
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<int64_t, int64_t, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const int64_t* const __restrict left,
    const int64_t* const __restrict right,
    const size_t size) {
    return op_compare_column_impl<int64_t, Op>(res_u8, left, right, size);
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<float, float, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const float* const __restrict left,
    const float* const __restrict right,
    const size_t size) {
    return op_compare_column_impl<float, Op>(res_u8, left, right, size);
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<double, double, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const double* const __restrict left,
    const double* const __restrict right,
    const size_t size) {
    return op_compare_column_impl<double, Op>(res_u8, left, right, size);
}

///////////////////////////////////////////////////////////////////////////

namespace {

template <typename T, RangeType Op>
bool
op_within_range_column_impl(uint8_t* const __restrict res_u8,
                            const T* const __restrict lower,
                            const T* const __restrict upper,
                            const T* const __restrict values,
                            const size_t size) {
    auto handler = [lower, upper, values](const svbool_t pred,
                                          const size_t idx) {
        using sve_t = SVEVector<T>;

        const auto lower_v = sve_t::load(pred, lower + idx);
        const auto upper_v = sve_t::load(pred, upper + idx);
        const auto values_v = sve_t::load(pred, values + idx);

        const svbool_t cmpl = CmpHelper<Range2Compare<Op>::lower>::compare(
            pred, lower_v, values_v);
        const svbool_t cmpu = CmpHelper<Range2Compare<Op>::upper>::compare(
            pred, values_v, upper_v);
        const svbool_t cmp = svand_b_z(pred, cmpl, cmpu);

        return cmp;
    };

    return op_mask_helper<T, decltype(handler)>(res_u8, size, handler);
}

}  // namespace

//
template <RangeType Op>
bool
OpWithinRangeColumnImpl<int8_t, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const int8_t* const __restrict lower,
    const int8_t* const __restrict upper,
    const int8_t* const __restrict values,
    const size_t size) {
    return op_within_range_column_impl<int8_t, Op>(
        res_u8, lower, upper, values, size);
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<int16_t, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const int16_t* const __restrict lower,
    const int16_t* const __restrict upper,
    const int16_t* const __restrict values,
    const size_t size) {
    return op_within_range_column_impl<int16_t, Op>(
        res_u8, lower, upper, values, size);
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<int32_t, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const int32_t* const __restrict lower,
    const int32_t* const __restrict upper,
    const int32_t* const __restrict values,
    const size_t size) {
    return op_within_range_column_impl<int32_t, Op>(
        res_u8, lower, upper, values, size);
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<int64_t, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const int64_t* const __restrict lower,
    const int64_t* const __restrict upper,
    const int64_t* const __restrict values,
    const size_t size) {
    return op_within_range_column_impl<int64_t, Op>(
        res_u8, lower, upper, values, size);
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<float, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const float* const __restrict lower,
    const float* const __restrict upper,
    const float* const __restrict values,
    const size_t size) {
    return op_within_range_column_impl<float, Op>(
        res_u8, lower, upper, values, size);
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<double, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const double* const __restrict lower,
    const double* const __restrict upper,
    const double* const __restrict values,
    const size_t size) {
    return op_within_range_column_impl<double, Op>(
        res_u8, lower, upper, values, size);
}

///////////////////////////////////////////////////////////////////////////

namespace {

template <typename T, RangeType Op>
bool
op_within_range_val_impl(uint8_t* const __restrict res_u8,
                         const T& lower,
                         const T& upper,
                         const T* const __restrict values,
                         const size_t size) {
    auto handler = [lower, upper, values](const svbool_t pred,
                                          const size_t idx) {
        using sve_t = SVEVector<T>;

        const auto lower_v = sve_t::set1(lower);
        const auto upper_v = sve_t::set1(upper);
        const auto values_v = sve_t::load(pred, values + idx);

        const svbool_t cmpl = CmpHelper<Range2Compare<Op>::lower>::compare(
            pred, lower_v, values_v);
        const svbool_t cmpu = CmpHelper<Range2Compare<Op>::upper>::compare(
            pred, values_v, upper_v);
        const svbool_t cmp = svand_b_z(pred, cmpl, cmpu);

        return cmp;
    };

    return op_mask_helper<T, decltype(handler)>(res_u8, size, handler);
}

}  // namespace

//
template <RangeType Op>
bool
OpWithinRangeValImpl<int8_t, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const int8_t& lower,
    const int8_t& upper,
    const int8_t* const __restrict values,
    const size_t size) {
    return op_within_range_val_impl<int8_t, Op>(
        res_u8, lower, upper, values, size);
}

template <RangeType Op>
bool
OpWithinRangeValImpl<int16_t, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const int16_t& lower,
    const int16_t& upper,
    const int16_t* const __restrict values,
    const size_t size) {
    return op_within_range_val_impl<int16_t, Op>(
        res_u8, lower, upper, values, size);
}

template <RangeType Op>
bool
OpWithinRangeValImpl<int32_t, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const int32_t& lower,
    const int32_t& upper,
    const int32_t* const __restrict values,
    const size_t size) {
    return op_within_range_val_impl<int32_t, Op>(
        res_u8, lower, upper, values, size);
}

template <RangeType Op>
bool
OpWithinRangeValImpl<int64_t, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const int64_t& lower,
    const int64_t& upper,
    const int64_t* const __restrict values,
    const size_t size) {
    return op_within_range_val_impl<int64_t, Op>(
        res_u8, lower, upper, values, size);
}

template <RangeType Op>
bool
OpWithinRangeValImpl<float, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const float& lower,
    const float& upper,
    const float* const __restrict values,
    const size_t size) {
    return op_within_range_val_impl<float, Op>(
        res_u8, lower, upper, values, size);
}

template <RangeType Op>
bool
OpWithinRangeValImpl<double, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const double& lower,
    const double& upper,
    const double* const __restrict values,
    const size_t size) {
    return op_within_range_val_impl<double, Op>(
        res_u8, lower, upper, values, size);
}

///////////////////////////////////////////////////////////////////////////

namespace {

template <ArithOpType AOp, CompareOpType CmpOp>
struct ArithHelperI64 {};

template <CompareOpType CmpOp>
struct ArithHelperI64<ArithOpType::Add, CmpOp> {
    static inline svbool_t
    op(const svbool_t pred,
       const svint64_t left,
       const svint64_t right,
       const svint64_t value) {
        // left + right == value
        return CmpHelper<CmpOp>::compare(
            pred, svadd_s64_z(pred, left, right), value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperI64<ArithOpType::Sub, CmpOp> {
    static inline svbool_t
    op(const svbool_t pred,
       const svint64_t left,
       const svint64_t right,
       const svint64_t value) {
        // left - right == value
        return CmpHelper<CmpOp>::compare(
            pred, svsub_s64_z(pred, left, right), value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperI64<ArithOpType::Mul, CmpOp> {
    static inline svbool_t
    op(const svbool_t pred,
       const svint64_t left,
       const svint64_t right,
       const svint64_t value) {
        // left * right == value
        return CmpHelper<CmpOp>::compare(
            pred, svmul_s64_z(pred, left, right), value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperI64<ArithOpType::Div, CmpOp> {
    static inline svbool_t
    op(const svbool_t pred,
       const svint64_t left,
       const svint64_t right,
       const svint64_t value) {
        // left / right == value
        return CmpHelper<CmpOp>::compare(
            pred, svdiv_s64_z(pred, left, right), value);
    }
};

//
template <ArithOpType AOp, CompareOpType CmpOp>
struct ArithHelperF32 {};

template <CompareOpType CmpOp>
struct ArithHelperF32<ArithOpType::Add, CmpOp> {
    static inline svbool_t
    op(const svbool_t pred,
       const svfloat32_t left,
       const svfloat32_t right,
       const svfloat32_t value) {
        // left + right == value
        return CmpHelper<CmpOp>::compare(
            pred, svadd_f32_z(pred, left, right), value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF32<ArithOpType::Sub, CmpOp> {
    static inline svbool_t
    op(const svbool_t pred,
       const svfloat32_t left,
       const svfloat32_t right,
       const svfloat32_t value) {
        // left - right == value
        return CmpHelper<CmpOp>::compare(
            pred, svsub_f32_z(pred, left, right), value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF32<ArithOpType::Mul, CmpOp> {
    static inline svbool_t
    op(const svbool_t pred,
       const svfloat32_t left,
       const svfloat32_t right,
       const svfloat32_t value) {
        // left * right == value
        return CmpHelper<CmpOp>::compare(
            pred, svmul_f32_z(pred, left, right), value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF32<ArithOpType::Div, CmpOp> {
    static inline svbool_t
    op(const svbool_t pred,
       const svfloat32_t left,
       const svfloat32_t right,
       const svfloat32_t value) {
        // left == right * value
        return CmpHelper<CmpOp>::compare(
            pred, left, svmul_f32_z(pred, right, value));
    }
};

//
template <ArithOpType AOp, CompareOpType CmpOp>
struct ArithHelperF64 {};

template <CompareOpType CmpOp>
struct ArithHelperF64<ArithOpType::Add, CmpOp> {
    static inline svbool_t
    op(const svbool_t pred,
       const svfloat64_t left,
       const svfloat64_t right,
       const svfloat64_t value) {
        // left + right == value
        return CmpHelper<CmpOp>::compare(
            pred, svadd_f64_z(pred, left, right), value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF64<ArithOpType::Sub, CmpOp> {
    static inline svbool_t
    op(const svbool_t pred,
       const svfloat64_t left,
       const svfloat64_t right,
       const svfloat64_t value) {
        // left - right == value
        return CmpHelper<CmpOp>::compare(
            pred, svsub_f64_z(pred, left, right), value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF64<ArithOpType::Mul, CmpOp> {
    static inline svbool_t
    op(const svbool_t pred,
       const svfloat64_t left,
       const svfloat64_t right,
       const svfloat64_t value) {
        // left * right == value
        return CmpHelper<CmpOp>::compare(
            pred, svmul_f64_z(pred, left, right), value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF64<ArithOpType::Div, CmpOp> {
    static inline svbool_t
    op(const svbool_t pred,
       const svfloat64_t left,
       const svfloat64_t right,
       const svfloat64_t value) {
        // left == right * value
        return CmpHelper<CmpOp>::compare(
            pred, left, svmul_f64_z(pred, right, value));
    }
};

}  // namespace

// todo: Mod

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<int8_t, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const int8_t* const __restrict src,
    const ArithHighPrecisionType<int8_t>& right_operand,
    const ArithHighPrecisionType<int8_t>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Mod) {
        return false;
    } else {
        using T = int64_t;

        auto handler = [src, right_operand, value](const svbool_t pred,
                                                   const size_t idx) {
            using sve_t = SVEVector<T>;

            const auto right_v = svdup_n_s64(right_operand);
            const auto value_v = svdup_n_s64(value);
            const svint64_t src_v = svld1sb_s64(pred, src + idx);

            const svbool_t cmp =
                ArithHelperI64<AOp, CmpOp>::op(pred, src_v, right_v, value_v);
            return cmp;
        };

        return op_mask_helper<T, decltype(handler)>(res_u8, size, handler);
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
    if constexpr (AOp == ArithOpType::Mod) {
        return false;
    } else {
        using T = int64_t;

        auto handler = [src, right_operand, value](const svbool_t pred,
                                                   const size_t idx) {
            using sve_t = SVEVector<T>;

            const auto right_v = svdup_n_s64(right_operand);
            const auto value_v = svdup_n_s64(value);
            const svint64_t src_v = svld1sh_s64(pred, src + idx);

            const svbool_t cmp =
                ArithHelperI64<AOp, CmpOp>::op(pred, src_v, right_v, value_v);
            return cmp;
        };

        return op_mask_helper<T, decltype(handler)>(res_u8, size, handler);
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
    if constexpr (AOp == ArithOpType::Mod) {
        return false;
    } else {
        using T = int64_t;

        auto handler = [src, right_operand, value](const svbool_t pred,
                                                   const size_t idx) {
            using sve_t = SVEVector<T>;

            const auto right_v = svdup_n_s64(right_operand);
            const auto value_v = svdup_n_s64(value);
            const svint64_t src_v = svld1sw_s64(pred, src + idx);

            const svbool_t cmp =
                ArithHelperI64<AOp, CmpOp>::op(pred, src_v, right_v, value_v);
            return cmp;
        };

        return op_mask_helper<T, decltype(handler)>(res_u8, size, handler);
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
    if constexpr (AOp == ArithOpType::Mod) {
        return false;
    } else {
        using T = int64_t;

        auto handler = [src, right_operand, value](const svbool_t pred,
                                                   const size_t idx) {
            using sve_t = SVEVector<T>;

            const auto right_v = svdup_n_s64(right_operand);
            const auto value_v = svdup_n_s64(value);
            const svint64_t src_v = svld1_s64(pred, src + idx);

            const svbool_t cmp =
                ArithHelperI64<AOp, CmpOp>::op(pred, src_v, right_v, value_v);
            return cmp;
        };

        return op_mask_helper<T, decltype(handler)>(res_u8, size, handler);
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
        using T = float;

        auto handler = [src, right_operand, value](const svbool_t pred,
                                                   const size_t idx) {
            using sve_t = SVEVector<T>;

            const auto right_v = svdup_n_f32(right_operand);
            const auto value_v = svdup_n_f32(value);
            const svfloat32_t src_v = svld1_f32(pred, src + idx);

            const svbool_t cmp =
                ArithHelperF32<AOp, CmpOp>::op(pred, src_v, right_v, value_v);
            return cmp;
        };

        return op_mask_helper<T, decltype(handler)>(res_u8, size, handler);
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
        using T = double;

        auto handler = [src, right_operand, value](const svbool_t pred,
                                                   const size_t idx) {
            using sve_t = SVEVector<T>;

            const auto right_v = svdup_n_f64(right_operand);
            const auto value_v = svdup_n_f64(value);
            const svfloat64_t src_v = svld1_f64(pred, src + idx);

            const svbool_t cmp =
                ArithHelperF64<AOp, CmpOp>::op(pred, src_v, right_v, value_v);
            return cmp;
        };

        return op_mask_helper<T, decltype(handler)>(res_u8, size, handler);
    }
}

///////////////////////////////////////////////////////////////////////////

}  // namespace sve
}  // namespace arm
}  // namespace detail
}  // namespace bitset
}  // namespace milvus
