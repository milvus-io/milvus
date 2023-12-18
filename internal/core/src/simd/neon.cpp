// Copyright (C) 2019-2023 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#if defined(__ARM_NEON)

#include "neon.h"

#include <cstddef>
#include <arm_neon.h>

namespace milvus {
namespace simd {

bool
AllFalseNEON(const bool* src, int64_t size) {
    int num_chunk = size / 16;

    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(src);
    for (size_t i = 0; i < num_chunk * 16; i += 16) {
        uint8x16_t data = vld1q_u8(ptr + i);
        if (vmaxvq_u8(data) != 0) {
            return false;
        }
    }

    for (size_t i = num_chunk * 16; i < size; ++i) {
        if (src[i]) {
            return false;
        }
    }

    return true;
}

bool
AllTrueNEON(const bool* src, int64_t size) {
    int num_chunk = size / 16;

    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(src);
    for (size_t i = 0; i < num_chunk * 16; i += 16) {
        uint8x16_t data = vld1q_u8(ptr + i);
        if (vminvq_u8(data) == 0) {
            return false;
        }
    }

    for (size_t i = num_chunk * 16; i < size; ++i) {
        if (!src[i]) {
            return false;
        }
    }

    return true;
}

void
InvertBoolNEON(bool* src, int64_t size) {
    int num_chunk = size / 16;
    uint8x16_t mask = vdupq_n_u8(0x01);
    uint8_t* ptr = reinterpret_cast<uint8_t*>(src);
    for (size_t i = 0; i < num_chunk * 16; i += 16) {
        uint8x16_t data = vld1q_u8(ptr + i);

        uint8x16_t flipped = veorq_u8(data, mask);

        vst1q_u8(ptr + i, flipped);
    }

    for (size_t i = num_chunk * 16; i < size; ++i) {
        src[i] = !src[i];
    }
}

void
AndBoolNEON(bool* left, bool* right, int64_t size) {
    int num_chunk = size / 16;
    uint8_t* lptr = reinterpret_cast<uint8_t*>(left);
    uint8_t* rptr = reinterpret_cast<uint8_t*>(right);
    for (size_t i = 0; i < num_chunk * 16; i += 16) {
        uint8x16_t l_reg = vld1q_u8(lptr + i);
        uint8x16_t r_reg = vld1q_u8(rptr + i);

        uint8x16_t res = vandq_u8(l_reg, r_reg);

        vst1q_u8(lptr + i, res);
    }

    for (size_t i = num_chunk * 16; i < size; ++i) {
        left[i] &= right[i];
    }
}

void
OrBoolNEON(bool* left, bool* right, int64_t size) {
    int num_chunk = size / 16;
    uint8_t* lptr = reinterpret_cast<uint8_t*>(left);
    uint8_t* rptr = reinterpret_cast<uint8_t*>(right);
    for (size_t i = 0; i < num_chunk * 16; i += 16) {
        uint8x16_t l_reg = vld1q_u8(lptr + i);
        uint8x16_t r_reg = vld1q_u8(rptr + i);

        uint8x16_t res = vorrq_u8(l_reg, r_reg);

        vst1q_u8(lptr + i, res);
    }

    for (size_t i = num_chunk * 16; i < size; ++i) {
        left[i] |= right[i];
    }
}

}  // namespace simd
}  // namespace milvus

#endif