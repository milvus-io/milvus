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
#include <cstdio>
#include <string>

#include "crc32c/crc32c.h"

namespace milvus::storage {

inline uint32_t
Crc32cUpdate(uint32_t crc, const void* data, size_t len) {
    return crc32c::Extend(crc, reinterpret_cast<const uint8_t*>(data), len);
}

inline uint32_t
Crc32cValue(const void* data, size_t len) {
    return crc32c::Crc32c(reinterpret_cast<const uint8_t*>(data), len);
}

// crc32c_combine: combine two CRC-32C values without needing original data.
// Given crc_a = CRC(A), crc_b = CRC(B), len_b = len(B),
// returns CRC(A || B).
//
// Uses GF(2) matrix exponentiation with the CRC-32C polynomial (Castagnoli).
// Equivalent to zlib's crc32_combine but for the CRC-32C polynomial.
namespace detail {

// The CRC-32C polynomial in reversed (reflected) form: 0x82F63B78
// x^32 + x^28 + x^27 + x^26 + x^25 + x^23 + x^22 + x^20 +
// x^19 + x^18 + x^14 + x^13 + x^11 + x^10 + x^9 + x^8 + x^6 + 1

// Multiply a 32x32 GF(2) matrix by a 32-bit vector
inline uint32_t
Gf2MatrixTimes(const uint32_t* mat, uint32_t vec) {
    uint32_t sum = 0;
    while (vec) {
        if (vec & 1) {
            sum ^= *mat;
        }
        vec >>= 1;
        mat++;
    }
    return sum;
}

// Square a 32x32 GF(2) matrix in-place
inline void
Gf2MatrixSquare(uint32_t* square, const uint32_t* mat) {
    for (int n = 0; n < 32; n++) {
        square[n] = Gf2MatrixTimes(mat, mat[n]);
    }
}

}  // namespace detail

inline uint32_t
Crc32cCombine(uint32_t crc1, uint32_t crc2, size_t len2) {
    if (len2 == 0) {
        return crc1;
    }

    // Build the "zeros operator" matrices for CRC-32C polynomial 0x82F63B78
    uint32_t even[32];  // even-power-of-two zeros operator
    uint32_t odd[32];   // odd-power-of-two zeros operator

    // Put operator for one zero bit in odd
    odd[0] = 0x82F63B78U;  // CRC-32C polynomial
    uint32_t row = 1;
    for (int n = 1; n < 32; n++) {
        odd[n] = row;
        row <<= 1;
    }

    // Put operator for two zero bits in even (square the odd operator)
    detail::Gf2MatrixSquare(even, odd);

    // Put operator for four zero bits in odd
    detail::Gf2MatrixSquare(odd, even);

    // Apply len2 zeros to crc1 using repeated squaring
    do {
        // Apply zeros operator for this bit of len2
        detail::Gf2MatrixSquare(even, odd);
        if (len2 & 1) {
            crc1 = detail::Gf2MatrixTimes(even, crc1);
        }
        len2 >>= 1;
        if (len2 == 0) {
            break;
        }

        // Another iteration: square, apply
        detail::Gf2MatrixSquare(odd, even);
        if (len2 & 1) {
            crc1 = detail::Gf2MatrixTimes(odd, crc1);
        }
        len2 >>= 1;
    } while (len2 != 0);

    // Combine the CRC values
    crc1 ^= crc2;
    return crc1;
}

inline std::string
Crc32cToHex(uint32_t crc) {
    char buf[9];
    std::snprintf(buf, sizeof(buf), "%08X", crc);
    return std::string(buf);
}

inline uint32_t
Crc32cFromHex(const std::string& hex) {
    return static_cast<uint32_t>(std::stoul(hex, nullptr, 16));
}

}  // namespace milvus::storage
