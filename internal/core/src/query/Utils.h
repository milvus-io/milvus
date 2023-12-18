// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <limits>
#include <string>

#include "query/Expr.h"
#include "common/Utils.h"
#include "simd/hook.h"

namespace milvus::query {

template <typename T, typename U>
inline bool
Match(const T& x, const U& y, OpType op) {
    PanicInfo(NotImplemented, "not supported");
}

template <>
inline bool
Match<std::string>(const std::string& str, const std::string& val, OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        default:
            PanicInfo(OpTypeInvalid, "not supported");
    }
}

template <>
inline bool
Match<std::string_view>(const std::string_view& str,
                        const std::string& val,
                        OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        default:
            PanicInfo(OpTypeInvalid, "not supported");
    }
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
gt_ub(int64_t t) {
    return t > std::numeric_limits<T>::max();
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
lt_lb(int64_t t) {
    return t < std::numeric_limits<T>::min();
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
out_of_range(int64_t t) {
    return gt_ub<T>(t) || lt_lb<T>(t);
}

inline void
AppendOneChunk(BitsetType& result, const bool* chunk_ptr, size_t chunk_len) {
    // Append a value once instead of BITSET_BLOCK_BIT_SIZE times.
    auto AppendBlock = [&result](const bool* ptr, int n) {
        for (int i = 0; i < n; ++i) {
#if defined(USE_DYNAMIC_SIMD)
            auto val = milvus::simd::get_bitset_block(ptr);
#else
            BitsetBlockType val = 0;
            // This can use CPU SIMD optimzation
            uint8_t vals[BITSET_BLOCK_SIZE] = {0};
            for (size_t j = 0; j < 8; ++j) {
                for (size_t k = 0; k < BITSET_BLOCK_SIZE; ++k) {
                    vals[k] |= uint8_t(*(ptr + k * 8 + j)) << j;
                }
            }
            for (size_t j = 0; j < BITSET_BLOCK_SIZE; ++j) {
                val |= BitsetBlockType(vals[j]) << (8 * j);
            }
#endif
            result.append(val);
            ptr += BITSET_BLOCK_SIZE * 8;
        }
    };
    // Append bit for these bits that can not be union as a block
    // Usually n less than BITSET_BLOCK_BIT_SIZE.
    auto AppendBit = [&result](const bool* ptr, int n) {
        for (int i = 0; i < n; ++i) {
            bool bit = *ptr++;
            result.push_back(bit);
        }
    };

    size_t res_len = result.size();

    int n_prefix =
        res_len % BITSET_BLOCK_BIT_SIZE == 0
            ? 0
            : std::min(BITSET_BLOCK_BIT_SIZE - res_len % BITSET_BLOCK_BIT_SIZE,
                       chunk_len);

    AppendBit(chunk_ptr, n_prefix);

    if (n_prefix == chunk_len)
        return;

    size_t n_block = (chunk_len - n_prefix) / BITSET_BLOCK_BIT_SIZE;
    size_t n_suffix = (chunk_len - n_prefix) % BITSET_BLOCK_BIT_SIZE;

    AppendBlock(chunk_ptr + n_prefix, n_block);

    AppendBit(chunk_ptr + n_prefix + n_block * BITSET_BLOCK_BIT_SIZE, n_suffix);

    return;
}

}  // namespace milvus::query
