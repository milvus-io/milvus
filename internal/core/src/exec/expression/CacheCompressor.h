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
#include <vector>

#include "common/Types.h"

namespace milvus {
namespace exec {

// Compression types stored in CacheEntryHeader.comp_type
constexpr uint8_t kCompTypeLZ4 = 0;
constexpr uint8_t kCompTypeRoaring = 1;
constexpr uint8_t kCompTypeRoaringInv =
    2;  // inverted + Roaring (density > 97%)
constexpr uint8_t kCompTypeRaw = 0xFF;

// Flag in valid_bit_count high bit: valid bitset is all-ones, not stored
constexpr uint32_t kValidAllOnesMask = 0x80000000u;

// Compressed data wrapper that supports both owned-buffer (Roaring) and
// zero-copy scatter-gather (Raw) modes. Designed to avoid intermediate
// allocations on the Raw fast path.
struct CompressedData {
    uint8_t comp_type{kCompTypeRaw};

    // Header (8 bytes): [result_bits][valid_bits_with_flag]
    char header[8];

    // For Roaring/RoaringInv: full payload owned here
    std::vector<char> payload;

    // For Raw: zero-copy pointers to original data (header still in `header`)
    const char* raw_result_ptr{nullptr};
    size_t raw_result_size{0};
    const char* raw_valid_ptr{nullptr};
    size_t raw_valid_size{0};

    // Total entry data size on disk
    size_t
    total_size() const {
        if (comp_type == kCompTypeRaw) {
            return 8 + raw_result_size + raw_valid_size;
        }
        return 8 + payload.size();
    }
};

class CacheCompressor {
 public:
    // Compress result+valid bitsets, auto-selecting the best method:
    //   density <= 3%     → Roaring
    //   density >= 97%    → inverted Roaring
    //   otherwise         → Raw (zero-copy)
    // Valid bitset: if all-ones, skipped entirely (flagged in header).
    static CompressedData
    Compress(const TargetBitmap& result,
             const TargetBitmap& valid,
             bool compression_enabled);

    // Backward-compat: returns a flat buffer (header + payload).
    // Used by tests; production path uses the CompressedData variant directly.
    static std::vector<char>
    Compress(const TargetBitmap& result,
             const TargetBitmap& valid,
             bool compression_enabled,
             uint8_t& out_comp_type);

    static bool
    Decompress(const char* data,
               uint32_t data_len,
               uint8_t comp_type,
               TargetBitmap& out_result,
               TargetBitmap& out_valid);

 private:
    static std::vector<char>
    CompressRoaring(const TargetBitmap& bset);

    static bool
    DecompressRoaring(const char* data,
                      uint32_t data_len,
                      uint32_t num_bits,
                      TargetBitmap& out);
};

}  // namespace exec
}  // namespace milvus
