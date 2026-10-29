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

// Entry formats stored in EntryPool::Payload::comp_type.
constexpr uint8_t kCompTypeIndependent = 3;
constexpr uint8_t kCompTypeRaw = 0xFF;

// Per-bitmap codecs inside Independent entries; Raw is also supported.
constexpr uint8_t kCompTypeRoaring = 1;
constexpr uint8_t kCompTypeRoaringInv =
    2;  // inverted + Roaring (density >= 97%)

// Flag in valid_bit_count high bit: valid bitset is all-ones, not stored
constexpr uint32_t kValidAllOnesMask = 0x80000000u;

// Each bitmap owns its Roaring bytes or borrows its input's Raw bytes until
// flattened into the cache entry. Mixed encodings need no temporary Raw copy.
struct CompressedData {
    uint8_t comp_type{kCompTypeRaw};
    uint8_t result_comp_type{kCompTypeRaw};
    uint8_t valid_comp_type{kCompTypeRaw};

    // Header (8 bytes): [result_bits][valid_bits_with_flag]
    char header[8];

    std::vector<char> result_payload;
    std::vector<char> valid_payload;

    // For Raw: zero-copy pointers to original data (header still in `header`)
    const char* raw_result_ptr{nullptr};
    size_t raw_result_size{0};
    const char* raw_valid_ptr{nullptr};
    size_t raw_valid_size{0};

    size_t
    result_size() const {
        return raw_result_size + result_payload.size();
    }

    size_t
    valid_size() const {
        return raw_valid_size + valid_payload.size();
    }

    const char*
    result_data() const {
        return result_comp_type == kCompTypeRaw ? raw_result_ptr
                                                : result_payload.data();
    }

    const char*
    valid_data() const {
        return valid_comp_type == kCompTypeRaw ? raw_valid_ptr
                                               : valid_payload.data();
    }

    // Independent entries add two codec bytes and a four-byte result length.
    size_t
    total_size() const {
        return (comp_type == kCompTypeRaw ? 8 : 14) + result_size() +
               valid_size();
    }
};

class CacheCompressor {
 public:
    // Select the encoding independently for result and validity:
    //   density <= 3%     → Roaring
    //   density >= 97%    → inverted Roaring
    //   otherwise         → Raw (zero-copy)
    // Valid bitset: if all-ones, skipped entirely (flagged in header).
    static CompressedData
    Compress(const TargetBitmap& result,
             const TargetBitmap& valid,
             bool compression_enabled);

    // Flatten both independently encoded bitmaps into an EntryPool buffer.
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
    CompressRoaring(const TargetBitmap& bset, bool inverted = false);

    static bool
    DecompressBitmap(const char* data,
                     uint32_t data_len,
                     uint32_t num_bits,
                     uint8_t comp_type,
                     TargetBitmap& out);

    static bool
    DecompressRoaring(const char* data,
                      uint32_t data_len,
                      uint32_t num_bits,
                      TargetBitmap& out);
};

}  // namespace exec
}  // namespace milvus
