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
#include <string_view>

// Single C++ source of truth for the MBF1 envelope layout (the bloom_match
// filter blob wire format; the Go twin is client/sbbf). Layout, all integers
// little-endian:
//   offset  size  field
//   0       4     magic "MBF1"
//   4       2     version       (= 1)
//   6       2     algo          (1 = parquet_sbbf_xxh64)
//   8       8     n_declared    (informational)
//   16      8     fpr_declared  (float64, informational)
//   24      4     num_blocks    (body length must equal num_blocks * 32)
//   28      4     reserved      (must be 0)
//   32      ...   body: SBBF blocks
// Full validation lives in exec/expression SplitBlockBloomFilterView::Parse;
// this header only centralizes the layout constants and field loads so no
// second copy of the offsets can drift.

namespace milvus::bloom_envelope {

constexpr size_t kHeaderSize = 32;
constexpr size_t kBytesPerBlock = 32;
constexpr uint16_t kVersion = 1;
constexpr uint16_t kAlgoParquetSbbfXxh64 = 1;
// Mirrors parquet::BlockSplitBloomFilter::kMaximumBloomFilterBytes and the Go
// builder's MaxFilterBytes.
constexpr uint64_t kMaxFilterBytes = 128 * 1024 * 1024;

inline uint16_t
LoadU16LE(const uint8_t* p) {
    return static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8);
}

inline uint32_t
LoadU32LE(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

inline uint64_t
LoadU64LE(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(p[i]) << (8 * i);
    }
    return v;
}

// Best-effort read of the informational n_declared header field for logging /
// error summaries. Returns 0 for blobs too short to carry the field; callers
// needing real validation use SplitBlockBloomFilterView::Parse.
inline uint64_t
DecodeNDeclared(std::string_view blob) {
    if (blob.size() < 16) {
        return 0;
    }
    return LoadU64LE(reinterpret_cast<const uint8_t*>(blob.data()) + 8);
}

}  // namespace milvus::bloom_envelope
