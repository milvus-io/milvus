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

// The bloom kind's data plane: a zero-copy prober over an MBF1-enveloped
// parquet Split-Block Bloom Filter (SBBF) blob. The physical expression that
// drives it lives in MembershipFilterExpr.h (unified with the roaring kind);
// only the probe algorithm stays here.

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "xxhash.h"

#include "common/BloomFilterEnvelope.h"

namespace milvus {
namespace exec {

// Zero-copy prober over an MBF1-enveloped parquet Split-Block Bloom Filter
// (SBBF) blob. The bit layout and probe algorithm are bit-identical to Arrow
// C++'s parquet::BlockSplitBloomFilter (parquet-format BloomFilter.md) and to
// the Go builder in client/membership/sbbf. Conformance is pinned by the shared golden
// vectors in client/membership/sbbf/testdata/golden_vectors.json.
//
// MBF1 envelope layout (all integers little-endian):
//   offset  size  field
//   0       4     magic "MBF1"
//   4       2     version       (= 1)
//   6       2     algo          (1 = parquet_sbbf_xxh64)
//   8       8     n_declared    (informational)
//   16      8     fpr_declared  (float64, informational)
//   24      4     num_blocks    (body length must equal num_blocks * 32)
//   28      1     domains       (bitmask: 1 = int64, 2 = utf8)
//   29      3     reserved      (must be 0)
//   32      ...   body: SBBF blocks
//
// VALUE DOMAINS: int64 values hash their 8-byte little-endian image and strings
// hash their raw UTF-8 bytes — into the SAME XXH64 output space. An 8-byte
// string and the int64 with the same byte image therefore hash identically
// (e.g. "ABCDEFGH" and 0x4847464544434241), which without a gate would let a
// probe match a filter that never recorded its domain at all. The envelope
// records the domains actually inserted and every probe below is gated on it,
// so a value can only match a filter built from its own domain. This is the
// single owner of that invariant: the typed-scalar path, the index
// reverse-lookup path and the per-row JSON path all inherit it from here.
//
// Lifetime: the view aliases the blob bytes owned by the logical
// expr::BloomFilterExpr, which every compiled physical expr holds via
// shared_ptr, so the view never outlives its backing storage.
class SplitBlockBloomFilterView {
 public:
    // Layout constants live in common/BloomFilterEnvelope.h (single C++
    // source of truth for the MBF1 envelope); aliased here for callers.
    static constexpr size_t kHeaderSize = bloom_envelope::kHeaderSize;
    static constexpr size_t kBytesPerBlock = bloom_envelope::kBytesPerBlock;
    static constexpr uint16_t kVersion = bloom_envelope::kVersion;
    static constexpr uint16_t kAlgoParquetSbbfXxh64 =
        bloom_envelope::kAlgoParquetSbbfXxh64;
    static constexpr uint64_t kMaxFilterBytes = bloom_envelope::kMaxFilterBytes;
    static constexpr uint8_t kDomainInt64 = bloom_envelope::kDomainInt64;
    static constexpr uint8_t kDomainUtf8 = bloom_envelope::kDomainUtf8;
    static constexpr uint8_t kDomainKnown = bloom_envelope::kDomainKnown;

    SplitBlockBloomFilterView() = default;

    // Validates the MBF1 envelope and returns a zero-copy view over blob.
    // Every header field is checked against the actual blob length before
    // any use, and nothing is allocated from untrusted fields. Malformed
    // input throws SegcoreError{ExprInvalid}: the request content (the
    // client/proxy-supplied blob) is to blame, so this classifies as an
    // input/parameter error, never a system error.
    static SplitBlockBloomFilterView
    Parse(std::string_view blob);

    // Probe: block = ((h >> 32) * num_blocks) >> 32; for word i in 0..7 the
    // checked bit is (uint32(h) * SALT[i]) >> 27; match iff all 8 bits set.
    bool
    Test(uint64_t hash) const {
        const auto block = static_cast<uint32_t>(
            ((hash >> 32) * static_cast<uint64_t>(num_blocks_)) >> 32);
        const uint8_t* block_ptr =
            body_ + static_cast<size_t>(block) * kBytesPerBlock;
        const auto key = static_cast<uint32_t>(hash);
        for (int i = 0; i < 8; ++i) {
            const uint32_t mask = uint32_t(1) << ((key * kSalt[i]) >> 27);
            if ((LoadWordLE(block_ptr + i * 4) & mask) == 0) {
                return false;
            }
        }
        return true;
    }

    // Whether the filter recorded any value in domain d (kDomainInt64 /
    // kDomainUtf8). A probe in an absent domain cannot be a member.
    bool
    HasDomain(uint8_t d) const {
        return (domains_ & d) != 0;
    }

    // INT8/16/32/64 values are widened to int64 and hashed as their 8-byte
    // little-endian encoding with XXH64(seed=0) — identical to parquet plain
    // encoding for INT64 and to client/membership/sbbf hashInt64.
    bool
    TestInt64(int64_t v) const {
        if (!HasDomain(kDomainInt64)) {
            return false;
        }
        uint8_t buf[8];
        const auto u = static_cast<uint64_t>(v);
        for (int i = 0; i < 8; ++i) {
            buf[i] = static_cast<uint8_t>(u >> (8 * i));
        }
        return Test(XXH64(buf, sizeof(buf), 0));
    }

    // VARCHAR values hash their raw UTF-8 bytes with XXH64(seed=0) —
    // identical to parquet plain encoding for BYTE_ARRAY and to
    // client/membership/sbbf hashString.
    bool
    TestBytes(const void* data, size_t len) const {
        if (!HasDomain(kDomainUtf8)) {
            return false;
        }
        return Test(XXH64(data, len, 0));
    }

    // Single probe dispatch for a typed scalar value: strings by raw bytes,
    // integers widened to int64. Shared by the raw-data and index-fallback
    // paths so their probe semantics cannot diverge.
    template <typename T>
    bool
    TestScalar(const T& v) const {
        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            return TestBytes(v.data(), v.size());
        } else {
            return TestInt64(static_cast<int64_t>(v));
        }
    }

 private:
    static uint32_t
    LoadWordLE(const uint8_t* p) {
        return static_cast<uint32_t>(p[0]) |
               (static_cast<uint32_t>(p[1]) << 8) |
               (static_cast<uint32_t>(p[2]) << 16) |
               (static_cast<uint32_t>(p[3]) << 24);
    }

    // Eight odd constants fixed by the parquet-format spec, mirrored from
    // parquet::BlockSplitBloomFilter::SALT.
    static constexpr uint32_t kSalt[8] = {0x47b6137bU,
                                          0x44974d91U,
                                          0x8824ad5bU,
                                          0xa2b7289dU,
                                          0x705495c7U,
                                          0x2df1424bU,
                                          0x9efc4947U,
                                          0x5c6bfb31U};

    const uint8_t* body_ = nullptr;
    uint32_t num_blocks_ = 0;
    // Domains recorded by the builder; 0 = empty filter, matches nothing.
    uint8_t domains_ = 0;
};

}  // namespace exec
}  // namespace milvus
