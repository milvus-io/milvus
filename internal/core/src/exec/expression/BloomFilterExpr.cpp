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

#include "BloomFilterExpr.h"

#include "common/EasyAssert.h"

namespace milvus {
namespace exec {

SplitBlockBloomFilterView
SplitBlockBloomFilterView::Parse(std::string_view blob) {
    if (blob.size() < kHeaderSize) {
        ThrowInfo(ExprInvalid,
                  "bloom filter blob too short: {} bytes, need at least {}",
                  blob.size(),
                  kHeaderSize);
    }
    // Defensive upper bound on the whole envelope, checked before any field
    // is read. The proxy is the operator-tunable gate: it rejects blobs above
    // proxy.maxMembershipFilterSize (default 64 MiB) at plan-build time and
    // validates the envelope via sbbf.Parse (same 128 MB format cap mirroring
    // Arrow's kMaximumBloomFilterBytes); in practice the blob is also bounded
    // by the gRPC transport limit. But a hand-crafted plan can reach segcore
    // directly; cap the blob at header + kMaxFilterBytes so a hostile plan
    // cannot force an unbounded body.
    // The request content (the client/proxy-supplied blob) is to blame, so this
    // is an input/parameter error, never a system error.
    constexpr uint64_t kMaxBlobSize = kHeaderSize + kMaxFilterBytes;
    if (blob.size() > kMaxBlobSize) {
        ThrowInfo(ExprInvalid,
                  "bloom filter blob too large: {} bytes, exceeds max {} "
                  "(header {} + body {})",
                  blob.size(),
                  kMaxBlobSize,
                  kHeaderSize,
                  kMaxFilterBytes);
    }
    const auto* p = reinterpret_cast<const uint8_t*>(blob.data());
    if (blob.substr(0, 4) != "MBF1") {
        ThrowInfo(ExprInvalid,
                  "bloom filter blob has invalid magic, expected \"MBF1\"");
    }
    if (auto version = bloom_envelope::LoadU16LE(p + 4); version != kVersion) {
        ThrowInfo(ExprInvalid,
                  "unsupported bloom filter version {}, expected {}",
                  version,
                  kVersion);
    }
    if (auto algo = bloom_envelope::LoadU16LE(p + 6);
        algo != kAlgoParquetSbbfXxh64) {
        ThrowInfo(ExprInvalid,
                  "unsupported bloom filter algo {}, expected {}",
                  algo,
                  kAlgoParquetSbbfXxh64);
    }
    const uint8_t domains = p[28];
    if ((domains & ~kDomainKnown) != 0) {
        ThrowInfo(ExprInvalid,
                  "bloom filter declares unknown value domains {:#04x}, known "
                  "bits {:#04x}",
                  static_cast<uint32_t>(domains),
                  static_cast<uint32_t>(kDomainKnown));
    }
    if (auto reserved = static_cast<uint8_t>(p[29] | p[30] | p[31]);
        reserved != 0) {
        ThrowInfo(ExprInvalid,
                  "bloom filter reserved field must be 0, got {}",
                  reserved);
    }
    const uint32_t num_blocks = bloom_envelope::LoadU32LE(p + 24);
    // SBBF invariant (parquet OptimalNumOfBytes): the filter body is a
    // power-of-two number of bytes in [32, kMaxFilterBytes], hence
    // num_blocks is a power of two in [1, kMaxFilterBytes / 32].
    constexpr uint64_t kMaxBlocks = kMaxFilterBytes / kBytesPerBlock;
    if (num_blocks == 0 || (num_blocks & (num_blocks - 1)) != 0 ||
        num_blocks > kMaxBlocks) {
        ThrowInfo(ExprInvalid,
                  "bloom filter num_blocks {} is not a power of two in "
                  "[1, {}]",
                  num_blocks,
                  kMaxBlocks);
    }
    const uint64_t body_len = blob.size() - kHeaderSize;
    if (body_len != static_cast<uint64_t>(num_blocks) * kBytesPerBlock) {
        ThrowInfo(ExprInvalid,
                  "bloom filter body length {} does not match num_blocks {} "
                  "(want {} bytes)",
                  body_len,
                  num_blocks,
                  static_cast<uint64_t>(num_blocks) * kBytesPerBlock);
    }
    SplitBlockBloomFilterView view;
    view.body_ = p + kHeaderSize;
    view.num_blocks_ = num_blocks;
    view.domains_ = domains;
    return view;
}

}  // namespace exec
}  // namespace milvus
