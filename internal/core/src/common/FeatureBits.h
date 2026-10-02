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

#include <atomic>
#include <cstdint>

namespace milvus {

// FeatureBit names one execution feature a request can use: how a filter
// was evaluated, which kind of scalar index served it, whether the interim
// index served a vector search, and so on. The values are the bit positions
// of the feature bit set reported to the Proxy, and the wire format shared
// with execBitFeatures in internal/featureusage/execbits.go, whose test parses
// this enum. Append only: a bit keeps its meaning across versions.
enum class FeatureBit : uint32_t {
    FilterPathScalarIndex = 0,
    FilterPathPkIndex = 1,
    FilterPathTextMatchIndex = 2,
    FilterPathJsonShredding = 3,
    FilterPathNgramIndex = 4,
    FilterPathBruteForce = 5,
    ScalarIndexBitmap = 6,
    ScalarIndexStlSort = 7,
    ScalarIndexTrie = 8,
    ScalarIndexInverted = 9,
    ScalarIndexHybrid = 10,
    ScalarIndexRtree = 11,
    ScalarIndexNgram = 12,
    ScalarIndexJsonFlat = 13,
    FilterIndexDeclined = 14,
    ExprCacheHit = 15,
    InterimIndexSearch = 16,
    StrictGroupSizeEffective = 17,
    // Set by the QueryNode in Go, never by segcore.
    TieredStorageColdRead = 18,
    ScalarIndexFmindex = 19,
};

// FeatureRecorder collects the FeatureBits one request used on one
// QueryNode. It is owned by the plan node, created only when the plan asks
// for it (PlanOption.collect_feature_bits), and shared by every segment the
// plan runs on, possibly concurrently, hence the atomic. A feature already
// recorded costs one relaxed load; the read-modify-write happens at most
// once per bit per request.
class FeatureRecorder {
 public:
    void
    Mark(FeatureBit bit) noexcept {
        const uint64_t mask = uint64_t{1} << static_cast<uint32_t>(bit);
        if ((bits_.load(std::memory_order_relaxed) & mask) == 0) {
            bits_.fetch_or(mask, std::memory_order_relaxed);
        }
    }

    uint64_t
    Bits() const noexcept {
        return bits_.load(std::memory_order_relaxed);
    }

 private:
    std::atomic<uint64_t> bits_{0};
};

// MarkFeature records bit on recorder when the request collects features;
// recorder is null otherwise, and this is one branch.
inline void
MarkFeature(FeatureRecorder* recorder, FeatureBit bit) noexcept {
    if (recorder != nullptr) {
        recorder->Mark(bit);
    }
}

}  // namespace milvus
