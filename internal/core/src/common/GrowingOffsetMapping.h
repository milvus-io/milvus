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
#include <memory>
#include <optional>
#include <vector>

#include "common/EasyAssert.h"
#include "common/OffsetMapping.h"

namespace milvus {

// Offset mapping for growing storage: rows arrive incrementally via Append()
// while searches are running against the same segment.
//
// CONCURRENCY MODEL -- single writer, lock-free readers.
//
// There is exactly ONE writer: a growing segment receives its inserts one at a
// time, in reserved logical order (querynode drives each vchannel from a single
// goroutine). Append still takes append_mutex_ so that a violation of that
// contract trips the ordering assertions instead of corrupting memory, but that
// mutex is never taken on the read path.
//
// Readers take NO lock. Both directions are dense arrays addressed by offset,
// stored in a fixed spine of geometrically growing chunks, so an append never
// moves an entry that is already visible. All publication happens through a
// single release-store to counts_: a reader acquire-loads it once, and that one
// load both fixes its (valid_count, total_count) snapshot and makes every chunk
// pointer and every element below those bounds visible. Reads above the bounds
// never happen, so there is nothing to tear.
//
// This replaces a pair of std::unordered_map guarded by a shared_mutex. The
// hash maps were the reason the lock existed at all -- a concurrent rehash
// makes find() undefined -- and at ~32 bytes per node-plus-bucket they cost
// roughly 64 bytes per VALID row across the two maps. Storage here is a
// single int32 array, physical -> logical: 4 bytes per valid row and nothing
// for null rows, so a sparse nullable column pays for the vectors it stores,
// not for every logical row (geometric chunking bounds allocation at 2x the
// payload). The logical -> physical direction is a binary search over that
// array -- it is strictly increasing -- so point lookups cost
// O(log valid_count). Those sit on result-bounded paths (fetching the rows a
// query returns). Search scan paths use ValidCountBelow for the visible
// physical bound and GetPhysicalToLogicalIds for contiguous p2l windows; they
// read physical -> logical directly and never search. An all-valid mapping
// (no nulls yet) short-circuits point lookups to the identity, and batch
// conversions gallop from a cursor on ascending inputs, so a full-range pass
// (flush, chunk views) costs O(N) rather than N binary searches. The old
// per-lookup shared_lock RMW -- a contention point across reader threads -- is
// gone either way.
class GrowingOffsetMapping final : public OffsetMapping {
 public:
    GrowingOffsetMapping();
    ~GrowingOffsetMapping() override;

    GrowingOffsetMapping(const GrowingOffsetMapping&) = delete;
    GrowingOffsetMapping&
    operator=(const GrowingOffsetMapping&) = delete;
    GrowingOffsetMapping(GrowingOffsetMapping&&) = delete;
    GrowingOffsetMapping&
    operator=(GrowingOffsetMapping&&) = delete;

    // Append `count` rows. start_logical / start_physical default to the
    // current counts; passing them explicitly lets callers assert the offsets
    // they reserved. Appends MUST arrive in ascending logical order (enforced
    // by AssertInfo) -- that is what keeps physical -> logical monotonic and
    // makes ValidCountBelow's binary search correct.
    void
    Append(const bool* valid_data,
           int64_t count,
           int64_t start_logical = -1,
           int64_t start_physical = -1);

    // Freeze the currently published (valid_count, total_count) pair in one
    // acquire load. The returned read-only view shares the append-only p2l
    // spine, but later appends cannot widen any of its lookup or transform
    // bounds. The view may outlive this writer object.
    std::shared_ptr<const OffsetMapping>
    Snapshot() const;

    // Binary search over the physical -> logical array (strictly increasing):
    // O(log valid_count). Returns -1 for null rows and out-of-range offsets.
    int64_t
    GetPhysicalOffset(int64_t logical_offset) const override;

    int64_t
    GetLogicalOffset(int64_t physical_offset) const override;

    int64_t
    GetValidCount() const override;

    // A growing mapping keeps growing under concurrent inserts, so a search
    // must NEVER use GetValidCount() as its scan bound: that count includes
    // rows published after the query fixed its visible-row bound, which are
    // neither acknowledged by ack_responder_ nor visible at the query
    // timestamp. Convert the plan-layer bound instead of asking the mapping
    // how big it is right now.
    //
    // Append assigns physical offsets in ascending logical order, so p2l is
    // monotonic and this is a plain binary search over a dense array.
    int64_t
    ValidCountBelow(int64_t logical_bound) const override;

    bool
    IsEnabled() const override;

    int64_t
    GetTotalCount() const override;

    BitsetTransformStatus
    TransformBitset(const BitsetView& bitset,
                    TargetBitmap& result) const override;

    void
    TransformOffsets(std::vector<int64_t>& offsets) const override;

    void
    TransformLogicalOffsets(std::vector<int64_t>& offsets) const override;

    OffsetMappingIdView
    GetPhysicalToLogicalIds(int64_t physical_offset,
                            int64_t count) const override;

    void
    FilterValidLogicalOffsets(
        const int64_t* logical_offsets,
        int64_t count,
        bool* valid_data,
        std::vector<int64_t>& physical_offsets) const override;

 private:
    struct Counts {
        int64_t valid;
        int64_t total;
    };

    class State;

    GrowingOffsetMapping(std::shared_ptr<State> state, Counts counts);

    static uint64_t
    PackCounts(int64_t valid, int64_t total) {
        return (static_cast<uint64_t>(static_cast<uint32_t>(valid)) << 32) |
               static_cast<uint32_t>(total);
    }

    Counts
    LoadCounts() const;

    int64_t
    GetPhysicalOffsetInternal(int64_t logical_offset,
                              const Counts& counts) const;

    int64_t
    GetLogicalOffsetInternal(int64_t physical_offset,
                             int64_t valid_count) const;

    int64_t
    LowerBound(int64_t logical_target, int64_t valid_count) const;

    int64_t
    GallopLowerBound(int64_t logical_target, int64_t from, int64_t bound) const;

    // State owns the fixed p2l spine and publication counter. Snapshot views
    // retain it after this writer is destroyed; writer objects themselves stay
    // non-copyable so there is still exactly one append entry point.
    std::shared_ptr<State> state_;
    std::optional<Counts> frozen_counts_;
};

}  // namespace milvus
