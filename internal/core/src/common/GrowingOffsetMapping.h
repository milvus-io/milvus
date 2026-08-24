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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <mutex>
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
    // Append-only array of int32_t for one writer and many lock-free readers.
    //
    // The spine is a FIXED array of chunk pointers -- it is never reallocated,
    // so a reader can index it without synchronising against the writer -- and
    // chunk c holds (kFirstChunk << c) entries, so total allocation stays
    // within 2x of the rows actually stored while a small mapping still starts
    // at one 4KB chunk.
    //
    // Reserve()/Set() are writer-only. Get() is safe for any index the owner
    // has published via its release-store to counts_; the matching acquire is
    // what orders the chunk pointer and element stores before it, which is why
    // the loads here can be relaxed.
    class ChunkedArray {
     public:
        static constexpr int64_t kFirstChunkLog2 = 10;
        static constexpr int64_t kFirstChunk = int64_t{1} << kFirstChunkLog2;
        // 22 chunks already cover the whole int32 offset range Append allows
        // (kFirstChunk * (2^22 - 1) > 2^31); 24 leaves margin at 8 bytes per
        // spine slot, so Reserve's bound check can never be the thing that
        // fires first.
        static constexpr int kMaxChunks = 24;
        // Fill value for freshly allocated slots. Never observable: Append
        // writes every slot below the published bound and readers never look
        // above it. It exists so a future partial-write bug shows up as an
        // absurd offset rather than as a plausible 0 or as -1 ("null row").
        static constexpr int32_t kUnset = std::numeric_limits<int32_t>::min();

        ChunkedArray() {
            for (auto& chunk : chunks_) {
                chunk.store(nullptr, std::memory_order_relaxed);
            }
        }

        ~ChunkedArray() {
            for (auto& chunk : chunks_) {
                delete[] chunk.load(std::memory_order_relaxed);
            }
        }

        ChunkedArray(const ChunkedArray&) = delete;
        ChunkedArray&
        operator=(const ChunkedArray&) = delete;

        // Writer only. Allocates every chunk needed to address `capacity`
        // entries. Idempotent, and safe to call while readers run: a chunk is
        // published before any element in it is, and neither is reachable
        // until counts_ moves.
        void
        Reserve(int64_t capacity);

        // Writer only; `index` must lie inside the reserved capacity. Neither
        // Set nor Get bounds-checks the chunk index: Reserve() is the single
        // enforcement point (it asserts against kMaxChunks), Append always
        // reserves the full batch before writing any of it, and readers never
        // look above the published counts. Re-checking per element would put a
        // branch on the per-row read path to restate an invariant that is
        // already established.
        void
        Set(int64_t index, int32_t value) {
            const int chunk = ChunkOf(index);
            chunks_[chunk].load(
                std::memory_order_relaxed)[PosOf(index, chunk)] = value;
        }

        int32_t
        Get(int64_t index) const {
            const int chunk = ChunkOf(index);
            return chunks_[chunk].load(
                std::memory_order_relaxed)[PosOf(index, chunk)];
        }

        OffsetMappingIdView
        View(int64_t index, int64_t count) const {
            if (count <= 0) {
                return {};
            }
            AssertInfo(
                index >= 0, "offset mapping index {} is negative", index);
            const int chunk = ChunkOf(index);
            auto* data = chunks_[chunk].load(std::memory_order_relaxed);
            AssertInfo(data != nullptr,
                       "growing offset mapping chunk {} is not allocated",
                       chunk);
            const auto pos = PosOf(index, chunk);
            const auto chunk_size = kFirstChunk << chunk;
            return {data + pos, std::min<int64_t>(count, chunk_size - pos)};
        }

     private:
        // Entry i lives in chunk floor(log2(i / kFirstChunk + 1)) because the
        // chunks form a geometric series: chunks 0..c-1 together hold
        // kFirstChunk * (2^c - 1) entries.
        static int
        ChunkOf(int64_t index) {
            const auto scaled =
                static_cast<uint64_t>(index) / kFirstChunk + 1;  // >= 1
            return 63 - __builtin_clzll(scaled);
        }

        static int64_t
        PosOf(int64_t index, int chunk) {
            return index - kFirstChunk * ((int64_t{1} << chunk) - 1);
        }

        std::atomic<int32_t*> chunks_[kMaxChunks];
    };

    // A (valid_count, total_count) pair read from one atomic load, so the two
    // can never come from different generations. Both fit in 32 bits: the
    // mapping stores offsets as int32_t and Append rejects anything larger.
    struct Counts {
        int64_t valid;
        int64_t total;
    };

    static uint64_t
    PackCounts(int64_t valid, int64_t total) {
        return (static_cast<uint64_t>(static_cast<uint32_t>(valid)) << 32) |
               static_cast<uint32_t>(total);
    }

    Counts
    LoadCounts() const {
        const uint64_t packed = counts_.load(std::memory_order_acquire);
        return Counts{static_cast<int64_t>(packed >> 32),
                      static_cast<int64_t>(packed & 0xffffffffULL)};
    }

    // Both take the counts snapshot as a parameter so callers can read it
    // once per batch instead of per element.
    int64_t
    GetPhysicalOffsetInternal(int64_t logical_offset,
                              const Counts& counts) const;

    int64_t
    GetLogicalOffsetInternal(int64_t physical_offset,
                             int64_t valid_count) const;

    // First physical index whose logical offset is >= logical_target, over
    // p2l_[0, valid_count). Shared by ValidCountBelow (the answer IS the
    // count) and GetPhysicalOffset (an exact hit is the row's physical slot,
    // a miss means the row is null).
    int64_t
    LowerBound(int64_t logical_target, int64_t valid_count) const;

    // LowerBound restricted to [from, bound), found by exponential search
    // from `from`. Precondition: the answer is >= from -- i.e. `from` is a
    // lower bound for some target <= logical_target. Lets an ascending batch
    // pay O(log gap) per element instead of O(log valid_count).
    int64_t
    GallopLowerBound(int64_t logical_target, int64_t from, int64_t bound) const;

    // Writer-vs-writer only; readers never take it. Present so that a caller
    // that breaks the single-writer contract hits Append's ordering assertions
    // instead of racing on chunk allocation.
    std::mutex append_mutex_;

    // physical -> logical, strictly increasing. The only stored direction;
    // logical -> physical is answered by binary search over it (LowerBound).
    ChunkedArray p2l_;

    // Packed (valid_count, total_count). The ONLY publication point: every
    // store is a release, every read is one acquire load. total_count == 0
    // means "no mapping yet", which is what IsEnabled() reports -- Append
    // returns early on an empty batch, so a mapping can never be enabled with
    // zero logical rows.
    std::atomic<uint64_t> counts_{0};
};

}  // namespace milvus
