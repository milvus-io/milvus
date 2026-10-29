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

#include "common/GrowingOffsetMapping.h"

#include <algorithm>
#include <limits>
#include <mutex>

#include "common/EasyAssert.h"

namespace milvus {

void
GrowingOffsetMapping::ChunkedArray::Reserve(int64_t capacity) {
    if (capacity <= 0) {
        return;
    }
    const int last_chunk = ChunkOf(capacity - 1);
    AssertInfo(last_chunk < kMaxChunks,
               "growing offset mapping capacity {} exceeds the addressable "
               "range ({} chunks)",
               capacity,
               kMaxChunks);
    for (int chunk = 0; chunk <= last_chunk; ++chunk) {
        if (chunks_[chunk].load(std::memory_order_relaxed) != nullptr) {
            continue;
        }
        const int64_t entries = kFirstChunk << chunk;
        auto* buffer = new int32_t[entries];
        std::fill_n(buffer, entries, kUnset);
        chunks_[chunk].store(buffer, std::memory_order_release);
    }
}

void
GrowingOffsetMapping::Append(const bool* valid_data,
                             int64_t count,
                             int64_t start_logical,
                             int64_t start_physical) {
    if (count == 0 || valid_data == nullptr) {
        return;
    }

    // Writer-vs-writer only. Readers are lock-free and see nothing until the
    // release-store to counts_ at the bottom of this function.
    std::lock_guard<std::mutex> writer(append_mutex_);
    const auto counts = LoadCounts();
    if (start_logical < 0) {
        start_logical = counts.total;
    }
    auto physical_idx = start_physical >= 0 ? start_physical : counts.valid;
    // Physical offsets MUST be claimed in ascending logical order, which is
    // what makes p2l monotonic and ValidCountBelow's binary search valid.
    //
    // Nothing downstream re-establishes that order, so these two assertions
    // ARE the enforcement -- there is no gate that would make an out-of-order
    // Append correct. The contract holds because the querynode pipeline
    // drives a vchannel from a single goroutine (streamPipeline::work ->
    // insertNode::Operate -> ProcessInsert), so a growing segment sees its
    // Insert calls one at a time, in reserved logical order.
    //
    // They also subsume the duplicate-offset checks a hash-map representation
    // needed: appending exactly at the current counts cannot collide.
    AssertInfo(physical_idx == counts.valid,
               "growing offset mapping requires physical offsets to be claimed "
               "in logical order: got start_physical={}, expected {}. A "
               "growing segment holding a nullable vector field must not "
               "receive concurrent or out-of-order Insert calls.",
               physical_idx,
               counts.valid);
    AssertInfo(start_logical == counts.total,
               "growing offset mapping requires logical offsets to be appended "
               "in order: got start_logical={}, expected {}. A growing segment "
               "holding a nullable vector field must not receive concurrent or "
               "out-of-order Insert calls.",
               start_logical,
               counts.total);

    const int64_t new_total = start_logical + count;
    // Offsets are stored as int32_t, as they were in the hash-map layout.
    // Fail loudly at the boundary instead of silently truncating.
    AssertInfo(new_total <= std::numeric_limits<int32_t>::max(),
               "growing offset mapping row count {} exceeds the int32 offset "
               "range",
               new_total);

    // Allocate before publishing anything, and only what this batch's valid
    // rows need. Reserving the all-valid worst case would park the whole
    // reservation idle on an all-null batch -- schema-evolution backfill
    // appends every pre-existing row of a new nullable vector field as one
    // such batch, which at 10M rows is ~64 MiB of chunks nothing ever reads,
    // per field, per growing segment.
    const int64_t batch_valid =
        std::count(valid_data, valid_data + count, true);
    p2l_.Reserve(counts.valid + batch_valid);

    for (int64_t i = 0; i < count; ++i) {
        if (valid_data[i]) {
            p2l_.Set(physical_idx, static_cast<int32_t>(start_logical + i));
            ++physical_idx;
        }
    }

    // Single publication point: this release-store is what makes the chunk
    // pointers and every element written above visible to a reader that
    // acquire-loads counts_.
    counts_.store(PackCounts(physical_idx, new_total),
                  std::memory_order_release);
}

int64_t
GrowingOffsetMapping::GetPhysicalOffset(int64_t logical_offset) const {
    const auto counts = LoadCounts();
    if (counts.total == 0) {
        return logical_offset;
    }
    return GetPhysicalOffsetInternal(logical_offset, counts);
}

int64_t
GrowingOffsetMapping::GetPhysicalOffsetInternal(int64_t logical_offset,
                                                const Counts& counts) const {
    if (logical_offset < 0 || logical_offset >= counts.total) {
        return -1;
    }
    if (counts.valid == counts.total) {
        // No nulls yet: the mapping is the identity. Keeps point lookups on
        // a nullable column that has not actually received a null at O(1).
        return logical_offset;
    }
    // An exact hit in p2l_ is the row's physical slot; a miss means the row
    // is null -- the same "not found" answer the hash-map layout gave by
    // having no entry at all.
    const int64_t pos = LowerBound(logical_offset, counts.valid);
    if (pos < counts.valid &&
        static_cast<int64_t>(p2l_.Get(pos)) == logical_offset) {
        return pos;
    }
    return -1;
}

int64_t
GrowingOffsetMapping::GetLogicalOffset(int64_t physical_offset) const {
    const auto counts = LoadCounts();
    if (counts.total == 0) {
        return physical_offset;
    }
    return GetLogicalOffsetInternal(physical_offset, counts.valid);
}

int64_t
GrowingOffsetMapping::GetLogicalOffsetInternal(int64_t physical_offset,
                                               int64_t valid_count) const {
    if (physical_offset < 0 || physical_offset >= valid_count) {
        return -1;
    }
    return p2l_.Get(physical_offset);
}

int64_t
GrowingOffsetMapping::GetValidCount() const {
    return LoadCounts().valid;
}

bool
GrowingOffsetMapping::IsEnabled() const {
    // Append returns early on an empty batch and otherwise pushes total_count
    // above zero, so "has logical rows" and "has a mapping" are the same
    // predicate; keeping a separate flag would only add a second thing to
    // publish atomically.
    return LoadCounts().total > 0;
}

int64_t
GrowingOffsetMapping::GetTotalCount() const {
    return LoadCounts().total;
}

int64_t
GrowingOffsetMapping::ValidCountBelow(int64_t logical_bound) const {
    const auto counts = LoadCounts();
    if (counts.total == 0) {
        // No mapping: logical and physical spaces coincide.
        return std::max<int64_t>(0, logical_bound);
    }
    if (logical_bound <= 0) {
        return 0;
    }
    if (logical_bound >= counts.total) {
        return counts.valid;
    }
    // Append walks rows in ascending logical order and assigns physical
    // offsets with physical_idx++, so physical -> logical is strictly
    // increasing. The first physical row whose logical offset has reached
    // the bound sits at the count of rows strictly below it.
    return LowerBound(logical_bound, counts.valid);
}

int64_t
GrowingOffsetMapping::LowerBound(int64_t logical_target,
                                 int64_t valid_count) const {
    int64_t lo = 0;
    int64_t hi = valid_count;
    while (lo < hi) {
        const int64_t mid = lo + (hi - lo) / 2;
        if (static_cast<int64_t>(p2l_.Get(mid)) < logical_target) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

int64_t
GrowingOffsetMapping::GallopLowerBound(int64_t logical_target,
                                       int64_t from,
                                       int64_t bound) const {
    if (from >= bound ||
        static_cast<int64_t>(p2l_.Get(from)) >= logical_target) {
        // Also the duplicate-input case: the previous answer still holds.
        return from;
    }
    // p2l_[lo] < target from here on; widen exponentially, then binary
    // search the bracketed window.
    int64_t lo = from;
    int64_t step = 1;
    while (lo + step < bound &&
           static_cast<int64_t>(p2l_.Get(lo + step)) < logical_target) {
        lo += step;
        step <<= 1;
    }
    int64_t hi = std::min(lo + step, bound);
    ++lo;
    while (lo < hi) {
        const int64_t mid = lo + (hi - lo) / 2;
        if (static_cast<int64_t>(p2l_.Get(mid)) < logical_target) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

OffsetMappingIdView
GrowingOffsetMapping::GetPhysicalToLogicalIds(int64_t physical_offset,
                                              int64_t count) const {
    const auto counts = LoadCounts();
    if (counts.total == 0 || count <= 0) {
        return {};
    }
    AssertInfo(physical_offset >= 0,
               "physical offset {} is negative",
               physical_offset);
    AssertInfo(physical_offset < counts.valid,
               "physical offset {} exceeds valid count {}",
               physical_offset,
               counts.valid);
    const auto available = counts.valid - physical_offset;
    return p2l_.View(physical_offset, std::min(count, available));
}

void
GrowingOffsetMapping::FilterValidLogicalOffsets(
    const int64_t* logical_offsets,
    int64_t count,
    bool* valid_data,
    std::vector<int64_t>& physical_offsets) const {
    const auto counts = LoadCounts();
    physical_offsets.clear();
    physical_offsets.reserve(count);
    if (counts.total == 0) {
        // No mapping: logical and physical spaces coincide. Mirror the other
        // conversions (and the NoOp/Sealed implementations) instead of
        // rejecting every offset against total_count == 0.
        for (int64_t i = 0; i < count; ++i) {
            const bool valid = logical_offsets[i] >= 0;
            valid_data[i] = valid;
            if (valid) {
                physical_offsets.push_back(logical_offsets[i]);
            }
        }
        return;
    }
    if (counts.valid == counts.total) {
        // No nulls yet: identity, minus the range check.
        for (int64_t i = 0; i < count; ++i) {
            const auto offset = logical_offsets[i];
            const bool valid = offset >= 0 && offset < counts.total;
            valid_data[i] = valid;
            if (valid) {
                physical_offsets.push_back(offset);
            }
        }
        return;
    }

    // Full-range passes (flush, chunk views) hand in ascending offsets, so
    // gallop from the previous hit: the whole batch then costs O(count +
    // valid) instead of one binary search per element. An out-of-order
    // element falls back to a fresh search and re-seeds the cursor.
    int64_t hint = 0;
    int64_t prev = std::numeric_limits<int64_t>::min();
    for (int64_t i = 0; i < count; ++i) {
        const auto offset = logical_offsets[i];
        if (offset < 0 || offset >= counts.total) {
            valid_data[i] = false;
            continue;
        }
        const int64_t pos = offset >= prev
                                ? GallopLowerBound(offset, hint, counts.valid)
                                : LowerBound(offset, counts.valid);
        hint = pos;
        prev = offset;
        const bool valid =
            pos < counts.valid && static_cast<int64_t>(p2l_.Get(pos)) == offset;
        valid_data[i] = valid;
        if (valid) {
            physical_offsets.push_back(pos);
        }
    }
}

}  // namespace milvus
