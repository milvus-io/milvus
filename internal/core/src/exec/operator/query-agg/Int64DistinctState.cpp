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

#include "exec/operator/query-agg/Int64DistinctState.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "common/EasyAssert.h"
#include "common/OpContext.h"

namespace milvus::exec {

namespace {

// Mirrors ankerl::unordered_dense's bucket sizing (calc_shifts_for_size with
// the default max load factor): the smallest power-of-two bucket count, at
// least 4, whose load-factor capacity holds `capacity` values.
// Int64DistinctStateTest checks MemoryUsageBytes() against this projection.
constexpr float kDistinctTableMaxLoadFactor = 0.8F;

uint64_t
DistinctTableBucketsForCapacity(uint64_t capacity, uint64_t max_buckets) {
    constexpr int kInitialShifts = 64 - 2;
    auto buckets = [max_buckets](int shifts) {
        return std::min(max_buckets, uint64_t{1} << (64 - shifts));
    };
    int shifts = kInitialShifts;
    while (shifts > 1 &&
           static_cast<uint64_t>(static_cast<float>(buckets(shifts)) *
                                 kDistinctTableMaxLoadFactor) < capacity) {
        --shifts;
    }
    return buckets(shifts);
}

}  // namespace

Int64DistinctState::Int64DistinctState(const Int64DistinctStateLimits& limits,
                                       milvus::OpContext* op_ctx)
    : limits_(limits), op_ctx_(op_ctx) {
    AssertInfo(limits_.max_entries >= 0,
               "distinct state max_entries must be non-negative, got {}",
               limits_.max_entries);
    AssertInfo(limits_.max_memory_bytes >= ProjectedBytesForCapacity(0),
               "distinct state max_memory_bytes must be at least {} (the "
               "empty table), got {}",
               ProjectedBytesForCapacity(0),
               limits_.max_memory_bytes);
}

int64_t
Int64DistinctState::ProjectedBytesForCapacity(int64_t capacity) {
    AssertInfo(capacity >= 0,
               "distinct state capacity must be non-negative, got {}",
               capacity);
    if (static_cast<uint64_t>(capacity) > Set::max_size()) {
        // More values than the table can index; never fits a budget.
        return std::numeric_limits<int64_t>::max();
    }
    const auto buckets = DistinctTableBucketsForCapacity(
        static_cast<uint64_t>(capacity), Set::max_bucket_count());
    return capacity * static_cast<int64_t>(sizeof(int64_t)) +
           static_cast<int64_t>(buckets * sizeof(Set::bucket_type));
}

int64_t
Int64DistinctState::MemoryUsageBytes() const {
    return static_cast<int64_t>(set_.values().capacity() * sizeof(int64_t) +
                                set_.bucket_count() * sizeof(Set::bucket_type));
}

void
Int64DistinctState::CheckUsable() const {
    if (failed_) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "distinct state is unusable after a failed AddBatch");
    }
}

void
Int64DistinctState::CheckCancelled() const {
    if (op_ctx_ != nullptr &&
        op_ctx_->cancellation_token.isCancellationRequested()) {
        ThrowInfo(ErrorCode::FollyCancel, "distinct state build cancelled");
    }
}

void
Int64DistinctState::Grow() {
    const auto size = static_cast<int64_t>(set_.size());
    if (size >= limits_.max_entries) {
        ThrowInfo(ErrorCode::InsufficientResource,
                  "distinct state exceeds its entry limit: {} distinct values "
                  "allowed",
                  limits_.max_entries);
    }

    // Double the reservation, clamped to the entry limit.
    int64_t target = kInitialCapacity;
    if (reserved_capacity_ > 0) {
        target = reserved_capacity_ > limits_.max_entries / 2
                     ? limits_.max_entries
                     : reserved_capacity_ * 2;
    }
    target = std::min(target, limits_.max_entries);

    if (ProjectedBytesForCapacity(target) > limits_.max_memory_bytes) {
        // Use the budget fully before rejecting: find the largest capacity in
        // (size, target) that still fits. The projection is monotonic.
        int64_t lo = size + 1;
        int64_t hi = target - 1;
        int64_t best = 0;
        while (lo <= hi) {
            const int64_t mid = lo + (hi - lo) / 2;
            if (ProjectedBytesForCapacity(mid) <= limits_.max_memory_bytes) {
                best = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        if (best == 0) {
            ThrowInfo(ErrorCode::InsufficientResource,
                      "distinct state exceeds its memory limit: {} distinct "
                      "values need more than {} bytes",
                      size + 1,
                      limits_.max_memory_bytes);
        }
        target = best;
    }

    set_.reserve(static_cast<size_t>(target));
    reserved_capacity_ = target;
}

void
Int64DistinctState::Fail() noexcept {
    failed_ = true;
    has_null_ = false;
    reserved_capacity_ = 0;
    try {
        Set().swap(set_);
    } catch (...) {
        // Allocating the empty table failed; drop the values at least.
        set_.clear();
    }
}

void
Int64DistinctState::AddBatch(const int64_t* values,
                             int64_t num_rows,
                             const TargetBitmapView* valid,
                             const TargetBitmapView* excluded) {
    CheckUsable();
    AssertInfo(num_rows >= 0,
               "distinct state batch size must be non-negative, got {}",
               num_rows);
    AssertInfo(values != nullptr || num_rows == 0,
               "distinct state batch of {} rows has no values",
               num_rows);
    AssertInfo(
        valid == nullptr || static_cast<int64_t>(valid->size()) == num_rows,
        "distinct state validity bitmap has {} bits for {} rows",
        valid == nullptr ? 0 : valid->size(),
        num_rows);
    AssertInfo(excluded == nullptr ||
                   static_cast<int64_t>(excluded->size()) == num_rows,
               "distinct state exclusion bitmap has {} bits for {} rows",
               excluded == nullptr ? 0 : excluded->size(),
               num_rows);

    try {
        CheckCancelled();
        for (int64_t row = 0; row < num_rows; ++row) {
            if (row > 0 && row % kCancellationCheckInterval == 0) {
                CheckCancelled();
            }
            if (excluded != nullptr && (*excluded)[row]) {
                continue;
            }
            if (valid != nullptr && !(*valid)[row]) {
                has_null_ = true;
                continue;
            }
            const int64_t value = values[row];
            if (static_cast<int64_t>(set_.size()) == reserved_capacity_) {
                if (set_.contains(value)) {
                    continue;
                }
                Grow();
            }
            set_.insert(value);
        }
    } catch (...) {
        Fail();
        throw;
    }
}

int64_t
Int64DistinctState::DistinctCount() const {
    CheckUsable();
    return static_cast<int64_t>(set_.size()) + (has_null_ ? 1 : 0);
}

int64_t
Int64DistinctState::NonNullCount() const {
    CheckUsable();
    return static_cast<int64_t>(set_.size());
}

bool
Int64DistinctState::HasNull() const {
    CheckUsable();
    return has_null_;
}

const std::vector<int64_t>&
Int64DistinctState::Values() const {
    CheckUsable();
    return set_.values();
}

Int64DistinctSnapshot
Int64DistinctState::ExportSnapshot() const {
    CheckUsable();
    Int64DistinctSnapshot snapshot;
    snapshot.values = set_.values();
    std::sort(snapshot.values.begin(), snapshot.values.end());
    snapshot.has_null = has_null_;
    return snapshot;
}

}  // namespace milvus::exec
