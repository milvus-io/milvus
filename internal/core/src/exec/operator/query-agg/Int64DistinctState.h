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
#include <vector>

#include "ankerl/unordered_dense.h"
#include "common/Types.h"

namespace milvus {
struct OpContext;
}  // namespace milvus

namespace milvus::exec {

// Resource limits of one Int64DistinctState. Both limits are hard: a batch
// that would need more is rejected with ErrorCode::InsufficientResource; the
// state is never truncated. NULL is kept as a flag and counts against neither
// limit.
struct Int64DistinctStateLimits {
    // Maximum number of distinct non-NULL values held.
    int64_t max_entries;
    // Maximum bytes of hash-table storage (value array plus bucket array) the
    // state may reserve. See Int64DistinctState::MemoryUsageBytes().
    int64_t max_memory_bytes;
};

// Values of a finished Int64DistinctState: unique non-NULL values in
// ascending order, plus whether at least one counted row was NULL.
struct Int64DistinctSnapshot {
    std::vector<int64_t> values;
    bool has_null = false;
};

// Exact distinct state for one Int64 column (Stanford DT01, #53848).
//
// The state consumes batches of values that have already passed filtering
// and MVCC visibility, or carries that decision in an exclusion bitmap. It
// deduplicates across batches, collapses all NULL rows into one NULL entry
// (result-DISTINCT semantics) and exposes the unique values. It is a local,
// single-threaded building block: it has no knowledge of plans, segments,
// or the distributed reduce, and is not thread-safe.
//
// Failure is explicit. If a batch exceeds a limit, or the operation is
// cancelled, AddBatch throws, releases the table memory, and the state
// becomes unusable: every later call except MemoryUsageBytes() and Failed()
// throws, so a partial result can never be read as an exact one.
class Int64DistinctState {
 public:
    // Rows between two cancellation checks inside one batch.
    static constexpr int64_t kCancellationCheckInterval = 1024;
    // Capacity of the first table allocation, before clamping to the limits.
    static constexpr int64_t kInitialCapacity = 64;

    // op_ctx may be nullptr, which disables cancellation checks. When set, it
    // must outlive the state.
    explicit Int64DistinctState(const Int64DistinctStateLimits& limits,
                                milvus::OpContext* op_ctx = nullptr);

    Int64DistinctState(const Int64DistinctState&) = delete;
    Int64DistinctState&
    operator=(const Int64DistinctState&) = delete;
    Int64DistinctState(Int64DistinctState&&) = default;
    Int64DistinctState&
    operator=(Int64DistinctState&&) = default;
    ~Int64DistinctState() = default;

    // Adds rows [0, num_rows) of one batch.
    //   values:   num_rows values; may be nullptr only when num_rows == 0.
    //             The value of a NULL row is ignored.
    //   valid:    validity bitmap, bit set = value present. nullptr means
    //             every row is non-NULL (a non-nullable column).
    //   excluded: filter/visibility bitmap with the exec pipeline's polarity
    //             (FilterBitsNode/MvccNode output): bit set = row filtered
    //             out, deleted or invisible, and skipped. nullptr means every
    //             row counts.
    // A non-null bitmap must hold exactly num_rows bits.
    void
    AddBatch(const int64_t* values,
             int64_t num_rows,
             const TargetBitmapView* valid,
             const TargetBitmapView* excluded);

    // Number of distinct result rows: unique non-NULL values, plus one if a
    // NULL row was seen. This is the query-result DISTINCT cardinality.
    int64_t
    DistinctCount() const;

    // Number of unique non-NULL values (the COUNT(DISTINCT x) input).
    int64_t
    NonNullCount() const;

    // True when at least one counted row was NULL.
    bool
    HasNull() const;

    // Unique non-NULL values in first-seen order, without copying. The
    // reference is invalidated by the next AddBatch.
    const std::vector<int64_t>&
    Values() const;

    // Copy of the state with values sorted ascending; deterministic, so
    // callers and tests can compare it directly.
    Int64DistinctSnapshot
    ExportSnapshot() const;

    // Bytes currently reserved by the hash table: value-array capacity plus
    // bucket array. Never exceeds limits.max_memory_bytes.
    int64_t
    MemoryUsageBytes() const;

    // Bytes the hash table reserves for `capacity` values. Exposed so tests
    // and callers can size limits; MemoryUsageBytes() equals this for the
    // capacity reserved by the state.
    static int64_t
    ProjectedBytesForCapacity(int64_t capacity);

    bool
    Failed() const {
        return failed_;
    }

    const Int64DistinctStateLimits&
    Limits() const {
        return limits_;
    }

 private:
    using Set = ankerl::unordered_dense::set<int64_t>;

    void
    CheckUsable() const;

    void
    CheckCancelled() const;

    // Makes room for one more value or throws InsufficientResource.
    void
    Grow();

    // Releases table memory and marks the state unusable.
    void
    Fail() noexcept;

    Int64DistinctStateLimits limits_;
    milvus::OpContext* op_ctx_;
    Set set_;
    // Values the table can hold without a reallocation.
    int64_t reserved_capacity_ = 0;
    bool has_null_ = false;
    bool failed_ = false;
};

}  // namespace milvus::exec
