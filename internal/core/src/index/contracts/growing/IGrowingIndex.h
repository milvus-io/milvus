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
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

#include "common/EasyAssert.h"
#include "index/contracts/query/IIndexReaderBase.h"

namespace milvus::index {

// An opaque lifetime pin on ONE published reader record and its coverage.
// Count, validity/offset mapping and dependency lifetimes belong to that record;
// the family decides whether the query engine itself is immutable. Copies retain
// the record; moves leave the source empty. Reader references and query-interface
// pointers must not outlive the pin. See README.md.
class GrowingIndexSnapshotPin {
 public:
    GrowingIndexSnapshotPin() = default;

    explicit operator bool() const noexcept {
        return snapshot_ != nullptr;
    }

    const IIndexReaderBase&
    Reader() const {
        AssertInfo(snapshot_ != nullptr,
                   "reader access through an empty snapshot pin");
        return *snapshot_->reader;
    }

    // Segment row prefix [0, end), NOT the reader's local ID cardinality.
    // An empty pin covers no rows, but is distinct from a published empty reader.
    int64_t
    CoveredRowEnd() const noexcept {
        return snapshot_ ? snapshot_->covered_row_end : 0;
    }

 private:
    friend class IGrowingIndex;

    struct Snapshot {
        Snapshot(std::unique_ptr<const IIndexReaderBase> value, int64_t end)
            : reader(std::move(value)), covered_row_end(end) {
        }

        const std::unique_ptr<const IIndexReaderBase> reader;
        const int64_t covered_row_end;
    };

    explicit GrowingIndexSnapshotPin(std::shared_ptr<const Snapshot> snapshot)
        : snapshot_(std::move(snapshot)) {
    }

    // Shared only inside the pin protocol. The record uniquely owns its
    // reader; consumers cannot extract or replace that owning pointer.
    std::shared_ptr<const Snapshot> snapshot_;
};

// Segment uniquely owns this long-lived publisher. Append input and query
// capabilities are independent mixins, not subclasses per reader capability.
class IGrowingIndex {
 public:
    virtual ~IGrowingIndex() = default;

    // Give interval-based writers an execution point before a query pins the
    // current generation. Synchronously publishing implementations need no
    // work and use this default. Errors propagate instead of falling back to
    // an older generation.
    virtual void
    CommitIfNeeded() {
    }

    // For an already-built owner, force every successfully accepted row into a
    // published read record before returning. Load/reopen use this stronger
    // boundary; interval-based query commits continue to use CommitIfNeeded().
    // An owner below its build threshold may remain unbuilt with an empty pin.
    // A vector owner whose first cold Build fails may do the same even at or
    // above the threshold, but only while no engine was ever published and the
    // complete raw source remains available for query fallback and a later
    // Build attempt. Flush must not turn either safe raw fallback into a
    // partially published reader. Once an engine is built, Add failure is
    // terminal and propagates; it cannot be treated as raw fallback.
    virtual void
    Flush() = 0;

    // Fixed read-side protocol: this only pins an already published record.
    GrowingIndexSnapshotPin
    PinSnapshot() const {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        return GrowingIndexSnapshotPin(snapshot_);
    }

    virtual DataType
    ValueType() const = 0;
    virtual std::string
    Family() const = 0;

 protected:
    // The writer must serialize append/publication work and supply a reader
    // record whose Count, validity/offset mapping, dependency lifetime and
    // Segment coverage are fixed together. Completing a higher reserved range
    // does not close an earlier hole. Null rows count toward coverage; element
    // cardinality does not determine row coverage. Tantivy/R-Tree records bind
    // immutable engine views. A Knowhere record may bind one live Add/Search
    // engine, provided every operation obeys the record's fixed physical prefix
    // and the engine supplies its own Add/Search concurrency safety. `const`
    // alone does not provide either guarantee. The pin freezes the logical and
    // physical prefix metadata, not the ANN hits produced later by a shared
    // live engine. If constructing, validating, or allocating this publication
    // fails, the current record remains intact; a retry publishes the already
    // accepted engine state and must not repeat Add.
    void
    PublishSnapshot(std::unique_ptr<const IIndexReaderBase> reader,
                    int64_t covered_row_end) {
        AssertInfo(reader != nullptr && covered_row_end >= 0,
                   "invalid growing snapshot publication");
        std::shared_ptr<const GrowingIndexSnapshotPin::Snapshot> next =
            std::make_shared<const GrowingIndexSnapshotPin::Snapshot>(std::move(reader),
                                                          covered_row_end);
        {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            AssertInfo(
                !snapshot_ || covered_row_end >= snapshot_->covered_row_end,
                "growing snapshot coverage must not decrease");
            snapshot_.swap(next);
        }
        // Drop the previous publication outside the lock. Existing pins retain
        // it independently, including after the IGrowingIndex is destroyed.
    }

 private:
    mutable std::mutex snapshot_mutex_;
    std::shared_ptr<const GrowingIndexSnapshotPin::Snapshot> snapshot_;
};

// Non-owning input views. Append must consume/copy borrowed storage before it
// returns. Null validity means all rows valid. These batches represent flat
// rows; nested column ingestion needs an explicit offsets/validity view and
// must not silently interpret element count as row_count.
template <typename T>
struct ScalarBatch {
    size_t row_count;
    const T* values;
    const bool* valid;
};

using TextBatch = ScalarBatch<std::string_view>;

// T is the physical element type for dense input and a sparse-row type for
// sparse input. Dense input contains row_count * dim elements; sparse input
// contains row_count row objects. dim is the schema/batch dimension respectively.
template <typename T>
struct VectorBatch {
    size_t row_count;
    const T* values;
    int64_t dim;
    const bool* valid;
};

template <typename Batch>
class IAppendable {
 public:
    virtual ~IAppendable() = default;

    // row_begin is a Segment row offset. Success means input was accepted,
    // not necessarily published. Failure must not publish input that was not
    // completely accepted; an earlier committed generation may still finish
    // publication. Partial engine writes require recovery before a later
    // publication. A retry of a wholly accepted immutable Segment row range
    // completes pending recovery/publication without adding it again. The
    // caller must not supply different payload for an already accepted range.
    virtual void
    Append(int64_t row_begin, const Batch& batch) = 0;
};

}  // namespace milvus::index
