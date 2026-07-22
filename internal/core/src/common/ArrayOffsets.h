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
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include <oneapi/tbb/concurrent_vector.h>

#include "cachinglayer/Manager.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "folly/FBVector.h"

namespace milvus {

class ChunkedColumnInterface;

class IArrayOffsets {
 public:
    virtual ~IArrayOffsets() = default;

    virtual int64_t
    GetRowCount() const = 0;

    virtual int64_t
    GetTotalElementCount() const = 0;

    // Convert element ID to row ID
    // returns pair of <row_id, element_index>
    // element id is contiguous between rows
    virtual std::pair<int32_t, int32_t>
    ElementIDToRowID(int32_t elem_id) const = 0;

    // Convert row ID to element ID range
    // elements with id in [ret.first, ret.last) belong to row_id
    virtual std::pair<int32_t, int32_t>
    ElementIDRangeOfRow(int32_t row_id) const = 0;

    // Convert row-level bitsets to element-level bitsets
    // row_start: starting row index (0-based)
    // row_bitset.size(): number of rows to process
    virtual std::pair<TargetBitmap, TargetBitmap>
    RowBitsetToElementBitset(const TargetBitmapView& row_bitset,
                             const TargetBitmapView& valid_row_bitset,
                             int64_t row_start) const = 0;

    // Convert row-level bitset to element offsets
    // Returns element IDs for all rows where row_bitset[row_id] is true
    virtual FixedVector<int32_t>
    RowBitsetToElementOffsets(const TargetBitmapView& row_bitset,
                              int64_t row_start) const = 0;

    // Convert row offsets to element offsets
    // Returns element IDs for all specified rows
    virtual FixedVector<int32_t>
    RowOffsetsToElementOffsets(
        const FixedVector<int32_t>& row_offsets) const = 0;

    // Iterate over rows and apply predicate with element range
    // predicate: function(element_start, element_end) -> bool
    //   element_start: first element ID of the row (inclusive)
    //   element_end: last element ID of the row (exclusive)
    // Returns: bitmap where bit[i] = predicate result for row (row_start + i)
    using ElementRangePredicate =
        std::function<bool(int32_t elem_start, int32_t elem_end)>;

    virtual TargetBitmap
    ForEachRowElementRange(const ElementRangePredicate& predicate,
                           int64_t row_start,
                           int64_t row_count) const = 0;
};

class ArrayOffsetsSealed : public IArrayOffsets {
    friend class ArrayOffsetsTest;

 public:
    ArrayOffsetsSealed() : row_to_element_start_({0}) {
    }

    explicit ArrayOffsetsSealed(std::vector<int32_t> row_to_element_start)
        : row_to_element_start_(std::move(row_to_element_start)) {
        AssertInfo(!row_to_element_start_.empty(),
                   "row_to_element_start must have at least one element");
    }

    // Build an all-zeros offsets (every row is an empty array) and charge its
    // heap cost to the caching layer, mirroring BuildFromSegment so the
    // destructor's RefundLoadedResource is balanced. Used when a scalar or
    // struct ARRAY field is materialized for old sealed rows (schema evolution)
    // without going through the normal offsets-build path.
    static std::shared_ptr<ArrayOffsetsSealed>
    BuildAllZeros(int64_t row_count) {
        auto result = std::make_shared<ArrayOffsetsSealed>(
            std::vector<int32_t>(row_count + 1, 0));
        result->resource_size_ = 4 * (row_count + 1);
        cachinglayer::Manager::GetInstance().ChargeLoadedResource(
            cachinglayer::ResourceUsage{result->resource_size_, 0});
        return result;
    }

    ~ArrayOffsetsSealed() {
        cachinglayer::Manager::GetInstance().RefundLoadedResource(
            {resource_size_, 0});
    }

    int64_t
    GetRowCount() const override {
        return static_cast<int64_t>(row_to_element_start_.size()) - 1;
    }

    int64_t
    GetTotalElementCount() const override {
        return row_to_element_start_.empty() ? 0 : row_to_element_start_.back();
    }

    std::pair<int32_t, int32_t>
    ElementIDToRowID(int32_t elem_id) const override;

    std::pair<int32_t, int32_t>
    ElementIDRangeOfRow(int32_t row_id) const override;

    std::pair<TargetBitmap, TargetBitmap>
    RowBitsetToElementBitset(const TargetBitmapView& row_bitset,
                             const TargetBitmapView& valid_row_bitset,
                             int64_t row_start) const override;

    FixedVector<int32_t>
    RowBitsetToElementOffsets(const TargetBitmapView& row_bitset,
                              int64_t row_start) const override;

    FixedVector<int32_t>
    RowOffsetsToElementOffsets(
        const FixedVector<int32_t>& row_offsets) const override;

    TargetBitmap
    ForEachRowElementRange(const ElementRangePredicate& predicate,
                           int64_t row_start,
                           int64_t row_count) const override;

    static std::shared_ptr<ArrayOffsetsSealed>
    BuildFromSegment(const void* segment, const FieldMeta& field_meta);

    static std::shared_ptr<ArrayOffsetsSealed>
    BuildFromColumn(const ChunkedColumnInterface& column,
                    const FieldMeta& field_meta,
                    int64_t row_count);

 private:
    const std::vector<int32_t> row_to_element_start_;
    int64_t resource_size_{0};
};

// Growing-segment row -> element-start table with lock-free readers.
//
// The starts table lives in fixed-size chunks whose addresses never change.
// Committing row i writes starts[i + 1] exactly once, then a release-store to
// committed_row_count_ publishes the completed prefix. Readers acquire-load
// that watermark and only touch immutable entries at or below it, so they do
// not contend with ingest.
//
// Insert calls may arrive concurrently and out of order. A writer-only mutex
// serializes the pending-row machinery; readers never acquire it. The
// watermark is published once per Insert call, preserving the old behavior
// where readers observe an inserted batch atomically.
//
// Growing offsets are not charged to the caching layer. Chunked storage
// eagerly allocates the first starts chunk (32 KiB) and then grows in 32 KiB
// steps.
class ArrayOffsetsGrowing : public IArrayOffsets {
 public:
    // Public so tests can target chunk boundaries.
    static constexpr int64_t kChunkBits = 13;
    static constexpr int64_t kEntriesPerChunk = int64_t{1} << kChunkBits;
    static constexpr int64_t kChunkMask = kEntriesPerChunk - 1;

    ArrayOffsetsGrowing() {
        // starts[0] is the sentinel for an empty table. The constructor
        // happens-before concurrent use, so this initialization needs no
        // synchronization.
        auto chunk = std::make_unique<int32_t[]>(kEntriesPerChunk);
        chunk[0] = 0;
        starts_chunks_.push_back(std::move(chunk));
    }

    void
    Insert(int64_t row_id_start, const int32_t* array_lengths, int64_t count);

    int64_t
    GetRowCount() const override {
        return committed_row_count_.load(std::memory_order_acquire);
    }

    int64_t
    GetTotalElementCount() const override {
        const int64_t committed =
            committed_row_count_.load(std::memory_order_acquire);
        return LoadStart(committed);
    }

    std::pair<int32_t, int32_t>
    ElementIDToRowID(int32_t elem_id) const override;

    std::pair<int32_t, int32_t>
    ElementIDRangeOfRow(int32_t row_id) const override;

    std::pair<TargetBitmap, TargetBitmap>
    RowBitsetToElementBitset(const TargetBitmapView& row_bitset,
                             const TargetBitmapView& valid_row_bitset,
                             int64_t row_start) const override;

    FixedVector<int32_t>
    RowBitsetToElementOffsets(const TargetBitmapView& row_bitset,
                              int64_t row_start) const override;

    FixedVector<int32_t>
    RowOffsetsToElementOffsets(
        const FixedVector<int32_t>& row_offsets) const override;

    TargetBitmap
    ForEachRowElementRange(const ElementRangePredicate& predicate,
                           int64_t row_start,
                           int64_t row_count) const override;

 private:
    struct PendingRow {
        int64_t row_id;
        int32_t array_len;
    };

    template <typename T>
    using ChunkDirectory = oneapi::tbb::concurrent_vector<std::unique_ptr<T[]>>;

    // Keep this value stable across compiler and -mtune changes. Homebrew
    // Clang/libc++ does not expose the C++17 interference-size constants;
    // GCC 12 uses 256 bytes on AArch64 and 64 bytes on x86-64.
#if defined(__APPLE__) && (defined(__aarch64__) || defined(__arm64__))
    static constexpr std::size_t kDestructiveInterferenceSize = 128;
#elif defined(__aarch64__) || defined(__arm64__)
    static constexpr std::size_t kDestructiveInterferenceSize = 256;
#else
    static constexpr std::size_t kDestructiveInterferenceSize = 64;
#endif

    // Reader-side helper. Callers must only pass indices covered by an
    // acquire-loaded committed_row_count_; those entries are immutable.
    int32_t
    LoadStart(int64_t idx) const {
        return starts_chunks_[idx >> kChunkBits][idx & kChunkMask];
    }

    // Writer-side helpers; write_mutex_ must be held.
    void
    WriteStart(int64_t idx, int32_t value);

    void
    CommitRow(int32_t array_len);

    void
    DrainPendingRows();

    void
    PublishCommitted();

 private:
    // Logical index i is stored at
    // starts_chunks_[i >> kChunkBits][i & kChunkMask]. TBB's directory can
    // grow concurrently without relocating existing entries.
    ChunkDirectory<int32_t> starts_chunks_;

    // Reader-visible publication watermark. Rows below it, plus the sentinel
    // at the watermark, are committed and immutable.
    std::atomic<int32_t> committed_row_count_{0};

    // Writer's working count, published at the end of each Insert call. Keep
    // it off the reader-hot watermark's cache line: CommitRow updates this for
    // every row, while readers repeatedly acquire-load the watermark.
    alignas(kDestructiveInterferenceSize) int32_t committed_rows_writer_{0};

    // Pending rows waiting for earlier rows to complete
    // Key: row_id, automatically sorted
    std::map<int64_t, PendingRow> pending_rows_;

    // Serializes writers only; readers never take this lock.
    std::mutex write_mutex_;
};

}  // namespace milvus
