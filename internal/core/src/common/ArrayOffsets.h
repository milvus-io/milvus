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

    // Batched form of ElementIDRangeOfRow: out[i] = element range of
    // row_ids[i]. Semantics per entry match the per-row method, but the
    // growing implementation resolves the committed watermark ONCE (and the
    // caller pays one virtual call) for the whole batch instead of once per
    // row. A row id outside [0, committed rows] yields {0, 0} on growing
    // (out-of-range insurance: a not-yet-committed row has no range yet).
    virtual void
    CopyRowElementRanges(const int32_t* row_ids,
                         int64_t count,
                         std::pair<int32_t, int32_t>* out) const = 0;

    // Contiguous form for the common sequential-batch case: copy
    // starts[row_start .. row_start + row_count] into out (row_count + 1
    // entries), so [out[i], out[i+1]) is row (row_start + i)'s element
    // range. Sealed is a straight memcpy; growing reads the committed
    // watermark once, copies per chunk-run, and clamps rows beyond the
    // committed count to the last committed total (equivalent to the per-row
    // method's {total, total} for row == committed row count, extended to
    // any not-yet-committed row).
    virtual void
    CopyRowElementStarts(int64_t row_start,
                         int64_t row_count,
                         int32_t* out) const = 0;

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

    // Apply per-row NULL info retained from the raw field: clear result[i]
    // for every row (row_start + i) whose ARRAY value is NULL. No-op when no
    // validity was recorded (non-nullable source: every row is valid).
    //
    // A nested array index only exposes element-level validity, and a NULL row
    // has zero elements -- indistinguishable at element level from an empty
    // array. Row-level consumers (e.g. array_contains over a nullable array via
    // a nested index) use this to exclude NULL rows exactly as the brute-force
    // path does. Thread-safe on growing segments (reader-lock-free: an atomic
    // watermark bounds the readable prefix); exposed as an apply-operation
    // rather than a raw bitmap pointer because the growing implementation's
    // bitmap grows concurrently with inserts.
    virtual void
    AndRowValidBitmap(TargetBitmapView result,
                      int64_t row_start,
                      int64_t row_count) const = 0;

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

    // Word-wise ANY-semantics reduction of an element-level bitmap to row
    // level. Bit j of elem_bitset corresponds to global element id
    // (elem_offset + j). For each i in [0, row_result.size()), sets
    // row_result[i] = true iff any element bit of row (row_start + i) is set.
    // Bits in row_result are only ever set, never cleared. elem_bitset must
    // cover the element ranges of all addressed rows.
    // This is the hot path used to fold element-level match bits back to
    // rows (MATCH_ANY over an all-valid element bitmap); it skips zero words
    // instead of testing element bits one by one.
    virtual void
    ElementBitsetToRowBitsetAny(const TargetBitmapView& elem_bitset,
                                int64_t elem_offset,
                                int64_t row_start,
                                TargetBitmapView row_result) const = 0;
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

    // Build zero element ranges for rows whose ARRAY value is NULL. Offsets
    // alone cannot distinguish NULL from a valid empty array, so retain an
    // explicit all-false row-valid bitmap for row-level consumers of nested
    // indexes. Used when schema evolution backfills a nullable ARRAY field
    // without a default value.
    static std::shared_ptr<ArrayOffsetsSealed>
    BuildAllNulls(int64_t row_count) {
        auto result = std::make_shared<ArrayOffsetsSealed>(
            std::vector<int32_t>(row_count + 1, 0));
        result->has_row_valid_ = true;
        result->row_valid_ = TargetBitmap(row_count, false);
        result->resource_size_ = 4 * (row_count + 1) + (row_count + 7) / 8;
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

    void
    CopyRowElementRanges(const int32_t* row_ids,
                         int64_t count,
                         std::pair<int32_t, int32_t>* out) const override;

    void
    CopyRowElementStarts(int64_t row_start,
                         int64_t row_count,
                         int32_t* out) const override;

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

    void
    AndRowValidBitmap(TargetBitmapView result,
                      int64_t row_start,
                      int64_t row_count) const override {
        if (!has_row_valid_) {
            return;
        }
        auto rows = static_cast<int64_t>(row_valid_.size());
        for (int64_t i = 0; i < row_count; ++i) {
            auto row = row_start + i;
            if (row >= rows) {
                break;
            }
            if (!row_valid_[row]) {
                result[i] = false;
            }
        }
    }

    TargetBitmap
    ForEachRowElementRange(const ElementRangePredicate& predicate,
                           int64_t row_start,
                           int64_t row_count) const override;

    void
    ElementBitsetToRowBitsetAny(const TargetBitmapView& elem_bitset,
                                int64_t elem_offset,
                                int64_t row_start,
                                TargetBitmapView row_result) const override;

    static std::shared_ptr<ArrayOffsetsSealed>
    BuildFromSegment(const void* segment, const FieldMeta& field_meta);

    static std::shared_ptr<ArrayOffsetsSealed>
    BuildFromColumn(const ChunkedColumnInterface& column,
                    const FieldMeta& field_meta,
                    int64_t row_count);

 private:
    const std::vector<int32_t> row_to_element_start_;
    // Per-row NULL bitmap (bit==true => non-null). Populated by the static
    // builders when the source field is nullable; empty & has_row_valid_==false
    // for non-nullable fields (all rows valid).
    TargetBitmap row_valid_;
    bool has_row_valid_{false};
    int64_t resource_size_{0};
};

// Growing-segment row -> element-start table with LOCK-FREE readers.
//
// Storage layout
//   The starts table (row_to_element_start in the sealed variant) and the
//   per-row validity bytes live in fixed-size chunks of kEntriesPerChunk
//   entries. A chunk's address never changes once allocated, so committed
//   entries never move (unlike a std::vector, whose realloc invalidates
//   concurrent readers). The chunk-pointer directory is a
//   tbb::concurrent_vector<std::unique_ptr<T[]>>: TBB guarantees elements
//   never relocate on growth and element access is safe concurrently with a
//   writer's push_back, and TBB is already a core dependency (see
//   segcore/ConcurrentVector.h, common/Channel.h). A pre-sized atomic
//   pointer array was considered instead, but it either hard-caps the row
//   count or needs a two-level radix tree -- tbb::concurrent_vector gives
//   unbounded growth with the same lock-free read property.
//
// Write-once entries + atomic watermark
//   starts[0] == 0 is preassigned by the constructor; committing row i
//   writes starts[i + 1] = new_total exactly once (the old idempotent
//   sentinel overwrite is gone). committed_row_count_ is the publication
//   watermark: the writer release-stores it only AFTER all starts entries
//   (and validity bytes) of the published prefix are written; readers
//   acquire-load it and touch only logical indices <= watermark. The
//   release/acquire pair plus write-once entries make every reader path
//   data-race-free without any lock. The watermark is published once per
//   Insert/InsertNulls batch (after the pending-row drain), so concurrent
//   readers observe each insert batch atomically, exactly like the old
//   shared_mutex implementation.
//
// Memory orderings (each atomic documented here, referenced from the .cpp):
//   - committed_row_count_: writer store(release) at batch end; reader
//     load(acquire). The writer itself never loads it (it owns
//     committed_rows_writer_ under write_mutex_).
//   - has_row_valid_: writer store(release) only AFTER the all-valid prefix
//     backfill completes (lazy materialization on the first NULL row);
//     writer load(relaxed) is fine because only the writer (serialized by
//     write_mutex_) stores it. Reader load(acquire) pairs with the release
//     so a reader that sees `true` also sees the whole backfilled prefix.
//     The flag store is sequenced before the watermark store that first
//     publishes the NULL row, so a reader that acquired watermark >= R+1
//     (R = first NULL row) is guaranteed to see the flag.
//
// Writers
//   Insert/InsertNulls may be called concurrently for different reserved
//   row ranges; a plain std::mutex (write_mutex_) serializes them and
//   protects the out-of-order machinery (pending_rows_,
//   committed_rows_writer_). Readers never take it.
//
// Memory accounting
//   None -- growing offsets were never charged to the caching layer and
//   still are not. Chunked storage over-allocates at most one partially
//   filled starts chunk (32 KiB) plus one validity chunk (8 KiB), on par
//   with the old vector's geometric-growth slack.
class ArrayOffsetsGrowing : public IArrayOffsets {
 public:
    // Chunk geometry (public so tests can target chunk boundaries).
    static constexpr int64_t kChunkBits = 13;
    static constexpr int64_t kEntriesPerChunk = int64_t{1} << kChunkBits;
    static constexpr int64_t kChunkMask = kEntriesPerChunk - 1;

    ArrayOffsetsGrowing() {
        // Preassign starts[0] = 0 (write-once; see class comment). The
        // constructor happens-before any concurrent use of the object, so
        // this needs no synchronization.
        auto chunk = std::make_unique<int32_t[]>(kEntriesPerChunk);
        chunk[0] = 0;
        starts_chunks_.push_back(std::move(chunk));
    }

    // array_lengths[i] < 0 marks a NULL row: recorded as zero elements AND
    // row-invalid (see AndRowValidBitmap); >= 0 is a real (possibly empty)
    // array. The insert extractors emit -1 for NULL rows so offsets-level
    // consumers can distinguish NULL from [] exactly like sealed segments.
    void
    Insert(int64_t row_id_start, const int32_t* array_lengths, int64_t count);

    // Backfill for schema evolution: `count` rows starting at row_id_start
    // whose ARRAY value is NULL (zero elements, row-invalid). Mirrors
    // ArrayOffsetsSealed::BuildAllNulls so growing and sealed segments agree
    // that historical rows of a backfilled nullable ARRAY field are NULL,
    // not empty arrays.
    void
    InsertNulls(int64_t row_id_start, int64_t count);

    int64_t
    GetRowCount() const override {
        // acquire: pairs with the writer's release-store, so a caller that
        // observes count N may subsequently read any entry of rows < N (and
        // the starts sentinel at N) without further synchronization.
        return committed_row_count_.load(std::memory_order_acquire);
    }

    int64_t
    GetTotalElementCount() const override {
        // starts[W] is the running total after the last committed row; the
        // entry was written before W was published (see class comment).
        const int64_t committed =
            committed_row_count_.load(std::memory_order_acquire);
        return LoadStart(committed);
    }

    std::pair<int32_t, int32_t>
    ElementIDToRowID(int32_t elem_id) const override;

    std::pair<int32_t, int32_t>
    ElementIDRangeOfRow(int32_t row_id) const override;

    void
    CopyRowElementRanges(const int32_t* row_ids,
                         int64_t count,
                         std::pair<int32_t, int32_t>* out) const override;

    void
    CopyRowElementStarts(int64_t row_start,
                         int64_t row_count,
                         int32_t* out) const override;

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

    void
    AndRowValidBitmap(TargetBitmapView result,
                      int64_t row_start,
                      int64_t row_count) const override;

    TargetBitmap
    ForEachRowElementRange(const ElementRangePredicate& predicate,
                           int64_t row_start,
                           int64_t row_count) const override;

    void
    ElementBitsetToRowBitsetAny(const TargetBitmapView& elem_bitset,
                                int64_t elem_offset,
                                int64_t row_start,
                                TargetBitmapView row_result) const override;

 private:
    struct PendingRow {
        int64_t row_id;
        int32_t array_len;
        bool valid;
    };

    template <typename T>
    using ChunkDirectory = oneapi::tbb::concurrent_vector<std::unique_ptr<T[]>>;

    // ---- reader-side helper (lock-free) ----
    // Callers must only pass logical indices covered by an acquire-loaded
    // committed_row_count_ (starts: idx <= watermark; validity: idx <
    // watermark) -- such entries are write-once and published, so a plain
    // read is race-free.
    int32_t
    LoadStart(int64_t idx) const {
        return starts_chunks_[idx >> kChunkBits][idx & kChunkMask];
    }

    // ---- writer-side helpers (write_mutex_ held) ----
    void
    WriteStart(int64_t idx, int32_t value);

    void
    WriteValid(int64_t idx, uint8_t value);

    // Backfill the all-valid prefix [0, committed_rows) and release-publish
    // has_row_valid_. Called on the first NULL row.
    void
    MaterializeRowValid(int32_t committed_rows);

    // Commit one row at committed_rows_writer_.
    void
    CommitRow(int32_t array_len, bool valid);

    void
    DrainPendingRows();

    // Release-store the writer's committed count into the reader-visible
    // watermark. Called once per Insert/InsertNulls batch, after the drain.
    void
    PublishCommitted();

    // memcpy `entry_count` starts entries beginning at logical index
    // `first_idx` into `out`, one chunk-run at a time. Reader-side; the
    // range must be covered by an acquire-loaded watermark.
    void
    CopyStartsSlice(int64_t first_idx, int64_t entry_count, int32_t* out) const;

 private:
    // Chunked starts table: logical index i lives at
    // starts_chunks_[i >> kChunkBits][i & kChunkMask]. Entries are
    // write-once (see class comment); committed entries never move.
    ChunkDirectory<int32_t> starts_chunks_;

    // Per-committed-row validity (1 = non-null), chunked like the starts
    // table, lockstep with the committed count. Lazily materialized on the
    // first NULL row so the common all-valid case stays overhead-free;
    // has_row_valid_ (not an emptiness check, which is ambiguous when the
    // first committed row is itself NULL) tells whether it was materialized.
    ChunkDirectory<uint8_t> valid_chunks_;

    // Reader-visible watermark: rows [0, committed_row_count_) are
    // committed and immutable. Ordering: see class comment.
    std::atomic<int32_t> committed_row_count_{0};

    // Whether row validity was materialized. Ordering: see class comment.
    std::atomic<bool> has_row_valid_{false};

    // ---- writer state below, guarded by write_mutex_ ----

    // The writer's working committed count; published to
    // committed_row_count_ at batch end by PublishCommitted().
    int32_t committed_rows_writer_{0};

    // Pending rows waiting for earlier rows to complete.
    // Key: row_id, automatically sorted.
    std::map<int64_t, PendingRow> pending_rows_;

    // Serializes writers (Insert / InsertNulls). Readers NEVER take it.
    std::mutex write_mutex_;
};

}  // namespace milvus
