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
#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <utility>
#include <vector>

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

    // Per-row NULL info retained from the raw field at build time.
    // Returns nullptr when no validity was recorded (treat every row as valid).
    // When non-null, bit[row]==true means the array at that row is NON-NULL.
    //
    // A nested array index only exposes element-level validity, and a NULL row
    // has zero elements -- indistinguishable at element level from an empty
    // array. Row-level consumers (e.g. array_contains over a nullable array via
    // a nested index) use this to exclude NULL rows exactly as the brute-force
    // path does.
    virtual const TargetBitmap*
    GetRowValidBitmap() const = 0;

    // Word-wise ANY-semantics reduction of an element-level bitmap to row
    // level. Bit j of elem_bitset corresponds to global element id
    // (elem_offset + j). For each i in [0, row_result.size()), sets
    // row_result[i] = true iff any element bit of row (row_start + i) is set.
    // Bits in row_result are only ever set, never cleared. elem_bitset must
    // cover the element ranges of all addressed rows.
    // This is the hot path used to aggregate element-level index hits
    // (nested array index) back to rows; it skips zero words instead of
    // testing element bits one by one.
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

    const TargetBitmap*
    GetRowValidBitmap() const override {
        return has_row_valid_ ? &row_valid_ : nullptr;
    }

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

    // Build offsets for the array living at `nested_path` inside a JSON column.
    // Mirrors BuildFromColumn, but each row's element count comes from parsing
    // the JSON document and taking the length of the array at the path. A row
    // whose JSON is NULL, whose path is absent, or whose value at the path is
    // not a real array contributes ZERO elements and is marked NON-valid in the
    // row-valid bitmap -- matching the three-valued MATCH semantics
    // (MaskJsonNonArrayRows) so such rows are excluded exactly as brute force
    // does. A genuine empty array [] contributes zero elements but stays valid.
    static std::shared_ptr<ArrayOffsetsSealed>
    BuildFromJsonColumn(const ChunkedColumnInterface& column,
                        const FieldMeta& field_meta,
                        const std::string& nested_path,
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

class ArrayOffsetsGrowing : public IArrayOffsets {
 public:
    ArrayOffsetsGrowing() = default;

    void
    Insert(int64_t row_id_start, const int32_t* array_lengths, int64_t count);

    int64_t
    GetRowCount() const override {
        std::shared_lock lock(mutex_);
        return committed_row_count_;
    }

    int64_t
    GetTotalElementCount() const override {
        std::shared_lock lock(mutex_);
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

    // Growing segments do not carry a nested scalar index, so the row-level
    // nested-index consumer never queries this. Report "no validity recorded".
    const TargetBitmap*
    GetRowValidBitmap() const override {
        return nullptr;
    }

    void
    ElementBitsetToRowBitsetAny(const TargetBitmapView& elem_bitset,
                                int64_t elem_offset,
                                int64_t row_start,
                                TargetBitmapView row_result) const override;

 private:
    struct PendingRow {
        int64_t row_id;
        int32_t array_len;
    };

    void
    DrainPendingRows();

 private:
    std::vector<int32_t> row_to_element_start_;

    // Number of rows committed (contiguous from 0)
    int32_t committed_row_count_ = 0;

    // Pending rows waiting for earlier rows to complete
    // Key: row_id, automatically sorted
    std::map<int64_t, PendingRow> pending_rows_;

    // Protects all member variables
    mutable std::shared_mutex mutex_;
};

}  // namespace milvus
