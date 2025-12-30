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

#include <vector>
#include <utility>
#include <map>
#include <memory>
#include <shared_mutex>
#include "cachinglayer/Manager.h"
#include "common/Types.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"

namespace milvus {

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
};

class ArrayOffsetsSealed : public IArrayOffsets {
    friend class ArrayOffsetsTest;

 public:
    ArrayOffsetsSealed() : element_row_ids_(), row_to_element_start_({0}) {
    }

    ArrayOffsetsSealed(std::vector<int32_t> element_row_ids,
                       std::vector<int32_t> row_to_element_start)
        : element_row_ids_(std::move(element_row_ids)),
          row_to_element_start_(std::move(row_to_element_start)) {
        AssertInfo(!row_to_element_start_.empty(),
                   "row_to_element_start must have at least one element");
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
        return element_row_ids_.size();
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

    static std::shared_ptr<ArrayOffsetsSealed>
    BuildFromSegment(const void* segment, const FieldMeta& field_meta);

 private:
    const std::vector<int32_t> element_row_ids_;
    const std::vector<int32_t> row_to_element_start_;
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
        return element_row_ids_.size();
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

 private:
    struct PendingRow {
        int64_t row_id;
        int32_t array_len;
    };

    void
    DrainPendingRows();

 private:
    std::vector<int32_t> element_row_ids_;

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
