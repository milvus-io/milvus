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
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Types.h"
#include "common/ValidityView.h"

namespace milvus {

enum class ScanValueKind {
    Default,
    FixedWidth,
    StringView,
    JsonView,
    ArrayView,
    VectorArrayView,
};

inline std::optional<ScanValueKind>
GetScanValueKindForDataType(DataType data_type) {
    if (data_type == DataType::BOOL || data_type == DataType::INT8 ||
        data_type == DataType::INT16 || data_type == DataType::INT32 ||
        data_type == DataType::INT64 || data_type == DataType::FLOAT ||
        data_type == DataType::DOUBLE || data_type == DataType::TIMESTAMPTZ) {
        return ScanValueKind::FixedWidth;
    }
    if (data_type == DataType::JSON) {
        return ScanValueKind::JsonView;
    }
    if (data_type == DataType::STRING || data_type == DataType::VARCHAR ||
        data_type == DataType::TEXT || data_type == DataType::GEOMETRY) {
        return ScanValueKind::StringView;
    }
    if (data_type == DataType::ARRAY) {
        return ScanValueKind::ArrayView;
    }
    if (data_type == DataType::VECTOR_ARRAY) {
        return ScanValueKind::VectorArrayView;
    }
    return std::nullopt;
}

enum class OffsetElementType {
    Int32,
    Int64,
};

// A non-owning view over positional row offsets. The array must remain alive
// only for the synchronous Take() call. Supporting both widths lets expression
// evaluation use its existing int32 offsets without a widening copy while
// retrieve/requery can pass segment int64 offsets.
struct OffsetView {
    const void* data = nullptr;
    int64_t size = 0;
    OffsetElementType element_type = OffsetElementType::Int32;

    static OffsetView
    From(const int32_t* offsets, int64_t count) {
        return OffsetView{offsets, count, OffsetElementType::Int32};
    }

    static OffsetView
    From(const int64_t* offsets, int64_t count) {
        return OffsetView{offsets, count, OffsetElementType::Int64};
    }

    int64_t
    operator[](int64_t index) const {
        AssertInfo(index >= 0 && index < size,
                   "take offset index {} out of range {}",
                   index,
                   size);
        AssertInfo(data != nullptr,
                   "take offsets are null with non-empty size {}",
                   size);
        if (element_type == OffsetElementType::Int32) {
            return static_cast<const int32_t*>(data)[index];
        }
        return static_cast<const int64_t*>(data)[index];
    }
};

struct TakeLocation {
    int64_t source_cell_id = -1;
    size_t cell_offset = 0;
};

// Default positional read plan for Cell-addressable backends. Locations
// preserve the requested logical order and duplicates while translating each
// segment offset into a source Cell and Cell-local offset. Backends whose read
// coordinate is not a Cell-local offset may consume TakeOptions directly.
struct TakePlan {
    std::vector<TakeLocation> locations;

    int64_t
    Size() const {
        return static_cast<int64_t>(locations.size());
    }
};

using CellSkipPredicate = std::function<bool(int64_t)>;

struct CellLocation {
    int64_t cell_id = -1;
    int64_t cell_offset = 0;
};

struct PlannedCellRange {
    int64_t cell_id = -1;
    int64_t row_start = 0;
    int64_t row_count = 0;
    bool skip_data = false;
};

struct ScanPlan {
    std::vector<PlannedCellRange> cells;
};

// The single Cell-geometry authority for one Column generation. Scan execution
// may supply an expression-specific Cell predicate, while the planner owns
// segment-offset-to-Cell conversion, Scan decision caching, and plan
// construction.
class ColumnPlanner {
 public:
    explicit ColumnPlanner(std::vector<int64_t> num_rows_until_cell);
    virtual ~ColumnPlanner() = default;

    CellLocation
    Locate(int64_t segment_offset) const;

    int64_t
    CellStart(int64_t cell_id) const;

    int64_t
    CellRows(int64_t cell_id) const;

    int64_t
    NumCells() const;

    int64_t
    NumRows() const;

    const std::vector<int64_t>&
    CellBoundaries() const;

    TakePlan
    PlanTake(const OffsetView& offsets) const;

    ScanPlan
    PlanScan(int64_t row_start,
             int64_t row_count,
             const CellSkipPredicate& preloaded_skip = {}) const;

    bool
    ShouldSkipCell(int64_t cell_id, const CellSkipPredicate& predicate) const;

 private:
    std::vector<int64_t> num_rows_until_cell_;
};

struct ValueView {
    ScanValueKind kind = ScanValueKind::Default;
    const void* data = nullptr;
    int64_t offset = 0;
    int32_t byte_width = 0;

    bool
    empty() const {
        return kind == ScanValueKind::Default || data == nullptr;
    }

    template <typename T>
    const T*
    data_as() const {
        AssertInfo(kind != ScanValueKind::Default && data != nullptr,
                   "scan value view is empty");
        return static_cast<const T*>(data) + offset;
    }
};

struct ScanBatch {
    // Every batch represents the dense row range
    // [row_id_start, row_id_start + size). Values are optional based on the
    // requested projection, while validity remains aligned with this range.
    ValueView values;
    // Empty means every row in this batch is valid. The view may reference
    // expanded bools or an LSB-first packed bitmap owned by `owner`.
    ValidityView validity;
    std::shared_ptr<void> owner;
    int64_t row_id_start = 0;
    int64_t size = 0;
};

struct OwnedTakeData {
    ValueView values;
    ValidityView validity;
    std::shared_ptr<void> owner;
    int64_t size = 0;
};

// Ordered positional access over one finite offset set. Callers do not need to
// retain the input offsets or manage backend Cell lifetimes. Reader-backed
// results may already contain one dense owned value array. Raw results pin only
// the Cell used by the current borrowed access; a borrowed string/JSON/array
// view remains valid until the next access that switches Cells, GetOwn(), or
// result destruction. Raw borrowed access and GetOwn() must complete while the
// OpContext passed to Take() remains alive. TakeResult is not thread-safe.
class TakeResult {
 public:
    virtual ~TakeResult() = default;

    virtual int64_t
    Size() const = 0;

    virtual ScanValueKind
    Kind() const = 0;

    virtual DataType
    GetDataType() const = 0;

    virtual bool
    IsValid(int64_t index) const = 0;

    // True when GetOwn() can return the existing dense result without
    // materializing Raw payload.
    virtual bool
    IsOwned() const = 0;

    // Return an ordered dense value collection whose lifetime is independent
    // of Raw Cell pins. Vortex results normally return their existing decoded
    // owner; Raw results materialize and cache this representation lazily.
    virtual OwnedTakeData
    GetOwn() const = 0;

    template <typename T>
    T
    Get(int64_t index) const {
        AssertInfo(index >= 0 && index < Size(),
                   "take result index {} out of range {}",
                   index,
                   Size());
        if constexpr (std::is_same_v<T, std::string_view>) {
            AssertInfo(Kind() == ScanValueKind::StringView,
                       "take result kind {} is not StringView",
                       static_cast<int>(Kind()));
            const auto data_type = GetDataType();
            AssertInfo(data_type == DataType::STRING ||
                           data_type == DataType::VARCHAR ||
                           data_type == DataType::TEXT ||
                           data_type == DataType::GEOMETRY,
                       "take result type {} is not string-like",
                       data_type);
            return StringViewAt(index);
        } else if constexpr (std::is_same_v<T, Json>) {
            AssertInfo(Kind() == ScanValueKind::JsonView,
                       "take result kind {} is not JsonView",
                       static_cast<int>(Kind()));
            AssertInfo(GetDataType() == DataType::JSON,
                       "take result type {} is not JSON",
                       GetDataType());
            return JsonAt(index);
        } else if constexpr (std::is_same_v<T, ArrayView>) {
            AssertInfo(Kind() == ScanValueKind::ArrayView,
                       "take result kind {} is not ArrayView",
                       static_cast<int>(Kind()));
            AssertInfo(GetDataType() == DataType::ARRAY,
                       "take result type {} is not ARRAY",
                       GetDataType());
            return ArrayAt(index);
        } else {
            AssertInfo(Kind() == ScanValueKind::FixedWidth,
                       "take result kind {} is not FixedWidth",
                       static_cast<int>(Kind()));
            const auto data_type = GetDataType();
            bool type_matches = false;
            if constexpr (std::is_same_v<T, bool>) {
                type_matches = data_type == DataType::BOOL;
            } else if constexpr (std::is_same_v<T, int8_t>) {
                type_matches = data_type == DataType::INT8;
            } else if constexpr (std::is_same_v<T, int16_t>) {
                type_matches = data_type == DataType::INT16;
            } else if constexpr (std::is_same_v<T, int32_t>) {
                type_matches = data_type == DataType::INT32;
            } else if constexpr (std::is_same_v<T, int64_t>) {
                type_matches = data_type == DataType::INT64 ||
                               data_type == DataType::TIMESTAMPTZ;
            } else if constexpr (std::is_same_v<T, float>) {
                type_matches = data_type == DataType::FLOAT;
            } else if constexpr (std::is_same_v<T, double>) {
                type_matches = data_type == DataType::DOUBLE;
            }
            AssertInfo(type_matches,
                       "take result type {} does not match requested value",
                       data_type);
            return *static_cast<const T*>(FixedValueAt(index));
        }
    }

 protected:
    virtual const void*
    FixedValueAt(int64_t index) const = 0;

    virtual std::string_view
    StringViewAt(int64_t index) const = 0;

    virtual Json
    JsonAt(int64_t index) const = 0;

    virtual ArrayView
    ArrayAt(int64_t index) const = 0;
};

struct TakeOptions {
    OffsetView offsets;
    ScanValueKind value_kind = ScanValueKind::Default;
};

enum class ScanReadMode {
    // Return data and, for nullable columns, aligned validity.
    DataAndValidity,
    // For nullable data scans, return aligned validity without constructing
    // or decoding data. Non-nullable columns must reject this mode.
    ValidityOnly,
};

class ScanCursor {
 public:
    virtual ~ScanCursor() = default;

    // Return at most one dense batch beginning exactly at the requested
    // absolute segment position. The returned size may be smaller than length
    // at a Cell, file, reader, or ownership boundary, but must never exceed the
    // requested range. Positions must move forward; a greater position seeks
    // without reading the intervening rows.
    virtual bool
    Next(int64_t position,
         int64_t length,
         ScanReadMode mode,
         ScanBatch* out) = 0;
};

enum class ScanPinPolicy {
    PerCall,
    UntilCellExhausted,
};

struct ScanOptions {
    ScanOptions() = default;

    ScanOptions(int64_t start_offset,
                ScanValueKind value_kind = ScanValueKind::Default,
                ScanPinPolicy pin_policy = ScanPinPolicy::PerCall,
                bool prefetch = false)
        : start_offset(start_offset),
          value_kind(value_kind),
          pin_policy(pin_policy),
          prefetch(prefetch) {
    }

    static ScanOptions
    ForData(int64_t start_offset,
            ScanValueKind value_kind = ScanValueKind::Default,
            ScanPinPolicy pin_policy = ScanPinPolicy::PerCall,
            bool prefetch = false) {
        return ScanOptions(start_offset, value_kind, pin_policy, prefetch);
    }

    int64_t start_offset = 0;
    ScanValueKind value_kind = ScanValueKind::Default;
    ScanPinPolicy pin_policy = ScanPinPolicy::PerCall;
    // Batch-warm all remaining Cells at cursor creation without retaining
    // their pins, matching the legacy multi-chunk inline prefetch. Reads still
    // pin the current Cell one at a time, but cold loads are submitted
    // together so tiered-storage I/O stays parallel.
    bool prefetch = false;
};

using ScanResult = std::unique_ptr<ScanCursor>;
using TakeResultPtr = std::unique_ptr<TakeResult>;

}  // namespace milvus
