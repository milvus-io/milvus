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
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "pb/plan.pb.h"

namespace milvus {

enum class ScanValueKind {
    Default,
    FixedWidth,
    StringView,
    JsonView,
    ArrayView,
    VectorArrayView,
};

enum class ValueEncoding {
    Empty,
    FixedWidth,
    StringView,
    JsonView,
    ArrayView,
    VectorArrayView,
};

enum class OffsetElementType {
    Int32,
    Int64,
};

// A non-owning view over positional row offsets. Callers must keep the
// underlying array alive for the lifetime of the TakeCursor. Supporting both
// widths lets expression evaluation use its existing int32 offsets without a
// widening copy while retrieve/requery can pass segment int64 offsets.
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

struct ValueView {
    ValueEncoding encoding = ValueEncoding::Empty;
    ScanValueKind kind = ScanValueKind::Default;
    DataType physical_type = DataType::NONE;
    DataType logical_type = DataType::NONE;
    const void* data = nullptr;
    int64_t offset = 0;
    int64_t size = 0;
    int32_t byte_width = 0;

    bool
    empty() const {
        return encoding == ValueEncoding::Empty || data == nullptr;
    }

    template <typename T>
    const T*
    data_as() const {
        AssertInfo(encoding != ValueEncoding::Empty && data != nullptr,
                   "scan value view is empty");
        return static_cast<const T*>(data) + offset;
    }
};

struct ScanBatch {
    // Every batch represents the dense row range
    // [row_id_start, row_id_start + size) for data scans. Payload fields are
    // optional based on ScanOptions. For data scans, values and validity are
    // dense over this range. For filter pushdown scans, row_ids is sparse and
    // validity, when present, is aligned with row_ids.
    ValueView values;
    // Evaluators consume validity as one bool per logical row. nullptr means
    // every row in this batch is valid. Storage-native bitmap encodings must
    // be normalized by the cursor before they cross this boundary.
    const bool* validity = nullptr;
    std::vector<int64_t> row_ids;
    std::shared_ptr<void> owner;
    int64_t row_id_start = 0;
    int64_t size = 0;
};

struct TakeBatch {
    // Take preserves the caller's offset order. values is a backend-native
    // addressable collection and selection maps each logical output item to
    // one value in that collection. A null selection means identity indexing.
    //
    // Raw fixed-width batches expose a pinned Chunk span plus chunk-local
    // selection offsets. Reader-backed formats may return already ordered,
    // dense decoded values with a null selection.
    ValueView values;
    const int32_t* selection = nullptr;
    // Validity uses the same indexing as values: selection[i] when selection
    // is present, otherwise i. nullptr means every logical item is valid.
    const bool* validity = nullptr;
    std::shared_ptr<void> owner;
    // Logical position in the input OffsetView of the first returned item.
    int64_t position = 0;
    int64_t size = 0;
    // Raw batches identify their physical source chunk so expression skip
    // statistics can remain chunk-aware. Reader-backed ordered results may
    // combine sources and leave this as -1.
    int64_t source_chunk_id = -1;
};

class TakeCursor {
 public:
    virtual ~TakeCursor() = default;

    // Number of input offsets already returned.
    virtual int64_t
    Position() const = 0;

    // Return the next ordered positional batch. Concatenating all successful
    // batches must produce exactly the input offset order, including duplicate
    // offsets. max_rows limits the logical items exposed to the caller; a
    // backend may physically read/materialize more data once and slice it here.
    virtual bool
    Next(int64_t max_rows, TakeBatch* out) = 0;
};

struct TakeOptions {
    OffsetView offsets;
    ScanValueKind value_kind = ScanValueKind::Default;
};

class ScanCursor {
 public:
    virtual ~ScanCursor() = default;

    // Source scan progress. For dense data scans this is the next logical row
    // that has neither been returned nor planner-skipped. Sparse row-id scans
    // may advance past entries that are already buffered inside the cursor.
    virtual int64_t
    Position() const = 0;

    // Return the next evaluated batch from the underlying source. A batch is
    // always one continuous logical range, contains at most max_rows rows, and
    // never crosses a skipped range or backend batch boundary. Non-nullable
    // scans may advance Position() across a data-skip range without returning
    // it. Nullable scans return that range as a validity-only batch so null
    // rows preserve expression Unknown semantics without reopening a cursor.
    // Sparse row-id batches contain at most max_rows row ids.
    virtual bool
    Next(int64_t max_rows, ScanBatch* out) = 0;
};

struct ScanRowRange {
    int64_t start = 0;
    int64_t end = 0;
};

struct ScanPlan {
    static ScanPlan
    Full(int64_t start, int64_t length) {
        return ScanPlan{ScanRowRange{start, start + length}, {}};
    }

    ScanRowRange requested_range;
    // Sorted, non-overlapping segment-offset ranges whose data values must not
    // be returned or evaluated. Non-nullable cursors skip these rows;
    // nullable cursors return validity-only batches for them.
    std::vector<ScanRowRange> skip_ranges;
};

enum class ScanProjection;

// PreparedScan owns the storage resources selected for one operator/expression
// window, including cache pins. Open() may create a data cursor that applies a
// skip plan or a validity-only cursor over a subrange; both reuse the same
// window-local resources. PreparedScan must not be retained across expression
// windows.
class PreparedScan {
 public:
    virtual ~PreparedScan() = default;

    virtual int64_t
    Start() const = 0;

    virtual int64_t
    End() const = 0;

    // Backend planner result for this prepared operator window. Backends keep
    // metadata and post-load pruning in Cell-ID form, then convert the union
    // to segment-offset ranges before exposing this cursor plan.
    virtual const ScanPlan&
    Plan() const = 0;

    virtual std::unique_ptr<ScanCursor>
    Open(const ScanPlan& plan, ScanProjection projection) const = 0;
};

enum class ScanOutput {
    // Filter pushdown payload: ScanBatch::row_ids contains predicate true or
    // unknown rows; ScanBatch::validity is aligned with row_ids.
    RowIds,
    // Dense data payload: ScanBatch::values contains values over the batch
    // range unless projection asks to omit data.
    Data,
};

enum class ScanProjection {
    // Return ScanBatch::values for dense data scans.
    Data,
    // Omit ScanBatch::values. Dense data scans still return validity over the
    // batch range; filter pushdown scans return row_ids plus row-aligned
    // validity.
    NoData,
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

inline ScanValueKind
ResolveDataScanValueKind(DataType data_type,
                         ScanProjection projection,
                         ScanValueKind requested_kind) {
    const auto column_kind = GetScanValueKindForDataType(data_type);
    AssertInfo(column_kind.has_value(),
               "data scan does not support column type {}",
               data_type);
    const auto resolved_kind = projection == ScanProjection::NoData ||
                                       requested_kind == ScanValueKind::Default
                                   ? *column_kind
                                   : requested_kind;
    AssertInfo(resolved_kind == *column_kind,
               "data scan kind {} does not match column type {}, expected {}",
               static_cast<int>(resolved_kind),
               data_type,
               static_cast<int>(*column_kind));
    return resolved_kind;
}

enum class ScanPredicate {
    None,
    Unary,
    BinaryRange,
};

struct ScanOptions {
    using CellSkipPredicate = std::function<bool(int64_t)>;

    ScanOptions() = default;

    ScanOptions(ScanOutput output,
                ScanPredicate predicate,
                int64_t start_offset,
                int64_t length,
                ScanProjection projection = ScanProjection::Data,
                ScanValueKind value_kind = ScanValueKind::Default,
                proto::plan::OpType op_type = proto::plan::OpType::Invalid,
                proto::plan::GenericValue value = {},
                proto::plan::GenericValue lower_value = {},
                proto::plan::GenericValue upper_value = {},
                bool lower_inclusive = false,
                bool upper_inclusive = false)
        : output(output),
          predicate(predicate),
          start_offset(start_offset),
          length(length),
          projection(projection),
          value_kind(value_kind),
          op_type(op_type),
          value(std::move(value)),
          lower_value(std::move(lower_value)),
          upper_value(std::move(upper_value)),
          lower_inclusive(lower_inclusive),
          upper_inclusive(upper_inclusive) {
    }

    static ScanOptions
    ForData(int64_t start_offset,
            int64_t length,
            ScanProjection projection = ScanProjection::Data,
            ScanValueKind value_kind = ScanValueKind::Default) {
        return ScanOptions(ScanOutput::Data,
                           ScanPredicate::None,
                           start_offset,
                           length,
                           projection,
                           value_kind);
    }

    static ScanOptions
    ForNoData(int64_t start_offset,
              int64_t length,
              ScanValueKind value_kind = ScanValueKind::Default) {
        return ForData(
            start_offset, length, ScanProjection::NoData, value_kind);
    }

    static ScanOptions
    ForUnary(int64_t start_offset,
             int64_t length,
             proto::plan::OpType op_type,
             const proto::plan::GenericValue& value) {
        return ScanOptions(ScanOutput::RowIds,
                           ScanPredicate::Unary,
                           start_offset,
                           length,
                           ScanProjection::NoData,
                           ScanValueKind::Default,
                           op_type,
                           value);
    }

    static ScanOptions
    ForBinaryRange(int64_t start_offset,
                   int64_t length,
                   const proto::plan::GenericValue& lower_value,
                   bool lower_inclusive,
                   const proto::plan::GenericValue& upper_value,
                   bool upper_inclusive) {
        return ScanOptions(ScanOutput::RowIds,
                           ScanPredicate::BinaryRange,
                           start_offset,
                           length,
                           ScanProjection::NoData,
                           ScanValueKind::Default,
                           proto::plan::OpType::Invalid,
                           {},
                           lower_value,
                           upper_value,
                           lower_inclusive,
                           upper_inclusive);
    }

    ScanOutput output = ScanOutput::Data;
    ScanPredicate predicate = ScanPredicate::None;
    int64_t start_offset = 0;
    int64_t length = 0;
    ScanProjection projection = ScanProjection::Data;
    ScanValueKind value_kind = ScanValueKind::Default;
    // The common Raw backend invokes metadata_skip_cell before pinning and
    // loaded_skip_cell after the selected Cells have been pinned. Backends
    // with native planners may keep their own scoped Cell identifiers instead.
    // Cell identifiers remain backend-private until PreparedScan converts the
    // final skipped set into segment-offset ScanPlan ranges.
    CellSkipPredicate metadata_skip_cell;
    CellSkipPredicate loaded_skip_cell;
    proto::plan::OpType op_type = proto::plan::OpType::Invalid;
    proto::plan::GenericValue value;
    proto::plan::GenericValue lower_value;
    proto::plan::GenericValue upper_value;
    bool lower_inclusive = false;
    bool upper_inclusive = false;
};

using ScanResult = std::unique_ptr<ScanCursor>;
using PreparedScanResult = std::shared_ptr<PreparedScan>;
using TakeResult = std::unique_ptr<TakeCursor>;

}  // namespace milvus
