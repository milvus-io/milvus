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
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/Array.h"
#include "common/ArrayValue.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Types.h"
#include "common/ValidityView.h"
#include "pb/plan.pb.h"

namespace milvus {

namespace detail {
class ColumnFilter;
using ColumnFilterPtr = std::shared_ptr<const ColumnFilter>;
}  // namespace detail

// The exact value representation requested by a Scan/Take caller. A backend
// validates this contract when the operation is opened and may normalize its
// storage representation while producing results. Next()/Get() never
// renegotiate the selected target.
enum class TargetType {
    None,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Float,
    Double,
    StringView,
    Json,
    ArrayView,
    ArrayValueView,
    VectorArrayView,
};

namespace detail {

template <typename T>
constexpr TargetType
TargetTypeOfOrNone() {
    using ValueType = std::remove_cv_t<std::remove_reference_t<T>>;
    if constexpr (std::is_same_v<ValueType, bool>) {
        return TargetType::Bool;
    } else if constexpr (std::is_same_v<ValueType, int8_t>) {
        return TargetType::Int8;
    } else if constexpr (std::is_same_v<ValueType, int16_t>) {
        return TargetType::Int16;
    } else if constexpr (std::is_same_v<ValueType, int32_t>) {
        return TargetType::Int32;
    } else if constexpr (std::is_same_v<ValueType, int64_t>) {
        return TargetType::Int64;
    } else if constexpr (std::is_same_v<ValueType, float>) {
        return TargetType::Float;
    } else if constexpr (std::is_same_v<ValueType, double>) {
        return TargetType::Double;
    } else if constexpr (std::is_same_v<ValueType, std::string_view>) {
        return TargetType::StringView;
    } else if constexpr (std::is_same_v<ValueType, Json>) {
        return TargetType::Json;
    } else if constexpr (std::is_same_v<ValueType, ArrayView>) {
        return TargetType::ArrayView;
    } else if constexpr (std::is_same_v<ValueType, ArrayValueView>) {
        return TargetType::ArrayValueView;
    } else if constexpr (std::is_same_v<ValueType, VectorArrayView>) {
        return TargetType::VectorArrayView;
    } else {
        return TargetType::None;
    }
}

}  // namespace detail

template <typename T>
inline constexpr bool HasTargetType =
    detail::TargetTypeOfOrNone<T>() != TargetType::None;

template <typename T>
constexpr TargetType
TargetTypeOf() {
    static_assert(HasTargetType<T>, "unsupported Scan/Take target type");
    return detail::TargetTypeOfOrNone<T>();
}

inline bool
IsFixedWidthTargetType(TargetType target_type) {
    return target_type == TargetType::Bool || target_type == TargetType::Int8 ||
           target_type == TargetType::Int16 ||
           target_type == TargetType::Int32 ||
           target_type == TargetType::Int64 ||
           target_type == TargetType::Float ||
           target_type == TargetType::Double;
}

inline bool
CanReadAsTargetType(DataType data_type, TargetType target_type) {
    switch (data_type) {
        case DataType::BOOL:
            return target_type == TargetType::Bool;
        case DataType::INT8:
            return target_type == TargetType::Int8;
        case DataType::INT16:
            return target_type == TargetType::Int16;
        case DataType::INT32:
            return target_type == TargetType::Int32;
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return target_type == TargetType::Int64;
        case DataType::FLOAT:
            return target_type == TargetType::Float;
        case DataType::DOUBLE:
            return target_type == TargetType::Double;
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
        case DataType::GEOMETRY:
            return target_type == TargetType::StringView;
        case DataType::JSON:
            return target_type == TargetType::Json;
        case DataType::ARRAY:
            return target_type == TargetType::ArrayView ||
                   target_type == TargetType::ArrayValueView;
        case DataType::VECTOR_ARRAY:
            return target_type == TargetType::VectorArrayView;
        default:
            return false;
    }
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

struct CellLocation {
    int64_t cell_id = -1;
    int64_t cell_offset = 0;
};

// The single Cell-geometry authority for one Column generation. Column
// backends use it for segment-offset-to-Cell conversion and Take planning;
// physical Cell details never leave the Column read implementation.
class ColumnPlanner {
 public:
    // Production Columns borrow their immutable generation metadata. The
    // rvalue overload retains ownership for standalone planners and tests.
    explicit ColumnPlanner(const std::vector<int64_t>& num_rows_until_cell);
    explicit ColumnPlanner(std::vector<int64_t>&& num_rows_until_cell);
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

 private:
    void
    ValidateBoundaries() const;

    std::shared_ptr<const std::vector<int64_t>> owned_num_rows_until_cell_;
    const std::vector<int64_t>* num_rows_until_cell_{nullptr};
};

struct ValueView {
    TargetType target_type = TargetType::None;
    const void* data = nullptr;
    int64_t offset = 0;
    int32_t byte_width = 0;

    bool
    empty() const {
        return target_type == TargetType::None || data == nullptr;
    }

    template <typename T>
    const T*
    data_as() const {
        AssertInfo(target_type == TargetTypeOf<T>() && data != nullptr,
                   "scan value view is empty");
        AssertInfo(byte_width == sizeof(T),
                   "scan value width {} does not match requested type width {}",
                   byte_width,
                   sizeof(T));
        return static_cast<const T*>(data) + offset;
    }
};

struct ScanBatch {
    // Every batch represents the dense row range
    // [row_id_start, row_id_start + size) for data scans. Filter pushdown
    // batches instead carry sparse row_ids.
    ValueView values;
    // Empty means every row in this batch is valid. The view may reference
    // expanded bools or an LSB-first packed bitmap owned by `owner`.
    ValidityView validity;
    // Filter pushdown batches may carry sparse row ids instead of a dense
    // segment range; data scans leave this empty.
    std::vector<int64_t> row_ids;
    std::shared_ptr<void> owner;
    int64_t row_id_start = 0;
    int64_t size = 0;
    // True only when the Column's filter proved that this batch's data is not
    // needed. Validity remains the field's real validity. The cursor splits at
    // filter boundaries, so this state applies to the complete dense batch.
    bool data_skipped = false;
};

struct OwnedTakeData {
    ValueView values;
    ValidityView validity;
    std::shared_ptr<void> owner;
    int64_t size = 0;
    // Empty means no positions were skipped. When present, this view is
    // aligned with the requested offsets; values at true positions are
    // unspecified and must not be evaluated.
    ValidityView data_skipped;
};

template <typename T>
struct TakeItem {
    std::optional<T> value;
    bool is_valid = true;
    bool data_skipped = false;
};

struct TakeItemState {
    bool is_valid = true;
    bool data_skipped = false;
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

    // A non-owning type-checked view over this result for a bounded caller
    // loop. It must not outlive the TakeResult. Access() validates the stable
    // result contract once; operator[] deliberately does not repeat bounds or
    // type checks for every item.
    template <typename T>
    class TypedAccessor {
     public:
        int64_t
        Size() const {
            return size_;
        }

        TakeItem<T>
        operator[](int64_t index) const {
            return result_->template GetUnchecked<T>(index);
        }

     private:
        friend class TakeResult;

        TypedAccessor(const TakeResult* result, int64_t size)
            : result_(result), size_(size) {
        }

        const TakeResult* result_;
        int64_t size_;
    };

    virtual int64_t
    Size() const = 0;

    virtual TargetType
    GetTargetType() const = 0;

    virtual DataType
    GetDataType() const = 0;

    // Validity-only access. It may pin the physical source because nullable
    // validity is colocated with data, but it must not construct/decode data or
    // evaluate the optional data filter.
    bool
    IsValid(int64_t index) const {
        CheckIndex(index);
        return PrepareItem(index, /*read_data=*/false).is_valid;
    }

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
    TypedAccessor<T>
    Access() const {
        const auto size = Size();
        AssertInfo(size >= 0, "take result has invalid size {}", size);
        const auto target_type = GetTargetType();
        AssertInfo(target_type == TargetTypeOf<T>(),
                   "take result target {} does not match requested target {}",
                   static_cast<int>(target_type),
                   static_cast<int>(TargetTypeOf<T>()));
        const auto data_type = GetDataType();
        AssertInfo(CanReadAsTargetType(data_type, target_type),
                   "take result type {} does not match requested target {}",
                   data_type,
                   static_cast<int>(target_type));
        return TypedAccessor<T>(this, size);
    }

    template <typename T>
    TakeItem<T>
    Get(int64_t index) const {
        auto accessor = Access<T>();
        AssertInfo(index >= 0 && index < accessor.Size(),
                   "take result index {} out of range {}",
                   index,
                   accessor.Size());
        return accessor[index];
    }

 protected:
    template <typename T>
    TakeItem<T>
    GetUnchecked(int64_t index) const {
        auto state = PrepareItem(index, /*read_data=*/true);
        TakeItem<T> item{std::nullopt, state.is_valid, state.data_skipped};
        if (!state.is_valid || state.data_skipped) {
            return item;
        }
        if constexpr (std::is_same_v<T, std::string_view>) {
            item.value.emplace(StringViewAt(index));
        } else if constexpr (std::is_same_v<T, Json>) {
            item.value.emplace(JsonAt(index));
        } else if constexpr (std::is_same_v<T, ArrayView>) {
            item.value.emplace(ArrayAt(index));
        } else if constexpr (std::is_same_v<T, ArrayValueView>) {
            item.value.emplace(ArrayValueAt(index));
        } else if constexpr (std::is_same_v<T, VectorArrayView>) {
            item.value.emplace(VectorArrayAt(index));
        } else {
            item.value.emplace(*static_cast<const T*>(FixedValueAt(index)));
        }
        return item;
    }

    void
    CheckIndex(int64_t index) const {
        const auto size = Size();
        AssertInfo(index >= 0 && index < size,
                   "take result index {} out of range {}",
                   index,
                   size);
    }

    virtual TakeItemState
    PrepareItem(int64_t index, bool read_data) const = 0;

    virtual const void*
    FixedValueAt(int64_t index) const = 0;

    virtual std::string_view
    StringViewAt(int64_t index) const = 0;

    virtual Json
    JsonAt(int64_t index) const = 0;

    virtual ArrayView
    ArrayAt(int64_t index) const = 0;

    virtual ArrayValueView
    ArrayValueAt(int64_t) const {
        ThrowInfo(ErrorCode::Unsupported,
                  "take result does not contain recursive ARRAY values");
    }

    virtual VectorArrayView
    VectorArrayAt(int64_t) const {
        ThrowInfo(ErrorCode::Unsupported,
                  "take result does not contain VECTOR_ARRAY values");
    }
};

struct TakeOptions {
    OffsetView offsets;
    TargetType target_type = TargetType::None;
    // Backend-neutral filter binding. Physical Cell/file planning and skip
    // decisions remain inside the Column implementation.
    detail::ColumnFilterPtr filter;
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

    // The next unread absolute segment position. A cursor is initialized at
    // ScanOptions::start_offset and advances by the source range consumed by
    // Next().
    virtual int64_t
    Position() const = 0;

    // Move the next unread position forward without reading the intervening
    // rows. Seeking backward is not supported; reopen a cursor instead.
    virtual void
    Seek(int64_t position) = 0;

    // Data scans return at most one dense batch beginning exactly at
    // Position(). The returned size may be smaller than length at a Cell,
    // file, reader, or ownership boundary, but must never exceed length.
    // Under CursorOwned, consume the borrowed data batch before the next
    // Next()/Seek(); under ResultOwned, its views remain valid while the
    // returned batch owner remains alive.
    //
    // Row-id scans consume the complete requested source range in one call and
    // return one sparse payload. They return true even when no row matches;
    // ScanBatch::size is then the row_ids count rather than consumed rows.
    //
    // A zero-length request returns false.
    virtual bool
    Next(int64_t length, ScanReadMode read_mode, ScanBatch* out) = 0;
};

enum class ScanPinPolicy {
    // The returned batch owns the physical Cell pin. Destroying or replacing
    // the batch releases that pin independently of cursor movement.
    ResultOwned,
    // The cursor owns the physical Cell pin. A returned batch is borrowed and
    // must be consumed before the next Next()/Seek() call.
    CursorOwned,
};

enum class ScanOutput {
    RowIds,
    Data,
};

enum class ScanPredicate {
    None,
    Unary,
    BinaryRange,
};

struct ScanOptions {
    ScanOptions() = default;

    ScanOptions(int64_t start_offset,
                TargetType target_type,
                ScanPinPolicy pin_policy = ScanPinPolicy::ResultOwned,
                bool prefetch = false)
        : start_offset(start_offset),
          target_type(target_type),
          pin_policy(pin_policy),
          prefetch(prefetch) {
    }

    static ScanOptions
    ForData(int64_t start_offset,
            TargetType target_type,
            ScanPinPolicy pin_policy = ScanPinPolicy::ResultOwned,
            bool prefetch = false) {
        return ScanOptions(start_offset, target_type, pin_policy, prefetch);
    }

    static ScanOptions
    ForUnary(int64_t start_offset,
             proto::plan::OpType op_type,
             const proto::plan::GenericValue& value,
             ScanPinPolicy pin_policy = ScanPinPolicy::ResultOwned) {
        ScanOptions options(start_offset, TargetType::None, pin_policy);
        options.output = ScanOutput::RowIds;
        options.predicate = ScanPredicate::Unary;
        options.op_type = op_type;
        options.value = value;
        return options;
    }

    static ScanOptions
    ForBinaryRange(int64_t start_offset,
                   const proto::plan::GenericValue& lower_value,
                   bool lower_inclusive,
                   const proto::plan::GenericValue& upper_value,
                   bool upper_inclusive,
                   ScanPinPolicy pin_policy = ScanPinPolicy::ResultOwned) {
        ScanOptions options(start_offset, TargetType::None, pin_policy);
        options.output = ScanOutput::RowIds;
        options.predicate = ScanPredicate::BinaryRange;
        options.lower_value = lower_value;
        options.upper_value = upper_value;
        options.lower_inclusive = lower_inclusive;
        options.upper_inclusive = upper_inclusive;
        return options;
    }

    ScanOutput output = ScanOutput::Data;
    ScanPredicate predicate = ScanPredicate::None;
    int64_t start_offset = 0;
    TargetType target_type = TargetType::None;
    ScanPinPolicy pin_policy = ScanPinPolicy::ResultOwned;
    // Batch-warm all remaining Cells at cursor creation without retaining
    // their pins, matching the legacy multi-chunk inline prefetch. Reads still
    // pin the current Cell one at a time, but cold loads are submitted
    // together so tiered-storage I/O stays parallel.
    bool prefetch = false;
    detail::ColumnFilterPtr filter;
    proto::plan::OpType op_type = proto::plan::OpType::Invalid;
    proto::plan::GenericValue value;
    proto::plan::GenericValue lower_value;
    proto::plan::GenericValue upper_value;
    bool lower_inclusive = false;
    bool upper_inclusive = false;
};

using ScanResult = std::unique_ptr<ScanCursor>;
using TakeResultPtr = std::unique_ptr<TakeResult>;

}  // namespace milvus
