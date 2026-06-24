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
#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "mmap/ChunkedColumnInterface.h"

namespace milvus {

namespace detail {

inline bool
IsSupportedUnaryScanOp(proto::plan::OpType op_type) {
    switch (op_type) {
        case proto::plan::OpType::GreaterThan:
        case proto::plan::OpType::GreaterEqual:
        case proto::plan::OpType::LessThan:
        case proto::plan::OpType::LessEqual:
        case proto::plan::OpType::Equal:
        case proto::plan::OpType::NotEqual:
            return true;
        default:
            return false;
    }
}

template <typename T>
inline bool
TryGetScanValue(const proto::plan::GenericValue& value, T* out) {
    if constexpr (std::is_same_v<T, bool>) {
        if (value.val_case() != proto::plan::GenericValue::kBoolVal) {
            return false;
        }
        *out = value.bool_val();
        return true;
    } else if constexpr (std::is_integral_v<T>) {
        if (value.val_case() != proto::plan::GenericValue::kInt64Val) {
            return false;
        }
        if (value.int64_val() <
                static_cast<int64_t>(std::numeric_limits<T>::min()) ||
            value.int64_val() >
                static_cast<int64_t>(std::numeric_limits<T>::max())) {
            return false;
        }
        *out = static_cast<T>(value.int64_val());
        return true;
    } else if constexpr (std::is_floating_point_v<T>) {
        if (value.val_case() == proto::plan::GenericValue::kFloatVal) {
            *out = static_cast<T>(value.float_val());
            return true;
        }
        if (value.val_case() == proto::plan::GenericValue::kInt64Val) {
            *out = static_cast<T>(value.int64_val());
            return true;
        }
        return false;
    }
    return false;
}

template <typename T>
inline bool
CanUseScanValue(const proto::plan::GenericValue& value) {
    T typed_value{};
    return TryGetScanValue<T>(value, &typed_value);
}

template <typename T>
inline bool
UnaryScanCompare(T lhs, T rhs, proto::plan::OpType op_type) {
    switch (op_type) {
        case proto::plan::OpType::GreaterThan:
            return lhs > rhs;
        case proto::plan::OpType::GreaterEqual:
            return lhs >= rhs;
        case proto::plan::OpType::LessThan:
            return lhs < rhs;
        case proto::plan::OpType::LessEqual:
            return lhs <= rhs;
        case proto::plan::OpType::Equal:
            return lhs == rhs;
        case proto::plan::OpType::NotEqual:
            return lhs != rhs;
        default:
            return false;
    }
}

inline void
ResetRowIdPayloadOutput(ChunkedColumnInterface::ScanBatch* out) {
    out->values = ChunkedColumnInterface::ValueView{};
    out->validity = ChunkedColumnInterface::ValidityView{};
    out->row_ids.clear();
    out->owner.reset();
    out->row_id_start = 0;
    out->size = 0;
}

inline void
AppendRowIdPayloadEntry(ChunkedColumnInterface::ScanBatch* out,
                        FixedVector<bool>* validity,
                        bool* has_invalid,
                        int64_t row_id,
                        bool valid) {
    AssertInfo(validity != nullptr, "row id payload validity owner is null");
    AssertInfo(has_invalid != nullptr, "row id payload invalid flag is null");
    if (!out->row_ids.empty()) {
        AssertInfo(out->row_ids.back() <= row_id,
                   "row id payload is not ordered: {} before {}",
                   out->row_ids.back(),
                   row_id);
    }
    out->row_ids.emplace_back(row_id);
    validity->push_back(valid);
    *has_invalid = *has_invalid || !valid;
}

inline void
FinalizeRowIdPayloadOutput(ChunkedColumnInterface::ScanBatch* out,
                           std::shared_ptr<FixedVector<bool>> validity,
                           bool has_invalid,
                           bool nullable) {
    AssertInfo(validity != nullptr, "row id payload validity owner is null");
    AssertInfo(validity->size() == out->row_ids.size(),
               "row id payload validity size {} does not match row ids size {}",
               validity->size(),
               out->row_ids.size());
    out->row_id_start = out->row_ids.empty() ? 0 : out->row_ids.front();
    out->size = static_cast<int64_t>(out->row_ids.size());
    out->validity.size = out->size;
    out->validity.nullable = nullable;
    if (out->row_ids.empty() || !has_invalid) {
        out->validity.encoding =
            ChunkedColumnInterface::ValidityEncoding::AllValid;
        out->validity.all_valid = true;
        return;
    }
    out->validity.encoding =
        ChunkedColumnInterface::ValidityEncoding::BoolArray;
    out->validity.data = validity->data();
    out->validity.offset = 0;
    out->validity.all_valid = false;
    out->owner = std::move(validity);
}

class FixedWidthRowIdScanCursor final
    : public ChunkedColumnInterface::ScanCursor {
 public:
    FixedWidthRowIdScanCursor(const ChunkedColumnInterface* column,
                              milvus::OpContext* op_ctx,
                              int64_t start_offset,
                              int64_t length,
                              DataType data_type,
                              proto::plan::OpType op_type,
                              const proto::plan::GenericValue& value)
        : column_(column),
          op_ctx_(op_ctx),
          data_type_(data_type),
          op_type_(op_type),
          value_(value),
          scan_pos_(start_offset),
          scan_end_(start_offset + length) {
        if (start_offset < scan_end_) {
            auto [chunk_id, offset] = column_->GetChunkIDByOffset(start_offset);
            current_chunk_id_ = static_cast<int64_t>(chunk_id);
            current_chunk_offset_ = static_cast<int64_t>(offset);
        }
    }

    bool
    Next(ChunkedColumnInterface::ScanBatch* out) override {
        AssertInfo(out != nullptr, "row id scan output batch is null");
        ResetRowIdPayloadOutput(out);
        if (scan_pos_ >= scan_end_) {
            return false;
        }

        while (scan_pos_ < scan_end_) {
            auto& span = GetCurrentSpan();
            const auto rows = span.get().row_count();
            if (current_chunk_offset_ >= rows) {
                ++current_chunk_id_;
                current_chunk_offset_ = 0;
                cached_span_.reset();
                continue;
            }

            const auto rows_left_in_chunk = rows - current_chunk_offset_;
            const auto rows_left_in_scan = scan_end_ - scan_pos_;
            const auto rows_to_scan =
                std::min<int64_t>(rows_left_in_chunk, rows_left_in_scan);
            auto validity = std::make_shared<FixedVector<bool>>();
            bool has_invalid = false;
            ScanSpan(
                span.get(), rows_to_scan, out, validity.get(), &has_invalid);

            scan_pos_ += rows_to_scan;
            current_chunk_offset_ += rows_to_scan;
            if (!out->row_ids.empty()) {
                FinalizeRowIdPayloadOutput(out,
                                           std::move(validity),
                                           has_invalid,
                                           column_->IsNullable());
                return true;
            }
        }
        return false;
    }

 private:
    PinWrapper<SpanBase>&
    GetCurrentSpan() {
        if (!cached_span_.has_value() ||
            cached_chunk_id_ != current_chunk_id_) {
            cached_span_ = column_->Span(op_ctx_, current_chunk_id_);
            cached_chunk_id_ = current_chunk_id_;
        }
        return cached_span_.value();
    }

    template <typename T>
    void
    ScanTypedSpan(const SpanBase& span,
                  int64_t rows_to_scan,
                  ChunkedColumnInterface::ScanBatch* out,
                  FixedVector<bool>* validity,
                  bool* has_invalid) {
        T value{};
        if (!TryGetScanValue<T>(value_, &value)) {
            return;
        }

        const auto* data = static_cast<const T*>(span.data());
        const auto* valid = span.valid_data();
        for (int64_t i = 0; i < rows_to_scan; ++i) {
            const auto local_offset = current_chunk_offset_ + i;
            if (valid != nullptr && !valid[local_offset]) {
                AppendRowIdPayloadEntry(
                    out, validity, has_invalid, scan_pos_ + i, false);
                continue;
            }
            if (UnaryScanCompare<T>(data[local_offset], value, op_type_)) {
                AppendRowIdPayloadEntry(
                    out, validity, has_invalid, scan_pos_ + i, true);
            }
        }
    }

    void
    ScanSpan(const SpanBase& span,
             int64_t rows_to_scan,
             ChunkedColumnInterface::ScanBatch* out,
             FixedVector<bool>* validity,
             bool* has_invalid) {
        switch (data_type_) {
            case DataType::BOOL:
                ScanTypedSpan<bool>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            case DataType::INT8:
                ScanTypedSpan<int8_t>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            case DataType::INT16:
                ScanTypedSpan<int16_t>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            case DataType::INT32:
                ScanTypedSpan<int32_t>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            case DataType::INT64:
            case DataType::TIMESTAMPTZ:
                ScanTypedSpan<int64_t>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            case DataType::FLOAT:
                ScanTypedSpan<float>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            case DataType::DOUBLE:
                ScanTypedSpan<double>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            default:
                break;
        }
    }

    const ChunkedColumnInterface* column_;
    milvus::OpContext* op_ctx_;
    DataType data_type_;
    proto::plan::OpType op_type_;
    proto::plan::GenericValue value_;
    int64_t scan_pos_;
    int64_t scan_end_;
    int64_t current_chunk_id_{0};
    int64_t current_chunk_offset_{0};
    int64_t cached_chunk_id_{-1};
    std::optional<PinWrapper<SpanBase>> cached_span_{std::nullopt};
};

template <typename T>
inline bool
BinaryRangeScanCompare(
    T value, T lower, bool lower_inclusive, T upper, bool upper_inclusive) {
    const bool lower_ok = lower_inclusive ? value >= lower : value > lower;
    const bool upper_ok = upper_inclusive ? value <= upper : value < upper;
    return lower_ok && upper_ok;
}

class FixedWidthBinaryRangeRowIdScanCursor final
    : public ChunkedColumnInterface::ScanCursor {
 public:
    FixedWidthBinaryRangeRowIdScanCursor(
        const ChunkedColumnInterface* column,
        milvus::OpContext* op_ctx,
        int64_t start_offset,
        int64_t length,
        DataType data_type,
        const proto::plan::GenericValue& lower_value,
        bool lower_inclusive,
        const proto::plan::GenericValue& upper_value,
        bool upper_inclusive)
        : column_(column),
          op_ctx_(op_ctx),
          data_type_(data_type),
          lower_value_(lower_value),
          upper_value_(upper_value),
          lower_inclusive_(lower_inclusive),
          upper_inclusive_(upper_inclusive),
          scan_pos_(start_offset),
          scan_end_(start_offset + length) {
        if (start_offset < scan_end_) {
            auto [chunk_id, offset] = column_->GetChunkIDByOffset(start_offset);
            current_chunk_id_ = static_cast<int64_t>(chunk_id);
            current_chunk_offset_ = static_cast<int64_t>(offset);
        }
    }

    bool
    Next(ChunkedColumnInterface::ScanBatch* out) override {
        AssertInfo(out != nullptr,
                   "binary range row id scan output batch is null");
        ResetRowIdPayloadOutput(out);
        if (scan_pos_ >= scan_end_) {
            return false;
        }

        while (scan_pos_ < scan_end_) {
            auto& span = GetCurrentSpan();
            const auto rows = span.get().row_count();
            if (current_chunk_offset_ >= rows) {
                ++current_chunk_id_;
                current_chunk_offset_ = 0;
                cached_span_.reset();
                continue;
            }

            const auto rows_left_in_chunk = rows - current_chunk_offset_;
            const auto rows_left_in_scan = scan_end_ - scan_pos_;
            const auto rows_to_scan =
                std::min<int64_t>(rows_left_in_chunk, rows_left_in_scan);
            auto validity = std::make_shared<FixedVector<bool>>();
            bool has_invalid = false;
            ScanSpan(
                span.get(), rows_to_scan, out, validity.get(), &has_invalid);

            scan_pos_ += rows_to_scan;
            current_chunk_offset_ += rows_to_scan;
            if (!out->row_ids.empty()) {
                FinalizeRowIdPayloadOutput(out,
                                           std::move(validity),
                                           has_invalid,
                                           column_->IsNullable());
                return true;
            }
        }
        return false;
    }

 private:
    PinWrapper<SpanBase>&
    GetCurrentSpan() {
        if (!cached_span_.has_value() ||
            cached_chunk_id_ != current_chunk_id_) {
            cached_span_ = column_->Span(op_ctx_, current_chunk_id_);
            cached_chunk_id_ = current_chunk_id_;
        }
        return cached_span_.value();
    }

    template <typename T>
    void
    ScanTypedSpan(const SpanBase& span,
                  int64_t rows_to_scan,
                  ChunkedColumnInterface::ScanBatch* out,
                  FixedVector<bool>* validity,
                  bool* has_invalid) {
        T lower{};
        T upper{};
        if (!TryGetScanValue<T>(lower_value_, &lower) ||
            !TryGetScanValue<T>(upper_value_, &upper)) {
            return;
        }

        const auto* data = static_cast<const T*>(span.data());
        const auto* valid = span.valid_data();
        for (int64_t i = 0; i < rows_to_scan; ++i) {
            const auto local_offset = current_chunk_offset_ + i;
            if (valid != nullptr && !valid[local_offset]) {
                AppendRowIdPayloadEntry(
                    out, validity, has_invalid, scan_pos_ + i, false);
                continue;
            }
            if (BinaryRangeScanCompare<T>(data[local_offset],
                                          lower,
                                          lower_inclusive_,
                                          upper,
                                          upper_inclusive_)) {
                AppendRowIdPayloadEntry(
                    out, validity, has_invalid, scan_pos_ + i, true);
            }
        }
    }

    void
    ScanSpan(const SpanBase& span,
             int64_t rows_to_scan,
             ChunkedColumnInterface::ScanBatch* out,
             FixedVector<bool>* validity,
             bool* has_invalid) {
        switch (data_type_) {
            case DataType::BOOL:
                ScanTypedSpan<bool>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            case DataType::INT8:
                ScanTypedSpan<int8_t>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            case DataType::INT16:
                ScanTypedSpan<int16_t>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            case DataType::INT32:
                ScanTypedSpan<int32_t>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            case DataType::INT64:
            case DataType::TIMESTAMPTZ:
                ScanTypedSpan<int64_t>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            case DataType::FLOAT:
                ScanTypedSpan<float>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            case DataType::DOUBLE:
                ScanTypedSpan<double>(
                    span, rows_to_scan, out, validity, has_invalid);
                break;
            default:
                break;
        }
    }

    const ChunkedColumnInterface* column_;
    milvus::OpContext* op_ctx_;
    DataType data_type_;
    proto::plan::GenericValue lower_value_;
    proto::plan::GenericValue upper_value_;
    bool lower_inclusive_;
    bool upper_inclusive_;
    int64_t scan_pos_;
    int64_t scan_end_;
    int64_t current_chunk_id_{0};
    int64_t current_chunk_offset_{0};
    int64_t cached_chunk_id_{-1};
    std::optional<PinWrapper<SpanBase>> cached_span_{std::nullopt};
};

class FixedWidthDataScanCursor final
    : public ChunkedColumnInterface::ScanCursor {
 public:
    FixedWidthDataScanCursor(const ChunkedColumnInterface* column,
                             milvus::OpContext* op_ctx,
                             int64_t start_offset,
                             int64_t length,
                             DataType data_type,
                             ChunkedColumnInterface::ScanProjection projection,
                             ChunkedColumnInterface::ScanValueKind value_kind)
        : column_(column),
          op_ctx_(op_ctx),
          data_type_(data_type),
          projection_(projection),
          value_kind_(value_kind),
          scan_pos_(start_offset),
          scan_end_(start_offset + length) {
        if (start_offset < scan_end_) {
            auto [chunk_id, offset] = column_->GetChunkIDByOffset(start_offset);
            current_chunk_id_ = static_cast<int64_t>(chunk_id);
            current_chunk_offset_ = static_cast<int64_t>(offset);
        }
    }

    bool
    Next(ChunkedColumnInterface::ScanBatch* out) override {
        AssertInfo(out != nullptr, "data scan output batch is null");
        out->values = ChunkedColumnInterface::ValueView{};
        out->validity = ChunkedColumnInterface::ValidityView{};
        out->row_ids.clear();
        out->owner.reset();
        out->row_id_start = 0;
        out->size = 0;
        if (scan_pos_ >= scan_end_) {
            return false;
        }

        while (scan_pos_ < scan_end_) {
            auto& span = GetCurrentSpan();
            const auto rows = span.get().row_count();
            if (current_chunk_offset_ >= rows) {
                ++current_chunk_id_;
                current_chunk_offset_ = 0;
                cached_span_.reset();
                continue;
            }

            const auto rows_left_in_chunk = rows - current_chunk_offset_;
            const auto rows_left_in_scan = scan_end_ - scan_pos_;
            const auto rows_to_return =
                std::min<int64_t>(rows_left_in_chunk, rows_left_in_scan);

            auto owner = std::make_shared<PinWrapper<SpanBase>>(span);
            if (projection_ != ChunkedColumnInterface::ScanProjection::NoData) {
                out->values.encoding =
                    ChunkedColumnInterface::ValueEncoding::FixedWidth;
                out->values.kind =
                    value_kind_ ==
                            ChunkedColumnInterface::ScanValueKind::Default
                        ? ChunkedColumnInterface::ScanValueKind::FixedWidth
                        : value_kind_;
                out->values.physical_type = data_type_;
                out->values.logical_type = data_type_;
                out->values.data = span.get().data();
                out->values.offset = current_chunk_offset_;
                out->values.size = rows_to_return;
                out->values.byte_width = span.get().element_sizeof();
            }
            out->validity.size = rows_to_return;
            out->validity.nullable = column_->IsNullable();
            if (span.get().valid_data() != nullptr) {
                out->validity.encoding =
                    ChunkedColumnInterface::ValidityEncoding::BoolArray;
                out->validity.data = span.get().valid_data();
                out->validity.offset = current_chunk_offset_;
                out->validity.all_valid = false;
            }
            out->owner = std::move(owner);
            out->row_id_start = scan_pos_;
            out->size = rows_to_return;

            scan_pos_ += rows_to_return;
            current_chunk_offset_ += rows_to_return;
            return true;
        }
        return false;
    }

 private:
    PinWrapper<SpanBase>&
    GetCurrentSpan() {
        if (!cached_span_.has_value() ||
            cached_chunk_id_ != current_chunk_id_) {
            cached_span_ = column_->Span(op_ctx_, current_chunk_id_);
            cached_chunk_id_ = current_chunk_id_;
        }
        return cached_span_.value();
    }

    const ChunkedColumnInterface* column_;
    milvus::OpContext* op_ctx_;
    DataType data_type_;
    ChunkedColumnInterface::ScanProjection projection_;
    ChunkedColumnInterface::ScanValueKind value_kind_;
    int64_t scan_pos_;
    int64_t scan_end_;
    int64_t current_chunk_id_{0};
    int64_t current_chunk_offset_{0};
    int64_t cached_chunk_id_{-1};
    std::optional<PinWrapper<SpanBase>> cached_span_{std::nullopt};
};

class ViewDataScanCursor final : public ChunkedColumnInterface::ScanCursor {
 public:
    ViewDataScanCursor(const ChunkedColumnInterface* column,
                       milvus::OpContext* op_ctx,
                       int64_t start_offset,
                       int64_t length,
                       DataType data_type,
                       ChunkedColumnInterface::ScanProjection projection,
                       ChunkedColumnInterface::ScanValueKind value_kind)
        : column_(column),
          op_ctx_(op_ctx),
          data_type_(data_type),
          projection_(projection),
          value_kind_(value_kind),
          scan_pos_(start_offset),
          scan_end_(start_offset + length) {
    }

    bool
    Next(ChunkedColumnInterface::ScanBatch* out) override {
        AssertInfo(out != nullptr, "view data scan output batch is null");
        ResetOutput(out);
        if (scan_pos_ >= scan_end_) {
            return false;
        }

        auto [chunk_id, offset] = column_->GetChunkIDByOffset(scan_pos_);
        const auto chunk_rows = column_->chunk_row_nums(chunk_id);
        const auto rows_to_return = std::min<int64_t>(
            {chunk_rows - static_cast<int64_t>(offset), scan_end_ - scan_pos_});
        AssertInfo(rows_to_return > 0,
                   "invalid view data scan batch at offset {}",
                   scan_pos_);

        out->row_id_start = scan_pos_;
        out->size = rows_to_return;
        if (projection_ == ChunkedColumnInterface::ScanProjection::NoData) {
            FillNoDataBatch(chunk_id, offset, out);
            scan_pos_ += rows_to_return;
            return true;
        }

        const auto range =
            std::make_pair(static_cast<int64_t>(offset), rows_to_return);
        switch (value_kind_) {
            case ChunkedColumnInterface::ScanValueKind::StringView:
                FillStringViewBatch(chunk_id, range, out);
                break;
            case ChunkedColumnInterface::ScanValueKind::JsonView:
                FillJsonViewBatch(chunk_id, range, out);
                break;
            case ChunkedColumnInterface::ScanValueKind::ArrayView:
                FillArrayViewBatch(chunk_id, range, out);
                break;
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "unsupported view data scan kind {}",
                          static_cast<int>(value_kind_));
        }

        scan_pos_ += rows_to_return;
        return true;
    }

 private:
    using StringViews =
        std::pair<std::vector<std::string_view>, FixedVector<bool>>;
    using ArrayViews = std::pair<std::vector<ArrayView>, FixedVector<bool>>;

    struct StringOwner {
        explicit StringOwner(PinWrapper<StringViews>&& views)
            : views(std::move(views)) {
        }
        PinWrapper<StringViews> views;
    };

    struct JsonOwner {
        explicit JsonOwner(PinWrapper<StringViews>&& views)
            : views(std::move(views)) {
            auto& strings = this->views.get().first;
            values.reserve(strings.size());
            for (const auto& value : strings) {
                values.emplace_back(Json(value));
            }
        }
        PinWrapper<StringViews> views;
        std::vector<Json> values;
    };

    struct ArrayOwner {
        explicit ArrayOwner(PinWrapper<ArrayViews>&& views)
            : views(std::move(views)) {
        }
        PinWrapper<ArrayViews> views;
    };

    struct ValidityOwner {
        explicit ValidityOwner(PinWrapper<Chunk*>&& chunk)
            : chunk(std::move(chunk)) {
        }
        PinWrapper<Chunk*> chunk;
    };

    static void
    ResetOutput(ChunkedColumnInterface::ScanBatch* out) {
        out->values = ChunkedColumnInterface::ValueView{};
        out->validity = ChunkedColumnInterface::ValidityView{};
        out->row_ids.clear();
        out->owner.reset();
        out->row_id_start = 0;
        out->size = 0;
    }

    void
    FillValidity(const FixedVector<bool>& valid_data,
                 ChunkedColumnInterface::ScanBatch* out) const {
        out->validity.size = out->size;
        out->validity.nullable = column_->IsNullable();
        if (!column_->IsNullable() || valid_data.empty()) {
            out->validity.encoding =
                ChunkedColumnInterface::ValidityEncoding::AllValid;
            out->validity.all_valid = true;
            return;
        }
        out->validity.encoding =
            ChunkedColumnInterface::ValidityEncoding::BoolArray;
        out->validity.data = valid_data.data();
        out->validity.offset = 0;
        out->validity.all_valid = false;
    }

    void
    FillNoDataBatch(int64_t chunk_id,
                    int64_t offset,
                    ChunkedColumnInterface::ScanBatch* out) const {
        out->validity.size = out->size;
        out->validity.nullable = column_->IsNullable();
        if (!column_->IsNullable()) {
            out->validity.encoding =
                ChunkedColumnInterface::ValidityEncoding::AllValid;
            out->validity.all_valid = true;
            return;
        }

        auto owner = std::make_shared<ValidityOwner>(
            column_->GetChunk(op_ctx_, chunk_id));
        const auto& valid_data = owner->chunk.get()->Valid();
        if (valid_data.empty()) {
            out->validity.encoding =
                ChunkedColumnInterface::ValidityEncoding::AllValid;
            out->validity.all_valid = true;
            return;
        }

        out->validity.encoding =
            ChunkedColumnInterface::ValidityEncoding::BoolArray;
        out->validity.data = valid_data.data();
        out->validity.offset = offset;
        out->validity.all_valid = false;
        out->owner = std::move(owner);
    }

    void
    FillStringViewBatch(int64_t chunk_id,
                        std::pair<int64_t, int64_t> range,
                        ChunkedColumnInterface::ScanBatch* out) const {
        auto owner = std::make_shared<StringOwner>(
            column_->StringViews(op_ctx_, chunk_id, range));
        auto& views = owner->views.get();
        out->values.encoding =
            ChunkedColumnInterface::ValueEncoding::StringView;
        out->values.kind = ChunkedColumnInterface::ScanValueKind::StringView;
        out->values.physical_type = data_type_;
        out->values.logical_type = data_type_;
        out->values.data = views.first.data();
        out->values.offset = 0;
        out->values.size = out->size;
        out->values.byte_width = sizeof(std::string_view);
        FillValidity(views.second, out);
        out->owner = std::move(owner);
    }

    void
    FillJsonViewBatch(int64_t chunk_id,
                      std::pair<int64_t, int64_t> range,
                      ChunkedColumnInterface::ScanBatch* out) const {
        auto owner = std::make_shared<JsonOwner>(
            column_->StringViews(op_ctx_, chunk_id, range));
        out->values.encoding = ChunkedColumnInterface::ValueEncoding::JsonView;
        out->values.kind = ChunkedColumnInterface::ScanValueKind::JsonView;
        out->values.physical_type = data_type_;
        out->values.logical_type = DataType::JSON;
        out->values.data = owner->values.data();
        out->values.offset = 0;
        out->values.size = out->size;
        out->values.byte_width = sizeof(Json);
        FillValidity(owner->views.get().second, out);
        out->owner = std::move(owner);
    }

    void
    FillArrayViewBatch(int64_t chunk_id,
                       std::pair<int64_t, int64_t> range,
                       ChunkedColumnInterface::ScanBatch* out) const {
        auto owner = std::make_shared<ArrayOwner>(
            column_->ArrayViews(op_ctx_, chunk_id, range));
        auto& views = owner->views.get();
        out->values.encoding = ChunkedColumnInterface::ValueEncoding::ArrayView;
        out->values.kind = ChunkedColumnInterface::ScanValueKind::ArrayView;
        out->values.physical_type = data_type_;
        out->values.logical_type = DataType::ARRAY;
        out->values.data = views.first.data();
        out->values.offset = 0;
        out->values.size = out->size;
        out->values.byte_width = sizeof(ArrayView);
        FillValidity(views.second, out);
        out->owner = std::move(owner);
    }

    const ChunkedColumnInterface* column_;
    milvus::OpContext* op_ctx_;
    DataType data_type_;
    ChunkedColumnInterface::ScanProjection projection_;
    ChunkedColumnInterface::ScanValueKind value_kind_;
    int64_t scan_pos_;
    int64_t scan_end_;
};

inline bool
CanUseFixedWidthRowIdScan(DataType data_type,
                          proto::plan::OpType op_type,
                          const proto::plan::GenericValue& value) {
    if (!IsSupportedUnaryScanOp(op_type)) {
        return false;
    }

    switch (data_type) {
        case DataType::BOOL:
            return CanUseScanValue<bool>(value);
        case DataType::INT8:
            return CanUseScanValue<int8_t>(value);
        case DataType::INT16:
            return CanUseScanValue<int16_t>(value);
        case DataType::INT32:
            return CanUseScanValue<int32_t>(value);
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return CanUseScanValue<int64_t>(value);
        case DataType::FLOAT:
            return CanUseScanValue<float>(value);
        case DataType::DOUBLE:
            return CanUseScanValue<double>(value);
        default:
            return false;
    }
}

inline bool
CanUseFixedWidthBinaryRangeRowIdScan(
    DataType data_type,
    const proto::plan::GenericValue& lower_value,
    const proto::plan::GenericValue& upper_value) {
    switch (data_type) {
        case DataType::BOOL:
            return CanUseScanValue<bool>(lower_value) &&
                   CanUseScanValue<bool>(upper_value);
        case DataType::INT8:
            return CanUseScanValue<int8_t>(lower_value) &&
                   CanUseScanValue<int8_t>(upper_value);
        case DataType::INT16:
            return CanUseScanValue<int16_t>(lower_value) &&
                   CanUseScanValue<int16_t>(upper_value);
        case DataType::INT32:
            return CanUseScanValue<int32_t>(lower_value) &&
                   CanUseScanValue<int32_t>(upper_value);
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return CanUseScanValue<int64_t>(lower_value) &&
                   CanUseScanValue<int64_t>(upper_value);
        case DataType::FLOAT:
            return CanUseScanValue<float>(lower_value) &&
                   CanUseScanValue<float>(upper_value);
        case DataType::DOUBLE:
            return CanUseScanValue<double>(lower_value) &&
                   CanUseScanValue<double>(upper_value);
        default:
            return false;
    }
}

inline std::unique_ptr<ChunkedColumnInterface::ScanCursor>
MakeFixedWidthRowIdScanCursor(const ChunkedColumnInterface* column,
                              milvus::OpContext* op_ctx,
                              int64_t start_offset,
                              int64_t length,
                              DataType data_type,
                              proto::plan::OpType op_type,
                              const proto::plan::GenericValue& value) {
    AssertInfo(
        start_offset >= 0 && length >= 0 &&
            start_offset + length <= static_cast<int64_t>(column->NumRows()),
        "row id scan range [{}, {}) out of rows {}",
        start_offset,
        start_offset + length,
        column->NumRows());
    if (!CanUseFixedWidthRowIdScan(data_type, op_type, value)) {
        return nullptr;
    }
    return std::make_unique<FixedWidthRowIdScanCursor>(
        column, op_ctx, start_offset, length, data_type, op_type, value);
}

inline std::unique_ptr<ChunkedColumnInterface::ScanCursor>
MakeFixedWidthBinaryRangeRowIdScanCursor(
    const ChunkedColumnInterface* column,
    milvus::OpContext* op_ctx,
    int64_t start_offset,
    int64_t length,
    DataType data_type,
    const proto::plan::GenericValue& lower_value,
    bool lower_inclusive,
    const proto::plan::GenericValue& upper_value,
    bool upper_inclusive) {
    AssertInfo(
        start_offset >= 0 && length >= 0 &&
            start_offset + length <= static_cast<int64_t>(column->NumRows()),
        "binary range row id scan range [{}, {}) out of rows {}",
        start_offset,
        start_offset + length,
        column->NumRows());
    if (!CanUseFixedWidthBinaryRangeRowIdScan(
            data_type, lower_value, upper_value)) {
        return nullptr;
    }
    return std::make_unique<FixedWidthBinaryRangeRowIdScanCursor>(
        column,
        op_ctx,
        start_offset,
        length,
        data_type,
        lower_value,
        lower_inclusive,
        upper_value,
        upper_inclusive);
}

inline std::unique_ptr<ChunkedColumnInterface::ScanCursor>
MakeFixedWidthDataScanCursor(const ChunkedColumnInterface* column,
                             milvus::OpContext* op_ctx,
                             int64_t start_offset,
                             int64_t length,
                             DataType data_type,
                             ChunkedColumnInterface::ScanProjection projection,
                             ChunkedColumnInterface::ScanValueKind value_kind) {
    AssertInfo(
        start_offset >= 0 && length >= 0 &&
            start_offset + length <= static_cast<int64_t>(column->NumRows()),
        "data scan range [{}, {}) out of rows {}",
        start_offset,
        start_offset + length,
        column->NumRows());
    if (!ChunkedColumnInterface::IsPrimitiveDataType(data_type)) {
        return nullptr;
    }
    return std::make_unique<FixedWidthDataScanCursor>(column,
                                                      op_ctx,
                                                      start_offset,
                                                      length,
                                                      data_type,
                                                      projection,
                                                      value_kind);
}

inline std::unique_ptr<ChunkedColumnInterface::ScanCursor>
MakeDataScanCursor(const ChunkedColumnInterface* column,
                   milvus::OpContext* op_ctx,
                   int64_t start_offset,
                   int64_t length,
                   DataType data_type,
                   ChunkedColumnInterface::ScanProjection projection,
                   ChunkedColumnInterface::ScanValueKind value_kind) {
    auto resolved_kind = value_kind;
    if (resolved_kind == ChunkedColumnInterface::ScanValueKind::Default) {
        if (ChunkedColumnInterface::IsPrimitiveDataType(data_type)) {
            resolved_kind = ChunkedColumnInterface::ScanValueKind::FixedWidth;
        } else if (data_type == DataType::JSON) {
            resolved_kind = ChunkedColumnInterface::ScanValueKind::JsonView;
        } else if (data_type == DataType::STRING ||
                   data_type == DataType::VARCHAR ||
                   data_type == DataType::TEXT ||
                   data_type == DataType::GEOMETRY) {
            resolved_kind = ChunkedColumnInterface::ScanValueKind::StringView;
        } else if (data_type == DataType::ARRAY) {
            resolved_kind = ChunkedColumnInterface::ScanValueKind::ArrayView;
        }
    }

    if (resolved_kind == ChunkedColumnInterface::ScanValueKind::FixedWidth) {
        return MakeFixedWidthDataScanCursor(column,
                                            op_ctx,
                                            start_offset,
                                            length,
                                            data_type,
                                            projection,
                                            resolved_kind);
    }

    if (resolved_kind == ChunkedColumnInterface::ScanValueKind::StringView ||
        resolved_kind == ChunkedColumnInterface::ScanValueKind::JsonView ||
        resolved_kind == ChunkedColumnInterface::ScanValueKind::ArrayView) {
        AssertInfo(start_offset >= 0 && length >= 0 &&
                       start_offset + length <=
                           static_cast<int64_t>(column->NumRows()),
                   "data scan range [{}, {}) out of rows {}",
                   start_offset,
                   start_offset + length,
                   column->NumRows());
        return std::make_unique<ViewDataScanCursor>(column,
                                                    op_ctx,
                                                    start_offset,
                                                    length,
                                                    data_type,
                                                    projection,
                                                    resolved_kind);
    }

    return nullptr;
}

}  // namespace detail

ChunkedColumnInterface::ScanResult
ChunkedColumnInterface::Scan(milvus::OpContext* op_ctx,
                             const ScanOptions& options) const {
    auto data_type = GetDefaultScanDataType();
    if (!data_type.has_value()) {
        return nullptr;
    }

    if (options.output != ScanOutput::Data ||
        options.predicate != ScanPredicate::None) {
        return nullptr;
    }

    return detail::MakeDataScanCursor(this,
                                      op_ctx,
                                      options.start_offset,
                                      options.length,
                                      *data_type,
                                      options.projection,
                                      options.value_kind);
}

}  // namespace milvus
