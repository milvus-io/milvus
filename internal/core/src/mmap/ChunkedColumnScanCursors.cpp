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
#include <cstring>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "mmap/ChunkedColumnInterface.h"

namespace milvus {

namespace detail {

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

            std::shared_ptr<void> owner;
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
                out->values.size = rows_to_return;
                out->values.byte_width = span.get().element_sizeof();
                if (span.get().valid_data() != nullptr) {
                    auto dense_owner = std::make_shared<NullableValuesOwner>(
                        span, current_chunk_offset_, rows_to_return);
                    out->values.data = dense_owner->values.data();
                    out->values.offset = 0;
                    owner = std::move(dense_owner);
                } else {
                    out->values.data = span.get().data();
                    out->values.offset = current_chunk_offset_;
                    owner = std::make_shared<PinWrapper<SpanBase>>(span);
                }
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
            if (owner == nullptr) {
                owner = std::make_shared<PinWrapper<SpanBase>>(span);
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
    struct NullableValuesOwner {
        NullableValuesOwner(PinWrapper<SpanBase>& span,
                            int64_t logical_offset,
                            int64_t row_count)
            : span(span), values(row_count * span.get().element_sizeof(), 0) {
            const auto* validity = span.get().valid_data();
            const auto* source = static_cast<const char*>(span.get().data());
            const auto byte_width = span.get().element_sizeof();
            int64_t physical_offset = 0;
            for (int64_t i = 0; i < logical_offset; ++i) {
                physical_offset += validity[i] ? 1 : 0;
            }
            for (int64_t i = 0; i < row_count; ++i) {
                if (!validity[logical_offset + i]) {
                    continue;
                }
                std::memcpy(values.data() + i * byte_width,
                            source + physical_offset * byte_width,
                            byte_width);
                ++physical_offset;
            }
        }

        PinWrapper<SpanBase> span;
        std::vector<char> values;
    };

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
