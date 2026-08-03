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
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "mmap/ChunkedColumnInterface.h"

namespace milvus {

namespace detail {

class PinnedScanInput final {
 public:
    PinnedScanInput(int64_t first_chunk_id,
                    std::vector<PinWrapper<Chunk*>>&& chunks)
        : first_chunk_id_(first_chunk_id), chunks_(std::move(chunks)) {
    }

    Chunk*
    GetChunk(int64_t chunk_id) const {
        const auto index = chunk_id - first_chunk_id_;
        AssertInfo(index >= 0 && index < static_cast<int64_t>(chunks_.size()),
                   "scan chunk {} is outside pinned range [{}, {})",
                   chunk_id,
                   first_chunk_id_,
                   first_chunk_id_ + chunks_.size());
        return chunks_[index].get();
    }

 private:
    int64_t first_chunk_id_;
    std::vector<PinWrapper<Chunk*>> chunks_;
};

std::shared_ptr<PinnedScanInput>
PinScanInput(const ChunkedColumnInterface* column,
             milvus::OpContext* op_ctx,
             int64_t start_offset,
             int64_t length,
             ChunkedColumnInterface::ScanProjection projection) {
    if (length == 0 ||
        (projection == ChunkedColumnInterface::ScanProjection::NoData &&
         !column->IsNullable())) {
        return nullptr;
    }

    const auto first_chunk_id =
        static_cast<int64_t>(column->GetChunkIDByOffset(start_offset).first);
    const auto last_chunk_id = static_cast<int64_t>(
        column->GetChunkIDByOffset(start_offset + length - 1).first);
    std::vector<int64_t> chunk_ids;
    chunk_ids.reserve(last_chunk_id - first_chunk_id + 1);
    for (auto chunk_id = first_chunk_id; chunk_id <= last_chunk_id;
         ++chunk_id) {
        chunk_ids.emplace_back(chunk_id);
    }
    auto chunks = column->PinChunks(op_ctx, chunk_ids);
    AssertInfo(chunks.size() == chunk_ids.size(),
               "scan pinned {} chunks for {} requested chunks",
               chunks.size(),
               chunk_ids.size());
    return std::make_shared<PinnedScanInput>(first_chunk_id, std::move(chunks));
}

class FixedWidthDataScanCursor final
    : public ChunkedColumnInterface::ScanCursor {
 public:
    FixedWidthDataScanCursor(const ChunkedColumnInterface* column,
                             std::shared_ptr<PinnedScanInput> input,
                             int64_t start_offset,
                             int64_t length,
                             DataType data_type,
                             ChunkedColumnInterface::ScanProjection projection,
                             ChunkedColumnInterface::ScanValueKind value_kind)
        : column_(column),
          input_(std::move(input)),
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

    int64_t
    Position() const override {
        return scan_pos_;
    }

    bool
    Next(int64_t max_rows, ChunkedColumnInterface::ScanBatch* out) override {
        AssertInfo(out != nullptr, "data scan output batch is null");
        AssertInfo(max_rows > 0,
                   "data scan max rows must be positive, got {}",
                   max_rows);
        out->values = ChunkedColumnInterface::ValueView{};
        out->validity = nullptr;
        out->owner.reset();
        out->row_id_start = 0;
        out->size = 0;
        if (scan_pos_ >= scan_end_) {
            return false;
        }

        while (scan_pos_ < scan_end_) {
            const auto rows = column_->chunk_row_nums(current_chunk_id_);
            if (current_chunk_offset_ >= rows) {
                ++current_chunk_id_;
                current_chunk_offset_ = 0;
                continue;
            }

            const auto rows_left_in_chunk = rows - current_chunk_offset_;
            const auto rows_left_in_scan = scan_end_ - scan_pos_;
            const auto rows_to_return = std::min<int64_t>(
                {rows_left_in_chunk, rows_left_in_scan, max_rows});

            if (projection_ == ChunkedColumnInterface::ScanProjection::NoData &&
                !column_->IsNullable()) {
                out->row_id_start = scan_pos_;
                out->size = rows_to_return;
                scan_pos_ += rows_to_return;
                current_chunk_offset_ += rows_to_return;
                return true;
            }

            AssertInfo(input_ != nullptr,
                       "fixed-width data scan has no pinned input");
            auto* chunk = input_->GetChunk(current_chunk_id_);
            auto* fixed_chunk = dynamic_cast<FixedWidthChunk*>(chunk);
            AssertInfo(fixed_chunk != nullptr,
                       "scan chunk {} is not fixed-width",
                       current_chunk_id_);
            const auto span = fixed_chunk->Span();
            AssertInfo(span.row_count() == rows,
                       "scan chunk {} row count mismatch, metadata {}, span {}",
                       current_chunk_id_,
                       rows,
                       span.row_count());
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
                out->values.byte_width = span.element_sizeof();
                // Primitive ChunkWriter keeps one payload slot per logical
                // row; validity masks null slots instead of compacting them.
                out->values.data = span.data();
                out->values.offset = current_chunk_offset_;
            }
            if (span.valid_data() != nullptr) {
                out->validity = span.valid_data() + current_chunk_offset_;
            }
            out->owner = input_;
            out->row_id_start = scan_pos_;
            out->size = rows_to_return;

            scan_pos_ += rows_to_return;
            current_chunk_offset_ += rows_to_return;
            return true;
        }
        return false;
    }

 private:
    const ChunkedColumnInterface* column_;
    std::shared_ptr<PinnedScanInput> input_;
    DataType data_type_;
    ChunkedColumnInterface::ScanProjection projection_;
    ChunkedColumnInterface::ScanValueKind value_kind_;
    int64_t scan_pos_;
    int64_t scan_end_;
    int64_t current_chunk_id_{0};
    int64_t current_chunk_offset_{0};
};

class ViewDataScanCursor final : public ChunkedColumnInterface::ScanCursor {
 public:
    ViewDataScanCursor(const ChunkedColumnInterface* column,
                       std::shared_ptr<PinnedScanInput> input,
                       int64_t start_offset,
                       int64_t length,
                       DataType data_type,
                       ChunkedColumnInterface::ScanProjection projection,
                       ChunkedColumnInterface::ScanValueKind value_kind)
        : column_(column),
          input_(std::move(input)),
          data_type_(data_type),
          projection_(projection),
          value_kind_(value_kind),
          scan_pos_(start_offset),
          scan_end_(start_offset + length) {
    }

    int64_t
    Position() const override {
        return scan_pos_;
    }

    bool
    Next(int64_t max_rows, ChunkedColumnInterface::ScanBatch* out) override {
        AssertInfo(out != nullptr, "view data scan output batch is null");
        AssertInfo(max_rows > 0,
                   "view data scan max rows must be positive, got {}",
                   max_rows);
        ResetOutput(out);
        if (scan_pos_ >= scan_end_) {
            return false;
        }

        auto [chunk_id, offset] = column_->GetChunkIDByOffset(scan_pos_);
        const auto chunk_rows = column_->chunk_row_nums(chunk_id);
        auto rows_to_return = std::min<int64_t>(
            chunk_rows - static_cast<int64_t>(offset), scan_end_ - scan_pos_);
        rows_to_return = std::min(rows_to_return, max_rows);
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
            case ChunkedColumnInterface::ScanValueKind::VectorArrayView:
                FillVectorArrayViewBatch(chunk_id, range, out);
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
    using VectorArrayViews =
        std::pair<std::vector<VectorArrayView>, FixedVector<bool>>;

    struct StringOwner {
        StringOwner(std::shared_ptr<PinnedScanInput> input, StringViews&& views)
            : input(std::move(input)), views(std::move(views)) {
        }
        std::shared_ptr<PinnedScanInput> input;
        StringViews views;
    };

    struct JsonOwner {
        JsonOwner(std::shared_ptr<PinnedScanInput> input, StringViews&& views)
            : input(std::move(input)), views(std::move(views)) {
            auto& strings = this->views.first;
            values.reserve(strings.size());
            for (const auto& value : strings) {
                values.emplace_back(Json(value));
            }
        }
        std::shared_ptr<PinnedScanInput> input;
        StringViews views;
        std::vector<Json> values;
    };

    struct ArrayOwner {
        ArrayOwner(std::shared_ptr<PinnedScanInput> input, ArrayViews&& views)
            : input(std::move(input)), views(std::move(views)) {
        }
        std::shared_ptr<PinnedScanInput> input;
        ArrayViews views;
    };

    struct VectorArrayOwner {
        VectorArrayOwner(std::shared_ptr<PinnedScanInput> input,
                         VectorArrayViews&& views)
            : input(std::move(input)), views(std::move(views)) {
        }
        std::shared_ptr<PinnedScanInput> input;
        VectorArrayViews views;
    };

    static void
    ResetOutput(ChunkedColumnInterface::ScanBatch* out) {
        out->values = ChunkedColumnInterface::ValueView{};
        out->validity = nullptr;
        out->owner.reset();
        out->row_id_start = 0;
        out->size = 0;
    }

    void
    FillValidity(const FixedVector<bool>& valid_data,
                 ChunkedColumnInterface::ScanBatch* out) const {
        if (!column_->IsNullable() || valid_data.empty()) {
            return;
        }
        out->validity = valid_data.data();
    }

    void
    FillNoDataBatch(int64_t chunk_id,
                    int64_t offset,
                    ChunkedColumnInterface::ScanBatch* out) const {
        if (!column_->IsNullable()) {
            return;
        }

        AssertInfo(input_ != nullptr, "nullable view scan has no pinned input");
        const auto& valid_data = input_->GetChunk(chunk_id)->Valid();
        if (valid_data.empty()) {
            return;
        }

        out->validity = valid_data.data() + offset;
        out->owner = input_;
    }

    void
    FillStringViewBatch(int64_t chunk_id,
                        std::pair<int64_t, int64_t> range,
                        ChunkedColumnInterface::ScanBatch* out) const {
        AssertInfo(input_ != nullptr, "view scan has no pinned input");
        auto* chunk = dynamic_cast<StringChunk*>(input_->GetChunk(chunk_id));
        AssertInfo(
            chunk != nullptr, "scan chunk {} is not string-like", chunk_id);
        auto owner =
            std::make_shared<StringOwner>(input_, chunk->StringViews(range));
        auto& views = owner->views;
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
        AssertInfo(input_ != nullptr, "view scan has no pinned input");
        auto* chunk = dynamic_cast<StringChunk*>(input_->GetChunk(chunk_id));
        AssertInfo(chunk != nullptr, "scan chunk {} is not JSON", chunk_id);
        auto owner =
            std::make_shared<JsonOwner>(input_, chunk->StringViews(range));
        out->values.encoding = ChunkedColumnInterface::ValueEncoding::JsonView;
        out->values.kind = ChunkedColumnInterface::ScanValueKind::JsonView;
        out->values.physical_type = data_type_;
        out->values.logical_type = DataType::JSON;
        out->values.data = owner->values.data();
        out->values.offset = 0;
        out->values.size = out->size;
        out->values.byte_width = sizeof(Json);
        FillValidity(owner->views.second, out);
        out->owner = std::move(owner);
    }

    void
    FillArrayViewBatch(int64_t chunk_id,
                       std::pair<int64_t, int64_t> range,
                       ChunkedColumnInterface::ScanBatch* out) const {
        AssertInfo(input_ != nullptr, "view scan has no pinned input");
        auto* chunk = dynamic_cast<ArrayChunk*>(input_->GetChunk(chunk_id));
        AssertInfo(chunk != nullptr, "scan chunk {} is not an array", chunk_id);
        auto owner = std::make_shared<ArrayOwner>(input_, chunk->Views(range));
        auto& views = owner->views;
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

    void
    FillVectorArrayViewBatch(int64_t chunk_id,
                             std::pair<int64_t, int64_t> range,
                             ChunkedColumnInterface::ScanBatch* out) const {
        AssertInfo(input_ != nullptr, "view scan has no pinned input");
        auto* chunk =
            dynamic_cast<VectorArrayChunk*>(input_->GetChunk(chunk_id));
        AssertInfo(
            chunk != nullptr, "scan chunk {} is not a vector array", chunk_id);
        auto owner =
            std::make_shared<VectorArrayOwner>(input_, chunk->Views(range));
        auto& views = owner->views;
        out->values.encoding =
            ChunkedColumnInterface::ValueEncoding::VectorArrayView;
        out->values.kind =
            ChunkedColumnInterface::ScanValueKind::VectorArrayView;
        out->values.physical_type = data_type_;
        out->values.logical_type = DataType::VECTOR_ARRAY;
        out->values.data = views.first.data();
        out->values.offset = 0;
        out->values.size = out->size;
        out->values.byte_width = sizeof(VectorArrayView);
        FillValidity(views.second, out);
        out->owner = std::move(owner);
    }

    const ChunkedColumnInterface* column_;
    std::shared_ptr<PinnedScanInput> input_;
    DataType data_type_;
    ChunkedColumnInterface::ScanProjection projection_;
    ChunkedColumnInterface::ScanValueKind value_kind_;
    int64_t scan_pos_;
    int64_t scan_end_;
};

class PreparedDataScan final : public ChunkedColumnInterface::PreparedScan {
 public:
    PreparedDataScan(const ChunkedColumnInterface* column,
                     std::shared_ptr<PinnedScanInput> input,
                     int64_t start_offset,
                     int64_t length,
                     DataType data_type,
                     ChunkedColumnInterface::ScanProjection projection,
                     ChunkedColumnInterface::ScanValueKind value_kind)
        : column_(column),
          input_(std::move(input)),
          start_(start_offset),
          end_(start_offset + length),
          data_type_(data_type),
          projection_(projection),
          value_kind_(value_kind) {
    }

    int64_t
    Start() const override {
        return start_;
    }

    int64_t
    End() const override {
        return end_;
    }

    std::unique_ptr<ChunkedColumnInterface::ScanCursor>
    Seek(int64_t position) const override {
        AssertInfo(position >= start_ && position <= end_,
                   "data scan cursor position {} outside prepared range [{}, {})",
                   position,
                   start_,
                   end_);
        const auto length = end_ - position;
        if (value_kind_ ==
            ChunkedColumnInterface::ScanValueKind::FixedWidth) {
            return std::make_unique<FixedWidthDataScanCursor>(column_,
                                                              input_,
                                                              position,
                                                              length,
                                                              data_type_,
                                                              projection_,
                                                              value_kind_);
        }
        return std::make_unique<ViewDataScanCursor>(column_,
                                                    input_,
                                                    position,
                                                    length,
                                                    data_type_,
                                                    projection_,
                                                    value_kind_);
    }

 private:
    const ChunkedColumnInterface* column_;
    std::shared_ptr<PinnedScanInput> input_;
    int64_t start_;
    int64_t end_;
    DataType data_type_;
    ChunkedColumnInterface::ScanProjection projection_;
    ChunkedColumnInterface::ScanValueKind value_kind_;
};

inline ChunkedColumnInterface::PreparedScanResult
PrepareDataScan(const ChunkedColumnInterface* column,
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
    std::optional<ChunkedColumnInterface::ScanValueKind> column_kind;
    if (ChunkedColumnInterface::IsPrimitiveDataType(data_type)) {
        column_kind = ChunkedColumnInterface::ScanValueKind::FixedWidth;
    } else if (data_type == DataType::JSON) {
        column_kind = ChunkedColumnInterface::ScanValueKind::JsonView;
    } else if (data_type == DataType::STRING ||
               data_type == DataType::VARCHAR || data_type == DataType::TEXT ||
               data_type == DataType::GEOMETRY) {
        column_kind = ChunkedColumnInterface::ScanValueKind::StringView;
    } else if (data_type == DataType::ARRAY) {
        column_kind = ChunkedColumnInterface::ScanValueKind::ArrayView;
    } else if (data_type == DataType::VECTOR_ARRAY) {
        column_kind = ChunkedColumnInterface::ScanValueKind::VectorArrayView;
    }
    if (!column_kind.has_value()) {
        return nullptr;
    }

    // Validity-only scans must not depend on the caller's value type. Choose
    // the cursor from the column's physical type so, for example, a nullable
    // VECTOR_ARRAY null predicate cannot accidentally request a fixed-width
    // cursor through its expression template type.
    const auto resolved_kind =
        projection == ChunkedColumnInterface::ScanProjection::NoData ||
                value_kind == ChunkedColumnInterface::ScanValueKind::Default
            ? *column_kind
            : value_kind;
    AssertInfo(resolved_kind == *column_kind,
               "data scan kind {} does not match column type {}, expected {}",
               static_cast<int>(resolved_kind),
               data_type,
               static_cast<int>(*column_kind));

    if (resolved_kind == ChunkedColumnInterface::ScanValueKind::FixedWidth) {
        if (!ChunkedColumnInterface::IsPrimitiveDataType(data_type)) {
            return nullptr;
        }
        auto input =
            PinScanInput(column, op_ctx, start_offset, length, projection);
        return std::make_shared<PreparedDataScan>(column,
                                                  std::move(input),
                                                  start_offset,
                                                  length,
                                                  data_type,
                                                  projection,
                                                  resolved_kind);
    }

    if (resolved_kind == ChunkedColumnInterface::ScanValueKind::StringView ||
        resolved_kind == ChunkedColumnInterface::ScanValueKind::JsonView ||
        resolved_kind == ChunkedColumnInterface::ScanValueKind::ArrayView ||
        resolved_kind ==
            ChunkedColumnInterface::ScanValueKind::VectorArrayView) {
        auto input =
            PinScanInput(column, op_ctx, start_offset, length, projection);
        return std::make_shared<PreparedDataScan>(column,
                                                  std::move(input),
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
    auto prepared = PrepareScan(op_ctx, options);
    return prepared == nullptr ? nullptr : prepared->Seek(options.start_offset);
}

ChunkedColumnInterface::PreparedScanResult
ChunkedColumnInterface::PrepareScan(milvus::OpContext* op_ctx,
                                    const ScanOptions& options) const {
    auto data_type = GetDefaultScanDataType();
    if (!data_type.has_value()) {
        return nullptr;
    }

    return detail::PrepareDataScan(this,
                                   op_ctx,
                                   options.start_offset,
                                   options.length,
                                   *data_type,
                                   options.projection,
                                   options.value_kind);
}

}  // namespace milvus
