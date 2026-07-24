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
#include <tuple>
#include <utility>
#include <vector>

#include "mmap/ChunkedColumnInterface.h"

namespace milvus {

namespace detail {

void
ValidateScanPlan(const ChunkedColumnInterface::ScanPlan& plan,
                 int64_t prepared_start,
                 int64_t prepared_end) {
    const auto& requested = plan.requested_range;
    AssertInfo(requested.start >= prepared_start &&
                   requested.start <= requested.end &&
                   requested.end <= prepared_end,
               "scan plan range [{}, {}) outside prepared range [{}, {})",
               requested.start,
               requested.end,
               prepared_start,
               prepared_end);
    auto previous_end = requested.start;
    for (const auto& range : plan.skip_ranges) {
        AssertInfo(range.start >= previous_end && range.start < range.end &&
                       range.end <= requested.end,
                   "invalid scan skip range [{}, {}) after {} in [{}, {})",
                   range.start,
                   range.end,
                   previous_end,
                   requested.start,
                   requested.end);
        previous_end = range.end;
    }
}

class PinnedScanInput final {
 public:
    PinnedScanInput(std::vector<int64_t> chunk_ids,
                    std::vector<PinWrapper<Chunk*>>&& chunks)
        : chunk_ids_(std::move(chunk_ids)), chunks_(std::move(chunks)) {
        AssertInfo(chunk_ids_.size() == chunks_.size(),
                   "scan input has {} chunk ids for {} pins",
                   chunk_ids_.size(),
                   chunks_.size());
    }

    Chunk*
    GetChunk(int64_t chunk_id) const {
        const auto it =
            std::lower_bound(chunk_ids_.begin(), chunk_ids_.end(), chunk_id);
        AssertInfo(it != chunk_ids_.end() && *it == chunk_id,
                   "scan chunk {} is not pinned",
                   chunk_id);
        return chunks_[std::distance(chunk_ids_.begin(), it)].get();
    }

 private:
    std::vector<int64_t> chunk_ids_;
    std::vector<PinWrapper<Chunk*>> chunks_;
};

std::shared_ptr<PinnedScanInput>
PinScanInput(const ChunkedColumnInterface* column,
             milvus::OpContext* op_ctx,
             const std::vector<int64_t>& chunk_ids,
             ChunkedColumnInterface::ScanProjection projection) {
    if (chunk_ids.empty() ||
        (projection == ChunkedColumnInterface::ScanProjection::NoData &&
         !column->IsNullable())) {
        return nullptr;
    }
    auto chunks = column->PinChunks(op_ctx, chunk_ids);
    AssertInfo(chunks.size() == chunk_ids.size(),
               "scan pinned {} chunks for {} requested chunks",
               chunks.size(),
               chunk_ids.size());
    return std::make_shared<PinnedScanInput>(chunk_ids, std::move(chunks));
}

std::vector<int64_t>
GetScanChunkIds(const ChunkedColumnInterface* column,
                int64_t start_offset,
                int64_t length) {
    std::vector<int64_t> chunk_ids;
    if (length == 0) {
        return chunk_ids;
    }
    const auto first_chunk_id =
        static_cast<int64_t>(column->GetChunkIDByOffset(start_offset).first);
    const auto last_chunk_id = static_cast<int64_t>(
        column->GetChunkIDByOffset(start_offset + length - 1).first);
    chunk_ids.reserve(last_chunk_id - first_chunk_id + 1);
    for (auto chunk_id = first_chunk_id; chunk_id <= last_chunk_id;
         ++chunk_id) {
        chunk_ids.emplace_back(chunk_id);
    }
    return chunk_ids;
}

std::vector<ChunkedColumnInterface::ScanRowRange>
SkippedCellsToRanges(const ChunkedColumnInterface* column,
                     int64_t start_offset,
                     int64_t length,
                     std::vector<int64_t> skipped_cell_ids) {
    std::sort(skipped_cell_ids.begin(), skipped_cell_ids.end());
    skipped_cell_ids.erase(
        std::unique(skipped_cell_ids.begin(), skipped_cell_ids.end()),
        skipped_cell_ids.end());
    std::vector<ChunkedColumnInterface::ScanRowRange> ranges;
    const auto scan_end = start_offset + length;
    for (const auto cell_id : skipped_cell_ids) {
        const auto cell_start = column->GetNumRowsUntilChunk(cell_id);
        const auto cell_end = cell_start + column->chunk_row_nums(cell_id);
        const auto range_start = std::max(start_offset, cell_start);
        const auto range_end = std::min(scan_end, cell_end);
        if (range_start >= range_end) {
            continue;
        }
        if (!ranges.empty() && ranges.back().end == range_start) {
            ranges.back().end = range_end;
        } else {
            ranges.emplace_back(
                ChunkedColumnInterface::ScanRowRange{range_start, range_end});
        }
    }
    return ranges;
}

class FixedWidthDataScanCursor final
    : public ChunkedColumnInterface::ScanCursor {
 public:
    FixedWidthDataScanCursor(
        const ChunkedColumnInterface* column,
        std::shared_ptr<PinnedScanInput> input,
        int64_t start_offset,
        int64_t length,
        DataType data_type,
        ChunkedColumnInterface::ScanProjection projection,
        ChunkedColumnInterface::ScanValueKind value_kind,
        std::vector<ChunkedColumnInterface::ScanRowRange> skip_ranges)
        : column_(column),
          input_(std::move(input)),
          data_type_(data_type),
          projection_(projection),
          value_kind_(value_kind),
          scan_pos_(start_offset),
          scan_end_(start_offset + length),
          skip_ranges_(std::move(skip_ranges)) {
        if (start_offset < scan_end_) {
            SetChunkPosition(start_offset);
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
        out->row_ids.clear();
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
            if (projection_ == ChunkedColumnInterface::ScanProjection::Data &&
                IsInSkippedRange()) {
                if (column_->IsNullable()) {
                    return FillSkippedValidityBatch(max_rows, out);
                }
                AdvancePastSkippedRanges();
            }
            if (scan_pos_ >= scan_end_) {
                return false;
            }
            const auto rows_left_in_chunk = rows - current_chunk_offset_;
            const auto rows_left_in_scan = scan_end_ - scan_pos_;
            const auto rows_before_skip =
                projection_ == ChunkedColumnInterface::ScanProjection::Data &&
                        skip_index_ < skip_ranges_.size()
                    ? skip_ranges_[skip_index_].start - scan_pos_
                    : rows_left_in_scan;
            const auto rows_to_return = std::min<int64_t>({rows_left_in_chunk,
                                                           rows_left_in_scan,
                                                           rows_before_skip,
                                                           max_rows});
            AssertInfo(rows_to_return > 0,
                       "invalid fixed-width scan batch at row {}",
                       scan_pos_);

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
    bool
    IsInSkippedRange() {
        while (skip_index_ < skip_ranges_.size() &&
               skip_ranges_[skip_index_].end <= scan_pos_) {
            ++skip_index_;
        }
        return skip_index_ < skip_ranges_.size() &&
               skip_ranges_[skip_index_].start <= scan_pos_;
    }

    bool
    FillSkippedValidityBatch(int64_t max_rows,
                             ChunkedColumnInterface::ScanBatch* out) {
        AssertInfo(input_ != nullptr,
                   "nullable skipped scan has no pinned input");
        const auto& skipped = skip_ranges_[skip_index_];
        const auto rows = column_->chunk_row_nums(current_chunk_id_);
        const auto rows_to_return =
            std::min<int64_t>({max_rows,
                               skipped.end - scan_pos_,
                               rows - current_chunk_offset_,
                               scan_end_ - scan_pos_});
        AssertInfo(rows_to_return > 0,
                   "invalid skipped validity batch at row {}",
                   scan_pos_);
        auto* chunk = input_->GetChunk(current_chunk_id_);
        auto* fixed_chunk = dynamic_cast<FixedWidthChunk*>(chunk);
        AssertInfo(fixed_chunk != nullptr,
                   "scan chunk {} is not fixed-width",
                   current_chunk_id_);
        const auto span = fixed_chunk->Span();
        AssertInfo(span.valid_data() != nullptr,
                   "nullable scan chunk {} has no validity",
                   current_chunk_id_);
        out->validity = span.valid_data() + current_chunk_offset_;
        out->owner = input_;
        out->row_id_start = scan_pos_;
        out->size = rows_to_return;
        scan_pos_ += rows_to_return;
        current_chunk_offset_ += rows_to_return;
        return true;
    }

    void
    SetChunkPosition(int64_t position) {
        auto [chunk_id, offset] = column_->GetChunkIDByOffset(position);
        current_chunk_id_ = static_cast<int64_t>(chunk_id);
        current_chunk_offset_ = static_cast<int64_t>(offset);
    }

    void
    AdvancePastSkippedRanges() {
        bool advanced = false;
        while (skip_index_ < skip_ranges_.size()) {
            const auto& range = skip_ranges_[skip_index_];
            if (range.end <= scan_pos_) {
                ++skip_index_;
                continue;
            }
            if (range.start > scan_pos_) {
                break;
            }
            scan_pos_ = range.end;
            ++skip_index_;
            advanced = true;
        }
        if (advanced && scan_pos_ < scan_end_) {
            SetChunkPosition(scan_pos_);
        }
    }

    const ChunkedColumnInterface* column_;
    std::shared_ptr<PinnedScanInput> input_;
    DataType data_type_;
    ChunkedColumnInterface::ScanProjection projection_;
    ChunkedColumnInterface::ScanValueKind value_kind_;
    int64_t scan_pos_;
    int64_t scan_end_;
    std::vector<ChunkedColumnInterface::ScanRowRange> skip_ranges_;
    size_t skip_index_{0};
    int64_t current_chunk_id_{0};
    int64_t current_chunk_offset_{0};
};

class ViewDataScanCursor final : public ChunkedColumnInterface::ScanCursor {
 public:
    ViewDataScanCursor(
        const ChunkedColumnInterface* column,
        std::shared_ptr<PinnedScanInput> input,
        int64_t start_offset,
        int64_t length,
        DataType data_type,
        ChunkedColumnInterface::ScanProjection projection,
        ChunkedColumnInterface::ScanValueKind value_kind,
        std::vector<ChunkedColumnInterface::ScanRowRange> skip_ranges)
        : column_(column),
          input_(std::move(input)),
          data_type_(data_type),
          projection_(projection),
          value_kind_(value_kind),
          scan_pos_(start_offset),
          scan_end_(start_offset + length),
          skip_ranges_(std::move(skip_ranges)) {
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

        if (projection_ == ChunkedColumnInterface::ScanProjection::Data &&
            IsInSkippedRange()) {
            if (column_->IsNullable()) {
                return FillSkippedValidityBatch(max_rows, out);
            }
            AdvancePastSkippedRanges();
        }
        if (scan_pos_ >= scan_end_) {
            return false;
        }

        auto [chunk_id, offset] = column_->GetChunkIDByOffset(scan_pos_);
        const auto chunk_rows = column_->chunk_row_nums(chunk_id);
        auto rows_to_return = std::min<int64_t>(
            chunk_rows - static_cast<int64_t>(offset), scan_end_ - scan_pos_);
        if (projection_ == ChunkedColumnInterface::ScanProjection::Data &&
            skip_index_ < skip_ranges_.size()) {
            rows_to_return = std::min(
                rows_to_return, skip_ranges_[skip_index_].start - scan_pos_);
        }
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
        out->row_ids.clear();
        out->owner.reset();
        out->row_id_start = 0;
        out->size = 0;
    }

    bool
    IsInSkippedRange() {
        while (skip_index_ < skip_ranges_.size() &&
               skip_ranges_[skip_index_].end <= scan_pos_) {
            ++skip_index_;
        }
        return skip_index_ < skip_ranges_.size() &&
               skip_ranges_[skip_index_].start <= scan_pos_;
    }

    bool
    FillSkippedValidityBatch(int64_t max_rows,
                             ChunkedColumnInterface::ScanBatch* out) {
        AssertInfo(input_ != nullptr,
                   "nullable skipped view scan has no pinned input");
        const auto& skipped = skip_ranges_[skip_index_];
        auto [chunk_id, offset] = column_->GetChunkIDByOffset(scan_pos_);
        const auto chunk_rows = column_->chunk_row_nums(chunk_id);
        const auto rows_to_return =
            std::min<int64_t>({max_rows,
                               skipped.end - scan_pos_,
                               chunk_rows - static_cast<int64_t>(offset),
                               scan_end_ - scan_pos_});
        AssertInfo(rows_to_return > 0,
                   "invalid skipped view validity batch at row {}",
                   scan_pos_);
        const auto& valid_data = input_->GetChunk(chunk_id)->Valid();
        AssertInfo(!valid_data.empty(),
                   "nullable scan chunk {} has no validity",
                   chunk_id);
        out->validity = valid_data.data() + offset;
        out->owner = input_;
        out->row_id_start = scan_pos_;
        out->size = rows_to_return;
        scan_pos_ += rows_to_return;
        return true;
    }

    void
    AdvancePastSkippedRanges() {
        while (skip_index_ < skip_ranges_.size()) {
            const auto& range = skip_ranges_[skip_index_];
            if (range.end <= scan_pos_) {
                ++skip_index_;
                continue;
            }
            if (range.start > scan_pos_) {
                break;
            }
            scan_pos_ = range.end;
            ++skip_index_;
        }
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
    std::vector<ChunkedColumnInterface::ScanRowRange> skip_ranges_;
    size_t skip_index_{0};
};

class PreparedDataScan final : public ChunkedColumnInterface::PreparedScan {
 public:
    PreparedDataScan(const ChunkedColumnInterface* column,
                     std::shared_ptr<PinnedScanInput> input,
                     int64_t start_offset,
                     int64_t length,
                     DataType data_type,
                     ChunkedColumnInterface::ScanProjection projection,
                     ChunkedColumnInterface::ScanValueKind value_kind,
                     std::vector<int64_t> skipped_cell_ids)
        : column_(column),
          input_(std::move(input)),
          start_(start_offset),
          end_(start_offset + length),
          plan_(ChunkedColumnInterface::ScanPlan::Full(start_offset, length)),
          data_type_(data_type),
          projection_(projection),
          value_kind_(value_kind) {
        plan_.skip_ranges = SkippedCellsToRanges(
            column_, start_offset, length, std::move(skipped_cell_ids));
    }

    int64_t
    Start() const override {
        return start_;
    }

    int64_t
    End() const override {
        return end_;
    }

    const ChunkedColumnInterface::ScanPlan&
    Plan() const override {
        return plan_;
    }

    std::unique_ptr<ChunkedColumnInterface::ScanCursor>
    Open(const ChunkedColumnInterface::ScanPlan& plan,
         ChunkedColumnInterface::ScanProjection projection) const override {
        ValidateScanPlan(plan, start_, end_);
        AssertInfo(
            projection_ == ChunkedColumnInterface::ScanProjection::Data ||
                projection == ChunkedColumnInterface::ScanProjection::NoData,
            "validity-only prepared scan cannot open a data cursor");
        auto effective_plan = plan;
        if (projection == ChunkedColumnInterface::ScanProjection::Data) {
            // Planner-pruned Cells may not be present in input_. Reopening a
            // cursor for a subrange must retain those skips automatically;
            // callers should only describe the logical range (and any
            // additional skips), not repeat backend pin-selection state.
            for (const auto& prepared_skip : plan_.skip_ranges) {
                const auto start = std::max(prepared_skip.start,
                                            plan.requested_range.start);
                const auto end =
                    std::min(prepared_skip.end, plan.requested_range.end);
                if (start < end) {
                    effective_plan.skip_ranges.emplace_back(
                        ChunkedColumnInterface::ScanRowRange{start, end});
                }
            }
            std::sort(effective_plan.skip_ranges.begin(),
                      effective_plan.skip_ranges.end(),
                      [](const auto& left, const auto& right) {
                          return std::tie(left.start, left.end) <
                                 std::tie(right.start, right.end);
                      });
            std::vector<ChunkedColumnInterface::ScanRowRange> merged;
            for (const auto& skip : effective_plan.skip_ranges) {
                if (!merged.empty() && skip.start <= merged.back().end) {
                    merged.back().end = std::max(merged.back().end, skip.end);
                } else {
                    merged.emplace_back(skip);
                }
            }
            effective_plan.skip_ranges = std::move(merged);
            ValidateScanPlan(effective_plan, start_, end_);
        }
        const auto start = effective_plan.requested_range.start;
        const auto length = effective_plan.requested_range.end - start;
        if (value_kind_ == ChunkedColumnInterface::ScanValueKind::FixedWidth) {
            return std::make_unique<FixedWidthDataScanCursor>(column_,
                                                              input_,
                                                              start,
                                                              length,
                                                              data_type_,
                                                              projection,
                                                              value_kind_,
                                                              effective_plan
                                                                  .skip_ranges);
        }
        return std::make_unique<ViewDataScanCursor>(column_,
                                                    input_,
                                                    start,
                                                    length,
                                                    data_type_,
                                                    projection,
                                                    value_kind_,
                                                    effective_plan.skip_ranges);
    }

 private:
    const ChunkedColumnInterface* column_;
    std::shared_ptr<PinnedScanInput> input_;
    int64_t start_;
    int64_t end_;
    ChunkedColumnInterface::ScanPlan plan_;
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
                ChunkedColumnInterface::ScanValueKind value_kind,
                const ChunkedColumnInterface::ScanOptions::CellSkipPredicate&
                    metadata_skip_cell,
                const ChunkedColumnInterface::ScanOptions::CellSkipPredicate&
                    loaded_skip_cell) {
    AssertInfo(
        start_offset >= 0 && length >= 0 &&
            start_offset + length <= static_cast<int64_t>(column->NumRows()),
        "data scan range [{}, {}) out of rows {}",
        start_offset,
        start_offset + length,
        column->NumRows());
    const auto column_kind = GetScanValueKindForDataType(data_type);
    if (!column_kind.has_value()) {
        return nullptr;
    }

    // Validity-only scans must not depend on the caller's value type. Choose
    // the cursor from the column's physical type so, for example, a nullable
    // VECTOR_ARRAY null predicate cannot accidentally request a fixed-width
    // cursor through its expression template type.
    const auto resolved_kind =
        ResolveDataScanValueKind(data_type, projection, value_kind);

    const auto scan_chunk_ids = GetScanChunkIds(column, start_offset, length);
    std::vector<int64_t> skipped_cell_ids;
    std::vector<int64_t> loaded_cell_ids;
    skipped_cell_ids.reserve(scan_chunk_ids.size());
    loaded_cell_ids.reserve(scan_chunk_ids.size());
    for (const auto cell_id : scan_chunk_ids) {
        if (metadata_skip_cell && metadata_skip_cell(cell_id)) {
            skipped_cell_ids.emplace_back(cell_id);
        } else {
            loaded_cell_ids.emplace_back(cell_id);
        }
    }
    const auto& pinned_cell_ids =
        column->IsNullable() ? scan_chunk_ids : loaded_cell_ids;
    auto input = PinScanInput(column, op_ctx, pinned_cell_ids, projection);
    if (loaded_skip_cell) {
        for (const auto cell_id : loaded_cell_ids) {
            if (loaded_skip_cell(cell_id)) {
                skipped_cell_ids.emplace_back(cell_id);
            }
        }
    }

    if (resolved_kind == ChunkedColumnInterface::ScanValueKind::FixedWidth) {
        if (!ChunkedColumnInterface::IsPrimitiveDataType(data_type)) {
            return nullptr;
        }
        return std::make_shared<PreparedDataScan>(column,
                                                  std::move(input),
                                                  start_offset,
                                                  length,
                                                  data_type,
                                                  projection,
                                                  resolved_kind,
                                                  std::move(skipped_cell_ids));
    }

    if (resolved_kind == ChunkedColumnInterface::ScanValueKind::StringView ||
        resolved_kind == ChunkedColumnInterface::ScanValueKind::JsonView ||
        resolved_kind == ChunkedColumnInterface::ScanValueKind::ArrayView ||
        resolved_kind ==
            ChunkedColumnInterface::ScanValueKind::VectorArrayView) {
        return std::make_shared<PreparedDataScan>(column,
                                                  std::move(input),
                                                  start_offset,
                                                  length,
                                                  data_type,
                                                  projection,
                                                  resolved_kind,
                                                  std::move(skipped_cell_ids));
    }

    return nullptr;
}

}  // namespace detail

ChunkedColumnInterface::ScanResult
ChunkedColumnInterface::Scan(milvus::OpContext* op_ctx,
                             const ScanOptions& options) const {
    auto prepared = PrepareScan(op_ctx, options);
    return prepared == nullptr
               ? nullptr
               : prepared->Open(prepared->Plan(), options.projection);
}

ChunkedColumnInterface::PreparedScanResult
ChunkedColumnInterface::PrepareScan(milvus::OpContext* op_ctx,
                                    const ScanOptions& options) const {
    auto data_type = GetDefaultScanDataType();
    if (!data_type.has_value()) {
        return nullptr;
    }

    if (options.output != ScanOutput::Data ||
        options.predicate != ScanPredicate::None) {
        return nullptr;
    }

    return detail::PrepareDataScan(this,
                                   op_ctx,
                                   options.start_offset,
                                   options.length,
                                   *data_type,
                                   options.projection,
                                   options.value_kind,
                                   options.metadata_skip_cell,
                                   options.loaded_skip_cell);
}

}  // namespace milvus
