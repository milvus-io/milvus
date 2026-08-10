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
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include "mmap/ChunkedColumnFilter.h"
#include "mmap/ChunkedColumnInterface.h"

namespace milvus {
namespace detail {

class PinnedScanInput final {
 public:
    PinnedScanInput(int64_t chunk_id, PinWrapper<Chunk*>&& chunk)
        : chunk_id_(chunk_id), chunk_(std::move(chunk)) {
    }

    Chunk*
    GetChunk(int64_t chunk_id) const {
        AssertInfo(chunk_id == chunk_id_,
                   "scan chunk {} is not pinned, current chunk {}",
                   chunk_id,
                   chunk_id_);
        return chunk_.get();
    }

 private:
    int64_t chunk_id_;
    PinWrapper<Chunk*> chunk_;
};

using StringViews = std::pair<std::vector<std::string_view>, ValidityView>;
using ArrayViews = std::pair<std::vector<ArrayView>, ValidityView>;
using ArrayValueViews = std::pair<std::vector<ArrayValueView>, ValidityView>;
using VectorArrayViews = std::pair<std::vector<VectorArrayView>, ValidityView>;

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
        values.reserve(this->views.first.size());
        for (const auto& value : this->views.first) {
            values.emplace_back(value);
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

struct ArrayValueOwner {
    ArrayValueOwner(std::shared_ptr<PinnedScanInput> input,
                    ArrayValueViews&& views)
        : input(std::move(input)), views(std::move(views)) {
    }

    std::shared_ptr<PinnedScanInput> input;
    ArrayValueViews views;
};

struct VectorArrayOwner {
    VectorArrayOwner(std::shared_ptr<PinnedScanInput> input,
                     VectorArrayViews&& views)
        : input(std::move(input)), views(std::move(views)) {
    }

    std::shared_ptr<PinnedScanInput> input;
    VectorArrayViews views;
};

class RawScanCursor final : public ChunkedColumnInterface::ScanCursor {
 public:
    RawScanCursor(const ChunkedColumnInterface* column,
                  const ColumnPlanner* planner,
                  milvus::OpContext* op_ctx,
                  int64_t start_offset,
                  ChunkedColumnInterface::TargetType target_type,
                  ChunkedColumnInterface::ScanPinPolicy pin_policy,
                  bool prefetch,
                  ColumnFilterPtr filter)
        : column_(column),
          planner_(planner),
          op_ctx_(op_ctx),
          target_type_(target_type),
          pin_policy_(pin_policy),
          filter_(std::move(filter)),
          scan_pos_(start_offset) {
        AssertInfo(column_ != nullptr, "raw scan column is null");
        AssertInfo(planner_ != nullptr, "raw scan planner is null");
        AssertInfo(start_offset >= 0 && start_offset <= planner_->NumRows(),
                   "data scan start {} out of rows {}",
                   start_offset,
                   planner_->NumRows());
        SetPhysicalPosition(start_offset);
        if (prefetch) {
            if (filter_ != nullptr &&
                filter_->Source() ==
                    ColumnFilter::MetricsSource::PreloadedStatistics) {
                prefetched_skip_decisions_.assign(planner_->NumCells(), -1);
            }
            PrefetchRemainingCells();
        }
    }

    int64_t
    Position() const override {
        return scan_pos_;
    }

    void
    Seek(int64_t position) override {
        AssertInfo(position >= scan_pos_,
                   "raw scan cannot seek backward from {} to {}",
                   scan_pos_,
                   position);
        AssertInfo(position <= planner_->NumRows(),
                   "raw scan seek {} out of rows {}",
                   position,
                   planner_->NumRows());
        if (position == scan_pos_) {
            return;
        }
        SetPhysicalPosition(position);
        if (cursor_input_ != nullptr && cursor_chunk_id_ != current_chunk_id_) {
            ReleaseCursorInput();
        }
    }

    bool
    Next(int64_t length,
         ChunkedColumnInterface::ScanReadMode read_mode,
         ChunkedColumnInterface::ScanBatch* out) override {
        AssertInfo(out != nullptr, "raw scan output batch is null");
        ResetOutput(out);
        AssertInfo(
            length >= 0, "raw scan length {} must be non-negative", length);
        if (length == 0 || scan_pos_ == planner_->NumRows()) {
            ReleaseCursorInput();
            return false;
        }
        length = std::min(length, planner_->NumRows() - scan_pos_);
        AssertInfo(
            read_mode != ChunkedColumnInterface::ScanReadMode::ValidityOnly ||
                column_->IsNullable(),
            "validity-only scan requested for non-nullable column");

        const auto chunk_id = current_chunk_id_;
        const auto chunk_rows = planner_->CellRows(chunk_id);
        const auto size =
            std::min<int64_t>(length, chunk_rows - current_chunk_offset_);
        AssertInfo(size > 0,
                   "raw scan made no progress in chunk {} at offset {}",
                   chunk_id,
                   current_chunk_offset_);

        out->row_id_start = scan_pos_;
        out->size = size;

        if (read_mode == ChunkedColumnInterface::ScanReadMode::ValidityOnly) {
            auto input = PinChunk(chunk_id);
            auto* chunk = input->GetChunk(chunk_id);
            FillValidityOnly(input, chunk, out);
            Advance(size);
            return true;
        }

        const auto preloaded_skip =
            filter_ != nullptr &&
            filter_->Source() ==
                ColumnFilter::MetricsSource::PreloadedStatistics;
        auto data_skipped = preloaded_skip && ShouldSkipCell(chunk_id);
        if (data_skipped && !column_->IsNullable()) {
            // Next() invalidates the previous batch. A CursorOwned pin from
            // that batch must not survive merely because this Cell needs no
            // data pin of its own.
            ReleaseCursorInput();
            out->data_skipped = true;
            Advance(size);
            return true;
        }

        auto input = PinChunk(chunk_id);
        auto* chunk = input->GetChunk(chunk_id);
        if (!data_skipped && filter_ != nullptr &&
            filter_->Source() == ColumnFilter::MetricsSource::LoadedPayload) {
            data_skipped = ShouldSkipCell(chunk_id);
        }

        if (data_skipped) {
            out->data_skipped = true;
            if (column_->IsNullable()) {
                FillValidityOnly(input, chunk, out);
            }
            Advance(size);
            return true;
        }

        if (IsFixedWidthTargetType(target_type_)) {
            FillFixedWidth(input, chunk, out);
        } else {
            FillView(input, chunk, out);
        }

        Advance(size);
        return true;
    }

 private:
    static void
    ResetOutput(ChunkedColumnInterface::ScanBatch* out) {
        out->values = ChunkedColumnInterface::ValueView{};
        out->validity = {};
        out->row_ids.clear();
        out->owner.reset();
        out->row_id_start = 0;
        out->size = 0;
        out->data_skipped = false;
    }

    void
    SetPhysicalPosition(int64_t position) {
        scan_pos_ = position;
        if (position == planner_->NumRows()) {
            current_chunk_id_ = planner_->NumCells();
            current_chunk_offset_ = 0;
            return;
        }
        const auto location = planner_->Locate(position);
        current_chunk_id_ = location.cell_id;
        current_chunk_offset_ = location.cell_offset;
    }

    std::shared_ptr<PinnedScanInput>
    PinChunk(int64_t chunk_id) {
        if (pin_policy_ == ChunkedColumnInterface::ScanPinPolicy::CursorOwned) {
            if (cursor_input_ != nullptr) {
                if (cursor_chunk_id_ == chunk_id) {
                    return cursor_input_;
                }
                // Release the previous Cell before pinning the next one. This
                // keeps CursorOwned at one physical Cell pin throughout Scan.
                ReleaseCursorInput();
            }
            cursor_input_ = std::make_shared<PinnedScanInput>(
                chunk_id, column_->GetChunk(op_ctx_, chunk_id));
            cursor_chunk_id_ = chunk_id;
            return cursor_input_;
        }
        return std::make_shared<PinnedScanInput>(
            chunk_id, column_->GetChunk(op_ctx_, chunk_id));
    }

    std::shared_ptr<PinnedScanInput>
    BatchPin(const std::shared_ptr<PinnedScanInput>& input) const {
        return pin_policy_ == ChunkedColumnInterface::ScanPinPolicy::ResultOwned
                   ? input
                   : nullptr;
    }

    void
    ReleaseCursorInput() {
        cursor_input_.reset();
        cursor_chunk_id_ = -1;
    }

    void
    PrefetchRemainingCells() {
        // Batch-warm every remaining Cell once, matching the legacy
        // multi-chunk path that submitted all remaining chunk ids to
        // prefetch_chunks() before evaluation. PrefetchChunks loads in
        // parallel and drops the pins immediately, so per-batch reads still
        // pin the current Cell but hit the warmed cache.
        if (current_chunk_id_ >= planner_->NumCells()) {
            return;
        }
        std::vector<int64_t> remaining;
        remaining.reserve(planner_->NumCells() - current_chunk_id_);
        for (int64_t cell_id = current_chunk_id_;
             cell_id < planner_->NumCells();
             ++cell_id) {
            if (planner_->CellRows(cell_id) == 0) {
                continue;
            }
            const auto data_skipped =
                filter_ != nullptr &&
                filter_->Source() ==
                    ColumnFilter::MetricsSource::PreloadedStatistics &&
                ShouldSkipCell(cell_id);
            // A nullable skipped Cell is still needed for its real validity.
            if (data_skipped && !column_->IsNullable()) {
                continue;
            }
            remaining.push_back(cell_id);
        }
        if (remaining.empty()) {
            return;
        }
        column_->PrefetchChunks(op_ctx_, remaining);
    }

    void
    FillValidityOnly(const std::shared_ptr<PinnedScanInput>& input,
                     Chunk* chunk,
                     ChunkedColumnInterface::ScanBatch* out) const {
        out->validity = chunk->Validity(current_chunk_offset_);
        out->owner = BatchPin(input);
    }

    void
    FillFixedWidth(const std::shared_ptr<PinnedScanInput>& input,
                   Chunk* chunk,
                   ChunkedColumnInterface::ScanBatch* out) const {
        auto* fixed_chunk = dynamic_cast<FixedWidthChunk*>(chunk);
        AssertInfo(fixed_chunk != nullptr,
                   "scan chunk {} is not fixed-width",
                   current_chunk_id_);
        const auto span = fixed_chunk->Span();
        AssertInfo(span.row_count() == planner_->CellRows(current_chunk_id_),
                   "scan chunk {} row count mismatch, metadata {}, span {}",
                   current_chunk_id_,
                   planner_->CellRows(current_chunk_id_),
                   span.row_count());
        out->values.target_type = target_type_;
        out->values.data = span.data();
        out->values.offset = current_chunk_offset_;
        out->values.byte_width = span.element_sizeof();
        out->validity = span.validity().Subview(current_chunk_offset_);
        out->owner = BatchPin(input);
    }

    void
    FillView(const std::shared_ptr<PinnedScanInput>& input,
             Chunk* chunk,
             ChunkedColumnInterface::ScanBatch* out) const {
        const auto range = std::make_pair(current_chunk_offset_, out->size);
        switch (target_type_) {
            case ChunkedColumnInterface::TargetType::StringView: {
                auto* string_chunk = dynamic_cast<StringChunk*>(chunk);
                AssertInfo(string_chunk != nullptr,
                           "scan chunk {} is not string-like",
                           current_chunk_id_);
                auto owner = std::make_shared<StringOwner>(
                    BatchPin(input), string_chunk->StringViews(range));
                out->values.target_type = target_type_;
                out->values.data = owner->views.first.data();
                out->values.offset = 0;
                out->values.byte_width = sizeof(std::string_view);
                FillViewValidity(owner->views.second, out);
                out->owner = std::move(owner);
                return;
            }
            case ChunkedColumnInterface::TargetType::Json: {
                auto* string_chunk = dynamic_cast<StringChunk*>(chunk);
                AssertInfo(string_chunk != nullptr,
                           "scan chunk {} is not JSON",
                           current_chunk_id_);
                auto owner = std::make_shared<JsonOwner>(
                    BatchPin(input), string_chunk->StringViews(range));
                out->values.target_type = target_type_;
                out->values.data = owner->values.data();
                out->values.offset = 0;
                out->values.byte_width = sizeof(Json);
                FillViewValidity(owner->views.second, out);
                out->owner = std::move(owner);
                return;
            }
            case ChunkedColumnInterface::TargetType::ArrayView: {
                auto* array_chunk = dynamic_cast<ArrayChunk*>(chunk);
                AssertInfo(array_chunk != nullptr,
                           "scan chunk {} is not an array",
                           current_chunk_id_);
                auto owner = std::make_shared<ArrayOwner>(
                    BatchPin(input), array_chunk->Views(range));
                out->values.target_type = target_type_;
                out->values.data = owner->views.first.data();
                out->values.offset = 0;
                out->values.byte_width = sizeof(ArrayView);
                FillViewValidity(owner->views.second, out);
                out->owner = std::move(owner);
                return;
            }
            case ChunkedColumnInterface::TargetType::ArrayValueView: {
                auto* array_chunk = dynamic_cast<ColumnarArrayChunk*>(chunk);
                AssertInfo(array_chunk != nullptr,
                           "scan chunk {} is not a recursive array",
                           current_chunk_id_);
                auto owner = std::make_shared<ArrayValueOwner>(
                    BatchPin(input), array_chunk->Views<ArrayValueView>(range));
                out->values.target_type = target_type_;
                out->values.data = owner->views.first.data();
                out->values.offset = 0;
                out->values.byte_width = sizeof(ArrayValueView);
                FillViewValidity(owner->views.second, out);
                out->owner = std::move(owner);
                return;
            }
            case ChunkedColumnInterface::TargetType::VectorArrayView: {
                auto* array_chunk = dynamic_cast<VectorArrayChunk*>(chunk);
                AssertInfo(array_chunk != nullptr,
                           "scan chunk {} is not a vector array",
                           current_chunk_id_);
                auto owner = std::make_shared<VectorArrayOwner>(
                    BatchPin(input), array_chunk->Views(range));
                out->values.target_type = target_type_;
                out->values.data = owner->views.first.data();
                out->values.offset = 0;
                out->values.byte_width = sizeof(VectorArrayView);
                FillViewValidity(owner->views.second, out);
                out->owner = std::move(owner);
                return;
            }
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "unsupported raw scan target type {}",
                          static_cast<int>(target_type_));
        }
    }

    void
    FillViewValidity(ValidityView validity,
                     ChunkedColumnInterface::ScanBatch* out) const {
        if (column_->IsNullable()) {
            out->validity = validity;
        }
    }

    void
    Advance(int64_t size) {
        scan_pos_ += size;
        current_chunk_offset_ += size;
        if (scan_pos_ == planner_->NumRows()) {
            current_chunk_id_ = planner_->NumCells();
            current_chunk_offset_ = 0;
            return;
        }

        if (current_chunk_offset_ == planner_->CellRows(current_chunk_id_)) {
            ++current_chunk_id_;
            current_chunk_offset_ = 0;
            while (current_chunk_id_ < planner_->NumCells() &&
                   planner_->CellRows(current_chunk_id_) == 0) {
                ++current_chunk_id_;
            }
        }
    }

    bool
    ShouldSkipCell(int64_t cell_id) {
        AssertInfo(
            filter_ != nullptr, "raw scan has no filter for Cell {}", cell_id);
        if (!prefetched_skip_decisions_.empty()) {
            auto& decision = prefetched_skip_decisions_[cell_id];
            if (decision < 0) {
                decision = filter_->CanSkipPhysicalCell(cell_id) ? 1 : 0;
            }
            return decision != 0;
        }
        if (filter_cell_id_ != cell_id) {
            filter_cell_id_ = cell_id;
            filter_cell_skipped_ = filter_->CanSkipPhysicalCell(cell_id);
        }
        return filter_cell_skipped_;
    }

    const ChunkedColumnInterface* column_;
    const ColumnPlanner* planner_{nullptr};
    milvus::OpContext* op_ctx_;
    ChunkedColumnInterface::TargetType target_type_;
    ChunkedColumnInterface::ScanPinPolicy pin_policy_;
    ColumnFilterPtr filter_;
    int64_t scan_pos_{0};
    int64_t current_chunk_id_{0};
    int64_t current_chunk_offset_{0};
    std::shared_ptr<PinnedScanInput> cursor_input_;
    int64_t cursor_chunk_id_{-1};
    int64_t filter_cell_id_{-1};
    bool filter_cell_skipped_{false};
    std::vector<int8_t> prefetched_skip_decisions_;
};

inline ChunkedColumnInterface::ScanResult
OpenDataScan(const ChunkedColumnInterface* column,
             const ColumnPlanner* planner,
             milvus::OpContext* op_ctx,
             int64_t start_offset,
             DataType data_type,
             ChunkedColumnInterface::TargetType target_type,
             ChunkedColumnInterface::ScanPinPolicy pin_policy,
             bool prefetch,
             ColumnFilterPtr filter) {
    AssertInfo(target_type == ChunkedColumnInterface::TargetType::None ||
                   CanReadAsTargetType(data_type, target_type),
               "data scan target {} does not match column type {}",
               static_cast<int>(target_type),
               data_type);
    return std::make_unique<RawScanCursor>(column,
                                           planner,
                                           op_ctx,
                                           start_offset,
                                           target_type,
                                           pin_policy,
                                           prefetch,
                                           std::move(filter));
}

}  // namespace detail

ChunkedColumnInterface::ScanResult
ChunkedColumnInterface::Scan(milvus::OpContext* op_ctx,
                             const ScanOptions& options) const {
    if (options.output != ScanOutput::Data ||
        options.predicate != ScanPredicate::None) {
        return nullptr;
    }
    auto data_type = GetDefaultScanDataType();
    if (!data_type.has_value()) {
        return nullptr;
    }
    return detail::OpenDataScan(this,
                                &Planner(),
                                op_ctx,
                                options.start_offset,
                                *data_type,
                                options.target_type,
                                options.pin_policy,
                                options.prefetch,
                                options.filter);
}

}  // namespace milvus
