// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
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
#include <utility>

#include "mmap/ChunkedColumnInterface.h"

namespace milvus {
namespace detail {

using StringViews = std::pair<std::vector<std::string_view>, FixedVector<bool>>;
using ArrayViews = std::pair<std::vector<ArrayView>, FixedVector<bool>>;

struct FixedTakeOwner {
    FixedTakeOwner(PinWrapper<SpanBase>&& input,
                   FixedVector<int32_t>&& selection)
        : input(std::move(input)), selection(std::move(selection)) {
    }

    PinWrapper<SpanBase> input;
    FixedVector<int32_t> selection;
};

struct StringTakeOwner {
    explicit StringTakeOwner(PinWrapper<StringViews>&& input)
        : input(std::move(input)) {
    }

    PinWrapper<StringViews> input;
};

struct JsonTakeOwner {
    explicit JsonTakeOwner(PinWrapper<StringViews>&& input)
        : input(std::move(input)) {
        const auto& strings = this->input.get().first;
        values.reserve(strings.size());
        for (const auto& value : strings) {
            values.emplace_back(value);
        }
    }

    PinWrapper<StringViews> input;
    std::vector<Json> values;
};

struct ArrayTakeOwner {
    explicit ArrayTakeOwner(PinWrapper<ArrayViews>&& input)
        : input(std::move(input)) {
    }

    PinWrapper<ArrayViews> input;
};

void
ResetTakeBatch(ChunkedColumnInterface::TakeBatch* out) {
    out->values = ChunkedColumnInterface::ValueView{};
    out->selection = nullptr;
    out->validity = nullptr;
    out->owner.reset();
    out->position = 0;
    out->size = 0;
    out->source_chunk_id = -1;
}

std::optional<ChunkedColumnInterface::ScanValueKind>
ResolveTakeValueKind(DataType data_type,
                     ChunkedColumnInterface::ScanValueKind requested) {
    ChunkedColumnInterface::ScanValueKind physical;
    if (ChunkedColumnInterface::IsPrimitiveDataType(data_type)) {
        physical = ChunkedColumnInterface::ScanValueKind::FixedWidth;
    } else if (data_type == DataType::JSON) {
        physical = ChunkedColumnInterface::ScanValueKind::JsonView;
    } else if (data_type == DataType::STRING ||
               data_type == DataType::VARCHAR || data_type == DataType::TEXT ||
               data_type == DataType::GEOMETRY) {
        physical = ChunkedColumnInterface::ScanValueKind::StringView;
    } else if (data_type == DataType::ARRAY) {
        physical = ChunkedColumnInterface::ScanValueKind::ArrayView;
    } else {
        return std::nullopt;
    }

    const auto resolved =
        requested == ChunkedColumnInterface::ScanValueKind::Default ? physical
                                                                    : requested;
    AssertInfo(resolved == physical,
               "take value kind {} does not match column type {}, expected {}",
               static_cast<int>(resolved),
               data_type,
               static_cast<int>(physical));
    return resolved;
}

class RawTakeCursor final : public ChunkedColumnInterface::TakeCursor {
 public:
    RawTakeCursor(const ChunkedColumnInterface* column,
                  milvus::OpContext* op_ctx,
                  ChunkedColumnInterface::OffsetView offsets,
                  DataType data_type,
                  ChunkedColumnInterface::ScanValueKind value_kind)
        : column_(column),
          op_ctx_(op_ctx),
          offsets_(offsets),
          data_type_(data_type),
          value_kind_(value_kind) {
    }

    int64_t
    Position() const override {
        return position_;
    }

    bool
    Next(int64_t max_rows, ChunkedColumnInterface::TakeBatch* out) override {
        AssertInfo(out != nullptr, "take output batch is null");
        AssertInfo(
            max_rows > 0, "take max rows must be positive, got {}", max_rows);
        ResetTakeBatch(out);
        if (position_ >= offsets_.size) {
            return false;
        }

        const auto batch_position = position_;
        const auto first_offset = offsets_[position_];
        auto [chunk_id, chunk_offset] =
            column_->GetChunkIDByOffset(first_offset);
        FixedVector<int32_t> local_offsets;
        local_offsets.reserve(
            std::min<int64_t>(max_rows, offsets_.size - position_));
        local_offsets.emplace_back(static_cast<int32_t>(chunk_offset));
        ++position_;
        while (position_ < offsets_.size &&
               static_cast<int64_t>(local_offsets.size()) < max_rows) {
            auto [next_chunk_id, next_chunk_offset] =
                column_->GetChunkIDByOffset(offsets_[position_]);
            if (next_chunk_id != chunk_id) {
                break;
            }
            local_offsets.emplace_back(static_cast<int32_t>(next_chunk_offset));
            ++position_;
        }

        out->position = batch_position;
        out->size = static_cast<int64_t>(local_offsets.size());
        out->source_chunk_id = static_cast<int64_t>(chunk_id);
        switch (value_kind_) {
            case ChunkedColumnInterface::ScanValueKind::FixedWidth:
                FillFixedWidth(chunk_id, std::move(local_offsets), out);
                break;
            case ChunkedColumnInterface::ScanValueKind::StringView:
                FillStringViews(chunk_id, local_offsets, out);
                break;
            case ChunkedColumnInterface::ScanValueKind::JsonView:
                FillJsonViews(chunk_id, local_offsets, out);
                break;
            case ChunkedColumnInterface::ScanValueKind::ArrayView:
                FillArrayViews(chunk_id, local_offsets, out);
                break;
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "unsupported raw take value kind {}",
                          static_cast<int>(value_kind_));
        }
        return true;
    }

 private:
    void
    FillFixedWidth(int64_t chunk_id,
                   FixedVector<int32_t>&& local_offsets,
                   ChunkedColumnInterface::TakeBatch* out) const {
        auto owner = std::make_shared<FixedTakeOwner>(
            column_->Span(op_ctx_, chunk_id), std::move(local_offsets));
        const auto& span = owner->input.get();
        out->values.encoding =
            ChunkedColumnInterface::ValueEncoding::FixedWidth;
        out->values.kind = ChunkedColumnInterface::ScanValueKind::FixedWidth;
        out->values.physical_type = data_type_;
        out->values.logical_type = data_type_;
        out->values.data = span.data();
        out->values.offset = 0;
        out->values.size = span.row_count();
        out->values.byte_width = span.element_sizeof();
        out->selection = owner->selection.data();
        out->validity = span.valid_data();
        out->owner = std::move(owner);
    }

    void
    FillStringViews(int64_t chunk_id,
                    const FixedVector<int32_t>& local_offsets,
                    ChunkedColumnInterface::TakeBatch* out) const {
        auto owner = std::make_shared<StringTakeOwner>(
            column_->StringViewsByOffsets(op_ctx_, chunk_id, local_offsets));
        const auto& [views, validity] = owner->input.get();
        out->values.encoding =
            ChunkedColumnInterface::ValueEncoding::StringView;
        out->values.kind = ChunkedColumnInterface::ScanValueKind::StringView;
        out->values.physical_type = data_type_;
        out->values.logical_type = data_type_;
        out->values.data = views.data();
        out->values.offset = 0;
        out->values.size = views.size();
        out->values.byte_width = sizeof(std::string_view);
        if (column_->IsNullable() && !validity.empty()) {
            out->validity = validity.data();
        }
        out->owner = std::move(owner);
    }

    void
    FillJsonViews(int64_t chunk_id,
                  const FixedVector<int32_t>& local_offsets,
                  ChunkedColumnInterface::TakeBatch* out) const {
        auto owner = std::make_shared<JsonTakeOwner>(
            column_->StringViewsByOffsets(op_ctx_, chunk_id, local_offsets));
        const auto& validity = owner->input.get().second;
        out->values.encoding = ChunkedColumnInterface::ValueEncoding::JsonView;
        out->values.kind = ChunkedColumnInterface::ScanValueKind::JsonView;
        out->values.physical_type = data_type_;
        out->values.logical_type = DataType::JSON;
        out->values.data = owner->values.data();
        out->values.offset = 0;
        out->values.size = owner->values.size();
        out->values.byte_width = sizeof(Json);
        if (column_->IsNullable() && !validity.empty()) {
            out->validity = validity.data();
        }
        out->owner = std::move(owner);
    }

    void
    FillArrayViews(int64_t chunk_id,
                   const FixedVector<int32_t>& local_offsets,
                   ChunkedColumnInterface::TakeBatch* out) const {
        auto owner = std::make_shared<ArrayTakeOwner>(
            column_->ArrayViewsByOffsets(op_ctx_, chunk_id, local_offsets));
        const auto& [views, validity] = owner->input.get();
        out->values.encoding = ChunkedColumnInterface::ValueEncoding::ArrayView;
        out->values.kind = ChunkedColumnInterface::ScanValueKind::ArrayView;
        out->values.physical_type = data_type_;
        out->values.logical_type = DataType::ARRAY;
        out->values.data = views.data();
        out->values.offset = 0;
        out->values.size = views.size();
        out->values.byte_width = sizeof(ArrayView);
        if (column_->IsNullable() && !validity.empty()) {
            out->validity = validity.data();
        }
        out->owner = std::move(owner);
    }

    const ChunkedColumnInterface* column_;
    milvus::OpContext* op_ctx_;
    ChunkedColumnInterface::OffsetView offsets_;
    DataType data_type_;
    ChunkedColumnInterface::ScanValueKind value_kind_;
    int64_t position_{0};
};

}  // namespace detail

ChunkedColumnInterface::TakeResult
ChunkedColumnInterface::Take(milvus::OpContext* op_ctx,
                             const TakeOptions& options) const {
    AssertInfo(options.offsets.size >= 0,
               "take offset count must be non-negative, got {}",
               options.offsets.size);
    if (options.offsets.size > 0) {
        AssertInfo(options.offsets.data != nullptr,
                   "take offsets are null with count {}",
                   options.offsets.size);
    }
    auto data_type = GetDefaultScanDataType();
    if (!data_type.has_value()) {
        return nullptr;
    }
    auto value_kind =
        detail::ResolveTakeValueKind(*data_type, options.value_kind);
    if (!value_kind.has_value()) {
        return nullptr;
    }
    return std::make_unique<detail::RawTakeCursor>(
        this, op_ctx, options.offsets, *data_type, *value_kind);
}

}  // namespace milvus
