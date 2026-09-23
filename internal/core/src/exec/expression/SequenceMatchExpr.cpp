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

#include "SequenceMatchExpr.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <tuple>
#include <utility>

#include <boost/core/span.hpp>

#include "common/ArrayOffsets.h"
#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "exec/expression/EvalCtx.h"
#include "segcore/SegmentInterface.h"

namespace milvus::exec {

namespace {
void
CollectFields(const proto::plan::Expr& expr, std::set<int64_t>& fields) {
    switch (expr.expr_case()) {
        case proto::plan::Expr::kBinaryExpr:
            CollectFields(expr.binary_expr().left(), fields);
            CollectFields(expr.binary_expr().right(), fields);
            return;
        case proto::plan::Expr::kUnaryRangeExpr:
            fields.insert(expr.unary_range_expr().column_info().field_id());
            return;
        case proto::plan::Expr::kCompareExpr:
            fields.insert(expr.compare_expr().left_column_info().field_id());
            fields.insert(expr.compare_expr().right_column_info().field_id());
            return;
        case proto::plan::Expr::kAlwaysTrueExpr:
            return;
        default:
            ThrowInfo(ExprInvalid, "unsupported SEQUENCE_MATCH predicate");
    }
}

sequence::Cell
OwnValue(const segcore::data_access_type& raw, DataType type) {
    if (!raw) {
        return std::nullopt;
    }
    switch (type) {
        case DataType::BOOL:
            return sequence::Value(segcore::get_from_variant<bool>(raw));
        case DataType::INT8:
            return sequence::Value(
                static_cast<int64_t>(segcore::get_from_variant<int8_t>(raw)));
        case DataType::INT16:
            return sequence::Value(
                static_cast<int64_t>(segcore::get_from_variant<int16_t>(raw)));
        case DataType::INT32:
            return sequence::Value(
                static_cast<int64_t>(segcore::get_from_variant<int32_t>(raw)));
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return sequence::Value(segcore::get_from_variant<int64_t>(raw));
        case DataType::FLOAT:
            return sequence::Value(
                static_cast<double>(segcore::get_from_variant<float>(raw)));
        case DataType::DOUBLE:
            return sequence::Value(segcore::get_from_variant<double>(raw));
        case DataType::VARCHAR:
        case DataType::TEXT:
            return sequence::Value(segcore::get_from_variant<std::string>(raw));
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported SEQUENCE_MATCH subfield type {}",
                      type);
    }
}
}  // namespace

PhySequenceMatchFilterExpr::PhySequenceMatchFilterExpr(
    std::vector<std::shared_ptr<Expr>> input,
    const std::shared_ptr<const milvus::expr::SequenceMatchExpr>& expr,
    milvus::OpContext* op_ctx,
    const segcore::SegmentInternalInterface* segment,
    int64_t active_count,
    int64_t batch_size)
    : Expr(DataType::BOOL,
           std::move(input),
           "PhySequenceMatchFilterExpr",
           op_ctx),
      expr_(expr),
      segment_(segment),
      reader_(op_ctx, segment, active_count),
      active_count_(active_count),
      batch_size_(batch_size) {
    const auto& spec = expr_->spec();
    fields_.insert(spec.order_time_field_id());
    fields_.insert(spec.tie_field_id());
    for (const auto& step : spec.steps()) {
        CollectFields(step.predicate(), fields_);
        if (step.has_window()) {
            fields_.insert(step.window().current_field_id());
            fields_.insert(step.window().prior_field_id());
        }
    }
}

void
PhySequenceMatchFilterExpr::MoveCursor() {
    if (!has_offset_input_) {
        current_pos_ += std::min(batch_size_, active_count_ - current_pos_);
    }
}

sequence::Row
PhySequenceMatchFilterExpr::LoadRow(int64_t element_start,
                                    int64_t element_end) const {
    sequence::Row row;
    const auto count = static_cast<size_t>(element_end - element_start);
    auto schema = segment_->get_schema_snapshot();
    for (auto raw_field : fields_) {
        const FieldId field_id(raw_field);
        const auto& meta = (*schema)[field_id];
        const auto type = meta.get_element_type();
        auto& cells = row[raw_field];
        cells.reserve(count);
        int64_t cached_chunk = -1;
        segcore::ChunkDataAccessor accessor;
        for (int64_t element = element_start; element < element_end;
             ++element) {
            int64_t chunk_id = 0;
            int64_t chunk_offset = element;
            if (segment_->type() == SegmentType::Growing) {
                chunk_id = element / reader_.SizePerChunk();
                chunk_offset = element % reader_.SizePerChunk();
            } else if (segment_->is_chunked() &&
                       reader_.NumChunkData(field_id) > 0) {
                std::tie(chunk_id, chunk_offset) =
                    reader_.GetChunkByOffset(field_id, element);
            }
            if (chunk_id != cached_chunk) {
                accessor = reader_.GetChunkDataAccessor(
                    type,
                    field_id,
                    static_cast<int>(chunk_id),
                    segcore::PinnedIndexView{});
                cached_chunk = chunk_id;
            }
            cells.push_back(
                OwnValue(accessor(static_cast<int>(chunk_offset)), type));
        }
    }
    return row;
}

void
PhySequenceMatchFilterExpr::ApplyStructValidity(ColumnVector* output,
                                                FieldId first_field_id,
                                                const OffsetVector* offsets,
                                                int64_t rows) const {
    TargetBitmapView values(output->GetRawData(), output->size());
    TargetBitmapView valid(output->GetValidRawData(), output->size());
    if (offsets != nullptr) {
        std::vector<int64_t> row_ids(rows);
        for (int64_t i = 0; i < rows; ++i) {
            row_ids[i] = (*offsets)[i];
        }
        reader_.ApplyFieldValidDataByOffsets(
            op_ctx_, first_field_id, row_ids.data(), rows, valid);
    } else {
        int64_t done = 0;
        while (done < rows) {
            auto [chunk, position] =
                reader_.GetChunkByOffset(first_field_id, current_pos_ + done);
            auto size =
                std::min(rows - done,
                         reader_.ChunkSize(first_field_id, chunk) - position);
            AssertInfo(size > 0, "invalid SEQUENCE_MATCH validity range");
            reader_.ApplyFieldValidData(
                op_ctx_, first_field_id, chunk, position, size, valid + done);
            done += size;
        }
    }
    for (int64_t i = 0; i < rows; ++i) {
        if (!valid[i]) {
            values[i] = false;
        }
    }
}

void
PhySequenceMatchFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    const auto* offsets = context.get_offset_input();
    SetHasOffsetInput(offsets != nullptr);
    const int64_t rows =
        offsets != nullptr
            ? static_cast<int64_t>(offsets->size())
            : std::min(batch_size_, active_count_ - current_pos_);
    if (rows <= 0) {
        result = nullptr;
        return;
    }
    auto output = std::make_shared<ColumnVector>(TargetBitmap(rows, false),
                                                 TargetBitmap(rows, true));
    TargetBitmapView bits(output->GetRawData(), output->size());
    const auto& active = context.get_bitmap_input();
    AssertInfo(active.empty() || active.size() == static_cast<size_t>(rows),
               "SEQUENCE_MATCH active bitmap has wrong size");

    auto schema = segment_->get_schema_snapshot();
    const auto& first =
        schema->GetFirstArrayFieldInStruct(expr_->spec().struct_name());
    if (!active.empty() && active.none()) {
        // The parent conjunction/disjunction has already made every row
        // inactive. Their local output bits are irrelevant to the final
        // Boolean result, so avoid touching the StructArray entirely.
        result = output;
        if (offsets == nullptr) {
            current_pos_ += rows;
        }
        return;
    }
    // The first child owns the row-to-element offsets and parent validity.
    // Both it and every referenced child need resident raw columns in the
    // MVP. Index-only reverse lookup is insufficient for the validity path,
    // and dropping the first child can also discard its offset mapping.
    if (segment_->type() == SegmentType::Sealed) {
        if (!segment_->HasFieldData(first.get_id())) {
            ThrowInfo(Unsupported,
                      "SEQUENCE_MATCH requires resident raw StructArray "
                      "subfield {}",
                      first.get_id().get());
        }
        for (auto raw_field : fields_) {
            if (!segment_->HasFieldData(FieldId(raw_field))) {
                ThrowInfo(Unsupported,
                          "SEQUENCE_MATCH requires resident raw StructArray "
                          "subfield {}",
                          raw_field);
            }
        }
    }
    auto array_offsets = segment_->GetArrayOffsets(first.get_id());
    AssertInfo(array_offsets != nullptr,
               "SEQUENCE_MATCH array offsets are unavailable");
    std::vector<int32_t> row_ids(rows);
    for (int64_t i = 0; i < rows; ++i) {
        row_ids[i] = offsets == nullptr ? current_pos_ + i : (*offsets)[i];
    }
    std::vector<std::pair<int32_t, int32_t>> ranges(rows);
    array_offsets->CopyRowElementRanges(row_ids.data(), rows, ranges.data());
    for (int64_t i = 0; i < rows; ++i) {
        if ((!active.empty() && !active[i]) ||
            ranges[i].second - ranges[i].first < expr_->spec().steps_size()) {
            continue;
        }
        auto row = LoadRow(ranges[i].first, ranges[i].second);
        bits[i] = sequence::Match(
            expr_->spec(), row, ranges[i].second - ranges[i].first);
    }
    ApplyStructValidity(output.get(), first.get_id(), offsets, rows);
    result = output;
    if (offsets == nullptr) {
        current_pos_ += rows;
    }
}

}  // namespace milvus::exec
