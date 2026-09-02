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

#include "exec/expression/TupleTermExpr.h"

#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "common/EasyAssert.h"
#include "storage/MmapManager.h"

namespace milvus {
namespace exec {

PhyTupleTermFilterExpr::PhyTupleTermFilterExpr(
    const std::vector<std::shared_ptr<Expr>>& input,
    const std::shared_ptr<const milvus::expr::TupleTermFilterExpr>& expr,
    const std::string& name,
    milvus::OpContext* op_ctx,
    const segcore::SegmentInternalInterface* segment,
    int64_t active_count,
    int64_t batch_size)
    : Expr(DataType::BOOL, std::move(input), name, op_ctx),
      expr_(expr),
      segment_chunk_reader_(op_ctx, segment, active_count),
      batch_size_(batch_size) {
    AssertInfo(batch_size_ > 0,
               "PhyTupleTermFilterExpr batch size should be greater than "
               "zero, but now: {}",
               batch_size_);
    AssertInfo(expr_->columns_.size() >= 2,
               "PhyTupleTermFilterExpr requires at least two columns, got {}",
               expr_->columns_.size());
    AssertInfo(expr_->membership_ != nullptr,
               "PhyTupleTermFilterExpr requires membership");

    // v1 requires raw field data for every participating column -- there is
    // no scalar-index coarse filter and no index-only reverse-lookup
    // fallback (see design doc Future work). Check every column up front, at
    // construction, rather than discovering a missing one mid-batch.
    column_readers_.reserve(expr_->columns_.size());
    for (auto& column : expr_->columns_) {
        if (!segment->HasFieldData(column.field_id_)) {
            ThrowInfo(
                FieldNotLoaded,
                "tuple 'in' cannot evaluate field {}: raw field data is not "
                "loaded. v1 requires raw field data for every participating "
                "column; scalar-index acceleration is not yet supported "
                "(see design doc 20260901-tuple-term-membership-expression, "
                "Future work)",
                column.field_id_.get());
        }
        column_readers_.push_back(BuildColumnReader(column));
    }
}

std::pair<int64_t, int64_t>
PhyTupleTermFilterExpr::GetChunkIdAndOffset(FieldId field,
                                            int64_t offset) const {
    // Growing segments chunk every field uniformly by SizePerChunk(); sealed
    // (and chunked) segments resolve the chunk through the segment's own
    // index instead. Mirrors PhyCompareFilterExpr::ProcessBothDataByOffsets's
    // get_chunk_id_and_offset lambda (CompareExpr.h) -- kept in sync with
    // that precedent rather than re-derived, since both need the identical
    // mapping for raw (non-indexed) field data.
    if (segment_chunk_reader_.segment_->type() == SegmentType::Growing) {
        auto size_per_chunk = segment_chunk_reader_.SizePerChunk();
        return {offset / size_per_chunk, offset % size_per_chunk};
    }
    return segment_chunk_reader_.segment_->get_chunk_by_offset(field, offset);
}

template <typename T>
PhyTupleTermFilterExpr::ColumnReader
PhyTupleTermFilterExpr::MakeColumnReader(
    const milvus::expr::ColumnInfo& column) const {
    FieldId field = column.field_id_;
    // Captures `this` (for op_ctx_/GetChunkIdAndOffset) and the column's
    // field id; the C++ storage type T is fixed here, at construction, so
    // the per-row hot path in ExecVisitorImpl never re-branches on DataType.
    //
    // Deliberately re-pins the field's chunk on every call rather than
    // caching the last-seen chunk id across consecutive rows the way
    // PhyCompareFilterExpr does: a correctness-first simplification (see the
    // class comment in TupleTermExpr.h) made because this code could not be
    // compiled or profiled in the environment it was authored in.
    return [this, field](int64_t row, std::string& key) -> bool {
        auto [chunk_id, chunk_offset] = GetChunkIdAndOffset(field, row);
        auto pw = segment_chunk_reader_.segment_->chunk_data<T>(
            op_ctx_, field, chunk_id);
        auto chunk = pw.get();
        auto validity = chunk.validity();
        if (validity && !validity[chunk_offset]) {
            return false;
        }
        const T* base = chunk.data();
        const T& v = base[chunk_offset];
        if constexpr (std::is_same_v<T, bool>) {
            EncodeTupleElementBool(v, key);
        } else if constexpr (std::is_same_v<T, float> ||
                             std::is_same_v<T, double>) {
            EncodeTupleElementDouble(static_cast<double>(v), key);
        } else if constexpr (std::is_same_v<T, std::string> ||
                             std::is_same_v<T, std::string_view>) {
            EncodeTupleElementBytes(v.data(), v.size(), key);
        } else {
            // int8_t / int16_t / int32_t / int64_t: widen to a canonical
            // int64 encoding, mirroring how BloomFilterExpr's TestScalar
            // widens narrow integers before hashing, so the same value
            // encodes identically regardless of the declared column width.
            EncodeTupleElementInt64(static_cast<int64_t>(v), key);
        }
        return true;
    };
}

PhyTupleTermFilterExpr::ColumnReader
PhyTupleTermFilterExpr::BuildColumnReader(
    const milvus::expr::ColumnInfo& column) const {
    switch (column.data_type_) {
        case DataType::BOOL:
            return MakeColumnReader<bool>(column);
        case DataType::INT8:
            return MakeColumnReader<int8_t>(column);
        case DataType::INT16:
            return MakeColumnReader<int16_t>(column);
        case DataType::INT32:
            return MakeColumnReader<int32_t>(column);
        case DataType::INT64:
            return MakeColumnReader<int64_t>(column);
        case DataType::FLOAT:
            return MakeColumnReader<float>(column);
        case DataType::DOUBLE:
            return MakeColumnReader<double>(column);
        case DataType::VARCHAR: {
            // Mirrors PhyMembershipFilterExpr's ExecVisitorImpl dispatch
            // (MembershipFilterExpr.cpp): a growing segment without mmap
            // enabled stores VARCHAR chunks as owned std::string; every
            // other layout (sealed, or growing with mmap) exposes a
            // std::string_view over the backing storage.
            if (segment_chunk_reader_.segment_->type() ==
                    SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                return MakeColumnReader<std::string>(column);
            }
            return MakeColumnReader<std::string_view>(column);
        }
        default:
            ThrowInfo(ExprInvalid,
                      "tuple 'in' does not support field data type: {} "
                      "(only top-level scalar fields are supported)",
                      column.data_type_);
    }
}

void
PhyTupleTermFilterExpr::MoveCursor() {
    if (!has_offset_input_) {
        auto remaining = segment_chunk_reader_.active_count_ - current_row_;
        auto size =
            remaining <= 0 ? int64_t{0} : std::min(batch_size_, remaining);
        current_row_ += size;
    }
}

VectorPtr
PhyTupleTermFilterExpr::ExecVisitorImpl(EvalCtx& context) {
    auto* input = context.get_offset_input();
    SetHasOffsetInput(input != nullptr);

    OffsetVector local_offsets;
    const OffsetVector* offsets = nullptr;
    int64_t real_batch_size = 0;

    if (input != nullptr) {
        if (input->empty()) {
            return nullptr;
        }
        real_batch_size = input->size();
        offsets = input;
    } else {
        auto remaining = segment_chunk_reader_.active_count_ - current_row_;
        auto size =
            remaining <= 0 ? int64_t{0} : std::min(batch_size_, remaining);
        if (size <= 0) {
            return nullptr;
        }
        local_offsets.resize(size);
        for (int64_t i = 0; i < size; ++i) {
            local_offsets[i] = static_cast<int32_t>(current_row_ + i);
        }
        current_row_ += size;
        real_batch_size = size;
        offsets = &local_offsets;
    }

    const auto& bitmap_input = context.get_bitmap_input();
    AssertInfo(bitmap_input.empty() ||
                   bitmap_input.size() == static_cast<size_t>(real_batch_size),
               "tuple 'in' bitmap input size {} does not match batch size {}",
               bitmap_input.size(),
               real_batch_size);
    bool has_bitmap_input = !bitmap_input.empty();

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    std::string key;
    for (int64_t i = 0; i < real_batch_size; ++i) {
        // Upstream-excluded candidates keep their initial (false, valid)
        // without being probed at all -- the same contract
        // PhyMembershipFilterExpr's raw-data path applies for bloom_match /
        // roaring_match (see MembershipFilterExpr.h's class comment), so
        // that a bitmap_input-pruned row is never treated as a positive
        // match regardless of what its (unread) value happens to be.
        if (has_bitmap_input && !bitmap_input[i]) {
            continue;
        }

        auto row = (*offsets)[i];
        key.clear();
        bool row_has_value = true;
        for (auto& read_column : column_readers_) {
            if (!read_column(row, key)) {
                row_has_value = false;
                break;
            }
        }
        if (!row_has_value) {
            // Any participating column NULL on this row -> the row never
            // matches, under either polarity (NOT is the existing generic
            // UnaryExpr wrapper around this node, so this three-valued
            // "false and invalid" here is what makes NOT correctly exclude
            // the row too, matching roaring_match's documented NULL
            // convention rather than naive boolean negation).
            res[i] = false;
            valid_res[i] = false;
            continue;
        }
        res[i] = expr_->membership_->Contains(key);
        // valid_res[i] stays true (initialized true above).
    }

    return res_vec;
}

void
PhyTupleTermFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    result = ExecVisitorImpl(context);
}

}  // namespace exec
}  // namespace milvus
