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

#include <algorithm>
#include <cstdint>
#include <fmt/core.h>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "common/Schema.h"
#include "common/Utils.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "exec/expression/ValueLookupSource.h"
#include "expr/ITypeExpr.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "segcore/SegmentChunkReader.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

class PhyColumnExpr : public Expr {
 public:
    PhyColumnExpr(std::vector<std::shared_ptr<Expr>> input,
                  const std::shared_ptr<const milvus::expr::ColumnExpr>& expr,
                  const std::string& name,
                  milvus::OpContext* op_ctx,
                  const segcore::SegmentInternalInterface* segment,
                  int64_t active_count,
                  int64_t batch_size)
        : Expr(expr->type(), std::move(input), name, op_ctx),
          segment_(segment),
          active_count_(active_count),
          segment_chunk_reader_(op_ctx, segment, active_count),
          batch_size_(batch_size),
          expr_(expr),
          value_lookup_(segment,
                        op_ctx,
                        expr->GetColumn().field_id_,
                        expr->GetColumn().data_type_,
                        active_count) {
        if (segment->type() == SegmentType::Growing && segment->is_chunked()) {
            num_chunk_ = segment_chunk_reader_.NumChunkData(
                expr_->GetColumn().field_id_);
        } else if (segment->type() == SegmentType::Growing) {
            num_chunk_ =
                upper_div(active_count_, segment_chunk_reader_.SizePerChunk());
        }
        AssertInfo(
            batch_size_ > 0,
            fmt::format("expr batch size should greater than zero, but now: {}",
                        batch_size_));
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    SetSnapshot(const segcore::SegmentReadSnapshot* snapshot) override {
        snapshot_ = snapshot;
    }

    void
    MoveCursor() override {
        if (!has_offset_input_) {
            if (HasValueReader() || segment_->type() == SegmentType::Sealed) {
                current_row_ =
                    std::min(current_row_ + batch_size_, active_count_);
                return;
            }
            if (segment_->is_chunked()) {
                segment_chunk_reader_.MoveCursorForMultipleChunk(
                    current_chunk_id_,
                    current_chunk_pos_,
                    expr_->GetColumn().field_id_,
                    num_chunk_,
                    batch_size_);
            } else {
                segment_chunk_reader_.MoveCursorForSingleChunk(
                    current_chunk_id_,
                    current_chunk_pos_,
                    num_chunk_,
                    batch_size_);
            }
        }
    }

 private:
    int64_t
    GetCurrentRows() const {
        if (HasValueReader() || segment_->type() == SegmentType::Sealed) {
            return current_row_;
        }
        if (segment_->is_chunked()) {
            return segment_chunk_reader_.NumRowsUntilChunk(
                       expr_->GetColumn().field_id_, current_chunk_id_) +
                   current_chunk_pos_;
        } else {
            return segment_->type() == SegmentType::Growing
                       ? current_chunk_id_ *
                                 segment_chunk_reader_.SizePerChunk() +
                             current_chunk_pos_
                       : current_chunk_pos_;
        }
    }

    int64_t
    GetNextBatchSize();

    template <typename T>
    VectorPtr
    DoEval(OffsetVector* input = nullptr);

    template <typename T>
    VectorPtr
    DoEvalFromValueReader(OffsetVector* input);

    bool
    HasValueReader() const {
        return value_lookup_.HasReader();
    }

    template <typename T>
    bool
    GatherFromValueReader(const int64_t* offsets,
                          int64_t count,
                          T* values,
                          TargetBitmapView valid);

    std::string
    ToString() const override {
        return fmt::format("{}", expr_->ToString());
    }

    bool
    IsSource() const override {
        return true;
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->GetColumn();
    }

    bool
    CanExecuteAllAtOnce() const override {
        return false;
    }

    int64_t num_chunk_{0};
    int64_t current_chunk_id_{0};
    int64_t current_chunk_pos_{0};
    int64_t current_row_{0};
    const segcore::SegmentInternalInterface* segment_;
    const int64_t active_count_;
    const segcore::SegmentReadSnapshot* snapshot_{nullptr};
    std::shared_ptr<ChunkedColumnInterface> data_column_;
    ScanResult scan_cursor_;
    // Only Growing still uses the Chunk-position compatibility reader.
    const segcore::SegmentChunkReader segment_chunk_reader_;
    int64_t batch_size_;
    std::shared_ptr<const milvus::expr::ColumnExpr> expr_;
    ValueLookupSource value_lookup_;
};

}  //namespace exec
}  // namespace milvus
