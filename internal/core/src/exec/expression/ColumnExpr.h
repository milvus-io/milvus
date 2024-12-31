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

#include <fmt/core.h>
#include <boost/variant.hpp>
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentChunkReader.h"

namespace milvus {
namespace exec {

class PhyColumnExpr : public Expr {
 public:
    PhyColumnExpr(const std::vector<std::shared_ptr<Expr>>& input,
                  const std::shared_ptr<const milvus::expr::ColumnExpr>& expr,
                  const std::string& name,
                  const segcore::SegmentInternalInterface* segment,
                  int64_t active_count,
                  int64_t batch_size)
        : Expr(expr->type(), std::move(input), name),
          segment_chunk_reader_(segment, active_count),
          batch_size_(batch_size),
          expr_(expr) {
        is_indexed_ = segment->HasIndex(expr_->GetColumn().field_id_);
        if (segment->is_chunked()) {
            num_chunk_ =
                is_indexed_
                    ? segment->num_chunk_index(expr_->GetColumn().field_id_)
                : segment->type() == SegmentType::Growing
                    ? upper_div(segment_chunk_reader_.active_count_,
                                segment_chunk_reader_.SizePerChunk())
                    : segment->num_chunk_data(expr_->GetColumn().field_id_);
        } else {
            num_chunk_ =
                is_indexed_
                    ? segment->num_chunk_index(expr_->GetColumn().field_id_)
                    : upper_div(segment_chunk_reader_.active_count_,
                                segment_chunk_reader_.SizePerChunk());
        }
        AssertInfo(
            batch_size_ > 0,
            fmt::format("expr batch size should greater than zero, but now: {}",
                        batch_size_));
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    MoveCursor() override {
        if (!has_offset_input_) {
            if (segment_chunk_reader_.segment_->is_chunked()) {
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
        if (segment_chunk_reader_.segment_->is_chunked()) {
            auto current_rows =
                is_indexed_ && segment_chunk_reader_.segment_->type() ==
                                   SegmentType::Sealed
                    ? current_chunk_pos_
                    : segment_chunk_reader_.segment_->num_rows_until_chunk(
                          expr_->GetColumn().field_id_, current_chunk_id_) +
                          current_chunk_pos_;
            return current_rows;
        } else {
            return segment_chunk_reader_.segment_->type() ==
                           SegmentType::Growing
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

 private:
    bool is_indexed_;

    int64_t num_chunk_{0};
    int64_t current_chunk_id_{0};
    int64_t current_chunk_pos_{0};

    const segcore::SegmentChunkReader segment_chunk_reader_;
    int64_t batch_size_;
    std::shared_ptr<const milvus::expr::ColumnExpr> expr_;
};

}  //namespace exec
}  // namespace milvus
