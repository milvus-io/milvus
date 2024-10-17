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

namespace milvus {
namespace exec {

// NOTE: similar to PhyCompareFilterExpr
using number = boost::variant<bool,
                              int8_t,
                              int16_t,
                              int32_t,
                              int64_t,
                              float,
                              double,
                              std::string>;
using ChunkDataAccessor = std::function<const number(int)>;
using MultipleChunkDataAccessor = std::function<const number()>;

class PhyColumnExpr : public Expr {
 public:
    PhyColumnExpr(const std::vector<std::shared_ptr<Expr>>& input,
                  const std::shared_ptr<const milvus::expr::ColumnExpr>& expr,
                  const std::string& name,
                  const segcore::SegmentInternalInterface* segment,
                  int64_t active_count,
                  int64_t batch_size)
        : Expr(expr->type(), std::move(input), name),
          active_count_(active_count),
          segment_(segment),
          batch_size_(batch_size),
          expr_(expr) {
        is_indexed_ = segment_->HasIndex(expr_->GetColumn().field_id_);
        size_per_chunk_ = segment_->size_per_chunk();
        // NOTE: similar to PhyCompareFilterExpr
        if (segment_->is_chunked()) {
            num_chunk_ =
                is_indexed_
                    ? segment_->num_chunk_index(expr_->GetColumn().field_id_)
                : segment_->type() == SegmentType::Growing
                    ? upper_div(active_count_, size_per_chunk_)
                    : segment_->num_chunk_data(expr_->GetColumn().field_id_);
        } else {
            num_chunk_ =
                is_indexed_
                    ? segment_->num_chunk_index(expr_->GetColumn().field_id_)
                    : upper_div(active_count_, size_per_chunk_);
        }
        AssertInfo(
            batch_size_ > 0,
            fmt::format("expr batch size should greater than zero, but now: {}",
                        batch_size_));
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    // NOTE: similar to PhyCompareFilterExpr
    void
    MoveCursor() override {
        if (segment_->is_chunked()) {
            MoveCursorForMultipleChunk();
        } else {
            MoveCursorForSingleChunk();
        }
    }

    void
    MoveCursorForMultipleChunk() {
        int64_t processed_rows = 0;
        for (int64_t chunk_id = current_chunk_id_; chunk_id < num_chunk_;
             ++chunk_id) {
            auto chunk_size = 0;
            if (segment_->type() == SegmentType::Growing) {
                chunk_size = chunk_id == num_chunk_ - 1
                                 ? active_count_ - chunk_id * size_per_chunk_
                                 : size_per_chunk_;
            } else {
                chunk_size = segment_->chunk_size(expr_->GetColumn().field_id_,
                                                  chunk_id);
            }

            for (int i = chunk_id == current_chunk_id_ ? current_chunk_pos_ : 0;
                 i < chunk_size;
                 ++i) {
                if (++processed_rows >= batch_size_) {
                    current_chunk_id_ = chunk_id;
                    current_chunk_pos_ = i + 1;
                }
            }
        }
    }

    void
    MoveCursorForSingleChunk() {
        int64_t processed_rows = 0;
        for (int64_t chunk_id = current_chunk_id_; chunk_id < num_chunk_;
             ++chunk_id) {
            auto chunk_size = chunk_id == num_chunk_ - 1
                                  ? active_count_ - chunk_id * size_per_chunk_
                                  : size_per_chunk_;

            for (int i = chunk_id == current_chunk_id_ ? current_chunk_pos_ : 0;
                 i < chunk_size;
                 ++i) {
                if (++processed_rows >= batch_size_) {
                    current_chunk_id_ = chunk_id;
                    current_chunk_pos_ = i + 1;
                }
            }
        }
    }

 private:
    int64_t
    GetCurrentRows() {
        if (segment_->is_chunked()) {
            auto current_rows =
                is_indexed_ && segment_->type() == SegmentType::Sealed
                    ? current_chunk_pos_
                    : segment_->num_rows_until_chunk(
                          expr_->GetColumn().field_id_, current_chunk_id_) +
                          current_chunk_pos_;
            return current_rows;
        } else {
            return segment_->type() == SegmentType::Growing
                       ? current_chunk_id_ * size_per_chunk_ +
                             current_chunk_pos_
                       : current_chunk_pos_;
        }
    }

    template <typename T>
    MultipleChunkDataAccessor
    GetChunkData(FieldId field_id,
                 bool index,
                 int64_t& current_chunk_id,
                 int64_t& current_chunk_pos);

    template <typename T>
    ChunkDataAccessor
    GetChunkData(FieldId field_id, int chunk_id, int data_barrier);

    MultipleChunkDataAccessor
    GetChunkData(DataType data_type,
                 FieldId field_id,
                 bool index,
                 int64_t& current_chunk_id,
                 int64_t& current_chunk_pos);

    ChunkDataAccessor
    GetChunkData(DataType data_type,
                 FieldId field_id,
                 int chunk_id,
                 int data_barrier);
    int64_t
    GetNextBatchSize();

    template <typename T>
    VectorPtr
    DoEval();

 private:
    bool is_indexed_;

    int64_t active_count_;
    int64_t num_chunk_{0};
    int64_t current_chunk_id_{0};
    int64_t current_chunk_pos_{0};
    int64_t size_per_chunk_{0};

    const segcore::SegmentInternalInterface* segment_;
    int64_t batch_size_;
    std::shared_ptr<const milvus::expr::ColumnExpr> expr_;
};

}  //namespace exec
}  // namespace milvus
