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

#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "exec/expression/Expr.h"
#include "exec/expression/SequenceMatchCore.h"
#include "segcore/SegmentChunkReader.h"

namespace milvus::exec {

class PhySequenceMatchFilterExpr : public Expr {
 public:
    PhySequenceMatchFilterExpr(
        std::vector<std::shared_ptr<Expr>> input,
        const std::shared_ptr<const milvus::expr::SequenceMatchExpr>& expr,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size);

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    MoveCursor() override;

    void
    SetSnapshot(const segcore::SegmentReadSnapshot* snapshot) override {
        reader_.SetSnapshot(snapshot);
    }

    bool
    IsSource() const override {
        return true;
    }

    // The compact ToString() deliberately omits client step literals and
    // windows; it cannot serve as a unique filter-bitmap cache key.
    bool
    IsCacheable() const override {
        return false;
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return std::nullopt;
    }

    std::string
    ToString() const override {
        return expr_->ToString();
    }

 private:
    sequence::Row
    LoadRow(int64_t element_start, int64_t element_end) const;

    void
    ApplyStructValidity(ColumnVector* result,
                        FieldId first_field_id,
                        const OffsetVector* offsets,
                        int64_t rows) const;

    std::shared_ptr<const milvus::expr::SequenceMatchExpr> expr_;
    const segcore::SegmentInternalInterface* segment_;
    segcore::SegmentChunkReader reader_;
    int64_t active_count_;
    int64_t batch_size_;
    int64_t current_pos_{0};
    std::set<int64_t> fields_;
};

}  // namespace milvus::exec
