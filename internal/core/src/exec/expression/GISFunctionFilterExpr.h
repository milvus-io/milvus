// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <fmt/core.h>
#include <memory>

#include "common/FieldDataInterface.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "segcore/SegmentInterface.h"
#include "common/GeometryCache.h"

namespace milvus {
namespace exec {

class PhyGISFunctionFilterExpr : public SegmentExpr {
 public:
    PhyGISFunctionFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::GISFunctionFilterExpr>& expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level)
        : SegmentExpr(std::move(input),
                      name,
                      op_ctx,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      DataType::GEOMETRY,
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

 private:
    VectorPtr
    EvalForIndexSegment();

    VectorPtr
    EvalForDataSegment();

 private:
    std::shared_ptr<const milvus::expr::GISFunctionFilterExpr> expr_;

    /*
     * Segment-level cache: run a single R-Tree Query for all index chunks to
     * obtain coarse candidate bitmaps. Subsequent batches reuse these cached
     * results to avoid repeated ScalarIndex::Query calls per chunk.
     */
    // whether coarse results have been prefetched once
    bool coarse_cached_ = false;
    // global coarse bitmap (segment-level)
    TargetBitmap coarse_global_;
    // global not-null bitmap (segment-level)
    TargetBitmap coarse_valid_global_;
};
}  //namespace exec
}  // namespace milvus
