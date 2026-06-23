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
#include <stdint.h>
#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Geometry.h"
#include "common/OpContext.h"
#include "common/PreparedGeometry.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "pb/plan.pb.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

// Evaluate a single GIS predicate using a prepared query geometry against an
// already-constructed `left` row geometry. This centralizes the prepared
// predicate semantics — notably the contains/within swap
// (left.contains(query) == query.within(left)) — so the per-predicate path
// (PhyGISFunctionFilterExpr::EvalForIndexSegment) and the optimizer's fusion
// path (PhyGISRefineConjunctExpr) stay in lockstep instead of drifting as new
// GISOps are added.
inline bool
EvaluateGISPreparedOp(proto::plan::GISFunctionFilterExpr_GISOp op,
                      const PreparedGeometry& prepared,
                      const Geometry& query_geom,
                      const Geometry& left,
                      double distance) {
    switch (op) {
        case proto::plan::GISFunctionFilterExpr_GISOp_Intersects:
            // Symmetric: prepared.intersects(left) == left.intersects(query)
            return prepared.intersects(left);
        case proto::plan::GISFunctionFilterExpr_GISOp_Touches:
            return prepared.touches(left);
        case proto::plan::GISFunctionFilterExpr_GISOp_Overlaps:
            return prepared.overlaps(left);
        case proto::plan::GISFunctionFilterExpr_GISOp_Crosses:
            return prepared.crosses(left);
        case proto::plan::GISFunctionFilterExpr_GISOp_Contains:
            // left.contains(query) == query.within(left)
            return prepared.within(left);
        case proto::plan::GISFunctionFilterExpr_GISOp_Within:
            // left.within(query) == query.contains(left)
            return prepared.contains(left);
        case proto::plan::GISFunctionFilterExpr_GISOp_Equals:
            // No prepared version - fall back to regular geometry.
            return left.equals(query_geom);
        case proto::plan::GISFunctionFilterExpr_GISOp_DWithin:
            // Distance-based operation - no prepared version.
            return left.dwithin(query_geom, distance);
        default:
            ThrowInfo(
                NotImplemented, "unknown GIS op : {}", static_cast<int>(op));
    }
}

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
        // DetermineExecPath();
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    DetermineExecPath() override;

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

    // Expose the logical GIS expr so the optimizer can group same-column GIS
    // predicates into a PhyGISCoarseConjunctExpr / PhyGISRefineConjunctExpr.
    const std::shared_ptr<const milvus::expr::GISFunctionFilterExpr>&
    GetGISExpr() const {
        return expr_;
    }

    std::string
    ToString() const override {
        return fmt::format("{}", expr_->ToString());
    }

    // The GIS filter slices by its own batch cursor (GetNextBatchSize) and never
    // reads the offset-input list, so it cannot serve the offset-input
    // (iterative-filter / rescore) path. Report false so IterativeFilterNode
    // takes its non-native fallback instead of feeding offsets into Eval.
    bool
    SupportOffsetInput() override {
        return false;
    }

    // A skipped batch (conjunct short-circuit via SkipFollowingExprs) must
    // still advance this expression's cursors, otherwise it desynchronizes
    // from its sibling expressions and later batches evaluate the wrong rows.
    // The base MoveCursor() covers every case except the growing interim-index
    // path: MoveCursorForIndex() asserts sealed-only, while
    // EvalForIndexSegment() on a growing segment advances the global index
    // position together with the data cursor -- mirror that here.
    // Unlike the base implementation, MoveCursorForData() is called without a
    // HasFieldData() guard: this exec path already walks data chunks
    // unconditionally (EvalForIndexSegment), and with no field data
    // num_data_chunk_ is 0, making the call a no-op -- the omission is
    // intentional, not an oversight.
    void
    MoveCursor() override {
        if (has_offset_input_ || execute_all_at_once_) {
            return;
        }
        if (UseIndexCursor() && segment_->type() != SegmentType::Sealed) {
            current_index_chunk_pos_ +=
                std::min(active_count_ - current_index_chunk_pos_, batch_size_);
            MoveCursorForData();
            return;
        }
        SegmentExpr::MoveCursor();
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
