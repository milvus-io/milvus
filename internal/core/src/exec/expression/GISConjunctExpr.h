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

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "common/Geometry.h"
#include "common/PreparedGeometry.h"
#include "common/Types.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "pb/plan.pb.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

// Shared state for one same-column geometry block (a set of GIS predicates on
// the SAME geometry field combined by a single boolean op). Produced by the
// optimizer and shared between the Coarse node (fills coarse_candidates once)
// and the Refine node (exact-refines surviving rows).
struct GISGroupState {
    // One geometry predicate of the block.
    // NOTE: GEOS objects are bound to a per-thread GEOSContextHandle, so the
    // query Geometry / PreparedGeometry MUST be built per-thread inside Eval
    // (mirroring PhyGISFunctionFilterExpr). Only the immutable WKT + op + the
    // segment-level coarse bitmap are shared here.
    struct Pred {
        proto::plan::GISFunctionFilterExpr_GISOp op;
        std::string query_wkt;
        bool has_index{false};
        // Per-predicate R-Tree coarse bitmap (segment-level, computed once).
        TargetBitmap coarse;
    };

    FieldId field_id;
    // Block combine op: true => AND (intersect coarse / && refine),
    //                   false => OR (union coarse / || refine).
    bool is_and{true};
    std::vector<Pred> preds;

    // B_coarse = combine(Ci) over all preds. Filled by the Coarse node, read by
    // the Refine node. Cached once per segment for the whole query.
    std::shared_ptr<TargetBitmap> coarse_candidates;
    std::atomic<bool> coarse_done{false};
};

using GISGroupStatePtr = std::shared_ptr<GISGroupState>;

// Coarse node: runs each predicate's R-Tree query once (segment-level), combines
// per is_and into coarse_candidates, and emits the per-batch slice. Scheduled
// EARLY (indexed bucket) so its bitmap prunes other predicates via bitmap_input.
class PhyGISCoarseConjunctExpr : public SegmentExpr {
 public:
    PhyGISCoarseConjunctExpr(GISGroupStatePtr state,
                             const std::string& name,
                             milvus::OpContext* op_ctx,
                             const segcore::SegmentInternalInterface* segment,
                             int64_t active_count,
                             int64_t batch_size,
                             int32_t consistency_level)
        : SegmentExpr({},
                      name,
                      op_ctx,
                      segment,
                      state->field_id,
                      /*nested_path=*/{},
                      DataType::GEOMETRY,
                      active_count,
                      batch_size,
                      consistency_level),
          st_(std::move(state)) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    std::string
    ToString() const override {
        return "PhyGISCoarseConjunctExpr";
    }

    // Self-managed segment-level cursor (independent of exec_path_, like
    // PhyLikeConjunctExpr): keeps the conjunction's SkipFollowingExprs in sync.
    void
    MoveCursor() override {
        current_pos_ += NextBatchSize();
    }

 private:
    int64_t
    NextBatchSize() const {
        auto remain = active_count_ - current_pos_;
        return remain < batch_size_ ? remain : batch_size_;
    }

    // Run the R-Tree index query for a single predicate, filling p.coarse.
    void
    RunRTreeQuery(GISGroupState::Pred& p);

    GISGroupStatePtr st_;
    int64_t current_pos_{0};
};

// Refine node: consumes bitmap_input (== scalars AND B_coarse), and for each
// surviving row constructs the row geometry ONCE and evaluates ALL predicates of
// the block against it (fusion, K->1). Scheduled LAST (heavy bucket).
class PhyGISRefineConjunctExpr : public SegmentExpr {
 public:
    PhyGISRefineConjunctExpr(GISGroupStatePtr state,
                             const std::string& name,
                             milvus::OpContext* op_ctx,
                             const segcore::SegmentInternalInterface* segment,
                             int64_t active_count,
                             int64_t batch_size,
                             int32_t consistency_level)
        : SegmentExpr({},
                      name,
                      op_ctx,
                      segment,
                      state->field_id,
                      /*nested_path=*/{},
                      DataType::GEOMETRY,
                      active_count,
                      batch_size,
                      consistency_level),
          st_(std::move(state)) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    std::string
    ToString() const override {
        return "PhyGISRefineConjunctExpr";
    }

    // Self-managed segment-level cursor (independent of exec_path_).
    void
    MoveCursor() override {
        current_pos_ += NextBatchSize();
    }

 private:
    int64_t
    NextBatchSize() const {
        auto remain = active_count_ - current_pos_;
        return remain < batch_size_ ? remain : batch_size_;
    }

    // Evaluate one predicate against an already-constructed left geometry using
    // a per-thread prepared query geometry (within/contains semantics swapped,
    // mirroring PhyGISFunctionFilterExpr::evaluate_geometry_prepared).
    bool
    EvalPrepared(proto::plan::GISFunctionFilterExpr_GISOp op,
                 const PreparedGeometry& prepared,
                 const Geometry& query_geom,
                 const Geometry& left) const;

    GISGroupStatePtr st_;
    int64_t current_pos_{0};
};

}  // namespace exec
}  // namespace milvus
