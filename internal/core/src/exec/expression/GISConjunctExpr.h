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

#include <functional>
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
        // An all-ones coarse is always CORRECT -- Refine still evaluates the
        // exact predicate -- but it prunes nothing, so the pruning gain
        // silently drops to zero. That is surfaced two ways instead of a
        // per-predicate flag: the refine ratio metric (a ratio near 1 means
        // coarse stopped pruning) and, for the unexpected "index present but
        // unusable" case, a single per-segment LOG_WARN raised by the Coarse
        // node after combining B_coarse.
        TargetBitmap coarse;
    };

    FieldId field_id;
    // Block combine op: true => AND (intersect coarse / && refine),
    //                   false => OR (union coarse / || refine).
    bool is_and{true};
    std::vector<Pred> preds;
    // Denominator of both pruning ratios below: the segment's MVCC-visible row
    // count, captured where the nodes are built (SplitFuseGISConjunct) so it is
    // available even if neither node ever runs.
    int64_t active_count{0};

    // B_coarse = combine(Ci) over all preds. Filled by the Coarse node, read by
    // the Refine node. Cached once per segment for the whole query.
    std::shared_ptr<TargetBitmap> coarse_candidates;
    // A fresh GISGroupState is created per segment (SplitFuseGISConjunct) and
    // per-segment batch execution is single-threaded, so this guard needs no
    // atomicity -- it is only read/written inside PhyGISCoarseConjunctExpr::Eval.
    bool coarse_done{false};

    // A state may be destroyed without being evaluated to completion: the
    // FilterBits result cache can bypass the expression tree entirely,
    // cancellation/errors can stop between batches, and an upstream conjunct
    // can skip both GIS nodes. Destructor-based metrics are valid only after
    // both node cursors account for the whole segment and Coarse actually
    // computed B_coarse. MoveCursor maintains these flags for both evaluated
    // and short-circuited batches.
    bool coarse_cursor_complete{false};
    bool refine_cursor_complete{false};

    // --- pruning observability -------------------------------------------
    // The point of split-fusion is that Refine only evaluates survivors. That
    // contract is invisible to ON/OFF equivalence tests: making Refine
    // evaluate every active row keeps every result bit identical and only
    // costs performance. These counters carry the contract out of the operator
    // -- see the destructor, which is the single place that reads them.
    //
    // Rows set in B_coarse after combining every predicate.
    int64_t coarse_selected{0};
    // Survivors of (scalars AND B_coarse) that Refine fetched, i.e. the rows it
    // paid to look at. This is the fetch cost the split exists to shrink and
    // must stay well below active_count for a selective query. It counts every
    // fetched survivor, including null/invalid geometries that are skipped
    // before any exact predicate runs -- Refine still fetched them, so they are
    // part of the cost being measured; the exact predicate (and the geometry
    // construction it needs) runs for the non-null subset.
    int64_t refined_rows{0};

    // Reports the two counters above as ratios of active_count, but only when
    // coarse_done and both cursor-complete flags prove every batch has been
    // accounted for. Defined out-of-line so the Prometheus headers stay out of
    // this one.
    ~GISGroupState();
};

using GISGroupStatePtr = std::shared_ptr<GISGroupState>;

// Test-only hook, invoked from ~GISGroupState alongside the metric emission so
// tests observe exactly what production reports. Production never sets it.
// Pass nullptr to clear. Set it before the query runs, from one thread.
using GISGroupStateObserver = std::function<void(const GISGroupState&)>;
void
SetGISGroupStateObserverForTest(GISGroupStateObserver observer);

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

    // The split nodes slice their output by the self-managed segment cursor
    // (current_pos_ / NextBatchSize over active_count_), NOT by an external
    // offset list. They therefore CANNOT serve the offset-input
    // (iterative-filter / rescore) path; reporting false makes
    // IterativeFilterNode fall back to its non-native execution instead of
    // feeding offsets into Eval (which would mis-size the result bitmap).
    bool
    SupportOffsetInput() override {
        return false;
    }

    // Self-managed segment-level cursor (independent of exec_path_, like
    // PhyLikeConjunctExpr): keeps the conjunction's SkipFollowingExprs in sync.
    void
    MoveCursor() override {
        current_pos_ += NextBatchSize();
        st_->coarse_cursor_complete = current_pos_ == active_count_;
    }

    // NOTE (2.6): the master PR overrides PrefetchRawData() here to a no-op and
    // documents why CanExecuteAllAtOnce() is deliberately NOT overridden. Neither
    // applies on 2.6: SegmentExpr has no lazy exec-path / prefetch machinery
    // (PrefetchRawData, WaitPrefetch, DetermineExecPath) and no all-at-once path
    // (CanExecuteAllAtOnce / SetExecuteAllAtOnce), so this node is always batched
    // and never prefetches. Both overrides come back with that machinery.

 private:
    int64_t
    NextBatchSize() const {
        auto remain = active_count_ - current_pos_;
        return remain < batch_size_ ? remain : batch_size_;
    }

    // Outcome of one predicate's R-Tree coarse query. Anything but kPruned
    // means p.coarse degraded to an all-set bitmap (results stay correct via
    // Refine; R-Tree pruning is lost for the segment); the two degrade causes
    // are kept apart so the caller can raise one accurately-worded per-segment
    // warning for each, instead of one per predicate.
    enum class CoarseOutcome {
        kPruned,         // index answered; p.coarse is a real candidate set
        kIndexUnusable,  // index reported present but the pin yielded none
        kIndexShort,     // legacy index shorter than active_count_ (see
                         // PromoteShortGISCoarseBitmap)
    };

    // Run the R-Tree index query for a single predicate, filling p.coarse.
    CoarseOutcome
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

    // See PhyGISCoarseConjunctExpr::SupportOffsetInput: the Refine node also
    // slices by its own segment cursor and asserts bitmap_input is sized by
    // real_batch_size, so it cannot consume an external offset list.
    bool
    SupportOffsetInput() override {
        return false;
    }

    // Self-managed segment-level cursor (independent of exec_path_).
    void
    MoveCursor() override {
        current_pos_ += NextBatchSize();
        st_->refine_cursor_complete = current_pos_ == active_count_;
    }

    // NOTE (2.6): the master PR overrides DetermineExecPath() (commit to
    // RawData so no R-Tree cell is pinned) and PrefetchRawData() (warm the
    // column only when reads will span it) here, plus a comment on why
    // CanExecuteAllAtOnce() stays inherited. None of that machinery exists on
    // 2.6 -- the scalar index is pinned eagerly in InitSegmentExpr, there is no
    // prefetch pool for expressions, and there is no all-at-once path -- so the
    // overrides are omitted. They come back with that machinery.

 private:
    int64_t
    NextBatchSize() const {
        auto remain = active_count_ - current_pos_;
        return remain < batch_size_ ? remain : batch_size_;
    }

    // Evaluate one predicate against an already-constructed left geometry using
    // a per-thread prepared query geometry (within/contains semantics swapped,
    // mirroring PhyGISFunctionFilterExpr::evaluate_geometry_prepared).
    // `ctx` must be the calling thread's GEOS context: `left` may be a
    // cache-owned geometry whose context is shared across concurrent queries.
    bool
    EvalPrepared(proto::plan::GISFunctionFilterExpr_GISOp op,
                 const PreparedGeometry& prepared,
                 const Geometry& query_geom,
                 const Geometry& left,
                 GEOSContextHandle_t ctx) const;

    GISGroupStatePtr st_;
    int64_t current_pos_{0};
};

}  // namespace exec
}  // namespace milvus
