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
#include "common/GeometryCache.h"
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
//
// `ctx` MUST be the calling thread's GEOS context (GetThreadLocalGEOSContext).
// The unprepared fallbacks (Equals, DWithin) would otherwise drive GEOS through
// `left`'s own stored context, and `left` is frequently a cache-owned Geometry
// whose context is shared by every concurrent query on that segment+field — a
// GEOS context is not thread-safe, so that is a data race. The prepared
// predicates are unaffected: they run on `prepared`'s context, which the caller
// already built on its own thread.
inline bool
EvaluateGISPreparedOp(proto::plan::GISFunctionFilterExpr_GISOp op,
                      const PreparedGeometry& prepared,
                      const Geometry& query_geom,
                      const Geometry& left,
                      double distance,
                      GEOSContextHandle_t ctx) {
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
            // No prepared version - fall back to regular geometry, on the
            // caller's per-thread context (see the note on `ctx` above).
            return left.equals(query_geom, ctx);
        case proto::plan::GISFunctionFilterExpr_GISOp_DWithin:
            // Distance-based operation - no prepared version; same per-thread
            // context requirement as Equals above.
            return left.dwithin(query_geom, distance, ctx);
        default:
            ThrowInfo(
                NotImplemented, "unknown GIS op : {}", static_cast<int>(op));
    }
}

// Promote a SHORT R-Tree coarse bitmap to the full active row space.
//
// The R-Tree Query() bitmap is sized by the index row count (Count()), while
// callers combine it in the segment's active row space. When Count() <
// active_count, the index predates placeholder-MBR indexing of
// empty/unparseable geometries: those old builders advanced absolute_offset
// even when they dropped a row, so the missing entries are INTERIOR holes, not
// a trailing suffix. Count() reveals how many entries are missing, not where.
// Once the index is short, therefore, no `false` bit in it is trustworthy as a
// negative -- padding only the tail with 1s (resize(active_count, true)) would
// leave interior holes false and silently drop matching rows. The only safe
// coarse for such an index is the full row space; exact refinement settles it.
//
// Both consumers of an R-Tree coarse bitmap -- the per-predicate path
// (PhyGISFunctionFilterExpr::EvalForIndexSegment) and the optimizer's fusion
// path (PhyGISCoarseConjunctExpr::RunRTreeQuery) -- MUST route through this
// helper so the short-index rule cannot drift between them again.
//
// Returns true when `coarse` was short and has been promoted; false when it
// already spanned (at least) `active_count` rows and was left untouched.
// Validity is deliberately NOT handled here: the per-predicate path needs
// IsNotNull(active_count) (absolute null offsets survive independently of the
// short entry count) while the fusion path derives nullness in Refine.
inline bool
PromoteShortGISCoarseBitmap(TargetBitmap& coarse, int64_t active_count) {
    if (static_cast<int64_t>(coarse.size()) >= active_count) {
        return false;
    }
    coarse = TargetBitmap(active_count, true);
    return true;
}

// Evaluates `op` with `left` as the row geometry and `*right` as the query
// geometry, on the caller's per-thread GEOS context. `right` is unused (may be
// nullptr) for STIsValid.
inline bool
EvaluateGISUnpreparedOp(proto::plan::GISFunctionFilterExpr_GISOp op,
                        const Geometry& left,
                        const Geometry* right,
                        double distance,
                        GEOSContextHandle_t ctx) {
    switch (op) {
        case proto::plan::GISFunctionFilterExpr_GISOp_Equals:
            return left.equals(*right, ctx);
        case proto::plan::GISFunctionFilterExpr_GISOp_Touches:
            return left.touches(*right, ctx);
        case proto::plan::GISFunctionFilterExpr_GISOp_Overlaps:
            return left.overlaps(*right, ctx);
        case proto::plan::GISFunctionFilterExpr_GISOp_Crosses:
            return left.crosses(*right, ctx);
        case proto::plan::GISFunctionFilterExpr_GISOp_Contains:
            return left.contains(*right, ctx);
        case proto::plan::GISFunctionFilterExpr_GISOp_Intersects:
            return left.intersects(*right, ctx);
        case proto::plan::GISFunctionFilterExpr_GISOp_Within:
            return left.within(*right, ctx);
        case proto::plan::GISFunctionFilterExpr_GISOp_DWithin:
            return left.dwithin(*right, distance, ctx);
        case proto::plan::GISFunctionFilterExpr_GISOp_STIsValid:
            return left.is_valid(ctx);
        default:
            ThrowInfo(NotImplemented,
                      "internal error: unknown GIS op : {}",
                      static_cast<int>(op));
    }
}

// Exact GIS predicate over raw WKB rows. With a geometry cache, rows are
// resolved by absolute segment offset; without one, the WKB is parsed per row.
// Empty or unparseable geometries are FALSE.
template <typename T>  // std::string (growing, no mmap) or std::string_view
struct GeometryScanKernel {
    static constexpr bool kNeedsSegmentOffsets = true;

    proto::plan::GISFunctionFilterExpr_GISOp op;
    const Geometry* right_source;  // nullptr for STIsValid
    double distance;
    std::shared_ptr<SimpleGeometryCache> geometry_cache;  // may be nullptr

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<T>& b, TriStateOut out) const {
        AssertInfo(b.segment_offsets != nullptr,
                   "segment_offsets should not be nullptr");
        const bool has_candidates = !b.candidates.empty();
        // Cache-owned geometries share one GEOS context; drive the predicate
        // on a per-thread context so concurrent read-locked queries never touch
        // the same non-thread-safe context. A throwing row cannot leak a
        // per-batch GEOS_init_r context either.
        GEOSContextHandle_t tls_ctx = GetThreadLocalGEOSContext();
        if (geometry_cache) {
            auto cache_lock = geometry_cache->AcquireReadLock();
            for (size_t i = 0; i < b.size; ++i) {
                if ((b.validity && !b.validity[i]) ||
                    (has_candidates && !b.candidates[i])) {
                    continue;
                }
                const auto* cached =
                    geometry_cache->GetByOffsetUnsafe(b.segment_offsets[i]);
                // nullptr = empty/corrupt placeholder row (the write paths keep
                // such rows, see SimpleGeometryCache::AppendDataAt); it can
                // never satisfy the predicate.
                if (cached == nullptr) {
                    continue;
                }
                if (EvaluateGISUnpreparedOp(
                        op, *cached, right_source, distance, tls_ctx)) {
                    out.SetTrue(i);
                }
            }
            return;
        }
        for (size_t i = 0; i < b.size; ++i) {
            if ((b.validity && !b.validity[i]) ||
                (has_candidates && !b.candidates[i])) {
                continue;
            }
            // TryParseFromWkb throws only on pre-parse allocation failure; a
            // corrupt/placeholder WKB row -- or a GEOS-swallowed parse-time OOM,
            // indistinguishable from it (see the KNOWN LIMIT note on
            // TryParseFromWkb) -- evaluates to FALSE, matching the cache branch.
            Geometry left;
            if (!left.TryParseFromWkb(
                    tls_ctx, b.data[i].data(), b.data[i].size())) {
                continue;
            }
            if (EvaluateGISUnpreparedOp(
                    op, left, right_source, distance, tls_ctx)) {
                out.SetTrue(i);
            }
        }
    }
};

static_assert(kKernelNeedsSegmentOffsets<GeometryScanKernel<std::string>> &&
              kKernelNeedsSegmentOffsets<GeometryScanKernel<std::string_view>>);

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

    // The index path ignores the offset-input list, and the raw path needs
    // segment offsets that the offset readers do not provide, so the GIS filter
    // cannot serve the offset-input (iterative-filter / rescore) path. Report
    // false so IterativeFilterNode takes its non-native fallback instead of
    // feeding offsets into Eval.
    bool
    SupportOffsetInput() override {
        return false;
    }

    // A skipped batch (conjunct short-circuit via SkipFollowingExprs) must
    // still advance this expression's cursors, otherwise it desynchronizes
    // from its sibling expressions and later batches evaluate the wrong rows.
    // The base MoveCursor() covers every case except the growing interim-index
    // path. EvalForIndexSegment() advances both the global index position and
    // the legacy data chunk cursor, so a skipped batch must mirror both
    // updates here.
    void
    MoveCursor() override {
        if (has_offset_input_ || execute_all_at_once_) {
            return;
        }
        if (UseIndexCursor() && segment_->type() != SegmentType::Sealed) {
            MoveCursorForIndex();
            MoveCursorForData();
            return;
        }
        SegmentExpr::MoveCursor();
    }

 private:
    VectorPtr
    EvalForIndexSegment();

    VectorPtr
    EvalForDataSegment(EvalCtx& context);

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
