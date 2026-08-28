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

#include "exec/expression/GISConjunctExpr.h"

#include <atomic>
#include <mutex>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Geometry.h"
#include "common/GeometryCache.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "exec/expression/GISFunctionFilterExpr.h"
#include "geos_c.h"
#include "index/Index.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "knowhere/dataset.h"
#include "log/Log.h"
#include "monitor/Monitor.h"
#include "pb/schema.pb.h"

namespace milvus {
namespace exec {

namespace {

std::mutex&
GISGroupStateObserverMutex() {
    static std::mutex mu;
    return mu;
}

GISGroupStateObserver&
GISGroupStateObserverSlot() {
    static GISGroupStateObserver observer;
    return observer;
}

}  // namespace

void
SetGISGroupStateObserverForTest(GISGroupStateObserver observer) {
    std::lock_guard<std::mutex> lock(GISGroupStateObserverMutex());
    GISGroupStateObserverSlot() = std::move(observer);
}

GISGroupState::~GISGroupState() {
    // Expression construction does not imply execution: FilterBits may return
    // a cached result, an upstream conjunct may skip this group, or an
    // exception/cancellation may stop between batches. In those cases the
    // zero/partial counters are not segment-level pruning ratios and must not
    // enter the histograms.
    const bool reportable = active_count > 0 && coarse_done &&
                            coarse_cursor_complete && refine_cursor_complete;
    if (reportable) {
        const double denom = static_cast<double>(active_count);
        milvus::monitor::internal_core_gis_coarse_ratio.Observe(
            static_cast<double>(coarse_selected) / denom);
        milvus::monitor::internal_core_gis_refine_ratio.Observe(
            static_cast<double>(refined_rows) / denom);
        // Same numbers, greppable per segment. refined_rows is the one the
        // equivalence tests cannot see: a Refine that ignores bitmap_input and
        // evaluates every active row returns identical bits and only shows up
        // here, as a ratio approaching 1.
        LOG_DEBUG(
            "GIS split-fusion pruning: field {} coarse {}/{} refined {}/{} "
            "({} predicates)",
            field_id.get(),
            coarse_selected,
            active_count,
            refined_rows,
            active_count,
            preds.size());
    }

    GISGroupStateObserver observer;
    {
        std::lock_guard<std::mutex> lock(GISGroupStateObserverMutex());
        observer = GISGroupStateObserverSlot();
    }
    if (observer && reportable) {
        observer(*this);
    }
}

// -------------------------------------------------------------------------
// Coarse node: run each predicate's R-Tree query once (segment-level), combine
// per is_and, cache, and emit the per-batch slice.
// -------------------------------------------------------------------------
PhyGISCoarseConjunctExpr::CoarseOutcome
PhyGISCoarseConjunctExpr::RunRTreeQuery(GISGroupState::Pred& p) {
    // Mirrors PhyGISFunctionFilterExpr::EvalForIndexSegment's coarse query.
    // NOTE: on 2.6 the scalar index is pinned eagerly in SegmentExpr's
    // constructor (InitSegmentExpr), so there is no EnsurePinnedIndex() to call
    // here -- pinned_index_/num_index_chunk_ are already populated.
    using Index = index::ScalarIndex<std::string>;

    // p.has_index was sampled at compile time from segment_->HasIndex(), but
    // HasIndex() can report true while the index is still mid-load, so the
    // eager pin may have yielded nothing (num_index_chunk_ != 1) or a non-string
    // index (the dynamic_cast below returns nullptr). Unlike the baseline
    // DetermineExecPath(), this coarse path has no RawData fallback, so guard
    // both here and degrade to an all-set coarse bitmap in that window -- the
    // same behavior as the no-index path (p.has_index == false). The Refine
    // node still evaluates the exact predicate, so results stay correct; we
    // only lose R-Tree pruning for this segment while the index warms up.
    const Index* scalar_index =
        (num_index_chunk_ == 1 && !pinned_index_.empty())
            ? dynamic_cast<const Index*>(pinned_index_[0].get())
            : nullptr;
    if (scalar_index == nullptr) {
        // Degrade to an all-set coarse: results stay correct (Refine still
        // evaluates the exact predicate) but this segment loses R-Tree pruning.
        // The caller aggregates this across the group's predicates and warns
        // once per segment (see Eval), instead of once per predicate here.
        p.coarse = TargetBitmap(active_count_, true);
        return CoarseOutcome::kIndexUnusable;
    }

    // GEOS objects are bound to the per-thread context.
    GEOSContextHandle_t ctx = GetThreadLocalGEOSContext();
    Geometry query_geom(ctx, p.query_wkt.c_str());

    auto ds = std::make_shared<milvus::Dataset>();
    ds->Set(milvus::index::OPERATOR_TYPE, p.op);
    ds->Set(milvus::index::MATCH_VALUE, query_geom);

    auto* idx_ptr = const_cast<Index*>(scalar_index);
    auto tmp = idx_ptr->Query(ds);
    // Query() returns a bitmap sized index->Count() -- every row appended to
    // the index -- while Eval combines it into a candidate bitmap sized
    // active_count_, the MVCC-visible row count at the query timestamp. On a
    // growing segment with a geometry index the two diverge (the ingest path
    // appends to the index before acking rows, and a query ts below the
    // newest inserts lowers active_count_ further), and TargetBitmap's
    // operator&=/|= size check is a bare assert() that is compiled out under
    // NDEBUG. Normalize into active_count_ space: keep the first
    // active_count_ bits.
    if (static_cast<int64_t>(tmp.size()) > active_count_) {
        TargetBitmap sliced;
        sliced.append(tmp, 0, active_count_);
        p.coarse = std::move(sliced);
        return CoarseOutcome::kPruned;
    }
    // The reverse direction -- the index reporting FEWER rows than are
    // visible -- is unreachable on a growing segment (SegmentGrowingImpl
    // appends to the index before acking rows) but IS reachable on a sealed
    // one: an R-Tree built before empty/unparseable geometries were kept as
    // placeholder entries under-reports the row space, and its missing
    // entries are interior holes, not a trailing suffix. Padding only the
    // tail with 1s would leave those holes false and silently drop matching
    // rows from `survivors &= coarse_slice` in Refine -- while the very same
    // predicate issued alone would take the per-predicate path's self-heal
    // and return them. Apply the one shared rule instead: promote the whole
    // row space to candidates (coarse ⊇ exact holds trivially, Refine still
    // evaluates the exact predicate) and lose R-Tree pruning for this
    // segment. Same defensive posture as the pin-empty degrade above.
    if (PromoteShortGISCoarseBitmap(tmp, active_count_)) {
        p.coarse = std::move(tmp);
        return CoarseOutcome::kIndexShort;
    }
    p.coarse = std::move(tmp);
    return CoarseOutcome::kPruned;
}

void
PhyGISCoarseConjunctExpr::Eval(EvalCtx& context, VectorPtr& result) {
    // NOTE (2.6): master calls WaitPrefetch() here to close the pinned_index_
    // race against the prefetch-pool DetermineExecPath()/EnsurePinnedIndex().
    // 2.6 has neither -- the index is pinned eagerly in InitSegmentExpr on the
    // constructing thread -- so there is nothing to wait for.
    auto real_batch_size = NextBatchSize();
    if (real_batch_size == 0) {
        result = nullptr;
        return;
    }

    // Phase 1: build B_coarse once for the whole segment.
    if (!st_->coarse_done) {
        TargetBitmap cand(active_count_, st_->is_and);  // AND -> 1s / OR -> 0s
        int unusable_index = 0;  // preds whose R-Tree pin came up empty
        int short_index = 0;     // preds whose legacy R-Tree is short
        int no_index = 0;        // preds with no geometry index at all
        for (auto& p : st_->preds) {
            if (p.has_index) {
                switch (RunRTreeQuery(p)) {
                    case CoarseOutcome::kIndexUnusable:
                        ++unusable_index;
                        break;
                    case CoarseOutcome::kIndexShort:
                        ++short_index;
                        break;
                    case CoarseOutcome::kPruned:
                        break;
                }
            } else {
                // No R-Tree index: coarse degenerates to the full set; the
                // Refine node still prunes via bitmap_input and fuses
                // construction. Expected, so it is aggregated into a single
                // DEBUG line below rather than logged per predicate.
                p.coarse = TargetBitmap(active_count_, true);
                ++no_index;
            }
            if (st_->is_and) {
                cand &= p.coarse;
            } else {
                cand |= p.coarse;
            }
            // p.coarse has been merged into cand and is never read again; the
            // Refine node consumes the combined coarse_candidates, not the
            // per-predicate bitmaps. Release it now so we don't hold one extra
            // active_count_-bit bitmap per predicate for the whole query life.
            p.coarse = TargetBitmap{};
        }
        // One log per segment per query (this block is guarded by coarse_done),
        // NOT one per predicate. The unusable-index case is unexpected and
        // loses all R-Tree pruning for the segment while the index warms up --
        // or permanently, if it is broken -- so warn: nothing else in the
        // pipeline reports it and a degraded deployment would otherwise look
        // exactly like a healthy one. All preds share the field's index, so
        // num_index_chunk_/pinned_index_ reflect that shared pin state.
        if (unusable_index > 0) {
            LOG_WARN(
                "GIS coarse pruning degraded to full scan: field {} reports an "
                "index but the pin yielded no usable string index for {} of {} "
                "predicate(s) (num_index_chunk={}, pinned={}); results stay "
                "correct via Refine, R-Tree pruning is lost for this segment",
                st_->field_id.get(),
                unusable_index,
                st_->preds.size(),
                num_index_chunk_,
                pinned_index_.size());
        }
        // Same shape as the per-predicate path's short-index warning
        // (GISFunctionFilterExpr.cpp), throttled the same way: this is an
        // expected upgrade state, not a bug, but it silently costs a full
        // refinement per query until the index is rebuilt.
        if (short_index > 0) {
            static std::atomic<int64_t> last_short_index_log_us{0};
            if (ShouldLogGeometryThrottled(last_short_index_log_us)) {
                LOG_WARN(
                    "GIS coarse pruning degraded to full scan: the R-Tree "
                    "index for field {} reports fewer rows than the segment "
                    "holds ({}) for {} of {} predicate(s); the missing "
                    "entries may be interior holes, so every row is treated "
                    "as a candidate for exact refinement. This index "
                    "predates placeholder-MBR indexing of empty/unparseable "
                    "geometries -- rebuild it to restore pruning (further "
                    "occurrences suppressed briefly).",
                    st_->field_id.get(),
                    active_count_,
                    short_index,
                    st_->preds.size());
            }
        }
        if (no_index > 0) {
            LOG_DEBUG(
                "GIS coarse pruning unavailable: field {} has no geometry "
                "index "
                "for {} of {} predicate(s), coarse degenerates to the full set",
                st_->field_id.get(),
                no_index,
                st_->preds.size());
        }
        // Reported once per segment by ~GISGroupState, together with
        // refined_rows: an all-ones coarse still returns correct results, so
        // without that a permanently degraded deployment is indistinguishable
        // from a healthy one.
        st_->coarse_selected = static_cast<int64_t>(cand.count());
        st_->coarse_candidates =
            std::make_shared<TargetBitmap>(std::move(cand));
        st_->coarse_done = true;
    }

    // Phase 2: emit slice [current_pos_, +real_batch_size).
    TargetBitmap out;
    out.append(*st_->coarse_candidates, current_pos_, real_batch_size);
    // Intersect with any mask an OUTER conjunction supplied. Within this same AND
    // it is redundant -- the conjunction re-ANDs Coarse's output and rebuilds
    // bitmap_input from the accumulated result, so scalar siblings still reach
    // Refine either way. But when the split group is NESTED (e.g. under an OR
    // arm: `id < 100 OR (st_intersects(geo,P1) AND st_within(geo,P2))`), the
    // inner conjunction's accumulator starts fresh and Coarse -- bucketed first
    // as the indexed expr -- is the only node positioned to consume the outer
    // mask before it is overwritten. Sound in BOTH directions: a row masked off
    // is already settled by the parent (TRUE under an OR arm, FALSE under an AND
    // arm), so dropping it here cannot change the recombined three-valued result,
    // and it spares Refine from building geometries for rows the outer arm
    // already decided (also making internal_core_gis_refine_ratio reflect the
    // real pruned work for nested shapes). coarse_candidates itself is left pure
    // B_coarse -- only this per-batch slice is narrowed. See PR #50675 review.
    const auto& outer_mask = context.get_bitmap_input();
    if (!outer_mask.empty()) {
        AssertInfo(static_cast<int64_t>(outer_mask.size()) == real_batch_size,
                   "bitmap_input size {} != real_batch_size {}",
                   outer_mask.size(),
                   real_batch_size);
        out &= outer_mask;
    }
    // valid is all-ones intentionally (see also the Refine node). PRECONDITION:
    // these split nodes NEVER sit under a NOT and "null == not-selected" for
    // them. This holds because split is only applied INSIDE a pure conjunction
    // chain (ReorderConjunctExpr recurses only into PhyConjunctFilterExpr; NOT
    // compiles to PhyLogicalUnaryExpr), and because SupportOffsetInput() returns
    // false so the offset-input path never reorders them either. Under that
    // precondition the three-valued And/Or result bits never consume `valid`
    // (only Not does), and geometry null rows keep their res bit false on both
    // the baseline and the split path -- so the selection set is identical even
    // though `valid` here diverges from the baseline's not-null bitmap. If a
    // split group could ever land under a NOT, this all-ones `valid` would
    // wrongly select null rows and must be replaced by the real not-null bitmap.
    // See PR #50675 review.
    TargetBitmap valid(real_batch_size, true);

    result = std::make_shared<ColumnVector>(std::move(out), std::move(valid));
    MoveCursor();
}

// -------------------------------------------------------------------------
// Refine node: consume bitmap_input, construct each surviving row's geometry
// ONCE, evaluate ALL predicates against it (fusion).
// -------------------------------------------------------------------------
bool
PhyGISRefineConjunctExpr::EvalPrepared(
    proto::plan::GISFunctionFilterExpr_GISOp op,
    const PreparedGeometry& prepared,
    const Geometry& query_geom,
    const Geometry& left,
    GEOSContextHandle_t ctx) const {
    // Delegate to the shared helper so the prepared-predicate semantics (the
    // contains/within swap in particular) never drift from the per-predicate
    // path. DWithin is filtered out before grouping, so distance is unused here.
    // Equals is NOT filtered out, and it is the helper's unprepared fallback --
    // hence `ctx`, so it never drives GEOS through a cache-owned `left`'s
    // shared context.
    return EvaluateGISPreparedOp(
        op, prepared, query_geom, left, /*distance=*/0.0, ctx);
}

void
PhyGISRefineConjunctExpr::Eval(EvalCtx& context, VectorPtr& result) {
    // NOTE (2.6): master defines PrefetchRawData() above this and calls
    // WaitPrefetch() here. 2.6's SegmentExpr has no expression-level prefetch,
    // so both are omitted (see the header).
    auto real_batch_size = NextBatchSize();
    if (real_batch_size == 0) {
        result = nullptr;
        return;
    }
    const auto seg_offset = current_pos_;

    TargetBitmap res(real_batch_size, false);
    // valid_res is all-ones intentionally; see the PRECONDITION on the Coarse
    // node's `valid` above (split runs only inside pure conjunctions and never
    // under a NOT, so "null == not-selected" is safe and the result bits never
    // consume `valid` -- this divergence from the baseline's not-null bitmap is
    // unobservable in the selection set).
    TargetBitmap valid_res(real_batch_size, true);

    // Survivors = batch slice of (bitmap_input == scalars ∧ B_coarse) ∧ B_coarse.
    TargetBitmap survivors(real_batch_size, true);
    const auto& pre = context.get_bitmap_input();
    if (!pre.empty()) {
        AssertInfo(static_cast<int64_t>(pre.size()) == real_batch_size,
                   "bitmap_input size {} != real_batch_size {}",
                   pre.size(),
                   real_batch_size);
        survivors &= pre;
    }
    if (st_->coarse_candidates != nullptr) {
        // Redundant by construction TODAY: the Coarse node sits in an earlier
        // bucket, so the conjunction has already folded B_coarse into
        // bitmap_input by the time Refine runs, and `pre` above carries it.
        // Kept as the safety net for the day that stops being true (a bucket
        // change putting Refine ahead of Coarse would leave this as the only
        // application of B_coarse). Being redundant, it is also the one part
        // of the pruning contract no test can pin -- removing it changes
        // neither the result bits NOR refined_rows, verified by deleting it
        // and watching the whole suite, including the counter assertions, stay
        // green. Do not "cover" it with a test that cannot fail.
        TargetBitmap coarse_slice;
        coarse_slice.append(
            *st_->coarse_candidates, seg_offset, real_batch_size);
        survivors &= coarse_slice;
    }

    if (!survivors.none()) {
        // Build per-thread query geometries + prepared forms ONCE per batch.
        // qgeoms is reserved so it never reallocates (prepared references it).
        GEOSContextHandle_t qctx = GetThreadLocalGEOSContext();
        std::vector<Geometry> qgeoms;
        std::vector<PreparedGeometry> preps;
        qgeoms.reserve(st_->preds.size());
        preps.reserve(st_->preds.size());
        for (auto& p : st_->preds) {
            qgeoms.emplace_back(qctx, p.query_wkt.c_str());
            preps.emplace_back(qctx, qgeoms.back());
        }

        auto eval_all = [&](const Geometry& left) -> bool {
            bool bit = st_->is_and;
            for (size_t j = 0; j < st_->preds.size(); ++j) {
                bool r = EvalPrepared(
                    st_->preds[j].op, preps[j], qgeoms[j], left, qctx);
                bit = st_->is_and ? (bit && r) : (bit || r);
                if (st_->is_and != bit) {
                    break;  // short-circuit
                }
            }
            return bit;
        };

        // Collect surviving absolute offsets within this batch.
        std::vector<int64_t> hit_local;
        std::vector<int64_t> hit_abs;
        hit_local.reserve(survivors.count());
        hit_abs.reserve(survivors.count());
        // Accumulated across batches: the number of rows this node actually
        // builds a geometry for and evaluates. This is the pruning contract
        // made observable -- see GISGroupState::refined_rows.
        st_->refined_rows += static_cast<int64_t>(survivors.count());
        for (int64_t i = 0; i < real_batch_size; ++i) {
            if (survivors[i]) {
                hit_local.emplace_back(i);
                hit_abs.emplace_back(seg_offset + i);
            }
        }

        // shared_ptr: an in-flight query keeps the published cache snapshot
        // alive across a concurrent sealed-segment reopen/drop.
        auto geometry_cache = segment_->GetGeometryCache(st_->field_id);

        if (geometry_cache) {
            auto cache_lock = geometry_cache->AcquireReadLock();
            for (size_t k = 0; k < hit_abs.size(); ++k) {
                auto cached = geometry_cache->GetByOffsetUnsafe(hit_abs[k]);
                if (cached == nullptr) {
                    continue;  // null/invalid geometry -> false
                }
                if (eval_all(*cached)) {
                    res.set(hit_local[k]);
                }
            }
        } else {
            // No geometry cache: fetch WKB once and construct each row geometry
            // ONCE, then evaluate all predicates against it (the K->1 win).
            // Thread the SegmentExpr's op_ctx_ (from qc->get_op_context()) so
            // tracing and tiered-storage accounting are preserved, matching the
            // other data-fetch paths in this expr rather than a bare local one.
            auto data_array = segment_->bulk_subscript(
                op_ctx_, st_->field_id, hit_abs.data(), hit_abs.size());
            auto geometry_array =
                static_cast<const milvus::proto::schema::GeometryArray*>(
                    &data_array->scalars().geometry_data());
            const auto& vd = data_array->valid_data();
            GEOSContextHandle_t local_ctx = GetThreadLocalGEOSContext();
            for (size_t k = 0; k < hit_abs.size(); ++k) {
                if (!vd.empty() && !vd[k]) {
                    continue;
                }
                const auto& wkb = geometry_array->data(k);
                // Tolerant parse, matching the per-predicate path: a non-null
                // row whose WKB is empty or corrupt is now KEPT by both write
                // paths and indexed with a placeholder MBR, so it does reach
                // refinement whenever the query bbox covers the origin. The
                // throwing Geometry(ctx, wkb) ctor would fail the entire query
                // on such a row; it can never satisfy the predicate, so
                // evaluate it to false instead.
                Geometry left;
                if (!left.TryParseFromWkb(local_ctx, wkb.data(), wkb.size())) {
                    continue;
                }
                if (eval_all(left)) {
                    res.set(hit_local[k]);
                }
            }
        }
    }

    result =
        std::make_shared<ColumnVector>(std::move(res), std::move(valid_res));
    MoveCursor();
}

}  // namespace exec
}  // namespace milvus
