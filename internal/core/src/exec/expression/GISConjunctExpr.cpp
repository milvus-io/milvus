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

#include <vector>

#include "common/EasyAssert.h"
#include "common/GeometryCache.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "exec/expression/GISFunctionFilterExpr.h"
#include "geos_c.h"
#include "index/Index.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "knowhere/dataset.h"
#include "pb/schema.pb.h"

namespace milvus {
namespace exec {

// -------------------------------------------------------------------------
// Coarse node: run each predicate's R-Tree query once (segment-level), combine
// per is_and, cache, and emit the per-batch slice.
// -------------------------------------------------------------------------
void
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
        p.coarse = TargetBitmap(active_count_, true);
        return;
    }

    // GEOS objects are bound to the per-thread context.
    GEOSContextHandle_t ctx = GetThreadLocalGEOSContext();
    Geometry query_geom(ctx, p.query_wkt.c_str());

    auto ds = std::make_shared<milvus::Dataset>();
    ds->Set(milvus::index::OPERATOR_TYPE, p.op);
    ds->Set(milvus::index::MATCH_VALUE, query_geom);

    auto* idx_ptr = const_cast<Index*>(scalar_index);
    auto tmp = idx_ptr->Query(ds);
    p.coarse = std::move(tmp);
}

void
PhyGISCoarseConjunctExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto real_batch_size = NextBatchSize();
    if (real_batch_size == 0) {
        result = nullptr;
        return;
    }

    // Phase 1: build B_coarse once for the whole segment.
    if (!st_->coarse_done) {
        TargetBitmap cand(active_count_, st_->is_and);  // AND -> 1s / OR -> 0s
        for (auto& p : st_->preds) {
            if (p.has_index) {
                RunRTreeQuery(p);
            } else {
                // No R-Tree index: coarse degenerates to the full set; the
                // Refine node still prunes via bitmap_input and fuses
                // construction.
                p.coarse = TargetBitmap(active_count_, true);
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
        st_->coarse_candidates =
            std::make_shared<TargetBitmap>(std::move(cand));
        st_->coarse_done = true;
    }

    // Phase 2: emit slice [current_pos_, +real_batch_size).
    TargetBitmap out;
    out.append(*st_->coarse_candidates, current_pos_, real_batch_size);
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

    MoveCursor();
    result = std::make_shared<ColumnVector>(std::move(out), std::move(valid));
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
    const Geometry& left) const {
    // Delegate to the shared helper so the prepared-predicate semantics (the
    // contains/within swap in particular) never drift from the per-predicate
    // path. DWithin is filtered out before grouping, so distance is unused here.
    return EvaluateGISPreparedOp(
        op, prepared, query_geom, left, /*distance=*/0.0);
}

void
PhyGISRefineConjunctExpr::Eval(EvalCtx& context, VectorPtr& result) {
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
                bool r =
                    EvalPrepared(st_->preds[j].op, preps[j], qgeoms[j], left);
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
        for (int64_t i = 0; i < real_batch_size; ++i) {
            if (survivors[i]) {
                hit_local.emplace_back(i);
                hit_abs.emplace_back(seg_offset + i);
            }
        }

        auto* geometry_cache = SimpleGeometryCacheManager::Instance().GetCache(
            segment_->get_segment_id(), st_->field_id);

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
                Geometry left(local_ctx, wkb.data(), wkb.size());
                if (eval_all(left)) {
                    res.set(hit_local[k]);
                }
            }
        }
    }

    MoveCursor();
    result =
        std::make_shared<ColumnVector>(std::move(res), std::move(valid_res));
}

}  // namespace exec
}  // namespace milvus
