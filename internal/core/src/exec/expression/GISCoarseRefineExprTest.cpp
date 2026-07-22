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

// Equivalence tests for the GIS coarse/refine split + same-column fusion
// optimization (queryNode.segcore.enableGISSplitFusion). For every filter that
// contains same-column geometry predicates, evaluating with the flag ON (split
// + fusion path) must yield exactly the same bitset as evaluating with the flag
// OFF (the original per-predicate PhyGISFunctionFilterExpr path).

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "ExprTestBase.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/GeometryCache.h"
#include "common/IndexMeta.h"
#include "exec/QueryContext.h"
#include "index/Meta.h"
#include "exec/expression/Expr.h"
#include "exec/expression/GISConjunctExpr.h"
#include "plan/PlanNode.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {

BitsetType
RunFilter(const std::shared_ptr<Schema>& schema,
          ScopedSchemaHandle& handle,
          const SegmentInternalInterface* seg,
          int64_t N,
          const std::string& expr) {
    auto bin = handle.ParseSearch(
        expr, "vec", 5, knowhere::metric::L2, R"({"nprobe":10})", 3);
    auto plan = CreateSearchPlanByExpr(schema, bin.data(), bin.size());
    return ExecuteQueryExpr(
        plan->plan_node_->plannodes_->sources()[0]->sources()[0],
        seg,
        N,
        MAX_TIMESTAMP);
}

// RAII guard so the global segcore flag is always restored, even on failure.
struct GISSplitFusionGuard {
    explicit GISSplitFusionGuard(bool enable) {
        SegcoreConfig::default_config().set_enable_gis_split_fusion(enable);
    }
    ~GISSplitFusionGuard() {
        SegcoreConfig::default_config().set_enable_gis_split_fusion(false);
    }
};

// Captures what ~GISGroupState reports -- the same two counters the
// internal_core_gis_{coarse,refine}_ratio metrics carry -- so a test can assert
// the pruning contract itself instead of only the result bits, which are
// identical whether or not Refine prunes. One snapshot per group per segment.
struct GISGroupStateCapture {
    struct Snapshot {
        int64_t active_count;
        int64_t coarse_selected;
        int64_t refined_rows;
    };
    std::vector<Snapshot> snapshots;

    GISGroupStateCapture() {
        milvus::exec::SetGISGroupStateObserverForTest(
            [this](const milvus::exec::GISGroupState& st) {
                snapshots.push_back(
                    {st.active_count, st.coarse_selected, st.refined_rows});
            });
    }
    ~GISGroupStateCapture() {
        milvus::exec::SetGISGroupStateObserverForTest(nullptr);
    }
};

// RAII guard for the geometry-cache flag. Must be set BEFORE the segment is
// loaded, because the cache is populated at field-load time
// (ChunkedSegmentSealedImpl::LoadFieldData).
struct GeometryCacheGuard {
    explicit GeometryCacheGuard(bool enable) {
        SegcoreConfig::default_config().set_enable_geometry_cache(enable);
    }
    ~GeometryCacheGuard() {
        SegcoreConfig::default_config().set_enable_geometry_cache(false);
    }
};

// RAII guard for the expr batch size, restored on scope exit. Used to force
// multiple Eval batches over a single segment so the split nodes' per-batch
// coarse slicing + dual-cursor advance is exercised across batch boundaries.
struct ExprBatchSizeGuard {
    int64_t saved;
    explicit ExprBatchSizeGuard(int64_t batch_size)
        : saved(EXEC_EVAL_EXPR_BATCH_SIZE.load()) {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(batch_size);
    }
    ~ExprBatchSizeGuard() {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved);
    }
};

// Filter shapes exercised by every equivalence test below.
const std::vector<std::string>&
EquivExprs() {
    static const std::vector<std::string> exprs = {
        // (1) single GIS leaf under AND with a scalar predicate
        R"expr(age >= 0 and st_intersects(geo, "POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))"))expr",
        // (2) OR-group of same-field GIS under AND (Shape B)
        R"expr(age >= 0 and (st_intersects(geo, "POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))") or st_intersects(geo, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")))expr",
        // (3) same-field AND group (intersects + within)
        R"expr(st_intersects(geo, "POLYGON((-50 -50, 50 -50, 50 50, -50 50, -50 -50))") and st_within(geo, "POLYGON((-100 -100, 100 -100, 100 100, -100 100, -100 -100))"))expr",
        // (4) within op combined with a scalar predicate
        R"expr(age >= 0 and st_within(geo, "POLYGON((-100 -100, 100 -100, 100 100, -100 100, -100 -100))"))expr",
        // (5) three same-field predicates mixed with a scalar
        R"expr(age >= 0 and st_intersects(geo, "POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))") and st_within(geo, "POLYGON((-100 -100, 100 -100, 100 100, -100 100, -100 -100))"))expr",
        // (6) single GIS only (no conjunction -> fusion must be a no-op)
        R"expr(st_intersects(geo, "POINT(0 0)"))expr",
        // (7) STIsValid (unary, empty query WKT, RawData-only) under AND with a
        // scalar. STIsValid MUST NOT be pulled into the GIS direct-fusion group
        // (it has no prepared-op case and an empty WKT) -- this case crashed
        // before as_groupable_gis became a whitelist (PR #50675 review).
        R"expr(st_isvalid(geo) and age >= 0)expr",
        // (8) STIsValid mixed with a groupable GIS leaf on the SAME field: the
        // groupable intersects must split/fuse while STIsValid stays on the
        // baseline path. Exactly the "st_isvalid(geo) AND st_intersects(...)"
        // shape called out as the high-severity crash.
        R"expr(st_isvalid(geo) and st_intersects(geo, "POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))"))expr",
        // (9) direct AND-leaf + Shape-B subgroup on the SAME field: `geo`
        // appears both as a direct conjunction leaf (st_within) and inside an
        // OR subgroup (Shape B). Per the NOTE in Expr.cpp the rewrite emits two
        // independent coarse/refine pairs for that field, so this dual-pair
        // path is the trickiest one -- pin it down with an ON-vs-OFF case.
        R"expr(st_within(geo, "POLYGON((-100 -100, 100 -100, 100 100, -100 100, -100 -100))") and (st_intersects(geo, "POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))") or st_intersects(geo, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")))expr",
        // (10) DWithin mixed with a groupable GIS leaf on the SAME field.
        // DWithin must stay on the baseline path: the fusion group drops its
        // distance (Pred carries none, EvalPrepared hardcodes 0.0) and
        // RunRTreeQuery skips the coarse bbox expansion
        // (create_bounding_box_for_dwithin), so a grouped DWithin would
        // silently under-match -- the quiet failure mode the as_groupable_gis
        // whitelist exists to prevent, pinned here so a future whitelist edit
        // fails this equivalence instead of going green. Unlike shape (8),
        // the baseline leaf here also queries the R-Tree, so this is the one
        // shape where an R-Tree-pinning baseline node and the split pair
        // coexist on one field. The 5,000,000 m geodesic radius matches a
        // meaningful minority of the globally-spread DataGen rows, keeping
        // the shape discriminating (a ~10 m radius would select nothing and
        // the ON-vs-OFF comparison would degenerate to 0 == 0).
        R"expr(st_dwithin(geo, "POINT(0 0)", 5000000) and st_intersects(geo, "POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))"))expr",
        // (11) SELECTIVE scalar upstream. Every `age >= 0` above selects all N
        // rows: DataGen fills a non-random INT64 field with `data[i] = i /
        // repeat_count` and repeat_count defaults to 1 (DataGen.h), so the
        // predicate is a tautology and `survivors &= pre` in the Refine node
        // (GISConjunctExpr.cpp) is a permanent no-op in the scalar direction.
        // This shape makes the scalar mask actually prune, so the AND of
        // scalars and B_coarse is exercised for real.
        // Kept in sync with kSelectiveAgeThreshold below.
        R"expr(age >= 900 and st_intersects(geo, "POLYGON((-50 -50, 50 -50, 50 50, -50 50, -50 -50))"))expr",
    };
    return exprs;
}

// Row threshold used by shape (11); with the standard N=1000 segments in this
// file it leaves 100 of 1000 rows for Refine.
constexpr int64_t kSelectiveAgeThreshold = 900;

// Shape (11)'s scalar mask only proves anything if it is genuinely selective
// AND the geometry predicate keeps some of what survives. Pin both ends:
// a result of 0 or N would make the ON-vs-OFF comparison degenerate to
// "0 == 0" / "all == all" and stop discriminating, exactly like the
// tautological `age >= 0` shapes it was added to compensate for.
void
AssertSelectiveShapeIsDiscriminating(const std::shared_ptr<Schema>& schema,
                                     ScopedSchemaHandle& handle,
                                     const SegmentInternalInterface* seg,
                                     int64_t N) {
    const auto& e = EquivExprs().back();
    GISGroupStateCapture capture;
    GISSplitFusionGuard on(true);
    auto res = RunFilter(schema, handle, seg, N, e);
    ASSERT_EQ(res.size(), static_cast<size_t>(N));
    auto hits = res.count();
    EXPECT_GT(hits, 0u) << "selective shape selected nothing, it no longer "
                           "discriminates: "
                        << e;
    EXPECT_LT(hits, static_cast<size_t>(N))
        << "selective shape selected every row, the scalar mask is not "
           "pruning: "
        << e;
    // The scalar predicate alone bounds the RESULT. This says nothing about
    // whether Refine pruned -- the outer conjunction re-ANDs `age >= 900`
    // anyway, so it holds even for a Refine that exact-evaluates every active
    // row. It only pins that the shape stays selective.
    EXPECT_LE(hits, static_cast<size_t>(N - kSelectiveAgeThreshold))
        << "more rows survived than the scalar predicate admits, so the shape "
           "is no longer selective: "
        << e;

    // The part the result bits cannot show: how many rows Refine actually
    // built a geometry for. Dropping `survivors &= pre` or
    // `survivors &= coarse_slice` (GISConjunctExpr.cpp) leaves every result
    // bit identical and is visible ONLY here.
    ASSERT_EQ(capture.snapshots.size(), 1u)
        << "expected exactly one GIS split-fusion group for shape: " << e;
    const auto& s = capture.snapshots.front();
    EXPECT_EQ(s.active_count, N);
    EXPECT_GT(s.refined_rows, 0)
        << "Refine evaluated nothing; the split path did not run";
    EXPECT_LE(s.refined_rows, N - kSelectiveAgeThreshold)
        << "Refine evaluated more rows than the scalar mask admits ("
        << s.refined_rows << " > " << (N - kSelectiveAgeThreshold)
        << "); `survivors &= pre` is not reaching Refine";
    EXPECT_LE(s.refined_rows, s.coarse_selected)
        << "Refine evaluated more rows than B_coarse selected ("
        << s.refined_rows << " > " << s.coarse_selected
        << "); `survivors &= coarse_slice` is not reaching Refine";
    // Every selected row must have been refined: the result is a subset of
    // what Refine looked at.
    EXPECT_GE(static_cast<size_t>(s.refined_rows), hits)
        << "result has rows Refine never evaluated";
}

// The coarse half of the pruning contract, on a segment with a REAL R-Tree
// (elsewhere in this file coarse_candidates is all-ones, so there is nothing to
// observe). The scalar predicate is deliberately tautological (`age >= 0`), so
// B_coarse is the only thing that can prune and the refine bound below cannot
// be satisfied via the scalar mask instead.
//
// What this pins: that the R-Tree coarse pass really ran and really narrowed
// the candidate set (a degrade to all-ones returns correct results and would
// otherwise be invisible), and that Refine evaluated no more rows than
// B_coarse admitted.
//
// What it deliberately does NOT pin: the `survivors &= coarse_slice` line in
// PhyGISRefineConjunctExpr. That AND is redundant while Coarse is bucketed
// ahead of Refine -- B_coarse reaches Refine through bitmap_input either way --
// so deleting it leaves both the result bits and refined_rows unchanged.
// Verified by deleting it: the entire suite, these assertions included, stays
// green. See the comment at that line.
constexpr const char* kCoarsePruningExpr =
    R"expr(age >= 0 and st_intersects(geo, "POLYGON((-50 -50, 50 -50, 50 50, -50 50, -50 -50))"))expr";

void
AssertCoarseMaskActuallyPrunes(const std::shared_ptr<Schema>& schema,
                               ScopedSchemaHandle& handle,
                               const SegmentInternalInterface* seg,
                               int64_t N) {
    GISGroupStateCapture capture;
    GISSplitFusionGuard on(true);
    auto res = RunFilter(schema, handle, seg, N, kCoarsePruningExpr);
    ASSERT_EQ(capture.snapshots.size(), 1u)
        << "expected exactly one GIS split-fusion group";
    const auto& s = capture.snapshots.front();
    ASSERT_EQ(s.active_count, N);
    // The R-Tree must have answered. A coarse bitmap that degenerated to
    // all-ones (pin failure, index mid-load) still returns correct results, so
    // nothing else in this file would notice -- and it would make the refine
    // bound below vacuous.
    EXPECT_GT(s.coarse_selected, 0)
        << "B_coarse selected nothing; the shape no longer discriminates";
    EXPECT_LT(s.coarse_selected, N)
        << "B_coarse selected every row: the R-Tree coarse pass degraded to "
           "all-ones, so pruning is gone even though results stay correct";
    EXPECT_GT(s.refined_rows, 0)
        << "Refine evaluated nothing; the split path did not run";
    EXPECT_LE(s.refined_rows, s.coarse_selected)
        << "Refine evaluated more rows than B_coarse selected ("
        << s.refined_rows << " > " << s.coarse_selected
        << "); `survivors &= coarse_slice` is not reaching Refine";
    EXPECT_GE(static_cast<size_t>(s.refined_rows), res.count())
        << "result has rows Refine never evaluated";
}

// For each shape, assert the fusion-ON bitset equals the fusion-OFF baseline on
// the SAME segment (so any geometry-cache state is shared between the two runs).
void
AssertFusionEquivalence(const std::shared_ptr<Schema>& schema,
                        ScopedSchemaHandle& handle,
                        const SegmentInternalInterface* seg,
                        int64_t N) {
    for (const auto& e : EquivExprs()) {
        BitsetType baseline;
        BitsetType fused;
        {
            GISSplitFusionGuard off(false);
            baseline = RunFilter(schema, handle, seg, N, e);
        }
        {
            GISSplitFusionGuard on(true);
            fused = RunFilter(schema, handle, seg, N, e);
        }

        ASSERT_EQ(baseline.size(), fused.size())
            << "size mismatch, expr: " << e;
        ASSERT_EQ(baseline.size(), static_cast<size_t>(N));
        for (int64_t i = 0; i < static_cast<int64_t>(baseline.size()); ++i) {
            ASSERT_EQ(baseline[i], fused[i])
                << "row " << i << " differs, expr: " << e;
        }
    }
}

std::shared_ptr<Schema>
MakeGISSchema(bool nullable_geo = false) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField("geo", DataType::GEOMETRY, nullable_geo);
    schema->AddDebugField("age", DataType::INT64);
    schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk_fid);
    return schema;
}

}  // namespace

TEST(GISCoarseRefineExprTest, EquivalenceFusionOnVsOff) {
    auto schema = MakeGISSchema();
    const int64_t N = 1000;
    auto dataset = DataGen(schema, N);
    auto seg = CreateSealedWithFieldDataLoaded(schema, dataset);
    ScopedSchemaHandle handle(*schema);

    AssertFusionEquivalence(schema, handle, seg.get(), N);
}

// ON-vs-OFF equivalence is blind to how much work Refine does -- making it
// evaluate every active row keeps every result bit identical. It is equally
// blind to an over-inclusive (even all-ones) coarse bitmap, because Refine
// re-evaluates the exact predicate and the conjunction ANDs once more at the
// end. So equivalence alone cannot tell "pruning works" from "pruning silently
// degraded to a full scan". This pins the one part that IS observable from
// outside the operator: the scalar mask must actually reach Refine.
TEST(GISCoarseRefineExprTest, SelectiveScalarMaskActuallyPrunes) {
    auto schema = MakeGISSchema();
    const int64_t N = 1000;
    auto dataset = DataGen(schema, N);
    auto seg = CreateSealedWithFieldDataLoaded(schema, dataset);
    ScopedSchemaHandle handle(*schema);

    AssertSelectiveShapeIsDiscriminating(schema, handle, seg.get(), N);
}

// Same equivalence, but with enableGeometryCache ON so the segment is loaded
// with a populated geometry cache. This exercises the Refine node's
// cache-backed branch (PhyGISRefineConjunctExpr::Eval `if (geometry_cache)`),
// which the cache-off test cannot reach. The optimization must be orthogonal to
// the cache (design doc section 9).
TEST(GISCoarseRefineExprTest, EquivalenceFusionWithGeometryCache) {
    GeometryCacheGuard cache_on(true);  // set BEFORE loading the segment

    auto schema = MakeGISSchema();
    const int64_t N = 1000;
    auto dataset = DataGen(schema, N);
    auto seg = CreateSealedWithFieldDataLoaded(schema, dataset);
    ScopedSchemaHandle handle(*schema);

    // Sanity: the cache must actually be populated, otherwise the Refine node
    // would silently fall back to the WKB path and this test would not cover
    // the cache branch it is meant to lock down.
    auto geo_fid = schema->get_field_id(FieldName("geo"));
    ASSERT_NE(milvus::exec::SimpleGeometryCacheManager::Instance().GetCache(
                  seg->get_segment_id(), geo_fid),
              nullptr);

    AssertFusionEquivalence(schema, handle, seg.get(), N);
}

// Equivalence with a NULLABLE geometry field, so ~50% of rows carry null
// geometry (DataGen's deterministic i%2 valid pattern). Exercises the
// null-handling branches in the Coarse/Refine nodes (the `valid` bitmaps and
// the Refine null-skip), which the non-nullable schema never reaches. The
// split path must still produce exactly the baseline selection on null rows.
TEST(GISCoarseRefineExprTest, EquivalenceFusionNullableGeometry) {
    auto schema = MakeGISSchema(/*nullable_geo=*/true);
    const int64_t N = 1000;
    auto dataset = DataGen(schema, N);
    auto seg = CreateSealedWithFieldDataLoaded(schema, dataset);
    ScopedSchemaHandle handle(*schema);

    // Sanity: the nullable geo column must actually contain null rows, else this
    // test degenerates into the non-nullable case.
    auto geo_fid = schema->get_field_id(FieldName("geo"));
    const auto& valid = dataset.get_col_valid(geo_fid);
    ASSERT_NE(std::count(valid.begin(), valid.end(), false), 0)
        << "nullable geo column produced no null rows";

    AssertFusionEquivalence(schema, handle, seg.get(), N);
}

// Equivalence on a GROWING segment. The baseline GIS path takes different
// data-type branches for growing vs. sealed segments (std::string vs.
// std::string_view chunk access), so the sealed-only tests above cannot lock
// down the growing path. This variant uses empty_index_meta, so no geometry
// index is created and it covers the Coarse node's "no index -> full coarse
// set" degenerate path; growing WITH a geometry R-Tree is covered separately
// below.
TEST(GISCoarseRefineExprTest, EquivalenceFusionGrowingSegment) {
    auto schema = MakeGISSchema();
    const int64_t N = 1000;
    auto dataset = DataGen(schema, N);
    auto seg = CreateGrowingWithFieldDataLoaded(schema,
                                                milvus::empty_index_meta,
                                                SegcoreConfig::default_config(),
                                                dataset);
    ScopedSchemaHandle handle(*schema);

    AssertFusionEquivalence(schema, handle, seg.get(), N);
}

// Equivalence on a GROWING segment that DOES carry a geometry R-Tree index.
// FieldIndexing creates the growing geometry index whenever the collection
// index meta has the field, and HasIndex() flips true once ingested rows are
// synced into it -- so "growing never has a geometry index" does NOT hold.
// This is the production shape for freshly ingested geo data and the only
// shape where the R-Tree Query() bitmap (sized by rows appended to the index)
// can be larger than active_count_ (MVCC-visible rows): RunRTreeQuery must
// normalize the index-sized bitmap into active_count_ space instead of
// feeding it to a size-checked bitwise combine (a bare assert() compiled out
// under NDEBUG). Runs equivalence at full visibility AND with
// active_count < index rows to pin the normalization down.
TEST(GISCoarseRefineExprTest, EquivalenceFusionGrowingSegmentWithRTreeIndex) {
    auto schema = MakeGISSchema();
    const int64_t N = 1000;
    auto dataset = DataGen(schema, N);

    auto geo_fid = schema->get_field_id(FieldName("geo"));
    std::map<FieldId, FieldIndexMeta> field_metas;
    field_metas.emplace(geo_fid,
                        FieldIndexMeta(geo_fid,
                                       {{knowhere::meta::INDEX_TYPE,
                                         milvus::index::RTREE_INDEX_TYPE}},
                                       {}));
    auto index_meta = std::make_shared<CollectionIndexMeta>(
        /*max_index_row_cnt=*/N * 2, std::move(field_metas));

    // The growing load path only appends into the indexing record when the
    // interim segment index is enabled; use a local copy so the global default
    // config is untouched.
    SegcoreConfig config = SegcoreConfig::default_config();
    config.set_enable_interim_segment_index(true);
    auto seg =
        CreateGrowingWithFieldDataLoaded(schema, index_meta, config, dataset);
    ScopedSchemaHandle handle(*schema);

    // Sanity: the growing segment must actually report a synced geometry
    // index, otherwise this degenerates into the no-index growing test above.
    auto geo_field_id = schema->get_field_id(FieldName("geo"));
    ASSERT_TRUE(seg->HasIndex(geo_field_id))
        << "growing segment did not build/sync the geometry R-Tree index";

    // Full visibility: active_count == rows in the index.
    AssertFusionEquivalence(schema, handle, seg.get(), N);

    // This is the only segment in this file with a real R-Tree, so it is the
    // only place the coarse half of the pruning contract is observable.
    AssertCoarseMaskActuallyPrunes(schema, handle, seg.get(), N);

    // Partial visibility: active_count < rows in the index -- the concurrent
    // ingestion shape (the insert path appends to the index before acking
    // rows; a query ts below the newest inserts lowers active_count too).
    // RunRTreeQuery must slice its index-sized bitmap down to active_count_.
    AssertFusionEquivalence(schema, handle, seg.get(), N - 137);
}

// Equivalence with a small expr batch size, so a single N=1000 segment is
// evaluated over MANY Eval batches. The default batch size (8192) makes the
// other tests run in a single batch, which never exercises the split nodes'
// per-batch slicing of the segment-level coarse_candidates bitmap, the
// MoveCursor advance, or the dual-cursor sync between the Coarse and Refine
// nodes across batch boundaries. Forcing several batches locks those paths
// down. (The R-Tree-indexed coarse path is covered separately by
// RTreeIndexTest.GIS_SplitFusion_Equivalence_Indexed.)
TEST(GISCoarseRefineExprTest, EquivalenceFusionMultiBatch) {
    ExprBatchSizeGuard batch_guard(128);  // 1000 rows -> 8 batches

    auto schema = MakeGISSchema();
    const int64_t N = 1000;
    auto dataset = DataGen(schema, N);
    auto seg = CreateSealedWithFieldDataLoaded(schema, dataset);
    ScopedSchemaHandle handle(*schema);

    AssertFusionEquivalence(schema, handle, seg.get(), N);
}

// The GIS filter slices only by its own batch cursor and never reads the
// offset-input list, so it MUST report SupportOffsetInput() == false. If it
// (or, with fusion ON, the conjunction wrapping it) reported true, the
// IterativeFilterNode native path would feed a sparse offset list into an Eval
// that ignores it and return misaligned rows (a silent wrong-results bug). This
// locks the contract on both the baseline and the split-fusion path so a future
// change cannot regress it. See PR #50675 review (Medium: SupportOffsetInput).
TEST(GISCoarseRefineExprTest, GISDoesNotSupportOffsetInput) {
    auto schema = MakeGISSchema();
    const int64_t N = 256;
    auto dataset = DataGen(schema, N);
    auto seg = CreateSealedWithFieldDataLoaded(schema, dataset);
    ScopedSchemaHandle handle(*schema);

    // Representative shapes: a bare GIS leaf (compiles to
    // PhyGISFunctionFilterExpr) and a same-column conjunction that fusion
    // rewrites into the Coarse/Refine nodes wrapped in a conjunction.
    const std::vector<std::string> shapes = {
        R"expr(st_intersects(geo, "POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))"))expr",
        R"expr(st_intersects(geo, "POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))") and st_within(geo, "POLYGON((-100 -100, 100 -100, 100 100, -100 100, -100 -100))"))expr",
    };

    auto top_supports_offset_input = [&](const std::string& expr) -> bool {
        auto bin = handle.ParseSearch(
            expr, "vec", 5, knowhere::metric::L2, R"({"nprobe":10})", 3);
        auto plan = CreateSearchPlanByExpr(schema, bin.data(), bin.size());
        auto filter_node =
            std::dynamic_pointer_cast<milvus::plan::FilterBitsNode>(
                plan->plan_node_->plannodes_->sources()[0]->sources()[0]);
        std::vector<milvus::expr::TypedExprPtr> filters{filter_node->filter()};
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, seg.get(), N, MAX_TIMESTAMP);
        milvus::exec::ExecContext exec_context(query_context.get());
        milvus::exec::ExprSet expr_set(filters, &exec_context);
        return expr_set.exprs()[0]->SupportOffsetInput();
    };

    for (const auto& e : shapes) {
        {
            GISSplitFusionGuard off(false);
            EXPECT_FALSE(top_supports_offset_input(e))
                << "fusion OFF, expr: " << e;
        }
        {
            GISSplitFusionGuard on(true);
            EXPECT_FALSE(top_supports_offset_input(e))
                << "fusion ON, expr: " << e;
        }
    }
}
