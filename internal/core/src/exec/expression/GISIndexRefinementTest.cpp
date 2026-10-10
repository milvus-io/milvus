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

#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "common/Geometry.h"
#include "common/Consts.h"
#include "common/LoadInfo.h"
#include "common/GeometryCache.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "expr/ITypeExpr.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/ISpatialReader.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"

namespace {
using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

std::string
CreateWkbFromWkt(const std::string& wkt) {
    return Geometry(GetThreadLocalGEOSContext(), wkt.c_str()).to_wkb_string();
}

// Fault injection at the expression boundary retains the real loaded R-Tree
// and its NULL state. Current loaders reject incomplete archives; a malformed
// short candidate bitmap is injected here to keep the consumer's defensive
// full-refinement branch covered without resurrecting the removed Index API.
class ShortSpatialCandidates final : public index::IIndexReaderBase,
                                     public index::ISpatialReader,
                                     public index::INullReader {
 public:
    explicit ShortSpatialCandidates(index::IIndexReaderBasePtr reader)
        : reader_(std::move(reader)) {
    }

    index::ReaderCaps
    Caps() const override {
        return reader_->Caps();
    }
    index::Domain
    CoordDomain() const override {
        return reader_->CoordDomain();
    }
    int64_t
    Count() const override {
        return reader_->Count();
    }
    DataType
    ValueType() const override {
        return reader_->ValueType();
    }
    int64_t
    MemoryUsage() const override {
        return reader_->MemoryUsage();
    }
    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return reader_->CellByteSize();
    }
    TargetBitmap
    Candidates(index::SpatialOp op, const Geometry& query) const override {
        auto candidates =
            dynamic_cast<const index::ISpatialReader&>(*reader_).Candidates(
                op, query);
        AssertInfo(candidates.size() > 2,
                   "short-candidate fixture needs three rows");
        candidates.reset(1);
        candidates.resize(candidates.size() - 1);
        return candidates;
    }
    TargetBitmap
    IsNull() const override {
        return dynamic_cast<const index::INullReader&>(*reader_).IsNull();
    }
    TargetBitmap
    IsNotNull() const override {
        return dynamic_cast<const index::INullReader&>(*reader_).IsNotNull();
    }

 private:
    index::IIndexReaderBasePtr reader_;
};

// RAII toggle for the static geometry-cache switch: restores the previous
// value even when a gtest ASSERT returns out of the test body early, so a
// failing test cannot leak the flag into later tests.
struct GeometryCacheFlagGuard {
    explicit GeometryCacheFlagGuard(bool enable)
        : previous_(milvus::segcore::SegcoreConfig::default_config()
                        .get_enable_geometry_cache()) {
        milvus::segcore::SegcoreConfig::default_config()
            .set_enable_geometry_cache(enable);
    }
    ~GeometryCacheFlagGuard() {
        milvus::segcore::SegcoreConfig::default_config()
            .set_enable_geometry_cache(previous_);
    }
    bool previous_;
};

class GISIndexRefinementTest : public ::testing::Test {
 protected:
    struct GeometrySegment {
        std::unique_ptr<SegmentSealed> sealed;
        FieldId pk_id;
        FieldId geo_id;
    };

    void
    LoadColumn(SegmentSealed& segment,
               FieldId field_id,
               const FieldDataPtr& data) {
        segment.LoadFieldData(raw_files_.Prepare(field_id, {data}));
    }

    void
    LoadGeometryColumn(SegmentSealed& segment,
                       FieldId field_id,
                       const std::vector<std::string>& wkbs,
                       const std::vector<uint8_t>& validity = {}) {
        auto data = storage::CreateFieldData(
            DataType::GEOMETRY, DataType::NONE, !validity.empty());
        if (validity.empty()) {
            data->FillFieldData(wkbs.data(), wkbs.size());
        } else {
            data->FillFieldData(wkbs.data(), validity.data(), wkbs.size(), 0);
        }
        LoadColumn(segment, field_id, data);
    }

    GeometrySegment
    MakeGeometrySegment(const std::vector<std::string>& wkbs,
                        const std::vector<uint8_t>& validity = {},
                        bool load_geometry = true) {
        auto schema = std::make_shared<Schema>();
        const auto pk = schema->AddDebugField("id", DataType::INT64);
        const auto geo =
            schema->AddDebugField("geo", DataType::GEOMETRY, !validity.empty());
        schema->set_primary_field_id(pk);
        auto sealed = CreateSealedSegment(schema);
        std::vector<int64_t> ids(wkbs.size());
        std::iota(ids.begin(), ids.end(), 0);
        const std::vector<int64_t> timestamps(wkbs.size(), 0);
        for (const auto field_id : {RowFieldID, pk, TimestampFieldID}) {
            auto data = storage::CreateFieldData(
                DataType::INT64, DataType::NONE, false);
            const auto& values =
                field_id == TimestampFieldID ? timestamps : ids;
            data->FillFieldData(values.data(), values.size());
            LoadColumn(*sealed, field_id, data);
        }
        if (load_geometry) {
            LoadGeometryColumn(*sealed, geo, wkbs, validity);
        }
        return {std::move(sealed), pk, geo};
    }

    void
    InstallGeometryIndex(GeometrySegment& segment,
                         const std::vector<std::string>& wkbs,
                         const std::vector<uint8_t>& validity = {},
                         bool short_candidates = false) {
        auto data = storage::CreateFieldData(
            DataType::GEOMETRY, DataType::NONE, !validity.empty());
        if (validity.empty()) {
            data->FillFieldData(wkbs.data(), wkbs.size());
        } else {
            data->FillFieldData(wkbs.data(), validity.data(), wkbs.size(), 0);
        }
        auto opened = test::expr_index::BuildIndex(segment.geo_id,
                                                   DataType::GEOMETRY,
                                                   index::RTREE_INDEX_TYPE,
                                                   {data});
        if (short_candidates) {
            opened.reader = std::make_unique<ShortSpatialCandidates>(
                std::move(opened.reader));
        }
        test::expr_index::InstallIndex(*segment.sealed,
                                       segment.geo_id,
                                       DataType::GEOMETRY,
                                       std::move(opened));
    }

    GeometrySegment
    MakeShortCandidatesSegment(int rows) {
        std::vector<std::string> wkbs(rows, CreateWkbFromWkt("POINT(0 0)"));
        wkbs.back().clear();
        std::vector<uint8_t> validity((rows + 7) / 8, 0);
        for (int row = 0; row < rows - 1; ++row) {
            validity[row / 8] |= static_cast<uint8_t>(1u << (row % 8));
        }
        auto segment = MakeGeometrySegment(wkbs, validity);
        InstallGeometryIndex(segment, wkbs, validity, true);
        return segment;
    }

    // Each test's segments die before this owner removes their raw binlogs.
    test::expr_index::RawFieldFiles raw_files_;
};

std::vector<std::string>
FourShapeWkbs(int rows) {
    const char* shapes[] = {"POINT(0 0)",
                            "POLYGON((-1 -1,1 -1,1 1,-1 1,-1 -1))",
                            "POLYGON((10 10,20 10,20 20,10 20,10 10))",
                            "LINESTRING(-1 0,1 0)"};
    std::vector<std::string> wkbs;
    wkbs.reserve(rows);
    for (int row = 0; row < rows; ++row) {
        wkbs.push_back(CreateWkbFromWkt(shapes[row % 4]));
    }
    return wkbs;
}

TEST_F(GISIndexRefinementTest, ExactRelationsRefineRTreeCandidates) {
    constexpr int N = 200;
    const auto wkbs = FourShapeWkbs(N);
    auto segment = MakeGeometrySegment(wkbs);
    InstallGeometryIndex(segment, wkbs);
    auto& sealed = segment.sealed;
    const auto geo_id = segment.geo_id;
    auto test_op = [&](const std::string& wkt,
                       proto::plan::GISFunctionFilterExpr_GISOp op,
                       std::function<bool(int)> expected) {
        auto gis_expr = std::make_shared<milvus::expr::GISFunctionFilterExpr>(
            milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY), op, wkt);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           gis_expr);
        BitsetType bits =
            ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(bits.size(), N);
        for (int i = 0; i < N; ++i) {
            EXPECT_EQ(bool(bits[i]), expected(i)) << "i=" << i;
        }
    };

    // exact within: polygon around origin should include indices 0,1,3
    test_op("POLYGON((-2 -2,2 -2,2 2,-2 2,-2 -2))",
            proto::plan::GISFunctionFilterExpr_GISOp_Within,
            [](int i) { return (i % 4 == 0) || (i % 4 == 1) || (i % 4 == 3); });

    // exact intersects: point (0,0) should intersect point, polygon containing it, and line through it
    test_op("POINT(0 0)",
            proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
            [](int i) { return (i % 4 == 0) || (i % 4 == 1) || (i % 4 == 3); });

    // exact equals: only the point equals
    test_op("POINT(0 0)",
            proto::plan::GISFunctionFilterExpr_GISOp_Equals,
            [](int i) { return (i % 4 == 0); });
}

// Restore the previous split-fusion setting even if
// ExecuteQueryExpr throws mid-run (a bare set/restore would leak the global
// flag into later tests).
struct GisSplitFusionFlagGuard {
    bool previous;
    explicit GisSplitFusionFlagGuard(bool enable)
        : previous(
              SegcoreConfig::default_config().get_enable_gis_split_fusion()) {
        milvus::segcore::SegcoreConfig::default_config()
            .set_enable_gis_split_fusion(enable);
    }
    ~GisSplitFusionFlagGuard() {
        milvus::segcore::SegcoreConfig::default_config()
            .set_enable_gis_split_fusion(previous);
    }
};

// RAII guard for the expr batch size, restored on scope exit, so the indexed
// equivalence check runs across MULTIPLE Eval batches (exercising the split
// nodes' per-batch coarse slicing + dual-cursor advance on the indexed path).
struct ExprBatchSizeGuardLocal {
    int64_t saved;
    explicit ExprBatchSizeGuardLocal(int64_t batch_size)
        : saved(milvus::EXEC_EVAL_EXPR_BATCH_SIZE.load()) {
        milvus::EXEC_EVAL_EXPR_BATCH_SIZE.store(batch_size);
    }
    ~ExprBatchSizeGuardLocal() {
        milvus::EXEC_EVAL_EXPR_BATCH_SIZE.store(saved);
    }
};

TEST_F(GISIndexRefinementTest, SplitFusionMatchesBaselineAcrossIndexedBatches) {
    constexpr int N = 200;
    const auto wkbs = FourShapeWkbs(N);
    auto segment = MakeGeometrySegment(wkbs);
    InstallGeometryIndex(segment, wkbs);
    auto& sealed = segment.sealed;
    const auto pk_id = segment.pk_id;
    const auto geo_id = segment.geo_id;
    ASSERT_TRUE(sealed->HasIndex(geo_id));

    // Build conjunction filters that combine a scalar predicate with same-column
    // geometry predicates (so SplitFuseGISConjunct fires and uses the index).
    auto col_geo = milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY);
    auto col_id = milvus::expr::ColumnInfo(pk_id, DataType::INT64);
    proto::plan::GenericValue zero;
    zero.set_int64_val(0);
    milvus::expr::TypedExprPtr scalar =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            col_id, proto::plan::OpType::GreaterEqual, zero);

    auto gis = [&](proto::plan::GISFunctionFilterExpr_GISOp op,
                   const std::string& wkt) -> milvus::expr::TypedExprPtr {
        return std::make_shared<milvus::expr::GISFunctionFilterExpr>(
            col_geo, op, wkt);
    };
    auto And = [](milvus::expr::TypedExprPtr a,
                  milvus::expr::TypedExprPtr b) -> milvus::expr::TypedExprPtr {
        return std::make_shared<milvus::expr::LogicalBinaryExpr>(
            milvus::expr::LogicalBinaryExpr::OpType::And, a, b);
    };
    auto Or = [](milvus::expr::TypedExprPtr a,
                 milvus::expr::TypedExprPtr b) -> milvus::expr::TypedExprPtr {
        return std::make_shared<milvus::expr::LogicalBinaryExpr>(
            milvus::expr::LogicalBinaryExpr::OpType::Or, a, b);
    };
    const auto kInter = proto::plan::GISFunctionFilterExpr_GISOp_Intersects;
    const auto kWithin = proto::plan::GISFunctionFilterExpr_GISOp_Within;
    const auto kDWithin = proto::plan::GISFunctionFilterExpr_GISOp_DWithin;

    std::vector<milvus::expr::TypedExprPtr> filters = {
        // scalar AND single GIS (indexed)
        And(scalar, gis(kInter, "POLYGON((-2 -2,2 -2,2 2,-2 2,-2 -2))")),
        // scalar AND (GIS OR GIS) -- Shape B
        And(scalar,
            Or(gis(kInter, "POLYGON((-2 -2,2 -2,2 2,-2 2,-2 -2))"),
               gis(kInter, "POLYGON((10 10,20 10,20 20,10 20,10 10))"))),
        // same-field AND group (intersects + within)
        And(gis(kInter, "POLYGON((-2 -2,2 -2,2 2,-2 2,-2 -2))"),
            gis(kWithin,
                "POLYGON((-100 -100,100 -100,100 100,-100 100,-100 -100))")),
        // direct AND-leaf + Shape-B subgroup on the SAME field: geo is both a
        // direct conjunction leaf (within) and inside an OR subgroup, so the
        // rewrite emits two independent coarse/refine pairs for it -- the
        // dual-pair path, on the indexed side.
        And(gis(kWithin,
                "POLYGON((-100 -100,100 -100,100 100,-100 100,-100 -100))"),
            Or(gis(kInter, "POLYGON((-2 -2,2 -2,2 2,-2 2,-2 -2))"),
               gis(kInter, "POLYGON((10 10,20 10,20 20,10 20,10 10))"))),
        // DWithin mixed with a groupable GIS leaf on the SAME field, on the
        // INDEXED side -- the config where a wrongly-grouped DWithin actually
        // corrupts the coarse phase (RunRTreeQuery would query the R-Tree
        // with the raw point instead of the distance-expanded bbox from
        // create_bounding_box_for_dwithin, and the group's Pred drops the
        // distance so refine degrades to 0.0). DWithin must stay on the
        // baseline path per the as_groupable_gis whitelist. Query point (3,3)
        // with a 1,000,000 m geodesic radius reaches the origin cluster
        // (point / small polygon / linestring, each a few hundred km away)
        // but not the (10,10)-(20,20) polygon (~1,100 km): a distance-0
        // degradation would select zero rows here, so ON-vs-OFF diverges
        // loudly if DWithin ever enters a fusion group.
        And(std::make_shared<milvus::expr::GISFunctionFilterExpr>(
                col_geo, kDWithin, "POINT(3 3)", /*distance=*/1000000.0),
            gis(kInter, "POLYGON((-2 -2,2 -2,2 2,-2 2,-2 -2))")),
    };

    auto run = [&](const milvus::expr::TypedExprPtr& f,
                   bool enable) -> BitsetType {
        // RAII: flag restored even if ExecuteQueryExpr throws.
        GisSplitFusionFlagGuard guard(enable);
        auto node =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, f);
        return ExecuteQueryExpr(node, sealed.get(), N, MAX_TIMESTAMP);
    };

    // Force multiple Eval batches (N=200 -> ~4 batches) so the indexed coarse
    // slicing is exercised across batch boundaries, not just a single batch.
    ExprBatchSizeGuardLocal batch_guard(64);

    for (const auto& f : filters) {
        BitsetType baseline = run(f, false);
        BitsetType fused = run(f, true);
        ASSERT_EQ(baseline.size(), fused.size());
        ASSERT_EQ(baseline.size(), static_cast<size_t>(N));
        for (int i = 0; i < N; ++i) {
            ASSERT_EQ(bool(baseline[i]), bool(fused[i])) << "row " << i;
        }
    }
}

TEST_F(GISIndexRefinementTest, IndexedRefinementSkipsCorruptWkbCandidate) {
    GeometryCacheFlagGuard cache_off(false);
    constexpr int N = 40;
    constexpr int kBad = 7;
    std::vector<std::string> wkbs(N, CreateWkbFromWkt("POINT(0 0)"));
    wkbs[kBad].resize(wkbs[kBad].size() / 2);
    auto segment = MakeGeometrySegment(wkbs);
    InstallGeometryIndex(segment, wkbs);
    auto& sealed = segment.sealed;
    const auto geo_id = segment.geo_id;
    // Origin-covering intersects query: must NOT throw, must select every valid
    // origin point and skip the unparseable row.
    auto gis_expr = std::make_shared<milvus::expr::GISFunctionFilterExpr>(
        milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY),
        proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        "POINT(0 0)");
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, gis_expr);
    BitsetType bits;
    ASSERT_NO_THROW(
        { bits = ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP); });
    ASSERT_EQ(bits.size(), static_cast<size_t>(N));
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(bool(bits[i]), i != kBad) << "row " << i;
    }
}

TEST_F(GISIndexRefinementTest,
       ShortCandidatesRecoverInteriorHoleAndPreserveTailNull) {
    GeometryCacheFlagGuard cache_off(false);
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    constexpr int N = 6;
    auto seg = MakeShortCandidatesSegment(N);
    auto& sealed = seg.sealed;
    auto geo_id = seg.geo_id;

    // The short coarse bitmap has no bit for row 1. A tail-only resize leaves
    // that interior bit false and silently loses a valid match; the full-scan
    // fallback must recover it through exact refinement.
    auto origin_intersects =
        std::make_shared<milvus::expr::GISFunctionFilterExpr>(
            milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY, {}, true),
            proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
            "POINT(0 0)");
    auto intersects_plan = std::make_shared<plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, origin_intersects);
    auto intersects_bits =
        ExecuteQueryExpr(intersects_plan, sealed.get(), N, MAX_TIMESTAMP);
    ASSERT_EQ(intersects_bits.size(), static_cast<size_t>(N));
    for (int i = 0; i < N - 1; ++i) {
        EXPECT_TRUE(intersects_bits[i]) << "valid row " << i;
    }
    EXPECT_FALSE(intersects_bits[N - 1]) << "NULL tail row must not match";

    auto intersects = std::make_shared<milvus::expr::GISFunctionFilterExpr>(
        milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY, {}, true),
        proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        "POINT(100 100)");
    auto not_intersects = std::make_shared<milvus::expr::LogicalUnaryExpr>(
        milvus::expr::LogicalUnaryExpr::OpType::LogicalNot, intersects);
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       not_intersects);
    auto bits = ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP);

    ASSERT_EQ(bits.size(), static_cast<size_t>(N));
    for (int i = 0; i < N - 1; ++i) {
        EXPECT_TRUE(bits[i]) << "non-null row " << i;
    }
    EXPECT_FALSE(bits[N - 1]) << "NULL tail row must remain unknown under NOT";

    sealed.reset();
}

// Regression for PR #50951 review (GISFunctionFilterExpr.cpp
// process_sealed_data): the short-candidate self-heal promotes EVERY row to
// a candidate, and with the geometry cache off (the default) refinement used
// to fetch all of them with ONE bulk_subscript -- a full-column WKB copy per
// query. Refinement now reads the candidates in batch_size_-row groups. Pin the
// batch size far below N so the group loop runs several full groups plus a
// partial tail, and check the answer is identical to the single-shot read:
// every valid row found, the NULL tail row not.
TEST_F(GISIndexRefinementTest,
       ShortCandidatesChunkedRefinementMatchesFullRead) {
    GeometryCacheFlagGuard cache_off(false);
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    constexpr int N = 11;  // 11 candidates / batch 4 -> groups 4,4,3
    auto seg = MakeShortCandidatesSegment(N);
    auto& sealed = seg.sealed;
    auto geo_id = seg.geo_id;

    auto run = [&](const char* wkt, bool negate) -> BitsetType {
        milvus::expr::TypedExprPtr e =
            std::make_shared<milvus::expr::GISFunctionFilterExpr>(
                milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY, {}, true),
                proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
                wkt);
        if (negate) {
            e = std::make_shared<milvus::expr::LogicalUnaryExpr>(
                milvus::expr::LogicalUnaryExpr::OpType::LogicalNot, e);
        }
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, e);
        return ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP);
    };

    // Reference: default batch size (>= N) -> a single bulk_subscript group.
    BitsetType single_hit = run("POINT(0 0)", false);
    BitsetType single_not = run("POINT(100 100)", true);
    ASSERT_EQ(single_hit.size(), static_cast<size_t>(N));
    for (int i = 0; i < N - 1; ++i) {
        ASSERT_TRUE(single_hit[i]) << "valid row " << i;
        ASSERT_TRUE(single_not[i]) << "non-null row " << i;
    }
    ASSERT_FALSE(single_hit[N - 1]);
    ASSERT_FALSE(single_not[N - 1]);

    // Chunked: batch 4 -> three bulk_subscript groups over the 11 candidates.
    // (Also shrinks the Eval batch, so per-batch slicing of the cached refined
    // bitmap crosses group boundaries too.)
    ExprBatchSizeGuardLocal batch_guard(4);
    BitsetType chunked_hit = run("POINT(0 0)", false);
    BitsetType chunked_not = run("POINT(100 100)", true);
    ASSERT_EQ(chunked_hit.size(), static_cast<size_t>(N));
    ASSERT_EQ(chunked_not.size(), static_cast<size_t>(N));
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(bool(chunked_hit[i]), bool(single_hit[i])) << "row " << i;
        EXPECT_EQ(bool(chunked_not[i]), bool(single_not[i])) << "row " << i;
    }

    sealed.reset();
}

// Regression for PR #50951 review (GISConjunctExpr.cpp RunRTreeQuery): the
// split/fusion coarse path used to pad a SHORT candidate bitmap only at
// the tail (resize(active_count_, true)). The missing entry of a legacy index
// is an INTERIOR hole (row 1 here), so its bit stayed false, Refine's
// `survivors &= coarse_slice` dropped the row, and `A AND B` on the same
// column silently lost a match that either predicate alone (per-predicate
// path, which self-heals to a full scan) would return. Both paths now share
// PromoteShortGISCoarseBitmap: fusion ON must equal fusion OFF, and both must
// return every valid row and reject the NULL tail.
TEST_F(GISIndexRefinementTest, SplitFusionShortCandidatesRecoverInteriorHole) {
    GeometryCacheFlagGuard cache_off(false);
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    constexpr int N = 6;
    auto seg = MakeShortCandidatesSegment(N);
    auto& sealed = seg.sealed;
    auto geo_id = seg.geo_id;

    auto col_geo =
        milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY, {}, true);
    auto gis = [&](proto::plan::GISFunctionFilterExpr_GISOp op,
                   const std::string& wkt) -> milvus::expr::TypedExprPtr {
        return std::make_shared<milvus::expr::GISFunctionFilterExpr>(
            col_geo, op, wkt);
    };
    // Same-field AND group: intersects(origin) AND within(big box). Both are
    // satisfied by every valid row, so any false in the answer below is a row
    // the coarse phase wrongly pruned.
    milvus::expr::TypedExprPtr filter =
        std::make_shared<milvus::expr::LogicalBinaryExpr>(
            milvus::expr::LogicalBinaryExpr::OpType::And,
            gis(proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
                "POINT(0 0)"),
            gis(proto::plan::GISFunctionFilterExpr_GISOp_Within,
                "POLYGON((-100 -100,100 -100,100 100,-100 100,-100 -100))"));

    auto run = [&](bool enable) -> BitsetType {
        GisSplitFusionFlagGuard guard(enable);
        auto node =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, filter);
        return ExecuteQueryExpr(node, sealed.get(), N, MAX_TIMESTAMP);
    };
    // Small batch so the coarse slice is consumed across several Eval batches.
    ExprBatchSizeGuardLocal batch_guard(2);

    BitsetType baseline = run(false);
    BitsetType fused = run(true);
    ASSERT_EQ(baseline.size(), static_cast<size_t>(N));
    ASSERT_EQ(fused.size(), static_cast<size_t>(N));
    for (int i = 0; i < N - 1; ++i) {
        EXPECT_TRUE(baseline[i]) << "baseline valid row " << i;
        EXPECT_TRUE(fused[i])
            << "fused valid row " << i << " (interior hole is row 1)";
    }
    EXPECT_FALSE(baseline[N - 1]) << "NULL tail row must not match";
    EXPECT_FALSE(fused[N - 1]) << "NULL tail row must not match";

    sealed.reset();
}

// Build the WKB column used by the corrupt-row tolerance tests: every row is
// POINT(0 0) except `bad_row`, whose WKB is truncated (unparseable).
std::vector<std::string>
MakeOriginWkbsWithOneCorruptRow(int n, int bad_row) {
    std::vector<std::string> wkbs;
    wkbs.reserve(n);
    auto ctx = GEOS_init_r();
    std::string origin_wkb =
        milvus::Geometry(ctx, "POINT(0 0)").to_wkb_string();
    GEOS_finish_r(ctx);
    for (int i = 0; i < n; ++i) {
        if (i == bad_row) {
            std::string bad = origin_wkb;
            bad.resize(bad.size() / 2);  // truncate -> unparseable
            wkbs.emplace_back(std::move(bad));
        } else {
            wkbs.emplace_back(origin_wkb);
        }
    }
    return wkbs;
}

// Regression for PR #50951 review (GISFunctionFilterExpr no-cache brute-force
// branch): with NO index and the geometry cache OFF (the default
// configuration), a GIS predicate over a segment containing a corrupt WKB row
// used the throwing Geometry(ctx, wkb) constructor on a per-batch
// GEOS_init_r() context -- one corrupt row failed the whole query AND leaked
// the context (the throw skipped GEOS_finish_r). Now the branch parses with
// TryParseFromWkb on a thread-local context: the query succeeds and the
// corrupt row simply evaluates to false.
TEST_F(GISIndexRefinementTest, RawRefinementSkipsCorruptWkbWithCacheDisabled) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    GeometryCacheFlagGuard cache_off(false);

    const int N = 40;
    const int kBad = 7;
    const auto wkbs = MakeOriginWkbsWithOneCorruptRow(N, kBad);
    auto segment = MakeGeometrySegment(wkbs);
    auto& sealed = segment.sealed;
    const auto geo_id = segment.geo_id;

    // No index loaded -> ExprExecPath::RawData -> the brute-force macro branch.
    auto gis_expr = std::make_shared<milvus::expr::GISFunctionFilterExpr>(
        milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY),
        proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        "POINT(0 0)");
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, gis_expr);
    BitsetType bits;
    ASSERT_NO_THROW(
        { bits = ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP); });
    ASSERT_EQ(bits.size(), static_cast<size_t>(N));
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(bool(bits[i]), i != kBad) << "row " << i;
    }
}

// Regression for PR #50951 review (GeometryCache.h AppendDataAt, the critical
// finding): with the geometry cache ENABLED, loading a segment containing one
// corrupt WKB row used the throwing Geometry ctor inside
// SimpleGeometryCache::AppendDataAt, so LoadFieldData -> LoadGeometryCache
// failed the ENTIRE segment load -- exactly the row shape the placeholder-MBR
// write paths deliberately keep. Now the corrupt row is cached as an invalid
// entry; the load succeeds and the cache branch of the filter macros skips the
// row (res=false) instead of tripping its former non-null assert.
TEST_F(GISIndexRefinementTest, GeometryCachePublishesAndRetiresWithRawColumn) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    GeometryCacheFlagGuard cache_on(true);

    const int N = 40;
    const int kBad = 7;
    const auto wkbs = MakeOriginWkbsWithOneCorruptRow(N, kBad);
    auto segment = MakeGeometrySegment(wkbs, {}, false);
    auto& sealed = segment.sealed;
    const auto geo_id = segment.geo_id;
    int64_t seg_id = -1;
    // Loading the column also constructs its geometry cache.
    ASSERT_NO_THROW(LoadGeometryColumn(*sealed, geo_id, wkbs));
    seg_id = sealed->get_segment_id();
    auto published_cache = sealed->GetGeometryCache(geo_id);
    ASSERT_NE(published_cache, nullptr);
    // Sealed caches live in the immutable published runtime state, not in the
    // process-global manager. That is what makes a failed/cancelled reopen
    // discard the staged replacement together with its unpublished column.
    EXPECT_EQ(milvus::exec::SimpleGeometryCacheManager::Instance().GetCache(
                  sealed->segment_instance_uid(), seg_id, geo_id),
              nullptr);

    // Query through the cache branch of the filter macros (cache is enabled
    // and populated by the load above).
    auto gis_expr = std::make_shared<milvus::expr::GISFunctionFilterExpr>(
        milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY),
        proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        "POINT(0 0)");
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, gis_expr);
    BitsetType bits;
    ASSERT_NO_THROW(
        { bits = ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP); });
    ASSERT_EQ(bits.size(), static_cast<size_t>(N));
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(bool(bits[i]), i != kBad) << "row " << i;
    }

    // Model the critical failure boundary of reopen: field loading and cache
    // construction mutate only a cloned runtime. If a later step throws or is
    // cancelled before Publish(), discarding that clone must leave the old
    // column/cache pair visible and must not leak the replacement globally.
    auto* sealed_impl =
        dynamic_cast<milvus::segcore::ChunkedSegmentSealedImpl*>(sealed.get());
    ASSERT_NE(sealed_impl, nullptr);
    auto staged_runtime = sealed_impl->TestCloneMutableRuntimeResourceState();
    auto unpublished_cache =
        std::make_shared<milvus::exec::SimpleGeometryCache>();
    staged_runtime->geometry_caches[geo_id] = unpublished_cache;
    EXPECT_EQ(sealed->GetGeometryCache(geo_id), published_cache);
    EXPECT_NE(sealed->GetGeometryCache(geo_id), unpublished_cache);
    staged_runtime.reset();
    EXPECT_EQ(sealed->GetGeometryCache(geo_id), published_cache);

    // Field retirement is another publication boundary: the cache must leave
    // the runtime snapshot together with the raw column.
    sealed->DropFieldData(geo_id);
    EXPECT_EQ(sealed->GetGeometryCache(geo_id), nullptr);
}

}  // namespace
