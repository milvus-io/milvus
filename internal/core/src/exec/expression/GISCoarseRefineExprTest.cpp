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
#include "common/GeometryCache.h"
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
    };
    return exprs;
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
MakeGISSchema() {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField("geo", DataType::GEOMETRY);
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
