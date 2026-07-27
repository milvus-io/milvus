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
#include "common/Geometry.h"
#include "common/OpContext.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "exec/QueryContext.h"
#include "exec/expression/Expr.h"
#include "exec/operator/RescoresNode.h"
#include "expr/ITypeExpr.h"
#include "geos_c.h"
#include "pb/plan.pb.h"
#include "query/Plan.h"
#include "rescores/Scorer.h"

#include "segcore/reduce_c.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

TEST(Rescorer, Normal) {
    //0. prepare schema
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto str_fid = schema->AddDebugField("string", DataType::VARCHAR);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    schema->set_primary_field_id(str_fid);
    size_t N = 50;

    //2. load raw data
    auto raw_data = DataGen(schema, N, 42, 0, 8, 10, false, false);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    //3. load index
    auto vector_data = raw_data.get_col<float>(vec_fid);
    auto indexing = GenVecIndexing(
        N, dim, vector_data.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.index_params = GenIndexParams(indexing.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(indexing));
    load_index_info.index_params["metric_type"] = knowhere::metric::L2;
    segment->LoadIndex(load_index_info);
    int topK = 10;
    int group_size = 3;

    // no result after search
    {
        ScopedSchemaHandle handle(*schema);
        ScopedSchemaHandle::ScorerParams scorer;
        scorer.weight = 4;
        auto plan = handle.ParseSearchWithScorers(schema,
                                                  "int8 >= 100 and int8 < -1",
                                                  "fakevec",
                                                  10,
                                                  "L2",
                                                  R"({"ef": 50})",
                                                  {scorer});
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    }

    // search result not empty but no boost filter
    {
        ScopedSchemaHandle handle(*schema);
        ScopedSchemaHandle::ScorerParams scorer;
        scorer.weight = 4;
        auto plan = handle.ParseSearchWithScorers(schema,
                                                  "int8 >= -1 and int8 < 100",
                                                  "fakevec",
                                                  10,
                                                  "L2",
                                                  R"({"ef": 50})",
                                                  {scorer});
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    }

    // random function with seed
    {
        ScopedSchemaHandle handle(*schema);
        ScopedSchemaHandle::ScorerParams scorer;
        scorer.type = proto::plan::FunctionTypeRandom;
        scorer.weight = 1;
        scorer.params = {{"seed", "123"}};
        auto plan = handle.ParseSearchWithScorers(schema,
                                                  "int8 >= -1 and int8 < 100",
                                                  "fakevec",
                                                  10,
                                                  "L2",
                                                  R"({"ef": 50})",
                                                  {scorer});
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    }

    // random function with field as random seed
    {
        ScopedSchemaHandle handle(*schema);
        ScopedSchemaHandle::ScorerParams scorer;
        scorer.type = proto::plan::FunctionTypeRandom;
        scorer.weight = 1;
        scorer.params = {{"field", "int64"}};
        auto plan = handle.ParseSearchWithScorers(schema,
                                                  "int8 >= -1 and int8 < 100",
                                                  "fakevec",
                                                  10,
                                                  "L2",
                                                  R"({"ef": 50})",
                                                  {scorer});
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    }

    // random function with field and seed
    {
        ScopedSchemaHandle handle(*schema);
        ScopedSchemaHandle::ScorerParams scorer;
        scorer.type = proto::plan::FunctionTypeRandom;
        scorer.weight = 1;
        scorer.params = {{"seed", "123"}, {"field", "int64"}};
        auto plan = handle.ParseSearchWithScorers(schema,
                                                  "int8 >= -1 and int8 < 100",
                                                  "fakevec",
                                                  10,
                                                  "L2",
                                                  R"({"ef": 50})",
                                                  {scorer});
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);

        auto search_result_same_seed =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);

        // should return same score when use same seed
        for (auto i = 0; i < 10; i++) {
            AssertInfo(search_result->distances_[i] ==
                           search_result_same_seed->distances_[i],
                       "distance not equal %f:%f",
                       search_result->distances_[i],
                       search_result_same_seed->distances_[i]);
        }
    }
}
// Test: TargetBitmap batch_score with out-of-bounds offsets (should NOT
// crash). Both WeightScorer and RandomScorer indexed bitmap[offsets[i]]
// without a bounds check, reading past the end of the bitset whenever the
// filter bitmap does not cover the whole segment.
TEST(WeightScorerTest, BatchScoreTargetBitmapOutOfBoundsOffsets) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto raw_data = DataGen(schema, 8);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    expr::TypedExprPtr filter = nullptr;
    rescores::WeightScorer scorer(filter, 2.0F);

    TargetBitmap bitmap(50);
    bitmap.set(10);
    bitmap.set(40);

    // Offsets where some are OUT OF BOUNDS (>= 50), e.g. when the filter
    // bitmap does not cover the whole segment.
    FixedVector<int32_t> offsets = {10, 40, 60, 100, 200};
    std::vector<std::optional<float>> boost_scores(offsets.size(),
                                                   std::nullopt);

    ASSERT_NO_THROW(scorer.batch_score(nullptr,
                                       segment.get(),
                                       proto::plan::FunctionModeSum,
                                       offsets,
                                       bitmap,
                                       boost_scores));

    // In-bounds matched offsets should be scored.
    EXPECT_TRUE(boost_scores[0].has_value());
    EXPECT_TRUE(boost_scores[1].has_value());

    // Out-of-bounds offsets should NOT have scores (safely skipped).
    EXPECT_FALSE(boost_scores[2].has_value());
    EXPECT_FALSE(boost_scores[3].has_value());
    EXPECT_FALSE(boost_scores[4].has_value());
}

TEST(RandomScorerTest, BatchScoreTargetBitmapOutOfBoundsOffsets) {
    // The segment is only consulted for get_segment_id() on the
    // no-seed-field path of random_score.
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto raw_data = DataGen(schema, 8);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    expr::TypedExprPtr filter = nullptr;
    ProtoParams params;
    auto* seed = params.Add();
    seed->set_key("seed");
    seed->set_value("42");
    rescores::RandomScorer scorer(filter, 1.0F, params);

    TargetBitmap bitmap(50);
    bitmap.set(10);
    bitmap.set(40);

    FixedVector<int32_t> offsets = {10, 40, 60, 100, 200};
    std::vector<std::optional<float>> boost_scores(offsets.size(),
                                                   std::nullopt);

    ASSERT_NO_THROW(scorer.batch_score(nullptr,
                                       segment.get(),
                                       proto::plan::FunctionModeSum,
                                       offsets,
                                       bitmap,
                                       boost_scores));

    // In-bounds matched offsets should be scored.
    EXPECT_TRUE(boost_scores[0].has_value());
    EXPECT_TRUE(boost_scores[1].has_value());

    // Out-of-bounds offsets should NOT have scores (safely skipped).
    EXPECT_FALSE(boost_scores[2].has_value());
    EXPECT_FALSE(boost_scores[3].has_value());
    EXPECT_FALSE(boost_scores[4].has_value());
}

// Test: a boost filter whose expression does not support offset input (GIS,
// text match) is evaluated batch by batch over the whole segment. The
// resulting bitset must cover every active row, not just the first
// expression batch (DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE = 8192 rows),
// otherwise offsets beyond the first batch silently lose their boost (or
// read out of bounds).
TEST(RescoresNode, NonNativeBoostFilterBitsetCoversAllBatches) {
    const int64_t N = 10000;  // more than one expression batch (8192)
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto geo_fid = schema->AddDebugField("geo", DataType::GEOMETRY);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk_fid);

    auto raw_data = DataGen(schema, N);
    proto::schema::FieldData* geo_field_data = nullptr;
    for (auto& fd : *raw_data.raw_->mutable_fields_data()) {
        if (fd.field_id() == geo_fid.get()) {
            geo_field_data = &fd;
            break;
        }
    }
    ASSERT_NE(geo_field_data, nullptr);
    // Even rows sit inside the query polygon, odd rows far outside.
    auto* geo_col = geo_field_data->mutable_scalars()->mutable_geometry_data();
    geo_col->clear_data();
    auto ctx = GEOS_init_r();
    for (int64_t i = 0; i < N; i++) {
        const char* wkt =
            (i % 2 == 0) ? "POINT (0.5 0.5)" : "POINT (100.0 100.0)";
        Geometry geom(ctx, wkt);
        geo_col->add_data(geom.to_wkb_string());
    }
    GEOS_finish_r(ctx);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    expr::TypedExprPtr filter = std::make_shared<expr::GISFunctionFilterExpr>(
        expr::ColumnInfo(geo_fid, DataType::GEOMETRY),
        proto::plan::GISFunctionFilterExpr_GISOp_Within,
        "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");

    auto query_context = std::make_shared<exec::QueryContext>(
        "test_rescore_gis_multi_batch", segment.get(), N, MAX_TIMESTAMP);
    OpContext op_context;
    query_context->set_op_context(&op_context);
    auto exec_context = exec::ExecContext(query_context.get());

    std::vector<expr::TypedExprPtr> filters;
    filters.emplace_back(filter);
    auto expr_set = std::make_unique<exec::ExprSet>(filters, &exec_context);
    exec::EvalCtx eval_ctx(&exec_context, expr_set.get());

    // GIS must take the non-native fallback.
    ASSERT_FALSE(expr_set->exprs()[0]->SupportOffsetInput());

    auto bitset = exec::EvalNonNativeBoostFilterAllBatches(
        &exec_context, expr_set.get(), eval_ctx, filter);

    // The bitset must cover every active row, not just the first batch.
    ASSERT_EQ(bitset.size(), N);
    for (int64_t offset :
         {int64_t(0), int64_t(1), int64_t(9000), int64_t(9001), N - 2, N - 1}) {
        bool expected = (offset % 2 == 0);
        ASSERT_EQ(bitset[offset], expected)
            << "wrong filter bit at offset " << offset;
    }
}
