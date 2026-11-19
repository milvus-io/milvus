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

#include "test_utils/ExprCacheTestUtils.h"
#include <gtest/gtest.h>

#include <cstdlib>
#include <thread>
#include <chrono>
#include <functional>
#include <string>
#include "exec/expression/ExprCache.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "folly/ScopeGuard.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/segment_c.h"
#include "segcore/test_utils/TextMatchTestUtils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::segcore::text_test;

namespace {

void
RunWithIsolatedExpressionCache(const std::function<void()>& test_case) {
    // The manager has no configuration snapshot API or public constructor.
    // Re-execution isolates its singleton, configuration, entries and metrics
    // from the parent test process, including fatal assertion exits.
    const auto previous_style = ::testing::FLAGS_gtest_death_test_style;
    auto restore_style = folly::makeGuard(
        [&] { ::testing::FLAGS_gtest_death_test_style = previous_style; });
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(
        {
            test_case();
            std::_Exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0),
        "");
}

}  // namespace

TEST(TextMatchCacheTest, ExprResCacheSealed) {
    RunWithIsolatedExpressionCache([] {
        using milvus::exec::ExprResCacheManager;
        auto& mgr = ExprResCacheManager::Instance();
        ExprResCacheManager::SetEnabled(true);
        mgr.Clear();
        mgr.SetCapacityBytes(1ULL << 20);
        auto clear_cache = folly::makeGuard([&] { mgr.Clear(); });

        auto schema = GenTestSchema();
        std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                            "swimming, football"};

        int64_t N = 2;
        uint64_t seed = 19190504;
        auto raw_data = DataGen(schema, N, seed);
        auto str_col = raw_data.raw_->mutable_fields_data()
                           ->at(1)
                           .mutable_scalars()
                           ->mutable_string_data()
                           ->mutable_data();
        for (int64_t i = 0; i < N; i++) {
            str_col->at(i) = raw_str[i];
        }

        auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
        seg->CreateTextIndex(FieldId(101));

        ASSERT_EQ(mgr.GetEntryCount(), 0);

        BitsetType final;
        auto expr = GetMatchExpr(schema, "football", OpType::TextMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);

        // Expect one cache entry inserted
        ASSERT_EQ(mgr.GetEntryCount(), 1);

        // Run again; should hit cache and not increase entries
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_EQ(mgr.GetEntryCount(), 1);
    });
}

TEST(TextMatchCacheTest, ExprResCacheFilterBitsDoesNotDuplicateTextMatchEntry) {
    RunWithIsolatedExpressionCache([] {
        using milvus::exec::ExprResCacheManager;
        auto& mgr = ExprResCacheManager::Instance();
        ExprResCacheManager::SetEnabled(true);
        mgr.Clear();
        mgr.SetCapacityBytes(1ULL << 20);
        auto clear_cache = folly::makeGuard([&] { mgr.Clear(); });

        auto schema = GenTestSchema();
        std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                            "swimming, football"};

        int64_t N = 2;
        uint64_t seed = 19190504;
        auto raw_data = DataGen(schema, N, seed);
        auto str_col = raw_data.raw_->mutable_fields_data()
                           ->at(1)
                           .mutable_scalars()
                           ->mutable_string_data()
                           ->mutable_data();
        for (int64_t i = 0; i < N; i++) {
            str_col->at(i) = raw_str[i];
        }

        auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
        seg->CreateTextIndex(FieldId(101));

        auto expr = GetMatchExpr(schema, "football", OpType::TextMatch);
        auto plan_fragment = plan::PlanFragment(expr);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, seg.get(), N, MAX_TIMESTAMP);
        query_context->set_read_snapshot(
            query_context->get_segment()->CaptureReadSnapshot());

        query_context->set_enable_expr_cache(true);
        query_context->set_enable_sub_expr_cache_write(false);

        auto row =
            ExecPlanNodeVisitor::ExecuteTask(plan_fragment, query_context);
        ASSERT_NE(row, nullptr);
        ASSERT_EQ(mgr.GetEntryCount(), 1);

        milvus::test::ExprCacheKey filter_key{
            seg->get_segment_id(),
            expr->filter()->CacheSignature().Derive(3),
            seg->CaptureReadSnapshot()->GetExprCacheGeneration()};
        ExprResCacheManager::Value filter_value;
        filter_value.active_count = N;
        ASSERT_TRUE(mgr.Get(filter_key, filter_value));

        milvus::test::ExprCacheKey text_match_key{
            seg->get_segment_id(),
            expr->filter()->CacheSignature(),
            seg->type() == SegmentType::Sealed
                ? seg->CaptureReadSnapshot()->GetExprCacheGeneration()
                : nullptr};
        ExprResCacheManager::Value text_match_value;
        text_match_value.active_count = N;
        ASSERT_FALSE(mgr.Get(text_match_key, text_match_value));
    });
}

namespace {

BitsetType
ExecuteFilterBitsWithCache(
    const std::shared_ptr<plan::FilterBitsNode>& filter_plan,
    const segcore::SegmentInternalInterface* segment,
    int64_t active_count,
    Timestamp timestamp,
    int64_t entity_ttl_physical_time_us,
    SchemaPtr read_schema,
    bool whole_filter = false) {
    auto plan_fragment = plan::PlanFragment(filter_plan);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        DEAFULT_QUERY_ID,
        segment,
        active_count,
        timestamp,
        0,
        0,
        milvus::query::PlanOptions(),
        std::make_shared<milvus::exec::QueryConfig>(),
        nullptr,
        std::unordered_map<std::string,
                           std::shared_ptr<milvus::exec::BaseConfig>>(),
        entity_ttl_physical_time_us);
    query_context->set_read_schema(std::move(read_schema));
    query_context->set_read_snapshot(
        query_context->get_segment()->CaptureReadSnapshot());

    query_context->set_enable_expr_cache(whole_filter);
    query_context->set_enable_sub_expr_cache_write(!whole_filter);
    auto row = ExecPlanNodeVisitor::ExecuteTask(plan_fragment, query_context);
    AssertInfo(row != nullptr,
               "ExecuteTask returned null row vector for query expression");
    auto col_vec = std::dynamic_pointer_cast<ColumnVector>(row->childrens()[0]);
    AssertInfo(col_vec != nullptr, "failed to cast to ColumnVector");
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    BitsetType query_view(view);
    query_view.flip();
    return query_view;
}

}  // namespace

TEST(TextMatchCacheTest, FilterCachesDistinguishTextPredicates) {
    RunWithIsolatedExpressionCache([] {
        using milvus::exec::ExprResCacheManager;
        auto& mgr = ExprResCacheManager::Instance();
        ExprResCacheManager::SetEnabled(true);
        mgr.Clear();
        mgr.SetCapacityBytes(1ULL << 20);
        auto clear_cache = folly::makeGuard([&] { mgr.Clear(); });

        auto schema = GenTestSchema();
        std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                            "swimming, football"};

        int64_t N = 2;
        uint64_t seed = 19190504;
        auto raw_data = DataGen(schema, N, seed);
        auto str_col = raw_data.raw_->mutable_fields_data()
                           ->at(1)
                           .mutable_scalars()
                           ->mutable_string_data()
                           ->mutable_data();
        for (int64_t i = 0; i < N; i++) {
            str_col->at(i) = raw_str[i];
        }

        auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
        seg->CreateTextIndex(FieldId(101));

        auto football = GetMatchExpr(schema, "football", OpType::TextMatch);
        auto swimming = GetMatchExpr(schema, "swimming", OpType::TextMatch);

        for (bool whole_filter : {false, true}) {
            mgr.Clear();
            auto football_result = ExecuteFilterBitsWithCache(
                football, seg.get(), N, MAX_TIMESTAMP, 0, schema, whole_filter);
            ASSERT_TRUE(football_result[0]);
            ASSERT_TRUE(football_result[1]);

            auto swimming_result = ExecuteFilterBitsWithCache(
                swimming, seg.get(), N, MAX_TIMESTAMP, 0, schema, whole_filter);
            ASSERT_FALSE(swimming_result[0]);
            ASSERT_TRUE(swimming_result[1]);
        }
    });
}

TEST(TextMatchCacheTest, FilterCachesRespectEntityTTLPhysicalTime) {
    RunWithIsolatedExpressionCache([] {
        using milvus::exec::ExprResCacheManager;
        auto& mgr = ExprResCacheManager::Instance();
        ExprResCacheManager::SetEnabled(true);
        mgr.Clear();
        mgr.SetCapacityBytes(1ULL << 20);
        auto clear_cache = folly::makeGuard([&] { mgr.Clear(); });

        auto schema = std::make_shared<Schema>();
        auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
        auto ttl_fid =
            schema->AddDebugField("ttl_field", DataType::TIMESTAMPTZ);
        schema->set_primary_field_id(pk_fid);
        schema->set_ttl_field_id(ttl_fid);

        int64_t N = 2;
        auto raw_data = DataGen(schema, N, 19190504);
        for (auto& field_data : *raw_data.raw_->mutable_fields_data()) {
            if (field_data.field_id() == ttl_fid.get()) {
                auto* data =
                    field_data.mutable_scalars()->mutable_timestamptz_data();
                data->set_data(0, 150);
                data->set_data(1, 250);
            }
        }

        auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);

        auto always_true_expr = std::make_shared<expr::AlwaysTrueExpr>();
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           always_true_expr);

        for (bool whole_filter : {false, true}) {
            mgr.Clear();
            auto before_expire = ExecuteFilterBitsWithCache(
                plan, seg.get(), N, MAX_TIMESTAMP, 100, schema, whole_filter);
            ASSERT_TRUE(before_expire[0]);
            ASSERT_TRUE(before_expire[1]);
            EXPECT_EQ(mgr.GetEntryCount(), whole_filter ? 1 : 0);

            auto after_expire = ExecuteFilterBitsWithCache(
                plan, seg.get(), N, MAX_TIMESTAMP, 200, schema, whole_filter);
            ASSERT_FALSE(after_expire[0]);
            ASSERT_TRUE(after_expire[1]);
            EXPECT_EQ(mgr.GetEntryCount(), whole_filter ? 2 : 0);
        }
    });
}

TEST(TextMatchCacheTest, ExprResCacheGrowingTextBypassesReadAndWrite) {
    RunWithIsolatedExpressionCache([] {
        using milvus::exec::ExprResCacheManager;
        auto& mgr = ExprResCacheManager::Instance();
        auto reset_cache = folly::makeGuard([&]() {
            mgr.Clear();
            EXPECT_TRUE(mgr.SetConfig(milvus::exec::CacheConfig{}));
            ExprResCacheManager::SetEnabled(false);
        });
        milvus::exec::CacheConfig config;
        config.mode = milvus::exec::CacheMode::Memory;
        config.admission_threshold = 1;
        config.mem_min_eval_duration_us = 0;
        ASSERT_TRUE(mgr.SetConfig(config));
        mgr.Clear();
        ExprResCacheManager::SetEnabled(true);

        auto schema = GenTestSchema();
        constexpr int64_t N = 2;
        auto raw_data = DataGen(schema, N);
        auto* strings = raw_data.raw_->mutable_fields_data()
                            ->at(1)
                            .mutable_scalars()
                            ->mutable_string_data();
        strings->set_data(0, "football basketball");
        strings->set_data(1, "swimming football");
        auto seg = CreateGrowingSegment(schema, empty_index_meta);
        auto offset = seg->PreInsert(N);
        seg->Insert(offset,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.owner);
        auto pin = seg->PinGrowingIndex(nullptr, FieldId(101));
        ASSERT_TRUE(pin);
        ASSERT_EQ(TextReader(pin.Reader()).MatchQuery("football", 1).count(),
                  N);

        for (auto op :
             {OpType::TextMatch, OpType::PhraseMatch, OpType::TextMatchFuzzy}) {
            SCOPED_TRACE(proto::plan::OpType_Name(op));
            auto expr = GetMatchExpr(schema, "football", op);
            auto result = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            EXPECT_EQ(result.count(), N);
            EXPECT_EQ(mgr.GetEntryCount(), 0);
            mgr.Clear();

            // Simulate a result cached before the index became visible, with the
            // same active_count. Explicit commit/reload above avoids a timed race.
            milvus::test::ExprCacheKey key{
                seg->get_segment_id(),
                expr->filter()->ToString(),
                seg->type() == SegmentType::Sealed
                    ? seg->CaptureReadSnapshot()->GetExprCacheGeneration()
                    : nullptr};
            ExprResCacheManager::Value stale;
            stale.result = std::make_shared<TargetBitmap>(N);
            stale.valid_result = std::make_shared<TargetBitmap>(N, true);
            stale.active_count = N;
            mgr.Put(key, stale);
            ASSERT_EQ(mgr.GetEntryCount(), 1);

            result = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            EXPECT_EQ(result.count(), N);
            ExprResCacheManager::Value still_cached;
            still_cached.active_count = N;
            ASSERT_TRUE(mgr.Get(key, still_cached));
            EXPECT_EQ(still_cached.result->count(), 0);
            mgr.Clear();
        }
    });
}

TEST(TextMatchCacheTest, EntityTTLSkipsDynamicCacheButKeepsStableLeafPolicies) {
    RunWithIsolatedExpressionCache([] {
        using namespace milvus::exec;
        auto& manager = ExprResCacheManager::Instance();
        CacheConfig config;
        config.mode = CacheMode::Memory;
        config.admission_threshold = 1;
        config.mem_min_eval_duration_us = 0;
        ExprResCacheManager::SetEnabled(true);
        for (bool indexed : {false, true}) {
            ASSERT_TRUE(manager.SetConfig(config));
            auto schema = std::make_shared<Schema>();
            const auto pk = schema->AddDebugField("pk", DataType::INT64);
            const auto user = schema->AddDebugField("value", DataType::INT64);
            const auto ttl =
                schema->AddDebugField("ttl", DataType::TIMESTAMPTZ, true);
            schema->set_primary_field_id(pk);
            schema->set_ttl_field_id(ttl);
            auto data = DataGen(schema, 3);
            for (auto& field : *data.raw_->mutable_fields_data()) {
                if (field.field_id() == user.get()) {
                    for (int i = 0; i < 3; ++i) {
                        field.mutable_scalars()->mutable_long_data()->set_data(
                            i, 10);
                    }
                }
                if (field.field_id() == ttl.get()) {
                    auto* scalars = field.mutable_scalars();
                    scalars->mutable_timestamptz_data()->set_data(0, 150);
                    scalars->mutable_timestamptz_data()->set_data(1, 250);
                    scalars->mutable_timestamptz_data()->set_data(2, 0);
                    scalars->set_valid_data(0, true);
                    scalars->set_valid_data(1, true);
                    scalars->set_valid_data(2, false);
                }
            }
            auto segment = CreateSealedWithFieldDataLoaded(schema, data);
            if (indexed) {
                const std::vector<int64_t> values{150, 250, 0};
                const uint8_t validity = 3;
                auto field = std::make_shared<FieldData<int64_t>>(
                    DataType::TIMESTAMPTZ, true);
                static_cast<FieldDataBase&>(*field).FillFieldData(
                    values.data(), &validity, 3, 0);
                auto opened = test::expr_index::BuildIndex(
                    ttl, DataType::TIMESTAMPTZ, index::ASCENDING_SORT, {field});
                test::expr_index::InstallIndex(
                    *segment, ttl, DataType::TIMESTAMPTZ, std::move(opened));
            }
            QueryContext policy_context(
                "ttl-cache-policy", segment.get(), 3, MAX_TIMESTAMP);
            policy_context.set_read_schema(schema);
            policy_context.set_read_snapshot(segment->CaptureReadSnapshot());
            auto ttl_predicate =
                CreateTTLFieldFilterExpression(&policy_context);
            ASSERT_NE(ttl_predicate, nullptr);
            ASSERT_EQ(ttl_predicate->inputs().size(), 2);
            EXPECT_FALSE(ttl_predicate->IsResultCacheEnabled());
            EXPECT_TRUE(ttl_predicate->inputs()[0]->IsResultCacheEnabled());
            EXPECT_FALSE(ttl_predicate->inputs()[1]->IsResultCacheEnabled());
            auto physical_ttl =
                CompileExpression(ttl_predicate, &policy_context, {}, false);
            ASSERT_EQ(physical_ttl->GetInputsRef().size(), 2);
            EXPECT_FALSE(physical_ttl->IsResultCacheEnabled());
            EXPECT_TRUE(
                physical_ttl->GetInputsRef()[0]->IsResultCacheEnabled());
            EXPECT_FALSE(
                physical_ttl->GetInputsRef()[1]->IsResultCacheEnabled());

            proto::plan::GenericValue threshold;
            threshold.set_int64_val(5);
            auto predicate = std::make_shared<expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(user, DataType::INT64),
                proto::plan::OpType::GreaterThan,
                threshold);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, predicate);
            for (int64_t now : {100, 200, 300}) {
                const auto bits = ExecuteFilterBitsWithCache(
                    plan, segment.get(), 3, MAX_TIMESTAMP, now, schema);
                EXPECT_EQ(bits[0], now < 150);
                EXPECT_EQ(bits[1], now < 250);
                EXPECT_TRUE(bits[2]);  // NULL TTL never expires.
                EXPECT_TRUE(predicate->IsResultCacheEnabled());
                // The dynamic comparison/OR must not add changing keys.
                // IS NULL keeps its default policy, but PhyNullExpr currently
                // reads field/index validity without a shared result cache.
                // Only the user leaf produces a cache entry in this fixture.
                EXPECT_EQ(manager.GetEntryCount(), 1);
            }
        }
    });
}

TEST(TextMatchCacheTest, WholeHitMovesPairAndKeepsOutputLease) {
    RunWithIsolatedExpressionCache([] {
        using namespace milvus::exec;
        auto& manager = ExprResCacheManager::Instance();
        CacheConfig config;
        config.mode = CacheMode::Memory;
        config.admission_threshold = 1;
        config.mem_min_eval_duration_us = 0;
        ASSERT_TRUE(manager.SetConfig(config));
        ExprResCacheManager::SetEnabled(true);
        auto schema = GenTestSchema();
        auto data = DataGen(schema, 2, 19190504);
        auto* strings = data.raw_->mutable_fields_data()
                            ->at(1)
                            .mutable_scalars()
                            ->mutable_string_data();
        strings->set_data(0, "football basketball");
        strings->set_data(1, "football swimming");
        auto segment = CreateSealedWithFieldDataLoaded(schema, data);
        segment->CreateTextIndex(FieldId(101));
        auto plan = GetMatchExpr(schema, "football", OpType::TextMatch);
        auto warm = ExecuteQueryExpr(plan, segment.get(), 2, MAX_TIMESTAMP);
        ASSERT_EQ(warm.count(), 2);
        ASSERT_EQ(manager.GetEntryCount(), 1);
        ASSERT_EQ(manager.GetMaterializationBytes(), 0);

        QueryContext query("text-whole-hit", segment.get(), 2, MAX_TIMESTAMP);
        query.set_read_snapshot(segment->CaptureReadSnapshot());
        ExecContext context(&query);
        auto compiled =
            CompileExpressions({plan->filter()}, &context, {}, false);
        ASSERT_TRUE(compiled[0]->CanExecuteAllAtOnce());
        compiled[0]->SetExecuteAllAtOnce();
        EvalCtx evaluation(&context);
        VectorPtr output;
        compiled[0]->Eval(evaluation, output);
        auto column = std::dynamic_pointer_cast<ColumnVector>(output);
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 2);
        EXPECT_TRUE(TargetBitmapView(column->GetRawData(), 2).all());
        EXPECT_TRUE(TargetBitmapView(column->GetValidRawData(), 2).all());
        EXPECT_EQ(manager.GetMaterializationBytes(), 16);
        compiled.clear();
        manager.Clear();
        EXPECT_EQ(manager.GetMaterializationBytes(), 16);
        output.reset();
        column.reset();
        EXPECT_EQ(manager.GetMaterializationBytes(), 0);
    });
}
