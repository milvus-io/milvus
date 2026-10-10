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

#include <cstdlib>
#include <functional>
#include <string>
#include "exec/expression/ExprCache.h"
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
    auto restore_style = folly::makeGuard([&] {
        ::testing::FLAGS_gtest_death_test_style = previous_style;
    });
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
        query_context->set_enable_expr_cache(true);
        query_context->set_enable_sub_expr_cache_write(false);

        auto row = ExecPlanNodeVisitor::ExecuteTask(plan_fragment, query_context);
        ASSERT_NE(row, nullptr);
        ASSERT_EQ(mgr.GetEntryCount(), 1);

        ExprResCacheManager::Key filter_key{seg->get_segment_id(),
                                            expr->ToString()};
        ExprResCacheManager::Value filter_value;
        filter_value.active_count = N;
        ASSERT_TRUE(mgr.Get(filter_key, filter_value));

        ExprResCacheManager::Key text_match_key{seg->get_segment_id(),
                                                expr->filter()->ToString()};
        ExprResCacheManager::Value text_match_value;
        text_match_value.active_count = N;
        ASSERT_FALSE(mgr.Get(text_match_key, text_match_value));
    });
}

namespace {

BitsetType
ExecuteFilterBitsWithFullCache(
    const std::shared_ptr<plan::FilterBitsNode>& filter_plan,
    const segcore::SegmentInternalInterface* segment,
    int64_t active_count,
    Timestamp timestamp,
    int64_t entity_ttl_physical_time_us) {
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
    query_context->set_enable_expr_cache(true);
    query_context->set_enable_sub_expr_cache_write(false);

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

TEST(TextMatchCacheTest, ExprResCacheFilterBitsUsesCurrentFilterNodeKey) {
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

        auto football_result = ExecuteFilterBitsWithFullCache(
            football, seg.get(), N, MAX_TIMESTAMP, 0);
        ASSERT_TRUE(football_result[0]);
        ASSERT_TRUE(football_result[1]);

        auto swimming_result = ExecuteFilterBitsWithFullCache(
            swimming, seg.get(), N, MAX_TIMESTAMP, 0);
        ASSERT_FALSE(swimming_result[0]);
        ASSERT_TRUE(swimming_result[1]);
    });
}

TEST(TextMatchCacheTest, ExprResCacheFilterBitsIncludesEntityTTLPhysicalTime) {
    RunWithIsolatedExpressionCache([] {
        using milvus::exec::ExprResCacheManager;
        auto& mgr = ExprResCacheManager::Instance();
        ExprResCacheManager::SetEnabled(true);
        mgr.Clear();
        mgr.SetCapacityBytes(1ULL << 20);
        auto clear_cache = folly::makeGuard([&] { mgr.Clear(); });

        auto schema = std::make_shared<Schema>();
        auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
        auto ttl_fid = schema->AddDebugField("ttl_field", DataType::TIMESTAMPTZ);
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

        auto before_expire =
            ExecuteFilterBitsWithFullCache(plan, seg.get(), N, MAX_TIMESTAMP, 100);
        ASSERT_TRUE(before_expire[0]);
        ASSERT_TRUE(before_expire[1]);

        auto after_expire =
            ExecuteFilterBitsWithFullCache(plan, seg.get(), N, MAX_TIMESTAMP, 200);
        ASSERT_FALSE(after_expire[0]);
        ASSERT_TRUE(after_expire[1]);
    });
}
