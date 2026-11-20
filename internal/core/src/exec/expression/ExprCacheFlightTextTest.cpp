// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <folly/ScopeGuard.h>
#include <future>
#include <thread>

#include "exec/expression/ExprCacheFlight.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/segment_c.h"
#include "segcore/test_utils/TextMatchTestUtils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::segcore::text_test;

TEST(ExprCacheFlightTextTest,
     PhraseMatchUsesFlightAndKeepsSlopAndNullSemantics) {
    using namespace milvus::exec;
    using namespace std::chrono_literals;
    auto& manager = ExprResCacheManager::Instance();
    auto& flights = manager.Flights();
    ExprResCacheManager::SetEnabled(false);
    auto cleanup = folly::makeGuard([&] {
        flights.Configure({});
        manager.Clear();
        ExprResCacheManager::SetEnabled(false);
    });
    auto schema = GenTestSchema({}, true);
    auto data = DataGen(schema, 4);
    auto* strings = data.raw_->mutable_fields_data()->at(1).mutable_scalars();
    const std::vector<std::string> text{
        "red blue", "red green blue", "blue red", ""};
    for (size_t i = 0; i < text.size(); ++i) {
        strings->mutable_string_data()->set_data(i, text[i]);
    }
    strings->set_valid_data(3, false);
    auto segment = CreateSealedWithFieldDataLoaded(schema, data);
    segment->CreateTextIndex(FieldId(101));
    auto phrase = GetMatchExpr(schema, "red blue", OpType::PhraseMatch, 0);
    auto expected = ExecuteQueryExpr(phrase, segment.get(), 4, MAX_TIMESTAMP);
    EXPECT_EQ(expected.count(), 1);

    CacheConfig cache;
    cache.mode = CacheMode::Memory;
    cache.admission_threshold = 1;
    cache.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(cache));
    ExprResCacheManager::SetEnabled(true);
    ExprCacheFlightConfig config;
    config.enabled = true;
    config.max_wait_ms = 5000;
    ASSERT_TRUE(flights.Configure(config));

    auto snapshot = segment->CaptureReadSnapshot();
    ExprCacheFlightCoordinator::Key key{{segment->get_segment_id(),
                                         phrase->filter()->ToString(),
                                         snapshot->GetExprCacheGeneration()},
                                        4,
                                        manager.ConfigEpoch()};
    // A controlled producer lets six real expression executions reach the
    // follower branch deterministically, without timing the text index.
    auto producer = flights.TryJoin(key);
    ASSERT_TRUE(producer.IsLeader());
    std::vector<std::future<BitsetType>> followers;
    auto abort = folly::makeGuard([&] { producer.Reset(); });
    for (int i = 0; i < 6; ++i) {
        followers.push_back(std::async(std::launch::async, [&] {
            return ExecuteQueryExpr(phrase, segment.get(), 4, MAX_TIMESTAMP);
        }));
    }
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (flights.GetStats().waiters != 6 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(flights.GetStats().waiters, 6);
    auto lease = manager.TryAcquireMaterialization(4);
    EXPECT_TRUE(lease.has_value());
    TargetBitmap result(4, false), valid(4, true);
    for (size_t i = 0; i < 4; ++i) result[i] = expected[i];
    valid[3] = false;
    if (lease)
        flights.Complete(producer,
                         std::make_shared<ExprCacheFlightCoordinator::Bitmaps>(
                             std::move(*lease), result, valid));
    producer.Reset();
    abort.dismiss();
    for (auto& follower : followers) {
        auto actual = follower.get();
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); ++i)
            EXPECT_EQ(actual[i], expected[i]);
    }
    EXPECT_EQ(manager.GetEntryCount(), 0);  // Followers never duplicate Put.

    // With no pre-registered producer, the real PhraseMatch lambda leads and
    // writes a result, and a different slop remains a different key.
    EXPECT_EQ(ExecuteQueryExpr(phrase, segment.get(), 4, MAX_TIMESTAMP).count(),
              1);
    auto with_slop = GetMatchExpr(schema, "red blue", OpType::PhraseMatch, 1);
    EXPECT_EQ(
        ExecuteQueryExpr(with_slop, segment.get(), 4, MAX_TIMESTAMP).count(),
        2);
    EXPECT_EQ(manager.GetEntryCount(), 2);

    // A cancellation received while the real expression is waiting must keep
    // the existing Driver -> SegcoreError cancellation classification.
    auto cancelled_phrase =
        GetMatchExpr(schema, "green blue", OpType::PhraseMatch, 0);
    auto cancel_key = key;
    cancel_key.cache_key.signature = cancelled_phrase->filter()->ToString();
    auto cancel_producer = flights.TryJoin(cancel_key);
    folly::CancellationSource cancellation;
    auto cancelled = std::async(std::launch::async, [&] {
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment.get(), 4, MAX_TIMESTAMP);
        query_context->set_read_snapshot(segment->CaptureReadSnapshot());
        auto op_context = milvus::OpContext(cancellation.getToken());
        query_context->set_op_context(&op_context);
        auto plan = plan::PlanFragment(cancelled_phrase);
        try {
            ExecPlanNodeVisitor::ExecuteTask(plan, query_context);
            return ErrorCode::UnexpectedError;
        } catch (const SegcoreError& error) {
            return error.get_error_code();
        }
    });
    const auto cancel_deadline = std::chrono::steady_clock::now() + 3s;
    while (flights.GetStats().waiters != 1 &&
           std::chrono::steady_clock::now() < cancel_deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(flights.GetStats().waiters, 1);
    cancellation.requestCancellation();
    EXPECT_EQ(cancelled.get(), ErrorCode::FollyCancel);
    cancel_producer.Reset();

    for (auto op : {OpType::TextMatch, OpType::TextMatchFuzzy}) {
        auto text_match = GetMatchExpr(schema, "red", op, 0);
        auto text_key = key;
        text_key.cache_key.signature = text_match->filter()->ToString();
        auto text_producer = flights.TryJoin(text_key);
        auto text_follower = std::async(std::launch::async, [&] {
            return ExecuteQueryExpr(
                text_match, segment.get(), 4, MAX_TIMESTAMP);
        });
        auto release_text = folly::makeGuard([&] { text_producer.Reset(); });
        const auto text_deadline = std::chrono::steady_clock::now() + 3s;
        while (flights.GetStats().waiters != 1 &&
               std::chrono::steady_clock::now() < text_deadline) {
            std::this_thread::yield();
        }
        EXPECT_EQ(flights.GetStats().waiters, 1);
        TargetBitmap text_result(4, true), text_valid(4, true);
        text_result[3] = false;
        text_valid[3] = false;
        auto text_lease = manager.TryAcquireMaterialization(4);
        ASSERT_TRUE(text_lease.has_value());
        flights.Complete(text_producer,
                         std::make_shared<ExprCacheFlightCoordinator::Bitmaps>(
                             std::move(*text_lease), text_result, text_valid));
        text_producer.Reset();
        EXPECT_EQ(text_follower.get().count(), 3);
        // Exercise the real producer too, with the operator-specific key.
        EXPECT_EQ(ExecuteQueryExpr(text_match, segment.get(), 4, MAX_TIMESTAMP)
                      .count(),
                  3);
    }
    EXPECT_EQ(flights.GetStats().flights, 0);
    EXPECT_EQ(manager.GetMaterializationBytes(), 0);
}
