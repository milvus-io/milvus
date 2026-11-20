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

#include <future>
#include <chrono>
#include <atomic>
#include <folly/ScopeGuard.h>

#include "exec/operator/VectorSearchNode.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentGrowingImpl.h"

namespace milvus::exec {
namespace {

TEST(PrefetchExecutorTest, StoppingQueuedWorkReleasesItWhilePoolIsBusy) {
    auto pool = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    std::promise<void> release;
    auto gate = release.get_future().share();
    auto blocker = folly::via(pool.get(), [gate] { gate.wait(); });
    auto unblock = folly::makeGuard([&] { release.set_value(); });

    folly::CancellationSource stop;
    PrefetchExecutor closing(pool, stop.getToken());
    auto owner = std::make_shared<int>(1);
    std::weak_ptr<int> borrowed = owner;
    auto discarded =
        closing.Submit([owner] { ADD_FAILURE() << "obsolete read"; });
    owner.reset();
    auto other = PrefetchExecutor(pool).Submit(
        [] { EXPECT_FALSE(PrefetchExecutor::IsStopped()); });

    stop.requestCancellation();
    EXPECT_TRUE(discarded.isReady());
    EXPECT_TRUE(borrowed.expired());
    EXPECT_FALSE(other.isReady());
    unblock.dismiss();
    release.set_value();
    std::move(blocker).get();
    std::move(discarded).get();
    std::move(other).get();
}

TEST(PrefetchExecutorTest, StopRacesWorkerAcquisition) {
    auto pool = std::make_shared<folly::CPUThreadPoolExecutor>(2);
    for (int attempt = 0; attempt < 100; ++attempt) {
        folly::CancellationSource stop;
        std::atomic<int> calls{0};
        auto cancel =
            std::async(std::launch::async, [&] { stop.requestCancellation(); });
        auto done = PrefetchExecutor(pool, stop.getToken()).Submit([&] {
            calls.fetch_add(1, std::memory_order_relaxed);
        });
        EXPECT_NO_THROW(std::move(done).get());
        cancel.get();
        EXPECT_LE(calls.load(), 1);
        auto after_stop = PrefetchExecutor(pool, stop.getToken()).Submit([] {
            ADD_FAILURE() << "submitted after stop";
        });
        EXPECT_TRUE(after_stop.isReady());
        EXPECT_NO_THROW(std::move(after_stop).get());
    }
}

TEST(VectorSearchNodeTest, DriverStopsRunningPrefetchBeforeJoining) {
    class FinishedNode : public PhyVectorSearchNode {
     public:
        using PhyVectorSearchNode::PhyVectorSearchNode;
        void
        PrefetchAsync(const PrefetchExecutor& executor) override {
            pending.emplace(executor.Submit([this] {
                started.set_value();
                finish.get_future().wait();
                stopped = PrefetchExecutor::IsStopped();
            }));
        }
        RowVectorPtr
        GetOutput() override {
            EXPECT_EQ(started.get_future().wait_for(std::chrono::seconds(5)),
                      std::future_status::ready);
            return nullptr;
        }
        bool
        IsFinished() override {
            return true;
        }
        void
        WaitPrefetch() override {
            finish.set_value();
            if (pending) {
                std::move(*pending).get();
            }
        }
        std::promise<void> started;
        std::promise<void> finish;
        std::optional<folly::Future<folly::Unit>> pending;
        bool stopped = false;
    };

    auto schema = std::make_shared<Schema>();
    auto field = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(schema->AddDebugField("pk", DataType::INT64));
    auto segment = segcore::CreateGrowingSegment(schema, empty_index_meta);
    folly::CancellationSource request;
    OpContext op_ctx(request.getToken());
    auto ctx = std::make_shared<QueryContext>(
        "driver_stop", segment.get(), 0, MAX_TIMESTAMP);
    ctx->set_op_context(&op_ctx);
    SearchInfo info{};
    info.field_id_ = field;
    ctx->set_search_info(info);
    auto node = std::make_shared<plan::VectorSearchNode>("ann");
    auto task = Task::Create("driver_stop", plan::PlanFragment(node), 0, ctx);
    DriverFactory factory{};
    FinishedNode* probe = nullptr;
    factory.consumer_supplier_ = [&](int id, DriverContext* driver) {
        auto op = std::make_unique<FinishedNode>(id, driver, node);
        probe = op.get();
        return op;
    };
    auto driver =
        factory.CreateDriver(std::make_unique<DriverContext>(task, 0, 0, 0, 0),
                             [](int) { return 1; });
    std::shared_ptr<BlockingState> blocked;
    EXPECT_EQ(driver->Next(blocked), nullptr);
    ASSERT_NE(probe, nullptr);
    EXPECT_TRUE(probe->stopped);
    EXPECT_FALSE(request.isCancellationRequested());
}

TEST(VectorSearchNodeTest, JoinsVectorPrefetchOnlyWhenANNConsumesInput) {
    class JoinProbe : public PhyVectorSearchNode {
     public:
        using PhyVectorSearchNode::PhyVectorSearchNode;
        void
        WaitPrefetch() override {
            ++joins;
            throw std::runtime_error("reached vector index consumption");
        }
        int joins = 0;
    };
    auto schema = std::make_shared<Schema>();
    auto field = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(schema->AddDebugField("pk", DataType::INT64));
    auto segment = segcore::CreateGrowingSegment(schema, empty_index_meta);
    auto ctx = std::make_shared<QueryContext>(
        "vector_join", segment.get(), 1, MAX_TIMESTAMP);
    SearchInfo info{};
    info.field_id_ = field;
    info.topk_ = 1;
    ctx->set_search_info(info);
    query::PlaceholderGroup placeholders;
    placeholders.emplace_back().num_of_queries_ = 1;
    ctx->set_placeholder_group(&placeholders);
    auto node = std::make_shared<plan::VectorSearchNode>("ann");
    auto task = Task::Create("vector_join", plan::PlanFragment(node), 0, ctx);
    DriverContext driver(task, 0, 0, 0, 0);

    for (bool all_filtered : {true, false}) {
        JoinProbe probe(0, &driver, node);
        EXPECT_EQ(probe.GetOutput(), nullptr);
        auto bitmap = std::make_shared<ColumnVector>(
            TargetBitmap(1, all_filtered), TargetBitmap(1, true));
        auto input =
            std::make_shared<RowVector>(std::vector<VectorPtr>{bitmap});
        probe.AddInput(input);
        EXPECT_EQ(probe.GetOutput(), nullptr);
        EXPECT_EQ(probe.joins, 0);
        probe.NoMoreInput();
        if (all_filtered) {
            EXPECT_NE(probe.GetOutput(), nullptr);
            EXPECT_EQ(probe.joins, 0);
        } else {
            EXPECT_THROW(probe.GetOutput(), std::runtime_error);
            EXPECT_EQ(probe.joins, 1);
        }
    }
}

}  // namespace
}  // namespace milvus::exec
