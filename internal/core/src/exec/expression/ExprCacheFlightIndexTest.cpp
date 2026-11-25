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
#include <atomic>
#include <condition_variable>
#include <future>
#include <filesystem>
#include <mutex>
#include <thread>

#include "exec/expression/ExprBatchTestUtils.h"
#include "exec/expression/ExprCacheFlight.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "query/ExecPlanNodeVisitor.h"

namespace milvus::test {
namespace {
using namespace exec;
using namespace std::chrono_literals;

// Block validity after the real predicate ran: publishing only the predicate
// result would allow followers to escape before the complete result is ready.
struct ReaderGate {
    std::atomic<int> predicates{0};
    std::mutex mutex;
    std::condition_variable cv;
    bool blocked{false};
    bool entered{false};

    void
    Enter() {
        std::unique_lock lock(mutex);
        entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return !blocked; });
    }
    void
    Release() {
        std::lock_guard lock(mutex);
        blocked = false;
        cv.notify_all();
    }
};

class GatedIntReader final : public index::IIndexReaderBase,
                             public index::IScalarPredicateReader<int64_t>,
                             public index::INullReader {
 public:
    GatedIntReader(index::IIndexReaderBasePtr inner,
                   std::shared_ptr<ReaderGate> gate)
        : inner_(std::move(inner)), gate_(std::move(gate)) {
    }
    index::ReaderCaps
    Caps() const override {
        return inner_->Caps();
    }
    index::Domain
    CoordDomain() const override {
        return inner_->CoordDomain();
    }
    int64_t
    Count() const override {
        return inner_->Count();
    }
    DataType
    ValueType() const override {
        return inner_->ValueType();
    }
    void
    Prefetch() const override {
        inner_->Prefetch();
    }
    TargetBitmap
    In(size_t n, const int64_t* values) const override {
        ++gate_->predicates;
        return Predicate().In(n, values);
    }
    TargetBitmap
    NotIn(size_t n, const int64_t* values) const override {
        ++gate_->predicates;
        return Predicate().NotIn(n, values);
    }
    TargetBitmap
    Range(const int64_t& value, index::CompareOp op) const override {
        ++gate_->predicates;
        return Predicate().Range(value, op);
    }
    TargetBitmap
    Range(const int64_t& lo,
          bool lo_inc,
          const int64_t& hi,
          bool hi_inc) const override {
        ++gate_->predicates;
        return Predicate().Range(lo, lo_inc, hi, hi_inc);
    }
    TargetBitmap
    IsNull() const override {
        return Nulls().IsNull();
    }
    TargetBitmap
    IsNotNull() const override {
        gate_->Enter();
        return Nulls().IsNotNull();
    }

 private:
    const index::IScalarPredicateReader<int64_t>&
    Predicate() const {
        return dynamic_cast<const index::IScalarPredicateReader<int64_t>&>(
            *inner_);
    }
    const index::INullReader&
    Nulls() const {
        return dynamic_cast<const index::INullReader&>(*inner_);
    }
    index::IIndexReaderBasePtr inner_;
    std::shared_ptr<ReaderGate> gate_;
};

proto::plan::GenericValue
Int(int64_t value) {
    proto::plan::GenericValue result;
    result.set_int64_val(value);
    return result;
}

void
ExpectSame(const ColumnVectorPtr& actual, const ColumnVectorPtr& expected) {
    ASSERT_EQ(actual->size(), expected->size());
    TargetBitmapView a(actual->GetRawData(), actual->size());
    TargetBitmapView av(actual->GetValidRawData(), actual->size());
    TargetBitmapView e(expected->GetRawData(), expected->size());
    TargetBitmapView ev(expected->GetValidRawData(), expected->size());
    for (size_t i = 0; i < actual->size(); ++i) {
        EXPECT_EQ(av[i], ev[i]) << i;
        if (ev[i])
            EXPECT_EQ(a[i], e[i]) << i;
    }
}

class ExprCacheFlightIndexTest : public ::testing::TestWithParam<CacheMode> {
 protected:
    static constexpr int64_t rows = 6;
    ExprResCacheManager& manager = ExprResCacheManager::Instance();
    ExprCacheFlightCoordinator& flights = manager.Flights();
    expr_index::RawFieldFiles files;
    segcore::SegmentSealedUPtr segment;
    std::shared_ptr<ReaderGate> gate = std::make_shared<ReaderGate>();
    FieldId field{100};
    std::filesystem::path directory;

    void
    SetUp() override {
        ExprResCacheManager::SetEnabled(false);
        manager.Clear();
        CacheConfig cache;
        directory =
            std::filesystem::temp_directory_path() /
            ("expr-index-flight-" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
        cache.mode = GetParam();
        cache.disk_base_path = directory.string();
        cache.disk_min_eval_duration_us = 0;
        cache.admission_threshold = 1;
        cache.mem_min_eval_duration_us = 0;
        ASSERT_TRUE(manager.SetConfig(cache));
        ExprCacheFlightConfig config;
        config.enabled = true;
        config.max_wait_ms = 5000;
        ASSERT_TRUE(flights.Configure(config));
    }
    void
    TearDown() override {
        EXPECT_EQ(flights.GetStats().waiters, 0);
        EXPECT_EQ(flights.GetStats().flights, 0);
        EXPECT_EQ(manager.GetMaterializationBytes(), 0);
        flights.Configure({});
        manager.Clear();
        ExprResCacheManager::SetEnabled(false);
        std::filesystem::remove_all(directory);
    }
    void
    Load(bool json) {
        auto schema = std::make_shared<Schema>();
        auto type = json ? DataType::JSON : DataType::INT64;
        field = schema->AddDebugField("value", type, true);
        FieldDataPtr data;
        if (json) {
            data = expr_index::JsonField({R"({"a": 1})",
                                          R"({"a": [1, 2]})",
                                          R"({"a": 3})",
                                          R"({"a": "x"})",
                                          R"({"a": null})",
                                          R"({})"},
                                         true);
        } else {
            auto ints = std::make_shared<FieldData<int64_t>>(type, true);
            const int64_t values[] = {0, 1, 2, 3, 4, 5};
            const uint8_t valid[] = {0x1F};
            ints->FillFieldData(values, valid, rows, 0);
            data = ints;
        }
        segment = segcore::CreateSealedSegment(schema);
        auto info = files.Prepare(field, {data});
        segment->LoadFieldData(info);
        auto opened = expr_index::BuildIndex(
            field,
            type,
            index::INVERTED_INDEX_TYPE,
            {data},
            json ? Config{{JSON_PATH, ""}, {JSON_CAST_TYPE, "JSON"}}
                 : Config::object());
        if (!json) {
            expr_index::DecorateIndex(opened, [g = gate](auto reader) {
                return std::make_unique<GatedIntReader>(std::move(reader), g);
            });
        }
        expr_index::InstallIndex(*segment, field, type, std::move(opened));
    }
    void
    LoadArray() {
        auto schema = std::make_shared<Schema>();
        field =
            schema->AddDebugArrayField("items[value]", DataType::INT64, false);
        std::vector<Array> arrays;
        for (int64_t i = 0; i < rows; ++i) {
            proto::schema::ScalarField value;
            value.mutable_long_data()->add_data(i);
            arrays.emplace_back(value);
        }
        auto data = std::make_shared<FieldData<Array>>(DataType::ARRAY, false);
        data->FillFieldData(arrays.data(), arrays.size());
        segment = segcore::CreateSealedSegment(schema);
        auto info = files.Prepare(field, {data});
        segment->LoadFieldData(info);
        auto opened = expr_index::BuildIndex(field,
                                             DataType::ARRAY,
                                             index::INVERTED_INDEX_TYPE,
                                             {data},
                                             Config::object(),
                                             DataType::INT64,
                                             true);
        ASSERT_EQ(opened.reader->CoordDomain(), index::Domain::Element);
        ASSERT_EQ(opened.reader->Count(), rows);
        expr_index::InstallIndex(*segment,
                                 field,
                                 DataType::ARRAY,
                                 std::move(opened),
                                 DataType::INT64);
    }
    std::vector<expr::TypedExprPtr>
    Predicates(bool json) {
        expr::ColumnInfo column(
            field,
            json ? DataType::JSON : DataType::INT64,
            json ? std::vector<std::string>{"a"} : std::vector<std::string>{});
        return {std::make_shared<expr::UnaryRangeFilterExpr>(
                    column, proto::plan::Equal, Int(1)),
                std::make_shared<expr::UnaryRangeFilterExpr>(
                    column, proto::plan::GreaterThan, Int(1)),
                std::make_shared<expr::BinaryRangeFilterExpr>(
                    column, Int(1), Int(3), true, false),
                std::make_shared<expr::TermFilterExpr>(
                    column, std::vector{Int(1), Int(3)})};
    }
    bool
    Waiters(size_t count) {
        auto deadline = std::chrono::steady_clock::now() + 3s;
        while (flights.GetStats().waiters != count &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        return flights.GetStats().waiters == count;
    }
    ExprCacheFlightCoordinator::Key
    Key(const expr::TypedExprPtr& expr) {
        return {{segment->get_segment_id(),
                 expr->ToString(),
                 segment->CaptureReadSnapshot()->GetExprCacheGeneration()},
                rows,
                manager.ConfigEpoch()};
    }
};

TEST_P(ExprCacheFlightIndexTest, ScalarPredicatesComputeOnceIncludingValidity) {
    Load(false);
    ExprBatchSizeGuard batches(2);
    for (const auto& expr : Predicates(false)) {
        SCOPED_TRACE(expr->ToString());
        ExprResCacheManager::SetEnabled(false);
        manager.Clear();
        auto expected = EvalExprInBatches(expr, segment.get(), rows);
        gate->predicates = 0;
        gate->blocked = true;
        gate->entered = false;
        ExprResCacheManager::SetEnabled(true);
        std::vector<std::future<ExprBatchEvalResult>> requests;
        auto release = folly::makeGuard([&] { gate->Release(); });
        auto evaluate = [&] {
            return EvalExprInBatches(expr, segment.get(), rows);
        };
        requests.push_back(std::async(std::launch::async, evaluate));
        {
            std::unique_lock lock(gate->mutex);
            EXPECT_TRUE(
                gate->cv.wait_for(lock, 3s, [&] { return gate->entered; }));
        }
        for (int i = 0; i < 3; ++i)
            requests.push_back(std::async(std::launch::async, evaluate));
        EXPECT_TRUE(Waiters(3));
        EXPECT_EQ(gate->predicates.load(), 1);
        for (auto& request : requests)
            EXPECT_EQ(request.wait_for(0ms), std::future_status::timeout);
        gate->Release();
        std::vector<ColumnVectorPtr> results;
        for (auto& request : requests) {
            auto actual = request.get();
            EXPECT_EQ(actual.batch_sizes, expected.batch_sizes);
            ExpectSame(actual.result, expected.result);
            results.push_back(actual.result);
        }
        EXPECT_EQ(gate->predicates.load(), 1);
        EXPECT_EQ(manager.GetEntryCount(), 1);
        // Neither a follower nor the cache may alias a request's mutable output.
        TargetBitmapView(results[1]->GetRawData(), rows).flip();
        TargetBitmapView(results[1]->GetValidRawData(), rows).reset();
        ExpectSame(results[0], expected.result);
        ExpectSame(results[2], expected.result);
        ExpectSame(EvalExprInBatches(expr, segment.get(), rows).result,
                   expected.result);
        EXPECT_EQ(gate->predicates.load(), 1);
    }
}

TEST_P(ExprCacheFlightIndexTest, JsonRowPredicatesAndExistsJoinFlights) {
    Load(true);
    ExprBatchSizeGuard batches(2);
    auto predicates = Predicates(true);
    expr::ColumnInfo column(field, DataType::JSON, {"a"});
    predicates.push_back(std::make_shared<expr::ExistsExpr>(column));
    predicates.push_back(std::make_shared<expr::JsonContainsExpr>(
        column,
        proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
        true,
        std::vector{Int(1), Int(2)}));
    for (const auto& expr : predicates) {
        SCOPED_TRACE(expr->ToString());
        ExprResCacheManager::SetEnabled(false);
        manager.Clear();
        auto expected = EvalExprInBatches(expr, segment.get(), rows);
        ExprResCacheManager::SetEnabled(true);
        auto producer = flights.TryJoin(Key(expr));
        ASSERT_TRUE(producer.IsLeader());
        auto follower = std::async(std::launch::async, [&] {
            return EvalExprInBatches(expr, segment.get(), rows);
        });
        auto release = folly::makeGuard([&] { producer.Reset(); });
        EXPECT_TRUE(Waiters(1));
        auto lease = manager.TryAcquireMaterialization(rows);
        ASSERT_TRUE(lease.has_value());
        TargetBitmap result(
            TargetBitmapView(expected.result->GetRawData(), rows));
        TargetBitmap valid(
            TargetBitmapView(expected.result->GetValidRawData(), rows));
        flights.Complete(producer,
                         std::make_shared<ExprCacheFlightCoordinator::Bitmaps>(
                             std::move(*lease), result, valid));
        producer.Reset();
        auto actual = follower.get();
        EXPECT_EQ(actual.batch_sizes, expected.batch_sizes);
        ExpectSame(actual.result, expected.result);
        EXPECT_EQ(manager.GetEntryCount(), 0);
        // Real producer, including nested JSON validity-cache computation.
        ExpectSame(EvalExprInBatches(expr, segment.get(), rows).result,
                   expected.result);
        EXPECT_GT(manager.GetEntryCount(), 0);
    }
}

TEST_P(ExprCacheFlightIndexTest, ScalarFollowerCancellationReachesDriver) {
    Load(false);
    auto expr = Predicates(false).front();
    ExprResCacheManager::SetEnabled(true);
    auto producer = flights.TryJoin(Key(expr));
    ASSERT_TRUE(producer.IsLeader());
    folly::CancellationSource cancellation;
    auto follower = std::async(std::launch::async, [&] {
        auto context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment.get(), rows, MAX_TIMESTAMP);
        context->set_read_snapshot(segment->CaptureReadSnapshot());
        OpContext op(cancellation.getToken());
        context->set_op_context(&op);
        auto node =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto plan = plan::PlanFragment(node);
        try {
            query::ExecPlanNodeVisitor::ExecuteTask(plan, context);
            return ErrorCode::UnexpectedError;
        } catch (const SegcoreError& error) {
            return error.get_error_code();
        }
    });
    auto release = folly::makeGuard([&] { producer.Reset(); });
    EXPECT_TRUE(Waiters(1));
    cancellation.requestCancellation();
    EXPECT_EQ(follower.get(), ErrorCode::FollyCancel);
    EXPECT_EQ(gate->predicates.load(), 0);
    EXPECT_EQ(manager.GetEntryCount(), 0);
}

TEST_P(ExprCacheFlightIndexTest, ElementDomainBypassesEvenWhenCountsEqual) {
    LoadArray();
    expr::ColumnInfo column(field, DataType::ARRAY, DataType::INT64);
    column.element_level_ = true;
    auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        column, proto::plan::GreaterThan, Int(1));
    auto expected = EvalExprInBatches(expr, segment.get(), rows).result;
    ExprResCacheManager::SetEnabled(true);
    auto producer = flights.TryJoin(Key(expr));
    ASSERT_TRUE(producer.IsLeader());
    auto request = std::async(std::launch::async, [&] {
        return EvalExprInBatches(expr, segment.get(), rows).result;
    });
    auto release = folly::makeGuard([&] { producer.Reset(); });
    EXPECT_EQ(request.wait_for(3s), std::future_status::ready);
    EXPECT_EQ(flights.GetStats().waiters, 0);
    producer.Reset();
    ExpectSame(request.get(), expected);
}

TEST_P(ExprCacheFlightIndexTest,
       ArrayContainsJoinsAfterElementToRowProjection) {
    LoadArray();
    expr::ColumnInfo column(field, DataType::ARRAY, DataType::INT64);
    auto expr = std::make_shared<expr::JsonContainsExpr>(
        column,
        proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
        true,
        std::vector{Int(1), Int(3)});
    auto expected = EvalExprInBatches(expr, segment.get(), rows).result;
    ExprResCacheManager::SetEnabled(true);
    auto producer = flights.TryJoin(Key(expr));
    ASSERT_TRUE(producer.IsLeader());
    auto request = std::async(std::launch::async, [&] {
        return EvalExprInBatches(expr, segment.get(), rows).result;
    });
    auto release = folly::makeGuard([&] { producer.Reset(); });
    EXPECT_TRUE(Waiters(1));
    auto lease = manager.TryAcquireMaterialization(rows);
    ASSERT_TRUE(lease.has_value());
    TargetBitmap result(TargetBitmapView(expected->GetRawData(), rows));
    TargetBitmap valid(TargetBitmapView(expected->GetValidRawData(), rows));
    flights.Complete(producer,
                     std::make_shared<ExprCacheFlightCoordinator::Bitmaps>(
                         std::move(*lease), result, valid));
    producer.Reset();
    ExpectSame(request.get(), expected);
    EXPECT_EQ(manager.GetEntryCount(), 0);
    ExpectSame(EvalExprInBatches(expr, segment.get(), rows).result, expected);
    EXPECT_EQ(manager.GetEntryCount(), 1);
}

INSTANTIATE_TEST_SUITE_P(Backends,
                         ExprCacheFlightIndexTest,
                         ::testing::Values(CacheMode::Memory, CacheMode::Disk),
                         [](const ::testing::TestParamInfo<CacheMode>& info) {
                             return info.param == CacheMode::Memory ? "Memory"
                                                                    : "Disk";
                         });

}  // namespace
}  // namespace milvus::test
