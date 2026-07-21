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

// Tests for exec/Driver.cpp's CALL_OPERATOR wrap: how an exception thrown
// inside an operator is classified as it crosses back through the driver to
// the CGO boundary. These guard error-code preservation and cancellation
// mapping, so they live next to Driver.cpp rather than in an expression suite.

#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <string>

#include "folly/CancellationToken.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "knowhere/comp/index_param.h"

#include "common/EasyAssert.h"
#include "common/Exception.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/type_c.h"
#include "exec/QueryContext.h"
#include "expr/ITypeExpr.h"
#include "futures/Future.h"
#include "futures/future_c_types.h"
#include "monitor/Monitor.h"
#include "pb/plan.pb.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

// The driver's CALL_OPERATOR wrap (exec/Driver.cpp) must preserve the error
// code of a classified SegcoreError thrown during operator execution instead
// of collapsing it to UnexpectedError: a user-fixable error (e.g. ExprInvalid)
// must not reach the CGO boundary looking like a retriable internal failure.
TEST(DriverTest, PreservesSegcoreErrorCode) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    // A NullExpr with an Invalid op on a non-nullable column passes plan
    // construction but throws SegcoreError(ExprInvalid) inside
    // PhyNullExpr::PreCheckNullable during operator execution — exactly the
    // classified-throw-inside-CALL_OPERATOR path this test pins down.
    auto null_expr = std::make_shared<milvus::expr::NullExpr>(
        milvus::expr::ColumnInfo(i64_fid, DataType::INT64),
        proto::plan::NullExpr_NullOp_Invalid);
    auto plan = milvus::test::CreateRetrievePlanByExpr(null_expr);

    try {
        auto result = ExecuteQueryExpr(plan, seg_promote, N, MAX_TIMESTAMP);
        FAIL() << "expected ExecOperatorException";
    } catch (const milvus::ExecOperatorException& e) {
        // The classified code chosen at the throw site survives the driver
        // wrap; the message still carries the operator context.
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::ExprInvalid);
        EXPECT_NE(std::string(e.what()).find("unsupported null expr type"),
                  std::string::npos);
        EXPECT_NE(std::string(e.what()).find("Operator::"), std::string::npos);
    }

    // The wrapper must also be catchable as SegcoreError with the same code —
    // this is what FailureCStatus(const std::exception*) and the futures
    // consume arm (Future.h thenError<milvus::SegcoreError>) rely on to build
    // the CStatus that crosses the CGO boundary.
    try {
        auto result = ExecuteQueryExpr(plan, seg_promote, N, MAX_TIMESTAMP);
        FAIL() << "expected SegcoreError";
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::ExprInvalid);
        CStatus status = milvus::FailureCStatus(&e);
        EXPECT_EQ(status.error_code,
                  static_cast<int>(milvus::ErrorCode::ExprInvalid));
        free((void*)status.error_msg);
    }
}

// A query executed under an already-canceled token throws
// folly::FutureCancellation inside the operator; CALL_OPERATOR (exec/Driver.cpp)
// must map it to ErrorCode::FollyCancel so a canceled/timed-out query surfaces
// as a real cancellation at the CGO boundary instead of a retriable
// UnexpectedError. Cancellation is independent of the vector type, so this runs
// once rather than being parameterized over metric types.
TEST(DriverTest, CancellationMapsToFollyCancel) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->AddDebugField("counter", DataType::INT64);
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    // Build a search plan for "counter > 500".
    ScopedSchemaHandle schema_handle(*schema);
    auto bin_plan = schema_handle.ParseSearch("counter > 500",
                                              "fakevec",
                                              10,
                                              knowhere::metric::L2,
                                              R"({"nprobe": 10})",
                                              3);
    auto plan =
        CreateSearchPlanByExpr(schema, bin_plan.data(), bin_plan.size());

    // Request cancellation before executing.
    folly::CancellationSource cancellation_source;
    auto cancellation_token = cancellation_source.getToken();
    cancellation_source.requestCancellation();

    query::ExecPlanNodeVisitor visitor(
        *seg_promote, MAX_TIMESTAMP, cancellation_token);
    try {
        auto result = visitor.get_moved_result(*plan->plan_node_);
        FAIL() << "expected ExecOperatorException";
    } catch (const milvus::ExecOperatorException& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::FollyCancel);
    }
}

// checkCancellation is the operator-side probe that raises FutureCancellation.
// It must be a no-op when there is no query/op context, and throw only once a
// token has actually been canceled.
TEST(DriverTest, CheckCancellationHelper) {
    // Does nothing when query_context is nullptr.
    ASSERT_NO_THROW(milvus::exec::checkCancellation(nullptr));

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    auto query_context = std::make_unique<milvus::exec::QueryContext>(
        "test_query", seg_promote, 0, MAX_TIMESTAMP);

    // Does not throw when op_context is nullptr.
    ASSERT_NO_THROW(milvus::exec::checkCancellation(query_context.get()));

    folly::CancellationSource source;
    milvus::OpContext op_context(source.getToken());
    query_context->set_op_context(&op_context);

    // Does not throw while the token is not canceled.
    ASSERT_NO_THROW(milvus::exec::checkCancellation(query_context.get()));

    // Throws once the token is canceled.
    source.requestCancellation();
    ASSERT_THROW(milvus::exec::checkCancellation(query_context.get()),
                 folly::FutureCancellation);
}

// The same classified ExprInvalid failure must also carry its code across the
// ASYNC Future/consume path (futures/Future.h) — the arm the real search /
// retrieve CGO boundary uses, distinct from the synchronous FailureCStatus path
// in PreservesSegcoreErrorCode. A driver-raised ExecOperatorException (a
// SegcoreError) must land in the thenError<milvus::SegcoreError> consume arm and
// reach CStatus as ExprInvalid (2028), not collapse to a generic error.
TEST(DriverTest, AsyncConsumePreservesSegcoreErrorCode) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    auto null_expr = std::make_shared<milvus::expr::NullExpr>(
        milvus::expr::ColumnInfo(i64_fid, DataType::INT64),
        proto::plan::NullExpr_NullOp_Invalid);
    auto plan = milvus::test::CreateRetrievePlanByExpr(null_expr);

    // Drive the plan through the async future: ExecuteQueryExpr runs the exec
    // Driver, whose CALL_OPERATOR throws ExecOperatorException(ExprInvalid); the
    // exception crosses asyncProduce into the SegcoreError consume arm.
    folly::CPUThreadPoolExecutor executor(2);
    auto future = milvus::futures::Future<int>::async(
        &executor, 0, [&](folly::CancellationToken token) {
            ExecuteQueryExpr(plan, seg_promote, N, MAX_TIMESTAMP);
            return new int(0);  // unreachable: the call above throws
        });

    std::mutex mu;
    mu.lock();
    future->registerReadyCallback(
        [](CLockedGoMutex* m) { ((std::mutex*)(m))->unlock(); },
        (CLockedGoMutex*)(&mu));
    mu.lock();
    ASSERT_TRUE(future->isReady());

    auto [r, s] = future->leakyGet();
    EXPECT_EQ(r, nullptr);
    EXPECT_EQ(s.error_code, static_cast<int>(milvus::ErrorCode::ExprInvalid));
    free((char*)(s.error_msg));
}

// A cancellation raised inside the exec driver reaches the async future as a
// SegcoreError carrying FollyCancel (not a raw folly::FutureCancellation), so
// asyncProduce must still count it as a during-execute cancel. The future's own
// token is left uncanceled (bypassing the runner's early-cancel prologue) and a
// SEPARATE, pre-canceled token is handed to the driver, so the cancellation is
// raised mid-execution by an operator -- exactly the wrapped path this metric
// fix covers.
TEST(DriverTest, AsyncDriverCancellationCountsDuringMetric) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->AddDebugField("counter", DataType::INT64);
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    ScopedSchemaHandle schema_handle(*schema);
    auto bin_plan = schema_handle.ParseSearch("counter > 500",
                                              "fakevec",
                                              10,
                                              knowhere::metric::L2,
                                              R"({"nprobe": 10})",
                                              3);
    auto plan =
        CreateSearchPlanByExpr(schema, bin_plan.data(), bin_plan.size());

    auto& counter =
        milvus::monitor::internal_cgo_cancel_during_execute_total_search;
    auto before = counter.Value();
    {
        folly::CPUThreadPoolExecutor executor(2);
        auto future = milvus::futures::Future<int>::async(
            &executor, 0, [&](folly::CancellationToken /*future_token*/) {
                folly::CancellationSource driver_cancel;
                driver_cancel.requestCancellation();
                query::ExecPlanNodeVisitor visitor(
                    *seg_promote, MAX_TIMESTAMP, driver_cancel.getToken());
                auto result = visitor.get_moved_result(*plan->plan_node_);
                return new int(0);  // unreachable: the driver throws above
            });

        std::mutex mu;
        mu.lock();
        future->registerReadyCallback(
            [](CLockedGoMutex* m) { ((std::mutex*)(m))->unlock(); },
            (CLockedGoMutex*)(&mu));
        mu.lock();
        auto [r, s] = future->leakyGet();
        EXPECT_EQ(r, nullptr);
        EXPECT_EQ(s.error_code,
                  static_cast<int>(milvus::ErrorCode::FollyCancel));
        free((char*)(s.error_msg));
    }  // Future (and its Metrics) destroyed here -> during-cancel counter fires

    EXPECT_DOUBLE_EQ(counter.Value(), before + 1);
}
