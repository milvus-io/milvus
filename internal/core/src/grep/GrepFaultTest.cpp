// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Compile the unchanged wrapper in this test translation unit with isolated
// C++ and C ABI names. This lets fault injection and the real Rust engine share
// all_tests without production hooks or replacement symbols. Do not add a second
// copy of Grep.cpp to this target; it is deliberately included below.
#define grep grep_fault_test
#define MgGrepPlan GrepFaultPlan
#define MgGrepSession GrepFaultSession
#define MgGrepResult GrepFaultResult
#define mg_grep_compile grep_fault_compile
#define mg_grep_plan_free grep_fault_plan_free
#define mg_grep_session_create grep_fault_session_create
#define mg_grep_session_free grep_fault_session_free
#define mg_grep_session_cancel grep_fault_session_cancel
#define mg_grep_execute_batch grep_fault_execute_batch
#define mg_grep_result_view grep_fault_result_view
#define mg_grep_result_free grep_fault_result_free

#include "grep/Grep.cpp"

#include <cstring>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

struct MgGrepPlan {};
struct MgGrepSession {};
struct MgGrepResult {
    MgGrepSpan span{0, 1};
    MgGrepLine line{{reinterpret_cast<const uint8_t*>("a"), 1}, &span, 1};
    std::vector<MgGrepTextResult> texts;
    MgGrepBatchView view{};
};

namespace {
struct Faults {
    MgGrepStatus compile = MG_GREP_OK;
    MgGrepStatus create = MG_GREP_OK;
    MgGrepStatus execute = MG_GREP_OK;
    bool partial_handle = false;
    bool null_plan = false;
    bool null_session = false;
    bool null_result = false;
    bool wrong_count = false;
    bool bad_span = false;
    int plans = 0, plans_freed = 0;
    int sessions = 0, sessions_freed = 0;
    int results = 0, results_freed = 0;
    int execute_calls = 0, cancel_calls = 0;
    std::function<void()> on_execute;
} faults;

void
Message(MgGrepError* error) {
    if (error) {
        std::strcpy(error->message, "same diagnostic for every status");
    }
}
}  // namespace

extern "C" {
MgGrepStatus
mg_grep_compile(const MgGrepSpecView*,
                const MgGrepCompileLimits*,
                MgGrepPlan** out,
                MgGrepError* error) {
    *out = nullptr;
    if (!faults.null_plan &&
        (faults.compile == MG_GREP_OK || faults.partial_handle)) {
        *out = new MgGrepPlan;
        ++faults.plans;
    }
    Message(error);
    return faults.compile;
}
void
mg_grep_plan_free(MgGrepPlan* plan) {
    if (plan) {
        ++faults.plans_freed;
        delete plan;
    }
}
MgGrepStatus
mg_grep_session_create(const MgGrepPlan*,
                       MgGrepSession** out,
                       MgGrepError* error) {
    *out = nullptr;
    if (!faults.null_session &&
        (faults.create == MG_GREP_OK || faults.partial_handle)) {
        *out = new MgGrepSession;
        ++faults.sessions;
    }
    Message(error);
    return faults.create;
}
void
mg_grep_session_free(MgGrepSession* session) {
    if (session) {
        ++faults.sessions_freed;
        delete session;
    }
}
void
mg_grep_session_cancel(MgGrepSession*) {
    ++faults.cancel_calls;
}
MgGrepStatus
mg_grep_execute_batch(MgGrepSession*,
                      const MgGrepBytes*,
                      size_t count,
                      uint64_t,
                      MgGrepResult** out,
                      MgGrepError* error) {
    ++faults.execute_calls;
    *out = nullptr;
    if (faults.on_execute) {
        faults.on_execute();
    }
    if (!faults.null_result &&
        (faults.execute == MG_GREP_OK || faults.partial_handle)) {
        auto* result = new MgGrepResult;
        result->texts.assign(count, {&result->line, 1});
        result->view = {result->texts.data(),
                        count + (faults.wrong_count ? 1 : 0)};
        if (faults.bad_span) {
            result->span.end = 2;
        }
        *out = result;
        ++faults.results;
    }
    Message(error);
    return faults.execute;
}
const MgGrepBatchView*
mg_grep_result_view(const MgGrepResult* result) {
    return result ? &result->view : nullptr;
}
void
mg_grep_result_free(MgGrepResult* result) {
    if (result) {
        ++faults.results_freed;
        delete result;
    }
}
}  // extern "C"

namespace milvus::grep {
namespace {
class GrepFault : public testing::Test {
 protected:
    void
    SetUp() override {
        faults = {};
    }
    void
    TearDown() override {
        EXPECT_EQ(faults.plans, faults.plans_freed);
        EXPECT_EQ(faults.sessions, faults.sessions_freed);
        EXPECT_EQ(faults.results, faults.results_freed);
    }
    Plan
    Compile() {
        return Plan::Compile({"a"}, {}, {1000000, 1000000});
    }
    uint64_t
    Budget() {
        return 10000;
    }
    template <typename Fn>
    void
    Expect(Fn fn, uint32_t code, Operation op) {
        try {
            fn();
            FAIL() << "expected error";
        } catch (const GrepError& e) {
            EXPECT_EQ(static_cast<uint32_t>(e.status()), code);
            EXPECT_EQ(e.operation(), op);
            auto status = FailureCStatus(&e);
            EXPECT_EQ(status.error_code, e.get_error_code());
            EXPECT_STREQ(status.error_msg, e.what());
            free(const_cast<char*>(status.error_msg));
        }
    }
};

TEST_F(GrepFault, EveryCompileStatusAndUnknownStatusArePreserved) {
    faults.partial_handle = true;
    for (auto code : {1u, 2u, 3u, 4u, 5u, 7u, 8u, 9u, 4242u}) {
        faults.compile = code;
        Expect([&] { Compile(); }, code, Operation::Compile);
        EXPECT_EQ(faults.plans, faults.plans_freed);
    }
}

TEST_F(GrepFault, SessionCreationFailureReleasesPartialHandle) {
    auto plan = Compile();
    faults.partial_handle = true;
    for (auto code : {1u, 7u, 9u, 4242u}) {
        faults.create = code;
        Expect([&] { plan.CreateSession(); }, code, Operation::CreateSession);
        EXPECT_EQ(faults.sessions, faults.sessions_freed);
    }
}

TEST_F(GrepFault, EveryExecutionStatusReleasesPartialResults) {
    faults.partial_handle = true;
    for (auto code : {1u, 2u, 3u, 4u, 5u, 7u, 8u, 9u, 4242u}) {
        auto session = Compile().CreateSession();
        faults.execute = code;
        Expect([&] { session.ExecuteBatch({"a"}, Budget()); },
               code,
               Operation::Execute);
        EXPECT_EQ(faults.results, faults.results_freed);
    }
}

TEST_F(GrepFault, SuccessfulNullHandlesAreInternalErrors) {
    faults.null_plan = true;
    Expect([&] { Compile(); }, MG_GREP_INTERNAL_ERROR, Operation::Compile);
    faults.null_plan = false;
    auto plan = Compile();
    faults.null_session = true;
    Expect([&] { plan.CreateSession(); },
           MG_GREP_INTERNAL_ERROR,
           Operation::CreateSession);
    faults.null_session = false;
    auto session = plan.CreateSession();
    faults.null_result = true;
    Expect([&] { session.ExecuteBatch({"a"}, Budget()); },
           MG_GREP_INTERNAL_ERROR,
           Operation::Execute);
}

TEST_F(GrepFault, WrongBatchShapeIsInternalAndDoesNotLeak) {
    auto session = Compile().CreateSession();
    faults.wrong_count = true;
    Expect([&] { session.ExecuteBatch({"a"}, Budget()); },
           MG_GREP_INTERNAL_ERROR,
           Operation::Execute);
    EXPECT_EQ(faults.results, faults.results_freed);
}

TEST_F(GrepFault, SpanOutsideContentIsInternalNotCallerInput) {
    auto session = Compile().CreateSession();
    faults.bad_span = true;
    auto result = session.ExecuteBatch({"a"}, Budget());
    Expect([&] { result.GetSpan(0, 0, 0); },
           MG_GREP_INTERNAL_ERROR,
           Operation::ReadResult);
}

TEST_F(GrepFault, CancellationAfterCCompletionPreservesTheResult) {
    auto session = Compile().CreateSession();
    auto token = session.GetCancellationToken();
    faults.on_execute = [&] { token.Cancel(); };
    {
        auto result = session.ExecuteBatch({"a"}, Budget());
        EXPECT_TRUE(result.Selected(0));
        EXPECT_EQ(faults.results_freed, 0);
    }
    EXPECT_EQ(faults.cancel_calls, 1);
    EXPECT_EQ(faults.results, 1);
    EXPECT_EQ(faults.results_freed, 1);
}

TEST_F(GrepFault, CallbackTokenKeepsTheSessionAlive) {
    CancellationToken token;
    {
        auto session = Compile().CreateSession();
        token = session.GetCancellationToken();
    }
    EXPECT_EQ(faults.sessions_freed, 0);
    token.Cancel();
    EXPECT_EQ(faults.cancel_calls, 1);
    token = {};
    EXPECT_EQ(faults.sessions_freed, 1);
}

TEST_F(GrepFault, AllocationAndInternalErrorsReleaseSessionDuringUnwind) {
    for (auto status : {MG_GREP_ALLOCATION_ERROR, MG_GREP_INTERNAL_ERROR}) {
        faults.execute = status;
        faults.partial_handle = true;
        Expect(
            [&] {
                auto session = Compile().CreateSession();
                auto token = session.GetCancellationToken();
                session.ExecuteBatch({"a"}, Budget());
            },
            status,
            Operation::Execute);
        EXPECT_EQ(faults.sessions, faults.sessions_freed);
        EXPECT_EQ(faults.results, faults.results_freed);
    }
}

TEST_F(GrepFault, MovingHandlesReleasesEachResourceOnce) {
    auto plan = Compile();
    auto second_plan = Compile();
    second_plan = std::move(plan);
    EXPECT_EQ(faults.plans_freed, 1);
    auto session = second_plan.CreateSession();
    auto second_session = second_plan.CreateSession();
    second_session = std::move(session);
    EXPECT_EQ(faults.sessions_freed, 1);
    auto result = second_session.ExecuteBatch({"a"}, Budget());
    auto second_result = second_session.ExecuteBatch({"a"}, Budget());
    second_result = std::move(result);
    EXPECT_EQ(faults.results_freed, 1);
}

TEST_F(GrepFault, ErrorOwnsBoundedUtf8Diagnostic) {
    std::string text;
    for (size_t i = 0; i < 300; ++i) {
        text += "中";
    }
    GrepError error(Status::InternalError, Operation::Execute, text);
    text.clear();
    EXPECT_EQ(std::strlen(error.what()), 510);
    auto copy = error;
    EXPECT_STREQ(copy.what(), error.what());
}
}  // namespace
}  // namespace milvus::grep

#undef mg_grep_result_free
#undef mg_grep_result_view
#undef mg_grep_execute_batch
#undef mg_grep_session_cancel
#undef mg_grep_session_free
#undef mg_grep_session_create
#undef mg_grep_plan_free
#undef mg_grep_compile
#undef MgGrepResult
#undef MgGrepSession
#undef MgGrepPlan
#undef grep
