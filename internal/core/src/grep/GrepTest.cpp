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
#include "grep/Grep.h"

#include <future>
#include <string>
#include <type_traits>
#include <utility>

#include <gtest/gtest.h>

namespace milvus::grep {
namespace {
CompileLimits
CompileBudget() {
    return {1024 * 1024, 1024 * 1024};
}
uint64_t
Budget() {
    return 200000000;
}
Plan
Compile(std::string_view pattern, MatchOptions options = {}) {
    return Plan::Compile({pattern}, options, CompileBudget());
}
template <typename Fn>
void
ExpectError(Fn&& fn, Status status, Operation operation) {
    try {
        fn();
        FAIL() << "expected a Grep error";
    } catch (const GrepError& error) {
        EXPECT_EQ(error.status(), status);
        EXPECT_EQ(error.operation(), operation);
        EXPECT_NE(error.what()[0], '\0');
    }
}

TEST(Grep, GenericCgoBoundaryPreservesErrorCodes) {
    static_assert(std::is_base_of_v<SegcoreError, GrepError>);
    struct Case {
        Status status;
        Operation operation;
        ErrorCode code;
    };
    const Case cases[] = {
        {Status::InvalidPattern, Operation::Compile, InvalidParameter},
        {Status::InvalidUtf8, Operation::Compile, InvalidParameter},
        {Status::InvalidUtf8, Operation::Execute, DataFormatBroken},
        {Status::ResourceLimit, Operation::Execute, InvalidParameter},
        {Status::ResourceLimit, Operation::Compile, InvalidParameter},
        {Status::Cancelled, Operation::Execute, FollyCancel},
        {Status::AllocationError, Operation::CreateSession, MemAllocateFailed},
        {Status::InvalidArgument, Operation::Compile, UnexpectedError},
        {Status::InvalidArgument, Operation::ReadResult, UnexpectedError},
        {Status::InternalError, Operation::Execute, UnexpectedError},
        {Status::EngineBuildError, Operation::Compile, InvalidParameter},
        {static_cast<Status>(4242), Operation::Execute, UnexpectedError},
    };
    for (const auto& test : cases) {
        try {
            throw GrepError(test.status, test.operation, "original diagnostic");
        } catch (const std::exception& error) {
            // The generic handler must work without a Grep-specific catch.
            auto status = FailureCStatus(&error);
            EXPECT_EQ(status.error_code, test.code);
            EXPECT_STREQ(status.error_msg, "original diagnostic");
            free(const_cast<char*>(status.error_msg));
        }
    }
}

TEST(Grep, BatchResultsAndByteSpans) {
    auto session = Compile("error").CreateSession();
    auto result = session.ExecuteBatch(
        {"ok", "error: error\r\nnext", "错误：error", ""}, Budget());
    EXPECT_FALSE(result.Selected(0));
    ASSERT_EQ(result.LineCount(1), 1);
    EXPECT_EQ(result.Content(1, 0), "error: error\r");
    ASSERT_EQ(result.SpanCount(1, 0), 2);
    EXPECT_EQ(result.GetSpan(1, 0, 0).start, 0);
    EXPECT_EQ(result.GetSpan(1, 0, 0).end, 5);
    EXPECT_EQ(result.GetSpan(1, 0, 1).start, 7);
    EXPECT_EQ(result.GetSpan(1, 0, 1).end, 12);
    EXPECT_EQ(result.GetSpan(2, 0, 0).start, 9);
    EXPECT_EQ(result.GetSpan(2, 0, 0).end, 14);
    EXPECT_FALSE(result.Selected(3));
    ExpectError([&] { result.Selected(4); },
                Status::InvalidArgument,
                Operation::ReadResult);
}

TEST(Grep, ResultAndViewsOutliveInputSessionAndPlan) {
    auto result = [] {
        std::string pattern = "error";
        auto plan = Compile(pattern);
        pattern.assign("different");
        auto session = plan.CreateSession();
        std::string input = "error";
        return session.ExecuteBatch({input}, Budget());
    }();
    auto content = result.Content(0, 0);
    auto moved = std::move(result);
    EXPECT_EQ(content, "error");
    EXPECT_EQ(moved.Content(0, 0), "error");
    ExpectError([&] { result.Selected(0); },
                Status::InvalidArgument,
                Operation::ReadResult);
}

TEST(Grep, EmptyLinesNulAndBomArePreserved) {
    auto session = Compile("").CreateSession();
    std::string binary("a\0b", 3);
    auto result =
        session.ExecuteBatch({"", "\n", "é", binary, "\xef\xbb\xbf"}, Budget());
    EXPECT_FALSE(result.Selected(0));
    ASSERT_EQ(result.LineCount(1), 1);
    EXPECT_TRUE(result.Content(1, 0).empty());
    EXPECT_EQ(result.GetSpan(1, 0, 0).end, 0);
    ASSERT_EQ(result.SpanCount(2, 0), 2);
    EXPECT_EQ(result.GetSpan(2, 0, 1).start, 2);
    EXPECT_EQ(result.Content(3, 0), binary);
    EXPECT_EQ(result.Content(4, 0).size(), 3);
    auto empty = session.ExecuteBatch({}, Budget());
    ExpectError([&] { empty.Selected(0); },
                Status::InvalidArgument,
                Operation::ReadResult);
}

TEST(Grep, TypedOptionsAndMultiplePatterns) {
    MatchOptions literal{
        PatternKind::Literal, CaseMode::Insensitive, Boundary::None};
    auto plan = Plan::Compile({"a.b", "timeout"}, literal, CompileBudget());
    auto session = plan.CreateSession();
    auto result = session.ExecuteBatch({"A.B axb TIMEOUT"}, Budget());
    ASSERT_EQ(result.SpanCount(0, 0), 2);
    EXPECT_EQ(result.GetSpan(0, 0, 1).start, 8);
    auto word =
        Compile("-2", {PatternKind::Regex, CaseMode::Sensitive, Boundary::Word})
            .CreateSession();
    EXPECT_EQ(word.ExecuteBatch({"x-2 -2 -2x"}, Budget()).SpanCount(0, 0), 1);
    auto line =
        Compile("error",
                {PatternKind::Regex, CaseMode::Sensitive, Boundary::Line})
            .CreateSession();
    EXPECT_EQ(
        line.ExecuteBatch({"error\nxerror\nerror"}, Budget()).LineCount(0), 2);
}

TEST(Grep, CompileErrorsPreserveStatusAndPhase) {
    ExpectError(
        [] { Compile("["); }, Status::InvalidPattern, Operation::Compile);
    ExpectError(
        [] { Compile("(?-u:a)"); }, Status::InvalidPattern, Operation::Compile);
    ExpectError([] { Compile(std::string(1, static_cast<char>(0xff))); },
                Status::InvalidUtf8,
                Operation::Compile);
    ExpectError(
        [] {
            auto limits = CompileBudget();
            limits.max_regex_bytes = 1;
            Plan::Compile({"[a-z]+"}, {}, limits);
        },
        Status::EngineBuildError,
        Operation::Compile);
}

TEST(Grep, InvalidConfigurationIsNotARegexError) {
    ExpectError([] { Plan::Compile({}, {}, CompileBudget()); },
                Status::InvalidArgument,
                Operation::Compile);
    ExpectError([] { Plan::Compile({"a"}, {}, {}); },
                Status::InvalidArgument,
                Operation::Compile);
    ExpectError(
        [] {
            Compile("a",
                    {static_cast<PatternKind>(99),
                     CaseMode::Sensitive,
                     Boundary::None});
        },
        Status::InvalidArgument,
        Operation::Compile);
    auto session = Compile("a").CreateSession();
    ExpectError([&] { session.ExecuteBatch({}, 0); },
                Status::InvalidArgument,
                Operation::Execute);
}

TEST(Grep, LargeInputAndRequestCancellation) {
    auto session = Compile("needle").CreateSession();
    const uint64_t limits{1 << 20};
    const std::string large(9 << 20, 'x');
    auto result = session.ExecuteBatch({large}, limits);
    EXPECT_FALSE(result.Selected(0));
    ExpectError([&] { result.Selected(1); },
                Status::InvalidArgument,
                Operation::ReadResult);
    session.GetCancellationToken().Cancel();
    ExpectError([&] { session.ExecuteBatch({large}, limits); },
                Status::Cancelled,
                Operation::Execute);
}

TEST(Grep, LimitsAndInvalidTextDoNotPoisonTheSession) {
    auto session = Compile("a").CreateSession();
    std::string invalid(1, static_cast<char>(0xff));
    ExpectError(
        [&] {
            session.ExecuteBatch({"a", invalid}, Budget());
        },
        Status::InvalidUtf8,
        Operation::Execute);
    auto limits = Budget();
    limits = 1;
    ExpectError([&] { session.ExecuteBatch({""}, limits); },
                Status::ResourceLimit,
                Operation::Execute);
    // Insufficient for Rust's complete result and view storage.
    limits = 128;
    ExpectError([&] { session.ExecuteBatch({"aa"}, limits); },
                Status::ResourceLimit,
                Operation::Execute);
    EXPECT_TRUE(session.ExecuteBatch({"a"}, Budget()).Selected(0));
}

TEST(Grep, ManyPatternsHaveNoSeparateCountLimit) {
    std::vector<std::string_view> patterns(600, "needle");
    auto session = Plan::Compile(patterns, {}, CompileBudget()).CreateSession();
    EXPECT_TRUE(session.ExecuteBatch({"needle"}, Budget()).Selected(0));
}

TEST(Grep, CheckedResultAccess) {
    auto session = Compile("a").CreateSession();
    auto result = session.ExecuteBatch({"a", "b"}, Budget());
    ExpectError([&] { result.LineCount(2); },
                Status::InvalidArgument,
                Operation::ReadResult);
    ExpectError([&] { result.Content(1, 0); },
                Status::InvalidArgument,
                Operation::ReadResult);
    ExpectError([&] { result.GetSpan(0, 0, 1); },
                Status::InvalidArgument,
                Operation::ReadResult);
}

TEST(Grep, MoveOnlyHandlesAndMoveAssignment) {
    static_assert(!std::is_copy_constructible_v<Plan>);
    static_assert(!std::is_copy_constructible_v<Session>);
    static_assert(!std::is_copy_constructible_v<Result>);
    static_assert(std::is_nothrow_move_constructible_v<Plan>);
    static_assert(std::is_nothrow_move_assignable_v<Session>);
    static_assert(std::is_copy_constructible_v<CancellationToken>);
    auto plan = Compile("a");
    auto other_plan = Compile("b");
    other_plan = std::move(plan);
    ExpectError([&] { plan.CreateSession(); },
                Status::InvalidArgument,
                Operation::CreateSession);
    auto session = other_plan.CreateSession();
    auto other = Compile("b").CreateSession();
    other = std::move(session);
    ExpectError([&] { session.ExecuteBatch({}, Budget()); },
                Status::InvalidArgument,
                Operation::Execute);
    auto result = other.ExecuteBatch({"a"}, Budget());
    auto replacement = other.ExecuteBatch({}, Budget());
    replacement = std::move(result);
    EXPECT_TRUE(replacement.Selected(0));
}

TEST(Grep, CancellationTokenPinsSessionAndIsSticky) {
    CancellationToken token;
    token.Cancel();
    {
        auto session = Compile("a").CreateSession();
        token = session.GetCancellationToken();
        auto result = session.ExecuteBatch({"a"}, Budget());
        token.Cancel();
        token.Cancel();
        ExpectError([&] { session.ExecuteBatch({}, Budget()); },
                    Status::Cancelled,
                    Operation::Execute);
        EXPECT_TRUE(result.Selected(0));
    }
    // The callback can run after the owner's lifetime without a dangling C handle.
    auto copy = token;
    token = {};
    copy.Cancel();
}

TEST(Grep, ConcurrentCancellation) {
    auto session = Compile("a").CreateSession();
    auto token = session.GetCancellationToken();
    std::promise<void> ready;
    auto started = ready.get_future();
    auto task = std::async(std::launch::async, [&] {
        std::string text(2000000, 'a');
        ready.set_value();
        ExpectError([&] { session.ExecuteBatch({text}, Budget()); },
                    Status::Cancelled,
                    Operation::Execute);
    });
    started.wait();
    token.Cancel();
    task.get();
}

TEST(Grep, SharedPlanIndependentSessions) {
    auto plan = Compile("error");
    std::vector<std::future<void>> tasks;
    for (int i = 0; i < 4; ++i) {
        tasks.push_back(std::async(std::launch::async, [&] {
            auto session = plan.CreateSession();
            EXPECT_EQ(
                session.ExecuteBatch({"error error"}, Budget()).SpanCount(0, 0),
                2);
        }));
    }
    for (auto& task : tasks) {
        task.get();
    }
}

}  // namespace
}  // namespace milvus::grep
