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

#include <limits>

#include "exec/expression/SequenceMatchCore.h"

namespace milvus::exec::sequence {
namespace {

constexpr int64_t kTime = 1;
constexpr int64_t kTie = 2;
constexpr int64_t kKind = 3;
constexpr int64_t kActor = 4;
constexpr int64_t kTarget = 5;

proto::plan::Expr
KindIs(const char* kind) {
    proto::plan::Expr expr;
    auto* unary = expr.mutable_unary_range_expr();
    unary->mutable_column_info()->set_field_id(kKind);
    unary->set_op(proto::plan::OpType::Equal);
    unary->mutable_value()->set_string_val(kind);
    return expr;
}

proto::plan::Expr
BindActorToPriorTarget(uint32_t step) {
    proto::plan::Expr expr;
    auto* cmp = expr.mutable_compare_expr();
    cmp->mutable_left_column_info()->set_field_id(kActor);
    cmp->mutable_right_column_info()->set_field_id(kTarget);
    cmp->mutable_right_column_info()->set_sequence_step_index(step);
    cmp->set_op(proto::plan::OpType::Equal);
    return expr;
}

proto::plan::Expr
And(proto::plan::Expr lhs, proto::plan::Expr rhs) {
    proto::plan::Expr result;
    auto* binary = result.mutable_binary_expr();
    binary->set_op(proto::plan::BinaryExpr::LogicalAnd);
    *binary->mutable_left() = std::move(lhs);
    *binary->mutable_right() = std::move(rhs);
    return result;
}

proto::plan::SequenceMatchExpr
TwoStep(int64_t maximum_gap = 1000) {
    proto::plan::SequenceMatchExpr spec;
    spec.set_struct_name("events");
    spec.set_order_time_field_id(kTime);
    spec.set_tie_field_id(kTie);
    *spec.add_steps()->mutable_predicate() = KindIs("conflict");
    auto* turn = spec.add_steps();
    *turn->mutable_predicate() = And(KindIs("turn"), BindActorToPriorTarget(0));
    auto* window = turn->mutable_window();
    window->set_current_field_id(kTime);
    window->set_prior_step_index(0);
    window->set_prior_field_id(kTime);
    window->set_min_ms(0);
    window->set_max_ms(maximum_gap);
    return spec;
}

Row
Events(std::vector<int64_t> times,
       std::vector<std::string> kinds,
       std::vector<std::string> actors,
       std::vector<std::string> targets) {
    Row row;
    for (size_t i = 0; i < times.size(); ++i) {
        row[kTime].push_back(Value(times[i]));
        row[kTie].push_back(Value(static_cast<int64_t>(i)));
        row[kKind].push_back(Value(kinds[i]));
        row[kActor].push_back(Value(actors[i]));
        row[kTarget].push_back(Value(targets[i]));
    }
    return row;
}

TEST(SequenceMatchCoreTest, TriesLaterStartWhenFirstFails) {
    auto row = Events({100, 200, 3000, 3500},
                      {"conflict", "turn", "conflict", "turn"},
                      {"cat-a", "car-b", "cat-c", "car-c"},
                      {"car-a", "", "car-c", ""});
    EXPECT_TRUE(Match(TwoStep(), row, 4));
}

TEST(SequenceMatchCoreTest, BindsTheSameVehicle) {
    auto row = Events(
        {100, 200}, {"conflict", "turn"}, {"cat-a", "car-b"}, {"car-a", ""});
    EXPECT_FALSE(Match(TwoStep(), row, 2));
    row[kActor][1] = Value(std::string("car-a"));
    EXPECT_TRUE(Match(TwoStep(), row, 2));
}

TEST(SequenceMatchCoreTest, WindowBoundariesAreInclusive) {
    auto row = Events(
        {100, 1100}, {"conflict", "turn"}, {"cat-a", "car-a"}, {"car-a", ""});
    EXPECT_TRUE(Match(TwoStep(), row, 2));
    row[kTime][1] = Value(int64_t{1101});
    EXPECT_FALSE(Match(TwoStep(), row, 2));
}

TEST(SequenceMatchCoreTest, SkipsMissingOrderKeysAndUsesTieKey) {
    auto row = Events(
        {100, 100}, {"turn", "conflict"}, {"car-a", "cat-a"}, {"", "car-a"});
    EXPECT_FALSE(Match(TwoStep(), row, 2));
    row[kTie][0] = Value(int64_t{2});
    row[kTie][1] = Value(int64_t{1});
    EXPECT_TRUE(Match(TwoStep(), row, 2));
    row[kTime][1] = std::nullopt;
    EXPECT_FALSE(Match(TwoStep(), row, 2));
}

TEST(SequenceMatchCoreTest, WideTimeDeltaDoesNotOverflow) {
    auto row = Events({INT64_MIN, INT64_MAX},
                      {"conflict", "turn"},
                      {"cat-a", "car-a"},
                      {"car-a", ""});
    EXPECT_FALSE(Match(TwoStep(), row, 2));
}

TEST(SequenceMatchCoreTest, CandidateBudgetFailsRatherThanMissingMatches) {
    auto row = Events({100, 200, 300, 400},
                      {"conflict", "conflict", "conflict", "conflict"},
                      {"cat-a", "cat-a", "cat-a", "cat-a"},
                      {"car-a", "car-a", "car-a", "car-a"});
    EXPECT_ANY_THROW(Match(TwoStep(), row, 4, 2));
}

TEST(SequenceMatchCoreTest, NaNDoesNotEqualAConstant) {
    const Cell nan = Value(std::numeric_limits<double>::quiet_NaN());
    const Cell one = Value(1.0);
    EXPECT_FALSE(ApplyOp(proto::plan::OpType::Equal, nan, one));
    EXPECT_FALSE(ApplyOp(proto::plan::OpType::LessThan, nan, one));
    EXPECT_TRUE(ApplyOp(proto::plan::OpType::NotEqual, nan, one));
}


}  // namespace
}  // namespace milvus::exec::sequence
