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

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/Types.h"
#include "common/Vector.h"
#include "exec/QueryContext.h"
#include "exec/expression/ConjunctExpr.h"
#include "exec/expression/EvalCtx.h"

namespace milvus::exec {
namespace {

class FixedBitmapExpr : public Expr {
 public:
    FixedBitmapExpr(TargetBitmap data, TargetBitmap valid)
        : Expr(DataType::BOOL, {}, "FixedBitmapExpr", nullptr),
          data_(std::move(data)),
          valid_(std::move(valid)) {
    }

    void
    Eval(EvalCtx&, VectorPtr& result) override {
        ++eval_count_;
        result = std::make_shared<ColumnVector>(data_.clone(), valid_.clone());
    }

    void
    MoveCursor() override {
        ++move_count_;
    }

    std::string
    ToString() const override {
        return "FixedBitmapExpr";
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return std::nullopt;
    }

    int eval_count_ = 0;
    int move_count_ = 0;

 private:
    TargetBitmap data_;
    TargetBitmap valid_;
};

// Each row given as {data, valid}.
std::shared_ptr<FixedBitmapExpr>
FixedRows(std::initializer_list<std::pair<bool, bool>> rows) {
    TargetBitmap data_bitmap(rows.size(), false);
    TargetBitmap valid_bitmap(rows.size(), false);
    size_t i = 0;
    for (const auto& [data, valid] : rows) {
        data_bitmap[i] = data;
        valid_bitmap[i] = valid;
        ++i;
    }
    return std::make_shared<FixedBitmapExpr>(std::move(data_bitmap),
                                             std::move(valid_bitmap));
}

std::shared_ptr<FixedBitmapExpr>
FixedBool(const bool data, const bool valid) {
    return FixedRows({{data, valid}});
}

// A boolean node that is not a conjunction (stand-in for NOT in the
// null-rejection propagation tests).
class PassThroughExpr : public Expr {
 public:
    explicit PassThroughExpr(ExprPtr input)
        : Expr(DataType::BOOL, {std::move(input)}, "PassThroughExpr", nullptr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override {
        inputs_[0]->Eval(context, result);
    }

    std::string
    ToString() const override {
        return "PassThroughExpr";
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return std::nullopt;
    }
};

}  // namespace

TEST(ConjunctExprTest, AndKeepsUnknownRowsActiveForFollowingFalse) {
    auto unknown = FixedBool(false, false);
    auto true_expr = FixedBool(true, true);
    auto false_expr = FixedBool(false, true);

    std::vector<ExprPtr> inputs{unknown, true_expr, false_expr};
    PhyConjunctFilterExpr conjunct(std::move(inputs), true, nullptr);

    QueryContext query_context("conjunct_test", nullptr, 1, 0);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    VectorPtr result;
    conjunct.Eval(eval_context, result);

    auto output = std::dynamic_pointer_cast<ColumnVector>(result);
    ASSERT_NE(output, nullptr);
    ASSERT_TRUE(output->IsBitmap());

    TargetBitmapView data(output->GetRawData(), output->size());
    TargetBitmapView valid(output->GetValidRawData(), output->size());
    ASSERT_EQ(output->size(), 1);
    EXPECT_FALSE(data[0]);
    EXPECT_TRUE(valid[0]);

    EXPECT_EQ(unknown->eval_count_, 1);
    EXPECT_EQ(true_expr->eval_count_, 1);
    EXPECT_EQ(false_expr->eval_count_, 1);
    EXPECT_EQ(false_expr->move_count_, 0);
}

TEST(ConjunctExprTest, NullRejectingAndSkipsFollowingForAllUnknown) {
    auto unknown = FixedBool(false, false);
    auto heavy = FixedBool(true, true);

    std::vector<ExprPtr> inputs{unknown, heavy};
    auto conjunct = std::make_shared<PhyConjunctFilterExpr>(
        std::move(inputs), true, nullptr);
    conjunct->MarkNullRejecting();

    QueryContext query_context("conjunct_test", nullptr, 1, 0);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    VectorPtr result;
    conjunct->Eval(eval_context, result);

    auto output = std::dynamic_pointer_cast<ColumnVector>(result);
    ASSERT_NE(output, nullptr);
    TargetBitmapView data(output->GetRawData(), output->size());
    TargetBitmapView valid(output->GetValidRawData(), output->size());
    ASSERT_EQ(output->size(), 1);
    // The row stays UNKNOWN, which the null-rejecting consumer treats as
    // FALSE.
    EXPECT_FALSE(data[0]);
    EXPECT_FALSE(valid[0]);

    // The UNKNOWN row can never become TRUE under AND, so the following
    // expression must be skipped, not evaluated.
    EXPECT_EQ(unknown->eval_count_, 1);
    EXPECT_EQ(heavy->eval_count_, 0);
    EXPECT_EQ(heavy->move_count_, 1);
}

TEST(ConjunctExprTest, NullRejectingAndStillEvaluatesForTrueRows) {
    auto first = FixedRows({{false, false}, {true, true}});
    auto second = FixedRows({{true, true}, {false, true}});

    std::vector<ExprPtr> inputs{first, second};
    auto conjunct = std::make_shared<PhyConjunctFilterExpr>(
        std::move(inputs), true, nullptr);
    conjunct->MarkNullRejecting();

    QueryContext query_context("conjunct_test", nullptr, 2, 0);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    VectorPtr result;
    conjunct->Eval(eval_context, result);

    // Row 1 is TRUE after the first expression, so the second must run.
    EXPECT_EQ(second->eval_count_, 1);
    EXPECT_EQ(second->move_count_, 0);

    auto output = std::dynamic_pointer_cast<ColumnVector>(result);
    ASSERT_NE(output, nullptr);
    TargetBitmapView data(output->GetRawData(), output->size());
    TargetBitmapView valid(output->GetValidRawData(), output->size());
    ASSERT_EQ(output->size(), 2);
    // Row 1: TRUE AND FALSE = FALSE.
    EXPECT_FALSE(data[1]);
    EXPECT_TRUE(valid[1]);
    // Row 0 is excluded either way (UNKNOWN or FALSE).
    EXPECT_FALSE(data[0] && valid[0]);
}

TEST(ConjunctExprTest, NullRejectingMatchesDefaultIncludedSet) {
    // The included set (definite TRUE rows) must be identical with and
    // without null-rejection; only excluded rows may differ between
    // UNKNOWN and FALSE.
    auto build_and_eval = [](bool null_rejecting) {
        auto first = FixedRows({{false, false}, {true, true}, {false, true}});
        auto second = FixedRows({{false, true}, {true, true}, {true, true}});
        std::vector<ExprPtr> inputs{first, second};
        auto conjunct = std::make_shared<PhyConjunctFilterExpr>(
            std::move(inputs), true, nullptr);
        if (null_rejecting) {
            conjunct->MarkNullRejecting();
        }
        QueryContext query_context("conjunct_test", nullptr, 3, 0);
        ExecContext exec_context(&query_context);
        EvalCtx eval_context(&exec_context);
        VectorPtr result;
        conjunct->Eval(eval_context, result);
        auto output = std::dynamic_pointer_cast<ColumnVector>(result);
        TargetBitmapView data(output->GetRawData(), output->size());
        TargetBitmapView valid(output->GetValidRawData(), output->size());
        std::vector<bool> included;
        for (size_t i = 0; i < output->size(); ++i) {
            included.push_back(data[i] && valid[i]);
        }
        return included;
    };

    EXPECT_EQ(build_and_eval(true), build_and_eval(false));
}

TEST(ConjunctExprTest, NullRejectingOrKeepsUnknownRowsActive) {
    // Under OR an UNKNOWN row can still become TRUE (UNKNOWN OR TRUE =
    // TRUE), so null-rejection must not skip the following expression.
    auto unknown = FixedBool(false, false);
    auto true_expr = FixedBool(true, true);

    std::vector<ExprPtr> inputs{unknown, true_expr};
    auto disjunct = std::make_shared<PhyConjunctFilterExpr>(
        std::move(inputs), false, nullptr);
    disjunct->MarkNullRejecting();

    QueryContext query_context("conjunct_test", nullptr, 1, 0);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    VectorPtr result;
    disjunct->Eval(eval_context, result);

    EXPECT_EQ(true_expr->eval_count_, 1);

    auto output = std::dynamic_pointer_cast<ColumnVector>(result);
    ASSERT_NE(output, nullptr);
    TargetBitmapView data(output->GetRawData(), output->size());
    TargetBitmapView valid(output->GetValidRawData(), output->size());
    // UNKNOWN OR TRUE = TRUE: the row must be included.
    EXPECT_TRUE(data[0]);
    EXPECT_TRUE(valid[0]);
}

TEST(ConjunctExprTest, OffsetInputErasesUnmaterializedReservedLikeSlot) {
    // ReorderConjunctExpr reserves index inputs_.size() for a runtime
    // PhyLikeConjunctExpr and records the LIKE positions via SetLikeIndices.
    // The batch-ngram path cannot be used with an offset input, so the init
    // block must erase the reserved slot on that path too — otherwise both
    // the Eval loop and the early-exit path (SkipFollowingExprs) would
    // index inputs_ past its end.
    auto first = FixedBool(false, true);  // definite FALSE -> early exit
    auto second = FixedBool(true, true);

    std::vector<ExprPtr> inputs{first, second};
    auto conjunct = std::make_shared<PhyConjunctFilterExpr>(
        std::move(inputs), true, nullptr);
    conjunct->SetLikeIndices({0, 1});
    conjunct->Reorder({0, 1, 2});

    QueryContext query_context("conjunct_test", nullptr, 1, 0);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);
    OffsetVector offsets;
    offsets.push_back(0);
    eval_context.set_offset_input(&offsets);

    VectorPtr result;
    conjunct->Eval(eval_context, result);

    auto output = std::dynamic_pointer_cast<ColumnVector>(result);
    ASSERT_NE(output, nullptr);
    TargetBitmapView data(output->GetRawData(), output->size());
    TargetBitmapView valid(output->GetValidRawData(), output->size());
    ASSERT_EQ(output->size(), 1);
    EXPECT_FALSE(data[0]);
    EXPECT_TRUE(valid[0]);
    // The definite-FALSE first input early-exits: the second expression is
    // skipped via MoveCursor and the erased reserved slot is never touched.
    EXPECT_EQ(first->eval_count_, 1);
    EXPECT_EQ(second->eval_count_, 0);
    EXPECT_EQ(second->move_count_, 1);
}

TEST(ConjunctExprTest, MarkNullRejectingStopsAtNonConjunctNodes) {
    std::vector<ExprPtr> inner_inputs{FixedBool(true, true),
                                      FixedBool(true, true)};
    auto inner_and = std::make_shared<PhyConjunctFilterExpr>(
        std::move(inner_inputs), true, nullptr);

    std::vector<ExprPtr> hidden_inputs{FixedBool(true, true),
                                       FixedBool(true, true)};
    auto hidden_and = std::make_shared<PhyConjunctFilterExpr>(
        std::move(hidden_inputs), true, nullptr);
    auto wrapper = std::make_shared<PassThroughExpr>(hidden_and);

    std::vector<ExprPtr> outer_inputs{inner_and, wrapper};
    auto outer_or = std::make_shared<PhyConjunctFilterExpr>(
        std::move(outer_inputs), false, nullptr);

    outer_or->MarkNullRejecting();

    // Propagates through nested AND/OR ...
    EXPECT_TRUE(outer_or->IsNullRejecting());
    EXPECT_TRUE(inner_and->IsNullRejecting());
    // ... but stops at any non-conjunct node (e.g. NOT), where FALSE and
    // UNKNOWN produce different results.
    EXPECT_FALSE(hidden_and->IsNullRejecting());
}

}  // namespace milvus::exec
