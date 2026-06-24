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

std::shared_ptr<FixedBitmapExpr>
FixedBool(const bool data, const bool valid) {
    TargetBitmap data_bitmap(1, data);
    TargetBitmap valid_bitmap(1, valid);
    return std::make_shared<FixedBitmapExpr>(std::move(data_bitmap),
                                             std::move(valid_bitmap));
}

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

}  // namespace milvus::exec
