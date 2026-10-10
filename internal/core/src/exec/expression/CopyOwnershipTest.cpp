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
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "exec/expression/Element.h"
#include "exec/expression/Expr.h"
#include "exec/expression/UnaryExpr.h"
#include "exec/expression/ValueExpr.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"

namespace milvus::exec {
namespace {

class LiteralRecordingReader
    : public index::IScalarPredicateReader<std::string_view>,
      public index::IPatternMatchReader {
 public:
    explicit LiteralRecordingReader(const std::string& literal)
        : literal_(literal) {
    }

    TargetBitmap
    In(size_t n, const std::string_view* values) const override {
        EXPECT_EQ(n, 1);
        return Record(*values);
    }

    TargetBitmap
    NotIn(size_t n, const std::string_view* values) const override {
        EXPECT_EQ(n, 1);
        return Record(*values);
    }

    TargetBitmap
    Range(const std::string_view& value, index::CompareOp) const override {
        return Record(value);
    }

    TargetBitmap
    Range(const std::string_view& lower,
          bool,
          const std::string_view& upper,
          bool) const override {
        Record(lower);
        return Record(upper);
    }

    TargetBitmap
    PatternMatch(std::string_view pattern, index::PatternOp) const override {
        return Record(pattern);
    }

 private:
    TargetBitmap
    Record(std::string_view value) const {
        EXPECT_EQ(value.data(), literal_.data());
        EXPECT_EQ(value.size(), literal_.size());
        EXPECT_EQ(value, literal_);
        return TargetBitmap(1, true);
    }

    const std::string& literal_;
};

template <typename T, proto::plan::OpType op>
void
ExpectOriginalIndexLiteral(const LiteralRecordingReader& reader,
                           const T& literal) {
    const auto result = [&] {
        if constexpr (op == proto::plan::PrefixMatch ||
                      op == proto::plan::Match ||
                      op == proto::plan::RegexMatch) {
            UnaryIndexFuncForMatch<T> function;
            return function(&reader, literal, op);
        } else {
            UnaryIndexFunc<std::string_view, op> function;
            return function(&reader, literal);
        }
    }();
    ASSERT_EQ(result.size(), 1);
    EXPECT_TRUE(result[0]);
}

template <typename T>
void
CheckIndexLiteralBorrowing() {
    const std::string literal(1024, 'x');
    LiteralRecordingReader reader(literal);
    const std::string_view view(literal);
    const auto& input = [&]() -> const T& {
        if constexpr (std::is_same_v<T, std::string>) {
            return literal;
        } else {
            return view;
        }
    }();
    ExpectOriginalIndexLiteral<T, proto::plan::Equal>(reader, input);
    ExpectOriginalIndexLiteral<T, proto::plan::NotEqual>(reader, input);
    ExpectOriginalIndexLiteral<T, proto::plan::GreaterThan>(reader, input);
    ExpectOriginalIndexLiteral<T, proto::plan::PrefixMatch>(reader, input);
    ExpectOriginalIndexLiteral<T, proto::plan::Match>(reader, input);
    ExpectOriginalIndexLiteral<T, proto::plan::RegexMatch>(reader, input);
}

}  // namespace

TEST(ExprOwnershipTest, IndexHelpersBorrowTheOriginalStringLiteral) {
    CheckIndexLiteralBorrowing<std::string>();
    CheckIndexLiteralBorrowing<std::string_view>();
}

TEST(ExprOwnershipTest, InputsAreTransferredToExpression) {
    auto child = std::make_shared<Expr>(
        DataType::BOOL, std::vector<ExprPtr>{}, "child", nullptr);
    std::vector<ExprPtr> inputs{child};
    const auto* buffer = inputs.data();
    Expr parent(DataType::BOOL, std::move(inputs), "parent", nullptr);
    EXPECT_EQ(parent.GetInputsRef().data(), buffer);
    EXPECT_EQ(child.use_count(), 2);
    inputs.clear();
    EXPECT_EQ(parent.GetInputsRef().at(0), child);
}

TEST(ExprOwnershipTest, ValueExpressionStillRejectsInputsAfterMove) {
    proto::plan::GenericValue value;
    value.set_int64_val(1);
    auto logical = std::make_shared<expr::ValueExpr>(value);
    std::vector<ExprPtr> inputs{std::make_shared<Expr>(
        DataType::BOOL, std::vector<ExprPtr>{}, "child", nullptr)};
    EXPECT_ANY_THROW(PhyValueExpr(
        std::move(inputs), logical, "value", nullptr, nullptr, 1, 1));
}

TEST(ExprOwnershipTest, CachedLiteralIsBorrowedAndOwnsItsPayload) {
    proto::plan::GenericValue value;
    value.mutable_array_val()->add_array()->set_string_val(
        std::string(1024, 'x'));
    SingleElement cache;
    cache.SetValue<proto::plan::Array>(value);
    const auto& first = cache.GetValue<proto::plan::Array>();
    const auto& second = cache.GetValue<proto::plan::Array>();
    EXPECT_EQ(&first, &second);
    value.Clear();
    ASSERT_EQ(first.array_size(), 1);
    EXPECT_EQ(first.array(0).string_val(), std::string(1024, 'x'));
}

}  // namespace milvus::exec
