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

#include <algorithm>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/Schema.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/QueryContext.h"
#include "exec/expression/ConjunctExpr.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/LogicalUnaryExpr.h"
#include "exec/expression/NullExpr.h"
#include "segcore/ChunkedSegmentSealedImpl.h"

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

    void
    SetFirstRow(bool value) {
        data_[0] = value;
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

enum class Truth { False, Unknown, True };

// Unlike FixedBitmapExpr, this leaf consumes the input mask and advances a
// sequential cursor, so assertions can observe work done on excluded rows.
class MaskAwareExpr : public Expr {
 public:
    MaskAwareExpr(std::vector<Truth> rows, size_t batch_size)
        : Expr(DataType::BOOL, {}, "MaskAwareExpr", nullptr),
          rows_(std::move(rows)),
          batch_size_(batch_size) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override {
        ++eval_count_;
        const auto* offsets = context.get_offset_input();
        const size_t size = offsets != nullptr
                                ? offsets->size()
                                : std::min(batch_size_, rows_.size() - cursor_);
        const auto& mask = context.get_bitmap_input();
        ASSERT_TRUE(mask.empty() || mask.size() == size);
        TargetBitmap data(size, false);
        TargetBitmap valid(size, true);
        for (size_t i = 0; i < size; ++i) {
            if (!mask.empty() && !mask[i]) {
                continue;
            }
            const size_t row = offsets != nullptr ? (*offsets)[i] : cursor_ + i;
            ASSERT_LT(row, rows_.size());
            evaluated_rows_.push_back(row);
            data[i] = rows_[row] == Truth::True;
            valid[i] = rows_[row] != Truth::Unknown;
        }
        if (throw_on_eval_) {
            throw std::runtime_error("injected expression failure");
        }
        if (offsets == nullptr) {
            cursor_ += size;
        }
        result =
            std::make_shared<ColumnVector>(std::move(data), std::move(valid));
    }

    void
    MoveCursor() override {
        ++move_count_;
        if (!has_offset_input_) {
            cursor_ += std::min(batch_size_, rows_.size() - cursor_);
        }
    }

    std::string
    ToString() const override {
        return "MaskAwareExpr";
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return std::nullopt;
    }

    std::vector<size_t> evaluated_rows_;
    size_t cursor_ = 0;
    int eval_count_ = 0;
    int move_count_ = 0;
    bool throw_on_eval_ = false;

 private:
    const std::vector<Truth> rows_;
    const size_t batch_size_;
};

void
ExpectMask(const EvalCtx& context, const TargetBitmap& expected) {
    const auto& actual = context.get_bitmap_input();
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i], expected[i]) << "candidate " << i;
    }
}

void
ExpectTruth(const ColumnVectorPtr& output, size_t row, Truth expected) {
    ASSERT_NE(output, nullptr);
    ASSERT_LT(row, output->size());
    TargetBitmapView data(output->GetRawData(), output->size());
    TargetBitmapView valid(output->GetValidRawData(), output->size());
    EXPECT_EQ(valid[row], expected != Truth::Unknown) << "row " << row;
    if (expected != Truth::Unknown) {
        EXPECT_EQ(data[row], expected == Truth::True) << "row " << row;
    }
}

}  // namespace

TEST(ConjunctExprTest, NestedMasksPreserveCandidatesAndThreeValuedLogic) {
    // Enumerate the complete truth table for A AND (B OR C), both directly
    // and below a real NOT, with and without a null-rejecting consumer.
    constexpr size_t size = 27;
    std::vector<Truth> a(size), b(size), c(size);
    for (size_t i = 0; i < size; ++i) {
        a[i] = static_cast<Truth>(i / 9);
        b[i] = static_cast<Truth>((i / 3) % 3);
        c[i] = static_cast<Truth>(i % 3);
    }
    for (const bool inherited_mask : {false, true}) {
        for (const bool negate : {false, true}) {
            for (const bool null_rejecting : {false, true}) {
                SCOPED_TRACE(::testing::Message()
                             << "inherited_mask=" << inherited_mask
                             << " negate=" << negate
                             << " null_rejecting=" << null_rejecting);
                auto first = std::make_shared<MaskAwareExpr>(a, size);
                auto second = std::make_shared<MaskAwareExpr>(b, size);
                auto third = std::make_shared<MaskAwareExpr>(c, size);
                auto inner = std::make_shared<PhyConjunctFilterExpr>(
                    std::vector<ExprPtr>{second, third}, false, nullptr);
                auto outer = std::make_shared<PhyConjunctFilterExpr>(
                    std::vector<ExprPtr>{first, inner}, true, nullptr);
                ExprPtr root = outer;
                if (negate) {
                    auto logical_not = std::make_shared<expr::LogicalUnaryExpr>(
                        expr::LogicalUnaryExpr::OpType::LogicalNot,
                        std::make_shared<expr::AlwaysTrueExpr>());
                    root = std::make_shared<PhyLogicalUnaryExpr>(
                        std::vector<ExprPtr>{outer},
                        logical_not,
                        "NOT",
                        nullptr);
                }
                if (null_rejecting) {
                    root->MarkNullRejecting();
                }
                QueryContext query_context("conjunct_test", nullptr, size, 0);
                ExecContext exec_context(&query_context);
                EvalCtx eval_context(&exec_context);
                TargetBitmap parent(inherited_mask ? size : 0, false);
                for (size_t i = 0; i < parent.size(); ++i) {
                    parent[i] = i % 4 != 0;
                }
                eval_context.set_bitmap_input(parent.clone());

                VectorPtr result;
                root->Eval(eval_context, result);

                auto output = std::dynamic_pointer_cast<ColumnVector>(result);
                ASSERT_NE(output, nullptr);
                ASSERT_EQ(output->size(), size);
                std::vector<size_t> expected_b, expected_c;
                const bool reject_unknown = null_rejecting && !negate;
                for (size_t i = 0; i < size; ++i) {
                    if (!parent.empty() && !parent[i]) {
                        continue;
                    }
                    if (a[i] == Truth::True ||
                        (!reject_unknown && a[i] == Truth::Unknown)) {
                        expected_b.push_back(i);
                        if (b[i] != Truth::True) {
                            expected_c.push_back(i);
                        }
                    }
                    auto expected = std::min(a[i], std::max(b[i], c[i]));
                    if (negate && expected != Truth::Unknown) {
                        expected = expected == Truth::True ? Truth::False
                                                           : Truth::True;
                    }
                    if (reject_unknown) {
                        TargetBitmapView data(output->GetRawData(), size);
                        TargetBitmapView valid(output->GetValidRawData(), size);
                        EXPECT_EQ(data[i] && valid[i], expected == Truth::True)
                            << "row " << i;
                    } else {
                        ExpectTruth(output, i, expected);
                    }
                }
                EXPECT_EQ(second->evaluated_rows_, expected_b);
                EXPECT_EQ(third->evaluated_rows_, expected_c);
                ExpectMask(eval_context, parent);
            }
        }
    }
}

TEST(ConjunctExprTest, InheritedMaskAllowsOrEarlyExit) {
    constexpr size_t size = 65;
    TargetBitmap parent(size, false);
    std::vector<Truth> rows(size, Truth::False);
    for (size_t i = 0; i < size; ++i) {
        parent[i] = i % 7 == 1;
        if (parent[i]) {
            rows[i] = Truth::True;
        }
    }
    auto first = std::make_shared<MaskAwareExpr>(rows, size);
    auto remaining = std::make_shared<MaskAwareExpr>(rows, size);
    PhyConjunctFilterExpr disjunct(
        std::vector<ExprPtr>{first, remaining}, false, nullptr);
    QueryContext query_context("conjunct_test", nullptr, size, 0);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);
    eval_context.set_bitmap_input(parent.clone());

    VectorPtr result;
    disjunct.Eval(eval_context, result);

    EXPECT_EQ(remaining->eval_count_, 0);
    EXPECT_EQ(remaining->move_count_, 1);
    EXPECT_EQ(remaining->cursor_, size);
    ExpectMask(eval_context, parent);
}

TEST(ConjunctExprTest, InheritedMaskRestoredAfterException) {
    constexpr size_t size = 65;
    TargetBitmap parent(size, false);
    std::vector<Truth> rows(size, Truth::True);
    for (size_t i = 0; i < size; ++i) {
        parent[i] = i % 5 == 0;
        if (i % 2 == 0) {
            rows[i] = Truth::False;
        }
    }
    for (const bool fail_first : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "fail_first=" << fail_first);
        auto first = std::make_shared<MaskAwareExpr>(rows, size);
        auto second = std::make_shared<MaskAwareExpr>(rows, size);
        first->throw_on_eval_ = fail_first;
        second->throw_on_eval_ = !fail_first;
        PhyConjunctFilterExpr conjunct(
            std::vector<ExprPtr>{first, second}, true, nullptr);
        QueryContext query_context("conjunct_test", nullptr, size, 0);
        ExecContext exec_context(&query_context);
        EvalCtx eval_context(&exec_context);
        eval_context.set_bitmap_input(parent.clone());

        VectorPtr result;
        EXPECT_THROW(conjunct.Eval(eval_context, result), std::runtime_error);

        EXPECT_EQ(first->eval_count_, 1);
        EXPECT_EQ(second->eval_count_, fail_first ? 0 : 1);
        ExpectMask(eval_context, parent);
    }
}

TEST(ConjunctExprTest, NestedMaskSkipsKeepCursorsAlignedThroughTail) {
    constexpr size_t batch_size = 65;
    constexpr size_t size = 2 * batch_size + 3;
    std::vector<Truth> a(size, Truth::False);
    std::vector<Truth> b(size, Truth::True);
    std::vector<Truth> c(size, Truth::True);
    a[batch_size + 1] = a[2 * batch_size - 1] = Truth::True;
    for (size_t i = 2 * batch_size; i < size; ++i) {
        a[i] = Truth::True;
        b[i] = Truth::False;
        c[i] = static_cast<Truth>(i - 2 * batch_size);
    }
    auto first = std::make_shared<MaskAwareExpr>(a, batch_size);
    auto second = std::make_shared<MaskAwareExpr>(b, batch_size);
    auto third = std::make_shared<MaskAwareExpr>(c, batch_size);
    auto inner = std::make_shared<PhyConjunctFilterExpr>(
        std::vector<ExprPtr>{second, third}, false, nullptr);
    PhyConjunctFilterExpr outer(
        std::vector<ExprPtr>{first, inner}, true, nullptr);
    QueryContext query_context("conjunct_test", nullptr, size, 0);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    for (size_t start = 0; start < size; start += batch_size) {
        VectorPtr result;
        outer.Eval(eval_context, result);
        auto output = std::dynamic_pointer_cast<ColumnVector>(result);
        ASSERT_NE(output, nullptr);
        ASSERT_EQ(output->size(), std::min(batch_size, size - start));
        for (size_t i = 0; i < output->size(); ++i) {
            const auto row = start + i;
            ExpectTruth(output, i, std::min(a[row], std::max(b[row], c[row])));
        }
        EXPECT_TRUE(eval_context.get_bitmap_input().empty());
    }
    EXPECT_EQ(second->eval_count_, 2);
    EXPECT_EQ(second->move_count_, 1);
    EXPECT_EQ(third->eval_count_, 1);
    EXPECT_EQ(third->move_count_, 2);
    EXPECT_EQ(third->evaluated_rows_,
              (std::vector<size_t>{
                  2 * batch_size, 2 * batch_size + 1, 2 * batch_size + 2}));
    EXPECT_EQ(first->cursor_, size);
    EXPECT_EQ(second->cursor_, size);
    EXPECT_EQ(third->cursor_, size);
}

TEST(ConjunctExprTest, NestedOffsetMasksUseCandidatePositions) {
    constexpr size_t size = 131;
    std::vector<Truth> a(size, Truth::True);
    std::vector<Truth> b(size, Truth::False);
    std::vector<Truth> c(size, Truth::False);
    b[130] = Truth::True;
    c[65] = Truth::Unknown;
    auto first = std::make_shared<MaskAwareExpr>(a, 65);
    auto second = std::make_shared<MaskAwareExpr>(b, 65);
    auto third = std::make_shared<MaskAwareExpr>(c, 65);
    auto inner = std::make_shared<PhyConjunctFilterExpr>(
        std::vector<ExprPtr>{second, third}, false, nullptr);
    PhyConjunctFilterExpr outer(
        std::vector<ExprPtr>{first, inner}, true, nullptr);
    for (const auto& node : std::vector<ExprPtr>{first, second, third, inner}) {
        node->SetHasOffsetInput(true);
    }
    outer.SetHasOffsetInput(true);
    QueryContext query_context("conjunct_test", nullptr, size, 0);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);
    OffsetVector offsets;
    for (int32_t row : {130, 3, 130, 65}) {
        offsets.push_back(row);
    }
    eval_context.set_offset_input(&offsets);
    TargetBitmap parent(offsets.size(), true);
    parent[1] = false;
    eval_context.set_bitmap_input(parent.clone());

    VectorPtr result;
    outer.Eval(eval_context, result);

    auto output = std::dynamic_pointer_cast<ColumnVector>(result);
    ASSERT_NE(output, nullptr);
    ASSERT_EQ(output->size(), offsets.size());
    ExpectTruth(output, 0, Truth::True);
    ExpectTruth(output, 2, Truth::True);
    ExpectTruth(output, 3, Truth::Unknown);
    EXPECT_EQ(second->evaluated_rows_, (std::vector<size_t>{130, 130, 65}));
    EXPECT_EQ(third->evaluated_rows_, (std::vector<size_t>{65}));
    ExpectMask(eval_context, parent);

    // Reuse the same expressions with a new candidate batch. All candidates
    // now match B, so C is skipped without moving its sequential cursor.
    offsets.clear();
    offsets.push_back(130);
    offsets.push_back(130);
    TargetBitmap next_parent(offsets.size(), true);
    eval_context.set_bitmap_input(next_parent.clone());
    outer.Eval(eval_context, result);
    output = std::dynamic_pointer_cast<ColumnVector>(result);
    ASSERT_NE(output, nullptr);
    ASSERT_EQ(output->size(), offsets.size());
    ExpectTruth(output, 0, Truth::True);
    ExpectTruth(output, 1, Truth::True);
    EXPECT_EQ(third->eval_count_, 1);
    EXPECT_EQ(third->move_count_, 1);
    EXPECT_EQ(first->cursor_, 0);
    EXPECT_EQ(second->cursor_, 0);
    EXPECT_EQ(third->cursor_, 0);
    ExpectMask(eval_context, next_parent);
}

TEST(ConjunctExprTest, NestedOrSkipKeepsNonnullableNullTailAligned) {
    // Observe the real NULL leaf's batch length before OR combines it with
    // its left operand (whose shorter length could hide a malformed tail).
    class RecordingNullExpr : public PhyNullExpr {
     public:
        using PhyNullExpr::PhyNullExpr;

        void
        Eval(EvalCtx& context, VectorPtr& result) override {
            PhyNullExpr::Eval(context, result);
            last_batch_size_ =
                std::dynamic_pointer_cast<ColumnVector>(result)->size();
        }

        size_t last_batch_size_ = 0;
    };

    constexpr size_t batch_size = 65;
    constexpr size_t size = batch_size + 1;
    auto schema = std::make_shared<Schema>();
    const auto pk = schema->AddDebugField("pk", DataType::INT64);
    const auto field = schema->AddDebugField("value", DataType::VARCHAR);
    schema->set_primary_field_id(pk);
    // This nonnullable NULL predicate needs only schema metadata.
    auto segment = segcore::CreateSealedSegment(schema);
    auto logical_null = std::make_shared<expr::NullExpr>(
        expr::ColumnInfo(field, DataType::VARCHAR),
        proto::plan::NullExpr_NullOp_IsNull);
    auto null_expr = std::make_shared<RecordingNullExpr>(std::vector<ExprPtr>{},
                                                         logical_null,
                                                         "IS NULL",
                                                         nullptr,
                                                         segment.get(),
                                                         size,
                                                         batch_size,
                                                         0);

    std::vector<Truth> a(size, Truth::False), b(size, Truth::False);
    a[1] = b[1] = Truth::True;
    a[batch_size] = Truth::True;
    auto first = std::make_shared<MaskAwareExpr>(a, batch_size);
    auto second = std::make_shared<MaskAwareExpr>(b, batch_size);
    auto inner = std::make_shared<PhyConjunctFilterExpr>(
        std::vector<ExprPtr>{second, null_expr}, false, nullptr);
    PhyConjunctFilterExpr outer(
        std::vector<ExprPtr>{first, inner}, true, nullptr);
    QueryContext query_context("conjunct_test", segment.get(), size, 0);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    VectorPtr result;
    outer.Eval(eval_context, result);
    auto output = std::dynamic_pointer_cast<ColumnVector>(result);
    ASSERT_NE(output, nullptr);
    ASSERT_EQ(output->size(), batch_size);
    ExpectTruth(output, 1, Truth::True);

    outer.Eval(eval_context, result);
    output = std::dynamic_pointer_cast<ColumnVector>(result);
    ASSERT_NE(output, nullptr);
    ASSERT_EQ(output->size(), 1);
    EXPECT_EQ(null_expr->last_batch_size_, 1);
    ExpectTruth(output, 0, Truth::False);
}

TEST(ConjunctExprTest, ColdSiblingDoesNotBlockFirstBatchAndCatchesUpOnUse) {
    class GatedLeaf : public FixedBitmapExpr {
     public:
        explicit GatedLeaf(bool value)
            : FixedBitmapExpr(TargetBitmap(1, value), TargetBitmap(1, true)) {
        }
        bool
        PrefetchReady() const override {
            return ready;
        }
        void
        WaitPrefetch(PrefetchWait mode) override {
            EXPECT_EQ(mode, PrefetchWait::Demand);
            EXPECT_TRUE(ready) << "joined before this predicate was consumed";
            ++joins;
        }
        bool ready = false;
        size_t joins = 0;
    };
    for (bool is_and : {true, false}) {
        auto first = FixedBool(!is_and, true);
        auto cold = std::make_shared<GatedLeaf>(is_and);
        auto last = FixedBool(is_and, true);
        PhyConjunctFilterExpr tree({first, cold, last}, is_and, nullptr);
        if (is_and) {
            tree.SetLikeIndices({1, 2});
            tree.Reorder({0, 1, 2, 3});
        }
        QueryContext query("cold_conjunct", nullptr, 1, 0);
        ExecContext exec(&query);
        EvalCtx ctx(&exec);
        VectorPtr result;
        EXPECT_FALSE(tree.TrySetExecuteAllAtOnce());
        tree.Eval(ctx, result);
        tree.Eval(ctx, result);
        EXPECT_EQ(first->eval_count_, 2);
        EXPECT_EQ(cold->joins, 0);
        EXPECT_EQ(cold->move_count_, 0);
        EXPECT_EQ(cold->eval_count_, 0);

        // The next batch passes the first predicate. Its sibling must start
        // exactly two batches later, even though it was cold for both skips.
        first->SetFirstRow(is_and);
        cold->ready = true;
        tree.Eval(ctx, result);
        EXPECT_EQ(cold->move_count_, 2);
        EXPECT_EQ(cold->eval_count_, 1);
        auto output = std::dynamic_pointer_cast<ColumnVector>(result);
        ASSERT_NE(output, nullptr);
        EXPECT_EQ(TargetBitmapView(output->GetRawData(), 1)[0], is_and);
        EXPECT_TRUE(TargetBitmapView(output->GetValidRawData(), 1)[0]);
    }
}

TEST(ConjunctExprTest, CleanupJoinsSiblingsAfterPrefetchFailure) {
    class JoinProbe : public FixedBitmapExpr {
     public:
        JoinProbe() : FixedBitmapExpr(TargetBitmap(1), TargetBitmap(1)) {
        }
        void
        WaitPrefetch(PrefetchWait mode) override {
            EXPECT_EQ(mode, PrefetchWait::Cleanup);
            ++joins;
            if (fail) {
                throw std::runtime_error("injected prefetch failure");
            }
        }
        bool fail = false;
        size_t joins = 0;
    };
    auto failed = std::make_shared<JoinProbe>();
    auto sibling = std::make_shared<JoinProbe>();
    failed->fail = true;
    PhyConjunctFilterExpr tree({failed, sibling}, true, nullptr);
    EXPECT_THROW(tree.WaitPrefetch(PrefetchWait::Cleanup), std::runtime_error);
    EXPECT_EQ(failed->joins, 1);
    EXPECT_EQ(sibling->joins, 1);
}

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

TEST(ConjunctExprTest, SkipFilterEligibility) {
    EXPECT_TRUE(CanUseSkipFilter(/*is_nullable=*/false,
                                 /*null_rejecting=*/false));
    EXPECT_TRUE(CanUseSkipFilter(/*is_nullable=*/false,
                                 /*null_rejecting=*/true));
    EXPECT_FALSE(CanUseSkipFilter(/*is_nullable=*/true,
                                  /*null_rejecting=*/false));
    EXPECT_TRUE(CanUseSkipFilter(/*is_nullable=*/true,
                                 /*null_rejecting=*/true));
}

}  // namespace milvus::exec
