// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>

#include <algorithm>
#include <barrier>
#include <future>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "common/Schema.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "exec/expression/OffsetExpressionEvaluator.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "test_utils/OriginalCohereFixture.h"
#include "test_utils/OriginalNqTextFixture.h"
#include "test_utils/storage_test_utils.h"

namespace milvus::exec {
namespace {

static_assert(!std::is_copy_constructible_v<OffsetExpressionWorkspace>);
static_assert(!std::is_copy_assignable_v<OffsetExpressionWorkspace>);
static_assert(!std::is_move_constructible_v<OffsetExpressionWorkspace>);
static_assert(!std::is_move_assignable_v<OffsetExpressionWorkspace>);

struct OriginalScalarFixture {
    explicit OriginalScalarFixture(bool text = false) {
        schema = std::make_shared<Schema>();
        field = schema->AddDebugField(
            "original", text ? DataType::VARCHAR : DataType::INT64);
        if (text) {
            const auto rows = LoadOriginalNqTextRows();
            strings.reserve(rows.size());
            for (const auto& row : rows) {
                strings.push_back(row.title);
            }
        } else {
            integers = LoadOriginalCohereIds();
        }
        const auto count = text ? strings.size() : integers.size();
        auto record = std::make_unique<InsertRecordProto>();
        if (text) {
            segcore::InsertCol(record.get(), strings, (*schema)[field], false);
        } else {
            segcore::InsertCol(record.get(), integers, (*schema)[field], false);
        }
        // This historical helper/container name does not generate values.
        // Every scalar above is an unchanged original source field; offsets
        // and timestamps below are only storage-test control metadata.
        segcore::GeneratedData data;
        data.schema_ = schema;
        data.raw_ = record.release();
        data.raw_->set_num_rows(count);
        for (size_t row = 0; row < count; ++row) {
            data.row_ids_.push_back(row);
            data.timestamps_.push_back(row);
        }
        segment = segcore::CreateSealedSegment(schema);
        LoadGeneratedDataIntoSegment(data, segment.get(), false);
        query = std::make_unique<QueryContext>(
            "original-offset-contract", segment.get(), count, MAX_TIMESTAMP);
        context = std::make_unique<ExecContext>(query.get());
    }

    expr::TypedExprPtr
    Mod() const {
        return Arithmetic(proto::plan::ArithOpType::Mod, 10, 5);
    }

    void
    UseNumericIndexOnly() {
        auto data =
            std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
        static_cast<FieldDataBase&>(*data).FillFieldData(integers.data(),
                                                         integers.size());
        auto opened = test::expr_index::BuildIndex(
            field, DataType::INT64, "STL_SORT", {data});
        test::expr_index::InstallIndex(
            *segment, field, DataType::INT64, std::move(opened));
        segment->DropFieldData(field);
        AssertInfo(!segment->HasFieldData(field),
                   "index-only test must not retain raw field data");
    }

    expr::TypedExprPtr
    Arithmetic(proto::plan::ArithOpType operation,
               int64_t operand,
               int64_t comparison) const {
        proto::plan::GenericValue rhs, threshold;
        rhs.set_int64_val(operand);
        threshold.set_int64_val(comparison);
        return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            expr::ColumnInfo(field, DataType::INT64, {}, false),
            proto::plan::OpType::LessThan,
            operation,
            threshold,
            rhs);
    }

    expr::TypedExprPtr
    Range() const {
        proto::plan::GenericValue value;
        value.set_int64_val(integers[integers.size() / 2]);
        return std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field, DataType::INT64, {}, false),
            proto::plan::OpType::GreaterThan,
            value);
    }

    std::shared_ptr<Schema> schema;
    FieldId field;
    std::vector<int64_t> integers;
    std::vector<std::string> strings;
    segcore::SegmentSealedUPtr segment;
    std::unique_ptr<QueryContext> query;
    std::unique_ptr<ExecContext> context;
};

std::shared_ptr<ColumnVector>
Reference(const expr::TypedExprPtr& root,
          ExecContext* context,
          OffsetVector& offsets) {
    ExprSet original({root}, context, false);
    EvalCtx evaluation(context, &offsets);
    std::vector<VectorPtr> output;
    original.Eval(evaluation, output);
    return std::dynamic_pointer_cast<ColumnVector>(output.at(0));
}

std::shared_ptr<ColumnVector>
FullScanReference(const expr::TypedExprPtr& root,
                  ExecContext* context,
                  size_t rows) {
    ExprSet original({root}, context, false);
    EvalCtx evaluation(context);
    std::vector<VectorPtr> output;
    TargetBitmap complete(rows, false), complete_valid(rows, false);
    TargetBitmapView data(complete), valid(complete_valid);
    size_t written = 0;
    while (written < rows) {
        original.Eval(evaluation, output);
        auto batch = std::dynamic_pointer_cast<ColumnVector>(output.at(0));
        AssertInfo(batch != nullptr && batch->IsBitmap() && batch->size() > 0 &&
                       batch->size() <= rows - written,
                   "full scan returned an invalid batch");
        TargetBitmapView batch_data(batch->GetRawData(), batch->size());
        TargetBitmapView batch_valid(batch->GetValidRawData(), batch->size());
        for (size_t i = 0; i < batch->size(); ++i) {
            data[written + i] = batch_data[i];
            valid[written + i] = batch_valid[i];
        }
        written += batch->size();
    }
    return std::make_shared<ColumnVector>(std::move(complete),
                                          std::move(complete_valid));
}

void
ExpectEqual(const std::shared_ptr<ColumnVector>& expected,
            const std::shared_ptr<ColumnVector>& actual) {
    ASSERT_NE(expected, nullptr);
    ASSERT_NE(actual, nullptr);
    ASSERT_EQ(actual->size(), expected->size());
    TargetBitmapView expected_data(expected->GetRawData(), expected->size());
    TargetBitmapView expected_valid(expected->GetValidRawData(),
                                    expected->size());
    TargetBitmapView actual_data(actual->GetRawData(), actual->size());
    TargetBitmapView actual_valid(actual->GetValidRawData(), actual->size());
    for (size_t i = 0; i < expected->size(); ++i) {
        EXPECT_EQ(actual_valid[i], expected_valid[i]) << "lane=" << i;
        EXPECT_EQ(actual_data[i], expected_data[i]) << "lane=" << i;
    }
}

TEST(OffsetExpressionFoundation, OriginalNumericOrderTailsAndRepeatedBatches) {
    OriginalScalarFixture fixture;
    const auto root = fixture.Mod();
    PreparedOffsetExpressionEvaluator prepared(root);
    auto workspace = prepared.CreateWorkspace(fixture.context.get());
    ASSERT_TRUE(workspace->SupportsOffsetInput());
    for (size_t count : {0, 1, 31, 32, 33, 63, 64, 65, 257, 1024, 2051}) {
        OffsetVector offsets;
        for (size_t i = 0; i < count; ++i) {
            // Repeated/out-of-order REQUESTS, not replicated source records.
            offsets.push_back((i * 73 + 11) % fixture.integers.size());
        }
        auto actual = workspace->EvalOffsets(offsets);
        ASSERT_EQ(actual->size(), count);
        if (count == 0) {
            continue;
        }
        ExpectEqual(Reference(root, fixture.context.get(), offsets), actual);
        TargetBitmapView truth(actual->GetRawData(), count);
        TargetBitmapView valid(actual->GetValidRawData(), count);
        for (size_t lane = 0; lane < count; ++lane) {
            EXPECT_TRUE(valid[lane]);
            EXPECT_EQ(truth[lane], fixture.integers[offsets[lane]] % 10 < 5);
        }
    }
}

TEST(OffsetExpressionFoundation, OriginalIndexOnlyNumericBatchesAndMaskGaps) {
    OriginalScalarFixture fixture;
    fixture.UseNumericIndexOnly();
    auto range = fixture.Range();
    auto mod = fixture.Mod();
    const std::vector<expr::TypedExprPtr> roots{
        range,
        mod,
        fixture.Arithmetic(proto::plan::Add, 7, 512),
        fixture.Arithmetic(proto::plan::Mul, 3, 1024),
        std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, range, mod),
        std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, range, mod)};
    for (const auto& root : roots) {
        PreparedOffsetExpressionEvaluator prepared(root);
        auto workspace = prepared.CreateWorkspace(fixture.context.get());
        for (const size_t count : {0, 1, 31, 32, 33, 63, 65, 257, 1027}) {
            OffsetVector offsets;
            for (size_t lane = 0; lane < count; ++lane) {
                // Repeated REQUESTS still reference unchanged original IDs.
                offsets.push_back(lane && lane % 7 == 0
                                      ? offsets.back()
                                      : (lane * 73 + 11) %
                                            fixture.integers.size());
            }
            const auto actual = workspace->EvalOffsets(offsets);
            ASSERT_EQ(actual->size(), count);
            if (count != 0) {
                ExpectEqual(Reference(root, fixture.context.get(), offsets),
                            actual);
            }
        }
        EXPECT_EQ(workspace->ReadStats().bulk_pins, 0);
    }
}

TEST(OffsetExpressionFoundation, OriginalLogicalOperatorsUseExistingKernels) {
    OriginalScalarFixture fixture;
    auto mod = fixture.Mod();
    auto range = fixture.Range();
    const auto not_mod = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, mod);
    std::vector<expr::TypedExprPtr> roots{
        range,
        not_mod,
        std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, range, mod),
        std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, range, not_mod)};
    OffsetVector offsets;
    for (int32_t i = 1023; i >= 0; --i) {
        offsets.push_back(i);
    }
    for (const auto& root : roots) {
        PreparedOffsetExpressionEvaluator prepared(root);
        auto worker = prepared.CreateWorkspace(fixture.context.get());
        ExpectEqual(Reference(root, fixture.context.get(), offsets),
                    worker->EvalOffsets(offsets));
    }
}

TEST(OffsetExpressionFoundation, OriginalAddAndMultiplyMatchScanAndOffset) {
    OriginalScalarFixture fixture;
    // Deliberately stay in the common, non-overflow domain. These source rows
    // cannot validate INT64 extremes or reconcile distinct overflow semantics.
    for (auto value : fixture.integers) {
        ASSERT_GE(value, std::numeric_limits<int64_t>::min() / 3 + 7);
        ASSERT_LE(value, std::numeric_limits<int64_t>::max() / 3 - 7);
    }
    struct Scenario {
        proto::plan::ArithOpType operation;
        int64_t operand;
        int64_t threshold;
        std::function<bool(int64_t)> oracle;
    };
    const std::vector<Scenario> scenarios{
        {proto::plan::ArithOpType::Add,
         7,
         512,
         [](int64_t value) { return value + 7 < 512; }},
        {proto::plan::ArithOpType::Mul, 3, 1024, [](int64_t value) {
             return value * 3 < 1024;
         }}};
    for (const auto& scenario : scenarios) {
        const auto root = fixture.Arithmetic(
            scenario.operation, scenario.operand, scenario.threshold);
        const auto full = FullScanReference(
            root, fixture.context.get(), fixture.integers.size());
        TargetBitmapView full_data(full->GetRawData(), full->size());
        TargetBitmapView full_valid(full->GetValidRawData(), full->size());
        for (size_t row = 0; row < fixture.integers.size(); ++row) {
            EXPECT_TRUE(full_valid[row]);
            EXPECT_EQ(full_data[row], scenario.oracle(fixture.integers[row]));
        }
        PreparedOffsetExpressionEvaluator prepared(root);
        auto worker = prepared.CreateWorkspace(fixture.context.get());
        for (size_t count : {1, 31, 32, 33, 63, 64, 65, 257, 1024, 2051}) {
            OffsetVector offsets;
            for (size_t i = 0; i < count; ++i) {
                // Duplicated requests/lanes do not duplicate source rows.
                offsets.push_back((i * 73 + 11) % fixture.integers.size());
            }
            auto actual = worker->EvalOffsets(offsets);
            ExpectEqual(Reference(root, fixture.context.get(), offsets),
                        actual);
            TargetBitmapView truth(actual->GetRawData(), actual->size());
            TargetBitmapView valid(actual->GetValidRawData(), actual->size());
            for (size_t lane = 0; lane < offsets.size(); ++lane) {
                EXPECT_TRUE(valid[lane]);
                EXPECT_EQ(truth[lane], full_data[offsets[lane]]);
                EXPECT_EQ(truth[lane],
                          scenario.oracle(fixture.integers[offsets[lane]]));
            }
        }
    }
}

TEST(OffsetExpressionFoundation, OriginalUnaryRangesMaskOrderAndTails) {
    OriginalScalarFixture fixture;
    const auto left_threshold = fixture.integers[fixture.integers.size() / 2];
    auto unary = [&](proto::plan::OpType operation, int64_t threshold) {
        proto::plan::GenericValue value;
        value.set_int64_val(threshold);
        return std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(fixture.field, DataType::INT64, {}, false),
            operation,
            value);
    };
    for (const bool is_and : {true, false}) {
        const auto right_threshold =
            fixture.integers[fixture.integers.size() * (is_and ? 3 : 1) / 4];
        auto root = std::make_shared<expr::LogicalBinaryExpr>(
            is_and ? expr::LogicalBinaryExpr::OpType::And
                   : expr::LogicalBinaryExpr::OpType::Or,
            unary(proto::plan::GreaterThan, left_threshold),
            unary(proto::plan::LessThan, right_threshold));
        PreparedOffsetExpressionEvaluator prepared(root);
        auto workspace = prepared.CreateWorkspace(fixture.context.get());
        for (const size_t count : {1, 31, 33, 65, 257, 1027}) {
            OffsetVector offsets;
            for (size_t lane = 0; lane < count; ++lane) {
                // Request duplicates/permutations are control metadata; every
                // predicate value remains an unchanged ORIGINAL Cohere ID.
                offsets.push_back(lane && lane % 7 == 0
                                      ? offsets.back()
                                      : (lane * 73 + 11) %
                                            fixture.integers.size());
            }
            auto actual = workspace->EvalOffsets(offsets);
            ExpectEqual(Reference(root, fixture.context.get(), offsets),
                        actual);
            ASSERT_EQ(actual->size(), count);
            TargetBitmapView truth(actual->GetRawData(), count);
            TargetBitmapView known(actual->GetValidRawData(), count);
            size_t active = 0, transitions = 0;
            bool previous = false;
            for (size_t lane = 0; lane < count; ++lane) {
                const auto value = fixture.integers[offsets[lane]];
                const bool left = value > left_threshold;
                const bool right = value < right_threshold;
                const bool candidate = is_and ? left : !left;
                active += candidate;
                transitions += lane != 0 && candidate != previous;
                previous = candidate;
                EXPECT_TRUE(known[lane]);
                EXPECT_EQ(truth[lane], is_and ? left && right : left || right)
                    << "AND=" << is_and << " lane=" << lane;
            }
            if (count >= 33) {
                // Both operators really pass a nonempty, noncontinuous mask
                // to the second Unary, unlike the existing MOD-only case.
                EXPECT_GT(active, 0);
                EXPECT_LT(active, count);
                EXPECT_GE(transitions, 2);
            }
        }
    }
}

TEST(OffsetExpressionFoundation, OriginalLikePairsMaskOrderAndTails) {
    OriginalScalarFixture fixture(true);
    auto like = [&](const char* pattern) {
        proto::plan::GenericValue value;
        value.set_string_val(pattern);
        return std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(fixture.field, DataType::VARCHAR, {}, false),
            proto::plan::Match,
            value);
    };
    for (const bool is_and : {true, false}) {
        auto root = std::make_shared<expr::LogicalBinaryExpr>(
            is_and ? expr::LogicalBinaryExpr::OpType::And
                   : expr::LogicalBinaryExpr::OpType::Or,
            like("%a%"),
            like("%e%"));
        PreparedOffsetExpressionEvaluator prepared(root);
        auto workspace = prepared.CreateWorkspace(fixture.context.get());
        for (const size_t count : {1, 31, 33, 65, 257, 1027}) {
            OffsetVector offsets;
            for (size_t lane = 0; lane < count; ++lane) {
                offsets.push_back(lane && lane % 7 == 0
                                      ? offsets.back()
                                      : (lane * 73 + 11) %
                                            fixture.strings.size());
            }
            auto actual = workspace->EvalOffsets(offsets);
            ExpectEqual(Reference(root, fixture.context.get(), offsets),
                        actual);
            ASSERT_EQ(actual->size(), count);
            TargetBitmapView truth(actual->GetRawData(), count);
            TargetBitmapView known(actual->GetValidRawData(), count);
            size_t active = 0, transitions = 0;
            bool previous = false;
            for (size_t lane = 0; lane < count; ++lane) {
                const auto& value = fixture.strings[offsets[lane]];
                const bool left = value.find('a') != std::string::npos;
                const bool right = value.find('e') != std::string::npos;
                const bool candidate = is_and ? left : !left;
                active += candidate;
                transitions += lane != 0 && candidate != previous;
                previous = candidate;
                EXPECT_TRUE(known[lane]);
                EXPECT_EQ(truth[lane], is_and ? left && right : left || right)
                    << "AND=" << is_and << " lane=" << lane;
            }
            if (count >= 33) {
                EXPECT_GT(active, 0);
                EXPECT_LT(active, count);
                EXPECT_GE(transitions, 2);
            }
        }
    }
}

TEST(OffsetExpressionFoundation, OriginalTextBorrowedViewsAndBudgets) {
    OriginalScalarFixture fixture(true);
    for (const auto* pattern : {"%a%", "The%", "%tion%"}) {
        proto::plan::GenericValue value;
        value.set_string_val(pattern);
        auto root = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(fixture.field, DataType::VARCHAR, {}, false),
            proto::plan::OpType::Match,
            value);
        OffsetVector offsets;
        for (size_t i = 0; i < 1024; ++i) {
            offsets.push_back((i * 37 + 3) % fixture.strings.size());
        }
        const auto expected = Reference(root, fixture.context.get(), offsets);
        for (size_t budget : {size_t{0}, 32UL << 20, 64UL << 20, 128UL << 20}) {
            OffsetExpressionOptions options;
            options.take.retained_bytes = budget;
            PreparedOffsetExpressionEvaluator prepared(root, options);
            auto worker = prepared.CreateWorkspace(fixture.context.get());
            ExpectEqual(expected, worker->EvalOffsets(offsets));
            ExpectEqual(expected, worker->EvalOffsets(offsets));
            EXPECT_LE(worker->ReadStats().retained_bytes, budget);
        }
    }
}

TEST(OffsetExpressionFoundation, InvalidFactoryAndOffsetsAreErrors) {
    OriginalScalarFixture fixture;
    EXPECT_ANY_THROW(PreparedOffsetExpressionEvaluator(nullptr));
    PreparedOffsetExpressionEvaluator prepared(fixture.Mod());
    EXPECT_ANY_THROW(prepared.CreateWorkspace(nullptr));
    auto worker = prepared.CreateWorkspace(fixture.context.get());
    OffsetVector invalid{-1};
    EXPECT_ANY_THROW(worker->EvalOffsets(invalid));
}

TEST(OffsetExpressionFoundation, NativeCancellationPrecedesTakeAndRecovers) {
    OriginalScalarFixture fixture;
    folly::CancellationSource cancellation;
    milvus::OpContext operation(cancellation.getToken());
    fixture.query->set_op_context(&operation);
    const auto root = fixture.Mod();
    PreparedOffsetExpressionEvaluator prepared(root);
    auto worker = prepared.CreateWorkspace(fixture.context.get());
    OffsetVector offsets{3, 1, 63};
    const auto before = worker->ReadStats();
    cancellation.requestCancellation();

    // Exercise the real native ExprSet::Eval cancellation-token check, not a
    // callback that merely throws the same exception class. No predicate result
    // or new raw Take pin is produced for the cancelled request.
    EXPECT_THROW(worker->EvalOffsets(offsets), folly::FutureCancellation);
    EXPECT_EQ(worker->ReadStats().bulk_pins, before.bulk_pins);
    EXPECT_EQ(worker->ReadStats().retained_bytes, before.retained_bytes);

    operation.cancellation_token = folly::CancellationToken{};
    ExpectEqual(Reference(root, fixture.context.get(), offsets),
                worker->EvalOffsets(offsets));
    fixture.query->set_op_context(nullptr);
}

TEST(OffsetExpressionFoundation, SharedFactoryUsesPrivateConcurrentContexts) {
    OriginalScalarFixture fixture;
    const PreparedOffsetExpressionEvaluator prepared(fixture.Mod());
    for (const size_t concurrency : {1, 4, 8}) {
        std::barrier simultaneous(static_cast<std::ptrdiff_t>(concurrency));
        std::vector<std::future<void>> tasks;
        for (size_t task = 0; task < concurrency; ++task) {
            tasks.push_back(std::async(std::launch::async, [&, task] {
                bool arrived = false;
                try {
                    QueryContext query("private-offset-task",
                                       fixture.segment.get(),
                                       fixture.integers.size(),
                                       MAX_TIMESTAMP);
                    ExecContext execution(&query);
                    // One genuinely shared immutable factory; the context,
                    // physical Expr tree, readers and scratch are task-local.
                    auto worker = prepared.CreateWorkspace(&execution);
                    arrived = true;
                    simultaneous.arrive_and_wait();
                    for (const size_t count : {65, 257, 1024}) {
                        OffsetVector offsets;
                        for (size_t i = 0; i < count; ++i) {
                            offsets.push_back((i * 73 + 11 + task) %
                                              fixture.integers.size());
                        }
                        auto output = worker->EvalOffsets(offsets);
                        TargetBitmapView truth(output->GetRawData(), count);
                        TargetBitmapView known(output->GetValidRawData(),
                                               count);
                        for (size_t lane = 0; lane < count; ++lane) {
                            if (!known[lane] ||
                                truth[lane] !=
                                    (fixture.integers[offsets[lane]] % 10 <
                                     5)) {
                                throw std::runtime_error(
                                    "private-context Expr truth mismatch");
                            }
                        }
                    }
                    if (worker->ReadStats().retained_bytes > (32UL << 20)) {
                        throw std::runtime_error(
                            "worker retention exceeded budget");
                    }
                } catch (...) {
                    if (!arrived)
                        simultaneous.arrive_and_drop();
                    throw;
                }
            }));
        }
        for (auto& task : tasks) {
            EXPECT_NO_THROW(task.get());
        }
    }
}

}  // namespace
}  // namespace milvus::exec
