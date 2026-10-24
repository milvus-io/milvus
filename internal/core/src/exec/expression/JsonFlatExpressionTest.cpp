// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <fmt/core.h>

#include <memory>
#include <string>
#include <vector>

#include "common/Schema.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "exec/expression/ExprCache.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "expr/ITypeExpr.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "test_utils/GenExprProto.h"

namespace milvus::test {

class JsonFlatExpressionTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        exec::ExprResCacheManager::Instance().Clear();
        exec::ExprResCacheManager::SetEnabled(false);
        json_data_ = {
            R"({"a": 1.0})",
            R"({"a": "abc"})",
            R"({"a": 3.0})",
            R"({"a": true})",
            R"({"a": {"b": 1}})",
            R"({"a": []})",
            R"({"a": ["a", "b"]})",
            R"({"a": null})",  // exists null
            R"(1)",
            R"("abc")",
            R"(1.0)",
            R"(true)",
            R"([1, 2, 3])",
            R"({"a": 1, "b": 2})",
            R"({})",
            R"(null)",
            R"({"a": 9007199254740992})",
            R"({"a": 9007199254740993})",
            R"({"a": 9007199254740994})",
        };
        auto schema = std::make_shared<Schema>();
        json_fid_ = schema->AddDebugField("json", DataType::JSON, true);
        auto field = expr_index::JsonField(json_data_, true);
        segment_ = segcore::CreateSealedSegment(schema);
        auto raw_info = raw_files_.Prepare(json_fid_, {field});
        segment_->LoadFieldData(raw_info);
        auto opened =
            expr_index::BuildIndex(json_fid_,
                                   DataType::JSON,
                                   index::INVERTED_INDEX_TYPE,
                                   {field},
                                   {{JSON_PATH, ""}, {JSON_CAST_TYPE, "JSON"}});
        expr_index::InstallIndex(*segment_,
                                 json_fid_,
                                 DataType::JSON,
                                 std::move(opened),
                                 DataType::NONE,
                                 &observed_index_pin_ctx_);
    }

    void
    TearDown() override {
        exec::ExprResCacheManager::Instance().Clear();
        exec::ExprResCacheManager::SetEnabled(false);
    }

    FieldId json_fid_;
    std::vector<std::string> json_data_;
    expr_index::RawFieldFiles raw_files_;
    segcore::SegmentSealedUPtr segment_;
    OpContext* observed_index_pin_ctx_{nullptr};
};

class JsonFlatContainsExpressionTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        exec::ExprResCacheManager::Instance().Clear();
        exec::ExprResCacheManager::SetEnabled(false);
        json_data_ = {
            R"({"a": [1, 2]})",
            R"({"a": [2]})",
            R"({"a": 1})",
            R"({"a": 2})",
            R"({"a": {"b": 1}})",
            R"({"a": []})",
            R"({"a": null})",
            R"({})",
            R"({"a": ["x"]})",
            R"({"a": [1]})",
            R"({"a": [9007199254740992]})",
            R"({"a": [9007199254740993]})",
            R"({"a": [9007199254740994]})",
            R"({"a": [9007199254740992.0]})",
        };
        auto schema = std::make_shared<Schema>();
        json_fid_ = schema->AddDebugField("json", DataType::JSON, true);
        const uint8_t validity[] = {0xFF, 0x3D};
        json_field_ = expr_index::JsonField(json_data_, true, validity);
        segment_ = segcore::CreateSealedSegment(schema);
        auto raw_info = raw_files_.Prepare(json_fid_, {json_field_});
        segment_->LoadFieldData(raw_info);
        auto opened =
            expr_index::BuildIndex(json_fid_,
                                   DataType::JSON,
                                   index::INVERTED_INDEX_TYPE,
                                   {json_field_},
                                   {{JSON_PATH, ""}, {JSON_CAST_TYPE, "JSON"}});
        expr_index::InstallIndex(
            *segment_, json_fid_, DataType::JSON, std::move(opened));
    }

    void
    TearDown() override {
        exec::ExprResCacheManager::Instance().Clear();
        exec::ExprResCacheManager::SetEnabled(false);
    }

    ColumnVectorPtr
    Evaluate(proto::plan::JSONContainsExpr_JSONOp op,
             std::vector<int64_t> values,
             bool negate = false) {
        std::vector<proto::plan::GenericValue> generic_values;
        for (auto value : values) {
            generic_values.emplace_back();
            generic_values.back().set_int64_val(value);
        }
        auto contains_expr = std::make_shared<expr::JsonContainsExpr>(
            expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
            op,
            true,
            generic_values);
        expr::TypedExprPtr filter_expr = contains_expr;
        if (negate) {
            filter_expr = std::make_shared<expr::LogicalUnaryExpr>(
                expr::LogicalUnaryExpr::OpType::LogicalNot, contains_expr);
        }
        return EvalExprInBatches(filter_expr, segment_.get(), json_data_.size())
            .result;
    }

    void
    CheckResult(const ColumnVectorPtr& result,
                const std::vector<bool>& expected_result,
                const std::vector<bool>& expected_valid) {
        ASSERT_EQ(result->size(), expected_result.size());
        ASSERT_EQ(result->size(), expected_valid.size());
        TargetBitmapView result_view(result->GetRawData(), result->size());
        TargetBitmapView valid_view(result->GetValidRawData(), result->size());
        for (size_t i = 0; i < result->size(); ++i) {
            EXPECT_EQ(valid_view[i], expected_valid[i]) << "row " << i;
            if (expected_valid[i]) {
                EXPECT_EQ(result_view[i], expected_result[i]) << "row " << i;
            }
        }
    }

    FieldId json_fid_;
    std::vector<std::string> json_data_;
    std::shared_ptr<FieldData<milvus::Json>> json_field_;
    expr_index::RawFieldFiles raw_files_;
    segcore::SegmentSealedUPtr segment_;
};

TEST_F(JsonFlatContainsExpressionTest, UsesExactPathThreeValuedValidity) {
    const std::vector<bool> expected_valid = {true,
                                              true,
                                              true,
                                              true,
                                              false,
                                              false,
                                              false,
                                              false,
                                              true,
                                              false,
                                              true,
                                              true,
                                              true,
                                              true};

    CheckResult(Evaluate(proto::plan::JSONContainsExpr_JSONOp_Contains, {1}),
                {true,
                 false,
                 true,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false},
                expected_valid);

    CheckResult(
        Evaluate(proto::plan::JSONContainsExpr_JSONOp_ContainsAny, {1, 3}),
        {true,
         false,
         true,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false},
        expected_valid);

    CheckResult(
        Evaluate(proto::plan::JSONContainsExpr_JSONOp_ContainsAll, {1, 2}),
        {true,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false},
        expected_valid);

    CheckResult(
        Evaluate(proto::plan::JSONContainsExpr_JSONOp_Contains, {1}, true),
        {false,
         true,
         false,
         true,
         false,
         false,
         false,
         false,
         true,
         false,
         true,
         true,
         true,
         true},
        expected_valid);
}

TEST_F(JsonFlatContainsExpressionTest,
       RawFallbackPreservesLargeInt64Precision) {
    ExprBatchSizeGuard batch_size_guard(5);
    proto::plan::GenericValue value;
    value.set_int64_val(9007199254740993LL);
    auto contains_expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::JSONContainsExpr_JSONOp_Contains,
        true,
        std::vector<proto::plan::GenericValue>{value});
    EXPECT_FALSE(CanExprExecuteAllAtOnce(
        contains_expr, segment_.get(), json_data_.size()));
    EXPECT_EQ(
        EvalExprBatchSizes(contains_expr, segment_.get(), json_data_.size()),
        (std::vector<int64_t>{5, 5, 4}));

    CheckResult(Evaluate(proto::plan::JSONContainsExpr_JSONOp_Contains,
                         {9007199254740993LL}),
                {false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 true,
                 false,
                 false},
                {true,
                 true,
                 false,
                 false,
                 false,
                 true,
                 false,
                 false,
                 true,
                 false,
                 true,
                 true,
                 true,
                 true});
}

TEST_F(JsonFlatContainsExpressionTest,
       ReusesExactPathValidityAcrossLiteralsAndOperators) {
    auto& cache = exec::ExprResCacheManager::Instance();
    exec::CacheConfig config;
    config.mode = exec::CacheMode::Memory;
    config.mem_max_bytes = 1 << 20;
    config.compression_enabled = true;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(cache.SetConfig(config));
    exec::ExprResCacheManager::SetEnabled(true);

    const std::vector<bool> expected_valid = {true,
                                              true,
                                              true,
                                              true,
                                              false,
                                              false,
                                              false,
                                              false,
                                              true,
                                              false,
                                              true,
                                              true,
                                              true,
                                              true};
    const auto check_validity = [&](const ColumnVectorPtr& result) {
        ASSERT_EQ(result->size(), expected_valid.size());
        TargetBitmapView valid(result->GetValidRawData(), result->size());
        for (size_t row = 0; row < expected_valid.size(); ++row) {
            EXPECT_EQ(valid[row], expected_valid[row]) << "row " << row;
        }
    };

    check_validity(
        Evaluate(proto::plan::JSONContainsExpr_JSONOp_Contains, {1}));
    EXPECT_EQ(cache.GetEntryCount(), 2);

    check_validity(
        Evaluate(proto::plan::JSONContainsExpr_JSONOp_Contains, {2}));
    EXPECT_EQ(cache.GetEntryCount(), 3);

    check_validity(
        Evaluate(proto::plan::JSONContainsExpr_JSONOp_ContainsAny, {1, 3}));
    EXPECT_EQ(cache.GetEntryCount(), 4);
}

TEST_F(JsonFlatExpressionTest, RootComparisonUsesIndex) {
    proto::plan::GenericValue value;
    value.set_int64_val(1);
    auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {""}),
        proto::plan::OpType::GreaterEqual,
        value,
        std::vector<proto::plan::GenericValue>());
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 3);
    EXPECT_TRUE(final[8]);
    EXPECT_TRUE(final[10]);
    EXPECT_TRUE(final[12]);
}

TEST_F(JsonFlatExpressionTest, ComparisonsAndNotPreserveUnknowns) {
    proto::plan::GenericValue value;
    value.set_int64_val(1);
    auto not_equal_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::OpType::NotEqual,
        value,
        std::vector<proto::plan::GenericValue>());
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       not_equal_expr);
    auto final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 4);
    EXPECT_TRUE(final[2]);
    EXPECT_TRUE(final[16]);
    EXPECT_TRUE(final[17]);
    EXPECT_TRUE(final[18]);

    auto greater_than_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::OpType::GreaterThan,
        value,
        std::vector<proto::plan::GenericValue>());
    auto not_greater_than_expr = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, greater_than_expr);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                  not_greater_than_expr);
    final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 2);
    EXPECT_TRUE(final[0]);
    EXPECT_TRUE(final[13]);
}

TEST_F(JsonFlatExpressionTest, JSONArrayEqualityFallsBackToRawData) {
    proto::plan::GenericValue value;
    auto* array = value.mutable_array_val();
    array->set_same_type(true);
    array->add_array()->set_string_val("a");
    array->add_array()->set_string_val("b");

    auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::OpType::Equal,
        value,
        std::vector<proto::plan::GenericValue>());
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);

    EXPECT_EQ(final.count(), 1);
    EXPECT_TRUE(final[6]);
    EXPECT_EQ(observed_index_pin_ctx_, nullptr);
}

TEST_F(JsonFlatExpressionTest,
       JSONContainsMixedAndArrayLiteralsFallBackToRawData) {
    ExprBatchSizeGuard batch_size_guard(7);
    const auto expect_three_batches = [&](const expr::TypedExprPtr& expr) {
        std::vector<int64_t> batch_sizes;
        EXPECT_NO_THROW(batch_sizes = EvalExprBatchSizes(
                            expr, segment_.get(), json_data_.size()));
        EXPECT_EQ(batch_sizes, (std::vector<int64_t>{7, 7, 5}));
    };

    proto::plan::GenericValue string_value;
    string_value.set_string_val("a");
    proto::plan::GenericValue int_value;
    int_value.set_int64_val(1);

    auto mixed_expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
        false,
        std::vector<proto::plan::GenericValue>{string_value, int_value});
    expect_three_batches(mixed_expr);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, mixed_expr);
    auto final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 1);
    EXPECT_TRUE(final[6]);

    proto::plan::GenericValue array_value;
    auto* array = array_value.mutable_array_val();
    array->set_same_type(true);
    array->add_array()->set_string_val("a");
    array->add_array()->set_string_val("b");
    auto array_expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::JSONContainsExpr_JSONOp_Contains,
        true,
        std::vector<proto::plan::GenericValue>{array_value});
    expect_three_batches(array_expr);
    plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, array_expr);
    final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 0);

    auto in_field_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{string_value},
        true);
    expect_three_batches(in_field_expr);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                  in_field_expr);
    final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 1);
    EXPECT_TRUE(final[6]);
}

TEST_F(JsonFlatExpressionTest, ReusesValidityAcrossLiteralsAndOperators) {
    auto& cache = exec::ExprResCacheManager::Instance();
    exec::CacheConfig config;
    config.mode = exec::CacheMode::Memory;
    config.mem_max_bytes = 1 << 20;
    config.compression_enabled = true;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(cache.SetConfig(config));
    exec::ExprResCacheManager::SetEnabled(true);

    auto evaluate = [&](std::vector<std::string> nested_path,
                        proto::plan::OpType op,
                        proto::plan::GenericValue value) {
        auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(json_fid_, DataType::JSON, std::move(nested_path)),
            op,
            value,
            std::vector<proto::plan::GenericValue>());
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           unary_expr);
        return query::ExecuteQueryExpr(
            plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    };
    auto int_value = [](int64_t literal) {
        proto::plan::GenericValue value;
        value.set_int64_val(literal);
        return value;
    };

    EXPECT_EQ(evaluate({"a"}, proto::plan::OpType::Equal, int_value(1)).count(),
              2);
    EXPECT_EQ(cache.GetEntryCount(), 2);

    exec::ExprResCacheManager::Key artifact_key{
        segment_->get_segment_id(),
        fmt::format("json-flat-validity:field={}:path-length=2:"
                    "path=/a:family={}",
                    json_fid_.get(),
                    static_cast<unsigned int>(DataType::DOUBLE))};
    exec::ExprResCacheManager::Value artifact;
    artifact.active_count = json_data_.size();
    ASSERT_TRUE(cache.Get(artifact_key, artifact));
    EXPECT_EQ(artifact.result->count(), 6);
    EXPECT_EQ(artifact.valid_result->count(), json_data_.size());
    artifact.active_count = json_data_.size() + 1;
    EXPECT_FALSE(cache.Get(artifact_key, artifact));

    EXPECT_EQ(evaluate({"a"}, proto::plan::OpType::Equal, int_value(3)).count(),
              1);
    EXPECT_EQ(cache.GetEntryCount(), 3);

    EXPECT_EQ(
        evaluate({"a"}, proto::plan::OpType::GreaterThan, int_value(1)).count(),
        4);
    EXPECT_EQ(cache.GetEntryCount(), 4);

    proto::plan::GenericValue string_value;
    string_value.set_string_val("abc");
    EXPECT_EQ(evaluate({"a"}, proto::plan::OpType::Equal, string_value).count(),
              1);
    EXPECT_EQ(cache.GetEntryCount(), 6);

    EXPECT_EQ(evaluate({"b"}, proto::plan::OpType::Equal, int_value(2)).count(),
              1);
    EXPECT_EQ(cache.GetEntryCount(), 8);

    EXPECT_EQ(cache.EraseSegment(segment_->get_segment_id()), 8);
    EXPECT_EQ(cache.GetEntryCount(), 0);
}

TEST_F(JsonFlatExpressionTest, RawFallbackPreservesLargeInt64LiteralPrecision) {
    ExprBatchSizeGuard batch_size_guard(7);
    const auto evaluate = [&](const expr::TypedExprPtr& expr,
                              bool can_execute_all_at_once) {
        EXPECT_EQ(
            CanExprExecuteAllAtOnce(expr, segment_.get(), json_data_.size()),
            can_execute_all_at_once);
        ExprBatchEvalResult evaluation;
        EXPECT_NO_THROW(evaluation = EvalExprInBatches(
                            expr, segment_.get(), json_data_.size()));
        EXPECT_EQ(evaluation.batch_sizes, (std::vector<int64_t>{7, 7, 5}));
        return evaluation.result;
    };

    proto::plan::GenericValue value;
    value.set_int64_val(9007199254740993LL);

    auto equal_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::OpType::Equal,
        value,
        std::vector<proto::plan::GenericValue>());
    auto result = evaluate(equal_expr, false);
    TargetBitmapView result_view(result->GetRawData(), result->size());
    TargetBitmapView valid_view(result->GetValidRawData(), result->size());
    EXPECT_TRUE(valid_view[16]);
    EXPECT_TRUE(valid_view[17]);
    EXPECT_TRUE(valid_view[18]);
    EXPECT_FALSE(result_view[16]);
    EXPECT_TRUE(result_view[17]);
    EXPECT_FALSE(result_view[18]);

    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{value},
        false);
    result = evaluate(term_expr, false);
    result_view = TargetBitmapView(result->GetRawData(), result->size());
    valid_view = TargetBitmapView(result->GetValidRawData(), result->size());
    EXPECT_TRUE(valid_view[16]);
    EXPECT_TRUE(valid_view[17]);
    EXPECT_TRUE(valid_view[18]);
    EXPECT_FALSE(result_view[16]);
    EXPECT_TRUE(result_view[17]);
    EXPECT_FALSE(result_view[18]);

    auto greater_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::OpType::GreaterThan,
        value,
        std::vector<proto::plan::GenericValue>());
    result = evaluate(greater_expr, false);
    result_view = TargetBitmapView(result->GetRawData(), result->size());
    valid_view = TargetBitmapView(result->GetValidRawData(), result->size());
    EXPECT_TRUE(valid_view[16]);
    EXPECT_TRUE(valid_view[17]);
    EXPECT_TRUE(valid_view[18]);
    EXPECT_FALSE(result_view[16]);
    EXPECT_FALSE(result_view[17]);
    EXPECT_TRUE(result_view[18]);

    auto between_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        value,
        value,
        true,
        true);
    result = evaluate(between_expr, false);
    result_view = TargetBitmapView(result->GetRawData(), result->size());
    valid_view = TargetBitmapView(result->GetValidRawData(), result->size());
    EXPECT_TRUE(valid_view[16]);
    EXPECT_TRUE(valid_view[17]);
    EXPECT_TRUE(valid_view[18]);
    EXPECT_FALSE(result_view[16]);
    EXPECT_TRUE(result_view[17]);
    EXPECT_FALSE(result_view[18]);

    proto::plan::GenericValue upper_float;
    upper_float.set_float_val(9007199254740994.0);
    auto mixed_lower_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        value,
        upper_float,
        true,
        true);
    result = evaluate(mixed_lower_expr, false);
    result_view = TargetBitmapView(result->GetRawData(), result->size());
    valid_view = TargetBitmapView(result->GetValidRawData(), result->size());
    EXPECT_FALSE(result_view[16]);
    EXPECT_TRUE(result_view[17]);
    EXPECT_TRUE(result_view[18]);

    proto::plan::GenericValue lower_float;
    lower_float.set_float_val(9007199254740992.0);
    auto mixed_upper_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        lower_float,
        value,
        true,
        true);
    result = evaluate(mixed_upper_expr, false);
    result_view = TargetBitmapView(result->GetRawData(), result->size());
    valid_view = TargetBitmapView(result->GetValidRawData(), result->size());
    EXPECT_TRUE(result_view[16]);
    EXPECT_TRUE(result_view[17]);
    EXPECT_FALSE(result_view[18]);
}

TEST_F(JsonFlatExpressionTest, EmptyJsonInIsDeterministicForEveryRow) {
    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{},
        false);
    auto check = [&](const expr::TypedExprPtr& filter_expr,
                     bool expected_result) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           filter_expr);
        auto result = gen_filter_res(
            plan.get(), segment_.get(), json_data_.size(), MAX_TIMESTAMP);
        TargetBitmapView result_view(result->GetRawData(), result->size());
        TargetBitmapView valid_view(result->GetValidRawData(), result->size());
        for (size_t i = 0; i < result->size(); ++i) {
            EXPECT_TRUE(valid_view[i]) << "row " << i;
            EXPECT_EQ(result_view[i], expected_result) << "row " << i;
        }
    };

    check(term_expr, false);
    check(std::make_shared<expr::LogicalUnaryExpr>(
              expr::LogicalUnaryExpr::OpType::LogicalNot, term_expr),
          true);
}

TEST_F(JsonFlatExpressionTest, RootExistsRejectsMissingAndNullValues) {
    auto expr = std::make_shared<expr::ExistsExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {""}));
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 15);
    EXPECT_FALSE(final[5]);
    EXPECT_FALSE(final[7]);
    EXPECT_FALSE(final[14]);
    EXPECT_FALSE(final[15]);
}
}  // namespace milvus::test
