// Copyright (C) 2019-2024 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <cstdint>
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <fmt/core.h>
#include <gtest/gtest.h>

#include "common/FieldDataInterface.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/comp/materialized_view.h"
#include "knowhere/config.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentSealed.h"

#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using DataType = milvus::DataType;
using Schema = milvus::Schema;
using FieldName = milvus::FieldName;
using FieldId = milvus::FieldId;

namespace {

// DataType::String is not supported in this test
const std::unordered_map<DataType, std::string> kDataTypeFieldName = {
    {DataType::VECTOR_FLOAT, "VectorFloatField"},
    {DataType::BOOL, "BoolField"},
    {DataType::INT8, "Int8Field"},
    {DataType::INT16, "Int16Field"},
    {DataType::INT32, "Int32Field"},
    {DataType::INT64, "Int64Field"},
    {DataType::FLOAT, "FloatField"},
    {DataType::DOUBLE, "DoubleField"},
    {DataType::VARCHAR, "VarCharField"},
    {DataType::JSON, "JSONField"},
};

constexpr size_t kFieldIdToTouchedCategoriesCntDefault = 0;
constexpr bool kIsPureAndDefault = true;
constexpr bool kHasNotDefault = false;

}  // namespace

class ExprMaterializedViewTest : public testing::Test {
 public:
    // NOTE: If your test fixture defines SetUpTestSuite() or TearDownTestSuite()
    // they must be declared public rather than protected in order to use TEST_P.
    // https://google.github.io/googletest/advanced.html#value-parameterized-tests
    static void
    SetUpTestSuite() {
        // create schema and assign field_id
        schema = std::make_shared<Schema>();
        for (const auto& [data_type, field_name] : kDataTypeFieldName) {
            if (data_type == DataType::VECTOR_FLOAT) {
                schema->AddDebugField(
                    field_name, data_type, kDim, knowhere::metric::L2);
            } else if (data_type == DataType::VARCHAR) {
                std::map<std::string, std::string> empty_params;
                schema->AddDebugVarcharField(FieldName(field_name),
                                             DataType::VARCHAR,
                                             256,
                                             false,
                                             false,
                                             false,
                                             empty_params,
                                             std::nullopt);
            } else {
                schema->AddDebugField(field_name, data_type);
            }
            auto field_id = schema->get_field_id(FieldName(field_name));
            if (data_type == DataType::INT64) {
                schema->set_primary_field_id(field_id);
            }
            data_field_info[data_type].field_id = field_id.get();
        }

        // generate data and prepare for search
        gen_data = std::make_unique<milvus::segcore::GeneratedData>(
            milvus::segcore::DataGen(schema, N));
        segment = CreateSealedWithFieldDataLoaded(schema, *gen_data);
        exec_plan_node_visitor =
            std::make_unique<milvus::query::ExecPlanNodeVisitor>(
                *segment, milvus::MAX_TIMESTAMP);

        // collect mv supported data type
        numeric_str_scalar_data_types.clear();
        for (const auto& e : kDataTypeFieldName) {
            if (e.first != DataType::VECTOR_FLOAT &&
                e.first != DataType::JSON) {
                numeric_str_scalar_data_types.insert(e.first);
            }
        }

        // create schema handle once for all tests
        schema_handle =
            std::make_unique<milvus::segcore::ScopedSchemaHandle>(*schema);
    }

    static void
    TearDownTestSuite() {
        schema_handle = nullptr;
        exec_plan_node_visitor = nullptr;
        segment = nullptr;
        gen_data = nullptr;
    }

 protected:
    // this function takes a string expression and returns a vector search plan
    std::unique_ptr<milvus::query::Plan>
    CreatePlan(const std::string& expr, bool mv_involved) {
        auto binary_plan = schema_handle->ParseSearch(expr,
                                                      "VectorFloatField",
                                                      1,
                                                      "L2",
                                                      R"({"nprobe": 1})",
                                                      3,
                                                      "",  // hints
                                                      mv_involved);
        return milvus::query::CreateSearchPlanByExpr(
            schema, binary_plan.data(), binary_plan.size());
    }

    knowhere::MaterializedViewSearchInfo
    ExecutePlan(const std::unique_ptr<milvus::query::Plan>& plan) {
        auto ph_group_raw = milvus::segcore::CreatePlaceholderGroup(1, kDim);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        segment->Search(plan.get(), ph_group.get(), milvus::MAX_TIMESTAMP);
        return plan->plan_node_->search_info_
            .search_params_[knowhere::meta::MATERIALIZED_VIEW_SEARCH_INFO];
    }

    knowhere::MaterializedViewSearchInfo
    TranslateThenExecuteWhenMvInvolved(const std::string& expr) {
        auto plan = CreatePlan(expr, true);
        return ExecutePlan(plan);
    }

    knowhere::MaterializedViewSearchInfo
    TranslateThenExecuteWhenMvNotInvolved(const std::string& expr) {
        auto plan = CreatePlan(expr, false);
        return ExecutePlan(plan);
    }

    static const std::unordered_set<DataType>&
    GetNumericAndVarcharScalarDataTypes() {
        return numeric_str_scalar_data_types;
    }

    int64_t
    GetFieldID(const DataType& data_type) {
        if (data_field_info.find(data_type) == data_field_info.end()) {
            throw std::runtime_error("Invalid data type " +
                                     fmt::format("{}", data_type));
        }

        return data_field_info[data_type].field_id;
    }

    std::string
    GetFieldName(const DataType& data_type) {
        auto it = kDataTypeFieldName.find(data_type);
        if (it == kDataTypeFieldName.end()) {
            throw std::runtime_error("Invalid data type " +
                                     fmt::format("{}", data_type));
        }
        return it->second;
    }

    // Get a test value as string for given data type
    std::string
    GetTestValue(const DataType& data_type, int value_index) {
        switch (data_type) {
            case DataType::BOOL:
                return value_index == 0 ? "false" : "true";
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64:
            case DataType::FLOAT:
            case DataType::DOUBLE:
                return std::to_string(value_index);
            case DataType::VARCHAR:
                return fmt::format("\"str{}\"", value_index);
            default:
                throw std::runtime_error("Invalid data type " +
                                         fmt::format("{}", data_type));
        }
    }

    void
    TestMvExpectDefault(knowhere::MaterializedViewSearchInfo& mv) {
        EXPECT_EQ(mv.field_id_to_touched_categories_cnt.size(),
                  kFieldIdToTouchedCategoriesCntDefault);
        EXPECT_EQ(mv.is_pure_and, kIsPureAndDefault);
        EXPECT_EQ(mv.has_not, kHasNotDefault);
    }

 protected:
    struct DataFieldInfo {
        std::string field_name;
        int64_t field_id;
    };

    static std::shared_ptr<Schema> schema;
    static std::unordered_map<DataType, DataFieldInfo> data_field_info;

 private:
    static std::unique_ptr<milvus::segcore::GeneratedData> gen_data;
    static milvus::segcore::SegmentSealedUPtr segment;
    static std::unique_ptr<milvus::query::ExecPlanNodeVisitor>
        exec_plan_node_visitor;
    static std::unordered_set<DataType> numeric_str_scalar_data_types;
    static std::unique_ptr<milvus::segcore::ScopedSchemaHandle> schema_handle;

    constexpr static size_t N = 1000;
    constexpr static size_t kDim = 16;
};

std::unordered_map<DataType, ExprMaterializedViewTest::DataFieldInfo>
    ExprMaterializedViewTest::data_field_info = {};
std::shared_ptr<Schema> ExprMaterializedViewTest::schema = nullptr;
std::unique_ptr<milvus::segcore::GeneratedData>
    ExprMaterializedViewTest::gen_data = nullptr;
milvus::segcore::SegmentSealedUPtr ExprMaterializedViewTest::segment = nullptr;
std::unique_ptr<milvus::query::ExecPlanNodeVisitor>
    ExprMaterializedViewTest::exec_plan_node_visitor = nullptr;
std::unordered_set<DataType>
    ExprMaterializedViewTest::numeric_str_scalar_data_types = {};
std::unique_ptr<milvus::segcore::ScopedSchemaHandle>
    ExprMaterializedViewTest::schema_handle = nullptr;

/*************** Test Cases Start ***************/

// Test plan without expr
// Should return default values
TEST_F(ExprMaterializedViewTest, TestMvNoExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        for (const auto& mv_involved : {true, false}) {
            // Empty expression means no predicate
            auto plan = CreatePlan("", mv_involved);
            auto mv = ExecutePlan(plan);
            TestMvExpectDefault(mv);
        }
    }
}

TEST_F(ExprMaterializedViewTest, TestMvNotInvolvedExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        std::string field_name = GetFieldName(data_type);
        std::string val = GetTestValue(data_type, 1);
        // F in [A] -> field_name in [val]
        std::string expr = fmt::format("{} in [{}]", field_name, val);
        auto plan = CreatePlan(expr, false);
        auto mv = ExecutePlan(plan);
        TestMvExpectDefault(mv);
    }
}

TEST_F(ExprMaterializedViewTest, TestMvNotInvolvedJsonExpr) {
    // json_contains(JSONField["A"], 1)
    std::string expr = R"(json_contains(JSONField["A"], 1))";
    auto plan = CreatePlan(expr, false);
    auto mv = ExecutePlan(plan);
    TestMvExpectDefault(mv);
}

// Test json_contains
TEST_F(ExprMaterializedViewTest, TestJsonContainsExpr) {
    // json_contains(JSONField["A"], 1)
    std::string expr = R"(json_contains(JSONField["A"], 1))";
    auto mv = TranslateThenExecuteWhenMvInvolved(expr);
    TestMvExpectDefault(mv);
}

// Test numeric and varchar expr: F0 in [A]
TEST_F(ExprMaterializedViewTest, TestInExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        std::string field_name = GetFieldName(data_type);
        std::string val = GetTestValue(data_type, 1);
        // F in [A]
        std::string expr = fmt::format("{} in [{}]", field_name, val);
        auto mv = TranslateThenExecuteWhenMvInvolved(expr);

        ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 1);
        auto field_id = GetFieldID(data_type);
        ASSERT_TRUE(mv.field_id_to_touched_categories_cnt.find(field_id) !=
                    mv.field_id_to_touched_categories_cnt.end());
        EXPECT_EQ(mv.field_id_to_touched_categories_cnt[field_id], 1);
        EXPECT_EQ(mv.has_not, false);
        EXPECT_EQ(mv.is_pure_and, true);
    }
}

// Test numeric and varchar expr: F0 in [A, A, A]
TEST_F(ExprMaterializedViewTest, TestInDuplicatesExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        std::string field_name = GetFieldName(data_type);
        std::string val = GetTestValue(data_type, 1);
        // F in [A, A, A]
        std::string expr =
            fmt::format("{} in [{}, {}, {}]", field_name, val, val, val);
        auto mv = TranslateThenExecuteWhenMvInvolved(expr);

        ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 1);
        auto field_id = GetFieldID(data_type);
        ASSERT_TRUE(mv.field_id_to_touched_categories_cnt.find(field_id) !=
                    mv.field_id_to_touched_categories_cnt.end());
        EXPECT_EQ(mv.field_id_to_touched_categories_cnt[field_id], 1);
        EXPECT_EQ(mv.has_not, false);
        EXPECT_EQ(mv.is_pure_and, true);
    }
}

// Test numeric and varchar expr: F0 not in [A]
TEST_F(ExprMaterializedViewTest, TestUnaryLogicalNotInExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        std::string field_name = GetFieldName(data_type);
        std::string val = GetTestValue(data_type, 1);
        // F not in [A]
        std::string expr = fmt::format("{} not in [{}]", field_name, val);
        auto mv = TranslateThenExecuteWhenMvInvolved(expr);

        ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 1);
        auto field_id = GetFieldID(data_type);
        ASSERT_TRUE(mv.field_id_to_touched_categories_cnt.find(field_id) !=
                    mv.field_id_to_touched_categories_cnt.end());
        EXPECT_EQ(mv.field_id_to_touched_categories_cnt[field_id], 1);
        EXPECT_EQ(mv.has_not, true);
        EXPECT_EQ(mv.is_pure_and, true);
    }
}

// Test numeric and varchar expr: F0 == A
TEST_F(ExprMaterializedViewTest, TestUnaryRangeEqualExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        std::string field_name = GetFieldName(data_type);
        std::string val = GetTestValue(data_type, 1);
        // F == A
        std::string expr = fmt::format("{} == {}", field_name, val);
        auto mv = TranslateThenExecuteWhenMvInvolved(expr);

        ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 1);
        auto field_id = GetFieldID(data_type);
        ASSERT_TRUE(mv.field_id_to_touched_categories_cnt.find(field_id) !=
                    mv.field_id_to_touched_categories_cnt.end());
        EXPECT_EQ(mv.field_id_to_touched_categories_cnt[field_id], 1);
        EXPECT_EQ(mv.has_not, false);
        EXPECT_EQ(mv.is_pure_and, true);
    }
}

// Test numeric and varchar expr: F0 != A
TEST_F(ExprMaterializedViewTest, TestUnaryRangeNotEqualExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        std::string field_name = GetFieldName(data_type);
        std::string val = GetTestValue(data_type, 1);
        // F != A
        std::string expr = fmt::format("{} != {}", field_name, val);
        auto mv = TranslateThenExecuteWhenMvInvolved(expr);

        ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 1);
        auto field_id = GetFieldID(data_type);
        ASSERT_TRUE(mv.field_id_to_touched_categories_cnt.find(field_id) !=
                    mv.field_id_to_touched_categories_cnt.end());
        EXPECT_EQ(mv.field_id_to_touched_categories_cnt[field_id], 1);
        EXPECT_EQ(mv.has_not, true);
        EXPECT_EQ(mv.is_pure_and, true);
    }
}

// Test numeric and varchar expr: F0 < A, F0 <= A, F0 > A, F0 >= A
TEST_F(ExprMaterializedViewTest, TestUnaryRangeCompareExpr) {
    const std::vector<std::string> ops = {"<", "<=", ">", ">="};
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        for (const auto& op : ops) {
            std::string field_name = GetFieldName(data_type);
            std::string val = GetTestValue(data_type, 1);
            // F op A
            std::string expr = fmt::format("{} {} {}", field_name, op, val);
            auto mv = TranslateThenExecuteWhenMvInvolved(expr);

            ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 1);
            auto field_id = GetFieldID(data_type);
            ASSERT_TRUE(mv.field_id_to_touched_categories_cnt.find(field_id) !=
                        mv.field_id_to_touched_categories_cnt.end());
            EXPECT_EQ(mv.field_id_to_touched_categories_cnt[field_id], 2);
            EXPECT_EQ(mv.has_not, false);
            EXPECT_EQ(mv.is_pure_and, true);
        }
    }
}

// Test numeric and varchar expr: F in [A, B, C]
TEST_F(ExprMaterializedViewTest, TestInMultipleExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        std::string field_name = GetFieldName(data_type);
        std::string val0 = GetTestValue(data_type, 0);
        std::string val1 = GetTestValue(data_type, 1);
        // F in [A, B]
        std::string expr =
            fmt::format("{} in [{}, {}]", field_name, val0, val1);
        auto mv = TranslateThenExecuteWhenMvInvolved(expr);

        ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 1);
        auto field_id = GetFieldID(data_type);
        ASSERT_TRUE(mv.field_id_to_touched_categories_cnt.find(field_id) !=
                    mv.field_id_to_touched_categories_cnt.end());
        EXPECT_EQ(mv.field_id_to_touched_categories_cnt[field_id], 2);
        EXPECT_EQ(mv.has_not, false);
        EXPECT_EQ(mv.is_pure_and, true);
    }
}

// Test numeric and varchar expr: F0 not in [A]
TEST_F(ExprMaterializedViewTest, TestUnaryLogicalNotInMultipleExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        std::string field_name = GetFieldName(data_type);
        std::string val0 = GetTestValue(data_type, 0);
        std::string val1 = GetTestValue(data_type, 1);
        // F not in [A, B]
        std::string expr =
            fmt::format("{} not in [{}, {}]", field_name, val0, val1);
        auto mv = TranslateThenExecuteWhenMvInvolved(expr);

        ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 1);
        auto field_id = GetFieldID(data_type);
        ASSERT_TRUE(mv.field_id_to_touched_categories_cnt.find(field_id) !=
                    mv.field_id_to_touched_categories_cnt.end());
        EXPECT_EQ(mv.field_id_to_touched_categories_cnt[field_id], 2);
        EXPECT_EQ(mv.has_not, true);
        EXPECT_EQ(mv.is_pure_and, true);
    }
}

// Test expr: F0 == A && F1 == B
TEST_F(ExprMaterializedViewTest, TestEqualAndEqualExpr) {
    const DataType c0_data_type = DataType::VARCHAR;
    const DataType c1_data_type = DataType::INT32;
    std::string f0 = GetFieldName(c0_data_type);
    std::string f1 = GetFieldName(c1_data_type);
    std::string val0 = GetTestValue(c0_data_type, 1);
    std::string val1 = GetTestValue(c1_data_type, 2);
    // F0 == A && F1 == B
    std::string expr = fmt::format("{} == {} && {} == {}", f0, val0, f1, val1);

    auto mv = TranslateThenExecuteWhenMvInvolved(expr);

    ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 2);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c0_data_type)],
              1);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c1_data_type)],
              1);
    EXPECT_EQ(mv.has_not, false);
    EXPECT_EQ(mv.is_pure_and, true);
}

// Test expr: F0 == A && F1 in [A, B]
TEST_F(ExprMaterializedViewTest, TestEqualAndInExpr) {
    const DataType c0_data_type = DataType::VARCHAR;
    const DataType c1_data_type = DataType::INT32;

    std::string f0 = GetFieldName(c0_data_type);
    std::string f1 = GetFieldName(c1_data_type);
    std::string val0 = GetTestValue(c0_data_type, 1);
    std::string val1 = GetTestValue(c1_data_type, 1);
    std::string val2 = GetTestValue(c1_data_type, 2);
    // F0 == A && F1 in [A, B]
    std::string expr =
        fmt::format("{} == {} && {} in [{}, {}]", f0, val0, f1, val1, val2);

    auto mv = TranslateThenExecuteWhenMvInvolved(expr);

    ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 2);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c0_data_type)],
              1);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c1_data_type)],
              2);
    EXPECT_EQ(mv.has_not, false);
    EXPECT_EQ(mv.is_pure_and, true);
}

// Test expr: F0 == A && F1 not in [A, B]
TEST_F(ExprMaterializedViewTest, TestEqualAndNotInExpr) {
    const DataType c0_data_type = DataType::VARCHAR;
    const DataType c1_data_type = DataType::INT32;

    std::string f0 = GetFieldName(c0_data_type);
    std::string f1 = GetFieldName(c1_data_type);
    std::string val0 = GetTestValue(c0_data_type, 1);
    std::string val1 = GetTestValue(c1_data_type, 1);
    std::string val2 = GetTestValue(c1_data_type, 2);
    // F0 == A && F1 not in [A, B]
    std::string expr =
        fmt::format("{} == {} && {} not in [{}, {}]", f0, val0, f1, val1, val2);

    auto mv = TranslateThenExecuteWhenMvInvolved(expr);

    ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 2);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c0_data_type)],
              1);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c1_data_type)],
              2);
    EXPECT_EQ(mv.has_not, true);
    EXPECT_EQ(mv.is_pure_and, true);
}

// Test expr: F0 == A || F1 == B
TEST_F(ExprMaterializedViewTest, TestEqualOrEqualExpr) {
    const DataType c0_data_type = DataType::VARCHAR;
    const DataType c1_data_type = DataType::INT32;

    std::string f0 = GetFieldName(c0_data_type);
    std::string f1 = GetFieldName(c1_data_type);
    std::string val0 = GetTestValue(c0_data_type, 1);
    std::string val1 = GetTestValue(c1_data_type, 2);
    // F0 == A || F1 == B
    std::string expr = fmt::format("{} == {} || {} == {}", f0, val0, f1, val1);

    auto mv = TranslateThenExecuteWhenMvInvolved(expr);

    ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 2);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c0_data_type)],
              1);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c1_data_type)],
              1);
    EXPECT_EQ(mv.has_not, false);
    EXPECT_EQ(mv.is_pure_and, false);
}

// Test expr: F0 == A && F1 in [A, B] || F2 == A
TEST_F(ExprMaterializedViewTest, TestEqualAndInOrEqualExpr) {
    const DataType c0_data_type = DataType::VARCHAR;
    const DataType c1_data_type = DataType::INT32;
    const DataType c2_data_type = DataType::INT16;

    std::string f0 = GetFieldName(c0_data_type);
    std::string f1 = GetFieldName(c1_data_type);
    std::string f2 = GetFieldName(c2_data_type);
    std::string val0 = GetTestValue(c0_data_type, 1);
    std::string val1 = GetTestValue(c1_data_type, 1);
    std::string val2 = GetTestValue(c1_data_type, 2);
    std::string val3 = GetTestValue(c2_data_type, 3);
    // F0 == A && (F1 in [A, B] || F2 == C)
    std::string expr = fmt::format("{} == {} && ({} in [{}, {}] || {} == {})",
                                   f0,
                                   val0,
                                   f1,
                                   val1,
                                   val2,
                                   f2,
                                   val3);

    auto mv = TranslateThenExecuteWhenMvInvolved(expr);

    ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 3);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c0_data_type)],
              1);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c1_data_type)],
              2);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c2_data_type)],
              1);
    EXPECT_EQ(mv.has_not, false);
    EXPECT_EQ(mv.is_pure_and, false);
}

// Test expr: F0 == A && not (F1 == B) || F2 == A
TEST_F(ExprMaterializedViewTest, TestEqualAndNotEqualOrEqualExpr) {
    const DataType c0_data_type = DataType::VARCHAR;
    const DataType c1_data_type = DataType::INT32;
    const DataType c2_data_type = DataType::INT16;

    std::string f0 = GetFieldName(c0_data_type);
    std::string f1 = GetFieldName(c1_data_type);
    std::string f2 = GetFieldName(c2_data_type);
    std::string val0 = GetTestValue(c0_data_type, 1);
    std::string val1 = GetTestValue(c1_data_type, 2);
    std::string val2 = GetTestValue(c2_data_type, 1);
    // F0 == A && (not(F1 == B) || F2 == C)
    std::string expr = fmt::format("{} == {} && (not({} == {}) || {} == {})",
                                   f0,
                                   val0,
                                   f1,
                                   val1,
                                   f2,
                                   val2);

    auto mv = TranslateThenExecuteWhenMvInvolved(expr);

    ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 3);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c0_data_type)],
              1);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c1_data_type)],
              1);
    EXPECT_EQ(mv.field_id_to_touched_categories_cnt[GetFieldID(c2_data_type)],
              1);
    EXPECT_EQ(mv.has_not, true);
    EXPECT_EQ(mv.is_pure_and, false);
}

// Test expr: A < F0 < B
TEST_F(ExprMaterializedViewTest, TestBinaryRangeExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        // Skip boolean - range comparison not supported for boolean
        if (data_type == DataType::BOOL) {
            continue;
        }
        std::string field_name = GetFieldName(data_type);
        std::string val0 = GetTestValue(data_type, 0);
        std::string val1 = GetTestValue(data_type, 1);
        // A < F < B (binary range using chained comparison syntax)
        std::string expr = fmt::format("{} < {} < {}", val0, field_name, val1);
        auto mv = TranslateThenExecuteWhenMvInvolved(expr);

        ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 1);
        auto field_id = GetFieldID(data_type);
        ASSERT_TRUE(mv.field_id_to_touched_categories_cnt.find(field_id) !=
                    mv.field_id_to_touched_categories_cnt.end());
        EXPECT_EQ(mv.field_id_to_touched_categories_cnt[field_id], 2);
        EXPECT_EQ(mv.has_not, false);
        EXPECT_EQ(mv.is_pure_and, true);
    }
}
