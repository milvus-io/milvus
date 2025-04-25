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
#include "expr/ITypeExpr.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/comp/materialized_view.h"
#include "knowhere/config.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/ExecPlanNodeVisitor.h"
#include "plan/PlanNode.h"
#include "segcore/SegmentSealed.h"

#include "test_utils/DataGen.h"

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

// use field name to get schema pb string
std::string
GetDataTypeSchemapbStr(const DataType& data_type) {
    if (kDataTypeFieldName.find(data_type) == kDataTypeFieldName.end()) {
        throw std::runtime_error("GetDataTypeSchemapbStr: Invalid data type " +
                                 std::to_string(static_cast<int>(data_type)));
    }

    std::string str = kDataTypeFieldName.at(data_type);
    str.erase(str.find("Field"), 5);
    return str;
}

constexpr size_t kFieldIdToTouchedCategoriesCntDefault = 0;
constexpr bool kIsPureAndDefault = true;
constexpr bool kHasNotDefault = false;

const std::string kFieldIdPlaceholder = "FID";
const std::string kVecFieldIdPlaceholder = "VEC_FID";
const std::string kDataTypePlaceholder = "DT";
const std::string kValPlaceholder = "VAL";
const std::string kPredicatePlaceholder = "PREDICATE_PLACEHOLDER";
const std::string kMvInvolvedPlaceholder = "MV_INVOLVED_PLACEHOLDER";
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
            } else {
                schema->AddDebugField(field_name, data_type);
            }
            data_field_info[data_type].field_id =
                schema->get_field_id(FieldName(field_name)).get();
            std::cout << field_name << " with id "
                      << data_field_info[data_type].field_id << std::endl;
        }

        // generate data and prepare for search
        gen_data = std::make_unique<milvus::segcore::GeneratedData>(
            milvus::segcore::DataGen(schema, N));
        segment = milvus::segcore::CreateSealedSegment(schema);
        auto fields = schema->get_fields();
        milvus::segcore::SealedLoadFieldData(*gen_data, *segment);
        exec_plan_node_visitor =
            std::make_unique<milvus::query::ExecPlanNodeVisitor>(
                *segment, milvus::MAX_TIMESTAMP);

        // prepare plan template
        plan_template = R"(vector_anns: <
                                    field_id: VEC_FID
                                    predicates: <
                                        PREDICATE_PLACEHOLDER
                                    >
                                    query_info: <
                                      topk: 1
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 1}"
                                      materialized_view_involved: MV_INVOLVED_PLACEHOLDER
                                    >
                                    placeholder_tag: "$0">)";
        const int64_t vec_field_id =
            data_field_info[DataType::VECTOR_FLOAT].field_id;
        ReplaceAllOccurrence(plan_template,
                             kVecFieldIdPlaceholder,
                             std::to_string(vec_field_id));

        // collect mv supported data type
        numeric_str_scalar_data_types.clear();
        for (const auto& e : kDataTypeFieldName) {
            if (e.first != DataType::VECTOR_FLOAT &&
                e.first != DataType::JSON) {
                numeric_str_scalar_data_types.insert(e.first);
            }
        }
    }

    static void
    TearDownTestSuite() {
        exec_plan_node_visitor = nullptr;
        segment = nullptr;
        gen_data = nullptr;
    }

 protected:
    // this function takes an predicate string in schemapb format
    // and return a vector search plan
    std::unique_ptr<milvus::query::Plan>
    CreatePlan(const std::string& predicate_str, const bool is_mv_enable) {
        auto plan_str = InterpolateTemplate(predicate_str);
        plan_str = InterpolateMvInvolved(plan_str, is_mv_enable);
        auto binary_plan = milvus::segcore::translate_text_plan_to_binary_plan(
            plan_str.c_str());
        return milvus::query::CreateSearchPlanByExpr(
            *schema, binary_plan.data(), binary_plan.size());
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

    // replace field id, data type and scalar value in a single expr schemapb plan
    std::string
    InterpolateSingleExpr(const std::string& expr_in,
                          const DataType& data_type) {
        std::string expr = expr_in;
        const int64_t field_id = data_field_info[data_type].field_id;
        ReplaceAllOccurrence(
            expr, kFieldIdPlaceholder, std::to_string(field_id));
        ReplaceAllOccurrence(
            expr, kDataTypePlaceholder, GetDataTypeSchemapbStr(data_type));

        // The user can use value placeholder and numeric values after it to distinguish different values
        // eg. VAL1, VAL2, VAL3 should be replaced with different values of the same data type
        std::regex pattern("VAL(\\d+)");
        std::string replacement = "";
        while (std::regex_search(expr, pattern)) {
            switch (data_type) {
                case DataType::BOOL:
                    ReplaceAllOccurrence(expr, "VAL0", "bool_val:false");
                    ReplaceAllOccurrence(expr, "VAL1", "bool_val:true");
                    break;
                case DataType::INT8:
                case DataType::INT16:
                case DataType::INT32:
                case DataType::INT64:
                    replacement = "int64_val:$1";
                    expr = std::regex_replace(expr, pattern, replacement);
                    break;
                case DataType::FLOAT:
                case DataType::DOUBLE:
                    replacement = "float_val:$1";
                    expr = std::regex_replace(expr, pattern, replacement);
                    break;
                case DataType::VARCHAR:
                    replacement = "string_val:\"str$1\"";
                    expr = std::regex_replace(expr, pattern, replacement);
                    break;
                case DataType::JSON:
                    break;
                default:
                    throw std::runtime_error(
                        "InterpolateSingleExpr: Invalid data type " +
                        fmt::format("{}", data_type));
            }

            // fmt::print("expr {} data_type {}\n", expr, data_type);
        }
        return expr;
    }

    knowhere::MaterializedViewSearchInfo
    TranslateThenExecuteWhenMvInolved(const std::string& predicate_str) {
        auto plan = CreatePlan(predicate_str, true);
        return ExecutePlan(plan);
    }

    knowhere::MaterializedViewSearchInfo
    TranslateThenExecuteWhenMvNotInolved(const std::string& predicate_str) {
        auto plan = CreatePlan(predicate_str, false);
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

    void
    TestMvExpectDefault(knowhere::MaterializedViewSearchInfo& mv) {
        EXPECT_EQ(mv.field_id_to_touched_categories_cnt.size(),
                  kFieldIdToTouchedCategoriesCntDefault);
        EXPECT_EQ(mv.is_pure_and, kIsPureAndDefault);
        EXPECT_EQ(mv.has_not, kHasNotDefault);
    }

    static void
    ReplaceAllOccurrence(std::string& str,
                         const std::string& occ,
                         const std::string& replace) {
        str = std::regex_replace(str, std::regex(occ), replace);
    }

    std::string
    InterpolateMvInvolved(const std::string& plan, const bool is_mv_involved) {
        std::string p = plan;
        ReplaceAllOccurrence(
            p, kMvInvolvedPlaceholder, is_mv_involved ? "true" : "false");
        return p;
    }

 private:
    std::string
    InterpolateTemplate(const std::string& predicate_str) {
        std::string plan_str = plan_template;
        ReplaceAllOccurrence(plan_str, kPredicatePlaceholder, predicate_str);
        return plan_str;
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
    static std::string plan_template;

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
std::string ExprMaterializedViewTest::plan_template = "";

/*************** Test Cases Start ***************/

// Test plan without expr
// Should return default values
TEST_F(ExprMaterializedViewTest, TestMvNoExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        for (const auto& mv_involved : {true, false}) {
            std::string plan_str = R"(vector_anns: <
                                    field_id: VEC_FID
                                    query_info: <
                                      topk: 1
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 1}"
                                    >
                                    placeholder_tag: "$0">)";
            const int64_t vec_field_id =
                data_field_info[DataType::VECTOR_FLOAT].field_id;
            ReplaceAllOccurrence(
                plan_str, kVecFieldIdPlaceholder, std::to_string(vec_field_id));
            plan_str = InterpolateMvInvolved(plan_str, mv_involved);
            auto binary_plan =
                milvus::segcore::translate_text_plan_to_binary_plan(
                    plan_str.c_str());
            auto plan = milvus::query::CreateSearchPlanByExpr(
                *schema, binary_plan.data(), binary_plan.size());
            auto mv = ExecutePlan(plan);
            TestMvExpectDefault(mv);
        }
    }
}

TEST_F(ExprMaterializedViewTest, TestMvNotInvolvedExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        std::string predicate = R"(
            term_expr: <
                column_info: <
                    field_id: FID
                    data_type: DT
                >
                values: < VAL1 >
            >
        )";
        predicate = InterpolateSingleExpr(predicate, data_type);
        auto plan = CreatePlan(predicate, false);
        auto mv = ExecutePlan(plan);
        TestMvExpectDefault(mv);
    }
}

TEST_F(ExprMaterializedViewTest, TestMvNotInvolvedJsonExpr) {
    std::string predicate =
        InterpolateSingleExpr(
            R"( json_contains_expr:<column_info:<field_id:FID data_type:DT nested_path:"A" > )",
            DataType::JSON) +
        InterpolateSingleExpr(
            R"( elements:<VAL1> op:Contains elements_same_type:true>)",
            DataType::INT64);
    auto plan = CreatePlan(predicate, false);
    auto mv = ExecutePlan(plan);
    TestMvExpectDefault(mv);
}

// Test json_contains
TEST_F(ExprMaterializedViewTest, TestJsonContainsExpr) {
    std::string predicate =
        InterpolateSingleExpr(
            R"( json_contains_expr:<column_info:<field_id:FID data_type:DT nested_path:"A" > )",
            DataType::JSON) +
        InterpolateSingleExpr(
            R"( elements:<VAL1> op:Contains elements_same_type:true>)",
            DataType::INT64);
    auto mv = TranslateThenExecuteWhenMvInolved(predicate);
    TestMvExpectDefault(mv);
}

// Test numeric and varchar expr: F0 in [A]
TEST_F(ExprMaterializedViewTest, TestInExpr) {
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        std::string predicate = R"(
            term_expr:<
                column_info:<
                    field_id: FID
                    data_type: DT
                >
                values:< VAL1 >
            >
        )";
        predicate = InterpolateSingleExpr(predicate, data_type);
        auto mv = TranslateThenExecuteWhenMvInolved(predicate);
        // fmt::print("Predicate: {}\n", predicate);

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
        std::string predicate = R"(
            term_expr:<
                column_info:<
                    field_id: FID
                    data_type: DT
                >
                values:< VAL1 >
                values:< VAL1 >
                values:< VAL1 >
            >
        )";
        predicate = InterpolateSingleExpr(predicate, data_type);
        auto mv = TranslateThenExecuteWhenMvInolved(predicate);
        // fmt::print("Predicate: {}\n", predicate);

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
        std::string predicate = R"(
            unary_expr:<
                op:Not
                child: <
                    term_expr:<
                        column_info:<
                            field_id: FID
                            data_type: DT
                        >
                        values:< VAL1 >
                    >
                >
            >
        )";
        predicate = InterpolateSingleExpr(predicate, data_type);
        auto mv = TranslateThenExecuteWhenMvInolved(predicate);

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
        std::string predicate = R"(
            unary_range_expr:<
                column_info:<
                    field_id:FID
                    data_type: DT
                >
                op:Equal
                value: < VAL1 >
            >
        )";
        predicate = InterpolateSingleExpr(predicate, data_type);
        auto mv = TranslateThenExecuteWhenMvInolved(predicate);

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
        std::string predicate = R"(
                unary_range_expr:<
                    column_info:<
                        field_id:FID
                        data_type: DT
                    >
                    op: NotEqual
                    value: < VAL1 >
                >
            )";
        predicate = InterpolateSingleExpr(predicate, data_type);
        auto mv = TranslateThenExecuteWhenMvInolved(predicate);

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
    const std::vector<std::string> ops = {
        "LessThan", "LessEqual", "GreaterThan", "GreaterEqual"};
    for (const auto& data_type : GetNumericAndVarcharScalarDataTypes()) {
        for (const auto& ops_str : ops) {
            std::string predicate = R"(
                unary_range_expr:<
                    column_info:<
                        field_id:FID
                        data_type: DT
                    >
                    op: )" + ops_str +
                                    R"(
                    value: < VAL1 >
                >
            )";
            predicate = InterpolateSingleExpr(predicate, data_type);
            auto mv = TranslateThenExecuteWhenMvInolved(predicate);

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
        std::string predicate = R"(
            term_expr:<
                column_info:<
                    field_id: FID
                    data_type: DT
                >
                values:< VAL0 >
                values:< VAL1 >
            >
        )";
        predicate = InterpolateSingleExpr(predicate, data_type);
        auto mv = TranslateThenExecuteWhenMvInolved(predicate);

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
        std::string predicate = R"(
            unary_expr:<
                op:Not
                child: <
                    term_expr:<
                        column_info:<
                            field_id: FID
                            data_type: DT
                        >
                        values:< VAL0 >
                        values:< VAL1 >
                    >
                >
            >
        )";
        predicate = InterpolateSingleExpr(predicate, data_type);
        auto mv = TranslateThenExecuteWhenMvInolved(predicate);

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
    std::string c0 = R"(
            unary_range_expr:<
                column_info:<
                    field_id:FID
                    data_type: DT
                >
                op:Equal
                value: < VAL1 >
            >
        )";
    c0 = InterpolateSingleExpr(c0, c0_data_type);
    std::string c1 = R"(
            unary_range_expr:<
                column_info:<
                    field_id:FID
                    data_type: DT
                >
                op:Equal
                value: < VAL2 >
            >
        )";
    c1 = InterpolateSingleExpr(c1, c1_data_type);
    std::string predicate = R"(
            binary_expr:<
                op:LogicalAnd
                left: <)" + c0 +
                            R"(>
                right: <)" + c1 +
                            R"(>
            >
        )";

    auto mv = TranslateThenExecuteWhenMvInolved(predicate);

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

    std::string c0 = R"(
            unary_range_expr:<
                column_info:<
                    field_id:FID
                    data_type: DT
                >
                op:Equal
                value: < VAL1 >
            >
        )";
    c0 = InterpolateSingleExpr(c0, c0_data_type);
    std::string c1 = R"(
            term_expr:<
                column_info:<
                    field_id: FID
                    data_type: DT
                >
                values:< VAL1 >
                values:< VAL2 >
            >
        )";
    c1 = InterpolateSingleExpr(c1, c1_data_type);
    std::string predicate = R"(
            binary_expr:<
                op:LogicalAnd
                left: <)" + c0 +
                            R"(>
                right: <)" + c1 +
                            R"(>
            >
        )";

    auto mv = TranslateThenExecuteWhenMvInolved(predicate);

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

    std::string c0 = R"(
            unary_range_expr:<
                column_info:<
                    field_id:FID
                    data_type: DT
                >
                op:Equal
                value: < VAL1 >
            >
        )";
    c0 = InterpolateSingleExpr(c0, c0_data_type);
    std::string c1 = R"(
            unary_expr:<
                op:Not
                child: <
                    term_expr:<
                        column_info:<
                            field_id: FID
                            data_type: DT
                        >
                        values:< VAL1 >
                        values:< VAL2 >
                    >
                >
            >
        )";
    c1 = InterpolateSingleExpr(c1, c1_data_type);
    std::string predicate = R"(
            binary_expr:<
                op:LogicalAnd
                left: <)" + c0 +
                            R"(>
                right: <)" + c1 +
                            R"(>
            >
        )";

    auto mv = TranslateThenExecuteWhenMvInolved(predicate);

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

    std::string c0 = R"(
            unary_range_expr:<
                column_info:<
                    field_id:FID
                    data_type: DT
                >
                op:Equal
                value: < VAL1 >
            >
        )";
    c0 = InterpolateSingleExpr(c0, c0_data_type);
    std::string c1 = R"(
            unary_range_expr:<
                column_info:<
                    field_id:FID
                    data_type: DT
                >
                op:Equal
                value: < VAL2 >
            >
        )";
    c1 = InterpolateSingleExpr(c1, c1_data_type);
    std::string predicate = R"(
            binary_expr:<
                op:LogicalOr
                left: <)" + c0 +
                            R"(>
                right: <)" + c1 +
                            R"(>
            >
        )";

    auto mv = TranslateThenExecuteWhenMvInolved(predicate);

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

    std::string c0 = R"(
            unary_range_expr:<
                column_info:<
                    field_id:FID
                    data_type: DT
                >
                op:Equal
                value: < VAL1 >
            >
        )";
    c0 = InterpolateSingleExpr(c0, c0_data_type);
    std::string c1 = R"(
            term_expr:<
                column_info:<
                    field_id: FID
                    data_type: DT
                >
                values:< VAL1 >
                values:< VAL2 >
            >
        )";
    c1 = InterpolateSingleExpr(c1, c1_data_type);
    std::string c2 = R"(
            unary_range_expr:<
                column_info:<
                    field_id:FID
                    data_type: DT
                >
                op:Equal
                value: < VAL3 >
            >
        )";
    c2 = InterpolateSingleExpr(c2, c2_data_type);

    std::string predicate = R"(
            binary_expr:<
                op:LogicalAnd
                left: <)" + c0 +
                            R"(>
                right: <
                    binary_expr:<
                        op:LogicalOr
                        left: <)" +
                            c1 +
                            R"(>
                        right: <)" +
                            c2 +
                            R"(>
                    >
                >
            >
        )";

    auto mv = TranslateThenExecuteWhenMvInolved(predicate);

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

    std::string c0 = R"(
            unary_range_expr:<
                column_info:<
                    field_id:FID
                    data_type: DT
                >
                op:Equal
                value: < VAL1 >
            >
        )";
    c0 = InterpolateSingleExpr(c0, c0_data_type);
    std::string c1 = R"(
            unary_expr:<
                op:Not
                child: <
                    unary_range_expr:<
                        column_info:<
                            field_id:FID
                            data_type: DT
                        >
                        op:Equal
                        value: < VAL2 >
                    >
                >
            >
        )";
    c1 = InterpolateSingleExpr(c1, c1_data_type);
    std::string c2 = R"(
            unary_range_expr:<
                column_info:<
                    field_id:FID
                    data_type: DT
                >
                op:Equal
                value: < VAL1 >
            >
        )";
    c2 = InterpolateSingleExpr(c2, c2_data_type);

    std::string predicate = R"(
            binary_expr:<
                op:LogicalAnd
                left: <)" + c0 +
                            R"(>
                right: <
                    binary_expr:<
                        op:LogicalOr
                        left: <)" +
                            c1 +
                            R"(>
                        right: <)" +
                            c2 +
                            R"(>
                    >
                >
            >
        )";

    auto mv = TranslateThenExecuteWhenMvInolved(predicate);

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
        std::string predicate = R"(
                binary_range_expr: <
                    column_info:<
                        field_id:FID
                        data_type: DT
                    >
                    lower_value: < VAL0 >
                    upper_value: < VAL1 >
                >
            )";
        predicate = InterpolateSingleExpr(predicate, data_type);
        auto mv = TranslateThenExecuteWhenMvInolved(predicate);

        ASSERT_EQ(mv.field_id_to_touched_categories_cnt.size(), 1);
        auto field_id = GetFieldID(data_type);
        ASSERT_TRUE(mv.field_id_to_touched_categories_cnt.find(field_id) !=
                    mv.field_id_to_touched_categories_cnt.end());
        EXPECT_EQ(mv.field_id_to_touched_categories_cnt[field_id], 2);
        EXPECT_EQ(mv.has_not, false);
        EXPECT_EQ(mv.is_pure_and, true);
    }
}
