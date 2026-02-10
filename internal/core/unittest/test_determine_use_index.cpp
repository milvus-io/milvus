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
#include <cstdint>
#include <memory>
#include <vector>

#include "common/Consts.h"
#include "common/Schema.h"
#include "exec/QueryContext.h"
#include "exec/Task.h"
#include "exec/expression/Expr.h"
#include "exec/expression/AlwaysTrueExpr.h"
#include "exec/expression/LogicalBinaryExpr.h"
#include "exec/expression/LogicalUnaryExpr.h"
#include "expr/ITypeExpr.h"
#include "index/ScalarIndex.h"
#include "plan/PlanNode.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/cachinglayer_test_utils.h"

using namespace milvus;
using namespace milvus::exec;
using namespace milvus::query;
using namespace milvus::segcore;

class DetermineUseIndexTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        vec_fid_ = schema_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        int64_fid_ = schema_->AddDebugField("int64", DataType::INT64);
        varchar_fid_ = schema_->AddDebugField("varchar", DataType::VARCHAR);
        schema_->set_primary_field_id(varchar_fid_);
    }

    // Helper: create a sealed segment with data loaded but no scalar index
    SegmentSealedSPtr
    CreateSegmentNoIndex() {
        auto raw_data = DataGen(schema_, N_);
        raw_data_ = std::make_shared<GeneratedData>(std::move(raw_data));
        auto segment = CreateSealedWithFieldDataLoaded(schema_, *raw_data_);
        return SegmentSealedSPtr(segment.release());
    }

    // Helper: create a sealed segment with a scalar index on int64 field
    SegmentSealedSPtr
    CreateSegmentWithInt64Index() {
        auto raw_data = DataGen(schema_, N_);
        raw_data_ = std::make_shared<GeneratedData>(std::move(raw_data));
        auto segment = CreateSealedWithFieldDataLoaded(schema_, *raw_data_);

        // Build and load STL_SORT scalar index for int64 field
        auto int64_col = raw_data_->get_col<int64_t>(int64_fid_);
        auto int64_index = milvus::index::CreateScalarIndexSort<int64_t>();
        int64_index->Build(N_, int64_col.data());

        LoadIndexInfo load_index_info;
        load_index_info.field_id = int64_fid_.get();
        load_index_info.field_type = DataType::INT64;
        load_index_info.index_params = GenIndexParams(int64_index.get());
        load_index_info.cache_index =
            CreateTestCacheIndex("test_int64", std::move(int64_index));
        segment->LoadIndex(load_index_info);

        return SegmentSealedSPtr(segment.release());
    }

    // Helper: compile expressions with a given segment
    std::vector<ExprPtr>
    Compile(const std::vector<expr::TypedExprPtr>& logical_exprs,
            const segcore::SegmentInternalInterface* segment) {
        auto query_context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment, N_, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        return CompileExpressions(logical_exprs, &context, {}, false);
    }

    // Helper: make UnaryRangeFilterExpr (int64 < value)
    expr::TypedExprPtr
    MakeUnaryRangeExpr(FieldId field_id,
                       DataType data_type,
                       proto::plan::OpType op,
                       int64_t value) {
        proto::plan::GenericValue val;
        val.set_int64_val(value);
        return std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_id, data_type),
            op,
            val,
            std::vector<proto::plan::GenericValue>{});
    }

    // Helper: make TermFilterExpr (field IN [values])
    expr::TypedExprPtr
    MakeTermExpr(FieldId field_id,
                 DataType data_type,
                 const std::vector<int64_t>& values) {
        std::vector<proto::plan::GenericValue> vals;
        for (auto v : values) {
            proto::plan::GenericValue gv;
            gv.set_int64_val(v);
            vals.push_back(gv);
        }
        return std::make_shared<expr::TermFilterExpr>(
            expr::ColumnInfo(field_id, data_type), vals);
    }

    // Helper: make BinaryRangeFilterExpr (lower < field < upper)
    expr::TypedExprPtr
    MakeBinaryRangeExpr(FieldId field_id,
                        DataType data_type,
                        int64_t lower,
                        int64_t upper,
                        bool lower_inclusive = false,
                        bool upper_inclusive = false) {
        proto::plan::GenericValue lower_val;
        lower_val.set_int64_val(lower);
        proto::plan::GenericValue upper_val;
        upper_val.set_int64_val(upper);
        return std::make_shared<expr::BinaryRangeFilterExpr>(
            expr::ColumnInfo(field_id, data_type),
            lower_val,
            upper_val,
            lower_inclusive,
            upper_inclusive);
    }

    SchemaPtr schema_;
    FieldId vec_fid_;
    FieldId int64_fid_;
    FieldId varchar_fid_;
    int64_t N_ = 1000;
    std::shared_ptr<GeneratedData> raw_data_;
};

// --------------------------------------------------------------------------
// Group 1: DetermineUseIndex — No Index
// --------------------------------------------------------------------------
TEST_F(DetermineUseIndexTest, DetermineUseIndex_NoIndex) {
    auto segment = CreateSegmentNoIndex();

    // UnaryRangeExpr on int64 without index
    auto unary_expr = MakeUnaryRangeExpr(
        int64_fid_, DataType::INT64, proto::plan::OpType::LessThan, 100);
    auto compiled = Compile({unary_expr}, segment.get());
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_FALSE(compiled[0]->CanExecuteAllAtOnce())
        << "No index => use_index_=false => CanExecuteAllAtOnce=false";

    // TermExpr on int64 without index
    auto term_expr = MakeTermExpr(int64_fid_, DataType::INT64, {1, 2, 3});
    compiled = Compile({term_expr}, segment.get());
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_FALSE(compiled[0]->CanExecuteAllAtOnce())
        << "No index => use_index_=false => CanExecuteAllAtOnce=false";

    // BinaryRangeExpr on int64 without index
    auto br_expr =
        MakeBinaryRangeExpr(int64_fid_, DataType::INT64, 10, 200, true, false);
    compiled = Compile({br_expr}, segment.get());
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_FALSE(compiled[0]->CanExecuteAllAtOnce())
        << "No index => use_index_=false => CanExecuteAllAtOnce=false";
}

// --------------------------------------------------------------------------
// Group 2: DetermineUseIndex — With Index
// --------------------------------------------------------------------------
TEST_F(DetermineUseIndexTest, DetermineUseIndex_WithIndex) {
    auto segment = CreateSegmentWithInt64Index();

    // UnaryRangeExpr(Equal) on indexed int64
    auto unary_expr = MakeUnaryRangeExpr(
        int64_fid_, DataType::INT64, proto::plan::OpType::Equal, 42);
    auto compiled = Compile({unary_expr}, segment.get());
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_TRUE(compiled[0]->CanExecuteAllAtOnce())
        << "With STL_SORT index => use_index_=true => CanExecuteAllAtOnce=true";

    // TermExpr on indexed int64
    auto term_expr = MakeTermExpr(int64_fid_, DataType::INT64, {1, 2, 3});
    compiled = Compile({term_expr}, segment.get());
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_TRUE(compiled[0]->CanExecuteAllAtOnce())
        << "With STL_SORT index => use_index_=true => CanExecuteAllAtOnce=true";

    // BinaryRangeExpr on indexed int64
    auto br_expr =
        MakeBinaryRangeExpr(int64_fid_, DataType::INT64, 10, 200, true, true);
    compiled = Compile({br_expr}, segment.get());
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_TRUE(compiled[0]->CanExecuteAllAtOnce())
        << "With STL_SORT index => use_index_=true => CanExecuteAllAtOnce=true";
}

// --------------------------------------------------------------------------
// Group 3: DetermineUseIndex — ARRAY type never uses index
// --------------------------------------------------------------------------
class DetermineUseIndexArrayTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        vec_fid_ = schema_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        array_fid_ = schema_->AddDebugField(
            "array_field", DataType::ARRAY, DataType::INT64);
        auto pk_fid = schema_->AddDebugField("pk", DataType::INT64);
        schema_->set_primary_field_id(pk_fid);
    }

    SchemaPtr schema_;
    FieldId vec_fid_;
    FieldId array_fid_;
    int64_t N_ = 1000;
};

TEST_F(DetermineUseIndexArrayTest, DetermineUseIndex_ArrayType) {
    auto raw_data = DataGen(schema_, N_);
    auto segment = CreateSealedWithFieldDataLoaded(schema_, raw_data);
    auto seg_ptr = segment.get();

    // UnaryRangeExpr on ARRAY field — ARRAY type should never use index
    proto::plan::GenericValue val;
    val.set_int64_val(42);
    auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(array_fid_, DataType::ARRAY, DataType::INT64),
        proto::plan::OpType::Equal,
        val,
        std::vector<proto::plan::GenericValue>{});

    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, seg_ptr, N_, MAX_TIMESTAMP);
    ExecContext context(query_context.get());
    auto compiled = CompileExpressions({unary_expr}, &context, {}, false);
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_FALSE(compiled[0]->CanExecuteAllAtOnce())
        << "ARRAY type => use_index_=false => CanExecuteAllAtOnce=false";

    // TermExpr on ARRAY field
    std::vector<proto::plan::GenericValue> term_vals;
    proto::plan::GenericValue gv;
    gv.set_int64_val(1);
    term_vals.push_back(gv);
    gv.set_int64_val(2);
    term_vals.push_back(gv);
    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(array_fid_, DataType::ARRAY, DataType::INT64),
        term_vals);

    compiled = CompileExpressions({term_expr}, &context, {}, false);
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_FALSE(compiled[0]->CanExecuteAllAtOnce())
        << "ARRAY type TermExpr => use_index_=false => "
           "CanExecuteAllAtOnce=false";

    // BinaryRangeExpr on ARRAY field
    proto::plan::GenericValue lower_val, upper_val;
    lower_val.set_int64_val(10);
    upper_val.set_int64_val(200);
    auto br_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(array_fid_, DataType::ARRAY, DataType::INT64),
        lower_val,
        upper_val,
        true,
        false);

    compiled = CompileExpressions({br_expr}, &context, {}, false);
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_FALSE(compiled[0]->CanExecuteAllAtOnce())
        << "ARRAY type BinaryRangeExpr => use_index_=false => "
           "CanExecuteAllAtOnce=false";
}

// --------------------------------------------------------------------------
// Group 4: CanExecuteAllAtOnce for non-SegmentExpr (AlwaysTrueExpr)
// --------------------------------------------------------------------------
TEST_F(DetermineUseIndexTest, CanExecuteAllAtOnce_AlwaysTrueExpr) {
    auto segment = CreateSegmentNoIndex();

    auto always_true_expr = std::make_shared<expr::AlwaysTrueExpr>();

    auto compiled = Compile({always_true_expr}, segment.get());
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_TRUE(compiled[0]->CanExecuteAllAtOnce())
        << "AlwaysTrueExpr always returns true for CanExecuteAllAtOnce";
}

// --------------------------------------------------------------------------
// Group 5: CanExecuteAllAtOnce for composite expressions
// --------------------------------------------------------------------------
TEST_F(DetermineUseIndexTest, CanExecuteAllAtOnce_LogicalBinaryBothIndexed) {
    auto segment = CreateSegmentWithInt64Index();

    // (int64 == 42) AND (int64 < 100) — both use index
    auto expr1 = MakeUnaryRangeExpr(
        int64_fid_, DataType::INT64, proto::plan::OpType::Equal, 42);
    auto expr2 = MakeUnaryRangeExpr(
        int64_fid_, DataType::INT64, proto::plan::OpType::LessThan, 100);
    auto and_expr = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, expr1, expr2);

    auto compiled = Compile({and_expr}, segment.get());
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_TRUE(compiled[0]->CanExecuteAllAtOnce())
        << "Both children indexed => LogicalBinary CanExecuteAllAtOnce=true";
}

TEST_F(DetermineUseIndexTest, CanExecuteAllAtOnce_LogicalBinaryMixed) {
    auto segment = CreateSegmentWithInt64Index();

    // (int64 == 42) AND (varchar < "abc")
    // int64 has index, varchar does NOT have index
    auto expr1 = MakeUnaryRangeExpr(
        int64_fid_, DataType::INT64, proto::plan::OpType::Equal, 42);

    proto::plan::GenericValue str_val;
    str_val.set_string_val("abc");
    auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(varchar_fid_, DataType::VARCHAR),
        proto::plan::OpType::LessThan,
        str_val,
        std::vector<proto::plan::GenericValue>{});

    auto and_expr = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, expr1, expr2);

    auto compiled = Compile({and_expr}, segment.get());
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_FALSE(compiled[0]->CanExecuteAllAtOnce())
        << "One child not indexed => LogicalBinary CanExecuteAllAtOnce=false";
}

TEST_F(DetermineUseIndexTest, CanExecuteAllAtOnce_LogicalUnary) {
    auto segment = CreateSegmentWithInt64Index();

    // NOT(int64 == 42) — child uses index
    auto child_expr = MakeUnaryRangeExpr(
        int64_fid_, DataType::INT64, proto::plan::OpType::Equal, 42);
    auto not_expr = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);

    auto compiled = Compile({not_expr}, segment.get());
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_TRUE(compiled[0]->CanExecuteAllAtOnce())
        << "NOT(indexed_expr) => propagates from child => "
           "CanExecuteAllAtOnce=true";

    // NOT(varchar < "abc") — child does NOT use index
    proto::plan::GenericValue str_val;
    str_val.set_string_val("abc");
    auto child_no_idx = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(varchar_fid_, DataType::VARCHAR),
        proto::plan::OpType::LessThan,
        str_val,
        std::vector<proto::plan::GenericValue>{});
    auto not_expr2 = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, child_no_idx);

    compiled = Compile({not_expr2}, segment.get());
    ASSERT_EQ(compiled.size(), 1);
    EXPECT_FALSE(compiled[0]->CanExecuteAllAtOnce())
        << "NOT(non_indexed_expr) => propagates from child => "
           "CanExecuteAllAtOnce=false";
}

// --------------------------------------------------------------------------
// Group 6: ExprSet CanExecuteAllAtOnce
// --------------------------------------------------------------------------
TEST_F(DetermineUseIndexTest, ExprSet_CanExecuteAllAtOnce) {
    auto segment_indexed = CreateSegmentWithInt64Index();

    // All indexed expressions
    auto expr1 = MakeUnaryRangeExpr(
        int64_fid_, DataType::INT64, proto::plan::OpType::Equal, 42);

    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, segment_indexed.get(), N_, MAX_TIMESTAMP);
    ExecContext exec_ctx(query_context.get());
    ExprSet expr_set({expr1}, &exec_ctx);
    EXPECT_TRUE(expr_set.CanExecuteAllAtOnce())
        << "All expressions indexed => ExprSet CanExecuteAllAtOnce=true";

    // Mix: create segment without index for varchar
    proto::plan::GenericValue str_val;
    str_val.set_string_val("abc");
    auto expr_no_idx = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(varchar_fid_, DataType::VARCHAR),
        proto::plan::OpType::LessThan,
        str_val,
        std::vector<proto::plan::GenericValue>{});

    ExprSet expr_set_mixed({expr_no_idx}, &exec_ctx);
    EXPECT_FALSE(expr_set_mixed.CanExecuteAllAtOnce())
        << "Non-indexed expression in set => ExprSet CanExecuteAllAtOnce=false";
}

// --------------------------------------------------------------------------
// Group 7: End-to-end correctness — FilterBitsNode
// --------------------------------------------------------------------------
TEST_F(DetermineUseIndexTest, FilterBitsNode_ExecuteAllAtOnce_Correctness) {
    auto segment = CreateSegmentWithInt64Index();

    // int64 == raw_data_[0] — should match exactly one row (or more if duplicates)
    auto int64_col = raw_data_->get_col<int64_t>(int64_fid_);
    int64_t target_value = int64_col[0];

    auto logical_expr = MakeUnaryRangeExpr(
        int64_fid_, DataType::INT64, proto::plan::OpType::Equal, target_value);

    std::vector<plan::PlanNodePtr> sources;
    auto filter_node = std::make_shared<plan::FilterBitsNode>(
        "plannode id 1", logical_expr, sources);
    auto plan = plan::PlanFragment(filter_node);
    auto query_context = std::make_shared<QueryContext>(
        "test_e2e_indexed",
        segment.get(),
        N_,
        MAX_TIMESTAMP,
        0,
        0,
        query::PlanOptions{false},
        std::make_shared<QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto task = Task::Create("task_indexed", plan, 0, query_context);
    int64_t num_rows = 0;
    for (;;) {
        auto result = task->Next();
        if (!result) {
            break;
        }
        num_rows += result->size();
    }
    EXPECT_EQ(num_rows, N_) << "Result should cover all rows";

    // Count expected matches
    int64_t expected_matches = 0;
    for (int i = 0; i < N_; i++) {
        if (int64_col[i] == target_value) {
            expected_matches++;
        }
    }
    EXPECT_GT(expected_matches, 0) << "Should have at least one match";
}

TEST_F(DetermineUseIndexTest, FilterBitsNode_NoIndex_Correctness) {
    auto segment = CreateSegmentNoIndex();

    // int64 < 100 — brute force (no index)
    auto logical_expr = MakeUnaryRangeExpr(
        int64_fid_, DataType::INT64, proto::plan::OpType::LessThan, 100);

    std::vector<plan::PlanNodePtr> sources;
    auto filter_node = std::make_shared<plan::FilterBitsNode>(
        "plannode id 1", logical_expr, sources);
    auto plan = plan::PlanFragment(filter_node);
    auto query_context = std::make_shared<QueryContext>(
        "test_e2e_no_index",
        segment.get(),
        N_,
        MAX_TIMESTAMP,
        0,
        0,
        query::PlanOptions{false},
        std::make_shared<QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto task = Task::Create("task_no_index", plan, 0, query_context);
    int64_t num_rows = 0;
    for (;;) {
        auto result = task->Next();
        if (!result) {
            break;
        }
        num_rows += result->size();
    }
    EXPECT_EQ(num_rows, N_) << "Result should cover all rows";
}
