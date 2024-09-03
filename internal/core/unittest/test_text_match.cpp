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

#include "common/Schema.h"
#include "segcore/segment_c.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "query/PlanProto.h"
#include "query/generated/ExecPlanNodeVisitor.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {
SchemaPtr
GenTestSchema() {
    auto schema = std::make_shared<Schema>();
    std::map<std::string, std::string> params;
    {
        FieldMeta f(FieldName("pk"), FieldId(100), DataType::INT64, false);
        schema->AddField(std::move(f));
        schema->set_primary_field_id(FieldId(100));
    }
    {
        FieldMeta f(FieldName("str"),
                    FieldId(101),
                    DataType::VARCHAR,
                    65536,
                    false,
                    true,
                    params);
        schema->AddField(std::move(f));
    }
    {
        FieldMeta f(FieldName("fvec"),
                    FieldId(102),
                    DataType::VECTOR_FLOAT,
                    16,
                    knowhere::metric::L2,
                    false);
        schema->AddField(std::move(f));
    }
    return schema;
}
}  // namespace

TEST(TextMatch, Index) {
    using Index = index::TextMatchIndex;
    auto index = std::make_unique<Index>(std::numeric_limits<int64_t>::max());
    index->CreateReader();
    index->AddText("football, basketball, pingpang", 0);
    index->AddText("swimming, football", 1);
    index->Commit();
    index->Reload();
    auto res = index->MatchQuery("football");
    ASSERT_TRUE(res[0]);
    ASSERT_TRUE(res[1]);
}

TEST(TextMatch, GrowingNaive) {
    auto schema = GenTestSchema();
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                        "swimming, football"};

    int64_t N = 2;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }

    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);

    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);

    auto get_text_match_expr = [&schema](const std::string& query) -> auto {
        const auto& str_meta = schema->operator[](FieldName("str"));
        auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                               proto::schema::DataType::VarChar,
                                               false,
                                               false);
        auto unary_range_expr =
            test::GenUnaryRangeExpr(OpType::TextMatch, query);
        unary_range_expr->set_allocated_column_info(column_info);
        auto expr = test::GenExpr();
        expr->set_allocated_unary_range_expr(unary_range_expr);

        auto parser = ProtoParser(*schema);
        auto typed_expr = parser.ParseExprs(*expr);
        auto parsed = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, typed_expr);
        return parsed;
    };

    {
        auto expr = get_text_match_expr("football");
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        visitor.ExecuteExprNode(expr, seg.get(), N, final);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
    }

    {
        auto expr = get_text_match_expr("swimming");
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        visitor.ExecuteExprNode(expr, seg.get(), N, final);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
    }

    {
        auto expr = get_text_match_expr("basketball, swimming");
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        visitor.ExecuteExprNode(expr, seg.get(), N, final);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
    }
}

TEST(TextMatch, SealedNaive) {
    auto schema = GenTestSchema();
    auto seg = CreateSealedSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                        "swimming, football"};

    int64_t N = 2;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }

    SealedLoadFieldData(raw_data, *seg);
    seg->CreateTextIndex(FieldId(101));

    auto get_text_match_expr = [&schema](const std::string& query) -> auto {
        const auto& str_meta = schema->operator[](FieldName("str"));
        auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                               proto::schema::DataType::VarChar,
                                               false,
                                               false);
        auto unary_range_expr =
            test::GenUnaryRangeExpr(OpType::TextMatch, query);
        unary_range_expr->set_allocated_column_info(column_info);
        auto expr = test::GenExpr();
        expr->set_allocated_unary_range_expr(unary_range_expr);

        auto parser = ProtoParser(*schema);
        auto typed_expr = parser.ParseExprs(*expr);
        auto parsed = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, typed_expr);
        return parsed;
    };

    {
        auto expr = get_text_match_expr("football");
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        visitor.ExecuteExprNode(expr, seg.get(), N, final);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
    }

    {
        auto expr = get_text_match_expr("swimming");
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        visitor.ExecuteExprNode(expr, seg.get(), N, final);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
    }

    {
        auto expr = get_text_match_expr("basketball, swimming");
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        visitor.ExecuteExprNode(expr, seg.get(), N, final);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
    }
}
