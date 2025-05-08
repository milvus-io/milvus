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
#include <boost/format.hpp>

#include "pb/plan.pb.h"
#include "segcore/SegmentSealed.h"

#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "pb/schema.pb.h"
#include "test_cachinglayer/cachinglayer_test_utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "query/PlanProto.h"
#include "query/ExecPlanNodeVisitor.h"
#include "index/InvertedIndexTantivy.h"
#include "test_utils/storage_test_utils.h"
using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

SchemaPtr
GenTestSchema() {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("str", DataType::VARCHAR);
    schema->AddDebugField("another_str", DataType::VARCHAR);
    schema->AddDebugField("json", DataType::JSON);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(pk);
    schema->AddDebugField("another_int64", DataType::INT64);
    return schema;
}

class GrowingSegmentRegexQueryTest : public ::testing::Test {
 public:
    void
    SetUp() override {
        schema = GenTestSchema();
        seg = CreateGrowingSegment(schema, empty_index_meta);
        raw_str = {
            "b\n",
            "a\n",
            "aaa\n",
            "abbb\n",
            "abcabcabc\n",
        };
        raw_json = {
            R"({"int":1})",
            R"({"float":1.0})",
            R"({"str":"aaa"})",
            R"({"str":"bbb"})",
            R"({"str":"abcabcabc"})",
        };

        N = 5;
        uint64_t seed = 19190504;
        auto raw_data = DataGen(schema, N, seed);
        auto str_col = raw_data.raw_->mutable_fields_data()
                           ->at(0)
                           .mutable_scalars()
                           ->mutable_string_data()
                           ->mutable_data();
        for (int64_t i = 0; i < N; i++) {
            str_col->at(i) = raw_str[i];
        }

        auto json_col = raw_data.raw_->mutable_fields_data()
                            ->at(2)
                            .mutable_scalars()
                            ->mutable_json_data()
                            ->mutable_data();
        for (int64_t i = 0; i < N; i++) {
            json_col->at(i) = raw_json[i];
        }

        seg->PreInsert(N);
        seg->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    void
    TearDown() override {
    }

 public:
    SchemaPtr schema;
    SegmentGrowingPtr seg;
    int64_t N;
    std::vector<std::string> raw_str;
    std::vector<std::string> raw_json;
};

TEST_F(GrowingSegmentRegexQueryTest, RegexQueryOnNonStringField) {
    int64_t operand = 120;
    const auto& int_meta = schema->operator[](FieldName("int64"));
    auto column_info = test::GenColumnInfo(
        int_meta.get_id().get(), proto::schema::DataType::Int64, false, false);
    auto unary_range_expr = test::GenUnaryRangeExpr(OpType::Match, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    auto segpromote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    BitsetType final;
    ASSERT_ANY_THROW(ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP));
}

TEST_F(GrowingSegmentRegexQueryTest, RegexQueryOnStringField) {
    std::string operand = "a%";
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto unary_range_expr = test::GenUnaryRangeExpr(OpType::Match, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    auto segpromote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP);
    ASSERT_FALSE(final[0]);
    ASSERT_TRUE(final[1]);
    ASSERT_TRUE(final[2]);
    ASSERT_TRUE(final[3]);
    ASSERT_TRUE(final[4]);
}

TEST_F(GrowingSegmentRegexQueryTest, RegexQueryOnJsonField) {
    std::string operand = "a%";
    const auto& str_meta = schema->operator[](FieldName("json"));
    auto column_info = test::GenColumnInfo(
        str_meta.get_id().get(), proto::schema::DataType::JSON, false, false);
    column_info->add_nested_path("str");
    auto unary_range_expr = test::GenUnaryRangeExpr(OpType::Match, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto segpromote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP);
    ASSERT_FALSE(final[0]);
    ASSERT_FALSE(final[1]);
    ASSERT_TRUE(final[2]);
    ASSERT_FALSE(final[3]);
    ASSERT_TRUE(final[4]);
}

struct MockStringIndex : index::StringIndexSort {
    const bool
    HasRawData() const override {
        return true;
    }

    bool
    SupportRegexQuery() const override {
        return false;
    }
};

class SealedSegmentRegexQueryTest : public ::testing::Test {
 public:
    void
    SetUp() override {
        schema = GenTestSchema();
        raw_str = {
            "b\n",
            "a\n",
            "aaa\n",
            "abbb\n",
            "abcabcabc\n",
        };
        raw_json = {
            R"({"int":1})",
            R"({"float":1.0})",
            R"({"str":"aaa"})",
            R"({"str":"bbb"})",
            R"({"str":"abcabcabc"})",
        };
        N = 5;
        uint64_t seed = 19190504;
        auto raw_data = DataGen(schema, N, seed);
        auto str_col = raw_data.raw_->mutable_fields_data()
                           ->at(0)
                           .mutable_scalars()
                           ->mutable_string_data()
                           ->mutable_data();
        auto int_col = raw_data.get_col<int64_t>(
            schema->get_field_id(FieldName("another_int64")));
        raw_int.assign(int_col.begin(), int_col.end());
        for (int64_t i = 0; i < N; i++) {
            str_col->at(i) = raw_str[i];
        }

        auto json_col = raw_data.raw_->mutable_fields_data()
                            ->at(2)
                            .mutable_scalars()
                            ->mutable_json_data()
                            ->mutable_data();
        for (int64_t i = 0; i < N; i++) {
            json_col->at(i) = raw_json[i];
        }

        seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
    }

    void
    TearDown() override {
    }

    void
    LoadStlSortIndex() {
        {
            proto::schema::StringArray arr;
            for (int64_t i = 0; i < N; i++) {
                *(arr.mutable_data()->Add()) = raw_str[i];
            }
            auto index = index::CreateStringIndexSort();
            std::vector<uint8_t> buffer(arr.ByteSize());
            ASSERT_TRUE(arr.SerializeToArray(buffer.data(), arr.ByteSize()));
            index->BuildWithRawDataForUT(arr.ByteSize(), buffer.data());
            LoadIndexInfo info{
                .field_id = schema->get_field_id(FieldName("str")).get(),
                .index_params = GenIndexParams(index.get()),
                .cache_index = CreateTestCacheIndex("test", std::move(index)),
            };
            seg->LoadIndex(info);
        }
        {
            auto index = index::CreateScalarIndexSort<int64_t>();
            index->BuildWithRawDataForUT(N, raw_int.data());
            LoadIndexInfo info{
                .field_id =
                    schema->get_field_id(FieldName("another_int64")).get(),
                .index_params = GenIndexParams(index.get()),
                .cache_index = CreateTestCacheIndex("test", std::move(index)),
            };
            seg->LoadIndex(info);
        }
    }

    void
    LoadInvertedIndex() {
        auto index =
            std::make_unique<index::InvertedIndexTantivy<std::string>>();
        index->BuildWithRawDataForUT(N, raw_str.data());
        LoadIndexInfo info{
            .field_id = schema->get_field_id(FieldName("str")).get(),
            .index_params = GenIndexParams(index.get()),
            .cache_index = CreateTestCacheIndex("test", std::move(index)),
        };
        seg->LoadIndex(info);
    }

    void
    LoadMockIndex() {
        proto::schema::StringArray arr;
        for (int64_t i = 0; i < N; i++) {
            *(arr.mutable_data()->Add()) = raw_str[i];
        }
        auto index = std::make_unique<MockStringIndex>();
        std::vector<uint8_t> buffer(arr.ByteSize());
        ASSERT_TRUE(arr.SerializeToArray(buffer.data(), arr.ByteSize()));
        index->BuildWithRawDataForUT(arr.ByteSize(), buffer.data());
        LoadIndexInfo info{
            .field_id = schema->get_field_id(FieldName("str")).get(),
            .index_params = GenIndexParams(index.get()),
            .cache_index = CreateTestCacheIndex("test", std::move(index)),
        };
        seg->LoadIndex(info);
    }

 public:
    SchemaPtr schema;
    SegmentSealedUPtr seg;
    int64_t N;
    std::vector<std::string> raw_str;
    std::vector<int64_t> raw_int;
    std::vector<std::string> raw_json;
};

TEST_F(SealedSegmentRegexQueryTest, BFRegexQueryOnNonStringField) {
    int64_t operand = 120;
    const auto& int_meta = schema->operator[](FieldName("another_int64"));
    auto column_info = test::GenColumnInfo(
        int_meta.get_id().get(), proto::schema::DataType::Int64, false, false);
    auto unary_range_expr = test::GenUnaryRangeExpr(OpType::Match, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    ASSERT_ANY_THROW(ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP));
}

TEST_F(SealedSegmentRegexQueryTest, BFRegexQueryOnStringField) {
    std::string operand = "a%";
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto unary_range_expr = test::GenUnaryRangeExpr(OpType::Match, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP);
    ASSERT_FALSE(final[0]);
    ASSERT_TRUE(final[1]);
    ASSERT_TRUE(final[2]);
    ASSERT_TRUE(final[3]);
    ASSERT_TRUE(final[4]);
}

TEST_F(SealedSegmentRegexQueryTest, BFRegexQueryOnJsonField) {
    std::string operand = "a%";
    const auto& str_meta = schema->operator[](FieldName("json"));
    auto column_info = test::GenColumnInfo(
        str_meta.get_id().get(), proto::schema::DataType::JSON, false, false);
    column_info->add_nested_path("str");
    auto unary_range_expr = test::GenUnaryRangeExpr(OpType::Match, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP);
    ASSERT_FALSE(final[0]);
    ASSERT_FALSE(final[1]);
    ASSERT_TRUE(final[2]);
    ASSERT_FALSE(final[3]);
    ASSERT_TRUE(final[4]);
}

TEST_F(SealedSegmentRegexQueryTest, RegexQueryOnIndexedNonStringField) {
    int64_t operand = 120;
    const auto& int_meta = schema->operator[](FieldName("another_int64"));
    auto column_info = test::GenColumnInfo(
        int_meta.get_id().get(), proto::schema::DataType::Int64, false, false);
    auto unary_range_expr = test::GenUnaryRangeExpr(OpType::Match, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    LoadStlSortIndex();

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*segpromote, MAX_TIMESTAMP);
    BitsetType final;
    ASSERT_ANY_THROW(ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP));
}

TEST_F(SealedSegmentRegexQueryTest, RegexQueryOnStlSortStringField) {
    std::string operand = "a%";
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto unary_range_expr = test::GenUnaryRangeExpr(OpType::Match, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    LoadStlSortIndex();

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP);
    ASSERT_FALSE(final[0]);
    ASSERT_TRUE(final[1]);
    ASSERT_TRUE(final[2]);
    ASSERT_TRUE(final[3]);
    ASSERT_TRUE(final[4]);
}

TEST_F(SealedSegmentRegexQueryTest, PrefixMatchOnInvertedIndexStringField) {
    std::string operand = "a";
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto unary_range_expr =
        test::GenUnaryRangeExpr(OpType::PrefixMatch, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    LoadInvertedIndex();

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP);
    ASSERT_FALSE(final[0]);
    ASSERT_TRUE(final[1]);
    ASSERT_TRUE(final[2]);
    ASSERT_TRUE(final[3]);
    ASSERT_TRUE(final[4]);
}

TEST_F(SealedSegmentRegexQueryTest, InnerMatchOnInvertedIndexStringField) {
    std::string operand = "a";
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto unary_range_expr =
        test::GenUnaryRangeExpr(OpType::InnerMatch, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    LoadInvertedIndex();

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP);
    ASSERT_FALSE(final[0]);
    ASSERT_TRUE(final[1]);
    ASSERT_TRUE(final[2]);
    ASSERT_TRUE(final[3]);
    ASSERT_TRUE(final[4]);
}

TEST_F(SealedSegmentRegexQueryTest, RegexQueryOnInvertedIndexStringField) {
    std::string operand = "a%";
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto unary_range_expr = test::GenUnaryRangeExpr(OpType::Match, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    LoadInvertedIndex();

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP);
    ASSERT_FALSE(final[0]);
    ASSERT_TRUE(final[1]);
    ASSERT_TRUE(final[2]);
    ASSERT_TRUE(final[3]);
    ASSERT_TRUE(final[4]);
}

TEST_F(SealedSegmentRegexQueryTest, PostfixMatchOnInvertedIndexStringField) {
    std::string operand = "a";
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto unary_range_expr =
        test::GenUnaryRangeExpr(OpType::PostfixMatch, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    LoadInvertedIndex();

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP);
    ASSERT_FALSE(final[0]);
    ASSERT_FALSE(final[1]);
    ASSERT_FALSE(final[2]);
    ASSERT_FALSE(final[3]);
    ASSERT_FALSE(final[4]);
}

TEST_F(SealedSegmentRegexQueryTest, RegexQueryOnUnsupportedIndex) {
    std::string operand = "a%";
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto unary_range_expr = test::GenUnaryRangeExpr(OpType::Match, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    LoadMockIndex();

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    BitsetType final;
    // regex query under this index will be executed using raw data (brute force).
    final = ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP);
    ASSERT_FALSE(final[0]);
    ASSERT_TRUE(final[1]);
    ASSERT_TRUE(final[2]);
    ASSERT_TRUE(final[3]);
    ASSERT_TRUE(final[4]);
}
