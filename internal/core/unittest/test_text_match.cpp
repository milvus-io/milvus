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
#include <optional>
#include <string>

#include "common/Schema.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "query/PlanProto.h"
#include "query/ExecPlanNodeVisitor.h"
#include "expr/ITypeExpr.h"
#include "segcore/segment_c.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {
SchemaPtr
GenTestSchema(std::map<std::string, std::string> params = {},
              bool nullable = false) {
    auto schema = std::make_shared<Schema>();
    {
        FieldMeta f(FieldName("pk"),
                    FieldId(100),
                    DataType::INT64,
                    false,
                    std::nullopt);
        schema->AddField(std::move(f));
        schema->set_primary_field_id(FieldId(100));
    }
    {
        FieldMeta f(FieldName("str"),
                    FieldId(101),
                    DataType::VARCHAR,
                    65536,
                    nullable,
                    true,
                    true,
                    params,
                    std::nullopt);
        schema->AddField(std::move(f));
    }
    {
        FieldMeta f(FieldName("fvec"),
                    FieldId(102),
                    DataType::VECTOR_FLOAT,
                    16,
                    knowhere::metric::L2,
                    false,
                    std::nullopt);
        schema->AddField(std::move(f));
    }
    return schema;
}
std::shared_ptr<milvus::plan::FilterBitsNode>
GetMatchExpr(SchemaPtr schema,
             const std::string& query,
             proto::plan::OpType op,
             int64_t slop = 0) {
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);

    auto unary_range_expr = test::GenUnaryRangeExpr(op, query);
    unary_range_expr->set_allocated_column_info(column_info);
    auto generic_for_slop = milvus::test::GenGenericValue(slop);
    unary_range_expr->add_extra_values()->CopyFrom(*generic_for_slop);
    delete generic_for_slop;
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(*schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);
    return parsed;
};

std::shared_ptr<milvus::plan::FilterBitsNode>
GetNotMatchExpr(SchemaPtr schema,
                const std::string& query,
                proto::plan::OpType op,
                int64_t slop = 0) {
    const auto& str_meta = schema->operator[](FieldName("str"));
    proto::plan::GenericValue val;
    val.set_string_val(query);
    std::vector<proto::plan::GenericValue> extra_values;
    auto generic_for_slop = milvus::test::GenGenericValue(slop);
    extra_values.push_back(*generic_for_slop);
    delete generic_for_slop;
    auto child_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        milvus::expr::ColumnInfo(str_meta.get_id(), DataType::VARCHAR),
        op,
        val,
        extra_values);
    auto expr = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    return parsed;
};
}  // namespace

TEST(ParseJson, Naive) {
    {
        std::string s(R"({"tokenizer": "jieba"})");
        nlohmann::json j = nlohmann::json::parse(s);
        auto m = j.get<std::map<std::string, std::string>>();
        for (const auto& [k, v] : m) {
            std::cout << k << ": " << v << std::endl;
        }
    }

    {
        std::string s(
            R"({"analyzer":"stop","stop_words":["an","the"],"case_insensitive":false})");
        nlohmann::json j = nlohmann::json::parse(s);
        for (const auto& [key, value] : j.items()) {
            std::cout << key << ": " << value.dump() << std::endl;
        }
    }
}

TEST(ParseTokenizerParams, NoTokenizerParams) {
    TypeParams params{{"k", "v"}};
    auto p = ParseTokenizerParams(params);
    ASSERT_EQ("{}", std::string(p));
}

TEST(ParseTokenizerParams, Default) {
    TypeParams params{{"analyzer_params", R"({"tokenizer": "standard"})"}};
    auto p = ParseTokenizerParams(params);
    ASSERT_EQ(params.at("analyzer_params"), p);
}

TEST(TextMatch, Index) {
    using Index = index::TextMatchIndex;
    auto index = std::make_unique<Index>(std::numeric_limits<int64_t>::max(),
                                         "unique_id",
                                         "milvus_tokenizer",
                                         "{}");
    index->CreateReader();
    index->AddText("football, basketball, pingpang", true, 0);
    index->AddText("", false, 1);
    index->AddText("swimming, football", true, 2);
    index->Commit();
    index->Reload();

    {
        auto res = index->MatchQuery("football");
        ASSERT_EQ(res.size(), 3);
        ASSERT_TRUE(res[0]);
        ASSERT_FALSE(res[1]);
        ASSERT_TRUE(res[2]);
        auto res1 = index->IsNull();
        ASSERT_FALSE(res1[0]);
        ASSERT_TRUE(res1[1]);
        ASSERT_FALSE(res1[2]);
        auto res2 = index->IsNotNull();
        ASSERT_TRUE(res2[0]);
        ASSERT_FALSE(res2[1]);
        ASSERT_TRUE(res2[2]);
        res = index->MatchQuery("nothing");
        ASSERT_EQ(res.size(), 3);
        ASSERT_FALSE(res[0]);
        ASSERT_FALSE(res[1]);
        ASSERT_FALSE(res[2]);
    }

    {
        auto res = index->PhraseMatchQuery("football", 0);
        ASSERT_EQ(res.size(), 3);
        ASSERT_TRUE(res[0]);
        ASSERT_FALSE(res[1]);
        ASSERT_TRUE(res[2]);

        auto res1 = index->PhraseMatchQuery("swimming football", 0);
        ASSERT_EQ(res1.size(), 3);
        ASSERT_FALSE(res1[0]);
        ASSERT_FALSE(res1[1]);
        ASSERT_TRUE(res1[2]);

        auto res2 = index->PhraseMatchQuery("football swimming", 0);
        ASSERT_EQ(res2.size(), 3);
        ASSERT_FALSE(res2[0]);
        ASSERT_FALSE(res2[1]);
        ASSERT_FALSE(res2[2]);

        auto res3 = index->PhraseMatchQuery("football swimming", 1);
        ASSERT_EQ(res3.size(), 3);
        ASSERT_FALSE(res3[0]);
        ASSERT_FALSE(res3[1]);
        ASSERT_FALSE(res3[2]);

        auto res4 = index->PhraseMatchQuery("football swimming", 2);
        ASSERT_EQ(res4.size(), 3);
        ASSERT_FALSE(res4[0]);
        ASSERT_FALSE(res4[1]);
        ASSERT_TRUE(res4[2]);
    }
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

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        auto expr =
            GetMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        BitsetType final;
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        auto expr1 =
            GetNotMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);

        auto expr2 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr2, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        auto expr3 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch, 1);
        final = ExecuteQueryExpr(expr3, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_FALSE(final[1]);
    }
}

TEST(TextMatch, GrowingNaiveNullable) {
    auto schema = GenTestSchema({}, true);
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {
        "football, basketball, pingpang", "swimming, football", ""};
    std::vector<bool> raw_str_valid = {true, true, false};

    int64_t N = 3;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    auto str_col_valid =
        raw_data.raw_->mutable_fields_data()->at(1).mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = raw_str_valid[i];
    }

    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);

    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        auto expr =
            GetMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        BitsetType final;
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_FALSE(final[2]);
        auto expr1 =
            GetNotMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);

        auto expr2 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr2, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);
        auto expr3 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch, 1);
        final = ExecuteQueryExpr(expr3, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);
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

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        auto expr =
            GetMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        BitsetType final;
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        auto expr1 =
            GetNotMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);

        auto expr2 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr2, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        auto expr3 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch, 1);
        final = ExecuteQueryExpr(expr3, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_FALSE(final[1]);
    }
}

TEST(TextMatch, SealedNaiveNullable) {
    auto schema = GenTestSchema({}, true);
    auto seg = CreateSealedSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {
        "football, basketball, pingpang", "swimming, football", ""};
    std::vector<bool> raw_str_valid = {true, true, false};

    int64_t N = 3;
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
    auto str_col_valid =
        raw_data.raw_->mutable_fields_data()->at(1).mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = raw_str_valid[i];
    }

    SealedLoadFieldData(raw_data, *seg);
    seg->CreateTextIndex(FieldId(101));
    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        auto expr =
            GetMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        BitsetType final;
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_FALSE(final[2]);
        auto expr1 =
            GetNotMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);

        auto expr2 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr2, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);
        auto expr3 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch, 1);
        final = ExecuteQueryExpr(expr3, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);
    }
}

TEST(TextMatch, GrowingJieBa) {
    auto schema = GenTestSchema({
        {"enable_match", "true"},
        {"enable_analyzer", "true"},
        {"analyzer_params", R"({"tokenizer": "jieba"})"},
    });
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {"青铜时代", "黄金时代"};

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

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        auto expr = GetMatchExpr(schema, "黄金时代", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
    }
}

TEST(TextMatch, GrowingJieBaNullable) {
    auto schema = GenTestSchema(
        {
            {"enable_match", "true"},
            {"enable_tokenizer", "true"},
            {"analyzer_params", R"({"tokenizer": "jieba"})"},
        },
        true);
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {"青铜时代", "黄金时代", ""};
    std::vector<bool> raw_str_valid = {true, true, false};

    int64_t N = 3;
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
    auto str_col_valid =
        raw_data.raw_->mutable_fields_data()->at(1).mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = raw_str_valid[i];
    }

    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);

    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        auto expr = GetMatchExpr(schema, "黄金时代", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_FALSE(final[2]);
    }
}

TEST(TextMatch, SealedJieBa) {
    auto schema = GenTestSchema({
        {"enable_match", "true"},
        {"enable_analyzer", "true"},
        {"analyzer_params", R"({"tokenizer": "jieba"})"},
    });
    auto seg = CreateSealedSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {"青铜时代", "黄金时代"};

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

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        auto expr = GetMatchExpr(schema, "黄金时代", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
    }
}

TEST(TextMatch, SealedJieBaNullable) {
    auto schema = GenTestSchema(
        {
            {"enable_match", "true"},
            {"enable_tokenizer", "true"},
            {"analyzer_params", R"({"tokenizer": "jieba"})"},
        },
        true);
    auto seg = CreateSealedSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {"青铜时代", "黄金时代", ""};
    std::vector<bool> raw_str_valid = {true, true, false};

    int64_t N = 3;
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
    auto str_col_valid =
        raw_data.raw_->mutable_fields_data()->at(1).mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = raw_str_valid[i];
    }

    SealedLoadFieldData(raw_data, *seg);
    seg->CreateTextIndex(FieldId(101));

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        auto expr = GetMatchExpr(schema, "黄金时代", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_FALSE(final[2]);
    }
}

// Test that growing segment loading flushed binlogs will build text match index.
TEST(TextMatch, GrowingLoadData) {
    int64_t N = 7;
    auto schema = GenTestSchema({}, true);
    schema->AddField(
        FieldName("RowID"), FieldId(0), DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     FieldId(1),
                     DataType::INT64,
                     false,
                     std::nullopt);
    std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                        "swimming, football",
                                        "golf",
                                        "",
                                        "baseball",
                                        "kungfu, football",
                                        ""};
    auto raw_data = DataGen(schema, N);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }
    auto str_col_valid =
        raw_data.raw_->mutable_fields_data()->at(1).mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = true;
    }
    // so we cannot match the second row
    str_col_valid->at(1) = false;

    auto storage_config = get_default_local_storage_config();
    auto cm = storage::CreateChunkManager(storage_config);
    auto load_info = PrepareInsertBinlog(
        1,
        2,
        3,
        storage_config.root_path + "/" + "test_growing_segment_load_data",
        raw_data,
        cm);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto status = LoadFieldData(segment.get(), &load_info);
    ASSERT_EQ(status.error_code, Success);
    ASSERT_EQ(segment->get_real_count(), N);
    ASSERT_NE(segment->get_field_avg_size(FieldId(101)), 0);

    // Check whether the text index has been built.
    auto expr = GetMatchExpr(schema, "football", OpType::TextMatch);
    BitsetType final;
    final = ExecuteQueryExpr(expr, segment.get(), N, MAX_TIMESTAMP);
    ASSERT_EQ(final.size(), N);
    ASSERT_TRUE(final[0]);
    ASSERT_FALSE(final[1]);
    ASSERT_FALSE(final[2]);
    ASSERT_FALSE(final[3]);
    ASSERT_FALSE(final[4]);
    ASSERT_TRUE(final[5]);
    ASSERT_FALSE(final[6]);
}

TEST(TextMatch, ConcurrentReadWriteWithNull) {
    auto schema = GenTestSchema({}, true);
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int64_t N = 1000;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col_valid =
        raw_data.raw_->mutable_fields_data()->at(1).mutable_valid_data();
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N - 1; i++) {
        str_col->at(i) = "";
    }
    str_col->at(N - 1) = "football";
    for (int64_t i = 0; i < N - 1; i++) {
        str_col_valid->at(i) = false;
    }

    std::thread writer([&seg, &raw_data, N]() {
        seg->PreInsert(N);
        seg->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    });

    std::thread reader([&seg, N]() {
        auto start = std::chrono::high_resolution_clock::now();
        ;
        const std::chrono::seconds timeout_duration{2};
        while (true) {
            if (start - std::chrono::high_resolution_clock::now() >
                timeout_duration) {
                ASSERT_TRUE(false)
                    << "Failed to get valid results within timeout";
                break;
            }
            BitsetType final;
            auto expr =
                GetMatchExpr(GenTestSchema(), "football", OpType::TextMatch);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            if (final.size() != N || !final[N - 1]) {
                continue;
            }
            for (int64_t i = 0; i < N - 1; i++) {
                ASSERT_FALSE(final[i]);
            }
            ASSERT_TRUE(final[N - 1]);
            break;
        }
    });

    writer.join();
    reader.join();
}
