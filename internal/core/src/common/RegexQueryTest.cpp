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

#include <folly/FBVector.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <ratio>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "gtest/gtest.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "index/Meta.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "knowhere/comp/index_param.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanProto.h"
#include "query/Utils.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/TimestampIndex.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "segcore/test_utils/ConsumerIndexTestUtils.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/GenExprProto.h"

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
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
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

    auto parser = ProtoParser(schema);
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

    auto parser = ProtoParser(schema);
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

    auto parser = ProtoParser(schema);
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

// Keep the fallback fixture's original distinction: reverse lookup is
// available, while the installed reader has no pattern query capability.
class StringValueOnlyReader final
    : public index::IIndexReaderBase,
      public index::IScalarValueReader<std::string_view> {
 public:
    explicit StringValueOnlyReader(index::IIndexReaderBasePtr reader)
        : reader_(std::move(reader)),
          values_(
              dynamic_cast<const index::IScalarValueReader<std::string_view>*>(
                  reader_.get())) {
        AssertInfo(values_ != nullptr, "fallback fixture needs string values");
    }

    index::ReaderCaps
    Caps() const override {
        return {.value_lookup = true,
                .cheap_value_lookup = reader_->Caps().cheap_value_lookup};
    }

    index::Domain
    CoordDomain() const override {
        return reader_->CoordDomain();
    }
    int64_t
    Count() const override {
        return reader_->Count();
    }
    DataType
    ValueType() const override {
        return reader_->ValueType();
    }
    int64_t
    MemoryUsage() const override {
        return reader_->MemoryUsage();
    }
    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return reader_->CellByteSize();
    }

    std::optional<std::string>
    Lookup(int64_t offset) const override {
        return values_->Lookup(offset);
    }

    void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<void(int64_t, const std::string_view*, bool)>&
               output) const override {
        values_->Gather(offsets, count, output);
    }

 private:
    index::IIndexReaderBasePtr reader_;
    const index::IScalarValueReader<std::string_view>* values_;
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
        const auto field_id = schema->get_field_id(FieldName("another_int64"));
        test::expr_index::InstallIndex(
            *seg,
            field_id,
            DataType::INT64,
            test::consumer::BuildScalarReader<int64_t>(field_id,
                                                       DataType::INT64,
                                                       index::ASCENDING_SORT,
                                                       N,
                                                       raw_int.data()));
    }

    void
    LoadInvertedIndex() {
        const auto field_id = schema->get_field_id(FieldName("str"));
        test::expr_index::InstallIndex(
            *seg,
            field_id,
            DataType::VARCHAR,
            test::expr_index::BuildIndex(
                field_id,
                DataType::VARCHAR,
                index::INVERTED_INDEX_TYPE,
                {test::expr_index::StringField(raw_str)}));
    }

    void
    LoadMockIndex() {
        const auto field_id = schema->get_field_id(FieldName("str"));
        auto opened = test::expr_index::BuildIndex(
            field_id,
            DataType::VARCHAR,
            index::MARISA_TRIE,
            {test::expr_index::StringField(raw_str)});
        opened.reader =
            std::make_unique<StringValueOnlyReader>(std::move(opened.reader));
        opened.caps = opened.reader->Caps();
        test::expr_index::InstallIndex(
            *seg, field_id, DataType::VARCHAR, std::move(opened));
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

    auto parser = ProtoParser(schema);
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

    auto parser = ProtoParser(schema);
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

    auto parser = ProtoParser(schema);
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

    auto parser = ProtoParser(schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    LoadStlSortIndex();

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*segpromote, MAX_TIMESTAMP);
    BitsetType final;
    ASSERT_ANY_THROW(ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP));
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

    auto parser = ProtoParser(schema);
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

    auto parser = ProtoParser(schema);
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

    auto parser = ProtoParser(schema);
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

TEST_F(SealedSegmentRegexQueryTest,
       RegexQueryWithStartAnchorOnInvertedIndexStringField) {
    std::string operand = "^abbb";
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto unary_range_expr =
        test::GenUnaryRangeExpr(OpType::RegexMatch, operand);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(schema);
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
    ASSERT_TRUE(final[3]);
    ASSERT_FALSE(final[4]);
}

TEST_F(SealedSegmentRegexQueryTest,
       RegexQueryOnInvertedIndexUsesRe2PartialMatchSemantics) {
    auto run_regex = [&](const std::string& operand) {
        const auto& str_meta = schema->operator[](FieldName("str"));
        auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                               proto::schema::DataType::VarChar,
                                               false,
                                               false);
        auto unary_range_expr =
            test::GenUnaryRangeExpr(OpType::RegexMatch, operand);
        unary_range_expr->set_allocated_column_info(column_info);
        auto expr = test::GenExpr();
        expr->set_allocated_unary_range_expr(unary_range_expr);

        auto parser = ProtoParser(schema);
        auto typed_expr = parser.ParseExprs(*expr);
        auto parsed = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, typed_expr);

        auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
        return ExecuteQueryExpr(parsed, segpromote, N, MAX_TIMESTAMP);
    };

    LoadInvertedIndex();

    auto multiline_end = run_regex("(?m)aaa$");
    ASSERT_FALSE(multiline_end[0]);
    ASSERT_FALSE(multiline_end[1]);
    ASSERT_TRUE(multiline_end[2]);
    ASSERT_FALSE(multiline_end[3]);
    ASSERT_FALSE(multiline_end[4]);

    auto word_boundary = run_regex("\\baaa\\b");
    ASSERT_FALSE(word_boundary[0]);
    ASSERT_FALSE(word_boundary[1]);
    ASSERT_TRUE(word_boundary[2]);
    ASSERT_FALSE(word_boundary[3]);
    ASSERT_FALSE(word_boundary[4]);

    auto lazy_quantifier = run_regex("a+?\\n");
    ASSERT_FALSE(lazy_quantifier[0]);
    ASSERT_TRUE(lazy_quantifier[1]);
    ASSERT_TRUE(lazy_quantifier[2]);
    ASSERT_FALSE(lazy_quantifier[3]);
    ASSERT_FALSE(lazy_quantifier[4]);
}

TEST(InvertedIndexRegexQueryTest, RegexQueryUsesRe2CharacterClassSemantics) {
    std::vector<std::string> raw_str = {
        "123",
        "\xD9\xA3",
    };

    auto opened =
        test::expr_index::BuildIndex(FieldId(100),
                                     DataType::VARCHAR,
                                     index::INVERTED_INDEX_TYPE,
                                     {test::expr_index::StringField(raw_str)});
    const auto* patterns =
        dynamic_cast<const index::IPatternMatchReader*>(opened.reader.get());
    ASSERT_NE(patterns, nullptr);
    auto bitset =
        patterns->PatternMatch("^\\d+$", index::PatternOp::RegexMatch);
    ASSERT_TRUE(bitset[0]);
    ASSERT_FALSE(bitset[1]);
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

    auto parser = ProtoParser(schema);
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

    auto parser = ProtoParser(schema);
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
