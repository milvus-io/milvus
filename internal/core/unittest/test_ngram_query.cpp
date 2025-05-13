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
#include <string>
#include <random>
#include <boost/filesystem.hpp>
#include <boost/foreach.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/stream.hpp>
#include <unordered_set>
#include <jemalloc/jemalloc.h>

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
#include "indexbuilder/IndexFactory.h"
#include "index/IndexFactory.h"
#include "index/NgramInvertedIndex.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::exec;

TEST(ConvertToNgramLiteralTest, EmptyString) {
    auto result = parse_ngram_pattern("");
    ASSERT_FALSE(result.has_value());
}

TEST(ConvertToNgramLiteralTest, ExactMatchSimple) {
    auto result = parse_ngram_pattern("abc");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->literal, "abc");
    EXPECT_EQ(result->type, MatchType::ExactMatch);
}

TEST(ConvertToNgramLiteralTest, ExactMatchWithEscapedPercent) {
    auto result = parse_ngram_pattern("ab\\%cd");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->literal, "ab%cd");
    EXPECT_EQ(result->type, MatchType::ExactMatch);
}

TEST(ConvertToNgramLiteralTest, ExactMatchWithEscapedSpecialChar) {
    auto result = parse_ngram_pattern("a.b");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->literal, "a\\.b");
    EXPECT_EQ(result->type, MatchType::ExactMatch);
}

TEST(ConvertToNgramLiteralTest, PrefixMatchSimple) {
    auto result = parse_ngram_pattern("%abc");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->literal, "abc");
    EXPECT_EQ(result->type, MatchType::PrefixMatch);
}

TEST(ConvertToNgramLiteralTest, PostfixMatchSimple) {
    auto result = parse_ngram_pattern("abc%");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->literal, "abc");
    EXPECT_EQ(result->type, MatchType::PostfixMatch);
}

TEST(ConvertToNgramLiteralTest, InnerMatchSimple) {
    auto result = parse_ngram_pattern("%abc%");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->literal, "abc");
    EXPECT_EQ(result->type, MatchType::InnerMatch);
}

TEST(ConvertToNgramLiteralTest, MatchSinglePercentMiddle) {
    auto result = parse_ngram_pattern("a%b");
    ASSERT_FALSE(result.has_value());
}

TEST(ConvertToNgramLiteralTest, MatchTypeReturnsNullopt) {
    EXPECT_FALSE(parse_ngram_pattern("%").has_value());
    // %a%b (n=2, not %xxx%) -> Match -> nullopt
    EXPECT_FALSE(parse_ngram_pattern("%a%b").has_value());
    // a%b%c (n=2, not %xxx%) -> Match -> nullopt
    EXPECT_FALSE(parse_ngram_pattern("a%b%c").has_value());
    // %% (n=2, not %xxx% because length is not > 2) -> Match -> nullopt
    EXPECT_FALSE(parse_ngram_pattern("%%").has_value());
    // %a%b%c% (n=3) -> Match -> nullopt
    EXPECT_FALSE(parse_ngram_pattern("%a%b%c%").has_value());
}

TEST(ConvertToNgramLiteralTest, UnescapedUnderscoreReturnsNullopt) {
    EXPECT_FALSE(parse_ngram_pattern("a_b").has_value());
    EXPECT_FALSE(parse_ngram_pattern("%a_b").has_value());
    EXPECT_FALSE(parse_ngram_pattern("a_b%").has_value());
    EXPECT_FALSE(parse_ngram_pattern("%a_b%").has_value());
}

TEST(ConvertToNgramLiteralTest, EscapedUnderscore) {
    auto result = parse_ngram_pattern("a\\_b");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->literal, "a_b");
    EXPECT_EQ(result->type, MatchType::ExactMatch);
}

auto
generate_field_meta(int64_t collection_id = 1,
                    int64_t partition_id = 2,
                    int64_t segment_id = 3,
                    int64_t field_id = 101,
                    DataType data_type = DataType::NONE,
                    DataType element_type = DataType::NONE,
                    bool nullable = false) -> storage::FieldDataMeta {
    auto meta = storage::FieldDataMeta{
        .collection_id = collection_id,
        .partition_id = partition_id,
        .segment_id = segment_id,
        .field_id = field_id,
    };
    meta.field_schema.set_data_type(
        static_cast<proto::schema::DataType>(data_type));
    meta.field_schema.set_element_type(
        static_cast<proto::schema::DataType>(element_type));
    meta.field_schema.set_nullable(nullable);
    return meta;
}

auto
generate_index_meta(int64_t segment_id = 3,
                    int64_t field_id = 101,
                    int64_t index_build_id = 1000,
                    int64_t index_version = 10000) -> storage::IndexMeta {
    return storage::IndexMeta{
        .segment_id = segment_id,
        .field_id = field_id,
        .build_id = index_build_id,
        .index_version = index_version,
    };
}

auto
generate_local_storage_config(const std::string& root_path)
    -> storage::StorageConfig {
    auto ret = storage::StorageConfig{};
    ret.storage_type = "local";
    ret.root_path = root_path;
    return ret;
}

struct ChunkManagerWrapper2 {
    ChunkManagerWrapper2(storage::ChunkManagerPtr cm) : cm_(cm) {
    }

    ~ChunkManagerWrapper2() {
        for (const auto& file : written_) {
            cm_->Remove(file);
        }

        boost::filesystem::remove_all(cm_->GetRootPath());
    }

    void
    Write(const std::string& filepath, void* buf, uint64_t len) {
        written_.insert(filepath);
        cm_->Write(filepath, buf, len);
    }

    const storage::ChunkManagerPtr cm_;
    std::unordered_set<std::string> written_;
};

TEST(NgramIndex, TestPerfNgram) {
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t index_build_id = 4000;
    int64_t index_version = 4000;

    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("ngram", DataType::VARCHAR);

    auto field_meta = generate_field_meta(collection_id,
                                          partition_id,
                                          segment_id,
                                          field_id.get(),
                                          DataType::VARCHAR,
                                          DataType::NONE,
                                          false);
    auto index_meta = generate_index_meta(
        segment_id, field_id.get(), index_build_id, index_version);

    std::string root_path = "/tmp/test-inverted-index/";
    auto storage_config = generate_local_storage_config(root_path);
    auto cm = CreateChunkManager(storage_config);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(1, 100);

    size_t nb = 0;
    boost::container::vector<std::string> data;

    // read words from "/home/spadea/working4/wiki_words_selected.txt" where each line is a word
    boost::filesystem::path file_path(
        "/home/spadea/working4/wiki_words_selected.txt");
    boost::iostreams::stream<boost::iostreams::file_source> file(
        file_path.string());
    std::string word;
    while (std::getline(file, word)) {
        data.push_back(word);
        nb++;
        if (nb > 10000) {
            break;
        }
    }

    std::cout << "nb: " << nb << std::endl;

    auto field_data = storage::CreateFieldData(DataType::VARCHAR, false);
    field_data->FillFieldData(data.data(), data.size());

    auto segment = CreateSealedSegment(schema);
    auto arrow_data_wrapper =
        storage::ConvertFieldDataToArrowDataWrapper(field_data);
    auto field_data_info = FieldDataInfo{
        field_id.get(),
        nb,
        std::vector<std::shared_ptr<ArrowDataWrapper>>{arrow_data_wrapper}};
    segment->LoadFieldData(field_id, field_data_info);

    // std::cout << "length:" << field_data->get_num_rows() << std::endl;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    auto get_binlog_path = [=](int64_t log_id) {
        return fmt::format("{}/{}/{}/{}/{}",
                           collection_id,
                           partition_id,
                           segment_id,
                           field_id.get(),
                           log_id);
    };

    auto log_path = get_binlog_path(0);

    auto cm_w = ChunkManagerWrapper2(cm);
    cm_w.Write(log_path, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext ctx(field_meta, index_meta, cm);
    std::vector<std::string> index_files;

    {
        Config config;
        config["index_type"] = milvus::index::INVERTED_INDEX_TYPE;
        config["insert_files"] = std::vector<std::string>{log_path};

        auto index = std::make_shared<index::NgramInvertedIndex>(ctx, 2, 4);
        index->Build(config);

        auto create_index_result = index->Upload();
        auto memSize = create_index_result->GetMemSize();
        auto serializedSize = create_index_result->GetSerializedSize();
        ASSERT_GT(memSize, 0);
        ASSERT_GT(serializedSize, 0);
        index_files = create_index_result->GetIndexFiles();
    }

    {
        index::CreateIndexInfo index_info{};
        index_info.index_type = milvus::index::INVERTED_INDEX_TYPE;
        index_info.field_type = DataType::VARCHAR;

        Config config;
        config["index_files"] = index_files;
        config["min_gram"] = 2;
        config["max_gram"] = 4;

        ctx.set_for_loading_index(true);
        auto index = std::make_shared<index::NgramInvertedIndex>(ctx, 2, 4);
        index->Load(milvus::tracer::TraceContext{}, config);

        auto cnt = index->Count();
        ASSERT_EQ(cnt, nb);

        // case 1: match without further filter
        // boost::container::vector<std::string> test_data{
        //     "wa",
        //     "un",
        //     "cou",
        //     "the",
        //     "roc",
        //     "ras",
        //     "cou",
        //     "mig",
        //     "ough",
        // };

        // case 2: match with further filter
        boost::container::vector<std::string> test_data{
            "story", "ctivi", "atche", "itera"};

        auto now = std::chrono::high_resolution_clock::now();
        for (auto& literal : test_data) {
            exec::SegmentExpr segment_expr(
                std::move(std::vector<exec::ExprPtr>{}),
                "SegmentExpr",
                segment.get(),
                field_id,
                {},
                DataType::VARCHAR,
                nb,
                8192,
                0);

            auto bitset2 =
                index->InnerMatchQuery(literal, &segment_expr).value();
            std::cout << "literal: " << literal
                      << " matched count: " << bitset2.count() << std::endl;
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - now);
        std::cout << "Execution time: " << duration.count() / 1000.f << "ms"
                  << std::endl;

        for (auto& literal : test_data) {
            std::string operand = "%" + literal + "%";
            auto unary_range_expr =
                test::GenUnaryRangeExpr(OpType::Match, operand);
            auto column_info = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr->set_allocated_column_info(column_info);
            auto expr = test::GenExpr();
            expr->set_allocated_unary_range_expr(unary_range_expr);
            auto parser = ProtoParser(*schema);
            auto typed_expr = parser.ParseExprs(*expr);
            auto parsed = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, typed_expr);
            BitsetType final;
            final = ExecuteQueryExpr(parsed, segment.get(), nb, MAX_TIMESTAMP);
            std::cout << "literal: " << operand
                      << " matched count: " << final.count() << std::endl;
        }
    }
}

TEST(NgramIndex, TestPerfNormal) {
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t field_id = 101;
    int64_t index_build_id = 4000;
    int64_t index_version = 4000;

    auto field_meta = generate_field_meta(collection_id,
                                          partition_id,
                                          segment_id,
                                          field_id,
                                          DataType::STRING,
                                          DataType::NONE,
                                          false);
    auto index_meta = generate_index_meta(
        segment_id, field_id, index_build_id, index_version);

    std::string root_path = "/tmp/test-inverted-index/";
    auto storage_config = generate_local_storage_config(root_path);
    auto cm = CreateChunkManager(storage_config);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(1, 100);

    size_t nb = 0;
    boost::container::vector<std::string> data;

    // read words from "/home/spadea/working4/wiki_words_selected.txt" where each line is a word
    boost::filesystem::path file_path(
        "/home/spadea/working4/wiki_words_selected.txt");
    boost::iostreams::stream<boost::iostreams::file_source> file(
        file_path.string());
    std::string word;
    while (std::getline(file, word)) {
        data.push_back(word);
        nb++;
    }

    std::cout << "nb: " << nb << std::endl;

    auto field_data = storage::CreateFieldData(DataType::STRING, false);
    field_data->FillFieldData(data.data(), data.size());

    // std::cout << "length:" << field_data->get_num_rows() << std::endl;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    auto get_binlog_path = [=](int64_t log_id) {
        return fmt::format("{}/{}/{}/{}/{}",
                           collection_id,
                           partition_id,
                           segment_id,
                           field_id,
                           log_id);
    };

    auto log_path = get_binlog_path(0);

    auto cm_w = ChunkManagerWrapper2(cm);
    cm_w.Write(log_path, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext ctx(field_meta, index_meta, cm);
    std::vector<std::string> index_files;

    {
        Config config;
        config["index_type"] = milvus::index::INVERTED_INDEX_TYPE;
        config["insert_files"] = std::vector<std::string>{log_path};

        auto index = indexbuilder::IndexFactory::GetInstance().CreateIndex(
            DataType::STRING, config, ctx);
        index->Build();

        auto create_index_result = index->Upload();
        auto memSize = create_index_result->GetMemSize();
        auto serializedSize = create_index_result->GetSerializedSize();
        ASSERT_GT(memSize, 0);
        ASSERT_GT(serializedSize, 0);
        index_files = create_index_result->GetIndexFiles();
    }

    {
        index::CreateIndexInfo index_info{};
        index_info.index_type = milvus::index::INVERTED_INDEX_TYPE;
        index_info.field_type = DataType::STRING;

        Config config;
        config["index_files"] = index_files;

        ctx.set_for_loading_index(true);
        auto index =
            index::IndexFactory::GetInstance().CreateIndex(index_info, ctx);
        index->Load(milvus::tracer::TraceContext{}, config);

        using IndexType = index::ScalarIndex<std::string>;
        auto real_index = dynamic_cast<IndexType*>(index.get());

        auto cnt = index->Count();
        ASSERT_EQ(cnt, nb);

        // boost::container::vector<std::string> test_data{
        //     "%wa%",
        //     "%un%",
        //     "%cou%",
        //     "%the%",
        //     "%roc%",
        //     "%ras%",
        //     "%cou%",
        //     "%mig%",
        //     "%ough%",
        // };

        // case 2: match with further filter
        boost::container::vector<std::string> test_data{
            "%story%", "%ctivi%", "%atche%", "%itera%"};

        auto now = std::chrono::high_resolution_clock::now();
        for (auto& literal : test_data) {
            auto bitset2 = real_index->PatternMatch(literal);
            std::cout << "literal: " << literal
                      << " matched count: " << bitset2.count() << std::endl;
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - now);
        std::cout << "Execution time: " << duration.count() / 1000.f << "ms"
                  << std::endl;
    }
}