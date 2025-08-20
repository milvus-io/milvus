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

#include "common/Schema.h"
#include "test_utils/GenExprProto.h"
#include "query/PlanProto.h"
#include "query/ExecPlanNodeVisitor.h"
#include "expr/ITypeExpr.h"
#include "test_utils/storage_test_utils.h"
#include "index/IndexFactory.h"
#include "index/NgramInvertedIndex.h"
#include "segcore/load_index_c.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "expr/ITypeExpr.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::exec;

void
test_ngram_with_data(const boost::container::vector<std::string>& data,
                     const std::string& literal,
                     proto::plan::OpType op_type,
                     const std::vector<bool>& expected_result,
                     bool forward_to_br = false) {
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t index_build_id = 4000;
    int64_t index_version = 4000;
    int64_t index_id = 5000;

    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("ngram", DataType::VARCHAR);

    auto field_meta = milvus::segcore::gen_field_meta(collection_id,
                                                      partition_id,
                                                      segment_id,
                                                      field_id.get(),
                                                      DataType::VARCHAR,
                                                      DataType::NONE,
                                                      false);
    auto index_meta = gen_index_meta(
        segment_id, field_id.get(), index_build_id, index_version);

    std::string root_path = "/tmp/test-inverted-index/";
    auto storage_config = gen_local_storage_config(root_path);
    auto cm = CreateChunkManager(storage_config);

    size_t nb = data.size();

    auto field_data = storage::CreateFieldData(DataType::VARCHAR, false);
    field_data->FillFieldData(data.data(), data.size());

    auto segment = CreateSealedSegment(schema);
    auto field_data_info = PrepareSingleFieldInsertBinlog(collection_id,
                                                          partition_id,
                                                          segment_id,
                                                          field_id.get(),
                                                          {field_data},
                                                          cm);
    segment->LoadFieldData(field_data_info);

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

    auto cm_w = ChunkManagerWrapper(cm);
    cm_w.Write(log_path, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext ctx(field_meta, index_meta, cm);
    std::vector<std::string> index_files;

    {
        Config config;
        config[milvus::index::INDEX_TYPE] = milvus::index::INVERTED_INDEX_TYPE;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};

        auto ngram_params = index::NgramParams{
            .loading_index = false,
            .min_gram = 2,
            .max_gram = 4,
        };
        auto index =
            std::make_shared<index::NgramInvertedIndex>(ctx, ngram_params);
        index->Build(config);

        auto create_index_result = index->Upload();
        auto memSize = create_index_result->GetMemSize();
        auto serializedSize = create_index_result->GetSerializedSize();
        ASSERT_GT(memSize, 0);
        ASSERT_GT(serializedSize, 0);
        index_files = create_index_result->GetIndexFiles();
    }

    {
        Config config;
        config[milvus::index::INDEX_FILES] = index_files;
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;

        auto ngram_params = index::NgramParams{
            .loading_index = true,
            .min_gram = 2,
            .max_gram = 4,
        };
        auto index =
            std::make_unique<index::NgramInvertedIndex>(ctx, ngram_params);
        index->Load(milvus::tracer::TraceContext{}, config);

        auto cnt = index->Count();
        ASSERT_EQ(cnt, nb);

        exec::SegmentExpr segment_expr(std::move(std::vector<exec::ExprPtr>{}),
                                       "SegmentExpr",
                                       segment.get(),
                                       field_id,
                                       {},
                                       DataType::VARCHAR,
                                       nb,
                                       8192,
                                       0);

        std::optional<TargetBitmap> bitset_opt =
            index->ExecuteQuery(literal, op_type, &segment_expr);
        if (forward_to_br) {
            ASSERT_TRUE(!bitset_opt.has_value());
        } else {
            auto bitset = std::move(bitset_opt.value());
            for (size_t i = 0; i < nb; i++) {
                ASSERT_EQ(bitset[i], expected_result[i]);
            }
        }
    }

    {
        std::map<std::string, std::string> index_params{
            {milvus::index::INDEX_TYPE, milvus::index::NGRAM_INDEX_TYPE},
            {milvus::index::MIN_GRAM, "2"},
            {milvus::index::MAX_GRAM, "4"},
            {milvus::LOAD_PRIORITY, "HIGH"},
        };
        milvus::segcore::LoadIndexInfo load_index_info{
            .collection_id = collection_id,
            .partition_id = partition_id,
            .segment_id = segment_id,
            .field_id = field_id.get(),
            .field_type = DataType::VARCHAR,
            .enable_mmap = true,
            .mmap_dir_path = "/tmp/test-ngram-index-mmap-dir",
            .index_id = index_id,
            .index_build_id = index_build_id,
            .index_version = index_version,
            .index_params = index_params,
            .index_files = index_files,
            .schema = field_meta.field_schema,
            .index_size = 1024 * 1024 * 1024,
        };

        uint8_t trace_id[16] = {0};
        uint8_t span_id[8] = {0};
        trace_id[0] = 1;
        span_id[0] = 2;
        CTraceContext trace{
            .traceID = trace_id,
            .spanID = span_id,
            .traceFlags = 0,
        };
        auto cload_index_info = static_cast<CLoadIndexInfo>(&load_index_info);
        AppendIndexV2(trace, cload_index_info);
        UpdateSealedSegmentIndex(segment.get(), cload_index_info);

        auto unary_range_expr = test::GenUnaryRangeExpr(op_type, literal);
        auto column_info = test::GenColumnInfo(
            field_id.get(), proto::schema::DataType::VarChar, false, false);
        unary_range_expr->set_allocated_column_info(column_info);
        auto expr = test::GenExpr();
        expr->set_allocated_unary_range_expr(unary_range_expr);
        auto parser = ProtoParser(schema);
        auto typed_expr = parser.ParseExprs(*expr);
        auto parsed = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, typed_expr);
        BitsetType final;
        final = ExecuteQueryExpr(parsed, segment.get(), nb, MAX_TIMESTAMP);
        for (size_t i = 0; i < nb; i++) {
            ASSERT_EQ(final[i], expected_result[i]);
        }
    }
}

TEST(NgramIndex, TestNgramWikiEpisode) {
    boost::container::vector<std::string> data;
    data.push_back(
        "'Indira Davelba Murillo Alvarado (Tegucigalpa, "
        "the youngest of eight siblings. She attended primary school at the "
        "Escuela 14 de Julio, and her secondary studies at the Instituto "
        "school called \"Indi del Bosque\", where she taught the children of "
        "Honduran women'");
    data.push_back(
        "Richmond Green Secondary School is a public secondary school in "
        "Richmond Hill, Ontario, Canada.");
    data.push_back(
        "The Gymnasium in 2002 Gymnasium Philippinum or Philippinum High "
        "School is an almost 500-year-old secondary school in Marburg, Hesse, "
        "Germany.");
    data.push_back(
        "Sir Winston Churchill Secondary School is a Canadian secondary school "
        "located in St. Catharines, Ontario.");
    data.push_back("Sir Winston Churchill Secondary School");

    // within min-max_gram
    {
        // inner match
        std::vector<bool> expected_result{true, true, true, true, true};
        test_ngram_with_data(
            data, "ary", proto::plan::OpType::InnerMatch, expected_result);

        expected_result = {false, true, false, true, true};
        test_ngram_with_data(
            data, "y S", proto::plan::OpType::InnerMatch, expected_result);

        expected_result = {true, true, true, true, false};
        test_ngram_with_data(
            data, "y s", proto::plan::OpType::InnerMatch, expected_result);

        // prefix
        expected_result = {false, false, false, true, true};
        test_ngram_with_data(
            data, "Sir", proto::plan::OpType::PrefixMatch, expected_result);

        // postfix
        expected_result = {false, false, false, false, true};
        test_ngram_with_data(
            data, "ool", proto::plan::OpType::PostfixMatch, expected_result);

        // match
        expected_result = {true, false, false, false, false};
        test_ngram_with_data(
            data, "%Alv%y s%", proto::plan::OpType::Match, expected_result);
    }

    // exceeds max_gram
    {
        // inner match
        std::vector<bool> expected_result{false, true, true, true, false};
        test_ngram_with_data(data,
                             "secondary school",
                             proto::plan::OpType::InnerMatch,
                             expected_result);

        // prefix
        expected_result = {false, false, false, true, true};
        test_ngram_with_data(data,
                             "Sir Winston",
                             proto::plan::OpType::PrefixMatch,
                             expected_result);

        // postfix
        expected_result = {false, false, true, false, false};
        test_ngram_with_data(data,
                             "Germany.",
                             proto::plan::OpType::PostfixMatch,
                             expected_result);

        // match
        expected_result = {true, true, true, true, false};
        test_ngram_with_data(data,
                             "%secondary%school%",
                             proto::plan::OpType::Match,
                             expected_result);
    }
}

TEST(NgramIndex, TestNgramSimple) {
    boost::container::vector<std::string> data(10000,
                                               "elementary school secondary");

    // all can be hit by ngram tantivy but will be filterred out by the second phase
    test_ngram_with_data(data,
                         "secondary school",
                         proto::plan::OpType::InnerMatch,
                         std::vector<bool>(10000, false));

    test_ngram_with_data(data,
                         "ele",
                         proto::plan::OpType::PrefixMatch,
                         std::vector<bool>(10000, true));

    test_ngram_with_data(data,
                         "%ary%sec%",
                         proto::plan::OpType::Match,
                         std::vector<bool>(10000, true));

    // should be forwarded to brute force
    test_ngram_with_data(data,
                         "%ary%s%",
                         proto::plan::OpType::Match,
                         std::vector<bool>(10000, true),
                         true);

    test_ngram_with_data(data,
                         "ary",
                         proto::plan::OpType::PostfixMatch,
                         std::vector<bool>(10000, true));
}

TEST(NgramIndex, TestNgramJson) {
    std::vector<std::string> json_raw_data = {
        R"(1)",
        R"({"a": "Milvus project"})",
        R"({"a": "Zilliz cloud"})",
        R"({"a": "Query Node"})",
        R"({"a": "Data Node"})",
        R"({"a": [1, 2, 3]})",
        R"({"a": {"b": 1}})",
        R"({"a": 1001})",
        R"({"a": true})",
        R"({"a": "Milvus", "b": "Zilliz cloud"})",
    };

    auto json_path = "/a";
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();

    index::CreateIndexInfo create_index_info{
        .index_type = index::INVERTED_INDEX_TYPE,
        .json_cast_type = JsonCastType::FromString("VARCHAR"),
        .json_path = json_path,
        .ngram_params = std::optional<index::NgramParams>{index::NgramParams{
            .loading_index = false,
            .min_gram = 2,
            .max_gram = 3,
        }},
    };
    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        create_index_info, file_manager_ctx);

    auto ngram_index = std::unique_ptr<index::NgramInvertedIndex>(
        static_cast<index::NgramInvertedIndex*>(inv_index.release()));

    std::vector<milvus::Json> jsons;
    for (auto& json : json_raw_data) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    json_field->add_json_data(jsons);
    ngram_index->BuildWithFieldData({json_field});
    ngram_index->finish();
    ngram_index->create_reader(milvus::index::SetBitsetSealed);

    auto segment = segcore::CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.cache_index =
        CreateTestCacheIndex("", std::move(ngram_index));

    std::map<std::string, std::string> index_params{
        {milvus::index::INDEX_TYPE, milvus::index::NGRAM_INDEX_TYPE},
        {milvus::index::MIN_GRAM, "2"},
        {milvus::index::MAX_GRAM, "3"},
        {milvus::LOAD_PRIORITY, "HIGH"},
        {JSON_PATH, json_path},
        {JSON_CAST_TYPE, "VARCHAR"}};
    load_index_info.index_params = index_params;

    segment->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {json_field}, cm);
    segment->LoadFieldData(load_info);

    std::vector<std::tuple<proto::plan::GenericValue,
                           std::vector<int64_t>,
                           proto::plan::OpType>>
        test_cases;
    proto::plan::GenericValue value;
    value.set_string_val("nothing");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{}, proto::plan::OpType::InnerMatch));

    value.set_string_val("il");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{1, 2, 9}, proto::plan::OpType::InnerMatch));

    value.set_string_val("lliz");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{2}, proto::plan::OpType::InnerMatch));

    value.set_string_val("Zi");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{2}, proto::plan::OpType::PrefixMatch));

    value.set_string_val("Zilliz");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{2}, proto::plan::OpType::PrefixMatch));

    value.set_string_val("de");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{3, 4}, proto::plan::OpType::PostfixMatch));

    value.set_string_val("Node");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{3, 4}, proto::plan::OpType::PostfixMatch));

    value.set_string_val("%ery%ode%");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{3}, proto::plan::OpType::Match));

    for (auto& test_case : test_cases) {
        auto value = std::get<0>(test_case);
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"a"}, true),
            std::get<2>(test_case),
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);
        auto expect_result = std::get<1>(test_case);
        EXPECT_EQ(result.count(), expect_result.size());
        for (auto& id : expect_result) {
            EXPECT_TRUE(result[id]);
        }
    }
}