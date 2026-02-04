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

#include <boost/container/vector.hpp>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <simdjson.h>
#include <stddef.h>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "bitset/bitset.h"
#include "bitset/detail/element_vectorized.h"
#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/Schema.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "exec/expression/Expr.h"
#include "exec/expression/function/FunctionFactory.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/IndexStats.h"
#include "index/Meta.h"
#include "index/NgramInvertedIndex.h"
#include "common/RegexQuery.h"
#include "index/Utils.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanProto.h"
#include "query/Utils.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "segcore/load_index_c.h"
#include "segcore/segment_c.h"
#include "simdjson/padded_string.h"
#include "storage/FileManager.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

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
    auto fs = storage::InitArrowFileSystem(storage_config);

    size_t nb = data.size();

    auto field_data =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, false);
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

    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    std::vector<std::string> index_files;

    auto index_size = 0;
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
        index_size = create_index_result->GetSerializedSize();
        ASSERT_GT(memSize, 0);
        ASSERT_GT(index_size, 0);
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
                                       nullptr,
                                       segment.get(),
                                       field_id,
                                       {},
                                       DataType::VARCHAR,
                                       nb,
                                       8192,
                                       0);
        if (op_type != proto::plan::OpType::Equal) {
            std::optional<TargetBitmap> bitset_opt =
                index->ExecuteQueryForUT(literal, op_type, &segment_expr);
            if (forward_to_br) {
                ASSERT_TRUE(!bitset_opt.has_value());
            } else {
                auto bitset = std::move(bitset_opt.value());
                for (size_t i = 0; i < nb; i++) {
                    ASSERT_EQ(bitset[i], expected_result[i]);
                }
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
        milvus::segcore::LoadIndexInfo load_index_info{};
        load_index_info.collection_id = collection_id;
        load_index_info.partition_id = partition_id;
        load_index_info.segment_id = segment_id;
        load_index_info.field_id = field_id.get();
        load_index_info.field_type = DataType::VARCHAR;
        load_index_info.enable_mmap = true;
        load_index_info.mmap_dir_path = "/tmp/test-ngram-index-mmap-dir";
        load_index_info.index_id = index_id;
        load_index_info.index_build_id = index_build_id;
        load_index_info.index_version = index_version;
        load_index_info.index_params = index_params;
        load_index_info.index_files = index_files;
        load_index_info.schema = field_meta.field_schema;
        load_index_info.index_size = index_size;

        uint8_t trace_id[16] = {0};
        uint8_t span_id[8] = {0};
        trace_id[0] = 1;
        span_id[0] = 2;
        CTraceContext trace{};
        trace.traceID = trace_id;
        trace.spanID = span_id;
        trace.traceFlags = 0;
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
            if (final[i] != expected_result[i]) {
                std::cout << "final[" << i << "] = " << final[i]
                          << ", expected_result[" << i
                          << "] = " << expected_result[i] << std::endl;
            }
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
        // equal, all should fail
        std::vector<bool> expected_result{false, false, false, false, false};
        test_ngram_with_data(
            data, "ary", proto::plan::OpType::Equal, expected_result);

        // inner match
        expected_result = {true, true, true, true, true};
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

// Test that ngram index should only be used for like operations
// (Match, InnerMatch, PrefixMatch, PostfixMatch)
// and NOT for other operations (Equal, NotEqual, In, NotIn, etc.)
// Issue: https://github.com/milvus-io/milvus/issues/44020
TEST(NgramIndex, TestNonLikeExpressionsWithNgram) {
    boost::container::vector<std::string> data = {"apple",
                                                  "banana",
                                                  "cherry",
                                                  "date",
                                                  "elderberry",
                                                  "fig",
                                                  "grape",
                                                  "honeydew",
                                                  "kiwi",
                                                  "lemon"};

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
    auto fs = storage::InitArrowFileSystem(storage_config);

    size_t nb = data.size();

    auto field_data =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, false);
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

    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    std::vector<std::string> index_files;

    // Build ngram index
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
        index_files = create_index_result->GetIndexFiles();
    }

    // Load index and test
    {
        std::map<std::string, std::string> index_params{
            {milvus::index::INDEX_TYPE, milvus::index::NGRAM_INDEX_TYPE},
            {milvus::index::MIN_GRAM, "2"},
            {milvus::index::MAX_GRAM, "4"},
            {milvus::LOAD_PRIORITY, "HIGH"},
        };
        milvus::segcore::LoadIndexInfo load_index_info{};
        load_index_info.collection_id = collection_id;
        load_index_info.partition_id = partition_id;
        load_index_info.segment_id = segment_id;
        load_index_info.field_id = field_id.get();
        load_index_info.field_type = DataType::VARCHAR;
        load_index_info.enable_mmap = true;
        load_index_info.mmap_dir_path = "/tmp/test-ngram-index-mmap-dir";
        load_index_info.index_id = index_id;
        load_index_info.index_build_id = index_build_id;
        load_index_info.index_version = index_version;
        load_index_info.index_params = index_params;
        load_index_info.index_files = index_files;
        load_index_info.schema = field_meta.field_schema;
        load_index_info.index_size = 1024 * 1024 * 1024;

        uint8_t trace_id[16] = {0};
        uint8_t span_id[8] = {0};
        trace_id[0] = 1;
        span_id[0] = 2;
        CTraceContext trace{};
        trace.traceID = trace_id;
        trace.spanID = span_id;
        trace.traceFlags = 0;
        auto cload_index_info = static_cast<CLoadIndexInfo>(&load_index_info);
        AppendIndexV2(trace, cload_index_info);
        UpdateSealedSegmentIndex(segment.get(), cload_index_info);

        // Test: TermFilterExpr (IN operator)
        {
            std::vector<proto::plan::GenericValue> values;
            proto::plan::GenericValue val1;
            val1.set_string_val("apple");
            values.push_back(val1);
            proto::plan::GenericValue val2;
            val2.set_string_val("banana");
            values.push_back(val2);
            proto::plan::GenericValue val3;
            val3.set_string_val("cherry");
            values.push_back(val3);

            auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
                milvus::expr::ColumnInfo(field_id, DataType::VARCHAR), values);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, term_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // Only apple, banana, cherry should match
            for (size_t i = 0; i < nb; i++) {
                if (i < 3) {
                    ASSERT_TRUE(final[i]) << "Expected true at index " << i;
                } else {
                    ASSERT_FALSE(final[i]) << "Expected false at index " << i;
                }
            }
        }

        // Test: UnaryRangeExpr with Equal operator
        {
            auto unary_range_expr =
                test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "apple");
            auto column_info = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr->set_allocated_column_info(column_info);
            auto expr = test::GenExpr();
            expr->set_allocated_unary_range_expr(unary_range_expr);
            auto parser = ProtoParser(schema);
            auto typed_expr = parser.ParseExprs(*expr);
            auto parsed = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, typed_expr);
            BitsetType final =
                ExecuteQueryExpr(parsed, segment.get(), nb, MAX_TIMESTAMP);
            // Only apple should match (exact match)
            for (size_t i = 0; i < nb; i++) {
                if (i == 0) {
                    ASSERT_TRUE(final[i]) << "Expected true at index " << i;
                } else {
                    ASSERT_FALSE(final[i]) << "Expected false at index " << i;
                }
            }
        }

        // Test: BinaryRangeFilterExpr
        {
            proto::plan::GenericValue lower_val;
            lower_val.set_string_val("cherry");
            proto::plan::GenericValue upper_val;
            upper_val.set_string_val("grape");

            auto binary_range_expr =
                std::make_shared<milvus::expr::BinaryRangeFilterExpr>(
                    milvus::expr::ColumnInfo(field_id, DataType::VARCHAR),
                    lower_val,
                    upper_val,
                    true,
                    true);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, binary_range_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // Strings between "cherry" and "grape" inclusive: cherry, date, elderberry, fig, grape
            for (size_t i = 0; i < nb; i++) {
                if (i >= 2 && i <= 6) {
                    ASSERT_TRUE(final[i]) << "Expected true at index " << i;
                } else {
                    ASSERT_FALSE(final[i]) << "Expected false at index " << i;
                }
            }
        }

        // Test: LogicalBinaryExpr with AND
        {
            // Create Equal expression
            auto unary_range_expr1 =
                test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "apple");
            auto column_info1 = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr1->set_allocated_column_info(column_info1);
            auto expr1 = test::GenExpr();
            expr1->set_allocated_unary_range_expr(unary_range_expr1);
            auto parser1 = ProtoParser(schema);
            auto typed_expr1 = parser1.ParseExprs(*expr1);

            // Create NotEqual expression
            auto unary_range_expr2 = test::GenUnaryRangeExpr(
                proto::plan::OpType::NotEqual, "banana");
            auto column_info2 = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr2->set_allocated_column_info(column_info2);
            auto expr2 = test::GenExpr();
            expr2->set_allocated_unary_range_expr(unary_range_expr2);
            auto parser2 = ProtoParser(schema);
            auto typed_expr2 = parser2.ParseExprs(*expr2);

            // Create LogicalBinaryExpr with AND
            auto logical_and_expr =
                std::make_shared<milvus::expr::LogicalBinaryExpr>(
                    milvus::expr::LogicalBinaryExpr::OpType::And,
                    typed_expr1,
                    typed_expr2);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, logical_and_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // Only apple should match (apple == "apple" AND apple != "banana")
            for (size_t i = 0; i < nb; i++) {
                if (i == 0) {
                    ASSERT_TRUE(final[i]) << "Expected true at index " << i;
                } else {
                    ASSERT_FALSE(final[i]) << "Expected false at index " << i;
                }
            }
        }

        // Test: LogicalUnaryExpr with NOT
        {
            // Create Equal expression
            auto unary_range_expr =
                test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "apple");
            auto column_info = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr->set_allocated_column_info(column_info);
            auto expr = test::GenExpr();
            expr->set_allocated_unary_range_expr(unary_range_expr);
            auto parser = ProtoParser(schema);
            auto typed_expr = parser.ParseExprs(*expr);

            // Create LogicalUnaryExpr with NOT
            auto logical_not_expr =
                std::make_shared<milvus::expr::LogicalUnaryExpr>(
                    milvus::expr::LogicalUnaryExpr::OpType::LogicalNot,
                    typed_expr);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, logical_not_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // All except apple should match (NOT (field == "apple"))
            for (size_t i = 0; i < nb; i++) {
                if (i != 0) {
                    ASSERT_TRUE(final[i]) << "Expected true at index " << i;
                } else {
                    ASSERT_FALSE(final[i]) << "Expected false at index " << i;
                }
            }
        }

        // Test: LogicalBinaryExpr with OR
        {
            // Create Equal expression
            auto unary_range_expr1 =
                test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "apple");
            auto column_info1 = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr1->set_allocated_column_info(column_info1);
            auto expr1 = test::GenExpr();
            expr1->set_allocated_unary_range_expr(unary_range_expr1);
            auto parser1 = ProtoParser(schema);
            auto typed_expr1 = parser1.ParseExprs(*expr1);

            // Create Equal expression for "banana"
            auto unary_range_expr2 =
                test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "banana");
            auto column_info2 = test::GenColumnInfo(
                field_id.get(), proto::schema::DataType::VarChar, false, false);
            unary_range_expr2->set_allocated_column_info(column_info2);
            auto expr2 = test::GenExpr();
            expr2->set_allocated_unary_range_expr(unary_range_expr2);
            auto parser2 = ProtoParser(schema);
            auto typed_expr2 = parser2.ParseExprs(*expr2);

            // Create LogicalBinaryExpr with OR
            auto logical_or_expr =
                std::make_shared<milvus::expr::LogicalBinaryExpr>(
                    milvus::expr::LogicalBinaryExpr::OpType::Or,
                    typed_expr1,
                    typed_expr2);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, logical_or_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // Apple and banana should match (apple == "apple" OR field == "banana")
            for (size_t i = 0; i < nb; i++) {
                if (i == 0 || i == 1) {
                    ASSERT_TRUE(final[i]) << "Expected true at index " << i;
                } else {
                    ASSERT_FALSE(final[i]) << "Expected false at index " << i;
                }
            }
        }

        // Test: NullExpr with IS_NULL
        {
            auto null_expr = std::make_shared<milvus::expr::NullExpr>(
                milvus::expr::ColumnInfo(field_id, DataType::VARCHAR),
                proto::plan::NullExpr_NullOp_IsNull);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, null_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // None should match since we have no null values
            for (size_t i = 0; i < nb; i++) {
                ASSERT_FALSE(final[i]) << "Expected false at index " << i;
            }
        }

        // Test: NullExpr with IS_NOT_NULL
        {
            auto null_expr = std::make_shared<milvus::expr::NullExpr>(
                milvus::expr::ColumnInfo(field_id, DataType::VARCHAR),
                proto::plan::NullExpr_NullOp_IsNotNull);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, null_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // All should match since we have no null values
            for (size_t i = 0; i < nb; i++) {
                ASSERT_TRUE(final[i]) << "Expected true at index " << i;
            }
        }

        // // Test: ExistsExpr
        // {
        //     auto exists_expr = std::make_shared<milvus::expr::ExistsExpr>(
        //         milvus::expr::ColumnInfo(field_id, DataType::VARCHAR));
        //     auto plan = std::make_shared<plan::FilterBitsNode>(
        //         DEFAULT_PLANNODE_ID, exists_expr);

        //     BitsetType final = ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
        //     // All should match since the field exists for all rows
        //     for (size_t i = 0; i < nb; i++) {
        //         ASSERT_TRUE(final[i]) << "Expected true at index " << i;
        //     }
        // }

        // Test: AlwaysTrueExpr
        {
            auto always_true_expr =
                std::make_shared<milvus::expr::AlwaysTrueExpr>();
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, always_true_expr);

            BitsetType final =
                ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
            // All should match
            for (size_t i = 0; i < nb; i++) {
                ASSERT_TRUE(final[i]) << "Expected true at index " << i;
            }
        }
    }
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
    value.set_string_val("liz");
    test_cases.push_back(std::make_tuple(
        value, std::vector<int64_t>{}, proto::plan::OpType::Equal));

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

// Test that ngram index should only be used for like operations on JSON fields
// and NOT for other operations (Equal, NotEqual, In, etc.)
TEST(NgramIndex, TestJsonNonLikeExpressionsWithNgram) {
    std::vector<std::string> json_raw_data = {R"({"name": "apple"})",
                                              R"({"name": "banana"})",
                                              R"({"name": "cherry"})",
                                              R"({"name": "date"})",
                                              R"({"name": "elderberry"})",
                                              R"({"name": "fig"})",
                                              R"({"name": "grape"})",
                                              R"({"name": "honeydew"})",
                                              R"({"name": "kiwi"})",
                                              R"({"name": "lemon"})"};

    auto json_path = "/name";
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
            .max_gram = 4,
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
        {milvus::index::MAX_GRAM, "4"},
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

    size_t nb = json_raw_data.size();

    // Test: JSON Equal operation
    {
        proto::plan::GenericValue value;
        value.set_string_val("apple");
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::Equal,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Only first record should match (exact match for "apple")
        EXPECT_EQ(result.count(), 1);
        EXPECT_TRUE(result[0]);
    }

    // Test: JSON NotEqual operation
    {
        proto::plan::GenericValue value;
        value.set_string_val("apple");
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::NotEqual,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // All except first record should match
        EXPECT_EQ(result.count(), 9);
        EXPECT_FALSE(result[0]);
        for (size_t i = 1; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON GreaterThan operation
    {
        proto::plan::GenericValue value;
        value.set_string_val("fig");
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::GreaterThan,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Records with names > "fig": grape, honeydew, kiwi, lemon
        EXPECT_EQ(result.count(), 4);
        for (size_t i = 6; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON LessThan operation
    {
        proto::plan::GenericValue value;
        value.set_string_val("date");
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::LessThan,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Records with names < "date": apple, banana, cherry
        EXPECT_EQ(result.count(), 3);
        for (size_t i = 0; i < 3; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON TermFilterExpr (IN operation)
    {
        std::vector<proto::plan::GenericValue> values;
        proto::plan::GenericValue val1, val2, val3;
        val1.set_string_val("apple");
        val2.set_string_val("cherry");
        val3.set_string_val("grape");
        values.push_back(val1);
        values.push_back(val2);
        values.push_back(val3);

        auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            values);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           term_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Only apple, cherry, grape should match
        EXPECT_EQ(result.count(), 3);
        EXPECT_TRUE(result[0]);  // apple
        EXPECT_TRUE(result[2]);  // cherry
        EXPECT_TRUE(result[6]);  // grape
    }

    // Test: JSON BinaryRangeFilterExpr
    {
        proto::plan::GenericValue lower_val;
        lower_val.set_string_val("cherry");
        proto::plan::GenericValue upper_val;
        upper_val.set_string_val("grape");

        auto binary_range_expr =
            std::make_shared<milvus::expr::BinaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(
                    json_fid, DataType::JSON, {"name"}, true),
                lower_val,
                upper_val,
                true,
                true);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           binary_range_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Strings between "cherry" and "grape" inclusive: cherry, date, elderberry, fig, grape
        EXPECT_EQ(result.count(), 5);
        for (size_t i = 2; i <= 6; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON NullExpr IS_NULL
    {
        auto null_expr = std::make_shared<milvus::expr::NullExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::NullExpr_NullOp_IsNull);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           null_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // None should match since all have non-null names
        EXPECT_EQ(result.count(), 0);
    }

    // Test: JSON NullExpr IS_NOT_NULL
    {
        auto null_expr = std::make_shared<milvus::expr::NullExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::NullExpr_NullOp_IsNotNull);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           null_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // All should match since all have non-null names
        EXPECT_EQ(result.count(), 10);
        for (size_t i = 0; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON ExistsExpr
    {
        auto exists_expr = std::make_shared<milvus::expr::ExistsExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true));
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           exists_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // All should match since all have the "name" field
        EXPECT_EQ(result.count(), 10);
        for (size_t i = 0; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON LogicalBinaryExpr with AND
    {
        // Create Equal expression for "apple"
        proto::plan::GenericValue val1;
        val1.set_string_val("apple");
        auto expr1 = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::Equal,
            val1,
            std::vector<proto::plan::GenericValue>{});

        // Create NotEqual expression for "banana"
        proto::plan::GenericValue val2;
        val2.set_string_val("banana");
        auto expr2 = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::NotEqual,
            val2,
            std::vector<proto::plan::GenericValue>{});

        // Create LogicalBinaryExpr with AND
        auto logical_and_expr =
            std::make_shared<milvus::expr::LogicalBinaryExpr>(
                milvus::expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           logical_and_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Only apple should match (name == "apple" AND name != "banana")
        EXPECT_EQ(result.count(), 1);
        EXPECT_TRUE(result[0]);
    }

    // Test: JSON LogicalUnaryExpr with NOT
    {
        proto::plan::GenericValue value;
        value.set_string_val("apple");
        auto equal_expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::Equal,
            value,
            std::vector<proto::plan::GenericValue>{});

        // Create LogicalUnaryExpr with NOT
        auto logical_not_expr =
            std::make_shared<milvus::expr::LogicalUnaryExpr>(
                milvus::expr::LogicalUnaryExpr::OpType::LogicalNot, equal_expr);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           logical_not_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // All except apple should match (NOT (name == "apple"))
        EXPECT_EQ(result.count(), 9);
        EXPECT_FALSE(result[0]);
        for (size_t i = 1; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }
}

// ============== Ngram Index Pattern Matching Consistency Tests ==============
// These tests verify that ngram index pattern matching produces the same results
// as RE2 and LikePatternMatcher. Ngram index uses a two-phase approach:
// 1. Phase 1: Use ngram matching to get candidate rows (may have false positives)
// 2. Phase 2: Use LikePatternMatcher to filter candidates (exact matching)
// The final result must be consistent with direct RE2/LikePatternMatcher matching.

TEST(NgramPatternMatchConsistency, MatchersMustAgree) {
    // Test data - strings that are long enough for ngram matching (min_gram=2)
    boost::container::vector<std::string> test_data = {
        "hello",      "hello world",  "world hello", "say hello there",
        "helloworld", "worldhello",   "testing",     "tested",
        "tester",     "test case",    "application", "apple pie",
        "pineapple",  "banana split", "aaaa",        "aaa",
        "aaab",       "abaa",         "abab",        "ababab",
        "xaax",       "xaaax",
    };

    // Patterns to test - all have segments >= 2 chars for ngram
    std::vector<std::pair<std::string, proto::plan::OpType>> test_cases = {
        // PrefixMatch tests
        {"hello", proto::plan::OpType::PrefixMatch},
        {"test", proto::plan::OpType::PrefixMatch},
        {"app", proto::plan::OpType::PrefixMatch},
        {"ab", proto::plan::OpType::PrefixMatch},

        // PostfixMatch tests
        {"world", proto::plan::OpType::PostfixMatch},
        {"ing", proto::plan::OpType::PostfixMatch},
        {"ple", proto::plan::OpType::PostfixMatch},
        {"ab", proto::plan::OpType::PostfixMatch},

        // InnerMatch tests
        {"ello", proto::plan::OpType::InnerMatch},
        {"test", proto::plan::OpType::InnerMatch},
        {"app", proto::plan::OpType::InnerMatch},
        {"aa", proto::plan::OpType::InnerMatch},
        {"ab", proto::plan::OpType::InnerMatch},

        // Match (LIKE pattern) tests - patterns with segments >= 2 chars
        {"hello%", proto::plan::OpType::Match},
        {"%world", proto::plan::OpType::Match},
        {"%ello%", proto::plan::OpType::Match},
        {"test%ing", proto::plan::OpType::Match},
        {"%aa%aa%", proto::plan::OpType::Match},  // Overlapping pattern
        {"ab%ab", proto::plan::OpType::Match},
        {"%ab%ab%", proto::plan::OpType::Match},
    };

    for (const auto& [pattern, op_type] : test_cases) {
        // Compute expected results using RE2 and LikePatternMatcher
        std::vector<bool> re2_results;
        std::vector<bool> like_results;

        PatternMatchTranslator translator;
        std::string like_pattern;

        // Convert to LIKE pattern based on op_type
        switch (op_type) {
            case proto::plan::OpType::PrefixMatch:
                like_pattern = pattern + "%";
                break;
            case proto::plan::OpType::PostfixMatch:
                like_pattern = "%" + pattern;
                break;
            case proto::plan::OpType::InnerMatch:
                like_pattern = "%" + pattern + "%";
                break;
            case proto::plan::OpType::Match:
                like_pattern = pattern;
                break;
            default:
                continue;
        }

        auto regex_pattern = translator(like_pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(like_pattern);

        for (const auto& data : test_data) {
            re2_results.push_back(re2_matcher(data));
            like_results.push_back(like_matcher(data));
        }

        // Verify RE2 and LikePatternMatcher agree
        for (size_t i = 0; i < test_data.size(); i++) {
            EXPECT_EQ(re2_results[i], like_results[i])
                << "RE2/LikePatternMatcher mismatch for ngram test:\n"
                << "  pattern=\"" << pattern
                << "\", op_type=" << static_cast<int>(op_type) << "\n"
                << "  like_pattern=\"" << like_pattern << "\"\n"
                << "  data=\"" << test_data[i] << "\"\n"
                << "  RE2=" << re2_results[i] << ", Like=" << like_results[i];
        }

        // Test with ngram index
        test_ngram_with_data(test_data, pattern, op_type, like_results);
    }
}

TEST(NgramPatternMatchConsistency, OverlappingPatterns) {
    // Test data specifically for overlapping patterns
    boost::container::vector<std::string> test_data = {
        "aa",
        "aaa",
        "aaaa",
        "aaaaa",
        "aaaaaa",
        "ab",
        "aba",
        "abab",
        "ababab",
        "abba",
        "aab",
        "baa",
        "xaax",
        "xaaax",
        "xaaaax",
        "abcabc",
        "abcabcabc",
    };

    // Overlapping patterns with segments >= 2 chars
    std::vector<std::string> patterns = {
        "%aa%aa%",     // Two overlapping "aa"
        "%aa%aa%aa%",  // Three overlapping "aa"
        "%ab%ab%",     // Two "ab"
        "%ab%ab%ab%",  // Three "ab"
        "aa%aa",       // Prefix and suffix both "aa"
        "ab%ab",       // Prefix and suffix both "ab"
        "%abc%abc%",   // Two "abc"
    };

    for (const auto& pattern : patterns) {
        // Compute expected results
        std::vector<bool> expected_results;

        PatternMatchTranslator translator;
        auto regex_pattern = translator(pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);

        for (const auto& data : test_data) {
            bool re2_result = re2_matcher(data);
            bool like_result = like_matcher(data);

            // RE2 and LikePatternMatcher must agree
            EXPECT_EQ(re2_result, like_result)
                << "RE2/LikePatternMatcher mismatch for overlapping pattern:\n"
                << "  pattern=\"" << pattern << "\"\n"
                << "  data=\"" << data << "\"\n"
                << "  RE2=" << re2_result << ", Like=" << like_result;

            expected_results.push_back(like_result);
        }

        // Test with ngram index
        test_ngram_with_data(
            test_data, pattern, proto::plan::OpType::Match, expected_results);
    }
}

TEST(NgramPatternMatchConsistency, UTF8Patterns) {
    // UTF-8 test data with strings long enough for ngram
    boost::container::vector<std::string> test_data = {
        "café latte",    // 2-byte UTF-8
        "hello café",    // Mixed
        "你好世界",      // 3-byte UTF-8 (Chinese)
        "test你好test",  // Mixed ASCII and Chinese
        "emoji😀test",    // 4-byte UTF-8 (emoji)
        "normal text",
        "café café",  // Repeated UTF-8
        "你好你好",   // Repeated Chinese
    };

    // Patterns with UTF-8 characters (segments must be >= 2 chars)
    std::vector<std::pair<std::string, proto::plan::OpType>> test_cases = {
        {"café", proto::plan::OpType::PrefixMatch},
        {"café", proto::plan::OpType::InnerMatch},
        {"你好", proto::plan::OpType::PrefixMatch},
        {"你好", proto::plan::OpType::InnerMatch},
        {"%café%", proto::plan::OpType::Match},
        {"%你好%", proto::plan::OpType::Match},
        {"%café%café%", proto::plan::OpType::Match},  // Overlapping UTF-8
        {"%你好%你好%", proto::plan::OpType::Match},  // Overlapping Chinese
    };

    for (const auto& [pattern, op_type] : test_cases) {
        // Compute expected results
        std::vector<bool> expected_results;

        PatternMatchTranslator translator;
        std::string like_pattern;

        switch (op_type) {
            case proto::plan::OpType::PrefixMatch:
                like_pattern = pattern + "%";
                break;
            case proto::plan::OpType::PostfixMatch:
                like_pattern = "%" + pattern;
                break;
            case proto::plan::OpType::InnerMatch:
                like_pattern = "%" + pattern + "%";
                break;
            case proto::plan::OpType::Match:
                like_pattern = pattern;
                break;
            default:
                continue;
        }

        auto regex_pattern = translator(like_pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(like_pattern);

        for (const auto& data : test_data) {
            bool re2_result = re2_matcher(data);
            bool like_result = like_matcher(data);

            EXPECT_EQ(re2_result, like_result)
                << "RE2/LikePatternMatcher UTF-8 mismatch:\n"
                << "  pattern=\"" << pattern << "\"\n"
                << "  data=\"" << data << "\"\n"
                << "  RE2=" << re2_result << ", Like=" << like_result;

            expected_results.push_back(like_result);
        }

        // Test with ngram index
        test_ngram_with_data(test_data, pattern, op_type, expected_results);
    }
}

TEST(NgramPatternMatchConsistency, EscapeSequences) {
    // Test data with special characters
    boost::container::vector<std::string> test_data = {
        "100% complete",
        "50% off sale",
        "file_name.txt",
        "path\\to\\file",
        "normal text",
        "%percent%",
        "_underscore_",
        "test\\escape",
    };

    // Patterns with escape sequences (segments >= 2 chars)
    std::vector<std::string> patterns = {
        "%100\\%%",   // Contains literal %
        "%file\\_%",  // Contains literal _
        "%\\\\%",     // Contains backslash
    };

    for (const auto& pattern : patterns) {
        std::vector<bool> expected_results;

        PatternMatchTranslator translator;
        auto regex_pattern = translator(pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);

        for (const auto& data : test_data) {
            bool re2_result = re2_matcher(data);
            bool like_result = like_matcher(data);

            EXPECT_EQ(re2_result, like_result)
                << "RE2/LikePatternMatcher escape mismatch:\n"
                << "  pattern=\"" << pattern << "\"\n"
                << "  data=\"" << data << "\"\n"
                << "  RE2=" << re2_result << ", Like=" << like_result;

            expected_results.push_back(like_result);
        }

        // Test with ngram index
        test_ngram_with_data(
            test_data, pattern, proto::plan::OpType::Match, expected_results);
    }
}