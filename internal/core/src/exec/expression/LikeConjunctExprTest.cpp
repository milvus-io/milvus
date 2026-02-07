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
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "exec/expression/function/FunctionFactory.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/IndexInfo.h"
#include "index/IndexStats.h"
#include "index/Meta.h"
#include "index/NgramInvertedIndex.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "query/Utils.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "segcore/load_index_c.h"
#include "segcore/segment_c.h"
#include "storage/FileManager.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::exec;

std::unique_ptr<proto::segcore::RetrieveResults>
RetrieveWithPlan(SegmentInterface* segment,
                 const query::RetrievePlan* plan,
                 Timestamp timestamp) {
    return segment->Retrieve(
        nullptr, plan, timestamp, DEFAULT_MAX_OUTPUT_SIZE, false);
}

// Test multiple LIKE conditions on multiple fields with AND using ngram index
// 2 fields x 2 LIKE conditions each = 4 total LIKE conditions
// title LIKE '%Database%' AND title LIKE '%Design%' AND content LIKE '%SQL%' AND content LIKE '%query%'
TEST(LikeConjunctExpr, TestMultiFieldMultiLikeWithRetrieve) {
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t index_version = 4000;

    EXEC_EVAL_EXPR_BATCH_SIZE.store(3);

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto title_fid = schema->AddDebugField("title", DataType::VARCHAR);
    auto content_fid = schema->AddDebugField("content", DataType::VARCHAR);
    schema->set_primary_field_id(pk_fid);

    // Use TestLocalPath to match LocalChunkManagerSingleton initialized in init_gtest.cpp
    auto storage_config = gen_local_storage_config(TestLocalPath);
    auto cm = CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

    // Initialize ArrowFileSystemSingleton for UpdateSealedSegmentIndex
    milvus_storage::ArrowFileSystemConfig arrow_conf;
    arrow_conf.storage_type = "local";
    arrow_conf.root_path = storage_config.root_path;
    milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(arrow_conf);

    // Test data: 8 rows
    // title conditions: "Database" AND "Design"
    // content conditions: "SQL" AND "query"
    std::vector<int64_t> pk_data = {0, 1, 2, 3, 4, 5, 6, 7};
    boost::container::vector<std::string> title_data = {
        "Database Design Patterns",        // 0: has "Database" + "Design" ✓
        "Machine Learning Fundamentals",   // 1: no match
        "Advanced Database Design Guide",  // 2: has "Database" + "Design" ✓
        "Web Development Design",          // 3: has "Design" only
        "Database Performance Tuning",     // 4: has "Database" only
        "Cloud Computing Basics",          // 5: no match
        "Distributed Database Design Systems",  // 6: has "Database" + "Design" ✓
        "Software Design Principles",           // 7: has "Design" only
    };
    boost::container::vector<std::string> content_data = {
        "SQL query optimization techniques",  // 0: has "SQL" + "query" ✓
        "Deep learning and neural networks",  // 1: no match
        "Advanced SQL query patterns",        // 2: has "SQL" + "query" ✓
        "HTML CSS JavaScript tutorial",       // 3: no match
        "SQL tuning and monitoring",          // 4: has "SQL" only
        "AWS Azure GCP comparison",           // 5: no match
        "Complex SQL query design",           // 6: has "SQL" + "query" ✓
        "Python Java C++ examples",           // 7: no match
    };
    // Expected matches (all 4 conditions): rows 0, 2, 6

    auto segment = CreateSealedSegment(schema);

    // Load system fields (row_id and timestamp) for proper Retrieve support
    size_t nb = pk_data.size();
    std::vector<int64_t> row_ids(nb);
    std::vector<int64_t> timestamps(nb);
    std::iota(row_ids.begin(), row_ids.end(), 0);
    std::iota(timestamps.begin(), timestamps.end(), 0);

    {
        auto row_id_field_data =
            storage::CreateFieldData(DataType::INT64, DataType::NONE, false);
        row_id_field_data->FillFieldData(row_ids.data(), row_ids.size());
        auto row_id_field_info =
            PrepareSingleFieldInsertBinlog(collection_id,
                                           partition_id,
                                           segment_id,
                                           RowFieldID.get(),
                                           {row_id_field_data},
                                           cm);
        segment->LoadFieldData(row_id_field_info);
    }
    {
        auto ts_field_data =
            storage::CreateFieldData(DataType::INT64, DataType::NONE, false);
        ts_field_data->FillFieldData(timestamps.data(), timestamps.size());
        auto ts_field_info =
            PrepareSingleFieldInsertBinlog(collection_id,
                                           partition_id,
                                           segment_id,
                                           TimestampFieldID.get(),
                                           {ts_field_data},
                                           cm);
        segment->LoadFieldData(ts_field_info);
    }

    // Create ChunkManagerWrapper at test level for cleanup after test ends
    auto cm_w = ChunkManagerWrapper(cm);

    // Helper to load field with ngram index
    auto load_varchar_field_with_ngram =
        [&](FieldId field_id,
            const boost::container::vector<std::string>& data,
            int64_t index_id,
            int64_t field_index_build_id) {
            auto field_meta = gen_field_meta(collection_id,
                                             partition_id,
                                             segment_id,
                                             field_id.get(),
                                             DataType::VARCHAR,
                                             DataType::NONE,
                                             false);
            auto index_meta = gen_index_meta(segment_id,
                                             field_id.get(),
                                             field_index_build_id,
                                             index_version);

            auto field_data = storage::CreateFieldData(
                DataType::VARCHAR, DataType::NONE, false);
            field_data->FillFieldData(data.data(), data.size());

            auto field_data_info =
                PrepareSingleFieldInsertBinlog(collection_id,
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

            auto log_path = fmt::format("{}/{}/{}/{}/{}",
                                        collection_id,
                                        partition_id,
                                        segment_id,
                                        field_id.get(),
                                        0);
            cm_w.Write(
                log_path, serialized_bytes.data(), serialized_bytes.size());

            storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);

            Config config;
            config[milvus::index::INDEX_TYPE] =
                milvus::index::INVERTED_INDEX_TYPE;
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
            auto index_files = create_index_result->GetIndexFiles();

            std::map<std::string, std::string> index_params{
                {milvus::index::INDEX_TYPE, milvus::index::NGRAM_INDEX_TYPE},
                {milvus::index::MIN_GRAM, "2"},
                {milvus::index::MAX_GRAM, "4"},
                {milvus::LOAD_PRIORITY, "HIGH"},
            };
            LoadIndexInfo load_index_info;
            load_index_info.collection_id = collection_id;
            load_index_info.partition_id = partition_id;
            load_index_info.segment_id = segment_id;
            load_index_info.field_id = field_id.get();
            load_index_info.field_type = DataType::VARCHAR;
            load_index_info.enable_mmap = true;
            load_index_info.mmap_dir_path =
                GetTestTempPath("test-like-conjunct-mmap-dir");
            load_index_info.index_id = index_id;
            load_index_info.index_build_id = field_index_build_id;
            load_index_info.index_version = index_version;
            load_index_info.index_params = index_params;
            load_index_info.index_files = index_files;
            load_index_info.schema = field_meta.field_schema;
            load_index_info.index_size = 1024 * 1024;

            uint8_t trace_id[16] = {0};
            uint8_t span_id[8] = {0};
            CTraceContext trace{
                .traceID = trace_id,
                .spanID = span_id,
                .traceFlags = 0,
            };
            auto cload_index_info =
                static_cast<CLoadIndexInfo>(&load_index_info);
            AppendIndexV2(trace, cload_index_info);
            UpdateSealedSegmentIndex(segment.get(), cload_index_info);
        };

    // Load pk field
    {
        auto pk_field_data =
            storage::CreateFieldData(DataType::INT64, DataType::NONE, false);
        pk_field_data->FillFieldData(pk_data.data(), pk_data.size());

        auto field_data_info = PrepareSingleFieldInsertBinlog(collection_id,
                                                              partition_id,
                                                              segment_id,
                                                              pk_fid.get(),
                                                              {pk_field_data},
                                                              cm);
        segment->LoadFieldData(field_data_info);
    }

    // Load title and content fields with ngram index (use different index_build_id for each)
    load_varchar_field_with_ngram(title_fid, title_data, 5001, 4001);
    load_varchar_field_with_ngram(content_fid, content_data, 5002, 4002);

    // Create 4 LIKE expressions:
    // title LIKE '%Database%' AND title LIKE '%Design%' AND content LIKE '%SQL%' AND content LIKE '%query%'
    proto::plan::GenericValue val_database, val_design, val_sql, val_query;
    val_database.set_string_val("Database");
    val_design.set_string_val("Design");
    val_sql.set_string_val("SQL");
    val_query.set_string_val("query");

    auto expr_title_database =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(title_fid, DataType::VARCHAR),
            proto::plan::OpType::InnerMatch,
            val_database,
            std::vector<proto::plan::GenericValue>{});

    auto expr_title_design =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(title_fid, DataType::VARCHAR),
            proto::plan::OpType::InnerMatch,
            val_design,
            std::vector<proto::plan::GenericValue>{});

    auto expr_content_sql =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(content_fid, DataType::VARCHAR),
            proto::plan::OpType::InnerMatch,
            val_sql,
            std::vector<proto::plan::GenericValue>{});

    auto expr_content_query =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(content_fid, DataType::VARCHAR),
            proto::plan::OpType::InnerMatch,
            val_query,
            std::vector<proto::plan::GenericValue>{});

    // Create nested AND expression: ((A AND B) AND C) AND D
    auto and_expr_1 = std::make_shared<milvus::expr::LogicalBinaryExpr>(
        milvus::expr::LogicalBinaryExpr::OpType::And,
        expr_title_database,
        expr_title_design);
    auto and_expr_2 = std::make_shared<milvus::expr::LogicalBinaryExpr>(
        milvus::expr::LogicalBinaryExpr::OpType::And,
        and_expr_1,
        expr_content_sql);
    auto and_expr = std::make_shared<milvus::expr::LogicalBinaryExpr>(
        milvus::expr::LogicalBinaryExpr::OpType::And,
        and_expr_2,
        expr_content_query);

    // Create RetrievePlan
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ =
        milvus::test::CreateRetrievePlanByExpr(and_expr);
    plan->field_ids_ = {pk_fid};

    // Execute retrieve
    auto results = RetrieveWithPlan(segment.get(), plan.get(), MAX_TIMESTAMP);

    // Verify results: rows 0, 2, 6 should match (all 4 LIKE conditions)
    ASSERT_EQ(results->fields_data_size(), 1);
    auto& field_data = results->fields_data(0);
    ASSERT_TRUE(field_data.has_scalars());
    auto& pk_results = field_data.scalars().long_data();

    std::set<int64_t> expected_pks = {0, 2, 6};
    std::set<int64_t> actual_pks;
    for (int i = 0; i < pk_results.data_size(); i++) {
        actual_pks.insert(pk_results.data(i));
    }

    EXPECT_EQ(actual_pks, expected_pks);

    EXEC_EVAL_EXPR_BATCH_SIZE.store(DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE);
}
