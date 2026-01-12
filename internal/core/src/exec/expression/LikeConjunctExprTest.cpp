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
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <memory>
#include <nlohmann/json.hpp>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

#include "common/Schema.h"
#include "common/Consts.h"
#include "test_utils/GenExprProto.h"
#include "query/PlanProto.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "expr/ITypeExpr.h"
#include "test_utils/storage_test_utils.h"
#include "index/IndexFactory.h"
#include "index/NgramInvertedIndex.h"
#include "segcore/load_index_c.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "milvus-storage/filesystem/fs.h"

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
            LoadIndexInfo load_index_info{
                .collection_id = collection_id,
                .partition_id = partition_id,
                .segment_id = segment_id,
                .field_id = field_id.get(),
                .field_type = DataType::VARCHAR,
                .enable_mmap = true,
                .mmap_dir_path = "/tmp/test-like-conjunct-mmap-dir",
                .index_id = index_id,
                .index_build_id = field_index_build_id,
                .index_version = index_version,
                .index_params = index_params,
                .index_files = index_files,
                .schema = field_meta.field_schema,
                .index_size = 1024 * 1024,
            };

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

// Helper function to truncate UTF-8 string safely
static std::string
TruncateUtf8(const std::string& str, size_t max_bytes) {
    if (str.size() <= max_bytes) {
        return str;
    }
    size_t pos = max_bytes;
    while (pos > 0 && (str[pos] & 0xC0) == 0x80) {
        --pos;
    }
    return str.substr(0, pos);
}

// Performance test for multiple LIKE AND conditions via conjunction
// Tests 4, 6, and 8 LIKE conditions with and without ngram index
TEST(LikeConjunctExpr, TestWikiDataConjunctionPerformance) {
    const std::string wiki_dir = "/home/spadea/working4/wiki-jsons";
    const size_t max_text_length = 0;  // Truncate to 500 bytes

    // Load wiki data
    boost::container::vector<std::string> text_data;
    size_t total_text_length = 0;

    std::cout << "\n=== Loading Wiki Data ===" << std::endl;
    std::cout << "Directory: " << wiki_dir << std::endl;

    for (const auto& entry : std::filesystem::directory_iterator(wiki_dir)) {
        if (entry.path().extension() == ".json") {
            std::ifstream file(entry.path());
            if (!file.is_open()) {
                continue;
            }
            nlohmann::json json_data = nlohmann::json::parse(file);
            for (const auto& item : json_data) {
                if (item.contains("text")) {
                    std::string text = item["text"].get<std::string>();
                    if (max_text_length > 0 && text.size() > max_text_length) {
                        text = TruncateUtf8(text, max_text_length);
                    }
                    total_text_length += text.size();
                    text_data.push_back(std::move(text));
                }
            }
            std::cout << "  Loaded: " << entry.path().filename()
                      << " (total: " << text_data.size() << " rows)"
                      << std::endl;
        }
    }

    const size_t nb = text_data.size();
    if (nb == 0) {
        std::cout << "No data loaded, skipping test." << std::endl;
        GTEST_SKIP();
    }

    double avg_length =
        nb > 0 ? static_cast<double>(total_text_length) / nb : 0;
    std::cout << "\nData Statistics:" << std::endl;
    std::cout << "  Total rows: " << nb << std::endl;
    std::cout << "  Avg text length: " << std::fixed << std::setprecision(2)
              << avg_length << " bytes" << std::endl;

    // Setup
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t index_build_id = 4000;
    int64_t index_version = 4000;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto text_fid = schema->AddDebugField("text", DataType::VARCHAR);
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

    auto field_meta = gen_field_meta(collection_id,
                                     partition_id,
                                     segment_id,
                                     text_fid.get(),
                                     DataType::VARCHAR,
                                     DataType::NONE,
                                     false);
    auto index_meta = gen_index_meta(
        segment_id, text_fid.get(), index_build_id, index_version);

    // Create segment and load field data
    auto segment = CreateSealedSegment(schema);

    // Generate row_ids and timestamps
    std::vector<int64_t> row_ids(nb);
    std::vector<int64_t> timestamps(nb);
    std::iota(row_ids.begin(), row_ids.end(), 0);
    std::iota(timestamps.begin(), timestamps.end(), 0);

    // Load row_id field (system field)
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

    // Load timestamp field (system field)
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

    // Load pk field
    std::vector<int64_t> pk_data(nb);
    std::iota(pk_data.begin(), pk_data.end(), 0);
    {
        auto pk_field_data =
            storage::CreateFieldData(DataType::INT64, DataType::NONE, false);
        pk_field_data->FillFieldData(pk_data.data(), pk_data.size());
        auto pk_field_info = PrepareSingleFieldInsertBinlog(collection_id,
                                                            partition_id,
                                                            segment_id,
                                                            pk_fid.get(),
                                                            {pk_field_data},
                                                            cm);
        segment->LoadFieldData(pk_field_info);
    }

    // Load text field
    auto text_field_data =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, false);
    text_field_data->FillFieldData(text_data.data(), text_data.size());
    auto text_field_info = PrepareSingleFieldInsertBinlog(collection_id,
                                                          partition_id,
                                                          segment_id,
                                                          text_fid.get(),
                                                          {text_field_data},
                                                          cm);
    segment->LoadFieldData(text_field_info);

    // Prepare index files
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(text_field_data);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);
    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    auto log_path = fmt::format("{}/{}/{}/{}/{}",
                                collection_id,
                                partition_id,
                                segment_id,
                                text_fid.get(),
                                0);
    auto cm_w = ChunkManagerWrapper(cm);
    cm_w.Write(log_path, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);

    // Build ngram index
    std::cout << "\nBuilding ngram index..." << std::endl;
    auto build_start = std::chrono::high_resolution_clock::now();
    std::vector<std::string> index_files;
    {
        Config config;
        config[milvus::index::INDEX_TYPE] = milvus::index::INVERTED_INDEX_TYPE;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};

        auto ngram_params = index::NgramParams{
            .loading_index = false,
            .min_gram = 2,
            .max_gram = 3,
        };
        auto index =
            std::make_shared<index::NgramInvertedIndex>(ctx, ngram_params);
        index->Build(config);
        auto result = index->Upload();
        index_files = result->GetIndexFiles();
    }
    auto build_end = std::chrono::high_resolution_clock::now();
    auto build_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        build_end - build_start)
                        .count();
    std::cout << "Index build time: " << build_ms << " ms" << std::endl;

    // Pattern configuration
    struct PatternConfig {
        std::string pattern;
        proto::plan::OpType op_type;
    };

    // Pattern Group 1: Mixed themes (expected 0 matches - control group)
    // Different topics AND-ed together won't match same articles
    std::vector<PatternConfig> mixed_patterns = {
        {"is a species of", proto::plan::OpType::InnerMatch},  // biology
        {"%species%family%", proto::plan::OpType::Match},      // biology
        {"railway station",
         proto::plan::OpType::InnerMatch},  // geography/transport
        {"%village%district%", proto::plan::OpType::Match},  // geography
        {"is a species of plant", proto::plan::OpType::InnerMatch},  // biology
        {"%railway%station%located%", proto::plan::OpType::Match},  // transport
        {"species of beetle in the",
         proto::plan::OpType::InnerMatch},                         // biology
        {"%surname%notable%people%", proto::plan::OpType::Match},  // biography
    };

    // Pattern Group 2: Species/biology theme (expected matches - experiment group)
    // All patterns commonly appear together in species articles
    std::vector<PatternConfig> species_patterns = {
        {"is a species", proto::plan::OpType::InnerMatch},  // very common
        {"%family%", proto::plan::OpType::Match},           // taxonomic family
        {"genus", proto::plan::OpType::InnerMatch},         // taxonomic genus
        {"%species%genus%", proto::plan::OpType::Match},    // species and genus
        {"described",
         proto::plan::OpType::InnerMatch},          // "first described by..."
        {"%endemic%", proto::plan::OpType::Match},  // endemic species
        {"native to", proto::plan::OpType::InnerMatch},  // native habitat
        {"%found%in%", proto::plan::OpType::Match},      // distribution info
    };

    struct PatternGroup {
        std::string name;
        std::vector<PatternConfig>& patterns;
    };
    std::vector<PatternGroup> pattern_groups = {
        {"Mixed Themes (control)", mixed_patterns},
        {"Species Theme (experiment)", species_patterns},
    };

    // Helper to create LIKE expressions and AND them together
    auto create_conjunction_expr =
        [&](int num_likes, const std::vector<PatternConfig>& patterns)
        -> std::shared_ptr<milvus::expr::ITypeFilterExpr> {
        std::vector<std::shared_ptr<milvus::expr::ITypeFilterExpr>> exprs;
        for (int i = 0; i < num_likes && i < (int)patterns.size(); i++) {
            proto::plan::GenericValue val;
            val.set_string_val(patterns[i].pattern);
            auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(text_fid, DataType::VARCHAR),
                patterns[i].op_type,
                val,
                std::vector<proto::plan::GenericValue>{});
            exprs.push_back(expr);
        }

        // Build nested AND: ((A AND B) AND C) AND D ...
        auto result = exprs[0];
        for (size_t i = 1; i < exprs.size(); i++) {
            result = std::make_shared<milvus::expr::LogicalBinaryExpr>(
                milvus::expr::LogicalBinaryExpr::OpType::And, result, exprs[i]);
        }
        return result;
    };

    // Helper to run retrieve and measure time
    auto run_retrieve =
        [&](const std::shared_ptr<milvus::expr::ITypeFilterExpr>& expr)
        -> std::pair<double, size_t> {
        auto plan = std::make_unique<query::RetrievePlan>(schema);
        plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
        plan->plan_node_->plannodes_ =
            milvus::test::CreateRetrievePlanByExpr(expr);
        plan->field_ids_ = {pk_fid};

        auto start = std::chrono::high_resolution_clock::now();
        auto results = segment->Retrieve(
            nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);
        auto end = std::chrono::high_resolution_clock::now();

        double ms =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                .count() /
            1000.0;
        size_t count = 0;
        if (results->fields_data_size() > 0) {
            count = results->fields_data(0).scalars().long_data().data_size();
        }
        return {ms, count};
    };

    // Test configuration
    const int warmup_runs = 1;
    const int test_runs = 3;
    std::vector<int> like_counts = {4, 6, 8};

    // Structure to store results
    struct TestResult {
        std::string group_name;
        int num_likes;
        bool with_index;
        double avg_ms;
        size_t result_count;
    };
    std::vector<TestResult> all_results;

    // Helper to get op type string
    auto op_type_str = [](proto::plan::OpType op) -> std::string {
        return op == proto::plan::OpType::InnerMatch ? "InnerMatch" : "Match";
    };

    // Run tests for each pattern group
    for (auto& group : pattern_groups) {
        auto& patterns = group.patterns;

        std::cout << "\n=== Pattern Group: " << group.name
                  << " ===" << std::endl;
        for (size_t i = 0; i < patterns.size(); i++) {
            std::cout << "  [" << i << "] " << std::setw(12)
                      << op_type_str(patterns[i].op_type) << ": \""
                      << patterns[i].pattern << "\"" << std::endl;
        }

        std::cout << "\n--- Phase 1: Without Ngram Index ---" << std::endl;

        for (int num_likes : like_counts) {
            auto expr = create_conjunction_expr(num_likes, patterns);

            // Count InnerMatch vs Match
            int inner_count = 0, match_count = 0;
            for (int i = 0; i < num_likes && i < (int)patterns.size(); i++) {
                if (patterns[i].op_type == proto::plan::OpType::InnerMatch)
                    inner_count++;
                else
                    match_count++;
            }

            // Warmup
            std::cout << "  " << num_likes << " LIKE (" << inner_count
                      << " InnerMatch + " << match_count
                      << " Match): " << std::flush;
            for (int w = 0; w < warmup_runs; w++) {
                run_retrieve(expr);
            }

            // Test runs
            double total_ms = 0;
            size_t count = 0;
            for (int r = 0; r < test_runs; r++) {
                auto [ms, cnt] = run_retrieve(expr);
                total_ms += ms;
                if (r == 0)
                    count = cnt;
            }
            double avg_ms = total_ms / test_runs;
            all_results.push_back(
                {group.name, num_likes, false, avg_ms, count});
            std::cout << std::fixed << std::setprecision(2) << avg_ms
                      << " ms, matches: " << count << std::endl;
        }
    }

    // Load ngram index
    std::cout << "\n=== Loading Ngram Index ===" << std::endl;
    {
        std::map<std::string, std::string> index_params{
            {milvus::index::INDEX_TYPE, milvus::index::NGRAM_INDEX_TYPE},
            {milvus::index::MIN_GRAM, "2"},
            {milvus::index::MAX_GRAM, "3"},
            {milvus::LOAD_PRIORITY, "HIGH"},
        };
        LoadIndexInfo load_index_info{
            .collection_id = collection_id,
            .partition_id = partition_id,
            .segment_id = segment_id,
            .field_id = text_fid.get(),
            .field_type = DataType::VARCHAR,
            .enable_mmap = true,
            .mmap_dir_path = "/tmp/test-like-conjunct-mmap/",
            .index_id = 5001,
            .index_build_id = index_build_id,
            .index_version = index_version,
            .index_params = index_params,
            .index_files = index_files,
            .schema = field_meta.field_schema,
            .index_size = 1024 * 1024,
        };

        uint8_t trace_id[16] = {0};
        uint8_t span_id[8] = {0};
        CTraceContext trace{
            .traceID = trace_id,
            .spanID = span_id,
            .traceFlags = 0,
        };
        auto cload_index_info = static_cast<CLoadIndexInfo>(&load_index_info);
        AppendIndexV2(trace, cload_index_info);
        UpdateSealedSegmentIndex(segment.get(), cload_index_info);
    }
    std::cout << "Ngram index loaded." << std::endl;

    // Run tests with ngram index for each pattern group
    for (auto& group : pattern_groups) {
        auto& patterns = group.patterns;

        std::cout << "\n=== Pattern Group: " << group.name
                  << " (With Index) ===" << std::endl;

        for (int num_likes : like_counts) {
            auto expr = create_conjunction_expr(num_likes, patterns);

            // Count InnerMatch vs Match
            int inner_count = 0, match_count = 0;
            for (int i = 0; i < num_likes && i < (int)patterns.size(); i++) {
                if (patterns[i].op_type == proto::plan::OpType::InnerMatch)
                    inner_count++;
                else
                    match_count++;
            }

            // Warmup
            std::cout << "  " << num_likes << " LIKE (" << inner_count
                      << " InnerMatch + " << match_count
                      << " Match): " << std::flush;
            for (int w = 0; w < warmup_runs; w++) {
                run_retrieve(expr);
            }

            // Test runs
            double total_ms = 0;
            size_t count = 0;
            for (int r = 0; r < test_runs; r++) {
                std::cout << std::endl;
                auto [ms, cnt] = run_retrieve(expr);
                total_ms += ms;
                if (r == 0)
                    count = cnt;
            }
            double avg_ms = total_ms / test_runs;
            all_results.push_back({group.name, num_likes, true, avg_ms, count});
            std::cout << std::fixed << std::setprecision(2) << avg_ms
                      << " ms, matches: " << count << std::endl;
        }
    }

    // Build summary string (output at the end to avoid mixing with debug logs)
    std::ostringstream summary;
    summary << "\n";
    summary << "============================================================="
               "==================\n";
    summary << "                              FINAL RESULTS                   "
               "                 \n";
    summary << "============================================================="
               "==================\n";

    for (auto& group : pattern_groups) {
        auto& patterns = group.patterns;

        summary << "\n>>> " << group.name << " <<<\n";
        summary << std::setw(12) << "LIKE Count" << std::setw(15)
                << "Without Index" << std::setw(15) << "With Index"
                << std::setw(12) << "Speedup" << std::setw(12) << "Matches\n";
        summary << std::string(66, '-') << "\n";

        for (int num_likes : like_counts) {
            double no_idx_ms = 0, with_idx_ms = 0;
            size_t count = 0;
            for (const auto& r : all_results) {
                if (r.group_name == group.name && r.num_likes == num_likes) {
                    if (r.with_index) {
                        with_idx_ms = r.avg_ms;
                        count = r.result_count;
                    } else {
                        no_idx_ms = r.avg_ms;
                    }
                }
            }
            double speedup = with_idx_ms > 0 ? no_idx_ms / with_idx_ms : 0;
            summary << std::setw(12) << num_likes << std::setw(14) << std::fixed
                    << std::setprecision(2) << no_idx_ms << " ms"
                    << std::setw(14) << with_idx_ms << " ms" << std::setw(11)
                    << speedup << "x" << std::setw(12) << count << "\n";

            // Print expressions used for this LIKE count
            summary << "    Expressions:\n";
            for (int i = 0; i < num_likes && i < (int)patterns.size(); i++) {
                summary << "      [" << i << "] " << std::setw(11)
                        << op_type_str(patterns[i].op_type) << ": \""
                        << patterns[i].pattern << "\"\n";
            }
        }
        summary << std::string(66, '-') << "\n";
    }

    // Output summary at the end
    std::cout << summary.str() << std::flush;

    // Verify results are consistent within each group
    for (auto& group : pattern_groups) {
        for (int num_likes : like_counts) {
            size_t no_idx_count = 0, with_idx_count = 0;
            for (const auto& r : all_results) {
                if (r.group_name == group.name && r.num_likes == num_likes) {
                    if (r.with_index) {
                        with_idx_count = r.result_count;
                    } else {
                        no_idx_count = r.result_count;
                    }
                }
            }
            EXPECT_EQ(no_idx_count, with_idx_count)
                << "Result count mismatch for " << group.name << " with "
                << num_likes << " LIKE conditions";
        }
    }
}
