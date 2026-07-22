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
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/IndexMeta.h"
#include "segcore/default_fs.h"
#include "segcore/segment_c.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/c_api_test_utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/SegcoreConfigUtils.h"
#include "storage/Util.h"
#include "storage/loon_ffi/property_singleton.h"
#include "knowhere/index/index_factory.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/transaction/transaction.h"

using namespace milvus;
using namespace milvus::segcore;

namespace fs = std::filesystem;

class FlushGrowingSegmentTest : public ::testing::Test {
 protected:
    struct ParsedBM25Stats {
        std::unordered_map<uint32_t, int32_t> rows_with_token;
        int64_t num_row = 0;
        int64_t num_token = 0;
    };

    void
    SetUp() override {
        // create a temporary directory for test output
        test_dir_ = "/tmp/flush_growing_test_" + std::to_string(time(nullptr));
        fs::create_directories(test_dir_);

        // Arrow filesystem is initialized by init_gtest.cpp
    }

    void
    TearDown() override {
        // cleanup test directory
        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
    }

    std::string test_dir_;

    std::vector<FieldDataPtr>
    ReadFlushedFieldData(const std::string& segment_path,
                         const CFlushResult& result,
                         FieldId field_id,
                         DataType data_type,
                         bool nullable,
                         int64_t dim,
                         DataType element_type = DataType::NONE) {
        auto properties =
            storage::LoonFFIPropertiesSingleton::GetInstance().GetProperties();
        EXPECT_NE(properties, nullptr);
        auto field_meta = gen_field_meta(
            1, 2, 3, field_id.get(), data_type, element_type, nullable);
        std::string manifest_json =
            "{\"base_path\":\"" + segment_path +
            "\",\"ver\":" + std::to_string(result.committed_version) + "}";
        auto field_datas = storage::GetFieldDatasFromManifest(manifest_json,
                                                              properties,
                                                              field_meta,
                                                              data_type,
                                                              dim,
                                                              element_type);

        // Contract check for the streaming variant every caller of this
        // helper implicitly covers: IterateFieldDataFromManifest must
        // deliver identical batches, in order, on the calling thread, even
        // though decode runs on a thread pool.
        std::vector<FieldDataPtr> streamed;
        auto caller_tid = std::this_thread::get_id();
        storage::IterateFieldDataFromManifest(
            manifest_json,
            properties,
            field_meta,
            data_type,
            dim,
            element_type,
            std::nullopt,
            [&](FieldDataPtr fd) {
                EXPECT_EQ(std::this_thread::get_id(), caller_tid);
                streamed.push_back(std::move(fd));
            });
        EXPECT_EQ(streamed.size(), field_datas.size());
        for (size_t i = 0; i < std::min(streamed.size(), field_datas.size());
             ++i) {
            EXPECT_EQ(streamed[i]->get_num_rows(),
                      field_datas[i]->get_num_rows());
            for (int64_t r = 0; r < streamed[i]->get_num_rows(); ++r) {
                EXPECT_EQ(streamed[i]->is_valid(r),
                          field_datas[i]->is_valid(r));
            }
        }

        return field_datas;
    }

    void
    AssertManifestHasColumn(const std::string& segment_path,
                            int64_t version,
                            FieldId field_id) {
        auto fs = GetDefaultArrowFileSystem();
        ASSERT_NE(fs, nullptr);

        auto txn_result = milvus_storage::api::transaction::Transaction::Open(
            fs, segment_path, version);
        ASSERT_TRUE(txn_result.ok()) << txn_result.status().ToString();
        auto txn = std::move(txn_result).ValueOrDie();

        auto manifest_result = txn->GetManifest();
        ASSERT_TRUE(manifest_result.ok())
            << manifest_result.status().ToString();
        auto manifest = manifest_result.ValueOrDie();

        auto column_name = std::to_string(field_id.get());
        bool found = false;
        for (const auto& column_group : manifest->columnGroups()) {
            ASSERT_NE(column_group, nullptr);
            found |= std::find(column_group->columns.begin(),
                               column_group->columns.end(),
                               column_name) != column_group->columns.end();
        }

        EXPECT_TRUE(found) << "missing field " << column_name
                           << " in committed manifest";
    }

    void
    AssertManifestColumnGroupsUseFormat(const std::string& segment_path,
                                        int64_t version,
                                        const std::string& format) {
        auto fs = GetDefaultArrowFileSystem();
        ASSERT_NE(fs, nullptr);

        auto txn_result = milvus_storage::api::transaction::Transaction::Open(
            fs, segment_path, version);
        ASSERT_TRUE(txn_result.ok()) << txn_result.status().ToString();
        auto txn = std::move(txn_result).ValueOrDie();

        auto manifest_result = txn->GetManifest();
        ASSERT_TRUE(manifest_result.ok())
            << manifest_result.status().ToString();
        auto manifest = manifest_result.ValueOrDie();

        ASSERT_FALSE(manifest->columnGroups().empty());
        for (const auto& column_group : manifest->columnGroups()) {
            ASSERT_NE(column_group, nullptr);
            EXPECT_EQ(column_group->format, format);
        }
    }

    std::vector<std::string>
    ManifestStatPaths(const std::string& segment_path,
                      int64_t version,
                      const std::string& stat_key) {
        auto fs = GetDefaultArrowFileSystem();
        EXPECT_NE(fs, nullptr);
        if (!fs) {
            return {};
        }

        auto txn_result = milvus_storage::api::transaction::Transaction::Open(
            fs, segment_path, version);
        EXPECT_TRUE(txn_result.ok()) << txn_result.status().ToString();
        if (!txn_result.ok()) {
            return {};
        }
        auto txn = std::move(txn_result).ValueOrDie();

        auto manifest_result = txn->GetManifest();
        EXPECT_TRUE(manifest_result.ok())
            << manifest_result.status().ToString();
        if (!manifest_result.ok()) {
            return {};
        }
        auto manifest = manifest_result.ValueOrDie();
        auto stats_it = manifest->stats().find(stat_key);
        EXPECT_NE(stats_it, manifest->stats().end())
            << "missing stat key " << stat_key;
        if (stats_it == manifest->stats().end()) {
            return {};
        }
        return stats_it->second.paths;
    }

    std::optional<ParsedBM25Stats>
    ParseBM25StatsBlob(const uint8_t* data, size_t size) {
        if (size < 20 || (size - 20) % 8 != 0) {
            ADD_FAILURE() << "invalid BM25 stats blob size " << size;
            return std::nullopt;
        }

        int32_t version = 0;
        ParsedBM25Stats stats;
        std::memcpy(&version, data, sizeof(version));
        std::memcpy(&stats.num_row, data + 4, sizeof(stats.num_row));
        std::memcpy(&stats.num_token, data + 12, sizeof(stats.num_token));
        EXPECT_EQ(version, 0);
        if (version != 0) {
            return std::nullopt;
        }

        auto entries = (size - 20) / 8;
        for (size_t i = 0; i < entries; i++) {
            uint32_t token = 0;
            int32_t row_count = 0;
            std::memcpy(&token, data + 20 + i * 8, sizeof(token));
            std::memcpy(&row_count, data + 20 + i * 8 + 4, sizeof(row_count));
            stats.rows_with_token[token] += row_count;
        }
        return stats;
    }

    std::optional<ParsedBM25Stats>
    ParseBM25StatsFromResult(const CFlushResult& result, FieldId field_id) {
        for (size_t i = 0; i < result.num_bm25_stats; i++) {
            if (result.bm25_field_ids[i] == field_id.get()) {
                return ParseBM25StatsBlob(result.bm25_stats[i],
                                          result.bm25_stats_sizes[i]);
            }
        }
        ADD_FAILURE() << "missing BM25 stats for field " << field_id.get();
        return std::nullopt;
    }

    std::optional<ParsedBM25Stats>
    ReadBM25StatsFromFile(const std::string& path) {
        auto fs = GetDefaultArrowFileSystem();
        EXPECT_NE(fs, nullptr);
        if (!fs) {
            return std::nullopt;
        }
        auto input_result = fs->OpenInputFile(path);
        EXPECT_TRUE(input_result.ok()) << input_result.status().ToString();
        if (!input_result.ok()) {
            return std::nullopt;
        }
        auto input = input_result.ValueOrDie();
        auto size_result = input->GetSize();
        EXPECT_TRUE(size_result.ok()) << size_result.status().ToString();
        if (!size_result.ok()) {
            return std::nullopt;
        }
        auto buffer_result = input->Read(size_result.ValueOrDie());
        EXPECT_TRUE(buffer_result.ok()) << buffer_result.status().ToString();
        if (!buffer_result.ok()) {
            return std::nullopt;
        }
        auto buffer = buffer_result.ValueOrDie();
        return ParseBM25StatsBlob(buffer->data(), buffer->size());
    }

    std::vector<uint8_t>
    ReadRawFile(const std::string& path) {
        auto fs = GetDefaultArrowFileSystem();
        EXPECT_NE(fs, nullptr);
        if (!fs) {
            return {};
        }
        auto input_result = fs->OpenInputFile(path);
        EXPECT_TRUE(input_result.ok()) << input_result.status().ToString();
        if (!input_result.ok()) {
            return {};
        }
        auto input = input_result.ValueOrDie();
        auto size_result = input->GetSize();
        EXPECT_TRUE(size_result.ok()) << size_result.status().ToString();
        if (!size_result.ok()) {
            return {};
        }
        auto buffer_result = input->Read(size_result.ValueOrDie());
        EXPECT_TRUE(buffer_result.ok()) << buffer_result.status().ToString();
        if (!buffer_result.ok()) {
            return {};
        }
        auto buffer = buffer_result.ValueOrDie();
        return std::vector<uint8_t>(buffer->data(),
                                    buffer->data() + buffer->size());
    }

    std::string
    SerializeSchemaBlob(const SchemaPtr& schema) {
        auto schema_proto = schema->ToProto();
        auto add_system_field = [&](FieldId field_id, const char* name) {
            auto has_field =
                std::any_of(schema_proto.fields().begin(),
                            schema_proto.fields().end(),
                            [&](const auto& field) {
                                return field.fieldid() == field_id.get();
                            });
            if (has_field) {
                return;
            }
            auto* field = schema_proto.add_fields();
            field->set_fieldid(field_id.get());
            field->set_name(name);
            field->set_data_type(proto::schema::DataType::Int64);
        };
        add_system_field(RowFieldID, "RowID");
        add_system_field(TimestampFieldID, "Timestamp");
        std::string blob;
        EXPECT_TRUE(schema_proto.SerializeToString(&blob));
        EXPECT_FALSE(blob.empty());
        return blob;
    }

    void
    SetFlushSchema(CFlushConfig& config, const std::string& schema_blob) {
        config.schema_blob = schema_blob.data();
        config.schema_length = static_cast<int64_t>(schema_blob.size());
    }

    IndexMetaPtr
    MakeVectorIndexMeta(FieldId vec_fid,
                        int64_t dim,
                        const std::string& index_type,
                        const std::string& metric_type) {
        std::map<std::string, std::string> index_params = {
            {"index_type", index_type}, {"metric_type", metric_type}};
        if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT) {
            index_params["nlist"] = "1";
        }
        std::map<std::string, std::string> type_params = {
            {"dim", std::to_string(dim)}};
        FieldIndexMeta field_index_meta(
            vec_fid, std::move(index_params), std::move(type_params));
        std::map<FieldId, FieldIndexMeta> field_map = {
            {vec_fid, field_index_meta}};
        return std::make_shared<CollectionIndexMeta>(100, std::move(field_map));
    }
};

#define C_FLUSH_CONFIG_WITH_SCHEMA(config, schema)           \
    auto config##_schema_blob = SerializeSchemaBlob(schema); \
    CFlushConfig config{};                                   \
    SetFlushSchema(config, config##_schema_blob)

// test basic flush with scalar fields
TEST_F(FlushGrowingSegmentTest, BasicFlushScalarFields) {
    // create schema with scalar fields
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto i32_fid = schema->AddDebugField("i32_field", DataType::INT32);
    auto f32_fid = schema->AddDebugField("f32_field", DataType::FLOAT);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 100;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;
    int64_t column_group_ids[] = {10};
    int64_t column_group_field_ids[] = {pk_fid.get(),
                                        i32_fid.get(),
                                        f32_fid.get(),
                                        RowFieldID.get(),
                                        TimestampFieldID.get()};
    size_t column_group_field_counts[] = {5};
    config.column_group_ids = column_group_ids;
    config.column_group_field_ids = column_group_field_ids;
    config.column_group_field_counts = column_group_field_counts;
    config.num_column_groups = 1;

    // flush data
    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_NE(result.manifest_path, nullptr);
    ASSERT_EQ(result.num_rows, N);
    ASSERT_GT(result.committed_version, 0);
    auto [expected_ts_from, expected_ts_to] = std::minmax_element(
        dataset.timestamps_.begin(), dataset.timestamps_.end());
    ASSERT_NE(expected_ts_from, dataset.timestamps_.end());
    EXPECT_EQ(result.timestamp_from, *expected_ts_from);
    EXPECT_EQ(result.timestamp_to, *expected_ts_to);
    ASSERT_GT(result.num_field_stats, 0);
    std::unordered_map<int64_t, int64_t> field_null_counts;
    for (size_t i = 0; i < result.num_field_stats; i++) {
        field_null_counts[result.field_ids[i]] = result.field_null_counts[i];
    }
    EXPECT_EQ(field_null_counts[pk_fid.get()], 0);
    ASSERT_EQ(result.num_column_groups, 1);
    EXPECT_EQ(result.column_group_ids[0], 10);
    EXPECT_GT(result.column_group_memory_sizes[0], 0);
    AssertManifestHasColumn(segment_path, result.committed_version, RowFieldID);

    // Note: manifest path may be relative to ArrowFileSystem root path
    // The actual file existence should be verified via ArrowFileSystem API
    // For this unit test, we just verify the flush completed successfully

    // cleanup
    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushAllowsStaleReadVersionOverwrite) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField("i32_field", DataType::INT32);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    int N = 100;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    std::string segment_path = test_dir_ + "/segment_stale_read_version";

    C_FLUSH_CONFIG_WITH_SCHEMA(first_config, schema);
    first_config.segment_path = segment_path.c_str();
    first_config.read_version = -1;
    first_config.retry_limit = 3;

    CFlushResult first_result{};
    auto status = FlushGrowingSegmentData(
        segment.get(), 0, 50, &first_config, &first_result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(first_result.num_rows, 50);
    auto acknowledged_version = first_result.committed_version;

    C_FLUSH_CONFIG_WITH_SCHEMA(orphan_config, schema);
    orphan_config.segment_path = segment_path.c_str();
    orphan_config.read_version = acknowledged_version;
    orphan_config.retry_limit = 3;

    CFlushResult orphan_result{};
    status = FlushGrowingSegmentData(
        segment.get(), 50, 75, &orphan_config, &orphan_result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(orphan_result.num_rows, 25);
    ASSERT_GT(orphan_result.committed_version, acknowledged_version);

    C_FLUSH_CONFIG_WITH_SCHEMA(retry_config, schema);
    retry_config.segment_path = segment_path.c_str();
    retry_config.read_version = acknowledged_version;
    retry_config.retry_limit = 3;

    CFlushResult retry_result{};
    status = FlushGrowingSegmentData(
        segment.get(), 50, 100, &retry_config, &retry_result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(retry_result.num_rows, 50);
    ASSERT_GT(retry_result.committed_version, orphan_result.committed_version);

    auto pk_datas = ReadFlushedFieldData(
        segment_path, retry_result, pk_fid, DataType::INT64, false, 0);
    int64_t total_rows = 0;
    for (const auto& data : pk_datas) {
        total_rows += data->get_num_rows();
    }
    EXPECT_EQ(total_rows, N);

    FreeFlushResult(&first_result);
    FreeFlushResult(&orphan_result);
    FreeFlushResult(&retry_result);
}

// test explicit writer format in flush config
TEST_F(FlushGrowingSegmentTest, FlushUsesWriterFormatFromConfig) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    int N = 10;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_writer_format";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.writer_format = LOON_FORMAT_PARQUET;

    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);
    AssertManifestColumnGroupsUseFormat(
        segment_path, result.committed_version, LOON_FORMAT_PARQUET);

    FreeFlushResult(&result);
}

// test flush with vector fields
TEST_F(FlushGrowingSegmentTest, FlushWithVectorFields) {
    // create schema with vector field
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid =
        schema->AddDebugField("vec", DataType::VECTOR_FLOAT, 128, "L2");
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 50;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_vec";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush data
    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with string/varchar fields
TEST_F(FlushGrowingSegmentTest, FlushWithStringFields) {
    // create schema with varchar field
    auto schema = std::make_shared<Schema>();
    auto str_fid = schema->AddDebugField("str_pk", DataType::VARCHAR);
    auto i64_fid = schema->AddDebugField("i64_field", DataType::INT64);
    schema->set_primary_field_id(str_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 30;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_str";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush data
    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with partial range (not from the beginning)
TEST_F(FlushGrowingSegmentTest, FlushPartialRange) {
    // create schema
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto i32_fid = schema->AddDebugField("i32_field", DataType::INT32);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 100;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_partial";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush only rows 20-50
    int start = 20;
    int end = 50;
    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), start, end, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, end - start);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with TEXT column config
TEST_F(FlushGrowingSegmentTest, FlushWithTextColumnConfig) {
#ifndef BUILD_VORTEX_BRIDGE
    GTEST_SKIP() << "Vortex support is not enabled";
#endif
    // create schema with varchar field (simulating TEXT)
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto text_fid = schema->AddDebugField("text_field", DataType::VARCHAR);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 20;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config with TEXT column
    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_text";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;

    // configure TEXT column
    int64_t text_field_ids[] = {text_fid.get()};
    std::string text_lob_path =
        test_dir_ + "/lobs_text/field_" + std::to_string(text_fid.get());
    const char* text_lob_paths[] = {text_lob_path.c_str()};
    config.text_field_ids = text_field_ids;
    config.text_lob_paths = text_lob_paths;
    config.num_text_columns = 1;

    // flush data
    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with nullable fields
TEST_F(FlushGrowingSegmentTest, FlushWithNullableFields) {
    // create schema with nullable field
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto nullable_fid =
        schema->AddDebugField("nullable_i32", DataType::INT32, true);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 50;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_nullable";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush data
    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushOrdinaryFieldSemanticsRoundTrip) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);

    DefaultValueType default_value;
    default_value.set_int_data(42);
    auto default_fid = schema->AddDebugFieldWithDefaultValue(
        "default_i32", DataType::INT32, default_value);

    auto nullable_str_fid =
        schema->AddDebugField("nullable_str", DataType::VARCHAR, true);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    auto array_fid =
        schema->AddDebugField("array", DataType::ARRAY, DataType::INT64);
    auto geometry_fid =
        schema->AddDebugField("geometry", DataType::GEOMETRY, true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {10, 11, 12};
    std::vector<Timestamp> timestamps = {100, 101, 102};
    std::vector<int64_t> pks = {1000, 1001, 1002};
    std::vector<std::string> nullable_strings = {"alpha", "", "gamma"};
    bool nullable_string_valid[N] = {true, false, true};
    std::vector<std::string> json_values = {
        R"({"k":1,"nested":{"v":"a"}})", "", R"({"k":3,"array":[1,2]})"};
    bool json_valid[N] = {true, false, true};

    std::vector<ScalarFieldProto> array_values(N);
    array_values[0].mutable_long_data()->add_data(1);
    array_values[0].mutable_long_data()->add_data(2);
    array_values[1].mutable_long_data()->add_data(3);
    array_values[2].mutable_long_data()->add_data(4);
    array_values[2].mutable_long_data()->add_data(5);
    array_values[2].mutable_long_data()->add_data(6);

    auto geos_ctx = GEOS_init_r();
    std::vector<std::string> geometry_values;
    geometry_values.emplace_back(
        Geometry(geos_ctx, "POINT (1 2)").to_wkb_string());
    geometry_values.emplace_back("");
    geometry_values.emplace_back(
        Geometry(geos_ctx, "POINT (3 4)").to_wkb_string());
    GEOS_finish_r(geos_ctx);
    bool geometry_valid[N] = {true, false, true};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(nullable_strings.data(),
                            nullable_string_valid,
                            N,
                            (*schema)[nullable_str_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            json_values.data(), json_valid, N, (*schema)[json_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            array_values.data(), nullptr, N, (*schema)[array_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            geometry_values.data(), geometry_valid, N, (*schema)[geometry_fid])
            .release());

    // default_i32 is intentionally absent from insert_data. Growing insert must
    // fill it exactly as the DataNode write-buffer path does.
    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_semantics";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    auto default_datas = ReadFlushedFieldData(
        segment_path, result, default_fid, DataType::INT32, true, 0);
    ASSERT_EQ(default_datas.size(), 1);
    ASSERT_EQ(default_datas[0]->get_num_rows(), N);
    ASSERT_EQ(default_datas[0]->get_valid_rows(), N);
    for (int i = 0; i < N; i++) {
        ASSERT_TRUE(default_datas[0]->is_valid(i));
        EXPECT_EQ(*static_cast<const int32_t*>(default_datas[0]->RawValue(i)),
                  42);
    }

    auto string_datas = ReadFlushedFieldData(
        segment_path, result, nullable_str_fid, DataType::VARCHAR, true, 0);
    ASSERT_EQ(string_datas.size(), 1);
    ASSERT_TRUE(string_datas[0]->is_valid(0));
    EXPECT_FALSE(string_datas[0]->is_valid(1));
    ASSERT_TRUE(string_datas[0]->is_valid(2));
    EXPECT_EQ(*static_cast<const std::string*>(string_datas[0]->RawValue(0)),
              "alpha");
    EXPECT_EQ(*static_cast<const std::string*>(string_datas[0]->RawValue(2)),
              "gamma");

    auto json_datas = ReadFlushedFieldData(
        segment_path, result, json_fid, DataType::JSON, true, 0);
    ASSERT_EQ(json_datas.size(), 1);
    ASSERT_TRUE(json_datas[0]->is_valid(0));
    EXPECT_FALSE(json_datas[0]->is_valid(1));
    ASSERT_TRUE(json_datas[0]->is_valid(2));
    EXPECT_EQ(std::string(
                  static_cast<const Json*>(json_datas[0]->RawValue(0))->data()),
              json_values[0]);
    EXPECT_EQ(std::string(
                  static_cast<const Json*>(json_datas[0]->RawValue(2))->data()),
              json_values[2]);

    auto array_datas = ReadFlushedFieldData(segment_path,
                                            result,
                                            array_fid,
                                            DataType::ARRAY,
                                            false,
                                            0,
                                            DataType::INT64);
    ASSERT_EQ(array_datas.size(), 1);
    auto row0_array =
        static_cast<const Array*>(array_datas[0]->RawValue(0))->output_data();
    auto row2_array =
        static_cast<const Array*>(array_datas[0]->RawValue(2))->output_data();
    EXPECT_EQ(row0_array.long_data().data().size(), 2);
    EXPECT_EQ(row0_array.long_data().data(0), 1);
    EXPECT_EQ(row0_array.long_data().data(1), 2);
    EXPECT_EQ(row2_array.long_data().data().size(), 3);
    EXPECT_EQ(row2_array.long_data().data(0), 4);
    EXPECT_EQ(row2_array.long_data().data(2), 6);

    auto geometry_datas = ReadFlushedFieldData(
        segment_path, result, geometry_fid, DataType::GEOMETRY, true, 0);
    ASSERT_EQ(geometry_datas.size(), 1);
    ASSERT_TRUE(geometry_datas[0]->is_valid(0));
    EXPECT_FALSE(geometry_datas[0]->is_valid(1));
    ASSERT_TRUE(geometry_datas[0]->is_valid(2));
    EXPECT_EQ(*static_cast<const std::string*>(geometry_datas[0]->RawValue(0)),
              geometry_values[0]);
    EXPECT_EQ(*static_cast<const std::string*>(geometry_datas[0]->RawValue(2)),
              geometry_values[2]);

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushTimestamptzPartialRangeRoundTrip) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto timestamptz_fid =
        schema->AddDebugField("event_time", DataType::TIMESTAMPTZ, true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 5;
    std::vector<int64_t> row_ids = {0, 1, 2, 3, 4};
    std::vector<Timestamp> timestamps = {100, 101, 102, 103, 104};
    std::vector<int64_t> pks = {10, 11, 12, 13, 14};
    std::vector<int64_t> event_times = {1700000000000,
                                        1700000001000,
                                        1700000002000,
                                        1700000003000,
                                        1700000004000};
    bool event_time_valid[N] = {true, false, true, false, true};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            event_times.data(), event_time_valid, N, (*schema)[timestamptz_fid])
            .release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_timestamptz";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result{};
    constexpr int64_t start = 1;
    constexpr int64_t end = 4;
    auto status =
        FlushGrowingSegmentData(segment.get(), start, end, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, end - start);

    auto field_datas = ReadFlushedFieldData(
        segment_path, result, timestamptz_fid, DataType::TIMESTAMPTZ, true, 0);
    ASSERT_EQ(field_datas.size(), 1);
    ASSERT_EQ(field_datas[0]->get_num_rows(), end - start);
    EXPECT_FALSE(field_datas[0]->is_valid(0));
    ASSERT_TRUE(field_datas[0]->is_valid(1));
    EXPECT_EQ(*static_cast<const int64_t*>(field_datas[0]->RawValue(1)),
              event_times[2]);
    EXPECT_FALSE(field_datas[0]->is_valid(2));

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushNullableScalarTypesPartialRangeRoundTrip) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto bool_fid = schema->AddDebugField("bool_field", DataType::BOOL, true);
    auto int8_fid = schema->AddDebugField("int8_field", DataType::INT8, true);
    auto int16_fid =
        schema->AddDebugField("int16_field", DataType::INT16, true);
    auto int64_fid =
        schema->AddDebugField("int64_field", DataType::INT64, true);
    auto float_fid =
        schema->AddDebugField("float_field", DataType::FLOAT, true);
    auto double_fid =
        schema->AddDebugField("double_field", DataType::DOUBLE, true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 5;
    std::vector<int64_t> row_ids = {0, 1, 2, 3, 4};
    std::vector<Timestamp> timestamps = {100, 101, 102, 103, 104};
    std::vector<int64_t> pks = {10, 11, 12, 13, 14};
    bool bool_values[N] = {true, false, true, false, true};
    std::vector<int8_t> int8_values = {-2, -1, 0, 1, 2};
    std::vector<int16_t> int16_values = {-200, -100, 0, 100, 200};
    std::vector<int64_t> int64_values = {1000, 1001, 1002, 1003, 1004};
    std::vector<float> float_values = {1.25F, 2.5F, 3.75F, 4.5F, 5.25F};
    std::vector<double> double_values = {10.25, 20.5, 30.75, 40.5, 50.25};
    bool valid_data[N] = {true, false, true, false, true};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(bool_values, valid_data, N, (*schema)[bool_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            int8_values.data(), valid_data, N, (*schema)[int8_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            int16_values.data(), valid_data, N, (*schema)[int16_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            int64_values.data(), valid_data, N, (*schema)[int64_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            float_values.data(), valid_data, N, (*schema)[float_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            double_values.data(), valid_data, N, (*schema)[double_fid])
            .release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_nullable_scalars";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result{};
    constexpr int64_t start = 1;
    constexpr int64_t end = 5;
    auto status =
        FlushGrowingSegmentData(segment.get(), start, end, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, end - start);

    auto bool_datas = ReadFlushedFieldData(
        segment_path, result, bool_fid, DataType::BOOL, true, 0);
    ASSERT_EQ(bool_datas.size(), 1);
    EXPECT_FALSE(bool_datas[0]->is_valid(0));
    ASSERT_TRUE(bool_datas[0]->is_valid(1));
    EXPECT_EQ(*static_cast<const bool*>(bool_datas[0]->RawValue(1)),
              bool_values[2]);
    EXPECT_FALSE(bool_datas[0]->is_valid(2));
    ASSERT_TRUE(bool_datas[0]->is_valid(3));
    EXPECT_EQ(*static_cast<const bool*>(bool_datas[0]->RawValue(3)),
              bool_values[4]);

    auto int8_datas = ReadFlushedFieldData(
        segment_path, result, int8_fid, DataType::INT8, true, 0);
    ASSERT_EQ(int8_datas.size(), 1);
    ASSERT_TRUE(int8_datas[0]->is_valid(1));
    EXPECT_EQ(*static_cast<const int8_t*>(int8_datas[0]->RawValue(1)),
              int8_values[2]);
    ASSERT_TRUE(int8_datas[0]->is_valid(3));
    EXPECT_EQ(*static_cast<const int8_t*>(int8_datas[0]->RawValue(3)),
              int8_values[4]);

    auto int16_datas = ReadFlushedFieldData(
        segment_path, result, int16_fid, DataType::INT16, true, 0);
    ASSERT_EQ(int16_datas.size(), 1);
    ASSERT_TRUE(int16_datas[0]->is_valid(1));
    EXPECT_EQ(*static_cast<const int16_t*>(int16_datas[0]->RawValue(1)),
              int16_values[2]);
    ASSERT_TRUE(int16_datas[0]->is_valid(3));
    EXPECT_EQ(*static_cast<const int16_t*>(int16_datas[0]->RawValue(3)),
              int16_values[4]);

    auto int64_datas = ReadFlushedFieldData(
        segment_path, result, int64_fid, DataType::INT64, true, 0);
    ASSERT_EQ(int64_datas.size(), 1);
    ASSERT_TRUE(int64_datas[0]->is_valid(1));
    EXPECT_EQ(*static_cast<const int64_t*>(int64_datas[0]->RawValue(1)),
              int64_values[2]);
    ASSERT_TRUE(int64_datas[0]->is_valid(3));
    EXPECT_EQ(*static_cast<const int64_t*>(int64_datas[0]->RawValue(3)),
              int64_values[4]);

    auto float_datas = ReadFlushedFieldData(
        segment_path, result, float_fid, DataType::FLOAT, true, 0);
    ASSERT_EQ(float_datas.size(), 1);
    ASSERT_TRUE(float_datas[0]->is_valid(1));
    EXPECT_FLOAT_EQ(*static_cast<const float*>(float_datas[0]->RawValue(1)),
                    float_values[2]);
    ASSERT_TRUE(float_datas[0]->is_valid(3));
    EXPECT_FLOAT_EQ(*static_cast<const float*>(float_datas[0]->RawValue(3)),
                    float_values[4]);

    auto double_datas = ReadFlushedFieldData(
        segment_path, result, double_fid, DataType::DOUBLE, true, 0);
    ASSERT_EQ(double_datas.size(), 1);
    ASSERT_TRUE(double_datas[0]->is_valid(1));
    EXPECT_DOUBLE_EQ(*static_cast<const double*>(double_datas[0]->RawValue(1)),
                     double_values[2]);
    ASSERT_TRUE(double_datas[0]->is_valid(3));
    EXPECT_DOUBLE_EQ(*static_cast<const double*>(double_datas[0]->RawValue(3)),
                     double_values[4]);

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushVectorArrayRoundTrip) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_array_fid = schema->AddDebugVectorArrayField(
        "emb_list", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 4;
    constexpr int array_len = 3;
    auto dataset = DataGen(schema, N, 42, 0, 1, array_len);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_vector_array";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    auto field_datas = ReadFlushedFieldData(segment_path,
                                            result,
                                            vec_array_fid,
                                            DataType::VECTOR_ARRAY,
                                            false,
                                            4,
                                            DataType::VECTOR_FLOAT);
    ASSERT_EQ(field_datas.size(), 1);
    ASSERT_EQ(field_datas[0]->get_num_rows(), N);

    auto expected = dataset.get_col<VectorFieldProto>(vec_array_fid);
    for (int i = 0; i < N; i++) {
        auto actual = static_cast<const milvus::VectorArray*>(
            field_datas[0]->RawValue(i));
        ASSERT_NE(actual, nullptr);
        EXPECT_EQ(actual->length(), array_len);
        EXPECT_EQ(actual->output_data().SerializeAsString(),
                  expected[i].SerializeAsString());
    }

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushNullableVectorArrayRoundTrip) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_array_fid = schema->AddDebugVectorArrayField(
        "emb_list", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2, true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 4;
    std::vector<int64_t> row_ids = {10, 11, 12, 13};
    std::vector<Timestamp> timestamps = {100, 101, 102, 103};
    std::vector<int64_t> pks = {1000, 1001, 1002, 1003};
    std::vector<VectorFieldProto> vec_arrays(N);

    auto fill_float_vectors = [](VectorFieldProto& vector_array,
                                 std::initializer_list<float> values) {
        vector_array.set_dim(4);
        vector_array.mutable_float_vector()->mutable_data()->Add(values.begin(),
                                                                 values.end());
    };

    fill_float_vectors(vec_arrays[0], {1.0F, 2.0F, 3.0F, 4.0F});
    vec_arrays[1].set_dim(4);
    vec_arrays[1].mutable_float_vector();
    fill_float_vectors(vec_arrays[2],
                       {5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F});
    vec_arrays[3].set_dim(4);
    vec_arrays[3].mutable_float_vector();
    bool valid_data[N] = {true, false, true, false};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            vec_arrays.data(), valid_data, N, (*schema)[vec_array_fid])
            .release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_nullable_vector_array";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    auto field_datas = ReadFlushedFieldData(segment_path,
                                            result,
                                            vec_array_fid,
                                            DataType::VECTOR_ARRAY,
                                            true,
                                            4,
                                            DataType::VECTOR_FLOAT);
    ASSERT_EQ(field_datas.size(), 1);
    ASSERT_EQ(field_datas[0]->get_num_rows(), N);
    ASSERT_EQ(field_datas[0]->get_valid_rows(), 2);
    EXPECT_TRUE(field_datas[0]->is_valid(0));
    EXPECT_FALSE(field_datas[0]->is_valid(1));
    EXPECT_TRUE(field_datas[0]->is_valid(2));
    EXPECT_FALSE(field_datas[0]->is_valid(3));

    auto vector_array_data =
        std::dynamic_pointer_cast<milvus::FieldData<milvus::VectorArray>>(
            field_datas[0]);
    ASSERT_NE(vector_array_data, nullptr);
    ASSERT_EQ(vector_array_data->value_at(0)->output_data().SerializeAsString(),
              vec_arrays[0].SerializeAsString());
    ASSERT_EQ(vector_array_data->value_at(1)->output_data().SerializeAsString(),
              vec_arrays[2].SerializeAsString());

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushVectorArrayElementTypesRoundTrip) {
    struct Case {
        DataType element_type;
        int64_t dim;
        std::string metric_type;
        std::string segment_suffix;
    };

    std::vector<Case> cases = {
        {DataType::VECTOR_BINARY, 16, "JACCARD", "binary"},
        {DataType::VECTOR_FLOAT16, 4, "L2", "fp16"},
        {DataType::VECTOR_BFLOAT16, 4, "L2", "bf16"},
        {DataType::VECTOR_INT8, 4, "L2", "int8"},
    };

    for (const auto& test_case : cases) {
        auto schema = std::make_shared<Schema>();
        auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
        auto vec_array_fid =
            schema->AddDebugVectorArrayField("emb_list",
                                             test_case.element_type,
                                             test_case.dim,
                                             test_case.metric_type);
        schema->set_primary_field_id(pk_fid);

        auto segment = CreateGrowingSegment(schema, empty_index_meta);
        ASSERT_NE(segment, nullptr);

        constexpr int N = 4;
        constexpr int array_len = 3;
        auto dataset = DataGen(schema, N, 42, 0, 1, array_len);
        segment->PreInsert(N);
        segment->Insert(0,
                        N,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);

        C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
        std::string segment_path =
            test_dir_ + "/segment_vector_array_" + test_case.segment_suffix;
        config.segment_path = segment_path.c_str();
        config.read_version = -1;
        config.retry_limit = 3;
        config.text_field_ids = nullptr;
        config.text_lob_paths = nullptr;
        config.num_text_columns = 0;

        CFlushResult result{};
        auto status =
            FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

        ASSERT_EQ(status.error_code, Success) << status.error_msg;
        ASSERT_EQ(result.num_rows, N);

        auto field_datas = ReadFlushedFieldData(segment_path,
                                                result,
                                                vec_array_fid,
                                                DataType::VECTOR_ARRAY,
                                                false,
                                                test_case.dim,
                                                test_case.element_type);
        ASSERT_EQ(field_datas.size(), 1);
        ASSERT_EQ(field_datas[0]->get_num_rows(), N);

        auto expected = dataset.get_col<VectorFieldProto>(vec_array_fid);
        for (int i = 0; i < N; i++) {
            auto actual = static_cast<const milvus::VectorArray*>(
                field_datas[0]->RawValue(i));
            ASSERT_NE(actual, nullptr);
            EXPECT_EQ(actual->length(), array_len);
            EXPECT_EQ(actual->get_element_type(), test_case.element_type);
            EXPECT_EQ(actual->output_data().SerializeAsString(),
                      expected[i].SerializeAsString());
        }

        FreeFlushResult(&result);
    }
}

TEST_F(FlushGrowingSegmentTest, FlushStringAndTextRoundTrip) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto string_fid = schema->AddDebugField("str", DataType::STRING, true);
    auto text_fid = schema->AddDebugField("text", DataType::TEXT, true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};
    std::vector<std::string> strings = {"plain-string", "", "tail-string"};
    bool string_valid[N] = {true, false, true};
    std::vector<std::string> texts = {
        "short text", "", std::string(512, 'x') + "-tail"};
    bool text_valid[N] = {true, false, true};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            strings.data(), string_valid, N, (*schema)[string_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(texts.data(), text_valid, N, (*schema)[text_fid])
            .release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_string_text";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    auto string_datas = ReadFlushedFieldData(
        segment_path, result, string_fid, DataType::STRING, true, 0);
    ASSERT_EQ(string_datas.size(), 1);
    ASSERT_TRUE(string_datas[0]->is_valid(0));
    EXPECT_FALSE(string_datas[0]->is_valid(1));
    ASSERT_TRUE(string_datas[0]->is_valid(2));
    EXPECT_EQ(*static_cast<const std::string*>(string_datas[0]->RawValue(0)),
              strings[0]);
    EXPECT_EQ(*static_cast<const std::string*>(string_datas[0]->RawValue(2)),
              strings[2]);

    auto text_datas = ReadFlushedFieldData(
        segment_path, result, text_fid, DataType::TEXT, true, 0);
    ASSERT_EQ(text_datas.size(), 1);
    ASSERT_TRUE(text_datas[0]->is_valid(0));
    EXPECT_FALSE(text_datas[0]->is_valid(1));
    ASSERT_TRUE(text_datas[0]->is_valid(2));
    EXPECT_EQ(*static_cast<const std::string*>(text_datas[0]->RawValue(0)),
              texts[0]);
    EXPECT_EQ(*static_cast<const std::string*>(text_datas[0]->RawValue(2)),
              texts[2]);

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushArrayElementTypesRoundTrip) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto int_array_fid =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT32);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    auto double_array_fid = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE, true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};
    std::vector<ScalarFieldProto> bool_arrays(N);
    bool_arrays[0].mutable_bool_data()->add_data(true);
    bool_arrays[0].mutable_bool_data()->add_data(false);
    bool_arrays[1].mutable_bool_data()->add_data(false);
    bool_arrays[2].mutable_bool_data()->add_data(true);
    bool_arrays[2].mutable_bool_data()->add_data(true);

    std::vector<ScalarFieldProto> int_arrays(N);
    int_arrays[0].mutable_int_data()->add_data(1);
    int_arrays[0].mutable_int_data()->add_data(2);
    int_arrays[1].mutable_int_data()->add_data(3);
    int_arrays[2].mutable_int_data()->add_data(4);
    int_arrays[2].mutable_int_data()->add_data(5);

    std::vector<ScalarFieldProto> string_arrays(N);
    string_arrays[0].mutable_string_data()->add_data("a");
    string_arrays[0].mutable_string_data()->add_data("b");
    string_arrays[1].mutable_string_data()->add_data("c");
    string_arrays[2].mutable_string_data()->add_data("d");
    string_arrays[2].mutable_string_data()->add_data("e");

    std::vector<ScalarFieldProto> double_arrays(N);
    double_arrays[0].mutable_double_data()->add_data(1.25);
    double_arrays[0].mutable_double_data()->add_data(2.5);
    double_arrays[1].mutable_double_data()->add_data(3.75);
    double_arrays[2].mutable_double_data()->add_data(4.5);
    bool double_valid[N] = {true, false, true};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            bool_arrays.data(), nullptr, N, (*schema)[bool_array_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            int_arrays.data(), nullptr, N, (*schema)[int_array_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            string_arrays.data(), nullptr, N, (*schema)[string_array_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            double_arrays.data(), double_valid, N, (*schema)[double_array_fid])
            .release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_arrays";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    auto bool_datas = ReadFlushedFieldData(segment_path,
                                           result,
                                           bool_array_fid,
                                           DataType::ARRAY,
                                           false,
                                           0,
                                           DataType::BOOL);
    ASSERT_EQ(bool_datas.size(), 1);
    EXPECT_EQ(static_cast<const Array*>(bool_datas[0]->RawValue(0))
                  ->output_data()
                  .SerializeAsString(),
              bool_arrays[0].SerializeAsString());
    EXPECT_EQ(static_cast<const Array*>(bool_datas[0]->RawValue(2))
                  ->output_data()
                  .SerializeAsString(),
              bool_arrays[2].SerializeAsString());

    auto int_datas = ReadFlushedFieldData(segment_path,
                                          result,
                                          int_array_fid,
                                          DataType::ARRAY,
                                          false,
                                          0,
                                          DataType::INT32);
    ASSERT_EQ(int_datas.size(), 1);
    EXPECT_EQ(static_cast<const Array*>(int_datas[0]->RawValue(0))
                  ->output_data()
                  .SerializeAsString(),
              int_arrays[0].SerializeAsString());
    EXPECT_EQ(static_cast<const Array*>(int_datas[0]->RawValue(2))
                  ->output_data()
                  .SerializeAsString(),
              int_arrays[2].SerializeAsString());

    auto string_datas = ReadFlushedFieldData(segment_path,
                                             result,
                                             string_array_fid,
                                             DataType::ARRAY,
                                             false,
                                             0,
                                             DataType::VARCHAR);
    ASSERT_EQ(string_datas.size(), 1);
    EXPECT_EQ(static_cast<const Array*>(string_datas[0]->RawValue(0))
                  ->output_data()
                  .SerializeAsString(),
              string_arrays[0].SerializeAsString());
    EXPECT_EQ(static_cast<const Array*>(string_datas[0]->RawValue(2))
                  ->output_data()
                  .SerializeAsString(),
              string_arrays[2].SerializeAsString());

    auto double_datas = ReadFlushedFieldData(segment_path,
                                             result,
                                             double_array_fid,
                                             DataType::ARRAY,
                                             true,
                                             0,
                                             DataType::DOUBLE);
    ASSERT_EQ(double_datas.size(), 1);
    ASSERT_TRUE(double_datas[0]->is_valid(0));
    EXPECT_FALSE(double_datas[0]->is_valid(1));
    ASSERT_TRUE(double_datas[0]->is_valid(2));
    EXPECT_EQ(static_cast<const Array*>(double_datas[0]->RawValue(0))
                  ->output_data()
                  .SerializeAsString(),
              double_arrays[0].SerializeAsString());
    EXPECT_EQ(static_cast<const Array*>(double_datas[0]->RawValue(2))
                  ->output_data()
                  .SerializeAsString(),
              double_arrays[2].SerializeAsString());

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushNullableFloatVectorKeepsCompactMapping) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid =
        schema->AddDebugField("vec", DataType::VECTOR_FLOAT, 2, "L2", true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};
    bool valid_data[N] = {false, true, true};
    std::vector<float> compact_vectors = {1.0F, 2.0F, 3.0F, 4.0F};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    auto vec_array = CreateVectorDataArrayFrom(
        compact_vectors.data(), valid_data, N, 2, (*schema)[vec_fid]);
    insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_nullable_vec";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_NE(result.manifest_path, nullptr);
    ASSERT_EQ(result.num_rows, N);

    auto field_datas = ReadFlushedFieldData(
        segment_path, result, vec_fid, DataType::VECTOR_FLOAT, true, 2);
    ASSERT_EQ(field_datas.size(), 1);
    auto field_data = field_datas[0];
    ASSERT_EQ(field_data->get_num_rows(), N);
    ASSERT_EQ(field_data->get_valid_rows(), 2);
    EXPECT_FALSE(field_data->is_valid(0));
    ASSERT_TRUE(field_data->is_valid(1));
    ASSERT_TRUE(field_data->is_valid(2));

    auto row1 = static_cast<const float*>(field_data->RawValue(1));
    auto row2 = static_cast<const float*>(field_data->RawValue(2));
    ASSERT_NE(row1, nullptr);
    ASSERT_NE(row2, nullptr);
    EXPECT_FLOAT_EQ(row1[0], 1.0F);
    EXPECT_FLOAT_EQ(row1[1], 2.0F);
    EXPECT_FLOAT_EQ(row2[0], 3.0F);
    EXPECT_FLOAT_EQ(row2[1], 4.0F);

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushRejectsEndOffsetBeyondRowCount) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid =
        schema->AddDebugField("vec", DataType::VECTOR_FLOAT, 2, "L2");
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    auto dataset = DataGen(schema, N);
    auto offset = segment->PreInsert(N);
    segment->Insert(offset,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_offset_out_of_range";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N + 1, &config, &result);

    EXPECT_NE(status.error_code, Success);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("exceeds growing segment"),
              std::string::npos);
    free(const_cast<char*>(status.error_msg));

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushFloatVectorFromIndexAfterChunksCleared) {
    constexpr int64_t dim = 4;
    constexpr int64_t row_count = 100;
    constexpr int64_t start = 13;
    constexpr int64_t end = 87;

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    InterimIndexConfigForTest interim_config;
    interim_config.chunk_rows = 16;
    interim_config.nlist = 1;
    interim_config.nprobe = 1;
    interim_config.dense_vector_interim_index_type =
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
    interim_config.sub_dim = dim;
    interim_config.refine_ratio = 1.0F;
    interim_config.refine_quant_type = "NONE";
    interim_config.refine_with_quant_flag = false;
    ApplyInterimIndexConfigForTest(interim_config, config);
    config.set_storage_v3_enabled(true);
    config.set_enable_growing_source_flush(true);

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    schema->set_primary_field_id(pk_fid);

    auto index_meta =
        MakeVectorIndexMeta(vec_fid,
                            dim,
                            knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                            knowhere::metric::L2);
    auto segment = CreateGrowingSegment(schema, index_meta, 1, config);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto dataset = DataGen(schema, row_count);
    auto offset = segment->PreInsert(row_count);
    segment->Insert(offset,
                    row_count,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    auto vec_base = segment_impl->get_insert_record().get_data_base(vec_fid);
    ASSERT_NE(vec_base, nullptr);
    ASSERT_EQ(vec_base->num_chunk(), 0);
    ASSERT_TRUE(segment_impl->CanReadRawVectorFromIndex(vec_fid));

    C_FLUSH_CONFIG_WITH_SCHEMA(flush_config, schema);
    std::string segment_path = test_dir_ + "/segment_index_fallback_float";
    flush_config.segment_path = segment_path.c_str();
    flush_config.read_version = -1;
    flush_config.retry_limit = 3;

    CFlushResult result{};
    auto status = FlushGrowingSegmentData(
        segment.get(), start, end, &flush_config, &result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, end - start);

    auto field_datas = ReadFlushedFieldData(
        segment_path, result, vec_fid, DataType::VECTOR_FLOAT, false, dim);
    ASSERT_EQ(field_datas.size(), 1);
    auto field_data = field_datas[0];
    ASSERT_EQ(field_data->get_num_rows(), end - start);
    auto source_vectors = dataset.get_col<float>(vec_fid);
    for (int64_t i = 0; i < end - start; i++) {
        auto row = static_cast<const float*>(field_data->RawValue(i));
        ASSERT_NE(row, nullptr);
        for (int64_t d = 0; d < dim; d++) {
            EXPECT_FLOAT_EQ(row[d], source_vectors[(start + i) * dim + d]);
        }
    }

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest,
       FlushNullableFloatVectorFromIndexKeepsCompactMapping) {
    constexpr int64_t dim = 4;
    constexpr int64_t row_count = 100;
    constexpr int64_t start = 13;
    constexpr int64_t end = 87;

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    InterimIndexConfigForTest interim_config;
    interim_config.chunk_rows = 16;
    interim_config.nlist = 1;
    interim_config.nprobe = 1;
    interim_config.dense_vector_interim_index_type =
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
    interim_config.sub_dim = dim;
    interim_config.refine_ratio = 1.0F;
    interim_config.refine_quant_type = "NONE";
    interim_config.refine_with_quant_flag = false;
    ApplyInterimIndexConfigForTest(interim_config, config);
    config.set_storage_v3_enabled(true);
    config.set_enable_growing_source_flush(true);

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2, true);
    schema->set_primary_field_id(pk_fid);

    auto index_meta =
        MakeVectorIndexMeta(vec_fid,
                            dim,
                            knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                            knowhere::metric::L2);
    auto segment = CreateGrowingSegment(schema, index_meta, 1, config);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    std::vector<int64_t> row_ids(row_count);
    std::vector<Timestamp> timestamps(row_count);
    std::vector<int64_t> pks(row_count);
    std::unique_ptr<bool[]> valid(new bool[row_count]);
    std::vector<float> compact_vectors;
    compact_vectors.reserve(row_count * dim);
    for (int64_t i = 0; i < row_count; i++) {
        row_ids[i] = i;
        timestamps[i] = 1000 + i;
        pks[i] = 10000 + i;
        valid[i] = i % 7 != 0;
        if (valid[i]) {
            for (int64_t d = 0; d < dim; d++) {
                compact_vectors.push_back(static_cast<float>(i * 10 + d));
            }
        }
    }

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(row_count);
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, row_count, (*schema)[pk_fid]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    auto vec_array = CreateVectorDataArrayFrom(compact_vectors.data(),
                                               valid.get(),
                                               row_count,
                                               compact_vectors.size() / dim,
                                               (*schema)[vec_fid]);
    insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

    segment->PreInsert(row_count);
    segment->Insert(
        0, row_count, row_ids.data(), timestamps.data(), insert_data.get());
    auto vec_base = segment_impl->get_insert_record().get_data_base(vec_fid);
    ASSERT_NE(vec_base, nullptr);
    ASSERT_EQ(vec_base->num_chunk(), 0);
    ASSERT_TRUE(segment_impl->CanReadRawVectorFromIndex(vec_fid));

    C_FLUSH_CONFIG_WITH_SCHEMA(flush_config, schema);
    std::string segment_path = test_dir_ + "/segment_index_fallback_nullable";
    flush_config.segment_path = segment_path.c_str();
    flush_config.read_version = -1;
    flush_config.retry_limit = 3;

    CFlushResult result{};
    auto status = FlushGrowingSegmentData(
        segment.get(), start, end, &flush_config, &result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, end - start);

    auto field_datas = ReadFlushedFieldData(
        segment_path, result, vec_fid, DataType::VECTOR_FLOAT, true, dim);
    ASSERT_EQ(field_datas.size(), 1);
    auto field_data = field_datas[0];
    ASSERT_EQ(field_data->get_num_rows(), end - start);
    for (int64_t i = 0; i < end - start; i++) {
        auto logical = start + i;
        EXPECT_EQ(field_data->is_valid(i), valid[logical]);
        if (!valid[logical]) {
            continue;
        }
        auto row = static_cast<const float*>(field_data->RawValue(i));
        ASSERT_NE(row, nullptr);
        for (int64_t d = 0; d < dim; d++) {
            EXPECT_FLOAT_EQ(row[d], static_cast<float>(logical * 10 + d));
        }
    }

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest,
       FlushSparseVectorAndBM25FromIndexAfterChunksCleared) {
    constexpr int64_t row_count = 64;
    constexpr int64_t start = 5;
    constexpr int64_t end = 25;

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    InterimIndexConfigForTest interim_config;
    interim_config.chunk_rows = 16;
    interim_config.nlist = 1;
    interim_config.nprobe = 1;
    ApplyInterimIndexConfigForTest(interim_config, config);
    config.set_storage_v3_enabled(true);
    config.set_enable_growing_source_flush(true);

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto sparse_fid = schema->AddDebugField(
        "bm25", DataType::VECTOR_SPARSE_U32_F32, 0, knowhere::metric::IP);
    schema->set_primary_field_id(pk_fid);

    auto index_meta =
        MakeVectorIndexMeta(sparse_fid,
                            0,
                            knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                            knowhere::metric::IP);
    auto segment = CreateGrowingSegment(schema, index_meta, 1, config);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    std::vector<int64_t> row_ids(row_count);
    std::vector<Timestamp> timestamps(row_count);
    std::vector<int64_t> pks(row_count);
    auto sparse_vectors =
        std::make_unique<knowhere::sparse::SparseRow<SparseValueType>[]>(
            row_count);
    std::unordered_map<uint32_t, int32_t> expected_rows_with_token;
    int64_t expected_num_token = 0;
    for (int64_t i = 0; i < row_count; i++) {
        row_ids[i] = i;
        timestamps[i] = 1000 + i;
        pks[i] = 10000 + i;
        sparse_vectors[i] = knowhere::sparse::SparseRow<SparseValueType>(2);
        auto common_token = static_cast<uint32_t>(10 + i % 3);
        auto unique_token = static_cast<uint32_t>(100 + i);
        sparse_vectors[i].set_at(0, common_token, 1.0F);
        sparse_vectors[i].set_at(1, unique_token, 2.0F);
        if (i >= start && i < end) {
            expected_rows_with_token[common_token]++;
            expected_rows_with_token[unique_token]++;
            expected_num_token += 3;
        }
    }

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(row_count);
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, row_count, (*schema)[pk_fid]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    auto sparse_array = CreateVectorDataArrayFrom(sparse_vectors.get(),
                                                  nullptr,
                                                  row_count,
                                                  row_count,
                                                  (*schema)[sparse_fid]);
    insert_data->mutable_fields_data()->AddAllocated(sparse_array.release());

    segment->PreInsert(row_count);
    segment->Insert(
        0, row_count, row_ids.data(), timestamps.data(), insert_data.get());
    auto vec_base = segment_impl->get_insert_record().get_data_base(sparse_fid);
    ASSERT_NE(vec_base, nullptr);
    ASSERT_EQ(vec_base->num_chunk(), 0);
    ASSERT_TRUE(segment_impl->CanReadRawVectorFromIndex(sparse_fid));

    int64_t bm25_field_ids[] = {sparse_fid.get()};
    int64_t bm25_stats_log_ids[] = {2001};
    C_FLUSH_CONFIG_WITH_SCHEMA(flush_config, schema);
    std::string segment_path = test_dir_ + "/segment_index_fallback_sparse";
    flush_config.segment_path = segment_path.c_str();
    flush_config.read_version = -1;
    flush_config.retry_limit = 3;
    flush_config.bm25_field_ids = bm25_field_ids;
    flush_config.bm25_stats_log_ids = bm25_stats_log_ids;
    flush_config.num_bm25_fields = 1;

    CFlushResult result{};
    auto status = FlushGrowingSegmentData(
        segment.get(), start, end, &flush_config, &result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, end - start);

    auto field_datas = ReadFlushedFieldData(segment_path,
                                            result,
                                            sparse_fid,
                                            DataType::VECTOR_SPARSE_U32_F32,
                                            false,
                                            0);
    ASSERT_EQ(field_datas.size(), 1);
    auto field_data = field_datas[0];
    ASSERT_EQ(field_data->get_num_rows(), end - start);
    for (int64_t i = 0; i < end - start; i++) {
        auto row =
            static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
                field_data->RawValue(i));
        ASSERT_NE(row, nullptr);
        EXPECT_EQ(row->data_byte_size(),
                  sparse_vectors[start + i].data_byte_size());
    }

    auto stats = ParseBM25StatsFromResult(result, sparse_fid);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->num_row, end - start);
    EXPECT_EQ(stats->num_token, expected_num_token);
    EXPECT_EQ(stats->rows_with_token, expected_rows_with_token);

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushPrimaryKeyStatsManifestAndCompound) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid =
        schema->AddDebugField("vec", DataType::VECTOR_FLOAT, 2, "L2");
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 4;
    auto dataset = DataGen(schema, N);
    auto offset = segment->PreInsert(N);
    segment->Insert(offset,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    std::string segment_path = test_dir_ + "/segment_pk_stats";
    std::vector<uint8_t> first_pk_blob = {'s', 'i', 'n', 'g', 'l', 'e', 'A'};
    std::vector<uint8_t> second_pk_blob = {'s', 'i', 'n', 'g', 'l', 'e', 'B'};
    std::vector<uint8_t> merged_pk_blob = {'m', 'e', 'r', 'g', 'e', 'd'};

    C_FLUSH_CONFIG_WITH_SCHEMA(first_config, schema);
    first_config.segment_path = segment_path.c_str();
    first_config.read_version = -1;
    first_config.retry_limit = 3;
    first_config.pk_stats_field_id = pk_fid.get();
    first_config.pk_stats_log_id = 101;
    first_config.pk_stats_blob = first_pk_blob.data();
    first_config.pk_stats_blob_size = first_pk_blob.size();

    CFlushResult first_result{};
    auto status = FlushGrowingSegmentData(
        segment.get(), 0, 2, &first_config, &first_result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(first_result.num_rows, 2);

    auto stat_key = "bloom_filter." + std::to_string(pk_fid.get());
    auto first_paths = ManifestStatPaths(
        segment_path, first_result.committed_version, stat_key);
    ASSERT_EQ(first_paths.size(), 1);
    auto first_full_path = segment_path + "/_stats/bloom_filter." +
                           std::to_string(pk_fid.get()) + "/101";
    EXPECT_EQ(first_paths[0], first_full_path);
    EXPECT_EQ(ReadRawFile(first_full_path), first_pk_blob);

    C_FLUSH_CONFIG_WITH_SCHEMA(second_config, schema);
    second_config.segment_path = segment_path.c_str();
    second_config.read_version = first_result.committed_version;
    second_config.retry_limit = 3;
    second_config.pk_stats_field_id = pk_fid.get();
    second_config.pk_stats_log_id = 102;
    second_config.pk_stats_blob = second_pk_blob.data();
    second_config.pk_stats_blob_size = second_pk_blob.size();
    second_config.merged_pk_stats_blob = merged_pk_blob.data();
    second_config.merged_pk_stats_blob_size = merged_pk_blob.size();

    CFlushResult second_result{};
    status = FlushGrowingSegmentData(
        segment.get(), 2, 4, &second_config, &second_result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(second_result.num_rows, 2);

    auto second_paths = ManifestStatPaths(
        segment_path, second_result.committed_version, stat_key);
    ASSERT_EQ(second_paths.size(), 3);

    auto stats_prefix = segment_path + "/_stats/bloom_filter." +
                        std::to_string(pk_fid.get()) + "/";
    auto second_full_path = stats_prefix + "102";
    auto merged_full_path = stats_prefix + "1";
    EXPECT_NE(
        std::find(second_paths.begin(), second_paths.end(), first_full_path),
        second_paths.end());
    EXPECT_NE(
        std::find(second_paths.begin(), second_paths.end(), second_full_path),
        second_paths.end());
    EXPECT_NE(
        std::find(second_paths.begin(), second_paths.end(), merged_full_path),
        second_paths.end());
    EXPECT_EQ(ReadRawFile(second_full_path), second_pk_blob);
    EXPECT_EQ(ReadRawFile(merged_full_path), merged_pk_blob);

    FreeFlushResult(&first_result);
    FreeFlushResult(&second_result);
}

TEST_F(FlushGrowingSegmentTest, FlushNullableInt8VectorKeepsCompactMapping) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid =
        schema->AddDebugField("vec", DataType::VECTOR_INT8, 4, "L2", true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};
    bool valid_data[N] = {true, false, true};
    std::vector<int8> compact_vectors = {1, 2, 3, 4, 5, 6, 7, 8};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    auto vec_array = CreateVectorDataArrayFrom(
        compact_vectors.data(), valid_data, N, 2, (*schema)[vec_fid]);
    insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_int8_vec";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    auto field_datas = ReadFlushedFieldData(
        segment_path, result, vec_fid, DataType::VECTOR_INT8, true, 4);
    ASSERT_EQ(field_datas.size(), 1);
    auto field_data = field_datas[0];
    ASSERT_EQ(field_data->get_num_rows(), N);
    ASSERT_EQ(field_data->get_valid_rows(), 2);
    ASSERT_TRUE(field_data->is_valid(0));
    EXPECT_FALSE(field_data->is_valid(1));
    ASSERT_TRUE(field_data->is_valid(2));

    auto row0 = static_cast<const int8*>(field_data->RawValue(0));
    auto row2 = static_cast<const int8*>(field_data->RawValue(2));
    ASSERT_NE(row0, nullptr);
    ASSERT_NE(row2, nullptr);
    EXPECT_EQ(std::vector<int8>(row0, row0 + 4),
              std::vector<int8>({1, 2, 3, 4}));
    EXPECT_EQ(std::vector<int8>(row2, row2 + 4),
              std::vector<int8>({5, 6, 7, 8}));

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushNullableFixedWidthVectorTypesRoundTrip) {
    struct Case {
        DataType data_type;
        int64_t dim;
        std::string metric_type;
        std::string segment_suffix;
    };

    std::vector<Case> cases = {
        {DataType::VECTOR_BINARY, 16, "JACCARD", "binary"},
        {DataType::VECTOR_FLOAT16, 2, "L2", "fp16"},
        {DataType::VECTOR_BFLOAT16, 2, "L2", "bf16"},
    };

    for (const auto& test_case : cases) {
        auto schema = std::make_shared<Schema>();
        auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
        auto vec_fid = schema->AddDebugField("vec",
                                             test_case.data_type,
                                             test_case.dim,
                                             test_case.metric_type,
                                             true);
        schema->set_primary_field_id(pk_fid);

        auto segment = CreateGrowingSegment(schema, empty_index_meta);
        ASSERT_NE(segment, nullptr);

        constexpr int N = 3;
        std::vector<int64_t> row_ids = {0, 1, 2};
        std::vector<Timestamp> timestamps = {10, 11, 12};
        std::vector<int64_t> pks = {100, 101, 102};
        bool valid_data[N] = {true, false, true};

        auto insert_data = std::make_unique<InsertRecordProto>();
        insert_data->set_num_rows(N);
        auto pk_array =
            CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid]);
        insert_data->mutable_fields_data()->AddAllocated(pk_array.release());

        std::vector<uint8_t> binary_vectors = {0x01, 0x02, 0xA0, 0xB0};
        std::vector<float16> fp16_vectors = {1, 2, 3, 4};
        std::vector<bfloat16> bf16_vectors = {5, 6, 7, 8};
        const void* vector_data = nullptr;
        switch (test_case.data_type) {
            case DataType::VECTOR_BINARY:
                vector_data = binary_vectors.data();
                break;
            case DataType::VECTOR_FLOAT16:
                vector_data = fp16_vectors.data();
                break;
            case DataType::VECTOR_BFLOAT16:
                vector_data = bf16_vectors.data();
                break;
            default:
                FAIL() << "unexpected vector type";
        }

        auto vec_array = CreateVectorDataArrayFrom(
            vector_data, valid_data, N, 2, (*schema)[vec_fid]);
        insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

        segment->PreInsert(N);
        segment->Insert(
            0, N, row_ids.data(), timestamps.data(), insert_data.get());

        C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
        std::string segment_path =
            test_dir_ + "/segment_nullable_" + test_case.segment_suffix;
        config.segment_path = segment_path.c_str();
        config.read_version = -1;
        config.retry_limit = 3;
        config.text_field_ids = nullptr;
        config.text_lob_paths = nullptr;
        config.num_text_columns = 0;

        CFlushResult result{};
        auto status =
            FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

        ASSERT_EQ(status.error_code, Success) << status.error_msg;
        ASSERT_EQ(result.num_rows, N);

        auto field_datas = ReadFlushedFieldData(segment_path,
                                                result,
                                                vec_fid,
                                                test_case.data_type,
                                                true,
                                                test_case.dim);
        ASSERT_EQ(field_datas.size(), 1);
        auto field_data = field_datas[0];
        ASSERT_EQ(field_data->get_num_rows(), N);
        ASSERT_EQ(field_data->get_valid_rows(), 2);
        ASSERT_TRUE(field_data->is_valid(0));
        EXPECT_FALSE(field_data->is_valid(1));
        ASSERT_TRUE(field_data->is_valid(2));

        switch (test_case.data_type) {
            case DataType::VECTOR_BINARY: {
                auto row0 =
                    static_cast<const uint8_t*>(field_data->RawValue(0));
                auto row2 =
                    static_cast<const uint8_t*>(field_data->RawValue(2));
                ASSERT_NE(row0, nullptr);
                ASSERT_NE(row2, nullptr);
                EXPECT_EQ(std::vector<uint8_t>(row0, row0 + 2),
                          std::vector<uint8_t>({0x01, 0x02}));
                EXPECT_EQ(std::vector<uint8_t>(row2, row2 + 2),
                          std::vector<uint8_t>({0xA0, 0xB0}));
                break;
            }
            case DataType::VECTOR_FLOAT16: {
                auto row0 =
                    static_cast<const float16*>(field_data->RawValue(0));
                auto row2 =
                    static_cast<const float16*>(field_data->RawValue(2));
                ASSERT_NE(row0, nullptr);
                ASSERT_NE(row2, nullptr);
                EXPECT_EQ(std::vector<float16>(row0, row0 + 2),
                          std::vector<float16>({1, 2}));
                EXPECT_EQ(std::vector<float16>(row2, row2 + 2),
                          std::vector<float16>({3, 4}));
                break;
            }
            case DataType::VECTOR_BFLOAT16: {
                auto row0 =
                    static_cast<const bfloat16*>(field_data->RawValue(0));
                auto row2 =
                    static_cast<const bfloat16*>(field_data->RawValue(2));
                ASSERT_NE(row0, nullptr);
                ASSERT_NE(row2, nullptr);
                EXPECT_EQ(std::vector<bfloat16>(row0, row0 + 2),
                          std::vector<bfloat16>({5, 6}));
                EXPECT_EQ(std::vector<bfloat16>(row2, row2 + 2),
                          std::vector<bfloat16>({7, 8}));
                break;
            }
            default:
                FAIL() << "unexpected vector type";
        }

        FreeFlushResult(&result);
    }
}

TEST_F(FlushGrowingSegmentTest, FlushNullableSparseVectorKeepsCompactMapping) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "vec", DataType::VECTOR_SPARSE_U32_F32, 0, std::nullopt, true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};
    bool valid_data[N] = {false, true, true};
    auto sparse_vectors = GenerateRandomSparseFloatVector(2, 16, 0.5);

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    auto vec_array = CreateVectorDataArrayFrom(
        sparse_vectors.get(), valid_data, N, 2, (*schema)[vec_fid]);
    insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_sparse_vec";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    auto field_datas = ReadFlushedFieldData(segment_path,
                                            result,
                                            vec_fid,
                                            DataType::VECTOR_SPARSE_U32_F32,
                                            true,
                                            0);
    ASSERT_EQ(field_datas.size(), 1);
    auto field_data = field_datas[0];
    ASSERT_EQ(field_data->get_num_rows(), N);
    ASSERT_EQ(field_data->get_valid_rows(), 2);
    EXPECT_FALSE(field_data->is_valid(0));
    ASSERT_TRUE(field_data->is_valid(1));
    ASSERT_TRUE(field_data->is_valid(2));

    auto row1 =
        static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
            field_data->RawValue(1));
    auto row2 =
        static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
            field_data->RawValue(2));
    ASSERT_NE(row1, nullptr);
    ASSERT_NE(row2, nullptr);
    EXPECT_EQ(row1->data_byte_size(), sparse_vectors[0].data_byte_size());
    EXPECT_EQ(row2->data_byte_size(), sparse_vectors[1].data_byte_size());

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushBM25StatsRangeAndCompoundManifest) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto sparse_fid = schema->AddDebugField(
        "bm25", DataType::VECTOR_SPARSE_U32_F32, 0, std::nullopt);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 4;
    std::vector<int64_t> row_ids = {0, 1, 2, 3};
    std::vector<Timestamp> timestamps = {10, 11, 12, 13};
    std::vector<int64_t> pks = {100, 101, 102, 103};
    auto sparse_vectors =
        std::make_unique<knowhere::sparse::SparseRow<SparseValueType>[]>(N);

    sparse_vectors[0] = knowhere::sparse::SparseRow<SparseValueType>(2);
    sparse_vectors[0].set_at(0, 10, 2.0F);
    sparse_vectors[0].set_at(1, 20, 1.0F);
    sparse_vectors[1] = knowhere::sparse::SparseRow<SparseValueType>(1);
    sparse_vectors[1].set_at(0, 10, 1.0F);
    sparse_vectors[2] = knowhere::sparse::SparseRow<SparseValueType>(1);
    sparse_vectors[2].set_at(0, 30, 4.0F);
    sparse_vectors[3] = knowhere::sparse::SparseRow<SparseValueType>(2);
    sparse_vectors[3].set_at(0, 10, 3.0F);
    sparse_vectors[3].set_at(1, 30, 1.0F);

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    auto sparse_array = CreateVectorDataArrayFrom(
        sparse_vectors.get(), nullptr, N, N, (*schema)[sparse_fid]);
    insert_data->mutable_fields_data()->AddAllocated(sparse_array.release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    std::string segment_path = test_dir_ + "/segment_bm25";
    int64_t bm25_field_ids[] = {sparse_fid.get()};
    int64_t first_bm25_stats_log_ids[] = {1001};
    int64_t second_bm25_stats_log_ids[] = {1002};

    C_FLUSH_CONFIG_WITH_SCHEMA(first_config, schema);
    first_config.segment_path = segment_path.c_str();
    first_config.read_version = -1;
    first_config.retry_limit = 3;
    first_config.bm25_field_ids = bm25_field_ids;
    first_config.bm25_stats_log_ids = first_bm25_stats_log_ids;
    first_config.num_bm25_fields = 1;
    first_config.write_merged_bm25_stats = false;

    CFlushResult first_result{};
    auto status = FlushGrowingSegmentData(
        segment.get(), 0, 2, &first_config, &first_result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(first_result.num_rows, 2);
    ASSERT_EQ(first_result.num_bm25_stats, 1);

    auto first_stats = ParseBM25StatsFromResult(first_result, sparse_fid);
    ASSERT_TRUE(first_stats.has_value());
    EXPECT_EQ(first_stats->num_row, 2);
    EXPECT_EQ(first_stats->num_token, 4);
    EXPECT_EQ(first_stats->rows_with_token[10], 2);
    EXPECT_EQ(first_stats->rows_with_token[20], 1);

    C_FLUSH_CONFIG_WITH_SCHEMA(second_config, schema);
    second_config.segment_path = segment_path.c_str();
    second_config.read_version = first_result.committed_version;
    second_config.retry_limit = 3;
    second_config.bm25_field_ids = bm25_field_ids;
    second_config.bm25_stats_log_ids = second_bm25_stats_log_ids;
    second_config.num_bm25_fields = 1;
    second_config.write_merged_bm25_stats = true;

    CFlushResult second_result{};
    status = FlushGrowingSegmentData(
        segment.get(), 2, 4, &second_config, &second_result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(second_result.num_rows, 2);
    ASSERT_EQ(second_result.num_bm25_stats, 1);

    auto second_stats = ParseBM25StatsFromResult(second_result, sparse_fid);
    ASSERT_TRUE(second_stats.has_value());
    EXPECT_EQ(second_stats->num_row, 2);
    EXPECT_EQ(second_stats->num_token, 8);
    EXPECT_EQ(second_stats->rows_with_token[10], 1);
    EXPECT_EQ(second_stats->rows_with_token[30], 2);

    auto stat_key = "bm25." + std::to_string(sparse_fid.get());
    auto stat_paths = ManifestStatPaths(
        segment_path, second_result.committed_version, stat_key);
    ASSERT_EQ(stat_paths.size(), 3);

    auto stats_prefix = "_stats/bm25." + std::to_string(sparse_fid.get()) + "/";
    auto merged_rel_path = stats_prefix + "1";
    auto merged_full_path = segment_path + "/" + merged_rel_path;
    size_t merged_path_count = 0;
    size_t delta_path_count = 0;
    for (const auto& path : stat_paths) {
        auto rel_path = path;
        auto full_prefix = segment_path + "/";
        if (rel_path.rfind(full_prefix, 0) == 0) {
            rel_path = rel_path.substr(full_prefix.size());
        }
        ASSERT_TRUE(rel_path.rfind(stats_prefix, 0) == 0) << rel_path;
        if (rel_path == merged_rel_path) {
            merged_path_count++;
        } else {
            EXPECT_NE(rel_path.substr(stats_prefix.size()), "");
            delta_path_count++;
        }
    }
    EXPECT_EQ(merged_path_count, 1);
    EXPECT_EQ(delta_path_count, 2);
    auto has_merged_path = std::any_of(
        stat_paths.begin(), stat_paths.end(), [&](const std::string& path) {
            auto suffix = "/" + merged_rel_path;
            return path == merged_rel_path || path == merged_full_path ||
                   (path.size() >= suffix.size() &&
                    path.compare(path.size() - suffix.size(),
                                 suffix.size(),
                                 suffix) == 0);
        });
    EXPECT_TRUE(has_merged_path);
    auto merged_stats = ReadBM25StatsFromFile(merged_full_path);
    ASSERT_TRUE(merged_stats.has_value());
    EXPECT_EQ(merged_stats->num_row, 4);
    EXPECT_EQ(merged_stats->num_token, 12);
    EXPECT_EQ(merged_stats->rows_with_token[10], 3);
    EXPECT_EQ(merged_stats->rows_with_token[20], 1);
    EXPECT_EQ(merged_stats->rows_with_token[30], 2);

    FreeFlushResult(&first_result);
    FreeFlushResult(&second_result);
}

// test flush with empty range
TEST_F(FlushGrowingSegmentTest, FlushEmptyRange) {
    // create schema
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 50;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_empty";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush empty range (start == end)
    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 10, 10, &config, &result);

    // should fail or return 0 rows
    if (status.error_code == Success) {
        EXPECT_EQ(result.num_rows, 0);
        FreeFlushResult(&result);
    }
}

// test flush with large data spanning multiple chunks
TEST_F(FlushGrowingSegmentTest, FlushLargeDataMultipleChunks) {
    // create schema
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid =
        schema->AddDebugField("vec", DataType::VECTOR_FLOAT, 64, "L2");
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert large data that spans multiple chunks
    // default chunk size is typically 32K rows
    int N = 50000;  // should span multiple chunks
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_large";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush all data
    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with multiple TEXT columns
TEST_F(FlushGrowingSegmentTest, FlushMultipleTextColumns) {
#ifndef BUILD_VORTEX_BRIDGE
    GTEST_SKIP() << "Vortex support is not enabled";
#endif
    // create schema with multiple varchar fields (simulating TEXT)
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto text1_fid = schema->AddDebugField("text1", DataType::VARCHAR);
    auto text2_fid = schema->AddDebugField("text2", DataType::VARCHAR);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 20;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config with multiple TEXT columns
    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_multi_text";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;

    // configure multiple TEXT columns
    int64_t text_field_ids[] = {text1_fid.get(), text2_fid.get()};
    std::string text1_lob_path =
        test_dir_ + "/lobs/field_" + std::to_string(text1_fid.get());
    std::string text2_lob_path =
        test_dir_ + "/lobs/field_" + std::to_string(text2_fid.get());
    const char* text_lob_paths[] = {text1_lob_path.c_str(),
                                    text2_lob_path.c_str()};
    config.text_field_ids = text_field_ids;
    config.text_lob_paths = text_lob_paths;
    config.num_text_columns = 2;

    // flush data
    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with bool field
TEST_F(FlushGrowingSegmentTest, FlushWithBoolField) {
    // create schema with bool field
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto bool_fid = schema->AddDebugField("bool_field", DataType::BOOL);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 50;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_bool";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush data
    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with all numeric types
TEST_F(FlushGrowingSegmentTest, FlushAllNumericTypes) {
    // create schema with all numeric types
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto i8_fid = schema->AddDebugField("i8_field", DataType::INT8);
    auto i16_fid = schema->AddDebugField("i16_field", DataType::INT16);
    auto i32_fid = schema->AddDebugField("i32_field", DataType::INT32);
    auto f32_fid = schema->AddDebugField("f32_field", DataType::FLOAT);
    auto f64_fid = schema->AddDebugField("f64_field", DataType::DOUBLE);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 50;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_numeric";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush data
    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with different vector types
TEST_F(FlushGrowingSegmentTest, FlushDifferentVectorTypes) {
    // test float16 vector
    {
        auto schema = std::make_shared<Schema>();
        auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
        auto vec_fid =
            schema->AddDebugField("vec", DataType::VECTOR_FLOAT16, 64, "L2");
        schema->set_primary_field_id(pk_fid);

        auto segment = CreateGrowingSegment(schema, empty_index_meta);
        ASSERT_NE(segment, nullptr);

        int N = 30;
        auto dataset = DataGen(schema, N);
        segment->PreInsert(N);
        segment->Insert(0,
                        N,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);

        C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
        std::string segment_path = test_dir_ + "/segment_fp16";
        config.segment_path = segment_path.c_str();
        config.read_version = -1;
        config.retry_limit = 3;
        config.text_field_ids = nullptr;
        config.text_lob_paths = nullptr;
        config.num_text_columns = 0;

        CFlushResult result{};
        auto status =
            FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

        ASSERT_EQ(status.error_code, Success) << status.error_msg;
        ASSERT_EQ(result.num_rows, N);

        FreeFlushResult(&result);
    }

    // test bfloat16 vector
    {
        auto schema = std::make_shared<Schema>();
        auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
        auto vec_fid =
            schema->AddDebugField("vec", DataType::VECTOR_BFLOAT16, 64, "L2");
        schema->set_primary_field_id(pk_fid);

        auto segment = CreateGrowingSegment(schema, empty_index_meta);
        ASSERT_NE(segment, nullptr);

        int N = 30;
        auto dataset = DataGen(schema, N);
        segment->PreInsert(N);
        segment->Insert(0,
                        N,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);

        C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
        std::string segment_path = test_dir_ + "/segment_bf16";
        config.segment_path = segment_path.c_str();
        config.read_version = -1;
        config.retry_limit = 3;
        config.text_field_ids = nullptr;
        config.text_lob_paths = nullptr;
        config.num_text_columns = 0;

        CFlushResult result{};
        auto status =
            FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

        ASSERT_EQ(status.error_code, Success) << status.error_msg;
        ASSERT_EQ(result.num_rows, N);

        FreeFlushResult(&result);
    }

    // test binary vector
    {
        auto schema = std::make_shared<Schema>();
        auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
        auto vec_fid = schema->AddDebugField(
            "vec", DataType::VECTOR_BINARY, 128, "JACCARD");
        schema->set_primary_field_id(pk_fid);

        auto segment = CreateGrowingSegment(schema, empty_index_meta);
        ASSERT_NE(segment, nullptr);

        int N = 30;
        auto dataset = DataGen(schema, N);
        segment->PreInsert(N);
        segment->Insert(0,
                        N,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);

        C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
        std::string segment_path = test_dir_ + "/segment_binary";
        config.segment_path = segment_path.c_str();
        config.read_version = -1;
        config.retry_limit = 3;
        config.text_field_ids = nullptr;
        config.text_lob_paths = nullptr;
        config.num_text_columns = 0;

        CFlushResult result{};
        auto status =
            FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

        ASSERT_EQ(status.error_code, Success) << status.error_msg;
        ASSERT_EQ(result.num_rows, N);

        FreeFlushResult(&result);
    }
}

// test FreeFlushResult with null manifest_path
TEST_F(FlushGrowingSegmentTest, FreeFlushResultNull) {
    CFlushResult result{};
    result.manifest_path = nullptr;
    result.committed_version = 0;
    result.num_rows = 0;

    // should not crash
    FreeFlushResult(&result);
}

// test invalid segment type (sealed segment should fail)
TEST_F(FlushGrowingSegmentTest, FlushSealedSegmentFails) {
    // create schema
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    // create sealed segment (not growing)
    auto segment = CreateSealedSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // prepare flush config
    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_sealed";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush should fail for sealed segment
    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, 10, &config, &result);

    EXPECT_NE(status.error_code, Success);
    free(const_cast<char*>(status.error_msg));
}

// The streaming reader decodes batches on a thread pool and delivers them on
// the calling thread. Comparing it against GetFieldDatasFromManifest would be
// circular (that helper now delegates to it), and single-batch cases cannot
// expose ordering bugs at all. This test forces multiple record batches by
// inserting well past loon's per-batch row limit (reader.record_batch_max_rows
// defaults to 8192) and validates the concatenated result against the values
// that were originally inserted, in order.
TEST_F(FlushGrowingSegmentTest,
       IterateFieldDataFromManifestMultiBatchOrdering) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto val_fid = schema->AddDebugField("val", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // > 2x the default 8192-row batch limit, so the reader must produce
    // several batches and their delivery order becomes observable.
    const int N = 20000;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    C_FLUSH_CONFIG_WITH_SCHEMA(config, schema);
    std::string segment_path = test_dir_ + "/segment_multibatch";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    auto properties =
        storage::LoonFFIPropertiesSingleton::GetInstance().GetProperties();
    ASSERT_NE(properties, nullptr);
    auto field_meta = gen_field_meta(
        1, 2, 3, val_fid.get(), DataType::INT64, DataType::NONE, false);
    std::string manifest_json =
        "{\"base_path\":\"" + segment_path +
        "\",\"ver\":" + std::to_string(result.committed_version) + "}";

    std::vector<FieldDataPtr> streamed;
    auto caller_tid = std::this_thread::get_id();
    storage::IterateFieldDataFromManifest(
        manifest_json,
        properties,
        field_meta,
        DataType::INT64,
        0,
        DataType::NONE,
        std::nullopt,
        [&](FieldDataPtr fd) {
            EXPECT_EQ(std::this_thread::get_id(), caller_tid);
            streamed.push_back(std::move(fd));
        });

    // The point of the test: this must actually span multiple batches,
    // otherwise ordering is trivially satisfied and nothing is verified.
    ASSERT_GT(streamed.size(), 1u)
        << "expected multiple record batches for " << N << " rows";

    // Compare values against the originally inserted column, in order.
    auto expected = dataset.get_col<int64_t>(val_fid);
    ASSERT_EQ(expected.size(), static_cast<size_t>(N));
    int64_t seen = 0;
    for (const auto& fd : streamed) {
        for (int64_t i = 0; i < fd->get_num_rows(); ++i) {
            ASSERT_LT(seen, N);
            EXPECT_EQ(*static_cast<const int64_t*>(fd->RawValue(i)),
                      expected[seen])
                << "value mismatch at row " << seen;
            ++seen;
        }
    }
    EXPECT_EQ(seen, N);

    FreeFlushResult(&result);
}
