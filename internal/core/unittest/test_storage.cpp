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

#include <arrow/scalar.h>
#include <boost/filesystem/path.hpp>
#include <gtest/gtest.h>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <filesystem>
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "aws/core/client/ClientConfiguration.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/common_type_c.h"
#include "common/type_c.h"
#include "gtest/gtest.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"
#include "segcore/segment_c.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
#include "storage/KeyRetriever.h"
#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"
#include "storage/minio/MinioChunkManager.h"
#include "storage/storage_c.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"

// Test-only subclass that exposes the protected ApplyChecksumConfigOverrides
// and NeedChecksumOverride helpers so we can assert their behavior directly.
class TestableMinioChunkManager : public milvus::storage::MinioChunkManager {
 public:
    using MinioChunkManager::ApplyChecksumConfigOverrides;
    using MinioChunkManager::NeedChecksumOverride;
};

using namespace std;
using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::storage;

string bucketName = "a-bucket";

void
FreeErrorStatus(CStatus& status) {
    if (status.error_msg != nullptr && status.error_msg[0] != '\0') {
        free(const_cast<char*>(status.error_msg));
        status.error_msg = nullptr;
    }
}

CStorageConfig
get_azure_storage_config() {
    auto endpoint = "core.windows.net";
    auto accessKey = "devstoreaccount1";
    auto accessValue =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
        "K1SZFPTOtr/KBHBeksoGMGw==";

    return CStorageConfig{endpoint,
                          bucketName.c_str(),
                          accessKey,
                          accessValue,
                          TestRemotePath.c_str(),
                          "remote",
                          "azure",
                          "",
                          "error",
                          "",
                          false,
                          "",
                          false,
                          false,
                          30000,
                          "",
                          false,
                          100};
}

class StorageTest : public testing::Test {
 public:
    StorageTest() {
    }
    ~StorageTest() {
    }
    virtual void
    SetUp() {
    }
};

TEST_F(StorageTest, InitLocalChunkManagerSingleton) {
    auto status = InitLocalChunkManagerSingleton("tmp");
    EXPECT_EQ(status.error_code, Success);
}

TEST_F(StorageTest, TextFieldDataFromManifestResolvesLobRefs) {
#ifndef BUILD_VORTEX_BRIDGE
    GTEST_SKIP() << "Vortex support is not enabled";
#endif

    std::string test_dir =
        "/tmp/text_manifest_reader_" +
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir);
    auto cleanup = [&]() {
        if (std::filesystem::exists(test_dir)) {
            std::filesystem::remove_all(test_dir);
        }
    };

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto text_fid = schema->AddDebugField("text", DataType::TEXT, true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};
    std::vector<std::string> texts = {
        "inline text for text match",
        "",
        std::string(70 * 1024, 'x') + " searchable-tail-token"};
    bool text_valid[N] = {true, false, true};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(texts.data(), text_valid, N, (*schema)[text_fid])
            .release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    CFlushConfig config{};
    std::string segment_path =
        test_dir + "/collection/partition/segment_text_lob";
    std::string text_lob_path = test_dir + "/collection/partition/lobs/" +
                                std::to_string(text_fid.get());
    int64_t text_field_ids[] = {text_fid.get()};
    const char* text_lob_paths[] = {text_lob_path.c_str()};
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = text_field_ids;
    config.text_lob_paths = text_lob_paths;
    config.num_text_columns = 1;

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    auto properties = LoonFFIPropertiesSingleton::GetInstance().GetProperties();
    ASSERT_NE(properties, nullptr);
    auto field_meta = gen_field_meta(
        1, 2, 3, text_fid.get(), DataType::TEXT, DataType::NONE, true);
    std::string manifest_json =
        "{\"base_path\":\"" + segment_path +
        "\",\"ver\":" + std::to_string(result.committed_version) + "}";

    auto raw_datas = GetFieldDatasFromManifest(manifest_json,
                                               properties,
                                               field_meta,
                                               DataType::TEXT,
                                               0,
                                               DataType::NONE);
    ASSERT_EQ(raw_datas.size(), 1);
    ASSERT_TRUE(raw_datas[0]->is_valid(0));
    EXPECT_FALSE(raw_datas[0]->is_valid(1));
    ASSERT_TRUE(raw_datas[0]->is_valid(2));
    EXPECT_NE(*static_cast<const std::string*>(raw_datas[0]->RawValue(0)),
              texts[0]);
    EXPECT_NE(*static_cast<const std::string*>(raw_datas[0]->RawValue(2)),
              texts[2]);

    auto text_datas =
        GetTextFieldDatasFromManifest(manifest_json, properties, field_meta);
    ASSERT_EQ(text_datas.size(), 1);
    ASSERT_TRUE(text_datas[0]->is_valid(0));
    EXPECT_FALSE(text_datas[0]->is_valid(1));
    ASSERT_TRUE(text_datas[0]->is_valid(2));
    EXPECT_EQ(*static_cast<const std::string*>(text_datas[0]->RawValue(0)),
              texts[0]);
    EXPECT_EQ(*static_cast<const std::string*>(text_datas[0]->RawValue(2)),
              texts[2]);

    FreeFlushResult(&result);
    cleanup();
}

// A growing segment born while a function output field was absent from the
// schema (add/drop-function churn) never materializes that column —
// Reopen/FillAbsentFields skip function outputs by design. Flushing with a
// newer schema that carries the field must skip the column instead of
// hitting the insert_record assert (issue #51117).
TEST_F(StorageTest, FlushGrowingSegmentSkipsNonMaterializedFunctionOutput) {
    std::string test_dir =
        "/tmp/flush_skip_fn_output_" +
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir);
    auto cleanup = [&]() {
        if (std::filesystem::exists(test_dir)) {
            std::filesystem::remove_all(test_dir);
        }
    };

    milvus::proto::schema::CollectionSchema schema_proto;
    schema_proto.set_name("flush_skip_fn_output");
    // Real flush schemas always carry the RowID/Timestamp system fields;
    // the flush validates the Timestamp column was exported.
    auto* row_id_field = schema_proto.add_fields();
    row_id_field->set_fieldid(0);
    row_id_field->set_name("RowID");
    row_id_field->set_data_type(milvus::proto::schema::DataType::Int64);
    auto* ts_field = schema_proto.add_fields();
    ts_field->set_fieldid(1);
    ts_field->set_name("Timestamp");
    ts_field->set_data_type(milvus::proto::schema::DataType::Int64);
    auto* pk_field = schema_proto.add_fields();
    pk_field->set_fieldid(100);
    pk_field->set_name("pk");
    pk_field->set_data_type(milvus::proto::schema::DataType::Int64);
    pk_field->set_is_primary_key(true);

    auto segment_schema = Schema::ParseFrom(schema_proto);
    auto segment = CreateGrowingSegment(segment_schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            pks.data(), nullptr, N, (*segment_schema)[FieldId(100)])
            .release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    // Flush schema carries a function output field the segment never
    // materialized.
    auto flush_proto = schema_proto;
    auto* fn_output = flush_proto.add_fields();
    fn_output->set_fieldid(101);
    fn_output->set_name("fn_sparse");
    fn_output->set_data_type(
        milvus::proto::schema::DataType::SparseFloatVector);
    fn_output->set_is_function_output(true);
    std::string schema_blob = flush_proto.SerializeAsString();

    CFlushConfig config{};
    std::string segment_path = test_dir + "/collection/partition/segment_fn";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.schema_blob = schema_blob.data();
    config.schema_length = static_cast<int64_t>(schema_blob.size());
    // Column-group config still carrying the skipped function output: the
    // accounting loop must tolerate its absence instead of failing the flush.
    int64_t column_group_ids[] = {0};
    int64_t column_group_field_ids[] = {0, 1, 100, 101};
    size_t column_group_field_counts[] = {4};
    config.column_group_ids = column_group_ids;
    config.column_group_field_ids = column_group_field_ids;
    config.column_group_field_counts = column_group_field_counts;
    config.num_column_groups = 1;

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);
    ASSERT_EQ(result.num_column_groups, 1u);
    for (size_t i = 0; i < result.num_field_stats; ++i) {
        EXPECT_NE(result.field_ids[i], 101);
    }
    for (size_t i = 0; i < result.num_flushed_fields; ++i) {
        EXPECT_NE(result.flushed_field_ids[i], 101);
    }

    FreeFlushResult(&result);
    cleanup();
}

// A regular field carried by a staler flush schema but absent from the
// segment's own schema was dropped before the segment was created: the
// flush must skip it instead of erroring or asserting.
TEST_F(StorageTest, FlushGrowingSegmentSkipsFieldAbsentFromSegmentSchema) {
    std::string test_dir =
        "/tmp/flush_skip_dropped_field_" +
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir);
    auto cleanup = [&]() {
        if (std::filesystem::exists(test_dir)) {
            std::filesystem::remove_all(test_dir);
        }
    };

    milvus::proto::schema::CollectionSchema schema_proto;
    schema_proto.set_name("flush_skip_dropped_field");
    auto* row_id_field = schema_proto.add_fields();
    row_id_field->set_fieldid(0);
    row_id_field->set_name("RowID");
    row_id_field->set_data_type(milvus::proto::schema::DataType::Int64);
    auto* ts_field = schema_proto.add_fields();
    ts_field->set_fieldid(1);
    ts_field->set_name("Timestamp");
    ts_field->set_data_type(milvus::proto::schema::DataType::Int64);
    auto* pk_field = schema_proto.add_fields();
    pk_field->set_fieldid(100);
    pk_field->set_name("pk");
    pk_field->set_data_type(milvus::proto::schema::DataType::Int64);
    pk_field->set_is_primary_key(true);

    auto segment_schema = Schema::ParseFrom(schema_proto);
    auto segment = CreateGrowingSegment(segment_schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            pks.data(), nullptr, N, (*segment_schema)[FieldId(100)])
            .release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    // Staler flush schema still carries an ordinary field the segment's own
    // schema never had (dropped before the segment was created).
    auto flush_proto = schema_proto;
    auto* extra = flush_proto.add_fields();
    extra->set_fieldid(101);
    extra->set_name("dropped_field");
    extra->set_data_type(milvus::proto::schema::DataType::VarChar);
    extra->set_nullable(true);
    auto* param = extra->add_type_params();
    param->set_key("max_length");
    param->set_value("64");
    std::string schema_blob = flush_proto.SerializeAsString();

    CFlushConfig config{};
    std::string segment_path = test_dir + "/collection/partition/segment_dp";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.schema_blob = schema_blob.data();
    config.schema_length = static_cast<int64_t>(schema_blob.size());

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);
    for (size_t i = 0; i < result.num_flushed_fields; ++i) {
        EXPECT_NE(result.flushed_field_ids[i], 101);
    }

    FreeFlushResult(&result);
    cleanup();
}

// A function-output column the ctor allocated but no insert ever filled
// (older-era replayed inserts omit it) is not materialized: the flush must
// skip it instead of tripping the empty-chunk assert.
TEST_F(StorageTest, FlushGrowingSegmentSkipsEmptyFunctionOutputColumn) {
    std::string test_dir =
        "/tmp/flush_skip_empty_fn_output_" +
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir);
    auto cleanup = [&]() {
        if (std::filesystem::exists(test_dir)) {
            std::filesystem::remove_all(test_dir);
        }
    };

    milvus::proto::schema::CollectionSchema schema_proto;
    schema_proto.set_name("flush_skip_empty_fn_output");
    auto* row_id_field = schema_proto.add_fields();
    row_id_field->set_fieldid(0);
    row_id_field->set_name("RowID");
    row_id_field->set_data_type(milvus::proto::schema::DataType::Int64);
    auto* ts_field = schema_proto.add_fields();
    ts_field->set_fieldid(1);
    ts_field->set_name("Timestamp");
    ts_field->set_data_type(milvus::proto::schema::DataType::Int64);
    auto* pk_field = schema_proto.add_fields();
    pk_field->set_fieldid(100);
    pk_field->set_name("pk");
    pk_field->set_data_type(milvus::proto::schema::DataType::Int64);
    pk_field->set_is_primary_key(true);
    // Function output present in the segment's own schema: the ctor
    // allocates its column, but replayed inserts never fill it.
    auto* fn_output = schema_proto.add_fields();
    fn_output->set_fieldid(101);
    fn_output->set_name("fn_sparse");
    fn_output->set_data_type(
        milvus::proto::schema::DataType::SparseFloatVector);
    fn_output->set_is_function_output(true);

    auto segment_schema = Schema::ParseFrom(schema_proto);
    auto segment = CreateGrowingSegment(segment_schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};

    // Insert carries no data for the function output; the consume path
    // exempts function outputs, leaving field 101 allocated but empty.
    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            pks.data(), nullptr, N, (*segment_schema)[FieldId(100)])
            .release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    std::string schema_blob = schema_proto.SerializeAsString();

    CFlushConfig config{};
    std::string segment_path = test_dir + "/collection/partition/segment_ef";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.schema_blob = schema_blob.data();
    config.schema_length = static_cast<int64_t>(schema_blob.size());

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);
    for (size_t i = 0; i < result.num_flushed_fields; ++i) {
        EXPECT_NE(result.flushed_field_ids[i], 101);
    }

    FreeFlushResult(&result);
    cleanup();
}

// A manifest column group whose only field was dropped from the segment
// schema before recovery is a legal leftover of drop semantics: reloading
// the growing segment must skip that group instead of failing the whole
// load with milvus-storage "No needed columns found in column group".
TEST_F(StorageTest, LoadGrowingSegmentSkipsDroppedFieldColumnGroup) {
    std::string test_dir =
        "/tmp/load_skip_dropped_group_" +
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir);
    auto cleanup = [&]() {
        if (std::filesystem::exists(test_dir)) {
            std::filesystem::remove_all(test_dir);
        }
    };

    // Reduced schema: what remains after field 101 is dropped.
    milvus::proto::schema::CollectionSchema reduced_proto;
    reduced_proto.set_name("load_skip_dropped_group");
    auto* row_id_field = reduced_proto.add_fields();
    row_id_field->set_fieldid(0);
    row_id_field->set_name("RowID");
    row_id_field->set_data_type(milvus::proto::schema::DataType::Int64);
    auto* ts_field = reduced_proto.add_fields();
    ts_field->set_fieldid(1);
    ts_field->set_name("Timestamp");
    ts_field->set_data_type(milvus::proto::schema::DataType::Int64);
    auto* pk_field = reduced_proto.add_fields();
    pk_field->set_fieldid(100);
    pk_field->set_name("pk");
    pk_field->set_data_type(milvus::proto::schema::DataType::Int64);
    pk_field->set_is_primary_key(true);

    // Full schema at flush time still carries field 101.
    auto full_proto = reduced_proto;
    auto* extra_field = full_proto.add_fields();
    extra_field->set_fieldid(101);
    extra_field->set_name("extra");
    extra_field->set_data_type(milvus::proto::schema::DataType::Int64);

    auto full_schema = Schema::ParseFrom(full_proto);
    auto segment = CreateGrowingSegment(full_schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};
    std::vector<int64_t> extras = {200, 201, 202};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            pks.data(), nullptr, N, (*full_schema)[FieldId(100)])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            extras.data(), nullptr, N, (*full_schema)[FieldId(101)])
            .release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    std::string schema_blob = full_proto.SerializeAsString();

    CFlushConfig config{};
    std::string segment_path = test_dir + "/collection/partition/segment_lg";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.schema_blob = schema_blob.data();
    config.schema_length = static_cast<int64_t>(schema_blob.size());
    // Force field 101 into its own column group so that dropping the field
    // leaves a group with no live field.
    config.schema_based_pattern = "0|1|100,101";

    CFlushResult result{};
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);
    std::string manifest_json =
        "{\"base_path\":\"" + segment_path +
        "\",\"ver\":" + std::to_string(result.committed_version) + "}";
    FreeFlushResult(&result);

    // Guard against a vacuous pass: the split pattern must have produced a
    // column group holding field 101 exclusively, or the load below would
    // succeed via projection without exercising the skip branch.
    auto properties = LoonFFIPropertiesSingleton::GetInstance().GetProperties();
    ASSERT_NE(properties, nullptr);
    auto manifest = GetLoonManifest(manifest_json, properties);
    ASSERT_EQ(manifest->columnGroups().size(), 2u);
    bool has_exclusive_101_group = false;
    for (const auto& cg : manifest->columnGroups()) {
        if (cg->columns.size() == 1 && cg->columns[0] == "101") {
            has_exclusive_101_group = true;
        }
    }
    ASSERT_TRUE(has_exclusive_101_group)
        << "expected a column group holding field 101 exclusively";

    // Reload the manifest under the reduced schema (field 101 dropped).
    auto reduced_schema = Schema::ParseFrom(reduced_proto);
    auto reloaded = CreateGrowingSegment(reduced_schema, empty_index_meta);
    ASSERT_NE(reloaded, nullptr);

    milvus::proto::segcore::SegmentLoadInfo load_info;
    load_info.set_segmentid(1);
    load_info.set_num_of_rows(N);
    load_info.set_manifest_path(manifest_json);
    reloaded->SetLoadInfo(load_info);

    milvus::tracer::TraceContext trace_ctx;
    ASSERT_NO_THROW(reloaded->Load(trace_ctx, nullptr));
    EXPECT_EQ(reloaded->get_row_count(), N);
    EXPECT_TRUE(reloaded->HasFieldData(FieldId(100)));
    EXPECT_FALSE(reloaded->HasFieldData(FieldId(101)));

    cleanup();
}

TEST_F(StorageTest, GetLocalUsedSize) {
    int64_t size = 0;
    auto lcm = LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    EXPECT_EQ(lcm->GetRootPath(), TestLocalPath);
    string test_dir =
        lcm->GetRootPath() + "tmp" +
        // add random number to avoid dir conflict
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count());
    string test_file = test_dir + "/test.txt";

    auto status = GetLocalUsedSize(test_dir.c_str(), &size);
    EXPECT_EQ(status.error_code, Success);
    EXPECT_EQ(size, 0);
    lcm->CreateDir(test_dir);
    lcm->CreateFile(test_file);
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    lcm->Write(test_file, data, sizeof(data));
    status = GetLocalUsedSize(test_dir.c_str(), &size);
    EXPECT_EQ(status.error_code, Success);
    EXPECT_EQ(size, 5);
    lcm->RemoveDir(test_dir);
}

TEST_F(StorageTest, InitRemoteChunkManagerSingleton) {
    CStorageConfig storageConfig = get_azure_storage_config();
    auto status = InitRemoteChunkManagerSingleton(storageConfig);
    EXPECT_STREQ(status.error_msg, "");
    EXPECT_EQ(status.error_code, Success);
    auto rcm =
        RemoteChunkManagerSingleton::GetInstance().GetRemoteChunkManager();
    EXPECT_EQ(rcm->GetRootPath(), TestRemotePath);
}

TEST_F(StorageTest, CleanRemoteChunkManagerSingleton) {
    CleanRemoteChunkManagerSingleton();
}

TEST_F(StorageTest, InitArrowReaderConfig) {
    auto default_cache_options =
        parquet::default_arrow_reader_properties().cache_options();

    auto status = InitArrowReaderConfig(CArrowReaderConfig{-1, 0});
    EXPECT_EQ(status.error_code, ConfigInvalid);
    FreeErrorStatus(status);
    status = InitArrowReaderConfig(CArrowReaderConfig{0, -1});
    EXPECT_EQ(status.error_code, ConfigInvalid);
    FreeErrorStatus(status);
    status = InitArrowReaderConfig(CArrowReaderConfig{64 * 1024, 32 * 1024});
    EXPECT_NE(status.error_code, Success);
    FreeErrorStatus(status);

    status =
        InitArrowReaderConfig(CArrowReaderConfig{32 * 1024, 4 * 1024 * 1024});
    ASSERT_EQ(status.error_code, Success) << status.error_msg;

    auto cache_options = GetArrowReaderProperties().cache_options();
    EXPECT_EQ(cache_options.hole_size_limit, 32 * 1024);
    EXPECT_EQ(cache_options.range_size_limit, 4 * 1024 * 1024);

    auto properties = LoonFFIPropertiesSingleton::GetInstance().GetProperties();
    ASSERT_NE(properties, nullptr);

    auto hole_size_limit = milvus_storage::api::GetValue<int64_t>(
        *properties, PROPERTY_READER_PARQUET_PREBUFFER_HOLE_SIZE_LIMIT);
    ASSERT_TRUE(hole_size_limit.ok()) << hole_size_limit.status().ToString();
    EXPECT_EQ(hole_size_limit.ValueOrDie(), 32 * 1024);

    auto range_size_limit = milvus_storage::api::GetValue<int64_t>(
        *properties, PROPERTY_READER_PARQUET_PREBUFFER_RANGE_SIZE_LIMIT);
    ASSERT_TRUE(range_size_limit.ok()) << range_size_limit.status().ToString();
    EXPECT_EQ(range_size_limit.ValueOrDie(), 4 * 1024 * 1024);

    status = InitArrowReaderConfig(CArrowReaderConfig{0, 0});
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    cache_options = GetArrowReaderProperties().cache_options();
    EXPECT_EQ(cache_options.hole_size_limit,
              default_cache_options.hole_size_limit);
    EXPECT_EQ(cache_options.range_size_limit,
              default_cache_options.range_size_limit);
}

class StorageUtilTest : public testing::Test {
 public:
    StorageUtilTest() = default;
    ~StorageUtilTest() {
    }
    void
    SetUp() override {
    }
};

TEST_F(StorageUtilTest, CreateArrowScalarFromDefaultValue) {
    {
        FieldMeta field_without_defval(
            FieldName("f"), FieldId(100), DataType::INT64, false, std::nullopt);
        ASSERT_ANY_THROW(
            CreateArrowScalarFromDefaultValue(field_without_defval));
    }
    {
        DefaultValueType default_value;
        default_value.set_int_data(10);
        FieldMeta int_field(FieldName("f"),
                            FieldId(100),
                            DataType::INT32,
                            false,
                            default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(int_field);
        ASSERT_TRUE(scalar->Equals(*arrow::MakeScalar(int32_t(10))));
    }
    {
        DefaultValueType default_value;
        default_value.set_long_data(10);
        FieldMeta long_field(FieldName("f"),
                             FieldId(100),
                             DataType::INT64,
                             false,
                             default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(long_field);
        ASSERT_TRUE(scalar->Equals(*arrow::MakeScalar(int64_t(10))));
    }
    {
        DefaultValueType default_value;
        default_value.set_float_data(1.0f);
        FieldMeta float_field(FieldName("f"),
                              FieldId(100),
                              DataType::FLOAT,
                              false,
                              default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(float_field);
        ASSERT_TRUE(scalar->ApproxEquals(*arrow::MakeScalar(1.0f)));
    }
    {
        DefaultValueType default_value;
        default_value.set_double_data(1.0f);
        FieldMeta double_field(FieldName("f"),
                               FieldId(100),
                               DataType::DOUBLE,
                               false,
                               default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(double_field);
        ASSERT_TRUE(scalar->ApproxEquals(arrow::DoubleScalar(1.0f)));
    }
    {
        DefaultValueType default_value;
        default_value.set_timestamptz_data(123456789);
        FieldMeta timestamptz_field(FieldName("f"),
                                    FieldId(100),
                                    DataType::TIMESTAMPTZ,
                                    false,
                                    default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(timestamptz_field);
        ASSERT_TRUE(scalar->Equals(*arrow::MakeScalar(int64_t(123456789))));
    }
    {
        DefaultValueType default_value;
        default_value.set_bool_data(true);
        FieldMeta bool_field(
            FieldName("f"), FieldId(100), DataType::BOOL, false, default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(bool_field);
        ASSERT_TRUE(scalar->Equals(*arrow::MakeScalar(true)));
    }
    {
        DefaultValueType default_value;
        default_value.set_string_data("bar");
        FieldMeta varchar_field(FieldName("f"),
                                FieldId(100),
                                DataType::VARCHAR,
                                false,
                                default_value);
        auto scalar = CreateArrowScalarFromDefaultValue(varchar_field);
        ASSERT_TRUE(scalar->Equals(*arrow::MakeScalar("bar")));
    }
    {
        FieldMeta unsupport_field(
            FieldName("f"), FieldId(100), DataType::JSON, false, std::nullopt);
        ASSERT_ANY_THROW(CreateArrowScalarFromDefaultValue(unsupport_field));
    }
}

TEST_F(StorageUtilTest, TestInitArrowFileSystem) {
    // Test local storage configuration
    {
        StorageConfig local_config;
        local_config.storage_type = "local";
        local_config.root_path = TestLocalPath;

        auto fs = InitArrowFileSystem(local_config);
        ASSERT_NE(fs, nullptr);
    }

    // // Test remote storage configuration (Azure)
    // {
    //     StorageConfig remote_config;
    //     remote_config.storage_type = "remote";
    //     remote_config.cloud_provider = "azure";
    //     remote_config.address = "core.windows.net";
    //     remote_config.bucket_name = "test-bucket";
    //     remote_config.access_key_id = "test-access-key";
    //     remote_config.access_key_value = "test-access-value";
    //     remote_config.root_path = "/tmp/milvus/remote_data";
    //     remote_config.iam_endpoint = "";
    //     remote_config.log_level = "error";
    //     remote_config.region = "";
    //     remote_config.useSSL = false;
    //     remote_config.sslCACert = "";
    //     remote_config.useIAM = false;
    //     remote_config.useVirtualHost = false;
    //     remote_config.requestTimeoutMs = 30000;
    //     remote_config.gcp_credential_json = "";

    //     auto fs = InitArrowFileSystem(remote_config);
    //     ASSERT_NE(fs, nullptr);
    // }
}

// Test cases for NormalizePath function
// NormalizePath uses boost::filesystem::path::lexically_normal() and then
// removes trailing "/." (only the dot, keeping the slash)
TEST_F(StorageUtilTest, NormalizePath) {
    // === Basic paths ===
    EXPECT_EQ(NormalizePath(boost::filesystem::path("a/b/c")), "a/b/c");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("")), "");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("file")), "file");

    // === Dot handling ===
    EXPECT_EQ(NormalizePath(boost::filesystem::path(".")), ".");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("./a/b")), "a/b");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("a/./b")), "a/b");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("a/b/.")), "a/b/");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("./a/./b/.")), "a/b/");

    // === Double dot (..) handling ===
    EXPECT_EQ(NormalizePath(boost::filesystem::path("a/b/../c")), "a/c");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("a/b/c/../../d")), "a/d");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("../a/b")), "../a/b");

    // === Trailing slash ===
    EXPECT_EQ(NormalizePath(boost::filesystem::path("a/b/")), "a/b/");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("files/")), "files/");

    // === Absolute paths ===
    EXPECT_EQ(NormalizePath(boost::filesystem::path("/a/b/c")), "/a/b/c");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("/a/./b")), "/a/b");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("/a/b/.")), "/a/b/");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("/")), "/");

    // === Multiple slashes ===
    EXPECT_EQ(NormalizePath(boost::filesystem::path("a//b//c")), "a/b/c");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("a/./b//c")), "a/b/c");

    // === Real-world scenarios (S3/MinIO) ===
    EXPECT_EQ(NormalizePath(boost::filesystem::path("bucket/index_files/123")),
              "bucket/index_files/123");
    // Key fix for 403 error
    EXPECT_EQ(
        NormalizePath(boost::filesystem::path("./index_files/segment_123")),
        "index_files/segment_123");

    // Path construction with root_path = "."
    boost::filesystem::path prefix = ".";
    boost::filesystem::path path = "index_files";
    boost::filesystem::path path1 = "segment_123";
    EXPECT_EQ(NormalizePath(prefix / path / path1), "index_files/segment_123");

    // Non-empty root path
    boost::filesystem::path prefix2 = "files";
    EXPECT_EQ(NormalizePath(prefix2 / path / path1),
              "files/index_files/segment_123");

    // Root path with trailing slash
    boost::filesystem::path prefix3 = "files/";
    EXPECT_EQ(NormalizePath(prefix3 / path / path1),
              "files/index_files/segment_123");

    // Empty root path
    boost::filesystem::path prefix4 = "";
    EXPECT_EQ(NormalizePath(prefix4 / path / path1), "index_files/segment_123");

    // === Edge cases ===
    EXPECT_EQ(NormalizePath(boost::filesystem::path("a/b/..")), "a");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("./a/../b/./c/../d")),
              "b/d");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("a/b c/d")), "a/b c/d");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("./.")), ".");
    EXPECT_EQ(NormalizePath(boost::filesystem::path("./..")), "..");
}

TEST(MinioChecksumConfig, OverridesAreWhenRequired) {
    // Regression guard: AWS SDK C++ 1.11.x defaults the checksum policy to
    // WHEN_SUPPORTED, which makes the V4 signer switch PutObject uploads to
    // aws-chunked + STREAMING-UNSIGNED-PAYLOAD-TRAILER. Aliyun OSS rejects
    // that combination (x-oss-ec=0017-00000804). MinioChunkManager must
    // override both directions to WHEN_REQUIRED so the SDK only adds
    // checksums when the operation model demands them.

    // Aws::Client::ClientConfiguration's default ctor reads SDK globals
    // (logger / http client factory) set up by Aws::InitAPI; without it the
    // ctor segfaults. Use MinioChunkManager's idempotent init helper so we
    // share init_count_ with any production code paths in the same binary.
    TestableMinioChunkManager init_guard;
    init_guard.InitSDKAPIDefault("info");

    Aws::Client::ClientConfiguration config;
    // Sanity check: the SDK defaults are WHEN_SUPPORTED for both directions.
    EXPECT_EQ(config.checksumConfig.requestChecksumCalculation,
              Aws::Client::RequestChecksumCalculation::WHEN_SUPPORTED);
    EXPECT_EQ(config.checksumConfig.responseChecksumValidation,
              Aws::Client::ResponseChecksumValidation::WHEN_SUPPORTED);

    TestableMinioChunkManager::ApplyChecksumConfigOverrides(config);

    EXPECT_EQ(config.checksumConfig.requestChecksumCalculation,
              Aws::Client::RequestChecksumCalculation::WHEN_REQUIRED);
    EXPECT_EQ(config.checksumConfig.responseChecksumValidation,
              Aws::Client::ResponseChecksumValidation::WHEN_REQUIRED);
}

TEST(MinioChecksumConfig, NeedChecksumOverrideDispatch) {
    using Mgr = TestableMinioChunkManager;

    // Non-AWS S3-compatible backends need the override.
    EXPECT_TRUE(Mgr::NeedChecksumOverride("gcp"));
    EXPECT_TRUE(Mgr::NeedChecksumOverride("aliyun"));
    EXPECT_TRUE(Mgr::NeedChecksumOverride("tencent"));
    EXPECT_TRUE(Mgr::NeedChecksumOverride("huawei"));

    // AWS S3 / MinIO accept the default WHEN_SUPPORTED behavior.
    EXPECT_FALSE(Mgr::NeedChecksumOverride("aws"));
    EXPECT_FALSE(Mgr::NeedChecksumOverride(""));
    EXPECT_FALSE(Mgr::NeedChecksumOverride("unknown"));
}
